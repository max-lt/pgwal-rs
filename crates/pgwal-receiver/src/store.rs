use std::fs::TryLockError;
use std::io;
use std::path::{Path, PathBuf};

use pgwal::{Lsn, is_segment_size, parse_segment_file_name, segment_file_name};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::error::Error;

pub(crate) const PARTIAL_SUFFIX: &str = ".partial";
const SYSTEM_ID_FILE: &str = "system_id";
const LOCK_FILE: &str = "lock";
const FLUSHED_FILE: &str = "flushed";

/// WAL of one cluster, stored as Postgres segment files.
///
/// Every segment is written from offset 0 and padded to full size, as in
/// `pg_wal`, so a restore can use the files as they are. The unfinished segment
/// carries a `.partial` suffix.
///
/// One store at a time owns a directory. The lock is released when the store
/// is dropped or the process dies.
pub(crate) struct WalStore {
    dir: PathBuf,
    timeline: u32,
    seg_size: u64,
    open: Option<(u64, File)>,
    written: Lsn,
    /// Records the flushed position at every flush. After a crash, it says
    /// how much local WAL is real.
    flushed_file: File,
    recorded: Lsn,
    _lock: std::fs::File,
}

impl WalStore {
    /// Refuses a directory that another store owns, or that holds the WAL of
    /// another cluster.
    pub async fn open(
        dir: PathBuf,
        system_id: u64,
        timeline: u32,
        seg_size: u64,
    ) -> Result<Self, Error> {
        fs::create_dir_all(&dir).await?;
        let lock = lock(&dir)?;
        match read_system_id(&dir).await? {
            Some(found) if found == system_id => {}
            Some(found) => {
                return Err(Error::SystemMismatch {
                    found,
                    expected: system_id,
                });
            }
            None => write_durably(&dir, SYSTEM_ID_FILE, system_id.to_string().as_bytes()).await?,
        }

        let flushed_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(FLUSHED_FILE))
            .await?;
        Ok(Self {
            dir,
            timeline,
            seg_size,
            open: None,
            written: Lsn(0),
            flushed_file,
            recorded: Lsn(0),
            _lock: lock,
        })
    }

    /// Where streaming restarts: the start of the unfinished segment, which gets
    /// rewritten whole, or the end of the last finished one.
    pub async fn resume_point(&self) -> Result<Option<Lsn>, Error> {
        let (mut partial, mut finished) = (None, None);
        for file in segment_files(&self.dir, self.seg_size).await? {
            if file.timeline != self.timeline {
                continue;
            }
            if file.partial {
                partial = partial.max(Some(file.segno));
            } else {
                finished = finished.max(Some(file.segno));
            }
        }
        Ok(match (partial, finished) {
            (Some(segno), _) => Some(Lsn(segno * self.seg_size)),
            (None, Some(segno)) => Some(Lsn((segno + 1) * self.seg_size)),
            (None, None) => None,
        })
    }

    pub fn start_at(&mut self, lsn: Lsn) {
        assert_eq!(
            lsn.segment_offset(self.seg_size),
            0,
            "{lsn} is not a segment boundary"
        );
        self.open = None;
        self.written = lsn;
        // A rewrite up to the recorded position still needs its flush.
        self.recorded = Lsn(0);
    }

    /// Appends WAL that starts at `start`, which must follow what is written.
    pub async fn write(&mut self, start: Lsn, mut data: &[u8]) -> Result<(), Error> {
        if start != self.written {
            return Err(Error::Protocol(format!(
                "WAL gap: expected {}, got {start}",
                self.written
            )));
        }
        while !data.is_empty() {
            let room = self.seg_size - self.written.segment_offset(self.seg_size);
            let n = data.len().min(room as usize);
            let segno = self.written.segment_no(self.seg_size);
            self.segment(segno).await?.write_all(&data[..n]).await?;
            data = &data[n..];
            self.written = Lsn(self.written.0 + n as u64);
            if n as u64 == room {
                self.finish_segment().await?;
            }
        }
        Ok(())
    }

    /// Makes everything written so far durable, and returns where it ends.
    pub async fn flush(&mut self) -> Result<Lsn, Error> {
        if self.written == self.recorded {
            return Ok(self.written);
        }
        if let Some((_, file)) = &mut self.open {
            // A tokio write ends on a blocking thread after write_all
            // returns, and only a flush gives its error: a sync drops it.
            file.flush().await?;
            file.sync_data().await?;
        }
        self.flushed_file.seek(io::SeekFrom::Start(0)).await?;
        self.flushed_file
            .write_all(&encode_flushed(self.timeline, self.written))
            .await?;
        self.flushed_file.flush().await?;
        self.flushed_file.sync_data().await?;
        self.recorded = self.written;
        Ok(self.written)
    }

    pub fn written(&self) -> Lsn {
        self.written
    }

    /// Opens segment `segno` for writing. A new file replaces any earlier one
    /// with the same name, so a rewrite leaves no stale tail.
    async fn segment(&mut self, segno: u64) -> Result<&mut File, Error> {
        if self.open.as_ref().is_none_or(|(open, _)| *open != segno) {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(self.partial_path(segno))
                .await?;
            file.set_len(self.seg_size).await?;
            sync_dir(&self.dir).await?;
            self.open = Some((segno, file));
        }
        let (_, file) = self.open.as_mut().expect("segment opened above");
        Ok(file)
    }

    async fn finish_segment(&mut self) -> Result<(), Error> {
        let Some((segno, mut file)) = self.open.take() else {
            return Ok(());
        };
        file.flush().await?;
        file.sync_data().await?;
        fs::rename(self.partial_path(segno), self.path(segno)).await?;
        sync_dir(&self.dir).await?;
        Ok(())
    }

    fn path(&self, segno: u64) -> PathBuf {
        self.dir
            .join(segment_file_name(self.timeline, segno, self.seg_size))
    }

    fn partial_path(&self, segno: u64) -> PathBuf {
        self.dir.join(format!(
            "{}{PARTIAL_SUFFIX}",
            segment_file_name(self.timeline, segno, self.seg_size)
        ))
    }
}

/// `lsn` and `timeline`, then a checksum of both: a torn write reads back
/// as no record.
fn encode_flushed(timeline: u32, lsn: Lsn) -> [u8; 20] {
    let mut record = [0u8; 20];
    record[..8].copy_from_slice(&lsn.0.to_le_bytes());
    record[8..12].copy_from_slice(&timeline.to_le_bytes());
    let sum = fnv1a(&record[..12]);
    record[12..].copy_from_slice(&sum.to_le_bytes());
    record
}

/// The flushed position a store recorded: `(timeline, lsn)`.
pub(crate) async fn read_flushed(dir: &Path) -> Result<Option<(u32, Lsn)>, Error> {
    let record = match fs::read(dir.join(FLUSHED_FILE)).await {
        Ok(record) => record,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let Some(record) = record.get(..20) else {
        return Ok(None);
    };
    let sum = u64::from_le_bytes(record[12..20].try_into().expect("8 bytes"));
    if sum != fnv1a(&record[..12]) {
        return Ok(None);
    }
    let lsn = Lsn(u64::from_le_bytes(record[..8].try_into().expect("8 bytes")));
    let timeline = u32::from_le_bytes(record[8..12].try_into().expect("4 bytes"));
    Ok(Some((timeline, lsn)))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &b| {
        (hash ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A segment file of a store.
pub(crate) struct SegmentFile {
    pub path: PathBuf,
    pub timeline: u32,
    pub segno: u64,
    /// The unfinished segment.
    pub partial: bool,
}

/// The segment files in `dir`.
pub(crate) async fn segment_files(dir: &Path, seg_size: u64) -> Result<Vec<SegmentFile>, Error> {
    let mut files = Vec::new();
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let (base, partial) = match name.strip_suffix(PARTIAL_SUFFIX) {
            Some(base) => (base, true),
            None => (name, false),
        };
        let Some((timeline, segno)) = parse_segment_file_name(base, seg_size) else {
            continue;
        };
        files.push(SegmentFile {
            path: entry.path(),
            timeline,
            segno,
            partial,
        });
    }
    Ok(files)
}

/// Removes the segment files of every timeline but `timeline`.
pub(crate) async fn remove_other_timelines(
    dir: &Path,
    timeline: u32,
    seg_size: u64,
) -> Result<(), Error> {
    for file in segment_files(dir, seg_size).await? {
        if file.timeline != timeline {
            fs::remove_file(file.path).await?;
        }
    }
    Ok(())
}

/// The system id a store directory holds WAL of.
pub(crate) async fn read_system_id(dir: &Path) -> Result<Option<u64>, Error> {
    match fs::read_to_string(dir.join(SYSTEM_ID_FILE)).await {
        Ok(text) => text
            .parse()
            .map(Some)
            .map_err(|_| Error::Store(format!("corrupt system id {text:?}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The start of the first local segment of `timeline`, and the segment size.
pub(crate) async fn first_segment(dir: &Path, timeline: u32) -> Result<Option<(Lsn, u64)>, Error> {
    let mut first: Option<(String, u64)> = None;
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let base = name.strip_suffix(PARTIAL_SUFFIX).unwrap_or(name);
        let is_segment = base.len() == 24 && base.bytes().all(|b| b.is_ascii_hexdigit());
        if !is_segment || u32::from_str_radix(&base[..8], 16).ok() != Some(timeline) {
            continue;
        }
        if first.as_ref().is_none_or(|(f, _)| base < f.as_str()) {
            // A crash between the truncate and the set_len of a segment
            // leaves it empty, with no WAL.
            let len = entry.metadata().await?.len();
            if is_segment_size(len) {
                first = Some((base.to_owned(), len));
            }
        }
    }
    let Some((name, seg_size)) = first else {
        return Ok(None);
    };
    let (_, segno) = parse_segment_file_name(&name, seg_size)
        .ok_or_else(|| Error::Store(format!("bad segment {name} of {seg_size} bytes")))?;
    Ok(Some((Lsn(segno * seg_size), seg_size)))
}

fn lock(dir: &Path) -> Result<std::fs::File, Error> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(LOCK_FILE))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(Error::Store(format!(
            "{} is in use by another receiver",
            dir.display()
        ))),
        Err(TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Replaces `dir/name` with `contents`. After a crash the file holds either
/// the old or the new contents, never a mix.
pub(crate) async fn write_durably(dir: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut file = File::create(&tmp).await?;
    file.write_all(contents).await?;
    file.flush().await?;
    file.sync_all().await?;
    fs::rename(&tmp, dir.join(name)).await?;
    sync_dir(dir).await
}

async fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir).await?.sync_all().await
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: u64 = 1 << 20;

    async fn open_store(dir: &Path) -> WalStore {
        WalStore::open(dir.to_path_buf(), 7401, 1, SEG)
            .await
            .unwrap()
    }

    fn partial_path(dir: &Path, segno: u64) -> PathBuf {
        dir.join(format!("{}.partial", segment_file_name(1, segno, SEG)))
    }

    #[tokio::test]
    async fn crossing_a_boundary_finishes_one_segment_and_resumes_at_the_next() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(tmp.path()).await;
        store.start_at(Lsn(SEG));
        let wal = vec![7u8; (SEG + SEG / 2) as usize];
        store.write(Lsn(SEG), &wal).await.unwrap();
        assert_eq!(store.flush().await.unwrap(), Lsn(SEG + wal.len() as u64));

        let finished = tmp.path().join(segment_file_name(1, 1, SEG));
        assert_eq!(fs::read(&finished).await.unwrap(), vec![7u8; SEG as usize]);
        assert_eq!(
            fs::metadata(partial_path(tmp.path(), 2))
                .await
                .unwrap()
                .len(),
            SEG
        );

        drop(store);
        let reopened = open_store(tmp.path()).await;
        assert_eq!(reopened.resume_point().await.unwrap(), Some(Lsn(2 * SEG)));
    }

    #[tokio::test]
    async fn filling_a_segment_exactly_leaves_no_partial() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(tmp.path()).await;
        store.start_at(Lsn(SEG));
        store
            .write(Lsn(SEG), &vec![1u8; SEG as usize])
            .await
            .unwrap();
        assert!(tmp.path().join(segment_file_name(1, 1, SEG)).exists());
        assert!(!partial_path(tmp.path(), 1).exists());
        assert!(!partial_path(tmp.path(), 2).exists());
        assert_eq!(store.resume_point().await.unwrap(), Some(Lsn(2 * SEG)));
    }

    #[tokio::test]
    async fn a_rewrite_leaves_no_stale_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(tmp.path()).await;
        store.start_at(Lsn(SEG));
        store.write(Lsn(SEG), &[7u8; 100]).await.unwrap();
        store.flush().await.unwrap();
        drop(store);

        let mut store = open_store(tmp.path()).await;
        let resume = store.resume_point().await.unwrap().unwrap();
        assert_eq!(resume, Lsn(SEG));
        store.start_at(resume);
        store.write(resume, &[9u8; 10]).await.unwrap();
        store.flush().await.unwrap();

        let segment = fs::read(partial_path(tmp.path(), 1)).await.unwrap();
        assert_eq!(&segment[..10], &[9u8; 10]);
        assert!(segment[10..].iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn resume_ignores_other_timelines_and_empty_stores() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path()).await;
        assert_eq!(store.resume_point().await.unwrap(), None);
        fs::write(tmp.path().join(segment_file_name(2, 5, SEG)), b"")
            .await
            .unwrap();
        assert_eq!(store.resume_point().await.unwrap(), None);
    }

    #[tokio::test]
    async fn the_store_files_read_back_and_a_torn_flush_record_reads_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(tmp.path()).await;
        store.start_at(Lsn(SEG));
        store.write(Lsn(SEG), &[1u8; 300]).await.unwrap();
        store.flush().await.unwrap();
        assert_eq!(
            read_flushed(tmp.path()).await.unwrap(),
            Some((1, Lsn(SEG + 300)))
        );
        assert_eq!(read_system_id(tmp.path()).await.unwrap(), Some(7401));
        assert_eq!(
            first_segment(tmp.path(), 1).await.unwrap(),
            Some((Lsn(SEG), SEG))
        );
        // A crash before the set_len of a new segment leaves it empty.
        fs::write(partial_path(tmp.path(), 0), b"").await.unwrap();
        assert_eq!(
            first_segment(tmp.path(), 1).await.unwrap(),
            Some((Lsn(SEG), SEG))
        );

        let mut torn = std::fs::read(tmp.path().join(FLUSHED_FILE)).unwrap();
        torn[3] ^= 0xFF;
        std::fs::write(tmp.path().join(FLUSHED_FILE), torn).unwrap();
        assert_eq!(read_flushed(tmp.path()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_a_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(tmp.path()).await;
        store.start_at(Lsn(0));
        assert!(store.write(Lsn(8), b"x").await.is_err());
    }

    #[tokio::test]
    async fn refuses_another_clusters_directory() {
        let tmp = tempfile::tempdir().unwrap();
        drop(open_store(tmp.path()).await);
        let other = WalStore::open(tmp.path().to_path_buf(), 9999, 1, SEG).await;
        assert!(matches!(other, Err(Error::SystemMismatch { .. })));
    }

    #[tokio::test]
    async fn one_store_owns_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let first = open_store(tmp.path()).await;
        let second = WalStore::open(tmp.path().to_path_buf(), 7401, 1, SEG).await;
        assert!(matches!(second, Err(Error::Store(_))));
        drop(first);
        open_store(tmp.path()).await;
    }
}

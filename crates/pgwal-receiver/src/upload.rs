//! Moves flushed WAL from the local store to a [`WalSink`], one object per
//! LSN range, and removes local segments once they are uploaded.
//!
//! An object never crosses a segment boundary. Its key is derived from its
//! cluster and its LSN range, so two receivers that upload the same WAL write
//! consistent objects, and two clusters never share a key.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pgwal::{Lsn, RangeKey, parse_segment_file_name, segment_file_name};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::store::{
    PARTIAL_SUFFIX, WalStore, first_segment, read_flushed, read_system_id, write_durably,
};

const WATERMARK_FILE: &str = "uploaded";

/// Largest object uploaded, so that memory stays bounded with 1 GB segments.
const MAX_OBJECT: u64 = 16 << 20;

#[async_trait]
pub trait WalSink: Send + Sync {
    /// Stores one object. After a crash, the same key can arrive again with the
    /// same bytes.
    async fn put(&self, key: &str, wal: Bytes) -> io::Result<()>;

    /// The keys under the directory `dir`, such as `7401/00000001`.
    async fn list(&self, dir: &str) -> io::Result<Vec<String>>;

    /// Deletes `keys`. A key that is gone already is no error.
    async fn delete(&self, keys: &[String]) -> io::Result<()>;
}

/// The LSN up to which a previous run uploaded on `timeline`. A watermark of
/// another timeline says nothing about this one.
pub(crate) async fn read_watermark(dir: &Path, timeline: u32) -> Result<Option<Lsn>, Error> {
    let text = match fs::read_to_string(dir.join(WATERMARK_FILE)).await {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let corrupt = || Error::Store(format!("corrupt upload watermark {text:?}"));
    let (mark_timeline, lsn) = text.split_once(' ').ok_or_else(corrupt)?;
    let mark_timeline: u32 = mark_timeline.parse().map_err(|_| corrupt())?;
    let lsn: Lsn = lsn.parse().map_err(|_| corrupt())?;
    if mark_timeline != timeline {
        warn!(
            mark_timeline,
            timeline, "upload watermark belongs to another timeline"
        );
        return Ok(None);
    }
    Ok(Some(lsn))
}

/// Uploads the WAL that a receiver flushed in `dir` but did not upload, and
/// returns where the sink holds the WAL of that store. `None` if `dir` holds
/// no flushed WAL.
///
/// Call it on a stopped store before its server restarts from an image: the
/// restarted server can write other WAL at the same LSNs.
pub async fn salvage(dir: &Path, sink: Arc<dyn WalSink>) -> Result<Option<Lsn>, Error> {
    if !fs::try_exists(dir).await? {
        return Ok(None);
    }
    let (Some(system_id), Some((timeline, flushed))) =
        (read_system_id(dir).await?, read_flushed(dir).await?)
    else {
        return Ok(None);
    };
    let Some((first, seg_size)) = first_segment(dir, timeline).await? else {
        return Ok(None);
    };
    // The store lock keeps a receiver out while the WAL is read.
    let _store = WalStore::open(dir.to_path_buf(), system_id, timeline, seg_size).await?;
    let mut uploader = Uploader::new(
        dir.to_path_buf(),
        system_id,
        timeline,
        seg_size,
        sink,
        first,
    )
    .await?;
    uploader.upload_to(flushed).await?;
    uploader.finish_compaction().await;
    Ok(Some(uploader.uploaded))
}

pub(crate) struct Uploader {
    dir: PathBuf,
    system_id: u64,
    timeline: u32,
    seg_size: u64,
    sink: Arc<dyn WalSink>,
    uploaded: Lsn,
    max_object: u64,
    /// At most one compaction runs next to the uploads: it bounds the memory
    /// to one segment.
    compaction: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}

/// A compaction must not outlive its run: a replay can follow the run. An
/// abort is safe, as the whole object goes up before the parts go.
impl Drop for Uploader {
    fn drop(&mut self) {
        if let Some(task) = &self.compaction {
            task.abort();
        }
    }
}

impl Uploader {
    /// `start` is the first LSN the local store holds.
    pub async fn new(
        dir: PathBuf,
        system_id: u64,
        timeline: u32,
        seg_size: u64,
        sink: Arc<dyn WalSink>,
        start: Lsn,
    ) -> Result<Self, Error> {
        let uploaded = match read_watermark(&dir, timeline).await? {
            Some(mark) if mark < start => {
                warn!(%mark, %start, "WAL between the upload watermark and the local start is missing from the sink");
                start
            }
            Some(mark) => mark,
            None => start,
        };
        Ok(Self {
            dir,
            system_id,
            timeline,
            seg_size,
            sink,
            uploaded,
            max_object: MAX_OBJECT,
            compaction: None,
        })
    }

    /// Uploads every `every` until `flushed` closes, then uploads the rest.
    /// Returns the LSN up to which the sink holds the WAL.
    pub async fn run(
        mut self,
        mut flushed: watch::Receiver<Lsn>,
        every: Duration,
    ) -> Result<Lsn, Error> {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut failing = false;
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = closed(&mut flushed) => break,
            }
            let upto = *flushed.borrow();
            // The stream keeps going on a failed upload; the next tick retries.
            match self.upload_to(upto).await {
                Ok(()) if failing => {
                    failing = false;
                    info!(uploaded = %self.uploaded, "WAL upload recovered");
                }
                Ok(()) => {}
                Err(e) if !failing => {
                    failing = true;
                    warn!(error = %e, "WAL upload failed; retrying");
                }
                Err(e) => debug!(error = %e, "WAL upload still failing"),
            }
        }
        let upto = *flushed.borrow();
        self.upload_to(upto).await?;
        self.finish_compaction().await;
        Ok(self.uploaded)
    }

    async fn upload_to(&mut self, upto: Lsn) -> Result<(), Error> {
        let before = self.uploaded;
        while self.uploaded < upto {
            let segno = self.uploaded.segment_no(self.seg_size);
            let end = upto
                .min(Lsn((segno + 1) * self.seg_size))
                .min(Lsn(self.uploaded.0 + self.max_object));
            let wal = self.read(segno, self.uploaded, end).await?;
            let key = RangeKey {
                system_id: self.system_id,
                timeline: self.timeline,
                start: self.uploaded,
                end,
            };
            self.sink.put(&key.to_string(), wal).await?;
            self.uploaded = end;
            // Durable before any segment it covers is removed.
            let mark = format!("{} {}", self.timeline, self.uploaded);
            write_durably(&self.dir, WATERMARK_FILE, mark.as_bytes()).await?;
            if end.0 == (segno + 1) * self.seg_size {
                self.start_compaction(segno).await;
            }
        }
        if self.uploaded > before {
            self.remove_uploaded_segments().await?;
        }
        Ok(())
    }

    /// Starts to replace the objects of the finished segment `segno` by one
    /// object, so that a replay fetches one object per segment. The segment
    /// is read now, while it is still on disk; the uploads go on meanwhile.
    async fn start_compaction(&mut self, segno: u64) {
        if self.seg_size > self.max_object {
            return;
        }
        self.finish_compaction().await;
        let whole = RangeKey {
            system_id: self.system_id,
            timeline: self.timeline,
            start: Lsn(segno * self.seg_size),
            end: Lsn((segno + 1) * self.seg_size),
        };
        match self.read(segno, whole.start, whole.end).await {
            Ok(wal) => {
                self.compaction = Some(tokio::spawn(compact(self.sink.clone(), whole, wal)));
            }
            Err(e) => warn!(error = %e, segno, "WAL segment compaction skipped"),
        }
    }

    /// Waits for the compaction in flight. The uploaded WAL is safe whatever
    /// happens to it.
    async fn finish_compaction(&mut self) {
        if let Some(task) = self.compaction.take()
            && let Err(e) = task.await.expect("compaction panicked")
        {
            warn!(error = %e, "WAL segment compaction failed");
        }
    }

    /// Reads `[from, to)` of segment `segno`. The stream can rename the segment
    /// from `.partial` to its final name between two attempts.
    async fn read(&self, segno: u64, from: Lsn, to: Lsn) -> Result<Bytes, Error> {
        let name = segment_file_name(self.timeline, segno, self.seg_size);
        let finished = self.dir.join(&name);
        let partial = self.dir.join(format!("{name}{PARTIAL_SUFFIX}"));
        for path in [&finished, &partial, &finished] {
            let mut file = match File::open(path).await {
                Ok(file) => file,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            file.seek(io::SeekFrom::Start(from.segment_offset(self.seg_size)))
                .await?;
            let mut wal = vec![0; (to.0 - from.0) as usize];
            file.read_exact(&mut wal).await?;
            return Ok(wal.into());
        }
        Err(Error::Store(format!("segment {name} is not in the store")))
    }

    async fn remove_uploaded_segments(&self) -> Result<(), Error> {
        let mut entries = fs::read_dir(&self.dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let Some((timeline, segno)) = name
                .to_str()
                .and_then(|name| parse_segment_file_name(name, self.seg_size))
            else {
                continue;
            };
            if timeline == self.timeline && (segno + 1) * self.seg_size <= self.uploaded.0 {
                fs::remove_file(entry.path()).await?;
            }
        }
        Ok(())
    }
}

/// Replaces the objects inside `whole` by one object that holds `wal`. The
/// whole object goes up before the parts go: a crash between the two leaves
/// overlapping objects with the same bytes.
async fn compact(sink: Arc<dyn WalSink>, whole: RangeKey, wal: Bytes) -> Result<(), Error> {
    let dir = format!("{}/{:08X}", whole.system_id, whole.timeline);
    let parts: Vec<String> = sink
        .list(&dir)
        .await?
        .into_iter()
        .filter(|key| {
            RangeKey::parse(key).is_some_and(|r| {
                r.system_id == whole.system_id
                    && r.timeline == whole.timeline
                    && r.start >= whole.start
                    && r.end <= whole.end
                    && r != whole
            })
        })
        .collect();
    if parts.is_empty() {
        return Ok(());
    }
    sink.put(&whole.to_string(), wal).await?;
    sink.delete(&parts).await?;
    Ok(())
}

/// Resolves once the sender is dropped.
async fn closed(flushed: &mut watch::Receiver<Lsn>) {
    while flushed.changed().await.is_ok() {}
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pgwal::assemble_segment;

    use super::*;

    const SEG: u64 = 1 << 20;
    const SYSTEM_ID: u64 = 7401;

    fn key(start: u64, end: u64) -> String {
        RangeKey {
            system_id: SYSTEM_ID,
            timeline: 1,
            start: Lsn(start),
            end: Lsn(end),
        }
        .to_string()
    }

    /// Fails the first `fail_first` puts, then stores objects in memory.
    #[derive(Default)]
    struct MemSink {
        objects: Mutex<BTreeMap<String, Bytes>>,
        fail_first: AtomicUsize,
    }

    #[async_trait]
    impl WalSink for MemSink {
        async fn put(&self, key: &str, wal: Bytes) -> io::Result<()> {
            if self
                .fail_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(io::Error::other("sink down"));
            }
            self.objects.lock().unwrap().insert(key.to_owned(), wal);
            Ok(())
        }

        async fn list(&self, dir: &str) -> io::Result<Vec<String>> {
            let prefix = format!("{dir}/");
            let objects = self.objects.lock().unwrap();
            Ok(objects
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect())
        }

        async fn delete(&self, keys: &[String]) -> io::Result<()> {
            let mut objects = self.objects.lock().unwrap();
            for key in keys {
                objects.remove(key);
            }
            Ok(())
        }
    }

    impl MemSink {
        fn keys(&self) -> Vec<String> {
            self.objects.lock().unwrap().keys().cloned().collect()
        }

        fn ranges(&self) -> Vec<(Lsn, Lsn, Bytes)> {
            self.objects
                .lock()
                .unwrap()
                .iter()
                .map(|(key, wal)| {
                    let key = RangeKey::parse(key).unwrap();
                    (key.start, key.end, wal.clone())
                })
                .collect()
        }
    }

    /// A store holding 1.5 segments of patterned WAL from `SEG`, flushed.
    async fn filled_store(dir: &Path) -> (WalStore, Vec<u8>, Lsn) {
        let mut store = WalStore::open(dir.to_path_buf(), SYSTEM_ID, 1, SEG)
            .await
            .unwrap();
        store.start_at(Lsn(SEG));
        let wal: Vec<u8> = (0..SEG + SEG / 2).map(|i| (i % 251) as u8).collect();
        store.write(Lsn(SEG), &wal).await.unwrap();
        let flushed = store.flush().await.unwrap();
        (store, wal, flushed)
    }

    async fn uploader(dir: &Path, sink: &Arc<MemSink>) -> Uploader {
        Uploader::new(dir.to_path_buf(), SYSTEM_ID, 1, SEG, sink.clone(), Lsn(SEG))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn uploads_per_segment_and_removes_what_is_uploaded() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, wal, flushed) = filled_store(tmp.path()).await;
        let sink = Arc::new(MemSink::default());
        uploader(tmp.path(), &sink)
            .await
            .upload_to(flushed)
            .await
            .unwrap();

        assert_eq!(sink.keys(), [key(SEG, 2 * SEG), key(2 * SEG, flushed.0)]);
        assert_eq!(
            assemble_segment(1, SEG, &sink.ranges()).unwrap(),
            wal[..SEG as usize]
        );
        assert!(!tmp.path().join(segment_file_name(1, 1, SEG)).exists());
        assert!(
            tmp.path()
                .join(format!("{}.partial", segment_file_name(1, 2, SEG)))
                .exists()
        );
        assert_eq!(read_watermark(tmp.path(), 1).await.unwrap(), Some(flushed));
    }

    /// Several upload intervals inside one segment leave several objects.
    /// When the segment finishes, one object replaces them, a leftover part
    /// of an earlier run included.
    #[tokio::test]
    async fn a_finished_segment_is_compacted_into_one_object() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = WalStore::open(tmp.path().to_path_buf(), SYSTEM_ID, 1, SEG)
            .await
            .unwrap();
        store.start_at(Lsn(SEG));
        let wal: Vec<u8> = (0..SEG + SEG / 2).map(|i| (i % 251) as u8).collect();
        let sink = Arc::new(MemSink::default());
        sink.objects
            .lock()
            .unwrap()
            .insert(key(SEG, SEG + 100), Bytes::from(wal[..100].to_vec()));
        let mut uploader = uploader(tmp.path(), &sink).await;

        let mut at = 0usize;
        for to in [SEG / 4, SEG / 2, SEG + SEG / 2] {
            store
                .write(Lsn(SEG + at as u64), &wal[at..to as usize])
                .await
                .unwrap();
            let flushed = store.flush().await.unwrap();
            uploader.upload_to(flushed).await.unwrap();
            at = to as usize;
        }
        uploader.finish_compaction().await;

        let end = SEG + wal.len() as u64;
        assert_eq!(sink.keys(), [key(SEG, 2 * SEG), key(2 * SEG, end)]);
        assert_eq!(
            assemble_segment(1, SEG, &sink.ranges()).unwrap(),
            wal[..SEG as usize]
        );
    }

    #[tokio::test]
    async fn caps_object_size() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, wal, flushed) = filled_store(tmp.path()).await;
        let sink = Arc::new(MemSink::default());
        let mut uploader = uploader(tmp.path(), &sink).await;
        uploader.max_object = SEG / 3 + 1;
        uploader.upload_to(flushed).await.unwrap();

        let ranges = sink.ranges();
        assert_eq!(ranges.len(), 5);
        assert!(ranges.iter().all(|(s, e, _)| e.0 - s.0 <= SEG / 3 + 1));
        assert_eq!(
            assemble_segment(1, SEG, &ranges).unwrap(),
            wal[..SEG as usize]
        );
    }

    #[tokio::test]
    async fn a_failed_put_keeps_the_watermark_and_the_retry_resends_the_same_key() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, _, flushed) = filled_store(tmp.path()).await;
        let sink = Arc::new(MemSink::default());
        sink.fail_first.store(1, Ordering::SeqCst);
        let mut uploader = uploader(tmp.path(), &sink).await;

        assert!(uploader.upload_to(flushed).await.is_err());
        assert_eq!(read_watermark(tmp.path(), 1).await.unwrap(), None);
        assert!(tmp.path().join(segment_file_name(1, 1, SEG)).exists());

        uploader.upload_to(flushed).await.unwrap();
        assert_eq!(sink.keys()[0], key(SEG, 2 * SEG));
        assert_eq!(read_watermark(tmp.path(), 1).await.unwrap(), Some(flushed));
    }

    #[tokio::test]
    async fn a_watermark_of_another_timeline_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        write_durably(tmp.path(), WATERMARK_FILE, b"1 0/3000000")
            .await
            .unwrap();
        assert_eq!(read_watermark(tmp.path(), 2).await.unwrap(), None);
        assert_eq!(
            read_watermark(tmp.path(), 1).await.unwrap(),
            Some(Lsn(0x300_0000))
        );
        write_durably(tmp.path(), WATERMARK_FILE, b"garbage")
            .await
            .unwrap();
        assert!(matches!(
            read_watermark(tmp.path(), 1).await,
            Err(Error::Store(_))
        ));
    }

    #[tokio::test]
    async fn closing_the_channel_uploads_the_rest_without_waiting_for_a_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, _, flushed) = filled_store(tmp.path()).await;
        let sink = Arc::new(MemSink::default());
        let (tx, rx) = watch::channel(Lsn(SEG));
        let run = tokio::spawn(
            uploader(tmp.path(), &sink)
                .await
                .run(rx, Duration::from_secs(3600)),
        );
        tx.send_replace(flushed);
        drop(tx);
        let uploaded = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("uploader waited for a tick")
            .unwrap()
            .unwrap();
        assert_eq!(uploaded, flushed);
        assert_eq!(sink.keys().len(), 2);
    }
}

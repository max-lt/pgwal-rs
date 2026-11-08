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
use pgwal::{Lsn, RangeKey, segment_file_name};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::store::{
    MARK_LEN, PARTIAL_SUFFIX, WalStore, first_segment, read_flushed, read_mark, read_system_id,
    segment_files, sync_dir, write_mark,
};

const WATERMARK_FILE: &str = "uploaded";

/// Where the WAL goes. The receiver removes its local copy of an object once
/// `put` returns: `put` returns only once the object is durable, and `list`
/// then shows it.
#[async_trait]
pub trait WalSink: Send + Sync {
    /// Stores one object. After a crash, the same key can arrive again with the
    /// same bytes.
    async fn put(&self, key: &str, data: Bytes) -> io::Result<()>;

    /// The object at `key`, or `None` if there is none.
    async fn get(&self, key: &str) -> io::Result<Option<Bytes>>;

    /// The keys that start with `prefix`, such as `7401/00000001/`.
    async fn list(&self, prefix: &str) -> io::Result<Vec<String>>;

    /// Deletes `keys`. A key that is gone already is no error.
    async fn delete(&self, keys: &[String]) -> io::Result<()>;
}

/// The LSN up to which a previous run uploaded on `timeline`. A watermark of
/// another timeline says nothing about this one.
pub(crate) async fn read_watermark(dir: &Path, timeline: u32) -> Result<Option<Lsn>, Error> {
    // After a failover, the watermark holds the old timeline until the first
    // upload on the new one.
    Ok(read_mark(&dir.join(WATERMARK_FILE))
        .await?
        .and_then(|(mark_timeline, lsn)| (mark_timeline == timeline).then_some(lsn)))
}

/// Uploads the WAL that a receiver flushed in `dir` but did not upload, and
/// returns where the sink holds the WAL of that store. `None` if `dir` holds
/// no segment of the timeline it flushed.
///
/// Call it on a stopped store before its server restarts from an image: the
/// restarted server can write other WAL at the same LSNs.
pub async fn salvage(dir: &Path, sink: Arc<dyn WalSink>) -> Result<Option<Lsn>, Error> {
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
    pub(crate) uploaded: Lsn,
    watermark: File,
    /// Objects that go up before the WAL, such as history files.
    pub(crate) pending: Vec<(String, Bytes)>,
    max_object: u64,
    /// No WAL went up yet in this run: an earlier run can have left parts of
    /// the first segment in the sink.
    first_put: bool,
    /// At most one compaction runs next to the uploads: it bounds the memory
    /// to one segment.
    compaction: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}

/// A replay that starts after the run must not see a delete in flight. An
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
        let mut watermark = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(WATERMARK_FILE))
            .await?;
        // A mark from the start: on a full disk, the write in place of a
        // later one still passes, and frees the uploaded segments.
        if watermark.metadata().await?.len() < MARK_LEN as u64 {
            write_mark(&mut watermark, timeline, uploaded).await?;
            sync_dir(&dir).await?;
        }
        // A crash between a mark and the removal of its segment leaves it.
        for file in segment_files(&dir, seg_size).await? {
            let up = (file.segno + 1) * seg_size <= uploaded.0;
            if file.timeline == timeline && !file.partial && up {
                fs::remove_file(file.path).await?;
            }
        }
        Ok(Self {
            dir,
            system_id,
            timeline,
            seg_size,
            sink,
            uploaded,
            watermark,
            pending: Vec::new(),
            max_object: crate::MAX_OBJECT as u64,
            first_put: true,
            compaction: None,
        })
    }

    /// Uploads every `every` until `flushed` closes, then uploads the rest.
    /// Publishes each new upload end in `uploaded`, and returns the LSN up to
    /// which the sink holds the WAL.
    pub async fn run(
        mut self,
        mut flushed: watch::Receiver<Lsn>,
        every: Duration,
        uploaded: watch::Sender<Lsn>,
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
            let result = self.upload_to(upto).await;
            uploaded.send_if_modified(|lsn| {
                let moved = *lsn != self.uploaded;
                *lsn = self.uploaded;
                moved
            });
            match result {
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
        while let Some((key, data)) = self.pending.first() {
            self.sink.put(key, data.clone()).await?;
            self.pending.remove(0);
        }
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
            let first_put = std::mem::replace(&mut self.first_put, false);
            self.uploaded = end;
            // Durable before any segment it covers is removed.
            write_mark(&mut self.watermark, self.timeline, self.uploaded).await?;
            if end.0 == (segno + 1) * self.seg_size {
                // A segment that went up as one object has no parts to
                // replace.
                if key.start.0 != segno * self.seg_size || first_put {
                    self.start_compaction(segno).await;
                }
                // At once, so a long backlog frees the disk as it goes up.
                let path = self
                    .dir
                    .join(segment_file_name(self.timeline, segno, self.seg_size));
                match fs::remove_file(path).await {
                    Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
                    _ => {}
                }
            }
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
}

/// Replaces the objects inside `whole` by one object that holds `wal`. The
/// whole object goes up before the parts go: a crash between the two leaves
/// overlapping objects with the same bytes.
async fn compact(sink: Arc<dyn WalSink>, whole: RangeKey, wal: Bytes) -> Result<(), Error> {
    let prefix = RangeKey::prefix(
        whole.system_id,
        whole.timeline,
        whole.start,
        whole.end.0 - whole.start.0,
    );
    let parts: Vec<String> = sink
        .list(&prefix)
        .await?
        .into_iter()
        .filter(|key| {
            RangeKey::parse(key)
                .is_some_and(|r| r.start >= whole.start && r.end <= whole.end && r != whole)
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

    /// Fails the first `fail_first` puts and every put of `fail_key`, and
    /// stores the other objects in memory.
    #[derive(Default)]
    struct MemSink {
        objects: Mutex<BTreeMap<String, Bytes>>,
        fail_first: AtomicUsize,
        fail_key: Mutex<Option<String>>,
        lists: AtomicUsize,
    }

    #[async_trait]
    impl WalSink for MemSink {
        async fn put(&self, key: &str, wal: Bytes) -> io::Result<()> {
            if self
                .fail_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
                || self.fail_key.lock().unwrap().as_deref() == Some(key)
            {
                return Err(io::Error::other("sink down"));
            }
            self.objects.lock().unwrap().insert(key.to_owned(), wal);
            Ok(())
        }

        async fn get(&self, key: &str) -> io::Result<Option<Bytes>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            let objects = self.objects.lock().unwrap();
            Ok(objects
                .keys()
                .filter(|k| k.starts_with(prefix))
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
        assert_eq!(read_watermark(tmp.path(), 1).await.unwrap(), Some(Lsn(SEG)));
        assert!(tmp.path().join(segment_file_name(1, 1, SEG)).exists());

        uploader.upload_to(flushed).await.unwrap();
        assert_eq!(sink.keys()[0], key(SEG, 2 * SEG));
        assert_eq!(read_watermark(tmp.path(), 1).await.unwrap(), Some(flushed));
    }

    /// A segment that goes up as one object has no part to compact, except
    /// the first one of a run: an earlier run can have left parts of it.
    #[tokio::test]
    async fn only_a_segment_with_parts_is_compacted() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = WalStore::open(tmp.path().to_path_buf(), SYSTEM_ID, 1, SEG)
            .await
            .unwrap();
        store.start_at(Lsn(SEG));
        store
            .write(Lsn(SEG), &vec![3; 3 * SEG as usize])
            .await
            .unwrap();
        let flushed = store.flush().await.unwrap();
        let sink = Arc::new(MemSink::default());
        let mut uploader = uploader(tmp.path(), &sink).await;
        uploader.upload_to(flushed).await.unwrap();
        uploader.finish_compaction().await;
        assert_eq!(sink.keys().len(), 3);
        assert_eq!(sink.lists.load(Ordering::SeqCst), 1);
    }

    /// A long backlog frees each segment once it is up: the disk need not
    /// hold the whole backlog until its end.
    #[tokio::test]
    async fn a_segment_goes_once_it_is_up_even_when_a_later_put_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, _, flushed) = filled_store(tmp.path()).await;
        let sink = Arc::new(MemSink::default());
        *sink.fail_key.lock().unwrap() = Some(key(2 * SEG, flushed.0));
        let mut uploader = uploader(tmp.path(), &sink).await;

        assert!(uploader.upload_to(flushed).await.is_err());
        assert_eq!(
            read_watermark(tmp.path(), 1).await.unwrap(),
            Some(Lsn(2 * SEG))
        );
        assert!(!tmp.path().join(segment_file_name(1, 1, SEG)).exists());
    }

    #[tokio::test]
    async fn a_watermark_of_another_timeline_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(WATERMARK_FILE);
        let mut file = File::create(&path).await.unwrap();
        write_mark(&mut file, 1, Lsn(0x300_0000)).await.unwrap();
        assert_eq!(read_watermark(tmp.path(), 2).await.unwrap(), None);
        assert_eq!(
            read_watermark(tmp.path(), 1).await.unwrap(),
            Some(Lsn(0x300_0000))
        );
        std::fs::write(&path, b"a torn mark of 20 b").unwrap();
        assert_eq!(read_watermark(tmp.path(), 1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn closing_the_channel_uploads_the_rest_without_waiting_for_a_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, _, flushed) = filled_store(tmp.path()).await;
        let sink = Arc::new(MemSink::default());
        let (tx, rx) = watch::channel(Lsn(SEG));
        let run = tokio::spawn(uploader(tmp.path(), &sink).await.run(
            rx,
            Duration::from_secs(3600),
            watch::channel(Lsn(0)).0,
        ));
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

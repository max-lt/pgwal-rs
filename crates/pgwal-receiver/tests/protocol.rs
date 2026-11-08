//! The receiver against a scripted walsender over an in-memory stream: resume,
//! timeouts, hang-ups and hostile input, without a Postgres server.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use pgwal::assemble_segment;
use pgwal_receiver::{
    Confirm, ConnConfig, Error, Lsn, RangeKey, StreamConfig, WalSink, receive, salvage,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const SEG: u64 = 1 << 20;
const SLOT: &str = "pgwal_test";
const CHUNK: usize = 128 << 10;
/// Timeline 2 begins inside segment 3.
const HISTORY: &str = "1\t0/300A00\tno recovery target specified\n";

#[derive(Default)]
struct MemSink(Mutex<BTreeMap<String, Bytes>>);

#[async_trait]
impl WalSink for MemSink {
    async fn put(&self, key: &str, wal: Bytes) -> io::Result<()> {
        self.0.lock().unwrap().insert(key.to_owned(), wal);
        Ok(())
    }

    async fn get(&self, key: &str) -> io::Result<Option<Bytes>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let objects = self.0.lock().unwrap();
        Ok(objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn delete(&self, keys: &[String]) -> io::Result<()> {
        let mut objects = self.0.lock().unwrap();
        for key in keys {
            objects.remove(key);
        }
        Ok(())
    }
}

impl MemSink {
    fn ranges(&self) -> Vec<(Lsn, Lsn, Bytes)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| !key.ends_with(".history"))
            .map(|(key, wal)| {
                let k = RangeKey::parse(key).unwrap();
                assert_eq!(k.system_id, 7401, "{key}");
                assert_eq!(wal.len() as u64, k.end.0 - k.start.0, "{key}");
                (k.start, k.end, wal.clone())
            })
            .collect()
    }

    /// Asserts that the objects of `timeline` cover `[from, to)` with no
    /// gap. They can overlap.
    fn assert_covers(&self, timeline: u32, from: Lsn, to: Lsn) {
        let mut ranges: Vec<RangeKey> = self
            .0
            .lock()
            .unwrap()
            .keys()
            .filter_map(|key| RangeKey::parse(key))
            .filter(|range| range.timeline == timeline)
            .collect();
        ranges.sort();
        let mut at = from;
        for range in ranges {
            assert!(range.start <= at, "a gap on timeline {timeline} at {at}");
            at = at.max(range.end);
        }
        assert_eq!(at, to, "timeline {timeline}");
    }

    /// Asserts that the objects tile `[from, to)` exactly: no gap, no overlap.
    fn assert_tiles(&self, from: Lsn, to: Lsn) {
        let mut at = from;
        for (start, end, _) in self.ranges() {
            assert_eq!(start, at, "gap or overlap at {at}");
            at = end;
        }
        assert_eq!(at, to);
    }
}

/// A [`MemSink`] that refuses writes until it is opened.
#[derive(Default)]
struct GateSink {
    open: std::sync::atomic::AtomicBool,
    inner: MemSink,
}

#[async_trait]
impl WalSink for GateSink {
    async fn put(&self, key: &str, wal: Bytes) -> io::Result<()> {
        if !self.open.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::other("object store down"));
        }
        self.inner.put(key, wal).await
    }

    async fn get(&self, key: &str) -> io::Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, keys: &[String]) -> io::Result<()> {
        self.inner.delete(keys).await
    }
}

fn config(dir: &Path) -> StreamConfig {
    StreamConfig {
        conn: ConnConfig {
            user: "postgres".into(),
            password: None,
            application_name: "pgwal".into(),
            idle_timeout: Duration::from_secs(5),
        },
        slot: SLOT.into(),
        dir: dir.to_path_buf(),
        upload_every: Duration::from_millis(20),
        confirm: Confirm::Flushed,
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8 ^ seed).collect()
}

/// Starts a receiver and returns the server end of its connection.
fn spawn_receiver(
    cfg: StreamConfig,
    sink: &Arc<MemSink>,
) -> (JoinHandle<Result<Lsn, Error>>, Walsender) {
    let (task, server, _stop) = spawn_stoppable(cfg, sink);
    (task, server)
}

/// As [`spawn_receiver`], plus the sender that stops the receiver. Dropping the
/// sender does not stop it.
fn spawn_stoppable(
    cfg: StreamConfig,
    sink: &Arc<MemSink>,
) -> (
    JoinHandle<Result<Lsn, Error>>,
    Walsender,
    oneshot::Sender<Option<Lsn>>,
) {
    let (client, server) = tokio::io::duplex(4 << 20);
    let sink: Arc<dyn WalSink> = sink.clone();
    let (stop_tx, stop_rx) = oneshot::channel();
    let stop = async move {
        match stop_rx.await {
            Ok(at) => at,
            Err(_) => std::future::pending().await,
        }
    };
    let task = tokio::spawn(async move { receive(client, &cfg, sink, stop).await });
    (
        task,
        Walsender {
            io: server,
            buf: BytesMut::new(),
        },
        stop_tx,
    )
}

async fn finish(task: JoinHandle<Result<Lsn, Error>>) -> Result<Lsn, Error> {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("receiver did not return")
        .unwrap()
}

/// The server side of a replication connection.
struct Walsender {
    io: DuplexStream,
    buf: BytesMut,
}

impl Walsender {
    async fn fill(&mut self) {
        let n = self.io.read_buf(&mut self.buf).await.unwrap();
        assert!(n > 0, "receiver closed the connection");
    }

    async fn read_startup(&mut self) -> BTreeMap<String, String> {
        while self.buf.len() < 4 {
            self.fill().await;
        }
        let len = u32::from_be_bytes(self.buf[..4].try_into().unwrap()) as usize;
        while self.buf.len() < len {
            self.fill().await;
        }
        let mut msg = self.buf.split_to(len);
        msg.advance(8);
        let mut params = BTreeMap::new();
        let mut fields = msg[..]
            .split(|&b| b == 0)
            .map(|f| String::from_utf8(f.to_vec()).unwrap());
        while let (Some(key), Some(value)) = (fields.next(), fields.next()) {
            if key.is_empty() {
                break;
            }
            params.insert(key, value);
        }
        params
    }

    async fn read(&mut self) -> (u8, BytesMut) {
        loop {
            if self.buf.len() >= 5 {
                let len = u32::from_be_bytes(self.buf[1..5].try_into().unwrap()) as usize;
                if self.buf.len() > len {
                    let mut msg = self.buf.split_to(1 + len);
                    let tag = msg[0];
                    msg.advance(5);
                    return (tag, msg);
                }
            }
            self.fill().await;
        }
    }

    async fn expect_query(&mut self) -> String {
        let (tag, body) = self.read().await;
        assert_eq!(tag, b'Q');
        String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut out = BytesMut::new();
        out.put_u8(tag);
        out.put_u32(4 + body.len() as u32);
        out.put_slice(body);
        self.io.write_all(&out).await.unwrap();
    }

    async fn ready(&mut self) {
        self.send(b'Z', b"I").await;
    }

    async fn row(&mut self, values: &[Option<&str>]) {
        // The receiver does not read the column names.
        self.send(b'T', &0i16.to_be_bytes()).await;
        let mut body = BytesMut::new();
        body.put_i16(values.len() as i16);
        for value in values {
            match value {
                Some(v) => {
                    body.put_i32(v.len() as i32);
                    body.put_slice(v.as_bytes());
                }
                None => body.put_i32(-1),
            }
        }
        self.send(b'D', &body).await;
        self.send(b'C', b"SELECT\0").await;
        self.ready().await;
    }

    async fn error(&mut self, code: &str, message: &str) {
        let body = format!("SERROR\0C{code}\0M{message}\0\0");
        self.send(b'E', body.as_bytes()).await;
    }

    /// Answers the handshake up to the slot creation, on timeline 1.
    async fn open(&mut self) {
        self.open_on(1).await;
    }

    async fn open_on(&mut self, timeline: u32) {
        self.open_with(timeline, HISTORY).await;
    }

    /// Answers the handshake on `timeline`, whose history file is `history`.
    async fn open_with(&mut self, timeline: u32, history: &str) {
        let params = self.read_startup().await;
        assert_eq!(params["replication"], "true");
        assert_eq!(params["application_name"], "pgwal");
        self.send(b'R', &0i32.to_be_bytes()).await;
        self.ready().await;
        assert_eq!(self.expect_query().await, "IDENTIFY_SYSTEM");
        self.row(&[
            Some("7401"),
            Some(&timeline.to_string()),
            Some("0/2000000"),
            None,
        ])
        .await;
        assert_eq!(self.expect_query().await, "SHOW wal_segment_size");
        self.row(&[Some("1MB")]).await;
        let create = self.expect_query().await;
        assert_eq!(
            create,
            format!("CREATE_REPLICATION_SLOT {SLOT} PHYSICAL RESERVE_WAL")
        );
        self.error("42710", "replication slot already exists").await;
        self.ready().await;
        if timeline > 1 {
            assert_eq!(
                self.expect_query().await,
                format!("TIMELINE_HISTORY {timeline}")
            );
            self.history(timeline, history).await;
        }
    }

    /// Answers a TIMELINE_HISTORY query.
    async fn history(&mut self, timeline: u32, history: &str) {
        self.row(&[Some(&format!("{timeline:08X}.history")), Some(history)])
            .await;
    }

    /// Ends the stream of an old timeline at its switch point.
    async fn copy_done(&mut self) {
        self.send(b'c', &[]).await;
    }

    /// Answers the rest of the handshake and returns the START_REPLICATION
    /// query. `restart` is the slot position, `SEG` by default.
    async fn start(&mut self, restart: Option<Lsn>) -> String {
        self.slot(restart.unwrap_or(Lsn(SEG))).await;
        let query = self.expect_query().await;
        assert!(query.starts_with("START_REPLICATION"), "{query}");
        self.send(b'W', &[0, 0, 0]).await;
        query
    }

    /// Answers READ_REPLICATION_SLOT with the restart position `restart`.
    async fn slot(&mut self, restart: Lsn) {
        assert_eq!(
            self.expect_query().await,
            format!("READ_REPLICATION_SLOT {SLOT}")
        );
        self.row(&[Some("physical"), Some(&restart.to_string()), Some("1")])
            .await;
    }

    async fn xlogdata(&mut self, start: Lsn, wal: &[u8], wal_end: Lsn) {
        let mut body = BytesMut::new();
        body.put_u8(b'w');
        body.put_u64(start.0);
        body.put_u64(wal_end.0);
        body.put_u64(0);
        body.put_slice(wal);
        self.send(b'd', &body).await;
    }

    /// Sends `wal` from `start` and announces its end as the server's WAL end.
    async fn stream(&mut self, start: Lsn, wal: &[u8]) -> Lsn {
        let end = Lsn(start.0 + wal.len() as u64);
        self.stream_announcing(start, wal, end).await;
        end
    }

    async fn stream_announcing(&mut self, start: Lsn, wal: &[u8], wal_end: Lsn) {
        for (i, chunk) in wal.chunks(CHUNK).enumerate() {
            self.xlogdata(Lsn(start.0 + (i * CHUNK) as u64), chunk, wal_end)
                .await;
        }
    }

    async fn keepalive(&mut self, wal_end: Lsn, reply_requested: bool) {
        let mut body = BytesMut::new();
        body.put_u8(b'k');
        body.put_u64(wal_end.0);
        body.put_u64(0);
        body.put_u8(reply_requested.into());
        self.send(b'd', &body).await;
    }

    /// Reads the next status update: `(flushed, reply_requested)`.
    async fn status(&mut self) -> (Lsn, bool) {
        loop {
            let (tag, mut body) = self.read().await;
            if tag != b'd' {
                continue;
            }
            assert_eq!(body.get_u8(), b'r');
            let written = body.get_u64();
            let flushed = body.get_u64();
            assert!(flushed <= written);
            assert_eq!(body.get_u64(), 0, "a receiver never reports apply");
            body.get_i64();
            return (Lsn(flushed), body.get_u8() == 1);
        }
    }

    /// Reads status updates until one confirms the flush of `lsn`.
    async fn await_flush(&mut self, lsn: Lsn) {
        while self.status().await.0 < lsn {}
    }

    /// Reads status updates until one asks for a reply.
    async fn await_reply_request(&mut self) {
        while !self.status().await.1 {}
    }

    /// Ends the stream the way a walsender does at shutdown.
    async fn end(&mut self) {
        self.send(b'C', b"COPY 0\0").await;
    }
}

#[tokio::test]
async fn streams_across_segments_confirms_the_flush_and_uploads() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);

    server.open().await;
    let query = server.start(Some(Lsn(SEG + 0x1234))).await;
    assert_eq!(
        query,
        format!("START_REPLICATION SLOT {SLOT} PHYSICAL 0/100000 TIMELINE 1"),
        "starts at the segment that holds the slot position"
    );
    let wal = pattern((SEG + SEG / 2) as usize, 0);
    let end = server.stream(Lsn(SEG), &wal).await;
    server.keepalive(end, true).await;
    server.await_flush(end).await;
    server.end().await;

    assert_eq!(finish(task).await.unwrap(), end);
    sink.assert_tiles(Lsn(SEG), end);
    let ranges = sink.ranges();
    assert_eq!(
        assemble_segment(1, SEG, &ranges).unwrap(),
        wal[..SEG as usize]
    );
    assert_eq!(
        assemble_segment(2, SEG, &ranges).unwrap()[..(SEG / 2) as usize],
        wal[SEG as usize..]
    );
}

#[tokio::test]
async fn confirm_uploaded_holds_the_flush_until_the_sink_has_the_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(GateSink::default());
    let cfg = StreamConfig {
        confirm: Confirm::Uploaded,
        ..config(tmp.path())
    };
    let (client, server) = tokio::io::duplex(4 << 20);
    let dyn_sink: Arc<dyn WalSink> = sink.clone();
    let task =
        tokio::spawn(async move { receive(client, &cfg, dyn_sink, std::future::pending()).await });
    let mut server = Walsender {
        io: server,
        buf: BytesMut::new(),
    };
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(0x40000, 5)).await;
    server.keepalive(end, true).await;
    // The WAL is fsynced before this status, but not uploaded.
    assert_eq!(server.status().await.0, Lsn(SEG));

    sink.open.store(true, std::sync::atomic::Ordering::SeqCst);
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);
    sink.inner.assert_tiles(Lsn(SEG), end);
}

#[tokio::test]
async fn resumes_at_the_unfinished_segment_without_uploading_twice() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let wal = pattern(0xC0000, 3);

    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let first = server.stream(Lsn(SEG), &wal[..0x80000]).await;
    server.await_flush(first).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), first);

    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    let query = server.start(None).await;
    assert!(query.contains("PHYSICAL 0/100000 "), "{query}");
    let end = server.stream(Lsn(SEG), &wal).await;
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);

    sink.assert_tiles(Lsn(SEG), end);
    assert_eq!(
        assemble_segment(1, SEG, &sink.ranges()).unwrap()[..wal.len()],
        wal[..]
    );
}

/// At shutdown the walsender ends the stream as soon as it has our flush
/// confirmation. A status we send after that finds the socket closed; the
/// stream still ended cleanly.
#[tokio::test]
async fn a_status_the_closed_server_never_reads_does_not_fail_a_clean_end() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(4096, 0)).await;
    server.keepalive(end, true).await;
    server.end().await;
    drop(server);

    assert_eq!(finish(task).await.unwrap(), end);
    sink.assert_tiles(Lsn(SEG), end);
}

#[tokio::test]
async fn a_stop_on_a_caught_up_server_ends_at_its_position() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server, stop) = spawn_stoppable(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;

    stop.send(None).unwrap();
    server.await_reply_request().await;
    server.keepalive(end, false).await;

    assert_eq!(finish(task).await.unwrap(), end);
    let tag = loop {
        match server.read().await {
            (b'd', _) => continue,
            (tag, _) => break tag,
        }
    };
    assert_eq!(
        tag, b'c',
        "after its last status, the receiver ends the copy"
    );
    sink.assert_tiles(Lsn(SEG), end);
}

/// WAL that ends where the server announces its end gets its flush at once,
/// as a synchronous commit waits for it. WAL behind that end waits for more,
/// up to MAX_FLUSH_DELAY.
#[tokio::test]
async fn the_flush_comes_at_once_when_caught_up_and_late_when_behind() {
    const MAX_FLUSH_DELAY: Duration = Duration::from_millis(100);
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let wal = pattern(8192, 7);
    let (half, end) = (Lsn(SEG + 4096), Lsn(SEG + 8192));

    let sent = Instant::now();
    server.stream_announcing(Lsn(SEG), &wal[..4096], end).await;
    server.await_flush(half).await;
    assert!(
        sent.elapsed() >= MAX_FLUSH_DELAY,
        "a flush behind the server end"
    );

    let sent = Instant::now();
    server.stream(half, &wal[4096..]).await;
    server.await_flush(end).await;
    assert!(sent.elapsed() < MAX_FLUSH_DELAY, "no flush when caught up");

    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);
}

/// The server answers a stop with what it has sent, short of what it had
/// flushed before the stop. The caller gives that flush position, and the
/// stream reads up to it.
#[tokio::test]
async fn a_stop_with_a_flush_position_reads_up_to_it() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server, stop) = spawn_stoppable(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let wal = pattern(8192, 6);
    let (sent, flushed) = (Lsn(SEG + 4096), Lsn(SEG + 8192));
    server.stream_announcing(Lsn(SEG), &wal[..4096], sent).await;
    server.await_flush(sent).await;

    stop.send(Some(flushed)).unwrap();
    server.await_reply_request().await;
    server.keepalive(sent, false).await;
    server.stream_announcing(sent, &wal[4096..], flushed).await;

    assert_eq!(finish(task).await.unwrap(), flushed);
    sink.assert_tiles(Lsn(SEG), flushed);
}

/// A walsender that lags answers with what it has sent, below the WAL end
/// its XLogData announced. The receiver keeps reading until that end.
#[tokio::test]
async fn a_stop_on_a_lagging_server_waits_for_the_announced_end() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server, stop) = spawn_stoppable(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let wal = pattern(8192, 5);
    let (sent, end) = (Lsn(SEG + 4096), Lsn(SEG + 8192));
    server.stream_announcing(Lsn(SEG), &wal[..4096], end).await;
    server.await_flush(sent).await;

    stop.send(None).unwrap();
    server.await_reply_request().await;
    server.keepalive(sent, false).await;
    server.stream_announcing(sent, &wal[4096..], end).await;

    assert_eq!(finish(task).await.unwrap(), end);
    sink.assert_tiles(Lsn(SEG), end);
    assert_eq!(
        assemble_segment(1, SEG, &sink.ranges()).unwrap()[..wal.len()],
        wal[..]
    );
}

/// A server under load announces a later WAL end in each XLogData. The
/// receiver stops at the end announced before the answer, or a stop under a
/// steady load never ends.
#[tokio::test]
async fn a_stop_ignores_the_wal_end_announced_after_the_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server, stop) = spawn_stoppable(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let wal = pattern(8192, 3);
    let (sent, end) = (Lsn(SEG + 4096), Lsn(SEG + 8192));
    server.stream_announcing(Lsn(SEG), &wal[..4096], end).await;
    server.await_flush(sent).await;

    stop.send(None).unwrap();
    server.await_reply_request().await;
    server.keepalive(sent, false).await;
    server
        .stream_announcing(sent, &wal[4096..], Lsn(SEG + 65536))
        .await;

    assert_eq!(finish(task).await.unwrap(), end);
    sink.assert_tiles(Lsn(SEG), end);
}

/// With nothing local on a later timeline, the stream starts at the segment
/// of the switch point, not at the slot, and the history file reaches the
/// sink.
#[tokio::test]
async fn a_later_timeline_starts_at_its_switch_point_and_archives_its_history() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    let query = server.start(None).await;
    assert_eq!(
        query,
        format!("START_REPLICATION SLOT {SLOT} PHYSICAL 0/300000 TIMELINE 2")
    );
    let end = server.stream(Lsn(3 * SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server.end().await;

    assert_eq!(finish(task).await.unwrap(), end);
    let objects = sink.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY.as_bytes());
    sink.assert_tiles(Lsn(3 * SEG), end);
}

/// The sink is down while a segment finishes, and the run dies. The next
/// run streams from the unfinished segment, and still uploads the finished
/// one before it.
#[tokio::test]
async fn a_restart_uploads_the_finished_segments_that_a_dead_run_kept() {
    let tmp = tempfile::tempdir().unwrap();
    let (client, server) = tokio::io::duplex(4 << 20);
    let mut server = Walsender {
        io: server,
        buf: BytesMut::new(),
    };
    let cfg = config(tmp.path());
    let task = tokio::spawn(async move {
        receive(
            client,
            &cfg,
            Arc::new(GateSink::default()),
            std::future::pending(),
        )
        .await
    });
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let wal = pattern((SEG + SEG / 2) as usize, 7);
    let end = server.stream(Lsn(SEG), &wal).await;
    server.await_flush(end).await;
    drop(server);
    assert!(finish(task).await.is_err(), "the run dies");

    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    let query = server.start(None).await;
    assert!(query.contains("PHYSICAL 0/200000 "), "{query}");
    let end = server.stream(Lsn(2 * SEG), &wal[SEG as usize..]).await;
    server.await_flush(end).await;
    server.end().await;

    assert_eq!(finish(task).await.unwrap(), end);
    sink.assert_tiles(Lsn(SEG), end);
}

/// A failover while the sink is down and the receiver lags behind the new
/// server. The next run streams the old timeline up to its end from the new
/// server, and the run after it uploads that WAL, removes its files, and
/// streams the new timeline.
#[tokio::test]
async fn a_failover_archives_the_old_timeline_up_to_its_end() {
    let tmp = tempfile::tempdir().unwrap();
    let (client, server) = tokio::io::duplex(4 << 20);
    let mut server = Walsender {
        io: server,
        buf: BytesMut::new(),
    };
    let cfg = config(tmp.path());
    let task = tokio::spawn(async move {
        receive(
            client,
            &cfg,
            Arc::new(GateSink::default()),
            std::future::pending(),
        )
        .await
    });
    let switch = Lsn(0x300A00);
    let old_wal = pattern((switch.0 - SEG) as usize, 3);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let lagged = server.stream(Lsn(SEG), &old_wal[..4096]).await;
    server.await_flush(lagged).await;
    drop(server);
    assert!(finish(task).await.is_err(), "the run dies");

    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    let query = server.start(None).await;
    assert!(query.ends_with("PHYSICAL 0/100000 TIMELINE 1"), "{query}");
    let old_end = server.stream(Lsn(SEG), &old_wal).await;
    assert_eq!(old_end, switch);
    server.await_flush(old_end).await;
    server.copy_done().await;
    assert_eq!(finish(task).await.unwrap(), switch);

    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    let query = server.start(None).await;
    assert!(query.ends_with("PHYSICAL 0/300000 TIMELINE 2"), "{query}");
    let end = server.stream(Lsn(3 * SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);

    for (timeline, from, to) in [(1, Lsn(SEG), switch), (2, Lsn(3 * SEG), end)] {
        sink.assert_covers(timeline, from, to);
    }
    let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("00000001"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

/// Two failovers, 1 to 2 to 3, while the receiver is down. The runs stream
/// each timeline to its end, and every history file reaches the sink.
#[tokio::test]
async fn a_chain_of_failovers_archives_every_timeline_and_history() {
    const HISTORY_2: &str = "1\t0/300A00\tno recovery target specified\n";
    const HISTORY_3: &str = "1\t0/300A00\tno recovery target specified\n\
                             2\t0/500A00\tno recovery target specified\n";
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let lagged = server.stream(Lsn(SEG), &pattern(4096, 1)).await;
    server.await_flush(lagged).await;
    server.end().await;
    finish(task).await.unwrap();

    // Each run: the timeline it streams, where, the end, and the history
    // queries it makes before START_REPLICATION.
    let runs: [(u32, Lsn, Lsn, &[u32]); 3] = [
        (1, Lsn(SEG), Lsn(0x300A00), &[2]),
        (2, Lsn(3 * SEG), Lsn(0x500A00), &[2]),
        (3, Lsn(5 * SEG), Lsn(5 * SEG + 4096), &[2]),
    ];
    for (timeline, from, to, queries) in runs {
        let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
        server.open_with(3, HISTORY_3).await;
        for queried in queries {
            assert_eq!(
                server.expect_query().await,
                format!("TIMELINE_HISTORY {queried}")
            );
            server.history(*queried, HISTORY_2).await;
        }
        let query = server.start(None).await;
        assert!(
            query.ends_with(&format!("PHYSICAL {from} TIMELINE {timeline}")),
            "{query}"
        );
        let end = server
            .stream(from, &pattern((to.0 - from.0) as usize, timeline as u8))
            .await;
        server.await_flush(end).await;
        if timeline < 3 {
            server.copy_done().await;
        } else {
            server.end().await;
        }
        assert_eq!(finish(task).await.unwrap(), to);
    }

    let objects = sink.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY_2.as_bytes());
    assert_eq!(&objects["7401/00000003.history"][..], HISTORY_3.as_bytes());
    for (timeline, from, to) in [
        (1, Lsn(SEG), Lsn(0x300A00)),
        (2, Lsn(3 * SEG), Lsn(0x500A00)),
        (3, Lsn(5 * SEG), Lsn(5 * SEG + 4096)),
    ] {
        sink.assert_covers(timeline, from, to);
    }
}

/// The sink is down on a later timeline. The history file waits for the
/// uploader, and the stream goes on: the server sees the flush.
#[tokio::test]
async fn a_sink_outage_on_a_later_timeline_does_not_stop_the_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(GateSink::default());
    let (client, server) = tokio::io::duplex(4 << 20);
    let mut server = Walsender {
        io: server,
        buf: BytesMut::new(),
    };
    let cfg = config(tmp.path());
    let dyn_sink: Arc<dyn WalSink> = sink.clone();
    let task =
        tokio::spawn(async move { receive(client, &cfg, dyn_sink, std::future::pending()).await });
    server.open_on(2).await;
    server.start(None).await;
    let end = server.stream(Lsn(3 * SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    sink.open.store(true, std::sync::atomic::Ordering::SeqCst);
    server.end().await;

    assert_eq!(finish(task).await.unwrap(), end);
    let objects = sink.inner.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY.as_bytes());
}

/// The sink is down while the receiver streams timeline 2 past the switch
/// point of timeline 3, and the run dies. The next run, on timeline 3,
/// salvages timeline 2 and archives its history file too.
#[tokio::test]
async fn a_second_failover_after_a_sink_outage_archives_the_parent_history() {
    const HISTORY_3: &str = "1\t0/300A00\tno recovery target specified\n\
                             2\t0/500A00\tno recovery target specified\n";
    let tmp = tempfile::tempdir().unwrap();
    let (client, server) = tokio::io::duplex(4 << 20);
    let mut server = Walsender {
        io: server,
        buf: BytesMut::new(),
    };
    let cfg = config(tmp.path());
    let task = tokio::spawn(async move {
        receive(
            client,
            &cfg,
            Arc::new(GateSink::default()),
            std::future::pending(),
        )
        .await
    });
    server.open_on(2).await;
    server.start(None).await;
    let end = server
        .stream(Lsn(3 * SEG), &pattern((0x501000 - 3 * SEG) as usize, 2))
        .await;
    server.await_flush(end).await;
    drop(server);
    assert!(finish(task).await.is_err(), "the run dies");

    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_with(3, HISTORY_3).await;
    assert_eq!(server.expect_query().await, "TIMELINE_HISTORY 2");
    server.history(2, HISTORY).await;
    assert!(
        server
            .start(None)
            .await
            .ends_with("PHYSICAL 0/500000 TIMELINE 3")
    );
    let end = server.stream(Lsn(5 * SEG), &pattern(4096, 3)).await;
    server.await_flush(end).await;
    server.end().await;
    finish(task).await.unwrap();

    let objects = sink.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY.as_bytes());
}

/// A new host with an empty store, after a failover that took the old host
/// down too. The sink ends early on the parent timeline: the run streams
/// the rest of it from the new server.
#[tokio::test]
async fn an_empty_store_streams_the_tail_of_the_parent_timeline() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let held = RangeKey {
        system_id: 7401,
        timeline: 1,
        start: Lsn(SEG),
        end: Lsn(SEG + 4096),
    };
    let old_wal = pattern((0x300A00 - SEG) as usize, 1);
    sink.0
        .lock()
        .unwrap()
        .insert(held.to_string(), Bytes::copy_from_slice(&old_wal[..4096]));
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    assert!(
        server
            .start(None)
            .await
            .ends_with("PHYSICAL 0/100000 TIMELINE 1")
    );
    let end = server.stream(Lsn(SEG), &old_wal).await;
    server.await_flush(end).await;
    server.copy_done().await;
    assert_eq!(finish(task).await.unwrap(), Lsn(0x300A00));

    let objects = sink.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY.as_bytes());
    let reached = objects
        .keys()
        .filter_map(|key| RangeKey::parse(key))
        .filter(|range| range.timeline == 1)
        .map(|range| range.end)
        .max();
    assert_eq!(reached, Some(Lsn(0x300A00)));
}

/// An empty store, and the server no longer has the rest of the parent
/// timeline after the end of the sink. The archive has a gap there, and the
/// stream follows the timeline of the server on the same connection.
#[tokio::test]
async fn a_gone_parent_tail_falls_back_to_the_timeline_of_the_server() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let held = RangeKey {
        system_id: 7401,
        timeline: 1,
        start: Lsn(SEG),
        end: Lsn(SEG + 4096),
    };
    sink.0
        .lock()
        .unwrap()
        .insert(held.to_string(), Bytes::from(pattern(4096, 1)));
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    assert!(
        server
            .start(None)
            .await
            .ends_with("PHYSICAL 0/100000 TIMELINE 1")
    );
    server
        .error("58P01", "requested WAL segment has already been removed")
        .await;
    server.ready().await;
    let query = server.expect_query().await;
    assert!(query.ends_with("PHYSICAL 0/300000 TIMELINE 2"), "{query}");
    server.send(b'W', &[0, 0, 0]).await;
    let end = server.stream(Lsn(3 * SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);
}

/// An empty store, as on a new host, and a sink that holds the timeline of
/// the server already: the run starts after what the sink holds.
#[tokio::test]
async fn an_empty_store_starts_after_what_the_sink_holds() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    for (timeline, start, end) in [(1, SEG, 0x300A00), (2, 3 * SEG, 5 * SEG)] {
        let key = RangeKey {
            system_id: 7401,
            timeline,
            start: Lsn(start),
            end: Lsn(end),
        };
        sink.0.lock().unwrap().insert(
            key.to_string(),
            Bytes::from(vec![0; (end - start) as usize]),
        );
    }
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    assert!(
        server
            .start(None)
            .await
            .ends_with("PHYSICAL 0/500000 TIMELINE 2")
    );
    let end = server.stream(Lsn(5 * SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);
}

/// A store on timeline 1 past the switch point, after a failover. The new
/// server recycled the first segment of timeline 2 since its promotion: the
/// stream starts at the slot, not with WalGone.
#[tokio::test]
async fn a_store_past_the_switch_falls_back_to_the_slot_of_the_new_server() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server
        .stream(Lsn(SEG), &pattern((0x301000 - SEG) as usize, 1))
        .await;
    server.await_flush(end).await;
    server.end().await;
    finish(task).await.unwrap();

    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    assert!(
        server
            .start(Some(Lsn(0x500123)))
            .await
            .ends_with("PHYSICAL 0/300000 TIMELINE 2")
    );
    server
        .error("58P01", "requested WAL segment has already been removed")
        .await;
    server.ready().await;
    assert!(
        server
            .expect_query()
            .await
            .ends_with("PHYSICAL 0/500000 TIMELINE 2")
    );
    server.send(b'W', &[0, 0, 0]).await;
    let end = server.stream(Lsn(5 * SEG), &pattern(4096, 2)).await;
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);
}

/// Postgres 16 and 17 stream from an invalidated slot, which keeps no WAL
/// and has no restart position. The receiver refuses it.
#[tokio::test]
async fn a_slot_with_no_restart_position_is_wal_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    assert_eq!(
        server.expect_query().await,
        format!("READ_REPLICATION_SLOT {SLOT}")
    );
    server.row(&[Some("physical"), None, None]).await;

    let err = finish(task).await.unwrap_err();
    assert!(matches!(err, Error::WalGone { .. }), "{err}");
}

/// A restart with a store, on Postgres 16 or 17, from a slot that was
/// invalidated meanwhile: the receiver refuses it too.
#[tokio::test]
async fn a_restart_on_a_slot_with_no_restart_position_is_wal_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server.end().await;
    finish(task).await.unwrap();

    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    assert_eq!(
        server.expect_query().await,
        format!("READ_REPLICATION_SLOT {SLOT}")
    );
    server.row(&[Some("physical"), None, None]).await;
    let err = finish(task).await.unwrap_err();
    assert!(matches!(err, Error::WalGone { .. }), "{err}");
}

/// The server ends the copy of a later timeline before its first message.
/// The history file still goes up.
#[tokio::test]
async fn an_end_before_the_first_message_still_archives_the_history() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    server.start(None).await;
    server.end().await;
    finish(task).await.unwrap();
    let objects = sink.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY.as_bytes());
}

/// The first segment of a later timeline was removed. The server says so
/// inside the copy, then waits for a command: the stream starts again at
/// the slot on the same connection.
#[tokio::test]
async fn a_removed_first_segment_inside_the_copy_falls_back_to_the_slot() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    assert!(
        server
            .start(Some(Lsn(0x500123)))
            .await
            .contains("PHYSICAL 0/300000 ")
    );
    server
        .error("58P01", "requested WAL segment has already been removed")
        .await;
    server.ready().await;
    assert!(server.expect_query().await.contains("PHYSICAL 0/500000 "));
    server.send(b'W', &[0, 0, 0]).await;
    let end = server.stream(Lsn(5 * SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server.end().await;

    assert_eq!(finish(task).await.unwrap(), end);
    sink.assert_tiles(Lsn(5 * SEG), end);
}

/// A segment removed later in the stream is WalGone, not a retry without
/// end.
#[tokio::test]
async fn a_removed_segment_inside_the_copy_is_wal_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    server
        .error("58P01", "requested WAL segment has already been removed")
        .await;

    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::WalGone { requested, .. } if requested == end),
        "{err}"
    );
}

#[tokio::test]
async fn an_invalidated_slot_is_reported_as_wal_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.expect_query().await; // READ_REPLICATION_SLOT
    server
        .row(&[Some("physical"), Some("0/100000"), Some("1")])
        .await;
    assert!(server.expect_query().await.starts_with("START_REPLICATION"));
    server
        .error("55000", "can no longer access replication slot")
        .await;
    server.ready().await;

    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::WalGone { requested, .. } if requested == Lsn(SEG)),
        "{err}"
    );
}

/// The slot was invalidated on a later timeline: the server refuses the
/// copy before it starts, and a retry gets the same answer. It is WalGone.
#[tokio::test]
async fn an_invalidated_slot_on_a_later_timeline_is_wal_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_on(2).await;
    server.slot(Lsn(SEG)).await;
    assert!(server.expect_query().await.contains("PHYSICAL 0/300000 "));
    server
        .error("55000", "can no longer access replication slot")
        .await;
    server.ready().await;

    let err = finish(task).await.unwrap_err();
    assert!(matches!(err, Error::WalGone { .. }), "{err}");
}

/// An empty store on timeline 1, as on a new host, and a slot ahead of the
/// sink: the run starts at the end of the sink, not at the slot.
#[tokio::test]
async fn an_empty_store_on_timeline_1_starts_at_the_end_of_the_sink() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let wal = pattern(4096, 1);
    let held = RangeKey {
        system_id: 7401,
        timeline: 1,
        start: Lsn(SEG),
        end: Lsn(SEG + 4096),
    };
    sink.0
        .lock()
        .unwrap()
        .insert(held.to_string(), Bytes::from(wal.clone()));
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    assert!(
        server
            .start(None)
            .await
            .ends_with("PHYSICAL 0/100000 TIMELINE 1")
    );
    let end = server.stream(Lsn(SEG), &pattern(8192, 1)).await;
    server.await_flush(end).await;
    server.end().await;
    assert_eq!(finish(task).await.unwrap(), end);
}

/// An empty store after two failovers, 1 to 2 to 3, and a sink that ends
/// early on timeline 1: the run starts with the rest of timeline 1, and
/// every history file goes up.
#[tokio::test]
async fn an_empty_store_after_two_failovers_starts_on_the_oldest_held_timeline() {
    const HISTORY_3: &str = "1\t0/300A00\tno recovery target specified\n\
                             2\t0/500A00\tno recovery target specified\n";
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let held = RangeKey {
        system_id: 7401,
        timeline: 1,
        start: Lsn(SEG),
        end: Lsn(SEG + 4096),
    };
    let old_wal = pattern((0x300A00 - SEG) as usize, 1);
    sink.0
        .lock()
        .unwrap()
        .insert(held.to_string(), Bytes::copy_from_slice(&old_wal[..4096]));
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open_with(3, HISTORY_3).await;
    assert_eq!(server.expect_query().await, "TIMELINE_HISTORY 2");
    server.history(2, HISTORY).await;
    assert!(
        server
            .start(None)
            .await
            .ends_with("PHYSICAL 0/100000 TIMELINE 1")
    );
    let end = server.stream(Lsn(SEG), &old_wal).await;
    server.await_flush(end).await;
    server.copy_done().await;
    assert_eq!(finish(task).await.unwrap(), Lsn(0x300A00));

    let objects = sink.0.lock().unwrap().clone();
    assert_eq!(&objects["7401/00000002.history"][..], HISTORY.as_bytes());
    assert_eq!(&objects["7401/00000003.history"][..], HISTORY_3.as_bytes());
}

#[tokio::test]
async fn a_gap_ends_the_stream_and_what_was_flushed_is_still_uploaded() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(100, 0)).await;
    server.await_flush(end).await;
    server
        .xlogdata(Lsn(end.0 + 100), b"after a hole", Lsn(end.0 + 112))
        .await;

    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::Protocol(ref m) if m.contains("gap")),
        "{err}"
    );
    sink.assert_tiles(Lsn(SEG), end);
}

#[tokio::test]
async fn a_hang_up_mid_stream_is_an_error_and_what_was_flushed_is_uploaded() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let end = server.stream(Lsn(SEG), &pattern(4096, 0)).await;
    server.await_flush(end).await;
    drop(server);

    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof),
        "{err}"
    );
    sink.assert_tiles(Lsn(SEG), end);
}

#[tokio::test]
async fn a_silent_server_times_out() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let mut cfg = config(tmp.path());
    cfg.conn.idle_timeout = Duration::from_millis(300);
    let (task, mut server) = spawn_receiver(cfg, &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;

    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::Io(ref e) if e.kind() == io::ErrorKind::TimedOut),
        "{err}"
    );
    drop(server);
}

#[tokio::test]
async fn a_server_error_during_streaming_surfaces() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    server
        .error(
            "57P01",
            "terminating connection due to administrator command",
        )
        .await;

    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::Db { ref code, .. } if code == "57P01"),
        "{err}"
    );
}

#[tokio::test]
async fn hostile_frames_are_errors_not_panics() {
    let frames: [&[u8]; 4] = [
        // A frame length far beyond the cap.
        &[b'd', 0x7f, 0xff, 0xff, 0xff],
        // A length that does not even cover itself.
        &[b'd', 0, 0, 0, 2],
        // XLogData cut inside its header.
        &[b'd', 0, 0, 0, 8, b'w', 1, 2, 3],
        // An unknown CopyData kind.
        &[b'd', 0, 0, 0, 5, b'x'],
    ];
    for frame in frames {
        let tmp = tempfile::tempdir().unwrap();
        let sink = Arc::new(MemSink::default());
        let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
        server.open().await;
        server.start(Some(Lsn(SEG))).await;
        server.io.write_all(frame).await.unwrap();
        let err = finish(task).await.unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "{frame:?}: {err}");
    }
}

#[tokio::test]
async fn a_cleartext_password_challenge_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (task, mut server) = spawn_receiver(config(tmp.path()), &sink);
    server.read_startup().await;
    server.send(b'R', &3i32.to_be_bytes()).await;
    let err = finish(task).await.unwrap_err();
    assert!(
        matches!(err, Error::Protocol(ref m) if m.contains("authentication method 3")),
        "{err}"
    );
}

#[tokio::test]
async fn a_second_receiver_on_the_same_directory_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    let (first, mut first_server) = spawn_receiver(config(tmp.path()), &sink);
    first_server.open().await;
    first_server.start(Some(Lsn(SEG))).await;

    let (second, mut second_server) = spawn_receiver(config(tmp.path()), &sink);
    second_server.open().await;
    let err = finish(second).await.unwrap_err();
    assert!(
        matches!(err, Error::Store(ref m) if m.contains("in use")),
        "{err}"
    );

    drop(first_server);
    assert!(finish(first).await.is_err());
}

#[tokio::test]
async fn invalid_config_is_refused_before_any_io() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(MemSink::default());
    for cfg in [
        StreamConfig {
            slot: "PgWal-Test".into(),
            ..config(tmp.path())
        },
        StreamConfig {
            upload_every: Duration::ZERO,
            ..config(tmp.path())
        },
    ] {
        let (task, server) = spawn_receiver(cfg, &sink);
        let err = finish(task).await.unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        drop(server);
    }
    assert!(!tmp.path().join("system_id").exists());
}

/// The object store is down for a whole run, and the run dies. The WAL it
/// flushed locally reaches the object store through `salvage`, exactly.
#[tokio::test]
async fn salvage_uploads_what_a_dead_run_flushed_but_never_uploaded() {
    let tmp = tempfile::tempdir().unwrap();
    let (client, server) = tokio::io::duplex(4 << 20);
    let mut server = Walsender {
        io: server,
        buf: BytesMut::new(),
    };
    let cfg = config(tmp.path());
    let task = tokio::spawn(async move {
        receive(
            client,
            &cfg,
            Arc::new(GateSink::default()),
            std::future::pending(),
        )
        .await
    });
    server.open().await;
    server.start(Some(Lsn(SEG))).await;
    let wal = pattern((SEG + SEG / 2) as usize, 7);
    let end = server.stream(Lsn(SEG), &wal).await;
    server.await_flush(end).await;
    drop(server);
    assert!(finish(task).await.is_err(), "the run dies");

    let sink = Arc::new(MemSink::default());
    assert_eq!(salvage(tmp.path(), sink.clone()).await.unwrap(), Some(end));
    sink.assert_tiles(Lsn(SEG), end);
    assert_eq!(
        assemble_segment(1, SEG, &sink.ranges()).unwrap(),
        wal[..SEG as usize]
    );

    let objects = sink.0.lock().unwrap().len();
    assert_eq!(salvage(tmp.path(), sink.clone()).await.unwrap(), Some(end));
    assert_eq!(sink.0.lock().unwrap().len(), objects, "nothing twice");
    assert_eq!(
        salvage(&tmp.path().join("absent"), sink.clone())
            .await
            .unwrap(),
        None
    );
}

use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use pgwal::{ArchiveKey, Lsn, timeline_switches};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::store::{WalStore, first_segment, read_flushed, remove_other_timelines};
use crate::upload::{Uploader, WalSink, read_watermark, salvage};
use crate::wire::{Conn, ConnConfig, CopyMessage, check_slot, wal_gone};

/// Longest delay before a flush while WAL keeps arriving.
const MAX_FLUSH_DELAY: Duration = Duration::from_millis(100);

pub struct StreamConfig {
    pub conn: ConnConfig,
    /// Physical replication slot, created on first use.
    pub slot: String,
    /// One directory per cluster. One receiver at a time can use it.
    pub dir: PathBuf,
    /// Interval between two uploads to the sink. It bounds the WAL a host loss
    /// can take with it.
    pub upload_every: Duration,
    /// When the server may drop the WAL it sent.
    pub confirm: Confirm,
}

/// The position the receiver reports to the server as flushed. The slot keeps
/// the WAL after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confirm {
    /// Once the WAL is fsynced in the local store. A loss of the host loses
    /// the WAL of one upload interval.
    Flushed,
    /// Once the WAL is in the sink. A receiver on another host with an empty
    /// store loses nothing: it starts at the slot, or on a later timeline at
    /// its switch point.
    Uploaded,
}

/// Streams WAL from the server behind `stream` into `cfg.dir`, uploads it to
/// `sink`, and returns the LSN up to which the sink holds the WAL.
///
/// The stream ends in one of three ways:
/// - The server ends it. A clean server shutdown waits for this receiver to
///   confirm the flush of the final WAL, so the result covers the shutdown
///   checkpoint.
/// - After a failover, the stream of a parent timeline reaches its end. Call
///   `receive` again: it goes on with the next timeline.
/// - `stop` resolves. The receiver asks the server for its position, reads
///   and flushes up to it, and ends the stream. Use it before the server is
///   paused or killed. WAL that the server writes after it answers is not in
///   the result.
///
/// On any error, the WAL flushed so far is still uploaded before the function
/// returns. Calling it again with the same directory resumes without a gap.
pub async fn receive<S>(
    stream: S,
    cfg: &StreamConfig,
    sink: Arc<dyn WalSink>,
    stop: impl Future<Output = ()>,
) -> Result<Lsn, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    validate(cfg)?;
    let mut conn = Conn::connect(stream, &cfg.conn).await?;
    let system = conn.identify_system().await?;
    let seg_size = conn.wal_segment_size().await?;
    conn.ensure_physical_slot(&cfg.slot).await?;

    // The history of the server's timeline lists its parents, and where each
    // one ends.
    let history = match system.timeline {
        1 => None,
        timeline => Some(conn.timeline_history(timeline).await?),
    };
    let switches = history
        .as_deref()
        .map(timeline_switches)
        .unwrap_or_default();
    let server_start = match switches.last() {
        Some((_, begin)) => Start::Begin(*begin),
        None => Start::Slot,
    };

    // After a failover, a store on a parent timeline streams that timeline
    // to its end, which the new server holds when this receiver lagged behind
    // it. The next run uploads it, removes its files, and starts the next
    // timeline there.
    let (timeline, start) = match read_flushed(&cfg.dir).await? {
        Some((old, flushed)) if old != system.timeline => {
            match switches.iter().position(|(tl, _)| *tl == old) {
                Some(at) if flushed < switches[at].1 => (old, Start::Resume(flushed)),
                Some(at) => {
                    let next = switches.get(at + 1).map_or(system.timeline, |(tl, _)| *tl);
                    salvage(&cfg.dir, sink.clone()).await?;
                    remove_other_timelines(&cfg.dir, next, seg_size).await?;
                    (next, Start::Resume(switches[at].1))
                }
                // Another branch of the history: its WAL goes up, and the
                // stream follows the server.
                None => {
                    salvage(&cfg.dir, sink.clone()).await?;
                    remove_other_timelines(&cfg.dir, system.timeline, seg_size).await?;
                    (system.timeline, server_start)
                }
            }
        }
        _ => (system.timeline, server_start),
    };

    // A restore follows the timelines through their history files.
    let mut histories = Vec::new();
    for later in switches.iter().map(|(tl, _)| *tl).chain([system.timeline]) {
        if later <= 1 || later < timeline {
            continue;
        }
        let content = match &history {
            Some(content) if later == system.timeline => content.clone(),
            _ => conn.timeline_history(later).await?,
        };
        let key = ArchiveKey::History {
            system_id: system.system_id,
            timeline: later,
        };
        histories.push((key.to_string(), Bytes::from(content)));
    }

    let mut store = WalStore::open(cfg.dir.clone(), system.system_id, timeline, seg_size).await?;
    let start = match store.resume_point().await? {
        Some(lsn) => Start::Resume(lsn),
        None => match read_watermark(&cfg.dir, timeline).await? {
            Some(uploaded) => Start::Resume(uploaded.max(start.lsn().unwrap_or_default())),
            None => start,
        },
    };
    let mut from = match start {
        Start::Resume(lsn) | Start::Begin(lsn) => lsn,
        Start::Slot => slot_position(&mut conn, &cfg.slot, system.xlogpos).await?,
    }
    .segment_start(seg_size);
    // A timeline that began long ago can have lost its first WAL. The error
    // comes inside the copy for a removed segment, so the first message
    // tells. The slot position is then the earliest WAL left.
    let begun = conn.start_replication(&cfg.slot, from, timeline).await;
    let first_message = match (begun, start) {
        (Ok(()), Start::Begin(_)) => match conn.next_message().await {
            Ok(Some(message)) => Ok(Some(message)),
            Ok(None) => return Ok(from),
            // ReadyForQuery follows the error.
            Err(e) => match wal_gone(e, from) {
                gone @ Error::WalGone { .. } => {
                    conn.ready().await?;
                    Err(gone)
                }
                e => Err(e),
            },
        },
        (begun, _) => begun.map(|()| None),
    };
    let first_message = match (first_message, start) {
        (Err(Error::WalGone { .. }), Start::Begin(begin)) => {
            let fallback = slot_position(&mut conn, &cfg.slot, system.xlogpos)
                .await?
                .max(begin)
                .segment_start(seg_size);
            warn!(%from, %fallback, "the first WAL of the timeline is gone; its archive starts later");
            from = fallback;
            conn.start_replication(&cfg.slot, from, timeline).await?;
            None
        }
        (first_message, _) => first_message?,
    };
    let start = from;
    store.start_at(start);
    info!(slot = %cfg.slot, system_id = system.system_id, timeline, %start, "streaming WAL");

    // Finished segments before the start can wait for their upload: a dead
    // run with the sink down leaves them.
    let first = first_segment(&cfg.dir, timeline)
        .await?
        .map_or(start, |(lsn, _)| lsn.min(start));
    let mut uploader = Uploader::new(
        cfg.dir.clone(),
        system.system_id,
        timeline,
        seg_size,
        sink,
        first,
    )
    .await?;
    uploader.first = histories;
    let (flushed_tx, flushed_rx) = watch::channel(start);
    let (uploaded_tx, uploaded_rx) = watch::channel(uploader.uploaded);
    let mut upload = AbortOnDrop(tokio::spawn(uploader.run(
        flushed_rx,
        cfg.upload_every,
        uploaded_tx,
    )));

    let confirmed = Confirmed {
        confirm: cfg.confirm,
        uploaded: uploaded_rx,
    };
    let streamed = stream_into(
        &mut conn,
        &mut store,
        &flushed_tx,
        confirmed,
        first_message,
        stop,
    )
    .await;
    // Closing the channel makes the uploader send the rest and stop, even when
    // the stream failed. The store lock stays held until it has stopped.
    drop(flushed_tx);
    let uploaded = (&mut upload.0).await.expect("uploader panicked");
    let flushed = streamed?;
    let uploaded = uploaded?;
    debug!(%flushed, %uploaded, "replication ended");
    Ok(uploaded)
}

/// The restart position of the slot, or the server position for a slot
/// that holds no WAL yet.
async fn slot_position<S>(conn: &mut Conn<S>, slot: &str, xlogpos: Lsn) -> Result<Lsn, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    Ok(conn.slot_restart_lsn(slot).await?.unwrap_or(xlogpos))
}

/// Where a run starts streaming.
#[derive(Clone, Copy)]
enum Start {
    /// After the WAL that the store or the sink holds.
    Resume(Lsn),
    /// Where the timeline of the server begins. Its first WAL can be gone.
    Begin(Lsn),
    /// At the slot.
    Slot,
}

impl Start {
    fn lsn(self) -> Option<Lsn> {
        match self {
            Start::Resume(lsn) | Start::Begin(lsn) => Some(lsn),
            Start::Slot => None,
        }
    }
}

/// A dropped `receive` stops its uploader, which reads the store that the
/// next `receive` opens.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The flushed position the server sees, and the uploads it can wait for.
struct Confirmed {
    confirm: Confirm,
    uploaded: watch::Receiver<Lsn>,
}

impl Confirmed {
    fn position(&self, flushed: Lsn) -> Lsn {
        match self.confirm {
            Confirm::Flushed => flushed,
            Confirm::Uploaded => flushed.min(*self.uploaded.borrow()),
        }
    }

    /// Resolves when an upload ends. Never resolves once the uploader has
    /// stopped.
    async fn upload_ended(&mut self) {
        if self.uploaded.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn stream_into<S>(
    conn: &mut Conn<S>,
    store: &mut WalStore,
    flushed_tx: &watch::Sender<Lsn>,
    mut confirmed: Confirmed,
    mut first_message: Option<CopyMessage>,
    stop: impl Future<Output = ()>,
) -> Result<Lsn, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stop = pin!(stop);
    let mut drain = Drain::Streaming;
    // The highest WAL end the server announced up to its answer to a stop.
    let mut target = Lsn(0);
    let mut flushed = store.flush().await?;
    let mut last_flush = Instant::now();
    loop {
        let msg = match first_message.take() {
            Some(message) => Some(message),
            None => tokio::select! {
                () = &mut stop, if drain == Drain::Streaming => {
                    drain = Drain::Asked;
                    conn.ask_position(store.written(), confirmed.position(flushed), SystemTime::now())
                        .await?;
                    continue;
                }
                () = confirmed.upload_ended(), if confirmed.confirm == Confirm::Uploaded => {
                    send_status(conn, store.written(), confirmed.position(flushed)).await;
                    continue;
                }
                msg = conn.next_message() => msg.map_err(|e| wal_gone(e, store.written()))?,
            },
        };
        let Some(msg) = msg else { break };
        let reply_requested = match msg {
            CopyMessage::XLogData {
                start,
                wal_end,
                data,
            } => {
                store.write(start, &data).await?;
                if drain != Drain::Answered {
                    target = target.max(wal_end);
                }
                false
            }
            CopyMessage::Keepalive {
                wal_end,
                reply_requested,
            } => {
                if drain == Drain::Asked {
                    drain = Drain::Answered;
                    target = target.max(wal_end);
                }
                reply_requested
            }
        };
        let done = drain == Drain::Answered && store.written() >= target;
        // Flush once a burst is drained rather than per message, but never
        // later than MAX_FLUSH_DELAY under a continuous stream.
        if done
            || reply_requested
            || !conn.has_buffered_frame()
            || last_flush.elapsed() >= MAX_FLUSH_DELAY
        {
            flushed = store.flush().await?;
            last_flush = Instant::now();
            flushed_tx.send_replace(flushed);
            send_status(conn, store.written(), confirmed.position(flushed)).await;
        }
        if done {
            break;
        }
    }
    let flushed = store.flush().await?;
    flushed_tx.send_replace(flushed);
    // A failed goodbye changes nothing: the server can be going away.
    conn.send_copy_done().await.ok();
    Ok(flushed)
}

/// Reports the positions. The server may have closed after the last status:
/// the read side decides how the stream ended.
async fn send_status<S>(conn: &mut Conn<S>, written: Lsn, flushed: Lsn)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(e) = conn.send_status(written, flushed, SystemTime::now()).await {
        debug!(error = %e, "status update not delivered");
    }
}

/// A stop: the receiver asks the server for its position, and ends the
/// stream once the WAL up to the answer is written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Drain {
    Streaming,
    Asked,
    Answered,
}

/// Drops `slot` on the server behind `stream`, if it exists. After an
/// [`Error::WalGone`], call [`salvage`], then this, then clear the store
/// directory: the next [`receive`] creates the slot again and starts at the
/// server's position. Without the salvage, the flushed WAL that is not in
/// the sink goes with the directory.
pub async fn forget_slot<S>(stream: S, conn: &ConnConfig, slot: &str) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    Conn::connect(stream, conn).await?.drop_slot(slot).await
}

fn validate(cfg: &StreamConfig) -> Result<(), Error> {
    check_slot(&cfg.slot)?;
    if cfg.upload_every.is_zero() || cfg.conn.idle_timeout.is_zero() {
        return Err(Error::Config(
            "upload_every and idle_timeout must be positive".into(),
        ));
    }
    Ok(())
}

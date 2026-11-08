use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use pgwal::{Lsn, history_key, timeline_start};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::store::{WalStore, read_flushed, remove_other_timelines};
use crate::upload::{Uploader, WalSink, read_watermark, salvage};
use crate::wire::{Conn, ConnConfig, CopyMessage, check_slot};

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
    /// store starts at the slot and loses nothing.
    Uploaded,
}

/// Streams WAL from the server behind `stream` into `cfg.dir`, uploads it to
/// `sink`, and returns the LSN up to which the sink holds the WAL.
///
/// The stream ends in one of two ways:
/// - The server ends it. A clean server shutdown waits for this receiver to
///   confirm the flush of the final WAL, so the result covers the shutdown
///   checkpoint.
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

    // A timeline above 1 starts at its switch point. Its history file goes to
    // the sink, so a restore can follow it and pick an unused timeline.
    let history = match system.timeline {
        1 => None,
        timeline => Some(conn.timeline_history(timeline).await?),
    };
    let switch_point = history
        .as_deref()
        .and_then(timeline_start)
        .unwrap_or_default();
    if let Some(content) = &history {
        sink.put(
            &history_key(system.system_id, system.timeline),
            Bytes::from(content.clone()),
        )
        .await?;
    }

    // After a failover, the WAL of the old timeline goes up before its files
    // go.
    if let Some((timeline, _)) = read_flushed(&cfg.dir).await?
        && timeline != system.timeline
    {
        salvage(&cfg.dir, sink.clone()).await?;
        remove_other_timelines(&cfg.dir, system.timeline, seg_size).await?;
    }
    let mut store =
        WalStore::open(cfg.dir.clone(), system.system_id, system.timeline, seg_size).await?;
    // Nothing local on a later timeline: start at its switch point, not at the
    // slot. A slot created after a promotion would leave the first WAL of the
    // timeline out of the archive.
    let resumed = match store.resume_point().await? {
        Some(lsn) => Some(lsn),
        None => read_watermark(&cfg.dir, system.timeline)
            .await?
            .map(|uploaded| uploaded.max(switch_point)),
    };
    let first_wal_can_be_gone = resumed.is_none() && history.is_some();
    let mut start = match resumed {
        Some(lsn) => lsn,
        None if history.is_some() => switch_point,
        None => slot_position(&mut conn, &cfg.slot, system.xlogpos).await?,
    }
    .segment_start(seg_size);
    match conn
        .start_replication(&cfg.slot, start, system.timeline)
        .await
    {
        // The first WAL of an old later timeline can be gone. Then the slot
        // position is the earliest WAL left.
        Err(Error::WalGone { .. }) if first_wal_can_be_gone => {
            let fallback = slot_position(&mut conn, &cfg.slot, system.xlogpos)
                .await?
                .max(switch_point)
                .segment_start(seg_size);
            warn!(%start, %fallback, "the first WAL of the timeline is gone; its archive starts later");
            start = fallback;
            conn.start_replication(&cfg.slot, start, system.timeline)
                .await?;
        }
        other => other?,
    }
    store.start_at(start);
    info!(slot = %cfg.slot, system_id = system.system_id, timeline = system.timeline, %start, "streaming WAL");

    let uploader = Uploader::new(
        cfg.dir.clone(),
        system.system_id,
        system.timeline,
        seg_size,
        sink,
        start,
    )
    .await?;
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
    let streamed = stream_into(&mut conn, &mut store, &flushed_tx, confirmed, stop).await;
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
    async fn uploaded(&mut self) {
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
        let msg = tokio::select! {
            () = &mut stop, if drain == Drain::Streaming => {
                drain = Drain::Asked;
                conn.ask_position(store.written(), confirmed.position(flushed), SystemTime::now())
                    .await?;
                continue;
            }
            () = confirmed.uploaded(), if confirmed.confirm == Confirm::Uploaded => {
                conn.send_status(store.written(), confirmed.position(flushed), SystemTime::now())
                    .await?;
                continue;
            }
            msg = conn.next_message() => msg?,
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
            // The server may have closed after our last status. The read side
            // decides how the stream ended.
            if let Err(e) = conn
                .send_status(
                    store.written(),
                    confirmed.position(flushed),
                    SystemTime::now(),
                )
                .await
            {
                debug!(error = %e, "status update not delivered");
            }
        }
        if done {
            conn.send_copy_done().await.ok();
            return Ok(flushed);
        }
    }
    let flushed = store.flush().await?;
    flushed_tx.send_replace(flushed);
    // The server is going away; a failed goodbye changes nothing.
    conn.send_copy_done().await.ok();
    Ok(flushed)
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
/// [`Error::WalGone`], call it and clear the store directory: the next
/// [`receive`] creates the slot again and starts at the server's position.
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

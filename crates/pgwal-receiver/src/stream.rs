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
use crate::store::WalStore;
use crate::upload::{Uploader, WalSink, read_watermark};
use crate::wire::{Conn, CopyMessage};

/// Longest delay before a flush while WAL keeps arriving.
const MAX_FLUSH_DELAY: Duration = Duration::from_millis(100);

pub struct StreamConfig {
    /// Physical replication slot, created on first use.
    pub slot: String,
    pub user: String,
    /// For SCRAM-SHA-256. `None` when the server trusts `user`.
    pub password: Option<String>,
    /// The name `synchronous_standby_names` matches.
    pub application_name: String,
    /// One directory per cluster. One receiver at a time can use it.
    pub dir: PathBuf,
    /// Interval between two uploads to the sink. It bounds the WAL a host loss
    /// can take with it.
    pub upload_every: Duration,
    /// Longest silence accepted from the server. An idle walsender sends a
    /// keepalive after half of its `wal_sender_timeout` (60 s by default), so
    /// this must be longer than that half, and the server must not set
    /// `wal_sender_timeout` to 0.
    pub idle_timeout: Duration,
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
    let mut conn = Conn::connect(
        stream,
        &cfg.user,
        cfg.password.as_deref(),
        &cfg.application_name,
        cfg.idle_timeout,
    )
    .await?;
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
        .as_ref()
        .and_then(|(_, content)| timeline_start(content))
        .unwrap_or_default();

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
    let mut start = match resumed {
        Some(lsn) => lsn,
        None if history.is_some() => switch_point,
        None => conn
            .slot_restart_lsn(&cfg.slot)
            .await?
            .unwrap_or(system.xlogpos),
    }
    .segment_start(seg_size);
    if let Err(e) = conn
        .start_replication(&cfg.slot, start, system.timeline)
        .await
    {
        // The first WAL of an old later timeline can be gone. Then the slot
        // position is the earliest WAL left.
        if !matches!(e, Error::WalGone { .. }) || resumed.is_some() || history.is_none() {
            return Err(e);
        }
        let fallback = conn
            .slot_restart_lsn(&cfg.slot)
            .await?
            .unwrap_or(system.xlogpos)
            .max(switch_point)
            .segment_start(seg_size);
        warn!(%start, %fallback, "the first WAL of the timeline is gone; its archive starts later");
        start = fallback;
        conn.start_replication(&cfg.slot, start, system.timeline)
            .await?;
    }
    store.start_at(start);
    info!(slot = %cfg.slot, system_id = system.system_id, timeline = system.timeline, %start, "streaming WAL");

    if let Some((_, content)) = history {
        sink.put(
            &history_key(system.system_id, system.timeline),
            Bytes::from(content),
        )
        .await?;
    }

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
    let upload = tokio::spawn(uploader.run(flushed_rx, cfg.upload_every));

    let streamed = stream_into(&mut conn, &mut store, &flushed_tx, stop).await;
    // Closing the channel makes the uploader send the rest and stop, even when
    // the stream failed. The store lock stays held until it has stopped.
    drop(flushed_tx);
    let uploaded = upload.await.expect("uploader panicked");
    let flushed = streamed?;
    let uploaded = uploaded?;
    debug!(%flushed, %uploaded, "replication ended");
    Ok(uploaded)
}

async fn stream_into<S>(
    conn: &mut Conn<S>,
    store: &mut WalStore,
    flushed_tx: &watch::Sender<Lsn>,
    stop: impl Future<Output = ()>,
) -> Result<Lsn, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stop = pin!(stop);
    let mut drain = Drain::default();
    let mut flushed = store.flush().await?;
    let mut last_flush = Instant::now();
    loop {
        let msg = tokio::select! {
            () = &mut stop, if !drain.requested => {
                drain.requested = true;
                // The answer is a keepalive with the server's position.
                conn.send_status(store.written(), flushed, true, SystemTime::now())
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
                drain.target = drain.target.max(wal_end);
                false
            }
            CopyMessage::Keepalive {
                wal_end,
                reply_requested,
            } => {
                if drain.requested {
                    drain.answered = true;
                    drain.target = drain.target.max(wal_end);
                }
                reply_requested
            }
        };
        let done = drain.done(store.written());
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
                .send_status(store.written(), flushed, false, SystemTime::now())
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

/// A stop in progress: the receiver ends the stream once the server has
/// answered and everything it announced is written.
#[derive(Default)]
struct Drain {
    requested: bool,
    answered: bool,
    /// The highest WAL end the server announced.
    target: Lsn,
}

impl Drain {
    fn done(&self, written: Lsn) -> bool {
        self.answered && written >= self.target
    }
}

/// Drops the slot of `cfg` on the server behind `stream`. After a
/// [`Error::WalGone`], call it and clear the store directory: the next
/// [`receive`] creates the slot again and starts at the server's position.
pub async fn forget_slot<S>(stream: S, cfg: &StreamConfig) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    validate(cfg)?;
    let mut conn = Conn::connect(
        stream,
        &cfg.user,
        cfg.password.as_deref(),
        &cfg.application_name,
        cfg.idle_timeout,
    )
    .await?;
    conn.drop_slot(&cfg.slot).await
}

fn validate(cfg: &StreamConfig) -> Result<(), Error> {
    if !valid_slot_name(&cfg.slot) {
        return Err(Error::Config(format!(
            "invalid replication slot name {:?}",
            cfg.slot
        )));
    }
    if cfg.upload_every.is_zero() || cfg.idle_timeout.is_zero() {
        return Err(Error::Config(
            "upload_every and idle_timeout must be positive".into(),
        ));
    }
    Ok(())
}

/// Postgres' own rule for slot names.
fn valid_slot_name(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_names() {
        assert!(valid_slot_name("pgwal_receiver"));
        assert!(!valid_slot_name(""));
        assert!(!valid_slot_name("PgWal"));
        assert!(!valid_slot_name("x; DROP"));
    }
}

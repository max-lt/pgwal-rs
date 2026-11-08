use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use pgwal::{ArchiveKey, Lsn, RangeKey, timeline_switches};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::store::{WalStore, first_segment, read_flushed, remove_other_timelines};
use crate::upload::{Uploader, WalSink, read_watermark, salvage};
use crate::wire::{
    Conn, ConnConfig, CopyMessage, SystemInfo, UNDEFINED_FILE, check_slot, wal_gone,
};

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
    /// store loses nothing: it starts at the slot, or after a failover at the
    /// end of the parent timeline in the sink.
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

    let stored = read_flushed(&cfg.dir).await?;
    let (mut timeline, mut start) =
        choose_start(cfg, &system, &switches, stored, &sink, seg_size).await?;
    let oldest = stored.map_or(timeline, |(tl, _)| tl.min(timeline));
    let histories = histories(&mut conn, &system, history.as_deref(), &switches, oldest).await?;

    let (mut store, from, first_message) = loop {
        let store = WalStore::open(cfg.dir.clone(), system.system_id, timeline, seg_size).await?;
        let resolved = match store.resume_point().await? {
            Some(lsn) => Start::Resume(lsn),
            None => match (read_watermark(&cfg.dir, timeline).await?, start) {
                (Some(uploaded), Start::Begin(floor)) => Start::Begin(uploaded.max(floor)),
                (Some(uploaded), Start::Resume(floor) | Start::ParentTail(floor)) => {
                    Start::Resume(uploaded.max(floor))
                }
                (Some(uploaded), Start::Slot) => Start::Resume(uploaded),
                (None, start) => start,
            },
        };
        let from = match resolved {
            Start::Resume(lsn) | Start::Begin(lsn) | Start::ParentTail(lsn) => lsn,
            Start::Slot => slot_position(&mut conn, &cfg.slot, system.xlogpos).await?,
        }
        .segment_start(seg_size);
        let probe = matches!(resolved, Start::Begin(_) | Start::ParentTail(_));
        match (
            begin_copy(&mut conn, &cfg.slot, from, timeline, probe).await,
            start,
        ) {
            // A timeline that began long ago can have lost its first WAL. The
            // slot position is then the earliest WAL left.
            (Err(Error::WalGone { .. }), Start::Begin(begin)) => {
                let fallback = slot_position(&mut conn, &cfg.slot, system.xlogpos)
                    .await?
                    .max(begin)
                    .segment_start(seg_size);
                warn!(%from, %fallback, "the first WAL of the timeline is gone; its archive starts later");
                conn.start_replication(&cfg.slot, fallback, timeline)
                    .await?;
                break (store, fallback, None);
            }
            // The server no longer has the rest of the parent: the archive
            // has a gap there, and the stream follows the server.
            (Err(Error::WalGone { .. }), Start::ParentTail(_)) => {
                warn!(timeline, %from, "the end of the parent timeline is gone; the archive has a gap");
                drop(store);
                timeline = system.timeline;
                start = server_start(&switches);
            }
            (begun, _) => break (store, from, begun?),
        }
    };
    store.start_at(from);
    info!(slot = %cfg.slot, system_id = system.system_id, timeline, start = %from, "streaming WAL");

    // Finished segments before the start can wait for their upload: a dead
    // run with the sink down leaves them.
    let first = first_segment(&cfg.dir, timeline)
        .await?
        .map_or(from, |(lsn, _)| lsn.min(from));
    let mut uploader = Uploader::new(
        cfg.dir.clone(),
        system.system_id,
        timeline,
        seg_size,
        sink,
        first,
    )
    .await?;
    uploader.pending = histories;
    let (flushed_tx, flushed_rx) = watch::channel(from);
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

/// The end of the WAL of `timeline` in the sink.
async fn sink_end(sink: &dyn WalSink, system_id: u64, timeline: u32) -> Result<Option<Lsn>, Error> {
    let keys = sink.list(&format!("{system_id}/{timeline:08X}/")).await?;
    Ok(keys
        .iter()
        .filter_map(|key| RangeKey::parse(key))
        .map(|range| range.end)
        .max())
}

/// The history files of the timelines from `oldest` to the one of the
/// server: a restore follows the timelines through them. The server can
/// lack the file of an old parent, as a standby made with pg_basebackup
/// has none of the timelines before its start.
async fn histories<S>(
    conn: &mut Conn<S>,
    system: &SystemInfo,
    history: Option<&str>,
    switches: &[(u32, Lsn)],
    oldest: u32,
) -> Result<Vec<(String, Bytes)>, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut files = Vec::new();
    for timeline in switches.iter().map(|(tl, _)| *tl).chain([system.timeline]) {
        if timeline <= 1 || timeline < oldest {
            continue;
        }
        let content = match history {
            Some(content) if timeline == system.timeline => content.to_owned(),
            _ => match conn.timeline_history(timeline).await {
                Ok(content) => content,
                Err(Error::Db { code, .. }) if code == UNDEFINED_FILE => continue,
                Err(e) => return Err(e),
            },
        };
        let key = ArchiveKey::History {
            system_id: system.system_id,
            timeline,
        };
        files.push((key.to_string(), Bytes::from(content)));
    }
    Ok(files)
}

/// Starts the copy at `from`. With `probe`, reads its first message too: a
/// removed segment is an error inside the copy, then ReadyForQuery. The
/// first message is `Some(None)` when the stream ends at once.
async fn begin_copy<S>(
    conn: &mut Conn<S>,
    slot: &str,
    from: Lsn,
    timeline: u32,
    probe: bool,
) -> Result<Option<Option<CopyMessage>>, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    conn.start_replication(slot, from, timeline).await?;
    if !probe {
        return Ok(None);
    }
    match conn.next_message().await {
        Ok(message) => Ok(Some(message)),
        Err(e) => match wal_gone(e, from) {
            gone @ Error::WalGone { .. } => {
                conn.ready().await?;
                Err(gone)
            }
            e => Err(e),
        },
    }
}

/// Where a run starts streaming.
#[derive(Clone, Copy)]
enum Start {
    /// After the WAL that the store holds.
    Resume(Lsn),
    /// Where the timeline of the server begins, or after what the sink holds
    /// of it. The slot when that WAL is gone.
    Begin(Lsn),
    /// The parent timeline after what the sink holds of it. The timeline of
    /// the server when that WAL is gone.
    ParentTail(Lsn),
    /// At the slot.
    Slot,
}

fn server_start(switches: &[(u32, Lsn)]) -> Start {
    match switches.last() {
        Some((_, begin)) => Start::Begin(*begin),
        None => Start::Slot,
    }
}

/// The timeline a run streams, and where it starts. After a failover, a
/// store on a parent timeline streams that timeline to its end, which the
/// new server holds when this receiver lagged behind it. The next run
/// uploads it, removes its files, and starts the next timeline there.
async fn choose_start(
    cfg: &StreamConfig,
    system: &SystemInfo,
    switches: &[(u32, Lsn)],
    stored: Option<(u32, Lsn)>,
    sink: &Arc<dyn WalSink>,
    seg_size: u64,
) -> Result<(u32, Start), Error> {
    let Some((old, flushed)) = stored else {
        return empty_store_start(system, switches, sink.as_ref()).await;
    };
    if old == system.timeline {
        return Ok((system.timeline, server_start(switches)));
    }
    let Some(at) = switches.iter().position(|(tl, _)| *tl == old) else {
        // Another branch of the history: its WAL goes up, and the stream
        // follows the server.
        salvage(&cfg.dir, sink.clone()).await?;
        remove_other_timelines(&cfg.dir, system.timeline, seg_size).await?;
        return Ok((system.timeline, server_start(switches)));
    };
    let end = switches[at].1;
    if flushed < end {
        return Ok((old, Start::Resume(flushed)));
    }
    let next = switches.get(at + 1).map_or(system.timeline, |(tl, _)| *tl);
    salvage(&cfg.dir, sink.clone()).await?;
    remove_other_timelines(&cfg.dir, next, seg_size).await?;
    Ok((next, Start::Resume(end)))
}

/// The start of an empty store, as on a new host. The sink can end before
/// the switch point of the parent timeline, or hold the timeline of the
/// server already.
async fn empty_store_start(
    system: &SystemInfo,
    switches: &[(u32, Lsn)],
    sink: &dyn WalSink,
) -> Result<(u32, Start), Error> {
    let Some(&(parent, begin)) = switches.last() else {
        return Ok((system.timeline, Start::Slot));
    };
    if let Some(held) = sink_end(sink, system.system_id, parent).await?
        && held < begin
    {
        return Ok((parent, Start::ParentTail(held)));
    }
    let held = sink_end(sink, system.system_id, system.timeline).await?;
    Ok((
        system.timeline,
        Start::Begin(held.unwrap_or_default().max(begin)),
    ))
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
    mut first_message: Option<Option<CopyMessage>>,
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
            Some(message) => message,
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
/// directory: the next [`receive`] creates the slot again. It starts at the
/// slot, or on a later timeline after what the sink holds. Without the salvage, the flushed WAL that is not in
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

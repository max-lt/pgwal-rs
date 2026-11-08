//! A base backup over the replication protocol, written to a [`WalSink`] in
//! parts of at most 16 MiB.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use pgwal::{ArchiveKey, ArchivedBackup, Lsn};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use crate::error::Error;
use crate::upload::WalSink;
use crate::wire::{BackupMessage, Conn, ConnConfig};

const MANIFEST: &str = "backup_manifest";

/// Takes a base backup of the server behind `stream`, without its WAL, and
/// writes it to `sink`.
///
/// A restore needs the WAL from `start` to `end` of the result. Take the
/// backup while [`receive`](crate::receive) streams `slot`, which keeps that
/// WAL on the server until the archive has it. Without the slot, or with a
/// slot that holds no WAL, the backup does not start: the error is
/// [`Error::Config`].
///
/// The server sends nothing during the checkpoint at the start: give `conn`
/// an idle timeout that covers it.
///
/// The `done` object goes last. On an error, the parts written so far stay
/// in the sink without it.
pub async fn base_backup<S>(
    stream: S,
    conn: &ConnConfig,
    slot: &str,
    sink: Arc<dyn WalSink>,
) -> Result<ArchivedBackup, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = Conn::connect(stream, conn).await?;
    let system = conn.identify_system().await?;
    if conn.slot_restart_lsn(slot).await?.is_none() {
        return Err(Error::Config(format!(
            "slot {slot} does not exist or holds no WAL: the WAL of a backup could leave the server"
        )));
    }
    let begun = conn.start_base_backup().await?;
    info!(start = %begun.start, timeline = begun.timeline, "base backup started");

    let mut parts = Parts {
        sink,
        system_id: system.system_id,
        start: begun.start,
        streams: Vec::new(),
        buf: BytesMut::new(),
    };
    while let Some(message) = conn.next_backup_message().await? {
        match message {
            BackupMessage::Archive { name } => parts.begin(name).await?,
            BackupMessage::Manifest => parts.begin(MANIFEST.into()).await?,
            BackupMessage::Data(data) => parts.write(data).await?,
        }
    }
    parts.flush().await?;
    let (end, end_timeline) = conn.finish_base_backup().await?;
    if end_timeline != begun.timeline {
        return Err(Error::Protocol(format!(
            "the timeline went from {} to {end_timeline} during the backup",
            begun.timeline
        )));
    }
    let backup = ArchivedBackup {
        system_id: system.system_id,
        timeline: begun.timeline,
        start: begun.start,
        end,
        streams: parts.streams,
        tablespaces: begun.tablespaces,
    };
    let has_base = backup.streams.iter().any(|(name, _)| name == "base.tar");
    let manifest_last = backup
        .streams
        .last()
        .is_some_and(|(name, _)| name == MANIFEST);
    if !has_base || !manifest_last {
        return Err(Error::Protocol(format!(
            "BASE_BACKUP: files {:?}, not base.tar then the manifest",
            backup.streams
        )));
    }
    parts
        .sink
        .put(
            &ArchiveKey::BackupDone {
                system_id: backup.system_id,
                start: backup.start,
            }
            .to_string(),
            Bytes::from(backup.to_text()),
        )
        .await?;
    info!(start = %backup.start, end = %backup.end, "base backup written");
    Ok(backup)
}

/// The files of the backup cut into parts. The last entry of `streams` is
/// the file being written, with the count of its parts already in the sink.
struct Parts {
    sink: Arc<dyn WalSink>,
    system_id: u64,
    start: Lsn,
    streams: Vec<(String, u32)>,
    buf: BytesMut,
}

impl Parts {
    async fn begin(&mut self, name: String) -> Result<(), Error> {
        if self.streams.iter().any(|(seen, _)| *seen == name) {
            return Err(Error::Protocol(format!("BASE_BACKUP: {name} twice")));
        }
        self.flush().await?;
        self.streams.push((name, 0));
        Ok(())
    }

    async fn write(&mut self, data: Bytes) -> Result<(), Error> {
        if self.streams.is_empty() {
            return Err(Error::Protocol("BASE_BACKUP: data before a file".into()));
        }
        self.buf.extend_from_slice(&data);
        while self.buf.len() >= crate::MAX_OBJECT {
            let part = self.buf.split_to(crate::MAX_OBJECT).freeze();
            self.put(part).await?;
        }
        Ok(())
    }

    /// Writes the rest of the current file. A file has at least one part.
    async fn flush(&mut self) -> Result<(), Error> {
        let Some((_, written)) = self.streams.last() else {
            return Ok(());
        };
        if self.buf.is_empty() && *written > 0 {
            return Ok(());
        }
        let rest = self.buf.split().freeze();
        self.put(rest).await
    }

    async fn put(&mut self, part: Bytes) -> Result<(), Error> {
        let (name, written) = self.streams.last_mut().expect("a file has started");
        let key = ArchiveKey::BackupPart {
            system_id: self.system_id,
            start: self.start,
            name: name.clone(),
            part: *written,
        };
        self.sink.put(&key.to_string(), part).await?;
        *written += 1;
        Ok(())
    }
}

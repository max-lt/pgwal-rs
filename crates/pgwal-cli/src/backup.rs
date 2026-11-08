use std::collections::BTreeSet;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::pin;

use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
use clap::Args;
use futures_util::{StreamExt, TryStreamExt};
use pgwal::{ArchiveKey, ArchivedBackup, Lsn, RangeKey, chain_end};
use pgwal_receiver::{WalSink, base_backup};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::ConnArgs;
use crate::archive::{self, ArchiveArgs};

/// Parts read ahead of the unpacking: about 32 MiB, and the reads in flight.
const PARTS_AHEAD: usize = 2;
/// Parts read from the archive at one time.
const PARTS_IN_FLIGHT: usize = 8;

#[derive(Args, Debug)]
pub struct BackupArgs {
    #[command(flatten)]
    conn: ConnArgs,
    /// The slot of `pgwal receive`. Without it, the backup does not start.
    #[arg(long, default_value = "pgwal")]
    slot: String,
    #[arg(long, default_value = "pgwal_backup")]
    application_name: String,
    #[command(flatten)]
    archive: ArchiveArgs,
    /// Longest silence accepted from the server, in seconds. It covers the
    /// checkpoint at the start of the backup.
    #[arg(long, default_value_t = 300)]
    idle_timeout: u64,
}

/// Takes a base backup into the archive and prints its start and end LSN.
pub async fn run(args: BackupArgs) -> Result<()> {
    let tls = args.conn.tls()?;
    let sink = args.archive.open()?;
    let conn = args.conn.config(args.application_name, args.idle_timeout);
    let stream = args.conn.connect(tls.as_ref()).await?;
    let backup = base_backup(stream, &conn, &args.slot, sink).await?;
    println!("{} {}", backup.start, backup.end);
    Ok(())
}

#[derive(Args, Debug)]
pub struct FetchArgs {
    #[command(flatten)]
    archive: ArchiveArgs,
    /// A missing or empty directory.
    #[arg(long)]
    pgdata: PathBuf,
    /// The cluster to fetch, when the archive holds backups of several.
    #[arg(long)]
    system_id: Option<u64>,
}

/// Writes the latest backup whose WAL the archive holds up to its end, and
/// prints its start LSN. Then `pgwal restore` writes the WAL.
pub async fn run_fetch(args: FetchArgs) -> Result<()> {
    let archive = args.archive.open()?;
    let mut keys = archive.list("").await?;
    let backup = latest(archive.as_ref(), &mut keys, args.system_id).await?;
    if !backup.tablespaces.is_empty() {
        bail!(
            "the backup at {} has tablespaces, and pgwal fetch writes the data directory only",
            backup.start
        );
    }
    empty_dir(&args.pgdata)?;
    for (name, parts) in &backup.streams {
        match name.as_str() {
            "base.tar" => unpack(archive.as_ref(), &backup, name, *parts, &args.pgdata).await?,
            "backup_manifest" => {
                let mut manifest = Vec::new();
                for part in 0..*parts {
                    manifest.extend(
                        archive::read(archive.as_ref(), &part_key(&backup, name, part)).await?,
                    );
                }
                tokio::fs::write(args.pgdata.join(name), manifest).await?;
            }
            other => bail!("the backup at {} has an unknown file {other}", backup.start),
        }
    }
    info!(start = %backup.start, end = %backup.end, "fetched");
    println!("{}", backup.start);
    Ok(())
}

async fn latest(
    archive: &dyn WalSink,
    keys: &mut Vec<String>,
    system_id: Option<u64>,
) -> Result<ArchivedBackup> {
    let mut found: Vec<(u64, Lsn)> = keys
        .iter()
        .filter_map(|key| match ArchiveKey::parse(key)? {
            ArchiveKey::BackupDone { system_id, start } => Some((system_id, start)),
            _ => None,
        })
        .filter(|(system, _)| system_id.is_none_or(|wanted| wanted == *system))
        .collect();
    let systems: BTreeSet<u64> = found.iter().map(|(system, _)| *system).collect();
    if systems.len() > 1 {
        bail!("the archive holds backups of the systems {systems:?}: choose one with --system-id");
    }
    found.sort();
    let mut listed_again = false;
    for (system, start) in found.into_iter().rev() {
        let key = ArchiveKey::BackupDone {
            system_id: system,
            start,
        }
        .to_string();
        let text = archive::read(archive, &key).await?;
        let backup = std::str::from_utf8(&text)
            .ok()
            .and_then(ArchivedBackup::parse)
            .filter(|b| (b.system_id, b.start) == (system, start))
            .with_context(|| format!("unreadable {key}"))?;
        if !covered(keys, &backup) && !listed_again {
            archive::list_again(archive, "", keys).await?;
            listed_again = true;
        }
        if covered(keys, &backup) {
            return Ok(backup);
        }
        warn!(start = %backup.start, end = %backup.end, "the archive lacks WAL up to the end of the backup");
    }
    bail!("no base backup in the archive with its WAL up to its end")
}

/// The archive holds the WAL of `backup` from its start to its end.
fn covered(keys: &[String], backup: &ArchivedBackup) -> bool {
    let ranges: Vec<RangeKey> = keys
        .iter()
        .filter_map(|key| RangeKey::parse(key))
        .filter(|r| r.system_id == backup.system_id)
        .collect();
    chain_end(&ranges, backup.timeline, backup.start) >= backup.end
}

fn empty_dir(dir: &Path) -> Result<()> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                bail!("{} is not empty", dir.display());
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => std::fs::create_dir_all(dir)?,
        Err(e) => return Err(e).with_context(|| format!("reading {}", dir.display())),
    }
    // Postgres refuses a data directory that others can read.
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn part_key(backup: &ArchivedBackup, name: &str, part: u32) -> String {
    ArchiveKey::BackupPart {
        system_id: backup.system_id,
        start: backup.start,
        name: name.to_owned(),
        part,
    }
    .to_string()
}

/// Unpacks the tar file `name` into `dir` as its parts arrive.
async fn unpack(
    archive: &dyn WalSink,
    backup: &ArchivedBackup,
    name: &str,
    parts: u32,
    dir: &Path,
) -> Result<()> {
    let (tx, rx) = mpsc::channel(PARTS_AHEAD);
    let dir = dir.to_path_buf();
    let unpacker = tokio::task::spawn_blocking(move || {
        let mut tar = tar::Archive::new(Parts {
            rx,
            part: Bytes::new(),
        });
        tar.set_preserve_permissions(true);
        tar.unpack(dir)
    });
    let fetched: Result<()> = async {
        let mut reads = pin!(
            futures_util::stream::iter(0..parts)
                .map(|part| async move {
                    archive::read(archive, &part_key(backup, name, part)).await
                })
                .buffered(PARTS_IN_FLIGHT)
        );
        while let Some(data) = reads.try_next().await? {
            // A closed channel means the unpacker failed: its error says why.
            if tx.send(data).await.is_err() {
                break;
            }
        }
        Ok(())
    }
    .await;
    drop(tx);
    let unpacked = unpacker.await.expect("the unpacker panicked");
    // A failed read ends the input early, and tar can take that for the end.
    fetched?;
    unpacked.with_context(|| format!("unpacking {name}"))
}

/// The parts of a file, in order, as one reader.
struct Parts {
    rx: mpsc::Receiver<Bytes>,
    part: Bytes,
}

impl Read for Parts {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.part.is_empty() {
            match self.rx.blocking_recv() {
                Some(part) => self.part = part,
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.part.len());
        buf[..n].copy_from_slice(&self.part[..n]);
        self.part.advance(n);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn backup(start: u64, end: u64) -> ArchivedBackup {
        ArchivedBackup {
            system_id: 7,
            timeline: 1,
            start: Lsn(start),
            end: Lsn(end),
            streams: Vec::new(),
            tablespaces: Vec::new(),
        }
    }

    fn range(timeline: u32, start: u64, end: u64) -> String {
        RangeKey {
            system_id: 7,
            timeline,
            start: Lsn(start),
            end: Lsn(end),
        }
        .to_string()
    }

    /// Misses `hidden` in its first listing, as a listing during a
    /// compaction can.
    struct FirstListMisses {
        sink: pgwal_receiver::DirSink,
        hidden: String,
        listed: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl WalSink for FirstListMisses {
        async fn put(&self, key: &str, data: Bytes) -> io::Result<()> {
            self.sink.put(key, data).await
        }

        async fn get(&self, key: &str) -> io::Result<Option<Bytes>> {
            self.sink.get(key).await
        }

        async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
            let mut keys = self.sink.list(prefix).await?;
            if !self.listed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                keys.retain(|key| *key != self.hidden);
            }
            Ok(keys)
        }

        async fn delete(&self, keys: &[String]) -> io::Result<()> {
            self.sink.delete(keys).await
        }
    }

    #[tokio::test]
    async fn a_listing_that_misses_wal_is_listed_again() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = FirstListMisses {
            sink: pgwal_receiver::DirSink::new(tmp.path()),
            hidden: range(1, 0x40, 0x90),
            listed: Default::default(),
        };
        let backup = backup(0x30, 0x60);
        let done = ArchiveKey::BackupDone {
            system_id: 7,
            start: backup.start,
        };
        for (key, data) in [
            (done.to_string(), backup.to_text()),
            (range(1, 0x00, 0x40), String::new()),
            (range(1, 0x40, 0x90), String::new()),
        ] {
            archive.put(&key, data.into()).await.unwrap();
        }
        let mut keys = archive.list("").await.unwrap();
        assert_eq!(latest(&archive, &mut keys, None).await.unwrap(), backup);
    }

    #[test]
    fn the_wal_must_reach_the_end_of_the_backup() {
        let keys = [
            range(1, 0x00, 0x40),
            range(1, 0x20, 0x80),
            range(1, 0x80, 0x90),
        ];
        assert!(covered(&keys, &backup(0x30, 0x90)));
        assert!(!covered(&keys, &backup(0x30, 0x91)));
        let gap = [range(1, 0x00, 0x40), range(1, 0x50, 0x90)];
        assert!(!covered(&gap, &backup(0x30, 0x60)));
        let other_timeline = [range(2, 0x00, 0x90)];
        assert!(!covered(&other_timeline, &backup(0x30, 0x60)));
    }
}

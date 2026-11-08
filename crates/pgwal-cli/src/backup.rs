use std::collections::BTreeSet;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
use clap::Args;
use pgwal::{ArchivedBackup, RangeKey, backup_done_key, backup_part_key, parse_backup_done_key};
use pgwal_receiver::base_backup;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::ConnArgs;
use crate::archive::{Archive, S3Args};

/// Parts read ahead of the unpacking: about 32 MiB.
const PARTS_AHEAD: usize = 2;

#[derive(Args, Debug)]
pub struct BackupArgs {
    #[command(flatten)]
    conn: ConnArgs,
    /// The slot of `pgwal receive`. Without it, the backup does not start.
    #[arg(long, default_value = "pgwal")]
    slot: String,
    #[arg(long, default_value = "pgwal_backup")]
    application_name: String,
    /// The archive of `pgwal receive`: a directory, or `s3://bucket/prefix/`.
    #[arg(long)]
    archive: String,
    #[command(flatten)]
    s3: S3Args,
    /// Longest silence accepted from the server, in seconds. It covers the
    /// checkpoint at the start of the backup.
    #[arg(long, default_value_t = 300)]
    idle_timeout: u64,
}

/// Takes a base backup into the archive and prints its start and end LSN.
pub async fn run(args: BackupArgs) -> Result<()> {
    let tls = args.conn.tls()?;
    let sink = Archive::open(&args.archive, &args.s3)?.sink();
    let conn = args.conn.config(args.application_name, args.idle_timeout);
    let stream = args.conn.connect(tls.as_ref()).await?;
    let backup = base_backup(stream, &conn, &args.slot, sink).await?;
    println!("{} {}", backup.start, backup.end);
    Ok(())
}

#[derive(Args, Debug)]
pub struct FetchArgs {
    #[arg(long)]
    archive: String,
    #[command(flatten)]
    s3: S3Args,
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
    let archive = Archive::open(&args.archive, &args.s3)?;
    let keys = archive.keys().await?;
    let backup = latest(&archive, &keys, args.system_id).await?;
    if !backup.tablespaces.is_empty() {
        bail!(
            "the backup at {} has tablespaces, and pgwal fetch writes the data directory only",
            backup.start
        );
    }
    empty_dir(&args.pgdata)?;
    for (name, parts) in &backup.streams {
        match name.as_str() {
            "base.tar" => unpack(&archive, &backup, name, *parts, &args.pgdata).await?,
            "backup_manifest" => {
                let mut manifest = Vec::new();
                for part in 0..*parts {
                    manifest.extend(archive.read(&part_key(&backup, name, part)).await?);
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
    archive: &Archive,
    keys: &[String],
    system_id: Option<u64>,
) -> Result<ArchivedBackup> {
    let mut found: Vec<(u64, pgwal::Lsn)> = keys
        .iter()
        .filter_map(|key| parse_backup_done_key(key))
        .filter(|(system, _)| system_id.is_none_or(|wanted| wanted == *system))
        .collect();
    let systems: BTreeSet<u64> = found.iter().map(|(system, _)| *system).collect();
    if systems.len() > 1 {
        bail!("the archive holds backups of the systems {systems:?}: choose one with --system-id");
    }
    found.sort();
    for (system, start) in found.into_iter().rev() {
        let key = backup_done_key(system, start);
        let text = archive.read(&key).await?;
        let backup = std::str::from_utf8(&text)
            .ok()
            .and_then(ArchivedBackup::parse)
            .filter(|b| (b.system_id, b.start) == (system, start))
            .with_context(|| format!("unreadable {key}"))?;
        if covered(keys, &backup) {
            return Ok(backup);
        }
        warn!(start = %backup.start, end = %backup.end, "the archive lacks WAL up to the end of the backup");
    }
    bail!("no base backup in the archive with its WAL up to its end")
}

/// The archive holds the WAL of `backup` from its start to its end.
fn covered(keys: &[String], backup: &ArchivedBackup) -> bool {
    let mut ranges: Vec<RangeKey> = keys
        .iter()
        .filter_map(|key| RangeKey::parse(key))
        .filter(|r| r.system_id == backup.system_id && r.timeline == backup.timeline)
        .collect();
    ranges.sort();
    let mut at = backup.start;
    for range in ranges {
        if range.start > at {
            break;
        }
        at = at.max(range.end);
    }
    at >= backup.end
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
    backup_part_key(backup.system_id, backup.start, name, part)
}

/// Unpacks the tar file `name` into `dir` as its parts arrive.
async fn unpack(
    archive: &Archive,
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
        for part in 0..parts {
            let data = archive.read(&part_key(backup, name, part)).await?;
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
    use pgwal::Lsn;

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

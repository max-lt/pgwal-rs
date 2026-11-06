use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use pgwal::{
    BackupLabel, ControlFile, Recovery, history_key, plan, request_archive_recovery,
    request_standby, segment_size, write_history, write_segment,
};
use tracing::{info, warn};

#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Directory of the archived objects, as `pgwal receive --archive` writes
    /// it.
    #[arg(long)]
    archive: PathBuf,
    /// A copy of the data directory: a base backup or a snapshot. Postgres
    /// must not run on it.
    #[arg(long)]
    pgdata: PathBuf,
    /// Restore up to the first gap of the archive, instead of stopping.
    #[arg(long)]
    accept_gap: bool,
    /// Start as a standby that stays in recovery, on the timeline of the
    /// archive, instead of a server that promotes. A copy of it after a
    /// clean stop is a new image to restore from.
    #[arg(long)]
    standby: bool,
}

/// Writes the archived WAL that follows the copy into its `pg_wal`. The
/// next start of Postgres replays it. Prints the LSN where the replay ends.
pub fn run(args: RestoreArgs) -> Result<()> {
    let pgdata = &args.pgdata;
    if pgdata.join("postmaster.pid").exists() {
        bail!(
            "{} has a postmaster.pid: stop Postgres, or remove the file of a copy",
            pgdata.display()
        );
    }
    let pg_wal = pgdata.join("pg_wal");
    std::fs::create_dir_all(&pg_wal)?;
    let control = ControlFile::read(pgdata)
        .with_context(|| format!("reading {}/global/pg_control", pgdata.display()))?;
    let label = BackupLabel::read(pgdata)?;
    let from = match &label {
        Some(label) => ControlFile {
            clean_shutdown: false,
            checkpoint: label.start,
            redo: label.start,
            timeline: label.timeline,
            ..control
        },
        None => control,
    };
    let seg_size = match segment_size(&pg_wal) {
        Ok(size) => size,
        Err(_) => label
            .as_ref()
            .and_then(BackupLabel::segment_size)
            .context("no segment in pg_wal and no backup_label: unknown WAL segment size")?,
    };

    let keys = archive_keys(&args.archive)?;
    let plan = plan(keys.iter().map(String::as_str), &from, seg_size);
    if let Some(gap) = plan.gap {
        if !args.accept_gap {
            bail!("the archive has a gap at {gap}; --accept-gap restores up to it");
        }
        warn!(%gap, "the archive has a gap: the replay stops there");
    }
    if label.is_some() && plan.segments.is_empty() {
        bail!(
            "the archive has no WAL after the backup start {}",
            from.redo
        );
    }

    for timeline in &plan.histories {
        let key = history_key(control.system_id, *timeline);
        write_history(&pg_wal, *timeline, &read_object(&args.archive, &key)?)?;
    }
    for (timeline, segno, keys) in &plan.segments {
        let objects = keys
            .iter()
            .map(|key| {
                let wal = read_object(&args.archive, &key.to_string())?;
                if wal.len() as u64 != key.end.0 - key.start.0 {
                    bail!("archived object {key} holds {} bytes", wal.len());
                }
                Ok((key.start, key.end, wal))
            })
            .collect::<Result<Vec<_>>>()?;
        write_segment(&pg_wal, *timeline, *segno, seg_size, &objects)?;
    }

    // A base backup replays as an archive recovery, then promotes.
    let recovery = match plan.recovery {
        Recovery::Crash { end } if label.is_some() => Recovery::Archive { end },
        recovery => recovery,
    };
    let (archive_recovery, end) = match recovery {
        Recovery::Nothing => (false, None),
        Recovery::Crash { end } => (false, Some(end)),
        Recovery::Archive { end } => (true, Some(end)),
    };
    if args.standby {
        request_standby(pgdata)?;
    } else if archive_recovery {
        request_archive_recovery(pgdata)?;
    }
    info!(
        system_id = control.system_id,
        from = %from.redo,
        end = %end.map_or("-".to_owned(), |e| e.to_string()),
        segments = plan.segments.len(),
        histories = plan.histories.len(),
        archive_recovery,
        standby = args.standby,
        "WAL written to pg_wal; start Postgres to replay it"
    );
    println!("{}", end.unwrap_or(from.redo));
    Ok(())
}

/// The keys of the archive: the file paths under `root`. Names that start
/// with `.` are files that an upload has not finished.
fn archive_keys(root: &Path) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let entries = std::fs::read_dir(root.join(&dir))
            .with_context(|| format!("reading {}", root.join(&dir).display()))?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with('.') {
                continue;
            }
            let path = dir.join(name);
            if entry.file_type()?.is_dir() {
                dirs.push(path);
            } else if let Some(key) = path.to_str() {
                keys.push(key.to_owned());
            }
        }
    }
    Ok(keys)
}

fn read_object(root: &Path, key: &str) -> Result<Vec<u8>> {
    std::fs::read(root.join(key)).with_context(|| format!("reading archived object {key}"))
}

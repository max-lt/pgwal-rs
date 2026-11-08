use std::path::{Path, PathBuf};
use std::pin::pin;

use anyhow::{Context, Result, bail};
use clap::Args;
use futures_util::{StreamExt, TryStreamExt};
use pgwal::{
    BackupLabel, ControlFile, Lsn, Recovery, history_key, plan, request_archive_recovery,
    request_standby, segment_size, write_history, write_segment,
};
use tracing::{info, warn};

use pgwal_receiver::WalSink;

use crate::archive::{self, S3Args};

/// Segments fetched at the same time, and objects of one segment. They bound
/// a restore to 64 reads in flight and about 64 MiB of segments in memory.
const SEGMENTS_IN_FLIGHT: usize = 4;
const OBJECTS_IN_FLIGHT: usize = 16;

#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// The archive, as `pgwal receive --archive` writes it: a directory, or
    /// `s3://bucket/prefix/`.
    #[arg(long)]
    archive: String,
    #[command(flatten)]
    s3: S3Args,
    /// A copy of the data directory: a base backup or a snapshot. Postgres
    /// must not run on it.
    #[arg(long)]
    pgdata: PathBuf,
    /// Restore up to the first gap of the archive, instead of stopping.
    #[arg(long)]
    accept_gap: bool,
    /// Start as a standby that stays in recovery, on the timeline of the
    /// archive, instead of a server that promotes. A copy of it after a
    /// clean stop is a new base backup.
    #[arg(long)]
    standby: bool,
}

/// Writes the archived WAL that follows the copy into its `pg_wal`. The
/// next start of Postgres replays it. Prints the LSN where the replay ends.
pub async fn run(args: RestoreArgs) -> Result<()> {
    let archive = archive::open(&args.archive, &args.s3)?;
    let end = apply(
        archive.as_ref(),
        &args.pgdata,
        args.accept_gap,
        args.standby,
    )
    .await?;
    println!("{end}");
    Ok(())
}

#[derive(Args, Debug)]
pub struct PlanArgs {
    /// The copy of the data directory to restore.
    #[arg(long)]
    pgdata: PathBuf,
    /// A prefix of the keys on stdin, such as a bucket prefix. The output
    /// keeps it.
    #[arg(long, default_value = "")]
    prefix: String,
}

/// Reads archive keys on stdin, one per line, and prints the ones that a
/// restore of `--pgdata` reads: the objects to fetch before `pgwal restore`.
pub fn run_plan(args: PlanArgs) -> Result<()> {
    let start = replay_start(&args.pgdata)?;
    let keys: Vec<String> = std::io::stdin()
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| line.strip_prefix(&args.prefix).map(str::to_owned))
        .collect();
    let plan = plan(keys.iter().map(String::as_str), &start.from, start.seg_size);
    if let Some(gap) = plan.gap {
        warn!(%gap, "the archive has a gap: a restore stops there");
    }
    for timeline in &plan.histories {
        println!(
            "{}{}",
            args.prefix,
            history_key(start.control.system_id, *timeline)
        );
    }
    for (_, _, keys) in &plan.segments {
        for key in keys {
            println!("{}{key}", args.prefix);
        }
    }
    Ok(())
}

/// Where the replay of a copy starts, and its segment size.
struct Start {
    control: ControlFile,
    /// `control`, with the start of `backup_label` when the copy has one.
    from: ControlFile,
    label: Option<BackupLabel>,
    seg_size: u64,
}

fn replay_start(pgdata: &Path) -> Result<Start> {
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
    let seg_size = match segment_size(&pgdata.join("pg_wal")) {
        Ok(size) => size,
        Err(_) => label
            .as_ref()
            .and_then(BackupLabel::segment_size)
            .context("no segment in pg_wal and no backup_label: unknown WAL segment size")?,
    };
    Ok(Start {
        control,
        from,
        label,
        seg_size,
    })
}

/// Writes the archived WAL that follows the copy in `pgdata` into its
/// `pg_wal`, and returns the LSN where the replay ends.
pub async fn apply(
    archive: &dyn WalSink,
    pgdata: &Path,
    accept_gap: bool,
    standby: bool,
) -> Result<Lsn> {
    if pgdata.join("postmaster.pid").exists() {
        bail!(
            "{} has a postmaster.pid: stop Postgres, or remove the file of a copy",
            pgdata.display()
        );
    }
    let pg_wal = pgdata.join("pg_wal");
    std::fs::create_dir_all(&pg_wal)?;
    let Start {
        control,
        from,
        label,
        seg_size,
    } = replay_start(pgdata)?;

    let keys = archive.list("").await?;
    let plan = plan(keys.iter().map(String::as_str), &from, seg_size);
    if let Some(gap) = plan.gap {
        if !accept_gap {
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
        write_history(&pg_wal, *timeline, &archive::read(archive, &key).await?)?;
    }
    let mut segments = pin!(
        futures_util::stream::iter(&plan.segments)
            .map(|(timeline, segno, keys)| async move {
                let objects: Vec<_> = futures_util::stream::iter(keys)
                    .map(|key| async move {
                        let wal = archive::read(archive, &key.to_string()).await?;
                        if wal.len() as u64 != key.end.0 - key.start.0 {
                            bail!("archived object {key} holds {} bytes", wal.len());
                        }
                        Ok((key.start, key.end, wal))
                    })
                    .buffered(OBJECTS_IN_FLIGHT)
                    .try_collect()
                    .await?;
                anyhow::Ok((*timeline, *segno, objects))
            })
            .buffered(SEGMENTS_IN_FLIGHT)
    );
    while let Some((timeline, segno, objects)) = segments.try_next().await? {
        write_segment(&pg_wal, timeline, segno, seg_size, &objects)?;
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
    if standby {
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
        standby,
        "WAL written to pg_wal; start Postgres to replay it"
    );
    Ok(end.unwrap_or(from.redo))
}

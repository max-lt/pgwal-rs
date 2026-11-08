use std::path::{Path, PathBuf};
use std::pin::pin;

use anyhow::{Context, Result, bail};
use clap::Args;
use futures_util::{StreamExt, TryStreamExt};
use pgwal::{
    AfterReplay, ArchiveKey, Lsn, Origin, Recovery, ReplayStart, plan, request_recovery,
    timeline_switches, write_history, write_segment,
};
use tracing::{info, warn};

use pgwal_receiver::WalSink;

use crate::archive::{self, ArchiveArgs};

/// Segments fetched at the same time, and objects of one segment. They bound
/// a restore to 64 reads in flight and 4 segments in memory.
const SEGMENTS_IN_FLIGHT: usize = 4;
const OBJECTS_IN_FLIGHT: usize = 16;

#[derive(Args, Debug)]
pub struct RestoreArgs {
    #[command(flatten)]
    archive: ArchiveArgs,
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

/// What a restore does at a gap of the archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnGap {
    Refuse,
    /// Restore up to the gap.
    Accept,
}

/// Writes the archived WAL that follows the copy into its `pg_wal`. The
/// next start of Postgres replays it. Prints the LSN where the replay ends.
pub async fn run(args: RestoreArgs) -> Result<()> {
    let archive = args.archive.open()?;
    let on_gap = if args.accept_gap {
        OnGap::Accept
    } else {
        OnGap::Refuse
    };
    let after = if args.standby {
        AfterReplay::StayStandby
    } else {
        AfterReplay::Promote
    };
    let end = apply(archive.as_ref(), &args.pgdata, on_gap, after).await?;
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
    let lines: Vec<String> = std::io::stdin().lines().collect::<Result<_, _>>()?;
    let keys = lines
        .iter()
        .filter_map(|line| line.strip_prefix(&args.prefix));
    // A plan from keys only: it does not read the switch points.
    let plan = plan(keys, &start, &[]);
    if let Some(gap) = plan.gap {
        warn!(%gap, "the archive has a gap: a restore stops there");
    }
    for timeline in &plan.histories {
        let key = ArchiveKey::History {
            system_id: start.system_id,
            timeline: *timeline,
        };
        println!("{}{key}", args.prefix);
    }
    for key in plan.segments.iter().flat_map(|segment| &segment.keys) {
        println!("{}{key}", args.prefix);
    }
    Ok(())
}

fn replay_start(pgdata: &Path) -> Result<ReplayStart> {
    ReplayStart::read(pgdata)
        .with_context(|| format!("reading the start point of {}", pgdata.display()))
}

/// Writes the archived WAL that follows the copy in `pgdata` into its
/// `pg_wal`, and returns the LSN where the replay ends.
pub async fn apply(
    archive: &dyn WalSink,
    pgdata: &Path,
    on_gap: OnGap,
    after: AfterReplay,
) -> Result<Lsn> {
    refuse_running(pgdata)?;
    let pg_wal = pgdata.join("pg_wal");
    std::fs::create_dir_all(&pg_wal)?;
    let start = replay_start(pgdata)?;

    let keys = archive.list("").await?;
    let switches = switches(archive, &keys, start.system_id).await?;
    let plan = plan(keys.iter().map(String::as_str), &start, &switches);
    if let Some(gap) = plan.gap {
        if on_gap == OnGap::Refuse {
            bail!("the archive has a gap at {gap}; --accept-gap restores up to it");
        }
        warn!(%gap, "the archive has a gap: the replay stops there");
    }
    if start.origin == Origin::BaseBackup && plan.segments.is_empty() {
        bail!(
            "the archive has no WAL after the backup start {}",
            start.from
        );
    }

    for timeline in &plan.histories {
        let key = ArchiveKey::History {
            system_id: start.system_id,
            timeline: *timeline,
        };
        write_history(
            &pg_wal,
            *timeline,
            &archive::read(archive, &key.to_string()).await?,
        )?;
    }
    let mut segments = pin!(
        futures_util::stream::iter(&plan.segments)
            .map(|segment| async move {
                let objects: Vec<_> = futures_util::stream::iter(&segment.keys)
                    .map(|key| async move {
                        let wal = archive::read(archive, &key.to_string()).await?;
                        anyhow::Ok((key.start, key.end, wal))
                    })
                    .buffered(OBJECTS_IN_FLIGHT)
                    .try_collect()
                    .await?;
                anyhow::Ok((segment, objects))
            })
            .buffered(SEGMENTS_IN_FLIGHT)
    );
    while let Some((segment, objects)) = segments.try_next().await? {
        write_segment(
            &pg_wal,
            segment.timeline,
            segment.segno,
            start.seg_size,
            &objects,
        )?;
    }

    let end = plan.recovery.end();
    let archive_recovery = matches!(plan.recovery, Recovery::Archive { .. });
    if after == AfterReplay::StayStandby || archive_recovery {
        request_recovery(pgdata, after)?;
    }
    info!(
        system_id = start.system_id,
        from = %start.from,
        ?end,
        segments = plan.segments.len(),
        histories = plan.histories.len(),
        archive_recovery,
        ?after,
        "WAL written to pg_wal; start Postgres to replay it"
    );
    Ok(end.unwrap_or(start.from))
}

/// The lines of the newest history file of `system_id` in the archive.
async fn switches(
    archive: &dyn WalSink,
    keys: &[String],
    system_id: u64,
) -> Result<Vec<(u32, Lsn)>> {
    let newest = keys
        .iter()
        .filter_map(|key| match ArchiveKey::parse(key)? {
            ArchiveKey::History {
                system_id: system,
                timeline,
            } if system == system_id => Some(timeline),
            _ => None,
        })
        .max();
    let Some(timeline) = newest else {
        return Ok(Vec::new());
    };
    let key = ArchiveKey::History {
        system_id,
        timeline,
    };
    let history = archive::read(archive, &key.to_string()).await?;
    let history = String::from_utf8(history.to_vec())
        .with_context(|| format!("archived object {key} is not text"))?;
    Ok(timeline_switches(&history))
}

/// Refuses a data directory where Postgres runs.
pub fn refuse_running(pgdata: &Path) -> Result<()> {
    if pgdata.join("postmaster.pid").exists() {
        bail!(
            "{} has a postmaster.pid: stop Postgres, or remove the file of a copy",
            pgdata.display()
        );
    }
    Ok(())
}

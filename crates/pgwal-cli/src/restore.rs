use std::path::{Path, PathBuf};
use std::pin::pin;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::Args;
use futures_util::{StreamExt, TryStreamExt};
use pgwal::{
    AfterReplay, ArchiveKey, Lsn, RangeKey, Recovery, ReplayPlan, ReplayStart, Segment, plan,
    request_recovery, timeline_switches, write_history, write_segment,
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

/// The plan of a restore of the copy in `pgdata`. It writes nothing.
pub struct Prepared {
    pub start: ReplayStart,
    pub plan: ReplayPlan,
}

/// Writes the archived WAL that follows the copy in `pgdata` into its
/// `pg_wal`.
pub async fn apply(
    archive: &dyn WalSink,
    pgdata: &Path,
    on_gap: OnGap,
    after: AfterReplay,
) -> Result<Lsn> {
    let prepared = prepare(archive, pgdata).await?;
    if let Some(gap) = prepared.plan.gap {
        if on_gap == OnGap::Refuse {
            bail!("the archive has a gap at {gap}; --accept-gap restores up to it");
        }
        warn!(%gap, "the archive has a gap: the replay stops there");
    }
    write(archive, pgdata, prepared, after).await
}

/// Reads the copy and plans its replay from the archive.
pub async fn prepare(archive: &dyn WalSink, pgdata: &Path) -> Result<Prepared> {
    refuse_running(pgdata)?;
    let start = replay_start(pgdata)?;
    let keys = archive.list("").await?;
    let switches = switches(archive, &keys, start.system_id).await?;
    let plan = plan(keys.iter().map(String::as_str), &start, &switches);
    // The archive holds the segment of the start of any copy that it serves.
    // An empty plan is the archive of another cluster, or a wrong location.
    if plan.segments.is_empty() {
        bail!(
            "the archive holds no WAL of system {} from {}: check --archive",
            start.system_id,
            start.from
        );
    }

    Ok(Prepared { start, plan })
}

/// The WAL objects of `segment`. Once a segment is whole, the receiver puts
/// it as one object and deletes its parts, also after the plan listed them:
/// a missing part then gives the planned range of that object.
async fn segment_objects(
    archive: &dyn WalSink,
    segment: &Segment,
    seg_size: u64,
) -> Result<Vec<(Lsn, Lsn, Bytes)>> {
    let objects: Vec<_> = futures_util::stream::iter(&segment.keys)
        .map(|key| async move {
            let name = key.to_string();
            let wal = archive
                .get(&name)
                .await
                .with_context(|| format!("reading archived object {name}"))?;
            anyhow::Ok(wal.map(|wal| (key.start, key.end, wal)))
        })
        .buffered(OBJECTS_IN_FLIGHT)
        .try_collect()
        .await?;
    if let Some(objects) = objects.into_iter().collect::<Option<Vec<_>>>() {
        return Ok(objects);
    }
    let (first, last) = (&segment.keys[0], &segment.keys[segment.keys.len() - 1]);
    let seg_start = Lsn(segment.segno * seg_size);
    let prefix = RangeKey::prefix(first.system_id, segment.timeline, seg_start, seg_size);
    let whole = archive
        .list(&prefix)
        .await?
        .iter()
        .filter_map(|key| RangeKey::parse(key))
        .find(|key| {
            (key.system_id, key.timeline) == (first.system_id, segment.timeline)
                && key.start <= first.start
                && key.end >= last.end
        })
        .with_context(|| format!("the archive lost WAL from {} to {}", first.start, last.end))?;
    let wal = archive::read(archive, &whole.to_string()).await?;
    // The replay ends where the plan says.
    let at = |lsn: Lsn| (lsn.0 - whole.start.0) as usize;
    let wal = wal.slice(at(first.start)..at(last.end));
    Ok(vec![(first.start, last.end, wal)])
}

/// Writes the WAL of `prepared` into the `pg_wal` of `pgdata`, asks for the
/// recovery that `after` says, and returns where the replay ends.
pub async fn write(
    archive: &dyn WalSink,
    pgdata: &Path,
    prepared: Prepared,
    after: AfterReplay,
) -> Result<Lsn> {
    let Prepared { start, plan } = prepared;
    let pg_wal = pgdata.join("pg_wal");
    std::fs::create_dir_all(&pg_wal)?;
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
                let objects = segment_objects(archive, segment, start.seg_size).await?;
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
    // A copy of a standby starts as a standby unless its standby.signal
    // goes, and promotes through an archive recovery.
    let archive_recovery =
        matches!(plan.recovery, Recovery::Archive { .. }) || pgdata.join("standby.signal").exists();
    if after == AfterReplay::StayStandby || archive_recovery {
        request_recovery(pgdata, after)?;
    }
    info!(
        system_id = start.system_id,
        from = %start.from,
        end = %end.unwrap_or(start.from),
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

#[cfg(test)]
mod tests {
    use pgwal_receiver::DirSink;

    use super::*;

    const SEG: u64 = 1 << 20;

    fn range(start: u64, end: u64) -> RangeKey {
        RangeKey {
            system_id: 7,
            timeline: 1,
            start: Lsn(start),
            end: Lsn(end),
        }
    }

    /// The receiver puts the whole segment and deletes its parts after the
    /// plan listed them.
    #[tokio::test]
    async fn a_part_gone_in_a_compaction_gives_the_whole_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = DirSink::new(tmp.path());
        let whole = range(SEG, 2 * SEG);
        let wal: Vec<u8> = (0..SEG).map(|i| i as u8).collect();
        archive
            .put(&whole.to_string(), Bytes::from(wal.clone()))
            .await
            .unwrap();
        let segment = Segment {
            timeline: 1,
            segno: 1,
            keys: vec![range(SEG, SEG + 100), range(SEG + 100, SEG + 300)],
        };
        let objects = segment_objects(&archive, &segment, SEG).await.unwrap();
        assert_eq!(
            objects,
            [(Lsn(SEG), Lsn(SEG + 300), Bytes::from(wal[..300].to_vec()))]
        );

        let gone = Segment {
            timeline: 1,
            segno: 2,
            keys: vec![range(2 * SEG, 2 * SEG + 100)],
        };
        assert!(segment_objects(&archive, &gone, SEG).await.is_err());
    }
}

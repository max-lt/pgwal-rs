//! `pgwal rebase`: moves a copy forward with a standby on the archive.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::Args;
use pgwal::{AfterReplay, ClusterState, ControlFile, Lsn};
use tracing::{info, warn};

use crate::archive::ArchiveArgs;
use crate::pg::{Bin, check_interrupted, path, watch_signals};
use crate::restore;
use crate::server::{
    POLL, PgArgs, Role, canonical, check_version, controldata_field, private_dir, start_server,
    tail,
};
use crate::verify::verify;

#[derive(Args, Debug)]
pub struct RebaseArgs {
    #[command(flatten)]
    archive: ArchiveArgs,
    /// The copy to move forward, in place: a base backup, or the output of
    /// an earlier rebase. Postgres must not run on it.
    #[arg(long)]
    pgdata: PathBuf,
    #[command(flatten)]
    pg: PgArgs,
    /// How long the replay can stand still before its position counts as the
    /// end of the archive. The last object can end inside a WAL record.
    #[arg(long, default_value_t = 10)]
    stall_seconds: u64,
}

/// Replays the archive onto the copy with a Postgres that stays in recovery
/// and listens on a private socket only, then stops it cleanly. The copy is
/// then a base backup at the last checkpoint of the primary in the archive.
/// Prints its redo point.
pub async fn run(mut args: RebaseArgs) -> Result<()> {
    args.pgdata = canonical(&args.pgdata)?;
    let pg = Bin(args.pg.pg_bin.clone());
    check_version(&pg, &args.pgdata)?;
    let archive = args.archive.open()?;
    let t = Instant::now();
    let prepared = restore::prepare(archive.as_ref(), &args.pgdata).await?;
    if let Some(gap) = prepared.plan.gap {
        bail!(
            "the archive has a gap at {gap}: a rebase needs the WAL up to the end of the archive"
        );
    }
    let end = prepared.plan.recovery.end().unwrap_or(prepared.start.from);
    // A standby accepts no connection before it is consistent. The check
    // comes before the download.
    if let Some(consistent) = consistency_point(&pg, &args.pgdata)?
        && end < consistent
    {
        bail!(
            "the archive ends at {end}, before the copy is consistent at {consistent}: \
             rebase once the archive holds it"
        );
    }
    let seg_size = prepared.start.seg_size;
    restore::write(
        archive.as_ref(),
        &args.pgdata,
        prepared,
        AfterReplay::StayStandby,
    )
    .await?;
    info!(
        seconds = t.elapsed().as_secs_f64(),
        "archive written to pg_wal"
    );
    let before = ControlFile::read(&args.pgdata)?.redo;
    // From here a signal stops the private server and removes its files.
    watch_signals();

    let socket = private_dir("rebase")?;
    let result = replay(&pg, &args, &socket, end, seg_size);
    let _ = std::fs::remove_dir_all(&socket);
    let reached = result?;

    std::fs::remove_file(args.pgdata.join("standby.signal"))?;
    // The manifest of pgwal fetch no longer matches the files.
    for stale in ["backup_label.old", "backup_manifest"] {
        match std::fs::remove_file(args.pgdata.join(stale)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
    }
    let control = ControlFile::read(&args.pgdata)?;
    if control.state != ClusterState::ShutDownInRecovery {
        bail!("the copy is not shut down in recovery: {:?}", control.state);
    }
    verify(&pg, &args.pgdata, &args.pg.user)?;
    if control.redo == before {
        warn!(
            redo = %control.redo,
            "the archive holds no checkpoint after the copy: the redo point stays"
        );
    }
    info!(
        before = %before,
        redo = %control.redo,
        replayed = %reached,
        timeline = control.timeline,
        "rebased"
    );
    println!("{}", control.redo);
    Ok(())
}

/// Where the copy becomes consistent: the end of its backup, from
/// `backup_manifest`, or the minimum recovery point of a rebased copy.
fn consistency_point(pg: &Bin, pgdata: &Path) -> Result<Option<Lsn>> {
    if let Ok(manifest) = std::fs::read_to_string(pgdata.join("backup_manifest")) {
        // "WAL-Ranges": [{ "Timeline": 1, "Start-LSN": "0/2000028", "End-LSN": "0/2000120" }]
        // A backup across a promotion has one range per timeline.
        let end = manifest
            .split("\"End-LSN\"")
            .skip(1)
            .filter_map(|rest| rest.split('"').nth(1)?.parse::<Lsn>().ok())
            .max();
        return Ok(end);
    }
    let data = pg.run("pg_controldata", &[path(pgdata)])?;
    Ok(controldata_field(&data, "Minimum recovery ending location")
        .and_then(|lsn| lsn.parse().ok())
        .filter(|lsn: &Lsn| lsn.0 != 0))
}

/// Starts the server, waits for the replay to reach `end`, and stops it.
/// Returns the replay position.
fn replay(pg: &Bin, args: &RebaseArgs, socket: &Path, end: Lsn, seg_size: u64) -> Result<Lsn> {
    let stall = Duration::from_secs(args.stall_seconds);
    'server: loop {
        let server = start_server(pg, &args.pgdata, socket, &args.pg.user, Role::Standby)?;
        let mut last = (Lsn(0), Instant::now());
        loop {
            let lsn = match server.replay_lsn() {
                Ok(lsn) => lsn,
                Err(e) => {
                    check_interrupted()?;
                    if pg
                        .run("pg_ctl", &["-D", path(&args.pgdata), "status"])
                        .is_err()
                    {
                        bail!(
                            "Postgres stopped during the replay: {e}\n{}",
                            tail(&server.log())
                        );
                    }
                    return Err(e);
                }
            };
            if lsn >= end {
                server.stop()?;
                return Ok(lsn);
            }
            if lsn != last.0 {
                last = (lsn, Instant::now());
            }
            if last.1.elapsed() < stall {
                std::thread::sleep(POLL);
                continue;
            }
            let paused = server.query("SELECT pg_get_wal_replay_pause_state()")?;
            if paused != "not paused" {
                // A setting raised on the primary pauses the replay. The
                // replay wrote the new value into pg_control, and the next
                // start passes it.
                if !server.has_raised_settings()? {
                    bail!("the replay is {paused} at {lsn}:\n{}", tail(&server.log()));
                }
                warn!(replayed = %lsn, "the replay paused on a raised setting: starting again");
                server.stop()?;
                continue 'server;
            }
            // The end of the archive: the startup process waits for more WAL.
            let waits = server
                .query("SELECT wait_event FROM pg_stat_activity WHERE backend_type = 'startup'")?;
            if waits == "RecoveryRetrieveRetryInterval" {
                // A standby takes an unreadable record for the end of the WAL.
                // The last object can end inside a record, but not a segment
                // before the end.
                if end.0 - lsn.0 > seg_size {
                    bail!(
                        "the replay stops at {lsn}, before the end of the archive at {end}: \
                         the archived WAL there is unreadable"
                    );
                }
                if lsn < end {
                    warn!(
                        replayed = %lsn,
                        archive_end = %end,
                        "the replay ends before the end of the archive: the last object ends inside a record"
                    );
                }
                server.stop()?;
                return Ok(lsn);
            }
            last.1 = Instant::now();
        }
    }
}

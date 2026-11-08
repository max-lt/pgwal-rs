use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Args;
use pgwal::{AfterReplay, ClusterState, ControlFile, Lsn};
use tracing::{info, warn};

use crate::archive::ArchiveArgs;
use crate::restore::{self, OnGap};

/// Port of the private socket. The server listens on no TCP port.
const PORT: &str = "5432";
const POLL: Duration = Duration::from_millis(200);

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
pub async fn run(args: RebaseArgs) -> Result<()> {
    let pg = Bin(args.pg.pg_bin.clone());
    check_version(&pg, &args.pgdata)?;
    let archive = args.archive.open()?;
    let t = Instant::now();
    let end = restore::apply(
        archive.as_ref(),
        &args.pgdata,
        OnGap::Refuse,
        AfterReplay::StayStandby,
    )
    .await?;
    info!(
        seconds = t.elapsed().as_secs_f64(),
        "archive written to pg_wal"
    );
    let before = ControlFile::read(&args.pgdata)?.redo;

    let socket = private_dir("rebase")?;
    let result = replay(&pg, &args, &socket, end);
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

/// Starts the server, waits for the replay to reach `end`, and stops it.
/// Returns the replay position.
fn replay(pg: &Bin, args: &RebaseArgs, socket: &Path, end: Lsn) -> Result<Lsn> {
    let server = start_server(pg, &args.pgdata, socket, &args.pg.user, Role::Standby)?;
    let log = server.log();

    let stall = Duration::from_secs(args.stall_seconds);
    let mut last = (Lsn(0), Instant::now());
    let reached = loop {
        let lsn = match server
            .query("SELECT pg_last_wal_replay_lsn()")
            .and_then(|out| {
                out.parse()
                    .map_err(|e| anyhow::anyhow!("pg_last_wal_replay_lsn: {e}"))
            }) {
            Ok(lsn) => lsn,
            Err(e)
                if pg
                    .run("pg_ctl", &["-D", path(&args.pgdata), "status"])
                    .is_err() =>
            {
                bail!("Postgres stopped during the replay: {e}\n{}", tail(&log));
            }
            Err(e) => return Err(e),
        };
        if lsn >= end {
            break lsn;
        }
        if lsn != last.0 {
            last = (lsn, Instant::now());
        } else if last.1.elapsed() >= stall {
            // Recovery pauses on a setting lower than on the primary.
            let paused = server.query("SELECT pg_get_wal_replay_pause_state()")?;
            if paused != "not paused" {
                bail!("the replay is {paused} at {lsn}:\n{}", tail(&log));
            }
            warn!(
                replayed = %lsn,
                archive_end = %end,
                "the replay stands still before the end of the archive: the last object ends inside a record"
            );
            break lsn;
        }
        std::thread::sleep(POLL);
    };
    server.stop()?;
    Ok(reached)
}

#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// The copy to check. Postgres must not run on it.
    #[arg(long)]
    pgdata: PathBuf,
    #[command(flatten)]
    pg: PgArgs,
}

/// The Postgres that runs on a copy.
#[derive(Args, Debug)]
struct PgArgs {
    /// Directory of the Postgres binaries, of the major version of the copy.
    /// Without it, they come from PATH.
    #[arg(long, env = "PGWAL_PG_BIN")]
    pg_bin: Option<PathBuf>,
    /// A superuser of the copy, for the local connections. It does not read
    /// PGUSER, which names the replication role of the other commands.
    #[arg(long, default_value = "postgres")]
    user: String,
}

pub fn run_verify(args: VerifyArgs) -> Result<()> {
    let pg = Bin(args.pg.pg_bin);
    check_version(&pg, &args.pgdata)?;
    verify(&pg, &args.pgdata, &args.pg.user)
}

/// Checks a stopped copy: its page checksums when it has them, then
/// `pg_amcheck` on the heap and the B-tree indexes of every database. The
/// amcheck pass runs on a scratch copy, because a server on the copy itself
/// would end its recovery and write WAL of its own.
pub fn verify(pg: &Bin, pgdata: &Path, user: &str) -> Result<()> {
    restore::refuse_running(pgdata)?;
    let t = Instant::now();
    let data = pg.run("pg_controldata", &[path(pgdata)])?;
    let checksums = data
        .lines()
        .find_map(|line| line.strip_prefix("Data page checksum version:"))
        .is_some_and(|version| version.trim() != "0");
    if checksums {
        pg.run("pg_checksums", &["--check", "-D", path(pgdata)])
            .context("the page checksums of the copy are wrong")?;
    }

    let name = pgdata.file_name().context("a data directory has a name")?;
    let scratch = pgdata.with_file_name(format!(
        ".{}.verify-{}",
        name.to_string_lossy(),
        std::process::id()
    ));
    let socket = private_dir("verify")?;
    let result = amcheck(pg, pgdata, &scratch, &socket, user);
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&socket);
    result?;
    info!(
        checksums,
        seconds = t.elapsed().as_secs_f64(),
        "copy verified"
    );
    Ok(())
}

/// Runs `pg_amcheck` on a scratch copy of `pgdata`, served on `socket`.
fn amcheck(pg: &Bin, pgdata: &Path, scratch: &Path, socket: &Path, user: &str) -> Result<()> {
    copy_dir(pgdata, scratch)?;
    for signal in ["standby.signal", "recovery.signal"] {
        let _ = std::fs::remove_file(scratch.join(signal));
    }
    let server = start_server(pg, scratch, socket, user, Role::Scratch)?;
    let amcheck = pg.run(
        "pg_amcheck",
        &[
            "-h",
            path(socket),
            "-p",
            PORT,
            "-U",
            user,
            "--all",
            "--install-missing",
            "--heapallindexed",
        ],
    );
    server.stop()?;
    amcheck.context("pg_amcheck found corruption in the copy")?;
    Ok(())
}

/// A directory that only this user can enter, for the socket, the log and
/// the trust rules of a private server. It must not exist yet.
fn private_dir(purpose: &str) -> Result<PathBuf> {
    // A short path: a Unix socket path holds about 100 bytes.
    let dir = PathBuf::from(format!("/tmp/pgwal-{purpose}-{}", std::process::id()));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// What a private server is for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A standby that replays the archive onto the copy.
    Standby,
    /// A primary on a scratch copy, which goes away.
    Scratch,
}

/// Starts Postgres on `pgdata`, on a socket in the private directory `dir`
/// only, with local connections trusted. The copy keeps its own
/// `pg_hba.conf`. The server archives nothing, follows no primary, and
/// logs to `dir`.
fn start_server<'a>(
    pg: &'a Bin,
    pgdata: &'a Path,
    dir: &'a Path,
    user: &'a str,
    role: Role,
) -> Result<Server<'a>> {
    let dir_name = path(dir);
    std::fs::write(dir.join("pg_hba.conf"), "local all all trust\n")?;
    let mut opts = format!(
        "-c listen_addresses= -k {dir_name} -p {PORT} -c unix_socket_permissions=0700 \
         -c hba_file={dir_name}/pg_hba.conf -c hot_standby=on -c archive_mode=off \
         -c primary_conninfo= -c primary_slot_name= -c logging_collector=off"
    );
    // A scratch primary runs no job of the extensions of the copy.
    if role == Role::Scratch {
        opts.push_str(" -c shared_preload_libraries=");
    }
    // A copy of a Debian primary has no postgresql.conf. The settings that a
    // standby needs at least as high as on the primary come from pg_control.
    if !pgdata.join("postgresql.conf").exists() {
        std::fs::write(dir.join("postgresql.conf"), "")?;
        std::fs::write(dir.join("pg_ident.conf"), "")?;
        opts.push_str(&format!(
            " -c config_file={dir_name}/postgresql.conf -c ident_file={dir_name}/pg_ident.conf"
        ));
        let data = pg.run("pg_controldata", &[path(pgdata)])?;
        for (field, setting) in PRIMARY_SETTINGS {
            let value = data
                .lines()
                .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
                .with_context(|| format!("pg_controldata has no {field}"))?;
            opts.push_str(&format!(" -c {setting}={}", value.trim()));
        }
    }
    let server = Server {
        pg,
        pgdata,
        dir,
        user,
    };
    let log = server.log();
    // A standby accepts connections once it is consistent, which can take
    // a whole checkpoint cycle of WAL.
    pg.run(
        "pg_ctl",
        &[
            "-D",
            path(pgdata),
            "-l",
            path(&log),
            "-o",
            &opts,
            "-t",
            START_TIMEOUT_SECONDS,
            "-w",
            "start",
        ],
    )
    .with_context(|| format!("starting Postgres:\n{}", tail(&log)))?;
    Ok(server)
}

/// How long `pg_ctl start` waits. It stops waiting when the server exits.
const START_TIMEOUT_SECONDS: &str = "86400";

/// The `pg_controldata` field of each setting a standby needs at least as
/// high as on the primary.
const PRIMARY_SETTINGS: [(&str, &str); 5] = [
    ("max_connections setting", "max_connections"),
    ("max_worker_processes setting", "max_worker_processes"),
    ("max_wal_senders setting", "max_wal_senders"),
    ("max_prepared_xacts setting", "max_prepared_transactions"),
    ("max_locks_per_xact setting", "max_locks_per_transaction"),
];

/// Copies a directory tree, with the targets of its links: a scratch
/// server must not write into the tablespaces or the WAL directory of the
/// copy. `fs::copy` clones each file where the file system can, as APFS,
/// btrfs and XFS do.
fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    std::fs::set_permissions(to, std::fs::metadata(from)?.permissions())?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// The Postgres binaries: in a directory, or on PATH.
pub struct Bin(Option<PathBuf>);

impl Bin {
    /// The output of `pg_controldata` is translated in other locales.
    fn command(&self, tool: &str) -> Command {
        let mut command = match &self.0 {
            Some(dir) => Command::new(dir.join(tool)),
            None => Command::new(tool),
        };
        command.env("LC_ALL", "C");
        command
    }

    /// Runs `tool`, and returns its stdout. A failure carries its stderr.
    fn run(&self, tool: &str, args: &[&str]) -> Result<String> {
        let out = self
            .command(tool)
            .args(args)
            .output()
            .with_context(|| format!("running {tool}"))?;
        if !out.status.success() {
            bail!(
                "{tool} failed: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }
}

/// Refuses binaries of another major version than the copy.
fn check_version(pg: &Bin, pgdata: &Path) -> Result<()> {
    let copy = std::fs::read_to_string(pgdata.join("PG_VERSION"))
        .with_context(|| format!("reading {}/PG_VERSION", pgdata.display()))?;
    let copy = copy.trim();
    // postgres (PostgreSQL) 18.6 (Debian 18.6-1.pgdg12+2)
    let version = pg.run("postgres", &["--version"])?;
    let binary = version
        .split_whitespace()
        .nth(2)
        .and_then(|v| v.split('.').next())
        .unwrap_or_default();
    if binary != copy {
        bail!(
            "the copy is Postgres {copy}, the binaries are {}",
            version.trim()
        );
    }
    Ok(())
}

/// A running server, stopped on drop if `stop` did not run.
struct Server<'a> {
    pg: &'a Bin,
    pgdata: &'a Path,
    /// The private directory of the socket and the log.
    dir: &'a Path,
    user: &'a str,
}

impl Server<'_> {
    fn log(&self) -> PathBuf {
        self.dir.join("postgres.log")
    }

    /// The one value that `sql` returns.
    fn query(&self, sql: &str) -> Result<String> {
        let out = self.pg.run(
            "psql",
            &[
                "-X",
                "-A",
                "-t",
                "-h",
                path(self.dir),
                "-p",
                PORT,
                "-U",
                self.user,
                "-d",
                "postgres",
                "-c",
                sql,
            ],
        )?;
        Ok(out.trim().to_owned())
    }

    /// A fast stop of a standby writes a restartpoint at the last checkpoint
    /// it replayed.
    fn stop(self) -> Result<()> {
        let pg = self.pg;
        let pgdata = self.pgdata;
        std::mem::forget(self);
        pg.run("pg_ctl", &["-D", path(pgdata), "-m", "fast", "-w", "stop"])?;
        Ok(())
    }
}

impl Drop for Server<'_> {
    fn drop(&mut self) {
        let _ = self.pg.run(
            "pg_ctl",
            &["-D", path(self.pgdata), "-m", "immediate", "-w", "stop"],
        );
    }
}

fn tail(log: &Path) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}

fn path(p: &Path) -> &str {
    p.to_str().expect("a UTF-8 path")
}

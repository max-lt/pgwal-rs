use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Args;
use pgwal::{ControlFile, Lsn};
use tracing::{info, warn};

use crate::restore;

/// Port of the private socket. The server listens on no TCP port.
const PORT: &str = "5432";
const POLL: Duration = Duration::from_millis(200);

#[derive(Args, Debug)]
pub struct RebaseArgs {
    /// Directory of the archived objects, as `pgwal receive --archive` writes
    /// it.
    #[arg(long)]
    archive: PathBuf,
    /// The copy to move forward, in place: a base backup, or the output of
    /// an earlier rebase. Postgres must not run on it.
    #[arg(long)]
    pgdata: PathBuf,
    /// Directory of the Postgres binaries, of the major version of the copy.
    /// Without it, they come from PATH.
    #[arg(long, env = "PGWAL_PG_BIN")]
    pg_bin: Option<PathBuf>,
    /// A superuser of the copy, for the local connection that watches the
    /// replay.
    #[arg(long, env = "PGUSER", default_value = "postgres")]
    user: String,
    /// How long the replay can stand still before its position counts as the
    /// end of the archive. The last object can end inside a WAL record.
    #[arg(long, default_value_t = 10)]
    stall_seconds: u64,
}

/// Replays the archive onto the copy with a Postgres that stays in recovery
/// and listens on a private socket only, then stops it cleanly. The copy is
/// then a base backup at the last checkpoint of the primary in the archive.
/// Prints its redo point.
pub fn run(args: RebaseArgs) -> Result<()> {
    let pg = Bin(args.pg_bin.clone());
    check_version(&pg, &args.pgdata)?;
    let end = restore::apply(&args.archive, &args.pgdata, false, true)?;
    let before = ControlFile::read(&args.pgdata)?.redo;

    let socket = PathBuf::from(format!("/tmp/pgwal-rebase-{}", std::process::id()));
    std::fs::create_dir_all(&socket)?;
    let result = replay(&pg, &args, &socket, end);
    let _ = std::fs::remove_dir_all(&socket);
    let reached = result?;

    std::fs::remove_file(args.pgdata.join("standby.signal"))?;
    let control = ControlFile::read(&args.pgdata)?;
    let data = controldata(&pg, &args.pgdata)?;
    if field(&data, "Database cluster state") != Some("shut down in recovery") {
        bail!("the copy is not shut down in recovery:\n{data}");
    }
    verify(&pg, &args.pgdata, &args.user)?;
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
    let _hba = TrustLocal::install(&args.pgdata)?;
    let log = socket.join("postgres.log");
    let server = start_server(pg, &args.pgdata, socket, &log)?;

    let stall = Duration::from_secs(args.stall_seconds);
    let mut last = (Lsn(0), Instant::now());
    let reached = loop {
        let lsn = match server.replay_lsn(socket, &args.user) {
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
    /// Directory of the Postgres binaries, of the major version of the copy.
    /// Without it, they come from PATH.
    #[arg(long, env = "PGWAL_PG_BIN")]
    pg_bin: Option<PathBuf>,
    /// A superuser of the copy.
    #[arg(long, env = "PGUSER", default_value = "postgres")]
    user: String,
}

pub fn run_verify(args: VerifyArgs) -> Result<()> {
    let pg = Bin(args.pg_bin);
    check_version(&pg, &args.pgdata)?;
    verify(&pg, &args.pgdata, &args.user)
}

/// Checks a stopped copy: its page checksums when it has them, then
/// `pg_amcheck` on the heap and the B-tree indexes of every database. The
/// amcheck pass runs on a scratch copy, because a server on the copy itself
/// would end its recovery and write WAL of its own.
pub fn verify(pg: &Bin, pgdata: &Path, user: &str) -> Result<()> {
    if pgdata.join("postmaster.pid").exists() {
        bail!(
            "{} has a postmaster.pid: stop Postgres first",
            pgdata.display()
        );
    }
    let t = Instant::now();
    let data = controldata(pg, pgdata)?;
    let checksums = field(&data, "Data page checksum version").is_some_and(|v| v != "0");
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
    let socket = PathBuf::from(format!("/tmp/pgwal-verify-{}", std::process::id()));
    let result = (|| {
        copy_dir(pgdata, &scratch)?;
        for signal in ["standby.signal", "recovery.signal"] {
            let _ = std::fs::remove_file(scratch.join(signal));
        }
        std::fs::write(scratch.join("pg_hba.conf"), "local all all trust\n")?;
        std::fs::create_dir_all(&socket)?;
        let log = socket.join("postgres.log");
        let server = start_server(pg, &scratch, &socket, &log)?;
        let amcheck = pg.run(
            "pg_amcheck",
            &[
                "-h",
                path(&socket),
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
        amcheck.context("pg_amcheck found corruption in the copy")
    })();
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

/// Starts Postgres on `pgdata`, on the private socket in `socket` only.
fn start_server<'a>(
    pg: &'a Bin,
    pgdata: &'a Path,
    socket: &Path,
    log: &Path,
) -> Result<Server<'a>> {
    let opts = format!(
        "-c listen_addresses= -k {} -p {PORT} -c hot_standby=on",
        path(socket)
    );
    let started = pg.run(
        "pg_ctl",
        &[
            "-D",
            path(pgdata),
            "-l",
            path(log),
            "-o",
            &opts,
            "-w",
            "start",
        ],
    );
    let server = Server { pg, pgdata };
    started.with_context(|| format!("starting Postgres:\n{}", tail(log)))?;
    Ok(server)
}

/// Copies a directory tree. `fs::copy` clones each file where the file
/// system can, as APFS, btrfs and XFS do.
fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    std::fs::set_permissions(to, std::fs::metadata(from)?.permissions())?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else if kind.is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(entry.path())?, &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// The Postgres binaries: in a directory, or on PATH.
pub struct Bin(Option<PathBuf>);

impl Bin {
    fn command(&self, tool: &str) -> Command {
        match &self.0 {
            Some(dir) => Command::new(dir.join(tool)),
            None => Command::new(tool),
        }
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
}

impl Server<'_> {
    fn replay_lsn(&self, socket: &Path, user: &str) -> Result<Lsn> {
        let out = self.pg.run(
            "psql",
            &[
                "-X",
                "-A",
                "-t",
                "-h",
                path(socket),
                "-p",
                PORT,
                "-U",
                user,
                "-d",
                "postgres",
                "-c",
                "SELECT pg_last_wal_replay_lsn()",
            ],
        )?;
        out.trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("pg_last_wal_replay_lsn: {e}"))
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

/// Lets local connections in without a password while the replay runs,
/// and puts the `pg_hba.conf` of the copy back on drop.
struct TrustLocal {
    path: PathBuf,
    original: Vec<u8>,
}

impl TrustLocal {
    fn install(pgdata: &Path) -> Result<TrustLocal> {
        let path = pgdata.join("pg_hba.conf");
        let original = std::fs::read(&path)?;
        std::fs::write(&path, "local all all trust\n")?;
        Ok(TrustLocal { path, original })
    }
}

impl Drop for TrustLocal {
    fn drop(&mut self) {
        if let Err(e) = std::fs::write(&self.path, &self.original) {
            warn!(path = %self.path.display(), "putting pg_hba.conf back failed: {e}");
        }
    }
}

fn controldata(pg: &Bin, pgdata: &Path) -> Result<String> {
    pg.run("pg_controldata", &[path(pgdata)])
}

/// The value of `name` in `pg_controldata` output.
fn field<'a>(data: &'a str, name: &str) -> Option<&'a str> {
    data.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
        .map(str::trim)
}

fn tail(log: &Path) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}

fn path(p: &Path) -> &str {
    p.to_str().expect("a UTF-8 path")
}

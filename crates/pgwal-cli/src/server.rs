//! The private Postgres servers of rebase and verify: on a socket in a
//! directory of mode 0700, with no archiving and no primary.

use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::Args;
use pgwal::Lsn;

use crate::pg::{Bin, OnSignal, check_interrupted, path};

/// Port of the private socket. The server listens on no TCP port.
pub const PORT: &str = "5432";

pub const POLL: Duration = Duration::from_millis(200);

/// The Postgres that runs on a copy.
#[derive(Args, Debug)]
pub struct PgArgs {
    /// Directory of the Postgres binaries, of the major version of the copy.
    /// Without it, they come from PATH.
    #[arg(long, env = "PGWAL_PG_BIN")]
    pub pg_bin: Option<PathBuf>,
    /// A superuser of the copy, for the local connections. It does not read
    /// PGUSER, which names the replication role of the other commands.
    #[arg(long, default_value = "postgres")]
    pub user: String,
}

/// Canonical form of `pgdata`: `.` has no file name for the scratch copy.
pub fn canonical(pgdata: &Path) -> Result<PathBuf> {
    pgdata
        .canonicalize()
        .with_context(|| format!("reading {}", pgdata.display()))
}

/// A directory that only this user can enter, for the socket, the log and
/// the trust rules of a private server. It must not exist yet.
pub fn private_dir(purpose: &str) -> Result<PathBuf> {
    // A short path: a Unix socket path holds about 100 bytes. The time in
    // the name keeps a later run with the same pid off a leftover.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .subsec_nanos();
    let dir = PathBuf::from(format!(
        "/tmp/pgwal-{purpose}-{}-{nanos:08x}",
        std::process::id()
    ));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// What a private server is for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// A standby that replays the archive onto the copy.
    Standby,
    /// A primary on a scratch copy, which goes away.
    Scratch,
}

/// Starts Postgres on `pgdata`, on a socket in the private directory `dir`
/// only, with local connections trusted. The copy keeps its own
/// `pg_hba.conf`. The server archives nothing, follows no primary, and
/// logs to `dir`.
pub fn start_server<'a>(
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
         -c archive_cleanup_command= -c recovery_end_command= -c primary_conninfo= \
         -c primary_slot_name= -c synchronous_standby_names= \
         -c max_logical_replication_workers=0 -c logging_collector=off \
         -c recovery_min_apply_delay=0 -c ssl=off -c huge_pages=off -c lc_messages=C \
         -c log_destination=stderr -c log_min_messages=warning"
    );
    // A scratch primary runs no job of the extensions of the copy.
    if role == Role::Scratch {
        opts.push_str(" -c shared_preload_libraries=");
    }
    // A copy of a Debian primary has no postgresql.conf.
    if !pgdata.join("postgresql.conf").exists() {
        std::fs::write(dir.join("postgresql.conf"), "")?;
        std::fs::write(dir.join("pg_ident.conf"), "")?;
        opts.push_str(&format!(
            " -c config_file={dir_name}/postgresql.conf -c ident_file={dir_name}/pg_ident.conf"
        ));
    }
    // The settings that a standby needs at least as high as on the primary
    // come from pg_control, which holds the newest values the replay saw.
    let data = pg.run("pg_controldata", &[path(pgdata)])?;
    for (field, setting) in PRIMARY_SETTINGS {
        let value = controldata_field(&data, field)
            .with_context(|| format!("pg_controldata has no {field}"))?;
        opts.push_str(&format!(" -c {setting}={value}"));
    }
    let server = Server {
        pg,
        pgdata,
        dir,
        user,
    };
    let log = server.log();
    pg.run(
        "pg_ctl",
        &[
            "-D",
            path(pgdata),
            "-l",
            path(&log),
            "-o",
            &opts,
            "-W",
            "start",
        ],
    )
    .with_context(|| format!("starting Postgres:\n{}", tail(&log)))?;
    server.wait_ready()?;
    Ok(server)
}

/// The value of `field` in the output of `pg_controldata`.
pub fn controldata_field<'a>(data: &'a str, field: &str) -> Option<&'a str> {
    data.lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .map(str::trim)
}

/// How long a standby that waits for WAL gets to accept a connection.
const READY_GRACE: Duration = Duration::from_secs(3);

/// How long `pg_ctl stop` waits. It stops waiting when the server exits.
pub const PG_CTL_STOP_SECONDS: &str = "86400";

/// The `pg_controldata` field of each setting a standby needs at least as
/// high as on the primary.
pub const PRIMARY_SETTINGS: [(&str, &str); 5] = [
    ("max_connections setting", "max_connections"),
    ("max_worker_processes setting", "max_worker_processes"),
    ("max_wal_senders setting", "max_wal_senders"),
    ("max_prepared_xacts setting", "max_prepared_transactions"),
    ("max_locks_per_xact setting", "max_locks_per_transaction"),
];

/// Refuses binaries of another major version than the copy.
pub fn check_version(pg: &Bin, pgdata: &Path) -> Result<()> {
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
pub struct Server<'a> {
    pg: &'a Bin,
    pgdata: &'a Path,
    /// The private directory of the socket and the log.
    dir: &'a Path,
    user: &'a str,
}

impl Server<'_> {
    pub fn log(&self) -> PathBuf {
        self.dir.join("postgres.log")
    }

    /// Waits until the server accepts connections. A standby does once it is
    /// consistent, which can take a whole checkpoint cycle of WAL. A standby
    /// that waits for WAL before that never will: the archive ends too early.
    pub fn wait_ready(&self) -> Result<()> {
        let started = Instant::now();
        let mut waiting_since = None;
        loop {
            check_interrupted()?;
            let ready = self.pg.exit_code(
                "pg_isready",
                &["-h", path(self.dir), "-p", PORT, "-U", self.user],
            )?;
            // 0: accepts connections. 1: starts up. 2: no answer yet.
            if ready == Some(0) {
                self.query("SELECT 1")
                    .context("the server is ready, and a query of --user fails")?;
                return Ok(());
            }
            let log = std::fs::read_to_string(self.log()).unwrap_or_default();
            // pg_ctl -W returns before the server writes postmaster.pid.
            if started.elapsed() >= READY_GRACE
                && self
                    .pg
                    .run("pg_ctl", &["-D", path(self.pgdata), "status"])
                    .is_err()
            {
                bail!("Postgres did not start:\n{}", tail(&self.log()));
            }
            // A standby can wait for WAL right after it is consistent: the
            // connection gets a moment to tell.
            if log.contains("waiting for WAL to become available") {
                let since = *waiting_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= READY_GRACE {
                    bail!(
                        "the archive ends before the copy is consistent:\n{}",
                        tail(&self.log())
                    );
                }
            }
            std::thread::sleep(POLL);
        }
    }

    pub fn replay_lsn(&self) -> Result<Lsn> {
        self.query("SELECT pg_last_wal_replay_lsn()")?
            .parse()
            .map_err(|e| anyhow::anyhow!("pg_last_wal_replay_lsn: {e}"))
    }

    /// A setting of pg_control is higher than the one the server runs with.
    pub fn has_raised_settings(&self) -> Result<bool> {
        let data = self.pg.run("pg_controldata", &[path(self.pgdata)])?;
        for (field, setting) in PRIMARY_SETTINGS {
            let needed: u64 = controldata_field(&data, field)
                .and_then(|value| value.parse().ok())
                .with_context(|| format!("pg_controldata has no {field}"))?;
            let running: u64 = self
                .query(&format!("SELECT current_setting('{setting}')"))?
                .parse()
                .with_context(|| format!("current_setting of {setting}"))?;
            if needed > running {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The one value that `sql` returns.
    pub fn query(&self, sql: &str) -> Result<String> {
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
    pub fn stop(self) -> Result<()> {
        let pg = self.pg;
        let pgdata = self.pgdata;
        std::mem::forget(self);
        // The restartpoint of the stop can write every dirty buffer. A
        // signal must not stop pg_ctl, or the server goes on on the copy.
        pg.exec(
            "pg_ctl",
            &[
                "-D",
                path(pgdata),
                "-m",
                "fast",
                "-t",
                PG_CTL_STOP_SECONDS,
                "-w",
                "stop",
            ],
            OnSignal::Finish,
        )?;
        Ok(())
    }
}

impl Drop for Server<'_> {
    fn drop(&mut self) {
        let _ = self.pg.exec(
            "pg_ctl",
            &["-D", path(self.pgdata), "-m", "immediate", "-w", "stop"],
            OnSignal::Finish,
        );
    }
}

pub fn tail(log: &Path) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}

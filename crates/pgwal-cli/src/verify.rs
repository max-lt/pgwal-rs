//! `pgwal verify`: page checksums and pg_amcheck on a stopped copy.

use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Args;
use pgwal::{ClusterState, ControlFile};
use tracing::info;

use crate::pg::{Bin, check_interrupted, path, watch_signals};
use crate::restore;
use crate::server::{
    PORT, PgArgs, Role, canonical, check_version, controldata_field, private_dir, start_server,
};

#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// The copy to check. Postgres must not run on it.
    #[arg(long)]
    pgdata: PathBuf,
    #[command(flatten)]
    pg: PgArgs,
}

pub fn run_verify(args: VerifyArgs) -> Result<()> {
    watch_signals();
    let pgdata = canonical(&args.pgdata)?;
    let pg = Bin(args.pg.pg_bin);
    check_version(&pg, &pgdata)?;
    verify(&pg, &pgdata, &args.pg.user)
}

/// Checks a stopped copy: its page checksums when it has them, then
/// `pg_amcheck` on the heap and the B-tree indexes of every database. The
/// amcheck pass runs on a scratch copy, because a server on the copy itself
/// would end its recovery and write WAL of its own.
pub fn verify(pg: &Bin, pgdata: &Path, user: &str) -> Result<()> {
    restore::refuse_running(pgdata)?;
    let state = ControlFile::read(pgdata)?.state;
    if state == ClusterState::Running {
        bail!(
            "{} did not stop cleanly: pgwal verify checks a copy after a clean stop, \
             such as the output of pgwal rebase",
            pgdata.display()
        );
    }
    let t = Instant::now();
    let data = pg.run("pg_controldata", &[path(pgdata)])?;
    let checksums = controldata_field(&data, "Data page checksum version")
        .is_some_and(|version| version != "0");
    if checksums {
        let checked = pg.run("pg_checksums", &["--check", "-D", path(pgdata)]);
        check_interrupted()?;
        checked.context("the page checksums of the copy are wrong")?;
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
    // Another user must not get the copy through a directory made first.
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(scratch)
        .with_context(|| format!("creating {}", scratch.display()))?;
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
    check_interrupted()?;
    amcheck.context("pg_amcheck found corruption in the copy")?;
    Ok(())
}

/// Copies a directory tree, with the targets of its links: a scratch
/// server must not write into the tablespaces or the WAL directory of the
/// copy. `fs::copy` clones each file where the file system can, as APFS,
/// btrfs and XFS do.
fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    std::fs::set_permissions(to, std::fs::metadata(from)?.permissions())?;
    for entry in std::fs::read_dir(from)? {
        check_interrupted()?;
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

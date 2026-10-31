//! Runs `pgwal receive` against a Postgres server that asks for
//! SCRAM-SHA-256 over TCP.
//!
//! Skipped when no Postgres binaries are found: set `PGWAL_PG_BIN` to their
//! directory.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

const PASSWORD: &str = "s3cret";

fn pg_bin() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("PGWAL_PG_BIN") {
        return Some(dir.into());
    }
    let out = Command::new("which").arg("initdb").output().ok()?;
    if !out.status.success() {
        return None;
    }
    Path::new(String::from_utf8_lossy(&out.stdout).trim())
        .parent()
        .map(Path::to_path_buf)
}

fn run(bin: &Path, tool: &str, args: &[&str]) {
    let out = Command::new(bin.join(tool)).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{tool} {args:?} failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Stops the server when the test ends, also on a panic.
struct Server<'a> {
    bin: &'a Path,
    data: PathBuf,
}

impl Drop for Server<'_> {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .args([
                "-D",
                self.data.to_str().unwrap(),
                "-m",
                "immediate",
                "-w",
                "stop",
            ])
            .output();
    }
}

/// Kills `pgwal` when the test ends, also on a panic.
struct Receiver(std::process::Child);

impl Drop for Receiver {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn objects_under(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .map(|e| e.unwrap().path())
        .map(|p| match p.is_dir() {
            true => objects_under(&p),
            false => 1,
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_archives_wal_with_scram_auth() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let pwfile = root.path().join("pw");
    std::fs::write(&pwfile, PASSWORD).unwrap();
    run(
        &bin,
        "initdb",
        &[
            "-D",
            data.to_str().unwrap(),
            "-U",
            "postgres",
            "--auth=scram-sha-256",
            "--pwfile",
            pwfile.to_str().unwrap(),
        ],
    );
    let port = free_port();
    let opts = format!(
        "-c listen_addresses=127.0.0.1 -p {port} -k {}",
        root.path().to_str().unwrap()
    );
    let log = root.path().join("log");
    run(
        &bin,
        "pg_ctl",
        &[
            "-D",
            data.to_str().unwrap(),
            "-l",
            log.to_str().unwrap(),
            "-o",
            &opts,
            "-w",
            "start",
        ],
    );
    let _server = Server {
        bin: &bin,
        data: data.clone(),
    };

    let archive = root.path().join("archive");
    let mut pgwal = Receiver(
        Command::new(env!("CARGO_BIN_EXE_pgwal"))
            .args([
                "receive",
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ])
            .arg("--dir")
            .arg(root.path().join("store"))
            .arg("--archive")
            .arg(&archive)
            .args(["--upload-every", "1"])
            .env("PGPASSWORD", PASSWORD)
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );

    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres password={PASSWORD} dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client
        .batch_execute("CREATE TABLE t AS SELECT g FROM generate_series(1, 10000) g")
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    while objects_under(&archive) == 0 {
        assert!(Instant::now() < deadline, "no object reached the archive");
        assert!(pgwal.0.try_wait().unwrap().is_none(), "pgwal exited early");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let active: bool = client
        .query_one(
            "SELECT active FROM pg_replication_slots WHERE slot_name = 'pgwal'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(active);

    run(
        Path::new("/bin"),
        "kill",
        &["-INT", &pgwal.0.id().to_string()],
    );
    let status = pgwal.0.wait().unwrap();
    assert!(status.success(), "pgwal exited with {status}");
}

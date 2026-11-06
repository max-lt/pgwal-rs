//! Takes a base backup without WAL, archives the WAL with `pgwal receive`,
//! and restores the backup with `pgwal restore`.
//!
//! Skipped when no Postgres binaries are found: set `PGWAL_PG_BIN` to their
//! directory.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio_postgres::NoTls;

const ROWS: i64 = 20_000;

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

fn path(p: &Path) -> &str {
    p.to_str().unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn pgwal(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pgwal"))
        .args(args)
        .output()
        .unwrap()
}

/// A server on a Unix socket only, and the directories of one test. The
/// servers stop when the test ends, also on a panic.
struct Setup {
    bin: PathBuf,
    // Under /tmp: a Unix socket path must fit in 104 bytes on macOS.
    root: TempDir,
    port: u16,
}

impl Setup {
    fn new(bin: PathBuf) -> Setup {
        let root = tempfile::Builder::new()
            .prefix("pwr")
            .tempdir_in("/tmp")
            .unwrap();
        let setup = Setup {
            bin,
            root,
            port: free_port(),
        };
        let data = setup.path("data");
        run(
            &setup.bin,
            "initdb",
            &[
                "-D",
                path(&data),
                "-U",
                "postgres",
                "-A",
                "trust",
                "--no-sync",
            ],
        );
        setup.start("data");
        setup
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn start(&self, dir: &str) {
        let opts = format!(
            "-c listen_addresses= -p {} -k {}",
            self.port,
            path(self.root.path())
        );
        let log = self.path(&format!("{dir}.log"));
        run(
            &self.bin,
            "pg_ctl",
            &[
                "-D",
                path(&self.path(dir)),
                "-l",
                path(&log),
                "-o",
                &opts,
                "-w",
                "start",
            ],
        );
    }

    fn stop(&self, dir: &str) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .args(["-D", path(&self.path(dir)), "-m", "fast", "-w", "stop"])
            .output();
    }

    /// Waits for the server to accept connections: `pg_ctl start` returns
    /// before the replay of a restore ends.
    async fn client(&self) -> tokio_postgres::Client {
        let config = format!(
            "host={} port={} user=postgres dbname=postgres",
            path(self.root.path()),
            self.port
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match tokio_postgres::connect(&config, NoTls).await {
                Ok((client, conn)) => {
                    tokio::spawn(conn);
                    return client;
                }
                Err(e) if Instant::now() < deadline => {
                    eprintln!("waiting for the server: {e}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => panic!("the server never accepted connections: {e}"),
            }
        }
    }

    fn receiver(&self) -> Child {
        Command::new(env!("CARGO_BIN_EXE_pgwal"))
            .args(["receive", "--host", path(self.root.path())])
            .args(["--port", &self.port.to_string(), "--user", "postgres"])
            .args(["--dir", path(&self.path("store"))])
            .args(["--archive", path(&self.path("archive"))])
            .args(["--upload-every", "1"])
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap()
    }

    /// Takes a base backup without WAL, writes rows across several segments,
    /// archives all of it and stops the server. Returns the archive keys.
    async fn archived_backup(&self) -> Vec<String> {
        let mut receiver = self.receiver();
        let client = self.client().await;
        let deadline = Instant::now() + Duration::from_secs(20);
        while client
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pgwal' AND active",
                &[],
            )
            .await
            .unwrap()
            .is_none()
        {
            assert!(Instant::now() < deadline, "the slot never became active");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        run(
            &self.bin,
            "pg_basebackup",
            &[
                "-h",
                path(self.root.path()),
                "-p",
                &self.port.to_string(),
                "-U",
                "postgres",
                "-D",
                path(&self.path("backup")),
                "-X",
                "none",
                "-c",
                "fast",
            ],
        );
        client
            .batch_execute("CREATE TABLE t (i bigint)")
            .await
            .unwrap();
        for batch in 0..4 {
            let (from, to) = (batch * ROWS / 4 + 1, (batch + 1) * ROWS / 4);
            client
                .execute(
                    "INSERT INTO t SELECT generate_series($1::bigint, $2::bigint)",
                    &[&from, &to],
                )
                .await
                .unwrap();
            client
                .batch_execute("SELECT pg_switch_wal()")
                .await
                .unwrap();
        }
        drop(client);
        // A clean shutdown waits for the receiver to confirm the final WAL.
        self.stop("data");
        Command::new("kill")
            .args(["-INT", &receiver.id().to_string()])
            .status()
            .unwrap();
        assert!(receiver.wait().unwrap().success(), "pgwal receive failed");
        let mut keys = Vec::new();
        let mut dirs = vec![self.path("archive")];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let p = entry.unwrap().path();
                match p.is_dir() {
                    true => dirs.push(p),
                    false => keys.push(
                        p.strip_prefix(self.path("archive"))
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_owned(),
                    ),
                }
            }
        }
        keys.sort();
        keys
    }

    fn restore(&self, extra: &[&str]) -> Output {
        let (archive, backup) = (self.path("archive"), self.path("backup"));
        let mut args = vec![
            "restore",
            "--archive",
            path(&archive),
            "--pgdata",
            path(&backup),
        ];
        args.extend_from_slice(extra);
        pgwal(&args)
    }
}

impl Drop for Setup {
    fn drop(&mut self) {
        self.stop("data");
        self.stop("backup");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_replays_the_archive_onto_a_base_backup() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    setup.archived_backup().await;

    let out = setup.restore(&[]);
    assert!(
        out.status.success(),
        "pgwal restore failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    setup.start("backup");
    let client = setup.client().await;
    let (rows, sum): (i64, i64) = client
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!((rows, sum), (ROWS, ROWS * (ROWS + 1) / 2));
    let in_recovery: bool = client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(!in_recovery, "the restored server did not promote");
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_refuses_an_archive_with_a_gap() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    let keys = setup.archived_backup().await;
    let wal: Vec<&String> = keys.iter().filter(|k| !k.ends_with(".history")).collect();
    assert!(wal.len() >= 3, "too few objects for a gap: {keys:?}");
    std::fs::remove_file(setup.path("archive").join(wal[wal.len() - 2])).unwrap();

    let out = setup.restore(&[]);
    assert!(!out.status.success(), "pgwal restore accepted a gap");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("gap"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!setup.path("backup/recovery.signal").exists());

    assert!(setup.restore(&["--accept-gap"]).status.success());
    setup.start("backup");
    let rows: i64 = setup
        .client()
        .await
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .unwrap()
        .get(0);
    assert!(rows < ROWS, "{rows} rows past the gap");
}

/// A standby restore replays the archive and stays on its timeline. A copy
/// of it after a clean stop is a new image, and a restore from it has every
/// row.
#[tokio::test(flavor = "multi_thread")]
async fn a_standby_restore_makes_an_image_to_restore_from() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    setup.archived_backup().await;

    let out = setup.restore(&["--standby"]);
    assert!(
        out.status.success(),
        "pgwal restore --standby failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let end = String::from_utf8(out.stdout).unwrap().trim().to_owned();
    setup.start("backup");
    let client = setup.client().await;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let reached: bool = client
            .query_one(
                &format!("SELECT pg_last_wal_replay_lsn() >= '{end}'::pg_lsn"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if reached {
            break;
        }
        assert!(Instant::now() < deadline, "the replay never reached {end}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let in_recovery: bool = client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(in_recovery, "the standby promoted");
    drop(client);
    setup.stop("backup");

    let image = pgwal::ControlFile::read(&setup.path("backup")).unwrap();
    assert_eq!(image.timeline, 1);
    std::fs::remove_file(setup.path("backup/standby.signal")).unwrap();
    let out = setup.restore(&[]);
    assert!(
        out.status.success(),
        "pgwal restore of the new image failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    setup.start("backup");
    let (rows, sum): (i64, i64) = setup
        .client()
        .await
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!((rows, sum), (ROWS, ROWS * (ROWS + 1) / 2));
}

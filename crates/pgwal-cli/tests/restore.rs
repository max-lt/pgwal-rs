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
                // Postgres 18 has page checksums by default; 16 and 17 not.
                "--data-checksums",
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

    /// A client once a restored server has promoted. `pg_ctl start` returns
    /// once the copy is consistent, and the replay goes on after it.
    async fn promoted_client(&self) -> tokio_postgres::Client {
        let client = self.client().await;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let in_recovery: bool = client
                .query_one("SELECT pg_is_in_recovery()", &[])
                .await
                .unwrap()
                .get(0);
            if !in_recovery {
                return client;
            }
            assert!(
                Instant::now() < deadline,
                "the restored server did not promote"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
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

    async fn wait_for_slot(&self, client: &tokio_postgres::Client) {
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
    }

    /// Stops the server, then the receiver once it has archived the final
    /// WAL: a clean shutdown waits for the receiver to confirm it.
    fn stop_with(&self, mut receiver: Child) {
        self.stop("data");
        Command::new("kill")
            .args(["-TERM", &receiver.id().to_string()])
            .status()
            .unwrap();
        assert!(receiver.wait().unwrap().success(), "pgwal receive failed");
    }

    /// Takes a base backup without WAL, writes rows across several segments,
    /// archives all of it and stops the server. Returns the archive keys.
    async fn archived_backup(&self) -> Vec<String> {
        let receiver = self.receiver();
        let client = self.client().await;
        self.wait_for_slot(&client).await;
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
            .batch_execute("CREATE TABLE t (i bigint); CREATE INDEX t_i ON t (i)")
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
        let file: String = client
            .query_one("SELECT pg_relation_filepath('t')", &[])
            .await
            .unwrap()
            .get(0);
        std::fs::write(self.path("t.path"), file).unwrap();
        drop(client);
        self.stop_with(receiver);
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
    let (rows, sum): (i64, i64) = setup
        .promoted_client()
        .await
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!((rows, sum), (ROWS, ROWS * (ROWS + 1) / 2));
}

/// `pgwal backup` writes a base backup into the archive while rows arrive,
/// `pgwal fetch` writes it into an empty directory, and a restore of it has
/// every row.
#[tokio::test(flavor = "multi_thread")]
async fn backup_then_fetch_then_restore_has_every_row() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    let receiver = setup.receiver();
    let client = setup.client().await;
    setup.wait_for_slot(&client).await;
    let insert = async |from: i64, to: i64| {
        client
            .execute(
                "INSERT INTO t SELECT generate_series($1::bigint, $2::bigint)",
                &[&from, &to],
            )
            .await
            .unwrap();
    };
    client
        .batch_execute("CREATE TABLE t (i bigint)")
        .await
        .unwrap();
    insert(1, ROWS / 2).await;
    let port = setup.port.to_string();
    let (root, archive) = (setup.root.path(), setup.path("archive"));
    let out = pgwal(&[
        "backup",
        "--host",
        path(root),
        "--port",
        &port,
        "--archive",
        path(&archive),
    ]);
    assert!(
        out.status.success(),
        "pgwal backup failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let taken = String::from_utf8(out.stdout).unwrap();
    insert(ROWS / 2 + 1, ROWS).await;
    drop(client);
    setup.stop_with(receiver);

    let copy = setup.path("backup");
    let fetch = || {
        pgwal(&[
            "fetch",
            "--archive",
            path(&archive),
            "--pgdata",
            path(&copy),
        ])
    };
    let out = fetch();
    assert!(
        out.status.success(),
        "pgwal fetch failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let start = String::from_utf8(out.stdout).unwrap();
    assert_eq!(taken.split_whitespace().next(), Some(start.trim()));
    assert!(copy.join("backup_manifest").exists());
    assert!(!fetch().status.success(), "a fetch into a full directory");
    run(&setup.bin, "pg_verifybackup", &["-n", path(&copy)]);

    let out = setup.restore(&[]);
    assert!(
        out.status.success(),
        "pgwal restore failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    setup.start("backup");
    let (rows, sum): (i64, i64) = setup
        .promoted_client()
        .await
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!((rows, sum), (ROWS, ROWS * (ROWS + 1) / 2));
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
/// of it after a clean stop is a new base backup, and a restore from it has every
/// row.
#[tokio::test(flavor = "multi_thread")]
async fn a_standby_restore_makes_a_new_base_backup() {
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

    let rebased = pgwal::ControlFile::read(&setup.path("backup")).unwrap();
    assert_eq!(rebased.timeline, 1);
    // The copy keeps its standby.signal: the restore must still promote.
    let out = setup.restore(&[]);
    assert!(
        out.status.success(),
        "pgwal restore of the new base backup failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    setup.start("backup");
    let (rows, sum): (i64, i64) = setup
        .promoted_client()
        .await
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!((rows, sum), (ROWS, ROWS * (ROWS + 1) / 2));
}

impl Setup {
    fn verify(&self) -> Output {
        Command::new(env!("CARGO_BIN_EXE_pgwal"))
            .args(["verify", "--pgdata", path(&self.path("backup"))])
            .args(["--pg-bin", path(&self.bin), "--user", "postgres"])
            .output()
            .unwrap()
    }

    fn rebase(&self) -> Output {
        let (archive, backup) = (self.path("archive"), self.path("backup"));
        Command::new(env!("CARGO_BIN_EXE_pgwal"))
            .args([
                "rebase",
                "--archive",
                path(&archive),
                "--pgdata",
                path(&backup),
            ])
            .args(["--pg-bin", path(&self.bin), "--user", "postgres"])
            .output()
            .unwrap()
    }
}

/// A rebase moves a base backup to the last checkpoint of the archive,
/// leaves no trace of its own server, and a restore of the result has every
/// row.
#[tokio::test(flavor = "multi_thread")]
async fn rebase_moves_a_base_backup_to_the_last_checkpoint() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    setup.archived_backup().await;
    let backup = setup.path("backup");
    let label = pgwal::BackupLabel::read(&backup).unwrap().unwrap();
    let hba = std::fs::read(backup.join("pg_hba.conf")).unwrap();
    // The private servers of rebase and verify must not run the archiving
    // of the copy, wait for its synchronous standby, delay the replay, or
    // load the certificate of the primary.
    let archived = setup.path("archived");
    let auto_conf = backup.join("postgresql.auto.conf");
    let conf = std::fs::read_to_string(&auto_conf).unwrap_or_default();
    std::fs::write(
        &auto_conf,
        format!(
            "{conf}archive_mode = on\narchive_command = 'touch {}'\n\
             synchronous_standby_names = 'pgwal'\nrecovery_min_apply_delay = '1h'\n\
             ssl = on\nssl_cert_file = '/nonexistent/server.crt'\n",
            archived.display()
        ),
    )
    .unwrap();

    let out = setup.rebase();
    assert!(
        out.status.success(),
        "pgwal rebase failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let redo: pgwal::Lsn = String::from_utf8(out.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let control = pgwal::ControlFile::read(&backup).unwrap();
    assert_eq!(control.redo, redo);
    assert!(
        redo > label.start,
        "redo {redo} did not move from {}",
        label.start
    );
    assert_eq!(control.timeline, 1);
    assert!(!backup.join("standby.signal").exists());
    assert!(!backup.join("postmaster.pid").exists());
    assert_eq!(std::fs::read(backup.join("pg_hba.conf")).unwrap(), hba);
    assert!(!archived.exists(), "a private server archived WAL");
    std::fs::write(&auto_conf, conf).unwrap();

    // Nothing new in the archive: the redo point stays. The copy has no
    // configuration files, as a copy of a Debian primary.
    let conf = ["postgresql.conf", "pg_hba.conf", "pg_ident.conf"];
    let aside = setup.path("conf");
    std::fs::create_dir(&aside).unwrap();
    for name in conf {
        std::fs::rename(backup.join(name), aside.join(name)).unwrap();
    }
    let again = setup.rebase();
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    assert_eq!(
        String::from_utf8(again.stdout).unwrap().trim(),
        redo.to_string()
    );
    for name in conf {
        assert!(!backup.join(name).exists(), "{name}");
        std::fs::rename(aside.join(name), backup.join(name)).unwrap();
    }

    let out = setup.restore(&[]);
    assert!(
        out.status.success(),
        "pgwal restore of the rebased copy failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    setup.start("backup");
    let (rows, sum): (i64, i64) = setup
        .promoted_client()
        .await
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!((rows, sum), (ROWS, ROWS * (ROWS + 1) / 2));
}

/// A wrong --archive holds nothing of the copy: the restore fails rather
/// than leave the copy without the WAL after it.
#[tokio::test(flavor = "multi_thread")]
async fn restore_refuses_an_archive_with_nothing_of_the_copy() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    setup.archived_backup().await;
    let empty = setup.path("empty");
    std::fs::create_dir(&empty).unwrap();
    let out = pgwal(&[
        "restore",
        "--archive",
        path(&empty),
        "--pgdata",
        path(&setup.path("backup")),
    ]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the restore passed:\n{stderr}");
    assert!(stderr.contains("holds no WAL"), "{stderr}");
}

/// The archive ends before the end of the base backup, as just after
/// pg_basebackup -X none. The rebase fails at once: a standby would wait
/// for consistency without end.
#[tokio::test(flavor = "multi_thread")]
async fn rebase_refuses_an_archive_short_of_the_backup_end() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    let keys = setup.archived_backup().await;
    let label = pgwal::BackupLabel::read(&setup.path("backup"))
        .unwrap()
        .unwrap();
    let archive = setup.path("archive");
    for key in &keys {
        let Some(range) = pgwal::RangeKey::parse(key) else {
            continue;
        };
        let file = archive.join(key);
        if range.start > label.start {
            std::fs::remove_file(&file).unwrap();
        } else if range.end > label.start {
            // Keep the head of the object, up to just after the start.
            let wal = std::fs::read(&file).unwrap();
            let short = pgwal::RangeKey {
                end: pgwal::Lsn(label.start.0 + 8),
                ..range
            };
            let len = (short.end.0 - short.start.0) as usize;
            std::fs::remove_file(&file).unwrap();
            std::fs::write(archive.join(short.to_string()), &wal[..len]).unwrap();
        }
    }

    let out = setup.rebase();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the rebase passed:\n{stderr}");
    assert!(stderr.contains("before the copy is consistent"), "{stderr}");

    // Without the manifest, the standby itself shows that it waits for WAL
    // before it is consistent.
    std::fs::remove_file(setup.path("backup/backup_manifest")).unwrap();
    let out = setup.rebase();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the rebase passed:\n{stderr}");
    assert!(stderr.contains("before the copy is consistent"), "{stderr}");
}

/// An archived object is damaged a few segments before the end. Postgres
/// takes the damage for the end of the WAL: the rebase must fail, not stop
/// there.
#[tokio::test(flavor = "multi_thread")]
async fn rebase_fails_on_unreadable_wal() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    let keys = setup.archived_backup().await;
    let label = pgwal::BackupLabel::read(&setup.path("backup"))
        .unwrap()
        .unwrap();
    let damaged = keys
        .iter()
        .filter_map(|key| pgwal::RangeKey::parse(key).map(|range| (range, key)))
        .filter(|(range, _)| range.start.0 >= label.start.0 + (16 << 20))
        .min_by_key(|(range, _)| range.start)
        .map(|(_, key)| setup.path("archive").join(key))
        .unwrap();
    let mut wal = std::fs::read(&damaged).unwrap();
    for byte in &mut wal[0x3010..0x3050] {
        *byte ^= 0xFF;
    }
    std::fs::write(&damaged, wal).unwrap();

    let out = setup.rebase();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the rebase passed:\n{stderr}");
    assert!(stderr.contains("unreadable"), "{stderr}");
}

/// The primary raised max_connections after the copy. The replay pauses on
/// the change, and the rebase starts its server again with the new value.
#[tokio::test(flavor = "multi_thread")]
async fn rebase_crosses_a_raise_of_max_connections() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    let receiver = setup.receiver();
    let client = setup.client().await;
    setup.wait_for_slot(&client).await;
    run(
        &setup.bin,
        "pg_basebackup",
        &[
            "-h",
            path(setup.root.path()),
            "-p",
            &setup.port.to_string(),
            "-U",
            "postgres",
            "-D",
            path(&setup.path("backup")),
            "-X",
            "none",
            "-c",
            "fast",
        ],
    );
    client
        .batch_execute("ALTER SYSTEM SET max_connections = 150")
        .await
        .unwrap();
    drop(client);
    setup.stop("data");
    setup.start("data");
    let client = setup.client().await;
    setup.wait_for_slot(&client).await;
    client
        .batch_execute(
            "CREATE TABLE t AS SELECT generate_series(1, 1000) AS i; \
             CHECKPOINT; SELECT pg_switch_wal()",
        )
        .await
        .unwrap();
    drop(client);
    setup.stop_with(receiver);

    let out = setup.rebase();
    assert!(
        out.status.success(),
        "pgwal rebase failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("starting again"),
        "no restart after the pause:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `pgwal plan` keeps the keys after the start of the copy, with their
/// prefix.
#[tokio::test(flavor = "multi_thread")]
async fn plan_lists_the_keys_a_restore_reads() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    let keys = setup.archived_backup().await;
    let backup = setup.path("backup");
    let start = pgwal::BackupLabel::read(&backup).unwrap().unwrap().start;

    let input: String = keys.iter().map(|k| format!("bucket/{k}\n")).collect();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pgwal"))
        .args(["plan", "--pgdata", path(&backup), "--prefix", "bucket/"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(child.stdin.as_mut().unwrap(), input.as_bytes()).unwrap();
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let planned: Vec<String> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();

    assert!(!planned.is_empty());
    assert!(planned.len() < keys.len(), "every key planned: {planned:?}");
    for key in &planned {
        let key = key.strip_prefix("bucket/").expect("the prefix stays");
        assert!(keys.contains(&key.to_owned()), "unknown key {key}");
        let range = pgwal::RangeKey::parse(key).unwrap();
        assert!(range.end > start, "{key} ends before the start {start}");
    }
}

/// `pgwal verify` finds a broken page: by its checksum when the copy has
/// page checksums, and by pg_amcheck when it has none.
#[tokio::test(flavor = "multi_thread")]
async fn verify_finds_a_broken_page() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let setup = Setup::new(bin);
    setup.archived_backup().await;
    let out = setup.rebase();
    assert!(
        out.status.success(),
        "pgwal rebase failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(setup.verify().status.success());

    // A --user that the copy lacks fails at once, with the reason.
    let out = pgwal(&[
        "verify",
        "--pgdata",
        path(&setup.path("backup")),
        "--pg-bin",
        path(&setup.bin),
        "--user",
        "nobody",
    ]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "verify passed with no role");
    assert!(stderr.contains("does not exist"), "{stderr}");

    // The line pointers of the first page of t start at byte 24. A normal
    // line pointer (lp_flags 1) at offset 0x7ff0 points past the 8 kB page.
    let backup = setup.path("backup");
    let file = backup.join(std::fs::read_to_string(setup.path("t.path")).unwrap());
    let mut bytes = std::fs::read(&file).unwrap();
    let lp: u32 = 0x7ff0 | (1 << 15) | (0x40 << 17);
    bytes[24..28].copy_from_slice(&lp.to_le_bytes());
    std::fs::write(&file, bytes).unwrap();

    let out = setup.verify();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "a bad checksum passed");
    assert!(stderr.contains("checksums"), "{stderr}");

    run(
        &setup.bin,
        "pg_checksums",
        &["--disable", "-D", path(&backup)],
    );
    let out = setup.verify();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "pg_amcheck missed the broken page");
    assert!(stderr.contains("pg_amcheck"), "{stderr}");
    let scratch = std::fs::read_dir(setup.root.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".verify-"))
        .count();
    assert_eq!(scratch, 0, "a scratch copy stayed");
}

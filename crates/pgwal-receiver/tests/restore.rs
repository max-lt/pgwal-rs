//! A base backup plus the WAL the receiver uploaded restores every committed
//! row.
//!
//! Skipped when no Postgres binaries are found: set `PGWAL_PG_BIN` to their
//! directory, or put `initdb` on PATH.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pgwal::{ArchiveKey, ClusterState, ControlFile, assemble_segment, segment_file_name};
use pgwal_receiver::{
    ArchivedBackup, Confirm, ConnConfig, DirSink, Error, Lsn, RangeKey, StreamConfig, WalSink,
    base_backup, forget_slot, receive,
};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;

const ROWS: i64 = 10_000;
const SLOT: &str = "pgwal_receiver";
/// The `initdb` default.
const SEG: u64 = 16 << 20;

type Ranges = Vec<(Lsn, Lsn, Bytes)>;

/// Rebuilds Postgres segment files in `out` from the WAL objects in
/// `objects`.
fn materialize(objects: &Path, out: &Path) {
    let mut segments: BTreeMap<(u32, u64), Ranges> = BTreeMap::new();
    for file in files_under(objects) {
        let key = file.strip_prefix(objects).unwrap().to_str().unwrap();
        let Some(key) = RangeKey::parse(key) else {
            continue;
        };
        let wal = Bytes::from(std::fs::read(&file).unwrap());
        segments
            .entry((key.timeline, key.start.segment_no(SEG)))
            .or_default()
            .push((key.start, key.end, wal));
    }
    std::fs::create_dir_all(out).unwrap();
    for ((tli, segno), ranges) in segments {
        let segment = assemble_segment(segno, SEG, &ranges).expect("gap in uploaded WAL");
        std::fs::write(out.join(segment_file_name(tli, segno, SEG)), segment).unwrap();
    }
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push(path);
        }
    }
    files
}

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

fn start(bin: &Path, data: &Path, sock: &Path, log: &Path) {
    start_with(bin, data, sock, log, "");
}

fn start_with(bin: &Path, data: &Path, sock: &Path, log: &Path, extra: &str) {
    let opts = format!("-c listen_addresses='' -k {} {extra}", path(sock));
    run(
        bin,
        "pg_ctl",
        &[
            "-D",
            path(data),
            "-l",
            path(log),
            "-o",
            &opts,
            "-w",
            "start",
        ],
    );
}

fn stop(bin: &Path, data: &Path) {
    run(
        bin,
        "pg_ctl",
        &["-D", path(data), "-m", "fast", "-w", "stop"],
    );
}

async fn connect(sock: &Path) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host={} user=postgres dbname=postgres", path(sock)),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// `pg_ctl -w start` returns once a restored copy is consistent, before the
/// replay ends.
async fn wait_promoted(client: &tokio_postgres::Client) {
    for _ in 0..300 {
        let in_recovery: bool = client
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get(0);
        if !in_recovery {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the restored server did not promote");
}

async fn wait_for_slot(client: &tokio_postgres::Client, active: bool) {
    for _ in 0..100 {
        let now: Option<bool> = client
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .await
            .unwrap()
            .map(|row| row.get(0));
        if now == Some(active) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("slot {SLOT} never became active = {active}");
}

fn spawn_receiver(root: &Path, wal: &Path, objects: &Path) -> JoinHandle<Result<Lsn, Error>> {
    let cfg = stream_config(wal);
    let sink = Arc::new(DirSink::new(objects));
    let socket = root.join(".s.PGSQL.5432");
    tokio::spawn(async move {
        let stream = UnixStream::connect(socket).await?;
        receive(stream, &cfg, sink, std::future::pending()).await
    })
}

async fn finish(receiver: JoinHandle<Result<Lsn, Error>>) -> Result<Lsn, Error> {
    tokio::time::timeout(Duration::from_secs(30), receiver)
        .await
        .expect("receiver did not return")
        .unwrap()
}

async fn insert(client: &tokio_postgres::Client, from: i64, to: i64) {
    client
        .execute(
            &format!("INSERT INTO t SELECT generate_series({from}, {to})"),
            &[],
        )
        .await
        .unwrap();
}

/// Configures `backup` to recover from the segment files in `wal`.
fn restore_from(backup: &Path, wal: &Path, root: &Path) {
    let script = root.join("restore.sh");
    let wal = path(wal);
    std::fs::write(
        &script,
        format!("#!/bin/sh\n[ -f {wal}/$1 ] && exec cp {wal}/$1 \"$2\"\nexit 1\n"),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(backup.join("recovery.signal"), "").unwrap();
    let mut conf = std::fs::read_to_string(backup.join("postgresql.auto.conf")).unwrap();
    conf.push_str(&format!("restore_command = '{} %f %p'\n", path(&script)));
    std::fs::write(backup.join("postgresql.auto.conf"), conf).unwrap();
}

/// The receiver loses its connection in the middle of the stream, resumes on
/// the same directory, and the archive stays complete.
#[tokio::test(flavor = "multi_thread")]
async fn base_backup_plus_uploaded_wal_restores_every_row_across_a_reconnect() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    // Under /tmp: a unix socket path must fit in 104 bytes on macOS.
    let tmp = tempfile::Builder::new()
        .prefix("brr")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    let (data, backup) = (root.join("data"), root.join("backup"));
    let (wal, objects, restored) = (
        root.join("wal"),
        root.join("objects"),
        root.join("restored"),
    );

    run(
        &bin,
        "initdb",
        &[
            "-D",
            path(&data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-sync",
            "--no-instructions",
        ],
    );
    start(&bin, &data, root, &root.join("primary.log"));

    let receiver = spawn_receiver(root, &wal, &objects);
    let client = connect(root).await;
    wait_for_slot(&client, true).await;
    run(
        &bin,
        "pg_basebackup",
        &[
            "-h",
            path(root),
            "-U",
            "postgres",
            "-D",
            path(&backup),
            "-X",
            "none",
            "-c",
            "fast",
        ],
    );

    client
        .batch_execute("CREATE TABLE t (i bigint); SELECT pg_switch_wal();")
        .await
        .unwrap();
    insert(&client, 1, ROWS).await;

    client
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE application_name = 'pgwal_receiver'",
            &[],
        )
        .await
        .unwrap();
    assert!(finish(receiver).await.is_err(), "the stream was cut");
    wait_for_slot(&client, false).await;

    let receiver = spawn_receiver(root, &wal, &objects);
    wait_for_slot(&client, true).await;
    insert(&client, ROWS + 1, 2 * ROWS).await;
    let written: String = client
        .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    drop(client);

    stop(&bin, &data);
    let uploaded = finish(receiver).await.unwrap();
    let written: Lsn = written.parse().unwrap();
    assert!(
        uploaded >= written,
        "uploaded {uploaded} < written {written}"
    );

    // Only the unfinished segment stays local once everything is uploaded.
    let local: Vec<_> = std::fs::read_dir(&wal)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.len() >= 24)
        .collect();
    assert!(
        local.iter().all(|name| name.ends_with(".partial")),
        "{local:?}"
    );

    materialize(&objects, &restored);
    restore_from(&backup, &restored, root);
    start(&bin, &backup, root, &root.join("restore.log"));
    let client = connect(root).await;
    wait_promoted(&client).await;
    let (rows, sum): (i64, i64) = client
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!(rows, 2 * ROWS);
    assert_eq!(sum, 2 * ROWS * (2 * ROWS + 1) / 2);
    drop(client);
    stop(&bin, &backup);
}

/// Writes the files of `backup` into `out`, and unpacks `base.tar` there.
fn unpack(backup: &ArchivedBackup, objects: &Path, out: &Path) {
    std::fs::create_dir(out).unwrap();
    std::fs::set_permissions(out, std::fs::Permissions::from_mode(0o700)).unwrap();
    for (name, parts) in &backup.streams {
        let mut file = Vec::new();
        for part in 0..*parts {
            let key = ArchiveKey::BackupPart {
                system_id: backup.system_id,
                start: backup.start,
                name: name.clone(),
                part,
            };
            file.extend(std::fs::read(objects.join(key.to_string())).unwrap());
        }
        std::fs::write(out.join(name), file).unwrap();
    }
    let tar = out.join("base.tar");
    let status = Command::new("tar")
        .args(["-xf", path(&tar), "-C", path(out)])
        .status()
        .unwrap();
    assert!(status.success(), "tar -xf failed");
    std::fs::remove_file(tar).unwrap();
}

/// `base_backup` writes the backup next to the WAL of the slot, while rows
/// arrive. `pg_verifybackup` accepts the copy, and the restore has every row.
#[tokio::test(flavor = "multi_thread")]
async fn a_base_backup_in_the_sink_restores_every_row() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let tmp = tempfile::Builder::new()
        .prefix("brb")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    let (data, copy) = (root.join("data"), root.join("copy"));
    let (wal, objects, restored) = (
        root.join("wal"),
        root.join("objects"),
        root.join("restored"),
    );
    run(
        &bin,
        "initdb",
        &[
            "-D",
            path(&data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-sync",
            "--no-instructions",
        ],
    );
    start(&bin, &data, root, &root.join("primary.log"));
    let receiver = spawn_receiver(root, &wal, &objects);
    let client = connect(root).await;
    wait_for_slot(&client, true).await;
    client
        .batch_execute("CREATE TABLE t (i bigint)")
        .await
        .unwrap();
    insert(&client, 1, ROWS).await;

    let backup = {
        let conn = conn("pgwal_backup");
        let sink = Arc::new(DirSink::new(&objects));
        let socket = root.join(".s.PGSQL.5432");
        let stream = UnixStream::connect(&socket).await.unwrap();
        let err = base_backup(stream, &conn, "missing", sink.clone())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        tokio::spawn(async move {
            base_backup(UnixStream::connect(socket).await?, &conn, SLOT, sink).await
        })
    };
    insert(&client, ROWS + 1, 2 * ROWS).await;
    let backup = backup.await.unwrap().unwrap();
    insert(&client, 2 * ROWS + 1, 3 * ROWS).await;
    drop(client);
    stop(&bin, &data);
    let uploaded = finish(receiver).await.unwrap();
    assert!(uploaded >= backup.end, "{uploaded} < {}", backup.end);

    let done = ArchiveKey::BackupDone {
        system_id: backup.system_id,
        start: backup.start,
    };
    let done = objects.join(done.to_string());
    let done = std::fs::read_to_string(done).unwrap();
    assert_eq!(ArchivedBackup::parse(&done).as_ref(), Some(&backup));
    let base_parts = backup.streams.iter().find(|(name, _)| name == "base.tar");
    assert!(
        base_parts.is_some_and(|(_, parts)| *parts > 1),
        "{:?}",
        backup.streams
    );

    unpack(&backup, &objects, &copy);
    run(&bin, "pg_verifybackup", &["-n", path(&copy)]);
    materialize(&objects, &restored);
    restore_from(&copy, &restored, root);
    start(&bin, &copy, root, &root.join("restore.log"));
    let client = connect(root).await;
    wait_promoted(&client).await;
    let (rows, sum): (i64, i64) = client
        .query_one("SELECT count(*), sum(i)::bigint FROM t", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!(rows, 3 * ROWS);
    assert_eq!(sum, 3 * ROWS * (3 * ROWS + 1) / 2);
    drop(client);
    stop(&bin, &copy);
}

/// A sink that keeps an object for good, as `S3Sink` does: a put of other
/// bytes under a key fails.
struct Immutable(DirSink);

#[async_trait::async_trait]
impl WalSink for Immutable {
    async fn put(&self, key: &str, data: Bytes) -> std::io::Result<()> {
        match self.0.get(key).await? {
            Some(old) if old != data => Err(std::io::Error::other(format!("{key}: other bytes"))),
            _ => self.0.put(key, data).await,
        }
    }

    async fn get(&self, key: &str) -> std::io::Result<Option<Bytes>> {
        self.0.get(key).await
    }

    async fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.0.list(prefix).await
    }

    async fn delete(&self, keys: &[String]) -> std::io::Result<()> {
        self.0.delete(keys).await
    }
}

/// A standby starts each backup at its last restartpoint, so with no new
/// checkpoint on the primary two backups have the same start. The parts of
/// a failed backup make way, and a whole backup is the result.
#[tokio::test(flavor = "multi_thread")]
async fn backups_of_an_idle_standby_share_their_start() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let tmp = tempfile::Builder::new()
        .prefix("bsb")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    let (data, standby, sb_sock) = (root.join("data"), root.join("standby"), root.join("sb"));
    std::fs::create_dir(&sb_sock).unwrap();
    run(
        &bin,
        "initdb",
        &[
            "-D",
            path(&data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-sync",
            "--no-instructions",
        ],
    );
    start(&bin, &data, root, &root.join("primary.log"));
    run(
        &bin,
        "pg_basebackup",
        &[
            "-D",
            path(&standby),
            "-R",
            "-h",
            path(root),
            "-U",
            "postgres",
            "--no-sync",
        ],
    );
    std::fs::set_permissions(&standby, std::fs::Permissions::from_mode(0o700)).unwrap();
    start(&bin, &standby, &sb_sock, &root.join("standby.log"));
    connect(&sb_sock)
        .await
        .batch_execute(&format!(
            "SELECT pg_create_physical_replication_slot('{SLOT}', true)"
        ))
        .await
        .unwrap();

    let sink: Arc<dyn WalSink> = Arc::new(Immutable(DirSink::new(root.join("objects"))));
    let socket = sb_sock.join(".s.PGSQL.5432");
    let backup = || async {
        // The backup_label gives the start time in seconds: a new second
        // gives other bytes.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let stream = UnixStream::connect(&socket).await.unwrap();
        base_backup(stream, &conn("pgwal_backup"), SLOT, sink.clone())
            .await
            .unwrap()
    };
    let first = backup().await;
    let done = ArchiveKey::BackupDone {
        system_id: first.system_id,
        start: first.start,
    };
    sink.delete(&[done.to_string()]).await.unwrap();
    let second = backup().await;
    assert_eq!(second.start, first.start);
    assert!(sink.get(&done.to_string()).await.unwrap().is_some());
    assert_eq!(backup().await, second);

    stop(&bin, &standby);
    stop(&bin, &data);
}

/// A sink that refuses the `done` object of a backup.
struct NoDone(DirSink);

#[async_trait::async_trait]
impl WalSink for NoDone {
    async fn put(&self, key: &str, data: Bytes) -> std::io::Result<()> {
        if key.ends_with("/done") {
            return Err(std::io::Error::other("sink down"));
        }
        self.0.put(key, data).await
    }

    async fn get(&self, key: &str) -> std::io::Result<Option<Bytes>> {
        self.0.get(key).await
    }

    async fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.0.list(prefix).await
    }

    async fn delete(&self, keys: &[String]) -> std::io::Result<()> {
        self.0.delete(keys).await
    }
}

/// A primary starts each backup at a new checkpoint, so no later backup
/// removes the parts of a failed one: the failed backup removes them.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_backup_leaves_no_part() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let tmp = tempfile::Builder::new()
        .prefix("bfb")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    let data = root.join("data");
    run(
        &bin,
        "initdb",
        &[
            "-D",
            path(&data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-sync",
            "--no-instructions",
        ],
    );
    start(&bin, &data, root, &root.join("primary.log"));
    connect(root)
        .await
        .batch_execute(&format!(
            "SELECT pg_create_physical_replication_slot('{SLOT}', true)"
        ))
        .await
        .unwrap();

    let objects = root.join("objects");
    let sink: Arc<dyn WalSink> = Arc::new(NoDone(DirSink::new(&objects)));
    let stream = UnixStream::connect(root.join(".s.PGSQL.5432"))
        .await
        .unwrap();
    let err = base_backup(stream, &conn("pgwal_backup"), SLOT, sink.clone())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("sink down"), "{err}");
    assert_eq!(sink.list("").await.unwrap(), Vec::<String>::new());

    stop(&bin, &data);
}

/// `pg_controldata` output as `label -> value`.
fn controldata(bin: &Path, data: &Path) -> BTreeMap<String, String> {
    let out = Command::new(bin.join("pg_controldata"))
        .arg(data)
        .output()
        .unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect()
}

fn assert_matches_controldata(bin: &Path, data: &Path) {
    let fields = controldata(bin, data);
    let control = ControlFile::parse(&std::fs::read(data.join("global/pg_control")).unwrap())
        .expect("pg_control parses");
    assert_eq!(
        control.system_id.to_string(),
        fields["Database system identifier"]
    );
    assert_eq!(
        control.state == ClusterState::ShutDown,
        fields["Database cluster state"] == "shut down"
    );
    assert_eq!(
        control.checkpoint.to_string(),
        fields["Latest checkpoint location"]
    );
    assert_eq!(
        control.redo.to_string(),
        fields["Latest checkpoint's REDO location"]
    );
    assert_eq!(
        control.timeline.to_string(),
        fields["Latest checkpoint's TimeLineID"]
    );
}

#[test]
fn control_file_matches_pg_controldata() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let tmp = tempfile::Builder::new()
        .prefix("brc")
        .tempdir_in("/tmp")
        .unwrap();
    let data = tmp.path().join("data");
    run(
        &bin,
        "initdb",
        &[
            "-D",
            path(&data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-sync",
            "--no-instructions",
        ],
    );
    assert_matches_controldata(&bin, &data);
    start(&bin, &data, tmp.path(), &tmp.path().join("pg.log"));
    assert_matches_controldata(&bin, &data);
    stop(&bin, &data);
}

fn conn(application_name: &str) -> ConnConfig {
    ConnConfig {
        user: "postgres".into(),
        password: None,
        application_name: application_name.into(),
        idle_timeout: Duration::from_secs(60),
    }
}

fn stream_config(wal: &Path) -> StreamConfig {
    StreamConfig {
        conn: conn("pgwal_receiver"),
        slot: SLOT.into(),
        dir: wal.to_path_buf(),
        upload_every: Duration::from_millis(100),
        confirm: Confirm::Flushed,
    }
}

/// A slot past `max_slot_wal_keep_size` loses its WAL. The receiver reports
/// the gap, `forget_slot` drops the slot, and the next stream starts again.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_slot_is_reported_then_recreated() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let tmp = tempfile::Builder::new()
        .prefix("brl")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    let (data, wal, objects) = (root.join("data"), root.join("wal"), root.join("objects"));
    run(
        &bin,
        "initdb",
        &[
            "-D",
            path(&data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-sync",
            "--no-instructions",
        ],
    );
    start_with(
        &bin,
        &data,
        root,
        &root.join("pg.log"),
        "-c max_slot_wal_keep_size=1MB -c max_wal_size=32MB",
    );
    let socket = root.join(".s.PGSQL.5432");
    let sink = Arc::new(DirSink::new(objects));
    let client = connect(root).await;
    client
        .batch_execute(&format!(
            "SELECT pg_create_physical_replication_slot('{SLOT}', true);
             CREATE TABLE t (i bigint);
             INSERT INTO t SELECT generate_series(1, 1500000);
             SELECT pg_switch_wal(); CHECKPOINT;
             INSERT INTO t SELECT generate_series(1, 100);
             SELECT pg_switch_wal(); CHECKPOINT;"
        ))
        .await
        .unwrap();

    let cfg = stream_config(&wal);
    let stream = UnixStream::connect(&socket).await.unwrap();
    let err = receive(stream, &cfg, sink.clone(), std::future::pending())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::WalGone { .. }), "{err}");

    forget_slot(UnixStream::connect(&socket).await.unwrap(), &cfg.conn, SLOT)
        .await
        .unwrap();
    std::fs::remove_dir_all(&wal).unwrap();

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<Lsn>();
    let receiver = {
        let (cfg, sink) = (stream_config(&wal), sink.clone());
        let stream = UnixStream::connect(&socket).await.unwrap();
        tokio::spawn(async move { receive(stream, &cfg, sink, async { stop_rx.await.ok() }).await })
    };
    wait_for_slot(&client, true).await;
    client
        .execute("INSERT INTO t SELECT generate_series(1, 1000)", &[])
        .await
        .unwrap();
    let written: Lsn = client
        .query_one("SELECT pg_current_wal_flush_lsn()::text", &[])
        .await
        .unwrap()
        .get::<_, String>(0)
        .parse()
        .unwrap();
    stop_tx.send(written).unwrap();
    let uploaded = finish(receiver).await.unwrap();
    assert!(uploaded >= written, "{uploaded} < {written}");
    drop(client);
    stop(&bin, &data);
}

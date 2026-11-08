//! Runs `pgwal receive` against Postgres servers that ask for SCRAM-SHA-256
//! over TCP, with and without TLS.
//!
//! Skipped when no Postgres binaries are found: set `PGWAL_PG_BIN` to their
//! directory.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tempfile::TempDir;
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

/// A CA, and a server certificate for 127.0.0.1 that it signs, as PEM.
struct Pki {
    ca: String,
    cert: String,
    key: String,
}

fn pki() -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let key = KeyPair::generate().unwrap();
    let cert = CertificateParams::new(vec!["127.0.0.1".into()])
        .unwrap()
        .signed_by(&key, &ca, &ca_key)
        .unwrap();
    Pki {
        ca: ca.pem(),
        cert: cert.pem(),
        key: key.serialize_pem(),
    }
}

/// A running server, stopped when the test ends, also on a panic.
struct Cluster {
    bin: PathBuf,
    root: TempDir,
    port: u16,
}

impl Cluster {
    /// With `tls`, the server takes TCP connections over TLS only.
    fn start(bin: PathBuf, tls: Option<&Pki>) -> Cluster {
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
        let host = match tls {
            Some(pki) => {
                std::fs::write(data.join("server.crt"), &pki.cert).unwrap();
                std::fs::write(data.join("server.key"), &pki.key).unwrap();
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    data.join("server.key"),
                    std::fs::Permissions::from_mode(0o600),
                )
                .unwrap();
                let mut conf = std::fs::OpenOptions::new()
                    .append(true)
                    .open(data.join("postgresql.conf"))
                    .unwrap();
                std::io::Write::write_all(&mut conf, b"ssl = on\n").unwrap();
                "hostssl"
            }
            None => "host",
        };
        std::fs::write(
            data.join("pg_hba.conf"),
            format!(
                "local all all scram-sha-256\n\
                 {host} all all 127.0.0.1/32 scram-sha-256\n\
                 {host} replication all 127.0.0.1/32 scram-sha-256\n"
            ),
        )
        .unwrap();
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
        Cluster { bin, root, port }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    /// A client over the Unix socket, which needs no TLS.
    async fn client(&self) -> tokio_postgres::Client {
        let (client, conn) = tokio_postgres::connect(
            &format!(
                "host={} port={} user=postgres password={PASSWORD} dbname=postgres",
                self.root.path().to_str().unwrap(),
                self.port
            ),
            NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(conn);
        client
    }

    /// Starts `pgwal receive` on TCP with `extra` arguments.
    fn receiver(&self, extra: &[&str]) -> Receiver {
        let child = Command::new(env!("CARGO_BIN_EXE_pgwal"))
            .args([
                "receive",
                "--host",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
            ])
            .arg("--dir")
            .arg(self.path("store"))
            .arg("--archive")
            .arg(self.path("archive"))
            .args(["--upload-every", "1"])
            .args(extra)
            .env("PGPASSWORD", PASSWORD)
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Receiver(child)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .args(["-D", self.path("data").to_str().unwrap()])
            .args(["-m", "immediate", "-w", "stop"])
            .output();
    }
}

/// Kills `pgwal` when the test ends, also on a panic.
struct Receiver(Child);

impl Receiver {
    fn interrupt(mut self) -> std::process::ExitStatus {
        run(
            Path::new("/bin"),
            "kill",
            &["-INT", &self.0.id().to_string()],
        );
        self.0.wait().unwrap()
    }
}

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

async fn slot_active(client: &tokio_postgres::Client) -> Option<bool> {
    client
        .query_opt(
            "SELECT active FROM pg_replication_slots WHERE slot_name = 'pgwal'",
            &[],
        )
        .await
        .unwrap()
        .map(|row| row.get(0))
}

/// Writes WAL, waits for an archived object, and stops `pgwal`.
async fn archive_and_stop(cluster: &Cluster, mut pgwal: Receiver) {
    let client = cluster.client().await;
    client
        .batch_execute("CREATE TABLE t AS SELECT g FROM generate_series(1, 10000) g")
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while objects_under(&cluster.path("archive")) == 0 {
        assert!(Instant::now() < deadline, "no object reached the archive");
        assert!(pgwal.0.try_wait().unwrap().is_none(), "pgwal exited early");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(slot_active(&client).await, Some(true));
    let status = pgwal.interrupt();
    assert!(status.success(), "pgwal exited with {status}");
    // The archive holds every row: no other user reads it.
    assert_private(&cluster.path("archive"));
}

/// Asserts that no file or directory under `dir` grants any access to the
/// group or to others.
fn assert_private(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{} has mode {mode:o}", path.display());
        if path.is_dir() {
            assert_private(&path);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_archives_wal_with_scram_auth() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let cluster = Cluster::start(bin, None);
    let pgwal = cluster.receiver(&[]);
    archive_and_stop(&cluster, pgwal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_full_archives_wal_over_tls() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let pki = pki();
    let cluster = Cluster::start(bin, Some(&pki));
    std::fs::write(cluster.path("ca.pem"), &pki.ca).unwrap();
    let ca = cluster.path("ca.pem");
    let pgwal = cluster.receiver(&[
        "--sslmode",
        "verify-full",
        "--sslrootcert",
        ca.to_str().unwrap(),
    ]);
    archive_and_stop(&cluster, pgwal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn require_archives_wal_without_a_ca() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let cluster = Cluster::start(bin, Some(&pki()));
    let pgwal = cluster.receiver(&["--sslmode", "require"]);
    archive_and_stop(&cluster, pgwal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_full_stops_on_a_certificate_of_another_ca() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let cluster = Cluster::start(bin, Some(&pki()));
    std::fs::write(cluster.path("other-ca.pem"), pki().ca).unwrap();
    let ca = cluster.path("other-ca.pem");
    let mut pgwal = cluster.receiver(&[
        "--sslmode",
        "verify-full",
        "--sslrootcert",
        ca.to_str().unwrap(),
    ]);
    // A certificate that fails the check fails every retry: the command
    // stops.
    let mut status = None;
    for _ in 0..50 {
        status = pgwal.0.try_wait().unwrap();
        if status.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        status.is_some_and(|s| !s.success()),
        "pgwal still runs or passed"
    );
    assert_eq!(slot_active(&cluster.client().await).await, None);
    assert_eq!(objects_under(&cluster.path("archive")), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn require_stops_on_a_server_without_tls() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipping: postgres binaries not found");
        return;
    };
    let cluster = Cluster::start(bin, None);
    let mut pgwal = cluster.receiver(&["--sslmode", "require"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = pgwal.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "pgwal did not stop");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(!status.success());
    assert_eq!(slot_active(&cluster.client().await).await, None);
}

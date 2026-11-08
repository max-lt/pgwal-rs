use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use pgwal_receiver::{Confirm, ConnConfig, Error, StreamConfig, receive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio_rustls::TlsConnector;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod archive;
mod backup;
mod pg;
mod rebase;
mod restore;
mod server;
mod tls;
mod verify;

use tls::SslMode;

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser, Debug)]
#[command(
    name = "pgwal",
    version,
    about = "Continuous archiving of the Postgres WAL"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Stream the WAL of a server over physical replication into an archive.
    Receive(ReceiveArgs),
    /// Take a base backup without WAL into an archive, while pgwal receive
    /// archives the WAL of the server.
    Backup(backup::BackupArgs),
    /// Write the latest base backup of an archive into an empty data
    /// directory.
    Fetch(backup::FetchArgs),
    /// Write the archived WAL that follows a base backup or a snapshot into
    /// its pg_wal.
    Restore(restore::RestoreArgs),
    /// Print the archive keys, read on stdin, that a restore of a copy reads.
    Plan(restore::PlanArgs),
    /// Move a copy of the data directory forward to the end of the archive,
    /// with a Postgres that stays in recovery and serves no client.
    Rebase(rebase::RebaseArgs),
    /// Check a stopped copy of the data directory: page checksums, and
    /// pg_amcheck on a scratch copy.
    Verify(verify::VerifyArgs),
}

/// Where to reach the server, as a role with the REPLICATION attribute.
#[derive(Args, Debug)]
pub struct ConnArgs {
    /// Server host, or the directory of its Unix socket when it starts with
    /// `/`. The password comes from PGPASSWORD only.
    #[arg(long, env = "PGHOST", default_value = "localhost")]
    host: String,
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    port: u16,
    #[arg(long, env = "PGUSER", default_value = "postgres")]
    user: String,
    /// TLS to the server. It applies to TCP only.
    #[arg(long, env = "PGSSLMODE", value_enum, default_value_t = SslMode::Disable)]
    sslmode: SslMode,
    /// PEM file of the CAs that `verify-full` trusts. Without it, it trusts
    /// the Mozilla roots.
    #[arg(long, env = "PGSSLROOTCERT")]
    sslrootcert: Option<PathBuf>,
}

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

impl ConnArgs {
    /// No TLS on a Unix socket, whatever `--sslmode` says, as libpq does.
    pub fn tls(&self) -> Result<Option<TlsConnector>> {
        if self.host.starts_with('/') {
            return Ok(None);
        }
        tls::connector(self.sslmode, self.sslrootcert.as_deref())
    }

    fn config(&self, application_name: String, idle_timeout: u64) -> ConnConfig {
        ConnConfig {
            user: self.user.clone(),
            password: std::env::var("PGPASSWORD").ok(),
            application_name,
            idle_timeout: Duration::from_secs(idle_timeout),
        }
    }

    pub async fn connect(&self, tls: Option<&TlsConnector>) -> std::io::Result<Box<dyn Io>> {
        if self.host.starts_with('/') {
            let socket = PathBuf::from(&self.host).join(format!(".s.PGSQL.{}", self.port));
            return Ok(Box::new(UnixStream::connect(socket).await?));
        }
        let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
        stream.set_nodelay(true)?;
        Ok(match tls {
            Some(tls) => Box::new(tls::connect(stream, tls, &self.host).await?),
            None => Box::new(stream),
        })
    }
}

#[derive(Args, Debug)]
struct ReceiveArgs {
    #[command(flatten)]
    conn: ConnArgs,
    /// Physical replication slot, created on first use.
    #[arg(long, default_value = "pgwal")]
    slot: String,
    #[arg(long, default_value = "pgwal")]
    application_name: String,
    /// Local store of the received WAL. One receiver at a time can use it.
    #[arg(long)]
    dir: PathBuf,
    #[command(flatten)]
    archive: archive::ArchiveArgs,
    /// Seconds between two uploads. A loss of this host loses at most that
    /// much WAL.
    #[arg(long, default_value_t = 2)]
    upload_every: u64,
    /// Longest silence accepted from the server, in seconds.
    #[arg(long, default_value_t = 60)]
    idle_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // The archive and the store hold every row and the password verifiers:
    // only this user reads what the command writes.
    // SAFETY: umask only sets the mask of the process.
    unsafe {
        libc::umask(0o077);
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // stdout is for command output, such as the end LSN of pgwal restore.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().cmd {
        Cmd::Receive(args) => run_receive(args).await,
        Cmd::Backup(args) => backup::run(args).await,
        Cmd::Fetch(args) => backup::run_fetch(args).await,
        Cmd::Restore(args) => restore::run(args).await,
        Cmd::Plan(args) => restore::run_plan(args),
        Cmd::Rebase(args) => rebase::run(args).await,
        Cmd::Verify(args) => verify::run_verify(args),
    }
}

async fn run_receive(args: ReceiveArgs) -> Result<()> {
    let tls = args.conn.tls()?;
    let cfg = StreamConfig {
        conn: args.conn.config(args.application_name, args.idle_timeout),
        slot: args.slot,
        dir: args.dir,
        upload_every: Duration::from_secs(args.upload_every),
        confirm: Confirm::Flushed,
    };
    let sink = args.archive.open()?;

    let (stop_tx, mut stop) = watch::channel(false);
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        info!("stopping");
        let _ = stop_tx.send(true);
    });

    let mut backoff = MIN_BACKOFF;
    loop {
        let stopped = {
            let mut stop = stop.clone();
            // A signal gives no flush position of the server.
            async move {
                let _ = stop.wait_for(|stopped| *stopped).await;
                None
            }
        };
        let connected = tokio::select! {
            connected = tokio::time::timeout(CONNECT_TIMEOUT, args.conn.connect(tls.as_ref())) => {
                connected.unwrap_or_else(|_| {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("no connection in {CONNECT_TIMEOUT:?}"),
                    ))
                })
            }
            _ = stop.wait_for(|stopped| *stopped) => return Ok(()),
        };
        let received = match connected {
            Ok(stream) => receive(stream, &cfg, sink.clone(), stopped).await,
            Err(e) => Err(e.into()),
        };
        match received {
            Ok(lsn) if *stop.borrow() => {
                info!(%lsn, "archived");
                return Ok(());
            }
            Ok(lsn) => {
                info!(%lsn, "the server ended the stream");
                backoff = MIN_BACKOFF;
            }
            Err(e @ (Error::Config(_) | Error::SystemMismatch { .. } | Error::WalGone { .. })) => {
                bail!(e)
            }
            // A server that refuses TLS, or a certificate that fails the
            // check, fails every retry the same way.
            Err(Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::Unsupported | std::io::ErrorKind::PermissionDenied
                ) =>
            {
                bail!(e)
            }
            // The WAL that the last upload could not send stays in the store.
            Err(e) if *stop.borrow() => bail!(e),
            Err(e) => warn!(retry_in = ?backoff, "receive failed: {e}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stop.wait_for(|stopped| *stopped) => return Ok(()),
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

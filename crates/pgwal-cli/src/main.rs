use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use pgwal_receiver::{Error, Lsn, StreamConfig, WalSink, receive};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::watch;
use tokio_rustls::TlsConnector;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod rebase;
mod restore;
mod sink;
mod tls;

use sink::DirSink;
use tls::SslMode;

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

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
    Verify(rebase::VerifyArgs),
}

#[derive(Args, Debug)]
struct ReceiveArgs {
    /// Server host, or the directory of its Unix socket when it starts with
    /// `/`. The password comes from PGPASSWORD only.
    #[arg(long, env = "PGHOST", default_value = "localhost")]
    host: String,
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    port: u16,
    /// A role with the REPLICATION attribute.
    #[arg(long, env = "PGUSER", default_value = "postgres")]
    user: String,
    /// TLS to the server. It applies to TCP only.
    #[arg(long, env = "PGSSLMODE", value_enum, default_value_t = SslMode::Disable)]
    sslmode: SslMode,
    /// PEM file of the CAs that `verify-full` trusts. Without it, it trusts
    /// the Mozilla roots.
    #[arg(long, env = "PGSSLROOTCERT")]
    sslrootcert: Option<PathBuf>,
    /// Physical replication slot, created on first use.
    #[arg(long, default_value = "pgwal")]
    slot: String,
    #[arg(long, default_value = "pgwal")]
    application_name: String,
    /// Local store of the received WAL. One receiver at a time can use it.
    #[arg(long)]
    dir: PathBuf,
    /// Directory that receives the archived objects.
    #[arg(long)]
    archive: PathBuf,
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
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // stdout is for command output, such as the end LSN of pgwal restore.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().cmd {
        Cmd::Receive(args) => run_receive(args).await,
        Cmd::Restore(args) => restore::run(args),
        Cmd::Plan(args) => restore::run_plan(args),
        Cmd::Rebase(args) => rebase::run(args),
        Cmd::Verify(args) => rebase::run_verify(args),
    }
}

async fn run_receive(args: ReceiveArgs) -> Result<()> {
    if args.host.starts_with('/') && args.sslmode != SslMode::Disable {
        bail!(
            "--sslmode applies to TCP only, not to the Unix socket in {}",
            args.host
        );
    }
    let tls = tls::connector(args.sslmode, args.sslrootcert.as_deref())?;
    let cfg = StreamConfig {
        slot: args.slot,
        user: args.user,
        password: std::env::var("PGPASSWORD").ok(),
        application_name: args.application_name,
        dir: args.dir,
        upload_every: Duration::from_secs(args.upload_every),
        idle_timeout: Duration::from_secs(args.idle_timeout),
    };
    let sink: Arc<dyn WalSink> = Arc::new(DirSink::new(args.archive));

    let (stop_tx, mut stop) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("stopping");
            let _ = stop_tx.send(true);
        }
    });

    let mut backoff = MIN_BACKOFF;
    loop {
        let stopped = {
            let mut stop = stop.clone();
            async move {
                let _ = stop.wait_for(|stopped| *stopped).await;
            }
        };
        match receive_once(
            &args.host,
            args.port,
            tls.as_ref(),
            &cfg,
            sink.clone(),
            stopped,
        )
        .await
        {
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
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::Unsupported => bail!(e),
            Err(e) => warn!(retry_in = ?backoff, "receive failed: {e}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stop.wait_for(|stopped| *stopped) => return Ok(()),
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn receive_once(
    host: &str,
    port: u16,
    tls: Option<&TlsConnector>,
    cfg: &StreamConfig,
    sink: Arc<dyn WalSink>,
    stop: impl Future<Output = ()>,
) -> Result<Lsn, Error> {
    if host.starts_with('/') {
        let socket = PathBuf::from(host).join(format!(".s.PGSQL.{port}"));
        receive(UnixStream::connect(socket).await?, cfg, sink, stop).await
    } else {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        match tls {
            Some(tls) => receive(tls::connect(stream, tls, host).await?, cfg, sink, stop).await,
            None => receive(stream, cfg, sink, stop).await,
        }
    }
}

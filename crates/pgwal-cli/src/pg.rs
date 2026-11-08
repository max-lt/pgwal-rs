//! The Postgres tools that rebase and verify run, and the signals that stop
//! them.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::signal::unix::{SignalKind, signal};

/// Stops the long commands of rebase and verify on Ctrl-C or SIGTERM. The
/// process then stops its private server and removes its files before it
/// exits.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

pub fn watch_signals() {
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        INTERRUPTED.store(true, Ordering::SeqCst);
    });
    if let Ok(mut terminate) = signal(SignalKind::terminate()) {
        tokio::spawn(async move {
            terminate.recv().await;
            INTERRUPTED.store(true, Ordering::SeqCst);
        });
    }
}

/// Fails once Ctrl-C or SIGTERM came.
pub fn check_interrupted() -> Result<()> {
    if INTERRUPTED.load(Ordering::SeqCst) {
        bail!("interrupted");
    }
    Ok(())
}

/// The Postgres binaries: in a directory, or on PATH.
pub struct Bin(pub Option<PathBuf>);

impl Bin {
    /// The output of `pg_controldata` is translated in other locales.
    fn command(&self, tool: &str) -> Command {
        let mut command = match &self.0 {
            Some(dir) => Command::new(dir.join(tool)),
            None => Command::new(tool),
        };
        command.env("LC_ALL", "C");
        command
    }

    /// Runs `tool`, and returns its stdout. A failure carries its stderr.
    /// Ctrl-C or SIGTERM stops it.
    pub fn run(&self, tool: &str, args: &[&str]) -> Result<String> {
        self.exec(tool, args, OnSignal::Stop)
    }

    pub fn exec(&self, tool: &str, args: &[&str], on_signal: OnSignal) -> Result<String> {
        let mut child = self
            .command(tool)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("running {tool}"))?;
        let stdout = read_all(child.stdout.take().expect("piped"));
        let stderr = read_all(child.stderr.take().expect("piped"));
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if on_signal == OnSignal::Stop && INTERRUPTED.load(Ordering::SeqCst) {
                let _ = child.kill();
                let _ = child.wait();
                bail!("interrupted during {tool}");
            }
            std::thread::sleep(CHILD_POLL);
        };
        let stdout = stdout.join().expect("the reader thread ends");
        let stderr = stderr.join().expect("the reader thread ends");
        if !status.success() {
            bail!(
                "{tool} failed: {}{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }
}

/// What a signal does to a command.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OnSignal {
    Stop,
    /// For the stop of a server, which must end even after a signal.
    Finish,
}

const CHILD_POLL: Duration = Duration::from_millis(20);

/// Reads `pipe` to its end in a thread, so that a full pipe never blocks
/// the command.
fn read_all(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut data = Vec::new();
        let _ = pipe.read_to_end(&mut data);
        data
    })
}

pub fn path(p: &Path) -> &str {
    p.to_str().expect("a UTF-8 path")
}

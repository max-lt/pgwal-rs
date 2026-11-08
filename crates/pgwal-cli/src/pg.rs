//! The Postgres tools that rebase and verify run, and the signals that stop
//! them.

use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::signal::unix::{SignalKind, signal};

/// Set on a watched signal.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Sets the flag of [`check_interrupted`] on a stop signal.
pub fn watch_signals() {
    on_stop_signal(|| INTERRUPTED.store(true, Ordering::SeqCst));
}

/// Calls `on` at SIGINT, SIGTERM or SIGHUP, as of a closed ssh session. A
/// signal that the process ignores, as nohup makes SIGHUP, stays ignored.
pub fn on_stop_signal(on: impl Fn() + Clone + Send + 'static) {
    for (kind, number) in [
        (SignalKind::interrupt(), libc::SIGINT),
        (SignalKind::terminate(), libc::SIGTERM),
        (SignalKind::hangup(), libc::SIGHUP),
    ] {
        if ignored(number) {
            continue;
        }
        if let Ok(mut stream) = signal(kind) {
            let on = on.clone();
            tokio::spawn(async move {
                stream.recv().await;
                on();
            });
        }
    }
}

fn ignored(number: libc::c_int) -> bool {
    // SAFETY: with no new action, sigaction only reads the current one into
    // a zeroed struct.
    unsafe {
        let mut current: libc::sigaction = std::mem::zeroed();
        libc::sigaction(number, std::ptr::null(), &mut current) == 0
            && current.sa_sigaction == libc::SIG_IGN
    }
}

/// Fails once a watched signal came.
pub fn check_interrupted() -> Result<()> {
    if INTERRUPTED.load(Ordering::SeqCst) {
        bail!("interrupted");
    }
    Ok(())
}

/// The Postgres binaries: in a directory, or on PATH.
pub struct Bin(pub Option<PathBuf>);

impl Bin {
    /// LC_ALL=C: the output of `pg_controldata` is translated in other
    /// locales. A process group of its own: a Ctrl-C in the terminal reaches
    /// pgwal only, which then stops the tool or lets it end.
    fn command(&self, tool: &str) -> Command {
        let mut command = match &self.0 {
            Some(dir) => Command::new(dir.join(tool)),
            None => Command::new(tool),
        };
        command.env("LC_ALL", "C").process_group(0);
        command
    }

    /// Runs `tool`, and returns its stdout. A failure carries its stderr.
    /// A watched signal stops it.
    pub fn run(&self, tool: &str, args: &[&str]) -> Result<String> {
        self.exec(tool, args, OnSignal::Stop)
    }

    pub fn exec(&self, tool: &str, args: &[&str], on_signal: OnSignal) -> Result<String> {
        let (status, stdout, stderr) = self.output(tool, args, on_signal)?;
        if !status.success() {
            bail!(
                "{tool} failed: {}{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }

    /// The exit code of `tool`, for a tool that answers with it.
    pub fn exit_code(&self, tool: &str, args: &[&str]) -> Result<Option<i32>> {
        Ok(self.output(tool, args, OnSignal::Stop)?.0.code())
    }

    fn output(
        &self,
        tool: &str,
        args: &[&str],
        on_signal: OnSignal,
    ) -> Result<(ExitStatus, Vec<u8>, Vec<u8>)> {
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
        Ok((status, stdout, stderr))
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

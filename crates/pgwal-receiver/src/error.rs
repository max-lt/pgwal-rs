/// A retry gets `Config`, `SystemMismatch` and `WalGone` again.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// `code` is the SQLSTATE.
    #[error("postgres {code}: {message}")]
    Db { code: String, message: String },
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("invalid config: {0}")]
    Config(String),
    #[error("store: {0}")]
    Store(String),
    /// One directory per cluster.
    #[error("store holds WAL of system {found}, not {expected}")]
    SystemMismatch { found: String, expected: String },
    /// The server no longer has the WAL from `requested`: its slot was
    /// invalidated, or the segment was removed. The archive has a gap.
    #[error("the server no longer has the WAL from {requested}: {message}")]
    WalGone {
        requested: pgwal::Lsn,
        message: String,
    },
}

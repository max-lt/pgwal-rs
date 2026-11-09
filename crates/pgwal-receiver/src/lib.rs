//! Streams the WAL of a server over physical replication and uploads it to a sink,
//! as objects keyed by LSN range.
//!
//! With [`Confirm::Flushed`], the receiver confirms a flush to Postgres once
//! the WAL is fsynced on local disk, not once it is in the sink. A loss of the receiver's
//! host then loses at most the WAL of one upload interval. With
//! [`Confirm::Uploaded`], it confirms once the WAL is in the sink.
//!
//! [`pgwal::plan`] lists the objects that the restore of a copy reads, and
//! [`pgwal::write_segment`] writes them into its `pg_wal`. The `pgwal
//! restore` command of `pgwal-cli` does both.
//!
//! ```no_run
//! # async fn run(sink: std::sync::Arc<dyn pgwal_receiver::WalSink>) -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//!
//! use pgwal_receiver::{Confirm, ConnConfig, Error, OnGap, StreamConfig, receive};
//!
//! let cfg = StreamConfig {
//!     conn: ConnConfig {
//!         user: "replicator".into(),
//!         password: None,
//!         application_name: "pgwal".into(),
//!         idle_timeout: Duration::from_secs(60),
//!     },
//!     slot: "pgwal".into(),
//!     dir: "/var/lib/pgwal/db1".into(),
//!     upload_every: Duration::from_secs(2),
//!     confirm: Confirm::Uploaded,
//!     on_gap: OnGap::Refuse,
//! };
//! loop {
//!     let stream = tokio::net::TcpStream::connect("db1.example:5432").await?;
//!     match receive(stream, &cfg, sink.clone(), std::future::pending()).await {
//!         Ok(_) => {}
//!         Err(e @ (Error::Config(_) | Error::SystemMismatch { .. } | Error::WalGone { .. })) => {
//!             return Err(e.into());
//!         }
//!         Err(e) => eprintln!("retrying: {e}"),
//!     }
//!     tokio::time::sleep(Duration::from_secs(1)).await;
//! }
//! # }
//! ```

#[cfg(feature = "runtime")]
mod backup;
#[cfg(feature = "runtime")]
mod dir;
mod error;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "runtime")]
mod store;
#[cfg(feature = "runtime")]
mod stream;
#[cfg(feature = "runtime")]
mod upload;
pub mod wire;

#[cfg(feature = "runtime")]
pub use backup::base_backup;
#[cfg(feature = "runtime")]
pub use dir::DirSink;
pub use error::Error;
#[cfg(feature = "runtime")]
pub use stream::{Confirm, OnGap, StreamConfig, forget_slot, receive};
#[cfg(feature = "runtime")]
pub use upload::{WalSink, salvage};
pub use wire::ConnConfig;

pub use pgwal::{ArchivedBackup, Lsn, RangeKey};

/// Largest object put in a sink, so that memory stays bounded with 1 GiB
/// segments.
#[cfg(feature = "runtime")]
const MAX_OBJECT: usize = 16 << 20;

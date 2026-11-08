//! Streams the WAL of a server over physical replication and uploads it to a sink,
//! as objects keyed by LSN range.
//!
//! By default the receiver confirms a flush to Postgres once the WAL is
//! fsynced on local disk, not once it is in the sink. A loss of the receiver's
//! host then loses at most the WAL of one upload interval. With
//! [`Confirm::Uploaded`], it confirms once the WAL is in the sink.
//!
//! To restore, rebuild each segment with [`pgwal::assemble_segment`] from the
//! objects whose keys [`RangeKey::parse`] reads, and give the segments to Postgres
//! through `restore_command`.
//!
//! ```no_run
//! # #[cfg(feature = "s3")]
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//!
//! use pgwal_receiver::s3::{S3Config, S3Sink};
//! use pgwal_receiver::{Error, StreamConfig, receive};
//!
//! let s3 = S3Config::new("https://s3.example", "wal", "auto", "key id", "secret");
//! let sink = Arc::new(S3Sink::new(s3)?);
//! let cfg = StreamConfig::new("pgwal", "replicator", "/var/lib/pgwal/db1");
//! loop {
//!     let stream = tokio::net::TcpStream::connect("db1.example:5432").await?;
//!     match receive(stream, &cfg, sink.clone(), std::future::pending()).await {
//!         Ok(_) => {}
//!         Err(e @ (Error::Config(_) | Error::SystemMismatch { .. } | Error::WalGone { .. })) => {
//!             return Err(e.into());
//!         }
//!         Err(e) => eprintln!("retrying: {e}"),
//!     }
//!     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//! }
//! # }
//! ```

#[cfg(feature = "runtime")]
mod backup;
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
pub use backup::{BackupConfig, base_backup};
pub use error::Error;
#[cfg(feature = "runtime")]
pub use stream::{Confirm, StreamConfig, forget_slot, receive};
#[cfg(feature = "runtime")]
pub use upload::{WalSink, salvage};

pub use pgwal::{ArchivedBackup, Lsn, RangeKey};

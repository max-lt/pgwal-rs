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

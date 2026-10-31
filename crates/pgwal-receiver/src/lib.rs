//! Streams the WAL of a server over physical replication and uploads it to a sink,
//! as objects keyed by LSN range.
//!
//! The receiver confirms a flush to Postgres once the WAL is fsynced on local
//! disk, not once it is in the sink. A loss of the receiver's host therefore
//! loses at most the WAL of one upload interval.
//!
//! To restore, rebuild each segment with [`pgwal::assemble_segment`] from the
//! objects whose keys [`RangeKey::parse`] reads, and give the segments to Postgres
//! through `restore_command`.

mod error;
mod store;
mod stream;
mod upload;
mod wire;

pub use error::Error;
pub use stream::{StreamConfig, forget_slot, receive};
pub use upload::{WalSink, salvage};

pub use pgwal::{Lsn, RangeKey};

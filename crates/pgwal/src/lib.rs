//! The WAL archive format and the restore steps, in plain `std`.
//!
//! It has no dependency, so a small static binary can use it.

mod apply;
mod archive;
mod backup;
mod control;
mod lsn;
mod plan;

pub use apply::{AfterReplay, request_recovery, segment_size, write_history, write_segment};
pub use archive::{ArchiveKey, RangeKey, assemble_segment, timeline_start, timeline_switches};
pub use backup::{ArchivedBackup, BackupLabel};
pub use control::{ClusterState, ControlFile};
pub use lsn::{Lsn, ParseLsnError, parse_segment_file_name, segment_file_name};
pub use plan::{Origin, Recovery, ReplayPlan, ReplayStart, Segment, chain_end, plan};

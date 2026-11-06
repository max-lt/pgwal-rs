//! The WAL archive format and the restore steps, in plain `std`.
//!
//! It has no dependency, so a small static binary can use it.

mod apply;
mod archive;
mod backup;
mod control;
mod lsn;
mod plan;

pub use apply::{request_archive_recovery, segment_size, write_history, write_segment};
pub use archive::{
    RangeKey, assemble_segment, history_file_name, history_key, overlay_segment, parse_history_key,
    timeline_start,
};
pub use backup::BackupLabel;
pub use control::ControlFile;
pub use lsn::{Lsn, ParseLsnError, parse_segment_file_name, segment_file_name};
pub use plan::{Recovery, ReplayPlan, plan};

use std::io;
use std::path::Path;

use crate::lsn::Lsn;

/// `DBState` in `pg_control.h`.
const DB_SHUTDOWNED: u32 = 1;
const DB_SHUTDOWNED_IN_RECOVERY: u32 = 2;

/// `PG_CONTROL_VERSION` of Postgres 13 to 16, 17 and 18.
const VERSIONS: [u32; 3] = [1300, 1700, 1800];

/// How the server last stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterState {
    /// A clean stop of a primary. Postgres then ignores WAL written after
    /// the checkpoint, unless an archive recovery is requested.
    ShutDown,
    /// A clean stop of a standby, at a restartpoint.
    ShutDownInRecovery,
    /// No clean stop, as in a snapshot of a running server. Postgres replays
    /// the WAL after the checkpoint.
    Running,
}

/// The fields of `global/pg_control` a restore reads. They lead the file, at
/// the same offsets in Postgres 13 to 18.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlFile {
    pub system_id: u64,
    pub state: ClusterState,
    pub checkpoint: Lsn,
    /// Where replay starts.
    pub redo: Lsn,
    pub timeline: u32,
}

impl ControlFile {
    /// `None` if `bytes` is too short or from an unknown `pg_control` version.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let u32_at = |at: usize| Some(u32::from_ne_bytes(bytes.get(at..at + 4)?.try_into().ok()?));
        let u64_at = |at: usize| Some(u64::from_ne_bytes(bytes.get(at..at + 8)?.try_into().ok()?));
        if !VERSIONS.contains(&u32_at(8)?) {
            return None;
        }
        let state = match u32_at(16)? {
            DB_SHUTDOWNED => ClusterState::ShutDown,
            DB_SHUTDOWNED_IN_RECOVERY => ClusterState::ShutDownInRecovery,
            _ => ClusterState::Running,
        };
        let control = ControlFile {
            system_id: u64_at(0)?,
            state,
            checkpoint: Lsn(u64_at(32)?),
            redo: Lsn(u64_at(40)?),
            timeline: u32_at(48)?,
        };
        (control.redo <= control.checkpoint && control.timeline > 0).then_some(control)
    }

    /// Reads `global/pg_control` of the data directory `data_dir`.
    pub fn read(data_dir: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(data_dir.join("global/pg_control"))?;
        Self::parse(&bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unreadable pg_control"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control_bytes(version: u32, state: u32, checkpoint: u64, redo: u64, tli: u32) -> Vec<u8> {
        let mut b = vec![0u8; 296];
        b[0..8].copy_from_slice(&7401u64.to_ne_bytes());
        b[8..12].copy_from_slice(&version.to_ne_bytes());
        b[16..20].copy_from_slice(&state.to_ne_bytes());
        b[32..40].copy_from_slice(&checkpoint.to_ne_bytes());
        b[40..48].copy_from_slice(&redo.to_ne_bytes());
        b[48..52].copy_from_slice(&tli.to_ne_bytes());
        b
    }

    #[test]
    fn reads_the_leading_fields_and_refuses_unknown_versions() {
        let control =
            ControlFile::parse(&control_bytes(1800, 1, 0x300_0148, 0x300_0148, 1)).unwrap();
        assert_eq!(
            control,
            ControlFile {
                system_id: 7401,
                state: ClusterState::ShutDown,
                checkpoint: Lsn(0x300_0148),
                redo: Lsn(0x300_0148),
                timeline: 1,
            }
        );
        let running =
            ControlFile::parse(&control_bytes(1700, 6, 0x400_0000, 0x3F0_0000, 2)).unwrap();
        assert_eq!(running.state, ClusterState::Running);
        let standby = ControlFile::parse(&control_bytes(1300, 2, 1, 1, 1)).unwrap();
        assert_eq!(standby.state, ClusterState::ShutDownInRecovery);
        assert_eq!(ControlFile::parse(&control_bytes(1201, 1, 1, 1, 1)), None);
        assert_eq!(ControlFile::parse(&control_bytes(1900, 1, 1, 1, 1)), None);
        assert_eq!(ControlFile::parse(&control_bytes(1800, 1, 1, 2, 1)), None);
        assert_eq!(ControlFile::parse(&[0; 20]), None);
    }
}

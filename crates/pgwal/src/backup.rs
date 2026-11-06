use std::io;
use std::path::Path;

use crate::lsn::{Lsn, segment_file_name};

/// The `backup_label` of a base backup taken while the server ran. Its start
/// point overrides `pg_control`: a checkpoint during the copy moves
/// `pg_control` past WAL that the replay still needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupLabel {
    /// `START WAL LOCATION`: the replay starts here.
    pub start: Lsn,
    /// `START TIMELINE`.
    pub timeline: u32,
    /// The segment file that holds `start`.
    start_file: String,
}

impl BackupLabel {
    /// `None` without a `START WAL LOCATION` or a `START TIMELINE`.
    pub fn parse(text: &str) -> Option<Self> {
        let field = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
        };
        // START WAL LOCATION: 0/2000028 (file 000000010000000000000002)
        let (start, file) = field("START WAL LOCATION")?.split_once(" (file ")?;
        Some(BackupLabel {
            start: start.parse().ok()?,
            timeline: field("START TIMELINE")?.parse().ok()?,
            start_file: file.strip_suffix(')')?.to_owned(),
        })
    }

    /// Reads `backup_label` of `data_dir`. `None` when the data directory
    /// has none.
    pub fn read(data_dir: &Path) -> io::Result<Option<Self>> {
        let text = match std::fs::read_to_string(data_dir.join("backup_label")) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        Self::parse(&text)
            .map(Some)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unreadable backup_label"))
    }

    /// The WAL segment size: the one size under which `start` falls in the
    /// file the label names. `None` if no size or more than one size fits.
    pub fn segment_size(&self) -> Option<u64> {
        let mut fits = (20..=30).map(|shift| 1u64 << shift).filter(|&size| {
            segment_file_name(self.timeline, self.start.segment_no(size), size) == self.start_file
        });
        let size = fits.next()?;
        fits.next().is_none().then_some(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LABEL: &str = "START WAL LOCATION: 0/5A3C128 (file 000000010000000000000005)\n\
                         CHECKPOINT LOCATION: 0/5A3C180\n\
                         BACKUP METHOD: streamed\n\
                         BACKUP FROM: primary\n\
                         START TIME: 2026-11-06 15:00:00 UTC\n\
                         LABEL: pg_basebackup base backup\n\
                         START TIMELINE: 1\n";

    #[test]
    fn reads_the_start_and_the_segment_size() {
        let label = BackupLabel::parse(LABEL).unwrap();
        assert_eq!((label.start, label.timeline), (Lsn(0x5A3_C128), 1));
        assert_eq!(label.segment_size(), Some(16 << 20));
        let small = LABEL.replace("000000010000000000000005", "00000001000000000000005A");
        assert_eq!(
            BackupLabel::parse(&small).unwrap().segment_size(),
            Some(1 << 20)
        );
    }

    #[test]
    fn refuses_a_label_without_its_fields() {
        assert_eq!(BackupLabel::parse("LABEL: x\n"), None);
        assert_eq!(
            BackupLabel::parse(&LABEL.replace("START TIMELINE: 1\n", "")),
            None
        );
        let wrong = LABEL.replace("000000010000000000000005", "000000010000000000000099");
        assert_eq!(BackupLabel::parse(&wrong).unwrap().segment_size(), None);
    }
}

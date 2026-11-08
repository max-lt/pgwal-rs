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

/// A base backup in an archive, as its [`ArchiveKey::BackupDone`] object
/// describes it.
///
/// [`ArchiveKey::BackupDone`]: crate::ArchiveKey::BackupDone
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchivedBackup {
    pub system_id: u64,
    pub timeline: u32,
    /// The replay starts here.
    pub start: Lsn,
    /// The copy is consistent once the replay reaches this LSN.
    pub end: Lsn,
    /// `(name, parts)`: `base.tar`, `<oid>.tar` for each tablespace, and
    /// `backup_manifest`.
    pub streams: Vec<(String, u32)>,
    /// `(oid, location)` of each tablespace on the server.
    pub tablespaces: Vec<(u32, String)>,
}

impl ArchivedBackup {
    /// Inverse of [`ArchivedBackup::parse`].
    pub fn to_text(&self) -> String {
        let mut text = format!(
            "SYSTEM: {}\nTIMELINE: {}\nSTART: {}\nEND: {}\n",
            self.system_id, self.timeline, self.start, self.end
        );
        for (name, parts) in &self.streams {
            text.push_str(&format!("STREAM: {name} {parts}\n"));
        }
        for (oid, location) in &self.tablespaces {
            text.push_str(&format!("TABLESPACE: {oid} {location}\n"));
        }
        text
    }

    /// `None` for an unknown or missing field, or `start` after `end`.
    pub fn parse(text: &str) -> Option<Self> {
        let (mut system_id, mut timeline, mut start, mut end) = (None, None, None, None);
        let (mut streams, mut tablespaces) = (Vec::new(), Vec::new());
        for line in text.lines() {
            let (field, value) = line.split_once(": ")?;
            match field {
                "SYSTEM" => system_id = Some(value.parse().ok()?),
                "TIMELINE" => timeline = Some(value.parse().ok()?),
                "START" => start = Some(value.parse().ok()?),
                "END" => end = Some(value.parse().ok()?),
                "STREAM" => {
                    let (name, parts) = value.rsplit_once(' ')?;
                    streams.push((name.to_owned(), parts.parse().ok()?));
                }
                "TABLESPACE" => {
                    let (oid, location) = value.split_once(' ')?;
                    tablespaces.push((oid.parse().ok()?, location.to_owned()));
                }
                _ => return None,
            }
        }
        let backup = ArchivedBackup {
            system_id: system_id?,
            timeline: timeline?,
            start: start?,
            end: end?,
            streams,
            tablespaces,
        };
        (backup.start <= backup.end).then_some(backup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backup_reads_back_from_its_done_object() {
        let backup = ArchivedBackup {
            system_id: 7401,
            timeline: 2,
            start: Lsn(0x1_0200_0028),
            end: Lsn(0x1_0200_0100),
            streams: vec![("base.tar".into(), 3), ("backup_manifest".into(), 1)],
            tablespaces: vec![(16385, "/srv/ts one".into())],
        };
        assert_eq!(ArchivedBackup::parse(&backup.to_text()), Some(backup));
        assert_eq!(ArchivedBackup::parse("SYSTEM: 7401\nTIMELINE: 1\n"), None);
    }

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

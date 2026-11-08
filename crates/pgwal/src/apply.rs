//! Writes a restore makes into a data directory before Postgres starts.

use std::io;
use std::path::Path;

use crate::archive::{history_file_name, overlay_segment};
use crate::lsn::{Lsn, parse_segment_file_name, segment_file_name};

/// The size of the segments in `pg_wal`, from the size of any segment file.
pub fn segment_size(pg_wal: &Path) -> io::Result<u64> {
    for entry in std::fs::read_dir(pg_wal)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Every segment name parses with the smallest segment size.
        if parse_segment_file_name(name, 1 << 20).is_some() {
            let size = entry.metadata()?.len();
            if size.is_power_of_two() && (1 << 20..=1 << 30).contains(&size) {
                return Ok(size);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "no WAL segment in pg_wal",
    ))
}

/// Lays `ranges` over segment `segno` of `timeline` in `pg_wal`. A segment
/// that does not exist yet starts as zeros. Each range holds `end - start`
/// bytes, or nothing is written.
pub fn write_segment(
    pg_wal: &Path,
    timeline: u32,
    segno: u64,
    seg_size: u64,
    ranges: &[(Lsn, Lsn, impl AsRef<[u8]>)],
) -> io::Result<()> {
    let path = pg_wal.join(segment_file_name(timeline, segno, seg_size));
    let mut segment = match std::fs::read(&path) {
        Ok(bytes) if bytes.len() as u64 == seg_size => bytes,
        Ok(_) => vec![0; seg_size as usize],
        Err(e) if e.kind() == io::ErrorKind::NotFound => vec![0; seg_size as usize],
        Err(e) => return Err(e),
    };
    if let Some((start, end, wal)) = ranges
        .iter()
        .find(|(start, end, wal)| wal.as_ref().len() as u64 != end.0 - start.0)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "the archived WAL object {start}-{end} holds {} bytes",
                wal.as_ref().len()
            ),
        ));
    }
    overlay_segment(&mut segment, segno, seg_size, ranges);
    std::fs::write(path, segment)
}

pub fn write_history(pg_wal: &Path, timeline: u32, content: &[u8]) -> io::Result<()> {
    std::fs::write(pg_wal.join(history_file_name(timeline)), content)
}

/// What Postgres does once it has replayed the WAL in `pg_wal`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterReplay {
    /// It promotes to an unused timeline.
    Promote,
    /// It stays a standby on the timeline of the archive.
    StayStandby,
}

/// Makes the next start of Postgres an archive recovery that reads the WAL
/// already in `pg_wal`. Safe to call twice.
pub fn request_recovery(data_dir: &Path, after: AfterReplay) -> io::Result<()> {
    // Postgres takes standby.signal over recovery.signal, and a copy of a
    // standby holds one.
    let (signal, other) = match after {
        AfterReplay::Promote => ("recovery.signal", "standby.signal"),
        AfterReplay::StayStandby => ("standby.signal", "recovery.signal"),
    };
    match std::fs::remove_file(data_dir.join(other)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    std::fs::write(data_dir.join(signal), b"")?;
    // Postgres reads the WAL in pg_wal when the restore command fails.
    const RESTORE_COMMAND: &str = "restore_command = 'false'";
    let path = data_dir.join("postgresql.auto.conf");
    let mut text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    if text.lines().any(|line| line.trim() == RESTORE_COMMAND) {
        return Ok(());
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(RESTORE_COMMAND);
    text.push('\n');
    std::fs::write(path, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: u64 = 1 << 20;

    #[test]
    fn writes_over_an_existing_segment_and_starts_a_new_one_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(segment_file_name(1, 1, SEG)),
            vec![7u8; SEG as usize],
        )
        .unwrap();
        assert_eq!(segment_size(dir.path()).unwrap(), SEG);

        let ranges = [(Lsn(SEG), Lsn(SEG + 4), b"abcd".to_vec())];
        write_segment(dir.path(), 1, 1, SEG, &ranges).unwrap();
        write_segment(dir.path(), 2, 1, SEG, &ranges).unwrap();
        let old = std::fs::read(dir.path().join(segment_file_name(1, 1, SEG))).unwrap();
        let new = std::fs::read(dir.path().join(segment_file_name(2, 1, SEG))).unwrap();
        assert_eq!((&old[..4], old[4]), (&b"abcd"[..], 7));
        assert_eq!((&new[..4], new[4]), (&b"abcd"[..], 0));

        let short = [(Lsn(SEG), Lsn(SEG + 10), b"abc".to_vec())];
        assert!(write_segment(dir.path(), 1, 1, SEG, &short).is_err());
    }

    #[test]
    fn a_recovery_request_is_written_once() {
        let dir = tempfile::tempdir().unwrap();
        request_recovery(dir.path(), AfterReplay::Promote).unwrap();
        request_recovery(dir.path(), AfterReplay::Promote).unwrap();
        assert!(dir.path().join("recovery.signal").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("postgresql.auto.conf")).unwrap(),
            "restore_command = 'false'\n"
        );
        request_recovery(dir.path(), AfterReplay::StayStandby).unwrap();
        assert!(dir.path().join("standby.signal").exists());
        assert!(!dir.path().join("recovery.signal").exists());
        request_recovery(dir.path(), AfterReplay::Promote).unwrap();
        assert!(!dir.path().join("standby.signal").exists());

        // A file edited by hand can lack its last newline.
        let conf = dir.path().join("postgresql.auto.conf");
        std::fs::write(&conf, "work_mem = '4MB'").unwrap();
        request_recovery(dir.path(), AfterReplay::Promote).unwrap();
        assert_eq!(
            std::fs::read_to_string(&conf).unwrap(),
            "work_mem = '4MB'\nrestore_command = 'false'\n"
        );
    }
}

//! Writes a restore makes into a data directory before Postgres starts.

use std::io;
use std::path::Path;

use crate::archive::{history_file_name, overlay_segment};
use crate::lsn::{Lsn, segment_file_name};

/// The size of the segments in `pg_wal`, from the size of any segment file.
pub fn segment_size(pg_wal: &Path) -> io::Result<u64> {
    for entry in std::fs::read_dir(pg_wal)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.len() == 24 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
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
/// that does not exist yet starts as zeros.
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
    overlay_segment(&mut segment, segno, seg_size, ranges).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "an archived WAL object is shorter than its key",
        )
    })?;
    std::fs::write(path, segment)
}

pub fn write_history(pg_wal: &Path, timeline: u32, content: &[u8]) -> io::Result<()> {
    std::fs::write(pg_wal.join(history_file_name(timeline)), content)
}

/// Makes the next start of Postgres an archive recovery that reads the WAL
/// already in `pg_wal` and then promotes. Safe to call twice.
pub fn request_archive_recovery(data_dir: &Path) -> io::Result<()> {
    std::fs::write(data_dir.join("recovery.signal"), b"")?;
    append_missing(
        &data_dir.join("postgresql.auto.conf"),
        &[
            // The WAL is already in pg_wal; Postgres reads it there when the
            // restore command fails.
            "restore_command = 'false'",
            // No reads during the replay. Without them, Postgres does not
            // require settings at least as high as when the WAL was written.
            "hot_standby = off",
        ],
    )
}

/// Appends each line of `lines` that `path` does not hold yet.
fn append_missing(path: &Path, lines: &[&str]) -> io::Result<()> {
    let mut text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    let before = text.len();
    for line in lines {
        if !text.lines().any(|l| l.trim() == *line) {
            text.push_str(line);
            text.push('\n');
        }
    }
    if text.len() > before {
        std::fs::write(path, text)?;
    }
    Ok(())
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
    fn an_archive_recovery_request_is_written_once() {
        let dir = tempfile::tempdir().unwrap();
        request_archive_recovery(dir.path()).unwrap();
        request_archive_recovery(dir.path()).unwrap();
        assert!(dir.path().join("recovery.signal").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("postgresql.auto.conf")).unwrap(),
            "restore_command = 'false'\nhot_standby = off\n"
        );
    }
}

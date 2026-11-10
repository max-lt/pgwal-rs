//! Writes a restore makes into a data directory before Postgres starts.

use std::io;
use std::path::Path;

use crate::archive::{history_file_name, overlay_segment};
use crate::lsn::{Lsn, is_segment_size, parse_segment_file_name, segment_file_name};

/// The size of the segments in `pg_wal`, from the size of any segment file.
pub fn segment_size(pg_wal: &Path) -> io::Result<u64> {
    for entry in std::fs::read_dir(pg_wal)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Every segment name parses with the smallest segment size.
        if parse_segment_file_name(name, 1 << 20).is_some() {
            let size = entry.metadata()?.len();
            if is_segment_size(size) {
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

/// Where the replay of a restore stops, before the end of the archive.
/// Postgres then promotes to an unused timeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryTarget {
    /// After the record that ends at this LSN or first passes it.
    Lsn(Lsn),
    /// After the last transaction that committed up to this time, in a form
    /// that Postgres reads as `timestamptz`, such as `2026-11-10 14:03:00+00`.
    /// Postgres refuses the zone `Z` here.
    Time(String),
}

/// A line of `postgresql.auto.conf` that a restore wants.
enum Setting<'a> {
    Is(&'a str, String),
    /// Empty, when the file gives the setting a value.
    Clear(&'a str),
}

/// Makes the next start of Postgres an archive recovery that reads the WAL
/// already in `pg_wal`, up to its end. It clears a recovery target that the
/// copy holds. Safe to call twice.
pub fn request_recovery(data_dir: &Path, after: AfterReplay) -> io::Result<()> {
    let mut settings = vec![
        Setting::Clear("recovery_target_lsn"),
        Setting::Clear("recovery_target_time"),
    ];
    // A standby of a copy made with pg_basebackup -R, or of a standby, would
    // stream from the live primary.
    if after == AfterReplay::StayStandby {
        settings.push(Setting::Is("primary_conninfo", String::new()));
        settings.push(Setting::Is("primary_slot_name", String::new()));
    }
    request(data_dir, after, settings)
}

/// As [`request_recovery`] with [`AfterReplay::Promote`], but the replay
/// stops at `target`. Postgres does not start when the WAL in `pg_wal` ends
/// before the target, or when the target is before the point where the copy
/// is consistent.
pub fn request_recovery_to(data_dir: &Path, target: &RecoveryTarget) -> io::Result<()> {
    // Postgres refuses a second target, even an empty one, after the first:
    // the other one is cleared before.
    let (name, other, value) = match target {
        RecoveryTarget::Lsn(lsn) => (
            "recovery_target_lsn",
            "recovery_target_time",
            lsn.to_string(),
        ),
        RecoveryTarget::Time(time) => {
            // The value goes between quotes into a configuration file.
            let plain = |c: char| c.is_ascii_alphanumeric() || " :+-./".contains(c);
            if time.is_empty() || !time.chars().all(plain) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("the target time {time:?} is not a plain timestamp"),
                ));
            }
            ("recovery_target_time", "recovery_target_lsn", time.clone())
        }
    };
    let settings = vec![
        Setting::Clear(other),
        Setting::Is(name, value),
        // The default pauses the replay at the target.
        Setting::Is("recovery_target_action", "promote".into()),
    ];
    request(data_dir, AfterReplay::Promote, settings)
}

fn request(data_dir: &Path, after: AfterReplay, mut settings: Vec<Setting>) -> io::Result<()> {
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
    settings.insert(0, Setting::Is("restore_command", "false".into()));
    let path = data_dir.join("postgresql.auto.conf");
    let mut text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    let before = text.len();
    // The later lines win.
    for setting in &settings {
        let (name, value, wanted) = match setting {
            Setting::Is(name, value) => (*name, value.as_str(), true),
            Setting::Clear(name) => (*name, "", false),
        };
        let line = format!("{name} = '{value}'");
        let last = text
            .lines()
            .rfind(|line| line.split('=').next().is_some_and(|n| n.trim() == name));
        match last {
            Some(last) if last.trim() == line => continue,
            None if !wanted => continue,
            _ => {}
        }
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&line);
        text.push('\n');
    }
    if text.len() == before {
        return Ok(());
    }
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
        std::fs::write(
            dir.path().join("postgresql.auto.conf"),
            "primary_conninfo = 'host=db1'\n",
        )
        .unwrap();
        request_recovery(dir.path(), AfterReplay::StayStandby).unwrap();
        request_recovery(dir.path(), AfterReplay::StayStandby).unwrap();
        assert!(dir.path().join("standby.signal").exists());
        assert!(!dir.path().join("recovery.signal").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("postgresql.auto.conf")).unwrap(),
            "primary_conninfo = 'host=db1'\nrestore_command = 'false'\n\
             primary_conninfo = ''\nprimary_slot_name = ''\n"
        );
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

    #[test]
    fn a_target_is_written_and_a_later_request_clears_it() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join("postgresql.auto.conf");
        let read = || std::fs::read_to_string(&conf).unwrap();
        std::fs::write(dir.path().join("standby.signal"), b"").unwrap();

        request_recovery_to(dir.path(), &RecoveryTarget::Lsn(Lsn(0x300_0148))).unwrap();
        assert!(dir.path().join("recovery.signal").exists());
        assert!(!dir.path().join("standby.signal").exists());
        assert_eq!(
            read(),
            "restore_command = 'false'\nrecovery_target_lsn = '0/3000148'\n\
             recovery_target_action = 'promote'\n"
        );

        // One target at a time: Postgres refuses two.
        let time = RecoveryTarget::Time("2026-11-10 14:03:00+00".into());
        request_recovery_to(dir.path(), &time).unwrap();
        request_recovery_to(dir.path(), &time).unwrap();
        let text = read();
        assert!(text.ends_with(
            "recovery_target_lsn = ''\nrecovery_target_time = '2026-11-10 14:03:00+00'\n"
        ));

        request_recovery(dir.path(), AfterReplay::Promote).unwrap();
        assert!(
            read().ends_with("recovery_target_time = ''\n"),
            "{}",
            read()
        );

        let quote = RecoveryTarget::Time("2026' x".into());
        assert!(request_recovery_to(dir.path(), &quote).is_err());
    }
}

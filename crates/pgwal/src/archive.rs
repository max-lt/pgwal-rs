use crate::lsn::Lsn;

/// The key of one uploaded object: the WAL bytes `[start, end)` of `timeline`
/// in the cluster `system_id`. The keys of one timeline sort by LSN.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RangeKey {
    pub system_id: u64,
    pub timeline: u32,
    pub start: Lsn,
    pub end: Lsn,
}

impl std::fmt::Display for RangeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{:08X}/{:016X}-{:016X}",
            self.system_id, self.timeline, self.start.0, self.end.0
        )
    }
}

impl RangeKey {
    /// A key prefix of every range of `timeline` that starts in
    /// `[start, start + len)`. `len` is a power of two, and `start` a
    /// multiple of it. The prefix can match a few more ranges around.
    pub fn prefix(system_id: u64, timeline: u32, start: Lsn, len: u64) -> String {
        let varying = ((len - 1).ilog2() / 4 + 1) as usize;
        let start = format!("{:016X}", start.0);
        format!("{system_id}/{timeline:08X}/{}", &start[..16 - varying])
    }

    /// Accepts only the exact form `Display` writes.
    pub fn parse(key: &str) -> Option<Self> {
        let (system_id, rest) = key.split_once('/')?;
        let (timeline, range) = rest.split_once('/')?;
        let (start, end) = range.split_once('-')?;
        let parsed = RangeKey {
            system_id: system_id.parse().ok()?,
            timeline: u32::from_str_radix(timeline, 16).ok()?,
            start: Lsn(u64::from_str_radix(start, 16).ok()?),
            end: Lsn(u64::from_str_radix(end, 16).ok()?),
        };
        (parsed.start < parsed.end && parsed.to_string() == key).then_some(parsed)
    }
}

/// Rebuilds segment `segno` from uploaded ranges, padded to full size as
/// Postgres expects. Ranges may overlap or belong to other segments. `None` if
/// they leave a gap, or if an object is shorter than its key says.
pub fn assemble_segment(
    segno: u64,
    seg_size: u64,
    ranges: &[(Lsn, Lsn, impl AsRef<[u8]>)],
) -> Option<Vec<u8>> {
    let (base, limit) = (segno * seg_size, (segno + 1) * seg_size);
    let mut ranges: Vec<_> = ranges.iter().collect();
    ranges.sort_by_key(|(start, _, _)| *start);
    let mut out = Vec::with_capacity(seg_size as usize);
    for (start, end, wal) in ranges {
        let covered = base + out.len() as u64;
        if end.0 <= covered || start.0 >= limit {
            continue;
        }
        if start.0 > covered {
            return None;
        }
        let skip = (covered - start.0) as usize;
        let take = (end.0.min(limit) - covered) as usize;
        out.extend_from_slice(wal.as_ref().get(skip..skip + take)?);
    }
    out.resize(seg_size as usize, 0);
    Some(out)
}

/// Lays archived WAL over `segment`, the bytes of segment `segno`. Ranges may
/// overlap, leave gaps, or belong to other segments. Each range holds
/// `end - start` bytes.
pub(crate) fn overlay_segment(
    segment: &mut [u8],
    segno: u64,
    seg_size: u64,
    ranges: &[(Lsn, Lsn, impl AsRef<[u8]>)],
) {
    assert_eq!(segment.len() as u64, seg_size, "a whole segment");
    let (base, limit) = (segno * seg_size, (segno + 1) * seg_size);
    for (start, end, wal) in ranges {
        let (from, to) = (start.0.max(base), end.0.min(limit));
        if from >= to {
            continue;
        }
        let skip = (from - start.0) as usize;
        let len = (to - from) as usize;
        let at = (from - base) as usize;
        segment[at..at + len].copy_from_slice(&wal.as_ref()[skip..skip + len]);
    }
}

/// The Postgres name of the history file of `timeline`.
pub(crate) fn history_file_name(timeline: u32) -> String {
    format!("{timeline:08X}.history")
}

/// The key of an object in an archive. The objects of one cluster are under
/// `{system_id}/`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArchiveKey {
    Range(RangeKey),
    /// The history file of `timeline`.
    History {
        system_id: u64,
        timeline: u32,
    },
    /// Part `part` of the file `name` of the base backup that starts at
    /// `start`. The parts of a file, in order, make the file.
    BackupPart {
        system_id: u64,
        start: Lsn,
        name: String,
        part: u32,
    },
    /// The description of the base backup that starts at `start`. It goes
    /// last: a backup without it is incomplete.
    BackupDone {
        system_id: u64,
        start: Lsn,
    },
}

impl std::fmt::Display for ArchiveKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveKey::Range(range) => range.fmt(f),
            ArchiveKey::History {
                system_id,
                timeline,
            } => write!(f, "{system_id}/{}", history_file_name(*timeline)),
            ArchiveKey::BackupPart {
                system_id,
                start,
                name,
                part,
            } => write!(f, "{system_id}/base/{:016X}/{name}.{part:06}", start.0),
            ArchiveKey::BackupDone { system_id, start } => {
                write!(f, "{system_id}/base/{:016X}/done", start.0)
            }
        }
    }
}

impl ArchiveKey {
    /// Accepts only the exact form `Display` writes.
    pub fn parse(key: &str) -> Option<Self> {
        if let Some(range) = RangeKey::parse(key) {
            return Some(ArchiveKey::Range(range));
        }
        let (system_id, rest) = key.split_once('/')?;
        let system_id = system_id.parse().ok()?;
        let parsed = if let Some(name) = rest.strip_suffix(".history") {
            ArchiveKey::History {
                system_id,
                timeline: u32::from_str_radix(name, 16).ok()?,
            }
        } else {
            let (start, file) = rest.strip_prefix("base/")?.split_once('/')?;
            let start = Lsn(u64::from_str_radix(start, 16).ok()?);
            if file == "done" {
                ArchiveKey::BackupDone { system_id, start }
            } else {
                let (name, part) = file.rsplit_once('.')?;
                ArchiveKey::BackupPart {
                    system_id,
                    start,
                    name: name.to_owned(),
                    part: part.parse().ok()?,
                }
            }
        };
        (parsed.to_string() == key).then_some(parsed)
    }
}

/// The lines of a history file: each parent timeline, with the LSN where
/// it ends, oldest first. `1\t0/3000148\tno recovery target specified`
/// says that timeline 1 ends at 0/3000148.
pub fn timeline_switches(history: &str) -> Vec<(u32, Lsn)> {
    history
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let timeline = fields.next()?.trim().parse().ok()?;
            let end = fields.next()?.trim().parse().ok()?;
            Some((timeline, end))
        })
        .collect()
}

/// Where the timeline a history file describes begins: the end of its
/// parent.
pub fn timeline_start(history: &str) -> Option<Lsn> {
    timeline_switches(history).last().map(|(_, end)| *end)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: u64 = 1 << 20;
    const SYSTEM_ID: u64 = 7401;

    fn key(start: u64, end: u64) -> String {
        RangeKey {
            system_id: SYSTEM_ID,
            timeline: 1,
            start: Lsn(start),
            end: Lsn(end),
        }
        .to_string()
    }

    #[test]
    fn range_keys_round_trip_sort_and_stay_canonical() {
        let k = key(0x100_0000, 0x100_0148);
        assert_eq!(k, "7401/00000001/0000000001000000-0000000001000148");
        assert_eq!(RangeKey::parse(&k).unwrap().to_string(), k);
        assert!(key(9, 10) < key(0x10, 0x11));
        for bad in [
            "7401/00000001/10-20".to_owned(),
            key(0xA0, 0xB0).to_lowercase(),
            format!("+{k}"),
            "07401/00000001/0000000001000000-0000000001000148".to_owned(),
            "7401/00000001/0000000001000148-0000000001000000".to_owned(),
            "00000001/0000000001000000-0000000001000148".to_owned(),
        ] {
            assert_eq!(RangeKey::parse(&bad), None, "{bad}");
        }
    }

    #[test]
    fn a_prefix_matches_the_ranges_that_start_in_its_span() {
        let prefix = RangeKey::prefix(SYSTEM_ID, 1, Lsn(0x100_0000), 0x100_0000);
        assert_eq!(prefix, "7401/00000001/0000000001");
        assert!(key(0x100_0000, 0x100_0148).starts_with(&prefix));
        assert!(key(0x1FF_FF00, 0x200_0000).starts_with(&prefix));
        assert!(!key(0x200_0000, 0x200_0100).starts_with(&prefix));
    }

    #[test]
    fn assembles_overlapping_ranges_and_rejects_gaps() {
        let r = |s: u64, e: u64| (Lsn(SEG + s), Lsn(SEG + e), vec![s as u8; (e - s) as usize]);
        let seg = assemble_segment(1, SEG, &[r(4, 10), r(0, 6)]).unwrap();
        assert_eq!(seg.len(), SEG as usize);
        assert_eq!(&seg[..10], &[0, 0, 0, 0, 0, 0, 4, 4, 4, 4]);
        assert!(seg[10..].iter().all(|&b| b == 0));
        assert_eq!(assemble_segment(1, SEG, &[r(2, 6)]), None);
        let short = (Lsn(SEG), Lsn(SEG + 10), b"abc".to_vec());
        assert_eq!(assemble_segment(1, SEG, &[short]), None);
    }

    #[test]
    fn overlays_only_what_the_archive_covers() {
        let mut segment = vec![9u8; SEG as usize];
        let ranges = [
            (Lsn(SEG + 4), Lsn(SEG + 8), b"abcd".to_vec()),
            (Lsn(SEG - 2), Lsn(SEG + 2), b"xyzw".to_vec()),
            (Lsn(2 * SEG), Lsn(2 * SEG + 1), b"!".to_vec()),
        ];
        overlay_segment(&mut segment, 1, SEG, &ranges);
        assert_eq!(&segment[..10], b"zw\x09\x09abcd\x09\x09");
        assert!(segment[10..].iter().all(|&b| b == 9));
    }

    #[test]
    fn archive_keys_round_trip() {
        let keys = [
            ArchiveKey::Range(RangeKey::parse(&key(0x10, 0x20)).unwrap()),
            ArchiveKey::History {
                system_id: 7401,
                timeline: 2,
            },
            ArchiveKey::BackupPart {
                system_id: 7401,
                start: Lsn(0x1_0200_0028),
                name: "base.tar".into(),
                part: 2,
            },
            ArchiveKey::BackupDone {
                system_id: 7401,
                start: Lsn(0x1_0200_0028),
            },
        ];
        let texts = [
            "7401/00000001/0000000000000010-0000000000000020",
            "7401/00000002.history",
            "7401/base/0000000102000028/base.tar.000002",
            "7401/base/0000000102000028/done",
        ];
        for (key, text) in keys.iter().zip(texts) {
            assert_eq!(key.to_string(), text);
            assert_eq!(ArchiveKey::parse(text).as_ref(), Some(key));
        }
        for bad in [
            "7401/0000000002.history",
            "7401/base/2000028/done",
            "7401/base/0000000102000028/base.tar.2",
            "7401/uploaded",
        ] {
            assert_eq!(ArchiveKey::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn timeline_starts() {
        let history = "1\t0/3000148\tno recovery target specified\n\n2\t0/5000000\tno recovery target specified\n";
        assert_eq!(timeline_start(history), Some(Lsn(0x500_0000)));
        assert_eq!(
            timeline_switches(history),
            [(1, Lsn(0x300_0148)), (2, Lsn(0x500_0000))]
        );
        assert_eq!(timeline_start(""), None);
    }
}

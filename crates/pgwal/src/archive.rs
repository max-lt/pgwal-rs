use crate::lsn::Lsn;

/// The key of one uploaded object: the WAL bytes `[start, end)` of `timeline`
/// in the cluster `system_id`. Keys of one cluster sort by LSN.
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
/// overlap, leave gaps, or belong to other segments. `None` if an object is
/// shorter than its key says.
pub fn overlay_segment(
    segment: &mut [u8],
    segno: u64,
    seg_size: u64,
    ranges: &[(Lsn, Lsn, impl AsRef<[u8]>)],
) -> Option<()> {
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
        segment[at..at + len].copy_from_slice(wal.as_ref().get(skip..skip + len)?);
    }
    Some(())
}

/// The Postgres name of the history file of `timeline`.
pub fn history_file_name(timeline: u32) -> String {
    format!("{timeline:08X}.history")
}

/// Object key of the history file of `timeline` in the cluster `system_id`.
pub fn history_key(system_id: u64, timeline: u32) -> String {
    format!("{system_id}/{}", history_file_name(timeline))
}

/// Inverse of [`history_key`]: `(system_id, timeline)`.
pub fn parse_history_key(key: &str) -> Option<(u64, u32)> {
    let (system_id, name) = key.split_once('/')?;
    let timeline = u32::from_str_radix(name.strip_suffix(".history")?, 16).ok()?;
    let parsed = (system_id.parse().ok()?, timeline);
    (history_key(parsed.0, parsed.1) == key).then_some(parsed)
}

/// Where the timeline a history file describes begins: the switch point on
/// its last line, such as `1\t0/3000148\tno recovery target specified`.
pub fn timeline_start(history: &str) -> Option<Lsn> {
    history
        .lines()
        .rfind(|line| !line.trim().is_empty() && !line.starts_with('#'))?
        .split('\t')
        .nth(1)?
        .trim()
        .parse()
        .ok()
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
        assert_eq!(overlay_segment(&mut segment, 1, SEG, &ranges), Some(()));
        assert_eq!(&segment[..10], b"zw\x09\x09abcd\x09\x09");
        assert!(segment[10..].iter().all(|&b| b == 9));
        let short = [(Lsn(SEG), Lsn(SEG + 10), b"abc".to_vec())];
        assert_eq!(overlay_segment(&mut segment, 1, SEG, &short), None);
    }

    #[test]
    fn history_keys_and_timeline_starts() {
        assert_eq!(history_key(7401, 2), "7401/00000002.history");
        assert_eq!(parse_history_key("7401/00000002.history"), Some((7401, 2)));
        assert_eq!(parse_history_key("7401/0000000002.history"), None);
        assert_eq!(
            parse_history_key("7401/00000001/0000000001000000-0000000001000148"),
            None
        );
        let history = "1\t0/3000148\tno recovery target specified\n\n2\t0/5000000\tno recovery target specified\n";
        assert_eq!(timeline_start(history), Some(Lsn(0x500_0000)));
        assert_eq!(timeline_start(""), None);
    }
}

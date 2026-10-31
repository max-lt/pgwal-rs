use std::collections::BTreeMap;

use crate::{ControlFile, Lsn, RangeKey, parse_history_key};

/// How Postgres reaches the end of the replayed WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    /// The archive holds nothing after the image.
    Nothing,
    /// Crash recovery replays `pg_wal` up to `end`.
    Crash { end: Lsn },
    /// Postgres ignores WAL after a clean shutdown, and crash recovery does
    /// not follow a timeline switch or stop at a gap. An archive recovery
    /// replays up to `end`, then promotes to an unused timeline.
    Archive { end: Lsn },
}

/// The archived WAL a restored image needs.
#[derive(Debug)]
pub struct ReplayPlan {
    /// Timelines whose history file goes into `pg_wal`.
    pub histories: Vec<u32>,
    /// `(timeline, segno, objects)`, one entry per segment file.
    pub segments: Vec<(u32, u64, Vec<RangeKey>)>,
    pub recovery: Recovery,
    /// Where the archive has a gap on the image timeline, if it has one.
    pub gap: Option<Lsn>,
}

/// Plans the replay of the archive `keys` onto an image that `control`
/// describes: the image timeline after its checkpoint, and every later
/// timeline. At the first gap on the image timeline, the rest goes and the
/// replay promotes: a server that went on from the gap on the same timeline
/// would write other WAL at the LSNs the archive keeps after it.
pub fn plan<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    control: &ControlFile,
    seg_size: u64,
) -> ReplayPlan {
    let (mut ranges, mut histories) = (Vec::new(), Vec::new());
    for key in keys {
        if let Some(range) = RangeKey::parse(key) {
            let follows = (range.timeline == control.timeline && range.end > control.checkpoint)
                || range.timeline > control.timeline;
            if range.system_id == control.system_id && follows {
                ranges.push(range);
            }
        } else if let Some((system_id, timeline)) = parse_history_key(key)
            && system_id == control.system_id
        {
            histories.push(timeline);
        }
    }
    let gap = drop_after_gap(&mut ranges, control);

    let end = ranges.iter().map(|r| r.end).max();
    let later_timeline = ranges.iter().any(|r| r.timeline > control.timeline);
    let recovery = match end {
        None if gap.is_none() => Recovery::Nothing,
        None => Recovery::Archive {
            end: control.checkpoint,
        },
        Some(end) if control.clean_shutdown || later_timeline || gap.is_some() => {
            Recovery::Archive { end }
        }
        Some(end) => Recovery::Crash { end },
    };

    // An object that crosses a segment boundary goes to each segment it
    // touches. The uploader never makes one; another writer could.
    let mut segments: BTreeMap<(u32, u64), Vec<RangeKey>> = BTreeMap::new();
    for range in ranges {
        let last = Lsn(range.end.0 - 1).segment_no(seg_size);
        for segno in range.start.segment_no(seg_size)..=last {
            segments
                .entry((range.timeline, segno))
                .or_default()
                .push(range);
        }
    }
    let segments = segments
        .into_iter()
        .map(|((timeline, segno), keys)| (timeline, segno, drop_contained(keys)))
        .collect();
    ReplayPlan {
        histories,
        segments,
        recovery,
        gap,
    }
}

/// Drops the keys whose range lies inside another key's range. A compaction
/// that stopped between its upload and its deletes leaves both.
fn drop_contained(mut keys: Vec<RangeKey>) -> Vec<RangeKey> {
    keys.sort_by(|a, b| a.start.cmp(&b.start).then(b.end.cmp(&a.end)));
    let mut reached = Lsn(0);
    keys.retain(|key| {
        let kept = key.end > reached;
        reached = reached.max(key.end);
        kept
    });
    keys
}

/// Keeps the ranges of the image timeline that chain from the checkpoint
/// without a gap. After a gap, drops the rest, later timelines included, and
/// returns where the gap starts.
fn drop_after_gap(ranges: &mut Vec<RangeKey>, control: &ControlFile) -> Option<Lsn> {
    let mut chain: Vec<&RangeKey> = ranges
        .iter()
        .filter(|r| r.timeline == control.timeline)
        .collect();
    chain.sort_by_key(|r| r.start);
    let mut reached = control.checkpoint;
    for range in chain {
        if range.start > reached {
            let gap = reached;
            ranges.retain(|r| r.timeline == control.timeline && r.start <= gap);
            return Some(gap);
        }
        reached = reached.max(range.end);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: u64 = 1 << 20;

    fn control() -> ControlFile {
        ControlFile {
            system_id: 7,
            clean_shutdown: false,
            checkpoint: Lsn(SEG + 0x100),
            redo: Lsn(SEG + 0x100),
            timeline: 1,
        }
    }

    fn key(timeline: u32, start: u64, end: u64) -> String {
        RangeKey {
            system_id: 7,
            timeline,
            start: Lsn(start),
            end: Lsn(end),
        }
        .to_string()
    }

    #[test]
    fn a_chain_after_the_checkpoint_gets_crash_recovery() {
        let keys = [key(1, SEG, SEG + 0x200), key(1, SEG + 0x200, SEG + 0x300)];
        let plan = plan(keys.iter().map(String::as_str), &control(), SEG);
        assert_eq!(
            plan.recovery,
            Recovery::Crash {
                end: Lsn(SEG + 0x300)
            }
        );
        assert_eq!(plan.gap, None);
        assert_eq!(plan.segments.len(), 1);
    }

    #[test]
    fn a_gap_drops_the_rest_and_promotes() {
        let keys = [
            key(1, SEG, SEG + 0x200),
            key(1, SEG + 0x400, SEG + 0x500),
            key(2, SEG + 0x500, SEG + 0x600),
        ];
        let plan = plan(keys.iter().map(String::as_str), &control(), SEG);
        assert_eq!(plan.gap, Some(Lsn(SEG + 0x200)));
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x200)
            }
        );
    }

    #[test]
    fn a_whole_segment_object_hides_its_parts() {
        let keys = [
            key(1, SEG, 2 * SEG),
            key(1, SEG, SEG + 0x200),
            key(1, SEG + 0x200, 2 * SEG),
        ];
        let plan = plan(keys.iter().map(String::as_str), &control(), SEG);
        let (_, _, objects) = &plan.segments[0];
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].end, Lsn(2 * SEG));
    }
}

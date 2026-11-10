use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::{ArchiveKey, BackupLabel, ClusterState, ControlFile, Lsn, RangeKey, segment_file_name};

/// How Postgres reaches the end of the replayed WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    /// The archive holds nothing after the copy.
    Nothing,
    /// Crash recovery replays `pg_wal` up to `end`.
    Crash { end: Lsn },
    /// Postgres ignores WAL after a clean shutdown, and crash recovery does
    /// not follow a timeline switch or stop at a gap. An archive recovery
    /// replays up to `end`, then promotes to an unused timeline.
    Archive { end: Lsn },
}

impl Recovery {
    pub fn end(self) -> Option<Lsn> {
        match self {
            Recovery::Nothing => None,
            Recovery::Crash { end } | Recovery::Archive { end } => Some(end),
        }
    }
}

/// What a copy of a data directory is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// A base backup with its `backup_label`.
    BaseBackup,
    /// A copy after a clean stop of a primary or of a standby.
    ShutDown,
    /// A copy of a server that did not stop cleanly, such as a snapshot.
    Running,
}

/// Where the replay of a copy of a data directory starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayStart {
    pub system_id: u64,
    pub timeline: u32,
    /// The replay needs the WAL from here: the start of `backup_label`, or
    /// the redo point of `pg_control`.
    pub from: Lsn,
    pub origin: Origin,
    pub seg_size: u64,
    /// `pg_wal` of the copy holds the segment of `from`. Without it, the
    /// archive must hold that segment from its first page, which Postgres
    /// reads first.
    pub has_start_segment: bool,
}

impl ReplayStart {
    /// Reads `backup_label` and `pg_control` of `data_dir`. The label wins.
    pub fn read(data_dir: &Path) -> io::Result<Self> {
        let control = ControlFile::read(data_dir)?;
        let label = BackupLabel::read(data_dir)?;
        let seg_size = control.seg_size;
        let (timeline, from, origin) = match label {
            Some(label) => (label.timeline, label.start, Origin::BaseBackup),
            None => {
                let origin = match control.state {
                    ClusterState::ShutDown | ClusterState::ShutDownInRecovery => Origin::ShutDown,
                    ClusterState::Running => Origin::Running,
                };
                (control.timeline, control.redo, origin)
            }
        };
        let name = segment_file_name(timeline, from.segment_no(seg_size), seg_size);
        Ok(ReplayStart {
            system_id: control.system_id,
            timeline,
            from,
            origin,
            seg_size,
            has_start_segment: data_dir.join("pg_wal").join(name).exists(),
        })
    }
}

/// The objects of one segment file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub timeline: u32,
    pub segno: u64,
    pub keys: Vec<RangeKey>,
}

/// The archived WAL a restored copy needs.
#[derive(Debug)]
pub struct ReplayPlan {
    /// Timelines whose history file goes into `pg_wal`.
    pub histories: Vec<u32>,
    pub segments: Vec<Segment>,
    pub recovery: Recovery,
    /// Where the archive has its first gap, if it has one.
    pub gap: Option<Lsn>,
}

/// Plans the replay of the archive `keys` onto the copy that `start`
/// describes: its timeline from the segment of `start.from`, then each
/// later timeline. `switches` are the lines of the newest history file,
/// from [`timeline_switches`](crate::timeline_switches): a timeline needs
/// its WAL up to its switch point only, and a later timeline that the file
/// does not list is left out. The replay stops at the first gap
/// and promotes, as a server that went on from the gap writes other WAL at
/// the LSNs after it.
pub fn plan<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    start: &ReplayStart,
    switches: &[(u32, Lsn)],
) -> ReplayPlan {
    let first = start.from.segment_start(start.seg_size);
    let (mut ranges, mut histories) = (Vec::new(), Vec::new());
    for key in keys {
        match ArchiveKey::parse(key) {
            Some(ArchiveKey::Range(range))
                if range.system_id == start.system_id
                    && ((range.timeline == start.timeline && range.end > first)
                        || range.timeline > start.timeline) =>
            {
                ranges.push(range);
            }
            Some(ArchiveKey::History {
                system_id,
                timeline,
            }) if system_id == start.system_id => histories.push(timeline),
            _ => {}
        }
    }
    // The replay follows the newest history file. A later timeline that it
    // does not list is another branch: its WAL is not on the way.
    let newest = (histories.iter().copied())
        .chain(ranges.iter().map(|r| r.timeline))
        .max();
    if !switches.is_empty() {
        ranges.retain(|r| {
            r.timeline == start.timeline
                || Some(r.timeline) == newest
                || switches.iter().any(|(tl, _)| *tl == r.timeline)
        });
    }
    let chain_from = if start.has_start_segment {
        start.from
    } else {
        first
    };
    let chain = chain_timelines(&mut ranges, start.timeline, chain_from, switches);
    let gap = chain.and_then(|chain| chain.gap);

    let later_timeline = ranges.iter().any(|r| r.timeline > start.timeline);
    let recovery = match chain {
        None => Recovery::Nothing,
        Some(chain) if start.origin != Origin::Running || later_timeline || gap.is_some() => {
            Recovery::Archive { end: chain.end }
        }
        Some(chain) => Recovery::Crash { end: chain.end },
    };

    // An object that crosses a segment boundary goes to each segment it
    // touches. The uploader never makes one; another writer could.
    let mut segments: BTreeMap<(u32, u64), Vec<RangeKey>> = BTreeMap::new();
    for range in ranges {
        let last = Lsn(range.end.0 - 1).segment_no(start.seg_size);
        for segno in range.start.segment_no(start.seg_size)..=last {
            segments
                .entry((range.timeline, segno))
                .or_default()
                .push(range);
        }
    }
    let segments = segments
        .into_iter()
        .map(|((timeline, segno), keys)| Segment {
            timeline,
            segno,
            keys: drop_contained(keys),
        })
        .collect();
    ReplayPlan {
        histories,
        segments,
        recovery,
        gap,
    }
}

/// How far the ranges of `timeline` hold the WAL from `from` with no hole.
pub fn chain_end<'a>(
    ranges: impl IntoIterator<Item = &'a RangeKey>,
    timeline: u32,
    from: Lsn,
) -> Lsn {
    let mut chain: Vec<&RangeKey> = ranges
        .into_iter()
        .filter(|r| r.timeline == timeline)
        .collect();
    chain.sort_by_key(|r| r.start);
    let mut reached = from;
    for range in chain {
        if range.start > reached {
            break;
        }
        reached = reached.max(range.end);
    }
    reached
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

/// How far the archive holds the WAL with no hole.
#[derive(Clone, Copy)]
struct Chain {
    end: Lsn,
    /// Where a hole starts. The chain ends there.
    gap: Option<Lsn>,
}

/// Chains each timeline in turn: `timeline` from `from`, a later one from
/// its first range, which must begin before the end of the chain so far. A
/// timeline with a switch point needs no WAL after it: the WAL there is of
/// another branch. At the first hole, drops the rest of the ranges. `None`
/// without ranges.
fn chain_timelines(
    ranges: &mut Vec<RangeKey>,
    timeline: u32,
    from: Lsn,
    switches: &[(u32, Lsn)],
) -> Option<Chain> {
    let mut timelines: Vec<u32> = ranges.iter().map(|r| r.timeline).collect();
    timelines.sort();
    timelines.dedup();
    let mut end = from;
    for &later in &timelines {
        let chain_from = if later == timeline {
            from
        } else {
            let first = ranges
                .iter()
                .filter(|r| r.timeline == later)
                .map(|r| r.start)
                .min()
                .expect("a timeline of the ranges has a range");
            if first > end {
                ranges.retain(|r| r.timeline < later);
                return Some(Chain {
                    end,
                    gap: Some(end),
                });
            }
            first
        };
        end = chain_end(ranges.iter(), later, chain_from);
        let switch = switches
            .iter()
            .find(|(tl, _)| *tl == later)
            .map(|(_, at)| *at);
        let hole = switch.is_none_or(|switch| end < switch)
            && ranges.iter().any(|r| r.timeline == later && r.start > end);
        if hole {
            ranges.retain(|r| r.timeline < later || (r.timeline == later && r.start <= end));
            return Some(Chain {
                end,
                gap: Some(end),
            });
        }
    }
    (!timelines.is_empty()).then_some(Chain { end, gap: None })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: u64 = 1 << 20;

    fn start() -> ReplayStart {
        ReplayStart {
            system_id: 7,
            timeline: 1,
            from: Lsn(SEG + 0x100),
            origin: Origin::Running,
            seg_size: SEG,
            has_start_segment: true,
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
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
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
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
        assert_eq!(plan.gap, Some(Lsn(SEG + 0x200)));
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x200)
            }
        );
    }

    #[test]
    fn the_head_of_the_start_segment_stays_in_the_plan() {
        // The start is SEG + 0x100: the object before it holds page 0.
        let keys = [key(1, SEG, SEG + 0x80), key(1, SEG + 0x80, SEG + 0x300)];
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
        assert_eq!(plan.gap, None);
        assert_eq!(plan.segments[0].keys.len(), 2);
    }

    #[test]
    fn a_whole_segment_object_hides_its_parts() {
        let keys = [
            key(1, SEG, 2 * SEG),
            key(1, SEG, SEG + 0x200),
            key(1, SEG + 0x200, 2 * SEG),
        ];
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
        let objects = &plan.segments[0].keys;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].end, Lsn(2 * SEG));
    }

    #[test]
    fn a_hole_on_a_later_timeline_stops_the_replay_there() {
        let keys = [
            key(1, SEG, SEG + 0x200),
            key(2, SEG + 0x200, SEG + 0x300),
            key(2, SEG + 0x400, SEG + 0x500),
            key(3, SEG + 0x500, SEG + 0x600),
        ];
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
        assert_eq!(plan.gap, Some(Lsn(SEG + 0x300)));
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x300)
            }
        );
        let kept: Vec<_> = plan.segments.iter().flat_map(|s| &s.keys).collect();
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn wal_of_a_timeline_after_its_switch_point_is_not_needed() {
        // A restore with --accept-gap promoted at SEG + 0x200, where timeline
        // 1 had a hole; the old primary went on past it.
        let keys = [
            key(1, SEG, SEG + 0x200),
            key(1, SEG + 0x400, SEG + 0x500),
            key(2, SEG, SEG + 0x900),
        ];
        let switches = [(1, Lsn(SEG + 0x200))];
        let plan = plan(keys.iter().map(String::as_str), &start(), &switches);
        assert_eq!(plan.gap, None);
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x900)
            }
        );
    }

    #[test]
    fn a_copy_without_its_start_segment_needs_its_first_page() {
        let keys = [key(1, SEG + 0x100, SEG + 0x300)];
        let bare = ReplayStart {
            has_start_segment: false,
            ..start()
        };
        let plan = plan(keys.iter().map(String::as_str), &bare, &[]);
        assert_eq!(plan.gap, Some(Lsn(SEG)));
    }

    #[test]
    fn a_later_timeline_must_meet_the_chain_before_it() {
        // Timeline 1 stops at SEG + 0x200; timeline 2 begins in the next
        // segment: the WAL between is missing.
        let keys = [key(1, SEG, SEG + 0x200), key(2, 3 * SEG, 3 * SEG + 0x100)];
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
        assert_eq!(plan.gap, Some(Lsn(SEG + 0x200)));
        assert!(plan.segments.iter().all(|s| s.timeline == 1));
    }

    #[test]
    fn the_end_is_the_end_of_the_last_timeline() {
        // The old primary wrote past the switch point before the failover.
        let keys = [key(1, SEG, SEG + 0x900), key(2, SEG + 0x200, SEG + 0x300)];
        let plan = plan(keys.iter().map(String::as_str), &start(), &[]);
        assert_eq!(plan.gap, None);
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x300)
            }
        );
    }

    #[test]
    fn a_clean_copy_gets_an_archive_recovery() {
        let keys = [key(1, SEG, SEG + 0x200)];
        let clean = ReplayStart {
            origin: Origin::ShutDown,
            ..start()
        };
        let plan = plan(keys.iter().map(String::as_str), &clean, &[]);
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x200)
            }
        );
    }

    #[test]
    fn a_timeline_off_the_history_does_not_hide_a_gap() {
        // Timeline 2 is a branch that timeline 3 never had: timeline 3 began
        // on timeline 1 at 3 * SEG + 0x80. The WAL of timeline 1 after
        // SEG + 0x200 is missing.
        let keys = [
            key(1, SEG, SEG + 0x200),
            key(2, SEG, 5 * SEG),
            key(3, 3 * SEG, 3 * SEG + 0x100),
            "7/00000002.history".to_owned(),
            "7/00000003.history".to_owned(),
        ];
        let switches = [(1, Lsn(3 * SEG + 0x80))];
        let plan = plan(keys.iter().map(String::as_str), &start(), &switches);
        assert_eq!(plan.gap, Some(Lsn(SEG + 0x200)));
        assert!(plan.segments.iter().all(|s| s.timeline == 1));
    }
}

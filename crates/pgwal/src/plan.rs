use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::{ArchiveKey, BackupLabel, ClusterState, ControlFile, Lsn, RangeKey, segment_size};

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
    /// The archive must hold the WAL from here: the start of `backup_label`,
    /// or the checkpoint of `pg_control`.
    pub from: Lsn,
    pub origin: Origin,
    pub seg_size: u64,
}

impl ReplayStart {
    /// Reads `backup_label` and `pg_control` of `data_dir`. The label wins:
    /// a checkpoint during the copy moves `pg_control` past WAL that the
    /// replay still needs.
    pub fn read(data_dir: &Path) -> io::Result<Self> {
        let control = ControlFile::read(data_dir)?;
        let label = BackupLabel::read(data_dir)?;
        let seg_size = match segment_size(&data_dir.join("pg_wal")) {
            Ok(size) => size,
            Err(e) => label
                .as_ref()
                .and_then(BackupLabel::segment_size)
                .ok_or(e)?,
        };
        Ok(match label {
            Some(label) => ReplayStart {
                system_id: control.system_id,
                timeline: label.timeline,
                from: label.start,
                origin: Origin::BaseBackup,
                seg_size,
            },
            None => ReplayStart {
                system_id: control.system_id,
                timeline: control.timeline,
                from: control.checkpoint,
                origin: match control.state {
                    ClusterState::ShutDown | ClusterState::ShutDownInRecovery => Origin::ShutDown,
                    ClusterState::Running => Origin::Running,
                },
                seg_size,
            },
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
/// describes: its timeline from `start.from`, then each later timeline.
/// The replay stops at the first gap and promotes, as a server that went on
/// from the gap writes other WAL at the LSNs after it.
pub fn plan<'a>(keys: impl IntoIterator<Item = &'a str>, start: &ReplayStart) -> ReplayPlan {
    let (mut ranges, mut histories) = (Vec::new(), Vec::new());
    for key in keys {
        match ArchiveKey::parse(key) {
            Some(ArchiveKey::Range(range))
                if range.system_id == start.system_id
                    && ((range.timeline == start.timeline && range.end > start.from)
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
    let (end, gap) = drop_after_gap(&mut ranges, start);

    let later_timeline = ranges.iter().any(|r| r.timeline > start.timeline);
    let recovery = match end {
        None if gap.is_none() => Recovery::Nothing,
        None => Recovery::Archive { end: start.from },
        Some(end) if start.origin != Origin::Running || later_timeline || gap.is_some() => {
            Recovery::Archive { end }
        }
        Some(end) => Recovery::Crash { end },
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

/// Chains each timeline in turn: the timeline of the copy from
/// `start.from`, a later one from its first range. At the first hole, drops
/// the rest of that timeline and every later timeline. Returns the end of
/// the last chain, and where the hole starts.
fn drop_after_gap(ranges: &mut Vec<RangeKey>, start: &ReplayStart) -> (Option<Lsn>, Option<Lsn>) {
    let mut timelines: Vec<u32> = ranges.iter().map(|r| r.timeline).collect();
    timelines.sort();
    timelines.dedup();
    let mut end = None;
    for timeline in timelines {
        let from = if timeline == start.timeline {
            start.from
        } else {
            ranges
                .iter()
                .filter(|r| r.timeline == timeline)
                .map(|r| r.start)
                .min()
                .expect("a timeline of the ranges has a range")
        };
        let reached = chain_end(ranges.iter(), timeline, from);
        end = Some(reached);
        let hole = ranges
            .iter()
            .any(|r| r.timeline == timeline && r.start > reached);
        if hole {
            ranges.retain(|r| {
                r.timeline < timeline || (r.timeline == timeline && r.start <= reached)
            });
            return (end, Some(reached));
        }
    }
    (end, None)
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
        let plan = plan(keys.iter().map(String::as_str), &start());
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
        let plan = plan(keys.iter().map(String::as_str), &start());
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
        let plan = plan(keys.iter().map(String::as_str), &start());
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
        let plan = plan(keys.iter().map(String::as_str), &start());
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
    fn the_end_is_the_end_of_the_last_timeline() {
        // The old primary wrote past the switch point before the failover.
        let keys = [key(1, SEG, SEG + 0x900), key(2, SEG + 0x200, SEG + 0x300)];
        let plan = plan(keys.iter().map(String::as_str), &start());
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
        let plan = plan(keys.iter().map(String::as_str), &clean);
        assert_eq!(
            plan.recovery,
            Recovery::Archive {
                end: Lsn(SEG + 0x200)
            }
        );
    }
}

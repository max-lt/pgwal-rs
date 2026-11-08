use std::fmt;
use std::str::FromStr;

/// A byte position in the WAL.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub fn segment_no(self, seg_size: u64) -> u64 {
        self.0 / seg_size
    }

    pub fn segment_offset(self, seg_size: u64) -> u64 {
        self.0 % seg_size
    }

    pub fn segment_start(self, seg_size: u64) -> Lsn {
        Lsn(self.0 - self.segment_offset(seg_size))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

#[derive(Debug)]
pub struct ParseLsnError(String);

impl fmt::Display for ParseLsnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid LSN {:?}", self.0)
    }
}

impl std::error::Error for ParseLsnError {}

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseLsnError(s.to_owned());
        let (hi, lo) = s.split_once('/').ok_or_else(err)?;
        let hi = u32::from_str_radix(hi, 16).map_err(|_| err())?;
        let lo = u32::from_str_radix(lo, 16).map_err(|_| err())?;
        Ok(Lsn(u64::from(hi) << 32 | u64::from(lo)))
    }
}

/// The `pg_wal` file name of segment `segno`, as Postgres names it.
pub fn segment_file_name(timeline: u32, segno: u64, seg_size: u64) -> String {
    let per_id = 0x1_0000_0000 / seg_size;
    format!("{timeline:08X}{:08X}{:08X}", segno / per_id, segno % per_id)
}

/// A segment size that Postgres accepts: a power of two from 1 MiB to 1 GiB.
pub fn is_segment_size(size: u64) -> bool {
    size.is_power_of_two() && (1 << 20..=1 << 30).contains(&size)
}

/// Inverse of [`segment_file_name`]: `(timeline, segno)`.
pub fn parse_segment_file_name(name: &str, seg_size: u64) -> Option<(u32, u64)> {
    if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let timeline = u32::from_str_radix(&name[..8], 16).ok()?;
    let id = u64::from_str_radix(&name[8..16], 16).ok()?;
    let seg = u64::from_str_radix(&name[16..], 16).ok()?;
    let per_id = 0x1_0000_0000 / seg_size;
    (seg < per_id).then_some((timeline, id * per_id + seg))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB16: u64 = 16 << 20;

    #[test]
    fn display_round_trips() {
        let lsn: Lsn = "16/B374D848".parse().unwrap();
        assert_eq!(lsn, Lsn(0x16_B374_D848));
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert!("16B374D848".parse::<Lsn>().is_err());
    }

    #[test]
    fn segment_names_match_postgres() {
        assert_eq!(segment_file_name(1, 1, MB16), "000000010000000000000001");
        assert_eq!(segment_file_name(1, 256, MB16), "000000010000000100000000");
        assert_eq!(
            parse_segment_file_name("000000010000000100000000", MB16),
            Some((1, 256))
        );
        assert_eq!(
            parse_segment_file_name("000000010000000000000100", MB16),
            None
        );
        assert_eq!(parse_segment_file_name("00000002.history", MB16), None);
    }
}

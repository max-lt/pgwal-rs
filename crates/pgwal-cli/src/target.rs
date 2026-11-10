//! The point where a restore stops, before the end of the archive.

use anyhow::{Context, Result, bail};
use clap::Args;
use pgwal::{Lsn, RecoveryTarget};

#[derive(Args, Debug, Default)]
pub struct TargetArgs {
    /// Stop at this LSN, such as `0/3000148`, instead of the end of the
    /// archive.
    #[arg(long, conflicts_with = "target_time")]
    target_lsn: Option<Lsn>,
    /// Stop at this time, in RFC 3339 with a zone, such as
    /// `2026-11-10T14:03:00Z` or `2026-11-10T15:03:00+01:00`.
    #[arg(long)]
    target_time: Option<String>,
}

/// A target, and its time in seconds since 1970 when it is a time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Lsn(Lsn),
    Time { text: String, unix: i64 },
}

impl TargetArgs {
    pub fn target(&self) -> Result<Option<Target>> {
        if let Some(lsn) = self.target_lsn {
            return Ok(Some(Target::Lsn(lsn)));
        }
        let Some(text) = &self.target_time else {
            return Ok(None);
        };
        let unix = rfc3339(text).with_context(|| {
            format!("--target-time {text:?} is not as 2026-11-10T14:03:00Z or with +01:00")
        })?;
        Ok(Some(Target::Time {
            text: text.clone(),
            unix,
        }))
    }
}

impl Target {
    pub fn recovery(&self) -> RecoveryTarget {
        match self {
            Target::Lsn(lsn) => RecoveryTarget::Lsn(*lsn),
            // Postgres refuses the `Z` zone in this setting.
            Target::Time { text, .. } => {
                RecoveryTarget::Time(match text.strip_suffix(['Z', 'z']) {
                    Some(utc) => format!("{utc}+00:00"),
                    None => text.clone(),
                })
            }
        }
    }
}

/// Seconds since 1970 of `date` at `time`, both in UTC: `2026-11-10` and
/// `14:03:00`. A fraction of a second is dropped.
fn unix(date: &str, time: &str) -> Option<i64> {
    let number = |text: &str, len: usize| {
        (text.len() == len && text.bytes().all(|b| b.is_ascii_digit()))
            .then(|| text.parse::<i64>().ok())
            .flatten()
    };
    let mut date = date.split('-');
    let (y, m, d) = (
        number(date.next()?, 4)?,
        number(date.next()?, 2)?,
        number(date.next()?, 2)?,
    );
    let time = time.split_once('.').map_or(time, |(whole, fraction)| {
        if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
            ""
        } else {
            whole
        }
    });
    let mut time = time.split(':');
    let (h, min, s) = (
        number(time.next()?, 2)?,
        number(time.next()?, 2)?,
        number(time.next()?, 2)?,
    );
    let valid = date.next().is_none()
        && time.next().is_none()
        && (1..=12).contains(&m)
        && (1..=31).contains(&d)
        && h < 24
        && min < 60
        && s < 61;
    if !valid {
        return None;
    }
    // Days from 1970-01-01, with the year counted from March.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + h * 3600 + min * 60 + s)
}

/// Seconds since 1970 of an RFC 3339 time with its zone.
fn rfc3339(text: &str) -> Option<i64> {
    let (date, rest) = text.split_once(['T', 't'])?;
    if let Some(time) = rest.strip_suffix(['Z', 'z']) {
        return unix(date, time);
    }
    let at = rest.rfind(['+', '-'])?;
    let (time, zone) = rest.split_at(at);
    let (zh, zm) = zone[1..].split_once(':')?;
    let digits = |text: &str| text.len() == 2 && text.bytes().all(|b| b.is_ascii_digit());
    if !digits(zh) || !digits(zm) {
        return None;
    }
    let offset = zh.parse::<i64>().ok()? * 3600 + zm.parse::<i64>().ok()? * 60;
    let sign = if zone.starts_with('-') { -1 } else { 1 };
    Some(unix(date, time)? - sign * offset)
}

/// When the backup with this `backup_manifest` started, in seconds since
/// 1970: the manifest gives the time of `backup_label` in GMT.
pub fn backup_time(manifest: &str) -> Result<i64> {
    const LABEL: &str = "\"Path\": \"backup_label\"";
    const MODIFIED: &str = "\"Last-Modified\": \"";
    let entry = manifest
        .split_once(LABEL)
        .and_then(|(_, rest)| rest.split_once('}'))
        .map(|(entry, _)| entry)
        .context("the backup manifest lists no backup_label")?;
    let stamp = entry
        .split_once(MODIFIED)
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(stamp, _)| stamp)
        .context("the backup manifest gives no time to backup_label")?;
    let parsed = stamp
        .strip_suffix(" GMT")
        .and_then(|stamp| stamp.split_once(' '))
        .and_then(|(date, time)| unix(date, time));
    match parsed {
        Some(unix) => Ok(unix),
        None => bail!("the backup manifest gives backup_label the time {stamp:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_rfc_3339_times() {
        assert_eq!(rfc3339("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339("2026-11-10T14:03:00Z"), Some(1_794_319_380));
        assert_eq!(rfc3339("2026-11-10T15:03:00+01:00"), Some(1_794_319_380));
        assert_eq!(
            rfc3339("2026-11-10T09:03:00.250-05:00"),
            Some(1_794_319_380)
        );
        assert_eq!(rfc3339("2024-02-29T00:00:00Z"), Some(1_709_164_800));
        for bad in [
            "2026-11-10 14:03:00",
            "2026-11-10T14:03:00",
            "2026-11-10T14:03Z",
            "2026-13-10T14:03:00Z",
            "2026-11-10T14:03:00+0100",
            "2026-11-10T14:03:00.Z",
            "2026-11-10T14:03:00' x'Z",
        ] {
            assert_eq!(rfc3339(bad), None, "{bad}");
        }
    }

    #[test]
    fn reads_the_start_time_of_a_backup() {
        let manifest = r#"{ "PostgreSQL-Backup-Manifest-Version": 2,
"Files": [
{ "Path": "backup_label", "Size": 225, "Last-Modified": "2026-11-10 14:03:00 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "8be75996" },
{ "Path": "PG_VERSION", "Size": 3, "Last-Modified": "2026-11-09 10:00:00 GMT" }
]}"#;
        assert_eq!(backup_time(manifest).unwrap(), 1_794_319_380);
        assert!(backup_time("{ \"Files\": [] }").is_err());
    }
}

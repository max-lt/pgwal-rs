//! The replication subset of the Postgres wire protocol, over any stream.
//!
//! Frames are decoded by hand: `postgres-protocol` cannot decode
//! `CopyBothResponse`. The server end is not trusted, so every
//! read is bounds-checked and frame sizes are capped.

use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pgwal::Lsn;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::Error;

/// Largest frame accepted. XLogData carries at most 128 kB of WAL.
const MAX_FRAME: usize = 16 << 20;

/// Microseconds from the Unix epoch to the Postgres epoch (2000-01-01).
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// SQLSTATE `duplicate_object`.
const DUPLICATE_OBJECT: &str = "42710";

/// SQLSTATE `undefined_object`.
const UNDEFINED_OBJECT: &str = "42704";

// Authentication request codes of the `R` message.
const AUTH_OK: i32 = 0;
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

pub(crate) struct SystemInfo {
    pub system_id: u64,
    pub timeline: u32,
    pub xlogpos: Lsn,
}

/// `wal_end` is the server's view of its WAL end: its flush position in
/// XLogData, what it has sent in a keepalive.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CopyMessage {
    XLogData {
        start: Lsn,
        wal_end: Lsn,
        data: Bytes,
    },
    Keepalive {
        wal_end: Lsn,
        reply_requested: bool,
    },
}

pub(crate) struct Conn<S> {
    stream: S,
    buf: BytesMut,
    /// Longest wait for the next bytes from the server.
    idle_timeout: Duration,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    /// Opens a physical replication connection. Without `password`, the
    /// backend must trust `user`. With it, the backend can ask for
    /// SCRAM-SHA-256.
    pub async fn connect(
        stream: S,
        user: &str,
        password: Option<&str>,
        application_name: &str,
        idle_timeout: Duration,
    ) -> Result<Self, Error> {
        let mut conn = Conn {
            stream,
            buf: BytesMut::with_capacity(256 * 1024),
            idle_timeout,
        };
        let mut out = BytesMut::new();
        frontend::startup_message(
            [
                ("user", user),
                ("replication", "true"),
                ("application_name", application_name),
            ],
            &mut out,
        )?;
        conn.stream.write_all(&out).await?;

        let mut scram = None;
        loop {
            let (tag, mut body) = conn.read_frame().await?;
            match tag {
                b'R' => match take_i32(&mut body)? {
                    AUTH_OK => {}
                    AUTH_SASL => {
                        let Some(password) = password else {
                            return Err(protocol("backend asked for a password; none given"));
                        };
                        if !body[..]
                            .split(|b| *b == 0)
                            .any(|m| m == SCRAM_SHA_256.as_bytes())
                        {
                            return Err(protocol("backend does not offer SCRAM-SHA-256"));
                        }
                        let client =
                            ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
                        let mut out = BytesMut::new();
                        frontend::sasl_initial_response(SCRAM_SHA_256, client.message(), &mut out)?;
                        conn.stream.write_all(&out).await?;
                        scram = Some(client);
                    }
                    AUTH_SASL_CONTINUE => {
                        let client = scram
                            .as_mut()
                            .ok_or_else(|| protocol("SASL continue before SASL start"))?;
                        client.update(&body)?;
                        let mut out = BytesMut::new();
                        frontend::sasl_response(client.message(), &mut out)?;
                        conn.stream.write_all(&out).await?;
                    }
                    AUTH_SASL_FINAL => scram
                        .as_mut()
                        .ok_or_else(|| protocol("SASL final before SASL start"))?
                        .finish(&body)?,
                    code => {
                        return Err(protocol(format!(
                            "backend asked for authentication method {code}; only trust and SCRAM-SHA-256 work"
                        )));
                    }
                },
                b'E' => return Err(db_error(body)),
                b'Z' => return Ok(conn),
                _ => {}
            }
        }
    }

    pub async fn identify_system(&mut self) -> Result<SystemInfo, Error> {
        let row = self.single_row("IDENTIFY_SYSTEM").await?;
        let col = |i: usize| {
            row.get(i)
                .cloned()
                .flatten()
                .ok_or_else(|| protocol("IDENTIFY_SYSTEM: missing column"))
        };
        Ok(SystemInfo {
            system_id: col(0)?
                .parse()
                .map_err(|_| protocol("IDENTIFY_SYSTEM: bad system id"))?,
            timeline: col(1)?
                .parse()
                .map_err(|_| protocol("IDENTIFY_SYSTEM: bad timeline"))?,
            xlogpos: col(2)?.parse().map_err(|e| protocol(format!("{e}")))?,
        })
    }

    pub async fn show(&mut self, setting: &str) -> Result<String, Error> {
        let row = self.single_row(&format!("SHOW {setting}")).await?;
        row.into_iter()
            .next()
            .flatten()
            .ok_or_else(|| protocol(format!("SHOW {setting}: no value")))
    }

    /// Creates the slot with its WAL reserved, unless it already exists.
    pub async fn ensure_physical_slot(&mut self, slot: &str) -> Result<(), Error> {
        let sql = format!("CREATE_REPLICATION_SLOT {slot} PHYSICAL RESERVE_WAL");
        match self.simple_query(&sql).await {
            Err(Error::Db { code, .. }) if code == DUPLICATE_OBJECT => Ok(()),
            other => other.map(drop),
        }
    }

    /// Drops the slot, if it exists.
    pub async fn drop_slot(&mut self, slot: &str) -> Result<(), Error> {
        match self
            .simple_query(&format!("DROP_REPLICATION_SLOT {slot}"))
            .await
        {
            Err(Error::Db { code, .. }) if code == UNDEFINED_OBJECT => Ok(()),
            other => other.map(drop),
        }
    }

    pub async fn slot_restart_lsn(&mut self, slot: &str) -> Result<Option<Lsn>, Error> {
        let row = self
            .single_row(&format!("READ_REPLICATION_SLOT {slot}"))
            .await?;
        row.get(1)
            .cloned()
            .flatten()
            .map(|s| s.parse().map_err(|e| protocol(format!("{e}"))))
            .transpose()
    }

    /// The history file of `timeline`: `(file name, content)`.
    pub async fn timeline_history(&mut self, timeline: u32) -> Result<(String, String), Error> {
        let row = self
            .single_row(&format!("TIMELINE_HISTORY {timeline}"))
            .await?;
        let col = |i: usize| {
            row.get(i)
                .cloned()
                .flatten()
                .ok_or_else(|| protocol("TIMELINE_HISTORY: missing column"))
        };
        Ok((col(0)?, col(1)?))
    }

    pub async fn start_replication(
        &mut self,
        slot: &str,
        start: Lsn,
        timeline: u32,
    ) -> Result<(), Error> {
        self.send_query(&format!(
            "START_REPLICATION SLOT {slot} PHYSICAL {start} TIMELINE {timeline}"
        ))
        .await?;
        // After an error the server sends ReadyForQuery, and the connection
        // takes another command.
        let mut err = None;
        loop {
            let (tag, body) = self.read_frame().await?;
            match tag {
                b'W' => return Ok(()),
                b'E' => err = Some(db_error(body)),
                b'Z' if err.is_some() => return Err(err.expect("checked")),
                b'N' => {}
                other => return Err(unexpected(other, "START_REPLICATION")),
            }
        }
    }

    /// The next message of the replication stream, or `None` once the server
    /// ends it.
    pub async fn next_message(&mut self) -> Result<Option<CopyMessage>, Error> {
        loop {
            let (tag, body) = self.read_frame().await?;
            match tag {
                b'd' => return parse_copy_data(body).map(Some),
                // A walsender that shuts down sends CommandComplete, not CopyDone.
                b'c' | b'C' => return Ok(None),
                b'E' => return Err(db_error(body)),
                b'N' => {}
                other => return Err(unexpected(other, "replication")),
            }
        }
    }

    pub fn has_buffered_frame(&self) -> bool {
        matches!(frame_len(&self.buf), Ok(Some(len)) if self.buf.len() > len)
    }

    /// Reports the written and flushed positions. With `reply_requested`, the
    /// server answers at once with a keepalive.
    pub async fn send_status(
        &mut self,
        written: Lsn,
        flushed: Lsn,
        reply_requested: bool,
    ) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64 - PG_EPOCH_OFFSET_MICROS);
        self.stream
            .write_all(&status_update(written, flushed, now, reply_requested))
            .await?;
        Ok(())
    }

    pub async fn send_copy_done(&mut self) -> Result<(), Error> {
        let mut out = BytesMut::new();
        frontend::copy_done(&mut out);
        self.stream.write_all(&out).await?;
        Ok(())
    }

    async fn single_row(&mut self, sql: &str) -> Result<Vec<Option<String>>, Error> {
        let mut rows = self.simple_query(sql).await?;
        if rows.len() != 1 {
            return Err(protocol(format!(
                "{sql}: expected one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.remove(0))
    }

    async fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>, Error> {
        self.send_query(sql).await?;
        let mut rows = Vec::new();
        let mut err = None;
        loop {
            let (tag, body) = self.read_frame().await?;
            match tag {
                b'D' => rows.push(data_row(body)?),
                b'E' => err = Some(db_error(body)),
                b'Z' => break,
                _ => {}
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }

    async fn send_query(&mut self, sql: &str) -> Result<(), Error> {
        let mut out = BytesMut::new();
        frontend::query(sql, &mut out)?;
        self.stream.write_all(&out).await?;
        Ok(())
    }

    async fn read_frame(&mut self) -> Result<(u8, BytesMut), Error> {
        loop {
            if let Some(len) = frame_len(&self.buf)? {
                if self.buf.len() > len {
                    let mut frame = self.buf.split_to(1 + len);
                    let tag = frame[0];
                    frame.advance(5);
                    return Ok((tag, frame));
                }
                self.buf.reserve(1 + len - self.buf.len());
            }
            let read = tokio::time::timeout(self.idle_timeout, self.stream.read_buf(&mut self.buf));
            match read.await {
                Ok(Ok(0)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed the connection",
                    )
                    .into());
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no message from the server for {:?}", self.idle_timeout),
                    )
                    .into());
                }
            }
        }
    }
}

/// The frame length field (which counts itself, not the tag), once buffered.
fn frame_len(buf: &[u8]) -> Result<Option<usize>, Error> {
    let Some(len) = buf.get(1..5) else {
        return Ok(None);
    };
    let len = u32::from_be_bytes(len.try_into().expect("slice of 4")) as usize;
    if !(4..=MAX_FRAME).contains(&len) {
        return Err(protocol(format!("frame length {len} out of bounds")));
    }
    Ok(Some(len))
}

fn parse_copy_data(mut body: BytesMut) -> Result<CopyMessage, Error> {
    match take_u8(&mut body)? {
        b'w' => {
            let start = Lsn(take_u64(&mut body)?);
            let wal_end = Lsn(take_u64(&mut body)?);
            take_u64(&mut body)?; // send time
            Ok(CopyMessage::XLogData {
                start,
                wal_end,
                data: body.freeze(),
            })
        }
        b'k' => {
            let wal_end = Lsn(take_u64(&mut body)?);
            take_u64(&mut body)?; // send time
            Ok(CopyMessage::Keepalive {
                wal_end,
                reply_requested: take_u8(&mut body)? == 1,
            })
        }
        other => Err(unexpected(other, "CopyData")),
    }
}

/// A standby status update. The apply position is 0: this client never replays.
fn status_update(written: Lsn, flushed: Lsn, now_micros: i64, reply_requested: bool) -> BytesMut {
    let mut out = BytesMut::with_capacity(39);
    out.put_u8(b'd');
    out.put_u32(4 + 1 + 8 * 4 + 1);
    out.put_u8(b'r');
    out.put_u64(written.0);
    out.put_u64(flushed.0);
    out.put_u64(0);
    out.put_i64(now_micros);
    out.put_u8(reply_requested.into());
    out
}

fn data_row(mut body: BytesMut) -> Result<Vec<Option<String>>, Error> {
    let cols = take_i16(&mut body)?;
    (0..cols)
        .map(|_| {
            let len = take_i32(&mut body)?;
            if len < 0 {
                return Ok(None);
            }
            need(&body, len as usize)?;
            let value = body.split_to(len as usize);
            String::from_utf8(value.to_vec())
                .map(Some)
                .map_err(|_| protocol("DataRow: value is not UTF-8"))
        })
        .collect()
}

fn db_error(mut body: BytesMut) -> Error {
    let (mut code, mut message) = (String::new(), String::new());
    while let Ok(field) = take_u8(&mut body)
        && field != 0
    {
        let Some(end) = body.iter().position(|&b| b == 0) else {
            break;
        };
        let value = String::from_utf8_lossy(&body.split_to(end)).into_owned();
        body.advance(1);
        match field {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    Error::Db { code, message }
}

fn need(body: &BytesMut, n: usize) -> Result<(), Error> {
    if body.len() < n {
        return Err(protocol("truncated message"));
    }
    Ok(())
}

fn take_u8(body: &mut BytesMut) -> Result<u8, Error> {
    need(body, 1)?;
    Ok(body.get_u8())
}

fn take_i16(body: &mut BytesMut) -> Result<i16, Error> {
    need(body, 2)?;
    Ok(body.get_i16())
}

fn take_i32(body: &mut BytesMut) -> Result<i32, Error> {
    need(body, 4)?;
    Ok(body.get_i32())
}

fn take_u64(body: &mut BytesMut) -> Result<u64, Error> {
    need(body, 8)?;
    Ok(body.get_u64())
}

fn protocol(msg: impl Into<String>) -> Error {
    Error::Protocol(msg.into())
}

fn unexpected(tag: u8, during: &str) -> Error {
    protocol(format!(
        "unexpected message {:?} during {during}",
        tag as char
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_xlogdata_and_keepalive() {
        let mut w = BytesMut::new();
        w.put_u8(b'w');
        w.put_u64(0x1_0000_0028);
        w.put_u64(0x1_0000_0100);
        w.put_u64(0);
        w.put_slice(b"wal");
        assert_eq!(
            parse_copy_data(w).unwrap(),
            CopyMessage::XLogData {
                start: Lsn(0x1_0000_0028),
                wal_end: Lsn(0x1_0000_0100),
                data: Bytes::from_static(b"wal"),
            }
        );

        let mut k = BytesMut::new();
        k.put_u8(b'k');
        k.put_u64(0x40);
        k.put_u64(0);
        k.put_u8(1);
        assert_eq!(
            parse_copy_data(k).unwrap(),
            CopyMessage::Keepalive {
                wal_end: Lsn(0x40),
                reply_requested: true
            }
        );
    }

    #[test]
    fn rejects_truncated_and_oversized_input() {
        let mut short = BytesMut::new();
        short.put_u8(b'w');
        short.put_u32(7);
        assert!(parse_copy_data(short).is_err());

        let mut huge = BytesMut::new();
        huge.put_u8(b'd');
        huge.put_u32(u32::MAX);
        assert!(frame_len(&huge).is_err());
    }

    #[test]
    fn status_update_matches_its_length_field() {
        let out = status_update(Lsn(2), Lsn(1), 0, true);
        let len = u32::from_be_bytes(out[1..5].try_into().unwrap()) as usize;
        assert_eq!(out.len(), 1 + len);
        assert_eq!(out[5], b'r');
        assert_eq!(out[out.len() - 1], 1, "reply requested");
    }
}

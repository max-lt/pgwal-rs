#![doc = include_str!("../README.md")]

use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::{Either, select};
use js_sys::Promise;
use pgwal::{ArchiveKey, Lsn, RangeKey, timeline_start};
use pgwal_receiver::Error as WireError;
use pgwal_receiver::wire::{Conn, ConnConfig, CopyMessage};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, future_to_promise};
use worker::{
    Bucket, ConnectionBuilder, Date, Delay, Env, Method, Request, Response, SecureTransport,
    Socket, SqlStorage, SqlStorageValue, State, event,
};

/// Largest row of WAL in SQLite. A row never crosses a segment.
const ROW_BYTES: usize = 1 << 20;
const TICK_MS: u64 = 250;
/// Longest wait for one step of a tick: sync, status update, upload.
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest silence of the server. A walsender sends a keepalive every
/// `wal_sender_timeout / 2`, 30 s by default.
const IDLE_MS: u64 = 90_000;
/// Wait for the server's CopyDone after ours.
const CLOSE_WAIT_MS: u64 = 10_000;
/// Longest gap between two status updates while no WAL arrives.
const STATUS_EVERY_MS: u64 = 10_000;
/// A step slower than this goes to the log as a warning.
const SLOW_STEP_MS: u64 = 2_000;
/// Delay before the next session after a failed one.
const RETRY_DELAY: Duration = Duration::from_secs(5);

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: worker::Context) -> worker::Result<Response> {
    let token = setting(&env, "ADMIN_TOKEN")?;
    if req.headers().get("authorization")?.as_deref() != Some(&format!("Bearer {token}")) {
        return Response::error("forbidden", 403);
    }
    env.durable_object("RECEIVER")?
        .get_by_name("receiver")?
        .fetch_with_request(req)
        .await
}

/// A var or a secret.
fn setting(env: &Env, name: &str) -> worker::Result<String> {
    match env.secret(name) {
        Ok(secret) => Ok(secret.to_string()),
        Err(_) => Ok(env.var(name)?.to_string()),
    }
}

fn now_ms() -> u64 {
    Date::now().as_millis()
}

fn system_time() -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(now_ms())
}

#[wasm_bindgen]
extern "C" {
    /// The JS storage of the object: worker 0.8 has no binding for `sync()`.
    #[wasm_bindgen(extends = js_sys::Object)]
    type RawStorage;

    #[wasm_bindgen(method, catch)]
    fn sync(this: &RawStorage) -> Result<Promise, JsValue>;
}

struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    slot: String,
    prefix: String,
    session_ms: u64,
    upload_ms: u64,
}

impl Config {
    fn from_env(env: &Env) -> worker::Result<Config> {
        let num = |name: &str| -> worker::Result<u64> {
            setting(env, name)?
                .parse()
                .map_err(|_| worker::Error::RustError(format!("{name} is not a number")))
        };
        Ok(Config {
            host: setting(env, "PGHOST")?,
            port: num("PGPORT")? as u16,
            user: setting(env, "PGUSER")?,
            password: setting(env, "PGPASSWORD")?,
            slot: setting(env, "SLOT")?,
            prefix: setting(env, "PREFIX").unwrap_or_default(),
            session_ms: num("SESSION_SECONDS")? * 1000,
            upload_ms: num("UPLOAD_SECONDS")? * 1000,
        })
    }
}

/// Why a session ended early.
enum Stop {
    /// The next alarm retries.
    Retry(String),
    /// A retry cannot fix it: the receiver stops.
    Fatal(String),
}

impl Stop {
    fn retry(e: impl ToString) -> Stop {
        Stop::Retry(e.to_string())
    }

    fn fatal(e: impl ToString) -> Stop {
        Stop::Fatal(e.to_string())
    }

    fn message(&self) -> &str {
        match self {
            Stop::Retry(m) | Stop::Fatal(m) => m,
        }
    }
}

impl From<WireError> for Stop {
    fn from(e: WireError) -> Stop {
        Stop::retry(e)
    }
}

impl From<worker::Error> for Stop {
    fn from(e: worker::Error) -> Stop {
        Stop::retry(e)
    }
}

/// What `/status` shows of the running session.
#[derive(Default)]
struct Live {
    phase: &'static str,
    since: u64,
    written: u64,
    synced: u64,
}

#[wasm_bindgen]
pub struct Receiver {
    state: State,
    env: Env,
    storage: RawStorage,
    live: RefCell<Live>,
}

#[wasm_bindgen]
impl Receiver {
    #[wasm_bindgen(constructor)]
    pub fn new(state: worker::worker_sys::DurableObjectState, env: Env) -> Receiver {
        let storage = state
            .storage()
            .expect("a Durable Object has storage")
            .unchecked_into();
        let receiver = Receiver {
            state: State::from(state),
            env,
            storage,
            live: RefCell::new(Live {
                phase: "idle",
                since: now_ms(),
                ..Live::default()
            }),
        };
        for ddl in [
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS wal(timeline INTEGER NOT NULL, start INTEGER NOT NULL, \
             end INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (timeline, start))",
        ] {
            receiver.sql().exec(ddl, None).expect("schema");
        }
        receiver
    }

    #[wasm_bindgen(js_name = fetch)]
    pub fn js_fetch(&self, req: worker::web_sys::Request) -> Promise {
        // SAFETY: as in worker's own durable_object macro, the runtime keeps
        // the object alive while one of its promises runs.
        let this: &'static Receiver = unsafe { &*(self as *const Receiver) };
        future_to_promise(AssertUnwindSafe(async move {
            this.handle(req.into())
                .await
                .map(|r| JsValue::from(worker::web_sys::Response::from(r)))
                .map_err(JsValue::from)
        }))
    }

    #[wasm_bindgen(js_name = alarm)]
    pub fn js_alarm(&self) -> Promise {
        // SAFETY: see js_fetch.
        let this: &'static Receiver = unsafe { &*(self as *const Receiver) };
        future_to_promise(AssertUnwindSafe(async move {
            this.alarm()
                .await
                .map(|()| JsValue::UNDEFINED)
                .map_err(JsValue::from)
        }))
    }
}

#[derive(Deserialize)]
struct Kv {
    k: String,
    v: String,
}

impl Receiver {
    fn sql(&self) -> SqlStorage {
        self.state.storage().sql()
    }

    fn bucket(&self) -> worker::Result<Bucket> {
        self.env.bucket("WAL")
    }

    fn get(&self, k: &str) -> Option<String> {
        #[derive(Deserialize)]
        struct V {
            v: String,
        }
        let cursor = self
            .sql()
            .exec("SELECT v FROM meta WHERE k = ?", vec![k.into()])
            .ok()?;
        cursor.to_array::<V>().ok()?.pop().map(|r| r.v)
    }

    fn set(&self, k: &str, v: impl ToString) -> worker::Result<()> {
        self.sql().exec(
            "INSERT INTO meta(k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            vec![k.into(), v.to_string().into()],
        )?;
        Ok(())
    }

    fn bump(&self, k: &str) -> worker::Result<()> {
        let n: u64 = self.get(k).and_then(|v| v.parse().ok()).unwrap_or(0);
        self.set(k, n + 1)
    }

    fn running(&self) -> bool {
        self.get("running").as_deref() == Some("1")
    }

    fn log(&self, level: &str, msg: &str, fields: Value) {
        let debug = setting(&self.env, "LOG_LEVEL").is_ok_and(|l| l == "debug");
        if level == "debug" && !debug {
            return;
        }
        let mut line = Map::new();
        line.insert("level".into(), level.into());
        line.insert("msg".into(), msg.into());
        if let Value::Object(fields) = fields {
            line.extend(fields);
        }
        let line = Value::Object(line).to_string();
        match level {
            "warn" => worker::console_warn!("{line}"),
            _ => worker::console_log!("{line}"),
        }
    }

    async fn handle(&self, req: Request) -> worker::Result<Response> {
        let url = req.url()?;
        let prefix = setting(&self.env, "PREFIX").unwrap_or_default();
        match (req.method(), url.path()) {
            (Method::Post, "/start") => {
                self.set("running", 1)?;
                self.state.storage().set_alarm(Duration::ZERO).await?;
                self.log("info", "start", json!({}));
                self.status()
            }
            (Method::Post, "/stop") => {
                self.set("running", 0)?;
                self.state.storage().delete_alarm().await?;
                self.log("info", "stop", json!({}));
                self.status()
            }
            (Method::Get, "/status") => self.status(),
            (Method::Get, "/list") => {
                let bucket = self.bucket()?;
                let mut list = bucket.list().prefix(prefix);
                if let Some((_, cursor)) = url.query_pairs().find(|(k, _)| k == "cursor") {
                    list = list.cursor(cursor.to_string());
                }
                let objects = list.execute().await?;
                let keys: Vec<String> = objects.objects().iter().map(|o| o.key()).collect();
                let cursor = objects.truncated().then(|| objects.cursor()).flatten();
                Response::from_json(&json!({ "keys": keys, "cursor": cursor }))
            }
            (Method::Get, "/object") => {
                let key = url
                    .query_pairs()
                    .find(|(k, _)| k == "key")
                    .map(|(_, v)| v.to_string())
                    .unwrap_or_default();
                match self.bucket()?.get(key).execute().await? {
                    Some(object) => match object.body() {
                        Some(body) => Response::from_bytes(body.bytes().await?),
                        None => Response::from_bytes(Vec::new()),
                    },
                    None => Response::error("not found", 404),
                }
            }
            (Method::Post, "/reset") => {
                if self.running() {
                    return Response::error("stop first", 409);
                }
                // An empty prefix would delete the whole bucket.
                if prefix.is_empty() {
                    return Response::error("reset needs a PREFIX", 409);
                }
                let bucket = self.bucket()?;
                let mut deleted = 0;
                loop {
                    let keys: Vec<String> = bucket
                        .list()
                        .prefix(prefix.clone())
                        .execute()
                        .await?
                        .objects()
                        .iter()
                        .map(|o| o.key())
                        .collect();
                    if keys.is_empty() {
                        break;
                    }
                    deleted += keys.len();
                    bucket.delete_multiple(keys).await?;
                }
                self.sql().exec("DELETE FROM meta", None)?;
                self.sql().exec("DELETE FROM wal", None)?;
                Response::from_json(&json!({ "deleted": deleted }))
            }
            _ => Response::error("not found", 404),
        }
    }

    fn status(&self) -> worker::Result<Response> {
        let mut out = Map::new();
        for Kv { k, v } in self
            .sql()
            .exec("SELECT k, v FROM meta", None)?
            .to_array::<Kv>()?
        {
            if (k.starts_with("flushed/") || k == "uploaded")
                && let Ok(n) = v.parse::<u64>()
            {
                out.insert(format!("{k}_lsn"), Lsn(n).to_string().into());
            }
            out.insert(k, v.into());
        }
        #[derive(Deserialize)]
        struct Buffered {
            rows: f64,
            bytes: f64,
        }
        let buffered = self
            .sql()
            .exec(
                "SELECT count(*) AS rows, coalesce(sum(length(data)), 0) AS bytes FROM wal",
                None,
            )?
            .one::<Buffered>()?;
        out.insert(
            "buffered".into(),
            json!({ "rows": buffered.rows, "bytes": buffered.bytes }),
        );
        let live = self.live.borrow();
        out.insert(
            "phase".into(),
            json!({ "name": live.phase, "age_ms": now_ms().saturating_sub(live.since) }),
        );
        out.insert(
            "unsynced_bytes".into(),
            live.written.saturating_sub(live.synced).into(),
        );
        Response::from_json(&Value::Object(out))
    }

    async fn alarm(&self) -> worker::Result<()> {
        if !self.running() {
            return Ok(());
        }
        let mut delay = Duration::ZERO;
        if let Err(stop) = self.session().await {
            let fatal = matches!(stop, Stop::Fatal(_));
            self.bump("errors")?;
            self.set("last_error", format!("{} {}", Date::now(), stop.message()))?;
            self.log(
                "warn",
                "session failed",
                json!({ "error": stop.message(), "fatal": fatal }),
            );
            if fatal {
                self.set("running", 0)?;
            }
            delay = RETRY_DELAY;
        }
        self.bump("sessions")?;
        self.set_phase("idle");
        if self.running() {
            self.state.storage().set_alarm(delay).await?;
        }
        Ok(())
    }

    fn set_phase(&self, phase: &'static str) {
        let mut live = self.live.borrow_mut();
        live.phase = phase;
        live.since = now_ms();
    }

    /// Runs one step of a tick under [`STEP_TIMEOUT`], and shows it in
    /// `/status` while it runs.
    async fn step<T>(
        &self,
        name: &'static str,
        f: impl Future<Output = Result<T, Stop>>,
    ) -> Result<T, Stop> {
        self.set_phase(name);
        let t = now_ms();
        let out = match select(pin!(f), pin!(Delay::from(STEP_TIMEOUT))).await {
            Either::Left((out, _)) => out,
            Either::Right(_) => Err(Stop::retry(format!(
                "{name} took more than {} s",
                STEP_TIMEOUT.as_secs()
            ))),
        };
        let ms = now_ms() - t;
        if ms > SLOW_STEP_MS {
            self.log("warn", "slow step", json!({ "step": name, "ms": ms }));
        } else {
            self.log("debug", "step", json!({ "step": name, "ms": ms }));
        }
        self.set_phase("stream");
        out
    }

    async fn sync(&self) -> Result<(), Stop> {
        let promise = self
            .storage
            .sync()
            .map_err(|e| Stop::retry(format!("sync: {e:?}")))?;
        JsFuture::from(promise)
            .await
            .map(drop)
            .map_err(|e| Stop::retry(format!("sync: {e:?}")))
    }

    /// One replication connection, ended after `SESSION_SECONDS`.
    async fn session(&self) -> Result<(), Stop> {
        let cfg = Config::from_env(&self.env)?;
        let t0 = now_ms();
        self.set_phase("connect");
        let socket = ConnectionBuilder::new()
            .secure_transport(SecureTransport::On)
            .allow_half_open(false)
            .connect(cfg.host.clone(), cfg.port)?;
        let conn_cfg = ConnConfig {
            user: cfg.user.clone(),
            password: Some(cfg.password.clone()),
            application_name: "pgwal-worker".into(),
            idle_timeout: Duration::from_millis(IDLE_MS),
        };
        let mut conn = Conn::connect(socket, &conn_cfg).await?;
        let result = self.stream(&cfg, &mut conn, t0).await;
        let mut socket: Socket = conn.into_inner();
        let _ = socket.close().await;
        self.set("last_session_end", Date::now().to_string())?;
        result
    }

    async fn stream(&self, cfg: &Config, conn: &mut Conn<Socket>, t0: u64) -> Result<(), Stop> {
        let system = conn.identify_system().await?;
        let seg_size = conn.wal_segment_size().await?;
        let system_id = system.system_id.to_string();
        if let Some(known) = self.get("system_id")
            && known != system_id
        {
            return Err(Stop::fatal(format!(
                "the server is system {system_id}, the archive holds {known}"
            )));
        }
        self.set("system_id", &system_id)?;
        self.set("seg_size", seg_size)?;
        conn.ensure_physical_slot(&cfg.slot).await?;

        // A timeline above 1 starts at its switch point. Its history goes to
        // the archive, so a restore can follow it.
        let timeline = system.timeline;
        let history = match timeline {
            1 => None,
            t => Some(conn.timeline_history(t).await?),
        };
        let switch_point = history
            .as_deref()
            .and_then(timeline_start)
            .unwrap_or_default();
        let flushed_key = format!("flushed/{timeline}");
        let resumed = self.get(&flushed_key).and_then(|v| v.parse().ok()).map(Lsn);
        let mut start = match resumed {
            Some(lsn) => lsn,
            None if history.is_some() => switch_point.segment_start(seg_size),
            None => conn
                .slot_restart_lsn(&cfg.slot)
                .await?
                .unwrap_or(system.xlogpos)
                .segment_start(seg_size),
        };
        if let Err(e) = conn.start_replication(&cfg.slot, start, timeline).await {
            let gone = matches!(e, WireError::WalGone { .. });
            if !gone {
                return Err(e.into());
            }
            if resumed.is_some() || history.is_none() {
                return Err(Stop::fatal(format!("gap in the archive: {e}")));
            }
            // The first WAL of a later timeline can be gone already.
            let fallback = conn
                .slot_restart_lsn(&cfg.slot)
                .await?
                .unwrap_or(system.xlogpos)
                .max(switch_point)
                .segment_start(seg_size);
            self.log(
                "warn",
                "the first WAL of the timeline is gone",
                json!({ "start": start.to_string(), "fallback": fallback.to_string() }),
            );
            start = fallback;
            conn.start_replication(&cfg.slot, start, timeline)
                .await
                .map_err(|e| Stop::fatal(format!("gap in the archive: {e}")))?;
        }
        if let Some(content) = &history {
            let history = ArchiveKey::History {
                system_id: system.system_id,
                timeline,
            };
            let key = format!("{}{history}", cfg.prefix);
            self.bucket()?
                .put(key, content.clone().into_bytes())
                .execute()
                .await?;
        }
        self.set("timeline", timeline)?;
        self.set("connect_ms", now_ms() - t0)?;
        self.set("connected_at", Date::now().to_string())?;
        self.log(
            "info",
            "streaming",
            json!({
                "system_id": system_id, "timeline": timeline,
                "start_lsn": start.to_string(), "connect_ms": now_ms() - t0,
            }),
        );

        let mut pending: Vec<u8> = Vec::new();
        let mut pending_start = start.0;
        let mut written = start.0;
        let mut durable = start.0;
        let mut synced = start.0;
        {
            let mut live = self.live.borrow_mut();
            live.written = written;
            live.synced = synced;
        }
        let deadline = t0 + cfg.session_ms;
        let mut next_tick = now_ms() + TICK_MS;
        let mut last_upload = now_ms();
        let mut last_status = 0;
        let mut last_message = now_ms();
        let mut copy_done = false;
        let mut close_at = u64::MAX;
        self.set_phase("stream");

        let streamed: Result<(), Stop> = async {
            loop {
                let now = now_ms();
                if now >= next_tick {
                    next_tick = now + TICK_MS;
                    if let Some(end) =
                        self.persist(timeline, seg_size, &mut pending, &mut pending_start)?
                    {
                        durable = end;
                    }
                    if !copy_done && (durable > synced || now - last_status > STATUS_EVERY_MS) {
                        // The flush is reported only once SQLite holds the WAL.
                        self.step("sync", self.sync()).await?;
                        synced = durable;
                        self.live.borrow_mut().synced = synced;
                        self.step("status", async {
                            conn.send_status(Lsn(synced), Lsn(synced), system_time())
                                .await
                                .map_err(Stop::from)
                        })
                        .await?;
                        last_status = now;
                    }
                    if now - last_upload >= cfg.upload_ms {
                        self.step("upload", self.upload(&cfg.prefix, seg_size)).await?;
                        last_upload = now_ms();
                    }
                    if !copy_done && (now >= deadline || !self.running()) {
                        let why = if now >= deadline { "deadline" } else { "stopped" };
                        self.log(
                            "info",
                            "ending session",
                            json!({ "why": why, "flushed_lsn": Lsn(synced).to_string(), "age_ms": now - t0 }),
                        );
                        conn.send_copy_done().await?;
                        copy_done = true;
                        close_at = now + CLOSE_WAIT_MS;
                    }
                    if now >= close_at {
                        self.log("warn", "no CopyDone from the server", json!({}));
                        return Ok(());
                    }
                    if now - last_message > IDLE_MS {
                        return Err(Stop::retry(format!("no message for {} s", IDLE_MS / 1000)));
                    }
                    continue;
                }
                let wait = Duration::from_millis(next_tick - now);
                let message = {
                    let next = pin!(conn.next_message());
                    match select(next, pin!(Delay::from(wait))).await {
                        Either::Left((message, _)) => message?,
                        Either::Right(_) => continue,
                    }
                };
                last_message = now_ms();
                match message {
                    Some(CopyMessage::XLogData { start, data, .. }) => {
                        if start.0 > written {
                            return Err(Stop::retry(format!(
                                "gap in the stream: got {start}, expected {}",
                                Lsn(written)
                            )));
                        }
                        let skip = (written - start.0) as usize;
                        if skip < data.len() {
                            pending.extend_from_slice(&data[skip..]);
                            written = start.0 + data.len() as u64;
                            self.live.borrow_mut().written = written;
                        }
                    }
                    Some(CopyMessage::Keepalive { reply_requested, .. }) => {
                        if reply_requested && !copy_done {
                            conn.send_status(Lsn(synced), Lsn(synced), system_time())
                                .await?;
                        }
                    }
                    // The server ended the stream, after our CopyDone or at
                    // its shutdown.
                    None => return Ok(()),
                }
            }
        }
        .await;

        // WAL received but not reported is still valid WAL: keep and upload it.
        self.persist(timeline, seg_size, &mut pending, &mut pending_start)?;
        if let Err(e) = self
            .step("final upload", self.upload(&cfg.prefix, seg_size))
            .await
        {
            self.log(
                "warn",
                "final upload failed",
                json!({ "error": e.message() }),
            );
        }
        self.log(
            "info",
            "session closed",
            json!({ "flushed_lsn": Lsn(pending_start).to_string(), "age_ms": now_ms() - t0 }),
        );
        streamed
    }

    /// Moves the received WAL into SQLite. The rows and the new flush
    /// position commit together, as no await separates them.
    fn persist(
        &self,
        timeline: u32,
        seg_size: u64,
        pending: &mut Vec<u8>,
        pending_start: &mut u64,
    ) -> Result<Option<u64>, Stop> {
        if pending.is_empty() {
            return Ok(None);
        }
        let mut pos = *pending_start;
        let mut off = 0;
        while off < pending.len() {
            let seg_end = (pos / seg_size + 1) * seg_size;
            let n = ROW_BYTES
                .min((seg_end - pos) as usize)
                .min(pending.len() - off);
            self.sql().exec(
                "INSERT INTO wal(timeline, start, end, data) VALUES (?, ?, ?, ?)",
                vec![
                    i64::from(timeline).into(),
                    (pos as i64).into(),
                    ((pos + n as u64) as i64).into(),
                    SqlStorageValue::Blob(pending[off..off + n].to_vec()),
                ],
            )?;
            pos += n as u64;
            off += n;
        }
        self.log(
            "debug",
            "persisted",
            json!({ "bytes": pending.len(), "flushed_lsn": Lsn(pos).to_string() }),
        );
        pending.clear();
        *pending_start = pos;
        self.set(&format!("flushed/{timeline}"), pos)?;
        Ok(Some(pos))
    }

    /// Uploads the stored WAL, one object per contiguous run of rows of one
    /// timeline inside one segment, and deletes the rows it uploaded.
    async fn upload(&self, prefix: &str, seg_size: u64) -> Result<(), Stop> {
        let Some(system_id) = self.get("system_id").and_then(|v| v.parse::<u64>().ok()) else {
            return Ok(());
        };
        let bucket = self.bucket()?;
        loop {
            let rows: Vec<(u32, u64, u64, Vec<u8>)> = self
                .sql()
                .exec(
                    "SELECT timeline, start, end, data FROM wal ORDER BY timeline, start LIMIT 32",
                    None,
                )?
                .raw()
                .map(|row| row.map(parse_row))
                .collect::<worker::Result<Option<Vec<_>>>>()?
                .ok_or_else(|| Stop::retry("bad row in the wal table"))?;
            let Some(first) = rows.first() else {
                return Ok(());
            };
            let (timeline, start, segno) = (first.0, first.1, first.1 / seg_size);
            let mut end = start;
            let mut body = Vec::new();
            for (t, s, e, data) in &rows {
                if *t != timeline || *s != end || s / seg_size != segno {
                    break;
                }
                body.extend_from_slice(data);
                end = *e;
            }
            let key = RangeKey {
                system_id,
                timeline,
                start: Lsn(start),
                end: Lsn(end),
            };
            let key = format!("{prefix}{key}");
            let t = now_ms();
            bucket.put(&key, body).execute().await?;
            self.log(
                "debug",
                "put",
                json!({ "key": key, "bytes": end - start, "ms": now_ms() - t }),
            );
            self.sql().exec(
                "DELETE FROM wal WHERE timeline = ? AND start >= ? AND start < ?",
                vec![
                    i64::from(timeline).into(),
                    (start as i64).into(),
                    (end as i64).into(),
                ],
            )?;
            self.set("uploaded", end)?;
            self.bump("objects")?;
        }
    }
}

fn parse_row(row: Vec<SqlStorageValue>) -> Option<(u32, u64, u64, Vec<u8>)> {
    let int = |v: &SqlStorageValue| match v {
        SqlStorageValue::Integer(i) => Some(*i as u64),
        SqlStorageValue::Float(f) => Some(*f as u64),
        _ => None,
    };
    let mut row = row.into_iter();
    let (t, s, e) = (row.next()?, row.next()?, row.next()?);
    let SqlStorageValue::Blob(data) = row.next()? else {
        return None;
    };
    Some((int(&t)? as u32, int(&s)?, int(&e)?, data))
}

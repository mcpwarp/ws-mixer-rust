//! Control channel (stream 0) messages, WIRE.md §2.7.
//!
//! Every message serializes with `t` as its first key. Incoming messages go
//! through the hand-written validator [`parse_control`]; there are no
//! `Deserialize` impls, so the validation order and error messages follow the
//! spec exactly.

use std::ops::RangeInclusive;

use serde::Serialize;
use serde_json::{Map, Number, Value};

use crate::error::{ErrorCode, truncate_utf8};
use crate::event::DrainReason;
use crate::frame::MAX_CONTROL_PAYLOAD;

/// `welcome`, the server's reply to `hello`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "t", rename = "welcome")]
#[non_exhaustive]
pub struct Welcome {
    /// Protocol version accepted by the server. Always `1` for ws-mixer.v1.
    pub v: u32,
    /// Connection id assigned by the server (ULID recommended), at most 64 characters.
    pub session: String,
    /// The server's own receive window in bytes.
    pub window: u32,
    /// The effective stream cap: `min(client ask, server policy)`.
    pub max_streams: u32,
    /// Keepalive ping interval in milliseconds (at least 5000, at most
    /// `i64::MAX`). Adding it to an `Instant` still needs checked arithmetic.
    pub ping_interval: u64,
    /// Keepalive deadline in milliseconds (at least 2 × `ping_interval`, at
    /// most `i64::MAX`). Adding it to an `Instant` still needs checked arithmetic.
    pub ping_timeout: u64,
    /// The server's identity, for logs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerInfo>,
    /// Capabilities the server advertised. Empty when absent.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    /// Opaque application answer to `hello.meta`. `None` when absent or `null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Map<String, Value>>,
}

/// `welcome.server`: the server's identity, the mirror of [`AgentInfo`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ServerInfo {
    /// Server implementation name, e.g. `"mcpwarp-edge"`.
    pub name: String,
    /// Server implementation version.
    pub version: String,
    /// The node that accepted this connection.
    pub node: String,
}

/// `hello.agent`: identifies the client SDK in the server's logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AgentInfo {
    /// SDK name. Defaults to `"ws-mixer-rust"`.
    pub sdk: String,
    /// SDK version. Defaults to [`SDK_VERSION`](crate::SDK_VERSION).
    pub sdk_version: String,
    /// Language runtime. Defaults to `"rust"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    /// Operating system and architecture. Defaults to e.g. `"macos/aarch64"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
}

impl AgentInfo {
    pub(crate) fn new(sdk: &str, sdk_version: &str) -> Self {
        Self {
            sdk: sdk.to_owned(),
            sdk_version: sdk_version.to_owned(),
            runtime: Some("rust".to_owned()),
            os: Some(format!(
                "{}/{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            )),
        }
    }
}

impl Default for AgentInfo {
    fn default() -> Self {
        Self::new(crate::SDK_NAME, crate::SDK_VERSION)
    }
}

/// `hello`, the client's first stream-0 frame.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "t", rename = "hello")]
pub(crate) struct Hello {
    pub v: u32,
    pub token: String,
    pub agent: AgentInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_streams: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Map<String, Value>>,
}

/// `ping`, sent by both sides on independent schedules.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "t", rename = "ping")]
pub(crate) struct Ping {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<i64>,
}

/// `pong`, echoing a `ping`'s `id` and `ts` verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "t", rename = "pong")]
pub(crate) struct Pong {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<i64>,
}

/// `drain`. The client only ever sends `reason: "client_requested"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "t", rename = "drain")]
pub(crate) struct Drain {
    pub reason: String,
    pub last_stream_id: u32,
    /// Negative values are clamped to 0; at most `i64::MAX`. The phase 2
    /// reader must still use checked arithmetic when adding it to an `Instant`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    /// As `deadline_ms`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `error`, always the last message before a WebSocket close.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "t", rename = "error")]
pub(crate) struct ErrorMsg {
    pub code: u32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_stream_id: Option<u32>,
}

/// `app`, an opaque application message, legal only after `welcome`.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "t", rename = "app")]
pub(crate) struct App {
    pub body: Map<String, Value>,
}

/// Longest `error.message` (WIRE.md §2.7).
pub(crate) const MAX_ERROR_MESSAGE: usize = 1024;

const WINDOW_RANGE: RangeInclusive<i128> = 16_384..=0x7fff_ffff;
const MAX_STREAMS_RANGE: RangeInclusive<i128> = 1..=100_000;
const STREAM_ID_RANGE: RangeInclusive<i128> = 0..=0x7fff_ffff;
const PING_ID_RANGE: RangeInclusive<i128> = 0..=(1 << 53) - 1;
const I64_RANGE: RangeInclusive<i128> = i64::MIN as i128..=i64::MAX as i128;
const DURATION_MS_RANGE: RangeInclusive<i128> = 0..=i64::MAX as i128;
const ANY_INT: RangeInclusive<i128> = i128::MIN..=i128::MAX;
const ANY_LEN: RangeInclusive<usize> = 0..=usize::MAX;
const PING_INTERVAL_FLOOR: u64 = 5_000;
const AGENT_FIELD_MAX: usize = 128;

impl Hello {
    /// A v1 `hello`. `window` and `max_streams` are always sent explicitly.
    pub(crate) fn new(
        token: String,
        agent: AgentInfo,
        window: u32,
        max_streams: u32,
        capabilities: Vec<String>,
        meta: Option<Map<String, Value>>,
    ) -> Self {
        Self {
            v: 1,
            token,
            agent,
            window: Some(window),
            max_streams: Some(max_streams),
            capabilities,
            meta,
        }
    }
}

impl Pong {
    /// The reply to `ping`: same `id` and `ts`.
    pub(crate) fn echo(ping: &Ping) -> Self {
        Self {
            id: ping.id,
            ts: ping.ts,
        }
    }
}

impl Drain {
    /// `reason` as a [`DrainReason`]; unknown values map to `Maintenance`.
    pub(crate) fn drain_reason(&self) -> DrainReason {
        DrainReason::from_wire(&self.reason)
    }

    /// Whether `reason` is outside the spec's enum. The reader counts these
    /// (WIRE.md §2.7: tolerate, degrade to `maintenance`, count).
    pub(crate) fn unknown_reason(&self) -> bool {
        self.drain_reason().as_str() != self.reason
    }
}

impl ErrorMsg {
    /// A connection-level `error` with `message` truncated to 1024 bytes on
    /// a character boundary. An empty `message` becomes `"{name} ({code})"`.
    pub(crate) fn new(code: ErrorCode, message: &str) -> Self {
        let message = if message.is_empty() {
            format!("{} ({})", code.name(), code.0)
        } else {
            truncate_utf8(message, MAX_ERROR_MESSAGE).to_owned()
        };
        Self {
            code: code.0,
            message,
            stream_id: None,
            last_stream_id: None,
        }
    }
}

/// One validated stream-0 message.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ControlMsg {
    Hello(Hello),
    Welcome(Welcome),
    Ping(Ping),
    Pong(Pong),
    Drain(Drain),
    Error(ErrorMsg),
    App(App),
}

/// A stream-0 message failed validation. Always connection-fatal.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub(crate) struct ControlError {
    /// `PROTOCOL_ERROR`, `UNSUPPORTED` for a version mismatch, or
    /// `ENHANCE_YOUR_CALM` for an oversized payload.
    pub code: ErrorCode,
    /// Names the offending field and always carries a number from the wire
    /// (WIRE.md §2.7): the message size is appended when the detail has none.
    pub message: String,
}

type Check<T> = Result<T, (ErrorCode, String)>;

fn protocol<T>(message: String) -> Check<T> {
    Err((ErrorCode::PROTOCOL_ERROR, message))
}

/// Validates one stream-0 DATA payload (WIRE.md §2.7).
///
/// Order: size, JSON parse, is an object, has `t`, `t` is a string, `t` is
/// one of the seven v1 types, then per-field checks in field-table order.
/// Unknown fields are ignored. An integer field takes any JSON number whose
/// value is integral (`1.0` and `1e0` are `1`, WIRE.md §2.7 Integer fields),
/// then its range check; a number with a fraction or a boolean is rejected.
/// A number serde_json reads as a float (a fraction, an exponent, or beyond
/// `u64`) is judged on its `f64` value, so beyond 2^53 it is already rounded.
///
/// `allow_subfloor_timing` (conformance only) skips the `welcome`
/// `ping_interval >= 5000` and `ping_timeout >= 2 * ping_interval` checks;
/// both must still be positive integers.
pub(crate) fn parse_control(
    bytes: &[u8],
    allow_subfloor_timing: bool,
) -> Result<ControlMsg, ControlError> {
    let len = bytes.len();
    parse_inner(bytes, allow_subfloor_timing).map_err(|(code, detail)| {
        let message = if detail.bytes().any(|b| b.is_ascii_digit()) {
            detail
        } else {
            format!("{detail} ({len}-byte control message)")
        };
        ControlError { code, message }
    })
}

fn parse_inner(bytes: &[u8], allow_subfloor_timing: bool) -> Check<ControlMsg> {
    if bytes.len() > MAX_CONTROL_PAYLOAD {
        return Err((
            ErrorCode::ENHANCE_YOUR_CALM,
            format!(
                "control message of {} bytes exceeds the {MAX_CONTROL_PAYLOAD}-byte limit",
                bytes.len()
            ),
        ));
    }
    let value: Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(e) => return protocol(format!("malformed JSON: {e}")),
    };
    let Value::Object(mut map) = value else {
        return protocol(format!(
            "control message must be a JSON object, got {}",
            describe(&value)
        ));
    };
    let t = match map.remove("t") {
        None => return protocol("control message missing required field \"t\"".into()),
        Some(Value::String(t)) => t,
        Some(other) => {
            return protocol(format!(
                "control message field \"t\" must be a string, got {}",
                describe(&other)
            ));
        }
    };
    let t: &'static str = match t.as_str() {
        "hello" => "hello",
        "welcome" => "welcome",
        "ping" => "ping",
        "pong" => "pong",
        "drain" => "drain",
        "error" => "error",
        "app" => "app",
        _ => {
            return protocol(format!(
                "unknown control message type {:?}",
                truncate_utf8(&t, 64)
            ));
        }
    };
    let mut f = Fields { t, map };
    Ok(match t {
        "hello" => ControlMsg::Hello(parse_hello(&mut f)?),
        "welcome" => ControlMsg::Welcome(parse_welcome(&mut f, allow_subfloor_timing)?),
        "ping" => {
            let (id, ts) = parse_ping_pong(&mut f)?;
            ControlMsg::Ping(Ping { id, ts })
        }
        "pong" => {
            let (id, ts) = parse_ping_pong(&mut f)?;
            ControlMsg::Pong(Pong { id, ts })
        }
        "drain" => ControlMsg::Drain(parse_drain(&mut f)?),
        "error" => ControlMsg::Error(parse_error(&mut f)?),
        _ => ControlMsg::App(App {
            body: f.req_object("body")?,
        }),
    })
}

fn parse_version(f: &mut Fields) -> Check<u32> {
    let v = f.required("v")?;
    // An integral beyond i128 is just another unsupported version.
    let n = match as_integer(&v) {
        Some(Integral::Huge) => None,
        _ => Some(f.int("v", &v, ANY_INT)?),
    };
    if n != Some(1) {
        let shown = n.map_or_else(|| v.to_string(), |n| n.to_string());
        return Err((
            ErrorCode::UNSUPPORTED,
            format!("{}.v {shown} is not supported: ws-mixer.v1 requires 1", f.t),
        ));
    }
    Ok(1)
}

fn parse_hello(f: &mut Fields) -> Check<Hello> {
    let v = parse_version(f)?;
    let token = f.req_string("token", 1..=4096)?;
    let mut agent = Fields {
        t: "hello.agent",
        map: f.req_object("agent")?,
    };
    let agent = AgentInfo {
        sdk: agent.req_string("sdk", 0..=AGENT_FIELD_MAX)?,
        sdk_version: agent.req_string("sdk_version", 0..=AGENT_FIELD_MAX)?,
        runtime: agent.opt_string("runtime", 0..=AGENT_FIELD_MAX)?,
        os: agent.opt_string("os", 0..=AGENT_FIELD_MAX)?,
    };
    Ok(Hello {
        v,
        token,
        agent,
        window: f.opt_int("window", WINDOW_RANGE)?.map(|n| n as u32),
        max_streams: f
            .opt_int("max_streams", MAX_STREAMS_RANGE)?
            .map(|n| n as u32),
        capabilities: f.opt_capabilities()?,
        meta: f.opt_object_or_null("meta")?,
    })
}

fn parse_welcome(f: &mut Fields, allow_subfloor_timing: bool) -> Check<Welcome> {
    let v = parse_version(f)?;
    let session = f.req_string("session", 0..=64)?;
    let window = f.req_int("window", WINDOW_RANGE)? as u32;
    let max_streams = f.req_int("max_streams", MAX_STREAMS_RANGE)? as u32;
    let ping_interval = f.req_int("ping_interval", DURATION_MS_RANGE)? as u64;
    let ping_timeout = f.req_int("ping_timeout", DURATION_MS_RANGE)? as u64;
    if allow_subfloor_timing {
        if ping_interval == 0 || ping_timeout == 0 {
            return protocol(format!(
                "welcome.ping_interval {ping_interval} and ping_timeout {ping_timeout} must be positive"
            ));
        }
    } else {
        if ping_interval < PING_INTERVAL_FLOOR {
            return protocol(format!(
                "welcome.ping_interval {ping_interval} is below the {PING_INTERVAL_FLOOR}ms floor"
            ));
        }
        if u128::from(ping_timeout) < 2 * u128::from(ping_interval) {
            return protocol(format!(
                "welcome.ping_timeout {ping_timeout} must be at least 2x ping_interval {ping_interval}"
            ));
        }
    }
    let server = match f.opt_object("server")? {
        None => None,
        Some(map) => {
            let mut s = Fields {
                t: "welcome.server",
                map,
            };
            Some(ServerInfo {
                name: s.opt_string("name", ANY_LEN)?.unwrap_or_default(),
                version: s.opt_string("version", ANY_LEN)?.unwrap_or_default(),
                node: s.opt_string("node", ANY_LEN)?.unwrap_or_default(),
            })
        }
    };
    let capabilities = f.opt_capabilities()?;
    let meta = f.opt_object_or_null("meta")?;
    Ok(Welcome {
        v,
        session,
        window,
        max_streams,
        ping_interval,
        ping_timeout,
        server,
        capabilities,
        meta,
    })
}

fn parse_ping_pong(f: &mut Fields) -> Check<(u64, Option<i64>)> {
    let id = f.req_int("id", PING_ID_RANGE)? as u64;
    let ts = f.opt_int("ts", I64_RANGE)?.map(|n| n as i64);
    Ok((id, ts))
}

fn parse_drain(f: &mut Fields) -> Check<Drain> {
    let clamped_ms = |n: i128| n.max(0) as u64;
    Ok(Drain {
        reason: f.req_string("reason", ANY_LEN)?,
        last_stream_id: f.req_int("last_stream_id", STREAM_ID_RANGE)? as u32,
        deadline_ms: f.opt_int("deadline_ms", I64_RANGE)?.map(clamped_ms),
        retry_after_ms: f.opt_int("retry_after_ms", I64_RANGE)?.map(clamped_ms),
        message: f.opt_string("message", 0..=256)?,
    })
}

fn parse_error(f: &mut Fields) -> Check<ErrorMsg> {
    Ok(ErrorMsg {
        code: f.req_int("code", 0..=i128::from(u32::MAX))? as u32,
        message: f.req_string("message", 0..=MAX_ERROR_MESSAGE)?,
        stream_id: f.opt_int("stream_id", STREAM_ID_RANGE)?.map(|n| n as u32),
        last_stream_id: f
            .opt_int("last_stream_id", STREAM_ID_RANGE)?
            .map(|n| n as u32),
    })
}

/// The remaining fields of one JSON object, consumed as they are validated.
struct Fields {
    /// Prefix for error messages, e.g. `"welcome"` or `"hello.agent"`.
    t: &'static str,
    map: Map<String, Value>,
}

impl Fields {
    fn required(&mut self, name: &str) -> Check<Value> {
        match self.map.remove(name) {
            Some(v) => Ok(v),
            None => protocol(format!("{} missing required field \"{name}\"", self.t)),
        }
    }

    fn req_int(&mut self, name: &str, range: RangeInclusive<i128>) -> Check<i128> {
        let v = self.required(name)?;
        self.int(name, &v, range)
    }

    fn opt_int(&mut self, name: &str, range: RangeInclusive<i128>) -> Check<Option<i128>> {
        match self.map.remove(name) {
            None => Ok(None),
            Some(v) => self.int(name, &v, range).map(Some),
        }
    }

    fn int(&self, name: &str, v: &Value, range: RangeInclusive<i128>) -> Check<i128> {
        let n = match as_integer(v) {
            Some(Integral::Value(n)) => n,
            Some(Integral::Huge) => {
                return protocol(format!(
                    "{}.{name} {v} out of range {}..={}",
                    self.t,
                    range.start(),
                    range.end()
                ));
            }
            None => {
                return protocol(format!(
                    "{}.{name} must be an integer, got {}",
                    self.t,
                    describe(v)
                ));
            }
        };
        if !range.contains(&n) {
            return protocol(format!(
                "{}.{name} {n} out of range {}..={}",
                self.t,
                range.start(),
                range.end()
            ));
        }
        Ok(n)
    }

    fn req_string(&mut self, name: &str, chars: RangeInclusive<usize>) -> Check<String> {
        let v = self.required(name)?;
        self.string(name, v, chars)
    }

    fn opt_string(&mut self, name: &str, chars: RangeInclusive<usize>) -> Check<Option<String>> {
        match self.map.remove(name) {
            None => Ok(None),
            Some(v) => self.string(name, v, chars).map(Some),
        }
    }

    /// String lengths count characters, as JSON Schema's `maxLength` does.
    fn string(&self, name: &str, v: Value, chars: RangeInclusive<usize>) -> Check<String> {
        let Value::String(s) = v else {
            return protocol(format!(
                "{}.{name} must be a string, got {}",
                self.t,
                describe(&v)
            ));
        };
        let n = s.chars().count();
        if !chars.contains(&n) {
            return protocol(format!(
                "{}.{name} is {n} characters, must be {}..={}",
                self.t,
                chars.start(),
                chars.end()
            ));
        }
        Ok(s)
    }

    fn req_object(&mut self, name: &str) -> Check<Map<String, Value>> {
        let v = self.required(name)?;
        self.object(name, v)
    }

    fn opt_object(&mut self, name: &str) -> Check<Option<Map<String, Value>>> {
        match self.map.remove(name) {
            None => Ok(None),
            Some(v) => self.object(name, v).map(Some),
        }
    }

    /// An optional object where `null` means absent (`hello.meta`, `welcome.meta`).
    fn opt_object_or_null(&mut self, name: &str) -> Check<Option<Map<String, Value>>> {
        match self.map.remove(name) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => self.object(name, v).map(Some),
        }
    }

    fn object(&self, name: &str, v: Value) -> Check<Map<String, Value>> {
        match v {
            Value::Object(m) => Ok(m),
            other => protocol(format!(
                "{}.{name} must be an object, got {}",
                self.t,
                describe(&other)
            )),
        }
    }

    fn opt_capabilities(&mut self) -> Check<Vec<String>> {
        let Some(v) = self.map.remove("capabilities") else {
            return Ok(Vec::new());
        };
        let Value::Array(items) = v else {
            return protocol(format!(
                "{}.capabilities must be an array of strings, got {}",
                self.t,
                describe(&v)
            ));
        };
        let mut out = Vec::with_capacity(items.len());
        for (i, item) in items.into_iter().enumerate() {
            match item {
                Value::String(s) if valid_capability(&s) => out.push(s),
                Value::String(s) => {
                    return protocol(format!(
                        "{}.capabilities[{i}] {:?} does not match ^[a-z0-9_.-]{{1,64}}$",
                        self.t,
                        truncate_utf8(&s, 80)
                    ));
                }
                other => {
                    return protocol(format!(
                        "{}.capabilities[{i}] must be a string, got {}",
                        self.t,
                        describe(&other)
                    ));
                }
            }
        }
        Ok(out)
    }
}

/// `^[a-z0-9_.-]{1,64}$`.
pub(crate) fn valid_capability(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-'))
}

/// An integral JSON number.
enum Integral {
    Value(i128),
    /// An integral float beyond `i128`, outside every field's range.
    Huge,
}

/// The value of an integral JSON number. `None` for every non-number and for
/// every number with a fraction.
fn as_integer(v: &Value) -> Option<Integral> {
    let Value::Number(n) = v else { return None };
    if let Some(n) = n.as_u64() {
        return Some(Integral::Value(n.into()));
    }
    if let Some(n) = n.as_i64() {
        return Some(Integral::Value(n.into()));
    }
    // serde_json never holds a non-finite float.
    let f = n.as_f64()?;
    if f.fract() != 0.0 {
        return None;
    }
    const LIMIT: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0; // 2^127
    Some(if (-LIMIT..LIMIT).contains(&f) {
        Integral::Value(f as i128)
    } else {
        Integral::Huge
    })
}

/// A short description of `v` for error messages, carrying the wire value
/// where it is small.
fn describe(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => format!("boolean {b}"),
        Value::Number(n) => describe_number(n),
        Value::String(s) => format!("string {:?}", truncate_utf8(s, 64)),
        Value::Array(a) => format!("array of {} elements", a.len()),
        Value::Object(o) => format!("object with {} fields", o.len()),
    }
}

fn describe_number(n: &Number) -> String {
    if n.as_f64().is_some_and(|f| f.fract() != 0.0) {
        format!("non-integer number {n}")
    } else {
        format!("number {n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => panic!("not an object"),
        }
    }

    #[test]
    fn hello_t_first() {
        let hello = Hello {
            v: 1,
            token: "tok".into(),
            agent: AgentInfo {
                sdk: "ws-mixer-rust".into(),
                sdk_version: "0.1.0".into(),
                runtime: None,
                os: None,
            },
            window: Some(262144),
            max_streams: None,
            capabilities: vec![],
            meta: Some(obj(json!({"k": 1}))),
        };
        assert_eq!(
            serde_json::to_string(&hello).unwrap(),
            r#"{"t":"hello","v":1,"token":"tok","agent":{"sdk":"ws-mixer-rust","sdk_version":"0.1.0"},"window":262144,"meta":{"k":1}}"#
        );
    }

    #[test]
    fn small_messages_t_first() {
        let cases: Vec<(String, &str)> = vec![
            (
                serde_json::to_string(&Ping {
                    id: 42,
                    ts: Some(7),
                })
                .unwrap(),
                r#"{"t":"ping","id":42,"ts":7}"#,
            ),
            (
                serde_json::to_string(&Pong { id: 42, ts: None }).unwrap(),
                r#"{"t":"pong","id":42}"#,
            ),
            (
                serde_json::to_string(&Drain {
                    reason: "client_requested".into(),
                    last_stream_id: 0,
                    deadline_ms: Some(5000),
                    retry_after_ms: None,
                    message: None,
                })
                .unwrap(),
                r#"{"t":"drain","reason":"client_requested","last_stream_id":0,"deadline_ms":5000}"#,
            ),
            (
                serde_json::to_string(&ErrorMsg {
                    code: 14,
                    message: "bye".into(),
                    stream_id: None,
                    last_stream_id: None,
                })
                .unwrap(),
                r#"{"t":"error","code":14,"message":"bye"}"#,
            ),
            (
                serde_json::to_string(&App {
                    body: obj(json!({"op": "register"})),
                })
                .unwrap(),
                r#"{"t":"app","body":{"op":"register"}}"#,
            ),
        ];
        for (got, want) in cases {
            assert_eq!(got, want);
        }
    }

    #[test]
    fn welcome_round_trip() {
        let raw = r#"{"t":"welcome","v":1,"session":"01J8","window":262144,"max_streams":64,"ping_interval":30000,"ping_timeout":90000,"server":{"name":"edge","version":"1.4.2","node":"n1"},"extra":true}"#;
        let w = match parse_control(raw.as_bytes(), false).unwrap() {
            ControlMsg::Welcome(w) => w,
            other => panic!("{other:?}"),
        };
        assert_eq!(w.session, "01J8");
        assert_eq!(w.ping_timeout, 90000);
        assert_eq!(w.server.as_ref().unwrap().node, "n1");
        assert!(w.capabilities.is_empty());
        let out = serde_json::to_string(&w).unwrap();
        assert!(out.starts_with(r#"{"t":"welcome","v":1,"#), "{out}");
        assert!(!out.contains("extra"));
    }

    fn parse(raw: &str) -> Result<ControlMsg, ControlError> {
        parse_control(raw.as_bytes(), false)
    }

    fn err(raw: &str) -> ControlError {
        let e = parse(raw).expect_err(raw);
        assert!(
            e.message.bytes().any(|b| b.is_ascii_digit()),
            "no number in {e}"
        );
        e
    }

    const WELCOME: &str = r#"{"t":"welcome","v":1,"session":"s1","window":262144,"max_streams":64,"ping_interval":30000,"ping_timeout":90000}"#;

    fn welcome_with(field: &str, value: &str) -> String {
        let mut v: Value = serde_json::from_str(WELCOME).unwrap();
        v[field] = serde_json::from_str(value).unwrap();
        // Re-render through a string so a float literal stays a float literal.
        v.to_string()
    }

    #[test]
    fn spec_control_fixtures() {
        use crate::frame::spec_fixtures::{json_files, spec_dir};
        let Some(spec) = spec_dir() else { return };
        let root = spec.join("fixtures/control");
        let files = json_files(&root);
        assert!(
            files.len() >= 47,
            "only {} control fixtures under {}, expected at least 47",
            files.len(),
            root.display()
        );
        for path in &files {
            let name = path.strip_prefix(&root).unwrap().display().to_string();
            let fx: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            let raw = match fx.get("raw").and_then(Value::as_str) {
                Some(raw) => raw.as_bytes().to_vec(),
                None => serde_json::to_vec(&fx["message"]).unwrap(),
            };
            let got = parse_control(&raw, false);
            if fx["wire_valid"].as_bool().unwrap() {
                if let Err(e) = got {
                    panic!("{name}: expected wire-valid, got {e}");
                }
                continue;
            }
            let e = match got {
                Err(e) => e,
                Ok(m) => panic!("{name}: expected rejection, parsed {m:?}"),
            };
            if let Some(code) = fx["expect"]["error_code"].as_str() {
                assert_eq!(e.code.name(), code, "{name}: {e}");
            }
            if let Some(needle) = fx["expect"]["message_contains"].as_str() {
                assert!(e.message.contains(needle), "{name}: {needle:?} not in {e}");
            }
            assert!(
                e.message.bytes().any(|b| b.is_ascii_digit()),
                "{name}: no number in {e}"
            );
        }
        eprintln!("spec control fixtures exercised: {}", files.len());
    }

    #[test]
    fn envelope_order() {
        assert!(err("{").message.starts_with("malformed JSON"));
        assert!(err("\u{feff}{}").message.starts_with("malformed JSON"));
        assert!(
            err("[1]")
                .message
                .contains("must be a JSON object, got array of 1")
        );
        assert!(err("7").message.contains("got number 7"));
        assert!(
            err(r#"{"v":1}"#)
                .message
                .contains("missing required field \"t\"")
        );
        // `t` type is checked before anything else in the object.
        let e = err(r#"{"t":2,"body":"x"}"#);
        assert!(
            e.message.contains("\"t\" must be a string, got number 2"),
            "{e}"
        );
        let e = err(r#"{"t":"regsiter","v":1}"#);
        assert_eq!(e.code, ErrorCode::PROTOCOL_ERROR);
        assert!(
            e.message
                .contains(r#"unknown control message type "regsiter""#),
            "{e}"
        );
        assert!(e.message.ends_with("(22-byte control message)"), "{e}");
        let big = format!(r#"{{"t":"app","body":{{"x":"{}"}}}}"#, "a".repeat(16_384));
        assert_eq!(err(&big).code, ErrorCode::ENHANCE_YOUR_CALM);
    }

    #[test]
    fn integral_floats_accepted_as_integers() {
        let base = parse(WELCOME).unwrap();
        for (field, value) in [
            ("v", "1.0"),
            ("v", "1e0"),
            ("window", "262144.0"),
            ("window", "2.62144e5"),
            ("max_streams", "6.4e1"),
            ("ping_interval", "3e4"),
            ("ping_timeout", "90000.0"),
        ] {
            assert_eq!(
                parse(&welcome_with(field, value)).unwrap(),
                base,
                "{field}={value}"
            );
        }
        let e = err(&welcome_with("v", "7e0"));
        assert_eq!(e.code, ErrorCode::UNSUPPORTED);
        assert!(e.message.contains("welcome.v 7 is not supported"), "{e}");
        assert_eq!(
            err(&welcome_with("v", "1e300")).code,
            ErrorCode::UNSUPPORTED
        );
        assert_eq!(
            parse(r#"{"t":"ping","id":1.0,"ts":-5e0}"#).unwrap(),
            ControlMsg::Ping(Ping {
                id: 1,
                ts: Some(-5)
            })
        );
        assert_eq!(
            parse(r#"{"t":"drain","reason":"rollout","last_stream_id":1.0,"deadline_ms":1e3,"retry_after_ms":-0.0}"#)
                .unwrap(),
            ControlMsg::Drain(Drain {
                reason: "rollout".into(),
                last_stream_id: 1,
                deadline_ms: Some(1000),
                retry_after_ms: Some(0),
                message: None,
            })
        );
        assert_eq!(
            parse(r#"{"t":"error","code":1.0,"message":"x","stream_id":3e0,"last_stream_id":4.0}"#)
                .unwrap(),
            ControlMsg::Error(ErrorMsg {
                code: 1,
                message: "x".into(),
                stream_id: Some(3),
                last_stream_id: Some(4),
            })
        );
    }

    #[test]
    fn fractional_and_out_of_range_floats_rejected() {
        for (field, value) in [
            ("v", "1.5"),
            ("window", "262144.5"),
            ("max_streams", "64.5"),
            ("ping_interval", "3.00001e4"),
        ] {
            let e = err(&welcome_with(field, value));
            assert_eq!(e.code, ErrorCode::PROTOCOL_ERROR, "{field}={value}");
            assert!(
                e.message.contains(&format!(
                    "welcome.{field} must be an integer, got non-integer number"
                )),
                "{e}"
            );
        }
        for raw in [
            r#"{"t":"ping","id":1.5}"#,
            r#"{"t":"pong","id":1,"ts":5.5}"#,
            r#"{"t":"drain","reason":"rollout","last_stream_id":1,"deadline_ms":1e-3}"#,
            r#"{"t":"error","code":1.5,"message":"x 1"}"#,
            r#"{"t":"error","code":1,"message":"x","stream_id":3.25}"#,
        ] {
            assert!(err(raw).message.contains("non-integer number"), "{raw}");
        }
        for (field, value, needle) in [
            ("window", "1e10", "welcome.window 10000000000 out of range"),
            ("window", "16383.0", "welcome.window 16383 out of range"),
            (
                "ping_timeout",
                "18446744073709551616",
                "welcome.ping_timeout 18446744073709551616 out of range",
            ),
            (
                "max_streams",
                "1e300",
                "welcome.max_streams 1e+300 out of range",
            ),
        ] {
            let e = err(&welcome_with(field, value));
            assert_eq!(e.code, ErrorCode::PROTOCOL_ERROR, "{field}={value}");
            assert!(e.message.contains(needle), "{field}={value}: {e}");
        }
        let e = err(&welcome_with("v", "true"));
        assert_eq!(e.code, ErrorCode::PROTOCOL_ERROR);
        assert!(
            e.message
                .contains("welcome.v must be an integer, got boolean true"),
            "{e}"
        );
        // Floats inside opaque objects are the application's business.
        assert!(parse(r#"{"t":"app","body":{"x":1.5}}"#).is_ok());
    }

    #[test]
    fn welcome_checks() {
        let w = match parse(WELCOME).unwrap() {
            ControlMsg::Welcome(w) => w,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            (w.window, w.max_streams, w.ping_interval),
            (262_144, 64, 30_000)
        );

        let e = err(&welcome_with("v", "2"));
        assert_eq!(e.code, ErrorCode::UNSUPPORTED);
        assert!(e.message.contains("welcome.v 2"), "{e}");
        assert_eq!(
            err(&welcome_with("v", "\"1\"")).code,
            ErrorCode::PROTOCOL_ERROR
        );

        for (field, value, needle) in [
            ("window", "16383", "welcome.window 16383 out of range"),
            (
                "window",
                "2147483648",
                "welcome.window 2147483648 out of range",
            ),
            ("max_streams", "0", "welcome.max_streams 0 out of range"),
            ("max_streams", "100001", "welcome.max_streams 100001"),
            (
                "ping_interval",
                "4999",
                "welcome.ping_interval 4999 is below",
            ),
            (
                "ping_timeout",
                "59999",
                "welcome.ping_timeout 59999 must be at least 2x ping_interval 30000",
            ),
            (
                "session",
                &format!("\"{}\"", "a".repeat(65)),
                "welcome.session is 65 characters",
            ),
            (
                "ping_timeout",
                "9223372036854775808",
                "welcome.ping_timeout 9223372036854775808 out of range",
            ),
            (
                "session",
                "7",
                "welcome.session must be a string, got number 7",
            ),
            (
                "capabilities",
                "[\"ok\",\"Bad\"]",
                "welcome.capabilities[1] \"Bad\"",
            ),
            (
                "capabilities",
                "[\"ok\",3]",
                "welcome.capabilities[1] must be a string",
            ),
            ("capabilities", "\"x\"", "must be an array of strings"),
            ("meta", "[]", "welcome.meta must be an object"),
            (
                "server",
                "{\"name\":1}",
                "welcome.server.name must be a string",
            ),
        ] {
            let e = err(&welcome_with(field, value));
            assert_eq!(e.code, ErrorCode::PROTOCOL_ERROR, "{field}={value}");
            assert!(e.message.contains(needle), "{field}={value}: {e}");
        }
        for (field, value) in [
            ("window", "16384"),
            ("window", "2147483647"),
            ("max_streams", "1"),
            ("max_streams", "100000"),
            ("ping_timeout", "60000"),
            ("ping_timeout", "9223372036854775807"),
            ("session", "\"\""),
            ("session", &format!("\"{}\"", "é".repeat(64))),
            ("meta", "null"),
            ("server", &format!("{{\"name\":\"{}\"}}", "n".repeat(500))),
            (
                "capabilities",
                &format!("[\"{}\",\"a.b-c_9\"]", "z".repeat(64)),
            ),
            ("unknown_field", "{\"x\":1.5}"),
        ] {
            assert!(
                parse(&welcome_with(field, value)).is_ok(),
                "{field}={value}"
            );
        }
        let e = err(&welcome_with(
            "capabilities",
            &format!("[\"{}\"]", "z".repeat(65)),
        ));
        assert!(e.message.contains("capabilities[0]"), "{e}");
        match parse(&welcome_with("meta", "null")).unwrap() {
            ControlMsg::Welcome(w) => assert_eq!(w.meta, None),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn subfloor_timing_skips_only_floors() {
        let raw = welcome_with("ping_interval", "100");
        let raw = {
            let mut v: Value = serde_json::from_str(&raw).unwrap();
            v["ping_timeout"] = json!(150);
            v.to_string()
        };
        assert!(parse_control(raw.as_bytes(), false).is_err());
        assert!(parse_control(raw.as_bytes(), true).is_ok());
        for (field, value) in [
            ("ping_interval", "100.5"),
            ("ping_interval", "\"100\""),
            ("ping_interval", "-1"),
            ("ping_interval", "0"),
            ("ping_timeout", "0"),
            ("window", "100"),
        ] {
            let e = parse_control(welcome_with(field, value).as_bytes(), true).unwrap_err();
            assert_eq!(e.code, ErrorCode::PROTOCOL_ERROR, "{field}={value}");
        }
    }

    #[test]
    fn drain_reason_mapping() {
        let drain = |reason: &str| match parse(&format!(
            r#"{{"t":"drain","reason":"{reason}","last_stream_id":7}}"#
        ))
        .unwrap()
        {
            ControlMsg::Drain(d) => d,
            other => panic!("{other:?}"),
        };
        for known in [
            "rollout",
            "overload",
            "id_exhausted",
            "replaced",
            "maintenance",
            "client_requested",
        ] {
            let d = drain(known);
            assert_eq!(d.drain_reason().as_str(), known);
            assert!(!d.unknown_reason(), "{known}");
        }
        let d = drain("v2_capacity_shed");
        assert_eq!(d.drain_reason(), DrainReason::Maintenance);
        assert!(d.unknown_reason());
        assert_eq!(d.reason, "v2_capacity_shed");
        assert_eq!(d.last_stream_id, 7);
        let e = err(r#"{"t":"drain","reason":"rollout","last_stream_id":2147483648}"#);
        assert!(
            e.message
                .contains("drain.last_stream_id 2147483648 out of range"),
            "{e}"
        );

        assert_eq!(drain("").reason, "");
        let long = "r".repeat(300);
        assert_eq!(drain(&long).reason, long);
        let d = match parse(
            r#"{"t":"drain","reason":"rollout","last_stream_id":1,"deadline_ms":-5,"retry_after_ms":9223372036854775807}"#,
        )
        .unwrap()
        {
            ControlMsg::Drain(d) => d,
            other => panic!("{other:?}"),
        };
        assert_eq!(d.deadline_ms, Some(0));
        assert_eq!(d.retry_after_ms, Some(i64::MAX as u64));
        for field in ["deadline_ms", "retry_after_ms"] {
            let e = err(&format!(
                r#"{{"t":"drain","reason":"rollout","last_stream_id":1,"{field}":9223372036854775808}}"#
            ));
            assert!(
                e.message
                    .contains(&format!("drain.{field} 9223372036854775808 out of range")),
                "{e}"
            );
        }
    }

    #[test]
    fn ping_pong_and_error_ranges() {
        assert_eq!(
            parse(r#"{"t":"ping","id":9007199254740991,"ts":0,"extra":[1]}"#).unwrap(),
            ControlMsg::Ping(Ping {
                id: (1 << 53) - 1,
                ts: Some(0)
            })
        );
        assert!(
            err(r#"{"t":"ping","id":-1}"#)
                .message
                .contains("ping.id -1 out of range")
        );
        assert!(
            err(r#"{"t":"pong","id":9007199254740992}"#)
                .message
                .contains("pong.id 9007199254740992")
        );
        assert_eq!(
            parse(r#"{"t":"error","code":4294967295,"message":"m 1","stream_id":41}"#).unwrap(),
            ControlMsg::Error(ErrorMsg {
                code: u32::MAX,
                message: "m 1".into(),
                stream_id: Some(41),
                last_stream_id: None,
            })
        );
        assert!(
            err(r#"{"t":"error","code":4294967296,"message":"m"}"#)
                .message
                .contains("4294967296")
        );
        assert_eq!(
            parse(r#"{"t":"error","code":1,"message":""}"#).unwrap(),
            ControlMsg::Error(ErrorMsg {
                code: 1,
                message: String::new(),
                stream_id: None,
                last_stream_id: None,
            })
        );
        assert!(
            err(&format!(
                r#"{{"t":"error","code":1,"message":"{}"}}"#,
                "m".repeat(1025)
            ))
            .message
            .contains("error.message is 1025 characters")
        );
        for (ts, want) in [
            ("-1", -1),
            ("-9223372036854775808", i64::MIN),
            ("9223372036854775807", i64::MAX),
        ] {
            let raw = format!(r#"{{"t":"ping","id":1,"ts":{ts}}}"#);
            let ping = match parse(&raw).unwrap() {
                ControlMsg::Ping(p) => p,
                other => panic!("{other:?}"),
            };
            assert_eq!(ping.ts, Some(want));
            let pong = serde_json::to_string(&Pong::echo(&ping)).unwrap();
            assert_eq!(pong, format!(r#"{{"t":"pong","id":1,"ts":{ts}}}"#));
        }
        assert!(
            err(r#"{"t":"ping","id":1,"ts":9223372036854775808}"#)
                .message
                .contains("ping.ts 9223372036854775808 out of range")
        );
    }

    #[test]
    fn hello_round_trip_and_checks() {
        let hello = Hello::new(
            "tok".into(),
            AgentInfo::default(),
            262_144,
            64,
            vec!["app.v2".into()],
            Some(obj(json!({"k": [1, 2]}))),
        );
        let wire = serde_json::to_string(&hello).unwrap();
        assert!(
            wire.starts_with(r#"{"t":"hello","v":1,"token":"tok","agent":{"sdk":"ws-mixer-rust""#),
            "{wire}"
        );
        assert_eq!(parse(&wire).unwrap(), ControlMsg::Hello(hello));

        let e = err(r#"{"t":"hello","v":2,"token":"x","agent":{"sdk":"a","sdk_version":"1"}}"#);
        assert_eq!(e.code, ErrorCode::UNSUPPORTED);
        let e = err(r#"{"t":"hello","v":1,"token":"x","agent":{"sdk":"a"}}"#);
        assert!(
            e.message
                .contains("hello.agent missing required field \"sdk_version\""),
            "{e}"
        );
        let long = "a".repeat(129);
        let e = err(&format!(
            r#"{{"t":"hello","v":1,"token":"x","agent":{{"sdk":"{long}","sdk_version":"1"}}}}"#
        ));
        assert!(
            e.message.contains("hello.agent.sdk is 129 characters"),
            "{e}"
        );

        let raw =
            r#"{"t":"hello","v":1,"token":"x","agent":{"sdk":"a","sdk_version":"1"},"meta":null}"#;
        match parse(raw).unwrap() {
            ControlMsg::Hello(h) => assert_eq!(h.meta, None),
            other => panic!("{other:?}"),
        }
        let e = err(
            r#"{"t":"hello","v":1,"token":"x","agent":{"sdk":"a","sdk_version":"1"},"meta":[1]}"#,
        );
        assert!(e.message.contains("hello.meta must be an object"), "{e}");
    }

    #[test]
    fn every_message_type_round_trips() {
        let msgs = [
            ControlMsg::Ping(Ping { id: 0, ts: None }),
            ControlMsg::Pong(Pong::echo(&Ping {
                id: 5,
                ts: Some(1_756_137_600_123),
            })),
            ControlMsg::Drain(Drain {
                reason: "client_requested".into(),
                last_stream_id: 0,
                deadline_ms: Some(5000),
                retry_after_ms: Some(0),
                message: Some("bye".into()),
            }),
            ControlMsg::Error(ErrorMsg::new(ErrorCode::APPLICATION_CLOSE, "CONN_LIMIT: 3")),
            ControlMsg::App(App {
                body: obj(json!({"nested": {"a": [1, "x", null]}})),
            }),
            ControlMsg::Welcome(match parse(WELCOME).unwrap() {
                ControlMsg::Welcome(w) => w,
                _ => unreachable!(),
            }),
        ];
        for msg in msgs {
            let wire = match &msg {
                ControlMsg::Hello(m) => serde_json::to_vec(m),
                ControlMsg::Welcome(m) => serde_json::to_vec(m),
                ControlMsg::Ping(m) => serde_json::to_vec(m),
                ControlMsg::Pong(m) => serde_json::to_vec(m),
                ControlMsg::Drain(m) => serde_json::to_vec(m),
                ControlMsg::Error(m) => serde_json::to_vec(m),
                ControlMsg::App(m) => serde_json::to_vec(m),
            }
            .unwrap();
            assert!(wire.starts_with(br#"{"t":""#));
            assert_eq!(parse_control(&wire, false).unwrap(), msg);
        }
    }

    #[test]
    fn error_message_truncated() {
        let e = ErrorMsg::new(ErrorCode::INTERNAL_ERROR, &"é".repeat(600));
        assert_eq!(e.message.len(), MAX_ERROR_MESSAGE);
        let e = ErrorMsg::new(ErrorCode::INTERNAL_ERROR, &format!("a{}", "é".repeat(600)));
        assert_eq!(e.message.len(), MAX_ERROR_MESSAGE - 1);
        let e = ErrorMsg::new(ErrorCode::FLOW_CONTROL_ERROR, "");
        assert_eq!(e.message, "FLOW_CONTROL_ERROR (3)");
    }

    #[test]
    fn digit_suffix_only_when_detail_has_none() {
        let e = err(r#"{"t":"ping","id":-1}"#);
        assert!(!e.message.contains("control message)"), "{e}");
        let e = err(r#"{"t":"ping"}"#);
        assert!(e.message.ends_with("(12-byte control message)"), "{e}");
    }

    #[test]
    fn mutated_input_never_panics() {
        let seeds = [
            WELCOME.to_owned(),
            r#"{"t":"welcome","v":1,"session":"s","window":262144,"max_streams":64,"ping_interval":30000,"ping_timeout":90000,"server":{"name":"e","version":"1","node":"n"},"capabilities":["a"],"meta":{"k":1}}"#.to_owned(),
            r#"{"t":"hello","v":1,"token":"x","agent":{"sdk":"a","sdk_version":"1","runtime":"r","os":"o"},"window":262144,"max_streams":64,"capabilities":["c"],"meta":{}}"#.to_owned(),
            r#"{"t":"ping","id":42,"ts":1756137600123}"#.to_owned(),
            r#"{"t":"pong","id":42,"ts":1756137600123}"#.to_owned(),
            r#"{"t":"drain","reason":"rollout","last_stream_id":7,"deadline_ms":5,"retry_after_ms":5,"message":"m"}"#.to_owned(),
            r#"{"t":"error","code":3,"message":"m 1","stream_id":1,"last_stream_id":1}"#.to_owned(),
            r#"{"t":"app","body":{"a":[1,2.5,null]}}"#.to_owned(),
        ];
        let values = [
            json!(null),
            json!(true),
            json!(0),
            json!(-1),
            json!(1.5),
            json!(u64::MAX),
            json!(i64::MIN),
            json!(""),
            json!("é".repeat(70)),
            json!([1, "x"]),
            json!({"x": 1}),
        ];
        let mut rng = fastrand::Rng::with_seed(0xc0de_1234);
        for _ in 0..20_000 {
            let seed = &seeds[rng.usize(..seeds.len())];
            let raw = match rng.u8(0..4) {
                0 => {
                    let mut b = seed.clone().into_bytes();
                    for _ in 0..rng.usize(1..4) {
                        let i = rng.usize(..b.len());
                        b[i] = rng.u8(..);
                    }
                    b
                }
                1 => {
                    let mut b = seed.clone().into_bytes();
                    let i = rng.usize(..b.len());
                    let j = rng.usize(i..=b.len());
                    b.drain(i..j);
                    b
                }
                2 => {
                    let len = rng.usize(0..64);
                    std::iter::repeat_with(|| rng.u8(..)).take(len).collect()
                }
                _ => {
                    let mut v: Value = serde_json::from_str(seed).unwrap();
                    let map = v.as_object_mut().unwrap();
                    let keys: Vec<String> = map.keys().cloned().collect();
                    let key = keys[rng.usize(..keys.len())].clone();
                    if rng.bool() {
                        map.remove(&key);
                    } else {
                        map.insert(key, values[rng.usize(..values.len())].clone());
                    }
                    serde_json::to_vec(&v).unwrap()
                }
            };
            let _ = parse_control(&raw, rng.bool());
        }
    }

    #[test]
    fn default_agent() {
        let a = AgentInfo::default();
        assert_eq!(a.sdk, "ws-mixer-rust");
        assert_eq!(a.sdk_version, crate::SDK_VERSION);
    }
}

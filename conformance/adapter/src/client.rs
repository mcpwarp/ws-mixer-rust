//! `connect` and the client's event loop.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};
use ws_mixer::{Client, DisconnectReason, Event, Events, ReconnectPolicy};

use crate::emit::{ack, cmd_error, emit};
use crate::{Adapter, lock};

/// `set_options` values the client uses. `ping_interval_ms`/`ping_timeout_ms`
/// are accepted and ignored: a client takes keepalive timing from `welcome`.
pub struct Options {
    pub window: u32,
    pub max_streams: u32,
    pub hello_timeout: Option<Duration>,
    pub time_scale: f64,
    pub floor_ms: u64,
    pub allow_subfloor_timing: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            window: 262_144,
            max_streams: 64,
            hello_timeout: None,
            time_scale: 1.0,
            floor_ms: 300,
            allow_subfloor_timing: false,
        }
    }
}

impl Options {
    pub fn apply(&mut self, cmd: &Map<String, Value>) -> Result<(), String> {
        if let Some(v) = cmd.get("window") {
            self.window = as_u32(v, "window")?;
        }
        if let Some(v) = cmd.get("max_streams") {
            self.max_streams = as_u32(v, "max_streams")?;
        }
        if let Some(v) = cmd.get("hello_timeout_ms") {
            let ms = v
                .as_u64()
                .ok_or("hello_timeout_ms must be a non-negative integer")?;
            self.hello_timeout = Some(Duration::from_millis(ms.max(1)));
        }
        if let Some(v) = cmd.get("time_scale") {
            self.time_scale = v.as_f64().ok_or("time_scale must be a number")?;
        }
        if let Some(v) = cmd.get("floor_ms") {
            self.floor_ms = v
                .as_u64()
                .ok_or("floor_ms must be a non-negative integer")?;
        }
        if let Some(v) = cmd.get("allow_subfloor_timing") {
            self.allow_subfloor_timing =
                v.as_bool().ok_or("allow_subfloor_timing must be a bool")?;
        }
        Ok(())
    }

    /// A duration the adapter received verbatim, scaled by `time_scale` and
    /// clamped to `floor_ms` (docs/CONFORMANCE.md §1.1, `floor_ms`).
    pub fn scale(&self, ms: u64) -> Duration {
        let scaled = (ms as f64 * self.time_scale).round() as u64;
        Duration::from_millis(scaled.max(self.floor_ms))
    }
}

pub fn as_u32(v: &Value, name: &str) -> Result<u32, String> {
    v.as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| format!("{name} must be an integer in u32 range"))
}

/// `connect.reconnect`: absent or `enabled:false` disables the SDK's loop.
fn reconnect_policy(v: Option<&Value>) -> Result<ReconnectPolicy, String> {
    let Some(r) = v.filter(|r| r.get("enabled").and_then(Value::as_bool) == Some(true)) else {
        return Ok(ReconnectPolicy::disabled());
    };
    let mut p = ReconnectPolicy::default();
    let ms = |key: &str| r.get(key).and_then(Value::as_u64).filter(|&n| n > 0);
    if let Some(base) = ms("baseMs") {
        p.base = Duration::from_millis(base);
    }
    if let Some(cap) = ms("capMs") {
        p.cap = Duration::from_millis(cap);
        // The SDK rejects cap < base; a scenario pinning only capMs means
        // "never wait longer than this".
        p.base = p.base.min(p.cap);
    }
    if let Some(n) = r.get("maxAttempts").and_then(Value::as_u64) {
        let n = u32::try_from(n).map_err(|_| "reconnect.maxAttempts out of range")?;
        p.max_attempts = NonZeroU32::new(n);
    }
    Ok(p)
}

pub async fn connect(ad: Arc<Adapter>, seq: Value, cmd: Map<String, Value>) {
    if lock(&ad.client).is_some() {
        return cmd_error(
            &seq,
            "connect: already connected (one connection per process)",
        );
    }
    let Some(url) = cmd.get("url").and_then(Value::as_str) else {
        return cmd_error(&seq, "connect: url is required");
    };
    let reconnect = match reconnect_policy(cmd.get("reconnect")) {
        Ok(p) => p,
        Err(e) => return cmd_error(&seq, format!("connect: {e}")),
    };
    let mut b = Client::builder(url).reconnect(reconnect);
    if let Some(token) = cmd.get("token").and_then(Value::as_str) {
        b = b.token(token);
    }
    {
        let o = lock(&ad.opts);
        b = b.window(o.window).max_streams(o.max_streams);
        if let Some(t) = o.hello_timeout {
            b = b.hello_timeout(t);
        }
        if o.allow_subfloor_timing {
            b = b.allow_subfloor_timing();
        }
    }
    let (client, events) = match b.connect() {
        Ok(c) => c,
        Err(e) => return cmd_error(&seq, format!("connect: {e}")),
    };
    *lock(&ad.client) = Some(client);
    event_loop(ad, seq, events).await;
}

async fn event_loop(ad: Arc<Adapter>, seq: Value, mut events: Events) {
    let mut acked = false;
    while let Some(event) = events.recv().await {
        match event {
            Event::Connected(c) => {
                if c.reconnect {
                    emit(
                        json!({"event": "reconnected", "session": c.session, "welcome": c.welcome}),
                    );
                } else {
                    acked = true;
                    ack(&seq);
                    emit(
                        json!({"event": "connected", "seq": seq, "session": c.session, "welcome": c.welcome}),
                    );
                }
                ad.connected.send_replace(true);
            }
            Event::Stream(s) => ad.streams.open(s),
            Event::App(m) => emit(json!({"event": "app", "body": m.body})),
            Event::Drain(d) => {
                let mut e = json!({
                    "event": "drain",
                    "reason": d.raw_reason,
                    "last_stream_id": d.last_stream_id,
                });
                if let Some(dl) = d.deadline {
                    e["deadline_ms"] = json!(dl.as_millis() as u64);
                }
                if let Some(m) = d.message {
                    e["message"] = json!(m);
                }
                emit(e);
            }
            Event::Disconnected(r) => {
                ad.connected.send_replace(false);
                ad.streams.conn_ended(r.conn_id);
                emit(disconnected(&r));
                // No welcome and nothing more to come: the connect failed.
                if !acked && r.retry_in.is_none() {
                    acked = true;
                    cmd_error(&seq, format!("connect: {}", r.message));
                }
            }
            _ => {}
        }
    }
    if !acked {
        cmd_error(&seq, "connect: the client ended before welcome");
    }
}

fn disconnected(r: &DisconnectReason) -> Value {
    let mut e = json!({
        "event": "disconnected",
        "message": r.message,
        "fatal": r.fatal,
        "phase": r.phase.as_str(),
    });
    if let Some(c) = r.ws_code {
        e["ws_code"] = json!(c);
    }
    if let Some(c) = r.error_code {
        e["error_code"] = json!(c.0);
    }
    if let Some(n) = r.error_name {
        e["error_name"] = json!(n);
    }
    if let Some(cr) = &r.close_reason {
        e["close_reason"] = json!(cr);
    }
    e
}

/// Waits up to 5 s for a welcomed connection (Go adapter `awaitConn`): the
/// runner paces on the wire and can send a command before the event loop
/// has seen `welcome`. Returns the client if one exists by then, welcomed
/// or not.
pub async fn await_client(ad: &Adapter) -> Option<Client> {
    let mut rx = ad.connected.subscribe();
    let _ = tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|c| *c)).await;
    lock(&ad.client).clone()
}

//! Event output: one JSON object per line on stdout, flushed every time.

use std::io::Write;
use std::sync::OnceLock;
use std::time::Instant;

use serde_json::{Value, json};

static START: OnceLock<Instant> = OnceLock::new();

/// Anchors `t_ms`. Called first thing in `main`.
pub fn init_clock() {
    START.get_or_init(Instant::now);
}

/// Milliseconds since process start, monotonic.
pub fn t_ms() -> u64 {
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Writes `event` as one line. Write errors (the runner went away, EPIPE)
/// are swallowed: there is nobody left to report them to.
pub fn emit(event: Value) {
    let mut line = event.to_string().into_bytes();
    line.push(b'\n');
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(&line).and_then(|()| out.flush());
}

pub fn ack(seq: &Value) {
    emit(json!({"event": "ack", "seq": seq}));
}

pub fn cmd_error(seq: &Value, message: impl Into<String>) {
    emit(json!({"event": "error", "seq": seq, "message": message.into()}));
}

/// The role-inapplicable shape the runner turns into a SKIP.
pub fn unsupported(seq: &Value, message: impl Into<String>) {
    emit(json!({
        "event": "error",
        "seq": seq,
        "ok": false,
        "unsupported": true,
        "error": "unsupported",
        "message": message.into(),
    }));
}

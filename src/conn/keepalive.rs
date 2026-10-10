//! Ping scheduling and the pong watchdog (WIRE.md §2.9 keepalive).
//!
//! The first ping goes out after `random(0, ping_interval)` (spec and JS
//! conn.ts; Go starts at a full interval), then one every `ping_interval`.
//! The watchdog measures from the last valid pong, so a slow link degrades
//! into latency rather than a disconnect loop.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;

use super::shared::{ConnShared, PanicGuard};
use crate::control::Ping;
use crate::error::ErrorCode;
use crate::frame;

/// Stands in for a deadline that does not fit in an `Instant` (an
/// `i64::MAX`-millisecond timing value from `welcome`).
const FAR_FUTURE: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

fn add(t: Instant, d: Duration) -> Instant {
    t.checked_add(d)
        .or_else(|| t.checked_add(FAR_FUTURE))
        .unwrap_or(t)
}

pub(crate) async fn run(shared: Arc<ConnShared>, ping_interval_ms: u64, ping_timeout_ms: u64) {
    let _guard = PanicGuard(shared.clone());
    let interval = Duration::from_millis(ping_interval_ms);
    let timeout = Duration::from_millis(ping_timeout_ms);
    let jitter = Duration::from_millis(fastrand::u64(0..ping_interval_ms.max(1)));
    let mut next_ping = add(Instant::now(), jitter);
    loop {
        let deadline = add(shared.lock().last_pong_at, timeout);
        tokio::select! {
            _ = shared.cancel.cancelled() => return,
            _ = tokio::time::sleep_until(next_ping) => {
                send_ping(&shared, timeout);
                // After a stall, one ping and then a fresh interval: no burst
                // catching up on the missed ones.
                let now = Instant::now();
                next_ping = add(next_ping, interval);
                if next_ping <= now {
                    next_ping = add(now, interval);
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                // Re-read: a pong may have re-armed the watchdog meanwhile.
                let last = shared.lock().last_pong_at;
                if Instant::now() >= add(last, timeout) {
                    let ms = Instant::now().duration_since(last).as_millis();
                    shared.fail(
                        ErrorCode::KEEPALIVE_TIMEOUT,
                        format!("no pong for {ms}ms (ping_timeout {ping_timeout_ms}ms)"),
                        false,
                    );
                    return;
                }
            }
        }
    }
}

fn send_ping(shared: &ConnShared, timeout: Duration) {
    let now = Instant::now();
    let mut cs = shared.lock();
    if cs.terminal.is_some() {
        return;
    }
    // Pings older than the deadline will never usefully be acked; a late
    // pong for one counts as a duplicate.
    cs.outstanding_pings
        .retain(|_, sent| now.duration_since(*sent) <= timeout);
    let id = cs.next_ping_id;
    cs.next_ping_id += 1;
    cs.outstanding_pings.insert(id, now);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok());
    let ping = frame::encode_control(&Ping { id, ts }).expect("a ping is far below 16 KiB");
    cs.control.push_back(ping);
    drop(cs);
    shared.writer_wake.notify_one();
}

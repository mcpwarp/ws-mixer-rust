//! Writer task: urgent, control and round-robin stream scheduling, and the
//! connection close sequence (WIRE.md §2.6, §2.8).

use std::sync::Arc;

use futures_util::{Sink, SinkExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::shared::{CloseRequest, ConnShared, PanicGuard, WriteJob};
use super::{CLOSE_ECHO_WAIT, SHUTDOWN_WRITE_TIMEOUT, WRITE_TIMEOUT};
use crate::stats::Counters;

pub(crate) async fn run<S>(mut sink: S, shared: Arc<ConnShared>)
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let _guard = PanicGuard(shared.clone());
    let mut write_failed = false;
    let mut shutdown = None;
    loop {
        match shared.next_write() {
            WriteJob::Frame(frame) => {
                if let Err(e) =
                    send(&mut sink, &shared, Message::Binary(frame), &mut shutdown).await
                {
                    // The transport is presumed dead: no error frame (Go
                    // failWrite, conn.go:627-638).
                    shared.abnormal(format!("write failed: {e}"), None);
                    write_failed = true;
                    break;
                }
            }
            WriteJob::Close(req) => {
                // No close went out: no echo to wait for.
                write_failed = !close_sequence(&mut sink, &shared, req, &mut shutdown).await;
                break;
            }
            WriteJob::Exit => break,
            WriteJob::Idle => {
                tokio::select! {
                    _ = shared.writer_wake.notified() => {}
                    _ = shared.cancel.cancelled() => break,
                }
            }
        }
    }
    if !write_failed {
        // Let the reader consume the peer's close echo, at most ~2 s
        // (WIRE.md §2.8).
        tokio::select! {
            _ = shared.reader_done.cancelled() => {}
            _ = tokio::time::sleep(CLOSE_ECHO_WAIT) => {}
            _ = shared.cancel.cancelled() => {}
        }
    }
    shared.cancel.cancel();
}

/// The deadline for every write once the connection has ended, fixed when the
/// writer first sees the end.
fn shutdown_deadline(shared: &ConnShared, shutdown: &mut Option<Instant>) -> Option<Instant> {
    if shutdown.is_none() && shared.terminal().is_some() {
        *shutdown = Some(Instant::now() + SHUTDOWN_WRITE_TIMEOUT);
    }
    *shutdown
}

/// One message: feed + flush, bounded by [`WRITE_TIMEOUT`] (by the shutdown
/// deadline once the connection has ended, also for a write already in
/// flight) and by cancellation.
async fn send<S>(
    sink: &mut S,
    shared: &ConnShared,
    msg: Message,
    shutdown: &mut Option<Instant>,
) -> Result<(), String>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let len = match &msg {
        Message::Binary(b) => b.len(),
        _ => 0,
    };
    let mut deadline =
        shutdown_deadline(shared, shutdown).unwrap_or_else(|| Instant::now() + WRITE_TIMEOUT);
    let write = sink.send(msg);
    tokio::pin!(write);
    loop {
        tokio::select! {
            r = &mut write => {
                return match r {
                    Ok(()) => {
                        Counters::add(&shared.stats.bytes_out, len);
                        Ok(())
                    }
                    Err(e) => Err(e.to_string()),
                };
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(match shutdown {
                    Some(_) => format!(
                        "no progress within {}s of the connection ending",
                        SHUTDOWN_WRITE_TIMEOUT.as_secs()
                    ),
                    None => format!("no progress for {}s", WRITE_TIMEOUT.as_secs()),
                });
            }
            // Any permit taken here is harmless: the writer re-reads its
            // queues after every send.
            _ = shared.writer_wake.notified(), if shutdown.is_none() => {
                if let Some(d) = shutdown_deadline(shared, shutdown) {
                    deadline = deadline.min(d);
                }
            }
            _ = shared.cancel.cancelled() => return Err("cancelled".into()),
        }
    }
}

/// `error{}` (if any), then the WebSocket Close (WIRE.md §2.8 steps 1-2).
/// Failures are ignored: the connection is ending either way. Returns whether
/// the close frame was sent.
async fn close_sequence<S>(
    sink: &mut S,
    shared: &ConnShared,
    req: CloseRequest,
    shutdown: &mut Option<Instant>,
) -> bool
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    if let Some(error) = req.error {
        if send(sink, shared, Message::Binary(error), shutdown)
            .await
            .is_err()
        {
            return false;
        }
    }
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::from(req.code),
        reason: req.reason.into(),
    }));
    send(sink, shared, close, shutdown).await.is_ok()
}

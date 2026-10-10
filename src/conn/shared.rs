//! State shared between a connection's tasks and its stream handles.
//!
//! Lock order: the connection mutex before any stream mutex. Neither is ever
//! held across an `.await`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_util::sync::CancellationToken;

use super::{abnormal_reason, base_reason, client_drain, local_error_reason, peer_close_reason};
use crate::control::ErrorMsg;
use crate::error::{ErrorCode, close_reason};
use crate::event::{ConnId, DisconnectReason, Phase};
use crate::frame;
use crate::stats::Counters;
use crate::stream::{StreamPhase, StreamShared};

/// Locks `m`, ignoring poisoning: every critical section leaves the state
/// consistent before anything that could panic.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Connection state shared by the reader, writer, keepalive task and every
/// [`Stream`](crate::Stream) on the connection. Never locked across an await.
pub(crate) struct ConnShared {
    pub(crate) conn_id: ConnId,
    pub(crate) session: String,
    /// Our `hello.window`: the receive credit of every stream and the WINDOW
    /// threshold base.
    pub(crate) our_window: u32,
    /// `welcome.window`: the initial send credit of every stream.
    pub(crate) peer_window: u32,
    /// `min(welcome.max_streams, our max_streams)`.
    pub(crate) max_streams: u32,
    pub(crate) state: Mutex<ConnState>,
    /// Wakes the writer: something was queued or the connection ended.
    pub(crate) writer_wake: Notify,
    /// Fired whenever a stream leaves the table.
    pub(crate) table_changed: Notify,
    /// Stops every task. Cancelled by the writer once the close sequence is
    /// done, or immediately on a write failure or an abandon.
    pub(crate) cancel: CancellationToken,
    /// Cancelled when the reader has stopped (the peer's close echo or end
    /// of stream was read).
    pub(crate) reader_done: CancellationToken,
    pub(crate) stats: Arc<Counters>,
}

pub(crate) struct ConnState {
    pub(crate) streams: HashMap<u32, Arc<StreamShared>>,
    pub(crate) highest_opened: u32,
    /// `last_stream_id` of the peer's `drain`. Separate from
    /// `local_drain` (Go drain.go:52 conflates the two).
    pub(crate) peer_drained: Option<u32>,
    /// `last_stream_id` of the first `drain` we sent. Later drains never
    /// announce more (WIRE.md §2.9), and OPENs above it are refused.
    pub(crate) local_drain: Option<u32>,
    /// A graceful close is in progress: new OPENs are refused.
    pub(crate) closing: bool,
    pub(crate) next_ping_id: u64,
    /// Pings awaiting a pong, by id, with their send time.
    pub(crate) outstanding_pings: HashMap<u64, Instant>,
    pub(crate) last_pong_at: Instant,
    /// Why the connection ended. Set once; first writer wins.
    pub(crate) terminal: Option<Arc<DisconnectReason>>,
    /// The close sequence the writer still has to perform, if any.
    pub(crate) close_req: Option<CloseRequest>,
    /// Pongs, ahead of everything else (WIRE.md §2.7).
    pub(crate) urgent: VecDeque<Bytes>,
    /// WINDOW, CLOSE, RESET, app, drain, ping.
    pub(crate) control: VecDeque<Bytes>,
    /// Round-robin order of streams with queued DATA.
    pub(crate) ready: VecDeque<u32>,
    pub(crate) in_ready: HashSet<u32>,
}

/// The connection close the writer performs: optional `error{}`, then a
/// WebSocket Close (WIRE.md §2.8).
#[derive(Clone, Debug)]
pub(crate) struct CloseRequest {
    pub(crate) error: Option<Bytes>,
    pub(crate) code: u16,
    pub(crate) reason: String,
}

/// The writer's next step.
pub(crate) enum WriteJob {
    Frame(Bytes),
    Close(CloseRequest),
    Idle,
    Exit,
}

impl ConnShared {
    pub(crate) fn new(
        conn_id: ConnId,
        session: String,
        our_window: u32,
        peer_window: u32,
        max_streams: u32,
        stats: Arc<Counters>,
    ) -> Self {
        Self {
            conn_id,
            session,
            our_window,
            peer_window,
            max_streams,
            state: Mutex::new(ConnState {
                streams: HashMap::new(),
                highest_opened: 0,
                peer_drained: None,
                local_drain: None,
                closing: false,
                next_ping_id: 0,
                outstanding_pings: HashMap::new(),
                last_pong_at: Instant::now(),
                terminal: None,
                close_req: None,
                urgent: VecDeque::new(),
                control: VecDeque::new(),
                ready: VecDeque::new(),
                in_ready: HashSet::new(),
            }),
            writer_wake: Notify::new(),
            table_changed: Notify::new(),
            cancel: CancellationToken::new(),
            reader_done: CancellationToken::new(),
            stats,
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, ConnState> {
        lock(&self.state)
    }

    pub(crate) fn terminal(&self) -> Option<Arc<DisconnectReason>> {
        self.lock().terminal.clone()
    }

    #[cfg(test)]
    pub(crate) fn push_control(&self, frame: Bytes) {
        self.lock().control.push_back(frame);
        self.writer_wake.notify_one();
    }

    /// Removes a stream that is fully closed from the table.
    pub(crate) fn retire_locked(&self, cs: &mut ConnState, id: u32) {
        if cs.streams.remove(&id).is_some() {
            cs.in_ready.remove(&id);
            self.table_changed.notify_waiters();
        }
    }

    /// Resets a live stream: marks it closed with `code`, drops its buffers,
    /// retires it and, unless the connection has ended, queues `RESET`.
    /// Never used in answer to a received RESET.
    pub(crate) fn reset_stream_locked(
        &self,
        cs: &mut ConnState,
        st: &StreamShared,
        code: ErrorCode,
        message: &str,
    ) {
        {
            let mut s = lock(&st.state);
            if s.phase == StreamPhase::Closed {
                return;
            }
            s.phase = StreamPhase::Closed;
            s.reset = Some((code, message.to_owned()));
            s.inbuf.clear();
            s.out.clear();
            s.wake_all();
        }
        if cs.terminal.is_none() {
            cs.control
                .push_back(frame::encode_reset(st.id, code, message));
            self.writer_wake.notify_one();
        }
        self.retire_locked(cs, st.id);
    }

    /// Records why the connection ended (first call wins), fails every open
    /// stream with `ConnectionLost`, and hands `close` to the writer. Returns
    /// whether this call ended the connection.
    pub(crate) fn end(&self, reason: DisconnectReason, close: Option<CloseRequest>) -> bool {
        let mut cs = self.lock();
        if cs.terminal.is_some() {
            return false;
        }
        let reason = Arc::new(reason);
        cs.terminal = Some(reason.clone());
        cs.close_req = close;
        cs.ready.clear();
        cs.in_ready.clear();
        for st in std::mem::take(&mut cs.streams).into_values() {
            let mut s = lock(&st.state);
            s.conn_lost = Some(reason.clone());
            s.out.clear();
            s.wake_all();
        }
        drop(cs);
        self.table_changed.notify_waiters();
        self.writer_wake.notify_one();
        true
    }

    /// Fails the connection with a ws-mixer error raised locally: `error{}`
    /// then close `4000 + code` (WIRE.md §2.8). `violation` counts it as a
    /// peer protocol violation.
    pub(crate) fn fail(&self, code: ErrorCode, message: String, violation: bool) {
        let close = CloseRequest {
            error: Some(error_frame(code, &message)),
            code: code.ws_close_wire(),
            reason: close_reason(&message).to_owned(),
        };
        let reason = local_error_reason(Some(self.conn_id), Phase::Connected, code, message);
        let msg = reason.message.clone();
        if self.end(reason, Some(close)) {
            if violation {
                Counters::inc(&self.stats.protocol_violations);
            }
            tracing::warn!(conn_id = %self.conn_id, session = %self.session, code = %code, "failing connection: {msg}");
        }
    }

    /// The peer sent `error`: close with its code (clamped on the wire)
    /// without replying `error` (WIRE.md §2.7).
    pub(crate) fn peer_error(&self, m: &ErrorMsg) {
        let code = ErrorCode(m.code);
        let close = CloseRequest {
            error: None,
            code: code.ws_close_wire(),
            reason: close_reason(&m.message).to_owned(),
        };
        let reason =
            super::peer_error_reason(Some(self.conn_id), Phase::Connected, code, &m.message);
        if self.end(reason, Some(close)) {
            // Only 4001/4003/4004 mean an SDK bug (WIRE.md §2.9); 0 and 14
            // are normal closes.
            match code {
                ErrorCode::NO_ERROR | ErrorCode::APPLICATION_CLOSE => {
                    tracing::info!(conn_id = %self.conn_id, session = %self.session, code = m.code, "peer closed the connection: {}", m.message);
                }
                ErrorCode::PROTOCOL_ERROR
                | ErrorCode::FLOW_CONTROL_ERROR
                | ErrorCode::FRAME_SIZE_ERROR => {
                    tracing::error!(conn_id = %self.conn_id, session = %self.session, code = m.code, "peer reported a connection error: {}", m.message);
                }
                _ => {
                    tracing::warn!(conn_id = %self.conn_id, session = %self.session, code = m.code, "peer reported a connection error: {}", m.message);
                }
            }
        }
    }

    /// The peer's close frame, unless this side already ended the connection
    /// (then it is the echo of our own close and carries nothing of the peer's).
    pub(crate) fn peer_closed(&self, frame: Option<&CloseFrame>) {
        let reason = peer_close_reason(Some(self.conn_id), Phase::Connected, frame);
        let msg = reason.message.clone();
        if self.end(reason, None) {
            tracing::info!(conn_id = %self.conn_id, session = %self.session, "{msg}");
        }
    }

    /// The transport died without a close frame.
    pub(crate) fn abnormal(
        &self,
        message: String,
        cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
    ) {
        let reason = abnormal_reason(Some(self.conn_id), Phase::Connected, message, cause);
        let msg = reason.message.clone();
        if self.end(reason, None) {
            tracing::info!(conn_id = %self.conn_id, session = %self.session, "{msg}");
        }
    }

    /// `close_with`: `error{APPLICATION_CLOSE}` then close 4014.
    pub(crate) fn close_application(&self, message: &str) {
        let code = ErrorCode::APPLICATION_CLOSE;
        let close = CloseRequest {
            error: Some(error_frame(code, message)),
            code: code.ws_close_wire(),
            reason: close_reason(message).to_owned(),
        };
        let reason = local_error_reason(
            Some(self.conn_id),
            Phase::Connected,
            code,
            format!("application close: {message}"),
        );
        self.end(reason, Some(close));
    }

    /// Ends the connection without any close handshake.
    #[cfg(test)]
    pub(crate) fn abandon(&self) {
        self.end(
            base_reason(
                Some(self.conn_id),
                Phase::Connected,
                "connection abandoned locally".into(),
            ),
            None,
        );
        self.cancel.cancel();
    }

    /// Queues `drain{client_requested}`. False when the connection has ended.
    pub(crate) fn queue_client_drain(&self, deadline: Duration) -> bool {
        let mut cs = self.lock();
        if cs.terminal.is_some() {
            return false;
        }
        let last = cs
            .local_drain
            .map_or(cs.highest_opened, |a| a.min(cs.highest_opened));
        cs.local_drain = Some(last);
        let drain = client_drain(last, deadline);
        cs.control
            .push_back(frame::encode_control(&drain).expect("a drain message is far below 16 KiB"));
        drop(cs);
        self.writer_wake.notify_one();
        true
    }

    /// The graceful close sequence (WIRE.md §2.10 rule 14).
    pub(crate) async fn close_graceful(&self, wait: Duration) {
        {
            let mut cs = self.lock();
            if cs.terminal.is_some() || cs.closing {
                return;
            }
            cs.closing = true;
        }
        self.queue_client_drain(wait);
        let deadline = Instant::now() + wait;
        loop {
            let changed = self.table_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let cs = self.lock();
                if cs.terminal.is_some() {
                    return;
                }
                if cs.streams.is_empty() {
                    break;
                }
            }
            tokio::select! {
                _ = &mut changed => {}
                _ = tokio::time::sleep_until(deadline) => break,
                _ = self.cancel.cancelled() => return,
            }
        }
        {
            let mut cs = self.lock();
            let survivors: Vec<_> = cs.streams.values().cloned().collect();
            for st in survivors {
                self.reset_stream_locked(&mut cs, &st, ErrorCode::CANCEL, "client closing");
            }
        }
        self.close_normal("client closed the connection");
    }

    /// `error{NO_ERROR, message}` then WebSocket close 1000. Open streams
    /// are failed like on any other connection end.
    pub(crate) fn close_normal(&self, message: &str) {
        let close = CloseRequest {
            error: Some(error_frame(ErrorCode::NO_ERROR, message)),
            code: 1000,
            reason: close_reason(message).to_owned(),
        };
        let mut reason = base_reason(Some(self.conn_id), Phase::Connected, message.into());
        reason.ws_code = Some(1000);
        self.end(reason, Some(close));
    }

    /// Picks the writer's next frame: urgent, then control, then the close
    /// sequence, then one DATA chunk from the next ready stream (WIRE.md §2.6
    /// rule 3, Go sched.go).
    pub(crate) fn next_write(&self) -> WriteJob {
        let mut cs = self.lock();
        if cs.terminal.is_some() && cs.close_req.is_none() {
            return WriteJob::Exit;
        }
        if let Some(f) = cs.urgent.pop_front() {
            return WriteJob::Frame(f);
        }
        if let Some(f) = cs.control.pop_front() {
            return WriteJob::Frame(f);
        }
        if let Some(close) = cs.close_req.take() {
            return WriteJob::Close(close);
        }
        while let Some(id) = cs.ready.pop_front() {
            let Some(st) = cs.streams.get(&id).cloned() else {
                cs.in_ready.remove(&id);
                continue;
            };
            let mut s = lock(&st.state);
            let Some(chunk) = s.out.pop_front() else {
                cs.in_ready.remove(&id);
                continue;
            };
            if s.out.is_empty() {
                cs.in_ready.remove(&id);
            } else {
                cs.ready.push_back(id);
            }
            s.wake_write();
            return WriteJob::Frame(chunk);
        }
        WriteJob::Idle
    }
}

/// Held by each connection task: if the task panics, the connection ends and
/// every task is cancelled, so nothing waits on a task that is gone.
pub(crate) struct PanicGuard(pub(crate) Arc<ConnShared>);

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.abnormal("connection task panicked".into(), None);
            self.0.cancel.cancel();
        }
    }
}

fn error_frame(code: ErrorCode, message: &str) -> Bytes {
    frame::encode_control(&ErrorMsg::new(code, message))
        .expect("error messages are capped at 1024 bytes")
}

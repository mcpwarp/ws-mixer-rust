//! Reader task: frame decoding and synchronous dispatch of every inbound
//! message (WIRE.md §2.2-§2.7). Never awaits application code: events go to
//! an unbounded channel guarded by a per-connection backlog counter.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Stream as FutStream, StreamExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::DrainSignal;
use super::shared::{ConnShared, PanicGuard, STREAM0_BURST, STREAM0_RATE, lock};
use crate::control::{ControlMsg, Drain, ErrorMsg, Ping, Pong, parse_control};
use crate::error::ErrorCode;
use crate::event::{AppMessage, DrainNotice, Envelope, Event, EventTx};
use crate::frame::{self, Decoded, Frame, FrameError};
use crate::stats::Counters;
use crate::stream::{Stream, StreamPhase, StreamShared};

/// Refused OPENs (STREAM_LIMIT) within [`REFUSED_OPEN_WINDOW`] that escalate
/// to `ENHANCE_YOUR_CALM` (WIRE.md §2.8 "repeat offence"; Go conn.go defaults).
const REFUSED_OPEN_LIMIT: u32 = 20;
const REFUSED_OPEN_WINDOW: Duration = Duration::from_secs(10);
/// Pongs queued behind a stalled socket at which the connection fails with
/// `ENHANCE_YOUR_CALM`.
const URGENT_LIMIT: usize = 1000;

pub(crate) async fn run<S>(
    mut stream: S,
    shared: Arc<ConnShared>,
    events: EventTx,
    backlog_limit: usize,
    allow_subfloor_timing: bool,
    drains: Option<UnboundedSender<DrainSignal>>,
) where
    S: FutStream<Item = Result<Message, WsError>> + Unpin,
{
    let _guard = PanicGuard(shared.clone());
    let mut d = Dispatcher {
        shared: shared.clone(),
        events,
        backlog: Arc::new(AtomicUsize::new(0)),
        backlog_limit,
        allow_subfloor_timing,
        drains,
        refused: 0,
        refused_since: None,
        stream0_seen: 0,
    };
    loop {
        let next = tokio::select! {
            biased;
            _ = shared.cancel.cancelled() => break,
            m = stream.next() => m,
        };
        match next {
            None => {
                shared.abnormal("connection closed without a close frame".into(), None);
                break;
            }
            Some(Err(WsError::Capacity(e))) => {
                shared.fail(
                    ErrorCode::FRAME_SIZE_ERROR,
                    format!("{e}: the limit is {} bytes", frame::MAX_MESSAGE),
                    true,
                );
                break;
            }
            Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => {
                shared.abnormal("connection closed".into(), None);
                break;
            }
            Some(Err(e)) => {
                let message = format!("connection lost: {e}");
                shared.abnormal(message, Some(Arc::new(e)));
                break;
            }
            Some(Ok(Message::Close(frame))) => shared.peer_closed(frame.as_ref()),
            Some(Ok(msg)) => {
                if shared.terminal().is_none() {
                    d.message(msg);
                }
            }
        }
    }
    shared.reader_done.cancel();
    shared.writer_wake.notify_one();
}

/// What a stream-frame handler decided while holding the locks.
enum Verdict {
    Ok,
    Fail(ErrorCode, String),
    Reset(Arc<StreamShared>, ErrorCode, String),
    Stale(&'static str),
}

struct Dispatcher {
    shared: Arc<ConnShared>,
    events: EventTx,
    backlog: Arc<AtomicUsize>,
    backlog_limit: usize,
    allow_subfloor_timing: bool,
    drains: Option<UnboundedSender<DrainSignal>>,
    refused: u32,
    refused_since: Option<Instant>,
    /// Stream-0 messages received after `welcome`.
    stream0_seen: u64,
}

impl Dispatcher {
    fn fail(&self, code: ErrorCode, message: String) {
        self.shared.fail(code, message, true);
    }

    fn message(&mut self, msg: Message) {
        match msg {
            Message::Binary(b) => {
                Counters::add(&self.shared.stats.bytes_in, b.len());
                self.binary(b);
            }
            Message::Text(t) => self.fail(
                ErrorCode::PROTOCOL_ERROR,
                format!(
                    "text message of {} bytes: ws-mixer.v1 uses binary messages only",
                    t.len()
                ),
            ),
            // WebSocket-level ping/pong are answered by tungstenite; raw
            // frames are never produced by a read.
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) | Message::Close(_) => {}
        }
    }

    fn binary(&mut self, b: Bytes) {
        let frame = match frame::decode(b) {
            Ok(Decoded::Frame(f)) => f,
            Ok(Decoded::Unknown {
                frame_type,
                stream_id,
                payload_len,
            }) => {
                Counters::inc(&self.shared.stats.unknown_frames);
                tracing::debug!(
                    conn_id = %self.shared.conn_id,
                    "ignoring frame of unknown type {frame_type:#04x} on stream {stream_id} ({payload_len} bytes)"
                );
                return;
            }
            Err(FrameError::Connection { code, message }) => return self.fail(code, message),
            Err(FrameError::Stream {
                stream_id,
                code,
                message,
            }) => {
                // The id checks of WIRE.md §2.5 come first: only a live
                // stream gets the RESET.
                let v = self.with_live("WINDOW", stream_id, |_, st, _| {
                    Verdict::Reset(st.clone(), code, message)
                });
                return self.apply(v, stream_id);
            }
        };
        let id = frame.stream_id();
        let v = match frame {
            Frame::Data { stream_id: 0, payload } => return self.control(&payload),
            Frame::Open { stream_id } => return self.open(stream_id),
            Frame::Data { payload, .. } => self.with_live("DATA", id, |_, st, s| {
                let n = payload.len();
                match s.phase {
                    StreamPhase::HalfClosedRemote => Verdict::Reset(
                        st.clone(),
                        ErrorCode::STREAM_CLOSED,
                        format!("DATA of {n} bytes on stream {id} after its CLOSE"),
                    ),
                    _ if n as i64 > s.recv_window => Verdict::Fail(
                        ErrorCode::FLOW_CONTROL_ERROR,
                        format!(
                            "stream {id}: received {n} DATA bytes with {} credit remaining",
                            s.recv_window
                        ),
                    ),
                    _ => {
                        s.recv_window -= n as i64;
                        if n > 0 {
                            s.inbuf.push_back(payload);
                            s.wake_read();
                        }
                        Verdict::Ok
                    }
                }
            }),
            Frame::Window { increment, .. } => self.with_live("WINDOW", id, |_, _, s| {
                // Applied in every live state, half-closed(remote) included:
                // a response larger than one window would stall otherwise
                // (design decision, Go stream.go:470-486).
                let window = s.send_window + i64::from(increment);
                if window > i64::from(i32::MAX) {
                    return Verdict::Fail(
                        ErrorCode::FLOW_CONTROL_ERROR,
                        format!(
                            "stream {id}: WINDOW of {increment} would push the send window to {window}, past 2^31-1"
                        ),
                    );
                }
                s.send_window = window;
                s.wake_write();
                Verdict::Ok
            }),
            Frame::Close { trailing, .. } => {
                if trailing > 0 {
                    Counters::inc(&self.shared.stats.close_frames_with_payload);
                }
                let shared = self.shared.clone();
                self.with_live("CLOSE", id, |cs, st, s| match s.phase {
                    StreamPhase::HalfClosedRemote | StreamPhase::Closed => Verdict::Reset(
                        st.clone(),
                        ErrorCode::STREAM_CLOSED,
                        format!("duplicate CLOSE on stream {id}"),
                    ),
                    StreamPhase::Open => {
                        s.phase = StreamPhase::HalfClosedRemote;
                        s.remote_closed = true;
                        s.wake_read();
                        Verdict::Ok
                    }
                    StreamPhase::HalfClosedLocal => {
                        s.phase = StreamPhase::Closed;
                        s.remote_closed = true;
                        s.wake_all();
                        shared.retire_locked(cs, id);
                        Verdict::Ok
                    }
                })
            }
            Frame::Reset { code, message, .. } => {
                let shared = self.shared.clone();
                self.with_live("RESET", id, |cs, _, s| {
                    // Never answered with a RESET (WIRE.md §2.5 note 2).
                    s.phase = StreamPhase::Closed;
                    s.reset = Some((code, message));
                    s.inbuf.clear();
                    s.out.clear();
                    s.wake_all();
                    shared.retire_locked(cs, id);
                    Verdict::Ok
                })
            }
        };
        self.apply(v, id);
    }

    /// Classifies `id` per the WIRE.md §2.5 receive table and, for a live
    /// stream, runs `f` with both locks held (connection, then stream).
    fn with_live<F>(&self, kind: &'static str, id: u32, f: F) -> Verdict
    where
        F: FnOnce(
            &mut super::shared::ConnState,
            &Arc<StreamShared>,
            &mut crate::stream::StreamState,
        ) -> Verdict,
    {
        let mut cs = self.shared.lock();
        if id > cs.highest_opened {
            return Verdict::Fail(
                ErrorCode::PROTOCOL_ERROR,
                format!(
                    "{kind} for stream {id}, which was never opened (highest_opened {})",
                    cs.highest_opened
                ),
            );
        }
        let Some(st) = cs.streams.get(&id).cloned() else {
            return Verdict::Stale(kind);
        };
        let mut s = lock(&st.state);
        f(&mut cs, &st, &mut s)
    }

    fn apply(&self, v: Verdict, id: u32) {
        match v {
            Verdict::Ok => {}
            Verdict::Fail(code, message) => self.fail(code, message),
            Verdict::Reset(st, code, message) => {
                let mut cs = self.shared.lock();
                self.shared
                    .reset_stream_locked(&mut cs, &st, code, &message);
            }
            Verdict::Stale(kind) => {
                Counters::inc(&self.shared.stats.stale_frames);
                tracing::debug!(conn_id = %self.shared.conn_id, "discarding {kind} for stream {id}, which is no longer live");
            }
        }
    }

    fn open(&mut self, id: u32) {
        let mut cs = self.shared.lock();
        if id <= cs.highest_opened {
            let h = cs.highest_opened;
            drop(cs);
            return self.fail(
                ErrorCode::PROTOCOL_ERROR,
                format!("OPEN for stream {id} is not above highest_opened {h}"),
            );
        }
        if let Some(last) = cs.peer_drained {
            if id > last {
                drop(cs);
                return self.fail(
                    ErrorCode::PROTOCOL_ERROR,
                    format!("OPEN for stream {id} exceeds drain last_stream_id {last}"),
                );
            }
        }
        // A refused OPEN still advances highest_opened (Go dispatch.go:134),
        // so later frames for it are stale, not never-opened.
        cs.highest_opened = id;
        let refusal = if cs.closing {
            Some((
                ErrorCode::REFUSED_STREAM,
                format!("stream {id} refused: the client is closing"),
            ))
        } else if let Some(last) = cs.local_drain.filter(|&last| id > last) {
            Some((
                ErrorCode::REFUSED_STREAM,
                format!("stream {id} refused: above our drain last_stream_id {last}"),
            ))
        } else if cs.streams.len() >= self.shared.max_streams as usize {
            Some((
                ErrorCode::STREAM_LIMIT,
                format!(
                    "max_streams={} exceeded by stream {id}",
                    self.shared.max_streams
                ),
            ))
        } else {
            None
        };
        if let Some((code, message)) = refusal {
            cs.control
                .push_back(frame::encode_reset(id, code, &message));
            drop(cs);
            self.shared.writer_wake.notify_one();
            Counters::inc(&self.shared.stats.refused_opens);
            if code == ErrorCode::STREAM_LIMIT && self.repeat_refusal() {
                self.fail(
                    ErrorCode::ENHANCE_YOUR_CALM,
                    format!(
                        "{REFUSED_OPEN_LIMIT} refused OPENs (STREAM_LIMIT) within {}s, the last for stream {id}",
                        REFUSED_OPEN_WINDOW.as_secs()
                    ),
                );
            }
            return;
        }
        let st = Arc::new(StreamShared::new(
            id,
            self.shared.our_window,
            self.shared.peer_window,
        ));
        cs.streams.insert(id, st.clone());
        drop(cs);
        Counters::inc(&self.shared.stats.streams_accepted);
        self.emit(Event::Stream(Stream::new(st, self.shared.clone())));
    }

    fn repeat_refusal(&mut self) -> bool {
        let now = Instant::now();
        match self.refused_since {
            Some(t) if now.duration_since(t) <= REFUSED_OPEN_WINDOW => {}
            _ => {
                self.refused_since = Some(now);
                self.refused = 0;
            }
        }
        self.refused += 1;
        self.refused >= REFUSED_OPEN_LIMIT
    }

    /// Hands an application event to the channel, failing the connection with
    /// `ENHANCE_YOUR_CALM` when the application has fallen too far behind.
    fn emit(&self, event: Event) {
        let pending = self.backlog.fetch_add(1, Ordering::AcqRel);
        if pending >= self.backlog_limit {
            self.backlog.fetch_sub(1, Ordering::AcqRel);
            let trigger = match &event {
                Event::Stream(s) => format!("OPEN for stream {}", s.id()),
                Event::Drain(d) => {
                    format!("drain (last_stream_id {}) on stream 0", d.last_stream_id)
                }
                _ => "app message on stream 0".to_owned(),
            };
            self.fail(
                ErrorCode::ENHANCE_YOUR_CALM,
                format!(
                    "application event backlog full (>{} pending stream/app/drain events) at {trigger}",
                    self.backlog_limit
                ),
            );
            return;
        }
        let envelope = Envelope {
            event,
            backlog: Some(self.backlog.clone()),
        };
        if self.events.send(envelope).is_err() {
            self.backlog.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn control(&mut self, payload: &[u8]) {
        self.stream0_seen += 1;
        // Before parsing, like Go dispatch.go handleControlData: every
        // stream-0 message counts, ping and pong included.
        if !self.shared.lock().stream0.allow() {
            return self.fail(
                ErrorCode::ENHANCE_YOUR_CALM,
                format!(
                    "stream-0 message {} exceeds the rate limit of {STREAM0_RATE}/s (burst {STREAM0_BURST})",
                    self.stream0_seen
                ),
            );
        }
        let msg = match parse_control(payload, self.allow_subfloor_timing) {
            Ok(m) => m,
            Err(e) => return self.fail(e.code, e.message),
        };
        match msg {
            ControlMsg::Hello(_) => self.fail(
                ErrorCode::PROTOCOL_ERROR,
                format!(
                    "hello received from the server ({}-byte control message)",
                    payload.len()
                ),
            ),
            ControlMsg::Welcome(_) => self.fail(
                ErrorCode::PROTOCOL_ERROR,
                format!(
                    "second welcome after the handshake completed ({}-byte control message)",
                    payload.len()
                ),
            ),
            ControlMsg::Ping(p) => self.ping(&p),
            ControlMsg::Pong(p) => self.pong(&p),
            ControlMsg::Drain(d) => self.drain(d),
            ControlMsg::Error(e) => self.peer_error(&e),
            ControlMsg::App(a) => self.emit(Event::App(AppMessage {
                conn_id: self.shared.conn_id,
                body: a.body,
            })),
        }
    }

    fn ping(&self, p: &Ping) {
        let pong = frame::encode_control(&Pong::echo(p)).expect("a pong is far below 16 KiB");
        // Pongs jump control and DATA. The urgent queue holds only pongs, so
        // appending keeps a burst of them in ping order.
        let mut cs = self.shared.lock();
        if cs.urgent.len() >= URGENT_LIMIT {
            drop(cs);
            return self.fail(
                ErrorCode::ENHANCE_YOUR_CALM,
                format!(
                    "ping {} with {URGENT_LIMIT} pongs already queued: the peer is not reading",
                    p.id
                ),
            );
        }
        cs.urgent.push_back(pong);
        drop(cs);
        self.shared.writer_wake.notify_one();
    }

    fn pong(&self, p: &Pong) {
        let mut cs = self.shared.lock();
        if p.id >= cs.next_ping_id {
            let next = cs.next_ping_id;
            drop(cs);
            return self.fail(
                ErrorCode::PROTOCOL_ERROR,
                format!("pong for id {} was never sent (next ping id {next})", p.id),
            );
        }
        if cs.outstanding_pings.remove(&p.id).is_some() {
            cs.last_pong_at = Instant::now();
        } else {
            drop(cs);
            Counters::inc(&self.shared.stats.duplicate_pongs);
            tracing::debug!(conn_id = %self.shared.conn_id, "ignoring duplicate pong {}", p.id);
        }
    }

    fn drain(&self, d: Drain) {
        if d.unknown_reason() {
            Counters::inc(&self.shared.stats.unknown_drain_reasons);
        }
        {
            let mut cs = self.shared.lock();
            // last_stream_id never increases (WIRE.md §2.9): keep the smallest.
            cs.peer_drained = Some(
                cs.peer_drained
                    .map_or(d.last_stream_id, |l| l.min(d.last_stream_id)),
            );
        }
        tracing::info!(conn_id = %self.shared.conn_id, reason = %d.reason, last_stream_id = d.last_stream_id, "server drain");
        if let Some(drains) = &self.drains {
            let _ = drains.send(DrainSignal {
                conn_id: self.shared.conn_id,
                retry_after: d.retry_after_ms.map(Duration::from_millis),
            });
        }
        self.emit(Event::Drain(DrainNotice {
            conn_id: self.shared.conn_id,
            reason: d.drain_reason(),
            raw_reason: d.reason,
            last_stream_id: d.last_stream_id,
            deadline: d.deadline_ms.map(Duration::from_millis),
            retry_after: d.retry_after_ms.map(Duration::from_millis),
            message: d.message,
        }));
    }

    fn peer_error(&self, e: &ErrorMsg) {
        self.shared.peer_error(e);
    }
}

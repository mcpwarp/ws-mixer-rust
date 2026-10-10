//! One WebSocket connection: dial, handshake, and the reader, writer and
//! keepalive tasks that run it.
//!
//! [`Conn::dial`] (or [`Conn::establish`] on an already-upgraded socket)
//! returns a [`Handshaken`] connection that has received `welcome` but whose
//! tasks are not running yet, so the supervisor can emit `Connected` before
//! the reader produces any `Stream`/`App`/`Drain` event. [`Handshaken::start`]
//! spawns the tasks and returns the [`LiveConn`] handle.

pub(crate) mod dial;
pub(crate) mod handshake;
pub(crate) mod keepalive;
pub(crate) mod reader;
pub(crate) mod shared;
pub(crate) mod writer;

#[cfg(test)]
mod interop_tests;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use http::{HeaderName, HeaderValue};
use serde_json::{Map, Value};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::control::{AgentInfo, App, Drain, Welcome};
use crate::error::{CloseError, ErrorCode, SendAppError};
use crate::event::{ConnId, DisconnectReason, EventTx, Phase};
use crate::frame::{self, MAX_MESSAGE};
use crate::stats::Counters;
use crate::tls::TlsConfig;
use shared::ConnShared;

/// Bound on one socket write (WIRE.md §2.6 rule 2 gates the writer on the
/// socket; a peer that stops reading must not wedge it forever).
pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound on the writes still pending once the connection has ended, so a
/// close against a peer that stopped reading finishes in ~2 s.
pub(crate) const SHUTDOWN_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long to wait for the peer's close echo (WIRE.md §2.8: ~2 s).
pub(crate) const CLOSE_ECHO_WAIT: Duration = Duration::from_secs(2);
/// How long a graceful close waits for streams to finish (WIRE.md §2.10 rule 14).
pub(crate) const GRACEFUL_CLOSE_WAIT: Duration = Duration::from_secs(5);

/// Everything one dial needs. Built by the supervisor from the
/// `ClientBuilder`; the token is passed separately because it is fetched per
/// dial.
#[derive(Clone, Debug)]
pub(crate) struct ConnConfig {
    pub(crate) url: String,
    pub(crate) headers: Vec<(HeaderName, HeaderValue)>,
    pub(crate) tls: Option<TlsConfig>,
    pub(crate) connect_timeout: Duration,
    pub(crate) hello_timeout: Duration,
    pub(crate) window: u32,
    pub(crate) max_streams: u32,
    pub(crate) agent: AgentInfo,
    pub(crate) capabilities: Vec<String>,
    pub(crate) meta: Option<Map<String, Value>>,
    pub(crate) allow_subfloor_timing: bool,
    pub(crate) stats: Arc<Counters>,
    /// Where the reader reports a server `drain`, besides the event channel:
    /// the supervisor starts the parallel reconnect from it.
    pub(crate) drains: Option<mpsc::UnboundedSender<DrainSignal>>,
}

/// A server `drain` as the supervisor sees it.
#[derive(Clone, Debug)]
pub(crate) struct DrainSignal {
    pub(crate) conn_id: ConnId,
    pub(crate) retry_after: Option<Duration>,
}

impl ConnConfig {
    /// The WebSocket limits: one frame per message, at most 65 544 bytes
    /// (WIRE.md §2.4). tokio-tungstenite never negotiates compression.
    pub(crate) fn ws_config() -> WebSocketConfig {
        WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE))
            .max_frame_size(Some(MAX_MESSAGE))
    }

    /// The backlog of undelivered `Stream`/`App`/`Drain` events at which the
    /// connection fails with `ENHANCE_YOUR_CALM` (Go conn.go:331-337).
    pub(crate) fn backlog_limit(&self) -> usize {
        (self.max_streams as usize + 64).max(128)
    }
}

/// A failed dial or handshake.
#[derive(Clone, Debug)]
pub(crate) struct ConnFailure {
    pub(crate) reason: DisconnectReason,
    /// `Retry-After` of an HTTP 429 or 503 upgrade response.
    pub(crate) retry_after: Option<Duration>,
    /// Our own welcome timeout: the locally generated `4001` that WIRE.md
    /// §2.9 calls a wedged edge, retried with plain backoff rather than
    /// reported as an SDK bug.
    pub(crate) welcome_timeout: bool,
}

impl From<DisconnectReason> for ConnFailure {
    fn from(reason: DisconnectReason) -> Self {
        Self {
            reason,
            retry_after: None,
            welcome_timeout: false,
        }
    }
}

/// Produces the upgraded WebSocket for one dial: TCP, TLS and the HTTP
/// upgrade ([`network_upgrade`]), or an in-memory transport in tests.
pub(crate) type Upgrade = Arc<
    dyn Fn(
            ConnConfig,
            String,
            ConnId,
        ) -> Pin<
            Box<dyn Future<Output = Result<WebSocketStream<dial::BoxIo>, ConnFailure>> + Send>,
        > + Send
        + Sync,
>;

/// The real network dialer, [`dial::connect`].
pub(crate) fn network_upgrade() -> Upgrade {
    Arc::new(|cfg, token, conn_id| {
        Box::pin(async move { dial::connect(&cfg, &token, conn_id).await })
    })
}

/// Entry points for one connection attempt.
pub(crate) struct Conn;

impl Conn {
    /// Dials `cfg.url` (TCP, TLS, WebSocket upgrade) and runs the
    /// `hello`/`welcome` handshake. Counts a dial and, on success, a connect.
    #[cfg(test)]
    pub(crate) async fn dial(
        cfg: &ConnConfig,
        token: String,
        events: EventTx,
        conn_id: ConnId,
    ) -> Result<Handshaken<dial::BoxIo>, ConnFailure> {
        Self::dial_via(&network_upgrade(), cfg, token, events, conn_id).await
    }

    /// [`dial`](Self::dial) over `upgrade`.
    pub(crate) async fn dial_via(
        upgrade: &Upgrade,
        cfg: &ConnConfig,
        token: String,
        events: EventTx,
        conn_id: ConnId,
    ) -> Result<Handshaken<dial::BoxIo>, ConnFailure> {
        Counters::inc(&cfg.stats.dials);
        let ws = upgrade(cfg.clone(), token.clone(), conn_id).await?;
        Self::establish(ws, cfg, token, events, conn_id).await
    }

    /// Runs the handshake on an already-upgraded WebSocket.
    pub(crate) async fn establish<S>(
        mut ws: WebSocketStream<S>,
        cfg: &ConnConfig,
        token: String,
        events: EventTx,
        conn_id: ConnId,
    ) -> Result<Handshaken<S>, ConnFailure>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let welcome = handshake::run(&mut ws, cfg, token, conn_id).await?;
        Counters::inc(&cfg.stats.connects);
        Ok(Handshaken {
            ws,
            welcome,
            cfg: cfg.clone(),
            events,
            conn_id,
        })
    }
}

/// A connection that has received `welcome` and whose tasks have not started.
/// Dropping it drops the socket without a close handshake.
pub(crate) struct Handshaken<S> {
    ws: WebSocketStream<S>,
    welcome: Welcome,
    cfg: ConnConfig,
    events: EventTx,
    conn_id: ConnId,
}

impl<S> Handshaken<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(crate) fn welcome(&self) -> &Welcome {
        &self.welcome
    }

    pub(crate) fn conn_id(&self) -> ConnId {
        self.conn_id
    }

    /// Closes without starting the tasks: `error{code, message}` and the
    /// matching WebSocket close, bounded by [`SHUTDOWN_WRITE_TIMEOUT`].
    pub(crate) async fn close(mut self, code: ErrorCode, message: &str) {
        let _ = tokio::time::timeout(
            SHUTDOWN_WRITE_TIMEOUT,
            handshake::close_with_error(&mut self.ws, code, message),
        )
        .await;
    }

    /// Splits the socket and spawns the reader, writer and keepalive tasks.
    pub(crate) fn start(self) -> LiveConn {
        use futures_util::StreamExt;

        let w = &self.welcome;
        let shared = Arc::new(ConnShared::new(
            self.conn_id,
            w.session.clone(),
            self.cfg.window,
            w.window,
            w.max_streams.min(self.cfg.max_streams),
            self.cfg.stats.clone(),
        ));
        let (sink, stream) = self.ws.split();
        let reader = tokio::spawn(reader::run(
            stream,
            shared.clone(),
            self.events,
            self.cfg.backlog_limit(),
            self.cfg.allow_subfloor_timing,
            self.cfg.drains.clone(),
        ));
        let writer = tokio::spawn(writer::run(sink, shared.clone()));
        let keepalive = tokio::spawn(keepalive::run(
            shared.clone(),
            w.ping_interval,
            w.ping_timeout,
        ));
        let done_shared = shared.clone();
        let done = tokio::spawn(async move {
            // Each task ends the connection if it panics (shared::PanicGuard),
            // so the join always completes.
            let _ = tokio::join!(reader, writer, keepalive);
            done_shared.cancel.cancel();
            done_shared.terminal().map_or_else(
                || {
                    abnormal_reason(
                        Some(done_shared.conn_id),
                        Phase::Connected,
                        "connection ended without a recorded reason".into(),
                        None,
                    )
                },
                |r| (*r).clone(),
            )
        });
        LiveConn {
            handle: ConnHandle {
                shared,
                welcome: Arc::new(self.welcome),
            },
            done,
        }
    }
}

/// A running connection: a cloneable control [`ConnHandle`] plus the task
/// that resolves to the final [`DisconnectReason`] once the reader, writer and
/// keepalive tasks have all been joined.
pub(crate) struct LiveConn {
    handle: ConnHandle,
    done: JoinHandle<DisconnectReason>,
}

impl LiveConn {
    pub(crate) fn handle(&self) -> &ConnHandle {
        &self.handle
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (ConnHandle, JoinHandle<DisconnectReason>) {
        (self.handle, self.done)
    }

    /// Waits for the connection to end and returns why. Every stream of the
    /// connection has been failed by then.
    pub(crate) async fn finished(self) -> DisconnectReason {
        let conn_id = self.handle.conn_id();
        self.done.await.unwrap_or_else(|e| {
            abnormal_reason(
                Some(conn_id),
                Phase::Connected,
                format!("connection task failed: {e}"),
                None,
            )
        })
    }
}

impl std::ops::Deref for LiveConn {
    type Target = ConnHandle;

    fn deref(&self) -> &ConnHandle {
        &self.handle
    }
}

/// Commands for a running connection. Every method is non-blocking: the
/// outcome is observed through [`LiveConn::finished`].
#[derive(Clone)]
pub(crate) struct ConnHandle {
    shared: Arc<ConnShared>,
    welcome: Arc<Welcome>,
}

impl ConnHandle {
    pub(crate) fn conn_id(&self) -> ConnId {
        self.shared.conn_id
    }

    pub(crate) fn welcome(&self) -> &Welcome {
        &self.welcome
    }

    /// Whether the connection has ended or is closing.
    pub(crate) fn is_terminal(&self) -> bool {
        self.shared.terminal().is_some()
    }

    /// Queues an `app` message. `body` must be a JSON object and the encoded
    /// message must fit in 16 KiB.
    pub(crate) fn send_app(&self, body: Value) -> Result<(), SendAppError> {
        let Value::Object(body) = body else {
            return Err(SendAppError::NotAnObject);
        };
        let frame = frame::encode_control(&App { body })
            .map_err(|e| SendAppError::TooLarge { bytes: e.bytes })?;
        let mut cs = self.shared.lock();
        if cs.terminal.is_some() {
            return Err(SendAppError::NotConnected);
        }
        cs.control.push_back(frame);
        drop(cs);
        self.shared.writer_wake.notify_one();
        Ok(())
    }

    /// Queues `drain{client_requested}` with `deadline`.
    pub(crate) fn send_drain(&self, deadline: Duration) -> Result<(), CloseError> {
        if self.shared.queue_client_drain(deadline) {
            Ok(())
        } else {
            Err(CloseError::NotConnected)
        }
    }

    /// Starts a graceful close in the background: `drain{client_requested}`,
    /// wait up to 5 s for the stream table to empty, `RESET(CANCEL)` the
    /// survivors, then `error{NO_ERROR}` and WebSocket close 1000.
    pub(crate) fn close_graceful(&self) {
        let shared = self.shared.clone();
        tokio::spawn(async move { shared.close_graceful(GRACEFUL_CLOSE_WAIT).await });
    }

    /// Closes as an application close: `error{APPLICATION_CLOSE, message}`
    /// and WebSocket close 4014 with the reason truncated to 123 bytes.
    pub(crate) fn close_with(&self, message: &str) {
        self.shared.close_application(message);
    }

    /// Abandons the connection without a close handshake.
    #[cfg(test)]
    pub(crate) fn cancel(&self) {
        self.shared.abandon();
    }

    /// Closes with `error{NO_ERROR, message}` and WebSocket close 1000,
    /// without the graceful close's `drain` and wait. For a retired
    /// connection whose streams have all finished.
    pub(crate) fn close_normal(&self, message: &str) {
        self.shared.close_normal(message);
    }

    /// Whether the connection has no live streams.
    pub(crate) fn is_idle(&self) -> bool {
        self.shared.lock().streams.is_empty()
    }

    /// Resolves once the connection has no live streams, or has ended.
    pub(crate) async fn wait_idle(&self) {
        loop {
            let changed = self.shared.table_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let cs = self.shared.lock();
                if cs.terminal.is_some() || cs.streams.is_empty() {
                    return;
                }
            }
            tokio::select! {
                _ = &mut changed => {}
                _ = self.shared.cancel.cancelled() => return,
            }
        }
    }
}

// --- DisconnectReason builders ---------------------------------------------

pub(crate) fn base_reason(
    conn_id: Option<ConnId>,
    phase: Phase,
    message: String,
) -> DisconnectReason {
    DisconnectReason {
        conn_id,
        phase,
        ws_code: None,
        error_code: None,
        error_name: None,
        http_status: None,
        fatal: false,
        message,
        close_reason: None,
        cause: None,
        retry_in: None,
    }
}

fn with_code(mut r: DisconnectReason, code: ErrorCode) -> DisconnectReason {
    r.error_code = Some(code);
    r.error_name = Some(code.name());
    r
}

/// A ws-mixer error this side raised and is closing on (CLIENT-SDK source 3
/// or 4): `ws_code` is the semantic `4000 + code`.
pub(crate) fn local_error_reason(
    conn_id: Option<ConnId>,
    phase: Phase,
    code: ErrorCode,
    message: String,
) -> DisconnectReason {
    let mut r = with_code(base_reason(conn_id, phase, message), code);
    r.ws_code = Some(code.ws_close_semantic());
    r.fatal = code == ErrorCode::UNSUPPORTED;
    r
}

/// The peer's `error` message (CLIENT-SDK source 1): its code, unclamped.
pub(crate) fn peer_error_reason(
    conn_id: Option<ConnId>,
    phase: Phase,
    code: ErrorCode,
    message: &str,
) -> DisconnectReason {
    let mut r = with_code(
        base_reason(
            conn_id,
            phase,
            format!("peer error {} ({}): {message}", code.name(), code.0),
        ),
        code,
    );
    r.ws_code = Some(code.ws_close_semantic());
    r
}

/// A close frame from the peer with no preceding `error` (CLIENT-SDK source
/// 2): `ws_code` as on the wire, `error_code` derived for 4001..=4999.
pub(crate) fn peer_close_reason(
    conn_id: Option<ConnId>,
    phase: Phase,
    frame: Option<&CloseFrame>,
) -> DisconnectReason {
    let Some(frame) = frame else {
        let mut r = base_reason(conn_id, phase, "peer closed without a status code".into());
        r.ws_code = Some(1005);
        return r;
    };
    let code = u16::from(frame.code);
    let reason = frame.reason.as_str();
    let message = if reason.is_empty() {
        format!("peer closed with code {code}")
    } else {
        format!("peer closed with code {code}: {reason}")
    };
    let mut r = base_reason(conn_id, phase, message);
    if (4001..=4999).contains(&code) {
        r = with_code(r, ErrorCode(u32::from(code) - 4000));
    }
    r.ws_code = Some(u32::from(code));
    r.close_reason = (!reason.is_empty()).then(|| reason.to_owned());
    r
}

/// Transport death with no close frame: no `ws_code`, no `error_code`.
pub(crate) fn abnormal_reason(
    conn_id: Option<ConnId>,
    phase: Phase,
    message: String,
    cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
) -> DisconnectReason {
    let mut r = base_reason(conn_id, phase, message);
    r.cause = cause;
    r
}

/// `drain{client_requested}` as sent by the client.
pub(crate) fn client_drain(last_stream_id: u32, deadline: Duration) -> Drain {
    Drain {
        reason: "client_requested".into(),
        last_stream_id,
        deadline_ms: Some(
            u64::try_from(deadline.as_millis())
                .unwrap_or(u64::MAX)
                .min(i64::MAX as u64),
        ),
        retry_after_ms: None,
        message: None,
    }
}

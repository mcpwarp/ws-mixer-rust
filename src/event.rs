//! Events delivered to the application.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::control::Welcome;
use crate::error::ErrorCode;
use crate::stream::Stream;

/// Identifies one WebSocket connection of a [`Client`](crate::Client).
///
/// Ids are assigned locally, start at 1 and increase with every dial; they
/// are unrelated to the server's `session`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnId(pub(crate) u64);

impl ConnId {
    /// The numeric value.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Something the application should react to.
///
/// Ordering guarantees: `Connected` for a connection arrives before any of
/// its `Stream`, `App` or `Drain` events, and its `Disconnected` arrives after
/// all of them. A `Redial` arrives after the `Disconnected` or `Drain` that
/// caused it and before the `Connected` or `Disconnected` of the dial it
/// announces.
#[derive(Debug)]
#[non_exhaustive]
pub enum Event {
    /// A `welcome` was received, on the first connection or after a reconnect.
    Connected(Connected),
    /// The server opened a stream. Read it continuously: unread data holds
    /// back flow-control credit.
    Stream(Stream),
    /// The server sent an `app` message.
    App(AppMessage),
    /// The server announced a drain. Unless reconnect is disabled or the
    /// reason is vetoed with
    /// [`ClientBuilder::no_redial_on_drain`](crate::ClientBuilder::no_redial_on_drain),
    /// the client dials a replacement connection in parallel; existing
    /// streams keep running. After a vetoed reason the application is
    /// expected to call [`Client::close_immediate`](crate::Client::close_immediate).
    Drain(DrainNotice),
    /// A connection attempt failed or a connection ended. Emitted exactly once
    /// per attempt, with one exception: a token rejected before `welcome`
    /// (HTTP 401 or close 4011) that the provider gets to refresh is retried
    /// at once without a `Disconnected`, as in the Go and JS SDKs (only an
    /// [`Event::Redial`] with [`RedialCause::TokenRefresh`] announces it);
    /// only the outcome of the retry is reported. A dial cancelled by
    /// [`Client::close`](crate::Client::close) reports nothing. When `fatal`
    /// is set, no further reconnect happens.
    Disconnected(DisconnectReason),
    /// The client scheduled its next dial: a reconnect after a failure, a
    /// drain's replacement, or the silent token-refresh redial. Emitted after
    /// the `Disconnected` or `Drain` event it follows (the token refresh has
    /// none) and before that dial starts. A drain whose reason is vetoed with
    /// [`ClientBuilder::no_redial_on_drain`](crate::ClientBuilder::no_redial_on_drain)
    /// gets none, and neither does that connection's end: the application is
    /// expected to call [`Client::close_immediate`](crate::Client::close_immediate).
    Redial(RedialNotice),
}

/// Why the client scheduled a dial, in [`RedialNotice::cause`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RedialCause {
    /// A dial failed or a connection ended; the reconnect policy picked the
    /// delay.
    Failure,
    /// The server drained the connection; this dial is its replacement.
    Drain,
    /// A token rejected before `welcome` (HTTP 401 or close 4011) is
    /// refreshed and redialed at once. No `Disconnected` precedes it.
    TokenRefresh,
}

impl RedialCause {
    /// `"failure"`, `"drain"` or `"token-refresh"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failure => "failure",
            Self::Drain => "drain",
            Self::TokenRefresh => "token-refresh",
        }
    }
}

impl fmt::Display for RedialCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Payload of [`Event::Redial`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedialNotice {
    /// The connection whose failure or drain led to this dial: the failed
    /// dial's or ended connection's id for [`RedialCause::Failure`] and
    /// [`RedialCause::TokenRefresh`], the draining connection for
    /// [`RedialCause::Drain`]. `None` if no connection was created.
    pub conn_id: Option<ConnId>,
    /// For [`RedialCause::Failure`], the consecutive reconnect count since
    /// the last stable connection, this dial included (`1` for the first).
    /// A drain replacement is not governed by it and reports `0`; the token
    /// refresh, the one immediate redial, reports `1`.
    pub attempt: u32,
    /// The delay before the dial actually used, jitter included. Zero for an
    /// immediate redial.
    pub delay: Duration,
    /// Why the dial was scheduled.
    pub cause: RedialCause,
}

/// Payload of [`Event::Connected`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Connected {
    /// The local connection id.
    pub conn_id: ConnId,
    /// The server-assigned session id (`welcome.session`).
    pub session: String,
    /// The full `welcome` message.
    pub welcome: Welcome,
    /// `true` for every welcome after the first one on this client.
    pub reconnect: bool,
}

/// Payload of [`Event::App`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AppMessage {
    /// The connection the message arrived on.
    pub conn_id: ConnId,
    /// The opaque `app.body` object.
    pub body: Map<String, Value>,
}

/// The `drain.reason` values defined by the spec.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DrainReason {
    /// A server rollout.
    Rollout,
    /// The server is overloaded.
    Overload,
    /// The connection ran out of stream ids.
    IdExhausted,
    /// The connection was replaced by a newer one.
    Replaced,
    /// Maintenance. Unknown reasons are also reported as this.
    Maintenance,
    /// The peer's application asked for the drain.
    ClientRequested,
}

impl DrainReason {
    /// The wire value, e.g. `"id_exhausted"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rollout => "rollout",
            Self::Overload => "overload",
            Self::IdExhausted => "id_exhausted",
            Self::Replaced => "replaced",
            Self::Maintenance => "maintenance",
            Self::ClientRequested => "client_requested",
        }
    }

    /// Parses a wire value. Unknown values degrade to [`DrainReason::Maintenance`].
    pub(crate) fn from_wire(s: &str) -> Self {
        match s {
            "rollout" => Self::Rollout,
            "overload" => Self::Overload,
            "id_exhausted" => Self::IdExhausted,
            "replaced" => Self::Replaced,
            "client_requested" => Self::ClientRequested,
            _ => Self::Maintenance,
        }
    }
}

impl fmt::Display for DrainReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Payload of [`Event::Drain`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DrainNotice {
    /// The draining connection.
    pub conn_id: ConnId,
    /// The parsed reason.
    pub reason: DrainReason,
    /// The reason exactly as received.
    pub raw_reason: String,
    /// The highest stream id the server has opened or will open on this connection.
    pub last_stream_id: u32,
    /// Grace period for in-flight streams. `None` means immediately.
    pub deadline: Option<Duration>,
    /// Reconnect hint. `None` means reconnect immediately with jitter.
    pub retry_after: Option<Duration>,
    /// Human-readable text from the server.
    pub message: Option<String>,
}

/// Where in its lifecycle a connection was when it ended.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Before the WebSocket upgrade completed (token fetch, TCP, TLS, HTTP).
    Dial,
    /// After the `101` response but before `welcome`.
    Handshake,
    /// After `welcome`.
    Connected,
}

impl Phase {
    /// `"dial"`, `"handshake"` or `"connected"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dial => "dial",
            Self::Handshake => "handshake",
            Self::Connected => "connected",
        }
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a connection attempt failed or a connection ended.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
#[non_exhaustive]
pub struct DisconnectReason {
    /// The connection, if one was created before the failure.
    pub conn_id: Option<ConnId>,
    /// Lifecycle phase at the time of the failure.
    pub phase: Phase,
    /// The semantic WebSocket close code, if a close happened. May exceed
    /// `u16::MAX` for application error codes (see [`ErrorCode::ws_close_semantic`]).
    pub ws_code: Option<u32>,
    /// The ws-mixer error code, set only from a peer `error` message, a bare
    /// close in `4001..=4999`, a locally raised connection error, or
    /// [`Client::close_with`](crate::Client::close_with).
    pub error_code: Option<ErrorCode>,
    /// The spec name of `error_code`.
    pub error_name: Option<&'static str>,
    /// The HTTP status of a failed upgrade.
    pub http_status: Option<u16>,
    /// `true` when the client gave up because of an error. An
    /// application-initiated close ([`close`](crate::Client::close) /
    /// [`close_with`](crate::Client::close_with)) is non-fatal but still final.
    /// The terminal signal is [`Events::recv`] returning `None` or
    /// [`ClientState::Closed`](crate::ClientState::Closed); a scheduled redial
    /// is signalled by [`retry_in`](Self::retry_in).
    pub fatal: bool,
    /// Human-readable description.
    pub message: String,
    /// The peer's WebSocket close reason, verbatim. Never our own.
    pub close_reason: Option<String>,
    /// The underlying error, e.g. an I/O error or a [`TokenError`](crate::TokenError).
    #[source]
    pub cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
    /// When the next dial is scheduled, if any.
    pub retry_in: Option<Duration>,
}

impl DisconnectReason {
    /// `true` for closes that indicate an implementation bug on one side
    /// (`4001` PROTOCOL_ERROR, `4003` FLOW_CONTROL_ERROR, `4004` FRAME_SIZE_ERROR).
    pub fn is_sdk_bug(&self) -> bool {
        matches!(self.ws_code, Some(4001 | 4003 | 4004))
    }

    /// The leading machine-readable token of `close_reason`, e.g.
    /// `"CONNECTION_LIMIT"` from `"CONNECTION_LIMIT: over per-tenant cap"`.
    ///
    /// Informational only; it never affects reconnect behaviour.
    pub fn reason_token(&self) -> Option<&str> {
        let reason = self.close_reason.as_deref()?;
        let end = reason
            .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
            .unwrap_or(reason.len());
        let token = &reason[..end];
        let terminated = reason[end..].is_empty() || reason[end..].starts_with(':');
        (!token.is_empty() && terminated).then_some(token)
    }
}

/// The receiving side of a [`Client`](crate::Client)'s event channel.
///
/// This is the client's lifeline: hold it for as long as the connection
/// should live. Dropping it closes the client gracefully, exactly like
/// [`Client::close`](crate::Client::close), even while `Client` handles are
/// still around; so does dropping everything. Afterwards
/// [`Client::send_app`](crate::Client::send_app) fails with
/// [`SendAppError::Closed`](crate::SendAppError::Closed) and
/// [`Client::wait_connected`](crate::Client::wait_connected) returns the
/// close reason.
///
/// The channel is unbounded so that calling into the client from the event
/// loop can never deadlock. Application events (`Stream`, `App`, `Drain`) are
/// still bounded per connection: if the application stops consuming, the
/// connection is failed with `ENHANCE_YOUR_CALM` once the backlog exceeds
/// `max(128, max_streams + 64)`.
pub struct Events {
    rx: mpsc::UnboundedReceiver<Envelope>,
}

/// One event on the channel. `backlog` is the sending connection's count of
/// undelivered `Stream`/`App`/`Drain` events, decremented on receipt.
pub(crate) struct Envelope {
    pub(crate) event: Event,
    pub(crate) backlog: Option<Arc<AtomicUsize>>,
}

impl Envelope {
    fn open(self) -> Event {
        if let Some(backlog) = self.backlog {
            backlog.fetch_sub(1, Ordering::AcqRel);
        }
        self.event
    }
}

/// The sending side of the event channel.
pub(crate) type EventTx = mpsc::UnboundedSender<Envelope>;

impl Events {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<Envelope>) -> Self {
        Self { rx }
    }

    /// Receives the next event. Returns `None` once the client has finished
    /// for good (after the final `Disconnected`).
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await.map(Envelope::open)
    }
}

impl fmt::Debug for Events {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Events").finish_non_exhaustive()
    }
}

impl futures_util::Stream for Events {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.rx.poll_recv(cx).map(|e| e.map(Envelope::open))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reason(ws_code: Option<u32>, close_reason: Option<&str>) -> DisconnectReason {
        DisconnectReason {
            conn_id: Some(ConnId(1)),
            phase: Phase::Connected,
            ws_code,
            error_code: None,
            error_name: None,
            http_status: None,
            fatal: false,
            message: "m".into(),
            close_reason: close_reason.map(str::to_owned),
            cause: None,
            retry_in: None,
        }
    }

    #[test]
    fn sdk_bug() {
        for code in [4001, 4003, 4004] {
            assert!(reason(Some(code), None).is_sdk_bug());
        }
        for code in [1000, 4002, 4009, 4011] {
            assert!(!reason(Some(code), None).is_sdk_bug());
        }
        assert!(!reason(None, None).is_sdk_bug());
    }

    #[test]
    fn reason_token() {
        let t = |r: &str| reason(None, Some(r)).reason_token().map(str::to_owned);
        assert_eq!(
            t("CONNECTION_LIMIT: over cap").as_deref(),
            Some("CONNECTION_LIMIT")
        );
        assert_eq!(t("QUOTA_2").as_deref(), Some("QUOTA_2"));
        assert_eq!(t("over cap"), None);
        assert_eq!(t("BAD token"), None);
        assert_eq!(t(""), None);
        assert_eq!(reason(None, None).reason_token(), None);
    }

    #[test]
    fn drain_reason_wire() {
        for r in [
            DrainReason::Rollout,
            DrainReason::Overload,
            DrainReason::IdExhausted,
            DrainReason::Replaced,
            DrainReason::Maintenance,
            DrainReason::ClientRequested,
        ] {
            assert_eq!(DrainReason::from_wire(r.as_str()), r);
        }
        assert_eq!(DrainReason::from_wire("reboot"), DrainReason::Maintenance);
    }

    #[test]
    fn redial_cause_names() {
        assert_eq!(RedialCause::Failure.to_string(), "failure");
        assert_eq!(RedialCause::Drain.to_string(), "drain");
        assert_eq!(RedialCause::TokenRefresh.to_string(), "token-refresh");
    }

    #[test]
    fn phase_names() {
        assert_eq!(Phase::Dial.to_string(), "dial");
        assert_eq!(Phase::Handshake.to_string(), "handshake");
        assert_eq!(Phase::Connected.to_string(), "connected");
    }
}

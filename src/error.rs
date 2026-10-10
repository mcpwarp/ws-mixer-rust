//! Wire error codes (WIRE.md §2.8) and the crate's error types.

use std::fmt;
use std::io;
use std::sync::Arc;

use crate::event::DisconnectReason;

/// A value from the ws-mixer.v1 error code space (WIRE.md §2.8).
///
/// One 32-bit space is shared by stream `RESET` frames and connection-level
/// `error` messages. `0x0000_0000..=0x0000_0FFF` is assigned by the spec;
/// codes `>= 0x1000_0000` are free for the application layer and remain valid
/// for stream resets. This is an open newtype rather than an enum so that
/// unknown codes survive a round trip untouched.
#[derive(Copy, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct ErrorCode(pub u32);

impl ErrorCode {
    /// Graceful shutdown. WebSocket close 1000.
    pub const NO_ERROR: Self = Self(0x00);
    /// Protocol violation with no more specific code. WebSocket close 4001.
    pub const PROTOCOL_ERROR: Self = Self(0x01);
    /// Unexpected failure on the sender's side. WebSocket close 4002.
    pub const INTERNAL_ERROR: Self = Self(0x02);
    /// Credit exceeded, or a window above 2^31-1. WebSocket close 4003.
    pub const FLOW_CONTROL_ERROR: Self = Self(0x03);
    /// Frame too large, or a fixed-size payload has the wrong size. WebSocket close 4004.
    pub const FRAME_SIZE_ERROR: Self = Self(0x04);
    /// `DATA` or `CLOSE` after the peer's `CLOSE`. Stream-level only.
    pub const STREAM_CLOSED: Self = Self(0x05);
    /// Nothing was processed; safe to retry elsewhere. Stream-level only.
    pub const REFUSED_STREAM: Self = Self(0x06);
    /// No longer needed; work may have started. Stream-level only.
    pub const CANCEL: Self = Self(0x07);
    /// `max_streams` exceeded. Stream-level only.
    pub const STREAM_LIMIT: Self = Self(0x08);
    /// Excessive load; back off. WebSocket close 4009.
    pub const ENHANCE_YOUR_CALM: Self = Self(0x09);
    /// Version or capability mismatch; do not retry unchanged. WebSocket close 4010.
    pub const UNSUPPORTED: Self = Self(0x0a);
    /// Authentication failed or expired. WebSocket close 4011.
    pub const UNAUTHORIZED: Self = Self(0x0b);
    /// Drain complete; reconnect immediately. WebSocket close 4012.
    pub const GOING_AWAY: Self = Self(0x0c);
    /// No `pong` within the deadline. WebSocket close 4013.
    pub const KEEPALIVE_TIMEOUT: Self = Self(0x0d);
    /// The application above ws-mixer closed the connection for its own
    /// reason. ws-mixer itself never emits it. WebSocket close 4014.
    pub const APPLICATION_CLOSE: Self = Self(0x0e);

    /// The spec name of this code, e.g. `"FLOW_CONTROL_ERROR"`.
    ///
    /// Unknown codes return `"INTERNAL_ERROR"`: the spec requires unknown codes
    /// to be treated as `INTERNAL_ERROR` and never trigger special behaviour.
    pub fn name(self) -> &'static str {
        self.known_name().unwrap_or("INTERNAL_ERROR")
    }

    fn known_name(self) -> Option<&'static str> {
        Some(match self.0 {
            0x00 => "NO_ERROR",
            0x01 => "PROTOCOL_ERROR",
            0x02 => "INTERNAL_ERROR",
            0x03 => "FLOW_CONTROL_ERROR",
            0x04 => "FRAME_SIZE_ERROR",
            0x05 => "STREAM_CLOSED",
            0x06 => "REFUSED_STREAM",
            0x07 => "CANCEL",
            0x08 => "STREAM_LIMIT",
            0x09 => "ENHANCE_YOUR_CALM",
            0x0a => "UNSUPPORTED",
            0x0b => "UNAUTHORIZED",
            0x0c => "GOING_AWAY",
            0x0d => "KEEPALIVE_TIMEOUT",
            0x0e => "APPLICATION_CLOSE",
            _ => return None,
        })
    }

    /// The semantic WebSocket close code: `1000` for `NO_ERROR`, otherwise
    /// `4000 + code` (saturating). May exceed `u16::MAX` for application codes.
    pub fn ws_close_semantic(self) -> u32 {
        if self == Self::NO_ERROR {
            1000
        } else {
            self.0.saturating_add(4000)
        }
    }

    /// The close code actually sent on the wire.
    ///
    /// Equal to [`ws_close_semantic`](Self::ws_close_semantic) when that is a
    /// legal close code (1000 or 4000..=4999); otherwise clamped to `4002`
    /// (`INTERNAL_ERROR`), since a WebSocket close code outside that range
    /// would be rejected and leave the peer with a bare abnormal closure.
    pub fn ws_close_wire(self) -> u16 {
        match self.ws_close_semantic() {
            c @ (1000 | 4000..=4999) => c as u16,
            _ => 4002,
        }
    }
}

impl fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.known_name() {
            Some(name) => write!(f, "ErrorCode({:#04x} {name})", self.0),
            None => write!(f, "ErrorCode({:#x})", self.0),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.known_name() {
            Some(name) => f.write_str(name),
            None => write!(f, "{:#x}", self.0),
        }
    }
}

impl From<u32> for ErrorCode {
    fn from(code: u32) -> Self {
        Self(code)
    }
}

impl From<ErrorCode> for u32 {
    fn from(code: ErrorCode) -> Self {
        code.0
    }
}

/// Longest WebSocket close reason in bytes (WIRE.md §2.4, RFC 6455 §5.5).
pub(crate) const MAX_CLOSE_REASON: usize = 123;

/// Truncates `s` to at most `max` bytes without splitting a UTF-8 character.
pub(crate) fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// The WebSocket close reason for `message`: truncated to 123 bytes on a
/// character boundary (WIRE.md §2.8).
pub(crate) fn close_reason(message: &str) -> &str {
    truncate_utf8(message, MAX_CLOSE_REASON)
}

/// [`ClientBuilder::connect`](crate::ClientBuilder::connect) or
/// [`Client::set_hello_meta`](crate::Client::set_hello_meta) rejected the
/// configuration.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// The URL did not parse or its scheme is not `ws` / `wss`.
    #[error("invalid url {url:?}: {reason}")]
    InvalidUrl {
        /// The URL as given.
        url: String,
        /// Why it was rejected.
        reason: String,
    },
    /// No token was configured.
    #[error("a token is required")]
    MissingToken,
    /// `window` is outside `16384..=2^31-1`.
    #[error("window {0} out of range 16384..=2147483647")]
    WindowOutOfRange(u32),
    /// `max_streams` is outside `1..=100000`.
    #[error("max_streams {0} out of range 1..=100000")]
    MaxStreamsOutOfRange(u32),
    /// `hello.meta` must be a JSON object.
    #[error("hello meta must be a JSON object")]
    MetaNotObject,
    /// A capability does not match `^[a-z0-9_.-]{1,64}$`.
    #[error("invalid capability {0:?}: must match ^[a-z0-9_.-]{{1,64}}$")]
    InvalidCapability(String),
    /// `agent` sdk or version is empty or longer than 128 characters.
    #[error("invalid agent field {0:?}: must be 1..=128 characters")]
    InvalidAgent(String),
    /// The header is managed by the SDK and cannot be set (e.g. `Authorization`).
    #[error("header {0} is managed by the SDK")]
    ForbiddenHeader(http::HeaderName),
    /// A duration option is zero or otherwise unusable.
    #[error("invalid {name}: {reason}")]
    InvalidDuration {
        /// The option name.
        name: &'static str,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// The encoded `hello` (with `meta`, and the token if it is static) would
    /// exceed the 16384-byte control message limit.
    #[error("hello of {bytes} bytes exceeds the 16384-byte control message limit")]
    HelloTooLarge {
        /// Encoded size of the `hello` message.
        bytes: usize,
    },
    /// The URL is `wss://` but no TLS configuration is compiled in: set one
    /// with [`ClientBuilder::tls`](crate::ClientBuilder::tls).
    #[error("wss:// needs a TLS configuration and none is compiled in: set ClientBuilder::tls")]
    NoTlsConfig,
    /// The TLS configuration could not be built.
    #[cfg(feature = "__rustls")]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(
            feature = "rustls-platform-verifier",
            feature = "rustls-webpki-roots",
            feature = "ring",
            feature = "aws-lc-rs"
        )))
    )]
    #[error("tls configuration: {0}")]
    Tls(#[source] rustls::Error),
    /// `connect` was called outside a tokio runtime.
    #[error("connect must be called from within a tokio runtime")]
    NoRuntime,
}

/// [`Client::send_app`](crate::Client::send_app) failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SendAppError {
    /// There is no welcomed connection right now. Messages are never queued
    /// across a reconnect.
    #[error("not connected")]
    NotConnected,
    /// `body` is not a JSON object.
    #[error("app body must be a JSON object")]
    NotAnObject,
    /// The encoded control frame exceeds the 16 KiB stream-0 payload limit.
    #[error("app message of {bytes} bytes exceeds the 16384-byte control frame limit")]
    TooLarge {
        /// Encoded size of the `app` message.
        bytes: usize,
    },
    /// The client has been closed.
    #[error("client closed")]
    Closed,
}

/// The error carried inside the [`io::Error`] a [`Stream`](crate::Stream)
/// returns. Retrieve it with [`io::Error::get_ref`] and `downcast_ref`.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The stream was reset, by the peer or locally. Maps to
    /// [`io::ErrorKind::ConnectionReset`].
    #[error("stream reset: {code}: {message}")]
    Reset {
        /// The `RESET` code.
        code: ErrorCode,
        /// The `RESET` message (decoded lossily if not UTF-8).
        message: String,
    },
    /// The connection carrying the stream died. Maps to
    /// [`io::ErrorKind::ConnectionAborted`], so a dead tunnel never looks
    /// like a clean EOF.
    #[error("connection lost: {0}")]
    ConnectionLost(Arc<DisconnectReason>),
    /// The write half was already closed. Maps to [`io::ErrorKind::BrokenPipe`].
    #[error("write side closed")]
    WriteClosed,
}

impl StreamError {
    /// The [`io::ErrorKind`] this error is reported as.
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            Self::Reset { .. } => io::ErrorKind::ConnectionReset,
            Self::ConnectionLost(_) => io::ErrorKind::ConnectionAborted,
            Self::WriteClosed => io::ErrorKind::BrokenPipe,
        }
    }
}

impl From<StreamError> for io::Error {
    fn from(e: StreamError) -> Self {
        io::Error::new(e.kind(), e)
    }
}

/// [`Client::drain`](crate::Client::drain) failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CloseError {
    /// There is no live connection to drain.
    #[error("no live connection to drain")]
    NotConnected,
    /// The client has been closed.
    #[error("client closed")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(ErrorCode::NO_ERROR.name(), "NO_ERROR");
        assert_eq!(ErrorCode::FLOW_CONTROL_ERROR.name(), "FLOW_CONTROL_ERROR");
        assert_eq!(ErrorCode::APPLICATION_CLOSE.name(), "APPLICATION_CLOSE");
        assert_eq!(ErrorCode(0x0f).name(), "INTERNAL_ERROR");
        assert_eq!(ErrorCode(0x1000_0000).name(), "INTERNAL_ERROR");
    }

    #[test]
    fn close_codes() {
        assert_eq!(ErrorCode::NO_ERROR.ws_close_semantic(), 1000);
        assert_eq!(ErrorCode::NO_ERROR.ws_close_wire(), 1000);
        assert_eq!(ErrorCode::PROTOCOL_ERROR.ws_close_wire(), 4001);
        assert_eq!(ErrorCode::APPLICATION_CLOSE.ws_close_wire(), 4014);
        assert_eq!(ErrorCode(999).ws_close_wire(), 4999);
        assert_eq!(ErrorCode(1000).ws_close_semantic(), 5000);
        assert_eq!(ErrorCode(1000).ws_close_wire(), 4002);
        assert_eq!(
            ErrorCode(0x1000_0000).ws_close_semantic(),
            0x1000_0000 + 4000
        );
        assert_eq!(ErrorCode(0x1000_0000).ws_close_wire(), 4002);
        assert_eq!(ErrorCode(u32::MAX).ws_close_semantic(), u32::MAX);
        assert_eq!(ErrorCode(u32::MAX).ws_close_wire(), 4002);
    }

    #[test]
    fn formatting() {
        assert_eq!(ErrorCode::CANCEL.to_string(), "CANCEL");
        assert_eq!(ErrorCode(0x1000_0001).to_string(), "0x10000001");
        assert_eq!(format!("{:?}", ErrorCode::CANCEL), "ErrorCode(0x07 CANCEL)");
    }

    #[test]
    fn close_reason_truncation() {
        assert_eq!(close_reason("short"), "short");
        let exact = "a".repeat(123);
        assert_eq!(close_reason(&exact), exact);
        let long = "a".repeat(200);
        assert_eq!(close_reason(&long).len(), 123);

        // 'é' is 2 bytes: 122 ASCII + 'é' would end at byte 124, so it is dropped.
        let two = format!("{}é", "a".repeat(122));
        assert_eq!(close_reason(&two), "a".repeat(122));
        // 121 ASCII + 'é' ends exactly at byte 123 and is kept.
        let fits = format!("{}éz", "a".repeat(121));
        assert_eq!(close_reason(&fits), format!("{}é", "a".repeat(121)));
        // A 4-byte emoji straddling the boundary at every offset.
        for pad in 119..=123 {
            let s = format!("{}😀tail", "a".repeat(pad));
            let want = if pad == 119 {
                format!("{}😀", "a".repeat(pad))
            } else {
                "a".repeat(pad)
            };
            assert_eq!(close_reason(&s), want, "pad {pad}");
        }
        let cjk = "語".repeat(50);
        let got = close_reason(&cjk);
        assert_eq!(got.len(), 123);
        assert_eq!(got.chars().count(), 41);
        assert_eq!(truncate_utf8("😀", 3), "");
    }

    #[test]
    fn stream_error_kinds() {
        let reset = StreamError::Reset {
            code: ErrorCode::CANCEL,
            message: "bye 7".into(),
        };
        let io_err: io::Error = reset.into();
        assert_eq!(io_err.kind(), io::ErrorKind::ConnectionReset);
        let inner = io_err
            .get_ref()
            .and_then(|e| e.downcast_ref::<StreamError>());
        assert!(matches!(
            inner,
            Some(StreamError::Reset { code, .. }) if *code == ErrorCode::CANCEL
        ));
        assert_eq!(
            io::Error::from(StreamError::WriteClosed).kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

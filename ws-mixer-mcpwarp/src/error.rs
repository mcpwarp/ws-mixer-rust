//! [`McpwarpError`] (MCPWARP.md §5.4) and [`ConfigError`] (§5.2).

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::codes::{AppErrorCode, RegisterErrorCode};
use crate::names::SERVICE_NAME_MAX;

/// What an [`McpwarpError`] is about. [`as_str`](Self::as_str) gives the
/// spec's code string, the same one the JS SDK puts in `McpwarpError.code`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum McpwarpErrorCode {
    /// A `registered.errors[]` code (MCPWARP.md §2.5).
    Register(RegisterErrorCode),
    /// An `error` op code (MCPWARP.md §2.6).
    App(AppErrorCode),
    /// An inbound envelope failed to decode (MCPWARP.md §2.8). Non-fatal.
    MalformedEnvelope,
    /// Closed before the service was registered.
    Closed,
    /// The edge rejected the token: close 4011, or HTTP 401 at the upgrade.
    Unauthorized,
    /// Another connection took the service over before it registered.
    TakenOver,
    /// Any other fatal transport outcome, incl. reconnect attempts exhausted.
    DisconnectedFatal,
}

impl McpwarpErrorCode {
    /// The code string, e.g. `"QUOTA_EXCEEDED"` or `"MALFORMED_ENVELOPE"`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Register(c) => c.as_str(),
            Self::App(c) => c.as_str(),
            Self::MalformedEnvelope => "MALFORMED_ENVELOPE",
            Self::Closed => "CLOSED",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::TakenOver => "TAKEN_OVER",
            Self::DisconnectedFatal => "DISCONNECTED_FATAL",
        }
    }
}

impl fmt::Display for McpwarpErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An mcpwarp-layer failure: a register refusal, an `error` op, a malformed
/// envelope, or why `expose()` gave up (MCPWARP.md §5.4).
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
#[non_exhaustive]
pub struct McpwarpError {
    /// The code.
    pub code: McpwarpErrorCode,
    /// The server's message, or this SDK's description.
    pub message: String,
    /// `true` when it ended the exposure.
    pub fatal: bool,
    /// The service the error is about, when there is one.
    pub service_name: Option<String>,
    /// The underlying error, e.g. the transport's
    /// [`DisconnectReason`](ws_mixer::DisconnectReason).
    #[source]
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}

impl McpwarpError {
    /// A non-fatal error with no service name and no source.
    pub fn new(code: McpwarpErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            fatal: false,
            service_name: None,
            source: None,
        }
    }

    /// Sets [`fatal`](Self::fatal).
    #[must_use]
    pub fn with_fatal(mut self, fatal: bool) -> Self {
        self.fatal = fatal;
        self
    }

    /// Sets [`service_name`](Self::service_name).
    #[must_use]
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Sets [`source`](Self::source).
    #[must_use]
    pub fn with_source(mut self, source: Arc<dyn Error + Send + Sync>) -> Self {
        self.source = Some(source);
        self
    }

    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::new(McpwarpErrorCode::MalformedEnvelope, message)
    }
}

/// A configuration rejected before anything is dialed (MCPWARP.md §5.2).
///
/// The message is the spec's, verbatim, and never contains the offending
/// value (a name or token may be private).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

#[allow(
    dead_code,
    reason = "the rows only expose() and the tunnel check are used once they land"
)]
impl ConfigError {
    /// §5.2 row 1.
    pub(crate) fn handler_or_target() -> Self {
        Self("mcpwarp: pass exactly one of `handler` or `target`".into())
    }

    /// §5.2 row 2.
    pub(crate) fn target_timeout_without_target() -> Self {
        Self("mcpwarp: `targetTimeoutMs` applies only to `target`".into())
    }

    /// §5.2 row 3.
    pub(crate) fn target_url() -> Self {
        Self("mcpwarp: `target` is not a valid URL".into())
    }

    /// §5.2 row 3 for an `https://` target, which this SDK does not support
    /// (the spec leaves the message to the SDK, without the URL).
    pub(crate) fn target_https() -> Self {
        Self("mcpwarp: `target` must be an http:// URL; https:// is not supported".into())
    }

    /// §5.2 row 4. The one row whose message carries the value, as the spec
    /// writes it: a duration, never a secret.
    pub(crate) fn target_timeout_range(ms: f64) -> Self {
        Self(format!(
            "mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got {ms}"
        ))
    }

    /// §5.2 row 5.
    pub(crate) fn name() -> Self {
        Self(format!(
            "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, at most {SERVICE_NAME_MAX} characters"
        ))
    }

    /// §5.2 row 6.
    pub(crate) fn integration_missing() -> Self {
        Self("mcpwarp: `integration` (your integration's registration UUID) is required".into())
    }

    /// §5.2 row 7.
    pub(crate) fn integration_syntax() -> Self {
        Self("mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else".into())
    }

    /// §5.2 row 8.
    pub(crate) fn kind() -> Self {
        Self("mcpwarp: `kind` must be \"http\" or \"stdio\"".into())
    }

    /// §5.2 row 9.
    pub(crate) fn token() -> Self {
        Self("mcpwarp: `token` is required".into())
    }

    /// §5.2 row 10, for `agent.runtime` / `agent.os`.
    pub(crate) fn agent_field(field: &str) -> Self {
        Self(format!(
            "mcpwarp: `agent.{field}` must be 1-64 printable ASCII characters"
        ))
    }

    /// §5.2 row 11, in the JS SDK's wording: `reconnect.<key>` must be
    /// `<rule>`.
    pub(crate) fn reconnect(key: &str, rule: &str) -> Self {
        Self(format!("mcpwarp: `reconnect.{key}` must be {rule}"))
    }

    /// `Tunnel::dispose` with a zero timeout (MCPWARP.md §6.3: positive).
    pub(crate) fn dispose_timeout() -> Self {
        Self("mcpwarp: dispose() `timeout` must be positive".into())
    }

    /// `Tunnel::new` outside a tokio runtime.
    pub(crate) fn no_runtime() -> Self {
        Self("mcpwarp: Tunnel::new must be called within a tokio runtime".into())
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_strings() {
        let c = |c: McpwarpErrorCode| c.as_str().to_owned();
        assert_eq!(
            c(McpwarpErrorCode::Register(RegisterErrorCode::QuotaExceeded)),
            "QUOTA_EXCEEDED"
        );
        assert_eq!(
            c(McpwarpErrorCode::App(AppErrorCode::UnsupportedVersion)),
            "UNSUPPORTED_VERSION"
        );
        assert_eq!(c(McpwarpErrorCode::MalformedEnvelope), "MALFORMED_ENVELOPE");
        assert_eq!(c(McpwarpErrorCode::Closed), "CLOSED");
        assert_eq!(c(McpwarpErrorCode::Unauthorized), "UNAUTHORIZED");
        assert_eq!(c(McpwarpErrorCode::TakenOver), "TAKEN_OVER");
        assert_eq!(c(McpwarpErrorCode::DisconnectedFatal), "DISCONNECTED_FATAL");
    }

    #[test]
    fn error_shape() {
        let e = McpwarpError::new(McpwarpErrorCode::Closed, "bye");
        assert_eq!(e.to_string(), "bye");
        assert!(!e.fatal);
        assert!(e.service_name.is_none() && e.source.is_none());
        let e = e.with_fatal(true).with_service_name("anki");
        assert!(e.fatal);
        assert_eq!(e.service_name.as_deref(), Some("anki"));
    }

    #[test]
    fn config_messages_are_verbatim() {
        assert_eq!(
            ConfigError::handler_or_target().to_string(),
            "mcpwarp: pass exactly one of `handler` or `target`"
        );
        assert_eq!(
            ConfigError::target_timeout_without_target().to_string(),
            "mcpwarp: `targetTimeoutMs` applies only to `target`"
        );
        assert_eq!(
            ConfigError::target_url().to_string(),
            "mcpwarp: `target` is not a valid URL"
        );
        assert_eq!(
            ConfigError::target_https().to_string(),
            "mcpwarp: `target` must be an http:// URL; https:// is not supported"
        );
        assert_eq!(
            ConfigError::target_timeout_range(0.0).to_string(),
            "mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got 0"
        );
        assert_eq!(
            ConfigError::target_timeout_range(2_147_483_647.5).to_string(),
            "mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got 2147483647.5"
        );
        assert_eq!(
            ConfigError::name().to_string(),
            "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, at most 30 characters"
        );
        assert_eq!(
            ConfigError::integration_missing().to_string(),
            "mcpwarp: `integration` (your integration's registration UUID) is required"
        );
        assert_eq!(
            ConfigError::integration_syntax().to_string(),
            "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else"
        );
        assert_eq!(
            ConfigError::kind().to_string(),
            "mcpwarp: `kind` must be \"http\" or \"stdio\""
        );
        assert_eq!(
            ConfigError::token().to_string(),
            "mcpwarp: `token` is required"
        );
        assert_eq!(
            ConfigError::agent_field("runtime").to_string(),
            "mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters"
        );
        assert_eq!(
            ConfigError::agent_field("os").message(),
            "mcpwarp: `agent.os` must be 1-64 printable ASCII characters"
        );
        assert_eq!(
            ConfigError::reconnect("stableAfter", "a finite number >= 0").to_string(),
            "mcpwarp: `reconnect.stableAfter` must be a finite number >= 0"
        );
    }
}

//! The tunnel's status snapshot (MCPWARP.md §6.4-§6.6).

use std::fmt;

use serde::{Deserialize, Serialize};

/// A tunnel status snapshot (MCPWARP.md §6.4). Absent fields are left out of
/// the JSON, never `null`.
///
/// `code` and `last_error` are set together; `url` only when `online`. A host
/// may build its own code-less status, e.g. the §7 "off" pattern:
///
/// ```
/// use ws_mixer_mcpwarp::{TunnelState, TunnelStatus};
///
/// let status = TunnelStatus {
///     last_error: Some("Add an mcpwarp token in the settings to go online.".into()),
///     ..TunnelStatus::new(TunnelState::Off)
/// };
/// assert_eq!(
///     serde_json::to_string(&status).unwrap(),
///     r#"{"state":"off","lastError":"Add an mcpwarp token in the settings to go online."}"#,
/// );
/// ```
///
/// A status from a newer peer with a state or code this SDK does not know
/// fails to deserialize; read it as a `serde_json::Value` and pass its
/// strings to [`describe_status_raw`](crate::describe_status_raw).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TunnelStatus {
    /// The state.
    pub state: TunnelState,
    /// The public URL, while `online`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Why the tunnel is not online, when there is a reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<TunnelStatusCode>,
    /// Set by the tunnel with `code`, as one of the
    /// [`last_error`](crate::last_error) constants. A host may set its own
    /// text on a code-less status (the §7 "off" pattern).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl TunnelStatus {
    /// A status with only `state` set.
    pub const fn new(state: TunnelState) -> Self {
        Self {
            state,
            url: None,
            code: None,
            last_error: None,
        }
    }
}

impl Default for TunnelStatus {
    /// `{state: "off"}`, a tunnel that was never configured.
    fn default() -> Self {
        Self::new(TunnelState::Off)
    }
}

macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $ty:ident ($rename:literal) { $($(#[$vmeta:meta])* $variant:ident => $wire:literal,)* }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = $rename)]
        #[non_exhaustive]
        pub enum $ty {
            $($(#[$vmeta])* $variant,)*
        }

        impl $ty {
            /// Every value, in spec order.
            pub const ALL: &'static [Self] = &[$(Self::$variant,)*];

            /// The wire value.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)*
                }
            }

            /// Parses a wire value; `None` for anything else.
            pub fn from_wire(s: &str) -> Option<Self> {
                match s {
                    $($wire => Some(Self::$variant),)*
                    _ => None,
                }
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

wire_enum! {
    /// `TunnelStatus.state` (MCPWARP.md §6.5).
    TunnelState("lowercase") {
        /// Not configured, or stopped.
        Off => "off",
        /// Dialling or registering.
        Connecting => "connecting",
        /// Registered; `url` is set.
        Online => "online",
        /// Disabled in the dashboard.
        Disabled => "disabled",
        /// Another connection took over the name.
        Conflict => "conflict",
        /// A fatal problem; the tunnel waits for a new `configure`.
        Error => "error",
    }
}

wire_enum! {
    /// `TunnelStatus.code` (MCPWARP.md §6.5): 13 codes, value = name.
    TunnelStatusCode("SCREAMING_SNAKE_CASE") {
        /// The token was rejected or may not publish this service.
        Unauthorized => "UNAUTHORIZED",
        /// Another connection took over the name.
        TakenOver => "TAKEN_OVER",
        /// A live connection was lost; reconnecting.
        Disconnected => "DISCONNECTED",
        /// A dial or register failed; retrying.
        Retrying => "RETRYING",
        /// Disabled in the dashboard.
        Disabled => "DISABLED",
        /// The tunnel settings are invalid.
        InvalidConfig => "INVALID_CONFIG",
        /// The service name is invalid.
        InvalidName => "INVALID_NAME",
        /// The account has no username.
        UsernameRequired => "USERNAME_REQUIRED",
        /// The service quota is used up.
        QuotaExceeded => "QUOTA_EXCEEDED",
        /// The name is in use by another server.
        Conflict => "CONFLICT",
        /// This SDK version is no longer supported.
        UnsupportedVersion => "UNSUPPORTED_VERSION",
        /// A fatal transport outcome.
        DisconnectedFatal => "DISCONNECTED_FATAL",
        /// Anything else.
        Internal => "INTERNAL",
    }
}

/// The `lastError` constants, verbatim (MCPWARP.md §6.6). A `lastError` never
/// carries the token, the service name, the URL or a server message.
pub mod last_error {
    /// `RETRYING`: redial after a failed dial.
    pub const RETRY_DIAL: &str = "Can't reach mcpwarp; retrying.";
    /// `RETRYING`: transient register retry, `OVERLOADED` re-send.
    pub const RETRY_REGISTER: &str = "mcpwarp couldn't register the service; retrying.";
    /// `DISCONNECTED`: live connection lost.
    pub const DISCONNECTED: &str = "Connection lost; retrying.";
    /// `TAKEN_OVER`: take-over.
    pub const TAKEN_OVER: &str = "Another connection took over this name.";
    /// `DISABLED`: disabled.
    pub const DISABLED: &str = "This service is disabled in the mcpwarp dashboard.";
    /// `UNAUTHORIZED`: token rejected (`UNAUTHORIZED`, `UNAUTHENTICATED`).
    pub const TOKEN_REJECTED: &str = "The token was rejected.";
    /// `UNAUTHORIZED`: `FORBIDDEN`.
    pub const TOKEN_FORBIDDEN: &str = "The token is not allowed to publish this service.";
    /// `INVALID_NAME`: invalid name (local or `INVALID_NAME` refusal).
    pub const INVALID_NAME: &str = "The service name is invalid: use lowercase letters, digits and dashes (not at the start or end), at most 30 characters.";
    /// `INVALID_CONFIG`: invalid settings.
    pub const INVALID_CONFIG: &str = "The tunnel settings are invalid.";
    /// `INVALID_CONFIG`: `token` not set.
    pub const TOKEN_NOT_SET: &str = "The token is not set.";
    /// `INVALID_CONFIG`: `name` not set.
    pub const NAME_NOT_SET: &str = "The service name is not set.";
    /// `USERNAME_REQUIRED`.
    pub const USERNAME_REQUIRED: &str = "Choose a username in your mcpwarp account first.";
    /// `QUOTA_EXCEEDED`.
    pub const QUOTA_EXCEEDED: &str = "Your mcpwarp account has no services left in its quota.";
    /// `CONFLICT`.
    pub const CONFLICT: &str = "The name is in use by another server.";
    /// `UNSUPPORTED_VERSION`.
    pub const UNSUPPORTED_VERSION: &str = "This SDK version is no longer supported; please update.";
    /// `DISCONNECTED_FATAL`: fatal transport outcome.
    pub const DISCONNECTED_FATAL: &str = "The connection to mcpwarp failed.";
    /// `INTERNAL`: anything else.
    pub const INTERNAL: &str = "Something went wrong; the log has the details.";
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wire_strings_match_serde() {
        for s in TunnelState::ALL {
            assert_eq!(serde_json::to_value(s).unwrap(), json!(s.as_str()));
            assert_eq!(TunnelState::from_wire(s.as_str()), Some(*s));
            assert_eq!(s.to_string(), s.as_str());
        }
        assert_eq!(TunnelStatusCode::ALL.len(), 13);
        for c in TunnelStatusCode::ALL {
            assert_eq!(serde_json::to_value(c).unwrap(), json!(c.as_str()));
            assert_eq!(TunnelStatusCode::from_wire(c.as_str()), Some(*c));
            assert_eq!(c.to_string(), c.as_str());
        }
        assert_eq!(TunnelStatusCode::from_wire("SOMETHING_NEW"), None);
    }

    #[test]
    fn status_json_shape() {
        let online = TunnelStatus {
            url: Some("https://a.mcpwarp.test/mcp".into()),
            ..TunnelStatus::new(TunnelState::Online)
        };
        assert_eq!(
            serde_json::to_value(&online).unwrap(),
            json!({"state": "online", "url": "https://a.mcpwarp.test/mcp"})
        );
        let err = TunnelStatus {
            state: TunnelState::Error,
            url: None,
            code: Some(TunnelStatusCode::InvalidName),
            last_error: Some(last_error::INVALID_NAME.into()),
        };
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(
            v,
            json!({"state": "error", "code": "INVALID_NAME", "lastError": last_error::INVALID_NAME})
        );
        assert_eq!(serde_json::from_value::<TunnelStatus>(v).unwrap(), err);
        assert_eq!(
            serde_json::to_value(TunnelStatus::default()).unwrap(),
            json!({"state": "off"})
        );
    }

    #[test]
    fn invalid_name_constant_names_the_max() {
        let max = format!("at most {} characters.", crate::SERVICE_NAME_MAX);
        assert!(last_error::INVALID_NAME.ends_with(&max));
    }
}

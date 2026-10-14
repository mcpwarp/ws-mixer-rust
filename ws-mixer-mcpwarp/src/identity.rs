//! What an agent says about itself on every dial (MCPWARP.md §3.1, §8):
//! `hello.meta`, `hello.agent.runtime`/`os`, the `X-Mcpwarp-Integration` and
//! `User-Agent` headers.

use http::{HeaderName, HeaderValue};
use serde_json::{Value, json};

use crate::error::ConfigError;
use crate::names::is_valid_integration_id;

/// `hello.meta.client` for this SDK.
pub const CLIENT_ID: &str = "sdk-rust";

/// This crate's version, sent as `hello.meta.version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The upgrade header carrying the integration id, equal to
/// `hello.meta.integration`.
pub const INTEGRATION_HEADER: HeaderName = HeaderName::from_static("x-mcpwarp-integration");

/// Overrides for `hello.agent.runtime` / `hello.agent.os` (MCPWARP.md §5.1
/// `agent`). Each must be 1-64 printable ASCII characters; `None` keeps the
/// detected value (`"rust"`, and `"<os>/<arch>"` from [`std::env::consts`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Agent {
    /// The runtime, e.g. `"tauri/2.1"`.
    pub runtime: Option<String>,
    /// The operating system, e.g. `"windows/x86_64"`.
    pub os: Option<String>,
}

/// A validated identity: the integration id (lowercased) and the agent's
/// runtime and OS, ready to apply to a [`ws_mixer::ClientBuilder`].
///
/// ```
/// use ws_mixer_mcpwarp::{Agent, Identity};
///
/// let id = Identity::new(
///     "6F1C2A9E-3B4D-4E5F-8A7B-0C1D2E3F4A5B",
///     &Agent { runtime: Some("tauri/2.1".into()), os: Some("windows/x86_64".into()) },
/// )?;
/// assert_eq!(id.integration(), "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b");
/// assert_eq!(id.hello_meta()["client"], "sdk-rust");
/// assert_eq!(
///     id.user_agent(),
///     format!("ws-mixer-rust/{} (tauri/2.1 windows/x86_64)", ws_mixer::SDK_VERSION)
/// );
/// # Ok::<(), ws_mixer_mcpwarp::ConfigError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    integration: String,
    runtime: String,
    os: String,
}

impl Identity {
    /// Validates `integration` and `agent` (MCPWARP.md §5.2 rows 6, 7, 10, in
    /// that order) and resolves the agent's runtime and OS.
    pub fn new(integration: &str, agent: &Agent) -> Result<Self, ConfigError> {
        if integration.is_empty() {
            return Err(ConfigError::integration_missing());
        }
        if !is_valid_integration_id(integration) {
            return Err(ConfigError::integration_syntax());
        }
        for (field, value) in [("runtime", &agent.runtime), ("os", &agent.os)] {
            if value.as_deref().is_some_and(|v| !valid_agent_field(v)) {
                return Err(ConfigError::agent_field(field));
            }
        }
        let detected = ws_mixer::AgentInfo::default();
        let pick = |given: &Option<String>, detected: Option<String>, fallback: &str| {
            given
                .clone()
                .or(detected)
                .unwrap_or_else(|| fallback.to_owned())
        };
        Ok(Self {
            integration: integration.to_ascii_lowercase(),
            runtime: pick(&agent.runtime, detected.runtime, "rust"),
            os: pick(&agent.os, detected.os, "unknown"),
        })
    }

    /// The integration id, lowercased.
    pub fn integration(&self) -> &str {
        &self.integration
    }

    /// `hello.agent.runtime`.
    pub fn runtime(&self) -> &str {
        &self.runtime
    }

    /// `hello.agent.os`, also `hello.meta.os`.
    pub fn os(&self) -> &str {
        &self.os
    }

    /// `hello.meta`: `{client, version, os, integration}` (MCPWARP.md §3.1).
    pub fn hello_meta(&self) -> Value {
        json!({
            "client": CLIENT_ID,
            "version": VERSION,
            "os": self.os,
            "integration": self.integration,
        })
    }

    /// `User-Agent`: `<sdk>/<sdk_version> (<runtime> <os>)`, with the
    /// transport's `hello.agent.sdk` and `sdk_version` (MCPWARP.md §3.1).
    pub fn user_agent(&self) -> String {
        format!(
            "{}/{} ({} {})",
            ws_mixer::AgentInfo::default().sdk,
            ws_mixer::SDK_VERSION,
            self.runtime,
            self.os
        )
    }

    /// Sets `hello.meta`, `hello.agent.runtime`/`os`, and the
    /// `X-Mcpwarp-Integration` and `User-Agent` upgrade headers on `builder`.
    /// `hello.agent.sdk`/`sdk_version` stay the transport's.
    ///
    /// Also vetoes the replacement dial for a `drain` with reason `replaced`
    /// ([`ws_mixer::ClientBuilder::no_redial_on_drain`]): that drain is a
    /// take-over (MCPWARP.md §3.3), so no replacement may ever send `hello`
    /// or `register`. The driver then closes the client with
    /// [`ws_mixer::Client::close_immediate`] ([`Command::CloseImmediate`]
    /// from [`Session::on_drain`]).
    ///
    /// [`Command::CloseImmediate`]: crate::session::Command::CloseImmediate
    /// [`Session::on_drain`]: crate::session::Session::on_drain
    pub fn apply(&self, builder: ws_mixer::ClientBuilder) -> ws_mixer::ClientBuilder {
        let mut b = builder
            .hello_meta(self.hello_meta())
            .agent_runtime(self.runtime.clone())
            .agent_os(self.os.clone())
            .no_redial_on_drain(ws_mixer::DrainReason::Replaced);
        // Both are printable ASCII (validated or detected), so these only
        // fail for a detected value no platform produces.
        if let Ok(v) = HeaderValue::from_str(&self.integration) {
            b = b.header(INTEGRATION_HEADER, v);
        }
        if let Ok(v) = HeaderValue::from_str(&self.user_agent()) {
            b = b.header(http::header::USER_AGENT, v);
        }
        b
    }
}

/// `^[\x20-\x7e]{1,64}$`.
fn valid_agent_field(v: &str) -> bool {
    (1..=64).contains(&v.len()) && v.bytes().all(|c| (0x20..=0x7e).contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";

    #[test]
    fn validation_order_and_messages() {
        let none = Agent::default();
        let err = |i: &str, a: &Agent| Identity::new(i, a).unwrap_err().to_string();
        assert_eq!(
            err("", &none),
            "mcpwarp: `integration` (your integration's registration UUID) is required"
        );
        assert_eq!(
            err("secret-value", &none),
            "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else"
        );
        let bad_runtime = Agent {
            runtime: Some(String::new()),
            os: Some("é".into()),
        };
        assert_eq!(
            err(ID, &bad_runtime),
            "mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters"
        );
        // An invalid integration wins over an invalid agent.
        assert_eq!(
            err("x", &bad_runtime),
            "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else"
        );
        for os in ["é", "a\tb", &"x".repeat(65)] {
            let a = Agent {
                runtime: None,
                os: Some(os.to_owned()),
            };
            assert_eq!(
                err(ID, &a),
                "mcpwarp: `agent.os` must be 1-64 printable ASCII characters",
                "{os:?}"
            );
        }
        let edge = Agent {
            runtime: Some(" ".into()),
            os: Some("~".repeat(64)),
        };
        assert!(Identity::new(ID, &edge).is_ok());
    }

    #[test]
    fn meta_and_user_agent() {
        let id = Identity::new(&ID.to_uppercase(), &Agent::default()).unwrap();
        assert_eq!(id.integration(), ID);
        let os = format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
        assert_eq!(id.runtime(), "rust");
        assert_eq!(id.os(), os);
        assert_eq!(
            id.hello_meta().to_string(),
            format!(
                r#"{{"client":"sdk-rust","version":"{VERSION}","os":"{os}","integration":"{ID}"}}"#
            )
        );
        assert_eq!(
            id.user_agent(),
            format!("ws-mixer-rust/{} (rust {os})", ws_mixer::SDK_VERSION)
        );

        let id = Identity::new(
            ID,
            &Agent {
                runtime: Some("tauri/2.1".into()),
                os: None,
            },
        )
        .unwrap();
        assert_eq!((id.runtime(), id.os()), ("tauri/2.1", os.as_str()));
        let id = Identity::new(
            ID,
            &Agent {
                runtime: None,
                os: Some("ios/17".into()),
            },
        )
        .unwrap();
        assert_eq!(id.hello_meta()["os"], "ios/17");
        assert!(id.user_agent().ends_with("(rust ios/17)"));
    }
}

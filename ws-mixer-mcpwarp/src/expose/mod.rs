//! `expose()` (MCPWARP.md §5): one call, one ws-mixer client, one
//! registered service. Registration follows the connection (re-sent on
//! every `welcome`); every stream the edge opens is one HTTP request,
//! served on its own task.

mod agent;
mod config;
mod handle;
pub(crate) mod observe;
#[cfg(all(test, feature = "test-util"))]
mod tests;

use std::error::Error;
use std::sync::Arc;

use ws_mixer::{BuildError, Client, DisconnectReason};

pub use config::{ExposeConfig, ExposeConfigBuilder};
pub use handle::ExposeHandle;

use crate::error::{McpwarpError, McpwarpErrorCode};
use crate::session::{Conflict, Registration};
use crate::transport::{ClientConnector, Connector};

/// What an exposure reports (MCPWARP.md §5.6), from
/// [`ExposeHandle::events`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ExposeEvent {
    /// A successful `register`: after every `welcome`, and after `enable`.
    Registered(Registration),
    /// Disabled in the dashboard (`disable`, its reason) or refused as
    /// `SERVER_DISABLED` (`"disabled"`). Requests get `503`.
    Disabled {
        /// The server id, if known.
        server_id: Option<String>,
        /// Why.
        reason: String,
    },
    /// Re-enabled; `register` follows.
    Enabled {
        /// The server id.
        server_id: String,
    },
    /// Taken over by another connection (a `conflict` op, or a `drain` with
    /// reason `replaced`). Sticky: the client is closed at once, nothing
    /// reconnects, and the state stays [`Conflict`](crate::ExposeState::Conflict)
    /// until [`ExposeHandle::close`].
    Conflict(Conflict),
    /// A non-fatal error: a transient register error (retried), a refusal
    /// after the first registration, an `error` op, a malformed envelope.
    Error(Arc<McpwarpError>),
    /// Application code failed: a handler error or panic (answered `500`), a
    /// response body that failed mid-way, a target failure, or a panic in
    /// `on_retry`.
    HandlerError(Arc<dyn Error + Send + Sync>),
    /// A transport disconnect, every one. The handle's state already
    /// reflects it.
    Disconnect(DisconnectReason),
    /// The exposure ended; the last event, after which receivers get
    /// `Closed`. `error` is set after a fatal failure.
    Close {
        /// The fatal error, if any.
        error: Option<Arc<McpwarpError>>,
    },
}

/// Exposes a local MCP server at its public mcpwarp URL (MCPWARP.md §5).
///
/// Dials the edge, registers `config`'s name after every `welcome`, and
/// serves every request on `/mcp` with the configured handler or target.
/// Resolves once the service is registered, or refused as disabled (state
/// [`Disabled`](crate::ExposeState::Disabled), no URL). Dial failures and
/// transient register errors are retried without limit unless
/// `reconnect.max_attempts` is set; [`on_retry`](ExposeConfigBuilder::on_retry)
/// sees each retry.
///
/// # Errors
///
/// A fatal [`McpwarpError`] (§5.4): a non-transient register refusal
/// (its code), `UNSUPPORTED_VERSION`, `UNAUTHORIZED` (a rejected token),
/// `TAKEN_OVER` (another connection took the name first),
/// `DISCONNECTED_FATAL` (any other fatal transport outcome, including an
/// invalid URL and reconnect attempts exhausted).
///
/// # Cancellation
///
/// Dropping the future before it resolves is the spec's `signal` abort
/// (§5.8): the client is closed.
///
/// ```no_run
/// use bytes::Bytes;
/// use http_body_util::Full;
/// use ws_mixer_mcpwarp::{ExposeConfig, expose};
/// use ws_mixer_mcpwarp::bridge::handler_fn;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ExposeConfig::builder()
///     .token(std::env::var("MCPWARP_TOKEN")?)
///     .name("notes")
///     .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
///     .handler(handler_fn(|_req| async {
///         Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
///     }))
///     .build()?;
/// let handle = expose(config).await?;
/// println!("live at {}{}", handle.url().unwrap_or_default(), handle.path());
/// tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
/// handle.close().await;
/// # Ok(())
/// # }
/// ```
pub async fn expose(config: ExposeConfig) -> Result<ExposeHandle, McpwarpError> {
    expose_with(config, Box::new(ClientConnector)).await
}

/// [`expose`] over any transport.
pub(crate) async fn expose_with(
    config: ExposeConfig,
    connector: Box<dyn Connector>,
) -> Result<ExposeHandle, McpwarpError> {
    let mut builder = Client::builder(&config.url)
        .token(config.token.clone())
        .reconnect(config.reconnect.clone());
    if let Some(tls) = config.tls.clone() {
        builder = builder.tls(tls);
    }
    let builder = config.identity.apply(builder);
    let transport = connector
        .connect(builder)
        .map_err(|e| build_error(e, &config.name))?;
    let client = transport.client.clone();
    // Dropping this future drops `control`, the driver's lifeline before a
    // handle exists: the driver then closes the client.
    let (shared, control, settle) = agent::spawn(config, transport);
    match settle.await {
        Ok(Ok(())) => Ok(ExposeHandle::new(shared, control, client)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(McpwarpError::new(
            McpwarpErrorCode::DisconnectedFatal,
            "mcpwarp: the expose() driver ended unexpectedly",
        )
        .with_fatal(true)),
    }
}

/// The transport refused the configuration: `DISCONNECTED_FATAL`. The URL is
/// not echoed.
fn build_error(e: BuildError, name: &str) -> McpwarpError {
    let base = |message: &str| {
        McpwarpError::new(McpwarpErrorCode::DisconnectedFatal, message)
            .with_fatal(true)
            .with_service_name(name)
    };
    match e {
        BuildError::InvalidUrl { .. } => base("mcpwarp: `url` must be a valid ws:// or wss:// URL"),
        BuildError::NoRuntime => base("mcpwarp: expose() must run within a tokio runtime"),
        e => base(&format!("mcpwarp: invalid client configuration: {e}")).with_source(Arc::new(e)),
    }
}

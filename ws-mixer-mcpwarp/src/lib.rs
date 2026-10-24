//! The mcpwarp layer over [ws-mixer]: expose a local MCP server at a public
//! mcpwarp URL ([MCPWARP.md]).
//!
//! An agent dials the edge with a [`ws_mixer::Client`], registers one named
//! service with a `register` envelope after every `welcome`, and answers each
//! stream the edge opens as one HTTP/1.1 request to `/mcp`.
//!
//! This release implements spec v0.8.0 for one service per connection, the
//! scope of every SDK (§6A, multi-service hosts, is informative guidance):
//!
//! - the envelope: [`encode_register`] / [`encode_unregister`] (byte-exact,
//!   §2.2) and [`decode_envelope`] (§2.8), with [`RegisterErrorCode`],
//!   [`AppErrorCode`] and [`McpwarpError`];
//! - name and integration-id syntax ([`is_valid_service_name`],
//!   [`is_valid_integration_id`], §2.4);
//! - identity on every dial ([`Identity`]: `hello.meta`, `hello.agent`,
//!   `X-Mcpwarp-Integration`, `User-Agent`, §3.1);
//! - the sans-IO registration state machine ([`session::Session`]: §2.7,
//!   §3.2, §3.3);
//! - request bridging ([`bridge::serve_stream`]: §4), with per-request
//!   state ([`bridge::ServeContext::state_for_request`]) for hosts serving
//!   several services, and with the `target` feature
//!   `bridge::target_reply` to tell a `Target`'s own `502`/`504` apart;
//! - [`expose()`]: one client, one registered service, every request served
//!   (§5), with [`ExposeHandle`] and [`ExposeEvent`];
//! - the tunnel controller ([`Tunnel`], §6): configure at runtime, one
//!   [`TunnelStatus`], a static token or a
//!   [`token_provider`](TunnelConfig::token_provider);
//! - status text ([`describe_status`], §7), and one service's label for a
//!   host listing several ([`describe_service_status_raw`], §6A.9; a
//!   multi-service host builds its own `describe_service_status` over it,
//!   §6A being informative).
//!
//! [ws-mixer]: https://docs.rs/ws-mixer
//! [MCPWARP.md]: https://github.com/mcpwarp/ws-mixer-spec/blob/v0.8.0/docs/MCPWARP.md
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod bridge;
mod codes;
mod envelope;
mod error;
mod expose;
mod identity;
mod names;
pub mod session;
mod status_text;
#[cfg(feature = "test-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub mod test_util;
mod transport;
mod tunnel;

pub use codes::{AppErrorCode, RegisterErrorCode, TRANSIENT_REGISTER_ERRORS};
pub use envelope::{
    InboundEnvelope, RegisteredService, ServiceError, UnregisteredService, decode_envelope,
    encode_register, encode_unregister,
};
pub use error::{ConfigError, McpwarpError, McpwarpErrorCode};
pub use expose::{ExposeConfig, ExposeConfigBuilder, ExposeEvent, ExposeHandle, expose};
pub use identity::{Agent, CLIENT_ID, INTEGRATION_HEADER, Identity, VERSION};
pub use names::{
    INTEGRATION_ID_RE, SERVICE_NAME_MAX, SERVICE_NAME_RE, ServiceKind, is_valid_integration_id,
    is_valid_service_name,
};
pub use session::{Conflict, ExposeState, Registration, RetryInfo};
pub use status_text::{
    DescribeOptions, STATUS_LOCALES, StatusMessages, StatusText, describe_service_status_raw,
    describe_status, describe_status_raw,
};
pub use tunnel::status::{TunnelState, TunnelStatus, TunnelStatusCode, last_error};
pub use tunnel::{
    DEFAULT_DISPOSE_TIMEOUT, Subscription, Tunnel, TunnelConfig, TunnelOptions,
    TunnelOptionsBuilder,
};

/// The envelope version, `v` (MCPWARP.md §2.1).
pub const MCPWARP_V: u64 = 1;

/// The edge's default URL (MCPWARP.md §2.4). The edge routes by `Host`; the
/// path is ignored.
pub const DEFAULT_URL: &str = "wss://connect.mcpwarp.io";

#[cfg(test)]
#[allow(
    dead_code,
    reason = "the helpers are used in a const item, which rustc 1.85 does not count as a use"
)]
mod auto_traits {
    use super::*;

    const fn send_sync_static<T: Send + Sync + 'static>() {}
    const fn send_static<T: Send + 'static>() {}

    const _: () = {
        send_sync_static::<McpwarpError>();
        send_sync_static::<ConfigError>();
        send_sync_static::<InboundEnvelope>();
        send_sync_static::<Identity>();
        send_sync_static::<session::Session>();
        send_sync_static::<session::Command>();
        send_sync_static::<ExposeHandle>();
        send_sync_static::<ExposeEvent>();
        send_static::<ExposeConfig>();
        send_static::<ExposeConfigBuilder>();
        send_sync_static::<Tunnel>();
        send_sync_static::<TunnelConfig>();
        send_sync_static::<Subscription>();
        send_static::<TunnelOptions>();
        send_static::<TunnelOptionsBuilder>();
    };

    /// `expose()`'s future can be spawned.
    fn _expose_is_send(config: ExposeConfig) {
        fn send<T: Send + 'static>(_: T) {}
        send(expose(config));
    }
}

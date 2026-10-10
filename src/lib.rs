//! Rust client SDK for the [ws-mixer.v1] protocol.
//!
//! ws-mixer multiplexes many independent, flow-controlled byte streams over a
//! single WebSocket. The server opens streams; this client accepts them and
//! exposes each one as a [`Stream`] implementing tokio's `AsyncRead` and
//! `AsyncWrite`. A JSON control channel (stream 0) carries the handshake,
//! keepalive, drain notices and opaque application messages ([`Client::send_app`]).
//!
//! The [`Client`] owns a reconnect loop that follows the spec's §2.9 state
//! machine: full-jitter exponential backoff, a stability window that resets
//! the attempt counter, and fatal versus retryable close classification
//! ([`classify_close`]). Everything the application needs to react to arrives
//! on the [`Events`] channel.
//!
//! **Status: pre-alpha.** The protocol and reconnect loop are implemented;
//! cross-SDK conformance runs and a crates.io release are still pending.
//!
//! # Example
//!
//! ```no_run
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use ws_mixer::{Client, Event};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let (client, mut events) = Client::builder("wss://edge.example.com/v1")
//!     .token("secret-token")
//!     .connect()?;
//!
//! while let Some(event) = events.recv().await {
//!     match event {
//!         Event::Connected(c) => println!("connected, session {}", c.session),
//!         Event::Stream(mut stream) => {
//!             tokio::spawn(async move {
//!                 let mut request = Vec::new();
//!                 stream.read_to_end(&mut request).await?;
//!                 stream.write_all(b"response").await?;
//!                 stream.close_write().await
//!             });
//!         }
//!         Event::Disconnected(reason) if reason.fatal => {
//!             eprintln!("giving up: {reason}");
//!             break;
//!         }
//!         _ => {}
//!     }
//! }
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Cargo features
//!
//! - `rustls-platform-verifier` (default): verify server certificates against
//!   the operating system's trust store.
//! - `rustls-webpki-roots`: enable `TlsConfig::WebpkiRoots`, the bundled
//!   Mozilla root set.
//! - `native-tls`: enable `TlsConfig::NativeTls`. On its own
//!   (`default-features = false`) it is the only TLS stack and rustls is not
//!   compiled at all; next to the rustls features, rustls stays the default
//!   and `NativeTls` is opt-in via [`ClientBuilder::tls`].
//! - `ring` (default): use rustls' ring crypto provider.
//! - `aws-lc-rs`: use rustls' aws-lc-rs crypto provider. Takes precedence
//!   over `ring` when both are enabled; to drop ring entirely, set
//!   `default-features = false` and re-enable the TLS features you need.
//! - `test-util`: constructors for every event payload (`new_for_test()`
//!   plus `with_*` setters) and `Stream::new_in_memory` with its peer types
//!   in `ws_mixer::test_util`, for testing code that consumes this crate
//!   without a server.
//!
//! A TLS stack is required even for `ws://`-only use: the rustls features
//! need `ring` or `aws-lc-rs`, otherwise enable `native-tls`. Builds without
//! either fail with a `compile_error!`.
//!
//! [ws-mixer.v1]: https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod client;
mod conn;
mod control;
mod error;
mod event;
mod frame;
mod policy;
mod stats;
mod stream;
#[cfg(feature = "test-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub mod test_util;
mod tls;
mod token;

pub use client::{Client, ClientBuilder, ClientState, ReconnectPolicy};
pub use control::{AgentInfo, ServerInfo, Welcome};
pub use error::{BuildError, CloseError, ErrorCode, SendAppError, StreamError};
pub use event::{
    AppMessage, ConnId, Connected, DisconnectReason, DrainNotice, DrainReason, Event, Events, Phase,
};
pub use policy::{CloseClass, classify_close};
pub use stats::Stats;
pub use stream::{Stream, StreamId, StreamReadHalf, StreamWriteHalf};
pub use tls::TlsConfig;
pub use token::{BoxError, Token, TokenError, TokenProvider};

/// The WebSocket subprotocol negotiated on every connection.
pub const SUBPROTOCOL: &str = "ws-mixer.v1";

/// This crate's version, sent as `hello.agent.sdk_version` by default.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The default `hello.agent.sdk` value.
pub(crate) const SDK_NAME: &str = "ws-mixer-rust";

#[cfg(test)]
#[allow(
    dead_code,
    reason = "the helpers are used in a const item, which rustc 1.85 does not count as a use"
)]
mod auto_traits {
    use super::*;

    const fn send_sync_static<T: Send + Sync + 'static>() {}
    const fn unpin<T: Unpin>() {}

    const _: () = {
        send_sync_static::<Client>();
        send_sync_static::<Events>();
        send_sync_static::<Event>();
        send_sync_static::<Stream>();
        send_sync_static::<StreamReadHalf>();
        send_sync_static::<StreamWriteHalf>();
        send_sync_static::<DisconnectReason>();
        send_sync_static::<Token>();
        send_sync_static::<TokenError>();
        unpin::<Stream>();
        unpin::<StreamReadHalf>();
        unpin::<StreamWriteHalf>();
    };
}

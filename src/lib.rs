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
//! This crate implements the client role of spec v0.8.1 and passes the spec
//! runner's 25 client fixtures and its 10 pair scenarios against the
//! ws-mixer-go v0.9.0 server.
//!
//! # Example
//!
//! Connect with a static token, echo every stream the server opens, log
//! application messages, and close when the client gives up:
//!
//! ```no_run
//! use tokio::io::AsyncWriteExt;
//! use ws_mixer::{Client, Event, SendAppError};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let (client, mut events) = Client::builder("wss://edge.example.com/v1/tunnel")
//!     .token("secret-token")
//!     .connect()?;
//!
//! while let Some(event) = events.recv().await {
//!     match event {
//!         Event::Connected(c) => {
//!             println!("conn {} up, session {}, reconnect {}", c.conn_id, c.session, c.reconnect);
//!             // Nothing is replayed after a reconnect: re-announce state here.
//!             match client.send_app(serde_json::json!({ "op": "ready" })).await {
//!                 Ok(()) | Err(SendAppError::NotConnected) => {}
//!                 Err(e) => return Err(e.into()),
//!             }
//!         }
//!         Event::Stream(stream) => {
//!             // Serve each stream on its own task; never block the event loop.
//!             tokio::spawn(async move {
//!                 let (mut rx, mut tx) = stream.into_split();
//!                 tokio::io::copy(&mut rx, &mut tx).await?; // until the peer's CLOSE
//!                 tx.shutdown().await // our CLOSE
//!             });
//!         }
//!         Event::App(msg) => println!("app on conn {}: {:?}", msg.conn_id, msg.body),
//!         Event::Drain(d) => println!("conn {} draining: {}", d.conn_id, d.reason),
//!         Event::Disconnected(r) => {
//!             println!("conn {:?} ended ({} phase): {r}", r.conn_id, r.phase);
//!             if r.fatal {
//!                 break;
//!             }
//!         }
//!         _ => {} // `Event` is non-exhaustive
//!     }
//! }
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! The crate's `examples/` directory has a runnable version of this
//! (`echo_server_client`) and a [`TokenProvider`] example with a state
//! printer driven by [`Client::watch_state`] (`token_provider`).
//!
//! # Lifecycle rules
//!
//! - **[`Events`] is the lifeline.** Keep receiving from it for as long as
//!   the client should live. Dropping it closes the client gracefully, even
//!   while [`Client`] handles exist; dropping only the `Client` handles does
//!   not. Application events are bounded per connection: a connection whose
//!   backlog of unreceived `Stream`/`App`/`Drain` events exceeds
//!   `max(128, max_streams + 64)` is failed with `ENHANCE_YOUR_CALM`.
//! - **One [`Event::Disconnected`] per connection attempt.** Every attempt
//!   that fails or ends reports exactly once, including a token rejected
//!   before `welcome` (HTTP 401, close 4011) that a [`Token::Provider`] gets to
//!   refresh (non-fatal, then an [`Event::Redial`] and the immediate retry).
//!   The exception: a dial abandoned by [`Client::close`], [`Client::close_with`],
//!   [`Client::close_immediate`] or a
//!   [`ClientBuilder::no_redial_on_drain`] veto reports nothing. Per connection,
//!   `Connected` comes first and `Disconnected` last, with all of its
//!   `Stream`/`App`/`Drain` events in between.
//! - **`fatal` is not the same as final.** [`DisconnectReason::fatal`] means
//!   the client gave up because of an error: HTTP 403/404, close 4010
//!   (`UNSUPPORTED`), 4011 after `welcome` or with a static token, a token
//!   rejected again after its one refresh, a token provider returning
//!   [`TokenError::Fatal`], reconnect disabled, or `max_attempts` reached.
//!   A close by the application ([`Client::close`],
//!   [`Client::close_with`], [`Client::close_immediate`]) is non-fatal but
//!   just as final, and so is the end of a connection whose drain was vetoed
//!   with [`ClientBuilder::no_redial_on_drain`].
//!   [`ClientState::Closed`] is published when closing *begins*: no more
//!   redials, but connections still draining after a hand-over may yet report
//!   their `Disconnected`, even after a fatal one. [`Events::recv`] returning
//!   `None` is the only end-of-events signal.
//!   [`DisconnectReason::retry_in`] is set when a redial is scheduled.
//! - **Every scheduled dial is announced.** [`Event::Redial`] reports each
//!   dial the client schedules, with its [`RedialCause`] (a failure, a drain's
//!   replacement, or the token refresh), attempt number and the delay
//!   actually used. It follows the `Disconnected` or `Drain` it answers.
//! - **Drain hand-over overlaps connections.** On [`Event::Drain`] the
//!   client dials a replacement (unless reconnect is disabled or the drain's
//!   reason is vetoed with [`ClientBuilder::no_redial_on_drain`]) while the old
//!   connection keeps serving its streams. The replacement's `Connected` may arrive *before* the old
//!   connection's `Disconnected`, so key any per-connection state on
//!   [`ConnId`] ([`Connected::conn_id`], [`Stream::conn_id`],
//!   [`DisconnectReason::conn_id`]), never on "the current connection".
//! - **`send_app` never queues.** [`Client::send_app`] writes to the live
//!   welcomed connection or fails with [`SendAppError::NotConnected`]; nothing
//!   is buffered across a reconnect. During a drain it uses the old
//!   connection until the replacement is welcomed. State the server needs on
//!   every connection belongs in `hello.meta`
//!   ([`ClientBuilder::hello_meta`], [`Client::set_hello_meta`]) or in a
//!   message sent on `Connected`.
//! - **Streams belong to one connection.** Read each [`Stream`]
//!   continuously (unread data holds back the peer's credit). When its
//!   connection dies, reads and writes fail with
//!   [`StreamError::ConnectionLost`] (`ConnectionAborted`), never a clean
//!   EOF; streams are not migrated to the next connection. Dropping a stream
//!   that is not fully closed sends `RESET(CANCEL)`: half-close with
//!   [`Stream::close_write`] / `shutdown()` and read to EOF to finish cleanly,
//!   or decline it up front with [`Stream::refuse`]. [`Stream::closed`]
//!   resolves when the stream ends, with a [`StreamCloseReason`] (finished,
//!   reset by either side, or connection lost).
//!
//! # TLS and Cargo features
//!
//! | Feature | Default | Effect |
//! |---|---|---|
//! | `rustls-platform-verifier` | yes | rustls verifying against the OS trust store (`TlsConfig::PlatformVerifier`) |
//! | `ring` | yes | rustls crypto provider: ring |
//! | `rustls-webpki-roots` | | rustls with the bundled Mozilla roots (`TlsConfig::WebpkiRoots`) |
//! | `aws-lc-rs` | | rustls crypto provider: aws-lc-rs; wins over `ring` when both are on |
//! | `native-tls` | | the platform TLS library (`TlsConfig::NativeTls`) |
//! | `test-util` | | `new_for_test()` constructors for every event payload and an in-memory `Stream` (`ws_mixer::test_util`) |
//! | `conformance` | | test-only knobs for the spec conformance adapter; not for applications |
//!
//! The default for `wss://` is the platform verifier, else webpki roots, else
//! native-tls; [`ClientBuilder::tls`] picks another, including
//! `TlsConfig::Custom` for a caller-built `rustls::ClientConfig`.
//!
//! The SDK builds every rustls configuration with an explicit crypto
//! provider and never installs or reads rustls' process-wide default
//! provider, so it cannot panic when ring and aws-lc-rs both end up in one
//! dependency graph (common in Tauri apps). To drop ring entirely, use
//! `default-features = false` with `aws-lc-rs` and the rustls features you
//! need. A TLS stack is required even for `ws://`-only use: the rustls
//! features need `ring` or `aws-lc-rs`, otherwise enable `native-tls`. Builds
//! without either fail with a `compile_error!`.
//!
//! # Runtime and MSRV
//!
//! tokio only; [`ClientBuilder::connect`] must be called inside a tokio
//! runtime. The minimum supported Rust version is 1.85 (edition 2024).
//!
//! [ws-mixer.v1]: https://github.com/mcpwarp/ws-mixer-spec/blob/v0.8.1/docs/WIRE.md
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
    AppMessage, ConnId, Connected, DisconnectReason, DrainNotice, DrainReason, Event, Events,
    Phase, RedialCause, RedialNotice,
};
pub use policy::{CloseClass, classify_close};
pub use stats::Stats;
pub use stream::{Stream, StreamCloseReason, StreamId, StreamReadHalf, StreamWriteHalf};
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
        send_sync_static::<StreamCloseReason>();
        send_sync_static::<DisconnectReason>();
        send_sync_static::<RedialNotice>();
        send_sync_static::<RedialCause>();
        send_sync_static::<Token>();
        send_sync_static::<TokenError>();
        unpin::<Stream>();
        unpin::<StreamReadHalf>();
        unpin::<StreamWriteHalf>();
    };
}

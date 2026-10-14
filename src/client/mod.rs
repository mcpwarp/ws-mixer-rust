//! The public [`Client`] handle and its builder.

pub(crate) mod policy;
pub(crate) mod supervisor;
pub(crate) mod token;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use http::{HeaderName, HeaderValue};
use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::conn::ConnConfig;
use crate::control::{AgentInfo, Welcome};
use crate::error::{BuildError, CloseError, SendAppError};
use crate::event::{DisconnectReason, DrainReason, Events};
use crate::stats::Stats;
use crate::tls::TlsConfig;
use crate::token::Token;
use supervisor::{Command, Link, Params, Supervisor};

/// Reconnect behaviour (WIRE.md §2.9).
///
/// Delays use full jitter: before each redial the attempt counter is
/// incremented and the delay is `random(0..1) * min(cap, base * 2^attempt)`.
/// A connection that stays up for `stable_after` resets the counter.
///
/// [`ClientBuilder::connect`] rejects `base`, `cap` or `stable_after` above
/// 24 hours and `connect_timeout` above 1 hour.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReconnectPolicy {
    /// `false`: the first disconnect is final.
    pub enabled: bool,
    /// Give up after this many consecutive failed attempts. `None` = unlimited.
    pub max_attempts: Option<NonZeroU32>,
    /// Backoff base. Default 1 s.
    pub base: Duration,
    /// Backoff cap. Default 60 s.
    pub cap: Duration,
    /// How long a connection must stay up to reset the attempt counter and the
    /// one-shot retry budgets. Default 10 s.
    pub stable_after: Duration,
    /// Bound on TCP connect, TLS and the WebSocket upgrade together (the
    /// `welcome` wait is separate, see [`ClientBuilder::hello_timeout`]).
    /// Also bounds each token provider call. Default 10 s.
    pub connect_timeout: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: None,
            base: Duration::from_secs(1),
            cap: Duration::from_secs(60),
            stable_after: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

impl ReconnectPolicy {
    /// The default policy with reconnect turned off: the first disconnect is
    /// final.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }
}

/// The client's lifecycle state, observable via [`Client::state`] and
/// [`Client::watch_state`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ClientState {
    /// Created, not dialing yet.
    Idle,
    /// A dial or handshake is in progress.
    Dialing,
    /// A welcomed connection is live.
    Connected,
    /// Waiting before the next dial.
    Backoff,
    /// Closed for good, by the application or after a fatal disconnect.
    Closed,
}

/// Configures and starts a [`Client`]. Created by [`Client::builder`].
#[derive(Debug)]
#[must_use = "a builder does nothing until `connect` is called"]
pub struct ClientBuilder {
    url: String,
    token: Option<Token>,
    hello_meta: Option<serde_json::Value>,
    agent: AgentInfo,
    capabilities: Vec<String>,
    window: u32,
    max_streams: u32,
    headers: Vec<(HeaderName, HeaderValue)>,
    reconnect: ReconnectPolicy,
    hello_timeout: Duration,
    tls: Option<TlsConfig>,
    allow_subfloor_timing: bool,
    no_redial_on: Vec<DrainReason>,
}

impl ClientBuilder {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
            token: None,
            hello_meta: None,
            agent: AgentInfo::default(),
            capabilities: Vec::new(),
            window: 262_144,
            max_streams: 64,
            headers: Vec::new(),
            reconnect: ReconnectPolicy::default(),
            hello_timeout: Duration::from_secs(10),
            tls: TlsConfig::default_for_build(),
            allow_subfloor_timing: false,
            no_redial_on: Vec::new(),
        }
    }

    /// Sets the bearer token. Required.
    pub fn token(mut self, t: impl Into<Token>) -> Self {
        self.token = Some(t.into());
        self
    }

    /// Sets `hello.meta`, delivered to the server application before
    /// `welcome`. Must be a JSON object; checked by [`connect`](Self::connect).
    /// That size check counts a provider token as empty; a `hello` that is
    /// still over 16 KiB at dial time is a fatal disconnect.
    pub fn hello_meta(mut self, meta: serde_json::Value) -> Self {
        self.hello_meta = Some(meta);
        self
    }

    /// Overrides `hello.agent.sdk` and `hello.agent.sdk_version`. Defaults to
    /// `"ws-mixer-rust"` and [`SDK_VERSION`](crate::SDK_VERSION).
    pub fn agent(mut self, sdk: &str, version: &str) -> Self {
        self.agent.sdk = sdk.to_owned();
        self.agent.sdk_version = version.to_owned();
        self
    }

    /// Overrides `hello.agent.runtime`, by default `"rust"`. 1 to 128
    /// characters; checked by [`connect`](Self::connect).
    pub fn agent_runtime(mut self, runtime: impl Into<String>) -> Self {
        self.agent.runtime = Some(runtime.into());
        self
    }

    /// Overrides `hello.agent.os`, by default `"<os>/<arch>"` from
    /// [`std::env::consts`], e.g. `"macos/aarch64"`. 1 to 128 characters;
    /// checked by [`connect`](Self::connect).
    pub fn agent_os(mut self, os: impl Into<String>) -> Self {
        self.agent.os = Some(os.into());
        self
    }

    /// Sets `hello.capabilities`. Each must match `^[a-z0-9_.-]{1,64}$`.
    pub fn capabilities(mut self, caps: impl IntoIterator<Item = String>) -> Self {
        self.capabilities = caps.into_iter().collect();
        self
    }

    /// Sets this client's per-stream receive window in bytes
    /// (`16384..=2^31-1`, default 262144).
    pub fn window(mut self, bytes: u32) -> Self {
        self.window = bytes;
        self
    }

    /// Sets how many concurrent streams this client accepts
    /// (`1..=100000`, default 64).
    pub fn max_streams(mut self, n: u32) -> Self {
        self.max_streams = n;
        self
    }

    /// Adds a header to the WebSocket upgrade request. `Authorization`,
    /// `Sec-WebSocket-Protocol` and the other upgrade headers are managed by
    /// the SDK and rejected by [`connect`](Self::connect).
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.push((name, value));
        self
    }

    /// Sets the reconnect policy.
    pub fn reconnect(mut self, p: ReconnectPolicy) -> Self {
        self.reconnect = p;
        self
    }

    /// How long to wait for `welcome` after sending `hello`. Default 10 s,
    /// at most 1 hour.
    pub fn hello_timeout(mut self, d: Duration) -> Self {
        self.hello_timeout = d;
        self
    }

    /// Sets the TLS configuration for `wss://` URLs.
    pub fn tls(mut self, t: TlsConfig) -> Self {
        self.tls = Some(t);
        self
    }

    /// Does not dial a replacement for a server `drain` with this `reason`;
    /// calling it again for the same reason has no effect. The
    /// [`Event::Drain`](crate::Event::Drain) is still delivered, but no
    /// [`Event::Redial`](crate::Event::Redial) follows: the drained
    /// connection stays the live one, no later drain on it dials either, and
    /// a replacement still pending from an earlier drain of it is abandoned.
    /// The application is expected to close the client in response,
    /// typically with [`Client::close_immediate`]. If the connection ends
    /// first, unless a replacement is already live, it reports one non-fatal
    /// `Disconnected` with no `retry_in` and the client closes (unlike
    /// reconnect disabled, which ends fatal); nothing is redialed. The same
    /// holds when the connection is failed with `ENHANCE_YOUR_CALM` because
    /// the event backlog was full and the `Drain` event itself was dropped.
    ///
    /// Abandoning a pending replacement prevents its `hello` only while its
    /// WebSocket upgrade has not completed; such a dial is cancelled
    /// silently. A replacement past its upgrade when the vetoed drain is
    /// processed is closed with `error{NO_ERROR}` and close 1000 whether or
    /// not its `welcome` arrived, and is never reported as `Connected`; the
    /// server will usually have seen its `hello` (an abandon landing between
    /// the upgrade and the `hello` write closes without a `hello` first).
    ///
    /// For a layer that treats a drain as a take-over: mcpwarp's
    /// `drain{reason: "replaced"}` must not lead to a `hello` on a
    /// replacement, which a replacement dialed with zero delay could
    /// otherwise send before the application sees the `Drain` event.
    pub fn no_redial_on_drain(mut self, reason: DrainReason) -> Self {
        if !self.no_redial_on.contains(&reason) {
            self.no_redial_on.push(reason);
        }
        self
    }

    /// Accepts `welcome` timing below the spec floors (`ping_interval < 5000`,
    /// `ping_timeout < 2 * ping_interval`). Conformance testing only.
    #[cfg(feature = "conformance")]
    #[doc(hidden)]
    pub fn allow_subfloor_timing(mut self) -> Self {
        self.allow_subfloor_timing = true;
        self
    }

    /// Validates the configuration, spawns the client's supervisor task on the
    /// current tokio runtime and starts dialing.
    ///
    /// Returns the client handle and its event channel. The client runs until
    /// [`Client::close`] / [`Client::close_with`], a fatal disconnect, or
    /// until the [`Events`] receiver is dropped (then it closes gracefully on
    /// its own, as it does when everything is dropped). Dropping only the
    /// `Client` handles keeps it running.
    pub fn connect(self) -> Result<(Client, Events), BuildError> {
        self.connect_via(crate::conn::network_upgrade(), Box::new(fastrand::f64))
    }

    /// [`connect`](Self::connect) with an injectable transport and RNG.
    pub(crate) fn connect_via(
        self,
        upgrade: crate::conn::Upgrade,
        rng: Box<dyn FnMut() -> f64 + Send>,
    ) -> Result<(Client, Events), BuildError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| BuildError::NoRuntime)?;
        let wss = self.validate_url()?;
        let token = self.token.clone().ok_or(BuildError::MissingToken)?;
        if !(16_384..=0x7fff_ffff).contains(&self.window) {
            return Err(BuildError::WindowOutOfRange(self.window));
        }
        if !(1..=100_000).contains(&self.max_streams) {
            return Err(BuildError::MaxStreamsOutOfRange(self.max_streams));
        }
        let meta = meta_object(self.hello_meta.clone())?;
        if let Some(bad) = self
            .capabilities
            .iter()
            .find(|c| !crate::control::valid_capability(c))
        {
            return Err(BuildError::InvalidCapability(bad.clone()));
        }
        let a = &self.agent;
        let agent_fields = [
            Some(&a.sdk),
            Some(&a.sdk_version),
            a.runtime.as_ref(),
            a.os.as_ref(),
        ];
        for field in agent_fields.into_iter().flatten() {
            if !(1..=128).contains(&field.chars().count()) {
                return Err(BuildError::InvalidAgent(field.clone()));
            }
        }
        if let Some((name, _)) = self.headers.iter().find(|(n, _)| is_managed_header(n)) {
            return Err(BuildError::ForbiddenHeader(name.clone()));
        }
        let p = &self.reconnect;
        for (name, d) in [
            ("hello_timeout", self.hello_timeout),
            ("reconnect.base", p.base),
            ("reconnect.cap", p.cap),
            ("reconnect.connect_timeout", p.connect_timeout),
        ] {
            if d.is_zero() {
                return Err(BuildError::InvalidDuration {
                    name,
                    reason: "must be greater than zero",
                });
            }
        }
        for (name, d, max, reason) in [
            (
                "hello_timeout",
                self.hello_timeout,
                MAX_TIMEOUT,
                "must be at most 1 hour",
            ),
            (
                "reconnect.connect_timeout",
                p.connect_timeout,
                MAX_TIMEOUT,
                "must be at most 1 hour",
            ),
            (
                "reconnect.base",
                p.base,
                MAX_BACKOFF,
                "must be at most 24 hours",
            ),
            (
                "reconnect.cap",
                p.cap,
                MAX_BACKOFF,
                "must be at most 24 hours",
            ),
            (
                "reconnect.stable_after",
                p.stable_after,
                MAX_BACKOFF,
                "must be at most 24 hours",
            ),
        ] {
            if d > max {
                return Err(BuildError::InvalidDuration { name, reason });
            }
        }
        if p.cap < p.base {
            return Err(BuildError::InvalidDuration {
                name: "reconnect.cap",
                reason: "must not be less than reconnect.base",
            });
        }
        let probe = HelloProbe {
            token: match &token {
                Token::Static(s) => s.clone(),
                Token::Provider(_) => String::new(),
            },
            agent: self.agent.clone(),
            window: self.window,
            max_streams: self.max_streams,
            capabilities: self.capabilities.clone(),
        };
        probe.check(&meta)?;
        let tls = if wss { prepare_tls(self.tls)? } else { None };
        if wss && tls.is_none() {
            return Err(BuildError::NoTlsConfig);
        }

        let stats = Arc::new(crate::stats::Counters::default());
        let cfg = ConnConfig {
            url: self.url,
            headers: self.headers,
            tls,
            connect_timeout: p.connect_timeout,
            hello_timeout: self.hello_timeout,
            window: self.window,
            max_streams: self.max_streams,
            agent: self.agent,
            capabilities: self.capabilities,
            meta,
            allow_subfloor_timing: self.allow_subfloor_timing,
            stats: stats.clone(),
            drains: None,
        };
        let (events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();
        let (commands_tx, commands) = tokio::sync::mpsc::unbounded_channel();
        let (state_tx, state) = watch::channel(ClientState::Idle);
        let (link_tx, link) = watch::channel(Link::Down);
        let _guard = runtime.enter();
        Supervisor::spawn(Params {
            cfg,
            token,
            policy: self.reconnect,
            no_redial_on: self.no_redial_on,
            upgrade,
            rng,
            events: events_tx,
            commands,
            state: state_tx,
            link: link_tx,
        });
        let client = Client {
            inner: Arc::new(Inner {
                commands: commands_tx,
                state,
                link,
                stats,
                probe,
            }),
        };
        Ok((client, Events::new(events_rx)))
    }

    /// Checks the URL; returns whether it is `wss://`.
    fn validate_url(&self) -> Result<bool, BuildError> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let invalid = |reason: String| BuildError::InvalidUrl {
            url: self.url.clone(),
            reason,
        };
        let req = self
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| invalid(e.to_string()))?;
        let wss = match req.uri().scheme_str() {
            Some("wss") => true,
            Some("ws") => false,
            _ => return Err(invalid("the scheme must be ws or wss".into())),
        };
        if req.uri().host().is_none_or(str::is_empty) {
            return Err(invalid("missing host".into()));
        }
        Ok(wss)
    }
}

/// Upgrade headers the SDK sets itself.
fn is_managed_header(name: &HeaderName) -> bool {
    use http::header::{
        AUTHORIZATION, CONNECTION, HOST, SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY,
        SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
    };
    [
        AUTHORIZATION,
        CONNECTION,
        HOST,
        SEC_WEBSOCKET_EXTENSIONS,
        SEC_WEBSOCKET_KEY,
        SEC_WEBSOCKET_PROTOCOL,
        SEC_WEBSOCKET_VERSION,
        UPGRADE,
    ]
    .contains(name)
}

fn meta_object(meta: Option<serde_json::Value>) -> Result<Option<Map<String, Value>>, BuildError> {
    match meta {
        None => Ok(None),
        Some(Value::Object(m)) => Ok(Some(m)),
        Some(_) => Err(BuildError::MetaNotObject),
    }
}

/// Upper bound on `hello_timeout` and `connect_timeout`.
const MAX_TIMEOUT: Duration = Duration::from_secs(3600);
/// Upper bound on the backoff `base`, `cap` and `stable_after`.
const MAX_BACKOFF: Duration = Duration::from_secs(24 * 3600);

/// Everything `hello` is built from except `meta`, to size-check a `meta`
/// before any dial uses it. A provider's token is only known at dial time and
/// counts as empty here.
struct HelloProbe {
    token: String,
    agent: AgentInfo,
    window: u32,
    max_streams: u32,
    capabilities: Vec<String>,
}

impl HelloProbe {
    fn check(&self, meta: &Option<Map<String, Value>>) -> Result<(), BuildError> {
        let hello = crate::control::Hello::new(
            self.token.clone(),
            self.agent.clone(),
            self.window,
            self.max_streams,
            self.capabilities.clone(),
            meta.clone(),
        );
        crate::frame::encode_control(&hello)
            .map(drop)
            .map_err(|e| BuildError::HelloTooLarge { bytes: e.bytes })
    }
}

/// Builds a rustls configuration once, so every dial reuses it and a broken
/// configuration fails `connect` rather than every dial.
#[cfg(feature = "__rustls")]
fn prepare_tls(tls: Option<TlsConfig>) -> Result<Option<TlsConfig>, BuildError> {
    match tls {
        #[cfg(feature = "native-tls")]
        Some(TlsConfig::NativeTls) => Ok(Some(TlsConfig::NativeTls)),
        Some(t) => crate::tls::rustls_config(t)
            .map(|c| Some(TlsConfig::Custom(c)))
            .map_err(BuildError::Tls),
        None => Ok(None),
    }
}

#[cfg(not(feature = "__rustls"))]
fn prepare_tls(tls: Option<TlsConfig>) -> Result<Option<TlsConfig>, BuildError> {
    Ok(tls)
}

/// A handle to a ws-mixer client. Cheap to clone; all clones control the same
/// client.
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    commands: tokio::sync::mpsc::UnboundedSender<Command>,
    state: watch::Receiver<ClientState>,
    link: watch::Receiver<Link>,
    stats: Arc<crate::stats::Counters>,
    probe: HelloProbe,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &*self.state.borrow())
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Starts configuring a client for `url` (`ws://` or `wss://`).
    pub fn builder(url: impl AsRef<str>) -> ClientBuilder {
        ClientBuilder::new(url.as_ref())
    }

    /// Waits until a connection is welcomed and returns its `welcome`.
    /// Returns immediately if one is live. Fails with the fatal disconnect
    /// reason if the client gives up first.
    ///
    /// After [`close`](Self::close) or [`close_with`](Self::close_with) it
    /// fails with a non-fatal reason describing the application's close.
    pub async fn wait_connected(&self) -> Result<Welcome, DisconnectReason> {
        let mut link = self.inner.link.clone();
        loop {
            match &*link.borrow_and_update() {
                Link::Up(h) => return Ok(h.welcome().clone()),
                Link::Closed(r) => return Err(r.clone()),
                Link::Down => {}
            }
            if link.changed().await.is_err() {
                // The supervisor is gone; it always publishes Closed first.
                return match &*link.borrow() {
                    Link::Up(h) => Ok(h.welcome().clone()),
                    Link::Closed(r) => Err(r.clone()),
                    Link::Down => Err(crate::conn::base_reason(
                        None,
                        crate::event::Phase::Dial,
                        "the client task ended".into(),
                    )),
                };
            }
        }
    }

    /// Sends an `app` message on the live connection.
    ///
    /// `body` must be a JSON object and the encoded message must fit in a
    /// 16 KiB control frame. Messages are never queued across a reconnect:
    /// without a welcomed connection this fails with
    /// [`SendAppError::NotConnected`]. During a server drain the message goes
    /// to the draining connection until its replacement is welcomed. The
    /// server may close with `ENHANCE_YOUR_CALM` (4009) if control messages
    /// exceed a burst of 100 or 50 per second.
    pub async fn send_app(&self, body: serde_json::Value) -> Result<(), SendAppError> {
        if !body.is_object() {
            return Err(SendAppError::NotAnObject);
        }
        let link = self.inner.link.borrow().clone();
        match link {
            Link::Up(h) => h.send_app(body),
            Link::Down => Err(SendAppError::NotConnected),
            Link::Closed(_) => Err(SendAppError::Closed),
        }
    }

    /// Replaces `hello.meta` for the next dial. `None` removes it. Does not
    /// affect the live connection. Fails if `meta` is not an object or the
    /// encoded `hello` would exceed 16 KiB. That check counts a provider token
    /// as empty; a `hello` that is still over 16 KiB at dial time is a fatal
    /// disconnect.
    pub fn set_hello_meta(&self, meta: Option<serde_json::Value>) -> Result<(), BuildError> {
        let meta = meta_object(meta)?;
        self.inner.probe.check(&meta)?;
        let _ = self.inner.commands.send(Command::SetMeta(meta));
        Ok(())
    }

    /// Closes gracefully: sends `drain{client_requested}`, waits up to 5 s for
    /// streams to finish, then `error{NO_ERROR}` and WebSocket close 1000.
    /// Stops the reconnect loop; a dial in progress is abandoned without an
    /// event (one past its upgrade is closed with `error{NO_ERROR}` and 1000,
    /// bounded by about 2 s). Each live connection reports one non-fatal
    /// `Disconnected`, then [`Events::recv`] returns `None`.
    ///
    /// Returns once every connection has closed, including such a dial.
    /// Idempotent.
    pub async fn close(&self) {
        let _ = self.inner.commands.send(Command::Close);
        self.inner.commands.closed().await;
    }

    /// Closes immediately as an application close: `error{APPLICATION_CLOSE,
    /// message}` and WebSocket close 4014, with the reason truncated to 123
    /// bytes. Stops the reconnect loop; a dial in progress is abandoned
    /// without an event, and one past its upgrade is closed the same way,
    /// bounded by about 2 s. Exactly one non-fatal `Disconnected` is emitted
    /// per live connection.
    ///
    /// Returns once every connection has closed, including such a dial. A
    /// no-op (apart from waiting) if a close is already in progress.
    pub async fn close_with(&self, message: impl Into<String>) {
        let _ = self.inner.commands.send(Command::CloseWith(message.into()));
        self.inner.commands.closed().await;
    }

    /// Closes at once with `error{NO_ERROR, message}` and WebSocket close
    /// 1000 (reason truncated to 123 bytes), skipping [`close`](Self::close)'s
    /// `drain` and 5 s wait: open streams end with the connection, as after
    /// [`close_with`](Self::close_with). For a peer whose streams are already
    /// stale, e.g. a service taken over by another connection. Stops the
    /// reconnect loop; a scheduled or in-progress dial is abandoned without
    /// an event, and one past its upgrade (`welcome` received or not) is
    /// closed the same way, bounded by about 2 s. Exactly one non-fatal
    /// `Disconnected` is emitted per live connection.
    ///
    /// Returns once every connection has closed. The live connection closes
    /// at once, but with such a dial in flight this may return up to ~2 s
    /// later (~5 s at worst, if its `welcome` lands during the fallback). A
    /// no-op (apart from waiting) if a close is already in progress.
    pub async fn close_immediate(&self, message: impl Into<String>) {
        let _ = self
            .inner
            .commands
            .send(Command::CloseImmediate(message.into()));
        self.inner.commands.closed().await;
    }

    /// Sends `drain{client_requested}` with `deadline` on the live connection:
    /// the server should stop opening streams on it. Does not close the
    /// connection or start a new one.
    pub async fn drain(&self, deadline: Duration) -> Result<(), CloseError> {
        let link = self.inner.link.borrow().clone();
        match link {
            Link::Up(h) => h.send_drain(deadline),
            Link::Down => Err(CloseError::NotConnected),
            Link::Closed(_) => Err(CloseError::Closed),
        }
    }

    /// The current lifecycle state.
    pub fn state(&self) -> ClientState {
        *self.inner.state.borrow()
    }

    /// A receiver that observes every state change.
    pub fn watch_state(&self) -> watch::Receiver<ClientState> {
        self.inner.state.clone()
    }

    /// Counters, cumulative across all connections.
    pub fn stats(&self) -> Stats {
        self.inner.stats.snapshot()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod builder_tests {
    use super::*;

    #[test]
    fn reconnect_defaults() {
        let p = ReconnectPolicy::default();
        assert!(p.enabled);
        assert_eq!(p.max_attempts, None);
        assert_eq!(p.base, Duration::from_secs(1));
        assert_eq!(p.cap, Duration::from_secs(60));
        assert_eq!(p.stable_after, Duration::from_secs(10));
        assert_eq!(p.connect_timeout, Duration::from_secs(10));

        let mut d = ReconnectPolicy::disabled();
        assert!(!d.enabled);
        d.enabled = true;
        assert_eq!(d, p);
    }

    #[test]
    fn builder_defaults_and_setters() {
        let b = Client::builder("ws://127.0.0.1:1/");
        assert_eq!(b.window, 262_144);
        assert_eq!(b.max_streams, 64);
        assert_eq!(b.hello_timeout, Duration::from_secs(10));
        assert_eq!(b.agent.sdk, "ws-mixer-rust");
        assert!(b.token.is_none());

        let b = b
            .token("t")
            .agent("custom", "9.9.9")
            .window(16_384)
            .max_streams(3)
            .capabilities(["a.b".to_owned()]);
        assert!(matches!(b.token, Some(Token::Static(ref s)) if s == "t"));
        assert_eq!(b.agent.sdk, "custom");
        assert_eq!(b.agent.sdk_version, "9.9.9");
        assert_eq!(b.window, 16_384);
        assert_eq!(b.max_streams, 3);
        assert_eq!(b.capabilities, ["a.b"]);
    }
}

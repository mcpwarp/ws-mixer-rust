//! The tunnel controller (MCPWARP.md §6): a long-lived [`Tunnel`] for hosts
//! with a settings UI. Call [`Tunnel::configure`] on every settings change;
//! it restarts the exposure only when something relevant changed, and
//! reports one [`TunnelStatus`].
//!
//! One actor task owns the controller. `configure`, `stop` and `dispose`
//! enqueue a command at call time and return a future for its completion;
//! every start of an exposure is a new generation, and anything an older
//! generation still reports is ignored (§6.8).

mod actor;
pub mod status;
#[cfg(all(test, feature = "test-util"))]
mod tests;

use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use ws_mixer::{ReconnectPolicy, TlsConfig, Token};

use crate::bridge::Handler;
use crate::error::ConfigError;
use crate::expose::{ExposeConfig, ExposeConfigBuilder};
use crate::identity::Agent;
use crate::names::ServiceKind;
use crate::session::RetryInfo;
use crate::transport::{ClientConnector, Connector};

use self::actor::{Actor, Msg};
use self::status::TunnelStatus;

/// How long [`Tunnel::dispose`] waits for the old connection by default
/// (MCPWARP.md §6.3).
pub const DEFAULT_DISPOSE_TIMEOUT: Duration = Duration::from_millis(5000);

/// What may change at runtime, typically read from the host's settings
/// (MCPWARP.md §6.2).
///
/// Disabled, or a token that trims to empty, is `off` without dialing,
/// whatever the other fields hold. Enabled with neither a token nor a
/// [`token_provider`](Self::token_provider), or with a `None` name, is state
/// `error`, code `INVALID_CONFIG`. The name's syntax is checked when the
/// connection starts (`error`/`INVALID_NAME`).
///
/// `Debug` never prints the token. Equality compares a provider by identity
/// (the same `Arc`).
///
/// ```
/// use ws_mixer_mcpwarp::TunnelConfig;
///
/// let config: TunnelConfig =
///     serde_json::from_str(r#"{"enabled": true, "token": "mcpwarp_pat_x", "name": "notes"}"#)?;
/// assert_eq!(config.url, None);
/// assert!(!format!("{config:?}").contains("mcpwarp_pat_x"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TunnelConfig {
    /// Whether the tunnel should be online.
    pub enabled: bool,
    /// The token (a PAT or connect-audience JWT); sent trimmed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// A token provider, used instead of [`token`](Self::token) when set
    /// (MCPWARP.md §6.2 Token provider). It is called on every dial, never
    /// cached. A rejected token before `welcome` gets one immediate refresh
    /// and redial (`on_retry` reason `"token-refresh"`); a second rejection
    /// is `error`/`UNAUTHORIZED`. [`TokenError::Unavailable`] is a failed
    /// dial, retried with backoff; [`TokenError::Fatal`] is
    /// `error`/`DISCONNECTED_FATAL`. A [`Token::Static`] here acts as
    /// `token` would. Never serialized.
    ///
    /// ```
    /// use ws_mixer::{Token, TokenError};
    /// use ws_mixer_mcpwarp::TunnelConfig;
    ///
    /// let config = TunnelConfig {
    ///     enabled: true,
    ///     token_provider: Some(Token::provider(|| async {
    ///         std::env::var("MCPWARP_TOKEN").map_err(TokenError::fatal)
    ///     })),
    ///     name: Some("notes".into()),
    ///     ..TunnelConfig::default()
    /// };
    /// // The same provider is the same config; another one is not.
    /// assert_eq!(config, config.clone());
    /// ```
    ///
    /// [`TokenError::Unavailable`]: ws_mixer::TokenError::Unavailable
    /// [`TokenError::Fatal`]: ws_mixer::TokenError::Fatal
    #[serde(skip)]
    pub token_provider: Option<Token>,
    /// The service name ([`is_valid_service_name`](crate::is_valid_service_name)).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The edge's URL; default [`DEFAULT_URL`](crate::DEFAULT_URL).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The service kind; default [`ServiceKind::Http`]. `None` and
    /// `Some(Http)` are different configs for [`Tunnel::configure`]'s
    /// idempotence check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<ServiceKind>,
}

impl fmt::Debug for TunnelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelConfig")
            .field("enabled", &self.enabled)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("token_provider", &self.token_provider)
            .field("name", &self.name)
            .field("url", &self.url)
            .field("kind", &self.kind)
            .finish()
    }
}

impl PartialEq for TunnelConfig {
    fn eq(&self, other: &Self) -> bool {
        let same_token = match (&self.token_provider, &other.token_provider) {
            (None, None) => true,
            (Some(Token::Static(a)), Some(Token::Static(b))) => a == b,
            (Some(Token::Provider(a)), Some(Token::Provider(b))) => Arc::ptr_eq(a, b),
            _ => false,
        };
        same_token
            && self.enabled == other.enabled
            && self.token == other.token
            && self.name == other.name
            && self.url == other.url
            && self.kind == other.kind
    }
}

impl Eq for TunnelConfig {}

type OnRetry = dyn Fn(&RetryInfo) + Send + Sync;
type ConnectorFactory = Arc<dyn Fn() -> Box<dyn Connector> + Send + Sync>;

/// The tunnel's fixed options (MCPWARP.md §6.1): everything of
/// [`ExposeConfig`] except what [`TunnelConfig`] changes at runtime. Built
/// with [`TunnelOptions::builder`].
pub struct TunnelOptions {
    /// A validated config whose url, token, name, kind, reconnect policy and
    /// observer are replaced per start.
    pub(crate) template: ExposeConfig,
    pub(crate) reconnect: ReconnectPolicy,
    /// §5.2 row 11, reported at start (§6.7 T8), not by `build`.
    pub(crate) reconnect_error: Option<ConfigError>,
    pub(crate) on_retry: Option<Arc<OnRetry>>,
    pub(crate) connector: ConnectorFactory,
}

impl fmt::Debug for TunnelOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelOptions")
            .field("integration", &self.template.identity.integration())
            .field("reconnect", &self.reconnect)
            .field("on_retry", &self.on_retry.is_some())
            .finish_non_exhaustive()
    }
}

impl TunnelOptions {
    /// Starts the options. `integration` and one of `handler` / `target`
    /// are required.
    ///
    /// ```
    /// use bytes::Bytes;
    /// use http_body_util::Full;
    /// use ws_mixer_mcpwarp::TunnelOptions;
    /// use ws_mixer_mcpwarp::bridge::handler_fn;
    ///
    /// let options = TunnelOptions::builder()
    ///     .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
    ///     .handler(handler_fn(|_req| async {
    ///         Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
    ///     }))
    ///     .build()?;
    /// # let _ = options;
    /// # Ok::<(), ws_mixer_mcpwarp::ConfigError>(())
    /// ```
    pub fn builder() -> TunnelOptionsBuilder {
        TunnelOptionsBuilder {
            expose: ExposeConfig::builder(),
            reconnect: ReconnectPolicy::default(),
            on_retry: None,
            connector: None,
        }
    }
}

/// Builds [`TunnelOptions`]; see [`TunnelOptions::builder`].
#[must_use = "a builder does nothing until `build` is called"]
pub struct TunnelOptionsBuilder {
    expose: ExposeConfigBuilder,
    reconnect: ReconnectPolicy,
    on_retry: Option<Arc<OnRetry>>,
    connector: Option<ConnectorFactory>,
}

impl fmt::Debug for TunnelOptionsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelOptionsBuilder")
            .field("expose", &self.expose)
            .field("reconnect", &self.reconnect)
            .finish_non_exhaustive()
    }
}

/// Stands in for the per-start token and name while the up-front options
/// are validated; never sent.
const PLACEHOLDER: &str = "placeholder";

impl TunnelOptionsBuilder {
    /// The integration's registration UUID, as for
    /// [`ExposeConfigBuilder::integration`]. Required.
    pub fn integration(mut self, id: impl Into<String>) -> Self {
        self.expose = self.expose.integration(id);
        self
    }

    /// Overrides `hello.agent.runtime` / `os`, as for
    /// [`ExposeConfigBuilder::agent`].
    pub fn agent(mut self, agent: Agent) -> Self {
        self.expose = self.expose.agent(agent);
        self
    }

    /// The TLS configuration for a `wss://` URL (MCPWARP.md §8).
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.expose = self.expose.tls(tls);
        self
    }

    /// The request body cap, as for
    /// [`ExposeConfigBuilder::max_request_body_bytes`].
    pub fn max_request_body_bytes(mut self, max: u64) -> Self {
        self.expose = self.expose.max_request_body_bytes(max);
        self
    }

    /// The client's reconnect policy. Unlike the other options it is
    /// checked when a connection starts: an invalid policy is state
    /// `error`/`INVALID_CONFIG` (MCPWARP.md §6.7 T8), not a `build` error.
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// Called for every scheduled reconnect and `register` retry of the
    /// current connection (MCPWARP.md §5.9, §6.8), never for a stopped one.
    /// Called on the tunnel's task, so a 0 ms redial may already have
    /// started: keep it short. A panic is caught and logged.
    pub fn on_retry(mut self, f: impl Fn(&RetryInfo) + Send + Sync + 'static) -> Self {
        self.on_retry = Some(Arc::new(f));
        self
    }

    /// The application, as for [`ExposeConfigBuilder::handler`]. Exactly
    /// one of `handler` and `target`.
    pub fn handler<H: Handler + Clone>(mut self, handler: H) -> Self {
        self.expose = self.expose.handler(handler);
        self
    }

    /// Forwards requests to a local `http://` server, as for
    /// [`ExposeConfigBuilder::target`].
    #[cfg(feature = "target")]
    #[cfg_attr(docsrs, doc(cfg(feature = "target")))]
    pub fn target(mut self, url: impl Into<String>) -> Self {
        self.expose = self.expose.target(url);
        self
    }

    /// The `target` response-header timeout, as for
    /// [`ExposeConfigBuilder::target_timeout`].
    #[cfg(feature = "target")]
    #[cfg_attr(docsrs, doc(cfg(feature = "target")))]
    pub fn target_timeout(mut self, timeout: Duration) -> Self {
        self.expose = self.expose.target_timeout(timeout);
        self
    }

    /// Runs every connection on `edge` instead of the network, for tests.
    #[cfg(feature = "test-util")]
    #[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
    pub fn fake_edge(mut self, edge: &crate::test_util::FakeEdge) -> Self {
        let edge = edge.clone();
        self.connector = Some(Arc::new(move || Box::new(edge.connector())));
        self
    }

    /// Validates the up-front options (MCPWARP.md §6.1): §5.2 rows 1-4
    /// (handler or target), 6-7 (integration) and 10 (agent), with §5.2's
    /// messages. Everything else is checked when a connection starts and
    /// shows up as a status.
    pub fn build(self) -> Result<TunnelOptions, ConfigError> {
        let template = self.expose.token(PLACEHOLDER).name(PLACEHOLDER).build()?;
        let reconnect_error = actor::check_reconnect(&self.reconnect, &template);
        Ok(TunnelOptions {
            template,
            reconnect: self.reconnect,
            reconnect_error,
            on_retry: self.on_retry,
            connector: self
                .connector
                .unwrap_or_else(|| Arc::new(|| Box::new(ClientConnector))),
        })
    }
}

/// A long-lived controller over [`expose()`](crate::expose()) for hosts whose
/// settings change at runtime (MCPWARP.md §6).
///
/// Cloning is cheap; clones share the tunnel. Dropping the last clone
/// disposes it in the background, as [`dispose`](Self::dispose) with
/// [`DEFAULT_DISPOSE_TIMEOUT`]. That needs the runtime to keep running: a
/// runtime shut down meanwhile cuts the `unregister` and close short, so
/// before shutting it down, await `dispose()`.
///
/// ```no_run
/// use bytes::Bytes;
/// use http_body_util::Full;
/// use ws_mixer_mcpwarp::bridge::handler_fn;
/// use ws_mixer_mcpwarp::{Tunnel, TunnelConfig, TunnelOptions};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let tunnel = Tunnel::new(
///     TunnelOptions::builder()
///         .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
///         .handler(handler_fn(|_req| async {
///             Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
///         }))
///         .build()?,
/// )?;
/// let _subscription = tunnel.subscribe(|status| println!("{status:?}"));
/// tunnel
///     .configure(TunnelConfig {
///         enabled: true,
///         token: Some(std::env::var("MCPWARP_TOKEN")?),
///         name: Some("notes".into()),
///         ..TunnelConfig::default()
///     })
///     .await;
/// // ... on every settings change: tunnel.configure(new_config).await;
/// tunnel.dispose(ws_mixer_mcpwarp::DEFAULT_DISPOSE_TIMEOUT)?.await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Tunnel {
    inner: Arc<Inner>,
}

struct Inner {
    tx: mpsc::UnboundedSender<Msg>,
    hub: Arc<StatusHub>,
    dispose_sent: AtomicBool,
    disposed: watch::Receiver<bool>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Dropped);
    }
}

impl fmt::Debug for Tunnel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tunnel")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl Tunnel {
    /// Creates a stopped (`off`) tunnel. Its task runs on the current tokio
    /// runtime.
    ///
    /// # Errors
    ///
    /// Only when called outside a tokio runtime. Invalid options were
    /// rejected by [`TunnelOptionsBuilder::build`]; problems with a config
    /// (an invalid name, say) show up as state `error`.
    pub fn new(options: TunnelOptions) -> Result<Self, ConfigError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| ConfigError::no_runtime())?;
        let hub = Arc::new(StatusHub::new());
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = watch::channel(false);
        runtime.spawn(Actor::new(options, hub.clone(), tx.clone(), rx, done_tx).run());
        Ok(Self {
            inner: Arc::new(Inner {
                tx,
                hub,
                dispose_sent: AtomicBool::new(false),
                disposed: done_rx,
            }),
        })
    }

    /// Applies `config` (MCPWARP.md §6.2).
    ///
    /// The call is queued when it is made, before the returned future is
    /// polled: calls take effect in call order and only the last of several
    /// back-to-back calls dials. Equal to the last config (same `enabled`,
    /// trimmed token or the same provider `Arc`, name, url and kind) is a
    /// no-op unless a `stop`, a
    /// conflict or an error came in between. Otherwise the current
    /// connection is stopped (unregister, up to 2 s, then the client's
    /// drain, up to 5 s more) and, when enabled with a token, a new one
    /// starts.
    ///
    /// The future resolves once the old connection is stopped and the new
    /// one is dialing (not when it is online); it never fails, and awaiting
    /// it is optional.
    pub fn configure(&self, config: TunnelConfig) -> impl Future<Output = ()> + Send + 'static {
        let (tx, rx) = oneshot::channel();
        let _ = self.inner.tx.send(Msg::Configure(config, tx));
        async move {
            let _ = rx.await;
        }
    }

    /// Stops the connection and goes `off` (MCPWARP.md §6.3). Queued like
    /// [`configure`](Self::configure); a later `configure`, even with the
    /// same config, starts again. Resolves once the connection has closed
    /// (about 7 s at worst); no event of it changes the status after that.
    pub fn stop(&self) -> impl Future<Output = ()> + Send + 'static {
        let (tx, rx) = oneshot::channel();
        let _ = self.inner.tx.send(Msg::Stop(tx));
        async move {
            let _ = rx.await;
        }
    }

    /// Disposes the tunnel for good (MCPWARP.md §6.3): status `off` goes to
    /// the listeners one last time, they are dropped, and later
    /// `configure`/`stop` calls do nothing. The future resolves once the
    /// connection has closed or `timeout` has passed, whichever is first; on
    /// timeout the connection keeps closing in the background.
    ///
    /// Memoized: every call shares the first call's result and timeout.
    ///
    /// # Errors
    ///
    /// A zero `timeout`.
    pub fn dispose(
        &self,
        timeout: Duration,
    ) -> Result<impl Future<Output = ()> + Send + 'static, ConfigError> {
        if timeout.is_zero() {
            return Err(ConfigError::dispose_timeout());
        }
        if !self.inner.dispose_sent.swap(true, Ordering::AcqRel) {
            let _ = self.inner.tx.send(Msg::Dispose(timeout));
        }
        let mut done = self.inner.disposed.clone();
        Ok(async move {
            let _ = done.wait_for(|d| *d).await;
        })
    }

    /// The current status, a copy.
    pub fn status(&self) -> TunnelStatus {
        self.inner.hub.status()
    }

    /// Calls `listener` now with the current status, then with every new
    /// status, in order (MCPWARP.md §6.4). An unchanged status is never
    /// delivered twice in a row.
    ///
    /// Listeners run on the tunnel's task (the immediate call on the
    /// caller's): keep them short. A panic is caught and logged as `status
    /// listener threw: <message>`; the other listeners are still called.
    /// Dropping the [`Subscription`] unsubscribes; a call already under way
    /// may still finish. After [`dispose`](Self::dispose), `listener` is
    /// called once with `off` and never again.
    pub fn subscribe(
        &self,
        listener: impl Fn(&TunnelStatus) + Send + Sync + 'static,
    ) -> Subscription {
        self.inner.hub.subscribe(Box::new(listener))
    }

    /// A receiver of the current status. Lossy: a slow reader sees only the
    /// latest status, so use [`subscribe`](Self::subscribe) to see every
    /// one. It keeps the last status after [`dispose`](Self::dispose).
    pub fn watch_status(&self) -> watch::Receiver<TunnelStatus> {
        self.inner.hub.watch.subscribe()
    }
}

/// A [`Tunnel::subscribe`] registration; dropping it unsubscribes.
#[must_use = "dropping the Subscription unsubscribes at once"]
pub struct Subscription {
    hub: Weak<StatusHub>,
    slot: Arc<Slot>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription").finish_non_exhaustive()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.slot.removed.store(true, Ordering::Release);
        if let Some(hub) = self.hub.upgrade() {
            // Dropped after the lock is released: a listener may own a
            // `Subscription`, whose drop takes the lock again.
            let removed: Vec<_> = {
                let mut st = hub.lock();
                let (gone, kept) = std::mem::take(&mut st.listeners)
                    .into_iter()
                    .partition(|s| s.id == self.slot.id);
                st.listeners = kept;
                gone
            };
            drop(removed);
        }
    }
}

type Listener = dyn Fn(&TunnelStatus) + Send + Sync;

/// One listener.
struct Slot {
    id: u64,
    f: Box<Listener>,
    /// The sequence number of the last status delivered: a status never
    /// reaches a listener after a newer one.
    delivered: Mutex<Option<u64>>,
    removed: AtomicBool,
}

impl Slot {
    fn deliver(&self, seq: u64, status: &TunnelStatus) {
        let mut last = lock(&self.delivered);
        if self.removed.load(Ordering::Acquire) || last.is_some_and(|l| l >= seq) {
            return;
        }
        *last = Some(seq);
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| (self.f)(status))) {
            tracing::error!(
                target: "mcpwarp",
                "status listener threw: {}",
                actor::panic_message(panic.as_ref())
            );
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The status and its listeners, behind one lock (MCPWARP.md §6.4).
pub(crate) struct StatusHub {
    state: Mutex<HubState>,
    watch: watch::Sender<TunnelStatus>,
}

struct HubState {
    status: TunnelStatus,
    /// Bumped on every change.
    seq: u64,
    listeners: Vec<Arc<Slot>>,
    next_id: u64,
    disposed: bool,
}

impl StatusHub {
    fn new() -> Self {
        Self {
            state: Mutex::new(HubState {
                status: TunnelStatus::default(),
                seq: 0,
                listeners: Vec::new(),
                next_id: 0,
                disposed: false,
            }),
            watch: watch::channel(TunnelStatus::default()).0,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HubState> {
        lock(&self.state)
    }

    pub(crate) fn status(&self) -> TunnelStatus {
        self.lock().status.clone()
    }

    /// Publishes `next` unless it equals the current status; listeners are
    /// called outside the lock. The actor is the only caller.
    pub(crate) fn set(&self, next: TunnelStatus) {
        let (seq, listeners) = {
            let mut st = self.lock();
            if st.status == next {
                return;
            }
            st.status = next.clone();
            st.seq += 1;
            (st.seq, st.listeners.clone())
        };
        self.watch.send_replace(next.clone());
        for slot in listeners {
            slot.deliver(seq, &next);
        }
    }

    /// Final: no listener is called after this.
    pub(crate) fn dispose(&self) {
        let removed = {
            let mut st = self.lock();
            st.disposed = true;
            std::mem::take(&mut st.listeners)
        };
        // Outside the lock: see `Subscription::drop`.
        drop(removed);
    }

    fn subscribe(self: &Arc<Self>, f: Box<Listener>) -> Subscription {
        let (status, seq, slot) = {
            let mut st = self.lock();
            st.next_id += 1;
            let slot = Arc::new(Slot {
                id: st.next_id,
                f,
                delivered: Mutex::new(None),
                removed: AtomicBool::new(false),
            });
            if !st.disposed {
                st.listeners.push(slot.clone());
            }
            (st.status.clone(), st.seq, slot)
        };
        slot.deliver(seq, &status);
        Subscription {
            hub: Arc::downgrade(self),
            slot,
        }
    }
}

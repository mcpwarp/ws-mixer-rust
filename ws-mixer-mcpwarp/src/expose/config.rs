//! [`ExposeConfig`] and its builder: the options of MCPWARP.md §5.1,
//! validated per §5.2.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use ws_mixer::{ReconnectPolicy, Stream, TlsConfig, Token};

use crate::DEFAULT_URL;
use crate::bridge::{DEFAULT_MAX_REQUEST_BODY_BYTES, Handler, ServeContext, serve_stream};
use crate::error::ConfigError;
use crate::identity::{Agent, Identity};
use crate::names::{ServiceKind, is_valid_integration_id, is_valid_service_name};
use crate::session::RetryInfo;
use crate::transport::BoxFut;

use super::observe::{ExposeObservation, Observe};

/// Serves one stream; cloneable so the tunnel can re-expose with the same
/// application.
pub(crate) trait ServeHandler: Send + 'static {
    fn serve(&self, stream: Stream, ctx: &ServeContext) -> BoxFut<'static, ()>;
    fn clone_box(&self) -> Box<dyn ServeHandler>;
}

impl<H: Handler + Clone> ServeHandler for H {
    fn serve(&self, stream: Stream, ctx: &ServeContext) -> BoxFut<'static, ()> {
        Box::pin(serve_stream(stream, ctx, self))
    }

    fn clone_box(&self) -> Box<dyn ServeHandler> {
        Box::new(self.clone())
    }
}

pub(crate) type OnRetry = dyn Fn(&RetryInfo) + Send + Sync;

/// A validated `expose()` configuration (MCPWARP.md §5.1). Built with
/// [`ExposeConfig::builder`]; cloning clones the handler and shares the
/// `on_retry` callback.
///
/// `Send` but not `Sync`: the handler need not be `Sync`. Move it, or clone
/// it per use.
pub struct ExposeConfig {
    pub(crate) url: String,
    pub(crate) token: Token,
    pub(crate) name: String,
    pub(crate) kind: ServiceKind,
    pub(crate) identity: Identity,
    pub(crate) reconnect: ReconnectPolicy,
    pub(crate) tls: Option<TlsConfig>,
    pub(crate) max_request_body_bytes: u64,
    pub(crate) on_retry: Option<Arc<OnRetry>>,
    pub(crate) handler: Box<dyn ServeHandler>,
    pub(crate) observe: Option<Arc<Observe>>,
}

impl Clone for ExposeConfig {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            token: self.token.clone(),
            name: self.name.clone(),
            kind: self.kind,
            identity: self.identity.clone(),
            reconnect: self.reconnect.clone(),
            tls: self.tls.clone(),
            max_request_body_bytes: self.max_request_body_bytes,
            on_retry: self.on_retry.clone(),
            handler: self.handler.clone_box(),
            observe: self.observe.clone(),
        }
    }
}

impl fmt::Debug for ExposeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The URL, token and name may be private; none is printed.
        f.debug_struct("ExposeConfig")
            .field("kind", &self.kind)
            .field("integration", &self.identity.integration())
            .field("reconnect", &self.reconnect)
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("on_retry", &self.on_retry.is_some())
            .finish_non_exhaustive()
    }
}

impl ExposeConfig {
    /// Starts a configuration. `token`, `name`, `integration` and one of
    /// `handler` / `target` are required.
    ///
    /// ```
    /// use bytes::Bytes;
    /// use http_body_util::Full;
    /// use ws_mixer_mcpwarp::ExposeConfig;
    /// use ws_mixer_mcpwarp::bridge::handler_fn;
    ///
    /// let config = ExposeConfig::builder()
    ///     .token("mcpwarp_pat_example")
    ///     .name("notes")
    ///     .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
    ///     .handler(handler_fn(|_req| async {
    ///         Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
    ///     }))
    ///     .build()?;
    /// # let _ = config;
    /// # Ok::<(), ws_mixer_mcpwarp::ConfigError>(())
    /// ```
    pub fn builder() -> ExposeConfigBuilder {
        ExposeConfigBuilder::default()
    }

    /// The service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The service kind.
    pub fn kind(&self) -> ServiceKind {
        self.kind
    }

    /// Sets the driver's observation hook, for the tunnel controller; see
    /// [`observe`](super::observe). Called from the driver task and from
    /// stream tasks.
    #[allow(dead_code, reason = "used by the tunnel controller (P4) and tests")]
    pub(crate) fn observe(
        mut self,
        f: impl Fn(&ExposeObservation) + Send + Sync + 'static,
    ) -> Self {
        self.observe = Some(Arc::new(f));
        self
    }
}

/// Builds an [`ExposeConfig`]; see [`ExposeConfig::builder`].
#[derive(Default)]
#[must_use = "a builder does nothing until `build` is called"]
pub struct ExposeConfigBuilder {
    url: Option<String>,
    token: Option<Token>,
    name: Option<String>,
    kind: ServiceKind,
    integration: Option<String>,
    agent: Agent,
    reconnect: ReconnectPolicy,
    tls: Option<TlsConfig>,
    max_request_body_bytes: Option<u64>,
    on_retry: Option<Arc<OnRetry>>,
    handler: Option<Box<dyn ServeHandler>>,
    #[cfg(feature = "target")]
    target: Option<String>,
    #[cfg(feature = "target")]
    target_timeout: Option<Duration>,
}

impl fmt::Debug for ExposeConfigBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExposeConfigBuilder")
            .field("kind", &self.kind)
            .field("handler", &self.handler.is_some())
            .finish_non_exhaustive()
    }
}

impl ExposeConfigBuilder {
    /// The edge's URL, `ws://` or `wss://`. Default [`DEFAULT_URL`]; the edge
    /// routes by `Host`, the path is ignored.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// The token: a PAT or connect-audience JWT, static or a provider called
    /// on every dial ([`Token::provider`]). Required; an empty static token
    /// is rejected.
    pub fn token(mut self, token: impl Into<Token>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// The service name, the public URL's slug
    /// ([`is_valid_service_name`](crate::is_valid_service_name)). Required.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// The service kind. Default [`ServiceKind::Http`].
    pub fn kind(mut self, kind: ServiceKind) -> Self {
        self.kind = kind;
        self
    }

    /// The integration's registration UUID, any case; sent lowercased.
    /// Required.
    pub fn integration(mut self, id: impl Into<String>) -> Self {
        self.integration = Some(id.into());
        self
    }

    /// Overrides `hello.agent.runtime` / `os` (each 1-64 printable ASCII
    /// characters).
    pub fn agent(mut self, agent: Agent) -> Self {
        self.agent = agent;
        self
    }

    /// The client's reconnect policy. `max_attempts` also bounds the
    /// attempts before the first registration.
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// The TLS configuration for a `wss://` URL (MCPWARP.md §8). Default:
    /// the transport's.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// The request body cap. Default
    /// [`DEFAULT_MAX_REQUEST_BODY_BYTES`](crate::bridge::DEFAULT_MAX_REQUEST_BODY_BYTES)
    /// (4 MiB); larger bodies get `413`.
    pub fn max_request_body_bytes(mut self, max: u64) -> Self {
        self.max_request_body_bytes = Some(max);
        self
    }

    /// Called before every scheduled reconnect and every `register` retry
    /// (MCPWARP.md §5.9). Called on the driver task: keep it short. A panic
    /// is caught and reported as [`ExposeEvent::HandlerError`](crate::ExposeEvent::HandlerError).
    pub fn on_retry(mut self, f: impl Fn(&RetryInfo) + Send + Sync + 'static) -> Self {
        self.on_retry = Some(Arc::new(f));
        self
    }

    /// The application: any [`Handler`], e.g. a tower service or
    /// [`handler_fn`](crate::bridge::handler_fn). Exactly one of `handler`
    /// and `target`.
    pub fn handler<H: Handler + Clone>(mut self, handler: H) -> Self {
        self.handler = Some(Box::new(handler));
        self
    }

    /// Forwards requests to a local `http://` server instead of a handler.
    /// `https://` is not supported and is rejected by
    /// [`build`](Self::build).
    #[cfg(feature = "target")]
    #[cfg_attr(docsrs, doc(cfg(feature = "target")))]
    pub fn target(mut self, url: impl Into<String>) -> Self {
        self.target = Some(url.into());
        self
    }

    /// How long a `target` has to send response headers before `504`.
    /// Default [`DEFAULT_TARGET_TIMEOUT`](crate::bridge::DEFAULT_TARGET_TIMEOUT)
    /// (30 s); positive and at most 2147483647 ms. Only with `target`.
    #[cfg(feature = "target")]
    #[cfg_attr(docsrs, doc(cfg(feature = "target")))]
    pub fn target_timeout(mut self, timeout: Duration) -> Self {
        self.target_timeout = Some(timeout);
        self
    }

    /// Validates per MCPWARP.md §5.2, first failure wins: 1 handler xor
    /// target, 2 target timeout only with target, 3 target URL (`http://`
    /// only), 4 target timeout range, 5 name, 6-7 integration, 9 token, 10
    /// agent, 11 reconnect. Row 8 (`kind`) cannot fail in Rust. No message
    /// carries the offending value.
    pub fn build(self) -> Result<ExposeConfig, ConfigError> {
        let handler = self.application()?;
        let name = self.name.filter(|n| is_valid_service_name(n));
        let Some(name) = name else {
            return Err(ConfigError::name());
        };
        let integration = self.integration.unwrap_or_default();
        if integration.is_empty() {
            return Err(ConfigError::integration_missing());
        }
        if !is_valid_integration_id(&integration) {
            return Err(ConfigError::integration_syntax());
        }
        let token = match self.token {
            Some(Token::Static(s)) if s.is_empty() => None,
            t => t,
        };
        let Some(token) = token else {
            return Err(ConfigError::token());
        };
        let identity = Identity::new(&integration, &self.agent)?;
        check_reconnect(&self.reconnect)?;
        Ok(ExposeConfig {
            url: self.url.unwrap_or_else(|| DEFAULT_URL.to_owned()),
            token,
            name,
            kind: self.kind,
            identity,
            reconnect: self.reconnect,
            tls: self.tls,
            max_request_body_bytes: self
                .max_request_body_bytes
                .unwrap_or(DEFAULT_MAX_REQUEST_BODY_BYTES),
            on_retry: self.on_retry,
            handler,
            observe: None,
        })
    }

    /// Rows 1-4.
    #[cfg(feature = "target")]
    fn application(&self) -> Result<Box<dyn ServeHandler>, ConfigError> {
        use crate::bridge::{DEFAULT_TARGET_TIMEOUT, Target};

        match (&self.handler, &self.target) {
            (Some(_), Some(_)) | (None, None) => return Err(ConfigError::handler_or_target()),
            (Some(h), None) => {
                if self.target_timeout.is_some() {
                    return Err(ConfigError::target_timeout_without_target());
                }
                return Ok(h.clone_box());
            }
            (None, Some(_)) => {}
        }
        let target = self.target.as_deref().unwrap_or_default();
        let uri = target
            .parse::<http::Uri>()
            .ok()
            .filter(|u| u.authority().is_some())
            .ok_or_else(ConfigError::target_url)?;
        if uri.scheme() == Some(&http::uri::Scheme::HTTPS) {
            return Err(ConfigError::target_https());
        }
        if uri.scheme() != Some(&http::uri::Scheme::HTTP) {
            return Err(ConfigError::target_url());
        }
        let timeout = self.target_timeout.unwrap_or(DEFAULT_TARGET_TIMEOUT);
        if timeout.is_zero() || timeout > MAX_TARGET_TIMEOUT {
            return Err(ConfigError::target_timeout_range(
                timeout.as_secs_f64() * 1000.0,
            ));
        }
        Ok(Box::new(Target::new(uri, timeout)))
    }

    /// Row 1.
    #[cfg(not(feature = "target"))]
    fn application(&self) -> Result<Box<dyn ServeHandler>, ConfigError> {
        self.handler
            .as_ref()
            .map(|h| h.clone_box())
            .ok_or_else(ConfigError::handler_or_target)
    }
}

/// `2^31 - 1` ms, the spec's `targetTimeoutMs` bound.
#[cfg(feature = "target")]
const MAX_TARGET_TIMEOUT: Duration = Duration::from_millis(2_147_483_647);

/// The transport's own bounds (`ClientBuilder::connect`), checked here so
/// they fail as §5.2 row 11 rather than at dial time.
const MAX_BACKOFF: Duration = Duration::from_secs(24 * 3600);
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(3600);

/// Row 11, keys in the spec's (JS) spelling.
fn check_reconnect(p: &ReconnectPolicy) -> Result<(), ConfigError> {
    let positive = |d: Duration, max: Duration| !d.is_zero() && d <= max;
    if !positive(p.base, MAX_BACKOFF) {
        return Err(ConfigError::reconnect(
            "base",
            "a positive duration of at most 24 hours",
        ));
    }
    if !positive(p.cap, MAX_BACKOFF) || p.cap < p.base {
        return Err(ConfigError::reconnect(
            "cap",
            "a positive duration of at most 24 hours, not less than `reconnect.base`",
        ));
    }
    if !positive(p.connect_timeout, MAX_CONNECT_TIMEOUT) {
        return Err(ConfigError::reconnect(
            "connectTimeout",
            "a positive duration of at most 1 hour",
        ));
    }
    if p.stable_after > MAX_BACKOFF {
        return Err(ConfigError::reconnect(
            "stableAfter",
            "a duration of at most 24 hours",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::handler_fn;
    use bytes::Bytes;
    use http_body_util::Full;

    const ID: &str = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";

    fn app() -> impl Handler + Clone + Sync {
        handler_fn(|_req| async { Ok(http::Response::new(Full::new(Bytes::new()))) })
    }

    fn valid() -> ExposeConfigBuilder {
        ExposeConfig::builder()
            .token("t")
            .name("notes")
            .integration(ID)
            .handler(app())
    }

    fn err(b: ExposeConfigBuilder) -> String {
        b.build().unwrap_err().to_string()
    }

    #[test]
    fn defaults() {
        let c = valid().build().unwrap();
        assert_eq!(c.url, DEFAULT_URL);
        assert_eq!(c.kind, ServiceKind::Http);
        assert_eq!(c.max_request_body_bytes, 4 * 1024 * 1024);
        assert_eq!(c.identity.integration(), ID);
        let c = valid()
            .integration(ID.to_uppercase())
            .kind(ServiceKind::Stdio)
            .build()
            .unwrap();
        assert_eq!(c.identity.integration(), ID);
        assert_eq!(c.clone().kind(), ServiceKind::Stdio);
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("notes") && !dbg.contains("wss://"), "{dbg}");
    }

    #[test]
    fn row_1_handler_xor_target() {
        let none = ExposeConfig::builder()
            .token("t")
            .name("notes")
            .integration(ID);
        assert_eq!(
            err(none),
            "mcpwarp: pass exactly one of `handler` or `target`"
        );
        #[cfg(feature = "target")]
        assert_eq!(
            err(valid().target("http://127.0.0.1:1/mcp")),
            "mcpwarp: pass exactly one of `handler` or `target`"
        );
    }

    #[cfg(feature = "target")]
    #[test]
    fn rows_2_to_4_target() {
        let target = || {
            ExposeConfig::builder()
                .token("t")
                .name("notes")
                .integration(ID)
        };
        assert_eq!(
            err(valid().target_timeout(Duration::from_secs(1))),
            "mcpwarp: `targetTimeoutMs` applies only to `target`"
        );
        for bad in ["not a url", "localhost:3000", "/mcp", "ftp://h/x"] {
            assert_eq!(
                err(target().target(bad)),
                "mcpwarp: `target` is not a valid URL",
                "{bad}"
            );
        }
        let https = "mcpwarp: `target` must be an http:// URL; https:// is not supported";
        assert_eq!(err(target().target("https://127.0.0.1/mcp")), https);
        // Row 3 before row 4, row 4 before row 5.
        assert_eq!(
            err(target().target("https://h/").target_timeout(Duration::ZERO)),
            https
        );
        assert_eq!(
            err(target()
                .name("Bad")
                .target("http://h/")
                .target_timeout(Duration::ZERO)),
            "mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got 0"
        );
        assert_eq!(
            err(target()
                .target("http://h/")
                .target_timeout(Duration::from_millis(2_147_483_648))),
            "mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got 2147483648"
        );
        assert!(
            target()
                .target("http://h/")
                .target_timeout(Duration::from_micros(1500))
                .build()
                .is_ok()
        );
        assert!(
            target()
                .target("http://127.0.0.1:8080/mcp?x=1")
                .target_timeout(Duration::from_millis(2_147_483_647))
                .build()
                .is_ok()
        );
    }

    #[test]
    fn rows_5_to_11_in_order() {
        let name = "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, at most 30 characters";
        let missing = "mcpwarp: `integration` (your integration's registration UUID) is required";
        let syntax = "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else";
        let token = "mcpwarp: `token` is required";
        let agent = "mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters";
        let bad_agent = Agent {
            runtime: Some(String::new()),
            os: None,
        };
        let no_token = || {
            ExposeConfig::builder()
                .name("notes")
                .integration(ID)
                .handler(app())
        };

        for bad in ["", "Notes", "-a", "a-", &"a".repeat(31)] {
            assert_eq!(err(valid().name(bad)), name, "{bad:?}");
        }
        let unnamed = ExposeConfig::builder()
            .token("t")
            .integration(ID)
            .handler(app());
        assert_eq!(err(unnamed), name);
        assert_eq!(err(valid().name("Bad").integration("")), name);
        let no_integration = ExposeConfig::builder()
            .token("t")
            .name("notes")
            .handler(app());
        assert_eq!(err(no_integration), missing);
        assert_eq!(err(valid().integration("")), missing);
        assert_eq!(err(valid().integration("secret").token("")), syntax);
        assert_eq!(err(no_token().agent(bad_agent.clone())), token);
        assert_eq!(err(no_token().token("")), token);
        assert_eq!(err(valid().agent(bad_agent)), agent);
        let os = Agent {
            runtime: None,
            os: Some("\u{e9}".into()),
        };
        assert_eq!(
            err(valid().agent(os)),
            "mcpwarp: `agent.os` must be 1-64 printable ASCII characters"
        );
        // A provider is never "empty".
        let provider = ws_mixer::Token::provider(|| async { Ok(String::new()) });
        assert!(no_token().token(provider).build().is_ok());
    }

    #[test]
    fn row_11_reconnect() {
        type Tweak = fn(&mut ReconnectPolicy);
        let policy = |f: Tweak| {
            let mut p = ReconnectPolicy::default();
            f(&mut p);
            p
        };
        let cases: [(Tweak, &str); 6] = [
            (
                |p| p.base = Duration::ZERO,
                "mcpwarp: `reconnect.base` must be a positive duration of at most 24 hours",
            ),
            (
                |p| p.base = Duration::from_secs(24 * 3600 + 1),
                "mcpwarp: `reconnect.base` must be a positive duration of at most 24 hours",
            ),
            (
                |p| p.cap = Duration::from_millis(500),
                "mcpwarp: `reconnect.cap` must be a positive duration of at most 24 hours, not less than `reconnect.base`",
            ),
            (
                |p| p.connect_timeout = Duration::ZERO,
                "mcpwarp: `reconnect.connectTimeout` must be a positive duration of at most 1 hour",
            ),
            (
                |p| p.connect_timeout = Duration::from_secs(3601),
                "mcpwarp: `reconnect.connectTimeout` must be a positive duration of at most 1 hour",
            ),
            (
                |p| p.stable_after = Duration::from_secs(24 * 3600 + 1),
                "mcpwarp: `reconnect.stableAfter` must be a duration of at most 24 hours",
            ),
        ];
        for (f, msg) in cases {
            assert_eq!(err(valid().reconnect(policy(f))), msg);
        }
        // Row 10 wins over row 11.
        let agent = Agent {
            runtime: None,
            os: Some(String::new()),
        };
        assert_eq!(
            err(valid()
                .agent(agent)
                .reconnect(policy(|p| p.base = Duration::ZERO))),
            "mcpwarp: `agent.os` must be 1-64 printable ASCII characters"
        );
        assert!(
            valid()
                .reconnect(policy(|p| p.stable_after = Duration::ZERO))
                .build()
                .is_ok()
        );
        assert!(
            valid()
                .reconnect(ReconnectPolicy::disabled())
                .build()
                .is_ok()
        );
    }
}

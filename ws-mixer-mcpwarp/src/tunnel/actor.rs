//! The tunnel's task: commands, generations, the apply queue and the status
//! mapping of MCPWARP.md §6.2-§6.9.
//!
//! Every message (a host call, or an observation of a running exposure)
//! arrives on one channel, so the actor sees them in the order they
//! happened. On each wake it first takes every queued message, which does
//! what the spec's synchronous part of a call does (validation, the desired
//! config, stopping the current generation, the status), and only then
//! moves the apply queue, so of several back-to-back `configure` calls only
//! the last dials (§6.2 step 4).
//!
//! A generation is one `expose()` run on its own task. `detach` ends it at
//! once by bumping `generation` (anything it still reports is stale, §6.8)
//! and tells its task to close it; the queue waits for those closes
//! ("draining") before the next start.

use std::any::Any;
use std::collections::{BTreeSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use ws_mixer::{BuildError, ClientBuilder, Phase, ReconnectPolicy, Token, TokenProvider};

use super::status::{TunnelState, TunnelStatus, TunnelStatusCode, last_error};
use super::{DEFAULT_DISPOSE_TIMEOUT, StatusHub, TunnelConfig, TunnelOptions, lock};
use crate::DEFAULT_URL;
use crate::bridge::handler_fn;
use crate::codes::{AppErrorCode, RegisterErrorCode};
use crate::error::{ConfigError, McpwarpError, McpwarpErrorCode};
use crate::expose::observe::{ExposeObservation, ObservedError, RetryKind};
use crate::expose::{ExposeConfig, ExposeHandle, expose_with};
use crate::names::is_valid_service_name;
use crate::session::ExposeState;
use crate::transport::{Connector, Transport};

/// What the actor receives.
pub(crate) enum Msg {
    Configure(TunnelConfig, oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
    Dispose(Duration),
    /// The last `Tunnel` clone was dropped.
    Dropped,
    /// An observation of generation `epoch`'s exposure.
    Observed {
        epoch: u64,
        obs: ExposeObservation,
    },
    /// Generation `epoch`'s client could not be built (§6.7 T8/T9).
    StartFailed {
        epoch: u64,
        error: McpwarpError,
        internal: bool,
    },
    /// Generation `epoch`'s connection has closed.
    Closed {
        epoch: u64,
    },
}

/// A status code with its `lastError` (§6.6).
#[derive(Copy, Clone, Debug)]
struct Problem {
    code: TunnelStatusCode,
    last_error: &'static str,
}

const fn problem(code: TunnelStatusCode, last_error: &'static str) -> Problem {
    Problem { code, last_error }
}

const RETRY_DIAL: Problem = problem(TunnelStatusCode::Retrying, last_error::RETRY_DIAL);
const RETRY_REGISTER: Problem = problem(TunnelStatusCode::Retrying, last_error::RETRY_REGISTER);
const LOST: Problem = problem(TunnelStatusCode::Disconnected, last_error::DISCONNECTED);
const TAKEN_OVER: Problem = problem(TunnelStatusCode::TakenOver, last_error::TAKEN_OVER);
const DISABLED: Problem = problem(TunnelStatusCode::Disabled, last_error::DISABLED);
const INVALID_NAME: Problem = problem(TunnelStatusCode::InvalidName, last_error::INVALID_NAME);
const INVALID_SETTINGS: Problem =
    problem(TunnelStatusCode::InvalidConfig, last_error::INVALID_CONFIG);
const INTERNAL: Problem = problem(TunnelStatusCode::Internal, last_error::INTERNAL);

fn with_problem(state: TunnelState, p: Problem) -> TunnelStatus {
    TunnelStatus {
        code: Some(p.code),
        last_error: Some(p.last_error.to_owned()),
        ..TunnelStatus::new(state)
    }
}

/// What a fatal error means for the user (§6.6); the technical message goes
/// to the log.
fn describe_problem(code: &McpwarpErrorCode) -> Problem {
    use TunnelStatusCode as C;
    match code {
        McpwarpErrorCode::Unauthorized
        | McpwarpErrorCode::Register(RegisterErrorCode::Unauthenticated) => {
            problem(C::Unauthorized, last_error::TOKEN_REJECTED)
        }
        McpwarpErrorCode::Register(RegisterErrorCode::Forbidden) => {
            problem(C::Unauthorized, last_error::TOKEN_FORBIDDEN)
        }
        McpwarpErrorCode::TakenOver => TAKEN_OVER,
        McpwarpErrorCode::Register(RegisterErrorCode::InvalidName) => INVALID_NAME,
        McpwarpErrorCode::Register(RegisterErrorCode::UsernameRequired) => {
            problem(C::UsernameRequired, last_error::USERNAME_REQUIRED)
        }
        McpwarpErrorCode::Register(RegisterErrorCode::QuotaExceeded) => {
            problem(C::QuotaExceeded, last_error::QUOTA_EXCEEDED)
        }
        McpwarpErrorCode::Register(RegisterErrorCode::Conflict) => {
            problem(C::Conflict, last_error::CONFLICT)
        }
        McpwarpErrorCode::App(AppErrorCode::UnsupportedVersion) => {
            problem(C::UnsupportedVersion, last_error::UNSUPPORTED_VERSION)
        }
        McpwarpErrorCode::DisconnectedFatal => {
            problem(C::DisconnectedFatal, last_error::DISCONNECTED_FATAL)
        }
        _ => INTERNAL,
    }
}

/// A message for the log: no `mcpwarp: ` prefix (§6.9).
fn plain(message: &str) -> &str {
    message.strip_prefix("mcpwarp: ").unwrap_or(message)
}

/// `<CODE>: <message>` with the error's own code (§6.9).
fn technical(e: &McpwarpError) -> String {
    format!("{}: {}", e.code, plain(&e.message))
}

/// A duration as JS prints a millisecond count: `1000`, `1.5`.
pub(crate) fn fmt_ms(d: Duration) -> String {
    if d.subsec_nanos() % 1_000_000 == 0 {
        d.as_millis().to_string()
    } else {
        (d.as_secs_f64() * 1000.0).to_string()
    }
}

pub(crate) fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}

/// JS `String.prototype.trim`: Unicode white space and the BOM, not NEL.
pub(crate) fn js_trim(s: &str) -> &str {
    s.trim_matches(|c: char| (c.is_whitespace() && c != '\u{85}') || c == '\u{feff}')
}

/// A config's token (§6.2): its provider when set, else `token`.
#[derive(Clone, Copy)]
enum ConfigToken<'a> {
    Unset,
    Text(&'a str),
    Provider(&'a Arc<dyn TokenProvider>),
}

fn config_token(c: &TunnelConfig) -> ConfigToken<'_> {
    match &c.token_provider {
        Some(Token::Provider(p)) => ConfigToken::Provider(p),
        Some(Token::Static(t)) => ConfigToken::Text(t),
        None => c
            .token
            .as_deref()
            .map_or(ConfigToken::Unset, ConfigToken::Text),
    }
}

/// The token a start hands the transport. A provider goes as is: called on
/// every dial.
pub(super) fn expose_token(c: &TunnelConfig) -> Token {
    match config_token(c) {
        ConfigToken::Provider(p) => Token::Provider(p.clone()),
        ConfigToken::Text(t) => Token::from(js_trim(t)),
        ConfigToken::Unset => Token::from(""),
    }
}

/// §6.2 check 3: disabled or a blank token is `off`.
fn will_start(c: &TunnelConfig) -> bool {
    c.enabled
        && match config_token(c) {
            ConfigToken::Unset => false,
            ConfigToken::Text(t) => !js_trim(t).is_empty(),
            ConfigToken::Provider(_) => true,
        }
}

/// §6.2 checks 4 and 5 (1, 2, 6 and 7 cannot fail in Rust): the log detail
/// and the problem.
fn invalid(c: &TunnelConfig) -> Option<(&'static str, Problem)> {
    let token = config_token(c);
    if !c.enabled || matches!(token, ConfigToken::Text(t) if js_trim(t).is_empty()) {
        return None;
    }
    if matches!(token, ConfigToken::Unset) {
        return Some((
            "`token` must be a string or a provider",
            problem(TunnelStatusCode::InvalidConfig, last_error::TOKEN_NOT_SET),
        ));
    }
    if c.name.is_none() {
        return Some((
            "`name` must be a string",
            problem(TunnelStatusCode::InvalidConfig, last_error::NAME_NOT_SET),
        ));
    }
    None
}

/// §6.2 step 3: a string token trimmed, a provider by identity.
fn same_config(a: &TunnelConfig, b: &TunnelConfig) -> bool {
    let same_token = match (config_token(a), config_token(b)) {
        (ConfigToken::Unset, ConfigToken::Unset) => true,
        (ConfigToken::Text(x), ConfigToken::Text(y)) => js_trim(x) == js_trim(y),
        (ConfigToken::Provider(x), ConfigToken::Provider(y)) => Arc::ptr_eq(x, y),
        _ => false,
    };
    a.enabled == b.enabled && same_token && a.name == b.name && a.url == b.url && a.kind == b.kind
}

/// §5.2 row 11 for the tunnel's policy, checked with `template`'s valid
/// integration so only the policy can fail.
pub(super) fn check_reconnect(p: &ReconnectPolicy, template: &ExposeConfig) -> Option<ConfigError> {
    ExposeConfig::builder()
        .token("t")
        .name("probe")
        .integration(template.identity.integration())
        .handler(handler_fn(|_req| async {
            Ok(http::Response::new(Full::new(Bytes::new())))
        }))
        .reconnect(p.clone())
        .build()
        .err()
}

macro_rules! log {
    (info, $($arg:tt)*) => { tracing::info!(target: "mcpwarp", $($arg)*) };
    (warn, $($arg:tt)*) => { tracing::warn!(target: "mcpwarp", $($arg)*) };
    (error, $($arg:tt)*) => { tracing::error!(target: "mcpwarp", $($arg)*) };
}

/// The desired config (§6.2): `id` stands for JS object identity.
struct Desired {
    id: u64,
    config: TunnelConfig,
}

enum TaskKind {
    /// Apply the desired config `id`, if it still is.
    Apply(u64),
    /// Wait for the closes started so far (`stop`, an invalid config).
    Drain,
}

struct Task {
    seq: u64,
    kind: TaskKind,
    waiters: Vec<oneshot::Sender<()>>,
    /// Set once started: the last generation detached by then, whose close
    /// (and every earlier one) the task waits for.
    upto: Option<u64>,
}

/// The running generation.
struct Active {
    epoch: u64,
    config_id: u64,
    name: String,
    abort: oneshot::Sender<()>,
    /// The exposure is closing on its own (a fatal error, `Closing` seen):
    /// only its `Close` matters now.
    ending: bool,
    /// Its task has ended: there is no close to wait for.
    closed: bool,
}

struct Disposing {
    deadline: Instant,
    timeout: Duration,
    done: bool,
}

pub(crate) struct Actor {
    options: TunnelOptions,
    hub: Arc<StatusHub>,
    tx: mpsc::UnboundedSender<Msg>,
    rx: mpsc::UnboundedReceiver<Msg>,
    /// Bumped by every start and every detach; an observation counts only
    /// if it carries the current value.
    generation: u64,
    desired: Option<Desired>,
    next_config_id: u64,
    active: Option<Active>,
    /// Detached generations whose close is under way.
    closing: BTreeSet<u64>,
    last_detached: u64,
    queue: VecDeque<Task>,
    next_seq: u64,
    /// The last task done; tasks finish in order.
    completed: u64,
    /// The apply task of `desired`, which a repeat of it waits for.
    applying: u64,
    disposed: bool,
    dispose: Option<Disposing>,
    done: watch::Sender<bool>,
}

impl Actor {
    pub(crate) fn new(
        options: TunnelOptions,
        hub: Arc<StatusHub>,
        tx: mpsc::UnboundedSender<Msg>,
        rx: mpsc::UnboundedReceiver<Msg>,
        done: watch::Sender<bool>,
    ) -> Self {
        Self {
            options,
            hub,
            tx,
            rx,
            generation: 0,
            desired: None,
            next_config_id: 0,
            active: None,
            closing: BTreeSet::new(),
            last_detached: 0,
            queue: VecDeque::new(),
            next_seq: 0,
            completed: 0,
            applying: 0,
            disposed: false,
            dispose: None,
            done,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            if self.finished() {
                return;
            }
            let deadline = self
                .dispose
                .as_ref()
                .filter(|d| !d.done)
                .map(|d| d.deadline);
            tokio::select! {
                msg = self.rx.recv() => {
                    let Some(msg) = msg else { return };
                    self.handle(msg);
                    while let Ok(msg) = self.rx.try_recv() {
                        self.handle(msg);
                    }
                }
                () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)),
                    if deadline.is_some() => self.dispose_timed_out(),
            }
            self.advance();
            self.check_disposed();
        }
    }

    /// Disposed, and nothing left to wait for. Unresolved waiters resolve
    /// when they are dropped.
    fn finished(&self) -> bool {
        self.dispose.as_ref().is_some_and(|d| d.done)
            && self.queue.is_empty()
            && self.closing.is_empty()
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Configure(config, waiter) => self.configure(config, waiter),
            Msg::Stop(waiter) => self.stop(waiter),
            Msg::Dispose(timeout) => self.start_dispose(timeout),
            Msg::Dropped => {
                if self.dispose.is_none() {
                    self.start_dispose(DEFAULT_DISPOSE_TIMEOUT);
                }
            }
            Msg::Observed { epoch, obs } => {
                if epoch == self.generation {
                    self.observe(obs);
                }
            }
            Msg::StartFailed {
                epoch,
                error,
                internal,
            } => {
                if epoch == self.generation {
                    let p = if internal { INTERNAL } else { INVALID_SETTINGS };
                    self.fail(p, format!("{}: {}", p.code, plain(&error.message)));
                }
            }
            Msg::Closed { epoch } => {
                self.closing.remove(&epoch);
                // Gone before it was detached (its task panicked).
                if let Some(a) = self.active.as_mut().filter(|a| a.epoch == epoch) {
                    a.closed = true;
                }
            }
        }
    }

    // --- host calls ----------------------------------------------------------

    fn configure(&mut self, config: TunnelConfig, waiter: oneshot::Sender<()>) {
        if self.disposed {
            let _ = waiter.send(());
            return;
        }
        if let Some((detail, p)) = invalid(&config) {
            log!(error, "{}: {detail}", p.code);
            self.desired = None;
            self.detach();
            self.set_status(with_problem(TunnelState::Error, p));
            self.enqueue(TaskKind::Drain, waiter);
            return;
        }
        if self
            .desired
            .as_ref()
            .is_some_and(|d| same_config(&d.config, &config))
        {
            self.wait_for(self.applying, waiter);
            return;
        }
        self.next_config_id += 1;
        let id = self.next_config_id;
        self.desired = Some(Desired { id, config });
        self.applying = self.enqueue(TaskKind::Apply(id), waiter);
    }

    fn stop(&mut self, waiter: oneshot::Sender<()>) {
        if self.disposed {
            let _ = waiter.send(());
            return;
        }
        self.desired = None;
        self.detach();
        self.set_status(TunnelStatus::new(TunnelState::Off));
        self.enqueue(TaskKind::Drain, waiter);
    }

    fn start_dispose(&mut self, timeout: Duration) {
        self.disposed = true;
        self.desired = None;
        self.detach();
        self.set_status(TunnelStatus::new(TunnelState::Off));
        self.hub.dispose();
        self.dispose = Some(Disposing {
            deadline: Instant::now() + timeout,
            timeout,
            done: false,
        });
    }

    fn dispose_timed_out(&mut self) {
        if let Some(d) = &mut self.dispose {
            if !d.done {
                log!(
                    warn,
                    "dispose() gave up waiting after {} ms",
                    fmt_ms(d.timeout)
                );
                d.done = true;
                self.done.send_replace(true);
            }
        }
    }

    fn check_disposed(&mut self) {
        let drained = self.queue.is_empty() && self.closing.is_empty();
        if let Some(d) = &mut self.dispose {
            if !d.done && drained {
                d.done = true;
                self.done.send_replace(true);
            }
        }
    }

    // --- the queue -----------------------------------------------------------

    fn enqueue(&mut self, kind: TaskKind, waiter: oneshot::Sender<()>) -> u64 {
        self.next_seq += 1;
        self.queue.push_back(Task {
            seq: self.next_seq,
            kind,
            waiters: vec![waiter],
            upto: None,
        });
        self.next_seq
    }

    fn wait_for(&mut self, seq: u64, waiter: oneshot::Sender<()>) {
        match self.queue.iter_mut().find(|t| t.seq == seq) {
            Some(task) => task.waiters.push(waiter),
            None => {
                let _ = waiter.send(());
            }
        }
    }

    /// Runs queued tasks until one has to wait for a close.
    fn advance(&mut self) {
        while let Some(task) = self.queue.front() {
            let upto = match task.upto {
                Some(upto) => upto,
                None => {
                    if let TaskKind::Apply(id) = task.kind {
                        if let Some(config) = self.desired_config(id) {
                            self.detach();
                            let state = if will_start(&config) {
                                TunnelState::Connecting
                            } else {
                                TunnelState::Off
                            };
                            self.set_status(TunnelStatus::new(state));
                        }
                    }
                    // JS `await this.draining`: every close started so far.
                    let upto = self.last_detached;
                    if let Some(task) = self.queue.front_mut() {
                        task.upto = Some(upto);
                    }
                    upto
                }
            };
            if self.closing.range(..=upto).next().is_some() {
                return;
            }
            let Some(task) = self.queue.pop_front() else {
                return;
            };
            if let TaskKind::Apply(id) = task.kind {
                if let Some(config) = self.desired_config(id) {
                    if will_start(&config) {
                        self.start(id, config);
                    } else {
                        log!(info, "off");
                    }
                }
            }
            self.completed = task.seq;
            for waiter in task.waiters {
                let _ = waiter.send(());
            }
        }
    }

    /// The config `id`, while it is still the desired one.
    fn desired_config(&self, id: u64) -> Option<TunnelConfig> {
        if self.disposed {
            return None;
        }
        self.desired
            .as_ref()
            .filter(|d| d.id == id)
            .map(|d| d.config.clone())
    }

    // --- generations ---------------------------------------------------------

    fn start(&mut self, id: u64, config: TunnelConfig) {
        self.generation += 1;
        let epoch = self.generation;
        self.set_status(TunnelStatus::new(TunnelState::Connecting));
        let name = config.name.clone().unwrap_or_default();
        let quoted = serde_json::to_string(&name).unwrap_or_default();
        match config.url.as_deref().filter(|u| !u.is_empty()) {
            Some(url) => log!(info, "connecting as {quoted} via {url}"),
            None => log!(info, "connecting as {quoted}"),
        }
        if !is_valid_service_name(&name) {
            self.fail_start(id, INVALID_NAME, ConfigError::name().message());
            return;
        }
        if let Some(e) = self.options.reconnect_error.clone() {
            self.fail_start(id, INVALID_SETTINGS, e.message());
            return;
        }

        let mut expose = self.options.template.clone();
        expose.url = config.url.clone().unwrap_or_else(|| DEFAULT_URL.to_owned());
        expose.token = expose_token(&config);
        expose.name = name.clone();
        expose.kind = config.kind.unwrap_or_default();
        expose.reconnect = self.options.reconnect.clone();
        expose.on_retry = None;
        let (closed_tx, closed_rx) = oneshot::channel();
        let closed_tx = Mutex::new(Some(closed_tx));
        let tx = self.tx.clone();
        let expose = expose.observe(move |obs| {
            let last = matches!(obs, ExposeObservation::Close { .. });
            let _ = tx.send(Msg::Observed {
                epoch,
                obs: obs.clone(),
            });
            if last {
                if let Some(closed) = lock(&closed_tx).take() {
                    let _ = closed.send(());
                }
            }
        });
        let failure = Arc::new(Mutex::new(None));
        let connector = Box::new(Probe {
            inner: (self.options.connector)(),
            failure: failure.clone(),
        });
        let (abort_tx, abort_rx) = oneshot::channel();
        tokio::spawn(run_generation(
            epoch,
            Box::pin(expose_with(expose, connector)),
            abort_rx,
            closed_rx,
            failure,
            self.tx.clone(),
        ));
        self.active = Some(Active {
            epoch,
            config_id: id,
            name,
            abort: abort_tx,
            ending: false,
            closed: false,
        });
    }

    /// Ends the current generation now (§6.8 Generations) and closes its
    /// connection in the background.
    fn detach(&mut self) {
        self.generation += 1;
        let Some(active) = self.active.take() else {
            return;
        };
        log!(info, "stopping");
        let _ = active.abort.send(());
        if !active.closed {
            self.closing.insert(active.epoch);
            self.last_detached = active.epoch;
        }
    }

    /// Clears `desired` if it is still the config `id`.
    fn forget(&mut self, id: u64) {
        if self.desired.as_ref().is_some_and(|d| d.id == id) {
            self.desired = None;
        }
    }

    /// A start that failed before dialing (§6.7 T7, T8).
    fn fail_start(&mut self, id: u64, p: Problem, detail: &str) {
        log!(error, "{}: {}", p.code, plain(detail));
        self.forget(id);
        self.detach();
        self.set_status(with_problem(TunnelState::Error, p));
    }

    /// A fatal failure of the current generation (§6.8 Fatal).
    fn fail(&mut self, p: Problem, log_line: String) {
        if p.code == TunnelStatusCode::TakenOver {
            self.conflicted();
            return;
        }
        log!(error, "{log_line}");
        if let Some(id) = self.active.as_ref().map(|a| a.config_id) {
            self.forget(id);
        }
        self.detach();
        self.set_status(with_problem(TunnelState::Error, p));
    }

    fn fail_with(&mut self, e: &McpwarpError) {
        self.fail(describe_problem(&e.code), technical(e));
    }

    /// §6.7 T34, T45: sticky until the next `configure`.
    fn conflicted(&mut self) {
        let Some(active) = &self.active else {
            return;
        };
        let quoted = serde_json::to_string(&active.name).unwrap_or_default();
        log!(
            warn,
            "another connection took over the name {quoted}; not reconnecting until configure() is called again"
        );
        let id = active.config_id;
        self.forget(id);
        self.detach();
        self.set_status(with_problem(TunnelState::Conflict, TAKEN_OVER));
    }

    fn set_status(&self, status: TunnelStatus) {
        self.hub.set(status);
    }

    fn current(&self) -> TunnelStatus {
        self.hub.status()
    }

    fn connecting(&self) -> bool {
        self.current().state == TunnelState::Connecting
    }

    // --- observations of the current generation ------------------------------

    fn ending(&self) -> bool {
        self.active.as_ref().is_none_or(|a| a.ending)
    }

    fn observe(&mut self, obs: ExposeObservation) {
        match obs {
            ExposeObservation::State(ExposeState::Conflict) => self.conflicted(),
            ExposeObservation::State(ExposeState::Closing | ExposeState::Closed) => {
                if let Some(a) = &mut self.active {
                    a.ending = true;
                }
            }
            ExposeObservation::State(_) => {}
            // T50: the `Disabled` right behind it is the one snapshot.
            ExposeObservation::Registered {
                disabled_next: true,
                ..
            } => {}
            ExposeObservation::Registered { registration, .. } => {
                log!(info, "online at {}", registration.url);
                self.set_status(TunnelStatus {
                    url: Some(registration.url),
                    ..TunnelStatus::new(TunnelState::Online)
                });
            }
            ExposeObservation::Disabled { reason, .. } => {
                log!(warn, "the service was disabled ({reason})");
                self.set_status(with_problem(TunnelState::Disabled, DISABLED));
            }
            ExposeObservation::Enabled { .. } => {
                log!(info, "the service was enabled, registering again");
                self.set_status(TunnelStatus::new(TunnelState::Connecting));
            }
            ExposeObservation::Conflict(_) => self.conflicted(),
            ExposeObservation::Error(e) => self.on_error(e),
            ExposeObservation::Disconnect { reason, superseded } => {
                if reason.fatal || self.ending() {
                    return;
                }
                let connected = reason.phase == Phase::Connected;
                if superseded {
                    if connected {
                        log!(info, "the superseded connection ended: {}", reason.message);
                    } else {
                        log!(warn, "disconnected: {}", reason.message);
                    }
                    return;
                }
                log!(warn, "disconnected: {}", reason.message);
                let current = self.current();
                if connected {
                    self.set_status(with_problem(TunnelState::Connecting, LOST));
                } else if current.state == TunnelState::Connecting
                    && current.code != Some(TunnelStatusCode::Disconnected)
                {
                    // After a lost connection the failed redials keep saying so.
                    self.set_status(with_problem(TunnelState::Connecting, RETRY_DIAL));
                }
            }
            ExposeObservation::Retry { info, kind } => {
                if self.ending() {
                    return;
                }
                log!(
                    warn,
                    "retry #{} in {} ms: {}",
                    info.attempt,
                    fmt_ms(info.delay),
                    info.reason
                );
                // While online or disabled this is a drain replacement's
                // register; the old connection still serves (T16).
                if matches!(kind, RetryKind::Register | RetryKind::Overloaded) && self.connecting()
                {
                    self.set_status(with_problem(TunnelState::Connecting, RETRY_REGISTER));
                }
                if let Some(f) = &self.options.on_retry {
                    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| f(&info))) {
                        log!(warn, "on_retry panicked: {}", panic_message(panic.as_ref()));
                    }
                }
            }
            ExposeObservation::Close { error } => match error {
                Some(e) => self.fail_with(&e),
                None => self.fail(INTERNAL, "INTERNAL: the connection closed".into()),
            },
        }
    }

    fn on_error(&mut self, e: ObservedError) {
        match e {
            // Any refusal that is not retried is terminal: after an earlier
            // registration nothing would retry it (T17-T23, T40, T41).
            ObservedError::Register {
                error,
                transient: false,
                ..
            } => self.fail_with(&error),
            ObservedError::Register { error, .. } => log!(warn, "{}", technical(&error)),
            ObservedError::App { error, .. } | ObservedError::Malformed { error } => {
                log!(warn, "{}", technical(&error));
                // T25; T26 while online or disabled.
                if self.connecting() {
                    self.set_status(with_problem(TunnelState::Connecting, INTERNAL));
                }
            }
            // Fail at once, not at `Close` after the client's graceful
            // close: the detach makes that `Close` stale.
            ObservedError::Transport { error, .. } | ObservedError::Fatal { error } => {
                self.fail_with(&error);
            }
            // Handler errors never change the status.
            ObservedError::Handler(e) => log!(warn, "{e}"),
        }
    }
}

/// One generation's `expose()`: runs until the actor detaches it, then
/// closes it and reports when its connection has closed.
async fn run_generation(
    epoch: u64,
    mut exposing: std::pin::Pin<
        Box<dyn Future<Output = Result<ExposeHandle, McpwarpError>> + Send>,
    >,
    mut abort: oneshot::Receiver<()>,
    closed: oneshot::Receiver<()>,
    failure: Arc<Mutex<Option<bool>>>,
    tx: mpsc::UnboundedSender<Msg>,
) {
    // Reports `Closed` however the task ends, a panic included, so the
    // apply queue never waits on a generation that is gone.
    let _closed = ClosedGuard {
        epoch,
        tx: tx.clone(),
    };
    // `exposing` first: its first poll starts the client (the dial).
    let outcome = tokio::select! {
        biased;
        r = &mut exposing => Some(r),
        _ = &mut abort => None,
    };
    match outcome {
        // Dropping `expose()` before it settles closes its client (§5.8).
        None => drop(exposing),
        Some(Ok(handle)) => {
            let _ = (&mut abort).await;
            handle.close().await;
        }
        Some(Err(error)) => {
            let built = lock(&failure).take();
            if let Some(internal) = built {
                let _ = tx.send(Msg::StartFailed {
                    epoch,
                    error,
                    internal,
                });
            }
            let _ = (&mut abort).await;
        }
    }
    // The exposure's last observation, or its hook dropped unused.
    let _ = closed.await;
}

struct ClosedGuard {
    epoch: u64,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Drop for ClosedGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Closed { epoch: self.epoch });
    }
}

/// Records why the client could not be built: `Some(true)` for no runtime
/// (T9 INTERNAL), `Some(false)` for anything else (T8 INVALID_CONFIG).
struct Probe {
    inner: Box<dyn Connector>,
    failure: Arc<Mutex<Option<bool>>>,
}

impl Connector for Probe {
    fn connect(self: Box<Self>, builder: ClientBuilder) -> Result<Transport, BuildError> {
        let Self { inner, failure } = *self;
        inner
            .connect(builder)
            .inspect_err(|e| *lock(&failure) = Some(matches!(e, BuildError::NoRuntime)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_trim_matches_js() {
        assert_eq!(js_trim("  t\t"), "t");
        assert_eq!(js_trim("\u{feff}t\u{a0}"), "t");
        assert_eq!(js_trim("\u{85}t"), "\u{85}t");
        assert_eq!(js_trim("   "), "");
    }

    #[test]
    fn ms_like_js() {
        assert_eq!(fmt_ms(Duration::from_millis(5000)), "5000");
        assert_eq!(fmt_ms(Duration::from_micros(1500)), "1.5");
        assert_eq!(fmt_ms(Duration::ZERO), "0");
    }

    #[test]
    fn config_checks() {
        let c = |enabled, token: Option<&str>, name: Option<&str>| TunnelConfig {
            enabled,
            token: token.map(Into::into),
            name: name.map(Into::into),
            ..TunnelConfig::default()
        };
        assert!(invalid(&c(false, None, None)).is_none());
        assert!(invalid(&c(true, Some(" "), None)).is_none());
        assert!(!will_start(&c(true, Some(" \u{feff}"), Some("n"))));
        let (detail, p) = invalid(&c(true, None, Some("n"))).unwrap();
        assert_eq!(detail, "`token` must be a string or a provider");
        assert_eq!(p.last_error, last_error::TOKEN_NOT_SET);
        let (detail, p) = invalid(&c(true, Some("t"), None)).unwrap();
        assert_eq!(detail, "`name` must be a string");
        assert_eq!(p.last_error, last_error::NAME_NOT_SET);
        assert!(will_start(&c(true, Some("t"), None)));
        assert!(same_config(
            &c(true, Some("t"), Some("n")),
            &c(true, Some(" t\n"), Some("n"))
        ));
        assert!(!same_config(
            &c(true, Some("t"), Some("n")),
            &TunnelConfig {
                kind: Some(crate::ServiceKind::Http),
                ..c(true, Some("t"), Some("n"))
            }
        ));
    }

    #[test]
    fn provider_config_checks() {
        let provider = || Token::provider(|| async { Ok("t".to_owned()) });
        let with = |token: Option<&str>, p: Option<Token>| TunnelConfig {
            enabled: true,
            token: token.map(Into::into),
            token_provider: p,
            name: Some("n".into()),
            ..TunnelConfig::default()
        };
        let p = provider();
        // A provider alone starts, and wins over a blank or missing token.
        assert!(invalid(&with(None, Some(p.clone()))).is_none());
        assert!(will_start(&with(None, Some(p.clone()))));
        assert!(will_start(&with(Some(" "), Some(p.clone()))));
        // Identity, not behaviour.
        assert!(same_config(
            &with(None, Some(p.clone())),
            &with(Some("other"), Some(p.clone()))
        ));
        assert!(!same_config(
            &with(None, Some(p.clone())),
            &with(None, Some(provider()))
        ));
        assert!(!same_config(&with(Some("t"), None), &with(None, Some(p))));
        // A static token there is a string token.
        let s = with(None, Some(Token::from(" t ")));
        assert!(will_start(&s));
        assert!(same_config(&s, &with(Some("t"), None)));
        assert!(!will_start(&with(Some("t"), Some(Token::from(" ")))));
    }

    #[test]
    fn provider_config_eq_and_debug() {
        let p = Token::provider(|| async { Ok("secret".to_owned()) });
        let c = TunnelConfig {
            token_provider: Some(p),
            ..TunnelConfig::default()
        };
        assert_eq!(c, c.clone());
        assert_ne!(
            c,
            TunnelConfig {
                token_provider: Some(Token::provider(|| async { Ok("secret".to_owned()) })),
                ..TunnelConfig::default()
            }
        );
        assert_ne!(c, TunnelConfig::default());
        assert!(format!("{c:?}").contains("Token::Provider(..)"));
        let json = serde_json::to_string(&c).unwrap();
        assert!(!json.contains("token_provider"), "{json}");
    }

    #[test]
    fn problems_per_code() {
        use McpwarpErrorCode as E;
        use RegisterErrorCode as R;
        let p = |c: E| describe_problem(&c);
        assert_eq!(p(E::Unauthorized).last_error, last_error::TOKEN_REJECTED);
        assert_eq!(
            p(E::Register(R::Unauthenticated)).last_error,
            last_error::TOKEN_REJECTED
        );
        assert_eq!(
            p(E::Register(R::Forbidden)).last_error,
            last_error::TOKEN_FORBIDDEN
        );
        assert_eq!(p(E::TakenOver).code, TunnelStatusCode::TakenOver);
        assert_eq!(
            p(E::Register(R::InvalidName)).code,
            TunnelStatusCode::InvalidName
        );
        assert_eq!(
            p(E::Register(R::UsernameRequired)).code,
            TunnelStatusCode::UsernameRequired
        );
        assert_eq!(
            p(E::Register(R::QuotaExceeded)).code,
            TunnelStatusCode::QuotaExceeded
        );
        assert_eq!(p(E::Register(R::Conflict)).code, TunnelStatusCode::Conflict);
        assert_eq!(
            p(E::App(AppErrorCode::UnsupportedVersion)).code,
            TunnelStatusCode::UnsupportedVersion
        );
        assert_eq!(
            p(E::DisconnectedFatal).code,
            TunnelStatusCode::DisconnectedFatal
        );
        for other in [
            E::Register(R::BadRequest),
            E::Register(R::NotImplemented),
            E::Register(R::Other("NEW".into())),
            E::MalformedEnvelope,
            E::Closed,
            E::App(AppErrorCode::BadRequest),
        ] {
            assert_eq!(p(other).code, TunnelStatusCode::Internal);
        }
    }
}

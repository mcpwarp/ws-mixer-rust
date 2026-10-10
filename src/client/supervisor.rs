//! The single-owner supervisor task: dials, owns the active and retiring
//! connections, applies reconnect decisions and emits the lifecycle events
//! (`Connected`, `Disconnected`). No locks: every piece of client state lives
//! in this task, and the [`Client`](super::Client) talks to it through a
//! command channel and reads two `watch` channels it publishes.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use serde_json::{Map, Value};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::policy::{self, Decision, Failure, FatalKind, PolicyState, RetryKind};
use super::{ClientState, ReconnectPolicy};
use crate::conn::dial::BoxIo;
use crate::conn::{
    Conn, ConnConfig, ConnFailure, ConnHandle, DrainSignal, Handshaken, Upgrade, base_reason,
};
use crate::error::ErrorCode;
use crate::event::{ConnId, Connected, DisconnectReason, Envelope, Event, EventTx, Phase};
use crate::token::{Token, TokenError};

/// After a retiring connection's stream table empties, how long to wait for
/// the server's own `4012` before closing it ourselves with 1000. The server
/// closes a drained connection as soon as its table empties (WIRE.md §2.9
/// rollout shape), so this only fires against a server that does not.
pub(crate) const RETIRE_GRACE: Duration = Duration::from_secs(2);

/// Stand-in deadline for a timer whose `Instant` would overflow.
const FAR_FUTURE: Duration = Duration::from_secs(365 * 24 * 3600);

const BY_APPLICATION: &str = "the client was closed by the application";

/// What the [`Client`](super::Client) asks of the supervisor.
#[derive(Debug)]
pub(crate) enum Command {
    /// Graceful close of every live connection; stops reconnecting.
    Close,
    /// Application close (4014) of every live connection; stops reconnecting.
    CloseWith(String),
    /// `hello.meta` for the next dial.
    SetMeta(Option<Map<String, Value>>),
}

/// The connection the client currently talks to, published for lock-free
/// reads by [`Client`](super::Client).
#[derive(Clone)]
pub(crate) enum Link {
    /// No welcomed connection right now.
    Down,
    /// The newest welcomed live connection: the active one, or during a
    /// drain hand-over the retiring one until its replacement is welcomed.
    Up(ConnHandle),
    /// Finished for good: the fatal reason, or the application's close.
    Closed(DisconnectReason),
}

/// Everything the supervisor needs, assembled by `ClientBuilder::connect`.
pub(crate) struct Params {
    pub(crate) cfg: ConnConfig,
    pub(crate) token: Token,
    pub(crate) policy: ReconnectPolicy,
    pub(crate) upgrade: Upgrade,
    pub(crate) rng: Box<dyn FnMut() -> f64 + Send>,
    pub(crate) events: EventTx,
    pub(crate) commands: mpsc::UnboundedReceiver<Command>,
    pub(crate) state: watch::Sender<ClientState>,
    pub(crate) link: watch::Sender<Link>,
}

type DialResult = Result<Handshaken<BoxIo>, DialError>;

enum DialError {
    Token(TokenError),
    Conn(ConnFailure),
}

struct Dialing {
    conn_id: ConnId,
    task: JoinHandle<DialResult>,
}

struct Entry {
    handle: ConnHandle,
    retiring: bool,
}

#[derive(Clone)]
enum Closing {
    Graceful,
    Application(String),
    /// A fatal disconnect: close what is still live (retiring connections)
    /// gracefully.
    Fatal,
}

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub(crate) struct Supervisor {
    cfg: ConnConfig,
    token: Token,
    policy: ReconnectPolicy,
    upgrade: Upgrade,
    rng: Box<dyn FnMut() -> f64 + Send>,
    events: EventTx,
    commands: mpsc::UnboundedReceiver<Command>,
    commands_open: bool,
    drains: mpsc::UnboundedReceiver<DrainSignal>,
    state: watch::Sender<ClientState>,
    link: watch::Sender<Link>,

    next_conn_id: u64,
    ps: PolicyState,
    conns: HashMap<ConnId, Entry>,
    active: Option<ConnId>,
    ends: FuturesUnordered<BoxFut<(ConnId, DisconnectReason)>>,
    idles: FuturesUnordered<BoxFut<ConnId>>,
    dialing: Option<Dialing>,
    aborted: Vec<JoinHandle<DialResult>>,
    next_dial: Option<Instant>,
    stability: Option<(ConnId, Instant)>,
    closing: Option<Closing>,
    ever_connected: bool,
}

impl Supervisor {
    pub(crate) fn spawn(p: Params) -> JoinHandle<()> {
        let (drain_tx, drains) = mpsc::unbounded_channel();
        let mut cfg = p.cfg;
        cfg.drains = Some(drain_tx);
        let s = Self {
            cfg,
            token: p.token,
            policy: p.policy,
            upgrade: p.upgrade,
            rng: p.rng,
            events: p.events,
            commands: p.commands,
            commands_open: true,
            drains,
            state: p.state,
            link: p.link,
            next_conn_id: 1,
            ps: PolicyState::default(),
            conns: HashMap::new(),
            active: None,
            ends: FuturesUnordered::new(),
            idles: FuturesUnordered::new(),
            dialing: None,
            aborted: Vec::new(),
            next_dial: None,
            stability: None,
            closing: None,
            ever_connected: false,
        };
        tokio::spawn(s.run())
    }

    async fn run(mut self) {
        self.start_dial();
        while !self.finished() {
            tokio::select! {
                cmd = self.commands.recv(), if self.commands_open => match cmd {
                    Some(cmd) => self.on_command(cmd),
                    None => self.commands_open = false,
                },
                // The Events receiver is gone: nobody can observe this
                // client any more.
                _ = self.events.closed(), if self.closing.is_none() => {
                    self.begin_close(
                        Closing::Graceful,
                        "the client was closed because the Events receiver was dropped",
                    );
                }
                Some(sig) = self.drains.recv() => self.on_drain(sig),
                r = join_dial(&mut self.dialing) => self.on_dial(r),
                Some((id, reason)) = self.ends.next(), if !self.ends.is_empty() => {
                    self.on_end(id, reason);
                }
                Some(id) = self.idles.next(), if !self.idles.is_empty() => self.on_idle(id),
                _ = sleep_until(self.next_dial) => {
                    self.next_dial = None;
                    self.start_dial();
                }
                _ = sleep_until(self.stability.map(|(_, at)| at)) => self.on_stable(),
            }
        }
        // Aborted dial tasks hold event senders: wait until they are gone so
        // the channel closes right after the last event. One that finished
        // its handshake before the abort is closed like a live connection.
        let (code, message) = match &self.closing {
            Some(Closing::Application(m)) => (ErrorCode::APPLICATION_CLOSE, m.clone()),
            _ => (ErrorCode::NO_ERROR, "the client was closed".to_owned()),
        };
        for task in self.aborted.drain(..) {
            if let Ok(Ok(hs)) = task.await {
                tracing::debug!(conn_id = %hs.conn_id(), "closing a dial that completed while closing");
                hs.close(code, &message).await;
            }
        }
        self.state.send_replace(ClientState::Closed);
        tracing::debug!("client finished");
    }

    fn finished(&self) -> bool {
        self.closing.is_some() && self.conns.is_empty() && self.dialing.is_none()
    }

    // --- dialing --------------------------------------------------------

    fn start_dial(&mut self) {
        if self.closing.is_some() || self.dialing.is_some() {
            return;
        }
        let conn_id = ConnId(self.next_conn_id);
        self.next_conn_id += 1;
        tracing::debug!(%conn_id, "dialing");
        let upgrade = self.upgrade.clone();
        let cfg = self.cfg.clone();
        let token = self.token.clone();
        let events = self.events.clone();
        let task = tokio::spawn(async move {
            let tok = super::token::fetch(&token, cfg.connect_timeout)
                .await
                .map_err(DialError::Token)?;
            Conn::dial_via(&upgrade, &cfg, tok, events, conn_id)
                .await
                .map_err(DialError::Conn)
        });
        self.dialing = Some(Dialing { conn_id, task });
        self.publish();
    }

    fn on_dial(&mut self, r: Result<DialResult, tokio::task::JoinError>) {
        let Some(Dialing { conn_id, .. }) = self.dialing.take() else {
            return;
        };
        let r = r.unwrap_or_else(|e| {
            Err(DialError::Conn(
                base_reason(Some(conn_id), Phase::Dial, format!("dial task failed: {e}")).into(),
            ))
        });
        match r {
            Ok(hs) => self.on_welcome(hs),
            Err(e) => {
                let (reason, failure) = dial_failure(conn_id, e);
                self.on_failure(reason, &failure);
            }
        }
    }

    fn on_welcome(&mut self, hs: Handshaken<BoxIo>) {
        let conn_id = hs.conn_id();
        let welcome = hs.welcome().clone();
        tracing::info!(%conn_id, session = %welcome.session, "connected");
        // Connected strictly before the reader can produce this connection's
        // Stream/App/Drain events.
        self.emit(Event::Connected(Connected {
            conn_id,
            session: welcome.session.clone(),
            welcome,
            reconnect: self.ever_connected,
        }));
        self.ever_connected = true;
        let live = hs.start();
        let handle = live.handle().clone();
        self.ends
            .push(Box::pin(async move { (conn_id, live.finished().await) }));
        if let Some(old) = self.active.take() {
            // Not reachable: dials only run with no active connection or
            // after a drain retired it. Keep one active connection anyway.
            if let Some(e) = self.conns.get_mut(&old) {
                e.retiring = true;
            }
        }
        self.conns.insert(
            conn_id,
            Entry {
                handle,
                retiring: false,
            },
        );
        self.active = Some(conn_id);
        // The stability timer belongs to this connection; an older one
        // ending never touches it (WIRE.md §2.9).
        self.stability = Some((conn_id, deadline_after(self.policy.stable_after)));
        self.publish();
    }

    // --- failures and decisions -------------------------------------------

    /// A failed dial, or the end of the active connection.
    fn on_failure(&mut self, mut reason: DisconnectReason, failure: &Failure) {
        let provider = self.token.is_provider();
        let decision = policy::decide(&mut self.ps, failure, &self.policy, provider, &mut self.rng);
        match decision {
            Decision::Fatal(kind) => {
                reason.fatal = true;
                match kind {
                    FatalKind::Classified => {}
                    FatalKind::Disabled => {
                        reason.message = format!("reconnect disabled: {}", reason.message);
                    }
                    FatalKind::Exhausted(n) => {
                        reason.message = format!(
                            "{n} consecutive reconnects without a stable connection: {}",
                            reason.message
                        );
                    }
                }
                tracing::error!(conn_id = ?reason.conn_id, phase = %reason.phase, "giving up: {}", reason.message);
                self.emit_disconnected(reason.clone());
                self.begin_close_with_reason(Closing::Fatal, reason);
            }
            Decision::Retry {
                kind: RetryKind::RefreshNow,
                ..
            } => {
                // Not reported: the refresh is part of the same connect
                // attempt (Go dialAndHandshake, JS connectOnce).
                tracing::info!(conn_id = ?reason.conn_id, "token rejected ({}); refreshing it and redialing", reason.message);
                self.next_dial = None;
                self.start_dial();
            }
            Decision::Retry { delay, kind, loud } => {
                reason.fatal = false;
                reason.retry_in = Some(delay);
                if loud {
                    tracing::error!(conn_id = ?reason.conn_id, "connection failed with a code that indicates an SDK bug: {}; reconnecting in {delay:?}", reason.message);
                } else {
                    tracing::info!(conn_id = ?reason.conn_id, ?kind, attempt = self.ps.attempt, "{}; reconnecting in {delay:?}", reason.message);
                }
                self.emit_disconnected(reason);
                self.schedule(delay);
            }
        }
    }

    fn schedule(&mut self, delay: Duration) {
        if delay.is_zero() {
            self.next_dial = None;
            self.start_dial();
        } else {
            self.next_dial = Some(deadline_after(delay));
            self.publish();
        }
    }

    /// When the next dial happens, for a disconnect that does not schedule one
    /// itself.
    fn pending_retry_in(&self) -> Option<Duration> {
        if self.dialing.is_some() {
            return Some(Duration::ZERO);
        }
        self.next_dial
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    // --- connection lifecycle ---------------------------------------------

    fn on_end(&mut self, conn_id: ConnId, mut reason: DisconnectReason) {
        let Some(entry) = self.conns.remove(&conn_id) else {
            return;
        };
        let was_active = self.active == Some(conn_id);
        if was_active {
            self.active = None;
        }
        if self.stability.is_some_and(|(id, _)| id == conn_id) {
            self.stability = None;
        }
        if self.closing.is_some() {
            reason.fatal = false;
            reason.retry_in = None;
            self.emit_disconnected(reason);
        } else if entry.retiring || !was_active {
            // A drained connection: its replacement is already scheduled.
            reason.fatal = false;
            reason.retry_in = self.pending_retry_in();
            tracing::info!(%conn_id, "retired connection ended: {}", reason.message);
            self.emit_disconnected(reason);
        } else {
            let failure = Failure::Conn {
                phase: reason.phase,
                ws_code: reason.ws_code,
                fatal: reason.fatal,
                welcome_timeout: false,
            };
            self.on_failure(reason, &failure);
        }
        self.publish();
    }

    fn on_drain(&mut self, sig: DrainSignal) {
        if self.closing.is_some() || self.active != Some(sig.conn_id) {
            // Already retiring (a repeated drain), or not ours to act on.
            return;
        }
        if !self.policy.enabled {
            // Let in-flight streams finish; the server's close then ends the
            // client with one fatal disconnect.
            return;
        }
        let Some(entry) = self.conns.get_mut(&sig.conn_id) else {
            return;
        };
        entry.retiring = true;
        self.active = None;
        let handle = entry.handle.clone();
        let conn_id = sig.conn_id;
        self.idles.push(Box::pin(async move {
            handle.wait_idle().await;
            tokio::time::sleep(RETIRE_GRACE).await;
            conn_id
        }));
        // Not governed by `attempt` (WIRE.md §2.9).
        let delay = match sig.retry_after {
            Some(d) => d.min(policy::MAX_SERVER_DELAY),
            None => policy::going_away_jitter(&self.policy, &mut self.rng),
        };
        tracing::info!(%conn_id, "server drain: dialing a replacement in {delay:?}");
        self.schedule(delay);
    }

    fn on_idle(&mut self, conn_id: ConnId) {
        let Some(entry) = self.conns.get(&conn_id) else {
            return;
        };
        if self.closing.is_some() || entry.handle.is_terminal() {
            return;
        }
        let handle = entry.handle.clone();
        if !handle.is_idle() {
            // The server opened another stream (<= last_stream_id) meanwhile.
            self.idles.push(Box::pin(async move {
                handle.wait_idle().await;
                tokio::time::sleep(RETIRE_GRACE).await;
                conn_id
            }));
            return;
        }
        tracing::debug!(%conn_id, "retired connection is idle; closing it");
        handle.close_normal("drained connection has no streams left");
    }

    fn on_stable(&mut self) {
        let Some((conn_id, _)) = self.stability.take() else {
            return;
        };
        // Still live and not superseded by a newer connection.
        let newest = self.conns.keys().max().copied();
        if newest == Some(conn_id) {
            tracing::debug!(%conn_id, "connection stable: resetting reconnect attempt and budgets");
            self.ps.stable();
        }
    }

    // --- closing ------------------------------------------------------------

    fn on_command(&mut self, cmd: Command) {
        match cmd {
            Command::Close => self.begin_close(Closing::Graceful, BY_APPLICATION),
            Command::CloseWith(message) => {
                self.begin_close(Closing::Application(message), BY_APPLICATION);
            }
            Command::SetMeta(meta) => self.cfg.meta = meta,
        }
    }

    fn begin_close(&mut self, mode: Closing, message: &str) {
        let reason = base_reason(
            None,
            if self.conns.is_empty() {
                Phase::Dial
            } else {
                Phase::Connected
            },
            message.into(),
        );
        self.begin_close_with_reason(mode, reason);
    }

    /// Stops reconnecting and closes every live connection. Each one still
    /// reports its own (non-fatal) `Disconnected` when it ends.
    fn begin_close_with_reason(&mut self, mode: Closing, reason: DisconnectReason) {
        if self.closing.is_some() {
            return;
        }
        if let Some(d) = self.dialing.take() {
            // Not a handshake failure: no event (CLIENT-SDK.md).
            tracing::debug!(conn_id = %d.conn_id, "cancelling dial");
            d.task.abort();
            self.aborted.push(d.task);
        }
        self.next_dial = None;
        self.stability = None;
        for entry in self.conns.values() {
            match &mode {
                Closing::Graceful | Closing::Fatal => entry.handle.close_graceful(),
                Closing::Application(message) => entry.handle.close_with(message),
            }
        }
        self.closing = Some(mode);
        self.link.send_replace(Link::Closed(reason));
        self.state.send_replace(ClientState::Closed);
    }

    // --- outputs ------------------------------------------------------------

    fn emit(&self, event: Event) {
        let _ = self.events.send(Envelope {
            event,
            backlog: None,
        });
    }

    fn emit_disconnected(&self, reason: DisconnectReason) {
        self.emit(Event::Disconnected(reason));
    }

    /// Recomputes the published link and state.
    fn publish(&self) {
        if self.closing.is_some() {
            return;
        }
        let newest = self
            .active
            .or_else(|| self.conns.keys().max().copied())
            .and_then(|id| self.conns.get(&id));
        let (link, state) = match newest {
            Some(e) => (Link::Up(e.handle.clone()), ClientState::Connected),
            None if self.dialing.is_some() => (Link::Down, ClientState::Dialing),
            None if self.next_dial.is_some() => (Link::Down, ClientState::Backoff),
            None => (Link::Down, ClientState::Idle),
        };
        self.link.send_replace(link);
        self.state.send_if_modified(|s| {
            let changed = *s != state;
            *s = state;
            changed
        });
    }
}

/// A panic in the supervisor must not leave a zombie client: close what is
/// live and publish `Closed` so waiters and senders see the end.
impl Drop for Supervisor {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        tracing::error!("client supervisor panicked; closing the client");
        if let Some(d) = self.dialing.take() {
            d.task.abort();
        }
        for entry in self.conns.values() {
            entry.handle.close_normal("the client task panicked");
        }
        let mut reason = base_reason(None, Phase::Connected, "the client task panicked".into());
        reason.fatal = true;
        self.link.send_replace(Link::Closed(reason));
        self.state.send_replace(ClientState::Closed);
    }
}

/// The reason and policy input for a failed dial.
fn dial_failure(conn_id: ConnId, e: DialError) -> (DisconnectReason, Failure) {
    match e {
        DialError::Token(e) => {
            let failure = match e {
                TokenError::Fatal(_) => Failure::TokenFatal,
                _ => Failure::TokenUnavailable,
            };
            let detail = std::error::Error::source(&e)
                .map(|s| format!("{e}: {s}"))
                .unwrap_or_else(|| e.to_string());
            let mut r = base_reason(Some(conn_id), Phase::Dial, detail);
            r.cause = Some(Arc::new(e));
            (r, failure)
        }
        DialError::Conn(f) => {
            let failure = match f.reason.http_status {
                Some(status) => Failure::Http {
                    status,
                    retry_after: f.retry_after,
                },
                None => Failure::Conn {
                    phase: f.reason.phase,
                    ws_code: f.reason.ws_code,
                    fatal: f.reason.fatal,
                    welcome_timeout: f.welcome_timeout,
                },
            };
            (*f.reason, failure)
        }
    }
}

async fn join_dial(d: &mut Option<Dialing>) -> Result<DialResult, tokio::task::JoinError> {
    match d {
        Some(d) => (&mut d.task).await,
        None => std::future::pending().await,
    }
}

/// `Instant::now() + d`, or [`FAR_FUTURE`] from now if that overflows.
fn deadline_after(d: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(d).unwrap_or_else(|| now + FAR_FUTURE)
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

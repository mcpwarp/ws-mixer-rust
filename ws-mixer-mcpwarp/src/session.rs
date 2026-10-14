//! The registration state machine for one service (MCPWARP.md §2.7, §3.2,
//! §3.3, and §5.9's connection-identity rules), free of sockets, clocks and
//! tasks: a port of ws-mixer-js `src/mcpwarp/session.ts`, plus the
//! superseded-connection bookkeeping JS keeps in its client.
//!
//! The driver feeds it transport inputs (`on_welcome`, `on_app`, `on_drain`,
//! `on_disconnected`, `close`, timer firings, failed sends) and executes the
//! [`Command`]s it queues, in order, via [`Session::poll_command`].

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use serde_json::{Map, Value};
use ws_mixer::{ConnId, DrainReason};

use crate::codes::{AppErrorCode, RegisterErrorCode};
use crate::envelope::{InboundEnvelope, decode_envelope, encode_register, encode_unregister};
use crate::error::{McpwarpError, McpwarpErrorCode};
use crate::names::ServiceKind;

/// First transient register retry delay (MCPWARP.md §3.2).
pub const REGISTER_RETRY_BASE: Duration = Duration::from_millis(1000);
/// Cap on the transient register retry delay.
pub const REGISTER_RETRY_CAP: Duration = Duration::from_millis(30_000);
/// The pause before re-sending after `OVERLOADED` (MCPWARP.md §2.6).
pub const OVERLOAD_PAUSE: Duration = Duration::from_millis(1000);
/// How long `close` waits for `unregistered` (MCPWARP.md §3.2).
pub const UNREGISTER_BUDGET: Duration = Duration::from_millis(2000);
/// The message of the immediate close after a take-over, and of the
/// `RESET(CANCEL)` of streams that arrive after it (MCPWARP.md §3.3, §4.4).
pub const TAKEN_OVER_MESSAGE: &str = "service taken over by another connection";

/// The agent's state (MCPWARP.md §5.5).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExposeState {
    /// Waiting for a `welcome`. The initial state.
    #[default]
    Connecting,
    /// `register` sent, no answer yet.
    Registering,
    /// Live at its URL.
    Registered,
    /// Disabled in the dashboard; requests get `503`.
    Disabled,
    /// Taken over by another connection; sticky.
    Conflict,
    /// `close` in progress.
    Closing,
    /// Closed.
    Closed,
}

impl ExposeState {
    /// The spec's string, e.g. `"registering"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Registering => "registering",
            Self::Registered => "registered",
            Self::Disabled => "disabled",
            Self::Conflict => "conflict",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }
}

impl std::fmt::Display for ExposeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One successful `register` (MCPWARP.md §5.5 `registration`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    /// The public URL; never empty.
    pub url: String,
    /// The server id.
    pub server_id: String,
    /// `true` when this `register` created the service.
    pub created: bool,
}

/// A take-over (MCPWARP.md §5.6 `conflict`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    /// The server id: the `conflict` op's `id`, or for a `drain` with reason
    /// `replaced` the last known one. `None`, never `""`, when none is known.
    pub server_id: Option<String>,
    /// The `conflict` op's `name`, or the agent's own for a drain.
    pub name: String,
    /// `"taken_over"` (or the op's own reason), or `"replaced"` for a drain.
    pub reason: String,
}

/// What `onRetry` receives (MCPWARP.md §5.9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryInfo {
    /// Consecutive retries of this kind.
    pub attempt: u32,
    /// The delay before the retry.
    pub delay: Duration,
    /// `"register failed: <CODE>: <message>"` for the session's retries.
    pub reason: String,
}

/// Why `register` is re-sent.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RegisterRetryKind {
    /// After a transient register error.
    Transient,
    /// After `error{OVERLOADED}`.
    Overloaded,
}

/// Where a non-fatal error came from, so a caller can classify it without
/// parsing messages.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorOrigin {
    /// An envelope that failed to decode.
    Malformed,
    /// A transient register error; a retry follows.
    RegisterTransient,
    /// A register refusal after an earlier registration; nothing retries it.
    RegisterRefusal,
    /// An `error` op.
    App,
}

/// What the session reports to its driver.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SessionEvent {
    /// The state changed.
    State(ExposeState),
    /// A successful `register`.
    Registered(Registration),
    /// Disabled by `disable` (its reason) or `SERVER_DISABLED` (`"disabled"`).
    Disabled {
        /// The server id, if known.
        server_id: Option<String>,
        /// Why.
        reason: String,
    },
    /// A matching `enable`; `register` follows.
    Enabled {
        /// The adopted server id.
        server_id: String,
    },
    /// Taken over (MCPWARP.md §3.3); a [`Command::CloseImmediate`] follows.
    Conflict(Conflict),
    /// A non-fatal error.
    Error {
        /// The error.
        error: McpwarpError,
        /// Where it came from.
        origin: ErrorOrigin,
    },
    /// A `register` will be re-sent after `info.delay`.
    Retry {
        /// For `onRetry`.
        info: RetryInfo,
        /// Why.
        kind: RegisterRetryKind,
    },
}

/// An `app` message the session sends.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Outbound {
    /// `register`.
    Register,
    /// `unregister`.
    Unregister,
}

/// The session's timers.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimerKind {
    /// The transient register retry.
    RegisterRetry,
    /// The `OVERLOADED` pause.
    Overload,
    /// The wait for `unregistered`.
    UnregisterBudget,
}

/// Identifies one started timer; firings of a cancelled or replaced timer
/// are ignored.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimerId {
    kind: TimerKind,
    seq: u64,
}

impl TimerId {
    /// Which timer.
    pub fn kind(self) -> TimerKind {
        self.kind
    }
}

/// What the driver must do, in order.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Command {
    /// Send `body` as an `app` message on the live connection. Report a
    /// failure with [`Session::on_send_failed`].
    SendApp {
        /// The `app` body.
        body: Value,
        /// Which message it is.
        op: Outbound,
    },
    /// Call [`Session::on_timer`] with `id` after `delay`.
    StartTimer {
        /// The timer.
        id: TimerId,
        /// When.
        delay: Duration,
    },
    /// Forget the timer `id`.
    CancelTimer(TimerId),
    /// Report an event.
    Emit(SessionEvent),
    /// The session failed: close the client gracefully, then call
    /// [`Session::on_closed`]. The error is fatal.
    Fatal(McpwarpError),
    /// Taken over: close the client at once with `error{NO_ERROR}` + 1000
    /// ([`ws_mixer::Client::close_immediate`]), no `unregister`, no drain
    /// grace (MCPWARP.md §3.3, §6.8 Take-over close). Nothing reconnects.
    CloseImmediate {
        /// The close message.
        message: String,
    },
    /// `close` is done with `unregister`: close the client gracefully, then
    /// call [`Session::on_closed`].
    CloseGraceful,
}

/// The registration state machine for one service. See the module docs.
#[derive(Debug)]
pub struct Session {
    name: String,
    kind: ServiceKind,
    state: ExposeState,
    url: Option<String>,
    server_id: Option<String>,
    registration: Option<Registration>,
    ever_registered: bool,
    taken_over: bool,

    connected: bool,
    sent_register_this_conn: bool,
    outstanding: Option<Outbound>,
    transient_failures: u32,
    overloaded_resends: u32,
    retry_timer: Option<TimerId>,
    overload_timer: Option<(TimerId, Outbound)>,
    unregister_wait: Option<TimerId>,
    next_timer: u64,

    /// The newest welcomed connection.
    live: Option<ConnId>,
    /// A drained connection whose replacement has not been welcomed yet.
    draining: Option<ConnId>,
    /// Drained connections whose replacement has been welcomed.
    superseded: HashSet<ConnId>,

    commands: VecDeque<Command>,
}

impl Session {
    /// A session for `name` in state [`ExposeState::Connecting`]. `name` is
    /// not validated here.
    pub fn new(name: impl Into<String>, kind: ServiceKind) -> Self {
        Self {
            name: name.into(),
            kind,
            state: ExposeState::Connecting,
            url: None,
            server_id: None,
            registration: None,
            ever_registered: false,
            taken_over: false,
            connected: false,
            sent_register_this_conn: false,
            outstanding: None,
            transient_failures: 0,
            overloaded_resends: 0,
            retry_timer: None,
            overload_timer: None,
            unregister_wait: None,
            next_timer: 0,
            live: None,
            draining: None,
            superseded: HashSet::new(),
            commands: VecDeque::new(),
        }
    }

    /// The next command to execute, in order.
    pub fn poll_command(&mut self) -> Option<Command> {
        self.commands.pop_front()
    }

    // --- accessors ---------------------------------------------------------

    /// The service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The service kind.
    pub fn kind(&self) -> ServiceKind {
        self.kind
    }

    /// The current state.
    pub fn state(&self) -> ExposeState {
        self.state
    }

    /// The latest registered URL.
    pub fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    /// The latest server id, from `registered`, `disable`, `enable` or
    /// `SERVER_DISABLED`.
    pub fn server_id(&self) -> Option<&str> {
        self.server_id.as_deref()
    }

    /// The latest successful registration.
    pub fn registration(&self) -> Option<&Registration> {
        self.registration.as_ref()
    }

    /// Whether any `register` has succeeded.
    pub fn ever_registered(&self) -> bool {
        self.ever_registered
    }

    /// Whether the service was taken over; sticky.
    pub fn is_taken_over(&self) -> bool {
        self.taken_over
    }

    /// Whether `conn` is a drained connection whose replacement has been
    /// welcomed (MCPWARP.md §2.7, §5.9).
    pub fn is_superseded(&self, conn: ConnId) -> bool {
        self.superseded.contains(&conn)
    }

    // --- inputs ------------------------------------------------------------

    /// `conn` was welcomed: send `register` unless taken over or closing
    /// (MCPWARP.md §3.2). A drained connection becomes superseded.
    pub fn on_welcome(&mut self, conn: ConnId) {
        if let Some(old) = self.draining.take() {
            if old != conn {
                self.superseded.insert(old);
            }
        }
        self.live = Some(conn);
        if self.is_ending() {
            return;
        }
        self.connected = true;
        self.sent_register_this_conn = false;
        self.cancel_timers();
        if self.taken_over {
            return;
        }
        self.set_state(ExposeState::Registering);
        self.send_register();
    }

    /// An `app` body arrived on `conn`. Ignored when `conn` is superseded.
    pub fn on_app(&mut self, conn: ConnId, body: &Map<String, Value>) {
        if self.superseded.contains(&conn) {
            return;
        }
        match decode_envelope(body) {
            Ok(Some(env)) => self.on_envelope(env),
            Ok(None) => {}
            Err(error) => self.emit(SessionEvent::Error {
                error,
                origin: ErrorOrigin::Malformed,
            }),
        }
    }

    /// `conn` announced a drain. `reason` `replaced` is a take-over
    /// (MCPWARP.md §3.3); returns whether it took the service over. Any other
    /// reason is a hand-over: the replacement's welcome supersedes `conn`.
    pub fn on_drain(&mut self, conn: ConnId, reason: DrainReason) -> bool {
        if self.superseded.contains(&conn) {
            return false;
        }
        if reason == DrainReason::Replaced {
            if self.is_ending() || self.taken_over {
                return false;
            }
            let conflict = Conflict {
                server_id: self.server_id.clone(),
                name: self.name.clone(),
                reason: "replaced".into(),
            };
            self.take_over(conflict);
            return true;
        }
        if self.live == Some(conn) {
            self.draining = Some(conn);
        }
        false
    }

    /// `conn` ended, or a dial for it failed. Returns `true` when that leaves
    /// the session alone (MCPWARP.md §5.9 Disconnect, §6.7 T29/T38): a
    /// superseded connection, or a replacement's failed dial while the
    /// drained connection still serves. Otherwise the session is back to
    /// `connecting` (T30, T44), a pending register retry cancelled.
    pub fn on_disconnected(&mut self, conn: ConnId) -> bool {
        if self.superseded.remove(&conn) {
            return true;
        }
        let welcomed = self.live == Some(conn) || self.draining == Some(conn);
        if self.draining.is_some() && !welcomed {
            return true;
        }
        if self.draining == Some(conn) {
            self.draining = None;
        }
        if self.live == Some(conn) {
            self.live = None;
        }
        if self.is_ending() {
            return false;
        }
        self.connected = false;
        self.sent_register_this_conn = false;
        self.outstanding = None;
        self.cancel_timers();
        if !self.taken_over {
            self.set_state(ExposeState::Connecting);
        }
        false
    }

    /// Close requested (MCPWARP.md §5.7 steps 1-2): `unregister` if this
    /// connection sent `register` and is connected, then wait for
    /// `unregistered` or [`UNREGISTER_BUDGET`]; a [`Command::CloseGraceful`]
    /// ends the wait. A no-op once closing.
    pub fn close(&mut self) {
        if self.is_ending() {
            return;
        }
        let was_registered = self.connected && self.sent_register_this_conn;
        self.set_state(ExposeState::Closing);
        self.cancel_timers();
        if !was_registered {
            self.push(Command::CloseGraceful);
            return;
        }
        let id = self.start_timer(TimerKind::UnregisterBudget, UNREGISTER_BUDGET);
        self.unregister_wait = Some(id);
        self.outstanding = Some(Outbound::Unregister);
        self.push(Command::SendApp {
            body: encode_unregister(&self.name),
            op: Outbound::Unregister,
        });
    }

    /// Abandoned before it settled (`expose()`'s `signal`, MCPWARP.md §5.8):
    /// `closing` at once, timers cancelled, a [`Command::CloseGraceful`]
    /// without `unregister`. Every later input is ignored, as after
    /// [`close`](Self::close). A no-op once closing.
    pub fn abort(&mut self) {
        if self.is_ending() {
            return;
        }
        self.set_state(ExposeState::Closing);
        self.cancel_timers();
        self.outstanding = None;
        self.push(Command::CloseGraceful);
    }

    /// A [`Command::SendApp`] failed. A failed `unregister` ends the close
    /// wait at once; a failed `register` is left to the next welcome.
    pub fn on_send_failed(&mut self, op: Outbound) {
        if op == Outbound::Unregister {
            self.end_unregister_wait();
        }
    }

    /// The timer `id` fired.
    pub fn on_timer(&mut self, id: TimerId) {
        match id.kind {
            TimerKind::RegisterRetry if self.retry_timer == Some(id) => {
                self.retry_timer = None;
                if !self.is_ending() && self.connected {
                    self.send_register();
                }
            }
            TimerKind::Overload => {
                let Some((current, op)) = self.overload_timer else {
                    return;
                };
                if current != id {
                    return;
                }
                self.overload_timer = None;
                if !self.connected {
                    return;
                }
                if op == Outbound::Register && !self.is_ending() {
                    self.send_register();
                } else if op == Outbound::Unregister && self.state == ExposeState::Closing {
                    self.push(Command::SendApp {
                        body: encode_unregister(&self.name),
                        op: Outbound::Unregister,
                    });
                }
            }
            TimerKind::UnregisterBudget if self.unregister_wait == Some(id) => {
                self.unregister_wait = None;
                self.push(Command::CloseGraceful);
            }
            _ => {}
        }
    }

    /// The client is closed (MCPWARP.md §5.7 step 4).
    pub fn on_closed(&mut self) {
        self.cancel_timers();
        if let Some(id) = self.unregister_wait.take() {
            self.push(Command::CancelTimer(id));
        }
        self.set_state(ExposeState::Closed);
    }

    // --- envelope handling -------------------------------------------------

    fn on_envelope(&mut self, env: InboundEnvelope) {
        match env {
            InboundEnvelope::Registered { services, errors } => {
                self.on_registered(services, errors);
            }
            InboundEnvelope::Unregistered { services } => {
                // The edge answers with no services when this connection held
                // nothing for the name.
                if self.outstanding == Some(Outbound::Unregister)
                    || services.iter().any(|s| s.name == self.name)
                {
                    if self.outstanding == Some(Outbound::Unregister) {
                        self.outstanding = None;
                    }
                    self.end_unregister_wait();
                }
            }
            InboundEnvelope::Disable { id, reason } => {
                if self.is_ending() || self.taken_over {
                    return;
                }
                if self.server_id.as_ref().is_none_or(|s| *s == id) {
                    self.server_id = Some(id);
                    self.set_state(ExposeState::Disabled);
                    self.emit(SessionEvent::Disabled {
                        server_id: self.server_id.clone(),
                        reason,
                    });
                }
            }
            InboundEnvelope::Enable { id, name } => {
                if self.is_ending() || self.taken_over || name != self.name {
                    return;
                }
                self.server_id = Some(id.clone());
                self.set_state(ExposeState::Registering);
                self.emit(SessionEvent::Enabled { server_id: id });
                self.send_register();
            }
            InboundEnvelope::Conflict { id, name, reason } => {
                if self.is_ending() || self.taken_over {
                    return;
                }
                let ours = name == self.name || self.server_id.as_ref() == Some(&id);
                if ours {
                    self.take_over(Conflict {
                        server_id: Some(id),
                        name,
                        reason,
                    });
                }
            }
            InboundEnvelope::Error { code, message } => self.on_app_error(code, message),
            _ => {}
        }
    }

    fn on_registered(
        &mut self,
        services: Vec<crate::envelope::RegisteredService>,
        errors: Vec<crate::envelope::ServiceError>,
    ) {
        if self.is_ending() || self.taken_over {
            return;
        }
        if let Some(svc) = services.into_iter().find(|s| s.name == self.name) {
            self.outstanding = None;
            self.transient_failures = 0;
            self.overloaded_resends = 0;
            let reg = Registration {
                url: svc.url,
                server_id: svc.id,
                created: svc.created,
            };
            self.url = Some(reg.url.clone());
            self.server_id = Some(reg.server_id.clone());
            self.registration = Some(reg.clone());
            self.ever_registered = true;
            self.set_state(ExposeState::Registered);
            self.emit(SessionEvent::Registered(reg));
            return;
        }
        let Some(err) = errors.into_iter().find(|e| e.name == self.name) else {
            return;
        };
        self.outstanding = None;
        if err.code == RegisterErrorCode::ServerDisabled {
            if err.id.is_some() {
                self.server_id = err.id;
            }
            self.set_state(ExposeState::Disabled);
            self.emit(SessionEvent::Disabled {
                server_id: self.server_id.clone(),
                reason: "disabled".into(),
            });
            return;
        }
        let reason = format!("register failed: {}: {}", err.code, err.message);
        let error = McpwarpError::new(McpwarpErrorCode::Register(err.code.clone()), err.message)
            .with_service_name(self.name.clone());
        if err.code.is_transient() {
            self.emit(SessionEvent::Error {
                error,
                origin: ErrorOrigin::RegisterTransient,
            });
            let delay = register_retry_delay(self.transient_failures);
            self.transient_failures = self.transient_failures.saturating_add(1);
            self.emit(SessionEvent::Retry {
                info: RetryInfo {
                    attempt: self.transient_failures,
                    delay,
                    reason,
                },
                kind: RegisterRetryKind::Transient,
            });
            if let Some(old) = self.retry_timer.take() {
                self.push(Command::CancelTimer(old));
            }
            self.retry_timer = Some(self.start_timer(TimerKind::RegisterRetry, delay));
            return;
        }
        if !self.ever_registered {
            self.fail(error.with_fatal(true));
            return;
        }
        // Registered before: expose() keeps the handle; the tunnel treats it
        // as terminal.
        self.emit(SessionEvent::Error {
            error,
            origin: ErrorOrigin::RegisterRefusal,
        });
    }

    fn on_app_error(&mut self, code: AppErrorCode, message: String) {
        match code {
            AppErrorCode::UnsupportedVersion => {
                let error = McpwarpError::new(McpwarpErrorCode::App(code), message)
                    .with_fatal(true)
                    .with_service_name(self.name.clone());
                self.fail(error);
            }
            AppErrorCode::Overloaded => {
                // Re-send what is outstanding once, after a pause.
                let Some(op) = self.outstanding else {
                    return;
                };
                if self.overload_timer.is_some() {
                    return;
                }
                let id = self.start_timer(TimerKind::Overload, OVERLOAD_PAUSE);
                self.overload_timer = Some((id, op));
                if op == Outbound::Register && !self.is_ending() {
                    self.overloaded_resends = self.overloaded_resends.saturating_add(1);
                    self.emit(SessionEvent::Retry {
                        info: RetryInfo {
                            attempt: self.overloaded_resends,
                            delay: OVERLOAD_PAUSE,
                            reason: format!("register failed: {code}: {message}"),
                        },
                        kind: RegisterRetryKind::Overloaded,
                    });
                }
            }
            code => {
                let error = McpwarpError::new(McpwarpErrorCode::App(code), message)
                    .with_service_name(self.name.clone());
                self.emit(SessionEvent::Error {
                    error,
                    origin: ErrorOrigin::App,
                });
            }
        }
    }

    // --- helpers -----------------------------------------------------------

    fn take_over(&mut self, conflict: Conflict) {
        self.taken_over = true;
        self.outstanding = None;
        // The edge no longer holds the name for this connection: close has
        // nothing to unregister.
        self.sent_register_this_conn = false;
        self.cancel_timers();
        self.set_state(ExposeState::Conflict);
        self.emit(SessionEvent::Conflict(conflict));
        self.push(Command::CloseImmediate {
            message: TAKEN_OVER_MESSAGE.into(),
        });
    }

    /// A fatal error: no more timers or registers; the driver closes. A
    /// no-op once closing: the close under way keeps its outcome.
    fn fail(&mut self, error: McpwarpError) {
        if self.is_ending() {
            return;
        }
        self.cancel_timers();
        self.set_state(ExposeState::Closing);
        self.push(Command::Fatal(error));
    }

    fn send_register(&mut self) {
        self.outstanding = Some(Outbound::Register);
        self.sent_register_this_conn = true;
        self.push(Command::SendApp {
            body: encode_register(&self.name, self.kind),
            op: Outbound::Register,
        });
    }

    fn end_unregister_wait(&mut self) {
        if let Some(id) = self.unregister_wait.take() {
            self.push(Command::CancelTimer(id));
            self.push(Command::CloseGraceful);
        }
    }

    fn is_ending(&self) -> bool {
        matches!(self.state, ExposeState::Closing | ExposeState::Closed)
    }

    fn set_state(&mut self, next: ExposeState) {
        if self.state != next {
            self.state = next;
            self.emit(SessionEvent::State(next));
        }
    }

    fn start_timer(&mut self, kind: TimerKind, delay: Duration) -> TimerId {
        self.next_timer += 1;
        let id = TimerId {
            kind,
            seq: self.next_timer,
        };
        self.push(Command::StartTimer { id, delay });
        id
    }

    /// Cancels the register retry and the overload pause (not the close
    /// wait).
    fn cancel_timers(&mut self) {
        if let Some(id) = self.retry_timer.take() {
            self.push(Command::CancelTimer(id));
        }
        if let Some((id, _)) = self.overload_timer.take() {
            self.push(Command::CancelTimer(id));
        }
    }

    fn emit(&mut self, event: SessionEvent) {
        self.push(Command::Emit(event));
    }

    fn push(&mut self, c: Command) {
        self.commands.push_back(c);
    }
}

/// `min(1000 * 2^n, 30000)` ms.
fn register_retry_delay(n: u32) -> Duration {
    REGISTER_RETRY_BASE
        .saturating_mul(1u32.checked_shl(n).unwrap_or(u32::MAX))
        .min(REGISTER_RETRY_CAP)
}

#[cfg(test)]
mod tests;

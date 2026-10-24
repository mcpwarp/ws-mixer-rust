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
/// The pause before the first re-send after `OVERLOADED` (MCPWARP.md §2.6);
/// it doubles with every consecutive `OVERLOADED`, capped at
/// [`REGISTER_RETRY_CAP`].
pub const OVERLOAD_PAUSE: Duration = Duration::from_millis(1000);
/// How long `close` waits for `unregistered` (MCPWARP.md §3.2).
pub const UNREGISTER_BUDGET: Duration = Duration::from_millis(2000);
/// The message of the immediate close after a take-over, and of the
/// `RESET(CANCEL)` of streams that arrive after it (MCPWARP.md §3.3, §4.4).
pub const TAKEN_OVER_MESSAGE: &str = "service taken over by another connection";
/// How many ended connections the session remembers as superseded or not
/// (MCPWARP.md §2.7 Ended connection). Late ops are flushed right after
/// their connection's disconnect, so a handful is plenty.
const ENDED_MEMORY: usize = 16;
/// The reason of the re-send after a `registered` that lists no entry for
/// the service (MCPWARP.md §2.6 One in flight).
const UNLISTED_REASON: &str = "register failed: registered reply did not list the service";

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
    /// `"register failed: <CODE>: <message>"` for the session's retries, or
    /// `"register failed: registered reply did not list the service"`.
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
///
/// It keeps one `register`/`unregister` in flight (MCPWARP.md §2.6 Frames
/// in flight) and assumes it is the only service on its connection: replies
/// are not matched by name, so any `registered`, `unregistered` or
/// whole-frame `error` on the live connection answers its outstanding frame.
/// A host multiplexing several sessions on one connection must hold back a
/// session's triggers ([`on_welcome`](Self::on_welcome), any of its timer
/// firings (transient retry or `OVERLOADED` re-send), an `enable`,
/// [`close`](Self::close)) until the connection's slot is free, and never
/// buffer an emitted [`Command::SendApp`]: emitting it marks the frame
/// outstanding, records that it sent `register` on this connection, and for
/// `unregister` starts the [`UNREGISTER_BUDGET`]. A reply may make the
/// session emit its next queued frame in the same call, which then holds the
/// slot before any other session may send. Route `registered`,
/// `unregistered` and whole-frame `error` only to the session whose frame is
/// on the wire; free the slot only on a reply, a disconnect or a replacement
/// welcome, never on a session's local state change; key the queue by
/// connection id across drains.
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
    /// A `registered`, or a whole-frame `error` answering a `register`,
    /// arrived on this connection: the edge may hold the name, so `close`
    /// unregisters (MCPWARP.md §3.2 Unregister).
    register_answered: bool,
    outstanding: Option<Outbound>,
    /// A `register` trigger that arrived while a frame was outstanding; sent
    /// once that frame is answered.
    register_queued: bool,
    /// `close` sent its `unregister` while a `register` was still awaiting
    /// its reply: the next reply answers that `register` (frame order).
    register_ahead: bool,
    transient_failures: u32,
    /// Consecutive `OVERLOADED` replies on this connection.
    overloaded_resends: u32,
    retry_timer: Option<TimerId>,
    overload_timer: Option<(TimerId, Outbound)>,
    unregister_wait: Option<TimerId>,
    next_timer: u64,
    /// A `disable` (id, reason) that arrived before any server id was known;
    /// applied if the next `registered` carries that id. It belongs to the
    /// live connection: dropped when that one ends or a replacement is
    /// welcomed (MCPWARP.md §2.7 Held `disable` lifetime).
    pending_disable: Option<(String, String)>,

    /// The newest welcomed connection.
    live: Option<ConnId>,
    /// The highest connection this session saw welcomed; ids are monotonic,
    /// so any other one up to it has ended (or is superseded).
    highest_welcomed: Option<ConnId>,
    /// A drained connection whose replacement has not been welcomed yet.
    draining: Option<ConnId>,
    /// Drained connections whose replacement has been welcomed.
    superseded: HashSet<ConnId>,
    /// The latest connections that ended, oldest first, each with whether
    /// it was superseded when it ended. Only the superseded flag is read
    /// from it; whether a connection ended follows from `highest_welcomed`.
    ended: VecDeque<(ConnId, bool)>,

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
            register_answered: false,
            outstanding: None,
            register_queued: false,
            register_ahead: false,
            transient_failures: 0,
            overloaded_resends: 0,
            retry_timer: None,
            overload_timer: None,
            unregister_wait: None,
            next_timer: 0,
            pending_disable: None,
            live: None,
            highest_welcomed: None,
            draining: None,
            superseded: HashSet::new(),
            ended: VecDeque::new(),
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

    /// The latest server id, from `registered`, `enable` or
    /// `SERVER_DISABLED`. A `disable` never introduces one: it applies only
    /// to the known id, or is held until `registered` names it.
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
    /// welcomed (MCPWARP.md §2.7, §5.9), including one that has since ended.
    pub fn is_superseded(&self, conn: ConnId) -> bool {
        self.superseded.contains(&conn) || self.ended.contains(&(conn, true))
    }

    /// Whether `conn` has ended, superseded or not: neither live nor
    /// draining, and no newer than the newest welcome. Covers connections
    /// this session never saw (they ended before it existed on a shared
    /// client) and ones evicted from `ended`.
    fn has_ended(&self, conn: ConnId) -> bool {
        Some(conn) != self.live
            && Some(conn) != self.draining
            && self.highest_welcomed.is_some_and(|h| conn <= h)
    }

    // --- inputs ------------------------------------------------------------

    /// `conn` was welcomed: send `register` unless taken over or closing
    /// (MCPWARP.md §3.2). A drained connection becomes superseded, and a
    /// held `disable` is dropped with the connection it belonged to.
    pub fn on_welcome(&mut self, conn: ConnId) {
        if let Some(old) = self.draining.take() {
            if old != conn {
                self.superseded.insert(old);
            }
        }
        self.live = Some(conn);
        self.highest_welcomed = self.highest_welcomed.max(Some(conn));
        self.pending_disable = None;
        if self.is_ending() {
            // No register went out on this connection: a paused unregister
            // re-send must not go out on it.
            self.connected = false;
            return;
        }
        self.connected = true;
        self.sent_register_this_conn = false;
        self.register_answered = false;
        self.outstanding = None;
        self.register_queued = false;
        self.overloaded_resends = 0;
        self.cancel_timers();
        if self.taken_over {
            return;
        }
        self.set_state(ExposeState::Registering);
        self.send_register();
    }

    /// An `app` body arrived on `conn`. Ignored when `conn` is superseded.
    /// Delivered after `conn` ended (MCPWARP.md §2.7 Ended connection),
    /// `registered`, `disable` and `enable` are ignored; `conflict` and
    /// `error` still apply, but only the live connection's replies settle
    /// the outstanding frame.
    pub fn on_app(&mut self, conn: ConnId, body: &Map<String, Value>) {
        if self.is_superseded(conn) {
            return;
        }
        match decode_envelope(body) {
            Ok(Some(
                InboundEnvelope::Registered { .. }
                | InboundEnvelope::Disable { .. }
                | InboundEnvelope::Enable { .. },
            )) if self.has_ended(conn) => {}
            Ok(Some(env)) => self.on_envelope(env, self.live == Some(conn)),
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
        if self.is_superseded(conn) {
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

    /// `conn` ended, or a dial for it failed. Decided by connection identity
    /// (MCPWARP.md §5.9 Disconnect, §6.8 Drain), never by state: returns
    /// `true` and leaves the session alone for a superseded connection, or
    /// for one it never saw welcomed while another is live (a replacement's
    /// failed dial while the drained connection still serves, T29; on a
    /// shared client, a connection that ended before this session existed).
    /// Otherwise the session is back to `connecting` (T30, T44), a pending
    /// register retry cancelled and a held `disable` dropped. A failure with
    /// no conn id needs no call. A welcomed `conn` is remembered as ended,
    /// for the ops delivered after its end ([`on_app`](Self::on_app)).
    pub fn on_disconnected(&mut self, conn: ConnId) -> bool {
        if self.superseded.remove(&conn) {
            self.remember_ended(conn, true);
            return true;
        }
        let welcomed = self.live == Some(conn) || self.draining == Some(conn);
        if !welcomed && self.live.is_some() {
            return true;
        }
        if welcomed {
            self.remember_ended(conn, false);
        }
        if self.draining == Some(conn) {
            self.draining = None;
        }
        if self.live == Some(conn) {
            self.live = None;
        }
        self.pending_disable = None;
        if self.is_ending() {
            // A paused unregister re-send must not fire on a later one.
            self.connected = false;
            return false;
        }
        self.connected = false;
        self.sent_register_this_conn = false;
        self.register_answered = false;
        self.outstanding = None;
        self.register_queued = false;
        self.cancel_timers();
        if !self.taken_over {
            self.set_state(ExposeState::Connecting);
        }
        false
    }

    /// Close requested (MCPWARP.md §5.7 steps 1-2): `unregister` if this
    /// connection sent `register`, is connected, and the edge may hold the
    /// name, then wait for `unregistered`, a whole-frame `error` answering
    /// the `unregister`, or [`UNREGISTER_BUDGET`]; a [`Command::CloseGraceful`]
    /// ends the wait. The edge may hold the name once a `registered` or a
    /// whole-frame `error` answered a `register` on this connection, or while
    /// a `register` is on the wire; not when every `register` was dropped by
    /// `OVERLOADED` (§3.2 Unregister). The `unregister` may go out while a
    /// `register` awaits its reply; that reply then settles the `register`,
    /// not the wait. While an `OVERLOADED` pauses the `unregister`, a
    /// whole-frame `error` answers nothing: the wait runs on to the re-sent
    /// `unregister`'s reply or the budget, which also cancels a re-send still
    /// paused. A no-op once closing.
    pub fn close(&mut self) {
        if self.is_ending() {
            return;
        }
        // A register paused by OVERLOADED was dropped: nothing answers it.
        let register_on_wire =
            self.outstanding == Some(Outbound::Register) && self.overload_timer.is_none();
        let unregister = self.connected
            && self.sent_register_this_conn
            && (self.register_answered || register_on_wire);
        self.register_ahead = unregister && register_on_wire;
        self.register_queued = false;
        self.set_state(ExposeState::Closing);
        self.cancel_timers();
        self.pending_disable = None;
        if !unregister {
            self.outstanding = None;
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
        self.register_queued = false;
        self.pending_disable = None;
        self.push(Command::CloseGraceful);
    }

    /// A [`Command::SendApp`] failed. A failed `unregister` ends the close
    /// wait at once; a failed `register` is left to the next welcome.
    pub fn on_send_failed(&mut self, op: Outbound) {
        // A failed register send means the connection is ending, so
        // `outstanding` stays set until the next welcome clears it.
        if op == Outbound::Unregister {
            self.end_unregister_wait();
        }
    }

    /// The timer `id` fired.
    pub fn on_timer(&mut self, id: TimerId) {
        match id.kind {
            TimerKind::RegisterRetry if self.retry_timer == Some(id) => {
                self.retry_timer = None;
                if !self.is_ending() {
                    self.request_register();
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
                // An unregister re-send still paused is past the budget.
                self.cancel_timers();
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

    /// `live`: `env` arrived on the live connection, so a reply settles the
    /// outstanding frame.
    fn on_envelope(&mut self, env: InboundEnvelope, live: bool) {
        match env {
            InboundEnvelope::Registered { services, errors } => {
                self.on_registered(services, errors, live);
            }
            InboundEnvelope::Unregistered { services } => {
                // The edge answers with no services when this connection held
                // nothing for the name.
                if self.outstanding == Some(Outbound::Unregister)
                    || services.iter().any(|s| s.name == self.name)
                {
                    self.transient_failures = 0;
                    if self.outstanding == Some(Outbound::Unregister) {
                        self.outstanding = None;
                        if live {
                            self.overloaded_resends = 0;
                            self.cancel_timers();
                        }
                    }
                    self.end_unregister_wait();
                }
            }
            InboundEnvelope::Disable { id, reason } => {
                if self.is_ending() || self.taken_over {
                    return;
                }
                match &self.server_id {
                    Some(known) if *known == id => self.apply_disable(reason),
                    Some(_) => {}
                    // The edge sends `disable` to every connection of the
                    // owner, so before our `registered` the id may be another
                    // service's: held until `registered` says (latest wins).
                    None => self.pending_disable = Some((id, reason)),
                }
            }
            InboundEnvelope::Enable { id, name } => {
                if self.is_ending() || self.taken_over {
                    return;
                }
                let ours = name == self.name || self.server_id.as_ref() == Some(&id);
                if !ours {
                    // An enable for the held disable's id ends the hold, even
                    // though it may be another service's (§2.7).
                    if self.pending_disable.as_ref().is_some_and(|(h, _)| *h == id) {
                        self.pending_disable = None;
                    }
                    return;
                }
                self.pending_disable = None;
                self.server_id = Some(id.clone());
                self.set_state(ExposeState::Registering);
                self.emit(SessionEvent::Enabled { server_id: id });
                self.request_register();
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
            InboundEnvelope::Error { code, message } => self.on_app_error(code, message, live),
            _ => {}
        }
    }

    fn on_registered(
        &mut self,
        services: Vec<crate::envelope::RegisteredService>,
        errors: Vec<crate::envelope::ServiceError>,
        live: bool,
    ) {
        // Not the live connection's reply (on a shared client, an older
        // connection's may arrive before this session's first welcome).
        if !live || self.taken_over {
            return;
        }
        self.register_answered = true;
        if self.is_ending() {
            // The reply to the register ahead of close's unregister: it
            // settles that register, the wait goes on.
            if live && self.register_ahead {
                self.register_ahead = false;
                self.overloaded_resends = 0;
            }
            return;
        }
        let listed = services.iter().any(|s| s.name == self.name)
            || errors.iter().any(|e| e.name == self.name);
        if !listed {
            // It answers the outstanding register all the same (§2.6 One in
            // flight): re-sent after the transient backoff. During an
            // OVERLOADED pause the dropped register is not on the wire, so
            // it answers nothing.
            if live && self.outstanding == Some(Outbound::Register) && self.overload_timer.is_none()
            {
                self.settle();
                self.schedule_transient_retry(UNLISTED_REASON.into());
                self.drain_queue();
            }
            return;
        }
        if live {
            self.settle();
        }
        self.on_own_entry(services, errors);
        self.drain_queue();
    }

    /// `registered` lists the service: in `services` or else in `errors`.
    fn on_own_entry(
        &mut self,
        services: Vec<crate::envelope::RegisteredService>,
        errors: Vec<crate::envelope::ServiceError>,
    ) {
        if let Some(svc) = services.into_iter().find(|s| s.name == self.name) {
            self.outstanding = None;
            self.transient_failures = 0;
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
            if let Some((id, reason)) = self.pending_disable.take() {
                if self.server_id.as_ref() == Some(&id) {
                    self.apply_disable(reason);
                }
            }
            return;
        }
        let Some(err) = errors.into_iter().find(|e| e.name == self.name) else {
            return;
        };
        self.outstanding = None;
        if !err.code.is_transient() {
            self.transient_failures = 0;
        }
        if err.code == RegisterErrorCode::ServerDisabled {
            // Disabled either way; one `disabled` event.
            self.pending_disable = None;
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
            self.schedule_transient_retry(reason);
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

    fn on_app_error(&mut self, code: AppErrorCode, message: String, live: bool) {
        match code {
            AppErrorCode::Overloaded => {
                // By frame order it drops the register ahead of close's
                // unregister: abandoned, the unregister still awaits its reply.
                // It still counts towards the next pause (§2.6).
                if live && self.register_ahead {
                    self.register_ahead = false;
                    self.overloaded_resends = self.overloaded_resends.saturating_add(1);
                    return;
                }
                // The outstanding frame was dropped: re-send it after the
                // backoff. An ended connection's OVERLOADED names no frame of
                // the live one.
                let Some(op) = self.outstanding else {
                    return;
                };
                if !live || self.overload_timer.is_some() {
                    return;
                }
                self.overloaded_resends = self.overloaded_resends.saturating_add(1);
                let delay = overload_delay(self.overloaded_resends);
                let id = self.start_timer(TimerKind::Overload, delay);
                self.overload_timer = Some((id, op));
                if op == Outbound::Register && !self.is_ending() {
                    self.emit(SessionEvent::Retry {
                        info: RetryInfo {
                            attempt: self.overloaded_resends,
                            delay,
                            reason: format!("register failed: {code}: {message}"),
                        },
                        kind: RegisterRetryKind::Overloaded,
                    });
                }
            }
            AppErrorCode::ConnectionLimit => self.emit_app_error(code, message),
            code => {
                // A whole-frame error answers the outstanding frame (§2.6 One
                // in flight), unknown codes included; during an OVERLOADED
                // pause the dropped frame is not on the wire, so it answers
                // nothing.
                if live && self.outstanding.is_some() && self.overload_timer.is_none() {
                    self.answer_with_error();
                }
                if code == AppErrorCode::UnsupportedVersion {
                    let error = McpwarpError::new(McpwarpErrorCode::App(code), message)
                        .with_fatal(true)
                        .with_service_name(self.name.clone());
                    self.fail(error);
                } else {
                    self.emit_app_error(code, message);
                }
                self.drain_queue();
            }
        }
    }

    /// A whole-frame `error` settles the outstanding frame. During `close`,
    /// frame order decides: it answers the `register` still ahead of the
    /// `unregister` if there is one, else the `unregister`, ending the wait.
    fn answer_with_error(&mut self) {
        if self.register_ahead {
            self.register_ahead = false;
            self.overloaded_resends = 0;
            return;
        }
        let answered = self.outstanding;
        self.settle();
        match answered {
            Some(Outbound::Register) => self.register_answered = true,
            Some(Outbound::Unregister) => self.end_unregister_wait(),
            None => {}
        }
    }

    fn emit_app_error(&mut self, code: AppErrorCode, message: String) {
        let error = McpwarpError::new(McpwarpErrorCode::App(code), message)
            .with_service_name(self.name.clone());
        self.emit(SessionEvent::Error {
            error,
            origin: ErrorOrigin::App,
        });
    }

    // --- helpers -----------------------------------------------------------

    fn take_over(&mut self, conflict: Conflict) {
        self.taken_over = true;
        self.outstanding = None;
        self.register_queued = false;
        self.pending_disable = None;
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

    /// A `disable` for the known server id.
    fn apply_disable(&mut self, reason: String) {
        self.set_state(ExposeState::Disabled);
        self.emit(SessionEvent::Disabled {
            server_id: self.server_id.clone(),
            reason,
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

    /// A reply answered the outstanding frame: the slot is free, the
    /// `OVERLOADED` counter reset and every pending re-send cancelled.
    fn settle(&mut self) {
        self.outstanding = None;
        self.overloaded_resends = 0;
        self.cancel_timers();
    }

    /// Re-send `register` after the transient backoff (MCPWARP.md §3.2).
    fn schedule_transient_retry(&mut self, reason: String) {
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
    }

    /// A trigger for `register`: sent now, or queued behind the outstanding
    /// frame (including one paused by `OVERLOADED`, whose re-send then
    /// carries it). Disconnected, the next welcome sends it.
    fn request_register(&mut self) {
        if !self.connected {
            return;
        }
        if self.outstanding.is_some() {
            self.register_queued = true;
        } else {
            self.send_register();
        }
    }

    /// Sends the queued `register` once nothing is outstanding.
    fn drain_queue(&mut self) {
        if self.register_queued
            && self.outstanding.is_none()
            && !self.is_ending()
            && !self.taken_over
        {
            self.register_queued = false;
            self.request_register();
        }
    }

    fn send_register(&mut self) {
        if let Some(id) = self.retry_timer.take() {
            self.push(Command::CancelTimer(id));
        }
        self.register_queued = false;
        self.outstanding = Some(Outbound::Register);
        self.sent_register_this_conn = true;
        self.push(Command::SendApp {
            body: encode_register(&self.name, self.kind),
            op: Outbound::Register,
        });
    }

    fn remember_ended(&mut self, conn: ConnId, superseded: bool) {
        if self.ended.len() == ENDED_MEMORY {
            self.ended.pop_front();
        }
        self.ended.push_back((conn, superseded));
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

/// The pause before the `n`-th consecutive `OVERLOADED` re-send (`n >= 1`):
/// `min(1000 * 2^(n-1), 30000)` ms.
fn overload_delay(n: u32) -> Duration {
    OVERLOAD_PAUSE
        .saturating_mul(1u32.checked_shl(n.saturating_sub(1)).unwrap_or(u32::MAX))
        .min(REGISTER_RETRY_CAP)
}

#[cfg(test)]
mod tests;

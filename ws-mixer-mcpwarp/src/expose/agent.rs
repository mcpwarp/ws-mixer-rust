//! The driver task: pumps the client's events into the [`Session`],
//! executes its commands, serves streams, and settles `expose()`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use ws_mixer::{ConnId, DisconnectReason, DrainReason, Event, RedialCause, RedialNotice, Stream};

use super::ExposeEvent;
use super::config::{ExposeConfig, OnRetry, ServeHandler};
use super::handle::{Control, Shared, Snapshot};
use super::observe::{ExposeObservation, ObservedError, RetryKind};
use crate::bridge::{RegistrationState, ServeContext, ServeState};
use crate::error::{McpwarpError, McpwarpErrorCode};
use crate::session::{
    Command, ErrorOrigin, ExposeState, RegisterRetryKind, RetryInfo, Session, SessionEvent, TimerId,
};
use crate::transport::{AppSender, BoxFut, EventSource, Transport};

/// An upper bound on the client's graceful close (its own grace is 5 s):
/// past it the close is counted done anyway.
pub(crate) const CLOSE_BOUND: Duration = Duration::from_secs(10);

/// How long a closed client's remaining events may take to arrive before
/// `Close` is sent anyway.
pub(crate) const EVENTS_AFTER_CLOSE: Duration = Duration::from_secs(1);

pub(crate) type Settle = oneshot::Receiver<Result<(), McpwarpError>>;

/// Spawns the driver for a started client. The returned sender is the
/// exposure's lifeline: when every clone is gone, the driver closes.
pub(crate) fn spawn(
    config: ExposeConfig,
    transport: Transport,
) -> (Arc<Shared>, mpsc::UnboundedSender<Control>, Settle) {
    let shared = Arc::new(Shared::new(config.observe.clone()));
    let (control_tx, control) = mpsc::unbounded_channel();
    let (settle_tx, settle_rx) = oneshot::channel();
    let state = shared.clone();
    let errors = shared.clone();
    let ctx = ServeContext::new(move || serve_state(state.state()))
        .max_request_body_bytes(config.max_request_body_bytes)
        .on_error(move |e| errors.handler_error(Arc::from(e)));
    let driver = Driver {
        session: Session::new(config.name.clone(), config.kind),
        name: config.name,
        sender: transport.sender,
        events: transport.events,
        events_done: false,
        control,
        control_done: false,
        shared: shared.clone(),
        ctx,
        handler: config.handler,
        on_retry: config.on_retry,
        timers: Vec::new(),
        settle: Some(settle_tx),
        closing: None,
        pending_error: None,
        early_takeover: false,
        draining: None,
        last_disconnect: None,
        finish_at: None,
        finished: false,
    };
    tokio::spawn(driver.run());
    (shared, control_tx, settle_rx)
}

/// What a request sees of the agent's state (MCPWARP.md §4.4 rows 0 and 3,
/// §5.7). Like the JS SDK, only `disabled`, `closing` and `closed` refuse:
/// a stream arriving while (re)registering is served.
pub(crate) fn serve_state(s: ExposeState) -> ServeState {
    match s {
        ExposeState::Connecting | ExposeState::Registering | ExposeState::Registered => {
            RegistrationState::Registered.into()
        }
        ExposeState::Disabled => RegistrationState::Disabled.into(),
        ExposeState::Closing | ExposeState::Closed => RegistrationState::Closing.into(),
        ExposeState::Conflict => ServeState {
            registration: RegistrationState::Unavailable,
            conflict: true,
        },
    }
}

/// Why the client is being closed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CloseKind {
    /// The exposure ends when the client is closed.
    Final,
    /// A take-over after `expose()` settled: the handle stays in
    /// `conflict` until [`ExposeHandle::close`](crate::ExposeHandle::close).
    TakeOver,
}

struct Closing {
    fut: BoxFut<'static, ()>,
    kind: CloseKind,
    /// Past it, a close that hangs is counted done.
    deadline: Instant,
}

enum Wake {
    Closed,
    Control(Option<Control>),
    Event(Option<Box<Event>>),
    Timer,
}

struct Driver {
    session: Session,
    name: String,
    sender: Arc<dyn AppSender>,
    events: Box<dyn EventSource>,
    events_done: bool,
    control: mpsc::UnboundedReceiver<Control>,
    control_done: bool,
    shared: Arc<Shared>,
    ctx: ServeContext,
    handler: Box<dyn ServeHandler>,
    on_retry: Option<Arc<OnRetry>>,
    timers: Vec<(TimerId, Instant)>,
    /// `Some` until `expose()` settles.
    settle: Option<oneshot::Sender<Result<(), McpwarpError>>>,
    closing: Option<Closing>,
    /// What the exposure ends with.
    pending_error: Option<McpwarpError>,
    /// Taken over before `expose()` settled: it rejects `TAKEN_OVER`.
    early_takeover: bool,
    /// A drained connection whose replacement has not been welcomed, for
    /// classifying a dial failure that has no connection id.
    draining: Option<ConnId>,
    /// The last disconnect's message, the `reason` of the redial it causes.
    last_disconnect: Option<String>,
    /// Set once the client is closed for good: when to send `close` at the
    /// latest if its events have not ended by then.
    finish_at: Option<Instant>,
    finished: bool,
}

impl Driver {
    async fn run(mut self) {
        loop {
            self.execute().await;
            // The client is closed; its last `Disconnected` events go out
            // before `close`, unless they take too long.
            if self
                .finish_at
                .is_some_and(|at| self.events_done || Instant::now() >= at)
            {
                self.session.on_closed();
                self.execute().await;
                self.finish();
            }
            if self.finished {
                return;
            }
            match self.wait().await {
                Wake::Closed => self.on_close_done(),
                Wake::Control(c) => self.on_control(c),
                Wake::Event(e) => self.on_event(e).await,
                Wake::Timer => self.fire_timers(),
            }
        }
    }

    async fn wait(&mut self) -> Wake {
        let next_timer = self
            .timers
            .iter()
            .map(|(_, at)| *at)
            .chain(self.finish_at)
            .min();
        let has_closing = self.closing.is_some();
        let closing = &mut self.closing;
        let control = &mut self.control;
        let events = &mut self.events;
        tokio::select! {
            biased;
            () = async {
                if let Some(c) = closing.as_mut() {
                    let _ = tokio::time::timeout_at(c.deadline, c.fut.as_mut()).await;
                }
            }, if has_closing => Wake::Closed,
            c = control.recv(), if !self.control_done => Wake::Control(c),
            e = events.recv(), if !self.events_done => Wake::Event(e.map(Box::new)),
            () = tokio::time::sleep_until(next_timer.unwrap_or_else(Instant::now)),
                if next_timer.is_some() => Wake::Timer,
        }
    }

    // --- session commands --------------------------------------------------

    async fn execute(&mut self) {
        self.publish_snapshot();
        while let Some(cmd) = self.session.poll_command() {
            match cmd {
                Command::SendApp { body, op } => {
                    if self.sender.send_app(body).await.is_err() {
                        self.session.on_send_failed(op);
                    }
                }
                Command::StartTimer { id, delay } => {
                    self.timers.retain(|(t, _)| *t != id);
                    self.timers.push((id, Instant::now() + delay));
                }
                Command::CancelTimer(id) => self.timers.retain(|(t, _)| *t != id),
                Command::Emit(event) => self.emit(event),
                Command::Fatal(error) => {
                    self.shared
                        .observe(&ExposeObservation::Error(ObservedError::Fatal {
                            error: Arc::new(error.clone()),
                        }));
                    self.pending_error = Some(error);
                    self.start_close(CloseKind::Final, None);
                }
                Command::CloseImmediate { message } => {
                    let kind = if self.early_takeover {
                        CloseKind::Final
                    } else {
                        CloseKind::TakeOver
                    };
                    self.start_close(kind, Some(message));
                }
                Command::CloseGraceful => self.start_close(CloseKind::Final, None),
            }
        }
    }

    fn publish_snapshot(&self) {
        self.shared.set_snapshot(Snapshot {
            url: self.session.url().map(str::to_owned),
            server_id: self.session.server_id().map(str::to_owned),
            registration: self.session.registration().cloned(),
        });
    }

    fn emit(&mut self, event: SessionEvent) {
        let shared = &self.shared;
        match event {
            SessionEvent::State(s) => shared.set_state(s),
            SessionEvent::Registered(reg) => {
                // Commands run after the whole input: a held `disable` that
                // this `registered` applied already shows in the state.
                shared.observe(&ExposeObservation::Registered {
                    registration: reg.clone(),
                    disabled_next: self.session.state() == ExposeState::Disabled,
                });
                shared.emit(ExposeEvent::Registered(reg));
                self.settle_ok();
            }
            SessionEvent::Disabled { server_id, reason } => {
                shared.observe(&ExposeObservation::Disabled {
                    server_id: server_id.clone(),
                    reason: reason.clone(),
                });
                shared.emit(ExposeEvent::Disabled { server_id, reason });
                self.settle_ok();
            }
            SessionEvent::Enabled { server_id } => {
                shared.observe(&ExposeObservation::Enabled {
                    server_id: server_id.clone(),
                });
                shared.emit(ExposeEvent::Enabled { server_id });
            }
            SessionEvent::Conflict(conflict) => {
                if self.settle.is_some() {
                    // Before the first registration there is no handle to
                    // put in `conflict`: expose() rejects (§5.4).
                    self.early_takeover = true;
                    self.pending_error = Some(
                        McpwarpError::new(
                            McpwarpErrorCode::TakenOver,
                            format!(
                                "mcpwarp: service \"{}\" was taken over by another connection before it registered",
                                self.name
                            ),
                        )
                        .with_fatal(true)
                        .with_service_name(self.name.clone()),
                    );
                } else {
                    shared.observe(&ExposeObservation::Conflict(conflict.clone()));
                    shared.emit(ExposeEvent::Conflict(conflict));
                }
            }
            SessionEvent::Error { error, origin } => {
                let error = Arc::new(error);
                let observed = match (&error.code, origin) {
                    (McpwarpErrorCode::Register(code), _) => ObservedError::Register {
                        error: error.clone(),
                        code: code.clone(),
                        transient: origin == ErrorOrigin::RegisterTransient,
                    },
                    (McpwarpErrorCode::App(code), _) => ObservedError::App {
                        error: error.clone(),
                        code: code.clone(),
                    },
                    _ => ObservedError::Malformed {
                        error: error.clone(),
                    },
                };
                shared.observe(&ExposeObservation::Error(observed));
                shared.emit(ExposeEvent::Error(error));
            }
            SessionEvent::Retry { info, kind } => {
                let kind = match kind {
                    RegisterRetryKind::Transient => RetryKind::Register,
                    RegisterRetryKind::Overloaded => RetryKind::Overloaded,
                };
                self.retry(info, kind);
            }
        }
    }

    fn settle_ok(&mut self) {
        if let Some(tx) = self.settle.take() {
            let _ = tx.send(Ok(()));
        }
    }

    fn retry(&self, info: RetryInfo, kind: RetryKind) {
        self.shared.observe(&ExposeObservation::Retry {
            info: info.clone(),
            kind,
        });
        let Some(f) = &self.on_retry else {
            return;
        };
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| f(&info))) {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".into());
            let e: crate::bridge::BoxError = format!("on_retry panicked: {msg}").into();
            self.shared.handler_error(Arc::from(e));
        }
    }

    fn start_close(&mut self, kind: CloseKind, immediate: Option<String>) {
        if let Some(c) = &mut self.closing {
            // A close is already running; the client ignores a second one.
            if kind == CloseKind::Final {
                c.kind = CloseKind::Final;
            }
            return;
        }
        let fut = match immediate {
            Some(message) => self.sender.close_immediate(message),
            None => self.sender.close(),
        };
        self.closing = Some(Closing {
            fut,
            kind,
            deadline: Instant::now() + CLOSE_BOUND,
        });
    }

    fn on_close_done(&mut self) {
        let Some(closing) = self.closing.take() else {
            return;
        };
        if closing.kind == CloseKind::Final {
            self.finish_at = Some(Instant::now() + EVENTS_AFTER_CLOSE);
        }
    }

    fn finish(&mut self) {
        self.finished = true;
        self.timers.clear();
        let error = self.pending_error.take();
        if let Some(tx) = self.settle.take() {
            let _ = tx.send(Err(error.clone().unwrap_or_else(|| {
                McpwarpError::new(
                    McpwarpErrorCode::Closed,
                    "mcpwarp: closed before the service was registered",
                )
                .with_fatal(true)
                .with_service_name(self.name.clone())
            })));
        }
        self.shared.finish(error.map(Arc::new));
    }

    fn fire_timers(&mut self) {
        let now = Instant::now();
        let mut due: Vec<_> = self
            .timers
            .iter()
            .filter(|(_, at)| *at <= now)
            .copied()
            .collect();
        due.sort_by_key(|(_, at)| *at);
        self.timers.retain(|(_, at)| *at > now);
        for (id, _) in due {
            self.session.on_timer(id);
        }
    }

    // --- handle ------------------------------------------------------------

    fn on_control(&mut self, control: Option<Control>) {
        match control {
            Some(Control::Close) => self.session.close(),
            None => {
                self.control_done = true;
                if self.settle.is_some() {
                    // expose() was dropped before it settled (§5.8): close
                    // the client, no unregister; requests get `503 service
                    // closing` meanwhile, and nothing registers again.
                    self.session.abort();
                } else {
                    // Every handle is gone.
                    self.session.close();
                }
            }
        }
    }

    // --- transport ---------------------------------------------------------

    fn ending(&self) -> bool {
        self.closing.is_some()
            || self.finish_at.is_some()
            || matches!(
                self.session.state(),
                ExposeState::Closing | ExposeState::Closed
            )
    }

    async fn on_event(&mut self, event: Option<Box<Event>>) {
        let Some(event) = event.map(|e| *e) else {
            self.events_done = true;
            self.on_transport_ended();
            return;
        };
        match event {
            Event::Connected(c) => {
                self.draining = None;
                self.session.on_welcome(c.conn_id);
            }
            Event::App(m) => self.session.on_app(m.conn_id, &m.body),
            Event::Stream(s) => self.serve(s),
            Event::Drain(d) => {
                if d.reason != DrainReason::Replaced {
                    self.draining = Some(d.conn_id);
                }
                // `replaced` is a take-over: the session queues the
                // immediate close; the transport dials no replacement.
                self.session.on_drain(d.conn_id, d.reason);
            }
            Event::Redial(r) => self.on_redial(&r),
            Event::Disconnected(r) => self.on_disconnected(r).await,
            _ => {}
        }
    }

    /// Never awaited here: one slow request must not hold up the rest.
    fn serve(&self, stream: Stream) {
        tokio::spawn(self.handler.serve(stream, &self.ctx));
    }

    fn on_redial(&mut self, r: &RedialNotice) {
        if self.session.is_taken_over() || self.ending() {
            return;
        }
        let (attempt, reason, kind) = match r.cause {
            RedialCause::Drain => (0, "drain".to_owned(), RetryKind::Drain),
            RedialCause::TokenRefresh => (1, "token-refresh".to_owned(), RetryKind::TokenRefresh),
            _ => (
                r.attempt,
                self.last_disconnect
                    .take()
                    .unwrap_or_else(|| r.cause.to_string()),
                RetryKind::Failure,
            ),
        };
        self.retry(
            RetryInfo {
                attempt,
                delay: r.delay,
                reason,
            },
            kind,
        );
    }

    async fn on_disconnected(&mut self, r: DisconnectReason) {
        let superseded = match r.conn_id {
            Some(conn) => {
                if self.draining == Some(conn) {
                    self.draining = None;
                }
                self.session.on_disconnected(conn)
            }
            // A dial that never made a connection changes nothing: either
            // no connection is live (already `connecting`), or it was a
            // drain replacement's while the drained one still serves.
            None => self.draining.is_some(),
        };
        // The state change goes out before the event, so a receiver woken
        // by `Disconnect` reads the new state.
        self.execute().await;
        self.last_disconnect = Some(r.message.clone());
        let fatal = r.fatal && !self.ending();
        let error = fatal.then(|| fatal_error(&r, &self.name));
        self.shared.observe(&ExposeObservation::Disconnect {
            reason: r.clone(),
            superseded,
        });
        if let Some(error) = &error {
            self.shared
                .observe(&ExposeObservation::Error(ObservedError::Transport {
                    error: Arc::new(error.clone()),
                    reason: r.clone(),
                }));
        }
        self.shared.emit(ExposeEvent::Disconnect(r));
        if let Some(error) = error {
            self.pending_error = Some(error);
            self.session.close();
        }
    }

    fn on_transport_ended(&mut self) {
        if self.ending() || self.session.is_taken_over() {
            return;
        }
        self.pending_error = Some(
            McpwarpError::new(
                McpwarpErrorCode::DisconnectedFatal,
                "mcpwarp: the ws-mixer client ended",
            )
            .with_fatal(true)
            .with_service_name(self.name.clone()),
        );
        self.session.close();
    }
}

/// A fatal transport outcome (MCPWARP.md §5.4): a rejected token is
/// `UNAUTHORIZED`, anything else `DISCONNECTED_FATAL`; the reason is the
/// source.
pub(crate) fn fatal_error(r: &DisconnectReason, name: &str) -> McpwarpError {
    let unauthorized = r.http_status == Some(401)
        || r.error_name == Some("UNAUTHORIZED")
        || r.ws_code == Some(4011);
    let code = if unauthorized {
        McpwarpErrorCode::Unauthorized
    } else {
        McpwarpErrorCode::DisconnectedFatal
    };
    McpwarpError::new(code, r.message.clone())
        .with_fatal(true)
        .with_service_name(name)
        .with_source(Arc::new(r.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_state_per_expose_state() {
        let reg = |s| serve_state(s).registration;
        assert_eq!(reg(ExposeState::Registered), RegistrationState::Registered);
        assert_eq!(reg(ExposeState::Registering), RegistrationState::Registered);
        assert_eq!(reg(ExposeState::Connecting), RegistrationState::Registered);
        assert_eq!(reg(ExposeState::Disabled), RegistrationState::Disabled);
        assert_eq!(reg(ExposeState::Closing), RegistrationState::Closing);
        assert_eq!(reg(ExposeState::Closed), RegistrationState::Closing);
        assert!(serve_state(ExposeState::Conflict).conflict);
        assert!(!serve_state(ExposeState::Registered).conflict);
    }
}

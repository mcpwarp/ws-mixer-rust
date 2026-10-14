//! [`ExposeHandle`] (MCPWARP.md §5.5) and the state it shares with the
//! driver.

use std::error::Error;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::{broadcast, mpsc, watch};

use super::ExposeEvent;
use super::observe::{ExposeObservation, Observe, ObservedError};
use crate::bridge::MCP_PATH;
use crate::error::McpwarpError;
use crate::session::{ExposeState, Registration};

/// How many events a slow [`ExposeHandle::events`] receiver may fall behind
/// before it lags (and skips the oldest).
pub(crate) const EVENT_CAPACITY: usize = 256;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the driver publishes and the handle reads.
pub(crate) struct Shared {
    pub(crate) state: watch::Sender<ExposeState>,
    snapshot: Mutex<Snapshot>,
    /// `None` once the exposure ended: receivers then see `Closed`.
    events: Mutex<Option<broadcast::Sender<ExposeEvent>>>,
    observe: Option<Arc<Observe>>,
    /// Set as `Close` is observed. Held across a handler error's check and
    /// observation, so one from a detached stream task is either observed
    /// before `Close` or dropped.
    ended: Mutex<bool>,
}

/// The session's last values, copied after every input.
#[derive(Clone, Debug, Default)]
pub(crate) struct Snapshot {
    pub(crate) url: Option<String>,
    pub(crate) server_id: Option<String>,
    pub(crate) registration: Option<Registration>,
}

impl Shared {
    pub(crate) fn new(observe: Option<Arc<Observe>>) -> Self {
        Self {
            state: watch::channel(ExposeState::Connecting).0,
            snapshot: Mutex::new(Snapshot::default()),
            events: Mutex::new(Some(broadcast::channel(EVENT_CAPACITY).0)),
            observe,
            ended: Mutex::new(false),
        }
    }

    pub(crate) fn state(&self) -> ExposeState {
        *self.state.borrow()
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        lock(&self.snapshot).clone()
    }

    pub(crate) fn set_snapshot(&self, s: Snapshot) {
        *lock(&self.snapshot) = s;
    }

    /// Publishes a state change.
    pub(crate) fn set_state(&self, s: ExposeState) {
        self.observe(&ExposeObservation::State(s));
        self.state.send_replace(s);
    }

    /// Calls the observation hook; a panic in it is dropped.
    pub(crate) fn observe(&self, o: &ExposeObservation) {
        if let Some(f) = &self.observe {
            let _ = catch_unwind(AssertUnwindSafe(|| f(o)));
        }
    }

    /// Broadcasts `event`; having no receiver is fine.
    pub(crate) fn emit(&self, event: ExposeEvent) {
        if let Some(tx) = &*lock(&self.events) {
            let _ = tx.send(event);
        }
    }

    /// Application code failed. Dropped once the exposure has ended.
    pub(crate) fn handler_error(&self, e: Arc<dyn Error + Send + Sync>) {
        let ended = lock(&self.ended);
        if *ended {
            return;
        }
        self.observe(&ExposeObservation::Error(ObservedError::Handler(e.clone())));
        self.emit(ExposeEvent::HandlerError(e));
    }

    /// Observes and broadcasts `Close`, the last of both, and ends every
    /// receiver.
    pub(crate) fn finish(&self, error: Option<Arc<McpwarpError>>) {
        {
            let mut ended = lock(&self.ended);
            *ended = true;
            self.observe(&ExposeObservation::Close {
                error: error.clone(),
            });
        }
        if let Some(tx) = lock(&self.events).take() {
            let _ = tx.send(ExposeEvent::Close { error });
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ExposeEvent> {
        match &*lock(&self.events) {
            Some(tx) => tx.subscribe(),
            // Ended: a receiver whose sender is gone reports `Closed`.
            None => broadcast::channel(1).1,
        }
    }
}

/// What a handle asks of the driver.
#[derive(Debug)]
pub(crate) enum Control {
    Close,
}

/// A live exposure: the service registered (or disabled) at the edge, its
/// requests served in the background (MCPWARP.md §5.5).
///
/// Cloning is cheap; clones share the exposure. Dropping the last clone
/// closes it like [`close`](Self::close), in the background.
///
/// [`Conflict`](ExposeState::Conflict) and [`Closed`](ExposeState::Closed)
/// are sticky, and events between `expose()` returning and
/// [`events`](Self::events) being called are not replayed: subscribe first,
/// then check [`state`](Self::state).
///
/// ```no_run
/// # async fn run(handle: ws_mixer_mcpwarp::ExposeHandle) {
/// use ws_mixer_mcpwarp::{ExposeEvent, ExposeState};
///
/// let mut events = handle.events();
/// if handle.state() == ExposeState::Conflict {
///     // Taken over before we subscribed.
/// }
/// while let Ok(event) = events.recv().await {
///     if let ExposeEvent::Conflict(c) = event {
///         eprintln!("taken over: {}", c.reason);
///     }
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct ExposeHandle {
    inner: Arc<Inner>,
}

struct Inner {
    shared: Arc<Shared>,
    control: mpsc::UnboundedSender<Control>,
    client: Option<ws_mixer::Client>,
}

impl fmt::Debug for ExposeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExposeHandle")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl ExposeHandle {
    pub(crate) fn new(
        shared: Arc<Shared>,
        control: mpsc::UnboundedSender<Control>,
        client: Option<ws_mixer::Client>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                shared,
                control,
                client,
            }),
        }
    }

    /// The public URL of the latest registration; never empty. `None` only
    /// when `expose()` resolved disabled and the service has not registered
    /// since.
    pub fn url(&self) -> Option<String> {
        self.inner.shared.snapshot().url
    }

    /// The latest server id, from `registered`, `disable`, `enable` or a
    /// `SERVER_DISABLED` refusal.
    pub fn server_id(&self) -> Option<String> {
        self.inner.shared.snapshot().server_id
    }

    /// The latest successful registration. After a take-over it keeps its
    /// last value, which now belongs to the new holder.
    pub fn registration(&self) -> Option<Registration> {
        self.inner.shared.snapshot().registration
    }

    /// Always `"/mcp"`, the only path requests are forwarded on.
    pub fn path(&self) -> &'static str {
        MCP_PATH
    }

    /// The current state. Already updated when an event about the change
    /// is received.
    pub fn state(&self) -> ExposeState {
        self.inner.shared.state()
    }

    /// A receiver that observes every state change.
    pub fn watch_state(&self) -> watch::Receiver<ExposeState> {
        self.inner.shared.state.subscribe()
    }

    /// The underlying ws-mixer client (escape hatch: stats, state). `None`
    /// only for an exposure driven by the `test-util` fake edge.
    pub fn client(&self) -> Option<&ws_mixer::Client> {
        self.inner.client.as_ref()
    }

    /// A receiver for the events from now on (MCPWARP.md §5.6). Events
    /// before the call are not replayed: the first
    /// [`Registered`](ExposeEvent::Registered) has already happened when
    /// `expose()` returns, read it from [`registration`](Self::registration);
    /// see the [type docs](Self) for the subscribe-then-check pattern.
    ///
    /// After [`Close`](ExposeEvent::Close), the last event, `recv` returns
    /// [`RecvError::Closed`](broadcast::error::RecvError::Closed); a receiver
    /// made after the exposure ended returns it at once. A receiver more
    /// than 256 events behind gets
    /// [`RecvError::Lagged`](broadcast::error::RecvError::Lagged) with the
    /// number skipped, then continues with the oldest event still held.
    pub fn events(&self) -> broadcast::Receiver<ExposeEvent> {
        self.inner.shared.subscribe()
    }

    /// Closes the exposure (MCPWARP.md §5.7): `unregister` if this
    /// connection registered, waiting up to 2 s for `unregistered`; then a
    /// graceful client close (`drain`, up to 5 s grace, close 1000), counted
    /// done after 10 s at most; then up to 1 s for the client's last
    /// [`Disconnect`](ExposeEvent::Disconnect) events, and
    /// [`Close`](ExposeEvent::Close). Returns once the exposure has ended
    /// (state [`Closed`](ExposeState::Closed), `Close` sent): about 13 s in
    /// the worst case, usually much less.
    ///
    /// Idempotent: every call waits for the same close. Once polled, the
    /// close is under way: dropping this future does not cancel it.
    pub async fn close(&self) {
        let _ = self.inner.control.send(Control::Close);
        // The driver drops its end once the exposure has ended.
        self.inner.control.closed().await;
    }
}

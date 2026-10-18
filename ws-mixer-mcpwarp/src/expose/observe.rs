//! The driver's observation hook, for the tunnel controller (MCPWARP.md §6):
//! the crate-private counterpart of the JS SDK's `observe`.
//!
//! Set it with [`ExposeConfig::observe`](super::ExposeConfig::observe). The
//! callback receives an [`ExposeObservation`] for everything the exposure
//! does, **from the start**: unlike [`ExposeEvent`](super::ExposeEvent)s,
//! observations made before `expose()` settles are not lost, and they carry
//! the classification the public events leave out (where an error came
//! from, whether a disconnect was a superseded connection's, what kind of
//! retry was scheduled). Decide by these fields, never by parsing messages.
//!
//! Ordering: each observation is made *immediately before* the matching
//! `ExposeEvent` is broadcast (and before `on_retry` is called), on the same
//! task, in the same order. `State` is observed when the handle's state
//! changes, so a `State` observation precedes the event that caused it (e.g.
//! `State(Registered)` then `Registered`). `Close` is the last observation.
//!
//! The callback runs on the driver task, and for `Error(Handler)` on a
//! stream's serving task: keep it short and never block. A panic in it is
//! caught and dropped.
//!
//! | Observation | When | Public event |
//! |---|---|---|
//! | `State(s)` | every state change, incl. `Closing` on a drop-before-settle | `watch_state` |
//! | `Registered(r)` | every successful `register`; `r.url` is the public URL | `Registered` |
//! | `Disabled{..}` | `disable` op or `SERVER_DISABLED` refusal | `Disabled` |
//! | `Enabled{..}` | matching `enable`; a `register` follows | `Enabled` |
//! | `Conflict(c)` | take-over **after** settle (before settle: no `Conflict`; `Close` carries `TAKEN_OVER`) | `Conflict` |
//! | `Error(Register{code, transient})` | a register refusal: transient (a `Retry{Register}` follows) or after the first registration | `Error` |
//! | `Error(App{code})` | an `error` op other than `UNSUPPORTED_VERSION` / `OVERLOADED` | `Error` |
//! | `Error(Malformed)` | an envelope that failed to decode | `Error` |
//! | `Error(Transport{reason})` | a fatal disconnect; the exposure then closes with this error | (`Close{error}`) |
//! | `Error(Handler)` | a handler error/panic, a response body failure, an `on_retry` panic | `HandlerError` |
//! | `Error(Fatal{error})` | the session failed (a refusal before the first registration, `UNSUPPORTED_VERSION`): observed when the driver starts closing the client, after `State(Closing)` and before the close runs; `Close{error}` carries the same error later | (`Close{error}`) |
//! | `Disconnect{reason, superseded}` | every transport disconnect; `superseded` = the session ignored it (a drained connection whose replacement welcomed, a replacement's failed dial while the drained one serves, or a connection the session never saw welcomed while another is live) | `Disconnect` |
//! | `Retry{info, kind}` | every scheduled retry: `Failure`/`TokenRefresh`/`Drain` redials, `Register` (transient) and `Overloaded` re-sends; fires whether or not `on_retry` is set; never for an abandoned `drain{replaced}` dial | (`on_retry`) |
//! | `Close{error}` | once, last; `error` is the fatal error (register refusal, `UNSUPPORTED_VERSION`, `UNAUTHORIZED`, `DISCONNECTED_FATAL`, `TAKEN_OVER`) or `None` after `close()` / an abort | `Close` |
//!
//! Fatal outcomes before the first registration also reject `expose()`
//! with the same error as `Close{error}`.

#![allow(
    dead_code,
    reason = "the fields are read by the tunnel controller (P4) and tests"
)]

use std::error::Error;
use std::sync::Arc;

use ws_mixer::DisconnectReason;

use crate::codes::{AppErrorCode, RegisterErrorCode};
use crate::error::McpwarpError;
use crate::session::{Conflict, ExposeState, Registration, RetryInfo};

/// The hook's type.
pub(crate) type Observe = dyn Fn(&ExposeObservation) + Send + Sync;

/// One thing the exposure did; see the [module docs](self).
#[derive(Clone, Debug)]
pub(crate) enum ExposeObservation {
    /// The state changed.
    State(ExposeState),
    /// A successful `register`.
    Registered(Registration),
    /// Disabled.
    Disabled {
        server_id: Option<String>,
        reason: String,
    },
    /// Re-enabled.
    Enabled { server_id: String },
    /// Taken over after settle.
    Conflict(Conflict),
    /// A classified error.
    Error(ObservedError),
    /// A transport disconnect.
    Disconnect {
        reason: DisconnectReason,
        /// The session ignored it: a drained connection whose replacement
        /// welcomed, a replacement's failed dial while the drained one
        /// serves, or a connection the session never saw welcomed while
        /// another is live.
        superseded: bool,
    },
    /// A retry was scheduled.
    Retry { info: RetryInfo, kind: RetryKind },
    /// The exposure ended.
    Close { error: Option<Arc<McpwarpError>> },
}

/// Where an error came from.
#[derive(Clone, Debug)]
pub(crate) enum ObservedError {
    /// A `registered.errors[]` entry for this name.
    Register {
        error: Arc<McpwarpError>,
        code: RegisterErrorCode,
        /// A retry follows.
        transient: bool,
    },
    /// An `error` op.
    App {
        error: Arc<McpwarpError>,
        code: AppErrorCode,
    },
    /// An envelope that failed to decode.
    Malformed { error: Arc<McpwarpError> },
    /// A fatal disconnect: `error` is `UNAUTHORIZED` or `DISCONNECTED_FATAL`.
    Transport {
        error: Arc<McpwarpError>,
        reason: DisconnectReason,
    },
    /// Application code failed.
    Handler(Arc<dyn Error + Send + Sync>),
    /// The session failed: the exposure is closing with `error`, which its
    /// `Close` repeats once the client has closed.
    Fatal { error: Arc<McpwarpError> },
}

/// What a [`ExposeObservation::Retry`] retries.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RetryKind {
    /// A redial after a failed dial or a lost connection (`info.attempt` is
    /// the transport's reconnect count).
    Failure,
    /// The one immediate redial after a rejected token (attempt 1, the delay
    /// as reported by the transport, currently 0).
    TokenRefresh,
    /// A drain's replacement dial (attempt 0, the actual delay).
    Drain,
    /// A `register` re-sent after a transient register error.
    Register,
    /// A `register` re-sent after `OVERLOADED`.
    Overloaded,
}

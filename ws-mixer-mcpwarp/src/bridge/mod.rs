//! Request bridging (MCPWARP.md §4): one HTTP/1.1 request per stream the
//! edge opens, answered by a [`Handler`].
//!
//! [`serve_stream`] is the whole of §4 for one stream: the head limits, the
//! `/mcp` path rule, availability, the body cap, the application call and
//! the response framing, in the §4.4 precedence order. `expose()` calls it
//! for every stream; an agent with its own connection management can call it
//! directly.
//!
//! ```no_run
//! use bytes::Bytes;
//! use http::Response;
//! use http_body_util::Full;
//! use ws_mixer_mcpwarp::bridge::{RegistrationState, ServeContext, handler_fn, serve_stream};
//!
//! # async fn run(mut events: ws_mixer::Events) {
//! let ctx = ServeContext::new(|| RegistrationState::Registered.into());
//! let app = handler_fn(|_req| async {
//!     Ok(Response::new(Full::new(Bytes::from_static(b"{}"))))
//! });
//! while let Some(event) = events.recv().await {
//!     if let ws_mixer::Event::Stream(stream) = event {
//!         tokio::spawn(serve_stream(stream, &ctx, &app));
//!     }
//! }
//! # }
//! ```

mod handler;
mod http1;
mod response;
#[cfg(feature = "target")]
mod target;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use ws_mixer::{ErrorCode, Stream};

pub use handler::{Handler, HandlerFn, RequestBody, handler_fn};
#[cfg(feature = "target")]
pub use target::{
    DEFAULT_TARGET_TIMEOUT, Target, TargetBody, TargetReply, TargetUrlError, target_reply,
};
pub use ws_mixer::BoxError;

use handler::Outcome;
use http1::{MAX_HEAD_BYTES, ReadError, Reader};
use response::Written;

/// The only request path forwarded: exact, case-sensitive, query ignored
/// (MCPWARP.md §4.3). Anything else is answered `404 {"error":"not_found"}`.
pub const MCP_PATH: &str = "/mcp";

/// The default request body cap: 4 MiB (`maxRequestBodyBytes`, MCPWARP.md
/// §4.2).
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: u64 = 4 * 1024 * 1024;

/// How long the edge has to deliver the request head, from the stream's
/// arrival: 10 s (MCPWARP.md §4.2). Past it the stream is reset with
/// `CANCEL`, without a response.
pub const HEAD_DEADLINE: Duration = Duration::from_secs(10);

/// Where the agent's registration stands, as far as a request is concerned
/// (MCPWARP.md §4.4 row 3).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RegistrationState {
    /// Requests reach the application.
    Registered,
    /// Disabled in the dashboard: `503 {"error":"service disabled"}`.
    Disabled,
    /// Closing or closed: `503 {"error":"service closing"}`.
    Closing,
    /// Unavailable for any other reason: `503 {"error":"service
    /// unavailable"}`.
    Unavailable,
}

/// The agent's state when a request arrives (the fixtures' `sdkState`,
/// MCPWARP.md §10.3).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ServeState {
    /// Whether requests reach the application, or which `503` they get.
    pub registration: RegistrationState,
    /// Taken over by another connection (§3.3): every stream is reset with
    /// `CANCEL` before anything is read (§4.4 row 0).
    pub conflict: bool,
}

impl ServeState {
    /// `registration`, not in conflict.
    pub fn new(registration: RegistrationState) -> Self {
        Self {
            registration,
            conflict: false,
        }
    }

    /// Sets the conflict flag.
    #[must_use]
    pub fn conflict(mut self, conflict: bool) -> Self {
        self.conflict = conflict;
        self
    }
}

impl From<RegistrationState> for ServeState {
    fn from(registration: RegistrationState) -> Self {
        Self::new(registration)
    }
}

/// What the agent knows about a request once its head is parsed (MCPWARP.md
/// §4.2), before any body byte is read: what
/// [`ServeContext::state_for_request`] decides by.
///
/// The strings are the head's bytes decoded as Latin-1, one char per byte:
/// a raw UTF-8 `café` reads `caf\u{c3}\u{a9}`.
#[derive(Debug)]
pub struct RequestInfo<'a> {
    host: &'a str,
    method: &'a str,
    target: &'a str,
}

impl RequestInfo<'_> {
    /// The `Host` header as received, case and port kept: compare hosts
    /// case-insensitively.
    pub fn host(&self) -> &str {
        self.host
    }

    /// The method as received.
    pub fn method(&self) -> &str {
        self.method
    }

    /// The path and query as received, not the escaped form the handler's
    /// URI carries. An absolute-form target is reduced to its path and
    /// query, and its fragment is dropped.
    pub fn target(&self) -> &str {
        self.target
    }
}

type StateFn = dyn Fn() -> ServeState + Send + Sync;
type RequestStateFn = dyn Fn(&RequestInfo<'_>) -> ServeState + Send + Sync;
type ErrorFn = dyn Fn(BoxError) + Send + Sync;

/// What [`serve_stream`] needs to know about the agent: its state (read
/// when the stream arrives for the conflict check and again after the head
/// for availability, or per request with
/// [`state_for_request`](Self::state_for_request)), the body cap, the head
/// deadline, and where non-fatal errors go.
///
/// Cloning is cheap; clones share the callbacks.
#[derive(Clone)]
pub struct ServeContext {
    state: Arc<StateFn>,
    request_state: Option<Arc<RequestStateFn>>,
    on_error: Option<Arc<ErrorFn>>,
    max_request_body_bytes: u64,
    head_deadline: Duration,
}

impl fmt::Debug for ServeContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServeContext")
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("head_deadline", &self.head_deadline)
            .field("state_for_request", &self.request_state.is_some())
            .finish_non_exhaustive()
    }
}

impl ServeContext {
    /// A context reading the agent's state from `state`, with the default
    /// body cap ([`DEFAULT_MAX_REQUEST_BODY_BYTES`]) and head deadline
    /// ([`HEAD_DEADLINE`]), and errors dropped.
    pub fn new(state: impl Fn() -> ServeState + Send + Sync + 'static) -> Self {
        Self {
            state: Arc::new(state),
            request_state: None,
            on_error: None,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            head_deadline: HEAD_DEADLINE,
        }
    }

    /// Per-request state read after the head and before the body, in place
    /// of the state given to [`new`](Self::new), which is then read only
    /// when the stream arrives (the §4.4 row 0 check). For a host serving
    /// several services on one client, routing by `Host`: a paused service
    /// gets its `503` (row 3) before its body is read, so an oversized body
    /// is not answered `413` (row 4) first.
    ///
    /// Called once the head parses, whatever the path; not called for a bad
    /// head. A state with `conflict` set resets the stream with `CANCEL`
    /// without reading the body, as row 0 does, on any path (row 0 beats
    /// the `404`). Otherwise its `registration` decides the `503` once the
    /// path is [`MCP_PATH`].
    ///
    /// The closure runs synchronously on the serving task: keep it cheap
    /// and never block.
    ///
    /// ```
    /// use ws_mixer_mcpwarp::bridge::{RegistrationState, ServeContext};
    ///
    /// let ctx = ServeContext::new(|| RegistrationState::Registered.into())
    ///     .state_for_request(|req| match req.host() {
    ///         "paused.example" => RegistrationState::Disabled.into(),
    ///         _ => RegistrationState::Registered.into(),
    ///     });
    /// # let _ = ctx;
    /// ```
    #[must_use]
    pub fn state_for_request(
        mut self,
        f: impl Fn(&RequestInfo<'_>) -> ServeState + Send + Sync + 'static,
    ) -> Self {
        self.request_state = Some(Arc::new(f));
        self
    }

    /// Sets the request body cap (`maxRequestBodyBytes`): a larger body is
    /// answered `413 {"error":"request body too large"}` without calling the
    /// application.
    #[must_use]
    pub fn max_request_body_bytes(mut self, max: u64) -> Self {
        self.max_request_body_bytes = max;
        self
    }

    /// Sets the head deadline. The spec's value is [`HEAD_DEADLINE`]; change
    /// it only in tests.
    #[must_use]
    pub fn head_deadline(mut self, deadline: Duration) -> Self {
        self.head_deadline = deadline;
        self
    }

    /// Receives the non-fatal errors of serving: a handler's error or panic
    /// (answered `500 {"error":"handler error"}`), a response body that
    /// failed mid-way, an unexpected failure (`500 {"error":"internal
    /// error"}`). Called on the serving task; keep it short.
    #[must_use]
    pub fn on_error(mut self, f: impl Fn(BoxError) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }

    fn report(&self, e: BoxError) {
        if let Some(f) = &self.on_error {
            f(e);
        }
    }
}

/// Serves one stream the edge opened: reads the HTTP/1.1 request, answers
/// it per MCPWARP.md §4, and ends the stream with `CLOSE` (or `RESET` where
/// the spec says so). Never fails; errors go to
/// [`ServeContext::on_error`].
///
/// The handler and the context are cloned before this returns, so the
/// future is `'static` and can be spawned. Spawn one per stream: streams
/// are served concurrently. Dropping the future drops the application call
/// and resets the stream with `CANCEL`.
///
/// Precedence (§4.4), first match answers:
///
/// 0. `conflict`: `RESET CANCEL "service taken over by another connection"`;
/// 1. a bad head: `431`, `400`, or past the deadline `RESET CANCEL`; then a
///    per-request [`state_for_request`](ServeContext::state_for_request) in
///    conflict: `RESET CANCEL`, as row 0, on any path;
/// 2. a path other than [`MCP_PATH`]: `404`;
/// 3. not [`RegistrationState::Registered`]: `503`;
/// 4. a body over the cap: `413`;
/// 5. a malformed body: `400`;
/// 6. the application.
///
/// After a refusal the rest of the request is read and dropped before the
/// stream is closed: a refused stream ends with `CLOSE`, never a `RESET`
/// (§4.4).
pub fn serve_stream<H: Handler + ?Sized>(
    stream: Stream,
    ctx: &ServeContext,
    handler: &H,
) -> impl Future<Output = ()> + Send + 'static {
    let responder = handler.responder();
    serve(stream, ctx.clone(), responder)
}

/// Aborts the application's task when serving is dropped or cancelled.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn serve(stream: Stream, ctx: ServeContext, responder: handler::Responder) {
    if (ctx.state)().conflict {
        stream.reset(ErrorCode::CANCEL, crate::session::TAKEN_OVER_MESSAGE);
        return;
    }
    let mut reader = Reader::new(stream);
    let raw = match tokio::time::timeout(ctx.head_deadline, reader.read_head(MAX_HEAD_BYTES)).await
    {
        Err(_) => {
            reader
                .get_ref()
                .reset(ErrorCode::CANCEL, "request head deadline exceeded");
            return;
        }
        Ok(Ok(raw)) => raw,
        Ok(Err(ReadError::Http(e))) => {
            return refuse(reader.into_inner(), e.status, &e.message, false).await;
        }
        Ok(Err(ReadError::Eof)) => {
            return refuse(reader.into_inner(), 400, "incomplete request", false).await;
        }
        Ok(Err(ReadError::Io)) => return,
    };
    let head = match http1::parse_head(&raw) {
        Ok(h) => h,
        Err(e) => return refuse(reader.into_inner(), e.status, &e.message, false).await,
    };
    let request_state = ctx.request_state.as_ref().map(|f| {
        f(&RequestInfo {
            host: &head.host,
            method: &head.method,
            target: &head.target,
        })
    });
    if request_state.is_some_and(|s| s.conflict) {
        reader
            .get_ref()
            .reset(ErrorCode::CANCEL, crate::session::TAKEN_OVER_MESSAGE);
        return;
    }
    let head_request = head.method == "HEAD";
    if http1::request_path(&head.target) != MCP_PATH {
        return refuse(reader.into_inner(), 404, "not_found", head_request).await;
    }
    let state = request_state.unwrap_or_else(|| (ctx.state)());
    let unavailable = match state.registration {
        RegistrationState::Registered => None,
        RegistrationState::Disabled => Some("service disabled"),
        RegistrationState::Closing => Some("service closing"),
        RegistrationState::Unavailable => Some("service unavailable"),
    };
    if let Some(reason) = unavailable {
        return refuse(reader.into_inner(), 503, reason, head_request).await;
    }
    let body = match http1::read_body(&mut reader, head.framing, ctx.max_request_body_bytes).await {
        Ok(b) => b,
        Err(ReadError::Http(e)) => {
            return refuse(reader.into_inner(), e.status, &e.message, head_request).await;
        }
        // read_body maps a clean EOF to its 400.
        Err(ReadError::Eof | ReadError::Io) => return,
    };
    let req = http1::ParsedRequest {
        method: head.method,
        target: http1::encode_target(&head.target),
        headers: head.headers,
        host: head.host,
        body,
    };
    let mut stream = reader.into_inner();

    let mut task = AbortOnDrop(tokio::spawn((responder.0)(req)));
    let joined = tokio::select! {
        biased;
        r = &mut task.0 => r,
        // The edge cancelled, or the connection is gone: drop the call.
        _ = stream.closed() => return,
    };
    let joined = joined.map(|outcome| {
        let Outcome::Response(res) = outcome else {
            return outcome;
        };
        let status = res.status().as_u16();
        match response::untouched_sdk_reply(&res) {
            // A Target's 502/504, possibly returned through a dispatching
            // handler, unedited.
            Some(r) => Outcome::Reply {
                status: r.status,
                message: r.message,
            },
            // §4.1: the agent never sends a 1xx.
            None if status < 200 => Outcome::Failed("handler returned a 1xx status".into()),
            None if status >= 600 => Outcome::Failed("handler returned an invalid status".into()),
            None => Outcome::Response(res),
        }
    });
    let (status, message) = match joined {
        Ok(Outcome::Response(res)) => {
            match response::write_response(&mut stream, res, head_request).await {
                Written::Complete => http1::drain(stream).await,
                Written::Gone | Written::Reset(None) => {}
                Written::Reset(Some(e)) => ctx.report(e),
            }
            return;
        }
        Ok(Outcome::Failed(e)) => {
            ctx.report(e);
            (500, "handler error".into())
        }
        Ok(Outcome::Reply { status, message }) => (status, message),
        Err(e) if e.is_panic() => {
            ctx.report(panic_error("handler", e.into_panic()));
            (500, "handler error".into())
        }
        Err(e) => {
            ctx.report(Box::new(e));
            (500, "internal error".into())
        }
    };
    refuse(stream, status, &message, head_request).await;
}

/// `"<what> panicked: <message>"` as an error.
pub(crate) fn panic_error(what: &str, payload: Box<dyn std::any::Any + Send>) -> BoxError {
    let msg = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into());
    format!("{what} panicked: {msg}").into()
}

/// Writes an SDK error reply and half-closes, while reading and dropping
/// whatever the edge still sends, so the stream ends with `CLOSE` in both
/// directions and is never reset (§4.4 "Ending a refused stream").
async fn refuse(stream: Stream, status: u16, message: &str, head_request: bool) {
    use tokio::io::AsyncWriteExt;

    let reply = response::error_reply(status, message, head_request);
    let (rd, mut wr) = stream.into_split();
    let write = async move {
        if wr.write_all(&reply).await.is_ok() {
            let _ = wr.shutdown().await;
        }
        wr
    };
    let (_wr, ()) = tokio::join!(write, http1::drain(rd));
}

#[cfg(test)]
mod tests;

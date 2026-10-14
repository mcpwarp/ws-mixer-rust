//! A fake edge for testing code that uses this crate (`test-util` feature):
//! [`FakeEdge`] stands in for the ws-mixer client under
//! [`expose()`](crate::expose()), so a test injects the client's events
//! (welcomes, `app` messages, drains, redials, disconnects, streams) and
//! observes what the agent sent and how it closed, without a socket.
//!
//! Injected events are queued until the exposure's client starts, so a
//! script can be set up front. Everything runs on the test's tokio runtime
//! and is friendly to a paused clock (`start_paused = true`): [`settle`]
//! lets spawned tasks run without moving time.
//!
//! ```
//! use bytes::Bytes;
//! use http_body_util::Full;
//! use ws_mixer_mcpwarp::bridge::handler_fn;
//! use ws_mixer_mcpwarp::test_util::{FakeEdge, Step};
//! use ws_mixer_mcpwarp::{ExposeConfig, ExposeState};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let config = ExposeConfig::builder()
//!     .token("t")
//!     .name("notes")
//!     .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
//!     .handler(handler_fn(|_req| async {
//!         Ok(http::Response::new(Full::new(Bytes::new())))
//!     }))
//!     .build()
//!     .unwrap();
//! let edge = FakeEdge::new();
//! let exposing = tokio::spawn(edge.expose(config));
//! edge.run([
//!     Step::Connected(1),
//!     Step::App(1, FakeEdge::registered("notes", "srv_1", "https://notes.example", true)),
//! ])
//! .await;
//! let handle = exposing.await.unwrap().unwrap();
//! assert_eq!(handle.state(), ExposeState::Registered);
//! assert_eq!(edge.sent_ops(), ["register"]);
//! # }
//! ```

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use ws_mixer::test_util::PeerHandle;
use ws_mixer::{
    AppMessage, BuildError, ClientBuilder, ConnId, Connected, DisconnectReason, DrainNotice, Event,
    RedialNotice, SendAppError, Stream,
};

use crate::MCPWARP_V;
use crate::error::McpwarpError;
use crate::expose::{ExposeConfig, ExposeHandle, expose_with};
use crate::transport::{AppSender, BoxFut, Connector, EventSource, Transport};

/// Lets every task that can run, run: about 100 scheduler rounds. Does not
/// move a paused clock.
pub async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

/// How the agent closed its client.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseCall {
    /// [`ws_mixer::Client::close`].
    Graceful,
    /// [`ws_mixer::Client::close_immediate`], with its message.
    Immediate(String),
}

/// One step of a script for [`FakeEdge::run`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Step {
    /// [`FakeEdge::connected`].
    Connected(u64),
    /// [`FakeEdge::app`].
    App(u64, Map<String, Value>),
    /// [`FakeEdge::drain`].
    Drain(DrainNotice),
    /// [`FakeEdge::redial`].
    Redial(RedialNotice),
    /// [`FakeEdge::disconnected`].
    Disconnected(DisconnectReason),
    /// Any event, as is.
    Event(Event),
    /// Sleeps (with a paused clock: advances time).
    Sleep(Duration),
}

/// A scripted stand-in for the edge and the ws-mixer client; see the
/// [module docs](self). Cloning is cheap; clones share the edge.
#[derive(Clone, Debug, Default)]
pub struct FakeEdge {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    /// The running client's event channel; `None` before the first connect
    /// and once the client closed.
    tx: Option<mpsc::UnboundedSender<Event>>,
    started: bool,
    pending: Vec<Event>,
    live: BTreeSet<u64>,
    sent: Vec<Value>,
    closes: Vec<CloseCall>,
    connects: usize,
    welcomes: usize,
    fail_sends: bool,
    hang_close: bool,
    next_stream: u32,
}

impl Inner {
    fn push(&mut self, event: Event) -> bool {
        match &self.tx {
            Some(tx) => tx.send(event).is_ok(),
            None if !self.started => {
                self.pending.push(event);
                true
            }
            None => false,
        }
    }

    /// What the real client does on close: every live connection reports
    /// one non-fatal `Disconnected`, then the events end.
    fn end_client(&mut self) {
        let live = std::mem::take(&mut self.live);
        if let Some(tx) = self.tx.take() {
            for conn in live {
                let _ = tx.send(Event::Disconnected(
                    DisconnectReason::new_for_test()
                        .with_conn_id(Some(ConnId::new_for_test(conn)))
                        .with_ws_code(Some(1000))
                        .with_message("client closed".into()),
                ));
            }
        }
    }
}

impl FakeEdge {
    /// A fresh edge with no client yet.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// [`expose()`](crate::expose) with this edge in place of the network.
    /// Each call starts a new client on the edge.
    pub fn expose(
        &self,
        config: ExposeConfig,
    ) -> impl Future<Output = Result<ExposeHandle, McpwarpError>> + Send + 'static {
        expose_with(config, Box::new(self.connector()))
    }

    /// The connector [`expose`](Self::expose) uses.
    pub(crate) fn connector(&self) -> FakeConnector {
        FakeConnector { edge: self.clone() }
    }

    // --- injecting ----------------------------------------------------------

    /// Delivers `event` to the client. Returns `false` if the client has
    /// closed (the event is dropped). Before a client starts, it is queued.
    pub fn send(&self, event: Event) -> bool {
        let mut inner = self.lock();
        match &event {
            Event::Connected(c) => {
                inner.live.insert(c.conn_id.get());
            }
            Event::Disconnected(r) => {
                if let Some(c) = r.conn_id {
                    inner.live.remove(&c.get());
                }
            }
            _ => {}
        }
        inner.push(event)
    }

    /// `conn` was welcomed.
    pub fn connected(&self, conn: u64) -> bool {
        let reconnect = {
            let mut inner = self.lock();
            inner.welcomes += 1;
            inner.welcomes > 1
        };
        self.send(Event::Connected(
            Connected::new_for_test()
                .with_conn_id(ConnId::new_for_test(conn))
                .with_reconnect(reconnect),
        ))
    }

    /// An `app` message on `conn`.
    pub fn app(&self, conn: u64, body: Map<String, Value>) -> bool {
        self.send(Event::App(
            AppMessage::new_for_test()
                .with_conn_id(ConnId::new_for_test(conn))
                .with_body(body),
        ))
    }

    /// A `drain`.
    pub fn drain(&self, notice: DrainNotice) -> bool {
        self.send(Event::Drain(notice))
    }

    /// A scheduled dial.
    pub fn redial(&self, notice: RedialNotice) -> bool {
        self.send(Event::Redial(notice))
    }

    /// A connection ended or a dial failed.
    pub fn disconnected(&self, reason: DisconnectReason) -> bool {
        self.send(Event::Disconnected(reason))
    }

    /// Opens a stream (in memory, [`Stream::new_in_memory`]) and returns the
    /// edge's end of it.
    pub fn stream(&self) -> PeerHandle {
        let id = {
            let mut inner = self.lock();
            inner.next_stream += 2;
            inner.next_stream - 1
        };
        let (stream, peer) = Stream::new_in_memory(id);
        self.send(Event::Stream(stream));
        peer
    }

    /// Plays `steps` in order, [`settle`]-ing after each.
    pub async fn run(&self, steps: impl IntoIterator<Item = Step>) {
        for step in steps {
            match step {
                Step::Connected(conn) => {
                    self.connected(conn);
                }
                Step::App(conn, body) => {
                    self.app(conn, body);
                }
                Step::Drain(d) => {
                    self.drain(d);
                }
                Step::Redial(r) => {
                    self.redial(r);
                }
                Step::Disconnected(r) => {
                    self.disconnected(r);
                }
                Step::Event(e) => {
                    self.send(e);
                }
                Step::Sleep(d) => tokio::time::sleep(d).await,
            }
            settle().await;
        }
    }

    /// Makes the agent's `send_app` calls fail (`NotConnected`) or succeed.
    pub fn fail_sends(&self, fail: bool) {
        self.lock().fail_sends = fail;
    }

    /// Makes the client's closes (graceful and immediate) hang forever, the
    /// event channel staying open: what the agent does when a close never
    /// completes. Closes are still recorded.
    pub fn hang_close(&self, hang: bool) {
        self.lock().hang_close = hang;
    }

    // --- observing ----------------------------------------------------------

    /// Every `app` body the agent sent, in order.
    pub fn sent(&self) -> Vec<Value> {
        self.lock().sent.clone()
    }

    /// The `op` of every `mcpwarp` envelope the agent sent, in order.
    pub fn sent_ops(&self) -> Vec<String> {
        self.lock()
            .sent
            .iter()
            .filter_map(|b| b["mcpwarp"]["op"].as_str().map(str::to_owned))
            .collect()
    }

    /// Every close the agent asked for, in order.
    pub fn closes(&self) -> Vec<CloseCall> {
        self.lock().closes.clone()
    }

    /// How many clients were started.
    pub fn connects(&self) -> usize {
        self.lock().connects
    }

    /// The connections welcomed and not yet ended.
    pub fn live(&self) -> Vec<u64> {
        self.lock().live.iter().copied().collect()
    }

    // --- envelopes ----------------------------------------------------------

    /// `{"mcpwarp": {"v": 1, "op": <op>, ...fields}}`.
    pub fn envelope(op: &str, fields: Value) -> Map<String, Value> {
        let mut env = Map::new();
        env.insert("v".into(), json!(MCPWARP_V));
        env.insert("op".into(), json!(op));
        if let Value::Object(f) = fields {
            env.extend(f);
        }
        let mut body = Map::new();
        body.insert("mcpwarp".into(), Value::Object(env));
        body
    }

    /// A `registered` with one service.
    pub fn registered(name: &str, id: &str, url: &str, created: bool) -> Map<String, Value> {
        Self::envelope(
            "registered",
            json!({"services": [{"name": name, "id": id, "url": url, "created": created}], "errors": []}),
        )
    }

    /// A `registered` with one error.
    pub fn register_error(name: &str, code: &str, message: &str) -> Map<String, Value> {
        Self::envelope(
            "registered",
            json!({"services": [], "errors": [{"name": name, "code": code, "message": message}]}),
        )
    }

    /// An `unregistered` for `name`.
    pub fn unregistered(name: &str, id: &str) -> Map<String, Value> {
        Self::envelope(
            "unregistered",
            json!({"services": [{"name": name, "id": id}]}),
        )
    }

    /// A `disable`.
    pub fn disable(id: &str, reason: &str) -> Map<String, Value> {
        Self::envelope("disable", json!({"id": id, "reason": reason}))
    }

    /// An `enable`.
    pub fn enable(id: &str, name: &str) -> Map<String, Value> {
        Self::envelope("enable", json!({"id": id, "name": name}))
    }

    /// A `conflict` with reason `taken_over`.
    pub fn conflict(id: &str, name: &str) -> Map<String, Value> {
        Self::envelope(
            "conflict",
            json!({"id": id, "name": name, "reason": "taken_over"}),
        )
    }

    /// An `error` op.
    pub fn error(code: &str, message: &str) -> Map<String, Value> {
        Self::envelope("error", json!({"code": code, "message": message}))
    }
}

/// Starts a fake client on a [`FakeEdge`]: the seam `expose()` dials
/// through.
#[derive(Clone, Debug)]
pub(crate) struct FakeConnector {
    edge: FakeEdge,
}

impl Connector for FakeConnector {
    fn connect(self: Box<Self>, _builder: ClientBuilder) -> Result<Transport, BuildError> {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut inner = self.edge.lock();
            inner.started = true;
            inner.connects += 1;
            for event in std::mem::take(&mut inner.pending) {
                let _ = tx.send(event);
            }
            inner.tx = Some(tx);
        }
        Ok(Transport {
            sender: Arc::new(FakeSender {
                edge: self.edge.clone(),
            }),
            events: Box::new(FakeEvents { rx }),
            client: None,
        })
    }
}

struct FakeSender {
    edge: FakeEdge,
}

impl AppSender for FakeSender {
    fn send_app(&self, body: Value) -> BoxFut<'_, Result<(), SendAppError>> {
        let result = {
            let mut inner = self.edge.lock();
            if inner.tx.is_none() {
                Err(SendAppError::Closed)
            } else if inner.fail_sends || inner.live.is_empty() {
                Err(SendAppError::NotConnected)
            } else {
                inner.sent.push(body);
                Ok(())
            }
        };
        Box::pin(async move { result })
    }

    fn close(&self) -> BoxFut<'static, ()> {
        self.close_as(CloseCall::Graceful)
    }

    fn close_immediate(&self, message: String) -> BoxFut<'static, ()> {
        self.close_as(CloseCall::Immediate(message))
    }
}

impl FakeSender {
    fn close_as(&self, call: CloseCall) -> BoxFut<'static, ()> {
        let mut inner = self.edge.lock();
        inner.closes.push(call);
        if inner.hang_close {
            return Box::pin(std::future::pending());
        }
        inner.end_client();
        Box::pin(async {})
    }
}

struct FakeEvents {
    rx: mpsc::UnboundedReceiver<Event>,
}

impl EventSource for FakeEvents {
    fn recv(&mut self) -> BoxFut<'_, Option<Event>> {
        Box::pin(self.rx.recv())
    }
}

//! Shared helpers for the `expose_*` tests, driven by the fake edge.
#![allow(dead_code, reason = "each test crate uses a different subset")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use ws_mixer::test_util::PeerHandle;
use ws_mixer::{ConnId, DisconnectReason, DrainNotice, DrainReason, RedialCause, RedialNotice};
use ws_mixer_mcpwarp::bridge::{Handler, RequestBody, handler_fn};
use ws_mixer_mcpwarp::test_util::{FakeEdge, settle};
use ws_mixer_mcpwarp::{
    ExposeConfig, ExposeConfigBuilder, ExposeEvent, ExposeHandle, McpwarpError, RetryInfo,
};

pub const ID: &str = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";
pub const NAME: &str = "notes";
pub const URL: &str = "https://notes-u1.mcpwarp.example";
pub const POST: &str = "POST /mcp HTTP/1.1\r\nHost: h.example\r\nContent-Length: 2\r\n\r\n{}";

pub fn echo() -> impl Handler + Clone + Sync {
    handler_fn(|req: http::Request<RequestBody>| async move {
        let body = req.into_body().collect().await?.to_bytes();
        let res = http::Response::builder()
            .header(http::header::CONTENT_LENGTH, body.len())
            .body(Full::new(body))?;
        Ok(res)
    })
}

pub fn builder() -> ExposeConfigBuilder {
    ExposeConfig::builder()
        .token("t")
        .name(NAME)
        .integration(ID)
        .handler(echo())
}

/// Records every `on_retry` call.
#[derive(Clone, Default)]
pub struct Retries(Arc<Mutex<Vec<RetryInfo>>>);

impl Retries {
    pub fn hook(&self, b: ExposeConfigBuilder) -> ExposeConfigBuilder {
        let calls = self.0.clone();
        b.on_retry(move |info| calls.lock().unwrap().push(info.clone()))
    }

    pub fn take(&self) -> Vec<(u32, Duration, String)> {
        std::mem::take(&mut *self.0.lock().unwrap())
            .into_iter()
            .map(|r| (r.attempt, r.delay, r.reason))
            .collect()
    }
}

pub type Exposing = JoinHandle<Result<ExposeHandle, McpwarpError>>;

/// Starts `expose()` on `edge` and lets its driver start.
pub async fn start(edge: &FakeEdge, config: ExposeConfig) -> Exposing {
    let task = tokio::spawn(edge.expose(config));
    settle().await;
    task
}

/// Welcomes connection 1 and registers: the handle, and its event receiver.
pub async fn registered(
    edge: &FakeEdge,
    config: ExposeConfig,
) -> (ExposeHandle, broadcast::Receiver<ExposeEvent>) {
    let task = start(edge, config).await;
    edge.connected(1);
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let handle = task.await.unwrap().unwrap();
    let events = handle.events();
    (handle, events)
}

pub fn conn(n: u64) -> ConnId {
    ConnId::new_for_test(n)
}

pub fn drain(n: u64, reason: DrainReason) -> DrainNotice {
    DrainNotice::new_for_test()
        .with_conn_id(conn(n))
        .with_reason(reason)
        .with_raw_reason(reason.as_str().into())
}

pub fn redial(n: Option<u64>, cause: RedialCause, attempt: u32, delay: Duration) -> RedialNotice {
    RedialNotice::new_for_test()
        .with_conn_id(n.map(conn))
        .with_cause(cause)
        .with_attempt(attempt)
        .with_delay(delay)
}

pub fn lost(n: u64, message: &str) -> DisconnectReason {
    DisconnectReason::new_for_test()
        .with_conn_id(Some(conn(n)))
        .with_ws_code(Some(1006))
        .with_message(message.into())
        .with_retry_in(Some(Duration::from_secs(1)))
}

/// Every event received so far.
pub fn drained(rx: &mut broadcast::Receiver<ExposeEvent>) -> Vec<ExposeEvent> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

/// A short tag per event, for order assertions.
pub fn tags(events: &[ExposeEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|e| match e {
            ExposeEvent::Registered(_) => "registered",
            ExposeEvent::Disabled { .. } => "disabled",
            ExposeEvent::Enabled { .. } => "enabled",
            ExposeEvent::Conflict(_) => "conflict",
            ExposeEvent::Error(_) => "error",
            ExposeEvent::HandlerError(_) => "handler_error",
            ExposeEvent::Disconnect(_) => "disconnect",
            ExposeEvent::Close { .. } => "close",
            _ => "other",
        })
        .collect()
}

/// Sends `request` on `peer` (and half-closes) and reads the reply to its
/// end.
pub async fn exchange(mut peer: PeerHandle, request: &str) -> String {
    peer.write(request.as_bytes()).unwrap();
    peer.close();
    let out = peer.read_to_end().await.unwrap();
    String::from_utf8(out).unwrap()
}

pub fn bytes(s: &'static str) -> Bytes {
    Bytes::from_static(s.as_bytes())
}

//! The observation hook, on the fake edge.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use ws_mixer::{
    ConnId, DisconnectReason, DrainNotice, DrainReason, Phase, RedialCause, RedialNotice,
};

use super::ExposeConfig;
use super::observe::{ExposeObservation, ObservedError};
use crate::bridge::handler_fn;
use crate::codes::AppErrorCode;
use crate::error::McpwarpErrorCode;
use crate::session::ExposeState;
use crate::test_util::{FakeEdge, settle};

const NAME: &str = "notes";
const URL: &str = "https://notes.example";

type Log = Arc<Mutex<Vec<ExposeObservation>>>;

fn config(log: &Log) -> ExposeConfig {
    let log = log.clone();
    ExposeConfig::builder()
        .token("t")
        .name(NAME)
        .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
        .handler(handler_fn(|_req| async {
            Err::<http::Response<Full<Bytes>>, _>("nope".into())
        }))
        .build()
        .unwrap()
        .observe(move |o| log.lock().unwrap().push(o.clone()))
}

fn take(log: &Log) -> Vec<ExposeObservation> {
    std::mem::take(&mut *log.lock().unwrap())
}

/// A compact rendering for order assertions.
fn tag(o: &ExposeObservation) -> String {
    match o {
        ExposeObservation::State(s) => format!("state:{s}"),
        ExposeObservation::Registered(r) => format!("registered:{}", r.url),
        ExposeObservation::Disabled { reason, .. } => format!("disabled:{reason}"),
        ExposeObservation::Enabled { .. } => "enabled".into(),
        ExposeObservation::Conflict(c) => format!("conflict:{}", c.reason),
        ExposeObservation::Error(e) => match e {
            ObservedError::Register {
                code, transient, ..
            } => format!("error:register:{code}:{transient}"),
            ObservedError::App { code, .. } => format!("error:app:{code}"),
            ObservedError::Malformed { .. } => "error:malformed".into(),
            ObservedError::Transport { error, reason } => {
                format!("error:transport:{}:{:?}", error.code, reason.http_status)
            }
            ObservedError::Handler(e) => format!("error:handler:{e}"),
            ObservedError::Fatal { error } => format!("error:fatal:{}", error.code),
        },
        ExposeObservation::Disconnect { superseded, .. } => format!("disconnect:{superseded}"),
        ExposeObservation::Retry { info, kind } => {
            format!("retry:{kind:?}:{}:{:?}", info.attempt, info.delay)
        }
        ExposeObservation::Close { error } => format!(
            "close:{}",
            error.as_ref().map_or("-".into(), |e| e.code.to_string())
        ),
    }
}

fn tags(log: &Log) -> Vec<String> {
    take(log).iter().map(tag).collect()
}

fn conn(n: u64) -> ConnId {
    ConnId::new_for_test(n)
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn observations_from_the_start() {
    let log = Log::default();
    let edge = FakeEdge::new();
    let task = tokio::spawn(edge.expose(config(&log)));
    settle().await;
    edge.redial(
        RedialNotice::new_for_test()
            .with_conn_id(None)
            .with_cause(RedialCause::TokenRefresh),
    );
    edge.connected(1);
    edge.app(1, FakeEdge::register_error(NAME, "TIMEOUT", "slow"));
    settle().await;
    // Before expose() settled: nothing is lost.
    assert_eq!(
        tags(&log),
        [
            "retry:TokenRefresh:1:0ns",
            "state:registering",
            "error:register:TIMEOUT:true",
            "retry:Register:1:1s",
        ]
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let handle = task.await.unwrap().unwrap();
    assert_eq!(
        tags(&log),
        ["state:registered", &format!("registered:{URL}")]
    );

    edge.app(1, FakeEdge::error("TOO_MANY_SERVICES", "x"));
    edge.app(1, FakeEdge::envelope("disable", serde_json::json!({})));
    edge.app(1, FakeEdge::register_error(NAME, "FORBIDDEN", "no"));
    edge.app(1, FakeEdge::error("OVERLOADED", "busy"));
    settle().await;
    let t = tags(&log);
    assert_eq!(
        t,
        [
            "error:app:TOO_MANY_SERVICES",
            "error:malformed",
            "error:register:FORBIDDEN:false",
        ]
    );

    // Disable, enable (the state before the event, as everywhere), then
    // OVERLOADED with the re-sent register outstanding.
    edge.app(1, FakeEdge::disable("srv_1", "maintenance"));
    edge.app(1, FakeEdge::enable("srv_1", NAME));
    edge.app(1, FakeEdge::error("OVERLOADED", "busy"));
    settle().await;
    assert_eq!(
        tags(&log),
        [
            "state:disabled",
            "disabled:maintenance",
            "state:registering",
            "enabled",
            "retry:Overloaded:1:1s",
        ]
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(
        edge.sent_ops(),
        ["register", "register", "register", "register"]
    );
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, false));
    settle().await;
    assert_eq!(
        tags(&log),
        ["state:registered", &format!("registered:{URL}")]
    );

    // A handler failure, observed from the stream's task.
    let mut peer = edge.stream();
    peer.write(b"POST /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n")
        .unwrap();
    peer.close();
    peer.read_to_end().await.unwrap();
    assert_eq!(tags(&log), ["error:handler:nope"]);

    // Drain hand-over: superseded old connection, a failed replacement dial
    // without a connection id while the drained one serves.
    edge.drain(
        DrainNotice::new_for_test()
            .with_conn_id(conn(1))
            .with_reason(DrainReason::Rollout),
    );
    edge.redial(
        RedialNotice::new_for_test()
            .with_conn_id(Some(conn(1)))
            .with_cause(RedialCause::Drain)
            .with_attempt(0)
            .with_delay(Duration::from_millis(800)),
    );
    edge.disconnected(
        DisconnectReason::new_for_test()
            .with_conn_id(None)
            .with_phase(Phase::Dial),
    );
    edge.redial(
        RedialNotice::new_for_test()
            .with_conn_id(None)
            .with_attempt(1)
            .with_delay(Duration::from_millis(300)),
    );
    edge.connected(2);
    edge.app(2, FakeEdge::registered(NAME, "srv_1", URL, false));
    edge.disconnected(DisconnectReason::new_for_test().with_conn_id(Some(conn(1))));
    settle().await;
    assert_eq!(
        tags(&log),
        [
            "retry:Drain:0:800ms",
            "disconnect:true",
            "retry:Failure:1:300ms",
            "state:registering",
            "state:registered",
            &format!("registered:{URL}"),
            "disconnect:true",
        ]
    );

    // The live connection's end: the state change is observed first.
    edge.disconnected(DisconnectReason::new_for_test().with_conn_id(Some(conn(2))));
    settle().await;
    assert_eq!(tags(&log), ["state:connecting", "disconnect:false"]);

    // A fatal one: classified as transport, then the close.
    edge.disconnected(
        DisconnectReason::new_for_test()
            .with_conn_id(None)
            .with_phase(Phase::Dial)
            .with_http_status(Some(401))
            .with_fatal(true),
    );
    settle().await;
    assert_eq!(
        tags(&log),
        [
            "disconnect:false",
            "error:transport:UNAUTHORIZED:Some(401)",
            "state:closing",
            "state:closed",
            "close:UNAUTHORIZED",
        ]
    );
    assert_eq!(handle.state(), ExposeState::Closed);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn take_overs_and_fatal_refusals() {
    // Before settle: no conflict observation; the close carries TAKEN_OVER.
    let log = Log::default();
    let edge = FakeEdge::new();
    let task = tokio::spawn(edge.expose(config(&log)));
    settle().await;
    edge.connected(1);
    edge.drain(
        DrainNotice::new_for_test()
            .with_conn_id(conn(1))
            .with_reason(DrainReason::Replaced),
    );
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(err.code, McpwarpErrorCode::TakenOver);
    settle().await;
    assert_eq!(
        tags(&log),
        [
            "state:registering",
            "state:conflict",
            "disconnect:false",
            "state:closed",
            "close:TAKEN_OVER",
        ]
    );

    // After settle: the conflict is observed, nothing closes the exposure.
    let log = Log::default();
    let edge = FakeEdge::new();
    let task = tokio::spawn(edge.expose(config(&log)));
    settle().await;
    edge.connected(1);
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let _handle = task.await.unwrap().unwrap();
    take(&log);
    edge.app(1, FakeEdge::conflict("srv_1", NAME));
    settle().await;
    assert_eq!(
        tags(&log),
        ["state:conflict", "conflict:taken_over", "disconnect:false"]
    );

    // A fatal UNSUPPORTED_VERSION app error.
    let log = Log::default();
    let edge = FakeEdge::new();
    let task = tokio::spawn(edge.expose(config(&log)));
    settle().await;
    edge.connected(1);
    edge.app(1, FakeEdge::error("UNSUPPORTED_VERSION", "v2"));
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(
        err.code,
        McpwarpErrorCode::App(AppErrorCode::UnsupportedVersion)
    );
    settle().await;
    let t = tags(&log);
    assert_eq!(
        t,
        [
            "state:registering",
            "state:closing",
            "error:fatal:UNSUPPORTED_VERSION",
            "disconnect:false",
            "state:closed",
            "close:UNSUPPORTED_VERSION",
        ]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_panicking_hook_is_dropped() {
    let edge = FakeEdge::new();
    let config = config(&Log::default()).observe(|_| panic!("observer"));
    let task = tokio::spawn(edge.expose(config));
    settle().await;
    edge.connected(1);
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    assert_eq!(
        task.await.unwrap().unwrap().state(),
        ExposeState::Registered
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_abort_ignores_everything_after_it() {
    let log = Log::default();
    let edge = FakeEdge::new();
    // The client's close never completes: the exposure stays in its grace.
    edge.hang_close(true);
    let task = tokio::spawn(edge.expose(config(&log)));
    settle().await;
    edge.connected(1);
    settle().await;
    task.abort();
    settle().await;
    assert_eq!(tags(&log), ["state:registering", "state:closing"]);

    // A late registration is neither observed nor settled.
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    // A transient error arms no retry.
    edge.app(1, FakeEdge::register_error(NAME, "TIMEOUT", "slow"));
    settle().await;
    assert!(tags(&log).is_empty());
    tokio::time::sleep(Duration::from_secs(5)).await;
    settle().await;
    assert!(tags(&log).is_empty());
    assert_eq!(edge.sent_ops(), ["register"]);

    // The close bound, then the wait for the client's last events.
    tokio::time::sleep(Duration::from_secs(6)).await;
    settle().await;
    assert_eq!(tags(&log), ["state:closed", "close:-"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn no_handler_error_is_observed_after_close() {
    let log = Log::default();
    let release = Arc::new(tokio::sync::Notify::new());
    let config = {
        let log = log.clone();
        let release = release.clone();
        ExposeConfig::builder()
            .token("t")
            .name(NAME)
            .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
            .handler(handler_fn(move |_req| {
                let release = release.clone();
                async move {
                    release.notified().await;
                    Err::<http::Response<Full<Bytes>>, _>("late".into())
                }
            }))
            .build()
            .unwrap()
            .observe(move |o| log.lock().unwrap().push(o.clone()))
    };
    let edge = FakeEdge::new();
    let task = tokio::spawn(edge.expose(config));
    settle().await;
    edge.connected(1);
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let handle = task.await.unwrap().unwrap();
    let mut peer = edge.stream();
    peer.write(b"POST /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n")
        .unwrap();
    peer.close();
    settle().await;

    edge.fail_sends(true);
    handle.close().await;
    assert_eq!(tags(&log).last().map(String::as_str), Some("close:-"));

    // The detached request now fails: answered, but not observed.
    release.notify_one();
    let reply = String::from_utf8(peer.read_to_end().await.unwrap()).unwrap();
    assert!(reply.starts_with("HTTP/1.1 500 "), "{reply}");
    settle().await;
    assert!(tags(&log).is_empty());
}

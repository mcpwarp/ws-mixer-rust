//! Take-over (conflict op, `drain{replaced}`) and the drain hand-over
//! (MCPWARP.md §3.3, §5.9), on the fake edge.
#![cfg(feature = "test-util")]

mod expose_support;

use std::time::Duration;

use expose_support::*;
use ws_mixer::{DrainReason, RedialCause};
use ws_mixer_mcpwarp::test_util::{CloseCall, FakeEdge, settle};
use ws_mixer_mcpwarp::{Conflict, ExposeEvent, ExposeState, McpwarpErrorCode};

const TAKEN_OVER: &str = "service taken over by another connection";

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn conflict_op_after_settle_takes_over() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let (handle, mut events) = registered(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.app(1, FakeEdge::conflict("srv_1", NAME));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Conflict);
    assert_eq!(edge.closes(), [CloseCall::Immediate(TAKEN_OVER.into())]);
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["conflict", "disconnect"]);
    let ExposeEvent::Conflict(c) = &ev[0] else {
        panic!("{ev:?}")
    };
    assert_eq!(
        c,
        &Conflict {
            server_id: Some("srv_1".into()),
            name: NAME.into(),
            reason: "taken_over".into()
        }
    );
    // Last values are kept; nothing registers again.
    assert_eq!(handle.url().as_deref(), Some(URL));
    tokio::time::sleep(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(handle.state(), ExposeState::Conflict);
    assert_eq!(edge.sent_ops(), ["register"]);
    assert!(retries.take().is_empty());
    // close() still works; no unregister after a take-over.
    handle.close().await;
    assert_eq!(handle.state(), ExposeState::Closed);
    assert_eq!(edge.sent_ops(), ["register"]);
    assert_eq!(
        edge.closes(),
        [CloseCall::Immediate(TAKEN_OVER.into()), CloseCall::Graceful]
    );
    assert_eq!(tags(&drained(&mut events)), ["close"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn conflict_op_before_settle_rejects_taken_over() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    edge.connected(1);
    edge.app(1, FakeEdge::conflict("srv_1", NAME));
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(err.code, McpwarpErrorCode::TakenOver);
    assert_eq!(
        err.message,
        "mcpwarp: service \"notes\" was taken over by another connection before it registered"
    );
    assert!(err.fatal);
    assert_eq!(edge.closes(), [CloseCall::Immediate(TAKEN_OVER.into())]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn someone_elses_conflict_is_ignored() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.app(1, FakeEdge::conflict("srv_2", "other"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registered);
    assert!(edge.closes().is_empty());
    assert!(drained(&mut events).is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn drain_replaced_after_settle_is_a_take_over_without_on_retry() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let (handle, mut events) = registered(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.drain(drain(1, DrainReason::Replaced));
    // The transport vetoes the replacement dial for `replaced`, so no Redial
    // follows; one arriving anyway (a race) must not reach on_retry either.
    edge.redial(redial(
        Some(1),
        RedialCause::Drain,
        0,
        Duration::from_millis(300),
    ));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Conflict);
    assert_eq!(edge.closes(), [CloseCall::Immediate(TAKEN_OVER.into())]);
    assert!(retries.take().is_empty());
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["conflict", "disconnect"]);
    assert!(matches!(
        &ev[0],
        ExposeEvent::Conflict(Conflict { server_id: Some(id), name, reason })
            if id == "srv_1" && name == NAME && reason == "replaced"
    ));
    assert_eq!(edge.sent_ops(), ["register"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn drain_replaced_with_no_server_id_omits_it() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    edge.connected(1);
    edge.app(1, FakeEdge::register_error(NAME, "SERVER_DISABLED", "off"));
    let handle = task.await.unwrap().unwrap();
    assert_eq!(handle.server_id(), None);
    let mut events = handle.events();
    edge.drain(drain(1, DrainReason::Replaced));
    settle().await;
    let ev = drained(&mut events);
    assert!(
        matches!(
            &ev[0],
            ExposeEvent::Conflict(Conflict {
                server_id: None,
                ..
            })
        ),
        "{ev:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn drain_replaced_before_settle_rejects_taken_over() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let task = start(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.connected(1);
    edge.drain(drain(1, DrainReason::Replaced));
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(err.code, McpwarpErrorCode::TakenOver);
    assert_eq!(edge.closes(), [CloseCall::Immediate(TAKEN_OVER.into())]);
    assert!(retries.take().is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn streams_after_a_take_over_are_reset() {
    let edge = FakeEdge::new();
    let (_handle, _events) = registered(&edge, builder().build().unwrap()).await;
    edge.app(1, FakeEdge::conflict("srv_1", NAME));
    // Queued before the close takes effect.
    let mut peer = edge.stream();
    settle().await;
    assert_eq!(
        peer.recv().await,
        Some(ws_mixer::test_util::PeerEvent::Reset {
            code: ws_mixer::ErrorCode::CANCEL,
            message: TAKEN_OVER.into()
        })
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn drain_rollout_hands_over_and_supersedes_the_old_connection() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let (handle, mut events) = registered(&edge, retries.hook(builder()).build().unwrap()).await;

    // An in-flight request on the old connection.
    let mut old_stream = edge.stream();
    settle().await;
    old_stream
        .write(b"POST /mcp HTTP/1.1\r\nHost: h.example\r\nContent-Length: 5\r\n\r\nhel")
        .unwrap();

    edge.drain(drain(1, DrainReason::Rollout));
    edge.redial(redial(
        Some(1),
        RedialCause::Drain,
        0,
        Duration::from_millis(1234),
    ));
    settle().await;
    assert_eq!(
        retries.take(),
        [(0, Duration::from_millis(1234), "drain".into())]
    );
    assert_eq!(handle.state(), ExposeState::Registered);

    edge.connected(2);
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "register"]);
    edge.app(2, FakeEdge::registered(NAME, "srv_1", URL, false));
    settle().await;

    // The superseded connection's messages are ignored...
    edge.app(1, FakeEdge::disable("srv_1", "stale"));
    edge.app(1, FakeEdge::conflict("srv_1", NAME));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registered);
    assert!(edge.closes().is_empty());
    // ...and its end leaves the state alone, though it is reported.
    edge.disconnected(lost(1, "drained"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registered);
    assert!(retries.take().is_empty());

    // The in-flight request still completes.
    old_stream.write(b"lo").unwrap();
    old_stream.close();
    let reply = String::from_utf8(old_stream.read_to_end().await.unwrap()).unwrap();
    assert!(reply.starts_with("HTTP/1.1 200 "), "{reply}");
    assert!(reply.ends_with("\r\n\r\nhello"), "{reply}");

    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["registered", "disconnect"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn drained_connection_ending_before_the_replacement_welcomes_is_a_loss() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    edge.drain(drain(1, DrainReason::Maintenance));
    edge.disconnected(lost(1, "gone"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Connecting);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn failed_replacement_dial_while_the_drained_one_serves_is_ignored() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    edge.drain(drain(1, DrainReason::Rollout));
    edge.disconnected(lost(5, "dial failed"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registered);
}

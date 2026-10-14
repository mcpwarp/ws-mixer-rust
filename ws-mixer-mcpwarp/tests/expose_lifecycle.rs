//! `expose()` resolution, events and `close()` (MCPWARP.md §5.4-§5.8), on the
//! fake edge.
#![cfg(feature = "test-util")]

mod expose_support;

use std::time::Duration;

use expose_support::*;
use tokio::time::Instant;
use ws_mixer::{DisconnectReason, Phase};
use ws_mixer_mcpwarp::test_util::{CloseCall, FakeEdge, settle};
use ws_mixer_mcpwarp::{
    AppErrorCode, ExposeEvent, ExposeState, McpwarpErrorCode, RegisterErrorCode, Registration,
};

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn registers_and_resolves() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    assert_eq!(edge.connects(), 1);
    assert!(edge.sent().is_empty());
    edge.connected(1);
    settle().await;
    assert_eq!(
        edge.sent()[0].to_string(),
        r#"{"mcpwarp":{"v":1,"op":"register","services":[{"name":"notes","kind":"http"}]}}"#
    );
    assert!(!task.is_finished());
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let handle = task.await.unwrap().unwrap();
    assert_eq!(handle.state(), ExposeState::Registered);
    assert_eq!(handle.url().as_deref(), Some(URL));
    assert_eq!(handle.server_id().as_deref(), Some("srv_1"));
    assert_eq!(
        handle.registration(),
        Some(Registration {
            url: URL.into(),
            server_id: "srv_1".into(),
            created: true
        })
    );
    assert_eq!(handle.path(), "/mcp");
    assert!(handle.client().is_none());
    assert_eq!(*handle.watch_state().borrow(), ExposeState::Registered);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_welcome_registers_again() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.disconnected(lost(1, "connection lost"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Connecting);
    edge.connected(2);
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registering);
    edge.app(2, FakeEdge::registered(NAME, "srv_1", URL, false));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registered);
    assert_eq!(edge.sent_ops(), ["register", "register"]);
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["disconnect", "registered"]);
    assert!(matches!(&ev[1], ExposeEvent::Registered(r) if !r.created));
    assert!(!handle.registration().unwrap().created);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn resolves_disabled_without_a_url() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    edge.connected(1);
    edge.app(
        1,
        FakeEdge::envelope(
            "registered",
            serde_json::json!({"errors": [{"name": NAME, "code": "SERVER_DISABLED", "message": "off", "id": "srv_9"}]}),
        ),
    );
    let handle = task.await.unwrap().unwrap();
    assert_eq!(handle.state(), ExposeState::Disabled);
    assert_eq!(handle.url(), None);
    assert_eq!(handle.registration(), None);
    assert_eq!(handle.server_id().as_deref(), Some("srv_9"));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn transient_register_error_retries_with_backoff() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let task = start(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.connected(1);
    edge.app(1, FakeEdge::register_error(NAME, "TIMEOUT", "slow"));
    settle().await;
    assert_eq!(
        retries.take(),
        [(
            1,
            Duration::from_secs(1),
            "register failed: TIMEOUT: slow".into()
        )]
    );
    assert_eq!(edge.sent_ops(), ["register"]);
    tokio::time::sleep(Duration::from_millis(999)).await;
    settle().await;
    assert_eq!(edge.sent_ops(), ["register"]);
    tokio::time::sleep(Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "register"]);
    edge.app(1, FakeEdge::register_error(NAME, "UNAVAILABLE", "again"));
    settle().await;
    assert_eq!(
        retries.take(),
        [(
            2,
            Duration::from_secs(2),
            "register failed: UNAVAILABLE: again".into()
        )]
    );
    assert!(!task.is_finished());
    tokio::time::sleep(Duration::from_secs(2)).await;
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "register", "register"]);
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let handle = task.await.unwrap().unwrap();
    assert_eq!(handle.state(), ExposeState::Registered);
    // A success resets the backoff: the next failure waits 1 s again.
    edge.app(1, FakeEdge::register_error(NAME, "INTERNAL", "x"));
    settle().await;
    assert_eq!(retries.take()[0].1, Duration::from_secs(1));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn non_transient_register_error_is_fatal() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let task = start(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.connected(1);
    edge.app(
        1,
        FakeEdge::register_error(NAME, "QUOTA_EXCEEDED", "over quota"),
    );
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(
        err.code,
        McpwarpErrorCode::Register(RegisterErrorCode::QuotaExceeded)
    );
    assert_eq!(err.message, "over quota");
    assert!(err.fatal);
    assert_eq!(err.service_name.as_deref(), Some(NAME));
    assert!(retries.take().is_empty());
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    assert_eq!(edge.sent_ops(), ["register"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unsupported_version_is_fatal() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    edge.connected(1);
    edge.app(1, FakeEdge::error("UNSUPPORTED_VERSION", "v2 only"));
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(
        err.code,
        McpwarpErrorCode::App(AppErrorCode::UnsupportedVersion)
    );
    assert!(err.fatal);
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
}

fn refused(status: u16) -> DisconnectReason {
    DisconnectReason::new_for_test()
        .with_conn_id(None)
        .with_phase(Phase::Dial)
        .with_http_status(Some(status))
        .with_fatal(true)
        .with_message(format!("upgrade refused: HTTP {status}"))
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fatal_transport_outcomes_before_settle() {
    let cases = [
        (refused(401), McpwarpErrorCode::Unauthorized),
        (
            refused(403).with_http_status(Some(403)),
            McpwarpErrorCode::DisconnectedFatal,
        ),
        (refused(404), McpwarpErrorCode::DisconnectedFatal),
        (
            DisconnectReason::new_for_test()
                .with_phase(Phase::Handshake)
                .with_ws_code(Some(4011))
                .with_error_name(Some("UNAUTHORIZED"))
                .with_fatal(true),
            McpwarpErrorCode::Unauthorized,
        ),
        (
            DisconnectReason::new_for_test()
                .with_ws_code(Some(4010))
                .with_fatal(true)
                .with_message("unsupported".into()),
            McpwarpErrorCode::DisconnectedFatal,
        ),
    ];
    for (reason, code) in cases {
        let edge = FakeEdge::new();
        let task = start(&edge, builder().build().unwrap()).await;
        let message = reason.message.clone();
        edge.disconnected(reason);
        let err = task.await.unwrap().unwrap_err();
        assert_eq!(err.code, code, "{message}");
        assert_eq!(err.message, message);
        assert!(err.fatal);
        let source = err.source.as_ref().unwrap();
        assert!(source.downcast_ref::<DisconnectReason>().is_some());
        assert_eq!(edge.closes(), [CloseCall::Graceful]);
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn non_fatal_dial_failures_keep_trying() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    for _ in 0..3 {
        edge.disconnected(refused(503).with_fatal(false));
        settle().await;
    }
    assert!(!task.is_finished());
    edge.connected(4);
    edge.app(4, FakeEdge::registered(NAME, "srv_1", URL, true));
    assert_eq!(
        task.await.unwrap().unwrap().state(),
        ExposeState::Registered
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fatal_disconnect_after_settle_closes_with_the_error() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.disconnected(
        DisconnectReason::new_for_test()
            .with_conn_id(Some(conn(1)))
            .with_fatal(true)
            .with_message("max reconnect attempts exhausted".into()),
    );
    settle().await;
    assert_eq!(handle.state(), ExposeState::Closed);
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["disconnect", "close"]);
    let ExposeEvent::Close { error: Some(err) } = &ev[1] else {
        panic!("{ev:?}");
    };
    assert_eq!(err.code, McpwarpErrorCode::DisconnectedFatal);
    // The connection was gone: nothing to unregister.
    assert_eq!(edge.sent_ops(), ["register"]);
    handle.close().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn close_unregisters_then_closes_and_is_idempotent() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    let closing = {
        let h = handle.clone();
        tokio::spawn(async move { h.close().await })
    };
    settle().await;
    assert_eq!(handle.state(), ExposeState::Closing);
    assert_eq!(edge.sent_ops(), ["register", "unregister"]);
    assert_eq!(
        edge.sent()[1].to_string(),
        r#"{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"notes"}]}}"#
    );
    assert!(edge.closes().is_empty());
    let second = {
        let h = handle.clone();
        tokio::spawn(async move { h.close().await })
    };
    edge.app(1, FakeEdge::unregistered(NAME, "srv_1"));
    closing.await.unwrap();
    second.await.unwrap();
    assert_eq!(handle.state(), ExposeState::Closed);
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    handle.close().await;
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    assert_eq!(edge.sent_ops(), ["register", "unregister"]);
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["disconnect", "close"]);
    assert!(matches!(&ev[1], ExposeEvent::Close { error: None }));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn close_waits_at_most_2s_for_unregistered() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    let t0 = Instant::now();
    handle.close().await;
    assert_eq!(t0.elapsed(), Duration::from_secs(2));
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn close_with_a_failed_unregister_send_closes_at_once() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    edge.fail_sends(true);
    let t0 = Instant::now();
    handle.close().await;
    assert_eq!(t0.elapsed(), Duration::ZERO);
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dropping_expose_before_it_settles_closes_the_client() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    edge.connected(1);
    settle().await;
    assert_eq!(edge.sent_ops(), ["register"]);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    settle().await;
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    // No unregister: abort is not close().
    assert_eq!(edge.sent_ops(), ["register"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn requests_while_an_aborted_expose_closes_get_503_closing() {
    let edge = FakeEdge::new();
    edge.hang_close(true);
    let task = start(&edge, builder().build().unwrap()).await;
    edge.connected(1);
    settle().await;
    task.abort();
    settle().await;
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    // A late registration does not reopen it.
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    let reply = exchange(edge.stream(), POST).await;
    assert!(
        reply.starts_with("HTTP/1.1 503 ") && reply.ends_with(r#"{"error":"service closing"}"#),
        "{reply}"
    );
    assert_eq!(edge.sent_ops(), ["register"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_state_is_current_when_disconnect_arrives() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    let seen = {
        let handle = handle.clone();
        tokio::spawn(async move {
            loop {
                if let ExposeEvent::Disconnect(_) = events.recv().await.unwrap() {
                    return handle.state();
                }
            }
        })
    };
    settle().await;
    edge.disconnected(lost(1, "lost"));
    assert_eq!(seen.await.unwrap(), ExposeState::Connecting);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn event_receivers_end_after_close() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.disconnected(
        DisconnectReason::new_for_test()
            .with_conn_id(Some(conn(1)))
            .with_fatal(true),
    );
    settle().await;
    assert_eq!(handle.state(), ExposeState::Closed);
    assert_eq!(tags(&drained(&mut events)), ["disconnect", "close"]);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    ));
    // A receiver made afterwards ends at once.
    let late = tokio::time::timeout(Duration::from_millis(1), handle.events().recv()).await;
    assert_eq!(
        late.expect("recv returned at once").unwrap_err(),
        tokio::sync::broadcast::error::RecvError::Closed
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hanging_client_close_is_bounded() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.hang_close(true);
    // No unregister wait: the send fails.
    edge.fail_sends(true);
    let t0 = Instant::now();
    handle.close().await;
    // The close bound (10 s), then the wait for the client's last events
    // (1 s), which never come.
    assert_eq!(t0.elapsed(), Duration::from_secs(11));
    assert_eq!(handle.state(), ExposeState::Closed);
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    assert_eq!(tags(&drained(&mut events)), ["close"]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dropping_the_last_handle_closes_in_the_background() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    let other = handle.clone();
    drop(handle);
    settle().await;
    assert_eq!(edge.sent_ops(), ["register"]);
    let mut state = other.watch_state();
    drop(other);
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "unregister"]);
    edge.app(1, FakeEdge::unregistered(NAME, "srv_1"));
    state.wait_for(|s| *s == ExposeState::Closed).await.unwrap();
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn disable_and_enable() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.app(1, FakeEdge::disable("srv_1", "maintenance"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Disabled);
    edge.app(1, FakeEdge::enable("srv_1", NAME));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registering);
    assert_eq!(edge.sent_ops(), ["register", "register"]);
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, false));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Registered);
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["disabled", "enabled", "registered"]);
    assert!(matches!(
        &ev[0],
        ExposeEvent::Disabled { server_id: Some(id), reason } if id == "srv_1" && reason == "maintenance"
    ));
    assert!(matches!(&ev[1], ExposeEvent::Enabled { server_id } if server_id == "srv_1"));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn non_fatal_errors_are_events() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(&edge, builder().build().unwrap()).await;
    edge.app(1, FakeEdge::error("TOO_MANY_SERVICES", "max 5"));
    edge.app(
        1,
        FakeEdge::envelope("disable", serde_json::json!({"id": 7})),
    );
    edge.app(1, FakeEdge::register_error(NAME, "FORBIDDEN", "no"));
    settle().await;
    let ev = drained(&mut events);
    let codes: Vec<_> = ev
        .iter()
        .map(|e| match e {
            ExposeEvent::Error(e) => {
                assert!(!e.fatal);
                e.code.as_str().to_owned()
            }
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        codes,
        ["TOO_MANY_SERVICES", "MALFORMED_ENVELOPE", "FORBIDDEN"]
    );
    assert_eq!(handle.state(), ExposeState::Registered);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn transport_ending_unexpectedly_is_fatal() {
    let edge = FakeEdge::new();
    let task = start(&edge, builder().build().unwrap()).await;
    // A second client on the same edge replaces the first one's channel.
    let config = builder().build().unwrap();
    let second = tokio::spawn(edge.expose(config));
    settle().await;
    let err = task.await.unwrap().unwrap_err();
    assert_eq!(err.code, McpwarpErrorCode::DisconnectedFatal);
    second.abort();
}

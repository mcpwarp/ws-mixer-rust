//! `on_retry` (MCPWARP.md §5.9): every scheduled reconnect and every
//! `register` retry, with the spec's reason strings, on the fake edge.
#![cfg(feature = "test-util")]

mod expose_support;

use std::time::Duration;

use expose_support::*;
use ws_mixer::RedialCause;
use ws_mixer_mcpwarp::test_util::{FakeEdge, Step, settle};
use ws_mixer_mcpwarp::{ExposeEvent, ExposeState};

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn redials_report_their_cause() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let task = start(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.run([
        // A rejected token refreshed: no Disconnected precedes it.
        Step::Redial(redial(None, RedialCause::TokenRefresh, 1, Duration::ZERO)),
        // A failed dial: the reason is the transport's text.
        Step::Disconnected(lost(1, "dial failed: connection refused")),
        Step::Redial(redial(
            Some(1),
            RedialCause::Failure,
            1,
            Duration::from_millis(700),
        )),
        // A zero-delay redial still fires.
        Step::Disconnected(lost(2, "going away")),
        Step::Redial(redial(Some(2), RedialCause::Failure, 2, Duration::ZERO)),
        Step::Connected(3),
        Step::App(3, FakeEdge::registered(NAME, "srv_1", URL, true)),
    ])
    .await;
    let handle = task.await.unwrap().unwrap();
    assert_eq!(
        retries.take(),
        [
            (1, Duration::ZERO, "token-refresh".into()),
            (
                1,
                Duration::from_millis(700),
                "dial failed: connection refused".into()
            ),
            (2, Duration::ZERO, "going away".into()),
        ]
    );

    // A drain's replacement: attempt 0, the actual delay, "drain".
    edge.drain(drain(3, ws_mixer::DrainReason::Overload));
    edge.redial(redial(
        Some(3),
        RedialCause::Drain,
        0,
        Duration::from_millis(1500),
    ));
    settle().await;
    assert_eq!(
        retries.take(),
        [(0, Duration::from_millis(1500), "drain".into())]
    );
    assert_eq!(handle.state(), ExposeState::Registered);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn overloaded_resend() {
    let edge = FakeEdge::new();
    let retries = Retries::default();
    let task = start(&edge, retries.hook(builder()).build().unwrap()).await;
    edge.connected(1);
    edge.app(1, FakeEdge::error("OVERLOADED", "busy"));
    settle().await;
    assert_eq!(
        retries.take(),
        [(
            1,
            Duration::from_millis(1000),
            "register failed: OVERLOADED: busy".into()
        )]
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "register"]);
    edge.app(1, FakeEdge::error("OVERLOADED", "still busy"));
    settle().await;
    assert_eq!(
        retries.take(),
        [(
            2,
            Duration::from_millis(2000),
            "register failed: OVERLOADED: still busy".into()
        )]
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;
    edge.app(1, FakeEdge::registered(NAME, "srv_1", URL, true));
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_panicking_on_retry_is_reported_not_fatal() {
    let edge = FakeEdge::new();
    let (handle, mut events) = registered(
        &edge,
        builder().on_retry(|_| panic!("boom")).build().unwrap(),
    )
    .await;
    edge.disconnected(lost(1, "lost"));
    edge.redial(redial(Some(1), RedialCause::Failure, 1, Duration::ZERO));
    settle().await;
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["disconnect", "handler_error"]);
    let ExposeEvent::HandlerError(e) = &ev[1] else {
        panic!()
    };
    assert_eq!(e.to_string(), "on_retry panicked: boom");
    assert_eq!(handle.state(), ExposeState::Connecting);
}

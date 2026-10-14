//! Streams under `expose()`: each one served by `serve_stream` with the
//! agent's live state (MCPWARP.md §4.4, §5.7), on the fake edge.
#![cfg(feature = "test-util")]

mod expose_support;

use expose_support::*;
use ws_mixer_mcpwarp::bridge::handler_fn;
use ws_mixer_mcpwarp::test_util::{FakeEdge, settle};
use ws_mixer_mcpwarp::{ExposeConfig, ExposeEvent, ExposeState};

fn status_and_body(reply: &str) -> (&str, &str) {
    let status = reply.split("\r\n").next().unwrap();
    let body = reply.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    (status, body)
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn registered_streams_reach_the_handler() {
    let edge = FakeEdge::new();
    let (_handle, _events) = registered(&edge, builder().build().unwrap()).await;
    // Several at once: served concurrently, none awaited inline.
    let peers: Vec<_> = (0..3).map(|_| edge.stream()).collect();
    settle().await;
    let mut replies = Vec::new();
    for peer in peers {
        replies.push(exchange(peer, POST).await);
    }
    for reply in replies {
        assert_eq!(
            status_and_body(&reply),
            ("HTTP/1.1 200 OK", "{}"),
            "{reply}"
        );
    }
    let other = edge.stream();
    let reply = exchange(other, "GET /other HTTP/1.1\r\nHost: h\r\n\r\n").await;
    assert!(reply.starts_with("HTTP/1.1 404 "), "{reply}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn disabled_streams_get_503_disabled() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    edge.app(1, FakeEdge::disable("srv_1", "maintenance"));
    settle().await;
    assert_eq!(handle.state(), ExposeState::Disabled);
    let reply = exchange(edge.stream(), POST).await;
    assert_eq!(
        status_and_body(&reply),
        (
            "HTTP/1.1 503 Service Unavailable",
            r#"{"error":"service disabled"}"#
        ),
        "{reply}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn closing_streams_get_503_closing() {
    let edge = FakeEdge::new();
    let (handle, _events) = registered(&edge, builder().build().unwrap()).await;
    let closing = {
        let h = handle.clone();
        tokio::spawn(async move { h.close().await })
    };
    settle().await;
    assert_eq!(handle.state(), ExposeState::Closing);
    let reply = exchange(edge.stream(), POST).await;
    assert_eq!(
        status_and_body(&reply),
        (
            "HTTP/1.1 503 Service Unavailable",
            r#"{"error":"service closing"}"#
        ),
        "{reply}"
    );
    edge.app(1, FakeEdge::unregistered(NAME, "srv_1"));
    closing.await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_body_cap_is_applied() {
    let edge = FakeEdge::new();
    let (_handle, _events) =
        registered(&edge, builder().max_request_body_bytes(1).build().unwrap()).await;
    let reply = exchange(edge.stream(), POST).await;
    assert!(reply.starts_with("HTTP/1.1 413 "), "{reply}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn handler_errors_are_500_and_an_event() {
    let edge = FakeEdge::new();
    let failing = handler_fn(|_req| async {
        Err::<http::Response<http_body_util::Full<bytes::Bytes>>, _>("database down".into())
    });
    let config = ExposeConfig::builder()
        .token("t")
        .name(NAME)
        .integration(ID)
        .handler(failing)
        .build()
        .unwrap();
    let (_handle, mut events) = registered(&edge, config).await;
    let reply = exchange(edge.stream(), POST).await;
    assert_eq!(
        status_and_body(&reply),
        (
            "HTTP/1.1 500 Internal Server Error",
            r#"{"error":"handler error"}"#
        ),
        "{reply}"
    );
    let ev = drained(&mut events);
    assert_eq!(tags(&ev), ["handler_error"]);
    let ExposeEvent::HandlerError(e) = &ev[0] else {
        panic!()
    };
    assert_eq!(e.to_string(), "database down");
}

#[cfg(feature = "target")]
#[tokio::test(flavor = "current_thread")]
async fn target_mode_forwards_to_a_local_server() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 4096];
        let mut seen = Vec::new();
        while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = sock.read(&mut buf).await.unwrap();
            seen.extend_from_slice(&buf[..n]);
        }
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nupsrv!")
            .await
            .unwrap();
        String::from_utf8(seen).unwrap()
    });
    let edge = FakeEdge::new();
    let config = ExposeConfig::builder()
        .token("t")
        .name(NAME)
        .integration(ID)
        .target(format!("http://{addr}/mcp"))
        .build()
        .unwrap();
    let (_handle, _events) = registered(&edge, config).await;
    let reply = exchange(edge.stream(), POST).await;
    assert_eq!(
        status_and_body(&reply),
        ("HTTP/1.1 200 OK", "upsrv!"),
        "{reply}"
    );
    let upstream = server.await.unwrap();
    assert!(upstream.starts_with("POST /mcp HTTP/1.1\r\n"), "{upstream}");
}

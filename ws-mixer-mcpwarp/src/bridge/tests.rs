use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::Response;
use http_body_util::{BodyExt, Full};
use tokio::sync::oneshot;
use ws_mixer::test_util::{PeerEvent, PeerHandle};

use super::*;

fn online() -> ServeContext {
    ServeContext::new(|| RegistrationState::Registered.into())
}

fn echo() -> impl Handler + Clone {
    handler_fn(|req: http::Request<RequestBody>| async move {
        let body = req.into_body().collect().await?.to_bytes();
        Ok(Response::new(Full::new(body)))
    })
}

/// Everything the stream sent, and how it ended.
async fn outcome(peer: &mut PeerHandle) -> (String, Option<PeerEvent>) {
    let mut out = Vec::new();
    loop {
        match peer.recv().await {
            Some(PeerEvent::Data(d)) => out.extend_from_slice(&d),
            other => return (String::from_utf8_lossy(&out).into_owned(), other),
        }
    }
}

async fn exchange(
    ctx: &ServeContext,
    handler: &(impl Handler + Clone),
    input: &[u8],
) -> (String, Option<PeerEvent>) {
    let (s, mut peer) = Stream::new_in_memory(1);
    let task = tokio::spawn(serve_stream(s, ctx, handler));
    peer.write(input).unwrap();
    peer.close();
    let res = outcome(&mut peer).await;
    task.await.unwrap();
    res
}

const POST: &str = "POST /mcp HTTP/1.1\r\nHost: h.example\r\nContent-Length: 2\r\n\r\n{}";

#[tokio::test]
async fn conflict_resets_before_reading() {
    let ctx = ServeContext::new(|| ServeState::new(RegistrationState::Registered).conflict(true));
    let (s, mut peer) = Stream::new_in_memory(1);
    serve_stream(s, &ctx, &echo()).await;
    assert_eq!(
        peer.recv().await,
        Some(PeerEvent::Reset {
            code: ErrorCode::CANCEL,
            message: "service taken over by another connection".into()
        })
    );
}

#[tokio::test(start_paused = true)]
async fn head_deadline_resets_without_a_response() {
    let (s, mut peer) = Stream::new_in_memory(1);
    let task = tokio::spawn(serve_stream(s, &online(), &echo()));
    peer.write(b"POST /mcp HTTP/1.1\r\nHost: h").unwrap();
    tokio::time::sleep(HEAD_DEADLINE - Duration::from_millis(1)).await;
    assert!(!task.is_finished());
    tokio::time::sleep(Duration::from_millis(2)).await;
    task.await.unwrap();
    assert_eq!(
        peer.recv().await,
        Some(PeerEvent::Reset {
            code: ErrorCode::CANCEL,
            message: "request head deadline exceeded".into()
        })
    );
}

#[tokio::test]
async fn handler_response_then_close() {
    let (out, end) = exchange(&online(), &echo(), POST.as_bytes()).await;
    assert_eq!(
        out,
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n"
    );
    assert_eq!(end, Some(PeerEvent::Close));
}

#[tokio::test]
async fn refusals_end_with_close() {
    let cases: [(&str, &str, ServeState); 6] = [
        (
            "GET /healthz HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc",
            "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 21\r\n\r\n{\"error\":\"not_found\"}",
            RegistrationState::Disabled.into(),
        ),
        (
            "HEAD /nope HTTP/1.1\r\nHost: h\r\n\r\n",
            "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 21\r\n\r\n",
            RegistrationState::Registered.into(),
        ),
        (
            POST,
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 31\r\n\r\n{\"error\":\"service unavailable\"}",
            RegistrationState::Unavailable.into(),
        ),
        (
            "GET /mcp HTTP/1.1\r\nHost: h\r\nbad name: x\r\n\r\n",
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 44\r\n\r\n{\"error\":\"invalid header name \\\"bad name\\\"\"}",
            RegistrationState::Registered.into(),
        ),
        (
            "GET /mcp HTTP/1.1\r\nHost: h\r\n",
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 30\r\n\r\n{\"error\":\"incomplete request\"}",
            RegistrationState::Registered.into(),
        ),
        (
            "POST /mcp HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\nzz\r\n",
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 32\r\n\r\n{\"error\":\"malformed chunk size\"}",
            RegistrationState::Registered.into(),
        ),
    ];
    for (input, want, state) in cases {
        let ctx = ServeContext::new(move || state);
        let (out, end) = exchange(&ctx, &echo(), input.as_bytes()).await;
        assert_eq!(out, want, "{input:?}");
        assert_eq!(end, Some(PeerEvent::Close), "{input:?}");
    }
}

#[tokio::test]
async fn refusal_is_written_before_the_body_and_drained() {
    let ctx = ServeContext::new(|| RegistrationState::Disabled.into());
    let (s, mut peer) = Stream::new_in_memory(1);
    let task = tokio::spawn(serve_stream(s, &ctx, &echo()));
    peer.write(b"POST /mcp HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n")
        .unwrap();
    let want = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 28\r\n\r\n{\"error\":\"service disabled\"}";
    let mut got = Vec::new();
    while got.len() < want.len() {
        match peer.recv().await {
            Some(PeerEvent::Data(d)) => got.extend_from_slice(&d),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(got, want.as_bytes());
    assert_eq!(peer.recv().await, Some(PeerEvent::Close));
    assert!(!task.is_finished(), "still draining");
    peer.write(b"zz-not-even-valid-chunking\r\n").unwrap();
    peer.close();
    task.await.unwrap();
    assert_eq!(peer.recv().await, None, "no RESET after the drain");
}

#[tokio::test]
async fn body_over_cap_is_413_without_the_handler() {
    let called = Arc::new(Mutex::new(false));
    let flag = called.clone();
    let handler = handler_fn(move |_req| {
        let flag = flag.clone();
        async move {
            *flag.lock().unwrap() = true;
            Ok(Response::new(Full::new(Bytes::new())))
        }
    });
    let ctx = online().max_request_body_bytes(4);
    let input = "POST /mcp HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n";
    let (out, end) = exchange(&ctx, &handler, input.as_bytes()).await;
    assert!(
        out.starts_with("HTTP/1.1 413 Content Too Large\r\n"),
        "{out}"
    );
    assert_eq!(end, Some(PeerEvent::Close));
    assert!(!*called.lock().unwrap());
}

#[tokio::test]
async fn handler_error_and_panic_are_500_and_reported() {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = errors.clone();
    let ctx = online().on_error(move |e| sink.lock().unwrap().push(e.to_string()));
    let failing = handler_fn(|_req| async { Err::<Response<Full<Bytes>>, _>("nope".into()) });
    fn kaboom() -> Result<Response<Full<Bytes>>, BoxError> {
        panic!("kaboom")
    }
    let panicking = handler_fn(|_req| async { kaboom() });
    let want = "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: 25\r\n\r\n{\"error\":\"handler error\"}";
    let (out, end) = exchange(&ctx, &failing, POST.as_bytes()).await;
    assert_eq!((out.as_str(), end), (want, Some(PeerEvent::Close)));
    let (out, end) = exchange(&ctx, &panicking, POST.as_bytes()).await;
    assert_eq!((out.as_str(), end), (want, Some(PeerEvent::Close)));
    assert_eq!(
        *errors.lock().unwrap(),
        ["nope", "handler panicked: kaboom"]
    );
}

#[tokio::test]
async fn unrepresentable_request_is_400() {
    let input = "POST /mcp HTTP/1.1\r\nHost: h\r\nx-ctl: a\x01b\r\nContent-Length: 0\r\n\r\n";
    let (out, end) = exchange(&online(), &echo(), input.as_bytes()).await;
    assert_eq!(
        out,
        "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 58\r\n\r\n{\"error\":\"request cannot be represented as a web Request\"}"
    );
    assert_eq!(end, Some(PeerEvent::Close));
}

#[tokio::test]
async fn expect_is_stripped_and_no_1xx_is_sent() {
    let seen = Arc::new(Mutex::new(None));
    let sink = seen.clone();
    let handler = handler_fn(move |req: http::Request<RequestBody>| {
        let sink = sink.clone();
        async move {
            *sink.lock().unwrap() = Some(req.headers().clone());
            Ok(Response::builder()
                .status(204)
                .body(Full::new(Bytes::new()))?)
        }
    });
    let input =
        "POST /mcp HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\n{}";
    let (out, _) = exchange(&online(), &handler, input.as_bytes()).await;
    assert_eq!(out, "HTTP/1.1 204 No Content\r\n\r\n");
    let headers = seen.lock().unwrap().take().unwrap();
    assert!(headers.get("expect").is_none() && headers.get("content-length").is_none());
}

#[tokio::test]
async fn edge_cancel_drops_the_handler_call() {
    struct Guard(Option<oneshot::Sender<()>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let dropped_tx = Arc::new(Mutex::new(Some(dropped_tx)));
    let handler = handler_fn(move |_req| {
        let guard = Guard(dropped_tx.lock().unwrap().take());
        async move {
            let _guard = guard;
            std::future::pending::<()>().await;
            Ok(Response::new(Full::new(Bytes::new())))
        }
    });
    let (s, mut peer) = Stream::new_in_memory(1);
    let task = tokio::spawn(serve_stream(s, &online(), &handler));
    peer.write(POST.as_bytes()).unwrap();
    peer.close();
    tokio::task::yield_now().await;
    peer.reset(ErrorCode::CANCEL, "client went away");
    tokio::time::timeout(Duration::from_secs(5), dropped_rx)
        .await
        .expect("handler dropped")
        .unwrap();
    task.await.unwrap();
    assert_eq!(peer.recv().await, None, "nothing written, no RESET back");
}

#[tokio::test]
async fn informational_or_invalid_status_is_a_handler_error() {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = errors.clone();
    let ctx = online().on_error(move |e| sink.lock().unwrap().push(e.to_string()));
    let want = "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: 25\r\n\r\n{\"error\":\"handler error\"}";
    for status in [100, 101, 103, 600, 999] {
        let handler = handler_fn(move |_req| async move {
            Ok(Response::builder()
                .status(http::StatusCode::from_u16(status)?)
                .body(Full::new(Bytes::from_static(b"x")))?)
        });
        let (out, end) = exchange(&ctx, &handler, POST.as_bytes()).await;
        assert_eq!(
            (out.as_str(), end),
            (want, Some(PeerEvent::Close)),
            "{status}"
        );
    }
    let errors = errors.lock().unwrap();
    assert_eq!(errors.len(), 5);
    assert_eq!(errors[0], "handler returned a 1xx status");
    assert_eq!(errors[3], "handler returned an invalid status");
}

#[tokio::test]
async fn raw_utf8_target_reaches_the_handler_encoded() {
    let seen = Arc::new(Mutex::new(None));
    let sink = seen.clone();
    let handler = handler_fn(move |req: http::Request<RequestBody>| {
        let sink = sink.clone();
        async move {
            *sink.lock().unwrap() = Some(req.uri().to_string());
            Ok(Response::new(Full::new(Bytes::new())))
        }
    });
    for (target, want) in [
        (
            "/mcp?q=caf\u{e9}&x={y}",
            "https://h.example/mcp?q=caf%C3%A9&x=%7By%7D",
        ),
        ("/mcp?x=100%", "https://h.example/mcp?x=100%"),
        ("/mcp?a=1#frag", "https://h.example/mcp?a=1"),
        ("/mcp?q=[1]", "https://h.example/mcp?q=[1]"),
    ] {
        let input = format!("GET {target} HTTP/1.1\r\nHost: h.example\r\n\r\n");
        let (out, _) = exchange(&online(), &handler, input.as_bytes()).await;
        assert!(out.starts_with("HTTP/1.1 200 OK\r\n"), "{target}: {out}");
        assert_eq!(
            seen.lock().unwrap().take().as_deref(),
            Some(want),
            "{target}"
        );
    }
}

//! `Identity::apply` on a real dial (MCPWARP.md §3.1): the upgrade headers
//! and `hello` a loopback WebSocket server receives, and no replacement dial
//! after a `drain` with reason `replaced` (§3.3).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use ws_mixer::{Client, DrainReason, Event, ReconnectPolicy};
use ws_mixer_mcpwarp::{Agent, Identity, VERSION};

/// The ws-mixer frame header before a DATA payload.
const FRAME_HEADER: usize = 8;

#[tokio::test]
async fn identity_reaches_the_upgrade_and_hello() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let headers = Arc::new(Mutex::new(None));
    let seen = headers.clone();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws =
            tokio_tungstenite::accept_hdr_async(tcp, move |req: &Request, mut resp: Response| {
                *seen.lock().unwrap() = Some(req.headers().clone());
                resp.headers_mut().insert(
                    "sec-websocket-protocol",
                    HeaderValue::from_static("ws-mixer.v1"),
                );
                Ok(resp)
            })
            .await
            .unwrap();
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Binary(b) => {
                    return serde_json::from_slice::<Value>(&b[FRAME_HEADER..]).unwrap();
                }
                _ => continue,
            }
        }
    });

    let id = Identity::new(
        "6F1C2A9E-3B4D-4E5F-8A7B-0C1D2E3F4A5B",
        &Agent {
            runtime: Some("tauri/2.1".into()),
            os: Some("windows/x86_64".into()),
        },
    )
    .unwrap();
    let builder = Client::builder(format!("ws://{addr}/"))
        .token("tok")
        .reconnect(ReconnectPolicy::disabled());
    let (client, _events) = id.apply(builder).connect().unwrap();
    let hello = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    client.close().await;

    assert_eq!(hello["t"], "hello");
    assert_eq!(
        hello["agent"],
        json!({
            "sdk": "ws-mixer-rust",
            "sdk_version": ws_mixer::SDK_VERSION,
            "runtime": "tauri/2.1",
            "os": "windows/x86_64",
        })
    );
    assert_eq!(
        serde_json::to_string(&hello["meta"]).unwrap(),
        format!(
            r#"{{"client":"sdk-rust","version":"{VERSION}","os":"windows/x86_64","integration":"6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"}}"#
        )
    );
    let h = headers.lock().unwrap().take().unwrap();
    assert_eq!(
        h.get_all("x-mcpwarp-integration")
            .iter()
            .collect::<Vec<_>>(),
        ["6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"]
    );
    assert_eq!(
        h.get_all("user-agent").iter().collect::<Vec<_>>(),
        [format!(
            "ws-mixer-rust/{} (tauri/2.1 windows/x86_64)",
            ws_mixer::SDK_VERSION
        )
        .as_str()]
    );
    assert_eq!(h["authorization"], "Bearer tok");
}

/// A stream-0 DATA frame carrying `v`.
fn control(v: Value) -> Message {
    let mut b = vec![0x01, 0, 0, 0, 0, 0, 0, 0];
    b.extend_from_slice(v.to_string().as_bytes());
    Message::Binary(b.into())
}

#[tokio::test]
async fn a_replaced_drain_never_dials_a_replacement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_hdr_async(tcp, |_: &Request, mut resp: Response| {
            resp.headers_mut().insert(
                "sec-websocket-protocol",
                HeaderValue::from_static("ws-mixer.v1"),
            );
            Ok(resp)
        })
        .await
        .unwrap();
        while !matches!(ws.next().await.unwrap().unwrap(), Message::Binary(_)) {}
        ws.send(control(json!({
            "t": "welcome", "v": 1, "session": "s1", "window": 262_144,
            "max_streams": 64, "ping_interval": 3_600_000, "ping_timeout": 10_800_000,
        })))
        .await
        .unwrap();
        // retry_after_ms 0: without the veto the replacement dial starts
        // before the application sees the drain.
        ws.send(control(json!({
            "t": "drain", "reason": "replaced", "last_stream_id": 0, "retry_after_ms": 0,
        })))
        .await
        .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await;
        (ws, second.is_ok())
    });

    let id = Identity::new("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b", &Agent::default()).unwrap();
    let builder = Client::builder(format!("ws://{addr}/")).token("tok");
    let (client, mut events) = id.apply(builder).connect().unwrap();
    let (_ws, redialed) = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    assert!(!redialed, "a replacement was dialed");
    let mut seen = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(100), events.recv()).await {
        seen.push(e);
    }
    assert!(
        matches!(seen.as_slice(), [Event::Connected(_), Event::Drain(d)] if d.reason == DrainReason::Replaced),
        "{seen:?}"
    );
    tokio::time::timeout(
        Duration::from_secs(10),
        client.close_immediate("TAKEN_OVER"),
    )
    .await
    .unwrap();
}

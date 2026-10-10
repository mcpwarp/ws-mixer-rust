//! `wss://` smoke tests against a real endpoint over the public internet.
//! Ignored by default; run with
//!
//! ```sh
//! cargo test --all-features --test wss_smoke -- --ignored --nocapture
//! ```
//!
//! With no ws-mixer edge available, they dial a public WebSocket echo server
//! (`WSMIXER_SMOKE_URL`, default `wss://echo.websocket.org`). That server
//! does not speak ws-mixer.v1, so the connection is expected to fail, but
//! only after TCP, TLS certificate verification and the HTTP upgrade
//! exchange have completed: the test asserts the failure is not a dial-level
//! TCP/TLS error.

#![cfg(any(
    feature = "rustls-platform-verifier",
    feature = "rustls-webpki-roots",
    feature = "native-tls"
))]

use std::time::Duration;

use ws_mixer::{Client, DisconnectReason, ErrorCode, Event, Phase, ReconnectPolicy, TlsConfig};

fn smoke_url() -> String {
    std::env::var("WSMIXER_SMOKE_URL").unwrap_or_else(|_| "wss://echo.websocket.org".into())
}

async fn dial_once(tls: TlsConfig) -> DisconnectReason {
    let url = smoke_url();
    let (client, mut events) = Client::builder(&url)
        .token("smoke-test")
        .tls(tls)
        .reconnect(ReconnectPolicy::disabled())
        .hello_timeout(Duration::from_secs(5))
        .connect()
        .expect("valid configuration");
    let reason = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = events.recv().await {
            if let Event::Disconnected(r) = event {
                return r;
            }
        }
        panic!("event channel ended without a Disconnected");
    })
    .await
    .expect("no Disconnected within 30s");
    client.close().await;
    println!(
        "{url}: phase={} fatal={} ws_code={:?} error_code={:?} http_status={:?}\n  message: {}\n  cause: {:?}",
        reason.phase,
        reason.fatal,
        reason.ws_code,
        reason.error_code,
        reason.http_status,
        reason.message,
        reason.cause,
    );
    reason
}

/// TLS and the HTTP upgrade completed when the server answered with an HTTP
/// status, a 101 without our subprotocol (`UNSUPPORTED`), or the failure
/// happened after the upgrade.
fn assert_past_tls(r: &DisconnectReason) {
    let past_tls = r.phase != Phase::Dial
        || r.http_status.is_some()
        || r.error_code == Some(ErrorCode::UNSUPPORTED);
    assert!(
        past_tls,
        "dial failed before the HTTP upgrade (TCP/TLS): {}",
        r.message
    );
}

#[cfg(feature = "rustls-platform-verifier")]
#[tokio::test]
#[ignore = "needs network access to a public wss:// endpoint"]
async fn platform_verifier_reaches_upgrade() {
    assert_past_tls(&dial_once(TlsConfig::PlatformVerifier).await);
}

#[cfg(feature = "rustls-webpki-roots")]
#[tokio::test]
#[ignore = "needs network access to a public wss:// endpoint"]
async fn webpki_roots_reach_upgrade() {
    assert_past_tls(&dial_once(TlsConfig::WebpkiRoots).await);
}

#[cfg(feature = "native-tls")]
#[tokio::test]
#[ignore = "needs network access to a public wss:// endpoint"]
async fn native_tls_reaches_upgrade() {
    assert_past_tls(&dial_once(TlsConfig::NativeTls).await);
}

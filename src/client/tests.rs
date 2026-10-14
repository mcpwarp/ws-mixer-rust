//! Supervisor tests: a [`Client`] whose dials are answered by the test over
//! in-memory WebSockets, with paused time.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role};

use super::*;
use crate::conn::dial::BoxIo;
use crate::conn::{ConnFailure, base_reason};
use crate::error::ErrorCode;
use crate::event::{ConnId, Connected, Event, Phase, RedialCause, RedialNotice};
use crate::frame::{self, Decoded, Frame};
use crate::token::TokenError;

const HOUR: u64 = 3_600_000;
const WAIT: Duration = Duration::from_secs(3600);
const S: Duration = Duration::from_secs(1);

/// One dial the client made, waiting for the test to answer it.
struct Dial {
    conn_id: ConnId,
    at: Instant,
    server: DuplexStream,
    reply: oneshot::Sender<Option<ConnFailure>>,
}

impl Dial {
    fn fail(self, f: ConnFailure) {
        let _ = self.reply.send(Some(f));
    }

    fn fail_http(self, status: u16) {
        let mut r = base_reason(Some(self.conn_id), Phase::Dial, format!("HTTP {status}"));
        r.http_status = Some(status);
        self.fail(r.into());
    }

    /// Completes the upgrade and reads the client's `hello`.
    async fn accept(self) -> Peer {
        let _ = self.reply.send(None);
        let ws = WebSocketStream::from_raw_socket(self.server, Role::Server, None).await;
        let mut peer = Peer {
            ws,
            conn_id: self.conn_id,
            hello: Value::Null,
        };
        let hello = peer.control().await;
        assert_eq!(hello["t"], "hello", "{hello}");
        peer.hello = hello;
        peer
    }

    /// Accepts and sends `welcome`.
    async fn welcome(self) -> Peer {
        let mut peer = self.accept().await;
        peer.welcome().await;
        peer
    }
}

#[derive(Debug)]
enum Got {
    Frame(Frame),
    Control(Value),
    Close(u16, String),
    End,
}

struct Peer {
    ws: WebSocketStream<DuplexStream>,
    conn_id: ConnId,
    hello: Value,
}

impl Peer {
    async fn send(&mut self, b: Bytes) {
        self.ws.send(Message::Binary(b)).await.unwrap();
    }

    async fn send_json(&mut self, v: Value) {
        self.send(frame::encode_data(0, v.to_string().as_bytes()))
            .await;
    }

    async fn welcome(&mut self) {
        let session = format!("s-{}", self.conn_id);
        self.send_json(json!({
            "t": "welcome", "v": 1, "session": session, "window": 262_144,
            "max_streams": 64, "ping_interval": HOUR, "ping_timeout": 3 * HOUR,
        }))
        .await;
    }

    async fn open(&mut self, id: u32) {
        self.send(Frame::Open { stream_id: id }.encode()).await;
    }

    async fn data(&mut self, id: u32, payload: &[u8]) {
        self.send(frame::encode_data(id, payload)).await;
    }

    async fn close_stream(&mut self, id: u32) {
        self.send(frame::encode_close(id)).await;
    }

    async fn drain(&mut self, retry_after_ms: Option<u64>) {
        self.drain_for("rollout", retry_after_ms).await;
    }

    async fn drain_for(&mut self, reason: &str, retry_after_ms: Option<u64>) {
        let mut d =
            json!({"t": "drain", "reason": reason, "last_stream_id": 1, "deadline_ms": 30_000});
        if let Some(ms) = retry_after_ms {
            d["retry_after_ms"] = json!(ms);
        }
        self.send_json(d).await;
    }

    async fn ws_close(&mut self, code: u16, reason: &str) {
        let _ = self
            .ws
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            })))
            .await;
    }

    /// `error{code}` then close `4000 + code`, as a server reports a
    /// connection error.
    async fn error_close(&mut self, code: ErrorCode, message: &str) {
        self.send_json(json!({"t": "error", "code": code.0, "message": message}))
            .await;
        self.ws_close(code.ws_close_wire(), message).await;
        self.finish().await;
    }

    async fn next(&mut self) -> Got {
        loop {
            let msg = tokio::time::timeout(WAIT, self.ws.next())
                .await
                .expect("timed out waiting for the client");
            return match msg {
                None | Some(Err(_)) => Got::End,
                Some(Ok(Message::Binary(b))) => match frame::decode(b).unwrap() {
                    Decoded::Frame(Frame::Data {
                        stream_id: 0,
                        payload,
                    }) => {
                        let v: Value = serde_json::from_slice(&payload).unwrap();
                        if v["t"] == "ping" || v["t"] == "pong" {
                            continue;
                        }
                        Got::Control(v)
                    }
                    Decoded::Frame(f) => Got::Frame(f),
                    Decoded::Unknown { .. } => panic!("client sent an unknown frame type"),
                },
                Some(Ok(Message::Close(f))) => match f {
                    Some(f) => Got::Close(f.code.into(), f.reason.to_string()),
                    None => Got::Close(1005, String::new()),
                },
                Some(Ok(_)) => continue,
            };
        }
    }

    async fn control(&mut self) -> Value {
        match self.next().await {
            Got::Control(v) => v,
            other => panic!("expected a control message, got {other:?}"),
        }
    }

    /// Reads to the end, which also flushes our side's close reply.
    async fn finish(&mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(Ok(_)) = self.ws.next().await {}
        })
        .await;
    }
}

/// The test's side of the client's dialer.
struct Server {
    dials: mpsc::UnboundedReceiver<Dial>,
}

impl Server {
    async fn dial(&mut self) -> Dial {
        tokio::time::timeout(WAIT, self.dials.recv())
            .await
            .expect("timed out waiting for a dial")
            .expect("dialer gone")
    }

    /// No dial within `window`.
    async fn no_dial(&mut self, window: Duration) {
        if let Ok(Some(d)) = tokio::time::timeout(window, self.dials.recv()).await {
            panic!("unexpected dial {}", d.conn_id);
        }
    }
}

fn upgrade() -> (crate::conn::Upgrade, Server) {
    let (tx, dials) = mpsc::unbounded_channel();
    let upgrade: crate::conn::Upgrade = Arc::new(move |_cfg, _token, conn_id| {
        let tx = tx.clone();
        Box::pin(async move {
            let (client, server) = tokio::io::duplex(1 << 20);
            let (reply, answer) = oneshot::channel();
            let _ = tx.send(Dial {
                conn_id,
                at: Instant::now(),
                server,
                reply,
            });
            match answer.await {
                Ok(None) => {
                    let io: BoxIo = Box::new(client);
                    Ok(WebSocketStream::from_raw_socket(
                        io,
                        Role::Client,
                        Some(ConnConfig::ws_config()),
                    )
                    .await)
                }
                Ok(Some(f)) => Err(f),
                Err(_) => Err(base_reason(
                    Some(conn_id),
                    Phase::Dial,
                    "dial dropped by the test".into(),
                )
                .into()),
            }
        })
    });
    (upgrade, Server { dials })
}

fn builder() -> ClientBuilder {
    Client::builder("ws://edge.test/v1/tunnel").token("tok")
}

fn start(b: ClientBuilder) -> (Client, Events, Server) {
    start_with_rng(b, 0.5)
}

fn start_with_rng(b: ClientBuilder, rng: f64) -> (Client, Events, Server) {
    let (up, server) = upgrade();
    let (client, events) = b.connect_via(up, Box::new(move || rng)).unwrap();
    (client, events, server)
}

/// A provider that hands out `t1`, `t2`, ... and counts its calls.
fn counting_provider() -> (Token, Arc<AtomicU32>) {
    let n = Arc::new(AtomicU32::new(0));
    let n2 = n.clone();
    let token = Token::provider(move || {
        let i = n2.fetch_add(1, Ordering::SeqCst) + 1;
        async move { Ok(format!("t{i}")) }
    });
    (token, n)
}

/// The next event, `Redial` included.
async fn next_any(events: &mut Events) -> Event {
    tokio::time::timeout(WAIT, events.recv())
        .await
        .expect("timed out waiting for an event")
        .expect("event channel closed")
}

/// The next event other than `Redial`; the `redial_*` tests check those.
async fn next(events: &mut Events) -> Event {
    loop {
        match next_any(events).await {
            Event::Redial(_) => {}
            e => return e,
        }
    }
}

async fn redial(events: &mut Events) -> RedialNotice {
    match next_any(events).await {
        Event::Redial(r) => r,
        other => panic!("expected Redial, got {other:?}"),
    }
}

async fn connected(events: &mut Events) -> Connected {
    match next(events).await {
        Event::Connected(c) => c,
        other => panic!("expected Connected, got {other:?}"),
    }
}

async fn disconnected(events: &mut Events) -> DisconnectReason {
    match next(events).await {
        Event::Disconnected(r) => r,
        other => panic!("expected Disconnected, got {other:?}"),
    }
}

async fn stream(events: &mut Events) -> crate::Stream {
    match next(events).await {
        Event::Stream(s) => s,
        other => panic!("expected Stream, got {other:?}"),
    }
}

/// The end of events, skipping `Redial`s queued before the close.
async fn ended(events: &mut Events) {
    loop {
        let e = tokio::time::timeout(WAIT, events.recv())
            .await
            .expect("timed out waiting for the end of events");
        match e {
            None => return,
            Some(Event::Redial(_)) => {}
            Some(e) => panic!("expected the end of events, got {e:?}"),
        }
    }
}

// --- token refresh ---------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn http_401_refreshes_the_token_and_redials_immediately() {
    let (token, calls) = counting_provider();
    let (_client, mut events, mut server) = start(builder().token(token));
    let d1 = server.dial().await;
    let t0 = d1.at;
    d1.fail_http(401);
    let d2 = server.dial().await;
    assert_eq!(d2.at, t0, "the refresh dial is immediate");
    let peer = d2.welcome().await;
    assert_eq!(peer.hello["token"], "t2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // The first rejection is not reported.
    let c = connected(&mut events).await;
    assert_eq!(c.conn_id, ConnId(2));
    assert!(!c.reconnect);
}

#[tokio::test(start_paused = true)]
async fn second_rejection_is_fatal_in_either_form() {
    for second_is_4011 in [false, true] {
        let (token, _) = counting_provider();
        let (client, mut events, mut server) = start(builder().token(token));
        server.dial().await.fail_http(401);
        let d2 = server.dial().await;
        let r = if second_is_4011 {
            let mut peer = d2.accept().await;
            peer.error_close(ErrorCode::UNAUTHORIZED, "expired 2").await;
            let r = disconnected(&mut events).await;
            assert_eq!(r.phase, Phase::Handshake);
            assert_eq!(r.ws_code, Some(4011));
            r
        } else {
            d2.fail_http(401);
            let r = disconnected(&mut events).await;
            assert_eq!(r.http_status, Some(401));
            r
        };
        assert!(r.fatal, "{r:?}");
        assert_eq!(r.retry_in, None);
        ended(&mut events).await;
        assert_eq!(client.state(), ClientState::Closed);
        assert!(client.wait_connected().await.unwrap_err().fatal);
        server.no_dial(Duration::from_secs(600)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn static_token_rejection_is_fatal_at_once() {
    let (_client, mut events, mut server) = start(builder());
    server.dial().await.fail_http(401);
    let r = disconnected(&mut events).await;
    assert!(r.fatal);
    ended(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn unauthorized_before_welcome_refreshes_after_welcome_is_fatal() {
    let (token, _) = counting_provider();
    let (_client, mut events, mut server) = start(builder().token(token));
    let mut p1 = server.dial().await.accept().await;
    p1.error_close(ErrorCode::UNAUTHORIZED, "token expired 1")
        .await;
    let mut p2 = server.dial().await.welcome().await;
    assert_eq!(p2.hello["token"], "t2");
    assert_eq!(connected(&mut events).await.conn_id, ConnId(2));
    p2.error_close(ErrorCode::UNAUTHORIZED, "revoked 2").await;
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.phase, r.ws_code, r.fatal),
        (Phase::Connected, Some(4011), true)
    );
    ended(&mut events).await;
}

// --- backoff -----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn application_close_after_welcome_starts_at_cap() {
    let (_client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.error_close(ErrorCode::APPLICATION_CLOSE, "CONNECTION_LIMIT: over cap")
        .await;
    let r = disconnected(&mut events).await;
    assert_eq!(r.ws_code, Some(4014));
    assert_eq!(r.error_name, Some("APPLICATION_CLOSE"));
    assert!(!r.fatal);
    // rng 0.5 * cap 60 s.
    assert_eq!(r.retry_in, Some(S * 30));
    let t = Instant::now();
    let d = server.dial().await;
    assert_eq!(d.at - t, S * 30);

    // Before welcome, 4014 is ordinary backoff (attempt 2: 0.5 * 4 s).
    let mut p = d.accept().await;
    p.error_close(ErrorCode::APPLICATION_CLOSE, "nope").await;
    let r = disconnected(&mut events).await;
    assert_eq!((r.phase, r.retry_in), (Phase::Handshake, Some(S * 2)));
}

#[tokio::test(start_paused = true)]
async fn welcome_then_close_does_not_reset_backoff() {
    let (_client, mut events, mut server) = start(builder());
    for want in [1, 2, 4, 8, 16] {
        let mut p = server.dial().await.welcome().await;
        connected(&mut events).await;
        p.ws_close(1011, "boom").await;
        p.finish().await;
        let r = disconnected(&mut events).await;
        assert_eq!(r.retry_in, Some(S * want), "{r:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn stability_resets_attempt() {
    let (_client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.ws_close(1011, "1").await;
    p.finish().await;
    assert_eq!(disconnected(&mut events).await.retry_in, Some(S));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.ws_close(1011, "2").await;
    p.finish().await;
    assert_eq!(disconnected(&mut events).await.retry_in, Some(S * 2));
    // This one stays up past stable_after (10 s).
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    tokio::time::sleep(Duration::from_secs(11)).await;
    p.ws_close(1011, "3").await;
    p.finish().await;
    assert_eq!(disconnected(&mut events).await.retry_in, Some(S));
}

#[tokio::test(start_paused = true)]
async fn keepalive_timeout_retries_immediately_once() {
    let (_client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.error_close(ErrorCode::KEEPALIVE_TIMEOUT, "no pong for 90000ms")
        .await;
    assert_eq!(
        disconnected(&mut events).await.retry_in,
        Some(Duration::ZERO)
    );
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.error_close(ErrorCode::KEEPALIVE_TIMEOUT, "no pong for 90000ms")
        .await;
    assert_eq!(disconnected(&mut events).await.retry_in, Some(S * 2));
}

#[tokio::test(start_paused = true)]
async fn max_attempts_exhausted_is_fatal() {
    let policy = ReconnectPolicy {
        max_attempts: std::num::NonZeroU32::new(2),
        ..ReconnectPolicy::default()
    };
    let (_client, mut events, mut server) = start(builder().reconnect(policy));
    for _ in 0..2 {
        drop(server.dial().await);
        assert!(!disconnected(&mut events).await.fatal);
    }
    drop(server.dial().await);
    let r = disconnected(&mut events).await;
    assert!(r.fatal);
    assert!(
        r.message.starts_with("2 consecutive reconnects"),
        "{}",
        r.message
    );
    ended(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn refresh_redial_happens_even_with_max_attempts_exhausted() {
    let (token, calls) = counting_provider();
    let policy = ReconnectPolicy {
        max_attempts: std::num::NonZeroU32::new(1),
        ..ReconnectPolicy::default()
    };
    let (_client, mut events, mut server) = start(builder().token(token).reconnect(policy));
    drop(server.dial().await);
    let r = disconnected(&mut events).await;
    assert_eq!((r.fatal, r.retry_in), (false, Some(S)));
    // attempt is now 1 of 1: any counted retry would be fatal.
    let d2 = server.dial().await;
    let t = d2.at;
    d2.fail_http(401);
    let d3 = server.dial().await;
    assert_eq!(d3.at, t, "the refresh dial is immediate");
    let p = d3.welcome().await;
    assert_eq!(p.hello["token"], "t3");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let c = connected(&mut events).await;
    assert_eq!((c.conn_id, c.reconnect), (ConnId(3), false));
}

#[tokio::test(start_paused = true)]
async fn going_away_without_drain_redials_with_short_jitter() {
    for code in [4012u16, 1001] {
        let (_client, mut events, mut server) = start(builder());
        // Two failed dials first: the jitter does not grow with attempt.
        for _ in 0..2 {
            drop(server.dial().await);
            disconnected(&mut events).await;
        }
        let mut p = server.dial().await.welcome().await;
        assert!(!connected(&mut events).await.reconnect);
        if code == 4012 {
            p.error_close(ErrorCode::GOING_AWAY, "draining: rollout")
                .await;
        } else {
            p.ws_close(1001, "going away").await;
            p.finish().await;
        }
        let r = disconnected(&mut events).await;
        assert_eq!(
            (r.ws_code, r.fatal, r.retry_in),
            (Some(u32::from(code)), false, Some(S)),
            "rng 0.5 * min(2 s, cap)"
        );
        let t = Instant::now();
        let d = server.dial().await;
        assert_eq!(d.at - t, S);
        let _p = d.welcome().await;
        let c = connected(&mut events).await;
        assert_eq!((c.conn_id, c.reconnect), (ConnId(4), true));
    }
}

#[tokio::test(start_paused = true)]
async fn max_bound_durations_survive_start_at_cap_and_welcome() {
    let day = Duration::from_secs(24 * 3600);
    let hour = Duration::from_secs(3600);
    let policy = ReconnectPolicy {
        base: day,
        cap: day,
        stable_after: day,
        connect_timeout: hour,
        ..ReconnectPolicy::default()
    };
    let (_client, mut events, mut server) = start(builder().reconnect(policy).hello_timeout(hour));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.error_close(ErrorCode::APPLICATION_CLOSE, "CONNECTION_LIMIT: over cap")
        .await;
    let r = disconnected(&mut events).await;
    assert_eq!(r.retry_in, Some(day / 2));
    let t = Instant::now();
    tokio::time::sleep(day / 2).await;
    let d = server.dial().await;
    assert_eq!(d.at - t, day / 2);
    let _p = d.welcome().await;
    assert!(connected(&mut events).await.reconnect);
}

#[tokio::test(start_paused = true)]
async fn retry_after_is_honoured() {
    let (_client, mut events, mut server) = start(builder());
    let d = server.dial().await;
    let mut r = base_reason(Some(d.conn_id), Phase::Dial, "HTTP 429".into());
    r.http_status = Some(429);
    d.fail(ConnFailure {
        reason: Box::new(r),
        retry_after: Some(S * 42),
        welcome_timeout: false,
    });
    assert_eq!(disconnected(&mut events).await.retry_in, Some(S * 42));
}

// --- token provider failures -----------------------------------------------

#[tokio::test(start_paused = true)]
async fn provider_unavailable_backs_off_then_succeeds() {
    let n = Arc::new(AtomicU32::new(0));
    let n2 = n.clone();
    let token = Token::provider(move || {
        let i = n2.fetch_add(1, Ordering::SeqCst);
        async move {
            if i == 0 {
                Err(TokenError::unavailable("refresh endpoint unreachable"))
            } else {
                Ok("fresh".to_owned())
            }
        }
    });
    let (_client, mut events, mut server) = start(builder().token(token));
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.phase, r.fatal, r.retry_in),
        (Phase::Dial, false, Some(S))
    );
    let cause = r.cause.as_ref().unwrap();
    assert!(matches!(
        cause.downcast_ref::<TokenError>(),
        Some(TokenError::Unavailable(_))
    ));
    assert!(
        r.message.contains("refresh endpoint unreachable"),
        "{}",
        r.message
    );
    let p = server.dial().await.welcome().await;
    assert_eq!(p.hello["token"], "fresh");
    connected(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn provider_timeout_is_unavailable() {
    let n = Arc::new(AtomicU32::new(0));
    let n2 = n.clone();
    let token = Token::provider(move || {
        let i = n2.fetch_add(1, Ordering::SeqCst);
        async move {
            if i == 0 {
                std::future::pending::<()>().await;
            }
            Ok("late".to_owned())
        }
    });
    let t0 = Instant::now();
    let (_client, mut events, mut server) = start(builder().token(token));
    let r = disconnected(&mut events).await;
    assert_eq!(t0.elapsed(), S * 10, "bounded by connect_timeout");
    assert!(!r.fatal);
    assert!(
        r.message.contains("did not answer within 10000ms"),
        "{}",
        r.message
    );
    server.dial().await.welcome().await;
    connected(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn provider_fatal_is_fatal_with_the_cause() {
    let token = Token::provider(|| async { Err(TokenError::fatal("signed out")) });
    let (_client, mut events, mut server) = start(builder().token(token));
    let r = disconnected(&mut events).await;
    assert!(r.fatal);
    let cause = r.cause.as_ref().unwrap();
    let e = cause.downcast_ref::<TokenError>().unwrap();
    assert_eq!(
        std::error::Error::source(e).unwrap().to_string(),
        "signed out"
    );
    ended(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn provider_panic_is_fatal() {
    let token = Token::provider(|| async { panic!("provider bug") });
    let (client, mut events, mut server) = start(builder().token(token));
    let r = disconnected(&mut events).await;
    assert!(r.fatal);
    assert!(matches!(
        r.cause.as_ref().unwrap().downcast_ref::<TokenError>(),
        Some(TokenError::Fatal(_))
    ));
    assert!(r.message.contains("provider bug"), "{}", r.message);
    ended(&mut events).await;
    assert_eq!(client.state(), ClientState::Closed);
    server.no_dial(Duration::from_secs(600)).await;
}

// --- drain -------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn drain_dials_in_parallel_and_retires_the_old_connection_after_its_streams() {
    let (client, mut events, mut server) = start(builder());
    let mut p1 = server.dial().await.welcome().await;
    connected(&mut events).await;
    p1.open(1).await;
    let mut s = stream(&mut events).await;
    p1.drain(None).await;
    match next(&mut events).await {
        Event::Drain(d) => assert_eq!(d.conn_id, ConnId(1)),
        other => panic!("expected Drain, got {other:?}"),
    }
    let t = Instant::now();
    let d2 = server.dial().await;
    assert_eq!(d2.at - t, S, "rng 0.5 * 2 s jitter");
    assert_eq!(client.state(), ClientState::Connected);
    let mut p2 = d2.welcome().await;
    let c = connected(&mut events).await;
    assert_eq!((c.conn_id, c.reconnect), (ConnId(2), true));

    // The in-flight stream finishes on the old connection.
    p1.data(1, b"request").await;
    p1.close_stream(1).await;
    let mut req = Vec::new();
    s.read_to_end(&mut req).await.unwrap();
    assert_eq!(req, b"request");
    s.write_all(b"response").await.unwrap();
    s.close_write().await.unwrap();
    match p1.next().await {
        Got::Frame(Frame::Data {
            stream_id: 1,
            payload,
        }) => assert_eq!(&payload[..], b"response"),
        other => panic!("expected DATA, got {other:?}"),
    }
    assert!(matches!(
        p1.next().await,
        Got::Frame(Frame::Close { stream_id: 1, .. })
    ));
    // Idle: closed by us with NO_ERROR / 1000 after the grace period.
    let e = p1.control().await;
    assert_eq!(
        (e["t"].as_str(), e["code"].as_u64()),
        (Some("error"), Some(0))
    );
    assert!(matches!(p1.next().await, Got::Close(1000, _)));
    p1.finish().await;
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.conn_id, r.ws_code, r.fatal),
        (Some(ConnId(1)), Some(1000), false)
    );

    // The new connection is the live one.
    client.send_app(json!({"on": 2})).await.unwrap();
    assert_eq!(p2.control().await, json!({"t": "app", "body": {"on": 2}}));
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn drain_honours_retry_after_and_ignores_a_repeat() {
    let (_client, mut events, mut server) = start(builder());
    let mut p1 = server.dial().await.welcome().await;
    connected(&mut events).await;
    p1.open(1).await;
    let _s = stream(&mut events).await;
    let t = Instant::now();
    p1.drain(Some(5000)).await;
    p1.drain(None).await;
    let d2 = server.dial().await;
    assert_eq!(d2.at - t, S * 5);
    // The server closes the old one with 4012 while the new one dials.
    p1.error_close(ErrorCode::GOING_AWAY, "draining: rollout")
        .await;
    let mut saw = Vec::new();
    for _ in 0..3 {
        saw.push(next(&mut events).await);
    }
    let r = saw
        .iter()
        .find_map(|e| match e {
            Event::Disconnected(r) => Some(r.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!((r.ws_code, r.fatal), (Some(4012), false));
    assert_eq!(
        r.retry_in,
        Some(Duration::ZERO),
        "the replacement dial is in flight"
    );
    let _p2 = d2.welcome().await;
    connected(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn stability_timer_belongs_to_the_new_connection_across_a_drain() {
    // The old connection is welcomed at t, drained at t+5 s, replaced at
    // t+6 s. Closing the new one 5 s after its welcome is past the old
    // connection's 10 s, not the new one's, so attempt is not reset. 11 s:
    // it is.
    for (up_for, want) in [(S * 5, S * 2), (S * 11, S)] {
        let (_client, mut events, mut server) = start(builder());
        drop(server.dial().await);
        assert_eq!(disconnected(&mut events).await.retry_in, Some(S));
        let mut p2 = server.dial().await.welcome().await;
        connected(&mut events).await;
        p2.open(1).await;
        let _s = stream(&mut events).await;
        tokio::time::sleep(S * 5).await;
        p2.drain(None).await;
        assert!(matches!(next(&mut events).await, Event::Drain(_)));
        let mut p3 = server.dial().await.welcome().await;
        assert_eq!(connected(&mut events).await.conn_id, ConnId(3));
        let welcomed = Instant::now();
        // The old connection ends first; that must not cancel conn 3's timer.
        p2.error_close(ErrorCode::GOING_AWAY, "draining: rollout")
            .await;
        let r = disconnected(&mut events).await;
        assert_eq!((r.conn_id, r.fatal), (Some(ConnId(2)), false));
        tokio::time::sleep_until(welcomed + up_for).await;
        p3.ws_close(1011, "x").await;
        p3.finish().await;
        let r = disconnected(&mut events).await;
        assert_eq!(r.conn_id, Some(ConnId(3)));
        assert_eq!(r.retry_in, Some(want), "closed {up_for:?} after welcome");
    }
}

#[tokio::test(start_paused = true)]
async fn fatal_disconnect_with_a_retiring_connection_live() {
    let (client, mut events, mut server) = start(builder());
    let mut p1 = server.dial().await.welcome().await;
    connected(&mut events).await;
    p1.open(1).await;
    let _s = stream(&mut events).await;
    p1.drain(None).await;
    assert!(matches!(next(&mut events).await, Event::Drain(_)));
    let mut p2 = server.dial().await.welcome().await;
    assert_eq!(connected(&mut events).await.conn_id, ConnId(2));
    p2.error_close(ErrorCode::UNSUPPORTED, "v2 only").await;
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.conn_id, r.ws_code, r.fatal),
        (Some(ConnId(2)), Some(4010), true)
    );
    assert_eq!(client.state(), ClientState::Closed);
    // The retiring connection is closed gracefully, then reports.
    loop {
        match p1.next().await {
            Got::Close(code, _) => {
                assert_eq!(code, 1000);
                break;
            }
            Got::End => panic!("connection 1 ended without a close"),
            _ => {}
        }
    }
    p1.finish().await;
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.conn_id, r.fatal, r.retry_in),
        (Some(ConnId(1)), false, None)
    );
    ended(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn drain_with_reconnect_disabled_runs_to_one_fatal_disconnect() {
    let (_client, mut events, mut server) = start(builder().reconnect(ReconnectPolicy::disabled()));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.drain(None).await;
    assert!(matches!(next(&mut events).await, Event::Drain(_)));
    server.no_dial(Duration::from_secs(60)).await;
    p.error_close(ErrorCode::GOING_AWAY, "draining: rollout")
        .await;
    let r = disconnected(&mut events).await;
    assert_eq!((r.ws_code, r.fatal), (Some(4012), true));
    ended(&mut events).await;
}

// --- close ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn close_with_stops_the_loop() {
    let (client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    let closing = {
        let client = client.clone();
        tokio::spawn(async move { client.close_with("APP-BYE: shutting down").await })
    };
    let e = p.control().await;
    assert_eq!(
        e,
        json!({"t": "error", "code": 14, "message": "APP-BYE: shutting down"})
    );
    assert!(matches!(p.next().await, Got::Close(4014, ref r) if r == "APP-BYE: shutting down"));
    p.finish().await;
    closing.await.unwrap();
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.ws_code, r.error_code, r.fatal),
        (Some(4014), Some(ErrorCode::APPLICATION_CLOSE), false)
    );
    assert_eq!(r.close_reason, None);
    ended(&mut events).await;
    assert_eq!(client.state(), ClientState::Closed);
    server.no_dial(Duration::from_secs(600)).await;
    assert_eq!(client.send_app(json!({})).await, Err(SendAppError::Closed));
    assert_eq!(client.drain(S).await, Err(CloseError::Closed));
    let w = client.wait_connected().await.unwrap_err();
    assert!(!w.fatal);
    // Idempotent.
    client.close().await;
}

#[tokio::test(start_paused = true)]
async fn close_is_graceful() {
    let (client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    let closing = tokio::spawn(async move { client.close().await });
    assert_eq!(p.control().await["t"], "drain");
    assert_eq!(p.control().await["code"], 0);
    assert!(matches!(p.next().await, Got::Close(1000, _)));
    p.finish().await;
    closing.await.unwrap();
    let r = disconnected(&mut events).await;
    assert_eq!((r.ws_code, r.fatal), (Some(1000), false));
    ended(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn close_during_backoff_cancels_the_redial() {
    let (client, mut events, mut server) = start(builder());
    drop(server.dial().await);
    let r = disconnected(&mut events).await;
    assert_eq!(r.retry_in, Some(S));
    assert_eq!(client.state(), ClientState::Backoff);
    assert_eq!(
        client.send_app(json!({})).await,
        Err(SendAppError::NotConnected)
    );
    assert_eq!(client.drain(S).await, Err(CloseError::NotConnected));
    client.close().await;
    ended(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn close_during_dial_cancels_it_without_an_event() {
    let (client, mut events, mut server) = start(builder());
    let d = server.dial().await;
    assert_eq!(client.state(), ClientState::Dialing);
    client.close().await;
    ended(&mut events).await;
    assert!(d.reply.is_closed(), "the dial was cancelled");
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn dropping_client_and_events_closes() {
    let (client, events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    tokio::task::yield_now().await;
    drop(client);
    drop(events);
    assert_eq!(p.control().await["t"], "drain");
    assert_eq!(p.control().await["code"], 0);
}

#[tokio::test(start_paused = true)]
async fn dropping_events_alone_closes_gracefully() {
    let (client, events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    client.wait_connected().await.unwrap();
    drop(events);
    let d = p.control().await;
    assert_eq!(
        (d["t"].as_str(), d["reason"].as_str()),
        (Some("drain"), Some("client_requested"))
    );
    let e = p.control().await;
    assert_eq!(
        (e["t"].as_str(), e["code"].as_u64()),
        (Some("error"), Some(0))
    );
    assert!(matches!(p.next().await, Got::Close(1000, _)));
    p.finish().await;
    client
        .watch_state()
        .wait_for(|s| *s == ClientState::Closed)
        .await
        .unwrap();
    assert_eq!(client.send_app(json!({})).await, Err(SendAppError::Closed));
    let r = client.wait_connected().await.unwrap_err();
    assert!(!r.fatal);
    assert_eq!(
        r.message,
        "the client was closed because the Events receiver was dropped"
    );
    // Returns once the supervisor is done.
    client.close().await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn close_immediate_sends_no_error_and_1000_without_draining() {
    let (client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let mut s = stream(&mut events).await;
    let t = Instant::now();
    let closing = {
        let client = client.clone();
        tokio::spawn(async move { client.close_immediate("TAKEN_OVER: replaced").await })
    };
    // No drain{client_requested} first, and no wait for the open stream.
    let e = p.control().await;
    assert_eq!(
        e,
        json!({"t": "error", "code": 0, "message": "TAKEN_OVER: replaced"})
    );
    assert!(matches!(p.next().await, Got::Close(1000, ref r) if r == "TAKEN_OVER: replaced"));
    assert_eq!(t.elapsed(), Duration::ZERO);
    p.finish().await;
    closing.await.unwrap();
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.conn_id, r.ws_code, r.error_code, r.fatal),
        (Some(ConnId(1)), Some(1000), None, false)
    );
    let mut buf = [0u8; 1];
    assert_eq!(
        s.read(&mut buf).await.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    ended(&mut events).await;
    assert_eq!(client.state(), ClientState::Closed);
    assert_eq!(client.send_app(json!({})).await, Err(SendAppError::Closed));
    assert!(!client.wait_connected().await.unwrap_err().fatal);
    server.no_dial(Duration::from_secs(600)).await;
    // Idempotent.
    client.close_immediate("again").await;
}

#[tokio::test(start_paused = true)]
async fn close_immediate_during_a_drain_abandons_the_replacement() {
    let (client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    p.drain(Some(5000)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    let r = redial(&mut events).await;
    assert_eq!((r.cause, r.delay), (RedialCause::Drain, S * 5));
    let closing = tokio::spawn(async move { client.close_immediate("TAKEN_OVER").await });
    assert_eq!(p.control().await["code"], 0);
    assert!(matches!(p.next().await, Got::Close(1000, _)));
    p.finish().await;
    closing.await.unwrap();
    let r = disconnected(&mut events).await;
    assert_eq!(
        (r.conn_id, r.ws_code, r.fatal),
        (Some(ConnId(1)), Some(1000), false)
    );
    ended(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

// --- Event::Redial -------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn redial_failure_follows_disconnected_with_attempt_and_delay() {
    let (_client, mut events, mut server) = start(builder());
    drop(server.dial().await);
    let r = match next_any(&mut events).await {
        Event::Disconnected(r) => r,
        other => panic!("expected Disconnected, got {other:?}"),
    };
    assert_eq!(r.retry_in, Some(S));
    let t = Instant::now();
    let n = redial(&mut events).await;
    assert_eq!(
        n,
        RedialNotice {
            conn_id: Some(ConnId(1)),
            attempt: 1,
            delay: S,
            cause: RedialCause::Failure,
        }
    );
    let d = server.dial().await;
    assert_eq!(d.at - t, S, "the delay reported is the one used");

    // A connection that ends after welcome: attempt 2, 0.5 * 4 s.
    let mut p = d.welcome().await;
    assert_eq!(connected(&mut events).await.conn_id, ConnId(2));
    p.ws_close(1011, "boom").await;
    p.finish().await;
    assert_eq!(disconnected(&mut events).await.retry_in, Some(S * 2));
    let n = redial(&mut events).await;
    assert_eq!(
        (n.conn_id, n.attempt, n.delay, n.cause),
        (Some(ConnId(2)), 2, S * 2, RedialCause::Failure)
    );

    // A zero-delay redial (the one immediate retry after 4013) is announced
    // too, before the dial it schedules reports anything.
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.error_close(ErrorCode::KEEPALIVE_TIMEOUT, "no pong").await;
    assert_eq!(
        disconnected(&mut events).await.retry_in,
        Some(Duration::ZERO)
    );
    let n = redial(&mut events).await;
    assert_eq!(
        (n.conn_id, n.attempt, n.delay, n.cause),
        (Some(ConnId(3)), 3, Duration::ZERO, RedialCause::Failure)
    );
    let _p = server.dial().await.welcome().await;
    assert_eq!(
        match next_any(&mut events).await {
            Event::Connected(c) => c.conn_id,
            other => panic!("expected Connected, got {other:?}"),
        },
        ConnId(4)
    );
}

#[tokio::test(start_paused = true)]
async fn no_redial_after_a_fatal_disconnect() {
    let (_client, mut events, mut server) = start(builder());
    server.dial().await.fail_http(403);
    match next_any(&mut events).await {
        Event::Disconnected(r) => assert!(r.fatal),
        other => panic!("expected Disconnected, got {other:?}"),
    }
    let e = tokio::time::timeout(WAIT, events.recv()).await.unwrap();
    assert!(e.is_none(), "expected the end of events, got {e:?}");
}

#[tokio::test(start_paused = true)]
async fn redial_drain_follows_the_drain_event_with_the_jitter_used() {
    let (_client, mut events, mut server) = start(builder());
    // A failure first: the drain replacement still reports attempt 0.
    drop(server.dial().await);
    disconnected(&mut events).await;
    assert_eq!(redial(&mut events).await.attempt, 1);
    let mut p = server.dial().await.welcome().await;
    assert_eq!(connected(&mut events).await.conn_id, ConnId(2));
    p.open(1).await;
    let _s = stream(&mut events).await;
    let t = Instant::now();
    p.drain(None).await;
    match next_any(&mut events).await {
        Event::Drain(d) => assert_eq!(d.conn_id, ConnId(2)),
        other => panic!("expected Drain, got {other:?}"),
    }
    let n = redial(&mut events).await;
    assert_eq!(
        n,
        RedialNotice {
            conn_id: Some(ConnId(2)),
            attempt: 0,
            delay: S,
            cause: RedialCause::Drain,
        },
        "rng 0.5 * 2 s jitter"
    );
    let d = server.dial().await;
    assert_eq!(d.at - t, n.delay);
    let _p3 = d.welcome().await;
    assert_eq!(connected(&mut events).await.conn_id, ConnId(3));

    // retry_after_ms is reported as given.
    let (_client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    p.drain(Some(7000)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    let n = redial(&mut events).await;
    assert_eq!(
        (n.attempt, n.delay, n.cause),
        (0, S * 7, RedialCause::Drain)
    );
}

#[tokio::test(start_paused = true)]
async fn redial_token_refresh_has_no_disconnected() {
    let (token, _) = counting_provider();
    let (_client, mut events, mut server) = start(builder().token(token));
    server.dial().await.fail_http(401);
    let n = redial(&mut events).await;
    assert_eq!(
        n,
        RedialNotice {
            conn_id: Some(ConnId(1)),
            attempt: 1,
            delay: Duration::ZERO,
            cause: RedialCause::TokenRefresh,
        }
    );
    let p = server.dial().await.welcome().await;
    assert_eq!(p.hello["token"], "t2");
    match next_any(&mut events).await {
        Event::Connected(c) => assert_eq!(c.conn_id, ConnId(2)),
        other => panic!("expected Connected, got {other:?}"),
    }

    // Also for a 4011 before welcome.
    let (token, _) = counting_provider();
    let (_client, mut events, mut server) = start(builder().token(token));
    let mut p1 = server.dial().await.accept().await;
    p1.error_close(ErrorCode::UNAUTHORIZED, "expired").await;
    let n = redial(&mut events).await;
    assert_eq!(
        (n.conn_id, n.attempt, n.cause),
        (Some(ConnId(1)), 1, RedialCause::TokenRefresh)
    );
}

#[tokio::test(start_paused = true)]
async fn no_redial_on_drain_vetoes_the_replacement() {
    let (client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    // retry_after_ms 0 would otherwise dial inside on_drain, before the
    // application sees the Drain.
    p.drain_for("replaced", Some(0)).await;
    match next_any(&mut events).await {
        Event::Drain(d) => assert_eq!(d.reason, DrainReason::Replaced),
        other => panic!("expected Drain, got {other:?}"),
    }
    server.no_dial(S * 5).await;
    assert_eq!(client.state(), ClientState::Connected);
    let closing = {
        let client = client.clone();
        tokio::spawn(async move { client.close_immediate("TAKEN_OVER").await })
    };
    assert_eq!(p.control().await["code"], 0);
    assert!(matches!(p.next().await, Got::Close(1000, _)));
    p.finish().await;
    closing.await.unwrap();
    match next_any(&mut events).await {
        Event::Disconnected(r) => assert_eq!((r.ws_code, r.fatal), (Some(1000), false)),
        other => panic!("expected Disconnected, got {other:?}"),
    }
    let e = tokio::time::timeout(WAIT, events.recv()).await.unwrap();
    assert!(e.is_none(), "no Redial, then the end; got {e:?}");
    server.no_dial(Duration::from_secs(600)).await;
}

/// The end of events with nothing, not even a `Redial`, before it.
async fn ended_strictly(events: &mut Events) {
    let e = tokio::time::timeout(WAIT, events.recv()).await.unwrap();
    assert!(e.is_none(), "expected the end of events, got {e:?}");
}

#[tokio::test(start_paused = true)]
async fn a_vetoed_connection_ending_on_its_own_is_final_without_a_redial() {
    // Its own early 4012 (empty table), with rng 0: GoingAway backoff would
    // redial at once.
    let (client, mut events, mut server) =
        start_with_rng(builder().no_redial_on_drain(DrainReason::Replaced), 0.0);
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.drain_for("replaced", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    p.error_close(ErrorCode::GOING_AWAY, "draining: replaced")
        .await;
    match next_any(&mut events).await {
        Event::Disconnected(r) => {
            assert_eq!((r.ws_code, r.fatal, r.retry_in), (Some(4012), false, None))
        }
        other => panic!("expected Disconnected, got {other:?}"),
    }
    ended_strictly(&mut events).await;
    assert_eq!(client.state(), ClientState::Closed);
    assert!(!client.wait_connected().await.unwrap_err().fatal);
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn a_vetoed_connection_ending_at_its_deadline_is_final_without_a_redial() {
    let (_client, mut events, mut server) =
        start_with_rng(builder().no_redial_on_drain(DrainReason::Replaced), 0.0);
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let mut s = stream(&mut events).await;
    p.drain_for("replaced", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    // The server's deadline (30 s) passes with the stream still open: it
    // resets it and closes 4012.
    tokio::time::sleep(S * 30).await;
    p.send(frame::encode_reset(1, ErrorCode::CANCEL, "drain deadline"))
        .await;
    p.error_close(ErrorCode::GOING_AWAY, "drain deadline").await;
    match next_any(&mut events).await {
        Event::Disconnected(r) => {
            assert_eq!((r.ws_code, r.fatal, r.retry_in), (Some(4012), false, None))
        }
        other => panic!("expected Disconnected, got {other:?}"),
    }
    ended_strictly(&mut events).await;
    let mut buf = [0u8; 1];
    assert!(s.read(&mut buf).await.is_err());
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn a_later_drain_on_a_vetoed_connection_dials_nothing() {
    let (client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    p.drain_for("replaced", Some(0)).await;
    p.drain_for("rollout", Some(0)).await;
    for _ in 0..2 {
        assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    }
    server.no_dial(S * 600).await;
    let closing = tokio::spawn(async move { client.close_immediate("TAKEN_OVER").await });
    assert_eq!(p.control().await["code"], 0);
    assert!(matches!(p.next().await, Got::Close(1000, _)));
    p.finish().await;
    closing.await.unwrap();
    assert!(matches!(
        next_any(&mut events).await,
        Event::Disconnected(r) if !r.fatal
    ));
    ended_strictly(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn a_vetoed_drain_on_a_retiring_connection_abandons_its_pending_replacement() {
    // Scheduled: the replacement's timer is cancelled.
    let (_client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    p.drain_for("rollout", Some(60_000)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    assert_eq!(redial(&mut events).await.delay, S * 60);
    p.drain_for("replaced", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    server.no_dial(S * 600).await;
    p.error_close(ErrorCode::GOING_AWAY, "drained").await;
    assert!(matches!(
        next_any(&mut events).await,
        Event::Disconnected(r) if !r.fatal && r.retry_in.is_none()
    ));
    ended_strictly(&mut events).await;

    // Dialing: the dial in flight is aborted before its upgrade completes.
    let (_client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    p.drain_for("rollout", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    assert_eq!(redial(&mut events).await.delay, Duration::ZERO);
    let mut d2 = server.dial().await;
    p.drain_for("replaced", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    tokio::time::timeout(WAIT, d2.reply.closed())
        .await
        .expect("the replacement dial was not aborted");
    server.no_dial(S * 600).await;
    p.error_close(ErrorCode::GOING_AWAY, "drained").await;
    assert!(matches!(
        next_any(&mut events).await,
        Event::Disconnected(r) if r.conn_id == Some(ConnId(1)) && !r.fatal
    ));
    ended_strictly(&mut events).await;
}

/// The next event, which must be a `Disconnected`.
async fn disconnected_raw(events: &mut Events) -> DisconnectReason {
    match next_any(events).await {
        Event::Disconnected(r) => r,
        other => panic!("expected Disconnected, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_vetoed_drain_and_the_close_in_one_read_still_veto() {
    // Regression guard for the biased select: the drain and the 4012 are
    // written back to back before the client runs. It exercises that case;
    // it does not prove both supervisor branches were ready in one poll.
    let (_client, mut events, mut server) =
        start_with_rng(builder().no_redial_on_drain(DrainReason::Replaced), 0.0);
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.drain_for("replaced", Some(0)).await;
    p.error_close(ErrorCode::GOING_AWAY, "draining: replaced")
        .await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    let r = disconnected_raw(&mut events).await;
    assert_eq!((r.fatal, r.retry_in), (false, None), "{r:?}");
    ended_strictly(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn a_replacement_waiting_for_welcome_abandoned_by_a_veto_is_closed_with_no_error() {
    let (_client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p1 = server.dial().await.welcome().await;
    connected(&mut events).await;
    p1.open(1).await;
    let _s = stream(&mut events).await;
    p1.drain_for("rollout", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    assert_eq!(redial(&mut events).await.delay, Duration::ZERO);
    // The replacement has sent hello; the server holds its welcome.
    let mut p2 = server.dial().await.accept().await;
    let t = Instant::now();
    p1.drain_for("replaced", Some(0)).await;
    let e = p2.control().await;
    assert_eq!(
        (e["t"].as_str(), e["code"].as_u64()),
        (Some("error"), Some(0)),
        "{e}"
    );
    assert!(matches!(p2.next().await, Got::Close(1000, _)));
    assert!(t.elapsed() < S, "closed promptly, not at client exit");
    p2.finish().await;
    assert!(matches!(
        next_any(&mut events).await,
        Event::Drain(d) if d.reason == DrainReason::Replaced
    ));
    p1.error_close(ErrorCode::GOING_AWAY, "drained").await;
    let r = disconnected_raw(&mut events).await;
    assert_eq!(
        (r.conn_id, r.fatal, r.retry_in),
        (Some(ConnId(1)), false, None),
        "no Connected or Disconnected for the abandoned replacement"
    );
    ended_strictly(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn a_welcomed_replacement_abandoned_by_a_veto_is_closed_at_once() {
    let (_client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p1 = server.dial().await.welcome().await;
    connected(&mut events).await;
    p1.open(1).await;
    let _s = stream(&mut events).await;
    p1.drain_for("rollout", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    assert_eq!(redial(&mut events).await.delay, Duration::ZERO);
    let mut p2 = server.dial().await.accept().await;
    // The replacement's welcome and the vetoed drain on the old connection
    // land before the supervisor runs again: the dial has finished its
    // handshake when the veto abandons it.
    p2.welcome().await;
    p1.drain_for("replaced", Some(0)).await;
    let t = Instant::now();
    let e = p2.control().await;
    assert_eq!(
        (e["t"].as_str(), e["code"].as_u64()),
        (Some("error"), Some(0))
    );
    assert!(matches!(p2.next().await, Got::Close(1000, _)));
    assert!(t.elapsed() < S, "closed promptly, not at client exit");
    // No Connected for it: only the vetoed drain.
    assert!(matches!(
        next_any(&mut events).await,
        Event::Drain(d) if d.reason == DrainReason::Replaced
    ));
    p1.error_close(ErrorCode::GOING_AWAY, "drained").await;
    let r = disconnected_raw(&mut events).await;
    assert_eq!(
        (r.conn_id, r.fatal, r.retry_in),
        (Some(ConnId(1)), false, None)
    );
    ended_strictly(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn a_vetoed_drain_lost_to_a_full_backlog_still_ends_without_a_redial() {
    // max_streams 64: the backlog limit is max(128, 64 + 64) = 128 events.
    let (_client, mut events, mut server) = start(
        builder()
            .max_streams(64)
            .no_redial_on_drain(DrainReason::Replaced),
    );
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    for i in 0..64 {
        p.open(2 * i + 1).await;
        p.send_json(json!({"t": "app", "body": {"i": i}})).await;
    }
    // Event 129: dropped, and the connection fails with ENHANCE_YOUR_CALM.
    p.drain_for("replaced", Some(0)).await;
    loop {
        match p.next().await {
            Got::Close(code, _) => {
                assert_eq!(code, 4009);
                break;
            }
            Got::End => panic!("closed without a close frame"),
            _ => {}
        }
    }
    p.finish().await;
    let mut kept = Vec::new();
    let r = loop {
        match next_any(&mut events).await {
            Event::Disconnected(r) => break r,
            e => kept.push(e),
        }
    };
    assert_eq!(kept.len(), 128);
    assert!(
        !kept
            .iter()
            .any(|e| matches!(e, Event::Drain(_) | Event::Redial(_))),
        "the drain was dropped"
    );
    assert_eq!(
        (r.ws_code, r.fatal, r.retry_in),
        (Some(4009), false, None),
        "{r:?}"
    );
    drop(kept);
    ended_strictly(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn a_vetoed_connection_ends_non_fatal_even_with_reconnect_disabled() {
    let (_client, mut events, mut server) = start(
        builder()
            .reconnect(ReconnectPolicy::disabled())
            .no_redial_on_drain(DrainReason::Replaced),
    );
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.drain_for("replaced", None).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    p.error_close(ErrorCode::GOING_AWAY, "draining: replaced")
        .await;
    let r = disconnected_raw(&mut events).await;
    assert_eq!((r.ws_code, r.fatal, r.retry_in), (Some(4012), false, None));
    ended_strictly(&mut events).await;
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn no_redial_on_drain_only_vetoes_its_reasons() {
    // Without the veto a replaced drain is an ordinary hand-over.
    let (_client, mut events, mut server) = start(builder());
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    let t = Instant::now();
    p.drain_for("replaced", Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    let n = redial(&mut events).await;
    assert_eq!((n.cause, n.delay), (RedialCause::Drain, Duration::ZERO));
    assert_eq!(server.dial().await.at, t);

    // A veto for another reason leaves rollout alone.
    let (_client, mut events, mut server) =
        start(builder().no_redial_on_drain(DrainReason::Replaced));
    let mut p = server.dial().await.welcome().await;
    connected(&mut events).await;
    p.open(1).await;
    let _s = stream(&mut events).await;
    p.drain(Some(0)).await;
    assert!(matches!(next_any(&mut events).await, Event::Drain(_)));
    assert_eq!(redial(&mut events).await.cause, RedialCause::Drain);
    server.dial().await;
}

// --- agent overrides -------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn agent_runtime_and_os_overrides_reach_hello() {
    let (_client, _events, mut server) = start(
        builder()
            .agent_runtime("tauri/2.1")
            .agent_os("windows/x86_64"),
    );
    let p = server.dial().await.accept().await;
    assert_eq!(
        p.hello["agent"],
        json!({
            "sdk": "ws-mixer-rust",
            "sdk_version": crate::SDK_VERSION,
            "runtime": "tauri/2.1",
            "os": "windows/x86_64",
        })
    );

    // Defaults when not overridden; overriding one keeps the other.
    let (_client, _events, mut server) = start(builder().agent_os("linux/arm"));
    let p = server.dial().await.accept().await;
    assert_eq!(p.hello["agent"]["runtime"], "rust");
    assert_eq!(p.hello["agent"]["os"], "linux/arm");
    let (_client, _events, mut server) = start(builder());
    let p = server.dial().await.accept().await;
    assert_eq!(
        p.hello["agent"]["os"],
        format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
    );
}

// --- events, state, meta, stats ---------------------------------------------

#[tokio::test(start_paused = true)]
async fn one_disconnected_per_connection_in_order() {
    let (client, mut events, mut server) = start(builder());
    let mut log = Vec::new();
    for round in 0..3u32 {
        let mut p = server.dial().await.welcome().await;
        p.open(1).await;
        p.send_json(json!({"t": "app", "body": {"round": round}}))
            .await;
        if round == 1 {
            p.drain(None).await;
            let p2 = server.dial().await.welcome().await;
            p.error_close(ErrorCode::GOING_AWAY, "drained").await;
            drop(p2);
        } else {
            p.ws_close(1011, "bye").await;
            p.finish().await;
        }
        // Drain what is there so far.
        while let Ok(Some(e)) = tokio::time::timeout(S * 120, events.recv()).await {
            let done = matches!(e, Event::Disconnected(_));
            log.push(e);
            if done && server.dials.is_empty() {
                break;
            }
        }
    }
    client.close().await;
    while let Some(e) = events.recv().await {
        log.push(e);
    }
    let mut per_conn: HashMap<ConnId, Vec<&'static str>> = HashMap::new();
    for e in &log {
        let (id, kind) = match e {
            Event::Connected(c) => (c.conn_id, "connected"),
            Event::Stream(s) => (s.conn_id(), "stream"),
            Event::App(a) => (a.conn_id, "app"),
            Event::Drain(d) => (d.conn_id, "drain"),
            Event::Disconnected(r) => (r.conn_id.unwrap(), "disconnected"),
            Event::Redial(_) => continue,
        };
        per_conn.entry(id).or_default().push(kind);
    }
    assert!(per_conn.len() >= 4, "{per_conn:?}");
    for (id, kinds) in &per_conn {
        assert_eq!(kinds.first(), Some(&"connected"), "{id}: {kinds:?}");
        assert_eq!(kinds.last(), Some(&"disconnected"), "{id}: {kinds:?}");
        assert_eq!(
            kinds.iter().filter(|k| **k == "disconnected").count(),
            1,
            "{id}: {kinds:?}"
        );
        assert_eq!(
            kinds.iter().filter(|k| **k == "connected").count(),
            1,
            "{id}: {kinds:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn set_hello_meta_applies_to_the_next_dial() {
    let (client, mut events, mut server) = start(builder().hello_meta(json!({"a": 1})));
    let mut p = server.dial().await.welcome().await;
    assert_eq!(p.hello["meta"], json!({"a": 1}));
    connected(&mut events).await;
    client.set_hello_meta(Some(json!({"b": 2}))).unwrap();
    assert!(matches!(
        client.set_hello_meta(Some(json!([1]))),
        Err(BuildError::MetaNotObject)
    ));
    assert!(matches!(
        client.set_hello_meta(Some(json!({"blob": "x".repeat(16_384)}))),
        Err(BuildError::HelloTooLarge { bytes }) if bytes > 16_384
    ));
    p.ws_close(1011, "x").await;
    p.finish().await;
    disconnected(&mut events).await;
    let mut p = server.dial().await.welcome().await;
    assert_eq!(p.hello["meta"], json!({"b": 2}));
    connected(&mut events).await;
    client.set_hello_meta(None).unwrap();
    p.ws_close(1011, "x").await;
    p.finish().await;
    disconnected(&mut events).await;
    let p = server.dial().await.accept().await;
    assert!(p.hello.get("meta").is_none(), "{}", p.hello);
}

#[tokio::test(start_paused = true)]
async fn oversized_hello_at_dial_time_is_fatal() {
    // Passes the build-time check, which counts a provider token as empty.
    let token = Token::provider(|| async { Ok("x".repeat(17 * 1024)) });
    let (client, mut events, mut server) = start(builder().token(token));
    let d = server.dial().await;
    let _ = d.reply.send(None);
    let mut peer = Peer {
        ws: WebSocketStream::from_raw_socket(d.server, Role::Server, None).await,
        conn_id: d.conn_id,
        hello: Value::Null,
    };
    let e = peer.control().await;
    assert_eq!(
        (e["t"].as_str(), e["code"].as_u64()),
        (Some("error"), Some(2))
    );
    assert!(matches!(peer.next().await, Got::Close(4002, _)));
    peer.finish().await;
    let r = disconnected(&mut events).await;
    assert!(r.fatal, "{r:?}");
    assert_eq!(r.phase, Phase::Handshake);
    assert_eq!(r.error_code, Some(ErrorCode::INTERNAL_ERROR));
    assert_eq!(r.retry_in, None);
    ended(&mut events).await;
    assert!(client.wait_connected().await.unwrap_err().fatal);
    server.no_dial(Duration::from_secs(600)).await;
}

#[tokio::test(start_paused = true)]
async fn watch_state_and_wait_connected() {
    let (client, mut events, mut server) = start(builder());
    let mut watch = client.watch_state();
    let d = server.dial().await;
    assert_eq!(*watch.borrow_and_update(), ClientState::Dialing);
    let waiter = {
        let client = client.clone();
        tokio::spawn(async move { client.wait_connected().await })
    };
    let mut p = d.welcome().await;
    connected(&mut events).await;
    assert_eq!(waiter.await.unwrap().unwrap().session, "s-1");
    assert_eq!(client.wait_connected().await.unwrap().session, "s-1");
    watch.changed().await.unwrap();
    assert_eq!(*watch.borrow_and_update(), ClientState::Connected);
    p.ws_close(1011, "x").await;
    p.finish().await;
    disconnected(&mut events).await;
    assert_eq!(*watch.borrow_and_update(), ClientState::Backoff);
    let d = server.dial().await;
    assert_eq!(*watch.borrow_and_update(), ClientState::Dialing);
    let _p = d.welcome().await;
    connected(&mut events).await;
    assert_eq!(*watch.borrow_and_update(), ClientState::Connected);
    client.close().await;
    assert_eq!(*watch.borrow_and_update(), ClientState::Closed);
}

#[tokio::test(start_paused = true)]
async fn stats_are_cumulative_across_reconnects() {
    let (client, mut events, mut server) = start(builder());
    for _ in 0..2 {
        let mut p = server.dial().await.welcome().await;
        connected(&mut events).await;
        p.open(1).await;
        let _s = stream(&mut events).await;
        p.ws_close(1011, "x").await;
        p.finish().await;
        disconnected(&mut events).await;
    }
    let s = client.stats();
    assert_eq!((s.dials, s.connects, s.streams_accepted), (2, 2, 2));
    assert!(s.bytes_in > 0 && s.bytes_out > 0);
}

#[tokio::test(start_paused = true)]
async fn connect_validates_the_configuration() {
    let (up, _server) = upgrade();
    let err = |b: ClientBuilder| b.connect_via(up.clone(), Box::new(|| 0.5)).unwrap_err();
    assert!(matches!(
        err(Client::builder("ws://h/").token("t").window(1)),
        BuildError::WindowOutOfRange(1)
    ));
    assert!(matches!(
        err(Client::builder("ws://h/")),
        BuildError::MissingToken
    ));
    assert!(matches!(
        err(Client::builder("http://h/").token("t")),
        BuildError::InvalidUrl { .. }
    ));
    assert!(matches!(
        err(Client::builder("not a url").token("t")),
        BuildError::InvalidUrl { .. }
    ));
    assert!(matches!(
        err(builder().max_streams(0)),
        BuildError::MaxStreamsOutOfRange(0)
    ));
    assert!(matches!(
        err(builder().hello_meta(json!(1))),
        BuildError::MetaNotObject
    ));
    assert!(matches!(
        err(builder().capabilities(["Bad".to_owned()])),
        BuildError::InvalidCapability(_)
    ));
    assert!(matches!(
        err(builder().agent("", "1")),
        BuildError::InvalidAgent(_)
    ));
    assert!(matches!(
        err(builder().agent_runtime("")),
        BuildError::InvalidAgent(_)
    ));
    assert!(matches!(
        err(builder().agent_os("x".repeat(129))),
        BuildError::InvalidAgent(_)
    ));
    assert!(matches!(
        err(builder().header(http::header::AUTHORIZATION, HeaderValue::from_static("x"))),
        BuildError::ForbiddenHeader(_)
    ));
    assert!(matches!(
        err(builder().hello_timeout(Duration::ZERO)),
        BuildError::InvalidDuration { .. }
    ));
    let p = ReconnectPolicy {
        cap: Duration::from_millis(1),
        ..ReconnectPolicy::default()
    };
    assert!(matches!(
        err(builder().reconnect(p)),
        BuildError::InvalidDuration { .. }
    ));
    let p = ReconnectPolicy {
        cap: Duration::MAX,
        ..ReconnectPolicy::default()
    };
    assert!(matches!(
        err(builder().reconnect(p)),
        BuildError::InvalidDuration {
            name: "reconnect.cap",
            ..
        }
    ));
    let day = Duration::from_secs(24 * 3600);
    let over = Duration::from_secs(24 * 3600 + 1);
    for (p, name) in [
        (
            ReconnectPolicy {
                base: over,
                cap: over,
                ..ReconnectPolicy::default()
            },
            "reconnect.base",
        ),
        (
            ReconnectPolicy {
                stable_after: over,
                ..ReconnectPolicy::default()
            },
            "reconnect.stable_after",
        ),
        (
            ReconnectPolicy {
                connect_timeout: Duration::from_secs(3601),
                ..ReconnectPolicy::default()
            },
            "reconnect.connect_timeout",
        ),
    ] {
        assert!(
            matches!(err(builder().reconnect(p)), BuildError::InvalidDuration { name: n, .. } if n == name),
            "{name}"
        );
    }
    assert!(matches!(
        err(builder().hello_timeout(Duration::from_secs(3601))),
        BuildError::InvalidDuration {
            name: "hello_timeout",
            ..
        }
    ));
    let at_max = ReconnectPolicy {
        base: day,
        cap: day,
        stable_after: day,
        connect_timeout: Duration::from_secs(3600),
        ..ReconnectPolicy::default()
    };
    assert!(
        builder()
            .reconnect(at_max)
            .hello_timeout(Duration::from_secs(3600))
            .connect_via(up.clone(), Box::new(|| 0.5))
            .is_ok()
    );
    assert!(matches!(
        err(builder().hello_meta(json!({"blob": "x".repeat(16_384)}))),
        BuildError::HelloTooLarge { bytes } if bytes > 16_384
    ));
    assert!(
        builder()
            .hello_meta(json!({"blob": "x".repeat(15_000)}))
            .connect_via(up.clone(), Box::new(|| 0.5))
            .is_ok()
    );
}

/// The safety net for a build with rustls but no built-in root store.
#[cfg(not(any(
    feature = "rustls-platform-verifier",
    feature = "rustls-webpki-roots",
    feature = "native-tls"
)))]
#[tokio::test]
async fn wss_without_a_tls_config_is_rejected() {
    assert!(matches!(
        Client::builder("wss://h/").token("t").connect(),
        Err(BuildError::NoTlsConfig)
    ));
}

#[test]
fn connect_outside_a_runtime_fails() {
    assert!(matches!(builder().connect(), Err(BuildError::NoRuntime)));
}

/// End to end over real TCP: `connect()` with the network dialer against a
/// tokio-tungstenite server that echoes the subprotocol.
#[tokio::test]
async fn connect_over_tcp() {
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (auth_tx, auth_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws =
            tokio_tungstenite::accept_hdr_async(tcp, move |req: &Request, mut resp: Response| {
                let _ = auth_tx.send(req.headers().get("authorization").cloned());
                resp.headers_mut().insert(
                    "sec-websocket-protocol",
                    HeaderValue::from_static("ws-mixer.v1"),
                );
                Ok(resp)
            })
            .await
            .unwrap();
        let hello = loop {
            if let Message::Binary(b) = ws.next().await.unwrap().unwrap() {
                let Decoded::Frame(Frame::Data { payload, .. }) = frame::decode(b).unwrap() else {
                    panic!("expected hello");
                };
                break serde_json::from_slice::<Value>(&payload).unwrap();
            }
        };
        let welcome = json!({
            "t": "welcome", "v": 1, "session": "tcp-1", "window": 262_144,
            "max_streams": 64, "ping_interval": HOUR, "ping_timeout": 3 * HOUR,
        });
        ws.send(Message::Binary(frame::encode_data(
            0,
            welcome.to_string().as_bytes(),
        )))
        .await
        .unwrap();
        // Read until the client's close completes.
        let mut last = None;
        while let Some(Ok(m)) = ws.next().await {
            last = Some(m);
        }
        (hello, last)
    });
    let (client, mut events) = Client::builder(format!("ws://{addr}/v1/tunnel"))
        .token("secret")
        .reconnect(ReconnectPolicy::disabled())
        .connect()
        .unwrap();
    let c = connected(&mut events).await;
    assert_eq!(c.session, "tcp-1");
    assert_eq!(
        auth_rx.await.unwrap(),
        Some(HeaderValue::from_static("Bearer secret"))
    );
    client.close_with("done").await;
    let r = disconnected(&mut events).await;
    assert_eq!((r.ws_code, r.fatal), (Some(4014), false));
    ended(&mut events).await;
    let (hello, last) = server.await.unwrap();
    assert_eq!(hello["token"], "secret");
    assert!(
        matches!(last, Some(Message::Close(Some(ref f))) if u16::from(f.code) == 4014),
        "{last:?}"
    );
    let s = client.stats();
    assert_eq!((s.dials, s.connects), (1, 1));
}

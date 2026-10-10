//! Connection tests against a scriptable fake peer: a server-role WebSocket
//! over `tokio::io::duplex`, with paused time.

use std::collections::{HashSet, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role};

use super::shared::{STREAM0_RATE, TokenBucket, WriteJob};
use super::*;
use crate::error::StreamError;
use crate::event::{Event, Events};
use crate::frame::{Decoded, Frame};
use crate::stream::{Stream, StreamShared};

const HOUR: u64 = 3_600_000;
const TIMEOUT: Duration = Duration::from_secs(120);

fn config() -> ConnConfig {
    ConnConfig {
        url: "ws://127.0.0.1:1/v1/tunnel".into(),
        headers: Vec::new(),
        tls: None,
        connect_timeout: Duration::from_secs(10),
        hello_timeout: Duration::from_secs(10),
        window: 16_384,
        max_streams: 64,
        agent: AgentInfo::default(),
        capabilities: Vec::new(),
        meta: None,
        allow_subfloor_timing: false,
        stats: Arc::new(Counters::default()),
        drains: None,
    }
}

fn welcome(window: u32) -> Value {
    json!({
        "t": "welcome", "v": 1, "session": "s-1", "window": window, "max_streams": 64,
        "ping_interval": HOUR, "ping_timeout": 3 * HOUR,
    })
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
    pending: VecDeque<Got>,
    pongs: HashSet<u64>,
    next_sync: u64,
}

impl Peer {
    async fn send(&mut self, b: Bytes) {
        self.ws.send(Message::Binary(b)).await.unwrap();
    }

    async fn send_json(&mut self, v: Value) {
        self.send(frame::encode_data(0, v.to_string().as_bytes()))
            .await;
    }

    async fn open(&mut self, id: u32) {
        self.send(Frame::Open { stream_id: id }.encode()).await;
    }

    async fn data(&mut self, id: u32, payload: &[u8]) {
        self.send(frame::encode_data(id, payload)).await;
    }

    async fn window(&mut self, id: u32, inc: u32) {
        self.send(frame::encode_window(id, inc)).await;
    }

    async fn close(&mut self, id: u32) {
        self.send(frame::encode_close(id)).await;
    }

    async fn reset(&mut self, id: u32, code: ErrorCode) {
        self.send(frame::encode_reset(id, code, "peer reset 1"))
            .await;
    }

    async fn ws_close(&mut self, code: u16, reason: &str) {
        self.ws
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            })))
            .await
            .unwrap();
    }

    /// The next message from the client, WebSocket ping/pong skipped.
    async fn recv_raw(&mut self) -> Got {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for the client");
            return match msg {
                None | Some(Err(_)) => Got::End,
                Some(Ok(Message::Binary(b))) => match frame::decode(b).unwrap() {
                    Decoded::Frame(Frame::Data {
                        stream_id: 0,
                        payload,
                    }) => Got::Control(serde_json::from_slice(&payload).unwrap()),
                    Decoded::Frame(f) => Got::Frame(f),
                    Decoded::Unknown { .. } => panic!("client sent an unknown frame type"),
                },
                Some(Ok(Message::Close(f))) => match f {
                    Some(f) => Got::Close(f.code.into(), f.reason.to_string()),
                    None => Got::Close(1005, String::new()),
                },
                Some(Ok(Message::Text(t))) => panic!("client sent text {t}"),
                Some(Ok(_)) => continue,
            };
        }
    }

    /// The next message, skipping the client's pings and recording its pongs.
    async fn next(&mut self) -> Got {
        if let Some(g) = self.pending.pop_front() {
            return g;
        }
        loop {
            match self.recv_raw().await {
                Got::Control(v) if v["t"] == "ping" => {}
                Got::Control(v) if v["t"] == "pong" => {
                    self.pongs.insert(v["id"].as_u64().unwrap());
                }
                g => return g,
            }
        }
    }

    async fn frame(&mut self) -> Frame {
        match self.next().await {
            Got::Frame(f) => f,
            other => panic!("expected a frame, got {other:?}"),
        }
    }

    async fn control(&mut self) -> Value {
        match self.next().await {
            Got::Control(v) => v,
            other => panic!("expected a control message, got {other:?}"),
        }
    }

    async fn ws_closed(&mut self) -> (u16, String) {
        match self.next().await {
            Got::Close(code, reason) => (code, reason),
            other => panic!("expected a close frame, got {other:?}"),
        }
    }

    /// Expects `error{code}` followed by close `wire`; returns the error message.
    async fn expect_error(&mut self, code: ErrorCode, wire: u16) -> String {
        let e = self.control().await;
        assert_eq!(e["t"], "error", "{e}");
        assert_eq!(e["code"], code.0, "{e}");
        let message = e["message"].as_str().unwrap().to_owned();
        assert!(message.bytes().any(|b| b.is_ascii_digit()), "{message}");
        let (got, _) = self.ws_closed().await;
        assert_eq!(got, wire);
        self.finish().await;
        message
    }

    /// Reads to the end, which also flushes our side's close reply.
    async fn finish(&mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(Ok(_)) = self.ws.next().await {}
        })
        .await;
    }

    /// Ping/pong round trip: everything the client's reader received before
    /// the ping has been dispatched. Frames seen meanwhile stay queued.
    async fn sync(&mut self) {
        self.next_sync += 1;
        let id = self.next_sync;
        self.send_json(json!({"t": "ping", "id": id, "ts": 7}))
            .await;
        while !self.pongs.contains(&id) {
            match self.recv_raw().await {
                Got::Control(v) if v["t"] == "ping" => {}
                Got::Control(v) if v["t"] == "pong" => {
                    assert_eq!(v["ts"], 7);
                    self.pongs.insert(v["id"].as_u64().unwrap());
                }
                g => self.pending.push_back(g),
            }
        }
    }

    /// Nothing but pings and pongs arrives while the client is idle.
    async fn expect_quiet(&mut self) {
        assert!(self.pending.is_empty(), "{:?}", self.pending);
        if let Ok(g) = tokio::time::timeout(Duration::from_secs(1), self.next()).await {
            panic!("expected silence, got {g:?}");
        }
    }
}

struct Harness {
    peer: Peer,
    conn: LiveConn,
    events: Events,
    stats: Arc<Counters>,
}

async fn pair() -> (WebSocketStream<DuplexStream>, Peer) {
    let (a, b) = tokio::io::duplex(1 << 20);
    let client =
        WebSocketStream::from_raw_socket(a, Role::Client, Some(ConnConfig::ws_config())).await;
    let server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
    let peer = Peer {
        ws: server,
        pending: VecDeque::new(),
        pongs: HashSet::new(),
        next_sync: 1_000_000,
    };
    (client, peer)
}

/// Runs the handshake up to the client's `hello`; the test answers it.
async fn start_handshake(
    cfg: ConnConfig,
) -> (
    Peer,
    tokio::task::JoinHandle<Result<Handshaken<DuplexStream>, ConnFailure>>,
    Events,
) {
    let (client, mut peer) = pair().await;
    let (tx, rx) = mpsc::unbounded_channel();
    let task =
        tokio::spawn(
            async move { Conn::establish(client, &cfg, "tok".into(), tx, ConnId(1)).await },
        );
    let hello = peer.control().await;
    assert_eq!(hello["t"], "hello");
    (peer, task, Events::new(rx))
}

async fn connect_with(cfg: ConnConfig, w: Value) -> Harness {
    let stats = cfg.stats.clone();
    let (mut peer, task, events) = start_handshake(cfg).await;
    peer.send_json(w).await;
    let hs = task.await.unwrap().expect("handshake");
    Harness {
        peer,
        conn: hs.start(),
        events,
        stats,
    }
}

async fn connect() -> Harness {
    connect_with(config(), welcome(262_144)).await
}

impl Harness {
    async fn stream(&mut self) -> Stream {
        match tokio::time::timeout(TIMEOUT, self.events.recv())
            .await
            .unwrap()
        {
            Some(Event::Stream(s)) => s,
            other => panic!("expected a stream, got {other:?}"),
        }
    }

    async fn open(&mut self, id: u32) -> Stream {
        self.peer.open(id).await;
        let s = self.stream().await;
        assert_eq!(s.id(), id);
        s
    }

    async fn finished(self) -> (DisconnectReason, Peer) {
        let mut peer = self.peer;
        let reason = tokio::time::timeout(TIMEOUT, async {
            let (r, _) = tokio::join!(self.conn.finished(), peer.finish());
            r
        })
        .await
        .unwrap();
        (reason, peer)
    }
}

fn stream_error(e: &io::Error) -> &StreamError {
    e.get_ref()
        .and_then(|e| e.downcast_ref::<StreamError>())
        .unwrap_or_else(|| panic!("not a StreamError: {e:?}"))
}

fn assert_lost(e: &io::Error) {
    assert_eq!(e.kind(), io::ErrorKind::ConnectionAborted, "{e}");
    assert!(
        matches!(stream_error(e), StreamError::ConnectionLost(_)),
        "{e}"
    );
}

fn assert_reset(e: &io::Error, code: ErrorCode) {
    assert_eq!(e.kind(), io::ErrorKind::ConnectionReset, "{e}");
    match stream_error(e) {
        StreamError::Reset { code: got, .. } => assert_eq!(*got, code),
        other => panic!("{other:?}"),
    }
}

async fn read_some(s: &mut Stream) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; 1 << 17];
    let n = tokio::time::timeout(TIMEOUT, s.read(&mut buf))
        .await
        .unwrap()?;
    buf.truncate(n);
    Ok(buf)
}

async fn read_to_end(s: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    tokio::time::timeout(TIMEOUT, s.read_to_end(&mut out))
        .await
        .unwrap()?;
    Ok(out)
}

fn assert_protocol_reason(r: &DisconnectReason, code: ErrorCode) {
    assert_eq!(r.phase, Phase::Connected);
    assert_eq!(r.ws_code, Some(code.ws_close_semantic()));
    assert_eq!(r.error_code, Some(code));
    assert_eq!(r.error_name, Some(code.name()));
    assert_eq!(r.close_reason, None);
    assert_eq!(r.conn_id, Some(ConnId(1)));
}

// --- §2.5 receive table ------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn never_opened_frames_are_connection_errors() {
    for kind in ["DATA", "WINDOW", "CLOSE", "RESET"] {
        let mut h = connect().await;
        let f = match kind {
            "DATA" => frame::encode_data(5, b"x"),
            "WINDOW" => frame::encode_window(5, 1),
            "CLOSE" => frame::encode_close(5),
            _ => frame::encode_reset(5, ErrorCode::CANCEL, ""),
        };
        h.peer.send(f).await;
        let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
        assert!(
            msg.contains(&format!("{kind} for stream 5, which was never opened")),
            "{msg}"
        );
        let stats = h.stats.clone();
        let (r, _) = h.finished().await;
        assert_protocol_reason(&r, ErrorCode::PROTOCOL_ERROR);
        assert_eq!(stats.snapshot().protocol_violations, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn open_must_increase() {
    let mut h = connect().await;
    let _s = h.open(3).await;
    h.peer.open(1).await;
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(
        msg.contains("OPEN for stream 1 is not above highest_opened 3"),
        "{msg}"
    );

    let mut h = connect().await;
    h.peer
        .send(Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 2]))
        .await;
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(msg.contains("even stream id 2"), "{msg}");
}

#[tokio::test(start_paused = true)]
async fn open_state() {
    let mut h = connect().await;
    let mut s = h.open(1).await;
    // DATA: buffered and readable.
    h.peer.data(1, b"hello").await;
    assert_eq!(read_some(&mut s).await.unwrap(), b"hello");

    // WINDOW: credits the sender. Our send window is welcome.window.
    let mut h = connect_with(config(), welcome(16_384)).await;
    let mut s = h.open(1).await;
    let writer = tokio::spawn(async move {
        s.write_all(&vec![7u8; 16_384 + 10]).await.unwrap();
        s
    });
    match h.peer.frame().await {
        Frame::Data {
            stream_id: 1,
            payload,
        } => assert_eq!(payload.len(), 16_384),
        f => panic!("{f:?}"),
    }
    h.peer.sync().await;
    h.peer.expect_quiet().await;
    h.peer.window(1, 10).await;
    match h.peer.frame().await {
        Frame::Data {
            stream_id: 1,
            payload,
        } => assert_eq!(payload.len(), 10),
        f => panic!("{f:?}"),
    }
    let _s = writer.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn close_keeps_buffer_reset_drops_it() {
    let mut h = connect().await;
    let mut a = h.open(1).await;
    let mut b = h.open(3).await;
    h.peer.data(1, b"abc").await;
    h.peer.data(1, b"def").await;
    h.peer.close(1).await;
    h.peer.data(3, b"lost").await;
    h.peer.reset(3, ErrorCode::CANCEL).await;
    h.peer.sync().await;
    assert_eq!(read_to_end(&mut a).await.unwrap(), b"abcdef");
    assert_eq!(read_some(&mut a).await.unwrap(), b"");
    assert_reset(&read_some(&mut b).await.unwrap_err(), ErrorCode::CANCEL);
    // A reset stream refuses writes with the same error.
    assert_reset(&b.write(b"x").await.unwrap_err(), ErrorCode::CANCEL);
    drop(b);
    h.peer.expect_quiet().await;
}

#[tokio::test(start_paused = true)]
async fn half_closed_local() {
    let mut h = connect().await;
    let mut s = h.open(1).await;
    s.close_write().await.unwrap();
    assert!(matches!(
        h.peer.frame().await,
        Frame::Close { stream_id: 1, .. }
    ));
    assert_eq!(
        s.write(b"x").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    // DATA is still buffered, WINDOW tolerated.
    h.peer.data(1, b"late").await;
    h.peer.window(1, 100).await;
    h.peer.sync().await;
    assert_eq!(read_some(&mut s).await.unwrap(), b"late");
    // CLOSE: fully closed, retired; later frames are stale.
    h.peer.close(1).await;
    assert_eq!(read_some(&mut s).await.unwrap(), b"");
    h.peer.data(1, b"stale").await;
    h.peer.sync().await;
    assert_eq!(h.stats.snapshot().stale_frames, 1);
    drop(s);
    h.peer.expect_quiet().await;

    // RESET in half-closed(local).
    let mut t = h.open(3).await;
    t.close_write().await.unwrap();
    assert!(matches!(
        h.peer.frame().await,
        Frame::Close { stream_id: 3, .. }
    ));
    h.peer.reset(3, ErrorCode::INTERNAL_ERROR).await;
    h.peer.sync().await;
    assert_reset(
        &read_some(&mut t).await.unwrap_err(),
        ErrorCode::INTERNAL_ERROR,
    );
    drop(t);
    h.peer.expect_quiet().await;
}

#[tokio::test(start_paused = true)]
async fn half_closed_remote() {
    let mut h = connect_with(config(), welcome(16_384)).await;
    // DATA after CLOSE: RESET(STREAM_CLOSED), stream only.
    let mut a = h.open(1).await;
    h.peer.close(1).await;
    h.peer.data(1, b"x").await;
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 1,
            code,
            message,
        } => {
            assert_eq!(code, ErrorCode::STREAM_CLOSED);
            assert!(
                message.contains("DATA of 1 bytes on stream 1 after its CLOSE"),
                "{message}"
            );
        }
        f => panic!("{f:?}"),
    }
    assert_reset(
        &read_some(&mut a).await.unwrap_err(),
        ErrorCode::STREAM_CLOSED,
    );

    // Duplicate CLOSE: RESET(STREAM_CLOSED).
    let _b = h.open(3).await;
    h.peer.close(3).await;
    h.peer.close(3).await;
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 3, code, ..
        } => assert_eq!(code, ErrorCode::STREAM_CLOSED),
        f => panic!("{f:?}"),
    }

    // WINDOW after CLOSE: credit applied (design decision; spec says ignore).
    let mut c = h.open(5).await;
    h.peer.close(5).await;
    assert_eq!(read_some(&mut c).await.unwrap(), b"");
    let writer = tokio::spawn(async move {
        c.write_all(&vec![1u8; 16_384 + 5]).await.unwrap();
        c.close_write().await.unwrap();
        c
    });
    assert!(
        matches!(h.peer.frame().await, Frame::Data { stream_id: 5, ref payload } if payload.len() == 16_384)
    );
    h.peer.window(5, 5).await;
    assert!(
        matches!(h.peer.frame().await, Frame::Data { stream_id: 5, ref payload } if payload.len() == 5)
    );
    assert!(matches!(
        h.peer.frame().await,
        Frame::Close { stream_id: 5, .. }
    ));
    drop(writer.await.unwrap());

    // RESET after CLOSE: closed, never answered.
    let mut d = h.open(7).await;
    h.peer.close(7).await;
    h.peer.reset(7, ErrorCode::CANCEL).await;
    h.peer.sync().await;
    assert_reset(&read_some(&mut d).await.unwrap_err(), ErrorCode::CANCEL);
    drop(d);
    h.peer.expect_quiet().await;
    assert!(!h.conn.is_terminal());
}

#[tokio::test(start_paused = true)]
async fn stale_frames_discarded_and_counted() {
    let mut h = connect().await;
    let s = h.open(1).await;
    h.peer.reset(1, ErrorCode::CANCEL).await;
    h.peer.sync().await;
    drop(s);
    h.peer.data(1, b"x").await;
    h.peer.window(1, 1).await;
    h.peer.close(1).await;
    h.peer.reset(1, ErrorCode::CANCEL).await;
    // WINDOW increment 0 on a dead stream: stale too, not a RESET.
    h.peer
        .send(Bytes::from_static(&[2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]))
        .await;
    h.peer.sync().await;
    h.peer.expect_quiet().await;
    assert_eq!(h.stats.snapshot().stale_frames, 5);
    // OPEN for a stale id is still a connection error.
    h.peer.open(1).await;
    h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
}

#[tokio::test(start_paused = true)]
async fn window_zero_increment_resets_only_that_stream() {
    let mut h = connect().await;
    let mut s = h.open(1).await;
    h.peer
        .send(Bytes::from_static(&[2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]))
        .await;
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 1, code, ..
        } => assert_eq!(code, ErrorCode::PROTOCOL_ERROR),
        f => panic!("{f:?}"),
    }
    assert_reset(
        &read_some(&mut s).await.unwrap_err(),
        ErrorCode::PROTOCOL_ERROR,
    );
    // Never opened: connection error first.
    h.peer
        .send(Bytes::from_static(&[2, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0]))
        .await;
    h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
}

// --- flow control ----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn window_returned_at_half_on_consumption() {
    let mut h = connect().await; // our window 16384
    let mut s = h.open(1).await;
    h.peer.data(1, &[0; 8191]).await;
    h.peer.sync().await;
    assert_eq!(read_some(&mut s).await.unwrap().len(), 8191);
    h.peer.expect_quiet().await;
    h.peer.data(1, &[0; 8193]).await;
    h.peer.sync().await;
    assert_eq!(read_some(&mut s).await.unwrap().len(), 8193);
    match h.peer.frame().await {
        Frame::Window {
            stream_id: 1,
            increment,
        } => assert_eq!(increment, 16_384),
        f => panic!("{f:?}"),
    }
    // Full window again; consumed after the peer's CLOSE: no WINDOW.
    h.peer.data(1, &[0; 16_384]).await;
    h.peer.close(1).await;
    h.peer.sync().await;
    assert_eq!(read_to_end(&mut s).await.unwrap().len(), 16_384);
    h.peer.expect_quiet().await;
}

#[tokio::test(start_paused = true)]
async fn data_beyond_credit_is_flow_control_error() {
    let mut h = connect().await;
    let _s = h.open(1).await;
    h.peer.data(1, &[0; 16_384]).await;
    h.peer.data(1, &[0; 1]).await;
    let msg = h
        .peer
        .expect_error(ErrorCode::FLOW_CONTROL_ERROR, 4003)
        .await;
    assert!(
        msg.contains("stream 1: received 1 DATA bytes with 0 credit remaining"),
        "{msg}"
    );
    let (r, _) = h.finished().await;
    assert_protocol_reason(&r, ErrorCode::FLOW_CONTROL_ERROR);
    assert!(r.is_sdk_bug());
}

#[tokio::test(start_paused = true)]
async fn window_past_max_is_flow_control_error() {
    let mut h = connect_with(config(), welcome(0x7fff_ffff)).await;
    let _s = h.open(1).await;
    h.peer.window(1, 1).await;
    let msg = h
        .peer
        .expect_error(ErrorCode::FLOW_CONTROL_ERROR, 4003)
        .await;
    assert!(msg.contains("2147483648"), "{msg}");
}

// --- writer scheduling -----------------------------------------------------------

fn decode_job(job: WriteJob) -> Frame {
    match job {
        WriteJob::Frame(b) => match frame::decode(b).unwrap() {
            Decoded::Frame(f) => f,
            d => panic!("{d:?}"),
        },
        _ => panic!("expected a frame"),
    }
}

#[test]
fn round_robin_with_control_and_pong_first() {
    let _rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .enter();
    let conn = Arc::new(ConnShared::new(
        ConnId(1),
        "s".into(),
        16_384,
        1 << 20,
        64,
        Arc::new(Counters::default()),
    ));
    let mut streams = Vec::new();
    for id in [1u32, 3, 5] {
        let st = Arc::new(StreamShared::new(id, 16_384, 1 << 20));
        conn.lock().streams.insert(id, st.clone());
        streams.push(Stream::new(st, conn.clone()));
    }
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let chunk = vec![9u8; 20_000];
    for s in &mut streams {
        for _ in 0..2 {
            assert_eq!(
                Pin::new(&mut *s)
                    .poll_write(&mut cx, &chunk)
                    .map(|r| r.unwrap()),
                Poll::Ready(16_384)
            );
        }
        // At most two chunks queued per stream.
        assert!(Pin::new(&mut *s).poll_write(&mut cx, &chunk).is_pending());
    }
    // CLOSE waits behind its own stream's DATA.
    assert!(
        Pin::new(&mut streams[0])
            .poll_shutdown(&mut cx)
            .is_pending()
    );
    conn.push_control(
        frame::encode_control(&crate::control::App {
            body: Default::default(),
        })
        .unwrap(),
    );
    conn.push_control(frame::encode_window(3, 1));
    conn.lock()
        .urgent
        .push_back(frame::encode_control(&crate::control::Pong { id: 4, ts: None }).unwrap());

    assert!(
        matches!(decode_job(conn.next_write()), Frame::Data { stream_id: 0, ref payload } if payload.starts_with(br#"{"t":"pong""#))
    );
    assert!(
        matches!(decode_job(conn.next_write()), Frame::Data { stream_id: 0, ref payload } if payload.starts_with(br#"{"t":"app""#))
    );
    assert!(matches!(
        decode_job(conn.next_write()),
        Frame::Window { stream_id: 3, .. }
    ));
    let order: Vec<u32> = (0..6)
        .map(|_| decode_job(conn.next_write()).stream_id())
        .collect();
    assert_eq!(order, [1, 3, 5, 1, 3, 5]);
    assert!(matches!(conn.next_write(), WriteJob::Idle));
    // Stream 1's DATA is out: now CLOSE is queued, after it.
    assert!(Pin::new(&mut streams[0]).poll_shutdown(&mut cx).is_ready());
    assert!(matches!(
        decode_job(conn.next_write()),
        Frame::Close { stream_id: 1, .. }
    ));
    // Credit is debited on enqueue.
    assert_eq!(
        super::shared::lock(&conn.lock().streams[&3].state).send_window,
        (1 << 20) - 2 * 16_384
    );
}

#[tokio::test(start_paused = true)]
async fn three_streams_round_robin_on_the_wire() {
    let mut h = connect().await;
    let mut writers = Vec::new();
    for id in [1, 3, 5] {
        let mut s = h.open(id).await;
        writers.push(tokio::spawn(async move {
            s.write_all(&vec![id as u8; 3 * 16_384]).await.unwrap();
            s.close_write().await.unwrap();
            s
        }));
    }
    let mut got = std::collections::HashMap::<u32, usize>::new();
    let mut closed = 0;
    while closed < 3 {
        match h.peer.frame().await {
            Frame::Data { stream_id, payload } => {
                assert!(payload.len() <= 16_384);
                assert!(payload.iter().all(|&b| b == stream_id as u8));
                *got.entry(stream_id).or_default() += payload.len();
            }
            Frame::Close { stream_id, .. } => {
                assert_eq!(got[&stream_id], 3 * 16_384, "CLOSE overtook DATA");
                closed += 1;
            }
            f => panic!("{f:?}"),
        }
    }
    for w in writers {
        drop(w.await.unwrap());
    }
}

// --- stream limits and drain ---------------------------------------------------

#[tokio::test(start_paused = true)]
async fn stream_limit_refusal_advances_highest_opened() {
    let mut cfg = config();
    cfg.max_streams = 1;
    let mut h = connect_with(cfg, welcome(262_144)).await;
    let _a = h.open(1).await;
    h.peer.open(3).await;
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 3,
            code,
            message,
        } => {
            assert_eq!(code, ErrorCode::STREAM_LIMIT);
            assert!(message.contains("max_streams=1"), "{message}");
        }
        f => panic!("{f:?}"),
    }
    // Refused, but opened as far as ids go: its frames are stale.
    h.peer.data(3, b"x").await;
    h.peer.sync().await;
    let snap = h.stats.snapshot();
    assert_eq!(
        (snap.refused_opens, snap.stale_frames, snap.streams_accepted),
        (1, 1, 1)
    );
    h.peer.open(3).await;
    h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
}

#[tokio::test(start_paused = true)]
async fn repeated_stream_limit_escalates() {
    let mut cfg = config();
    cfg.max_streams = 1;
    let mut h = connect_with(cfg, welcome(262_144)).await;
    let _a = h.open(1).await;
    for i in 0..20 {
        h.peer.open(3 + 2 * i).await;
    }
    for _ in 0..20 {
        assert!(matches!(
            h.peer.frame().await,
            Frame::Reset {
                code: ErrorCode::STREAM_LIMIT,
                ..
            }
        ));
    }
    let msg = h
        .peer
        .expect_error(ErrorCode::ENHANCE_YOUR_CALM, 4009)
        .await;
    assert!(msg.contains("the last for stream 41"), "{msg}");
}

#[tokio::test(start_paused = true)]
async fn open_beyond_drain_last_stream_id() {
    let mut h = connect().await;
    let _a = h.open(1).await;
    h.peer
        .send_json(json!({"t": "drain", "reason": "rollout", "last_stream_id": 3, "deadline_ms": 1500, "message": "bye"}))
        .await;
    match h.events.recv().await {
        Some(Event::Drain(d)) => {
            assert_eq!(d.reason, crate::event::DrainReason::Rollout);
            assert_eq!(d.last_stream_id, 3);
            assert_eq!(d.deadline, Some(Duration::from_millis(1500)));
            assert_eq!(d.retry_after, None);
            assert_eq!(d.message.as_deref(), Some("bye"));
        }
        other => panic!("{other:?}"),
    }
    let _b = h.open(3).await;
    h.peer.open(5).await;
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(
        msg.contains("OPEN for stream 5 exceeds drain last_stream_id 3"),
        "{msg}"
    );
}

#[tokio::test(start_paused = true)]
async fn local_drain_never_increases_and_refuses_later_opens() {
    let mut h = connect().await;
    let _a = h.open(3).await;
    h.conn.send_drain(Duration::from_millis(2500)).unwrap();
    assert_eq!(h.peer.control().await["last_stream_id"], 3);
    // An OPEN above the id we announced: refused, stream-scoped.
    h.peer.open(5).await;
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 5,
            code,
            message,
        } => {
            assert_eq!(code, ErrorCode::REFUSED_STREAM);
            assert!(
                message.contains("above our drain last_stream_id 3"),
                "{message}"
            );
        }
        f => panic!("{f:?}"),
    }
    // highest_opened is now 5, but a second drain still says 3.
    h.conn.send_drain(Duration::from_millis(1000)).unwrap();
    let d = h.peer.control().await;
    assert_eq!(
        (d["t"].as_str(), d["last_stream_id"].as_u64()),
        (Some("drain"), Some(3))
    );
    // Refused, but opened as far as ids go: its frames are stale.
    h.peer.data(5, b"x").await;
    h.peer.sync().await;
    let snap = h.stats.snapshot();
    assert_eq!(
        (snap.refused_opens, snap.stale_frames, snap.streams_accepted),
        (1, 1, 1)
    );
    // The graceful close's drain does not increase it either.
    h.conn.close_graceful();
    let d = h.peer.control().await;
    assert_eq!(
        (d["t"].as_str(), d["last_stream_id"].as_u64()),
        (Some("drain"), Some(3))
    );
    assert!(!h.conn.is_terminal());
}

#[tokio::test(start_paused = true)]
async fn unknown_drain_reason_counted() {
    let mut h = connect().await;
    h.peer
        .send_json(json!({"t": "drain", "reason": "v9_shed", "last_stream_id": 0}))
        .await;
    match h.events.recv().await {
        Some(Event::Drain(d)) => {
            assert_eq!(d.reason, crate::event::DrainReason::Maintenance);
            assert_eq!(d.raw_reason, "v9_shed");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(h.stats.snapshot().unknown_drain_reasons, 1);
}

// --- keepalive -----------------------------------------------------------------

fn fast_ping_welcome() -> Value {
    json!({
        "t": "welcome", "v": 1, "session": "s-1", "window": 262_144, "max_streams": 64,
        "ping_interval": 5000, "ping_timeout": 10_000,
    })
}

#[tokio::test(start_paused = true)]
async fn keepalive_timeout_closes_4013() {
    let t0 = Instant::now();
    let mut h = connect_with(config(), fast_ping_welcome()).await;
    let mut pings = 0;
    let e = loop {
        match h.peer.recv_raw().await {
            Got::Control(v) if v["t"] == "ping" => {
                assert_eq!(v["id"], pings);
                assert!(v["ts"].is_i64());
                pings += 1;
            }
            Got::Control(v) => break v,
            g => panic!("{g:?}"),
        }
    };
    assert!(pings >= 1, "the first ping comes within one interval");
    assert_eq!(e["t"], "error");
    assert_eq!(e["code"], 13);
    let elapsed = Instant::now().duration_since(t0);
    assert!(
        elapsed >= Duration::from_secs(10) && elapsed < Duration::from_secs(11),
        "{elapsed:?}"
    );
    assert_eq!(h.peer.ws_closed().await.0, 4013);
    let (r, _) = h.finished().await;
    assert_protocol_reason(&r, ErrorCode::KEEPALIVE_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn pongs_rearm_the_watchdog_and_duplicates_are_counted() {
    let h = connect_with(config(), fast_ping_welcome()).await;
    let Harness {
        mut peer,
        conn,
        stats,
        ..
    } = h;
    let t0 = Instant::now();
    let mut answered = 0;
    while Instant::now().duration_since(t0) < Duration::from_secs(60) {
        match peer.recv_raw().await {
            Got::Control(v) if v["t"] == "ping" => {
                let pong = json!({"t": "pong", "id": v["id"], "ts": v["ts"]});
                peer.send_json(pong.clone()).await;
                if answered == 0 {
                    peer.send_json(pong).await;
                }
                answered += 1;
            }
            g => panic!("{g:?}"),
        }
    }
    assert!(answered >= 11, "{answered}");
    assert!(!conn.is_terminal());
    assert_eq!(stats.snapshot().duplicate_pongs, 1);
}

#[tokio::test(start_paused = true)]
async fn pong_for_unsent_id_is_protocol_error() {
    let mut h = connect().await;
    h.peer.send_json(json!({"t": "pong", "id": 1000})).await;
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(msg.contains("pong for id 1000 was never sent"), "{msg}");
}

async fn send_apps(peer: &mut Peer, range: std::ops::Range<u32>) {
    for i in range {
        peer.send_json(json!({"t": "app", "body": {"i": i}})).await;
    }
}

async fn expect_apps(events: &mut Events, range: std::ops::Range<u32>) {
    for i in range {
        match events.recv().await {
            Some(Event::App(a)) => assert_eq!(a.body["i"], i),
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn stream_zero_burst_over_100_is_enhance_your_calm() {
    let mut h = connect().await;
    send_apps(&mut h.peer, 0..101).await;
    let msg = h
        .peer
        .expect_error(ErrorCode::ENHANCE_YOUR_CALM, 4009)
        .await;
    assert!(msg.contains("stream-0 message 101 "), "{msg}");
    assert!(msg.contains("50/s (burst 100)"), "{msg}");
    expect_apps(&mut h.events, 0..100).await;
    let stats = h.stats.clone();
    let (r, _) = h.finished().await;
    assert_protocol_reason(&r, ErrorCode::ENHANCE_YOUR_CALM);
    assert_eq!(stats.snapshot().protocol_violations, 1);
}

#[tokio::test(start_paused = true)]
async fn stream_zero_bucket_refills_at_50_per_second() {
    let mut h = connect().await;
    send_apps(&mut h.peer, 0..100).await;
    expect_apps(&mut h.events, 0..100).await;
    assert!(!h.conn.is_terminal());
    tokio::time::sleep(Duration::from_secs(1)).await;
    send_apps(&mut h.peer, 100..151).await;
    let msg = h
        .peer
        .expect_error(ErrorCode::ENHANCE_YOUR_CALM, 4009)
        .await;
    assert!(msg.contains("stream-0 message 151 "), "{msg}");
    expect_apps(&mut h.events, 100..150).await;
}

#[tokio::test(start_paused = true)]
async fn steady_40_per_second_is_never_limited() {
    let mut h = connect().await;
    for i in 0..1200 {
        send_apps(&mut h.peer, i..i + 1).await;
        expect_apps(&mut h.events, i..i + 1).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    h.peer.sync().await;
    h.peer.expect_quiet().await;
    assert!(!h.conn.is_terminal());
}

#[tokio::test(start_paused = true)]
async fn stalled_keepalive_sends_one_ping_not_a_burst() {
    let mut w = fast_ping_welcome();
    w["ping_timeout"] = json!(HOUR);
    let mut h = connect_with(config(), w).await;
    let first = h.peer.recv_raw().await;
    assert!(
        matches!(first, Got::Control(ref v) if v["t"] == "ping" && v["id"] == 0),
        "{first:?}"
    );
    // Five intervals pass at once.
    tokio::time::advance(Duration::from_secs(25)).await;
    let mut ids = Vec::new();
    while let Ok(g) = tokio::time::timeout(Duration::from_secs(1), h.peer.recv_raw()).await {
        match g {
            Got::Control(v) if v["t"] == "ping" => ids.push(v["id"].as_u64().unwrap()),
            g => panic!("{g:?}"),
        }
    }
    assert_eq!(ids, [1]);
}

// --- application events ----------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn event_backlog_overflow_is_enhance_your_calm() {
    let mut cfg = config();
    cfg.max_streams = 4; // limit max(128, 4 + 64) = 128
    let mut h = connect_with(cfg, welcome(262_144)).await;
    // Spread over 1s to stay inside the stream-0 rate limit.
    send_apps(&mut h.peer, 0..64).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    send_apps(&mut h.peer, 64..128).await;
    h.peer.sync().await;
    h.peer.expect_quiet().await;
    // Delivering one frees one slot.
    assert!(matches!(h.events.recv().await, Some(Event::App(a)) if a.body["i"] == 0));
    h.peer
        .send_json(json!({"t": "app", "body": {"i": 128}}))
        .await;
    h.peer
        .send_json(json!({"t": "app", "body": {"i": 129}}))
        .await;
    let msg = h
        .peer
        .expect_error(ErrorCode::ENHANCE_YOUR_CALM, 4009)
        .await;
    assert!(msg.contains("128"), "{msg}");
    assert!(msg.contains("at app message on stream 0"), "{msg}");
    // Everything already queued is still delivered.
    for i in 1..=128 {
        match h.events.recv().await {
            Some(Event::App(a)) => assert_eq!(a.body["i"], i),
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn app_messages_both_ways() {
    let mut h = connect().await;
    h.peer
        .send_json(json!({"t": "app", "body": {"op": "hi", "n": 1.5}}))
        .await;
    match h.events.recv().await {
        Some(Event::App(a)) => {
            assert_eq!(a.conn_id, ConnId(1));
            assert_eq!(Value::Object(a.body), json!({"op": "hi", "n": 1.5}));
        }
        other => panic!("{other:?}"),
    }
    h.conn.send_app(json!({"op": "register"})).unwrap();
    assert_eq!(
        h.peer.control().await,
        json!({"t": "app", "body": {"op": "register"}})
    );
    assert_eq!(h.conn.send_app(json!([1])), Err(SendAppError::NotAnObject));
    let big = json!({"x": "a".repeat(16_384)});
    assert!(
        matches!(h.conn.send_app(big), Err(SendAppError::TooLarge { bytes }) if bytes > 16_384)
    );
}

#[tokio::test(start_paused = true)]
async fn stream_zero_payload_over_16k_is_enhance_your_calm() {
    let mut h = connect().await;
    h.peer.data(0, &[b' '; 16_385]).await;
    let msg = h
        .peer
        .expect_error(ErrorCode::ENHANCE_YOUR_CALM, 4009)
        .await;
    assert!(msg.contains("16385"), "{msg}");
}

#[tokio::test(start_paused = true)]
async fn malformed_messages() {
    let mut h = connect().await;
    h.peer.ws.send(Message::Text("{}".into())).await.unwrap();
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(msg.contains("text message of 2 bytes"), "{msg}");

    let mut h = connect().await;
    h.peer.send(Bytes::from(vec![1u8; 70_000])).await;
    h.peer.expect_error(ErrorCode::FRAME_SIZE_ERROR, 4004).await;
    let (r, _) = h.finished().await;
    assert_protocol_reason(&r, ErrorCode::FRAME_SIZE_ERROR);

    let mut h = connect().await;
    h.peer.send(Bytes::from_static(&[1, 0, 0])).await;
    h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;

    let mut h = connect().await;
    h.peer.send_json(json!({"t": "regsiter"})).await;
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(msg.contains("regsiter"), "{msg}");
}

#[tokio::test(start_paused = true)]
async fn unknown_frame_type_is_counted() {
    let mut h = connect().await;
    h.peer
        .send(Bytes::from_static(&[0x7f, 1, 2, 3, 0, 0, 0, 9, 1]))
        .await;
    h.peer.sync().await;
    h.peer.expect_quiet().await;
    assert_eq!(h.stats.snapshot().unknown_frames, 1);
}

#[tokio::test(start_paused = true)]
async fn second_welcome_and_hello_are_protocol_errors() {
    let mut h = connect().await;
    h.peer.send_json(welcome(262_144)).await;
    let msg = h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(msg.contains("second welcome"), "{msg}");

    let mut h = connect().await;
    h.peer
        .send_json(
            json!({"t": "hello", "v": 1, "token": "t", "agent": {"sdk": "x", "sdk_version": "1"}}),
        )
        .await;
    h.peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
}

// --- handshake ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn hello_contents() {
    let mut cfg = config();
    cfg.capabilities = vec!["a.b".into()];
    cfg.meta = Some(json!({"k": 1}).as_object().unwrap().clone());
    let (client, mut peer) = pair().await;
    let (tx, _rx) = mpsc::unbounded_channel();
    let task =
        tokio::spawn(
            async move { Conn::establish(client, &cfg, "tok".into(), tx, ConnId(1)).await },
        );
    let hello = peer.control().await;
    assert_eq!(hello["t"], "hello");
    assert_eq!(hello["v"], 1);
    assert_eq!(hello["token"], "tok");
    assert_eq!(hello["agent"]["sdk"], "ws-mixer-rust");
    assert_eq!(hello["window"], 16_384);
    assert_eq!(hello["max_streams"], 64);
    assert_eq!(hello["capabilities"], json!(["a.b"]));
    assert_eq!(hello["meta"], json!({"k": 1}));
    peer.send_json(welcome(262_144)).await;
    let hs = task.await.unwrap().unwrap();
    assert_eq!(hs.welcome().session, "s-1");
    assert_eq!(hs.conn_id(), ConnId(1));
}

#[tokio::test(start_paused = true)]
async fn handle_drain_and_parts() {
    let mut h = connect().await;
    let _a = h.open(3).await;
    let handle = h.conn.handle().clone();
    assert_eq!(handle.conn_id(), ConnId(1));
    assert_eq!(handle.welcome().session, "s-1");
    handle.send_drain(Duration::from_millis(2500)).unwrap();
    assert_eq!(
        h.peer.control().await,
        json!({"t": "drain", "reason": "client_requested", "last_stream_id": 3, "deadline_ms": 2500})
    );
    let Harness { mut peer, conn, .. } = h;
    let (handle, done) = conn.into_parts();
    peer.ws_close(1000, "").await;
    let (r, _) = tokio::join!(done, peer.finish());
    assert_eq!(r.unwrap().ws_code, Some(1000));
    assert!(handle.is_terminal());
    assert_eq!(
        handle.send_drain(Duration::ZERO),
        Err(crate::error::CloseError::NotConnected)
    );
}

#[tokio::test(start_paused = true)]
async fn welcome_timeout_is_handshake_4001() {
    let t0 = Instant::now();
    let (mut peer, task, _events) = start_handshake(config()).await;
    let msg = peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert!(msg.contains("no welcome within 10000ms"), "{msg}");
    assert!(Instant::now().duration_since(t0) >= Duration::from_secs(10));
    let f = task.await.unwrap().err().unwrap();
    assert!(f.welcome_timeout);
    let r = f.reason;
    assert_eq!(r.phase, Phase::Handshake);
    assert_eq!(r.ws_code, Some(4001));
    assert_eq!(r.error_code, Some(ErrorCode::PROTOCOL_ERROR));
    assert_eq!(r.close_reason, None);
    assert_eq!(r.http_status, None);
    assert!(!r.fatal);
}

async fn handshake_failure(reply: Got) -> (DisconnectReason, Peer, Arc<Counters>) {
    let cfg = config();
    let stats = cfg.stats.clone();
    let (mut peer, task, _events) = start_handshake(cfg).await;
    let is_close = matches!(reply, Got::Close(..));
    match reply {
        Got::Frame(f) => peer.send(f.encode()).await,
        Got::Control(v) => peer.send_json(v).await,
        Got::Close(code, reason) => peer.ws_close(code, &reason).await,
        Got::End => {}
    }
    let r = tokio::time::timeout(TIMEOUT, async {
        let (r, _) = tokio::join!(task, async {
            if is_close {
                peer.finish().await;
            }
        });
        r
    })
    .await
    .unwrap()
    .unwrap()
    .err()
    .unwrap();
    assert!(!r.welcome_timeout);
    let r = *r.reason;
    assert_eq!(r.phase, Phase::Handshake);
    (r, peer, stats)
}

#[tokio::test(start_paused = true)]
async fn frames_before_welcome_are_protocol_errors() {
    let (r, mut peer, stats) = handshake_failure(Got::Frame(Frame::Open { stream_id: 1 })).await;
    assert_eq!(r.ws_code, Some(4001));
    assert!(
        r.message
            .contains("OPEN frame for stream 1 received before welcome"),
        "{}",
        r.message
    );
    peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;
    assert_eq!(stats.snapshot().protocol_violations, 1);

    let (r, mut peer, _) = handshake_failure(Got::Control(json!({"t": "app", "body": {}}))).await;
    assert!(
        r.message.contains("app received before welcome"),
        "{}",
        r.message
    );
    peer.expect_error(ErrorCode::PROTOCOL_ERROR, 4001).await;

    let (r, _, _) = handshake_failure(Got::Control(json!({"t": "ping", "id": 1}))).await;
    assert_eq!(r.error_code, Some(ErrorCode::PROTOCOL_ERROR));
}

#[tokio::test(start_paused = true)]
async fn unsupported_welcome_version_is_fatal_4010() {
    let mut w = welcome(262_144);
    w["v"] = json!(2);
    let (r, mut peer, _) = handshake_failure(Got::Control(w)).await;
    assert_eq!(r.ws_code, Some(4010));
    assert_eq!(r.error_code, Some(ErrorCode::UNSUPPORTED));
    assert!(r.fatal);
    peer.expect_error(ErrorCode::UNSUPPORTED, 4010).await;
}

#[tokio::test(start_paused = true)]
async fn subfloor_welcome_timing_rejected_unless_allowed() {
    let mut w = welcome(262_144);
    w["ping_interval"] = json!(100);
    w["ping_timeout"] = json!(300);
    let (r, _, _) = handshake_failure(Got::Control(w.clone())).await;
    assert!(
        r.message.contains("below the 5000ms floor"),
        "{}",
        r.message
    );

    let mut cfg = config();
    cfg.allow_subfloor_timing = true;
    let (mut peer, task, _events) = start_handshake(cfg).await;
    peer.send_json(w).await;
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn peer_error_and_close_before_welcome() {
    let (r, mut peer, _) = handshake_failure(Got::Control(
        json!({"t": "error", "code": 11, "message": "token expired 1"}),
    ))
    .await;
    assert_eq!(r.ws_code, Some(4011));
    assert_eq!(r.error_code, Some(ErrorCode::UNAUTHORIZED));
    assert_eq!(r.close_reason, None);
    // No error in reply, just the close.
    assert_eq!(peer.ws_closed().await.0, 4011);

    let (r, _, _) = handshake_failure(Got::Close(4011, "nope".into())).await;
    assert_eq!(r.ws_code, Some(4011));
    assert_eq!(r.error_code, Some(ErrorCode::UNAUTHORIZED));
    assert_eq!(r.close_reason.as_deref(), Some("nope"));

    let (r, _, _) = handshake_failure(Got::Close(1001, String::new())).await;
    assert_eq!(r.ws_code, Some(1001));
    assert_eq!(r.error_code, None);
    assert_eq!(r.close_reason, None);
}

// --- connection end ---------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn peer_error_code_above_999_is_clamped_on_the_wire_only() {
    let mut h = connect().await;
    h.peer
        .send_json(json!({"t": "error", "code": 1000, "message": "custom failure 1000"}))
        .await;
    // No error in reply; close code clamped to 4002.
    let (code, reason) = h.peer.ws_closed().await;
    assert_eq!((code, reason.as_str()), (4002, "custom failure 1000"));
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, Some(5000));
    assert_eq!(r.error_code, Some(ErrorCode(1000)));
    assert_eq!(r.error_name, Some("INTERNAL_ERROR"));
    assert_eq!(r.close_reason, None);
    assert!(r.message.contains("custom failure 1000"), "{}", r.message);
}

#[tokio::test(start_paused = true)]
async fn bare_peer_close() {
    let mut h = connect().await;
    let mut s = h.open(1).await;
    h.peer.ws_close(4012, "draining: rollout").await;
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, Some(4012));
    assert_eq!(r.error_code, Some(ErrorCode::GOING_AWAY));
    assert_eq!(r.close_reason.as_deref(), Some("draining: rollout"));
    assert_lost(&read_some(&mut s).await.unwrap_err());

    let h = {
        let mut h = connect().await;
        h.peer.ws_close(1000, "").await;
        h
    };
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, Some(1000));
    assert_eq!(r.error_code, None);
    assert_eq!(r.close_reason, None);
}

#[tokio::test(start_paused = true)]
async fn abnormal_closure_fails_every_stream() {
    let mut h = connect().await;
    let mut a = h.open(1).await;
    let mut b = h.open(3).await;
    let mut c = h.open(5).await;
    let mut d = h.open(7).await;
    h.peer.data(1, b"buffered").await;
    h.peer.data(3, b"done").await;
    h.peer.close(3).await;
    h.peer.reset(7, ErrorCode::CANCEL).await;
    h.peer.sync().await;
    let pending = tokio::spawn(async move {
        let r = read_some(&mut c).await;
        (c, r)
    });
    tokio::task::yield_now().await;
    let Harness { peer, conn, .. } = h;
    drop(peer);
    let r = tokio::time::timeout(TIMEOUT, conn.finished())
        .await
        .unwrap();
    assert_eq!(r.ws_code, None);
    assert_eq!(r.error_code, None);
    assert_eq!(r.phase, Phase::Connected);

    assert_eq!(read_some(&mut a).await.unwrap(), b"buffered");
    assert_lost(&read_some(&mut a).await.unwrap_err());
    assert_lost(&a.write(b"x").await.unwrap_err());
    assert_lost(&a.flush().await.unwrap_err());
    assert_eq!(read_to_end(&mut b).await.unwrap(), b"done");
    let (mut c, res) = pending.await.unwrap();
    assert_lost(&res.unwrap_err());
    assert_lost(&c.close_write().await.unwrap_err());
    assert_reset(&read_some(&mut d).await.unwrap_err(), ErrorCode::CANCEL);
}

#[tokio::test(start_paused = true)]
async fn graceful_close_without_streams() {
    let h = connect().await;
    h.conn.close_graceful();
    let mut h = h;
    let d = h.peer.control().await;
    assert_eq!(
        d,
        json!({"t": "drain", "reason": "client_requested", "last_stream_id": 0, "deadline_ms": 5000})
    );
    let e = h.peer.control().await;
    assert_eq!(e["t"], "error");
    assert_eq!(e["code"], 0);
    assert_eq!(h.peer.ws_closed().await.0, 1000);
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, Some(1000));
    assert_eq!(r.error_code, None);
    assert!(!r.fatal);
}

#[tokio::test(start_paused = true)]
async fn graceful_close_waits_for_streams_then_cancels_survivors() {
    let mut h = connect().await;
    let mut a = h.open(1).await;
    let _b = h.open(3).await;
    let t0 = Instant::now();
    h.conn.close_graceful();
    let d = h.peer.control().await;
    assert_eq!(d["last_stream_id"], 3);
    // New OPENs are refused while closing.
    h.peer.open(5).await;
    assert!(matches!(
        h.peer.frame().await,
        Frame::Reset {
            stream_id: 5,
            code: ErrorCode::REFUSED_STREAM,
            ..
        }
    ));
    // Stream 1 finishes normally.
    h.peer.close(1).await;
    assert_eq!(read_to_end(&mut a).await.unwrap(), b"");
    a.write_all(b"bye").await.unwrap();
    a.close_write().await.unwrap();
    assert!(matches!(
        h.peer.frame().await,
        Frame::Data { stream_id: 1, .. }
    ));
    assert!(matches!(
        h.peer.frame().await,
        Frame::Close { stream_id: 1, .. }
    ));
    // Stream 3 never does: CANCEL at the 5 s deadline.
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 3, code, ..
        } => assert_eq!(code, ErrorCode::CANCEL),
        f => panic!("{f:?}"),
    }
    assert!(Instant::now().duration_since(t0) >= Duration::from_secs(5));
    let e = h.peer.control().await;
    assert_eq!(
        (e["t"].as_str(), e["code"].as_u64()),
        (Some("error"), Some(0))
    );
    assert_eq!(h.peer.ws_closed().await.0, 1000);
}

#[tokio::test(start_paused = true)]
async fn graceful_close_finishes_early_when_streams_end() {
    let mut h = connect().await;
    let mut a = h.open(1).await;
    let t0 = Instant::now();
    h.conn.close_graceful();
    assert_eq!(h.peer.control().await["t"], "drain");
    h.peer.close(1).await;
    assert_eq!(read_to_end(&mut a).await.unwrap(), b"");
    a.close_write().await.unwrap();
    assert!(matches!(
        h.peer.frame().await,
        Frame::Close { stream_id: 1, .. }
    ));
    assert_eq!(h.peer.control().await["code"], 0);
    assert_eq!(h.peer.ws_closed().await.0, 1000);
    assert!(Instant::now().duration_since(t0) < Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn close_with_sends_application_close() {
    let mut h = connect().await;
    h.conn.send_app(json!({"last": true})).unwrap();
    let message = format!("CONN_LIMIT: {}", "é".repeat(200));
    h.conn.close_with(&message);
    assert_eq!(h.conn.send_app(json!({})), Err(SendAppError::NotConnected));
    assert_eq!(h.peer.control().await["t"], "app");
    let e = h.peer.control().await;
    assert_eq!(e["code"], 14);
    assert_eq!(e["message"], message.as_str());
    let (code, reason) = h.peer.ws_closed().await;
    assert_eq!(code, 4014);
    assert_eq!(reason.as_str(), crate::error::close_reason(&message));
    assert!(reason.len() <= 123 && reason.starts_with("CONN_LIMIT: "));
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, Some(4014));
    assert_eq!(r.error_code, Some(ErrorCode::APPLICATION_CLOSE));
    assert_eq!(r.close_reason, None);
    assert!(!r.fatal);
}

#[tokio::test(start_paused = true)]
async fn cancel_ends_without_handshake() {
    let mut h = connect().await;
    let mut s = h.open(1).await;
    h.conn.cancel();
    assert!(matches!(h.peer.next().await, Got::End));
    assert_lost(&read_some(&mut s).await.unwrap_err());
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, None);
}

#[tokio::test(start_paused = true)]
async fn close_without_status_is_1005() {
    let mut h = connect().await;
    h.peer.ws.send(Message::Close(None)).await.unwrap();
    let (r, _) = h.finished().await;
    assert_eq!(r.ws_code, Some(1005));
    assert_eq!((r.error_code, r.close_reason), (None, None));
}

#[tokio::test(start_paused = true)]
async fn app_events_before_peer_error_are_delivered() {
    let mut h = connect().await;
    for i in 1..=2 {
        h.peer
            .send_json(json!({"t": "app", "body": {"i": i}}))
            .await;
    }
    h.peer
        .send_json(json!({"t": "error", "code": 2, "message": "internal 2"}))
        .await;
    assert_eq!(h.peer.ws_closed().await.0, 4002);
    for i in 1..=2 {
        match h.events.recv().await {
            Some(Event::App(a)) => assert_eq!(a.body["i"], i),
            other => panic!("{other:?}"),
        }
    }
    let (r, _) = h.finished().await;
    assert_eq!(r.error_code, Some(ErrorCode::INTERNAL_ERROR));
}

#[tokio::test(start_paused = true)]
async fn missing_close_echo_finishes_in_2s() {
    let Harness { mut peer, conn, .. } = connect().await;
    let t0 = Instant::now();
    conn.close_with("bye 1");
    // The peer reads nothing, so it never echoes the close.
    let r = tokio::time::timeout(TIMEOUT, conn.finished())
        .await
        .unwrap();
    let elapsed = Instant::now().duration_since(t0);
    assert!(
        elapsed >= Duration::from_secs(2) && elapsed < Duration::from_millis(2500),
        "{elapsed:?}"
    );
    assert_eq!(r.ws_code, Some(4014));
    peer.expect_error(ErrorCode::APPLICATION_CLOSE, 4014).await;
}

// --- stalled socket -----------------------------------------------------------------

type CountingWriter = tokio::task::JoinHandle<(Stream, io::Result<()>)>;

/// Opens stream 1, credits it up to 2^31-1 (`window` is the welcome window)
/// and writes into it until the socket is full, the peer not reading: the
/// client's writer is then blocked mid-send and stream 1's out queue is full.
/// Returns the writing task and the bytes `poll_write` has accepted.
async fn stall_writer(h: &mut Harness, window: u32) -> (CountingWriter, Arc<AtomicUsize>) {
    let mut s = h.open(1).await;
    h.peer.window(1, 0x7fff_ffff - window).await;
    let written = Arc::new(AtomicUsize::new(0));
    let w = written.clone();
    let task = tokio::spawn(async move {
        let chunk = vec![1u8; 16_384];
        let r: io::Result<()> = async {
            loop {
                let n = s.write(&chunk).await?;
                w.fetch_add(n, Ordering::SeqCst);
            }
        }
        .await;
        (s, r)
    });
    // Paused time moves only once every task is idle: nothing moves now.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stalled = written.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(written.load(Ordering::SeqCst), stalled);
    assert!(stalled >= 1 << 20, "{stalled}");
    (task, written)
}

#[tokio::test(start_paused = true)]
async fn blocked_writes_fail_when_the_connection_dies() {
    let mut h = connect_with(config(), welcome(16_384)).await;
    // Stream 1 blocks on a full out queue.
    let (queue_blocked, _) = stall_writer(&mut h, 16_384).await;
    // Stream 3 blocks on zero credit: one chunk queued, then no credit.
    let mut s = h.open(3).await;
    let credit_blocked = tokio::spawn(async move { s.write_all(&vec![3u8; 16_384 + 10]).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!credit_blocked.is_finished());
    let Harness { peer, conn, .. } = h;
    drop(peer);
    let r = tokio::time::timeout(TIMEOUT, conn.finished())
        .await
        .unwrap();
    assert_eq!((r.ws_code, r.error_code), (None, None));
    let (_s, res) = queue_blocked.await.unwrap();
    assert_lost(&res.unwrap_err());
    assert_lost(&credit_blocked.await.unwrap().unwrap_err());
}

#[tokio::test(start_paused = true)]
async fn peer_not_reading_hits_the_write_timeout_without_an_error_frame() {
    let mut h = connect().await;
    let t0 = Instant::now();
    let (writer, _) = stall_writer(&mut h, 262_144).await;
    let Harness { mut peer, conn, .. } = h;
    let r = tokio::time::timeout(TIMEOUT, conn.finished())
        .await
        .unwrap();
    let elapsed = Instant::now().duration_since(t0);
    assert!(
        elapsed >= Duration::from_secs(10) && elapsed < Duration::from_secs(11),
        "{elapsed:?}"
    );
    assert_eq!((r.ws_code, r.error_code), (None, None));
    assert!(r.message.contains("no progress for 10s"), "{}", r.message);
    assert_lost(&writer.await.unwrap().1.unwrap_err());
    // Only DATA was ever written: no error frame, no close.
    loop {
        match peer.next().await {
            Got::Frame(Frame::Data { stream_id: 1, .. }) => {}
            Got::End => break,
            g => panic!("{g:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn close_with_against_a_non_reading_peer_finishes_in_2s() {
    let mut h = connect().await;
    let (writer, _) = stall_writer(&mut h, 262_144).await;
    let Harness { peer, conn, .. } = h;
    let t0 = Instant::now();
    conn.close_with("bye 2");
    let r = tokio::time::timeout(TIMEOUT, conn.finished())
        .await
        .unwrap();
    let elapsed = Instant::now().duration_since(t0);
    assert!(
        elapsed >= Duration::from_secs(2) && elapsed < Duration::from_millis(2500),
        "{elapsed:?}"
    );
    assert_eq!(r.ws_code, Some(4014));
    assert_lost(&writer.await.unwrap().1.unwrap_err());
    drop(peer);
}

#[tokio::test(start_paused = true)]
async fn pong_jumps_queued_data_on_the_wire() {
    let mut h = connect().await;
    let (_writer, written) = stall_writer(&mut h, 262_144).await;
    h.peer
        .send_json(json!({"t": "ping", "id": 77, "ts": 7}))
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Stream 1 still has two chunks queued; everything else was handed to
    // the writer before the pong.
    let before = written.load(Ordering::SeqCst) - 2 * 16_384;
    let mut seen = 0;
    loop {
        match h.peer.recv_raw().await {
            Got::Frame(Frame::Data {
                stream_id: 1,
                payload,
            }) => seen += payload.len(),
            Got::Control(v) if v["t"] == "pong" => {
                assert_eq!(v["id"], 77);
                break;
            }
            Got::Control(v) if v["t"] == "ping" => {}
            g => panic!("{g:?}"),
        }
    }
    assert_eq!(seen, before);
    assert!(matches!(
        h.peer.recv_raw().await,
        Got::Frame(Frame::Data { stream_id: 1, .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn pong_queue_overflow_is_enhance_your_calm() {
    let mut h = connect().await;
    let (_writer, _) = stall_writer(&mut h, 262_144).await;
    // The stream-0 rate limit would trip first.
    h.conn.handle().shared.lock().stream0 = TokenBucket::new(2000.0, STREAM0_RATE);
    for id in 0..=1000 {
        h.peer.send_json(json!({"t": "ping", "id": id})).await;
    }
    let Harness { peer, conn, .. } = h;
    let r = tokio::time::timeout(TIMEOUT, conn.finished())
        .await
        .unwrap();
    assert_protocol_reason(&r, ErrorCode::ENHANCE_YOUR_CALM);
    assert!(
        r.message
            .contains("ping 1000 with 1000 pongs already queued"),
        "{}",
        r.message
    );
    drop(peer);
}

// --- stream handles -----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn drop_resets_unfinished_streams_only() {
    let mut h = connect().await;
    let a = h.open(1).await;
    drop(a);
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 1, code, ..
        } => assert_eq!(code, ErrorCode::CANCEL),
        f => panic!("{f:?}"),
    }
    let mut b = h.open(3).await;
    h.peer.close(3).await;
    assert_eq!(read_to_end(&mut b).await.unwrap(), b"");
    b.close_write().await.unwrap();
    assert!(matches!(
        h.peer.frame().await,
        Frame::Close { stream_id: 3, .. }
    ));
    drop(b);
    let c = h.open(5).await;
    c.refuse();
    assert!(matches!(
        h.peer.frame().await,
        Frame::Reset {
            stream_id: 5,
            code: ErrorCode::REFUSED_STREAM,
            ..
        }
    ));
    let d = h.open(7).await;
    d.reset(ErrorCode(0x1000_0001), "app code");
    d.reset(ErrorCode::CANCEL, "ignored: already reset");
    match h.peer.frame().await {
        Frame::Reset {
            stream_id: 7,
            code,
            message,
        } => {
            assert_eq!(code, ErrorCode(0x1000_0001));
            assert_eq!(message, "app code");
        }
        f => panic!("{f:?}"),
    }
    drop(d);
    h.peer.sync().await;
    h.peer.expect_quiet().await;
}

#[tokio::test(start_paused = true)]
async fn split_halves_work_concurrently() {
    let mut h = connect().await;
    let s = h.open(1).await;
    let (mut r, mut w) = s.into_split();
    assert_eq!((r.id(), w.id()), (1, 1));
    let reader = tokio::spawn(async move { read_to_end(&mut r).await.unwrap() });
    let writer = tokio::spawn(async move {
        w.write_all(&vec![5u8; 40_000]).await.unwrap();
        w.shutdown().await.unwrap();
    });
    for _ in 0..4 {
        h.peer.data(1, &[6u8; 4000]).await;
    }
    h.peer.close(1).await;
    let mut got = 0;
    loop {
        match h.peer.frame().await {
            Frame::Data {
                stream_id: 1,
                payload,
            } => got += payload.len(),
            Frame::Window { .. } => {}
            Frame::Close { stream_id: 1, .. } => break,
            f => panic!("{f:?}"),
        }
    }
    assert_eq!(got, 40_000);
    writer.await.unwrap();
    assert_eq!(reader.await.unwrap(), vec![6u8; 16_000]);
    // Both halves dropped after a full close: no RESET.
    h.peer.sync().await;
    h.peer.expect_quiet().await;
}

// --- dial (real sockets, real time) -------------------------------------------------

mod dial_tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    async fn listener() -> (TcpListener, ConnConfig) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = config();
        cfg.url = format!("ws://{}/v1/tunnel", l.local_addr().unwrap());
        cfg.headers = vec![(
            http::HeaderName::from_static("x-extra"),
            http::HeaderValue::from_static("1"),
        )];
        cfg.connect_timeout = Duration::from_secs(5);
        (l, cfg)
    }

    #[tokio::test]
    async fn missing_subprotocol_is_fatal_unsupported() {
        let (l, cfg) = listener().await;
        let server = tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let mut seen = None;
            let cb = |req: &Request, resp: Response| {
                seen = Some(req.headers().clone());
                Ok(resp)
            };
            let ws = tokio_tungstenite::accept_hdr_async(tcp, cb).await;
            (seen.unwrap(), ws.is_ok())
        });
        let (tx, _rx) = mpsc::unbounded_channel();
        let f = Conn::dial(&cfg, "tok".into(), tx, ConnId(7))
            .await
            .err()
            .unwrap();
        let r = f.reason;
        assert_eq!(r.phase, Phase::Dial);
        assert_eq!(r.error_code, Some(ErrorCode::UNSUPPORTED));
        assert_eq!(r.error_name, Some("UNSUPPORTED"));
        assert_eq!((r.ws_code, r.http_status), (None, None));
        assert!(r.fatal);
        assert_eq!(r.conn_id, Some(ConnId(7)));
        assert_eq!(cfg.stats.snapshot().dials, 1);

        let (headers, _) = server.await.unwrap();
        assert_eq!(headers["authorization"], "Bearer tok");
        assert_eq!(headers["sec-websocket-protocol"], "ws-mixer.v1");
        assert_eq!(headers["x-extra"], "1");
        let ua = headers["user-agent"].to_str().unwrap();
        assert!(
            ua.starts_with(&format!("ws-mixer-rust/{} (rust; ", crate::SDK_VERSION)),
            "{ua}"
        );
        assert!(!headers.contains_key("sec-websocket-extensions"));
    }

    async fn http_reply(raw: impl Into<String>) -> ConnFailure {
        let raw = raw.into();
        let (l, cfg) = listener().await;
        tokio::spawn(async move {
            let (mut tcp, _) = l.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = tcp.read(&mut buf).await;
            tcp.write_all(raw.as_bytes()).await.unwrap();
            tcp.shutdown().await.unwrap();
        });
        let (tx, _rx) = mpsc::unbounded_channel();
        Conn::dial(&cfg, "tok".into(), tx, ConnId(1))
            .await
            .err()
            .unwrap()
    }

    #[tokio::test]
    async fn http_rejections() {
        let f =
            http_reply("HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\n\r\nno protocol").await;
        assert_eq!(f.reason.phase, Phase::Dial);
        assert_eq!(f.reason.http_status, Some(400));
        assert_eq!((f.reason.error_code, f.reason.ws_code), (None, None));
        assert!(!f.reason.fatal);
        assert!(f.reason.message.contains("400"), "{}", f.reason.message);

        let f = http_reply(
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 7\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert_eq!(f.reason.http_status, Some(429));
        assert_eq!(f.retry_after, Some(Duration::from_secs(7)));

        let at = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(120));
        let f = http_reply(format!(
            "HTTP/1.1 503 Service Unavailable\r\nRetry-After: {at}\r\nContent-Length: 0\r\n\r\n"
        ))
        .await;
        assert_eq!(f.reason.http_status, Some(503));
        let wait = f.retry_after.unwrap();
        assert!(
            wait > Duration::from_secs(100) && wait <= Duration::from_secs(120),
            "{wait:?}"
        );

        // Retry-After only counts on 429 and 503.
        let f = http_reply(
            "HTTP/1.1 500 Internal Server Error\r\nRetry-After: 7\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert_eq!(f.reason.http_status, Some(500));
        assert_eq!(f.retry_after, None);

        let f = http_reply("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n").await;
        assert_eq!(f.reason.http_status, Some(401));
        assert_eq!(f.reason.error_code, None);
    }

    #[test]
    fn retry_after_seconds_and_http_date() {
        use super::super::dial::parse_retry_after;
        let date = "Sun, 06 Nov 1994 08:49:37 GMT";
        let at = httpdate::parse_http_date(date).unwrap();
        let s = Duration::from_secs;
        assert_eq!(parse_retry_after(" 12 ", at), Some(s(12)));
        assert_eq!(parse_retry_after(date, at - s(30)), Some(s(30)));
        assert_eq!(parse_retry_after(date, at + s(30)), Some(Duration::ZERO));
        assert_eq!(parse_retry_after("soon", at), None);
        assert_eq!(parse_retry_after("-1", at), None);
    }

    #[tokio::test]
    async fn connect_refused_and_bad_urls() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let mut cfg = config();
        cfg.url = format!("ws://{addr}/v1/tunnel");
        let (tx, _rx) = mpsc::unbounded_channel();
        let f = Conn::dial(&cfg, "tok".into(), tx.clone(), ConnId(1))
            .await
            .err()
            .unwrap();
        assert_eq!(f.reason.phase, Phase::Dial);
        assert!(f.reason.cause.is_some());

        cfg.url = "http://example.com/".into();
        let f = Conn::dial(&cfg, "tok".into(), tx, ConnId(1))
            .await
            .err()
            .unwrap();
        assert_eq!(f.reason.phase, Phase::Dial);
    }

    #[tokio::test]
    async fn dial_and_handshake_over_tcp() {
        let (l, cfg) = listener().await;
        let server = tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let cb = |_: &Request, mut resp: Response| {
                resp.headers_mut().insert(
                    "sec-websocket-protocol",
                    http::HeaderValue::from_static("ws-mixer.v1"),
                );
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(tcp, cb).await.unwrap();
            let hello = ws.next().await.unwrap().unwrap();
            let hello = match hello {
                Message::Binary(b) => b,
                m => panic!("{m:?}"),
            };
            assert!(hello[8..].starts_with(br#"{"t":"hello""#));
            let w = welcome(262_144).to_string();
            ws.send(Message::Binary(frame::encode_data(0, w.as_bytes())))
                .await
                .unwrap();
            ws.send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(4012),
                reason: "bye".into(),
            })))
            .await
            .unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });
        let (tx, _rx) = mpsc::unbounded_channel();
        let hs = Conn::dial(&cfg, "tok".into(), tx, ConnId(1)).await.unwrap();
        assert_eq!(hs.welcome().window, 262_144);
        let live = hs.start();
        let r = live.finished().await;
        assert_eq!(r.ws_code, Some(4012));
        assert_eq!(r.close_reason.as_deref(), Some("bye"));
        server.await.unwrap();
        let s = cfg.stats.snapshot();
        assert_eq!((s.dials, s.connects), (1, 1));
        assert!(s.bytes_in > 0 && s.bytes_out > 0);
    }
}

#[test]
fn backlog_limit() {
    let mut cfg = config();
    cfg.max_streams = 64;
    assert_eq!(cfg.backlog_limit(), 128);
    cfg.max_streams = 1000;
    assert_eq!(cfg.backlog_limit(), 1064);
}

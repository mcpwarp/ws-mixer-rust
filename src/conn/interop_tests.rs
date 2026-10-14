//! Interop against ws-mixer-go's `cmd/testserver` (the JS SDK's interop
//! peer). Ignored by default: needs Go and a ws-mixer-go checkout, found via
//! `$WSMIXER_GOSERVER_DIR`, `.goserver` (`make fetch-goserver`) or
//! `../ws-mixer-go`. Run with `cargo test -- --ignored interop`.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::{Conn, ConnConfig};
use crate::control::AgentInfo;
use crate::error::ErrorCode;
use crate::event::{ConnId, DrainReason, Event, Events};
use crate::stats::Counters;

fn goserver_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("WSMIXER_GOSERVER_DIR") {
        return Some(PathBuf::from(dir));
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    [root.join(".goserver"), root.join("../ws-mixer-go")]
        .into_iter()
        .find(|d| d.join("cmd/testserver/main.go").is_file())
}

fn checked_out_tag(dir: &Path) -> Option<String> {
    if let Ok(pin) = std::fs::read_to_string(dir.join(".pin")) {
        return Some(pin.trim().to_owned());
    }
    let out = Command::new("git")
        .args(["describe", "--tags", "--exact-match"])
        .current_dir(dir)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// The testserver child; killed on drop so a failed test never leaks it.
struct Server {
    child: Child,
    stdin: ChildStdin,
    events: mpsc::UnboundedReceiver<Value>,
}

impl Server {
    fn start(dir: &Path) -> Self {
        // Built once: the interop tests run in parallel and must not
        // overwrite the binary while another one is starting it.
        static BUILT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        let bin = BUILT.get_or_init(|| {
            let bin =
                Path::new(env!("CARGO_MANIFEST_DIR")).join("target/interop/wsmixer-testserver");
            let status = Command::new("go")
                .args(["build", "-o"])
                .arg(&bin)
                .arg("./cmd/testserver")
                .current_dir(dir)
                .status()
                .expect("running go build");
            assert!(status.success(), "go build ./cmd/testserver failed");
            bin
        });
        let mut child = Command::new(bin)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("starting the testserver");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, events) = mpsc::unbounded_channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                eprintln!("testserver: {line}");
                if let Ok(v) = serde_json::from_str(&line) {
                    if tx.send(v).is_err() {
                        break;
                    }
                }
            }
        });
        Self {
            child,
            stdin,
            events,
        }
    }

    fn cmd(&mut self, v: Value) {
        writeln!(self.stdin, "{v}").unwrap();
        self.stdin.flush().unwrap();
    }

    async fn event(&mut self, name: &str) -> Value {
        loop {
            let v = tokio::time::timeout(Duration::from_secs(20), self.events.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for testserver event {name}"))
                .expect("testserver exited");
            assert_ne!(v["event"], "error", "testserver error: {v}");
            if v["event"] == name {
                return v;
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn next_event(events: &mut Events) -> Event {
    tokio::time::timeout(Duration::from_secs(20), events.recv())
        .await
        .expect("timed out waiting for a client event")
        .expect("event channel closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Go and a ws-mixer-go checkout; run with --ignored"]
async fn interop_with_go_testserver() {
    let Some(dir) = goserver_dir() else {
        eprintln!(
            "skipping: no ws-mixer-go checkout (set WSMIXER_GOSERVER_DIR or run `make fetch-goserver`)"
        );
        return;
    };
    let pin = include_str!("../../goserver.pin").trim();
    match checked_out_tag(&dir) {
        Some(tag) if tag == pin => {}
        other => eprintln!(
            "warning: {} is at {other:?}, goserver.pin is {pin}",
            dir.display()
        ),
    }
    tokio::time::timeout(Duration::from_secs(180), run(&dir))
        .await
        .expect("interop test timed out");
}

/// The public `Client` against the testserver: drain, parallel reconnect,
/// then a fresh round trip on the replacement connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Go and a ws-mixer-go checkout; run with --ignored"]
async fn interop_client_drain_reconnect() {
    let Some(dir) = goserver_dir() else {
        eprintln!(
            "skipping: no ws-mixer-go checkout (set WSMIXER_GOSERVER_DIR or run `make fetch-goserver`)"
        );
        return;
    };
    tokio::time::timeout(Duration::from_secs(180), run_client(&dir))
        .await
        .expect("interop test timed out");
}

/// Answers one server-opened stream: request in, `echo:` response out.
async fn echo(events: &mut Events) -> tokio::task::JoinHandle<String> {
    let Event::Stream(mut s) = next_event(events).await else {
        panic!("expected a stream");
    };
    tokio::spawn(async move {
        let mut req = Vec::new();
        s.read_to_end(&mut req).await.unwrap();
        let mut resp = b"echo:".to_vec();
        resp.extend_from_slice(&req);
        s.write_all(&resp).await.unwrap();
        s.close_write().await.unwrap();
        String::from_utf8(req).unwrap()
    })
}

async fn run_client(dir: &Path) {
    let mut server = Server::start(dir);
    let addr = server.event("listening").await["addr"]
        .as_str()
        .unwrap()
        .to_owned();
    server.cmd(json!({"cmd": "wait_conn"}));
    let (client, mut events) = crate::Client::builder(format!("ws://{addr}/v1/tunnel"))
        .token("test-token")
        .connect()
        .unwrap();
    let first = match next_event(&mut events).await {
        Event::Connected(c) => c,
        other => panic!("expected Connected, got {other:?}"),
    };
    assert!(!first.reconnect);
    assert_eq!(
        server.event("connected").await["session"],
        first.session.as_str()
    );

    server.cmd(json!({"cmd": "open_streams", "count": 1, "prefix": "before"}));
    assert_eq!(echo(&mut events).await.await.unwrap(), "before-0");
    assert_eq!(
        server.event("stream_result").await["response"],
        "echo:before-0"
    );

    // Drain: the client dials a replacement in parallel; the old connection
    // ends (the server's 4012 once its table is empty, or our own 1000).
    server.cmd(json!({"cmd": "drain"}));
    server.cmd(json!({"cmd": "wait_conn"}));
    let mut second = None;
    let mut old_end = None;
    while second.is_none() || old_end.is_none() {
        match next_event(&mut events).await {
            Event::Drain(d) => assert_eq!(d.conn_id, first.conn_id),
            Event::Redial(r) => assert_eq!(
                (r.conn_id, r.cause),
                (Some(first.conn_id), crate::RedialCause::Drain)
            ),
            Event::Connected(c) => second = Some(c),
            Event::Disconnected(r) => {
                assert_eq!(r.conn_id, Some(first.conn_id), "{r:?}");
                assert!(!r.fatal, "{r:?}");
                assert!(matches!(r.ws_code, Some(4012 | 1000)), "{r:?}");
                old_end = Some(r);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    let second = second.unwrap();
    assert!(second.reconnect);
    assert_ne!(second.session, first.session);
    server.event("connected").await;

    server.cmd(json!({"cmd": "open_streams", "count": 1, "prefix": "after"}));
    assert_eq!(echo(&mut events).await.await.unwrap(), "after-0");
    let r = server.event("stream_result").await;
    assert_eq!(
        (r["ok"].as_bool(), r["response"].as_str()),
        (Some(true), Some("echo:after-0"))
    );
    client.send_app(json!({"on": "replacement"})).await.unwrap();
    server.cmd(json!({"cmd": "wait_app"}));
    assert_eq!(
        server.event("app_received").await["body"],
        json!({"on": "replacement"})
    );

    client.close().await;
    match next_event(&mut events).await {
        Event::Disconnected(r) => assert_eq!(
            (r.conn_id, r.ws_code, r.fatal),
            (Some(second.conn_id), Some(1000), false)
        ),
        other => panic!("expected Disconnected, got {other:?}"),
    }
    assert!(events.recv().await.is_none());
    let s = client.stats();
    assert_eq!(
        (
            s.dials,
            s.connects,
            s.streams_accepted,
            s.protocol_violations
        ),
        (2, 2, 2, 0)
    );
    server.cmd(json!({"cmd": "quit"}));
}

async fn run(dir: &Path) {
    let mut server = Server::start(dir);
    let addr = server.event("listening").await["addr"]
        .as_str()
        .unwrap()
        .to_owned();
    server.cmd(json!({"cmd": "wait_conn"}));

    let stats = Arc::new(Counters::default());
    let cfg = ConnConfig {
        url: format!("ws://{addr}/v1/tunnel"),
        headers: Vec::new(),
        tls: None,
        connect_timeout: Duration::from_secs(10),
        hello_timeout: Duration::from_secs(10),
        window: 262_144,
        max_streams: 64,
        agent: AgentInfo::default(),
        capabilities: Vec::new(),
        meta: None,
        allow_subfloor_timing: false,
        stats: stats.clone(),
        drains: None,
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let mut events = Events::new(rx);
    let hs = Conn::dial(&cfg, "test-token".into(), tx, ConnId(1))
        .await
        .unwrap_or_else(|f| panic!("dial failed: {:?}", f.reason));
    let session = hs.welcome().session.clone();
    let live = hs.start();
    assert_eq!(server.event("connected").await["session"], session.as_str());

    // Server-opened streams: request in, echoed response out.
    server.cmd(json!({"cmd": "open_streams", "count": 3, "prefix": "req"}));
    let mut handlers = Vec::new();
    for _ in 0..3 {
        let Event::Stream(mut s) = next_event(&mut events).await else {
            panic!("expected a stream");
        };
        handlers.push(tokio::spawn(async move {
            let mut req = Vec::new();
            s.read_to_end(&mut req).await.unwrap();
            let mut resp = b"echo:".to_vec();
            resp.extend_from_slice(&req);
            s.write_all(&resp).await.unwrap();
            s.close_write().await.unwrap();
            String::from_utf8(req).unwrap()
        }));
    }
    let mut requests = Vec::new();
    for h in handlers {
        requests.push(h.await.unwrap());
    }
    requests.sort();
    assert_eq!(requests, ["req-0", "req-1", "req-2"]);
    let mut responses = Vec::new();
    for _ in 0..3 {
        let r = server.event("stream_result").await;
        assert_eq!(r["ok"], true, "{r}");
        responses.push(r["response"].as_str().unwrap().to_owned());
    }
    responses.sort();
    assert_eq!(responses, ["echo:req-0", "echo:req-1", "echo:req-2"]);

    // app both ways.
    server.cmd(json!({"cmd": "send_app", "body": {"hello": "from-go"}}));
    match next_event(&mut events).await {
        Event::App(a) => assert_eq!(Value::Object(a.body), json!({"hello": "from-go"})),
        other => panic!("expected app, got {other:?}"),
    }
    live.send_app(json!({"hello": "from-rust", "n": 1}))
        .unwrap();
    server.cmd(json!({"cmd": "wait_app"}));
    assert_eq!(
        server.event("app_received").await["body"],
        json!({"hello": "from-rust", "n": 1})
    );

    // Drain: notice, then 4012 once the (empty) stream table lets it close.
    server.cmd(json!({"cmd": "drain"}));
    match next_event(&mut events).await {
        Event::Drain(d) => {
            assert_eq!(d.reason, DrainReason::Rollout);
            assert_eq!(d.deadline, Some(Duration::from_secs(2)));
        }
        other => panic!("expected drain, got {other:?}"),
    }
    let reason = tokio::time::timeout(Duration::from_secs(20), live.finished())
        .await
        .expect("connection did not end after drain");
    assert_eq!(reason.ws_code, Some(4012), "{reason:?}");
    assert_eq!(reason.error_code, Some(ErrorCode::GOING_AWAY));
    // Emitted by different goroutines: either order.
    let mut pending = vec!["drain_sent", "disconnected"];
    while !pending.is_empty() {
        let v = tokio::time::timeout(Duration::from_secs(20), server.events.recv())
            .await
            .expect("timed out waiting for drain_sent and disconnected")
            .expect("testserver exited");
        pending.retain(|e| v["event"] != *e);
    }
    server.cmd(json!({"cmd": "quit"}));

    let s = stats.snapshot();
    assert_eq!((s.streams_accepted, s.protocol_violations), (3, 0));
    eprintln!("interop ok: session {session}, {s:?}");
}

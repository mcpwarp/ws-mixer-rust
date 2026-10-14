//! `expose()` over a real ws-mixer client against ws-mixer-go's
//! `cmd/testserver`, which plays the edge: it forwards `app` messages both
//! ways, opens streams and drains on command. Ignored by default: needs Go
//! and a ws-mixer-go checkout (`$WSMIXER_GOSERVER_DIR`, `.goserver` from
//! `make fetch-goserver`, or `../ws-mixer-go`). Run with
//!
//! ```sh
//! cargo test -p ws-mixer-mcpwarp --all-features --test expose_live -- --ignored
//! ```

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use ws_mixer_mcpwarp::bridge::{RequestBody, handler_fn};
use ws_mixer_mcpwarp::{ExposeConfig, ExposeState, McpwarpErrorCode, RetryInfo, expose};

const ID: &str = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn goserver_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("WSMIXER_GOSERVER_DIR") {
        return Some(PathBuf::from(dir));
    }
    [repo().join(".goserver"), repo().join("../ws-mixer-go")]
        .into_iter()
        .find(|d| d.join("cmd/testserver/main.go").is_file())
}

/// The testserver child, killed on drop.
struct Server {
    child: Child,
    stdin: ChildStdin,
    events: mpsc::UnboundedReceiver<Value>,
}

impl Server {
    fn start(dir: &Path) -> Self {
        static BUILT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        let bin = BUILT.get_or_init(|| {
            let bin = repo().join("target/interop/wsmixer-testserver-mcpwarp");
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

    /// Waits for the next `app` message and returns its body.
    async fn app(&mut self) -> Value {
        self.cmd(json!({"cmd": "wait_app"}));
        self.event("app_received").await["body"].clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn registered(url: &str, created: bool) -> Value {
    json!({"mcpwarp": {"v": 1, "op": "registered",
        "services": [{"name": "notes", "id": "srv_1", "url": url, "created": created}], "errors": []}})
}

fn config(addr: &str, token: &str, retries: Arc<Mutex<Vec<RetryInfo>>>) -> ExposeConfig {
    ExposeConfig::builder()
        .url(format!("ws://{addr}/v1/tunnel"))
        .token(token)
        .name("notes")
        .integration(ID)
        .on_retry(move |r| retries.lock().unwrap().push(r.clone()))
        .handler(handler_fn(|req: http::Request<RequestBody>| async move {
            let body: Bytes = req.into_body().collect().await?.to_bytes();
            let res = http::Response::builder()
                .header(http::header::CONTENT_LENGTH, body.len())
                .body(Full::new(body))?;
            Ok(res)
        }))
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Go and a ws-mixer-go checkout; run with --ignored"]
async fn expose_against_go_testserver() {
    let Some(dir) = goserver_dir() else {
        eprintln!(
            "skipping: no ws-mixer-go checkout (set WSMIXER_GOSERVER_DIR or run `make fetch-goserver`)"
        );
        return;
    };
    let mut server = Server::start(&dir);
    let addr = server.event("listening").await["addr"]
        .as_str()
        .unwrap()
        .to_owned();
    let retries = Arc::new(Mutex::new(Vec::new()));

    // Register on the first welcome.
    server.cmd(json!({"cmd": "wait_conn"}));
    let exposing = tokio::spawn(expose(config(&addr, "test-token", retries.clone())));
    server.event("connected").await;
    assert_eq!(
        server.app().await,
        json!({"mcpwarp": {"v": 1, "op": "register", "services": [{"name": "notes", "kind": "http"}]}})
    );
    server.cmd(json!({"cmd": "send_app", "body": registered("https://notes.example", true)}));
    let handle = tokio::time::timeout(Duration::from_secs(10), exposing)
        .await
        .expect("expose() did not resolve")
        .unwrap()
        .unwrap();
    assert_eq!(handle.state(), ExposeState::Registered);
    assert_eq!(handle.url().as_deref(), Some("https://notes.example"));
    assert!(handle.client().is_some());

    // A request: the testserver appends "-0" to the prefix, inside the body.
    let request = "POST /mcp HTTP/1.1\r\nHost: notes.example\r\nContent-Length: 4\r\n\r\nab";
    server.cmd(json!({"cmd": "open_streams", "count": 1, "prefix": request}));
    let r = server.event("stream_result").await;
    assert_eq!(r["ok"], true, "{r}");
    let response = r["response"].as_str().unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.ends_with("\r\n\r\nab-0"), "{response}");

    // A rollout drain: the replacement registers again; on_retry saw it.
    server.cmd(json!({"cmd": "drain"}));
    server.cmd(json!({"cmd": "wait_conn"}));
    server.event("connected").await;
    assert_eq!(server.app().await["mcpwarp"]["op"], "register");
    server.cmd(json!({"cmd": "send_app", "body": registered("https://notes.example", false)}));
    let mut state = handle.watch_state();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !handle.registration().is_some_and(|r| !r.created) {
            state.changed().await.unwrap();
        }
    })
    .await
    .expect("no registration on the replacement");
    {
        let calls = retries.lock().unwrap();
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!((calls[0].attempt, calls[0].reason.as_str()), (0, "drain"));
        assert!(calls[0].delay <= Duration::from_secs(2));
    }

    // close(): unregister on the live connection, then a graceful close.
    let closing = {
        let h = handle.clone();
        tokio::spawn(async move { h.close().await })
    };
    assert_eq!(
        server.app().await,
        json!({"mcpwarp": {"v": 1, "op": "unregister", "services": [{"name": "notes"}]}})
    );
    server.cmd(json!({"cmd": "send_app", "body":
        {"mcpwarp": {"v": 1, "op": "unregistered", "services": [{"name": "notes", "id": "srv_1"}]}}}));
    tokio::time::timeout(Duration::from_secs(10), closing)
        .await
        .expect("close() hung")
        .unwrap();
    assert_eq!(handle.state(), ExposeState::Closed);
    assert_eq!(
        handle.client().unwrap().state(),
        ws_mixer::ClientState::Closed
    );

    // A rejected static token is fatal: UNAUTHORIZED.
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        expose(config(&addr, "wrong", Arc::default())),
    )
    .await
    .expect("expose() did not settle")
    .unwrap_err();
    assert_eq!(err.code, McpwarpErrorCode::Unauthorized, "{err:?}");
    assert!(err.fatal);

    server.cmd(json!({"cmd": "quit"}));
}

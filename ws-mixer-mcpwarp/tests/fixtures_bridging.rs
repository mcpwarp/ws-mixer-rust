//! `spec/fixtures/mcpwarp/bridging` (MCPWARP.md §10.3), as an agent runner:
//! each fixture's `in` is written on one in-memory stream served by
//! `bridge::serve_stream` in the fixture's `sdkState`, then:
//!
//! - no `1xx` precedes the response, and exactly one status line is written;
//! - the stream ends with `CLOSE`, never `RESET`;
//! - the application call equals `upstream` (or never happens when it is
//!   null);
//! - an SDK-generated reply equals `out` byte for byte; an application
//!   response is compared by status line, header set (names lower-cased,
//!   order free) and de-chunked body, both sides parsed as
//!   `check-fixtures.mjs` parses messages;
//! - a handler that fails is reported exactly once.
//!
//! Requests are buffered, so `*_reset` fixtures (streaming adapters only)
//! are listed and skipped (§10.3). `target` fixtures need the `target`
//! feature; the local server is a TCP listener on an ephemeral port that
//! stands in for the fixture's `127.0.0.1:3000`.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::Response;
use http_body_util::{BodyExt, Full};
use serde_json::Value;
use ws_mixer::Stream;
use ws_mixer::test_util::{PeerEvent, PeerHandle};
use ws_mixer_mcpwarp::bridge::{
    RegistrationState, RequestBody, ServeContext, ServeState, handler_fn, serve_stream,
};

/// Fixtures the runner must have run for the pinned spec (v0.6.2).
const MIN_RUN: usize = if cfg!(feature = "target") { 13 } else { 12 };

#[derive(Clone, Debug, PartialEq, Eq)]
struct Upstream {
    method: String,
    url: String,
    headers: BTreeMap<String, String>,
    body: Option<String>,
}

impl Upstream {
    fn from_fixture(v: &Value, addr: Option<&str>) -> Self {
        let url = v["url"].as_str().unwrap();
        Self {
            method: v["method"].as_str().unwrap().into(),
            url: match addr {
                Some(a) => url.replace("127.0.0.1:3000", a),
                None => url.into(),
            },
            headers: v["headers"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().into()))
                .collect(),
            body: v.get("body").map(|b| b.as_str().unwrap().into()),
        }
    }
}

type Calls = Arc<Mutex<Vec<Upstream>>>;

/// `name -> value`, duplicates joined with `", "` (as `check-fixtures.mjs`
/// `forwardableHeaders` and a fetch `Headers` see them).
fn header_map<'a>(pairs: impl Iterator<Item = (String, &'a [u8])>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::<String, String>::new();
    for (name, value) in pairs {
        let value = String::from_utf8_lossy(value).into_owned();
        out.entry(name)
            .and_modify(|v| {
                v.push_str(", ");
                v.push_str(&value);
            })
            .or_insert(value);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridging_fixtures() {
    let Some(root) = common::mcpwarp_fixtures() else {
        return;
    };
    let files = common::json_files(&root.join("bridging"));
    let mut run = 0;
    let mut notes = Vec::new();
    for path in &files {
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let fx = common::read_json(path);
        if fx.get("outReset").is_some() {
            notes.push(format!(
                "note: bridging/{name}: skipped, a *_reset fixture applies only to an adapter that streams the request body; this SDK buffers it (§10.3), covered by body_413"
            ));
            continue;
        }
        if fx["mode"] == "target" && !cfg!(feature = "target") {
            notes.push(format!(
                "note: bridging/{name}: skipped, target mode needs the `target` feature"
            ));
            continue;
        }
        run_fixture(&name, &fx).await;
        run += 1;
    }
    for n in &notes {
        println!("{n}");
    }
    println!("bridging {run}/{}", files.len());
    assert!(run >= MIN_RUN, "only {run} bridging fixtures ran");
}

fn expand_in(fx: &Value) -> String {
    let mut s = fx["in"].as_str().unwrap().to_owned();
    if let Some(map) = fx.get("inExpand").and_then(Value::as_object) {
        for (marker, spec) in map {
            let repeat = spec["repeat"].as_str().unwrap();
            let count = usize::try_from(spec["count"].as_u64().unwrap()).unwrap();
            s = s.replace(marker, &repeat.repeat(count));
        }
    }
    s
}

fn state_of(fx: &Value) -> ServeState {
    match fx["sdkState"].as_str().unwrap() {
        "online" => RegistrationState::Registered.into(),
        "disabled" => RegistrationState::Disabled.into(),
        "closing" => RegistrationState::Closing.into(),
        other => panic!("sdkState {other}"),
    }
}

async fn run_fixture(name: &str, fx: &Value) {
    let state = state_of(fx);
    let behaviour = fx["handler"]["behaviour"]
        .as_str()
        .unwrap_or("echo")
        .to_owned();
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = errors.clone();
    let ctx = ServeContext::new(move || state)
        .max_request_body_bytes(fx["limits"]["maxRequestBodyBytes"].as_u64().unwrap())
        .on_error(move |e| sink.lock().unwrap().push(e.to_string()));
    let calls: Calls = Arc::default();

    let (stream, mut peer) = Stream::new_in_memory(1);
    let (serving, local) = match fx["mode"].as_str().unwrap() {
        "handler" => {
            let h = fixture_handler(&behaviour, calls.clone());
            (tokio::spawn(serve_stream(stream, &ctx, &h)), None)
        }
        "target" => serve_target(fx, stream, &ctx, calls.clone()).await,
        other => panic!("{name}: mode {other}"),
    };

    let input = expand_in(fx);
    let want_out = fx["out"].as_str().unwrap();
    let head_end = input.find("\r\n\r\n").map(|i| i + 4);
    let mut raw = Vec::new();
    match head_end {
        Some(end) if fx["sdkState"] != "online" => {
            // 404/503 come right after the head: they must not wait for the body.
            peer.write(&input.as_bytes()[..end]).unwrap();
            while raw.len() < want_out.len() {
                match recv(&mut peer, name).await {
                    Some(PeerEvent::Data(d)) => raw.extend_from_slice(&d),
                    other => panic!("{name}: {other:?} before the refusal was complete"),
                }
            }
            peer.write(&input.as_bytes()[end..]).unwrap();
        }
        _ => peer.write(input.as_bytes()).unwrap(),
    }
    peer.close();

    let mut events = Vec::new();
    loop {
        match recv(&mut peer, name).await {
            Some(PeerEvent::Data(d)) => raw.extend_from_slice(&d),
            Some(e) => events.push(e),
            None => break,
        }
    }
    tokio::time::timeout(Duration::from_secs(15), serving)
        .await
        .unwrap_or_else(|_| panic!("{name}: serve_stream did not finish"))
        .unwrap();
    if let Some(l) = local {
        l.abort();
    }

    let raw = String::from_utf8(raw).unwrap_or_else(|e| panic!("{name}: {e}"));
    assert!(!raw.starts_with("HTTP/1.1 1"), "{name}: a 1xx was sent");
    assert_eq!(
        status_lines(&raw),
        1,
        "{name}: exactly one status line: {raw:?}"
    );
    assert_eq!(
        events,
        [PeerEvent::Close],
        "{name}: the stream ends with CLOSE only"
    );

    let calls = calls.lock().unwrap().clone();
    let addr = local_addr(fx, &calls);
    match fx.get("upstream") {
        Some(Value::Null) => assert!(calls.is_empty(), "{name}: the app must not be called"),
        Some(up) => {
            assert_eq!(calls.len(), 1, "{name}: one app call");
            let want = Upstream::from_fixture(up, addr.as_deref());
            assert_eq!(calls[0], want, "{name}: upstream");
        }
        None => panic!("{name}: no upstream key"),
    }

    let from_app = !fx["upstream"].is_null() && behaviour == "echo";
    let method = input.split(' ').next().unwrap();
    if from_app {
        let got =
            parse_response(&raw, method == "HEAD").unwrap_or_else(|e| panic!("{name}: out: {e}"));
        let want = parse_response(want_out, method == "HEAD").unwrap();
        assert_eq!(got.status_line, want.status_line, "{name}: status line");
        assert_eq!(got.headers, want.headers, "{name}: headers");
        assert_eq!(got.body, want.body, "{name}: body");
    } else {
        assert_eq!(raw, want_out, "{name}: an SDK reply is byte-exact");
    }
    let errors = errors.lock().unwrap();
    if behaviour == "throw" {
        assert_eq!(
            errors.len(),
            1,
            "{name}: the handler error is reported once: {errors:?}"
        );
    } else {
        assert!(errors.is_empty(), "{name}: {errors:?}");
    }
}

async fn recv(peer: &mut PeerHandle, name: &str) -> Option<PeerEvent> {
    tokio::time::timeout(Duration::from_secs(15), peer.recv())
        .await
        .unwrap_or_else(|_| panic!("{name}: timed out waiting for the stream"))
}

/// The fixture's application: `echo` answers `200` with the request's
/// content-type and body and no length; `throw` fails.
fn fixture_handler(
    behaviour: &str,
    calls: Calls,
) -> impl ws_mixer_mcpwarp::bridge::Handler + Clone {
    let throw = match behaviour {
        "echo" => false,
        "throw" => true,
        other => panic!("handler behaviour {other}"),
    };
    handler_fn(move |req: http::Request<RequestBody>| {
        let calls = calls.clone();
        async move {
            let (parts, body) = req.into_parts();
            let body = body.collect().await?.to_bytes();
            let headers = header_map(
                parts
                    .headers
                    .iter()
                    .map(|(n, v)| (n.as_str().to_owned(), v.as_bytes())),
            );
            let content_type = parts.headers.get(http::header::CONTENT_TYPE).cloned();
            calls.lock().unwrap().push(Upstream {
                method: parts.method.to_string(),
                url: parts.uri.to_string(),
                headers,
                body: Some(String::from_utf8_lossy(&body).into_owned()),
            });
            if throw {
                return Err("fixture handler throws".into());
            }
            let mut res = Response::new(Full::new(body));
            if let Some(ct) = content_type {
                res.headers_mut().insert(http::header::CONTENT_TYPE, ct);
            }
            Ok::<_, ws_mixer::BoxError>(res)
        }
    })
}

#[cfg(feature = "target")]
async fn serve_target(
    fx: &Value,
    stream: Stream,
    ctx: &ServeContext,
    calls: Calls,
) -> (
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
) {
    use ws_mixer_mcpwarp::bridge::{DEFAULT_TARGET_TIMEOUT, Target};

    assert_eq!(
        fx["handler"]["behaviour"], "hang",
        "target fixtures describe a hanging server"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let url = fx["target"]
        .as_str()
        .unwrap()
        .replace("127.0.0.1:3000", &addr);
    let timeout = fx["limits"]["targetTimeoutMs"]
        .as_u64()
        .map_or(DEFAULT_TARGET_TIMEOUT, Duration::from_millis);
    let target = Target::new(url.parse().unwrap(), timeout);
    let local = tokio::spawn(hanging_server(listener, addr, calls));
    (
        tokio::spawn(serve_stream(stream, ctx, &target)),
        Some(local),
    )
}

#[cfg(not(feature = "target"))]
async fn serve_target(
    _: &Value,
    _: Stream,
    _: &ServeContext,
    _: Calls,
) -> (
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
) {
    unreachable!("target fixtures are skipped without the `target` feature")
}

/// Records the request it receives, then never answers. Host, framing and
/// the URL are mapped back to what a fetch-style client was handed: the
/// wire's `host` must be the target's, and `content-length` /
/// `transfer-encoding` are the client's framing, not request headers.
#[cfg(feature = "target")]
async fn hanging_server(listener: tokio::net::TcpListener, addr: String, calls: Calls) {
    use tokio::io::AsyncReadExt;

    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 4096];
        let n = sock.read(&mut chunk).await.unwrap();
        assert!(n > 0, "the client closed before the head");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8(buf[..head_end - 4].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let mut start = lines.next().unwrap().split(' ');
    let (method, target) = (
        start.next().unwrap().to_owned(),
        start.next().unwrap().to_owned(),
    );
    let pairs: Vec<(String, String)> = lines
        .map(|l| {
            let (n, v) = l.split_once(':').unwrap();
            (n.to_ascii_lowercase(), v.trim().to_owned())
        })
        .collect();
    let host: Vec<_> = pairs.iter().filter(|(n, _)| n == "host").collect();
    assert_eq!(host.len(), 1);
    assert_eq!(host[0].1, addr, "Host is the target's");
    let length: usize = pairs
        .iter()
        .find(|(n, _)| n == "content-length")
        .map_or(0, |(_, v)| v.parse().unwrap());
    while buf.len() < head_end + length {
        let mut chunk = [0u8; 4096];
        let n = sock.read(&mut chunk).await.unwrap();
        assert!(n > 0, "the client closed inside the body");
        buf.extend_from_slice(&chunk[..n]);
    }
    let headers = header_map(
        pairs
            .iter()
            .filter(|(n, _)| !matches!(n.as_str(), "host" | "content-length" | "transfer-encoding"))
            .map(|(n, v)| (n.clone(), v.as_bytes())),
    );
    calls.lock().unwrap().push(Upstream {
        method,
        url: format!("http://{addr}{target}"),
        headers,
        body: Some(String::from_utf8_lossy(&buf[head_end..head_end + length]).into_owned()),
    });
    // Hang with the connection open until the runner aborts this task.
    std::future::pending::<()>().await;
    drop(sock);
}

/// The local server's address, from the URL it recorded.
fn local_addr(fx: &Value, calls: &[Upstream]) -> Option<String> {
    if fx["mode"] != "target" {
        return None;
    }
    let url = &calls.first()?.url;
    let rest = url.strip_prefix("http://")?;
    Some(rest[..rest.find('/').unwrap_or(rest.len())].to_owned())
}

fn status_lines(raw: &str) -> usize {
    let b = raw.as_bytes();
    (0..b.len())
        .filter(|&i| {
            b[i..].starts_with(b"HTTP/1.")
                && b.len() >= i + 12
                && matches!(b[i + 7], b'0' | b'1')
                && b[i + 8] == b' '
                && b[i + 9..i + 12].iter().all(u8::is_ascii_digit)
        })
        .count()
}

#[derive(Debug)]
struct Parsed {
    status_line: String,
    headers: Vec<(String, String)>,
    body: Bytes,
}

const TOKEN_EXTRA: &[u8] = b"!#$%&'*+.^_`|~-";

/// `check-fixtures.mjs` `parseHttpMessage(text, "response", {headOnly})`:
/// lower-cased header names, CRLF-only head, framing by Content-Length or
/// chunked (strictly decoded), else no body. Headers come back sorted.
fn parse_response(text: &str, head_only: bool) -> Result<Parsed, String> {
    let end = text
        .find("\r\n\r\n")
        .ok_or("no blank line (CRLF CRLF) ends the head")?;
    let head = &text[..end];
    if head.replace("\r\n", "").contains(['\r', '\n']) {
        return Err("head lines must end in CRLF".into());
    }
    let mut lines = head.split("\r\n");
    let start = lines.next().unwrap_or_default().to_owned();
    let ok_start = start.len() > 13
        && start.starts_with("HTTP/1.1 ")
        && start[9..12].bytes().all(|b| b.is_ascii_digit())
        && start.as_bytes()[12] == b' ';
    if !ok_start {
        return Err(format!("not a status line: {start:?}"));
    }
    let mut headers = Vec::new();
    for line in lines {
        let idx = line.find(':').filter(|&i| i > 0);
        let name = idx.map(|i| &line[..i]);
        let valid = name.is_some_and(|n| {
            n.bytes()
                .all(|b| b.is_ascii_alphanumeric() || TOKEN_EXTRA.contains(&b))
        });
        let (Some(i), true) = (idx, valid) else {
            return Err(format!("malformed header line {line:?}"));
        };
        headers.push((
            line[..i].to_ascii_lowercase(),
            line[i + 1..].trim().to_owned(),
        ));
    }
    let get = |n: &str| -> Vec<&str> {
        headers
            .iter()
            .filter(|(k, _)| k == n)
            .map(|(_, v)| v.as_str())
            .collect()
    };
    let rest = &text.as_bytes()[end + 4..];
    let (cl, te) = (get("content-length"), get("transfer-encoding"));
    if !cl.is_empty() && !te.is_empty() {
        return Err("both Content-Length and Transfer-Encoding".into());
    }
    let digits = |v: &str| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit());
    if !cl.is_empty() && (cl.len() != 1 || !digits(cl[0])) {
        return Err(format!("malformed Content-Length {cl:?}"));
    }
    if !te.is_empty() && (te.len() != 1 || !te[0].eq_ignore_ascii_case("chunked")) {
        return Err(format!("unsupported Transfer-Encoding {te:?}"));
    }
    let body = if head_only {
        if !rest.is_empty() {
            return Err(format!(
                "{} bytes after a head that must stand alone",
                rest.len()
            ));
        }
        Bytes::new()
    } else if let Some(cl) = cl.first() {
        let n: usize = cl.parse().map_err(|e| format!("{e}"))?;
        if rest.len() != n {
            return Err(format!(
                "Content-Length {n} but the body is {} bytes",
                rest.len()
            ));
        }
        Bytes::copy_from_slice(rest)
    } else if !te.is_empty() {
        decode_chunked(rest)?
    } else {
        if !rest.is_empty() {
            return Err(format!(
                "{} body bytes without Content-Length or Transfer-Encoding",
                rest.len()
            ));
        }
        Bytes::new()
    };
    headers.sort();
    Ok(Parsed {
        status_line: start,
        headers,
        body,
    })
}

/// `check-fixtures.mjs` `decodeChunked`.
fn decode_chunked(buf: &[u8]) -> Result<Bytes, String> {
    let mut out = Vec::new();
    let mut pos = 0;
    loop {
        let eol = buf[pos..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|i| i + pos)
            .ok_or("chunk-size line without CRLF")?;
        let line = String::from_utf8_lossy(&buf[pos..eol]);
        let size_text = line.split(';').next().unwrap_or_default();
        if size_text.is_empty() || !size_text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("malformed chunk size {size_text:?}"));
        }
        let size = usize::from_str_radix(size_text, 16).map_err(|e| e.to_string())?;
        pos = eol + 2;
        if size == 0 {
            if &buf[pos..] != b"\r\n" {
                return Err("last chunk not followed by exactly CRLF".into());
            }
            return Ok(out.into());
        }
        if pos + size + 2 > buf.len() {
            return Err("chunk shorter than its size".into());
        }
        out.extend_from_slice(&buf[pos..pos + size]);
        pos += size;
        if &buf[pos..pos + 2] != b"\r\n" {
            return Err("missing CRLF after chunk data".into());
        }
        pos += 2;
    }
}

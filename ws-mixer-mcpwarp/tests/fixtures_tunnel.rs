//! `spec/fixtures/mcpwarp/tunnel` (MCPWARP.md §10.4, the fixture README's
//! runner contract), against the real [`Tunnel`] -> `expose()` path on the
//! `test-util` fake edge, each fixture on its own paused-clock runtime.
//!
//! The fake edge stands in for the ws-mixer client, so this runner plays the
//! client's supervisor (the README's "runner that fakes the ws-mixer client"):
//! dials are numbered over the fixture (the `conn` ordinal is the
//! `ConnId`), a failed dial or lost connection is a `Disconnected` then a
//! `Redial` with WIRE.md §2.9's delay at `rng = 0.5` (the drain jitter is
//! 1000 ms), and the scheduled dial starts when `retry`/`tick` move the clock
//! past it. Disconnect reasons carry the fixture's `{fatal, wsCode,
//! errorName: code}` verbatim; `dial_failed{httpStatus}` is `{phase: dial,
//! httpStatus, fatal}` with 401/403/404 fatal. The edge answers `unregister`
//! with `unregistered` unless the connection is held.
//!
//! Checked as `check-fixtures.mjs` checks the corpus (event shapes against
//! the README's Events table, legal `(state, code)` pairs, `lastError`
//! constants, `url` only while online, repeats only from `subscribe`), then
//! run: every snapshot the primary listener got (plus each `subscribe`'s)
//! equals `expect` as JSON values, every one of them legal, the dial count
//! equals `expectDials`, each `retry` is matched by the last `on_retry` call
//! since the previous `retry`/`tick` (its reason, delay bound, the drain's
//! attempt 0, and `OVERLOADED`'s attempt 1, 2, ... back to 1 after any other
//! reply: a `registered`, a `register_error`, or an `app_error` other than
//! `OVERLOADED` and `CONNECTION_LIMIT` answering the frame, i.e. not during
//! an `OVERLOADED` pause), and every `configure`/`stop`/`dispose` future has
//! finished.
//!
//! `07_invalid_config`'s `kind: "ftp"` is not representable as a
//! `ServiceKind`: that event and the two snapshots it produces are skipped,
//! as the fixture allows.
//!
//! A fixture with a `token_provider` event passes each token as a provider
//! (one per token value) that pops the queued results, and the runner's
//! supervisor calls the client's provider on every dial attempt, as the
//! transport does: a failed call is a `Disconnected` with no dial, and the
//! first rejected token before `welcome` is a non-fatal `Disconnected`, then
//! a `Redial{TokenRefresh}` with delay 0 (CLIENT-SDK.md's Rejected token
//! row). `disconnect.flush_after` ops go out right after the `Disconnected`
//! on the same channel, so post-disconnect delivery is supported. Group
//! fixtures (`ms_*`, `config.services`) are skipped: they are host guidance
//! (MCPWARP.md §10.6), not SDK conformance. Skips print `skip: tunnel/<file>: <why>`, never
//! `tunnel: skipped` (conformance.yml fails on that).
#![cfg_attr(not(feature = "test-util"), allow(dead_code, unused_imports))]

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};

/// A floor on the fixtures run for the pinned spec (v0.8.0 has 58: 44
/// single-service ones, 01-32 and 34-45, all run, 07 without its
/// unrepresentable config; the 14 `ms_*` host-guidance fixtures are skipped).
const MIN_RUN: usize = 44;

/// The `runnerRequirement`s this runner meets.
const SUPPORTED_REQUIREMENTS: [&str; 1] = ["post-disconnect-delivery"];

/// The edge messages a `disconnect.flush_after` may carry.
const FLUSHABLE: [&str; 6] = [
    "registered",
    "register_error",
    "app_error",
    "disabled",
    "enabled",
    "conflict",
];

/// Why this runner does not run `fx`, if it does not.
fn skip_reason(fx: &Value) -> Option<String> {
    if fx["config"].get("services").is_some() {
        return Some("host guidance (MCPWARP.md §10.6)".into());
    }
    match fx.get("runnerRequirement").and_then(Value::as_str) {
        Some(r) if !SUPPORTED_REQUIREMENTS.contains(&r) => Some(format!("runnerRequirement {r}")),
        _ => None,
    }
}

#[cfg(not(feature = "test-util"))]
#[test]
fn tunnel_fixtures() {
    println!("tunnel: skipped, the runner needs `--features test-util`");
}

#[cfg(feature = "test-util")]
#[test]
fn tunnel_fixtures() {
    let Some(root) = common::mcpwarp_fixtures() else {
        return;
    };
    let dir = root.join("tunnel");
    let vocab = parse_vocabulary(&std::fs::read_to_string(dir.join("README.md")).unwrap());
    assert!(!vocab.is_empty(), "tunnel/README.md: no Events table");
    let files = common::json_files(&dir);
    let mut failures = Vec::new();
    let mut skipped = 0;
    for file in &files {
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        let fx = common::read_json(file);
        if let Some(why) = skip_reason(&fx) {
            println!("skip: tunnel/{name}: {why}");
            skipped += 1;
            continue;
        }
        let result = check_fixture(&fx, &vocab).and_then(|()| {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(run::run(&fx))
        });
        match result {
            Ok(notes) => {
                for n in notes {
                    println!("note: tunnel/{name}: {n}");
                }
            }
            Err(e) => failures.push(format!("tunnel/{name}: {e}")),
        }
    }
    let passed = files.len() - skipped - failures.len();
    println!("tunnel {passed}/{} ({skipped} skipped)", files.len());
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    assert!(passed >= MIN_RUN, "only {passed} tunnel fixtures ran");
}

// --- check-fixtures.mjs: the corpus itself ------------------------------------

/// `name -> Some((required, optional))` for an `{a, b?}` payload, `None` for
/// any other (a partial config, the burst array).
type Vocabulary = BTreeMap<String, Option<(Vec<String>, Vec<String>)>>;

/// `parseTunnelVocabulary`: the README's Events table.
fn parse_vocabulary(markdown: &str) -> Vocabulary {
    let mut vocab = Vocabulary::new();
    let Some(section) = markdown.split("\n## Events").nth(1) else {
        return vocab;
    };
    let section = section.split("\n## ").next().unwrap_or_default();
    for line in section.lines() {
        let Some(rest) = line.strip_prefix("| `") else {
            continue;
        };
        let Some((name, rest)) = rest.split_once("` | ") else {
            continue;
        };
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
            continue;
        }
        let Some((payload, _)) = rest.split_once(" | ") else {
            continue;
        };
        let payload = payload.replace('`', "");
        let payload = payload.trim();
        if !payload.starts_with('{') {
            vocab.insert(name.into(), None);
            continue;
        }
        let inner = &payload[1..payload.rfind('}').unwrap_or(payload.len())];
        let inner = strip_groups(inner);
        let keys: Vec<String> = inner
            .split(',')
            .map(|k| k.split(':').next().unwrap_or_default().trim().to_owned())
            .filter(|k| !k.is_empty())
            .collect();
        let required = keys.iter().filter(|k| !k.ends_with('?')).cloned().collect();
        let optional = keys
            .iter()
            .filter_map(|k| k.strip_suffix('?').map(str::to_owned))
            .collect();
        vocab.insert(name.into(), Some((required, optional)));
    }
    vocab
}

/// Removes `[...]` and `{...}` groups, innermost first, until none is left.
fn strip_groups(s: &str) -> String {
    let mut s = s.to_owned();
    loop {
        let mut open: Option<(usize, char)> = None;
        let mut cut = None;
        for (i, c) in s.char_indices() {
            match c {
                '[' | '{' => open = Some((i, c)),
                ']' | '}' => {
                    if let Some((o, oc)) = open {
                        if (oc == '[' && c == ']') || (oc == '{' && c == '}') {
                            cut = Some((o, i));
                            break;
                        }
                    }
                    open = None;
                }
                _ => {}
            }
        }
        match cut {
            Some((a, b)) => s.replace_range(a..=b, ""),
            None => return s,
        }
    }
}

const STATES: [&str; 6] = [
    "off",
    "connecting",
    "online",
    "disabled",
    "conflict",
    "error",
];

fn legal_codes(state: &str) -> &'static [Option<&'static str>] {
    match state {
        "off" | "online" => &[None],
        "connecting" => &[
            None,
            Some("DISCONNECTED"),
            Some("RETRYING"),
            Some("INTERNAL"),
        ],
        "disabled" => &[Some("DISABLED")],
        "conflict" => &[Some("TAKEN_OVER")],
        "error" => &[
            Some("UNAUTHORIZED"),
            Some("INVALID_CONFIG"),
            Some("INVALID_NAME"),
            Some("USERNAME_REQUIRED"),
            Some("QUOTA_EXCEEDED"),
            Some("CONFLICT"),
            Some("UNSUPPORTED_VERSION"),
            Some("DISCONNECTED_FATAL"),
            Some("INTERNAL"),
        ],
        _ => &[],
    }
}

fn last_errors(code: &str) -> &'static [&'static str] {
    use ws_mixer_mcpwarp::last_error as L;
    match code {
        "RETRYING" => &[L::RETRY_DIAL, L::RETRY_REGISTER],
        "DISCONNECTED" => &[L::DISCONNECTED],
        "TAKEN_OVER" => &[L::TAKEN_OVER],
        "DISABLED" => &[L::DISABLED],
        "UNAUTHORIZED" => &[L::TOKEN_REJECTED, L::TOKEN_FORBIDDEN],
        "INVALID_NAME" => &[L::INVALID_NAME],
        "INVALID_CONFIG" => &[L::INVALID_CONFIG, L::TOKEN_NOT_SET, L::NAME_NOT_SET],
        "USERNAME_REQUIRED" => &[L::USERNAME_REQUIRED],
        "QUOTA_EXCEEDED" => &[L::QUOTA_EXCEEDED],
        "CONFLICT" => &[L::CONFLICT],
        "UNSUPPORTED_VERSION" => &[L::UNSUPPORTED_VERSION],
        "DISCONNECTED_FATAL" => &[L::DISCONNECTED_FATAL],
        "INTERNAL" => &[L::INTERNAL],
        _ => &[],
    }
}

/// One snapshot, as the checker checks `expect` entries; `tokens` are the
/// fixture's non-blank trimmed tokens.
fn check_snapshot(snap: &Value, tokens: &[String]) -> Result<(), String> {
    let o = snap
        .as_object()
        .ok_or_else(|| format!("{snap}: a snapshot must be an object"))?;
    for k in o.keys() {
        if !["state", "code", "lastError", "url"].contains(&k.as_str()) {
            return Err(format!("{snap}: unknown TunnelStatus key {k:?}"));
        }
    }
    let state = o.get("state").and_then(Value::as_str).unwrap_or_default();
    if !STATES.contains(&state) {
        return Err(format!("{snap}: unknown state"));
    }
    let code = match o.get("code") {
        None => None,
        Some(Value::String(c)) => Some(c.as_str()),
        Some(_) => return Err(format!("{snap}: code must be a string")),
    };
    if !legal_codes(state).contains(&code) {
        return Err(format!("{snap}: not a legal (state, code) pair"));
    }
    match (code, o.get("lastError")) {
        (None, None) => {}
        (Some(code), Some(Value::String(le))) => {
            if !last_errors(code).contains(&le.as_str()) {
                return Err(format!(
                    "{snap}: lastError is not a §6.6 constant for {code}"
                ));
            }
            for t in tokens {
                let leaks = if t.chars().count() < 4 {
                    le == t
                } else {
                    le.contains(t.as_str())
                };
                if leaks {
                    return Err(format!("{snap}: lastError contains the token"));
                }
            }
        }
        _ => {
            return Err(format!(
                "{snap}: lastError must be present exactly when a code is"
            ));
        }
    }
    let url = o.get("url");
    if state == "online" {
        if !url.and_then(Value::as_str).is_some_and(|u| !u.is_empty()) {
            return Err(format!("{snap}: online carries a non-empty url"));
        }
    } else if url.is_some() {
        return Err(format!("{snap}: url is only set while online"));
    }
    Ok(())
}

fn check_event(
    ev: &Value,
    vocab: &Vocabulary,
    dials: Option<u64>,
    in_burst: bool,
) -> Result<(), String> {
    let o = ev
        .as_object()
        .filter(|o| o.len() == 1)
        .ok_or_else(|| format!("{ev}: an event is an object with exactly one key"))?;
    let (name, payload) = o.iter().next().unwrap();
    let spec = vocab
        .get(name)
        .ok_or_else(|| format!("{ev}: unknown event {name:?}"))?;
    if in_burst && name != "configure" && name != "stop" {
        return Err(format!("{ev}: a burst holds only configure / stop events"));
    }
    if name == "burst" {
        let items = payload
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or_else(|| format!("{ev}: burst is a non-empty array"))?;
        return items
            .iter()
            .try_for_each(|b| check_event(b, vocab, dials, true));
    }
    let p = payload
        .as_object()
        .ok_or_else(|| format!("{ev}: the payload must be an object"))?;
    let Some((required, optional)) = spec else {
        return Ok(());
    };
    for k in p.keys() {
        if !required.contains(k) && !optional.contains(k) {
            return Err(format!("{ev}: unknown payload key {k:?}"));
        }
    }
    for k in required {
        if !p.contains_key(k) {
            return Err(format!("{ev}: missing payload key {k:?}"));
        }
    }
    if let Some(conn) = p.get("conn") {
        let ok = conn
            .as_u64()
            .is_some_and(|c| c >= 1 && dials.is_none_or(|d| c <= d));
        if !ok {
            return Err(format!("{ev}: conn must be a dial ordinal 1..expectDials"));
        }
    }
    if name == "dial_failed" {
        if let Some(s) = p.get("httpStatus") {
            if !s.as_u64().is_some_and(|s| (200..=599).contains(&s)) {
                return Err(format!("{ev}: httpStatus must be an HTTP status 200..599"));
            }
        }
    }
    if let Some(flush) = p.get("flush_after") {
        let items = flush
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or_else(|| format!("{ev}: flush_after is a non-empty array"))?;
        for item in items {
            check_event(item, vocab, dials, false)?;
            let (kind, payload) = item.as_object().unwrap().iter().next().unwrap();
            if !FLUSHABLE.contains(&kind.as_str()) || payload.get("conn").is_some() {
                return Err(format!(
                    "{ev}: flush_after holds edge messages without conn"
                ));
            }
        }
    }
    if name == "token_provider"
        && !["ok", "unavailable", "error"].contains(&p["result"].as_str().unwrap_or_default())
    {
        return Err(format!("{ev}: result is ok, unavailable or error"));
    }
    Ok(())
}

fn configs(events: &[Value], out: &mut Vec<Value>) {
    for ev in events {
        if let Some(c) = ev.get("configure").filter(|c| c.is_object()) {
            out.push(c.clone());
        }
        if let Some(b) = ev.get("burst").and_then(Value::as_array) {
            configs(b, out);
        }
    }
}

/// The fixture's non-blank tokens, trimmed.
fn tokens(fx: &Value) -> Vec<String> {
    let mut all = vec![fx["config"].clone()];
    configs(fx["events"].as_array().map_or(&[], Vec::as_slice), &mut all);
    all.iter()
        .filter_map(|c| c["token"].as_str())
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
        .collect()
}

fn check_fixture(fx: &Value, vocab: &Vocabulary) -> Result<(), String> {
    if !fx["description"].as_str().is_some_and(|d| !d.is_empty()) {
        return Err("description must be a non-empty string".into());
    }
    if !fx["config"].is_object() {
        return Err("config must be an object".into());
    }
    let events = fx["events"].as_array().ok_or("events must be an array")?;
    let expect = fx["expect"]
        .as_array()
        .filter(|e| !e.is_empty())
        .ok_or("expect must be a non-empty array")?;
    let dials = match fx.get("expectDials") {
        None => None,
        Some(d) => Some(
            d.as_u64()
                .ok_or("expectDials must be a non-negative integer")?,
        ),
    };
    for ev in events {
        check_event(ev, vocab, dials, false)?;
    }
    let flushes = events.iter().any(|e| {
        e.get("disconnect")
            .is_some_and(|d| d.get("flush_after").is_some())
    });
    if flushes && fx["runnerRequirement"] != "post-disconnect-delivery" {
        return Err("a flush_after needs runnerRequirement post-disconnect-delivery".into());
    }
    let subscribes = events
        .iter()
        .filter(|e| e.get("subscribe").is_some())
        .count();
    let repeats = expect.windows(2).filter(|w| w[0] == w[1]).count();
    if repeats > subscribes {
        return Err(format!(
            "{repeats} snapshot(s) repeat the one before, but only a subscribe event ({subscribes} here) delivers an unchanged status"
        ));
    }
    let tokens = tokens(fx);
    expect
        .iter()
        .try_for_each(|s| check_snapshot(s, &tokens).map_err(|e| format!("expect: {e}")))
}

// --- the run ------------------------------------------------------------------

#[cfg(feature = "test-util")]
mod run {
    use super::*;

    use futures_util::FutureExt;
    use tokio::time::Instant;
    use ws_mixer::{
        CloseClass, ConnId, DisconnectReason, DrainNotice, DrainReason, Phase, RedialCause,
        RedialNotice, Token, TokenError, classify_close,
    };
    use ws_mixer_mcpwarp::bridge::handler_fn;
    use ws_mixer_mcpwarp::test_util::{FakeEdge, settle};
    use ws_mixer_mcpwarp::{
        DEFAULT_DISPOSE_TIMEOUT, RetryInfo, Tunnel, TunnelConfig, TunnelOptions, TunnelStatus,
    };

    const INTEGRATION: &str = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10";

    /// The client's default policy (base 1 s, cap 60 s, stable after 10 s).
    const BASE: Duration = Duration::from_secs(1);
    const CAP: Duration = Duration::from_secs(60);
    const STABLE_AFTER: Duration = Duration::from_secs(10);
    /// `min(2 s, cap)` at `rng = 0.5`.
    const DRAIN_DELAY: Duration = Duration::from_millis(1000);

    #[derive(Debug, Default)]
    struct Dial {
        client: usize,
        upgraded: bool,
        welcomed: bool,
        ended: bool,
        held: bool,
    }

    /// One ws-mixer client, as its supervisor sees it.
    #[derive(Debug, Default)]
    struct Client {
        /// `close` was called.
        closed: bool,
        /// Gave up (a fatal disconnect).
        dead: bool,
        /// The name of the `register` it sent.
        name: Option<String>,
        /// The welcomed, not drained connection.
        active: Option<usize>,
        attempt: u32,
        keepalive_used: bool,
        stable: Option<(usize, Instant)>,
        /// The scheduled dial.
        timer: Option<Instant>,
        /// The token provider of the config it runs, called per dial.
        provider: Option<Token>,
        /// The one refresh after a rejected token was used; re-armed by a
        /// stable connection.
        refresh_spent: bool,
    }

    impl Client {
        fn running(&self) -> bool {
            !self.closed && !self.dead
        }
    }

    enum Plan {
        Backoff,
        GoingAway,
        StartAtCap,
        Immediate,
    }

    struct Runner<'a> {
        fx: &'a Value,
        edge: FakeEdge,
        tunnel: Tunnel,
        dials: Vec<Dial>,
        clients: Vec<Client>,
        seen_connects: usize,
        seen_closes: usize,
        seen_sent: usize,
        server_ids: HashMap<String, String>,
        retries: Arc<Mutex<Vec<RetryInfo>>>,
        snapshots: Arc<Mutex<Vec<Value>>>,
        pending: Arc<AtomicUsize>,
        expect: Vec<Value>,
        notes: Vec<String>,
        /// The last `OVERLOADED` re-send's attempt; 0 after `registered`.
        overloaded: u32,
        /// An `OVERLOADED` `app_error` whose `retry` step has not come yet:
        /// the dropped frame is not on the wire, so an `app_error` answers
        /// nothing.
        overload_paused: bool,
        /// One provider per token value, in a `token_provider` fixture.
        providers: Option<HashMap<String, Token>>,
        /// The results of the next provider calls, oldest first.
        results: Arc<Mutex<VecDeque<String>>>,
        /// The token of the last `configure` call.
        last_token: Option<String>,
        /// The last drain's `retry_after_ms`, a floor for its `retry`.
        drain_floor: Option<Duration>,
    }

    /// A provider returning `token`, or failing per the queued result.
    fn provider(token: String, results: Arc<Mutex<VecDeque<String>>>) -> Token {
        Token::provider(move || {
            let next = results.lock().unwrap().pop_front();
            let token = token.clone();
            async move {
                match next.as_deref() {
                    None | Some("ok") => Ok(token),
                    Some("unavailable") => Err(TokenError::unavailable("fixture: unavailable")),
                    Some(_) => Err(TokenError::fatal("fixture: provider error")),
                }
            }
        })
    }

    fn conn(d: usize) -> ConnId {
        ConnId::new_for_test(d as u64 + 1)
    }

    fn snapshot(s: &TunnelStatus) -> Value {
        serde_json::to_value(s).unwrap()
    }

    pub(super) async fn run(fx: &Value) -> Result<Vec<String>, String> {
        let edge = FakeEdge::new();
        let retries = Arc::new(Mutex::new(Vec::new()));
        let rec = retries.clone();
        let options = TunnelOptions::builder()
            .integration(INTEGRATION)
            .handler(handler_fn(|_req| async {
                Ok(http::Response::new(http_body_util::Full::new(
                    bytes::Bytes::from_static(b"ok"),
                )))
            }))
            .on_retry(move |info| rec.lock().unwrap().push(info.clone()))
            .fake_edge(&edge)
            .build()
            .map_err(|e| e.to_string())?;
        let tunnel = Tunnel::new(options).map_err(|e| e.to_string())?;
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        let primary = snapshots.clone();
        let _subscription = tunnel.subscribe(move |s| primary.lock().unwrap().push(snapshot(s)));
        let mut r = Runner {
            fx,
            edge,
            tunnel,
            dials: Vec::new(),
            clients: Vec::new(),
            seen_connects: 0,
            seen_closes: 0,
            seen_sent: 0,
            server_ids: HashMap::new(),
            retries,
            snapshots,
            pending: Arc::new(AtomicUsize::new(0)),
            expect: fx["expect"].as_array().unwrap().clone(),
            notes: Vec::new(),
            overloaded: 0,
            overload_paused: false,
            providers: fx["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e.get("token_provider").is_some())
                .then(HashMap::new),
            results: Arc::new(Mutex::new(VecDeque::new())),
            last_token: None,
            drain_floor: None,
        };
        for (i, ev) in fx["events"].as_array().unwrap().iter().enumerate() {
            r.event(ev)
                .await
                .map_err(|e| format!("events[{i}] {ev}: {e}"))?;
            r.quiesce().await;
        }
        let got = r.snapshots.lock().unwrap().clone();
        let pending = r.pending.load(Ordering::SeqCst);
        let dials = r.dials.len();
        // Leave nothing running: as the JS runner does.
        if let Ok(f) = r.tunnel.dispose(Duration::from_millis(1)) {
            tokio::spawn(f);
        }
        tokio::time::sleep(Duration::from_secs(60)).await;

        if got != r.expect {
            return Err(format!(
                "snapshots differ\n  got:    {}\n  expect: {}",
                Value::Array(got),
                Value::Array(r.expect)
            ));
        }
        let tokens = tokens(fx);
        for s in &got {
            check_snapshot(s, &tokens).map_err(|e| format!("produced: {e}"))?;
        }
        if let Some(want) = fx["expectDials"].as_u64() {
            if dials as u64 != want {
                return Err(format!("dials: got {dials}, expect {want}"));
            }
        }
        if pending != 0 {
            return Err(format!(
                "{pending} configure/stop/dispose future(s) still pending at the end"
            ));
        }
        Ok(r.notes)
    }

    impl Runner<'_> {
        // --- the host side ------------------------------------------------------

        fn track(&self, f: impl std::future::Future<Output = ()> + Send + 'static) {
            let pending = self.pending.clone();
            pending.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                f.await;
                pending.fetch_sub(1, Ordering::SeqCst);
            });
        }

        /// `merged()`: shallow over the base config, `null` = absent.
        /// `None` for a config this SDK cannot represent. In a
        /// `token_provider` fixture a non-blank token becomes its provider.
        fn merged(&mut self, over: &Value) -> Option<TunnelConfig> {
            let mut out: Map<String, Value> = self.fx["config"].as_object().unwrap().clone();
            for (k, v) in over.as_object().unwrap() {
                if v.is_null() {
                    out.remove(k);
                } else {
                    out.insert(k.clone(), v.clone());
                }
            }
            let mut config: TunnelConfig = serde_json::from_value(Value::Object(out)).ok()?;
            self.last_token.clone_from(&config.token);
            if let (Some(providers), Some(token)) = (&mut self.providers, &config.token) {
                if !token.trim().is_empty() {
                    let results = self.results.clone();
                    let p = providers
                        .entry(token.clone())
                        .or_insert_with(|| provider(token.clone(), results));
                    config.token_provider = Some(p.clone());
                    config.token = None;
                }
            }
            Some(config)
        }

        fn call(&mut self, ev: &Value) -> Result<(), String> {
            let (kind, payload) = ev.as_object().unwrap().iter().next().unwrap();
            match kind.as_str() {
                "configure" => match self.merged(payload) {
                    Some(config) => self.track(self.tunnel.configure(config)),
                    None => {
                        // Unrepresentable (07's kind "ftp"): skip it and the
                        // two snapshots it produces.
                        let at = self.snapshots.lock().unwrap().len();
                        if self.expect.len() < at + 2 {
                            return Err("no snapshots left to skip".into());
                        }
                        self.expect.drain(at..at + 2);
                        self.notes.push(format!(
                            "configure {payload} is not representable; skipped with its two snapshots"
                        ));
                    }
                },
                "stop" => self.track(self.tunnel.stop()),
                other => return Err(format!("burst: unexpected {other}")),
            }
            Ok(())
        }

        async fn event(&mut self, ev: &Value) -> Result<(), String> {
            let (kind, payload) = ev.as_object().unwrap().iter().next().unwrap();
            let conn_of = |p: &Value| p["conn"].as_u64().map(|c| c as usize - 1);
            match kind.as_str() {
                "configure" | "stop" => self.call(ev)?,
                "burst" => {
                    for inner in payload.as_array().unwrap() {
                        self.call(inner)?;
                    }
                }
                "dispose" => {
                    let timeout = payload["timeoutMs"]
                        .as_u64()
                        .map_or(DEFAULT_DISPOSE_TIMEOUT, Duration::from_millis);
                    let f = self.tunnel.dispose(timeout).map_err(|e| e.to_string())?;
                    self.track(f);
                }
                "subscribe" => {
                    let snapshots = self.snapshots.clone();
                    drop(
                        self.tunnel
                            .subscribe(move |s| snapshots.lock().unwrap().push(snapshot(s))),
                    );
                }
                "connected" => {
                    let d = self.dial(conn_of(payload))?;
                    self.dials[d].upgraded = true;
                }
                "dial_failed" => {
                    let d = self.dial(conn_of(payload))?;
                    let status = payload["httpStatus"].as_u64().map(|s| s as u16);
                    self.dial_failed(d, status);
                }
                "welcome" => {
                    let d = self.dial(conn_of(payload))?;
                    self.welcome(d);
                }
                "registered" | "register_error" | "app_error" | "disabled" | "enabled"
                | "conflict" => {
                    let d = self.dial(conn_of(payload))?;
                    self.edge_message(kind, payload, d);
                }
                "token_provider" => {
                    let result = payload["result"].as_str().unwrap().to_owned();
                    self.results.lock().unwrap().push_back(result);
                }
                "disconnect" => {
                    let d = self.dial(conn_of(payload))?;
                    let fatal = payload["fatal"]
                        .as_bool()
                        .ok_or("fatal must be a boolean")?;
                    let code = payload["code"].as_str().map(str::to_owned);
                    let ws_code = payload["wsCode"].as_u64().map(|c| c as u32);
                    self.disconnect(d, fatal, code, ws_code);
                    // Received before the end, delivered after it (§2.7
                    // Ended connection): the same channel keeps the order.
                    for item in payload["flush_after"].as_array().into_iter().flatten() {
                        let (kind, payload) = item.as_object().unwrap().iter().next().unwrap();
                        self.edge_message(kind, payload, d);
                    }
                }
                "drain" => {
                    let d = self.dial(conn_of(payload))?;
                    let reason = payload["reason"].as_str().unwrap_or("rollout").to_owned();
                    let retry_after = payload["retry_after_ms"]
                        .as_u64()
                        .map(Duration::from_millis);
                    self.drain(d, &reason, retry_after);
                }
                "hold" => {
                    // Modelled as "no `unregistered` answer" (the 2 s
                    // unregister wait); the client's drain grace (up to 5 s
                    // with open streams) is not: the fake's close completes
                    // at once.
                    let d = self.dial(conn_of(payload))?;
                    self.dials[d].held = true;
                }
                "retry" => {
                    let max = Duration::from_millis(payload["delayMs"].as_u64().unwrap());
                    let cause = payload["cause"].as_str().unwrap();
                    // README: the last call since the previous retry/tick;
                    // an earlier one may have been cancelled.
                    let pending = std::mem::take(&mut *self.retries.lock().unwrap());
                    let Some(last) = pending.last() else {
                        return Err(format!("retry ({cause}): no retry was scheduled"));
                    };
                    match cause {
                        "drain" if last.attempt != 0 => {
                            return Err(format!("retry (drain): attempt {} != 0", last.attempt));
                        }
                        "overloaded" => {
                            let want = self.overloaded + 1;
                            if last.attempt != want {
                                return Err(format!(
                                    "retry (overloaded): attempt {} != {want}",
                                    last.attempt
                                ));
                            }
                            self.overloaded = want;
                            self.overload_paused = false;
                        }
                        _ => {}
                    }
                    if last.delay > max {
                        return Err(format!("retry ({cause}): delay {:?} > {max:?}", last.delay));
                    }
                    if matches!(cause, "register" | "overloaded") && last.delay != max {
                        return Err(format!(
                            "retry ({cause}): delay {:?} != {max:?}",
                            last.delay
                        ));
                    }
                    // The fake supervisor set this delay itself: this only
                    // proves mcpwarp forwards it. The transport honouring
                    // retry_after_ms is ws-mixer's
                    // `drain_honours_retry_after_and_ignores_a_repeat`.
                    if cause == "drain" {
                        if let Some(floor) = self.drain_floor.take() {
                            if last.delay < floor {
                                return Err(format!(
                                    "retry (drain): delay {:?} < retry_after {floor:?}",
                                    last.delay
                                ));
                            }
                        }
                    }
                    let r = last.reason.as_str();
                    let ok = match cause {
                        "drain" => r == "drain",
                        "register" => {
                            r.starts_with("register failed: ")
                                && !r.starts_with("register failed: OVERLOADED: ")
                        }
                        "overloaded" => r.starts_with("register failed: OVERLOADED: "),
                        "token-refresh" => {
                            r == "token-refresh" && last.attempt == 1 && last.delay.is_zero()
                        }
                        "dial" | "lost" => {
                            r != "drain"
                                && r != "token-refresh"
                                && !r.starts_with("register failed")
                        }
                        other => return Err(format!("unknown retry cause {other}")),
                    };
                    if !ok {
                        return Err(format!("retry ({cause}) fired {last:?}"));
                    }
                    let delay = last.delay;
                    self.advance(delay).await;
                }
                "tick" => {
                    self.retries.lock().unwrap().clear();
                    let ms = payload["ms"].as_u64().unwrap();
                    self.advance(Duration::from_millis(ms)).await;
                }
                other => return Err(format!("unknown fixture event {other}")),
            }
            Ok(())
        }

        // --- the edge -----------------------------------------------------------

        /// The dial `conn` addresses, the most recent one by default.
        fn dial(&self, conn: Option<usize>) -> Result<usize, String> {
            let d = conn.unwrap_or(self.dials.len().wrapping_sub(1));
            if d < self.dials.len() {
                Ok(d)
            } else {
                Err(format!("no dial #{}", d.wrapping_add(1)))
            }
        }

        fn name_of(&self, d: usize) -> String {
            self.clients[self.dials[d].client]
                .name
                .clone()
                .unwrap_or_else(|| self.fx["config"]["name"].as_str().unwrap().to_owned())
        }

        fn server_id(&self, d: usize) -> String {
            let name = self.name_of(d);
            self.server_ids
                .get(&name)
                .cloned()
                .unwrap_or_else(|| format!("srv_{name}"))
        }

        fn app(&self, d: usize, op: &str, fields: Value) {
            self.edge.app(d as u64 + 1, FakeEdge::envelope(op, fields));
        }

        /// One of the [`FLUSHABLE`] edge messages on dial `d`.
        fn edge_message(&mut self, kind: &str, payload: &Value, d: usize) {
            let name = self.name_of(d);
            match kind {
                "registered" => {
                    // README: the OVERLOADED attempt is back to 1 after it.
                    self.overloaded = 0;
                    let mut services = Vec::new();
                    for svc in payload["services"].as_array().unwrap() {
                        if let Some(id) = svc["id"].as_str() {
                            self.server_ids.insert(name.clone(), id.to_owned());
                        }
                        let mut entry = Map::new();
                        entry.insert("name".into(), json!(name));
                        entry.extend(svc.as_object().unwrap().clone());
                        services.push(Value::Object(entry));
                    }
                    self.app(d, "registered", json!({ "services": services }));
                }
                "register_error" => {
                    self.overloaded = 0;
                    let mut entry = Map::new();
                    entry.insert("name".into(), json!(name));
                    entry.insert("code".into(), payload["code"].clone());
                    if let Some(m) = payload.get("message") {
                        entry.insert("message".into(), m.clone());
                    }
                    if let Some(id) = payload.get("id") {
                        self.server_ids
                            .insert(name.clone(), id.as_str().unwrap().to_owned());
                        entry.insert("id".into(), id.clone());
                    }
                    self.app(d, "registered", json!({ "errors": [entry] }));
                }
                "app_error" => {
                    match payload["code"].as_str() {
                        Some("OVERLOADED") => self.overload_paused = true,
                        Some("CONNECTION_LIMIT") => {}
                        _ if !self.overload_paused => self.overloaded = 0,
                        _ => {}
                    }
                    let mut fields = json!({ "code": payload["code"] });
                    if let Some(m) = payload.get("message") {
                        fields["message"] = m.clone();
                    }
                    self.app(d, "error", fields);
                }
                "disabled" => {
                    // `id`: another (maybe unknown) server id, never adopted
                    // as the agent's.
                    let id = payload
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| json!(self.server_id(d)));
                    let mut fields = json!({ "id": id });
                    if let Some(r) = payload.get("reason") {
                        fields["reason"] = r.clone();
                    }
                    self.app(d, "disable", fields);
                }
                "enabled" => {
                    let fields = json!({ "id": self.server_id(d), "name": name });
                    self.app(d, "enable", fields);
                }
                "conflict" => {
                    let mut fields = json!({ "id": self.server_id(d), "name": name });
                    if let Some(r) = payload.get("reason") {
                        fields["reason"] = r.clone();
                    }
                    self.app(d, "conflict", fields);
                }
                other => unreachable!("not an edge message: {other}"),
            }
        }

        // --- the client's supervisor --------------------------------------------

        /// A dial attempt of `client`. With a provider, its call comes
        /// first: a failed call ends the attempt before the socket, so it is
        /// no dial (README `token_provider`). `true` if that injected events.
        fn new_dial(&mut self, client: usize) -> bool {
            if let Some(Token::Provider(p)) = self.clients[client].provider.clone() {
                let outcome = p
                    .token()
                    .now_or_never()
                    .expect("the runner's provider answers at once");
                if let Err(e) = outcome {
                    let fatal = matches!(e, TokenError::Fatal(_));
                    let source = std::error::Error::source(&e).map(ToString::to_string);
                    let reason = DisconnectReason::new_for_test()
                        .with_conn_id(None)
                        .with_phase(Phase::Dial)
                        .with_fatal(fatal)
                        .with_message(format!("{e}: {}", source.unwrap_or_default()));
                    if fatal {
                        self.edge.disconnected(reason);
                        self.clients[client].dead = true;
                    } else {
                        self.schedule(client, None, Plan::Backoff, reason);
                    }
                    return true;
                }
            }
            self.dials.push(Dial {
                client,
                ..Dial::default()
            });
            false
        }

        /// Ends dial `d`; its stability window counts if it had passed.
        fn end(&mut self, d: usize) {
            self.dials[d].ended = true;
            self.dials[d].held = false;
            let c = &mut self.clients[self.dials[d].client];
            if let Some((sd, at)) = c.stable {
                if sd == d {
                    if Instant::now() >= at {
                        c.attempt = 0;
                        c.keepalive_used = false;
                        c.refresh_spent = false;
                    }
                    c.stable = None;
                }
            }
            if c.active == Some(d) {
                c.active = None;
            }
        }

        /// `policy::decide` at `rng = 0.5`: the attempt is counted first.
        fn delay(&mut self, client: usize, plan: Plan) -> (u32, Duration) {
            let c = &mut self.clients[client];
            if let Some((_, at)) = c.stable {
                if Instant::now() >= at {
                    c.attempt = 0;
                    c.keepalive_used = false;
                    c.refresh_spent = false;
                    c.stable = None;
                }
            }
            c.attempt += 1;
            let delay = match plan {
                Plan::Backoff => {
                    let factor = 1u32 << c.attempt.min(20);
                    BASE.saturating_mul(factor).min(CAP) / 2
                }
                Plan::GoingAway => DRAIN_DELAY,
                Plan::StartAtCap => CAP / 2,
                Plan::Immediate => Duration::ZERO,
            };
            (c.attempt, delay)
        }

        fn schedule(
            &mut self,
            client: usize,
            conn_id: Option<ConnId>,
            plan: Plan,
            reason: DisconnectReason,
        ) {
            let (attempt, delay) = self.delay(client, plan);
            self.edge.disconnected(reason.with_retry_in(Some(delay)));
            self.edge.redial(
                RedialNotice::new_for_test()
                    .with_conn_id(conn_id)
                    .with_cause(RedialCause::Failure)
                    .with_attempt(attempt)
                    .with_delay(delay),
            );
            self.clients[client].timer = Some(Instant::now() + delay);
        }

        /// Whether a token rejected before `welcome` gets its one refresh.
        fn can_refresh(&self, client: usize) -> bool {
            let c = &self.clients[client];
            matches!(c.provider, Some(Token::Provider(_))) && !c.refresh_spent
        }

        /// The refresh after a rejected token: a non-fatal `Disconnected`,
        /// then `Redial{TokenRefresh}` at once (CLIENT-SDK.md's Rejected
        /// token row). Faked here for fixture 38; the real supervisor is
        /// covered by ws-mixer's `redial_token_refresh_follows_a_non_fatal_disconnected`.
        fn refresh(&mut self, client: usize, d: usize, reason: DisconnectReason) {
            self.clients[client].refresh_spent = true;
            self.edge
                .disconnected(reason.with_fatal(false).with_retry_in(Some(Duration::ZERO)));
            self.edge.redial(
                RedialNotice::new_for_test()
                    .with_conn_id(Some(conn(d)))
                    .with_cause(RedialCause::TokenRefresh)
                    .with_attempt(1)
                    .with_delay(Duration::ZERO),
            );
            self.clients[client].timer = Some(Instant::now());
        }

        fn dial_failed(&mut self, d: usize, status: Option<u16>) {
            let client = self.dials[d].client;
            if !self.clients[client].running() || self.dials[d].ended {
                return;
            }
            self.end(d);
            let refresh = status == Some(401) && self.can_refresh(client);
            let fatal = !refresh && matches!(status, Some(401 | 403 | 404));
            let reason = DisconnectReason::new_for_test()
                .with_conn_id(Some(conn(d)))
                .with_phase(Phase::Dial)
                .with_http_status(status)
                .with_fatal(fatal)
                .with_message(status.map_or("connection refused".into(), |s| format!("HTTP {s}")));
            if refresh {
                self.refresh(client, d, reason);
                return;
            }
            if fatal {
                self.edge.disconnected(reason);
                self.clients[client].dead = true;
                return;
            }
            self.schedule(client, Some(conn(d)), Plan::Backoff, reason);
        }

        fn disconnect(
            &mut self,
            d: usize,
            fatal: bool,
            code: Option<String>,
            ws_code: Option<u32>,
        ) {
            let client = self.dials[d].client;
            if self.clients[client].closed || self.dials[d].ended {
                return;
            }
            let dial = &self.dials[d];
            let phase = if dial.welcomed {
                Phase::Connected
            } else if dial.upgraded {
                Phase::Handshake
            } else {
                Phase::Dial
            };
            let was_active = self.clients[client].active == Some(d);
            let rejected = phase != Phase::Connected
                && (ws_code == Some(4011) || code.as_deref() == Some("UNAUTHORIZED"));
            self.end(d);
            let reason = DisconnectReason::new_for_test()
                .with_conn_id(Some(conn(d)))
                .with_phase(phase)
                .with_ws_code(ws_code)
                .with_error_name(code.map(|c| &*Box::leak(c.into_boxed_str())))
                .with_fatal(fatal)
                .with_message(format!("closed {}", ws_code.unwrap_or(1006)));
            if !fatal && rejected && self.clients[client].running() && self.can_refresh(client) {
                self.refresh(client, d, reason);
                return;
            }
            if fatal {
                self.edge.disconnected(reason);
                self.clients[client].dead = true;
                return;
            }
            if !self.clients[client].running() {
                self.edge.disconnected(reason);
                return;
            }
            if phase == Phase::Connected && !was_active {
                // A drained connection: its replacement is already scheduled.
                self.edge.disconnected(reason);
                return;
            }
            let plan = match ws_code {
                None => Plan::Backoff,
                Some(code) => {
                    match classify_close(u16::try_from(code).unwrap_or(4002), phase, false) {
                        CloseClass::ImmediateJitter2s => Plan::GoingAway,
                        CloseClass::StartAtCap => Plan::StartAtCap,
                        CloseClass::KeepaliveImmediateOnce
                            if !self.clients[client].keepalive_used =>
                        {
                            self.clients[client].keepalive_used = true;
                            Plan::Immediate
                        }
                        _ => Plan::Backoff,
                    }
                }
            };
            self.schedule(client, Some(conn(d)), plan, reason);
        }

        fn welcome(&mut self, d: usize) {
            let client = self.dials[d].client;
            self.dials[d].upgraded = true;
            self.dials[d].welcomed = true;
            self.edge.connected(d as u64 + 1);
            let c = &mut self.clients[client];
            c.active = Some(d);
            c.stable = Some((d, Instant::now() + STABLE_AFTER));
        }

        /// A `drain`; `retry_after` is used as is for the replacement dial,
        /// as the transport does (within WIRE.md §2.9's hint + 0-2000 ms).
        /// The floor check against it only proves mcpwarp forwards the delay;
        /// the transport honouring `retry_after_ms` is covered by ws-mixer's
        /// `drain_honours_retry_after_and_ignores_a_repeat`.
        fn drain(&mut self, d: usize, reason: &str, retry_after: Option<Duration>) {
            let parsed = match reason {
                "rollout" => DrainReason::Rollout,
                "overload" => DrainReason::Overload,
                "id_exhausted" => DrainReason::IdExhausted,
                "replaced" => DrainReason::Replaced,
                "client_requested" => DrainReason::ClientRequested,
                _ => DrainReason::Maintenance,
            };
            self.edge.drain(
                DrainNotice::new_for_test()
                    .with_conn_id(conn(d))
                    .with_reason(parsed)
                    .with_raw_reason(reason.to_owned())
                    .with_retry_after(retry_after),
            );
            let client = self.dials[d].client;
            // `replaced` is vetoed (no replacement); a drain of anything but
            // the active connection dials nothing either.
            if parsed == DrainReason::Replaced
                || !self.clients[client].running()
                || self.clients[client].active != Some(d)
            {
                return;
            }
            self.clients[client].active = None;
            self.drain_floor = retry_after;
            let delay = retry_after.unwrap_or(DRAIN_DELAY);
            self.edge.redial(
                RedialNotice::new_for_test()
                    .with_conn_id(Some(conn(d)))
                    .with_cause(RedialCause::Drain)
                    .with_attempt(0)
                    .with_delay(delay),
            );
            self.clients[client].timer = Some(Instant::now() + delay);
        }

        /// Follows what the SDK did: new clients (each a dial), `register`
        /// names, `unregister` answers, closes. `true` if it injected
        /// something.
        fn sync(&mut self) -> bool {
            let mut injected = false;
            while self.seen_connects < self.edge.connects() {
                self.seen_connects += 1;
                // Configures are serialized: the newest one's client.
                let provider = self
                    .providers
                    .as_ref()
                    .zip(self.last_token.as_ref())
                    .and_then(|(p, t)| p.get(t).cloned());
                self.clients.push(Client {
                    provider,
                    ..Client::default()
                });
                // The SDK counts OVERLOADED per connection (§2.6); the runner
                // resets its count per runner client (one per ws-mixer client
                // build), which every fixture's single-connection
                // OVERLOADED runs fit.
                self.overloaded = 0;
                self.overload_paused = false;
                injected |= self.new_dial(self.clients.len() - 1);
            }
            let sent = self.edge.sent();
            for body in &sent[self.seen_sent..] {
                let env = &body["mcpwarp"];
                let name = env["services"][0]["name"].as_str().map(str::to_owned);
                let newest = self.clients.iter().rposition(|c| !c.closed);
                match (env["op"].as_str(), newest) {
                    (Some("register"), Some(c)) => self.clients[c].name = name,
                    (Some("unregister"), Some(c)) => {
                        let Some(d) = self.clients[c].active else {
                            continue;
                        };
                        if self.dials[d].held {
                            continue;
                        }
                        let name = name.unwrap_or_default();
                        self.app(
                            d,
                            "unregistered",
                            json!({ "services": [{ "name": name, "id": format!("srv_{name}") }] }),
                        );
                        injected = true;
                    }
                    _ => {}
                }
            }
            self.seen_sent = sent.len();
            let closes = self.edge.closes().len();
            while self.seen_closes < closes {
                self.seen_closes += 1;
                if let Some(c) = self.clients.iter_mut().find(|c| !c.closed) {
                    c.closed = true;
                    c.timer = None;
                }
            }
            injected
        }

        /// Lets the SDK run until it is quiescent, answering the edge's part.
        async fn quiesce(&mut self) {
            for _ in 0..50 {
                settle().await;
                if !self.sync() {
                    return;
                }
            }
        }

        /// Moves the clock by `by`, starting each scheduled dial on time.
        async fn advance(&mut self, by: Duration) {
            let target = Instant::now() + by;
            loop {
                let next = self
                    .clients
                    .iter()
                    .filter(|c| c.running())
                    .filter_map(|c| c.timer)
                    .filter(|t| *t <= target)
                    .min();
                let Some(at) = next else {
                    tokio::time::sleep_until(target).await;
                    self.quiesce().await;
                    return;
                };
                tokio::time::sleep_until(at).await;
                self.quiesce().await;
                let now = Instant::now();
                let due: BTreeSet<usize> = self
                    .clients
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.running() && c.timer.is_some_and(|t| t <= now))
                    .map(|(i, _)| i)
                    .collect();
                for c in due {
                    self.clients[c].timer = None;
                    let _ = self.new_dial(c);
                }
                self.quiesce().await;
            }
        }
    }
}

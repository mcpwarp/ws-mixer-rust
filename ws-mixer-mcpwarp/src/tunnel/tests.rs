//! The tunnel on the fake edge: burst, dispose, subscribe, drop, and the
//! rows no fixture reaches (T8 from the transport, T25 malformed).

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use http_body_util::Full;
use ws_mixer::{BuildError, ClientBuilder, ReconnectPolicy};

use crate::transport::{Connector, Transport};

use super::status::{TunnelState, TunnelStatus, TunnelStatusCode, last_error};
use super::{Subscription, Tunnel, TunnelConfig, TunnelOptions, TunnelOptionsBuilder};
use crate::bridge::handler_fn;
use crate::test_util::{CloseCall, FakeEdge, settle};
use crate::{DescribeOptions, describe_status};

const ID: &str = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";
const URL: &str = "https://notes-u1.mcpwarp.example/mcp";

fn options() -> TunnelOptionsBuilder {
    TunnelOptions::builder()
        .integration(ID)
        .handler(handler_fn(|_req| async {
            Ok(http::Response::new(Full::new(Bytes::from_static(b"ok"))))
        }))
}

fn tunnel(edge: &FakeEdge) -> Tunnel {
    Tunnel::new(options().fake_edge(edge).build().unwrap()).unwrap()
}

fn config(name: &str) -> TunnelConfig {
    TunnelConfig {
        enabled: true,
        token: Some("t".into()),
        name: Some(name.into()),
        ..TunnelConfig::default()
    }
}

fn bad_policy() -> ReconnectPolicy {
    let mut p = ReconnectPolicy::default();
    p.base = Duration::ZERO;
    p
}

type Seen = Arc<Mutex<Vec<TunnelStatus>>>;

fn record(t: &Tunnel) -> (Seen, Subscription) {
    let seen = Seen::default();
    let s = seen.clone();
    let sub = t.subscribe(move |status| s.lock().unwrap().push(status.clone()));
    (seen, sub)
}

fn states(seen: &Seen) -> Vec<TunnelState> {
    seen.lock().unwrap().iter().map(|s| s.state).collect()
}

fn online() -> TunnelStatus {
    TunnelStatus {
        url: Some(URL.into()),
        ..TunnelStatus::new(TunnelState::Online)
    }
}

/// Welcomes connection 1 and registers `name`.
async fn go_online(edge: &FakeEdge, name: &str) {
    edge.connected(1);
    settle().await;
    edge.app(1, FakeEdge::registered(name, "srv_1", URL, true));
    settle().await;
}

/// Everything logged under the `mcpwarp` target while the guard lives.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Logs;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Logs {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Self::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    fn has(&self, line: &str) -> bool {
        self.text().contains(line)
    }
}

#[test]
fn new_outside_a_runtime_fails() {
    let err = Tunnel::new(options().build().unwrap()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "mcpwarp: Tunnel::new must be called within a tokio runtime"
    );
}

#[test]
fn build_checks_the_up_front_options_only() {
    let err = TunnelOptions::builder()
        .integration(ID)
        .build()
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "mcpwarp: pass exactly one of `handler` or `target`"
    );
    let err = options().integration("nope").build().unwrap_err();
    assert!(
        err.to_string()
            .contains("`integration` must be an RFC 4122 UUID")
    );
    // The reconnect policy is checked at start (T8), not here.
    assert!(options().reconnect(bad_policy()).build().is_ok());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn burst_configure_stop_configure_dials_only_the_last() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    // No yield between the calls: each is queued when made.
    let a = t.configure(config("alpha"));
    let s = t.stop();
    let b = t.configure(config("bravo"));
    settle().await;
    assert_eq!(edge.connects(), 1);
    edge.connected(1);
    settle().await;
    assert_eq!(edge.sent()[0]["mcpwarp"]["services"][0]["name"], "bravo");
    assert!(a.now_or_never().is_some());
    assert!(s.now_or_never().is_some());
    assert!(b.now_or_never().is_some());
    assert_eq!(states(&seen), [TunnelState::Off, TunnelState::Connecting]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn burst_configure_stop_never_dials() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    let a = t.configure(config("alpha"));
    let s = t.stop();
    settle().await;
    assert_eq!(edge.connects(), 0);
    assert!(a.now_or_never().is_some() && s.now_or_never().is_some());
    assert_eq!(states(&seen), [TunnelState::Off]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn configure_is_idempotent_and_a_repeat_waits_for_the_same_apply() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    let first = t.configure(config("notes"));
    let again = t.configure(TunnelConfig {
        token: Some(" t\n".into()),
        ..config("notes")
    });
    settle().await;
    assert!(first.now_or_never().is_some() && again.now_or_never().is_some());
    go_online(&edge, "notes").await;
    t.configure(config("notes")).await;
    assert_eq!(edge.connects(), 1);
    assert_eq!(*seen.lock().unwrap().last().unwrap(), online());
    assert_eq!(seen.lock().unwrap().len(), 3);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dispose_is_memoized_bounded_and_final() {
    let (logs, _guard) = Logs::capture();
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    assert_eq!(
        t.dispose(Duration::ZERO).err().unwrap().to_string(),
        "mcpwarp: dispose() `timeout` must be positive"
    );
    t.configure(config("notes")).await;
    go_online(&edge, "notes").await;

    // The edge never answers `unregister`: the close takes the 2 s budget.
    let first = t.dispose(Duration::from_millis(100)).unwrap();
    let second = t.dispose(Duration::from_secs(60)).unwrap();
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "unregister"]);
    tokio::time::sleep(Duration::from_millis(100)).await;
    settle().await;
    assert!(first.now_or_never().is_some());
    assert!(
        second.now_or_never().is_some(),
        "the first call's timeout wins"
    );
    assert!(logs.has("dispose() gave up waiting after 100 ms"));
    assert_eq!(
        states(&seen),
        [
            TunnelState::Off,
            TunnelState::Connecting,
            TunnelState::Online,
            TunnelState::Off
        ]
    );

    // Final: nothing dials, nothing is delivered.
    t.configure(config("other")).await;
    t.stop().await;
    assert_eq!(edge.connects(), 1);
    assert_eq!(seen.lock().unwrap().len(), 4);
    let (late, _late_sub) = record(&t);
    assert_eq!(*late.lock().unwrap(), [TunnelStatus::default()]);
    // The old connection still closes in the background.
    tokio::time::sleep(Duration::from_secs(3)).await;
    settle().await;
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    assert!(
        t.dispose(Duration::from_millis(1))
            .unwrap()
            .now_or_never()
            .is_some()
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn subscribe_immediate_dedupe_unsubscribe_and_panics() {
    let (logs, _guard) = Logs::capture();
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    let _bad = t.subscribe(|_| panic!("boom"));
    assert!(logs.has("status listener threw: boom"));
    let (other, other_sub) = record(&t);
    assert_eq!(*other.lock().unwrap(), [TunnelStatus::default()]);

    t.configure(config("notes")).await;
    drop(other_sub);
    go_online(&edge, "notes").await;
    // A refused `register` while disabled re-reports `disabled`: no new
    // snapshot (§6.4 dedup).
    edge.app(1, FakeEdge::disable("srv_1", "disabled"));
    settle().await;
    edge.app(1, FakeEdge::disable("srv_1", "again"));
    settle().await;

    let disabled = TunnelStatus {
        code: Some(TunnelStatusCode::Disabled),
        last_error: Some(last_error::DISABLED.into()),
        ..TunnelStatus::new(TunnelState::Disabled)
    };
    assert_eq!(
        *seen.lock().unwrap(),
        [
            TunnelStatus::default(),
            TunnelStatus::new(TunnelState::Connecting),
            online(),
            disabled.clone(),
        ]
    );
    // Unsubscribed after `connecting`.
    assert_eq!(states(&other), [TunnelState::Off, TunnelState::Connecting]);
    // The panicking listener did not stop the others, and is still called.
    assert_eq!(
        logs.text().matches("status listener threw: boom").count(),
        4
    );
    assert_eq!(t.status(), disabled);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn watch_status_sees_the_latest() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let mut rx = t.watch_status();
    assert_eq!(*rx.borrow_and_update(), TunnelStatus::default());
    t.configure(config("notes")).await;
    go_online(&edge, "notes").await;
    assert!(rx.has_changed().unwrap());
    assert_eq!(*rx.borrow_and_update(), online());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn last_drop_disposes_in_the_background() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let rx = t.watch_status();
    t.configure(config("notes")).await;
    go_online(&edge, "notes").await;
    let clone = t.clone();
    drop(t);
    settle().await;
    assert_eq!(edge.sent_ops(), ["register"], "a clone is still alive");
    drop(clone);
    settle().await;
    assert_eq!(edge.sent_ops(), ["register", "unregister"]);
    edge.app(1, FakeEdge::unregistered("notes", "srv_1"));
    settle().await;
    assert_eq!(edge.closes(), [CloseCall::Graceful]);
    assert_eq!(*rx.borrow(), TunnelStatus::default());
}

/// MCPWARP.md §7's host "off" pattern: a blank token is plain `off` (no
/// dial, no error); the host shows its own hint on a code-less status.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn host_off_pattern() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    t.configure(TunnelConfig {
        token: Some("  ".into()),
        ..config("notes")
    })
    .await;
    t.configure(TunnelConfig {
        enabled: false,
        token: None,
        name: None,
        ..TunnelConfig::default()
    })
    .await;
    assert_eq!(edge.connects(), 0);
    assert_eq!(*seen.lock().unwrap(), [TunnelStatus::default()]);

    let hint = "Add an mcpwarp token in the settings to go online.";
    let shown = TunnelStatus {
        last_error: Some(hint.into()),
        ..t.status()
    };
    let text = describe_status(&shown, &DescribeOptions::default());
    assert_eq!(text.label, "Off");
    assert_eq!(text.detail.as_deref(), Some(hint));
}

/// §6.7 T8 from the transport: a URL the client refuses is
/// `INVALID_CONFIG`, without a dial.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_url_is_invalid_config() {
    let (logs, _guard) = Logs::capture();
    let t = Tunnel::new(options().build().unwrap()).unwrap();
    let (seen, _sub) = record(&t);
    t.configure(TunnelConfig {
        url: Some("not a url".into()),
        ..config("notes")
    })
    .await;
    settle().await;
    let last = seen.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.state, TunnelState::Error);
    assert_eq!(last.code, Some(TunnelStatusCode::InvalidConfig));
    assert_eq!(last.last_error.as_deref(), Some(last_error::INVALID_CONFIG));
    assert!(
        logs.has("INVALID_CONFIG: `url` must be a valid ws:// or wss:// URL"),
        "{}",
        logs.text()
    );
    // §6.9: the URL may be logged, but not in the error line.
    let text = logs.text();
    let error = text.lines().find(|l| l.contains("INVALID_CONFIG")).unwrap();
    assert!(!error.contains("not a url"), "{error}");
}

/// §6.7 T8 for `reconnect`: checked at start, no dial.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_reconnect_is_invalid_config_at_start() {
    let (logs, _guard) = Logs::capture();
    let edge = FakeEdge::new();
    let options = options().reconnect(bad_policy()).fake_edge(&edge);
    let t = Tunnel::new(options.build().unwrap()).unwrap();
    let (seen, _sub) = record(&t);
    t.configure(config("notes")).await;
    settle().await;
    assert_eq!(edge.connects(), 0);
    assert_eq!(
        states(&seen),
        [
            TunnelState::Off,
            TunnelState::Connecting,
            TunnelState::Error
        ]
    );
    assert!(
        logs.has(
            "INVALID_CONFIG: `reconnect.base` must be a positive duration of at most 24 hours"
        )
    );
}

/// §6.7 T25 (malformed envelope) and T26, and the §6.9 log lines.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn malformed_envelope_and_log_lines() {
    let (logs, _guard) = Logs::capture();
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    t.configure(TunnelConfig {
        token: Some("secret-token-value".into()),
        ..config("notes")
    })
    .await;
    edge.connected(1);
    settle().await;
    let malformed = FakeEdge::envelope("registered", serde_json::json!({"services": "x"}));
    edge.app(1, malformed.clone());
    settle().await;
    assert_eq!(
        seen.lock().unwrap().last().unwrap().code,
        Some(TunnelStatusCode::Internal)
    );
    edge.app(1, FakeEdge::registered("notes", "srv_1", URL, true));
    settle().await;
    edge.app(1, malformed);
    settle().await;
    // T26: online stays online.
    assert_eq!(t.status(), online());
    t.stop().await;
    let text = logs.text();
    for line in [
        "connecting as \"notes\"",
        "MALFORMED_ENVELOPE: ",
        &format!("online at {URL}"),
        "stopping",
    ] {
        assert!(text.contains(line), "{line:?} missing from:\n{text}");
    }
    assert!(!text.contains("secret-token-value"));
    assert!(!text.contains("mcpwarp: "), "no prefix in:\n{text}");
}

fn unsupported() -> TunnelStatus {
    TunnelStatus {
        code: Some(TunnelStatusCode::UnsupportedVersion),
        last_error: Some(last_error::UNSUPPORTED_VERSION.into()),
        ..TunnelStatus::new(TunnelState::Error)
    }
}

/// A fatal session error is `error` as soon as the driver starts closing,
/// not after the (here hanging) close: `Error(Fatal)`, before `Close`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fatal_error_is_shown_before_the_close_finishes() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    let (seen, _sub) = record(&t);
    t.configure(config("notes")).await;
    go_online(&edge, "notes").await;
    edge.hang_close(true);
    edge.app(1, FakeEdge::error("UNSUPPORTED_VERSION", "v2"));
    settle().await;
    assert_eq!(
        edge.closes(),
        [CloseCall::Graceful],
        "the close is under way"
    );
    assert_eq!(t.status(), unsupported());
    // The `Close` after the close bound changes nothing.
    tokio::time::sleep(Duration::from_secs(12)).await;
    settle().await;
    assert_eq!(seen.lock().unwrap().last().unwrap(), &unsupported());
    assert_eq!(seen.lock().unwrap().len(), 4);
}

/// T37: after a refusal before the first registration, the same config is
/// a new one even while the refused connection is still closing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn same_config_after_a_refusal_dials_again() {
    let edge = FakeEdge::new();
    let t = tunnel(&edge);
    t.configure(config("notes")).await;
    edge.hang_close(true);
    edge.connected(1);
    settle().await;
    edge.app(
        1,
        FakeEdge::register_error("notes", "UNAUTHENTICATED", "bad token"),
    );
    settle().await;
    assert_eq!(t.status().code, Some(TunnelStatusCode::Unauthorized));
    // Before the refused connection's `Close`.
    let again = t.configure(config("notes"));
    settle().await;
    assert_eq!(t.status(), TunnelStatus::new(TunnelState::Connecting));
    edge.hang_close(false);
    tokio::time::sleep(Duration::from_secs(12)).await;
    settle().await;
    assert!(again.now_or_never().is_some());
    assert_eq!(edge.connects(), 2, "the retry was not swallowed");
}

struct Boom;

impl Connector for Boom {
    fn connect(self: Box<Self>, _: ClientBuilder) -> Result<Transport, BuildError> {
        panic!("connector boom")
    }
}

/// A generation whose task panics does not wedge the apply queue.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_panicking_generation_does_not_wedge_the_queue() {
    let edge = FakeEdge::new();
    let mut options = options().fake_edge(&edge).build().unwrap();
    let fake = options.connector.clone();
    let first = AtomicBool::new(true);
    options.connector = Arc::new(move || {
        if first.swap(false, Ordering::SeqCst) {
            Box::new(Boom)
        } else {
            fake()
        }
    });
    let t = Tunnel::new(options).unwrap();
    t.configure(config("alpha")).await;
    settle().await;
    let next = t.configure(config("bravo"));
    settle().await;
    assert!(next.now_or_never().is_some());
    assert_eq!(edge.connects(), 1);
    edge.connected(1);
    settle().await;
    assert_eq!(edge.sent()[0]["mcpwarp"]["services"][0]["name"], "bravo");
}

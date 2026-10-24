//! The session against a fake clock and a recording driver. Ports
//! ws-mixer-js `test/mcpwarp-session.test.ts` and adds one test per
//! normative rule of MCPWARP.md §2.7, §3.2, §3.3 and §5.9's connection
//! identity (tunnel rows T29, T38, T44).

use serde_json::json;

use super::*;

const MS: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, PartialEq)]
enum Closed {
    Graceful,
    Immediate(String),
    Fatal(String),
}

struct H {
    s: Session,
    now: Duration,
    timers: Vec<(TimerId, Duration)>,
    /// The `mcpwarp` envelopes sent.
    sent: Vec<Value>,
    events: Vec<SessionEvent>,
    closed: Vec<Closed>,
    fatals: Vec<McpwarpError>,
    /// Sends fail (reported back like a driver would).
    send_fails: bool,
}

fn c(n: u64) -> ConnId {
    ConnId::new_for_test(n)
}

impl H {
    fn new() -> Self {
        Self {
            s: Session::new("anki", ServiceKind::Http),
            now: Duration::ZERO,
            timers: Vec::new(),
            sent: Vec::new(),
            events: Vec::new(),
            closed: Vec::new(),
            fatals: Vec::new(),
            send_fails: false,
        }
    }

    fn pump(&mut self) {
        while let Some(cmd) = self.s.poll_command() {
            match cmd {
                Command::SendApp { body, op } => {
                    self.sent.push(body["mcpwarp"].clone());
                    if self.send_fails {
                        self.s.on_send_failed(op);
                    }
                }
                Command::StartTimer { id, delay } => self.timers.push((id, self.now + delay)),
                Command::CancelTimer(id) => self.timers.retain(|(t, _)| *t != id),
                Command::Emit(e) => self.events.push(e),
                Command::Fatal(e) => {
                    self.closed.push(Closed::Fatal(e.code.as_str().to_owned()));
                    self.fatals.push(e);
                }
                Command::CloseImmediate { message } => self.closed.push(Closed::Immediate(message)),
                Command::CloseGraceful => self.closed.push(Closed::Graceful),
            }
        }
    }

    fn advance(&mut self, by: Duration) {
        let until = self.now + by;
        loop {
            self.timers.sort_by_key(|(_, at)| *at);
            match self.timers.first() {
                Some(&(id, at)) if at <= until => {
                    self.timers.remove(0);
                    self.now = at;
                    self.s.on_timer(id);
                    self.pump();
                }
                _ => break,
            }
        }
        self.now = until;
    }

    fn welcome(&mut self, conn: u64) {
        self.s.on_welcome(c(conn));
        self.pump();
    }

    fn disconnect(&mut self, conn: u64) -> bool {
        let superseded = self.s.on_disconnected(c(conn));
        self.pump();
        superseded
    }

    fn drain(&mut self, conn: u64, reason: DrainReason) -> bool {
        let took = self.s.on_drain(c(conn), reason);
        self.pump();
        took
    }

    fn close(&mut self) {
        self.s.close();
        self.pump();
    }

    fn closed_client(&mut self) {
        self.s.on_closed();
        self.pump();
    }

    fn app_on(&mut self, conn: u64, env: Value) {
        let mut m = json!({"v": 1});
        m.as_object_mut()
            .unwrap()
            .extend(env.as_object().unwrap().clone());
        let body = json!({ "mcpwarp": m });
        self.s.on_app(c(conn), body.as_object().unwrap());
        self.pump();
    }

    /// An app message on the newest welcomed connection (or connection 1).
    fn app(&mut self, env: Value) {
        let conn = self.s.live.map_or(1, ConnId::get);
        self.app_on(conn, env);
    }

    fn registered(&mut self, created: bool) {
        self.app(json!({"op": "registered", "services": [
            {"name": "anki", "id": "srv_anki", "url": "https://anki-u.x/mcp", "created": created}
        ]}));
    }

    fn reg_error(&mut self, code: &str, extra: Value) {
        let mut e = json!({"name": "anki", "code": code, "message": code.to_lowercase()});
        e.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        self.app(json!({"op": "registered", "errors": [e]}));
    }

    fn ops(&self) -> Vec<String> {
        self.sent
            .iter()
            .map(|m| m["op"].as_str().unwrap().to_owned())
            .collect()
    }

    fn states(&self) -> Vec<&'static str> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::State(s) => Some(s.as_str()),
                _ => None,
            })
            .collect()
    }

    fn registrations(&self) -> Vec<Registration> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::Registered(r) => Some(r.clone()),
                _ => None,
            })
            .collect()
    }

    fn disabled(&self) -> Vec<(Option<String>, String)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::Disabled { server_id, reason } => {
                    Some((server_id.clone(), reason.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn enabled(&self) -> Vec<String> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::Enabled { server_id } => Some(server_id.clone()),
                _ => None,
            })
            .collect()
    }

    fn conflicts(&self) -> Vec<Conflict> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::Conflict(c) => Some(c.clone()),
                _ => None,
            })
            .collect()
    }

    fn errors(&self) -> Vec<(String, ErrorOrigin)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::Error { error, origin } => {
                    assert!(!error.fatal);
                    Some((error.code.as_str().to_owned(), *origin))
                }
                _ => None,
            })
            .collect()
    }

    fn retries(&self) -> Vec<(RetryInfo, RegisterRetryKind)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                SessionEvent::Retry { info, kind } => Some((info.clone(), *kind)),
                _ => None,
            })
            .collect()
    }
}

fn register_env() -> Value {
    json!({"v": 1, "op": "register", "services": [{"name": "anki", "kind": "http"}]})
}

// --- ports of the JS session tests ----------------------------------------------

#[test]
fn registers_on_every_welcome_and_reports_created_each_time() {
    let mut h = H::new();
    assert_eq!(h.s.state(), ExposeState::Connecting);
    h.welcome(1);
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert_eq!(h.sent, [register_env()]);
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert_eq!(h.s.url(), Some("https://anki-u.x/mcp"));
    assert_eq!(h.s.server_id(), Some("srv_anki"));
    h.disconnect(1);
    assert_eq!(h.s.state(), ExposeState::Connecting);
    h.welcome(2);
    h.registered(false);
    assert_eq!(h.ops(), ["register", "register"]);
    let reg = |created| Registration {
        url: "https://anki-u.x/mcp".into(),
        server_id: "srv_anki".into(),
        created,
    };
    assert_eq!(h.registrations(), [reg(true), reg(false)]);
    assert_eq!(h.s.registration(), Some(&reg(false)));
    assert_eq!(
        h.states(),
        [
            "registering",
            "registered",
            "connecting",
            "registering",
            "registered"
        ]
    );
}

#[test]
fn ignores_entries_for_other_names_and_non_mcpwarp_bodies() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "registered",
        "services": [{"name": "other", "id": "x", "url": "https://other-u.x/mcp"}],
        "errors": [{"name": "other2", "code": "CONFLICT"}]}));
    h.s.on_app(c(1), json!({"hello": 1}).as_object().unwrap());
    h.pump();
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.fatals.is_empty() && h.errors().is_empty());
}

#[test]
fn server_disabled_adopts_the_error_id() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("SERVER_DISABLED", json!({"id": "srv_9"}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    assert_eq!(h.disabled(), [(Some("srv_9".into()), "disabled".into())]);
    assert_eq!(h.s.server_id(), Some("srv_9"));
    // The connection is kept (§3.2 Disabled).
    assert!(h.closed.is_empty());
}

#[test]
fn transient_errors_back_off_1_2_4_capped_at_30s_and_reset_on_success() {
    let mut h = H::new();
    h.welcome(1);
    let mut delays = Vec::new();
    for _ in 0..7 {
        h.reg_error("UNAVAILABLE", json!({}));
        let before = h.sent.len();
        let start = h.now;
        while h.sent.len() == before {
            h.advance(MS * 100);
        }
        delays.push((h.now - start).as_millis());
    }
    assert_eq!(delays, [1000, 2000, 4000, 8000, 16000, 30000, 30000]);
    let errs = h.errors();
    assert_eq!(errs.len(), 7);
    assert!(
        errs.iter()
            .all(|e| *e == ("UNAVAILABLE".to_owned(), ErrorOrigin::RegisterTransient))
    );
    let retries = h.retries();
    assert_eq!(
        retries.iter().map(|(r, _)| r.attempt).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6, 7]
    );
    assert_eq!(
        retries[0],
        (
            RetryInfo {
                attempt: 1,
                delay: MS * 1000,
                reason: "register failed: UNAVAILABLE: unavailable".into()
            },
            RegisterRetryKind::Transient
        )
    );
    assert_eq!(h.s.state(), ExposeState::Registering);
    h.registered(false);
    h.reg_error("TIMEOUT", json!({}));
    let before = h.sent.len();
    h.advance(MS * 1000);
    assert_eq!(h.sent.len(), before + 1);
}

#[test]
fn transient_backoff_carries_across_reconnects() {
    // §3.2: n resets only on a successful `registered`.
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.disconnect(1);
    h.welcome(2);
    h.reg_error("INTERNAL", json!({}));
    let r = h.retries();
    assert_eq!(
        r.iter().map(|(i, _)| i.delay).collect::<Vec<_>>(),
        [MS * 1000, MS * 2000]
    );
}

#[test]
fn a_disconnect_cancels_a_pending_retry_and_the_next_welcome_registers() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.disconnect(1);
    assert!(h.timers.is_empty());
    h.advance(MS * 5000);
    assert_eq!(h.ops(), ["register"]);
    h.welcome(2);
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn a_refusal_is_fatal_before_the_first_registration_and_an_error_after() {
    let mut first = H::new();
    first.welcome(1);
    first.reg_error("QUOTA_EXCEEDED", json!({}));
    assert_eq!(first.fatals.len(), 1);
    let e = &first.fatals[0];
    assert_eq!(e.code.as_str(), "QUOTA_EXCEEDED");
    assert!(e.fatal);
    assert_eq!(e.service_name.as_deref(), Some("anki"));
    assert_eq!(e.message, "quota_exceeded");
    assert_eq!(first.closed, [Closed::Fatal("QUOTA_EXCEEDED".into())]);

    let mut later = H::new();
    later.welcome(1);
    later.registered(false);
    later.disconnect(1);
    later.welcome(2);
    later.reg_error("CONFLICT", json!({}));
    later.reg_error("SOMETHING_NEW", json!({}));
    assert!(later.fatals.is_empty());
    assert_eq!(
        later.errors(),
        [
            ("CONFLICT".to_owned(), ErrorOrigin::RegisterRefusal),
            ("SOMETHING_NEW".to_owned(), ErrorOrigin::RegisterRefusal)
        ]
    );
    later.advance(MS * 60_000);
    assert_eq!(
        later.ops(),
        ["register", "register"],
        "no retry until the next welcome"
    );
}

#[test]
fn every_listed_refusal_and_an_unknown_code_is_fatal_before_registration() {
    for code in [
        "INVALID_NAME",
        "USERNAME_REQUIRED",
        "QUOTA_EXCEEDED",
        "CONFLICT",
        "BAD_REQUEST",
        "UNAUTHENTICATED",
        "FORBIDDEN",
        "NOT_IMPLEMENTED",
        "RATE_LIMITED",
    ] {
        let mut h = H::new();
        h.welcome(1);
        h.reg_error(code, json!({}));
        assert_eq!(h.closed, [Closed::Fatal(code.into())], "{code}");
        assert!(h.timers.is_empty());
    }
    for code in [
        "INTERNAL",
        "NO_RESPONDERS",
        "TIMEOUT",
        "ABORTED",
        "CONNECTION_LOST",
        "MALFORMED_REPLY",
        "UNAVAILABLE",
        "SERVICE_ERROR",
    ] {
        let mut h = H::new();
        h.welcome(1);
        h.reg_error(code, json!({}));
        assert!(h.closed.is_empty(), "{code} is transient");
        assert_eq!(h.retries().len(), 1, "{code}");
    }
}

#[test]
fn error_origins() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.app(
        json!({"op": "error", "code": "INTERNAL", "message": "an app error that shares the name"}),
    );
    h.app(json!({"op": "disable"}));
    assert_eq!(
        h.errors(),
        [
            ("INTERNAL".to_owned(), ErrorOrigin::RegisterTransient),
            ("INTERNAL".to_owned(), ErrorOrigin::App),
            ("MALFORMED_ENVELOPE".to_owned(), ErrorOrigin::Malformed)
        ]
    );
    assert_eq!(
        h.retries().iter().map(|(_, k)| *k).collect::<Vec<_>>(),
        [RegisterRetryKind::Transient]
    );
}

#[test]
fn drain_replaced_takes_over_like_conflict_and_a_second_changes_nothing() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    assert!(h.drain(1, DrainReason::Replaced));
    assert_eq!(h.s.state(), ExposeState::Conflict);
    assert_eq!(
        h.conflicts(),
        [Conflict {
            server_id: Some("srv_anki".into()),
            name: "anki".into(),
            reason: "replaced".into()
        }]
    );
    assert_eq!(h.closed, [Closed::Immediate(TAKEN_OVER_MESSAGE.into())]);
    assert!(!h.drain(1, DrainReason::Replaced));
    h.app(json!({"op": "conflict", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.conflicts().len(), 1);
    assert_eq!(h.closed.len(), 1);
    h.disconnect(1);
    h.welcome(2);
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn disable_matching_id_and_enable_reregisters() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "disable", "id": "srv_other"}));
    assert_eq!(h.s.state(), ExposeState::Registered);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "disabled"}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    h.app(json!({"op": "enable", "id": "srv_other", "name": "other"}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    h.app(json!({"op": "enable", "id": "srv_new", "name": "anki"}));
    assert_eq!(h.enabled(), ["srv_new"]);
    // id not compared, then adopted (§2.7).
    assert_eq!(h.s.server_id(), Some("srv_new"));
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert_eq!(h.ops(), ["register", "register"]);
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Registered);
}

// --- disable before any server id is known (v0.6.3) ------------------------------

#[test]
fn a_disable_before_any_server_id_is_held_and_applied_by_a_matching_registered() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "maintenance"}));
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert_eq!(h.s.server_id(), None);
    assert!(h.disabled().is_empty());
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Disabled);
    assert_eq!(h.s.server_id(), Some("srv_anki"));
    assert_eq!(h.registrations().len(), 1);
    assert_eq!(
        h.disabled(),
        [(Some("srv_anki".into()), "maintenance".into())]
    );
    // As a `disable` right after `registered` would.
    assert_eq!(h.states(), ["registering", "registered", "disabled"]);
    assert!(h.closed.is_empty());
}

#[test]
fn a_held_disable_for_another_id_is_dropped_by_registered() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_other"}));
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
    // Dropped, not kept for a later `registered` that names its id.
    h.app(json!({"op": "registered", "services": [
        {"name": "anki", "id": "srv_other", "url": "https://anki-u.x/mcp"}
    ]}));
    assert_eq!(h.s.server_id(), Some("srv_other"));
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
}

#[test]
fn only_the_latest_held_disable_counts() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "first"}));
    h.app(json!({"op": "disable", "id": "srv_other", "reason": "second"}));
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());

    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_other", "reason": "first"}));
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "second"}));
    h.registered(true);
    assert_eq!(h.disabled(), [(Some("srv_anki".into()), "second".into())]);
}

#[test]
fn a_disconnect_drops_a_held_disable() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    assert!(!h.disconnect(1));
    h.welcome(2);
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
}

#[test]
fn a_matching_enable_drops_a_held_disable() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.s.state(), ExposeState::Registering);
    // Queued behind the outstanding register (one in flight).
    assert_eq!(h.ops(), ["register"]);
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn an_enable_matching_only_the_server_id_is_ours() {
    // §2.7: `name` or `id`, e.g. after a rename.
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "renamed"}));
    assert_eq!(h.enabled(), ["srv_anki"]);
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn an_enable_for_the_held_disables_id_clears_the_hold() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "maintenance"}));
    // Neither our name nor a known id: not ours, but it ends the hold.
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "other"}));
    assert!(h.enabled().is_empty());
    assert_eq!(h.ops(), ["register"]);
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());

    // An enable for some other id keeps the hold.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "maintenance"}));
    h.app(json!({"op": "enable", "id": "srv_x", "name": "other"}));
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Disabled);
}

#[test]
fn server_disabled_drops_a_held_disable_with_one_disabled_event() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "maintenance"}));
    h.reg_error("SERVER_DISABLED", json!({"id": "srv_anki"}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    assert_eq!(h.disabled(), [(Some("srv_anki".into()), "disabled".into())]);
    // Accepted while disabled; the dropped hold is not applied by it.
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert_eq!(h.disabled().len(), 1);
}

#[test]
fn a_held_disable_is_dropped_by_the_drain_replacements_welcome() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "maintenance"}));
    assert!(!h.drain(1, DrainReason::Rollout));
    h.welcome(2);
    h.registered(true);
    // The hold belonged to connection 1 (§2.7 Held `disable` lifetime).
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
}

#[test]
fn a_held_disable_is_dropped_when_the_drained_connection_ends_before_the_welcome() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    assert!(!h.drain(1, DrainReason::Rollout));
    assert!(!h.disconnect(1));
    h.welcome(2);
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
}

#[test]
fn overloaded_resends_the_outstanding_register_once_after_1s() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    assert_eq!(
        h.retries(),
        [(
            RetryInfo {
                attempt: 1,
                delay: MS * 1000,
                reason: "register failed: OVERLOADED: busy".into()
            },
            RegisterRetryKind::Overloaded
        )]
    );
    assert!(h.errors().is_empty());
    h.advance(MS * 999);
    assert_eq!(h.ops(), ["register"]);
    h.advance(MS);
    assert_eq!(h.ops(), ["register", "register"]);
    h.registered(false);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.advance(MS * 5000);
    assert_eq!(h.ops(), ["register", "register"], "nothing outstanding");
}

/// `OVERLOADED`, then the time until the re-send went out.
fn overloaded_resend_after(h: &mut H) -> u128 {
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    let (before, start) = (h.sent.len(), h.now);
    while h.sent.len() == before {
        h.advance(MS * 100);
    }
    (h.now - start).as_millis()
}

#[test]
fn overloaded_attempt_counts_consecutive_resends_and_resets_on_success() {
    let mut h = H::new();
    h.welcome(1);
    let delays: Vec<_> = (0..3).map(|_| overloaded_resend_after(&mut h)).collect();
    assert_eq!(delays, [1000, 2000, 4000]);
    h.registered(false);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    assert_eq!(
        h.retries()
            .iter()
            .map(|(r, _)| (r.attempt, r.delay.as_millis()))
            .collect::<Vec<_>>(),
        [(1, 1000), (2, 2000), (3, 4000), (1, 1000)]
    );
    assert_eq!(h.ops().len(), 6);
}

#[test]
fn overloaded_backoff_caps_at_30s() {
    let mut h = H::new();
    h.welcome(1);
    let delays: Vec<_> = (0..7).map(|_| overloaded_resend_after(&mut h)).collect();
    assert_eq!(delays, [1000, 2000, 4000, 8000, 16000, 30000, 30000]);
}

#[test]
fn overloaded_counter_resets_on_an_error_entry_and_a_whole_frame_error() {
    // A transient error entry: its own backoff (1 s), and the next
    // OVERLOADED starts over at 1 s (fixture tunnel/26_t16).
    let mut h = H::new();
    h.welcome(1);
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    assert_eq!(overloaded_resend_after(&mut h), 2000);
    h.reg_error("INTERNAL", json!({}));
    h.advance(MS * 1000);
    assert_eq!(h.ops().len(), 4);
    assert_eq!(overloaded_resend_after(&mut h), 1000);

    // A whole-frame error op answers the frame and resets it too.
    let mut h = H::new();
    h.welcome(1);
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    assert_eq!(overloaded_resend_after(&mut h), 2000);
    h.app(json!({"op": "error", "code": "BAD_REQUEST"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(overloaded_resend_after(&mut h), 1000);
}

#[test]
fn overloaded_counter_resets_on_a_new_connections_welcome() {
    let mut h = H::new();
    h.welcome(1);
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    assert_eq!(overloaded_resend_after(&mut h), 2000);
    h.disconnect(1);
    h.welcome(2);
    assert_eq!(overloaded_resend_after(&mut h), 1000);
}

// --- one frame in flight (§2.6, §3.2) --------------------------------------------

#[test]
fn an_enable_during_an_overloaded_pause_sends_no_second_frame_and_success_stops_the_resend() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.ops(), ["register"], "folded into the scheduled re-send");
    h.registered(false);
    assert!(h.timers.is_empty(), "the OVERLOADED re-send is cancelled");
    // The enable's register goes out after the reply, alone.
    assert_eq!(h.ops(), ["register", "register"]);
    h.advance(MS * 60_000);
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn an_enable_while_a_register_is_outstanding_waits_for_the_reply() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.ops(), ["register", "register"]);
    h.registered(false);
    assert_eq!(h.ops(), ["register", "register", "register"]);
    h.registered(false);
    assert_eq!(h.ops().len(), 3, "one queued trigger, one register");
}

#[test]
fn an_enable_during_a_transient_retry_sends_at_once_and_cancels_the_retry() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.ops(), ["register", "register"]);
    assert!(h.timers.is_empty());
    h.advance(MS * 60_000);
    assert_eq!(h.ops().len(), 2);
}

#[test]
fn a_whole_frame_error_answers_the_outstanding_register() {
    for code in ["TOO_MANY_SERVICES", "SOMETHING_NEW"] {
        let mut h = H::new();
        h.welcome(1);
        h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
        h.advance(MS * 1000);
        assert_eq!(h.ops(), ["register", "register"], "{code}: re-sent");
        h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
        assert_eq!(h.ops().len(), 2, "{code}: queued behind the re-send");
        h.app(json!({"op": "error", "code": code}));
        assert_eq!(h.errors(), [(code.to_owned(), ErrorOrigin::App)]);
        // The slot is freed; the queued trigger goes out.
        assert!(h.timers.is_empty(), "{code}");
        assert_eq!(h.ops().len(), 3, "{code}");
        // The slot is free again after the next reply.
        h.registered(false);
        h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
        assert_eq!(h.ops().len(), 4, "{code}");
    }
}

#[test]
fn an_error_op_during_an_overloaded_pause_answers_nothing() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.errors(), [("UNKNOWN_OP".to_owned(), ErrorOrigin::App)]);
    assert_eq!(h.timers.len(), 1, "the re-send is still scheduled");
    h.advance(MS * 999);
    assert_eq!(h.ops(), ["register"]);
    h.advance(MS);
    assert_eq!(h.ops(), ["register", "register"]);

    // UNSUPPORTED_VERSION stays fatal.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    assert_eq!(h.closed, [Closed::Fatal("UNSUPPORTED_VERSION".into())]);
}

#[test]
fn an_empty_registered_during_an_overloaded_pause_answers_nothing() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "registered"}));
    assert_eq!(
        h.retries().iter().map(|(_, k)| *k).collect::<Vec<_>>(),
        [RegisterRetryKind::Overloaded]
    );
    assert_eq!(h.timers.len(), 1, "the re-send is still scheduled");
    assert_eq!(h.s.state(), ExposeState::Registering);
    h.advance(MS * 999);
    assert_eq!(h.ops(), ["register"]);
    h.advance(MS);
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn a_listed_registered_during_an_overloaded_pause_resets_the_counter() {
    let mut h = H::new();
    h.welcome(1);
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.registered(false);
    assert!(h.timers.is_empty(), "the 2000 ms re-send is cancelled");
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(overloaded_resend_after(&mut h), 1000);
}

#[test]
fn an_error_op_during_an_overloaded_pause_keeps_the_counter() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    h.advance(MS * 1000);
    assert_eq!(h.ops(), ["register", "register"]);
    assert_eq!(overloaded_resend_after(&mut h), 2000);
}

#[test]
fn an_empty_registered_during_an_overloaded_pause_keeps_the_counter() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "registered"}));
    h.advance(MS * 1000);
    assert_eq!(h.ops(), ["register", "register"]);
    assert_eq!(overloaded_resend_after(&mut h), 2000);
}

#[test]
fn a_registered_with_nothing_outstanding_settles_the_handle() {
    // §3.2 One controller per connection: one name, accepted by name.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert_eq!(h.registrations().len(), 1);
    assert_eq!(h.s.url(), Some("https://anki-u.x/mcp"));
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn connection_limit_does_not_answer_the_frame() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "CONNECTION_LIMIT"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.ops(), ["register"], "still outstanding");
}

#[test]
fn a_late_error_from_an_ended_connection_does_not_settle_the_new_frame() {
    let mut h = H::new();
    h.welcome(1);
    h.disconnect(1);
    h.welcome(2);
    h.app_on(1, json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.errors(), [("UNKNOWN_OP".to_owned(), ErrorOrigin::App)]);
    h.app_on(1, json!({"op": "error", "code": "OVERLOADED"}));
    assert!(h.timers.is_empty() && h.retries().is_empty());
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(
        h.ops(),
        ["register", "register"],
        "register 2 is outstanding"
    );
}

#[test]
fn a_registered_listing_none_of_the_names_is_answered_by_a_transient_resend() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "registered"}));
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.errors().is_empty() && h.registrations().is_empty());
    assert_eq!(
        h.retries(),
        [(
            RetryInfo {
                attempt: 1,
                delay: MS * 1000,
                reason: "register failed: registered reply did not list the service".into()
            },
            RegisterRetryKind::Transient
        )]
    );
    h.advance(MS * 1000);
    assert_eq!(h.ops(), ["register", "register"]);
    // The transient counter carries on.
    h.app(json!({"op": "registered", "services": [], "errors": []}));
    assert_eq!(h.retries()[1].0.delay, MS * 2000);
    // With nothing outstanding it is ignored.
    h.advance(MS * 2000);
    h.registered(false);
    h.app(json!({"op": "registered"}));
    assert_eq!(h.retries().len(), 2);
    assert_eq!(h.s.state(), ExposeState::Registered);
}

#[test]
fn overloaded_resend_is_skipped_once_disconnected() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.disconnect(1);
    h.advance(MS * 2000);
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn a_replaced_drain_with_no_known_server_id_has_none() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("SERVER_DISABLED", json!({}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    assert_eq!(h.disabled(), [(None, "disabled".into())]);
    assert!(h.drain(1, DrainReason::Replaced));
    assert_eq!(
        h.conflicts(),
        [Conflict {
            server_id: None,
            name: "anki".into(),
            reason: "replaced".into()
        }]
    );
}

#[test]
fn unsupported_version_is_fatal_other_app_errors_are_not() {
    let mut h = H::new();
    h.welcome(1);
    for code in [
        "CONNECTION_LIMIT",
        "TOO_MANY_SERVICES",
        "UNKNOWN_OP",
        "BAD_REQUEST",
        "WHATEVER",
    ] {
        h.app(json!({"op": "error", "code": code}));
    }
    assert!(h.fatals.is_empty());
    assert_eq!(
        h.errors().into_iter().map(|e| e.0).collect::<Vec<_>>(),
        [
            "CONNECTION_LIMIT",
            "TOO_MANY_SERVICES",
            "UNKNOWN_OP",
            "BAD_REQUEST",
            "WHATEVER"
        ]
    );
    h.app(json!({"op": "error", "code": "UNSUPPORTED_VERSION", "message": "v2 only"}));
    assert_eq!(h.closed, [Closed::Fatal("UNSUPPORTED_VERSION".into())]);
    assert!(h.fatals[0].fatal);
}

#[test]
fn a_fatal_while_closing_changes_nothing() {
    // A second UNSUPPORTED_VERSION during the fatal close: one Fatal.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    h.app(json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    assert_eq!(h.closed, [Closed::Fatal("UNSUPPORTED_VERSION".into())]);
    assert_eq!(h.fatals.len(), 1);

    // During a user close (unregister wait): the close stays error-free; the
    // error answers the unregister, ending the wait.
    let mut h = H::new();
    h.welcome(1);
    h.registered(true);
    h.close();
    assert_eq!(h.s.state(), ExposeState::Closing);
    h.app(json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    assert!(h.fatals.is_empty());
    assert_eq!(h.closed, [Closed::Graceful]);
    assert_eq!(h.s.state(), ExposeState::Closing);
}

#[test]
fn a_malformed_envelope_is_a_non_fatal_error() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "registered", "services": [{"name": "anki", "id": "srv", "url": ""}]}));
    assert_eq!(
        h.errors(),
        [("MALFORMED_ENVELOPE".to_owned(), ErrorOrigin::Malformed)]
    );
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.closed.is_empty());
}

#[test]
fn unknown_version_and_ops_are_ignored() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"v": 2, "op": "conflict", "id": "srv_anki", "name": "anki"}));
    h.app(json!({"op": "frobnicate"}));
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.errors().is_empty() && h.conflicts().is_empty());
}

#[test]
fn close_sends_unregister_and_ends_on_unregistered_for_our_name() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    assert_eq!(h.s.state(), ExposeState::Closing);
    assert_eq!(h.ops(), ["register", "unregister"]);
    assert_eq!(
        h.sent[1],
        json!({"v": 1, "op": "unregister", "services": [{"name": "anki"}]})
    );
    assert!(h.closed.is_empty());
    // Any unregistered ends the wait while one is outstanding.
    h.app(json!({"op": "unregistered", "services": [{"name": "other", "id": "x"}]}));
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty());
    h.closed_client();
    assert_eq!(h.s.state(), ExposeState::Closed);
    assert_eq!(h.states().last(), Some(&"closed"));
}

#[test]
fn close_ends_after_the_2s_budget_without_unregistered() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    h.advance(MS * 1999);
    assert!(h.closed.is_empty());
    h.advance(MS);
    assert_eq!(h.closed, [Closed::Graceful]);
    // A late unregistered changes nothing.
    h.app(json!({"op": "unregistered", "services": [{"name": "anki"}]}));
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn a_failed_unregister_send_ends_the_wait_at_once() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.send_fails = true;
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty());
}

#[test]
fn close_sends_nothing_while_disconnected_and_ignores_later_registrations() {
    let mut h = H::new();
    h.welcome(1);
    h.disconnect(1);
    h.close();
    assert_eq!(h.ops(), ["register"]);
    assert_eq!(h.closed, [Closed::Graceful]);
    h.welcome(2);
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Closing);
    assert_eq!(h.ops(), ["register"]);
    // Idempotent.
    h.close();
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn close_unregisters_iff_register_went_out_on_the_live_connection() {
    // Sent but not answered yet: still unregisters.
    let mut h = H::new();
    h.welcome(1);
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);

    // Never welcomed: nothing to unregister.
    let mut h = H::new();
    h.close();
    assert!(h.ops().is_empty());
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn overloaded_unregister_is_resent_once_while_closing() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    assert!(
        h.retries().is_empty(),
        "onRetry is for register re-sends only"
    );
    h.advance(MS * 1000);
    assert_eq!(h.ops(), ["register", "unregister", "unregister"]);
    h.advance(MS * 1000);
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn a_disconnect_during_a_paused_unregister_resend_sends_nothing() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.disconnect(1);
    h.advance(MS * 1500);
    assert_eq!(h.ops(), ["register", "unregister"]);
    h.advance(MS * 500);
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn a_replacement_welcomed_during_a_paused_unregister_resend_gets_nothing() {
    // §3.2: no register went out on the replacement, so no unregister.
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.drain(1, DrainReason::Rollout);
    h.welcome(2);
    h.advance(MS * 1500);
    assert_eq!(h.ops(), ["register", "unregister"]);
    h.advance(MS * 500);
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn the_unregister_budget_cancels_a_resend_paused_past_it() {
    // n = 3 at close, 4 for the unregister's OVERLOADED: its pause is
    // 8000 ms, past the 2000 ms budget (§3.2 Unregister).
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    assert_eq!(overloaded_resend_after(&mut h), 2000);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.close();
    let at_close = h.ops().len();
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.advance(MS * 2000);
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty());
    h.advance(MS * 10_000);
    assert_eq!(
        h.ops().len(),
        at_close,
        "no unregister re-send after the budget"
    );
}

#[test]
fn overloaded_dropping_the_register_ahead_counts_so_the_unregister_pause_is_2000ms() {
    // §2.6: n counts every consecutive OVERLOADED on the connection, this one
    // included; the first dropped the register ahead (n = 1), the second the
    // unregister (n = 2: min(1000 * 2^1, 30000) = 2000 ms).
    let mut h = H::new();
    h.welcome(1);
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    let pause = h
        .timers
        .iter()
        .find(|(t, _)| t.kind() == TimerKind::Overload)
        .map(|(_, at)| *at - h.now);
    assert_eq!(pause, Some(MS * 2000));
}

#[test]
fn an_error_op_during_a_paused_unregister_does_not_end_the_close_wait() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.errors(), [("UNKNOWN_OP".to_owned(), ErrorOrigin::App)]);
    assert!(h.closed.is_empty());
    h.advance(MS * 999);
    assert_eq!(h.ops(), ["register", "unregister"]);
    h.advance(MS);
    assert_eq!(h.ops(), ["register", "unregister", "unregister"]);
    h.app(json!({"op": "unregistered"}));
    assert_eq!(h.closed, [Closed::Graceful]);
}

#[test]
fn conflict_for_our_name_or_id_is_sticky() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "conflict", "id": "srv_other", "name": "other", "reason": "taken_over"}));
    assert_eq!(h.s.state(), ExposeState::Registered);
    h.reg_error("UNAVAILABLE", json!({})); // arms a retry timer
    h.app(json!({"op": "conflict", "id": "srv_anki", "name": "anki", "reason": "taken_over"}));
    assert_eq!(h.s.state(), ExposeState::Conflict);
    assert!(h.s.is_taken_over());
    assert_eq!(
        h.conflicts(),
        [Conflict {
            server_id: Some("srv_anki".into()),
            name: "anki".into(),
            reason: "taken_over".into()
        }]
    );
    assert!(h.timers.is_empty());
    assert_eq!(h.closed, [Closed::Immediate(TAKEN_OVER_MESSAGE.into())]);
    h.app(json!({"op": "conflict", "id": "srv_anki", "name": "anki", "reason": "taken_over"}));
    assert_eq!(h.conflicts().len(), 1);

    h.disconnect(1);
    assert_eq!(h.s.state(), ExposeState::Conflict);
    h.welcome(2);
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.registered(false);
    h.app(json!({"op": "error", "code": "OVERLOADED"}));
    h.advance(MS * 60_000);
    assert_eq!(h.s.state(), ExposeState::Conflict);
    assert_eq!(h.ops(), ["register"]);
    assert!(h.enabled().is_empty() && h.disabled().is_empty());
    // Last values are kept (§3.3).
    assert_eq!(h.s.url(), Some("https://anki-u.x/mcp"));
    assert_eq!(h.registrations().len(), 1);

    h.close();
    assert_eq!(h.ops(), ["register"], "no unregister after a take-over");
    h.closed_client();
    assert_eq!(
        h.states(),
        ["registering", "registered", "conflict", "closing", "closed"]
    );
}

#[test]
fn conflict_matches_by_id_when_the_name_differs() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "conflict", "id": "srv_anki", "name": "renamed"}));
    assert_eq!(h.s.state(), ExposeState::Conflict);
    assert_eq!(
        h.conflicts(),
        [Conflict {
            server_id: Some("srv_anki".into()),
            name: "renamed".into(),
            reason: "taken_over".into()
        }]
    );
}

#[test]
fn conflict_before_any_registration_matches_by_name_only() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "conflict", "id": "srv_x", "name": "other"}));
    assert_eq!(h.s.state(), ExposeState::Registering);
    h.app(json!({"op": "conflict", "id": "srv_x", "name": "anki"}));
    assert_eq!(h.s.state(), ExposeState::Conflict);
    assert!(!h.s.ever_registered());
}

// --- §2.7 acceptance by state ---------------------------------------------------

#[test]
fn nothing_is_accepted_while_closing_except_unregistered_and_errors() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.close();
    h.registered(true);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.app(json!({"op": "conflict", "id": "srv_anki", "name": "anki"}));
    assert!(!h.drain(1, DrainReason::Replaced));
    assert_eq!(h.s.state(), ExposeState::Closing);
    assert_eq!(h.registrations().len(), 1);
    assert!(h.disabled().is_empty() && h.enabled().is_empty() && h.conflicts().is_empty());
    assert!(h.closed.is_empty(), "registered does not end the wait");
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.errors(), [("UNKNOWN_OP".to_owned(), ErrorOrigin::App)]);
    // Nothing ahead of the unregister: the error answers it.
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty());
}

#[test]
fn close_wait_follows_frame_order_when_a_register_is_still_ahead() {
    // An error op answers the register first; the next one the unregister.
    let mut h = H::new();
    h.welcome(1);
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);
    h.app(json!({"op": "error", "code": "TOO_MANY_SERVICES"}));
    assert!(h.closed.is_empty(), "it answered the register");
    h.app(json!({"op": "error", "code": "SOMETHING_NEW"}));
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty());

    // A registered answers the register; the wait goes on until an error op.
    let mut h = H::new();
    h.welcome(1);
    h.close();
    h.registered(true);
    assert!(h.closed.is_empty());
    assert!(
        h.registrations().is_empty(),
        "no registration while closing"
    );
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.closed, [Closed::Graceful]);

    // Or until unregistered, as before.
    let mut h = H::new();
    h.welcome(1);
    h.close();
    h.app(json!({"op": "unregistered"}));
    assert_eq!(h.closed, [Closed::Graceful]);

    // A register only ever dropped by OVERLOADED is not ahead, and there is
    // nothing to unregister: the close is immediate.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.close();
    assert_eq!(h.ops(), ["register"]);
    assert_eq!(h.closed, [Closed::Graceful]);
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.closed, [Closed::Graceful]);

    // OVERLOADED after close drops the register ahead: abandoned, nothing
    // re-sent, and the next error answers the unregister.
    let mut h = H::new();
    h.welcome(1);
    h.close();
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    let sent = h.sent.len();
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.closed, [Closed::Graceful]);
    h.advance(MS * 30_000);
    assert_eq!(h.sent.len(), sent, "nothing sent after the OVERLOADED");
    assert_eq!(h.ops(), ["register", "unregister"]);
}

#[test]
fn close_skips_the_unregister_when_every_register_was_dropped_by_overloaded() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.close();
    assert_eq!(h.ops(), ["register"]);
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty(), "no unregister budget");

    // Re-sent and dropped again.
    let mut h = H::new();
    h.welcome(1);
    assert_eq!(overloaded_resend_after(&mut h), 1000);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.close();
    assert_eq!(h.ops(), ["register", "register"]);
    assert_eq!(h.closed, [Closed::Graceful]);
    assert!(h.timers.is_empty());
}

#[test]
fn close_unregisters_after_any_reply_to_a_register() {
    // A whole-frame error op answered the register.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);
    assert!(h.closed.is_empty());

    // An empty registered.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "registered"}));
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);

    // A registered naming the service during the OVERLOADED pause.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.registered(true);
    assert!(h.timers.is_empty(), "the re-send is cancelled");
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);

    // Registered earlier on this connection, the re-register dropped.
    let mut h = H::new();
    h.welcome(1);
    h.registered(true);
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    h.close();
    assert_eq!(h.ops(), ["register", "register", "unregister"]);
    h.advance(MS * 5000);
    assert_eq!(
        h.ops(),
        ["register", "register", "unregister"],
        "the dropped register is not re-sent"
    );
}

#[test]
fn a_registered_from_another_connection_before_any_welcome_is_ignored() {
    let mut h = H::new();
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Connecting);
    assert_eq!(h.s.url(), None);
    assert_eq!(h.s.server_id(), None);
    assert!(h.registrations().is_empty());
}

#[test]
fn registered_is_accepted_while_disabled_and_resets_backoff() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.reg_error("INTERNAL", json!({}));
    h.reg_error("SERVER_DISABLED", json!({"id": "srv_anki"}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Registered);
    h.reg_error("INTERNAL", json!({}));
    assert_eq!(h.retries().last().unwrap().0.delay, MS * 1000);
}

fn retry_delays(h: &H) -> Vec<u128> {
    h.retries()
        .iter()
        .map(|(r, _)| r.delay.as_millis())
        .collect()
}

#[test]
fn a_non_transient_error_entry_resets_the_transient_backoff() {
    // §3.2: a SERVER_DISABLED entry is a successful reply for the counter.
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.reg_error("INTERNAL", json!({}));
    h.reg_error("SERVER_DISABLED", json!({"id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.reg_error("INTERNAL", json!({}));
    assert_eq!(retry_delays(&h), [1000, 2000, 1000]);

    // So is a refusal on a re-register.
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.reg_error("INTERNAL", json!({}));
    h.advance(MS * 1000);
    h.reg_error("FORBIDDEN", json!({}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.reg_error("INTERNAL", json!({}));
    assert_eq!(h.ops().len(), 4);
    assert_eq!(retry_delays(&h), [1000, 1000]);
    assert!(h.fatals.is_empty());
}

#[test]
fn a_whole_frame_error_leaves_the_transient_backoff_alone() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    h.advance(MS * 1000);
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    h.reg_error("INTERNAL", json!({}));
    assert_eq!(h.ops().len(), 3);
    assert_eq!(retry_delays(&h), [1000, 2000]);
}

// --- §3.2 register on every welcome, incl. a drain replacement --------------------

#[test]
fn a_drain_replacement_registers_on_its_welcome() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    assert!(!h.drain(1, DrainReason::Rollout));
    assert_eq!(h.s.state(), ExposeState::Registered, "T31: unchanged");
    h.welcome(2);
    assert_eq!(h.ops(), ["register", "register"]);
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.s.is_superseded(c(1)));
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Registered);
}

#[test]
fn a_drain_replacements_welcome_cancels_an_overloaded_resend_paused_on_the_drained_connection() {
    // §2.6: the pause and its counter belong to connection 1; the welcome of
    // connection 2 starts fresh with its own register.
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
    let (stale, _) = h.timers[0];
    assert_eq!(stale.kind(), TimerKind::Overload);
    assert!(!h.drain(1, DrainReason::Rollout));
    h.welcome(2);
    assert_eq!(h.sent, [register_env(), register_env()]);
    assert!(h.timers.is_empty(), "no timer from connection 1 remains");
    // A firing the driver failed to cancel is ignored too.
    h.s.on_timer(stale);
    h.pump();
    assert_eq!(h.ops(), ["register", "register"]);
    assert_eq!(h.s.state(), ExposeState::Registering);
    // The OVERLOADED counter starts over on connection 2.
    assert_eq!(overloaded_resend_after(&mut h), 1000);
}

#[test]
fn a_stale_transient_retry_from_the_drained_connection_does_not_fire_on_the_replacement() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    let (stale, _) = h.timers[0];
    assert_eq!(stale.kind(), TimerKind::RegisterRetry);
    assert!(!h.drain(1, DrainReason::Rollout));
    h.welcome(2);
    assert_eq!(h.sent, [register_env(), register_env()]);
    assert!(h.timers.is_empty(), "no timer from connection 1 remains");
    h.s.on_timer(stale);
    h.pump();
    assert_eq!(h.ops(), ["register", "register"]);
}

// --- §5.9 / §6.8 connection identity ---------------------------------------------

#[test]
fn t38_a_superseded_connection_end_changes_nothing_even_while_registering() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.drain(1, DrainReason::Rollout);
    h.welcome(2);
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.disconnect(1), "superseded");
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert_eq!(h.ops(), ["register", "register"]);
    // Still superseded once it ended: its late ops stay ignored.
    assert!(h.s.is_superseded(c(1)));
}

#[test]
fn t38_messages_on_a_superseded_connection_are_ignored() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.drain(1, DrainReason::Rollout);
    h.welcome(2);
    for env in [
        json!({"op": "registered", "services": [{"name": "anki", "id": "srv_anki", "url": "https://old/mcp"}]}),
        json!({"op": "disable", "id": "srv_anki"}),
        json!({"op": "enable", "id": "srv_anki", "name": "anki"}),
        json!({"op": "conflict", "id": "srv_anki", "name": "anki"}),
        json!({"op": "error", "code": "UNSUPPORTED_VERSION"}),
    ] {
        h.app_on(1, env);
    }
    assert!(!h.drain(1, DrainReason::Replaced));
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert_eq!(h.ops(), ["register", "register"]);
    assert_eq!(h.registrations().len(), 1);
    assert!(h.closed.is_empty() && h.errors().is_empty());
    // The replacement's own messages count.
    h.app_on(2, json!({"op": "registered", "services": [{"name": "anki", "id": "srv_anki", "url": "https://new/mcp"}]}));
    assert_eq!(h.s.url(), Some("https://new/mcp"));
}

#[test]
fn t44_a_drained_connection_ending_before_its_replacement_welcome_is_a_lost_connection() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.drain(1, DrainReason::Rollout);
    assert!(!h.disconnect(1));
    assert_eq!(h.s.state(), ExposeState::Connecting);
    // The replacement then welcomes and registers.
    h.welcome(2);
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn t29_a_replacement_dial_failing_while_the_drained_connection_serves_changes_nothing() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.drain(1, DrainReason::Rollout);
    assert!(h.disconnect(2), "replacement dial 2 failed");
    assert!(h.disconnect(3), "replacement dial 3 failed");
    assert_eq!(h.s.state(), ExposeState::Registered);
    // The drained connection keeps serving; its app messages still count.
    h.app_on(1, json!({"op": "disable", "id": "srv_anki"}));
    assert_eq!(h.s.state(), ExposeState::Disabled);
    // Its end is then T44.
    assert!(!h.disconnect(1));
    assert_eq!(h.s.state(), ExposeState::Connecting);
    // Now a failing dial is an ordinary one.
    assert!(!h.disconnect(4));
}

#[test]
fn t30_the_live_connection_ending_is_a_lost_connection() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    assert!(!h.disconnect(1));
    assert_eq!(h.s.state(), ExposeState::Connecting);
    // A failed dial afterwards changes nothing further.
    assert!(!h.disconnect(2));
    assert_eq!(h.s.state(), ExposeState::Connecting);
}

#[test]
fn an_unknown_connection_ending_while_another_is_live_changes_nothing() {
    // A shared client: conn 1 drained and ended before this session existed
    // to see it; the session was welcomed on conn 2.
    let mut h = H::new();
    h.welcome(2);
    h.registered(true);
    assert!(h.disconnect(1));
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.s.connected);
    assert_eq!(h.states(), ["registering", "registered"]);
    // Still connected: close unregisters.
    h.close();
    assert_eq!(h.ops(), ["register", "unregister"]);
}

#[test]
fn an_unknown_connection_ending_keeps_a_pending_register_retry() {
    let mut h = H::new();
    h.welcome(2);
    h.reg_error("INTERNAL", json!({}));
    assert_eq!(h.timers.len(), 1);
    assert!(h.disconnect(1));
    assert_eq!(h.timers.len(), 1);
    assert_eq!(h.s.state(), ExposeState::Registering);
    h.advance(MS * 1000);
    assert_eq!(h.ops(), ["register", "register"]);
}

#[test]
fn the_live_connection_ending_after_an_ignored_unknown_one_is_a_lost_connection() {
    let mut h = H::new();
    h.welcome(2);
    h.registered(true);
    assert!(h.disconnect(1));
    assert!(!h.disconnect(2));
    assert_eq!(h.s.state(), ExposeState::Connecting);
    assert!(!h.s.connected);
}

#[test]
fn conflict_stays_conflict_across_disconnects() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.app(json!({"op": "conflict", "id": "srv_anki", "name": "anki"}));
    h.disconnect(1);
    assert_eq!(h.s.state(), ExposeState::Conflict);
}

// --- §2.7 Ended connection (T51) -------------------------------------------------

#[test]
fn t51_registered_disable_enable_after_their_connection_ended_are_ignored() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(true);
    assert!(!h.disconnect(1));
    assert_eq!(h.s.state(), ExposeState::Connecting);
    for env in [
        json!({"op": "disable", "id": "srv_anki", "reason": "disabled"}),
        json!({"op": "registered", "services": [{"name": "anki", "id": "srv_anki", "url": "https://old/mcp"}]}),
        json!({"op": "enable", "id": "srv_anki", "name": "anki"}),
    ] {
        h.app_on(1, env);
    }
    assert_eq!(h.s.state(), ExposeState::Connecting);
    assert!(h.disabled().is_empty() && h.enabled().is_empty());
    assert_eq!(h.registrations().len(), 1);
    assert_eq!(h.ops(), ["register"]);
    // The reconnect is unaffected.
    h.welcome(2);
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Registered);
}

#[test]
fn t51_a_conflict_after_its_connection_ended_still_applies() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(true);
    assert!(!h.disconnect(1));
    h.app_on(
        1,
        json!({"op": "conflict", "id": "srv_anki", "name": "anki"}),
    );
    assert_eq!(h.s.state(), ExposeState::Conflict);
    assert_eq!(h.conflicts().len(), 1);
    assert_eq!(
        h.closed,
        [Closed::Immediate(TAKEN_OVER_MESSAGE.into())],
        "no reconnect"
    );
    h.welcome(2);
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn t51_an_error_after_its_connection_ended_still_applies() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(true);
    assert!(!h.disconnect(1));
    h.app_on(
        1,
        json!({"op": "error", "code": "UNKNOWN_OP", "message": "x"}),
    );
    assert_eq!(h.errors(), [("UNKNOWN_OP".to_owned(), ErrorOrigin::App)]);
    h.app_on(1, json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    assert_eq!(h.closed, [Closed::Fatal("UNSUPPORTED_VERSION".into())]);
}

#[test]
fn t51_a_superseded_connection_stays_ignored_after_it_ended() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.drain(1, DrainReason::Rollout);
    h.welcome(2);
    assert!(h.disconnect(1));
    h.app_on(
        1,
        json!({"op": "conflict", "id": "srv_anki", "name": "anki"}),
    );
    h.app_on(1, json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    assert_eq!(h.s.state(), ExposeState::Registering);
    assert!(h.closed.is_empty() && h.conflicts().is_empty());
}

#[test]
fn t51_a_drained_connection_ended_before_the_welcome_counts_as_ended() {
    let mut h = H::new();
    h.welcome(1);
    h.registered(false);
    h.drain(1, DrainReason::Rollout);
    assert!(!h.disconnect(1), "T44");
    h.app_on(1, json!({"op": "disable", "id": "srv_anki"}));
    assert_eq!(h.s.state(), ExposeState::Connecting);
    h.app_on(
        1,
        json!({"op": "conflict", "id": "srv_anki", "name": "anki"}),
    );
    assert_eq!(h.s.state(), ExposeState::Conflict);
}

#[test]
fn ended_connections_are_remembered_up_to_a_bound() {
    let mut h = H::new();
    for n in 1..=(ENDED_MEMORY as u64 + 4) {
        h.welcome(n);
        h.disconnect(n);
    }
    assert_eq!(h.s.ended.len(), ENDED_MEMORY);
    // Evicted, yet still ended: that follows from the newest welcome.
    assert!(h.s.has_ended(c(1)));
    assert!(h.s.has_ended(c(ENDED_MEMORY as u64 + 4)));
    assert!(!h.s.has_ended(c(ENDED_MEMORY as u64 + 5)));
}

#[test]
fn t51_a_connection_below_the_live_one_never_welcomed_here_counts_as_ended() {
    // A shared client: conn 1 ended before this session existed; its late
    // ops must not touch the session welcomed on conn 2.
    let mut h = H::new();
    h.welcome(2);
    h.registered(true);
    h.app_on(1, json!({"op": "disable", "id": "srv_anki"}));
    h.app_on(1, json!({"op": "registered", "services": [{"name": "anki", "id": "srv_x", "url": "https://old/mcp"}]}));
    h.app_on(1, json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert_eq!(h.s.server_id(), Some("srv_anki"));
    assert!(h.disabled().is_empty() && h.enabled().is_empty());
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn stale_timer_firings_are_ignored() {
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("INTERNAL", json!({}));
    let (stale, _) = h.timers[0];
    h.reg_error("INTERNAL", json!({})); // replaces the timer
    h.s.on_timer(stale);
    h.pump();
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn a_registered_cancels_a_pending_transient_retry() {
    // The reply to the first register arrives just as its retry is due.
    let mut h = H::new();
    h.welcome(1);
    h.reg_error("TIMEOUT", json!({}));
    h.advance(MS * 999);
    h.registered(true);
    assert!(h.timers.is_empty());
    h.advance(MS * 60_000);
    assert_eq!(h.ops(), ["register"]);
}

#[test]
fn register_retry_delay_saturates() {
    assert_eq!(register_retry_delay(0), MS * 1000);
    assert_eq!(register_retry_delay(4), MS * 16_000);
    assert_eq!(register_retry_delay(5), MS * 30_000);
    assert_eq!(register_retry_delay(40), MS * 30_000);
}

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
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "other"}));
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
    assert_eq!(h.ops(), ["register", "register"]);
    h.registered(false);
    assert_eq!(h.s.state(), ExposeState::Registered);
    assert!(h.disabled().is_empty());
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
fn a_held_disable_survives_a_drain_hand_over() {
    let mut h = H::new();
    h.welcome(1);
    h.app(json!({"op": "disable", "id": "srv_anki", "reason": "maintenance"}));
    assert!(!h.drain(1, DrainReason::Rollout));
    h.welcome(2);
    h.registered(true);
    assert_eq!(h.s.state(), ExposeState::Disabled);
    assert_eq!(
        h.disabled(),
        [(Some("srv_anki".into()), "maintenance".into())]
    );
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

#[test]
fn overloaded_attempt_counts_consecutive_resends_and_resets_on_success() {
    let mut h = H::new();
    h.welcome(1);
    let overloaded = |h: &mut H| {
        h.app(json!({"op": "error", "code": "OVERLOADED", "message": "busy"}));
        h.advance(MS * 1000);
    };
    overloaded(&mut h);
    overloaded(&mut h);
    overloaded(&mut h);
    h.registered(false);
    h.app(json!({"op": "disable", "id": "srv_anki"}));
    h.app(json!({"op": "enable", "id": "srv_anki", "name": "anki"}));
    overloaded(&mut h);
    assert_eq!(
        h.retries()
            .iter()
            .map(|(r, _)| r.attempt)
            .collect::<Vec<_>>(),
        [1, 2, 3, 1]
    );
    assert_eq!(h.ops().len(), 6);
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

    // During a user close (unregister wait): the close stays error-free.
    let mut h = H::new();
    h.welcome(1);
    h.registered(true);
    h.close();
    assert_eq!(h.s.state(), ExposeState::Closing);
    h.app(json!({"op": "error", "code": "UNSUPPORTED_VERSION"}));
    assert!(h.fatals.is_empty() && h.closed.is_empty());
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
    h.app(json!({"op": "error", "code": "UNKNOWN_OP"}));
    assert_eq!(h.errors(), [("UNKNOWN_OP".to_owned(), ErrorOrigin::App)]);
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
    // Its id is forgotten once it ended.
    assert!(!h.s.is_superseded(c(1)));
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
fn register_retry_delay_saturates() {
    assert_eq!(register_retry_delay(0), MS * 1000);
    assert_eq!(register_retry_delay(4), MS * 16_000);
    assert_eq!(register_retry_delay(5), MS * 30_000);
    assert_eq!(register_retry_delay(40), MS * 30_000);
}

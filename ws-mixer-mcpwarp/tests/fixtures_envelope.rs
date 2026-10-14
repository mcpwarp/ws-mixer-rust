//! `spec/fixtures/mcpwarp/envelope` (MCPWARP.md §10.2), as an agent runner:
//!
//! - `register` / `unregister` valid cases: the encoder's output equals
//!   `message`, byte for byte.
//! - Edge -> agent cases: `wire_valid: true` decodes without error, with the
//!   §2.3 defaults; `MALFORMED_ENVELOPE` fails with a message containing
//!   `message_contains`; `UNSUPPORTED_VERSION` / `UNKNOWN_OP` decode as
//!   unknown.
//! - Agent -> edge invalid cases are the edge runner's (the edge's answer);
//!   they are listed and skipped.

mod common;

use serde_json::{Map, Value};
use ws_mixer_mcpwarp::{
    AppErrorCode, InboundEnvelope, McpwarpErrorCode, RegisterErrorCode, RegisteredService,
    ServiceError, ServiceKind, UnregisteredService, decode_envelope, encode_register,
    encode_unregister, is_valid_service_name,
};

/// Floors for the pinned spec (v0.6.1): raise them as the corpus grows.
const MIN_ENCODED: usize = 3;
const MIN_DECODED: usize = 43;

#[test]
fn envelope_fixtures() {
    let Some(root) = common::mcpwarp_fixtures() else {
        return;
    };
    let mut encoded = 0;
    let mut decoded = 0;
    let mut names_checked = 0;
    let mut noted = Vec::new();
    for bucket in ["valid", "invalid"] {
        for path in common::json_files(&root.join("envelope").join(bucket)) {
            let f = common::read_json(&path);
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let op = f["op"].as_str().unwrap_or_else(|| panic!("{name}: no op"));
            let wire_valid = f["wire_valid"].as_bool().unwrap();
            let schema_valid = f["schema_valid"].as_bool().unwrap();
            match op {
                "register" | "unregister" if schema_valid && wire_valid => {
                    check_encoder(&name, op, &f["message"]);
                    encoded += 1;
                }
                "register" | "unregister" => {
                    // The edge's INVALID_NAME is the edge runner's to check;
                    // the agent refuses those names before dialing.
                    if f["expect"]["error_code"] == "INVALID_NAME" {
                        let svc = f["message"]["services"][0]["name"].as_str().unwrap();
                        assert!(!is_valid_service_name(svc), "{name}: {svc:?} passes");
                        names_checked += 1;
                    }
                    noted.push(name);
                }
                _ => {
                    check_decoder(&name, op, &f);
                    decoded += 1;
                }
            }
        }
    }
    for name in &noted {
        println!("note: envelope/{name}: agent -> edge invalid case, edge runner only; skipped");
    }
    println!(
        "envelope {} ({} asserted, {} noted)",
        encoded + decoded + noted.len(),
        encoded + decoded,
        noted.len()
    );
    assert!(encoded >= MIN_ENCODED, "only {encoded} encoder cases ran");
    assert!(decoded >= MIN_DECODED, "only {decoded} decoder cases ran");
    // register_name_{too_long,trailing_dash,uppercase}.
    assert!(
        names_checked >= 3,
        "only {names_checked} INVALID_NAME names checked"
    );
    println!("  of the noted, {names_checked} INVALID_NAME names fail is_valid_service_name");
}

fn check_encoder(name: &str, op: &str, message: &Value) {
    let svc = &message["services"][0];
    let svc_name = svc["name"].as_str().unwrap();
    let body = if op == "register" {
        let kind = match svc["kind"].as_str().unwrap() {
            "http" => ServiceKind::Http,
            "stdio" => ServiceKind::Stdio,
            k => panic!("{name}: kind {k}"),
        };
        encode_register(svc_name, kind)
    } else {
        encode_unregister(svc_name)
    };
    assert_eq!(
        serde_json::to_string(&body["mcpwarp"]).unwrap(),
        serde_json::to_string(message).unwrap(),
        "{name}: encoder bytes"
    );
    assert_eq!(body.as_object().unwrap().len(), 1, "{name}: only `mcpwarp`");
}

fn check_decoder(name: &str, op: &str, f: &Value) {
    let message = &f["message"];
    let mut body = Map::new();
    body.insert("mcpwarp".into(), message.clone());
    let got = decode_envelope(&body);
    if f["wire_valid"].as_bool().unwrap() {
        let env = got
            .unwrap_or_else(|e| panic!("{name}: decode failed: {e}"))
            .unwrap_or_else(|| panic!("{name}: decoded as not ours"));
        assert_eq!(env, expected(op, message.as_object().unwrap()), "{name}");
        return;
    }
    let expect = &f["expect"];
    let code = expect["error_code"].as_str().unwrap();
    match code {
        "MALFORMED_ENVELOPE" => {
            let e = got.expect_err(&format!("{name}: decoded, want MALFORMED_ENVELOPE"));
            assert_eq!(e.code, McpwarpErrorCode::MalformedEnvelope, "{name}");
            assert!(!e.fatal, "{name}: MALFORMED_ENVELOPE is non-fatal");
            if let Some(want) = expect["message_contains"].as_str() {
                assert!(
                    e.message.contains(want),
                    "{name}: {:?} does not contain {want:?}",
                    e.message
                );
            }
        }
        "UNSUPPORTED_VERSION" | "UNKNOWN_OP" => {
            let env = got.unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(
                matches!(env, Some(InboundEnvelope::Unknown { .. })),
                "{name}: want unknown, got {env:?}"
            );
        }
        other => panic!("{name}: unexpected expect.error_code {other} for a wire-invalid case"),
    }
}

/// What `message` must decode to: present fields as given, absent ones per
/// the §2.3 defaults. Only called for wire-valid messages.
fn expected(op: &str, m: &Map<String, Value>) -> InboundEnvelope {
    let s = |o: &Map<String, Value>, k: &str| o.get(k).and_then(Value::as_str).map(str::to_owned);
    let list = |k: &str| -> Vec<Map<String, Value>> {
        m.get(k)
            .and_then(Value::as_array)
            .map(|a| a.iter().map(|v| v.as_object().unwrap().clone()).collect())
            .unwrap_or_default()
    };
    match op {
        "registered" => InboundEnvelope::Registered {
            services: list("services")
                .iter()
                .map(|o| RegisteredService {
                    name: s(o, "name").unwrap(),
                    id: s(o, "id").unwrap(),
                    url: s(o, "url").unwrap(),
                    created: o.get("created").and_then(Value::as_bool).unwrap_or(false),
                })
                .collect(),
            errors: list("errors")
                .iter()
                .map(|o| ServiceError {
                    name: s(o, "name").unwrap(),
                    code: RegisterErrorCode::from_wire(&s(o, "code").unwrap()),
                    message: s(o, "message").unwrap_or_default(),
                    id: s(o, "id"),
                })
                .collect(),
        },
        "unregistered" => InboundEnvelope::Unregistered {
            services: list("services")
                .iter()
                .map(|o| UnregisteredService {
                    name: s(o, "name").unwrap(),
                    id: s(o, "id").unwrap_or_default(),
                })
                .collect(),
        },
        "disable" => InboundEnvelope::Disable {
            id: s(m, "id").unwrap(),
            reason: s(m, "reason").unwrap_or_else(|| "disabled".into()),
        },
        "enable" => InboundEnvelope::Enable {
            id: s(m, "id").unwrap(),
            name: s(m, "name").unwrap(),
        },
        "conflict" => InboundEnvelope::Conflict {
            id: s(m, "id").unwrap(),
            name: s(m, "name").unwrap(),
            reason: s(m, "reason").unwrap_or_else(|| "taken_over".into()),
        },
        "error" => InboundEnvelope::Error {
            code: AppErrorCode::from_wire(&s(m, "code").unwrap()),
            message: s(m, "message").unwrap_or_default(),
        },
        other => panic!("op {other}"),
    }
}

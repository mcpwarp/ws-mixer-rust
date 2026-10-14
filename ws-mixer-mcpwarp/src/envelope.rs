//! The `app.body.mcpwarp` envelope (MCPWARP.md §2.2-§2.8): what the agent
//! sends (`register`, `unregister`) and what the edge sends back.

use serde_json::{Map, Value, json};

use crate::MCPWARP_V;
use crate::codes::{AppErrorCode, RegisterErrorCode};
use crate::error::McpwarpError;
use crate::names::ServiceKind;

/// A `registered.services[]` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredService {
    /// The service name.
    pub name: String,
    /// The server id.
    pub id: String,
    /// The public URL; never empty.
    pub url: String,
    /// `true` only when this `register` created the service.
    pub created: bool,
}

/// A `registered.errors[]` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceError {
    /// The service name.
    pub name: String,
    /// Why the register failed.
    pub code: RegisterErrorCode,
    /// The edge's message; `""` when absent.
    pub message: String,
    /// The server id, carried by `SERVER_DISABLED`; `None` when absent.
    pub id: Option<String>,
}

/// An `unregistered.services[]` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnregisteredService {
    /// The service name.
    pub name: String,
    /// The server id; `""` when absent.
    pub id: String,
}

/// A decoded edge -> agent envelope, with the §2.3 defaults applied.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum InboundEnvelope {
    /// The reply to `register`.
    Registered {
        /// Successful registrations.
        services: Vec<RegisteredService>,
        /// Refusals.
        errors: Vec<ServiceError>,
    },
    /// The reply to `unregister`.
    Unregistered {
        /// The services that were held; may be empty.
        services: Vec<UnregisteredService>,
    },
    /// The service was disabled in the dashboard.
    Disable {
        /// The server id.
        id: String,
        /// Why; `"disabled"` when absent.
        reason: String,
    },
    /// The service was re-enabled.
    Enable {
        /// The server id.
        id: String,
        /// The service name.
        name: String,
    },
    /// Another connection took the name over (MCPWARP.md §3.3).
    Conflict {
        /// The server id.
        id: String,
        /// The service name.
        name: String,
        /// Why; `"taken_over"` when absent.
        reason: String,
    },
    /// An envelope-level failure.
    Error {
        /// The code.
        code: AppErrorCode,
        /// The edge's message; `""` when absent.
        message: String,
    },
    /// `v` other than 1, or an op this SDK does not know: ignored by the
    /// agent (§2.8 rules 2, 3). `op` and `v` are as received, `Null` when
    /// absent.
    Unknown {
        /// The raw `op`.
        op: Value,
        /// The raw `v`.
        v: Value,
    },
}

/// The `app` body that registers `name`, byte-exact per MCPWARP.md §2.2.
///
/// ```
/// use ws_mixer_mcpwarp::{ServiceKind, encode_register};
///
/// let body = encode_register("anki", ServiceKind::Http);
/// assert_eq!(
///     body.to_string(),
///     r#"{"mcpwarp":{"v":1,"op":"register","services":[{"name":"anki","kind":"http"}]}}"#
/// );
/// ```
pub fn encode_register(name: &str, kind: ServiceKind) -> Value {
    json!({"mcpwarp": {"v": MCPWARP_V, "op": "register", "services": [{"name": name, "kind": kind.as_str()}]}})
}

/// The `app` body that unregisters `name`, byte-exact per MCPWARP.md §2.2.
///
/// ```
/// use ws_mixer_mcpwarp::encode_unregister;
///
/// assert_eq!(
///     encode_unregister("anki").to_string(),
///     r#"{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"anki"}]}}"#
/// );
/// ```
pub fn encode_unregister(name: &str) -> Value {
    json!({"mcpwarp": {"v": MCPWARP_V, "op": "unregister", "services": [{"name": name}]}})
}

/// Decodes an `app` body per MCPWARP.md §2.8.
///
/// - `Ok(None)`: no object-valued `mcpwarp` key; not this layer's message.
/// - `Ok(Some(InboundEnvelope::Unknown { .. }))`: `v` is not 1, or the op is
///   unknown (including the agent's own `register` / `unregister`).
/// - `Err`: a known op with a field of the wrong type, a missing required
///   field, or a `registered` service without a non-empty `url`, in any entry
///   (`MALFORMED_ENVELOPE`, non-fatal). Fields are checked in the spec
///   checker's order, so the first defect named is the checker's.
///
/// ```
/// use serde_json::json;
/// use ws_mixer_mcpwarp::{InboundEnvelope, decode_envelope};
///
/// let body = json!({"mcpwarp": {"v": 1, "op": "disable", "id": "srv_anki"}});
/// let env = decode_envelope(body.as_object().unwrap()).unwrap();
/// assert_eq!(
///     env,
///     Some(InboundEnvelope::Disable { id: "srv_anki".into(), reason: "disabled".into() })
/// );
///
/// let body = json!({"mcpwarp": {"v": 1, "op": "disable"}});
/// let err = decode_envelope(body.as_object().unwrap()).unwrap_err();
/// assert_eq!(err.code.as_str(), "MALFORMED_ENVELOPE");
/// assert_eq!(err.message, "disable.id must be a string");
///
/// assert_eq!(decode_envelope(json!({"other": 1}).as_object().unwrap()).unwrap(), None);
/// ```
pub fn decode_envelope(body: &Map<String, Value>) -> Result<Option<InboundEnvelope>, McpwarpError> {
    let Some(Value::Object(env)) = body.get("mcpwarp") else {
        return Ok(None);
    };
    let unknown = || InboundEnvelope::Unknown {
        op: env.get("op").cloned().unwrap_or(Value::Null),
        v: env.get("v").cloned().unwrap_or(Value::Null),
    };
    // JS `v === 1` also holds for 1.0.
    if env.get("v").and_then(Value::as_f64) != Some(1.0) {
        return Ok(Some(unknown()));
    }
    let decoded = match env.get("op").and_then(Value::as_str) {
        Some("registered") => {
            let mut services = Vec::new();
            for s in list(env, "services", "registered")? {
                let w = "registered.services[]";
                let name = req_str(s, "name", w)?;
                let id = req_str(s, "id", w)?;
                let url = match s.get("url") {
                    Some(Value::String(u)) if !u.is_empty() => u.clone(),
                    _ => {
                        return Err(McpwarpError::malformed(
                            "registered.services[].url must be a non-empty string",
                        ));
                    }
                };
                let created = match s.get("created") {
                    None | Some(Value::Null) => false,
                    Some(Value::Bool(b)) => *b,
                    Some(_) => {
                        return Err(McpwarpError::malformed(
                            "registered.services[].created must be a boolean",
                        ));
                    }
                };
                services.push(RegisteredService {
                    name,
                    id,
                    url,
                    created,
                });
            }
            let mut errors = Vec::new();
            for e in list(env, "errors", "registered")? {
                let w = "registered.errors[]";
                errors.push(ServiceError {
                    name: req_str(e, "name", w)?,
                    code: RegisterErrorCode::from_wire(&req_str(e, "code", w)?),
                    message: opt_str(e, "message", w)?.unwrap_or_default(),
                    id: opt_str(e, "id", w)?,
                });
            }
            InboundEnvelope::Registered { services, errors }
        }
        Some("unregistered") => {
            let mut services = Vec::new();
            for s in list(env, "services", "unregistered")? {
                let w = "unregistered.services[]";
                services.push(UnregisteredService {
                    name: req_str(s, "name", w)?,
                    id: opt_str(s, "id", w)?.unwrap_or_default(),
                });
            }
            InboundEnvelope::Unregistered { services }
        }
        Some("disable") => InboundEnvelope::Disable {
            id: req_str(env, "id", "disable")?,
            reason: opt_str(env, "reason", "disable")?.unwrap_or_else(|| "disabled".into()),
        },
        Some("enable") => InboundEnvelope::Enable {
            id: req_str(env, "id", "enable")?,
            name: req_str(env, "name", "enable")?,
        },
        Some("conflict") => InboundEnvelope::Conflict {
            id: req_str(env, "id", "conflict")?,
            name: req_str(env, "name", "conflict")?,
            reason: opt_str(env, "reason", "conflict")?.unwrap_or_else(|| "taken_over".into()),
        },
        Some("error") => InboundEnvelope::Error {
            code: AppErrorCode::from_wire(&req_str(env, "code", "error")?),
            message: opt_str(env, "message", "error")?.unwrap_or_default(),
        },
        _ => unknown(),
    };
    Ok(Some(decoded))
}

/// A required string (§2.8 rule 5).
fn req_str(o: &Map<String, Value>, key: &str, place: &str) -> Result<String, McpwarpError> {
    match o.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(not_a_string(place, key)),
    }
}

/// An optional string: absent is `None`; anything but a string, `null`
/// included, is malformed (§2.8 rule 5).
fn opt_str(o: &Map<String, Value>, key: &str, place: &str) -> Result<Option<String>, McpwarpError> {
    match o.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(not_a_string(place, key)),
    }
}

fn not_a_string(place: &str, key: &str) -> McpwarpError {
    McpwarpError::malformed(format!("{place}.{key} must be a string"))
}

/// A list: absent or `null` is empty; otherwise an array of objects (§2.8
/// rule 6).
fn list<'a>(
    o: &'a Map<String, Value>,
    key: &str,
    place: &str,
) -> Result<Vec<&'a Map<String, Value>>, McpwarpError> {
    match o.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(Value::as_object)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| not_a_list(place, key)),
        Some(_) => Err(not_a_list(place, key)),
    }
}

fn not_a_list(place: &str, key: &str) -> McpwarpError {
    McpwarpError::malformed(format!("{place}.{key} must be an array of objects"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::McpwarpErrorCode;

    fn decode(v: Value) -> Result<Option<InboundEnvelope>, McpwarpError> {
        decode_envelope(v.as_object().unwrap())
    }

    fn env(m: Value) -> Result<Option<InboundEnvelope>, McpwarpError> {
        decode(json!({ "mcpwarp": m }))
    }

    fn malformed(m: Value) -> String {
        let e = env(m).unwrap_err();
        assert_eq!(e.code, McpwarpErrorCode::MalformedEnvelope);
        assert!(!e.fatal);
        e.message
    }

    #[test]
    fn byte_exact_encoders() {
        assert_eq!(
            encode_register("anki", ServiceKind::Http).to_string(),
            r#"{"mcpwarp":{"v":1,"op":"register","services":[{"name":"anki","kind":"http"}]}}"#
        );
        assert_eq!(
            encode_register("notes", ServiceKind::Stdio).to_string(),
            r#"{"mcpwarp":{"v":1,"op":"register","services":[{"name":"notes","kind":"stdio"}]}}"#
        );
        assert_eq!(
            encode_unregister("anki").to_string(),
            r#"{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"anki"}]}}"#
        );
    }

    #[test]
    fn not_ours() {
        for body in [
            json!({}),
            json!({"mcpwarp": null}),
            json!({"mcpwarp": 1}),
            json!({"mcpwarp": "x"}),
            json!({"mcpwarp": [1]}),
        ] {
            assert_eq!(decode(body.clone()).unwrap(), None, "{body}");
        }
    }

    #[test]
    fn unknown_version_or_op() {
        assert_eq!(
            env(json!({"v": 2, "op": "disable", "id": 5})).unwrap(),
            Some(InboundEnvelope::Unknown {
                op: json!("disable"),
                v: json!(2)
            })
        );
        assert_eq!(
            env(json!({"op": "disable"})).unwrap(),
            Some(InboundEnvelope::Unknown {
                op: json!("disable"),
                v: Value::Null
            })
        );
        assert_eq!(
            env(json!({"v": "1", "op": "disable"})).unwrap(),
            Some(InboundEnvelope::Unknown {
                op: json!("disable"),
                v: json!("1")
            })
        );
        for op in [
            json!("frobnicate"),
            json!("register"),
            json!(7),
            Value::Null,
        ] {
            assert!(matches!(
                env(json!({"v": 1, "op": op})).unwrap(),
                Some(InboundEnvelope::Unknown { .. })
            ));
        }
        // 1.0 is 1, as in JS.
        assert!(matches!(
            env(json!({"v": 1.0, "op": "enable", "id": "a", "name": "b"})).unwrap(),
            Some(InboundEnvelope::Enable { .. })
        ));
    }

    #[test]
    fn defaults() {
        assert_eq!(
            env(json!({"v": 1, "op": "registered"})).unwrap(),
            Some(InboundEnvelope::Registered {
                services: vec![],
                errors: vec![]
            })
        );
        assert_eq!(
            env(json!({"v": 1, "op": "registered", "services": null, "errors": null})).unwrap(),
            Some(InboundEnvelope::Registered {
                services: vec![],
                errors: vec![]
            })
        );
        assert_eq!(
            env(json!({"v": 1, "op": "registered",
                "services": [{"name": "a", "id": "i", "url": "u", "created": null, "x": 1}],
                "errors": [{"name": "a", "code": "NEW_CODE"}]}))
            .unwrap(),
            Some(InboundEnvelope::Registered {
                services: vec![RegisteredService {
                    name: "a".into(),
                    id: "i".into(),
                    url: "u".into(),
                    created: false
                }],
                errors: vec![ServiceError {
                    name: "a".into(),
                    code: RegisterErrorCode::Other("NEW_CODE".into()),
                    message: String::new(),
                    id: None
                }]
            })
        );
        assert_eq!(
            env(json!({"v": 1, "op": "unregistered", "services": [{"name": "a"}]})).unwrap(),
            Some(InboundEnvelope::Unregistered {
                services: vec![UnregisteredService {
                    name: "a".into(),
                    id: String::new()
                }]
            })
        );
        assert_eq!(
            env(json!({"v": 1, "op": "conflict", "id": "i", "name": "n"})).unwrap(),
            Some(InboundEnvelope::Conflict {
                id: "i".into(),
                name: "n".into(),
                reason: "taken_over".into()
            })
        );
        assert_eq!(
            env(json!({"v": 1, "op": "error", "code": "OVERLOADED"})).unwrap(),
            Some(InboundEnvelope::Error {
                code: AppErrorCode::Overloaded,
                message: String::new()
            })
        );
    }

    #[test]
    fn malformed_messages() {
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered", "services": {}})),
            "registered.services must be an array of objects"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered", "services": [[]]})),
            "registered.services must be an array of objects"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered", "errors": "x"})),
            "registered.errors must be an array of objects"
        );
        assert_eq!(
            malformed(
                json!({"v": 1, "op": "registered", "errors": [{"name": "a", "code": "X", "id": null}]})
            ),
            "registered.errors[].id must be a string"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "enable", "id": null, "name": "a"})),
            "enable.id must be a string"
        );
    }

    #[test]
    fn checker_defect_order() {
        // name, id, url, created: the first defect wins, per entry, services
        // before errors (spec/tools/check-fixtures.mjs).
        let all_bad = json!({"v": 1, "op": "registered",
            "services": [{"name": 1, "id": 2, "url": "", "created": "x"}],
            "errors": [{"name": 1}]});
        assert_eq!(
            malformed(all_bad),
            "registered.services[].name must be a string"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered",
                "services": [{"name": "a", "id": 2, "url": "", "created": "x"}]})),
            "registered.services[].id must be a string"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered",
                "services": [{"name": "a", "id": "i", "url": "", "created": "x"}]})),
            "registered.services[].url must be a non-empty string"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered",
                "services": [{"name": "a", "id": "i", "url": "u", "created": "x"}]})),
            "registered.services[].created must be a boolean"
        );
        assert_eq!(
            malformed(
                json!({"v": 1, "op": "registered", "services": "x", "errors": [{"name": 1}]})
            ),
            "registered.services must be an array of objects"
        );
        assert_eq!(
            malformed(json!({"v": 1, "op": "registered",
                "errors": [{"name": "a", "code": 1, "message": null}]})),
            "registered.errors[].code must be a string"
        );
    }
}

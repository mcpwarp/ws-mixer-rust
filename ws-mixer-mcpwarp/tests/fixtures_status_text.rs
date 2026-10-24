//! `spec/fixtures/mcpwarp/status-text` (MCPWARP.md §7), mirroring the
//! status-text part of `spec/tools/check-fixtures.mjs`:
//!
//! - every `<locale>.json` has exactly the 17 legal `<state>.<code|none>`
//!   keys, and each entry equals `describe_status({state, code})`;
//! - the seven multi-service keys of §6A.9 (`rules.json` `hostOnly`): each
//!   `service.<state>` equals `describe_service_status_raw(state)`,
//!   `lastError.NO_SERVICE_REGISTERED` the group's `error`/`INTERNAL` status;
//! - the strings derived from each table (a state's label, a code's
//!   sentence) equal the embedded ones, so the shipped tables cannot drift;
//! - `rules.json` constants match what the implementation hard-codes;
//! - every `cases.json` case gives exactly `expect`, `input.service` cases
//!   through `describe_service_status_raw`.
//!
//! The `hostOnly` keys and `hostOnlyCases` are host guidance (MCPWARP.md
//! §10.6), which an SDK runner may skip; they are run here because the crate
//! ships `describe_service_status_raw` and the group sentence for hosts.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::{Value, json};
use ws_mixer_mcpwarp::{
    DescribeOptions, STATUS_LOCALES, StatusText, TunnelState, TunnelStatus, TunnelStatusCode,
    describe_service_status_raw, describe_status, describe_status_raw, last_error,
};

/// Floors for the pinned spec (v0.8.0): raise them as the corpus grows.
const MIN_TABLE_ENTRIES: usize = 144;
const MIN_CASES: usize = 37;

/// A service group's `lastError` when nothing registered (§6A.9).
const NO_SERVICE_REGISTERED: &str = "No service could be registered.";

/// MCPWARP.md §6A.9: the seven multi-service keys of every table.
const MULTI_SERVICE_KEYS: &[&str] = &[
    "lastError.NO_SERVICE_REGISTERED",
    "service.active",
    "service.disabled",
    "service.pending",
    "service.rejected",
    "service.taken_over",
    "service.unregistered",
];

fn is_multi_service_key(k: &str) -> bool {
    k.starts_with("service.") || k.starts_with("lastError.")
}

/// MCPWARP.md §6.5: the 17 legal (state, code) pairs.
const LEGAL_PAIRS: &[(&str, &[Option<&str>])] = &[
    ("off", &[None]),
    (
        "connecting",
        &[
            None,
            Some("RETRYING"),
            Some("DISCONNECTED"),
            Some("INTERNAL"),
        ],
    ),
    ("online", &[None]),
    ("disabled", &[Some("DISABLED")]),
    ("conflict", &[Some("TAKEN_OVER")]),
    (
        "error",
        &[
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
    ),
];

fn legal_keys() -> Vec<String> {
    let mut keys: Vec<_> = LEGAL_PAIRS
        .iter()
        .flat_map(|(state, codes)| {
            codes
                .iter()
                .map(move |c| format!("{state}.{}", c.unwrap_or("none")))
        })
        .collect();
    keys.sort();
    keys
}

fn status_dir() -> Option<std::path::PathBuf> {
    let root = common::mcpwarp_fixtures()?;
    let dir = root.join("status-text");
    assert!(dir.is_dir(), "{}: missing", dir.display());
    for required in ["rules.json", "cases.json", "en.json"] {
        assert!(
            dir.join(required).is_file(),
            "status-text/{required} is missing"
        );
    }
    Some(dir)
}

fn is_special_key(k: &str) -> bool {
    k.starts_with('_') || k.starts_with('$')
}

/// `<locale>.json` files by locale.
fn tables(dir: &Path) -> BTreeMap<String, serde_json::Map<String, Value>> {
    common::json_files(dir)
        .into_iter()
        .filter_map(|p| {
            let base = p.file_stem().unwrap().to_str().unwrap().to_owned();
            if base == "rules" || base == "cases" {
                return None;
            }
            let Value::Object(map) = common::read_json(&p) else {
                panic!("{}: not an object", p.display());
            };
            Some((base, map))
        })
        .collect()
}

fn opts(locale: Option<&str>) -> DescribeOptions {
    DescribeOptions {
        locale: locale.map(str::to_owned),
        messages: None,
    }
}

fn to_json(t: &StatusText) -> Value {
    serde_json::to_value(t).unwrap()
}

/// `describe_status_raw`, and the typed `describe_status` too when the state
/// and code are ones the enums know; both must agree.
fn describe(
    state: &str,
    code: Option<&str>,
    last_error: Option<&str>,
    o: &DescribeOptions,
) -> Value {
    let raw = describe_status_raw(state, code, last_error, o);
    let typed_code = match code {
        None => Some(None),
        Some(c) => TunnelStatusCode::from_wire(c).map(Some),
    };
    if let (Some(s), Some(c)) = (TunnelState::from_wire(state), typed_code) {
        let status = TunnelStatus {
            state: s,
            url: None,
            code: c,
            last_error: last_error.map(str::to_owned),
        };
        assert_eq!(
            describe_status(&status, o),
            raw,
            "typed vs raw: {state} {code:?}"
        );
    }
    to_json(&raw)
}

#[test]
fn status_text_tables() {
    let Some(dir) = status_dir() else {
        return;
    };
    let tables = tables(&dir);
    let mut files: Vec<_> = tables.keys().cloned().collect();
    files.sort();
    let mut want: Vec<_> = STATUS_LOCALES.iter().map(|s| s.to_string()).collect();
    want.sort();
    assert_eq!(files, want, "table files vs STATUS_LOCALES");

    let legal = legal_keys();
    let en = &tables["en"];
    let mut en_keys: Vec<_> = en.keys().cloned().collect();
    en_keys.sort();
    let multi: Vec<_> = en_keys.iter().filter(|k| is_multi_service_key(k)).collect();
    assert_eq!(multi, MULTI_SERVICE_KEYS, "en.json: multi-service keys");
    for k in &en_keys {
        assert!(
            is_special_key(k) || is_multi_service_key(k) || legal.contains(k),
            "en.json: \"{k}\" is not a legal pairing"
        );
    }
    for k in &legal {
        assert!(
            en_keys.contains(k),
            "en.json: legal pairing \"{k}\" has no entry"
        );
    }

    let mut entries = 0;
    for (locale, data) in &tables {
        let mut keys: Vec<_> = data.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, en_keys, "{locale}.json: key set differs from en.json");

        // Derived per rules.json keyFormat: states[state] = label, codes[code] = detail ?? label.
        let mut states = BTreeMap::new();
        let mut codes = BTreeMap::new();
        let o = opts(Some(locale));
        for (k, v) in data {
            if is_special_key(k) {
                continue;
            }
            if is_multi_service_key(k) {
                let got = match k.strip_prefix("service.") {
                    // `{label}` only: a rejected service's detail needs its code.
                    Some(state) => {
                        json!({"label": describe_service_status_raw(state, None, Some(locale)).label})
                    }
                    None => describe("error", Some("INTERNAL"), Some(NO_SERVICE_REGISTERED), &o),
                };
                assert_eq!(&got, v, "{locale}.json: \"{k}\"");
                entries += 1;
                continue;
            }
            let obj = v
                .as_object()
                .unwrap_or_else(|| panic!("{locale}.json: \"{k}\" must be an object"));
            assert!(
                obj.keys().all(|x| x == "label" || x == "detail"),
                "{locale}.json: \"{k}\" must be {{label, detail?}}"
            );
            let label = obj["label"].as_str().unwrap();
            let detail = obj.get("detail").map(|d| d.as_str().unwrap());
            if let Some(d) = detail {
                assert_ne!(d, label, "{locale}.json: \"{k}\".detail equals its label");
            }
            let (state, code) = k.split_once('.').unwrap();
            let prev = states.insert(state, label);
            assert!(
                prev.is_none_or(|p| p == label),
                "{locale}.json: {state}.* labels differ"
            );
            let code = (code != "none").then_some(code);
            if let Some(c) = code {
                let sentence = detail.unwrap_or(label);
                let prev = codes.insert(c, sentence);
                assert!(
                    prev.is_none_or(|p| p == sentence),
                    "{locale}.json: *.{c} sentences differ"
                );
            }

            assert_eq!(
                &describe(state, code, None, &o),
                v,
                "{locale}.json: \"{k}\""
            );
            entries += 1;
        }

        // The embedded strings, one by one.
        let state_keys: Vec<_> = TunnelState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            states.keys().copied().collect::<Vec<_>>(),
            sorted(&state_keys),
            "{locale}: states"
        );
        let code_keys: Vec<_> = TunnelStatusCode::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            codes.keys().copied().collect::<Vec<_>>(),
            sorted(&code_keys),
            "{locale}: codes"
        );
        for (state, label) in &states {
            assert_eq!(
                describe_status_raw(state, None, None, &o).label,
                *label,
                "{locale} states.{state}"
            );
        }
        for (code, sentence) in &codes {
            let t = describe_status_raw("\u{0}", Some(code), None, &o);
            assert_eq!(
                t.detail.as_deref(),
                Some(*sentence),
                "{locale} codes.{code}"
            );
        }
    }
    println!("status-text table entries: {entries}");
    assert!(
        entries >= MIN_TABLE_ENTRIES,
        "{entries} table entries, want >= {MIN_TABLE_ENTRIES}"
    );
}

fn sorted<'a>(v: &[&'a str]) -> Vec<&'a str> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v
}

#[test]
fn status_text_cases() {
    let Some(dir) = status_dir() else {
        return;
    };
    let doc = common::read_json(&dir.join("cases.json"));
    let cases = doc["cases"]
        .as_array()
        .expect("cases.json: \"cases\" array");
    assert!(!cases.is_empty());
    let mut run = 0;
    for (i, c) in cases.iter().enumerate() {
        let desc = c["description"].as_str().expect("description");
        let input = &c["input"];
        let service = input.get("service");
        let status = service
            .unwrap_or(&input["status"])
            .as_object()
            .expect("input.status or input.service");
        let locale = match &c["input"]["locale"] {
            Value::Null => None,
            Value::String(s) => Some(s.as_str()),
            other => panic!("cases[{i}]: locale {other}"),
        };
        let field = |k: &str| {
            status
                .get(k)
                .map(|v| v.as_str().unwrap_or_else(|| panic!("cases[{i}]: {k}")))
        };
        let expect = &c["expect"];
        assert!(expect["label"].is_string(), "cases[{i}]: expect.label");
        let state = field("state").expect("state");
        // A group status's `services` is ignored (§6A.9).
        let got = match service {
            Some(_) => to_json(&describe_service_status_raw(state, field("code"), locale)),
            None => describe(state, field("code"), field("lastError"), &opts(locale)),
        };
        assert_eq!(&got, expect, "cases[{i}] {desc}");
        run += 1;
    }
    println!("status-text cases: {run} of {}", cases.len());
    assert!(run >= MIN_CASES, "{run} cases, want >= {MIN_CASES}");
}

#[test]
fn status_text_rules() {
    let Some(dir) = status_dir() else {
        return;
    };
    let rules = common::read_json(&dir.join("rules.json"));
    let tables = tables(&dir);

    assert_eq!(rules["locales"], json!(STATUS_LOCALES), "locales");
    assert_eq!(
        rules["localeResolution"]["order"],
        json!(STATUS_LOCALES),
        "localeResolution.order"
    );
    let mut files: Vec<_> = tables.keys().map(String::as_str).collect();
    files.sort_unstable();
    assert_eq!(
        sorted(STATUS_LOCALES),
        files,
        "rules.locales vs table files"
    );

    // zhAlias: a bare "zh" is zh-CN.
    let alias = rules["zhAlias"].as_str().unwrap();
    assert_eq!(
        describe("online", None, None, &opts(Some("zh"))),
        describe("online", None, None, &opts(Some(alias)))
    );

    // colonJoiner, read back from INVALID_CONFIG composition.
    let joiners = rules["colonJoiner"].as_object().unwrap();
    assert_eq!(joiners.len(), STATUS_LOCALES.len());
    for &locale in STATUS_LOCALES {
        let o = opts(Some(locale));
        let sentence = describe_status_raw("\u{0}", Some("INVALID_CONFIG"), None, &o)
            .detail
            .unwrap();
        let detail = describe_status_raw("error", Some("INVALID_CONFIG"), Some("x"), &o)
            .detail
            .unwrap();
        let joiner = detail
            .strip_prefix(&sentence)
            .and_then(|r| r.strip_suffix('x'))
            .unwrap();
        assert_eq!(
            joiners[locale].as_str(),
            Some(joiner),
            "colonJoiner.{locale}"
        );
    }

    assert_eq!(rules["detailOmittedWhenEqualLabel"], json!(true));
    assert_eq!(rules["detailOmittedWhenEmpty"], json!(true));
    // The implementation hard-codes /:\s+/g -> "; " (with JS \s).
    assert_eq!(
        rules["lastErrorColonRewrite"],
        json!({"pattern": ":\\s+", "flags": "g", "replacement": "; "})
    );
    assert_eq!(
        rules["invalidConfigComposition"]["genericLastError"].as_str(),
        Some(last_error::INVALID_CONFIG)
    );

    let host = &rules["hostOffPattern"]["example"];
    let got = describe(
        host["status"]["state"].as_str().unwrap(),
        None,
        host["status"]["lastError"].as_str(),
        &opts(host["locale"].as_str()),
    );
    assert_eq!(got, host["expect"], "hostOffPattern.example");

    for (code, state) in rules["codeSentenceEqualsStateLabel"].as_object().unwrap() {
        for &locale in STATUS_LOCALES {
            let o = opts(Some(locale));
            let sentence = describe_status_raw("\u{0}", Some(code), None, &o).detail;
            let label = describe_status_raw(state.as_str().unwrap(), None, None, &o).label;
            assert_eq!(
                sentence,
                Some(label),
                "codeSentenceEqualsStateLabel {code} ({locale})"
            );
        }
    }

    for entry in rules["newInSpec"].as_array().unwrap() {
        let (locale, key) = entry.as_str().unwrap().split_once(':').unwrap();
        assert!(tables[locale].contains_key(key), "newInSpec {locale}:{key}");
    }

    let host_only: Vec<&str> = rules["hostOnly"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap())
        .collect();
    assert_eq!(sorted(&host_only), MULTI_SERVICE_KEYS, "hostOnly");

    // keyFormat.pattern: every table key is "<state>.<none|CODE>" or one of
    // the multi-service keys.
    assert_eq!(
        rules["keyFormat"]["pattern"].as_str(),
        Some(
            "^((off|connecting|online|disabled|conflict|error)\\.(none|[A-Z][A-Z_]*)|service\\.(pending|active|disabled|rejected|taken_over|unregistered)|lastError\\.NO_SERVICE_REGISTERED)$"
        )
    );
    for (locale, data) in &tables {
        for k in data.keys().filter(|k| !is_special_key(k)) {
            if is_multi_service_key(k) {
                assert!(MULTI_SERVICE_KEYS.contains(&k.as_str()), "{locale}: {k}");
                continue;
            }
            let (state, code) = k.split_once('.').unwrap();
            assert!(TunnelState::from_wire(state).is_some(), "{locale}: {k}");
            assert!(
                code == "none"
                    || (code.starts_with(|c: char| c.is_ascii_uppercase())
                        && code.chars().all(|c| c.is_ascii_uppercase() || c == '_')),
                "{locale}: {k}"
            );
        }
    }
    println!("status-text rules: checked");
}

//! Status text (MCPWARP.md §7): a [`TunnelStatus`] as short localized UI
//! text. Plain lookup tables, no locale library; never panics.

mod tables;

use std::collections::HashMap;

use serde::Serialize;

use crate::TunnelStatus;
use tables::{TABLES, Table};

/// The shipped locales, in resolution order: a bare language picks the first
/// locale with that language (MCPWARP.md §7 rule 1).
pub const STATUS_LOCALES: &[&str] = &["en", "de", "es", "fr", "ja", "zh-CN"];

/// `lastError` of `INVALID_CONFIG` that adds nothing to the sentence.
const GENERIC_INVALID: &str = crate::last_error::INVALID_CONFIG;

/// Options for [`describe_status`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeOptions {
    /// A BCP 47-ish tag, resolved per MCPWARP.md §7 rule 2: exact
    /// (case-insensitive, `_` read as `-`), then by language (`zh-TW` is
    /// `zh-CN`, `de-AT` is `de`), then `en`. `None` is `en`.
    pub locale: Option<String>,
    /// Per-call string overrides over the resolved locale.
    pub messages: Option<StatusMessages>,
}

/// String overrides (MCPWARP.md §7 rule 3), keyed by wire value: `states`
/// by state (`"error"`), `codes` by code (`"CONFLICT"`). Keys the resolved
/// table does not have are ignored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusMessages {
    /// State labels, e.g. `"error" => "Problem"`.
    pub states: HashMap<String, String>,
    /// Code sentences, e.g. `"CONFLICT" => "Name taken"`.
    pub codes: HashMap<String, String>,
}

/// A status as UI text.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct StatusText {
    /// The state, localized.
    pub label: String,
    /// Why, localized; `None` when there is nothing to explain or it would
    /// repeat `label`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// `describeStatus` (MCPWARP.md §7).
///
/// `label` is the state; `detail` the code's sentence, for `INVALID_CONFIG`
/// followed by a per-locale colon and `last_error`, or `last_error` itself
/// when there is no code.
///
/// ```
/// use ws_mixer_mcpwarp::{
///     DescribeOptions, TunnelState, TunnelStatus, TunnelStatusCode, describe_status, last_error,
/// };
///
/// let status = TunnelStatus {
///     code: Some(TunnelStatusCode::InvalidConfig),
///     last_error: Some(last_error::TOKEN_NOT_SET.into()),
///     ..TunnelStatus::new(TunnelState::Error)
/// };
/// let text = describe_status(&status, &DescribeOptions::default());
/// assert_eq!(text.label, "Error");
/// assert_eq!(text.detail.as_deref(), Some("Invalid settings: The token is not set."));
///
/// let de = DescribeOptions { locale: Some("de-AT".into()), ..Default::default() };
/// assert_eq!(describe_status(&TunnelStatus::default(), &de).label, "Aus");
/// ```
pub fn describe_status(status: &TunnelStatus, options: &DescribeOptions) -> StatusText {
    describe_status_raw(
        status.state.as_str(),
        status.code.map(|c| c.as_str()),
        status.last_error.as_deref(),
        options,
    )
}

/// [`describe_status`] over wire strings, for a state or code this SDK does
/// not know: an unknown state is its own label, an unknown code shows
/// `last_error`.
///
/// ```
/// use ws_mixer_mcpwarp::{DescribeOptions, describe_status_raw};
///
/// let text = describe_status_raw(
///     "connecting",
///     Some("SOMETHING_NEW"),
///     Some("Waiting for the edge."),
///     &DescribeOptions::default(),
/// );
/// assert_eq!(text.label, "Connecting…");
/// assert_eq!(text.detail.as_deref(), Some("Waiting for the edge."));
/// ```
pub fn describe_status_raw(
    state: &str,
    code: Option<&str>,
    last_error: Option<&str>,
    options: &DescribeOptions,
) -> StatusText {
    let table = resolve_locale(options.locale.as_deref());
    let messages = options.messages.as_ref();
    let label = match table.state(state) {
        Some(s) => messages
            .and_then(|m| m.states.get(state))
            .map_or(s, String::as_str),
        None => state,
    };
    let sentence = code.and_then(|c| {
        let s = table.code(c)?;
        Some(
            messages
                .and_then(|m| m.codes.get(c))
                .map_or(s, String::as_str),
        )
    });
    let detail = match (sentence, last_error) {
        (None, le) => le.map(str::to_owned),
        (Some(s), Some(le)) if code == Some("INVALID_CONFIG") && !le.is_empty() => {
            Some(invalid_config_detail(s, le, table.joiner))
        }
        (Some(s), _) => Some(s.to_owned()),
    };
    StatusText {
        detail: detail.filter(|d| !d.is_empty() && d != label),
        label: label.to_owned(),
    }
}

/// MCPWARP.md §7 rule 2. Never fails: anything unmatched is `en`.
fn resolve_locale(locale: Option<&str>) -> &'static Table {
    let Some(locale) = locale else {
        return &TABLES[0];
    };
    let tag = locale
        .trim_matches(is_js_whitespace)
        .replace('_', "-")
        .to_lowercase();
    if let Some(t) = TABLES.iter().find(|t| t.locale.to_lowercase() == tag) {
        return t;
    }
    let lang = primary_subtag(&tag);
    TABLES
        .iter()
        .find(|t| primary_subtag(&t.locale.to_lowercase()) == lang)
        .unwrap_or(&TABLES[0])
}

fn primary_subtag(tag: &str) -> &str {
    tag.split('-').next().unwrap_or(tag)
}

/// MCPWARP.md §7 rule 6, `INVALID_CONFIG` with a non-empty `lastError`.
fn invalid_config_detail(sentence: &str, last_error: &str, joiner: &str) -> String {
    if last_error == GENERIC_INVALID {
        sentence.to_owned()
    } else if last_error.starts_with(sentence) {
        last_error.to_owned()
    } else {
        format!("{sentence}{joiner}{}", rewrite_colons(last_error))
    }
}

/// JS `s.replace(/:\s+/g, "; ")`.
fn rewrite_colons(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(':') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let skipped = after.trim_start_matches(is_js_whitespace);
        out.push_str(if skipped.len() < after.len() {
            "; "
        } else {
            ":"
        });
        rest = skipped;
    }
    out.push_str(rest);
    out
}

/// JS `\s` and `String.prototype.trim`: ECMAScript WhiteSpace and
/// LineTerminator, which is Unicode `White_Space` without U+0085 and with
/// U+FEFF.
fn is_js_whitespace(c: char) -> bool {
    (c.is_whitespace() && c != '\u{85}') || c == '\u{feff}'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TunnelState, TunnelStatusCode, last_error};

    fn opts(locale: &str) -> DescribeOptions {
        DescribeOptions {
            locale: Some(locale.into()),
            messages: None,
        }
    }

    fn raw(
        state: &str,
        code: Option<&str>,
        le: Option<&str>,
        o: &DescribeOptions,
    ) -> (String, Option<String>) {
        let t = describe_status_raw(state, code, le, o);
        (t.label, t.detail)
    }

    fn invalid_config(le: &str, o: &DescribeOptions) -> Option<String> {
        raw("error", Some("INVALID_CONFIG"), Some(le), o).1
    }

    #[test]
    fn locales_are_the_tables_in_order() {
        let order: Vec<_> = TABLES.iter().map(|t| t.locale).collect();
        assert_eq!(order, STATUS_LOCALES);
    }

    #[test]
    fn locale_resolution() {
        let cases = [
            ("en", "en"),
            ("EN", "en"),
            ("de", "de"),
            ("zh-CN", "zh-CN"),
            ("zh-cn", "zh-CN"),
            ("zh_CN", "zh-CN"),
            ("ZH_cn", "zh-CN"),
            ("zh", "zh-CN"),
            ("zh-TW", "zh-CN"),
            ("zh-Hant", "zh-CN"),
            ("zh_Hant_TW", "zh-CN"),
            ("de-AT", "de"),
            ("de_CH", "de"),
            ("fr-CA", "fr"),
            ("ja-JP", "ja"),
            ("es-419", "es"),
            (" de-AT ", "de"),
            ("\t\nfr\u{feff}", "fr"),
            ("\u{3000}ja\u{2028}", "ja"),
            ("pt-BR", "en"),
            ("", "en"),
            ("   ", "en"),
            ("-", "en"),
            ("-de", "en"),
            ("d", "en"),
            ("deu", "en"),
            ("cn", "en"),
            // U+0085 is not JS whitespace: not trimmed, so no match.
            ("\u{85}de", "en"),
        ];
        for (input, want) in cases {
            assert_eq!(resolve_locale(Some(input)).locale, want, "{input:?}");
        }
        assert_eq!(resolve_locale(None).locale, "en");
    }

    #[test]
    fn js_whitespace_set() {
        for c in [
            '\t', '\n', '\u{b}', '\u{c}', '\r', ' ', '\u{a0}', '\u{1680}', '\u{2000}', '\u{200a}',
            '\u{2028}', '\u{2029}', '\u{202f}', '\u{205f}', '\u{3000}', '\u{feff}',
        ] {
            assert!(is_js_whitespace(c), "{c:?}");
        }
        for c in ['\u{85}', '\u{180e}', '\u{200b}', 'a', ':', '\0'] {
            assert!(!is_js_whitespace(c), "{c:?}");
        }
    }

    #[test]
    fn colon_rewrite() {
        let cases = [
            ("", ""),
            ("plain", "plain"),
            ("a: b", "a; b"),
            ("a:b", "a:b"),
            ("a:", "a:"),
            ("a: ", "a; "),
            ("a:\t\n b", "a; b"),
            ("https://host: x", "https://host; x"),
            ("a:: b", "a:; b"),
            ("a: : b", "a; ; b"),
            ("x:\u{feff}y", "x; y"),
            ("x:\u{a0}y", "x; y"),
            ("x:\u{3000}y", "x; y"),
            ("x:\u{85}y", "x:\u{85}y"),
            ("é:  ü", "é; ü"),
        ];
        for (input, want) in cases {
            assert_eq!(rewrite_colons(input), want, "{input:?}");
        }
    }

    #[test]
    fn typed_and_raw_agree() {
        let status = TunnelStatus {
            code: Some(TunnelStatusCode::Retrying),
            last_error: Some(last_error::RETRY_DIAL.into()),
            ..TunnelStatus::new(TunnelState::Connecting)
        };
        let o = opts("fr");
        assert_eq!(
            describe_status(&status, &o),
            describe_status_raw(
                "connecting",
                Some("RETRYING"),
                Some(last_error::RETRY_DIAL),
                &o
            )
        );
    }

    #[test]
    fn invalid_config_detail_edges() {
        let en = DescribeOptions::default();
        // Empty lastError: the sentence alone.
        assert_eq!(invalid_config("", &en).as_deref(), Some("Invalid settings"));
        assert_eq!(
            raw("error", Some("INVALID_CONFIG"), None, &en).1.as_deref(),
            Some("Invalid settings")
        );
        // Whitespace-only is non-empty in JS: appended as is.
        assert_eq!(
            invalid_config("  ", &en).as_deref(),
            Some("Invalid settings:   ")
        );
        assert_eq!(
            invalid_config("\u{feff}", &en).as_deref(),
            Some("Invalid settings: \u{feff}")
        );
        // The generic lastError adds nothing.
        assert_eq!(
            invalid_config(GENERIC_INVALID, &opts("ja")).as_deref(),
            Some("設定が無効です")
        );
        // Already starts with the sentence: kept, not rewritten.
        assert_eq!(
            invalid_config("Invalid settings: a: b", &en).as_deref(),
            Some("Invalid settings: a: b")
        );
        // Nested colons: each ':' + whitespace run becomes "; ".
        assert_eq!(
            invalid_config("url: wss://h:1:  bad:\tport", &en).as_deref(),
            Some("Invalid settings: url; wss://h:1; bad; port")
        );
        // A BOM after a colon is whitespace in JS.
        assert_eq!(
            invalid_config("a:\u{feff}b", &en).as_deref(),
            Some("Invalid settings: a; b")
        );
        // Per-locale joiners.
        assert_eq!(
            invalid_config("x", &opts("fr")).as_deref(),
            Some("Paramètres non valides\u{a0}: x")
        );
        assert_eq!(
            invalid_config("x", &opts("zh")).as_deref(),
            Some("设置无效：x")
        );
        assert_eq!(
            invalid_config("x", &opts("es")).as_deref(),
            Some("Configuración no válida: x")
        );
    }

    #[test]
    fn code_less_and_unknown_code_show_last_error_unchanged() {
        let en = DescribeOptions::default();
        assert_eq!(
            raw("error", None, Some("a: b"), &en),
            ("Error".into(), Some("a: b".into()))
        );
        assert_eq!(
            raw("error", Some("NEW_CODE"), Some("x."), &en),
            ("Error".into(), Some("x.".into()))
        );
        assert_eq!(
            raw("error", Some("NEW_CODE"), None, &en),
            ("Error".into(), None)
        );
        assert_eq!(
            raw("error", Some("invalid_config"), Some("x"), &en)
                .1
                .as_deref(),
            Some("x")
        );
        assert_eq!(
            raw("off", None, Some(""), &opts("de")),
            ("Aus".into(), None)
        );
        // Whitespace-only is non-empty: shown.
        assert_eq!(raw("off", None, Some(" "), &en).1.as_deref(), Some(" "));
        // Equal to the label: left out.
        assert_eq!(raw("off", None, Some("Off"), &en).1, None);
    }

    #[test]
    fn unknown_state_is_its_own_label() {
        let en = DescribeOptions::default();
        assert_eq!(raw("paused", None, None, &en), ("paused".into(), None));
        assert_eq!(raw("Online", None, None, &en).0, "Online");
        assert_eq!(raw("ONLINE", None, None, &en).0, "ONLINE");
        assert_eq!(raw("", None, None, &en), (String::new(), None));
        // Unknown state with the label as lastError is still left out.
        assert_eq!(raw("x", None, Some("x"), &en).1, None);
    }

    #[test]
    fn other_codes_ignore_last_error() {
        assert_eq!(
            raw(
                "error",
                Some("UNAUTHORIZED"),
                Some(last_error::TOKEN_REJECTED),
                &opts("fr")
            )
            .1
            .as_deref(),
            Some("Jeton d'accès refusé")
        );
        for locale in STATUS_LOCALES {
            let o = opts(locale);
            assert_eq!(
                raw("disabled", Some("DISABLED"), Some(last_error::DISABLED), &o).1,
                None
            );
            assert_eq!(
                raw(
                    "conflict",
                    Some("TAKEN_OVER"),
                    Some(last_error::TAKEN_OVER),
                    &o
                )
                .1,
                None
            );
        }
    }

    #[test]
    fn overrides() {
        let messages = StatusMessages {
            states: HashMap::from([
                ("error".into(), "Problem".into()),
                ("paused".into(), "Paused".into()),
            ]),
            codes: HashMap::from([
                ("CONFLICT".into(), "Name vergeben".into()),
                ("NEW_CODE".into(), "ignored".into()),
            ]),
        };
        let o = DescribeOptions {
            locale: Some("de".into()),
            messages: Some(messages),
        };
        assert_eq!(
            raw("error", Some("CONFLICT"), None, &o),
            ("Problem".into(), Some("Name vergeben".into()))
        );
        // Untouched keys come from the resolved locale.
        assert_eq!(raw("off", None, None, &o).0, "Aus");
        assert_eq!(
            raw("error", Some("INTERNAL"), None, &o).1.as_deref(),
            Some("Unerwarteter Fehler")
        );
        // Keys outside the table are ignored.
        assert_eq!(raw("paused", None, None, &o).0, "paused");
        assert_eq!(
            raw("error", Some("NEW_CODE"), Some("le"), &o).1.as_deref(),
            Some("le")
        );
        // Per call only.
        assert_eq!(raw("error", None, None, &opts("de")).0, "Fehler");

        // An overridden sentence differs from the label, so it is shown.
        let o = DescribeOptions {
            locale: None,
            messages: Some(StatusMessages {
                codes: HashMap::from([("DISABLED".into(), "Turned off by an admin".into())]),
                ..Default::default()
            }),
        };
        assert_eq!(
            raw("disabled", Some("DISABLED"), None, &o),
            (
                "Disabled in the mcpwarp dashboard".into(),
                Some("Turned off by an admin".into())
            )
        );

        // INVALID_CONFIG composes over the overridden sentence.
        let o = DescribeOptions {
            locale: Some("fr".into()),
            messages: Some(StatusMessages {
                codes: HashMap::from([("INVALID_CONFIG".into(), "Réglages".into())]),
                ..Default::default()
            }),
        };
        assert_eq!(
            invalid_config("a: b", &o).as_deref(),
            Some("Réglages\u{a0}: a; b")
        );
        assert_eq!(
            invalid_config("Réglages x", &o).as_deref(),
            Some("Réglages x")
        );

        // An empty override gives an empty label and no detail.
        let o = DescribeOptions {
            locale: None,
            messages: Some(StatusMessages {
                states: HashMap::from([("error".into(), String::new())]),
                codes: HashMap::from([("CONFLICT".into(), String::new())]),
            }),
        };
        assert_eq!(
            raw("error", Some("CONFLICT"), Some("x"), &o),
            (String::new(), None)
        );
    }

    #[test]
    fn status_text_json() {
        let t = describe_status(&TunnelStatus::default(), &DescribeOptions::default());
        assert_eq!(serde_json::to_string(&t).unwrap(), r#"{"label":"Off"}"#);
    }
}

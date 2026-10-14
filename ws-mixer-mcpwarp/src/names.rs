//! Service-name and integration-id syntax (MCPWARP.md §2.4), checked by hand
//! rather than with a regex engine.

use serde::{Deserialize, Serialize};

/// The service-name syntax, for display and for other regex engines.
/// [`is_valid_service_name`] implements it.
pub const SERVICE_NAME_RE: &str = "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$";

/// The longest service name the edge accepts, in characters.
pub const SERVICE_NAME_MAX: usize = 30;

/// The integration-id syntax, tested after lowercasing: an RFC 4122 / 9562
/// UUID in text form (version 1-8, variant `8`/`9`/`a`/`b`), or the nil or
/// max UUID. [`is_valid_integration_id`] implements it.
pub const INTEGRATION_ID_RE: &str = "^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$";

/// Whether `name` is a service name the edge accepts: at most
/// [`SERVICE_NAME_MAX`] characters of lowercase ASCII letters, digits and
/// inner dashes ([`SERVICE_NAME_RE`]).
///
/// ```
/// use ws_mixer_mcpwarp::is_valid_service_name;
///
/// assert!(is_valid_service_name("anki"));
/// assert!(is_valid_service_name("zotero-notes-2"));
/// assert!(!is_valid_service_name("Anki"));
/// assert!(!is_valid_service_name("anki-"));
/// assert!(!is_valid_service_name(""));
/// ```
pub fn is_valid_service_name(name: &str) -> bool {
    let b = name.as_bytes();
    let alnum = |c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    match (b.first(), b.last()) {
        (Some(first), Some(last)) => {
            b.len() <= SERVICE_NAME_MAX
                && alnum(first)
                && alnum(last)
                && b.iter().all(|c| alnum(c) || *c == b'-')
        }
        _ => false,
    }
}

/// Whether `id` matches [`INTEGRATION_ID_RE`] after ASCII lowercasing. No
/// other character lowercases into the pattern's alphabet, so this agrees
/// with a full Unicode lowercasing.
///
/// ```
/// use ws_mixer_mcpwarp::is_valid_integration_id;
///
/// assert!(is_valid_integration_id("6F1C2A9E-3B4D-4E5F-8A7B-0C1D2E3F4A5B"));
/// assert!(is_valid_integration_id("00000000-0000-0000-0000-000000000000"));
/// assert!(!is_valid_integration_id("6f1c2a9e-3b4d-0e5f-8a7b-0c1d2e3f4a5b")); // version 0
/// assert!(!is_valid_integration_id("not-a-uuid"));
/// ```
pub fn is_valid_integration_id(id: &str) -> bool {
    let id = id.to_ascii_lowercase();
    if id == "00000000-0000-0000-0000-000000000000" || id == "ffffffff-ffff-ffff-ffff-ffffffffffff"
    {
        return true;
    }
    let b = id.as_bytes();
    if b.len() != 36 {
        return false;
    }
    let hex = |c: u8| c.is_ascii_digit() || (b'a'..=b'f').contains(&c);
    b.iter().enumerate().all(|(i, &c)| match i {
        8 | 13 | 18 | 23 => c == b'-',
        14 => (b'1'..=b'8').contains(&c),
        19 => matches!(c, b'8' | b'9' | b'a' | b'b'),
        _ => hex(c),
    })
}

/// `register.services[].kind` (MCPWARP.md §2.3).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceKind {
    /// `"http"`, the default.
    #[default]
    Http,
    /// `"stdio"`.
    Stdio,
}

impl ServiceKind {
    /// The wire value, `"http"` or `"stdio"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Stdio => "stdio",
        }
    }
}

impl std::fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_names() {
        for ok in [
            "a",
            "0",
            "anki",
            "a-b",
            "a--b",
            "zotero-library-notes-search-01",
        ] {
            assert!(is_valid_service_name(ok), "{ok}");
        }
        for bad in [
            "",
            "-",
            "-a",
            "a-",
            "A",
            "anki mcp",
            "a_b",
            "a.b",
            "zotero-library-notes-search-012",
            "ünki",
            "anki\n",
        ] {
            assert!(!is_valid_service_name(bad), "{bad:?}");
        }
        assert_eq!("zotero-library-notes-search-01".len(), SERVICE_NAME_MAX);
    }

    #[test]
    fn integration_ids() {
        let ok = [
            "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b",
            "6F1C2A9E-3B4D-1E5F-BA7B-0C1D2E3F4A5B",
            "6f1c2a9e-3b4d-8e5f-9a7b-0c1d2e3f4a5b",
            "00000000-0000-0000-0000-000000000000",
            "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
        ];
        for id in ok {
            assert!(is_valid_integration_id(id), "{id}");
        }
        let bad = [
            "",
            "6f1c2a9e-3b4d-0e5f-8a7b-0c1d2e3f4a5b", // version 0
            "6f1c2a9e-3b4d-9e5f-8a7b-0c1d2e3f4a5b", // version 9
            "6f1c2a9e-3b4d-4e5f-7a7b-0c1d2e3f4a5b", // variant 7
            "6f1c2a9e-3b4d-4e5f-ca7b-0c1d2e3f4a5b", // variant c
            "6f1c2a9e3b4d4e5f8a7b0c1d2e3f4a5b",
            "{6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b}",
            "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5g",
            " 6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b",
            "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b ",
            "6f1c2a9e_3b4d-4e5f-8a7b-0c1d2e3f4a5b",
        ];
        for id in bad {
            assert!(!is_valid_integration_id(id), "{id:?}");
        }
    }

    #[test]
    fn kind_wire_values() {
        assert_eq!(ServiceKind::default(), ServiceKind::Http);
        assert_eq!(
            serde_json::to_string(&ServiceKind::Stdio).unwrap(),
            "\"stdio\""
        );
        assert_eq!(ServiceKind::Http.to_string(), "http");
    }
}

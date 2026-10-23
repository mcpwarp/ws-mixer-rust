//! Register and envelope error codes (MCPWARP.md §2.5, §2.6). Each code's
//! wire value equals its name; unknown values are kept as `Other`.

use std::fmt;

macro_rules! wire_codes {
    (
        $(#[$meta:meta])*
        $ty:ident { $($(#[$vmeta:meta])* $variant:ident => $wire:literal,)* }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $ty {
            $($(#[$vmeta])* $variant,)*
            /// A code this SDK does not know, verbatim.
            Other(String),
        }

        impl $ty {
            /// The wire value, e.g. `"INVALID_NAME"`.
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)*
                    Self::Other(s) => s,
                }
            }

            /// Parses a wire value; anything unknown is [`Self::Other`].
            pub fn from_wire(s: &str) -> Self {
                match s {
                    $($wire => Self::$variant,)*
                    _ => Self::Other(s.to_owned()),
                }
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<&str> for $ty {
            fn from(s: &str) -> Self {
                Self::from_wire(s)
            }
        }
    };
}

wire_codes! {
    /// `registered.errors[].code` (MCPWARP.md §2.5).
    RegisterErrorCode {
        /// The edge rejected the name. Refusal.
        InvalidName => "INVALID_NAME",
        /// The account has no username. Refusal.
        UsernameRequired => "USERNAME_REQUIRED",
        /// The service quota is used up. Refusal.
        QuotaExceeded => "QUOTA_EXCEEDED",
        /// The name is in use by another server. Refusal.
        Conflict => "CONFLICT",
        /// The service is disabled in the dashboard; carries the server id.
        ServerDisabled => "SERVER_DISABLED",
        /// Transient.
        Internal => "INTERNAL",
        /// Transient.
        NoResponders => "NO_RESPONDERS",
        /// Transient.
        Timeout => "TIMEOUT",
        /// Transient.
        Aborted => "ABORTED",
        /// Transient.
        ConnectionLost => "CONNECTION_LOST",
        /// Transient.
        MalformedReply => "MALFORMED_REPLY",
        /// RPC pass-through. Refusal.
        BadRequest => "BAD_REQUEST",
        /// RPC pass-through. Refusal.
        Unauthenticated => "UNAUTHENTICATED",
        /// RPC pass-through. Refusal.
        Forbidden => "FORBIDDEN",
        /// RPC pass-through. Refusal.
        NotImplemented => "NOT_IMPLEMENTED",
        /// Transient.
        Unavailable => "UNAVAILABLE",
        /// Transient.
        ServiceError => "SERVICE_ERROR",
    }
}

wire_codes! {
    /// `error.code`, the envelope-level failures (MCPWARP.md §2.6).
    AppErrorCode {
        /// The envelope was malformed.
        BadRequest => "BAD_REQUEST",
        /// `v` is not supported. Fatal.
        UnsupportedVersion => "UNSUPPORTED_VERSION",
        /// The `op` is unknown.
        UnknownOp => "UNKNOWN_OP",
        /// Too many entries in `services[]`.
        TooManyServices => "TOO_MANY_SERVICES",
        /// Too many `app` messages in flight: re-send the outstanding one
        /// after `min(1000 × 2^(n−1), 30000)` ms, `n` the consecutive
        /// `OVERLOADED` replies.
        Overloaded => "OVERLOADED",
        /// Too many connections per owner; the edge then closes with 4014.
        ConnectionLimit => "CONNECTION_LIMIT",
    }
}

/// The transient register codes: retried on the same connection with
/// backoff, never fatal (MCPWARP.md §2.5, §3.2).
pub const TRANSIENT_REGISTER_ERRORS: [RegisterErrorCode; 8] = [
    RegisterErrorCode::Internal,
    RegisterErrorCode::NoResponders,
    RegisterErrorCode::Timeout,
    RegisterErrorCode::Aborted,
    RegisterErrorCode::ConnectionLost,
    RegisterErrorCode::MalformedReply,
    RegisterErrorCode::Unavailable,
    RegisterErrorCode::ServiceError,
];

impl RegisterErrorCode {
    /// Whether the code is in [`TRANSIENT_REGISTER_ERRORS`].
    pub fn is_transient(&self) -> bool {
        TRANSIENT_REGISTER_ERRORS.contains(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_codes_round_trip() {
        let all = [
            "INVALID_NAME",
            "USERNAME_REQUIRED",
            "QUOTA_EXCEEDED",
            "CONFLICT",
            "SERVER_DISABLED",
            "INTERNAL",
            "NO_RESPONDERS",
            "TIMEOUT",
            "ABORTED",
            "CONNECTION_LOST",
            "MALFORMED_REPLY",
            "BAD_REQUEST",
            "UNAUTHENTICATED",
            "FORBIDDEN",
            "NOT_IMPLEMENTED",
            "UNAVAILABLE",
            "SERVICE_ERROR",
        ];
        for s in all {
            let c = RegisterErrorCode::from_wire(s);
            assert!(!matches!(c, RegisterErrorCode::Other(_)), "{s}");
            assert_eq!(c.as_str(), s);
            assert_eq!(c.to_string(), s);
        }
        let other = RegisterErrorCode::from_wire("RATE_LIMITED");
        assert_eq!(other, RegisterErrorCode::Other("RATE_LIMITED".into()));
        assert_eq!(other.as_str(), "RATE_LIMITED");
        assert!(!other.is_transient());
    }

    #[test]
    fn transient_set() {
        let transient: Vec<_> = TRANSIENT_REGISTER_ERRORS
            .iter()
            .map(|c| c.as_str())
            .collect();
        assert_eq!(
            transient,
            [
                "INTERNAL",
                "NO_RESPONDERS",
                "TIMEOUT",
                "ABORTED",
                "CONNECTION_LOST",
                "MALFORMED_REPLY",
                "UNAVAILABLE",
                "SERVICE_ERROR"
            ]
        );
        for c in [
            "SERVER_DISABLED",
            "INVALID_NAME",
            "BAD_REQUEST",
            "FORBIDDEN",
            "X",
        ] {
            assert!(!RegisterErrorCode::from_wire(c).is_transient(), "{c}");
        }
    }

    #[test]
    fn app_codes_round_trip() {
        for s in [
            "BAD_REQUEST",
            "UNSUPPORTED_VERSION",
            "UNKNOWN_OP",
            "TOO_MANY_SERVICES",
            "OVERLOADED",
            "CONNECTION_LIMIT",
        ] {
            let c = AppErrorCode::from_wire(s);
            assert!(!matches!(c, AppErrorCode::Other(_)), "{s}");
            assert_eq!(c.as_str(), s);
        }
        assert_eq!(
            AppErrorCode::from_wire("INTERNAL"),
            AppErrorCode::Other("INTERNAL".into())
        );
    }
}

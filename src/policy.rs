//! Close-code classification for the reconnect loop (WIRE.md §2.9).

use crate::event::Phase;

/// How the reconnect loop treats a WebSocket close.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseClass {
    /// Do not reconnect.
    Fatal,
    /// Fetch a new token from the provider and redial immediately, once per
    /// stability window; a second rejection is fatal.
    RefreshTokenOnce,
    /// Redial after a random delay of up to 2 s (server drain or going away).
    ImmediateJitter2s,
    /// Redial immediately once per stability window, then back off normally.
    KeepaliveImmediateOnce,
    /// Back off, starting at the policy's `cap`.
    StartAtCap,
    /// Normal full-jitter exponential backoff.
    Backoff,
    /// Normal backoff, logged loudly: the close indicates an implementation bug.
    BackoffLoud,
}

/// Classifies a WebSocket close received from the peer in `phase`.
///
/// `ws_code` is the close code as sent on the wire, not
/// [`DisconnectReason::ws_code`](crate::DisconnectReason::ws_code) (which may
/// hold a semantic code above `u16::MAX`). Closes the client generates itself
/// are not classified here: the welcome-timeout `4001` is plain `Backoff`,
/// not `BackoffLoud`.
///
/// `token_is_provider` is whether the client uses a
/// [`Token::Provider`](crate::Token::Provider); only providers can refresh.
/// [`Phase::Dial`] is treated like [`Phase::Handshake`]: both are before
/// `welcome`. The close reason text never influences the result.
///
/// | Close | Phase | Class |
/// |---|---|---|
/// | 4010 | any | `Fatal` |
/// | 4011 | connected | `Fatal` |
/// | 4011 | before welcome, provider | `RefreshTokenOnce` |
/// | 4011 | before welcome, static token | `Fatal` |
/// | 4012, 1001 | connected | `ImmediateJitter2s` |
/// | 4013 | connected | `KeepaliveImmediateOnce` |
/// | 4009, 4014 | connected | `StartAtCap` |
/// | 4009, 4014 | before welcome | `Backoff` |
/// | 4001, 4003, 4004 | any | `BackoffLoud` |
/// | anything else | any | `Backoff` |
pub fn classify_close(ws_code: u16, phase: Phase, token_is_provider: bool) -> CloseClass {
    let connected = phase == Phase::Connected;
    match ws_code {
        4010 => CloseClass::Fatal,
        4011 if connected => CloseClass::Fatal,
        4011 if token_is_provider => CloseClass::RefreshTokenOnce,
        4011 => CloseClass::Fatal,
        4012 | 1001 if connected => CloseClass::ImmediateJitter2s,
        4013 if connected => CloseClass::KeepaliveImmediateOnce,
        4009 | 4014 if connected => CloseClass::StartAtCap,
        4001 | 4003 | 4004 => CloseClass::BackoffLoud,
        _ => CloseClass::Backoff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use CloseClass::*;
    use Phase::{Connected, Dial, Handshake};

    const ALL: [Phase; 3] = [Dial, Handshake, Connected];
    const PRE: [Phase; 2] = [Dial, Handshake];

    fn both(code: u16, phase: Phase, want: CloseClass) {
        for provider in [false, true] {
            assert_eq!(
                classify_close(code, phase, provider),
                want,
                "code {code} phase {phase} provider {provider}"
            );
        }
    }

    #[test]
    fn unsupported_is_always_fatal() {
        for p in ALL {
            both(4010, p, Fatal);
        }
    }

    #[test]
    fn unauthorized_after_welcome_is_fatal() {
        both(4011, Connected, Fatal);
    }

    #[test]
    fn unauthorized_before_welcome_with_provider_refreshes() {
        for p in PRE {
            assert_eq!(classify_close(4011, p, true), RefreshTokenOnce);
        }
    }

    #[test]
    fn unauthorized_before_welcome_with_static_token_is_fatal() {
        for p in PRE {
            assert_eq!(classify_close(4011, p, false), Fatal);
        }
    }

    #[test]
    fn going_away_after_welcome_is_immediate() {
        both(4012, Connected, ImmediateJitter2s);
        both(1001, Connected, ImmediateJitter2s);
    }

    #[test]
    fn keepalive_timeout_after_welcome_is_immediate_once() {
        both(4013, Connected, KeepaliveImmediateOnce);
    }

    #[test]
    fn calm_and_application_close_after_welcome_start_at_cap() {
        both(4009, Connected, StartAtCap);
        both(4014, Connected, StartAtCap);
    }

    #[test]
    fn calm_and_application_close_before_welcome_back_off() {
        for p in PRE {
            both(4009, p, Backoff);
            both(4014, p, Backoff);
        }
    }

    #[test]
    fn sdk_bug_codes_back_off_loudly() {
        for p in ALL {
            for code in [4001, 4003, 4004] {
                both(code, p, BackoffLoud);
            }
        }
    }

    #[test]
    fn everything_else_backs_off() {
        for p in ALL {
            for code in [1000, 1006, 1009, 1011, 4000, 4002, 4005, 4015, 4999] {
                both(code, p, Backoff);
            }
        }
        for p in PRE {
            both(4012, p, Backoff);
            both(1001, p, Backoff);
            both(4013, p, Backoff);
        }
    }
}

//! Pure reconnect decisions: backoff delay, attempt and refresh budgets, and
//! the stability window (WIRE.md §2.9 "Reconnect", CLIENT-SDK.md "Rejected
//! token" / "Provider failure" / "Fatal set").

use std::time::Duration;

use super::ReconnectPolicy;
use crate::event::Phase;
use crate::policy::{CloseClass, classify_close};

/// Upper bound on a server-supplied delay (`Retry-After` on an HTTP 429/503,
/// `drain.retry_after_ms`), so a bogus hint cannot park the client for days.
pub(crate) const MAX_SERVER_DELAY: Duration = Duration::from_secs(300);

/// Jitter bound for `drain`, 4012 and 1001 (WIRE.md §2.9). See
/// [`going_away_jitter`] for how it combines with the backoff cap.
pub(crate) const DRAIN_JITTER: Duration = Duration::from_secs(2);

/// Highest exponent in `base * 2^attempt`; the cap is reached long before.
const MAX_EXPONENT: u32 = 30;

/// Reconnect bookkeeping that survives across connections.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PolicyState {
    /// Consecutive reconnects since the last stable connection.
    pub(crate) attempt: u32,
    /// The one immediate retry after a 4013 is spent.
    pub(crate) keepalive_retry_used: bool,
    /// The one token refresh after a 401 / pre-welcome 4011 is spent.
    pub(crate) refresh_retry_used: bool,
}

impl PolicyState {
    /// A connection stayed up `stable_after` past its welcome.
    pub(crate) fn stable(&mut self) {
        *self = Self::default();
    }
}

/// What failed, as far as the policy cares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The provider returned [`TokenError::Unavailable`](crate::TokenError::Unavailable)
    /// or did not answer within `connect_timeout`.
    TokenUnavailable,
    /// The provider returned [`TokenError::Fatal`](crate::TokenError::Fatal).
    TokenFatal,
    /// The upgrade was answered with a non-101 status.
    Http {
        status: u16,
        retry_after: Option<Duration>,
    },
    /// A dial, handshake or connection failure other than the above.
    Conn {
        phase: Phase,
        /// The semantic close code, if a close happened.
        ws_code: Option<u32>,
        /// Already fatal at the source (missing subprotocol echo, UNSUPPORTED).
        fatal: bool,
        /// Our own welcome timeout: plain backoff, not an SDK-bug close.
        welcome_timeout: bool,
    },
}

/// How a retry was decided.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum RetryKind {
    /// Counted against `attempt`: backoff, start-at-cap, 4012 jitter,
    /// Retry-After.
    Normal,
    /// The one token refresh: call the provider again and redial now. Not
    /// counted against `attempt`; reported as a non-fatal disconnect.
    RefreshNow,
    /// The one immediate retry after a 4013. Counted against `attempt`.
    ImmediateKeepalive,
}

/// Why the client gives up.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum FatalKind {
    /// The failure itself is fatal (WIRE.md §2.9 fatal set).
    Classified,
    /// Reconnect is disabled: the first failure is final.
    Disabled,
    /// `max_attempts` consecutive reconnects failed.
    Exhausted(u32),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    Fatal(FatalKind),
    Retry {
        delay: Duration,
        kind: RetryKind,
        /// Log at error level: 4001/4003/4004 from the peer mean an SDK bug.
        loud: bool,
    },
}

/// What [`classify`] says to do before the attempt bookkeeping.
enum Plan {
    Fatal,
    Refresh,
    Backoff { loud: bool },
    Fixed(Duration),
    GoingAway,
    StartAtCap,
    KeepaliveImmediate,
}

fn classify(state: &mut PolicyState, f: &Failure, provider: bool) -> Plan {
    let class = match *f {
        Failure::TokenFatal => return Plan::Fatal,
        Failure::TokenUnavailable => return Plan::Backoff { loud: false },
        Failure::Http {
            status,
            retry_after,
        } => {
            return match status {
                401 => refresh(state, provider),
                403 | 404 => Plan::Fatal,
                429 | 503 => match retry_after {
                    Some(d) => Plan::Fixed(d.min(MAX_SERVER_DELAY)),
                    None => Plan::Backoff { loud: false },
                },
                _ => Plan::Backoff { loud: false },
            };
        }
        Failure::Conn { fatal: true, .. } => return Plan::Fatal,
        Failure::Conn { ws_code: None, .. } => return Plan::Backoff { loud: false },
        Failure::Conn {
            phase,
            ws_code: Some(code),
            welcome_timeout,
            ..
        } => {
            // A semantic code above u16 came from an error code > 999, which
            // went out on the wire as 4002.
            let wire = u16::try_from(code).unwrap_or(4002);
            match classify_close(wire, phase, provider) {
                CloseClass::BackoffLoud if welcome_timeout => CloseClass::Backoff,
                c => c,
            }
        }
    };
    match class {
        CloseClass::Fatal => Plan::Fatal,
        CloseClass::RefreshTokenOnce => refresh(state, provider),
        CloseClass::ImmediateJitter2s => Plan::GoingAway,
        CloseClass::KeepaliveImmediateOnce if !state.keepalive_retry_used => {
            state.keepalive_retry_used = true;
            Plan::KeepaliveImmediate
        }
        CloseClass::KeepaliveImmediateOnce | CloseClass::Backoff => Plan::Backoff { loud: false },
        CloseClass::StartAtCap => Plan::StartAtCap,
        CloseClass::BackoffLoud => Plan::Backoff { loud: true },
    }
}

/// A rejected token: one refresh per stability window for a provider; a
/// static token, or a second rejection, is fatal.
fn refresh(state: &mut PolicyState, provider: bool) -> Plan {
    if provider && !state.refresh_retry_used {
        state.refresh_retry_used = true;
        Plan::Refresh
    } else {
        Plan::Fatal
    }
}

/// `min(cap, base * 2^attempt)`.
pub(crate) fn backoff_ceiling(policy: &ReconnectPolicy, attempt: u32) -> Duration {
    let factor = 1u32 << attempt.min(MAX_EXPONENT);
    policy.base.saturating_mul(factor).min(policy.cap)
}

/// Decides what to do about `f`. `provider` is whether the token is a
/// [`Token::Provider`](crate::Token::Provider); `rng` returns values in
/// `[0, 1)`.
///
/// Every retry except [`RetryKind::RefreshNow`] first checks `max_attempts`
/// (and whether reconnect is enabled), then increments `attempt`, then
/// computes the delay from the incremented value (Go client_reconnect.go
/// reportAndSchedule, JS connectOnce).
pub(crate) fn decide(
    state: &mut PolicyState,
    f: &Failure,
    policy: &ReconnectPolicy,
    provider: bool,
    rng: &mut impl FnMut() -> f64,
) -> Decision {
    let plan = classify(state, f, provider);
    let (kind, loud) = match plan {
        Plan::Fatal => return Decision::Fatal(FatalKind::Classified),
        // Not gated on `enabled`/`max_attempts`: the refresh reports a
        // non-fatal `Disconnected`, then `Redial{TokenRefresh}`, and is not
        // counted as an attempt.
        Plan::Refresh => {
            return Decision::Retry {
                delay: Duration::ZERO,
                kind: RetryKind::RefreshNow,
                loud: false,
            };
        }
        Plan::KeepaliveImmediate => (RetryKind::ImmediateKeepalive, false),
        Plan::Backoff { loud } => (RetryKind::Normal, loud),
        Plan::Fixed(_) | Plan::GoingAway | Plan::StartAtCap => (RetryKind::Normal, false),
    };
    if !policy.enabled {
        return Decision::Fatal(FatalKind::Disabled);
    }
    if let Some(max) = policy.max_attempts {
        if state.attempt >= max.get() {
            return Decision::Fatal(FatalKind::Exhausted(max.get()));
        }
    }
    state.attempt = state.attempt.saturating_add(1);
    let delay = match plan {
        Plan::KeepaliveImmediate => Duration::ZERO,
        Plan::Fixed(d) => d,
        Plan::GoingAway => going_away_jitter(policy, rng),
        Plan::StartAtCap => jitter(policy.cap, rng),
        _ => jitter(backoff_ceiling(policy, state.attempt), rng),
    };
    Decision::Retry { delay, kind, loud }
}

/// The delay before redialing after a `drain` without `retry_after_ms`, or a
/// 4012 / 1001 close: `random(0, min(2s, cap))`. WIRE.md §2.9 says 0–2000 ms;
/// a `cap` below 2 s is the application asking for faster redials, so it
/// shortens this bound too.
pub(crate) fn going_away_jitter(
    policy: &ReconnectPolicy,
    rng: &mut impl FnMut() -> f64,
) -> Duration {
    jitter(DRAIN_JITTER.min(policy.cap), rng)
}

/// `random(0, bound)`.
pub(crate) fn jitter(bound: Duration, rng: &mut impl FnMut() -> f64) -> Duration {
    bound.mul_f64(rng().clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    const S: Duration = Duration::from_secs(1);

    fn policy() -> ReconnectPolicy {
        ReconnectPolicy::default()
    }

    fn conn(phase: Phase, ws_code: u32) -> Failure {
        Failure::Conn {
            phase,
            ws_code: Some(ws_code),
            fatal: false,
            welcome_timeout: false,
        }
    }

    fn http(status: u16) -> Failure {
        Failure::Http {
            status,
            retry_after: None,
        }
    }

    fn abnormal(phase: Phase) -> Failure {
        Failure::Conn {
            phase,
            ws_code: None,
            fatal: false,
            welcome_timeout: false,
        }
    }

    /// `decide` with an RNG that always returns `r`.
    fn run(
        state: &mut PolicyState,
        f: &Failure,
        p: &ReconnectPolicy,
        provider: bool,
        r: f64,
    ) -> Decision {
        decide(state, f, p, provider, &mut || r)
    }

    fn retry(delay: Duration, kind: RetryKind) -> Decision {
        Decision::Retry {
            delay,
            kind,
            loud: false,
        }
    }

    fn normal(delay: Duration) -> Decision {
        retry(delay, RetryKind::Normal)
    }

    const FATAL: Decision = Decision::Fatal(FatalKind::Classified);

    #[test]
    fn backoff_increments_before_computing_and_saturates_at_cap() {
        let p = policy();
        let mut st = PolicyState::default();
        let f = abnormal(Phase::Connected);
        // rng 0.5: half of min(cap, base * 2^attempt) with attempt 1, 2, ...
        let want = [1, 2, 4, 8, 16, 30, 30, 30];
        for (i, secs) in want.into_iter().enumerate() {
            assert_eq!(
                run(&mut st, &f, &p, false, 0.5),
                normal(S * secs),
                "step {i}"
            );
            assert_eq!(st.attempt, i as u32 + 1);
        }
    }

    #[test]
    fn backoff_bounds() {
        let p = policy();
        let f = abnormal(Phase::Dial);
        for attempt in 0..40 {
            let mut lo = PolicyState {
                attempt,
                ..Default::default()
            };
            let mut hi = lo.clone();
            assert_eq!(run(&mut lo, &f, &p, false, 0.0), normal(Duration::ZERO));
            let Decision::Retry { delay, .. } = run(&mut hi, &f, &p, false, 0.999_999) else {
                panic!()
            };
            let ceiling = backoff_ceiling(&p, attempt + 1);
            assert!(delay < ceiling && delay <= p.cap, "{attempt}: {delay:?}");
        }
        // The exponent saturates instead of overflowing.
        assert_eq!(backoff_ceiling(&p, u32::MAX), p.cap);
        let tiny = ReconnectPolicy {
            base: Duration::from_nanos(1),
            cap: Duration::MAX,
            ..policy()
        };
        assert_eq!(backoff_ceiling(&tiny, 1000), Duration::from_nanos(1 << 30));
    }

    #[test]
    fn stability_resets_attempt_and_budgets() {
        let mut st = PolicyState {
            attempt: 7,
            keepalive_retry_used: true,
            refresh_retry_used: true,
        };
        st.stable();
        assert_eq!(st, PolicyState::default());
    }

    #[test]
    fn max_attempts_checked_before_increment() {
        let p = ReconnectPolicy {
            max_attempts: NonZeroU32::new(2),
            ..policy()
        };
        let mut st = PolicyState::default();
        let f = abnormal(Phase::Dial);
        assert!(matches!(
            run(&mut st, &f, &p, false, 0.5),
            Decision::Retry { .. }
        ));
        assert!(matches!(
            run(&mut st, &f, &p, false, 0.5),
            Decision::Retry { .. }
        ));
        assert_eq!(st.attempt, 2);
        assert_eq!(
            run(&mut st, &f, &p, false, 0.5),
            Decision::Fatal(FatalKind::Exhausted(2))
        );
        assert_eq!(st.attempt, 2);
        // Immediate and start-at-cap retries count too.
        let st = PolicyState {
            attempt: 2,
            ..Default::default()
        };
        for f in [
            conn(Phase::Connected, 4012),
            conn(Phase::Connected, 4014),
            conn(Phase::Connected, 4013),
        ] {
            assert_eq!(
                run(&mut st.clone(), &f, &p, false, 0.5),
                Decision::Fatal(FatalKind::Exhausted(2)),
                "{f:?}"
            );
        }
    }

    #[test]
    fn disabled_makes_every_retry_fatal_except_the_refresh() {
        let p = ReconnectPolicy::disabled();
        let mut st = PolicyState::default();
        for f in [
            abnormal(Phase::Connected),
            conn(Phase::Connected, 4012),
            http(503),
        ] {
            assert_eq!(
                run(&mut st, &f, &p, true, 0.5),
                Decision::Fatal(FatalKind::Disabled)
            );
        }
        assert_eq!(st.attempt, 0);
        assert_eq!(
            run(&mut st, &http(401), &p, true, 0.5),
            retry(Duration::ZERO, RetryKind::RefreshNow)
        );
    }

    #[test]
    fn refresh_is_not_gated_on_max_attempts() {
        let p = ReconnectPolicy {
            max_attempts: NonZeroU32::new(1),
            ..policy()
        };
        let mut st = PolicyState {
            attempt: 1,
            ..Default::default()
        };
        assert_eq!(
            run(&mut st, &abnormal(Phase::Dial), &p, true, 0.5),
            Decision::Fatal(FatalKind::Exhausted(1))
        );
        for f in [http(401), conn(Phase::Handshake, 4011)] {
            let mut st = st.clone();
            assert_eq!(
                run(&mut st, &f, &p, true, 0.5),
                retry(Duration::ZERO, RetryKind::RefreshNow),
                "{f:?}"
            );
            assert_eq!(st.attempt, 1);
        }
    }

    #[test]
    fn start_at_cap_after_welcome_only() {
        let p = policy();
        for code in [4009, 4014] {
            let mut st = PolicyState::default();
            assert_eq!(
                run(&mut st, &conn(Phase::Connected, code), &p, false, 0.5),
                normal(S * 30)
            );
            assert_eq!(st.attempt, 1);
            for phase in [Phase::Dial, Phase::Handshake] {
                let mut st = PolicyState::default();
                assert_eq!(run(&mut st, &conn(phase, code), &p, false, 0.5), normal(S));
            }
        }
    }

    #[test]
    fn going_away_jitters_up_to_two_seconds_after_welcome() {
        let p = policy();
        for code in [4012, 1001] {
            let mut st = PolicyState {
                attempt: 9,
                ..Default::default()
            };
            assert_eq!(
                run(&mut st, &conn(Phase::Connected, code), &p, false, 0.5),
                normal(S)
            );
            assert_eq!(st.attempt, 10);
            let mut st = PolicyState::default();
            assert_eq!(
                run(&mut st, &conn(Phase::Handshake, code), &p, false, 0.5),
                normal(S)
            );
        }
        // The jitter bound respects a smaller cap.
        let small = ReconnectPolicy {
            cap: Duration::from_millis(300),
            ..policy()
        };
        assert_eq!(
            run(
                &mut PolicyState::default(),
                &conn(Phase::Connected, 4012),
                &small,
                false,
                0.5
            ),
            normal(Duration::from_millis(150))
        );
    }

    #[test]
    fn keepalive_timeout_immediate_once_until_stability() {
        let p = policy();
        let mut st = PolicyState::default();
        let f = conn(Phase::Connected, 4013);
        assert_eq!(
            run(&mut st, &f, &p, false, 0.5),
            retry(Duration::ZERO, RetryKind::ImmediateKeepalive)
        );
        assert_eq!(st.attempt, 1);
        // Welcome-then-4013 again: normal backoff, budget stays spent.
        assert_eq!(run(&mut st, &f, &p, false, 0.5), normal(S * 2));
        assert_eq!(run(&mut st, &f, &p, false, 0.5), normal(S * 4));
        assert!(st.keepalive_retry_used);
        st.stable();
        assert_eq!(
            run(&mut st, &f, &p, false, 0.5),
            retry(Duration::ZERO, RetryKind::ImmediateKeepalive)
        );
        // Before welcome 4013 is plain backoff and leaves the budget alone.
        let mut st = PolicyState::default();
        assert_eq!(
            run(&mut st, &conn(Phase::Handshake, 4013), &p, false, 0.5),
            normal(S)
        );
        assert!(!st.keepalive_retry_used);
    }

    #[test]
    fn rejected_token_refreshes_once_with_provider() {
        let p = policy();
        for first in [http(401), conn(Phase::Handshake, 4011)] {
            for second in [http(401), conn(Phase::Handshake, 4011)] {
                let mut st = PolicyState {
                    attempt: 3,
                    ..Default::default()
                };
                assert_eq!(
                    run(&mut st, &first, &p, true, 0.5),
                    retry(Duration::ZERO, RetryKind::RefreshNow)
                );
                assert!(st.refresh_retry_used);
                assert_eq!(st.attempt, 3, "the refresh is not an attempt");
                assert_eq!(run(&mut st, &second, &p, true, 0.5), FATAL);
            }
        }
    }

    #[test]
    fn refresh_budget_survives_welcome_until_stability() {
        let p = policy();
        let mut st = PolicyState::default();
        assert!(matches!(
            run(&mut st, &http(401), &p, true, 0.5),
            Decision::Retry {
                kind: RetryKind::RefreshNow,
                ..
            }
        ));
        // The refresh dial welcomed, then the connection dropped before
        // stability: the budget is still spent.
        assert!(matches!(
            run(&mut st, &abnormal(Phase::Connected), &p, true, 0.5),
            Decision::Retry { .. }
        ));
        assert_eq!(
            run(&mut st, &conn(Phase::Handshake, 4011), &p, true, 0.5),
            FATAL
        );
        st.stable();
        assert!(matches!(
            run(&mut st, &http(401), &p, true, 0.5),
            Decision::Retry {
                kind: RetryKind::RefreshNow,
                ..
            }
        ));
    }

    #[test]
    fn non_rejection_failure_on_the_refresh_dial_backs_off() {
        let p = policy();
        let mut st = PolicyState::default();
        run(&mut st, &http(401), &p, true, 0.5);
        for f in [
            http(503),
            abnormal(Phase::Dial),
            conn(Phase::Handshake, 4001),
        ] {
            assert!(
                matches!(
                    run(&mut st.clone(), &f, &p, true, 0.5),
                    Decision::Retry {
                        kind: RetryKind::Normal,
                        ..
                    }
                ),
                "{f:?}"
            );
        }
        // Provider unavailable on the refresh call: budget spent, backoff.
        assert_eq!(
            run(&mut st, &Failure::TokenUnavailable, &p, true, 0.5),
            normal(S)
        );
        assert!(st.refresh_retry_used);
    }

    #[test]
    fn static_token_rejection_is_fatal() {
        let p = policy();
        for f in [
            http(401),
            conn(Phase::Handshake, 4011),
            conn(Phase::Dial, 4011),
        ] {
            let mut st = PolicyState::default();
            assert_eq!(run(&mut st, &f, &p, false, 0.5), FATAL, "{f:?}");
            assert!(!st.refresh_retry_used);
        }
    }

    #[test]
    fn fatal_set() {
        let p = policy();
        let fatal = [
            conn(Phase::Connected, 4010),
            conn(Phase::Handshake, 4010),
            conn(Phase::Connected, 4011),
            http(403),
            http(404),
            Failure::TokenFatal,
            Failure::Conn {
                phase: Phase::Dial,
                ws_code: None,
                fatal: true,
                welcome_timeout: false,
            },
        ];
        for f in fatal {
            for provider in [false, true] {
                let mut st = PolicyState::default();
                assert_eq!(run(&mut st, &f, &p, provider, 0.5), FATAL, "{f:?}");
                assert_eq!(st, PolicyState::default());
            }
        }
    }

    #[test]
    fn token_unavailable_is_a_counted_backoff() {
        let p = ReconnectPolicy {
            max_attempts: NonZeroU32::new(1),
            ..policy()
        };
        let mut st = PolicyState::default();
        assert_eq!(
            run(&mut st, &Failure::TokenUnavailable, &p, true, 0.5),
            normal(S)
        );
        assert_eq!(
            run(&mut st, &Failure::TokenUnavailable, &p, true, 0.5),
            Decision::Fatal(FatalKind::Exhausted(1))
        );
    }

    #[test]
    fn sdk_bug_closes_back_off_loudly_but_our_welcome_timeout_does_not() {
        let p = policy();
        for code in [4001, 4003, 4004] {
            for phase in [Phase::Handshake, Phase::Connected] {
                assert_eq!(
                    run(
                        &mut PolicyState::default(),
                        &conn(phase, code),
                        &p,
                        false,
                        0.5
                    ),
                    Decision::Retry {
                        delay: S,
                        kind: RetryKind::Normal,
                        loud: true
                    }
                );
            }
        }
        let timeout = Failure::Conn {
            phase: Phase::Handshake,
            ws_code: Some(4001),
            fatal: false,
            welcome_timeout: true,
        };
        assert_eq!(
            run(&mut PolicyState::default(), &timeout, &p, false, 0.5),
            normal(S)
        );
    }

    #[test]
    fn retry_after_is_honoured_and_clamped() {
        let p = policy();
        for status in [429, 503] {
            let f = Failure::Http {
                status,
                retry_after: Some(S * 7),
            };
            let mut st = PolicyState::default();
            assert_eq!(run(&mut st, &f, &p, false, 0.5), normal(S * 7));
            assert_eq!(st.attempt, 1);
            let f = Failure::Http {
                status,
                retry_after: Some(S * 86_400),
            };
            assert_eq!(
                run(&mut PolicyState::default(), &f, &p, false, 0.5),
                normal(MAX_SERVER_DELAY)
            );
            assert_eq!(
                run(&mut PolicyState::default(), &http(status), &p, false, 0.5),
                normal(S)
            );
        }
        // Retry-After on any other status is ignored.
        let f = Failure::Http {
            status: 500,
            retry_after: Some(S * 7),
        };
        assert_eq!(
            run(&mut PolicyState::default(), &f, &p, false, 0.5),
            normal(S)
        );
    }

    #[test]
    fn everything_else_backs_off() {
        let p = policy();
        let others = [
            abnormal(Phase::Dial),
            abnormal(Phase::Handshake),
            abnormal(Phase::Connected),
            http(400),
            http(500),
            http(502),
            conn(Phase::Connected, 1000),
            conn(Phase::Connected, 1006),
            conn(Phase::Connected, 1011),
            conn(Phase::Connected, 4000),
            conn(Phase::Connected, 4002),
            conn(Phase::Connected, 4999),
            // error code 1000 from the peer: semantic 5000, 4002 on the wire.
            conn(Phase::Connected, 5000),
            conn(Phase::Connected, 0x1000_0000 + 4000),
        ];
        for f in others {
            assert_eq!(
                run(&mut PolicyState::default(), &f, &p, true, 0.5),
                normal(S),
                "{f:?}"
            );
        }
    }
}

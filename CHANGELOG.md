# Changelog

All notable changes to `ws-mixer` (the Rust client SDK for ws-mixer.v1) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Planned as 0.2.0, the first published release. Implements the client role of ws-mixer spec
v0.6.1; conformance 24/24 client fixtures and 10/10 pair scenarios against ws-mixer-go v0.8.0.

### Added

- `Event::Redial(RedialNotice)`: every dial the client schedules is announced with its
  `conn_id`, `attempt`, the `delay` actually used and a `RedialCause` (`Failure`, `Drain`,
  `TokenRefresh`). It follows the `Disconnected` or `Drain` it answers and precedes the dial; the
  token-refresh redial, which still has no `Disconnected`, is no longer silent.
  `RedialCause::as_str` / `Display` give `"failure"`, `"drain"`, `"token-refresh"`.
- `ClientBuilder::agent_runtime` / `agent_os` override `hello.agent.runtime` / `os` (1 to 128
  characters, checked by `connect()`).
- `Client::close_immediate(message)`: `error{NO_ERROR, message}` and close 1000 at once (reason
  truncated to 123 bytes), without `close()`'s drain and 5 s wait; abandons a scheduled or
  running dial. For a peer whose streams are already stale, such as a service taken over by
  another connection.
- `ClientBuilder::no_redial_on_drain(DrainReason)`: no replacement dial (and no `Redial`) for a
  server drain with that reason; the application closes the client instead. For layers that treat
  a drain as a take-over, where a zero-delay replacement could otherwise send `hello`. A vetoed
  connection never redials: later drains on it are ignored, a replacement still pending from an
  earlier drain is abandoned, and its own end closes the client non-fatally unless a replacement
  is already live (also when it is failed with `ENHANCE_YOUR_CALM` because the event backlog was
  full and the `Drain` event was dropped). Abandoning a replacement prevents its `hello` only
  while its upgrade has not completed; such a dial is cancelled silently. A replacement past its
  upgrade is closed with `error{NO_ERROR}` and close 1000 whether or not its `welcome` arrived,
  and is never reported as `Connected`; the server will usually have seen its `hello` (an
  abandon landing between the upgrade and the `hello` write sends `error{NO_ERROR}` and 1000
  without a `hello` first).
- `test-util`: `RedialNotice::new_for_test()` and its setters.

### Changed

- An abandoned dial (by a close or a veto) that is past its upgrade is closed at once with
  `error{...}` and the matching close (`NO_ERROR`/1000, or `APPLICATION_CLOSE`/4014 for
  `close_with`), whether or not `welcome` arrived, rather than dropped or closed only when the
  client finishes. A dial not yet upgraded is still cancelled without an event. `close()` and
  `close_with()` return only once such a dial has closed too (bounded by about 2 s).
- The reader now emits `Event::Drain` before telling the supervisor to dial a replacement, so the
  replacement's `Event::Redial` always comes after the `Drain`, and dials no replacement when the
  `Drain` event was dropped because the event backlog is full.
- Spec pin v0.4.1 -> v0.6.1, ws-mixer-go pin v0.7.0 -> v0.8.0. The conformance results are
  unchanged (24/24, 10/10).
- The repository is a Cargo workspace with the new
  [`ws-mixer-mcpwarp`](ws-mixer-mcpwarp/CHANGELOG.md) crate, released on its own; the `ws-mixer`
  package excludes it.

## 0.1.0 (never published)

Implemented the client role of ws-mixer spec v0.4.1.

### Added

- `Client` / `ClientBuilder`: dial over `ws://` or `wss://` with the `ws-mixer.v1` subprotocol and
  a bearer token, `hello`/`welcome` handshake with a configurable timeout, `hello.meta`,
  capabilities, receive window, `max_streams` and extra upgrade headers, all validated by
  `connect()`. `ClientBuilder::agent` overrides `hello.agent`.
- Reconnect loop per WIRE.md §2.9: full-jitter exponential backoff, a stability window that resets
  the attempt counter and the one-shot budgets, `max_attempts`, the fatal close set, start-at-cap
  for 4009/4014 after `welcome`, a redial within 2 s for 4012/1001, one immediate retry for 4013,
  `Retry-After` on
  HTTP 429/503, and `ReconnectPolicy::disabled()`. Server-requested delays (`Retry-After`,
  `drain.retry_after_ms`) are capped at 5 minutes.
- `Token`: static or `TokenProvider` (also `Token::provider` for async closures), called on every
  dial. One silent refresh on HTTP 401 or a pre-welcome 4011. `TokenError::Unavailable` backs off,
  `TokenError::Fatal` stops the client.
- `Events` channel with `Connected`, `Stream`, `App`, `Drain` and `Disconnected` events, per
  connection ordering, one `Disconnected` per attempt and a per-connection backlog limit
  (`ENHANCE_YOUR_CALM`). Dropping `Events` closes the client. `Events` also implements
  `futures::Stream`.
- `DisconnectReason` with phase, semantic WebSocket close code, ws-mixer error code and name, HTTP
  status, peer close reason, cause, `fatal` and `retry_in`; `classify_close` exposes the close
  classification table.
- `Stream`: server-opened streams as tokio `AsyncRead + AsyncWrite` with per-stream flow control,
  half-close (`close_write`), `reset`, `refuse`, `into_split`, and `RESET(CANCEL)` on drop.
  Errors carry a downcastable `StreamError`; a dead connection is never reported as EOF.
- `Stream::closed` / `poll_closed` (also on both split halves): resolves when a stream ends, with a
  `StreamCloseReason` (`Finished`, `Reset { code, message, by_peer }` or `ConnectionLost`).
- `Client::send_app`, `close` (graceful drain then 1000), `close_with` (`APPLICATION_CLOSE`/4014),
  `drain`, `set_hello_meta`, `state`/`watch_state`, `wait_connected` (resolves with the live or next
  `welcome`, or the reason the client gave up) and cumulative `stats`.
- Server drain hand-over: the client dials a replacement in parallel while the draining
  connection finishes its streams.
- Keepalive with a jittered first ping and a pong watchdog; stream-0 inbound rate limit.
- TLS: rustls with the OS verifier (default), bundled webpki roots, native-tls, or a custom
  `rustls::ClientConfig`; ring (default) or aws-lc-rs as the explicit crypto provider, never the
  process-wide default.
- `test-util` feature: `new_for_test()` constructors for every event payload and
  `Stream::new_in_memory` with a scriptable `PeerHandle`.
- Conformance adapter (`conformance/adapter`, not published): 24/24 client fixtures and 10/10
  pair scenarios against ws-mixer-go v0.7.0.
- Examples `echo_server_client` and `token_provider`; an ignored `wss://` smoke test.

[Unreleased]: https://github.com/mcpwarp/ws-mixer-rust/commits/main

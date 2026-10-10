# Changelog

All notable changes to `ws-mixer` (the Rust client SDK for ws-mixer.v1) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First release, planned as 0.1.0. Implements the client role of ws-mixer spec v0.4.1.

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

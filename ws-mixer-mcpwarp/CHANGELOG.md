# Changelog

All notable changes to `ws-mixer-mcpwarp` (the mcpwarp layer over ws-mixer) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First release, planned as 0.1.0. Implements MCPWARP.md §2-§8 of ws-mixer spec v0.6.1 on
ws-mixer 0.2. Spec fixtures: envelope 60 (46 asserted, 14 edge-runner only), bridging 13 of 14,
tunnel 34/34, status text 102 table entries, 26 cases and the rules.

### Added

- Envelope (§2): `encode_register` / `encode_unregister` (byte-exact), `decode_envelope` (§2.8
  decode rules, defects reported in the spec checker's order), `InboundEnvelope`,
  `RegisteredService`, `ServiceError`, `UnregisteredService`, `RegisterErrorCode`, `AppErrorCode`,
  `TRANSIENT_REGISTER_ERRORS`, `McpwarpError`, `McpwarpErrorCode`, `ConfigError` (§5.2 messages
  verbatim, never echoing the value), `MCPWARP_V`, `DEFAULT_URL`.
- Names (§2.4): `SERVICE_NAME_RE`, `SERVICE_NAME_MAX`, `is_valid_service_name`,
  `INTEGRATION_ID_RE`, `is_valid_integration_id`, `ServiceKind`.
- Identity (§3.1, §8): `Identity`, `Agent`, `CLIENT_ID`, `VERSION`, `INTEGRATION_HEADER`;
  `hello.meta` `{client: "sdk-rust", version, os, integration}`, `X-Mcpwarp-Integration`,
  `User-Agent` `<sdk>/<v> (<runtime> <os>)`, applied to a `ws_mixer::ClientBuilder` with
  `Identity::apply`, which also vetoes the replacement dial for a `drain{replaced}` take-over
  (`no_redial_on_drain`).
- `session::Session`: the sans-IO registration state machine for one service (§2.7, §3.2, §3.3,
  and §5.9's superseded-connection rules), a port of ws-mixer-js `session.ts`. `ExposeState`,
  `Registration`, `Conflict`, `RetryInfo`.
- Request bridging (§4): `bridge::serve_stream`, `ServeContext`, `ServeState`,
  `RegistrationState`, `Handler` (any matching `tower_service::Service`), `handler_fn`,
  `RequestBody`, `MCP_PATH`, `DEFAULT_MAX_REQUEST_BODY_BYTES`, `HEAD_DEADLINE`, and with the
  `target` feature `Target` (also a `Service`, for forwarding to one of several local servers),
  `TargetBody`, `TargetUrlError`, `DEFAULT_TARGET_TIMEOUT`.
- `expose()` (§5): `ExposeConfig` / `ExposeConfigBuilder` (validated per §5.2, first failure
  wins), `ExposeHandle` (URL, server id, state and `watch_state`, the client, `events()`,
  idempotent `close()`), `ExposeEvent`. Dropping the `expose()` future is the §5.8 abort.
  `on_retry` reasons are §5.9's strings: `"drain"`, `"token-refresh"`,
  `"register failed: <CODE>: <message>"`.
- Tunnel controller (§6): `Tunnel` (`new`, `configure`, `stop`, `dispose`, `status`,
  `subscribe`, `watch_status`), `TunnelOptions` / `TunnelOptionsBuilder`, `TunnelConfig`,
  `Subscription`, `DEFAULT_DISPOSE_TIMEOUT`, `TunnelStatus`, `TunnelState`, `TunnelStatusCode`
  and the `last_error` constants.
- Status text (§7): `describe_status`, `describe_status_raw`, `DescribeOptions`,
  `StatusMessages`, `StatusText`, `STATUS_LOCALES` (`en`, `de`, `es`, `fr`, `ja`, `zh-CN`).
- `test-util` feature: `test_util::FakeEdge`, a scripted fake edge under `expose()` and `Tunnel`
  (`TunnelOptionsBuilder::fake_edge`).
- Cargo features: `target` (default) and the TLS features `rustls-platform-verifier` and `ring`
  (default), `rustls-webpki-roots`, `aws-lc-rs`, `native-tls`, forwarded to ws-mixer.
- Fixture runners for `spec/fixtures/mcpwarp` (`make mcpwarp-fixtures`) and examples
  `expose_echo` and `tunnel_settings`.

### Decisions

- Request bodies are buffered (up to `max_request_body_bytes`, 4 MiB by default). The bridging
  fixture `body_413_streamed_reset`, which applies only to adapters that stream the body, is
  noted and skipped.
- `target` mode forwards over plain `http://` only (hyper); an `https://` target is a
  `ConfigError`.
- A take-over (`conflict` op or `drain{replaced}`) closes with `error{NO_ERROR}` and WebSocket
  close 1000 at once (`ws_mixer::Client::close_immediate`), skipping the §2.10 drain, and never
  sends `hello` or `register` on a replacement.
- The tunnel publishes a status when its actor task processes an input, not when the call is
  made; calls are queued in call order.
- TLS is set once on `ExposeConfig` / `TunnelOptions` (`tls(ws_mixer::TlsConfig)`), the Rust form
  of §8's WebSocket factory.
- `serde_json` is built with `preserve_order` so `register` / `unregister` keep their key order
  byte for byte.
- The request target is percent-encoded per the §4.3 rule of the upcoming spec v0.6.3 (fragment
  dropped, bytes outside RFC 3986's allowed set escaped as `%XX`, `%` kept); the v0.6.1 text
  forwards the origin-form target as is.
- A handler response status below 200 or from 600 up is answered `500 handler error` and
  reported.
- The 64 KiB request head cap counts the blank line that ends the head.
- Divergence: in target mode a request the HTTP types cannot represent (e.g. a control character
  in a header value) is answered `400 request cannot be represented as a web Request`, as §4.5
  words it; the JS SDK answers `500 internal error` and the Go CLI most likely `502`.

[Unreleased]: https://github.com/mcpwarp/ws-mixer-rust/commits/main

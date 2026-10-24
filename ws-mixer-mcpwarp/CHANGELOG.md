# Changelog

All notable changes to `ws-mixer-mcpwarp` (the mcpwarp layer over ws-mixer) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.1 (unreleased)

The first release published to crates.io: 0.1.0 was tagged (`ws-mixer-mcpwarp-v0.1.0`) but never
published, so the one source-breaking change below (`TunnelConfig::token_provider`) ships in a
patch. Implements ws-mixer spec v0.8.0 (pin moved v0.6.1 -> v0.6.2 -> v0.7.0 -> v0.7.2 ->
v0.8.0, tag `5962fec`) for one service per connection, every SDK's scope (§6A is host guidance);
ws-mixer-go pin v0.8.0 -> v0.9.0; depends on ws-mixer 0.2.1. Spec fixtures:
envelope 68 (52 asserted, 14 edge-runner only, 2 host-guidance skipped; 0.1.0: 60/46), bridging 22
of 27 (0.1.0: 13 of 14), tunnel 44 of 58 with the 14 `ms_*` skipped (0.1.0: 34/34), status text
144 table entries, 37 cases and the rules (0.1.0: 102, 26). Transport conformance: 25/25 client
fixtures, 10/10 pair scenarios.

### Added

- `TunnelConfig::token_provider: Option<ws_mixer::Token>` (§6.2 Token provider): a provider called
  on every dial, used instead of `token` when set. `#[serde(skip)]` (never in the settings JSON),
  `Debug` prints `Token::Provider(..)`, and `PartialEq` and `configure`'s no-op check compare a
  provider by identity (the same `Arc`). A `Token::Static` there acts as `token` would. A struct
  literal now needs `..TunnelConfig::default()`. Tunnel fixtures T46-T48:
  `TokenError::Unavailable` is a failed dial with backoff, `TokenError::Fatal` is
  `error`/`DISCONNECTED_FATAL`, and the first token rejected before `welcome` gets one immediate
  refresh and redial (`on_retry` `{attempt: 1, delay: 0, reason: "token-refresh"}`); a second
  rejection is `error`/`UNAUTHORIZED`.
- `describe_service_status_raw(state, code, locale)` (§6A.9): one service's `service.<state>` label
  and its detail (`rejected`: the sentence for its register code; `disabled` with `DISABLED`: the
  dashboard sentence), over wire strings and without overrides; a multi-service host builds its
  own spec-shaped `describe_service_status` (§9.2) over it (§6A, informative). `describe_status` and
  `describe_status_raw` give `error`/`INTERNAL` with `lastError` `"No service could be
  registered."` the `lastError.NO_SERVICE_REGISTERED` detail.
- `bridge::RequestInfo` and `ServeContext::state_for_request(Fn(&RequestInfo) -> ServeState)`:
  the state read per request, after the head and before the body, for a host serving several
  services on one client. A state with `conflict` set resets the stream with `CANCEL` "service
  taken over by another connection"; one not `Registered` is answered `503` before the body is
  read (§4.4 row 3, so it beats `413`). The state given to `ServeContext::new` is then read only
  when the stream arrives (row 0).
- `bridge::TargetReply` (`Unreachable { code, .. }`, `Timeout`, with `status()`) and
  `bridge::target_reply(&Response<B>)` (`target` feature): tells a `Target`'s own `502`/`504`
  from the local server's responses. The tag survives in-place edits, not a rebuilt response.
- New spec fixtures pass: tunnel `36_t32_http_401_upgrade_unauthorized` (an observable `401` on the
  upgrade is `UNAUTHORIZED`, `403`/`404` `DISCONNECTED_FATAL`, `503` `RETRYING`) and
  `37_t27_http_401_upgrade_unobservable_retrying`; envelope `registered_multiple_defects`,
  `registered_defects_id_before_url_created` and `registered_defects_url_before_created`; at
  v0.7.0 also tunnel `38`-`45` (token provider, held and stale `disable`, drain `retry_after`,
  `TOO_MANY_SERVICES`, dispose in conflict) and the new single-service bridging fixtures
  (`handler_connect_400`, `head_mcp_400_headonly`, the `target_*` encoding set and others).

### Changed

- A `disable` with an unknown id while no server id is known is held and applied only if the next
  successful `registered` carries that id (a transient `registered{errors}` keeps the hold); it is never adopted as the server id (the edge sends
  `disable` to every connection of the owner, so before `registered` the id may be another
  service's). The latest held `disable` wins; it is dropped on a disconnect, on every `welcome`,
  `close`, abort, take-over, a matching `enable`, an `enable` carrying the held id (even another
  service's) or a `SERVER_DISABLED` refusal. `Session::server_id` and `ExposeHandle::server_id`
  therefore come from `registered`, `enable` or `SERVER_DISABLED` only. Normative in v0.7.0 (§2.7
  Unknown `disable` id and Held `disable` lifetime, fixture `42_disable_unknown_id_held`).
- T50: a `registered` that brings a held `disable`'s id gives the tunnel one `disabled`/`DISABLED`
  snapshot, with no `online` before it. `ExposeHandle` still emits `Registered` then `Disabled`.
- T51 (§2.7 Ended connection): a `registered`, `disable` or `enable` delivered after its
  connection ended is ignored; `conflict` and `error` from it still apply, and a superseded
  connection's ops stay ignored. `Session::is_superseded` now stays `true` after a superseded
  connection ends (it was `false` once `on_disconnected` had run for it).
- `Session` follows the frames-in-flight rules (§2.6 One in flight, §3.2; v0.7.2, clarified in
  v0.8.0):
  - `OVERLOADED` keeps its own counter: the re-send waits `min(1000 × 2^(n−1), 30000)` ms (was
    1000 ms every time; `on_retry` reports that delay). The counter resets on any reply that
    settles the outstanding frame and on every `welcome`.
  - One `register`/`unregister` in flight. A `register` trigger (`enable`, the transient retry
    timer) while a frame is outstanding is queued and sent once that frame is answered; one that
    arrives while the frame waits out an `OVERLOADED` folds into that re-send. Any settling reply
    cancels the pending re-send timers.
  - A whole-frame `error` with any code except `OVERLOADED` and `CONNECTION_LIMIT`, unknown codes
    included, answers the outstanding frame (`UNSUPPORTED_VERSION` is still fatal).
  - A `registered` that lists none of this service's entries, empty included, answers the
    outstanding `register` and is re-sent after the transient backoff (`on_retry` reason
    `"register failed: registered reply did not list the service"`).
  - During an `OVERLOADED` pause the dropped `register` is not on the wire: a whole-frame `error`
    (still emitted, `UNSUPPORTED_VERSION` still fatal) or a `registered` listing none of the
    service's entries answers nothing, and the re-send goes out as scheduled. A `registered`
    naming the service still settles the handle and cancels the re-send. Likewise during
    `close()`: a whole-frame `error` while the `unregister` is paused does not end the close wait,
    which runs to the re-sent `unregister`'s reply or the 2000 ms cap.
  - While closing, the `OVERLOADED` that drops the `register` ahead of the `unregister` counts
    towards the next pause; the 2000 ms cap cancels an `unregister` re-send still paused, and a
    re-send sends nothing once the connection dropped or a replacement was welcomed during the
    pause (it could go out after the connection was gone).
  - The transient register backoff resets on a `registered` with a non-transient entry for the
    service (a success, a `SERVER_DISABLED` entry or a refusal) and on an `unregistered`; a
    whole-frame `error` leaves it unchanged (it was reset by a success only).
  - `enable` matches by name or by server id.
  - `close()`'s wait ends on `unregistered` or on a whole-frame `error` whose turn it is: a
    `register` still unanswered when the `unregister` went out takes the first reply.
  - `close()` skips its `unregister` when every `register` the connection sent was dropped by
    `OVERLOADED` and no `registered` arrived on it, and closes at once; a `register` answered by
    a whole-frame `error` or an empty `registered` still gets one (it was sent whenever a
    `register` had gone out).
  - `Session` assumes it is the only controller on its connection: replies are not matched by
    name. A host running several sessions on one connection must hold back each session's
    triggers until the connection's slot is free and route each reply to the session whose frame
    it answers, by frame order.
- Request bridging: `CONNECT`, `TRACE` and `TRACK` (any case) are answered `400 request cannot be
  represented as a web Request` in handler and target mode alike (§4.5), the application not
  called.
- With ws-mixer 0.2.1, the first token a provider gets rejected before `welcome` also shows as a
  non-fatal `ExposeEvent::Disconnect` before the token-refresh redial.
- spec v0.6.2 makes the 0.1.0 rulings normative (take-over close, upgrade-status mapping, envelope
  defect order, `hello.meta.client`, the TLS hook), and v0.7.0 the §4.3 target percent-encoding;
  no behaviour change.

### Fixed

- `Session::on_disconnected` ignores a connection the session never saw welcomed or draining
  while another connection is live (returns `true`). On a shared client, a session created after
  an earlier connection ended was reset to `connecting` by that connection's `Disconnected`.
- A `HEAD` request refused while its head is parsed (`400`, `431`, `400 incomplete request`) gets
  a head-only reply, as every other SDK-generated reply to `HEAD` (§4.5).

### Decisions

- `serde_json` stays built with `preserve_order`: `register` / `unregister` keep the spec's key
  order byte for byte, and ws-mixer's `send_app` takes a `serde_json::Value`. It is a feature of
  the host's whole dependency graph; the README says how to check.
- One service per connection, every SDK's scope; §6A is informative host guidance since v0.8.0:
  no service group. The encode side of envelope `register_two_services` and
  `register_batch_valid`, the bridging `multi_service_*` (4) and tunnel `ms_01`..`ms_14` are host
  guidance (§10.6) and skipped; the status-text `hostOnly` entries are embedded and checked.
- There is no `trust_server_id_header` (§4.8: an SDK does no routing and has no such option):
  every request is served by the one exposed service, never routed by `X-Mcpwarp-Server-Id` or
  `X-Mcpwarp-Server-Name`, which reach the application as ordinary headers.

## 0.1.0 (tagged `ws-mixer-mcpwarp-v0.1.0`, not published)

First release. Implements MCPWARP.md §2-§8 of ws-mixer spec v0.6.1 on
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

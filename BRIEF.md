# ws-mixer-rust

Rust client SDK for the ws-mixer v1 protocol. Public crate for third-party MCP
developers who want a remote transport, and the transport layer of the
`mcpwarp` desktop app (sibling repo `mcpwarp-desktop`).

## Contract

- Spec: `../ws-mixer-spec` (pin the current tag in `spec.pin`, same convention
  as `ws-mixer-go` and `ws-mixer-js`). `docs/WIRE.md` is normative,
  `docs/CLIENT-SDK.md` describes expected client behaviour, `docs/CONFORMANCE.md`
  describes the adapter protocol.
- Reference implementations: `../ws-mixer-go` (`wsmixer/`), `../ws-mixer-js`.
- Ship a `conformance-adapter` binary and `conformance/COUNTS.json` so the spec
  runner can test this SDK like the others.

## What the desktop app needs from the crate

- Async (tokio) `Client` with the reconnect state machine from spec §2.9
  (full-jitter backoff, stability window, fatal vs retryable closes), a token
  provider callback with a "temporarily unavailable" error variant, and
  callbacks or channels for connect / disconnect(reason) / stream / app / drain.
- `Stream` implementing `AsyncRead + AsyncWrite` with half-close and reset.
- `send_app(json)` on stream 0 and a way to set `hello.meta`.
- Custom `Authorization` header on the upgrade (required by spec; browser
  WebSocket cannot do this, so the crate is native-only).
- No mcpwarp-specific logic. The `{"mcpwarp":{...}}` envelope lives in the
  desktop repo.

## Coordination

The desktop session orchestrator will message this session for API questions.

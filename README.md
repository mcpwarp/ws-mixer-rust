# ws-mixer

Rust client SDK for the [ws-mixer.v1](https://github.com/mcpwarp/ws-mixer-spec/blob/v0.6.2/docs/WIRE.md)
protocol: many independent, flow-controlled byte streams multiplexed over one WebSocket.

The server opens streams; the client accepts each one as a `Stream` implementing tokio's
`AsyncRead + AsyncWrite`, exchanges opaque JSON `app` messages on the control channel, and
reconnects on its own following the spec's backoff, drain and fatal-close rules. Everything the
application reacts to arrives on one `Events` channel.

Native only (tokio). TLS via rustls with the OS trust store by default.

Sibling SDKs: [ws-mixer-go](https://github.com/mcpwarp/ws-mixer-go) (protocol core, client and
server side) and [ws-mixer-js](https://github.com/mcpwarp/ws-mixer-js) (Node client).

## Status

0.2.0, not yet released. Implements the client role of spec **v0.6.2** (`spec.pin`). The spec
runner's conformance suite passes in full:

- fixture mode: 24/24 client fixtures
- pair mode: 10/10 scenarios with ws-mixer-go **v0.9.0** (`goserver.pin`) as the server

Floors are checked in at `conformance/COUNTS.json`. Fixtures from the upcoming spec v0.6.3 (not
tagged yet) are picked up when `spec.pin` moves.

New in 0.2.0: additive API, with the close and drain behaviour changes listed under "Changed" in
the CHANGELOG:

- `Event::Redial(RedialNotice)` announces every dial the client schedules, with its
  `RedialCause` (failure, drain replacement, token refresh), attempt and the delay actually used.
- `ClientBuilder::agent_runtime` / `agent_os` override `hello.agent.runtime` / `os`.
- `ClientBuilder::no_redial_on_drain(reason)` dials no replacement for a drain with that reason
  (for layers that treat it as a take-over).
- `Client::close_immediate(message)` sends `error{NO_ERROR}` and close 1000 at once, without
  `close()`'s drain and wait.

See [CHANGELOG.md](CHANGELOG.md) for the details.

## Workspace

| Crate | Path | Version | What |
|---|---|---|---|
| `ws-mixer` | `/` | 0.2.0 | this crate: the ws-mixer.v1 client |
| [`ws-mixer-mcpwarp`](ws-mixer-mcpwarp/) | `ws-mixer-mcpwarp/` | 0.1.1 | the mcpwarp layer on top |
| `ws-mixer-conformance-adapter` | `conformance/adapter/` | not published | the spec runner's adapter |

Each published crate has its own README, CHANGELOG, LICENSE and NOTICE, and is released on its
own (see [Releasing](#releasing)).

## mcpwarp layer

[`ws-mixer-mcpwarp`](ws-mixer-mcpwarp/README.md) implements
[MCPWARP.md](https://github.com/mcpwarp/ws-mixer-spec/blob/v0.6.2/docs/MCPWARP.md) §2-§8 of spec
v0.6.2 on this crate: expose a local MCP server at a public mcpwarp URL. It offers `expose()`
(one service, one call), `Tunnel` (a controller for apps with a settings UI, with status text in
six locales) and the Tier 1 building blocks (envelope, codes, identity, the sans-IO registration
machine, request bridging) for agents that run their own `ws_mixer::Client`. It passes every
agent-side mcpwarp fixture set of the spec: envelope 63 (49 asserted, 14 edge-runner only),
bridging 13 of 14 (one applies only to streamed request bodies), tunnel 36/36, status text 102
table entries and 26 cases.

## Install

Not on crates.io yet; depend on a checkout:

```toml
[dependencies]
ws-mixer = { path = "../ws-mixer-rust" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "io-util"] }
serde_json = "1"
```

Once published: `ws-mixer = "0.2"`.

## Quick start

```rust
use tokio::io::AsyncWriteExt;
use ws_mixer::{Client, Event, SendAppError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut events) = Client::builder("wss://edge.example.com/v1/tunnel")
        .token("secret-token")
        .connect()?;

    while let Some(event) = events.recv().await {
        match event {
            Event::Connected(c) => {
                println!("conn {} up, session {}, reconnect {}", c.conn_id, c.session, c.reconnect);
                // Nothing is replayed after a reconnect: re-announce state here.
                match client.send_app(serde_json::json!({ "op": "ready" })).await {
                    Ok(()) | Err(SendAppError::NotConnected) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Event::Stream(stream) => {
                // Serve each stream on its own task; never block the event loop.
                tokio::spawn(async move {
                    let (mut rx, mut tx) = stream.into_split();
                    tokio::io::copy(&mut rx, &mut tx).await?; // until the peer's CLOSE
                    tx.shutdown().await // our CLOSE
                });
            }
            Event::App(msg) => println!("app on conn {}: {:?}", msg.conn_id, msg.body),
            Event::Drain(d) => println!("conn {} draining: {}", d.conn_id, d.reason),
            Event::Disconnected(r) => {
                println!("conn {:?} ended ({} phase): {r}", r.conn_id, r.phase);
                if r.fatal {
                    break;
                }
            }
            _ => {} // `Event` is non-exhaustive
        }
    }
    client.close().await;
    Ok(())
}
```

For a token that changes between dials, pass `Token::provider(|| async { ... })` or implement
`TokenProvider`; return `TokenError::unavailable(e)` for failures worth retrying and
`TokenError::fatal(e)` for ones that are not.

Examples (`cargo run --example <name>`):

- `echo_server_client`: the quick start as a runnable program, with Ctrl-C handling.
- `token_provider`: a file-backed `TokenProvider` with `Unavailable`/`Fatal` handling, events
  mapped to serializable UI messages and a status line driven by `Client::watch_state`; the
  shape a Tauri bridge takes, without the Tauri dependency.

## Features

| Feature | Default | Effect |
|---|---|---|
| `rustls-platform-verifier` | yes | rustls verifying against the OS trust store (`TlsConfig::PlatformVerifier`) |
| `ring` | yes | rustls crypto provider: ring |
| `rustls-webpki-roots` | | rustls with the bundled Mozilla roots (`TlsConfig::WebpkiRoots`) |
| `aws-lc-rs` | | rustls crypto provider: aws-lc-rs; wins over `ring` when both are on |
| `native-tls` | | the platform TLS library (`TlsConfig::NativeTls`); alone, rustls is not compiled |
| `test-util` | | `new_for_test()` constructors for every event payload and an in-memory `Stream` with a scriptable peer (`ws_mixer::test_util`), for testing consumers without a server |
| `conformance` | | test-only knobs for the spec conformance adapter; not for applications |

The SDK builds each rustls config with an explicit crypto provider and never touches rustls'
process-wide default, so ring and aws-lc-rs can coexist in one dependency graph (common in Tauri
apps) without the "no process-level CryptoProvider" panic. To drop ring entirely:
`default-features = false, features = ["rustls-platform-verifier", "aws-lc-rs"]`. A TLS stack is
required even for `ws://`-only use; a build without one fails with `compile_error!`.

MSRV: Rust 1.85 (edition 2024).

## Lifecycle rules

The crate docs spell these out; in short:

- **`Events` is the lifeline.** Dropping it closes the client gracefully. Dropping only the
  `Client` handles does not. Stop consuming and a connection whose backlog exceeds
  `max(128, max_streams + 64)` app events is failed with `ENHANCE_YOUR_CALM`.
- **One `Disconnected` per connection attempt**, after all of that connection's other events.
  Exceptions: a pre-welcome token rejection that a provider refreshes is retried without a
  `Disconnected`, and a dial abandoned by `close()`, `close_with()`, `close_immediate()` or a
  `no_redial_on_drain` veto reports nothing.
- **Every scheduled dial is announced.** `Event::Redial` carries the cause (failure, drain
  replacement, token refresh), the attempt number and the delay actually used; it follows the
  `Disconnected` or `Drain` it answers.
- **`fatal` is not the same as final.** `fatal` means the client gave up on an error. `close()`,
  `close_with()` and `close_immediate()` are non-fatal but final, and so is the end of a
  connection whose drain was vetoed with `no_redial_on_drain`. `ClientState::Closed` is published
  when closing begins (no more redials); connections still draining after a hand-over may report
  `Disconnected` after that, even after a fatal one. `Events::recv()` returning `None` is the only
  end-of-events signal. `retry_in` is set when a redial is scheduled.
- **Drain hand-over overlaps connections.** The replacement's `Connected` can arrive before the
  draining connection's `Disconnected`; key per-connection state on `conn_id`.
- **`send_app` never queues.** No welcomed connection means `SendAppError::NotConnected`.
  Re-send what the server needs on every `Connected`, or put it in `hello_meta`.
- **Streams belong to one connection.** Read them continuously. A dead connection fails reads and
  writes with `ConnectionAborted`, never a clean EOF. Dropping a stream that is not fully closed
  sends `RESET(CANCEL)`; finish with `close_write()`/`shutdown()` plus reading to EOF, or decline
  with `refuse()`. `closed().await` resolves when the stream ends, with a `StreamCloseReason`
  (finished, reset by either side, or connection lost).

## Development

```sh
make test                  # cargo test (ws-mixer and ws-mixer-mcpwarp)
make clippy                # clippy, all targets and features, warnings denied
make fmt                   # cargo fmt
make mcpwarp-fixtures      # ws-mixer-mcpwarp's spec fixture tests against .spec/ (with test-util)
make smoke                 # build the conformance adapter and check it starts
make conformance-fixture   # spec runner, fixture mode (needs Go)
make conformance-pair      # spec runner, ws-mixer-go server x this client (needs Go)
cargo deny check           # licenses, advisories, duplicate crates
```

`make fetch-spec` and `make fetch-goserver` shallow-clone ws-mixer-spec at `spec.pin` (v0.6.2)
into `.spec/` and ws-mixer-go at `goserver.pin` (v0.9.0) into `.goserver/`, re-cloning when the
pin changes; the conformance targets do this on their own. The spec fixture tests look in
`$WSMIXER_SPEC_DIR`, then `.spec/` (ws-mixer also tries `../ws-mixer-spec`), and are skipped when
none exists; ws-mixer-mcpwarp's fail instead when `CI` is set.

Tests that need the network are ignored by default:

```sh
cargo test --all-features --test wss_smoke -- --ignored --nocapture
```

dials a public `wss://` echo server (override with `WSMIXER_SMOKE_URL`) with each compiled TLS
option and checks that TLS and the HTTP upgrade complete.

## For mcpwarp-desktop

How a desktop host that exposes several local MCP servers should use the mcpwarp layer:

- **Use the Tier 1 modules, not `Tunnel`.** `Tunnel` and `expose()` manage one service on one
  client. A host with several services runs its own `ws_mixer::Client` (configured with
  `Identity::apply`) and composes `session::Session`, `bridge::serve_stream` and
  `describe_status`.
- **One `session::Session` per service.** On every `Connected`, call `on_welcome` on each session
  and execute the `Command::SendApp` commands it queues (that is the `register`); do not call
  `encode_register` yourself. Feed every session the same transport events (`on_app`,
  `on_drain`, `on_disconnected`); a session ignores envelopes for other names. A session created
  while a connection is already live needs `on_welcome(<live conn>)` from you; it is not
  retroactive.
- **Handle a session's close commands per service.** `Command::Fatal` and
  `Command::CloseGraceful` mean "close the client" for a single-service agent. On a shared
  client, treat them as the end of that one service: drop or reset its session and stop routing
  its host, and keep the connection open for the others.
- **A `conflict` is per service, a `drain{replaced}` per connection.** The `Conflict` event's
  reason (or `on_drain` returning `true`) tells them apart. `"replaced"`: every session on the
  connection is in conflict; close the client at once with `Client::close_immediate` and never
  redial (`Identity::apply` already vetoes the replacement dial). Any other reason
  (`"taken_over"`): that one service lost its name to another install; keep the connection and
  the other services, ignore that session's `Command::CloseImmediate`, send no `unregister` for
  it, and never `register` the name again until the user reconfigures it. Its `Host` must get
  `RESET CANCEL`, not a response: return `ServeState::new(..).conflict(true)` for it from
  `ServeContext::state_for_request`.
- **The integration id is one constant product UUID**, the same on every install, passed to
  `Identity::new`. It is not a per-user or per-install value.
- **Only `/mcp` is forwarded.** `serve_stream` answers every other path `404`; a child server
  that listens elsewhere is reached through its `Target` URL, not through the public path.
- **Dispatch on `Host` in one handler.** Pass one `handler_fn` to `serve_stream` that picks the
  child by `req.uri().host()` (each service has its own public host) and calls that child's
  `Target`, cached per child and cloned per request (clones share the connection pool). The
  ws-mixer-mcpwarp README has the full snippet.
- **Availability is per `Host`.** `ServeContext::state_for_request` reads the state per request,
  after the head and before the body: return `Disabled` for a paused service and `Unavailable`
  while its child restarts, `Registered` for the rest, and `serve_stream` answers that host's
  `503` before reading the body. Answer your own `503` from the handler only when you need a
  `Retry-After` header; `bridge::target_reply` tells you a `Target` found its child down.
- **Features: keep the defaults** (`target`, `rustls-platform-verifier`, `ring`). Do not enable
  `aws-lc-rs`. The SDK always passes an explicit rustls provider, so an `aws-lc-rs` that arrives
  through another dependency (common in Tauri) coexists with `ring` without the process-default
  panic. ws-mixer-mcpwarp also turns on `serde_json/preserve_order` for the whole graph
  (`cargo tree -e features -i serde_json | grep preserve_order` shows whether it was already on).

## Releasing

The two crates are versioned and tagged separately, with the crate name in the tag:
`ws-mixer-v0.2.0` and `ws-mixer-mcpwarp-v0.1.0`. ws-mixer-mcpwarp depends on
`ws-mixer = { path = "..", version = "0.2" }`, so ws-mixer goes first, and ws-mixer-mcpwarp's
own `cargo publish --dry-run` resolves `ws-mixer` from crates.io and fails until 0.2.0 is there.

From a clean `main` with CI green:

```sh
# 1. Checks
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
make mcpwarp-fixtures conformance-fixture conformance-pair
cargo deny check

# 2. ws-mixer
cargo publish --dry-run -p ws-mixer
cargo publish -p ws-mixer
git tag -a ws-mixer-v0.2.0 -m "ws-mixer 0.2.0"

# 3. ws-mixer-mcpwarp: step 2's ws-mixer 0.2.0 must be in the crates.io index first
#    (step 2's `cargo publish` waits for that before it returns)
cargo publish --dry-run -p ws-mixer-mcpwarp
cargo publish -p ws-mixer-mcpwarp
git tag -a ws-mixer-mcpwarp-v0.1.0 -m "ws-mixer-mcpwarp 0.1.0"

git push origin ws-mixer-v0.2.0 ws-mixer-mcpwarp-v0.1.0
```

Before step 2, `cargo publish --dry-run --workspace` (cargo 1.90+) packages both crates and
verifies ws-mixer-mcpwarp against the packaged ws-mixer, without crates.io. Cargo treats that
packaged ws-mixer 0.2.0 like any registry crate, keyed by name and version, so an earlier
dry-run's extraction (`~/.cargo/registry/src/*/ws-mixer-0.2.0`) and build are reused after the
source changed; the symptom is a ws-mixer API reported missing. Delete that directory and run
with a fresh `CARGO_TARGET_DIR`.

ws-mixer-mcpwarp's package leaves out `tests/` (the fixture runners need the spec checkout), so
cargo prints one "ignoring test" warning per test target. Move each crate's `[Unreleased]`
CHANGELOG heading to the version and date in the release commit.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

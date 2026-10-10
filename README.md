# ws-mixer

Rust client SDK for the [ws-mixer.v1](https://github.com/mcpwarp/ws-mixer-spec/blob/v0.4.1/docs/WIRE.md)
protocol: many independent, flow-controlled byte streams multiplexed over one WebSocket.

The server opens streams; the client accepts each one as a `Stream` implementing tokio's
`AsyncRead + AsyncWrite`, exchanges opaque JSON `app` messages on the control channel, and
reconnects on its own following the spec's backoff, drain and fatal-close rules. Everything the
application reacts to arrives on one `Events` channel.

Native only (tokio). TLS via rustls with the OS trust store by default.

Sibling SDKs: [ws-mixer-go](https://github.com/mcpwarp/ws-mixer-go) (protocol core, client and
server side) and [ws-mixer-js](https://github.com/mcpwarp/ws-mixer-js) (Node client).

## Status

0.1.0, not yet released. Implements the client role of spec **v0.4.1** (`spec.pin`). The spec
runner's conformance suite passes in full:

- fixture mode: 24/24 client fixtures
- pair mode: 10/10 scenarios with ws-mixer-go **v0.7.0** (`goserver.pin`) as the server

Floors are checked in at `conformance/COUNTS.json`.

## Install

Not on crates.io yet; depend on a checkout:

```toml
[dependencies]
ws-mixer = { path = "../ws-mixer-rust" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "io-util"] }
serde_json = "1"
```

Once published: `ws-mixer = "0.1"`.

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
  Exceptions: a pre-welcome token rejection that a provider refreshes is retried silently, and a
  dial abandoned by `close()` reports nothing.
- **`fatal` is not the same as final.** `fatal` means the client gave up on an error. `close()`
  and `close_with()` are non-fatal but final. `ClientState::Closed` is published when closing
  begins (no more redials); connections still draining after a hand-over may report
  `Disconnected` after that, even after a fatal one. `Events::recv()` returning `None` is the only
  end-of-events signal. `retry_in` is set when a redial is scheduled.
- **Drain hand-over overlaps connections.** The replacement's `Connected` can arrive before the
  draining connection's `Disconnected`; key per-connection state on `conn_id`.
- **`send_app` never queues.** No welcomed connection means `SendAppError::NotConnected`.
  Re-send what the server needs on every `Connected`, or put it in `hello_meta`.
- **Streams belong to one connection.** Read them continuously. A dead connection fails reads and
  writes with `ConnectionAborted`, never a clean EOF. Dropping a stream that is not fully closed
  sends `RESET(CANCEL)`; finish with `close_write()`/`shutdown()` plus reading to EOF, or decline
  with `refuse()`.

## Development

```sh
make test                  # cargo test
make clippy                # clippy, all targets and features, warnings denied
make fmt                   # cargo fmt
make smoke                 # build the conformance adapter and check it starts
make conformance-fixture   # spec runner, fixture mode (needs Go)
make conformance-pair      # spec runner, ws-mixer-go server x this client (needs Go)
cargo deny check           # licenses, advisories, duplicate crates
```

`make fetch-spec` and `make fetch-goserver` shallow-clone ws-mixer-spec at `spec.pin` (v0.4.1)
into `.spec/` and ws-mixer-go at `goserver.pin` (v0.7.0) into `.goserver/`; the conformance
targets do this on their own. The spec fixture tests look in `$WSMIXER_SPEC_DIR`, then `.spec/`,
then `../ws-mixer-spec`, and are skipped when none exists.

Tests that need the network are ignored by default:

```sh
cargo test --all-features --test wss_smoke -- --ignored --nocapture
```

dials a public `wss://` echo server (override with `WSMIXER_SMOKE_URL`) with each compiled TLS
option and checks that TLS and the HTTP upgrade complete.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

# ws-mixer-rust

Rust client SDK for the [ws-mixer.v1](https://github.com/mcpwarp/ws-mixer-spec)
protocol: many flow-controlled byte streams multiplexed over one WebSocket.
The server opens streams; the client accepts them as `AsyncRead + AsyncWrite`
values, exchanges opaque JSON `app` messages on the control channel, and
reconnects on its own following the spec's backoff and drain rules.

Native only (tokio). TLS via rustls with the OS trust store by default.

**Status: pre-alpha.** The protocol and the reconnect loop are implemented,
and the spec runner's client fixtures (24/24) and go->rust pair scenarios
(10/10) pass. A crates.io release is still pending.
Spec version: see `spec.pin`.

## Usage

Not on crates.io yet (publication is pending); depend on a local checkout:

```toml
[dependencies]
ws-mixer = { path = "../ws-mixer-rust" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "io-util"] }
serde_json = "1"
```

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ws_mixer::{Client, Event, Token, TokenError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut events) = Client::builder("wss://edge.example.com/v1")
        .token(Token::provider(|| async {
            // Missing configuration never fixes itself: fatal.
            let url = std::env::var("EDGE_TOKEN_URL").map_err(TokenError::fatal)?;
            // A failed refresh may succeed on the next dial: retry with backoff.
            fetch_token(&url).await.map_err(TokenError::unavailable)
        }))
        .hello_meta(serde_json::json!({ "app": "demo" }))
        .connect()?;

    while let Some(event) = events.recv().await {
        match event {
            Event::Connected(c) => println!("connected, session {}", c.session),
            Event::Stream(mut stream) => {
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    stream.read_to_end(&mut request).await?;
                    stream.write_all(b"response").await?;
                    stream.close_write().await
                });
            }
            Event::App(msg) => println!("app message: {:?}", msg.body),
            Event::Disconnected(reason) if reason.fatal => {
                eprintln!("giving up: {reason}");
                break;
            }
            _ => {}
        }
    }
    client.close().await;
    Ok(())
}

// Your token refresh call, e.g. an HTTP request to your auth service.
async fn fetch_token(url: &str) -> std::io::Result<String> {
    todo!()
}
```

## Development

```sh
make test          # unit tests
make clippy        # clippy, all targets and features, warnings denied
make smoke         # build the conformance adapter and check it starts
make conformance-fixture   # spec runner, fixture mode (needs Go)
make conformance-pair      # spec runner, ws-mixer-go server x this client
```

The spec and the Go reference server are fetched at the tags in `spec.pin` and
`goserver.pin` into `.spec/` and `.goserver/`.

The conformance adapter (`conformance/adapter`, client role only) passes all
24 client fixtures and all 10 go->rust pair scenarios.
`conformance/COUNTS.json` holds the floors.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

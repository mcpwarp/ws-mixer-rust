//! A minimal consumer: connect, echo every stream the server opens back to
//! it, and log application messages and connection changes.
//!
//! ```sh
//! WSMIXER_URL=wss://edge.example.com/v1/tunnel WSMIXER_TOKEN=secret \
//!     cargo run --example echo_server_client
//! ```
//!
//! Ctrl-C closes the client gracefully; a second Ctrl-C exits at once. Against ws-mixer-go's
//! `cmd/testserver`, use its `ws://` URL and expected token.

use std::error::Error;

use tokio::io::AsyncWriteExt;
use ws_mixer::{Client, Event, Stream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("WSMIXER_URL").map_err(|_| "set WSMIXER_URL")?;
    let token = std::env::var("WSMIXER_TOKEN").map_err(|_| "set WSMIXER_TOKEN")?;

    let (client, mut events) = Client::builder(&url).token(token).connect()?;

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    let mut closing = false;
    loop {
        let event = tokio::select! {
            event = events.recv() => event,
            _ = &mut ctrl_c, if !closing => {
                closing = true;
                // Keep receiving while the close runs: it reports one
                // Disconnected per live connection, then the channel ends.
                let client = client.clone();
                tokio::spawn(async move { client.close().await });
                // A second Ctrl-C skips the graceful close.
                tokio::spawn(async {
                    let _ = tokio::signal::ctrl_c().await;
                    std::process::exit(130);
                });
                continue;
            }
        };
        let Some(event) = event else { break };
        match event {
            Event::Connected(c) => println!(
                "connected: conn {} session {} (reconnect: {})",
                c.conn_id, c.session, c.reconnect
            ),
            Event::Stream(stream) => {
                tokio::spawn(echo(stream));
            }
            Event::App(msg) => println!("app on conn {}: {:?}", msg.conn_id, msg.body),
            Event::Drain(d) => println!(
                "conn {} draining ({}); a replacement is being dialed",
                d.conn_id, d.reason
            ),
            Event::Disconnected(r) => {
                println!(
                    "disconnected: conn {:?} in {} phase: {r}",
                    r.conn_id, r.phase
                );
                if let Some(delay) = r.retry_in {
                    println!("  retrying in {delay:?}");
                }
                if r.fatal {
                    println!("  fatal, giving up");
                }
            }
            _ => {}
        }
    }
    println!("client finished: {:?}", client.state());
    Ok(())
}

/// Copies everything the server sends back to it, then half-closes. Errors
/// (a reset, or the connection dying) just end the task; dropping the halves
/// resets the stream if it is not fully closed.
async fn echo(stream: Stream) {
    let id = stream.id();
    let (mut rx, mut tx) = stream.into_split();
    let result = async {
        let n = tokio::io::copy(&mut rx, &mut tx).await?;
        tx.shutdown().await?;
        Ok::<_, std::io::Error>(n)
    }
    .await;
    match result {
        Ok(n) => println!("stream {id}: echoed {n} bytes"),
        Err(e) => println!("stream {id}: {e}"),
    }
}

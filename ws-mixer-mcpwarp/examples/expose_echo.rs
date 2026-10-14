//! Exposes an echo handler at a public mcpwarp URL: every `POST /mcp` body
//! is sent back as is. Press Enter to unregister and close.
//!
//! ```sh
//! MCPWARP_TOKEN=mcpwarp_pat_... \
//! MCPWARP_INTEGRATION=6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b \
//! cargo run -p ws-mixer-mcpwarp --example expose_echo
//! ```
//!
//! `MCPWARP_NAME` (default `echo`) and `MCPWARP_URL` (default
//! `wss://connect.mcpwarp.io`) are optional.

use bytes::Bytes;
use http::Response;
use http_body_util::{BodyExt, Full};
use tokio::sync::broadcast::error::RecvError;
use ws_mixer_mcpwarp::bridge::{RequestBody, handler_fn};
use ws_mixer_mcpwarp::{DEFAULT_URL, ExposeConfig, ExposeEvent, expose};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = |key: &str| std::env::var(key).map_err(|_| format!("set {key}"));
    let echo = handler_fn(|req: http::Request<RequestBody>| async move {
        let body: Bytes = req.into_body().collect().await?.to_bytes();
        let res = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::CONTENT_LENGTH, body.len())
            .body(Full::new(body))?;
        Ok(res)
    });
    let config = ExposeConfig::builder()
        .url(std::env::var("MCPWARP_URL").unwrap_or_else(|_| DEFAULT_URL.into()))
        .token(env("MCPWARP_TOKEN")?)
        .name(std::env::var("MCPWARP_NAME").unwrap_or_else(|_| "echo".into()))
        .integration(env("MCPWARP_INTEGRATION")?)
        .on_retry(|info| {
            eprintln!(
                "retry #{} in {:?}: {}",
                info.attempt, info.delay, info.reason
            );
        })
        .handler(echo)
        .build()?;

    let handle = expose(config).await?;
    println!(
        "live at {}{} (press Enter to stop)",
        handle.url().unwrap_or_default(),
        handle.path()
    );

    let mut events = handle.events();
    tokio::spawn(async move {
        loop {
            let event = match events.recv().await {
                Ok(event) => event,
                Err(RecvError::Lagged(skipped)) => {
                    eprintln!("({skipped} events skipped)");
                    continue;
                }
                // After `Close`.
                Err(RecvError::Closed) => break,
            };
            match event {
                ExposeEvent::Registered(r) => println!("registered: {}", r.url),
                ExposeEvent::Disabled { reason, .. } => println!("disabled: {reason}"),
                ExposeEvent::Conflict(c) => println!("taken over ({})", c.reason),
                ExposeEvent::Error(e) => eprintln!("error: {e}"),
                ExposeEvent::HandlerError(e) => eprintln!("handler error: {e}"),
                ExposeEvent::Disconnect(r) => eprintln!("disconnected: {r}"),
                ExposeEvent::Close { error } => {
                    println!(
                        "closed{}",
                        error.map(|e| format!(": {e}")).unwrap_or_default()
                    );
                }
                _ => {}
            }
        }
    });

    tokio::task::spawn_blocking(|| std::io::stdin().read_line(&mut String::new())).await??;
    handle.close().await;
    Ok(())
}

//! A [`TokenProvider`] that distinguishes temporary from permanent failures,
//! plus a reconnect-aware status printer driven by [`Client::watch_state`].
//!
//! The structure is what a desktop bridge (e.g. a Tauri app) does: the SDK
//! events are mapped to plain, serializable UI messages on one task and the
//! client state is mirrored on another. In a Tauri app, `ui_sink` would call
//! `app_handle.emit("tunnel", msg)` instead of printing, and the token would
//! come from the app's session store instead of a file. This example has no
//! Tauri dependency.
//!
//! ```sh
//! echo secret > /tmp/wsmixer-token
//! WSMIXER_URL=wss://edge.example.com/v1/tunnel WSMIXER_TOKEN_FILE=/tmp/wsmixer-token \
//!     cargo run --example token_provider
//! ```
//!
//! The token file is read on every dial, so rotating it takes effect on the
//! next reconnect. Deleting it makes dials fail temporarily (the client
//! keeps backing off); emptying it means "signed out" and stops the client.

use std::error::Error;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;

use tokio::sync::mpsc;
use ws_mixer::{Client, ClientState, Event, Token, TokenError, TokenProvider};

/// Reads the bearer token from a file on every dial.
struct FileToken {
    path: PathBuf,
}

impl TokenProvider for FileToken {
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send + '_>> {
        Box::pin(async move {
            match tokio::fs::read_to_string(&self.path).await {
                Ok(s) if s.trim().is_empty() => {
                    Err(TokenError::fatal("signed out: token file is empty"))
                }
                Ok(s) => Ok(s.trim().to_owned()),
                // May appear later (a login in progress): back off and retry.
                Err(e) if e.kind() == io::ErrorKind::NotFound => Err(TokenError::unavailable(e)),
                // Anything else will not fix itself.
                Err(e) => Err(TokenError::fatal(e)),
            }
        })
    }
}

/// What the UI is told: plain serializable data, no `Stream`s or SDK handles.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UiMessage {
    Online {
        conn: u64,
        session: String,
        reconnect: bool,
    },
    Offline {
        conn: Option<u64>,
        reason: String,
        retry_in_ms: Option<u64>,
        fatal: bool,
    },
    Draining {
        conn: u64,
        reason: String,
    },
    App {
        body: serde_json::Map<String, serde_json::Value>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("WSMIXER_URL").map_err(|_| "set WSMIXER_URL")?;
    let path = std::env::var_os("WSMIXER_TOKEN_FILE")
        .map(PathBuf::from)
        .ok_or("set WSMIXER_TOKEN_FILE")?;

    let (client, mut events) = Client::builder(&url)
        .token(Token::Provider(std::sync::Arc::new(FileToken { path })))
        .hello_meta(serde_json::json!({ "app": "token-provider-example" }))
        .connect()?;

    // State mirror: the latest state, including the backoff between dials
    // (a watch channel coalesces transitions the task has not seen yet).
    let mut state = client.watch_state();
    let state_task = tokio::spawn(async move {
        loop {
            let s = *state.borrow_and_update();
            let label = match s {
                ClientState::Idle => "idle",
                ClientState::Dialing => "connecting...",
                ClientState::Connected => "online",
                ClientState::Backoff => "offline, waiting to retry",
                ClientState::Closed => "closed",
                _ => "unknown",
            };
            println!("[state] {label}");
            if s == ClientState::Closed || state.changed().await.is_err() {
                break;
            }
        }
    });

    // UI sink: in a Tauri app this is `app_handle.emit(...)`.
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiMessage>();
    let ui_sink = tokio::spawn(async move {
        while let Some(msg) = ui_rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(json) => println!("[ui] {json}"),
                Err(e) => eprintln!("[ui] unserializable message: {e}"),
            }
        }
    });

    // Event pump: the only place that receives from `events`. It must keep
    // running for the client to stay alive.
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    let mut closing = false;
    loop {
        let event = tokio::select! {
            event = events.recv() => event,
            _ = &mut ctrl_c, if !closing => {
                closing = true;
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
        let msg = match event {
            Event::Connected(c) => UiMessage::Online {
                conn: c.conn_id.get(),
                session: c.session,
                reconnect: c.reconnect,
            },
            Event::Disconnected(r) => UiMessage::Offline {
                conn: r.conn_id.map(|c| c.get()),
                reason: r.to_string(),
                retry_in_ms: r
                    .retry_in
                    .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                fatal: r.fatal,
            },
            // During a drain the replacement's Online may arrive before this
            // connection's Offline: the UI keys on `conn`, not on order.
            Event::Drain(d) => UiMessage::Draining {
                conn: d.conn_id.get(),
                reason: d.reason.to_string(),
            },
            Event::App(m) => UiMessage::App { body: m.body },
            Event::Stream(stream) => {
                // This example serves no streams.
                stream.refuse();
                continue;
            }
            _ => continue,
        };
        let _ = ui_tx.send(msg);
    }

    drop(ui_tx);
    let _ = tokio::join!(state_task, ui_sink);
    Ok(())
}

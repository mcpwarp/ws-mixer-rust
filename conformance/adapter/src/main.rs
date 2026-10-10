//! Conformance adapter for ws-mixer-rust (client role).
//!
//! Speaks the JSON Lines protocol of ws-mixer-spec's docs/CONFORMANCE.md §1:
//! commands on stdin, events on stdout, one object per line, logs on stderr.
//! The stdin loop never blocks: anything that waits runs on its own task,
//! and `write`/`close_write` go through a per-stream FIFO (`streams`).

mod client;
mod emit;
mod streams;

use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, watch};
use ws_mixer::Client;

use client::{Options, as_u32, await_client};
use emit::{ack, cmd_error, emit, unsupported};
use streams::{Op, Streams};

pub struct Adapter {
    opts: Mutex<Options>,
    /// Set once by `connect`; a watch so `await_client` can wait for it.
    client: watch::Sender<Option<Client>>,
    streams: Arc<Streams>,
}

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn main() {
    emit::init_clock();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("adapter: cannot start the tokio runtime: {e}");
            std::process::exit(1);
        }
    };
    emit(json!({
        "event": "ready",
        "sdk": "ws-mixer-rust",
        "sdk_version": ws_mixer::SDK_VERSION,
        "roles": ["client"],
    }));

    // stdin is read on a plain thread so the async side never blocks on it.
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lines() {
            let line = match line {
                Ok(l) => Ok(l),
                // The bad line is consumed, so the next read starts clean.
                Err(e) if e.kind() == io::ErrorKind::InvalidData => Err(e.to_string()),
                Err(_) => break,
            };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rt.block_on(run(rx));
    // stdin EOF. Exit without waiting for the client or stray tasks.
    std::process::exit(0);
}

async fn run(mut lines: mpsc::UnboundedReceiver<Result<String, String>>) {
    let ad = Arc::new(Adapter {
        opts: Mutex::new(Options::default()),
        client: watch::Sender::new(None),
        streams: Streams::new(),
    });
    while let Some(line) = lines.recv().await {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                emit(json!({"event": "error", "message": format!("bad command line: {e}")}));
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let cmd = match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(m)) => m,
            Ok(_) => {
                emit(json!({"event": "error", "message": "command is not a JSON object"}));
                continue;
            }
            Err(e) => {
                emit(json!({"event": "error", "message": format!("bad command json: {e}")}));
                continue;
            }
        };
        dispatch(&ad, cmd);
    }
}

/// Handles one command. Runs on the stdin loop, so it must not wait:
/// anything that can is spawned. `write`/`close_write` are enqueued here,
/// synchronously, to keep their order per stream.
fn dispatch(ad: &Arc<Adapter>, cmd: Map<String, Value>) {
    let seq = cmd.get("seq").cloned().unwrap_or(Value::Null);
    let name = cmd.get("cmd").and_then(Value::as_str).unwrap_or_default();
    match name {
        "set_options" => {
            // The client already took its options; changing them now would
            // only half-apply (time_scale/floor_ms but not window etc.).
            if ad.client.borrow().is_some() {
                return cmd_error(&seq, "set_options: not allowed after connect");
            }
            match lock(&ad.opts).apply(&cmd) {
                Ok(()) => ack(&seq),
                Err(e) => cmd_error(&seq, format!("set_options: {e}")),
            }
        }
        "listen" | "open_stream" => unsupported(
            &seq,
            format!("unsupported command {name}: ws-mixer-rust is client-only"),
        ),
        "connect" => client::connect(ad, seq, &cmd),
        "write" => {
            let id = match stream_id(&cmd) {
                Ok(id) => id,
                Err(e) => return cmd_error(&seq, format!("write: {e}")),
            };
            let b64 = cmd.get("data_b64").and_then(Value::as_str).unwrap_or("");
            match data_encoding::BASE64.decode(b64.as_bytes()) {
                Ok(data) => ad.streams.enqueue(id, seq, Op::Write(data)),
                Err(e) => cmd_error(&seq, format!("write: bad data_b64: {e}")),
            }
        }
        "close_write" => match stream_id(&cmd) {
            Ok(id) => ad.streams.enqueue(id, seq, Op::CloseWrite),
            Err(e) => cmd_error(&seq, format!("close_write: {e}")),
        },
        "reset" => {
            let id = match stream_id(&cmd) {
                Ok(id) => id,
                Err(e) => return cmd_error(&seq, format!("reset: {e}")),
            };
            let code = match cmd.get("code").map(|v| as_u32(v, "code")).transpose() {
                Ok(c) => c.unwrap_or(0),
                Err(e) => return cmd_error(&seq, format!("reset: {e}")),
            };
            let message = cmd
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            // Abort the queue here, before any later write/close_write for
            // the id can be enqueued behind it.
            let worker = ad.streams.abort_queue(id);
            let streams = ad.streams.clone();
            tokio::spawn(async move { streams.reset(seq, id, code, message, worker).await });
        }
        "send_app" => {
            let ad = ad.clone();
            let body = cmd.get("body").cloned().unwrap_or_else(|| json!({}));
            tokio::spawn(async move {
                let Some(client) = await_client(&ad).await else {
                    return cmd_error(&seq, "send_app before connection established");
                };
                match client.send_app(body).await {
                    Ok(()) => ack(&seq),
                    Err(e) => cmd_error(&seq, format!("send_app: {e}")),
                }
            });
        }
        "drain" => {
            let reason = cmd.get("reason").and_then(Value::as_str);
            if reason.is_some_and(|r| r != "client_requested") {
                return cmd_error(&seq, "drain: a client may only drain with client_requested");
            }
            let deadline = match cmd.get("deadline_ms").and_then(Value::as_u64) {
                Some(ms) if ms > 0 => lock(&ad.opts).scale(ms),
                _ => Duration::ZERO,
            };
            let ad = ad.clone();
            tokio::spawn(async move {
                let Some(client) = await_client(&ad).await else {
                    return cmd_error(&seq, "drain before connection established");
                };
                match client.drain(deadline).await {
                    Ok(()) => ack(&seq),
                    Err(e) => cmd_error(&seq, format!("drain: {e}")),
                }
            });
        }
        "close" => {
            // 0/absent: graceful close. 14: application close, which also
            // ends the reconnect loop for good. Nothing else is legal.
            let code = match cmd.get("code") {
                None | Some(Value::Null) => 0,
                Some(v) => match v.as_u64() {
                    Some(c @ (0 | 14)) => c,
                    _ => {
                        return cmd_error(
                            &seq,
                            format!("close: code {v} not allowed (only 0 or 14)"),
                        );
                    }
                },
            };
            let message = cmd
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let ad = ad.clone();
            tokio::spawn(async move {
                let existing = ad.client.borrow().clone();
                let client = match existing {
                    Some(c) => c,
                    None => match await_client(&ad).await {
                        Some(c) => c,
                        None => return cmd_error(&seq, "close before connection established"),
                    },
                };
                ack(&seq);
                if code == 14 {
                    client.close_with(message).await;
                } else {
                    client.close().await;
                }
            });
        }
        "shutdown" => {
            ack(&seq);
            std::process::exit(0);
        }
        _ => cmd_error(&seq, format!("unknown command {name:?}")),
    }
}

fn stream_id(cmd: &Map<String, Value>) -> Result<u32, String> {
    as_u32(cmd.get("id").ok_or("id is required")?, "id")
}

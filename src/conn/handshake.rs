//! `hello` / `welcome` exchange and welcome validation (WIRE.md §2.7, §2.9).
//!
//! Runs on the unsplit socket between the 101 and `welcome`; every failure
//! here is reported with phase `Handshake`.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::{
    CLOSE_ECHO_WAIT, ConnConfig, ConnFailure, WRITE_TIMEOUT, abnormal_reason, local_error_reason,
    peer_close_reason, peer_error_reason,
};
use crate::control::{ControlMsg, Hello, Welcome, parse_control};
use crate::error::{ErrorCode, close_reason};
use crate::event::{ConnId, DisconnectReason, Phase};
use crate::frame::{self, Decoded, Frame, FrameError};
use crate::stats::Counters;

const PHASE: Phase = Phase::Handshake;

pub(crate) async fn run<S>(
    ws: &mut WebSocketStream<S>,
    cfg: &ConnConfig,
    token: String,
    conn_id: ConnId,
) -> Result<Welcome, ConnFailure>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let id = Some(conn_id);
    let hello = Hello::new(
        token,
        cfg.agent.clone(),
        cfg.window,
        cfg.max_streams,
        cfg.capabilities.clone(),
        cfg.meta.clone(),
    );
    let hello = match frame::encode_control(&hello) {
        Ok(f) => f,
        Err(e) => {
            let msg = format!(
                "hello of {} bytes exceeds the {}-byte control message limit",
                e.bytes,
                frame::MAX_CONTROL_PAYLOAD
            );
            // Redialing sends the same oversized hello again.
            let mut reason = fail(ws, conn_id, ErrorCode::INTERNAL_ERROR, msg).await;
            reason.fatal = true;
            return Err(reason.into());
        }
    };
    let hello_len = hello.len();
    match tokio::time::timeout(WRITE_TIMEOUT, ws.send(Message::Binary(hello))).await {
        Ok(Ok(())) => Counters::add(&cfg.stats.bytes_out, hello_len),
        Ok(Err(e)) => {
            let message = format!("sending hello failed: {e}");
            return Err(abnormal_reason(id, PHASE, message, Some(Arc::new(e))).into());
        }
        Err(_) => {
            let message = format!(
                "sending hello made no progress for {}s",
                WRITE_TIMEOUT.as_secs()
            );
            return Err(abnormal_reason(id, PHASE, message, None).into());
        }
    }

    let start = Instant::now();
    let deadline = start
        .checked_add(cfg.hello_timeout)
        .unwrap_or(start + Duration::from_secs(365 * 24 * 3600));
    let violation = |code: ErrorCode, message: String| {
        Counters::inc(&cfg.stats.protocol_violations);
        (code, message)
    };
    let mut welcome_timeout = false;
    let (code, message) = loop {
        let msg = match tokio::time::timeout_at(deadline, ws.next()).await {
            Err(_) => {
                welcome_timeout = true;
                break (
                    ErrorCode::PROTOCOL_ERROR,
                    format!(
                        "no welcome within {}ms of hello",
                        cfg.hello_timeout.as_millis()
                    ),
                );
            }
            Ok(None) => {
                let message = "connection closed before welcome".to_owned();
                return Err(abnormal_reason(id, PHASE, message, None).into());
            }
            Ok(Some(Err(WsError::Capacity(e)))) => {
                break violation(
                    ErrorCode::FRAME_SIZE_ERROR,
                    format!("{e}: the limit is {} bytes", frame::MAX_MESSAGE),
                );
            }
            Ok(Some(Err(e))) => {
                let message = format!("connection lost before welcome: {e}");
                return Err(abnormal_reason(id, PHASE, message, Some(Arc::new(e))).into());
            }
            Ok(Some(Ok(m))) => m,
        };
        let payload = match msg {
            Message::Binary(b) => {
                Counters::add(&cfg.stats.bytes_in, b.len());
                b
            }
            Message::Close(f) => {
                let reason = peer_close_reason(id, PHASE, f.as_ref());
                drain_echo(ws).await;
                return Err(reason.into());
            }
            Message::Text(t) => {
                break violation(
                    ErrorCode::PROTOCOL_ERROR,
                    format!(
                        "text message of {} bytes: ws-mixer.v1 uses binary messages only",
                        t.len()
                    ),
                );
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
        };
        let len = payload.len();
        match frame::decode(payload) {
            Err(FrameError::Connection { code, message }) => break violation(code, message),
            Ok(Decoded::Frame(Frame::Data {
                stream_id: 0,
                payload,
            })) => match parse_control(&payload, cfg.allow_subfloor_timing) {
                Err(e) => break violation(e.code, e.message),
                Ok(ControlMsg::Welcome(w)) => return Ok(w),
                Ok(ControlMsg::Error(e)) => {
                    let code = ErrorCode(e.code);
                    match code {
                        ErrorCode::NO_ERROR | ErrorCode::APPLICATION_CLOSE => {
                            tracing::info!(%conn_id, code = e.code, "peer closed the connection before welcome: {}", e.message);
                        }
                        ErrorCode::PROTOCOL_ERROR
                        | ErrorCode::FLOW_CONTROL_ERROR
                        | ErrorCode::FRAME_SIZE_ERROR => {
                            tracing::error!(%conn_id, code = e.code, "peer reported a connection error before welcome: {}", e.message);
                        }
                        _ => {
                            tracing::warn!(%conn_id, code = e.code, "peer reported a connection error before welcome: {}", e.message);
                        }
                    }
                    let reason = peer_error_reason(id, PHASE, code, &e.message);
                    close(ws, None, code.ws_close_wire(), close_reason(&e.message)).await;
                    return Err(reason.into());
                }
                Ok(other) => {
                    break violation(
                        ErrorCode::PROTOCOL_ERROR,
                        format!(
                            "{} received before welcome ({}-byte control message)",
                            control_name(&other),
                            payload.len()
                        ),
                    );
                }
            },
            Ok(Decoded::Frame(f)) => {
                break violation(
                    ErrorCode::PROTOCOL_ERROR,
                    format!(
                        "{} frame for stream {} received before welcome",
                        frame_name(&f),
                        f.stream_id()
                    ),
                );
            }
            Err(FrameError::Stream { stream_id, .. }) => {
                break violation(
                    ErrorCode::PROTOCOL_ERROR,
                    format!("WINDOW frame for stream {stream_id} received before welcome"),
                );
            }
            Ok(Decoded::Unknown {
                frame_type,
                stream_id,
                ..
            }) => {
                break violation(
                    ErrorCode::PROTOCOL_ERROR,
                    format!(
                        "frame of type {frame_type:#04x} for stream {stream_id} ({len} bytes) received before welcome"
                    ),
                );
            }
        }
    };
    Err(ConnFailure {
        reason: Box::new(fail(ws, conn_id, code, message).await),
        retry_after: None,
        welcome_timeout,
    })
}

/// Raises a ws-mixer error before `welcome`: `error{}` + close, then the
/// reason (phase `Handshake`, semantic `ws_code`, no `close_reason`).
async fn fail<S>(
    ws: &mut WebSocketStream<S>,
    conn_id: ConnId,
    code: ErrorCode,
    message: String,
) -> DisconnectReason
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tracing::warn!(%conn_id, code = %code, "handshake failed: {message}");
    close_with_error(ws, code, &message).await;
    local_error_reason(Some(conn_id), PHASE, code, message)
}

/// Sends `error{code, message}` and the matching close, then waits briefly
/// for the peer's echo. Best effort.
pub(crate) async fn close_with_error<S>(ws: &mut WebSocketStream<S>, code: ErrorCode, message: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let error = frame::encode_control(&crate::control::ErrorMsg::new(code, message))
        .expect("error messages are capped at 1024 bytes");
    close(ws, Some(error), code.ws_close_wire(), close_reason(message)).await;
}

/// Sends the optional `error` frame and the Close, then waits briefly for the
/// peer's echo. Best effort.
async fn close<S>(ws: &mut WebSocketStream<S>, error: Option<bytes::Bytes>, code: u16, reason: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let send = async {
        if let Some(error) = error {
            ws.send(Message::Binary(error)).await?;
        }
        ws.send(Message::Close(Some(CloseFrame {
            code: CloseCode::from(code),
            reason: reason.into(),
        })))
        .await
    };
    if let Ok(Ok(())) = tokio::time::timeout(CLOSE_ECHO_WAIT, send).await {
        drain_echo(ws).await;
    }
}

/// Reads until the stream ends (flushing tungstenite's automatic close
/// reply) or [`CLOSE_ECHO_WAIT`] passes.
async fn drain_echo<S>(ws: &mut WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _ = tokio::time::timeout(CLOSE_ECHO_WAIT, async {
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
}

fn frame_name(f: &Frame) -> &'static str {
    match f {
        Frame::Open { .. } => "OPEN",
        Frame::Data { .. } => "DATA",
        Frame::Window { .. } => "WINDOW",
        Frame::Close { .. } => "CLOSE",
        Frame::Reset { .. } => "RESET",
    }
}

fn control_name(m: &ControlMsg) -> &'static str {
    match m {
        ControlMsg::Hello(_) => "hello",
        ControlMsg::Welcome(_) => "welcome",
        ControlMsg::Ping(_) => "ping",
        ControlMsg::Pong(_) => "pong",
        ControlMsg::Drain(_) => "drain",
        ControlMsg::Error(_) => "error",
        ControlMsg::App(_) => "app",
    }
}

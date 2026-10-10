//! TCP connect, TLS and the WebSocket upgrade with the `ws-mixer.v1`
//! subprotocol and `Authorization: Bearer` header (WIRE.md §2.1).
//!
//! Every failure here is phase `Dial`: CLIENT-SDK.md puts the HTTP upgrade
//! response, including a 101 without the subprotocol echo, in that phase.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use http::header::{AUTHORIZATION, RETRY_AFTER, SEC_WEBSOCKET_PROTOCOL, USER_AGENT};
use http::{HeaderValue, StatusCode};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use super::{ConnConfig, ConnFailure, base_reason};
use crate::error::ErrorCode;
use crate::event::{ConnId, DisconnectReason, Phase};
use crate::tls::TlsConfig;

/// The transport under the WebSocket: plain TCP or a TLS stream over it.
pub(crate) trait Io: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {}
pub(crate) type BoxIo = Box<dyn Io>;

type Cause = Arc<dyn std::error::Error + Send + Sync>;

fn failure(conn_id: ConnId, message: String, cause: Option<Cause>) -> ConnFailure {
    let mut r = base_reason(Some(conn_id), Phase::Dial, message);
    r.cause = cause;
    r.into()
}

/// Connects and upgrades, bounded as a whole by `cfg.connect_timeout`.
pub(crate) async fn connect(
    cfg: &ConnConfig,
    token: &str,
    conn_id: ConnId,
) -> Result<WebSocketStream<BoxIo>, ConnFailure> {
    let request = build_request(cfg, token).map_err(|(m, c)| failure(conn_id, m, c))?;
    match tokio::time::timeout(cfg.connect_timeout, upgrade(cfg, request, conn_id)).await {
        Ok(r) => r,
        Err(_) => Err(failure(
            conn_id,
            format!(
                "connect to {} timed out after {}ms",
                cfg.url,
                cfg.connect_timeout.as_millis()
            ),
            None,
        )),
    }
}

fn build_request(cfg: &ConnConfig, token: &str) -> Result<Request, (String, Option<Cause>)> {
    let mut req = cfg.url.as_str().into_client_request().map_err(|e| {
        (
            format!("invalid url {:?}: {e}", cfg.url),
            Some(Arc::new(e) as Cause),
        )
    })?;
    let headers = req.headers_mut();
    for (name, value) in &cfg.headers {
        headers.append(name.clone(), value.clone());
    }
    let bearer = HeaderValue::try_from(format!("Bearer {token}"))
        .map_err(|_| ("token is not a valid HTTP header value".to_owned(), None))?;
    headers.insert(AUTHORIZATION, bearer);
    headers.insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(crate::SUBPROTOCOL),
    );
    if !headers.contains_key(USER_AGENT) {
        let a = &cfg.agent;
        let ua = format!(
            "{}/{} ({}; {})",
            a.sdk,
            a.sdk_version,
            a.runtime.as_deref().unwrap_or("rust"),
            a.os.as_deref().unwrap_or(std::env::consts::OS)
        );
        if let Ok(v) = HeaderValue::try_from(ua) {
            headers.insert(USER_AGENT, v);
        }
    }
    Ok(req)
}

async fn upgrade(
    cfg: &ConnConfig,
    request: Request,
    conn_id: ConnId,
) -> Result<WebSocketStream<BoxIo>, ConnFailure> {
    let uri = request.uri();
    let tls = match uri.scheme_str() {
        Some("wss") => true,
        Some("ws") => false,
        other => {
            return Err(failure(
                conn_id,
                format!("unsupported url scheme {other:?}: use ws:// or wss://"),
                None,
            ));
        }
    };
    let Some(host) = uri.host() else {
        return Err(failure(
            conn_id,
            format!("url {} has no host", cfg.url),
            None,
        ));
    };
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });

    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| {
            failure(
                conn_id,
                format!("tcp connect to {host}:{port} failed: {e}"),
                Some(Arc::new(e)),
            )
        })?;
    // Small control frames must not wait for Nagle.
    let _ = tcp.set_nodelay(true);

    let io: BoxIo = if tls {
        let config = cfg.tls.clone().ok_or_else(|| {
            failure(
                conn_id,
                "wss:// needs a TLS configuration, and none is compiled in: set ClientBuilder::tls"
                    .into(),
                None,
            )
        })?;
        tls_connect(config, &host, tcp)
            .await
            .map_err(|(m, c)| failure(conn_id, m, c))?
    } else {
        Box::new(tcp)
    };

    let (ws, response) =
        tokio_tungstenite::client_async_with_config(request, io, Some(ConnConfig::ws_config()))
            .await
            .map_err(|e| upgrade_failure(conn_id, e))?;

    // tungstenite already rejects a 101 without an offered protocol; this
    // also pins the exact value.
    let echoed = response.headers().get(SEC_WEBSOCKET_PROTOCOL);
    if echoed.map(HeaderValue::as_bytes) != Some(crate::SUBPROTOCOL.as_bytes()) {
        return Err(subprotocol_failure(
            conn_id,
            format!("server echoed subprotocol {echoed:?}"),
        ));
    }
    Ok(ws)
}

/// WIRE.md §2.1: a 101 without the subprotocol echo means a proxy or the
/// wrong endpoint answered. Fatal, reported as `UNSUPPORTED` with neither a
/// close code nor an HTTP status (CLIENT-SDK.md source 3).
fn subprotocol_failure(conn_id: ConnId, detail: String) -> ConnFailure {
    let mut r: DisconnectReason = base_reason(
        Some(conn_id),
        Phase::Dial,
        format!(
            "the server did not accept the {} subprotocol ({detail}); not retrying",
            crate::SUBPROTOCOL
        ),
    );
    r.error_code = Some(ErrorCode::UNSUPPORTED);
    r.error_name = Some(ErrorCode::UNSUPPORTED.name());
    r.fatal = true;
    tracing::error!(%conn_id, "{}", r.message);
    r.into()
}

fn upgrade_failure(conn_id: ConnId, e: WsError) -> ConnFailure {
    match e {
        WsError::Http(response) => {
            let status = response.status();
            let body = response
                .body()
                .as_deref()
                .map(|b| String::from_utf8_lossy(&b[..b.len().min(256)]).into_owned())
                .unwrap_or_default();
            let mut r = base_reason(
                Some(conn_id),
                Phase::Dial,
                format!("upgrade rejected with HTTP {}: {body}", status.as_u16()),
            );
            r.http_status = Some(status.as_u16());
            if status == StatusCode::BAD_REQUEST {
                // WIRE.md §2.1: no subprotocol overlap. Retried with backoff,
                // but almost certainly a version mismatch worth shouting about.
                tracing::error!(%conn_id, "{}", r.message);
            } else {
                tracing::warn!(%conn_id, "{}", r.message);
            }
            let retry_after = matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            )
            .then(|| response.headers().get(RETRY_AFTER))
            .flatten()
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_retry_after(v, SystemTime::now()));
            ConnFailure {
                reason: Box::new(r),
                retry_after,
                welcome_timeout: false,
            }
        }
        WsError::Protocol(ProtocolError::SecWebSocketSubProtocolError(sub)) => {
            subprotocol_failure(conn_id, sub.to_string())
        }
        e => {
            let message = format!("websocket upgrade failed: {e}");
            failure(conn_id, message, Some(Arc::new(e)))
        }
    }
}

/// `Retry-After` as delay-seconds or an HTTP-date (RFC 9110 §10.2.3). A
/// date in the past means retry now.
pub(crate) fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

async fn tls_connect(
    config: TlsConfig,
    host: &str,
    tcp: TcpStream,
) -> Result<BoxIo, (String, Option<Cause>)> {
    // Only the compile_error! in tls.rs should speak for a build with no
    // TLS stack.
    #[cfg(not(any(feature = "__rustls", feature = "native-tls")))]
    let _ = (host, tcp);
    match config {
        #[cfg(feature = "native-tls")]
        TlsConfig::NativeTls => native_tls_connect(host, tcp).await,
        #[cfg(feature = "__rustls")]
        other => rustls_connect(other, host, tcp).await,
    }
}

#[cfg(feature = "native-tls")]
async fn native_tls_connect(host: &str, tcp: TcpStream) -> Result<BoxIo, (String, Option<Cause>)> {
    use tokio_native_tls::native_tls;
    let connector = native_tls::TlsConnector::new().map_err(|e| {
        (
            format!("native-tls setup failed: {e}"),
            Some(Arc::new(e) as Cause),
        )
    })?;
    let stream = tokio_native_tls::TlsConnector::from(connector)
        .connect(host, tcp)
        .await
        .map_err(|e| {
            (
                format!("tls handshake with {host} failed: {e}"),
                Some(Arc::new(e) as Cause),
            )
        })?;
    Ok(Box::new(stream))
}

#[cfg(feature = "__rustls")]
async fn rustls_connect(
    config: TlsConfig,
    host: &str,
    tcp: TcpStream,
) -> Result<BoxIo, (String, Option<Cause>)> {
    let rustls_config = crate::tls::rustls_config(config).map_err(|e| {
        (
            format!("tls configuration failed: {e}"),
            Some(Arc::new(e) as Cause),
        )
    })?;
    let name = rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| {
        (
            format!("invalid tls server name {host:?}: {e}"),
            Some(Arc::new(e) as Cause),
        )
    })?;
    let stream = tokio_rustls::TlsConnector::from(rustls_config)
        .connect(name, tcp)
        .await
        .map_err(|e| {
            (
                format!("tls handshake with {host} failed: {e}"),
                Some(Arc::new(e) as Cause),
            )
        })?;
    Ok(Box::new(stream))
}

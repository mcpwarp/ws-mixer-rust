//! `target` mode (MCPWARP.md §4.5, §4.6): each request is forwarded to a
//! local `http://` server with hyper.

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response, Uri};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tower_service::Service;
use ws_mixer::BoxError;

use super::handler::RequestBody;
use super::response::{HOP_BY_HOP, connection_tokens, sdk_reply_response};

/// How long a [`Target`] waits for the local server's response headers by
/// default: 30 s (`targetTimeoutMs`, MCPWARP.md §5.1).
pub const DEFAULT_TARGET_TIMEOUT: Duration = Duration::from_secs(30);

/// The body of a [`Target`]'s responses: the local server's body as it
/// streams in, or the JSON of a `502`/`504` reply.
///
/// [`TargetBody::new`] wraps any other body, so a handler that sometimes
/// answers itself can return the same response type.
pub struct TargetBody(UnsyncBoxBody<Bytes, BoxError>);

impl TargetBody {
    /// Wraps `body`.
    pub fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        Self(body.map_err(Into::into).boxed_unsync())
    }

    /// An empty body.
    pub fn empty() -> Self {
        Self::new(http_body_util::Empty::new())
    }
}

impl Default for TargetBody {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for TargetBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetBody").finish_non_exhaustive()
    }
}

impl http_body::Body for TargetBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        Pin::new(&mut self.0).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

/// A [`Target`] URL that is not an absolute `http://` URL. The message does
/// not echo the URL.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "mcpwarp: a `target` must be an absolute http:// URL without credentials (https is not supported)"
)]
pub struct TargetUrlError(());

/// A local HTTP server requests are forwarded to.
///
/// A `Target` is a [`Handler`](super::Handler) for
/// [`serve_stream`](super::serve_stream) (`target` mode), and a
/// [`tower_service::Service`] in its own right, so a custom handler can
/// forward to one of several targets.
///
/// `/mcp` maps onto the target URL, with the request's query appended to
/// the target's own. `Host` becomes the target's; `x-forwarded-host` (the
/// request's `Host`) and `x-forwarded-proto: https` carry the public ones;
/// `accept-encoding: identity`. Redirects are passed through, not followed.
///
/// The call never fails for a local server that is slow or gone: it
/// resolves to a response. No response headers within the timeout:
/// `504 {"error":"local server timed out"}`; the server cannot be reached:
/// `502 {"error":"local server unreachable (<code>)"}`. Those responses are
/// tagged, and `serve_stream` writes them as the SDK's byte-exact error
/// replies (MCPWARP.md §4.5) whichever handler returns them, as long as
/// they come back unedited. One a handler edited (a header added, the
/// status or body changed, e.g. by a middleware layer) is written as an
/// ordinary app response, edits included. Removing the tag (clearing the
/// response's extensions) is fine too: the response is then an app
/// response as well.
///
/// Cloning is cheap and shares the connection pool.
///
/// Forwarding by public host, from a handler:
///
/// ```no_run
/// use std::collections::HashMap;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use tower_service::Service;
/// use ws_mixer_mcpwarp::bridge::{BoxError, Target, TargetBody, handler_fn};
///
/// # fn main() -> Result<(), BoxError> {
/// let timeout = Duration::from_secs(30);
/// let targets: Arc<HashMap<&str, Target>> = Arc::new(HashMap::from([
///     ("notes.example", Target::try_new("http://127.0.0.1:3001/mcp".parse()?, timeout)?),
///     ("files.example", Target::try_new("http://127.0.0.1:3002/mcp".parse()?, timeout)?),
/// ]));
/// let app = handler_fn(move |req: http::Request<_>| {
///     let targets = targets.clone();
///     async move {
///         let host = req.uri().host().unwrap_or_default();
///         match targets.get(host) {
///             Some(target) => target.clone().call(req).await,
///             None => Ok(http::Response::builder()
///                 .status(404)
///                 .body(TargetBody::empty())?),
///         }
///     }
/// });
/// # let _ = app;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Target {
    inner: Arc<Inner>,
}

struct Inner {
    /// `scheme://authority/path`, without query or fragment.
    base: String,
    query: Option<String>,
    timeout: Duration,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Target")
            .field("base", &self.inner.base)
            .field("timeout", &self.inner.timeout)
            .finish_non_exhaustive()
    }
}

impl Target {
    /// Forwards to `url`, answering `504` when no response headers arrive
    /// within `timeout` ([`DEFAULT_TARGET_TIMEOUT`] is the spec's default).
    /// Fails unless `url` is an absolute `http://` URL without userinfo:
    /// `https://` is not supported, which MCPWARP.md §5.2 row 3 makes a
    /// configuration error, and credentials in the URL are refused (a fetch
    /// `Request` refuses them too), so they never reach logs or `Debug`.
    ///
    /// ```
    /// use std::time::Duration;
    /// use ws_mixer_mcpwarp::bridge::Target;
    ///
    /// let local = Target::try_new("http://127.0.0.1:3000/mcp".parse()?, Duration::from_secs(30));
    /// assert!(local.is_ok());
    /// let tls = Target::try_new("https://127.0.0.1/mcp".parse()?, Duration::from_secs(30));
    /// assert!(tls.is_err());
    /// # Ok::<(), http::uri::InvalidUri>(())
    /// ```
    pub fn try_new(url: Uri, timeout: Duration) -> Result<Self, TargetUrlError> {
        let has_userinfo = url.authority().is_some_and(|a| a.as_str().contains('@'));
        if url.scheme() != Some(&http::uri::Scheme::HTTP)
            || url.authority().is_none()
            || has_userinfo
        {
            return Err(TargetUrlError(()));
        }
        Ok(Self::build(&url, timeout))
    }

    /// [`try_new`](Self::try_new), panicking on a URL it refuses.
    ///
    /// # Panics
    ///
    /// If `url` is not an absolute `http://` URL.
    pub fn new(url: Uri, timeout: Duration) -> Self {
        match Self::try_new(url, timeout) {
            Ok(t) => t,
            Err(e) => panic!("{e}"),
        }
    }

    fn build(url: &Uri, timeout: Duration) -> Self {
        let authority = url.authority().map(|a| a.as_str()).unwrap_or_default();
        let base = format!("http://{authority}{}", url.path());
        let query = url.query().filter(|q| !q.is_empty()).map(str::to_owned);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build_http();
        Self {
            inner: Arc::new(Inner {
                base,
                query,
                timeout,
                client,
            }),
        }
    }

    /// The per-request timeout for response headers.
    pub fn timeout(&self) -> Duration {
        self.inner.timeout
    }
}

impl Service<Request<RequestBody>> for Target {
    type Response = Response<TargetBody>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response<TargetBody>, BoxError>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    /// Forwards `req`, which is shaped as a handler receives it (URI
    /// `https://<Host><target>`, the `host` header the public one).
    fn call(&mut self, req: Request<RequestBody>) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move { forward(&inner, req).await })
    }
}

fn upstream_uri(inner: &Inner, incoming_query: Option<&str>) -> String {
    let query: Vec<&str> = [inner.query.as_deref(), incoming_query]
        .into_iter()
        .flatten()
        .filter(|q| !q.is_empty())
        .collect();
    if query.is_empty() {
        inner.base.clone()
    } else {
        format!("{}?{}", inner.base, query.join("&"))
    }
}

/// The request's headers minus hop-by-hop ones, `Connection`-listed names,
/// `expect`, `content-length` and `host`.
fn upstream_headers(headers: &HeaderMap) -> HeaderMap {
    let listed = connection_tokens(
        headers
            .get_all(http::header::CONNECTION)
            .iter()
            .map(|v| v.as_bytes()),
    );
    let mut out = HeaderMap::with_capacity(headers.len() + 3);
    for (name, value) in headers {
        let n = name.as_str();
        if HOP_BY_HOP.contains(&n)
            || listed.contains(n)
            || matches!(n, "expect" | "content-length" | "host")
        {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn upstream_request(
    inner: &Inner,
    req: Request<RequestBody>,
) -> Result<Request<Full<Bytes>>, BoxError> {
    let (parts, body) = req.into_parts();
    let public_host = match parts.headers.get(http::header::HOST) {
        Some(h) => h.clone(),
        None => HeaderValue::from_str(parts.uri.authority().map_or("", |a| a.as_str()))?,
    };
    let mut headers = upstream_headers(&parts.headers);
    headers.insert(
        http::header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    headers.insert("x-forwarded-host", public_host);
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    let mut upstream = Request::builder()
        .method(parts.method)
        .uri(upstream_uri(inner, parts.uri.query()))
        .body(body)?;
    *upstream.headers_mut() = headers;
    Ok(upstream)
}

async fn forward(
    inner: &Inner,
    req: Request<RequestBody>,
) -> Result<Response<TargetBody>, BoxError> {
    let upstream = upstream_request(inner, req)?;
    match tokio::time::timeout(inner.timeout, inner.client.request(upstream)).await {
        Err(_) => Ok(sdk_reply_response(504, "local server timed out").map(TargetBody)),
        Ok(Err(e)) => {
            let message = match cause_code(&e) {
                Some(code) => format!("local server unreachable ({code})"),
                None => "local server unreachable".into(),
            };
            Ok(sdk_reply_response(502, &message).map(TargetBody))
        }
        Ok(Ok(res)) => Ok(res.map(TargetBody::new)),
    }
}

/// The Node-style code of the first I/O error in `e`'s source chain, as the
/// JS SDK reports `fetch`'s `cause.code`.
fn cause_code(e: &(dyn StdError + 'static)) -> Option<&'static str> {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if let Some(io) = err.downcast_ref::<io::Error>() {
            return io_code(io.kind());
        }
        cur = err.source();
    }
    None
}

fn io_code(kind: io::ErrorKind) -> Option<&'static str> {
    use io::ErrorKind as K;
    Some(match kind {
        K::ConnectionRefused => "ECONNREFUSED",
        K::ConnectionReset => "ECONNRESET",
        K::ConnectionAborted => "ECONNABORTED",
        K::TimedOut => "ETIMEDOUT",
        K::AddrNotAvailable => "EADDRNOTAVAIL",
        K::AddrInUse => "EADDRINUSE",
        K::HostUnreachable => "EHOSTUNREACH",
        K::NetworkUnreachable => "ENETUNREACH",
        K::NetworkDown => "ENETDOWN",
        K::BrokenPipe => "EPIPE",
        K::PermissionDenied => "EACCES",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use ws_mixer::Stream;

    use super::super::response::SdkReply;
    use super::super::{RegistrationState, ServeContext, handler_fn, serve_stream};
    use super::*;

    fn target(url: &str) -> Target {
        Target::new(url.parse().unwrap(), DEFAULT_TARGET_TIMEOUT)
    }

    /// A request as the handler path builds it.
    fn req(path_and_query: &str) -> Request<RequestBody> {
        Request::builder()
            .method("POST")
            .uri(format!("https://pub.example{path_and_query}"))
            .header("host", "pub.example")
            .header("accept-encoding", "gzip")
            .header("x-a", "1")
            .body(Full::new(Bytes::from_static(b"{}")))
            .unwrap()
    }

    fn reply_of(res: &Response<TargetBody>) -> (u16, String) {
        let r = res.extensions().get::<SdkReply>().expect("an SDK reply");
        assert_eq!(r.status, res.status().as_u16());
        (r.status, r.message.clone())
    }

    #[tokio::test]
    async fn query_merge() {
        let t = target("http://127.0.0.1:3000/mcp");
        let uri = |t: &Target, q| upstream_uri(&t.inner, q);
        assert_eq!(uri(&t, Some("x=1")), "http://127.0.0.1:3000/mcp?x=1");
        assert_eq!(uri(&t, Some("")), "http://127.0.0.1:3000/mcp");
        let t = target("http://127.0.0.1:3000/api?k=v#frag");
        assert_eq!(uri(&t, None), "http://127.0.0.1:3000/api?k=v");
        assert_eq!(
            uri(&t, Some("x=1&y")),
            "http://127.0.0.1:3000/api?k=v&x=1&y"
        );
        let t = target("http://localhost:8080");
        assert_eq!(uri(&t, None), "http://localhost:8080/");
    }

    #[tokio::test]
    async fn upstream_headers_and_uri() {
        let t = target("http://127.0.0.1:3000/mcp");
        let r = upstream_request(&t.inner, req("/mcp?q=caf%C3%A9")).unwrap();
        assert_eq!(r.uri(), "http://127.0.0.1:3000/mcp?q=caf%C3%A9");
        let h: Vec<_> = r
            .headers()
            .iter()
            .map(|(n, v)| (n.as_str(), v.to_str().unwrap()))
            .collect();
        assert_eq!(
            h,
            [
                ("accept-encoding", "identity"),
                ("x-a", "1"),
                ("x-forwarded-host", "pub.example"),
                ("x-forwarded-proto", "https"),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "absolute http:// URL")]
    fn new_panics_on_https() {
        let _ = target("https://127.0.0.1/mcp");
    }

    #[test]
    fn try_new_refuses_without_echoing() {
        for url in [
            "https://secret.example/mcp",
            "/relative",
            "ftp://h/x",
            "http://user:secret@127.0.0.1:3000/mcp",
        ] {
            let e = Target::try_new(url.parse().unwrap(), DEFAULT_TARGET_TIMEOUT).unwrap_err();
            assert!(!e.to_string().contains("secret"), "{e}");
        }
    }

    #[tokio::test]
    async fn unreachable_is_502_with_code() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let res = target(&format!("http://{addr}/mcp"))
            .call(req("/mcp"))
            .await
            .unwrap();
        assert_eq!(
            reply_of(&res),
            (502, "local server unreachable (ECONNREFUSED)".into())
        );
    }

    #[tokio::test]
    async fn no_headers_in_time_is_504() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut t = Target::new(
            format!("http://{addr}/mcp").parse().unwrap(),
            Duration::from_millis(50),
        );
        let hold = tokio::spawn(async move { listener.accept().await });
        let res = t.call(req("/mcp")).await.unwrap();
        assert_eq!(reply_of(&res), (504, "local server timed out".into()));
        hold.abort();
    }

    /// Answers every connection with `200` and `body` after reading the head.
    async fn answering_server(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut chunk = [0u8; 1024];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let res = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(res.as_bytes()).await;
                });
            }
        });
        (addr, task)
    }

    async fn exchange(handler: &(impl crate::bridge::Handler + Clone), host: &str) -> String {
        let input = format!("POST /mcp HTTP/1.1\r\nHost: {host}\r\nContent-Length: 2\r\n\r\n{{}}");
        exchange_raw(handler, &input).await
    }

    async fn exchange_raw(handler: &(impl crate::bridge::Handler + Clone), input: &str) -> String {
        let ctx = ServeContext::new(|| RegistrationState::Registered.into());
        let (s, mut peer) = Stream::new_in_memory(1);
        let task = tokio::spawn(serve_stream(s, &ctx, handler));
        peer.write(input.as_bytes()).unwrap();
        peer.close();
        let out = peer.read_to_end().await.unwrap();
        task.await.unwrap();
        String::from_utf8(out).unwrap()
    }

    #[tokio::test]
    async fn dispatching_handler_forwards_by_host() {
        let (a_addr, a) = answering_server("A").await;
        let (b_addr, b) = answering_server("B").await;
        let hang = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hang_addr = hang.local_addr().unwrap();
        let hold = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                held.push(hang.accept().await.unwrap());
            }
        });
        let targets: Arc<HashMap<&str, Target>> = Arc::new(HashMap::from([
            ("a.example", target(&format!("http://{a_addr}/mcp"))),
            ("b.example", target(&format!("http://{b_addr}/mcp"))),
            (
                "slow.example",
                Target::new(
                    format!("http://{hang_addr}/mcp").parse().unwrap(),
                    Duration::from_millis(50),
                ),
            ),
        ]));
        let app = handler_fn(move |req: Request<RequestBody>| {
            let targets = targets.clone();
            async move {
                let host = req.uri().host().unwrap_or_default().to_owned();
                targets[host.as_str()].clone().call(req).await
            }
        });

        assert_eq!(
            exchange(&app, "a.example").await,
            "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nA"
        );
        assert_eq!(
            exchange(&app, "b.example").await,
            "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nB"
        );
        assert_eq!(
            exchange(&app, "slow.example").await,
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: application/json\r\nContent-Length: 34\r\n\r\n{\"error\":\"local server timed out\"}"
        );
        for t in [a, b, hold] {
            t.abort();
        }
    }

    #[tokio::test]
    async fn target_mode_writes_the_exact_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let t = target(&format!("http://{addr}/mcp"));
        assert_eq!(
            exchange(&t, "pub.example").await,
            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: application/json\r\nContent-Length: 51\r\n\r\n{\"error\":\"local server unreachable (ECONNREFUSED)\"}"
        );
    }

    #[tokio::test]
    async fn edited_reply_is_an_app_response() {
        let hang = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = hang.local_addr().unwrap();
        let hold = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                held.push(hang.accept().await.unwrap());
            }
        });
        let slow = Target::new(
            format!("http://{addr}/mcp").parse().unwrap(),
            Duration::from_millis(50),
        );
        let t = slow.clone();
        let with_cors = handler_fn(move |req: Request<RequestBody>| {
            let mut t = t.clone();
            async move {
                let mut res = t.call(req).await?;
                res.headers_mut()
                    .insert("access-control-allow-origin", HeaderValue::from_static("*"));
                Ok(res)
            }
        });
        assert_eq!(
            exchange(&with_cors, "pub.example").await,
            "HTTP/1.1 504 Gateway Timeout\r\ncontent-type: application/json\r\naccess-control-allow-origin: *\r\nTransfer-Encoding: chunked\r\n\r\n22\r\n{\"error\":\"local server timed out\"}\r\n0\r\n\r\n"
        );
        let t = slow.clone();
        let untagged = handler_fn(move |req: Request<RequestBody>| {
            let mut t = t.clone();
            async move {
                let mut res = t.call(req).await?;
                res.extensions_mut().clear();
                Ok(res)
            }
        });
        assert!(
            exchange(&untagged, "pub.example")
                .await
                .starts_with("HTTP/1.1 504 Gateway Timeout\r\ncontent-type: application/json\r\nTransfer-Encoding: chunked\r\n")
        );
        assert_eq!(
            exchange(&slow, "pub.example").await,
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: application/json\r\nContent-Length: 34\r\n\r\n{\"error\":\"local server timed out\"}"
        );
        hold.abort();
    }

    #[tokio::test]
    async fn unrepresentable_request_is_400_in_target_mode() {
        let t = target("http://127.0.0.1:9/mcp");
        let input =
            "POST /mcp HTTP/1.1\r\nHost: pub.example\r\nx-ctl: a\x01b\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(
            exchange_raw(&t, input).await,
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 58\r\n\r\n{\"error\":\"request cannot be represented as a web Request\"}"
        );
    }

    #[test]
    fn forwarded_host_falls_back_to_the_authority() {
        let t = target("http://127.0.0.1:3000/mcp");
        let req = Request::builder()
            .uri("https://pub.example:8443/mcp")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let up = upstream_request(&t.inner, req).unwrap();
        assert_eq!(up.headers()["x-forwarded-host"], "pub.example:8443");
    }
}

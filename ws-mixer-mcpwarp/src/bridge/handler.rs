//! The application side of a bridged request: [`Handler`], implemented for
//! every [`tower_service::Service`] of the right shape, and [`handler_fn`].

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, Request, Response};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use tower_service::Service;
use ws_mixer::BoxError;

use super::http1::{ParsedRequest, latin1_bytes};
use super::response::{HOP_BY_HOP, connection_tokens};

/// The request body a handler receives: the whole body, buffered (MCPWARP.md
/// §4.2). Empty for `GET`/`HEAD` and for a request without body framing.
pub type RequestBody = Full<Bytes>;

pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// What the application made of one request.
pub(crate) enum Outcome {
    /// Written back as is.
    Response(Response<ResponseBody>),
    /// The handler failed: `500 handler error`, reported as an error.
    Failed(BoxError),
    /// An SDK error reply (`400`, `502`, `504`), not reported.
    Reply { status: u16, message: String },
}

pub(crate) use sealed::Responder;

pub(crate) mod sealed {
    use super::{BoxFuture, Outcome, ParsedRequest};

    /// One request's worth of application: built synchronously, run on its
    /// own task.
    pub struct Responder(pub(crate) Box<dyn FnOnce(ParsedRequest) -> BoxFuture<Outcome> + Send>);

    impl Responder {
        pub(crate) fn new(
            f: impl FnOnce(ParsedRequest) -> BoxFuture<Outcome> + Send + 'static,
        ) -> Self {
            Self(Box::new(f))
        }
    }

    pub trait Sealed {
        /// The application for one request. Called when the stream arrives,
        /// so the handler is cloned before any await.
        fn responder(&self) -> Responder;
    }
}

/// An application [`serve_stream`](super::serve_stream) can hand requests
/// to.
///
/// Implemented for every [`tower_service::Service`] that takes an
/// [`http::Request<RequestBody>`] and returns an [`http::Response`] with a
/// [`Bytes`] body (cloned once per request, as tower services are), and for
/// `Target` (`target` feature). Build one from a closure
/// with [`handler_fn`]. Sealed: it cannot be implemented outside this crate.
///
/// The request a handler gets (MCPWARP.md §4.6): URI
/// `https://<Host><target>`; the edge's headers minus hop-by-hop ones,
/// `Connection`-listed names, `expect` and `content-length`; the buffered
/// body. A handler error or panic is answered `500 {"error":"handler
/// error"}` and reported through [`ServeContext::on_error`](super::ServeContext::on_error).
pub trait Handler: sealed::Sealed + Send + 'static {}

impl<S, B> sealed::Sealed for S
where
    S: Service<Request<RequestBody>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    fn responder(&self) -> Responder {
        let mut svc = self.clone();
        Responder::new(move |req| {
            Box::pin(async move {
                let Some(req) = to_handler_request(req) else {
                    return Outcome::Reply {
                        status: 400,
                        message: "request cannot be represented as a web Request".into(),
                    };
                };
                if let Err(e) = poll_fn(|cx| svc.poll_ready(cx)).await {
                    return Outcome::Failed(e.into());
                }
                match svc.call(req).await {
                    Ok(res) => Outcome::Response(res.map(|b| b.map_err(Into::into).boxed_unsync())),
                    Err(e) => Outcome::Failed(e.into()),
                }
            })
        })
    }
}

impl<S, B> Handler for S
where
    S: Service<Request<RequestBody>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
}

/// The request headers forwarded to the application (§4.6): hop-by-hop,
/// `Connection`-listed, `expect` and `content-length` removed.
pub(crate) fn forwardable_headers(headers: &[(String, String)]) -> Vec<(&str, &str)> {
    let listed = connection_tokens(
        headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case("connection"))
            .map(|(_, v)| v.as_bytes()),
    );
    headers
        .iter()
        .filter(|(n, _)| {
            let n = n.to_ascii_lowercase();
            !HOP_BY_HOP.contains(&n.as_str())
                && !listed.contains(&n)
                && n != "expect"
                && n != "content-length"
        })
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect()
}

pub(crate) fn header_pair(name: &str, value: &str) -> Option<(HeaderName, HeaderValue)> {
    let name = HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = HeaderValue::from_bytes(&latin1_bytes(value)?).ok()?;
    Some((name, value))
}

/// Whether the request's body is passed on: never for `GET`/`HEAD`.
pub(crate) fn forwarded_body(req: &ParsedRequest) -> Bytes {
    match &req.body {
        Some(b) if req.method != "GET" && req.method != "HEAD" => b.clone(),
        _ => Bytes::new(),
    }
}

/// The handler's request; `None` when `http` cannot represent it.
fn to_handler_request(req: ParsedRequest) -> Option<Request<RequestBody>> {
    let method = Method::from_bytes(req.method.as_bytes()).ok()?;
    let target = req.target.split('#').next().unwrap_or_default();
    let uri: http::Uri = format!("https://{}{}", req.host, target).parse().ok()?;
    let mut builder = Request::builder().method(method).uri(uri);
    let headers = builder.headers_mut()?;
    for (name, value) in forwardable_headers(&req.headers) {
        let (name, value) = header_pair(name, value)?;
        headers.append(name, value);
    }
    builder.body(Full::new(forwarded_body(&req))).ok()
}

/// A [`Handler`] from an async closure; see [`handler_fn`].
#[derive(Clone)]
pub struct HandlerFn<F> {
    f: F,
}

impl<F> fmt::Debug for HandlerFn<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerFn").finish_non_exhaustive()
    }
}

/// A [`Handler`] that calls `f` for each request.
///
/// ```
/// use bytes::Bytes;
/// use http::Response;
/// use http_body_util::{BodyExt, Full};
/// use ws_mixer_mcpwarp::bridge::handler_fn;
///
/// let echo = handler_fn(|req: http::Request<_>| async move {
///     let body = req.into_body().collect().await?.to_bytes();
///     Ok(Response::new(Full::new(body)))
/// });
/// # let _ = echo;
/// ```
pub fn handler_fn<F, Fut, B>(f: F) -> HandlerFn<F>
where
    F: Fn(Request<RequestBody>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<Response<B>, BoxError>> + Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    HandlerFn { f }
}

impl<F, Fut, B> Service<Request<RequestBody>> for HandlerFn<F>
where
    F: Fn(Request<RequestBody>) -> Fut,
    Fut: Future<Output = Result<Response<B>, BoxError>>,
{
    type Response = Response<B>;
    type Error = BoxError;
    type Future = Fut;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<RequestBody>) -> Fut {
        (self.f)(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(
        headers: &[(&str, &str)],
        method: &str,
        body: Option<&'static [u8]>,
    ) -> ParsedRequest {
        ParsedRequest {
            method: method.into(),
            target: "/mcp?x=1#frag".into(),
            headers: headers
                .iter()
                .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
                .collect(),
            host: "h.example".into(),
            body: body.map(Bytes::from_static),
        }
    }

    #[test]
    fn handler_request_shape() {
        let req = to_handler_request(parsed(
            &[
                ("Host", "h.example"),
                ("Connection", "x-hop, keep-alive"),
                ("X-Hop", "1"),
                ("Keep-Alive", "timeout=5"),
                ("Upgrade", "h2c"),
                ("Content-Length", "2"),
                ("Mcp-Session-Id", "abc"),
                ("Accept", "a"),
                ("accept", "b"),
                ("x-latin1", "caf\u{e9}"),
            ],
            "POST",
            Some(b"{}"),
        ))
        .unwrap();
        assert_eq!(req.uri(), "https://h.example/mcp?x=1");
        let names: Vec<_> = req.headers().keys().map(|k| k.as_str()).collect();
        assert_eq!(names, ["host", "mcp-session-id", "accept", "x-latin1"]);
        let accept: Vec<_> = req.headers().get_all("accept").iter().collect();
        assert_eq!(accept, ["a", "b"]);
        assert_eq!(req.headers()["x-latin1"].as_bytes(), b"caf\xe9");
        assert_eq!(req.body().clone().into_data_for_test(), "{}");

        let get = to_handler_request(parsed(&[], "GET", Some(b"ignored"))).unwrap();
        assert_eq!(get.body().clone().into_data_for_test(), "");
    }

    #[test]
    fn unrepresentable_requests() {
        assert!(to_handler_request(parsed(&[("x-ctl", "a\u{1}b")], "POST", None)).is_none());
        let mut p = parsed(&[], "POST", None);
        p.host = "bad host".into();
        assert!(to_handler_request(p).is_none());
    }

    trait IntoData {
        fn into_data_for_test(self) -> Bytes;
    }

    impl IntoData for Full<Bytes> {
        fn into_data_for_test(self) -> Bytes {
            futures_util::FutureExt::now_or_never(self.collect())
                .unwrap()
                .unwrap()
                .to_bytes()
        }
    }
}

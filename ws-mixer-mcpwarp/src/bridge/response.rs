//! The response side of MCPWARP.md §4: byte-exact error replies (§4.5) and
//! the writer for the application's responses (§4.1 framing, §4.6 headers).

use std::collections::HashSet;
use std::panic::AssertUnwindSafe;

use bytes::Bytes;
use futures_util::FutureExt as _;
use http::{HeaderMap, Response};
use http_body::Body as _;
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use ws_mixer::{BoxError, ErrorCode, Stream};

use super::handler::ResponseBody;
use super::panic_error;

/// Connection-scoped headers never forwarded across a hop (§4.6).
pub(crate) const HOP_BY_HOP: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The RFC 9110 reason phrase, `Unknown` for a status not listed (§4.5).
/// The same table as the JS SDK.
pub(crate) fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Content",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

/// Responses that never carry a body: to `HEAD`, `1xx`, `204`, `304`.
pub(crate) fn is_bodyless(head_request: bool, status: u16) -> bool {
    head_request || status < 200 || status == 204 || status == 304
}

/// `HTTP/1.1 <status> <reason>\r\nContent-Type: application/json\r\n
/// Content-Length: <n>\r\n\r\n{"error":"<message>"}`; to `HEAD` the same
/// head and no body (§4.5).
pub(crate) fn error_reply(status: u16, message: &str, head_request: bool) -> Vec<u8> {
    let body = serde_json::json!({ "error": message }).to_string();
    let mut out = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        reason_phrase(status),
        body.len()
    )
    .into_bytes();
    if !is_bodyless(head_request, status) {
        out.extend_from_slice(body.as_bytes());
    }
    out
}

/// Marks a response that stands for an SDK error reply (`502`/`504` from a
/// [`Target`](super::Target)): `serve_stream` writes it with
/// [`error_reply`], byte-exact, whichever handler returned it, as long as
/// the response is still as built ([`untouched_sdk_reply`]).
#[derive(Clone, Debug)]
pub(crate) struct SdkReply {
    pub(crate) status: u16,
    pub(crate) message: String,
    /// The I/O cause code of a `502`, when known.
    #[cfg_attr(
        not(feature = "target"),
        allow(dead_code, reason = "read only by target_reply")
    )]
    pub(crate) code: Option<&'static str>,
    /// Length of the JSON body.
    body_len: u64,
}

/// The error reply as an ordinary response (lower-case headers, the same
/// JSON body), tagged with [`SdkReply`].
#[cfg_attr(
    not(feature = "target"),
    allow(dead_code, reason = "only a Target produces SDK replies as responses")
)]
pub(crate) fn sdk_reply_response(
    status: u16,
    message: &str,
    code: Option<&'static str>,
) -> Response<ResponseBody> {
    let body = serde_json::json!({ "error": message }).to_string();
    let body_len = body.len() as u64;
    let mut res = Response::new(
        http_body_util::Full::new(Bytes::from(body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *res.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::BAD_GATEWAY);
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    res.extensions_mut().insert(SdkReply {
        status,
        message: message.to_owned(),
        code,
        body_len,
    });
    res
}

/// The [`SdkReply`] a response carries, if it is still exactly as
/// [`sdk_reply_response`] built it: same status, only `content-type:
/// application/json`, a body of the tagged length. A response a handler
/// edited (a header added, the status or body changed) is an app response,
/// and its tag is ignored.
pub(crate) fn untouched_sdk_reply(res: &Response<ResponseBody>) -> Option<SdkReply> {
    let tag = res.extensions().get::<SdkReply>()?;
    let headers = res.headers();
    let untouched = res.status().as_u16() == tag.status
        && headers.len() == 1
        && headers
            .get(http::header::CONTENT_TYPE)
            .map(|v| v.as_bytes())
            == Some(b"application/json")
        && res.body().size_hint().exact() == Some(tag.body_len);
    untouched.then(|| tag.clone())
}

/// Names listed in `Connection` (lower-cased): hop-by-hop too.
pub(crate) fn connection_tokens<'a>(values: impl Iterator<Item = &'a [u8]>) -> HashSet<String> {
    values
        .flat_map(|v| v.split(|&b| b == b','))
        .map(|t| String::from_utf8_lossy(t).trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

/// A `content-length` the response declares validly: exactly one value,
/// digits only, at most `2^53 - 1` (the JS SDK's bound).
fn declared_length(headers: &HeaderMap) -> Option<u64> {
    let mut values = headers.get_all(http::header::CONTENT_LENGTH).iter();
    let (Some(v), None) = (values.next(), values.next()) else {
        return None;
    };
    let v = v.to_str().ok()?.trim();
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    v.parse::<u64>().ok().filter(|n| *n < 1 << 53)
}

fn head_bytes(res: &Response<ResponseBody>, framing: Option<&str>) -> Vec<u8> {
    let status = res.status().as_u16();
    let reason = custom_reason(res).unwrap_or_else(|| reason_phrase(status).as_bytes());
    let mut out = format!("HTTP/1.1 {status} ").into_bytes();
    out.extend_from_slice(reason);
    out.extend_from_slice(b"\r\n");
    let listed = connection_tokens(
        res.headers()
            .get_all(http::header::CONNECTION)
            .iter()
            .map(|v| v.as_bytes()),
    );
    for (name, value) in res.headers() {
        let n = name.as_str();
        if HOP_BY_HOP.contains(&n) || listed.contains(n) || n == "content-length" {
            continue;
        }
        out.extend_from_slice(n.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if let Some(f) = framing {
        out.extend_from_slice(f.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// A local server's own reason phrase, which hyper keeps only when it is
/// not the canonical one.
#[cfg(feature = "target")]
fn custom_reason(res: &Response<ResponseBody>) -> Option<&[u8]> {
    res.extensions()
        .get::<hyper::ext::ReasonPhrase>()
        .map(|r| r.as_bytes())
}

#[cfg(not(feature = "target"))]
fn custom_reason(_: &Response<ResponseBody>) -> Option<&[u8]> {
    None
}

/// How writing a response ended.
#[derive(Debug)]
pub(crate) enum Written {
    /// Whole response written, write side closed.
    Complete,
    /// The stream ended under it (the edge cancelled, the connection went
    /// away); nothing more can be sent.
    Gone,
    /// Reset with `INTERNAL_ERROR` after part of the response went out;
    /// carries the body's error when the body failed.
    Reset(Option<BoxError>),
}

/// Writes `res` as HTTP/1.1, then half-closes (§4.1): bodyless for `HEAD`,
/// `1xx`, `204`, `304`; `Content-Length: 0` for a body already at its end;
/// `Content-Length: N` when the response declares a valid one; otherwise
/// chunked, one chunk per body frame. Stops quietly if the stream ends
/// while the body is pending.
pub(crate) async fn write_response(
    stream: &mut Stream,
    res: Response<ResponseBody>,
    head_request: bool,
) -> Written {
    let bodyless = is_bodyless(head_request, res.status().as_u16());
    let length = if bodyless {
        None
    } else if res.body().is_end_stream() {
        Some(0)
    } else {
        declared_length(res.headers())
    };
    let framing = match (bodyless, length) {
        (true, _) => None,
        (false, Some(n)) => Some(format!("Content-Length: {n}")),
        (false, None) => Some("Transfer-Encoding: chunked".to_owned()),
    };
    let head = head_bytes(&res, framing.as_deref());
    let mut body = res.into_body();
    if stream.write_all(&head).await.is_err() {
        return Written::Gone;
    }
    if !bodyless {
        let res = match length {
            Some(n) => copy_with_length(stream, &mut body, n).await,
            None => copy_chunked(stream, &mut body).await,
        };
        if let Some(early) = res {
            return early;
        }
    }
    drop(body);
    match stream.close_write().await {
        Ok(()) => Written::Complete,
        Err(_) => Written::Gone,
    }
}

enum Next {
    Data(Bytes),
    End,
    Failed(BoxError),
    Gone,
}

/// The next data frame, unless the stream ends first.
async fn next_data(stream: &Stream, body: &mut ResponseBody) -> Next {
    loop {
        let frame = tokio::select! {
            biased;
            _ = stream.closed() => return Next::Gone,
            // A panicking body fails the response; it is not polled again.
            f = AssertUnwindSafe(body.frame()).catch_unwind() => match f {
                Ok(f) => f,
                Err(payload) => return Next::Failed(panic_error("response body", payload)),
            },
        };
        match frame {
            None => return Next::End,
            Some(Err(e)) => return Next::Failed(e),
            Some(Ok(f)) => match f.into_data() {
                Ok(d) if !d.is_empty() => return Next::Data(d),
                // Empty data and trailers carry nothing to send.
                _ => {}
            },
        }
    }
}

fn reset_internal(stream: &Stream, message: &str, err: Option<BoxError>) -> Written {
    stream.reset(ErrorCode::INTERNAL_ERROR, message);
    Written::Reset(err)
}

async fn copy_with_length(
    stream: &mut Stream,
    body: &mut ResponseBody,
    length: u64,
) -> Option<Written> {
    let mut remaining = length;
    while remaining > 0 {
        let data = match next_data(stream, body).await {
            Next::Data(d) => d,
            Next::End => {
                return Some(reset_internal(
                    stream,
                    "response body shorter than Content-Length",
                    None,
                ));
            }
            Next::Failed(e) => {
                return Some(reset_internal(stream, "response body failed", Some(e)));
            }
            Next::Gone => return Some(Written::Gone),
        };
        let take = usize::try_from(remaining).map_or(data.len(), |r| r.min(data.len()));
        if stream.write_all(&data[..take]).await.is_err() {
            return Some(Written::Gone);
        }
        remaining -= take as u64;
    }
    None
}

async fn copy_chunked(stream: &mut Stream, body: &mut ResponseBody) -> Option<Written> {
    loop {
        let data = match next_data(stream, body).await {
            Next::Data(d) => d,
            Next::End => break,
            Next::Failed(e) => {
                return Some(reset_internal(stream, "response body failed", Some(e)));
            }
            Next::Gone => return Some(Written::Gone),
        };
        let mut chunk = format!("{:x}\r\n", data.len()).into_bytes();
        chunk.reserve(data.len() + 2);
        chunk.extend_from_slice(&data);
        chunk.extend_from_slice(b"\r\n");
        if stream.write_all(&chunk).await.is_err() {
            return Some(Written::Gone);
        }
    }
    if stream.write_all(b"0\r\n\r\n").await.is_err() {
        return Some(Written::Gone);
    }
    None
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use http_body::Frame;
    use http_body_util::{Empty, Full, StreamBody};
    use tokio::sync::mpsc;
    use ws_mixer::test_util::{PeerEvent, PeerHandle};

    use super::*;

    fn boxed<B>(b: B) -> ResponseBody
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        b.map_err(Into::into).boxed_unsync()
    }

    /// A body fed frame by frame through a channel.
    fn channel_body() -> (mpsc::UnboundedSender<Result<Bytes, BoxError>>, ResponseBody) {
        let (tx, rx) = mpsc::unbounded_channel::<Result<Bytes, BoxError>>();
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item.map(Frame::data), rx))
        });
        (tx, StreamBody::new(stream).boxed_unsync())
    }

    async fn written(peer: &mut PeerHandle) -> (Vec<u8>, Option<PeerEvent>) {
        let mut out = Vec::new();
        loop {
            match peer.recv().await {
                Some(PeerEvent::Data(d)) => out.extend_from_slice(&d),
                other => return (out, other),
            }
        }
    }

    async fn run(res: Response<ResponseBody>, head_request: bool) -> (String, Option<PeerEvent>) {
        let (mut s, mut peer) = Stream::new_in_memory(1);
        let w = write_response(&mut s, res, head_request).await;
        assert!(matches!(w, Written::Complete), "{w:?}");
        let (bytes, end) = written(&mut peer).await;
        (String::from_utf8(bytes).unwrap(), end)
    }

    #[test]
    fn error_replies_are_byte_exact() {
        assert_eq!(
            error_reply(413, "request body too large", false),
            b"HTTP/1.1 413 Content Too Large\r\nContent-Type: application/json\r\nContent-Length: 34\r\n\r\n{\"error\":\"request body too large\"}"
        );
        assert_eq!(
            error_reply(503, "service disabled", true),
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 28\r\n\r\n"
        );
        assert_eq!(
            error_reply(599, "x", false),
            b"HTTP/1.1 599 Unknown\r\nContent-Type: application/json\r\nContent-Length: 13\r\n\r\n{\"error\":\"x\"}"
        );
        // Content-Length counts UTF-8 bytes.
        let r = error_reply(400, "invalid header name \"\u{e9}\"", false);
        assert!(r.ends_with("{\"error\":\"invalid header name \\\"\u{e9}\\\"\"}".as_bytes()));
        assert!(String::from_utf8_lossy(&r).contains("Content-Length: 38\r\n"));
    }

    #[tokio::test]
    async fn chunked_without_a_declared_length() {
        let res = Response::builder()
            .header("Content-Type", "application/json")
            .body(boxed(Full::new(Bytes::from_static(b"{\"ok\":true}"))))
            .unwrap();
        let (out, end) = run(res, false).await;
        assert_eq!(
            out,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{\"ok\":true}\r\n0\r\n\r\n"
        );
        assert_eq!(end, Some(PeerEvent::Close));
    }

    #[tokio::test]
    async fn declared_length_wins_and_size_hint_is_ignored() {
        let res = Response::builder()
            .header("content-length", " 3 ")
            .body(boxed(Full::new(Bytes::from_static(b"abcdef"))))
            .unwrap();
        let (out, _) = run(res, false).await;
        assert_eq!(out, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc");

        for bad in [&["3", "3"][..], &["x"], &["-1"], &["9007199254740992"]] {
            let mut b = Response::builder();
            for v in bad {
                b = b.header("content-length", *v);
            }
            let res = b
                .body(boxed(Full::new(Bytes::from_static(b"abc"))))
                .unwrap();
            let (out, _) = run(res, false).await;
            assert!(
                out.contains("Transfer-Encoding: chunked\r\n"),
                "{bad:?}: {out}"
            );
            assert!(!out.contains("ontent-length"), "{bad:?}: {out}");
        }
    }

    #[tokio::test]
    async fn ended_body_is_content_length_zero() {
        let res = Response::builder()
            .header("content-length", "10")
            .body(boxed(Empty::<Bytes>::new()))
            .unwrap();
        let (out, _) = run(res, false).await;
        assert_eq!(out, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    }

    #[tokio::test]
    async fn bodyless_responses() {
        for (status, head_request) in [(200, true), (204, false), (304, false)] {
            let res = Response::builder()
                .status(status)
                .header("etag", "\"x\"")
                .body(boxed(Full::new(Bytes::from_static(b"never sent"))))
                .unwrap();
            let (out, end) = run(res, head_request).await;
            let reason = reason_phrase(status);
            assert_eq!(
                out,
                format!("HTTP/1.1 {status} {reason}\r\netag: \"x\"\r\n\r\n")
            );
            assert_eq!(end, Some(PeerEvent::Close));
        }
    }

    #[tokio::test]
    async fn headers_filtered_and_set_cookie_split() {
        let res = Response::builder()
            .header("Set-Cookie", "a=1")
            .header("set-cookie", "b=2; Path=/")
            .header("Connection", "close, X-Hop")
            .header("x-hop", "dropped")
            .header("keep-alive", "timeout=5")
            .header("transfer-encoding", "gzip")
            .header("X-Kept", "yes")
            .body(boxed(Empty::<Bytes>::new()))
            .unwrap();
        let (out, _) = run(res, false).await;
        assert_eq!(
            out,
            "HTTP/1.1 200 OK\r\nset-cookie: a=1\r\nset-cookie: b=2; Path=/\r\nx-kept: yes\r\nContent-Length: 0\r\n\r\n"
        );
    }

    #[tokio::test]
    async fn short_body_resets() {
        let (mut s, mut peer) = Stream::new_in_memory(1);
        let res = Response::builder()
            .header("content-length", "10")
            .body(boxed(Full::new(Bytes::from_static(b"abc"))))
            .unwrap();
        assert!(matches!(
            write_response(&mut s, res, false).await,
            Written::Reset(None)
        ));
        // A RESET discards whatever DATA was still queued, so only the end is certain.
        let (_, end) = written(&mut peer).await;
        assert_eq!(
            end,
            Some(PeerEvent::Reset {
                code: ErrorCode::INTERNAL_ERROR,
                message: "response body shorter than Content-Length".into()
            })
        );
    }

    #[tokio::test]
    async fn failing_body_resets() {
        let (mut s, mut peer) = Stream::new_in_memory(1);
        let (tx, body) = channel_body();
        tx.send(Ok(Bytes::from_static(b"part"))).unwrap();
        tx.send(Err("boom".into())).unwrap();
        let res = Response::new(body);
        let w = write_response(&mut s, res, false).await;
        assert!(
            matches!(&w, Written::Reset(Some(e)) if e.to_string() == "boom"),
            "{w:?}"
        );
        let (_, end) = written(&mut peer).await;
        assert_eq!(
            end,
            Some(PeerEvent::Reset {
                code: ErrorCode::INTERNAL_ERROR,
                message: "response body failed".into()
            })
        );
    }

    #[tokio::test]
    async fn edge_cancel_mid_body_stops_quietly() {
        let (mut s, mut peer) = Stream::new_in_memory(1);
        let (tx, body) = channel_body();
        tx.send(Ok(Bytes::from_static(b"event: 1\n\n"))).unwrap();
        let writer =
            tokio::spawn(async move { write_response(&mut s, Response::new(body), false).await });
        let mut seen = Vec::new();
        while !seen.ends_with(b"event: 1\n\n\r\n") {
            match peer.recv().await {
                Some(PeerEvent::Data(d)) => seen.extend_from_slice(&d),
                other => panic!("{other:?}"),
            }
        }
        peer.reset(ErrorCode::CANCEL, "client went away");
        assert!(matches!(writer.await.unwrap(), Written::Gone));
        assert!(
            tx.send(Ok(Bytes::from_static(b"late"))).is_err(),
            "body dropped"
        );
        assert_eq!(peer.recv().await, None, "no RESET in answer to RESET");
    }

    #[tokio::test]
    async fn empty_frames_are_skipped() {
        let frames: Vec<Result<Frame<Bytes>, Infallible>> = vec![
            Ok(Frame::data(Bytes::new())),
            Ok(Frame::data(Bytes::from_static(b"x"))),
            Ok(Frame::trailers(HeaderMap::new())),
        ];
        let body = StreamBody::new(futures_util::stream::iter(frames));
        let (out, _) = run(Response::new(boxed(body)), false).await;
        assert!(out.ends_with("\r\n\r\n1\r\nx\r\n0\r\n\r\n"), "{out}");
    }

    #[tokio::test]
    async fn panicking_body_resets_with_internal_error() {
        let panics = futures_util::stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<Frame<Bytes>, Infallible>>> {
                panic!("body boom")
            },
        );
        let (mut s, mut peer) = Stream::new_in_memory(1);
        let res = Response::new(boxed(StreamBody::new(panics)));
        let w = write_response(&mut s, res, false).await;
        assert!(
            matches!(&w, Written::Reset(Some(e)) if e.to_string() == "response body panicked: body boom"),
            "{w:?}"
        );
        let (_, end) = written(&mut peer).await;
        assert_eq!(
            end,
            Some(PeerEvent::Reset {
                code: ErrorCode::INTERNAL_ERROR,
                message: "response body failed".into()
            })
        );
    }
}

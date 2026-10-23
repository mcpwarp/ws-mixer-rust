//! The request side of MCPWARP.md §4.1-§4.2: a buffered reader over the
//! stream, the head parser and the body reader, with the spec's exact `400`
//! messages. Head bytes are decoded as latin1 (one char per byte), as the JS
//! SDK does, so a header value round-trips byte for byte.

use std::io;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Request head cap, request line and blank line included (§4.2).
pub(crate) const MAX_HEAD_BYTES: usize = 64 * 1024;
/// Chunk-size and trailer line cap, CRLF excluded (§4.2).
pub(crate) const MAX_CHUNK_LINE: usize = 4096;
/// `Number.MAX_SAFE_INTEGER`: the largest `Content-Length` the JS SDK
/// accepts, kept for parity.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

const READ_RESERVE: usize = 16 * 1024;
/// The most a body read reserves before the bytes arrive.
const MAX_UPFRONT_RESERVE: usize = 64 * 1024;

/// A malformed or over-limit request: the error reply it maps to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HttpError {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl HttpError {
    pub(crate) fn bad(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            message: message.into(),
        }
    }

    pub(crate) fn too_large() -> Self {
        Self {
            status: 413,
            message: "request body too large".into(),
        }
    }

    fn head_too_large() -> Self {
        Self {
            status: 431,
            message: "request head too large".into(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReadError {
    /// The bytes break a §4.2 rule.
    Http(HttpError),
    /// Clean end of stream in the middle of a message.
    Eof,
    /// The stream was reset or its connection lost: nobody to answer.
    Io,
}

impl From<HttpError> for ReadError {
    fn from(e: HttpError) -> Self {
        Self::Http(e)
    }
}

impl From<io::Error> for ReadError {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}

/// A buffered byte reader that owns its source.
pub(crate) struct Reader<R> {
    inner: R,
    buf: BytesMut,
    eof: bool,
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            buf: BytesMut::new(),
            eof: false,
        }
    }

    pub(crate) fn get_ref(&self) -> &R {
        &self.inner
    }

    /// The bytes read but not consumed yet.
    pub(crate) fn buffered(&self) -> &[u8] {
        &self.buf
    }

    /// The source; bytes buffered but not consumed are dropped.
    pub(crate) fn into_inner(self) -> R {
        self.inner
    }

    async fn fill(&mut self) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        self.buf.reserve(READ_RESERVE);
        if self.inner.read_buf(&mut self.buf).await? == 0 {
            self.eof = true;
            return Ok(false);
        }
        Ok(true)
    }

    /// The head up to (not including) the blank line; what follows stays
    /// buffered. `431` once the head, blank line included, cannot fit in
    /// `max` bytes.
    pub(crate) async fn read_head(&mut self, max: usize) -> Result<Bytes, ReadError> {
        let mut scanned: usize = 0;
        loop {
            let from = scanned.saturating_sub(3);
            if let Some(i) = find(&self.buf[from..], b"\r\n\r\n").map(|i| i + from) {
                if i + 4 > max {
                    return Err(HttpError::head_too_large().into());
                }
                let mut head = self.buf.split_to(i + 4);
                head.truncate(i);
                return Ok(head.freeze());
            }
            // A blank line ending within `max` would lie in what is already here.
            if self.buf.len() >= max {
                return Err(HttpError::head_too_large().into());
            }
            scanned = self.buf.len();
            if !self.fill().await? {
                return Err(ReadError::Eof);
            }
        }
    }

    /// One line, CRLF excluded. `400 line too long` past `max` bytes.
    pub(crate) async fn read_line(&mut self, max: usize) -> Result<Bytes, ReadError> {
        let mut scanned: usize = 0;
        loop {
            let from = scanned.saturating_sub(1);
            if let Some(i) = find(&self.buf[from..], b"\r\n").map(|i| i + from) {
                if i > max {
                    return Err(HttpError::bad("line too long").into());
                }
                let mut line = self.buf.split_to(i + 2);
                line.truncate(i);
                return Ok(line.freeze());
            }
            // A line of `max` bytes may have its CR here and its LF still to come.
            if self.buf.len() > max + 1 {
                return Err(HttpError::bad("line too long").into());
            }
            scanned = self.buf.len();
            if !self.fill().await? {
                return Err(ReadError::Eof);
            }
        }
    }

    /// Exactly `n` bytes. Memory grows with what arrives, not with `n`.
    pub(crate) async fn read_exact(&mut self, n: usize) -> Result<Bytes, ReadError> {
        if self.buf.len() < n {
            self.buf
                .reserve((n - self.buf.len()).min(MAX_UPFRONT_RESERVE));
        }
        while self.buf.len() < n {
            if !self.fill().await? {
                return Err(ReadError::Eof);
            }
        }
        Ok(self.buf.split_to(n).freeze())
    }
}

/// Reads and drops everything up to the end of the stream. A reset or a
/// lost connection ends it too.
pub(crate) async fn drain<R: AsyncRead + Unpin>(mut r: R) {
    let mut scratch = vec![0u8; READ_RESERVE];
    while let Ok(n) = r.read(&mut scratch).await {
        if n == 0 {
            break;
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// How the request body is framed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Framing {
    None,
    Length(u64),
    Chunked,
}

/// A validated request head; the body is still unread.
#[derive(Clone, Debug)]
pub(crate) struct RequestHead {
    pub(crate) method: String,
    /// Origin-form path and query as received (absolute-form cut to it).
    pub(crate) target: String,
    /// In order, names as received, `Expect` removed. Latin1 strings.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) host: String,
    pub(crate) framing: Framing,
}

/// A request read whole: the head and the buffered body (`None` when the
/// request had no body framing).
#[derive(Clone, Debug)]
pub(crate) struct ParsedRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) host: String,
    pub(crate) body: Option<Bytes>,
}

pub(crate) fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| char::from(b)).collect()
}

/// The bytes of a latin1 string; `None` if a char is above U+00FF.
pub(crate) fn latin1_bytes(s: &str) -> Option<Vec<u8>> {
    s.chars().map(|c| u8::try_from(c).ok()).collect()
}

/// JS `String.prototype.trim` over a latin1 string.
pub(crate) fn js_trim(s: &str) -> &str {
    s.trim_matches(|c| matches!(c, '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{a0}'))
}

/// RFC 9110 `token`: method and header-name syntax.
pub(crate) fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// Parses a request head (without its blank line) per §4.2.
pub(crate) fn parse_head(raw: &[u8]) -> Result<RequestHead, HttpError> {
    let text = latin1(raw);
    let mut lines = text.split("\r\n");
    let start = lines.next().unwrap_or_default();
    let (method, target) = parse_request_line(start)?;
    let target = parse_target(target)?;
    let all = parse_header_lines(lines)?;

    let mut hosts = all
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case("host"))
        .map(|(_, v)| v);
    let host = match (hosts.next(), hosts.next()) {
        (Some(h), None) if !h.is_empty() => h.clone(),
        _ => return Err(HttpError::bad("missing or duplicate Host header")),
    };
    let framing = body_framing(&all)?;
    let headers = all
        .into_iter()
        .filter(|(n, _)| !n.eq_ignore_ascii_case("expect"))
        .collect();
    Ok(RequestHead {
        method: method.to_owned(),
        target,
        headers,
        host,
        framing,
    })
}

/// Whether the request line at the start of `head` (up to its first CRLF, or
/// all of it) parses with method `HEAD`: every reply the SDK generates is
/// then head-only, a head-parse `400` or `431` included (§4.5).
pub(crate) fn is_head_request_line(head: &[u8]) -> bool {
    let line = &head[..find(head, b"\r\n").unwrap_or(head.len())];
    parse_request_line(&latin1(line)).is_ok_and(|(method, _)| method == "HEAD")
}

/// `^([^ ]+) ([^ ]+) HTTP/1\.([01])$`, method a token.
fn parse_request_line(line: &str) -> Result<(&str, &str), HttpError> {
    let malformed = || HttpError::bad("malformed request line");
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed());
    };
    if method.is_empty()
        || target.is_empty()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || !is_token(method)
    {
        return Err(malformed());
    }
    Ok((method, target))
}

/// Length of a leading `scheme://authority` (`scheme` per RFC 3986), if any.
fn authority_prefix(target: &str, http_only: bool) -> Option<usize> {
    let sep = target.find("://")?;
    let scheme = &target[..sep];
    let ok = if http_only {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    } else {
        scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-'))
    };
    if !ok {
        return None;
    }
    let rest = &target[sep + 3..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    Some(sep + 3 + end)
}

/// Origin-form as is; absolute-form `http(s)://` validated, then cut to its
/// path and query as written (fragment dropped, not normalised).
fn parse_target(raw: &str) -> Result<String, HttpError> {
    if raw.starts_with('/') {
        return Ok(raw.to_owned());
    }
    let Some(end) = authority_prefix(raw, true) else {
        return Err(HttpError::bad("unsupported request target form"));
    };
    let authority = &raw[raw.find("://").unwrap_or(0) + 3..end];
    // Only the authority is checked here; the path and query are escaped
    // later, exactly as an origin-form target is.
    if authority.is_empty()
        || !valid_port(authority)
        || authority.parse::<http::uri::Authority>().is_err()
    {
        return Err(HttpError::bad("malformed request target"));
    }
    let rest = raw[end..].split('#').next().unwrap_or_default();
    Ok(if rest.starts_with('/') {
        rest.to_owned()
    } else {
        format!("/{rest}")
    })
}

/// An authority's port, if any, is empty or a number up to 65535 (as a URL
/// parser requires; `http::Uri` checks it only when asked).
fn valid_port(authority: &str) -> bool {
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    let port = match host_port.rfind(']') {
        Some(i) => host_port[i + 1..].strip_prefix(':'),
        None => host_port.rsplit_once(':').map(|(_, p)| p),
    };
    match port {
        None | Some("") => true,
        Some(p) => p.bytes().all(|b| b.is_ascii_digit()) && p.parse::<u16>().is_ok(),
    }
}

/// The forwarded target (MCPWARP.md §4.3, v0.6.3): `#` and everything after
/// it dropped; every byte outside RFC 3986's set (unreserved, sub-delims,
/// `:` `@` `/` `?` `[` `]`, and `%`) escaped as `%XX`, upper-case hex: bytes
/// `>= 0x80` (a raw UTF-8 `café` becomes `caf%C3%A9`), controls, space,
/// `"`, `<`, `>`, `\`, `^`, `` ` ``, `{`, `|`, `}`. `%` is never escaped, so
/// an existing escape and a lone `%` are both kept as written.
pub(crate) fn encode_target(target: &str) -> String {
    let target = target.split('#').next().unwrap_or_default();
    let keep = |b: u8| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b':'
                    | b'@'
                    | b'/'
                    | b'?'
                    | b'['
                    | b']'
                    | b'%'
            )
    };
    let mut out = String::with_capacity(target.len());
    for c in target.chars() {
        // Head text is latin1: one char per byte.
        let b = u8::try_from(c).unwrap_or(b'?');
        if keep(b) {
            out.push(char::from(b));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push('%');
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 0xf)]));
        }
    }
    out
}

/// The path of a request target, not normalised: `scheme://authority`
/// stripped, cut at the first `?` or `#` (§4.3).
pub(crate) fn request_path(target: &str) -> &str {
    let (rest, absolute) = match authority_prefix(target, false) {
        Some(end) => (&target[end..], true),
        None => (target, false),
    };
    let path = rest.split(['?', '#']).next().unwrap_or_default();
    if absolute && path.is_empty() {
        "/"
    } else {
        path
    }
}

/// Header lines per RFC 9112 §5, names as received, values trimmed.
fn parse_header_lines<'a>(
    lines: impl Iterator<Item = &'a str>,
) -> Result<Vec<(String, String)>, HttpError> {
    let mut out = Vec::new();
    for line in lines {
        if line.starts_with([' ', '\t']) {
            return Err(HttpError::bad("obsolete header line folding"));
        }
        let colon = match line.find(':') {
            Some(i) if i > 0 => i,
            _ => return Err(HttpError::bad("malformed header line")),
        };
        let name = &line[..colon];
        if !is_token(name) {
            let quoted = serde_json::to_string(name).unwrap_or_default();
            return Err(HttpError::bad(format!("invalid header name {quoted}")));
        }
        out.push((name.to_owned(), js_trim(&line[colon + 1..]).to_owned()));
    }
    Ok(out)
}

fn body_framing(headers: &[(String, String)]) -> Result<Framing, HttpError> {
    let values = |name: &str| -> Vec<&str> {
        headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .collect()
    };
    let te = values("transfer-encoding");
    let cl = values("content-length");
    if !te.is_empty() && !cl.is_empty() {
        return Err(HttpError::bad("both Transfer-Encoding and Content-Length"));
    }
    if !te.is_empty() {
        if te.len() != 1 || !js_trim(te[0]).eq_ignore_ascii_case("chunked") {
            return Err(HttpError::bad("unsupported Transfer-Encoding"));
        }
        return Ok(Framing::Chunked);
    }
    if cl.is_empty() {
        return Ok(Framing::None);
    }
    let all: Vec<&str> = cl.iter().flat_map(|v| v.split(',')).map(js_trim).collect();
    if all
        .iter()
        .any(|v| v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(HttpError::bad("malformed Content-Length"));
    }
    if all.iter().any(|v| *v != all[0]) {
        return Err(HttpError::bad("conflicting Content-Length values"));
    }
    match all[0].parse::<u64>() {
        Ok(n) if n <= MAX_SAFE_INTEGER => Ok(Framing::Length(n)),
        _ => Err(HttpError::bad("malformed Content-Length")),
    }
}

/// Reads the body the head announces, refusing it with `413` as soon as it
/// is known to exceed `max` (before buffering past the cap). EOF inside it
/// is `400 stream ended inside the request body`.
pub(crate) async fn read_body<R: AsyncRead + Unpin>(
    r: &mut Reader<R>,
    framing: Framing,
    max: u64,
) -> Result<Option<Bytes>, ReadError> {
    let res = match framing {
        Framing::None => return Ok(None),
        Framing::Length(n) if n > max => return Err(HttpError::too_large().into()),
        Framing::Length(n) => match usize::try_from(n) {
            Ok(n) => r.read_exact(n).await,
            Err(_) => return Err(HttpError::too_large().into()),
        },
        Framing::Chunked => read_chunked(r, max).await,
    };
    match res {
        Ok(b) => Ok(Some(b)),
        Err(ReadError::Eof) => Err(HttpError::bad("stream ended inside the request body").into()),
        Err(e) => Err(e),
    }
}

/// RFC 9112 §7.1; extensions ignored, trailers read and dropped.
async fn read_chunked<R: AsyncRead + Unpin>(
    r: &mut Reader<R>,
    max: u64,
) -> Result<Bytes, ReadError> {
    let mut body = BytesMut::new();
    let mut total: u64 = 0;
    loop {
        let line = latin1(&r.read_line(MAX_CHUNK_LINE).await?);
        let size_text = js_trim(line.split(';').next().unwrap_or_default());
        if size_text.is_empty() || !size_text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(HttpError::bad("malformed chunk size").into());
        }
        // Too many digits for u64 is a size over any cap.
        let size = u64::from_str_radix(size_text, 16).unwrap_or(u64::MAX);
        if size == 0 {
            break;
        }
        total = total.saturating_add(size);
        if total > max {
            return Err(HttpError::too_large().into());
        }
        let size = usize::try_from(size).map_err(|_| HttpError::too_large())?;
        body.extend_from_slice(&r.read_exact(size).await?);
        if !r.read_line(MAX_CHUNK_LINE).await?.is_empty() {
            return Err(HttpError::bad("missing CRLF after chunk data").into());
        }
    }
    while !r.read_line(MAX_CHUNK_LINE).await?.is_empty() {}
    Ok(body.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(s: &str) -> Result<RequestHead, HttpError> {
        parse_head(s.as_bytes())
    }

    fn bad(s: &str) -> String {
        let e = head(s).unwrap_err();
        assert_eq!(e.status, 400, "{s:?}");
        e.message
    }

    async fn reader(bytes: &[u8]) -> Reader<&[u8]> {
        Reader::new(bytes)
    }

    #[test]
    fn request_line() {
        let h = head("POST /mcp?x=1 HTTP/1.1\r\nHost: h").unwrap();
        assert_eq!((h.method.as_str(), h.target.as_str()), ("POST", "/mcp?x=1"));
        assert!(head("GET / HTTP/1.0\r\nHost: h").is_ok());
        for line in [
            "GET / HTTP/2.0",
            "GET  / HTTP/1.1",
            "GET / HTTP/1.1 ",
            "G(T / HTTP/1.1",
            "GET /",
            "",
        ] {
            assert_eq!(bad(&format!("{line}\r\nHost: h")), "malformed request line");
        }
    }

    #[test]
    fn targets() {
        let t = |raw: &str| head(&format!("GET {raw} HTTP/1.1\r\nHost: h")).map(|h| h.target);
        assert_eq!(t("https://h.example/mcp?x=1#f").unwrap(), "/mcp?x=1");
        assert_eq!(t("HTTP://h.example?x=1").unwrap(), "/?x=1");
        assert_eq!(t("http://h.example").unwrap(), "/");
        for raw in ["*", "h.example:443", "ftp://h/x", "\\mcp"] {
            assert_eq!(
                t(raw).unwrap_err().message,
                "unsupported request target form"
            );
        }
        assert_eq!(
            t("http:///mcp").unwrap_err().message,
            "malformed request target"
        );
        for raw in ["http://h:9x9/", "http://h:65536/", "http://[::1/mcp"] {
            assert_eq!(
                t(raw).unwrap_err().message,
                "malformed request target",
                "{raw}"
            );
        }
        assert_eq!(t("http://u@[::1]:8080/mcp").unwrap(), "/mcp");
        // Absolute-form path and query get the same treatment as origin-form.
        let raw_utf8 = t("http://h/mcp?q=caf\u{e9}").unwrap();
        assert_eq!(raw_utf8, "/mcp?q=caf\u{c3}\u{a9}");
        assert_eq!(encode_target(&raw_utf8), "/mcp?q=caf%C3%A9");
        assert_eq!(t("http://h/mcp?x=%zz").unwrap(), "/mcp?x=%zz");
    }

    #[test]
    fn path_rule() {
        for ok in ["/mcp", "/mcp?x=1", "/mcp#f", "https://h.example/mcp?x=1"] {
            assert_eq!(request_path(ok), "/mcp", "{ok}");
        }
        for no in ["/mcp/", "/MCP", "/", "/healthz", "/a/../mcp", "\\mcp"] {
            assert_ne!(request_path(no), "/mcp", "{no}");
        }
        assert_eq!(request_path("https://h.example?x"), "/");
    }

    #[test]
    fn header_lines() {
        assert_eq!(
            bad("GET / HTTP/1.1\r\nHost: h\r\n x: folded"),
            "obsolete header line folding"
        );
        assert_eq!(
            bad("GET / HTTP/1.1\r\nHost: h\r\n\tx: folded"),
            "obsolete header line folding"
        );
        assert_eq!(
            bad("GET / HTTP/1.1\r\nHost: h\r\nno colon"),
            "malformed header line"
        );
        assert_eq!(
            bad("GET / HTTP/1.1\r\nHost: h\r\n: v"),
            "malformed header line"
        );
        assert_eq!(
            bad("GET / HTTP/1.1\r\nHost: h\r\nbad name: v"),
            r#"invalid header name "bad name""#
        );
        assert_eq!(
            bad("GET / HTTP/1.1\r\nHost: h\r\nq\"\u{1}: v"),
            r#"invalid header name "q\"\u0001""#
        );
        let h = head("GET / HTTP/1.1\r\nHost:  h \r\nX-A:\tv  \r\nexpect: 100-continue").unwrap();
        assert_eq!(h.host, "h");
        assert_eq!(
            h.headers,
            [("Host".into(), "h".into()), ("X-A".into(), "v".into())]
        );
    }

    #[test]
    fn host_rule() {
        let msg = "missing or duplicate Host header";
        assert_eq!(bad("GET / HTTP/1.1\r\nx: y"), msg);
        assert_eq!(bad("GET / HTTP/1.1\r\nHost: "), msg);
        assert_eq!(bad("GET / HTTP/1.1\r\nHost: a\r\nhost: a"), msg);
    }

    #[test]
    fn framing_rules() {
        let f =
            |extra: &str| head(&format!("POST / HTTP/1.1\r\nHost: h{extra}")).map(|h| h.framing);
        assert_eq!(f("").unwrap(), Framing::None);
        assert_eq!(f("\r\nContent-Length: 5").unwrap(), Framing::Length(5));
        assert_eq!(
            f("\r\nContent-Length: 5, 5\r\ncontent-length: 5").unwrap(),
            Framing::Length(5)
        );
        assert_eq!(
            f("\r\nTransfer-Encoding: Chunked").unwrap(),
            Framing::Chunked
        );
        let err = |extra: &str| f(extra).unwrap_err().message;
        assert_eq!(
            err("\r\nTransfer-Encoding: chunked\r\nContent-Length: 5"),
            "both Transfer-Encoding and Content-Length"
        );
        assert_eq!(
            err("\r\nTransfer-Encoding: gzip, chunked"),
            "unsupported Transfer-Encoding"
        );
        assert_eq!(
            err("\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked"),
            "unsupported Transfer-Encoding"
        );
        assert_eq!(
            err("\r\nContent-Length: 5\r\nContent-Length: 6"),
            "conflicting Content-Length values"
        );
        assert_eq!(
            err("\r\nContent-Length: 05, 5"),
            "conflicting Content-Length values"
        );
        assert_eq!(err("\r\nContent-Length: -1"), "malformed Content-Length");
        assert_eq!(err("\r\nContent-Length: 5,"), "malformed Content-Length");
        assert_eq!(
            err("\r\nContent-Length: 9007199254740992"),
            "malformed Content-Length"
        );
    }

    #[tokio::test]
    async fn head_cap_counts_the_blank_line() {
        let pad = |n: usize| {
            let mut s = String::from("GET / HTTP/1.1\r\nHost: h\r\nx: ");
            let fill = n - s.len() - 4;
            s.push_str(&"a".repeat(fill));
            s.push_str("\r\n\r\n");
            assert_eq!(s.len(), n);
            s
        };
        let fits = pad(MAX_HEAD_BYTES);
        let mut r = reader(fits.as_bytes()).await;
        assert!(r.read_head(MAX_HEAD_BYTES).await.is_ok());
        let over = pad(MAX_HEAD_BYTES + 1);
        let mut r = reader(over.as_bytes()).await;
        assert!(matches!(
            r.read_head(MAX_HEAD_BYTES).await,
            Err(ReadError::Http(e)) if e.status == 431 && e.message == "request head too large"
        ));
        // No blank line at all: refused once the cap is reached.
        let endless = "a".repeat(MAX_HEAD_BYTES);
        let mut r = reader(endless.as_bytes()).await;
        assert!(
            matches!(r.read_head(MAX_HEAD_BYTES).await, Err(ReadError::Http(e)) if e.status == 431)
        );
        let mut r = reader(b"GET / HTTP/1.1\r\n").await;
        assert!(matches!(
            r.read_head(MAX_HEAD_BYTES).await,
            Err(ReadError::Eof)
        ));
    }

    async fn chunked(body: &str, max: u64) -> Result<Option<Bytes>, ReadError> {
        let mut r = reader(body.as_bytes()).await;
        read_body(&mut r, Framing::Chunked, max).await
    }

    fn http_err(r: Result<Option<Bytes>, ReadError>) -> HttpError {
        match r {
            Err(ReadError::Http(e)) => e,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn chunked_bodies() {
        let ok = chunked(
            "3;ext=1\r\nabc\r\n 2 \r\nde\r\n0\r\nTrailer: x\r\n\r\n",
            100,
        )
        .await;
        assert_eq!(ok.unwrap().unwrap(), "abcde");
        assert_eq!(
            http_err(chunked("x\r\n", 100).await).message,
            "malformed chunk size"
        );
        assert_eq!(
            http_err(chunked("\r\n", 100).await).message,
            "malformed chunk size"
        );
        assert_eq!(
            http_err(chunked("3\r\nabcX\r\n0\r\n\r\n", 100).await).message,
            "missing CRLF after chunk data"
        );
        assert_eq!(
            http_err(chunked("3\r\nab", 100).await).message,
            "stream ended inside the request body"
        );
        let e = http_err(chunked("a\r\n0123456789\r\n7\r\n0123456\r\n0\r\n\r\n", 16).await);
        assert_eq!(
            (e.status, e.message.as_str()),
            (413, "request body too large")
        );
        let e = http_err(chunked("ffffffffffffffffffffffff\r\n", 16).await);
        assert_eq!(e.status, 413);
    }

    #[tokio::test]
    async fn chunk_line_cap() {
        let at_cap = format!("{}1\r\na\r\n0\r\n\r\n", "0".repeat(MAX_CHUNK_LINE - 1));
        assert_eq!(chunked(&at_cap, 100).await.unwrap().unwrap(), "a");
        let over = format!("{}1\r\na\r\n0\r\n\r\n", "0".repeat(MAX_CHUNK_LINE));
        assert_eq!(http_err(chunked(&over, 100).await).message, "line too long");
        // MAX + 1 bytes could still be a MAX-byte line and its CR.
        let endless = "0".repeat(MAX_CHUNK_LINE + 2);
        assert_eq!(
            http_err(chunked(&endless, 100).await).message,
            "line too long"
        );
        let trailer = format!("0\r\nx: {}\r\n\r\n", "a".repeat(MAX_CHUNK_LINE));
        assert_eq!(
            http_err(chunked(&trailer, 100).await).message,
            "line too long"
        );
    }

    #[tokio::test]
    async fn content_length_bodies() {
        let mut r = reader(b"abcdef").await;
        assert_eq!(
            read_body(&mut r, Framing::Length(3), 3)
                .await
                .unwrap()
                .unwrap(),
            "abc"
        );
        let mut r = reader(b"abcdef").await;
        let e = http_err(read_body(&mut r, Framing::Length(4), 3).await);
        assert_eq!(e.status, 413);
        let mut r = reader(b"ab").await;
        let e = http_err(read_body(&mut r, Framing::Length(3), 3).await);
        assert_eq!(e.message, "stream ended inside the request body");
        let mut r = reader(b"").await;
        assert!(read_body(&mut r, Framing::None, 0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn huge_content_length_does_not_preallocate() {
        let mut r = reader(b"only a few bytes").await;
        let res = read_body(&mut r, Framing::Length(1 << 40), u64::MAX).await;
        assert_eq!(
            http_err(res).message,
            "stream ended inside the request body"
        );
        assert!(
            r.buf.capacity() <= 4 * MAX_UPFRONT_RESERVE,
            "{}",
            r.buf.capacity()
        );

        let mut r = reader(b"ffffffffff\r\nabc").await;
        assert!(read_body(&mut r, Framing::Chunked, u64::MAX).await.is_err());
        assert!(
            r.buf.capacity() <= 4 * MAX_UPFRONT_RESERVE,
            "{}",
            r.buf.capacity()
        );
    }

    /// Yields one queued piece per read.
    struct Pieces(std::collections::VecDeque<Vec<u8>>);

    impl AsyncRead for Pieces {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            if let Some(p) = self.0.pop_front() {
                buf.put_slice(&p);
            }
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn chunk_line_at_cap_split_between_cr_and_lf() {
        let mut line = "0".repeat(MAX_CHUNK_LINE - 1);
        line.push('1');
        let pieces = [
            format!("{line}\r").into_bytes(),
            b"\na\r\n0\r\n\r\n".to_vec(),
        ];
        let mut r = Reader::new(Pieces(pieces.into_iter().collect()));
        let body = read_body(&mut r, Framing::Chunked, 100).await.unwrap();
        assert_eq!(body.unwrap(), "a");
    }

    #[test]
    fn target_encoding() {
        assert_eq!(encode_target("/mcp?q=caf\u{c3}\u{a9}"), "/mcp?q=caf%C3%A9");
        assert_eq!(
            encode_target("/a b\"<>\\^`{|}\u{7f}\u{1}"),
            "/a%20b%22%3C%3E%5C%5E%60%7B%7C%7D%7F%01"
        );
        let kept = "/p-._~!$&'()*+,;=:@/[]%2F?x=1&y=%20&z=[a]:@/?";
        assert_eq!(encode_target(kept), kept);
        // A malformed escape is passed through as written, not re-escaped.
        assert_eq!(encode_target("/mcp?x=%zz&y=%"), "/mcp?x=%zz&y=%");
        // v0.6.3 §4.3, one byte per class, then every byte.
        for (byte, want) in [
            ('a', "a"),
            ('Z', "Z"),
            ('7', "7"),
            ('-', "-"),
            ('.', "."),
            ('_', "_"),
            ('~', "~"),
            ('!', "!"),
            ('$', "$"),
            ('&', "&"),
            ('\'', "'"),
            ('(', "("),
            (')', ")"),
            ('*', "*"),
            ('+', "+"),
            (',', ","),
            (';', ";"),
            ('=', "="),
            (':', ":"),
            ('@', "@"),
            ('/', "/"),
            ('?', "?"),
            ('[', "["),
            (']', "]"),
            ('%', "%"),
            (' ', "%20"),
            ('\u{0}', "%00"),
            ('\u{1f}', "%1F"),
            ('"', "%22"),
            ('<', "%3C"),
            ('>', "%3E"),
            ('\\', "%5C"),
            ('^', "%5E"),
            ('`', "%60"),
            ('{', "%7B"),
            ('|', "%7C"),
            ('}', "%7D"),
            ('\u{7f}', "%7F"),
            ('\u{80}', "%80"),
            ('\u{ff}', "%FF"),
        ] {
            assert_eq!(
                encode_target(&format!("/{byte}")),
                format!("/{want}"),
                "{byte:?}"
            );
        }
        for b in 0..=255u8 {
            let allowed = b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/?[]%".contains(&b);
            let got = encode_target(&format!("/{}", char::from(b)));
            match b {
                b'#' => assert_eq!(got, "/"),
                _ if allowed => assert_eq!(got, format!("/{}", char::from(b))),
                _ => assert_eq!(got, format!("/%{b:02X}")),
            }
        }
        // The upcoming v0.6.3 fixtures.
        assert_eq!(encode_target("/mcp?x=100%"), "/mcp?x=100%");
        assert_eq!(encode_target("/mcp?a=1#frag"), "/mcp?a=1");
        assert_eq!(encode_target("/mcp#f?x=1"), "/mcp");
        assert_eq!(encode_target("/mcp?q=[1]"), "/mcp?q=[1]");
        for t in [
            "/mcp?q=caf\u{c3}\u{a9}",
            "/a{b}|c^d`e\"f<g>h\\i",
            "/mcp?x=%zz",
            "/mcp?x=100%",
            "/mcp?q=[1]",
        ] {
            let encoded = encode_target(t);
            assert!(
                format!("https://h{encoded}").parse::<http::Uri>().is_ok(),
                "{encoded}"
            );
        }
    }
}

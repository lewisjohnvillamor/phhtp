//! HTTP/1.1 request parsing and response serialisation.

use std::borrow::Cow;

use crate::date;

/// Maximum number of headers accepted on a request. Anything beyond this is a 431.
pub const MAX_HEADERS: usize = 64;

/// Size limits enforced while parsing.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum size of request line + headers, in bytes.
    pub max_head: usize,
    /// Maximum `Content-Length` accepted, in bytes.
    pub max_body: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_head: 16 * 1024,
            max_body: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Options,
    Patch,
    Other,
}

impl Method {
    fn from_token(s: &str) -> Method {
        match s {
            "GET" => Method::Get,
            "HEAD" => Method::Head,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "OPTIONS" => Method::Options,
            "PATCH" => Method::Patch,
            _ => Method::Other,
        }
    }
}

/// A protocol-level error. Each maps to a status code and closes the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpError {
    /// Malformed request line / headers, missing `Host` on HTTP/1.1, bad `Content-Length`.
    BadRequest,
    /// Head larger than [`Limits::max_head`] or more than [`MAX_HEADERS`] headers.
    HeadersTooLarge,
    /// `Content-Length` larger than [`Limits::max_body`].
    PayloadTooLarge,
    /// `Transfer-Encoding: chunked` request bodies are not supported.
    NotImplemented,
}

impl HttpError {
    pub fn status(self) -> u16 {
        match self {
            HttpError::BadRequest => 400,
            HttpError::HeadersTooLarge => 431,
            HttpError::PayloadTooLarge => 413,
            HttpError::NotImplemented => 501,
        }
    }
}

/// A fully parsed request borrowing from the input buffer. No allocations.
pub struct Request<'a> {
    pub method: Method,
    pub method_str: &'a str,
    /// Path without the query string.
    pub path: &'a str,
    /// Everything after the first `?`, if any.
    pub query: Option<&'a str>,
    /// `0` for HTTP/1.0, `1` for HTTP/1.1.
    pub minor_version: u8,
    pub body: &'a [u8],
    /// Whether the connection should stay open after this request is answered.
    pub keep_alive: bool,
    headers: [httparse::Header<'a>; MAX_HEADERS],
    n_headers: usize,
}

impl<'a> Request<'a> {
    pub fn headers(&self) -> &[httparse::Header<'a>] {
        &self.headers[..self.n_headers]
    }

    /// Case-insensitive header lookup (first match).
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers()
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case(name))
            .map(|h| h.value)
    }

    pub fn header_str(&self, name: &str) -> Option<&'a str> {
        self.header(name).and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Minimal `key=value&key2=value2` lookup. No percent-decoding.
    pub fn query_param(&self, key: &str) -> Option<&'a str> {
        self.query?
            .split('&')
            .filter_map(|kv| kv.split_once('=').or(Some((kv, ""))))
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v)
    }
}

// `Request` is a ~2 KiB stack value on purpose: boxing it would cost an
// allocation per request, which is exactly the overhead this crate avoids.
#[allow(clippy::large_enum_variant)]
pub enum Parse<'a> {
    /// Need more bytes.
    Partial,
    /// A complete request; `consumed` bytes of the buffer belong to it.
    Complete {
        request: Request<'a>,
        consumed: usize,
    },
}

/// Try to parse one request from the front of `buf`.
pub fn parse<'a>(buf: &'a [u8], limits: &Limits) -> Result<Parse<'a>, HttpError> {
    let mut hdrs = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut r = httparse::Request::new(&mut hdrs);
    let head_len = match r.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => {
            if buf.len() >= limits.max_head {
                return Err(HttpError::HeadersTooLarge);
            }
            return Ok(Parse::Partial);
        }
        Err(httparse::Error::TooManyHeaders) => return Err(HttpError::HeadersTooLarge),
        Err(_) => return Err(HttpError::BadRequest),
    };
    if head_len > limits.max_head {
        return Err(HttpError::HeadersTooLarge);
    }

    // httparse guarantees these are present on `Complete`.
    let method_str = r.method.ok_or(HttpError::BadRequest)?;
    let target = r.path.ok_or(HttpError::BadRequest)?;
    let minor_version = r.version.ok_or(HttpError::BadRequest)?;
    let n_headers = r.headers.len();
    // `r` is no longer used past this point, so `hdrs` is free again.

    let headers = hdrs;
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (target, None),
    };
    if !(path.starts_with('/') || path == "*") {
        return Err(HttpError::BadRequest);
    }

    let mut keep_alive = minor_version == 1;
    let mut content_length: Option<usize> = None;
    let mut has_host = false;
    for h in &headers[..n_headers] {
        if h.name.eq_ignore_ascii_case("content-length") {
            let v = parse_content_length(h.value).ok_or(HttpError::BadRequest)?;
            match content_length {
                Some(prev) if prev != v => return Err(HttpError::BadRequest),
                _ => content_length = Some(v),
            }
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            if contains_token(h.value, b"chunked") {
                return Err(HttpError::NotImplemented);
            }
            return Err(HttpError::BadRequest);
        } else if h.name.eq_ignore_ascii_case("connection") {
            if contains_token(h.value, b"close") {
                keep_alive = false;
            } else if contains_token(h.value, b"keep-alive") {
                keep_alive = true;
            }
        } else if h.name.eq_ignore_ascii_case("host") {
            has_host = true;
        }
    }
    if minor_version == 1 && !has_host {
        return Err(HttpError::BadRequest);
    }

    let body_len = content_length.unwrap_or(0);
    if body_len > limits.max_body {
        return Err(HttpError::PayloadTooLarge);
    }
    let total = head_len + body_len;
    if buf.len() < total {
        return Ok(Parse::Partial);
    }
    Ok(Parse::Complete {
        request: Request {
            method: Method::from_token(method_str),
            method_str,
            path,
            query,
            minor_version,
            body: &buf[head_len..total],
            keep_alive,
            headers,
            n_headers,
        },
        consumed: total,
    })
}

fn parse_content_length(v: &[u8]) -> Option<usize> {
    if v.is_empty() || v.len() > 18 {
        return None;
    }
    let mut n: usize = 0;
    for &b in v {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(n)
}

/// Does a comma-separated header value contain `token` (case-insensitive)?
fn contains_token(value: &[u8], token: &[u8]) -> bool {
    value
        .split(|&b| b == b',')
        .map(|t| t.trim_ascii())
        .any(|t| t.eq_ignore_ascii_case(token))
}

pub enum Body {
    Static(&'static [u8]),
    Owned(Vec<u8>),
}

impl Body {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Body::Static(b) => b,
            Body::Owned(v) => v,
        }
    }
}

/// An outgoing response. Serialised with [`Response::write_to`].
pub struct Response {
    pub status: u16,
    pub content_type: Cow<'static, str>,
    pub body: Body,
    /// Extra headers (e.g. `Allow`). Names must be valid header names.
    pub extra: Vec<(&'static str, String)>,
}

impl Response {
    pub fn new(status: u16, content_type: impl Into<Cow<'static, str>>, body: Body) -> Self {
        Self {
            status,
            content_type: content_type.into(),
            body,
            extra: Vec::new(),
        }
    }

    pub fn text(status: u16, body: &'static str) -> Self {
        Self::new(
            status,
            "text/plain; charset=utf-8",
            Body::Static(body.as_bytes()),
        )
    }

    pub fn json<T: serde::Serialize>(status: u16, value: &T) -> Self {
        let body = serde_json::to_vec(value).expect("serialising a response value cannot fail");
        Self::new(status, "application/json", Body::Owned(body))
    }

    pub fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.extra.push((name, value.into()));
        self
    }

    /// Append the wire form of this response to `out`.
    ///
    /// `keep_alive = false` adds `Connection: close`; `head_only` omits the body
    /// (for `HEAD`) while still advertising its `Content-Length`.
    pub fn write_to(&self, out: &mut Vec<u8>, keep_alive: bool, head_only: bool) {
        let body = self.body.as_slice();
        self.write_head(out, body.len(), keep_alive);
        if !head_only {
            out.extend_from_slice(body);
        }
    }

    /// Append only the status line and headers, advertising `content_length`
    /// for a body the caller sends separately (a file, a large buffer).
    pub fn write_head(&self, out: &mut Vec<u8>, content_length: usize, keep_alive: bool) {
        out.extend_from_slice(b"HTTP/1.1 ");
        push_u16(out, self.status);
        out.push(b' ');
        out.extend_from_slice(reason(self.status).as_bytes());
        out.extend_from_slice(b"\r\nServer: phhtp\r\nDate: ");
        date::with_http_date(|d| out.extend_from_slice(d));
        out.extend_from_slice(b"\r\nContent-Type: ");
        out.extend_from_slice(self.content_type.as_bytes());
        out.extend_from_slice(b"\r\nContent-Length: ");
        push_usize(out, content_length);
        for (name, value) in &self.extra {
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
        }
        if !keep_alive {
            out.extend_from_slice(b"\r\nConnection: close");
        }
        out.extend_from_slice(b"\r\n\r\n");
    }
}

fn push_u16(out: &mut Vec<u8>, v: u16) {
    push_usize(out, v as usize)
}

fn push_usize(out: &mut Vec<u8>, mut v: usize) {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&tmp[i..]);
}

pub fn reason(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        411 => "Length Required",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim() -> Limits {
        Limits::default()
    }

    #[test]
    fn parses_simple_get() {
        let buf = b"GET /a/b?x=1&y HTTP/1.1\r\nHost: h\r\nX-Foo: bar\r\n\r\n";
        match parse(buf, &lim()).unwrap() {
            Parse::Complete { request, consumed } => {
                assert_eq!(consumed, buf.len());
                assert_eq!(request.method, Method::Get);
                assert_eq!(request.path, "/a/b");
                assert_eq!(request.query, Some("x=1&y"));
                assert_eq!(request.query_param("x"), Some("1"));
                assert_eq!(request.query_param("y"), Some(""));
                assert_eq!(request.query_param("z"), None);
                assert_eq!(request.header_str("x-foo"), Some("bar"));
                assert!(request.keep_alive);
                assert!(request.body.is_empty());
            }
            _ => panic!("expected complete"),
        }
    }

    #[test]
    fn partial_then_complete_with_body() {
        let full = b"POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello";
        assert!(matches!(
            parse(&full[..10], &lim()).unwrap(),
            Parse::Partial
        ));
        assert!(matches!(
            parse(&full[..full.len() - 2], &lim()).unwrap(),
            Parse::Partial
        ));
        match parse(full, &lim()).unwrap() {
            Parse::Complete { request, consumed } => {
                assert_eq!(consumed, full.len());
                assert_eq!(request.body, b"hello");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn pipelined_requests_consume_only_the_first() {
        let buf = b"GET / HTTP/1.1\r\nHost: h\r\n\r\nGET /2 HTTP/1.1\r\nHost: h\r\n\r\n";
        match parse(buf, &lim()).unwrap() {
            Parse::Complete { request, consumed } => {
                assert_eq!(request.path, "/");
                assert_eq!(consumed, 27);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn http10_defaults_to_close() {
        let buf = b"GET / HTTP/1.0\r\n\r\n";
        let Parse::Complete { request, .. } = parse(buf, &lim()).unwrap() else {
            panic!()
        };
        assert!(!request.keep_alive);
        let buf = b"GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n";
        let Parse::Complete { request, .. } = parse(buf, &lim()).unwrap() else {
            panic!()
        };
        assert!(request.keep_alive);
        let buf = b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n";
        let Parse::Complete { request, .. } = parse(buf, &lim()).unwrap() else {
            panic!()
        };
        assert!(!request.keep_alive);
    }

    #[test]
    fn errors() {
        assert_eq!(
            parse(b"GET / HTTP/1.1\r\n\r\n", &lim()).err(),
            Some(HttpError::BadRequest)
        );
        assert_eq!(
            parse(b"BLAH\r\n\r\n", &lim()).err(),
            Some(HttpError::BadRequest)
        );
        assert_eq!(
            parse(
                b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length: x\r\n\r\n",
                &lim()
            )
            .err(),
            Some(HttpError::BadRequest)
        );
        assert_eq!(
            parse(
                b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n",
                &lim()
            )
            .err(),
            Some(HttpError::NotImplemented)
        );
        let small = Limits {
            max_head: 64,
            max_body: 8,
        };
        assert_eq!(
            parse(
                b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 9\r\n\r\n",
                &small
            )
            .err(),
            Some(HttpError::PayloadTooLarge)
        );
        let long = format!("GET /{} HTTP/1.1\r\n", "a".repeat(100));
        assert_eq!(
            parse(long.as_bytes(), &small).err(),
            Some(HttpError::HeadersTooLarge)
        );
    }

    #[test]
    fn response_wire_format() {
        let r = Response::text(200, "hi").with_header("X-A", "1");
        let mut out = Vec::new();
        r.write_to(&mut out, false, false);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\nServer: phhtp\r\nDate: "));
        assert!(s.contains("\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 2\r\nX-A: 1\r\nConnection: close\r\n\r\nhi"));
        let mut out = Vec::new();
        r.write_to(&mut out, true, true);
        let s = String::from_utf8(out).unwrap();
        assert!(s.ends_with("\r\n\r\n"));
        assert!(!s.contains("Connection: close"));
    }
}

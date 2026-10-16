//! The per-connection processing step shared by every runtime.
//!
//! Contract for a runtime:
//!
//! 1. Append whatever was read from the socket to `inbuf`.
//! 2. Call [`process`]. It consumes every *complete* request at the front of
//!    `inbuf` (pipelining works for free), appends the responses to `outbuf`,
//!    and drains what it consumed.
//! 3. If [`Step::delay_ms`] is `Some(ms)`, wait `ms` milliseconds **before**
//!    writing `outbuf`. The response is already serialised, but the handler asked
//!    for it to be held back. Do not block other connections while waiting.
//! 4. Write `outbuf` to the socket, clear it.
//! 5. If [`Step::flow`] is [`Flow::Close`], close the socket after the write.
//! 6. If a delay was honoured and `inbuf` is not empty, go back to step 2:
//!    `process` stops after a delaying request so the hold-back is applied
//!    per request, not to a whole pipelined batch. Otherwise go to step 1.

use crate::app::{App, FileReq, Outcome};
use crate::http::{parse, Method, Parse, Response};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Keep the connection open after flushing `outbuf`.
    Continue,
    /// Close the connection after flushing `outbuf`.
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    pub flow: Flow,
    /// Hold `outbuf` back for this many milliseconds before writing it.
    pub delay_ms: Option<u64>,
}

/// Largest `inbuf` a runtime should ever hold; beyond this the peer is misbehaving.
pub fn max_inbuf(app: &App) -> usize {
    app.limits.max_head + app.limits.max_body
}

/// [`process`] for runtimes that can read files: stops at a `/static` request
/// and hands it back in `file`. The runtime reads the file (through whatever
/// I/O it has), calls [`file_response`] to append the response, and then
/// continues exactly as after a delay: if `inbuf` is not empty, call again.
pub struct StepFs {
    pub step: Step,
    pub file: Option<FileReq>,
}

pub fn process(app: &App, inbuf: &mut Vec<u8>, outbuf: &mut Vec<u8>) -> Step {
    process_inner(app, inbuf, outbuf, false).step
}

pub fn process_fs(app: &App, inbuf: &mut Vec<u8>, outbuf: &mut Vec<u8>) -> StepFs {
    process_inner(app, inbuf, outbuf, true)
}

/// Append the response to a [`FileReq`] once the runtime has read the file:
/// `Ok(body)` becomes 200 (headers only for `HEAD`), `NotFound` 404, anything
/// else 500. For large bodies a runtime may pass `Ok(Vec::new())` with
/// `body_len = Some(n)` to get the head alone and send the body itself.
pub fn file_response(
    outbuf: &mut Vec<u8>,
    req: &FileReq,
    body: std::io::Result<Vec<u8>>,
    body_len: Option<usize>,
) {
    match body {
        Ok(body) => {
            let len = body_len.unwrap_or(body.len());
            Response::new(200, req.content_type, crate::http::Body::Static(b"")).write_head(
                outbuf,
                len,
                req.keep_alive,
            );
            if !req.head_only && body_len.is_none() {
                outbuf.extend_from_slice(&body);
            }
        }
        Err(e) => {
            let status = match e.kind() {
                std::io::ErrorKind::NotFound => 404,
                std::io::ErrorKind::PermissionDenied => 403,
                _ => 500,
            };
            let msg = match status {
                404 => "not found",
                403 => "forbidden",
                _ => "file read failed",
            };
            Response::json(
                status,
                &serde_json::json!({ "error": msg, "status": status }),
            )
            .write_to(outbuf, req.keep_alive, req.head_only);
        }
    }
}

fn process_inner(app: &App, inbuf: &mut Vec<u8>, outbuf: &mut Vec<u8>, files: bool) -> StepFs {
    let mut consumed = 0usize;
    let mut flow = Flow::Continue;
    let mut delay_ms = None;
    let mut file = None;

    loop {
        let rest = &inbuf[consumed..];
        if rest.is_empty() {
            break;
        }
        match parse(rest, &app.limits) {
            Ok(Parse::Partial) => break,
            Ok(Parse::Complete {
                request,
                consumed: n,
            }) => {
                consumed += n;
                let head_only = request.method == Method::Head;
                let keep = request.keep_alive;
                match app.handle(&request) {
                    Outcome::Respond(r) => r.write_to(outbuf, keep, head_only),
                    Outcome::Delay { ms, response } => {
                        response.write_to(outbuf, keep, head_only);
                        delay_ms = Some(ms);
                    }
                    Outcome::File(f) if files => file = Some(f),
                    Outcome::File(_) => Response::json(
                        501,
                        &serde_json::json!({ "error": "file serving is not implemented in this runtime", "status": 501 }),
                    )
                    .write_to(outbuf, keep, head_only),
                }
                if !keep {
                    flow = Flow::Close;
                    break;
                }
                if delay_ms.is_some() || file.is_some() {
                    break;
                }
            }
            Err(e) => {
                App::error_response(e).write_to(outbuf, false, false);
                flow = Flow::Close;
                consumed = inbuf.len();
                break;
            }
        }
    }

    if consumed > 0 {
        inbuf.drain(..consumed);
    }
    StepFs {
        step: Step { flow, delay_ms },
        file,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app() -> App {
        App::new("s", "t")
    }

    fn count_responses(out: &[u8]) -> usize {
        std::str::from_utf8(out)
            .unwrap()
            .matches("HTTP/1.1 ")
            .count()
    }

    #[test]
    fn pipelined_batch_is_answered_in_one_step() {
        let a = app();
        let mut inbuf =
            b"GET / HTTP/1.1\r\nHost: h\r\n\r\nGET /health HTTP/1.1\r\nHost: h\r\n\r\nGET /js"
                .to_vec();
        let mut out = Vec::new();
        let step = process(&a, &mut inbuf, &mut out);
        assert_eq!(
            step,
            Step {
                flow: Flow::Continue,
                delay_ms: None
            }
        );
        assert_eq!(count_responses(&out), 2);
        assert_eq!(inbuf, b"GET /js");
    }

    #[test]
    fn close_stops_processing() {
        let a = app();
        let mut inbuf =
            b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n".to_vec();
        let mut out = Vec::new();
        let step = process(&a, &mut inbuf, &mut out);
        assert_eq!(step.flow, Flow::Close);
        assert_eq!(count_responses(&out), 1);
        assert!(std::str::from_utf8(&out)
            .unwrap()
            .contains("Connection: close"));
    }

    #[test]
    fn delay_stops_after_the_delaying_request() {
        let a = app();
        let mut inbuf =
            b"GET /sleep?ms=7 HTTP/1.1\r\nHost: h\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n"
                .to_vec();
        let mut out = Vec::new();
        let step = process(&a, &mut inbuf, &mut out);
        assert_eq!(
            step,
            Step {
                flow: Flow::Continue,
                delay_ms: Some(7)
            }
        );
        assert_eq!(count_responses(&out), 1);
        assert!(!inbuf.is_empty());
        out.clear();
        let step = process(&a, &mut inbuf, &mut out);
        assert_eq!(step.delay_ms, None);
        assert_eq!(count_responses(&out), 1);
        assert!(inbuf.is_empty());
    }

    #[test]
    fn static_route_is_501_without_fs_and_a_file_req_with_it() {
        let dir = std::env::temp_dir().join(format!("phhtp-static-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), b"hello file").unwrap();
        let a = App::new("s", "t").with_static_dir(&dir);
        let mut inbuf =
            b"GET /static/a.txt HTTP/1.1\r\nHost: h\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n"
                .to_vec();
        let mut out = Vec::new();
        let step = process(&a, &mut inbuf.clone(), &mut out);
        assert_eq!(step.flow, Flow::Continue);
        assert!(std::str::from_utf8(&out)
            .unwrap()
            .starts_with("HTTP/1.1 501 "));
        assert_eq!(count_responses(&out), 2);

        out.clear();
        let sf = process_fs(&a, &mut inbuf, &mut out);
        let f = sf.file.expect("file request");
        assert_eq!(f.path, dir.join("a.txt"));
        assert!(out.is_empty(), "stops before the file response");
        assert!(!inbuf.is_empty(), "second request waits");
        file_response(&mut out, &f, std::fs::read(&f.path), None);
        let text = std::str::from_utf8(&out).unwrap();
        assert!(
            text.starts_with("HTTP/1.1 200 ") && text.ends_with("hello file"),
            "{text}"
        );
        assert!(text.contains("Content-Length: 10"));
        let sf = process_fs(&a, &mut inbuf, &mut out);
        assert!(sf.file.is_none() && inbuf.is_empty());

        // traversal and dot files never resolve
        for bad in ["/static/../x", "/static/.env", "/static/a/b", "/static/"] {
            let mut inb = format!("GET {bad} HTTP/1.1\r\nHost: h\r\n\r\n").into_bytes();
            let mut o = Vec::new();
            assert!(process_fs(&a, &mut inb, &mut o).file.is_none(), "{bad}");
            assert!(
                std::str::from_utf8(&o)
                    .unwrap()
                    .starts_with("HTTP/1.1 404 "),
                "{bad}"
            );
        }
        let mut inb = b"GET /static/missing.txt HTTP/1.1\r\nHost: h\r\n\r\n".to_vec();
        let mut o = Vec::new();
        let f = process_fs(&a, &mut inb, &mut o).file.unwrap();
        file_response(&mut o, &f, std::fs::read(&f.path), None);
        assert!(std::str::from_utf8(&o)
            .unwrap()
            .starts_with("HTTP/1.1 404 "));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn protocol_error_closes_and_discards_input() {
        let a = app();
        let mut inbuf = b"NOPE\r\n\r\ntrailing junk".to_vec();
        let mut out = Vec::new();
        let step = process(&a, &mut inbuf, &mut out);
        assert_eq!(step.flow, Flow::Close);
        assert!(inbuf.is_empty());
        assert!(std::str::from_utf8(&out)
            .unwrap()
            .starts_with("HTTP/1.1 400 "));
    }
}

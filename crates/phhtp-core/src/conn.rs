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

use crate::app::{App, Outcome};
use crate::http::{parse, Method, Parse};

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

pub fn process(app: &App, inbuf: &mut Vec<u8>, outbuf: &mut Vec<u8>) -> Step {
    let mut consumed = 0usize;
    let mut flow = Flow::Continue;
    let mut delay_ms = None;

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
                }
                if !keep {
                    flow = Flow::Close;
                    break;
                }
                if delay_ms.is_some() {
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
    Step { flow, delay_ms }
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

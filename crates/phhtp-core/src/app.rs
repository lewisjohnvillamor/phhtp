//! The application: routing and endpoint handlers.
//!
//! Endpoints (all bodies are `Content-Length` framed):
//!
//! | Method | Path             | Purpose                                              |
//! |--------|------------------|------------------------------------------------------|
//! | GET    | `/`              | `text/plain` hello. Pure framing overhead.           |
//! | GET    | `/health`        | `{"status":"ok"}`                                    |
//! | GET    | `/json`          | small JSON document built with `serde_json`          |
//! | POST   | `/echo`          | echoes the body and its `Content-Type`               |
//! | POST   | `/sign`          | HMAC-SHA256 of the body → `{"signature":"<hex>"}`    |
//! | POST   | `/verify`        | checks `X-Signature: <hex>` against the body         |
//! | GET    | `/compute?n=N`   | N chained SHA-256 rounds (CPU-bound, N ≤ 100000)     |
//! | GET    | `/sleep?ms=M`    | responds after M ms (simulated upstream I/O, ≤ 10 s) |

use serde::Serialize;

use crate::http::{Body, HttpError, Limits, Method, Request, Response};
use crate::sig;

/// What a handler wants the runtime to do.
pub enum Outcome {
    /// Write this response now.
    Respond(Response),
    /// Wait `ms` milliseconds *without blocking other connections*, then write.
    Delay { ms: u64, response: Response },
}

pub struct App {
    pub limits: Limits,
    secret: Vec<u8>,
    runtime: String,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Hello<'a> {
    message: &'static str,
    runtime: &'a str,
    method: &'a str,
    headers: usize,
}

#[derive(Serialize)]
struct Signed {
    alg: &'static str,
    bytes: usize,
    signature: String,
}

#[derive(Serialize)]
struct Verified {
    valid: bool,
}

#[derive(Serialize)]
struct Computed {
    n: u32,
    digest: String,
}

#[derive(Serialize)]
struct Slept {
    ms: u64,
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    status: u16,
}

const MAX_COMPUTE: u32 = 100_000;
const MAX_SLEEP_MS: u64 = 10_000;

impl App {
    pub fn new(secret: impl Into<Vec<u8>>, runtime: impl Into<String>) -> Self {
        Self {
            limits: Limits::default(),
            secret: secret.into(),
            runtime: runtime.into(),
        }
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn runtime(&self) -> &str {
        &self.runtime
    }

    pub fn handle(&self, req: &Request<'_>) -> Outcome {
        use Method::*;
        let m = req.method;
        let resp = match req.path {
            "/" => match m {
                Get | Head => Response::text(200, "Hello, World!"),
                _ => allow("GET, HEAD"),
            },
            "/health" => match m {
                Get | Head => Response::json(200, &Health { status: "ok" }),
                _ => allow("GET, HEAD"),
            },
            "/json" => match m {
                Get | Head => Response::json(
                    200,
                    &Hello {
                        message: "Hello, World!",
                        runtime: &self.runtime,
                        method: req.method_str,
                        headers: req.headers().len(),
                    },
                ),
                _ => allow("GET, HEAD"),
            },
            "/echo" => match m {
                Post | Put => {
                    let ct = req
                        .header_str("content-type")
                        .unwrap_or("application/octet-stream")
                        .to_owned();
                    Response::new(200, ct, Body::Owned(req.body.to_vec()))
                }
                _ => allow("POST, PUT"),
            },
            "/sign" => match m {
                Post => {
                    let sig = sig::sign(&self.secret, req.body);
                    Response::json(
                        200,
                        &Signed {
                            alg: "HMAC-SHA256",
                            bytes: req.body.len(),
                            signature: hex::encode(sig),
                        },
                    )
                }
                _ => allow("POST"),
            },
            "/verify" => match m {
                Post => match req.header_str("x-signature").map(hex::decode) {
                    Some(Ok(sig_bytes)) => {
                        if sig::verify(&self.secret, req.body, &sig_bytes) {
                            Response::json(200, &Verified { valid: true })
                        } else {
                            Response::json(401, &Verified { valid: false })
                        }
                    }
                    Some(Err(_)) => error(400, "x-signature is not valid hex"),
                    None => error(400, "missing x-signature header"),
                },
                _ => allow("POST"),
            },
            "/compute" => match m {
                Get => {
                    let n = req
                        .query_param("n")
                        .and_then(|v| v.parse::<u32>().ok())
                        .unwrap_or(1000)
                        .clamp(1, MAX_COMPUTE);
                    let seed = req.query_param("seed").unwrap_or("phhtp");
                    let digest = sig::sha256_chain(seed.as_bytes(), n);
                    Response::json(
                        200,
                        &Computed {
                            n,
                            digest: hex::encode(digest),
                        },
                    )
                }
                _ => allow("GET"),
            },
            "/sleep" => match m {
                Get => {
                    let ms = req
                        .query_param("ms")
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(10)
                        .min(MAX_SLEEP_MS);
                    return Outcome::Delay {
                        ms,
                        response: Response::json(200, &Slept { ms }),
                    };
                }
                _ => allow("GET"),
            },
            _ => error(404, "not found"),
        };
        Outcome::Respond(resp)
    }

    /// The response sent for a protocol error, before closing the connection.
    pub fn error_response(err: HttpError) -> Response {
        let msg = match err {
            HttpError::BadRequest => "bad request",
            HttpError::HeadersTooLarge => "request header fields too large",
            HttpError::PayloadTooLarge => "payload too large",
            HttpError::NotImplemented => "chunked request bodies are not supported",
        };
        error(err.status(), msg)
    }
}

fn error(status: u16, msg: &'static str) -> Response {
    Response::json(status, &ErrorBody { error: msg, status })
}

fn allow(methods: &'static str) -> Response {
    error(405, "method not allowed").with_header("Allow", methods)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{parse, Parse};

    fn app() -> App {
        App::new("secret", "test")
    }

    fn call(app: &App, raw: &[u8]) -> Response {
        let Parse::Complete { request, .. } = parse(raw, &app.limits).unwrap() else {
            panic!()
        };
        match app.handle(&request) {
            Outcome::Respond(r) => r,
            Outcome::Delay { response, .. } => response,
        }
    }

    #[test]
    fn routes() {
        let a = app();
        assert_eq!(call(&a, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n").status, 200);
        assert_eq!(
            call(&a, b"GET /nope HTTP/1.1\r\nHost: h\r\n\r\n").status,
            404
        );
        let r = call(&a, b"POST / HTTP/1.1\r\nHost: h\r\n\r\n");
        assert_eq!(r.status, 405);
        assert_eq!(r.extra, vec![("Allow", "GET, HEAD".to_string())]);
        let r = call(&a, b"GET /json HTTP/1.1\r\nHost: h\r\n\r\n");
        let v: serde_json::Value = serde_json::from_slice(r.body.as_slice()).unwrap();
        assert_eq!(v["runtime"], "test");
    }

    #[test]
    fn sign_then_verify() {
        let a = app();
        let r = call(
            &a,
            b"POST /sign HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello",
        );
        assert_eq!(r.status, 200);
        let v: serde_json::Value = serde_json::from_slice(r.body.as_slice()).unwrap();
        let sig = v["signature"].as_str().unwrap().to_owned();
        assert_eq!(sig, hex::encode(sig::sign(b"secret", b"hello")));

        let ok = format!(
            "POST /verify HTTP/1.1\r\nHost: h\r\nX-Signature: {sig}\r\nContent-Length: 5\r\n\r\nhello"
        );
        assert_eq!(call(&a, ok.as_bytes()).status, 200);
        let bad = format!(
            "POST /verify HTTP/1.1\r\nHost: h\r\nX-Signature: {sig}\r\nContent-Length: 5\r\n\r\nhellp"
        );
        assert_eq!(call(&a, bad.as_bytes()).status, 401);
        let nohex =
            b"POST /verify HTTP/1.1\r\nHost: h\r\nX-Signature: zz\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(call(&a, nohex).status, 400);
        let missing = b"POST /verify HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(call(&a, missing).status, 400);
    }

    #[test]
    fn echo_keeps_content_type() {
        let a = app();
        let r = call(
            &a,
            b"POST /echo HTTP/1.1\r\nHost: h\r\nContent-Type: text/x-y\r\nContent-Length: 3\r\n\r\nabc",
        );
        assert_eq!(r.content_type, "text/x-y");
        assert_eq!(r.body.as_slice(), b"abc");
    }

    #[test]
    fn sleep_is_a_delay_and_clamped() {
        let a = app();
        let Parse::Complete { request, .. } = parse(
            b"GET /sleep?ms=99999 HTTP/1.1\r\nHost: h\r\n\r\n",
            &a.limits,
        )
        .unwrap() else {
            panic!()
        };
        match a.handle(&request) {
            Outcome::Delay { ms, .. } => assert_eq!(ms, MAX_SLEEP_MS),
            _ => panic!("expected delay"),
        }
    }
}

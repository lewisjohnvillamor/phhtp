//! Boots every runtime on an ephemeral port and checks that they all behave
//! identically over a real socket.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use phhtp_core::App;
use phhtp_server::{serve, Config, Runtime};

fn all_runtimes() -> Vec<Runtime> {
    let mut v = vec![
        Runtime::Threads,
        Runtime::Pool,
        Runtime::Mio,
        Runtime::Tokio,
        Runtime::TokioSt,
    ];
    if phhtp_server::runtime::uring::available() {
        v.push(Runtime::Uring);
    } else {
        eprintln!("io_uring unavailable here; skipping the uring runtime");
    }
    v
}

fn boot(rt: Runtime) -> SocketAddr {
    let (tx, rx) = mpsc::channel();
    let app = Arc::new(App::new("secret", rt.name()));
    let cfg = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        workers: 2,
        ready: Some(tx),
    };
    std::thread::spawn(move || {
        serve(rt, cfg, app).expect("server exited");
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("server did not report ready")
}

/// Read exactly one response (headers + Content-Length body). Returns (status, headers, body).
fn read_response(s: &mut TcpStream, buf: &mut Vec<u8>) -> (u16, String, Vec<u8>) {
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8(buf[..pos].to_vec()).unwrap();
            let status: u16 = head[9..12].parse().unwrap();
            let cl: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("Content-Length: "))
                .unwrap()
                .parse()
                .unwrap();
            while buf.len() < pos + 4 + cl {
                let n = read_retry(s, &mut tmp);
                assert!(n > 0, "eof before body complete");
                buf.extend_from_slice(&tmp[..n]);
            }
            let body = buf[pos + 4..pos + 4 + cl].to_vec();
            buf.drain(..pos + 4 + cl);
            return (status, head, body);
        }
        let n = read_retry(s, &mut tmp);
        assert!(n > 0, "eof before headers complete");
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// `read` that retries on EINTR. io_uring delivers task-work notifications to
/// the owning process with a signal-like mechanism, and in this test the client
/// threads share a process with the server, so plain reads can be interrupted.
fn read_retry(s: &mut TcpStream, buf: &mut [u8]) -> usize {
    loop {
        match s.read(buf) {
            Ok(n) => return n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read: {e}"),
        }
    }
}

fn connect(addr: SocketAddr) -> TcpStream {
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s
}

#[test]
fn keep_alive_pipelining_and_routes() {
    for rt in all_runtimes() {
        let addr = boot(rt);
        let mut s = connect(addr);
        let mut buf = Vec::new();

        // Two pipelined requests in a single write.
        s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\nGET /json HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let (st, head, body) = read_response(&mut s, &mut buf);
        assert_eq!((rt, st), (rt, 200));
        assert!(head.contains("Server: phhtp"), "{rt:?}: {head}");
        assert!(head.contains("\r\nDate: "), "{rt:?}");
        assert!(!head.contains("Connection: close"), "{rt:?}");
        assert_eq!(body, b"Hello, World!");
        let (st, _, body) = read_response(&mut s, &mut buf);
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["runtime"], rt.name(), "{rt:?}");

        // Same connection is still open: HEAD has headers but no body.
        s.write_all(b"HEAD /json HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let (st, head, body) = read_response_head_only(&mut s, &mut buf);
        assert_eq!(st, 200);
        assert!(head.contains("Content-Length: "));
        assert!(body.is_empty());

        // 404 and 405 keep the connection open.
        s.write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\nPOST / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 404);
        let (st, head, _) = read_response(&mut s, &mut buf);
        assert_eq!(st, 405);
        assert!(head.contains("Allow: GET, HEAD"));

        // Body split across two writes.
        s.write_all(b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Type: text/x\r\nContent-Length: 11\r\n\r\nhello").unwrap();
        std::thread::sleep(Duration::from_millis(20));
        s.write_all(b" world").unwrap();
        let (st, head, body) = read_response(&mut s, &mut buf);
        assert_eq!(st, 200);
        assert!(head.contains("Content-Type: text/x"));
        assert_eq!(body, b"hello world");

        // Connection: close is honoured.
        s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        let (st, head, _) = read_response(&mut s, &mut buf);
        assert_eq!(st, 200);
        assert!(head.contains("Connection: close"));
        let mut rest = Vec::new();
        s.read_to_end(&mut rest).unwrap(); // read_to_end retries EINTR itself
        assert!(rest.is_empty(), "{rt:?}: server left bytes after close");
    }
}

fn read_response_head_only(s: &mut TcpStream, buf: &mut Vec<u8>) -> (u16, String, Vec<u8>) {
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8(buf[..pos].to_vec()).unwrap();
            let status: u16 = head[9..12].parse().unwrap();
            buf.drain(..pos + 4);
            return (status, head, Vec::new());
        }
        let n = read_retry(s, &mut tmp);
        assert!(n > 0);
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[test]
fn sign_and_verify_roundtrip() {
    for rt in all_runtimes() {
        let addr = boot(rt);
        let mut s = connect(addr);
        let mut buf = Vec::new();
        s.write_all(b"POST /sign HTTP/1.1\r\nHost: x\r\nContent-Length: 7\r\n\r\npayload")
            .unwrap();
        let (st, _, body) = read_response(&mut s, &mut buf);
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sig = v["signature"].as_str().unwrap();
        assert_eq!(sig.len(), 64);
        let req = format!("POST /verify HTTP/1.1\r\nHost: x\r\nX-Signature: {sig}\r\nContent-Length: 7\r\n\r\npayload");
        s.write_all(req.as_bytes()).unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 200);
        let req = format!("POST /verify HTTP/1.1\r\nHost: x\r\nX-Signature: {sig}\r\nContent-Length: 7\r\n\r\npayloaD");
        s.write_all(req.as_bytes()).unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 401);
    }
}

#[test]
fn protocol_errors_close_the_connection() {
    for rt in all_runtimes() {
        let addr = boot(rt);
        // Missing Host on HTTP/1.1.
        let mut s = connect(addr);
        let mut buf = Vec::new();
        s.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
        let (st, head, _) = read_response(&mut s, &mut buf);
        assert_eq!((rt, st), (rt, 400));
        assert!(head.contains("Connection: close"));
        let mut rest = Vec::new();
        s.read_to_end(&mut rest).unwrap();

        // Oversized body is rejected up front.
        let mut s = connect(addr);
        let mut buf = Vec::new();
        s.write_all(b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999\r\n\r\n")
            .unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 413);

        // Chunked request bodies are not implemented.
        let mut s = connect(addr);
        let mut buf = Vec::new();
        s.write_all(b"POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 501);

        // Garbage.
        let mut s = connect(addr);
        let mut buf = Vec::new();
        s.write_all(b"\x00\x01\x02 junk\r\n\r\n").unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 400);
    }
}

#[test]
fn sleep_does_not_block_other_connections() {
    // 8 concurrent /sleep?ms=200 must finish in ~200ms, not 8×200ms, on every
    // runtime — including the mio timer heap and the tokio sleep. The blocking
    // runtimes pass because they have one thread per connection here.
    for rt in all_runtimes() {
        let addr = boot(rt);
        let start = Instant::now();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(move || {
                    let mut s = connect(addr);
                    let mut buf = Vec::new();
                    s.write_all(b"GET /sleep?ms=200 HTTP/1.1\r\nHost: x\r\n\r\n")
                        .unwrap();
                    let (st, _, body) = read_response(&mut s, &mut buf);
                    assert_eq!(st, 200);
                    assert_eq!(body, br#"{"ms":200}"#);
                    // The connection is still usable afterwards.
                    s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
                    assert_eq!(read_response(&mut s, &mut buf).0, 200);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(200), "{rt:?}: {elapsed:?}");
        assert!(
            elapsed < Duration::from_millis(900),
            "{rt:?}: sleeps serialised: {elapsed:?}"
        );
    }
}

#[test]
fn sleep_then_pipelined_request_is_answered_after_the_delay() {
    for rt in all_runtimes() {
        let addr = boot(rt);
        let mut s = connect(addr);
        let mut buf = Vec::new();
        let t0 = Instant::now();
        s.write_all(
            b"GET /sleep?ms=100 HTTP/1.1\r\nHost: x\r\n\r\nGET /health HTTP/1.1\r\nHost: x\r\n\r\n",
        )
        .unwrap();
        assert_eq!(read_response(&mut s, &mut buf).0, 200);
        assert!(t0.elapsed() >= Duration::from_millis(100), "{rt:?}");
        let (st, _, body) = read_response(&mut s, &mut buf);
        assert_eq!(st, 200);
        assert_eq!(body, br#"{"status":"ok"}"#);
    }
}

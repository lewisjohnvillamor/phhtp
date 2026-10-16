use std::cell::Cell;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

use ringrt::net::TcpListener;
use ringrt::{spawn, time::sleep, yield_now, Runtime};

fn skip() -> bool {
    if Runtime::available() {
        false
    } else {
        eprintln!("io_uring unavailable, skipping");
        true
    }
}

#[test]
fn block_on_returns_value_and_runs_spawned_tasks() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    let counter = Rc::new(Cell::new(0));
    let c = Rc::clone(&counter);
    let v = rt
        .block_on(async move {
            for _ in 0..10 {
                let c = Rc::clone(&c);
                spawn(async move {
                    yield_now().await;
                    c.set(c.get() + 1);
                });
            }
            sleep(Duration::from_millis(20)).await;
            c.get() * 2
        })
        .unwrap();
    assert_eq!(v, 20);
}

#[test]
fn sleeps_run_concurrently() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    let t0 = Instant::now();
    rt.block_on(async {
        let done = Rc::new(Cell::new(0));
        for _ in 0..50 {
            let d = Rc::clone(&done);
            spawn(async move {
                sleep(Duration::from_millis(100)).await;
                d.set(d.get() + 1);
            });
        }
        while done.get() < 50 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .unwrap();
    let el = t0.elapsed();
    assert!(
        el >= Duration::from_millis(100) && el < Duration::from_millis(500),
        "{el:?}"
    );
}

#[test]
fn deadlock_is_an_error_not_a_hang() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    let r = rt.block_on(std::future::pending::<()>());
    assert!(r.is_err());
}

#[test]
fn second_runtime_on_same_thread_is_refused_until_drop() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    assert!(Runtime::new().is_err());
    drop(rt);
    assert!(Runtime::new().is_ok());
}

#[test]
fn tcp_echo_roundtrip() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        // Blocking std client on another thread; retries EINTR because io_uring
        // task-work notifications can interrupt syscalls in this process.
        let client = std::thread::spawn(move || {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            let msg = vec![b'x'; 100_000];
            s.write_all(&msg).unwrap();
            s.shutdown(std::net::Shutdown::Write).unwrap();
            let mut got = Vec::new();
            let mut tmp = [0u8; 8192];
            loop {
                match s.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => got.extend_from_slice(&tmp[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => panic!("{e}"),
                }
            }
            assert_eq!(got, msg);
        });

        let stream = listener.accept().await.unwrap();
        let mut buf = Vec::with_capacity(16 * 1024);
        loop {
            if stream.recv(&mut buf).await.unwrap() == 0 {
                break;
            }
            let (r, b) = stream.send_all(buf).await;
            r.unwrap();
            buf = b;
            buf.clear();
        }
        drop(stream);
        client.join().unwrap();
    })
    .unwrap();
}

#[test]
fn dropping_runtime_with_inflight_accept_is_clean() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        // Start an accept that will never complete, then return and drop everything.
        spawn(async move {
            let _ = listener.accept().await;
        });
        yield_now().await;
    })
    .unwrap();
    drop(rt); // must cancel the accept and return, not hang
}

#[test]
fn many_sequential_accepts_and_large_transfer() {
    // Exercises multishot accept across connections and multishot recv across
    // many provided buffers (1 MiB > 4 KiB buffer size).
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    eprintln!("buffer ring: {}", rt.has_buffer_ring());
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            for round in 0..20 {
                let mut s = std::net::TcpStream::connect(addr).unwrap();
                let msg = vec![b'a' + (round % 26) as u8; if round == 19 { 1 << 20 } else { 100 }];
                s.write_all(&msg).unwrap();
                s.shutdown(std::net::Shutdown::Write).unwrap();
                let mut got = Vec::new();
                let mut tmp = [0u8; 65536];
                loop {
                    match s.read(&mut tmp) {
                        Ok(0) => break,
                        Ok(n) => got.extend_from_slice(&tmp[..n]),
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => panic!("{e}"),
                    }
                }
                assert_eq!(got.len(), msg.len());
                assert_eq!(got, msg);
            }
        });
        for _ in 0..20 {
            let stream = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            loop {
                if stream.recv(&mut buf).await.unwrap() == 0 {
                    break;
                }
            }
            let (r, _) = stream.send_all(buf).await;
            r.unwrap();
        }
        client.join().unwrap();
    })
    .unwrap();
}

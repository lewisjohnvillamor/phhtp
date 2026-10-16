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

#[test]
fn detached_sends_arrive_in_order_and_flush_waits() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
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
            got
        });
        let stream = listener.accept().await.unwrap();
        let mut expected = Vec::new();
        for i in 0..64u32 {
            let mut b = ringrt::net::TcpStream::take_buffer(64 * 1024);
            b.extend(std::iter::repeat_n(b'a' + (i % 26) as u8, 40_000));
            expected.extend_from_slice(&b);
            stream.send_detached(b).await.unwrap();
        }
        stream.flush().await.unwrap();
        drop(stream);
        let got = client.join().unwrap();
        assert_eq!(got.len(), expected.len());
        assert!(got == expected, "bytes arrived out of order or corrupted");
    })
    .unwrap();
}

#[test]
fn join_handle_returns_output_and_abort_cancels() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let h = spawn(async {
            sleep(Duration::from_millis(10)).await;
            42
        });
        assert_eq!(h.await, Some(42));

        let ran = Rc::new(Cell::new(false));
        let r = Rc::clone(&ran);
        let h = spawn(async move {
            sleep(Duration::from_millis(200)).await;
            r.set(true);
        });
        yield_now().await;
        h.abort();
        assert_eq!(h.await, None);
        sleep(Duration::from_millis(250)).await;
        assert!(!ran.get(), "aborted task must not run to completion");
    })
    .unwrap();
}

#[test]
fn timeout_fires_and_passes_through() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let r = ringrt::time::timeout(Duration::from_millis(30), sleep(Duration::from_millis(300)))
            .await;
        assert!(r.is_err());
        let r = ringrt::time::timeout(Duration::from_millis(300), async { 7 }).await;
        assert_eq!(r, Ok(7));
        // An expired timeout must leave no dangling op: the runtime can still idle-detect.
    })
    .unwrap();
    assert!(rt.block_on(std::future::pending::<()>()).is_err());
}

#[test]
fn fs_read_through_the_ring() {
    if skip() {
        return;
    }
    let dir = std::env::temp_dir().join(format!("ringrt-fs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("blob.bin");
    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &data).unwrap();
    let rt = Runtime::new().unwrap();
    let (meta, got, missing) = rt
        .block_on(async move {
            let f = ringrt::fs::File::open(&path).await.unwrap();
            let meta = f.metadata().await.unwrap();
            let got = f.read_to_end(meta.size as usize).await.unwrap();
            let missing = ringrt::fs::read(&dir.join("nope")).await;
            (meta, got, missing)
        })
        .unwrap();
    assert!(meta.is_file && meta.size == data.len() as u64);
    assert_eq!(got, data);
    assert_eq!(missing.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    let _ = std::fs::remove_dir_all(
        std::env::temp_dir().join(format!("ringrt-fs-{}", std::process::id())),
    );
}

#[test]
fn zero_copy_send_delivers_everything_in_order() {
    if skip() {
        return;
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
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
            got
        });
        let stream = listener.accept().await.unwrap();
        let mut expect = Vec::new();
        for i in 0..20u8 {
            let chunk = vec![i; 100_000];
            expect.extend_from_slice(&chunk);
            if let Err(e) = stream.send_detached_zc(chunk).await {
                panic!("send_detached_zc #{i}: {e} (raw {:?})", e.raw_os_error());
            }
        }
        stream.flush().await.unwrap();
        drop(stream);
        let got = client.join().unwrap();
        assert_eq!(got.len(), expect.len());
        assert!(got == expect);
    })
    .unwrap();
}

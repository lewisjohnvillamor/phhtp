//! `phhtp-bench`: a keep-alive HTTP/1.1 load generator.
//!
//! Opens `-c` connections, sends the same request back-to-back on each one
//! (one in flight per connection, no pipelining), and records every
//! request's latency in an HDR histogram. Reports p50/p90/p99/p99.9/max.
//!
//! ```text
//! phhtp-bench run http://127.0.0.1:8080/json -c 64 -d 10
//! phhtp-bench run http://127.0.0.1:8080/sign -X POST --body-size 256 --json >> results.jsonl
//! phhtp-bench report results.jsonl
//! ```

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser)]
#[command(name = "phhtp-bench", version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)] // RunArgs is big; it is parsed once
enum Cmd {
    /// Run a load test against one URL.
    Run(RunArgs),
    /// Render JSONL results (from `run --json`) as a Markdown table.
    Report { file: String },
}

#[derive(Parser, Clone)]
struct RunArgs {
    /// Target, e.g. http://127.0.0.1:8080/json
    url: String,
    /// Concurrent keep-alive connections.
    #[arg(short, long, default_value_t = 64)]
    connections: usize,
    /// Measured duration in seconds.
    #[arg(short, long, default_value_t = 10)]
    duration: u64,
    /// Warm-up seconds (traffic is sent but not recorded).
    #[arg(short, long, default_value_t = 2)]
    warmup: u64,
    /// HTTP method.
    #[arg(short = 'X', long, default_value = "GET")]
    method: String,
    /// Literal request body.
    #[arg(long, conflicts_with = "body_size")]
    body: Option<String>,
    /// Send a body of this many pseudo-random bytes instead of `--body`.
    #[arg(long)]
    body_size: Option<usize>,
    /// Extra header(s), `Name: value`.
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
    /// Label stored with `--json` output (e.g. the server runtime).
    #[arg(long, default_value = "")]
    label: String,
    /// Emit one JSON object instead of the human-readable summary.
    #[arg(long)]
    json: bool,
    /// Client worker threads (default: all cores).
    #[arg(long)]
    threads: Option<usize>,
    /// Per-request timeout in seconds; a timed-out request counts as an error
    /// and its connection is re-opened.
    #[arg(long, default_value_t = 10)]
    timeout: u64,
    /// Free-form note stored with `--json` output (e.g. "workers=2").
    #[arg(long, default_value = "")]
    note: String,
    /// Repeat the whole measurement this many times and report the median of
    /// each statistic (plus the req/s range). Interleave configurations
    /// yourself; one run on a shared box can be ±15% off.
    #[arg(long, default_value_t = 1)]
    reps: usize,
    /// Open-loop mode: target total request rate (req/s) spread evenly over the
    /// connections. Latency is measured from the *intended* send time, so a
    /// stalled server shows up in the tail instead of being hidden by the
    /// client waiting (coordinated omission). 0 = closed loop (default).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Connection churn: send `Connection: close`, open a new connection for
    /// every request, and include the connect in the measured latency.
    #[arg(long)]
    close: bool,
    /// Do not fetch the server's `/stats` (CPU, RSS, context switches, disk)
    /// at the edges of the measured window. Fetched by default; silently
    /// skipped when the server does not serve it.
    #[arg(long)]
    no_stats: bool,
    /// Maximum simultaneous connection attempts. Bounds the SYN burst so the
    /// server's accept backlog (and the kernel's SYN queue) is not overflowed
    /// by the load generator itself; 0 = unbounded.
    #[arg(long, default_value_t = 256)]
    connect_concurrency: usize,
    /// HTTP pipelining depth: write this many requests back-to-back on a
    /// connection before reading the responses. Latency is per batch (the
    /// time until the last response of the batch arrives). 1 = off.
    #[arg(long, default_value_t = 1)]
    pipeline: usize,
    /// Extra idle connections held open for the whole run without sending
    /// anything measured (they cost the server a socket and per-connection
    /// state). With `--idle-partial` each one first sends half a request line
    /// and then stalls, like a slow or hostile client.
    #[arg(long, default_value_t = 0)]
    idle: usize,
    #[arg(long)]
    idle_partial: bool,
    /// Mixed workload: extra GET paths to rotate through round-robin with the
    /// URL's path (e.g. `--mix /json --mix "/compute?n=2000"`). Rotation is per
    /// connection, so every connection sees every path.
    #[arg(long)]
    mix: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Summary {
    label: String,
    #[serde(default)]
    note: String,
    #[serde(default = "one")]
    reps: usize,
    #[serde(default)]
    rps_min: f64,
    #[serde(default)]
    rps_max: f64,
    url: String,
    method: String,
    connections: usize,
    duration_s: u64,
    requests: u64,
    rps: f64,
    errors: u64,
    non_2xx: u64,
    bytes_read: u64,
    p50_us: u64,
    p90_us: u64,
    p99_us: u64,
    p999_us: u64,
    #[serde(default)]
    p9999_us: u64,
    max_us: u64,
    mean_us: f64,
    /// Server-side resource deltas over the measured window, from `/stats`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    server: Option<ServerUse>,
    /// This load generator's own CPU over the window, to show it is not the ceiling.
    #[serde(default)]
    client_cpu_pct: f64,
    #[serde(default)]
    client_us_per_req: f64,
    #[serde(default)]
    mode: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct ServerUse {
    /// CPU seconds consumed by the server process during the window.
    cpu_user_s: f64,
    cpu_sys_s: f64,
    /// (user + sys) / wall, in percent of one core (200 = two cores busy).
    cpu_pct: f64,
    /// CPU microseconds per completed request.
    cpu_us_per_req: f64,
    /// Context switches per request.
    ctx_vol_per_req: f64,
    ctx_invol_per_req: f64,
    minflt: u64,
    majflt: u64,
    rss_mb: f64,
    rss_peak_mb: f64,
    threads: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
}

fn one() -> usize {
    1
}

struct WorkerOut {
    hist: Histogram<u64>,
    requests: u64,
    errors: u64,
    non_2xx: u64,
    bytes: u64,
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(args) => run(args),
        Cmd::Report { file } => {
            if let Err(e) = report(&file) {
                eprintln!("report: {e}");
                std::process::exit(1);
            }
        }
    }
}

fn parse_url(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or("only http:// URLs are supported")?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().map_err(|_| "bad port")?),
        None => (hostport, 80),
    };
    Ok((host.to_string(), port, path.to_string()))
}

fn build_request(args: &RunArgs, host: &str, port: u16, path: &str) -> Vec<u8> {
    let body: Vec<u8> = match (&args.body, args.body_size) {
        (Some(b), _) => b.as_bytes().to_vec(),
        (None, Some(n)) => {
            // xorshift so the payload is not trivially compressible.
            let mut x: u32 = 0x9E37_79B9;
            (0..n)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    b'a' + (x % 26) as u8
                })
                .collect()
        }
        (None, None) => Vec::new(),
    };
    let mut req = format!(
        "{} {} HTTP/1.1\r\nHost: {}:{}\r\nUser-Agent: phhtp-bench\r\nAccept: */*\r\n",
        args.method, path, host, port
    );
    if args.close {
        req.push_str("Connection: close\r\n");
    }
    let mut has_ct = false;
    for h in &args.headers {
        req.push_str(h);
        req.push_str("\r\n");
        if h.to_ascii_lowercase().starts_with("content-type:") {
            has_ct = true;
        }
    }
    if !body.is_empty() || args.method != "GET" {
        if !has_ct && !body.is_empty() {
            req.push_str("Content-Type: application/octet-stream\r\n");
        }
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");
    let mut out = req.into_bytes();
    out.extend_from_slice(&body);
    out
}

fn run(args: RunArgs) {
    let (host, port, path) = match parse_url(&args.url) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("bad url: {e}");
            std::process::exit(2);
        }
    };
    let addr: SocketAddr = match std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), port))
        .ok()
        .and_then(|mut it| it.next())
    {
        Some(a) => a,
        None => {
            eprintln!("cannot resolve {host}:{port}");
            std::process::exit(2);
        }
    };
    let mut reqs = vec![build_request(&args, &host, port, &path)];
    for p in &args.mix {
        let mut a = args.clone();
        a.method = "GET".into();
        a.body = None;
        a.body_size = None;
        reqs.push(build_request(&a, &host, port, p));
    }
    let request = Arc::new(reqs);
    let threads = args.threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut runs: Vec<Summary> = Vec::new();
    for rep in 0..args.reps.max(1) {
        if rep > 0 {
            std::thread::sleep(Duration::from_millis(500));
        }
        runs.push(measure(&rt, &args, addr, &request));
    }
    let summary = median_summary(runs);

    if args.json {
        println!("{}", serde_json::to_string(&summary).unwrap());
    } else {
        print_human(&summary);
    }
}

fn measure(
    rt: &tokio::runtime::Runtime,
    args: &RunArgs,
    addr: SocketAddr,
    request: &Arc<Vec<Vec<u8>>>,
) -> Summary {
    let (total, server, client_cpu_s) = rt.block_on(async {
        let start = Instant::now();
        let warm_end = start + Duration::from_secs(args.warmup);
        let deadline = warm_end + Duration::from_secs(args.duration);
        let interval = if args.rate > 0 {
            Some(Duration::from_secs_f64(
                args.connections as f64 / args.rate as f64,
            ))
        } else {
            None
        };
        let cpu_task = tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(warm_end)).await;
            let a = self_cpu_seconds();
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            self_cpu_seconds() - a
        });
        let stats_task = if args.no_stats {
            None
        } else {
            let (host, port, _) = parse_url(&args.url).unwrap();
            Some(tokio::spawn(sample_server(
                addr, host, port, warm_end, deadline,
            )))
        };
        let gate = Arc::new(tokio::sync::Semaphore::new(
            if args.connect_concurrency == 0 {
                tokio::sync::Semaphore::MAX_PERMITS
            } else {
                args.connect_concurrency
            },
        ));
        let mut tasks = Vec::with_capacity(args.connections);
        for _ in 0..args.connections {
            let req = Arc::clone(request);
            tasks.push(tokio::spawn(worker(WorkerCfg {
                addr,
                req,
                warm_end,
                deadline,
                timeout: Duration::from_secs(args.timeout),
                gate: Arc::clone(&gate),
                interval,
                close: args.close,
                pipeline: args.pipeline.max(1),
            })));
        }
        let mut idlers = Vec::with_capacity(args.idle);
        for _ in 0..args.idle {
            idlers.push(tokio::spawn(idle_conn(
                addr,
                deadline,
                Arc::clone(&gate),
                args.idle_partial,
            )));
        }
        let mut total = WorkerOut {
            hist: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
            requests: 0,
            errors: 0,
            non_2xx: 0,
            bytes: 0,
        };
        for t in tasks {
            let w = t.await.expect("worker task panicked");
            total.hist.add(&w.hist).unwrap();
            total.requests += w.requests;
            total.errors += w.errors;
            total.non_2xx += w.non_2xx;
            total.bytes += w.bytes;
        }
        for t in idlers {
            let _ = t.await;
        }
        let server = match stats_task {
            Some(t) => t
                .await
                .ok()
                .flatten()
                .map(|(a, b)| server_use(&a, &b, total.requests)),
            None => None,
        };
        let client_cpu_s = cpu_task.await.unwrap_or(0.0);
        (total, server, client_cpu_s)
    });

    let h = &total.hist;
    Summary {
        label: args.label.clone(),
        note: args.note.clone(),
        reps: 1,
        rps_min: 0.0,
        rps_max: 0.0,
        url: args.url.clone(),
        method: args.method.clone(),
        connections: args.connections,
        duration_s: args.duration,
        requests: total.requests,
        rps: total.requests as f64 / args.duration as f64,
        errors: total.errors,
        non_2xx: total.non_2xx,
        bytes_read: total.bytes,
        p50_us: h.value_at_quantile(0.50),
        p90_us: h.value_at_quantile(0.90),
        p99_us: h.value_at_quantile(0.99),
        p999_us: h.value_at_quantile(0.999),
        p9999_us: h.value_at_quantile(0.9999),
        max_us: h.max(),
        mean_us: h.mean(),
        server,
        client_cpu_pct: client_cpu_s / args.duration as f64 * 100.0,
        client_us_per_req: client_cpu_s * 1e6 / total.requests.max(1) as f64,
        mode: format!(
            "{}{}",
            if args.rate > 0 {
                format!("open-loop {} req/s", args.rate)
            } else {
                "closed-loop".into()
            },
            if args.close {
                ", connection per request"
            } else {
                ", keep-alive"
            }
        ) + &if args.pipeline > 1 {
            format!(", pipeline {}", args.pipeline)
        } else {
            String::new()
        } + &if args.idle > 0 {
            format!(
                ", {} idle{}",
                args.idle,
                if args.idle_partial {
                    " (partial request)"
                } else {
                    ""
                }
            )
        } else {
            String::new()
        } + &if args.mix.is_empty() {
            String::new()
        } else {
            format!(", mix of {} paths", args.mix.len() + 1)
        },
    }
}

/// User + system CPU seconds of this process (getrusage).
fn self_cpu_seconds() -> f64 {
    // SAFETY: plain getrusage into a zeroed struct.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru.ru_utime.tv_sec as f64
            + ru.ru_utime.tv_usec as f64 / 1e6
            + ru.ru_stime.tv_sec as f64
            + ru.ru_stime.tv_usec as f64 / 1e6
    }
}

/// Fetch `/stats` just as the measured window opens and again as it closes.
async fn sample_server(
    addr: SocketAddr,
    host: String,
    port: u16,
    warm_end: Instant,
    deadline: Instant,
) -> Option<(serde_json::Value, serde_json::Value)> {
    tokio::time::sleep_until(tokio::time::Instant::from_std(warm_end)).await;
    let a = fetch_stats(addr, &host, port).await?;
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    let b = fetch_stats(addr, &host, port).await?;
    Some((a, b))
}

async fn fetch_stats(addr: SocketAddr, host: &str, port: u16) -> Option<serde_json::Value> {
    let mut s = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    let req = format!("GET /stats HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    let resp = tokio::time::timeout(Duration::from_secs(2), read_response(&mut s, &mut buf))
        .await
        .ok()?
        .ok()?;
    if resp.status != 200 {
        return None;
    }
    let body_start = buf.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    serde_json::from_slice(&buf[body_start..resp.len]).ok()
}

fn server_use(a: &serde_json::Value, b: &serde_json::Value, requests: u64) -> ServerUse {
    let f = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    let u = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
    let wall_s = (f(b, "ts_us") - f(a, "ts_us")).max(1.0) / 1e6;
    let user = f(b, "utime_s") - f(a, "utime_s");
    let sys = f(b, "stime_s") - f(a, "stime_s");
    let reqs = requests.max(1) as f64;
    ServerUse {
        cpu_user_s: user,
        cpu_sys_s: sys,
        cpu_pct: (user + sys) / wall_s * 100.0,
        cpu_us_per_req: (user + sys) * 1e6 / reqs,
        ctx_vol_per_req: (u(b, "vol_ctx").saturating_sub(u(a, "vol_ctx"))) as f64 / reqs,
        ctx_invol_per_req: (u(b, "invol_ctx").saturating_sub(u(a, "invol_ctx"))) as f64 / reqs,
        minflt: u(b, "minflt").saturating_sub(u(a, "minflt")),
        majflt: u(b, "majflt").saturating_sub(u(a, "majflt")),
        rss_mb: f(b, "rss_kb") / 1024.0,
        rss_peak_mb: f(b, "hwm_kb") / 1024.0,
        threads: u(b, "threads"),
        disk_read_bytes: u(b, "disk_read_bytes").saturating_sub(u(a, "disk_read_bytes")),
        disk_write_bytes: u(b, "disk_write_bytes").saturating_sub(u(a, "disk_write_bytes")),
    }
}

/// Median of every statistic across repetitions (each taken independently),
/// with the req/s range recorded.
fn median_summary(mut runs: Vec<Summary>) -> Summary {
    fn med_u64(v: &mut [u64]) -> u64 {
        v.sort_unstable();
        v[v.len() / 2]
    }
    fn med_f64(v: &mut [f64]) -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    }
    let n = runs.len();
    let mut first = runs.remove(0);
    if n == 1 {
        first.rps_min = first.rps;
        first.rps_max = first.rps;
        return first;
    }
    runs.insert(0, first);
    let mut rps: Vec<f64> = runs.iter().map(|r| r.rps).collect();
    let mut out = runs[0].clone_shallow();
    out.reps = n;
    out.rps_min = rps.iter().cloned().fold(f64::INFINITY, f64::min);
    out.rps_max = rps.iter().cloned().fold(0.0, f64::max);
    out.rps = med_f64(&mut rps);
    out.requests = med_u64(&mut runs.iter().map(|r| r.requests).collect::<Vec<_>>());
    out.errors = runs.iter().map(|r| r.errors).sum();
    out.non_2xx = runs.iter().map(|r| r.non_2xx).sum();
    out.bytes_read = runs.iter().map(|r| r.bytes_read).sum();
    out.p50_us = med_u64(&mut runs.iter().map(|r| r.p50_us).collect::<Vec<_>>());
    out.p90_us = med_u64(&mut runs.iter().map(|r| r.p90_us).collect::<Vec<_>>());
    out.p99_us = med_u64(&mut runs.iter().map(|r| r.p99_us).collect::<Vec<_>>());
    out.p999_us = med_u64(&mut runs.iter().map(|r| r.p999_us).collect::<Vec<_>>());
    out.p9999_us = med_u64(&mut runs.iter().map(|r| r.p9999_us).collect::<Vec<_>>());
    out.max_us = med_u64(&mut runs.iter().map(|r| r.max_us).collect::<Vec<_>>());
    out.mean_us = med_f64(&mut runs.iter().map(|r| r.mean_us).collect::<Vec<_>>());
    let servers: Vec<&ServerUse> = runs.iter().filter_map(|r| r.server.as_ref()).collect();
    if !servers.is_empty() {
        let mf = |g: fn(&ServerUse) -> f64| {
            med_f64(&mut servers.iter().map(|s| g(s)).collect::<Vec<_>>())
        };
        let mu = |g: fn(&ServerUse) -> u64| {
            med_u64(&mut servers.iter().map(|s| g(s)).collect::<Vec<_>>())
        };
        out.server = Some(ServerUse {
            cpu_user_s: mf(|s| s.cpu_user_s),
            cpu_sys_s: mf(|s| s.cpu_sys_s),
            cpu_pct: mf(|s| s.cpu_pct),
            cpu_us_per_req: mf(|s| s.cpu_us_per_req),
            ctx_vol_per_req: mf(|s| s.ctx_vol_per_req),
            ctx_invol_per_req: mf(|s| s.ctx_invol_per_req),
            minflt: mu(|s| s.minflt),
            majflt: mu(|s| s.majflt),
            rss_mb: mf(|s| s.rss_mb),
            rss_peak_mb: mf(|s| s.rss_peak_mb),
            threads: mu(|s| s.threads),
            disk_read_bytes: mu(|s| s.disk_read_bytes),
            disk_write_bytes: mu(|s| s.disk_write_bytes),
        });
    }
    out
}

impl Summary {
    fn clone_shallow(&self) -> Summary {
        serde_json::from_str(&serde_json::to_string(self).unwrap()).unwrap()
    }
}

struct WorkerCfg {
    addr: SocketAddr,
    req: Arc<Vec<Vec<u8>>>,
    warm_end: Instant,
    deadline: Instant,
    timeout: Duration,
    gate: Arc<tokio::sync::Semaphore>,
    interval: Option<Duration>,
    close: bool,
    pipeline: usize,
}

/// Hold one connection open until `deadline`, sending nothing (or, with
/// `partial`, half a request line and then nothing). Reconnects if the server
/// closes it, so a server that times out idle clients is still measured with
/// `n` idle sockets rather than zero.
async fn idle_conn(
    addr: SocketAddr,
    deadline: Instant,
    gate: Arc<tokio::sync::Semaphore>,
    partial: bool,
) {
    let mut sink = [0u8; 256];
    while Instant::now() < deadline {
        let s = {
            let _permit = gate.acquire().await.expect("semaphore never closed");
            match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr)).await {
                Ok(Ok(s)) => s,
                _ => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        };
        let mut s = s;
        if partial
            && s.write_all(b"GET /json HTTP/1.1\r\nHost: idle\r\nX-Slow: ")
                .await
                .is_err()
        {
            continue;
        }
        // Wait for the deadline or for the server to hang up, whichever first.
        let left = deadline.saturating_duration_since(Instant::now());
        let _ = tokio::time::timeout(left, s.read(&mut sink)).await;
    }
}

async fn worker(cfg: WorkerCfg) -> WorkerOut {
    let WorkerCfg {
        addr,
        req,
        warm_end,
        deadline,
        timeout,
        gate,
        interval,
        close,
        pipeline,
    } = cfg;
    let mut out = WorkerOut {
        hist: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
        requests: 0,
        errors: 0,
        non_2xx: 0,
        bytes: 0,
    };
    let mut stream: Option<TcpStream> = None;
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    // Open loop: each connection has its own schedule of intended send times.
    let next_intended = Instant::now();
    let mut seq: u64 = 0;
    // Mixed workloads rotate through the request set; pipelining writes
    // `pipeline` consecutive entries of that rotation in one go.
    let mut rr: usize = 0;
    let mut batch: Vec<u8> = Vec::new();

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        // In open-loop mode wait for the intended time; `intended` is the
        // timestamp latency is measured from, whether we were late or not.
        let intended = if let Some(iv) = interval {
            let t = next_intended + iv.mul_f64(seq as f64);
            if t > now {
                // Tokio's timer has 1 ms resolution; sleep to just short of the
                // intended time, then yield-spin the last stretch so client
                // lateness does not masquerade as server latency.
                let coarse = t.checked_sub(Duration::from_micros(1500)).unwrap_or(t);
                if coarse > Instant::now() {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(coarse)).await;
                }
                while Instant::now() < t {
                    tokio::task::yield_now().await;
                }
            }
            seq += 1;
            t
        } else {
            now
        };
        // Churn mode: the connect is part of the request.
        let t_connect = if close { Some(Instant::now()) } else { None };
        let _ = t_connect;
        let s = match stream.as_mut() {
            Some(s) => s,
            None => {
                buf.clear();
                let _permit = gate.acquire().await.expect("semaphore never closed");
                match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr)).await {
                    Ok(Ok(s)) => {
                        let _ = s.set_nodelay(true);
                        stream.insert(s)
                    }
                    _ => {
                        if now >= warm_end {
                            out.errors += 1;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                }
            }
        };

        // Latency clock: intended time (open loop) or the moment before
        // connect (churn) or the moment before the write (closed loop).
        let t0 = if interval.is_some() {
            intended
        } else if close {
            t_connect.unwrap_or(now)
        } else {
            Instant::now()
        };
        let wire: &[u8] = if pipeline == 1 && req.len() == 1 {
            &req[0]
        } else {
            batch.clear();
            for _ in 0..pipeline {
                batch.extend_from_slice(&req[rr % req.len()]);
                rr += 1;
            }
            &batch
        };
        if s.write_all(wire).await.is_err() {
            stream = None;
            if t0 >= warm_end {
                out.errors += 1;
            }
            continue;
        }
        // Read every response of the batch; the batch's latency is recorded
        // once per request so pipelined rows stay comparable per request.
        let mut got = 0usize;
        let mut bytes = 0u64;
        let mut non_2xx = 0u64;
        let mut closed = false;
        let mut failed = false;
        while got < pipeline && !closed {
            let resp = match tokio::time::timeout(timeout, read_response(s, &mut buf)).await {
                Ok(r) => r,
                Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "request timed out")),
            };
            match resp {
                Ok(resp) => {
                    got += 1;
                    bytes += resp.len as u64;
                    if !(200..300).contains(&resp.status) {
                        non_2xx += 1;
                    }
                    buf.drain(..resp.len);
                    closed = resp.close;
                }
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        let latency = t0.elapsed();
        if t0 >= warm_end {
            out.requests += got as u64;
            out.bytes += bytes;
            out.non_2xx += non_2xx;
            for _ in 0..got {
                let _ = out.hist.record(latency.as_micros().max(1) as u64);
            }
            if failed || (closed && got < pipeline) {
                out.errors += (pipeline - got).max(1) as u64;
            }
        }
        if failed || closed {
            stream = None;
            buf.clear();
        }
    }
    out
}

struct Resp {
    status: u16,
    len: usize,
    close: bool,
}

async fn read_response(s: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<Resp> {
    loop {
        if !buf.is_empty() {
            let mut hdrs = [httparse::EMPTY_HEADER; 64];
            let mut r = httparse::Response::new(&mut hdrs);
            match r.parse(buf) {
                Ok(httparse::Status::Complete(head)) => {
                    let status = r.code.unwrap_or(0);
                    let mut cl: Option<usize> = None;
                    let mut close = r.version == Some(0);
                    for h in r.headers.iter() {
                        if h.name.eq_ignore_ascii_case("content-length") {
                            cl = std::str::from_utf8(h.value)
                                .ok()
                                .and_then(|v| v.trim().parse().ok());
                        } else if h.name.eq_ignore_ascii_case("connection") {
                            if h.value.eq_ignore_ascii_case(b"close") {
                                close = true;
                            } else if h.value.eq_ignore_ascii_case(b"keep-alive") {
                                close = false;
                            }
                        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
                            return Err(io::Error::other("chunked responses unsupported"));
                        }
                    }
                    let cl = cl.ok_or_else(|| io::Error::other("no content-length"))?;
                    if buf.len() >= head + cl {
                        return Ok(Resp {
                            status,
                            len: head + cl,
                            close,
                        });
                    }
                }
                Ok(httparse::Status::Partial) => {}
                Err(e) => return Err(io::Error::other(e)),
            }
        }
        let start = buf.len();
        buf.resize(start + 16 * 1024, 0);
        let n = s.read(&mut buf[start..]).await;
        let n = match n {
            Ok(n) => n,
            Err(e) => {
                buf.truncate(start);
                return Err(e);
            }
        };
        buf.truncate(start + n);
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
    }
}

fn ms(us: u64) -> String {
    format!("{:.2}ms", us as f64 / 1000.0)
}

fn print_human(s: &Summary) {
    let label = if s.label.is_empty() {
        String::new()
    } else {
        format!("[{}] ", s.label)
    };
    println!("{label}{} {}   [{}]", s.method, s.url, s.mode);
    println!(
        "  connections {:<6} duration {}s   threads(client) {}",
        s.connections,
        s.duration_s,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );
    println!(
        "  requests    {:<10} {:>12.1} req/s   errors {}   non-2xx {}   read {:.1} MiB",
        s.requests,
        s.rps,
        s.errors,
        s.non_2xx,
        s.bytes_read as f64 / (1024.0 * 1024.0)
    );
    if s.reps > 1 {
        println!(
            "  reps        {:<10} medians above; req/s range {:.0} .. {:.0}",
            s.reps, s.rps_min, s.rps_max
        );
    }
    println!(
        "  client      cpu {:.0}% of a core   {:.1} µs CPU/req (the load generator itself)",
        s.client_cpu_pct, s.client_us_per_req
    );
    if let Some(u) = &s.server {
        println!(
            "  server      cpu {:.0}% of a core ({:.2}s user + {:.2}s sys)   {:.1} µs CPU/req   ctx {:.2}+{:.2}/req   RSS {:.1} MB (peak {:.1})   threads {}   disk r/w {}/{} B   faults {}/{}",
            u.cpu_pct, u.cpu_user_s, u.cpu_sys_s, u.cpu_us_per_req, u.ctx_vol_per_req, u.ctx_invol_per_req,
            u.rss_mb, u.rss_peak_mb, u.threads, u.disk_read_bytes, u.disk_write_bytes, u.minflt, u.majflt
        );
    }
    println!(
        "  latency     p50 {}  p90 {}  p99 {}  p99.9 {}  p99.99 {}  max {}  mean {}",
        ms(s.p50_us),
        ms(s.p90_us),
        ms(s.p99_us),
        ms(s.p999_us),
        ms(s.p9999_us),
        ms(s.max_us),
        ms(s.mean_us as u64)
    );
}

fn report(file: &str) -> io::Result<()> {
    let f = std::fs::File::open(file)?;
    let mut rows: Vec<Summary> = Vec::new();
    for line in io::BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Summary>(&line) {
            Ok(s) => rows.push(s),
            Err(e) => eprintln!("skipping line: {e}"),
        }
    }
    let endpoint = |s: &Summary| {
        let path = s
            .url
            .splitn(4, '/')
            .nth(3)
            .map(|p| format!("/{p}"))
            .unwrap_or_else(|| "/".into());
        format!("{} {}", s.method, path)
    };
    let mut out = io::stdout().lock();
    writeln!(
        out,
        "| runtime | endpoint | conns | req/s | p50 | p90 | p99 | max | errors |"
    )?;
    writeln!(out, "|---|---|---:|---:|---:|---:|---:|---:|---:|")?;
    // Group by endpoint so runtimes line up next to each other.
    let mut endpoints: Vec<String> = rows.iter().map(endpoint).collect();
    endpoints.dedup();
    let mut seen = Vec::new();
    for ep in endpoints {
        if seen.contains(&ep) {
            continue;
        }
        seen.push(ep.clone());
        for s in rows.iter().filter(|s| endpoint(s) == ep) {
            let (cpu, us, rss) = match &s.server {
                Some(u) => (
                    format!("{:.0}", u.cpu_pct),
                    format!("{:.1}", u.cpu_us_per_req),
                    format!("{:.1}", u.rss_mb),
                ),
                None => ("-".into(), "-".into(), "-".into()),
            };
            writeln!(
                out,
                "| {} | `{}` | {} | {:.0} | {} | {} | {} | {} | {} | {} | {} | {} |",
                if s.label.is_empty() { "-" } else { &s.label },
                ep,
                s.connections,
                s.rps,
                ms(s.p50_us),
                ms(s.p90_us),
                ms(s.p99_us),
                ms(s.max_us),
                s.errors + s.non_2xx,
                cpu,
                us,
                rss
            )?;
        }
    }
    Ok(())
}

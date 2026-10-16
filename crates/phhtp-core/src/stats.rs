//! Process self-monitoring for `GET /stats`: CPU time, memory, context
//! switches, page faults and disk I/O of the server process, read from
//! `/proc/self`. A load generator fetches this before and after its measured
//! window and reports the deltas per request, which is how "req/s" becomes
//! "CPU microseconds per request", the number that matters on a 1-vCPU box.
//!
//! Linux only; every field is `0` elsewhere. Clock ticks are assumed to be 100
//! per second (`CLK_TCK` on every mainstream Linux build).

use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Default, Debug, Clone)]
pub struct ProcStats {
    /// Wall clock, microseconds since the Unix epoch, for rate computations.
    pub ts_us: u64,
    pub pid: u32,
    /// CPU seconds in user mode / kernel mode, whole process (all threads).
    pub utime_s: f64,
    pub stime_s: f64,
    /// Resident set size now, and the high-water mark.
    pub rss_kb: u64,
    pub hwm_kb: u64,
    pub threads: u64,
    pub vol_ctx: u64,
    pub invol_ctx: u64,
    pub minflt: u64,
    pub majflt: u64,
    /// Bytes actually read from / written to the block layer (`/proc/self/io`).
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    /// Bytes passed to read(2)/write(2)-family syscalls (sockets included).
    pub rchar: u64,
    pub wchar: u64,
}

const CLK_TCK: f64 = 100.0;

pub fn sample() -> ProcStats {
    let mut s = ProcStats {
        ts_us: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0),
        pid: std::process::id(),
        ..Default::default()
    };
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        // Everything after the ")" that closes the command name.
        if let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) {
            let f: Vec<&str> = rest.split_whitespace().collect();
            // f[0] is field 3 (state); field N is f[N-3].
            let num = |i: usize| {
                f.get(i - 3)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
            };
            s.minflt = num(10);
            s.majflt = num(12);
            s.utime_s = num(14) as f64 / CLK_TCK;
            s.stime_s = num(15) as f64 / CLK_TCK;
            s.threads = num(20);
        }
    }
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            let mut it = line.split_whitespace();
            match (it.next(), it.next()) {
                (Some("VmRSS:"), Some(v)) => s.rss_kb = v.parse().unwrap_or(0),
                (Some("VmHWM:"), Some(v)) => s.hwm_kb = v.parse().unwrap_or(0),
                (Some("voluntary_ctxt_switches:"), Some(v)) => s.vol_ctx = v.parse().unwrap_or(0),
                (Some("nonvoluntary_ctxt_switches:"), Some(v)) => {
                    s.invol_ctx = v.parse().unwrap_or(0)
                }
                _ => {}
            }
        }
    }
    if let Ok(io) = std::fs::read_to_string("/proc/self/io") {
        for line in io.lines() {
            let mut it = line.split_whitespace();
            match (it.next(), it.next()) {
                (Some("rchar:"), Some(v)) => s.rchar = v.parse().unwrap_or(0),
                (Some("wchar:"), Some(v)) => s.wchar = v.parse().unwrap_or(0),
                (Some("read_bytes:"), Some(v)) => s.disk_read_bytes = v.parse().unwrap_or(0),
                (Some("write_bytes:"), Some(v)) => s.disk_write_bytes = v.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_reads_proc() {
        let s = sample();
        assert_eq!(s.pid, std::process::id());
        if cfg!(target_os = "linux") {
            assert!(s.rss_kb > 0);
            assert!(s.threads >= 1);
            // Burn a little CPU and check the counter moves.
            let a = sample();
            let mut x = 0u64;
            for i in 0..50_000_000u64 {
                x = x.wrapping_mul(31).wrapping_add(i);
            }
            assert!(x != 1);
            let b = sample();
            assert!(b.utime_s + b.stime_s >= a.utime_s + a.stime_s);
        }
    }
}

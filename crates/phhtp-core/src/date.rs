//! `Date:` header formatting (RFC 7231 IMF-fixdate), cached per thread per second.
//!
//! Formatting a date on every response is measurable at high request rates, so
//! each thread keeps the last formatted second around.

use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

const LEN: usize = 29;

thread_local! {
    static CACHE: Cell<(u64, [u8; LEN])> = const { Cell::new((u64::MAX, [0; LEN])) };
}

/// Call `f` with the current time formatted as e.g. `Sun, 06 Nov 1994 08:49:37 GMT`.
pub fn with_http_date<R>(f: impl FnOnce(&[u8]) -> R) -> R {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    CACHE.with(|c| {
        let (cached_secs, buf) = c.get();
        if cached_secs == secs {
            f(&buf)
        } else {
            let buf = format_http_date(secs);
            c.set((secs, buf));
            f(&buf)
        }
    })
}

/// Format a unix timestamp as IMF-fixdate. Pure, allocation-free.
pub fn format_http_date(secs: u64) -> [u8; LEN] {
    const DAYS: [&[u8; 3]; 7] = [b"Thu", b"Fri", b"Sat", b"Sun", b"Mon", b"Tue", b"Wed"];
    const MONTHS: [&[u8; 3]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];

    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u64;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as usize;
    let y = (yoe + era * 400 + if m <= 2 { 1 } else { 0 }) as u64;
    let wd = DAYS[days.rem_euclid(7) as usize];

    let mut out = [b' '; LEN];
    out[0..3].copy_from_slice(wd);
    out[3] = b',';
    two(&mut out[5..7], d);
    out[8..11].copy_from_slice(MONTHS[m - 1]);
    out[12] = b'0' + ((y / 1000) % 10) as u8;
    out[13] = b'0' + ((y / 100) % 10) as u8;
    out[14] = b'0' + ((y / 10) % 10) as u8;
    out[15] = b'0' + (y % 10) as u8;
    two(&mut out[17..19], hh);
    out[19] = b':';
    two(&mut out[20..22], mm);
    out[22] = b':';
    two(&mut out[23..25], ss);
    out[26..29].copy_from_slice(b"GMT");
    out
}

fn two(dst: &mut [u8], v: u64) {
    dst[0] = b'0' + (v / 10) as u8;
    dst[1] = b'0' + (v % 10) as u8;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        assert_eq!(
            &format_http_date(784_111_777),
            b"Sun, 06 Nov 1994 08:49:37 GMT"
        );
        assert_eq!(&format_http_date(0), b"Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(
            &format_http_date(951_782_400),
            b"Tue, 29 Feb 2000 00:00:00 GMT"
        );
        assert_eq!(
            &format_http_date(1_760_572_800),
            b"Thu, 16 Oct 2025 00:00:00 GMT"
        );
    }
}

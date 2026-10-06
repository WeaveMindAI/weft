//! Turning new work away while the worker's memory is nearly full.
//!
//! A worker takes many runs at once (`workers.concurrency`), and what one
//! run holds depends on the program: a run that loads a large file, or a
//! node that keeps a big value in memory, can take far more than another.
//! Past its memory the platform kills the whole worker, and every run on
//! it dies with it. So a worker refuses to start more work once its memory
//! is nearly full, answering `503` with a `Retry-After`: an outside caller
//! is told to try again in a moment, and a run weft hands it is handed
//! again on the next delivery. Runs already going are never touched.
//!
//! The limit is the container's own (its cgroup's `memory.max`); a
//! container whose cgroup sets none refuses nothing, and one that shows no
//! cgroup at all (a sandbox) reads its own total from `/proc/meminfo`.

use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// How full the memory may be before new work is turned away.
const FULL_ABOVE: f64 = 0.90;

/// What a refused caller is told to wait before asking again.
const RETRY_AFTER_SECS: u64 = 1;

/// How much memory is in use and how much there is, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Memory {
    pub used: u64,
    pub limit: u64,
}

impl Memory {
    /// Whether new work is turned away at this reading.
    pub fn full(&self) -> bool {
        self.limit > 0 && (self.used as f64) > (self.limit as f64) * FULL_ABOVE
    }
}

/// The container's memory as its cgroup (v2, then v1) counts it; `None`
/// when the cgroup sets no limit. Only where there is no cgroup at all (a
/// sandbox that shows none) is the machine's own count read, which there
/// is the sandbox's.
pub fn read() -> Option<Memory> {
    let file = |path: &str| std::fs::read_to_string(path).ok();
    for (current, max) in [
        ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"),
        ("/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ] {
        if std::path::Path::new(max).exists() {
            let (current, max) = (file(current), file(max));
            if current.is_none() || max.is_none() {
                // The cgroup is there and cannot be read: the guard cannot
                // see the memory, which is said once rather than per request.
                static SAID: std::sync::Once = std::sync::Once::new();
                SAID.call_once(|| {
                    tracing::warn!(
                        target: "weft_engine::memory_guard",
                        "this container's memory cgroup cannot be read, so a worker nearly out of memory does not turn new work away"
                    )
                });
            }
            return from_cgroup(current, max);
        }
    }
    file("/proc/meminfo").as_deref().and_then(from_meminfo)
}

/// A cgroup's reading: its usage and its limit, when it sets one (`max`,
/// or cgroup v1's huge "no limit" number, sets none).
fn from_cgroup(current: Option<String>, max: Option<String>) -> Option<Memory> {
    let used = current?.trim().parse::<u64>().ok()?;
    let limit = max?.trim().parse::<u64>().ok()?;
    // cgroup v1 says "no limit" with a number near the largest it holds.
    (limit < 1 << 60).then_some(Memory { used, limit })
}

/// The machine's reading from `/proc/meminfo`: all of it, and all of it
/// less what is available.
fn from_meminfo(meminfo: &str) -> Option<Memory> {
    let kib = |name: &str| {
        meminfo
            .lines()
            .find_map(|line| line.strip_prefix(name)?.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())
    };
    let total = kib("MemTotal:")? * 1024;
    let available = kib("MemAvailable:")? * 1024;
    Some(Memory { used: total.saturating_sub(available), limit: total })
}

/// Middleware: refuse a request that would start work while the memory is
/// full (see the module doc). Every request a worker serves but its health
/// check starts work: a run weft hands it, or a live caller attaching,
/// which starts its run. The health check always answers.
pub async fn refuse_when_full(request: Request, next: Next) -> Response {
    if request.uri().path() != "/_weft/healthz" {
        if let Some(memory) = read().filter(Memory::full) {
            tracing::warn!(
                target: "weft_engine::memory_guard",
                used = memory.used, limit = memory.limit, path = %request.uri().path(),
                "the worker's memory is nearly full; new work is turned away until runs end"
            );
            let mut answer = (StatusCode::SERVICE_UNAVAILABLE, "This worker is busy right now; try again in a moment.").into_response();
            answer.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECS));
            return answer;
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cgroup_with_a_limit_is_read_and_one_without_is_not() {
        assert_eq!(from_cgroup(Some("100\n".into()), Some("1000\n".into())), Some(Memory { used: 100, limit: 1000 }));
        assert_eq!(from_cgroup(Some("100\n".into()), Some("max\n".into())), None);
        assert_eq!(from_cgroup(Some("100".into()), Some("9223372036854771712".into())), None, "cgroup v1's no limit");
        assert_eq!(from_cgroup(None, Some("1000".into())), None);
    }

    #[test]
    fn meminfo_reads_total_and_available() {
        let meminfo = "MemTotal:        1000 kB\nMemFree:          100 kB\nMemAvailable:     250 kB\n";
        assert_eq!(from_meminfo(meminfo), Some(Memory { used: 750 * 1024, limit: 1000 * 1024 }));
    }

    #[test]
    fn work_is_turned_away_only_past_the_line() {
        assert!(!Memory { used: 900, limit: 1000 }.full());
        assert!(Memory { used: 901, limit: 1000 }.full());
        assert!(!Memory { used: 5, limit: 0 }.full(), "no limit read refuses nothing");
    }
}

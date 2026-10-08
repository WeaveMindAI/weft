//! Turning new work away while the worker's memory is nearly full.
//!
//! What one run holds depends on the program: a run that loads a large
//! file, or a node that keeps a big value in memory, can take far more
//! than another. Past its memory the platform kills the whole worker, and
//! every run on it dies with it. So a worker refuses to start more work
//! while its memory is past [`FULL_ABOVE`] of its limit, answering `503`
//! with a `Retry-After`: an outside caller is told to try again in a
//! moment, and a run weft hands it is handed again on the next delivery.
//! Runs already going are never touched. Everything else that fills up
//! behind a busy worker makes new work wait instead (the writer lanes'
//! byte budget, `crate::journal_writer`): waiting does not free memory a
//! run already holds, which is why this one guard refuses.
//!
//! The limit is the container's own (its cgroup's `memory.max`); a
//! container whose cgroup sets none (a local install caps no container)
//! and one that shows no cgroup at all (a sandbox) read the machine's own
//! from `/proc/meminfo`. The cgroup counts memory the process holds
//! whether or not it is in use, which is honest because the worker's
//! allocator hands freed memory back to the system (the generated worker
//! binary runs on jemalloc). The reading is taken by a sampler every
//! [`SAMPLE_EVERY`], never on a request's path.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// How full the memory may be before new work is turned away.
const FULL_ABOVE: f64 = 0.90;

/// What a refused caller is told to wait before asking again.
const RETRY_AFTER_SECS: u64 = 1;

/// How often the sampler reads the memory.
const SAMPLE_EVERY: std::time::Duration = std::time::Duration::from_millis(100);

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

/// The container's memory as its cgroup (v2, then v1) counts it, or the
/// machine's own where the cgroup sets no limit or there is none (a
/// sandbox, whose machine is its own); `None` when it cannot be read.
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
                        target: "weft_engine::busy",
                        "this container's memory cgroup cannot be read, so a worker nearly out of memory does not turn new work away"
                    )
                });
                return None;
            }
            match from_cgroup(current, max) {
                Some(memory) => return Some(memory),
                // No limit on the container: the machine's is the one.
                None => break,
            }
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

/// Whether the worker's memory is nearly full, as its sampler last read
/// it ([`MemoryGuard::sample`]).
#[derive(Default)]
pub struct MemoryGuard {
    full: std::sync::atomic::AtomicBool,
}

impl MemoryGuard {
    /// A guard whose sampler reads the memory every [`SAMPLE_EVERY`] for as
    /// long as the guard is held.
    pub fn sampling() -> Arc<Self> {
        let guard = Arc::new(Self::default());
        let held = Arc::downgrade(&guard);
        tokio::spawn(async move {
            let mut every = tokio::time::interval(SAMPLE_EVERY);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                every.tick().await;
                let Some(guard) = held.upgrade() else { return };
                guard.sample(read());
            }
        });
        guard
    }

    /// Take a reading, saying the change once each way rather than per
    /// refused request.
    fn sample(&self, reading: Option<Memory>) {
        let full = reading.filter(Memory::full);
        let was = self.full.swap(full.is_some(), std::sync::atomic::Ordering::Relaxed);
        match (was, full) {
            (false, Some(memory)) => tracing::warn!(
                target: "weft_engine::busy",
                used = memory.used, limit = memory.limit,
                "the worker's memory is nearly full; new work is turned away until runs end"
            ),
            (true, None) => tracing::info!(target: "weft_engine::busy", "the worker's memory has room again; new work is taken again"),
            _ => {}
        }
    }

    fn is_full(&self) -> bool {
        self.full.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Middleware: refuse a request that would start work while the worker's
/// memory is nearly full (see the module doc). Every request a worker
/// serves but its health check starts work: a run weft hands it, or a
/// caller, which starts its run. The health check always answers.
pub async fn refuse_when_busy(State(guard): State<Arc<MemoryGuard>>, request: Request, next: Next) -> Response {
    if request.uri().path() != "/_weft/healthz" && guard.is_full() {
        return busy();
    }
    next.run(request).await
}

/// What a request turned away is answered: here for memory, at the door
/// for a worker taking all the runs it may (`crate::door::permits`).
pub(crate) fn busy() -> Response {
    let mut answer = (StatusCode::SERVICE_UNAVAILABLE, "This worker is busy right now; try again in a moment.").into_response();
    answer.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECS));
    answer
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt as _;

    /// A worker whose memory is nearly full turns new work away, and its
    /// health check still answers; once it has room, it takes work again.
    #[tokio::test]
    async fn work_is_turned_away_while_the_memory_is_nearly_full() {
        let guard = Arc::new(MemoryGuard::default());
        guard.sample(Some(Memory { used: 950, limit: 1000 }));
        let app = axum::Router::new()
            .route("/_weft/fire", axum::routing::post(|| async { "started" }))
            .route("/_weft/healthz", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(guard.clone(), refuse_when_busy));
        let call = |method: &str, path: &str| {
            axum::http::Request::builder().method(method).uri(path).body(axum::body::Body::empty()).unwrap()
        };
        let refused = app.clone().oneshot(call("POST", "/_weft/fire")).await.unwrap();
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused.headers()[header::RETRY_AFTER], RETRY_AFTER_SECS.to_string().as_str());
        let health = app.clone().oneshot(call("GET", "/_weft/healthz")).await.unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        guard.sample(Some(Memory { used: 100, limit: 1000 }));
        assert_eq!(app.oneshot(call("POST", "/_weft/fire")).await.unwrap().status(), StatusCode::OK);
    }

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

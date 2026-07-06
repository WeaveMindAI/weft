//! Memory-pressure abstraction: how close this pod is to its memory
//! limit, as a fraction in `[0.0, 1.0]`.
//!
//! Both pooled pods (listener, infra-supervisor) decide saturation from
//! REAL memory pressure rather than a work-item count, because a count
//! is a dishonest proxy for load (5 always-connected sockets are not 500
//! idle timers; a project mid-heavy-apply is not an idle one). Pressure
//! is `current_usage / limit` read from the pod's cgroup: the real "how
//! close am I to being OOM-killed" number, which is exactly what should
//! trigger spilling work to another pod.
//!
//! Production reads the cgroup (v2 first, then v1). When there is no
//! memory limit (a typical local dev machine, no cgroup cap), pressure
//! reads as `0.0` and the pod never saturates on memory alone, which is
//! the correct local behavior (one pod until the machine itself is
//! genuinely squeezed) with NO local special-case in the logic: it falls
//! out of "no limit means no pressure."
//!
//! Tests use `FakeMemPressure`, a settable fraction, so the saturation
//! decision is exercised deterministically without touching the cgroup.

use std::sync::Arc;

/// The memory-pressure fraction at or above which a pod reports itself
/// saturated and the dispatcher stops placing new work on it. Below
/// 100% on purpose: a pod calls itself full with headroom to spare so
/// it is not already in trouble when it sheds load. Shared by BOTH
/// pooled pods (listener + supervisor) so their saturation thresholds
/// cannot drift. An honest default; a deployment may tune it.
pub const SATURATION_MEM_FRACTION: f64 = 0.75;

/// Pure saturation decision: is `fraction` at or above `threshold`?
/// Extracted so the decision is layer-1 testable without reading a real
/// cgroup. NaN (a malformed read) is treated as NOT saturated so a
/// transient read glitch never wedges placement; the production reader
/// returns 0.0 (not NaN) on any failure, so NaN should not occur.
pub fn is_saturated(fraction: f64, threshold: f64) -> bool {
    fraction >= threshold
}

/// One pod's load for scale-down planning: its name + its memory
/// pressure. Shared by both pooled pools (listener, supervisor) so the
/// consolidation math is identical for both.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolPodLoad {
    pub pod_name: String,
    pub mem_pressure: f64,
}

/// Pure scale-down decision over MEMORY pressure: pick a pod to drain,
/// or `None`. Shared by the listener and supervisor pools so their
/// consolidation logic cannot diverge from their (memory-based)
/// saturation logic.
///
/// A pod is a drain candidate only when the OTHER live pods have enough
/// combined memory headroom (`threshold - mem_pressure` each, floored at
/// 0) to absorb the target's pressure, so re-placing its work never
/// pushes a survivor over the saturation threshold. Among candidates,
/// drain the LEAST-pressured (cheapest, least likely to re-saturate a
/// survivor). Requires >= 2 live pods (never drain the last one).
///
/// The headroom check is a CONSERVATIVE estimate: memory does not
/// transfer 1:1 when work moves between pods (a process's RSS does not
/// shrink instantly, and the new pod's footprint for the same work
/// differs), so this assumes the target's full pressure lands on the
/// survivors. A slightly-wrong estimate is self-correcting and never
/// overloads anyone: re-placement goes through the normal saturated-aware
/// path, so a survivor that actually crosses the threshold mid-drain
/// refuses (503) and the work lands elsewhere or spawns a pod.
pub fn plan_memory_scaledown(pods: &[PoolPodLoad], threshold: f64) -> Option<String> {
    if pods.len() < 2 {
        return None;
    }
    let mut best: Option<&PoolPodLoad> = None;
    for target in pods {
        let survivor_headroom: f64 = pods
            .iter()
            .filter(|p| p.pod_name != target.pod_name)
            .map(|p| (threshold - p.mem_pressure).max(0.0))
            .sum();
        if target.mem_pressure <= survivor_headroom
            && best.as_ref().map_or(true, |b| target.mem_pressure < b.mem_pressure)
        {
            best = Some(target);
        }
    }
    best.map(|p| p.pod_name.clone())
}

/// Reads this pod's current memory pressure as a fraction in
/// `[0.0, 1.0]`. `Send + Sync` so it can live behind an `Arc` on shared
/// state; cheap to call (a couple of small file reads).
pub trait MemPressure: Send + Sync + 'static {
    /// Current memory usage / limit, clamped to `[0.0, 1.0]`. Returns
    /// `0.0` when there is no limit (uncapped, e.g. local dev) or on any
    /// read failure (fail-open: a read glitch must not make a pod look
    /// saturated and shed all its work).
    fn fraction(&self) -> f64;
}

/// Production reader: the pod's own cgroup memory accounting.
#[derive(Default, Clone)]
pub struct CgroupMemPressure;

impl CgroupMemPressure {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }

    /// cgroup v2: `/sys/fs/cgroup/memory.current` over
    /// `/sys/fs/cgroup/memory.max`. `memory.max` is the literal string
    /// `max` when unlimited. Returns `None` if the v2 files aren't
    /// present (then the caller tries v1).
    fn read_v2() -> Option<f64> {
        let current = read_u64("/sys/fs/cgroup/memory.current")?;
        let max_raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
        let max_raw = max_raw.trim();
        if max_raw == "max" {
            // Uncapped: no pressure.
            return Some(0.0);
        }
        let max: u64 = max_raw.parse().ok()?;
        Some(ratio(current, max))
    }

    /// cgroup v1: `memory.usage_in_bytes` over `memory.limit_in_bytes`.
    /// The v1 "unlimited" sentinel is a huge near-`u64::MAX` value, not a
    /// string; treat an implausibly large limit as uncapped.
    fn read_v1() -> Option<f64> {
        let usage = read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes")?;
        let limit = read_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;
        // v1 unlimited is a sentinel near u64::MAX (e.g.
        // 0x7FFFFFFFFFFFF000). Anything in the exabyte range is "no real
        // limit"; treat as uncapped rather than dividing by it.
        if limit >= (1u64 << 62) {
            return Some(0.0);
        }
        Some(ratio(usage, limit))
    }
}

/// Warn at most once per process when the cgroup looks present but
/// unreadable (a genuinely-broken prod pod), so the fail-open-to-0.0
/// behavior is legible instead of silent.
static CGROUP_UNREADABLE_WARNED: std::sync::Once = std::sync::Once::new();

impl MemPressure for CgroupMemPressure {
    fn fraction(&self) -> f64 {
        // v2 first (current kernels), then v1. Any failure (no cgroup,
        // unreadable, parse error) falls through to 0.0: fail-open, a
        // read glitch must never make the pod look saturated.
        if let Some(f) = Self::read_v2().or_else(Self::read_v1) {
            return f;
        }
        // Both readers failed. Two legitimate-looking cases collapse to
        // 0.0 here: (a) NO cgroup filesystem at all (local / non-
        // containerized, the expected case, stays silent), and (b) a
        // cgroup root EXISTS but its memory files are unreadable (a
        // containerized pod whose accounting we cannot read: a real
        // misconfiguration). We keep fail-open in both, but (b) means the
        // dispatcher will see 0 pressure forever and never scale this
        // pod's project up, so make it legible with a one-shot WARN.
        if std::path::Path::new("/sys/fs/cgroup").exists() {
            CGROUP_UNREADABLE_WARNED.call_once(|| {
                tracing::warn!(
                    target: "weft_platform_traits::mem_pressure",
                    "cgroup present but memory accounting unreadable; reporting 0.0 \
                     pressure (fail-open). Memory-based autoscale is effectively \
                     disabled for this pod until the cgroup is readable."
                );
            });
        }
        0.0
    }
}

fn read_u64(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn ratio(num: u64, den: u64) -> f64 {
    if den == 0 {
        return 0.0;
    }
    (num as f64 / den as f64).clamp(0.0, 1.0)
}

// ---------- fake ----------

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::MemPressure;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Settable memory pressure for tests. Stores the fraction as bits in
    /// an atomic so it is `Send + Sync` without a lock; `set` updates it.
    pub struct FakeMemPressure {
        bits: AtomicU64,
    }

    impl FakeMemPressure {
        pub fn new(fraction: f64) -> Arc<Self> {
            Arc::new(Self {
                bits: AtomicU64::new(fraction.to_bits()),
            })
        }

        /// Update the reported fraction (e.g. drive a pod over the
        /// saturation threshold mid-test).
        pub fn set(&self, fraction: f64) {
            self.bits.store(fraction.to_bits(), Ordering::Relaxed);
        }
    }

    impl MemPressure for FakeMemPressure {
        fn fraction(&self) -> f64 {
            f64::from_bits(self.bits.load(Ordering::Relaxed))
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeMemPressure;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturated_at_or_above_threshold() {
        assert!(!is_saturated(0.0, 0.75));
        assert!(!is_saturated(0.74, 0.75));
        assert!(is_saturated(0.75, 0.75));
        assert!(is_saturated(0.9, 0.75));
        assert!(is_saturated(1.0, 0.75));
    }

    #[test]
    fn nan_is_not_saturated() {
        assert!(!is_saturated(f64::NAN, 0.75));
    }

    #[test]
    fn ratio_clamps_and_guards_zero_denominator() {
        assert_eq!(ratio(0, 0), 0.0);
        assert_eq!(ratio(5, 0), 0.0);
        assert_eq!(ratio(50, 100), 0.5);
        // Over-limit (usage > limit, possible transiently) clamps to 1.0.
        assert_eq!(ratio(150, 100), 1.0);
    }

    #[test]
    fn fake_reports_and_updates() {
        let f = FakeMemPressure::new(0.3);
        assert_eq!(f.fraction(), 0.3);
        f.set(0.8);
        assert_eq!(f.fraction(), 0.8);
    }

    fn load(name: &str, p: f64) -> PoolPodLoad {
        PoolPodLoad {
            pod_name: name.into(),
            mem_pressure: p,
        }
    }

    #[test]
    fn scaledown_never_drains_the_last_pod() {
        assert_eq!(plan_memory_scaledown(&[load("a", 0.1)], 0.75), None);
        assert_eq!(plan_memory_scaledown(&[], 0.75), None);
    }

    #[test]
    fn scaledown_drains_least_pressured_when_headroom_fits() {
        // a=0.2, b=0.1; either fits on the other (headroom ~0.65). b is
        // less pressured so it is the cheaper, chosen target.
        let pods = [load("a", 0.2), load("b", 0.1)];
        assert_eq!(plan_memory_scaledown(&pods, 0.75), Some("b".into()));
    }

    #[test]
    fn scaledown_refuses_when_nothing_fits() {
        // Two pods each at 0.6, threshold 0.75: a survivor offers only
        // 0.15 headroom, far below the 0.6 a drain would move. No drain.
        let pods = [load("a", 0.6), load("b", 0.6)];
        assert_eq!(plan_memory_scaledown(&pods, 0.75), None);
    }

    #[test]
    fn scaledown_saturated_survivor_offers_no_headroom() {
        // c is over threshold (no headroom). a (0.1) can still move onto
        // b (headroom 0.65); a is the least-pressured candidate.
        let pods = [load("a", 0.1), load("b", 0.1), load("c", 0.9)];
        assert_eq!(plan_memory_scaledown(&pods, 0.75), Some("a".into()));
    }
}

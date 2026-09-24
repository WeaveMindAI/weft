//! How fast an install's own clocks run.
//!
//! Every protocol timer the runtime keeps for itself (a heartbeat, the
//! silence after which a pod counts as dead, a lease, a reaper's tick, a
//! scale-down's grace) is read through [`scaled`], so one factor speeds
//! all of them up together. Scaling them together keeps every ratio the
//! protocol leans on (a heartbeat always beats several times inside its
//! stale window, a lease always outlives its renewal), which is what
//! makes a fast install the same system as a normal one, only quicker.
//!
//! The factor comes from `WEFT_TIME_SCALE`, set on every process of one
//! install: the daemon puts it on the dispatcher, and the dispatcher puts
//! it on every pod it spawns, because a worker's heartbeat and the
//! dispatcher's stale check must agree on it. Unset means `1`, real time.
//! A test cell sets something like `0.05` so a sixty-second scale-down
//! tick comes round in three seconds.
//!
//! Two kinds of duration are never scaled. Durations a PERSON chose (a
//! project's grace period, a wait node's timeout, a poll interval in a
//! trigger's config) are the program's behaviour, not the runtime's
//! housekeeping. And budgets for REAL work (a new pod's spawn grace, a
//! boot deadline, a claim held while a verb runs) wait for something that
//! takes as long in a fast install as in any other.

use std::sync::OnceLock;
use std::time::Duration;

/// The variable every process of an install reads its factor from.
// SYNC: TIME_SCALE_ENV <-> crates/weft-cli/src/commands/daemon.rs (the
//       dispatcher manifest's substitution), deploy/k8s/dispatcher.yaml,
//       deploy/k8s/broker.yaml
pub const TIME_SCALE_ENV: &str = "WEFT_TIME_SCALE";

/// This process's factor: `1` unless [`TIME_SCALE_ENV`] says otherwise.
///
/// Read once. A value that is not a positive number stops the process
/// with a message naming the variable, at the first timer that asks,
/// which every binary makes happen at startup through [`announce`]: a
/// typo must never quietly become real time in one pod and fast time in
/// the next.
pub fn factor() -> f64 {
    static FACTOR: OnceLock<f64> = OnceLock::new();
    *FACTOR.get_or_init(|| match parse(std::env::var(TIME_SCALE_ENV).ok().as_deref()) {
        Ok(f) => f,
        Err(e) => panic!("{e}"),
    })
}

/// Read this process's factor at startup and say so when it is not
/// real time. Every binary calls it right after its logging is up, so a
/// malformed value stops the process at boot rather than at its first
/// timer, and a fast install is never mistaken for a normal one in its
/// logs.
#[cfg(feature = "runtime")]
pub fn announce() {
    let factor = factor();
    if factor != 1.0 {
        tracing::warn!(
            factor,
            "this install's own timers run at {factor} times real time ({TIME_SCALE_ENV})"
        );
    }
}

/// The factor a raw `WEFT_TIME_SCALE` value names. Pure, so the rule is
/// tested without touching the process environment.
pub fn parse(raw: Option<&str>) -> Result<f64, String> {
    let Some(raw) = raw.map(str::trim).filter(|r| !r.is_empty()) else {
        return Ok(1.0);
    };
    match raw.parse::<f64>() {
        Ok(f) if f.is_finite() && f > 0.0 => Ok(f),
        _ => Err(format!(
            "{TIME_SCALE_ENV} is '{raw}', which is not a positive number. It is the factor \
             this install's own timers run at: 1 is real time, 0.05 runs them twenty \
             times faster. Unset it for real time."
        )),
    }
}

/// `real` at this process's pace.
pub fn scaled(real: Duration) -> Duration {
    scaled_by(real, factor())
}

/// `real_secs` at this process's pace, for the timers kept as unix
/// seconds (lease expiries, stale cutoffs compared in SQL). Never below
/// one second: a zero-length lease or grace would expire as it is
/// written.
pub fn scaled_secs(real_secs: i64) -> i64 {
    scaled_secs_by(real_secs, factor())
}

/// [`scaled`] with an explicit factor: the pure rule.
pub fn scaled_by(real: Duration, factor: f64) -> Duration {
    real.mul_f64(factor)
}

/// [`scaled_secs`] with an explicit factor: the pure rule.
pub fn scaled_secs_by(real_secs: i64, factor: f64) -> i64 {
    ((real_secs as f64 * factor).round() as i64).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_or_blank_is_real_time() {
        assert_eq!(parse(None), Ok(1.0));
        assert_eq!(parse(Some("  ")), Ok(1.0));
    }

    #[test]
    fn a_positive_number_is_the_factor() {
        assert_eq!(parse(Some("0.05")), Ok(0.05));
        assert_eq!(parse(Some("2")), Ok(2.0));
    }

    #[test]
    fn anything_else_is_refused_naming_the_variable() {
        for bad in ["0", "-1", "fast", "NaN", "inf"] {
            let err = parse(Some(bad)).unwrap_err();
            assert!(err.contains(TIME_SCALE_ENV) && err.contains(bad), "{err}");
        }
    }

    #[test]
    fn seconds_scale_and_never_reach_zero() {
        assert_eq!(scaled_secs_by(60, 0.05), 3);
        assert_eq!(scaled_secs_by(10, 0.01), 1);
        assert_eq!(scaled_secs_by(30, 1.0), 30);
    }

    #[test]
    fn durations_scale_below_a_second() {
        assert_eq!(scaled_by(Duration::from_secs(10), 0.05), Duration::from_millis(500));
    }
}

//! General wait policy: how a durable wait (`ctx.await_signal`, and the
//! same machinery the bus and future constructs reuse) behaves when it
//! cannot resolve immediately.
//!
//! Two orthogonal facts drive every wait:
//!   - the DECISION: hold the worker warm for a bounded time, or go to a
//!     real durable suspension right away.
//!   - the HOLD TIME: when the decision is "hold", how long before giving
//!     up. Never infinite.
//!
//! Both resolve through a three-level chain, innermost wins:
//!   1. the language default ([`LANGUAGE_DEFAULT_HOLD_SECS`], and the
//!      decision implied by the construct's [`SuspendPolicy`]),
//!   2. the construct/trigger default ([`SuspendPolicy`], embedded by any
//!      trigger of this shape via `#[serde(flatten)]`),
//!   3. the per-call override ([`WaitOverride`], an argument on the
//!      individual `await`).
//!
//! This module is deliberately NOT caller-specific: a live caller trigger
//! seeds the defaults, but the bus and any future trigger reuse the exact
//! same types and resolution. The caller machinery only consults the
//! resolved [`WaitPolicy`].

use serde::{Deserialize, Serialize};

/// Language-wide default hold time, the floor of the resolution chain. A
/// construct default or a per-call override replaces it; nothing makes a
/// hold infinite.
pub const LANGUAGE_DEFAULT_HOLD_SECS: u64 = 60;

/// The construct/trigger-level suspension defaults. A reusable block any
/// trigger of a caller-like shape embeds (flattened) so it does not
/// re-define how its waits behave. Holds exactly the two facts a trigger
/// needs to seed: whether the run may be suspended at all, and the default
/// hold time for waits that hold.
///
/// Each field carries its own `#[serde(default)]` so the block
/// deserializes even when embedded with `#[serde(flatten)]` and entirely
/// absent from the wire (a default on a flattened CONTAINER is ignored by
/// serde, so the defaults must sit on the fields). The struct's `Default`
/// is the safe general default for a caller-like trigger: NOT suspendable
/// (`can_suspend = false`), so a trigger that forgets to set it does not
/// silently let an interactive run become a background job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspendPolicy {
    /// Whether the execution may go to a real durable suspension (park the
    /// worker, resume later, outliving anything attached). `false` = the
    /// run cannot be suspended: a wait holds the worker for a bounded time
    /// and, if it cannot make progress (hold elapses, or a per-call
    /// override insists on suspending), the program is killed at the
    /// suspension point. `true` = a wait defaults to a real suspension.
    #[serde(default)]
    pub can_suspend: bool,
    /// Default bound for a wait whose resolved decision is "hold". Middle
    /// of the resolution chain (overrides the language default; a per-call
    /// override beats it). Never infinite.
    #[serde(default = "default_hold_secs_field")]
    pub default_hold_secs: u64,
}

fn default_hold_secs_field() -> u64 {
    LANGUAGE_DEFAULT_HOLD_SECS
}

impl Default for SuspendPolicy {
    fn default() -> Self {
        // Safe general default: NOT suspendable. A trigger that wants to
        // outlive its caller opts in with `can_suspend = true`.
        Self {
            can_suspend: false,
            default_hold_secs: LANGUAGE_DEFAULT_HOLD_SECS,
        }
    }
}

/// What a wait does when it cannot resolve immediately. The default for a
/// run is implied by its [`SuspendPolicy`] (`can_suspend = true` ->
/// `Suspend`, `false` -> `Hold`); a per-call override sets it explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitDecision {
    /// Hold the worker warm for the resolved hold time, waiting in-process
    /// for the signal. On expiry, what happens is governed by the run's
    /// `can_suspend` (suspendable -> fall through to a real suspension;
    /// not -> kill the program).
    Hold,
    /// Go to a real durable suspension immediately (do not hold). In a
    /// non-suspendable run this is the contradiction surfaced loud at the
    /// suspension point (not at setup, since an override is invisible
    /// there).
    Suspend,
}

/// Per-`await` override of the wait policy. Every field optional: an
/// absent field inherits from the construct default / language default.
/// The innermost level of the resolution chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WaitOverride {
    /// Override the decision (hold vs suspend) for this one wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<WaitDecision>,
    /// Override the hold time for this one wait (only meaningful when the
    /// resolved decision is `Hold`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_secs: Option<u64>,
}

/// The fully-resolved policy for one wait, after applying the chain. This
/// is what the engine acts on; it never re-reads the inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitPolicy {
    pub decision: WaitDecision,
    /// The hold bound (only consulted when `decision == Hold`).
    pub hold_secs: u64,
    /// Carried through so the suspension point knows what to do when a
    /// `Hold` expires or a `Suspend` is reached: a non-suspendable run
    /// kills the program rather than parking durably.
    pub can_suspend: bool,
}

/// Resolve a wait's effective policy from the chain: construct
/// [`SuspendPolicy`] (with the language default already folded into its
/// `default_hold_secs`) plus an optional per-call [`WaitOverride`].
///
/// - decision: the override wins; else the construct's `can_suspend`
///   implies it (`true` -> `Suspend`, `false` -> `Hold`).
/// - hold_secs: the override wins; else the construct default.
pub fn resolve_wait_policy(policy: SuspendPolicy, override_: Option<WaitOverride>) -> WaitPolicy {
    let ov = override_.unwrap_or_default();
    let decision = ov.decision.unwrap_or(if policy.can_suspend {
        WaitDecision::Suspend
    } else {
        WaitDecision::Hold
    });
    let hold_secs = ov.hold_secs.unwrap_or(policy.default_hold_secs);
    WaitPolicy {
        decision,
        hold_secs,
        can_suspend: policy.can_suspend,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tied() -> SuspendPolicy {
        SuspendPolicy { can_suspend: false, default_hold_secs: 120 }
    }
    fn survives() -> SuspendPolicy {
        SuspendPolicy { can_suspend: true, default_hold_secs: 120 }
    }

    #[test]
    fn non_suspendable_run_defaults_to_hold_with_construct_time() {
        let p = resolve_wait_policy(tied(), None);
        assert_eq!(p.decision, WaitDecision::Hold);
        assert_eq!(p.hold_secs, 120);
        assert!(!p.can_suspend);
    }

    #[test]
    fn suspendable_run_defaults_to_suspend() {
        let p = resolve_wait_policy(survives(), None);
        assert_eq!(p.decision, WaitDecision::Suspend);
        assert!(p.can_suspend);
    }

    #[test]
    fn per_call_override_beats_construct_default() {
        // A wait in a non-suspendable run insists on a real suspend: the
        // override wins here; the contradiction is surfaced LATER, at the
        // suspension point, never at resolution time.
        let p = resolve_wait_policy(
            tied(),
            Some(WaitOverride { decision: Some(WaitDecision::Suspend), hold_secs: None }),
        );
        assert_eq!(p.decision, WaitDecision::Suspend);
        assert!(!p.can_suspend, "the run is still non-suspendable; the kill happens at suspend");

        // A wait overrides only the hold time, keeping the inherited decision.
        let p2 = resolve_wait_policy(
            tied(),
            Some(WaitOverride { decision: None, hold_secs: Some(5) }),
        );
        assert_eq!(p2.decision, WaitDecision::Hold);
        assert_eq!(p2.hold_secs, 5);
    }

    #[test]
    fn language_default_used_when_construct_inherits_it() {
        // SuspendPolicy::default() is the safe default: NOT suspendable,
        // language-default hold. So an inherited wait holds (not suspends).
        let p = resolve_wait_policy(SuspendPolicy::default(), None);
        assert_eq!(p.hold_secs, LANGUAGE_DEFAULT_HOLD_SECS);
        assert_eq!(p.decision, WaitDecision::Hold);
        assert!(!p.can_suspend);
    }
}

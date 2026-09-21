//! What to do with in-flight Fire-phase executions of a targeted
//! project when an operation needs the running set out of the way:
//! a lifecycle verb (stop / terminate / deactivate) on the
//! broker/dispatcher side, the worker replacement inside an activate,
//! or the CLI's stale-binary build gate. One definition for every
//! crate; the serde derives are the single source of truth for the
//! wire spelling (TEXT column, JSON field, and CLI flag value all
//! agree). The whole vocabulary lives here so the CLI (which speaks
//! only weft-core) builds the same typed shapes the dispatcher reads.

use serde::{Deserialize, Serialize};

/// Default cap on a `RunningPolicy::Wait` drain before the lifecycle
/// op proceeds anyway (with a loud warning). A parameter everywhere it
/// applies, and this is only what it means when the request does not
/// say: the supervisor's stop/terminate drain, the dispatcher's
/// worker-replacement drain, and the trigger-side deactivate wait
/// (`DeactivateSpec::drain_timeout_secs`, reachable as
/// `--drain-timeout` and as the graph picker's "wait at most" box).
///
/// A cap rather than an open-ended wait because the person doing this
/// is answering "how long am I willing to hold", and past it the
/// remaining executions are cancelled and the op lands, so a
/// deactivate is never stuck behind one long run nobody is watching.
/// A minute, because a program's executions usually land in seconds
/// and a wait nobody chose is the thing that reads as a hang; anyone
/// with a longer run to protect passes `--drain-timeout`. (The
/// `infra_lifecycle_command` column carries the same number as its DDL
/// default; no insert relies on it, every command row is written with
/// the cap the request resolved to, and moving it there takes a
/// migration.)
// SYNC: DEFAULT_DRAIN_TIMEOUT_SECS <-> packages/weft-graph/src/protocol.ts DEFAULT_DRAIN_TIMEOUT_SECS
pub const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 60;

/// `DEFAULT_DRAIN_TIMEOUT_SECS` as a serde default: what a wire shape
/// with a drain cap reads when the request leaves it out.
pub fn default_drain_timeout_secs() -> u64 {
    DEFAULT_DRAIN_TIMEOUT_SECS
}

// SYNC: RunningPolicy <-> packages/weft-graph/src/protocol.ts DeactivationSpec.runningPolicy,
// packages/weft-graph/src/webview/lib/components/project/DeactivationPicker.svelte (the
// picker opens on the `#[default]` variant)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RunningPolicy {
    /// Cancel running execs immediately, then proceed. The default
    /// wherever a request does not say: nothing waits on somebody's
    /// running work unless they asked it to.
    #[default]
    Cancel,
    /// Wait until running_count reaches 0, then proceed. New fires are
    /// gated per the project's lifecycle axes (set by the
    /// trigger-deactivate / park step).
    Wait,
}

impl RunningPolicy {
    /// Wire string. Must match the serde rename above; the round-trip
    /// test below pins them together.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Wait => "wait",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "cancel" => Some(Self::Cancel),
            "wait" => Some(Self::Wait),
            _ => None,
        }
    }

    pub const VARIANTS: &'static [Self] = &[Self::Cancel, Self::Wait];
}


impl std::fmt::Display for RunningPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The running-work answer as a request carries it: the policy and
/// the cap that only ever bounds a wait, both optional because a
/// request may leave them to the default, or to a picker's answer
/// that outranks them. Flattened into every request that asks the
/// question (activate, bake, infra sync/stop/terminate, the per-node
/// infra verbs), so the wire spells it one way and every handler
/// resolves it through [`Self::resolve`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningChoice {
    #[serde(default, rename = "runningPolicy", skip_serializing_if = "Option::is_none")]
    pub running_policy: Option<RunningPolicy>,
    #[serde(default, rename = "drainTimeoutSecs", skip_serializing_if = "Option::is_none")]
    pub drain_timeout_secs: Option<u64>,
}

impl RunningChoice {
    /// The one answer to "what about the running executions" for a
    /// request: the picker's answer when one was given (`asked`, the
    /// trigger-deactivation spec an active project's verb carries),
    /// otherwise what the request said, otherwise the shared default
    /// (cancel, and the default cap). The picker outranks the body
    /// because a person who answered once in the picker did not
    /// answer differently in a flag they never saw.
    pub fn resolve(&self, asked: Option<&DeactivateSpec>) -> (RunningPolicy, u64) {
        let policy = asked
            .map(|spec| spec.running_policy)
            .or(self.running_policy)
            .unwrap_or_default();
        let cap = asked
            .and_then(|spec| spec.drain_timeout_secs)
            .or(self.drain_timeout_secs)
            .unwrap_or(DEFAULT_DRAIN_TIMEOUT_SECS);
        (policy, cap)
    }
}

/// How a `deactivate` command should treat the project's signal
/// table on entry.
// SYNC: DeactivationMode <-> packages/weft-graph/src/protocol.ts DeactivationSpec.mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeactivationMode {
    /// Drop every signal (entries + suspensions) and forget the
    /// project ever ran. Only legal with `RunningPolicy::Cancel`.
    Wipe,
    /// Keep DB signal rows; unregister from listener; arm a deadline
    /// after which the project auto-wipes. Suspended fires are kept
    /// alive on the gate (visible=false, accepting=true on entry
    /// signals only) for `grace_minutes`.
    Hibernate,
    /// Keep DB signal rows; unregister from listener. No deadline,
    /// no eventual wipe. Reactivate fully restores.
    Park,
}

impl DeactivationMode {
    /// Wire string. Must match the serde rename above; the round-trip
    /// test below pins them together.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wipe => "wipe",
            Self::Hibernate => "hibernate",
            Self::Park => "park",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "wipe" => Some(Self::Wipe),
            "hibernate" => Some(Self::Hibernate),
            "park" => Some(Self::Park),
            _ => None,
        }
    }

    pub const VARIANTS: &'static [Self] = &[Self::Wipe, Self::Hibernate, Self::Park];
}

impl std::fmt::Display for DeactivationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Payload for a `verb=deactivate` lifecycle command. Stored on
/// the `infra_lifecycle_command.spec_json` column by the supervisor
/// when a HealthProtocol fires, and deserialized by the dispatcher's
/// `lifecycle_claimer` when it claims the row.
///
/// Also used as the HTTP request body for the dispatcher's
/// `/deactivate` endpoint and as the embedded `triggerDeactivation`
/// field on Sync / Stop / Terminate / Resync. One typed shape, no
/// per-endpoint duplicates: the CLI and the supervisor build this
/// struct, the dispatcher reads it.
///
/// ONE wire spelling: camelCase (`graceMinutes`, `runningPolicy`).
// SYNC: DeactivateSpec <-> packages/weft-graph/src/protocol.ts DeactivationSpec
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeactivateSpec {
    pub mode: DeactivationMode,
    /// Hibernate window in minutes. Only meaningful when
    /// `mode = Hibernate`; ignored otherwise. Default 15 keeps the
    /// wire shape forgiving for clients deactivating with
    /// Park/Wipe (no grace concept).
    #[serde(default = "default_grace_minutes", rename = "graceMinutes")]
    pub grace_minutes: u32,
    #[serde(rename = "runningPolicy")]
    pub running_policy: RunningPolicy,
    /// Cap in seconds on the `wait` drain: how long a deactivation
    /// sits in `Deactivating` before the remaining executions are
    /// cancelled and it lands. The user picks it with the wait choice
    /// (same "wait at most N, then proceed" as the infra drains);
    /// absent = `DEFAULT_DRAIN_TIMEOUT_SECS`. Ignored with
    /// `runningPolicy = cancel` (nothing to wait for).
    #[serde(default, rename = "drainTimeoutSecs", skip_serializing_if = "Option::is_none")]
    pub drain_timeout_secs: Option<u64>,
}

/// The hibernate window a spec gets when the request leaves it out.
pub const DEFAULT_GRACE_MINUTES: u32 = 15;

fn default_grace_minutes() -> u32 {
    DEFAULT_GRACE_MINUTES
}

impl DeactivateSpec {
    /// Verify the (mode, policy) combination is coherent. The
    /// only illegal combo is `Wipe + Wait`: wipe drops every
    /// suspended fire, so waiting for them to drain first is
    /// contradictory. Lives next to the wire type so every caller
    /// (the CLI's prompt, broker handler, dispatcher's /deactivate,
    /// supervisor's enqueue path) shares one validator.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.mode == DeactivationMode::Wipe && self.running_policy == RunningPolicy::Wait {
            return Err(
                "wipe requires runningPolicy=cancel; waiting before wiping is contradictory",
            );
        }
        Ok(())
    }

    /// Whether this deactivation waits for the running executions
    /// before it lands, which ends by cancelling whatever is still
    /// running at the cap.
    ///
    /// The policy decides this and the mode has no say: they are two
    /// separate questions. The mode is about the SUSPENDED fires and
    /// the door (drop them, keep them hidden, keep them and stay
    /// visible); the policy is about what is RUNNING right now. A
    /// person picking `wait` is answering the second question, so
    /// `wait` waits under park exactly as it does under hibernate.
    ///
    /// `wipe` never gets here: [`Self::validate`] refuses it with
    /// `wait` up front, so the only combinations that reach this are
    /// the ones where waiting means something.
    pub fn drains(&self) -> bool {
        self.running_policy == RunningPolicy::Wait
    }

    /// The spec's running-work answer as a request would carry it:
    /// what a verb that deactivates and then activates again hands
    /// the activate, so the person's one answer governs both halves.
    pub fn running_choice(&self) -> RunningChoice {
        RunningChoice { running_policy: Some(self.running_policy), drain_timeout_secs: self.drain_timeout_secs }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_roundtrip() {
        for v in RunningPolicy::VARIANTS {
            assert_eq!(RunningPolicy::parse(v.as_str()), Some(*v));
            let json = serde_json::to_string(v).expect("serialize");
            assert_eq!(json, format!("\"{}\"", v.as_str()));
            let back: RunningPolicy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, *v);
        }
        for v in DeactivationMode::VARIANTS {
            assert_eq!(DeactivationMode::parse(v.as_str()), Some(*v));
            let json = serde_json::to_string(v).expect("serialize");
            assert_eq!(json, format!("\"{}\"", v.as_str()));
            let back: DeactivationMode = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, *v);
        }
    }

    fn picked(policy: RunningPolicy, cap: Option<u64>) -> DeactivateSpec {
        DeactivateSpec { mode: DeactivationMode::Park, grace_minutes: 15, running_policy: policy, drain_timeout_secs: cap }
    }

    /// The person's answer governs the infra side too. Terminate used
    /// to cancel whatever the picker said, so somebody who asked to
    /// wait watched their executions die anyway.
    #[test]
    fn the_picker_beats_the_body() {
        let body = RunningChoice { running_policy: Some(RunningPolicy::Cancel), drain_timeout_secs: None };
        let (policy, _) = body.resolve(Some(&picked(RunningPolicy::Wait, None)));
        assert_eq!(policy, RunningPolicy::Wait, "terminate still waits when asked to");
        let body = RunningChoice { running_policy: Some(RunningPolicy::Wait), drain_timeout_secs: None };
        let (policy, _) = body.resolve(Some(&picked(RunningPolicy::Cancel, None)));
        assert_eq!(policy, RunningPolicy::Cancel, "and stop cancels when asked to");
    }

    /// The picker is only shown for an ACTIVE project: there is nothing
    /// to park or wipe otherwise. With no answer the body decides, and
    /// with no body either the shared default stands: cancel.
    #[test]
    fn with_no_picker_the_body_decides_and_then_the_default() {
        let (policy, cap) = RunningChoice::default().resolve(None);
        assert_eq!(policy, RunningPolicy::Cancel);
        assert_eq!(cap, DEFAULT_DRAIN_TIMEOUT_SECS);
        let body = RunningChoice { running_policy: Some(RunningPolicy::Wait), drain_timeout_secs: Some(900) };
        assert_eq!(body.resolve(None), (RunningPolicy::Wait, 900));
    }

    /// "Wait at most this long" is one answer, not one per tier.
    #[test]
    fn the_cap_comes_from_the_same_answer() {
        let body = RunningChoice { running_policy: None, drain_timeout_secs: Some(900) };
        let (_, cap) = body.resolve(Some(&picked(RunningPolicy::Wait, Some(30))));
        assert_eq!(cap, 30, "the picker's cap, not the body's");
        let (_, cap) = body.resolve(None);
        assert_eq!(cap, 900, "and the body's when there was no picker");
    }

    /// `wipe` is the one pair that is refused rather than answered,
    /// because a wipe drops the suspended fires outright and there is
    /// nothing coherent for a wait to mean beside it.
    #[test]
    fn the_policy_decides_the_wait_whatever_the_mode() {
        let spec = |mode, running_policy| DeactivateSpec {
            mode, grace_minutes: 15, running_policy, drain_timeout_secs: None,
        };
        for mode in [DeactivationMode::Hibernate, DeactivationMode::Park] {
            assert!(spec(mode, RunningPolicy::Wait).drains(), "{mode:?} + wait waits");
            assert!(!spec(mode, RunningPolicy::Cancel).drains(), "{mode:?} + cancel does not");
            assert!(spec(mode, RunningPolicy::Wait).validate().is_ok(), "{mode:?} + wait is legal");
        }
        assert!(!spec(DeactivationMode::Wipe, RunningPolicy::Cancel).drains());
        assert!(spec(DeactivationMode::Wipe, RunningPolicy::Wait).validate().is_err());
    }

    /// A deactivate-then-activate hands the activate the spec's own
    /// answer, never the wire default.
    #[test]
    fn a_spec_hands_its_answer_to_the_activate() {
        let choice = picked(RunningPolicy::Wait, Some(30)).running_choice();
        assert_eq!(choice.resolve(None), (RunningPolicy::Wait, 30));
    }
}

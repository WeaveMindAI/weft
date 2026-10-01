//! The lifecycle state of one infra node copy: what `infra_node.status`
//! stores, what the supervisor writes, and what a program's
//! `ctx.infra(..).status()` reads. One enum for all of them, so a status
//! nobody knows fails to decode instead of reading as some other state.

use serde::{Deserialize, Serialize};

/// Lifecycle-state of an infra node row, written into `infra_node.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InfraNodeStatus {
    /// Mid-apply: the apply task started but hasn't successfully
    /// written `Running` yet.
    Provisioning,
    /// Infra node is up: the supervisor sees its units ready.
    Running,
    /// Stopped by the user: each unit stopped or left running per its
    /// `onStop`, disks kept.
    Stopped,
    /// Supervisor declared the node below its readiness threshold.
    Flaky,
    /// The most recent apply (or post-apply execute) failed.
    /// `failure_stage` carries the structured reason.
    Failed,
    /// Transient: supervisor mid-stop.
    Stopping,
    /// Transient: supervisor mid-terminate. Row is removed on success.
    Terminating,
}

impl InfraNodeStatus {
    /// Every status, for tests that walk them.
    pub const VARIANTS: &'static [Self] = &[
        Self::Provisioning,
        Self::Running,
        Self::Stopped,
        Self::Flaky,
        Self::Failed,
        Self::Stopping,
        Self::Terminating,
    ];

    /// The wire string, as serde writes it (the test below pins them).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Provisioning => "provisioning",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Flaky => "flaky",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
            Self::Terminating => "terminating",
        }
    }

    /// Back from the wire string; `None` for one no status spells.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "provisioning" => Some(Self::Provisioning),
            "running" => Some(Self::Running),
            "stopped" => Some(Self::Stopped),
            "flaky" => Some(Self::Flaky),
            "failed" => Some(Self::Failed),
            "stopping" => Some(Self::Stopping),
            "terminating" => Some(Self::Terminating),
            _ => None,
        }
    }

    /// Statuses where the node is expected to have running units
    /// the health loop should observe. Used by the supervisor's
    /// health tick: a node mid-apply or mid-stop has no SLO; only
    /// `Running` / `Flaky` does.
    pub fn expects_running_units(self) -> bool {
        matches!(self, Self::Running | Self::Flaky)
    }

    /// Statuses an apply can work on in place, keeping the units that
    /// are up. `Terminating` cannot: a terminate stamped it and did not
    /// finish, so the apply finishes that terminate first and starts the
    /// copy fresh. Every other status either has live state to reattach
    /// to (Running/Flaky/Stopped/Stopping) or is mid-failure that
    /// re-apply handles idempotently (Provisioning/Failed).
    pub fn applies_in_place(self) -> bool {
        !matches!(self, Self::Terminating)
    }

    /// Coarse precedence for rolling N per-unit statuses up to one
    /// node-level status. Higher wins. Transient/bad states dominate
    /// healthy ones so the node never looks "running" while a unit is
    /// mid-terminate or failed. Must match the dispatcher's
    /// `infra_rollup` precedence so the two agree.
    pub fn rollup_rank(self) -> u8 {
        match self {
            Self::Terminating => 7,
            Self::Stopping => 6,
            Self::Provisioning => 5,
            Self::Failed => 4,
            Self::Flaky => 3,
            Self::Running => 2,
            Self::Stopped => 1,
        }
    }

    /// Roll a set of per-unit statuses up to one node-level status by
    /// `rollup_rank` (worst-of-units). Empty -> `Stopped` (no units
    /// up is the degenerate "nothing running" case).
    pub fn rollup<'a>(units: impl IntoIterator<Item = &'a InfraNodeStatus>) -> InfraNodeStatus {
        units
            .into_iter()
            .copied()
            .max_by_key(|s| s.rollup_rank())
            .unwrap_or(InfraNodeStatus::Stopped)
    }

    /// The node status a completed apply stamps (`set_applied`), from
    /// the roster it writes: every reconciled unit just came up
    /// `Running`, and the only other status a unit can hold at that
    /// point is `Flaky` (a frozen unit the apply left alone), so the
    /// node is `Flaky` if any unit is and `Running` otherwise. The
    /// general `rollup` would say `Stopped` for a unit-less roster
    /// (a spec of shared resources only), which a successful apply is
    /// not. The broker and the supervisor's test fake both stamp
    /// through this, so the node status agrees with its roster.
    pub fn applied_rollup<'a>(units: impl IntoIterator<Item = &'a InfraNodeStatus>) -> InfraNodeStatus {
        if units.into_iter().any(|s| *s == InfraNodeStatus::Flaky) {
            InfraNodeStatus::Flaky
        } else {
            InfraNodeStatus::Running
        }
    }
}


impl std::fmt::Display for InfraNodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_status_round_trips_through_its_wire_string() {
        for status in InfraNodeStatus::VARIANTS {
            assert_eq!(InfraNodeStatus::parse(status.as_str()), Some(*status));
            assert_eq!(serde_json::to_value(status).unwrap(), serde_json::json!(status.as_str()));
        }
        assert!(serde_json::from_value::<InfraNodeStatus>(serde_json::json!("sleeping")).is_err());
    }
}

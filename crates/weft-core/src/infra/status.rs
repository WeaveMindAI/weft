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
    // SYNC: expects_running_units <-> weft_broker_client::lifecycle_command::runs_infra
    pub fn expects_running_units(self) -> bool {
        matches!(self, Self::Running | Self::Flaky)
    }

    /// Whether a copy in this status answers at its endpoints: running,
    /// or flaky. A flaky copy failed a health check or a few, which says
    /// something may be wrong, never that nothing is there: a lookup that
    /// read it as down would tell a run to `weft infra start` a node that
    /// is up. Every read of where a copy answers (the broker's, the
    /// dispatcher's `/live`, the supervisor asking a node to remove what
    /// another put inside it) goes by this one rule.
    pub fn answers(self) -> bool {
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


/// Why a container of an infra unit stopped on its own (and its host
/// brought it back). What the host sees (`UnitObservation::restart`),
/// what `infra_unit.last_restart_cause` keeps (`as_str`, the exit code in
/// its own column), and what the status answer carries.
// SYNC: RestartCause <-> packages/weft-graph/src/protocol.ts RestartCause
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RestartCause {
    /// It used more memory than it is allowed, and a process in it was
    /// killed for it: the container's own (its host brought it back), or
    /// one process inside a container that kept running (a database
    /// backend, after which the database dropped its connections).
    OutOfMemory,
    /// Its process exited with `code`.
    Exited { code: i64 },
    /// The host no longer remembers why.
    Unknown,
}

impl RestartCause {
    /// Its kind as serde writes it, which `infra_unit.last_restart_cause`
    /// keeps.
    // SYNC: as_str <-> crates/weft-dispatcher/src/infra_node.rs (infra_unit.last_restart_cause's CHECK)
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OutOfMemory => "out_of_memory",
            Self::Exited { .. } => "exited",
            Self::Unknown => "unknown",
        }
    }

    /// The exit code, for the cause that has one.
    pub const fn exit_code(&self) -> Option<i64> {
        match self {
            Self::Exited { code } => Some(*code),
            Self::OutOfMemory | Self::Unknown => None,
        }
    }
}

/// The latest time a container of an infra unit stopped on its own and
/// its host brought it back, since weft last started the unit: when it
/// stopped (unix seconds, the host's clock), which is what tells one
/// restart from the next, and why. The health check writes it onto the
/// unit once, the first time it sees it; the next start of the unit by
/// weft clears it.
// SYNC: LastRestart <-> packages/weft-graph/src/protocol.ts LastRestart
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastRestart {
    pub at_unix: i64,
    pub cause: RestartCause,
}

impl LastRestart {
    /// What a person is told about it, `node` being the node's place as
    /// the program spells it (`db`). The time is UTC, so it reads the same
    /// whatever clock the reader's machine keeps.
    // SYNC: describe <-> packages/weft-graph/src/status.ts describeRestart
    pub fn describe(&self, node: &str) -> String {
        let at = chrono::DateTime::from_timestamp(self.at_unix, 0)
            .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| format!("unix time {}", self.at_unix));
        match &self.cause {
            RestartCause::OutOfMemory => {
                format!("{node} ran out of memory at {at}: a process in it was killed because it needs more memory than its container may use; give it more")
            }
            RestartCause::Exited { code } => format!("{node} stopped (exit code {code}) and was restarted at {at}"),
            RestartCause::Unknown => format!("{node} stopped and was restarted at {at}, for a reason its host no longer knows"),
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

    /// The kind a column keeps is the one serde writes, so the SQL that
    /// builds a cause back from its columns reads as this type.
    #[test]
    fn a_restart_cause_keeps_the_kind_serde_writes() {
        for cause in [RestartCause::OutOfMemory, RestartCause::Exited { code: 3 }, RestartCause::Unknown] {
            assert_eq!(serde_json::to_value(&cause).unwrap()["kind"], serde_json::json!(cause.as_str()));
        }
        let restart = LastRestart { at_unix: 7, cause: RestartCause::Exited { code: 3 } };
        let wire = serde_json::to_value(&restart).unwrap();
        assert_eq!(wire, serde_json::json!({ "atUnix": 7, "cause": { "kind": "exited", "code": 3 } }));
        assert_eq!(serde_json::from_value::<LastRestart>(wire).unwrap(), restart);
    }

    #[test]
    fn a_restart_reads_as_what_happened_and_what_to_do() {
        // 2026-10-27 14:03:20 UTC.
        let at_unix = 1_793_109_800;
        let said = |cause| LastRestart { at_unix, cause }.describe("one.db");
        assert_eq!(
            said(RestartCause::OutOfMemory),
            "one.db ran out of memory at 2026-10-27 14:03 UTC: a process in it was killed because it needs more memory than its container may use; give it more"
        );
        assert_eq!(said(RestartCause::Exited { code: 137 }), "one.db stopped (exit code 137) and was restarted at 2026-10-27 14:03 UTC");
        assert_eq!(
            said(RestartCause::Unknown),
            "one.db stopped and was restarted at 2026-10-27 14:03 UTC, for a reason its host no longer knows"
        );
    }
}

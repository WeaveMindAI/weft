//! What the install's project registry (`/projects`, `/projects/{id}`)
//! reads and answers, and the CLI sends and reads: one Rust type per
//! message, so the two ends cannot drift.

use serde::{Deserialize, Serialize};

/// One project as a list shows it: its shared activations read as one
/// status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub status: String,
}

/// `POST /projects`: the project exists on this install, under the
/// caller's tenant, with nothing built yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareRequest {
    pub id: uuid::Uuid,
    pub name: String,
}

crate::wire_enum! {
    /// Lifecycle status of a project's activation, written into
    /// `project.status` (and each activation's row). The dispatcher
    /// writes it, the supervisor reads it through the broker, and the
    /// status answer carries it.
    /// SYNC: ProjectStatus <-> packages/weft-graph/src/protocol.ts LifecycleStatus
    pub enum ProjectStatus {
        /// Fresh row, never activated.
        Registered = "registered",
        /// Transient: user clicked activate; trigger setup in flight.
        Activating = "activating",
        /// Live; worker pool spawns on demand and fires execute.
        Active = "active",
        /// Transient: user clicked deactivate with runningPolicy=wait;
        /// running executions are draining.
        Deactivating = "deactivating",
        /// Idle; gate refuses / parks fires per the lifecycle axes.
        Inactive = "inactive",
    }
}

crate::wire_enum! {
    /// The build transition axis of a project, orthogonal to its
    /// [`ProjectStatus`]: `Building` while a verb's image build is in
    /// flight, `CancellingBuild` after the person asked to cancel and
    /// before the driving process lands it back at `None`. While not
    /// `None`, the only verb offered is `cancel_build`.
    /// SYNC: ProjectTransition <-> packages/weft-graph/src/protocol.ts ProjectTransition,
    ///       packages/weft-graph/src/status.ts VALID_TRANSITIONS
    pub enum ProjectTransition {
        None = "none",
        Building = "building",
        CancellingBuild = "cancelling_build",
    }
}

impl ProjectTransition {
    /// True while a build transition is in flight (either phase).
    pub fn is_building(self) -> bool {
        matches!(self, Self::Building | Self::CancellingBuild)
    }

    /// What a verb refused because of this transition tells the person:
    /// `None` when there is no build to wait on.
    pub fn refusal(self) -> Option<&'static str> {
        match self {
            Self::Building => Some("the project is building; wait for the build to finish, or cancel it with `weft cancel-build`"),
            Self::CancellingBuild => Some("the project's build is being cancelled; try again once it has stopped"),
            Self::None => None,
        }
    }
}

/// `GET /projects/{id}/status`'s query: the hashes the client computed
/// for its current source, each compared with what the project runs to
/// set one drift bit. A hash left out leaves its bit unset.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusQuery {
    /// Compared against the running binary hash: "the worker image
    /// needs rebuilding".
    #[serde(default, rename = "desiredBinaryHash", skip_serializing_if = "Option::is_none")]
    pub desired_binary_hash: Option<String>,
    /// The same build's binary hash with the whole catalog compiled in
    /// (`weft run --full`). A running image built either way is
    /// current: drift means the running hash matches neither.
    #[serde(default, rename = "desiredFullBinaryHash", skip_serializing_if = "Option::is_none")]
    pub desired_full_binary_hash: Option<String>,
    /// Compared against the running definition hash: "the project shape
    /// has changed" (a config or topology edit not resynced yet).
    #[serde(default, rename = "desiredDefinitionHash", skip_serializing_if = "Option::is_none")]
    pub desired_definition_hash: Option<String>,
    /// Compared against the running infra hash: the upgrade signal.
    #[serde(default, rename = "desiredInfraHash", skip_serializing_if = "Option::is_none")]
    pub desired_infra_hash: Option<String>,
}

impl StatusQuery {
    /// The query string, `?` included; empty when no hash is named.
    pub fn to_query_string(&self) -> String {
        let pairs = [
            ("desiredBinaryHash", &self.desired_binary_hash),
            ("desiredFullBinaryHash", &self.desired_full_binary_hash),
            ("desiredDefinitionHash", &self.desired_definition_hash),
            ("desiredInfraHash", &self.desired_infra_hash),
        ];
        let named: Vec<String> =
            pairs.iter().filter_map(|(key, value)| value.as_ref().map(|v| format!("{key}={v}"))).collect();
        if named.is_empty() {
            String::new()
        } else {
            format!("?{}", named.join("&"))
        }
    }
}

/// `GET /projects/{id}/status`: everything a client renders about one
/// project in one answer (registration, listener, infra, executions,
/// drift, and the verbs the dispatcher accepts right now).
// SYNC: ProjectStatusResponse <-> packages/weft-graph/src/status.ts RawStatusPayload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectStatusResponse {
    pub id: uuid::Uuid,
    pub name: String,
    /// The aggregate status over the shared activations.
    pub status: ProjectStatus,
    /// The build transition axis, orthogonal to `status`.
    pub transition: ProjectTransition,
    /// The person-facing mode derived from the lifecycle axes; the
    /// action bar reads it verbatim.
    pub mode: crate::activation::ActivationMode,
    /// Unix-second deadline after which fires are refused (hibernate's
    /// grace window). `None` outside hibernate. Surfaced so the action
    /// bar can render a countdown.
    pub fires_deadline_unix: Option<i64>,
    /// Count of running, non-suspended executions right now. Drives the
    /// deactivating-state UI: progress towards drain.
    pub running_count: usize,
    pub listener_running: bool,
    /// True when the program has a shared trigger (one not marked
    /// `@per_instance`): what the action bar arms. Clients show the
    /// listening indicator ONLY when this is true; `listener_running`
    /// then says whether it is listening now.
    pub has_triggers: bool,
    pub infra: Vec<ProjectInfraEntry>,
    pub executions: ProjectExecutionsSummary,
    /// True when the project has any infra-typed nodes in its source:
    /// whether a client shows the infra controls at all.
    pub has_infra: bool,
    /// Whether this install has built the program at least once. Until
    /// it has, it knows nothing of the program's nodes, so an empty
    /// `infra` says nothing about the source.
    #[serde(default)]
    pub built: bool,
    /// The image builds running for the project right now.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builds: Vec<BuildInFlight>,
    /// True when live `infra_node` rows exist whose node is NOT in the
    /// current source (the node was deleted while deployed). Never gates
    /// run/activate; clients OR it into their infra-controls check so the
    /// controls for live infra never vanish.
    pub orphaned_infra: bool,
    /// Aggregate infra state across the project's infra nodes, one of
    /// nine values: "none" (no infra nodes defined), "running" (all up),
    /// "provisioning", "stopping", "terminating" (transitional),
    /// "stopped" (all down), "partial" (mixed), "flaky", "failed".
    /// SYNC: infra_rollup values <-> packages/weft-graph/src/status.ts (infra_rollup consumer)
    pub infra_rollup: String,
    /// An infra operation is in flight that the rollup cannot see yet (a
    /// claimed stop still draining, a provisioning run before any node
    /// row flips): the window where every verb but cancel is refused.
    pub infra_busy: bool,
    /// Desired vs running hash drift. Each bit is only meaningful when
    /// the caller named the matching hash in the [`StatusQuery`].
    pub drift: ProjectDrift,
    /// Verbs the dispatcher accepts right now, from its state machine.
    /// Clients render the action bar from this list directly.
    pub available_actions: Vec<String>,
    /// Every instance's copy of a per-instance infra node, and its state.
    /// `infra` above lists the shared copies only.
    pub instance_infra: Vec<InstanceInfraEntry>,
    /// Every trigger activation, shared and per instance: what is
    /// listening, for whom, and how what stopped went down. `status` and
    /// `mode` above are the aggregate over the shared ones.
    pub activations: Vec<ActivationEntry>,
    /// Counts of preserved state, for the reactivate-time prompt.
    pub preservation: PreservationCounts,
    /// The public entries whose limits refused calls in the last two
    /// minutes, so an author sees a limit acting. Empty when none did.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limited: Vec<LimitedEntry>,
}

/// What `DELETE /projects/{id}` answers: what a forced removal could not
/// take off the cloud, one sentence each, naming it (empty otherwise).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectRemoved {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub left: Vec<String>,
}

/// One image build the project waits on, running on the install's
/// builder: one it started, or one another project started of the same
/// content.
// SYNC: BuildInFlight <-> packages/weft-graph/src/status.ts RawStatusPayload.builds
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildInFlight {
    /// The image ref it pushes.
    pub image: String,
    /// The builder's own id for it.
    pub build: String,
    #[serde(rename = "startedAtUnix")]
    pub started_at_unix: i64,
    /// Where its log is read, when the builder keeps one at an address.
    #[serde(default, rename = "logUrl", skip_serializing_if = "Option::is_none")]
    pub log_url: Option<String>,
}

/// One public entry that refused calls recently, and by which limit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitedEntry {
    /// The entry's node, as the program spells it.
    pub node: String,
    /// What refused: `this caller's calls per minute`, `this entry's
    /// calls per minute`, `this entry's runs at once`.
    pub limit: String,
    pub refused: u64,
}

/// One instance's copy of an infra node in the status answer.
// SYNC: InstanceInfraEntry <-> packages/weft-graph/src/protocol.ts InstanceInfraEntry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfraEntry {
    pub node: String,
    pub instance: crate::instance::InstanceId,
    pub status: String,
    /// How far a start of the copy got, while one is under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<crate::infra::wire::ApplyProgress>,
}

/// One trigger activation in the status answer.
// SYNC: ActivationEntry <-> packages/weft-graph/src/protocol.ts ActivationEntry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationEntry {
    pub trigger: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    pub status: ProjectStatus,
    /// Where it stands as a person reads it: its status, or for an
    /// inactive one the way it went down.
    pub mode: crate::activation::ActivationMode,
    /// An instance's fires parked because the instance has not given (or
    /// gave an invalid) value they need: how many, and why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<crate::program::WaitingFires>,
}

/// What a deactivated project kept, for the reactivate-time choice.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreservationCounts {
    /// Total fires queued across every signal in the project. Entry
    /// triggers append one per fire; resume signals at most one. Drives
    /// the "execute parked / drop parked / wipe" choice on reactivate.
    pub parked: usize,
    /// Resume signals with no submission yet: they stay across the
    /// inactive window so their suspended execution can resume later.
    pub suspended: usize,
}

/// Desired vs running drift, one bit per thing that can lag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectDrift {
    /// "Infra is stale relative to source." Drives the Upgrade button.
    pub infra_drift: bool,
    /// "Worker BINARY needs rebuilding." Flips on engine, node code,
    /// type-set or `weft.toml` edits; the dialog before running asks
    /// about killing running executions when it is set.
    pub binary_drift: bool,
    /// "Project SHAPE has changed." A config or topology edit flips it
    /// without flipping `binary_drift`; the next execution picks up the
    /// new definition.
    pub definition_drift: bool,
    /// "The listeners fire an older program." The registrations pin the
    /// code and graph they were made against, so a rebuild or a
    /// definition edit leaves them firing the old one until `weft
    /// resync`, which the verb list then offers.
    pub activation_drift: bool,
}

/// One infra place in the status answer.
// SYNC: ProjectInfraEntry <-> packages/weft-graph/src/protocol.ts InfraPlacementStatus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectInfraEntry {
    /// The node's placement, spelled the way the PROGRAM writes the node
    /// (`db`, or `one.db` inside the file the site `one` includes): what
    /// a person is shown, what the editor matches, and what every
    /// per-node verb takes. A file included twice lists its infra twice.
    pub node: String,
    /// Infra node type (e.g. "whatsapp_bridge"), so the extension can
    /// decorate the node without re-parsing the source.
    pub node_type: String,
    /// The copy's state (`provisioning`, `running`, `flaky`, `stopping`,
    /// `stopped`, `terminating`, `failed`),
    /// [`INFRA_NOT_STARTED`](crate::infra::wire::INFRA_NOT_STARTED) for a
    /// place with no copy and nothing starting one, or
    /// [`INFRA_PER_INSTANCE`](crate::infra::wire::INFRA_PER_INSTANCE) for
    /// a `@per_instance` place, which has no shared copy.
    pub status: String,
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "failureStage")]
    pub failure_stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "failureMessage")]
    pub failure_message: Option<String>,
    /// For a `per_instance` place: how many instances have a copy (each
    /// is listed in `instance_infra`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_copy_count: Option<usize>,
    /// How far a start of the copy got, while one is under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<crate::infra::wire::ApplyProgress>,
}

// SYNC: ProjectExecutionsSummary <-> packages/weft-graph/src/status.ts RawStatusPayload.executions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectExecutionsSummary {
    pub total: usize,
    pub last_completed_at: Option<u64>,
    pub last_execution_id: Option<String>,
    pub last_status: Option<crate::program::SummaryStatus>,
    /// Every execution running right now (suspended ones excluded), the
    /// same set `running_count` counts. The editor REPLACES its own
    /// running set with this on every status refresh, so a terminal event
    /// lost to a dropped stream never leaves a Stop button on a run that
    /// ended.
    ///
    /// OLDEST FIRST, so the last one is the most recently started: the
    /// editor's action bar follows "the latest run" and nothing else here
    /// says which that is. A queued run with no journal row yet sorts
    /// last, which is right, it is the newest thing in the list.
    ///
    /// Each carries its phase: the setup an `infra start` or an
    /// activation runs is running too, and the editor shows it as that
    /// verb working instead of as a run with a Stop button.
    pub running: Vec<RunningExecution>,
}

// SYNC: RunningExecution <-> packages/weft-graph/src/status.ts RunningExecution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningExecution {
    pub execution_id: String,
    pub phase: crate::primitive::Phase,
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::wire_enum_roundtrip_tests!(ProjectStatus, ProjectTransition);

    #[test]
    fn status_query_string_names_only_the_hashes_given() {
        assert_eq!(StatusQuery::default().to_query_string(), "");
        let q = StatusQuery {
            desired_binary_hash: Some("b".into()),
            desired_infra_hash: Some("i".into()),
            ..Default::default()
        };
        assert_eq!(q.to_query_string(), "?desiredBinaryHash=b&desiredInfraHash=i");
    }
}

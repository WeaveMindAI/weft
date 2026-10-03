//! In-memory `BrokerSupervisorOps` for tests.
//!
//! Mirrors what the broker's Postgres state would look like:
//! projects keyed by id, infra_nodes keyed by (project_id, node_id,
//! instance): one row per copy,
//! a pending lifecycle command queue, etc. Reads pull from the
//! state-of-the-world maps; writes update them AND append to a
//! call log so tests can assert ordering.
//!
//! The command claim keeps the broker's contract, because the lifecycle
//! loop's orchestration depends on it: a command stays issued until
//! `command_complete` records it, only an owned project's commands are
//! handed out, never a busy project's, and a claim with nothing to hand
//! out holds until a command is issued or its wait ends.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;

use anyhow::Result;
use async_trait::async_trait;

use weft_broker_client::protocol::{
    SupervisorClaim, SupervisorCommandRow, SupervisorInfraNode, SupervisorProject,
    SupervisorSyncOwnershipResponse,
};

use super::BrokerSupervisorOps;

/// A `UnitRuntime` at the given status with default windows + Stop. Seeds the fake's per-unit roster in tests.
fn unit_runtime(
    status: weft_broker_client::protocol::InfraNodeStatus,
) -> weft_broker_client::protocol::UnitRuntime {
    weft_broker_client::protocol::UnitRuntime {
        status,
        stop_behavior: weft_core::StopBehavior::Stop,
        flaky_after_seconds: 30,
        recovery_after_seconds: 30,
        image_refs: Default::default(),
    }
}

/// One recorded broker call. Used for ordering / argument
/// assertions in tests.
#[derive(Debug, Clone, PartialEq)]
pub enum BrokerCall {
    SyncOwnership {
        replica: String,
        held_projects: Vec<uuid::Uuid>,
    },
    OwnedProjects {
        replica: String,
    },
    GoneCopies {
        project: uuid::Uuid,
        copy_ids: Vec<String>,
    },
    InfraNodes {
        project_id: uuid::Uuid,
    },
    HealthProtocols {
        project_id: uuid::Uuid,
    },
    ClaimCommand {
        claimer: String,
        busy_projects: Vec<uuid::Uuid>,
    },
    EventRecord {
        project_id: uuid::Uuid,
        node_id: Option<String>,
        instance: Option<weft_core::instance::InstanceId>,
        kind: String,
        payload: serde_json::Value,
    },
    SetStatus {
        command_id: Option<i64>,
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        unit: Option<String>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<String>,
    },
    SetWaiting {
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        waiting: String,
    },
    RemoveNode {
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        command_id: i64,
    },
    CommandComplete {
        command_id: i64,
        error: Option<String>,
        cancelled: bool,
    },
    CommandCancelRequested {
        command_id: i64,
    },
    RunningCount {
        project_id: uuid::Uuid,
        copies: weft_core::instance::Copies,
    },
    InfraCommandsInFlight {
        project_id: uuid::Uuid,
    },
    SetProvisioning {
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        copy_id: String,
        keep_disks: Vec<String>,
    },
    SetApplied {
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        copy_id: String,
        applied_spec_hash: String,
        addresses: weft_broker_client::protocol::AppliedEndpoints,
        keep_disks: Vec<String>,
        notes: Vec<String>,
    },
    ProjectImageTags {
        project_id: uuid::Uuid,
        node_id: String,
    },
    EnqueueLifecycle {
        project_id: uuid::Uuid,
        spec: weft_broker_client::protocol::LifecycleSpec,
    },
}

/// One infra copy: (project, node, instance), `None` the shared copy.
type CopyKey = (uuid::Uuid, String, Option<weft_core::instance::InstanceId>);

fn copy_key(project_id: uuid::Uuid, node_id: &str, instance: Option<&weft_core::instance::InstanceId>) -> CopyKey {
    (project_id, node_id.to_string(), instance.cloned())
}

/// Whose running count answers for `copies`, as the broker counts: a
/// instance's live runs for an instance's copies, the project's live workers
/// (`None`) for the shared ones and for every copy.
fn running_key(
    project_id: uuid::Uuid,
    copies: &weft_core::instance::Copies,
) -> (uuid::Uuid, Option<weft_core::instance::InstanceId>) {
    match copies {
        weft_core::instance::Copies::Instance(i) => (project_id, Some(i.clone())),
        weft_core::instance::Copies::Shared | weft_core::instance::Copies::Every => (project_id, None),
    }
}

#[derive(Default)]
struct Inner {
    /// All projects under this tenant. Keyed by project_id.
    projects: HashMap<uuid::Uuid, SupervisorProject>,
    /// Tenant id this fake is scoped to. Set on construction;
    /// `projects_for_tenant(other)` returns empty.
    tenant_id: String,

    /// Infra nodes keyed by (project_id, node_id, instance): one per copy.
    infra_nodes: HashMap<CopyKey, SupervisorInfraNode>,

    /// Health protocols JSON per project. `None` entries return
    /// `Ok(None)` (caller falls back to `default_protocols`).
    health_protocols: HashMap<uuid::Uuid, Option<serde_json::Value>>,

    /// Every issued lifecycle command, oldest first; tests push via
    /// `enqueue_command`. A command stays here after it is claimed, as
    /// the broker's row does: `completed_commands` is what retires it.
    commands: Vec<SupervisorCommandRow>,

    /// Running execution counts returned by `running_count`, keyed as
    /// the broker counts: an instance's copies by that instance's live runs,
    /// the shared ones (and every copy) by the project's live workers
    /// (`None`). Absent = 0.
    running_counts: HashMap<(uuid::Uuid, Option<weft_core::instance::InstanceId>), i64>,

    /// Per-project uncompleted supervisor commands returned by
    /// `infra_commands_in_flight`. Absent = none.
    infra_commands_in_flight: HashMap<uuid::Uuid, Vec<weft_broker_client::protocol::InFlightCommand>>,

    /// Per-(project, node) image tag map returned by `project_image_tags`.
    image_tags: HashMap<(uuid::Uuid, String), HashMap<String, String>>,

    /// Completed lifecycle commands (id, optional error message,
    /// cancelled flag).
    completed_commands: Vec<(i64, Option<String>, bool)>,

    /// Per-command `cancel_requested` flag returned by
    /// `command_cancel_requested`. Absent = false. Tests set it to
    /// simulate the user's `/infra/cancel` landing mid-command.
    cancel_requested: HashMap<i64, bool>,

    /// project_id of each claimed command, so `command_complete` (which
    /// carries only a command_id) can apply the same per-project
    /// ownership gate the broker does.
    claimed_command_project: HashMap<i64, uuid::Uuid>,

    /// Who holds each project's exclusive `infra_owner` lease. Absent =
    /// this supervisor (the common case for a seeded project). With
    /// another owner, every ownership-gated write (set_provisioning /
    /// set_applied / set_status / remove_node / command_complete)
    /// returns `Displaced`, exactly as the broker's
    /// `owns_project_predicate` would, and the claim hands out none of
    /// the project's commands.
    owners: HashMap<uuid::Uuid, Owner>,

    /// Projects whose lease moves to another supervisor the moment one of
    /// their commands is claimed: models ownership moving mid-command.
    displaced_on_claim: std::collections::HashSet<uuid::Uuid>,

    /// Projects whose lease moves to another supervisor right after a
    /// `gone_copies` judges them for this one: models ownership moving
    /// between the sweep's judgment and its deletion.
    displaced_on_judgment: std::collections::HashSet<uuid::Uuid>,

    /// Projects that exist but have no infra to manage and no
    /// `infra_node` row: `ownable_project` only while the host holds
    /// their copies or a command waits on them.
    infraless: std::collections::HashSet<uuid::Uuid>,

    /// Projects whose `running_count` answers only once the gate opens,
    /// so a test can hold a `wait`-policy command mid-drain.
    running_count_gates: HashMap<uuid::Uuid, Arc<tokio::sync::Semaphore>>,

    /// Places (project, node) the program no longer declares, any copy
    /// of them. Absent = declared (the seeded default).
    undeclared: std::collections::HashSet<(uuid::Uuid, String)>,

    calls: Vec<BrokerCall>,
}

/// Who holds a project's `infra_owner` lease, as the fake models it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
    /// This supervisor.
    Us,
    /// Another supervisor.
    Other,
    /// Nobody: never claimed, or the lease lapsed. The next ownership
    /// tick takes it.
    Nobody,
}

impl Inner {
    /// Whether this supervisor still owns `project_id`. Absent = owned
    /// (the seeded default). Mirrors the broker's `owns_project_predicate`.
    fn owns(&self, project_id: uuid::Uuid) -> bool {
        self.owners.get(&project_id).copied().unwrap_or(Owner::Us) == Owner::Us
    }

    fn completed(&self, command_id: i64) -> bool {
        self.completed_commands.iter().any(|(id, _, _)| *id == command_id)
    }

    /// The oldest uncompleted command of an owned project not in `busy`,
    /// as the broker's `next_command` picks it.
    fn next_command(&self, busy: &[uuid::Uuid]) -> Option<SupervisorCommandRow> {
        self.commands
            .iter()
            .find(|c| !self.completed(c.id) && self.owns(c.project_id) && !busy.contains(&c.project_id))
            .cloned()
    }

    /// Whether a supervisor may own `project_id`, as the broker's
    /// `ownable_project` answers it: infra to manage, copies on the host
    /// (`held`), or an uncompleted command waiting on it.
    fn ownable(&self, project_id: uuid::Uuid, held: &[uuid::Uuid]) -> bool {
        !self.infraless.contains(&project_id) || held.contains(&project_id) || self.has_work(project_id)
    }

    /// Whether `project_id` gives its owner something to look at now, as
    /// the broker's `supervisor_work` answers it: infra nodes, or an
    /// uncompleted command.
    fn has_work(&self, project_id: uuid::Uuid) -> bool {
        self.infra_nodes.keys().any(|k| k.0 == project_id)
            || self.commands.iter().any(|c| c.project_id == project_id && !self.completed(c.id))
    }

    /// Whether an uncompleted command waits on a project nobody holds.
    fn unowned_work_waiting(&self) -> bool {
        self.commands.iter().any(|c| {
            !self.completed(c.id) && self.owners.get(&c.project_id) == Some(&Owner::Nobody)
        })
    }
}

pub struct FakeBroker {
    inner: Mutex<Inner>,
    /// Raised when a command is issued, as the row's own notification
    /// is on the broker: it ends a held claim's wait.
    issued: tokio::sync::Notify,
}

impl FakeBroker {
    pub fn new(tenant_id: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                tenant_id: tenant_id.to_string(),
                ..Default::default()
            }),
            issued: tokio::sync::Notify::new(),
        })
    }

    // ---------- seeding ----------

    pub fn add_project(&self, project_id: uuid::Uuid) {
        self.add_project_with_status(project_id, weft_broker_client::protocol::ProjectStatus::Active)
    }

    /// Same as `add_project` but with an explicit status. Used by
    /// tests that exercise `HealthCondition::ProjectStatusEq`.
    pub fn add_project_with_status(
        &self,
        project_id: uuid::Uuid,
        status: weft_broker_client::protocol::ProjectStatus,
    ) {
        let mut inner = self.inner.lock();
        let tenant_id = inner.tenant_id.clone();
        inner.projects.insert(
            project_id,
            SupervisorProject {
                project_id,
                tenant_id,
                status,
                health_parked: false,
            },
        );
    }

    /// Seed a project whose program declares no infra and that has no
    /// `infra_node` row left, owned by nobody: only a host holding its
    /// copies, or a command waiting on it, gets it claimed.
    pub fn add_infraless_project(&self, project_id: uuid::Uuid) {
        self.add_project_with_status(project_id, weft_broker_client::protocol::ProjectStatus::Inactive);
        let mut inner = self.inner.lock();
        inner.infraless.insert(project_id);
        inner.owners.insert(project_id, Owner::Nobody);
    }

    /// Move `project_id`'s lease to another supervisor right after the
    /// next `gone_copies` judges it for this one.
    pub fn displace_on_judgment(&self, project_id: uuid::Uuid) {
        self.inner.lock().displaced_on_judgment.insert(project_id);
    }

    /// Flip `health_parked` on a seeded project. Lets a test
    /// distinguish "the health loop parked something still down" (true)
    /// from "a person deactivated" (false) when exercising the
    /// auto-recover gate.
    pub fn set_health_parked(&self, project_id: uuid::Uuid, parked: bool) {
        if let Some(p) = self.inner.lock().projects.get_mut(&project_id) {
            p.health_parked = parked;
        }
    }

    /// Seed a node with ONE unit (named after `node_id`) at `status`.
    /// Tests label their single workload `weft.dev/unit = node_id`, so
    /// this gives the health loop a one-unit roster matching them.
    pub fn add_infra_node(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        copy_id: &str,
        status: weft_broker_client::protocol::InfraNodeStatus,
    ) {
        let mut units = BTreeMap::new();
        units.insert(node_id.to_string(), unit_runtime(status));
        self.add_infra_node_with(
            project_id,
            node_id,
            copy_id,
            status,
            None,
            BTreeMap::new(),
            units,
        );
    }

    /// Seed a multi-unit node: `units` maps unit name -> status.
    pub fn add_infra_node_units(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        copy_id: &str,
        units: &[(&str, weft_broker_client::protocol::InfraNodeStatus)],
    ) {
        let units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime> = units
            .iter()
            .map(|(u, s)| (u.to_string(), unit_runtime(*s)))
            .collect();
        let status =
            weft_broker_client::protocol::InfraNodeStatus::rollup(units.values().map(|u| &u.status));
        self.add_infra_node_with(
            project_id,
            node_id,
            copy_id,
            status,
            None,
            BTreeMap::new(),
            units,
        );
    }

    /// Seed an infra_node row. Pass `endpoints` as you'd expect a
    /// post-apply state: a non-empty map for `running`, empty for
    /// `provisioning` / `stopped`.
    pub fn add_infra_node_with(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        copy_id: &str,
        status: weft_broker_client::protocol::InfraNodeStatus,
        applied_spec_hash: Option<String>,
        endpoints: BTreeMap<String, String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) {
        self.inner.lock().infra_nodes.insert(
            copy_key(project_id, node_id, None),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                copy_id: copy_id.to_string(),
                status,
                applied_spec_hash,
                applied_at_unix: None,
                addresses: weft_broker_client::protocol::AppliedEndpoints {
                    install_urls: endpoints.clone(),
                    urls: endpoints,
                    public_paths: BTreeMap::new(),
                    doors: BTreeMap::new(),
                },
                keep_disks: Vec::new(),
                units,
                instance: None,
            },
        );
    }

    /// Seed one instance's copy of a node, with ONE unit (named
    /// after `node_id`) at `status`, beside any shared copy.
    pub fn add_instance_infra_node(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: &weft_core::instance::InstanceId,
        copy_id: &str,
        status: weft_broker_client::protocol::InfraNodeStatus,
    ) {
        let mut units = BTreeMap::new();
        units.insert(node_id.to_string(), unit_runtime(status));
        self.inner.lock().infra_nodes.insert(
            copy_key(project_id, node_id, Some(instance)),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                copy_id: copy_id.to_string(),
                status,
                applied_spec_hash: None,
                applied_at_unix: None,
                addresses: Default::default(),
                keep_disks: Vec::new(),
                units,
                instance: Some(instance.clone()),
            },
        );
    }

    /// The program no longer declares `node` of `project` (it was
    /// removed from the source): what `gone_copies` judges row-less
    /// copies against.
    pub fn undeclare(&self, project_id: uuid::Uuid, node_id: &str) {
        self.inner.lock().undeclared.insert((project_id, node_id.to_string()));
    }

    /// The disks a seeded copy recorded at its apply (what terminate,
    /// and a Fresh apply finishing a failed terminate, keep).
    pub fn set_keep_disks(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        keep_disks: Vec<String>,
    ) {
        let mut inner = self.inner.lock();
        let node = inner
            .infra_nodes
            .get_mut(&copy_key(project_id, node_id, instance))
            .expect("set_keep_disks on a seeded row");
        node.keep_disks = keep_disks;
    }

    pub fn set_health_protocols(&self, project_id: uuid::Uuid, protocols: serde_json::Value) {
        self.inner
            .lock()
            .health_protocols
            .insert(project_id, Some(protocols));
    }

    /// Issue a command: it waits until `command_complete` records it,
    /// and a claim held for one wakes.
    pub fn enqueue_command(&self, cmd: SupervisorCommandRow) {
        self.inner.lock().commands.push(cmd);
        self.issued.notify_waiters();
    }

    /// Simulate ownership of `project_id` moving to another supervisor (`false`)
    /// or back to this supervisor (`true`). With `false`, every
    /// ownership-gated write for the project returns `Displaced`,
    /// mirroring the broker's `infra_owner` gate, and none of its
    /// commands is handed out; an in-flight one is left uncompleted for
    /// the new owner.
    pub fn set_project_owned(&self, project_id: uuid::Uuid, owned: bool) {
        let owner = if owned { Owner::Us } else { Owner::Other };
        self.inner.lock().owners.insert(project_id, owner);
    }

    /// Simulate nobody holding `project_id`'s lease (never claimed, or
    /// lapsed). The next `sync_ownership` takes it and reports it
    /// claimed; until then its commands are unowned work.
    pub fn set_project_unowned(&self, project_id: uuid::Uuid) {
        self.inner.lock().owners.insert(project_id, Owner::Nobody);
    }

    /// Move `project_id`'s lease to another supervisor the moment one of its
    /// commands is claimed: ownership moving mid-command.
    pub fn displace_on_claim(&self, project_id: uuid::Uuid) {
        self.inner.lock().displaced_on_claim.insert(project_id);
    }

    /// Hold every `running_count` of `project_id` until
    /// [`Self::open_running_count`]: a `wait`-policy command stays
    /// mid-drain for as long as the test needs it running.
    pub fn gate_running_count(&self, project_id: uuid::Uuid) {
        self.inner
            .lock()
            .running_count_gates
            .insert(project_id, Arc::new(tokio::sync::Semaphore::new(0)));
    }

    /// Let every held and later `running_count` of `project_id` answer.
    pub fn open_running_count(&self, project_id: uuid::Uuid) {
        if let Some(gate) = self.inner.lock().running_count_gates.get(&project_id) {
            gate.close();
        }
    }

    /// What `running_count` answers for `copies`: one instance's live runs
    /// for an instance's copies, the project's live workers for the shared
    /// ones and for every copy (the one count the broker gives both).
    pub fn set_running_count(&self, project_id: uuid::Uuid, copies: &weft_core::instance::Copies, n: i64) {
        self.inner.lock().running_counts.insert(running_key(project_id, copies), n);
    }

    /// The uncompleted supervisor commands `infra_commands_in_flight`
    /// answers for the project; empty for none.
    pub fn set_infra_commands_in_flight(
        &self,
        project_id: uuid::Uuid,
        commands: Vec<weft_broker_client::protocol::InFlightCommand>,
    ) {
        self.inner.lock().infra_commands_in_flight.insert(project_id, commands);
    }

    // ---------- introspection ----------

    pub fn calls(&self) -> Vec<BrokerCall> {
        self.inner.lock().calls.clone()
    }

    /// All `event_record` calls in order, returned as
    /// `(project_id, node_id, instance, kind, payload)` for ergonomic
    /// pattern-matching in tests.
    #[allow(clippy::type_complexity)]
    pub fn events(
        &self,
    ) -> Vec<(uuid::Uuid, Option<String>, Option<weft_core::instance::InstanceId>, String, serde_json::Value)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                BrokerCall::EventRecord {
                    project_id,
                    node_id,
                    instance,
                    kind,
                    payload,
                } => Some((
                    *project_id,
                    node_id.clone(),
                    instance.clone(),
                    kind.clone(),
                    payload.clone(),
                )),
                _ => None,
            })
            .collect()
    }

    /// All `set_status` calls in order, as `(project_id, node_id,
    /// instance, status)`.
    pub fn status_writes(
        &self,
    ) -> Vec<(uuid::Uuid, String, Option<weft_core::instance::InstanceId>, weft_broker_client::protocol::InfraNodeStatus)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                BrokerCall::SetStatus {
                    project_id,
                    node_id,
                    instance,
                    status,
                    ..
                } => Some((*project_id, node_id.clone(), instance.clone(), *status)),
                _ => None,
            })
            .collect()
    }

    /// Look up an infra_node's current state (what the supervisor
    /// would observe on the next infra_nodes call).
    pub fn infra_node(&self, project_id: uuid::Uuid, node_id: &str) -> Option<SupervisorInfraNode> {
        self.infra_copy(project_id, node_id, None)
    }

    /// One copy of a node: the shared one (`instance = None`) or an instance's.
    pub fn infra_copy(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> Option<SupervisorInfraNode> {
        self.inner
            .lock()
            .infra_nodes
            .get(&copy_key(project_id, node_id, instance))
            .cloned()
    }

    /// `command_complete` calls that recorded an error (cancelled
    /// completions excluded: a cancel is not a failure).
    pub fn failed_commands(&self) -> Vec<(i64, String)> {
        self.inner
            .lock()
            .completed_commands
            .iter()
            .filter(|(_, _, cancelled)| !cancelled)
            .filter_map(|(id, err, _)| err.as_ref().map(|e| (*id, e.clone())))
            .collect()
    }

    pub fn completed_commands(&self) -> Vec<(i64, Option<String>, bool)> {
        self.inner.lock().completed_commands.clone()
    }

    /// Script the user's `/infra/cancel` landing mid-command: the next
    /// `command_cancel_requested(command_id)` poll returns true.
    pub fn set_cancel_requested(&self, command_id: i64) {
        self.inner.lock().cancel_requested.insert(command_id, true);
    }

    /// Commands completed with the `cancelled` outcome.
    pub fn cancelled_commands(&self) -> Vec<i64> {
        self.inner
            .lock()
            .completed_commands
            .iter()
            .filter(|(_, _, cancelled)| *cancelled)
            .map(|(id, _, _)| *id)
            .collect()
    }
}

impl Default for FakeBroker {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            issued: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait]
impl BrokerSupervisorOps for FakeBroker {
    async fn sync_ownership(&self, replica: &str, held_projects: &[uuid::Uuid]) -> Result<SupervisorSyncOwnershipResponse> {
        // A single supervisor: it takes every project nobody holds that
        // it may own (`ownable`) and renews what it owns while it may
        // still own it; a lease it stops renewing lapses (at once here,
        // after the lease time on the broker). Partitioning between
        // supervisors is covered by the broker's SQL-level tests. The
        // owned set is every lease it holds, as the broker's is.
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SyncOwnership {
            replica: replica.to_string(),
            held_projects: held_projects.to_vec(),
        });
        let lapsed: Vec<uuid::Uuid> = inner
            .projects
            .keys()
            .filter(|id| inner.owns(**id) && !inner.ownable(**id, held_projects))
            .copied()
            .collect();
        for id in lapsed {
            inner.owners.insert(id, Owner::Nobody);
        }
        let mut taken: Vec<uuid::Uuid> = inner
            .owners
            .iter()
            .filter(|(id, owner)| **owner == Owner::Nobody && inner.ownable(**id, held_projects))
            .map(|(id, _)| *id)
            .collect();
        taken.sort_unstable();
        for id in &taken {
            inner.owners.insert(*id, Owner::Us);
        }
        let claimed = taken.into_iter().filter(|id| inner.projects.contains_key(id)).collect();
        let owned = inner.projects.values().filter(|p| inner.owns(p.project_id)).cloned().collect();
        // Every project row, whoever holds its lease, as the broker reads
        // its `project` table. The fake keeps no lease times: a sibling's
        // lease lapses a whole lease from now, and one nobody holds has
        // lapsed.
        let with_work: Vec<uuid::Uuid> = inner.projects.keys().copied().filter(|id| inner.has_work(*id)).collect();
        let owns_work = with_work.iter().any(|id| inner.owns(*id));
        let others_lapse_in_secs = inner
            .projects
            .keys()
            .filter(|id| inner.has_work(**id) || held_projects.contains(id))
            .filter_map(|id| match inner.owners.get(id) {
                Some(Owner::Other) => Some(weft_broker_client::lifecycle_command::infra_owner_lease_secs()),
                Some(Owner::Nobody) => Some(0),
                Some(Owner::Us) | None => None,
            })
            .min();
        Ok(SupervisorSyncOwnershipResponse { owned, claimed, owns_work, others_lapse_in_secs })
    }

    async fn owned_projects(&self, replica: &str) -> Result<Vec<SupervisorProject>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::OwnedProjects { replica: replica.to_string() });
        // A project whose ownership moved away is no longer this
        // supervisor's, which is what the broker's lease query answers too.
        Ok(inner.projects.values().filter(|p| inner.owns(p.project_id)).cloned().collect())
    }

    async fn gone_copies(
        &self,
        _replica: &str,
        project: uuid::Uuid,
        copies: &[weft_core::infra::NodeRef],
    ) -> Result<Option<Vec<String>>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::GoneCopies {
            project,
            copy_ids: copies.iter().map(|c| c.copy_id.clone()).collect(),
        });
        assert!(copies.iter().all(|c| c.project == project), "gone_copies is asked about one project at a time");
        // The broker's judgment: a removed project's copies are all gone;
        // an existing project's are judged only for its owner (None
        // otherwise, the broker's 410), and gone when no row holds the
        // copy and the program no longer declares its place.
        if !inner.projects.contains_key(&project) {
            return Ok(Some(copies.iter().map(|c| c.copy_id.clone()).collect()));
        }
        if !inner.owns(project) {
            return Ok(None);
        }
        let gone = copies
            .iter()
            .filter(|c| {
                !inner.infra_nodes.values().any(|n| n.copy_id == c.copy_id)
                    && (inner.infraless.contains(&project) || inner.undeclared.contains(&(project, c.node.clone())))
            })
            .map(|c| c.copy_id.clone())
            .collect();
        if inner.displaced_on_judgment.remove(&project) {
            inner.owners.insert(project, Owner::Other);
        }
        Ok(Some(gone))
    }

    async fn infra_nodes(&self, project_id: uuid::Uuid) -> Result<Vec<SupervisorInfraNode>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::InfraNodes {
            project_id,
        });
        Ok(inner
            .infra_nodes
            .iter()
            .filter(|((p, _, _), _)| *p == project_id)
            .map(|(_, n)| n.clone())
            .collect())
    }

    async fn health_protocols(
        &self,
        project_id: uuid::Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::HealthProtocols {
            project_id,
        });
        Ok(inner
            .health_protocols
            .get(&project_id)
            .cloned()
            .unwrap_or(None))
    }

    /// The broker's held claim: the next command, else a hold (on the
    /// tokio clock) until one is issued or `wait` ends. A wake that
    /// finds nothing for this supervisor answers `UnownedWork` when a command
    /// waits on a project nobody holds, as the broker does after a wake.
    async fn claim_command(
        &self,
        claimer: &str,
        busy_projects: &[uuid::Uuid],
        wait: std::time::Duration,
    ) -> Result<SupervisorClaim> {
        self.inner.lock().calls.push(BrokerCall::ClaimCommand {
            claimer: claimer.to_string(),
            busy_projects: busy_projects.to_vec(),
        });
        let deadline = tokio::time::Instant::now() + wait;
        let mut woken_once = false;
        loop {
            // Armed before the look, so an issue landing between the
            // look and the wait still ends the wait.
            let issued = self.issued.notified();
            tokio::pin!(issued);
            issued.as_mut().enable();
            {
                let mut inner = self.inner.lock();
                if let Some(cmd) = inner.next_command(busy_projects) {
                    inner.claimed_command_project.insert(cmd.id, cmd.project_id);
                    if inner.displaced_on_claim.remove(&cmd.project_id) {
                        inner.owners.insert(cmd.project_id, Owner::Other);
                    }
                    return Ok(SupervisorClaim::Command(cmd));
                }
                if woken_once && inner.unowned_work_waiting() {
                    return Ok(SupervisorClaim::UnownedWork);
                }
            }
            if tokio::time::timeout_at(deadline, issued).await.is_err() {
                return Ok(SupervisorClaim::Nothing);
            }
            woken_once = true;
        }
    }

    async fn event_record(
        &self,
        project_id: uuid::Uuid,
        node_id: Option<&str>,
        instance: Option<&weft_core::instance::InstanceId>,
        event: weft_broker_client::protocol::InfraEvent,
    ) -> Result<i64> {
        let (kind, payload) = event.into_record();
        let mut inner = self.inner.lock();
        let id = inner.calls.len() as i64 + 1;
        inner.calls.push(BrokerCall::EventRecord {
            project_id,
            node_id: node_id.map(|s| s.to_string()),
            instance: instance.cloned(),
            kind: kind.as_str().to_string(),
            payload,
        });
        Ok(id)
    }

    async fn set_status(
        &self,
        _replica: &str,
        command_id: Option<i64>,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetStatus {
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            unit: unit.map(|s| s.to_string()),
            status,
            failure_stage,
            failure_message: failure_message.map(|s| s.to_string()),
        });
        // Every write is ownership-gated, with or without a command,
        // matching the broker. A lost-ownership project is Displaced.
        if !inner.owns(project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        if let Some(node) = inner
            .infra_nodes
            .get_mut(&copy_key(project_id, node_id, instance))
        {
            // Mirror prod: per-unit sets that unit then rolls up the
            // node status; node-wide sets every unit AND the node
            // status to the value directly (so a unit-less roster still
            // takes a Failed / Terminating stamp). A per-unit stamp for
            // a unit NOT in the roster is Gone, exactly like prod's
            // fence (`units_json ? $1` matches no row while the supervisor
            // still owns the project): the old silent no-op here let a
            // caller bug pass tests.
            match unit {
                Some(u) => {
                    let Some(ur) = node.units.get_mut(u) else {
                        return Ok(weft_broker_client::WriteOutcome::Gone);
                    };
                    ur.status = status;
                    node.status = weft_broker_client::protocol::InfraNodeStatus::rollup(
                        node.units.values().map(|u| &u.status),
                    );
                }
                None => {
                    for ur in node.units.values_mut() {
                        ur.status = status;
                    }
                    node.status = status;
                }
            }
            Ok(weft_broker_client::WriteOutcome::Applied(
                weft_broker_client::protocol::SupervisorSetStatusResponse {},
            ))
        } else {
            // No row to update while still owning the project: Gone.
            Ok(weft_broker_client::WriteOutcome::Gone)
        }
    }

    async fn set_waiting(
        &self,
        _replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        waiting: &str,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetWaitingResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetWaiting {
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            waiting: waiting.to_string(),
        });
        if !inner.owns(project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        Ok(weft_broker_client::WriteOutcome::Applied(weft_broker_client::protocol::SupervisorSetWaitingResponse {}))
    }

    async fn remove_node(
        &self,
        _replica: &str,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        command_id: i64,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorRemoveNodeResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::RemoveNode {
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            command_id,
        });
        // Ownership-gated, like the broker: a lost-ownership project
        // does NOT cascade-delete; it is Displaced so the supervisor
        // aborts and leaves the command for the new owner.
        if !inner.owns(project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        let removed = inner
            .infra_nodes
            .remove(&copy_key(project_id, node_id, instance))
            .is_some();
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorRemoveNodeResponse { removed },
        ))
    }

    async fn command_complete(
        &self,
        _replica: &str,
        command_id: i64,
        error: Option<&str>,
        cancelled: bool,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorCommandCompleteResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::CommandComplete {
            command_id,
            error: error.map(|s| s.to_string()),
            cancelled,
        });
        // Ownership-gated, like the broker: if this supervisor lost ownership of
        // the command's project, completion is Displaced and the command
        // stays uncompleted for the new owner to finish.
        if let Some(project_id) = inner.claimed_command_project.get(&command_id).cloned() {
            if !inner.owns(project_id) {
                return Ok(weft_broker_client::WriteOutcome::Displaced);
            }
        }
        // Exactly-once, like the broker's `completed_at_unix IS NULL`:
        // a second completion is Gone.
        if inner.completed(command_id) {
            return Ok(weft_broker_client::WriteOutcome::Gone);
        }
        inner
            .completed_commands
            .push((command_id, error.map(|s| s.to_string()), cancelled));
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorCommandCompleteResponse {},
        ))
    }

    async fn command_cancel_requested(&self, command_id: i64) -> Result<bool> {
        let mut inner = self.inner.lock();
        inner
            .calls
            .push(BrokerCall::CommandCancelRequested { command_id });
        Ok(inner.cancel_requested.get(&command_id).copied().unwrap_or(false))
    }

    async fn running_count(&self, project_id: uuid::Uuid, copies: &weft_core::instance::Copies) -> Result<i64> {
        let gate = {
            let mut inner = self.inner.lock();
            inner.calls.push(BrokerCall::RunningCount {
                project_id,
                copies: copies.clone(),
            });
            inner.running_count_gates.get(&project_id).cloned()
        };
        if let Some(gate) = gate {
            // Opening the gate closes it: every acquire then returns.
            let _ = gate.acquire().await;
        }
        Ok(self.inner.lock().running_counts.get(&running_key(project_id, copies)).copied().unwrap_or(0))
    }

    async fn infra_commands_in_flight(
        &self,
        project_id: uuid::Uuid,
    ) -> Result<Vec<weft_broker_client::protocol::InFlightCommand>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::InfraCommandsInFlight {
            project_id,
        });
        Ok(inner.infra_commands_in_flight.get(&project_id).cloned().unwrap_or_default())
    }

    async fn set_provisioning(
        &self,
        _replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        keep_disks: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetProvisioningResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetProvisioning {
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            copy_id: copy_id.to_string(),
            keep_disks: keep_disks.clone(),
        });
        if !inner.owns(project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        // Prod's `write_apply_row` INSERTs from the still-uncompleted
        // command row: a completed command matches nothing (Gone).
        if inner.completed(command_id) {
            return Ok(weft_broker_client::WriteOutcome::Gone);
        }
        // Mirror prod's `write_apply_row`: the provisioning stamp writes
        // a flat Provisioning (the node IS mid-apply, whatever a frozen
        // or carried unit says); set_applied derives from the roster.
        inner.infra_nodes.insert(
            copy_key(project_id, node_id, instance),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                copy_id: copy_id.to_string(),
                status: weft_broker_client::protocol::InfraNodeStatus::Provisioning,
                applied_spec_hash: None,
                applied_at_unix: None,
                addresses: Default::default(),
                keep_disks,
                units,
                instance: instance.cloned(),
            },
        );
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorSetProvisioningResponse {},
        ))
    }

    async fn set_applied(
        &self,
        _replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        applied_spec_hash: &str,
        addresses: weft_broker_client::protocol::AppliedEndpoints,
        keep_disks: Vec<String>,
        notes: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetAppliedResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetApplied {
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            copy_id: copy_id.to_string(),
            applied_spec_hash: applied_spec_hash.to_string(),
            addresses: addresses.clone(),
            keep_disks: keep_disks.clone(),
            notes,
        });
        if !inner.owns(project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        // As in set_provisioning: a completed command matches nothing.
        if inner.completed(command_id) {
            return Ok(weft_broker_client::WriteOutcome::Gone);
        }
        // The same roster-derived status prod's set_applied writes.
        let status = weft_broker_client::protocol::InfraNodeStatus::applied_rollup(
            units.values().map(|u| &u.status),
        );
        inner.infra_nodes.insert(
            copy_key(project_id, node_id, instance),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                copy_id: copy_id.to_string(),
                status,
                applied_spec_hash: Some(applied_spec_hash.to_string()),
                // The fake has no wall clock; a test that needs a
                // settled copy seeds one.
                applied_at_unix: None,
                addresses,
                keep_disks,
                units,
                instance: instance.cloned(),
            },
        );
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorSetAppliedResponse {},
        ))
    }

    async fn project_image_tags(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
    ) -> Result<HashMap<String, String>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::ProjectImageTags {
            project_id,
            node_id: node_id.to_string(),
        });
        Ok(inner
            .image_tags
            .get(&(project_id, node_id.to_string()))
            .cloned()
            .unwrap_or_default())
    }
    async fn enqueue_lifecycle(
        &self,
        project_id: uuid::Uuid,
        spec: weft_broker_client::protocol::LifecycleSpec,
    ) -> Result<i64> {
        let mut inner = self.inner.lock();
        let id = inner.calls.len() as i64 + 1;
        inner.calls.push(BrokerCall::EnqueueLifecycle {
            project_id,
            spec,
        });
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P1: uuid::Uuid = uuid::Uuid::from_u128(1);

    #[tokio::test]
    async fn owned_projects_returns_seeded() {
        let b = FakeBroker::new("alice");
        b.add_project(P1);
        let projects = b.owned_projects("sup-1").await.unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].project_id, P1);
        assert_eq!(projects[0].tenant_id, "alice");
    }

    #[tokio::test]
    async fn sync_ownership_records_call_and_returns_seeded() {
        let b = FakeBroker::new("alice");
        b.add_project(P1);
        let synced = b.sync_ownership("sup-1", &[]).await.unwrap();
        assert_eq!(synced.owned.len(), 1);
        assert!(synced.claimed.is_empty(), "a seeded project is already owned");
        assert!(!synced.owns_work, "a project with no node, copy or command gives nothing to look at");
        assert!(b.calls().iter().any(|c| matches!(c, BrokerCall::SyncOwnership { replica, .. } if replica == "sup-1")));
    }

    #[tokio::test]
    async fn set_status_updates_in_place() {
        let b = FakeBroker::new("alice");
        b.add_infra_node(
            P1,
            "n1",
            "inst1",
            weft_broker_client::protocol::InfraNodeStatus::Provisioning,
        );
        b.set_status(
            "sup-1",
            None,
            P1,
            "n1",
            None,
            Some("n1"),
            weft_broker_client::protocol::InfraNodeStatus::Running,
            None,
            None,
        )
        .await
        .unwrap();
        let node = b.infra_node(P1, "n1").unwrap();
        assert_eq!(
            node.status,
            weft_broker_client::protocol::InfraNodeStatus::Running
        );
    }

    #[tokio::test]
    async fn remove_node_drops_row() {
        let b = FakeBroker::new("alice");
        b.add_infra_node(
            P1,
            "n1",
            "inst1",
            weft_broker_client::protocol::InfraNodeStatus::Running,
        );
        let outcome = b.remove_node("sup-1", P1, "n1", None, 1).await.unwrap();
        assert!(matches!(
            outcome,
            weft_broker_client::WriteOutcome::Applied(
                weft_broker_client::protocol::SupervisorRemoveNodeResponse { removed: true }
            )
        ));
        assert!(b.infra_node(P1, "n1").is_none());
    }

    fn stop(id: i64, project_id: uuid::Uuid) -> SupervisorCommandRow {
        SupervisorCommandRow {
            id,
            project_id,
            node_id: None,
            verb: weft_broker_client::protocol::InfraLifecycleVerb::Stop,
            running_policy: Some(weft_broker_client::protocol::RunningPolicy::Wait),
            spec_json: None,
            force: false,
            drain_timeout_secs: weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
            copies: weft_core::instance::Copies::Shared,
        }
    }

    async fn claim(b: &FakeBroker, busy: &[uuid::Uuid]) -> SupervisorClaim {
        b.claim_command("sup-1", busy, std::time::Duration::ZERO).await.unwrap()
    }

    #[tokio::test]
    async fn a_command_stays_claimable_until_completed() {
        let b = FakeBroker::new("alice");
        b.enqueue_command(stop(1, P1));
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Command(c) if c.id == 1));
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Command(c) if c.id == 1));
        assert!(matches!(claim(&b, &[P1]).await, SupervisorClaim::Nothing), "a busy project's command waits");
        b.command_complete("sup-1", 1, None, false).await.unwrap();
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Nothing));
    }

    #[tokio::test]
    async fn only_an_owned_projects_command_is_handed_out() {
        let b = FakeBroker::new("alice");
        b.set_project_owned(P1, false);
        b.enqueue_command(stop(1, P1));
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Nothing));
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_claim_wakes_on_an_issue_and_names_unowned_work() {
        let b = FakeBroker::new("alice");
        b.set_project_unowned(P1);
        let wait = std::time::Duration::from_secs(25);
        let held = b.claim_command("sup-1", &[], wait);
        tokio::pin!(held);
        assert!(futures::poll!(held.as_mut()).is_pending(), "nothing to hand out: the claim holds");
        b.enqueue_command(stop(1, P1));
        assert!(matches!(held.await.unwrap(), SupervisorClaim::UnownedWork));
        // Without a wake the hold ends on its own, answering Nothing.
        assert!(matches!(
            b.claim_command("sup-1", &[], wait).await.unwrap(),
            SupervisorClaim::Nothing
        ));
    }

    #[tokio::test]
    async fn events_are_introspectable() {
        let b = FakeBroker::new("alice");
        b.event_record(
            P1,
            Some("n1"),
            None,
            weft_broker_client::protocol::InfraEvent::Flaky(
                weft_broker_client::protocol::FlakyPayload { reason: "unit 'a' is not ready: exit 1".into() },
            ),
        )
        .await
        .unwrap();
        b.event_record(
            P1,
            Some("n1"),
            None,
            weft_broker_client::protocol::InfraEvent::Recovered,
        )
        .await
        .unwrap();
        let ev = b.events();
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].3, "flaky");
        assert_eq!(ev[1].3, "recovered");
    }
}

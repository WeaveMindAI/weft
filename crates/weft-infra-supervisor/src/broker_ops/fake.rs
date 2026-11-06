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
    HealthRuleState, ProjectTrouble, SupervisorClaim, SupervisorCommandRow, SupervisorInfraNode,
    SupervisorProject, SupervisorSetHealthStateRequest, SupervisorSetHealthStateResponse, SupervisorSyncOwnershipRequest,
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
        not_ready_since_unix: None,
        ready_since_unix: None,
        last_restart: None,
    }
}

/// One recorded broker call. Used for ordering / argument
/// assertions in tests.
#[derive(Debug, Clone, PartialEq)]
pub enum BrokerCall {
    SyncOwnership {
        replica: String,
        held_projects: Option<Vec<uuid::Uuid>>,
        looked: Vec<uuid::Uuid>,
        settled: Vec<uuid::Uuid>,
        failed: Vec<ProjectTrouble>,
        gave_up: Vec<ProjectTrouble>,
        all_failed: Option<String>,
        ending: bool,
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
    HealthState {
        project_id: uuid::Uuid,
    },
    SetHealthState {
        project_id: uuid::Uuid,
        rules: Vec<HealthRuleState>,
    },
    ClaimCommand {
        claimer: String,
        busy_commands: Vec<i64>,
        cancelling: Vec<i64>,
    },
    EventRecord {
        project_id: uuid::Uuid,
        node_id: Option<String>,
        instance: Option<weft_core::instance::InstanceId>,
        kind: String,
        payload: serde_json::Value,
    },
    SetStatus {
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        unit: Option<String>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<String>,
    },
    SetUnitHealth {
        project_id: uuid::Uuid,
        node_id: String,
        instance: Option<weft_core::instance::InstanceId>,
        unit: String,
        status: weft_broker_client::protocol::InfraNodeStatus,
        not_ready_since_unix: Option<i64>,
        ready_since_unix: Option<i64>,
        restart: Option<weft_core::infra::LastRestart>,
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

/// One copy's `infra_node` row: what `infra_nodes` hands the supervisor,
/// and the columns its writes change that it never reads back.
struct CopyRow {
    node: SupervisorInfraNode,
    progress: CopyProgress,
}

impl CopyRow {
    /// A row seeded, or written by an apply's stamp, with nothing in
    /// progress (`provisioning_since_unix` is the apply's own to set).
    fn at_rest(node: SupervisorInfraNode) -> Self {
        Self { node, progress: CopyProgress::default() }
    }
}

/// The columns of a copy's row the supervisor writes and never reads
/// back, as the broker's writes leave them: a stamp's failure, what the
/// change in progress waits on (`set_waiting`, cleared by a stamp that
/// changes the status), and when an apply began provisioning (cleared
/// once the copy leaves provisioning).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyProgress {
    pub failure_stage: Option<weft_broker_client::protocol::FailureStage>,
    pub failure_message: Option<String>,
    pub waiting_on: Option<String>,
    /// The fake has no wall clock: an apply's provisioning stamp writes
    /// [`PROVISIONING_SINCE_UNIX`], and what a test checks is that it is
    /// there or gone.
    pub provisioning_since_unix: Option<i64>,
}

/// What `set_provisioning` stamps as `provisioning_since_unix`.
pub const PROVISIONING_SINCE_UNIX: i64 = 1;

#[derive(Default)]
struct Inner {
    /// All projects under this tenant. Keyed by project_id.
    projects: HashMap<uuid::Uuid, SupervisorProject>,
    /// Tenant id this fake is scoped to. Set on construction;
    /// `projects_for_tenant(other)` returns empty.
    tenant_id: String,

    /// Infra nodes keyed by (project_id, node_id, instance): one per copy.
    infra_nodes: HashMap<CopyKey, CopyRow>,

    /// Where each project's health rules stand, as `set_health_state`
    /// last wrote it.
    health_state: HashMap<uuid::Uuid, Vec<HealthRuleState>>,

    /// How each health command `enqueue_lifecycle` queued ended: every one
    /// succeeds as it is queued, unless `health_commands_run_on`.
    health_endings: HashMap<i64, weft_broker_client::protocol::HealthCommandEnding>,

    /// The health commands `enqueue_lifecycle` queues keep running until a
    /// test ends them (`end_health_command`).
    health_commands_run_on: bool,

    /// Every issued lifecycle command, oldest first; tests push via
    /// `enqueue_command`. A command stays here after it is claimed, as
    /// the broker's row does: `completed_commands` is what retires it.
    commands: Vec<SupervisorCommandRow>,

    /// Commands whose `set_waiting` answers only once released, so a test
    /// can hold a command mid-run.
    held_commands: HashMap<i64, Arc<tokio::sync::Semaphore>>,

    /// Per-project uncompleted supervisor commands returned by
    /// `infra_commands_in_flight`. Absent = none.
    infra_commands_in_flight: HashMap<uuid::Uuid, Vec<weft_broker_client::protocol::InFlightCommand>>,

    /// Per-(project, node) image tag map returned by `project_image_tags`.
    image_tags: HashMap<(uuid::Uuid, String), HashMap<String, String>>,

    /// Completed lifecycle commands (id, optional error message,
    /// cancelled flag).
    completed_commands: Vec<(i64, Option<String>, bool)>,

    /// Commands a person asked to cancel: handed out flagged, and
    /// answered as a cancel to the held claim of the process running them.
    cancel_requested: std::collections::HashSet<i64>,

    /// How many of the next `command_complete` calls fail, as a broker
    /// that cannot answer would.
    completions_failing: usize,

    /// How many of the next `owned_projects` calls fail, as a broker
    /// whose read errors does.
    owned_reads_failing: usize,

    /// Every `enqueue_lifecycle` records its call and then never
    /// answers, as a broker that hangs would.
    hang_enqueues: bool,

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

    /// The oldest uncompleted command of an owned project, not in `busy`,
    /// that no older uncompleted command of its project reaches a copy of,
    /// as the broker's `next_command` picks it, flagged when a person
    /// asked to cancel it. A single supervisor: a command it claimed
    /// before and does not name busy is its to run again.
    fn next_command(&self, busy: &[i64]) -> Option<SupervisorCommandRow> {
        let waiting: Vec<&SupervisorCommandRow> = self.commands.iter().filter(|c| !self.completed(c.id)).collect();
        waiting
            .iter()
            .find(|c| {
                self.owns(c.project_id)
                    && !busy.contains(&c.id)
                    && !waiting.iter().any(|o| o.project_id == c.project_id && o.id < c.id && o.overlaps(c))
            })
            .map(|c| SupervisorCommandRow { cancel_requested: self.cancel_requested.contains(&c.id), ..(*c).clone() })
    }

    /// Whether a supervisor may own `project_id`, as the broker's
    /// `ownable_project` answers it: infra to manage, infra copies, copies
    /// on the host (`held`), or an uncompleted command waiting on it.
    fn ownable(&self, project_id: uuid::Uuid, held: &[uuid::Uuid]) -> bool {
        !self.infraless.contains(&project_id) || self.has_infra_nodes(project_id) || held.contains(&project_id) || self.has_work(project_id)
    }

    /// Whether `project_id` has infra copies, as the broker's
    /// `has_infra_nodes` answers it.
    fn has_infra_nodes(&self, project_id: uuid::Uuid) -> bool {
        self.infra_nodes.keys().any(|k| k.0 == project_id)
    }

    /// Whether `project_id` gives its owner something to do now that
    /// nothing will announce, as the broker's `supervisor_work` answers it:
    /// an uncompleted command.
    fn has_work(&self, project_id: uuid::Uuid) -> bool {
        self.commands.iter().any(|c| c.project_id == project_id && !self.completed(c.id))
    }

    /// Whether an uncompleted command waits on a project nobody holds.
    fn unowned_work_waiting(&self) -> bool {
        self.commands.iter().any(|c| {
            !self.completed(c.id) && self.owners.get(&c.project_id) == Some(&Owner::Nobody)
        })
    }

    /// The uncompleted commands of `project_id` that reach the copy of
    /// `node_id` owned by `instance`: the issued ones not yet completed,
    /// and the ones a test scripted in flight (`set_infra_commands_in_flight`),
    /// both standing for the broker's uncompleted `infra_lifecycle_command`
    /// rows. `Some(id)` keeps command `id` alone.
    fn uncompleted_reaching(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        id: Option<i64>,
    ) -> bool {
        let issued = self.commands.iter().any(|c| {
            id.is_none_or(|id| c.id == id)
                && c.project_id == project_id
                && !self.completed(c.id)
                && weft_broker_client::protocol::InFlightCommand { node_id: c.node_id.clone(), copies: c.copies.clone() }
                    .reaches(node_id, instance)
        });
        let scripted = id.is_none()
            && self
                .infra_commands_in_flight
                .get(&project_id)
                .is_some_and(|commands| commands.iter().any(|c| c.reaches(node_id, instance)));
        issued || scripted
    }

    /// Stamp `status` on a copy, per unit or node-wide, under the broker's
    /// fence (`weft_broker::lifecycle_writes::set_status`, the one path
    /// both `set_status` and `set_unit_health` take there): the project
    /// still owned (else Displaced), the copy's row there, the unit in its
    /// roster for a per-unit stamp, and either `command` still uncompleted
    /// and reaching the copy or, with no command (the health reconcile),
    /// no uncompleted command reaching it (else Gone). Per unit, the
    /// unit's status and health windows are set and the node status rolls
    /// up; node-wide, every unit and the node take the status directly (so
    /// a unit-less roster still takes a Failed / Terminating stamp). The
    /// row's other columns change as the broker's write changes them: the
    /// failure becomes `failure` (none clears it), what it waits on goes
    /// when the node status changes, and the provisioning start goes once
    /// the node is not provisioning.
    #[allow(clippy::too_many_arguments)]
    fn stamp(
        &mut self,
        command: Option<i64>,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure: (Option<weft_broker_client::protocol::FailureStage>, Option<&str>),
        not_ready_since_unix: Option<i64>,
        ready_since_unix: Option<i64>,
    ) -> weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse> {
        use weft_broker_client::WriteOutcome;
        if !self.owns(project_id) {
            return WriteOutcome::Displaced;
        }
        let commands_pass = match command {
            Some(id) => self.uncompleted_reaching(project_id, node_id, instance, Some(id)),
            None => !self.uncompleted_reaching(project_id, node_id, instance, None),
        };
        if !commands_pass {
            return WriteOutcome::Gone;
        }
        let Some(CopyRow { node, progress }) = self.infra_nodes.get_mut(&copy_key(project_id, node_id, instance)) else {
            return WriteOutcome::Gone;
        };
        let before = node.status;
        match unit {
            Some(u) => {
                let Some(ur) = node.units.get_mut(u) else {
                    return WriteOutcome::Gone;
                };
                ur.status = status;
                ur.not_ready_since_unix = not_ready_since_unix;
                ur.ready_since_unix = ready_since_unix;
                node.status = weft_broker_client::protocol::InfraNodeStatus::rollup(node.units.values().map(|u| &u.status));
            }
            None => {
                for ur in node.units.values_mut() {
                    ur.status = status;
                }
                node.status = status;
            }
        }
        progress.failure_stage = failure.0;
        progress.failure_message = failure.1.map(str::to_string);
        if node.status != before {
            progress.waiting_on = None;
        }
        if node.status != weft_broker_client::protocol::InfraNodeStatus::Provisioning {
            progress.provisioning_since_unix = None;
        }
        WriteOutcome::Applied(weft_broker_client::protocol::SupervisorSetStatusResponse {})
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
                health_down: false,
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

    /// Flip `health_down` on a seeded project. Lets a test
    /// distinguish "the health loop parked something still down" (true)
    /// from "a person deactivated" (false) when exercising the
    /// auto-recover gate.
    pub fn set_health_down(&self, project_id: uuid::Uuid, parked: bool) {
        if let Some(p) = self.inner.lock().projects.get_mut(&project_id) {
            p.health_down = parked;
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
        endpoints: BTreeMap<String, weft_core::infra::EndpointRecord>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) {
        self.inner.lock().infra_nodes.insert(
            copy_key(project_id, node_id, None),
            CopyRow::at_rest(SupervisorInfraNode {
                node_id: node_id.to_string(),
                copy_id: copy_id.to_string(),
                status,
                applied_spec_hash,
                applied_at_unix: None,
                addresses: weft_broker_client::protocol::AppliedEndpoints {
                    urls: endpoints,
                    public_paths: BTreeMap::new(),
                    doors: BTreeMap::new(),
                },
                keep_disks: Vec::new(),
                units,
                instance: None,
                health_rules: None,
            }),
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
            CopyRow::at_rest(SupervisorInfraNode {
                node_id: node_id.to_string(),
                copy_id: copy_id.to_string(),
                status,
                applied_spec_hash: None,
                applied_at_unix: None,
                addresses: Default::default(),
                keep_disks: Vec::new(),
                units,
                instance: Some(instance.clone()),
                health_rules: None,
            }),
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
        node.node.keep_disks = keep_disks;
    }

    /// The health rules a seeded copy's last apply stamped (what its node
    /// wrote: `None` for none, so it runs the built-in ones).
    pub fn set_health_rules(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        rules: Option<Vec<weft_core::infra::health::HealthRule>>,
    ) {
        let mut inner = self.inner.lock();
        let node = inner
            .infra_nodes
            .get_mut(&copy_key(project_id, node_id, instance))
            .expect("set_health_rules on a seeded row");
        node.node.health_rules = rules;
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

    /// Hold command `command_id` at its next `set_waiting` (a stop's or a
    /// terminate's, before the host works on a copy) until
    /// [`Self::release_command`]: it stays mid-run for as long as the test
    /// needs it running.
    pub fn hold_command(&self, command_id: i64) {
        self.inner.lock().held_commands.insert(command_id, Arc::new(tokio::sync::Semaphore::new(0)));
    }

    /// Let every held and later `set_waiting` of `command_id` answer.
    pub fn release_command(&self, command_id: i64) {
        if let Some(gate) = self.inner.lock().held_commands.get(&command_id) {
            gate.close();
        }
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

    /// All `set_status` and `set_unit_health` calls in order, as
    /// `(project_id, node_id, instance, status)`.
    pub fn status_writes(
        &self,
    ) -> Vec<(uuid::Uuid, String, Option<weft_core::instance::InstanceId>, weft_broker_client::protocol::InfraNodeStatus)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                BrokerCall::SetStatus { project_id, node_id, instance, status, .. }
                | BrokerCall::SetUnitHealth { project_id, node_id, instance, status, .. } => {
                    Some((*project_id, node_id.clone(), instance.clone(), *status))
                }
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
            .map(|row| row.node.clone())
    }

    /// What the broker's writes left in a copy's row beside what the
    /// supervisor reads ([`CopyProgress`]).
    pub fn copy_progress(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> Option<CopyProgress> {
        self.inner.lock().infra_nodes.get(&copy_key(project_id, node_id, instance)).map(|row| row.progress.clone())
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

    /// Script the user's `/infra/cancel`: the command is handed out
    /// flagged, and a claim held by the process running it wakes and
    /// answers the cancel, as the broker's does on `cancel:<id>`.
    pub fn set_cancel_requested(&self, command_id: i64) {
        self.inner.lock().cancel_requested.insert(command_id);
        self.issued.notify_waiters();
    }

    /// Make the next `count` calls of `command_complete` fail, as a broker
    /// that cannot answer does.
    pub fn fail_completions(&self, count: usize) {
        self.inner.lock().completions_failing = count;
    }

    /// Make the next `count` calls of `owned_projects` fail, as a broker
    /// whose read errors does.
    pub fn fail_owned_reads(&self, count: usize) {
        self.inner.lock().owned_reads_failing = count;
    }

    /// Make every `enqueue_lifecycle` record its call and then never
    /// return, so a test can hold a health rule's action mid-flight.
    pub fn hang_lifecycle_enqueues(&self) {
        self.inner.lock().hang_enqueues = true;
    }

    /// Keep every health command queued from now on running, until
    /// `end_health_command` ends it.
    pub fn keep_health_commands_running(&self) {
        self.inner.lock().health_commands_run_on = true;
    }

    /// End health command `command` with `outcome` (and its `message`).
    pub fn end_health_command(&self, command: i64, outcome: weft_broker_client::protocol::LifecycleOutcome, message: Option<&str>) {
        let ending = weft_broker_client::protocol::HealthCommandEnding { command, outcome, message: message.map(str::to_string) };
        self.inner.lock().health_endings.insert(command, ending);
    }

    /// What `set_health_state` last wrote for the project.
    pub fn health_state_rows(&self, project_id: uuid::Uuid) -> Vec<HealthRuleState> {
        self.inner.lock().health_state.get(&project_id).cloned().unwrap_or_default()
    }

    /// Seed where the project's health rules stand.
    pub fn set_health_state_rows(&self, project_id: uuid::Uuid, rules: Vec<HealthRuleState>) {
        self.inner.lock().health_state.insert(project_id, rules);
    }

    /// The projects syncs reported failed (tried again), in order, one
    /// entry per sync that reported it.
    pub fn failed(&self) -> Vec<uuid::Uuid> {
        self.troubles(|failed, _| failed)
    }

    /// The projects syncs reported a health rule gave up on, in
    /// order, one entry per sync that reported it.
    pub fn gave_up(&self) -> Vec<uuid::Uuid> {
        self.troubles(|_, gave_up| gave_up)
    }

    fn troubles(&self, which: for<'a> fn(&'a [ProjectTrouble], &'a [ProjectTrouble]) -> &'a [ProjectTrouble]) -> Vec<uuid::Uuid> {
        self.inner
            .lock()
            .calls
            .iter()
            .flat_map(|c| match c {
                BrokerCall::SyncOwnership { failed, gave_up, .. } => which(failed, gave_up).iter().map(|t| t.project_id).collect(),
                _ => Vec::new(),
            })
            .collect()
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
    async fn sync_ownership(&self, req: &SupervisorSyncOwnershipRequest) -> Result<SupervisorSyncOwnershipResponse> {
        // A single supervisor: it takes every project nobody holds that
        // it may own (`ownable`) and renews what it owns while it may
        // still own it; a lease it stops renewing lapses (at once here,
        // after the lease time on the broker). Ending a pass gives every
        // lease back. Partitioning between supervisors, the failures'
        // retries and the booking of the next pass are covered by the
        // broker's SQL-level tests. The owned set is every lease it holds,
        // as the broker's is.
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SyncOwnership {
            replica: req.replica.clone(),
            held_projects: req.held_projects.clone(),
            looked: req.looked.clone(),
            settled: req.settled.clone(),
            failed: req.failed.clone(),
            gave_up: req.gave_up.clone(),
            all_failed: req.all_failed.clone(),
            ending: req.ending,
        });
        let held_projects: &[uuid::Uuid] = req.held_projects.as_deref().unwrap_or_default();
        let mut claimed = Vec::new();
        match req.ending {
            true => {
                let given_back: Vec<uuid::Uuid> = inner.projects.keys().filter(|id| inner.owns(**id)).copied().collect();
                for id in given_back {
                    inner.owners.insert(id, Owner::Nobody);
                }
            }
            false => {
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
                claimed = taken.into_iter().filter(|id| inner.projects.contains_key(id)).collect();
            }
        }
        let owned = inner.projects.values().filter(|p| inner.owns(p.project_id)).cloned().collect();
        Ok(SupervisorSyncOwnershipResponse { owned, claimed, next_due_unix: None })
    }

    async fn owned_projects(&self, replica: &str) -> Result<Vec<SupervisorProject>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::OwnedProjects { replica: replica.to_string() });
        if inner.owned_reads_failing > 0 {
            inner.owned_reads_failing -= 1;
            anyhow::bail!("the owned projects did not read");
        }
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
                !inner.infra_nodes.values().any(|row| row.node.copy_id == c.copy_id)
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
            .map(|(_, row)| row.node.clone())
            .collect())
    }

    async fn health_state(&self, project_id: uuid::Uuid) -> Result<weft_broker_client::protocol::SupervisorHealthStateResponse> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::HealthState { project_id });
        let rules = inner.health_state.get(&project_id).cloned().unwrap_or_default();
        let mut ended: Vec<weft_broker_client::protocol::HealthCommandEnding> = rules
            .iter()
            .filter_map(|r| r.queued.and_then(|q| inner.health_endings.get(&q.command).cloned()))
            .collect();
        ended.sort_by_key(|e| e.command);
        ended.dedup();
        Ok(weft_broker_client::protocol::SupervisorHealthStateResponse { rules, ended })
    }

    async fn set_health_state(
        &self,
        req: &SupervisorSetHealthStateRequest,
    ) -> Result<weft_broker_client::WriteOutcome<SupervisorSetHealthStateResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetHealthState { project_id: req.project_id, rules: req.rules.clone() });
        if !inner.owns(req.project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        inner.health_state.insert(req.project_id, req.rules.clone());
        Ok(weft_broker_client::WriteOutcome::Applied(SupervisorSetHealthStateResponse {}))
    }

    /// The broker's held claim: the busy commands a person asked to
    /// cancel (none of `cancelling`), else the next command, else a hold
    /// (on the tokio clock) until one is issued or cancelled or `wait`
    /// ends. A wake that finds nothing for this supervisor answers
    /// `UnownedWork` when a command waits on a project nobody holds, as
    /// the broker does after a wake.
    async fn claim_command(
        &self,
        claimer: &str,
        busy_commands: &[i64],
        cancelling: &[i64],
        wait: std::time::Duration,
    ) -> Result<SupervisorClaim> {
        self.inner.lock().calls.push(BrokerCall::ClaimCommand {
            claimer: claimer.to_string(),
            busy_commands: busy_commands.to_vec(),
            cancelling: cancelling.to_vec(),
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
                let cancels: Vec<i64> = busy_commands
                    .iter()
                    .filter(|id| inner.cancel_requested.contains(id) && !cancelling.contains(id))
                    .copied()
                    .collect();
                if !cancels.is_empty() {
                    return Ok(SupervisorClaim::Cancel { commands: cancels });
                }
                if let Some(cmd) = inner.next_command(busy_commands) {
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
        command_id: i64,
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
        // `BrokerSupervisorClient`'s `set_status` always sends the
        // health windows empty.
        Ok(inner.stamp(Some(command_id), project_id, node_id, instance, unit, status, (failure_stage, failure_message), None, None))
    }

    async fn set_unit_health(
        &self,
        _replica: &str,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        unit: &str,
        status: weft_broker_client::protocol::InfraNodeStatus,
        not_ready_since_unix: Option<i64>,
        ready_since_unix: Option<i64>,
        restart: Option<&weft_core::infra::LastRestart>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetUnitHealth {
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            unit: unit.to_string(),
            status,
            not_ready_since_unix,
            ready_since_unix,
            restart: restart.cloned(),
        });
        // The broker's `set_unit_health` stamps through the same write as
        // `set_status`, with no failure; a restart it carries lands on the
        // unit with it.
        let stamped = inner.stamp(None, project_id, node_id, instance, Some(unit), status, (None, None), not_ready_since_unix, ready_since_unix);
        if let (Some(restart), weft_broker_client::WriteOutcome::Applied(_)) = (restart, &stamped) {
            if let Some(ur) = inner.infra_nodes.get_mut(&copy_key(project_id, node_id, instance)).and_then(|row| row.node.units.get_mut(unit)) {
                ur.last_restart = Some(restart.clone());
            }
        }
        Ok(stamped)
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
        let gate = {
            let mut inner = self.inner.lock();
            inner.calls.push(BrokerCall::SetWaiting {
                command_id,
                project_id,
                node_id: node_id.to_string(),
                instance: instance.cloned(),
                waiting: waiting.to_string(),
            });
            inner.held_commands.get(&command_id).cloned()
        };
        if let Some(gate) = gate {
            // Releasing the gate closes it: every acquire then returns.
            let _ = gate.acquire().await;
        }
        let mut inner = self.inner.lock();
        if !inner.owns(project_id) {
            return Ok(weft_broker_client::WriteOutcome::Displaced);
        }
        // As the broker's write: the copy's row, reached by the command
        // still uncompleted, else nothing matches (Gone).
        let reaching = inner.uncompleted_reaching(project_id, node_id, instance, Some(command_id));
        let Some(row) = inner.infra_nodes.get_mut(&copy_key(project_id, node_id, instance)).filter(|_| reaching) else {
            return Ok(weft_broker_client::WriteOutcome::Gone);
        };
        row.progress.waiting_on = Some(waiting.to_string());
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
        if inner.completions_failing > 0 {
            inner.completions_failing -= 1;
            anyhow::bail!("the broker could not answer");
        }
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
        health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
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
        // A failure and what an earlier change waited on go; the
        // provisioning start is stamped.
        inner.infra_nodes.insert(
            copy_key(project_id, node_id, instance),
            CopyRow {
                node: SupervisorInfraNode {
                    node_id: node_id.to_string(),
                    copy_id: copy_id.to_string(),
                    status: weft_broker_client::protocol::InfraNodeStatus::Provisioning,
                    applied_spec_hash: None,
                    applied_at_unix: None,
                    addresses: Default::default(),
                    keep_disks,
                    units,
                    instance: instance.cloned(),
                    health_rules,
                },
                progress: CopyProgress { provisioning_since_unix: Some(PROVISIONING_SINCE_UNIX), ..CopyProgress::default() },
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
        health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
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
        // A failure, what the apply waited on and its provisioning start
        // all go.
        inner.infra_nodes.insert(
            copy_key(project_id, node_id, instance),
            CopyRow::at_rest(SupervisorInfraNode {
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
                health_rules,
            }),
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
        let (id, hang) = {
            let mut inner = self.inner.lock();
            let id = inner.calls.len() as i64 + 1;
            inner.calls.push(BrokerCall::EnqueueLifecycle {
                project_id,
                spec,
            });
            if !inner.health_commands_run_on {
                let ending = weft_broker_client::protocol::HealthCommandEnding {
                    command: id,
                    outcome: weft_broker_client::protocol::LifecycleOutcome::Succeeded,
                    message: None,
                };
                inner.health_endings.insert(id, ending);
            }
            (id, inner.hang_enqueues)
        };
        if hang {
            std::future::pending::<()>().await;
        }
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
        let req = SupervisorSyncOwnershipRequest {
            replica: "sup-1".into(),
            held_projects: Some(vec![]),
            looked: vec![],
            settled: vec![],
            failed: vec![],
            gave_up: vec![],
            all_failed: None,
            ending: false,
        };
        let synced = b.sync_ownership(&req).await.unwrap();
        assert_eq!(synced.owned.len(), 1);
        assert!(synced.claimed.is_empty(), "a seeded project is already owned");
        // Ending a pass gives the lease back.
        let ended = b.sync_ownership(&SupervisorSyncOwnershipRequest { ending: true, ..req }).await.unwrap();
        assert!(ended.owned.is_empty());
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
        // A health window the unit was in: a per-unit stamp clears it,
        // as the broker's does.
        b.set_unit_health(
            "sup-1",
            P1,
            "n1",
            None,
            "n1",
            weft_broker_client::protocol::InfraNodeStatus::Provisioning,
            Some(10),
            Some(20),
            None,
        )
        .await
        .unwrap();
        b.enqueue_command(stop(1, P1));
        b.set_status(
            "sup-1",
            1,
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
        let unit = &node.units["n1"];
        assert_eq!((unit.not_ready_since_unix, unit.ready_since_unix), (None, None));
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
            spec_json: None,
            force: false,
            copies: weft_core::instance::Copies::Shared,
            cancel_requested: false,
        }
    }

    async fn claim(b: &FakeBroker, busy: &[i64]) -> SupervisorClaim {
        b.claim_command("sup-1", busy, &[], std::time::Duration::ZERO).await.unwrap()
    }

    #[tokio::test]
    async fn a_command_stays_claimable_until_completed() {
        let b = FakeBroker::new("alice");
        b.enqueue_command(stop(1, P1));
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Command(c) if c.id == 1));
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Command(c) if c.id == 1));
        assert!(matches!(claim(&b, &[1]).await, SupervisorClaim::Nothing), "a command already running here is not handed out again");
        b.command_complete("sup-1", 1, None, false).await.unwrap();
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Nothing));
    }

    /// A stamp passes the broker's whole fence: a health write while a
    /// command reaches the copy is Gone, and so is a command's stamp once
    /// that command completed or when it was never issued; with the
    /// command done, the health write goes through.
    #[tokio::test]
    async fn a_stamp_is_refused_while_the_fence_says_no() {
        use weft_broker_client::protocol::InfraNodeStatus as Status;
        use weft_broker_client::WriteOutcome;
        let b = FakeBroker::new("alice");
        b.add_infra_node(P1, "n1", "inst1", Status::Running);
        let health = || b.set_unit_health("sup-1", P1, "n1", None, "n1", Status::Flaky, Some(10), None, None);
        let stamp = |command| b.set_status("sup-1", command, P1, "n1", None, Some("n1"), Status::Stopping, None, None);
        assert!(matches!(stamp(1).await.unwrap(), WriteOutcome::Gone), "a command never issued stamps nothing");
        b.enqueue_command(stop(1, P1));
        assert!(matches!(health().await.unwrap(), WriteOutcome::Gone), "no health write while a command is in flight");
        assert_eq!(b.infra_node(P1, "n1").unwrap().status, Status::Running);
        assert!(matches!(stamp(1).await.unwrap(), WriteOutcome::Applied(_)));
        b.command_complete("sup-1", 1, None, false).await.unwrap();
        assert!(matches!(stamp(1).await.unwrap(), WriteOutcome::Gone), "a completed command stamps nothing more");
        assert!(matches!(health().await.unwrap(), WriteOutcome::Applied(_)));
        assert_eq!(b.infra_node(P1, "n1").unwrap().status, Status::Flaky);
    }

    /// A stamp changes the rest of the copy's row as the broker's does:
    /// the failure is the stamp's own (none clears it), what the change
    /// waits on stays while the status does and goes once it moves, and
    /// the provisioning start goes once the copy leaves provisioning.
    #[tokio::test]
    async fn a_stamp_writes_its_failure_and_clears_what_the_change_left() {
        use weft_broker_client::protocol::{FailureStage, InfraNodeStatus as Status};
        use weft_broker_client::WriteOutcome;
        let b = FakeBroker::new("alice");
        b.enqueue_command(SupervisorCommandRow { verb: weft_broker_client::protocol::InfraLifecycleVerb::Apply, ..stop(1, P1) });
        let units = BTreeMap::from([("n1".to_string(), unit_runtime(Status::Provisioning))]);
        let provisioned = b.set_provisioning("sup-1", 1, P1, "n1", None, "inst1", Vec::new(), None, units).await.unwrap();
        assert!(matches!(provisioned, WriteOutcome::Applied(_)));
        let progress = || b.copy_progress(P1, "n1", None).unwrap();
        assert_eq!(progress(), CopyProgress { provisioning_since_unix: Some(PROVISIONING_SINCE_UNIX), ..CopyProgress::default() });
        assert!(matches!(b.set_waiting("sup-1", 1, P1, "n1", None, "the image").await.unwrap(), WriteOutcome::Applied(_)));
        let stamp = |status, failure: Option<(FailureStage, &'static str)>| {
            b.set_status("sup-1", 1, P1, "n1", None, Some("n1"), status, failure.map(|f| f.0), failure.map(|f| f.1))
        };

        assert!(matches!(stamp(Status::Provisioning, None).await.unwrap(), WriteOutcome::Applied(_)));
        assert_eq!(progress().waiting_on.as_deref(), Some("the image"), "the status did not move");
        assert_eq!(progress().provisioning_since_unix, Some(PROVISIONING_SINCE_UNIX), "still provisioning");

        assert!(matches!(stamp(Status::Failed, Some((FailureStage::Apply, "boom"))).await.unwrap(), WriteOutcome::Applied(_)));
        assert_eq!(
            progress(),
            CopyProgress { failure_stage: Some(FailureStage::Apply), failure_message: Some("boom".into()), ..CopyProgress::default() },
            "the failure is written, and what the apply waited on and when it began provisioning go"
        );

        assert!(matches!(stamp(Status::Running, None).await.unwrap(), WriteOutcome::Applied(_)));
        assert_eq!(progress(), CopyProgress::default(), "a stamp with no failure clears it");

        b.command_complete("sup-1", 1, None, false).await.unwrap();
        assert!(matches!(b.set_waiting("sup-1", 1, P1, "n1", None, "late").await.unwrap(), WriteOutcome::Gone), "a completed command records nothing");
        assert_eq!(progress().waiting_on, None);
    }

    /// A cancel flags the command it is handed out with, and answers the
    /// claim of the process running it once, until it says it heard.
    #[tokio::test]
    async fn a_cancel_is_handed_out_and_answered_to_its_runner() {
        let b = FakeBroker::new("alice");
        b.enqueue_command(stop(1, P1));
        b.set_cancel_requested(1);
        assert!(matches!(claim(&b, &[]).await, SupervisorClaim::Command(c) if c.cancel_requested));
        assert!(matches!(claim(&b, &[1]).await, SupervisorClaim::Cancel { commands } if commands == vec![1]));
        let heard = b.claim_command("sup-1", &[1], &[1], std::time::Duration::ZERO).await.unwrap();
        assert!(matches!(heard, SupervisorClaim::Nothing));
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
        let held = b.claim_command("sup-1", &[], &[], wait);
        tokio::pin!(held);
        assert!(futures::poll!(held.as_mut()).is_pending(), "nothing to hand out: the claim holds");
        b.enqueue_command(stop(1, P1));
        assert!(matches!(held.await.unwrap(), SupervisorClaim::UnownedWork));
        // Without a wake the hold ends on its own, answering Nothing.
        assert!(matches!(
            b.claim_command("sup-1", &[], &[], wait).await.unwrap(),
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

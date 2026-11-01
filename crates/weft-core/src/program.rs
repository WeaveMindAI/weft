//! What a program may ask of a project, beyond its own run: everything
//! a person can do to one from the CLI, on the server, with the same
//! parameters and the same refusals. Each area's calls are one enum in a
//! module here ([`infra`], [`triggers`], [`instances`], [`runs`],
//! [`versions`]); the ctx builders (`ctx.infra(..)`, `ctx.trigger(..)`,
//! `ctx.values()`, `ctx.runs()`, `ctx.project(..)` and the rest) each build
//! one [`ProgramCall`]; the runtime carries it to the dispatcher as one
//! durable task and hands back its answer.
//!
//! A call names the project it acts on, or acts on the calling run's own
//! project ([`ProgramCallPayload::project`]): a program may act on any
//! project of the same owner. Inside a project any instance may be named
//! from any run, because the program is the author's own code; instance
//! limits are enforced at the outside doors (an instance token, the
//! `Weft-Instance` header), not here.
//!
//! A call that takes something down carries a [`TakeDownChoice`] (what
//! happens at the door, to what is held and to what is running; the rest
//! is the command's default) and a [`StopSelf`]: the calling run is never
//! among the runs a take-down reaches, and `StopSelf::Include` stops it
//! too, after the take-down is queued.
//!
//! [`TakeDownChoice`]: crate::take_down::TakeDownChoice
//! [`StopSelf`]: crate::tag::StopSelf

pub mod infra;
pub mod instances;
pub mod project;
pub mod runs;
pub mod settings;
pub mod triggers;
pub mod versions;

use serde::{Deserialize, Serialize};

use crate::instance::InstanceId;
use crate::tag::StopSelf;

/// One call a program makes on a project, by area.
// SYNC: ProgramCall <-> crates/weft-dispatcher/src/actions/mod.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "area", rename_all = "snake_case")]
pub enum ProgramCall {
    Infra(infra::InfraCall),
    Triggers(triggers::TriggerCall),
    Instances(instances::InstanceCall),
    Runs(runs::RunCall),
    Versions(versions::VersionCall),
    Project(project::ProjectCall),
    Settings(settings::SettingsCall),
}

impl ProgramCall {
    /// Whether the call can take the calling run down with it, so it
    /// carries a [`StopSelf`] that matters.
    pub fn takes_down(&self) -> bool {
        match self {
            ProgramCall::Infra(call) => call.takes_down(),
            ProgramCall::Triggers(call) => call.takes_down(),
            ProgramCall::Runs(call) => call.takes_down(),
            ProgramCall::Instances(_) | ProgramCall::Versions(_) | ProgramCall::Project(_) | ProgramCall::Settings(_) => false,
        }
    }

    /// The name the call is journaled under (`ctx.run`), so a replayed
    /// run gets the recorded answer instead of asking again.
    pub fn journal_name(&self) -> &'static str {
        match self {
            ProgramCall::Infra(call) => call.journal_name(),
            ProgramCall::Triggers(call) => call.journal_name(),
            ProgramCall::Instances(call) => call.journal_name(),
            ProgramCall::Runs(call) => call.journal_name(),
            ProgramCall::Versions(call) => call.journal_name(),
            ProgramCall::Project(call) => call.journal_name(),
            ProgramCall::Settings(call) => call.journal_name(),
        }
    }

    /// Every call's journal name, area by area: what the dispatcher's list
    /// of actions is held to (`weft_dispatcher::actions::NAMES`).
    pub fn journal_names() -> Vec<&'static str> {
        [
            infra::InfraCall::JOURNAL_NAMES,
            triggers::TriggerCall::JOURNAL_NAMES,
            instances::InstanceCall::JOURNAL_NAMES,
            runs::RunCall::JOURNAL_NAMES,
            versions::VersionCall::JOURNAL_NAMES,
            project::ProjectCall::JOURNAL_NAMES,
            settings::SettingsCall::JOURNAL_NAMES,
        ]
        .concat()
    }
}

/// Namespace of program calls' ids: generated once and frozen.
const PROGRAM_CALL_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x5e1f_93c2_7a0d_4b6e_9f24_c8a1_3d7b_e092);

/// The id of the call a run makes at `step`, the place in the run that
/// makes it (the run, its node, the node's loop frames and the call's
/// index there, as the worker spells them in the call's task's dedup key,
/// which a replay reaches the same): the same however often the call is
/// issued again, so what the call makes (a run it starts) is made once.
/// Derived by the install from the task's own key, never taken from what
/// the worker says.
pub fn program_call_id(step: &str) -> uuid::Uuid {
    uuid::Uuid::new_v5(&PROGRAM_CALL_NAMESPACE, step.as_bytes())
}

/// The durable task that carries a [`ProgramCall`]: who asked, what
/// happens to the asker when the call takes it down, and the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramCallPayload {
    /// The calling run (its run).
    pub by: uuid::Uuid,
    pub stop_self: StopSelf,
    /// The project the call acts on, one of the same owner's; the calling
    /// run's own when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<uuid::Uuid>,
    pub call: ProgramCall,
}

/// What a call answered, and whether the asker is being stopped by it
/// (`StopSelf::Include` on a take-down): that run then waits for its own
/// cancel and never returns from the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramCallOutcome {
    pub value: serde_json::Value,
    #[serde(default)]
    pub stops_asker: bool,
}

/// One copy of an infra node, as `ctx.infra(..).status()` and
/// `.copies()` answer it.
// SYNC: InfraCopy <-> packages/weft-graph/src/protocol.ts InfraCopy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraCopy {
    /// The node, spelled the way the program writes it.
    pub node: String,
    /// Whose copy: `None` for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    /// `Provisioning` also while a start is queued but not yet picked
    /// up, `Stopping` while a stop is.
    pub status: crate::infra::InfraNodeStatus,
    /// Why it failed, when it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

impl InfraCopy {
    /// Whether the copy answers now.
    pub fn is_running(&self) -> bool {
        self.status == crate::infra::InfraNodeStatus::Running
    }
}

/// Fires of one trigger that wait in its queue on something no clock
/// brings ([`crate::door_fire::WaitsOn`]): a value its instance has not
/// given (or gave an invalid one), infra the run reads, connections the
/// install has not picked, or a hand-over that kept failing, which holds
/// the trigger's queue. What they wait on hands them over again, and so
/// does activating the trigger.
// SYNC: WaitingFires <-> packages/weft-graph/src/protocol.ts WaitingFires, crates/weft-core/src/door_fire.rs UnclockedWait (on)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingFires {
    pub fires: u32,
    /// What the fire parked or stopped last waits on.
    pub on: crate::door_fire::UnclockedWait,
    /// Why, as it was said: an instance's refusal names the field
    /// (`instance 'ada' at 'answer': 'key' is not filled`), a failure is
    /// its error.
    pub reason: String,
}

/// What `InfraCall::Start` answers: a start queued, one already
/// under way or done, or none yet for a passing reason (a build in
/// progress, a trigger of the copy mid-activation), which
/// `ctx.infra(..).start()` and `.request_start()` ask again at their next
/// look.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
pub enum InfraStartAnswer {
    Started,
    AlreadyRunning,
    AlreadyStarting,
    Waiting { reason: String },
}

impl InfraStartAnswer {
    /// Whether weft accepted the start (it is under way or done): every
    /// answer but `Waiting`.
    pub fn accepted(&self) -> bool {
        !matches!(self, InfraStartAnswer::Waiting { .. })
    }
}

/// What `InfraCall::Stop` and `InfraCall::Terminate`
/// answer: the copy was taken down by this call, it was already down (or
/// on its way down), or there is no copy of that node for that instance,
/// which is also what a copy terminated earlier reads as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "copy", rename_all = "snake_case")]
pub enum InfraDownAnswer {
    TakenDown,
    AlreadyDown,
    NoCopy,
}

/// What `InstanceCall::ConnectionsForget` answers: how many connections
/// went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionsForgotten {
    pub forgotten: u64,
}

/// What `InstanceCall::TokensRevoke` answers: how many tokens went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokensRevoked {
    pub revoked: u64,
}

/// One instance's activation of a trigger that exists once per instance.
// SYNC: InstanceTrigger <-> packages/weft-graph/src/protocol.ts InstanceTrigger
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceTrigger {
    /// The trigger, spelled the way the program writes it.
    pub trigger: String,
    pub mode: crate::activation::ActivationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<WaitingFires>,
}

/// Everything weft holds for one instance of the project, as
/// `ctx.instances().list()`, `weft instances` and the editor's instance
/// picker read it: counts and states, never a value or a secret.
// SYNC: InstanceHoldings <-> packages/weft-graph/src/protocol.ts InstanceHoldings
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceHoldings {
    pub instance: InstanceId,
    /// Values the instance gave for `@instance_filled` fields.
    pub values: u32,
    /// Connections the instance made in this project.
    pub connections: u32,
    /// Instance tokens scoped to it that still work.
    pub tokens: u32,
    /// Its copies of `@per_instance` infra nodes.
    pub copies: Vec<InfraCopy>,
    /// Its activations of per-instance triggers.
    pub triggers: Vec<InstanceTrigger>,
    /// When its latest run started (unix seconds); `None` when it has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_unix: Option<i64>,
}

impl InstanceHoldings {
    fn empty(instance: InstanceId) -> Self {
        Self { instance, values: 0, connections: 0, tokens: 0, copies: Vec::new(), triggers: Vec::new(), last_run_unix: None }
    }

    /// One entry per instance that appears in any of the sources, sorted by
    /// instance. `values`, `connections` and `tokens` are counts per instance;
    /// a copy without an instance (the shared one) belongs to no instance here.
    pub fn merge(
        values: Vec<(InstanceId, u32)>,
        connections: Vec<(InstanceId, u32)>,
        tokens: Vec<(InstanceId, u32)>,
        copies: Vec<InfraCopy>,
        triggers: Vec<(InstanceId, InstanceTrigger)>,
        last_runs: Vec<(InstanceId, i64)>,
    ) -> Vec<InstanceHoldings> {
        let mut by_instance = std::collections::BTreeMap::<InstanceId, InstanceHoldings>::new();
        fn at(by_instance: &mut std::collections::BTreeMap<InstanceId, InstanceHoldings>, instance: InstanceId) -> &mut InstanceHoldings {
            by_instance.entry(instance.clone()).or_insert_with(|| InstanceHoldings::empty(instance))
        }
        for (instance, n) in values {
            at(&mut by_instance, instance).values += n;
        }
        for (instance, n) in connections {
            at(&mut by_instance, instance).connections += n;
        }
        for (instance, n) in tokens {
            at(&mut by_instance, instance).tokens += n;
        }
        for copy in copies {
            if let Some(instance) = copy.instance.clone() {
                at(&mut by_instance, instance).copies.push(copy);
            }
        }
        for (instance, trigger) in triggers {
            at(&mut by_instance, instance).triggers.push(trigger);
        }
        for (instance, at_unix) in last_runs {
            let held = at(&mut by_instance, instance);
            held.last_run_unix = held.last_run_unix.max(Some(at_unix));
        }
        by_instance.into_values().collect()
    }
}

crate::wire_enum! {
    /// Where a run stands, as a filter asks for it: each word is the
    /// [`RunSummary::status`] a run reads in a listing.
    // SYNC: RunStatus <-> crates/weft-dispatcher/src/store/postgres.rs list_runs (status clause), extension-vscode/src/sidebar/runSearch.ts STATUSES, extension-vscode/src/sidebar/runs.ts RunStatus
    pub enum RunStatus {
        /// Not ended: no terminal event yet. A run parked on a wait has
        /// not ended either, so it is here too.
        Running = "running",
        /// Not ended, and parked on a wait (a resume signal is registered
        /// for it): the listing's `waiting_for_input`.
        WaitingForInput = "waiting_for_input",
        Completed = "completed",
        Failed = "failed",
        Cancelled = "cancelled",
    }
}

impl RunStatus {
    /// Whether a run that reads `status` (a parked run reads
    /// `waiting_for_input`) is one this filter reaches: `running` reaches
    /// a run parked on a wait too, since it has not ended.
    pub fn reaches(self, status: RunStatus) -> bool {
        match self {
            Self::Running => matches!(status, Self::Running | Self::WaitingForInput),
            other => status == other,
        }
    }
}

/// Which runs a clean (or a listing) reaches. Every filter narrows; none
/// set is every run of the project.
// SYNC: RunFilter <-> crates/weft-cli/src/commands/runs.rs (clean's flags)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunFilter {
    /// Runs for this instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    /// Runs standing this way ([`RunStatus`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<RunStatus>,
    /// Runs started by this node (the trigger that fired, or the node a
    /// run was started from), spelled the way the program writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Runs carrying this tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Runs started at least this many seconds ago.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub older_than_secs: Option<u64>,
}

crate::wire_enum! {
    /// Where one node of a run stands ([`NodeState::status`]).
    // SYNC: NodeStatus <-> catalog/logic/get_node_state/metadata.json (the `state` output's description)
    pub enum NodeStatus {
        /// It has not fired in this run (yet).
        Pending = "pending",
        Running = "running",
        /// It is waiting on a signal (`ctx.await_signal`): an outside job,
        /// a person, a timer.
        Waiting = "waiting",
        Completed = "completed",
        Failed = "failed",
        Skipped = "skipped",
        /// The run was cancelled while it had not finished.
        Cancelled = "cancelled",
    }
}

/// Where one node of a run stands, as `RunCall::NodeState` answers
/// it, read from the run's record, so it stays readable for as long as
/// the run is kept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "NodeStateWire", into = "NodeStateWire")]
pub struct NodeState {
    pub status: NodeStatus,
    /// While it waits: what its wait kept of the last answer it saw (a
    /// poll of an outside job's status); `None` before the first one, and
    /// for a wait whose kind has no answer to keep. On the wire as `live`
    /// (the raw answer, unparsed) or `liveTooLarge` (its size in bytes,
    /// past what a wait keeps), never both.
    pub live: Option<crate::signal::listener_protocol::LastAnswer>,
    /// When it failed: the error, as raw text.
    pub error: Option<String>,
}

/// [`NodeState`] as the program reads it: `live` and `liveTooLarge` are
/// two keys of one answer, so a shape holding both, or either of the
/// wrong type, is refused rather than read as no answer.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeStateWire {
    status: NodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    live: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    live_too_large: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl TryFrom<NodeStateWire> for NodeState {
    type Error = String;

    fn try_from(wire: NodeStateWire) -> Result<Self, String> {
        use crate::signal::listener_protocol::LastAnswer;
        let live = match (wire.live, wire.live_too_large) {
            (Some(text), None) => Some(LastAnswer::Kept(text)),
            (None, Some(bytes)) => Some(LastAnswer::TooLarge(bytes)),
            (None, None) => None,
            (Some(_), Some(_)) => return Err("a node's state holds both `live` and `liveTooLarge`".into()),
        };
        Ok(Self { status: wire.status, live, error: wire.error })
    }
}

impl From<NodeState> for NodeStateWire {
    fn from(state: NodeState) -> Self {
        use crate::signal::listener_protocol::LastAnswer;
        let (live, live_too_large) = match state.live {
            Some(LastAnswer::Kept(text)) => (Some(text), None),
            Some(LastAnswer::TooLarge(bytes)) => (None, Some(bytes)),
            None => (None, None),
        };
        Self { status: state.status, live, live_too_large, error: state.error }
    }
}

/// The most runs one listing answers (`ctx.runs().list(..)`, the
/// runs page): a listing past it pages with a narrower filter.
pub const MAX_RUNS_PAGE: u32 = 200;

/// What `RunCall::Count` answers: how many runs match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunsCounted {
    pub total: u64,
}

/// What a clean did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanOutcome {
    /// Runs deleted.
    pub deleted: u64,
    /// Runs still going that were cancelled (`running: cancel`); their
    /// rows go with the next clean.
    pub cancelled: u64,
    /// Runs still going that were left alone (`running: leave`).
    pub left_running: u64,
}

crate::wire_enum! {
    /// What a clean does with the runs it matches that are still going: a
    /// run is never deleted from under itself.
    #[derive(Default)]
    pub enum StillGoing {
        /// Leave them running: a clean never stops a run nobody asked it
        /// to stop.
        #[default]
        Leave = "leave",
        /// Cancel them; their rows go with the next clean.
        Cancel = "cancel",
    }
}

/// `POST /runs/clean`: delete every run the filter reaches. What
/// `weft clean --project .. --instance .. --status ..` sends; a program's
/// `ctx.runs()..clean(..)` runs the same clean.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanRequest {
    /// Only this project's runs; every project of the caller's when absent.
    #[serde(default)]
    pub project: Option<uuid::Uuid>,
    #[serde(default)]
    pub filter: RunFilter,
    /// What happens to matching runs still going.
    #[serde(default)]
    pub running: StillGoing,
}

/// One run, as the runs listing shows it.
// SYNC: RunSummary <-> weavemind/website/src/routes/(app)/executions/+page.ts (Execution),
//       extension-vscode/src/sidebar/runs.ts (RunSummary)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub run_id: crate::RunId,
    pub project_id: uuid::Uuid,
    pub entry_node: String,
    /// Where the run stands: a running run parked on a wait reads
    /// `waiting_for_input`.
    pub status: RunStatus,
    /// What kind of run this was: a `fire` (a trigger fired or a
    /// manual run), or one of the two setup phases an activate /
    /// resync / infra start runs. The listing mixes all three, and
    /// "has my trigger fired since the change" is unanswerable without
    /// it. Copied from the `RunStarted` row, or, when that row
    /// no longer decodes, from the `run.phase` column the
    /// listing filtered on.
    pub phase: crate::primitive::Phase,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    /// The tags the run put on itself (`ctx.tag_run`), in the
    /// order it claimed them. Empty for a run that never tagged.
    pub tags: Vec<String>,
    /// For a `cancelled` run: who or what stopped it (a person, a
    /// sibling run, the caller leaving, the runtime). What the
    /// runs panel draws the row's icon and words from. `None`
    /// for every other status, and for a cancel row written without
    /// a cause.
    pub cancel_cause: Option<crate::run::CancelCause>,
    /// For a `failed` run: what failed it, as its terminal row says.
    /// `None` for every other status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How many node firings the run skipped (a branch that did not
    /// flow). A completed run with skips is still completed; the count
    /// is what the panel says beside it.
    pub skipped_nodes: u64,
    /// Who the run is for (`RunStarted.instance`); `None` for a run
    /// for nobody in particular.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
}

/// One page of runs plus the total number matching the same filters
/// (ignoring limit/offset), so a consumer can render page controls without a
/// second count round-trip.
// SYNC: RunPage <-> extension-vscode/src/sidebar/runs.ts (RunPage),
//       weavemind/website/src/routes/(app)/executions/+page.ts (ExecutionPage, in the weavemind repo)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunPage {
    pub runs: Vec<RunSummary>,
    pub total: u64,
}

/// `GET /runs/{id}`: one run as the listing shows it, and what it
/// is parked on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDetail {
    #[serde(flatten)]
    pub summary: RunSummary,
    /// The waits the run holds, in registration order; empty unless it is
    /// `waiting_for_input`. They ride with the status they produced, so a
    /// client that learns the run is parked learns from the same read
    /// what it is parked on.
    pub waiting: Vec<ParkedWait>,
}

/// One wait a parked run holds: the node, the token that answers it, and
/// the signal kind (a `timer` is woken, anything else expects a value).
/// What `weft wake <run_id> <node>` matches against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkedWait {
    /// The waiting node's place, spelled the way a person writes it
    /// (`one.review` inside the file the site `one` includes): the signal
    /// row's own key, so it is both what a reader is shown and what `weft
    /// wake` names.
    pub node: String,
    pub token: String,
    pub kind: String,
}

/// `GET /runs/resolve/{prefix}`: the one run of the caller's
/// whose id starts with the prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedRun {
    pub run_id: crate::RunId,
}

/// `DELETE /runs/{id}`: the run's project, and the versions the run
/// left bare that went with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedRun {
    pub project: uuid::Uuid,
    pub swept: Vec<String>,
}

/// `GET /runs/{id}/logs`: the tail of a run's log, and the limit
/// that cut it, so a reader can tell a full page from the whole log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunLogs {
    pub limit: u32,
    pub lines: Vec<RunLogLine>,
    /// Why the lines name their node by id rather than the way the program
    /// is written: the program the run ran cannot be read. Absent when
    /// they are spelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unspelled: Option<String>,
}

/// `GET /runs/{id}/events`: a run's events, kept by the query's filters
/// and spelled the way the program the run ran is written
/// (`crate::live_event::spelled`), each as it reads on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvents {
    /// How many events the run has, before the filters.
    pub total: usize,
    pub events: Vec<serde_json::Value>,
    /// Why the events name nodes by id: the program the run ran cannot be
    /// read. Absent when they are spelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unspelled: Option<String>,
}

/// `POST /runs/{id}/values`: the values some events of the run name, by
/// where the record keeps them (`crate::record_ref::ValueRef`, as the
/// events carry them): what the editor reads when it shows a firing's
/// input or output.
// SYNC: ValuesRequest <-> packages/weft-graph/src/protocol.ts ValuesRequest
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValuesRequest {
    pub refs: Vec<crate::record_ref::ValueRef>,
}

/// What each reference of a [`ValuesRequest`] reads, in its order.
// SYNC: ValuesAnswer <-> packages/weft-graph/src/protocol.ts ValuesAnswer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValuesAnswer {
    pub values: Vec<KeptValue>,
}

/// One value read back: the value, or why it cannot be.
// SYNC: KeptValue <-> packages/weft-graph/src/protocol.ts KeptValue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KeptValue {
    Value { value: serde_json::Value },
    /// The run's record is gone: its keep time ran out, or `weft clean`
    /// erased it.
    Gone { gone: String },
    /// The run's record is there and does not hold the value where the
    /// reference says: a row or a key it names is missing, or the row
    /// keeps no value. Said for this value alone.
    Broken { broken: String },
}

/// One line of a run's log.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunLogLine {
    /// The seed run this line was carried over from, for a seeded run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<crate::RunId>,
    pub at_unix: u64,
    pub level: String,
    /// The firing this line is about: the node, spelled the way the
    /// program the run ran is written (`one.strip` for the `strip` of the
    /// file the site `one` includes), and the loop iteration it was in.
    /// Absent on the wire for a run-level line (the run itself failing or
    /// being cancelled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: crate::LoopFrames,
    pub message: String,
}

/// Whose money a call spent, as a cost filter names it.
// SYNC: PaidBy <-> crates/weft-core/src/access/mod.rs CredentialOwner
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaidBy {
    /// The runtime's own credential (its credit).
    Platform,
    /// The author's own connection.
    Author,
    /// An instance's own connection (any instance, or the one `instance`
    /// narrows to).
    Instance,
}

impl PaidBy {
    /// Which of these a call spent on `owner`'s credential is.
    pub fn of(owner: &crate::CredentialOwner) -> Self {
        match owner {
            crate::CredentialOwner::Platform => PaidBy::Platform,
            crate::CredentialOwner::Author => PaidBy::Author,
            crate::CredentialOwner::Instance(_) => PaidBy::Instance,
        }
    }

    /// Its word, as the wire and a cost's row (`run_cost.paid_by`) spell it.
    // SYNC: PaidBy::as_str <-> the serde names above, run_cost's paid_by check (crates/weft-dispatcher/src/store/postgres.rs)
    pub fn as_str(&self) -> &'static str {
        match self {
            PaidBy::Platform => "platform",
            PaidBy::Author => "author",
            PaidBy::Instance => "instance",
        }
    }

    /// The word [`Self::as_str`] spells, read back.
    pub fn parse(word: &str) -> Option<Self> {
        [PaidBy::Platform, PaidBy::Author, PaidBy::Instance].into_iter().find(|paid_by| paid_by.as_str() == word)
    }
}

/// Which cost records `ctx.costs()` reads. Every filter narrows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CostFilter {
    /// Costs of runs for this instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    /// Costs booked by this node, spelled the way the program writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paid_by: Option<PaidBy>,
    /// Costs of one run (`ctx.run_id` for the calling run's own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<uuid::Uuid>,
    /// Costs booked at or after this unix second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_unix: Option<u64>,
}

/// One cost record, as `ctx.costs().list()` answers it. Weft reports the
/// price it measured; what anybody is billed is the program's business.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CostRecord {
    pub run: uuid::Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    pub node: String,
    pub service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// `None` when the service's price could not be measured.
    pub amount_usd: Option<f64>,
    pub paid_by: crate::CredentialOwner,
    pub at_unix: u64,
}

/// An instance token a program minted. The value is shown here once; the
/// server keeps only its hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintedInstanceToken {
    /// Revoke it by this id.
    pub id: uuid::Uuid,
    /// The token itself, for the instance's browser or extension to
    /// present in the `Weft-Instance-Token` header (or as a signal token).
    pub token: String,
    pub expires_at_unix: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::wire_enum_roundtrip_tests!(RunStatus);
    crate::wire_enum_roundtrip_tests!(StillGoing);
    crate::wire_enum_roundtrip_tests!(NodeStatus);
    use crate::signal::listener_protocol::LastAnswer;

    /// A node's state reaches the program in the shape `GetNodeState`
    /// hands on: its status word, and the raw answer or error only when
    /// there is one.
    #[test]
    fn a_node_state_reaches_the_program_in_its_wire_shape() {
        let waiting = NodeState { status: NodeStatus::Waiting, live: Some(LastAnswer::Kept("{\"status\":\"IN_QUEUE\"}".into())), error: None };
        assert_eq!(
            serde_json::to_value(&waiting).unwrap(),
            serde_json::json!({ "status": "waiting", "live": "{\"status\":\"IN_QUEUE\"}" })
        );
        let large = NodeState { status: NodeStatus::Waiting, live: Some(LastAnswer::TooLarge(70_000)), error: None };
        let wire = serde_json::to_value(&large).unwrap();
        assert_eq!(wire, serde_json::json!({ "status": "waiting", "liveTooLarge": 70_000 }));
        assert_eq!(serde_json::from_value::<NodeState>(wire).unwrap(), large);
        for refused in [
            serde_json::json!({ "status": "waiting", "live": "a", "liveTooLarge": 3 }),
            serde_json::json!({ "status": "waiting", "live": 5 }),
            serde_json::json!({ "status": "waiting", "liveTooLarge": "x" }),
        ] {
            assert!(serde_json::from_value::<NodeState>(refused.clone()).is_err(), "{refused}");
        }
        let failed = NodeState { status: NodeStatus::Failed, live: None, error: Some("boom".into()) };
        let wire = serde_json::to_value(&failed).unwrap();
        assert_eq!(wire, serde_json::json!({ "status": "failed", "error": "boom" }));
        assert_eq!(serde_json::from_value::<NodeState>(wire).unwrap(), failed);
        let call = ProgramCall::Runs(runs::RunCall::NodeState { run: crate::RunId::from_u128(4), node: "one.render".into() });
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!((&wire["area"], &wire["call"]), (&serde_json::json!("runs"), &serde_json::json!("node_state")));
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), call);
    }

    #[test]
    fn running_reaches_a_run_parked_on_a_wait_and_waiting_only_that() {
        assert!(RunStatus::Running.reaches(RunStatus::Running) && RunStatus::Running.reaches(RunStatus::WaitingForInput));
        assert!(RunStatus::WaitingForInput.reaches(RunStatus::WaitingForInput) && !RunStatus::WaitingForInput.reaches(RunStatus::Running));
        assert!(RunStatus::Failed.reaches(RunStatus::Failed) && !RunStatus::Failed.reaches(RunStatus::Running));
    }


    /// Each kind of credential is one of the three, and each word is the
    /// one the wire spells and reads back.
    #[test]
    fn paid_by_is_the_kind_of_credential_and_its_wire_word() {
        use crate::CredentialOwner;
        let ada = CredentialOwner::Instance(InstanceId::new("ada").unwrap());
        assert_eq!(PaidBy::of(&ada), PaidBy::Instance);
        assert_eq!(PaidBy::of(&CredentialOwner::Author), PaidBy::Author);
        assert_eq!(PaidBy::of(&CredentialOwner::Platform), PaidBy::Platform);
        for paid_by in [PaidBy::Platform, PaidBy::Author, PaidBy::Instance] {
            assert_eq!(serde_json::to_value(&paid_by).unwrap(), serde_json::json!(paid_by.as_str()));
            assert_eq!(PaidBy::parse(paid_by.as_str()), Some(paid_by));
        }
        assert_eq!(PaidBy::parse("ours"), None);
    }

    #[test]
    fn a_call_round_trips_under_its_tag() {
        let call = ProgramCall::Infra(infra::InfraCall::Stop {
            node: "bridge".into(),
            instance: Some(InstanceId::new("ada").unwrap()),
            take_down: crate::take_down::TakeDownChoice { running: Some(crate::take_down::Running::Cancel), ..Default::default() },
            force: false,
        });
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!((&wire["area"], &wire["call"]), (&serde_json::json!("infra"), &serde_json::json!("stop")));
        assert!(wire.get("force").is_none(), "a plain stop says nothing of force: {wire}");
        assert_eq!(wire["instance"], "ada");
        assert_eq!(wire["take_down"], serde_json::json!({ "running": "cancel" }));
        let back: ProgramCall = serde_json::from_value(wire).unwrap();
        assert_eq!(back, call);
    }

    #[test]
    fn the_instances_listing_round_trips_under_its_tag() {
        let list = ProgramCall::Instances(instances::InstanceCall::List);
        let wire = serde_json::to_value(&list).unwrap();
        assert_eq!(wire, serde_json::json!({ "area": "instances", "call": "list" }));
        let back: ProgramCall = serde_json::from_value(wire).unwrap();
        assert_eq!(back, list);
        assert!(!list.takes_down());
    }

    fn instance(id: &str) -> InstanceId {
        InstanceId::new(id).unwrap()
    }

    /// A call on the asker's own project names no project; one on another
    /// of the owner's names it beside the call.
    #[test]
    fn a_payload_names_its_project_only_when_it_is_another() {
        let call = ProgramCall::Versions(versions::VersionCall::Tree);
        let own = ProgramCallPayload { by: uuid::Uuid::nil(), stop_self: StopSelf::Keep, project: None, call: call.clone() };
        let wire = serde_json::to_value(&own).unwrap();
        assert!(wire.get("project").is_none(), "{wire}");
        assert_eq!(wire["call"], serde_json::json!({ "area": "versions", "call": "tree" }));
        let other = ProgramCallPayload { project: Some(uuid::Uuid::from_u128(7)), ..own };
        let back: ProgramCallPayload = serde_json::from_value(serde_json::to_value(&other).unwrap()).unwrap();
        assert_eq!(back, other);
    }

    /// A call issued again at the same place of the same run is the same
    /// call; any other place is another.
    #[test]
    fn a_call_is_named_after_the_place_that_makes_it() {
        assert_eq!(program_call_id("program_call/r1/n/f/0"), program_call_id("program_call/r1/n/f/0"));
        assert_ne!(program_call_id("program_call/r1/n/f/0"), program_call_id("program_call/r1/n/f/1"));
        assert_ne!(program_call_id("program_call/r1/n/f/0"), program_call_id("program_call/r2/n/f/0"));
    }

    #[test]
    fn holdings_merge_one_entry_per_instance_from_every_source() {
        let copy = |who: Option<&str>, status: crate::infra::InfraNodeStatus| InfraCopy {
            node: "bridge".into(),
            instance: who.map(instance),
            status,
            failure: None,
        };
        let waiting = InstanceTrigger {
            trigger: "inbox".into(),
            mode: crate::activation::ActivationMode::Active,
            waiting: Some(WaitingFires {
                fires: 2,
                on: crate::door_fire::UnclockedWait::Instance,
                reason: "instance 'cyd' at 'answer': 'key' is not filled".into(),
            }),
        };
        let merged = InstanceHoldings::merge(
            vec![(instance("ada"), 3)],
            vec![(instance("bob"), 1)],
            vec![(instance("ada"), 2)],
            // The shared copy belongs to no instance in this listing.
            vec![copy(None, crate::infra::InfraNodeStatus::Running), copy(Some("bob"), crate::infra::InfraNodeStatus::Stopped)],
            vec![(instance("cyd"), waiting.clone())],
            vec![(instance("ada"), 5), (instance("ada"), 9)],
        );
        let instances: Vec<&str> = merged.iter().map(|m| m.instance.as_str()).collect();
        assert_eq!(merged[0].last_run_unix, Some(9), "the latest run counts");
        assert_eq!(merged[1].last_run_unix, None);
        assert_eq!(instances, ["ada", "bob", "cyd"]);
        assert_eq!((merged[0].values, merged[0].connections, merged[0].tokens), (3, 0, 2));
        assert!(merged[0].copies.is_empty() && merged[0].triggers.is_empty());
        assert_eq!(merged[1].connections, 1);
        assert_eq!(merged[1].copies, vec![copy(Some("bob"), crate::infra::InfraNodeStatus::Stopped)]);
        assert_eq!(merged[2].triggers, vec![waiting]);
        assert_eq!((merged[2].values, merged[2].connections, merged[2].tokens), (0, 0, 0));
    }

    #[test]
    fn holdings_of_no_instance_merge_to_nothing() {
        assert!(InstanceHoldings::merge(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()).is_empty());
    }

    #[test]
    fn only_take_downs_can_stop_the_asker() {
        assert!(ProgramCall::Infra(infra::InfraCall::Terminate {
            node: "x".into(),
            instance: None,
            take_down: crate::take_down::TakeDownChoice::default(),
            disks: crate::infra::TerminateDisks::KeepListed,
        })
        .takes_down());
        assert!(!ProgramCall::Infra(infra::InfraCall::Start { node: "x".into(), instance: None }).takes_down());
        assert!(!ProgramCall::Instances(instances::InstanceCall::ConnectionsList { instance: InstanceId::new("a").unwrap() }).takes_down());
    }

    #[test]
    fn the_runs_listing_and_count_round_trip_under_their_tags() {
        let filter = RunFilter { tag: Some("draft".into()), instance: Some(instance("ada")), ..RunFilter::default() };
        let list = ProgramCall::Runs(runs::RunCall::List(runs::RunList { filter: filter.clone(), limit: 20, ..runs::RunList::default() }));
        let wire = serde_json::to_value(&list).unwrap();
        assert_eq!(wire, serde_json::json!({ "area": "runs", "call": "list", "filter": { "tag": "draft", "instance": "ada" }, "limit": 20 }));
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), list);
        let count = ProgramCall::Runs(runs::RunCall::Count { filter });
        let wire = serde_json::to_value(&count).unwrap();
        assert_eq!(wire["call"], "count");
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), count);
        assert!(!list.takes_down() && !count.takes_down());
        assert_eq!(serde_json::to_value(RunsCounted { total: 3 }).unwrap(), serde_json::json!({ "total": 3 }));
    }

    #[test]
    fn a_run_listing_names_its_fields_in_camel_case() {
        let run = RunSummary {
            run_id: crate::RunId::from_u128(1),
            project_id: uuid::Uuid::from_u128(2),
            entry_node: "hook".into(),
            status: RunStatus::WaitingForInput,
            phase: crate::primitive::Phase::Fire,
            started_at: 10,
            completed_at: None,
            tags: vec!["draft".into()],
            cancel_cause: None,
            error: None,
            skipped_nodes: 3,
            instance: None,
        };
        let page = RunPage { runs: vec![run], total: 1 };
        let wire = serde_json::to_value(&page).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({ "runs": [{
                "runId": "00000000-0000-0000-0000-000000000001",
                "projectId": "00000000-0000-0000-0000-000000000002",
                "entryNode": "hook",
                "status": "waiting_for_input",
                "phase": "fire",
                "startedAt": 10,
                "completedAt": null,
                "tags": ["draft"],
                "cancelCause": null,
                "skippedNodes": 3,
            }], "total": 1 })
        );
        let back: RunPage = serde_json::from_value(wire).unwrap();
        assert_eq!(back.runs[0].run_id, page.runs[0].run_id);
        assert_eq!(
            serde_json::to_value(ResolvedRun { run_id: crate::RunId::nil() }).unwrap(),
            serde_json::json!({ "runId": "00000000-0000-0000-0000-000000000000" })
        );
        let line = RunLogLine {
            inherited_from: Some(crate::RunId::nil()),
            at_unix: 4,
            level: "info".into(),
            node: None,
            frames: Default::default(),
            message: "m".into(),
        };
        assert_eq!(
            serde_json::to_value(line).unwrap(),
            serde_json::json!({ "inheritedFrom": "00000000-0000-0000-0000-000000000000", "atUnix": 4, "level": "info", "message": "m" })
        );
        let filter = RunFilter { older_than_secs: Some(60), ..RunFilter::default() };
        assert_eq!(serde_json::to_value(&filter).unwrap(), serde_json::json!({ "olderThanSecs": 60 }));
    }

    #[test]
    fn an_empty_filter_says_nothing_on_the_wire() {
        assert_eq!(serde_json::to_value(RunFilter::default()).unwrap(), serde_json::json!({}));
        assert_eq!(serde_json::to_value(CostFilter::default()).unwrap(), serde_json::json!({}));
    }
}

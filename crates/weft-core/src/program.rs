//! What a program may ask of its own project, beyond its own run: start,
//! stop or read an infra copy, activate or take down triggers, read and
//! change what an instance provides, list or forget an instance's connections,
//! list every instance weft holds anything for, read what calls cost, clean
//! runs, and revoke instance tokens. The ctx builders (`ctx.infra(..)`,
//! `ctx.trigger(..)`, `ctx.triggers()`, `ctx.values()`,
//! `ctx.connections()`, `ctx.instances()`, `ctx.costs()`, `ctx.runs()`,
//! `ctx.tokens()`) each build one [`ProgramCall`]; the runtime carries it
//! to the dispatcher as one durable task and hands back its answer.
//!
//! Every call is scoped to the calling run's project: the broker takes the
//! project from the run, never from the call. Inside the project any instance
//! may be named from any run, because the program is the author's own code;
//! instance limits are enforced at the outside doors (an instance token, the
//! `Weft-Instance` header), not here.
//!
//! A call that takes something down carries a [`DeactivateSpec`] (what
//! happens to parked and running work) and a [`StopSelf`]: the calling run
//! is never among the runs a take-down reaches, and `StopSelf::Include`
//! stops it too, after the take-down is queued.

use serde::{Deserialize, Serialize};

use crate::activation::ActivationScope;
use crate::instance::InstanceId;
use crate::running_policy::{DeactivateSpec, RunningPolicy};
use crate::tag::StopSelf;

/// One call a program makes on its project.
// SYNC: ProgramCall <-> crates/weft-dispatcher/src/task_kinds/program_call.rs (the executor's match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum ProgramCall {
    /// Bring up a copy of an infra node: the shared one, or an instance's
    /// copy of a node that exists once per instance. Answers once the start
    /// is queued; `ctx.infra(..).start()` then waits on
    /// [`ProgramCall::InfraStatus`] until it runs.
    InfraStart { node: String, instance: Option<InstanceId> },
    /// Scale a copy down, keeping its disk.
    InfraStop { node: String, instance: Option<InstanceId>, spec: DeactivateSpec },
    /// Delete a copy and its disks, keeping the ones its node lists in
    /// `keepOnTerminate` unless `disks` says the owner is going for good.
    InfraTerminate { node: String, instance: Option<InstanceId>, spec: DeactivateSpec, disks: crate::infra::TerminateDisks },
    /// One copy's state, or none when it was never started (or was
    /// terminated).
    InfraStatus { node: String, instance: Option<InstanceId> },
    /// Every copy of a node that exists: the shared one and each
    /// instance's.
    InfraCopies { node: String },
    /// Activate triggers: the ones `scope` names, or every trigger of its
    /// owner.
    TriggerActivate { scope: ActivationScope },
    /// Take triggers down.
    TriggerDeactivate { scope: ActivationScope, spec: DeactivateSpec },
    /// An instance's connections.
    ConnectionsList { instance: InstanceId },
    /// Forget every connection of an instance (the values naming them go
    /// with them).
    ConnectionsForget { instance: InstanceId },
    /// What an instance provides for the program's `@instance_filled` fields.
    ValuesGet { instance: InstanceId },
    /// Change what an instance provides: `set` given, `clear` forgotten, all
    /// or none, re-arming the instance's live triggers that read them.
    ValuesChange {
        instance: InstanceId,
        #[serde(default)]
        set: Vec<crate::run_spec::InstanceValueInput>,
        #[serde(default)]
        clear: Vec<crate::run_spec::InstanceFieldRef>,
    },
    /// Forget everything an instance provides (what wiping an instance does),
    /// re-arming its live triggers that read any of it.
    ValuesForget { instance: InstanceId },
    /// Every instance weft holds anything for in this project, with what it
    /// holds ([`InstanceHoldings`]).
    InstancesList,
    /// What calls cost, filtered.
    CostsList { filter: CostFilter },
    /// Delete runs matching `filter`. Runs still going follow `running`:
    /// `cancel` stops them (their rows go with the next clean), `wait`
    /// leaves them running.
    RunsClean { filter: RunFilter, running: RunningPolicy },
    /// The newest runs matching `filter`, at most `limit` of them
    /// (1 to [`MAX_RUNS_PAGE`]), with how many match in all
    /// ([`ExecutionPage`]).
    RunsList { filter: RunFilter, limit: u32 },
    /// How many runs match `filter` ([`RunsCounted`]).
    RunsCount { filter: RunFilter },
    /// Revoke instance tokens: every token of `instance`, or the one `id`.
    TokensRevoke { instance: InstanceId, id: Option<uuid::Uuid> },
}

impl ProgramCall {
    /// Whether the call can take the calling run down with it, so it
    /// carries a [`StopSelf`] that matters.
    pub fn takes_down(&self) -> bool {
        matches!(
            self,
            ProgramCall::InfraStop { .. }
                | ProgramCall::InfraTerminate { .. }
                | ProgramCall::TriggerDeactivate { .. }
                | ProgramCall::RunsClean { .. }
        )
    }

    /// The name the call is journaled under (`ctx.run`), so a replayed
    /// run gets the recorded answer instead of asking again.
    pub fn journal_name(&self) -> &'static str {
        match self {
            ProgramCall::InfraStart { .. } => "weft.infra.start",
            ProgramCall::InfraStop { .. } => "weft.infra.stop",
            ProgramCall::InfraTerminate { .. } => "weft.infra.terminate",
            ProgramCall::InfraStatus { .. } => "weft.infra.status",
            ProgramCall::InfraCopies { .. } => "weft.infra.copies",
            ProgramCall::TriggerActivate { .. } => "weft.triggers.activate",
            ProgramCall::TriggerDeactivate { .. } => "weft.triggers.deactivate",
            ProgramCall::ConnectionsList { .. } => "weft.connections.list",
            ProgramCall::ConnectionsForget { .. } => "weft.connections.forget",
            ProgramCall::ValuesGet { .. } => "weft.values.get",
            ProgramCall::ValuesChange { .. } => "weft.values.change",
            ProgramCall::ValuesForget { .. } => "weft.values.forget",
            ProgramCall::InstancesList => "weft.instances.list",
            ProgramCall::CostsList { .. } => "weft.costs.list",
            ProgramCall::RunsClean { .. } => "weft.runs.clean",
            ProgramCall::RunsList { .. } => "weft.runs.list",
            ProgramCall::RunsCount { .. } => "weft.runs.count",
            ProgramCall::TokensRevoke { .. } => "weft.tokens.revoke",
        }
    }
}

/// The durable task that carries a [`ProgramCall`]: who asked, what
/// happens to the asker when the call takes it down, and the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramCallPayload {
    /// The calling run (its execution).
    pub by: uuid::Uuid,
    pub stop_self: StopSelf,
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

/// Fires of one instance's trigger that wait, parked, because the instance
/// has not given (or gave an invalid) value the fire needs. They are not
/// retried on a timer: the instance's next change of values routes them
/// again, and so does activating the trigger.
// SYNC: WaitingFires <-> packages/weft-graph/src/protocol.ts ActivationEntry.waiting
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingFires {
    pub fires: u32,
    /// Why, naming the field (`instance 'ada' at 'answer': 'key' is not
    /// filled`): the refusal of the fire parked last.
    pub reason: String,
}

/// What `ProgramCall::InfraStart` answers: a start queued, one already
/// under way or done, or none yet for a passing reason (a build in
/// progress, a trigger of the copy mid-activation), which
/// `ctx.infra(..).start()` asks again at its next look.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
pub enum InfraStartAnswer {
    Started,
    AlreadyRunning,
    AlreadyStarting,
    Waiting { reason: String },
}

impl InfraStartAnswer {
    /// Whether a start is under way: every answer but `Waiting`.
    pub fn queued(&self) -> bool {
        !matches!(self, InfraStartAnswer::Waiting { .. })
    }
}

/// What `ProgramCall::ConnectionsForget` answers: how many connections
/// went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionsForgotten {
    pub forgotten: u64,
}

/// What `ProgramCall::TokensRevoke` answers: how many tokens went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokensRevoked {
    pub revoked: u64,
}

/// One instance's activation of a trigger that exists once per instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceTrigger {
    /// The trigger, spelled the way the program writes it.
    pub trigger: String,
    pub mode: crate::activation::ActivationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<WaitingFires>,
}

/// Everything weft holds for one instance of the project, as
/// `ctx.instances().list()` answers it: counts and states, never a value or
/// a secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl InstanceHoldings {
    fn empty(instance: InstanceId) -> Self {
        Self { instance, values: 0, connections: 0, tokens: 0, copies: Vec::new(), triggers: Vec::new() }
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
        by_instance.into_values().collect()
    }
}

crate::wire_enum! {
    /// Where a run stands, as a filter asks for it: each word is the
    /// [`ExecutionSummary::status`] a run reads in a listing.
    // SYNC: RunStatus <-> crates/weft-dispatcher/src/journal/postgres.rs list_executions (status clause)
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
    /// Whether a run whose decoded status is `status` (the honest one, a
    /// parked run already `waiting_for_input`) is one this filter
    /// reaches. This decides what a filtered `weft clean` may delete, not
    /// what a listing shows: the listing filters in SQL, which cannot see
    /// a row that no longer decodes, so such a row lists under the filter
    /// as `corrupt`. A corrupt row is reached by no filter, so a filtered
    /// clean never deletes it.
    pub fn reaches(self, status: SummaryStatus) -> bool {
        let SummaryStatus::Run(status) = status else { return false };
        match self {
            Self::Running => matches!(status, Self::Running | Self::WaitingForInput),
            other => status == other,
        }
    }
}

/// What a listing says about one run: where it stands ([`RunStatus`]),
/// or `corrupt` when its row no longer decodes. Separate from
/// [`RunStatus`] because a filter can never ask for a corrupt row (SQL
/// cannot select a row by failing to decode it). On the wire it is the
/// one flat word, `running` .. `cancelled` or `corrupt`.
// SYNC: SummaryStatus <-> extension-vscode/src/sidebar/executions.ts ExecutionSummary['status']
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SummaryStatus {
    Run(RunStatus),
    Corrupt,
}

impl SummaryStatus {
    pub const CORRUPT: &'static str = "corrupt";

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run(status) => status.as_str(),
            Self::Corrupt => Self::CORRUPT,
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        if s == Self::CORRUPT {
            return Some(Self::Corrupt);
        }
        RunStatus::parse(s).map(Self::Run)
    }

    /// The status a listing shows, given whether the run is parked on a
    /// wait (a resume signal is registered for it): a running run that
    /// is parked reads `waiting_for_input`; every other status is kept.
    // SYNC: SummaryStatus::parked <-> crates/weft-journal/src/events.rs run_parked_sql
    pub fn parked(self, parked: bool) -> Self {
        match self {
            Self::Run(RunStatus::Running) if parked => Self::Run(RunStatus::WaitingForInput),
            other => other,
        }
    }
}

impl From<RunStatus> for SummaryStatus {
    fn from(status: RunStatus) -> Self {
        Self::Run(status)
    }
}

impl std::fmt::Display for SummaryStatus {
    /// Padded, so a listing column (`{status:<9}`) lines up.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self.as_str())
    }
}

impl Serialize for SummaryStatus {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SummaryStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let word = String::deserialize(deserializer)?;
        Self::parse(&word).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown run status `{word}` (expected one of {}, {})",
                RunStatus::accepted(),
                Self::CORRUPT
            ))
        })
    }
}

/// Which runs a clean (or a listing) reaches. Every filter narrows; none
/// set is every run of the project.
// SYNC: RunFilter <-> crates/weft-cli/src/commands/executions.rs (clean's flags)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

/// The most runs one listing answers (`ctx.runs().list(..)`, the
/// executions page): a listing past it pages with a narrower filter.
pub const MAX_RUNS_PAGE: u32 = 200;

/// What `ProgramCall::RunsCount` answers: how many runs match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunsCounted {
    pub total: u64,
}

/// What a clean did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanOutcome {
    /// Runs deleted.
    pub deleted: u64,
    /// Runs still going that were cancelled (`running: cancel`); their
    /// rows go with the next clean.
    pub cancelled: u64,
    /// Runs still going that were left alone (`running: wait`).
    pub left_running: u64,
}

/// `POST /executions/clean`: delete every run the filter reaches. What
/// `weft clean --project .. --instance .. --status ..` sends; a program's
/// `ctx.runs()..clean(..)` runs the same clean.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanRequest {
    /// Only this project's runs; every project of the caller's when absent.
    #[serde(default)]
    pub project: Option<uuid::Uuid>,
    #[serde(default)]
    pub filter: RunFilter,
    /// What happens to matching runs still going. Absent is `wait`: a
    /// clean never stops a run nobody asked it to stop.
    #[serde(default = "default_clean_running")]
    pub running: RunningPolicy,
}

fn default_clean_running() -> RunningPolicy {
    RunningPolicy::Wait
}

/// One run, as the executions listing shows it.
// SYNC: ExecutionSummary <-> weavemind/website/src/routes/(app)/executions/+page.ts (Execution),
//       extension-vscode/src/sidebar/executions.ts (ExecutionSummary)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSummary {
    pub execution_id: crate::ExecutionId,
    pub project_id: uuid::Uuid,
    pub entry_node: String,
    /// Where the run stands ([`RunStatus`]: a running run parked on a
    /// wait reads `waiting_for_input`), or `corrupt` (the row no longer
    /// decodes; `entry_node` is empty then, and the row is listed so it
    /// can be inspected via replay and deleted).
    pub status: SummaryStatus,
    /// What kind of run this was: a `fire` (a trigger fired or a
    /// manual run), or one of the two setup phases an activate /
    /// resync / infra start runs. The listing mixes all three, and
    /// "has my trigger fired since the change" is unanswerable without
    /// it. Copied from the `ExecutionStarted` row, or, when that row
    /// no longer decodes, from the `execution.phase` column the
    /// listing filtered on.
    pub phase: crate::primitive::Phase,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    /// The tags the run put on itself (`ctx.tag_execution`), in the
    /// order it claimed them. Empty for a run that never tagged.
    pub tags: Vec<String>,
    /// For a `cancelled` run: who or what stopped it (a person, a
    /// sibling run, the caller leaving, the runtime). What the
    /// executions panel draws the row's icon and words from. `None`
    /// for every other status, and for a cancel row written without
    /// a cause.
    pub cancel_cause: Option<crate::exec::CancelCause>,
    /// For a `failed` run: what failed it, as its terminal row says.
    /// `None` for every other status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How many node firings the run skipped (a branch that did not
    /// flow). A completed run with skips is still completed; the count
    /// is what the panel says beside it.
    pub skipped_nodes: u64,
    /// Who the run is for (`ExecutionStarted.instance`); `None` for a run
    /// for nobody in particular.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
}

/// One page of executions plus the total number matching the same filters
/// (ignoring limit/offset), so a consumer can render page controls without a
/// second count round-trip.
// SYNC: ExecutionPage <-> weavemind/website/src/routes/(app)/executions/+page.ts (ExecutionPage),
//       extension-vscode/src/sidebar/executions.ts (ExecutionPage)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPage {
    pub executions: Vec<ExecutionSummary>,
    pub total: u64,
}

/// `GET /executions/{id}`: one run as the listing shows it, and what it
/// is parked on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionDetail {
    #[serde(flatten)]
    pub summary: ExecutionSummary,
    /// The waits the run holds, in registration order; empty unless it is
    /// `waiting_for_input`. They ride with the status they produced, so a
    /// client that learns the run is parked learns from the same read
    /// what it is parked on.
    pub waiting: Vec<ParkedWait>,
}

/// One wait a parked run holds: the node, the token that answers it, and
/// the signal kind (a `timer` is woken, anything else expects a value).
/// What `weft wake <execution_id> <node>` matches against.
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

/// `GET /executions/resolve/{prefix}`: the one execution of the caller's
/// whose id starts with the prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedExecution {
    pub execution_id: crate::ExecutionId,
}

/// `DELETE /executions/{id}`: the run's project, and the versions the run
/// left bare that went with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedExecution {
    pub project: uuid::Uuid,
    pub swept: Vec<String>,
}

/// `GET /executions/{id}/logs`: the tail of a run's log, and the limit
/// that cut it, so a reader can tell a full page from the whole log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionLogs {
    pub limit: u32,
    pub lines: Vec<ExecutionLogLine>,
}

/// One line of a run's log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionLogLine {
    /// The seed run this line was carried over from, for a seeded run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<crate::ExecutionId>,
    pub at_unix: u64,
    pub level: String,
    /// The firing this line is about: the node, and the loop
    /// iteration it was in. Absent on the wire for a run-level line
    /// (the run itself failing or being cancelled).
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
    /// Whether a call spent on `owner`'s credential is one of these.
    pub fn covers(&self, owner: &crate::CredentialOwner) -> bool {
        matches!(
            (self, owner),
            (PaidBy::Platform, crate::CredentialOwner::Platform)
                | (PaidBy::Author, crate::CredentialOwner::Author)
                | (PaidBy::Instance, crate::CredentialOwner::Instance(_))
        )
    }
}

/// Which cost records `ctx.costs()` reads. Every filter narrows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Costs of one run (`ctx.execution_id` for the calling run's own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<uuid::Uuid>,
    /// Costs booked at or after this unix second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_unix: Option<u64>,
}

/// One cost record, as `ctx.costs().list()` answers it. Weft reports the
/// price it measured; what anybody is billed is the program's business.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    use crate::running_policy::DeactivationMode;

    crate::wire_enum_roundtrip_tests!(RunStatus);

    #[test]
    fn running_reaches_a_run_parked_on_a_wait_and_waiting_only_that() {
        let run = SummaryStatus::Run;
        assert!(RunStatus::Running.reaches(run(RunStatus::Running)) && RunStatus::Running.reaches(run(RunStatus::WaitingForInput)));
        assert!(RunStatus::WaitingForInput.reaches(run(RunStatus::WaitingForInput)) && !RunStatus::WaitingForInput.reaches(run(RunStatus::Running)));
        assert!(RunStatus::Failed.reaches(run(RunStatus::Failed)) && !RunStatus::Failed.reaches(run(RunStatus::Running)));
        assert!(RunStatus::VARIANTS.iter().all(|f| !f.reaches(SummaryStatus::Corrupt)));
    }

    /// A summary status is the one flat word on the wire, both ways,
    /// `corrupt` included, and an unknown word is refused.
    #[test]
    fn summary_status_round_trips_as_one_flat_word() {
        let every = RunStatus::VARIANTS.iter().map(|s| SummaryStatus::Run(*s)).chain([SummaryStatus::Corrupt]);
        for status in every {
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, format!("\"{}\"", status.as_str()));
            assert_eq!(serde_json::from_str::<SummaryStatus>(&json).unwrap(), status);
        }
        assert_eq!(serde_json::to_string(&SummaryStatus::Corrupt).unwrap(), "\"corrupt\"");
        assert!(serde_json::from_str::<SummaryStatus>("\"unknown\"").is_err());
    }

    #[test]
    fn only_a_running_run_that_is_parked_reads_waiting() {
        let running = SummaryStatus::Run(RunStatus::Running);
        assert_eq!(running.parked(true), SummaryStatus::Run(RunStatus::WaitingForInput));
        assert_eq!(running.parked(false), running);
        let done = SummaryStatus::Run(RunStatus::Completed);
        assert_eq!(done.parked(true), done);
        assert_eq!(SummaryStatus::Corrupt.parked(true), SummaryStatus::Corrupt);
    }

    fn spec() -> DeactivateSpec {
        DeactivateSpec {
            mode: DeactivationMode::Wipe,
            grace_minutes: 0,
            running_policy: RunningPolicy::Cancel,
            drain_timeout_secs: None,
        }
    }

    #[test]
    fn paid_by_covers_its_own_kind_of_credential() {
        use crate::CredentialOwner;
        let ada = CredentialOwner::Instance(InstanceId::new("ada").unwrap());
        assert!(PaidBy::Instance.covers(&ada));
        assert!(!PaidBy::Instance.covers(&CredentialOwner::Author));
        assert!(PaidBy::Author.covers(&CredentialOwner::Author));
        assert!(PaidBy::Platform.covers(&CredentialOwner::Platform));
        assert!(!PaidBy::Platform.covers(&ada));
    }

    #[test]
    fn a_call_round_trips_under_its_tag() {
        let call = ProgramCall::InfraStop {
            node: "bridge".into(),
            instance: Some(InstanceId::new("ada").unwrap()),
            spec: spec(),
        };
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(wire["call"], "infra_stop");
        assert_eq!(wire["instance"], "ada");
        let back: ProgramCall = serde_json::from_value(wire).unwrap();
        assert_eq!(back, call);
    }

    #[test]
    fn the_instances_listing_round_trips_under_its_tag() {
        let wire = serde_json::to_value(ProgramCall::InstancesList).unwrap();
        assert_eq!(wire, serde_json::json!({ "call": "instances_list" }));
        let back: ProgramCall = serde_json::from_value(wire).unwrap();
        assert_eq!(back, ProgramCall::InstancesList);
        assert!(!ProgramCall::InstancesList.takes_down());
    }

    fn instance(id: &str) -> InstanceId {
        InstanceId::new(id).unwrap()
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
            waiting: Some(WaitingFires { fires: 2, reason: "instance 'cyd' at 'answer': 'key' is not filled".into() }),
        };
        let merged = InstanceHoldings::merge(
            vec![(instance("ada"), 3)],
            vec![(instance("bob"), 1)],
            vec![(instance("ada"), 2)],
            // The shared copy belongs to no instance in this listing.
            vec![copy(None, crate::infra::InfraNodeStatus::Running), copy(Some("bob"), crate::infra::InfraNodeStatus::Stopped)],
            vec![(instance("cyd"), waiting.clone())],
        );
        let instances: Vec<&str> = merged.iter().map(|m| m.instance.as_str()).collect();
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
        assert!(InstanceHoldings::merge(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()).is_empty());
    }

    #[test]
    fn only_take_downs_can_stop_the_asker() {
        assert!(ProgramCall::InfraTerminate {
            node: "x".into(),
            instance: None,
            spec: spec(),
            disks: crate::infra::TerminateDisks::KeepListed,
        }.takes_down());
        assert!(!ProgramCall::InfraStart { node: "x".into(), instance: None }.takes_down());
        assert!(!ProgramCall::ConnectionsList { instance: InstanceId::new("a").unwrap() }.takes_down());
    }

    #[test]
    fn the_runs_listing_and_count_round_trip_under_their_tags() {
        let filter = RunFilter { tag: Some("draft".into()), instance: Some(instance("ada")), ..RunFilter::default() };
        let list = ProgramCall::RunsList { filter: filter.clone(), limit: 20 };
        let wire = serde_json::to_value(&list).unwrap();
        assert_eq!(wire, serde_json::json!({ "call": "runs_list", "filter": { "tag": "draft", "instance": "ada" }, "limit": 20 }));
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), list);
        let count = ProgramCall::RunsCount { filter };
        let wire = serde_json::to_value(&count).unwrap();
        assert_eq!(wire["call"], "runs_count");
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), count);
        assert!(!list.takes_down() && !count.takes_down());
        assert_eq!(serde_json::to_value(RunsCounted { total: 3 }).unwrap(), serde_json::json!({ "total": 3 }));
    }

    #[test]
    fn an_empty_filter_says_nothing_on_the_wire() {
        assert_eq!(serde_json::to_value(RunFilter::default()).unwrap(), serde_json::json!({}));
        assert_eq!(serde_json::to_value(CostFilter::default()).unwrap(), serde_json::json!({}));
    }
}

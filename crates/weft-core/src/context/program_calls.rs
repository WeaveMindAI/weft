//! What a program reaches beyond its own run: its infra copies, its
//! triggers, what its instances provide, its instances' connections, the
//! list of its instances, its costs, its runs and its instance tokens.
//! Each builder here ends in one call, carried to the dispatcher
//! as the calling run (`crate::program::ProgramCall`).
//!
//! Every answer is journaled (`ctx.run`), so a body replayed after a
//! durable wait reads it back instead of asking again: a program that
//! terminated a copy does not terminate it twice. A call that takes
//! something down never reaches the run that made it, unless it passes
//! `StopSelf::Include` and the run is among what it reaches; the call then
//! does not return, and the run ends with the rest.
//!
//! An instance is named by the id the program chose for it (`"user-42"`);
//! inside its own project a program may name any instance, since it is the
//! author's code. What an outsider may do is decided at the doors (an
//! instance token, the `Weft-Instance` header), never here.

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::activation::ActivationScope;
use crate::error::{WeftError, WeftResult};
use crate::instance::InstanceId;
use crate::infra::{InfraNodeStatus, TerminateDisks};
use crate::instance_door::ValuesChanged;
use crate::program::{
    CleanOutcome, ConnectionsForgotten, CostFilter, CostRecord, ExecutionPage, InfraCopy, InfraDownAnswer, InfraStartAnswer, InstanceHoldings,
    MintedInstanceToken, PaidBy, ProgramCall, RunFilter, RunsCounted, TokensRevoked, MAX_RUNS_PAGE,
};
use crate::running_policy::{DeactivateSpec, RunningPolicy};
use crate::signal::timer::{Timer, TimerSpec};
use crate::tag::StopSelf;
use crate::ExecutionId;

use super::ExecutionContext;

/// The name an instance token's mint is journaled under. Only the token's id
/// is recorded (never its value): a replay revokes that one and mints a
/// fresh token, since the value cannot be read back.
const MINT_JOURNAL_NAME: &str = "weft.tokens.mint";

/// How long a start waits between looks at a copy coming up.
const START_LOOK_AGAIN: Timer = Timer { spec: TimerSpec::After { duration_ms: 3_000 } };

impl ExecutionContext {
    /// One infra node's copies: the shared one, or an instance's copy of
    /// a node marked `@per_instance` (`.instance(id)`). The node is named
    /// the way the program writes it (`bridge`, or `one.bridge` inside the
    /// file the site `one` includes).
    pub fn infra(&self, node: impl Into<String>) -> InfraCalls<'_> {
        InfraCalls { ctx: self, node: node.into(), instance: None }
    }

    /// One trigger's activation: the shared one, or an instance's
    /// (`.instance(id)`) for a trigger that exists once per instance.
    pub fn trigger(&self, node: impl Into<String>) -> TriggerCalls<'_> {
        TriggerCalls { ctx: self, triggers: vec![node.into()], instance: None }
    }

    /// Every trigger of one owner: every shared trigger, or with
    /// `.instance(id)` every per-instance trigger of that instance.
    pub fn triggers(&self) -> TriggerCalls<'_> {
        TriggerCalls { ctx: self, triggers: Vec::new(), instance: None }
    }

    /// What an instance provides for the program's `@instance_filled`
    /// fields: `ctx.values().instance(id)`.
    pub fn values(&self) -> ValueCalls<'_> {
        ValueCalls { ctx: self }
    }

    /// An instance's connections: `ctx.connections().instance(id)`.
    pub fn connections(&self) -> ConnectionCalls<'_> {
        ConnectionCalls { ctx: self }
    }

    /// The project's instances: `ctx.instances().list()`.
    pub fn instances(&self) -> InstanceCalls<'_> {
        InstanceCalls { ctx: self }
    }

    /// What the project's calls cost, narrowed by the filters chained on
    /// it, read with `.list()`. Weft reports the price it measured; what
    /// anybody is billed is the program's to decide.
    pub fn costs(&self) -> CostQuery<'_> {
        CostQuery { ctx: self, instance: None, filter: CostFilter::default() }
    }

    /// The project's runs, narrowed by the filters chained on it: listed
    /// with `.list(..)`, counted with `.count()`, deleted with `.clean(..)`.
    pub fn runs(&self) -> RunQuery<'_> {
        RunQuery { ctx: self, instance: None, filter: RunFilter::default() }
    }

    /// Instance tokens: `mint_for_instance` hands a browser or extension
    /// a credential that acts inside one instance of this project.
    pub fn tokens(&self) -> TokenCalls<'_> {
        TokenCalls { ctx: self }
    }

    /// Carry one call as this run, journaled so a replay reads the
    /// answer back, and decode its answer. The call is keyed on its
    /// journal step, which a replay reaches at the same index, so a call
    /// carried again (its answer lost) is the same call.
    async fn program_call<T: DeserializeOwned>(&self, call: ProgramCall, stop_self: StopSelf) -> WeftResult<T> {
        let name = call.journal_name();
        let (call_index, replayed) = self.handle.run_step(name).await?;
        let value = match replayed {
            Some(value) => value,
            None => {
                let value = crate::storage::media::strip_links(&self.handle.program_call(call, stop_self, call_index).await?);
                self.handle.run_record(name, call_index, &value).await?;
                value
            }
        };
        serde_json::from_value(value)
            .map_err(|e| WeftError::NodeExecution(format!("{name}: the runtime answered a shape this node cannot read: {e}")))
    }
}

/// An instance id as the program wrote it, checked when a call is made so
/// a builder chain stays a chain.
fn instance_id(raw: &Option<String>) -> WeftResult<Option<InstanceId>> {
    raw.as_deref()
        .map(|m| InstanceId::new(m).map_err(|e| WeftError::Input(format!("instance id: {e}"))))
        .transpose()
}

fn required_instance(raw: &str) -> WeftResult<InstanceId> {
    InstanceId::new(raw).map_err(|e| WeftError::Input(format!("instance id: {e}")))
}

/// One infra node's copies. See [`ExecutionContext::infra`].
pub struct InfraCalls<'a> {
    ctx: &'a ExecutionContext,
    node: String,
    instance: Option<String>,
}

impl InfraCalls<'_> {
    /// That instance's copy, of a node marked `@per_instance`.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    /// Bring the copy up, and return once it answers, so what comes
    /// next (activating the triggers that read it, say) can use it: a
    /// [`Self::request_start`], then a wait for the copy to run.
    /// Starting a copy that runs, or is already starting, does not start
    /// it twice. A copy that fails to come up fails this call with the
    /// reason, and so does one somebody stops (or terminates) while this
    /// waits: the start never undoes their stop.
    ///
    /// The wait has no deadline (a copy pulling a large image takes as
    /// long as it takes) and holds no worker: between looks the run
    /// parks on a timer, so a start that moves the project's workers
    /// next to their infra never waits on the run that asked for it.
    pub async fn start(self) -> WeftResult<()> {
        let instance = instance_id(&self.instance)?;
        self.ask_until_accepted(&instance).await?;
        let node = self.node;
        loop {
            // An accepted start reads `provisioning` from the moment the
            // runtime accepted it (the runtime counts the start under way,
            // not only what the copy's row says), so every other answer is
            // where the copy really stands.
            let status = ProgramCall::InfraStatus { node: node.clone(), instance: instance.clone() };
            let copy: Option<InfraCopy> = self.ctx.program_call(status, StopSelf::Keep).await?;
            match copy.as_ref().map(|c| c.status) {
                Some(InfraNodeStatus::Running) => return Ok(()),
                Some(InfraNodeStatus::Failed) => {
                    let why = copy.and_then(|c| c.failure).unwrap_or_else(|| "no reason recorded".into());
                    return Err(WeftError::NodeExecution(format!(
                        "infra '{node}' failed to start: {why}; `weft infra status` shows the copy"
                    )));
                }
                None => {
                    return Err(WeftError::NodeExecution(format!(
                        "infra '{node}' did not come up: its start ended without a copy (the setup run failed \
                         or was cancelled, or somebody terminated it); `weft executions` shows the setup run"
                    )));
                }
                Some(stopped @ (InfraNodeStatus::Stopped | InfraNodeStatus::Stopping | InfraNodeStatus::Terminating)) => {
                    return Err(WeftError::NodeExecution(format!(
                        "infra '{node}' is {stopped}: somebody took it down while this start waited for it"
                    )));
                }
                Some(InfraNodeStatus::Provisioning | InfraNodeStatus::Flaky) => {}
            }
            self.ctx.await_signal(START_LOOK_AGAIN).await?;
        }
    }

    /// Ask for the copy to come up, and return once weft accepted the
    /// start, without waiting for the copy to run. From then on a status
    /// read ([`Self::status`], [`Self::copies`]) shows the copy as
    /// `provisioning`, so a program answering a caller fast (a route
    /// replying 202) replies after this and the caller's next read finds
    /// the copy. A start refused for a passing reason (a build in
    /// progress, a trigger of the copy mid-activation) is asked again
    /// every few seconds until it is accepted; a copy already running or
    /// starting is not started twice. Whether the copy then comes up is
    /// for a later `status` read to tell: nothing here waits for it.
    pub async fn request_start(self) -> WeftResult<()> {
        let instance = instance_id(&self.instance)?;
        self.ask_until_accepted(&instance).await
    }

    /// Ask for the start until weft accepts it, parking on a timer
    /// between refusals for a passing reason.
    async fn ask_until_accepted(&self, instance: &Option<InstanceId>) -> WeftResult<()> {
        loop {
            let call = ProgramCall::InfraStart { node: self.node.clone(), instance: instance.clone() };
            let answer: InfraStartAnswer = self.ctx.program_call(call, StopSelf::Keep).await?;
            if answer.accepted() {
                return Ok(());
            }
            self.ctx.await_signal(START_LOOK_AGAIN).await?;
        }
    }

    /// Scale the copy down, keeping its disk. `spec` says what happens to
    /// the triggers reading it and the runs using it; `stop_self` whether
    /// this run goes too when it uses the copy. Answers whether there was
    /// a copy to take down ([`InfraDownAnswer`]), so a program can tell an
    /// instance with no copy (a mistyped id, one terminated before) apart.
    pub async fn stop(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        let call = ProgramCall::InfraStop { node: self.node, instance: instance_id(&self.instance)?, spec };
        self.ctx.program_call(call, stop_self).await
    }

    /// Delete the copy and its disks, keeping the ones its node lists in
    /// `keepOnTerminate` (a later start of the same copy finds them
    /// again). Same `spec` and `stop_self` as [`Self::stop`].
    pub async fn terminate(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        self.take_away(spec, stop_self, TerminateDisks::KeepListed).await
    }

    /// Delete the copy and every one of its disks, the ones listed in
    /// `keepOnTerminate` too: the copy's owner is going for good (a
    /// instance being wiped). Same `spec` and `stop_self` as [`Self::stop`].
    pub async fn wipe(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        self.take_away(spec, stop_self, TerminateDisks::DeleteAll).await
    }

    async fn take_away(self, spec: DeactivateSpec, stop_self: StopSelf, disks: TerminateDisks) -> WeftResult<InfraDownAnswer> {
        let call = ProgramCall::InfraTerminate { node: self.node, instance: instance_id(&self.instance)?, spec, disks };
        self.ctx.program_call(call, stop_self).await
    }

    /// The copy's state now, or `None` when it was never started (or was
    /// terminated).
    pub async fn status(self) -> WeftResult<Option<InfraCopy>> {
        let call = ProgramCall::InfraStatus { node: self.node, instance: instance_id(&self.instance)? };
        self.ctx.program_call(call, StopSelf::Keep).await
    }

    /// Every copy of the node that exists: the shared one and each
    /// instance's.
    pub async fn copies(self) -> WeftResult<Vec<InfraCopy>> {
        self.ctx.program_call(ProgramCall::InfraCopies { node: self.node }, StopSelf::Keep).await
    }
}

/// Triggers' activations. See [`ExecutionContext::trigger`] and
/// [`ExecutionContext::triggers`].
pub struct TriggerCalls<'a> {
    ctx: &'a ExecutionContext,
    triggers: Vec<String>,
    instance: Option<String>,
}

impl TriggerCalls<'_> {
    /// Only these triggers (named the way the program writes them), on
    /// top of the ones already named; none named is every trigger of the
    /// owner.
    pub fn only<S: Into<String>>(mut self, triggers: impl IntoIterator<Item = S>) -> Self {
        self.triggers.extend(triggers.into_iter().map(Into::into));
        self
    }

    /// That instance's activations, of triggers that exist once per
    /// instance.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    fn scope(&self) -> WeftResult<ActivationScope> {
        Ok(ActivationScope { triggers: self.triggers.clone(), instance: instance_id(&self.instance)? })
    }

    /// Activate them. An instance's trigger reading that instance's infra copy
    /// needs the copy running first. Activating what is active does
    /// nothing.
    pub async fn activate(self) -> WeftResult<()> {
        let call = ProgramCall::TriggerActivate { scope: self.scope()? };
        self.ctx.program_call::<Value>(call, StopSelf::Keep).await.map(drop)
    }

    /// Take them down under `spec` (wipe, hibernate or park; cancel or
    /// wait for the runs they fired). `stop_self` decides this run's fate
    /// when one of them fired it.
    pub async fn deactivate(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<()> {
        let call = ProgramCall::TriggerDeactivate { scope: self.scope()?, spec };
        self.ctx.program_call::<Value>(call, stop_self).await.map(drop)
    }
}

/// See [`ExecutionContext::connections`].
pub struct ConnectionCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> ConnectionCalls<'a> {
    /// One instance's connections.
    pub fn instance(self, id: impl Into<String>) -> InstanceConnections<'a> {
        InstanceConnections { ctx: self.ctx, instance: id.into() }
    }
}

/// One instance's connections.
pub struct InstanceConnections<'a> {
    ctx: &'a ExecutionContext,
    instance: String,
}

impl InstanceConnections<'_> {
    /// Every connection the instance has in this project.
    pub async fn list(self) -> WeftResult<Vec<crate::access::wire::GrantSummary>> {
        let call = ProgramCall::ConnectionsList { instance: required_instance(&self.instance)? };
        self.ctx.program_call(call, StopSelf::Keep).await
    }

    /// Forget every connection the instance has in this project; the
    /// values naming them go with them. Answers how many went.
    pub async fn forget(self) -> WeftResult<u64> {
        let call = ProgramCall::ConnectionsForget { instance: required_instance(&self.instance)? };
        let answer: ConnectionsForgotten = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.forgotten)
    }
}

/// See [`ExecutionContext::instances`].
pub struct InstanceCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl InstanceCalls<'_> {
    /// Every instance weft holds anything for in this project (a value,
    /// a connection, an infra copy, an instance token, a trigger
    /// activation), one entry each with what it holds: counts and states,
    /// never a value or a secret. A trigger's fires waiting on a value the
    /// instance has not given show under that trigger, with the reason.
    pub async fn list(self) -> WeftResult<Vec<InstanceHoldings>> {
        self.ctx.program_call(ProgramCall::InstancesList, StopSelf::Keep).await
    }
}

/// See [`ExecutionContext::values`].
pub struct ValueCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> ValueCalls<'a> {
    /// One instance's values.
    pub fn instance(self, id: impl Into<String>) -> InstanceValueCalls<'a> {
        InstanceValueCalls { ctx: self.ctx, instance: id.into(), set: Vec::new(), clear: Vec::new() }
    }
}

/// One instance's values: read them with [`Self::get`], or chain
/// [`Self::set`] and [`Self::clear`] and make the change with
/// [`Self::apply`].
pub struct InstanceValueCalls<'a> {
    ctx: &'a ExecutionContext,
    instance: String,
    set: Vec<crate::run_spec::InstanceValueInput>,
    clear: Vec<crate::run_spec::InstanceFieldRef>,
}

impl InstanceValueCalls<'_> {
    /// What the instance gives, by step (named the way the program writes
    /// it) and field. A connection reads as its `{id, identity}` handle.
    pub async fn get(self) -> WeftResult<crate::instance::InstanceValues> {
        let call = ProgramCall::ValuesGet { instance: required_instance(&self.instance)? };
        self.ctx.program_call(call, StopSelf::Keep).await
    }

    /// Give `value` for `field` of the step `step`, a field the program
    /// writes `@instance_filled`. For a connection field the value is one
    /// of the instance's connections: `json!({ "id": <connection id> })`.
    pub fn set(mut self, step: impl Into<String>, field: impl Into<String>, value: Value) -> Self {
        self.set.push(crate::run_spec::InstanceValueInput { step: step.into(), field: field.into(), value });
        self
    }

    /// Forget everything the instance provides, in one change (what
    /// wiping an instance does); its live triggers reading any of it are set up
    /// again. Answers the triggers set up again.
    pub async fn forget(self) -> WeftResult<Vec<String>> {
        let call = ProgramCall::ValuesForget { instance: required_instance(&self.instance)? };
        let answer: ValuesChanged = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.rearmed)
    }

    /// Forget the instance's value for `field` of `step`: the field then
    /// falls back as if never given.
    pub fn clear(mut self, step: impl Into<String>, field: impl Into<String>) -> Self {
        self.clear.push(crate::run_spec::InstanceFieldRef { step: step.into(), field: field.into() });
        self
    }

    /// Make the change, all or none: each value is held to its field and
    /// to the node's rules first, and a live trigger of the instance that
    /// reads one is set up again with the new values before this returns.
    /// Answers the triggers set up again.
    pub async fn apply(self) -> WeftResult<Vec<String>> {
        let call = ProgramCall::ValuesChange { instance: required_instance(&self.instance)?, set: self.set, clear: self.clear };
        let answer: ValuesChanged = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.rearmed)
    }
}

/// See [`ExecutionContext::costs`].
pub struct CostQuery<'a> {
    ctx: &'a ExecutionContext,
    instance: Option<String>,
    filter: CostFilter,
}

impl CostQuery<'_> {
    /// Costs of that instance's runs.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    /// Costs one node booked, named the way the program writes it.
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.filter.node = Some(node.into());
        self
    }

    /// Costs of one service (`openrouter`, `elevenlabs`).
    pub fn service(mut self, service: impl Into<String>) -> Self {
        self.filter.service = Some(service.into());
        self
    }

    /// Costs paid from one kind of credential: the runtime's, the
    /// author's, or an instance's own.
    pub fn paid_by(mut self, paid_by: PaidBy) -> Self {
        self.filter.paid_by = Some(paid_by);
        self
    }

    /// Costs of one run (`ctx.execution_id` for this one).
    pub fn run(mut self, execution_id: ExecutionId) -> Self {
        self.filter.run = Some(execution_id);
        self
    }

    /// Costs booked at or after this unix second.
    pub fn since(mut self, unix: u64) -> Self {
        self.filter.since_unix = Some(unix);
        self
    }

    pub async fn list(mut self) -> WeftResult<Vec<CostRecord>> {
        self.filter.instance = instance_id(&self.instance)?;
        self.ctx.program_call(ProgramCall::CostsList { filter: self.filter }, StopSelf::Keep).await
    }
}

/// See [`ExecutionContext::runs`].
pub struct RunQuery<'a> {
    ctx: &'a ExecutionContext,
    instance: Option<String>,
    filter: RunFilter,
}

impl RunQuery<'_> {
    /// Runs for that instance.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    /// Runs standing this way: `running` (not ended, a run parked on a
    /// wait included), `waiting_for_input` (parked on a wait),
    /// `completed`, `failed` or `cancelled`. Any other word is refused
    /// naming these, rather than matching nothing.
    pub fn status(mut self, status: &str) -> WeftResult<Self> {
        let parsed = crate::program::RunStatus::parse(status).ok_or_else(|| WeftError::Input(format!(
            "runs: '{status}' is not a run status; use one of: {}",
            crate::program::RunStatus::accepted()
        )))?;
        self.filter.status = Some(parsed);
        Ok(self)
    }

    /// Runs started by this node (the trigger that fired, or the node a
    /// run was started from).
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.filter.node = Some(node.into());
        self
    }

    /// Runs carrying this tag (`ctx.tag_execution`), normalized the
    /// same way, so the value a run tagged itself with finds it. An
    /// empty tag is refused.
    pub fn tag(mut self, tag: &str) -> WeftResult<Self> {
        let tag = crate::tag::normalize_tag(tag).map_err(|e| WeftError::Input(format!("runs: {e}")))?;
        self.filter.tag = Some(tag);
        Ok(self)
    }

    /// Runs started at least this long ago.
    pub fn older_than(mut self, age: std::time::Duration) -> Self {
        self.filter.older_than_secs = Some(age.as_secs());
        self
    }

    /// Delete them. A run still going is never deleted from under itself:
    /// `running: Cancel` stops it (its rows go with the next clean),
    /// `Wait` leaves it running. This run is never deleted; with
    /// `StopSelf::Include` and matching the filters, it is stopped.
    pub async fn clean(mut self, running: RunningPolicy, stop_self: StopSelf) -> WeftResult<CleanOutcome> {
        self.filter.instance = instance_id(&self.instance)?;
        self.ctx.program_call(ProgramCall::RunsClean { filter: self.filter, running }, stop_self).await
    }

    /// The newest `limit` of them (1 to [`MAX_RUNS_PAGE`]), newest first,
    /// with how many match in all (`total`), so a listing that stopped
    /// short says so. A run parked on a wait reads `waiting_for_input`.
    pub async fn list(mut self, limit: u32) -> WeftResult<ExecutionPage> {
        if !(1..=MAX_RUNS_PAGE).contains(&limit) {
            return Err(WeftError::Input(format!(
                "runs list: a limit of {limit} is out of range; give 1 to {MAX_RUNS_PAGE}, and narrow the filters to reach older runs"
            )));
        }
        self.filter.instance = instance_id(&self.instance)?;
        self.ctx.program_call(ProgramCall::RunsList { filter: self.filter, limit }, StopSelf::Keep).await
    }

    /// How many of them there are.
    pub async fn count(mut self) -> WeftResult<u64> {
        self.filter.instance = instance_id(&self.instance)?;
        let answer: RunsCounted = self.ctx.program_call(ProgramCall::RunsCount { filter: self.filter }, StopSelf::Keep).await?;
        Ok(answer.total)
    }
}

/// See [`ExecutionContext::tokens`].
pub struct TokenCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> TokenCalls<'a> {
    /// A token that acts inside one instance of this project, `instance`,
    /// for a browser or extension: it starts that instance's runs
    /// (through the project's live routes, in the `Weft-Instance-Token`
    /// header), answers its waits, reads its own copies' displays and
    /// manages its connections, and nothing else. `expires_in` is required (a year at
    /// most). The value is in the answer once and never again; the
    /// runtime keeps only its hash.
    ///
    /// The token's id is chosen here and journaled BEFORE the mint, so a
    /// run replayed past this call mints under the same id: the token is
    /// replaced (a new value, since the old one cannot be read back, and
    /// the old value stops working), never left beside a second one.
    pub async fn mint_for_instance(self, instance: impl Into<String>, expires_in: std::time::Duration) -> WeftResult<MintedInstanceToken> {
        let instance = required_instance(&instance.into())?;
        // The expiry is refused before the id is journaled, so a refused
        // mint leaves no record of a token that never existed.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| WeftError::NodeExecution(format!("the clock is before 1970: {e}")))?
            .as_secs();
        crate::signal_token::expiry_at(now, expires_in.as_secs()).map_err(WeftError::Input)?;
        let (call_index, replayed) = self.ctx.handle.run_step(MINT_JOURNAL_NAME).await?;
        let id = match replayed {
            Some(recorded) => recorded
                .get("id")
                .and_then(Value::as_str)
                .and_then(|id| id.parse::<uuid::Uuid>().ok())
                .ok_or_else(|| WeftError::NodeExecution(format!("the journaled instance token mint is unreadable: {recorded}")))?,
            None => {
                let id = uuid::Uuid::new_v4();
                self.ctx.handle.run_record(MINT_JOURNAL_NAME, call_index, &serde_json::json!({ "id": id })).await?;
                id
            }
        };
        self.ctx.handle.mint_instance_token(&instance, expires_in.as_secs(), true, id).await
    }

    /// One instance's tokens.
    pub fn instance(self, id: impl Into<String>) -> InstanceTokens<'a> {
        InstanceTokens { ctx: self.ctx, instance: id.into() }
    }
}

/// One instance's tokens.
pub struct InstanceTokens<'a> {
    ctx: &'a ExecutionContext,
    instance: String,
}

impl InstanceTokens<'_> {
    /// Revoke every token of the instance in this project. Answers how many
    /// went.
    pub async fn revoke(self) -> WeftResult<u64> {
        let call = ProgramCall::TokensRevoke { instance: required_instance(&self.instance)?, id: None };
        let answer: TokensRevoked = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.revoked)
    }
}

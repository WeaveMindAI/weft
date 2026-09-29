//! What a program reaches beyond its own run: its infra copies, its
//! triggers, what its members provide, its members' connections, the list
//! of its members, its costs, its runs and its member tokens. Each builder here ends in one call, carried to the dispatcher
//! as the calling run (`crate::program::ProgramCall`).
//!
//! Every answer is journaled (`ctx.run`), so a run replayed after a crash
//! or a resume reads it back instead of asking again: a program that
//! terminated a copy does not terminate it twice. A call that takes
//! something down never reaches the run that made it, unless it passes
//! `StopSelf::Include` and the run is among what it reaches; the call then
//! does not return, and the run ends with the rest.
//!
//! A member is named by the id the program chose for them (`"user-42"`);
//! inside its own project a program may name any member, since it is the
//! author's code. What an outsider may do is decided at the doors (a member
//! token, the `Weft-Member` header), never here.

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::activation::ActivationScope;
use crate::error::{WeftError, WeftResult};
use crate::member::MemberId;
use crate::infra::{InfraNodeStatus, TerminateDisks};
use crate::member_door::ValuesChanged;
use crate::program::{
    CleanOutcome, ConnectionsForgotten, CostFilter, CostRecord, InfraCopy, InfraStartAnswer, MemberHoldings, MintedMemberToken,
    PaidBy, ProgramCall, RunFilter, TokensRevoked,
};
use crate::running_policy::{DeactivateSpec, RunningPolicy};
use crate::signal::timer::{Timer, TimerSpec};
use crate::tag::StopSelf;
use crate::ExecutionId;

use super::ExecutionContext;

/// The name a member token's mint is journaled under. Only the token's id
/// is recorded (never its value): a replay revokes that one and mints a
/// fresh token, since the value cannot be read back.
const MINT_JOURNAL_NAME: &str = "weft.tokens.mint";

/// How long a start waits between looks at a copy coming up.
const START_LOOK_AGAIN: Timer = Timer { spec: TimerSpec::After { duration_ms: 3_000 } };

impl ExecutionContext {
    /// One infra node's copies: the shared one, or a member's copy of a
    /// node marked `@per_member` (`.member(id)`). The node is named the way
    /// the program writes it (`bridge`, or `one.bridge` inside the file the
    /// site `one` includes).
    pub fn infra(&self, node: impl Into<String>) -> InfraCalls<'_> {
        InfraCalls { ctx: self, node: node.into(), member: None }
    }

    /// One trigger's activation: the shared one, or a member's
    /// (`.member(id)`) for a trigger that exists once per member.
    pub fn trigger(&self, node: impl Into<String>) -> TriggerCalls<'_> {
        TriggerCalls { ctx: self, triggers: vec![node.into()], member: None }
    }

    /// Every trigger of one owner: every shared trigger, or with
    /// `.member(id)` every per-member trigger of that member.
    pub fn triggers(&self) -> TriggerCalls<'_> {
        TriggerCalls { ctx: self, triggers: Vec::new(), member: None }
    }

    /// What a member provides for the program's `@member_filled` fields:
    /// `ctx.values().member(id)`.
    pub fn values(&self) -> ValueCalls<'_> {
        ValueCalls { ctx: self }
    }

    /// A member's connections: `ctx.connections().member(id)`.
    pub fn connections(&self) -> ConnectionCalls<'_> {
        ConnectionCalls { ctx: self }
    }

    /// The project's members: `ctx.members().list()`.
    pub fn members(&self) -> MemberCalls<'_> {
        MemberCalls { ctx: self }
    }

    /// What the project's calls cost, narrowed by the filters chained on
    /// it, read with `.list()`. Weft reports the price it measured; what
    /// anybody is billed is the program's to decide.
    pub fn costs(&self) -> CostQuery<'_> {
        CostQuery { ctx: self, member: None, filter: CostFilter::default() }
    }

    /// The project's runs, narrowed by the filters chained on it, deleted
    /// with `.clean(..)`.
    pub fn runs(&self) -> RunQuery<'_> {
        RunQuery { ctx: self, member: None, filter: RunFilter::default() }
    }

    /// Member tokens: `mint_for_member` hands a browser or extension a
    /// credential that acts as one member of this project.
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

/// A member id as the program wrote it, checked when a call is made so a
/// builder chain stays a chain.
fn member_id(raw: &Option<String>) -> WeftResult<Option<MemberId>> {
    raw.as_deref()
        .map(|m| MemberId::new(m).map_err(|e| WeftError::Input(format!("member id: {e}"))))
        .transpose()
}

fn required_member(raw: &str) -> WeftResult<MemberId> {
    MemberId::new(raw).map_err(|e| WeftError::Input(format!("member id: {e}")))
}

/// One infra node's copies. See [`ExecutionContext::infra`].
pub struct InfraCalls<'a> {
    ctx: &'a ExecutionContext,
    node: String,
    member: Option<String>,
}

impl InfraCalls<'_> {
    /// That member's copy, of a node marked `@per_member`.
    pub fn member(mut self, id: impl Into<String>) -> Self {
        self.member = Some(id.into());
        self
    }

    /// Bring the copy up, and return once it answers, so what comes
    /// next (activating the triggers that read it, say) can use it.
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
        let member = member_id(&self.member)?;
        let node = self.node;
        let start = || ProgramCall::InfraStart { node: node.clone(), member: member.clone() };
        // Until a start is queued (one refused for a passing reason, a
        // build in progress or a trigger of the copy mid-activation, is
        // asked again at each look), what the copy reads is no answer to
        // this start.
        let mut queued = false;
        loop {
            if !queued {
                let answer: InfraStartAnswer = self.ctx.program_call(start(), StopSelf::Keep).await?;
                queued = answer.queued();
                if !queued {
                    self.ctx.await_signal(START_LOOK_AGAIN).await?;
                    continue;
                }
            }
            // A queued start reads `provisioning` from the moment the call
            // above answered (the runtime counts the start under way, not
            // only what the copy's row says), so every other answer is
            // where the copy really stands.
            let status = ProgramCall::InfraStatus { node: node.clone(), member: member.clone() };
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

    /// Scale the copy down, keeping its disk. `spec` says what happens to
    /// the triggers reading it and the runs using it; `stop_self` whether
    /// this run goes too when it uses the copy.
    pub async fn stop(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<()> {
        let call = ProgramCall::InfraStop { node: self.node, member: member_id(&self.member)?, spec };
        self.ctx.program_call::<Value>(call, stop_self).await.map(drop)
    }

    /// Delete the copy and its disks, keeping the ones its node lists in
    /// `keepOnTerminate` (a later start of the same copy finds them
    /// again). Same `spec` and `stop_self` as [`Self::stop`].
    pub async fn terminate(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<()> {
        self.take_away(spec, stop_self, TerminateDisks::KeepListed).await
    }

    /// Delete the copy and every one of its disks, the ones listed in
    /// `keepOnTerminate` too: the copy's owner is going for good (a
    /// member being wiped). Same `spec` and `stop_self` as [`Self::stop`].
    pub async fn wipe(self, spec: DeactivateSpec, stop_self: StopSelf) -> WeftResult<()> {
        self.take_away(spec, stop_self, TerminateDisks::DeleteAll).await
    }

    async fn take_away(self, spec: DeactivateSpec, stop_self: StopSelf, disks: TerminateDisks) -> WeftResult<()> {
        let call = ProgramCall::InfraTerminate { node: self.node, member: member_id(&self.member)?, spec, disks };
        self.ctx.program_call::<Value>(call, stop_self).await.map(drop)
    }

    /// The copy's state now, or `None` when it was never started (or was
    /// terminated).
    pub async fn status(self) -> WeftResult<Option<InfraCopy>> {
        let call = ProgramCall::InfraStatus { node: self.node, member: member_id(&self.member)? };
        self.ctx.program_call(call, StopSelf::Keep).await
    }

    /// Every copy of the node that exists: the shared one and each
    /// member's.
    pub async fn copies(self) -> WeftResult<Vec<InfraCopy>> {
        self.ctx.program_call(ProgramCall::InfraCopies { node: self.node }, StopSelf::Keep).await
    }
}

/// Triggers' activations. See [`ExecutionContext::trigger`] and
/// [`ExecutionContext::triggers`].
pub struct TriggerCalls<'a> {
    ctx: &'a ExecutionContext,
    triggers: Vec<String>,
    member: Option<String>,
}

impl TriggerCalls<'_> {
    /// Only these triggers (named the way the program writes them), on
    /// top of the ones already named; none named is every trigger of the
    /// owner.
    pub fn only<S: Into<String>>(mut self, triggers: impl IntoIterator<Item = S>) -> Self {
        self.triggers.extend(triggers.into_iter().map(Into::into));
        self
    }

    /// That member's activations, of triggers that exist once per member.
    pub fn member(mut self, id: impl Into<String>) -> Self {
        self.member = Some(id.into());
        self
    }

    fn scope(&self) -> WeftResult<ActivationScope> {
        Ok(ActivationScope { triggers: self.triggers.clone(), member: member_id(&self.member)? })
    }

    /// Activate them. A member's trigger reading that member's infra copy
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
    /// One member's connections.
    pub fn member(self, id: impl Into<String>) -> MemberConnections<'a> {
        MemberConnections { ctx: self.ctx, member: id.into() }
    }
}

/// One member's connections.
pub struct MemberConnections<'a> {
    ctx: &'a ExecutionContext,
    member: String,
}

impl MemberConnections<'_> {
    /// Every connection the member has in this project.
    pub async fn list(self) -> WeftResult<Vec<crate::access::wire::GrantSummary>> {
        let call = ProgramCall::ConnectionsList { member: required_member(&self.member)? };
        self.ctx.program_call(call, StopSelf::Keep).await
    }

    /// Forget every connection the member has in this project; the
    /// values naming them go with them. Answers how many went.
    pub async fn forget(self) -> WeftResult<u64> {
        let call = ProgramCall::ConnectionsForget { member: required_member(&self.member)? };
        let answer: ConnectionsForgotten = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.forgotten)
    }
}

/// See [`ExecutionContext::members`].
pub struct MemberCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl MemberCalls<'_> {
    /// Every member weft holds anything for in this project (a value, a
    /// connection, an infra copy, a member token, a trigger activation),
    /// one entry each with what it holds: counts and states, never a
    /// value or a secret. A trigger's fires waiting on a value the member
    /// has not given show under that trigger, with the reason.
    pub async fn list(self) -> WeftResult<Vec<MemberHoldings>> {
        self.ctx.program_call(ProgramCall::MembersList, StopSelf::Keep).await
    }
}

/// See [`ExecutionContext::values`].
pub struct ValueCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> ValueCalls<'a> {
    /// One member's values.
    pub fn member(self, id: impl Into<String>) -> MemberValueCalls<'a> {
        MemberValueCalls { ctx: self.ctx, member: id.into(), set: Vec::new(), clear: Vec::new() }
    }
}

/// One member's values: read them with [`Self::get`], or chain
/// [`Self::set`] and [`Self::clear`] and make the change with
/// [`Self::apply`].
pub struct MemberValueCalls<'a> {
    ctx: &'a ExecutionContext,
    member: String,
    set: Vec<crate::run_spec::MemberValueInput>,
    clear: Vec<crate::run_spec::MemberFieldRef>,
}

impl MemberValueCalls<'_> {
    /// What the member gives, by step (named the way the program writes
    /// it) and field. A connection reads as its `{id, identity}` handle.
    pub async fn get(self) -> WeftResult<crate::member::MemberValues> {
        let call = ProgramCall::ValuesGet { member: required_member(&self.member)? };
        self.ctx.program_call(call, StopSelf::Keep).await
    }

    /// Give `value` for `field` of the step `step`, a field the program
    /// writes `@member_filled`. For a connection field the value is one
    /// of the member's connections: `json!({ "id": <connection id> })`.
    pub fn set(mut self, step: impl Into<String>, field: impl Into<String>, value: Value) -> Self {
        self.set.push(crate::run_spec::MemberValueInput { step: step.into(), field: field.into(), value });
        self
    }

    /// Forget everything the member provides, in one change (what wiping
    /// a member does); their live triggers reading any of it are set up
    /// again. Answers the triggers set up again.
    pub async fn forget(self) -> WeftResult<Vec<String>> {
        let call = ProgramCall::ValuesForget { member: required_member(&self.member)? };
        let answer: ValuesChanged = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.rearmed)
    }

    /// Forget the member's value for `field` of `step`: the field then
    /// falls back as if never given.
    pub fn clear(mut self, step: impl Into<String>, field: impl Into<String>) -> Self {
        self.clear.push(crate::run_spec::MemberFieldRef { step: step.into(), field: field.into() });
        self
    }

    /// Make the change, all or none: each value is held to its field and
    /// to the node's rules first, and a live trigger of the member that
    /// reads one is set up again with the new values before this returns.
    /// Answers the triggers set up again.
    pub async fn apply(self) -> WeftResult<Vec<String>> {
        let call = ProgramCall::ValuesChange { member: required_member(&self.member)?, set: self.set, clear: self.clear };
        let answer: ValuesChanged = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.rearmed)
    }
}

/// See [`ExecutionContext::costs`].
pub struct CostQuery<'a> {
    ctx: &'a ExecutionContext,
    member: Option<String>,
    filter: CostFilter,
}

impl CostQuery<'_> {
    /// Costs of that member's runs.
    pub fn member(mut self, id: impl Into<String>) -> Self {
        self.member = Some(id.into());
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
    /// author's, or a member's own.
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
        self.filter.member = member_id(&self.member)?;
        self.ctx.program_call(ProgramCall::CostsList { filter: self.filter }, StopSelf::Keep).await
    }
}

/// See [`ExecutionContext::runs`].
pub struct RunQuery<'a> {
    ctx: &'a ExecutionContext,
    member: Option<String>,
    filter: RunFilter,
}

impl RunQuery<'_> {
    /// Runs for that member.
    pub fn member(mut self, id: impl Into<String>) -> Self {
        self.member = Some(id.into());
        self
    }

    /// Runs that ended this way (`completed`, `failed`, `cancelled`), or
    /// `running` for the ones still going.
    pub fn status(mut self, status: impl Into<String>) -> Self {
        self.filter.status = Some(status.into());
        self
    }

    /// Runs started by this node (the trigger that fired, or the node a
    /// run was started from).
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.filter.node = Some(node.into());
        self
    }

    /// Runs carrying this tag (`ctx.tag_execution`).
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.filter.tag = Some(tag.into());
        self
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
        self.filter.member = member_id(&self.member)?;
        self.ctx.program_call(ProgramCall::RunsClean { filter: self.filter, running }, stop_self).await
    }
}

/// See [`ExecutionContext::tokens`].
pub struct TokenCalls<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> TokenCalls<'a> {
    /// A token acting as `member` of this project, for that member's
    /// browser or extension: it starts runs as them (through the project's
    /// live routes, in the `Weft-Member-Token` header), answers their
    /// waits, reads their own copies' displays and manages their
    /// connections, and nothing else. `expires_in` is required (a year at
    /// most). The value is in the answer once and never again; the
    /// runtime keeps only its hash.
    ///
    /// The token's id is chosen here and journaled BEFORE the mint, so a
    /// run replayed past this call mints under the same id: the token is
    /// replaced (a new value, since the old one cannot be read back, and
    /// the old value stops working), never left beside a second one.
    pub async fn mint_for_member(self, member: impl Into<String>, expires_in: std::time::Duration) -> WeftResult<MintedMemberToken> {
        let member = required_member(&member.into())?;
        let (call_index, replayed) = self.ctx.handle.run_step(MINT_JOURNAL_NAME).await?;
        let id = match replayed {
            Some(recorded) => recorded
                .get("id")
                .and_then(Value::as_str)
                .and_then(|id| id.parse::<uuid::Uuid>().ok())
                .ok_or_else(|| WeftError::NodeExecution(format!("the journaled member token mint is unreadable: {recorded}")))?,
            None => {
                let id = uuid::Uuid::new_v4();
                self.ctx.handle.run_record(MINT_JOURNAL_NAME, call_index, &serde_json::json!({ "id": id })).await?;
                id
            }
        };
        self.ctx.handle.mint_member_token(&member, expires_in.as_secs(), true, id).await
    }

    /// One member's tokens.
    pub fn member(self, id: impl Into<String>) -> MemberTokens<'a> {
        MemberTokens { ctx: self.ctx, member: id.into() }
    }
}

/// One member's tokens.
pub struct MemberTokens<'a> {
    ctx: &'a ExecutionContext,
    member: String,
}

impl MemberTokens<'_> {
    /// Revoke every token of the member in this project. Answers how many
    /// went.
    pub async fn revoke(self) -> WeftResult<u64> {
        let call = ProgramCall::TokensRevoke { member: required_member(&self.member)?, id: None };
        let answer: TokensRevoked = self.ctx.program_call(call, StopSelf::Keep).await?;
        Ok(answer.revoked)
    }
}

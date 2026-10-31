//! `ctx.infra(..)`: one infra node's copies; `ctx.all_infra()`: what
//! reaches every copy of the project at once. The verbs of `weft infra`.

use crate::infra::wire::{CommandStatus, DoorsResponse, InfraLogs, LifecycleCommandIssued, LogCursor};
use crate::infra::{InfraNodeStatus, TerminateDisks};
use crate::instance::InstanceId;
use crate::live::{LiveFeed, LivePress};
use crate::program::infra::{InfraCall, InfraCancelAnswer, InfraLogsRequest, RebakeAnswer, SetupEnd};
use crate::program::{InfraCopy, InfraDownAnswer, InfraStartAnswer};
use crate::signal::timer::{Timer, TimerSpec};
use crate::take_down::TakeDownChoice;
use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::program::ProgramCall;
use crate::tag::StopSelf;

use super::{instance_id, ProjectCalls};

/// How long a call that follows something slow (a copy coming up, a
/// setup, a command) waits between looks.
const LOOK_AGAIN: Timer = Timer { spec: TimerSpec::After { duration_ms: 3_000 } };

impl<'a> ProjectCalls<'a> {
    /// One infra node's copies: the shared one, or an instance's copy of
    /// a node marked `@per_instance` (`.instance(id)`). The node is named
    /// the way the program writes it (`bridge`, or `one.bridge` inside the
    /// file the site `one` includes).
    pub fn infra(self, node: impl Into<String>) -> InfraCalls<'a> {
        InfraCalls { at: self, node: node.into(), instance: None }
    }

    /// What reaches every infra copy of the project at once: halting the
    /// work under way, asking the supervisor to look again, the doors,
    /// every container's logs, and following a lifecycle command.
    pub fn all_infra(self) -> AllInfraCalls<'a> {
        AllInfraCalls { at: self }
    }
}

/// One infra node's copies. See [`ProjectCalls::infra`].
pub struct InfraCalls<'a> {
    at: ProjectCalls<'a>,
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
            let status = ProgramCall::Infra(InfraCall::Status { node: node.clone(), instance: instance.clone() });
            let copy: Option<InfraCopy> = self.at.call(status, StopSelf::Keep).await?;
            match copy.as_ref().map(|c| c.status) {
                Some(InfraNodeStatus::Running) => return Ok(()),
                Some(InfraNodeStatus::Failed) => {
                    let why = copy.and_then(|c| c.failure).unwrap_or_else(|| "no reason recorded".into());
                    return Err(WeftError::NodeFailed(format!(
                        "infra '{node}' failed to start: {why}; `weft infra status` shows the copy"
                    )));
                }
                None => {
                    return Err(WeftError::NodeFailed(format!(
                        "infra '{node}' did not come up: its start ended without a copy (the setup run failed \
                         or was cancelled, or somebody terminated it); `weft runs` shows the setup run"
                    )));
                }
                Some(stopped @ (InfraNodeStatus::Stopped | InfraNodeStatus::Stopping | InfraNodeStatus::Terminating)) => {
                    return Err(WeftError::NodeFailed(format!(
                        "infra '{node}' is {stopped}: somebody took it down while this start waited for it"
                    )));
                }
                Some(InfraNodeStatus::Provisioning | InfraNodeStatus::Flaky) => {}
            }
            self.at.ctx.await_signal(LOOK_AGAIN).await?;
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
            let call = ProgramCall::Infra(InfraCall::Start { node: self.node.clone(), instance: instance.clone() });
            let answer: InfraStartAnswer = self.at.call(call, StopSelf::Keep).await?;
            if answer.accepted() {
                return Ok(());
            }
            self.at.ctx.await_signal(LOOK_AGAIN).await?;
        }
    }

    /// Scale the copy down, keeping its disk, the way `weft infra stop
    /// --infra <node>` does, with the same refusals. `take_down` says what
    /// happens to the triggers whose cut contains it (they come back when
    /// it runs again) and to the runs reaching it, each answer left out
    /// being an infra command's default: the door holds, what is held is
    /// kept, and the copy waits for its runs to end. `stop_self` decides
    /// whether this run goes too when it reaches the copy. Answers whether
    /// there was a copy to take down ([`InfraDownAnswer`]), so a program
    /// can tell an instance with no copy (a mistyped id, one terminated
    /// before) apart; stopping a copy already down answers so and does
    /// nothing.
    pub async fn stop(self, take_down: TakeDownChoice, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        self.stop_as(take_down, stop_self, false).await
    }

    /// [`Self::stop`], bringing down the units whose `on_stop` would keep
    /// them up, as `weft infra stop --force` does.
    pub async fn force_stop(self, take_down: TakeDownChoice, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        self.stop_as(take_down, stop_self, true).await
    }

    async fn stop_as(self, take_down: TakeDownChoice, stop_self: StopSelf, force: bool) -> WeftResult<InfraDownAnswer> {
        let call = ProgramCall::Infra(InfraCall::Stop { node: self.node, instance: instance_id(&self.instance)?, take_down, force });
        self.at.call(call, stop_self).await
    }

    /// Delete the copy and its disks, keeping the ones its node lists in
    /// `keepOnTerminate` (a later start of the same copy finds them
    /// again). Same `take_down` and `stop_self` as [`Self::stop`].
    pub async fn terminate(self, take_down: TakeDownChoice, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        self.take_away(take_down, stop_self, TerminateDisks::KeepListed).await
    }

    /// Delete the copy and every one of its disks, the ones listed in
    /// `keepOnTerminate` too: the copy's owner is going for good (a
    /// instance being wiped). Same `take_down` and `stop_self` as [`Self::stop`].
    pub async fn wipe(self, take_down: TakeDownChoice, stop_self: StopSelf) -> WeftResult<InfraDownAnswer> {
        self.take_away(take_down, stop_self, TerminateDisks::DeleteAll).await
    }

    /// Cycle the copy onto the registered build's infra when it runs on
    /// another one (a stop, then a start; nothing is built), the way
    /// `weft infra upgrade --infra <node>` does, with the same refusals:
    /// the triggers whose cut contains it go down as `take_down` says, the
    /// way [`Self::stop`] does, and come back with it. A copy already on
    /// the registered build, stopped, or missing is left as it is, and the
    /// answer names no command. Answers once the upgrade is under way,
    /// with its command, which [`AllInfraCalls::follow_command`] follows
    /// to its end.
    pub async fn upgrade(self, take_down: TakeDownChoice, stop_self: StopSelf) -> WeftResult<LifecycleCommandIssued> {
        let call = ProgramCall::Infra(InfraCall::Upgrade { node: self.node, instance: instance_id(&self.instance)?, take_down });
        self.at.call(call, stop_self).await
    }

    async fn take_away(self, take_down: TakeDownChoice, stop_self: StopSelf, disks: TerminateDisks) -> WeftResult<InfraDownAnswer> {
        let call = ProgramCall::Infra(InfraCall::Terminate { node: self.node, instance: instance_id(&self.instance)?, take_down, disks });
        self.at.call(call, stop_self).await
    }

    /// The copy's state now, or `None` when it was never started (or was
    /// terminated).
    pub async fn status(self) -> WeftResult<Option<InfraCopy>> {
        let call = ProgramCall::Infra(InfraCall::Status { node: self.node, instance: instance_id(&self.instance)? });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Every copy of the node that exists: the shared one and each
    /// instance's.
    pub async fn copies(self) -> WeftResult<Vec<InfraCopy>> {
        self.no_instance("copies")?;
        self.at.call(ProgramCall::Infra(InfraCall::Copies { node: self.node }), StopSelf::Keep).await
    }

    /// The last `tail` lines each container of every copy of the node
    /// wrote, and the cursor [`Self::logs_after`] reads on from.
    pub async fn logs(self, tail: usize) -> WeftResult<InfraLogs> {
        self.no_instance("logs")?;
        let request = InfraLogsRequest { node: Some(self.node), tail, after: None };
        self.at.call(ProgramCall::Infra(InfraCall::Logs { request }), StopSelf::Keep).await
    }

    /// The lines the node's containers wrote after `cursor` (an earlier
    /// answer's).
    pub async fn logs_after(self, cursor: LogCursor) -> WeftResult<InfraLogs> {
        self.no_instance("logs")?;
        let request = InfraLogsRequest { node: Some(self.node), after: Some(cursor), ..Default::default() };
        self.at.call(ProgramCall::Infra(InfraCall::Logs { request }), StopSelf::Keep).await
    }

    /// What the copy's card shows (`weft infra show`): a node that serves
    /// one (`features.live_endpoint`), on a copy that runs.
    pub async fn show(self) -> WeftResult<LiveFeed> {
        let call = ProgramCall::Infra(InfraCall::Show { node: self.node, instance: instance_id(&self.instance)? });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Press the button `kind` (an item's `actionKind`) the copy's card
    /// offers, with `payload`, and answer what the container answered
    /// (`weft infra press`). The container's refusal fails the call with
    /// its words.
    pub async fn press(self, kind: impl Into<String>, payload: Value) -> WeftResult<Value> {
        let press = LivePress { kind: kind.into(), payload };
        let call = ProgramCall::Infra(InfraCall::Press { node: self.node, instance: instance_id(&self.instance)?, press });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Make the node's baked outputs again (`weft infra rebake`): run its
    /// infra setup again, which saves the new values, for a copy whose
    /// values changed in a way weft did not see. Returns once the setup
    /// ended; a setup of the same copies already under way is waited out
    /// first. Like [`Self::start`], every wait parks the run on a timer.
    pub async fn rebake(self) -> WeftResult<()> {
        let instance = instance_id(&self.instance)?;
        let run = loop {
            let call = ProgramCall::Infra(InfraCall::Rebake { node: self.node.clone(), instance: instance.clone() });
            match self.at.call::<RebakeAnswer>(call, StopSelf::Keep).await? {
                RebakeAnswer::Started { run } => break run,
                RebakeAnswer::NothingToSetUp => return Ok(()),
                RebakeAnswer::Waiting { .. } => {
                    self.at.ctx.await_signal(LOOK_AGAIN).await?;
                }
            }
        };
        loop {
            let end: SetupEnd = self.at.call(ProgramCall::Infra(InfraCall::Setup { run }), StopSelf::Keep).await?;
            match end {
                SetupEnd::Completed => return Ok(()),
                SetupEnd::Going => {
                    self.at.ctx.await_signal(LOOK_AGAIN).await?;
                }
                SetupEnd::Failed { reason } => return Err(rebake_ended(&self.node, "failed", run, reason)),
                SetupEnd::Cancelled { reason } => return Err(rebake_ended(&self.node, "cancelled", run, reason)),
            }
        }
    }

    /// A call that reads every copy of the node, refused when an instance
    /// was named rather than leaving the name unread.
    fn no_instance(&self, what: &str) -> WeftResult<()> {
        match &self.instance {
            None => Ok(()),
            Some(instance) => Err(WeftError::Input(format!(
                "infra '{}': {what} reads every copy of the node, so it takes no instance (`.instance(\"{instance}\")`)",
                self.node
            ))),
        }
    }
}

/// The error a rebake whose setup run `run` ended `how` (failed,
/// cancelled) fails with: the reason the run recorded, or, when it
/// recorded none, a pointer to `weft runs`.
fn rebake_ended(node: &str, how: &str, run: crate::RunId, reason: Option<String>) -> WeftError {
    WeftError::NodeFailed(match reason {
        Some(reason) => format!("the rebake of infra '{node}' {how}: {reason}"),
        None => format!("the rebake of infra '{node}' {how}: its setup run {run} did not complete; `weft runs` shows it"),
    })
}

/// What reaches every infra copy of a project at once. See
/// [`ProjectCalls::all_infra`].
pub struct AllInfraCalls<'a> {
    at: ProjectCalls<'a>,
}

impl AllInfraCalls<'_> {
    /// Halt the infra work under way for one owner's copies: the shared
    /// ones (`None`), or that instance's (`weft infra cancel`). Starts and
    /// stops halt between steps and nothing is rolled back; what each
    /// copy reached stays, for a later start or terminate to carry on
    /// from. Answers whether there was anything under way to halt.
    pub async fn cancel(self, instance: Option<&str>) -> WeftResult<bool> {
        let instance = instance_id(&instance.map(str::to_string))?;
        let answer: InfraCancelAnswer = self.at.call(ProgramCall::Infra(InfraCall::Cancel { instance }), StopSelf::Keep).await?;
        Ok(answer.cancelled)
    }

    /// Ask the supervisor to look at the project's infra again, so what
    /// it gave up on is tried once more (`weft infra look`).
    pub async fn look(self) -> WeftResult<()> {
        self.at.call::<Value>(ProgramCall::Infra(InfraCall::Look), StopSelf::Keep).await.map(drop)
    }

    /// Every door the project's infra has serving, at the address its
    /// host gave it, and the copies whose doors have no address yet.
    pub async fn doors(self) -> WeftResult<DoorsResponse> {
        self.at.call(ProgramCall::Infra(InfraCall::Doors), StopSelf::Keep).await
    }

    /// The last `tail` lines each container of every copy wrote, and the
    /// cursor [`Self::logs_after`] reads on from.
    pub async fn logs(self, tail: usize) -> WeftResult<InfraLogs> {
        let request = InfraLogsRequest { tail, ..Default::default() };
        self.at.call(ProgramCall::Infra(InfraCall::Logs { request }), StopSelf::Keep).await
    }

    /// The lines every container wrote after `cursor` (an earlier
    /// answer's).
    pub async fn logs_after(self, cursor: LogCursor) -> WeftResult<InfraLogs> {
        let request = InfraLogsRequest { after: Some(cursor), ..Default::default() };
        self.at.call(ProgramCall::Infra(InfraCall::Logs { request }), StopSelf::Keep).await
    }

    /// Whether lifecycle command `id` (one a stop, terminate or upgrade
    /// answered) has finished, and how, as it stands now.
    pub async fn command(self, id: i64) -> WeftResult<CommandStatus> {
        self.at.call(ProgramCall::Infra(InfraCall::Command { id }), StopSelf::Keep).await
    }

    /// Lifecycle command `id`'s end, once it has one. The wait has no
    /// deadline (a stop waits for the runs reaching its copies when it
    /// was asked to) and holds no worker: between looks the run parks on
    /// a timer.
    pub async fn follow_command(self, id: i64) -> WeftResult<CommandStatus> {
        loop {
            let status: CommandStatus = self.at.call(ProgramCall::Infra(InfraCall::Command { id }), StopSelf::Keep).await?;
            if status.done {
                return Ok(status);
            }
            self.at.ctx.await_signal(LOOK_AGAIN).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rebake_that_ended_says_its_reason_or_points_at_weft_runs() {
        let run = crate::RunId::from_u128(3);
        let failed = rebake_ended("db", "failed", run, Some("the extension is missing".into()));
        assert_eq!(failed.to_string(), WeftError::NodeFailed("the rebake of infra 'db' failed: the extension is missing".into()).to_string());
        let cancelled = rebake_ended("db", "cancelled", run, None).to_string();
        assert!(cancelled.contains("the rebake of infra 'db' cancelled: its setup run") && cancelled.contains("`weft runs`"), "{cancelled}");
    }
}

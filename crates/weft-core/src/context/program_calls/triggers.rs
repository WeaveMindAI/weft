//! `ctx.trigger(..)` and `ctx.triggers()`: triggers' activations, their
//! settings, and bringing the ones on onto the registered build.

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use crate::activation::{ActivateRequest, ActivateResponse, ActivationScope, BakeRequest, CancelActivateResponse};
use crate::builds::BuildHashes;
use crate::deactivation::{DeactivateRequest, ResyncAnswer, ResyncRequest, ResyncResponse};
use crate::instance::{InstanceId, InstanceTarget};
use crate::program::triggers::{BakeAnswer, TriggerCall};
use crate::context::kept::Step;
use crate::context::Kept;
use crate::signal::to_spec;
use crate::take_down::{ReleaseHeld, TakeDownChoice};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::program::ProgramCall;
use crate::tag::StopSelf;

use super::{required_instance, ProjectCalls};

impl<'a> ProjectCalls<'a> {
    /// One trigger's activations: the shared one, or, for a trigger that
    /// exists once per instance, the instances' named with `.instance(id)`
    /// (repeatable), every instance's with `.all_instances()`, or both
    /// with `.all()`.
    pub fn trigger(self, node: impl Into<String>) -> TriggerCalls<'a> {
        TriggerCalls { at: self, triggers: vec![node.into()], instances: Vec::new(), owners: Owners::Named }
    }

    /// Every trigger of its owners: every shared trigger, or every
    /// per-instance trigger of the instances named with `.instance(id)`,
    /// of every instance with `.all_instances()`, or both with `.all()`.
    pub fn triggers(self) -> TriggerCalls<'a> {
        TriggerCalls { at: self, triggers: Vec::new(), instances: Vec::new(), owners: Owners::Named }
    }

    /// Take up again the resync the project holds (one that stopped with
    /// triggers down), where it stopped, with the answers and the build it
    /// began with, and return once it ends, the way
    /// [`TriggerCalls::resync`] does (`weft resync --resume`). Refused
    /// when the project holds no resync, and on this run's own project.
    /// Following it pauses the run on a timer between looks, where
    /// `state` comes in.
    pub async fn resume_resync<S: Serialize + DeserializeOwned + Send>(self, state: &mut Kept<S>) -> WeftResult<ResyncResponse> {
        self.resync_followed("triggers.resume_resync", TriggerCall::ResumeResync, state).await
    }

    /// Ask for the resync `call` once (the task it hands back is kept with
    /// a pause), and its answer once it ended: at once when it was done,
    /// else after following its task.
    async fn resync_followed<S: Serialize + DeserializeOwned + Send>(
        self,
        helper: &'static str,
        call: TriggerCall,
        state: &mut Kept<S>,
    ) -> WeftResult<ResyncResponse> {
        let call = &call;
        state
            .follow(helper, &(self.project, call), to_spec(super::project::FOLLOW_LOOK_AGAIN), None::<Resyncing>, move |place| async move {
                let place = match place {
                    Some(place) => place,
                    None => match self.call::<ResyncAnswer>(ProgramCall::Triggers(call.clone()), StopSelf::Keep).await? {
                        ResyncAnswer::Done(response) => return Ok(Step::Done(response)),
                        ResyncAnswer::UnderWay { task, brought_back } => Resyncing { task, brought_back },
                    },
                };
                Ok(match self.task_ended::<ResyncResponse>(place.task).await? {
                    Some(followed) => Step::Done(ResyncResponse { brought_back: place.brought_back, ..followed }),
                    None => Step::LookAgain(Some(place)),
                })
            })
            .await
    }
}

/// A resync under way, between the looks that follow it.
#[derive(Clone, Serialize, Deserialize)]
struct Resyncing {
    task: uuid::Uuid,
    brought_back: Option<crate::maintenance::ProjectLifted>,
}

/// Whose activations, beside the instances named: what `.all_instances()`
/// and `.all()` add.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Owners {
    Named,
    EveryInstance,
    Everything,
}

/// [`RunContext::triggers`].
pub struct TriggerCalls<'a> {
    at: ProjectCalls<'a>,
    triggers: Vec<String>,
    instances: Vec<String>,
    owners: Owners,
}

impl<'a> TriggerCalls<'a> {
    /// Only these triggers (named the way the program writes them), on
    /// top of the ones already named; none named is every trigger of the
    /// owner.
    pub fn only<S: Into<String>>(mut self, triggers: impl IntoIterator<Item = S>) -> Self {
        self.triggers.extend(triggers.into_iter().map(Into::into));
        self
    }

    /// That instance's activations too, of triggers that exist once per
    /// instance (call it again for another instance).
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instances.push(id.into());
        self
    }

    /// Every instance's activations, of triggers that exist once per
    /// instance.
    pub fn all_instances(mut self) -> Self {
        self.owners = Owners::EveryInstance;
        self
    }

    /// The shared activations and every instance's, in one call (what a
    /// deploy resyncs).
    pub fn all(mut self) -> Self {
        self.owners = Owners::Everything;
        self
    }

    fn scope(&self) -> WeftResult<ActivationScope> {
        trigger_scope(&self.triggers, &self.instances, self.owners)
    }

    /// Turn on the ones that are off, leaving the ones on as they are
    /// (with none off, this does nothing). What they held while down runs
    /// (paced so new arrivals still get through) or is dropped, as `held`
    /// says. An instance's trigger reading that instance's infra copy
    /// needs the copy running first. Chain `.port(..)` or `.on_build(..)`
    /// before awaiting it; answers what it turned on and where their
    /// routes answer.
    pub fn activate(self, held: ReleaseHeld) -> Activate<'a> {
        Activate { calls: self, held, port: None, build: BuildHashes::default() }
    }

    /// Take them down as `take_down` says, each answer left out being the
    /// default: the door holds what arrives, what is held is kept, and the
    /// runs going finish on their own version (`Running::Wait`; the call
    /// answers once the triggers are down, never waiting on runs from
    /// inside this one) or are cancelled (`Running::Cancel`). `stop_self`
    /// decides this run's fate when one of them fired it.
    pub async fn deactivate(self, take_down: TakeDownChoice, stop_self: StopSelf) -> WeftResult<()> {
        let call = ProgramCall::Triggers(TriggerCall::Deactivate { request: DeactivateRequest { choice: take_down, scope: self.scope()? } });
        self.at.call::<Value>(call, stop_self).await.map(drop)
    }

    /// End the activations among them still being turned on (`weft
    /// cancel-activate`): each one's setup run cancelled, triggers that
    /// held work left down with their door taking, the others refusing.
    /// Named nothing (`ctx.triggers()` alone), it also gives up the resync
    /// the project holds, and the answer names the triggers it leaves
    /// down. Refused when none of them is activating and no resync is
    /// held.
    pub async fn cancel_activate(self) -> WeftResult<CancelActivateResponse> {
        let call = ProgramCall::Triggers(TriggerCall::CancelActivate { scope: self.scope()? });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Prepare their settings on the registered build without turning
    /// anything on or touching a run (`weft bake`): one setup per owner.
    /// The answer also names the parts of the build that do not compile.
    pub async fn bake(self) -> WeftResult<BakeAnswer> {
        let call = ProgramCall::Triggers(TriggerCall::Bake { request: BakeRequest { scope: self.scope()? } });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Bring the ones that are on and behind the registered build onto it
    /// (`weft resync`): each owner's taken down as `take_down` says (the
    /// same answers and defaults as [`Self::deactivate`]), and, once the
    /// old version has nothing left running, turned back on on the new
    /// build. One that is off is never turned on; the answer names it.
    /// Chain `.on_build(..)` before awaiting it. It returns once the
    /// resync ended, however long the old version's runs take: between
    /// looks the run pauses on a timer (where `state` comes in) and holds
    /// no worker. Refused on this run's own project, whose runs on the old
    /// version include this one.
    pub fn resync<'k, S>(self, take_down: TakeDownChoice, state: &'k mut Kept<S>) -> Resync<'a, 'k, S> {
        Resync { calls: self, take_down, build: BuildHashes::default(), state }
    }
}

/// [`TriggerCalls::activate`], awaited to carry it out.
pub struct Activate<'a> {
    calls: TriggerCalls<'a>,
    held: ReleaseHeld,
    port: Option<u16>,
    build: BuildHashes,
}

impl Activate<'_> {
    /// On an install that gives projects a port (a machine), the port the
    /// project's own address opens on, kept for later activates (`weft
    /// activate --port`). Another program holding it refuses the activate.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// The build this activate means (what a build call answered): it is
    /// refused when another build registered since.
    pub fn on_build(mut self, build: BuildHashes) -> Self {
        self.build = build;
        self
    }
}

impl<'a> IntoFuture for Activate<'a> {
    type Output = WeftResult<ActivateResponse>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let request = ActivateRequest { build: self.build, scope: self.calls.scope()?, held: self.held, port: self.port };
            self.calls.at.call(ProgramCall::Triggers(TriggerCall::Activate { request }), StopSelf::Keep).await
        })
    }
}

/// [`TriggerCalls::resync`], awaited to carry it out.
pub struct Resync<'a, 'k, S> {
    calls: TriggerCalls<'a>,
    take_down: TakeDownChoice,
    build: BuildHashes,
    state: &'k mut Kept<S>,
}

impl<S> Resync<'_, '_, S> {
    /// The build to bring the triggers onto (what a build call answered):
    /// the resync is refused when another build registered since.
    pub fn on_build(mut self, build: BuildHashes) -> Self {
        self.build = build;
        self
    }
}

impl<'a, 'k, S: Serialize + DeserializeOwned + Send> IntoFuture for Resync<'a, 'k, S>
where
    'a: 'k,
{
    type Output = WeftResult<ResyncResponse>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'k>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let request = ResyncRequest { build: self.build, choice: self.take_down, scope: self.calls.scope()? };
            self.calls.at.resync_followed("triggers.resync", TriggerCall::Resync { request }, self.state).await
        })
    }
}

/// The activations a [`TriggerCalls`] names: `triggers` (every one when
/// empty) of the program's own and of the instances `instances`, or of
/// every instance (`owners`), which names none beside it.
fn trigger_scope(triggers: &[String], instances: &[String], owners: Owners) -> WeftResult<ActivationScope> {
    let named = instances.iter().map(|id| required_instance(id)).collect::<WeftResult<Vec<InstanceId>>>()?;
    let instances = match (owners, named.is_empty()) {
        (Owners::EveryInstance | Owners::Everything, false) => {
            return Err(WeftError::Input(
                "`.all_instances()` and `.all()` act on every instance and `.instance(..)` names some; call one of them".into(),
            ))
        }
        (Owners::EveryInstance, true) => InstanceTarget::EveryInstance,
        (Owners::Everything, true) => InstanceTarget::Everything,
        (Owners::Named, _) => InstanceTarget::named(named),
    };
    Ok(ActivationScope { triggers: triggers.to_vec(), instances })
}

#[cfg(test)]
mod trigger_scope_tests {
    use super::*;

    /// No instance named is the program's own; named ones are those;
    /// every instance is all of them, and naming some beside it is refused.
    #[test]
    fn a_trigger_call_names_its_activations() {
        let feed = vec!["feed".to_string()];
        let own = trigger_scope(&feed, &[], Owners::Named).unwrap();
        assert_eq!((own.triggers, own.instances), (feed.clone(), InstanceTarget::Shared));
        let ada = trigger_scope(&[], &["ada".to_string()], Owners::Named).unwrap();
        assert_eq!(ada.instances, InstanceTarget::named(vec![InstanceId::new("ada").unwrap()]));
        assert_eq!(trigger_scope(&feed, &[], Owners::EveryInstance).unwrap().instances, InstanceTarget::EveryInstance);
        assert_eq!(trigger_scope(&feed, &[], Owners::Everything).unwrap().instances, InstanceTarget::Everything);
        let both = trigger_scope(&feed, &["ada".to_string()], Owners::EveryInstance).unwrap_err();
        assert!(both.to_string().contains("call one of them"), "{both}");
        assert!(trigger_scope(&feed, &["ada".to_string()], Owners::Everything).is_err());
        assert!(trigger_scope(&feed, &["".to_string()], Owners::Named).is_err(), "an instance must be named");
    }
}

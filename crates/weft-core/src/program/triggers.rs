//! A program's calls on a project's trigger activations
//! (`ctx.trigger(..)`, `ctx.triggers()`): turning them on and off,
//! cancelling one being turned on, preparing their settings, and bringing
//! the ones on onto the registered build.

use serde::{Deserialize, Serialize};

use crate::activation::{ActivateRequest, ActivationScope, BakeRequest};
use crate::deactivation::{DeactivateRequest, ResyncRequest};

/// One call on triggers' activations, each carrying the request its
/// command sends (`weft activate`, `weft deactivate`, `weft
/// cancel-activate`, `weft bake`, `weft resync`).
// SYNC: TriggerCall <-> crates/weft-dispatcher/src/actions/triggers.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum TriggerCall {
    /// Turn on the triggers the request names that are off.
    Activate { request: ActivateRequest },
    /// Take triggers down.
    Deactivate { request: DeactivateRequest },
    /// End the activations of `scope` still being turned on; with nothing
    /// named, give up the resync the project holds too.
    CancelActivate { scope: ActivationScope },
    /// Prepare triggers' settings without turning anything on.
    Bake { request: BakeRequest },
    /// Bring the triggers on and behind the registered build onto it.
    Resync { request: ResyncRequest },
    /// Take up again the resync the project holds, where it stopped.
    ResumeResync,
}

impl TriggerCall {
    pub const NAMES: &'static [&'static str] = &[
        "weft.triggers.activate",
        "weft.triggers.deactivate",
        "weft.triggers.cancel_activate",
        "weft.triggers.bake",
        "weft.triggers.resync",
        "weft.triggers.resume_resync",
    ];

    pub fn name(&self) -> &'static str {
        match self {
            TriggerCall::Activate { .. } => "weft.triggers.activate",
            TriggerCall::Deactivate { .. } => "weft.triggers.deactivate",
            TriggerCall::CancelActivate { .. } => "weft.triggers.cancel_activate",
            TriggerCall::Bake { .. } => "weft.triggers.bake",
            TriggerCall::Resync { .. } => "weft.triggers.resync",
            TriggerCall::ResumeResync => "weft.triggers.resume_resync",
        }
    }

    /// Only a deactivate reaches the asking run: a resync of the asking
    /// run's own project is refused.
    pub fn takes_down(&self) -> bool {
        matches!(self, TriggerCall::Deactivate { .. })
    }
}

/// What a bake answers (`POST /projects/{id}/bake`,
/// `ctx.triggers().bake()`): one setup per owner, and the parts of the
/// baked build that do not compile (`ProjectDefinition::broken`), which no
/// setup that needs one ran, so whoever baked is told about every one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BakeAnswer {
    pub baked: Vec<crate::run_spec::BakeSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub broken: Vec<crate::project::Broken>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::ProgramCall;

    fn round_trip(call: TriggerCall) -> serde_json::Value {
        let wrapped = ProgramCall::Triggers(call);
        let wire = serde_json::to_value(&wrapped).unwrap();
        assert_eq!(serde_json::from_value::<ProgramCall>(wire.clone()).unwrap(), wrapped);
        wire
    }

    /// Every trigger call carries its area and call tags, and its request
    /// the way the route reads it.
    #[test]
    fn every_trigger_call_round_trips_with_its_tags() {
        let activate = round_trip(TriggerCall::Activate {
            request: ActivateRequest {
                build: crate::builds::BuildHashes { binary_hash: Some("b".into()), ..Default::default() },
                port: Some(8080),
                ..Default::default()
            },
        });
        assert_eq!((activate["area"].as_str(), activate["call"].as_str()), (Some("triggers"), Some("activate")));
        assert_eq!(activate["request"]["binaryHash"], "b");
        assert_eq!(activate["request"]["port"], 8080);
        let deactivate = round_trip(TriggerCall::Deactivate {
            request: DeactivateRequest {
                choice: crate::take_down::TakeDownChoice { door_for_minutes: Some(5), ..Default::default() },
                ..Default::default()
            },
        });
        assert_eq!(deactivate["call"], "deactivate");
        assert_eq!(deactivate["request"]["doorForMinutes"], 5);
        assert_eq!(round_trip(TriggerCall::CancelActivate { scope: ActivationScope::shared() })["call"], "cancel_activate");
        assert_eq!(round_trip(TriggerCall::Bake { request: BakeRequest::default() })["call"], "bake");
        let resync = round_trip(TriggerCall::Resync {
            request: ResyncRequest { build: crate::builds::BuildHashes { definition_hash: Some("d".into()), ..Default::default() }, ..Default::default() },
        });
        assert_eq!((resync["call"].as_str(), resync["request"]["definitionHash"].as_str()), (Some("resync"), Some("d")));
        assert_eq!(round_trip(TriggerCall::ResumeResync)["call"], "resume_resync");
    }
}

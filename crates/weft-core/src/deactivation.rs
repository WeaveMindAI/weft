//! What `POST /projects/{id}/deactivate` and `/resync` read and answer,
//! and how a person reads whose triggers a lifecycle verb touched. ONE
//! definition for the dispatcher and the CLI.

use serde::{Deserialize, Serialize};

use crate::instance::{InstanceId, Owner};
use crate::activation::{ActivationScope, ActivationTarget};
use crate::running_policy::DeactivateSpec;

/// Whose triggers a deactivate took down, in order, and which instances
/// still have a trigger on afterwards, so a plain deactivate (the
/// program's own triggers) can say that instances' are still listening
/// and how to take them down too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeactivateResponse {
    pub deactivated: Vec<Owner>,
    pub instances_still_on: Vec<InstanceId>,
}

/// `POST /projects/{id}/deactivate`: the take-down choice (mode,
/// running policy and drain cap) and which activations: every shared
/// trigger by default, or the ones named, for the instance named; or,
/// with `all_instances`, for every instance whose triggers are on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeactivateRequest {
    #[serde(flatten)]
    pub spec: DeactivateSpec,
    #[serde(default)]
    pub scope: ActivationScope,
    /// Every instance with a trigger on, each taken down with the same
    /// spec and the scope's triggers (all of that instance's when none are
    /// named). The program's own triggers are left as they are: a plain
    /// deactivate is for those. Refused beside `scope.instance`, which names
    /// one instance instead.
    #[serde(default, rename = "allInstances", skip_serializing_if = "std::ops::Not::not")]
    pub all_instances: bool,
}

/// `POST /projects/{id}/resync`: what the activate points at (the
/// hashes, the reactivate choice), the trigger-deactivation answer,
/// required because a resync only acts on triggers that are on (428
/// without it), and which activations. The running-work answer is the
/// picker's, so the body carries no `runningPolicy` of its own: one
/// answer governs the trigger drain and the worker replacement alike.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResyncRequest {
    #[serde(flatten)]
    pub target: ActivationTarget,
    #[serde(default, rename = "triggerDeactivation", skip_serializing_if = "Option::is_none")]
    pub trigger_deactivation: Option<DeactivateSpec>,
    /// Which activations. Left out (no trigger, no instance): every
    /// owner with a trigger on, the program's and each instance's.
    #[serde(default)]
    pub scope: ActivationScope,
}

/// How a person reads one owner's triggers: "the program's triggers" or
/// "instance 'ada''s triggers".
pub fn whose_triggers(owner: &Owner) -> String {
    match owner {
        Owner::Shared => "the program's triggers".to_string(),
        Owner::Instance(instance) => format!("instance '{instance}''s triggers"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_deactivate_names_all_instances_only_when_asked() {
        let spec: DeactivateSpec =
            serde_json::from_value(serde_json::json!({ "mode": "wipe", "runningPolicy": "cancel" })).unwrap();
        let plain = DeactivateRequest { spec: spec.clone(), scope: ActivationScope::default(), all_instances: false };
        let wire = serde_json::to_value(&plain).unwrap();
        assert!(wire.get("allInstances").is_none(), "{wire}");
        assert_eq!(wire["mode"], "wipe");
        let every = serde_json::to_value(DeactivateRequest { all_instances: true, ..plain }).unwrap();
        assert_eq!(every["allInstances"], true);
        assert!(serde_json::from_value::<DeactivateRequest>(every).unwrap().all_instances);
    }

    #[test]
    fn each_owner_reads_as_whose_triggers() {
        assert_eq!(whose_triggers(&Owner::Shared), "the program's triggers");
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        assert_eq!(whose_triggers(&ada), "instance 'ada''s triggers");
    }
}

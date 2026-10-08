//! Which trigger activations a lifecycle verb names.
//!
//! The unit of activation is ONE trigger for ONE owner: the program's
//! shared copy of a trigger, or one instance's copy of a per-instance one.
//! These are the names every surface agrees on (the CLI's `--trigger` and
//! `--instance`, the editor's action bar, a program's `ctx.trigger(..)`),
//! resolved against the program so a mismatch is refused where it is
//! written.

use serde::{Deserialize, Serialize};

use crate::instance::{InstanceId, Owner};
use crate::project::ProjectDefinition;
use crate::running_policy::{DeactivationMode, RunningChoice};

/// Where one activation stands, as a person reads it: `registered`,
/// `activating`, `active`, `deactivating`, or for one taken down, the
/// way it went (`wipe`, `hibernate`, `park`).
// SYNC: ActivationMode <-> packages/weft-graph/src/protocol.ts ActivationMode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActivationMode {
    Registered,
    Activating,
    Active,
    Deactivating,
    #[serde(untagged)]
    Down(DeactivationMode),
}

impl ActivationMode {
    /// The wire word (the test below pins it to serde's).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Activating => "activating",
            Self::Active => "active",
            Self::Deactivating => "deactivating",
            Self::Down(how) => how.as_str(),
        }
    }
}

/// One activation: a trigger, spelled the way the program reads it
/// (`door`, or `one.door` for the `door` inside the file the site `one`
/// includes), for one owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActivationKey {
    pub trigger: String,
    pub owner: Owner,
}

impl ActivationKey {
    pub fn new(trigger: impl Into<String>, owner: Owner) -> Self {
        Self { trigger: trigger.into(), owner }
    }

    pub fn instance(&self) -> Option<&InstanceId> {
        self.owner.instance()
    }
}

impl std::fmt::Display for ActivationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.owner {
            Owner::Shared => write!(f, "'{}'", self.trigger),
            Owner::Instance(instance) => write!(f, "'{}' of instance '{instance}'", self.trigger),
        }
    }
}

/// Which activations a verb acts on: every trigger of one owner, or the
/// named ones. What `weft activate` / `deactivate` / `resync` / `bake` /
/// `cancel-activate` send (`--trigger`, `--instance`), what the editor's
/// action bar sends (everything shared), and what a program's
/// `ctx.trigger(..)` / `ctx.triggers()` calls send.
// SYNC: ActivationScope <-> packages/weft-graph/src/protocol.ts ActivationScope
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActivationScope {
    /// The triggers, by address. Empty means every trigger the owner
    /// has: every shared trigger for [`Owner::Shared`], every
    /// per-instance trigger for an instance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<String>,
    /// Whose activations: `None` for the program's shared ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
}

impl ActivationScope {
    /// Every shared trigger: what a plain `weft activate` means.
    pub fn shared() -> Self {
        Self::default()
    }

    pub fn owner(&self) -> Owner {
        Owner::from_instance(self.instance.clone())
    }

    /// The activations this scope names, against `project`. A trigger
    /// that exists once per instance needs an instance, and a shared one
    /// cannot be activated for an instance, so either mismatch is refused
    /// naming the fix. An unknown or non-trigger address is refused too.
    /// Sorted, never empty unless the project has no trigger of the
    /// owner's kind (the caller words that case).
    pub fn resolve(&self, project: &ProjectDefinition) -> Result<Vec<ActivationKey>, String> {
        let owner = self.owner();
        let places = crate::project::trigger_places(project);
        let spelled_per_instance: Vec<(String, bool)> = places
            .iter()
            .map(|place| {
                (
                    crate::project::address_of(project, &place.id, &place.path),
                    crate::project::is_per_instance(project, &place.id),
                )
            })
            .collect();
        let mut keys: Vec<ActivationKey> = if self.triggers.is_empty() {
            spelled_per_instance
                .into_iter()
                .filter(|(_, per_instance)| *per_instance == matches!(owner, Owner::Instance(_)))
                .map(|(trigger, _)| ActivationKey::new(trigger, owner.clone()))
                .collect()
        } else {
            let mut keys = Vec::new();
            for named in &self.triggers {
                let Some((trigger, per_instance)) = spelled_per_instance.iter().find(|(t, _)| t == named) else {
                    return Err(format!("'{named}' is not a trigger of this program"));
                };
                match (&owner, per_instance) {
                    (Owner::Shared, true) => {
                        return Err(format!(
                            "trigger '{trigger}' exists once per instance; name which with --instance <id>"
                        ));
                    }
                    (Owner::Instance(instance), false) => {
                        return Err(format!(
                            "trigger '{trigger}' is shared by every instance, so it cannot be activated \
                             for instance '{instance}'; leave --instance out"
                        ));
                    }
                    _ => keys.push(ActivationKey::new(trigger.clone(), owner.clone())),
                }
            }
            keys
        };
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// The per-instance triggers a plain activation of the program's
    /// shared triggers leaves off (each exists once per instance, so it
    /// needs one named), sorted. Empty for any other scope. What `weft
    /// activate` reports, so nobody reads "activated" as "every trigger is
    /// on".
    pub fn per_instance_left_out(&self, project: &ProjectDefinition) -> Vec<String> {
        if self.instance.is_some() || !self.triggers.is_empty() {
            return Vec::new();
        }
        let mut left: Vec<String> = crate::project::trigger_places(project)
            .iter()
            .filter(|place| crate::project::is_per_instance(project, &place.id))
            .map(|place| crate::project::address_of(project, &place.id, &place.path))
            .collect();
        left.sort();
        left.dedup();
        left
    }
}


/// The owners among `live` (the keys of the activations that are on),
/// each once: the program itself first when any of its shared triggers
/// is on, then every instance in id order. What a plain `weft resync`
/// brings up to date, one owner at a time, and (instances only) what
/// `weft deactivate --all-instances` takes down.
pub fn owners_of<'a>(live: impl IntoIterator<Item = &'a ActivationKey>) -> Vec<Owner> {
    let owners: std::collections::BTreeSet<Owner> = live.into_iter().map(|key| key.owner.clone()).collect();
    owners.into_iter().collect()
}

/// `POST /projects/{id}/activate`.
///
/// `running` (`runningPolicy` / `drainTimeoutSecs`) says what happens to
/// a worker built from an older image than the one being activated:
/// `cancel` (the default) cancels what it runs and replaces it now;
/// `wait` lets its in-flight work land first, up to the cap, then
/// replaces it. The CLI's `--running-policy` and `--drain-timeout` on
/// `weft activate`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivateRequest {
    #[serde(flatten)]
    pub target: ActivationTarget,
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Which activations: the triggers named (every one of the owner's
    /// when none is) for the instance named (the shared ones when none
    /// is). What `weft activate --trigger --instance` and a program's
    /// `ctx.trigger(..).instance(..).activate()` send; the action bar
    /// sends nothing, which is every shared trigger.
    #[serde(default)]
    pub scope: ActivationScope,
    /// The port the project's own address opens on, on an install that
    /// gives projects one (a machine), when the person asked for that one
    /// (`weft activate --port`): another program holding it is an error.
    /// Unset, the project keeps the port it had, and moves to a free one
    /// when another program took it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// What an activate points the project at: the hashes of the version
/// that built (absent when activating by id, which keeps the recorded
/// ones) and the answer to "what about the state a hibernated or parked
/// project kept". The half of an activate a resync carries verbatim; the
/// running-work half it takes from its own picker.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivationTarget {
    #[serde(flatten)]
    pub build: crate::builds::BuildHashes,
    /// Absent is [`ReactivateChoice::ExecuteParkedKeepSuspended`].
    #[serde(default, rename = "reactivateChoice", skip_serializing_if = "Option::is_none")]
    pub reactivate_choice: Option<ReactivateChoice>,
}

crate::wire_enum! {
    /// What an activate does with the state a deactivate kept (parked fires
    /// and suspended runs). Matters only when there is some; otherwise the
    /// activate is a fresh start whatever the choice.
    #[derive(Default)]
    pub enum ReactivateChoice {
        /// Replay every parked fire; suspended runs keep waiting.
        #[default]
        ExecuteParkedKeepSuspended = "execute_parked_keep_suspended",
        /// Drop the parked fires; suspended runs keep waiting.
        KeepSuspendedOnly = "keep_suspended_only",
        /// Drop every signal and cancel the runs these triggers fired: a
        /// fresh start, as after a `wipe` deactivate.
        WipeAll = "wipe_all",
    }
}

/// The `--reactivate-choice` flag's parse: a word nobody spells is
/// refused naming the ones that exist.
impl std::str::FromStr for ReactivateChoice {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Self::parse(s).ok_or_else(|| format!("unknown reactivate choice '{s}'; must be one of: {}", Self::accepted()))
    }
}

/// `POST /projects/{id}/bake`: which triggers to prepare, and what
/// happens to a worker built from an older image first.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BakeRequest {
    #[serde(flatten)]
    pub running: RunningChoice,
    #[serde(default)]
    pub scope: ActivationScope,
}

/// What an activate answers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivateResponse {
    pub urls: Vec<ActivationUrl>,
    /// The program's shared infra that is not running, and how to start
    /// it. Activating starts only what a trigger reads (and refuses when
    /// that is down), so a piece nothing in the program touches (a
    /// database only a website uses) would otherwise stay off unnoticed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub infra_not_running: Option<String>,
    /// The per-instance triggers a plain activate left off, each needing
    /// an instance named (`ActivationScope::per_instance_left_out`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_instance_left_out: Vec<String>,
    /// The project's own address, serving its routes at its root, when
    /// the install gives it one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<crate::projects::ProjectAddress>,
}

/// One address a listener answers on, for the trigger at `node_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationUrl {
    pub node_id: String,
    pub url: String,
}

/// What a resync answers: the listener URLs its reactivations minted,
/// and whose triggers it brought up to date, in the order it did them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResyncResponse {
    pub urls: Vec<ActivationUrl>,
    pub resynced: Vec<Owner>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_activate_flattens_its_target_and_running_choice() {
        let body = ActivateRequest {
            target: ActivationTarget {
                build: crate::builds::BuildHashes { binary_hash: Some("b".into()), ..Default::default() },
                reactivate_choice: Some(ReactivateChoice::WipeAll),
            },
            running: RunningChoice { running_policy: Some(crate::RunningPolicy::Wait), drain_timeout_secs: Some(9) },
            scope: ActivationScope::default(),
            port: None,
        };
        let wire = serde_json::to_value(&body).unwrap();
        assert_eq!(wire["binaryHash"], "b");
        assert_eq!(wire["reactivateChoice"], "wipe_all");
        assert_eq!(wire["runningPolicy"], "wait");
        assert_eq!(wire["drainTimeoutSecs"], 9);
        assert!(wire.get("definitionHash").is_none(), "an unnamed hash stays off the wire: {wire}");
        let back: ActivateRequest = serde_json::from_value(wire).unwrap();
        assert_eq!(back.target.reactivate_choice, Some(ReactivateChoice::WipeAll));
    }

    crate::wire_enum_roundtrip_tests!(ReactivateChoice);

    #[test]
    fn an_unknown_reactivate_choice_is_refused_naming_the_real_ones() {
        assert!("keep".parse::<ReactivateChoice>().unwrap_err().contains("wipe_all"));
    }

    #[test]
    fn an_activation_mode_is_one_flat_word_on_the_wire() {
        let words = [
            (ActivationMode::Registered, "registered"),
            (ActivationMode::Activating, "activating"),
            (ActivationMode::Active, "active"),
            (ActivationMode::Deactivating, "deactivating"),
            (ActivationMode::Down(DeactivationMode::Wipe), "wipe"),
            (ActivationMode::Down(DeactivationMode::Hibernate), "hibernate"),
            (ActivationMode::Down(DeactivationMode::Park), "park"),
        ];
        for (mode, word) in words {
            assert_eq!(serde_json::to_value(mode).unwrap(), serde_json::json!(word));
            assert_eq!(mode.as_str(), word);
            assert_eq!(serde_json::from_value::<ActivationMode>(serde_json::json!(word)).unwrap(), mode);
        }
        assert!(serde_json::from_value::<ActivationMode>(serde_json::json!("asleep")).is_err());
    }

    fn instance(id: &str) -> Owner {
        Owner::Instance(InstanceId::new(id).unwrap())
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                {"id": "cron", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}, "features": {"isTrigger": true}},
                {"id": "receive", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}, "features": {"isTrigger": true}, "perInstance": "derived"},
                {"id": "step", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}},
            ],
            "edges": [],
        }))
        .unwrap()
    }

    #[test]
    fn every_trigger_of_the_owner_kind() {
        let shared = ActivationScope::shared().resolve(&project()).unwrap();
        assert_eq!(shared, vec![ActivationKey::new("cron", Owner::Shared)]);
        let scope = ActivationScope { instance: Some(InstanceId::new("m").unwrap()), ..Default::default() };
        assert_eq!(scope.resolve(&project()).unwrap(), vec![ActivationKey::new("receive", instance("m"))]);
    }

    #[test]
    fn a_plain_activation_names_the_per_instance_triggers_it_leaves_off() {
        assert_eq!(ActivationScope::shared().per_instance_left_out(&project()), vec!["receive".to_string()]);
        let scope = ActivationScope { instance: Some(InstanceId::new("m").unwrap()), ..Default::default() };
        assert!(scope.per_instance_left_out(&project()).is_empty(), "an instance's activation leaves nothing off");
        let named = ActivationScope { triggers: vec!["cron".into()], instance: None };
        assert!(named.per_instance_left_out(&project()).is_empty(), "named triggers are exactly what was asked");
    }

    #[test]
    fn owners_come_once_each_the_program_first() {
        let live = [
            ActivationKey::new("receive", instance("b")),
            ActivationKey::new("cron", Owner::Shared),
            ActivationKey::new("receive", instance("a")),
            ActivationKey::new("other", instance("b")),
        ];
        assert_eq!(owners_of(&live), vec![Owner::Shared, instance("a"), instance("b")]);
        assert!(owners_of(&[]).is_empty());
    }

    #[test]
    fn a_named_trigger_must_fit_the_owner() {
        let err = ActivationScope { triggers: vec!["receive".into()], instance: None }.resolve(&project()).unwrap_err();
        assert!(err.contains("--instance"), "{err}");
        let err = ActivationScope { triggers: vec!["cron".into()], instance: Some(InstanceId::new("m").unwrap()) }
            .resolve(&project())
            .unwrap_err();
        assert!(err.contains("shared by every instance"), "{err}");
        let err = ActivationScope { triggers: vec!["step".into()], instance: None }.resolve(&project()).unwrap_err();
        assert!(err.contains("not a trigger"), "{err}");
        let one = ActivationScope { triggers: vec!["cron".into()], instance: None }.resolve(&project()).unwrap();
        assert_eq!(one, vec![ActivationKey::new("cron", Owner::Shared)]);
    }
}

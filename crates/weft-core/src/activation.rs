//! Which trigger activations a lifecycle verb names.
//!
//! The unit of activation is ONE trigger for ONE owner: the program's
//! shared copy of a trigger, or one instance's copy of a per-instance one.
//! These are the names every surface agrees on (the CLI's `--trigger`,
//! `--instance` and `--all-instances`, the editor's selection, a program's
//! `ctx.trigger(..)`), resolved against the program so a mismatch is
//! refused where it is written.

use serde::{Deserialize, Serialize};

use crate::instance::{InstanceId, InstanceTarget, Owner};
use crate::project::ProjectDefinition;
use crate::take_down::{Door, ReleaseHeld};

/// Where one activation stands, as a person reads it: `registered`
/// (never turned on), `activating`, `active`, or for one taken down, what
/// its door does (`take`, `hide`, `refuse`).
// SYNC: ActivationMode <-> packages/weft-graph/src/protocol.ts ActivationMode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActivationMode {
    Registered,
    Activating,
    Active,
    #[serde(untagged)]
    Down(Door),
}

impl ActivationMode {
    /// The wire word (the test below pins it to serde's).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Activating => "activating",
            Self::Active => "active",
            Self::Down(door) => door.as_str(),
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

/// Which activations a verb acts on: the named triggers, or every one,
/// of the owners `instances` names. What `weft activate` / `deactivate` /
/// `resync` / `bake` / `cancel-activate` send (`--trigger`, `--instance`,
/// `--all-instances`), what the editor sends for its selection, and what
/// a program's `ctx.trigger(..)` / `ctx.triggers()` calls send.
// SYNC: ActivationScope <-> packages/weft-graph/src/protocol.ts ActivationScope
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationScope {
    /// The triggers, by address. Empty means every trigger each owner
    /// has: every shared trigger for the shared owner, every per-instance
    /// trigger for an instance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<String>,
    /// Whose activations: the program's shared ones when absent.
    #[serde(default, skip_serializing_if = "InstanceTarget::is_shared")]
    pub instances: InstanceTarget,
}

impl ActivationScope {
    /// Every shared trigger: what a plain `weft activate` means.
    pub fn shared() -> Self {
        Self::default()
    }

    /// The named triggers (every one when empty) of one owner.
    pub fn of_owner(triggers: Vec<String>, owner: &Owner) -> Self {
        Self { triggers, instances: InstanceTarget::of(owner.instance().cloned()) }
    }

    /// The activations this scope names, against `project`, for every
    /// owner it names (`known` is every instance weft holds anything for,
    /// which [`InstanceTarget::EveryInstance`] reaches). A trigger that exists once
    /// per instance needs an instance, and a shared one cannot be named
    /// for an instance, so either mismatch is refused naming the fix; an
    /// unknown or non-trigger address is refused too. Sorted; empty when
    /// the program has no trigger of the owners' kind (the caller words
    /// that case).
    pub fn resolve<'a>(&self, project: &ProjectDefinition, known: impl IntoIterator<Item = &'a InstanceId>) -> Result<Vec<ActivationKey>, String> {
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
        // Which kinds of trigger the owners named have: the shared ones
        // the program's own, the per-instance ones an instance's; `--all`
        // names both, each trigger reaching the owners of its kind.
        let reaches_shared = self.instances.admits(&Owner::Shared);
        let reaches_instances = !self.instances.is_shared();
        let triggers: Vec<(String, bool)> = if self.triggers.is_empty() {
            spelled_per_instance
                .iter()
                .filter(|(_, p)| if *p { reaches_instances } else { reaches_shared })
                .cloned()
                .collect()
        } else {
            let mut named = Vec::new();
            for trigger in &self.triggers {
                let Some((_, is_per_instance)) = spelled_per_instance.iter().find(|(t, _)| t == trigger) else {
                    return Err(format!("'{trigger}' is not a trigger of this program"));
                };
                match (*is_per_instance, reaches_shared, reaches_instances) {
                    (true, _, false) => {
                        return Err(format!(
                            "trigger '{trigger}' exists once per instance; name the instances (`--instance <id>`, \
                             `--all-instances` or `--all` on a command, the `instance` input on a node)"
                        ));
                    }
                    (false, false, _) => {
                        return Err(format!(
                            "trigger '{trigger}' is shared by every instance, so it has no copy of an instance's; \
                             name no instance (leave out `--instance` and `--all-instances` on a command, or pass \
                             `--all`; leave the `instance` input unwired on a node)"
                        ));
                    }
                    _ => named.push((trigger.clone(), *is_per_instance)),
                }
            }
            named
        };
        let mut keys: Vec<ActivationKey> = self
            .instances
            .owners(known)
            .into_iter()
            .flat_map(|owner| {
                let per_instance_owner = owner.instance().is_some();
                triggers
                    .iter()
                    .filter(move |(_, per_instance)| *per_instance == per_instance_owner)
                    .map(move |(trigger, _)| ActivationKey::new(trigger.clone(), owner.clone()))
            })
            .collect();
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// Whether `key` is among what this scope names, read off the key
    /// alone: its owner is named, and its trigger is (or every trigger
    /// is). What a verb that only takes triggers down reads its rows with.
    pub fn admits(&self, key: &ActivationKey) -> bool {
        self.instances.admits(&key.owner) && (self.triggers.is_empty() || self.triggers.contains(&key.trigger))
    }

    /// The per-instance triggers a plain activation of the program's
    /// shared triggers leaves off (each exists once per instance, so it
    /// needs one named), sorted. Empty for any other scope. What `weft
    /// activate` reports, so nobody reads "activated" as "every trigger is
    /// on".
    pub fn per_instance_left_out(&self, project: &ProjectDefinition) -> Vec<String> {
        if !self.instances.is_shared() || !self.triggers.is_empty() {
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
/// is on, then every instance in id order.
pub fn owners_of<'a>(live: impl IntoIterator<Item = &'a ActivationKey>) -> Vec<Owner> {
    let owners: std::collections::BTreeSet<Owner> = live.into_iter().map(|key| key.owner.clone()).collect();
    owners.into_iter().collect()
}

/// `keys` grouped by owner, the program first: how a reply names them.
pub fn by_owner(keys: &[ActivationKey]) -> Vec<OwnerTriggers> {
    let mut grouped: std::collections::BTreeMap<Owner, Vec<String>> = std::collections::BTreeMap::new();
    for key in keys {
        grouped.entry(key.owner.clone()).or_default().push(key.trigger.clone());
    }
    grouped.into_iter().map(|(owner, triggers)| OwnerTriggers { owner, triggers }).collect()
}

/// `POST /projects/{id}/activations`: turn on every trigger the scope names
/// that is off, and leave the ones on as they are. It never touches the
/// runs going, and never starts infra.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ActivateRequest {
    /// The build the caller just made: refused when another registered
    /// since. Absent (activating by id, a program's own call), the
    /// registered build.
    #[serde(flatten)]
    pub build: crate::builds::BuildHashes,
    /// Which activations: the triggers named (every one of each owner's
    /// when none is) of the owners named (the shared ones when none is).
    #[serde(default)]
    pub scope: ActivationScope,
    /// What happens to the work the triggers held while they were down
    /// (`weft activate --held run|drop`).
    #[serde(default)]
    pub held: ReleaseHeld,
    /// The port the project's own address opens on, on an install that
    /// gives projects one (a machine), when the person asked for that one
    /// (`weft activate --port`): another program holding it is an error.
    /// Unset, the project keeps the port it had, and moves to a free one
    /// when another program took it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// `POST /projects/{id}/bake`: which triggers to prepare.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BakeRequest {
    #[serde(default)]
    pub scope: ActivationScope,
}

/// What an activate answers: what it turned on, what it did with what
/// they held, and where their routes answer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivateResponse {
    /// The triggers it turned on, by owner. Empty when every one named was
    /// on already: nothing to turn on, which is no error.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub turned_on: Vec<OwnerTriggers>,
    /// What those triggers held while they were down, and whether it is
    /// released (paced) or dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held: Option<HeldReleased>,
    #[serde(default)]
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
    /// The parts of the activated version that do not compile
    /// (`ProjectDefinition::broken`): no trigger whose cut needs one came
    /// on, and the CLI tells whoever activated about every one, by id or
    /// not.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub broken: Vec<crate::project::Broken>,
}

/// What `POST /projects/{id}/activations` answers: the task that carries the
/// activation out, followed in short looks
/// (`GET /projects/{id}/follow/{task}`) until it ends with the
/// [`ActivateResponse`], so no request has to stay open while its setup
/// runs, and the activation goes on whatever becomes of the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivateStarted {
    pub task: uuid::Uuid,
}

/// What an activate did with the work its triggers held: how much there
/// was, and the answer it acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeldReleased {
    pub held: crate::projects::HeldWork,
    pub release: ReleaseHeld,
}

/// One address a listener answers on, for the trigger at `node_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationUrl {
    pub node_id: String,
    pub url: String,
}

/// One owner's triggers, by name.
// SYNC: OwnerTriggers <-> packages/weft-graph/src/protocol.ts OwnerTriggers
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerTriggers {
    pub owner: Owner,
    pub triggers: Vec<String>,
}

impl OwnerTriggers {
    /// The triggers as a person reads them: `'door', 'feed'` for the
    /// program's own, `'inbox' of instance 'ada'` for an instance's.
    // SYNC: OwnerTriggers::words <-> packages/weft-graph/src/protocol.ts describeOwnerTriggers
    pub fn words(&self) -> String {
        let names = self.triggers.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", ");
        match &self.owner {
            Owner::Shared => names,
            Owner::Instance(instance) => format!("{names} of instance '{instance}'"),
        }
    }

    /// Several owners' triggers as a person reads them: each owner's
    /// [`Self::words`], apart with `; `.
    // SYNC: OwnerTriggers::list_words <-> packages/weft-graph/src/protocol.ts describeOwnerTriggers
    pub fn list_words(owned: &[OwnerTriggers]) -> String {
        owned.iter().map(OwnerTriggers::words).collect::<Vec<_>>().join("; ")
    }
}

/// What `POST /projects/{id}/cancel-activate` answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelActivateResponse {
    /// Set when the cancel gave up a resync (under way, or stopped): the
    /// triggers it had taken down and not brought back, which stay down
    /// until `weft activate` turns them on. Empty when it had brought
    /// every one back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resync_given_up: Option<Vec<OwnerTriggers>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_activate_flattens_its_build_and_names_what_to_do_with_held_work() {
        let body = ActivateRequest {
            build: crate::builds::BuildHashes { binary_hash: Some("b".into()), ..Default::default() },
            held: ReleaseHeld::Drop,
            ..Default::default()
        };
        let wire = serde_json::to_value(&body).unwrap();
        assert_eq!(wire["binaryHash"], "b");
        assert_eq!(wire["held"], "drop");
        assert!(wire.get("definitionHash").is_none(), "an unnamed hash stays off the wire: {wire}");
        let back: ActivateRequest = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(back.held, ReleaseHeld::Run, "held work runs unless the request says drop");
    }

    #[test]
    fn an_activation_mode_is_one_flat_word_on_the_wire() {
        let words = [
            (ActivationMode::Registered, "registered"),
            (ActivationMode::Activating, "activating"),
            (ActivationMode::Active, "active"),
            (ActivationMode::Down(Door::Take), "take"),
            (ActivationMode::Down(Door::Hide), "hide"),
            (ActivationMode::Down(Door::Refuse), "refuse"),
        ];
        for (mode, word) in words {
            assert_eq!(serde_json::to_value(mode).unwrap(), serde_json::json!(word));
            assert_eq!(mode.as_str(), word);
            assert_eq!(serde_json::from_value::<ActivationMode>(serde_json::json!(word)).unwrap(), mode);
        }
        assert!(serde_json::from_value::<ActivationMode>(serde_json::json!("park")).is_err());
    }

    fn instance(id: &str) -> Owner {
        Owner::Instance(InstanceId::new(id).unwrap())
    }

    fn ids(ids: &[&str]) -> Vec<InstanceId> {
        ids.iter().map(|id| InstanceId::new(*id).unwrap()).collect()
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                {"id": "cron", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}, "features": {"isTrigger": true}},
                {"id": "receive", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}, "features": {"isTrigger": true}, "perInstance": "derived"},
                {"id": "plain", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}},
            ],
            "edges": [],
        }))
        .unwrap()
    }

    /// No flag is the shared triggers only; named instances get their
    /// per-instance ones, and every instance weft knows for "all".
    #[test]
    fn every_trigger_of_each_owner_named() {
        let known = ids(&["b", "a"]);
        let shared = ActivationScope::shared().resolve(&project(), &known).unwrap();
        assert_eq!(shared, vec![ActivationKey::new("cron", Owner::Shared)]);
        let named = ActivationScope { instances: InstanceTarget::named(ids(&["m"])), ..Default::default() };
        assert_eq!(named.resolve(&project(), &known).unwrap(), vec![ActivationKey::new("receive", instance("m"))]);
        let all = ActivationScope { instances: InstanceTarget::EveryInstance, ..Default::default() };
        assert_eq!(
            all.resolve(&project(), &known).unwrap(),
            vec![ActivationKey::new("receive", instance("a")), ActivationKey::new("receive", instance("b"))]
        );
    }

    /// `--all` is every shared trigger for the program and every
    /// per-instance one for each instance; a trigger named beside it reaches
    /// the owners of its kind, shared or per instance.
    #[test]
    fn all_names_each_trigger_for_the_owners_of_its_kind() {
        let known = ids(&["b", "a"]);
        let everything = ActivationScope { instances: InstanceTarget::Everything, ..Default::default() };
        assert_eq!(
            everything.resolve(&project(), &known).unwrap(),
            vec![
                ActivationKey::new("cron", Owner::Shared),
                ActivationKey::new("receive", instance("a")),
                ActivationKey::new("receive", instance("b")),
            ]
        );
        let shared = ActivationScope { triggers: vec!["cron".into()], instances: InstanceTarget::Everything };
        assert_eq!(shared.resolve(&project(), &known).unwrap(), vec![ActivationKey::new("cron", Owner::Shared)]);
        let per_instance = ActivationScope { triggers: vec!["receive".into()], instances: InstanceTarget::Everything };
        assert_eq!(
            per_instance.resolve(&project(), &known).unwrap(),
            vec![ActivationKey::new("receive", instance("a")), ActivationKey::new("receive", instance("b"))]
        );
        let none: [InstanceId; 0] = [];
        assert_eq!(everything.resolve(&project(), &none).unwrap(), vec![ActivationKey::new("cron", Owner::Shared)], "no instance yet");
    }

    #[test]
    fn a_plain_activation_names_the_per_instance_triggers_it_leaves_off() {
        assert_eq!(ActivationScope::shared().per_instance_left_out(&project()), vec!["receive".to_string()]);
        let scope = ActivationScope { instances: InstanceTarget::EveryInstance, ..Default::default() };
        assert!(scope.per_instance_left_out(&project()).is_empty(), "an instance's activation leaves nothing off");
        let named = ActivationScope { triggers: vec!["cron".into()], ..Default::default() };
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
        assert_eq!(
            by_owner(&live),
            vec![
                OwnerTriggers { owner: Owner::Shared, triggers: vec!["cron".into()] },
                OwnerTriggers { owner: instance("a"), triggers: vec!["receive".into()] },
                OwnerTriggers { owner: instance("b"), triggers: vec!["receive".into(), "other".into()] },
            ]
        );
    }

    #[test]
    fn a_named_trigger_must_fit_the_owner() {
        let none: [InstanceId; 0] = [];
        let err = ActivationScope { triggers: vec!["receive".into()], ..Default::default() }.resolve(&project(), &none).unwrap_err();
        assert!(err.contains("--all-instances"), "{err}");
        let err = ActivationScope { triggers: vec!["cron".into()], instances: InstanceTarget::named(ids(&["m"])) }
            .resolve(&project(), &none)
            .unwrap_err();
        assert!(err.contains("name no instance"), "{err}");
        let err = ActivationScope { triggers: vec!["plain".into()], ..Default::default() }.resolve(&project(), &none).unwrap_err();
        assert!(err.contains("not a trigger"), "{err}");
        let one = ActivationScope { triggers: vec!["cron".into()], ..Default::default() }.resolve(&project(), &none).unwrap();
        assert_eq!(one, vec![ActivationKey::new("cron", Owner::Shared)]);
    }

    #[test]
    fn a_scope_admits_the_keys_of_the_owners_and_triggers_it_names() {
        let scope = ActivationScope { triggers: vec!["receive".into()], instances: InstanceTarget::EveryInstance };
        assert!(scope.admits(&ActivationKey::new("receive", instance("z"))));
        assert!(!scope.admits(&ActivationKey::new("other", instance("z"))));
        assert!(!scope.admits(&ActivationKey::new("receive", Owner::Shared)));
        assert!(ActivationScope::shared().admits(&ActivationKey::new("cron", Owner::Shared)));
    }
}

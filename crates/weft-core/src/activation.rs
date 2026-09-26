//! Which trigger activations a lifecycle verb names.
//!
//! The unit of activation is ONE trigger for ONE owner: the program's
//! shared copy of a trigger, or one member's copy of a per-member one.
//! These are the names every surface agrees on (the CLI's `--trigger` and
//! `--member`, the editor's action bar, a program's `ctx.trigger(..)`),
//! resolved against the program so a mismatch is refused where it is
//! written.

use serde::{Deserialize, Serialize};

use crate::member::{MemberId, Owner};
use crate::project::ProjectDefinition;
use crate::running_policy::DeactivationMode;

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

    pub fn member(&self) -> Option<&MemberId> {
        self.owner.member()
    }
}

impl std::fmt::Display for ActivationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.owner {
            Owner::Shared => write!(f, "'{}'", self.trigger),
            Owner::Member(member) => write!(f, "'{}' of member '{member}'", self.trigger),
        }
    }
}

/// Which activations a verb acts on: every trigger of one owner, or the
/// named ones. What `weft activate` / `deactivate` / `resync` / `bake` /
/// `cancel-activate` send (`--trigger`, `--member`), what the editor's
/// action bar sends (everything shared), and what a program's
/// `ctx.trigger(..)` / `ctx.triggers()` calls send.
// SYNC: ActivationScope <-> packages/weft-graph/src/protocol.ts ActivationScope
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActivationScope {
    /// The triggers, by address. Empty means every trigger the owner
    /// has: every shared trigger for [`Owner::Shared`], every
    /// per-member trigger for a member.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<String>,
    /// Whose activations: `None` for the program's shared ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberId>,
}

impl ActivationScope {
    /// Every shared trigger: what a plain `weft activate` means.
    pub fn shared() -> Self {
        Self::default()
    }

    pub fn owner(&self) -> Owner {
        Owner::from_member(self.member.clone())
    }

    /// The activations this scope names, against `project`. A trigger
    /// that exists once per member needs a member, and a shared one
    /// cannot be activated for a member, so either mismatch is refused
    /// naming the fix. An unknown or non-trigger address is refused too.
    /// Sorted, never empty unless the project has no trigger of the
    /// owner's kind (the caller words that case).
    pub fn resolve(&self, project: &ProjectDefinition) -> Result<Vec<ActivationKey>, String> {
        let owner = self.owner();
        let places = crate::project::trigger_places(project);
        let spelled_per_member: Vec<(String, bool)> = places
            .iter()
            .map(|place| {
                (
                    crate::project::address_of(project, &place.id, &place.path),
                    crate::project::is_per_member(project, &place.id),
                )
            })
            .collect();
        let mut keys: Vec<ActivationKey> = if self.triggers.is_empty() {
            spelled_per_member
                .into_iter()
                .filter(|(_, per_member)| *per_member == matches!(owner, Owner::Member(_)))
                .map(|(trigger, _)| ActivationKey::new(trigger, owner.clone()))
                .collect()
        } else {
            let mut keys = Vec::new();
            for named in &self.triggers {
                let Some((trigger, per_member)) = spelled_per_member.iter().find(|(t, _)| t == named) else {
                    return Err(format!("'{named}' is not a trigger of this program"));
                };
                match (&owner, per_member) {
                    (Owner::Shared, true) => {
                        return Err(format!(
                            "trigger '{trigger}' exists once per member; name whose with --member <id>"
                        ));
                    }
                    (Owner::Member(member), false) => {
                        return Err(format!(
                            "trigger '{trigger}' is shared by every member, so it cannot be activated \
                             for member '{member}'; leave --member out"
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
}


/// The owners among `live` (the keys of the activations that are on),
/// each once: the program itself first when any of its shared triggers
/// is on, then every member in id order. What a plain `weft resync`
/// brings up to date, one owner at a time, and (members only) what
/// `weft deactivate --all-members` takes down.
pub fn owners_of<'a>(live: impl IntoIterator<Item = &'a ActivationKey>) -> Vec<Owner> {
    let owners: std::collections::BTreeSet<Owner> = live.into_iter().map(|key| key.owner.clone()).collect();
    owners.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn member(id: &str) -> Owner {
        Owner::Member(MemberId::new(id).unwrap())
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                {"id": "cron", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}, "features": {"isTrigger": true}},
                {"id": "receive", "nodeType": "T", "config": {}, "position": {"x": 0, "y": 0}, "features": {"isTrigger": true}, "perMember": "derived"},
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
        let scope = ActivationScope { member: Some(MemberId::new("m").unwrap()), ..Default::default() };
        assert_eq!(scope.resolve(&project()).unwrap(), vec![ActivationKey::new("receive", member("m"))]);
    }

    #[test]
    fn owners_come_once_each_the_program_first() {
        let live = [
            ActivationKey::new("receive", member("b")),
            ActivationKey::new("cron", Owner::Shared),
            ActivationKey::new("receive", member("a")),
            ActivationKey::new("other", member("b")),
        ];
        assert_eq!(owners_of(&live), vec![Owner::Shared, member("a"), member("b")]);
        assert!(owners_of(&[]).is_empty());
    }

    #[test]
    fn a_named_trigger_must_fit_the_owner() {
        let err = ActivationScope { triggers: vec!["receive".into()], member: None }.resolve(&project()).unwrap_err();
        assert!(err.contains("--member"), "{err}");
        let err = ActivationScope { triggers: vec!["cron".into()], member: Some(MemberId::new("m").unwrap()) }
            .resolve(&project())
            .unwrap_err();
        assert!(err.contains("shared by every member"), "{err}");
        let err = ActivationScope { triggers: vec!["step".into()], member: None }.resolve(&project()).unwrap_err();
        assert!(err.contains("not a trigger"), "{err}");
        let one = ActivationScope { triggers: vec!["cron".into()], member: None }.resolve(&project()).unwrap();
        assert_eq!(one, vec![ActivationKey::new("cron", Owner::Shared)]);
    }
}

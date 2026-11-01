//! What taking infra copies down reaches: the triggers whose cut contains
//! them, and the runs whose own cut does.
//!
//! A trigger's cut is recorded with its setup (`trigger_activation`'s
//! `depends_on`, `weft_core::run_spec::TriggerCut`), and a run's cut is the
//! program it runs (`run.definition_hash`), so both answers read what was
//! set up and what runs, never the source as it is now. A shared copy is
//! reached by every owner's triggers and runs (an instance's trigger may
//! read shared infra); an instance's copy only by that instance's.

use std::collections::{BTreeSet, HashMap};

use weft_core::instance::Copies;
use weft_core::run_spec::TriggerCuts;
use weft_core::RunId;
use weft_task_store::db::Client as _;

use crate::activation_store::Activation;
use crate::state::DispatcherState;

/// Some infra copies going down: the places, and whose copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraReach {
    /// The infra places, spelled the way the program writes them.
    pub places: BTreeSet<String>,
    pub copies: Copies,
}

impl InfraReach {
    /// Whether a trigger or run of `owner` (`None`: the shared one) whose
    /// cut contains `depends_on` is reached.
    fn reaches<'a>(&self, depends_on: impl IntoIterator<Item = &'a String>, owner: Option<&weft_core::instance::InstanceId>) -> bool {
        let owners_copy = match &self.copies {
            Copies::Instance(instance) => owner == Some(instance),
            Copies::Shared | Copies::Every => true,
        };
        owners_copy && depends_on.into_iter().any(|place| self.places.contains(place))
    }
}

impl InfraReach {
    /// The copies `copies` of every infra place `project` declares of their
    /// kind (the shared places for the shared copies, the per-instance ones
    /// for an instance's, all of them for every copy), or of the one
    /// `node` names: what an infra command over the whole owner, or over one
    /// node, takes down.
    pub fn of_command(project: &weft_core::ProjectDefinition, node: Option<&str>, copies: Copies) -> Self {
        let places = match node {
            Some(node) => BTreeSet::from([node.to_string()]),
            None => {
                let declared = weft_core::project::DeclaredInfra::of(project);
                weft_core::project::infra_place_spellings(project)
                    .into_keys()
                    .filter(|place| match &copies {
                        Copies::Shared => declared.declares(place, false),
                        Copies::Instance(_) => declared.declares(place, true),
                        Copies::Every => true,
                    })
                    .collect()
            }
        };
        Self { places, copies }
    }
}

/// The infra places the activation's trigger depends on: what its setup
/// recorded, or, for one whose first setup has not finished, what the
/// registered build's cut of it says (`cuts`, as its registration walked
/// them: `DispatcherState::trigger_cuts`).
pub fn depends_on(activation: &Activation, cuts: &TriggerCuts) -> Vec<String> {
    match &activation.cut {
        Some(cut) => cut.depends_on.clone(),
        None => registered_depends_on(cuts, &activation.key.trigger),
    }
}

/// What the registered build's cut of `trigger` depends on: nothing for a
/// trigger the build does not have, or whose cut cannot be walked.
pub fn registered_depends_on(cuts: &TriggerCuts, trigger: &str) -> Vec<String> {
    cuts.get(trigger).and_then(|cut| cut.as_ref().ok()).map(|cut| cut.depends_on.clone()).unwrap_or_default()
}

/// Of `activations`, the ones whose trigger's cut contains one of the
/// copies `reach` names.
pub fn activations_reached<'a>(reach: &InfraReach, activations: &'a [Activation], cuts: &TriggerCuts) -> Vec<&'a Activation> {
    activations.iter().filter(|a| reach.reaches(&depends_on(a, cuts), a.key.instance())).collect()
}

/// The triggers on whose cut contains a copy `reach` names: what taking
/// those copies down takes down with them, whoever takes them down (an
/// infra verb, `crate::api::infra::triggers_reached`, or the health check,
/// `crate::lifecycle_claimer`).
pub fn live_triggers_reached(reach: &InfraReach, activations: &[Activation], cuts: &TriggerCuts) -> Vec<weft_core::activation::ActivationKey> {
    activations_reached(reach, activations, cuts)
        .into_iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .map(|a| a.key.clone())
        .collect()
}

/// One live run `reach` reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReached {
    pub run_id: RunId,
    /// Paused on a wait, rather than queued or going now.
    pub paused: bool,
}

/// The infra places a run of the program `definition_hash` reaches
/// (`weft_core::project::infra_reached`), each program read once.
struct Programs<'a> {
    state: &'a DispatcherState,
    project_id: uuid::Uuid,
    read: HashMap<String, BTreeSet<String>>,
}

impl Programs<'_> {
    async fn infra_of(&mut self, definition_hash: &str) -> anyhow::Result<&BTreeSet<String>> {
        if !self.read.contains_key(definition_hash) {
            let program = self
                .state
                .program(self.project_id, definition_hash)
                .await?
                .ok_or_else(|| anyhow::anyhow!("no program {definition_hash} of project {} is on record", self.project_id))?;
            self.read.insert(definition_hash.to_string(), weft_core::project::infra_reached(&program));
        }
        Ok(&self.read[definition_hash])
    }
}

/// Every run of the project that has not ended whose own cut contains one
/// of the copies `reach` names, never `except` (the run that asked: its
/// own `StopSelf` decides its fate).
pub async fn runs_reached(state: &DispatcherState, project_id: uuid::Uuid, reach: &InfraReach, except: Option<RunId>) -> anyhow::Result<Vec<RunReached>> {
    let rows: Vec<(RunId, Option<String>, Option<String>, bool)> = state
        .pg_pool
        .all(
            "SELECT run_id, instance_id, definition_hash, state = 'parked' FROM run \
             WHERE project_id = $1 AND state <> 'ended' ORDER BY started_at, run_id",
            &[&project_id],
        )
        .await?;
    let mut programs = Programs { state, project_id, read: HashMap::new() };
    let mut out = Vec::new();
    for (run_id, instance, definition_hash, paused) in rows {
        // A run of no program (a node test) reaches no infra.
        let Some(definition_hash) = definition_hash else { continue };
        if Some(run_id) == except {
            continue;
        }
        let instance = instance.map(weft_core::instance::InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("run {run_id} holds a broken instance: {e}"))?;
        if reach.reaches(programs.infra_of(&definition_hash).await?, instance.as_ref()) {
            out.push(RunReached { run_id, paused });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};

    fn activation(trigger: &str, owner: Owner, depends_on: &[&str]) -> Activation {
        Activation {
            key: ActivationKey::new(trigger, owner),
            lifecycle: crate::activation_store::ActivationLifecycle::active(),
            program: None,
            source_version: None,
            driver_task: None,
            activating: None,
            cut: Some(crate::activation_store::RecordedCut {
                fingerprint: "f".into(),
                depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
            }),
        }
    }

    /// A shared copy reaches every owner's triggers whose cut contains it;
    /// an instance's copy only that instance's; a trigger whose cut has no
    /// infra is never reached.
    #[test]
    fn a_copy_reaches_the_triggers_whose_cut_contains_it() {
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        let bob = Owner::Instance(InstanceId::new("bob").unwrap());
        let rows = [
            activation("door", Owner::Shared, &["db"]),
            activation("inbox", ada.clone(), &["db", "bridge"]),
            activation("inbox", bob.clone(), &["bridge"]),
            activation("tick", Owner::Shared, &[]),
        ];
        let reached = |places: &[&str], copies: Copies| -> Vec<String> {
            let reach = InfraReach { places: places.iter().map(|s| s.to_string()).collect(), copies };
            activations_reached(&reach, &rows, &TriggerCuts::new()).into_iter().map(|a| a.key.to_string()).collect()
        };
        assert_eq!(reached(&["db"], Copies::Shared), vec!["'door'", "'inbox' of instance 'ada'"]);
        assert_eq!(reached(&["bridge"], Copies::Instance(InstanceId::new("bob").unwrap())), vec!["'inbox' of instance 'bob'"]);
        assert!(reached(&["nothing"], Copies::Every).is_empty());
    }
}

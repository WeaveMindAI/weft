//! A run's birth: the events every way of starting a run writes first.

use crate::RunEvent;

/// Everything a run is born with. Every start path (manual run,
/// setup phases, entry-trigger fire, live-trigger fire) fills one, so a
/// field added to the birth has one home.
pub struct Birth<'a> {
    pub run_id: weft_core::RunId,
    pub project_id: uuid::Uuid,
    pub phase: weft_core::context::Phase,
    pub entry_node: &'a str,
    pub kicks: &'a [weft_core::run_spec::KickPlan],
    /// The program the run executes (a cut of the project's program,
    /// stored under its own hash) and the registered program it was cut
    /// from, as the run's start keeps it: the birth names both from it.
    pub program: &'a crate::record::RunsProgram,
    /// What the run is given at the places its program says (handed
    /// values, stand-ins' outputs): the run's own, on its birth.
    pub given: &'a weft_core::project::Given,
    /// The worker binary the run executes on.
    pub binary_hash: &'a str,
    /// The run this one inherits from (`weft run --seed`); `None` for a
    /// run from nothing, which is every fire and every setup phase.
    pub seed: Option<crate::Seed>,
    pub source_version: Option<&'a str>,
    /// Which instance the run is for and what it provides; `None` for a
    /// shared run.
    pub instance: Option<RunFor<'a>>,
    /// The install's picks for the run's connections (`weft_core::picks::run_picks`).
    pub picks: &'a weft_core::picks::Picks,
    /// The trigger whose firing starts this run, spelled; `None` for a
    /// run started by hand and for a setup run.
    pub fired_trigger: Option<&'a str>,
    /// For a run started by hand through a trigger that answers a caller,
    /// that trigger's spec: its stand-in caller serves the firing kick's
    /// request (`RunEvent::RunStarted::stand_in`).
    pub stand_in: Option<&'a weft_core::primitive::SignalSpec>,
    /// How the run is kept (`weft_core::run_settings`): the starting
    /// signal's, `weft run`'s flags, or the runtime's bookkeeping settings
    /// for a setup run. Required: a way of starting runs that does not say
    /// how they are kept does not compile.
    pub settings: weft_core::run_settings::RunSettings,
    pub at_unix: u64,
}

/// The two event shapes that give a run its identity: the one
/// `RunStarted` and one `NodeKicked` per root. How they are written
/// differs per path (a queued run's first row, a worker's batch); the
/// events do not.
pub fn birth_events(birth: Birth<'_>) -> (RunEvent, Vec<RunEvent>) {
    let Birth {
        run_id, project_id, phase, entry_node, kicks, program, given, binary_hash, seed, source_version, instance, picks,
        fired_trigger, stand_in, settings, at_unix,
    } = birth;
    let start = RunEvent::RunStarted {
        run_id,
        project_id,
        entry_node: entry_node.to_string(),
        phase,
        definition_hash: Some(program.definition_hash().to_string()),
        cut_from: program.cut_from().map(str::to_string),
        given: Box::new(given.clone()),
        binary_hash: Some(binary_hash.to_string()),
        source_version: source_version.map(str::to_string),
        run_kind: weft_core::run::RunKind::Project,
        seed,
        instance: instance.map(|m| m.instance.clone()),
        instance_values: Box::new(instance.map(|m| m.values.clone()).unwrap_or_default()),
        picks: Box::new(picks.clone()),
        fired_trigger: fired_trigger.map(str::to_string),
        stand_in: stand_in.cloned().map(Box::new),
        settings,
        at_unix,
    };
    let kick_events = kicks
        .iter()
        .map(|kick| RunEvent::NodeKicked {
            run_id,
            node_id: kick.node.clone(),
            frames: kick.frames.clone(),
            firing: kick.firing,
            payload: kick.payload.clone(),
            port_snapshot: kick.port_snapshot.clone(),
            at_unix,
        })
        .collect();
    (start, kick_events)
}

/// The rows a run is born with for the infra places it reads baked
/// (`weft_core::infra::bake::covered`): a line in each one's log saying it
/// did not run and why. Their saved values go out from what the run is
/// given, where its program has each stand in for itself
/// (`weft_core::project::Given`).
pub fn baked_rows(
    run_id: weft_core::RunId,
    program: &weft_core::project::ProjectDefinition,
    index: &weft_core::project::ProgramIndex,
    baked: &weft_core::infra::bake::Saved,
    at_unix: u64,
) -> Vec<RunEvent> {
    baked
        .iter()
        .map(|(place, values)| RunEvent::LogLine {
            run_id,
            node_id: place.id.clone(),
            frames: index.frames_at(program, place),
            level: "info".to_string(),
            message: format!(
                "did not run: everything this run reads from it is baked, so its saved {} went out instead",
                values.keys().map(String::as_str).collect::<Vec<_>>().join(", ")
            ),
            at_unix_ms: None,
            seq: None,
            at_unix,
        })
        .collect()
}

/// Which instance a run is for, and what it provides for its `@instance_filled`
/// fields: the two travel together from the check that read the values
/// to the birth that journals them, so a run for an instance can never be
/// born without the values that check approved.
#[derive(Debug, Clone, Copy)]
pub struct RunFor<'a> {
    pub instance: &'a weft_core::instance::InstanceId,
    pub values: &'a weft_core::instance::InstanceValues,
}

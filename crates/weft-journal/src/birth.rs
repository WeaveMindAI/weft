//! A run's birth: the events every way of starting a run writes first.

use crate::ExecEvent;

/// Everything an execution is born with. Every start path (manual run,
/// setup phases, entry-trigger fire, live-trigger fire) fills one, so a
/// field added to the birth has one home.
pub struct Birth<'a> {
    pub execution_id: weft_core::ExecutionId,
    pub project_id: uuid::Uuid,
    pub phase: weft_core::context::Phase,
    pub entry_node: &'a str,
    pub kicks: &'a [weft_core::run_spec::KickPlan],
    /// The program the run executes: its definition and its worker binary.
    pub definition_hash: &'a str,
    pub binary_hash: &'a str,
    /// The run's selection, recorded so the engine dispatches nothing
    /// outside it and a resume rebuilds the same boundary. Every trigger
    /// fire, targeted manual run and setup phase carries one (a setup
    /// phase's is `RunSelection::setup` over its triggers or infra
    /// nodes, and the engine refuses a setup row without it); `None`
    /// only for a manual run of the whole graph.
    pub selection: Option<&'a weft_core::project::selection::RecordedSelection>,
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
    /// request (`ExecEvent::ExecutionStarted::stand_in`).
    pub stand_in: Option<&'a weft_core::primitive::SignalSpec>,
    /// How the run is kept (`weft_core::run_settings`): the starting
    /// signal's, `weft run`'s flags, or the runtime's bookkeeping settings
    /// for a setup run. Required: a way of starting runs that does not say
    /// how they are kept does not compile.
    pub settings: weft_core::run_settings::RunSettings,
    pub at_unix: u64,
}

/// The two event shapes that give an execution its identity: the one
/// `ExecutionStarted` and one `NodeKicked` per root. How they are written
/// differs per path (a queued run's first row, a worker's batch); the
/// events do not.
pub fn birth_events(birth: Birth<'_>) -> (ExecEvent, Vec<ExecEvent>) {
    let Birth {
        execution_id, project_id, phase, entry_node, kicks, definition_hash, binary_hash, selection, seed, source_version, instance, picks,
        fired_trigger, stand_in, settings, at_unix,
    } = birth;
    let start = ExecEvent::ExecutionStarted {
        execution_id,
        project_id,
        entry_node: entry_node.to_string(),
        phase,
        definition_hash: Some(definition_hash.to_string()),
        binary_hash: Some(binary_hash.to_string()),
        source_version: source_version.map(str::to_string),
        run_kind: weft_core::exec::RunKind::Execution,
        selection: selection.cloned(),
        seed,
        instance: instance.map(|m| m.instance.clone()),
        instance_values: Box::new(instance.map(|m| m.values.clone()).unwrap_or_default()),
        picks: Box::new(picks.clone()),
        fired_trigger: fired_trigger.map(str::to_string),
        stand_in: stand_in.cloned(),
        settings,
        at_unix,
    };
    let kick_events = kicks
        .iter()
        .map(|kick| ExecEvent::NodeKicked {
            execution_id,
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

/// The rows a run is born with for what its suppliers hand it instead of
/// running (`RunSelection::suppliers`: an `--emit` by hand, an infra
/// place read baked): each value a source supplies, as that source's
/// own emission (`provided: true`), a stream's items one by one then
/// closed, and every output a source supplies nothing for closed. A
/// source is a node at the frames it emits under, with its values by port.
pub fn supplied_rows<'a>(
    project: &weft_core::ProjectDefinition,
    execution_id: weft_core::ExecutionId,
    sources: impl IntoIterator<Item = (&'a str, &'a weft_core::frames::LoopFrames, std::collections::BTreeMap<&'a str, &'a serde_json::Value>)>,
    at_unix: u64,
) -> Vec<ExecEvent> {
    let mut events = Vec::new();
    for (source, frames, ports) in sources {
        let Some(node) = project.nodes.iter().find(|node| node.id == source) else { continue };
        // Two places of one node (a file included twice) emit under their
        // own frames; their ids differ by them.
        let at = if frames.is_empty() { source.to_string() } else { format!("{source}\0{}", weft_core::frames::frames_text(frames)) };
        for port in &node.outputs {
            let supplied = ports.get(port.name.as_str()).copied();
            let generator = matches!(port.port_type, weft_core::weft_type::WeftType::Generator(_));
            let values: Vec<&serde_json::Value> = match supplied {
                Some(serde_json::Value::Array(items)) if generator => items.iter().collect(),
                Some(value) => vec![value],
                None => vec![],
            };
            for (index, value) in values.into_iter().enumerate() {
                events.push(ExecEvent::PortEmitted {
                    execution_id,
                    emission_id: uuid::Uuid::new_v5(&execution_id, format!("supplied\0{at}\0{}\0{index}", port.name).as_bytes()),
                    node_id: source.to_string(),
                    frames: frames.clone(),
                    port: port.name.clone(),
                    value: std::sync::Arc::new(value.clone()),
                    provided: true,
                    at_unix,
                });
            }
            if generator || supplied.is_none() {
                events.push(ExecEvent::PortClosed {
                    execution_id,
                    emission_id: uuid::Uuid::new_v5(&execution_id, format!("supplied-end\0{at}\0{}", port.name).as_bytes()),
                    node_id: source.to_string(),
                    frames: frames.clone(),
                    port: port.name.clone(),
                    provided: true,
                    at_unix,
                });
            }
        }
    }
    events
}

/// The rows a run is born with for the infra places it reads baked
/// (`weft_core::infra::bake::covered`): their saved values, as they
/// supply them ([`supplied_rows`]), and a line in each one's log saying
/// it did not run and why.
pub fn baked_rows(
    project: &weft_core::ProjectDefinition,
    execution_id: weft_core::ExecutionId,
    baked: &weft_core::infra::bake::Saved,
    at_unix: u64,
) -> Vec<ExecEvent> {
    let frames: Vec<(&weft_core::frames::Located, weft_core::frames::LoopFrames)> = baked.keys().map(|place| (place, place.frames())).collect();
    let mut rows: Vec<ExecEvent> = frames
        .iter()
        .map(|(place, frames)| ExecEvent::LogLine {
            execution_id,
            node_id: place.id.clone(),
            frames: frames.clone(),
            level: "info".to_string(),
            message: format!(
                "did not run: everything this run reads from it is baked, so its saved {} went out instead",
                baked[*place].keys().map(String::as_str).collect::<Vec<_>>().join(", ")
            ),
            at_unix_ms: None,
            seq: None,
            at_unix,
        })
        .collect();
    rows.extend(supplied_rows(
        project,
        execution_id,
        frames.iter().map(|(place, frames)| (place.id.as_str(), frames, baked[*place].iter().map(|(port, value)| (port.as_str(), value)).collect())),
        at_unix,
    ));
    rows
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


#[cfg(test)]
mod supplied_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_supplier_is_born_with_its_values_and_its_other_outputs_closed() {
        let project: weft_core::ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "db", "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                "inputs": [], "features": {}, "requiresInfra": false,
                "outputs": [
                    {"name": "access", "portType": "String", "required": true},
                    {"name": "status", "portType": "String", "required": true}
                ]
            }],
            "edges": []
        }))
        .unwrap();
        let execution_id = weft_core::ExecutionId::from_u128(9);
        let frames = weft_core::frames::LoopFrames::new();
        let value = json!("a1");
        let supplied = || [("db", &frames, std::collections::BTreeMap::from([("access", &value)]))];
        let rows = supplied_rows(&project, execution_id, supplied(), 5);
        assert!(matches!(&rows[0], ExecEvent::PortEmitted { node_id, port, value, provided: true, .. }
            if node_id == "db" && port == "access" && **value == json!("a1")));
        assert!(matches!(&rows[1], ExecEvent::PortClosed { port, provided: true, .. } if port == "status"));
        assert_eq!(rows.len(), 2);
        let again = serde_json::to_value(supplied_rows(&project, execution_id, supplied(), 5)).unwrap();
        assert_eq!(again, serde_json::to_value(&rows).unwrap(), "the same run is born the same");
    }
}

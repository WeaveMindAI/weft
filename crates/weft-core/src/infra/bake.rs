//! An infra node's baked outputs: values its infra setup makes and weft
//! saves with its copy, so a run can use them without running the node.
//!
//! The node's metadata marks which of its outputs are baked
//! (`OutputSpec::baked`, carried as `NodeDefinition::baked_outputs`). The
//! run that applies its infra (the infra setup) runs its body and saves
//! what went out on those outputs (`weft_engine`'s execution driver); the
//! infra itself can change a saved value later with no node running
//! ([`PushedValues`]).
//!
//! A later run asks one question per such node: is every output this run
//! reads from it baked, with a value saved? If so ([`covered`]), the node
//! does not run: its saved values go out in its place, the way an `--emit`
//! hands a value over by hand (`RunSelection::suppliers`), nothing that
//! only feeds it runs either, and its log says why. If the run reads any
//! output that is not baked (a status that moves), the node runs as any
//! node does, and a baked output its body sends nothing on carries the
//! saved value instead of closing.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::Located;
use crate::project::ProjectDefinition;

/// Saved values by place: port to value.
pub type Saved = BTreeMap<Located, BTreeMap<String, Value>>;

/// What the running copies a run for `instance` reads have saved for
/// their baked outputs, by place: each place's copy is the one the run
/// reads (`super::run_gate::copies_read`), and a copy that is not running
/// counts for nothing. A saved value that no longer fits its output's type
/// (the node's code changed since it was saved) counts for nothing either,
/// checked by the same gate a node's emitted values pass
/// (`WeftType::accepts_runtime_value`): the node then runs, and its run
/// either sends a fresh value or fails naming `weft infra upgrade`.
pub fn saved_for_run(
    project: &ProjectDefinition,
    copies: &[super::run_gate::InfraCopyUp],
    instance: Option<&crate::instance::InstanceId>,
) -> Saved {
    let mut out = Saved::new();
    let wanted = super::run_gate::copies_read(project, None, instance);
    for place in crate::project::infra_places(project) {
        let Some(node) = project.nodes.iter().find(|n| n.id == place.id) else { continue };
        if node.baked_outputs.is_empty() {
            continue;
        }
        let spelled = crate::project::address_of(project, &place.id, &place.path);
        let Some((_, Some(copy))) = wanted.iter().find(|(at, _)| *at == spelled) else { continue };
        let Some(row) = copies.iter().find(|row| row.node_id == spelled && row.instance.as_ref() == *copy && row.running) else {
            continue;
        };
        let fits = |port: &str, value: &Value| {
            node.outputs
                .iter()
                .find(|output| output.name == port)
                .is_some_and(|output| output.port_type.accepts_runtime_value(value))
        };
        let values: BTreeMap<String, Value> = row
            .baked
            .iter()
            .filter(|(port, value)| node.baked_outputs.contains(*port) && fits(port, value))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !values.is_empty() {
            out.insert(place, values);
        }
    }
    out
}

/// Of `saved`, the places a run whose nodes are `runs` reads only baked,
/// saved outputs of: at least one output read, every one read baked and
/// saved. Reads are the wires into a node of `runs`; a place named in
/// `named` (what the run was asked to start at, stop at or run) always
/// runs.
pub fn covered(project: &ProjectDefinition, saved: &Saved, runs: &BTreeSet<Located>, named: &BTreeSet<String>) -> Saved {
    saved
        .iter()
        .filter(|(place, values)| {
            if named.contains(&place.id) || !runs.contains(*place) {
                return false;
            }
            let read = crate::project::selection::ports_read_by(project, place, runs);
            !read.is_empty() && read.iter().all(|port| values.contains_key(port))
        })
        .map(|(place, values)| (place.clone(), values.clone()))
        .collect()
}

/// What an infra's own containers tell weft changed of what its node
/// handed weft, with no node running, pushed to the agent beside their
/// unit (`weft_platform_traits::unit_agent::VALUES_PATH`). weft writes it
/// where the node put it: `connection` into the connection the node
/// published (a password a reset minted), `outputs` over its saved baked
/// outputs. Only what the node handed weft already can change.
// SYNC: PushedValues <-> catalog/postgres/database/images/credential/bootstrap.py (push_values)
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushedValues {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub connection: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_push_names_what_it_changes_and_nothing_else() {
        let pushed: PushedValues = serde_json::from_value(serde_json::json!({ "connection": { "password": "p2" } })).unwrap();
        assert_eq!(pushed.connection["password"], "p2");
        assert!(pushed.outputs.is_empty());
        assert_eq!(serde_json::to_value(&pushed).unwrap(), serde_json::json!({ "connection": { "password": "p2" } }));
        assert!(serde_json::from_value::<PushedValues>(serde_json::json!({ "values": {} })).is_err(), "an unknown key is refused");
    }
}

#[cfg(test)]
mod covered_tests {
    use super::*;
    use crate::infra::run_gate::InfraCopyUp;
    use serde_json::json;

    /// `cfg -> db.database`, `db.access -> query.access`, `trig.out ->
    /// query.q`, and `db.status -> watch.in`.
    fn program() -> ProjectDefinition {
        let node = |id: &str, inputs: &[&str], outputs: &[&str], extra: serde_json::Value| {
            let mut n = json!({
                "id": id, "nodeType": "T", "label": null, "config": {},
                "position": {"x": 0, "y": 0},
                "inputs": inputs.iter().map(|p| json!({"name": p, "portType": "String", "required": true})).collect::<Vec<_>>(),
                "outputs": outputs.iter().map(|p| json!({"name": p, "portType": "String", "required": true})).collect::<Vec<_>>(),
                "features": {}, "requiresInfra": false,
            });
            n.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            n
        };
        let nodes = vec![
            node("cfg", &[], &["out"], json!({})),
            node("db", &["database"], &["access", "status"], json!({"requiresInfra": true, "bakedOutputs": ["access"]})),
            node("trig", &[], &["out"], json!({"features": {"isTrigger": true}})),
            node("query", &["access", "q"], &["rows"], json!({})),
            node("watch", &["in"], &[], json!({})),
        ];
        let edges = [
            ("cfg", "out", "db", "database"),
            ("db", "access", "query", "access"),
            ("trig", "out", "query", "q"),
            ("db", "status", "watch", "in"),
        ];
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": nodes,
            "edges": edges.iter().map(|(a, ap, b, bp)| json!({
                "id": format!("{a}.{ap}->{b}.{bp}"), "source": a, "target": b, "sourceHandle": ap, "targetHandle": bp
            })).collect::<Vec<_>>(),
        }))
        .unwrap()
    }

    fn copy(running: bool, baked: &[(&str, serde_json::Value)]) -> InfraCopyUp {
        InfraCopyUp { node_id: "db".into(), instance: None, running, baked: baked.iter().map(|(k, v)| (k.to_string(), v.clone())).collect() }
    }

    fn places(names: &[&str]) -> BTreeSet<Located> {
        names.iter().map(|n| Located::top(*n)).collect()
    }

    #[test]
    fn only_a_running_copy_with_something_saved_counts() {
        let project = program();
        assert_eq!(saved_for_run(&project, &[copy(true, &[("access", json!("a1"))])], None)[&Located::top("db")]["access"], json!("a1"));
        assert!(saved_for_run(&project, &[copy(false, &[("access", json!("a1"))])], None).is_empty());
        assert!(saved_for_run(&project, &[copy(true, &[])], None).is_empty());
        // `access` is a String; a value saved before the type changed
        // does not fit, so it is no reason to skip the node.
        assert!(saved_for_run(&project, &[copy(true, &[("access", json!(7))])], None).is_empty());
    }

    /// A run that reaches a baked place without starting anywhere (a
    /// `--target`) skips the place and what only fed it.
    #[test]
    fn a_skipped_place_takes_what_only_fed_it_out_of_the_run() {
        let project = program();
        let saved = saved_for_run(&project, &[copy(true, &[("access", json!("a1"))])], None);
        let bounds = crate::project::selection::SelectionBounds { target: vec!["query".into()], ..Default::default() };
        let (selection, baked) = crate::run_spec::carve_reading_baked(&project, bounds, &saved, &BTreeSet::new()).unwrap();
        assert!(baked.contains_key(&Located::top("db")), "db is skipped");
        assert!(selection.suppliers.contains(&Located::top("db")));
        assert!(!selection.nodes.contains(&Located::top("db")));
        assert!(!selection.nodes.contains(&Located::top("cfg")), "cfg only fed db: {:?}", selection.nodes);
        assert!(selection.nodes.contains(&Located::top("query")) && selection.nodes.contains(&Located::top("trig")));
    }

    #[test]
    fn a_place_is_covered_only_when_this_run_reads_nothing_but_its_saved_outputs() {
        let project = program();
        let saved = saved_for_run(&project, &[copy(true, &[("access", json!("a1"))])], None);
        let none = BTreeSet::new();
        // The fire reaches query, which reads only db.access: covered.
        assert!(covered(&project, &saved, &places(&["trig", "query", "db", "cfg"]), &none).contains_key(&Located::top("db")));
        // A run that also runs watch reads db.status, which is not baked.
        assert!(covered(&project, &saved, &places(&["trig", "query", "db", "cfg", "watch"]), &none).is_empty());
        // A place the run was asked to run runs.
        let named = BTreeSet::from(["db".to_string()]);
        assert!(covered(&project, &saved, &places(&["trig", "query", "db", "cfg"]), &named).is_empty());
        // Nothing in the run reads it: it runs as asked.
        assert!(covered(&project, &saved, &places(&["db", "cfg"]), &none).is_empty());
    }
}

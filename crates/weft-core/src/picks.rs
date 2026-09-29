//! The connections a program's own access nodes use, picked on each
//! install rather than written in the source.
//!
//! A connection lives in one install's store, so a connection's id means
//! nothing on another install: a pick written into the source worked on
//! the machine that made it and nowhere else. So the source never holds
//! one. The compiler marks every access node's connection field that is
//! neither wired nor `@member_filled` as picked on the install
//! ([`install_picked_literal`]); each install keeps what was picked for it
//! (`weft connect`, the editor's Connect button, with `--on <target>` for
//! another install) beside the values members give, with the program as
//! the owner; and a run carries the picks it was born with, which the
//! engine puts where the marker stands, exactly as it does a member's
//! values.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::member::{MemberValues, PlaceValues};
use crate::project::{NodeDefinition, ProjectDefinition};
use crate::run_spec::Refusal;

/// The key the marker is lowered to, a structured value no string a
/// program writes can read as (the same reasoning as
/// [`crate::member::MEMBER_FILLED_KEY`]).
// SYNC: INSTALL_PICKED_KEY <-> packages/weft-graph/src/protocol.ts INSTALL_PICKED_KEY
pub const INSTALL_PICKED_KEY: &str = "__weft_install_picked__";

/// Where a connection pick written in the source is explained, for the
/// refusal that meets one.
pub const PICKS_DOC: &str = "https://weavemindai.github.io/weft/build/connections.html#picked-on-each-install";

/// The literal a field picked on the install is compiled to.
pub fn install_picked_literal() -> Value {
    serde_json::json!({ INSTALL_PICKED_KEY: {} })
}

/// Whether `value` is [`install_picked_literal`].
pub fn is_install_picked(value: &Value) -> bool {
    matches!(value, Value::Object(map) if map.len() == 1 && map.get(INSTALL_PICKED_KEY).is_some_and(Value::is_object))
}

/// Whether a written literal names no value yet: a field a member fills
/// or one picked on the install. What every reader that must not judge
/// the marker itself (readiness, the rules, the port-type check) asks.
pub fn fills_later(value: &Value) -> bool {
    crate::member::as_member_filled(value).is_some() || is_install_picked(value)
}

/// The fields of `node` picked on the install.
pub fn picked_fields(node: &NodeDefinition) -> impl Iterator<Item = &str> {
    node.port_literals.iter().filter(|(_, value)| is_install_picked(value)).map(|(field, _)| field.as_str())
}

/// The picks a run carries: place (`slack`, `one.slack`) -> field -> the
/// connection handle (`{id, identity}`), read when the run is born.
pub type Picks = MemberValues;

/// Put the install's picks into `literals` (the constants delivered to
/// one firing of `node` at one place): each picked field takes its
/// handle, or is removed, so an optional connection reads as none and a
/// required one was refused before the run was born ([`run_picks`]).
pub fn fill_picks(node: &NodeDefinition, literals: &mut serde_json::Map<String, Value>, picks: Option<&PlaceValues>) {
    for field in picked_fields(node) {
        match picks.and_then(|p| p.get(field)) {
            Some(handle) => {
                literals.insert(field.to_string(), handle.clone());
            }
            None => {
                literals.remove(field);
            }
        }
    }
}

/// Every place of a node with a field picked on the install, spelled.
pub fn picked_places(project: &ProjectDefinition) -> Vec<(String, &NodeDefinition)> {
    crate::project::selection::every_place(project)
        .into_iter()
        .filter_map(|place| {
            let node = project.nodes.iter().find(|n| n.id == place.id)?;
            picked_fields(node).next()?;
            Some((crate::project::address_of(project, &place.id, &place.path), node))
        })
        .collect()
}

/// The service a picked field connects to, off its widget (the compiler
/// stamps it).
fn service_of<'a>(node: &'a NodeDefinition, field: &str) -> Option<&'a str> {
    match node.inputs.iter().find(|i| i.name == field).and_then(|i| i.widget.as_ref()) {
        Some(crate::node::Widget::Access { service: Some(service), .. }) => Some(service),
        _ => None,
    }
}

/// Whether a run can go without a pick for `field` (its node's recipe
/// declares the connection optional).
fn optional(node: &NodeDefinition, field: &str) -> bool {
    matches!(
        node.inputs.iter().find(|i| i.name == field).and_then(|i| i.widget.as_ref()),
        Some(crate::node::Widget::Access { optional: true, .. })
    )
}

/// What a run over `selection` carries of the install's `stored` picks:
/// the ones at its places, or the refusal naming every required
/// connection nobody picked on this install, with the command that
/// picks it.
pub fn run_picks(
    project: &ProjectDefinition,
    selection: &crate::project::selection::RunSelection,
    stored: &Picks,
) -> Result<Picks, Refusal> {
    let mut picks = Picks::new();
    let mut refusal = Refusal { errors: Vec::new() };
    let mut seen = BTreeSet::new();
    for place in &selection.nodes {
        let Some(node) = project.nodes.iter().find(|n| n.id == place.id) else { continue };
        if picked_fields(node).next().is_none() {
            continue;
        }
        let spelled = crate::project::address_of(project, &place.id, &place.path);
        if !seen.insert(spelled.clone()) {
            continue;
        }
        let mut kept = PlaceValues::new();
        for field in picked_fields(node) {
            match stored.get(&spelled).and_then(|fields| fields.get(field)) {
                Some(handle) => {
                    kept.insert(field.to_string(), handle.clone());
                }
                None if optional(node, field) => {}
                None => refusal.errors.push(format!(
                    "'{spelled}' has no {} connection picked on this install; pick one with \
                     `weft connect --node {spelled}` (add `--on <target>` for another install)",
                    service_of(node, field).unwrap_or("service")
                )),
            }
        }
        if !kept.is_empty() {
            picks.insert(spelled, kept);
        }
    }
    if refusal.is_empty() { Ok(picks) } else { Err(refusal) }
}

/// One pick the author asks the install to keep: the connection `connection`
/// for `field` of the step at `step` (its place, spelled), a field that
/// connects to `service`.
// SYNC: PickInput <-> packages/weft-graph/src/protocol.ts PickInput
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PickInput {
    pub step: String,
    pub field: String,
    pub connection: uuid::Uuid,
    pub service: String,
}

/// Body for `PUT /projects/{id}/picks`: pick and forget, all or none.
/// What the dispatcher reads and `weft connect` writes.
// SYNC: ChangePicks <-> packages/weft-graph/src/protocol.ts ChangePicks
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePicks {
    #[serde(default)]
    pub set: Vec<PickInput>,
    #[serde(default)]
    pub clear: Vec<crate::run_spec::MemberFieldRef>,
}

/// A [`PickInput`] the program accepts, with the service its field
/// connects to (the store holds the connection to it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedPick {
    pub step: String,
    pub field: String,
    pub connection: uuid::Uuid,
    pub service: String,
}

/// Hold picks to the program the install last built (`project`, none
/// before the first build) before anything is stored. A pick for a place that program has must be for a
/// field picked on the install there, to its service. A place it does not
/// have yet is kept as asked: connecting comes before the first build as
/// often as after it, and a pick the program never reads stays stored and
/// unused, as a member's value for a field no longer filled does. Whether
/// the connection is the author's and of that service is the store's to
/// check, where the connection is. All or nothing: the refusal names
/// every pick refused.
pub fn check_picks(project: Option<&ProjectDefinition>, inputs: &[PickInput]) -> Result<Vec<CheckedPick>, Refusal> {
    let known: BTreeMap<String, &NodeDefinition> = project
        .into_iter()
        .flat_map(|project| {
            crate::project::selection::every_place(project).into_iter().filter_map(move |place| {
                let node = project.nodes.iter().find(|n| n.id == place.id)?;
                Some((crate::project::address_of(project, &place.id, &place.path), node))
            })
        })
        .collect();
    let mut refusal = Refusal { errors: Vec::new() };
    let mut checked = Vec::new();
    for input in inputs {
        if let Some(node) = known.get(&input.step) {
            if !picked_fields(node).any(|field| field == input.field) {
                refusal.errors.push(format!("'{}.{}' is no connection picked on the install", input.step, input.field));
                continue;
            }
            if service_of(node, &input.field) != Some(input.service.as_str()) {
                refusal.errors.push(format!(
                    "'{}.{}' connects to '{}', not '{}'",
                    input.step,
                    input.field,
                    service_of(node, &input.field).unwrap_or("no service"),
                    input.service
                ));
                continue;
            }
        }
        checked.push(CheckedPick {
            step: input.step.clone(),
            field: input.field.clone(),
            connection: input.connection,
            service: input.service.clone(),
        });
    }
    if refusal.is_empty() { Ok(checked) } else { Err(refusal) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn access_node(id: &str, optional: bool) -> NodeDefinition {
        let mut node: NodeDefinition = serde_json::from_value(json!({
            "id": id, "nodeType": "SlackAccess", "position": { "x": 0, "y": 0 },
        }))
        .unwrap();
        node.inputs.push(
            serde_json::from_value(json!({
                "name": "access", "portType": "Access", "required": true,
            }))
            .unwrap(),
        );
        node.inputs[0].widget = Some(crate::node::Widget::Access { service: Some("slack".into()), optional });
        node.port_literals.insert("access".into(), install_picked_literal());
        node
    }

    fn project(nodes: Vec<NodeDefinition>) -> ProjectDefinition {
        serde_json::from_value(json!({ "id": uuid::Uuid::nil(), "nodes": nodes, "edges": [] })).unwrap()
    }

    #[test]
    fn the_marker_is_told_apart_from_every_written_value() {
        assert!(is_install_picked(&install_picked_literal()));
        assert!(fills_later(&install_picked_literal()));
        assert!(!is_install_picked(&json!({ "id": "x" })));
        assert!(!is_install_picked(&json!({ INSTALL_PICKED_KEY: "text", "other": 1 })));
    }

    #[test]
    fn a_run_carries_its_picks_and_is_refused_a_required_one_missing() {
        let p = project(vec![access_node("slack", false), access_node("maybe", true)]);
        let all = crate::project::selection::RunSelection::whole(&p);
        let handle = json!({ "id": uuid::Uuid::nil(), "identity": "me" });
        let stored: Picks = BTreeMap::from([("slack".into(), BTreeMap::from([("access".into(), handle.clone())]))]);
        assert_eq!(run_picks(&p, &all, &stored).unwrap()["slack"]["access"], handle);
        let refused = run_picks(&p, &all, &Picks::new()).unwrap_err();
        assert_eq!(refused.errors.len(), 1, "the optional one is not required: {refused:?}");
        assert!(refused.errors[0].contains("weft connect --node slack"), "{refused:?}");
    }

    #[test]
    fn filling_puts_the_handle_in_or_takes_the_marker_out() {
        let node = access_node("slack", true);
        let mut literals: serde_json::Map<String, Value> = node.port_literals.clone().into_iter().collect();
        fill_picks(&node, &mut literals, None);
        assert!(!literals.contains_key("access"));
        let handle = json!({ "id": uuid::Uuid::nil() });
        let mut literals: serde_json::Map<String, Value> = node.port_literals.clone().into_iter().collect();
        fill_picks(&node, &mut literals, Some(&BTreeMap::from([("access".into(), handle.clone())])));
        assert_eq!(literals["access"], handle);
    }

    #[test]
    fn a_pick_is_held_to_the_program_where_it_has_the_place() {
        let p = project(vec![access_node("slack", false)]);
        let pick = |step: &str, field: &str, service: &str| PickInput {
            step: step.into(),
            field: field.into(),
            connection: uuid::Uuid::nil(),
            service: service.into(),
        };
        assert_eq!(check_picks(Some(&p), &[pick("slack", "access", "slack")]).unwrap()[0].service, "slack");
        assert!(check_picks(Some(&p), &[pick("slack", "x", "slack")]).is_err(), "no such picked field");
        assert!(check_picks(Some(&p), &[pick("slack", "access", "github")]).is_err(), "another service");
        assert!(check_picks(Some(&p), &[pick("later", "access", "slack")]).is_ok(), "a place not built yet is kept");
        assert!(check_picks(None, &[pick("slack", "anything", "slack")]).is_ok(), "nothing built yet, everything is kept");
    }
}

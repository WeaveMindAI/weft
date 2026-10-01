//! The connections a program's own access nodes use, picked on each
//! install rather than written in the source.
//!
//! A connection lives in one install's store, so a connection's id means
//! nothing on another install: a pick written into the source worked on
//! the machine that made it and nowhere else. So the source never holds
//! one. The compiler marks every access node's connection field that is
//! neither wired nor `@instance_filled` as picked on the install
//! ([`install_picked_literal`]); each install keeps what was picked for it
//! (`weft connect`, the editor's Connect button, with `--on <target>` for
//! another install) beside the values instances are given, with the
//! program as the owner; and a run carries the picks it was born with,
//! which the engine puts where the marker stands, exactly as it does an
//! instance's values.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::instance::{InstanceValues, PlaceValues};
use crate::project::{NodeDefinition, ProjectDefinition};
use crate::run_spec::Refusal;

/// The key the marker is lowered to, a structured value no string a
/// program writes can read as (the same reasoning as
/// [`crate::instance::INSTANCE_FILLED_KEY`]).
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

/// Whether a written literal names no value yet: a field an instance
/// fills or one picked on the install. What every reader that must not
/// judge the marker itself (readiness, the rules, the port-type check) asks.
pub fn fills_later(value: &Value) -> bool {
    crate::instance::as_instance_filled(value).is_some() || is_install_picked(value)
}

/// The fields of `node` picked on the install.
pub fn picked_fields(node: &NodeDefinition) -> impl Iterator<Item = &str> {
    node.port_literals.iter().filter(|(_, value)| is_install_picked(value)).map(|(field, _)| field.as_str())
}

/// The picks a run carries: place (`slack`, `one.slack`) -> field -> the
/// connection handle (`{id, identity}`), read when the run is born.
pub type Picks = InstanceValues;

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
    pub clear: Vec<crate::run_spec::InstanceFieldRef>,
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
/// unused, as an instance's value for a field no longer filled does. Whether
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

/// One thing the install keeps at a place, read back to check it against
/// the program: a pick (`instance: None`) or one instance's value for a
/// field written `@instance_filled`. `service` is the connection's
/// service when the value is a connection, else `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredField {
    pub step: String,
    pub field: String,
    pub service: Option<String>,
    pub instance: Option<crate::instance::InstanceId>,
}

/// A place the install keeps picks or values for that the program no
/// longer has, and the places it has now that could be where that node
/// went (best guess first): each has the same fields, to the same
/// services, and nothing stored for them yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stranded {
    pub step: String,
    pub candidates: Vec<String>,
}

impl Stranded {
    /// What a person reads: both addresses and the command that moves
    /// the picks across.
    pub fn message(&self) -> String {
        let first = &self.candidates[0];
        let others = if self.candidates.len() > 1 {
            format!(" (or {})", self.candidates[1..].iter().map(|c| format!("'{c}'")).collect::<Vec<_>>().join(", "))
        } else {
            String::new()
        };
        format!(
            "'{}' has connections or values stored on this install and is no longer in the program; \
             did you move it to '{first}'{others}? `weft connect --move {} {first}` carries them across",
            self.step, self.step
        )
    }
}

/// Every place of `project`, spelled, with its node.
fn places_of(project: &ProjectDefinition) -> BTreeMap<String, &NodeDefinition> {
    crate::project::selection::every_place(project)
        .into_iter()
        .filter_map(|place| {
            let node = project.nodes.iter().find(|n| n.id == place.id)?;
            Some((crate::project::address_of(project, &place.id, &place.path), node))
        })
        .collect()
}

/// Whether `node` can hold `stored`: the same field, filled the same way
/// (picked on the install for a pick, `@instance_filled` for an
/// instance's value), connecting to the same service when it is a
/// connection.
fn holds(node: &NodeDefinition, stored: &StoredField) -> bool {
    let filled_the_same_way = match stored.instance {
        None => picked_fields(node).any(|f| f == stored.field),
        Some(_) => crate::instance::instance_filled_fields(node).any(|(f, _)| f == stored.field),
    };
    filled_the_same_way && stored.service.as_deref().is_none_or(|service| service_of(node, &stored.field) == Some(service))
}

/// How many dotted segments two addresses share at the end: a node moved
/// into a folder keeps its own name and the names under it.
fn shared_tail(a: &str, b: &str) -> usize {
    a.rsplit('.').zip(b.rsplit('.')).take_while(|(x, y)| x == y).count()
}

/// The places `stored` keeps things for that `project` no longer has, each
/// with the places that could have taken its node over (see [`Stranded`]).
/// A place with no such candidate is left out: its node is gone, and what
/// it kept stays stored and unused, the same as a pick made before the
/// first build.
pub fn stranded(project: &ProjectDefinition, stored: &[StoredField]) -> Vec<Stranded> {
    let places = places_of(project);
    let taken: BTreeSet<(&str, &str)> = stored.iter().map(|s| (s.step.as_str(), s.field.as_str())).collect();
    let mut by_step: BTreeMap<&str, Vec<&StoredField>> = BTreeMap::new();
    for s in stored.iter().filter(|s| !places.contains_key(&s.step)) {
        by_step.entry(s.step.as_str()).or_default().push(s);
    }
    by_step
        .into_iter()
        .filter_map(|(step, fields)| {
            let mut candidates: Vec<&String> = places
                .iter()
                .filter(|(place, node)| {
                    fields.iter().all(|s| holds(node, s) && !taken.contains(&(place.as_str(), s.field.as_str())))
                })
                .map(|(place, _)| place)
                .collect();
            if candidates.is_empty() {
                return None;
            }
            candidates.sort_by_key(|place| std::cmp::Reverse(shared_tail(step, place)));
            Some(Stranded { step: step.to_string(), candidates: candidates.into_iter().cloned().collect() })
        })
        .collect()
}

/// What an activation checks of the install's picks before any trigger
/// moves: every connection the program needs picked on the install is
/// picked (an instance's own fields are the instance's to fill, per call),
/// and nothing is stored under a place the program moved away from. The
/// refusal names every gap, each with its fix.
pub fn activation_picks(project: &ProjectDefinition, picks: &Picks, stored: &[StoredField]) -> Result<(), Refusal> {
    let mut refusal = match run_picks(project, &crate::project::selection::RunSelection::whole(project), picks) {
        Ok(_) => Refusal { errors: Vec::new() },
        Err(refusal) => refusal,
    };
    refusal.errors.extend(stranded(project, stored).iter().map(Stranded::message));
    if refusal.is_empty() { Ok(()) } else { Err(refusal) }
}

/// Body for `POST /projects/{id}/picks/move`: carry everything the install
/// keeps at the place `from` (its picks and every instance's values) to
/// the place `to`. What `weft connect --move` sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovePicks {
    pub from: String,
    pub to: String,
}

/// What a move carried, and the triggers it set up again.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PicksMoved {
    pub picks: u64,
    pub instance_values: u64,
    pub rearmed: Vec<String>,
}

/// Hold a move to the program and to what is stored: `to` is a place of
/// the program with nothing stored yet, `from` has something stored, and
/// the node at `to` can hold every field kept at `from` (the same field,
/// filled the same way, to the same service). A move never merges and
/// never happens on its own: the person names both places.
pub fn check_move(project: &ProjectDefinition, stored: &[StoredField], request: &MovePicks) -> Result<(), String> {
    let MovePicks { from, to } = request;
    if from == to {
        return Err(format!("'{from}' is both where the picks are and where they would go"));
    }
    let places = places_of(project);
    let Some(node) = places.get(to) else {
        return Err(format!(
            "'{to}' is no place of the program this install last built; build the program with the \
             node there first, and name it the way the program spells it (`one.step` inside an included file)"
        ));
    };
    let moving: Vec<&StoredField> = stored.iter().filter(|s| &s.step == from).collect();
    if moving.is_empty() {
        return Err(format!("nothing is stored at '{from}' on this install"));
    }
    if stored.iter().any(|s| &s.step == to) {
        return Err(format!(
            "'{to}' already has connections or values stored; a move never merges, so clear them first \
             (`weft connect --node {to} --disconnect`, and `--instance <id>` for an instance's)"
        ));
    }
    let misfits: BTreeSet<&str> = moving.iter().filter(|s| !holds(node, s)).map(|s| s.field.as_str()).collect();
    if !misfits.is_empty() {
        return Err(format!(
            "'{to}' is a {} that cannot take what '{from}' keeps ({}): not the same field, filled the same \
             way, to the same service, so it is not the same kind of node",
            node.node_type,
            misfits.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(())
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

    fn instance_filled_node(id: &str) -> NodeDefinition {
        let mut node = access_node(id, false);
        node.port_literals.insert("access".into(), crate::instance::instance_filled_literal(None));
        node
    }

    fn kept(step: &str, service: Option<&str>, instance: Option<&str>) -> StoredField {
        StoredField {
            step: step.into(),
            field: "access".into(),
            service: service.map(Into::into),
            instance: instance.map(|i| crate::instance::InstanceId::new(i).unwrap()),
        }
    }

    #[test]
    fn a_pick_left_behind_by_a_moved_node_names_where_it_went() {
        let p = project(vec![access_node("studio_agent", false), access_node("agent", false)]);
        // `agent` has its pick; `studio_agent` lacks one and could take the old one.
        let stored = [kept("old.agent", Some("slack"), None), kept("agent", Some("slack"), None)];
        let found = stranded(&p, &stored);
        assert_eq!(found, vec![Stranded { step: "old.agent".into(), candidates: vec!["studio_agent".into()] }]);
        assert!(found[0].message().contains("weft connect --move old.agent studio_agent"), "{}", found[0].message());
    }

    #[test]
    fn the_candidate_sharing_the_longest_tail_comes_first() {
        let p = project(vec![access_node("x", false), access_node("agent", false)]);
        let found = stranded(&p, &[kept("chat.agent", Some("slack"), None)]);
        assert_eq!(found[0].candidates, vec!["agent".to_string(), "x".to_string()]);
    }

    #[test]
    fn a_place_nothing_could_have_taken_over_is_not_stranded() {
        let p = project(vec![access_node("agent", false)]);
        assert!(stranded(&p, &[kept("gone", Some("github"), None)]).is_empty(), "another service");
        assert!(stranded(&p, &[kept("gone", Some("slack"), Some("m"))]).is_empty(), "an instance value needs @instance_filled");
        let with_values = project(vec![instance_filled_node("agent")]);
        let found = stranded(&with_values, &[kept("gone", Some("slack"), Some("m"))]);
        assert_eq!(found[0].candidates, vec!["agent".to_string()]);
        let taken = [kept("gone", Some("slack"), Some("m")), kept("agent", Some("slack"), Some("n"))];
        assert!(stranded(&with_values, &taken).is_empty(), "another instance already filled the new place");
    }

    #[test]
    fn activation_names_the_missing_pick_and_where_it_was_left() {
        let p = project(vec![access_node("studio_agent", false)]);
        let refused = activation_picks(&p, &Picks::new(), &[kept("agent", Some("slack"), None)]).unwrap_err();
        assert_eq!(refused.errors.len(), 2, "{refused:?}");
        assert!(refused.errors[0].contains("weft connect --node studio_agent"), "{refused:?}");
        assert!(refused.errors[1].contains("weft connect --move agent studio_agent"), "{refused:?}");
        let handle = json!({ "id": uuid::Uuid::nil() });
        let picks: Picks = BTreeMap::from([("studio_agent".into(), BTreeMap::from([("access".into(), handle)]))]);
        assert!(activation_picks(&p, &picks, &[kept("studio_agent", Some("slack"), None)]).is_ok());
    }

    #[test]
    fn a_move_is_held_to_the_program_and_to_what_is_stored() {
        let p = project(vec![access_node("to", false), instance_filled_node("filled")]);
        let ask = |from: &str, to: &str| MovePicks { from: from.into(), to: to.into() };
        let stored = [kept("from", Some("slack"), None)];
        assert!(check_move(&p, &stored, &ask("from", "to")).is_ok());
        assert!(check_move(&p, &stored, &ask("from", "nowhere")).unwrap_err().contains("no place"));
        assert!(check_move(&p, &stored, &ask("empty", "to")).unwrap_err().contains("nothing is stored"));
        let both = [kept("from", Some("slack"), None), kept("to", Some("slack"), None)];
        assert!(check_move(&p, &both, &ask("from", "to")).unwrap_err().contains("never merges"));
        assert!(check_move(&p, &stored, &ask("from", "filled")).unwrap_err().contains("not the same kind"));
        let other_service = [kept("from", Some("github"), None)];
        assert!(check_move(&p, &other_service, &ask("from", "to")).unwrap_err().contains("not the same kind"));
        let values = [kept("from", Some("slack"), Some("m")), kept("from", None, Some("n"))];
        assert!(check_move(&p, &values, &ask("from", "filled")).is_ok(), "every instance's values move together");
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

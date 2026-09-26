//! The declarative validation rules a node's metadata declares
//! (`validate: [{ when, then }]`), evaluated against one node of a
//! compiled program.
//!
//! ONE evaluator for every caller: the compiler checks a program with it,
//! and the dispatcher checks a member's values with it, on the node with
//! the values swapped in ([`crate::member::filled_node`]). A rule can
//! never mean one thing at compile time and another when a member fills
//! a field.
//!
//! A field written `@member_filled` with no fallback has a value nobody
//! knows yet. A condition that only asks whether a value is there answers
//! yes (the member will provide one, or the run is refused); a condition
//! about the value's content has no answer, and a rule whose answer
//! depends on it does not fire. It is asked again, with the value in,
//! when the member provides one.

use serde_json::Value;

use crate::node::{Condition, RunDirection, ValidationRule};
use crate::project::{NodeDefinition, ProjectDefinition};

/// Whether `rule` fires on `node`. Only a condition that is known to hold
/// fires a rule; one that depends on a value nobody knows yet does not.
/// `custom_outputs` are the output ports the source added beyond the
/// node type's own ([`custom_outputs`]).
pub fn fires(rule: &ValidationRule, node: &NodeDefinition, project: &ProjectDefinition, custom_outputs: &[String]) -> bool {
    evaluate(&rule.when, node, project, custom_outputs) == Some(true)
}

/// The message a fired rule shows: `{id}`, `{port}`, `{field}` and
/// `{custom_outputs}` replaced from the node.
pub fn message(rule: &ValidationRule, node: &NodeDefinition, custom_outputs: &[String]) -> String {
    let mut s = rule.then.message.replace("{id}", &node.id);
    if let Some(p) = &rule.then.port {
        s = s.replace("{port}", p);
    }
    if let Some(f) = &rule.then.field {
        s = s.replace("{field}", f);
    }
    if s.contains("{custom_outputs}") {
        s = s.replace("{custom_outputs}", &custom_outputs.join(", "));
    }
    s
}

/// The output ports of `node` its type does not declare (`declared`): the
/// ones the source added, written `-> (verdict: String)`.
pub fn custom_outputs<'a>(node: &NodeDefinition, declared: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let declared: Vec<&str> = declared.into_iter().collect();
    node.outputs.iter().map(|p| p.name.clone()).filter(|name| !declared.contains(&name.as_str())).collect()
}

/// What a field holds, as a rule sees it.
enum Written<'a> {
    Known(Option<&'a Value>),
    /// `@member_filled` with no fallback: there, with a value nobody knows.
    Unknown,
}

fn written<'a>(node: &'a NodeDefinition, field: &str) -> Written<'a> {
    match node.written_value(field) {
        Some(value) => match crate::member::as_member_filled(value) {
            Some(filled) => match filled.fallback {
                Some(fallback) => Written::Known(Some(fallback)),
                None => Written::Unknown,
            },
            None => Written::Known(Some(value)),
        },
        None => Written::Known(None),
    }
}

/// A condition's answer: `Some` when known, `None` when it depends on a
/// value nobody knows yet (see the module docs). The combinators are the
/// three-valued ones: `all` is false as soon as one part is, `any` true as
/// soon as one part is, and otherwise unknown when a part is.
pub fn evaluate(cond: &Condition, node: &NodeDefinition, project: &ProjectDefinition, custom_outputs: &[String]) -> Option<bool> {
    let about = |field: &str, test: &dyn Fn(Option<&Value>) -> bool| match written(node, field) {
        Written::Known(value) => Some(test(value)),
        Written::Unknown => None,
    };
    match cond {
        Condition::InputSatisfied { port } => Some(input_satisfied(node, project, port)),
        Condition::InputWired { port } => Some(has_incoming_edge(node, project, port)),
        Condition::OutputWired { port } => Some(has_outgoing_edge(node, project, port)),
        Condition::InputSourceType { port, equals } => {
            // Vacuously true if the port has no wired edges (use
            // `all(input_wired, input_source_type)` to require both).
            Some(
                project
                    .edges
                    .iter()
                    .filter(|e| e.target == node.id && e.target_handle.as_deref() == Some(port))
                    .filter_map(|e| project.nodes.iter().find(|n| n.id == e.source))
                    .all(|n| &n.node_type == equals),
            )
        }
        // A member-filled field is there: the member provides it, or the
        // run is refused before it starts.
        Condition::ConfigPresent { field } => match written(node, field) {
            Written::Known(value) => Some(value.is_some_and(|v| !v.is_null())),
            Written::Unknown => Some(true),
        },
        Condition::ConfigNonempty { field } => match written(node, field) {
            Written::Known(value) => Some(is_nonempty(value)),
            Written::Unknown => Some(true),
        },
        Condition::ConfigEquals { field, equals } => about(field, &|v| v == Some(equals)),
        Condition::ConfigInSet { field, values } => {
            about(field, &|v| v.and_then(Value::as_str).is_some_and(|s| values.iter().any(|v| v == s)))
        }
        // Absent/non-string field -> false (not satisfied), like every sibling
        // ConfigX condition. A malformed regex (a metadata-authoring bug) also
        // yields false: it can't match, so the condition fails CLOSED rather
        // than silently evaluating true and suppressing/forcing a diagnostic.
        Condition::ConfigMatches { field, regex } => about(field, &|v| {
            v.and_then(Value::as_str)
                .and_then(|s| regex::Regex::new(regex).ok().map(|r| r.is_match(s)))
                .unwrap_or(false)
        }),
        Condition::RunReaches { direction, types } => Some(run_reaches(node, project, *direction, types)),
        Condition::CustomOutputsDeclared {} => Some(!custom_outputs.is_empty()),
        Condition::All { of } => {
            let answers: Vec<Option<bool>> = of.iter().map(|c| evaluate(c, node, project, custom_outputs)).collect();
            if answers.contains(&Some(false)) {
                Some(false)
            } else if answers.contains(&None) {
                None
            } else {
                Some(true)
            }
        }
        Condition::Any { of } => {
            let answers: Vec<Option<bool>> = of.iter().map(|c| evaluate(c, node, project, custom_outputs)).collect();
            if answers.contains(&Some(true)) {
                Some(true)
            } else if answers.contains(&None) {
                None
            } else {
                Some(false)
            }
        }
        Condition::Not { of } => evaluate(of, node, project, custom_outputs).map(|b| !b),
    }
}

/// Whether `node` sits in a run with a node of one of `types`, looking
/// `direction` from it. The run is the same selection a fire computes
/// (`RunSelection::carve`: forward from the seed, then back for what
/// that needs), taken at every place the node runs (once per call
/// site for a node inside an included file). Downstream: every run
/// seeded at the node holds one of the types. Upstream: every place
/// of the node is in the run of some node of one of the types. A seed
/// that cannot be carved (a cut inside a loop) counts as not reaching,
/// so a malformed program never silences the rule.
fn run_reaches(node: &NodeDefinition, project: &ProjectDefinition, direction: RunDirection, types: &[String]) -> bool {
    use crate::frames::Located;
    use crate::project::selection::{every_place, RunSelection, SelectionBounds};
    let of_type = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && types.contains(&n.node_type));
    let program_of = |seed: &Located| -> Option<RunSelection> {
        let is_trigger = project.nodes.iter().any(|n| n.id == seed.id && n.features.is_trigger);
        let spelled = crate::project::address_of(project, &seed.id, &seed.path);
        let bounds = if is_trigger {
            SelectionBounds { fire: Some(spelled), ..Default::default() }
        } else {
            SelectionBounds { from: vec![spelled], ..Default::default() }
        };
        RunSelection::carve(project, &bounds).ok()
    };
    let places: Vec<Located> = every_place(project).into_iter().filter(|place| place.id == node.id).collect();
    match direction {
        RunDirection::Downstream => {
            places.iter().all(|place| program_of(place).is_some_and(|run| run.nodes.iter().any(of_type)))
        }
        RunDirection::Upstream => {
            let seeds: Vec<Located> = every_place(project).into_iter().filter(of_type).collect();
            places
                .iter()
                .all(|place| seeds.iter().any(|seed| program_of(seed).is_some_and(|run| run.nodes.contains(place))))
        }
    }
}

/// Port is "satisfied" if either (a) it has a wired incoming edge, or
/// (b) a written constant drives it (`port_literals`, where the enrich
/// normalization homes every port-driving value; a `@member_filled`
/// one counts, since the member provides it). This covers `Llm {
/// prompt: "hi" }` where prompt is provided by a literal rather than a
/// wire.
pub fn input_satisfied(node: &NodeDefinition, project: &ProjectDefinition, port: &str) -> bool {
    has_incoming_edge(node, project, port) || literal_fills(node, port)
}

/// Does a written constant fill the port: the runtime's own line on a
/// `null` (data on a nullable port, nothing anywhere else), so a port
/// this rule calls filled is one the firing sees filled.
pub fn literal_fills(node: &NodeDefinition, port: &str) -> bool {
    node.port_literals.get(port).is_some_and(|v| crate::exec::ready::literal_is_data(node, port, v))
}

pub fn has_incoming_edge(node: &NodeDefinition, project: &ProjectDefinition, port: &str) -> bool {
    project.edges.iter().any(|e| e.target == node.id && e.target_handle.as_deref() == Some(port))
}

pub fn has_outgoing_edge(node: &NodeDefinition, project: &ProjectDefinition, port: &str) -> bool {
    project.edges.iter().any(|e| e.source == node.id && e.source_handle.as_deref() == Some(port))
}

fn is_nonempty(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::String(s)) => !s.trim().is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
        Some(_) => true,
    }
}

#[cfg(test)]
#[path = "tests/rules_tests.rs"]
mod tests;

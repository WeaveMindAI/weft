//! The declarative validation rules a node's metadata declares
//! (`validate: [{ when, then }]`), evaluated against one node of a
//! compiled program.
//!
//! ONE evaluator for every caller: the compiler checks a program with it,
//! and the dispatcher checks an instance's values with it, on the node
//! with the values swapped in ([`crate::instance::filled_node`]). A rule
//! can never mean one thing at compile time and another when an instance
//! fills a field.
//!
//! A field written `@instance_filled` with no fallback has a value nobody
//! knows yet. A condition that only asks whether a value is there answers
//! yes (the instance will be given one, or the run is refused); a condition
//! about the value's content has no answer, and a rule whose answer
//! depends on it does not fire. It is asked again, with the value in,
//! when the instance is given one.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeSet, HashMap};

use serde_json::Value;

use crate::frames::Located;
use crate::node::{Condition, Named, NodeRole, NodeWith, RunDirection, ValidationRule};
use crate::project::graph::{GraphView, ProjectGraph};
use crate::project::{NodeDefinition, ProjectDefinition};

/// What every rule checked against one program shares: the program,
/// indexed once, and the runs carved from it. A run carved from a seed
/// is the same whichever node or rule asks, so each is carved at most
/// once however many rules ask about it. Build one per pass over a
/// program, and ask every rule of that pass through it.
pub struct RuleContext<'a> {
    graph: ProjectGraph<'a>,
    places: OnceCell<BTreeSet<Located>>,
    /// The nodes of the run seeded at each place asked about; `None`
    /// when that seed cannot be carved.
    runs: RefCell<HashMap<Located, Option<BTreeSet<Located>>>>,
}

impl<'a> RuleContext<'a> {
    pub fn new(project: &'a ProjectDefinition) -> Self {
        Self { graph: ProjectGraph::new(project), places: OnceCell::new(), runs: RefCell::new(HashMap::new()) }
    }

    pub fn project(&self) -> &ProjectDefinition {
        self.graph.project()
    }

    fn places(&self) -> &BTreeSet<Located> {
        self.places.get_or_init(|| crate::project::selection::every_place_in(&self.graph))
    }

    /// Whether the run seeded at `seed` (a fire when it is a trigger, a
    /// cut from it otherwise) holds a place `holds` accepts. A seed that
    /// cannot be carved answers false.
    fn run_holds(&self, seed: &Located, holds: impl Fn(&BTreeSet<Located>) -> bool) -> bool {
        use crate::project::selection::{RunSelection, SelectionBounds};
        if let Some(run) = self.runs.borrow().get(seed) {
            return run.as_ref().is_some_and(&holds);
        }
        let spelled = crate::project::address_of(self.project(), &seed.id, &seed.path);
        let bounds = if self.graph.is_trigger(&seed.id) {
            SelectionBounds { fire: Some(spelled), ..Default::default() }
        } else {
            SelectionBounds { from: vec![spelled], ..Default::default() }
        };
        let run = RunSelection::carve_in(&self.graph, &bounds).ok().map(|run| run.nodes);
        let answer = run.as_ref().is_some_and(&holds);
        self.runs.borrow_mut().insert(seed.clone(), run);
        answer
    }

    /// How many runs this context has carved.
    #[cfg(test)]
    pub(crate) fn carved(&self) -> usize {
        self.runs.borrow().len()
    }
}

/// Whether `rule` fires on `node`. Only a condition that is known to hold
/// fires a rule; one that depends on a value nobody knows yet does not.
/// `custom_outputs` are the output ports the source added beyond the
/// node type's own ([`custom_outputs`]).
pub fn fires(rule: &ValidationRule, node: &NodeDefinition, cx: &RuleContext, custom_outputs: &[String]) -> bool {
    evaluate(&rule.when, node, cx, custom_outputs) == Some(true)
}

/// The message a fired rule shows: `{id}`, `{port}`, `{field}` and
/// `{custom_outputs}` replaced from the node; `{per_instance_reason}`
/// with why it is per instance ("it reads 'blender'", "it sits inside
/// group 'work', which receives 'blender'"); `{names}` with the names
/// its `input_names` conditions refuse, each with the full spelling it
/// most likely meant when one exists (`'box' (did you mean
/// 'work.box'?)`); `{with}` with the nodes its
/// `downstream_of` conditions found.
pub fn message(rule: &ValidationRule, node: &NodeDefinition, cx: &RuleContext, custom_outputs: &[String]) -> String {
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
    if s.contains("{per_instance_reason}") {
        s = s.replace("{per_instance_reason}", &per_instance_reasons(node, cx).join(", and "));
    }
    if s.contains("{names}") || s.contains("{with}") {
        let (mut names, mut with_found) = (Vec::new(), Vec::new());
        collect_from_condition(&rule.when, node, cx, &mut names, &mut with_found);
        // Sorted, so the message reads the same whatever order the source
        // map iterates in (serde_json's key order depends on which crates
        // share the build), and so a name written twice is listed once.
        names.sort();
        names.dedup();
        with_found.sort();
        with_found.dedup();
        s = s.replace("{names}", &names.join(", ")).replace("{with}", &quoted(&with_found));
    }
    s
}

fn quoted(items: &[String]) -> String {
    items.iter().map(|i| format!("'{i}'")).collect::<Vec<_>>().join(", ")
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
    /// `@instance_filled` with no fallback, or a connection picked on the
    /// install (`crate::picks`): there, with a value nobody knows.
    Unknown,
}

fn written<'a>(node: &'a NodeDefinition, field: &str) -> Written<'a> {
    match node.written_value(field) {
        Some(value) if crate::picks::is_install_picked(value) => Written::Unknown,
        Some(value) => match crate::instance::as_instance_filled(value) {
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
pub fn evaluate(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, custom_outputs: &[String]) -> Option<bool> {
    let (project, graph) = (cx.project(), &cx.graph);
    let about = |field: &str, test: &dyn Fn(Option<&Value>) -> bool| match written(node, field) {
        Written::Known(value) => Some(test(value)),
        Written::Unknown => None,
    };
    match cond {
        Condition::InputSatisfied { port } => Some(input_satisfied(node, graph, port)),
        Condition::InputWired { port } => Some(has_incoming_edge(node, graph, port)),
        Condition::OutputWired { port } => Some(has_outgoing_edge(node, graph, port)),
        Condition::InputSourceType { port, equals } => {
            // Vacuously true if the port has no wired edges (use
            // `all(input_wired, input_source_type)` to require both).
            Some(
                graph
                    .edges_into(&node.id)
                    .filter(|e| e.target_handle.as_deref() == Some(port))
                    .filter_map(|e| graph.node(&e.source))
                    .all(|n| &n.node_type == equals),
            )
        }
        // An instance-filled field is there: the instance is given it, or the
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
        Condition::RunReaches { direction, with } => Some(run_reaches(node, cx, *direction, with)),
        Condition::DownstreamOf { with } => Some(!upstream_with(node, cx, with).is_empty()),
        Condition::PerInstance {} => Some(node.per_instance.is_some()),
        Condition::InputNames { port, names } => match written(node, port) {
            Written::Known(value) => Some(misnamed(value, project, *names).is_empty()),
            Written::Unknown => None,
        },
        Condition::CustomOutputsDeclared {} => Some(!custom_outputs.is_empty()),
        Condition::All { of } => settle(of, false, |c| evaluate(c, node, cx, custom_outputs)),
        Condition::Any { of } => settle(of, true, |c| evaluate(c, node, cx, custom_outputs)),
        Condition::Not { of } => evaluate(of, node, cx, custom_outputs).map(|b| !b),
    }
}

/// The three-valued `all` (`decisive` false) or `any` (`decisive` true):
/// the decisive answer as soon as one part gives it, the parts after it
/// left unasked; otherwise unknown when a part is, and the other answer
/// when none is.
fn settle(parts: &[Condition], decisive: bool, answer: impl Fn(&Condition) -> Option<bool>) -> Option<bool> {
    let mut unknown = false;
    for part in parts {
        match answer(part) {
            Some(b) if b == decisive => return Some(decisive),
            Some(_) => {}
            None => unknown = true,
        }
    }
    (!unknown).then_some(!decisive)
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
fn run_reaches(node: &NodeDefinition, cx: &RuleContext, direction: RunDirection, with: &NodeWith) -> bool {
    let of_type = |place: &Located| cx.graph.node(&place.id).is_some_and(|n| with.matches(&n.features));
    let places = cx.places();
    let mut here = places.iter().filter(|place| place.id == node.id);
    match direction {
        RunDirection::Downstream => here.all(|place| cx.run_holds(place, |run| run.iter().any(of_type))),
        RunDirection::Upstream => {
            let seeds: Vec<&Located> = places.iter().filter(|place| of_type(place)).collect();
            here.all(|place| seeds.iter().any(|seed| cx.run_holds(seed, |run| run.contains(place))))
        }
    }
}

/// Every node upstream of `node` along the wires (itself included when
/// a loop's wiring leads back to it), walked through the boundaries
/// flattening left, which are ordinary nodes: the program's backward
/// closure ([`crate::project::upstream_closure`]) from what feeds it.
fn upstream_of<'a>(node: &NodeDefinition, cx: &'a RuleContext) -> Vec<&'a NodeDefinition> {
    let feeders: Vec<String> = cx.graph.edges_into(&node.id).map(|e| e.source.clone()).collect();
    let seen = crate::project::upstream_closure(&cx.graph, &feeders);
    cx.project().nodes.iter().filter(|n| seen.contains(&n.id)).collect()
}

/// The nodes `with` a feature upstream of `node` (`downstream_of`), by id.
fn upstream_with(node: &NodeDefinition, cx: &RuleContext, with: &NodeWith) -> Vec<String> {
    upstream_of(node, cx).into_iter().filter(|n| with.matches(&n.features)).map(|n| n.id.clone()).collect()
}

/// Why `node` exists once per instance, one clause per node that made it
/// so (the ones marked `@per_instance` or holding an `@instance_filled`
/// field, among itself and everything upstream of it), each telling the
/// path the way it runs:
/// - "it reads 'blender'" when the value flows into `node` along wires,
///   through a group's port of the same name when there is one;
/// - "it sits inside group 'work', which receives 'blender'" when it only
///   reaches `node` because a group enclosing it takes the value in: a
///   group that receives a per-instance value on any port makes
///   everything reading any of its ports per instance, so `node` may
///   read none of that value itself;
/// - "it reads from group 'work', which receives 'blender'" the same way
///   through a group that does not enclose `node`.
fn per_instance_reasons(node: &NodeDefinition, cx: &RuleContext) -> Vec<String> {
    use crate::instance::PerInstance;
    let mut out: Vec<String> = Vec::new();
    match node.per_instance {
        Some(PerInstance::Marked) => out.push("it is marked `@per_instance`".to_string()),
        Some(PerInstance::Filled) => out.push("one of its own fields is `@instance_filled`".to_string()),
        _ => {}
    }
    let starts = |n: &NodeDefinition| matches!(n.per_instance, Some(PerInstance::Marked | PerInstance::Filled));
    let upstream = upstream_of(node, cx);
    let along_wires = reads_along_wires(node, cx);
    for origin in upstream.iter().filter(|n| starts(n) && n.id != node.id) {
        if along_wires.contains(origin.id.as_str()) {
            out.push(format!("it reads '{}'", origin.id));
            continue;
        }
        // The value reached `node` through a group boundary where it
        // changed ports: a group taking it in. The one enclosing `node`,
        // outermost first, is the truest name; any other on the way
        // otherwise.
        let mut receiving: Vec<&str> = upstream
            .iter()
            .filter(|b| upstream_of(b, cx).iter().any(|u| u.id == origin.id))
            .filter_map(|b| b.group_boundary.as_ref().map(|g| g.group_id.as_str()))
            .collect();
        receiving.sort();
        let clause = match node.scope.iter().find(|group| receiving.contains(&group.as_str())) {
            Some(group) => format!("it sits inside group '{group}', which receives '{}'", origin.id),
            None => match receiving.first() {
                Some(group) => format!("it reads from group '{group}', which receives '{}'", origin.id),
                None => format!("it reads '{}'", origin.id),
            },
        };
        out.push(clause);
    }
    out
}

/// The nodes whose value flows into `node` along wires, port to port,
/// from any place `node` runs at: the plain data walk
/// ([`crate::project::selection::upstream_by_wires`]), where an ordinary
/// node passes any input to every output and a group boundary passes
/// each port straight through, so only the input of the same name.
fn reads_along_wires<'a>(node: &NodeDefinition, cx: &'a RuleContext) -> BTreeSet<&'a str> {
    let here: Vec<Located> = cx.places().iter().filter(|place| place.id == node.id).cloned().collect();
    let reached = crate::project::selection::upstream_by_wires(&cx.graph, &here);
    reached.iter().filter_map(|place| cx.graph.node(&place.id)).map(|n| n.id.as_str()).collect()
}

/// The names in `value` (a String, each String of a list, or each key
/// of an object) that are not what `named` asks for (`input_names`).
fn misnamed(value: Option<&Value>, project: &ProjectDefinition, named: Named) -> Vec<String> {
    let names: Vec<&str> = match value {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        Some(Value::Object(map)) => map.keys().map(String::as_str).collect(),
        _ => Vec::new(),
    };
    let node_at = |spelled: &str| {
        let (id, _) = crate::project::resolve_address(project, spelled);
        project.nodes.iter().find(|n| n.id == id)
    };
    let fits = |spelled: &str| match named {
        Named::Node { role, per_instance } => {
            let Some(found) = node_at(spelled) else { return false };
            let role_fits = match role {
                None => true,
                Some(NodeRole::Infra) => crate::project::infra_place_spellings(project).contains(spelled),
                Some(NodeRole::Trigger) => found.features.is_trigger,
            };
            role_fits && per_instance.is_none_or(|wanted| found.per_instance.is_some() == wanted)
        }
        // `node.field` split at its last dot, the way the runtime reads it:
        // the node may be spelled through its call sites (`one.send.model`).
        Named::Field { instance_filled } => {
            let Some((step, field)) = spelled.rsplit_once('.') else { return false };
            let Some(found) = node_at(step).filter(|_| !field.is_empty()) else { return false };
            let has_field = found.inputs.iter().any(|i| i.name == field) || found.written_value(field).is_some();
            let filled = crate::instance::instance_filled_fields(found).any(|(f, _)| f == field);
            has_field && instance_filled.is_none_or(|wanted| filled == wanted)
        }
    };
    names.into_iter().filter(|name| !fits(name)).map(str::to_string).collect()
}

/// One refused name, quoted, with the spellings it most likely meant:
/// the places that fit and end in it, which is what a node inside a
/// group or an included file is called when written by its short name
/// (`box` for `work.box`). A field name is matched on its node part.
fn refused_name(name: &str, cx: &RuleContext, named: Named) -> String {
    let project = cx.project();
    let (step, field) = match named {
        Named::Field { .. } => match name.rsplit_once('.') {
            Some((step, field)) => (step, Some(field)),
            None => return format!("'{name}'"),
        },
        Named::Node { .. } => (name, None),
    };
    let suffix = format!(".{step}");
    let meant: BTreeSet<String> = cx
        .places()
        .iter()
        .map(|place| crate::project::address_of(project, &place.id, &place.path))
        .filter(|spelled| spelled.ends_with(&suffix))
        .map(|spelled| match field {
            Some(field) => format!("{spelled}.{field}"),
            None => spelled,
        })
        .filter(|candidate| misnamed(Some(&Value::String(candidate.clone())), project, named).is_empty())
        .collect();
    if meant.is_empty() {
        format!("'{name}'")
    } else {
        format!("'{name}' (did you mean {}?)", meant.iter().map(|m| format!("'{m}'")).collect::<Vec<_>>().join(" or "))
    }
}

/// Every name the rule's `input_names` conditions refuse on `node`
/// (`{names}`), and every node its `downstream_of` conditions found
/// (`{with}`), walked through the combinators.
fn collect_from_condition(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, names: &mut Vec<String>, with_found: &mut Vec<String>) {
    match cond {
        Condition::InputNames { port, names: named } => {
            if let Written::Known(value) = written(node, port) {
                names.extend(misnamed(value, cx.project(), *named).iter().map(|name| refused_name(name, cx, *named)));
            }
        }
        Condition::DownstreamOf { with } => with_found.extend(upstream_with(node, cx, with)),
        Condition::All { of } | Condition::Any { of } => {
            for c in of {
                collect_from_condition(c, node, cx, names, with_found);
            }
        }
        Condition::Not { of } => collect_from_condition(of, node, cx, names, with_found),
        _ => {}
    }
}

/// Port is "satisfied" if either (a) it has a wired incoming edge, or
/// (b) a written constant drives it (`port_literals`, where the enrich
/// normalization homes every port-driving value; an `@instance_filled`
/// one counts, since the instance is given it). This covers `Llm {
/// prompt: "hi" }` where prompt is provided by a literal rather than a
/// wire.
pub fn input_satisfied(node: &NodeDefinition, project: &impl GraphView, port: &str) -> bool {
    has_incoming_edge(node, project, port) || literal_fills(node, port)
}

/// Does a written constant fill the port: the runtime's own line on a
/// `null` (data on a nullable port, nothing anywhere else), so a port
/// this rule calls filled is one the firing sees filled.
pub fn literal_fills(node: &NodeDefinition, port: &str) -> bool {
    node.port_literals.get(port).is_some_and(|v| crate::exec::ready::literal_is_data(node, port, v))
}

pub fn has_incoming_edge(node: &NodeDefinition, project: &impl GraphView, port: &str) -> bool {
    project.edges_into(&node.id).any(|e| e.target_handle.as_deref() == Some(port))
}

pub fn has_outgoing_edge(node: &NodeDefinition, project: &impl GraphView, port: &str) -> bool {
    project.edges_out_of(&node.id).any(|e| e.source_handle.as_deref() == Some(port))
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

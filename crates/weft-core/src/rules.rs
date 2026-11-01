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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::Located;
use crate::node::{Condition, Named, NodeMetadata, NodeRole, NodeWith, RunDirection, ValidationRule};
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
    /// What each per-instance origin asked about reaches
    /// (`places::per_instance_reach`), walked once however many places
    /// ask why they are per instance.
    reaches: RefCell<HashMap<Located, std::rc::Rc<BTreeSet<Located>>>>,
}

impl<'a> RuleContext<'a> {
    pub fn new(project: &'a ProjectDefinition) -> Self {
        Self {
            graph: ProjectGraph::new(project),
            places: OnceCell::new(),
            runs: RefCell::new(HashMap::new()),
            reaches: RefCell::new(HashMap::new()),
        }
    }

    pub fn project(&self) -> &ProjectDefinition {
        self.graph.project()
    }

    fn places(&self) -> &BTreeSet<Located> {
        self.places.get_or_init(|| crate::project::places::every_place_in(&self.graph))
    }

    /// The places an instance value at `origin` reaches.
    fn reach_of(&self, origin: &Located) -> std::rc::Rc<BTreeSet<Located>> {
        if let Some(reach) = self.reaches.borrow().get(origin) {
            return reach.clone();
        }
        let reach = std::rc::Rc::new(crate::project::places::per_instance_reach(&self.graph, vec![origin.clone()]));
        self.reaches.borrow_mut().insert(origin.clone(), reach.clone());
        reach
    }

    /// Whether the run seeded at `seed` (a fire when it is a trigger, a
    /// cut from it otherwise) holds a place `holds` accepts. A seed that
    /// cannot be carved answers false.
    fn run_holds(&self, seed: &Located, holds: impl Fn(&BTreeSet<Located>) -> bool) -> bool {
        use crate::project::cut::SelectionBounds;
        if let Some(run) = self.runs.borrow().get(seed) {
            return run.as_ref().is_some_and(&holds);
        }
        let spelled = crate::project::address_of(self.project(), &seed.id, &seed.path);
        let bounds = if self.graph.is_trigger(&seed.id) {
            SelectionBounds { fire: Some(spelled), ..Default::default() }
        } else {
            SelectionBounds { from: vec![spelled], ..Default::default() }
        };
        let run = crate::project::cut::walk(&self.graph, &bounds).ok().map(|run| run.kept());
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
/// `custom` are the ports the source added beyond the node type's own
/// ([`CustomPorts::of`]).
pub fn fires(rule: &ValidationRule, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> bool {
    evaluate(&rule.when, node, cx, custom) == Some(true)
}

/// The message a fired rule shows: `{id}`, `{port}`, `{field}`,
/// `{customInputs}` and `{customOutputs}` replaced from the node; `{perInstanceReason}`
/// with why it is per instance ("it reads 'blender'", "it sits inside
/// group 'work', whose gate reads 'blender'"); `{names}` with the names
/// its `input_names` conditions refuse, each with the full spelling it
/// most likely meant when one exists (`'box' (did you mean
/// 'work.box'?)`), and the added inputs its `custom_inputs_fit`
/// conditions refuse; `{with}` with the nodes its
/// `downstream_of` conditions found.
pub fn message(rule: &ValidationRule, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> String {
    let mut s = rule.then.message.replace("{id}", &node.id);
    if let Some(p) = &rule.then.port {
        s = s.replace("{port}", p);
    }
    if let Some(f) = &rule.then.field {
        s = s.replace("{field}", f);
    }
    if s.contains("{customInputs}") {
        s = s.replace("{customInputs}", &custom.inputs.join(", "));
    }
    if s.contains("{customOutputs}") {
        s = s.replace("{customOutputs}", &custom.outputs.join(", "));
    }
    if s.contains("{perInstanceReason}") {
        s = s.replace("{perInstanceReason}", &per_instance_reasons(node, cx).join(", and "));
    }
    if s.contains("{names}") || s.contains("{with}") {
        let (mut names, mut with_found) = (Vec::new(), Vec::new());
        collect_from_condition(&rule.when, node, cx, custom, &mut names, &mut with_found);
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

/// The ports a program declared on a node beyond its type's own, in the
/// order it wrote them: what `custom_inputs_declared`,
/// `custom_inputs_fit`, `custom_outputs_declared` and the
/// `{customInputs}` / `{customOutputs}` message holes read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomPorts {
    /// Inputs written `ExecPython(photo: Image)` (`canAddInputPorts`):
    /// every input not from the node's spec, the same ones the running
    /// node lists as created (`ValueBag::created`). The language's gate
    /// ports (`_should_flow`) come from the spec, so they are never on it.
    #[serde(default, rename = "customInputs", skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    /// Outputs written `-> (verdict: String)` (`canAddOutputPorts`).
    #[serde(default, rename = "customOutputs", skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<String>,
}

impl CustomPorts {
    /// The ports of `node` its type's metadata `meta` does not declare.
    /// Read once enrich has merged the metadata's ports into the node.
    pub fn of(node: &NodeDefinition, meta: &NodeMetadata) -> Self {
        Self {
            inputs: node.inputs.iter().filter(|p| !p.from_spec).map(|p| p.name.clone()).collect(),
            outputs: node
                .outputs
                .iter()
                .map(|p| &p.name)
                .filter(|name| !meta.outputs.iter().any(|o| o.name == **name))
                .cloned()
                .collect(),
        }
    }
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
pub fn evaluate(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> Option<bool> {
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
        Condition::PerInstance {} => Some(node.per_instance_somewhere()),
        Condition::InputNames { port, names } => match written(node, port) {
            Written::Known(value) => Some(misnamed(value, project, *names).is_empty()),
            Written::Unknown => None,
        },
        Condition::CustomInputsDeclared {} => Some(!custom.inputs.is_empty()),
        Condition::CustomOutputsDeclared {} => Some(!custom.outputs.is_empty()),
        Condition::OutputTypeFits { port, within } => Some(
            node.outputs
                .iter()
                .find(|output| output.name == *port)
                .is_some_and(|output| crate::weft_type::WeftType::fits_within(&output.port_type, within)),
        ),
        Condition::CustomInputsFit { one_of } => Some(unfit_inputs(node, custom, one_of).is_empty()),
        Condition::All { of } => settle(of, false, |c| evaluate(c, node, cx, custom)),
        Condition::Any { of } => settle(of, true, |c| evaluate(c, node, cx, custom)),
        Condition::Not { of } => evaluate(of, node, cx, custom).map(|b| !b),
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
/// `direction` from it. The run is the same walk a fire computes
/// (`crate::project::cut::walk`: forward from the seed, then back for
/// what that needs), taken at every place the node runs (once per call
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

/// Every node upstream of `node` (itself included when a loop's wiring
/// leads back to it): the program's backward closure
/// ([`crate::project::upstream_closure`]) from what feeds it and from
/// the groups around it, whose gates decide whether it runs. By id, as a
/// rule is asked of one definition: what feeds any call of an included
/// file is upstream of that file's members, the answer for the call
/// where it holds.
fn upstream_of<'a>(node: &NodeDefinition, cx: &'a RuleContext) -> Vec<&'a NodeDefinition> {
    let feeders: Vec<String> = cx.graph.edges_into(&node.id)
        .map(|e| e.source.clone())
        .chain(node.scope.iter().cloned())
        .collect();
    let seen = crate::project::upstream_closure(&cx.graph, &feeders);
    cx.project().nodes.iter().filter(|n| seen.contains(&n.id)).collect()
}

/// The nodes `with` a feature upstream of `node` (`downstream_of`), by id.
fn upstream_with(node: &NodeDefinition, cx: &RuleContext, with: &NodeWith) -> Vec<String> {
    upstream_of(node, cx).into_iter().filter(|n| with.matches(&n.features)).map(|n| n.id.clone()).collect()
}

/// Why `node` exists once per instance, for a rule asked of the node as
/// a whole: the reasons at each place where it is per instance
/// ([`per_instance_reasons_at`]), each led by the place when the node runs
/// at more than one (a node inside a file included twice may be per
/// instance under one site only, and the reasons differ per site).
pub fn per_instance_reasons(node: &NodeDefinition, cx: &RuleContext) -> Vec<String> {
    let here: Vec<&Located> = cx.places().iter().filter(|place| place.id == node.id).collect();
    let mut out: Vec<String> = Vec::new();
    // Its own mark holds at every place, so it is said once, unled.
    let own = own_mark_reason(node);
    out.extend(own.clone());
    for place in here.iter().filter(|place| node.per_instance_at(&place.path).is_some()) {
        let lead = (here.len() > 1).then(|| format!("at '{}', ", crate::project::address_of(&cx.graph, &place.id, &place.path)));
        for reason in per_instance_reasons_at(place, cx).into_iter().filter(|reason| own.as_ref() != Some(reason)) {
            let reason = format!("{}{reason}", lead.as_deref().unwrap_or(""));
            if !out.contains(&reason) {
                out.push(reason);
            }
        }
    }
    out
}

/// The reason a node's own mark gives, when it carries one: it holds at
/// every place the node runs.
fn own_mark_reason(node: &NodeDefinition) -> Option<String> {
    match node.per_instance {
        Some(crate::instance::PerInstance::Marked) => Some("it is marked `@per_instance`".to_string()),
        Some(crate::instance::PerInstance::Filled) => Some("one of its own fields is `@instance_filled`".to_string()),
        _ => None,
    }
}

/// Why the node at `place` exists once per instance there, one clause per
/// node that made it so (the ones marked `@per_instance` or holding an
/// `@instance_filled` field, among itself and every place whose instance
/// value reaches this one, `places::per_instance_reach`), each telling the
/// path the way it runs:
/// - "it reads 'blender'" when the value flows into `place` along wires;
/// - "it sits inside group 'work', whose gate reads 'blender'" when the
///   value decides whether a group around `place` runs: the group runs
///   once per instance, and so does everything inside it;
/// - "it sits inside loop 'each', which runs on what 'blender' gives it"
///   when the value reaches what starts a loop around `place`.
///
/// Every walk is from `place` alone, so a site that feeds the same
/// included file an instance's value never gives a reason for another
/// site.
pub fn per_instance_reasons_at(place: &Located, cx: &RuleContext) -> Vec<String> {
    use crate::instance::PerInstance;
    use crate::project::places;
    let g = &cx.graph;
    let Some(node) = g.node(&place.id) else { return Vec::new() };
    let mut out: Vec<String> = own_mark_reason(node).into_iter().collect();
    let spelled = |at: &Located| crate::project::address_of(g, &at.id, &at.path);
    let along_wires = places::upstream_by_wires(g, std::slice::from_ref(place));
    let origins = cx.places().iter().filter(|at| {
        *at != place && g.node(&at.id).is_some_and(|n| matches!(n.per_instance, Some(PerInstance::Marked | PerInstance::Filled)))
    });
    for origin in origins {
        let reach = cx.reach_of(origin);
        if !reach.contains(place) {
            continue;
        }
        let reason = if along_wires.contains(origin) {
            format!("it reads '{}'", spelled(origin))
        } else {
            // Not along the wires: the value reaches a gate of a group
            // around `place`, or what starts a loop around it (its
            // `LoopIn` reads the list and the gate alike), outermost
            // first. Reached the way the mark is (a gate it opened on the
            // way counts), so a gate fed by something per instance only
            // through another gate still names the group.
            let gated = places::scopes_around(g, place).into_iter().find_map(|scope| {
                let is_loop = g.is_loop(&scope.id);
                let decides: Vec<Located> = if is_loop {
                    let loop_in = Located::new(crate::project::loop_in_id(&scope.id), scope.path.clone());
                    places::incoming(g, &loop_in).map(|(_, source)| source).collect()
                } else {
                    places::incoming(g, &scope)
                        .filter(|(edge, _)| crate::run::skip::is_gate_port(edge.target_port()))
                        .map(|(_, source)| source)
                        .collect()
                };
                decides.iter().any(|source| source == origin || reach.contains(source)).then_some((scope, is_loop))
            });
            match gated {
                Some((group, false)) => format!(
                    "it sits inside group '{}', whose gate reads '{}'",
                    crate::project::group_address(g, &group.id, &group.path),
                    spelled(origin)
                ),
                Some((group, true)) => format!(
                    "it sits inside loop '{}', which runs on what '{}' gives it",
                    crate::project::group_address(g, &group.id, &group.path),
                    spelled(origin)
                ),
                // Reached neither along the wires nor through a gate
                // around it: it reads something that runs per instance
                // because of `origin` (a node inside a group the value
                // gates), and the one it reads is what it is told. The
                // walk reaches a place only along a wire from a place it
                // reached, or by opening a scope around it, so such a
                // wire is always there.
                None => {
                    let source = places::incoming(g, place)
                        .map(|(_, source)| source)
                        .find(|source| reach.contains(source))
                        .expect("a place reached past every gate around it is reached along one of its own wires");
                    format!("it reads '{}'", spelled(&source))
                }
            }
        };
        if !out.contains(&reason) {
            out.push(reason);
        }
    }
    out
}

/// The inputs the source added to `node` whose declared type fits in
/// none of `one_of` (`custom_inputs_fit`), in the order it wrote them. A
/// type still open (a type variable) fits; an added input the node does
/// not carry does not.
fn unfit_inputs<'a>(node: &NodeDefinition, custom: &'a CustomPorts, one_of: &[crate::weft_type::WeftType]) -> Vec<&'a str> {
    custom
        .inputs
        .iter()
        .filter(|name| {
            !node.inputs.iter().find(|input| input.name == **name).is_some_and(|input| {
                one_of.iter().any(|allowed| crate::weft_type::WeftType::fits_within(&input.port_type, allowed))
            })
        })
        .map(String::as_str)
        .collect()
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
    let node_at = |spelled: &str| match crate::project::places::named(project, spelled) {
        Ok(crate::project::places::Named::Node(place)) => project.nodes.iter().find(|n| n.id == place.id).map(|n| (n, place)),
        _ => None,
    };
    let fits = |spelled: &str| match named {
        Named::Node { role, per_instance } => {
            let Some((found, place)) = node_at(spelled) else { return false };
            let role_fits = match role {
                None => true,
                Some(NodeRole::Infra) => crate::project::infra_place_spellings(project).contains_key(spelled),
                Some(NodeRole::Trigger) => found.features.is_trigger,
            };
            role_fits && per_instance.is_none_or(|wanted| found.per_instance_at(&place.path).is_some() == wanted)
        }
        // `node.field` split at its last dot, the way the runtime reads it:
        // the node may be spelled through its call sites (`one.send.model`).
        Named::Field { instance_filled } => {
            let Some((at, field)) = spelled.rsplit_once('.') else { return false };
            let Some((found, _)) = node_at(at).filter(|_| !field.is_empty()) else { return false };
            let has_field = found.inputs.iter().any(|i| i.name == field) || found.written_value(field).is_some();
            let filled = crate::instance::instance_filled_fields(found).any(|(f, _)| f == field);
            has_field && instance_filled.is_none_or(|wanted| filled == wanted)
        }
        Named::State {} => crate::project::places::state_place(project, spelled).is_ok(),
    };
    names.into_iter().filter(|name| !fits(name)).map(str::to_string).collect()
}

/// One refused name, quoted, with the spellings it most likely meant:
/// the places that fit and end in it, which is what a node inside a
/// group or an included file is called when written by its short name
/// (`box` for `work.box`). A field name is matched on its node part.
fn refused_name(name: &str, cx: &RuleContext, named: Named) -> String {
    let project = cx.project();
    let (at, field) = match named {
        Named::Field { .. } => match name.rsplit_once('.') {
            Some((at, field)) => (at, Some(field)),
            None => return format!("'{name}'"),
        },
        Named::Node { .. } | Named::State {} => (name, None),
    };
    let suffix = format!(".{at}");
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

/// Every name the rule's `input_names` conditions refuse on `node` and
/// every added input its `custom_inputs_fit` conditions refuse
/// (`{names}`), and every node its `downstream_of` conditions found
/// (`{with}`), walked through the combinators.
fn collect_from_condition(
    cond: &Condition,
    node: &NodeDefinition,
    cx: &RuleContext,
    custom: &CustomPorts,
    names: &mut Vec<String>,
    with_found: &mut Vec<String>,
) {
    match cond {
        Condition::InputNames { port, names: named } => {
            if let Written::Known(value) = written(node, port) {
                names.extend(misnamed(value, cx.project(), *named).iter().map(|name| refused_name(name, cx, *named)));
            }
        }
        Condition::CustomInputsFit { one_of } => {
            names.extend(unfit_inputs(node, custom, one_of).into_iter().map(|name| format!("'{name}'")));
        }
        Condition::DownstreamOf { with } => with_found.extend(upstream_with(node, cx, with)),
        Condition::All { of } | Condition::Any { of } => {
            for c in of {
                collect_from_condition(c, node, cx, custom, names, with_found);
            }
        }
        Condition::Not { of } => collect_from_condition(of, node, cx, custom, names, with_found),
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
    node.port_literals.get(port).is_some_and(|v| crate::run::ready::literal_is_data(node, port, v))
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

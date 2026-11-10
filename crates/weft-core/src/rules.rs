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
//!
//! A value that arrives on a wire is nobody's to know at compile time
//! either. A rule whose answer leans on one (a setting a wire fills, an
//! optional wired input that may close empty) is left undecided, recorded
//! on the node ([`Recheck`], folded down to the questions about values),
//! and asked again by the same evaluator before the node's body runs,
//! with the values that arrived ([`recheck`]). A field written
//! `@instance_filled(null)` reads the same way: an instance may leave it
//! empty without its run being refused, so even whether it is set is
//! known only when the node fires.
//!
//! Every condition answers yes, no or not yet known, and the combinators
//! fold those answers: one no settles an `all` and one yes an `any`;
//! otherwise an unknown part leaves the whole unknown, and what is left to
//! ask (the leftover) is the unknown parts alone. The compile records a
//! rule's leftover on the node, and the run answers only that.
//!
//! Which install the program is built for is read off the program
//! (`ProjectDefinition::install`, set by the install's own version build,
//! and by every command whose compile is held to an install's, such as
//! `weft status`). A compile for no install has no answer to a rule
//! asking about it (`local_install`), which then does not fire. The
//! install's answer is folded like any other, so a rule it settles is
//! never recorded for the run; a program compiled for an install never
//! records one still asking it.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::Located;
use crate::install::InstallKind;
use crate::node::{Condition, Named, NodeMetadata, NodeRole, NodeWith, Refusal, RunDirection, Unique, ValidationRule, ValueChecks, ValueList, WiredList};
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
    /// The rules of `project` as the program reads them: `local_install`
    /// answered with the install it was compiled for
    /// (`ProjectDefinition::install`), none for a compile for no install.
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
/// `downstream_of` conditions found; `{noneOfFound}` with the values its
/// `noneOf` checks refuse (`'Read', 'Task'`), and `{noneMatchFound}` the
/// ones its `noneMatch` checks refuse ([`Refusal::hole`]); `{wiredNode}`
/// with the nodes its `noneOf` and `noneMatch` checks read a list off,
/// each with its type (`ClaudeCode 'claude'`).
pub fn message(rule: &ValidationRule, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> String {
    let at = Reading::Program { cx, custom };
    let s = names_hole(holes_but_found(rule, node, cx, custom), &rule.when, node, &at, cx);
    Refusal::ALL.into_iter().fold(s, |s, refusal| found_hole(s, refusal, &rule.when, node, &at))
}

/// [`message`] with every hole filled but the ones naming what a value
/// holds (`{names}`, `{noneOfFound}`, `{noneMatchFound}`), which a
/// recorded rule fills from what arrives.
fn holes_but_found(rule: &ValidationRule, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> String {
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
    if s.contains("{with}") {
        let mut with_found = Vec::new();
        collect_with(&rule.when, node, cx, &mut with_found);
        // Sorted, so the message reads the same whatever order the source
        // map iterates in, and a node found twice is listed once.
        with_found.sort();
        with_found.dedup();
        s = s.replace("{with}", &quoted(&with_found));
    }
    if s.contains("{wiredNode}") {
        let mut listers = Vec::new();
        collect_listers(&rule.when, node, cx, &mut listers);
        listers.sort();
        listers.dedup();
        s = s.replace("{wiredNode}", &listers.join(", "));
    }
    s
}

/// `s` with `refusal`'s hole (`{noneOfFound}`, `{noneMatchFound}`)
/// filled from what `at` reads of `cond`'s checks of that kind: every
/// value one refuses, quoted, sorted, each once.
fn found_hole(s: String, refusal: Refusal, cond: &Condition, node: &NodeDefinition, at: &Reading) -> String {
    if !s.contains(refusal.hole()) {
        return s;
    }
    let mut found = Vec::new();
    collect_refused(refusal, cond, node, at, &mut found);
    let mut found: Vec<String> = found
        .into_iter()
        .map(|value| match value {
            Value::String(text) => format!("'{text}'"),
            other => other.to_string(),
        })
        .collect();
    found.sort();
    found.dedup();
    s.replace(refusal.hole(), &found.join(", "))
}

/// `s` with `{names}` filled from what `at` reads of `cond`'s
/// `input_names` and `custom_inputs_fit` conditions; `cx` is the program
/// a refused name is matched against for what it most likely meant.
fn names_hole(s: String, cond: &Condition, node: &NodeDefinition, at: &Reading, cx: &RuleContext) -> String {
    if !s.contains("{names}") {
        return s;
    }
    let mut names = Vec::new();
    collect_names(cond, node, at, cx, &mut names);
    // Sorted, so the message reads the same whatever order the source
    // map iterates in (serde_json's key order depends on which crates
    // share the build), and so a name written twice is listed once.
    names.sort();
    names.dedup();
    s.replace("{names}", &names.join(", "))
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
    /// install (`crate::picks`): there, with a value nobody knows. Also a
    /// file's content a build named (`crate::content`): there, and read
    /// only by the worker that runs it (the compile ruled on the content
    /// itself before naming it).
    Unknown,
    /// Known only when the node fires: fed by a wire, or an instance's
    /// value the instance may leave empty (`@instance_filled(null)`).
    /// `may_be_absent` when the node can fire with nothing there (an
    /// optional input whose wire closed empty, a blank text where a blank
    /// is no value, an instance's value left empty), so even whether it is
    /// set is unknown.
    Arrives { may_be_absent: bool },
}

/// The values that arrived at one firing, as the run-time recheck reads
/// them ([`recheck`]): the node's input bag, by input name.
pub trait Arrived {
    fn value(&self, name: &str) -> Option<&Value>;
}

impl Arrived for serde_json::Map<String, Value> {
    fn value(&self, name: &str) -> Option<&Value> {
        self.get(name)
    }
}

/// Where a rule reads what it asks about.
enum Reading<'a> {
    /// The program as compiled: its graph, and the values its source
    /// wrote. What the compiler and an instance's check read.
    Program { cx: &'a RuleContext<'a>, custom: &'a CustomPorts },
    /// The values that arrived at one firing of the node, in the program
    /// it runs in: what a [`Recheck`] reads. A question about the graph
    /// has no answer here, and never needs one: the compile answered it,
    /// and folded it out of the rule.
    Arrived { project: &'a ProjectDefinition, values: &'a dyn Arrived },
}

impl Reading<'_> {
    fn project(&self) -> &ProjectDefinition {
        match self {
            Reading::Program { cx, .. } => cx.project(),
            Reading::Arrived { project, .. } => project,
        }
    }
}

fn written<'a>(node: &'a NodeDefinition, field: &str, at: &Reading<'a>) -> Written<'a> {
    let cx = match at {
        Reading::Arrived { values, .. } => return Written::Known(values.value(field)),
        Reading::Program { cx, .. } => cx,
    };
    // A wire is authoritative over a written value (`ready::fill_input_from_literals`).
    if has_incoming_edge(node, &cx.graph, field) {
        return Written::Arrives { may_be_absent: wired_may_arrive_empty(node, field) };
    }
    match node.written_value(field) {
        Some(value) if crate::picks::is_install_picked(value) || crate::content::ContentRef::from_value(value).is_some() => Written::Unknown,
        Some(value) => match crate::instance::as_instance_filled(value) {
            Some(filled) if filled.may_stay_empty => Written::Arrives { may_be_absent: true },
            Some(filled) => match filled.fallback {
                Some(fallback) => Written::Known(as_given(node, field, fallback)),
                None => Written::Unknown,
            },
            None => Written::Known(as_given(node, field, value)),
        },
        None => Written::Known(None),
    }
}

/// `value` as the node is given it: none at all when it is a blank text
/// where a blank is no value (`ready::blank_is_absent`).
fn as_given<'a>(node: &NodeDefinition, field: &str, value: &'a Value) -> Option<&'a Value> {
    let blank = node.inputs.iter().find(|i| i.name == field).is_some_and(|input| crate::run::ready::blank_is_absent(input, value));
    (!blank).then_some(value)
}

/// Whether the node can fire with nothing on the wired input `port`: an
/// optional input runs on a wire that closed empty, and a blank text on
/// an input where a blank is no value reaches the node as nothing.
fn wired_may_arrive_empty(node: &NodeDefinition, port: &str) -> bool {
    node.inputs.iter().find(|i| i.name == port).is_some_and(|input| {
        !input.is_required(true) || crate::run::ready::blank_can_be_absent(input)
    })
}

/// A condition's answer: `Some` when known, `None` when it depends on a
/// value nobody knows yet (see the module docs). The combinators are the
/// three-valued ones: `all` is false as soon as one part is, `any` true as
/// soon as one part is, and otherwise unknown when a part is.
pub fn evaluate(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> Option<bool> {
    fold(cond, node, &Reading::Program { cx, custom }).known()
}

/// A condition as far as one reading decides it.
enum Folded {
    Known(bool),
    /// Undecided: the condition with every part that was decided taken
    /// out. A part decided inside an undecided `all` is true (a false one
    /// would have decided it), and inside an undecided `any` false, so
    /// dropping it changes nothing: what is left asks only what was not
    /// known.
    Open(Condition),
}

impl Folded {
    fn known(self) -> Option<bool> {
        match self {
            Folded::Known(b) => Some(b),
            Folded::Open(_) => None,
        }
    }
}

/// THE evaluator: `cond` on `node`, read through `at`. The decisive
/// answer of an `all` (false) or an `any` (true) leaves the parts after
/// it unasked.
fn fold(cond: &Condition, node: &NodeDefinition, at: &Reading) -> Folded {
    match cond {
        Condition::All { of } => settle(of, false, node, at, |of| Condition::All { of }),
        Condition::Any { of } => settle(of, true, node, at, |of| Condition::Any { of }),
        Condition::Not { of } => match fold(of, node, at) {
            Folded::Known(b) => Folded::Known(!b),
            Folded::Open(open) => Folded::Open(Condition::Not { of: Box::new(open) }),
        },
        // A list read off a wired node is resolved first, so a leftover
        // carries the values themselves and a run reads no other node.
        leaf => {
            let leaf = listed(leaf, node, at);
            match answer(&leaf, node, at) {
                Some(b) => Folded::Known(b),
                None => Folded::Open(leaf.into_owned()),
            }
        }
    }
}

/// `cond` with every list its `noneOf` and `noneMatch` checks read off a
/// wired node ([`ValueList::Wired`]) written in as the values it lists. Only the
/// program's reading sees the wires; a reading of what arrived gets
/// `cond` as it is, and a recorded rule never holds a wired list.
fn listed<'c>(cond: &'c Condition, node: &NodeDefinition, at: &Reading) -> std::borrow::Cow<'c, Condition> {
    use std::borrow::Cow;
    let (Condition::Value { field, checks }, Reading::Program { cx, .. }) = (cond, at) else { return Cow::Borrowed(cond) };
    if checks.wired_lists().next().is_none() {
        return Cow::Borrowed(cond);
    }
    let mut checks = checks.clone();
    let mut level: Option<&mut ValueChecks> = Some(&mut checks);
    while let Some(here) = level {
        for refusal in Refusal::ALL {
            let list = refusal.of_mut(here);
            if let Some(ValueList::Wired(wired)) = list {
                *list = Some(ValueList::Listed(wired_values(node, wired, cx)));
            }
        }
        level = here.each.as_deref_mut().map(|each| &mut each.checks);
    }
    Cow::Owned(Condition::Value { field: field.clone(), checks })
}

/// The nodes whose output is wired into `node`'s input `port`: followed
/// back through a loop's `LoopIn` (which passes a value in under the
/// same port name) to the node it comes from. A wire crossing a group or
/// a call site already runs from that node (`crate::project::Edge::via`);
/// one from a group's own port carries a value written there, and no node.
fn wired_from<'a>(node: &NodeDefinition, port: &str, cx: &'a RuleContext) -> Vec<&'a NodeDefinition> {
    let mut found: Vec<&NodeDefinition> = Vec::new();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut pending = vec![(node.id.clone(), port.to_string())];
    while let Some((id, port)) = pending.pop() {
        if !seen.insert((id.clone(), port.clone())) {
            continue;
        }
        for edge in cx.graph.edges_into(&id).filter(|e| e.target_handle.as_deref() == Some(port.as_str()) && !e.to_group_port() && !e.from_group_port()) {
            let Some(source) = cx.graph.node(&edge.source) else { continue };
            let from = edge.source_port();
            if source.node_type == crate::project::boundary_types::LOOP_IN && source.inputs.iter().any(|input| input.name == from) {
                pending.push((source.id.clone(), from.to_string()));
            } else if !found.iter().any(|n| n.id == source.id) {
                found.push(source);
            }
        }
    }
    found
}

/// What is wrong with the lists `rules` read off a node wired into
/// `node`, each with the wired node, the diagnostic's code and what to
/// tell. A list the wired node does not declare is `list-not-declared`
/// unless the rule marks it `optional`: a check reading a list nobody
/// there declares would refuse nothing, so the program is refused instead
/// (a misspelled list, or a node wired in that is not the kind the rule
/// expects). An item of a list a `noneMatch` reads that is no regex is
/// `list-not-regex`: it would match nothing. An input nothing is wired
/// into names no node, and the input's own rules say what is missing.
pub fn wired_list_faults<'a>(rules: &[ValidationRule], node: &NodeDefinition, cx: &'a RuleContext) -> Vec<(&'a NodeDefinition, &'static str, String)> {
    fn walk<'c>(cond: &'c Condition, wired: &mut Vec<(Refusal, &'c WiredList)>) {
        match cond {
            Condition::Value { checks, .. } => {
                for level in checks.levels() {
                    wired.extend(level.lists().filter_map(|(refusal, list)| list.wired().map(|w| (refusal, w))));
                }
            }
            Condition::All { of } | Condition::Any { of } => of.iter().for_each(|c| walk(c, wired)),
            Condition::Not { of } => walk(of, wired),
            _ => {}
        }
    }
    let mut wired = Vec::new();
    rules.iter().for_each(|rule| walk(&rule.when, &mut wired));
    let mut out: Vec<(&NodeDefinition, &'static str, String)> = Vec::new();
    let reader = format!("{} '{}'", node.node_type, node.id);
    for (refusal, list) in wired {
        for lister in wired_from(node, &list.of_node_wired_into, cx) {
            let told = match lister.lists.get(&list.list) {
                None if list.optional => continue,
                None => vec![(
                    "list-not-declared",
                    format!(
                        "{} declares no list '{}' that {reader} reads through `{}`",
                        spelled_with_type(lister, cx),
                        list.list,
                        list.of_node_wired_into
                    ),
                )],
                Some(_) if refusal == Refusal::NoneOf => continue,
                Some(items) => items
                    .iter()
                    .filter_map(|item| crate::node::pattern_fault(&Value::String(item.clone())).err())
                    .map(|why| {
                        let lister = spelled_with_type(lister, cx);
                        ("list-not-regex", format!("{lister} lists {why} in '{}', which {reader} matches against through `{}`", list.list, list.of_node_wired_into))
                    })
                    .collect(),
            };
            for (code, told) in told {
                if !out.iter().any(|(_, _, said)| *said == told) {
                    out.push((lister, code, told));
                }
            }
        }
    }
    out
}

/// `node` as a message names it: its type and every place it runs
/// (`ClaudeCode 'claude'`).
fn spelled_with_type(node: &NodeDefinition, cx: &RuleContext) -> String {
    let places: Vec<String> = cx
        .places()
        .iter()
        .filter(|place| place.id == node.id)
        .map(|place| format!("'{}'", crate::project::address_of(&cx.graph, &place.id, &place.path)))
        .collect();
    match places.is_empty() {
        true => format!("{} '{}'", node.node_type, node.id),
        false => format!("{} {}", node.node_type, places.join(", ")),
    }
}

/// The values `wired` names: the list of that name each node wired into
/// its input declares (`NodeDefinition::lists`), every one once, in the
/// order they come. None at all when nothing is wired there; a node wired
/// there that declares no such list adds none, and the compile refuses the
/// program for it ([`wired_list_faults`]).
fn wired_values(node: &NodeDefinition, wired: &WiredList, cx: &RuleContext) -> Vec<Value> {
    let mut values: Vec<Value> = Vec::new();
    for lister in wired_from(node, &wired.of_node_wired_into, cx) {
        for item in lister.lists.get(&wired.list).into_iter().flatten() {
            let item = Value::String(item.clone());
            if !values.contains(&item) {
                values.push(item);
            }
        }
    }
    values
}

/// The three-valued `all` (`decisive` false) or `any` (`decisive` true):
/// the decisive answer as soon as one part gives it, the parts after it
/// left unasked; otherwise the undecided parts, rebuilt by `rebuild`
/// (one alone stands for itself), and the other answer when none is.
fn settle(parts: &[Condition], decisive: bool, node: &NodeDefinition, at: &Reading, rebuild: fn(Vec<Condition>) -> Condition) -> Folded {
    let mut open = Vec::new();
    for part in parts {
        match fold(part, node, at) {
            Folded::Known(b) if b == decisive => return Folded::Known(decisive),
            Folded::Known(_) => {}
            Folded::Open(part) => open.push(part),
        }
    }
    match open.len() {
        0 => Folded::Known(!decisive),
        1 => Folded::Open(open.remove(0)),
        _ => Folded::Open(rebuild(open)),
    }
}

/// One condition that is no combinator, read through `at`.
fn answer(cond: &Condition, node: &NodeDefinition, at: &Reading) -> Option<bool> {
    match cond {
        // A value nobody knows yet is there (the instance is given it, or
        // the run is refused before it starts) and not blank, and says
        // nothing of its content. A wired one is there when the node
        // cannot fire without it; anything more waits for what arrives.
        // A list still read off a wired node ([`listed`] resolves every
        // one the program's reading meets) is a recorded rule this
        // runtime does not read: no answer.
        Condition::Value { checks, .. } if checks.wired_lists().next().is_some() => None,
        Condition::Value { field, checks } => match written(node, field, at) {
            Written::Known(value) => Some(value_holds(checks, value)),
            Written::Unknown => (!checks.reads_content()).then_some(true),
            Written::Arrives { may_be_absent } => (checks.is_empty() && !may_be_absent).then_some(true),
        },
        Condition::InputNames { port, names } => match written(node, port, at) {
            Written::Known(value) => Some(misnamed(value, at.project(), *names).is_empty()),
            Written::Unknown | Written::Arrives { .. } => None,
        },
        Condition::InputSatisfied { port } => match at {
            Reading::Arrived { values, .. } => Some(values.value(port).is_some_and(|v| !v.is_null())),
            Reading::Program { cx, .. } if has_incoming_edge(node, &cx.graph, port) => {
                (!wired_may_arrive_empty(node, port)).then_some(true)
            }
            Reading::Program { .. } if crate::instance::may_stay_empty(node, port) => None,
            Reading::Program { .. } => Some(literal_fills(node, port)),
        },
        // The install the program was compiled for, read off the program
        // the compile or the run reads.
        Condition::LocalInstall {} => at.project().install.map(|install| install == InstallKind::Local),
        // The rest is about the program's shape, which only the compile
        // reads (and which it decided, so a recheck never asks it).
        _ => match at {
            Reading::Program { cx, custom } => Some(about_the_program(cond, node, cx, custom)),
            Reading::Arrived { .. } => None,
        },
    }
}

/// A condition about the program's shape rather than a value: its wires,
/// the runs it carves, the ports its source added.
fn about_the_program(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> bool {
    let graph = &cx.graph;
    match cond {
        Condition::InputWired { port } => has_incoming_edge(node, graph, port),
        Condition::OutputWired { port } => has_outgoing_edge(node, graph, port),
        Condition::RunReaches { direction, with } => run_reaches(node, cx, *direction, with),
        Condition::DownstreamOf { with } => !upstream_with(node, cx, with).is_empty(),
        Condition::PerInstance {} => node.per_instance_somewhere(),
        Condition::CustomInputsDeclared {} => !custom.inputs.is_empty(),
        Condition::CustomOutputsDeclared {} => !custom.outputs.is_empty(),
        Condition::OutputTypeFits { port, within } => node
            .outputs
            .iter()
            .find(|output| output.name == *port)
            .is_some_and(|output| crate::weft_type::WeftType::fits_within(&output.port_type, within)),
        Condition::CustomInputsFit { one_of } => unfit_inputs(node, custom, one_of).is_empty(),
        other => unreachable!("{other:?} is a question about a value, answered by `answer`"),
    }
}

/// A `value` condition on what a field holds: nothing there (or null) is
/// false, unless no check is asked, where it is the answer itself.
fn value_holds(checks: &ValueChecks, value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(value) => checks_hold(checks, value, true),
    }
}

/// Whether `value` holds every check of `checks`. At the `top` (the
/// field's own value) a check meeting the wrong kind of value is false
/// (`matches` on a number); inside an `each` it passes, the type check
/// owning kinds.
fn checks_hold(checks: &ValueChecks, value: &Value, top: bool) -> bool {
    let of_kind = |answer: Option<bool>| answer.unwrap_or(!top);
    let ValueChecks { nonempty, equals, one_of, none_of: _, none_match, matches: regex, count, range, length, unique, each } = checks;
    let refused = |refusal: Refusal| match refusal.of(checks) {
        None => false,
        Some(ValueList::Listed(listed)) => refusal.refuses(listed, value),
        Some(ValueList::Wired(_)) => unreachable!("a wired list is written in before its checks are asked (`listed`, `answer`)"),
    };
    (!nonempty || is_nonempty(Some(value)))
        && equals.as_ref().is_none_or(|equals| equals == value)
        && one_of.as_ref().is_none_or(|set| set.contains(value))
        && !refused(Refusal::NoneOf)
        && (none_match.is_none() || of_kind(value.as_str().map(|_| !refused(Refusal::NoneMatch))))
        && regex.as_ref().is_none_or(|regex| of_kind(value.as_str().map(|text| crate::node::regex_matches(regex, text))))
        && count.as_ref().is_none_or(|count| count.holds(crate::node::item_count(value)))
        && range.as_ref().is_none_or(|range| of_kind(value.as_f64().map(|n| range.holds(n))))
        && length.as_ref().is_none_or(|length| of_kind(value.as_str().map(|text| length.holds(text.chars().count() as u64))))
        && unique.as_ref().is_none_or(|unique| all_differ(unique, value))
        && each.as_ref().is_none_or(|each| {
            let items = match value {
                Value::Array(items) => items.as_slice(),
                one => std::slice::from_ref(one),
            };
            items.iter().all(|item| match &each.key {
                None => checks_hold(&each.checks, item, false),
                Some(key) => match item.get(key.as_str()) {
                    None | Some(Value::Null) => true,
                    Some(set) => checks_hold(&each.checks, set, false),
                },
            })
        })
}

/// Whether no two items of `value` are alike ([`Unique`]): a list's
/// items, or by a key, a list's records or a dict's values. A dict's keys
/// never repeat, and any other value is one item.
fn all_differ(unique: &Unique, value: &Value) -> bool {
    let compared: Vec<&Value> = match (unique, value) {
        (Unique::Items, Value::Array(items)) => items.iter().collect(),
        (Unique::Items, _) => return true,
        (Unique::Key(key), Value::Array(items)) => items.iter().filter_map(|item| item.get(key.as_str())).collect(),
        (Unique::Key(key), Value::Object(entries)) => entries.values().filter_map(|item| item.get(key.as_str())).collect(),
        (Unique::Key(_), _) => return true,
    };
    let compared: Vec<&Value> = compared.into_iter().filter(|v| !v.is_null()).collect();
    compared.iter().enumerate().all(|(i, a)| !compared[..i].contains(a))
}

/// One of a node's rules the compiler could not decide
/// ([`NodeDefinition::recheck`]), checked before each firing's body on
/// the values that arrived ([`recheck`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Recheck {
    /// The rule's condition with every part the compiler decided taken
    /// out ([`Folded::Open`]): only questions about values are left.
    pub when: Condition,
    /// The rule's message with its holes filled at compile time, but
    /// `{names}` when `when` still asks for names, and `{noneOfFound}` or
    /// `{noneMatchFound}` when it still asks that check: those are what
    /// arrived, filled when the rule fires.
    pub message: String,
}

/// The rules of `node` (`rules`, the ones its type answers to) the
/// compile leaves undecided, each folded down to what is left to ask
/// ([`Recheck`]). Only an error can fail a firing, so a warning, an info
/// or a hint left undecided is left out: it already said nothing. The
/// install the program is compiled for is answered here like any other
/// question, so a rule it settles is not recorded, and only a compile for
/// no install, which never runs, records one still asking it.
pub fn undecided(rules: &[ValidationRule], node: &NodeDefinition, cx: &RuleContext, custom: &CustomPorts) -> Vec<Recheck> {
    let at = Reading::Program { cx, custom };
    rules
        .iter()
        .filter(|rule| rule.then.severity == crate::node::RuleSeverity::Error)
        .filter_map(|rule| match fold(&rule.when, node, &at) {
            Folded::Known(_) => None,
            Folded::Open(when) => {
                let mut message = holes_but_found(rule, node, cx, custom);
                if !asks_names(&when) {
                    message = names_hole(message, &rule.when, node, &at, cx);
                }
                for refusal in Refusal::ALL.into_iter().filter(|refusal| !asks_refusal(&when, *refusal)) {
                    message = found_hole(message, refusal, &rule.when, node, &at);
                }
                Some(Recheck { when, message })
            }
        })
        .collect()
}

/// Whether `cond` holds a `value` condition with a `refusal` check, whose
/// refused values fill its hole ([`Refusal::hole`]).
fn asks_refusal(cond: &Condition, refusal: Refusal) -> bool {
    match cond {
        Condition::Value { checks, .. } => checks.levels().any(|level| refusal.of(level).is_some()),
        Condition::All { of } | Condition::Any { of } => of.iter().any(|c| asks_refusal(c, refusal)),
        Condition::Not { of } => asks_refusal(of, refusal),
        _ => false,
    }
}

/// Whether `cond` holds a `local_install` condition.
fn asks_install(cond: &Condition) -> bool {
    match cond {
        Condition::LocalInstall {} => true,
        Condition::All { of } | Condition::Any { of } => of.iter().any(asks_install),
        Condition::Not { of } => asks_install(of),
        _ => false,
    }
}

/// Whether `cond` holds an `input_names` condition, whose refused names
/// fill `{names}`.
fn asks_names(cond: &Condition) -> bool {
    match cond {
        Condition::InputNames { .. } => true,
        Condition::All { of } | Condition::Any { of } => of.iter().any(asks_names),
        Condition::Not { of } => asks_names(of),
        _ => false,
    }
}

/// Check `node`'s recorded rules ([`NodeDefinition::recheck`]) on the
/// `values` that arrived at one of its firings, in `project`, the program
/// it runs in. The first that fires fails the firing with its own
/// message, as an input error: the fix is in the program, so a wired
/// `_error` does not catch it. A node with no recorded rule returns at
/// once, so it pays nothing.
pub fn recheck(node: &NodeDefinition, project: &ProjectDefinition, values: &dyn Arrived) -> crate::error::WeftResult<()> {
    if node.recheck.is_empty() {
        return Ok(());
    }
    let at = Reading::Arrived { project, values };
    for rule in &node.recheck {
        match fold(&rule.when, node, &at) {
            Folded::Known(false) => {}
            Folded::Known(true) => {
                let mut message = rule.message.clone();
                if message.contains("{names}") {
                    message = names_hole(message, &rule.when, node, &at, &RuleContext::new(project));
                }
                for refusal in Refusal::ALL {
                    message = found_hole(message, refusal, &rule.when, node, &at);
                }
                return Err(crate::error::WeftError::Input(message));
            }
            // Only a compile for no install records a rule still asking
            // the install, and such a program never runs (each install
            // compiles its own versions): it was made by hand or by a
            // command's own compile.
            Folded::Open(open) if project.install.is_none() && asks_install(&open) => {
                return Err(crate::error::WeftError::Input(format!(
                    "the rule \"{}\" asks which install the program is built for, and this program was \
                     compiled for none; run it through an install, which builds it for itself",
                    rule.message
                )));
            }
            // Every part of a recorded rule asks about a value or the
            // install, and both are known by now: a part left open is a
            // compiled program this runtime does not read.
            Folded::Open(open) => {
                return Err(crate::error::WeftError::Input(format!(
                    "the rule \"{}\" asks {open:?}, which the values that arrived cannot answer; \
                     the program was compiled by another weft, rebuild it",
                    rule.message
                )));
            }
        }
    }
    Ok(())
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

/// Every name `cond`'s `input_names` conditions refuse on `node`, read
/// through `at`, and every added input its `custom_inputs_fit` conditions
/// refuse (`{names}`), walked through the combinators.
fn collect_names(cond: &Condition, node: &NodeDefinition, at: &Reading, cx: &RuleContext, names: &mut Vec<String>) {
    match cond {
        Condition::InputNames { port, names: named } => {
            if let Written::Known(value) = written(node, port, at) {
                names.extend(misnamed(value, at.project(), *named).iter().map(|name| refused_name(name, cx, *named)));
            }
        }
        Condition::CustomInputsFit { one_of } => {
            if let Reading::Program { custom, .. } = at {
                names.extend(unfit_inputs(node, custom, one_of).into_iter().map(|name| format!("'{name}'")));
            }
        }
        Condition::All { of } | Condition::Any { of } => {
            for c in of {
                collect_names(c, node, at, cx, names);
            }
        }
        Condition::Not { of } => collect_names(of, node, at, cx, names),
        _ => {}
    }
}

/// Every value `cond`'s `refusal` checks refuse in what `at` reads (its
/// hole), walked through the combinators and into `each`.
fn collect_refused(refusal: Refusal, cond: &Condition, node: &NodeDefinition, at: &Reading, found: &mut Vec<Value>) {
    match cond {
        Condition::Value { .. } => {
            let resolved = listed(cond, node, at);
            let Condition::Value { field, checks } = &*resolved else { unreachable!("`listed` keeps the kind") };
            if let Written::Known(Some(value)) = written(node, field, at) {
                refused_by(refusal, checks, value, found);
            }
        }
        Condition::All { of } | Condition::Any { of } => {
            for c in of {
                collect_refused(refusal, c, node, at, found);
            }
        }
        Condition::Not { of } => collect_refused(refusal, of, node, at, found),
        _ => {}
    }
}

/// The values in `value` a `refusal` check of `checks` refuses: the value
/// itself, and inside `each` every item (or its `key`), as [`checks_hold`]
/// reads them.
fn refused_by(refusal: Refusal, checks: &ValueChecks, value: &Value, found: &mut Vec<Value>) {
    if let Some(ValueList::Listed(listed)) = refusal.of(checks) {
        if refusal.refuses(listed, value) {
            found.push(value.clone());
        }
    }
    if let Some(each) = &checks.each {
        let items = match value {
            Value::Array(items) => items.as_slice(),
            one => std::slice::from_ref(one),
        };
        for item in items {
            match &each.key {
                None => refused_by(refusal, &each.checks, item, found),
                Some(key) => {
                    if let Some(set) = item.get(key.as_str()).filter(|v| !v.is_null()) {
                        refused_by(refusal, &each.checks, set, found);
                    }
                }
            }
        }
    }
}

/// Every node `cond`'s `noneOf` and `noneMatch` checks read a list off
/// (`{wiredNode}`),
/// spelled `Type 'place'`, walked through the combinators.
fn collect_listers(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, listers: &mut Vec<String>) {
    match cond {
        Condition::Value { checks, .. } => {
            for wired in checks.wired_lists() {
                for lister in wired_from(node, &wired.of_node_wired_into, cx) {
                    listers.push(spelled_with_type(lister, cx));
                }
            }
        }
        Condition::All { of } | Condition::Any { of } => {
            for c in of {
                collect_listers(c, node, cx, listers);
            }
        }
        Condition::Not { of } => collect_listers(of, node, cx, listers),
        _ => {}
    }
}

/// Every node `cond`'s `downstream_of` conditions found (`{with}`),
/// walked through the combinators.
fn collect_with(cond: &Condition, node: &NodeDefinition, cx: &RuleContext, with_found: &mut Vec<String>) {
    match cond {
        Condition::DownstreamOf { with } => with_found.extend(upstream_with(node, cx, with)),
        Condition::All { of } | Condition::Any { of } => {
            for c in of {
                collect_with(c, node, cx, with_found);
            }
        }
        Condition::Not { of } => collect_with(of, node, cx, with_found),
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

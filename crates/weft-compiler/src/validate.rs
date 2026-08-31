//! Graph validation. Runs after enrichment. Emits structured
//! Diagnostic objects (errors + warnings) for the IDE's Problems
//! panel and for the full compile pipeline.
//!
//! Ported from v1's weft-parser.ts validation passes; line ranges
//! for each rule documented at the helper function that implements
//! it.

use weft_core::node::{
    Condition, MetadataCatalog, RuleDiagnostic, RuleSeverity, ValidationLevel, ValidationRule,
};
use weft_core::exec::skip::SHOULD_FLOW_PORT;
use weft_core::project::{NodeDefinition, Span};
use weft_core::weft_type::WeftType;
use weft_core::ProjectDefinition;

use crate::weft_compiler::read_loop_port_list_vetted;
use crate::{Diagnostic, Severity};

/// Which validation rules to run. `Structural` checks only editor-
/// time errors (graph shape, required ports wired or satisfied by a
/// literal, config shape). `Runtime` additionally runs rules flagged
/// `level: runtime` (e.g. missing credentials), which we deliberately
/// skip during editing so an AI builder or human-in-the-loop can
/// sketch a project without filling every secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationMode {
    Structural,
    Runtime,
}

/// Run every validation rule against an enriched project and collect
/// all diagnostics. Returns an empty vector for a clean program.
/// `catalog` provides per-node metadata (including declarative
/// `validate` rules).
pub fn validate(project: &ProjectDefinition, catalog: &dyn MetadataCatalog) -> Vec<Diagnostic> {
    validate_with_mode(project, catalog, ValidationMode::Structural)
}

pub fn validate_with_mode(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mode: ValidationMode,
) -> Vec<Diagnostic> {
    // The catalog's type registry is scoped around the WHOLE run, so
    // every rule that resolves a declared name (named-type-conflict's
    // registry half in particular) sees the project's table on every
    // caller, not just the ones that remembered to scope it (the
    // build path once forgot, silently no-op-ing the rule). The scope
    // stack is re-entrant, so already-scoped callers nest harmlessly.
    let registry = catalog.type_registry();
    registry.scoped(|| validate_scoped(project, catalog, mode))
}

fn validate_scoped(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mode: ValidationMode,
) -> Vec<Diagnostic> {
    let mut d = Vec::new();
    check_duplicates(project, &mut d);
    check_edge_node_refs(project, &mut d);
    check_scope_reachability(project, &mut d);
    check_port_resolution(project, &mut d);
    check_type_compat(project, &mut d);
    check_port_coverage(project, catalog, &mut d);
    check_config_derived_ports(project, catalog, &mut d);
    check_loop_config(project, &mut d);
    check_double_driven_ports(project, &mut d);
    check_warnings(project, &mut d);
    check_output_reachability(project, &mut d);
    check_declarative_rules(project, catalog, mode, &mut d);
    check_reserved_names(project, catalog, &mut d);
    check_graph_shape(project, &mut d);
    check_generator_wiring(project, &mut d);
    check_named_type_conflicts(project, &mut d);
    d
}

/// named-type-conflict: nominal compatibility compares the NAME alone
/// ("one name, one body" is the contract), so every restatement of a
/// named type across the project's ports must carry the same body, and
/// must match the registry's declaration when one is in scope. This is
/// the AUTHORING half of the guarantee; the parser only enforces the
/// registry-independent half (a body must be concrete), because
/// deserialization must stay a pure function of the string (a stored
/// project keeps parsing after its declaration is edited).
fn check_named_type_conflicts(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    fn walk<'t>(ty: &'t WeftType, out: &mut Vec<(&'t str, &'t WeftType)>) {
        match ty {
            WeftType::Named { name, body } => {
                out.push((name, body));
                walk(body, out);
            }
            WeftType::List(inner) | WeftType::Generator(inner) => walk(inner, out),
            WeftType::Dict(k, v) => {
                walk(k, out);
                walk(v, out);
            }
            WeftType::Union(members) => members.iter().for_each(|m| walk(m, out)),
            WeftType::Record(fields) => fields.iter().for_each(|f| walk(&f.ty, out)),
            _ => {}
        }
    }

    let registry = weft_core::weft_type::TypeRegistry::current();
    let mut seen: std::collections::HashMap<&str, (&WeftType, String)> =
        std::collections::HashMap::new();
    for node in &project.nodes {
        let span = node.header_span_or_default();
        let ports = node
            .inputs
            .iter()
            .map(|p| (&p.name, &p.port_type))
            .chain(node.outputs.iter().map(|p| (&p.name, &p.port_type)));
        for (port_name, port_type) in ports {
            let mut named = Vec::new();
            walk(port_type, &mut named);
            for (name, body) in named {
                let site = format!("{}.{}", node.id, port_name);
                if let Some(weft_core::weft_type::WeftType::Named { body: declared, .. }) =
                    registry.lookup(name)
                {
                    if declared.as_ref() != body {
                        push(d, span, Severity::Error, "named-type-conflict",
                            format!(
                                "port '{site}' restates type `{name}` as `{body}`, but \
                                 it is declared as `{declared}`; a named type has one \
                                 body everywhere"
                            ));
                        continue;
                    }
                }
                match seen.get(name) {
                    None => {
                        seen.insert(name, (body, site));
                    }
                    Some((first_body, first_site)) if *first_body != body => {
                        push(d, span, Severity::Error, "named-type-conflict",
                            format!(
                                "type `{name}` appears with two different bodies: \
                                 `{first_body}` at '{first_site}' and `{body}` at \
                                 '{site}'; a named type has one body everywhere"
                            ));
                    }
                    Some(_) => {}
                }
            }
        }
    }
}

/// `Generator[T]` wiring rules. A stream is a typed, one-directional,
/// live edge between exactly two running node bodies, which pins down
/// three things the rest of the type system does not:
///
/// - generator-multiple-consumers: a Generator OUTPUT feeds exactly ONE
///   input. Two consumers on one stream is a question with no defined
///   answer (do both see every item, or do they race?); fanning out is
///   Bus territory. (Many-to-one needs nothing here: the general
///   `duplicate-input-port` rule already forbids two edges into any
///   input.)
/// - generator-through-group: a stream cannot cross a plain Group
///   boundary (a Passthrough forwards ONE value per firing, not a live
///   item flow) nor a loop's boundary except as the loop's own `over`
///   port (checked by `check_loop_config`). Put the producer and its
///   consumer in the same scope.
/// - generator-in-container: `Generator[T]` is a PORT type only. Nested
///   inside a List/Dict/Record/Union it would make the stream a value
///   to store or copy, which it is not.
fn check_generator_wiring(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    /// Any `Generator` anywhere in `ty` (nominal bodies included).
    fn contains_generator(ty: &WeftType) -> bool {
        match ty {
            WeftType::Generator(_) => true,
            WeftType::List(inner) => contains_generator(inner),
            WeftType::Dict(k, v) => contains_generator(k) || contains_generator(v),
            WeftType::Union(members) => members.iter().any(contains_generator),
            WeftType::Record(fields) => fields.iter().any(|f| contains_generator(&f.ty)),
            WeftType::Named { body, .. } => contains_generator(body),
            _ => false,
        }
    }

    /// A `Generator` anywhere BELOW the root position. The root itself
    /// may be a stream (directly, or through a nominal alias like
    /// `type Rows = Generator[Number]`, which `as_generator` peels);
    /// any deeper occurrence makes the stream a storable value, which
    /// it is not.
    fn generator_below_root(ty: &WeftType) -> bool {
        match ty.as_generator() {
            Some(element) => contains_generator(element),
            None => contains_generator(ty),
        }
    }

    let by_id: std::collections::HashMap<&str, &NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    for node in &project.nodes {
        let span = node.header_span_or_default();
        for port in &node.inputs {
            if generator_below_root(&port.port_type) {
                push(d, span, Severity::Error, "generator-in-container",
                    format!(
                        "input '{}.{}: {}': Generator[T] is a port type; a stream cannot \
                         sit inside a list, dict, record, or union",
                        node.id, port.name, port.port_type
                    ));
            }
            // An unwired stream has no meaning: a stream cannot be
            // defaulted and has no zero value (the port's runtime
            // value is a live handle the engine installs), so a
            // Generator input must always be wired, which is what
            // `required` without a default guarantees. Only PASSTHROUGH
            // boundaries are exempt: their `required` shape is a
            // flatten artifact, never a user declaration, and their
            // stream ports are already reported by the boundary-node
            // rule below. A LoopIn's `required` mirrors the user's own
            // `over` declaration (a stream `over` port is legal), so
            // it stays under this rule: an optional stream `over`
            // would otherwise compile as an unwirable loop.
            if node.node_type != "Passthrough"
                && port.port_type.as_generator().is_some()
                && (!port.required || port.default.is_some())
            {
                push(d, span, Severity::Error, "generator-input-must-be-required",
                    format!(
                        "input '{}.{}': a Generator input must be required and cannot \
                         have a default; a stream has no zero value, so an unwired \
                         stream port could never be satisfied",
                        node.id, port.name
                    ));
            }
        }
        for port in &node.outputs {
            if generator_below_root(&port.port_type) {
                push(d, span, Severity::Error, "generator-in-container",
                    format!(
                        "output '{}.{}: {}': Generator[T] is a port type; a stream cannot \
                         sit inside a list, dict, record, or union",
                        node.id, port.name, port.port_type
                    ));
            }
            if port.port_type.as_generator().is_some() {
                let consumers = project
                    .edges
                    .iter()
                    .filter(|e| {
                        e.source == node.id && e.source_handle.as_deref() == Some(&port.name)
                    })
                    .count();
                if consumers > 1 {
                    push(d, span, Severity::Error, "generator-multiple-consumers",
                        format!(
                            "stream '{}.{}' feeds {consumers} inputs; a Generator output \
                             connects to exactly ONE consumer (a stream has one taker). \
                             To broadcast, use a Bus",
                            node.id, port.name
                        ));
                }
            }
        }
    }

    // Group boundaries: a plain-Group boundary forwards one value per
    // firing, so a stream's items cannot flow through it. The ban
    // lives on the BOUNDARY NODE, not on edges: a stream-typed port on
    // a boundary Passthrough is the crossing itself, whichever side is
    // wired (the project's own ENTRY boundary has no incoming edge at
    // all, so an edge-based rule would let a stream-typed project
    // input compile clean and ship an unsatisfiable graph). One port,
    // one diagnostic, by construction. The project-interface boundary
    // (the root anonymous component's `__in`/`__out`) gets its own
    // wording: there IS no outer scope to move the producer into.
    let root_group_ids: std::collections::HashSet<&str> = project
        .groups
        .iter()
        .filter(|g| g.parent_group_id.is_none() && g.anonymous)
        .map(|g| g.id.as_str())
        .collect();
    for node in &project.nodes {
        let Some(gb) = node.group_boundary.as_ref() else { continue };
        if node.node_type != "Passthrough" {
            continue;
        }
        let span = node.header_span_or_default();
        // A Passthrough forwards, so each port appears as an input AND
        // an output; one NAME is one crossing and one diagnostic.
        let stream_ports: std::collections::BTreeSet<&str> = node
            .inputs
            .iter()
            .map(|p| (&p.name, &p.port_type))
            .chain(node.outputs.iter().map(|p| (&p.name, &p.port_type)))
            .filter(|(_, ty)| ty.as_generator().is_some())
            .map(|(name, _)| name.as_str())
            .collect();
        for port_name in stream_ports {
            if root_group_ids.contains(gb.group_id.as_str()) {
                push(d, span, Severity::Error, "generator-through-group",
                    format!(
                        "'{port_name}' declares a stream on the project's {} boundary; an \
                         execution's inputs and outputs are values, not live edges. \
                         Produce (or consume) the stream inside the project instead",
                        match gb.role {
                            weft_core::project::GroupBoundaryRole::In => "input",
                            weft_core::project::GroupBoundaryRole::Out => "output",
                        },
                    ));
            } else {
                push(d, span, Severity::Error, "generator-through-group",
                    format!(
                        "'{port_name}' carries a stream across the boundary of group \
                         '{}'; a stream is a live edge between two running nodes. Put the \
                         producer and its consumer in the same scope (a loop consumes a \
                         stream only through its own `over`)",
                        gb.group_id,
                    ));
            }
        }
    }

    for edge in &project.edges {
        let (Some(src), Some(tgt)) =
            (by_id.get(edge.source.as_str()), by_id.get(edge.target.as_str()))
        else {
            continue;
        };
        let src_generator = src
            .outputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref())
            .is_some_and(|p| p.port_type.as_generator().is_some());
        let tgt_generator = tgt
            .inputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.target_handle.as_deref())
            .is_some_and(|p| p.port_type.as_generator().is_some());
        if !src_generator && !tgt_generator {
            continue;
        }
        let span = edge.span.unwrap_or_default();
        // A stream flowing into a generic `T` port would instantiate
        // the variable with a live handle nobody pulls (the consumer
        // receives the raw marker and hangs the producer at its buffer
        // cap); the enrich pass refuses the binding, and this rule
        // names the real problem at the edge.
        if src_generator && !tgt_generator {
            // "Generic" is ANY unresolved leaf, at any depth (`T`,
            // `List[T]`, `Dict[String, T]`, `MustOverride`): enrich
            // refused the binding for all of them (a stream never
            // instantiates a typevar), and check_type_compat stays
            // quiet on this edge because THIS rule owns it, so the
            // predicates must be one condition, not two that drift.
            let tgt_port = tgt
                .inputs
                .iter()
                .find(|p| Some(p.name.as_str()) == edge.target_handle.as_deref());
            let tgt_generic =
                tgt_port.is_some_and(|p| p.port_type.contains_unresolved_leaf());
            if tgt_generic {
                push(d, span, Severity::Error, "generator-into-generic-port",
                    format!(
                        "edge '{}.{} -> {}.{}': the source is a stream, and the target \
                         declares '{}', a generic type that cannot consume one. A stream \
                         needs a port declared Generator[T]; to inspect items, consume \
                         the stream in a node and emit what you want to see",
                        edge.source,
                        edge.source_handle.as_deref().unwrap_or("?"),
                        edge.target,
                        edge.target_handle.as_deref().unwrap_or("?"),
                        tgt_port.map(|p| p.port_type.to_string()).unwrap_or_default(),
                    ));
                continue;
            }
        }
        // The mirror direction: a generic SOURCE cannot produce the
        // live stream a `Generator` input needs (enrich refuses that
        // binding too), and "connect it to something concrete" would
        // mislead here exactly as it would above.
        if tgt_generator && !src_generator {
            let src_port = src
                .outputs
                .iter()
                .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref());
            let src_generic =
                src_port.is_some_and(|p| p.port_type.contains_unresolved_leaf());
            if src_generic {
                push(d, span, Severity::Error, "generator-into-generic-port",
                    format!(
                        "edge '{}.{} -> {}.{}': the target is a stream port, and the \
                         source declares '{}', a generic type that cannot produce one. \
                         Declare the source output as Generator[T] (or feed the port \
                         from a real stream producer)",
                        edge.source,
                        edge.source_handle.as_deref().unwrap_or("?"),
                        edge.target,
                        edge.target_handle.as_deref().unwrap_or("?"),
                        src_port.map(|p| p.port_type.to_string()).unwrap_or_default(),
                    ));
                continue;
            }
        }
        // Loop boundaries: a stream INTO LoopOut (a gather/carry) has
        // no meaning, and a stream OUT of a LoopOut cannot exist
        // (nothing inside can produce one across the boundary); the
        // LoopIn side's rules live in `check_loop_config`
        // (`generator-not-iterated`), so it is deliberately NOT
        // re-reported here. Group (Passthrough) boundaries are banned
        // on the boundary NODE above, not per edge.
        // The target arm needs no generator guard: the early return
        // above already proved one endpoint of this edge is a stream.
        let banned_endpoint =
            tgt.node_type == "LoopOut" || (src.node_type == "LoopOut" && src_generator);
        if banned_endpoint {
            push(d, span, Severity::Error, "generator-through-group",
                format!(
                    "edge '{}.{} -> {}.{}' carries a stream across a loop boundary; a \
                     stream is a live edge between two running nodes. Put the producer \
                     and its consumer in the same scope (a loop consumes a stream only \
                     through its own `over`)",
                    edge.source,
                    edge.source_handle.as_deref().unwrap_or("?"),
                    edge.target,
                    edge.target_handle.as_deref().unwrap_or("?"),
                ));
        }
    }
}

// There is deliberately NO compile-time permission check: source holds
// only a connection id, so source alone cannot say what a connection
// actually holds. The check lives where the answer does: live in the
// editor when a connection is picked, at connect time, and at run-time
// resolution (the drift backstop).

/// Structural rules that make the trigger/fire semantics well-defined.
///
/// graph-cycle: the wire graph must be a DAG. Iteration is the Loop
///   container's job and cross-node feedback is the bus's; a wire cycle
///   has no execution order and would wedge the engine.
/// trigger-in-loop: a trigger inside a Loop container. Registration
///   happens once per project and fires land outside any iteration, so
///   a per-iteration trigger has no meaning.
/// trigger-into-trigger: a trigger wired (directly or transitively)
///   into another trigger. The downstream trigger's inputs snapshot at
///   setup and its upstream is never walked at fire, so the wiring
///   could never deliver; reject until the semantics are designed.
/// trigger-into-infra: an infra node downstream of a trigger.
///   Provisioning happens before any fire exists, so the value can
///   never be there.
fn check_graph_shape(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use std::collections::{HashMap, HashSet};

    // Node-level outgoing adjacency.
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &project.edges {
        outgoing.entry(e.source.as_str()).or_default().push(e.target.as_str());
    }

    // graph-cycle: iterative DFS with a three-color marking; report each
    // node where a back edge is found (one diagnostic per cycle entry).
    let mut state: HashMap<&str, u8> = HashMap::new(); // 0 absent, 1 in-stack, 2 done
    for start in project.nodes.iter().map(|n| n.id.as_str()) {
        if state.contains_key(start) {
            continue;
        }
        let mut stack: Vec<(&str, usize)> = vec![(start, 0)];
        state.insert(start, 1);
        while let Some((node, idx)) = stack.pop() {
            let next = outgoing.get(node).and_then(|v| v.get(idx)).copied();
            match next {
                Some(child) => {
                    stack.push((node, idx + 1));
                    match state.get(child) {
                        Some(1) => {
                            let span = project
                                .nodes
                                .iter()
                                .find(|n| n.id == child)
                                .map(|n| n.header_span_or_default())
                                .unwrap_or_default();
                            push(d, span, Severity::Error, "graph-cycle",
                                format!(
                                    "the wires form a cycle through '{child}'; a wire graph \
                                     must be acyclic (iterate with a Loop, exchange feedback \
                                     over a bus)"
                                ));
                        }
                        Some(_) => {}
                        None => {
                            state.insert(child, 1);
                            stack.push((child, 0));
                        }
                    }
                }
                None => {
                    state.insert(node, 2);
                }
            }
        }
    }

    // trigger-in-loop: a trigger whose scope chain crosses a Loop group.
    let loop_groups: HashSet<&str> = project
        .groups
        .iter()
        .filter(|g| matches!(g.kind, weft_core::project::GroupKind::Loop { .. }))
        .map(|g| g.id.as_str())
        .collect();
    for node in project.nodes.iter().filter(|n| n.features.is_trigger) {
        if node.scope.iter().any(|s| loop_groups.contains(s.as_str())) {
            push(d, node.header_span_or_default(), Severity::Error, "trigger-in-loop",
                format!(
                    "trigger '{}' is inside a Loop; a trigger registers once and fires \
                     outside any iteration, so it cannot live in a loop body",
                    node.id
                ));
        }
    }

    // trigger-into-trigger / trigger-into-infra: walk downstream from
    // each trigger.
    let triggers: HashSet<&str> = project
        .nodes
        .iter()
        .filter(|n| n.features.is_trigger)
        .map(|n| n.id.as_str())
        .collect();
    let infra: HashSet<&str> = project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .map(|n| n.id.as_str())
        .collect();
    for trigger in &triggers {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut frontier: Vec<&str> = outgoing.get(trigger).cloned().unwrap_or_default();
        while let Some(node) = frontier.pop() {
            if !seen.insert(node) {
                continue;
            }
            let span = project
                .nodes
                .iter()
                .find(|n| n.id == node)
                .map(|n| n.header_span_or_default())
                .unwrap_or_default();
            if triggers.contains(node) {
                push(d, span, Severity::Error, "trigger-into-trigger",
                    format!(
                        "trigger '{trigger}' is wired into trigger '{node}'; a trigger's \
                         inputs snapshot at setup, so another trigger's output can never \
                         reach it"
                    ));
            }
            if infra.contains(node) {
                push(d, span, Severity::Error, "trigger-into-infra",
                    format!(
                        "trigger '{trigger}' is wired into infra node '{node}'; \
                         provisioning happens before any fire exists, so the value can \
                         never be there"
                    ));
            }
            frontier.extend(outgoing.get(node).cloned().unwrap_or_default());
        }
    }
}

/// A node or group named after a known node type is ambiguous: a reference
/// like `MyName.port` parses as an INLINE node of that type, not a reference to
/// the declaration. Flag it on the declaration line. The catalog is dynamic, so
/// this lives here (where the catalog is in hand) rather than in the
/// catalog-agnostic parser; the structural keywords `Group`/`Passthrough` are
/// rejected in the parser instead, since they are not catalog entries.
fn check_reserved_names(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    out: &mut Vec<Diagnostic>,
) {
    // Compare the LOCAL name, not the scoped id: a node inside a group has id
    // `grp.Llm`, but the ambiguous reference written in source is the local
    // `Llm.port`, and `grp.Llm` would never match a catalog entry. The local
    // segment is what collides with a type name.
    let mut flag = |id: &str, span: Option<weft_core::project::Span>| {
        let local = id.rsplit('.').next().unwrap_or(id);
        if catalog.lookup(local).is_some() {
            let span = span.unwrap_or_default();
            out.push(Diagnostic::at(
                span,
                Severity::Error,
                "reserved-name",
                format!("'{local}' is a node type name and cannot be used as a node or group name (a reference like '{local}.port' would parse as an inline node)"),
            ));
        }
    };
    for node in &project.nodes {
        flag(&node.id, node.header_span);
    }
    for group in &project.groups {
        flag(&group.id, group.header_span);
    }
}

/// Evaluate each node's declarative `validate` rules from its
/// metadata, emit Diagnostics for rules that fire. Safe by
/// construction: the grammar is closed, no user Rust runs here.
fn check_declarative_rules(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mode: ValidationMode,
    out: &mut Vec<Diagnostic>,
) {
    for node in &project.nodes {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        for rule in &meta.validate {
            // Skip runtime-only rules in structural mode. Structural
            // mode is the editor path; runtime rules fire at run time.
            if matches!(rule.then.level, ValidationLevel::Runtime)
                && mode == ValidationMode::Structural
            {
                continue;
            }
            if eval_condition(&rule.when, node, project) {
                emit_rule_diagnostic(node, rule, out);
            }
        }
    }
}

fn eval_condition(cond: &Condition, node: &NodeDefinition, project: &ProjectDefinition) -> bool {
    match cond {
        Condition::InputSatisfied { port } => input_satisfied(node, project, port),
        Condition::InputWired { port } => has_incoming_edge(node, project, port),
        Condition::InputSourceType { port, equals } => {
            // Vacuously true if the port has no wired edges (use
            // `all(input_wired, input_source_type)` to require both).
            let sources: Vec<&NodeDefinition> = project
                .edges
                .iter()
                .filter(|e| e.target == node.id && e.target_handle.as_deref() == Some(port))
                .filter_map(|e| project.nodes.iter().find(|n| n.id == e.source))
                .collect();
            sources.iter().all(|n| &n.node_type == equals)
        }
        Condition::ConfigPresent { field } => node
            .config
            .get(field)
            .map(|v| !v.is_null())
            .unwrap_or(false),
        Condition::ConfigNonempty { field } => is_nonempty(node.config.get(field)),
        Condition::ConfigEquals { field, equals } => {
            node.config.get(field).map(|v| v == equals).unwrap_or(false)
        }
        Condition::ConfigInSet { field, values } => node
            .config
            .get(field)
            .and_then(|v| v.as_str())
            .map(|s| values.iter().any(|v| v == s))
            .unwrap_or(false),
        Condition::ConfigMatches { field, regex } => node
            .config
            .get(field)
            .and_then(|v| v.as_str())
            // Absent/non-string field -> false (not satisfied), like every sibling
            // ConfigX condition. A malformed regex (a metadata-authoring bug) also
            // yields false: it can't match, so the condition fails CLOSED rather
            // than silently evaluating true and suppressing/forcing a diagnostic.
            .and_then(|s| regex::Regex::new(regex).ok().map(|r| r.is_match(s)))
            .unwrap_or(false),
        Condition::All { of } => of.iter().all(|c| eval_condition(c, node, project)),
        Condition::Any { of } => of.iter().any(|c| eval_condition(c, node, project)),
        Condition::Not { of } => !eval_condition(of, node, project),
    }
}

/// Port is "satisfied" if either (a) it has a wired incoming edge, or
/// (b) a non-null body literal drives it (`port_literals`, where the
/// enrich normalization homes every port-driving value). This covers
/// `Llm { prompt: "hi" }` where prompt is provided by a literal rather
/// than a wire.
fn input_satisfied(node: &NodeDefinition, project: &ProjectDefinition, port: &str) -> bool {
    if has_incoming_edge(node, project, port) {
        return true;
    }
    node.port_literals
        .get(port)
        .map(|v| !v.is_null())
        .unwrap_or(false)
}

fn has_incoming_edge(node: &NodeDefinition, project: &ProjectDefinition, port: &str) -> bool {
    project
        .edges
        .iter()
        .any(|e| e.target == node.id && e.target_handle.as_deref() == Some(port))
}

fn is_nonempty(v: Option<&serde_json::Value>) -> bool {
    match v {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::String(s)) => !s.trim().is_empty(),
        Some(serde_json::Value::Array(a)) => !a.is_empty(),
        Some(serde_json::Value::Object(o)) => !o.is_empty(),
        Some(_) => true,
    }
}

fn emit_rule_diagnostic(node: &NodeDefinition, rule: &ValidationRule, out: &mut Vec<Diagnostic>) {
    let span = node.header_span_or_default();
    let severity = match rule.then.severity {
        RuleSeverity::Error => Severity::Error,
        RuleSeverity::Warning => Severity::Warning,
        RuleSeverity::Info => Severity::Info,
        RuleSeverity::Hint => Severity::Hint,
    };
    let message = interpolate(&rule.then.message, node, &rule.then);
    let code = match rule.then.level {
        ValidationLevel::Structural => "rule-structural",
        ValidationLevel::Runtime => "rule-runtime",
    };
    out.push(Diagnostic::at(span, severity, code, message));
}

/// Replace `{id}`, `{port}`, `{field}` placeholders in the rule
/// message with concrete values from the context.
fn interpolate(template: &str, node: &NodeDefinition, diag: &RuleDiagnostic) -> String {
    let mut s = template.replace("{id}", &node.id);
    if let Some(p) = &diag.port {
        s = s.replace("{port}", p);
    }
    if let Some(f) = &diag.field {
        s = s.replace("{field}", f);
    }
    s
}

fn push(
    d: &mut Vec<Diagnostic>,
    span: Span,
    severity: Severity,
    code: &str,
    message: impl Into<String>,
) {
    // One construction path: a span -> the four Diagnostic position fields. The
    // span is the offending node/edge/field's own range (its full extent), so
    // validate diagnostics get the same ranged underlines the parse layer does.
    d.push(Diagnostic::at(span, severity, code, message));
}

// ─── group 1: structural integrity ──────────────────────────────────────────

/// duplicate-node-id: two nodes with the same id. After flattening,
/// every node id is required to be globally unique (the parser rejects
/// same-scope dups at parse time, but scope-aware dups may still
/// collide post-flatten due to inline expressions, group renames, or
/// bad manual input).
///
/// v1 ref: weft-parser.ts:4089-4090
fn check_duplicates(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let mut seen: std::collections::HashMap<&str, usize> = Default::default();
    for node in &project.nodes {
        let span = node.header_span_or_default();
        match seen.get(node.id.as_str()) {
            Some(first_line) => push(
                d,
                span,
                Severity::Error,
                "duplicate-node-id",
                format!(
                    "duplicate node id '{}' (first declared at line {})",
                    node.id, first_line
                ),
            ),
            None => {
                seen.insert(&node.id, span.start_line);
            }
        }
    }
}

/// double-driven-port: an input port may have ONE driver. A port that is
/// wired from upstream AND set in the node body (a `port_literals` entry,
/// per the enrich normalization) has two, and which value wins would be
/// invisible at the call site; reject at compile time. Body values carry
/// no field defaults, so a hit is always an explicit assignment. A
/// same-named config FIELD on a braces-closed port is not a
/// driver (it lives in `config`, not `port_literals`) and stays legal.
fn check_double_driven_ports(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for node in &project.nodes {
        for name in node.port_literals.keys() {
            let wired = project
                .edges
                .iter()
                .any(|e| e.target == node.id && e.target_handle.as_deref() == Some(name.as_str()));
            if wired {
                push(
                    d,
                    node.port_literal_spans
                        .get(name)
                        .map(|s| s.span)
                        .unwrap_or_else(|| node.header_span_or_default()),
                    Severity::Error,
                    "double-driven-port",
                    format!(
                        "input '{}' of '{}' has two drivers: it is wired from upstream AND \
                         set in the node body; remove one",
                        name, node.id
                    ),
                );
            }
        }
    }
}

/// unknown-source-node / unknown-target-node: edges must reference
/// nodes that actually exist in the project.
///
/// v1 ref: weft-parser.ts:4467-4470
fn check_edge_node_refs(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let ids: std::collections::HashSet<&str> =
        project.nodes.iter().map(|n| n.id.as_str()).collect();
    for edge in &project.edges {
        let span = edge.span.unwrap_or_default();
        if !ids.contains(edge.source.as_str()) {
            push(
                d,
                span,
                Severity::Error,
                "unknown-source-node",
                format!("edge references unknown source node '{}'", edge.source),
            );
        }
        if !ids.contains(edge.target.as_str()) {
            push(
                d,
                span,
                Severity::Error,
                "unknown-target-node",
                format!("edge references unknown target node '{}'", edge.target),
            );
        }
    }
}

/// scope-reachability: an edge's endpoints must be in reachable
/// scopes. A node at scope ["outer", "inner"] cannot directly wire
/// to a node at scope ["other"]; it must go through a group
/// passthrough. After flattening, the passthroughs exist and edges
/// have been rewired; any remaining cross-scope edge is a leak.
///
/// v1 ref: weft-parser.ts:4492-4495
///
/// Post-flatten check: an edge is valid iff for each endpoint, the
/// other endpoint is either in the same scope, the parent scope, or
/// a passthrough. Passthroughs bridge between scopes so we ignore
/// scope checks on edges where either end is a Passthrough.
fn check_scope_reachability(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    for edge in &project.edges {
        let Some(src) = by_id.get(edge.source.as_str()) else {
            continue;
        };
        let Some(tgt) = by_id.get(edge.target.as_str()) else {
            continue;
        };
        if crate::enrich::is_lowering_builtin(&src.node_type)
            || crate::enrich::is_lowering_builtin(&tgt.node_type)
        {
            continue;
        }
        if src.scope == tgt.scope {
            continue;
        }
        let span = edge.span.unwrap_or_default();
        push(
            d,
            span,
            Severity::Error,
            "scope-reachability",
            format!(
                "edge '{}.{} -> {}.{}' crosses scope boundaries without a group or loop boundary",
                edge.source,
                edge.source_handle.as_deref().unwrap_or("?"),
                edge.target,
                edge.target_handle.as_deref().unwrap_or("?"),
            ),
        );
    }
}

// ─── group 2: port resolution ───────────────────────────────────────────────

/// unknown-source-port / unknown-target-port: the edge handles must
/// resolve to real ports on the enriched node. port-typo-suggestion:
/// for each unresolved handle, pick the closest existing port by
/// Levenshtein distance if it's within a threshold.
///
/// v1 ref: weft-parser.ts:4502-4518 (resolution) + `didYouMean` helper
fn check_port_resolution(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    for edge in &project.edges {
        let span = edge.span.unwrap_or_default();
        let Some(src) = by_id.get(edge.source.as_str()) else { continue };
        let Some(tgt) = by_id.get(edge.target.as_str()) else { continue };

        if let Some(handle) = edge.source_handle.as_deref() {
            if !src.outputs.iter().any(|p| p.name == handle) {
                let names: Vec<&str> = src.outputs.iter().map(|p| p.name.as_str()).collect();
                let suggestion = did_you_mean(handle, &names);
                let msg = match suggestion {
                    Some(s) => format!(
                        "node '{}' has no output port '{}'. Did you mean '{}'?",
                        edge.source, handle, s
                    ),
                    None => format!(
                        "node '{}' has no output port '{}'. Available: [{}]",
                        edge.source,
                        handle,
                        names.join(", ")
                    ),
                };
                push(d, span, Severity::Error, "unknown-source-port", msg);
            }
        }

        if let Some(handle) = edge.target_handle.as_deref() {
            if !tgt.inputs.iter().any(|p| p.name == handle) {
                let names: Vec<&str> = tgt.inputs.iter().map(|p| p.name.as_str()).collect();
                let suggestion = did_you_mean(handle, &names);
                let msg = match suggestion {
                    Some(s) => format!(
                        "node '{}' has no input port '{}'. Did you mean '{}'?",
                        edge.target, handle, s
                    ),
                    None => format!(
                        "node '{}' has no input port '{}'. Available: [{}]",
                        edge.target,
                        handle,
                        names.join(", ")
                    ),
                };
                push(d, span, Severity::Error, "unknown-target-port", msg);
            }
        }
    }

    // duplicate-input-port (1:1 rule): for each (target_node,
    // target_port), at most one edge. Sits here because it piggybacks
    // on port resolution.
    let mut seen: std::collections::HashMap<(String, String), usize> = Default::default();
    for edge in &project.edges {
        let Some(handle) = edge.target_handle.as_deref() else { continue };
        let key = (edge.target.clone(), handle.to_string());
        let span = edge.span.unwrap_or_default();
        if let Some(first) = seen.get(&key) {
            push(
                d,
                span,
                Severity::Error,
                "duplicate-input-port",
                format!(
                    "port '{}.{}' already has a driver at line {}; an input can be fed by exactly one edge",
                    edge.target, handle, first
                ),
            );
        } else {
            seen.insert(key, span.start_line);
        }
    }
}

/// Return the element of `candidates` with Levenshtein distance <= 2
/// from `input`, if any. Tie-breaks on the first candidate.
fn did_you_mean<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(usize, &str)> = None;
    for c in candidates {
        let d = levenshtein(input, c);
        if d <= 2 && best.is_none_or(|(bd, _)| d < bd) {
            best = Some((d, c));
        }
    }
    best.map(|(_, s)| s)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let m = a.len();
    let n = b.len();
    if m == 0 { return n; }
    if n == 0 { return m; }
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0usize; n + 1];
    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = *[
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost,
            ]
            .iter()
            .min()
            .unwrap();
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

// ─── group 3: type checking ─────────────────────────────────────────────────

/// type-mismatch: edge source type must be assignable to target type.
/// unresolved-typevar: a connected port still has a TypeVar after
///   enrichment (couldn't be resolved from context).
/// must-override-unmet: a connected port is MustOverride, meaning the
///   user must declare a concrete type in weft source.
///
/// v1 refs: weft-parser.ts:3936-3940 (mismatch), 3962-3981 (typevar +
/// must-override).
fn check_type_compat(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    for edge in &project.edges {
        let span = edge.span.unwrap_or_default();
        let Some(src) = by_id.get(edge.source.as_str()) else { continue };
        let Some(tgt) = by_id.get(edge.target.as_str()) else { continue };
        let Some(src_port) = src
            .outputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref())
        else { continue };
        let Some(tgt_port) = tgt
            .inputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.target_handle.as_deref())
        else { continue };

        // An edge onto a non-wireable input is illegal as a whole;
        // input-not-wireable (check_port_coverage) owns that situation,
        // and stacking type-level diagnostics on an edge that cannot
        // exist would bury the real cause.
        if !tgt_port.exposure.wireable() {
            continue;
        }

        // A stream source into an unresolved NON-STREAM target: the
        // `generator-into-generic-port` rule (check_generator_wiring)
        // owns this edge WHOLE, MustOverride and TypeVar alike; its
        // predicate is the same `contains_unresolved_leaf`, so the two
        // cannot drift. Stacking must-override-unmet or
        // unresolved-typevar on top would give contradictory advice
        // ("declare a concrete type" vs "declare Generator[T]"). A
        // stream-typed target is NOT that rule's edge (it skips
        // generator-to-generator pairs): enrich binds its element
        // type element-wise, so it falls through here and reports
        // only when the element is genuinely unresolvable (a
        // `Generator[T]` port with nothing concrete upstream).
        if src_port.port_type.as_generator().is_some()
            && tgt_port.port_type.as_generator().is_none()
            && tgt_port.port_type.contains_unresolved_leaf()
        {
            continue;
        }
        // The mirror direction the same rule owns: a generic source
        // wired into a stream-typed target.
        if tgt_port.port_type.as_generator().is_some()
            && src_port.port_type.as_generator().is_none()
            && src_port.port_type.contains_unresolved_leaf()
        {
            continue;
        }

        if src_port.port_type.is_must_override() {
            push(d, span, Severity::Error, "must-override-unmet",
                format!(
                    "source port '{}.{}' is MustOverride. Declare a concrete type in weft source.",
                    edge.source, src_port.name,
                ));
            continue;
        }
        if tgt_port.port_type.is_must_override() {
            push(d, span, Severity::Error, "must-override-unmet",
                format!(
                    "target port '{}.{}' is MustOverride. Declare a concrete type in weft source.",
                    edge.target, tgt_port.name,
                ));
            continue;
        }

        // A nested unresolved leaf (`A | T`, `List[T]`) is as unusable as a
        // bare one: the runtime gate cannot validate against it.
        if src_port.port_type.contains_unresolved_leaf() {
            push(d, span, Severity::Error, "unresolved-typevar",
                format!(
                    "source port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    edge.source, src_port.name, src_port.port_type,
                ));
            continue;
        }
        if tgt_port.port_type.contains_unresolved_leaf() {
            push(d, span, Severity::Error, "unresolved-typevar",
                format!(
                    "target port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    edge.target, tgt_port.name, tgt_port.port_type,
                ));
            continue;
        }

        if !weft_core::weft_type::WeftType::is_compatible(&src_port.port_type, &tgt_port.port_type) {
            // When the pair is refused as a wire but the Cast table
            // accepts it (a JsonDict claiming a declared type, text
            // that would parse), name the door.
            let cast_hint = if weft_core::weft_type::WeftType::cast_allowed(
                &src_port.port_type,
                &tgt_port.port_type,
            )
            .is_ok()
            {
                "; wire it through a Cast node to convert and validate the value"
            } else {
                ""
            };
            push(d, span, Severity::Error, "type-mismatch",
                format!(
                    "cannot connect '{}.{}: {}' to '{}.{}: {}'{cast_hint}",
                    edge.source, src_port.name, src_port.port_type,
                    edge.target, tgt_port.name, tgt_port.port_type,
                ));
        }
    }

    // cast-not-allowed: any node declaring the checked-cast semantic
    // (`features.castPorts`, metadata-declared; the compiler never
    // names a node type) must have a resolved conversion pair the one
    // weft-core table accepts (`WeftType::cast_allowed`). The runtime
    // cast is the same door, so an allowed pair either converts or
    // fails loudly at run time with a field-level message, and a
    // nonsense pair (JsonDict -> Number) dies here instead.
    for node in &project.nodes {
        let Some(cast) = &node.features.cast_ports else { continue };
        let input = node.inputs.iter().find(|p| p.name == cast.input);
        let output = node.outputs.iter().find(|p| p.name == cast.output);
        let (Some(input), Some(output)) = (input, output) else { continue };
        if let Err(e) = weft_core::weft_type::WeftType::cast_allowed(
            &input.port_type,
            &output.port_type,
        ) {
            push(d, node.header_span_or_default(), Severity::Error, "cast-not-allowed", e);
        }
    }

    // config-type-mismatch: walk each node's config fields vs port
    // types. (Port-type-override incompatibility is caught upstream by
    // the connection type check on the overridden port.)
    for node in &project.nodes {
        // Literal values live in TWO stores by written form: wireable
        // drivers in `port_literals`, `config`-exposure braces values in
        // `config`. Type-check both against the declared input type.
        let literal_values = node
            .port_literals
            .iter()
            .map(|(k, v)| (k, v, node.port_literal_spans.get(k).map(|s| s.span)))
            .chain(
                node.config
                    .as_object()
                    .into_iter()
                    .flatten()
                    .map(|(k, v)| (k, v, node.config_spans.get(k).map(|s| s.span))),
            );
        for (key, value, span) in literal_values {
            let Some(input) = node.inputs.iter().find(|p| p.name == *key) else { continue };
            // Only type-check plain-data literals: braces values on
            // `all`/`config` inputs. On assignment-only inputs (files,
            // TypeVars, MustOverride) the written value is a
            // marker/handle whose inferred JSON shape does not match the
            // input type by construction.
            if !input.exposure.allows_braces_literal() { continue }
            // A widget-shaped handle (a remote_select's `{id, label}`
            // pick, an access widget's `{id, identity}` connect handle)
            // is legal on its input by the widget's own contract; the
            // runtime unwraps it when the input bag is built. Only the
            // OBJECT form is exempt: a plain value (a pasted raw id
            // string) still type-checks normally.
            if value.is_object()
                && matches!(
                    input.widget,
                    Some(weft_core::node::Widget::RemoteSelect { .. })
                        | Some(weft_core::node::Widget::Access { .. })
                )
            {
                continue;
            }
            // The culprit is the literal itself; fall back to the node
            // header if the span is missing.
            let span = span.or(node.header_span).unwrap_or_default();
            let inferred = weft_core::weft_type::WeftType::infer(value);
            // Enrich already cast every castable literal in place, so a
            // shape still mismatching here is a GENUINE type error iff
            // the cast refuses too; the refusal reason names WHY. A
            // still-castable value only occurs when a caller validates
            // an un-enriched graph, where the value is fine (the
            // compile pass will cast it), so no diagnostic fires.
            if !weft_core::weft_type::WeftType::is_compatible(&inferred, &input.port_type) {
                if let Err(why) = input.port_type.cast_value(value) {
                    push(d, span, Severity::Error, "config-type-mismatch",
                        format!(
                            "config '{}.{}: {}' incompatible with input type '{}': {}",
                            node.id, key, inferred, input.port_type, why,
                        ));
                }
            }
        }
    }
}

// ─── group 4: port coverage ─────────────────────────────────────────────────

/// required-port-unmet: required input with no driver (no edge + no
///   body value).
/// require-one-of-unmet: each @require_one_of group must have at
///   least one port driven.
/// port-literal-placement: a literal drives a port at a placement the
///   port forbids (a braces value where only the assignment form is
///   legal, or any literal on a wires-only port).
/// undeclared-port-no-custom: node doesn't support canAddInputPorts
///   but config has a key not matching any declared input.
///
/// v1 refs: 3428-3470 (required + require_one_of), 3502-3506
/// (wired-only), 4313-4319 (undeclared-no-custom).
fn check_port_coverage(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    d: &mut Vec<Diagnostic>,
) {
    // Build "is this (node, port) driven by an edge?" lookup once.
    let driven: std::collections::HashSet<(String, String)> = project
        .edges
        .iter()
        .filter_map(|e| Some((e.target.clone(), e.target_handle.clone()?)))
        .collect();

    for node in &project.nodes {
        if node.node_type == "Passthrough" {
            continue;
        }
        let span = node.header_span_or_default();

        for input in &node.inputs {
            let exposure = input.exposure;
            let has_edge = driven.contains(&(node.id.clone(), input.name.clone()));
            // Body-supplied driving value, normalized by enrich into
            // port_literals (braces on `all` inputs, assignment
            // statements on any input).
            let has_literal = node
                .port_literals
                .get(&input.name)
                .map(|v| !v.is_null())
                .unwrap_or(false);
            // A `config`-exposure input's braces value stays in `config`.
            let braces_value = node
                .config
                .get(&input.name)
                .map(|v| !v.is_null())
                .unwrap_or(false);

            // input-not-wireable: a `config` input is design-time
            // configuration; no wire (edge, braces endpoint, inline
            // expression: they all lower to edges) may drive it.
            if has_edge && !exposure.wireable() {
                let edge_span = project
                    .edges
                    .iter()
                    .find(|e| e.target == node.id && e.target_handle.as_deref() == Some(&input.name))
                    .and_then(|e| e.span)
                    .unwrap_or(span);
                push(
                    d,
                    edge_span,
                    Severity::Error,
                    "input-not-wireable",
                    format!(
                        "input '{}.{}' is configuration-only: it cannot be driven by a \
                         wire. Set it in the config braces instead",
                        node.id, input.name
                    ),
                );
            }

            // A wires-only input takes no literal in ANY form (this
            // catches the assignment statement, `id.input = ...`). A
            // `config` input takes its literal only in the braces, so an
            // assignment statement on it is misplaced too. When the
            // input is ALSO wired, double-driven-port owns the situation
            // (its "remove one driver" message is the accurate one).
            if has_literal && !has_edge && !exposure.allows_assignment_literal() {
                let lit_span = node.port_literal_spans.get(&input.name).map(|s| s.span).unwrap_or(span);
                let message = if exposure.allows_braces_literal() {
                    format!(
                        "input '{}.{}' takes a literal only in the config braces: write \
                         it as {{ {}: ... }} on the node",
                        node.id, input.name, input.name
                    )
                } else {
                    format!(
                        "input '{}.{}' takes no literal: wire it from another node",
                        node.id, input.name
                    )
                };
                push(d, lit_span, Severity::Error, "port-literal-placement", message);
            }

            // required + default = satisfiable: the runtime supplies the
            // default when nothing else drives the input.
            if input.required && !has_edge && !has_literal && !braces_value && input.default.is_none() {
                push(
                    d,
                    span,
                    Severity::Error,
                    "required-port-unmet",
                    format!(
                        "required input '{}.{}' has no driver (no edge, no body value, no default)",
                        node.id, input.name
                    ),
                );
            }

            // A braces value naming an input whose exposure excludes the
            // braces form is a mis-aimed literal (it stayed in `config`
            // because enrich only homes `all`-exposure braces values in
            // port_literals).
            if braces_value && !exposure.allows_braces_literal() {
                let cfg_span = node.config_spans.get(&input.name).map(|s| s.span).unwrap_or(span);
                let message = if exposure.allows_literal() {
                    format!(
                        "input '{}.{}' takes a literal only as an assignment: the braces \
                         form cannot drive it. Wire it or write {}.{} = ...",
                        node.id, input.name, node.id, input.name
                    )
                } else {
                    format!(
                        "input '{}.{}' takes no literal: wire it from another node",
                        node.id, input.name
                    )
                };
                push(d, cfg_span, Severity::Error, "port-literal-placement", message);
            }

            // The widget-level literal checks: the value is wherever the
            // input's written form homed it, read only from a home the
            // exposure legally allows. A MIS-PLACED literal (a braces
            // value on an assignment-only input, an assignment on a
            // config input) already gets port-literal-placement; running
            // the widget checks on it too would stack a second error on
            // one mistake.
            let literal_value = node
                .port_literals
                .get(&input.name)
                .filter(|_| exposure.allows_assignment_literal())
                .or_else(|| {
                    node.config
                        .get(&input.name)
                        .filter(|_| exposure.allows_braces_literal())
                });
            let literal_span = || {
                node.port_literal_spans
                    .get(&input.name)
                    .or_else(|| node.config_spans.get(&input.name))
                    .map(|s| s.span)
                    .unwrap_or(span)
            };
            // literal-out-of-range: a number widget's min/max bound
            // the literal at compile time (the editor clamps on
            // blur; this is the backstop for hand-written source).
            if let Some(weft_core::node::Widget::Number { min, max, .. }) = &input.widget {
                // Range-check the CAST value so a stringified number
                // in an un-enriched graph is bounded consistently
                // with the type check. An uncastable value is not
                // silently passed: config-type-mismatch reports it.
                let cast_num = literal_value
                    .and_then(|v| input.port_type.cast_value(v).ok())
                    .and_then(|v| v.as_f64());
                if let Some(n) = cast_num {
                    if min.is_some_and(|m| n < m) || max.is_some_and(|m| n > m) {
                        push(
                            d,
                            literal_span(),
                            Severity::Error,
                            "literal-out-of-range",
                            format!(
                                "input '{}.{}': {} is outside the allowed range [{}, {}]",
                                node.id,
                                input.name,
                                n,
                                min.map_or("-inf".into(), |m| m.to_string()),
                                max.map_or("inf".into(), |m| m.to_string()),
                            ),
                        );
                    }
                }
            }
        }

        // @require_one_of: each inner group must have at least one
        // satisfied (driven or configured-non-null) input.
        for group in &node.features.one_of_required {
            if group.is_empty() {
                continue;
            }
            let any_met = group.iter().any(|port_name| {
                driven.contains(&(node.id.clone(), port_name.clone()))
                    || node
                        .port_literals
                        .get(port_name)
                        .map(|v| !v.is_null())
                        .unwrap_or(false)
            });
            if !any_met {
                push(
                    d,
                    span,
                    Severity::Error,
                    "require-one-of-unmet",
                    format!(
                        "node '{}' declares @require_one_of({}) but none is driven",
                        node.id,
                        group.join(", ")
                    ),
                );
            }
        }

        // undeclared-port-no-custom: if the node can't accept custom
        // inputs (features.can_add_input_ports == false), every
        // config key must name a declared input or output (literal-
        // emitter nodes like Text drive their output via config). A
        // node that derives ports from a config list holds that list
        // under a key it DECLARES as a config input (the catalog load
        // refuses `portsFromConfig` naming an undeclared input), so it
        // needs no exemption here. LoopIn/LoopOut carry the loop config
        // (`over`, `parallel`, ...) in their config blob;
        // check_loop_config validates those keys exhaustively against
        // its own known-keys list, so exactly those two types are
        // exempt here (matched by node_type, the same key
        // check_loop_config selects on, so the two checks partition the
        // space with no gap).
        // An unknown node type (a lenient-parse placeholder in the
        // editor; a hard enrich error in a strict compile) has no
        // declared inputs at all, so every config key would
        // false-positive here.
        if !node.features.can_add_input_ports
            && !matches!(node.node_type.as_str(), "LoopIn" | "LoopOut")
            && catalog.lookup(&node.node_type).is_some()
        {
            let Some(obj) = node.config.as_object() else { continue };
            let known_inputs: std::collections::HashSet<&str> =
                node.inputs.iter().map(|p| p.name.as_str()).collect();
            let known_outputs: std::collections::HashSet<&str> =
                node.outputs.iter().map(|p| p.name.as_str()).collect();
            for key in obj.keys() {
                // Compiler/editor plumbing keys (`_`-reserved,
                // `parentId`) are validated by the parser / merged at
                // flatten time and never need to match a declared
                // input.
                if weft_core::project::is_internal_config_key(key) {
                    continue;
                }
                if known_inputs.contains(key.as_str())
                    || known_outputs.contains(key.as_str())
                {
                    continue;
                }
                push(
                    d,
                    node.config_spans.get(key).map(|s| s.span).unwrap_or(span),
                    Severity::Error,
                    "undeclared-port-no-custom",
                    format!(
                        "node '{}' does not accept custom inputs; config key '{}' doesn't match any declared input or output",
                        node.id, key
                    ),
                );
            }
        }

        // duplicate-port: two ports of the same name on the same side.
        // Config-derived ports (a form's fields, a switch's cases) are
        // where this comes from in practice, since a catalog's own port
        // list is checked at load and the source-vs-catalog merge is
        // by name. Detection runs PER SIDE: a node legitimately has an
        // input AND an output sharing a name (a passthrough's
        // `value`/`value`), so crossing the two would false-positive
        // every one of them.
        let sides: [Vec<&str>; 2] = [
            node.inputs.iter().map(|p| p.name.as_str()).collect(),
            node.outputs.iter().map(|p| p.name.as_str()).collect(),
        ];
        for names in sides {
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for name in names {
                if !seen.insert(name) {
                    push(
                        d,
                        span,
                        Severity::Error,
                        "duplicate-port",
                        format!("node '{}' has two ports named '{}' on one side", node.id, name),
                    );
                }
            }
        }
    }

    // Note: literal-to-output-port from the v1 audit is enforced at
    // parse time: a `node.port = "literal"` line is lowered to a config
    // field on the node (not an edge), which the port checks reject when
    // the target port is output-only. No extra check needed at validate.
}

/// The entries a node derives its ports from (a form's `fields`, a
/// switch's `cases`) have to say what they claim to say, or the ports
/// they were supposed to make just are not there and every wire to them
/// reads as a typo.
///
/// Rules, all about one entry list:
///  - the value under the key is a LIST of objects.
///  - every entry names a `kind` this node offers.
///  - every entry names its port, under the key its spec asks for.
///  - every entry carries only keys its spec declares, so a mistyped
///    test (`euqals`) fails here instead of quietly becoming a case
///    that matches everything.
///  - every test carries the shape it declares: a value the matched
///    input could hold, a list of them, a number, or a regular
///    expression that compiles.
///  - a CATCH-ALL entry (one carrying no test at all) is unique and
///    last, since the entries are matched in order and anything after it
///    could never be reached.
fn check_config_derived_ports(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    d: &mut Vec<Diagnostic>,
) {
    for node in &project.nodes {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        let Some(ports_from_config) = &meta.ports_from_config else { continue };
        let span = node.header_span_or_default();
        let key_span = node
            .config_spans
            .get(&ports_from_config.field)
            .map(|s| s.span)
            .unwrap_or(span);
        let Some(raw) = node.config.get(&ports_from_config.field) else { continue };
        let Some(entries) = raw.as_array() else {
            push(
                d,
                key_span,
                Severity::Error,
                "config-ports-not-a-list",
                format!(
                    "node '{}': '{}' holds the list this node's ports come from, so it must be \
                     a list",
                    node.id, ports_from_config.field
                ),
            );
            continue;
        };

        let kinds: Vec<&str> = ports_from_config.specs.iter().map(|s| s.kind.as_str()).collect();
        let mut catch_all_at: Option<usize> = None;
        for (index, entry) in entries.iter().enumerate() {
            let Some(obj) = entry.as_object() else {
                push(
                    d,
                    key_span,
                    Severity::Error,
                    "config-entry-not-an-object",
                    format!(
                        "node '{}': {} of '{}' is not an object",
                        node.id,
                        entry_label(index, None),
                        ports_from_config.field
                    ),
                );
                continue;
            };
            let kind = obj.get("kind").and_then(|v| v.as_str()).unwrap_or_default();
            let Some(spec) = ports_from_config.spec_for(kind) else {
                push(
                    d,
                    key_span,
                    Severity::Error,
                    "unknown-config-entry-kind",
                    format!(
                        "node '{}': {} of '{}' has kind '{}', which {} does not offer \
                         (it takes: {})",
                        node.id,
                        entry_label(index, None),
                        ports_from_config.field,
                        kind,
                        node.node_type,
                        kinds.join(", ")
                    ),
                );
                continue;
            };
            let port_name = obj.get(&spec.key_field).and_then(|v| v.as_str()).unwrap_or_default();
            if port_name.is_empty() {
                push(
                    d,
                    key_span,
                    Severity::Error,
                    "config-entry-without-a-port",
                    format!(
                        "node '{}': {} of '{}' needs a '{}' naming the port it adds",
                        node.id,
                        entry_label(index, None),
                        ports_from_config.field,
                        spec.key_field
                    ),
                );
                continue;
            }
            // Every key an entry may carry, so anything else is a
            // mistyped one, null-valued or not: null-is-absent applies
            // to a DECLARED field's value (the loop below), never to
            // the key itself, or a typo'd key with a cleared value
            // would vanish from diagnostics.
            let mut allowed: Vec<&str> = vec!["kind", spec.key_field.as_str()];
            allowed.extend(spec.fields.iter().map(|f| f.key.as_str()));
            let unknown: Vec<&String> =
                obj.keys().filter(|key| !allowed.contains(&key.as_str())).collect();
            for key in &unknown {
                push(
                    d,
                    key_span,
                    Severity::Error,
                    "unknown-config-entry-key",
                    format!(
                        "node '{}': {} of '{}' carries '{}', which a '{}' entry \
                         does not take (it takes: {})",
                        node.id,
                        entry_label(index, Some(port_name)),
                        ports_from_config.field,
                        key,
                        spec.kind,
                        allowed.join(", ")
                    ),
                );
            }
            // What the entries are matched against, when they compete.
            // Its type is what holds a test to a value it could see.
            let matched_type = ports_from_config
                .match_input
                .as_ref()
                .and_then(|name| node.inputs.iter().find(|p| p.name == *name))
                .map(|p| p.port_type.clone());
            for field in &spec.fields {
                // A present `null` counts as absent, the same rule record
                // types apply to their optional fields: the editor clears a
                // box by writing null, and the runtime reads null as "not
                // filled in" (a form field's label falls back to its key).
                // SYNC: null-is-absent <->
                //       packages/weft-graph/src/webview/lib/utils/port-specs.ts (hasValue),
                //       catalog/human/form_helpers.rs (build_form_fields)
                match obj.get(field.key.as_str()).filter(|v| !v.is_null()) {
                    Some(value) => {
                        // Where the list IS the choice set, an empty one
                        // on a required field chose nothing: the branch
                        // could never fire (a `valueList` `in`) or the
                        // human would get an empty dropdown. Where a
                        // list is one VALUE being matched, `[]` is a
                        // legitimate value and says nothing here.
                        // SYNC: empty-list-is-nothing-chosen <->
                        //       crates/weft-core/src/node.rs
                        //       (SpecField::empty_list_means_nothing_chosen),
                        //       packages/weft-graph/src/webview/lib/utils/port-specs.ts
                        //       (isEmptyChoiceSet)
                        let empty_choice_set = field.required
                            && field.empty_list_means_nothing_chosen()
                            && value.as_array().is_some_and(|a| a.is_empty());
                        if empty_choice_set {
                            push(
                                d,
                                key_span,
                                Severity::Error,
                                "config-entry-bad-value",
                                format!(
                                    "node '{}': {} of '{}' sets `{}` to an empty list, \
                                     which chooses nothing, and a '{}' needs at least one",
                                    node.id,
                                    entry_label(index, Some(port_name)),
                                    ports_from_config.field,
                                    field.key,
                                    spec.kind
                                ),
                            );
                        } else if let Some(problem) =
                            spec_field_problem(field, value, matched_type.as_ref())
                        {
                            push(
                                d,
                                key_span,
                                Severity::Error,
                                "config-entry-bad-value",
                                format!(
                                    "node '{}': {} of '{}' sets `{}`, and {}",
                                    node.id,
                                    entry_label(index, Some(port_name)),
                                    ports_from_config.field,
                                    field.key,
                                    problem
                                ),
                            );
                        }
                    }
                    None if field.required => push(
                        d,
                        key_span,
                        Severity::Error,
                        "config-entry-missing-value",
                        format!(
                            "node '{}': {} of '{}' is a '{}', which needs a '{}'",
                            node.id,
                            entry_label(index, Some(port_name)),
                            ports_from_config.field,
                            spec.kind,
                            field.key
                        ),
                    ),
                    None => {}
                }
            }
            // A kind that takes anything is the branch reached when no
            // earlier entry matched, so a second one can never fire.
            if spec.catch_all {
                match catch_all_at {
                    Some(first) => push(
                        d,
                        key_span,
                        Severity::Error,
                        "duplicate-catch-all",
                        format!(
                            "node '{}': entries {} and {} of '{}' both match anything; the \
                             second can never be reached",
                            node.id,
                            first + 1,
                            index + 1,
                            ports_from_config.field
                        ),
                    ),
                    None => catch_all_at = Some(index),
                }
            }
        }
        if let Some(at) = catch_all_at {
            if at + 1 != entries.len() {
                push(
                    d,
                    key_span,
                    Severity::Error,
                    "catch-all-not-last",
                    format!(
                        "node '{}': entry {} of '{}' matches anything, so the {} after it can \
                         never be reached. Write it last",
                        node.id,
                        at + 1,
                        ports_from_config.field,
                        entries.len() - at - 1
                    ),
                );
            }
        }
    }
}

/// How a diagnostic names one entry of a config-derived list: its
/// position, plus the port it names once the entry has gotten that far
/// (two entries can carry the same port name, so the position stays).
fn entry_label(index: usize, port_name: Option<&str>) -> String {
    match port_name {
        // `None`: the diagnostic fires before the entry has named its
        // port, so there is nothing to quote.
        None => format!("entry {}", index + 1),
        Some(name) => format!("entry {} ('{}')", index + 1, name),
    }
}

/// What is wrong with one value a kind asked for, or `None` when it
/// fits. Reads only the SHAPE the spec declares, so the compiler holds
/// it honest without knowing what any node means by the key.
fn spec_field_problem(
    field: &weft_core::node::SpecField,
    value: &serde_json::Value,
    matched_type: Option<&WeftType>,
) -> Option<String> {
    use weft_core::node::SpecShape;
    // Every shape below `Typed` measures the value against the input
    // named by `matchInput`. Manifest validation guarantees that input
    // exists whenever a spec declares such a field, so a missing type
    // here is a broken manifest; report it rather than skipping the
    // check silently.
    fn matched(matched_type: Option<&WeftType>) -> Result<&WeftType, String> {
        matched_type.ok_or_else(|| {
            "its spec matches entries against an input this node does not declare \
             (broken node metadata: `matchInput` names no input)"
                .to_string()
        })
    }
    match field.shape {
        SpecShape::Typed => {
            let declared = field.value_type.as_ref()?;
            (!declared.accepts_runtime_value(value))
                .then(|| format!("{value} is not a {declared}"))
        }
        SpecShape::Value => {
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            (!ty.accepts_runtime_value(value))
                .then(|| format!("{value} is not a {ty}, which is what it is matched against"))
        }
        SpecShape::ValueList => {
            let Some(items) = value.as_array() else {
                return Some(format!("{value} is not a list of values to match against"));
            };
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            items.iter().find(|item| !ty.accepts_runtime_value(item)).map(|item| {
                format!("{item} is not a {ty}, which is what it is matched against")
            })
        }
        SpecShape::Number => {
            if !value.is_number() {
                return Some(format!("{value} is not a number, so there is nothing to compare"));
            }
            // Comparing a number against something that never holds one
            // is a branch that can never be taken.
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            (!ty.is_unresolved() && !ty.accepts_runtime_value(&serde_json::json!(0)))
                .then(|| format!("what it matches against is a {ty}, never a number"))
        }
        SpecShape::Element => {
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            if ty.is_unresolved() {
                return None;
            }
            match ty.structural() {
                WeftType::List(item) => (!item.accepts_runtime_value(value))
                    .then(|| format!("{value} is not a {item}, so no {ty} could hold it")),
                WeftType::Primitive(weft_core::weft_type::WeftPrimitive::String) => value
                    .as_str()
                    .is_none()
                    .then(|| format!("{value} is not text, so no text could contain it")),
                other => Some(format!(
                    "there is nothing to look inside: what it matches against is a {other}, \
                     not a list or text"
                )),
            }
        }
        SpecShape::Regex => match value.as_str() {
            None => Some(format!("{value} is not a regular expression")),
            Some(pattern) => regex::Regex::new(pattern)
                .err()
                .map(|e| format!("{value} is not a regular expression that compiles: {e}")),
        },
    }
}

// ─── group 5: loop config validation ─────────────────────────────────────────

/// Validate the config block on each `Loop` decl (via its `LoopIn` /
/// `LoopOut` boundary nodes). Rules per the plan:
///  - `parallel` defaults to false (sequential) when omitted.
///  - `over` / `carry` reference declared ports.
///  - `parallel: true` AND `carry` non-empty → `parallel-with-carry`.
///  - `parallel: true` AND `over` empty → `parallel-without-over`.
///  - `parallel: true` AND `self.done = ...` in body → `parallel-with-done`.
///  - Port in both `over` and `carry` → `over-and-carry-overlap`.
///  - Reserved port names `index` (input) / `done` (output) → `reserved-port-name`.
///  - Gather output declared as `List[T]` instead of `List[T | Null]` →
///    `gather-output-must-be-nullable`.
///  - Boundary unpaired → `loop-boundary-unpaired`.
fn check_loop_config(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use std::collections::{HashMap, HashSet};

    // Collect LoopIn / LoopOut node pairs keyed by their shared group id.
    let mut ins: HashMap<&str, &NodeDefinition> = HashMap::new();
    let mut outs: HashMap<&str, &NodeDefinition> = HashMap::new();
    for n in &project.nodes {
        let Some(gb) = &n.group_boundary else { continue };
        match n.node_type.as_str() {
            "LoopIn" => { ins.insert(gb.group_id.as_str(), n); }
            "LoopOut" => { outs.insert(gb.group_id.as_str(), n); }
            _ => {}
        }
    }
    for (gid, in_node) in &ins {
        if !outs.contains_key(gid) {
            push(d, in_node.header_span_or_default(), Severity::Error, "loop-boundary-unpaired",
                format!("loop '{gid}' has LoopIn but no matching LoopOut"));
        }
    }
    for (gid, out_node) in &outs {
        if !ins.contains_key(gid) {
            push(d, out_node.header_span_or_default(), Severity::Error, "loop-boundary-unpaired",
                format!("loop '{gid}' has LoopOut but no matching LoopIn"));
        }
    }

    for (gid, in_node) in &ins {
        let Some(out_node) = outs.get(gid).copied() else { continue };
        let cfg = &in_node.config;
        let span = in_node.header_span_or_default();
        let span_for = |key: &str| -> weft_core::project::Span {
            in_node
                .config_spans
                .get(key)
                .map(|cs| cs.span)
                .unwrap_or_else(|| span)
        };

        // Unknown config keys are rejected loudly: a typo'd knob
        // (`max_itres: 10`) silently running the loop uncapped is
        // exactly the masked-bug class the language forbids.
        // `parentId` is the compiler-internal boundary pointer merged
        // in at flatten time, never a user key (lowering rejects a
        // user-written `parentId` before the merge).
        const KNOWN_LOOP_KEYS: &[&str] =
            &["parentId", "parallel", "over", "carry", "max_iters", "trim_on_mismatch"];
        if let Some(obj) = cfg.as_object() {
            for key in obj.keys() {
                if !KNOWN_LOOP_KEYS.contains(&key.as_str()) {
                    push(d, span_for(key), Severity::Error, "loop-unknown-config-field",
                        format!(
                            "loop '{gid}': unknown config field '{key}' (known: parallel, \
                             over, carry, max_iters, trim_on_mismatch)"
                        ));
                }
            }
        }

        // `parallel` defaults to false (sequential, the safer mode:
        // carry / self.done work, no ordering surprises); the flatten
        // step materializes the default, so it is always present
        // here. A wrong-typed value (`parallel: "true"`) is an ERROR,
        // never a coercion: silently running sequential would also
        // silently skip every parallel-interplay rule below.
        let parallel: bool = match cfg.get("parallel") {
            Some(serde_json::Value::Bool(b)) => *b,
            Some(other) => {
                push(d, span_for("parallel"), Severity::Error, "loop-parallel-not-boolean",
                    format!("loop '{gid}': `parallel` must be a boolean literal (got {other})"));
                continue;
            }
            // Absent means the flatten step did NOT materialize the
            // default into the LoopIn config. The default lives in ONE
            // place (flatten); validate verifies it is there rather than
            // re-defaulting here, because a silent default would also
            // silently skip every parallel-interplay rule below.
            None => {
                push(d, span_for("parallel"), Severity::Error, "loop-config-missing-parallel",
                    format!("loop '{gid}': internal invariant broken: flatten did not materialize `parallel` into the LoopIn config"));
                continue;
            }
        };

        // `max_iters`: non-negative integer literal when present
        // (`max_iters: 0` is a legal zero-iteration cap).
        if let Some(v) = cfg.get("max_iters") {
            if v.as_u64().is_none() {
                push(d, span_for("max_iters"), Severity::Error, "loop-max-iters-not-integer",
                    format!("loop '{gid}': `max_iters` must be a non-negative integer literal (got {v})"));
            }
        }
        // `trim_on_mismatch`: boolean literal when present.
        if let Some(v) = cfg.get("trim_on_mismatch") {
            if !v.is_boolean() {
                push(d, span_for("trim_on_mismatch"), Severity::Error, "loop-trim-not-boolean",
                    format!("loop '{gid}': `trim_on_mismatch` must be a boolean literal (got {v})"));
            }
        }

        // A malformed over/carry (a scalar value, or non-string
        // entries) is already rejected at lowering time
        // (`read_loop_port_list` pushes [loop-config-malformed]);
        // validate must not pile a second diagnostic
        // (parallel-without-over, unbounded, ...) onto the same
        // mistake. `lists_ok` gates exactly the rules that READ
        // over/carry; everything else (reserved names, parallel-with-
        // done, type checks) still reports in the same compile.
        let well_formed_list = |key: &str| {
            cfg.get(key).is_none_or(|v| {
                v.as_array().is_some_and(|arr| arr.iter().all(|e| e.is_string()))
            })
        };
        let lists_ok = well_formed_list("over") && well_formed_list("carry");
        let over: Vec<String> = read_loop_port_list_vetted(cfg.as_object(), "over");
        let carry: Vec<String> = read_loop_port_list_vetted(cfg.as_object(), "carry");

        if lists_ok && parallel && !carry.is_empty() {
            push(d, span_for("carry"), Severity::Error, "parallel-with-carry",
                format!("loop '{gid}': parallel: true forbids carry ports (carry implies sequential)"));
        }
        if lists_ok && parallel && over.is_empty() {
            push(d, span_for("parallel"), Severity::Error, "parallel-without-over",
                format!("loop '{gid}': parallel: true requires a non-empty 'over' list"));
        }

        // `self.done = ...` writes are detectable here: lowering maps
        // them to an edge whose target is `{loop_id}__out` on port
        // `done`, so a scope-local scan reduces to "any edge whose
        // target is this loop's LoopOut id and target port is `done`".
        // A `self.done` write inside a nested loop's body targets that
        // nested loop's `__out`, never this one, so the check is
        // naturally scope-local.
        let loop_out_id = weft_core::project::boundary_out_id(gid);
        let done_wired = project.edges.iter().any(|e| {
            e.target == loop_out_id && e.target_handle.as_deref() == Some("done")
        });

        // parallel-with-done: in parallel mode, any `self.done` write
        // inside THIS loop's body is rejected.
        if parallel && done_wired {
            push(d, span, Severity::Error, "parallel-with-done",
                format!("loop '{gid}': parallel: true forbids `self.done = ...` connections in the body"));
        }

        // loop-unbounded-no-termination: a SEQUENTIAL loop with no
        // `over` (nothing to exhaust), no `max_iters` (no cap), and no
        // `self.done` write (no vote to stop) is provably infinite.
        // Reject at compile time. A loop that has ANY of the three is
        // the user's own program: trusted, unbounded by the runtime.
        if lists_ok && !parallel && over.is_empty() && cfg.get("max_iters").is_none() && !done_wired {
            push(d, span, Severity::Error, "loop-unbounded-no-termination",
                format!(
                    "loop '{gid}': sequential loop declares no 'over', no 'max_iters', and \
                     never writes `self.done`; it can never terminate. Iterate a list with \
                     `over: [...]`, cap it with `max_iters`, or wire `self.done = ...` in the body."
                ));
        }

        let carry_set: HashSet<&String> = carry.iter().collect();
        if lists_ok {
            for p in &over {
                if carry_set.contains(p) {
                    push(d, span_for("over"), Severity::Error, "over-and-carry-overlap",
                        format!("loop '{gid}': port '{p}' listed in both 'over' and 'carry'"));
                }
            }
        }

        // Reserved port names: 'index' as user input, 'done' as user output.
        for port in &in_node.inputs {
            if port.name == "index" {
                push(d, span, Severity::Error, "reserved-port-name",
                    format!("loop '{gid}': 'index' is reserved (the implicit per-iteration index port)"));
            }
        }
        for port in &out_node.outputs {
            if port.name == "done" {
                push(d, span, Severity::Error, "reserved-port-name",
                    format!("loop '{gid}': 'done' is reserved (the implicit done-vote port)"));
            }
        }

        // Gather outputs must be declared `List[T | Null]`. The
        // reserved name `done` already errored above; flagging its
        // nullability too would double-report one mistake. Which
        // outputs are carries is read off the carry list, so a
        // malformed one skips this rule (`lists_ok`).
        for port in &out_node.outputs {
            if !lists_ok || carry_set.contains(&port.name) || port.name == "done" {
                continue;
            }
            if !is_list_of_nullable(&port.port_type) {
                push(d, span, Severity::Error, "gather-output-must-be-nullable",
                    format!(
                        "loop '{gid}': gather output '{}' must be declared as List[T | Null] (was {}); per-iteration body failures produce null slots",
                        port.name, port.port_type,
                    ));
            }
        }

        // over ports must exist on LoopIn and be List[T] or
        // Generator[T]. A stream in `over` must be ALONE: zipping a
        // stream against a list (or another stream) has no defined
        // count to trim against, and the runtime pulls exactly one
        // stream per loop.
        let mut over_has_stream = false;
        for p in &over {
            match in_node.inputs.iter().find(|x| &x.name == p) {
                None => {
                    push(d, span_for("over"), Severity::Error, "loop-over-unknown-port",
                        format!("loop '{gid}': 'over' references unknown input port '{p}'"));
                }
                Some(port) if port.port_type.as_generator().is_some() => {
                    over_has_stream = true;
                }
                Some(port) => match port.port_type.structural() {
                    weft_core::weft_type::WeftType::List(_) => {}
                    other => {
                        push(d, span_for("over"), Severity::Error, "over-not-a-list",
                            format!(
                                "loop '{gid}': 'over' port '{p}' must be List[T] or \
                                 Generator[T], got {other}"
                            ));
                    }
                },
            }
        }
        if over_has_stream && over.len() > 1 {
            push(d, span_for("over"), Severity::Error, "over-stream-not-alone",
                format!(
                    "loop '{gid}': a Generator port in 'over' must be the ONLY over port \
                     (a loop iterates one stream at a time; zip upstream if you need more)"
                ));
        }
        // A Generator input on the loop that is NOT the over port has
        // no meaning: LoopIn broadcasts every non-over input verbatim
        // into each iteration, and a stream cannot be copied per
        // iteration (it is a live edge with one taker).
        for port in &in_node.inputs {
            if port.port_type.as_generator().is_some() && !over.contains(&port.name) {
                push(d, span, Severity::Error, "generator-not-iterated",
                    format!(
                        "loop '{gid}': Generator input '{}' must be iterated (`over: \
                         [\"{}\"]`); a stream cannot broadcast into the loop body",
                        port.name, port.name
                    ));
            }
        }
        // A stream cannot be carried between iterations either: a
        // carry value round-trips the journal as JSON, and a stream is
        // a live edge.
        for p in &carry {
            let carried_generator = out_node
                .outputs
                .iter()
                .find(|x| &x.name == p)
                .map(|x| x.port_type.as_generator().is_some())
                .unwrap_or(false);
            if carried_generator {
                push(d, span_for("carry"), Severity::Error, "generator-not-carriable",
                    format!(
                        "loop '{gid}': carry port '{p}' is a Generator; a stream is a live \
                         edge between two running nodes and cannot be carried across \
                         iterations"
                    ));
            }
        }
        // Carry ports: the output side is the source of truth; the
        // input side (synthesized by lowering when not user-declared)
        // must mirror it. The single home for carry semantics:
        // lowering only synthesizes, validate reports.
        for p in &carry {
            let Some(out_port) = out_node.outputs.iter().find(|x| &x.name == p) else {
                push(d, span_for("carry"), Severity::Error, "loop-carry-unknown-port",
                    format!("loop '{gid}': 'carry' references unknown output port '{p}'"));
                continue;
            };
            let Some(in_port) = in_node.inputs.iter().find(|x| &x.name == p) else {
                // Unreachable when lowering synthesized the input;
                // reachable if the flattened shape drifted.
                push(d, span_for("carry"), Severity::Error, "loop-carry-unknown-port",
                    format!("loop '{gid}': carry port '{p}' has no matching input on the loop"));
                continue;
            };
            if in_port.port_type != out_port.port_type {
                push(d, span_for("carry"), Severity::Error, "carry-port-type-mismatch",
                    format!(
                        "loop '{gid}': carry port '{p}' declared with mismatched types \
                         (input: {}, output: {}); both sides of a carry port must be the same type",
                        in_port.port_type, out_port.port_type,
                    ));
            }
        }
    }
}

fn is_list_of_nullable(ty: &weft_core::weft_type::WeftType) -> bool {
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    let WeftType::List(inner) = ty else { return false };
    match inner.as_ref() {
        WeftType::Union(members) => members.iter().any(|m| matches!(m, WeftType::Primitive(WeftPrimitive::Null))),
        WeftType::Primitive(WeftPrimitive::Null) => true,
        _ => false,
    }
}

// ─── group 6: warnings ──────────────────────────────────────────────────────

/// orphan-outputs: a non-debug node whose outputs are all unconnected
///   is probably a mistake.
/// no-required-skip: a node with inputs but none marked required will
///   never be skipped, even if all inputs are null. Usually a modeling
///   error (the user wanted at least one to be required).
/// config-null-literal: `key: null` is meaningless.
///
/// v1 refs: 4022-4039 (orphan-outputs), 4048-4068 (no-required-skip),
/// 4296-4299 (config-null-literal).
fn check_warnings(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    // Build "used as source" set.
    let source_nodes: std::collections::HashSet<&str> = project
        .edges
        .iter()
        .map(|e| e.source.as_str())
        .collect();

    for node in &project.nodes {
        if node.node_type == "Passthrough" {
            continue;
        }
        let span = node.header_span_or_default();

        // orphan-outputs: only flag when the node has outputs at all.
        // Nodes like Debug (no outputs) are terminal and exempt, and so is
        // any node marked as an output (`_is_output: true`): it is a
        // declared terminus, its unconsumed ports are the point.
        if !node.outputs.is_empty()
            && !node.is_output()
            && !source_nodes.contains(node.id.as_str())
        {
            push(
                d,
                span,
                Severity::Warning,
                "orphan-outputs",
                format!(
                    "node '{}' produces outputs but none are consumed by downstream nodes",
                    node.id
                ),
            );
        }

        // no-required-skip: only applies when the node has inputs,
        // none are required, no @require_one_of is declared, and the
        // node has outputs. Terminal nodes (no outputs) exist
        // explicitly to run on every invocation (Debug, DLQ, audit
        // sinks); we don't flag them.
        // Only WIREABLE inputs count: a `config`-exposure input is a
        // design-time setting, not upstream data, so it neither makes
        // the node "have inputs" nor satisfies the warning. Nor does
        // `_should_flow`, which every node carries: it is permission,
        // not the data the warning is about.
        let wireable: Vec<_> = node
            .inputs
            .iter()
            .filter(|p| p.exposure.wireable() && p.name != SHOULD_FLOW_PORT)
            .collect();
        // A node whose created inputs are optional BY NATURE (a join,
        // which exists to run on whichever branch survived) is the one
        // shape this warning's advice is wrong for.
        if !wireable.is_empty()
            && !node.outputs.is_empty()
            && wireable.iter().all(|p| !p.required)
            && !node.features.optional_custom_inputs
            && node.features.one_of_required.is_empty()
        {
            push(
                d,
                span,
                Severity::Warning,
                "no-required-skip",
                format!(
                    "node '{}' has no required inputs; it will run even when all upstream values are null. Consider marking one input required or adding @require_one_of.",
                    node.id
                ),
            );
        }

        // config-null-literal: `key: null` in config is almost always
        // a mistake. Null is the default for absent keys.
        if let Some(obj) = node.config.as_object() {
            for (key, v) in obj {
                if v.is_null() {
                    push(
                        d,
                        node.config_spans.get(key).map(|s| s.span).unwrap_or(span),
                        Severity::Warning,
                        "config-null-literal",
                        format!(
                            "config '{}.{}: null' is redundant; omit the key to let the default apply",
                            node.id, key
                        ),
                    );
                }
            }
        }
    }
}

/// no-output / unreachable-node: the project's output set is every
/// node whose `is_output()` resolves to true (Debug defaults to true,
/// any node can set `is_output: true` in its config). Emit:
///   - **Error** if no node resolves as output: the project can
///     never produce anything.
///   - **Warning** on every node that isn't upstream of some output
///     and isn't a trigger (triggers are entry points, not part of
///     the user-visible output DAG).
///
/// Passthroughs (group boundaries) are exempt: they exist to bridge
/// scopes, not as standalone targets. Trigger nodes are exempt:
/// they'd otherwise warn even when they're correctly wired into
/// fire-time subgraphs.
fn check_output_reachability(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    // A component file (an anonymous top-level Group, used via @include) is
    // not a standalone runnable project: its outputs are the group's
    // interface ports, surfaced as the root group's __out Passthrough. Use
    // that as the output set so the no-output / unreachable rules don't fire
    // spuriously when the file is opened on its own.
    let component_out: Option<String> = project
        .groups
        .iter()
        .find(|g| g.parent_group_id.is_none() && g.anonymous)
        .map(|g| weft_core::project::boundary_out_id(&g.id));

    let outputs: Vec<&str> = if let Some(ref out_pt) = component_out {
        // The component's output sink: everything upstream of the group's
        // __out boundary is reachable; no top-level output node is required.
        vec![out_pt.as_str()]
    } else {
        project
            .nodes
            .iter()
            .filter(|n| n.is_output())
            .map(|n| n.id.as_str())
            .collect()
    };

    if outputs.is_empty() {
        // Project-level diagnostic (no single culprit): a default span renders
        // it as a file-level problem.
        push(
            d,
            Span::default(),
            Severity::Error,
            "no-output-node",
            "project has no output node (Debug, or any node with `_is_output: true`). \
             The run will have nothing to produce.",
        );
        return;
    }

    // BFS upstream from every output. The result is the set of
    // nodes that contribute to at least one output.
    let mut reached: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut frontier: Vec<&str> = outputs.clone();
    while let Some(id) = frontier.pop() {
        if !reached.insert(id) {
            continue;
        }
        for edge in &project.edges {
            if edge.target == id {
                frontier.push(edge.source.as_str());
            }
        }
    }

    for node in &project.nodes {
        // Group boundaries are plumbing, not user-visible nodes.
        if node.node_type == "Passthrough" {
            continue;
        }
        // Triggers are entry points: they legitimately lack
        // downstream paths in the setup graph, and fire-time graphs
        // are computed separately per trigger. Don't warn.
        if node.features.is_trigger {
            continue;
        }
        if !reached.contains(node.id.as_str()) {
            let span = node.header_span_or_default();
            push(
                d,
                span,
                Severity::Warning,
                "unreachable-from-output",
                format!(
                    "node '{}' is not upstream of any output. \
                     Its value won't appear in run results. \
                     Add an output (e.g. a Debug node) downstream, or \
                     flip the node's config `is_output: true`.",
                    node.id
                ),
            );
        }
    }
}


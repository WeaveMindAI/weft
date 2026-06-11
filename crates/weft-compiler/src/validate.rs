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
use weft_core::project::{NodeDefinition, Span};
use weft_core::ProjectDefinition;

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
    let mut d = Vec::new();
    check_duplicates(project, &mut d);
    check_edge_node_refs(project, &mut d);
    check_scope_reachability(project, &mut d);
    check_port_resolution(project, &mut d);
    check_type_compat(project, &mut d);
    check_port_coverage(project, catalog, &mut d);
    check_loop_config(project, &mut d);
    check_warnings(project, &mut d);
    check_output_reachability(project, &mut d);
    check_declarative_rules(project, catalog, mode, &mut d);
    check_reserved_names(project, catalog, &mut d);
    d
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

/// Port is "satisfied" if either (a) it has a wired incoming edge,
/// or (b) the port is `configurable` and the node's config has a
/// non-null same-named field. This covers `Llm { prompt: "hi" }`
/// where prompt is provided by a literal rather than a wire.
fn input_satisfied(node: &NodeDefinition, project: &ProjectDefinition, port: &str) -> bool {
    if has_incoming_edge(node, project, port) {
        return true;
    }
    let is_configurable = node
        .inputs
        .iter()
        .find(|p| p.name == port)
        .map(|p| p.configurable)
        .unwrap_or(false);
    if !is_configurable {
        return false;
    }
    node.config
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
        if matches!(src.node_type.as_str(), "Passthrough" | "LoopIn" | "LoopOut")
            || matches!(tgt.node_type.as_str(), "Passthrough" | "LoopIn" | "LoopOut")
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
        if d <= 2 && best.map_or(true, |(bd, _)| d < bd) {
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

        if matches!(&src_port.port_type, weft_core::weft_type::WeftType::TypeVar(_)) {
            push(d, span, Severity::Error, "unresolved-typevar",
                format!(
                    "source port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    edge.source, src_port.name, src_port.port_type,
                ));
            continue;
        }
        if matches!(&tgt_port.port_type, weft_core::weft_type::WeftType::TypeVar(_)) {
            push(d, span, Severity::Error, "unresolved-typevar",
                format!(
                    "target port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    edge.target, tgt_port.name, tgt_port.port_type,
                ));
            continue;
        }

        if !weft_core::weft_type::WeftType::is_compatible(&src_port.port_type, &tgt_port.port_type) {
            push(d, span, Severity::Error, "type-mismatch",
                format!(
                    "cannot connect '{}.{}: {}' to '{}.{}: {}'",
                    edge.source, src_port.name, src_port.port_type,
                    edge.target, tgt_port.name, tgt_port.port_type,
                ));
        }
    }

    // config-type-mismatch + incompatible-port-type-override: walk
    // each node's config fields vs port types.
    // v1 refs: 3515-3519 (config literal), 4224-4258 (port type
    // override incompatibility).
    for node in &project.nodes {
        let Some(obj) = node.config.as_object() else { continue };
        for (key, value) in obj {
            let Some(port) = node.inputs.iter().find(|p| p.name == *key) else { continue };
            if !port.configurable { continue }
            // The culprit is the config field itself; fall back to the node
            // header if the field span is missing.
            let span = node.config_spans.get(key).map(|s| s.span)
                .or(node.header_span).unwrap_or_default();
            let inferred = weft_core::weft_type::WeftType::infer(value);
            if !weft_core::weft_type::WeftType::is_compatible(&inferred, &port.port_type) {
                push(d, span, Severity::Error, "config-type-mismatch",
                    format!(
                        "config '{}.{}: {}' incompatible with port type '{}'",
                        node.id, key, inferred, port.port_type,
                    ));
            }
        }
    }
}

// ─── group 4: port coverage ─────────────────────────────────────────────────

/// required-port-unmet: required input with no driver (no edge + no
///   config value).
/// require-one-of-unmet: each @require_one_of group must have at
///   least one port driven.
/// wired-only-port-config: configurable=false port has a config value.
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

        for port in &node.inputs {
            let has_edge = driven.contains(&(node.id.clone(), port.name.clone()));
            let has_config = node
                .config
                .get(&port.name)
                .map(|v| !v.is_null())
                .unwrap_or(false);

            if port.required && !has_edge && !has_config {
                push(
                    d,
                    span,
                    Severity::Error,
                    "required-port-unmet",
                    format!(
                        "required input '{}.{}' has no driver (no edge, no config)",
                        node.id, port.name
                    ),
                );
            }

            if has_config && !port.configurable {
                push(
                    d,
                    span,
                    Severity::Error,
                    "wired-only-port-config",
                    format!(
                        "port '{}.{}' is wired-only but has a config value. Remove the config or make the port configurable.",
                        node.id, port.name
                    ),
                );
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
                        .config
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
        // config key must name a real port (input or output, since
        // literal-emitter nodes like Text drive their output via
        // config) or a declared form field.
        if !node.features.can_add_input_ports && !node.features.has_form_schema {
            let Some(obj) = node.config.as_object() else { continue };
            let known_inputs: std::collections::HashSet<&str> =
                node.inputs.iter().map(|p| p.name.as_str()).collect();
            let known_outputs: std::collections::HashSet<&str> =
                node.outputs.iter().map(|p| p.name.as_str()).collect();
            // Metadata-declared fields are also valid config keys;
            // their values drive node behavior at runtime.
            let Some(meta) = catalog.lookup(&node.node_type) else { continue };
            let known_fields: std::collections::HashSet<&str> =
                meta.fields.iter().map(|f| f.key.as_str()).collect();
            for key in obj.keys() {
                // Reserved per-instance keys (`_label`, `_is_output`,
                // `_tags`) are validated by the parser and never need
                // to match a declared port. `parentId` / `fields` are
                // layout-side keys co-resident in the same config blob.
                if key.starts_with('_') || key == "parentId" || key == "fields" {
                    continue;
                }
                if known_inputs.contains(key.as_str())
                    || known_outputs.contains(key.as_str())
                    || known_fields.contains(key.as_str())
                {
                    continue;
                }
                push(
                    d,
                    node.config_spans.get(key).map(|s| s.span).unwrap_or(span),
                    Severity::Error,
                    "undeclared-port-no-custom",
                    format!(
                        "node '{}' does not accept custom inputs; config key '{}' doesn't match any declared port or field",
                        node.id, key
                    ),
                );
            }
        }

        // form-field-conflict: form fields materialize as ports. If
        // a form field's key clashes with an already-declared port,
        // the generated port would collide.
        //
        // Form-derived ports already live in `node.inputs` /
        // `node.outputs` after enrich. Duplicate-detection runs PER
        // SIDE: a single field can't materialize twice in `inputs`
        // (or twice in `outputs`), but a node legitimately can have
        // an input AND an output sharing a name (e.g. a passthrough
        // `value`/`value`). Crossing the chain would false-positive
        // every passthrough that happens to declare a form schema.
        if node.features.has_form_schema {
            for ports in [&node.inputs, &node.outputs] {
                let mut seen: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for port in ports.iter() {
                    if !seen.insert(port.name.as_str()) {
                        push(
                            d,
                            span,
                            Severity::Error,
                            "form-field-conflict",
                            format!(
                                "form field key '{}' duplicates a port of the same direction on '{}'",
                                port.name, node.id
                            ),
                        );
                    }
                }
            }
        }
    }

    // Note: literal-to-output-port from the v1 audit is enforced at
    // parse time: a `node.port = "literal"` line is lowered to a config
    // field on the node (not an edge), which the port checks reject when
    // the target port is output-only. No extra check needed at validate.
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
                .map(|cs| cs.span.clone())
                .unwrap_or_else(|| span.clone())
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

        // Non-string entries in over / carry are already rejected at
        // lowering time (`read_loop_port_list` pushes a CompileError);
        // here we just read the post-lowering values, which validate
        // can trust are strings.
        let read_port_list = |key: &str| -> Vec<String> {
            cfg.get(key).and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default()
        };
        let over: Vec<String> = read_port_list("over");
        let carry: Vec<String> = read_port_list("carry");

        if parallel && !carry.is_empty() {
            push(d, span_for("carry"), Severity::Error, "parallel-with-carry",
                format!("loop '{gid}': parallel: true forbids carry ports (carry implies sequential)"));
        }
        if parallel && over.is_empty() {
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
        let loop_out_id = format!("{gid}__out");
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
        if !parallel && over.is_empty() && cfg.get("max_iters").is_none() && !done_wired {
            push(d, span, Severity::Error, "loop-unbounded-no-termination",
                format!(
                    "loop '{gid}': sequential loop declares no 'over', no 'max_iters', and \
                     never writes `self.done`; it can never terminate. Iterate a list with \
                     `over: [...]`, cap it with `max_iters`, or wire `self.done = ...` in the body."
                ));
        }

        let carry_set: HashSet<&String> = carry.iter().collect();
        for p in &over {
            if carry_set.contains(p) {
                push(d, span_for("over"), Severity::Error, "over-and-carry-overlap",
                    format!("loop '{gid}': port '{p}' listed in both 'over' and 'carry'"));
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
        // nullability too would double-report one mistake.
        for port in &out_node.outputs {
            if carry_set.contains(&port.name) || port.name == "done" {
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

        // over ports must exist on LoopIn and be List[T].
        for p in &over {
            match in_node.inputs.iter().find(|x| &x.name == p) {
                None => {
                    push(d, span_for("over"), Severity::Error, "loop-over-unknown-port",
                        format!("loop '{gid}': 'over' references unknown input port '{p}'"));
                }
                Some(port) => {
                    if !matches!(port.port_type, weft_core::weft_type::WeftType::List(_)) {
                        push(d, span_for("over"), Severity::Error, "over-not-a-list",
                            format!("loop '{gid}': 'over' port '{p}' must be List[T], got {}", port.port_type));
                    }
                }
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
        // Nodes like Debug (no outputs) are terminal and exempt.
        if !node.outputs.is_empty() && !source_nodes.contains(node.id.as_str()) {
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
        if !node.inputs.is_empty()
            && !node.outputs.is_empty()
            && node.inputs.iter().all(|p| !p.required)
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
        .map(|g| format!("{}__out", g.id));

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
            "project has no output node (Debug, or any node with `is_output: true`). \
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


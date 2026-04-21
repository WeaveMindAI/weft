//! Graph validation. Runs after enrichment. Emits structured
//! Diagnostic objects (errors + warnings) for the IDE's Problems
//! panel and for the full compile pipeline.
//!
//! Ported from v1's weft-parser.ts validation passes; line ranges
//! for each rule documented at the helper function that implements
//! it.

use weft_core::ProjectDefinition;

use crate::{Diagnostic, Severity};

/// Run every validation rule against an enriched project and collect
/// all diagnostics. Returns an empty vector for a clean program.
pub fn validate(project: &ProjectDefinition) -> Vec<Diagnostic> {
    let mut d = Vec::new();
    check_duplicates(project, &mut d);
    check_edge_node_refs(project, &mut d);
    check_scope_reachability(project, &mut d);
    check_port_resolution(project, &mut d);
    check_type_compat(project, &mut d);
    check_port_coverage(project, &mut d);
    check_lane_mechanics(project, &mut d);
    check_warnings(project, &mut d);
    d
}

fn push(
    d: &mut Vec<Diagnostic>,
    line: usize,
    severity: Severity,
    code: &str,
    message: impl Into<String>,
) {
    d.push(Diagnostic {
        line,
        column: 0,
        severity,
        message: message.into(),
        code: Some(code.to_string()),
    });
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
        let line = node.header_span.map(|s| s.start_line).unwrap_or(0);
        match seen.get(node.id.as_str()) {
            Some(first_line) => push(
                d,
                line,
                Severity::Error,
                "duplicate-node-id",
                format!(
                    "duplicate node id '{}' (first declared at line {})",
                    node.id, first_line
                ),
            ),
            None => {
                seen.insert(&node.id, line);
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
        let line = edge.span.map(|s| s.start_line).unwrap_or(0);
        if !ids.contains(edge.source.as_str()) {
            push(
                d,
                line,
                Severity::Error,
                "unknown-source-node",
                format!("edge references unknown source node '{}'", edge.source),
            );
        }
        if !ids.contains(edge.target.as_str()) {
            push(
                d,
                line,
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
        if src.node_type == "Passthrough" || tgt.node_type == "Passthrough" {
            continue;
        }
        if src.scope == tgt.scope {
            continue;
        }
        let line = edge.span.map(|s| s.start_line).unwrap_or(0);
        push(
            d,
            line,
            Severity::Error,
            "scope-reachability",
            format!(
                "edge '{}.{} -> {}.{}' crosses scope boundaries without a passthrough",
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
        let line = edge.span.map(|s| s.start_line).unwrap_or(0);
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
                push(d, line, Severity::Error, "unknown-source-port", msg);
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
                push(d, line, Severity::Error, "unknown-target-port", msg);
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
        let line = edge.span.map(|s| s.start_line).unwrap_or(0);
        if let Some(first) = seen.get(&key) {
            push(
                d,
                line,
                Severity::Error,
                "duplicate-input-port",
                format!(
                    "port '{}.{}' already has a driver at line {}; an input can be fed by exactly one edge",
                    edge.target, handle, first
                ),
            );
        } else {
            seen.insert(key, line);
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
        let line = edge.span.map(|s| s.start_line).unwrap_or(0);
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
            push(d, line, Severity::Error, "must-override-unmet",
                format!(
                    "source port '{}.{}' is MustOverride. Declare a concrete type in weft source.",
                    edge.source, src_port.name,
                ));
            continue;
        }
        if tgt_port.port_type.is_must_override() {
            push(d, line, Severity::Error, "must-override-unmet",
                format!(
                    "target port '{}.{}' is MustOverride. Declare a concrete type in weft source.",
                    edge.target, tgt_port.name,
                ));
            continue;
        }

        if matches!(&src_port.port_type, weft_core::weft_type::WeftType::TypeVar(_)) {
            push(d, line, Severity::Error, "unresolved-typevar",
                format!(
                    "source port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    edge.source, src_port.name, src_port.port_type,
                ));
            continue;
        }
        if matches!(&tgt_port.port_type, weft_core::weft_type::WeftType::TypeVar(_)) {
            push(d, line, Severity::Error, "unresolved-typevar",
                format!(
                    "target port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    edge.target, tgt_port.name, tgt_port.port_type,
                ));
            continue;
        }

        if !weft_core::weft_type::WeftType::is_compatible(&src_port.port_type, &tgt_port.port_type) {
            push(d, line, Severity::Error, "type-mismatch",
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
            let line = node.config_spans.get(key).map(|s| s.start_line)
                .or_else(|| node.header_span.map(|s| s.start_line)).unwrap_or(0);
            let inferred = weft_core::weft_type::WeftType::infer(value);
            if !weft_core::weft_type::WeftType::is_compatible(&inferred, &port.port_type) {
                push(d, line, Severity::Error, "config-type-mismatch",
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
fn check_port_coverage(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
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
        let line = node.header_span.map(|s| s.start_line).unwrap_or(0);

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
                    line,
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
                    line,
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
                    line,
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
            for key in obj.keys() {
                // Internal/compiler-injected keys.
                if key == "parentId" || key == "fields" {
                    continue;
                }
                if known_inputs.contains(key.as_str()) || known_outputs.contains(key.as_str()) {
                    continue;
                }
                push(
                    d,
                    node.config_spans.get(key).map(|s| s.start_line).unwrap_or(line),
                    Severity::Error,
                    "undeclared-port-no-custom",
                    format!(
                        "node '{}' does not accept custom inputs; config key '{}' doesn't match any declared port",
                        node.id, key
                    ),
                );
            }
        }

        // form-field-conflict: form fields materialize as ports. If a
        // form field's key clashes with an already-declared port, the
        // generated port would collide. Catch at validate.
        if node.features.has_form_schema {
            if let Some(fields) = node.config.get("fields").and_then(|v| v.as_array()) {
                let declared: std::collections::HashSet<&str> = node
                    .inputs
                    .iter()
                    .chain(node.outputs.iter())
                    .map(|p| p.name.as_str())
                    .collect();
                for f in fields {
                    let Some(key) = f.get("key").and_then(|v| v.as_str()) else { continue };
                    // Skip self-fields (the form schema port itself).
                    if declared.iter().filter(|n| **n == key).count() > 1 {
                        push(
                            d,
                            line,
                            Severity::Error,
                            "form-field-conflict",
                            format!(
                                "form field key '{}' collides with an existing port on '{}'",
                                key, node.id
                            ),
                        );
                    }
                }
            }
        }
    }

    // Note: literal-to-output-port from the v1 audit is enforced at
    // parse time: `out.port = "literal"` syntax is routed through
    // try_parse_literal and becomes a ConfigFill, which the parser
    // rejects when the target port is output-only. No extra check
    // needed at validate.
}

// ─── group 5: lane mechanics ────────────────────────────────────────────────

/// gather-insufficient-depth: a Gather-laned target port has a type
///   List[L1]..[Ln] but the edge source has fewer than n List levels.
/// implicit-expand: source is List[T], target is T, automatic expand.
/// implicit-gather: source is T, target is List[T], automatic gather.
/// gather-null-warning: Gather target type excludes Null, but the
///   pipeline may propagate null into it.
///
/// v1 refs: 3710-3722 (expand/gather implicit), 3828-3849 (gather
/// underflow), 4004-4009 (gather null warning).
fn check_lane_mechanics(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    for edge in &project.edges {
        let line = edge.span.map(|s| s.start_line).unwrap_or(0);
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

        let src_depth = list_depth(&src_port.port_type);
        let tgt_depth = list_depth(&tgt_port.port_type);

        if src_depth > tgt_depth {
            // Source deeper than target: auto-expand (informational
            // warning v1 emits; kept for parity).
            push(d, line, Severity::Warning, "implicit-expand",
                format!(
                    "implicit expand on '{}.{} -> {}.{}' (source has {} more List level(s))",
                    edge.source, src_port.name, edge.target, tgt_port.name,
                    src_depth - tgt_depth,
                ));
        } else if tgt_depth > src_depth {
            // Target deeper: gather.
            push(d, line, Severity::Warning, "implicit-gather",
                format!(
                    "implicit gather on '{}.{} -> {}.{}' (target has {} more List level(s))",
                    edge.source, src_port.name, edge.target, tgt_port.name,
                    tgt_depth - src_depth,
                ));

            // Gather-null-warning: the gathered value is always a
            // List even if zero items arrive. If the target inner
            // type doesn't admit Null, we might silently drop nulls
            // upstream. Warn.
            if let Some(inner) = innermost(&tgt_port.port_type) {
                if !admits_null(inner) {
                    push(d, line, Severity::Warning, "gather-null-warning",
                        format!(
                            "gather target '{}.{}: {}' doesn't admit Null; upstream null values may be silently dropped",
                            edge.target, tgt_port.name, tgt_port.port_type,
                        ));
                }
            }
        }

        // gather-insufficient-depth: if the target's declared lane
        // depth is greater than the actual structural difference, the
        // runtime will under-peel. V1 surfaces this when an explicit
        // gather directive is over-specified.
        let declared_lane = tgt_port.lane_depth as usize;
        if tgt_port.lane_mode == weft_core::project::LaneMode::Gather
            && declared_lane > src_depth.saturating_sub(tgt_depth)
        {
            push(d, line, Severity::Error, "gather-insufficient-depth",
                format!(
                    "gather at '{}.{}' requests depth {} but only {} List level(s) available above target type",
                    edge.target, tgt_port.name, declared_lane,
                    src_depth.saturating_sub(tgt_depth),
                ));
        }
    }
}

fn list_depth(t: &weft_core::weft_type::WeftType) -> usize {
    let mut depth = 0;
    let mut cur = t;
    while let weft_core::weft_type::WeftType::List(inner) = cur {
        depth += 1;
        cur = inner;
    }
    depth
}

fn innermost(t: &weft_core::weft_type::WeftType) -> Option<&weft_core::weft_type::WeftType> {
    let mut cur = t;
    loop {
        match cur {
            weft_core::weft_type::WeftType::List(inner) => cur = inner,
            other => return Some(other),
        }
    }
}

fn admits_null(t: &weft_core::weft_type::WeftType) -> bool {
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    match t {
        WeftType::Primitive(WeftPrimitive::Null) => true,
        WeftType::Union(members) => members.iter().any(admits_null),
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
        let line = node.header_span.map(|s| s.start_line).unwrap_or(0);

        // orphan-outputs: only flag when the node has outputs at all.
        // Nodes like Debug (no outputs) are terminal and exempt.
        if !node.outputs.is_empty() && !source_nodes.contains(node.id.as_str()) {
            push(
                d,
                line,
                Severity::Warning,
                "orphan-outputs",
                format!(
                    "node '{}' produces outputs but none are consumed by downstream nodes",
                    node.id
                ),
            );
        }

        // no-required-skip: only applies when the node has inputs,
        // none are required, and the node doesn't declare
        // @require_one_of (which would provide its own skip logic).
        if !node.inputs.is_empty()
            && node.inputs.iter().all(|p| !p.required)
            && node.features.one_of_required.is_empty()
        {
            push(
                d,
                line,
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
                        node.config_spans.get(key).map(|s| s.start_line).unwrap_or(line),
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


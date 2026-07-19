//! Post-compilation enrichment. Given a parsed ProjectDefinition and
//! a NodeCatalog, populate each NodeDefinition's inputs/outputs/features
//! from the catalog, materialize form-derived ports, merge
//! weft-declared custom ports for nodes with
//! canAddInputPorts/canAddOutputPorts, and validate that every
//! referenced node type exists.
//!
//! Three node-types are built-in (not catalog): `Passthrough` (group
//! boundary), `LoopIn` / `LoopOut` (loop boundary). Their port shapes
//! are written by the compiler's lowering (`flatten_group` in
//! `weft_compiler.rs`); enrich does not consult the catalog for them
//! (it `continue`s past these node types).

use serde_json::Value;

use weft_core::node::{FormFieldPort, MetadataCatalog};
use weft_core::project::{PortDefinition, ProjectDefinition, Span};
use weft_core::weft_type::WeftType;

use crate::error::{CompileError, CompileResult};

/// One enrich failure carrying the SOURCE SPAN of the offending node, so a
/// diagnostic consumer (the editor's squiggles, the CLI's error lines) points
/// at the exact line instead of a span-less blob. The span is the node's
/// header (`header_span_or_default`); `Span::default()` only for the rare
/// project-level error with no node to blame.
pub struct EnrichError {
    pub span: Span,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortDirection {
    Input,
    Output,
}

impl PortDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

fn merge_ports(
    catalog_ports: &[PortDefinition],
    weft_ports: &[PortDefinition],
    can_add: bool,
    node_id: &str,
    span: Span,
    direction: PortDirection,
    errors: &mut Vec<EnrichError>,
) -> Vec<PortDefinition> {
    use std::collections::HashMap;

    let catalog_by_name: HashMap<&str, &PortDefinition> = catalog_ports
        .iter()
        .map(|p| (p.name.as_str(), p))
        .collect();

    let mut result: Vec<PortDefinition> = catalog_ports
        .iter()
        .map(|cp| {
            let Some(wp) = weft_ports.iter().find(|w| w.name == cp.name) else {
                return cp.clone();
            };
            let mut merged = cp.clone();
            merged.required = wp.required;
            if !wp.port_type.is_must_override() {
                if cp.port_type.is_must_override()
                    || WeftType::is_compatible(&wp.port_type, &cp.port_type)
                {
                    merged.port_type = wp.port_type.clone();
                } else {
                    errors.push(EnrichError { span, message: format!(
                        "node '{}': {} port '{}' declared type {} incompatible with catalog type {}",
                        node_id,
                        direction.as_str(),
                        cp.name,
                        wp.port_type,
                        cp.port_type,
                    )});
                }
            }
            merged
        })
        .collect();

    for wp in weft_ports {
        if catalog_by_name.contains_key(wp.name.as_str()) {
            continue;
        }
        if !can_add {
            // The user wrote a port in source that this node type does
            // not accept in this direction. Dropping it silently makes
            // it vanish from the materialized node (edges wired to it
            // later surface as a confusing unknown-port error, and an
            // unwired one disappears with no signal). Fail loud instead.
            // Not gated by EnrichPolicy: that only forgives unknown node
            // TYPES; a known type rejecting an authored port is a hard
            // authoring error in every mode.
            errors.push(EnrichError { span, message: format!(
                "node '{}': declares custom {} port '{}' but node type does not support custom {} ports",
                node_id,
                direction.as_str(),
                wp.name,
                direction.as_str(),
            )});
            continue;
        }
        // A user-added port with the placeholder `MustOverride` type is the
        // graph editor's default for "I added a port but haven't set the
        // type yet". Keep the port in the materialized list so the UI shows
        // it (the user just added it), and surface the missing-type as an
        // error diagnostic instead. Without this, the round-trip silently
        // ate the port and it vanished from the canvas.
        if wp.port_type.is_must_override() {
            errors.push(EnrichError { span, message: format!(
                "node '{}': custom {} port '{}' needs a concrete type",
                node_id,
                direction.as_str(),
                wp.name,
            )});
        }
        result.push(wp.clone());
    }

    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichPolicy {
    Strict,
    Lenient,
}

pub fn enrich(project: &mut ProjectDefinition, catalog: &dyn MetadataCatalog) -> CompileResult<()> {
    enrich_with_policy(project, catalog, EnrichPolicy::Strict)
}

/// String wrapper over [`enrich_collecting`]: joins the per-node errors into
/// one `CompileError::Enrich` for callers that only want a pass/fail (no
/// per-error spans). Diagnostic consumers call `enrich_collecting` directly to
/// keep each error's line.
pub fn enrich_with_policy(
    project: &mut ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    policy: EnrichPolicy,
) -> CompileResult<()> {
    let errors = enrich_collecting(project, catalog, policy);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CompileError::Enrich(
            errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join("; "),
        ))
    }
}

/// Enrich, returning every failure with the SOURCE SPAN of the offending node
/// (empty vec = clean). The span-preserving core; the string-joining
/// `enrich_with_policy` wraps it. Diagnostic-producing callers (the editor
/// parse, the CLI) use this so each error squiggles its own line.
pub fn enrich_collecting(
    project: &mut ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    policy: EnrichPolicy,
) -> Vec<EnrichError> {
    let mut errors: Vec<EnrichError> = Vec::new();
    // One [reserved-node-type] error per offending TYPE, not per node:
    // boundary types appear on two nodes per group, and a project full
    // of groups would otherwise report the same corrupt catalog entry
    // 2xN times.
    let mut reported_reserved: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for node in project.nodes.iter_mut() {
        // Built-in boundary node-types. Their ports are written by the
        // compiler's lowering pass (Passthrough by group-flatten, LoopIn
        // / LoopOut by loop-lowering); enrich does not consult the
        // catalog for them. A catalog entry that masquerades as one of
        // these built-in names is rejected loud as a corrupt catalog:
        // letting a catalog impl shadow the runtime built-in would
        // silently break group / loop boundaries.
        if matches!(node.node_type.as_str(), "Passthrough" | "LoopIn" | "LoopOut") {
            if catalog.lookup(&node.node_type).is_some()
                && reported_reserved.insert(node.node_type.clone())
            {
                errors.push(EnrichError { span: node.header_span_or_default(), message: format!(
                    "[reserved-node-type] catalog declares '{}' but that name is a built-in language type; rename the catalog node",
                    node.node_type,
                )});
            }
            continue;
        }

        // A catalog claiming any other reserved type keyword is also a
        // corrupt catalog (e.g. an entry literally named 'Group' or
        // 'Loop'). The reserved-type set is the single source of truth
        // for this check.
        if crate::weft_compiler::is_reserved_type_keyword(&node.node_type) {
            if catalog.lookup(&node.node_type).is_some()
                && reported_reserved.insert(node.node_type.clone())
            {
                errors.push(EnrichError { span: node.header_span_or_default(), message: format!(
                    "[reserved-node-type] catalog declares '{}' but that name is a reserved language keyword",
                    node.node_type,
                )});
            }
            continue;
        }

        let Some(meta) = catalog.lookup(&node.node_type) else {
            if policy == EnrichPolicy::Strict {
                errors.push(EnrichError {
                    span: node.header_span_or_default(),
                    message: format!("unknown node type: '{}'", node.node_type),
                });
            }
            continue;
        };

        // The offending node's header span, so a port error squiggles its line.
        let node_span = node.header_span_or_default();
        let weft_inputs = std::mem::take(&mut node.inputs);
        let weft_outputs = std::mem::take(&mut node.outputs);

        let catalog_inputs: Vec<PortDefinition> = meta
            .inputs
            .iter()
            .map(|p| PortDefinition {
                name: p.name.clone(),
                port_type: p.port_type.clone(),
                required: p.required,
                description: None,
                configurable: meta.input_configurable(p),
                synthesized_from_carry: false,
            })
            .collect();
        let mut catalog_outputs: Vec<PortDefinition> = meta
            .outputs
            .iter()
            .map(|p| PortDefinition {
                name: p.name.clone(),
                port_type: p.port_type.clone(),
                required: p.required,
                description: None,
                configurable: false,
                synthesized_from_carry: false,
            })
            .collect();

        // A form-schema node's ports are DERIVED from its `fields` config. Fold
        // them into the catalog port set BEFORE merging the source-declared
        // ports, so they count as known ports: a header that re-declares a
        // derived port (the graph editor never writes these, but a hand-authored
        // `.weft` may) merges cleanly IF it matches by name + type, and a header
        // port that does NOT match a derived (or catalog) port is the genuine
        // "custom port on a node that forbids them" error. Declaring them is
        // always OPTIONAL: omitting the header is the normal case.
        let mut catalog_inputs = catalog_inputs;
        if meta.features.has_form_schema {
            let (form_inputs, form_outputs) =
                derive_form_ports(&node.config, &meta.form_field_specs);
            catalog_inputs.extend(form_inputs);
            catalog_outputs.extend(form_outputs);
        }

        let inputs = merge_ports(
            &catalog_inputs,
            &weft_inputs,
            meta.features.can_add_input_ports,
            &node.id,
            node_span,
            PortDirection::Input,
            &mut errors,
        );
        let outputs = merge_ports(
            &catalog_outputs,
            &weft_outputs,
            meta.features.can_add_output_ports,
            &node.id,
            node_span,
            PortDirection::Output,
            &mut errors,
        );

        node.inputs = inputs;
        node.outputs = outputs;
        node.features = meta.features.clone();
        node.requires_infra = meta.requires_infra;
        node.images = meta.images.clone();
    }

    // Port errors mean the topology is malformed; skip type resolution (it would
    // walk a broken graph) and return them. A clean merge runs type resolution,
    // whose failure (an unresolvable TypeVar) is a project-level enrich error
    // with no single node to blame (`Span::default`).
    if errors.is_empty() {
        if let Err(e) = resolve_type_vars(project) {
            errors.push(EnrichError { span: Span::default(), message: format!("{e}") });
        }
    }
    errors
}

fn resolve_type_vars(project: &mut ProjectDefinition) -> CompileResult<()> {
    loop {
        let mut changed = false;

        let snapshot: Vec<(String, String, WeftType, String, String, WeftType)> = project
            .edges
            .iter()
            .filter_map(|edge| {
                let src_port = edge.source_handle.as_deref()?;
                let tgt_port = edge.target_handle.as_deref()?;
                let src_type = project
                    .nodes
                    .iter()
                    .find(|n| n.id == edge.source)
                    .and_then(|n| n.outputs.iter().find(|p| p.name == src_port))
                    .map(|p| p.port_type.clone())?;
                let tgt_type = project
                    .nodes
                    .iter()
                    .find(|n| n.id == edge.target)
                    .and_then(|n| n.inputs.iter().find(|p| p.name == tgt_port))
                    .map(|p| p.port_type.clone())?;
                Some((
                    edge.source.clone(),
                    src_port.to_string(),
                    src_type,
                    edge.target.clone(),
                    tgt_port.to_string(),
                    tgt_type,
                ))
            })
            .collect();

        for (src_node, _src_port, src_type, tgt_node, _tgt_port, tgt_type) in snapshot {
            if let WeftType::TypeVar(name) = &tgt_type {
                if !src_type.is_unresolved() {
                    if substitute_type_var(project, &tgt_node, name, &src_type) {
                        changed = true;
                    }
                }
            }
            if let WeftType::TypeVar(name) = &src_type {
                if !tgt_type.is_unresolved() {
                    if substitute_type_var(project, &src_node, name, &tgt_type) {
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            break;
        }
    }

    Ok(())
}

fn substitute_type_var(
    project: &mut ProjectDefinition,
    node_id: &str,
    var_name: &str,
    concrete: &WeftType,
) -> bool {
    let Some(node) = project.nodes.iter_mut().find(|n| n.id == node_id) else {
        return false;
    };
    let mut changed = false;
    for port in node.inputs.iter_mut().chain(node.outputs.iter_mut()) {
        changed |= replace_in_type(&mut port.port_type, var_name, concrete);
    }
    changed
}

fn replace_in_type(ty: &mut WeftType, var_name: &str, concrete: &WeftType) -> bool {
    match ty {
        WeftType::TypeVar(n) if n == var_name => {
            *ty = concrete.clone();
            true
        }
        WeftType::List(inner) => replace_in_type(inner, var_name, concrete),
        WeftType::Dict(key, val) => {
            let a = replace_in_type(key, var_name, concrete);
            let b = replace_in_type(val, var_name, concrete);
            a || b
        }
        WeftType::Union(members) => {
            let mut any = false;
            for m in members.iter_mut() {
                any |= replace_in_type(m, var_name, concrete);
            }
            any
        }
        _ => false,
    }
}

/// The (input, output) ports a form-schema node's `fields` config derives from
/// its specs. Pure: reads each field's `fieldType` + `key`, matches the spec,
/// and resolves its `adds_inputs` / `adds_outputs` templates. The enricher folds
/// these into the node's known ports (see the call site).
fn derive_form_ports(
    config: &Value,
    specs: &[weft_core::FormFieldSpec],
) -> (Vec<PortDefinition>, Vec<PortDefinition>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let Some(fields) = config.get("fields").and_then(|f| f.as_array()) else {
        return (inputs, outputs);
    };

    for field in fields {
        let Some(obj) = field.as_object() else { continue };
        let field_type = obj
            .get("fieldType")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("field_type").and_then(|v| v.as_str()))
            .or_else(|| {
                obj.get("field_type")
                    .and_then(|v| v.get("kind"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or_default();
        let key = obj.get("key").and_then(|v| v.as_str()).unwrap_or_default();
        if key.is_empty() || field_type.is_empty() {
            continue;
        }

        let Some(spec) = specs.iter().find(|s| s.field_type == field_type) else {
            continue;
        };

        for port in &spec.adds_inputs {
            inputs.push(materialize_port(port, key, false));
        }
        for port in &spec.adds_outputs {
            outputs.push(materialize_port(port, key, true));
        }
    }
    (inputs, outputs)
}

fn materialize_port(template: &FormFieldPort, key: &str, is_output: bool) -> PortDefinition {
    let name = template.resolve_name(key);
    let port_type = match &template.port_type {
        WeftType::TypeVar(n) if n == "T_Auto" => WeftType::type_var(&format!("T__{key}")),
        other => other.clone(),
    };
    PortDefinition {
        name,
        port_type: port_type.clone(),
        required: !is_output,
        description: None,
        configurable: !is_output && port_type.is_default_configurable(),
        synthesized_from_carry: false,
    }
}

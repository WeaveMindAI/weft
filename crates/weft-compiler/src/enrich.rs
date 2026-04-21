//! Post-compilation enrichment. Given a parsed ProjectDefinition and
//! a NodeCatalog, populate each NodeDefinition's inputs/outputs/features
//! from the catalog, materialize form-derived ports, and validate
//! that every referenced node type exists.
//!
//! v1's enrich lives at `crates-v1/weft-nodes/src/enrich.rs` and is
//! much larger (~1600 lines). It also handles:
//! - Custom weft-added ports (canAddInputPorts/canAddOutputPorts).
//! - `T_Auto` per-field-instance TypeVar replacement.
//! - Merging weft-declared ports with catalog ports (weft overrides
//!   `required`).
//! - Filtering UI-only nodes.
//!
//! This v2 port starts with the minimum required for the 5 starter
//! nodes to work end to end. The other features get added as
//! additional nodes need them.

use serde_json::Value;

use weft_core::node::{FormFieldPort, NodeCatalog};
use weft_core::project::{PortDefinition, ProjectDefinition};
use weft_core::weft_type::WeftType;

use crate::error::{CompileError, CompileResult};

/// Enrich every node in the project with its catalog metadata, then
/// resolve TypeVars across connected edges.
pub fn enrich(project: &mut ProjectDefinition, catalog: &dyn NodeCatalog) -> CompileResult<()> {
    let mut errors = Vec::new();

    for node in project.nodes.iter_mut() {
        if node.node_type == "Passthrough" {
            // Passthrough ports are set by the compiler at group
            // flatten time; no catalog lookup.
            continue;
        }

        let Some(node_impl) = catalog.lookup(&node.node_type) else {
            errors.push(format!("unknown node type: '{}'", node.node_type));
            continue;
        };
        let meta = node_impl.metadata();

        // Base ports from catalog.
        let mut inputs: Vec<PortDefinition> = meta
            .inputs
            .iter()
            .map(|p| PortDefinition {
                name: p.name.clone(),
                port_type: p.port_type.clone(),
                required: p.required,
                description: None,
                lane_mode: p.lane_mode,
                lane_depth: 1,
                configurable: p.configurable || p.port_type.is_default_configurable(),
            })
            .collect();
        let mut outputs: Vec<PortDefinition> = meta
            .outputs
            .iter()
            .map(|p| PortDefinition {
                name: p.name.clone(),
                port_type: p.port_type.clone(),
                required: p.required,
                description: None,
                lane_mode: p.lane_mode,
                lane_depth: 1,
                configurable: false,
            })
            .collect();

        // Form-derived ports (for nodes declaring has_form_schema).
        if meta.features.has_form_schema {
            let specs = catalog.form_field_specs(&node.node_type);
            materialize_form_ports(&node.config, specs, &mut inputs, &mut outputs);
        }

        node.inputs = inputs;
        node.outputs = outputs;
        node.features = meta.features;
        node.entry = meta.entry;
    }

    if !errors.is_empty() {
        return Err(CompileError::Enrich(errors.join("; ")));
    }

    resolve_type_vars(project)?;
    Ok(())
}

/// Walk edges; wherever one end is a concrete type and the other is
/// a TypeVar, substitute the concrete type for that TypeVar across
/// the node's ports. Iterate to a fixed point (resolving one
/// TypeVar can open new resolutions). `MustOverride` left alone
/// (compile error elsewhere).
fn resolve_type_vars(project: &mut ProjectDefinition) -> CompileResult<()> {
    loop {
        let mut changed = false;

        // Collect all edge endpoint types into a snapshot so we can
        // mutate nodes without fighting the borrow checker.
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
            // Tgt has a TypeVar, src is concrete: resolve the var on
            // the tgt side.
            if let WeftType::TypeVar(name) = &tgt_type {
                if !src_type.is_unresolved() {
                    if substitute_type_var(project, &tgt_node, name, &src_type) {
                        changed = true;
                    }
                }
            }
            // Src has a TypeVar, tgt is concrete: resolve on the src
            // side (rarer, but it happens when a trigger's output
            // type is `T` and downstream expects `String`).
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

/// Replace every occurrence of `TypeVar(var_name)` in the given
/// node's inputs and outputs with `concrete`. Returns true if any
/// replacement happened.
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

/// Given the node's `config.fields` array and the node type's
/// FormFieldSpecs, produce extra input and output ports. Used by
/// HumanQuery and runner-style trigger nodes.
fn materialize_form_ports(
    config: &Value,
    specs: &[weft_core::FormFieldSpec],
    inputs: &mut Vec<PortDefinition>,
    outputs: &mut Vec<PortDefinition>,
) {
    let Some(fields) = config.get("fields").and_then(|f| f.as_array()) else {
        return;
    };

    for field in fields {
        let Some(obj) = field.as_object() else { continue };
        let field_type = obj
            .get("field_type")
            .and_then(|v| v.get("kind"))
            .and_then(|v| v.as_str())
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
        lane_mode: Default::default(),
        lane_depth: 1,
        configurable: !is_output && port_type.is_default_configurable(),
    }
}

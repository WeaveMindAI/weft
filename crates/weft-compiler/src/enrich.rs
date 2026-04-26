//! Post-compilation enrichment. Given a parsed ProjectDefinition and
//! a NodeCatalog, populate each NodeDefinition's inputs/outputs/features
//! from the catalog, materialize form-derived ports, merge
//! weft-declared custom ports for nodes with
//! canAddInputPorts/canAddOutputPorts, and validate that every
//! referenced node type exists.

use serde_json::Value;

use weft_core::node::{FormFieldPort, MetadataCatalog};
use weft_core::project::{PortDefinition, ProjectDefinition};
use weft_core::weft_type::WeftType;

use crate::error::{CompileError, CompileResult};

/// Which side of a port we're merging. Only used for human-readable
/// diagnostic messages from `merge_ports`.
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

/// Merge weft-declared ports with catalog ports. Rules:
///
/// 1. Every catalog port is present in the result. Catalog is the
///    source of truth for port identity, lane_mode, and type
///    (except when catalog type is `MustOverride`, where the
///    weft-declared type becomes the real type).
/// 2. A weft port that matches a catalog port by name can:
///    - override `required` (either direction),
///    - narrow the type if catalog's type is MustOverride, or
///      compatible with the weft declaration.
///    - Incompatible types produce an enrich error tied to the
///      node id.
/// 3. A weft port with no matching catalog port:
///    - If the node has `can_add` for this direction, added to
///      the result verbatim (must carry a real type).
///    - Else a warning is logged and the port is dropped. The
///      compile still succeeds (the weft graph just loses the
///      connection, which downstream validate flags).
fn merge_ports(
    catalog_ports: &[PortDefinition],
    weft_ports: &[PortDefinition],
    can_add: bool,
    node_id: &str,
    direction: PortDirection,
    errors: &mut Vec<String>,
) -> Vec<PortDefinition> {
    use std::collections::HashMap;

    let catalog_by_name: HashMap<&str, &PortDefinition> = catalog_ports
        .iter()
        .map(|p| (p.name.as_str(), p))
        .collect();

    // Walk catalog ports first so the result carries them all, in
    // declaration order. For each one, if the weft source re-stated
    // it, apply the override rules.
    let mut result: Vec<PortDefinition> = catalog_ports
        .iter()
        .map(|cp| {
            let Some(wp) = weft_ports.iter().find(|w| w.name == cp.name) else {
                return cp.clone();
            };
            let mut merged = cp.clone();
            // required: weft wins in either direction.
            merged.required = wp.required;

            // Type override. Catalog `MustOverride` means "the node
            // doesn't know yet, whoever wires me tells me." In that
            // case the weft type IS the type. Otherwise the weft
            // type must be compatible with (a subtype / narrowing
            // of) the catalog type; incompatible → hard error.
            if !wp.port_type.is_must_override() {
                if cp.port_type.is_must_override()
                    || WeftType::is_compatible(&wp.port_type, &cp.port_type)
                {
                    merged.port_type = wp.port_type.clone();
                    // The user RESTATED the type in the .weft
                    // source: surface that to validate so the
                    // implicit-expand/gather warnings can stay
                    // quiet for edges where both sides are
                    // user-stated.
                    merged.user_typed = true;
                } else {
                    errors.push(format!(
                        "node '{}': {} port '{}' declared type {} incompatible with catalog type {}",
                        node_id,
                        direction.as_str(),
                        cp.name,
                        wp.port_type,
                        cp.port_type,
                    ));
                }
            }

            if wp.lane_mode != cp.lane_mode && wp.lane_mode != Default::default() {
                tracing::warn!(
                    "enrich: node '{}' {} port '{}': weft lane_mode {:?} differs from catalog {:?}; using catalog",
                    node_id,
                    direction.as_str(),
                    cp.name,
                    wp.lane_mode,
                    cp.lane_mode,
                );
            }
            merged
        })
        .collect();

    // Then: weft ports that are NOT in the catalog. These are only
    // valid on nodes with can_add_<direction>_ports.
    for wp in weft_ports {
        if catalog_by_name.contains_key(wp.name.as_str()) {
            continue;
        }
        if !can_add {
            tracing::warn!(
                "enrich: node '{}' declares custom {} port '{}' but node type does not support custom {} ports; dropping",
                node_id,
                direction.as_str(),
                wp.name,
                direction.as_str(),
            );
            continue;
        }
        if wp.port_type.is_must_override() {
            errors.push(format!(
                "node '{}': custom {} port '{}' needs a concrete type",
                node_id,
                direction.as_str(),
                wp.name,
            ));
            continue;
        }
        result.push(wp.clone());
    }

    result
}

/// Policy for handling unknown node types during enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichPolicy {
    /// Fail the whole enrichment on any unknown node type. Used by the
    /// full compile pipeline where an unknown type is a hard error.
    Strict,
    /// Skip unknown types and continue. Used by the /parse endpoint
    /// during interactive editing where the user might be partway
    /// through typing a node type name.
    Lenient,
}

/// Strict enrichment. Fails on unknown node types. Equivalent to
/// `enrich_with_policy(project, catalog, EnrichPolicy::Strict)`.
pub fn enrich(project: &mut ProjectDefinition, catalog: &dyn MetadataCatalog) -> CompileResult<()> {
    enrich_with_policy(project, catalog, EnrichPolicy::Strict)
}

/// Enrich every node in the project with its catalog metadata, then
/// resolve TypeVars across connected edges. Policy controls what to do
/// with unknown node types.
pub fn enrich_with_policy(
    project: &mut ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    policy: EnrichPolicy,
) -> CompileResult<()> {
    let mut errors = Vec::new();

    for node in project.nodes.iter_mut() {
        let Some(meta) = catalog.lookup(&node.node_type) else {
            if policy == EnrichPolicy::Strict {
                errors.push(format!("unknown node type: '{}'", node.node_type));
            }
            // Lenient: leave inputs/outputs empty; render shows a
            // placeholder box. Diagnostics pass surfaces the real
            // error separately.
            continue;
        };

        // Passthrough is a real catalog entry, but its ports are
        // written by the compiler's group-flatten pass, not derived
        // from metadata. Skip port enrichment; let features through.
        // entry_signals is empty by construction for passthroughs.
        if node.node_type == "Passthrough" {
            node.features = meta.features.clone();
            node.entry_signals = Vec::new();
            continue;
        }

        // Snapshot the weft-declared ports before we overwrite from
        // catalog. The parser stores whatever the user wrote in the
        // `NodeType(in: T) -> (out: U)` header into node.inputs /
        // node.outputs. merge_ports below consumes these to respect
        // user-declared custom ports on nodes with canAddInputPorts /
        // canAddOutputPorts, and to override `required` on catalog
        // ports that the user re-stated with `*`.
        let weft_inputs = std::mem::take(&mut node.inputs);
        let weft_outputs = std::mem::take(&mut node.outputs);

        // Base ports from catalog. user_typed=false because the
        // type came from metadata.json, not the .weft source.
        let catalog_inputs: Vec<PortDefinition> = meta
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
                user_typed: false,
            })
            .collect();
        let catalog_outputs: Vec<PortDefinition> = meta
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
                user_typed: false,
            })
            .collect();

        let mut inputs = merge_ports(
            &catalog_inputs,
            &weft_inputs,
            meta.features.can_add_input_ports,
            &node.id,
            PortDirection::Input,
            &mut errors,
        );
        let mut outputs = merge_ports(
            &catalog_outputs,
            &weft_outputs,
            meta.features.can_add_output_ports,
            &node.id,
            PortDirection::Output,
            &mut errors,
        );

        // Form-derived ports (for nodes declaring has_form_schema).
        if meta.features.has_form_schema {
            let specs = catalog.form_field_specs(&node.node_type);
            materialize_form_ports(&node.config, specs, &mut inputs, &mut outputs);
        }

        node.inputs = inputs;
        node.outputs = outputs;
        node.features = meta.features.clone();
        node.requires_infra = meta.requires_infra;
        node.sidecar = meta.features.sidecar.clone();

        // Resolve each declared entry signal's tag against the
        // node's config. Each `WakeSignalKind` variant documents
        // which config fields it expects; contract failures
        // (missing field, bad type) become enrich errors tied to
        // this node.
        let config_map = match node.config.as_object() {
            Some(obj) => obj.clone().into_iter().collect(),
            None => std::collections::HashMap::new(),
        };
        let mut resolved_signals =
            Vec::with_capacity(meta.entry_signals.len());
        for tag in &meta.entry_signals {
            match weft_core::primitive::WakeSignalKind::resolve_from_config(
                tag.kind,
                &config_map,
            ) {
                Ok(resolved) => resolved_signals.push(
                    weft_core::primitive::WakeSignalSpec {
                        kind: resolved,
                        is_resume: tag.is_resume,
                    },
                ),
                Err(e) => errors.push(format!(
                    "node '{}' entry signal: {}",
                    node.id, e.message
                )),
            }
        }
        node.entry_signals = resolved_signals;
    }

    if !errors.is_empty() {
        return Err(CompileError::Enrich(errors.join("; ")));
    }

    resolve_type_vars(project)?;
    infer_lane_modes(project);
    Ok(())
}

/// Walk edges; infer `lane_mode` + `lane_depth` on the target
/// port whenever source and target disagree on list depth.
/// A `List[T]` source into a `T` target is implicit expand (set
/// the target's `lane_mode = Expand, lane_depth = delta`). A
/// `T` source into a `List[T]` target is implicit gather (set
/// the target's `lane_mode = Gather`). Matches the warnings
/// already emitted by validate.rs so runtime and compiler agree.
fn infer_lane_modes(project: &mut ProjectDefinition) {
    use weft_core::project::LaneMode;

    let mut edits: Vec<(String, String, LaneMode, u32)> = Vec::new();
    for edge in &project.edges {
        let Some(src_handle) = edge.source_handle.as_deref() else { continue };
        let Some(tgt_handle) = edge.target_handle.as_deref() else { continue };
        let Some(src_node) = project.nodes.iter().find(|n| n.id == edge.source) else { continue };
        let Some(tgt_node) = project.nodes.iter().find(|n| n.id == edge.target) else { continue };
        let Some(src_port) = src_node.outputs.iter().find(|p| p.name == src_handle) else { continue };
        let Some(tgt_port) = tgt_node.inputs.iter().find(|p| p.name == tgt_handle) else { continue };

        let src_depth = list_depth(&src_port.port_type);
        let tgt_depth = list_depth(&tgt_port.port_type);
        if src_depth > tgt_depth {
            let delta = (src_depth - tgt_depth) as u32;
            edits.push((
                edge.target.clone(),
                tgt_handle.to_string(),
                LaneMode::Expand,
                delta,
            ));
        } else if tgt_depth > src_depth {
            let delta = (tgt_depth - src_depth) as u32;
            edits.push((
                edge.target.clone(),
                tgt_handle.to_string(),
                LaneMode::Gather,
                delta,
            ));
        }
    }

    for (node_id, port_name, mode, depth) in edits {
        let Some(node) = project.nodes.iter_mut().find(|n| n.id == node_id) else { continue };
        let Some(port) = node.inputs.iter_mut().find(|p| p.name == port_name) else { continue };
        // Don't clobber an explicit Expand/Gather on the port
        // (the node catalog declared its own lane mechanics).
        if port.lane_mode == LaneMode::Single {
            port.lane_mode = mode;
            port.lane_depth = depth;
        }
    }
}

fn list_depth(ty: &WeftType) -> usize {
    match ty {
        WeftType::List(inner) => 1 + list_depth(inner),
        _ => 0,
    }
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
        // Form-derived ports come from form_field_specs in the
        // node's metadata, not the .weft source.
        user_typed: false,
    }
}

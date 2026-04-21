//! Post-compilation enrichment of ProjectDefinition using the NodeTypeRegistry.
//!
//! SYNC WARNING: The frontend has a parallel pipeline in weft-parser.ts
//! (resolveAndValidateTypes). Both must produce identical errors for the same
//! input. When changing logic here, update weft-parser.ts too (and vice versa).
//! This backend pipeline is the authoritative check at execution time; the
//! frontend copy provides instant editor feedback.
//!
//! The weft compiler is a pure parser (string → ProjectDefinition) that lives in
//! weft-core and cannot depend on weft-nodes. After compilation, this
//! module enriches the output with metadata from the actual node registry:
//!
//! - Populates `features` (isTrigger, isInfrastructure, canAddInputPorts, etc.)
//! - Merges weft-declared ports with catalog ports:
//!   - Catalog is source of truth for portType and laneMode
//!   - Weft can override `required` (promote with `*`)
//!   - Weft can add new custom ports if the node supports canAddInputPorts/canAddOutputPorts
//!   - Nodes with hasFormSchema derive ports from config.fields; those are preserved too
//! - Filters out nodes not in the registry (UI-only: Annotation, etc.)

use std::collections::HashMap;
use weft_core::project::{ProjectDefinition, NodeDefinition, PortDefinition, LaneMode};
use weft_core::WeftType;
use crate::node::FormFieldSpec;
use crate::registry::NodeTypeRegistry;

/// Merge weft-declared ports with catalog ports.
///
/// Rules:
/// 1. Start with catalog ports as the base.
/// 2. For each weft-declared port that matches a catalog port by name:
///    - Override `required` if weft says `*` (promote to required).
///    - Validate laneMode: if weft specifies one and it conflicts with catalog, log a warning
///      and use catalog's laneMode. If it matches or weft doesn't specify, use catalog.
///    - portType: catalog is source of truth. Weft type is ignored (catalog wins).
/// 3. For weft-declared ports that don't match any catalog port:
///    - If the node supports custom ports (can_add), add them with weft-declared attributes.
///    - Otherwise, log a warning and skip.
/// 4. Apply lane_modes() overrides from the node registry (for nodes like ForEach, Collect).
fn merge_ports(
    catalog_ports: &[PortDefinition],
    weft_ports: &[PortDefinition],
    can_add: bool,
    node_id: &str,
    direction: &str,
    errors: &mut Vec<String>,
) -> Vec<PortDefinition> {
    use weft_core::WeftType;

    let catalog_map: std::collections::HashMap<&str, &PortDefinition> = catalog_ports.iter()
        .map(|p| (p.name.as_str(), p))
        .collect();

    // Start with catalog ports, applying weft overrides
    let mut result: Vec<PortDefinition> = catalog_ports.iter().map(|cp| {
        if let Some(wp) = weft_ports.iter().find(|wp| wp.name == cp.name) {
            let mut merged = cp.clone();
            // v2: Weft can override required/optional in either direction.
            // If the Weft code explicitly declares a port, its required flag wins.
            merged.required = wp.required;
            // Type override rules:
            // - MustOverride catalog: weft provides the type (required).
            // - Non-MustOverride catalog: weft can narrow (compatible subset) or re-state.
            //   The narrowed type becomes the actual type for downstream validation.
            //   Incompatible types = error.
            if !wp.portType.is_must_override() {
                if cp.portType.is_must_override() {
                    merged.portType = wp.portType.clone();
                } else if WeftType::is_compatible(&wp.portType, &cp.portType) {
                    // Weft type fits into catalog type (exact match or valid narrowing).
                    // Apply narrowed type so downstream validation uses it.
                    merged.portType = wp.portType.clone();
                } else {
                    errors.push(format!(
                        "Node '{}': {} port '{}' has catalog type {} but Weft declares incompatible type {}",
                        node_id, direction, cp.name, cp.portType, wp.portType,
                    ));
                }
            }
            // Validate laneMode
            if wp.laneMode != LaneMode::Single && wp.laneMode != cp.laneMode {
                tracing::warn!(
                    "enrich_project: node '{}' {} port '{}': weft declares {:?} but catalog says {:?}. Using catalog.",
                    node_id, direction, cp.name, wp.laneMode, cp.laneMode
                );
            }
            merged
        } else {
            cp.clone()
        }
    }).collect();

    // Add new custom ports from weft that aren't in the catalog
    for wp in weft_ports {
        if catalog_map.contains_key(wp.name.as_str()) {
            continue;
        }
        if can_add {
            if wp.portType.is_must_override() {
                errors.push(format!(
                    "Node '{}': new {} port '{}' requires a type declaration",
                    node_id, direction, wp.name,
                ));
            }
            result.push(wp.clone());
        } else {
            tracing::warn!(
                "enrich_project: node '{}' {} port '{}': custom port declared in weft but node does not support custom {} ports. Skipping.",
                node_id, direction, wp.name, direction
            );
        }
    }

    result
}

/// Enrich a compiled ProjectDefinition with metadata from the node registry.
///
/// For each node:
/// 1. Set features from the catalog.
/// 2. Merge weft-declared ports (from `in:`/`out:` blocks) with catalog ports.
/// 3. Apply lane_modes() overrides from the node registry.
/// 4. Remove nodes not in the registry (UI-only: Annotation, etc.)
pub fn enrich_project(wf: &mut ProjectDefinition, registry: &NodeTypeRegistry) -> Result<(), Vec<String>> {
    // Check for unknown node types (not in registry, not Passthrough).
    // UI-only nodes (Annotation) are silently removed. Unknown types are errors.
    let ui_only_types: std::collections::HashSet<&str> = ["Annotation"].into_iter().collect();
    let mut remove_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut unknown_type_errors: Vec<String> = Vec::new();

    for node in &wf.nodes {
        if node.groupBoundary.is_some() { continue; }
        let type_str = node.nodeType.0.as_str();
        if registry.get(type_str).is_none() {
            if ui_only_types.contains(type_str) {
                remove_ids.insert(node.id.clone());
            } else {
                unknown_type_errors.push(format!("Unknown node type '{}' on node '{}'", type_str, node.id));
            }
        }
    }

    if !unknown_type_errors.is_empty() {
        return Err(unknown_type_errors);
    }

    if !remove_ids.is_empty() {
        tracing::debug!("enrich_project: filtering out {} UI-only nodes: {:?}", remove_ids.len(), remove_ids);
        wf.nodes.retain(|n| !remove_ids.contains(&n.id));
        wf.edges.retain(|e| !remove_ids.contains(&e.source) && !remove_ids.contains(&e.target));
    }

    let mut type_errors: Vec<String> = Vec::new();

    for node in &mut wf.nodes {
        let type_str = node.nodeType.0.as_str();

        // Group boundary nodes have ports from the Weft group declaration, not the catalog.
        // Skip port merge, their ports are already correct from the compiler.
        if node.groupBoundary.is_some() {
            continue;
        }

        if let Some(backend_node) = registry.get(type_str) {
            let meta = backend_node.metadata();

            // Read custom-port flags before moving features.
            // hasFormSchema nodes derive ports from config.fields at build time;
            // those ports must survive enrichment even though canAdd*Ports is false.
            let has_form_schema = meta.features.hasFormSchema;
            let can_add_inputs = meta.features.canAddInputPorts || has_form_schema;
            let can_add_outputs = meta.features.canAddOutputPorts || has_form_schema;

            // Preserve weft-declared oneOfRequired before overwriting with catalog features
            let weft_one_of = std::mem::take(&mut node.features.oneOfRequired);

            // Set features from registry
            node.features = meta.features;

            // Merge weft-declared oneOfRequired groups (append to catalog's)
            for group in weft_one_of {
                if !node.features.oneOfRequired.contains(&group) {
                    node.features.oneOfRequired.push(group);
                }
            }

            // Build catalog port lists
            let catalog_inputs: Vec<PortDefinition> = meta.inputs.iter().map(|p| PortDefinition {
                name: p.name.to_string(),
                portType: p.portType.clone(),
                required: p.required,
                description: None,
                laneMode: p.laneMode,
                laneDepth: 1,
                configurable: p.configurable,
            }).collect();
            let catalog_outputs: Vec<PortDefinition> = meta.outputs.iter().map(|p| PortDefinition {
                name: p.name.to_string(),
                portType: p.portType.clone(),
                required: p.required,
                description: None,
                laneMode: p.laneMode,
                laneDepth: 1,
                configurable: p.configurable,
            }).collect();

            // node.inputs from the compiler contains weft-declared ports (from in: blocks).
            let weft_inputs = std::mem::take(&mut node.inputs);
            let weft_outputs = std::mem::take(&mut node.outputs);

            node.inputs = merge_ports(&catalog_inputs, &weft_inputs, can_add_inputs, &node.id, "input", &mut type_errors);
            node.outputs = merge_ports(&catalog_outputs, &weft_outputs, can_add_outputs, &node.id, "output", &mut type_errors);

            // For hasFormSchema nodes, derive input/output ports from config.fields
            // using the node's form_field_specs. This mirrors the frontend's
            // deriveInputsFromFields / deriveOutputsFromFields logic.
            if has_form_schema {
                let specs = backend_node.form_field_specs();
                if !specs.is_empty() {
                    let (derived_inputs, derived_outputs) = derive_ports_from_form_fields(&node.config, &specs);
                    // Add derived ports that don't already exist
                    for port in derived_inputs {
                        if !node.inputs.iter().any(|p| p.name == port.name) {
                            node.inputs.push(port);
                        }
                    }
                    for port in derived_outputs {
                        if !node.outputs.iter().any(|p| p.name == port.name) {
                            node.outputs.push(port);
                        }
                    }
                }
            }

            // Apply lane mode overrides from the node registry.
            // This lets nodes like ForEach (Expand output) and Collect (Gather input)
            // declare their lane modes without requiring every PortDef to carry the field.
            let lane_modes = backend_node.lane_modes();
            for (port_name, mode) in &lane_modes {
                for port in node.inputs.iter_mut().chain(node.outputs.iter_mut()) {
                    if port.name == *port_name {
                        port.laneMode = *mode;
                    }
                }
            }
        }
    }

    // Step 0.5: Literal-driven dynamic input port synthesis.
    //
    // For each node, look at `node.config` keys that are NOT already in
    // `node.inputs`. Each such key is a user-written literal assignment
    // for a port that the catalog doesn't declare. On canAddInputPorts
    // nodes, synthesize the port with the type inferred from the literal.
    // On frozen-port nodes, this is a compile error. The required flag
    // on a literal-only port is effectively a no-op (no edge can deliver
    // null); if an edge is later added, edge synthesis or an explicit
    // declaration overrides it.
    synthesize_literal_driven_ports(wf, registry, &mut type_errors);

    // Step 0.6: Synthesize EDGE-driven ports. For every edge targeting a
    // key that doesn't exist yet on a canAddInputPorts node, create the
    // port with a fresh TypeVar and `required: true`. resolve_and_narrow
    // flows the edge source's type into the TypeVar during Step 2.
    // Required-by-default means "skip this node if the upstream sends
    // null": the common case where an upstream skip should cascade
    // downstream. Users who want the node to tolerate nulls declare the
    // port explicitly with `?` in the signature; the explicit declaration
    // wins over edge synthesis.
    synthesize_edge_driven_ports(wf, registry, &mut type_errors);

    // Step 1: Call resolve_types for nodes that implement it (Pack, Unpack, etc.)
    for node in &mut wf.nodes {
        let type_str = node.nodeType.0.as_str();
        if let Some(backend_node) = registry.get(type_str) {
            let resolved = backend_node.resolve_types(&node.inputs, &node.outputs);
            for (name, pt) in &resolved.inputs {
                if let Some(port) = node.inputs.iter_mut().find(|p| p.name == *name) {
                    port.portType = pt.clone();
                }
            }
            for (name, pt) in &resolved.outputs {
                if let Some(port) = node.outputs.iter_mut().find(|p| p.name == *name) {
                    port.portType = pt.clone();
                }
            }
        }
    }

    // Step 2: Resolve TypeVars AND narrow input types in one pass.
    // For each edge: narrow the target port type from the source, AND extract
    // TypeVar bindings. Both happen together because narrowing determines which
    // part of a union matched, and TypeVar extraction uses that match.
    resolve_and_narrow(wf, &mut type_errors);

    // Step 2.05: Narrow group-boundary passthrough output ports from their
    // incoming edge source types. Passthrough inputs stay as-declared (the
    // group signature is the contract), but outputs should reflect what was
    // actually wired in so inner consumers see the narrowed type instead of
    // the wider declared type. Applied iteratively because narrowing one
    // passthrough can feed into another.
    narrow_group_passthroughs(wf);

    // Step 2.1: For every configurable input port that has a same-named config
    // value on the node, infer the value's type and check it against the port
    // type. This catches "user wrote `template: 42` but the port expects String"
    // at enrich time instead of silently failing at runtime.
    validate_config_filled_ports(wf, &mut type_errors);

    // Step 2.2: Enforce required ports must be satisfied. A required port is
    // satisfied by an incoming edge, or, for configurable ports, by a non-null
    // same-named config value. Wired-only required ports with no edge are
    // compile errors (e.g. `config` on EmailSend must be wired to EmailConfig).
    validate_required_ports(wf, &mut type_errors);

    // Step 2.5: Infer expand/gather from type mismatches on edges
    infer_lane_modes(wf, &mut type_errors);

    // Step 3: Validate stack depth (expand/gather balance)
    validate_stack_depth(wf, &mut type_errors);

    // Step 4: Validate edge type compatibility (now that TypeVars are resolved)
    validate_edge_types(wf, &mut type_errors);

    // Step 5: Check for remaining unresolved TypeVars and MustOverride on connected ports
    validate_no_unresolved(wf, &mut type_errors);

    // Step 6: Validate edge ports exist
    if let Err(edge_errors) = validate_edge_ports(wf) {
        type_errors.extend(edge_errors);
    }

    if type_errors.is_empty() {
        Ok(())
    } else {
        for e in &type_errors {
            tracing::error!("[enrich_project] {}", e);
        }
        Err(type_errors)
    }
}

/// Sentinel TypeVar name emitted by `FormFieldPort::any` and any other catalog
/// helper that wants "an auto-scoped TypeVar per port instance, independent
/// from sibling ports". At port-materialization time, every occurrence of
/// this marker (including inside List/Dict/Union) is replaced with a unique
/// TypeVar scoped to the surrounding field key.
const AUTO_TYPE_VAR_MARKER: &str = "T_Auto";

/// Replace every occurrence of the `T_Auto` marker in `port_type` with a
/// key-scoped TypeVar like `T__{key}`. Other TypeVars (explicit `T`, `T1`,
/// etc.) are left alone so that catalog authors can still express "these
/// ports must share a type" by writing `T` explicitly.
fn materialize_auto_type_vars(port_type: &WeftType, key: &str) -> WeftType {
    match port_type {
        WeftType::TypeVar(name) if name == AUTO_TYPE_VAR_MARKER => {
            WeftType::TypeVar(format!("T__{}", key))
        }
        WeftType::List(inner) => {
            WeftType::List(Box::new(materialize_auto_type_vars(inner, key)))
        }
        WeftType::Dict(k, v) => WeftType::Dict(
            Box::new(materialize_auto_type_vars(k, key)),
            Box::new(materialize_auto_type_vars(v, key)),
        ),
        WeftType::Union(types) => WeftType::union(
            types.iter().map(|t| materialize_auto_type_vars(t, key)).collect()
        ),
        _ => port_type.clone(),
    }
}

/// Derive input and output ports from a node's config.fields using form field specs.
/// Mirrors the frontend's deriveInputsFromFields / deriveOutputsFromFields.

fn derive_ports_from_form_fields(
    config: &serde_json::Value,
    specs: &[FormFieldSpec],
) -> (Vec<PortDefinition>, Vec<PortDefinition>) {
    let fields = match config.get("fields") {
        Some(serde_json::Value::Array(arr)) => arr.clone(),
        Some(serde_json::Value::String(s)) => {
            serde_json::from_str::<Vec<serde_json::Value>>(s).unwrap_or_default()
        }
        _ => return (vec![], vec![]),
    };

    let spec_map: std::collections::HashMap<&str, &FormFieldSpec> = specs.iter()
        .map(|s| (s.field_type, s))
        .collect();

    let mut inputs = Vec::new();
    let mut outputs = Vec::new();

    for field in &fields {
        let field_type = field.get("fieldType").and_then(|v| v.as_str()).unwrap_or("display");
        let key = match field.get("key").and_then(|v| v.as_str()) {
            Some(k) => k,
            None => continue,
        };

        let spec = match spec_map.get(field_type) {
            Some(s) => s,
            None => {
                tracing::warn!("Unknown form field type '{}' for key '{}' : no ports derived", field_type, key);
                continue;
            }
        };

        // Form field ports default to required (same as the language default).
        // Set "required": false explicitly to make a port optional.
        let required = field.get("required").and_then(|v| v.as_bool()).unwrap_or(true);

        for port_template in &spec.adds_inputs {
            let portType = materialize_auto_type_vars(&port_template.port_type, key);
            let configurable = portType.is_default_configurable();
            inputs.push(PortDefinition {
                name: port_template.resolve_name(key),
                portType,
                required,
                description: None,
                laneMode: LaneMode::Single,
                laneDepth: 1,
                configurable,
            });
        }

        for port_template in &spec.adds_outputs {
            let portType = materialize_auto_type_vars(&port_template.port_type, key);
            let configurable = portType.is_default_configurable();
            outputs.push(PortDefinition {
                name: port_template.resolve_name(key),
                portType,
                required: false,
                description: None,
                laneMode: LaneMode::Single,
                laneDepth: 1,
                configurable,
            });
        }
    }

    (inputs, outputs)
}

/// Combined TypeVar resolution + type narrowing in one pass.
/// For each node: narrow input ports from source types, extract TypeVar bindings,
/// apply bindings to all ports. Processes edges iteratively until stable.
pub fn resolve_and_narrow(wf: &mut ProjectDefinition, errors: &mut Vec<String>) {
    // Multiple iterations: narrowing on one node may enable resolution on downstream nodes.
    let max_iterations = wf.nodes.len() + 1;
    for _ in 0..max_iterations {
        let mut changed = false;

        // Build fresh port type snapshots each iteration
        let weft_type: HashMap<(String, String, String), WeftType> = {
            let mut m = HashMap::new();
            for node in &wf.nodes {
                for port in &node.outputs {
                    m.insert((node.id.clone(), "out".to_string(), port.name.clone()), port.portType.clone());
                }
                for port in &node.inputs {
                    m.insert((node.id.clone(), "in".to_string(), port.name.clone()), port.portType.clone());
                }
            }
            m
        };
        let port_lane_modes: HashMap<(String, String, String), LaneMode> = {
            let mut m = HashMap::new();
            for node in &wf.nodes {
                for port in &node.inputs {
                    m.insert((node.id.clone(), "in".to_string(), port.name.clone()), port.laneMode);
                }
            }
            m
        };

        for node_idx in 0..wf.nodes.len() {
            let node_id = wf.nodes[node_idx].id.clone();
            let mut bindings: HashMap<String, WeftType> = HashMap::new();
            // Provenance side-channel: maps TypeVar name -> human-readable string
            // describing where it was first bound. Used by bind_type_var to emit
            // informative conflict errors like "T was bound to X by edge foo->bar".
            let mut binding_sources: HashMap<String, String> = HashMap::new();

            // Phase 1: For each incoming edge, narrow input port and extract TypeVar bindings
            for edge in &wf.edges {
                if edge.target == node_id {
                    let target_port_name = edge.targetHandle.as_deref().unwrap_or("default");
                    let target_port_type = wf.nodes[node_idx].inputs.iter()
                        .find(|p| p.name == target_port_name)
                        .map(|p| p.portType.clone());

                    let source_type = weft_type.get(&(
                        edge.source.clone(),
                        "out".to_string(),
                        edge.sourceHandle.clone().unwrap_or_else(|| "default".to_string()),
                    ));

                    let src_type = match source_type {
                        Some(t) if !t.is_type_var() => t,
                        _ => continue,
                    };
                    if let Some(target_type) = target_port_type {
                        // 1. Extract TypeVar bindings by structural matching
                        if target_type.is_type_var() || contains_type_var(&target_type) {
                            {
                                let ctx = format!("edge {}.{} -> {}.{}", edge.source, edge.sourceHandle.as_deref().unwrap_or("default"), edge.target, edge.targetHandle.as_deref().unwrap_or("default"));
                                extract_type_var_bindings(&target_type, src_type, &mut bindings, &mut binding_sources, &ctx, &node_id, errors);
                            }
                        }

                        // 2. Apply TypeVar substitutions immediately so narrowing sees concrete types
                        if !bindings.is_empty() {
                            if let Some(port) = wf.nodes[node_idx].inputs.iter_mut()
                                .find(|p| p.name == target_port_name)
                            {
                                let substituted = substitute_type_vars(&port.portType, &bindings);
                                if substituted != port.portType {
                                    port.portType = substituted;
                                    changed = true;
                                }
                            }
                        }

                        // 4. No narrowing on input ports.
                        // Input types declare what the node *accepts*, not what it *receives*.
                        // Narrowing inputs causes bugs when the declared type is intentionally
                        // wider than the source (e.g. List[List[String] | Null] to accept
                        // null lanes from gather).

                    }
                }

                // Phase 2: For outgoing edges, extract TypeVar bindings from target
                if edge.source == node_id {
                    let source_port_type = wf.nodes[node_idx].outputs.iter()
                        .find(|p| p.name == edge.sourceHandle.as_deref().unwrap_or("default"))
                        .map(|p| &p.portType);

                    if let Some(src_type) = source_port_type {
                        if src_type.is_type_var() || contains_type_var(src_type) {
                            let target_key = (
                                edge.target.clone(),
                                "in".to_string(),
                                edge.targetHandle.clone().unwrap_or_else(|| "default".to_string()),
                            );
                            if let Some(tgt_type) = weft_type.get(&target_key) {
                                if !tgt_type.is_type_var() {
                                    let lane_mode = port_lane_modes.get(&target_key).copied().unwrap_or(LaneMode::Single);
                                    let wire_type = match lane_mode {
                                        LaneMode::Single => tgt_type.clone(),
                                        LaneMode::Expand => WeftType::list(tgt_type.clone()),
                                        LaneMode::Gather => {
                                            match tgt_type {
                                                WeftType::List(inner) => *inner.clone(),
                                                other => other.clone(),
                                            }
                                        }
                                    };
                                    if !wire_type.is_type_var() {
                                        {
                                        let ctx = format!("edge {}.{} -> {}.{}", edge.source, edge.sourceHandle.as_deref().unwrap_or("default"), edge.target, edge.targetHandle.as_deref().unwrap_or("default"));
                                        extract_type_var_bindings(src_type, &wire_type, &mut bindings, &mut binding_sources, &ctx, &node_id, errors);
                                    }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Apply bindings
            if !bindings.is_empty() {
                let node = &mut wf.nodes[node_idx];
                for port in node.inputs.iter_mut().chain(node.outputs.iter_mut()) {
                    let new_type = substitute_type_vars(&port.portType, &bindings);
                    if new_type != port.portType {
                        port.portType = new_type;
                        changed = true;
                    }
                }
            }
        }

        if !changed { break; }
    }
}

// resolve_type_vars and narrow_input_types removed : replaced by resolve_and_narrow above.

/// Check if a WeftType contains any TypeVar references (at any depth).
fn contains_type_var(pt: &WeftType) -> bool {
    match pt {
        WeftType::TypeVar(_) => true,
        WeftType::List(inner) => contains_type_var(inner),
        WeftType::Dict(k, v) => contains_type_var(k) || contains_type_var(v),
        WeftType::Union(types) => types.iter().any(contains_type_var),
        _ => false,
    }
}

/// Extract TypeVar bindings by structurally matching a pattern (with TypeVars) against a concrete type.
/// E.g. pattern List[T] + concrete List[Dict[String, Number]] → bindings {T: Dict[String, Number]}
fn extract_type_var_bindings(
    pattern: &WeftType,
    concrete: &WeftType,
    bindings: &mut HashMap<String, WeftType>,
    binding_sources: &mut HashMap<String, String>,
    source_context: &str,
    node_id: &str,
    errors: &mut Vec<String>,
) {
    match (pattern, concrete) {
        (WeftType::TypeVar(name), _) if !concrete.is_type_var() => {
            bind_type_var(bindings, binding_sources, source_context, name, concrete, node_id, errors);
        }
        (WeftType::List(p_inner), WeftType::List(c_inner)) => {
            extract_type_var_bindings(p_inner, c_inner, bindings, binding_sources, source_context, node_id, errors);
        }
        (WeftType::Dict(pk, pv), WeftType::Dict(ck, cv)) => {
            extract_type_var_bindings(pk, ck, bindings, binding_sources, source_context, node_id, errors);
            extract_type_var_bindings(pv, cv, bindings, binding_sources, source_context, node_id, errors);
        }
        // Union pattern against non-union concrete: wrap concrete in a single-element "union"
        (WeftType::Union(p_types), concrete) if !matches!(concrete, WeftType::Union(_)) => {
            let c_types = vec![concrete.clone()];
            let concrete_patterns: Vec<&WeftType> = p_types.iter().filter(|p| !matches!(p, WeftType::TypeVar(_))).collect();
            let type_vars: Vec<&str> = p_types.iter().filter_map(|p| if let WeftType::TypeVar(n) = p { Some(n.as_str()) } else { None }).collect();

            if !type_vars.is_empty() {
                let mut remaining: Vec<WeftType> = Vec::new();
                for c in &c_types {
                    let matched = concrete_patterns.iter().any(|p| WeftType::is_compatible(c, p) && WeftType::is_compatible(p, c));
                    if !matched { remaining.push(c.clone()); }
                }
                // Resolve extras to Empty (bottom type) when source is more specific.
                // e.g. pattern `Number | T` matched against `Number` → T = Empty.
                // Empty in a union adds nothing: `Number | Empty` = `Number`.
                while remaining.len() < type_vars.len() {
                    remaining.push(WeftType::Primitive(weft_core::weft_type::WeftPrimitive::Empty));
                }
                {
                    for (i, var_name) in type_vars.iter().enumerate() {
                        if i == type_vars.len() - 1 {
                            let resolved = if remaining.len() == 1 { remaining.remove(0) } else { WeftType::union(remaining.drain(..).collect()) };
                            bind_type_var(bindings, binding_sources, source_context, var_name, &resolved, node_id, errors);
                        } else {
                            bind_type_var(bindings, binding_sources, source_context, var_name, &remaining.remove(0), node_id, errors);
                        }
                    }
                }
            }
        }
        (WeftType::Union(p_types), WeftType::Union(c_types)) => {
            // Separate pattern into concrete types and TypeVars
            let mut concrete_patterns: Vec<&WeftType> = Vec::new();
            let mut type_vars: Vec<&str> = Vec::new();
            for p in p_types {
                match p {
                    WeftType::TypeVar(name) => type_vars.push(name),
                    other => concrete_patterns.push(other),
                }
            }

            if type_vars.is_empty() {
                // No TypeVars in union : just recurse on matching pairs
                if p_types.len() == c_types.len() {
                    for (p, c) in p_types.iter().zip(c_types.iter()) {
                        extract_type_var_bindings(p, c, bindings, binding_sources, source_context, node_id, errors);
                    }
                }
            } else {
                // Remove concrete types matched by pattern's concrete parts
                let mut remaining: Vec<WeftType> = Vec::new();
                for c in c_types {
                    let matched = concrete_patterns.iter().any(|p| WeftType::is_compatible(c, p) && WeftType::is_compatible(p, c));
                    if !matched {
                        remaining.push(c.clone());
                    }
                }

                // Resolve extras to Empty (bottom type) when source is more specific.
                while remaining.len() < type_vars.len() {
                    remaining.push(WeftType::Primitive(weft_core::weft_type::WeftPrimitive::Empty));
                }
                {
                    // Each TypeVar consumes one type, last one takes all remaining
                    for (i, var_name) in type_vars.iter().enumerate() {
                        if i == type_vars.len() - 1 {
                            // Last TypeVar: absorb all remaining
                            let resolved = if remaining.len() == 1 {
                                remaining.remove(0)
                            } else {
                                WeftType::union(remaining.drain(..).collect())
                            };
                            bind_type_var(bindings, binding_sources, source_context, var_name, &resolved, node_id, errors);
                        } else {
                            // Not last: consume one
                            let resolved = remaining.remove(0);
                            bind_type_var(bindings, binding_sources, source_context, var_name, &resolved, node_id, errors);
                        }
                    }
                }
            }
        }
        _ => {} // No TypeVar to extract, or incompatible structures
    }
}

/// Try to bind a TypeVar name to a concrete type. If already bound, check consistency.
fn bind_type_var(
    bindings: &mut HashMap<String, WeftType>,
    binding_sources: &mut HashMap<String, String>,
    source_context: &str,
    var_name: &str,
    concrete: &WeftType,
    node_id: &str,
    errors: &mut Vec<String>,
) {
    if let Some(existing) = bindings.get(var_name) {
        if !WeftType::is_compatible(concrete, existing) && !WeftType::is_compatible(existing, concrete) {
            let prior = binding_sources.get(var_name).cloned().unwrap_or_else(|| "prior wiring".to_string());
            errors.push(format!(
                "Node '{}': type variable {} has conflicting bindings, bound to {} by {}, but {} expects {}",
                node_id, var_name, existing, prior, source_context, concrete
            ));
        }
    } else {
        bindings.insert(var_name.to_string(), concrete.clone());
        binding_sources.insert(var_name.to_string(), source_context.to_string());
    }
}

/// Replace TypeVar references with their bound types. Recurses into List, Dict, Stack, Union.
/// For every node, scan its input ports. For each port that is configurable,
/// doesn't have an incoming edge (edges take precedence), and has a matching
/// non-null config value on the node, infer the value's WeftType and check it
/// against the port type. Errors are collected into `type_errors`.
/// Enforce that every required input port on every non-passthrough node is
/// satisfied. A port is satisfied if:
///   1. it has an incoming edge, OR
///   2. it is configurable AND has a non-null same-named config value.
///
/// Group In boundaries are validated against their user-facing interface
/// ports: a required interface input must be wired from outside. Group Out
/// boundaries are skipped (their inputs are fed by inner nodes, which are
/// validated individually). If it compiles it runs, so an unwired required
/// group input is a compile-time error, not a runtime skip.
fn validate_required_ports(wf: &mut ProjectDefinition, errors: &mut Vec<String>) {
    use weft_core::project::GroupBoundaryRole;

    let edge_targets: std::collections::HashSet<(String, String)> = wf.edges.iter()
        .map(|e| {
            let port = e.targetHandle.clone().unwrap_or_else(|| "default".to_string());
            (e.target.clone(), port)
        })
        .collect();

    for node in &wf.nodes {
        // Skip Out boundaries: their "required" semantics are handled by
        // whichever inner node produces the output, not by the boundary.
        let is_out_boundary = node.groupBoundary.as_ref()
            .map(|gb| gb.role == GroupBoundaryRole::Out)
            .unwrap_or(false);
        if is_out_boundary { continue; }

        // For In boundaries, report errors against the group id (friendlier
        // than `grp__in`) with the "Group" label.
        let (display_id, display_label) = match node.groupBoundary.as_ref() {
            Some(gb) if gb.role == GroupBoundaryRole::In => {
                (gb.groupId.clone(), "Group")
            }
            _ => (node.id.clone(), "Node"),
        };
        let is_group_boundary = node.groupBoundary.is_some();

        for port in &node.inputs {
            if !port.required { continue; }
            if edge_targets.contains(&(node.id.clone(), port.name.clone())) {
                continue; // wired
            }
            // Group interface ports cannot be config-filled: they must be
            // wired. Regular nodes may satisfy a required port via config if
            // the port is configurable.
            if !is_group_boundary && port.configurable {
                let has_config = node.config.get(&port.name)
                    .map(|v| !v.is_null())
                    .unwrap_or(false);
                if has_config { continue; }
            }
            let hint = if is_group_boundary {
                " (wire an edge into this group input)".to_string()
            } else if port.configurable {
                format!(" (wire an edge or set a '{}' config value)", port.name)
            } else {
                " (wire an edge, this port cannot be filled by config)".to_string()
            };
            errors.push(format!(
                "{} '{}': required input port '{}' is not connected{}",
                display_label, display_id, port.name, hint,
            ));
        }

        // oneOfRequired groups: at least one port in each group must be
        // satisfied (wired, or config-filled for non-group nodes).
        for group in &node.features.oneOfRequired {
            if group.is_empty() { continue; }
            let any_satisfied = group.iter().any(|port_name| {
                if edge_targets.contains(&(node.id.clone(), port_name.clone())) {
                    return true;
                }
                if !is_group_boundary {
                    let port = node.inputs.iter().find(|p| &p.name == port_name);
                    if let Some(p) = port {
                        if p.configurable {
                            if let Some(v) = node.config.get(port_name) {
                                if !v.is_null() { return true; }
                            }
                        }
                    }
                }
                false
            });
            if !any_satisfied {
                let group_str = group.join(", ");
                errors.push(format!(
                    "{} '{}': at least one of [{}] must be connected",
                    display_label, display_id, group_str,
                ));
            }
        }
    }
}

/// Synthesize input ports for literal config values whose keys aren't
/// declared anywhere by the catalog (not a catalog input port, not a
/// catalog config field, not a catalog output port) nor by the weft port
/// signature. Runs after merge_ports. Mirrors the frontend's synthesis pass.
///
/// Rule (see docs): a literal `key: value` in a node block or on a
/// connection line falls into one of these buckets:
///   - catalog input port → normal config-fill, no synthesis
///   - catalog config field → user-set knob, no synthesis
///   - catalog output port → ERROR (can't set what the node produces)
///   - undeclared + canAddInputPorts → synthesize an input port with
///     type inferred via WeftType::infer. `required: false` is a cosmetic
///     default; it has no runtime effect on literal-only ports (no edge
///     can deliver null) and is overridden if an edge is later added.
///   - undeclared + !canAddInputPorts → ERROR (fixed port signature)
///
/// Edges to undeclared keys are handled by synthesize_edge_driven_ports
/// (the pass that runs right after this one), not here.
fn synthesize_literal_driven_ports(
    wf: &mut ProjectDefinition,
    registry: &NodeTypeRegistry,
    errors: &mut Vec<String>,
) {
    // Reserved config keys that are metadata, not ports.
    // - `label`: node display name (user-set).
    // - `parentId`: internal scope marker the parser attaches to group
    //   children and inline anons so downstream stages know which group
    //   they belong to.
    const RESERVED: &[&str] = &["label", "parentId"];

    for node in &mut wf.nodes {
        if node.groupBoundary.is_some() { continue; }

        let type_str = node.nodeType.0.as_str();
        let backend_node = match registry.get(type_str) { Some(b) => b, None => continue };
        let meta = backend_node.metadata();
        let can_add = meta.features.canAddInputPorts || meta.features.hasFormSchema;

        // Catalog-declared names we must NOT synthesize ports for.
        // - Input port names: already-declared inputs (no synthesis needed).
        // - Catalog `fields`: user-set knobs, legitimate config keys, skip.
        // - Output port names: producing values, not receiving them → error.
        let declared_inputs: std::collections::HashSet<String> =
            node.inputs.iter().map(|p| p.name.clone()).collect();
        let catalog_field_names: std::collections::HashSet<String> =
            meta.fields.iter().map(|f| f.key.to_string()).collect();
        let catalog_output_names: std::collections::HashSet<String> =
            meta.outputs.iter().map(|p| p.name.to_string()).collect();

        let candidates: Vec<String> = match node.config.as_object() {
            Some(obj) => obj.keys()
                .filter(|k| !RESERVED.contains(&k.as_str()))
                .filter(|k| !declared_inputs.contains(k.as_str()))
                .filter(|k| !catalog_field_names.contains(k.as_str()))
                .cloned()
                .collect(),
            None => continue,
        };

        for key in candidates {
            let value = node.config.get(&key).cloned().unwrap_or(serde_json::Value::Null);
            // Null config values carry no information.
            if value.is_null() { continue; }

            // Rule #3: literal assignment to an output port is an error.
            if catalog_output_names.contains(&key) {
                errors.push(format!(
                    "Node '{}' (type '{}'): cannot assign a literal to output port '{}'. Output ports are produced by the node, not set by the user.",
                    node.id, type_str, key,
                ));
                continue;
            }

            // Rule #4: undeclared key.
            if !can_add {
                errors.push(format!(
                    "Node '{}' (type '{}'): cannot add custom input port '{}' because this node type has a fixed port signature. Remove the assignment or use a node type that supports custom input ports.",
                    node.id, type_str, key,
                ));
                continue;
            }

            let inferred = WeftType::infer(&value);
            node.inputs.push(PortDefinition {
                name: key,
                portType: inferred,
                required: false,
                description: None,
                laneMode: weft_core::LaneMode::Single,
                laneDepth: 1,
                configurable: true,
            });
        }
    }
}

/// Synthesize input ports for edges that target an undeclared port on a
/// `canAddInputPorts` node. Rule:
///   - Target has catalog input `x` → no synthesis (catalog wins)
///   - Target has weft-declared input `x` → no synthesis (signature wins)
///   - Target is canAddInputPorts AND `x` is undeclared → synthesize
///     `x: TypeVar(fresh), required: true`. The fresh TypeVar unifies with
///     the edge source's type during resolve_and_narrow, so no type is
///     hand-copied here.
///   - Target is not canAddInputPorts AND `x` is undeclared → leave alone;
///     validate_edge_ports will emit a clear error later.
///
/// Literal-driven synthesis already ran, so by the time we get here, any
/// port that has a literal default is already in `node.inputs`. Edges whose
/// target matches a literal-synthesized port are fine as-is (the edge just
/// shadows the literal default at runtime).
fn synthesize_edge_driven_ports(
    wf: &mut ProjectDefinition,
    registry: &NodeTypeRegistry,
    _errors: &mut Vec<String>,
) {
    // Reserved keys that are metadata, not ports. An edge targeting these
    // is a real error (wiring to `label` makes no sense) and should be left
    // for validate_edge_ports to catch with its native error message.
    const RESERVED: &[&str] = &["label", "parentId"];

    // Build a map of node id → (can_add, catalog_output_names, catalog_field_names,
    // existing_input_names). We need these to decide whether a given edge
    // target is already covered or must be synthesized.
    struct TargetInfo {
        can_add: bool,
        catalog_outputs: std::collections::HashSet<String>,
        catalog_fields: std::collections::HashSet<String>,
        existing_inputs: std::collections::HashSet<String>,
    }
    let mut target_info: std::collections::HashMap<String, TargetInfo> = std::collections::HashMap::new();
    for node in &wf.nodes {
        if node.groupBoundary.is_some() { continue; }
        let type_str = node.nodeType.0.as_str();
        let backend_node = match registry.get(type_str) { Some(b) => b, None => continue };
        let meta = backend_node.metadata();
        let can_add = meta.features.canAddInputPorts || meta.features.hasFormSchema;
        target_info.insert(node.id.clone(), TargetInfo {
            can_add,
            catalog_outputs: meta.outputs.iter().map(|p| p.name.to_string()).collect(),
            catalog_fields: meta.fields.iter().map(|f| f.key.to_string()).collect(),
            existing_inputs: node.inputs.iter().map(|p| p.name.clone()).collect(),
        });
    }

    // Walk edges; for each one whose target has an undeclared input port on
    // a canAddInputPorts node, synthesize the port.
    // Collect (target_id, port_name) pairs first so we can push_back into
    // node.inputs without borrow conflicts.
    let mut to_synthesize: Vec<(String, String)> = Vec::new();
    let mut already_added: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for edge in &wf.edges {
        let target_id = edge.target.as_str();
        let target_port = edge.targetHandle.as_deref().unwrap_or("default");
        // Reserved metadata keys are never ports, regardless of canAdd.
        if RESERVED.contains(&target_port) { continue; }
        let info = match target_info.get(target_id) { Some(i) => i, None => continue };

        // Already a valid port? Nothing to do.
        if info.existing_inputs.contains(target_port) { continue; }
        // Catalog config field (a user-set knob, not a port)? Leave alone.
        if info.catalog_fields.contains(target_port) { continue; }
        // Hitting an output port? Leave it; validate_edge_ports will error.
        if info.catalog_outputs.contains(target_port) { continue; }
        // Node is frozen? Leave it; validate_edge_ports will error.
        if !info.can_add { continue; }
        // Dedup in case multiple edges target the same synthesized port
        // (shouldn't happen under the 1-driver rule, but be safe).
        let key = (target_id.to_string(), target_port.to_string());
        if already_added.insert(key.clone()) {
            to_synthesize.push(key);
        }
    }

    // Apply the synthesis. The fresh TypeVar name includes the node id and
    // port name so it can't collide with any other TypeVar in the project.
    for (target_id, port_name) in to_synthesize {
        if let Some(node) = wf.nodes.iter_mut().find(|n| n.id == target_id) {
            let scope = format!("{}_{}", target_id, port_name);
            // Sanitize: TypeVar names use only [A-Za-z0-9_].
            let sanitized: String = scope.chars()
                .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
                .collect();
            node.inputs.push(PortDefinition {
                name: port_name,
                portType: WeftType::TypeVar(format!("T__{}", sanitized)),
                required: true,
                description: None,
                laneMode: weft_core::LaneMode::Single,
                laneDepth: 1,
                configurable: true,
            });
            // Update target_info so downstream edges in the same pass see
            // the new port as existing.
            if let Some(info) = target_info.get_mut(&target_id) {
                info.existing_inputs.insert(node.inputs.last().unwrap().name.clone());
            }
        }
    }
}

fn validate_config_filled_ports(wf: &mut ProjectDefinition, errors: &mut Vec<String>) {
    // Build set of (target_node_id, target_port_name) that have an incoming edge.
    let edge_targets: std::collections::HashSet<(String, String)> = wf.edges.iter()
        .filter_map(|e| {
            let port = e.targetHandle.clone().unwrap_or_else(|| "default".to_string());
            Some((e.target.clone(), port))
        })
        .collect();

    for node in &wf.nodes {
        // Skip group boundary nodes, they don't have a real backing config.
        if node.groupBoundary.is_some() { continue; }
        let config = match node.config.as_object() {
            Some(o) => o,
            None => continue,
        };
        for port in &node.inputs {
            // A config value exists for this port name. If the port is
            // wired (edge wins at runtime), keep the config as-is (stale
            // values are harmless, runtime ignores them). If the port is
            // NOT configurable but a value is present, it's a parse error:
            // wired-only ports cannot accept a literal.
            let has_config_value = config.get(&port.name)
                .map(|v| !v.is_null())
                .unwrap_or(false);
            if !has_config_value { continue; }

            if edge_targets.contains(&(node.id.clone(), port.name.clone())) {
                // Edge wins, literal is kept but unused. Skip the type check.
                continue;
            }

            if !port.configurable {
                errors.push(format!(
                    "Node '{}': input port '{}' is wired-only and cannot be set from config. Wire an edge instead.",
                    node.id, port.name,
                ));
                continue;
            }

            // Unresolved port types (TypeVar/MustOverride) can't be checked.
            if port.portType.is_unresolved() { continue; }

            let value = config.get(&port.name).unwrap();
            let inferred = WeftType::infer(value);
            if !WeftType::is_compatible(&inferred, &port.portType) {
                errors.push(format!(
                    "Node '{}': config field '{}' has type {} but the port expects {}",
                    node.id, port.name, inferred, port.portType,
                ));
            }
        }
    }
}

/// Narrow group-boundary passthrough output ports from their incoming-edge
/// source types. A group `__in` / `__out` passthrough has identical input and
/// output port shapes (from the group signature); the input side is the
/// declared contract, but the output should reflect the actual wired source
/// type so inner/outer consumers see the narrowed type. Runs iteratively:
/// narrowing one passthrough can feed into another downstream.
fn narrow_group_passthroughs(wf: &mut ProjectDefinition) {
    use weft_core::WeftType;

    // Build (node_id, port_name) -> output_type for non-passthrough nodes only once;
    // passthrough types change during iteration so we re-read each iteration.
    let max_iterations = wf.nodes.len() + 1;
    for _ in 0..max_iterations {
        // Snapshot current output port types for every node (needed to read wf
        // while mutating it).
        let out_types: std::collections::HashMap<(String, String), WeftType> = {
            let mut m = std::collections::HashMap::new();
            for node in &wf.nodes {
                for port in &node.outputs {
                    m.insert((node.id.clone(), port.name.clone()), port.portType.clone());
                }
            }
            m
        };

        let mut changed = false;
        for node_idx in 0..wf.nodes.len() {
            if wf.nodes[node_idx].groupBoundary.is_none() {
                continue;
            }
            let node_id = wf.nodes[node_idx].id.clone();

            // For each output port, find the matching input port (same name) and
            // any incoming edge feeding that input. The output narrows to the
            // incoming source type when it's a subtype of the declared output.
            let output_names: Vec<String> = wf.nodes[node_idx].outputs.iter()
                .map(|p| p.name.clone())
                .collect();

            for out_name in output_names {
                // Find the incoming edge to this passthrough's matching input port.
                let incoming_src = wf.edges.iter()
                    .find(|e| e.target == node_id
                        && e.targetHandle.as_deref().unwrap_or("default") == out_name)
                    .and_then(|e| {
                        let src_port = e.sourceHandle.clone().unwrap_or_else(|| "default".to_string());
                        out_types.get(&(e.source.clone(), src_port)).cloned()
                    });

                let Some(src_type) = incoming_src else { continue; };
                if src_type.is_unresolved() {
                    continue;
                }

                let out_port = wf.nodes[node_idx].outputs.iter_mut()
                    .find(|p| p.name == out_name);
                let Some(out_port) = out_port else { continue; };

                // Only narrow if the incoming source type is compatible with
                // (i.e., subtype of) the declared output type. If incompatible,
                // leave the declared type; validate_edge_types will catch the
                // mismatch downstream.
                if !WeftType::is_compatible(&src_type, &out_port.portType) {
                    continue;
                }
                if src_type != out_port.portType {
                    out_port.portType = src_type;
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }
}

fn substitute_type_vars(port_type: &WeftType, bindings: &HashMap<String, WeftType>) -> WeftType {
    match port_type {
        WeftType::TypeVar(name) => {
            bindings.get(name).cloned().unwrap_or_else(|| port_type.clone())
        }
        WeftType::List(inner) => WeftType::List(Box::new(substitute_type_vars(inner, bindings))),
        WeftType::Dict(k, v) => WeftType::Dict(
            Box::new(substitute_type_vars(k, bindings)),
            Box::new(substitute_type_vars(v, bindings)),
        ),
        WeftType::Union(types) => WeftType::union(
            types.iter().map(|t| substitute_type_vars(t, bindings)).collect()
        ),
        _ => port_type.clone(),
    }
}

/// Validate stack depth: expand/gather must be balanced.
/// Walks the graph topologically, tracking stack depth at each node.
/// - Expand input: depth increases by 1. Validates upstream type is List[T].
/// - Gather input: depth decreases by 1. Validates depth > 0 (there's a stack to gather).
/// - Output ports inherit the node's post-input depth, modified by output lane modes.
/// Infer expand/gather lane modes from type mismatches on edges.
/// For each edge, compare the source output type with the target input type.
/// If the source has more List[] wrappers, set target to Expand.
/// If the target has more List[] wrappers, set target to Gather.
/// Only infers on ports that are currently LaneMode::Single (doesn't override explicit modes).
pub fn infer_lane_modes(wf: &mut ProjectDefinition, _errors: &mut Vec<String>) {
    use weft_core::WeftType;

    // Build port type lookups, separate maps for inputs and outputs to avoid
    // name collisions (e.g. select_input creates both input List[String] and output String
    // with the same port name).
    let input_types: std::collections::HashMap<(String, String), WeftType> = {
        let mut m = std::collections::HashMap::new();
        for node in &wf.nodes {
            for port in &node.inputs {
                m.insert((node.id.clone(), port.name.clone()), port.portType.clone());
            }
        }
        m
    };
    let output_types: std::collections::HashMap<(String, String), WeftType> = {
        let mut m = std::collections::HashMap::new();
        for node in &wf.nodes {
            for port in &node.outputs {
                m.insert((node.id.clone(), port.name.clone()), port.portType.clone());
            }
        }
        m
    };

    // For each edge, check type compatibility and infer lane mode
    let mut inferences: Vec<(String, String, LaneMode, u32)> = Vec::new(); // (node_id, port_name, mode, depth)

    for edge in &wf.edges {
        let source_port = edge.sourceHandle.as_deref().unwrap_or("default");
        let target_port = edge.targetHandle.as_deref().unwrap_or("default");

        let src_type = match output_types.get(&(edge.source.clone(), source_port.to_string())) {
            Some(t) if !t.is_unresolved() => t.clone(),
            _ => continue,
        };
        let tgt_type = match input_types.get(&(edge.target.clone(), target_port.to_string())) {
            Some(t) if !t.is_unresolved() => t.clone(),
            _ => continue,
        };

        // Check if target port already has an explicit lane mode (from catalog or </>)
        let current_mode = wf.nodes.iter()
            .find(|n| n.id == edge.target)
            .and_then(|n| n.inputs.iter().find(|p| p.name == target_port))
            .map(|p| p.laneMode)
            .unwrap_or(LaneMode::Single);
        if current_mode != LaneMode::Single {
            continue; // Don't override explicit modes
        }

        // If types are directly compatible, no lane mode change needed
        let compat = WeftType::is_compatible(&src_type, &tgt_type);
        if compat {
            continue;
        }

        // Try expand: peel List[] wrappers from source, check if inner is compatible with target
        if let Some(depth) = try_expand_depth(&src_type, &tgt_type) {
            inferences.push((edge.target.clone(), target_port.to_string(), LaneMode::Expand, depth));
            continue;
        }

        // Try gather: peel List[] wrappers from target, check if source is compatible with inner
        if let Some(depth) = try_gather_depth(&src_type, &tgt_type) {
            inferences.push((edge.target.clone(), target_port.to_string(), LaneMode::Gather, depth));
            continue;
        }

        // Neither expand nor gather works, and types aren't compatible: type error
        // (This will be caught by validate_edge_types later, so we don't error here)
    }

    // Apply inferences
    for (node_id, port_name, mode, depth) in inferences {
        if let Some(node) = wf.nodes.iter_mut().find(|n| n.id == node_id) {
            if let Some(port) = node.inputs.iter_mut().find(|p| p.name == port_name) {
                port.laneMode = mode;
                port.laneDepth = depth;
                // Adjust port type to the element type (post-expand/gather)
                match mode {
                    LaneMode::Expand => {
                        // Port type should be the element type after peeling `depth` List wrappers from source
                        // We leave port type as declared by the user (it's the target type)
                    }
                    LaneMode::Gather => {
                        // Port type stays as declared (it's the collected List type)
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Try to find how many List[] wrappers to peel from source to make it compatible with target.
/// Returns Some(depth) if peeling `depth` List wrappers from source yields a type compatible with target.
fn try_expand_depth(src: &WeftType, tgt: &WeftType) -> Option<u32> {
    let mut current = src.clone();
    let mut depth = 0u32;
    loop {
        match current {
            WeftType::List(inner) => {
                depth += 1;
                if WeftType::is_compatible(&inner, tgt) {
                    return Some(depth);
                }
                current = *inner;
            }
            _ => return None,
        }
    }
}

/// Try to find how many List[] wrappers to peel from target to make source compatible with the inner type.
/// Returns Some(depth) if source is compatible with the type after peeling `depth` List wrappers from target.
fn try_gather_depth(src: &WeftType, tgt: &WeftType) -> Option<u32> {
    let mut current = tgt.clone();
    let mut depth = 0u32;
    loop {
        match current {
            WeftType::List(inner) => {
                depth += 1;
                if WeftType::is_compatible(src, &inner) {
                    return Some(depth);
                }
                current = *inner;
            }
            _ => return None,
        }
    }
}

pub fn validate_stack_depth(wf: &ProjectDefinition, errors: &mut Vec<String>) {
    let node_map: HashMap<&str, &NodeDefinition> = wf.nodes.iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    // Build incoming edges per node
    let mut incoming: HashMap<&str, Vec<&weft_core::project::Edge>> = HashMap::new();
    for edge in &wf.edges {
        incoming.entry(edge.target.as_str()).or_default().push(edge);
    }

    // Compute stack depth at each node's output using BFS
    // depth_at_output[node_id] = stack depth after processing this node's lane modes
    let mut depth_at_output: HashMap<&str, i32> = HashMap::new();

    // Topological order: process nodes whose all sources have been processed
    let mut processed = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<&str> = std::collections::VecDeque::new();

    // Start with nodes that have no incoming edges.
    // Account for output lane modes (e.g. splitter with Expand output starts at depth 1).
    for node in &wf.nodes {
        let has_incoming = wf.edges.iter().any(|e| e.target == node.id);
        if !has_incoming {
            let mut depth: i32 = 0;
            for op in &node.outputs {
                match op.laneMode {
                    LaneMode::Expand => { depth = 1; }
                    LaneMode::Gather => { depth = -1; } // will be caught as error
                    LaneMode::Single => {}
                }
            }
            depth_at_output.insert(&node.id, depth);
            processed.insert(node.id.as_str());
            queue.push_back(&node.id);
        }
    }

    // BFS: process each node, propagate depth to targets
    let max_iterations = wf.nodes.len() * 2 + 1;
    let mut iterations = 0;
    while let Some(source_id) = queue.pop_front() {
        iterations += 1;
        if iterations > max_iterations { break; } // cycle protection

        let source_depth = *depth_at_output.get(source_id).unwrap_or(&0);

        // Find all edges from this node
        for edge in &wf.edges {
            if edge.source != source_id { continue; }
            let target_id = edge.target.as_str();
            let target_node = match node_map.get(target_id) {
                Some(n) => n,
                None => continue,
            };

            // Determine the input depth at the target port
            let target_port_name = edge.targetHandle.as_deref().unwrap_or("default");
            let target_port = target_node.inputs.iter().find(|p| p.name == target_port_name);

            let mut target_depth = source_depth;

            if let Some(port) = target_port {
                let lane_depth = port.laneDepth.max(1) as i32;
                match port.laneMode {
                    LaneMode::Expand => {
                        // Validate: source must output enough List[] levels for the expand depth
                        let source_port_name = edge.sourceHandle.as_deref().unwrap_or("default");
                        if let Some(source_node) = node_map.get(source_id) {
                            if let Some(sp) = source_node.outputs.iter().find(|p| p.name == source_port_name) {
                                if !sp.portType.is_unresolved() {
                                    let mut check_type = sp.portType.clone();
                                    for _level in 0..lane_depth {
                                        match check_type {
                                            WeftType::List(inner) => { check_type = *inner; }
                                            _ => {
                                                errors.push(format!(
                                                    "Expand error: {}.{} receives {} but needs {} List level(s) to expand",
                                                    target_id, target_port_name, sp.portType, lane_depth,
                                                ));
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        target_depth += lane_depth;
                    }
                    LaneMode::Gather => {
                        if source_depth < lane_depth {
                            errors.push(format!(
                                "Gather error: {}.{} tries to gather {} level(s) but current depth is only {}. Add an Expand upstream first.",
                                target_id, target_port_name, lane_depth, source_depth,
                            ));
                        }
                        target_depth -= lane_depth;
                    }
                    LaneMode::Single => {}
                }
            }

            // Also check output lane modes on the target node
            let mut output_depth = target_depth;
            for op in &target_node.outputs {
                let op_depth = op.laneDepth.max(1) as i32;
                match op.laneMode {
                    LaneMode::Expand => { output_depth = target_depth + op_depth; }
                    LaneMode::Gather => {
                        if target_depth < op_depth {
                            errors.push(format!(
                                "Gather error: {}.{} tries to gather {} level(s) but current depth is only {}.",
                                target_id, op.name, op_depth, target_depth,
                            ));
                        }
                        output_depth = target_depth - op_depth;
                    }
                    LaneMode::Single => {}
                }
            }

            // Depth merging: lower depths broadcast to higher depths.
            // Take the max : runtime validates shape compatibility.
            if let Some(&existing) = depth_at_output.get(target_id) {
                if output_depth > existing {
                    depth_at_output.insert(target_id, output_depth);
                }
            } else {
                depth_at_output.insert(target_id, output_depth);
            }

            if !processed.contains(target_id) {
                // Check if all incoming edges' sources have been processed
                let all_sources_done = incoming.get(target_id)
                    .map(|edges| edges.iter().all(|e| processed.contains(e.source.as_str())))
                    .unwrap_or(true);
                if all_sources_done {
                    processed.insert(target_id);
                    queue.push_back(target_id);
                }
            }
        }
    }
}

/// Validate type compatibility on all edges.
/// Compute the wire type for a port, accounting for lane mode transformations.
/// Port types are post-operation. Wire types are what flows on the edge.
///
/// Source output:
///   Single: wire = declared
///   Expand: wire = declared (each lane carries element T, node produced List[T] which was split)
///   Gather: wire = declared (node collected into List[T], that's what flows out)
///
/// Target input (expected wire type):
///   Single: wire = declared
///   Expand: wire = List[declared] (a list arrives, expand unwraps it)
///   Gather: wire = inner(declared) (individual elements arrive, gather collects into List[T])
fn source_wire_type(port: &PortDefinition) -> WeftType {
    // For source ports, the wire type equals the declared type regardless of lane mode.
    // Expand output: node produces List[T], expand splits, wire carries T = declared.
    // Gather output: node collects, wire carries List[T] = declared.
    // Single: wire = declared.
    port.portType.clone()
}

fn target_expected_wire_type(port: &PortDefinition) -> WeftType {
    let depth = port.laneDepth.max(1);
    match port.laneMode {
        LaneMode::Single => port.portType.clone(),
        LaneMode::Expand => {
            // Expand input: declared = T (post-expand element). Wire must carry List[...[T]...] (depth levels).
            let mut wire = port.portType.clone();
            for _ in 0..depth {
                wire = WeftType::list(wire);
            }
            wire
        }
        LaneMode::Gather => {
            // Gather input: declared = List[...[T]...] (post-gather). Wire carries the innermost T.
            let mut wire = port.portType.clone();
            for _ in 0..depth {
                match wire {
                    WeftType::List(inner) => wire = *inner,
                    other => { wire = other; break; }
                }
            }
            wire
        }
    }
}

pub fn validate_edge_types(wf: &ProjectDefinition, errors: &mut Vec<String>) {
    let node_map: HashMap<&str, &NodeDefinition> = wf.nodes.iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    for edge in &wf.edges {
        let source_port_name = edge.sourceHandle.as_deref().unwrap_or("default");
        let target_port_name = edge.targetHandle.as_deref().unwrap_or("default");

        let source_port = node_map.get(edge.source.as_str())
            .and_then(|n| n.outputs.iter().find(|p| p.name == source_port_name));
        let target_port = node_map.get(edge.target.as_str())
            .and_then(|n| n.inputs.iter().find(|p| p.name == target_port_name));

        if let (Some(sp), Some(tp)) = (source_port, target_port) {
            let src_wire = source_wire_type(sp);
            let tgt_wire = target_expected_wire_type(tp);

            if src_wire.is_unresolved() || tgt_wire.is_unresolved() {
                continue;
            }
            // Null at the top level of the source type is never a type error.
            // Required ports skip the node on null (null propagation).
            // Optional ports pass null through to the node's code.
            // Either way, the executor handles it. Strip Null from the source
            // before checking compatibility so that e.g. String | Null flowing
            // into String (required or optional) is accepted.
            let effective_src = if src_wire.contains_null() {
                src_wire.without_null()
            } else {
                src_wire.clone()
            };
            if !WeftType::is_compatible(&effective_src, &tgt_wire) {
                errors.push(format!(
                    "Type mismatch: {}.{} outputs {} (wire: {}) but {}.{} expects {} (wire: {})",
                    edge.source, source_port_name, sp.portType, src_wire,
                    edge.target, target_port_name, tp.portType, tgt_wire,
                ));
            }
        }
    }
}

/// Check for remaining MustOverride ports that have connections (they should have been overridden).
pub fn validate_no_unresolved(wf: &ProjectDefinition, errors: &mut Vec<String>) {
    let mut connected_inputs: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let mut connected_outputs: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for edge in &wf.edges {
        connected_outputs.insert((edge.source.clone(), edge.sourceHandle.clone().unwrap_or_default()));
        connected_inputs.insert((edge.target.clone(), edge.targetHandle.clone().unwrap_or_default()));
    }

    for node in &wf.nodes {
        for port in &node.inputs {
            let key = (node.id.clone(), port.name.clone());
            if !connected_inputs.contains(&key) { continue; }
            if port.portType.is_must_override() {
                errors.push(format!(
                    "Node '{}': input port '{}' has type MustOverride : declare the type in Weft (e.g. {}: String)",
                    node.id, port.name, port.name,
                ));
            } else if contains_type_var(&port.portType) {
                errors.push(format!(
                    "Node '{}': input port '{}' has unresolved type variable in '{}' : could not infer type from connections",
                    node.id, port.name, port.portType,
                ));
            }
        }
        for port in &node.outputs {
            let key = (node.id.clone(), port.name.clone());
            if !connected_outputs.contains(&key) { continue; }
            if port.portType.is_must_override() {
                errors.push(format!(
                    "Node '{}': output port '{}' has type MustOverride : declare the type in Weft (e.g. {}: String)",
                    node.id, port.name, port.name,
                ));
            } else if contains_type_var(&port.portType) {
                errors.push(format!(
                    "Node '{}': output port '{}' has unresolved type variable in '{}' : could not infer type from connections",
                    node.id, port.name, port.portType,
                ));
            }
        }
    }
}

/// Validate that every edge references ports that exist on the source/target nodes.
/// Returns errors for any edge that references a non-existent port.
fn validate_edge_ports(wf: &ProjectDefinition) -> Result<(), Vec<String>> {
    let node_map: std::collections::HashMap<&str, &NodeDefinition> = wf.nodes.iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    let mut errors = Vec::new();

    for edge in &wf.edges {
        let source_port = edge.sourceHandle.as_deref().unwrap_or("default");
        let target_port = edge.targetHandle.as_deref().unwrap_or("default");

        if let Some(source_node) = node_map.get(edge.source.as_str()) {
            if !source_node.outputs.iter().any(|p| p.name == source_port) {
                errors.push(format!(
                    "Edge '{}': source node '{}' ({}) has no output port '{}'. Available outputs: {:?}",
                    edge.id, edge.source, source_node.nodeType,
                    source_port,
                    source_node.outputs.iter().map(|p| &p.name).collect::<Vec<_>>(),
                ));
            }
        }

        if let Some(target_node) = node_map.get(edge.target.as_str()) {
            if !target_node.inputs.iter().any(|p| p.name == target_port) {
                errors.push(format!(
                    "Edge '{}': target node '{}' ({}) has no input port '{}'. Available inputs: {:?}",
                    edge.id, edge.target, target_node.nodeType,
                    target_port,
                    target_node.inputs.iter().map(|p| &p.name).collect::<Vec<_>>(),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        for e in &errors {
            tracing::error!("[enrich_project] {}", e);
        }
        Err(errors)
    }
}

#[cfg(test)]
#[path = "tests/enrich_tests.rs"]
mod tests;

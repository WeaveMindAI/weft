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

use weft_core::node::{FormFieldPort, MetadataCatalog, Widget};
use weft_core::project::{InputDefinition, PortDefinition, ProjectDefinition, Span};
use weft_core::weft_type::{Exposure, WeftType};

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

/// Move every body value that DRIVES a wireable input out of `config`
/// into `port_literals`, now that the full input list is known: a
/// braces value on an `all`-exposure input, and an assignment statement
/// (`ConfigOrigin::Connection`) on ANY input (the statement names the
/// input unambiguously; validate rejects it where the input's exposure
/// forbids it). After this pass a value has one home per FORM: a
/// wireable input's driving value lives in `port_literals` (the engine
/// feeds it onto the input), while a `config`-exposure input's braces
/// value stays in `config` (it is design-time configuration, merged
/// into the runtime bag at construction, and edited through the config
/// home in the editor). A braces value on an assignment-/wire-only
/// input also stays in `config`, where validate rejects it
/// (`port-literal-placement`).
fn normalize_port_literals(node: &mut weft_core::project::NodeDefinition) {
    use weft_core::project::ConfigOrigin;
    let Some(cfg) = node.config.as_object_mut() else { return };
    let moved: Vec<String> = node
        .inputs
        .iter()
        .filter(|input| {
            let literal_form = node
                .config_spans
                .get(&input.name)
                .is_some_and(|s| s.origin == ConfigOrigin::Connection);
            cfg.contains_key(&input.name) && (input.exposure == Exposure::All || literal_form)
        })
        .map(|input| input.name.clone())
        .collect();
    for name in moved {
        if let Some(value) = cfg.remove(&name) {
            node.port_literals.insert(name.clone(), value);
        }
        if let Some(span) = node.config_spans.remove(&name) {
            node.port_literal_spans.insert(name, span);
        }
    }
}

/// The name/type/required core the input and output merges share. The
/// two directions genuinely need distinct instance types (an input
/// carries exposure + editor surface, an output is a bare wire port),
/// but the source-vs-catalog merge logic is one rule; this trait keeps
/// it written once.
trait MergeSlot: Clone {
    fn name(&self) -> &str;
    fn slot_type(&self) -> &WeftType;
    fn set_slot_type(&mut self, ty: WeftType);
    fn required(&self) -> bool;
    fn set_required(&mut self, required: bool);
}

impl MergeSlot for InputDefinition {
    fn name(&self) -> &str { &self.name }
    fn slot_type(&self) -> &WeftType { &self.port_type }
    fn set_slot_type(&mut self, ty: WeftType) { self.port_type = ty; }
    fn required(&self) -> bool { self.required }
    fn set_required(&mut self, required: bool) { self.required = required; }
}

impl MergeSlot for PortDefinition {
    fn name(&self) -> &str { &self.name }
    fn slot_type(&self) -> &WeftType { &self.port_type }
    fn set_slot_type(&mut self, ty: WeftType) { self.port_type = ty; }
    fn required(&self) -> bool { self.required }
    fn set_required(&mut self, required: bool) { self.required = required; }
}

fn merge_ports<T: MergeSlot>(
    catalog_ports: &[T],
    weft_ports: &[T],
    can_add: bool,
    node_id: &str,
    span: Span,
    direction: PortDirection,
    errors: &mut Vec<EnrichError>,
) -> Vec<T> {
    use std::collections::HashMap;

    let catalog_by_name: HashMap<&str, &T> = catalog_ports
        .iter()
        .map(|p| (p.name(), p))
        .collect();

    let mut result: Vec<T> = catalog_ports
        .iter()
        .map(|cp| {
            let Some(wp) = weft_ports.iter().find(|w| w.name() == cp.name()) else {
                return cp.clone();
            };
            let mut merged = cp.clone();
            merged.set_required(wp.required());
            if !wp.slot_type().is_must_override() {
                if cp.slot_type().is_must_override()
                    || WeftType::is_compatible(wp.slot_type(), cp.slot_type())
                {
                    merged.set_slot_type(wp.slot_type().clone());
                } else {
                    errors.push(EnrichError { span, message: format!(
                        "node '{}': {} port '{}' declared type {} incompatible with catalog type {}",
                        node_id,
                        direction.as_str(),
                        cp.name(),
                        wp.slot_type(),
                        cp.slot_type(),
                    )});
                }
            }
            merged
        })
        .collect();

    for wp in weft_ports {
        if catalog_by_name.contains_key(wp.name()) {
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
                wp.name(),
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
        if wp.slot_type().is_must_override() {
            errors.push(EnrichError { span, message: format!(
                "node '{}': custom {} port '{}' needs a concrete type",
                node_id,
                direction.as_str(),
                wp.name(),
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

        let catalog_inputs: Vec<InputDefinition> = meta
            .inputs
            .iter()
            .map(|spec| InputDefinition {
                name: spec.name.clone(),
                port_type: spec.input_type.clone(),
                required: spec.required,
                description: spec.description.clone(),
                exposure: spec.effective_exposure(),
                // The DECLARED widget only; the type-derived default is
                // stamped after TypeVar resolution (see the final pass),
                // so a `T` input resolved to Image gets a file picker.
                widget: spec.widget.clone(),
                default: spec.default.clone(),
                label: spec.label.clone(),
                placeholder: spec.placeholder.clone(),
                synthesized_from_carry: false,
                from_spec: true,
            })
            .collect();
        let mut catalog_outputs: Vec<PortDefinition> = meta
            .outputs
            .iter()
            .map(|p| PortDefinition {
                name: p.name.clone(),
                port_type: p.port_type.clone(),
                required: p.required,
                description: p.description.clone(),
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

        // A header PORT declaration naming a `config`-exposure input is
        // a collision, not a merge: a config input is not wireable, so
        // "I declared it as a port" can only mean the user expects to
        // wire it. Every input name is in ONE namespace now, so this is
        // the whole collision rule (a custom port can no longer shadow a
        // config input silently, the hole the old field/port split had).
        for wp in &weft_inputs {
            if let Some(cp) = catalog_inputs.iter().find(|cp| cp.name == wp.name) {
                if cp.exposure == Exposure::Config {
                    errors.push(EnrichError { span: node_span, message: format!(
                        "node '{}': input '{}' of node type {} is configuration-only \
                         (exposure `config`); it cannot be declared as a port. Set it in \
                         the config braces instead",
                        node.id, wp.name, node.node_type,
                    )});
                }
            }
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
        normalize_port_literals(node);
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

    // Effective-widget stamping, AFTER TypeVar resolution so a `T` input
    // resolved to Image gets the file picker its concrete type implies.
    // Declared widgets are already in place from the catalog mapping;
    // everything still blank (type-derived inputs, user-added ports,
    // form-derived inputs) fills from the RESOLVED instance type. Runs
    // even when errors were collected: the editor renders lenient
    // parses and needs every input's widget regardless.
    for node in project.nodes.iter_mut() {
        for input in node.inputs.iter_mut() {
            if input.widget.is_none() {
                input.widget = Some(Widget::default_for_type(&input.port_type));
            }
        }
        cast_literals(node);
    }
    errors
}

/// Literal lenience: a written literal whose JSON shape doesn't match
/// its input's declared type but UNAMBIGUOUSLY converts (a `"18"` on a
/// Number input, a JSON-in-a-string on a structural input) is cast in
/// place at compile time, in the compiled definition only (source keeps
/// what the author wrote). One cast site: validate, the runtime, and
/// the editor all read the cast value. A value that cannot cast is left
/// untouched, so validate reports the genuine mismatch
/// (`config-type-mismatch`). Same gate as validate's literal
/// type-check: only inputs whose exposure admits a braces literal
/// (assignment-only inputs hold markers/handles, never castable data).
fn cast_literals(node: &mut weft_core::project::NodeDefinition) {
    for input in &node.inputs {
        // Only `all`/`config` inputs carry castable plain data.
        // Assignment-only inputs (files, TypeVars, MustOverride, or an
        // author-closed input) hold markers/handles whose written JSON
        // shape intentionally differs from the input type, so they are
        // deliberately excluded from casting AND from validate's
        // type-check (same gate there); the omission is a design
        // choice, not a gap.
        if !input.exposure.allows_braces_literal() {
            continue;
        }
        let stores = [
            node.port_literals.get(&input.name).cloned(),
            node.config.get(&input.name).cloned(),
        ];
        for (idx, value) in stores.into_iter().enumerate() {
            let Some(value) = value else { continue };
            if WeftType::is_compatible(&WeftType::infer(&value), &input.port_type) {
                continue;
            }
            if let Ok(cast) = input.port_type.cast_value(&value) {
                if idx == 0 {
                    node.port_literals.insert(input.name.clone(), cast);
                } else if let Some(cfg) = node.config.as_object_mut() {
                    cfg.insert(input.name.clone(), cast);
                }
            }
        }
    }
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
    for input in node.inputs.iter_mut() {
        changed |= replace_in_type(&mut input.port_type, var_name, concrete);
    }
    for output in node.outputs.iter_mut() {
        changed |= replace_in_type(&mut output.port_type, var_name, concrete);
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
) -> (Vec<InputDefinition>, Vec<PortDefinition>) {
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
            inputs.push(InputDefinition::from_wire_port(materialize_port(port, key, false)));
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
        port_type,
        required: !is_output,
        description: None,
        synthesized_from_carry: false,
    }
}

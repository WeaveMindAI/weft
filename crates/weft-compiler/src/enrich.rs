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
                requires_scopes: spec.requires_scopes.clone(),
                requires_values: spec.requires_values.clone(),
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

        // An ACCESS NODE (metadata carries the `service` recipe):
        // stamp the service name onto its `access` widget, so the
        // runtime bag and the editor read it off the instance. The
        // permissions are NOT materialized as an input: they are ticked
        // once at connect time and live on the stored connection, never
        // in source.
        if let Some(service_spec) = &meta.service {
            for input in catalog_inputs.iter_mut() {
                if let Some(Widget::Access { service }) = &mut input.widget {
                    *service = Some(service_spec.service.clone());
                }
            }
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
        // A node that hands out a connection to something it runs
        // itself carries that service's recipe from here on. Resolved
        // now, against the WHOLE catalog, because the built worker
        // ships only the node types this project uses and so usually
        // does not include the service's own access node.
        if let Some(service) = &meta.publishes {
            match weft_core::access::spec::spec_for_service(catalog.all(), service) {
                // Refused HERE too, not only where the connection is
                // finally written: a recipe shape a published
                // connection cannot have is an authoring mistake, and
                // it belongs on the author's screen at build time.
                Some(spec) => match weft_core::access::spec::publishable_fields(spec) {
                    Ok(_) => node.published_service = Some(spec.clone()),
                    Err(message) => errors.push(EnrichError { span: node_span, message }),
                },
                None => errors.push(EnrichError {
                    span: node_span,
                    message: format!(
                        "node '{}' publishes a '{service}' connection, but no node in the \
                         catalog declares that service",
                        node.id
                    ),
                }),
            }
        }
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
            // Bind typevars at ANY depth (a `List[T]` port wired to a
            // `List[Number]` binds `T = Number`), matching what the
            // validator counts as unresolved; a bare-`T`-only binder
            // would leave nested vars unresolved forever.
            let mut bindings = Vec::new();
            collect_type_var_bindings(&tgt_type, &src_type, &mut bindings);
            for (name, concrete) in &bindings {
                if substitute_type_var(project, &tgt_node, name, concrete) {
                    changed = true;
                }
            }
            let mut bindings = Vec::new();
            collect_type_var_bindings(&src_type, &tgt_type, &mut bindings);
            for (name, concrete) in &bindings {
                if substitute_type_var(project, &src_node, name, concrete) {
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }

    Ok(())
}

/// Walk `port` and `other` in parallel and record every typevar in
/// `port` that lines up with a RESOLVED subtree of `other`. Containers
/// recurse positionally, records by field name; a union is never
/// descended (which member aligns is ambiguous). Nominal aliases peel
/// (`structural()`) on both sides here, so an alias over a container
/// binds exactly like the spelled-out type; substitution never needs
/// to descend one, because a declared body is concrete by
/// construction (the registry and the wire parser both refuse a type
/// variable inside one).
fn collect_type_var_bindings(
    port: &WeftType,
    other: &WeftType,
    out: &mut Vec<(String, WeftType)>,
) {
    // Match on the STRUCTURAL types so a nominal alias behaves like
    // its spelled-out body (`Feed = Generator[Number]` feeding a
    // `Generator[T]` port binds `T = Number`); what gets BOUND is the
    // unpeeled type, so the nominal name survives into the enriched
    // shape.
    match (port.structural(), other.structural()) {
        (WeftType::TypeVar(name), _) => {
            // A stream never instantiates a type variable, in either
            // direction: a generic port receiving a whole stream would
            // get the raw live-handle marker it cannot pull from, and
            // a generic source cannot produce the live stream a
            // Generator input needs. Validate's
            // `generator-into-generic-port` rule reports both edges;
            // leaving the var unbound here is what keeps the bad
            // binding out of the enriched shape. (Element-wise binding
            // through matching Generator ports happens below.)
            if !other.contains_unresolved_leaf() && other.as_generator().is_none() {
                out.push((name.clone(), other.clone()));
            }
        }
        (WeftType::List(a), WeftType::List(b)) => collect_type_var_bindings(a, b, out),
        // Element-wise: `Generator[T]` wired to `Generator[Number]`
        // binds `T = Number` (a generic stream CONSUMER is well-typed;
        // only a bare `T` receiving a whole stream is refused above).
        (WeftType::Generator(a), WeftType::Generator(b)) => {
            collect_type_var_bindings(a, b, out)
        }
        (WeftType::Dict(ak, av), WeftType::Dict(bk, bv)) => {
            collect_type_var_bindings(ak, bk, out);
            collect_type_var_bindings(av, bv, out);
        }
        (WeftType::Record(af), WeftType::Record(bf)) => {
            for fa in af {
                if let Some(fb) = bf.iter().find(|f| f.name == fa.name) {
                    collect_type_var_bindings(&fa.ty, &fb.ty, out);
                }
            }
        }
        _ => {}
    }
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
        WeftType::List(inner) | WeftType::Generator(inner) => {
            replace_in_type(inner, var_name, concrete)
        }
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
        // Substitution rewrites every shape a typevar can OCCUR in,
        // a superset of the shapes collection binds through: collection
        // never descends a union, but a union member's `T` is the same
        // node-scoped variable a sibling port binds, so it must be
        // rewritten here or the port stays half-resolved and validate
        // reports unresolved-typevar on an edge that already
        // determined the type. No `Named` arm: the registry refuses a
        // declared body carrying a type variable, so an alias body
        // never holds one.
        WeftType::Record(fields) => {
            let mut any = false;
            for f in fields.iter_mut() {
                any |= replace_in_type(&mut f.ty, var_name, concrete);
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

/// Replace every `T_Auto` placeholder with a TypeVar scoped to the
/// field key, recursing through every container arm: `List[T_Auto]`
/// reads as "a list of anything, scoped to this field", so a nested
/// placeholder scopes exactly like a top-level one.
/// SYNC: materialize_auto_type_vars <-> packages/weft-graph/src/webview/lib/utils/form-field-specs.ts materializeAutoTypeVars
fn materialize_auto_type_vars(t: &WeftType, key: &str) -> WeftType {
    match t {
        WeftType::TypeVar(n) if n == "T_Auto" => WeftType::type_var(&format!("T__{key}")),
        WeftType::List(inner) => WeftType::List(Box::new(materialize_auto_type_vars(inner, key))),
        WeftType::Generator(inner) => {
            WeftType::Generator(Box::new(materialize_auto_type_vars(inner, key)))
        }
        WeftType::Dict(k, v) => WeftType::Dict(
            Box::new(materialize_auto_type_vars(k, key)),
            Box::new(materialize_auto_type_vars(v, key)),
        ),
        WeftType::Union(members) => {
            WeftType::Union(members.iter().map(|m| materialize_auto_type_vars(m, key)).collect())
        }
        WeftType::Record(fields) => WeftType::Record(
            fields
                .iter()
                .map(|f| weft_core::weft_type::RecordField {
                    name: f.name.clone(),
                    ty: materialize_auto_type_vars(&f.ty, key),
                    optional: f.optional,
                })
                .collect(),
        ),
        // No `Named` arm: a declared body is concrete by construction
        // (the registry and the wire parser both refuse a type
        // variable inside one), so there is never a `T_Auto` to
        // materialize beneath an alias, and rebuilding the body here
        // would be the door to two same-named types with different
        // bodies.
        other => other.clone(),
    }
}

fn materialize_port(template: &FormFieldPort, key: &str, is_output: bool) -> PortDefinition {
    let name = template.resolve_name(key);
    let port_type = materialize_auto_type_vars(&template.port_type, key);
    PortDefinition {
        name,
        port_type,
        required: !is_output,
        description: None,
        synthesized_from_carry: false,
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    fn parse(s: &str) -> WeftType {
        weft_core::weft_type::WeftType::parse(s).expect("type parses")
    }

    /// Layer-1: element-wise binding through Generator ports, and
    /// through a NOMINAL ALIAS of one (the match peels `structural()`,
    /// and what binds is the unpeeled type so the alias name survives).
    #[test]
    fn generator_element_binds_through_nominal_aliases() {
        // Bare: Generator[T] <- Generator[Number] binds T = Number.
        let mut out = Vec::new();
        collect_type_var_bindings(
            &parse("Generator[T]"),
            &parse("Generator[Number]"),
            &mut out,
        );
        assert_eq!(out, vec![("T".to_string(), parse("Number"))]);

        // Aliased source: Feed = Generator[Number] binds the same.
        let feed = WeftType::Named {
            name: "Feed".into(),
            body: Box::new(parse("Generator[Number]")),
        };
        let mut out = Vec::new();
        collect_type_var_bindings(&parse("Generator[T]"), &feed, &mut out);
        assert_eq!(out, vec![("T".to_string(), parse("Number"))]);

        // Aliased LIST source into List[T] binds T to the alias's
        // element too (same peel, and the UNPEELED element binds).
        let history = WeftType::Named {
            name: "ChatHistory".into(),
            body: Box::new(parse("List[Number]")),
        };
        let mut out = Vec::new();
        collect_type_var_bindings(&parse("List[T]"), &history, &mut out);
        assert_eq!(out, vec![("T".to_string(), parse("Number"))]);

        // A bare T never binds from a WHOLE stream, aliased or not.
        let mut out = Vec::new();
        collect_type_var_bindings(&parse("T"), &feed, &mut out);
        assert!(out.is_empty(), "a whole stream never instantiates a typevar: {out:?}");
    }
}

#[cfg(test)]
mod substitution_tests {
    use super::*;

    fn parse(s: &str) -> WeftType {
        weft_core::weft_type::WeftType::parse(s).expect("type parses")
    }

    /// Layer-1 invariant: substitution rewrites every shape a typevar
    /// can occur in, so a collected binding never leaves a port
    /// half-resolved. (No Named case: the registry refuses a declared
    /// body carrying a type variable, so an alias body never holds
    /// one.)
    #[test]
    fn substitution_rewrites_every_shape_a_typevar_can_occur_in() {
        // A record field carrying the var.
        let mut rec = parse("{a: T, b: String}");
        assert!(replace_in_type(&mut rec, "T", &parse("Number")));
        assert_eq!(rec, parse("{a: Number, b: String}"));

        // A generator element carrying the var.
        let mut gen = parse("Generator[T]");
        assert!(replace_in_type(&mut gen, "T", &parse("Number")));
        assert_eq!(gen, parse("Generator[Number]"));

        // A union member carrying the var: collection never binds
        // THROUGH a union, but the var is node-scoped, so a sibling
        // port's binding must resolve it here too.
        let mut union = parse("T | Null");
        assert!(replace_in_type(&mut union, "T", &parse("Number")));
        assert_eq!(union, parse("Number | Null"));

        // A dict value carrying the var.
        let mut dict = parse("Dict[String, T]");
        assert!(replace_in_type(&mut dict, "T", &parse("Number")));
        assert_eq!(dict, parse("Dict[String, Number]"));
    }
}

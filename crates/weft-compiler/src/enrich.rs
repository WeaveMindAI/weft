//! Post-compilation enrichment. Given a parsed ProjectDefinition and
//! a NodeCatalog, populate each NodeDefinition's inputs/outputs/features
//! from the catalog, materialize config-derived ports, merge
//! weft-declared custom ports for nodes with
//! canAddInputPorts/canAddOutputPorts, and validate that every
//! referenced node type exists.
//!
//! Three node-types are built-in (not catalog): `Passthrough` (group
//! boundary), `LoopIn` / `LoopOut` (loop boundary). Their port shapes
//! are written by the compiler's lowering (`flatten_group` in
//! `weft_compiler.rs`); enrich does not consult the catalog for them
//! (it `continue`s past these node types).

use weft_core::exec::skip::SHOULD_FLOW_PORT;
use weft_core::node::{derive_config_ports, materialize_auto_type_vars, MetadataCatalog, Widget, AUTO_TYPE_VAR};
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
    /// The file the span's coordinates live in, when the offending node
    /// came from a full-mode `@include` (same rule as
    /// `NodeDefinition::source_file`). None = the compiled source.
    pub file: Option<String>,
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

/// The wire-port core the input and output merges share. The two
/// directions genuinely need distinct instance types (an input carries
/// exposure + editor surface, an output is a bare wire port), but both
/// hold one [`PortDefinition`], and the source-vs-catalog merge is one
/// rule over that struct's fields; this hands the merge that struct so
/// a new port field never needs a second accessor list.
trait AsPort: Clone {
    fn port(&self) -> &PortDefinition;
    fn port_mut(&mut self) -> &mut PortDefinition;
}

impl AsPort for InputDefinition {
    fn port(&self) -> &PortDefinition { &self.port }
    fn port_mut(&mut self) -> &mut PortDefinition { &mut self.port }
}

impl AsPort for PortDefinition {
    fn port(&self) -> &PortDefinition { self }
    fn port_mut(&mut self) -> &mut PortDefinition { self }
}

fn merge_ports<T: AsPort>(
    catalog_ports: &[T],
    weft_ports: &[T],
    can_add: bool,
    node_id: &str,
    span: Span,
    file: Option<&str>,
    direction: PortDirection,
    errors: &mut Vec<EnrichError>,
) -> Vec<T> {
    use std::collections::HashMap;

    let catalog_by_name: HashMap<&str, &T> = catalog_ports
        .iter()
        .map(|p| (p.port().name.as_str(), p))
        .collect();

    let mut result: Vec<T> = catalog_ports
        .iter()
        .map(|cp| {
            let Some(wp) = weft_ports.iter().find(|w| w.port().name == cp.port().name) else {
                return cp.clone();
            };
            let (cp_port, wp_port) = (cp.port(), wp.port());
            let mut merged = cp.clone();
            merged.port_mut().required = wp_port.required;
            // The catalog clone has no declared stamp; the header DOES
            // declare this port, so the editor's round-trip needs the
            // authored spelling carried onto the merged result.
            merged.port_mut().declared_type = wp_port.declared_type.clone();
            if !wp_port.port_type.is_must_override() {
                if cp_port.port_type.is_must_override()
                    || WeftType::is_compatible(&wp_port.port_type, &cp_port.port_type)
                {
                    merged.port_mut().port_type = wp_port.port_type.clone();
                } else {
                    errors.push(EnrichError { span, file: file.map(str::to_string), message: format!(
                        "node '{}': {} port '{}' declared type {} incompatible with catalog type {}",
                        node_id,
                        direction.as_str(),
                        cp_port.name,
                        wp_port.port_type,
                        cp_port.port_type,
                    )});
                }
            }
            merged
        })
        .collect();

    for wp in weft_ports {
        if catalog_by_name.contains_key(wp.port().name.as_str()) {
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
            errors.push(EnrichError { span, file: file.map(str::to_string), message: format!(
                "node '{}': declares custom {} port '{}' but node type does not support custom {} ports",
                node_id,
                direction.as_str(),
                wp.port().name,
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
        if wp.port().port_type.is_must_override() {
            errors.push(EnrichError { span, file: file.map(str::to_string), message: format!(
                "node '{}': custom {} port '{}' needs a concrete type",
                node_id,
                direction.as_str(),
                wp.port().name,
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

/// Node-types written by the compiler's lowering passes (Passthrough by
/// group-flatten, LoopIn/LoopOut by loop-lowering). They have no catalog
/// entry by design: enrich fills their ports itself, and diagnostics must
/// not flag them as unknown types.
pub fn is_lowering_builtin(node_type: &str) -> bool {
    matches!(node_type, "Passthrough" | "LoopIn" | "LoopOut")
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
    // Where each wire LANDS, per target node: the port it feeds and the
    // source line it was written on. Read while the node loop holds
    // `project.nodes` mutably, so it is collected up front.
    let incoming_wires = incoming_wires_by_node(project);

    for node in project.nodes.iter_mut() {
        // Built-in boundary node-types. Their ports are written by the
        // compiler's lowering pass (Passthrough by group-flatten, LoopIn
        // / LoopOut by loop-lowering); enrich does not consult the
        // catalog for them. A catalog entry that masquerades as one of
        // these built-in names is rejected loud as a corrupt catalog:
        // letting a catalog impl shadow the runtime built-in would
        // silently break group / loop boundaries.
        if is_lowering_builtin(&node.node_type) {
            if catalog.lookup(&node.node_type).is_some()
                && reported_reserved.insert(node.node_type.clone())
            {
                errors.push(EnrichError { span: node.header_span_or_default(), file: node.source_file.clone(), message: format!(
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
                errors.push(EnrichError { span: node.header_span_or_default(), file: node.source_file.clone(), message: format!(
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
                    file: node.source_file.clone(),
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
                port: PortDefinition {
                    name: spec.name.clone(),
                    port_type: spec.input_type.clone(),
                    required: spec.required,
                    description: spec.description.clone(),
                    synthesized_from_carry: false,
                    declared_type: None,
                },
                exposure: spec.effective_exposure(),
                // The DECLARED widget only; the type-derived default is
                // stamped after TypeVar resolution (see the final pass),
                // so a `T` input resolved to Image gets a file picker.
                widget: spec.widget.clone(),
                default: spec.default.clone(),
                label: spec.label.clone(),
                placeholder: spec.placeholder.clone(),
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
                declared_type: None,
            })
            .collect();

        // The ports of a node that derives them from its own config (a form's
        // `fields`, a switch's `cases`). Fold them into the catalog port set
        // BEFORE merging the source-declared ports, so they count as known
        // ports: a header that re-declares a derived port (the graph editor
        // never writes these, but a hand-authored `.weft` may) merges cleanly
        // IF it matches by name + type, and a header port that does NOT match a
        // derived (or catalog) port is the genuine "custom port on a node that
        // forbids them" error. Declaring them is always OPTIONAL: omitting the
        // header is the normal case.
        let mut catalog_inputs = catalog_inputs;
        if let Some(ports_from_config) = &meta.ports_from_config {
            let (derived_inputs, derived_outputs) =
                derive_config_ports(&node.config, ports_from_config);
            catalog_inputs.extend(derived_inputs);
            catalog_outputs.extend(derived_outputs);
        }

        // `_should_flow` is on EVERY node: the port that says whether it
        // runs at all. Unwired, its `true` default fills it and nothing
        // changes; wired, a `false` or a closure skips the node. It sits
        // with the catalog ports because the node type owns it, not the
        // instance, and the node body never sees it (the input bag drops
        // it: it is the language's decision, not the node's data).
        catalog_inputs.push(InputDefinition {
            port: PortDefinition {
                name: SHOULD_FLOW_PORT.to_string(),
                // Its OWN type variable, never the node's `T`: a join whose
                // branches carry Strings must not also force its permission
                // to be a String.
                port_type: WeftType::type_var("T__should_flow"),
                // Optional: unwired and unset, the node runs. The skip rule
                // reads this port itself, so a closure on it does not need
                // the required-input rule to bite.
                required: false,
                description: Some(
                    "Whether this node runs. A `false` value or a closed input skips it, \
                     and everything downstream closes in turn."
                        .to_string(),
                ),
                synthesized_from_carry: false,
                declared_type: None,
            },
            exposure: Exposure::All,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            from_spec: true,
            requires_scopes: None,
            requires_values: None,
        });

        // An ACCESS NODE (metadata carries the `service` recipe):
        // stamp the service name onto its `access` widget, so the
        // runtime bag and the editor read it off the instance. The
        // permissions are NOT materialized as an input: they are ticked
        // once at connect time and live on the stored connection, never
        // in source. The stamp itself is `AccessSpec::stamp_onto`, the
        // one definition the compact wiring view shares.
        if let Some(service_spec) = &meta.service {
            for input in catalog_inputs.iter_mut() {
                if let Some(widget) = &mut input.widget {
                    service_spec.stamp_onto(widget);
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
                    errors.push(EnrichError { span: node_span, file: node.source_file.clone(), message: format!(
                        "node '{}': input '{}' of node type {} is configuration-only \
                         (exposure `config`); it cannot be declared as a port. Set it in \
                         the config braces instead",
                        node.id, wp.name, node.node_type,
                    )});
                }
            }
        }

        // Ports this node's own config CREATES: a key that names no
        // declared port, on a node type that accepts custom inputs. They
        // join the source-declared ports, since that is exactly what they
        // are: a port the author wrote, in the other form. On a node type
        // that does NOT accept custom inputs nothing is created and
        // validate reports the key (`undeclared-port-no-custom`), which
        // knows how to say it with the key's own span.
        let mut weft_inputs = weft_inputs;
        if meta.features.can_add_input_ports {
            weft_inputs.extend(created_input_ports(
                node,
                &catalog_inputs,
                &weft_inputs,
                &meta.features,
                incoming_wires.get(node.id.as_str()).map(Vec::as_slice).unwrap_or(&[]),
                &mut errors,
            ));
        }
        // A `?` on a key that creates no port would silently mean nothing,
        // so it is refused where it cannot apply.
        for key in &node.optional_ports {
            if !weft_inputs.iter().any(|p| p.name == *key) {
                errors.push(EnrichError { span: node_span, file: node.source_file.clone(), message: format!(
                    "node '{}': '{}?' marks a port optional, but '{}' is not a port this node \
                     creates. Declare optionality on the port itself (`{}?: Type`) instead",
                    node.id, key, key, key,
                )});
            }
        }

        let inputs = merge_ports(
            &catalog_inputs,
            &weft_inputs,
            meta.features.can_add_input_ports,
            &node.id,
            node_span,
            node.source_file.as_deref(),
            PortDirection::Input,
            &mut errors,
        );
        let outputs = merge_ports(
            &catalog_outputs,
            &weft_outputs,
            meta.features.can_add_output_ports,
            &node.id,
            node_span,
            node.source_file.as_deref(),
            PortDirection::Output,
            &mut errors,
        );

        node.inputs = inputs;
        node.outputs = outputs;
        // A declared port that narrows a catalog typevar narrows it
        // EVERYWHERE on this node: `Wait(value: String)` pins the `T`
        // that `value` carries in and out, so the pass-through output
        // is String too. Without this the input alone narrows and the
        // output stays `T`, which the validator then reports as an
        // unresolvable typevar on a program that named the type.
        //
        // Two ports of the same var narrowed to different types is a
        // contradiction the merge cannot see (each declaration is only
        // ever compared against the catalog's `T`, and everything is
        // compatible with an unresolved type), so it is refused here,
        // naming both: silently keeping the first would give the node a
        // contract it never had.
        let mut narrowings: Vec<(String, WeftType)> = Vec::new();
        for cp in &catalog_inputs {
            if let Some(merged) = node.inputs.iter().find(|p| p.name == cp.name) {
                collect_type_var_bindings(&cp.port_type, &merged.port_type, &mut narrowings);
            }
        }
        for cp in &catalog_outputs {
            if let Some(merged) = node.outputs.iter().find(|p| p.name == cp.name) {
                collect_type_var_bindings(&cp.port_type, &merged.port_type, &mut narrowings);
            }
        }
        let mut settled: Vec<(String, WeftType)> = Vec::new();
        for (var, concrete) in narrowings {
            match settled.iter().find(|(v, _)| *v == var) {
                Some((_, first)) if *first != concrete => {
                    errors.push(EnrichError { span: node_span, file: node.source_file.clone(), message: format!(
                        "node '{}': the declared ports pin type '{var}' to two different types \
                         ({first} and {concrete}); every port that carries '{var}' on one node \
                         has to name the same type",
                        node.id,
                    )});
                }
                Some(_) => {}
                None => settled.push((var, concrete)),
            }
        }
        for (var, concrete) in &settled {
            substitute_type_var_on(node, var, concrete);
        }
        // The instance's own `@require_one_of` groups were lowered from
        // source onto `node.features` before enrich; the catalog's
        // features replace everything else but must not wipe them
        // (they once did, and every `@require_one_of` on a catalog-typed
        // node silently vanished). A group the catalog itself declares
        // stays too.
        let own_one_of = std::mem::take(&mut node.features.one_of_required);
        node.features = meta.features.clone();
        // A group the catalog declares and the author repeats at the
        // call site is ONE constraint; appending both would report one
        // unsatisfied node twice at the same span.
        for group in own_one_of {
            if !node.features.one_of_required.contains(&group) {
                node.features.one_of_required.push(group);
            }
        }
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
                    Err(message) => errors.push(EnrichError { span: node_span, file: node.source_file.clone(), message }),
                },
                None => errors.push(EnrichError {
                    span: node_span,
                    file: node.source_file.clone(),
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

    // Type resolution runs whatever the merge found: it is a per-edge
    // fixed-point walk over the ports that DO exist, so a node the
    // catalog could not resolve (empty ports) or a port the merge
    // refused simply contributes no binding. Gating it on a clean
    // merge once meant one unknown node type anywhere left every
    // type variable in the project unresolved, and the validator then
    // buried the one real error under an `unresolved-typevar` for
    // every Switch, Debug and `_should_flow` in the file.
    resolve_type_vars(project);

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

fn resolve_type_vars(project: &mut ProjectDefinition) {
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
    substitute_type_var_on(node, var_name, concrete)
}

/// Replace every occurrence of `var_name` on ONE node's ports, at any
/// depth, and say whether anything moved. The single substitution
/// step: the edge-driven fixed point above and the declared-narrowing
/// pass in `enrich_collecting` both go through it.
fn substitute_type_var_on(
    node: &mut weft_core::project::NodeDefinition,
    var_name: &str,
    concrete: &WeftType,
) -> bool {
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

/// One wire arriving at a node: which of its ports it feeds, and where
/// the line that wrote it is. Enough to create the port the wire lands
/// on when that port does not exist yet.
struct IncomingWire {
    target_port: String,
    span: Span,
}

/// Every wire's landing point, keyed by target node id. Built once
/// before the enrich loop, which holds the node list mutably.
fn incoming_wires_by_node(
    project: &ProjectDefinition,
) -> std::collections::HashMap<String, Vec<IncomingWire>> {
    let mut by_node: std::collections::HashMap<String, Vec<IncomingWire>> =
        std::collections::HashMap::new();
    for edge in &project.edges {
        let Some(target_port) = &edge.target_handle else { continue };
        by_node.entry(edge.target.clone()).or_default().push(IncomingWire {
            target_port: target_port.clone(),
            span: edge.span.unwrap_or_default(),
        });
    }
    by_node
}

/// The input ports a node's own config CREATES: one per config key (or
/// arriving wire) that names no declared port, on a node type that
/// accepts custom inputs.
///
/// Ordered by SOURCE POSITION, because a node whose behaviour depends on
/// the order of its inputs (`FirstInOrder`) reads them in the order they
/// were written. Config lands in a sorted map, so map order is
/// alphabetical and useless; the span of each field (or of the wire that
/// feeds it) is the only record of what the author wrote first.
///
/// A wired port takes the `T_Auto` sentinel by default, which resolves
/// against the edge like any other type variable, so the port ends up
/// with the source port's type. A node type that wants every created
/// input to share ONE type (a join) says so with `customInputType`.
/// A port fed by a LITERAL has no edge to resolve against, so its type
/// is inferred from the literal; a `null` literal infers nothing and is
/// refused rather than guessed.
fn created_input_ports(
    node: &weft_core::project::NodeDefinition,
    catalog_inputs: &[InputDefinition],
    weft_inputs: &[InputDefinition],
    features: &weft_core::NodeFeatures,
    incoming: &[IncomingWire],
    errors: &mut Vec<EnrichError>,
) -> Vec<InputDefinition> {
    let declared = |name: &str| {
        catalog_inputs.iter().any(|p| p.name == name) || weft_inputs.iter().any(|p| p.name == name)
    };

    // (span, name, type) per created port, gathered from the two forms a
    // config key can take, then ordered by where it was written.
    let mut created: Vec<(Span, String, WeftType)> = Vec::new();
    for wire in incoming {
        if declared(&wire.target_port) {
            continue;
        }
        let port_type = features
            .custom_input_type
            .clone()
            .unwrap_or_else(|| WeftType::type_var(AUTO_TYPE_VAR));
        created.push((wire.span, wire.target_port.clone(), port_type));
    }
    if let Some(config) = node.config.as_object() {
        for (key, value) in config {
            if weft_core::project::is_internal_config_key(key) || declared(key) {
                continue;
            }
            // Both anchor halves from the ENTRY: on a node spliced from an
            // included file the key's line lives in that file, and a span
            // paired with the node's own file would jump the wrong buffer.
            let (span, file) = match node.config_spans.get(key) {
                Some(s) => (s.span, s.source_file.clone()),
                None => (Span::default(), node.source_file.clone()),
            };
            if value.is_null() {
                errors.push(EnrichError { span, file, message: format!(
                    "node '{}': config key '{}' creates a port, and `null` says nothing about \
                     its type. Declare it (`{}: Type` in the signature) or give it a value",
                    node.id, key, key,
                )});
                continue;
            }
            created.push((span, key.clone(), WeftType::infer(value)));
        }
    }
    created.sort_by_key(|(span, _, _)| (span.start_line, span.start_column));
    // One port per NAME, first written wins. Two lines can name the same
    // port (a wire and a literal, or two wires): that is a real mistake,
    // and validate names it (`duplicate-input-port` /
    // `port-double-driven`). Creating the port twice on top of it would
    // bury that message under a second one about the port list.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    created.retain(|(_, name, _)| seen.insert(name.clone()));

    created
        .into_iter()
        .map(|(_, name, port_type)| {
            let port_type = materialize_auto_type_vars(&port_type, &name);
            // Required unless the node type says its created inputs are
            // optional by nature, or this key carries the `?` marker.
            let required =
                !features.optional_custom_inputs && !node.optional_ports.contains(&name);
            InputDefinition::from_wire_port(PortDefinition {
                name,
                port_type,
                required,
                description: None,
                synthesized_from_carry: false,
                // Created by a config KEY, not the header: must never
                // round-trip into the signature.
                declared_type: None,
            })
        })
        .collect()
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

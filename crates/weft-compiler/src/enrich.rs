//! Post-compilation enrichment. Given a parsed ProjectDefinition and
//! a `MetadataCatalog`, populate each NodeDefinition's inputs/outputs/features
//! from the catalog, materialize config-derived ports, merge
//! weft-declared custom ports for nodes with
//! canAddInputPorts/canAddOutputPorts, and validate that every
//! referenced node type exists.
//!
//! Two node-types are built-in (not catalog): `LoopIn` / `LoopOut`, a
//! loop's boundaries. Their ports are written by the compiler's lowering
//! (`push_loop_boundaries` in `weft_compiler.rs`), never by enrich: enrich
//! does not consult the catalog for them (it `continue`s past these node
//! types), and only resolves the type variables the two share, per place.
//! A group is no node: what enrich does for its ports (cast the literals
//! written on them) it does on the group itself.

use weft_core::run::skip::{SHOULD_FLOW_PORT, SHOULD_NOT_FLOW_PORT};
use weft_core::node::{derive_config_ports, materialize_auto_type_vars, MetadataCatalog, Widget, AUTO_TYPE_VAR};
use weft_core::project::{InputDefinition, PortDefinition, ProjectDefinition, Span};
use weft_core::node::Accepts;
use weft_core::weft_type::WeftType;

/// One enrich failure carrying the SOURCE SPAN of the offending node, so a
/// diagnostic consumer (the editor's squiggles, the CLI's error lines) points
/// at the exact line instead of a span-less blob. The span is the node's
/// header (`header_span_or_default`); `Span::default()` only for the rare
/// project-level error with no node to blame.
pub struct EnrichError {
    /// The diagnostic code: `enrich` for a node whose ports could not be
    /// worked out, or the specific code of a check enrich owns (a
    /// metadata wire's, `crate::metadata`).
    pub code: &'static str,
    pub span: Span,
    pub message: String,
    /// The file the span's coordinates live in, when the offending node
    /// came from a full-mode `@include` (same rule as
    /// `NodeDefinition::source_file`). None = the compiled source.
    pub file: Option<String>,
    /// The nodes and groups the error is about, by id
    /// (`Diagnostic::subjects`): the node being enriched, or both ends of
    /// a wire; empty only for a fault of the catalog itself, which no cut
    /// can step around.
    pub subjects: Vec<String>,
}

impl EnrichError {
    /// The finding this error is, for a caller that reports diagnostics.
    pub fn diagnostic(&self) -> crate::Diagnostic {
        crate::Diagnostic::at(self.span, crate::Severity::Error, self.code, self.message.clone())
            .in_file(self.file.as_deref())
            .about(self.subjects.iter().cloned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortDirection {
    Input,
    Output,
}

impl PortDirection {
    fn side(self) -> weft_core::weft_type::PortSide {
        match self {
            PortDirection::Input => weft_core::weft_type::PortSide::Input,
            PortDirection::Output => weft_core::weft_type::PortSide::Output,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

/// Move every constant written for an input port out of `config` into
/// `port_literals`, now that the full input list is known (catalog
/// ports, the ports the config list derives, and the ports the source
/// created). After this pass a constant has ONE home whichever
/// spelling wrote it: every config key that names an input port moves to
/// `port_literals` (the engine delivers it as a pulse on the port, the
/// editor edits it there), and `config` keeps only what is not a port
/// (the `_`-reserved keys, a loop's knobs, and on a node that takes no
/// custom inputs the undeclared keys validate reports).
fn normalize_port_literals(node: &mut weft_core::project::NodeDefinition) {
    let Some(cfg) = node.config.as_object_mut() else { return };
    let moved: Vec<String> = node
        .inputs
        .iter()
        .filter(|input| cfg.contains_key(&input.name))
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
/// accepted drivers + editor surface, an output is a bare wire port), but both
/// hold one [`PortDefinition`], and the source-vs-catalog merge is one
/// rule over that struct's fields; this hands the merge that struct so
/// a new port field never needs a second accessor list.
/// The program's `?` rides along: on an input it is `optional`, on an
/// output it means nothing.
trait AsPort: Clone {
    fn port(&self) -> &PortDefinition;
    fn port_mut(&mut self) -> &mut PortDefinition;
    fn optional(&self) -> bool;
    fn set_optional(&mut self, optional: bool);
}

impl AsPort for InputDefinition {
    fn port(&self) -> &PortDefinition { &self.port }
    fn port_mut(&mut self) -> &mut PortDefinition { &mut self.port }
    fn optional(&self) -> bool { self.optional }
    fn set_optional(&mut self, optional: bool) { self.optional = optional; }
}

impl AsPort for PortDefinition {
    fn port(&self) -> &PortDefinition { self }
    fn port_mut(&mut self) -> &mut PortDefinition { self }
    fn optional(&self) -> bool { false }
    fn set_optional(&mut self, _: bool) {}
}

/// Whether a type the source declares on a catalog port may replace the
/// catalog's: `Err` says why not. An input narrows to anything that fits
/// in the catalog's type; an output is the node's contract, so it takes
/// another type only where the node lets the program say what it holds
/// (a narrowable output, within its bound) or a type variable is pinned.
type DeclarationFits<'a> = &'a dyn Fn(&PortDefinition, &WeftType) -> Result<(), String>;

#[allow(clippy::too_many_arguments)]
fn merge_ports<T: AsPort>(
    catalog_ports: &[T],
    weft_ports: &[T],
    can_add: bool,
    fits: DeclarationFits<'_>,
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
            merged.set_optional(wp.optional());
            // The catalog clone has no declared stamp; the header DOES
            // declare this port, so the editor's round-trip needs the
            // authored spelling carried onto the merged result.
            merged.port_mut().declared_type = wp_port.declared_type.clone();
            // A port the header names with no type keeps the catalog's.
            if !wp_port.is_untyped(direction.side()) {
                match fits(cp_port, &wp_port.port_type) {
                    Ok(()) => merged.port_mut().port_type = wp_port.port_type.clone(),
                    Err(why) => errors.push(EnrichError {
                        code: "enrich",
                        span,
                        file: file.map(str::to_string),
                        subjects: vec![node_id.to_string()],
                        message: format!(
                            "node '{}': {} port '{}' is declared {}, {why}",
                            node_id,
                            direction.as_str(),
                            cp_port.name,
                            wp_port.port_type,
                        ),
                    }),
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
            errors.push(EnrichError { code: "enrich", span, file: file.map(str::to_string), subjects: vec![node_id.to_string()], message: format!(
                "node '{}': declares custom {} port '{}' but node type does not support custom {} ports",
                node_id,
                direction.as_str(),
                wp.port().name,
                direction.as_str(),
            )});
            continue;
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

/// The message for a type the catalog does not serve: the catalog's own
/// sentence when it saw the node (not ready yet, or failed to load),
/// otherwise unknown.
pub fn unknown_type_message(catalog: &dyn MetadataCatalog, node_type: &str) -> String {
    catalog
        .unavailable(node_type)
        .unwrap_or_else(|| format!("unknown node type: '{node_type}'"))
}

/// The two gate ports, which sit on EVERY node: the port that says
/// whether it runs at all, and the same gate read the other way round.
/// Unwired, `_should_flow`'s `true` default fills it and nothing
/// changes; wired, a `false` or a closure skips the node.
///
/// They sit with the catalog ports because the node type owns them, not
/// the node in the program, and the node body never sees them (the input bag drops
/// them: the language's decision, not the node's data).
///
/// One port each rather than one port with a flag: a wire names the
/// port it lands on, which is what makes the polarity visible in source
/// and in the graph. A node uses ONE of them; wiring both is refused by
/// validate.
fn gate_inputs() -> [InputDefinition; 2] {
    [
        InputDefinition {
            port: PortDefinition {
                name: SHOULD_FLOW_PORT.to_string(),
                // Its OWN type variable, never the node's `T`: a join whose
                // branches carry Strings must not also force its permission
                // to be a String.
                port_type: WeftType::port_var(weft_core::weft_type::PortSide::Input, SHOULD_FLOW_PORT),
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
            // Both, and never switchable: `_should_flow` is the
            // language's gate, and validate types its constant itself.
            accepts: Accepts::both(),
            // The skip rules read the gate themselves (rule 0); a closure
            // on it is never the required-input rule's to act on.
            optional: true,
            passes_through: false,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            from_spec: true,
            requires_scopes: None,
            requires_values: None,
        },
        InputDefinition {
            port: PortDefinition {
                name: SHOULD_NOT_FLOW_PORT.to_string(),
                port_type: WeftType::port_var(weft_core::weft_type::PortSide::Input, SHOULD_NOT_FLOW_PORT),
                required: false,
                description: Some(
                    "Whether this node runs, read the other way round. A value here skips \
                     it; a CLOSED input (the thing never happened) is what runs it."
                        .to_string(),
                ),
                synthesized_from_carry: false,
                declared_type: None,
            },
            accepts: Accepts::both(),
            optional: true,
            passes_through: false,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            from_spec: true,
            requires_scopes: None,
            requires_values: None,
        },
    ]
}

/// Enrich, returning every failure with the SOURCE SPAN of the offending node
/// (empty vec = clean), so each error squiggles its own line. Every
/// pipeline in `crate` (the editor's parse, the strict compile, the build's
/// `compile_program`) enriches through this.
pub fn enrich_collecting(
    project: &mut ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    policy: EnrichPolicy,
) -> Vec<EnrichError> {
    // Metadata wires first: each writes its value into the input it
    // points at and leaves the program's wires, so everything below sees
    // a written literal there and no wire (`crate::metadata`).
    let mut errors: Vec<EnrichError> = crate::metadata::resolve(project, catalog);
    // One [reserved-node-type] error per offending TYPE, not per node:
    // boundary types appear on two nodes per loop, and a project full
    // of loops would otherwise report the same corrupt catalog entry
    // 2xN times.
    let mut reported_reserved: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    // Where each wire LANDS, per target node: the port it feeds and the
    // source line it was written on. Read while the node loop holds
    // `project.nodes` mutably, so it is collected up front.
    let incoming_wires = incoming_wires_by_node(project);

    for node in project.nodes.iter_mut() {
        // An opaque `@include` node (interface parse only): its file's
        // body is not in the graph, so the parser recorded which node
        // types are in there and this settles what they MEAN against the
        // catalog. Without it the editor reads an include as an empty box
        // and hides the Activate button on a project whose only trigger
        // is inside one. The include node itself gets neither
        // `requiresInfra` nor `is_trigger`: those drive real per-node
        // work (a provisioned infra slot, a trigger's mount URL) and the
        // alias is not the node that does it.
        if let Some(contents) = &mut node.include_contents {
            for node_type in &contents.node_types {
                let Some(meta) = catalog.lookup(node_type) else { continue };
                contents.requires_infra |= meta.requires_infra;
                contents.has_trigger |= meta.features.is_trigger;
            }
            // The gate ports too, or the editor underlines
            // `f._should_flow = x` on an include that the real build
            // accepts: this interface parse is the only enrichment the
            // include node gets, and structural validation runs on it.
            let gates: Vec<InputDefinition> = gate_inputs()
                .into_iter()
                .filter(|gate| !node.inputs.iter().any(|p| p.name == gate.name))
                .collect();
            node.inputs.extend(gates);
            // And a value written for one of its ports (`keep.n = 7`)
            // gets the one home every port constant has, so the editor
            // shows and edits it like any node's. The build, which
            // inlines the file, lands the same line on the call site's
            // port.
            normalize_port_literals(node);
            continue;
        }

        // Built-in boundary node-types. Their ports are written by the
        // compiler's loop lowering (LoopIn / LoopOut); enrich does not
        // consult the catalog for them. A catalog entry that masquerades
        // as one of these built-in names is rejected loud as a corrupt
        // catalog: letting a catalog impl shadow the runtime built-in
        // would silently break loop boundaries.
        if weft_core::project::boundary_types::is_boundary(&node.node_type) {
            if catalog.lookup(&node.node_type).is_some()
                && reported_reserved.insert(node.node_type.clone())
            {
                errors.push(EnrichError { code: "enrich", span: node.header_span_or_default(), file: node.source_file.clone(), subjects: Vec::new(), message: format!(
                    "[reserved-node-type] catalog declares '{}' but that name is a built-in language type; rename the catalog node",
                    node.node_type,
                )});
            }
            continue;
        }

        // A catalog claiming any other type name weft owns is also a
        // corrupt catalog (e.g. an entry literally named 'Group' or
        // 'Loop'). `own_types` is the single source of truth for this
        // check.
        if weft_core::project::own_types::is_own(&node.node_type) {
            if catalog.lookup(&node.node_type).is_some()
                && reported_reserved.insert(node.node_type.clone())
            {
                errors.push(EnrichError { code: "enrich", span: node.header_span_or_default(), file: node.source_file.clone(), subjects: Vec::new(), message: format!(
                    "[reserved-node-type] catalog declares '{}' but that name is a reserved language keyword",
                    node.node_type,
                )});
            }
            continue;
        }

        let Some(meta) = catalog.lookup(&node.node_type) else {
            // A node with an include path that reaches here is the
            // placeholder an included file that does not compile leaves
            // behind (`broken_include`): that file's own errors say what is
            // wrong, and its type was never a catalog name.
            if policy == EnrichPolicy::Strict && node.include_path.is_none() {
                errors.push(EnrichError {
                    code: "enrich",
                    span: node.header_span_or_default(),
                    file: node.source_file.clone(),
                    subjects: vec![node.id.clone()],
                    message: unknown_type_message(catalog, &node.node_type),
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
                accepts: spec.effective_accepts(meta.is_compiler_read(&spec.name)),
                // Optional in the metadata is required once wired, until
                // the program writes otherwise.
                optional: false,
                passes_through: false,
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
                required: true,
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
                derive_config_ports(node.written_value(&ports_from_config.field), ports_from_config);
            catalog_inputs.extend(derived_inputs);
            catalog_outputs.extend(derived_outputs);
        }

        catalog_inputs.extend(gate_inputs());

        // An ACCESS NODE (metadata carries the `service` recipe):
        // stamp the service name onto its `access` widget, so the
        // runtime bag and the editor read it off the node. The
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

        // A header PORT declaration naming a compiler-read input is a
        // collision, not a merge: the compiler reads that value to build
        // the node, so "I declared it as a port" can only mean the user
        // expects to wire it, which it never takes. Every input name is
        // in ONE namespace, so this is the whole collision rule.
        for wp in &weft_inputs {
            if catalog_inputs.iter().any(|cp| cp.name == wp.name) && meta.is_compiler_read(&wp.name) {
                errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!(
                    "node '{}': input '{}' of node type {} is read by the compiler to build \
                     the node; it takes an inline value only and cannot be declared as a \
                     port. Write it in the config braces instead",
                    node.id, wp.name, node.node_type,
                )});
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
                &catalog_outputs,
                &weft_outputs,
                &meta.features,
                incoming_wires.get(node.id.as_str()).map(Vec::as_slice).unwrap_or(&[]),
                &mut errors,
            ));
        }
        // A `?` on a key that creates no port would silently mean nothing,
        // so it is refused where it cannot apply.
        for key in &node.optional_ports {
            if !weft_inputs.iter().any(|p| p.name == *key) {
                errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!(
                    "node '{}': '{}?' marks a port optional, but '{}' is not a port this node \
                     creates. Declare optionality on the port itself (`{}?: Type`) instead",
                    node.id, key, key, key,
                )});
            }
        }

        // A program may make an input its node calls optional required
        // (`x: T`) or optional even when wired (`x?: T`), but never make a
        // required one optional: the node's code is written to need it.
        for wp in &weft_inputs {
            if !wp.required && catalog_inputs.iter().any(|cp| cp.name == wp.name && cp.required) {
                errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!(
                    "node '{}': input '{}' of node type {} is required, and its code needs it: \
                     declare it `{}: {}` without `?`, or drop it from the node's inputs",
                    node.id, wp.name, node.node_type, wp.name, wp.port_type,
                )});
            }
        }

        // SYNC: input_fits <-> packages/weft-graph/src/webview/lib/projection/header-ports.ts declarationRule
        let input_fits = |catalog: &PortDefinition, declared: &WeftType| {
            if WeftType::is_compatible(declared, &catalog.port_type) {
                Ok(())
            } else {
                Err(format!("which does not fit in its type {}", catalog.port_type))
            }
        };
        let inputs = merge_ports(
            &catalog_inputs,
            &weft_inputs,
            meta.features.can_add_input_ports,
            &input_fits,
            &node.id,
            node_span,
            node.source_file.as_deref(),
            PortDirection::Input,
            &mut errors,
        );
        // Every node has an `_error` output the language gave it and
        // the runtime fills; a source port of that name would read as
        // the author's own value on it. The declared port is dropped
        // before the merge so this is the one error it earns, not this
        // plus a type mismatch against the catalog's.
        let mut weft_outputs = weft_outputs;
        let port = weft_core::node::ERROR_PORT;
        if weft_outputs.iter().any(|p| p.name == port) {
            weft_outputs.retain(|p| p.name != port);
            errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!(
                "node '{}': '{port}' is the output every node already has for its failures \
                 (wire it to handle them); remove '{port}' from the node's outputs, or give \
                 your own port another name",
                node.id,
            )});
        }
        // A node that pairs its added ports makes the output of each input
        // the program adds, and an added output the header names with no
        // type takes the node's type for it, as a created input takes
        // `customInputType`. A name its own ports already use is no added
        // input's output; validate refuses it (`unpaired-port`).
        if meta.features.pairs_added_ports {
            for input in &weft_inputs {
                let taken = catalog_inputs.iter().any(|p| p.name == input.name)
                    || catalog_outputs.iter().any(|p| p.name == input.name)
                    || weft_outputs.iter().any(|p| p.name == input.name);
                if !taken {
                    weft_outputs.push(PortDefinition {
                        name: input.name.clone(),
                        port_type: WeftType::port_var(weft_core::weft_type::PortSide::Output, &input.name),
                        required: true,
                        description: None,
                        synthesized_from_carry: false,
                        declared_type: None,
                    });
                }
            }
        }
        if let Some(custom) = &meta.features.custom_output_type {
            for output in weft_outputs.iter_mut() {
                if !catalog_outputs.iter().any(|p| p.name == output.name) && output.is_untyped(weft_core::weft_type::PortSide::Output) {
                    output.port_type = custom.clone();
                }
            }
        }
        let output_fits = |catalog: &PortDefinition, declared: &WeftType| {
            meta.output_declaration_fits(&catalog.name, &catalog.port_type, declared)
        };
        let outputs = merge_ports(
            &catalog_outputs,
            &weft_outputs,
            meta.features.can_add_output_ports,
            &output_fits,
            &node.id,
            node_span,
            node.source_file.as_deref(),
            PortDirection::Output,
            &mut errors,
        );

        node.inputs = inputs;
        node.outputs = outputs;
        // The node's bounds: its type's, then the ones the program wrote
        // after the type name (`KeepFile[T: Image]`), which hold on top.
        // A written one must bound a variable the node's ports use, which
        // only now, with the type's own ports in, can be asked.
        let written_params = std::mem::take(&mut node.type_params);
        let mut used = std::collections::BTreeSet::new();
        for ty in node.inputs.iter().map(|p| &p.port_type).chain(node.outputs.iter().map(|p| &p.port_type)) {
            ty.collect_type_vars(&mut used);
        }
        if let Err(why) = weft_core::weft_type::TypeParam::check_used(&written_params, &used) {
            errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!("node '{}': {why}", node.id) });
        }
        node.type_params = meta.type_param_list();
        node.type_params.extend(written_params);
        // Each list is acyclic on its own; together they may not be
        // (the type bounds `T` by `T2`, the program `T2` by `T`).
        if let Err(why) = weft_core::weft_type::TypeParam::check_cycles(&node.type_params) {
            errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!("node '{}': {why}", node.id) });
        }
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
                    errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message: format!(
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
        // A member of a one-of group is decided by its group: the node
        // skips when every member closed, not when one wired member did.
        // A member the program wrote required stays required.
        for input in node.inputs.iter_mut() {
            if !input.required && node.features.one_of_required.iter().any(|g| g.contains(&input.name)) {
                input.optional = true;
            }
        }
        node.requires_infra = meta.requires_infra;
        node.images = meta.images.clone();
        node.fires_with = meta.fires_with.clone();
        node.baked_outputs = meta.outputs.iter().filter(|o| o.baked).map(|o| o.name.clone()).collect();
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
                    Err(message) => errors.push(EnrichError { code: "enrich", span: node_span, file: node.source_file.clone(), subjects: vec![node.id.clone()], message }),
                },
                None => errors.push(EnrichError {
                    code: "enrich",
                    span: node_span,
                    file: node.source_file.clone(),
                    subjects: vec![node.id.clone()],
                    message: format!(
                        "node '{}' publishes a '{service}' connection, but no node in the \
                         catalog declares that service",
                        node.id
                    ),
                }),
            }
        }
        normalize_port_literals(node);
        // An access node's connection is picked on each install, never
        // written in the source (`weft_core::picks`): a field neither
        // wired nor `@instance_filled` is marked picked on the install, and
        // a connection written there (an instance's fallback included) is
        // the old way, refused naming the fix, since its id means nothing
        // on any other install.
        if let Some(input) = meta.access_input() {
            let wired = incoming_wires
                .get(&node.id)
                .is_some_and(|wires| wires.iter().any(|w| w.target_port == input.name));
            match node.port_literals.get(&input.name) {
                None if !wired => {
                    node.port_literals.insert(input.name.clone(), weft_core::picks::install_picked_literal());
                }
                // An instance's fallback is a connection written in the
                // source too, with the same id that means nothing elsewhere.
                Some(value)
                    if weft_core::instance::as_instance_filled(value).is_some_and(|filled| filled.fallback.is_some()) =>
                {
                    let span = node.written_span(&input.name).map(|s| s.span).unwrap_or(node_span);
                    errors.push(EnrichError {
                        code: "enrich",
                        span,
                        file: node.source_file.clone(),
                        subjects: vec![node.id.clone()],
                        message: format!(
                            "'{id}.{field}' falls back to a connection written in the source. That was the \
                             old way: a connection lives in one install, so its id means nothing on any \
                             other. Write `@instance_filled` alone; an instance that connected none is refused, \
                             naming the field. See {doc}",
                            id = node.id,
                            field = input.name,
                            doc = weft_core::picks::PICKS_DOC,
                        ),
                    });
                }
                Some(value) if !weft_core::picks::fills_later(value) => {
                    let span = node.written_span(&input.name).map(|s| s.span).unwrap_or(node_span);
                    errors.push(EnrichError {
                        code: "enrich",
                        span,
                        file: node.source_file.clone(),
                        subjects: vec![node.id.clone()],
                        message: format!(
                            "'{id}.{field}' holds a connection written in the source. That was the old way \
                             of connecting a node: a connection lives in one install, so its id means \
                             nothing on any other. Erase this line and connect again with `weft connect \
                             --node {id}` (or the node's Connect button); the pick is kept by the install. \
                             See {doc}",
                            id = node.id,
                            field = input.name,
                            doc = weft_core::picks::PICKS_DOC,
                        ),
                    });
                }
                _ => {}
            }
        }
        // A node with an `@instance_filled` field carries what checking an
        // instance's value needs (its rules) and, when the field is its
        // connection, what the instance's connect page needs (the
        // service's recipe): the instance door has only the program to
        // read them from.
        let filled: Vec<String> = weft_core::instance::instance_filled_fields(node).map(|(field, _)| field.to_string()).collect();
        node.instance_rules = (!filled.is_empty()).then(|| weft_core::project::InstanceRules {
            rules: crate::validate::node_rules(meta, node),
            custom_outputs: crate::validate::node_custom_outputs(node, meta),
        });
        node.instance_service = match (meta.access_input(), &meta.service) {
            (Some(input), Some(spec)) if filled.contains(&input.name) => Some(spec.clone()),
            _ => None,
        };
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
    // A node whose places resolve apart (`place_types`) is stamped from
    // its own types: the widget is what the editor draws, and the editor
    // never sees an included file's places; a widget derived from a type
    // adds no rule the run checks (only a declared one does).
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
    // A literal on a group's own port is cast the same way, against the
    // type the group declares there: the group sends it to every member
    // that reads the port (`weft_core::run::scope`). A loop's sit on its
    // `LoopIn`, cast above.
    for group in project.groups.iter_mut().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        for input in &group.in_ports {
            if input.port_type.is_unresolved() { continue; }
            let Some(value) = group.port_literals.get(&input.name).cloned() else { continue };
            if WeftType::is_compatible(&crate::file_ref::literal_type(&value), &input.port_type) { continue; }
            if let Ok(cast) = input.port_type.cast_value(&value) {
                group.port_literals.insert(input.name.clone(), cast);
            }
        }
    }
    // A group's input gets what a node input of its type gets, worked out
    // from its resolved type: the drivers it takes and the widget the
    // editor draws a value written there with. A loop's are its
    // `LoopIn`'s, stamped above, whose types are the ones its iterations
    // resolve.
    let loop_inputs: std::collections::HashMap<String, Vec<InputDefinition>> = project.nodes.iter()
        .filter_map(|n| n.group_boundary.as_ref()
            .filter(|b| b.role == weft_core::project::GroupBoundaryRole::In)
            .map(|b| (b.group_id.clone(), n.inputs.clone())))
        .collect();
    for group in project.groups.iter_mut() {
        let boundary = loop_inputs.get(&group.id);
        for input in group.in_ports.iter_mut() {
            match boundary.and_then(|inputs| inputs.iter().find(|i| i.name == input.name)) {
                Some(stamped) => {
                    input.accepts = stamped.accepts;
                    input.widget = stamped.widget.clone();
                }
                None => {
                    input.accepts = Accepts::for_type(&input.port_type);
                    input.widget = Some(Widget::default_for_type(&input.port_type));
                }
            }
        }
    }
    mark_per_instance_slice(project);
    errors
}

/// Everything that reads a per-instance value becomes per-instance too.
///
/// The author writes only the starting points (`@per_instance` on an infra
/// node, `@instance_filled` on a field); whatever sits downstream of one along the
/// wires reads that instance's value, so it exists per instance as well: a
/// trigger reading an instance's bridge address is armed once per instance,
/// and a node reading an instance's connection runs in that instance's runs.
/// A wire runs straight from a node to the node that reads it, through any
/// group's port, so the walk follows wires and nothing else, with one more
/// step: a wire landing on a group's gate decides whether the group runs,
/// so everything inside a group whose gate reads a per-instance value runs
/// per instance too. A loop's boundaries are ordinary nodes the wires
/// cross, and its `LoopIn` (where its gate lands) launches every
/// iteration, so reaching it reaches the loop's inside. A node an include's body
/// shares between call sites is one node, so one per-instance site makes
/// it per-instance for every site: the conservative answer, and the one
/// that never lets an instance's value reach a shared run unnoticed.
///
/// Derived and filled marks are recomputed from scratch each time, so a
/// stale one can never outlive what it came from.
pub fn mark_per_instance_slice(project: &mut ProjectDefinition) {
    use weft_core::instance::PerInstance;
    for node in project.nodes.iter_mut() {
        if matches!(node.per_instance, Some(PerInstance::Derived | PerInstance::Filled)) {
            node.per_instance = None;
        }
        // The second starting point: a node with a field each instance fills.
        if node.per_instance.is_none() && weft_core::instance::instance_filled_fields(node).next().is_some() {
            node.per_instance = Some(PerInstance::Filled);
        }
    }
    let mut downstream: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
    for edge in project.edges.iter().filter(|edge| !edge.to_group_port()) {
        downstream.entry(edge.source.as_str()).or_default().push(edge.target.as_str());
    }
    // A group reached through its gate reaches everything inside it, and
    // a call site the body it calls.
    let mut inside: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
    for node in &project.nodes {
        for group in &node.scope {
            inside.entry(group.as_str()).or_default().push(node.id.as_str());
        }
    }
    for group in &project.groups {
        if let weft_core::project::GroupKind::Call { body, .. } = &group.kind {
            inside.entry(group.id.as_str()).or_default().push(body.as_str());
        }
    }
    let mut reached: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut frontier: Vec<&str> = project
        .nodes
        .iter()
        .filter(|n| matches!(n.per_instance, Some(PerInstance::Marked | PerInstance::Filled)))
        .map(|n| n.id.as_str())
        .collect();
    let groups: std::collections::HashSet<&str> = project.groups.iter().map(|g| g.id.as_str()).collect();
    // A loop's gate lands on its `LoopIn`, which launches every iteration:
    // reaching it reaches the loop's inside, the members no wire feeds
    // among them.
    let loop_of: std::collections::HashMap<&str, &str> = project.nodes.iter()
        .filter_map(|n| n.group_boundary.as_ref()
            .filter(|b| b.role == weft_core::project::GroupBoundaryRole::In)
            .map(|b| (n.id.as_str(), b.group_id.as_str())))
        .collect();
    while let Some(id) = frontier.pop() {
        let opens = if groups.contains(id) { Some(id) } else { loop_of.get(id).copied() };
        let next = downstream.get(id).map(Vec::as_slice).unwrap_or_default().iter()
            // A gate reached: the group's inside. A group's own port (a
            // wire from it) is reached only through its gate, never on
            // its own: a literal is no instance's value.
            .chain(opens.and_then(|group| inside.get(group)).into_iter().flatten());
        for &next in next {
            if reached.insert(next.to_string()) {
                frontier.push(next);
            }
        }
    }
    let reached: std::collections::BTreeSet<String> = reached;
    for node in project.nodes.iter_mut() {
        if node.per_instance.is_none() && reached.contains(&node.id) {
            node.per_instance = Some(PerInstance::Derived);
        }
    }
}

/// Literal lenience: a written literal whose JSON shape doesn't match
/// its input's declared type but UNAMBIGUOUSLY converts (a `"18"` on a
/// Number input, a JSON-in-a-string on a structural input) is cast in
/// place at compile time, in the compiled definition only (source keeps
/// what the author wrote). One cast site: validate, the runtime, and
/// the editor all read the cast value. A value that cannot cast is left
/// untouched, so validate reports the genuine mismatch
/// (`config-type-mismatch`). Every constant is a candidate, whatever
/// spelling wrote it; a marker's type is the type it declares (see
/// `file_ref::literal_type`), so a file marker never looks like a
/// String to cast. A port whose type is still open (an unbound
/// variable) has nothing to cast toward yet.
fn cast_literals(node: &mut weft_core::project::NodeDefinition) {
    for input in &node.inputs {
        if input.port_type.is_unresolved() {
            continue;
        }
        let Some(value) = node.port_literals.get(&input.name).cloned() else { continue };
        // A `@instance_filled` field casts its fallback, inside the marker;
        // the marker itself is never a value to cast.
        if let Some(filled) = weft_core::instance::as_instance_filled(&value) {
            let Some(fallback) = filled.fallback.cloned() else { continue };
            if WeftType::is_compatible(&crate::file_ref::literal_type(&fallback), &input.port_type) {
                continue;
            }
            if let Ok(cast) = input.port_type.cast_value(&fallback) {
                node.port_literals.insert(input.name.clone(), weft_core::instance::instance_filled_literal(Some(cast)));
            }
            continue;
        }
        if WeftType::is_compatible(&crate::file_ref::literal_type(&value), &input.port_type) {
            continue;
        }
        if let Ok(cast) = input.port_type.cast_value(&value) {
            node.port_literals.insert(input.name.clone(), cast);
        }
    }
}

/// Whether `Empty` (the item type of an empty list or object) sits
/// anywhere in `ty`: a type read off such a constant says nothing about
/// what that place holds.
fn holds_empty(ty: &WeftType) -> bool {
    match ty {
        WeftType::Primitive(weft_core::weft_type::WeftPrimitive::Empty) => true,
        WeftType::List(inner) | WeftType::Dict(inner) | WeftType::Generator(inner) => holds_empty(inner),
        WeftType::Union(members) => members.iter().any(holds_empty),
        WeftType::Record { fields, .. } => fields.iter().any(|f| holds_empty(&f.ty)),
        WeftType::Named { body, .. } => holds_empty(body),
        _ => false,
    }
}

/// Work out every type variable from what is connected, at every place
/// a node or a group runs. A node inside an included file runs once per
/// site that includes it (however deep), and each place resolves its
/// variables on its own: `blur` included once for an Image and once for
/// a Video holds `T = Image` at one place and `T = Video` at the other.
/// The node stays one node; where its places disagree, it carries each
/// place's types ([`weft_core::project::NodeDefinition::place_types`]).
/// A group's ports are typed the same way, place by place (a group inside
/// an included file, the file's own body among them, is one group run at
/// every place a site reaches it): a wire through group ports is a chain
/// of stretches, from its source through each group port it passes to its
/// target, and each stretch binds its two ends against each other, so a
/// port that declares a concrete type is what both sides of it agree to
/// and a port typed by a variable takes what reaches it and hands it on.
fn resolve_type_vars(project: &mut ProjectDefinition) {
    use weft_core::frames::Located;
    use weft_core::project::graph::GraphView;
    let graph = weft_core::project::graph::ProjectGraph::new(project);
    let position: std::collections::HashMap<String, usize> =
        project.nodes.iter().enumerate().map(|(at, n)| (n.id.clone(), at)).collect();
    // Each place's own copy of what resolution changes: the port types and
    // the type parameters, nothing else of the node or the group. A loop's
    // ports are its two boundaries', which are nodes.
    let mut typed: std::collections::BTreeMap<Located, PlacePorts> = weft_core::project::places::every_place(project)
        .into_iter()
        .filter_map(|at| {
            let node = &project.nodes[*position.get(&at.id)?];
            Some((at, PlacePorts::of(node)))
        })
        .collect();
    for at in weft_core::project::places::every_group_place(project) {
        let Some(group) = graph.group(&at.id) else { continue };
        if matches!(group.kind, weft_core::project::GroupKind::Loop { .. }) { continue; }
        typed.insert(at, PlacePorts::of_group(group));
    }
    // Each wire at every place it runs, as its stretches: the port each
    // starts at, the keys it reads on the way, the port it lands at.
    let mut stretches: Vec<(Slot, Vec<String>, Slot)> = Vec::new();
    // A wire that stops at a group port nobody reads still types the
    // ports it passes.
    for (edge, source, target) in weft_core::project::places::wires_at_places(project) {
        let places = weft_core::project::places::crossing_places(&graph, edge, &source);
        let mut from: Option<Slot> = (!edge.from_group_port())
            .then(|| (source.clone(), weft_core::project::CrossingSide::Out, edge.source_port().to_string()));
        let mut keys: Vec<String> = Vec::new();
        for (crossing, at) in edge.via.iter().zip(places) {
            keys.extend(crossing.path.iter().cloned());
            if !typed.contains_key(&at) { continue; }
            let here: Slot = (at, crossing.side, crossing.port.clone());
            if let Some(start) = from.take() {
                stretches.push((start, std::mem::take(&mut keys), here.clone()));
            }
            keys.clear();
            from = Some(here);
        }
        let (Some(start), Some(target)) = (from, target) else { continue };
        let keys = match edge.via.len() {
            0 => edge.path.clone(),
            n => { let mut rest = keys; rest.extend(edge.path_after(n - 1)); rest }
        };
        stretches.push((start, keys, (target, weft_core::project::CrossingSide::In, edge.target_port().to_string())));
    }
    // The other half of a loop's boundary, at the same place: the two
    // share the loop's variables.
    let sibling = |at: &Located| -> Option<Located> {
        let boundary = project.nodes[*position.get(&at.id)?].group_boundary.as_ref()?;
        let id = match boundary.role {
            weft_core::project::GroupBoundaryRole::In => weft_core::project::loop_out_id(&boundary.group_id),
            weft_core::project::GroupBoundaryRole::Out => weft_core::project::loop_in_id(&boundary.group_id),
        };
        Some(Located::new(id, at.path.clone()))
    };
    let substitute = |typed: &mut std::collections::BTreeMap<Located, PlacePorts>, at: &Located, var: &str, concrete: &WeftType| {
        let mut changed = typed.get_mut(at).is_some_and(|ports| ports.substitute(var, concrete));
        if let Some(ports) = sibling(at).and_then(|other| typed.get_mut(&other)) {
            changed |= ports.substitute(var, concrete);
        }
        changed
    };

    // A constant written on a port pins the variables of its type the
    // way a wire would (`KeepFile { file: @asset("cat.png", Image) }`
    // makes `T` an Image, so the `T` it hands out is one too): the
    // constant IS what arrives. A value each instance or install fills
    // later says nothing yet, and neither does an empty list or object
    // about its items (`[]` pins no `T` in `List[T]`; a wire may still).
    // A constant a metadata wire wrote is of its metadata port's type,
    // which it states (`port_literal_types`), never the record its JSON
    // reads as.
    let mut literal_bindings: Vec<(Located, String, WeftType)> = Vec::new();
    for (at, ports) in &typed {
        let (literals, stated) = match position.get(&at.id) {
            Some(&node) => (&project.nodes[node].port_literals, &project.nodes[node].port_literal_types),
            None => match graph.group(&at.id) {
                Some(group) => (&group.port_literals, &group.port_literal_types),
                None => continue,
            },
        };
        for (name, port_type) in &ports.inputs {
            if !port_type.contains_unresolved_leaf() {
                continue;
            }
            let Some(value) = literals.get(name) else { continue };
            if weft_core::picks::fills_later(value) {
                continue;
            }
            let literal = stated.get(name).cloned().unwrap_or_else(|| crate::file_ref::literal_type(value));
            if !literal.contains_unresolved_leaf() {
                let mut bindings = Vec::new();
                collect_type_var_bindings(port_type, &literal, &mut bindings);
                literal_bindings.extend(
                    bindings
                        .into_iter()
                        .filter(|(_, ty)| !holds_empty(ty))
                        .map(|(name, ty)| (at.clone(), name, ty)),
                );
            }
        }
    }
    for (at, name, concrete) in &literal_bindings {
        substitute(&mut typed, at, name, concrete);
    }
    loop {
        let mut changed = false;
        for ((from, from_side, from_port), keys, (to, to_side, to_port)) in &stretches {
            let Some(from_type) = typed.get(from).and_then(|ports| ports.on(*from_side, from_port)).cloned() else { continue };
            let Some(to_type) = typed.get(to).and_then(|ports| ports.on(*to_side, to_port)).cloned() else { continue };
            // What the stretch DELIVERS: its start's type with the keys
            // read on the way. A path that does not fit binds nothing;
            // validate names the problem.
            let Ok(delivered) = weft_core::deref::walk_path(&from_type, keys) else { continue };
            // Bind typevars at ANY depth (a `List[T]` port wired to a
            // `List[Number]` binds `T = Number`), matching what the
            // validator counts as unresolved; a bare-`T`-only binder
            // would leave nested vars unresolved forever.
            let mut bindings = Vec::new();
            collect_type_var_bindings(&to_type, &delivered, &mut bindings);
            for (name, concrete) in &bindings {
                changed |= substitute(&mut typed, to, name, concrete);
            }
            let mut bindings = Vec::new();
            collect_type_var_bindings(&delivered, &to_type, &mut bindings);
            for (name, concrete) in &bindings {
                changed |= substitute(&mut typed, from, name, concrete);
            }
        }
        if !changed {
            break;
        }
    }

    // Back onto the nodes and the groups: one set of types where every
    // place agrees, and each place's where they do not.
    let mut by_owner: std::collections::HashMap<String, Vec<(Located, PlacePorts)>> = std::collections::HashMap::new();
    for (at, ports) in typed {
        by_owner.entry(at.id.clone()).or_default().push((at, ports));
    }
    let per_place = |places: Vec<(Located, PlacePorts)>| -> Vec<weft_core::project::PlaceTypes> {
        let first = &places[0].1;
        if places.iter().all(|(_, ports)| ports == first) {
            return Vec::new();
        }
        places
            .into_iter()
            .map(|(at, ports)| weft_core::project::PlaceTypes {
                place: at.path,
                inputs: ports.inputs.into_iter().collect(),
                outputs: ports.outputs.into_iter().collect(),
                type_params: ports.type_params,
            })
            .collect()
    };
    for node in project.nodes.iter_mut() {
        let Some(places) = by_owner.remove(&node.id) else { continue };
        let first = places[0].1.clone();
        for (input, (_, resolved)) in node.inputs.iter_mut().zip(&first.inputs) {
            input.port_type = resolved.clone();
        }
        for (output, (_, resolved)) in node.outputs.iter_mut().zip(&first.outputs) {
            output.port_type = resolved.clone();
        }
        node.type_params = first.type_params;
        node.place_types = per_place(places);
    }
    for group in project.groups.iter_mut() {
        let Some(places) = by_owner.remove(&group.id) else { continue };
        let first = places[0].1.clone();
        for (port, (_, resolved)) in group.in_ports.iter_mut().zip(&first.inputs) {
            port.port_type = resolved.clone();
        }
        for (port, (_, resolved)) in group.out_ports.iter_mut().zip(&first.outputs) {
            port.port_type = resolved.clone();
        }
        group.type_params = first.type_params;
        group.place_types = per_place(places);
    }
}

/// One end of a stretch of wire in type resolution: the place (a node's
/// or a group's), which side of it, and the port.
type Slot = (weft_core::frames::Located, weft_core::project::CrossingSide, String);

/// One place's copy of what type resolution works out: a node's port
/// types, in the node's port order, and its type parameters.
#[derive(Clone, PartialEq)]
struct PlacePorts {
    inputs: Vec<(String, WeftType)>,
    outputs: Vec<(String, WeftType)>,
    type_params: Vec<weft_core::weft_type::TypeParam>,
}

impl PlacePorts {
    fn of(node: &weft_core::project::NodeDefinition) -> Self {
        Self {
            inputs: node.inputs.iter().map(|p| (p.name.clone(), p.port_type.clone())).collect(),
            outputs: node.outputs.iter().map(|p| (p.name.clone(), p.port_type.clone())).collect(),
            type_params: node.type_params.clone(),
        }
    }

    fn of_group(group: &weft_core::project::GroupDefinition) -> Self {
        Self {
            inputs: group.in_ports.iter().map(|p| (p.name.clone(), p.port_type.clone())).collect(),
            outputs: group.out_ports.iter().map(|p| (p.name.clone(), p.port_type.clone())).collect(),
            type_params: group.type_params.clone(),
        }
    }

    /// The type of `port` on one side: a node's input or a group's in
    /// port (`In`), a node's output or a group's out port (`Out`).
    fn on(&self, side: weft_core::project::CrossingSide, port: &str) -> Option<&WeftType> {
        let ports = match side {
            weft_core::project::CrossingSide::In => &self.inputs,
            weft_core::project::CrossingSide::Out => &self.outputs,
        };
        ports.iter().find(|(name, _)| name == port).map(|(_, ty)| ty)
    }

    fn substitute(&mut self, var_name: &str, concrete: &WeftType) -> bool {
        let types = self.inputs.iter_mut().chain(self.outputs.iter_mut()).map(|(_, ty)| ty);
        substitute_in(types, &mut self.type_params, var_name, concrete)
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
// SYNC: collect_type_var_bindings <-> packages/weft-graph/src/webview/lib/types/index.ts collectTypeVarBindings,
// packages/weft-graph/src/webview/lib/projection/header-ports.ts pinnedBoundProblem
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
        (WeftType::Dict(a), WeftType::Dict(b)) => collect_type_var_bindings(a, b, out),
        (WeftType::Record { fields: af, .. }, WeftType::Record { fields: bf, .. }) => {
            for fa in af {
                if let Some(fb) = bf.iter().find(|f| f.name == fa.name) {
                    collect_type_var_bindings(&fa.ty, &fb.ty, out);
                }
            }
        }
        _ => {}
    }
}

/// Replace every occurrence of `var_name` on ONE node's ports, at any
/// depth, and say whether anything moved. The single substitution
/// step: the edge-driven fixed point above and the declared-narrowing
/// pass in `enrich_collecting` both go through it. A bound on the
/// variable records what it resolved to, and every bound naming it
/// gets it substituted, so validate holds each bound to its variable
/// with the others already in.
fn substitute_type_var_on(
    node: &mut weft_core::project::NodeDefinition,
    var_name: &str,
    concrete: &WeftType,
) -> bool {
    let types = node.inputs.iter_mut().map(|p| &mut p.port.port_type).chain(node.outputs.iter_mut().map(|p| &mut p.port_type));
    substitute_in(types, &mut node.type_params, var_name, concrete)
}

/// The substitution step over a set of port types and the type
/// parameters beside them.
fn substitute_in<'a>(
    types: impl Iterator<Item = &'a mut WeftType>,
    type_params: &mut [weft_core::weft_type::TypeParam],
    var_name: &str,
    concrete: &WeftType,
) -> bool {
    let mut changed = false;
    for ty in types {
        changed |= ty.substitute_var(var_name, concrete);
    }
    for param in type_params.iter_mut() {
        if param.name == var_name && param.resolved.is_none() {
            param.resolved = Some(concrete.clone());
        }
        param.bound.substitute_var(var_name, concrete);
    }
    changed
}

/// One wire arriving at a node: which of its ports it feeds, and where
/// the line that wrote it is. Enough to create the port the wire lands
/// on when that port does not exist yet.
struct IncomingWire {
    /// The part the wire comes from (`validate::part_by_id`: a loop's
    /// boundary reads as its loop), so an error about the wire names both
    /// of its ends.
    source: String,
    target_port: String,
    span: Span,
    /// The file the span is a position IN. The wire's own, not the
    /// node's: `site.port = x` is written where the site is, and the
    /// node it lands on may have been spliced in from an included file.
    /// Pairing this span with that node's file points a squiggle at a
    /// line number in the wrong buffer.
    source_file: Option<String>,
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
            source: crate::validate::part_by_id(project, &edge.source).to_string(),
            target_port: target_port.clone(),
            span: edge.span.unwrap_or_default(),
            source_file: edge.source_file.clone(),
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
///
/// A name that is one of the node's OUTPUTS (catalog or signature) and
/// no input creates nothing: an output takes no value, a firing emits
/// on it, so a key or a wire aimed at it is a mistake named as such
/// rather than a port quietly added beside the output.
fn created_input_ports(
    node: &weft_core::project::NodeDefinition,
    catalog_inputs: &[InputDefinition],
    weft_inputs: &[InputDefinition],
    catalog_outputs: &[PortDefinition],
    weft_outputs: &[PortDefinition],
    features: &weft_core::NodeFeatures,
    incoming: &[IncomingWire],
    errors: &mut Vec<EnrichError>,
) -> Vec<InputDefinition> {
    let declared = |name: &str| {
        catalog_inputs.iter().any(|p| p.name == name) || weft_inputs.iter().any(|p| p.name == name)
    };
    let is_output = |name: &str| {
        catalog_outputs.iter().any(|p| p.name == name) || weft_outputs.iter().any(|p| p.name == name)
    };
    let output_takes_no_value = |name: &str| {
        format!(
            "node '{}': '{}' is an output port, and an output takes no value: a firing emits \
             on it. Read it with `{}.{}`; to hand the node a value, declare an input",
            node.id, name, node.id, name,
        )
    };

    // (span, name, type) per created port, gathered from the two forms a
    // config key can take, then ordered by where it was written.
    let mut created: Vec<(Span, String, WeftType)> = Vec::new();
    for wire in incoming {
        if declared(&wire.target_port) {
            continue;
        }
        if is_output(&wire.target_port) {
            // Both anchor halves from the WIRE, for the same reason the
            // config branch below takes both from the entry: a span and
            // a file that came from different places name a line in a
            // buffer the line is not in.
            errors.push(EnrichError {
                code: "enrich",
                span: wire.span,
                file: wire.source_file.clone(),
                subjects: vec![wire.source.clone(), node.id.clone()],
                message: output_takes_no_value(&wire.target_port),
            });
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
            if is_output(key) {
                errors.push(EnrichError { code: "enrich", span, file, subjects: vec![node.id.clone()], message: output_takes_no_value(key) });
                continue;
            }
            if value.is_null() {
                errors.push(EnrichError { code: "enrich", span, file, subjects: vec![node.id.clone()], message: format!(
                    "node '{}': config key '{}' creates a port, and `null` says nothing about \
                     its type. Declare it (`{}: Type` in the signature) or give it a value",
                    node.id, key, key,
                )});
                continue;
            }
            // The node's type for what it creates, as for a wired key; else
            // the type the value states (a metadata wire's), else the
            // type its JSON reads as.
            let port_type = features.custom_input_type.clone()
                .or_else(|| node.port_literal_types.get(key).cloned())
                .unwrap_or_else(|| WeftType::infer(value));
            created.push((span, key.clone(), port_type));
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
                !features.join && !node.optional_ports.contains(&name);
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
            // A key's `?` (or a join's branch) runs without it even when
            // its wire closed.
            .with_optional(!required)
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

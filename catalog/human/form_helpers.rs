//! Generic form-field helpers for the nodes that put a form in front
//! of a person.
//!
//! The pipeline is data-driven by the `portsFromConfig` metadata key,
//! declared once in the package root's `metadata.json` and inherited by
//! every member (HumanQuery, HumanTrigger). The catalog loads it, the
//! compiler's enrich pass materializes ports from it, and these helpers
//! shape the runtime form schema (sent to the human) and map the user's
//! response back to output ports. The node reads its specs from its own
//! `manifest().ports_from_config`, the same document the catalog sees.
//!
//! Adding a new field type means: add a spec to that metadata. Nothing
//! in the engine, dispatcher, or UI needs to change as long as the
//! spec's `render.component` is something the consumer already knows how
//! to draw.

use std::collections::HashMap;

use serde_json::{Map, Value};
use weft::node::PortSpec;
use weft::node::PortTemplate;
use weft::node::NodeOutput;
use weft::node::{FormFieldRender, FormFieldSource};
use weft::signal::{Form, FormField, FormSchema};
use weft::{ValueBag, WeftResult};

/// The port specs a form node runs on. A form node whose metadata does
/// not declare `portsFromConfig` cannot build a form at all, so this
/// fails loud rather than rendering an empty one.
pub fn form_specs(manifest: &'static weft::node::NodeMetadata) -> WeftResult<&'static [PortSpec]> {
    match &manifest.ports_from_config {
        Some(ports) => Ok(&ports.specs),
        None => weft::node_bail!(
            "node type '{}' builds a form but its metadata declares no `portsFromConfig`",
            manifest.node_type
        ),
    }
}

/// Assemble the whole [`Form`] signal from a form node's inputs: the
/// fields array, `title`/`description`, and `prefill` (the values
/// display / prefilled / source=input fields lift out by key: the
/// query node projects its wired inputs, the trigger has no upstream
/// and passes an empty object). ONE builder for both nodes, so "what
/// a human form signal looks like" is defined once; only the
/// `form_type` and the prefill source genuinely differ.
pub fn build_form(
    inputs: &ValueBag,
    specs: &[PortSpec],
    form_type: &str,
    prefill: &Value,
) -> WeftResult<Form> {
    let raw_fields = parse_form_fields(inputs.object()?)?;
    let title: String = inputs.get_or("title", String::new())?;
    let description: Option<String> = inputs.opt("description")?;
    let schema = FormSchema {
        fields: build_form_fields(&raw_fields, specs, prefill)?,
    };
    Ok(Form {
        form_type: form_type.to_string(),
        schema,
        title: if title.is_empty() { None } else { Some(title) },
        description,
        // Browser extension / human-in-the-loop processors enumerate
        // this consumer kind.
        consumer_kind: Some("human_in_the_loop".into()),
    })
}

/// Pull the `fields` array off a node's config. The canonical shape is a
/// JSON array (what the compiler produces). An absent `fields` is a form
/// with no fields; any other shape is a broken config and fails loudly
/// instead of silently becoming an empty form.
pub fn parse_form_fields(config: &serde_json::Map<String, Value>) -> WeftResult<Vec<Value>> {
    match config.get("fields") {
        Some(Value::Array(arr)) => Ok(arr.clone()),
        None => Ok(Vec::new()),
        Some(other) => weft::node_bail!(
            "the form's `fields` must be a list of field entries, and it is {other}"
        ),
    }
}

/// Read the `kind` off one entry of `config.fields`, the key every
/// config-derived port list uses to name its spec.
pub fn kind_of(field: &Value) -> Option<&str> {
    field.get("kind").and_then(|v| v.as_str())
}

/// Resolve one raw entry against the node's specs: its key, kind and
/// spec. The ONE place the three malformed-entry shapes fail (a missing
/// `key`, a missing `kind`, an unknown kind), shared by the build path
/// and the response mapping so the two firings of a trigger node can
/// never disagree on what a well-formed entry is.
fn resolve_field<'a>(
    raw: &'a Value,
    spec_map: &HashMap<&str, &'a PortSpec>,
) -> WeftResult<(&'a str, &'a str, &'a PortSpec)> {
    let Some(key) = raw.get("key").and_then(|v| v.as_str()) else {
        weft::node_bail!("a form field has no `key`; every field names its port");
    };
    let Some(kind) = kind_of(raw) else {
        weft::node_bail!("form field '{key}' has no `kind`");
    };
    let Some(spec) = spec_map.get(kind) else {
        weft::node_bail!("form field '{key}' has kind '{kind}', which this node does not offer");
    };
    Ok((key, kind, spec))
}

/// Build the runtime form schema sent to the human.
///
/// `raw_fields` is the node's `config.fields` array. `specs` is the
/// node's port specs. `input` is the node's input port values: fields
/// that need a pre-fill (display, image, prefilled, source=input) read
/// from here using their `key`.
///
/// One `FormField` per entry, and an entry the specs cannot serve is an
/// ERROR rather than a dropped field: the compiler refuses an unknown
/// `kind` and a spec with no render, so reaching either here means the
/// person would have silently been shown an incomplete form.
pub fn build_form_fields(
    raw_fields: &[Value],
    specs: &[PortSpec],
    input: &Value,
) -> WeftResult<Vec<FormField>> {
    let spec_map: HashMap<&str, &PortSpec> =
        specs.iter().map(|s| (s.kind.as_str(), s)).collect();

    let mut fields = Vec::with_capacity(raw_fields.len());
    for raw in raw_fields {
        let (key, kind, spec) = resolve_field(raw, &spec_map)?;
        let (key, kind) = (key.to_string(), kind.to_string());

        let label = raw
            .get("label")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| key.clone());
        // What this kind asked the author to fill in, gathered off the
        // entry by the keys the spec asks for. `label` is a column of
        // its own on the wire, so it is not repeated in here. A present
        // null is absent (the editor clears a box by writing null), so
        // it never reaches the wire.
        // SYNC: null-is-absent <->
        //       crates/weft-compiler/src/validate.rs (the spec-field loop),
        //       packages/weft-graph/src/webview/lib/utils/port-specs.ts (hasValue)
        let mut config = Map::new();
        for field in &spec.fields {
            if field.key == "label" {
                continue;
            }
            if let Some(value) = raw.get(field.key.as_str()).filter(|v| !v.is_null()) {
                config.insert(field.key.clone(), value.clone());
            }
        }

        // The render comes from the SPEC (the typed FormFieldRender
        // travels the wire as-is); an entry cannot carry its own,
        // because the compiler admits only `kind`, the key field and
        // the spec's declared fields as entry keys.
        let Some(render) = spec.render.clone() else {
            weft::node_bail!(
                "form field '{key}' has kind '{kind}', which declares no render, \
                 so nothing knows how to draw it"
            );
        };

        // Pre-fill `value` for fields whose render needs an
        // upstream input port: display + image inherently
        // (they show data), prefilled-flagged components, and
        // any select/multiselect with `source: "input"`.
        let value = if render_needs_input(&render) {
            input.get(&key).cloned()
        } else {
            None
        };

        fields.push(FormField { field_type: kind, key, label, render, value, config });
    }
    Ok(fields)
}

fn render_needs_input(render: &FormFieldRender) -> bool {
    matches!(render.component.as_str(), "readonly" | "image")
        || render.source == Some(FormFieldSource::Input)
        || render.prefilled
}

/// Map the form response onto output ports declared by the node's
/// specs, dispatched on each field's `kind`:
///   * `approve_reject`: the response is the decision (`true` or
///     `"approve"`, `false` or `"reject"`). The chosen side's port
///     (`{key}_approved` or `{key}_rejected`) gets `true`; the other is
///     OMITTED so the engine emits a structural closure on it at
///     termination (the losing branch is cut by the closure marker,
///     not a `false` data pulse).
///   * Display-only kinds (no outputs): nothing is emitted.
///   * Every other kind declares the single `{key}` port, which gets
///     `response[key]` as-is.
///
/// A spec whose outputs fit none of these shapes fails loudly naming
/// its kind: guessing would hand downstream a value nobody defined.
pub fn map_response_to_ports(
    response: &Value,
    raw_fields: &[Value],
    specs: &[PortSpec],
) -> WeftResult<NodeOutput> {
    let spec_map: HashMap<&str, &PortSpec> =
        specs.iter().map(|s| (s.kind.as_str(), s)).collect();

    let mut output = NodeOutput::new();
    for field in raw_fields {
        // Same loud refusals as `build_form_fields`: silently skipping a
        // malformed field here would close its output ports and hand
        // downstream a closure instead of an error (a trigger's build
        // and wake are two different firings, so a schema saved before a
        // metadata change can reach this side alone).
        let (key, kind, spec) = resolve_field(field, &spec_map)?;
        if spec.adds_outputs.is_empty() {
            continue;
        }
        let raw_value = response.get(key).cloned().unwrap_or(Value::Null);
        emit_outputs_for_field(&mut output, kind, &spec.adds_outputs, key, raw_value)?;
    }
    Ok(output)
}

/// The kind whose response is a decision split across two ports.
const APPROVE_REJECT: &str = "approve_reject";

/// For one field, set the output ports its kind defines (see
/// [`map_response_to_ports`] for the shapes).
fn emit_outputs_for_field(
    output: &mut NodeOutput,
    kind: &str,
    ports: &[PortTemplate],
    key: &str,
    raw_value: Value,
) -> WeftResult<()> {
    let templates: Vec<&str> = ports.iter().map(|p| p.name_template.as_str()).collect();
    if kind == APPROVE_REJECT {
        if templates != ["{key}_approved", "{key}_rejected"] {
            weft::node_bail!(
                "form kind '{kind}' declares outputs {templates:?}; its response maps \
                 only onto [\"{{key}}_approved\", \"{{key}}_rejected\"]"
            );
        }
        let approved = match &raw_value {
            Value::Bool(b) => *b,
            Value::String(s) if s == "approve" => true,
            Value::String(s) if s == "reject" => false,
            other => weft::node_bail!(
                "form field '{key}' ({kind}) needs a decision (true or \"approve\", false \
                 or \"reject\"), and the response carried {other}"
            ),
        };
        // The losing side is omitted: the engine closes it at
        // termination, which tells its consumer the branch is dead.
        let chosen = if approved { &ports[0] } else { &ports[1] };
        output.outputs.insert(chosen.resolve_name(key), Value::Bool(true));
        return Ok(());
    }
    if templates != ["{key}"] {
        weft::node_bail!(
            "form kind '{kind}' declares outputs {templates:?}; a kind other than \
             '{APPROVE_REJECT}' maps its response only onto the single \"{{key}}\" port"
        );
    }
    output.outputs.insert(ports[0].resolve_name(key), raw_value);
    Ok(())
}

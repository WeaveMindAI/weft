//! Generic form-field helpers for nodes with `has_form_schema`.
//!
//! The pipeline is data-driven by the `formFieldSpecs` metadata key,
//! declared once in the package root's `metadata.json` and inherited by
//! every member (HumanQuery, HumanTrigger). The catalog loads it, the
//! compiler's enrich pass materializes ports from it, and these helpers
//! shape the runtime form schema (sent to the human) and map the user's
//! response back to output ports. The node reads its specs from its own
//! `manifest().form_field_specs`, the same document the catalog sees.
//!
//! Adding a new field type means: add an entry to that metadata. Nothing
//! in the engine, dispatcher, or UI needs to change as long as the
//! entry's `render.component` is something the consumer already knows how
//! to draw.

use std::collections::HashMap;

use serde_json::{Map, Value};
use weft_core::node::FormFieldSpec;
use weft_core::node::FormFieldPort;
use weft_core::node::NodeOutput;
use weft_core::signal::FormField;

/// Pull the `fields` array off a node's config. The canonical shape is a
/// JSON array (what the compiler produces); anything else means no fields.
pub fn parse_form_fields(config: &serde_json::Map<String, Value>) -> Vec<Value> {
    match config.get("fields") {
        Some(Value::Array(arr)) => arr.clone(),
        _ => Vec::new(),
    }
}

/// Read the `fieldType` off one entry of `config.fields`. Source .weft
/// files use `fieldType: "display"` (camelCase, flat).
pub fn field_type_of(field: &Value) -> Option<&str> {
    field.get("fieldType").and_then(|v| v.as_str())
}

/// Build the runtime form schema sent to the human.
///
/// `raw_fields` is the node's `config.fields` array. `specs` is the
/// node's `form_field_specs`. `input` is the node's input port
/// values: fields that need a pre-fill (display, image, prefilled,
/// source=input) read from here using their `key`.
///
/// Returns one `FormField` per valid entry. Entries with an
/// unknown `fieldType` (no matching spec) are dropped with a
/// warning so a misspelled type doesn't silently render an empty
/// form.
pub fn build_form_fields(
    raw_fields: &[Value],
    specs: &[FormFieldSpec],
    input: &Value,
) -> Vec<FormField> {
    let spec_map: HashMap<&str, &FormFieldSpec> =
        specs.iter().map(|s| (s.field_type.as_str(), s)).collect();

    raw_fields
        .iter()
        .filter_map(|raw| {
            let key = raw.get("key").and_then(|v| v.as_str())?.to_string();
            let field_type = field_type_of(raw)?.to_string();
            let spec = match spec_map.get(field_type.as_str()) {
                Some(s) => s,
                None => {
                    tracing::warn!(
                        target: "weft::human",
                        "unknown fieldType '{field_type}' for field '{key}'; dropping"
                    );
                    return None;
                }
            };

            let label = raw
                .get("label")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| key.clone());
            let config = raw
                .get("config")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new()));

            // Resolve render: explicit on the source field wins,
            // otherwise inherit the spec's default. The wire shape
            // is opaque JSON the consumer interprets via
            // `render.component`.
            let render = raw
                .get("render")
                .cloned()
                .unwrap_or_else(|| spec.render.clone());

            // Pre-fill `value` for fields whose render needs an
            // upstream input port: display + image inherently
            // (they show data), prefilled-flagged components, and
            // any select/multiselect with `source: "input"`.
            let needs_input = render_needs_input(&render);
            let value = if needs_input {
                input.get(&key).cloned()
            } else {
                None
            };

            Some(FormField {
                field_type,
                key,
                label,
                render,
                value,
                config,
            })
        })
        .collect()
}

fn render_needs_input(render: &Value) -> bool {
    let component = render.get("component").and_then(|v| v.as_str());
    let source = render.get("source").and_then(|v| v.as_str());
    let prefilled = render
        .get("prefilled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    matches!(component, Some("readonly") | Some("image"))
        || source == Some("input")
        || prefilled
}

/// Map the form response onto output ports declared by the node's
/// specs. Driven entirely by `adds_outputs` in the spec; no
/// per-field-type knowledge here.
///
/// Conventions a spec author should follow:
///   * Single-port outputs (`adds_outputs.len() == 1`,
///     `name_template = "{key}"`): the port value is `response[key]`.
///   * `approve_reject`-style: two boolean ports
///     (`{key}_approved`, `{key}_rejected`). The user response is
///     `{ key: bool }`; we set the active port `true` and OMIT the
///     inactive port so the engine emits a structural closure on it
///     at termination (the inactive branch is cut by the closure
///     marker, not a `null` data pulse). See `emit_outputs_for_field`.
///   * Display-only fields (no outputs): no-op; the port set is
///     empty so nothing is emitted.
///   * Anything else with a single output: `response[key]` mapped
///     verbatim.
///
/// If a spec uses templates we don't know how to fill (e.g. a
/// future "{key}.something"), we still emit it as-is and pass
/// `response[key]` so the node author's contract works.
pub fn map_response_to_ports(
    response: &Value,
    raw_fields: &[Value],
    specs: &[FormFieldSpec],
) -> NodeOutput {
    let spec_map: HashMap<&str, &FormFieldSpec> =
        specs.iter().map(|s| (s.field_type.as_str(), s)).collect();

    let mut output = NodeOutput::new();
    for field in raw_fields {
        let Some(field_type) = field_type_of(field) else { continue };
        let Some(key) = field.get("key").and_then(|v| v.as_str()) else { continue };
        let Some(spec) = spec_map.get(field_type) else {
            continue;
        };
        if spec.adds_outputs.is_empty() {
            continue;
        }
        let raw_value = response.get(key).cloned().unwrap_or(Value::Null);
        emit_outputs_for_field(&mut output, &spec.adds_outputs, key, &raw_value);
    }
    output
}

/// For one field, set every output port the spec declares. The
/// port-name template (`{key}_approved`, `{key}_rejected`,
/// `{key}`) drives both the port name and the value picker:
///   * Plain `{key}` ports get the raw response value.
///   * `{key}_<suffix>` ports interpret a boolean response: the
///     active-side port (the one whose suffix matches the response's
///     truthiness) gets `true`; the inactive-side port is OMITTED
///     from the output entirely so the engine emits a structural
///     closure on it at termination. A consumer wired to the
///     inactive port sees "this branch is structurally dead" via the
///     closure marker, not a `null` data pulse.
fn emit_outputs_for_field(
    output: &mut NodeOutput,
    ports: &[FormFieldPort],
    key: &str,
    raw_value: &Value,
) {
    // Single port keyed exactly to `{key}`: pass through.
    if ports.len() == 1 && ports[0].name_template == "{key}" {
        output.outputs.insert(ports[0].resolve_name(key), raw_value.clone());
        return;
    }

    // Multi-port pattern: split a single response value across
    // suffix-tagged ports. Boolean-style splits (approve/reject,
    // yes/no, on/off) are covered; for unknown suffixes we copy
    // the raw value to every port and let the node author tune
    // the spec.
    let truthy_suffixes: &[&str] = &["approved", "yes", "on", "true"];
    let falsy_suffixes: &[&str] = &["rejected", "no", "off", "false"];

    let response_truthy = match raw_value {
        Value::Bool(b) => *b,
        Value::String(s) => {
            let lc = s.to_ascii_lowercase();
            ["approve", "approved", "true", "yes", "on"].contains(&lc.as_str())
        }
        Value::Null => false,
        _ => true,
    };

    let mut handled_split = false;
    for port in ports {
        let resolved = port.resolve_name(key);
        let suffix = resolved
            .strip_prefix(key)
            .and_then(|rest| rest.strip_prefix('_'));
        if let Some(suffix) = suffix {
            if truthy_suffixes.contains(&suffix) {
                handled_split = true;
                if response_truthy {
                    output.outputs.insert(resolved, Value::Bool(true));
                }
                // Else: omit. The engine emits a closure at termination,
                // signaling "this branch is structurally dead" to the
                // downstream consumer.
                continue;
            }
            if falsy_suffixes.contains(&suffix) {
                handled_split = true;
                if !response_truthy {
                    output.outputs.insert(resolved, Value::Bool(true));
                }
                // Else: omit, see above.
                continue;
            }
        }
    }

    if handled_split {
        return;
    }

    // Fallback: catalog declared multiple outputs we don't
    // recognize. Emit the raw response on each so the node author
    // can iterate without losing data.
    for port in ports {
        output.outputs.insert(port.resolve_name(key), raw_value.clone());
    }
}

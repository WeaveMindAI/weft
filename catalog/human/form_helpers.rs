//! Generic form-field helpers for nodes with `has_form_schema`.
//!
//! The pipeline is data-driven by `form_field_specs.json` next to
//! the node's metadata. The catalog loads the specs at boot, the
//! compiler's enrich pass materializes ports from them, and these
//! helpers shape the runtime form schema (sent to the human) and
//! map the user's response back to output ports.
//!
//! Adding a new field type means: add an entry to that JSON.
//! Nothing in the engine, dispatcher, or UI needs to change as
//! long as the entry's `render.component` is something the
//! consumer already knows how to draw.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::{Map, Value};
use weft_core::node::FormFieldSpec;
use weft_core::node::FormFieldPort;
use weft_core::node::NodeOutput;
use weft_core::primitive::FormField;

/// Embedded copy of `form_field_specs.json` shared by HumanQuery
/// and HumanTrigger. Parsed once on first access. The catalog
/// loader also reads the same file from disk, so the on-disk and
/// in-binary versions stay in sync as long as authors edit the
/// JSON (the only source of truth) and rebuild.
const SPECS_JSON: &str = include_str!("form_field_specs.json");

pub fn human_form_field_specs() -> &'static [FormFieldSpec] {
    static CELL: OnceLock<Vec<FormFieldSpec>> = OnceLock::new();
    CELL.get_or_init(|| {
        serde_json::from_str(SPECS_JSON)
            .expect("human/form_field_specs.json must be valid FormFieldSpec[]")
    })
}

/// Pull the `fields` array off a node's config. Accepts either a
/// raw JSON array (the canonical shape after parse) or a JSON
/// string (legacy shape from older serializers).
pub fn parse_form_fields(config: &HashMap<String, Value>) -> Vec<Value> {
    match config.get("fields") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(Value::String(s)) => serde_json::from_str::<Vec<Value>>(s).unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Read the `fieldType` off one entry of `config.fields`. Source
/// .weft files use `fieldType: "display"` (camelCase, flat); some
/// older code paths produced `field_type: { kind: "display" }`. We
/// accept both so a HumanQuery authored in either era keeps
/// working.
pub fn field_type_of(field: &Value) -> Option<&str> {
    field
        .get("fieldType")
        .and_then(|v| v.as_str())
        .or_else(|| field.get("field_type").and_then(|v| v.as_str()))
        .or_else(|| {
            field
                .get("field_type")
                .and_then(|v| v.get("kind"))
                .and_then(|v| v.as_str())
        })
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
                        "unknown fieldType '{field_type}' for field '{key}' — dropping"
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
/// specs. Driven entirely by `adds_outputs` in the spec — no
/// per-field-type knowledge here.
///
/// Conventions a spec author should follow:
///   * Single-port outputs (`adds_outputs.len() == 1`,
///     `name_template = "{key}"`): the port value is `response[key]`.
///   * `approve_reject`-style: two boolean ports
///     (`{key}_approved`, `{key}_rejected`). The user response is
///     `{ key: bool }`; we set the matching port `true` and the
///     other `null` so downstream required-port propagation cuts
///     the inactive branch.
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

    let mut output = NodeOutput::empty();
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
///   * `{key}_<suffix>` ports interpret a boolean response: if
///     truthy AND suffix matches the truthy convention
///     (`approved`, `yes`, `true`), the port gets `true`; the
///     opposite-suffix port gets `null`. This keeps the v1
///     null-cuts-flow contract while staying generic: any node
///     can declare `{key}_approved` + `{key}_rejected` paired
///     ports and the runtime does the split for free.
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
                output.outputs.insert(
                    resolved,
                    if response_truthy { Value::Bool(true) } else { Value::Null },
                );
                continue;
            }
            if falsy_suffixes.contains(&suffix) {
                handled_split = true;
                output.outputs.insert(
                    resolved,
                    if !response_truthy { Value::Bool(true) } else { Value::Null },
                );
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

//! Every name a node chooses is spelled the way weft spells it
//! (`weft_core::casing`): the node type in PascalCase; its ports, its
//! settings, the fields of its record types, the values its connection
//! stores and every key of its `metadata.json` in camelCase. Discovery
//! runs these checks on every node it loads, the standard library's and
//! a project's own alike, and turns each name that is off into a catalog
//! warning naming the node, the file and the spelling that fits. A
//! warning never leaves a node out: the node loads as usual.
//!
//! Only names weft or the node author choose are held to this. What is
//! someone else's (a value the node passes to a provider, a field a
//! third-party library serializes) is DATA, and the walk skips it; each
//! skip is listed with the reason it is not ours to name, in
//! `weft_core::node::METADATA_PLACES` and in `THIRD_PARTY_TYPES` below.
//!
//! Discovery calls this on the `serde_json::Value` and the
//! `NodeMetadata` it already holds, so the check reads no file and
//! parses nothing a second time.

use serde_json::{Map, Value};
use weft_core::casing::Casing;
use weft_core::node::{metadata_place_at, InputSpec, MetadataPlace, NodeMetadata, OutputSpec, PortsFromConfig};
use weft_core::weft_type::WeftType;
use weft_core::project::MetadataPort;

/// Port names the LANGUAGE reserves. Every one starts with `_`, which
/// keeps it out of every node's own namespace, so it is the language's
/// spelling and never a node's.
const RESERVED_PORTS: &[(&str, &str)] = &[
    (weft_core::run::skip::SHOULD_FLOW_PORT, "the gate every node takes, owned by the language"),
    (weft_core::run::skip::SHOULD_NOT_FLOW_PORT, "the inverted gate, owned by the language"),
    (weft_core::node::ERROR_PORT, "the failure output every node has, owned by the language"),
    (weft_core::tag::TAGS_CONFIG_KEY, "the tags key the language reads off every node"),
];

/// Declared types whose record fields copy a third-party wire format
/// verbatim. A rename here breaks the round trip the type exists for.
const THIRD_PARTY_TYPES: &[(&str, &str)] = &[
    (
        "ChatMessage",
        "mirrors minillmlib's message serde field for field (`tool_call_id`, `tool_calls`, \
         `cache_breakpoint`); `tests/integration/chat_types.rs` holds the two together",
    ),
    (
        "ChatContentPart",
        "mirrors minillmlib's content-part serde, itself the OpenAI wire shape (`image_url`, \
         `input_audio`, `duration_secs`); `tests/integration/chat_types.rs` holds the two together",
    ),
];

/// The stored values a connection keeps are named by the recipe and
/// read by name (`{smtpHost}` in a template, `requiresValues`, node
/// code), so each is a name weft chooses even though it sits in a JSON
/// string. These are the lists whose items' `name` is one.
const VALUE_NAME_LISTS: &[&str] = &["fields", "captures", "registrationFields"];

/// Every name in one `metadata.json` document (a node's own, or a
/// package root's defaults) that is off: every key that is a metadata key
/// or a name, at every depth, every stored-value name, and the record
/// fields of every type the document declares. One message per name, as
/// `Casing::check` words it. Declared types are parsed through the
/// registry that is active, so call this inside discovery's
/// `TypeRegistry::scoped`.
pub(crate) fn check_document(document: &Map<String, Value>, out: &mut Vec<String>) {
    walk_map(out, &mut Vec::new(), document, metadata_place_at(&[]));
    if let Some(types) = document.get("types").and_then(Value::as_object) {
        for name in types.keys() {
            if let Some(ty) = WeftType::parse(name) {
                check_record_fields(out, &ty, true);
            }
        }
    }
}

/// Every name a loaded node chooses that is off, read from its typed
/// metadata: the node type, its ports (and the record fields of their
/// types), the stored values its inputs require, and the ports its
/// `portsFromConfig` specs make. `inherited` says whether a top-level key
/// came from the package root rather than the node's own file: what is
/// inherited is the root's to spell, checked once on the root
/// ([`check_defaults_ports`]), so it is skipped here.
pub(crate) fn check_node(meta: &NodeMetadata, inherited: impl Fn(&str) -> bool, out: &mut Vec<String>) {
    check(out, Casing::Pascal, "node type", &meta.node_type);
    if !inherited("inputs") {
        check_inputs(&meta.inputs, out);
    }
    if !inherited("outputs") {
        check_outputs(&meta.outputs, out);
    }
    if !inherited("metadataPorts") {
        check_metadata_ports(&meta.metadata_ports, out);
    }
    if !inherited("portsFromConfig") {
        if let Some(pfc) = &meta.ports_from_config {
            check_ports_from_config(pfc, out);
        }
    }
}

/// The ports a package root's defaults declare (`inputs`, `outputs`,
/// `portsFromConfig`), checked as [`check_node`] checks a node's own.
/// A key whose value does not parse is skipped: every member inheriting
/// it fails to load with that parse error, which is the message to read.
pub(crate) fn check_defaults_ports(defaults: &Map<String, Value>, out: &mut Vec<String>) {
    fn parsed<T: serde::de::DeserializeOwned>(defaults: &Map<String, Value>, key: &str) -> Option<T> {
        defaults.get(key).and_then(|v| serde_json::from_value(v.clone()).ok())
    }
    if let Some(inputs) = parsed::<Vec<InputSpec>>(defaults, "inputs") {
        check_inputs(&inputs, out);
    }
    if let Some(outputs) = parsed::<Vec<OutputSpec>>(defaults, "outputs") {
        check_outputs(&outputs, out);
    }
    if let Some(ports) = parsed::<Vec<MetadataPort>>(defaults, "metadataPorts") {
        check_metadata_ports(&ports, out);
    }
    if let Some(pfc) = parsed::<PortsFromConfig>(defaults, "portsFromConfig") {
        check_ports_from_config(&pfc, out);
    }
}

fn reserved(name: &str) -> bool {
    RESERVED_PORTS.iter().any(|(r, _)| *r == name)
}

fn check_inputs(inputs: &[InputSpec], out: &mut Vec<String>) {
    for input in inputs {
        if !reserved(&input.name) {
            check(out, Casing::Camel, "input", &input.name);
        }
        check_record_fields(out, &input.input_type, false);
        for value in input.requires_values.iter().flatten() {
            check(out, Casing::Camel, "stored value name", value);
        }
    }
}

fn check_outputs(outputs: &[OutputSpec], out: &mut Vec<String>) {
    for output in outputs {
        if !reserved(&output.name) {
            check(out, Casing::Camel, "output", &output.name);
        }
        check_record_fields(out, &output.port_type, false);
    }
}

/// A metadata port's name and the record fields of its type. Its value is
/// the node's data, spelled however that data is.
fn check_metadata_ports(ports: &[MetadataPort], out: &mut Vec<String>) {
    for port in ports {
        check(out, Casing::Camel, "metadata port", &port.name);
        check_record_fields(out, &port.port_type, false);
    }
}

fn check_ports_from_config(pfc: &PortsFromConfig, out: &mut Vec<String>) {
    for spec in &pfc.specs {
        check(out, Casing::Camel, "entry key field", &spec.key_field);
        for field in &spec.fields {
            check(out, Casing::Camel, "entry field", &field.key);
        }
        // A template makes a port per entry; fill it with a
        // well-spelled key and the port it makes must be too.
        for template in spec.adds_inputs.iter().chain(&spec.adds_outputs) {
            let port = template.name_template.replace("{key}", "answer");
            check(out, Casing::Camel, "port made by a template", &port);
        }
    }
}

fn check(out: &mut Vec<String>, casing: Casing, what: &str, name: &str) {
    if let Err(message) = casing.check(what, name) {
        out.push(message);
    }
}

/// Walk one value of a `metadata.json`, checking every key that is a
/// metadata key or a name, and every stored-value name. `path` is where
/// the value sits, as `METADATA_PLACES` patterns spell it.
fn walk<'a>(out: &mut Vec<String>, path: &mut Vec<&'a str>, value: &'a Value) {
    let place = metadata_place_at(path);
    if matches!(place, Some(MetadataPlace::Data)) {
        return;
    }
    match value {
        Value::Object(map) => walk_map(out, path, map, place),
        Value::Array(items) => {
            path.push("[]");
            for item in items {
                walk(out, path, item);
            }
            path.pop();
        }
        _ => {}
    }
}

/// The keys of one map at `path`, which sits at `place`, and every value
/// below them.
fn walk_map<'a>(out: &mut Vec<String>, path: &mut Vec<&'a str>, map: &'a Map<String, Value>, place: Option<MetadataPlace>) {
    let in_value_list = path.len() >= 2
        && path[path.len() - 1] == "[]"
        && path.first() == Some(&"service")
        && VALUE_NAME_LISTS.contains(&path[path.len() - 2]);
    for (key, child) in map {
        let segment = match place {
            Some(MetadataPlace::NamesAre(casing, what)) => {
                check(out, casing, what, key.strip_suffix('?').unwrap_or(key));
                "*"
            }
            _ => {
                check(out, Casing::Camel, "metadata key", key);
                key.as_str()
            }
        };
        if in_value_list && key == "name" {
            if let Some(name) = child.as_str() {
                check(out, Casing::Camel, "stored value name", name);
            }
        }
        path.push(segment);
        walk(out, path, child);
        path.pop();
    }
}

/// Every record field name inside `ty`, checked camelCase, except
/// inside a declared type that copies a third party.
fn check_record_fields(out: &mut Vec<String>, ty: &WeftType, declaration: bool) {
    match ty {
        WeftType::Record { fields, .. } => {
            for field in fields {
                check(out, Casing::Camel, "record field", &field.name);
                check_record_fields(out, &field.ty, false);
            }
        }
        // A named type's fields are checked once, where it is declared
        // (`declaration`); everywhere it is used, it is only its name, so
        // a package root's type warns on the root and never again on each
        // member whose ports use it.
        WeftType::Named { name, body } => {
            if declaration && !THIRD_PARTY_TYPES.iter().any(|(t, _)| t == name) {
                check_record_fields(out, body, false);
            }
        }
        WeftType::List(inner) | WeftType::Generator(inner) | WeftType::Dict(inner) => {
            check_record_fields(out, inner, false)
        }
        WeftType::Union(members) => {
            for m in members {
                check_record_fields(out, m, false);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(document: Value) -> Vec<String> {
        let mut out = Vec::new();
        check_document(document.as_object().expect("an object"), &mut out);
        out
    }

    #[test]
    fn a_key_that_is_data_or_a_third_partys_is_not_held_to_weft_spelling() {
        let found = messages(serde_json::json!({
            "inputs": [{ "name": "x", "default": { "user_id": 1 } }],
            "accessApps": { "google": { "label": "Acme", "client_id": "id" } },
            "service": { "acquisition": { "authParams": { "access_type": "offline" } } },
        }));
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_snake_case_metadata_key_and_a_wrong_name_are_each_one_message() {
        let found = messages(serde_json::json!({
            "some_key": 1,
            "service": { "events": { "driveChanges": { "fields": { "file_id": "String" } } } },
        }));
        let mut found = found;
        found.sort();
        assert_eq!(
            found,
            [
                "event field 'file_id' is not camelCase: write 'fileId'",
                "event topic 'driveChanges' is not snake_case: write 'drive_changes'",
                "metadata key 'some_key' is not camelCase: write 'someKey'",
            ]
        );
    }

    /// A metadata port's name is the node's to spell; its value is its data,
    /// spelled however that data is (a JSON Schema's own keys).
    #[test]
    fn a_metadata_ports_name_is_checked_and_its_value_is_not() {
        let document = serde_json::json!({
            "type": "Search",
            "label": "Search",
            "description": "d",
            "metadataPorts": [{ "name": "tool_spec", "type": "{ name: String, extra: JsonDict }", "value": { "name": "s", "extra": { "additional_properties": false } } }],
        });
        assert!(messages(document.clone()).is_empty(), "the value is data");
        let meta: NodeMetadata = serde_json::from_value(document).expect("parses");
        let mut found = Vec::new();
        check_node(&meta, |_| false, &mut found);
        assert_eq!(found, ["metadata port 'tool_spec' is not camelCase: write 'toolSpec'"]);
    }
}

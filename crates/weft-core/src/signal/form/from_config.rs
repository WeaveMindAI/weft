//! A form whose fields a node's own settings list, the way HumanQuery's
//! and HumanTrigger's `fields` do.
//!
//! The node's metadata declares `portsFromConfig`: the setting holding
//! the entries, and one spec per entry `kind` saying how that kind is
//! drawn (`render`), which of the entry's keys reach the consumer
//! (`fields`), and the ports it adds. The compiler's enrich pass makes
//! the ports from the same specs, so a new field kind is a new spec in
//! metadata and nothing here changes. [`ConfigForm`] turns the entries
//! into the [`FormField`]s every form is built from, and an [`Answers`]
//! back onto the ports the entries added.

use std::collections::HashMap;

use serde_json::{Map, Value};

use super::{Answer, Answers, FormField};
use crate::context::ValueBag;
use crate::error::WeftResult;
use crate::node::{FormFieldSource, NodeMetadata, NodeOutput, PortSpec, PortsFromConfig};

/// A node's form entries, read against the specs its metadata declares.
pub struct ConfigForm<'a> {
    entries: Vec<Value>,
    specs: HashMap<&'a str, &'a PortSpec>,
}

impl<'a> ConfigForm<'a> {
    /// The entries a node's settings hold, under the setting its
    /// `portsFromConfig` names. A node whose metadata declares no
    /// `portsFromConfig` has no fields to build a form from, so this
    /// fails rather than drawing an empty one.
    pub fn of(manifest: &'a NodeMetadata, inputs: &ValueBag) -> WeftResult<ConfigForm<'a>> {
        let Some(config) = &manifest.ports_from_config else {
            crate::node_bail!(
                "node type '{}' builds a form but its metadata declares no `portsFromConfig`",
                manifest.node_type
            );
        };
        Ok(ConfigForm::new(inputs.list(&config.field)?, config))
    }

    /// `entries` read against `config`'s specs.
    pub fn new(entries: Vec<Value>, config: &'a PortsFromConfig) -> ConfigForm<'a> {
        ConfigForm { entries, specs: config.specs.iter().map(|s| (s.kind.as_str(), s)).collect() }
    }

    /// Resolve one entry against the specs: its key, kind and spec. The
    /// ONE place the three malformed-entry shapes fail (a missing `key`,
    /// a missing `kind`, an unknown kind), shared by building and
    /// reading so a trigger's two firings (its setup builds, its wake
    /// reads) can never disagree on what a well-formed entry is.
    fn resolve<'e>(&self, entry: &'e Value) -> WeftResult<(&'e str, &'e str, &'a PortSpec)> {
        let Some(key) = entry.get("key").and_then(|v| v.as_str()) else {
            crate::node_bail!("a form field has no `key`; every field names its port");
        };
        let Some(kind) = entry.get("kind").and_then(|v| v.as_str()) else {
            crate::node_bail!("form field '{key}' has no `kind`");
        };
        let Some(spec) = self.specs.get(kind) else {
            crate::node_bail!("form field '{key}' has kind '{kind}', which this node does not offer");
        };
        Ok((key, kind, spec))
    }

    /// One [`FormField`] per entry. `prefill` holds, by key, the values a
    /// field that shows or starts from something takes: a shown field
    /// (`readonly`, `image`), a `prefilled` one, and a select whose
    /// options come from an input (`source: input`). A query passes its
    /// wired inputs; a trigger has none and passes an empty record.
    ///
    /// An entry the specs cannot serve is an error rather than a dropped
    /// field: the compiler refuses an unknown `kind` and a spec with no
    /// render, so reaching either here means the person would have been
    /// shown an incomplete form.
    pub fn fields(&self, prefill: &Value) -> WeftResult<Vec<FormField>> {
        let mut fields = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let (key, kind, spec) = self.resolve(entry)?;
            // The render comes from the SPEC: an entry cannot carry its
            // own, because the compiler admits only `kind`, the key
            // field and the spec's declared fields as entry keys.
            let Some(render) = spec.render.clone() else {
                crate::node_bail!(
                    "form field '{key}' has kind '{kind}', which declares no render, \
                     so nothing knows how to draw it"
                );
            };
            let label = entry.get("label").and_then(|v| v.as_str()).unwrap_or(key);
            let mut field = FormField::new(kind, key, label, render);
            // What this kind asked the author to fill in, gathered off
            // the entry by the keys the spec asks for. `label` is a
            // column of its own on the wire, so it is not repeated in
            // here. A present null is absent (the editor clears a box by
            // writing null), so it never reaches the wire.
            // SYNC: null-is-absent <->
            //       crates/weft-compiler/src/validate.rs (the spec-field loop),
            //       packages/weft-graph/src/webview/lib/utils/port-specs.ts (hasValue)
            let mut config = Map::new();
            for spec_field in &spec.fields {
                if spec_field.key == "label" {
                    continue;
                }
                if let Some(value) = entry.get(spec_field.key.as_str()).filter(|v| !v.is_null()) {
                    config.insert(spec_field.key.clone(), value.clone());
                }
            }
            field.config = config;
            if takes_prefill(&field) {
                field.value = prefill.get(key).cloned();
            }
            fields.push(field);
        }
        Ok(fields)
    }

    /// Put `answers` on the ports the entries added, by each entry's
    /// spec:
    ///   * a decision sets `{key}Approved` or `{key}Rejected` to `true`,
    ///     and OMITS the other, so the engine closes it at termination
    ///     (the losing branch is cut by the closure, not by a `false`
    ///     pulse);
    ///   * a kind that adds no outputs sets nothing;
    ///   * every other answer goes as it came on the single `{key}` port.
    ///
    /// A spec whose outputs fit none of these shapes fails naming its
    /// kind: guessing would hand downstream a value nobody defined.
    pub fn outputs(&self, answers: &Answers) -> WeftResult<NodeOutput> {
        let mut output = NodeOutput::new();
        for entry in &self.entries {
            // Same refusals as `fields`: skipping a malformed entry here
            // would close its ports and hand downstream a closure instead
            // of an error (a trigger's setup and wake are two firings, so
            // a schema saved before a metadata change can reach this side
            // alone).
            let (key, kind, spec) = self.resolve(entry)?;
            if spec.adds_outputs.is_empty() {
                continue;
            }
            let templates: Vec<&str> = spec.adds_outputs.iter().map(|p| p.name_template.as_str()).collect();
            match answers.get(key) {
                Some(Answer::Decision(approved)) => {
                    if templates != ["{key}Approved", "{key}Rejected"] {
                        crate::node_bail!(
                            "form kind '{kind}' declares outputs {templates:?}; a decision maps \
                             only onto [\"{{key}}Approved\", \"{{key}}Rejected\"]"
                        );
                    }
                    let chosen = &spec.adds_outputs[if *approved { 0 } else { 1 }];
                    output.outputs.insert(chosen.resolve_name(key), Value::Bool(true));
                }
                Some(Answer::Given(value)) => {
                    if templates != ["{key}"] {
                        crate::node_bail!(
                            "form kind '{kind}' declares outputs {templates:?}; an answer other \
                             than a decision maps only onto the single \"{{key}}\" port"
                        );
                    }
                    output.outputs.insert(spec.adds_outputs[0].resolve_name(key), value.clone());
                }
                None => crate::node_bail!(
                    "form kind '{kind}' declares outputs {templates:?}, but its component \
                     '{}' only shows a value and asks nothing",
                    spec.render.as_ref().map(|r| r.component.as_str()).unwrap_or("none")
                ),
            }
        }
        Ok(output)
    }
}

/// Does this field show or start from a value the node hands it?
fn takes_prefill(field: &FormField) -> bool {
    !field.asks() || field.render.source == Some(FormFieldSource::Input) || field.render.prefilled
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::FormSchema;
    use serde_json::json;

    /// A cut of the human package's specs: one of each shape the
    /// building and the reading tell apart.
    fn config() -> PortsFromConfig {
        serde_json::from_value(json!({
            "field": "fields",
            "specs": [
                { "kind": "display", "render": { "component": "readonly" }, "fields": ["label"],
                  "addsInputs": [{ "nameTemplate": "{key}", "portType": "T_Auto" }] },
                { "kind": "approve_reject", "render": { "component": "buttons", "source": "static" },
                  "fields": ["label", "approveLabel", "rejectLabel"],
                  "addsOutputs": [{ "nameTemplate": "{key}Approved", "portType": "Boolean" },
                                  { "nameTemplate": "{key}Rejected", "portType": "Boolean" }] },
                { "kind": "select_input", "render": { "component": "select", "source": "input" }, "fields": ["label"],
                  "addsInputs": [{ "nameTemplate": "{key}", "portType": "List[String]" }],
                  "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }] },
                { "kind": "text_input", "render": { "component": "text" }, "fields": ["label", "placeholder"],
                  "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }] },
                { "kind": "no_render", "fields": ["label"] },
                { "kind": "shown_with_output", "render": { "component": "readonly" },
                  "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }] }
            ]
        }))
        .expect("the specs parse")
    }

    #[test]
    fn entries_become_fields_with_their_spec_render_config_and_prefill() {
        let config = config();
        let form = ConfigForm::new(
            vec![
                json!({ "kind": "display", "key": "question" }),
                json!({ "kind": "approve_reject", "key": "send", "label": "Send it?", "approveLabel": "Yes", "rejectLabel": null }),
                json!({ "kind": "select_input", "key": "pick" }),
                json!({ "kind": "text_input", "key": "email", "placeholder": "you@example.com" }),
            ],
            &config,
        );
        let fields = form
            .fields(&json!({ "question": "Why?", "pick": ["a", "b"], "email": "never shown", "send": "never shown" }))
            .unwrap();
        let json = serde_json::to_value(FormSchema { fields }).unwrap();
        assert_eq!(
            json,
            json!({ "fields": [
                { "fieldType": "display", "key": "question", "label": "question",
                  "render": { "component": "readonly" }, "value": "Why?", "config": {} },
                { "fieldType": "approve_reject", "key": "send", "label": "Send it?",
                  "render": { "component": "buttons", "source": "static" }, "config": { "approveLabel": "Yes" } },
                { "fieldType": "select_input", "key": "pick", "label": "pick",
                  "render": { "component": "select", "source": "input" }, "value": ["a", "b"], "config": {} },
                { "fieldType": "text_input", "key": "email", "label": "email",
                  "render": { "component": "text" }, "config": { "placeholder": "you@example.com" } }
            ] })
        );
    }

    #[test]
    fn a_malformed_entry_fails_both_building_and_reading() {
        let config = config();
        for (entry, expected) in [
            (json!({ "kind": "display" }), "has no `key`"),
            (json!({ "key": "a" }), "has no `kind`"),
            (json!({ "kind": "nope", "key": "a" }), "does not offer"),
        ] {
            let form = ConfigForm::new(vec![entry], &config);
            let built = form.fields(&json!({})).unwrap_err().to_string();
            assert!(built.contains(expected), "{built}");
            let read = form.outputs(&Answers::default()).unwrap_err().to_string();
            assert!(read.contains(expected), "{read}");
        }
        let form = ConfigForm::new(vec![json!({ "kind": "no_render", "key": "a" })], &config);
        assert!(form.fields(&json!({})).unwrap_err().to_string().contains("declares no render"));
    }

    #[test]
    fn answers_land_on_the_ports_their_entries_added() {
        let config = config();
        let form = ConfigForm::new(
            vec![
                json!({ "kind": "display", "key": "question" }),
                json!({ "kind": "approve_reject", "key": "send" }),
                json!({ "kind": "approve_reject", "key": "keep" }),
                json!({ "kind": "text_input", "key": "email" }),
                json!({ "kind": "text_input", "key": "unsent" }),
            ],
            &config,
        );
        let schema = FormSchema { fields: form.fields(&json!({ "question": "Why?" })).unwrap() };
        let answers = schema.answers(&json!({ "send": "approve", "keep": false, "email": "a@b.c" })).unwrap();
        let out = form.outputs(&answers).unwrap().outputs;
        assert_eq!(out.get("sendApproved"), Some(&json!(true)));
        assert_eq!(out.get("sendRejected"), None, "the losing side is closed, never a false pulse");
        assert_eq!(out.get("keepRejected"), Some(&json!(true)));
        assert_eq!(out.get("keepApproved"), None);
        assert_eq!(out.get("email"), Some(&json!("a@b.c")));
        assert_eq!(out.get("unsent"), Some(&Value::Null), "a field sent nothing for carries null");
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn a_spec_whose_outputs_fit_no_answer_shape_fails_naming_its_kind() {
        let config = config();
        let form = ConfigForm::new(vec![json!({ "kind": "shown_with_output", "key": "a" })], &config);
        let schema = FormSchema { fields: form.fields(&json!({})).unwrap() };
        let err = form.outputs(&schema.answers(&json!({})).unwrap()).unwrap_err().to_string();
        assert!(err.contains("'shown_with_output'") && err.contains("asks nothing"), "{err}");
    }
}

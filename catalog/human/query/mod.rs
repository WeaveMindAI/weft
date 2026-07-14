//! HumanQuery: pauses mid-execution waiting for a human form
//! submission. Single-phase: `execute` builds a Form signal with
//! `is_resume=true`, awaits its fire, maps the submission to outputs.

use async_trait::async_trait;

use weft_core::signal::{Form, FormSchema};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::form_helpers::{build_form_fields, map_response_to_ports, parse_form_fields};

#[derive(NodeManifest)]
pub struct HumanQueryNode;

#[async_trait]
impl Node for HumanQueryNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let raw_fields = parse_form_fields(&ctx.config.values);
        let specs = &self.manifest().form_field_specs;

        let title = ctx
            .config
            .values
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let description = ctx
            .config
            .values
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Project the node's input port values into a flat
        // {key: value} map so display / prefilled / source=input
        // fields can lift them out by key.
        let mut input_obj = serde_json::Map::new();
        for (k, v) in ctx.input.iter() {
            input_obj.insert(k.clone(), v.clone());
        }
        let input_value = serde_json::Value::Object(input_obj);

        let fields = build_form_fields(&raw_fields, specs, &input_value);

        let schema = FormSchema {
            title: title.clone(),
            description: description.clone(),
            fields,
        };

        let submission = ctx
            .await_signal(Form {
                form_type: "human-query".to_string(),
                schema,
                title: if title.is_empty() { None } else { Some(title) },
                description,
                consumer_kind: Some("human_in_the_loop".into()),
            })
            .await?;
        ctx.pulse_downstream(map_response_to_ports(&submission, &raw_fields, specs)).await
    }
}

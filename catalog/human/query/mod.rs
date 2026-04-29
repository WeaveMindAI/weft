use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::primitive::{FormSchema, WakeSignalKind, WakeSignalSpec};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

use super::form_helpers::{
    build_form_fields, human_form_field_specs, map_response_to_ports, parse_form_fields,
};

pub struct HumanQueryNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for HumanQueryNode {
    fn node_type(&self) -> &'static str {
        "HumanQuery"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("HumanQuery metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let raw_fields = parse_form_fields(&ctx.config.values);
        let specs = human_form_field_specs();

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

        let spec = WakeSignalSpec {
            kind: WakeSignalKind::Form {
                form_type: "human-query".to_string(),
                schema,
                title: if title.is_empty() { None } else { Some(title) },
                description,
            },
            is_resume: true,
        };
        let submission = ctx.await_signal(spec).await?;
        Ok(map_response_to_ports(&submission, &raw_fields, specs))
    }
}

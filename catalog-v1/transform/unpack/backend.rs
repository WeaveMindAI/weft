//! Unpack Node - Extracts fields from a Dict into individual outputs

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, PortDefinition, WeftType, ResolvedTypes, ExecutionContext};
use crate::{NodeResult, register_node};

/// Unpack node that extracts fields from a Dict input into individual outputs.
/// Users can add arbitrary output ports, and values are extracted from the input Dict.
#[derive(Default)]
pub struct UnpackNode;

#[async_trait]
impl Node for UnpackNode {
    fn node_type(&self) -> &'static str {
        "Unpack"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Unpack",
            inputs: vec![
                PortDef::new("in", "Dict[String, T]", true),
            ],
            outputs: vec![],
            features: NodeFeatures {
                canAddOutputPorts: true,
                ..Default::default()
            },
            fields: vec![],
        }
    }

    fn resolve_types(
        &self,
        _inputs: &[PortDefinition],
        outputs: &[PortDefinition],
    ) -> ResolvedTypes {
        if outputs.is_empty() {
            return ResolvedTypes::default();
        }
        let value_types: Vec<WeftType> = outputs.iter()
            .map(|p| p.portType.clone())
            .collect();
        let value_type = WeftType::union(value_types);
        let in_type = WeftType::dict(
            WeftType::primitive(weft_core::weft_type::WeftPrimitive::String),
            value_type,
        );
        ResolvedTypes {
            inputs: vec![("in".to_string(), in_type)],
            outputs: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let input_dict = ctx.input.get("in")
            .cloned()
            .unwrap_or(ctx.input.clone());

        match input_dict {
            serde_json::Value::Object(map) => {
                NodeResult::completed(serde_json::Value::Object(map))
            }
            _ => NodeResult::completed(serde_json::json!({}))
        }
    }
}

register_node!(UnpackNode);

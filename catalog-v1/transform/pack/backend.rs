//! Pack Node - Combines multiple inputs into a single Dict output

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, PortDefinition, WeftType, ResolvedTypes, ExecutionContext};
use crate::{NodeResult, register_node};

/// Pack node that combines multiple inputs into a single Dict output.
/// Users can add arbitrary input ports, and all values are bundled into one Dict.
#[derive(Default)]
pub struct PackNode;

#[async_trait]
impl Node for PackNode {
    fn node_type(&self) -> &'static str {
        "Pack"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Pack",
            inputs: vec![],
            outputs: vec![
                PortDef::new("out", "Dict[String, T]", false),
            ],
            features: NodeFeatures {
                canAddInputPorts: true,
                ..Default::default()
            },
            fields: vec![],
        }
    }

    fn resolve_types(
        &self,
        inputs: &[PortDefinition],
        _outputs: &[PortDefinition],
    ) -> ResolvedTypes {
        if inputs.is_empty() {
            return ResolvedTypes::default();
        }
        let value_types: Vec<WeftType> = inputs.iter()
            .map(|p| p.portType.clone())
            .collect();
        let value_type = WeftType::union(value_types);
        let out_type = WeftType::dict(
            WeftType::primitive(weft_core::weft_type::WeftPrimitive::String),
            value_type,
        );
        ResolvedTypes {
            inputs: vec![],
            outputs: vec![("out".to_string(), out_type)],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "out": ctx.input }))
    }
}

register_node!(PackNode);

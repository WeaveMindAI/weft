//! Template Node - String interpolation with {{variable}} syntax

use async_trait::async_trait;
use regex::Regex;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TemplateNode;

#[async_trait]
impl Node for TemplateNode {
    fn node_type(&self) -> &'static str {
        "Template"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Template",
            inputs: vec![
                PortDef::new("template", "String", true),
            ],
            outputs: vec![
                PortDef::new("text", "String", false),
            ],
            features: NodeFeatures {
                canAddInputPorts: true,
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let template = ctx.input.get("template")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Replace {{variable}} with values from input ports
        let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();
        
        let result = re.replace_all(template, |caps: &regex::Captures| {
            let var_name = &caps[1];
            
            // Look up the variable in input ports
            if let Some(value) = ctx.input.get(var_name) {
                match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "".to_string(),
                    _ => value.to_string(),
                }
            } else {
                // Keep original placeholder if no value provided
                format!("{{{{{}}}}}", var_name)
            }
        });

        NodeResult::completed(serde_json::json!({
            "text": result.to_string(),
        }))
    }
}

register_node!(TemplateNode);

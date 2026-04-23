//! ApiPost: a webhook trigger node. Declares a `Webhook` entry
//! primitive in its metadata; the dispatcher mints a URL per project
//! activation. On POST, the dispatcher routes the body into a new
//! execution and invokes this node's `execute` with the body's
//! fields as input.
//!
//! Users declare the expected body shape as output ports on the
//! node (features.canAddOutputPorts). At fire time we copy each
//! requested field out of the JSON body onto its matching output
//! port, plus `receivedAt` with the timestamp.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct ApiPostNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for ApiPostNode {
    fn node_type(&self) -> &'static str {
        "ApiPost"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("ApiPost metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        // The dispatcher feeds us the request body by merging its
        // top-level keys into the input bag, plus a `receivedAt`
        // field. Forward every input key we see to an output
        // (v1 behaviour): whatever the user declared as an output
        // port will pick up the matching body field.
        let mut output = NodeOutput::empty();
        for (k, v) in ctx.input.iter() {
            output = output.set(k.as_str(), v.clone());
        }
        if output.get("receivedAt").is_none() {
            output = output.set(
                "receivedAt",
                Value::String(chrono::Utc::now().to_rfc3339()),
            );
        }
        Ok(output)
    }

}

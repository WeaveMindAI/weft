//! WhatsAppBridge: infra node. Provisioning is declarative (see
//! metadata.json `sidecar` field); the dispatcher applies the
//! manifests at `weft infra up`. At runtime (Fire phase) this node
//! queries the sidecar's `/outputs` endpoint and forwards the
//! result to its declared output ports.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct WhatsAppBridgeNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for WhatsAppBridgeNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppBridge"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("WhatsAppBridge metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        match ctx.phase {
            Phase::InfraSetup | Phase::TriggerSetup | Phase::Fire => {
                query_outputs(&ctx).await
            }
        }
    }
}

/// Ask the dispatcher for this node's sidecar endpoint, call its
/// `/outputs` helper, forward every top-level field to a matching
/// output port. Also emits `endpointUrl` so downstream triggers
/// can pick it up.
async fn query_outputs(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    let endpoint = ctx.sidecar_endpoint().await?;
    let outputs_url = endpoint
        .replace("/action", "/outputs")
        .trim_end_matches('/')
        .to_string();
    let final_url = if outputs_url.ends_with("/outputs") {
        outputs_url
    } else {
        format!("{outputs_url}/outputs")
    };
    let resp = reqwest::Client::new()
        .get(&final_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| weft_core::error::WeftError::NodeExecution(format!("GET {final_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(weft_core::error::WeftError::NodeExecution(format!(
            "sidecar /outputs returned {}",
            resp.status()
        )));
    }
    let sidecar_outputs: Value = resp
        .json()
        .await
        .map_err(|e| weft_core::error::WeftError::NodeExecution(format!("parse /outputs: {e}")))?;

    let mut output = NodeOutput::empty().set("endpointUrl", Value::String(endpoint.clone()));
    if let Value::Object(map) = sidecar_outputs {
        for (k, v) in map {
            output = output.set(k, v);
        }
    }
    Ok(output)
}

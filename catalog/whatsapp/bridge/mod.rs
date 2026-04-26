//! WhatsAppBridge: infra node.
//!
//! - `Phase::InfraSetup`: the node calls `ctx.provision_sidecar`
//!   with its own SidecarSpec. The dispatcher applies k8s
//!   manifests and returns the handle. The node emits
//!   `endpointUrl` so downstream nodes that need it during the
//!   same setup sub-execution can wire through edges.
//! - `Phase::Fire` / `Phase::TriggerSetup`: the node queries the
//!   sidecar's `/outputs` endpoint and forwards the fields to
//!   its output ports.

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
            Phase::InfraSetup => provision(&ctx).await,
            Phase::Fire | Phase::TriggerSetup => query_outputs(&ctx).await,
        }
    }
}

/// InfraSetup: ask the dispatcher to apply this node's sidecar
/// manifests. The spec comes from the node's own metadata, which
/// the runtime receives as part of the `NodeMetadata` read at
/// startup. Emit the endpoint URL + instance id as outputs so
/// the setup subgraph can thread them to anything that needs them.
async fn provision(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    let meta: NodeMetadata = serde_json::from_str(METADATA_JSON)
        .expect("WhatsAppBridge metadata.json must be valid");
    let spec = meta.features.sidecar.ok_or_else(|| {
        weft_core::error::WeftError::Config(
            "WhatsAppBridge metadata missing sidecar spec".into(),
        )
    })?;

    let handle = ctx.provision_sidecar(spec).await?;

    Ok(NodeOutput::empty()
        .set("endpointUrl", Value::String(handle.endpoint_url.clone()))
        .set("instanceId", Value::String(handle.instance_id.clone())))
}

/// Fire / TriggerSetup: ask the dispatcher for this node's
/// sidecar endpoint, call its `/outputs` helper, forward every
/// top-level field to a matching output port. Also emits
/// `endpointUrl` so downstream triggers can pick it up.
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

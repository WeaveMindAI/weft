//! LiveSocket: trigger node that turns a weft program into a live WebSocket
//! endpoint. An outside caller opens a WebSocket at the project's gateway
//! URL; the dispatcher routes the held socket to a worker; this trigger
//! fires a fresh execution whose nodes hold a two-way conversation with the
//! caller via `ctx.caller()` (send / receive / request / close).
//!
//! Two phases:
//!   - `Phase::TriggerSetup`: build a `LiveSocket` signal from the node's
//!     fields and register it. The dispatcher mounts the public route.
//!   - `Phase::Fire`: the caller is already attached for this run; this node
//!     just kicks the graph (downstream nodes drive `ctx.caller()`), emitting
//!     a `started` pulse.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::signal::{LiveConnectionConfig, LiveSocket};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct LiveSocketNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for LiveSocketNode {
    fn node_type(&self) -> &'static str {
        "LiveSocket"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("LiveSocket metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        match ctx.phase {
            Phase::TriggerSetup => {
                let common = LiveConnectionConfig::from_node_fields(&ctx.config.values);
                ctx.register_signal(LiveSocket { common }).await
            }
            Phase::Fire => {
                ctx.pulse_downstream(NodeOutput::empty().set("started", Value::Bool(true)))
                    .await
            }
            Phase::InfraSetup => Ok(()),
        }
    }
}

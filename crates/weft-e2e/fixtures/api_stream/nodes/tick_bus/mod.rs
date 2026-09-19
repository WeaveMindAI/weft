//! TickBus: a producer standing in for an LLM stream in the streaming e2e.
//! Opens a bus on `ticks`, sends three text deltas the way `LlmStream`
//! sends its fragments (`delta` messages carrying a string), and closes
//! the bus by dropping the guard.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusOptions;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct TickBusNode;

#[async_trait]
impl Node for TickBusNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bus = ctx.open_bus("ticks", BusOptions::default(), "ticker").await?;
        for delta in ["t1", "t2", "t3"] {
            bus.send("delta", json!(delta)).node_err("tick send")?;
        }
        Ok(())
    }
}

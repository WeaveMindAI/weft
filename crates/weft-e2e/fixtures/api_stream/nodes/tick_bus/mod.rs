//! TickBus: a producer standing in for an LLM stream in the streaming e2e.
//! Sends three text deltas on `ticks` the way `LlmStream` sends its
//! fragments (`delta` messages carrying a string), opening the bus with
//! the first one; it is the bus's only member, so the bus ends when the
//! body returns and no node can still join it, failed when the body
//! failed. With `failAt` it
//! fails instead when it reaches that tick: at 0 before the bus opened
//! (a provider refusing the call), later halfway through the stream.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusOptions;
use weft::{RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct TickBusNode;

#[async_trait]
impl Node for TickBusNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let fail_at: Option<f64> = ctx.inputs.opt("failAt")?;
        let mut bus = ctx.pending_bus("ticks", BusOptions::default(), "ticker");
        for (tick, delta) in ["t1", "t2", "t3"].into_iter().enumerate() {
            if fail_at == Some(tick as f64) {
                weft::node_bail!("the stand-in model went away after {tick} tick(s)");
            }
            bus.open().await?.send("delta", json!(delta)).node_err("tick send")?;
        }
        Ok(())
    }
}

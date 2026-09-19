//! QuietBus: a producer that says one thing and then nothing, standing
//! in for a watched table that never changes. Opens a bus on `feed`,
//! sends one text delta, and then waits for ever: the bus stays open
//! and the Stream behind it has nothing to write, which is the shape
//! in which a caller who hung up used to hold its run open for ever.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusOptions;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct QuietBusNode;

#[async_trait]
impl Node for QuietBusNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bus = ctx.open_bus("feed", BusOptions::default(), "quiet").await?;
        bus.send("delta", json!("first")).node_err("quiet send")?;
        std::future::pending::<()>().await;
        Ok(())
    }
}

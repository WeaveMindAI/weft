//! PostgresExecuteQuery: run any SQL against the wired database, with
//! `$1`-style parameters, and emit the answered rows.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::postgres::{connect, query_json};

#[derive(NodeManifest)]
pub struct PostgresExecuteQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for PostgresExecuteQueryNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let params: Vec<Value> = ctx.inputs.list("params")?;

        let conn = ctx.open(&account).await?;
        let client = connect(&ctx, &conn).await?;
        let rows = query_json(&client, &query, &params).await?;
        let count = rows.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("rows", json!(rows)).set("count", count))
            .await
    }
}

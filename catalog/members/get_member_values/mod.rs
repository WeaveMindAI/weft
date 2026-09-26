//! GetMemberValues: what one member gave for `@member_filled` fields
//! (`ctx.values().member(..).get()`), keyed the way SetMemberValues
//! takes them.

use async_trait::async_trait;
use serde_json::{Map, Value};

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::member;

#[derive(NodeManifest)]
pub struct GetMemberValuesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GetMemberValuesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let by_step = ctx.values().member(member(&ctx)?).get().await?;
        let mut values = Map::new();
        for (step, fields) in by_step {
            for (field, value) in fields {
                values.insert(format!("{step}.{field}"), value);
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("values", Value::Object(values))).await
    }
}

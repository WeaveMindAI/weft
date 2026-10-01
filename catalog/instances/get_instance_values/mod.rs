//! GetInstanceValues: what one instance was given for `@instance_filled`
//! fields (`ctx.values().instance(..).get()`), keyed the way
//! SetInstanceValues takes them.

use async_trait::async_trait;
use serde_json::{Map, Value};

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::instance;

#[derive(NodeManifest)]
pub struct GetInstanceValuesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GetInstanceValuesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let by_step = ctx.values().instance(instance(&ctx)?).get().await?;
        let mut values = Map::new();
        for (step, fields) in by_step {
            for (field, value) in fields {
                values.insert(format!("{step}.{field}"), value);
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("values", Value::Object(values))).await
    }
}

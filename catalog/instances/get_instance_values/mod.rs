//! GetInstanceValues: what one instance was given for `@instance_filled`
//! fields (`ctx.values().instance(..).get()`), keyed the way
//! SetInstanceValues takes them.

use async_trait::async_trait;
use serde_json::{Map, Value};

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};


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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let by_node = ctx.values().instance(ctx.inputs.get::<InstanceId>("instance")?).get().await?;
        let mut values = Map::new();
        for (node, fields) in by_node {
            for (field, value) in fields {
                values.insert(format!("{node}.{field}"), value);
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("values", Value::Object(values))).await
    }
}

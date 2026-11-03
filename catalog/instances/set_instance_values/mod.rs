//! SetInstanceValues: give one instance values for `@instance_filled`
//! fields and clear others, in one change (`ctx.values().instance(..)`).

use async_trait::async_trait;
use serde_json::Value;

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{node_bail, RunContext, Node, NodeManifest, WeftResult};


#[derive(NodeManifest)]
pub struct SetInstanceValuesNode;

#[cfg(feature = "node-tests")]
mod tests;

/// `node.field` split at its last dot (the node may be spelled through
/// its call sites: `one.read.spreadsheet`).
pub fn field_of(raw: &str) -> WeftResult<(String, String)> {
    match raw.rsplit_once('.') {
        Some((node, field)) if !node.is_empty() && !field.is_empty() => Ok((node.to_string(), field.to_string())),
        _ => node_bail!("'{raw}' is not `node.field` (`read.spreadsheet`)"),
    }
}

#[async_trait]
impl Node for SetInstanceValuesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let values: serde_json::Map<String, Value> = ctx.inputs.opt("values")?.unwrap_or_default();
        let clear: Vec<String> = ctx.inputs.opt("clear")?.unwrap_or_default();
        if values.is_empty() && clear.is_empty() {
            node_bail!("nothing to change: give `values`, `clear`, or both");
        }
        let mut change = ctx.values().instance(ctx.inputs.get::<InstanceId>("instance")?);
        for (target, value) in values {
            let (node, field) = field_of(&target)?;
            change = change.set(node, field, value);
        }
        for target in clear {
            let (node, field) = field_of(&target)?;
            change = change.clear(node, field);
        }
        let rearmed = change.apply().await?;
        ctx.pulse_downstream(NodeOutput::new().set("rearmed", rearmed)).await
    }
}

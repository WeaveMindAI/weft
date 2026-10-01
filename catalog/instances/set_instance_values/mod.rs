//! SetInstanceValues: give one instance values for `@instance_filled`
//! fields and clear others, in one change (`ctx.values().instance(..)`).

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::instance;

#[derive(NodeManifest)]
pub struct SetInstanceValuesNode;

#[cfg(feature = "node-tests")]
mod tests;

/// `node.field` split at its last dot (the node may be spelled through
/// its call sites: `one.read.spreadsheet`).
pub fn field_of(raw: &str) -> WeftResult<(String, String)> {
    match raw.rsplit_once('.') {
        Some((step, field)) if !step.is_empty() && !field.is_empty() => Ok((step.to_string(), field.to_string())),
        _ => node_bail!("'{raw}' is not `node.field` (`read.spreadsheet`)"),
    }
}

#[async_trait]
impl Node for SetInstanceValuesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let values: serde_json::Map<String, Value> = ctx.inputs.opt("values")?.unwrap_or_default();
        let clear: Vec<String> = ctx.inputs.opt("clear")?.unwrap_or_default();
        if values.is_empty() && clear.is_empty() {
            node_bail!("nothing to change: give `values`, `clear`, or both");
        }
        let mut change = ctx.values().instance(instance(&ctx)?);
        for (target, value) in values {
            let (step, field) = field_of(&target)?;
            change = change.set(step, field, value);
        }
        for target in clear {
            let (step, field) = field_of(&target)?;
            change = change.clear(step, field);
        }
        let rearmed = change.apply().await?;
        ctx.pulse_downstream(NodeOutput::new().set("rearmed", rearmed)).await
    }
}

//! Multiply: a project-local custom node. Multiplies two Number inputs and
//! emits the product. Exists to prove the e2e rig can compile a user-authored
//! node into the worker binary and run it.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct MultiplyNode;

#[async_trait]
impl Node for MultiplyNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Both inputs are required, so the engine only fires once they are
        // present; read them loudly (`get` errors on absent/wrong type).
        let a: f64 = ctx.input.get("a")?;
        let b: f64 = ctx.input.get("b")?;
        let product = a * b;
        ctx.pulse_downstream(
            NodeOutput::empty().set("product", Value::from(product)),
        )
        .await
    }
}

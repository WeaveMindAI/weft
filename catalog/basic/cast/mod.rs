//! Cast: convert a value into the type declared on the output.
//!
//! The target type is whatever the weft source declared on the `value`
//! output (the metadata ships it as `MustOverride`, so the compiler
//! demands a concrete declaration). The conversion itself is
//! `WeftType::cast_value`: the same rules the compiler's literal
//! lenience applies, so text parses into scalars and JSON structures,
//! scalars stringify, and an object claiming a declared
//! custom type is validated against its structure with a field-level
//! error when it does not fit. Impossible pairs never reach this body
//! (the compiler's `cast-not-allowed` check refuses them).

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CastNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CastNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: serde_json::Value = ctx.inputs.get("value")?;
        // The metadata ships MustOverride and the compiler refuses an
        // un-overridden one, so the type is always resolved here.
        let target = ctx.output_type("value")?;
        match target.cast_value(&value) {
            Ok(cast) => ctx.pulse_downstream(NodeOutput::new().set("value", cast)).await,
            Err(e) => node_bail!("cannot cast the value into {target}: {e}"),
        }
    }
}

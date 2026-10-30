//! Cast: convert a value into the type declared on the output.
//!
//! The target type is the `value` output's: a type variable of its own
//! that the program pins, by declaring it (`-> (value: Signup)`) or by
//! wiring the output into a port of a concrete type, which the
//! compiler works it out from (a `value` it cannot pin is its
//! `unresolved-typevar`). The conversion itself is
//! `WeftType::cast_requested`: the rules the compiler's literal
//! lenience applies, so text parses into scalars and JSON structures and
//! scalars stringify, plus the one a written value never gets (a list
//! or an object into text is its JSON), and an object claiming a declared
//! custom type is validated against its structure with a field-level
//! error when it does not fit. Impossible pairs never reach this body
//! (the compiler's `cast-not-allowed` check refuses them).

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{node_bail, RunContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let value: serde_json::Value = ctx.inputs.get("value")?;
        // The compiler refuses a connected output whose variable nothing
        // pinned, so the type is resolved here.
        let target = ctx.output_type("value")?;
        match target.cast_requested(&value) {
            Ok(cast) => ctx.pulse_downstream(NodeOutput::new().set("value", cast)).await,
            Err(e) => node_bail!("cannot cast the value into {target}: {e}"),
        }
    }
}

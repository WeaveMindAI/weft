//! ProfileGreet: a project-local custom node consuming the user-declared
//! `Profile` named type (declared in this node's metadata). Exists to prove
//! a value cast into a custom type flows into a typed node input and is
//! readable as plain structured data.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ProfileGreetNode;

#[async_trait]
impl Node for ProfileGreetNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let profile: serde_json::Value = ctx.inputs.get("profile")?;
        let Some(name) = profile["name"].as_str() else {
            node_bail!("profile has no name field");
        };
        let Some(age) = profile["age"].as_f64() else {
            node_bail!("profile has no age field");
        };
        let greeting = format!("Hello {name} ({age})");
        ctx.pulse_downstream(NodeOutput::new().set("greeting", greeting)).await
    }
}

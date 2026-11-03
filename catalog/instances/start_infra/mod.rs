//! StartInfra: bring up a copy of an infra node, the program's own or,
//! given `instance`, that instance's copy of a `@per_instance` node, and
//! fire `done` once the copy answers, or, with `waitUntilRunning` off,
//! once weft accepted the start. The node keeps no state of its own: a
//! body run again after a pause between looks at the copy makes the
//! same start call, which carries on from where it paused.

use async_trait::async_trait;

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};


#[derive(NodeManifest)]
pub struct StartInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StartInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let wait_until_running: bool = ctx.inputs.get("waitUntilRunning")?;
        let mut copy = ctx.infra(node);
        if let Some(id) = ctx.inputs.opt::<InstanceId>("instance")? {
            copy = copy.instance(id);
        }
        let mut state = ctx.keep(()).await?;
        if wait_until_running {
            copy.start(&mut state).await?;
        } else {
            copy.request_start(&mut state).await?;
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}

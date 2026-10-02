//! AwaitJob: wait for an outside job to finish without holding a worker.
//!
//! Parks on a `PollEndpoint` over the job's status address. The listener
//! polls it, the first poll at once, and the first answer whose `status`
//! reads COMPLETED or FAILED resumes the run as the value `await_signal`
//! returns. The node emits the job's `result`, or fails with its `error`.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct AwaitJobNode;

#[async_trait]
impl Node for AwaitJobNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        let answer = ctx
            .await_signal(PollEndpoint {
                url,
                interval_secs: 5,
                filters: vec![Predicate::regex("status", "^(COMPLETED|FAILED)$")],
                ..Default::default()
            })
            .await?;
        if answer["status"] == "FAILED" {
            return Err(weft::node_error(format!("the job failed: {}", answer["error"])));
        }
        let result = answer["result"]
            .as_str()
            .ok_or_else(|| weft::node_error(format!("the finished job carries no result: {answer}")))?
            .to_string();
        ctx.pulse_downstream(NodeOutput::new().set("result", result)).await
    }
}

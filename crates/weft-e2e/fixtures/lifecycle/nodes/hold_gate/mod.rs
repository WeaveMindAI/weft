//! HoldGate: hold the execution open until a release URL says go.
//!
//! Polls the configured URL until its body contains `release`, then emits
//! `done`. Exists so the e2e rig has a REAL running execution it can hold
//! open and release on cue: the rig starts a run, watches it sit in
//! `running`, drives lifecycle verbs against it (drain-gated infra ops,
//! deactivate wait/cancel, resume), and flips the fake's body to let it
//! finish. Cancellation needs nothing special: the engine drops this future,
//! which aborts the in-flight poll.

use std::time::Duration;

use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct HoldGateNode;

const POLL_INTERVAL: Duration = Duration::from_millis(300);

#[async_trait]
impl Node for HoldGateNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        let client = ctx.http();
        loop {
            // Transient errors (fake not yet up, connection blip) just poll
            // again: the gate's contract is "held until released", and the
            // rig's release is the only exit. A genuinely dead fake shows up
            // as the TEST timing out on the run, loudly.
            if let Ok(resp) = client.get(&url).send().await {
                if let Ok(body) = resp.text().await {
                    if body.contains("release") {
                        break;
                    }
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", "released")).await
    }
}

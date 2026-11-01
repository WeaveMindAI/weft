//! What the computer's action nodes share: the one call that asks the
//! computer on a node's `computer` input to do something, and the
//! screenshot it may answer with.

use base64::Engine;
use serde_json::Value;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::storage::{StorageScope};
use weft::{EndpointHandle, NodeErrExt, RunContext, WeftResult};

// SYNC: computer actions and routes <-> catalog/computer/computer/images/computer/computer.py
//       (ACTIONS, JOBS_PREFIX)
pub const SCREENSHOT: &str = "screenshot";
pub const ZOOM: &str = "zoom";
pub const CLICK: &str = "click";
pub const DRAG: &str = "drag";
pub const MOVE: &str = "move";
pub const MOUSE: &str = "mouse";
pub const CURSOR: &str = "cursor";
pub const SCROLL: &str = "scroll";
pub const TYPE: &str = "type";
pub const KEY: &str = "key";
pub const EXEC: &str = "exec";
pub const OUTPUT: &str = "output";
pub const END_SESSION: &str = "end_session";
pub const JOBS_PATH: &str = "/jobs/";

/// The computer on the node's `computer` input, reached.
pub async fn endpoint(ctx: &RunContext) -> WeftResult<EndpointHandle> {
    let computer: InfraHandle = ctx.inputs.get("computer")?;
    ctx.endpoint_of(&computer).await
}

/// Ask the computer on the node's `computer` input to do `action`, and
/// hand back what it answered. An action that did not fully happen on
/// the computer (a key name it does not know, a point off the screen)
/// fails the node with the computer's own words. A press of stop ends
/// the call: an action can take a while (a key held down for minutes).
pub async fn act(ctx: &RunContext, action: &str, payload: Value) -> WeftResult<Value> {
    let computer = endpoint(ctx).await?;
    let cancel = ctx.cancellation();
    tokio::select! {
        answer = computer.action(action, payload) => answer,
        err = cancel.cancelled_err() => Err(err),
    }
}

/// Do `action`, then send `result` on the node's `result` output: the
/// text a model reads back for an action that answers nothing more.
pub async fn act_and_say(ctx: &RunContext, action: &str, payload: Value, result: &str) -> WeftResult<()> {
    act(ctx, action, payload).await?;
    ctx.pulse_downstream(NodeOutput::new().set("result", result)).await
}

/// The image in the computer's answer (`png`, base64), stored for the run
/// like any image a node makes: a conversation that shows it to a model
/// holds a reference to it, which reads for as long as the run is kept.
pub async fn store_png(ctx: &RunContext, answer: &Value, filename: &str) -> WeftResult<Value> {
    let encoded = answer["png"].as_str().node_err(format!("the computer answered no image: {answer}"))?;
    let png = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .node_err("the computer's image is not base64")?;
    ctx.storage(StorageScope::Run).put(png, "image/png", filename, None).await
}

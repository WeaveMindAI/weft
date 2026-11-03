//! RunCommand: run a command in one of the computer's shell sessions,
//! and hand back its whole output as a stored file.
//!
//! A session keeps its folder and its exported variables from one
//! command to the next. A quick command answers in the same call. A
//! longer one answers with its id at once, and the node parks on the
//! command's status route (`PollEndpoint`), so no worker is held while
//! it runs. The node's state holds the computer's answer to the start
//! and, once it ended, the command's end, so the body run again after
//! the wait does not run the command a second time and comes straight
//! back to the wait on the same command. Once it ended, its output is
//! read off the computer in pieces, straight into storage, so an output
//! of any size never sits whole in memory. Cutting it down for a reader
//! (a model) is a later node's job (`TextPreview`), never this one's.

use async_trait::async_trait;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use weft::access::client;
use weft::node::NodeOutput;
use weft::signal::poll_endpoint::MIN_POLL_INTERVAL_SECS;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::StorageScope;
use weft::{node_bail, EndpointHandle, Node, NodeErrExt, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct RunCommandNode;

/// Where the command stands: the computer's answer to the start (a
/// finished command, or a running one with its id), then, for one that
/// was still running, the answer that ended the wait on it.
#[derive(Default, Serialize, Deserialize)]
struct Command {
    started: Option<Value>,
    ended: Option<Value>,
}

#[cfg(feature = "node-tests")]
mod tests;

// SYNC: job answer <-> catalog/computer/computer/images/computer/computer.py (RUNNING, DONE, LOST, Job.answer, Job.read)
const RUNNING: &str = "running";
const DONE: &str = "done";
const LOST: &str = "lost";

/// What the output file holds when the call only restarted the session.
pub const RESTARTED: &str = "The session was restarted.";

/// How a finished command ended, from the computer's answer: its exit
/// code and how many bytes of output it wrote. A command that is no
/// longer on the computer (it restarted while the node waited) fails the
/// node with the computer's words.
pub fn finished(answer: &Value) -> WeftResult<(i64, u64)> {
    match answer["status"].as_str() {
        Some(DONE) => {
            let code = answer["exitCode"].as_i64().node_err(format!("the computer answered a command with no exit code: {answer}"))?;
            let size = answer["size"].as_u64().node_err(format!("the computer answered a command with no output size: {answer}"))?;
            Ok((code, size))
        }
        Some(LOST) => {
            let why = answer["error"].as_str().node_err(format!("the computer lost the command and said nothing more: {answer}"))?;
            node_bail!("{why}")
        }
        _ => node_bail!("the computer answered a command with something that is not a finished command: {answer}"),
    }
}

/// One piece of a command's output, as the computer hands it over: the
/// bytes, where the next piece starts, and whether this was the last.
pub fn piece(answer: &Value) -> WeftResult<(Vec<u8>, u64, bool)> {
    let data = answer["data"].as_str().node_err(format!("the computer answered a piece of output with no data: {answer}"))?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(data).node_err("the computer's piece of output is not base64")?;
    let next = answer["next"].as_u64().node_err(format!("the computer answered a piece of output with no next offset: {answer}"))?;
    let end = answer["end"].as_bool().node_err(format!("the computer answered a piece of output without saying whether it was the last: {answer}"))?;
    Ok((bytes, next, end))
}

/// The output of command `id`, read off the computer piece by piece as
/// storage takes it.
fn output_stream(computer: EndpointHandle, id: String, size: u64) -> weft::ByteStream {
    let pieces = futures::stream::try_unfold(Some(0u64), move |from| {
        let (computer, id) = (computer.clone(), id.clone());
        async move {
            let Some(from) = from else { return Ok(None) };
            let answer = computer
                .action(actions::OUTPUT, json!({ "id": id, "from": from }))
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let (bytes, next, end) = piece(&answer).map_err(|e| std::io::Error::other(e.to_string()))?;
            if next != from + bytes.len() as u64 || next > size {
                return Err(std::io::Error::other(format!(
                    "the computer handed over the output of command {id} out of order: from {from}, {} bytes, next {next}, of {size}",
                    bytes.len()
                )));
            }
            Ok(Some((bytes::Bytes::from(bytes), (!end).then_some(next))))
        }
    });
    Box::pin(pieces)
}

#[async_trait]
impl Node for RunCommandNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let mut payload: Map<String, Value> = ctx.inputs.get("args")?;
        if let Some(session) = ctx.inputs.opt::<String>("session")? {
            payload.insert("session".into(), json!(session));
        }
        let mut state = ctx.keep(Command::default()).await?;
        let started = state
            .once(|s| &mut s.started, async |_| actions::act(&ctx, actions::EXEC, Value::Object(payload)).await)
            .await?;
        let storage = ctx.storage(StorageScope::Run);
        if started["restarted"] == true {
            let output = storage.put(RESTARTED.as_bytes().to_vec(), "text/plain", "output.txt", None).await?;
            return ctx.pulse_downstream(NodeOutput::new().set("output", output).set("exitCode", 0)).await;
        }
        let computer = actions::endpoint(&ctx).await?;
        let answer = if started["status"] == RUNNING {
            let id = started["id"].as_str().node_err(format!("the computer answered a running command with no id: {started}"))?;
            // As often as a wait may look: a model waiting on the command
            // should get it back as soon as it ends.
            let status = PollEndpoint {
                url: client::url(computer.url(), &[actions::JOBS, id])?.into(),
                interval_secs: MIN_POLL_INTERVAL_SECS,
                filters: vec![Predicate::neq("status", RUNNING)],
                ..Default::default()
            };
            state.wait(|s| &mut s.ended, status).await?
        } else {
            started
        };
        let (exit_code, size) = finished(&answer)?;
        let id = answer["id"].as_str().node_err(format!("the computer answered a finished command with no id: {answer}"))?;
        let output = storage.put_stream(output_stream(computer, id.to_string(), size), "text/plain", "output.txt", None).await?;
        ctx.pulse_downstream(NodeOutput::new().set("output", output).set("exitCode", exit_code)).await
    }
}

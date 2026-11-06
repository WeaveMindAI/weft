//! RunCommand: run a command in one of the computer's shell sessions,
//! and hand back its whole output as a stored file.
//!
//! A session keeps its folder and its exported variables from one
//! command to the next. The command runs through the computer's shared
//! `command::run`: a quick one answers in the same call, a longer one
//! parks the node on its status route without holding a worker, and the
//! body run again after the wait does not run it a second time. Once it
//! ended, its output is read off the computer in pieces, straight into
//! storage, so an output of any size never sits whole in memory. Cutting
//! it down for a reader (a model) is a later node's job (`TextPreview`),
//! never this one's.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{Node, NodeManifest, RunContext, WeftResult};

use super::actions;
use super::command::{self, Command, Ran};

#[derive(NodeManifest)]
pub struct RunCommandNode;

#[cfg(feature = "node-tests")]
mod tests;

/// What the output file holds when the call only restarted the session.
pub const RESTARTED: &str = "The session was restarted.";

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
        let computer = actions::endpoint(&ctx).await?;
        let storage = ctx.storage(StorageScope::Run);
        let (output, exit_code) = match command::run(&computer, &mut state, |s| s, Value::Object(payload)).await? {
            Ran::Restarted => (storage.put(RESTARTED.as_bytes().to_vec(), "text/plain", "output.txt", None).await?, 0),
            Ran::Finished(finished) => {
                let output = storage.put_stream(command::output(computer, &finished), "text/plain", "output.txt", None).await?;
                (output, finished.exit_code)
            }
        };
        ctx.pulse_downstream(NodeOutput::new().set("output", output).set("exitCode", exit_code)).await
    }
}

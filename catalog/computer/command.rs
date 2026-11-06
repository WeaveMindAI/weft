//! Running a shell command on the computer and reading back what it
//! printed: what `RunCommand` does, for any node that runs a command on
//! a computer it is handed (a package using this one calls it as
//! `computer::command`).
//!
//! A quick command answers in the same call. A longer one answers with
//! its id at once, and the node parks on the command's status route
//! (`PollEndpoint`), so no worker is held while it runs. The node's state
//! holds the computer's answer to the start and, once it ended, the
//! command's end, so the body run again after the wait does not run the
//! command a second time and comes straight back to the wait on the same
//! command. Once it ended, its output is read off the computer in pieces.

use base64::Engine;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use weft::access::client;
use weft::signal::poll_endpoint::MIN_POLL_INTERVAL_SECS;
use weft::signal::{PollEndpoint, Predicate};
use weft::{node_bail, EndpointHandle, Kept, NodeErrExt, WeftResult};

use super::actions;

// SYNC: job answer <-> catalog/computer/computer/images/computer/computer.py (RUNNING, DONE, LOST, Job.answer, Job.read)
const RUNNING: &str = "running";
const DONE: &str = "done";
const LOST: &str = "lost";

/// Where a command stands, for the state of the node running it: the
/// computer's answer to the start (a finished command, or a running one
/// with its id), then, for one that was still running, the answer that
/// ended the wait on it. Empty until the command is started.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Command {
    started: Option<Value>,
    ended: Option<Value>,
}

/// How a command came back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ran {
    /// The call only restarted the session (`restart: true` and no
    /// command): nothing ran.
    Restarted,
    Finished(Finished),
}

/// A command that ended on the computer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finished {
    pub id: String,
    pub exit_code: i64,
    /// How many bytes of output it wrote.
    pub size: u64,
}

/// Run the computer's `exec` with `payload` (`command`, and optionally
/// `session`, `restart`, `timeoutSecs`) and come back once the command
/// ended, however long it runs (a person may be waiting on it: no
/// deadline). `command` is where the node's state keeps the
/// [`Command`]: a body run again after the wait does not start the
/// command again, and comes back to the wait on the same one. A command
/// the computer no longer has (it restarted while the node waited) fails
/// the node with the computer's words.
pub async fn run<S>(
    computer: &EndpointHandle,
    state: &mut Kept<S>,
    command: impl Fn(&mut S) -> &mut Command + Copy,
    payload: Value,
) -> WeftResult<Ran>
where
    S: Serialize + DeserializeOwned + Send,
{
    let started = state
        .once(move |s| &mut command(s).started, async |_| computer.action(actions::EXEC, payload).await)
        .await?;
    if started["restarted"] == true {
        return Ok(Ran::Restarted);
    }
    let answer = if started["status"] == RUNNING {
        let id = started["id"].as_str().node_err(format!("the computer answered a running command with no id: {started}"))?;
        // As often as a wait may look: whoever waits on the command
        // (a model, a person) should get it back as soon as it ends.
        let status = PollEndpoint {
            url: client::url(computer.url(), &[actions::JOBS, id])?.into(),
            interval_secs: MIN_POLL_INTERVAL_SECS,
            filters: vec![Predicate::neq("status", RUNNING)],
            ..Default::default()
        };
        state.wait(move |s| &mut command(s).ended, status).await?
    } else {
        started
    };
    finished(&answer).map(Ran::Finished)
}

/// How a finished command ended, from the computer's answer. A command
/// that is no longer on the computer fails the node with the computer's
/// words.
pub fn finished(answer: &Value) -> WeftResult<Finished> {
    match answer["status"].as_str() {
        Some(DONE) => {
            let id = answer["id"].as_str().node_err(format!("the computer answered a finished command with no id: {answer}"))?;
            let exit_code = answer["exitCode"].as_i64().node_err(format!("the computer answered a command with no exit code: {answer}"))?;
            let size = answer["size"].as_u64().node_err(format!("the computer answered a command with no output size: {answer}"))?;
            Ok(Finished { id: id.to_string(), exit_code, size })
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

/// The piece of the finished command's output from `from`, checked to
/// follow on from what came before, with where the next one starts
/// (`None` after the last).
async fn read_piece(computer: &EndpointHandle, finished: &Finished, from: u64) -> WeftResult<(Vec<u8>, Option<u64>)> {
    let (id, size) = (&finished.id, finished.size);
    let (bytes, next, end) = piece(&computer.action(actions::OUTPUT, json!({ "id": id, "from": from })).await?)?;
    if next != from + bytes.len() as u64 || next > size {
        node_bail!(
            "the computer handed over the output of command {id} out of order: from {from}, {} bytes, next {next}, of {size}",
            bytes.len()
        );
    }
    Ok((bytes, (!end).then_some(next)))
}

/// The output of the finished command, read off the computer piece by
/// piece as whoever reads the stream takes it (storage, for one of any
/// size), so it never sits whole in memory.
pub fn output(computer: EndpointHandle, finished: &Finished) -> weft::ByteStream {
    let finished = finished.clone();
    let pieces = futures::stream::try_unfold(Some(0u64), move |from| {
        let (computer, finished) = (computer.clone(), finished.clone());
        async move {
            let Some(from) = from else { return Ok(None) };
            let (bytes, next) = read_piece(&computer, &finished, from).await.map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok(Some((bytes::Bytes::from(bytes), next)))
        }
    });
    Box::pin(pieces)
}

/// The whole output of the finished command as text, trailing white
/// space cut. Only for a command whose output is short by construction
/// (a script that prints nothing but why it failed): the whole of it
/// sits in memory.
pub async fn output_text(computer: &EndpointHandle, finished: &Finished) -> WeftResult<String> {
    let mut printed = Vec::new();
    let mut from = Some(0u64);
    while let Some(at) = from {
        let (bytes, next) = read_piece(computer, finished, at).await?;
        printed.extend(bytes);
        from = next;
    }
    Ok(String::from_utf8_lossy(&printed).trim_end().to_string())
}

//! AskAgent: one step of a conversation with a coding agent.
//!
//! A firing hands the agent ONE thing, a message, the result of a tool
//! call it made, or the answer to a question it asked, and comes back with
//! whatever ends this step first: the agent's reply (with the whole
//! conversation as a ChatHistory file), a call of one of the program's own
//! tools, or a question for a person. In the last two cases the agent is
//! still inside its turn, blocked on that call in its server, and the next
//! firing's `toolResult` or `answer` carries the turn on.
//!
//! Everything the agent does meanwhile goes out on `stream` (a bus the
//! agent node opened and stays on, which outlives every firing of this
//! node: each firing joins it, sends its events and leaves as it returns,
//! and the bus ends when a CloseBus ends it). The
//! node reads the server's numbered events live while they come; once the
//! agent goes quiet it waits on them without holding a worker, and its
//! state (the turn and how far it read) is all it needs to read on from
//! where it stopped after a pause, so no event is lost and none is sent on
//! the bus twice.
//!
//! Every turn goes out with the firing's key (`ctx.firing_key()`), the same
//! when the body runs again after a crash between the post and the save
//! of its answer: the server answers a key it has seen with the turn it
//! started for it and starts nothing, so one firing starts at most one turn.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::signal::poll_endpoint::MIN_POLL_INTERVAL_SECS;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::StorageScope;
use weft::{node_bail, EndpointMethod, InfraHandle, Node, NodeErrExt, NodeManifest, RunContext, WeftResult};

use super::agent::{EVENTS_PATH, HISTORY_PATH, LIVE_WAIT_SECS, LOGIN_EVENT, TURNS_PATH};

#[derive(NodeManifest)]
pub struct AskAgentNode;

#[cfg(feature = "node-tests")]
mod tests;

/// Where this firing stands: the turn it is part of, and the number of the
/// last of its events it sent on the bus.
#[derive(Default, Serialize, Deserialize)]
struct Asking {
    turn: Option<String>,
    after: u64,
    /// The answer that ended the last wait on a quiet agent, kept until the
    /// body reads on past it.
    woke: Option<Value>,
}

// The statuses that end a read; every other one (WORKING, WAITING_TOOL,
// WAITING_QUESTION, QUEUED) reads as still working.
// SYNC: turn statuses <-> catalog/agents/server/events.py (DONE, FAILED, LOST)
const DONE: &str = "done";
const FAILED: &str = "failed";
const LOST: &str = "lost";
// SYNC: pending event types <-> catalog/agents/server/events.py (TOOL_CALL, QUESTION)
const TOOL_CALL: &str = "tool_call";
const QUESTION: &str = "question";

/// What the server is asked for: exactly one of a message, a tool result
/// and an answer, from the inputs that arrived, under `key`, the firing's
/// own (`ctx.firing_key()`), which a body run again sends again.
// SYNC: POST /turns `key` <-> catalog/agents/server/agent_server.py
pub fn turn_request(inputs: &Map<String, Value>, key: &str) -> WeftResult<Value> {
    let given: Vec<&str> = ["message", "toolResult", "answer"].into_iter().filter(|k| inputs.get(*k).is_some_and(|v| !v.is_null())).collect();
    let [one] = given.as_slice() else {
        node_bail!(
            "AskAgent takes exactly one of `message`, `toolResult` and `answer` per firing, and this one got {}",
            if given.is_empty() { "none".to_string() } else { given.join(" and ") }
        )
    };
    Ok(match *one {
        "message" => json!({
            "message": inputs["message"],
            "tools": inputs.get("tools").filter(|t| !t.is_null()).cloned().unwrap_or(json!([])),
            "startOver": inputs.get("startOver").and_then(Value::as_bool).unwrap_or(false),
            "key": key,
        }),
        other => {
            if inputs.get("startOver").and_then(Value::as_bool) == Some(true) {
                node_bail!("`startOver` starts a new conversation with a `message`; it cannot come with the `{other}` of a turn already running");
            }
            json!({ other: inputs[other], "key": key })
        }
    })
}

/// What one read of a turn ended on.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// The agent replied: the turn is over.
    Reply(String),
    /// It calls one of the program's tools and waits for the result.
    ToolCall(Value),
    /// It asks a question (its own ask tool, or a permission) and waits.
    Question(Value),
    /// The turn failed, or was lost with its server: said in the agent's words.
    Failed(String),
    /// Still working (or queued behind another run's turn).
    Working,
}

/// What a read of the server's `/events` answered: what the node acts on.
pub fn outcome(answer: &Value) -> WeftResult<Outcome> {
    let status = answer["status"].as_str().node_err(format!("the agent server answered a turn with no status: {answer}"))?;
    Ok(match status {
        DONE => Outcome::Reply(answer["reply"].as_str().unwrap_or_default().to_string()),
        FAILED | LOST => Outcome::Failed(answer["error"].as_str().unwrap_or("the agent server gave no reason").to_string()),
        _ => match answer.get("pending").filter(|p| !p.is_null()) {
            Some(pending) if pending["type"] == TOOL_CALL => Outcome::ToolCall(json!({
                "id": pending["callId"],
                "name": pending["name"],
                "arguments": pending["arguments"],
            })),
            Some(pending) if pending["type"] == QUESTION => Outcome::Question(pending["question"].clone()),
            Some(pending) => node_bail!("the agent server waits on something AskAgent does not know: {pending}"),
            None => Outcome::Working,
        },
    })
}

/// The path of a read of `turn`'s events after event `after`, held open
/// up to `wait` seconds.
pub fn events_path(turn: &str, after: u64, wait: u64) -> WeftResult<String> {
    Ok(weft::access::client::url("", &[EVENTS_PATH])?.query("turn", turn).query("after", after).query("wait", wait).into())
}

// SYNC: QUESTION_PATH <-> catalog/agents/server/agent_server.py (QUESTION_PATH)
const QUESTION_PATH: &str = "question";

/// Where the agent server at `server` says whether its agent still waits
/// on `question`: the question's `statusUrl`, which AnswerAgent's form
/// waits on beside the person, so a question the agent dropped stops
/// being asked. The question names the address because only the firing
/// that hands it out knows which server asked it.
pub fn status_url(server: &str, question: &Value) -> WeftResult<String> {
    let id = question["id"].as_str().node_err(format!("the agent server sent a question with no id: {question}"))?;
    Ok(weft::access::client::url(server, &[QUESTION_PATH])?.query("id", id).into())
}

/// The path of the conversation as it stood when `turn` ended: never the
/// conversation now, which may already hold the next queued turn's message.
pub fn history_path(turn: &str) -> WeftResult<String> {
    Ok(weft::access::client::url("", &[HISTORY_PATH])?.query("turn", turn).into())
}

#[async_trait]
impl Node for AskAgentNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let request = turn_request(ctx.inputs.object()?, &ctx.firing_key())?;
        let agent: InfraHandle = ctx.inputs.get("agent")?;
        let server = ctx.endpoint_of(&agent).await?;
        let stream = match ctx.inputs.raw("stream") {
            Some(marker) if !marker.is_null() => Some(ctx.join_bus("stream", &turn_name(&ctx))?),
            _ => None,
        };
        let mut state = ctx.keep(Asking::default()).await?;
        let started = state
            .once(|s| &mut s.turn, async |st| {
                let answer = server.call(EndpointMethod::Post, TURNS_PATH, Some(request)).await?;
                // The events up to the call this firing answers went out
                // with the firing that handed the call out.
                st.after = answer["after"].as_u64().node_err(format!("the agent server took the turn and said nothing of where to read from: {answer}"))?;
                answer["turn"].as_str().map(str::to_string).node_err(format!("the agent server took the turn and named none: {answer}"))
            })
            .await?;
        loop {
            let answer = server.call(EndpointMethod::Get, &events_path(&started, state.after, LIVE_WAIT_SECS)?, None).await?;
            // Our own server's answer, so a read with no `events` or an
            // event with no number or type is its bug, refused whether or
            // not a `stream` is wired to carry the events on.
            let events = answer["events"].as_array().node_err(format!("the agent server answered a read of the turn with no events: {answer}"))?.clone();
            for event in &events {
                let n = event["n"].as_u64().node_err(format!("the agent server sent an event with no number: {event}"))?;
                let kind = event["type"].as_str().node_err(format!("the agent server sent an event with no type: {event}"))?;
                if n <= state.after {
                    continue;
                }
                match &stream {
                    Some(bus) => bus.send(kind.to_string(), event.clone()).node_err("sending the agent's event on `stream`")?,
                    // The agent node hands a fresh login to the agent that
                    // asks, and it hears the ask on `stream` alone.
                    None if kind == LOGIN_EVENT => node_bail!(
                        "the agent asked for a fresh login, which its agent node hands it when it hears the ask on \
                         `stream`, and this AskAgent's `stream` is not wired; wire the agent node's `stream` into it"
                    ),
                    None => {}
                }
                state.after = n;
            }
            if !events.is_empty() {
                state.save().await?;
            }
            match outcome(&answer)? {
                Outcome::Reply(reply) => {
                    let history = server.call(EndpointMethod::Get, &history_path(&started)?, None).await?;
                    let messages = history["messages"].clone();
                    let bytes = serde_json::to_vec(&messages).node_err("writing the conversation")?;
                    let file = ctx.storage(StorageScope::Run).put(bytes, "application/json", "conversation.json", None).await?;
                    return ctx.pulse_downstream(NodeOutput::new().set("reply", reply).set("history", file)).await;
                }
                Outcome::ToolCall(call) => return ctx.pulse_downstream(NodeOutput::new().set("toolCall", call)).await,
                Outcome::Question(mut question) => {
                    question["statusUrl"] = json!(status_url(server.url(), &question)?);
                    return ctx.pulse_downstream(NodeOutput::new().set("question", question)).await;
                }
                Outcome::Failed(why) => node_bail!("{why}"),
                Outcome::Working if events.is_empty() => {
                    // Quiet: wait for the next event without holding a worker.
                    let news = PollEndpoint {
                        url: format!("{}{}", server.url().trim_end_matches('/'), events_path(&started, state.after, 0)?),
                        interval_secs: MIN_POLL_INTERVAL_SECS,
                        filters: vec![Predicate::eq("news", "true")],
                        ..Default::default()
                    };
                    state.wait(|s| &mut s.woke, news).await?;
                    state.woke = None;
                }
                Outcome::Working => {}
            }
        }
    }
}

/// The name this firing joins the agent's stream under: its node, and the
/// iteration it is in when it is inside a loop (`ask-turn#3`). A name is
/// held by one firing at a time, and two firings of one AskAgent can be on
/// the same stream at once: two iterations of a parallel loop, or a
/// sequential loop whose next turn starts while this one is still
/// finishing.
fn turn_name(ctx: &RunContext) -> String {
    let at = weft::frames::frames_text(&ctx.frames);
    if at.is_empty() { format!("{}-turn", ctx.node_id) } else { format!("{}-turn#{at}", ctx.node_id) }
}

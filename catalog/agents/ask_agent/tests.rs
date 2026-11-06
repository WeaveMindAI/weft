//! AskAgent against a fake agent server: the server's answers are queued
//! in order, and the calls the node made are read back. A message comes
//! back as a reply with the conversation, a tool call and a question come
//! out and the next firing carries the turn on, a quiet agent makes the
//! node wait without holding a worker and read on from its saved place
//! (no event lost, none sent twice), a lost turn fails saying so, the
//! inputs are held to exactly one of message, tool result and answer, and
//! a body run again after a crash between the post and its save posts the
//! turn again under the same key, so the server starts it once.

use serde_json::{json, Value};

use weft::bus::BusOptions;
use weft::context::HarnessRead;
use weft::signal::Predicate;
use weft::storage::StoredFile;
use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::{events_path, history_path, outcome, turn_request, AskAgentNode, Outcome};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("a_firing_takes_exactly_one_of_message_tool_result_and_answer", one_of_three),
        NodeTest::basic("a_read_of_the_server_says_what_the_node_does_next", reads),
        NodeTest::basic("a_read_names_its_turn_and_place", paths),
        NodeTest::fake("a_message_comes_back_as_the_reply_with_the_conversation", replies),
        NodeTest::fake("a_tool_call_comes_out_and_its_result_carries_the_turn_on", tool_call),
        NodeTest::fake("a_question_comes_out_and_the_answer_carries_the_turn_on", question),
        NodeTest::fake("a_quiet_agent_parks_the_run_and_it_reads_on_from_its_place", quiet),
        NodeTest::fake("a_lost_turn_fails_saying_to_send_it_again", lost),
        NodeTest::fake("an_event_with_no_type_fails_loud", untyped),
        NodeTest::fake("an_event_with_no_type_fails_loud_with_no_stream_wired", untyped_unstreamed),
        NodeTest::fake("a_read_with_no_events_fails_loud", no_events),
        NodeTest::fake("a_turn_posted_again_after_a_crash_carries_the_same_key", crash_after_post),
    ]
}

const AGENT: &str = "http://agent.example:9000";

fn agent(rig: &FakeRig) -> Value {
    rig.declare_infra("claude", "api", AGENT)
}

fn answers(rig: &FakeRig, method: EndpointMethod, path: &str, answer: Value) {
    rig.answer_infra("claude", "api", method, path, answer);
}

fn read(turn: &str, after: u64) -> String {
    events_path(turn, after, super::LIVE_WAIT_SECS).expect("a plain turn id makes a path")
}

fn event(n: u64, kind: &str, more: Value) -> Value {
    let mut e = json!({ "n": n, "turn": "t1", "type": kind });
    e.as_object_mut().unwrap().extend(more.as_object().unwrap().clone());
    e
}

fn status(status: &str, events: Vec<Value>, pending: Value) -> Value {
    json!({ "turn": "t1", "status": status, "events": events, "pending": pending, "error": null, "reply": null, "news": true })
}

/// Everything that went out on the bus behind `marker`, in order.
async fn sent(rig: &FakeRig, marker: &Value) -> WeftResult<Vec<Value>> {
    let bus = rig.bus(marker)?;
    bus.inner_arc().close(weft::bus::StreamEnd::Finished);
    let mut cursor = bus.cursor_from_start()?;
    let mut out = Vec::new();
    while let Some(entry) = cursor.next(&mut HarnessRead).await? {
        // Joins and leaves are the bus's own; the node's events are its messages.
        if let Some((_, value)) = entry.as_json() {
            out.push(value.clone());
        }
    }
    Ok(out)
}

/// A posted turn's body without its `key`, and the key: what every test
/// but the one about the key compares.
fn keyed(body: &Value) -> (Value, String) {
    let mut body = body.clone();
    let key = body.as_object_mut().and_then(|b| b.remove("key")).and_then(|k| k.as_str().map(str::to_string)).expect("every turn carries a key");
    (body, key)
}

fn one_of_three() -> WeftResult<()> {
    let request = |inputs: Value| turn_request(inputs.as_object().unwrap(), "k1");
    let message = request(json!({ "message": "hi", "tools": [{ "name": "t", "parameters": {} }] }))?;
    assert_eq!(message, json!({ "message": "hi", "tools": [{ "name": "t", "parameters": {} }], "startOver": false, "key": "k1" }));
    let result = request(json!({ "toolResult": { "callId": "c1", "output": "42" }, "message": null }))?;
    assert_eq!(result, json!({ "toolResult": { "callId": "c1", "output": "42" }, "key": "k1" }));
    let error = request(json!({ "message": "hi", "answer": { "questionId": "q" } })).expect_err("two").to_string();
    assert!(error.contains("got message and answer"), "{error}");
    let error = request(json!({})).expect_err("none").to_string();
    assert!(error.contains("got none"), "{error}");
    let error = request(json!({ "answer": { "questionId": "q" }, "startOver": true })).expect_err("start over").to_string();
    assert!(error.contains("starts a new conversation with a `message`"), "{error}");
    Ok(())
}

fn reads() -> WeftResult<()> {
    assert_eq!(outcome(&json!({ "status": "done", "reply": "ok" }))?, Outcome::Reply("ok".into()));
    assert_eq!(outcome(&json!({ "status": "lost", "error": "send it again" }))?, Outcome::Failed("send it again".into()));
    assert_eq!(outcome(&json!({ "status": "working", "pending": null }))?, Outcome::Working);
    assert_eq!(outcome(&json!({ "status": "queued" }))?, Outcome::Working);
    let call = json!({ "status": "waiting_tool", "pending": { "n": 3, "type": "tool_call", "callId": "c1", "name": "lookup", "arguments": "{}" } });
    assert_eq!(outcome(&call)?, Outcome::ToolCall(json!({ "id": "c1", "name": "lookup", "arguments": "{}" })));
    let asked = json!({ "status": "waiting_question", "pending": { "n": 4, "type": "question", "question": { "id": "q1" } } });
    assert_eq!(outcome(&asked)?, Outcome::Question(json!({ "id": "q1" })));
    assert!(outcome(&json!({ "events": [] })).is_err(), "an answer with no status is refused");
    Ok(())
}

fn paths() -> WeftResult<()> {
    assert_eq!(events_path("a1b2-3", 7, 10)?, "/events?turn=a1b2-3&after=7&wait=10");
    assert_eq!(history_path("a1b2-3")?, "/history?turn=a1b2-3", "the conversation as that turn left it");
    Ok(())
}

async fn replies(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    let (_bus, stream) = rig.seed_bus(BusOptions::default())?;
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), status("working", vec![event(1, "text", json!({ "text": "on it" }))], Value::Null));
    let done = json!({ "turn": "t1", "status": "done", "events": [event(2, "done", json!({ "reply": "all done" }))], "pending": null, "reply": "all done" });
    answers(&rig, EndpointMethod::Get, &read("t1", 1), done);
    let conversation = json!([{ "role": "user", "content": "go" }, { "role": "assistant", "content": "all done" }]);
    answers(&rig, EndpointMethod::Get, "/history?turn=t1", json!({ "messages": conversation }));
    let outcome = rig.run(&AskAgentNode, json!({ "agent": agent, "stream": stream, "message": "go" })).await.ok()?;
    assert_eq!(outcome.outputs["reply"], json!("all done"));
    let file = StoredFile::from_value(&outcome.outputs["history"])?;
    let written: Value = serde_json::from_slice(&rig.stored_bytes(&file.key)?).unwrap();
    assert_eq!(written, conversation);
    assert!(outcome.outputs.get("toolCall").is_none() && outcome.outputs.get("question").is_none());
    let posted = keyed(rig.endpoint_calls()[0].body.as_ref().expect("a body"));
    assert_eq!(posted.0, json!({ "message": "go", "tools": [], "startOver": false }));
    let kinds: Vec<Value> = sent(&rig, &stream).await?.into_iter().map(|e| e["type"].clone()).collect();
    assert_eq!(kinds, vec![json!("text"), json!("done")], "every event once, in order");
    Ok(())
}

async fn tool_call(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    let call = event(1, "tool_call", json!({ "callId": "c1", "name": "lookup", "arguments": "{\"key\":\"x\"}" }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), status("waiting_tool", vec![call.clone()], call));
    let outcome = rig.run(&AskAgentNode, json!({ "agent": agent, "message": "look it up" })).await.ok()?;
    assert_eq!(outcome.outputs["toolCall"], json!({ "id": "c1", "name": "lookup", "arguments": "{\"key\":\"x\"}" }));
    assert!(outcome.outputs.get("reply").is_none());

    // The next firing hands the result over: the same turn carries on.
    // It reads on after the call, which the firing before it already sent.
    let rig = rig.fresh();
    let agent = rig.declare_infra("claude", "api", AGENT);
    let (_bus, stream) = rig.seed_bus(BusOptions::default())?;
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 1 }));
    let done = json!({ "turn": "t1", "status": "done", "events": [event(2, "done", json!({ "reply": "42" }))], "pending": null, "reply": "42" });
    answers(&rig, EndpointMethod::Get, &read("t1", 1), done);
    answers(&rig, EndpointMethod::Get, "/history?turn=t1", json!({ "messages": [] }));
    let outcome = rig.run(&AskAgentNode, json!({ "agent": agent, "stream": stream, "toolResult": { "callId": "c1", "output": "42" } })).await.ok()?;
    assert_eq!(outcome.outputs["reply"], json!("42"));
    let kinds: Vec<Value> = sent(&rig, &stream).await?.into_iter().map(|e| e["type"].clone()).collect();
    assert_eq!(kinds, vec![json!("done")], "the call is not sent a second time");
    assert_eq!(keyed(rig.endpoint_calls()[0].body.as_ref().expect("a body")).0, json!({ "toolResult": { "callId": "c1", "output": "42" } }));
    Ok(())
}

async fn question(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    let asked = json!({ "id": "q1", "kind": "permission", "questions": [], "permission": { "tool": "Bash", "title": "Bash wants to run", "detail": "date", "input": {}, "choices": [] } });
    let pending = event(1, "question", json!({ "question": asked }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), status("waiting_question", vec![pending.clone()], pending));
    let outcome = rig.run(&AskAgentNode, json!({ "agent": agent, "message": "run date" })).await.ok()?;
    // It carries where AnswerAgent reads whether the agent still waits on it.
    let mut handed = asked;
    handed["statusUrl"] = json!(format!("{AGENT}/question?id=q1"));
    assert_eq!(outcome.outputs["question"], handed);
    Ok(())
}

async fn quiet(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    // The agent node's side: on the bus the whole time, as it is in a run.
    let (mut on_it, stream) = rig.seed_bus(BusOptions::default())?;
    on_it.register("agent").map_err(|e| weft::WeftError::NodeFailed(e.to_string()))?;
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), status("working", vec![event(1, "text", json!({ "text": "thinking" }))], Value::Null));
    answers(&rig, EndpointMethod::Get, &read("t1", 1), status("working", vec![], Value::Null));
    let input = json!({ "agent": agent, "stream": stream, "message": "build it" });
    rig.signal_paused();
    let paused = rig.run(&AskAgentNode, input.clone()).await;
    assert!(paused.paused(), "a quiet agent parks the run: {:?}", paused.result);
    let saved = rig.saved_state(&AskAgentNode).expect("the place is kept");
    assert_eq!((saved["turn"].clone(), saved["after"].clone()), (json!("t1"), json!(1)));

    // The body runs again from its top: the turn is not posted again, and
    // the read goes on after event 1, which is not sent a second time.
    rig.signal(json!({ "news": true }));
    let done = json!({ "turn": "t1", "status": "done", "events": [event(2, "done", json!({ "reply": "built" }))], "pending": null, "reply": "built" });
    answers(&rig, EndpointMethod::Get, &read("t1", 1), done);
    answers(&rig, EndpointMethod::Get, "/history?turn=t1", json!({ "messages": [] }));
    let outcome = rig.run(&AskAgentNode, input).await.ok()?;
    assert_eq!(outcome.outputs["reply"], json!("built"));
    let posts = rig.endpoint_calls().iter().filter(|c| c.path == "/turns").count();
    assert_eq!(posts, 1, "the turn was posted once");
    let awaited = rig.awaited_signals();
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(awaited[0].config["url"], json!(format!("{AGENT}/events?turn=t1&after=1&wait=0")));
    assert_eq!(awaited[0].match_predicates, vec![Predicate::eq("news", "true")]);
    // The firing joined once and left once, as its body returned, never
    // over the pause; the agent still being on the bus keeps it open.
    assert!(!on_it.is_closed(), "the firing leaving ends nothing while the agent is on the bus");
    let mut entries = rig.bus(&stream)?.cursor_from_start()?;
    on_it.close();
    let (mut joins, mut leaves) = (0, 0);
    while let Some(entry) = entries.next(&mut HarnessRead).await? {
        match entry.kind {
            weft::bus::BusEntryKind::Joined { name } if name != "agent" => joins += 1,
            weft::bus::BusEntryKind::Left { name, .. } if name != "agent" => leaves += 1,
            _ => {}
        }
    }
    assert_eq!((joins, leaves), (1, 1), "on the bus from its first run to its return");
    let numbers: Vec<Value> = sent(&rig, &stream).await?.into_iter().map(|e| e["n"].clone()).collect();
    assert_eq!(numbers, vec![json!(1), json!(2)], "no event lost, none sent twice");
    Ok(())
}

async fn lost(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    let gone = json!({ "turn": "t1", "status": "lost", "events": [], "pending": null,
                       "error": "the agent server restarted in the middle of this turn, so the turn is lost; send the message again", "reply": null });
    answers(&rig, EndpointMethod::Get, &read("t1", 0), gone);
    let failure = rig.run(&AskAgentNode, json!({ "agent": agent, "message": "go" })).await.failure()?;
    assert!(failure.contains("send the message again"), "{failure}");
    Ok(())
}

async fn untyped(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    let (_bus, stream) = rig.seed_bus(BusOptions::default())?;
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), status("working", vec![json!({ "n": 1, "turn": "t1", "text": "hi" })], Value::Null));
    let failure = rig.run(&AskAgentNode, json!({ "agent": agent, "stream": stream, "message": "go" })).await.failure()?;
    assert!(failure.contains("sent an event with no type"), "{failure}");
    Ok(())
}

async fn untyped_unstreamed(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), status("working", vec![json!({ "n": 1, "turn": "t1", "text": "hi" })], Value::Null));
    let failure = rig.run(&AskAgentNode, json!({ "agent": agent, "message": "go" })).await.failure()?;
    assert!(failure.contains("sent an event with no type"), "nothing to send it on, and still refused: {failure}");
    Ok(())
}

async fn no_events(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    answers(&rig, EndpointMethod::Get, &read("t1", 0), json!({ "turn": "t1", "status": "done", "pending": null, "reply": "all done" }));
    let failure = rig.run(&AskAgentNode, json!({ "agent": agent, "message": "go" })).await.failure()?;
    assert!(failure.contains("answered a read of the turn with no events"), "{failure}");
    Ok(())
}

/// The worker dies after the turn went out and before its answer was
/// saved: the body runs again from its top and posts the turn again, with
/// the same key, which the server answers with the turn it already
/// started. A firing of another run posts another key.
async fn crash_after_post(rig: FakeRig) -> WeftResult<()> {
    let agent = agent(&rig);
    let input = json!({ "agent": agent, "message": "go" });
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    rig.crash_at_next_save();
    let crashed = rig.run(&AskAgentNode, input.clone()).await;
    assert!(crashed.result.is_err(), "the worker died at the save: {:?}", crashed.result);
    assert!(rig.saved_state(&AskAgentNode).is_none(), "the turn's answer was never saved");

    // The server dedups by key: the same turn comes back.
    answers(&rig, EndpointMethod::Post, "/turns", json!({ "turn": "t1", "after": 0 }));
    let done = json!({ "turn": "t1", "status": "done", "events": [event(1, "done", json!({ "reply": "ok" }))], "pending": null, "reply": "ok" });
    answers(&rig, EndpointMethod::Get, &read("t1", 0), done);
    answers(&rig, EndpointMethod::Get, "/history?turn=t1", json!({ "messages": [] }));
    let outcome = rig.run(&AskAgentNode, input.clone()).await.ok()?;
    assert_eq!(outcome.outputs["reply"], json!("ok"));
    assert_eq!(outcome.run_id, crashed.run_id, "the body ran again in the same run");
    let posts: Vec<(Value, String)> =
        rig.endpoint_calls().iter().filter(|c| c.path == "/turns").map(|c| keyed(c.body.as_ref().expect("a body"))).collect();
    assert_eq!(posts.len(), 2, "posted again after the crash");
    assert_eq!(posts[0], posts[1], "the same turn under the same key");
    assert_eq!(posts[0].1.len(), 64, "a fixed-length key: {}", posts[0].1);

    let other = rig.fresh();
    let agent = other.declare_infra("claude", "api", AGENT);
    other.answer_infra("claude", "api", EndpointMethod::Post, "/turns", json!({ "turn": "t2", "after": 0 }));
    other.answer_infra("claude", "api", EndpointMethod::Get, &read("t2", 0), json!({ "turn": "t2", "status": "done", "events": [], "pending": null, "reply": "ok" }));
    other.answer_infra("claude", "api", EndpointMethod::Get, "/history?turn=t2", json!({ "messages": [] }));
    other.run(&AskAgentNode, json!({ "agent": agent, "message": "go" })).await.ok()?;
    let another = keyed(other.endpoint_calls()[0].body.as_ref().expect("a body")).1;
    assert_ne!(another, posts[0].1, "another run posts another key");
    Ok(())
}

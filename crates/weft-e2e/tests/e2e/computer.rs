//! A computer a program works on, end to end: the `Computer` infra node's
//! real container (screen, mouse, keyboard, shell), the action nodes
//! against it, the premade `perform.weft` doing one tool call in either
//! vendor's style, and the premade `agent.weft` driven through exact turns
//! by a scripted model (`ChatFake`), so nothing here pays for a model.
#![cfg(feature = "e2e")]

use serde_json::{json, Value};
use weft_e2e::fakes::ChatFake;
use weft_e2e::{ensure, infra, project::Project, run};

/// What a Debug sink received on `data`.
fn data_of(settled: &run::SettledRun, node: &str) -> Value {
    settled.input_of(node).and_then(|input| input.get("data").cloned()).unwrap_or(Value::Null)
}

#[tokio::test]
async fn the_default_agent_works_the_computer_until_it_answers() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let call = |id: &str, name: &str, arguments: &str| {
        json!({ "id": id, "type": "function", "function": { "name": name, "arguments": arguments } })
    };
    let model = ChatFake::start(vec![
        // Two actions in one turn: their results stay together, and the
        // screenshot follows them in one message.
        json!({ "role": "assistant", "content": "", "tool_calls": [
            call("t1", "screenshot", "{}"),
            call("t2", "bash", "{\"command\": \"echo hi\"}"),
        ] }),
        // A turn whose first action fails: the second is answered
        // without running, and no screenshot message follows.
        json!({ "role": "assistant", "content": "", "tool_calls": [
            call("t3", "key", "{\"text\": \"NotAKey\"}"),
            call("t4", "screenshot", "{}"),
        ] }),
        json!({ "role": "assistant", "content": "Done: the shell said hi." }),
    ])
    .await?;
    let mut project = Project::prepare("computer", disp).await?;
    project.set_main(&format!(
        r#"
box = Computer
model = CustomProvider {{ baseUrl: "{}", model: "scripted" }}

agent = @include("__catalog/base_catalog/computer/agent.weft")
agent.task = "Say what the shell answers to echo hi."
agent.computer = box.computer
agent.model = model.provider

out = Debug {{ data: agent.answer }}
"#,
        model.base_url()
    ))?;
    infra::start_and_wait_running(&mut project, "box").await?;

    // Read through the program, so a node of the included agent is found
    // under its call (`agent.end`).
    let settled = run::run_and_settle(&mut project).await?.reading(project.definition()?);
    settled.completed()?;
    anyhow::ensure!(data_of(&settled, "out") == json!("Done: the shell said hi."), "{}", data_of(&settled, "out"));

    // Three turns: the model was asked three times, offered Anthropic's
    // computer tools each time, and tools were allowed on every turn.
    let requests = model.requests().await;
    anyhow::ensure!(requests.len() == 3, "three turns, got {}", requests.len());
    let offered: Vec<&str> = requests[0]["tools"]
        .as_array()
        .map(|tools| tools.iter().filter_map(|t| t["function"]["name"].as_str()).collect())
        .unwrap_or_default();
    anyhow::ensure!(offered.contains(&"left_click") && offered.contains(&"bash"), "Anthropic's names are offered: {offered:?}");
    anyhow::ensure!(requests.iter().all(|r| r["tool_choice"] == json!("auto")), "tools allowed on every turn");

    // What the model read back after the first turn: the assistant's two
    // calls, both results right after them, then the screenshot in one
    // user message.
    let second = requests[1]["messages"].as_array().cloned().unwrap_or_default();
    let tail: Vec<(&str, &str)> = second
        .iter()
        .rev()
        .take(3)
        .rev()
        .map(|m| (m["role"].as_str().unwrap_or(""), m["tool_call_id"].as_str().unwrap_or("")))
        .collect();
    anyhow::ensure!(tail == [("tool", "t1"), ("tool", "t2"), ("user", "")], "the results, then the screenshot: {second:?}");
    let result_of = |messages: &[Value], id: &str| {
        messages.iter().find(|m| m["role"] == "tool" && m["tool_call_id"] == id).map(|m| m["content"].clone())
    };
    anyhow::ensure!(result_of(&second, "t1") == Some(json!("Took a screenshot of the 1280x800 screen.")), "{second:?}");
    let shell = result_of(&second, "t2").and_then(|c| c.as_str().map(str::to_string)).unwrap_or_default();
    anyhow::ensure!(shell.starts_with("hi") && shell.ends_with("(exit code 0)"), "the shell's output: {second:?}");
    let image = second.last().cloned().unwrap_or_default();
    anyhow::ensure!(
        image["content"].as_array().is_some_and(|parts| parts.iter().filter(|p| p["type"] == "image_url").count() == 1),
        "one screenshot in the message: {image}"
    );
    // After the second turn: the failure, the action answered without
    // running, and no screenshot message, since nothing ran to take one.
    let third = requests[2]["messages"].as_array().cloned().unwrap_or_default();
    let failed = result_of(&third, "t3").and_then(|c| c.as_str().map(str::to_string)).unwrap_or_default();
    anyhow::ensure!(failed.contains("No such key name"), "the model reads the failure: {failed}");
    anyhow::ensure!(
        result_of(&third, "t4") == Some(json!("Not executed: an earlier computer action in this turn failed.")),
        "{third:?}"
    );
    anyhow::ensure!(third.last().is_some_and(|m| m["role"] == "tool"), "no screenshot message after a turn that took none: {third:?}");

    // The agent's shell session ended with its answer.
    settled.assert_completed("agent.end")?;

    infra::terminate_and_wait_gone(&project, "box").await?;
    project.finish().await
}

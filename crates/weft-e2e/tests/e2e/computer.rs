//! A computer a program works on, end to end: the `Computer` infra node's
//! real container (screen, mouse, keyboard, shell), the action nodes
//! against it, the premade `perform.weft` doing one tool call in either
//! vendor's style, and the premade `agent.weft` driven through exact turns
//! by a scripted model (`ChatFake`), so nothing here pays for a model.
#![cfg(feature = "e2e")]

use serde_json::{json, Value};
use weft_e2e::fakes::ChatFake;
use weft_e2e::{display, ensure, infra, poll_until, project::Project, run, storage};

/// What a Debug sink received on `data`.
fn data_of(settled: &run::SettledRun, node: &str) -> Value {
    settled.input_of(node).and_then(|input| input.get("data").cloned()).unwrap_or(Value::Null)
}

/// The bytes of the stored file a value names.
async fn bytes_of(project: &Project, file: &Value) -> anyhow::Result<Vec<u8>> {
    let key = weft_core::storage::StoredFile::from_value(file)?.key;
    storage::download(project.dispatcher(), &key).await
}

const PNG_MAGIC: &[u8] = b"\x89PNG\r\n\x1a\n";

#[tokio::test]
async fn a_computer_is_worked_by_hand_and_through_perform() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("computer", disp).await?;
    infra::start_and_wait_running(&mut project, "box").await?;

    // By hand: the screenshot is a PNG of the screen, the typing landed
    // in the terminal (the shell reads the file the typed command
    // wrote), and the shell answers.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    let shot = bytes_of(&project, &data_of(&settled, "shot")).await?;
    anyhow::ensure!(shot.starts_with(PNG_MAGIC), "the screenshot is a PNG, got {} bytes starting {:?}", shot.len(), &shot[..shot.len().min(8)]);
    let said = bytes_of(&project, &data_of(&settled, "said")).await?;
    anyhow::ensure!(said == b"typed\nhi\n", "the shell read what the typing did: {:?}", String::from_utf8_lossy(&said));

    // The node's card shows the screen as it is now, and the shell idle
    // in the folder the last command ended in, each with its button. A
    // unit's port answers a moment after it reports ready, and the read
    // door answers 502 in that gap, so the first read waits for it.
    let pid = project.id();
    let token = display::mint_display_token(project.dispatcher(), &pid, "weft-e2e-computer", &["box"], false).await?;
    poll_until("the computer's first card", std::time::Duration::from_secs(30), std::time::Duration::from_millis(500), || async {
        let (status, body) = project.dispatcher().get_raw_bearer(&format!("/signal-token/displays/{pid}/box"), &token).await?;
        match status {
            reqwest::StatusCode::BAD_GATEWAY => Ok(None),
            status if status.is_success() => Ok(Some(())),
            status => anyhow::bail!("the card's read door answered {status}: {body}"),
        }
    })
    .await?;
    let card = display::as_token(project.dispatcher(), &token, &pid, "box").await?;
    let screen = card.item("Screen").cloned().unwrap_or_default();
    anyhow::ensure!(
        screen["type"] == "image" && screen["data"].as_str().is_some_and(|d| d.starts_with("data:image/jpeg;base64,")),
        "the card shows the screen: {:?}",
        card.labels()
    );
    anyhow::ensure!(card.expect_text("Shell")?.contains("the default session: idle in /home/user"), "{:?}", card.data("Shell"));
    anyhow::ensure!(card.item("Shell").is_some_and(|i| i["action"]["actionKind"] == "restart_shell"), "the shell's button");

    // Through perform: a call in each style (Anthropic's left_click,
    // OpenAI's move), a screenshot and a shell command, and one that
    // fails on the computer, whose error the model reads instead of the
    // run failing.
    project.set_main(
        r#"
box = Computer

anthropic = @include("__catalog/base_catalog/computer/perform.weft")
anthropic.computer = box.computer
anthropic.call = { "id": "a1", "name": "left_click", "arguments": "{\"coordinate\": [300, 200]}" }

openai = @include("__catalog/base_catalog/computer/perform.weft")
openai.computer = box.computer
openai.call = { "id": "o1", "name": "move", "arguments": "{\"x\": 10, \"y\": 20, \"keys\": [\"SHIFT\"]}" }

look = @include("__catalog/base_catalog/computer/perform.weft")
look.computer = box.computer
look.call = { "id": "l1", "name": "screenshot", "arguments": "" }

shell = @include("__catalog/base_catalog/computer/perform.weft")
shell.computer = box.computer
shell.call = { "id": "s1", "name": "bash", "arguments": "{\"command\": \"echo from the shell\"}" }

broken = @include("__catalog/base_catalog/computer/perform.weft")
broken.computer = box.computer
broken.call = { "id": "b1", "name": "keypress", "arguments": "{\"keys\": [\"NOT_A_KEY\"]}" }

clicked = Debug { data: anthropic.result }
moved = Debug { data: openai.result }
looked = Debug { data: look.result }
picture = Debug { data: look.image }
ran = Debug { data: shell.result }
refused = Debug { data: broken.result }
why = Debug { data: broken.error }
"#,
    )?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    anyhow::ensure!(data_of(&settled, "clicked") == json!("Clicked."), "{}", data_of(&settled, "clicked"));
    anyhow::ensure!(data_of(&settled, "moved") == json!("Moved the mouse."), "{}", data_of(&settled, "moved"));
    anyhow::ensure!(
        data_of(&settled, "looked") == json!("Took a screenshot of the 1280x800 screen."),
        "{}",
        data_of(&settled, "looked")
    );
    let picture = bytes_of(&project, &data_of(&settled, "picture")).await?;
    anyhow::ensure!(picture.starts_with(PNG_MAGIC), "perform hands the screenshot out on image");
    let ran = data_of(&settled, "ran").as_str().unwrap_or_default().to_string();
    anyhow::ensure!(ran.starts_with("from the shell") && ran.ends_with("(exit code 0)"), "the command's output and exit code: {ran:?}");
    let refused = data_of(&settled, "refused").as_str().unwrap_or_default().to_string();
    anyhow::ensure!(refused.contains("No such key name"), "the model reads xdotool's refusal: {refused}");
    anyhow::ensure!(data_of(&settled, "why").as_str() == Some(refused.as_str()), "the same failure comes out on error");

    infra::terminate_and_wait_gone(&project, "box").await?;
    project.finish().await
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

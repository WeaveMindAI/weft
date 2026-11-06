//! A coding agent driven from a program, end to end: the `ClaudeCode` node
//! runs its agent server as a process unit on this machine, from a stand-in
//! `claude` (`catalog/agents/server/testing/fake_claude.py`, speaking Claude
//! Code's stream-json as recorded from real runs, so nothing here pays for a
//! model). The premade `conversation.weft` holds the loop: a permission
//! request and a question each reach a person as a form while the run waits,
//! the turn carries on with each answer, the reply reaches the person, an
//! empty next message ends the conversation, and `CloseBus` ends the agent
//! node's part of the run, which then ends cleanly.
#![cfg(feature = "e2e")]

use serde_json::{json, Value};
use weft_e2e::signal::DiscoveredSignal;
use weft_e2e::{client::Dispatcher, ensure, human, infra, project::Project, run};

/// The form `node` shows after `answered`: the same node asks again in the
/// next firing, under a token of its own.
async fn next_form(disp: &Dispatcher, pid: &uuid::Uuid, node: &str, answered: &DiscoveredSignal) -> anyhow::Result<DiscoveredSignal> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let form = human::wait_for_form_by_node(disp, pid, node).await?;
        if form.token() != answered.token() {
            return Ok(form);
        }
        anyhow::ensure!(std::time::Instant::now() < deadline, "'{node}' never asked again after its first form was answered");
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
}

/// The repo's stand-in for Claude Code.
fn fake_claude() -> std::path::PathBuf {
    // The fixtures sit in crates/weft-e2e/, two levels under the checkout.
    weft_e2e::project::fixtures_root().join("../../../catalog/agents/server/testing/fake_claude.py")
}

#[tokio::test]
async fn an_agent_asks_a_person_twice_and_the_run_ends_with_its_bus() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("agents", disp.clone()).await?;
    let pid = project.id();
    // The folder it works in and the program it runs, both on this machine.
    let work = tempfile::tempdir()?;
    let program = work.path().join("claude");
    std::fs::write(&program, format!("#!/bin/sh\nexec python3 {} \"$@\"\n", fake_claude().display()))?;
    std::fs::set_permissions(&program, std::os::unix::fs::PermissionsExt::from_mode(0o755))?;
    let folder = work.path().join("work");
    std::fs::create_dir(&folder)?;
    project.set_main(&format!(
        r#"
claude = ClaudeCode {{
  folder: "{}"
  program: "{}"
}}

talk = @include("__catalog/base_catalog/agents/conversation.weft")
talk.agent = claude.agent
talk.stream = claude.stream
talk.task = "please bash, then ask me"

out = Debug {{ data: talk.reply }}
"#,
        folder.display(),
        program.display()
    ))?;
    infra::start_and_wait_running(&mut project, "claude").await?;
    let run_id = run::start(&mut project).await?;

    // The permission request: the stand-in asks to run Bash.
    let permission = human::wait_for_form_by_node(&disp, &pid, "talk.messages.turn.answerIt").await?;
    run::wait_for_status(&disp, run_id, "waiting_for_input").await?;
    human::answer_form(&disp, &permission, &json!({ "choice": "allow", "note": "" })).await?;
    // Its question, with options: answered with one of them.
    let question = next_form(&disp, &pid, "talk.messages.turn.answerIt", &permission).await?;
    human::answer_form(&disp, &question, &json!({ "q0": "Blue", "q0.other": "" })).await?;
    // The reply reaches the person, who ends the conversation.
    let reply = human::wait_for_form_by_node(&disp, &pid, "talk.messages.next").await?;
    human::answer_form(&disp, &reply, &json!({ "nextMessage": "" })).await?;

    let settled = run::SettledRun::observe(&disp, run_id).await?.reading(project.definition()?);
    settled.completed()?;
    // The stand-in replies with the answers it got: the question's, keyed by
    // its text, as Claude Code reads them.
    let said = settled.input_of("out").and_then(|input| input.get("data").cloned()).unwrap_or(Value::Null);
    let answers: Value = said.as_str().and_then(|text| serde_json::from_str(text).ok()).unwrap_or(Value::Null);
    anyhow::ensure!(answers == json!({ "Red or blue?": "Blue" }), "the agent's reply: {said}");
    project.finish().await
}

//! Human-in-the-loop: an event starts a flow on an active project; a mid-flow
//! HumanQuery suspends; the rig answers it over HTTP (no browser) and asserts
//! the run resumed with the approval.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, human, project::Project, run, SettledRun};

#[tokio::test]
async fn human_query_resumes_with_approval() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    // Stand up the fake feed that the entry trigger subscribes to, then activate
    // (a HumanQuery resume only dispatches on an active project; activation also
    // brings up the listener that routes the form submission).
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Snapshot existing executions (activation creates a TriggerSetup run) so we
    // can identify the genuine Fire execution the event creates.
    let before = run::execution_colors(&disp, &pid).await?;

    // Push an event to START a fresh execution; it runs to HumanQuery and
    // suspends. (Brief settle so the listener's SSE connect is live first.)
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let color =
        run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;

    // Play the human: wait for the query form, approve it.
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;

    // The suspended run resumes and completes; approval flows to Debug as true.
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(true))?;

    project.finish().await
}

//! A HumanTrigger ENTRY form is enumerable via the signal-token list (so the
//! browser extension can show it under "Triggers") and firing it starts a fresh
//! execution. Entry-trigger forms used to be excluded from enumeration; this
//! pins the fixed behavior: the list returns them tagged `isResume=false`, and a
//! submit to `POST /signal/{token}` runs the graph.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, human, project::Project, run, signal::SignalScope, SettledRun};

#[tokio::test]
async fn human_trigger_entry_form_enumerates_and_fires() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_trigger_enum", disp.clone()).await?;
    let pid = project.id();

    // Activate: the HumanTrigger registers its entry form signal.
    project.activate().await?;
    let before = run::execution_colors(&disp, &pid).await?;

    // The entry form appears in the signal-token enumeration, tagged as a
    // trigger (isResume=false), NOT a resume task.
    let scope = SignalScope::open(&disp, &pid).await?;
    let form = human::wait_for_form_by_node(&disp, &pid, "start").await?;
    assert_eq!(
        form.is_resume(),
        Some(false),
        "an entry HumanTrigger form must enumerate as a trigger (isResume=false)"
    );
    assert_eq!(form.kind(), Some("form"));
    // The scope handle is the reusable enumeration token; a second listing
    // through it still shows the trigger (it stays registered while active).
    assert!(
        scope.signal_for_node("start").await?.is_some(),
        "the trigger stays listed while the project is active"
    );

    // Firing the entry form (POST /signal/{token}) starts a fresh execution.
    human::answer_form(&disp, &form, &json!({ "go": "approve" })).await?;
    let color = run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    // approve_reject 'go' -> go_approved = true reaches Debug.
    settled.assert_input("out", "data", &json!(true))?;

    project.finish().await
}

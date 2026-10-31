//! A caller's credential through a run that pauses: made a secret at the
//! door, it is revealed into the request the program sends after the run
//! carried on from its record (a fresh drive, nothing of the first in its
//! memory), in its headers and in its body, whose declared type says where
//! it is; and nothing that reads the run's record finds it: the replay
//! with its values, and a search of the project's runs.
#![cfg(feature = "e2e")]

use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{ensure, fakes::RequestsFake, human, live, project::Project, run};

#[tokio::test]
async fn a_callers_credential_survives_a_pause_and_is_never_written_down() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("route_secret", disp.clone()).await?;
    let pid = project.id();
    let service = RequestsFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_HOST__", service.url().trim_start_matches("http://"))?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = run::runs(&disp, &pid).await?;

    let token = format!("Bearer e2e-{}", uuid::Uuid::new_v4().simple());
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/whoami"), &[("authorization", token.as_str())], &json!({})).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let run_id = run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;

    // The run pauses on the form; answering it carries the run on from its
    // record, and the credential goes out revealed.
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    run::SettledRun::observe(&disp, run_id).await?.completed()?;
    let seen = service.seen().await;
    anyhow::ensure!(
        seen.iter().any(|request| request.headers.iter().any(|(name, value)| name == "authorization" && *value == token)),
        "the service never got the caller's credential in its headers: {seen:?}"
    );
    anyhow::ensure!(
        seen.iter().any(|request| request.body["caller"]["authorization"] == json!(token)),
        "the service never got the caller's credential in its body: {seen:?}"
    );

    // Nothing that reads the record finds it.
    let replay: Value = disp.get_json(&format!("/runs/{run_id}/replay?with_values=true")).await?;
    let replay = replay.to_string();
    anyhow::ensure!(!replay.contains(&token[7..]), "the run's record holds the credential: {replay}");
    anyhow::ensure!(replay.contains(weft_core::secret::SECRET_MARKER_KEY), "the record has no marker where the credential was: {replay}");
    let found = |listing: &Value| listing["runs"].as_array().is_some_and(|runs| runs.iter().any(|r| r["runId"] == json!(run_id.to_string())));
    // The run is indexed (found by a node it ran through), and nothing in
    // its entry is the credential.
    let indexed: Value = serde_json::from_str(project.weft(&["runs", "--through", "forward", "--json"]).await?.trim())?;
    anyhow::ensure!(found(&indexed), "the run is not in the search index: {indexed}");
    let by_token: Value = serde_json::from_str(project.weft(&["runs", "--search", &token[7..], "--json"]).await?.trim())?;
    anyhow::ensure!(!found(&by_token), "the search found the run by its credential: {by_token}");

    project.finish().await
}

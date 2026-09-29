//! A route that does not record its runs (`recorded: false`): a page
//! polling it answers every time and leaves nothing in the executions
//! listing, and a call that fails is written down afterwards, whole, so
//! it lists like any recorded run.
#![cfg(feature = "e2e")]

use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{client::poll_until, ensure, live, project::Project};

/// The project's fire runs, as the listing has them, by entry node.
async fn listed(disp: &weft_e2e::client::Dispatcher, pid: uuid::Uuid) -> anyhow::Result<Vec<Value>> {
    let page: Value = disp.get_json(&format!("/executions?project_id={pid}&phase=fire&limit=50")).await?;
    Ok(page["executions"].as_array().cloned().unwrap_or_default())
}

#[tokio::test]
async fn an_unrecorded_route_answers_and_only_its_failures_list() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_unrecorded", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let pid = project.id();

    // A page polling the status: every call answers.
    for _ in 0..3 {
        let (status, _, body) =
            live::http_request(&disp, Method::GET, &format!("{base}/status"), &[], None).await?;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        assert_eq!(serde_json::from_slice::<Value>(&body)?, json!({ "state": "fine" }));
    }

    // A call that fails tells the caller so.
    let (status, _, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/broken"), &[], None).await?;
    assert!(status.as_u16() >= 500, "a failed run is an error to its caller: {status} {}", String::from_utf8_lossy(&body));

    // Its run is written after the answer, so wait for it to list.
    let failed = poll_until("the failed unrecorded run to list", Duration::from_secs(60), Duration::from_millis(250), || {
        let disp = disp.clone();
        async move {
            let rows = listed(&disp, pid).await?;
            Ok(rows.into_iter().find(|row| row["entry_node"] == "broken"))
        }
    })
    .await?;
    assert_eq!(failed["status"], "failed", "{failed}");

    // The successful calls left nothing, whatever time has passed.
    let rows = listed(&disp, pid).await?;
    assert!(
        !rows.iter().any(|row| row["entry_node"] == "status"),
        "a successful unrecorded run is never listed: {rows:?}"
    );
    let execution_id = failed["execution_id"].as_str().unwrap_or_default();
    let cli = project.weft(&["executions", "--project", &pid.to_string()]).await?;
    assert!(cli.contains(&execution_id[..8.min(execution_id.len())]), "`weft executions` lists the failed run {execution_id}: {cli}");

    project.finish().await
}

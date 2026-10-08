//! Routes whose runs cannot pause: a wait holds in the worker while the
//! caller is on the line (or the run is unrecorded), its answer carries the
//! run on, and a wait that outlasts the route's `holdSecs` is given up,
//! failing the call with the reason.
#![cfg(feature = "e2e")]

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{client::poll_until, ensure, live, project::Project};

#[tokio::test]
async fn a_route_holds_its_wait_and_gives_up_after_its_hold() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("route_hold", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let pid = project.id();

    // Tied to its caller, and unrecorded: the timer answers the wait in
    // the worker, and the caller gets the reply after it.
    for path in ["tied", "unrecorded"] {
        let started = Instant::now();
        let (status, _, body) = live::http_request(&disp, Method::GET, &format!("{base}/{path}"), &[], None).await?;
        assert_eq!(status, 200, "{path}: {}", String::from_utf8_lossy(&body));
        assert_eq!(serde_json::from_slice::<Value>(&body)?, json!({ "state": "waited" }), "{path}");
        assert!(started.elapsed() >= Duration::from_secs(1), "{path}: the reply came after the wait");
    }

    // A thirty second timer under a one second hold: given up, and the
    // caller hears why long before the timer.
    let started = Instant::now();
    let (status, _, body) = live::http_request(&disp, Method::GET, &format!("{base}/impatient"), &[], None).await?;
    let said = String::from_utf8_lossy(&body).to_string();
    assert_eq!(status.as_u16(), 500, "{said}");
    assert!(said.contains("gave up its wait") && said.contains("holdSecs"), "{said}");
    assert!(started.elapsed() < Duration::from_secs(20), "given up after its hold, not the timer");

    // Its run lists failed. Its ending is written after the caller heard,
    // so wait for the row to leave `running`.
    let failed = poll_until("the given-up run to end", Duration::from_secs(60), Duration::from_millis(250), || {
        let disp = disp.clone();
        async move {
            let page: Value = disp.get_json(&format!("/executions?project_id={pid}&phase=fire&limit=50")).await?;
            Ok(page["executions"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .find(|row| row["entry_node"] == "impatient" && row["status"] != "running"))
        }
    })
    .await?;
    assert_eq!(failed["status"], "failed", "{failed}");

    project.finish().await
}

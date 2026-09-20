//! Layer-4: a trigger inside an included file activates and fires like a
//! trigger inside a group. The `include_trigger` fixture holds one route
//! at the top level and one inside a file included at site `one`; both
//! register on activation, both answer, and the included route's run
//! carries the site's call frame.
#![cfg(feature = "e2e")]

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{display, ensure, live, run, project::Project};

#[tokio::test]
async fn a_route_inside_an_included_file_registers_and_answers() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_trigger", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = run::execution_colors(&disp, &project.id()).await?;

    // The included route's display is reached by its PLACE, the way the
    // editor spells it after walking into the include (`one.door`): its
    // registration is keyed by that spelling, and the file's own id for
    // the node opens nothing.
    let pid = project.id();
    let shown = display::as_editor(&disp, &pid, "one.door").await?;
    anyhow::ensure!(!shown.labels().is_empty(), "the included route's display is empty");
    let (status, _) = disp.get_raw(&format!("/projects/{pid}/signals/@src:lib:door.door/live")).await?;
    anyhow::ensure!(status == 404, "the compiled id must open no display, got {status}");

    // The top-level route answers as it always did.
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/front"), &[], &json!({ "text": "hi" })).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "heard": "hi", "where": "front" }));

    // The route inside the included file answers under its site.
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/door"), &[], &json!({ "text": "hi" })).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "heard": "HI", "where": "inside" }));

    // Both fires are their own runs; the included one ran under the
    // site's call frame and is addressed through it.
    let colors = run::wait_for_triggered_executions(&disp, &project.id(), &before, 2, std::time::Duration::from_secs(60)).await?;
    let mut inside = 0;
    for color in colors {
        let settled = project.settled(color).await?;
        settled.completed()?;
        let frames: Vec<Value> = settled.events_of("one.door").map(|e| e.frames().clone()).collect();
        if !frames.is_empty() {
            inside += 1;
            anyhow::ensure!(frames.iter().all(|f| *f == json!([{"site": "one"}])), "{frames:?}");
            settled.assert_completed("one.door_answer")?;
            settled.assert_untouched("front_answer")?;
        } else {
            settled.assert_completed("front_answer")?;
        }
    }
    anyhow::ensure!(inside == 1, "exactly one run fired the included route");
    project.finish().await
}

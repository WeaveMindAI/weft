//! Layer-4: a `_should_flow` wire from a route into a group's door runs
//! everything behind the door. The `gated_include` fixture answers one
//! route from inside a plain group and another from inside an included
//! file, and nothing but the `_should_flow` wire connects either to its
//! route. Before the fix the fire's program held only the route and the
//! door, and every caller got "the run ended without answering".
#![cfg(feature = "e2e")]

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{ensure, live, run, project::Project};

#[tokio::test]
async fn a_group_gated_by_a_route_answers_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("gated_include", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = run::execution_colors(&disp, &project.id()).await?;

    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/grouped"), &[], &json!({})).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "where": "group" }));

    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/included"), &[], &json!({})).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "where": "include" }));

    // Each fire ran the nodes behind its door and nothing behind the other.
    let colors = run::wait_for_triggered_executions(&disp, &project.id(), &before, 2, std::time::Duration::from_secs(60)).await?;
    let mut seen = (false, false);
    for color in colors {
        let settled = project.settled(color).await?;
        settled.completed()?;
        // `body` has no wire into it: the pre-run root list and the
        // group's own launcher both kick it, at the same place, and the
        // two kicks are one firing (a loop body is the case where they
        // are not, covered by `loops`).
        if settled.events_of("work.answer").next().is_some() {
            seen.0 = true;
            settled.assert_completed("work.body")?;
            settled.assert_completed("work.answer")?;
            settled.assert_untouched("deep.answer")?;
            let fired = settled.node_outputs("work.body").len();
            anyhow::ensure!(fired == 1, "work.body fired {fired} times");
        } else {
            seen.1 = true;
            settled.assert_completed("deep.body")?;
            settled.assert_completed("deep.answer")?;
            settled.assert_untouched("work.answer")?;
            let fired = settled.node_outputs("deep.body").len();
            anyhow::ensure!(fired == 1, "deep.body fired {fired} times");
        }
    }
    anyhow::ensure!(seen == (true, true), "one fire per door");
    project.finish().await
}

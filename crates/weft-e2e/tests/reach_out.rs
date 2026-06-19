//! Reach-out trigger: the system dials OUT to an SSE feed the rig stands up;
//! a pushed event fires a fresh execution carrying the event payload.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, project::Project, run, SettledRun};

#[tokio::test]
async fn sse_event_fires_execution_with_payload() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
    let pid = project.id();

    // Stand up the fake SSE feed and point the custom trigger at it.
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;

    // Activate: the listener dials the feed and holds the SSE connection open.
    project.activate().await?;

    // Snapshot existing executions (activation creates a TriggerSetup run) so we
    // can tell the genuine Fire execution apart from it.
    let before = run::execution_colors(&disp, &pid).await?;

    // Give the listener a moment to establish the subscription, then push one
    // matching event. (The subscription is live once activate returns, but the
    // listener's connect loop reconnects with backoff, so a brief settle avoids
    // racing the very first connect.)
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("tick", &json!({ "value": 99 }).to_string());

    // A fresh execution fires; assert it carried value=99 to Debug.
    let color =
        run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    // The SSE payload `{"value":99}` carries through as a JSON integer (the
    // event JSON is passed verbatim; no f64 coercion on this path).
    settled.assert_input("out", "data", &json!(99))?;

    project.finish().await
}

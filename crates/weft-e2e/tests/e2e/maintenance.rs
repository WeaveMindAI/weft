//! The install's maintenance (`weft maintenance`): everything switched off
//! for an update, nothing turned on while it lasts, and what it took down
//! back on once it is lifted.
//!
//! Maintenance is install-wide, which every other test sharing the install
//! under test would feel, so each test here runs in a cell of its own.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::fakes::SseFake;
use weft_e2e::{project::Project, run, Cell, SettledRun};

/// A trigger that is on goes down with `weft maintenance on`, cannot be
/// switched on while the maintenance lasts, the update check passes, and
/// `weft maintenance off` brings it back listening: an event after the
/// lift fires a run.
#[tokio::test]
async fn a_trigger_comes_back_after_the_maintenance() -> anyhow::Result<()> {
    let cell = Cell::start().await?;
    let disp = cell.dispatcher();
    let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;
    let pid = project.id();
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;

    let refused = project.weft_refused(&["maintenance", "check"]).await?;
    assert!(refused.contains("weft maintenance on"), "the check names the way through:\n{refused}");

    project.weft(&["maintenance", "on"]).await?;
    weft_e2e::status::wait_until_status(&disp, &pid, "inactive", Duration::from_secs(60)).await?;
    let refused = project.weft_refused(&["activate"]).await?;
    assert!(refused.contains("maintenance"), "an activate during the maintenance is refused, naming it:\n{refused}");
    project.weft(&["maintenance", "check"]).await?;
    let status = project.weft(&["maintenance"]).await?;
    assert!(status.contains("in maintenance since") && status.contains("'feed'"), "it says what the lift brings back:\n{status}");

    let off = project.weft(&["maintenance", "off"]).await?;
    assert!(off.contains("turned on 'feed'") && off.contains("the maintenance is lifted"), "it says what came back:\n{off}");
    weft_e2e::status::wait_until_status(&disp, &pid, "active", Duration::from_secs(60)).await?;
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    let before = run::runs(&disp, &pid).await?;
    feed.push_event("tick", &json!({ "value": 7 }).to_string())?;
    let run_id = run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    SettledRun::observe(&disp, run_id).await?.completed()?;
    let status = project.weft(&["maintenance"]).await?;
    assert!(status.contains("not in maintenance"), "the lift ended it:\n{status}");

    project.finish().await?;
    cell.finish().await
}

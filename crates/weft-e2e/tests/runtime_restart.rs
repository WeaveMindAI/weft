//! What an install keeps across its own restart: the runtime process stops
//! and starts again (a reboot, an upgrade), and the work it was holding
//! carries on as if nothing happened.
//!
//! Every test here restarts the runtime, which the default install's other
//! tests would feel, so each runs in a cell at real time: the timers under
//! test are ones a person configured, which keep their real length at any
//! pace.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::fakes::SseFake;
use weft_e2e::{project::Project, run, Cell, SettledRun};

/// A run parked on a timer finishes on time even though the runtime that
/// registered the timer is gone before it is due: the wake lives in the
/// install's database, and the runtime that comes back delivers it.
#[tokio::test]
async fn a_timer_fires_across_a_runtime_restart() -> anyhow::Result<()> {
    let cell = Cell::start(1.0).await?;
    let disp = cell.dispatcher();
    let mut project = Project::prepare("wait_format", disp.clone()).await?;
    project.set_node_config("hold", "seconds", "30")?;

    let execution_id = run::start(&mut project).await?;
    run::wait_for_status(&disp, execution_id, "waiting_for_input").await?;
    cell.restart().await?;

    let settled = SettledRun::observe(&disp, execution_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("Hi quentin, you have 3 left"))?;

    project.finish().await?;
    cell.finish().await
}

/// A trigger that holds a connection open (an SSE feed) is listening again
/// after the runtime comes back, with nobody activating anything: the
/// listener picks up every signal the install had registered.
#[tokio::test]
async fn a_held_connection_trigger_listens_again_after_a_runtime_restart() -> anyhow::Result<()> {
    let cell = Cell::start(1.0).await?;
    let disp = cell.dispatcher();
    let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;
    let pid = project.id();
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;

    cell.restart().await?;
    // The old connection died with the old runtime, so a reader now is the
    // new runtime's listener.
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;

    let before = run::executions(&disp, &pid).await?;
    feed.push_event("tick", &json!({ "value": 7 }).to_string());
    let execution_id = run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, execution_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(7))?;

    project.finish().await?;
    cell.finish().await
}

/// A program's infrastructure outlives the runtime: its unit keeps
/// running while the runtime restarts, the new runtime's supervisor adopts
/// it, and a run reaches it again.
#[tokio::test]
async fn infra_keeps_running_across_a_runtime_restart() -> anyhow::Result<()> {
    let cell = Cell::start(1.0).await?;
    let disp = cell.dispatcher();
    let mut project = Project::prepare("infra_min", disp.clone()).await?;
    weft_e2e::infra::start_and_wait_running(&mut project, "svc").await?;

    cell.restart().await?;
    weft_e2e::infra::wait_running(&project, "svc").await?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;

    weft_e2e::infra::terminate_and_wait_gone(&project, "svc").await?;
    project.finish().await?;
    cell.finish().await
}

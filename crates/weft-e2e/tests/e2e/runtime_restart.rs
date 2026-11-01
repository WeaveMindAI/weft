//! What an install keeps across its own restart: the runtime process stops
//! and starts again (a reboot, an upgrade), and the work it was holding
//! carries on as if nothing happened.
//!
//! The test restarts the runtime, which the install under test's other
//! tests would feel, so it runs in a cell. One cell and one restart carry
//! every kind of held work at once.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::fakes::SseFake;
use weft_e2e::{project::Project, run, Cell, SettledRun};

/// Everything the install was holding carries on across one restart of
/// its runtime, with nobody doing anything:
/// - a run parked on a timer finishes on time, though the runtime that
///   registered the timer is gone before it is due: the wake lives in the
///   install's database, and the runtime that comes back delivers it;
/// - a trigger that holds a connection open (an SSE feed) is listening
///   again: the listener picks up every signal the install had registered;
/// - a program's infrastructure keeps running: the new runtime's supervisor
///   adopts its unit, and a run reaches it again.
#[tokio::test]
async fn held_work_carries_on_across_a_runtime_restart() -> anyhow::Result<()> {
    let cell = Cell::start().await?;
    let disp = cell.dispatcher();

    let mut infra = Project::prepare("infra_min", disp.clone()).await?;
    weft_e2e::infra::start_and_wait_running(&mut infra, "svc").await?;

    let mut held = Project::prepare("reach_out_feed", disp.clone()).await?;
    let feed = SseFake::start().await?;
    held.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    held.activate().await?;
    let held_id = held.id();
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;

    // Parked last, right before the restart, so its 30 seconds are due
    // only after the runtime that registered them is gone.
    let mut parked = Project::prepare("wait_format", disp.clone()).await?;
    parked.set_node_config("hold", "seconds", "30")?;
    let parked_run = run::start(&mut parked).await?;
    run::wait_for_status(&disp, parked_run, "waiting_for_input").await?;
    cell.restart().await?;

    // The old connection died with the old runtime, so a reader now is the
    // new runtime's listener.
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    let before = run::runs(&disp, &held_id).await?;
    feed.push_event("tick", &json!({ "value": 7 }).to_string())?;
    let fired = run::wait_for_triggered_run(&disp, &held_id, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, fired).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(7))?;

    weft_e2e::infra::wait_running(&infra, "svc").await?;
    let settled = run::run_and_settle(&mut infra).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;

    let settled = SettledRun::observe(&disp, parked_run).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("Hi quentin, you have 3 left"))?;

    weft_e2e::infra::terminate_and_wait_gone(&infra, "svc").await?;
    infra.finish().await?;
    held.finish().await?;
    parked.finish().await?;
    cell.finish().await
}

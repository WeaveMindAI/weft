//! A trigger fire runs its own program and leaves the other alone.
//!
//! Two triggers in one file share an upstream node (see
//! fixtures/two_programs/main.weft). Firing one journals the fire's
//! subgraph on `ExecutionStarted`, so the shared node's pulse into the
//! other program is absorbed without a trace and the run completes,
//! while the fired program's own dangling node (no output asks for it)
//! journals one `outside_this_run` skip. The regression this pins: fires
//! used to journal no boundary, so that pulse parked on a partial input
//! set forever and every fire of a two-trigger project ended as a stuck
//! failure.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, human, project::Project, run, SettledRun};

#[tokio::test]
async fn a_fire_runs_its_own_program_and_skips_the_other() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("two_programs", disp.clone()).await?;
    let pid = project.id();

    project.activate().await?;
    let before = run::execution_colors(&disp, &pid).await?;

    let form = human::wait_for_form_by_node(&disp, &pid, "start_x").await?;
    human::answer_form(&disp, &form, &json!({ "go": "approve" })).await?;
    let color = run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, color).await?;

    // The fired program runs whole and the run completes.
    settled.completed()?;
    settled.assert_completed("shared")?;
    settled.assert_completed("gate_x")?;
    settled.assert_completed("out_x")?;
    settled.assert_input("out_x", "data", &json!("shared"))?;
    // The fired program's own dangling node journals exactly one skip
    // saying why: it is reachable from the trigger, no output wants it.
    settled.assert_skip_reason("side_x", "outside_this_run")?;
    // The other program: not a trace. The shared pulse into `gate_y` is
    // absorbed silently, its trigger was not kicked, its output never
    // heard of this run.
    settled.assert_untouched("gate_y")?;
    settled.assert_untouched("start_y")?;
    settled.assert_untouched("out_y")?;

    project.finish().await
}

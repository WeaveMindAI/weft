//! Platform: the worker dies while two steps are running, and neither runs
//! again. In a durable run, a step that was mid-body may have partly
//! happened (an email sent, a row written), so the worker that picks the
//! run back up fails it with a message saying so instead of starting it
//! over, and a node that catches its failures hands this one to its wired
//! `error` like any other. A fast run's record trails it, so it is not
//! picked back up at all: it ends cancelled.
//!
//! Shape: the `step_crash` fixture runs two `ExecPython` steps that sleep
//! for ten minutes. Once both have started, every worker of the project is
//! removed (a fake crash). The claim on the run lapses and a fresh worker
//! takes it. Durable, the run ends failed: `plain` failed with the crash
//! message, `caught` completed with that message on `error`, `handler`
//! received it. Fast, the run ends cancelled, saying its worker went away.
//! Either way each step started exactly once.
//!
//! A worker told to stop (`SIGTERM`) is the other case: it gives its runs
//! a few seconds, then stops the steps still running and hands every
//! recorded run back, fast ones included. The next worker carries the run
//! on from its record and fails those steps the way it fails a crashed one.
#![cfg(feature = "e2e")]

use std::time::Duration;

use weft_e2e::{ensure, platform::Platform, project::Project, run, SettledRun};

/// What the worker that picks the run back up says about a step the dead
/// one was running.
const WENT_AWAY: &str = "went away while it was running; it was not run again";

#[tokio::test]
async fn a_step_the_worker_died_in_is_failed_never_run_again() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("step_crash", disp.clone()).await?;
    let pid = project.id();

    let execution_id = run::start_durable(&mut project).await?;
    run::wait_for_nodes_started(&disp, execution_id, &["plain", "caught"]).await?;
    let killed = platform.kill_workers(&pid).await?;
    anyhow::ensure!(!killed.is_empty(), "no worker was running the steps when the test removed them");

    // The dead worker's claim lapses before a fresh one takes the run.
    let settled = SettledRun::observe_within(&disp, execution_id, Duration::from_secs(240)).await?;
    settled.failed_with(WENT_AWAY)?;

    for step in ["plain", "caught"] {
        let starts = settled.events_of(step).filter(|e| e.kind() == "node_started").count();
        anyhow::ensure!(starts == 1, "'{step}' started {starts} times; a step never runs twice by itself");
    }
    let plain_failure = settled
        .events_of("plain")
        .find(|e| e.kind() == "node_failed")
        .and_then(|e| e.str_field("error").map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("'plain' was not failed"))?;
    anyhow::ensure!(plain_failure.contains(WENT_AWAY), "{plain_failure}");

    // The caught one completed, its failure on `error`, and the branch
    // reading `error` ran with it.
    settled.assert_completed("caught")?;
    let caught = settled
        .input_of("handler")
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("'handler' never received the caught failure"))?;
    anyhow::ensure!(caught.contains(WENT_AWAY), "{caught}");

    project.finish().await
}

#[tokio::test]
async fn a_fast_run_whose_worker_died_ends_cancelled_never_run_again() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("step_crash", disp.clone()).await?;
    let pid = project.id();

    let execution_id = run::start(&mut project).await?;
    run::wait_for_nodes_started(&disp, execution_id, &["plain", "caught"]).await?;
    let killed = platform.kill_workers(&pid).await?;
    anyhow::ensure!(!killed.is_empty(), "no worker was running the steps when the test removed them");

    // The dead worker's claim lapses, a fresh worker takes the run's task
    // and ends it instead of driving it.
    let settled = SettledRun::observe_within(&disp, execution_id, Duration::from_secs(240)).await?;
    anyhow::ensure!(settled.status == "cancelled", "expected the fast run to be cancelled, it is {}", settled.status);
    let reason = settled.cancel_reason().unwrap_or_default();
    anyhow::ensure!(reason.contains("a fast run lives in its worker's memory"), "{reason}");
    for step in ["plain", "caught"] {
        let starts = settled.events_of(step).filter(|e| e.kind() == "node_started").count();
        anyhow::ensure!(starts == 1, "'{step}' started {starts} times; a fast run is never run again");
    }

    project.finish().await
}

#[tokio::test]
async fn a_fast_run_whose_worker_is_told_to_stop_is_carried_on_by_the_next_one() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("step_crash", disp.clone()).await?;
    let pid = project.id();

    let execution_id = run::start(&mut project).await?;
    run::wait_for_nodes_started(&disp, execution_id, &["plain", "caught"]).await?;
    let told = platform.stop_workers(&pid).await?;
    anyhow::ensure!(!told.is_empty(), "no worker was running the steps when the test stopped them");

    // Handed back, not lost: the next worker fails the two steps that were
    // cut short and the run goes on to its end like a durable one would.
    let settled = SettledRun::observe_within(&disp, execution_id, Duration::from_secs(240)).await?;
    settled.failed_with(WENT_AWAY)?;
    for step in ["plain", "caught"] {
        let starts = settled.events_of(step).filter(|e| e.kind() == "node_started").count();
        anyhow::ensure!(starts == 1, "'{step}' started {starts} times; a step cut short is not run again");
    }
    settled.assert_completed("caught")?;

    project.finish().await
}

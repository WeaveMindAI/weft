//! Platform: the worker dies while two steps are running, and neither runs
//! again. A step that was mid-body may have partly happened (an email sent,
//! a row written), so the worker that picks the run back up fails it with a
//! message saying so instead of starting it over. A node that catches its
//! failures hands this one to its wired `error` like any other.
//!
//! Shape: the `step_crash` fixture runs two `ExecPython` steps that sleep
//! for ten minutes. Once both have started, every worker of the project is
//! removed (a fake crash). The claim on the run lapses, a fresh worker
//! takes it, and the run ends failed: `plain` failed with the crash
//! message, `caught` completed with that message on `error`, `handler`
//! received it, and each step started exactly once.
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

    let execution_id = run::start(&mut project).await?;
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

//! Platform: the worker goes away while two nodes are running, and neither
//! runs again. A node that was mid-body may have partly happened (an email
//! sent, a row written), so the worker that picks the run back up fails it
//! with a message saying so instead of starting it over, and a node with
//! its `_error` wired hands this one there like any other.
//!
//! Shape: the `node_crash` fixture runs two `ExecPython` nodes that sleep
//! for ten minutes. Once both have started, the project's workers go away,
//! two ways:
//!
//!   - killed (a fake crash), in a durable run: the claim on the run
//!     lapses and a fresh worker takes it. The run ends failed: `plain`
//!     failed with the crash message, `caught` completed with that message
//!     on `_error`, `handler` received it.
//!   - told to stop (`SIGTERM`), in a fast run: the worker gives its runs
//!     a few seconds, then stops the nodes still running and hands every
//!     recorded run back. The next worker carries the run on from its
//!     record, fails those nodes the way it fails a crashed one, and the
//!     run ends failed.
//!
//! Either way each node started exactly once.
#![cfg(feature = "e2e")]

use std::time::Duration;

use weft_e2e::{ensure, platform::Platform, project::Project, run, SettledRun};

/// What the worker that picks the run back up says about a node the dead
/// one was running.
const WENT_AWAY: &str = "went away while it was running; it was not run again";

#[tokio::test]
async fn a_node_the_worker_died_in_is_failed_never_run_again() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("node_crash", disp.clone()).await?;
    let pid = project.id();

    let run_id = run::start_durable(&mut project).await?;
    run::wait_for_nodes_started(&disp, run_id, &["plain", "caught"]).await?;
    let killed = platform.kill_workers(&pid).await?;
    anyhow::ensure!(!killed.is_empty(), "no worker was running the nodes when the test removed them");

    // The dead worker's claim lapses before a fresh one takes the run.
    let settled = SettledRun::observe_within(&disp, run_id, Duration::from_secs(240)).await?;
    settled.failed_with(WENT_AWAY)?;

    for node in ["plain", "caught"] {
        let starts = settled.events_of(node).filter(|e| e.kind() == "node_started").count();
        anyhow::ensure!(starts == 1, "'{node}' started {starts} times; a node never runs twice by itself");
    }
    let plain_failure = settled
        .events_of("plain")
        .find(|e| e.kind() == "node_failed")
        .and_then(|e| e.str_field("error").map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("'plain' was not failed"))?;
    anyhow::ensure!(plain_failure.contains(WENT_AWAY), "{plain_failure}");

    // The caught one completed, its failure on `_error`, and the branch
    // reading `_error` ran with it.
    settled.assert_completed("caught")?;
    let caught = settled
        .input_of("handler")
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("'handler' never received the caught failure"))?;
    anyhow::ensure!(caught.contains(WENT_AWAY), "{caught}");

    project.finish().await
}

#[tokio::test]
async fn a_fast_run_whose_worker_is_told_to_stop_is_carried_on_by_the_next_one() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("node_crash", disp.clone()).await?;
    let pid = project.id();

    let run_id = run::start(&mut project).await?;
    run::wait_for_nodes_started(&disp, run_id, &["plain", "caught"]).await?;
    let told = platform.stop_workers(&pid).await?;
    anyhow::ensure!(!told.is_empty(), "no worker was running the nodes when the test stopped them");

    // Handed back, not lost: the next worker fails the two nodes that were
    // cut short and the run goes on to its end like a durable one would.
    let settled = SettledRun::observe_within(&disp, run_id, Duration::from_secs(240)).await?;
    settled.failed_with(WENT_AWAY)?;
    for node in ["plain", "caught"] {
        let starts = settled.events_of(node).filter(|e| e.kind() == "node_started").count();
        anyhow::ensure!(starts == 1, "'{node}' started {starts} times; a node cut short is not run again");
    }
    settled.assert_completed("caught")?;

    project.finish().await
}

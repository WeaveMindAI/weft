//! The lifetime of a streaming run with zero custom Rust (a bus behind a
//! route): a caller who hangs up on a feed that has gone quiet still ends it.
#![cfg(feature = "e2e")]

use weft_e2e::run::SettledRun;
use weft_e2e::{ensure, live, project::Project};

/// A feed that says one thing and then goes quiet, and a caller who hangs
/// up. The run behind the route is cancelled without the program writing
/// another byte: the worker watches the connection itself, and the
/// keepalive the framing ignores is the second way it finds out. Before
/// this, only a real write could discover the caller had left, so a
/// watched table nobody touched held its run open for ever, one per
/// connection.
#[tokio::test]
async fn a_caller_leaving_a_quiet_stream_ends_its_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::runs(&disp, &project.id()).await?;

    let first = live::stream_first_chunk_then_hang_up(&disp, &format!("{base}/quiet")).await?;
    assert_eq!(String::from_utf8_lossy(&first), "data: first\n\n");

    // The caller is gone and the program has nothing more to say. The run
    // must end anyway, and end as the caller's doing.
    let run_ids = weft_e2e::run::wait_for_triggered_runs(
        &disp,
        &project.id(),
        &before,
        1,
        std::time::Duration::from_secs(60),
    )
    .await?;
    let settled = SettledRun::observe_within(
        &disp,
        run_ids[0],
        std::time::Duration::from_secs(120),
    )
    .await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "the run should end with its caller, got {} ({:?})",
        settled.status,
        settled.cancel_reason()
    );

    project.finish().await
}

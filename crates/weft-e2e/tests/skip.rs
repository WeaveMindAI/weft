//! Null-propagation: a closed gate output makes the downstream node skip.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn gate_false_skips_downstream() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("skip", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    // The execution itself completes (a skip is not a failure).
    settled.completed()?;
    // The gate ran, but `out` was skipped (its input closed) and never started.
    settled.assert_skipped("out")?;

    project.finish().await
}

//! Branching: a node told not to flow does not run, and everything
//! behind it closes in turn.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_false_should_flow_skips_the_node_and_everything_behind_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("skip", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    // The execution itself completes (a skip is not a failure).
    settled.completed()?;
    // The guarded node was told not to run, and the node reading its
    // output closed in turn: neither ever started.
    settled.assert_skipped("blocked")?;
    settled.assert_skipped("out")?;

    project.finish().await
}

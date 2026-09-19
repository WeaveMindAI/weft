//! Branching, both ways round: a node told not to flow does not run and
//! everything behind it closes in turn, and a node gated on
//! `_should_not_flow` runs on the CLOSURE of what it watches.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_gate_decides_both_ways_round() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("skip", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    // The execution itself completes (a skip is not a failure).
    settled.completed()?;
    // The guarded node was told not to run, and the node reading its
    // output closed in turn: neither ever started.
    settled.assert_skipped("blocked")?;
    settled.assert_skipped("out")?;

    // The gate read the other way round. `missing` never arrived, so
    // it closed, and the closure is what RAN the node behind it: the
    // one place in the language where a closure starts a node instead
    // of stopping one.
    settled.assert_completed("instead")?;
    settled.assert_input("told", "data", &serde_json::json!("the picture never came"))?;

    // And the mirror: a value did arrive on `value`, so the node gated
    // on its absence stayed off, with its own reason so the inspector
    // can say which of the two gates decided.
    settled.assert_skipped("never")?;
    settled.assert_skip_reason("never", "did_flow")?;
    settled.assert_skipped("quiet")?;

    project.finish().await
}

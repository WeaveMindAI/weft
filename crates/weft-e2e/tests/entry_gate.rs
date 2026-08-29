//! An ENTRY node turned off by a written `_should_flow: false`.
//!
//! Entry nodes have no incoming wires, so the wired-guard path
//! (tests/skip.rs) never exercises this decision: it is read from the
//! node's own braces where its kick is synthesized. The regression this
//! pins: the gate used to be silently ignored on entry nodes, so a node
//! its author turned off ran anyway.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_false_should_flow_literal_turns_off_an_entry_node() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("entry_gate", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    // A branch turned off is not a failure: the run completes.
    settled.completed()?;
    // The entry node is skipped as a DECISION (`did_not_flow`, its
    // author said no), exactly once, and never produced a value.
    settled.assert_skip_reason("gated", "did_not_flow")?;
    // Its consumer closes in turn, as a CONSEQUENCE: the input it
    // needed can never arrive. The two reasons rendering differently
    // is what lets a user tell "I turned this off" from "something
    // upstream turned this off".
    settled.assert_skip_reason("out", "required_input_closed")?;

    project.finish().await
}

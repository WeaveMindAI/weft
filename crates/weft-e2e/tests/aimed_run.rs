//! Aimed runs: `weft run --target <output>` executes only the target's
//! upstream closure, and holds that boundary honestly in the journal.
//!
//! The fixture fans one source into two output branches plus a leaf
//! that feeds no output (see fixtures/aimed_run/main.weft). Three
//! contracts are pinned across two runs of the same project:
//!
//!  - untargeted: pulses run everything they reach, the non-output
//!    leaf included, and nothing skips;
//!  - aimed: the sibling branch's first node (fed directly by an
//!    in-scope producer) journals exactly ONE skip whose reason is
//!    `outside_this_run`, and nodes past that boundary stay blank;
//!  - aiming at a non-output (or unknown) node is refused before
//!    anything runs.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn an_untargeted_run_reaches_everything_the_wiring_does() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("aimed_run", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Both output branches AND the side-effect leaf ran: an untargeted
    // run behaves like a fire, pulses go wherever the wiring takes
    // them, is_output or not.
    settled.assert_completed("left")?;
    settled.assert_completed("out_left")?;
    settled.assert_completed("right")?;
    settled.assert_completed("out_right")?;
    settled.assert_completed("side")?;

    project.finish().await
}

#[tokio::test]
async fn an_aimed_run_holds_its_boundary_and_says_why() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("aimed_run", disp).await?;

    let settled = run::run_targeted_and_settle(&mut project, &["out_left"]).await?;
    settled.completed()?;
    // The aimed branch runs whole.
    settled.assert_completed("left")?;
    settled.assert_completed("out_left")?;
    // The boundary nodes (fed directly by the in-scope source) journal
    // exactly one skip each, with the reason a user can act on.
    settled.assert_skip_reason("right", "outside_this_run")?;
    settled.assert_skip_reason("side", "outside_this_run")?;
    // Past the boundary, NOTHING: no cascade is manufactured for a
    // region that was never going to run, so the inspector shows the
    // boundary row and blank beyond, not a wall of skips.
    settled.assert_untouched("out_right")?;

    project.finish().await
}

#[tokio::test]
async fn aiming_at_a_non_output_or_unknown_node_is_refused() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("aimed_run", disp).await?;

    // `right` exists but is not an output node; `nope` does not exist.
    // Both must be refused by the dispatcher before anything runs, with
    // the message naming the problem. The refused run still built and
    // registered the project on its way in, so mark it for teardown.
    project.mark_registered();
    for (target, expect) in [("right", "not an output node"), ("nope", "no node")] {
        let err = project
            .weft(&["run", "--json", "--target", target])
            .await
            .expect_err("an invalid target must refuse the run");
        let msg = format!("{err:#}");
        anyhow::ensure!(msg.contains(expect), "target '{target}': {msg}");
    }

    project.finish().await
}

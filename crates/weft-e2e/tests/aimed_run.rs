//! Aimed runs: `weft run --target <node>` runs the target and what it
//! needs, its upstream closure, and nothing else: the run is held to
//! that set, so a root shared with another branch never drags the
//! branch in.
//!
//! The fixture holds two independent chains and a leaf hanging off the
//! first (see fixtures/aimed_run/main.weft). Three contracts are pinned:
//!
//!  - untargeted: every root is kicked and everything runs;
//!  - aimed at a chain's end: that chain's root is the only kick, the
//!    chain runs up to the target, the leaf hanging off it is never
//!    touched (it is downstream of the chain, not upstream of the
//!    target), and neither is the other chain (no skip row, nothing);
//!  - any node is a target: aiming at a node in the middle of a chain
//!    runs that node and its upstream only, while a name that is no
//!    node is refused before anything runs.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn an_untargeted_run_kicks_every_root() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("aimed_run", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_completed("left_src")?;
    settled.assert_completed("left")?;
    settled.assert_completed("out_left")?;
    settled.assert_completed("side")?;
    settled.assert_completed("right_src")?;
    settled.assert_completed("right")?;
    settled.assert_completed("out_right")?;

    project.finish().await
}

#[tokio::test]
async fn an_aimed_run_kicks_only_what_the_target_needs() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("aimed_run", disp).await?;

    let settled = run::run_targeted_and_settle(&mut project, &["out_left"]).await?;
    settled.completed()?;
    // The chain runs up to the target. The leaf hanging off `left` is
    // not upstream of the target, so the pulse into it is absorbed and
    // it never appears: an aimed run is what the target needs, not
    // what its roots happen to reach.
    // The target's ROOT is what gets kicked: `left_src` ran, and
    // `left` read its value rather than being kicked bare.
    settled.assert_completed("left_src")?;
    settled.assert_completed("left")?;
    settled.assert_completed("out_left")?;
    settled.assert_untouched("side")?;
    // The right chain's root was never kicked, so the chain is blank:
    // no skip row is manufactured for a region that never started.
    settled.assert_untouched("right_src")?;
    settled.assert_untouched("right")?;
    settled.assert_untouched("out_right")?;

    project.finish().await
}

#[tokio::test]
async fn any_node_is_a_target_and_an_unknown_name_is_refused() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("aimed_run", disp).await?;

    // A node in the middle of a chain: its root is kicked, it runs,
    // and nothing past it does: the target is the end of the run.
    let settled = run::run_targeted_and_settle(&mut project, &["left"]).await?;
    settled.completed()?;
    settled.assert_completed("left_src")?;
    settled.assert_completed("left")?;
    settled.assert_untouched("out_left")?;
    settled.assert_untouched("side")?;
    settled.assert_untouched("right_src")?;
    settled.assert_untouched("right")?;

    // A name that is no node is refused by the dispatcher before anything
    // runs, with the message naming it.
    let err = project
        .weft(&["run", "--json", "--target", "nope"])
        .await
        .expect_err("an unknown target must refuse the run");
    let msg = format!("{err:#}");
    anyhow::ensure!(msg.contains("no node 'nope'"), "{msg}");

    project.finish().await
}

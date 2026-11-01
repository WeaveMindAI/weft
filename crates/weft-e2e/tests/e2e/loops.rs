//! Loop: a sequential map over a LIST, doubling each element (the
//! stream-driven loop path is gated by `stream_rows`).
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn sequential_map_doubles_each_element() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("loop_map", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Five list elements -> 5 iterations.
    settled.assert_loop_iterations("doubler", 5)?;
    // The assembled list reaches Debug's `data` input. A weft Number is
    // one type whatever its JSON spelling, and the rig compares numbers
    // by value, so this passes whether the producer emitted `0` (this
    // fixture's Python source) or `0.0` (a Rust node).
    settled.assert_input("out", "data", &json!([0, 2, 4, 6, 8]))?;
    // The unwired `factor` inside the loop fired once per iteration and
    // never once more at the loop's own level (which left `double` holding
    // a `by` with no `n`, and the run stuck, before the fix).
    for node in ["doubler.factor", "doubler.double"] {
        let fired = settled.node_outputs(node).len();
        anyhow::ensure!(fired == 5, "{node} fired {fired} times, expected one per iteration");
    }

    project.finish().await
}

/// A loop with no output port ends, over items and over an empty list:
/// nothing writes into its end, which used to start once at the loop's
/// own level, wait on an iteration that never opens there, and leave the
/// run stuck (`... never started, waiting on its scope to open`).
#[tokio::test]
async fn a_loop_with_no_output_ends_over_items_and_over_nothing() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("loop_map", disp).await?;
    for (items, iterations) in [("[1, 2]", 2), ("[]", 0)] {
        project.set_main(&format!(
            "src = ExecPython() -> (items: List[Number]) {{\n  code: \"return {{'items': {items}}}\"\n}}\n\
             each = Loop(values: List[Number]) -> () {{\n  over: [\"values\"]\n  log = Debug\n  log.data = self.values\n}}\n\
             each.values = src.items\n"
        ))?;
        let settled = run::run_and_settle(&mut project).await?;
        settled.completed()?;
        settled.assert_loop_iterations("each", iterations)?;
        let logged = settled.events_of("each.log").filter(|e| e.kind() == "node_completed").count();
        anyhow::ensure!(logged == iterations, "the body ran once per item: {logged} for {items}");
    }
    project.finish().await
}

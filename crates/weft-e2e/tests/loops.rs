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

    project.finish().await
}

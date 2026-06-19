//! Loop: a sequential map over a Range, doubling each element.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn sequential_map_doubles_each_element() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("loop_map", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Range { to: 5 } -> 5 iterations.
    settled.assert_loop_iterations("doubler", 5)?;
    // The assembled list reaches Debug's `data` input. weft Numbers are f64, so
    // the JSON carries floats (0.0, not 0).
    settled.assert_input("out", "data", &json!([0.0, 2.0, 4.0, 6.0, 8.0]))?;

    project.finish().await
}

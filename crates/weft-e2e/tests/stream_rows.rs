//! Layer-4 gate for Generator[T]: a producer walks its rows in ONE
//! body, yields only the kept ones (each yield waiting for its pull),
//! and a sequential loop does per-row work and gathers. Pins the
//! producer-cursor-across-iterations shape end to end.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_row_feed_streams_kept_rows_into_a_sequential_loop() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("stream_rows", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // Three kept rows (the two '#' rows are skipped by the producer's
    // own decision, not by list slicing) -> three loop iterations.
    settled.assert_loop_iterations("work", 3)?;
    settled.assert_input("sink", "data", &json!(["ALPHA", "BETA", "GAMMA"]))?;

    project.finish().await
}

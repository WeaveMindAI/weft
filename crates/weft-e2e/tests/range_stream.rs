//! Range driving a stream-driven loop end to end: the catalog's
//! count-based producer yields one number per pull and the loop
//! doubles each (`loop_map` covers the list-driven path; this is the
//! pull-driven one with a catalog producer).
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn range_streams_one_number_per_iteration() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("range_stream", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Range { to: 5 } yields 0..5 -> 5 iterations.
    settled.assert_loop_iterations("doubler", 5)?;
    settled.assert_input("out", "data", &json!([0.0, 2.0, 4.0, 6.0, 8.0]))?;

    project.finish().await
}

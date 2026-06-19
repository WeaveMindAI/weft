//! A project-local custom node compiles into the worker and runs.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn custom_node_compiles_and_runs() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("custom_node", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // 6 * 7 = 42 reaches Debug (Numbers are f64, so 42.0).
    settled.assert_input("out", "data", &json!(42.0))?;

    project.finish().await
}

//! Plain run: a literal flows into Debug. Proves the run + assert core end to
//! end against a live cluster.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn plain_text_reaches_debug() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("plain", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Debug is a sink (no output); assert the value arrived on its `data` input.
    settled.assert_input("out", "data", &json!("hello weft"))?;

    project.finish().await
}

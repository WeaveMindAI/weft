//! A project whose nodes are all base catalog nodes runs on the standard
//! worker the install made ready when it started, so its first run
//! compiles nothing, and its value arrives where it is wired.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_stock_project_runs_on_the_ready_standard_worker() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let install = disp.install().clone();
    let mut project = Project::prepare("plain", disp).await?;
    let record = weft_core::images::ImageRefs::load(&install.dir())?
        .ok_or_else(|| anyhow::anyhow!("the install recorded no images when it started"))?;
    let hash = record.standard_worker.rsplit(':').next().unwrap_or_default().to_string();
    let program = weft_platform_local::images::Program::Worker(&hash).file(&weft_platform_local::MachineLayout::here().workers());
    anyhow::ensure!(program.is_file(), "the install made the standard worker {} ready when it started", program.display());

    let out = project.weft(&["build"]).await?;
    anyhow::ensure!(out.contains(&format!("on worker {}", &hash[..16])), "a stock project builds to the standard worker: {out}");
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Debug is a sink (no output); the value arrived on its `data` input.
    settled.assert_input("out", "data", &json!("hello weft"))?;
    project.finish().await
}

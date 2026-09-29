//! A project whose nodes are all base catalog nodes runs on the standard
//! worker the install made ready when it started, so its first run
//! compiles nothing.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_stock_project_runs_on_the_ready_standard_worker() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("plain", disp).await?;
    let printed = project.weft(&["build-images", "--print"]).await?;
    let standard = printed
        .lines()
        .find(|l| l.contains("weft-worker:"))
        .ok_or_else(|| anyhow::anyhow!("build-images --print names no standard worker: {printed}"))?;
    let hash = standard.rsplit(':').next().unwrap_or_default().to_string();
    let bare = format!("weft-worker:{hash}");
    let present = tokio::process::Command::new("docker").args(["image", "inspect", &bare]).output().await?;
    anyhow::ensure!(present.status.success(), "the install made {bare} ready when it started");

    let out = project.weft(&["build"]).await?;
    anyhow::ensure!(out.contains(&format!("on worker image {}", &hash[..16])), "a stock project builds to the standard worker: {out}");
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    project.finish().await
}

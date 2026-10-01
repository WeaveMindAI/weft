//! An infra node that asks for a GPU gets one: the container lists the
//! host's GPU, and terminating it removes it.
//!
//! Needs a machine whose Docker has the NVIDIA runtime; skips elsewhere,
//! saying so.
#![cfg(feature = "e2e")]

use weft_e2e::platform::{Platform, Role};
use weft_e2e::{ensure, infra, project::Project, run};

/// Whether this machine's Docker can hand a container a GPU.
async fn docker_has_gpus() -> anyhow::Result<bool> {
    let out = tokio::process::Command::new("docker").args(["info", "--format", "{{json .Runtimes}}"]).output().await?;
    anyhow::ensure!(out.status.success(), "docker info failed: {}", String::from_utf8_lossy(&out.stderr));
    Ok(String::from_utf8_lossy(&out.stdout).contains("\"nvidia\""))
}

#[tokio::test]
async fn an_infra_node_that_asks_for_a_gpu_sees_it() -> anyhow::Result<()> {
    if !docker_has_gpus().await? {
        eprintln!("SKIP: this machine's Docker has no NVIDIA runtime (install the NVIDIA container toolkit)");
        return Ok(());
    }
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("infra_gpu", disp).await?;

    infra::start_and_wait_running(&mut project, "gpu").await?;
    let pid = project.id();
    anyhow::ensure!(!platform.containers_for_project(&pid, Role::Infra).await?.is_empty(), "the unit runs");

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    let input = settled.input_of("out").ok_or_else(|| anyhow::anyhow!("the Debug node never ran"))?;
    let seen = input.get("data").and_then(|v| v.as_str()).unwrap_or_default();
    anyhow::ensure!(seen.starts_with("GPU 0:"), "the container sees no GPU: {seen:?}");

    infra::terminate_and_wait_gone(&project, "gpu").await?;
    let left = platform.containers_for_project(&pid, Role::Infra).await?;
    anyhow::ensure!(left.is_empty(), "terminating removes the unit, left: {left:?}");
    project.finish().await
}

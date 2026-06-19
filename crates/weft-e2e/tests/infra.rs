//! Infra lifecycle: provision a minimal sidecar, run against it, terminate, and
//! assert cleanup.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, infra, project::Project, run};

#[tokio::test]
async fn infra_node_provisions_runs_and_terminates() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_min", disp).await?;

    // Provision the infra and wait until the sidecar reports running. This
    // builds the sidecar image, applies the manifests, and waits for the pod's
    // readiness probe.
    let endpoint = infra::start_and_wait_running(&mut project, "svc").await?;
    eprintln!("mini_service endpoint: {endpoint}");

    // With infra running, a run resolves the endpoint, reads /outputs, and emits
    // status="ready" to Debug.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;

    // Terminate and assert the node is actually gone (cleanup happened).
    infra::terminate_and_wait_gone(&project, "svc").await?;

    project.finish().await
}

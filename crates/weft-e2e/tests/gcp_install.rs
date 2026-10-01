//! A real install on GCP, end to end. By hand only: it deploys to a live
//! cloud and spends money there (a Cloud Build, a Cloud Run worker, a
//! Compute Engine machine for a minute or two).
//!
//! It needs an install made by the "install on GCP" workflow, and:
//!
//!   WEFT_E2E_GCP_URL           the install's address (`https://<ip>`)
//!   WEFT_E2E_GCP_OPERATOR_KEY  an operator key for it
//!   WEFT_E2E_GCP_PROJECT       the GCP project it runs in
//!   WEFT_E2E_GCP_REGION        its region
//!
//! and `weft login` done for that address on this machine, since the CLI
//! the rig drives finds its key there. It skips when a variable is unset.
//!
//! What it proves: a project deploys (the install builds it on Cloud
//! Build), a route answers through the machine's front door, an infra
//! node comes up on its own machine and goes away, and the program's
//! worker service is left able to scale to zero, so an idle project
//! costs nothing.
#![cfg(feature = "e2e")]

use std::sync::Arc;

use serde_json::json;
use weft_e2e::client::{AuthProvider, Dispatcher};
use weft_e2e::{ensure, infra, live, project::Project};

struct OperatorKey(String);

impl AuthProvider for OperatorKey {
    fn authorization(&self) -> Option<String> {
        Some(format!("Bearer {}", self.0))
    }
}

#[tokio::test]
async fn a_project_runs_on_a_real_gcp_install_and_idles_at_zero() -> anyhow::Result<()> {
    let Some(env) = ensure::env_group_or_skip(
        "gcp",
        &["WEFT_E2E_GCP_URL", "WEFT_E2E_GCP_OPERATOR_KEY", "WEFT_E2E_GCP_PROJECT", "WEFT_E2E_GCP_REGION"],
    ) else {
        return Ok(());
    };
    let [url, key, gcp_project, region] = <[String; 4]>::try_from(env).expect("four vars requested");
    let disp = Dispatcher::for_install(&url, weft_core::infra::Install::default_install())?
        .with_auth(Arc::new(OperatorKey(key)));
    ensure::wait_healthy(&disp).await?;

    // A route, through the front door.
    let mut project = Project::prepare("web_trigger", disp.clone()).await?;
    let path = project.unique_live_path()?;
    project.activate().await?;
    let bytes = live::http_post(&disp, &path, &json!({ "message": "weft-e2e-gcp" })).await?;
    let body = String::from_utf8_lossy(&bytes);
    anyhow::ensure!(body.contains("\"port_said\":\"weft-e2e-gcp\""), "the route did not answer through the install: {body}");

    // The worker service scales to zero when nobody calls.
    let pid = project.id();
    let out = tokio::process::Command::new("gcloud")
        .args([
            "run",
            "services",
            "list",
            "--project",
            &gcp_project,
            "--region",
            &region,
            "--filter",
            &format!("metadata.labels.weft-project={}", pid.simple()),
            "--format",
            "value(spec.template.metadata.annotations['autoscaling.knative.dev/minScale'])",
        ])
        .output()
        .await?;
    anyhow::ensure!(out.status.success(), "gcloud run services list: {}", String::from_utf8_lossy(&out.stderr));
    let min_scales = String::from_utf8_lossy(&out.stdout).trim().to_string();
    anyhow::ensure!(!min_scales.is_empty(), "no Cloud Run service carries the project's label {pid}");
    anyhow::ensure!(
        min_scales.lines().all(|m| m.is_empty() || m == "0"),
        "an idle project must keep no worker warm by default, found min scale {min_scales}"
    );
    project.finish().await?;

    // An infra node on a machine of its own, up and gone.
    let mut infra_project = Project::prepare("infra_min", disp).await?;
    let endpoint = infra::start_and_wait_running(&mut infra_project, "svc").await?;
    eprintln!("mini_service endpoint on GCP: {endpoint}");
    infra::terminate_and_wait_gone(&infra_project, "svc").await?;
    infra_project.finish().await
}

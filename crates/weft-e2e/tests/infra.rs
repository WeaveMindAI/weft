//! Infra lifecycle: provision a minimal sidecar, run against it, terminate, and
//! assert cleanup.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, infra, project::Project, run, SettledRun};

#[tokio::test]
async fn infra_node_provisions_runs_and_terminates() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_min", disp).await?;
    project.write_file("src/main.weft", r#"
scope = Group() -> (status: String) {
  enabled = Text { value: "true" }
  ready = Cast -> (value: Boolean) { value: enabled.value }
  svc = MiniService { _should_flow: ready.value }
  after = Debug
  after.data = svc.status
  unrelated = Text { value: "must not run during preparation" }
  self.status = svc.status
}
out = Debug
out.data = scope.status
"#)?;

    // Provision the infra and wait until the sidecar reports running. This
    // builds the sidecar image, applies the manifests, and waits for the pod's
    // readiness probe.
    let endpoint = infra::start_and_wait_running(&mut project, "scope.svc").await?;
    project.mark_registered();
    eprintln!("mini_service endpoint: {endpoint}");
    let setups = run::execution_colors(project.dispatcher(), &project.id()).await?;
    anyhow::ensure!(setups.len() == 1, "one infra setup, got {setups:?}");
    SettledRun::observe(project.dispatcher(), *setups.iter().next().unwrap()).await?.completed()?
        .assert_completed("scope.enabled")?.assert_completed("scope.ready")?.assert_completed("scope.svc")?
        .assert_untouched("scope.after")?.assert_untouched("scope.unrelated")?.assert_untouched("out")?;

    // With infra running, a run resolves the endpoint, reads /outputs, and emits
    // status="ready" to Debug.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;

    // Terminate and assert the node is actually gone (cleanup happened).
    infra::terminate_and_wait_gone(&project, "scope.svc").await?;
    project.write_file("src/main.weft", r#"
scope = Loop(values: List[Number]) -> (statuses: List[String | Null]) {
  over: ["values"]
  svc = MiniService
  self.statuses = svc.status
}
scope.values = [1]
"#)?;
    let refused = project.weft_refused(&["infra", "start"]).await?;
    anyhow::ensure!(refused.contains("inside a Loop"), "{refused}");

    project.finish().await
}

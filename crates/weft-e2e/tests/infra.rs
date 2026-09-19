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

/// A door: one endpoint of an infra node made reachable from the
/// machine the runtime runs on, so a client that is not a weft node can
/// speak to it directly.
///
/// The whole point of the shape is that the node decides. There is no
/// command that opens a door and nothing in a project's source reaches
/// past what the node's spec declared, so this drives it the only way
/// there is: through the input the node's author exposed, and it checks
/// the cluster actually followed both ways.
#[tokio::test]
async fn an_infra_node_is_reachable_from_this_machine_when_its_own_input_says_so(
) -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_min", disp).await?;

    // Off (the default): the endpoint answers inside the cluster only,
    // and there is no door to list.
    project.write_file("src/main.weft", r#"
svc = MiniService
out = Debug
out.data = svc.status
"#)?;
    infra::start_and_wait_running(&mut project, "svc").await?;
    project.mark_registered();
    let none = project.weft(&["infra", "list-doors"]).await?;
    anyhow::ensure!(none.contains("no doors"), "nothing is reachable by default: {none}");

    // On: the same node, same program, one input flipped.
    project.write_file("src/main.weft", r#"
svc = MiniService { reachable: true }
out = Debug
out.data = svc.status
"#)?;
    project.weft(&["infra", "upgrade", "--mode", "wipe"]).await?;

    let listed: serde_json::Value =
        serde_json::from_str(project.weft(&["infra", "list-doors", "--json"]).await?.trim())?;
    let doors = listed["doors"].as_array().cloned().unwrap_or_default();
    anyhow::ensure!(doors.len() == 1, "one door, on the endpoint the node named: {listed}");
    anyhow::ensure!(doors[0]["node"] == json!("svc"), "{listed}");
    anyhow::ensure!(doors[0]["endpoint"] == json!("api"), "{listed}");
    let port = doors[0]["port"].as_u64().unwrap_or(0);
    anyhow::ensure!(port != 0, "the runtime reports the port it was given: {listed}");

    // The door carries the service's own protocol, so the sidecar's own
    // route answers through it. This is the whole promise: a client that
    // is not a weft node talking to the program's infrastructure.
    let body = reqwest::get(format!("http://127.0.0.1:{port}/outputs"))
        .await?
        .error_for_status()?
        .text()
        .await?;
    anyhow::ensure!(body.contains("ready"), "the sidecar answered through the door: {body}");

    // And back off: a door is not a one-way door.
    project.write_file("src/main.weft", r#"
svc = MiniService
out = Debug
out.data = svc.status
"#)?;
    project.weft(&["infra", "upgrade", "--mode", "wipe"]).await?;
    let closed = project.weft(&["infra", "list-doors"]).await?;
    anyhow::ensure!(closed.contains("no doors"), "the input closed it again: {closed}");

    infra::terminate_and_wait_gone(&project, "svc").await?;
    project.finish().await
}

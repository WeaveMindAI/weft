//! Infra lifecycle: provision a minimal sidecar, run against it, terminate, and
//! assert cleanup.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::platform::{Platform, Role};
use weft_e2e::{display, ensure, infra, project::Project, run, SettledRun};

#[tokio::test]
async fn infra_node_provisions_runs_and_terminates() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
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
    // builds the sidecar image, starts its containers, and waits for its
    // readiness probe.
    let endpoint = infra::start_and_wait_running(&mut project, "scope.svc").await?;
    eprintln!("mini_service endpoint: {endpoint}");
    let pid = project.id();
    let running = platform.containers_for_project(&pid, Role::Infra).await?;
    anyhow::ensure!(!running.is_empty(), "a running infra node has containers on this machine");
    let setups = run::executions(project.dispatcher(), &project.id()).await?;
    anyhow::ensure!(setups.len() == 1, "one infra setup, got {setups:?}");
    SettledRun::observe(project.dispatcher(), *setups.iter().next().unwrap()).await?.completed()?
        .assert_completed("scope.enabled")?.assert_completed("scope.ready")?.assert_completed("scope.svc")?
        .assert_untouched("scope.after")?.assert_untouched("scope.unrelated")?.assert_untouched("out")?;

    // An infra node has a display only when its metadata NAMES the
    // endpoint serving `/live`. MiniService names none, so it has
    // nothing to show, and the doors say so the same way they would
    // for a node that does not exist. A node that speaks only TCP is
    // the real case: polling one would 502 every tick.
    let watcher =
        display::mint_display_token(project.dispatcher(), &pid, "weft-e2e-all", &[], true).await?;
    let listed = display::list_for_token(project.dispatcher(), &watcher).await?;
    // The listing carries every node that HAS a display; this project's
    // only infra node has none, and it declares no trigger, so there is
    // nothing to carry.
    anyhow::ensure!(
        listed.is_empty(),
        "an infra node with no liveEndpoint must not be listed as showing anything, got {listed:?}"
    );
    let status = display::read_status(project.dispatcher(), &watcher, &pid, "scope.svc").await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::NOT_FOUND,
        "reading a display that does not exist must be 404, got {status}"
    );

    // With infra running, a run resolves the endpoint, reads /outputs, and emits
    // status="ready" to Debug.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;

    // Terminate and assert the node is actually gone (cleanup happened).
    infra::terminate_and_wait_gone(&project, "scope.svc").await?;
    let left = platform.containers_for_project(&pid, Role::Infra).await?;
    anyhow::ensure!(left.is_empty(), "terminating removes the node's containers, left: {left:?}");
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
/// the install actually followed both ways.
#[tokio::test]
async fn an_infra_node_is_reachable_from_this_machine_when_its_own_input_says_so(
) -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_min", disp).await?;

    // Off (the default): the endpoint answers inside the install only,
    // and there is no door to list.
    project.write_file("src/main.weft", r#"
svc = MiniService
out = Debug
out.data = svc.status
"#)?;
    infra::start_and_wait_running(&mut project, "svc").await?;
    let none = project.weft(&["infra", "list-doors"]).await?;
    anyhow::ensure!(none.contains("no doors"), "nothing is reachable by default: {none}");

    // On: the same node, same program, one input flipped.
    project.write_file("src/main.weft", r#"
svc = MiniService { reachable: true }
out = Debug
out.data = svc.status
"#)?;
    project.weft(&["infra", "upgrade", "--mode", "wipe"]).await?;

    let listed: weft_core::infra::wire::DoorsResponse =
        serde_json::from_str(project.weft(&["infra", "list-doors", "--json"]).await?.trim())?;
    anyhow::ensure!(listed.doors.len() == 1, "one door, on the endpoint the node named: {listed:?}");
    anyhow::ensure!(listed.doors[0].copy.node == "svc", "{listed:?}");
    anyhow::ensure!(listed.doors[0].endpoint == "api", "{listed:?}");
    let address = &listed.doors[0].address;

    // The door carries the service's own protocol, so the sidecar's own
    // route answers through it. This is the whole promise: a client that
    // is not a weft node talking to the program's infrastructure.
    let body = reqwest::get(format!("http://{address}/outputs"))
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

/// An endpoint its node marks public answers through the install's own
/// address, at `/infra/<project>/<instance>/<path>`, with the prefix taken
/// off on the way to the unit.
#[tokio::test]
async fn a_public_infra_endpoint_answers_at_the_installs_door() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_min", disp.clone()).await?;
    project.write_file("src/main.weft", r#"
svc = MiniService { public: true }
out = Debug
out.data = svc.status
"#)?;
    infra::start_and_wait_running(&mut project, "svc").await?;

    let status: serde_json::Value = disp.get_json(&format!("/projects/{}/infra/status", project.id())).await?;
    let address = status["nodes"]
        .as_array()
        .and_then(|n| n.iter().find_map(|n| n["public_urls"]["api"].as_str().map(str::to_string)))
        .ok_or_else(|| anyhow::anyhow!("the public endpoint names its address: {status}"))?;
    let path = &address[address.find("/infra/").ok_or_else(|| anyhow::anyhow!("not a door address: {address}"))?..];
    anyhow::ensure!(path.ends_with("/svc"), "the node's own path closes the address: {address}");
    let body = reqwest::get(format!("{}{path}/outputs", disp.base().trim_end_matches('/')))
        .await?
        .error_for_status()?
        .text()
        .await?;
    anyhow::ensure!(body.contains("ready"), "the sidecar answered through the install's door: {body}");

    infra::terminate_and_wait_gone(&project, "svc").await?;
    project.finish().await
}

/// A unit whose service crashes comes back on its own, and a run reaches
/// it again afterwards.
#[tokio::test]
async fn a_unit_whose_service_crashes_comes_back() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_min", disp).await?;
    project.write_file("src/main.weft", r#"
svc = MiniService { reachable: true }
out = Debug
out.data = svc.status
"#)?;
    infra::start_and_wait_running(&mut project, "svc").await?;
    let listed: weft_core::infra::wire::DoorsResponse = serde_json::from_str(project.weft(&["infra", "list-doors", "--json"]).await?.trim())?;
    let door = format!("http://{}", listed.doors.first().ok_or_else(|| anyhow::anyhow!("the sidecar's door: {listed:?}"))?.address);

    // The service dies mid-request, so the call itself fails.
    anyhow::ensure!(reqwest::get(format!("{door}/crash")).await.is_err(), "the crash answered");
    weft_e2e::poll_until("the crashed service to answer again", std::time::Duration::from_secs(120), std::time::Duration::from_secs(1), || {
        let url = format!("{door}/outputs");
        async move { Ok(reqwest::get(url).await.ok().filter(|r| r.status().is_success()).map(|_| ())) }
    })
    .await?;
    infra::wait_running(&project, "svc").await?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;

    infra::terminate_and_wait_gone(&project, "svc").await?;
    project.finish().await
}

/// A node waits on a long job in its own container without holding a
/// worker: it starts the job, parks on a poll of the job's status route
/// at the endpoint's worker address, and the listener does the polling.
/// The container says "running" twice before "done", so the result
/// reaching Debug proves the listener reached the unit (through the
/// address weft's own roles use, which on a local install is a loopback
/// port rather than the Docker name the workers use) and resumed the run
/// on the answer that passed the filter.
#[tokio::test]
async fn a_node_parks_on_a_long_job_in_its_own_infra() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_job", disp).await?;
    infra::start_and_wait_running(&mut project, "svc").await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    let result = settled.input_of("out").and_then(|i| i.get("data").cloned());
    anyhow::ensure!(
        result.as_ref().and_then(|v| v.as_str()).is_some_and(|s| s.starts_with("rendered ")),
        "the finished job's result reached Debug, got {result:?}"
    );

    infra::terminate_and_wait_gone(&project, "svc").await?;
    project.finish().await
}

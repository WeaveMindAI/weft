//! Layer-4: an infra node inside a file included twice is two INSTANCES.
//! The `include_twice_infra` fixture includes a file holding a
//! MiniService at the sites `one` and `two`; starting infra provisions
//! two sidecars with two rows, two endpoints and two status lines, each
//! call's nodes reach their own call's instance, one instance is stopped
//! on its own by its place, and terminate takes both down.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::client::poll_until;
use weft_e2e::{ensure, infra, project::Project, run};

/// The status of one instance, by its place, or `None` when it has no row.
async fn status_of(project: &Project, place: &str) -> anyhow::Result<Option<String>> {
    let nodes = infra::status(project.dispatcher(), &project.id()).await?;
    Ok(nodes
        .iter()
        .find(|n| n.node() == Some(place))
        .and_then(|n| n.status().map(str::to_string)))
}

#[tokio::test]
async fn a_file_included_twice_provisions_its_infra_once_per_call() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_twice_infra", disp).await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;

    // Two instances, two endpoints.
    let one = infra::start_and_wait_running(&mut project, "one.svc").await?;
    let two = infra::wait_running(&project, "two.svc").await?;
    anyhow::ensure!(one != two, "each call's instance has its own endpoint: {one} / {two}");
    let listed = infra::status(project.dispatcher(), &project.id()).await?;
    let mut places: Vec<&str> = listed.iter().filter_map(|n| n.node()).collect();
    places.sort();
    anyhow::ensure!(places == vec!["one.svc", "two.svc"], "one status line per instance: {listed:?}");
    anyhow::ensure!(
        !format!("{listed:?}").contains("@src:"),
        "the file's own path never reaches a reader: {listed:?}"
    );
    // The provisioning run is listed as started by a place too, not by
    // the file's node.
    let setups: serde_json::Value = project
        .dispatcher()
        .get_json(&format!("/executions?project_id={}&phase=infra_setup", project.id()))
        .await?;
    let entries: Vec<&str> = setups["executions"]
        .as_array()
        .map(|runs| runs.iter().filter_map(|r| r["entry_node"].as_str()).collect())
        .unwrap_or_default();
    anyhow::ensure!(
        !entries.is_empty() && entries.iter().all(|e| *e == "one.svc" || *e == "two.svc"),
        "an infra setup run is started by a place: {setups}"
    );

    // A run reaches each call's own instance, and each call's rows
    // carry that call's frame. Observed with the program in hand, so the
    // names below are read through their sites.
    let color = run::start(&mut project).await?;
    let settled = project.settled(color).await?;
    settled.completed()?;
    settled.assert_input("first", "data", &json!("ready"))?;
    settled.assert_input("second", "data", &json!("ready"))?;
    settled.assert_completed("one.after")?;
    settled.assert_completed("two.after")?;
    let frames_of = |spelled: &str| {
        settled.events_of(spelled).next().map(|e| e.frames().clone()).unwrap_or(serde_json::Value::Null)
    };
    anyhow::ensure!(frames_of("one.svc") == json!([{"site": "one"}]), "{}", frames_of("one.svc"));
    anyhow::ensure!(frames_of("two.svc") == json!([{"site": "two"}]), "{}", frames_of("two.svc"));

    // The per-node verbs take the place. A bare `svc` names nothing, and
    // the refusal spells the name that works.
    let refused = project.weft_refused(&["infra", "node-stop", "svc"]).await?;
    anyhow::ensure!(refused.contains("site.svc"), "it names the spelling that works: {refused}");
    project.weft(&["infra", "node-stop", "one.svc", "--force"]).await?;
    poll_until(
        "one.svc to be stopped while two.svc keeps running",
        Duration::from_secs(120),
        Duration::from_millis(750),
        || async {
            let one = status_of(&project, "one.svc").await?;
            let two = status_of(&project, "two.svc").await?;
            anyhow::ensure!(two.as_deref() == Some("running"), "stopping one call must not touch the other: {two:?}");
            Ok((one.as_deref() == Some("stopped")).then_some(()))
        },
    )
    .await?;

    // Terminate takes both instances down, and both rows with them.
    infra::terminate_and_wait_gone(&project, "one.svc").await?;
    infra::wait_gone(&project, "two.svc").await?;

    project.finish().await
}

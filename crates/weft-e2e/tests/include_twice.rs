//! Layer-4: one file included twice is two of everything. The
//! `include_twice` fixture includes `lib/part.weft` at the sites `one`
//! and `two`; the file holds a wait, a trigger and an access node. Each
//! is two PLACES in the program, named through its site, and every
//! surface that keys, lists or takes a node does so per place: a wait is
//! woken one call at a time, a trigger registers and shows its display
//! once per call, a display grant for one call opens that call alone,
//! and `weft connect` names the access node once per call. The
//! compiler's own key for a node of the file is refused wherever a
//! person types a name.
#![cfg(feature = "e2e")]

mod common;

use std::time::Duration;

use common::color_of;

use serde_json::{json, Value};
use weft_e2e::client::poll_until;
use weft_e2e::{display, ensure, run, project::Project};

/// The `waiting` list of a run, each entry's node spelling.
async fn waiting_on(disp: &weft_e2e::Dispatcher, color: uuid::Uuid) -> anyhow::Result<Vec<String>> {
    let detail: Value = disp.get_json(&format!("/executions/{color}")).await?;
    anyhow::ensure!(
        !detail.to_string().contains("@src:"),
        "the file's own path never reaches a reader: {detail}"
    );
    Ok(detail["waiting"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|w| w["node"].as_str().map(str::to_string))
        .collect())
}

/// One run parks on the wait of BOTH calls, each listed under its own
/// name; `weft wake` resolves one call's wait and leaves the other
/// parked; the second wake completes the run, and each call's rows
/// carry that call's frame.
#[tokio::test]
async fn each_call_holds_its_own_wait_and_is_woken_on_its_own() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("include_twice", disp.clone()).await?;

    let stdout = project.weft(&["run", "--json", "--target", "first", "--target", "second"]).await?;
    let held = color_of(&stdout)?;
    // `waiting_for_input` means ONE wait is registered, not both: each
    // call registers its own, and the second can land a moment after the
    // first. So the run is watched until both are in.
    run::wait_for_status(&disp, held, "waiting_for_input").await?;
    let disp_for_poll = disp.clone();
    poll_until(
        "two waits, one per call",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || {
            let disp = disp_for_poll.clone();
            async move {
                let mut waits = waiting_on(&disp, held).await?;
                waits.sort();
                Ok((waits == vec!["one.hold", "two.hold"]).then_some(()))
            }
        },
    )
    .await?;

    // The compiler's key resolves (it IS the node's id) and is refused
    // with the spelling that works.
    let refused = project.weft_refused(&["wake", &held.to_string(), "@src:lib:part.hold"]).await?;
    anyhow::ensure!(refused.contains("inside an included file"), "{refused}");
    anyhow::ensure!(refused.contains("site.hold"), "it names the spelling that works: {refused}");

    // Waking `one.hold` wakes that call alone: `two.hold` stays parked.
    let woke: Value =
        serde_json::from_str(project.weft(&["wake", &held.to_string(), "one.hold", "--json"]).await?.trim())?;
    anyhow::ensure!(woke["node"] == json!("one.hold"), "it answers in the same spelling: {woke}");
    let disp_for_poll = disp.clone();
    poll_until(
        "the run to be parked on two.hold alone",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || {
            let disp = disp_for_poll.clone();
            async move {
                let waits = waiting_on(&disp, held).await?;
                Ok((waits == vec!["two.hold"]).then_some(()))
            }
        },
    )
    .await?;
    let status = run::status_of(&disp, held).await?;
    anyhow::ensure!(status == "waiting_for_input", "still parked on the other call, got {status}");

    project.weft(&["wake", &held.to_string(), "two.hold"]).await?;
    let settled = project.settled(held).await?;
    settled.completed()?;
    settled.assert_input("first", "data", &json!("hello"))?;
    settled.assert_input("second", "data", &json!("hello"))?;
    // Each call's rows carry that call's frame and nothing else.
    let frames_of = |spelled: &str| {
        settled.events_of(spelled).next().map(|e| e.frames().clone()).unwrap_or(Value::Null)
    };
    anyhow::ensure!(frames_of("one.hold") == json!([{"site": "one"}]), "{}", frames_of("one.hold"));
    anyhow::ensure!(frames_of("two.hold") == json!([{"site": "two"}]), "{}", frames_of("two.hold"));

    project.finish().await
}

/// Activating registers the file's trigger once per call, each with its
/// own display under its own name. A display grant names one call, and
/// the compiler's key for the node opens nothing.
#[tokio::test]
async fn each_call_registers_its_own_trigger_with_its_own_display() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_twice", disp.clone()).await?;
    project.activate().await?;
    let pid = project.id();

    // The editor's door, by the place it walked into.
    for place in ["one.tick", "two.tick"] {
        let shown = display::as_editor(&disp, &pid, place).await?;
        anyhow::ensure!(
            shown.item("Fires").is_some(),
            "the trigger at {place} shows its schedule, got {:?}",
            shown.labels()
        );
    }
    let (status, _) = disp.get_raw(&format!("/projects/{pid}/signals/@src:lib:part.tick/live")).await?;
    anyhow::ensure!(status == 404, "the compiled id must open no display, got {status}");

    // The token door: every display, one per call, spelled per call.
    let all = display::mint_display_token(&disp, &pid, "weft-e2e-all", &[], true).await?;
    let listed = display::list_for_token(&disp, &all).await?;
    let mut nodes: Vec<&str> = listed.iter().filter_map(|e| e["node"].as_str()).collect();
    nodes.sort();
    anyhow::ensure!(nodes == vec!["one.tick", "two.tick"], "listing: {listed:?}");

    // A grant for one call opens that call and no other.
    let scoped = display::mint_display_token(&disp, &pid, "weft-e2e-one", &["one.tick"], false).await?;
    let listed = display::list_for_token(&disp, &scoped).await?;
    let nodes: Vec<&str> = listed.iter().filter_map(|e| e["node"].as_str()).collect();
    anyhow::ensure!(nodes == vec!["one.tick"], "listing: {listed:?}");
    anyhow::ensure!(display::as_token(&disp, &scoped, &pid, "one.tick").await?.item("Fires").is_some());
    let status = display::read_status(&disp, &scoped, &pid, "two.tick").await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::NOT_FOUND,
        "a grant for one call must not open the other, got {status}"
    );

    project.finish().await
}

/// `weft connect` names the file's access node once per call, takes
/// either name, and refuses a bare name with both of them.
#[tokio::test]
async fn connect_names_an_access_node_once_per_call() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("include_twice", disp).await?;

    for place in ["one.key", "two.key"] {
        let listed = project.weft(&["connect", "--node", place, "--list", "--json"]).await?;
        let out: Value = serde_json::from_str(listed.trim())?;
        anyhow::ensure!(out["node"] == json!(place), "it answers in the call's spelling: {out}");
        anyhow::ensure!(!listed.contains("@src:"), "the file's own path never reaches a reader: {listed}");
    }
    // A bare `key` is nobody's name: the refusal spells both calls.
    let refused = project.weft_refused(&["connect", "--node", "key", "--list"]).await?;
    anyhow::ensure!(
        refused.contains("one.key") && refused.contains("two.key"),
        "the refusal names both calls: {refused}"
    );

    project.finish().await
}

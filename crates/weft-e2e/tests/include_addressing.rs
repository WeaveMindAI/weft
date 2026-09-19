//! Layer-4: a node inside an included file is named through the SITE
//! that includes the file, on every verb that takes one and every line
//! that prints one.
//!
//! The compiler keys such a node by the file's own path
//! (`@src:lib:gate.hold`), which the language calls unspellable on
//! purpose. `weft events` already held that line; `weft wake` and
//! `weft connect` did not, and each had its own way of breaking it:
//! wake compared what you typed against the raw id, so only the
//! unspellable form worked, and connect both printed that form and
//! accepted nothing else.
#![cfg(feature = "e2e")]

mod common;

use common::color_of;

use serde_json::{json, Value};
use weft_e2e::{ensure, run, project::Project};

/// `weft wake <color> gate.hold` resolves the timer inside the included
/// file, and the id the runtime keys that timer by is refused with the
/// spelling that works. The run is aimed at `out`, which leaves the
/// access node beside the timer untouched.
#[tokio::test]
async fn wake_reaches_a_timer_inside_an_included_file_through_its_site() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_addressing", disp.clone()).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let held = color_of(&stdout)?;
    run::wait_for_status(&disp, held, "waiting_for_input").await?;

    // The compiler's own id resolves (it IS the node's id), so nothing
    // but a deliberate refusal keeps it out of a person's hands.
    let refused = project.weft_refused(&["wake", &held.to_string(), "@src:lib:gate.hold"]).await?;
    anyhow::ensure!(refused.contains("inside an included file"), "{refused}");
    anyhow::ensure!(refused.contains("site.hold"), "it names the spelling that works: {refused}");

    // The spelling the program uses.
    let woke: Value =
        serde_json::from_str(project.weft(&["wake", &held.to_string(), "gate.hold", "--json"]).await?.trim())?;
    anyhow::ensure!(woke["node"] == json!("gate.hold"), "it answers in the same spelling: {woke}");
    project.settled(held).await?.completed()?.assert_input("out", "data", &json!("hello"))?;

    project.finish().await
}

/// A run parked inside an included file says which node it is waiting
/// on, and says it the way the program spells it. That field is what a
/// client learns the run is parked ON, so the editor and any agent read
/// it, and the compiler's own id must not be in it.
#[tokio::test]
async fn a_parked_run_names_the_node_it_waits_on_through_its_site() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_addressing", disp.clone()).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let held = color_of(&stdout)?;
    run::wait_for_status(&disp, held, "waiting_for_input").await?;

    let detail: Value = disp.get_json(&format!("/executions/{held}")).await?;
    let waits = detail["waiting"].as_array().cloned().unwrap_or_default();
    anyhow::ensure!(waits.len() == 1, "one wait, the timer: {detail}");
    anyhow::ensure!(waits[0]["node"] == json!("gate.hold"), "spelled through its site: {detail}");
    anyhow::ensure!(
        !detail.to_string().contains("@src:"),
        "the file's own path never reaches a reader: {detail}"
    );

    project.weft(&["stop", &held.to_string()]).await?;
    project.finish().await
}

/// `weft connect` names the access node inside the included file
/// through its site, both when it prints the node list and when it
/// takes `--node`. It used to do neither: every target carried the
/// file's path, and that was the only string `--node` matched.
#[tokio::test]
async fn connect_addresses_an_access_node_through_its_site() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("include_addressing", disp).await?;

    // Nothing here runs the program: `weft connect` reads the source.
    let listed = project.weft(&["connect", "--node", "gate.key", "--list", "--json"]).await?;
    let out: Value = serde_json::from_str(listed.trim())?;
    anyhow::ensure!(out["node"] == json!("gate.key"), "it answers in the site's spelling: {out}");
    anyhow::ensure!(!listed.contains("@src:"), "the file's own path never reaches a reader: {listed}");

    // The id the compiler keys it by is not a spelling anyone is asked
    // to type, so it names no node here.
    let refused = project.weft_refused(&["connect", "--node", "@src:lib:gate.key", "--list"]).await?;
    anyhow::ensure!(refused.contains("gate.key"), "the refusal names the one that works: {refused}");

    project.finish().await
}

/// A node that HAS a connection reads back as having one. A constant
/// written in source for an input port has two homes and the compiler
/// moves it between them, so reading only one of them answered "no
/// connection picked" for every project that had one.
#[tokio::test]
async fn a_picked_connection_reads_back_as_picked() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("include_addressing", disp).await?;

    // The project mints its own key, so nothing here needs a third party.
    project
        .weft(&["connect", "--node", "gate.key", "--door", "own", "--set", "keys=e2e-secret"])
        .await?;

    let listed = project.weft(&["connect", "--node", "gate.key", "--list"]).await?;
    anyhow::ensure!(
        listed.contains("is connected as"),
        "the pick the last command wrote has to read back: {listed}"
    );
    anyhow::ensure!(!listed.contains("no connection picked"), "{listed}");

    project.finish().await
}

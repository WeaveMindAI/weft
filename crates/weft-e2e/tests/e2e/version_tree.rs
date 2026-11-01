//! The version tree: every run records the code it ran; a seeded run
//! inherits what did not change; a spec runs one piece of the graph
//! with a value handed in, or fires a trigger with nothing activated;
//! a run freezes as an example, runs again, and is compared for review.
//!
//! The fixture (fixtures/version_tree/src/main.weft) is a chain `src -> mid
//! -> out` plus a trigger program `tick -> stamp`. Five contracts:
//!
//!  - a plain run lands in the tree as a run under a version, and `weft
//!    tree --json` says the folder's working copy is that version;
//!  - `run --seed --seed-before out` re-runs `out` and takes `src` and `mid`
//!    from the first run: their rows are painted inherited, and the run
//!    answer names what was inherited and what ran;
//!  - `run --from mid='{"value":"hello"}'` runs `mid` and `out` with
//!    the value handed in and never touches `src`;
//!  - `run --fire tick=...` fires the trigger's program on the code on
//!    disk after baking, with the project never activated;
//!  - `freeze` writes `examples/<name>.json`; `run chain` runs current
//!    code with those parameters and `diff` shows changes for review.
#![cfg(feature = "e2e")]

use crate::common;

use common::{my_copy, run_id_of, inherited, summary_of, tree_of, version_ids};

use serde_json::{json, Value};
use weft_e2e::{ensure, project::Project, run::SettledRun};

#[tokio::test]
async fn every_run_records_a_version_and_a_seeded_run_inherits_the_unchanged_nodes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    let first = run_id_of(&stdout)?;
    let settled = SettledRun::observe(project.dispatcher(), first).await?;
    settled.completed()?;
    settled.assert_completed("src")?.assert_completed("mid")?.assert_completed("out")?;
    assert!(!inherited(&settled, "src"), "a run from nothing inherits nothing");

    // The tree holds the run under the version the folder's working copy is.
    let tree = tree_of(&project).await?;
    let runs = tree["runs"].as_array().expect("runs");
    anyhow::ensure!(runs.iter().any(|r| r["runId"] == json!(first.to_string())), "the run is in the tree: {tree}");
    let copy = my_copy(&tree)?;
    anyhow::ensure!(
        copy["workingVersion"] == copy["headVersion"] && version_ids(&tree).iter().any(|v| copy["workingVersion"] == json!(v)),
        "the folder's files are the recorded version head is on: {tree}"
    );
    anyhow::ensure!(copy["headRun"] == json!(first.to_string()), "head's run is the run: {tree}");

    // Seeded: `out` re-runs by name, `src` and `mid` are inherited.
    let stdout = project.weft(&["run", "--json", "--target", "out", "--seed", "--seed-before", "out"]).await?;
    let second = run_id_of(&stdout)?;
    let summary = summary_of(&stdout);
    anyhow::ensure!(summary.contains("inherited from") && summary.contains("1 ran (out)"), "summary: {summary}");
    let settled = SettledRun::observe(project.dispatcher(), second).await?;
    settled.completed()?;
    settled.assert_completed("out")?;
    anyhow::ensure!(inherited(&settled, "src"), "src is painted inherited");
    anyhow::ensure!(inherited(&settled, "mid"), "mid is painted inherited");
    anyhow::ensure!(!inherited(&settled, "out"), "out ran in this run");
    settled.assert_input("out", "data", &json!("hello"))?;

    project.finish().await
}

/// Changed results remain reviewable evidence until explicitly accepted,
/// and a failed run of the example stays a failure that replaces nothing.
#[tokio::test]
async fn a_frozen_example_drifts_and_is_frozen_again() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    let first = run_id_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), first).await?.completed()?;
    project.weft(&["freeze", "chain", &first.to_string(), "--json"]).await?;
    let refused = project.weft_refused(&["run", "nothing-here"]).await?;
    anyhow::ensure!(refused.contains("no spec at") && refused.contains("nothing-here"), "{refused}");
    project.weft(&["run", "--json", "--from", "mid={\"value\":\"x\"}", "--save", "plain-spec"]).await?;
    let plain = run_id_of(&project.weft(&["run", "plain-spec", "--json"]).await?)?;
    SettledRun::observe(project.dispatcher(), plain).await?.completed()?.assert_input("mid", "value", &json!("x"))?;
    let accepted = project.read_file("examples/chain.json")?;

    // Run the new code with frozen parameters, leaving the accepted file alone.
    project.write_file("prompts/greeting.txt", "moved")?;
    let drifted = run_id_of(&project.weft(&["run", "chain", "--json"]).await?)?;
    SettledRun::observe(project.dispatcher(), drifted).await?.completed()?.assert_input("out", "data", &json!("moved"))?;
    let examples: Value = serde_json::from_str(project.weft(&["examples", "--json"]).await?.trim())?;
    let chain = examples.as_array().unwrap().iter().find(|e| e["name"] == json!("chain")).cloned().unwrap();
    anyhow::ensure!(chain["frozen"] == json!(true) && chain.get("verdict").is_none(), "{examples}");

    // Differences are a successful comparison, with both values for review.
    let out = project.weft(&["diff", &drifted.to_string(), "example:chain", "--full", "--json"]).await?;
    let diff: Value = serde_json::from_str(out.lines().find(|l| l.trim_start().starts_with('{')).unwrap_or("").trim())?;
    let differing = diff["differing"].as_array().unwrap();
    anyhow::ensure!(!differing.is_empty() && differing.iter().any(|d| d["node"] == json!("src") && d["left"]["value"] == json!("moved") && d["right"]["value"] == json!("hello")), "{diff}");
    anyhow::ensure!(project.read_file("examples/chain.json")? == accepted, "run and diff never accept changes implicitly");
    let same: Value = serde_json::from_str(project.weft(&["diff", &first.to_string(), "example:chain", "--json"]).await?.trim())?;
    anyhow::ensure!(same["differing"].as_array().unwrap().is_empty(), "a run against its own example: {same}");

    // Freezing again from the drifted run keeps the spec and takes the new values.
    project.weft(&["freeze", "chain", &drifted.to_string(), "--json"]).await?;
    let diff: Value = serde_json::from_str(project.weft(&["diff", &drifted.to_string(), "example:chain", "--json"]).await?.trim())?;
    anyhow::ensure!(diff["differing"].as_array().unwrap().is_empty(), "explicit freeze accepted these outputs: {diff}");
    let accepted = project.read_file("examples/chain.json")?;

    // A failed run of the example stays a failure: it is the folder's head
    // run, listed failed, still compared, and it never replaces the
    // accepted evidence. `src` is text, and a Cast to Number fails loudly.
    let main = project.read_file("src/main.weft")?;
    project.set_main(&main.replace("mid = Cast -> (value: String)", "mid = Cast -> (value: Number)"))?;
    let failed_run = run_id_of(&project.weft(&["run", "chain", "--json"]).await?)?;
    SettledRun::observe(project.dispatcher(), failed_run).await?.failed_with("cannot cast")?;
    let tree = tree_of(&project).await?;
    let failed = my_copy(&tree)?["headRun"].as_str().expect("failed run recorded");
    anyhow::ensure!(failed == failed_run.to_string(), "the failed run must be the recorded head run");
    let row = tree["runs"].as_array().unwrap().iter().find(|row| row["runId"] == failed).unwrap();
    anyhow::ensure!(row["listing"]["status"] == "failed", "run failure is retained: {row}");
    let diff: Value = serde_json::from_str(project.weft(&["diff", failed, "example:chain", "--json"]).await?.trim())?;
    anyhow::ensure!(!diff["differing"].as_array().unwrap().is_empty(), "failure outputs remain reviewable: {diff}");
    anyhow::ensure!(project.read_file("examples/chain.json")? == accepted, "a failed run never overwrites accepted evidence");

    project.finish().await
}

/// All triggers require a matching bake, including one with only config.
/// Preparing them must not activate their real schedules.
#[tokio::test]
async fn never_baked_triggers_refuse_and_baking_allows_fire_without_activation() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;

    let tick = "tick={\"scheduledTime\":\"2026-04-04T00:00:00Z\",\"actualTime\":\"2026-04-04T00:00:00Z\"}";
    let refused = project.weft_refused(&["run", "--json", "--fire", tick]).await?;
    anyhow::ensure!(refused.contains("weft bake"), "{refused}");
    let wired = "wired={\"scheduledTime\":\"2026-04-04T00:00:00Z\",\"actualTime\":\"2026-04-04T00:00:00Z\"}";
    let refused = project.weft_refused(&["run", "--fire", wired]).await?;
    anyhow::ensure!(refused.contains("weft bake"), "{refused}");
    project.weft(&["bake", "--json"]).await?;
    let tree = tree_of(&project).await?;
    anyhow::ensure!(tree["activatedVersions"] == json!([]), "bake does not activate: {tree}");
    let stdout = project.weft(&["run", "--json", "--fire", tick]).await?;
    let settled = SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?;
    settled.completed()?.assert_completed("tick")?;
    settled.assert_input("stamp", "data", &json!("2026-04-04T00:00:00Z"))?;

    let stdout = project.weft(&["run", "--json", "--fire", wired]).await?;
    SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?.completed()?
        .assert_completed("wired")?.assert_untouched("sched")?;

    // Even a change outside the fired branch changes the baked program identity.
    project.write_file("prompts/greeting.txt", "new code")?;
    let refused = project.weft_refused(&["run", "--fire", wired]).await?;
    anyhow::ensure!(refused.contains("weft bake"), "changed code requires a new bake: {refused}");

    project.finish().await
}

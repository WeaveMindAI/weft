//! The version tree: every run records the code it ran; a seeded run
//! inherits what did not change; a spec runs one piece of the graph
//! with a value handed in, or fires a trigger with nothing activated;
//! a run freezes as an example, runs again, and is compared for review.
//!
//! The fixture (fixtures/version_tree/src/main.weft) is a chain `src -> mid
//! -> out` plus a trigger program `tick -> stamp`. Five contracts:
//!
//!  - a plain run lands in the tree as a run under a version, and `weft
//!    tree --json` says the files on disk are that version;
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

mod common;

use common::{color_of, inherited, summary_of, tree_of, warnings_of};

use serde_json::{json, Value};
use weft_e2e::{ensure, project::Project, run::SettledRun};

#[tokio::test]
async fn every_run_records_a_version_and_a_seeded_run_inherits_the_unchanged_nodes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let first = color_of(&stdout)?;
    let settled = SettledRun::observe(project.dispatcher(), first).await?;
    settled.completed()?;
    settled.assert_completed("src")?.assert_completed("mid")?.assert_completed("out")?;
    assert!(!inherited(&settled, "src"), "a run from nothing inherits nothing");

    // The tree holds the run under the version the disk is.
    let tree: Value = serde_json::from_str(project.weft(&["tree", "--json"]).await?.trim())?;
    let runs = tree["runs"].as_array().expect("runs");
    anyhow::ensure!(runs.iter().any(|r| r["color"] == json!(first.to_string())), "the run is in the tree: {tree}");
    anyhow::ensure!(tree["disk_version"].is_string(), "the files on disk are a recorded version: {tree}");
    anyhow::ensure!(tree["head"]["head_run"] == json!(first.to_string()), "head's run is the run: {tree}");

    // Seeded: `out` re-runs by name, `src` and `mid` are inherited.
    let stdout = project.weft(&["run", "--json", "--target", "out", "--seed", "--seed-before", "out"]).await?;
    let second = color_of(&stdout)?;
    let summary = summary_of(&stdout);
    anyhow::ensure!(summary.contains("inherited from") && summary.contains("1 ran (out)"), "summary: {summary}");
    let settled = SettledRun::observe(project.dispatcher(), second).await?;
    settled.completed()?;
    settled.assert_completed("out")?;
    anyhow::ensure!(inherited(&settled, "src"), "src is painted inherited");
    anyhow::ensure!(inherited(&settled, "mid"), "mid is painted inherited");
    anyhow::ensure!(!inherited(&settled, "out"), "out ran in this color");
    settled.assert_input("out", "data", &json!("hello"))?;

    project.finish().await
}

#[tokio::test]
async fn a_scoped_run_takes_a_value_by_hand_and_a_spec_fires_a_trigger_without_activation() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project
        .weft(&["run", "--json", "--from", "mid={\"value\":\"by hand\"}"])
        .await?;
    project.mark_registered();
    let color = color_of(&stdout)?;
    let settled = SettledRun::observe(project.dispatcher(), color).await?;
    settled.completed()?;
    settled.assert_untouched("src")?;
    settled.assert_input("mid", "value", &json!("by hand"))?;
    settled.assert_input("out", "data", &json!("by hand"))?;

    // Bake prepares the trigger's ports without arming its schedule.
    project.weft(&["bake", "--json"]).await?;
    let stdout = project
        .weft(&["run", "--json", "--fire", "tick={\"scheduledTime\":\"2026-01-01T00:00:00Z\",\"actualTime\":\"2026-01-01T00:00:00Z\"}", "--save", "tick-once"])
        .await?;
    let color = color_of(&stdout)?;
    let settled = SettledRun::observe(project.dispatcher(), color).await?;
    settled.completed()?;
    settled.assert_completed("tick")?;
    settled.assert_input("stamp", "data", &json!("2026-01-01T00:00:00Z"))?;
    settled.assert_untouched("src")?;
    anyhow::ensure!(project.dir().join("examples").join("tick-once.json").is_file(), "--save wrote the spec");

    project.finish().await
}

#[tokio::test]
async fn a_frozen_example_runs_current_code_and_can_be_compared() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let color = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), color).await?.completed()?;

    let frozen: Value = serde_json::from_str(project.weft(&["freeze", "chain", &color.to_string(), "--json"]).await?.trim())?;
    anyhow::ensure!(frozen["wires"].as_u64().unwrap_or(0) >= 2, "the example holds the chain's wires: {frozen}");
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(project.dir().join("examples/chain.json"))?)?;
    anyhow::ensure!(spec["expected"]["wires"].is_array(), "examples/chain.json is frozen: {spec}");

    let rerun = color_of(&project.weft(&["run", "chain", "--json"]).await?)?;
    SettledRun::observe(project.dispatcher(), rerun).await?.completed()?;
    let diff: Value = serde_json::from_str(project.weft(&["diff", &rerun.to_string(), "example:chain", "--json"]).await?.trim())?;
    anyhow::ensure!(diff["differing"].as_array().unwrap().is_empty(), "same outputs: {diff}");
    let examples: Value = serde_json::from_str(project.weft(&["examples", "--json"]).await?.trim())?;
    anyhow::ensure!(examples[0]["frozen"] == json!(true) && examples[0].get("verdict").is_none(), "examples carry evidence, no verdict: {examples}");

    project.finish().await
}

/// `weft tree --json`, parsed.
fn version_count(tree: &Value) -> usize {
    tree["versions"].as_array().map(Vec::len).unwrap_or(0)
}

/// Checkpoint, branch and prune move the tree: a label names a version,
/// a branch restores and deletes files and moves head, a dirty tree is
/// refused until kept or discarded, and prune refuses under head.
#[tokio::test]
async fn checkpoint_branch_and_prune_move_the_tree() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let first = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), first).await?.completed()?;
    let v1 = tree_of(&project).await?["head"]["head_version"].as_str().unwrap().to_string();

    // The code is unchanged: a checkpoint records nothing new but still
    // moves head off the run.
    let same: Value = serde_json::from_str(project.weft(&["checkpoint", "base", "--json"]).await?.trim())?;
    anyhow::ensure!(same["created"] == json!(false) && same["version"] == json!(v1), "already at v1: {same}");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(tree["head"]["head_run"].is_null(), "a checkpoint clears head's run: {tree}");

    // An edit and an added file make a new version under v1, labelled.
    project.write_file("prompts/greeting.txt", "louder")?;
    project.write_file("prompts/extra.txt", "spare")?;
    let loud: Value = serde_json::from_str(project.weft(&["checkpoint", "loud", "--json"]).await?.trim())?;
    let v2 = loud["version"].as_str().unwrap().to_string();
    anyhow::ensure!(loud["created"] == json!(true) && v2 != v1, "a new version: {loud}");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(version_count(&tree) == 2, "two versions: {tree}");
    anyhow::ensure!(tree["disk_version"] == json!(v2), "the disk is v2: {tree}");
    let v2_row = tree["versions"].as_array().unwrap().iter().find(|v| v["id"] == json!(v2)).cloned().unwrap();
    anyhow::ensure!(v2_row["parent_id"] == json!(v1) && v2_row["label"] == json!("loud"), "v2 under v1, labelled: {v2_row}");
    anyhow::ensure!(v2_row["diff"]["changed"] == json!(["prompts/greeting.txt"]) && v2_row["diff"]["added"] == json!(["prompts/extra.txt"]), "the diff names the files: {v2_row}");

    // Unkept edits refuse a branch, naming the file; `--discard` throws them away.
    project.write_file("prompts/greeting.txt", "unkept")?;
    let refused = project.weft_refused(&["branch", &first.to_string()]).await?;
    anyhow::ensure!(refused.contains("the tree has changes since head") && refused.contains("prompts/greeting.txt"), "{refused}");
    project.weft(&["branch", &first.to_string(), "--discard"]).await?;
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == "hello", "the run's version is back on disk");
    anyhow::ensure!(!project.has_file("prompts/extra.txt"), "a file the version never held is gone");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(tree["head"]["head_version"] == json!(v1) && tree["head"]["head_run"] == json!(first.to_string()), "head is the run again: {tree}");
    anyhow::ensure!(tree["disk_version"] == json!(v1), "{tree}");

    // A label names its version too, and disk_version is null off any version.
    project.weft(&["branch", "loud"]).await?;
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == "louder" && project.has_file("prompts/extra.txt"), "v2 restored whole");
    project.write_file("prompts/greeting.txt", "nowhere")?;
    anyhow::ensure!(tree_of(&project).await?["disk_version"].is_null(), "an unrecorded edit is no version");
    project.weft(&["branch", "loud", "--discard"]).await?;

    // Prune refuses under head, then takes the subtree once head has left.
    let refused = project.weft_refused(&["prune", &v2, "--yes"]).await?;
    // The discriminating half: the ACTIVATION refusal carries "inside
    // the subtree" too, so matching only that would pass with the head
    // check deleted.
    anyhow::ensure!(refused.contains("head is on version"), "{refused}");
    project.weft(&["branch", &first.to_string()]).await?;
    let pruned: Value = serde_json::from_str(project.weft(&["prune", "loud", "--yes", "--json"]).await?.trim())?;
    anyhow::ensure!(pruned["deleted"] == json!(true), "{pruned}");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(version_count(&tree) == 1, "only v1 is left: {tree}");
    let refused = project.weft_refused(&["prune", "louder-than-any-id"]).await?;
    anyhow::ensure!(refused.contains("no version is labelled or starts with"), "{refused}");

    project.finish().await
}

/// Seeding follows the edit: an upstream edit re-runs everything after
/// it, a wholly reused run executes no bodies, omitting `--seed` runs everything,
/// reuse bounds need `--seed`, and a bare head seeds from the newest
/// settled run on its version.
#[tokio::test]
async fn seeding_follows_the_edit_and_a_wholly_reused_run_executes_no_bodies() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let first = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), first).await?.completed()?;

    // Nothing changed: the run retains the complete history without executing it.
    let stdout = project.weft(&["run", "--json", "--target", "out", "--seed"]).await?;
    let reused = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    reused.completed()?;
    for node in ["src", "mid", "out"] {
        anyhow::ensure!(inherited(&reused, node), "{node} is reused");
    }
    let refused = project.weft_refused(&["run", "--json", "--target", "out", "--seed-before", "out"]).await?;
    anyhow::ensure!(refused.contains("need --seed"), "{refused}");

    // An edit upstream of everything: nothing is inherited.
    project.write_file("prompts/greeting.txt", "changed")?;
    let stdout = project.weft(&["run", "--json", "--target", "out", "--seed"]).await?;
    let second = color_of(&stdout)?;
    let summary = summary_of(&stdout);
    anyhow::ensure!(summary.contains("0 nodes inherited") && summary.contains("3 ran"), "summary: {summary}");
    let settled = SettledRun::observe(project.dispatcher(), second).await?;
    settled.completed()?.assert_input("out", "data", &json!("changed"))?;
    anyhow::ensure!(!inherited(&settled, "src"), "src re-ran");

    // A run without a seed inherits nothing.
    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    let third = color_of(&stdout)?;
    let fresh = SettledRun::observe(project.dispatcher(), third).await?;
    fresh.completed()?.assert_input("out", "data", &json!("changed"))?;
    anyhow::ensure!(!inherited(&fresh, "src") && !inherited(&fresh, "mid") && !inherited(&fresh, "out"), "an unseeded run executes the chain");

    // A checkpoint leaves head bare; the seed is the newest settled run on it.
    project.weft(&["checkpoint", "--json"]).await?;
    let stdout = project.weft(&["run", "--json", "--target", "out", "--seed", "--seed-before", "out"]).await?;
    let summary = summary_of(&stdout);
    anyhow::ensure!(summary.contains(&format!("inherited from {}", &third.to_string()[..8])), "seeded from the newest run: {summary}");
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?;

    // A cleaned seed breaks nothing already settled, but the tree forgets the run.
    project.weft(&["clean", &third.to_string(), "--yes"]).await?;
    let tree = tree_of(&project).await?;
    anyhow::ensure!(!tree["runs"].as_array().unwrap().iter().any(|r| r["color"] == json!(third.to_string())), "the run row went with the journal: {tree}");

    project.finish().await
}

/// Missing crossing values warn and settle normally. Saved parameters
/// accept overrides; seeded starts use the inherited upstream value.
#[tokio::test]
async fn scoped_runs_refuse_plainly_and_a_saved_spec_runs_by_name() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let refused = project.weft_refused(&["run", "--json", "--target", "missing"]).await?;
    anyhow::ensure!(refused.contains("unknown node 'missing'"), "{refused}");
    for phase in ["build_start", "build_skip", "image_push_start", "dispatcher_call_start"] {
        anyhow::ensure!(!refused.contains(phase), "a refused cut must not build or register: {refused}");
    }

    let stdout = project.weft(&["run", "--json", "--from", "mid"]).await?;
    project.mark_registered();
    anyhow::ensure!(
        warnings_of(&stdout).iter().any(|warning| warning.contains("mid") && warning.contains("value")),
        "missing crossing input should be explained: {stdout}"
    );
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?.assert_untouched("src")?;
    let refused = project.weft_refused(&["run", "--json", "--from", "out", "--target", "src"]).await?;
    anyhow::ensure!(refused.contains("selection is empty"), "{refused}");
    let refused = project.weft_refused(&["run", "--json", "--fire", "src={}"]).await?;
    anyhow::ensure!(refused.contains("is not a trigger"), "{refused}");
    // Saved, then run by name: the value by hand reaches `mid`, `src` never runs.
    let stdout = project.weft(&["run", "--json", "--from", "mid={\"value\":\"by hand\"}", "--save", "mid-only"]).await?;
    let saved_run = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), saved_run).await?.completed()?;
    let tree = tree_of(&project).await?;
    anyhow::ensure!(tree["runs"].as_array().unwrap().iter().any(|run| run["color"] == saved_run.to_string() && run["example"] == "mid-only"), "--save links its first run: {tree}");
    let seeded = project.weft(&["run", "mid-only", "--json", "--seed"]).await?;
    let seeded = SettledRun::observe(project.dispatcher(), color_of(&seeded)?).await?;
    seeded.completed()?;
    anyhow::ensure!(inherited(&seeded, "mid") && inherited(&seeded, "out"), "identical carved backups reuse the selected work");
    let stdout = project.weft(&["run", "--json", "mid-only"]).await?;
    let settled = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    settled.completed()?.assert_input("mid", "value", &json!("by hand"))?.assert_untouched("src")?;
    let stdout = project.weft(&["run", "mid-only", "--json", "--from", "mid={\"value\":\"override\",\"removed_port\":123}"]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?.assert_input("mid", "value", &json!("override"))?;
    anyhow::ensure!(warnings_of(&stdout).iter().any(|warning| warning.contains("removed_port")), "removed ports warn and are ignored: {stdout}");

    // With `--seed`, the crossing input needs no value: its source is inherited.
    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?;
    let stdout = project.weft(&["run", "--json", "--from", "out", "--seed", "--seed-before", "out"]).await?;
    let settled = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("out")?;
    anyhow::ensure!(inherited(&settled, "src") && inherited(&settled, "mid"), "the source chain is inherited");

    project.finish().await
}

/// Changed results remain reviewable evidence until explicitly accepted.
#[tokio::test]
async fn a_frozen_example_drifts_and_is_frozen_again() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let first = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), first).await?.completed()?;
    project.weft(&["freeze", "chain", &first.to_string(), "--json"]).await?;
    let refused = project.weft_refused(&["run", "nothing-here"]).await?;
    anyhow::ensure!(refused.contains("no spec at") && refused.contains("nothing-here"), "{refused}");
    project.weft(&["run", "--json", "--from", "mid={\"value\":\"x\"}", "--save", "plain-spec"]).await?;
    let plain = color_of(&project.weft(&["run", "plain-spec", "--json"]).await?)?;
    SettledRun::observe(project.dispatcher(), plain).await?.completed()?.assert_input("mid", "value", &json!("x"))?;
    let accepted = project.read_file("examples/chain.json")?;

    // Run the new code with frozen parameters, leaving the accepted file alone.
    project.write_file("prompts/greeting.txt", "moved")?;
    let drifted = color_of(&project.weft(&["run", "chain", "--json"]).await?)?;
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

    project.finish().await
}

/// Trigger inputs come from a bake. Emit bypasses the trigger body;
/// from cannot supply its inputs, with or without a fire payload.
#[tokio::test]
async fn a_fired_trigger_uses_baked_inputs_and_emit_bypasses_its_body() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    // Refused at the door, naming both ways forward.
    let fire = "wired={\"scheduledTime\":\"2026-02-02T00:00:00Z\",\"actualTime\":\"2026-02-02T00:00:00Z\"}";
    let refused = project
        .weft_refused(&["run", "--fire", fire, "--from", "wired={\"cron\":\"0 0 * * * *\"}"])
        .await?;
    project.mark_registered();
    anyhow::ensure!(refused.contains("cannot be a from start") && refused.contains("--emit"), "{refused}");
    project.weft(&["bake", "--json"]).await?;

    // An explicit extra start may run, but the trigger keeps its baked ports.
    let stdout = project.weft(&["run", "--json", "--fire", fire, "--from", "sched"]).await?;
    let settled = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("sched")?.assert_completed("wired")?;
    settled.assert_input("beat", "data", &json!("2026-02-02T00:00:00Z"))?;

    // Standing in for it: the declared value reaches what the trigger
    // feeds, and the trigger itself never runs. This is the way past a
    // step whose act you do not want performed at all.
    let stdout = project
        .weft(&["run", "--json", "--emit", "wired={\"scheduledTime\":\"2026-03-03T00:00:00Z\"}"])
        .await?;
    let settled = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    settled.completed()?;
    settled.assert_input("beat", "data", &json!("2026-03-03T00:00:00Z"))?;
    settled.assert_untouched("wired")?;

    project.finish().await
}

/// Listener preparation and fire keep exact ordinary-group boundaries;
/// extracting a trigger from inside a loop is refused on every entry path.
#[tokio::test]
async fn trigger_preparation_and_fire_cut_precisely_inside_an_ordinary_group() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;
    let graph = r#"
scope = Group() -> (stamp: String) {
  sched = Text { value: "0 0 * * * *" }
  tick = Cron { cron: sched.value }
  after = Debug
  after.data = tick.scheduledTime
  unrelated = Text { value: "must stay outside the cut" }
  self.stamp = tick.scheduledTime
}
out = Debug
out.data = scope.stamp
"#;
    project.write_file("src/main.weft", graph)?;
    let bake = project.weft(&["bake", "--json"]).await?;
    project.mark_registered();
    SettledRun::observe(project.dispatcher(), color_of(&bake)?).await?.completed()?
        .assert_completed("scope.sched")?.assert_completed("scope.tick")?
        .assert_untouched("scope.after")?.assert_untouched("scope.unrelated")?.assert_untouched("out")?;
    let fire = r#"scope.tick={"scheduledTime":"2026-04-04T00:00:00Z","actualTime":"2026-04-04T00:00:00Z"}"#;
    let output = project.weft(&["run", "--json", "--fire", fire]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&output)?).await?.completed()?
        .assert_completed("scope.tick")?.assert_input("out", "data", &json!("2026-04-04T00:00:00Z"))?
        .assert_untouched("scope.sched")?.assert_untouched("scope.unrelated")?;
    let output = project.weft(&["run", "--json", "--fire", fire, "--before", "scope.after"]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&output)?).await?.completed()?
        .assert_completed("scope.tick")?.assert_untouched("scope.after")?.assert_untouched("out")?;
    // A shared producer feeds both trigger setup and the trigger's consumer.
    // Setup must exclude the consumer; firing must execute the producer again.
    let shared = graph.replace(
        "  after = Debug\n  after.data = tick.scheduledTime",
        r#"  after = Format(schedule: String, stamp: String) {
    template: "{{schedule}} / {{stamp}}"
    schedule: sched.value
    stamp: tick.scheduledTime
  }"#,
    );
    project.write_file("src/main.weft", &shared)?;
    let bake = project.weft(&["bake", "--json"]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&bake)?).await?.completed()?
        .assert_completed("scope.sched")?.assert_completed("scope.tick")?
        .assert_untouched("scope.after")?.assert_untouched("scope.unrelated")?.assert_untouched("out")?;
    let output = project.weft(&["run", "--json", "--fire", fire]).await?;
    let shared_run = SettledRun::observe(project.dispatcher(), color_of(&output)?).await?;
    shared_run.completed()?.assert_completed("scope.sched")?.assert_completed("scope.tick")?
        .assert_completed("scope.after")?
        .assert_input("scope.after", "schedule", &json!("0 0 * * * *"))?
        .assert_input("scope.after", "stamp", &json!("2026-04-04T00:00:00Z"))?
        .assert_untouched("scope.unrelated")?;
    anyhow::ensure!(!inherited(&shared_run, "scope.sched"), "the shared producer must run again");

    // Emitting the trigger's output still gathers the consumer's side input.
    let output = project.weft(&["run", "--json", "--emit", r#"scope.tick={"scheduledTime":"manual"}"#]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&output)?).await?.completed()?
        .assert_untouched("scope.tick")?.assert_completed("scope.sched")?
        .assert_input("scope.after", "schedule", &json!("0 0 * * * *"))?
        .assert_input("scope.after", "stamp", &json!("manual"))?
        .assert_untouched("scope.unrelated")?;

    project.write_file("src/main.weft", &graph.replace("  sched =", "  _should_flow: false\n  sched ="))?;
    let bake = project.weft(&["bake", "--json"]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&bake)?).await?.completed()?
        .assert_skipped("scope.tick")?.assert_untouched("scope.after")?.assert_untouched("scope.unrelated")?;
    let refused = project.weft_refused(&["run", "--fire", fire]).await?;
    anyhow::ensure!(refused.contains("scope.tick"), "{refused}");
    let loop_graph = r#"
scope = Loop(values: List[Number]) -> (stamps: List[String | Null]) {
  over: ["values"]
  tick = Cron { cron: "0 0 * * * *" }
  self.stamps = tick.scheduledTime
}
scope.values = [1]
"#;
    project.write_file("src/main.weft", loop_graph)?;
    for args in [vec!["bake"], vec!["activate"], vec!["run", "--fire", fire]] {
        let refused = project.weft_refused(&args).await?;
        anyhow::ensure!(refused.contains("inside a Loop"), "{refused}");
    }
    project.finish().await
}

/// All triggers require a matching bake, including one with only config.
/// Preparing them must not activate their real schedules.
#[tokio::test]
async fn never_baked_triggers_refuse_and_baking_allows_fire_without_activation() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let tick = "tick={\"scheduledTime\":\"2026-04-04T00:00:00Z\",\"actualTime\":\"2026-04-04T00:00:00Z\"}";
    let refused = project.weft_refused(&["run", "--json", "--fire", tick]).await?;
    project.mark_registered();
    anyhow::ensure!(refused.contains("weft bake"), "{refused}");
    let wired = "wired={\"scheduledTime\":\"2026-04-04T00:00:00Z\",\"actualTime\":\"2026-04-04T00:00:00Z\"}";
    let refused = project.weft_refused(&["run", "--fire", wired]).await?;
    anyhow::ensure!(refused.contains("weft bake"), "{refused}");
    project.weft(&["bake", "--json"]).await?;
    let tree = tree_of(&project).await?;
    anyhow::ensure!(tree["head"]["activation_version"].is_null(), "bake does not activate: {tree}");
    let stdout = project.weft(&["run", "--json", "--fire", tick]).await?;
    let settled = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("tick")?;
    settled.assert_input("stamp", "data", &json!("2026-04-04T00:00:00Z"))?;

    let stdout = project.weft(&["run", "--json", "--fire", wired]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?
        .assert_completed("wired")?.assert_untouched("sched")?;

    // Even a change outside the fired branch changes the baked program identity.
    project.write_file("prompts/greeting.txt", "new code")?;
    let refused = project.weft_refused(&["run", "--fire", wired]).await?;
    anyhow::ensure!(refused.contains("weft bake"), "changed code requires a new bake: {refused}");

    project.finish().await
}

/// The activated case: once a project is active, its triggers have
/// registered the ports they set up with, and firing one BY HAND
/// replays that same snapshot, exactly as a real event does. Before
/// this the hand-fired path sent no snapshot at all, so a trigger whose
/// port is wired failed on that port under `weft run --fire` while the
/// identical trigger fired by a real event ran fine.
#[tokio::test]
async fn a_hand_fired_trigger_replays_what_activation_registered() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    // Activation runs `sched` and registers `wired` with the cron it
    // read off that wire. Nothing is handed in below, and `sched` is
    // not in the run: the snapshot is the only source of the port.
    project.activate().await?;

    let fire = "wired={\"scheduledTime\":\"2026-03-03T00:00:00Z\",\"actualTime\":\"2026-03-03T00:00:00Z\"}";
    let stdout = project.weft(&["run", "--json", "--fire", fire]).await?;
    let settled = SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("wired")?;
    settled.assert_input("beat", "data", &json!("2026-03-03T00:00:00Z"))?;
    settled.assert_untouched("sched")?;

    project.weft(&["deactivate", "--mode", "wipe", "--running-policy", "cancel"]).await?;
    let stdout = project.weft(&["run", "--json", "--fire", fire]).await?;
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?.assert_completed("wired")?;
    project.finish().await
}

/// Activation records the version the triggers run on without moving
/// head, prune refuses under it, and deactivating clears it.
#[tokio::test]
async fn activation_pins_a_version_in_the_tree() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    project.activate().await?;
    let tree = tree_of(&project).await?;
    let active = tree["head"]["activation_version"].as_str().map(str::to_string);
    anyhow::ensure!(active.is_some() && tree["head"]["head_version"].is_null(), "activation pins the version, head stays: {tree}");
    anyhow::ensure!(tree["versions"].as_array().unwrap().iter().any(|v| v["id"] == json!(active)), "the version is in the tree: {tree}");
    let refused = project.weft_refused(&["prune", active.as_deref().unwrap(), "--yes"]).await?;
    anyhow::ensure!(refused.contains("activated version is inside the subtree"), "{refused}");
    project.weft(&["deactivate", "--mode", "wipe", "--running-policy", "cancel"]).await?;
    let tree = tree_of(&project).await?;
    anyhow::ensure!(tree["head"]["activation_version"].is_null(), "deactivate clears it: {tree}");

    project.finish().await
}

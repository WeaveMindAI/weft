//! Layer-4: an included file is compiled once and every `@include` of it
//! is a call. The `include_calls` fixture includes one file three times:
//! twice in a chain at the top and once inside a loop, and the file
//! itself holds a loop and a node that lives beside it under `src/`.
//! The rig proves each call runs under its own frame, a cut spelled
//! through a site runs that one call, the CLI addresses a node through
//! its site, and a frozen cut inside the file replays.
#![cfg(feature = "e2e")]

mod common;

use common::color_of;

use serde_json::{json, Value};
use weft_e2e::{ensure, project::Project};

/// Every call of the file runs the file's one body under its own call
/// frame, the node beside the code runs in it, and a loop inside the
/// file nests inside the loop around a call.
#[tokio::test]
async fn a_file_included_three_times_runs_once_per_call() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_calls", disp).await?;
    let stdout = project.weft(&["run", "--json"]).await?;
    project.mark_registered();
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?;
    // The chain: " hello " is trimmed and shouted twice over.
    settled.assert_input("two.strip", "text", &json!("HELLO"))?;
    settled.assert_input("out", "data", &json!("HELLO"))?;
    for node in ["one.strip", "one.loud", "one.tally", "two.strip", "two.loud", "two.tally"] {
        settled.assert_completed(node)?;
    }
    // Each call's rows carry that call's frame and nothing else.
    let frames_of = |spelled: &str| settled.events_of(spelled).next().map(|e| e.frames().clone()).unwrap_or(Value::Null);
    anyhow::ensure!(frames_of("one.strip") == json!([{"site": "one"}]), "{}", frames_of("one.strip"));
    anyhow::ensure!(frames_of("two.strip") == json!([{"site": "two"}]), "{}", frames_of("two.strip"));
    // The call inside the loop: one call per iteration, each under its
    // iteration and its site, and the file's own loop inside each.
    settled.assert_loop_iterations("outer", 2)?;
    settled.assert_completed("outer.call.strip")?;
    settled.assert_input("total", "data", &json!(["A", "B"]))?;
    let inner: Vec<Value> = settled.events_of("outer.call.strip")
        .filter(|e| e.kind() == "node_completed").map(|e| e.frames().clone()).collect();
    anyhow::ensure!(inner == vec![json!([{"index": 0}, {"site": "outer.call"}]), json!([{"index": 1}, {"site": "outer.call"}])], "{inner:?}");
    // "hello" twice and "a", "b" once: the file's loop ran 5 + 5 + 1 + 1 times.
    settled.assert_loop_iterations("@src:lib:clean.each", 12)?;
    project.finish().await
}

/// A cut spelled through a site runs inside that one call: starting at
/// `one.strip` runs the rest of that call and the next call, and nothing
/// upstream, nothing in the other branches of the file that the target
/// does not need, and nothing in the other sites.
#[tokio::test]
async fn a_cut_spelled_through_a_site_runs_that_one_call() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_calls", disp).await?;

    let stdout = project.weft(&["run", "--json", "--from", r#"one.strip={"text":" cut "}"#, "--target", "two.strip"]).await?;
    project.mark_registered();
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?
        .assert_input("one.strip", "text", &json!(" cut "))?
        .assert_completed("one.loud")?
        .assert_input("two.strip", "text", &json!("CUT"))?
        .assert_untouched("src")?
        .assert_untouched("one.tally")?
        .assert_untouched("two.loud")?
        .assert_untouched("out")?
        .assert_untouched("outer.call.strip")?
        .assert_untouched("total")?;

    // Up to a node inside a call: the file runs from its site, and the
    // rest of the program does not.
    let stdout = project.weft(&["run", "--json", "--target", "two.strip"]).await?;
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("src")?.assert_completed("one.loud")?.assert_completed("two.strip")?
        .assert_untouched("two.loud")?.assert_untouched("out")?.assert_untouched("total")?;

    // Before a node inside a call: the call starts, the node does not.
    let stdout = project.weft(&["run", "--json", "--from", r#"one.strip={"text":"x"}"#, "--before", "two.strip"]).await?;
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("one.loud")?.assert_untouched("two.strip")?.assert_untouched("two.loud")?;

    // A site inside a loop is cut with the loop.
    let refused = project.weft_refused(&["run", "--target", "outer.call.strip"]).await?;
    anyhow::ensure!(refused.contains("inside loop"), "{refused}");
    project.finish().await
}

/// `weft events` reads and writes a node of an included file the way the
/// source does: `--node one.strip` is the file's node under the call
/// `one` and only that, and every row prints its node the same way, so
/// two calls of one file read apart. The file's own id is nobody's to
/// type.
#[tokio::test]
async fn events_are_addressed_through_the_site() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_calls", disp).await?;
    let stdout = project.weft(&["run", "--json"]).await?;
    project.mark_registered();
    let color = color_of(&stdout)?;
    project.settled(color).await?.completed()?;
    let rows: Vec<Value> = serde_json::from_str(project.weft(&["events", &color.to_string(), "--node", "one.strip", "--json"]).await?.trim())?;
    anyhow::ensure!(!rows.is_empty(), "the call's rows");
    anyhow::ensure!(rows.iter().all(|r| r["node"] == json!("one.strip") && r["frames"] == json!([{"site": "one"}])), "{rows:?}");
    let all: Vec<Value> = serde_json::from_str(project.weft(&["events", &color.to_string(), "--json"]).await?.trim())?;
    let strips: std::collections::BTreeSet<&str> = all.iter().filter_map(|r| r["node"].as_str()).filter(|n| n.ends_with(".strip")).collect();
    anyhow::ensure!(strips == ["one.strip", "outer.call.strip", "two.strip"].into_iter().collect(), "{strips:?}");
    let refused = project.weft_refused(&["events", &color.to_string(), "--node", "@src:lib:clean.strip"]).await?;
    anyhow::ensure!(refused.contains("inside an included file"), "{refused}");
    project.finish().await
}

/// A cut that starts inside an included file freezes with its start
/// spelled through the site, and runs again from the file.
#[tokio::test]
async fn a_frozen_cut_inside_an_included_file_replays() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("include_calls", disp).await?;
    let stdout = project.weft(&["run", "--json", "--from", r#"one.strip={"text":" frozen "}"#, "--target", "two.strip"]).await?;
    project.mark_registered();
    let color = color_of(&stdout)?;
    project.settled(color).await?.completed()?.assert_input("two.strip", "text", &json!("FROZEN"))?;
    project.weft(&["freeze", "inner", &color.to_string(), "--expect", "two.strip"]).await?;
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(project.dir().join("examples/inner.json"))?)?;
    anyhow::ensure!(spec["from"]["one.strip"]["text"] == json!(" frozen "), "{spec}");
    anyhow::ensure!(spec["expected"]["focus"] == json!(["two.strip"]), "{spec}");
    let stdout = project.weft(&["run", "inner", "--json"]).await?;
    let again = project.settled(color_of(&stdout)?).await?;
    again.completed()?.assert_input("two.strip", "text", &json!("FROZEN"))?.assert_untouched("src")?.assert_untouched("out")?;
    project.finish().await
}

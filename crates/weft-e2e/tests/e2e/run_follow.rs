//! Following a run and reading the values its events name: a fast run's
//! bursts reach `weft run` whole, and a value an event names by where the
//! run's record keeps it reads back through `POST /runs/{id}/values`, on
//! a run of a loop and on a run seeded from it.
#![cfg(feature = "e2e")]

use serde_json::{json, Value};

use common::{inherited, run_id_of};
use weft_e2e::{ensure, project::Project, run::SettledRun};

use crate::common;

/// `weft run` follows a fast run to its end: every iteration's line is
/// printed once, and the run's completion is the last thing it says.
#[tokio::test]
async fn a_fast_run_is_followed_to_its_end_with_every_event() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("run_follow", disp).await?;

    let printed = project.weft(&["run"]).await?;
    anyhow::ensure!(!printed.contains("live updates"), "the follow never lost its stream:\n{printed}");
    let completions = printed.lines().filter(|line| line.contains("node_completed") && line.contains("lines.line")).count();
    anyhow::ensure!(completions == 400, "every iteration's line, once: {completions} printed");
    let last = printed.lines().rev().find(|line| line.contains("run=")).unwrap_or_default();
    anyhow::ensure!(last.contains("completed run="), "it ends on the run's completion: {last}");

    project.finish().await
}

/// The values a run's events name read back as the events with their
/// values written in show them: an iteration's item (a part of the stream
/// item), its output, the gathered list, on the run and on a run seeded
/// from it that reuses the loop.
#[tokio::test]
async fn the_values_a_run_names_read_back_on_a_run_and_its_seeded_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("run_follow", disp).await?;

    let stdout = project.weft(&["run", "--json"]).await?;
    let first = SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?;
    first.completed()?;
    let stdout = project.weft(&["run", "--json", "--seed"]).await?;
    let seeded = SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?;
    seeded.completed()?;
    anyhow::ensure!(inherited(&seeded, "out"), "the seeded run reuses what the first one did");

    for run in [first.run_id, seeded.run_id] {
        let by_reference: Vec<Value> = project.dispatcher().get_json(&format!("/runs/{run}/replay")).await?;
        let written: Vec<Value> = project.dispatcher().get_json(&format!("/runs/{run}/replay?with_values=true")).await?;
        anyhow::ensure!(by_reference.len() == written.len(), "the same events either way");
        // Every value a firing's start or end names, by reference, with
        // the value it reads as.
        let mut refs = Vec::new();
        let mut expected = Vec::new();
        for (reference, value) in by_reference.iter().zip(&written) {
            for side in ["input", "output"] {
                let (Some(Value::Object(at)), Some(Value::Object(is))) = (reference.get(side), value.get(side)) else { continue };
                for (port, at) in at {
                    refs.push(at.clone());
                    expected.push(is[port].clone());
                }
            }
        }
        anyhow::ensure!(refs.len() > 800, "a value per iteration's input and output at least: {}", refs.len());
        anyhow::ensure!(refs.iter().any(|at| at["from"] == "list"), "the gathered list is a list of what each iteration wrote");
        let read: Value = project.dispatcher().post_json(&format!("/runs/{run}/values"), &json!({ "refs": refs })).await?;
        let read = read["values"].as_array().cloned().unwrap_or_default();
        anyhow::ensure!(read.len() == expected.len(), "one answer per reference");
        for (i, (answer, value)) in read.iter().zip(&expected).enumerate() {
            anyhow::ensure!(answer.get("value") == Some(value), "value {i} of run {run} reads back as {value}: {answer}");
        }
    }

    project.finish().await
}

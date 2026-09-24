//! Layer-4: growing a program with the version tree, where the program
//! parks and branches. The `version_tree_grow` fixture holds four
//! branches off one source: an `@include`, a HumanQuery, a Wait on a
//! timer, and a Loop. The rig proves an included file as its group,
//! runs a saved example, answers its human step, reviews its outputs, wakes a
//! timer (and is refused on a form), and sees a loop go stale whole.
#![cfg(feature = "e2e")]

mod common;

use common::{color_of, inherited, summary_of, warnings_of};

use serde_json::{json, Value};
use weft_e2e::{ensure, human, project::Project, run, SettledRun};

/// An included file is proved as its group, with its boundary input
/// handed in. Starting inside it stays carved, and a seed stops before
/// the whole loop rather than cutting into its body.
#[tokio::test]
async fn an_include_runs_as_its_group_and_a_loop_goes_stale_whole() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree_grow", disp).await?;

    // The include alone: `src` never runs, the group's node does.
    let stdout = project
        .weft(&["run", "--json", "--group", r#"triage={"text":"quiet"}"#])
        .await?;
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?.assert_completed("triage.up")?.assert_completed("triage.unrelated")?.assert_untouched("src")?.assert_untouched("shout")?;
    settled.assert_input("triage.up", "text", &json!("quiet"))?;
    anyhow::ensure!(summary_of(&stdout).contains("1 input backup"), "{}", summary_of(&stdout));

    let stdout = project.weft(&["run", "--json", "--group", r#"triage={"text":"quiet"}"#, "--seed"]).await?;
    let reused = project.settled(color_of(&stdout)?).await?;
    reused.completed()?;
    anyhow::ensure!(inherited(&reused, "triage.up") && inherited(&reused, "triage.unrelated"),
        "an unchanged group cut must reuse its completed work");

    for (endpoint, member, source, after) in [
        ("triage", "triage.up", "src", "shout"),
        ("echoes", "echoes.step", "nums", "total"),
    ] {
        let stdout = project.weft(&["run", "--json", "--target", endpoint]).await?;
        let run = project.settled(color_of(&stdout)?).await?;
        run.completed()?.assert_completed(source)?.assert_completed(member)?.assert_untouched(after)?;
        if endpoint == "triage" {
            run.assert_completed("triage.unrelated")?;
        } else {
            run.assert_loop_iterations("echoes", 2)?;
        }
        let stdout = project.weft(&["run", "--json", "--before", endpoint]).await?;
        project.settled(color_of(&stdout)?).await?.completed()?
            .assert_completed(source)?.assert_untouched(member)?.assert_untouched(after)?;
    }

    // A port a step inside needs, handed nothing, is refused before the
    // run starts (running it would only skip everything behind it), and
    // the refusal names the port and the step. It never widens the group.
    let refused = project.weft_refused(&["run", "--group", "triage"]).await?;
    anyhow::ensure!(
        refused.contains("triage.text gets nothing in this run") && refused.contains("triage.first.text"),
        "{refused}"
    );

    // A `--from` inside the include takes values at that node.
    let stdout = project
        .weft(&["run", "--json", "--from", r#"triage.up={"text":"inner"}"#, "--target", "shout"])
        .await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_input("shout", "data", &json!("INNER"))?
        .assert_untouched("src")?.assert_untouched("triage.first")?.assert_untouched("triage.unrelated")?;
    let stdout = project.weft(&["run", "--json", "--from", r#"triage={"text":"entry"}"#]).await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_input("shout", "data", &json!("ENTRY"))?.assert_untouched("src")?;
    project.weft_refused(&["run", "--group", "triage", "--from", "triage"]).await?;
    let stdout = project.weft(&["run", "--json", "--from", r#"triage={"text":"cut"}"#, "--before", "triage.up"]).await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_completed("triage.first")?
        .assert_untouched("triage.up")?.assert_untouched("triage.last")?.assert_untouched("triage.unrelated")?.assert_untouched("shout")?;
    let stdout = project.weft(&["run", "--json", "--from", r#"triage={"text":"cut"}"#, "--target", "triage.up"]).await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_completed("triage.up")?
        .assert_untouched("triage.last")?.assert_untouched("triage.unrelated")?.assert_untouched("shout")?;
    let stdout = project.weft(&["run", "--json", "--from", r#"triage={"text":"blocked","_should_flow":false}"#]).await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_skipped("triage.up")?.assert_skipped("shout")?;

    let stdout = project.weft(&["run", "--json", "--group", r#"echoes={"values":[3,5]}"#]).await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_loop_iterations("echoes", 2)?
        .assert_untouched("nums")?.assert_untouched("total")?;
    let stdout = project.weft(&["run", "--json", "--from", r#"echoes={"values":[3,5]}"#]).await?;
    project.settled(color_of(&stdout)?).await?.completed()?.assert_input("total", "data", &json!([6,10]))?
        .assert_untouched("nums")?;

    // The loop: two iterations, then one stale node re-runs the loop whole.
    let stdout = project.weft(&["run", "--json", "--target", "total"]).await?;
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?.assert_loop_iterations("echoes", 2)?.assert_input("total", "data", &json!([2, 4]))?;
    project.weft_refused(&["run", "--seed", "--seed-before", "echoes.step"]).await?;
    let stdout = project.weft(&["run", "--json", "--target", "total", "--seed", "--seed-until", "nums"]).await?;
    let settled = project.settled(color_of(&stdout)?).await?;
    settled.completed()?.assert_loop_iterations("echoes", 2)?.assert_input("total", "data", &json!([2, 4]))?;
    anyhow::ensure!(inherited(&settled, "nums"), "the loop's source is inherited");
    anyhow::ensure!(!inherited(&settled, "echoes.step"), "the loop body ran again");

    project.finish().await
}

/// Finite supplied generators close normally, including the empty stream,
/// and a frozen example preserves that use case without executing its source.
#[tokio::test]
async fn finite_supplied_streams_freeze_and_run_without_the_original_producer() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("range_stream", disp).await?;
    for (name, supply, expected) in [
        ("items", r#"nums={"values":[3,5]}"#, json!([6,10])),
        ("empty", r#"nums={"values":[]}"#, json!([])),
    ] {
        let out = project.weft(&["run", "--json", "--emit", supply]).await?;
        let color = color_of(&out)?;
        SettledRun::observe(project.dispatcher(), color).await?.completed()?
            .assert_untouched("nums")?.assert_input("out", "data", &expected)?;
        // The loop's gathered list is frozen as the loop's own output.
        project.weft(&["freeze", name, &color.to_string(), "--expect", "doubler"]).await?;
        let accepted = std::fs::read_to_string(project.dir().join(format!("examples/{name}.json")))?;
        let out = project.weft(&["run", name, "--json"]).await?;
        let replay = color_of(&out)?;
        SettledRun::observe(project.dispatcher(), replay).await?.completed()?
            .assert_untouched("nums")?.assert_input("out", "data", &expected)?;
        project.weft(&["diff", &replay.to_string(), &format!("example:{name}"), "--full"]).await?;
        anyhow::ensure!(std::fs::read_to_string(project.dir().join(format!("examples/{name}.json")))? == accepted,
            "running and comparing cannot replace accepted evidence");
    }
    for flag in ["--from", "--target", "--before", "--emit"] {
        let node = if flag == "--emit" { r#"doubler.step={"out":1}"# } else { "doubler.step" };
        let refused = project.weft_refused(&["run", flag, node]).await?;
        anyhow::ensure!(refused.contains("inside loop"), "{refused}");
    }
    project.finish().await
}

#[tokio::test]
async fn frozen_carved_inputs_survive_seed_cleanup_and_explicit_cut_repair() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree_grow", disp).await?;
    let out = project.weft(&["run", "--json", "--target", "shout"]).await?;
    let seed = color_of(&out)?;
    project.settled(seed).await?.completed()?;
    let out = project.weft(&["run", "--json", "--seed", "--from", "triage.up", "--target", "shout"]).await?;
    let carved = color_of(&out)?;
    project.settled(carved).await?.completed()?.assert_input("shout", "data", &json!("HELLO"))?;
    project.weft(&["freeze", "carved", &carved.to_string(), "--expect", "triage.last"]).await?;
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(project.dir().join("examples/carved.json"))?)?;
    anyhow::ensure!(spec["from"]["triage.up"]["text"] == json!("hello"), "{spec}");
    project.weft(&["clean", &seed.to_string(), "--yes"]).await?;
    project.write_file("prompts/greeting.txt", "a different source")?;
    let out = project.weft(&["run", "carved", "--json"]).await?;
    project.settled(color_of(&out)?).await?.completed()?
        .assert_input("shout", "data", &json!("HELLO"))?.assert_untouched("src")?;
    let path = project.dir().join("src/triage.weft");
    project.write_file("src/triage.weft", &std::fs::read_to_string(path)?.replace("up =", "upper =").replace("up.text", "upper.text").replace("up.out", "upper.out"))?;
    let refused = project.weft_refused(&["run", "carved"]).await?;
    anyhow::ensure!(refused.contains("triage.up"), "{refused}");
    let out = project.weft(&["run", "carved", "--json", "--clear", "from", "--from",
        r#"triage.upper={"text":"repaired","removed_port":1}"#]).await?;
    project.settled(color_of(&out)?).await?.completed()?
        .assert_input("shout", "data", &json!("REPAIRED"))?.assert_untouched("src")?;
    anyhow::ensure!(!warnings_of(&out).is_empty(), "removed input port is explained");
    project.finish().await
}

/// Running a frozen example asks the human again. Its old answer is evidence,
/// and reviewing a changed answer does not fail the diff command.
#[tokio::test]
async fn a_frozen_example_runs_with_a_new_human_answer_and_explicit_acceptance() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree_grow", disp.clone()).await?;
    let pid = project.id();

    // A run to the gate parks on `review`; the rig plays the person.
    let stdout = project.weft(&["run", "--json", "--target", "gate"]).await?;
    let first = color_of(&stdout)?;
    run::wait_for_status(&disp, first, "waiting_for_input").await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    project.settled(first).await?.completed()?.assert_input("gate", "data", &json!(true))?;

    // Frozen: the answer and what the person was shown ride along.
    let frozen: Value = serde_json::from_str(project.weft(&["freeze", "approve", &first.to_string(), "--json"]).await?.trim())?;
    anyhow::ensure!(frozen["wires"].as_u64().unwrap_or(0) >= 2, "{frozen}");
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(project.dir().join("examples/approve.json"))?)?;
    let answers = spec["answers"].as_array().cloned().unwrap_or_default();
    anyhow::ensure!(answers.len() == 1 && answers[0]["node"] == json!("review"), "one answer, on review: {spec}");
    anyhow::ensure!(answers[0]["payload"]["decision"] == json!("approve"), "{spec}");
    anyhow::ensure!(answers[0]["question"]["context"] == json!("hello"), "the question the person saw: {spec}");

    let out = project.weft(&["run", "approve", "--json"]).await?;
    let parked = color_of(&out)?;
    anyhow::ensure!(parked != first, "an example starts a new run");
    run::wait_for_status(&disp, parked, "waiting_for_input").await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::answer_form(&disp, &review, &json!({ "decision": "reject" })).await?;
    project.settled(parked).await?.completed()?;
    let diff: Value = serde_json::from_str(project.weft(&["diff", "example:approve", &parked.to_string(), "--json"]).await?.trim())?;
    anyhow::ensure!(diff["same"] == json!(false), "changed outputs are reviewable: {diff}");
    project.weft(&["freeze", "approve", &parked.to_string(), "--expect", "gate"]).await?;
    let accepted: Value = serde_json::from_str(&std::fs::read_to_string(project.dir().join("examples/approve.json"))?)?;
    anyhow::ensure!(accepted["expected"]["focus"] == json!(["gate"]), "{accepted}");
    anyhow::ensure!(accepted["answers"][0]["payload"]["decision"] == json!("reject"), "{accepted}");
    let examples: Value = serde_json::from_str(project.weft(&["examples", "--json"]).await?.trim())?;
    anyhow::ensure!(examples[0]["last_run"] == json!(parked.to_string()) && examples[0].get("verdict").is_none(), "{examples}");

    project.finish().await
}

/// `weft wake` resolves a pure time wait now and refuses a wait that
/// expects a value, naming its kind.
#[tokio::test]
async fn wake_resolves_a_timer_and_refuses_a_form() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree_grow", disp.clone()).await?;

    let stdout = project.weft(&["run", "--json", "--target", "late"]).await?;
    let held = color_of(&stdout)?;
    run::wait_for_status(&disp, held, "waiting_for_input").await?;
    let refused = project.weft_refused(&["wake", &held.to_string(), "src"]).await?;
    anyhow::ensure!(refused.contains("is not waiting on anything"), "{refused}");
    let woke: Value = serde_json::from_str(project.weft(&["wake", &held.to_string(), "hold", "--json"]).await?.trim())?;
    anyhow::ensure!(woke["node"] == json!("hold"), "{woke}");
    project.settled(held).await?.completed()?.assert_input("late", "data", &json!("hello"))?;

    let stdout = project.weft(&["run", "--json", "--target", "gate"]).await?;
    let asked = color_of(&stdout)?;
    run::wait_for_status(&disp, asked, "waiting_for_input").await?;
    let refused = project.weft_refused(&["wake", &asked.to_string(), "review"]).await?;
    anyhow::ensure!(refused.contains("waiting on a form signal, which expects a value"), "{refused}");
    project.weft(&["stop", &asked.to_string()]).await?;

    project.finish().await
}

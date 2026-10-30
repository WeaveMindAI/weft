//! Layer-4: the project-lifecycle state machine, transition by transition.
//!
//! `compute_available_actions` (crates/weft-dispatcher/src/api/project.rs)
//! defines the reconciliation table: every stable state
//! offers exactly its legal verbs, and every transitional state offers ONLY
//! its cancel. Every verb that takes triggers down answers the same three
//! questions (`weft_core::take_down`: the door, what is held, what is
//! running). These tests drive ONE evolving project per test through real
//! verbs (CLI + HTTP, as a user would) and assert the table's answer
//! (`available_actions`), each trigger's state (`triggers`), the rejections
//! (REJ cells must 4xx), and what becomes of the runs (`--running wait`
//! lets them finish on their own version and the command goes on only once
//! they have: a deactivate answers, a resync brings its triggers back on
//! the new build, an infra stop lets the infra go; `cancel` stops them).
//!
//! The lever for "a real run is running" is the `lifecycle` fixture's
//! HoldGate node: it polls a rig-owned fake URL until the rig releases it, so
//! the rig can hold a run open across a take-down and finish it on cue.
//! Source-shape transitions (no-infra -> infra -> orphan, plain -> trigger)
//! are real `main.weft` rewrites followed by the next verb, exactly as a user
//! editing source.
//!
//! Coverage that deliberately does NOT live here:
//!   - `cancel_build`: the build gate only engages when there is a builder; the
//!     local CLI builds before the verb, so there is no `building` window to
//!     cancel against this rig.
//!   - `cancel_activate`: trigger setup on this rig completes in milliseconds
//!     (no deterministic `activating` window to race); the same master rule
//!     is covered below through the window an infra stop's `--running wait`
//!     holds open.
#![cfg(feature = "e2e")]

use crate::common;

use std::time::Duration;

use common::*;
use serde_json::json;
use weft_e2e::client::cli;
use weft_e2e::fakes::{PollFake, SseFake};
use weft_e2e::status::{self, STATUS_DEADLINE};
use weft_e2e::{ensure, human, infra, run, Project, SettledRun};

/// Source flips the infra concern on and off: a no-infra project offers no
/// infra verbs; adding MiniService to source and starting infra lights the
/// infra controls; removing it from source orphans the LIVE infra (visible,
/// non-gating, still stoppable) until the user terminates it.
#[tokio::test]
async fn source_flips_infra_and_orphan_lifecycle() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    // Base shape: no infra. Release up-front so runs settle immediately.
    let gate = PollFake::start(RELEASE).await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &gate.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?.assert_input("out", "data", &json!("released"))?;

    // Stable no-infra row: run is the only verb (no triggers, no infra).
    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(!s.has_infra() && !s.orphaned_infra(), "no-infra project reports infra");
    s.assert_actions_exactly(&["run"])?;

    // Source gains infra: import the node, rewrite main, start infra.
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.set_main(&graph_infra())?;
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;

    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(s.has_infra(), "registered definition must declare infra now");
    s.assert_actions_exactly(&["run", "infra_stop", "infra_terminate"])?;

    // A run against running infra works.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // Source LOSES infra while it is live: the next verb re-registers the
    // no-infra shape; the live infra becomes an ORPHAN: visible, offered
    // stop/terminate, no start (nothing to provision from), and it does NOT
    // gate run.
    project.set_main(&graph_hold(&gate.url()))?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(!s.has_infra(), "source no longer declares infra");
    anyhow::ensure!(s.orphaned_infra(), "live infra with no source node must be an orphan");
    s.assert_actions_exactly(&["run", "infra_stop", "infra_terminate"])?;

    // Terminating the orphan clears the last infra trace.
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(!s.orphaned_infra(), "terminated orphan must clear the orphan flag");
    s.assert_actions_exactly(&["run"])?;

    project.finish().await
}

/// The infra concern's REJ cells, the stop that waits for its runs, and
/// infra cancel: resting infra gates run; a stop with `--running wait`
/// over a running run holds the infra up (master rule: only
/// `infra_cancel` offered, run rejected, the run keeps running); `weft
/// infra cancel` calls the stop off WITHOUT killing the run; the released
/// run then completes untouched.
#[tokio::test]
async fn infra_stop_drains_and_cancel_halts() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(HOLD).await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.set_main(&graph_infra_hold(&gate.url()))?;

    // Run before infra is up: the CLI registers, then the dispatcher rejects
    // (declared infra not running). That registration makes the resting row
    // observable.
    let attempt = cli(project.dispatcher(), project.dir(), &["run", "--json"]).await?;
    anyhow::ensure!(
        !attempt.success,
        "run must fail while declared infra is not running; stdout: {}",
        attempt.stdout
    );
    let s = status::fetch(&disp, &pid).await?;
    s.assert_actions_exactly(&["infra_start"])?;
    assert_run_rejected(&project, "infra resting", INFRA_GATE).await?;

    // Provision, then hold a run open.
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    status::fetch(&disp, &pid)
        .await?
        .assert_actions_exactly(&["run", "infra_stop", "infra_terminate"])?;
    let run_id = run::start(&mut project).await?;
    status::wait_until(&disp, &pid, "run to be live", STATUS_DEADLINE, |s| {
        s.running_count() >= 1
    })
    .await?;

    // Stop with `--running wait` (an infra stop's default, spelled out):
    // the infra stays up until the run reaching it ends. While the stop
    // waits the master rule collapses the table to its cancel, and nothing
    // new is admitted.
    let stop = spawn_weft(&project, vec!["infra".into(), "stop".into(), "--running".into(), "wait".into()]);
    status::wait_until(&disp, &pid, "stop drain to gate the table", STATUS_DEADLINE, |s| {
        s.available_actions() == vec!["infra_cancel"]
    })
    .await?;
    assert_run_rejected(&project, "infra stop draining", LIFECYCLE_GATE).await?;
    anyhow::ensure!(
        run_status(&disp, run_id).await? == "running",
        "the waiting stop must not kill the running run"
    );

    // Cancel the stop: HALT, not rollback. The infra stays running, the
    // run stays running, the table returns to the stable running row.
    project.weft(&["infra", "cancel"]).await?;
    status::wait_until(&disp, &pid, "cancelled stop to settle", STATUS_DEADLINE, |s| {
        s.available_actions() == vec!["run", "infra_stop", "infra_terminate"]
            && s.infra_rollup() == "running"
    })
    .await?;
    anyhow::ensure!(
        run_status(&disp, run_id).await? == "running",
        "infra cancel must not touch the running run"
    );
    let stop_out = stop.await??;
    anyhow::ensure!(
        !stop_out.success,
        "the cancelled stop must not report success; stdout: {}",
        stop_out.stdout
    );

    // Release: the held run completes untouched by all of the above.
    gate.set_body(RELEASE).await;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    // Terminate with nothing running: immediate, back to the resting row.
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    status::fetch(&disp, &pid).await?.assert_actions_exactly(&["infra_start"])?;

    project.finish().await
}

/// The trigger axis: activate (again, with nothing off: no error);
/// deactivate with `--running wait` (the default), which takes the trigger
/// down at once and answers only once the run it fired has ended;
/// deactivate with `--running cancel`, which cancels it; resync under
/// `wait`, which lets the run going on the old version finish there and
/// only then brings the trigger back on the new build; and resync under
/// `cancel`, which cancels that run and switches. All against REAL Fire
/// runs held open by the gate.
#[tokio::test]
async fn deactivate_and_resync_wait_for_or_cancel_the_running_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(HOLD).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go", &gate.url()))?;

    project.activate().await?;
    let s = status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    s.assert_actions_exactly(&["run", "activate", "deactivate"])?;
    anyhow::ensure!(s.trigger_state("start", None) == "on", "the trigger is on: {:?}", s.trigger("start"));
    // Activating with nothing off turns nothing on, and says so: a script
    // can always run it.
    let again = summary_of(&project.weft(&["activate", "--json"]).await?);
    anyhow::ensure!(again.contains("nothing to turn on"), "{again}");

    // Deactivate with the defaults: the trigger is down at once, and the
    // command answers only once the run it fired has finished. The gate
    // holds the run, so the deactivate cannot have answered before the
    // release.
    let run_id = fire_and_hold(&feed, &disp, &pid, "go").await?;
    let deactivate = spawn_weft(&project, words(&["deactivate", "--json"]));
    let s = status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    anyhow::ensure!(s.trigger_state("start", None) == "off", "the trigger is down: {:?}", s.trigger("start"));
    anyhow::ensure!(!deactivate.is_finished(), "the deactivate answered while the run it fired was still going");
    anyhow::ensure!(run_status(&disp, run_id).await? == "running", "`--running wait` must leave the running run alone");
    gate.set_body(RELEASE).await;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;
    let said = summary_of(&answered(deactivate).await?);
    anyhow::ensure!(
        said.contains("took down 'start'") && said.contains("1 run waited for, finishing on their own version"),
        "{said}"
    );
    status::fetch(&disp, &pid).await?.assert_actions_exactly(&["run", "activate"])?;

    // Back on, a new run held, and `--running cancel` stops it.
    gate.set_body(HOLD).await;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    let run_id = fire_and_hold(&feed, &disp, &pid, "go").await?;
    let said = summary_of(&project.weft(&["deactivate", "--running", "cancel", "--json"]).await?);
    anyhow::ensure!(said.contains("1 run cancelled"), "{said}");
    let settled = SettledRun::observe(&disp, run_id).await?;
    anyhow::ensure!(settled.status == "cancelled", "`--running cancel` must cancel the held run; got {}", settled.status);

    // Resync under `wait`: back on, a run held, then the source edited
    // (a new event name), so the trigger is behind the build. The resync
    // takes it down, and it stays down while the run on the old version
    // goes; once that run finishes there, the trigger comes back on the
    // new build.
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    let run_id = fire_and_hold(&feed, &disp, &pid, "go").await?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go2", &gate.url()))?;
    let resync = spawn_weft(&project, words(&["resync", "--running", "wait", "--json"]));
    status::wait_until(&disp, &pid, "the resync to take the trigger down", STATUS_DEADLINE, |s| {
        s.transition() == "resyncing" && s.trigger_state("start", None) == "off"
    })
    .await?;
    anyhow::ensure!(!resync.is_finished(), "the resync answered while the old version still had a run going");
    anyhow::ensure!(run_status(&disp, run_id).await? == "running", "the resync must leave the old version's run going");
    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(
        s.trigger_state("start", None) == "off",
        "the trigger stays down until the old version has nothing left running: {:?}",
        s.trigger("start")
    );
    gate.set_body(RELEASE).await;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;
    let said = summary_of(&answered(resync).await?);
    anyhow::ensure!(said.contains("1 run waited for, finishing on their own version"), "{said}");
    let s = status::wait_until(&disp, &pid, "the trigger back on the new build", STATUS_DEADLINE, |s| {
        s.trigger_state("start", None) == "on"
    })
    .await?;
    anyhow::ensure!(s.status() == "active" && !s.offers("resync"), "nothing is behind any more: {:?}", s.available_actions());

    // Resync under `cancel`: the NEW registration fires (a run held on
    // it), the source moves again, and the resync cancels that run and
    // switches at once.
    gate.set_body(HOLD).await;
    let run_id = fire_and_hold(&feed, &disp, &pid, "go2").await?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go3", &gate.url()))?;
    let said = summary_of(&project.weft(&["resync", "--running", "cancel", "--json"]).await?);
    anyhow::ensure!(said.contains("1 run cancelled"), "{said}");
    let settled = SettledRun::observe(&disp, run_id).await?;
    anyhow::ensure!(settled.status == "cancelled", "a resync under `cancel` must cancel the old version's run; got {}", settled.status);
    let s = status::wait_until(&disp, &pid, "the trigger back on the newer build", STATUS_DEADLINE, |s| {
        s.trigger_state("start", None) == "on"
    })
    .await?;
    anyhow::ensure!(s.status() == "active" && !s.offers("resync"), "nothing is behind any more: {:?}", s.available_actions());

    gate.set_body(RELEASE).await;
    let before = run::runs(&disp, &pid).await?;
    let run_id = fire_until_run(&feed, &disp, &pid, "go3", &before).await?;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    project.finish().await
}

/// Push `event` and wait until the run it fires is running (held by the
/// gate, which the caller set to hold).
async fn fire_and_hold(feed: &SseFake, disp: &weft_e2e::Dispatcher, pid: &uuid::Uuid, event: &str) -> anyhow::Result<uuid::Uuid> {
    let before = run::runs(disp, pid).await?;
    let run_id = fire_until_run(feed, disp, pid, event, &before).await?;
    run::wait_for_status(disp, run_id, "running").await?;
    Ok(run_id)
}

/// A command line for [`spawn_weft`].
fn words(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| arg.to_string()).collect()
}

/// What a command [`spawn_weft`] started printed, once it has answered;
/// an error when it failed.
async fn answered(command: tokio::task::JoinHandle<anyhow::Result<weft_e2e::client::CliOutput>>) -> anyhow::Result<String> {
    let out = command.await??;
    anyhow::ensure!(out.success, "`{}` failed:\n{}\n{}", out.invocation, out.stdout, out.stderr);
    Ok(out.stdout)
}

/// A trigger taken down with `--door hide` keeps listening and holds what
/// arrives instead of running it; `weft activate` releases what it held,
/// and it runs. On a second round `weft activate --held drop` drops what
/// was held instead, and nothing runs from it.
#[tokio::test]
async fn a_held_event_runs_when_activate_releases_it_or_is_dropped() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(RELEASE).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go", &gate.url()))?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;

    project.weft(&["deactivate", "--door", "hide"]).await?;
    let s = status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    let copy = s.trigger_copy("start", None).cloned().unwrap_or_default();
    anyhow::ensure!(copy["state"] == "off" && copy["mode"] == "hide", "down, its door hiding: {copy}");

    // The event is held, and nothing runs from it while the trigger is down.
    let before = fire_runs(&disp, &pid).await?;
    push_until_held(&feed, &disp, &pid, "go", "start").await?;
    anyhow::ensure!(fire_runs(&disp, &pid).await? == before, "a held event starts no run");

    // Activate releases it: it runs, on the version the trigger is back on.
    let said = summary_of(&project.weft(&["activate", "--json"]).await?);
    anyhow::ensure!(said.contains("1 arrival released"), "{said}");
    let run_id = wait_for_one_new_fire(&disp, &pid, &before).await?;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;
    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(s.trigger_held("start", None) == (0, 0), "nothing is held once it is back: {:?}", s.trigger("start"));

    // Second round: what is held is dropped instead.
    project.weft(&["deactivate", "--door", "hide"]).await?;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    let before = fire_runs(&disp, &pid).await?;
    push_until_held(&feed, &disp, &pid, "go", "start").await?;
    let said = summary_of(&project.weft(&["activate", "--held", "drop", "--json"]).await?);
    anyhow::ensure!(said.contains("1 arrival dropped"), "{said}");
    let s = status::wait_until(&disp, &pid, "the trigger back on", STATUS_DEADLINE, |s| s.trigger_state("start", None) == "on").await?;
    anyhow::ensure!(s.trigger_held("start", None) == (0, 0), "the dropped event is gone: {:?}", s.trigger("start"));
    // What an activate releases is handed over before it answers (each
    // event's run is born as it is handed over), so a run released by
    // mistake would be on record by now.
    anyhow::ensure!(fire_runs(&disp, &pid).await? == before, "a dropped event never runs");

    project.finish().await
}

/// All three concerns at once: an ACTIVE project with RUNNING infra offers
/// the full verb set; `infra stop` takes down only the triggers that read
/// the infra, so a trigger on another branch keeps listening while
/// nothing that needs the stopped infra is offered; a fresh start brings
/// the full cycle back; terminate returns to resting.
#[tokio::test]
async fn an_infra_stop_takes_down_only_what_reads_it_and_recovers() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(RELEASE).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_trigger_infra_hold(&feed.url(), "go", &gate.url()))?;

    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE)
        .await?
        .assert_actions_exactly(&["run", "activate", "deactivate", "infra_stop", "infra_terminate"])?;

    // Stop while ACTIVE: the stop takes down the triggers whose cut
    // contains this service, and this service sits on its own branch
    // (`svc -> svc_out`, nothing to do with the trigger), so the trigger
    // keeps listening while the service stops.
    project.weft(&["infra", "stop", "--running", "cancel"]).await?;
    let s = status::wait_until(&disp, &pid, "stopped infra under a live trigger", STATUS_DEADLINE, |s| {
        s.status() == "active" && s.infra_rollup() == "stopped"
    })
    .await?;
    anyhow::ensure!(s.trigger_state("start", None) == "on", "the trigger keeps listening: {:?}", s.trigger("start"));
    // `run` is the other half: a run waits on the infra IT touches, and
    // a whole-graph run touches this service, so it is refused by the
    // run's own pre-flight (a run aimed away from it would go).
    // What the TABLE reports; the doors re-check the same rules after
    // their build, in `require_trigger_infra` and `versions::run`. The
    // trigger's setup reads nothing of the service, so `activate` stays
    // offered.
    s.assert_actions_exactly(&["activate", "deactivate", "infra_start", "infra_terminate"])?;
    assert_run_rejected(&project, "infra stopped", INFRA_GATE).await?;
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?
        .assert_actions_exactly(&["run", "activate", "deactivate", "infra_stop", "infra_terminate"])?;

    // The revived deployment actually fires end to end.
    let before = run::runs(&disp, &pid).await?;
    let run_id = fire_until_run(&feed, &disp, &pid, "go", &before).await?;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    // Wind down: deactivate, terminate, resting row.
    project.weft(&["deactivate", "--running", "cancel"]).await?;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    status::fetch(&disp, &pid).await?.assert_actions_exactly(&["activate", "infra_start"])?;

    project.finish().await
}

/// An infra stop takes down only the triggers whose cut contains that
/// infra, so the project is left partly on: status shows the one reading
/// it off and the other still on. Once the infra runs again, the trigger
/// it took down comes back on by itself and fires. One a person switched
/// off themselves stays off through the next stop and start.
#[tokio::test]
async fn an_infra_start_brings_back_the_triggers_its_stop_took_down() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let feed = SseFake::start().await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    // `reads` takes its event name off the service, so it reads that infra;
    // `plain` reads nothing of it.
    project.set_main(&format!(
        "svc = MiniService\n\
         reads = TestSseTrigger -> (value: String) {{ url: \"{url}\" }}\n\
         reads.event_name = svc.status\n\
         heard = Debug\n\
         heard.data = reads.value\n\
         plain = TestSseTrigger -> (value: String) {{\n\
         \x20 url: \"{url}\"\n\
         \x20 event_name: \"plain\"\n\
         }}\n\
         also = Debug\n\
         also.data = plain.value\n",
        url = feed.url()
    ))?;

    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    project.activate().await?;
    let s = status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    // Both sides of the cut, as status reads them before anything stops.
    anyhow::ensure!(s.trigger_depends_on("reads") == vec![INFRA_NODE], "{:?}", s.trigger("reads"));
    anyhow::ensure!(s.trigger_depends_on("plain").is_empty(), "{:?}", s.trigger("plain"));
    let takes_down = s.infra_takes_down(INFRA_NODE);
    anyhow::ensure!(
        takes_down == vec!["reads"],
        "stopping the service takes `reads` down and nothing else: {takes_down:?}"
    );

    let said = summary_of(&project.weft(&["infra", "stop", "--infra", INFRA_NODE, "--running", "cancel", "--json"]).await?);
    anyhow::ensure!(said.contains("'reads'") && !said.contains("'plain'"), "the stop names what it took down: {said}");
    // `plain` still listens, so the project reads active; `reads` is down,
    // and turning it on waits for its infra.
    let s = status::wait_until(&disp, &pid, "the stop took down the trigger reading it", STATUS_DEADLINE, |s| {
        s.status() == "active" && s.infra_rollup() == "stopped"
    })
    .await?;
    s.assert_actions_exactly(&["deactivate", "infra_start", "infra_terminate"])?;
    anyhow::ensure!(
        s.trigger_state("reads", None) == "off" && s.trigger_state("plain", None) == "on",
        "only the trigger reading the infra goes down: reads {:?}, plain {:?}",
        s.trigger("reads"),
        s.trigger("plain")
    );

    // The start brings `reads` back with its infra: nothing is left off.
    project.weft(&["infra", "start"]).await?;
    status::wait_until(&disp, &pid, "the start brought the trigger back", STATUS_DEADLINE, |s| {
        s.infra_rollup() == "running" && s.trigger_state("reads", None) == "on" && s.trigger_state("plain", None) == "on"
    })
    .await?
    .assert_actions_exactly(&["run", "activate", "deactivate", "infra_stop", "infra_terminate"])?;

    // `plain` never let go of the feed, so one subscriber says nothing about
    // `reads`: wait until both are listening before pushing.
    feed.wait_for_subscribers(2, std::time::Duration::from_secs(60)).await?;
    let before = run::runs(&disp, &pid).await?;
    let run_id = fire_until_run(&feed, &disp, &pid, "ready", &before).await?;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("heard", "data", &json!("go"))?;

    // Switched off by a person, `reads` stays off through a stop and a
    // start: only what the stop itself took down comes back.
    project.weft(&["deactivate", "--trigger", "reads", "--running", "cancel"]).await?;
    project.weft(&["infra", "stop", "--running", "cancel"]).await?;
    status::wait_until(&disp, &pid, "the infra stopped", STATUS_DEADLINE, |s| s.infra_rollup() == "stopped").await?;
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    let s = status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    s.assert_actions_exactly(&["run", "deactivate", "activate", "infra_stop", "infra_terminate"])?;
    anyhow::ensure!(s.trigger_state("reads", None) == "off", "a trigger a person took down stays down: {:?}", s.trigger("reads"));

    project.weft(&["deactivate", "--door", "refuse", "--held", "drop", "--running", "cancel"]).await?;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    project.finish().await
}

/// What a deploy does: the commands the deploy workflow runs on every
/// push (`weft new`'s `ci/gcp.yml`), in its order and with its
/// `--running wait`, bring up to date only what a person left on and
/// running. On a project never deployed they succeed and turn nothing on
/// and start nothing. Then, with trigger `a` on and `b` switched off by
/// hand, and infra `svc` running and `db` stopped by hand, a code change
/// that moves all four followed by the same commands brings `a` onto the
/// new build and replaces `svc`, while `b` stays off and `db` stays
/// stopped (and behind, for the person who starts it).
#[tokio::test]
async fn a_deploy_brings_up_to_date_only_what_is_on_and_running() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    // One feed per trigger, so a push reaches only the trigger it is for.
    let feed_a = SseFake::start().await?;
    let feed_b = SseFake::start().await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_two_of_each(&feed_a, &feed_b, false))?;

    // Never deployed: every command succeeds and touches nothing.
    let said = deploy(&project).await?;
    anyhow::ensure!(
        said[1].contains("nothing to resync"),
        "a resync with nothing on resyncs nothing: {said:?}"
    );
    let s = status::fetch(&disp, &pid).await?;
    for trigger in ["a", "b"] {
        anyhow::ensure!(s.trigger_state(trigger, None) != "on", "a deploy turns no trigger on: {:?}", s.trigger(trigger));
    }
    for place in [INFRA_NODE, "db"] {
        anyhow::ensure!(
            s.infra_state(place) == weft_core::infra::wire::INFRA_NOT_STARTED,
            "a deploy starts no infra: {:?}",
            s.infra_place(place)
        );
    }
    s.assert_actions_exactly(&["activate", "infra_start"])?;

    // Everything on and running, then `b` and `db` taken down by hand.
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    infra::wait_running(&project, "db").await?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    project.weft(&["deactivate", "--trigger", "b", "--running", "wait"]).await?;
    project.weft(&["infra", "stop", "--infra", "db", "--running", "wait"]).await?;
    status::wait_until(&disp, &pid, "`b` off and `db` stopped", STATUS_DEADLINE, |s| {
        s.trigger_state("a", None) == "on"
            && s.trigger_state("b", None) == "off"
            && s.infra_state(INFRA_NODE) == "running"
            && s.infra_state("db") == "stopped"
    })
    .await?;

    // The change moves both triggers (new event names) and both services
    // (made public), so all four are behind the build.
    project.set_main(&graph_two_of_each(&feed_a, &feed_b, true))?;
    let said = deploy(&project).await?;
    anyhow::ensure!(said[1].contains("resynced") && said[1].contains("'a'") && !said[1].contains("'b'"), "{said:?}");
    let s = status::wait_until(&disp, &pid, "the deploy to settle", STATUS_DEADLINE, |s| {
        s.transition() == "none" && s.infra_state(INFRA_NODE) == "running" && s.trigger_state("a", None) == "on"
    })
    .await?;
    anyhow::ensure!(s.trigger_state("b", None) == "off", "a trigger switched off by hand stays off: {:?}", s.trigger("b"));
    anyhow::ensure!(!s.offers("resync"), "no trigger that is on is behind any more: {:?}", s.available_actions());
    anyhow::ensure!(!s.infra_behind(INFRA_NODE), "the running service is on the new build: {:?}", s.infra_place(INFRA_NODE));
    anyhow::ensure!(
        s.infra_state("db") == "stopped" && s.infra_behind("db"),
        "a service stopped by hand stays stopped, and behind: {:?}",
        s.infra_place("db")
    );
    // The replaced copy runs the new source: it is public now.
    let copies = infra::status(&disp, &pid).await?;
    let svc = copies.iter().find(|c| c.node == INFRA_NODE && c.instance.is_none());
    anyhow::ensure!(svc.is_some_and(|c| c.public_urls.contains_key("api")), "the replaced service is public: {svc:?}");

    // `a` fires on its new event name.
    let before = run::runs(&disp, &pid).await?;
    let run_id = fire_until_run(&feed_a, &disp, &pid, "a2", &before).await?;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("a_out", "data", &json!("go"))?;

    project.weft(&["deactivate", "--door", "refuse", "--held", "drop", "--running", "cancel"]).await?;
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    infra::wait_gone(&project, "db").await?;
    project.finish().await
}

/// A node in front of a trigger runs at the trigger's setup and never at
/// a fire, so an edit to it is what puts the trigger behind: `weft status`
/// (which reads the source) shows it, and `weft resync` sets the trigger
/// up again, running the edited node, so the trigger then listens with
/// what the node gives now.
#[tokio::test]
async fn an_edit_in_front_of_a_trigger_puts_it_behind_and_a_resync_runs_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_named_in_front(&feed, "first"))?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;

    project.set_main(&graph_named_in_front(&feed, "second"))?;
    let s = status::ProjectStatus(serde_json::from_str(&project.weft(&["status", "--json"]).await?)?);
    anyhow::ensure!(s.trigger_state("a", None) == "behind", "the edit in front of `a` puts it behind: {:?}", s.trigger("a"));
    let said = summary_of(&project.weft(&["resync", "--running", "wait", "--json"]).await?);
    anyhow::ensure!(said.contains("resynced") && said.contains("'a'"), "the resync takes `a` up: {said:?}");
    status::wait_until(&disp, &pid, "`a` back on", STATUS_DEADLINE, |s| s.trigger_state("a", None) == "on").await?;

    // It listens for the event the edited node names now.
    let before = run::runs(&disp, &pid).await?;
    let run_id = fire_until_run(&feed, &disp, &pid, "second", &before).await?;
    SettledRun::observe(&disp, run_id).await?.completed()?.assert_input("a_out", "data", &json!("go"))?;

    project.weft(&["deactivate", "--door", "refuse", "--held", "drop", "--running", "cancel"]).await?;
    project.finish().await
}

/// Graph: trigger `a` listening on `feed` for the event a Text node in
/// front of it names, into a Debug.
fn graph_named_in_front(feed: &SseFake, event: &str) -> String {
    format!(
        "pick = Text {{\n\
         \x20 value: \"{event}\"\n\
         }}\n\
         a = TestSseTrigger -> (value: String) {{\n\
         \x20 url: \"{url}\"\n\
         }}\n\
         a.event_name = pick.value\n\
         a_out = Debug\n\
         a_out.data = a.value\n",
        url = feed.url(),
    )
}

/// Graph: triggers `a` and `b`, each on its own feed into a Debug, and
/// two MiniServices `svc` and `db`, each into a Debug, every branch apart
/// from the others. `changed` renames both events and makes both services
/// public, which puts all four behind the build that had them unchanged.
fn graph_two_of_each(feed_a: &SseFake, feed_b: &SseFake, changed: bool) -> String {
    let (suffix, service) = if changed { ("2", "MiniService { public: true }") } else { ("", "MiniService") };
    format!(
        "a = TestSseTrigger -> (value: String) {{\n\
         \x20 url: \"{url_a}\"\n\
         \x20 event_name: \"a{suffix}\"\n\
         }}\n\
         a_out = Debug\n\
         a_out.data = a.value\n\
         b = TestSseTrigger -> (value: String) {{\n\
         \x20 url: \"{url_b}\"\n\
         \x20 event_name: \"b{suffix}\"\n\
         }}\n\
         b_out = Debug\n\
         b_out.data = b.value\n\
         svc = {service}\n\
         svc_out = Debug\n\
         svc_out.data = svc.status\n\
         db = {service}\n\
         db_out = Debug\n\
         db_out.data = db.status\n",
        url_a = feed_a.url(),
        url_b = feed_b.url(),
    )
}

/// What the deploy workflow runs on every push, in its order, each with
/// `--running wait` (its default): the summary each answered.
async fn deploy(project: &Project) -> anyhow::Result<Vec<String>> {
    let mut said = Vec::new();
    for command in [&["infra", "upgrade", "--all"][..], &["resync", "--all"]] {
        let mut args = command.to_vec();
        args.extend(["--running", "wait", "--json"]);
        said.push(summary_of(&project.weft(&args).await?));
    }
    Ok(said)
}

/// A run paused on a person (HumanQuery) survives deactivate + activate:
/// the trigger, down, holds it as a paused run, `weft activate` brings
/// the trigger back without touching it, and the form is still
/// answerable afterwards, resuming the original run to completion.
#[tokio::test]
async fn suspension_survives_deactivate_reactivate() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Fire and run to the HumanQuery suspension.
    let before = run::runs(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;

    // Deactivate with the defaults: what the trigger fired that is paused
    // is kept, and the trigger reads as holding it.
    project.weft(&["deactivate"]).await?;
    let s = status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    anyhow::ensure!(s.offers("activate"), "a trigger that is down can be turned on, got {:?}", s.available_actions());
    anyhow::ensure!(s.trigger_held("start", None) == (0, 1), "the paused run is held: {:?}", s.trigger("start"));
    let status = run_status(&disp, run_id).await?;
    anyhow::ensure!(
        status == "waiting_for_input",
        "deactivate must not touch a suspended run \
         (and the list must report it honestly); got '{status}'"
    );

    // Activate (nothing is ever asked: `--held run`, the default, keeps
    // the paused run waiting on its person), answer the form, and the
    // ORIGINAL run resumes to completion.
    project.weft(&["activate"]).await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    anyhow::ensure!(run_status(&disp, run_id).await? == "waiting_for_input", "activate leaves the paused run waiting");
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    SettledRun::observe(&disp, run_id)
        .await?
        .completed()?
        .assert_input("out", "data", &json!(true))?;

    project.finish().await
}

/// A resync over a run waiting on a person keeps it (`--held keep`, the
/// default) and moves it onto the new build: the trigger comes straight
/// back on, the run keeps waiting, and once the form is answered the
/// ORIGINAL run resumes on the new build, reaching the node the edit added.
#[tokio::test]
async fn a_resync_moves_a_run_waiting_on_a_person_onto_the_new_build() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    let before = run::runs(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;

    // One more node reads the answer, so the trigger's cut moves and it is
    // behind the build the resync makes.
    let main = project.read_file("src/main.weft")?;
    project.write_file("src/main.weft", &format!("{main}\nalso = Debug\nalso.data = review.decisionApproved\n"))?;
    project.weft(&["resync"]).await?;
    let s = status::wait_until(&disp, &pid, "the trigger back on the new build", STATUS_DEADLINE, |s| {
        s.trigger_state("start", None) == "on"
    })
    .await?;
    anyhow::ensure!(s.status() == "active", "the resync brings its trigger straight back: {}", s.status());
    let status = run_status(&disp, run_id).await?;
    anyhow::ensure!(status == "waiting_for_input", "the resync must leave the waiting run waiting; got '{status}'");

    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?.assert_input("out", "data", &json!(true))?;
    settled.assert_input("also", "data", &json!(true))?;

    project.finish().await
}

/// One binary answers a project's routes. With `front` on, a change to
/// the code of a node of the project gives it a new build, and turning
/// `back` on there is refused, naming `front` and the way through; `weft
/// resync --all` moves `front` to the new build (its answers say the new
/// code ran), and `back` then comes on. `weft rm --journal` takes the
/// project away, history and all, waiting in short looks for every run.
#[tokio::test]
async fn a_trigger_on_a_new_build_waits_for_a_resync_of_the_others() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("one_binary", disp.clone()).await?;
    let base = project.unique_live_path()?;
    let heard = |route: &'static str, text: &'static str| {
        let (disp, url) = (disp.clone(), format!("{base}/{route}"));
        async move {
            let (status, _, body) =
                weft_e2e::live::http_json(&disp, reqwest::Method::POST, &url, &[], &json!({ "text": text })).await?;
            anyhow::ensure!(status == 200, "{route} answered {status}: {}", String::from_utf8_lossy(&body));
            Ok::<_, anyhow::Error>(serde_json::from_slice::<serde_json::Value>(&body)?)
        }
    };

    project.weft(&["activate", "--trigger", "front"]).await?;
    assert_eq!(heard("front", "hi").await?, json!({ "heard": "hi" }));

    let code = project.dir().join("nodes/echo/mod.rs");
    let before = std::fs::read_to_string(&code)?;
    let after = before.replace("let said = text;", "let said = format!(\"again: {text}\");");
    anyhow::ensure!(after != before, "the fixture's marked line moved: {}", code.display());
    std::fs::write(&code, after)?;

    let refused = project.weft_refused(&["activate", "--trigger", "back"]).await?;
    anyhow::ensure!(
        refused.contains("one binary answers a project's routes") && refused.contains("'front'") && refused.contains("`weft resync --all`"),
        "the refusal names the trigger on the old build and the way through:\n{refused}"
    );
    assert_eq!(heard("front", "hi").await?, json!({ "heard": "hi" }), "the refusal moved nothing");

    project.weft(&["resync", "--all"]).await?;
    assert_eq!(heard("front", "hi").await?, json!({ "heard": "again: hi" }), "front runs the new code");
    project.weft(&["activate", "--trigger", "back"]).await?;
    assert_eq!(heard("back", "yo").await?, json!({ "heard": "again: yo" }));

    let id = project.id().to_string();
    project.weft(&["rm", &id, "--journal", "--yes"]).await?;
    project.finish().await
}

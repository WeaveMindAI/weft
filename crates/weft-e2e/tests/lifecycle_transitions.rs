//! Layer-4: the project-lifecycle state machine, transition by transition.
//!
//! `docs/` (repo root) defines the reconciliation table: every stable state
//! offers exactly its legal verbs, every transitional state offers ONLY its
//! cancel, and every disruptive verb drains running work per the caller's
//! running-policy. These tests drive ONE evolving project per test through
//! real verbs (CLI + HTTP, as a user would) and assert the table's answer
//! (`available_actions`), the rejections (REJ cells must 4xx), and the drain
//! semantics (a Wait drain holds the verb open, admits nothing new, and lets
//! the running execution finish).
//!
//! The lever for "a real execution is running" is the `lifecycle` fixture's
//! HoldGate node: it polls a rig-owned fake URL until the rig releases it, so
//! the rig can hold an execution open across a drain and finish it on cue.
//! Source-shape transitions (no-infra -> infra -> orphan, plain -> trigger)
//! are real `main.weft` rewrites followed by the next verb, exactly as a user
//! editing source.
//!
//! Coverage that deliberately does NOT live here:
//!   - `cancel_build`: the build gate only engages when there is a builder; the
//!     local CLI builds before the verb, so there is no `building` window to
//!     cancel against this rig.
//!   - `cancel_activate`: trigger setup on this rig completes in milliseconds
//!     (no deterministic `activating` window to race); the deactivating-side
//!     verbs (`cancel_running`, `resume_active`) ARE covered below via the
//!     drain-held window, which exercises the same master rule.
#![cfg(feature = "e2e")]

mod common;

use std::time::Duration;

use common::*;
use serde_json::{json, Value};
use weft_e2e::client::cli;
use weft_e2e::fakes::{PollFake, SseFake};
use weft_e2e::status::{self, STATUS_DEADLINE};
use weft_e2e::{ensure, human, infra, run, Project, SettledRun};

/// Source flips the infra concern on and off: a no-infra project runs in the
/// shared pool and offers no infra verbs; adding MiniService to source and
/// starting infra moves the worker into the project namespace and lights the
/// infra controls; removing it from source orphans the LIVE infra (visible,
/// non-gating, still stoppable) until the user terminates it.
#[tokio::test]
async fn source_flips_infra_and_orphan_lifecycle() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = weft_e2e::Platform::connect().await?;
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
    // And the worker lives in the shared pool.
    let pods = platform.worker_pods_for_project(&pid).await?;
    anyhow::ensure!(
        pods.iter().any(|p| p.namespace == "wft-shared-workers"),
        "no-infra worker must be in the shared pool; rows: {pods:?}"
    );

    // Source gains infra: import the node, rewrite main, start infra.
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.set_main(&graph_infra())?;
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;

    let s = status::fetch(&disp, &pid).await?;
    anyhow::ensure!(s.has_infra(), "registered definition must declare infra now");
    s.assert_actions_exactly(&["run", "infra_stop", "infra_terminate"])?;

    // A run against running infra works and the worker sits in the project
    // namespace (every alive pod: provisioning included, per the network
    // wall around infra).
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    let pods = platform.worker_pods_for_project(&pid).await?;
    for p in pods.iter().filter(|p| p.status == "alive" || p.status == "spawning") {
        anyhow::ensure!(
            p.namespace.starts_with("wft-project-"),
            "infra project's live worker must be in the project namespace; got {p:?}"
        );
    }

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

/// The infra concern's REJ cells, the drain-held stop, and infra cancel:
/// resting infra gates run; a stop with a running execution drains (master
/// rule: only `infra_cancel` offered, run rejected, the execution keeps
/// running); `weft infra cancel` halts the stop WITHOUT killing the
/// execution; the released execution then completes untouched.
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
    let attempt = cli(project.dir(), &["run", "--json"]).await?;
    anyhow::ensure!(
        !attempt.success,
        "run must fail while declared infra is not running; stdout: {}",
        attempt.stdout
    );
    project.mark_registered();
    let s = status::fetch(&disp, &pid).await?;
    s.assert_actions_exactly(&["infra_start"])?;
    assert_verb_rejected(&disp, &format!("/projects/{pid}/run"), "infra resting").await?;

    // Provision, then hold a run open.
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    status::fetch(&disp, &pid)
        .await?
        .assert_actions_exactly(&["run", "infra_stop", "infra_terminate"])?;
    let color = run::start(&mut project).await?;
    status::wait_until(&disp, &pid, "run to be live", STATUS_DEADLINE, |s| {
        s.running_count() >= 1
    })
    .await?;

    // Stop with the Wait policy: the supervisor drains. While the stop is in
    // flight the master rule collapses the table to its cancel, and nothing
    // new is admitted.
    let stop = spawn_weft(
        project.dir().to_path_buf(),
        vec![
            "infra".into(),
            "stop".into(),
            "--mode".into(),
            "park".into(),
            "--running-policy".into(),
            "wait".into(),
            "--drain-timeout".into(),
            "300".into(),
        ],
    );
    status::wait_until(&disp, &pid, "stop drain to gate the table", STATUS_DEADLINE, |s| {
        s.available_actions() == vec!["infra_cancel"]
    })
    .await?;
    assert_verb_rejected(&disp, &format!("/projects/{pid}/run"), "infra stop draining").await?;
    anyhow::ensure!(
        exec_status(&disp, color).await? == "running",
        "the Wait drain must not kill the running execution"
    );

    // Cancel the stop: HALT, not rollback. The infra stays running, the
    // execution stays running, the table returns to the stable running row.
    project.weft(&["infra", "cancel"]).await?;
    status::wait_until(&disp, &pid, "cancelled stop to settle", STATUS_DEADLINE, |s| {
        s.available_actions() == vec!["run", "infra_stop", "infra_terminate"]
            && s.infra_rollup() == "running"
    })
    .await?;
    anyhow::ensure!(
        exec_status(&disp, color).await? == "running",
        "infra cancel must not touch the running execution"
    );
    let stop_out = stop.await??;
    anyhow::ensure!(
        !stop_out.success,
        "the cancelled stop must not report success; stdout: {}",
        stop_out.stdout
    );

    // Release: the held execution completes untouched by all of the above.
    gate.set_body(RELEASE).await;
    SettledRun::observe(&disp, color)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    // Terminate with nothing running: immediate, back to the resting row.
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    status::fetch(&disp, &pid).await?.assert_actions_exactly(&["infra_start"])?;

    project.finish().await
}

/// The trigger axis: activate/deactivate, the deactivating window's two
/// verbs (resume_active rolls forward, cancel_running gives up the wait),
/// and resync re-registering an edited source. All against a REAL running
/// Fire execution held open by the gate.
#[tokio::test]
async fn deactivate_drain_resume_cancel_and_resync() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(HOLD).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go", &gate.url()))?;

    project.activate().await?;
    let s = status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    s.assert_actions_exactly(&["run", "deactivate"])?;
    assert_verb_rejected(&disp, &format!("/projects/{pid}/activate"), "already active").await?;

    // Fire: the execution holds.
    let before = run::execution_colors(&disp, &pid).await?;
    let color = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;

    // Deactivate with Wait: the drain holds the verb open. The window offers
    // exactly give-up (cancel_running) and change-your-mind (resume_active).
    let deact = spawn_weft(
        project.dir().to_path_buf(),
        vec![
            "deactivate".into(),
            "--mode".into(),
            "park".into(),
            "--running-policy".into(),
            "wait".into(),
            "--drain-timeout".into(),
            "300".into(),
        ],
    );
    status::wait_until(&disp, &pid, "deactivating window", STATUS_DEADLINE, |s| {
        s.status() == "deactivating"
    })
    .await?
    .assert_actions_exactly(&["cancel_running", "resume_active"])?;
    assert_verb_rejected(&disp, &format!("/projects/{pid}/run"), "deactivating").await?;

    // Change your mind: POST /activate rolls Deactivating -> Active. The
    // held execution is untouched.
    let _: Value = disp.post_json(&format!("/projects/{pid}/activate"), &json!({})).await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    let _ = deact.await??; // the resumed deactivate returns; its exit is the CLI's report of the resume
    anyhow::ensure!(
        exec_status(&disp, color).await? == "running",
        "resume_active must leave the running execution alone"
    );

    // Second deactivate, same window, but this time GIVE UP the wait:
    // cancel_running finishes the drain immediately by cancelling.
    let deact = spawn_weft(
        project.dir().to_path_buf(),
        vec![
            "deactivate".into(),
            "--mode".into(),
            "park".into(),
            "--running-policy".into(),
            "wait".into(),
            "--drain-timeout".into(),
            "300".into(),
        ],
    );
    status::wait_until(&disp, &pid, "second deactivating window", STATUS_DEADLINE, |s| {
        s.status() == "deactivating"
    })
    .await?;
    disp.post_empty(&format!("/projects/{pid}/cancel-running"), &json!({}))
        .await?;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    let deact_out = deact.await??;
    anyhow::ensure!(
        deact_out.success,
        "deactivate must complete once the drain is cancelled; stderr: {}",
        deact_out.stderr
    );
    let settled = SettledRun::observe(&disp, color).await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "cancel_running must cancel the held execution; got {}",
        settled.status
    );

    // Resync: reactivate, edit the source (new event name), resync with an
    // explicit deactivation spec, and prove the NEW registration fires.
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go2", &gate.url()))?;
    project
        .weft(&["resync", "--mode", "wipe", "--running-policy", "cancel"])
        .await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;

    gate.set_body(RELEASE).await;
    let before = run::execution_colors(&disp, &pid).await?;
    let color = fire_until_execution(&feed, &disp, &pid, "go2", &before).await?;
    SettledRun::observe(&disp, color)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    project.finish().await
}

/// All three concerns at once: an ACTIVE project with RUNNING infra offers
/// the full verb set; `infra stop` on it auto-deactivates first (one click,
/// the user's deactivation spec) and lands on the stopped row where nothing
/// that needs infra is offered; a fresh start + activate brings the full
/// cycle back; terminate returns to resting.
#[tokio::test]
async fn active_infra_stop_auto_deactivates_and_recovers() -> anyhow::Result<()> {
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
        .assert_actions_exactly(&["run", "deactivate", "infra_stop", "infra_terminate"])?;

    // Stop while ACTIVE: auto-deactivates with the caller's spec, then stops.
    project
        .weft(&["infra", "stop", "--mode", "park", "--running-policy", "cancel"])
        .await?;
    let s = status::wait_until(&disp, &pid, "stopped row", STATUS_DEADLINE, |s| {
        s.status() == "inactive" && s.infra_rollup() == "stopped"
    })
    .await?;
    s.assert_actions_exactly(&["infra_start", "infra_terminate"])?;
    assert_verb_rejected(&disp, &format!("/projects/{pid}/run"), "infra stopped").await?;
    assert_verb_rejected(&disp, &format!("/projects/{pid}/activate"), "infra stopped").await?;

    // Start again: the full set returns (activate included: triggers +
    // running infra).
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;
    status::fetch(&disp, &pid)
        .await?
        .assert_actions_exactly(&["run", "activate", "infra_stop", "infra_terminate"])?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;

    // The revived deployment actually fires end to end.
    let before = run::execution_colors(&disp, &pid).await?;
    let color = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;
    SettledRun::observe(&disp, color)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    // Wind down: deactivate, terminate, resting row.
    project
        .weft(&["deactivate", "--mode", "park", "--running-policy", "cancel"])
        .await?;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    status::fetch(&disp, &pid).await?.assert_actions_exactly(&["infra_start"])?;

    project.finish().await
}

/// A SUSPENDED execution (HumanQuery) survives deactivate + reactivate: the
/// inactive row offers `reactivate` (preserved state exists, not a bare
/// activate), and the form is still answerable after the round trip, resuming
/// the original execution to completion.
#[tokio::test]
async fn suspension_survives_deactivate_reactivate() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Fire and run to the HumanQuery suspension.
    let before = run::execution_colors(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let color =
        run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;

    // Deactivate: no RUNNING work (the execution is suspended, holding no
    // worker), so the Wait drain lands immediately; the suspension is
    // PRESERVED, which is exactly why the inactive row offers `reactivate`.
    project
        .weft(&["deactivate", "--mode", "park", "--running-policy", "wait"])
        .await?;
    let s = status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    anyhow::ensure!(
        s.offers("reactivate"),
        "preserved suspension must offer reactivate, got {:?}",
        s.available_actions()
    );
    let status = exec_status(&disp, color).await?;
    anyhow::ensure!(
        status == "waiting_for_input",
        "deactivate must not touch a suspended execution \
         (and the list must report it honestly); got '{status}'"
    );

    // Reactivate with the explicit keep-the-suspension choice (preserved
    // state makes `weft activate` demand a choice; the rig is
    // non-interactive by construction), answer the form, and the ORIGINAL
    // execution resumes to completion.
    project
        .weft(&["activate", "--reactivate-choice", "execute_parked_keep_suspended"])
        .await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    SettledRun::observe(&disp, color)
        .await?
        .completed()?
        .assert_input("out", "data", &json!(true))?;

    project.finish().await
}

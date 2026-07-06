//! Layer-4: lifecycle transitions against a REAL multi-worker fleet.
//!
//! A project scales horizontally (memory saturation spawns extra workers),
//! so every drain / replacement decision must hold for N pods, not one.
//! Kind cannot produce real memory pressure (its pods run without memory
//! limits, so a worker's pressure always reads 0 and any forced DB value
//! is clobbered by the next heartbeat), so the fleet is built with the
//! rig's OWN levers instead:
//!
//!   - `Platform::add_second_worker` clones a real running worker pod
//!     (same binary, own name via the downward API) and registers it in
//!     the pool, exactly like `add_second_listener` does for listeners;
//!   - `Platform::set_worker_draining` flips the system's real
//!     no-new-admissions flag to STEER a fire onto a chosen pod, making
//!     "execution X runs on pod Y" deterministic.
//!
//! What stays out of scope here (and why it is safe): the DECISION to
//! scale up (saturation math, admittable picks) is covered at layer 3
//! against real SQL; these tests cover what happens to transitions ONCE a
//! fleet exists, which is the part layer 3 cannot see.
#![cfg(feature = "e2e")]

mod common;

use common::*;
use serde_json::json;
use weft_e2e::fakes::{PollFake, SseFake};
use weft_e2e::status::{self, STATUS_DEADLINE};
use weft_e2e::{ensure, run, Project, SettledRun};

/// Wait until the execution's color has an OWNER pod and return it.
async fn wait_for_owner(
    platform: &weft_e2e::Platform,
    color: uuid::Uuid,
) -> anyhow::Result<String> {
    weft_e2e::client::poll_until(
        "execution to be claimed by a worker",
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(300),
        || async { platform.execution_owner(&color).await },
    )
    .await
}

/// Two workers share one project's queue: a fire steered at each pod runs
/// there, and a deactivate-with-wait drains BOTH before flipping Inactive
/// (the drain counts the project's executions, not one pod's).
#[tokio::test]
async fn two_workers_share_the_queue_and_drain_together() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = weft_e2e::Platform::connect().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(HOLD).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go", &gate.url()))?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;

    // Fire #1: spawns worker A and holds on it.
    let before = run::execution_colors(&disp, &pid).await?;
    let c1 = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;
    let pod_a = wait_for_owner(&platform, c1).await?;

    // Build the fleet: clone A into B, then STEER fire #2 onto B by
    // draining A for the duration of the fire (the claim filter refuses
    // unpinned work on a draining pod).
    let pod_b = platform.add_second_worker(&pid).await?;
    platform.set_worker_draining(&pod_a, true).await?;
    let before = run::execution_colors(&disp, &pid).await?;
    let c2 = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;
    let owner2 = wait_for_owner(&platform, c2).await?;
    platform.set_worker_draining(&pod_a, false).await?;
    anyhow::ensure!(
        owner2 == pod_b,
        "with pod A draining the fire must land on the clone; got {owner2} (A={pod_a}, B={pod_b})"
    );

    // Both pods hold one running execution each.
    status::wait_until(&disp, &pid, "both executions live", STATUS_DEADLINE, |s| {
        s.running_count() >= 2
    })
    .await?;

    // Deactivate with Wait: the drain must hold while EITHER pod still
    // runs work, and complete once both do.
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
    status::wait_until(&disp, &pid, "deactivating with two live runs", STATUS_DEADLINE, |s| {
        s.status() == "deactivating" && s.running_count() >= 2
    })
    .await?;
    gate.set_body(RELEASE).await;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    let out = deact.await??;
    anyhow::ensure!(out.success, "deactivate must succeed once both pods drained: {}", out.stderr);
    SettledRun::observe(&disp, c1).await?.completed()?;
    SettledRun::observe(&disp, c2).await?.completed()?;

    project.finish().await
}

/// A STALE fleet is replaced as a whole: with TWO workers each holding a
/// running execution, an infra start on an edited source dooms BOTH
/// (image + namespace change), drains them through the shared loop
/// (nothing killed while held), and lands every live worker on the new
/// image in the project namespace once released. The executions complete
/// untouched.
#[tokio::test]
async fn stale_fleet_is_drained_and_replaced_as_a_whole() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = weft_e2e::Platform::connect().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let pid = project.id();

    let gate = PollFake::start(HOLD).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go", &gate.url()))?;
    project.activate().await?;
    status::wait_until_status(&disp, &pid, "active", STATUS_DEADLINE).await?;

    // A held execution on each of two workers (steered like above).
    let before = run::execution_colors(&disp, &pid).await?;
    let c1 = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;
    let pod_a = wait_for_owner(&platform, c1).await?;
    let pod_b = platform.add_second_worker(&pid).await?;
    platform.set_worker_draining(&pod_a, true).await?;
    let before = run::execution_colors(&disp, &pid).await?;
    let c2 = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;
    let owner2 = wait_for_owner(&platform, c2).await?;
    platform.set_worker_draining(&pod_a, false).await?;
    anyhow::ensure!(owner2 == pod_b, "steering failed: {owner2} != {pod_b}");

    // Source gains infra (a binary change AND a namespace change at once):
    // the pre-apply reconcile must doom BOTH old pods, drain them, and
    // only then proceed.
    project.set_main(&graph_trigger_infra_hold(&feed.url(), "go", &gate.url()))?;
    let mut start = spawn_weft(
        project.dir().to_path_buf(),
        vec!["infra".into(), "start".into(), "--drain-timeout".into(), "300".into()],
    );

    // Both old pods flip to DRAINING (the multi-pod doomed set) while
    // their held executions keep running. The gate holds `infra start`
    // open, so the CLI finishing DURING this window means it died early
    // (build error, rejected verb): surface its output immediately
    // instead of a blind observation timeout.
    let observe = weft_e2e::client::poll_until(
        "both stale workers to be draining",
        std::time::Duration::from_secs(180),
        std::time::Duration::from_millis(500),
        || async {
            let rows = platform.worker_pods_for_project(&pid).await?;
            let both = [&pod_a, &pod_b].iter().all(|p| {
                rows.iter()
                    .any(|r| r.pod_name == **p && r.draining && r.status == "alive")
            });
            Ok(both.then_some(()))
        },
    );
    tokio::select! {
        joined = &mut start => {
            let out = joined??;
            anyhow::bail!(
                "`weft infra start` exited before the drain was even observed \
                 (success={}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
                out.success, out.stdout, out.stderr
            );
        }
        observed = observe => { observed?; }
    }
    anyhow::ensure!(
        exec_status(&disp, c1).await? == "running" && exec_status(&disp, c2).await? == "running",
        "the Wait drain must not kill held executions on either pod"
    );

    // Release: both drain out, the sync proceeds to provision, and the
    // fleet is respawned on the new image in the project namespace.
    gate.set_body(RELEASE).await;
    SettledRun::observe(&disp, c1).await?.completed()?;
    SettledRun::observe(&disp, c2).await?.completed()?;
    let out = start.await??;
    anyhow::ensure!(out.success, "infra start must succeed after the drain: {}", out.stderr);

    let rows = platform.worker_pods_for_project(&pid).await?;
    for old in [&pod_a, &pod_b] {
        anyhow::ensure!(
            !rows.iter().any(|r| r.pod_name == **old
                && (r.status == "alive" || r.status == "spawning")),
            "stale pod {old} must be gone after the fleet replacement"
        );
    }
    // Guard against a vacuous pass: this loop asserts the RESPAWN landed in the
    // project namespace, so if no live worker exists (respawn hasn't happened)
    // the per-row check would pass by iterating nothing. Require at least one.
    anyhow::ensure!(
        rows.iter().any(|r| r.status == "alive" || r.status == "spawning"),
        "expected at least one live worker after the fleet replacement"
    );
    for live in rows.iter().filter(|r| r.status == "alive" || r.status == "spawning") {
        anyhow::ensure!(
            live.namespace.starts_with("wft-project-"),
            "post-replacement worker {} must live in the project namespace, got {}",
            live.pod_name,
            live.namespace
        );
    }
    // And the revived deployment works end to end on the new image.
    let before = run::execution_colors(&disp, &pid).await?;
    let c3 = fire_until_execution(&feed, &disp, &pid, "go", &before).await?;
    SettledRun::observe(&disp, c3)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    // Wind down: the project stayed ACTIVE through the whole
    // replacement (a start never deactivates), so deactivate
    // explicitly before the flagless terminate.
    project
        .weft(&["deactivate", "--mode", "park", "--running-policy", "cancel"])
        .await?;
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    weft_e2e::infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    project.finish().await
}

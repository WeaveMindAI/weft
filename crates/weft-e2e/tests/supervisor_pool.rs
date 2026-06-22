//! Layer-4: the pooled infra-supervisor's lifecycle against a REAL
//! provisioned project, as one chained flow (provisioning a backing
//! service is expensive, so this builds the fixture once and asserts the
//! whole story in sequence; every step depends on the prior state):
//!
//!   1. COLD START. Provisioning a project's infra spawns exactly one
//!      supervisor pod and gives it exactly one exclusive `infra_owner`
//!      lease over the project; the service comes up `running`.
//!   2. A SECOND POD DOES NOT STEAL OWNERSHIP. Cloning a second real
//!      supervisor pod leaves the project owned by exactly one pod (the
//!      original): a fresh pod claims only UNOWNED projects.
//!   3. CONSOLIDATION = SAFE MIGRATION. We use TWO real projects and move
//!      the second's ownership onto the cloned pod B, so BOTH pods own
//!      work (A owns P1, B owns P2). This is what forces the real
//!      `drain_one` migration path: with neither pod empty, the 30s idle
//!      reaper cannot short-circuit by reaping an empty pod, so the only
//!      way the pool returns to one is the 60s scale-down sweep draining
//!      one pod and the SURVIVOR adopting the drained pod's project (the
//!      adopt / hand-off path). (A single-project setup is vacuous here:
//!      moving the sole project to B empties A, and the idle reaper just
//!      reaps A without any migration ever running.) With both idle
//!      (uncapped cgroup → 0 pressure) the planner has headroom to fold.
//!      Throughout we poll each project's owner and assert it is ALWAYS
//!      one of {none-briefly, A, B} AND transitions AT MOST ONCE: a real
//!      double-actor bug shows up as the owner VALUE oscillating (the PK
//!      forbids two rows, so a count is not the tell), and a dropped
//!      ownership shows up as it never converging. After consolidation the
//!      lone survivor owns BOTH projects, the drained pod is reaped, and
//!      the services never blipped down.
//!   4. CLEANUP. Terminate both projects' infra.
//!
//! All levers are the rig's operator-style ones (kubectl + Postgres +
//! the dispatcher's public API), never a test-only endpoint. The drain
//! itself is driven by the dispatcher's real scale-down sweep, not poked
//! from the test: we create the precondition (a second pod) and let the
//! production reaper consolidate.

#![cfg(feature = "e2e")]

use std::time::{Duration, Instant};

use weft_e2e::{ensure, infra, platform::Platform, project::Project};

const NODE: &str = "svc";

#[tokio::test]
async fn supervisor_cold_start_then_safe_consolidation() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;
    let mut p1 = Project::prepare("infra_min", disp.clone()).await?;
    let mut p2 = Project::prepare("infra_min", disp.clone()).await?;
    let pid1 = p1.id();
    let pid2 = p2.id();

    // ---- 1. Cold start: provision both projects' infra ----
    infra::start_and_wait_running(&mut p1, NODE).await?;
    infra::start_and_wait_running(&mut p2, NODE).await?;

    // Each project is owned by exactly one supervisor. The dispatcher
    // spawns the first supervisor lazily on infra-sync; the broker's
    // ownership claim hands it both projects.
    let owner_a = wait_for_single_owner(&platform, &pid1).await?;
    wait_for_single_owner(&platform, &pid2).await?;
    let live = platform.live_supervisor_pods().await?;
    anyhow::ensure!(
        live.iter().any(|p| p.pod_name == owner_a),
        "the owning supervisor {owner_a} is not in the live registry: {live:?}"
    );
    assert_service_running(&disp, &pid1).await?;
    assert_service_running(&disp, &pid2).await?;

    // ---- 2. A second pod does not steal ownership ----
    let pod_b = platform.add_second_supervisor().await?;
    anyhow::ensure!(pod_b != owner_a, "clone produced the origin's name");
    // Two pods are now live, but both projects are still owned by exactly
    // one (the original): a fresh supervisor claims only UNOWNED projects.
    anyhow::ensure!(
        platform.live_supervisor_pods().await?.len() >= 2,
        "expected at least two live supervisor pods after cloning"
    );
    for pid in [&pid1, &pid2] {
        anyhow::ensure!(
            platform.infra_owner_of(pid).await?.as_deref() == Some(owner_a.as_str()),
            "ownership must stay with the original pod when a fresh pod joins"
        );
    }

    // ---- 3. Consolidation drains one pod, the survivor ADOPTS its
    //         project; owner of each project moves at most once ----
    // Move P2 onto B so BOTH pods own work (A owns P1, B owns P2). With
    // neither pod empty, the idle reaper cannot short-circuit by reaping
    // an empty pod; the only path back to one pod is the scale-down
    // sweep draining one and the survivor adopting its project. This
    // write sets exactly the columns the broker's claim CTE writes
    // (supervisor_pod + a renewed lease): a state production produces.
    platform.place_infra_owner_on(&pid2, &pod_b).await?;
    anyhow::ensure!(
        platform.infra_owner_of(&pid2).await?.as_deref() == Some(pod_b.as_str()),
        "ownership move onto the clone did not take"
    );
    // The clone was created with a spawn-grace window (so the idle reaper
    // did not delete it before it owned a project). Now that it owns one,
    // expire its grace so the scale-down planner treats it as an
    // established member and can consolidate it (the planner only considers
    // past-grace pods). Without this the pool never folds back to one.
    platform.expire_supervisor_grace(&pod_b).await?;

    // Both pods idle (uncapped cgroup → 0 pressure), so the planner has
    // headroom to fold one onto the other. Whichever pod is drained, its
    // project migrates to the survivor. We poll until the pool is back to
    // one live pod; throughout, each project's owner must stay in {A, B}
    // (or briefly none) AND change AT MOST ONCE (a flapping owner is the
    // double-actor tell, since the PK hides a two-row count). Services
    // must stay running across the hand-off.
    let allowed = [owner_a.clone(), pod_b.clone()];
    let survivor = wait_for_consolidation_owner_stable(
        &platform,
        &disp,
        &[pid1, pid2],
        &allowed,
        &[owner_a.clone(), pod_b.clone()], // initial owner of [P1, P2]
    )
    .await?;

    // The survivor owns BOTH projects now (it adopted the drained pod's),
    // and the drained pod is gone from the registry.
    for pid in [&pid1, &pid2] {
        anyhow::ensure!(
            platform.infra_owner_of(pid).await?.as_deref() == Some(survivor.as_str()),
            "after consolidation the survivor must own every project"
        );
    }
    let drained = if survivor == owner_a { &pod_b } else { &owner_a };
    anyhow::ensure!(
        !platform
            .live_supervisor_pods()
            .await?
            .iter()
            .any(|p| &p.pod_name == drained),
        "the drained supervisor {drained} must be reaped from the registry"
    );

    // Settle recheck: ownership must not bounce after consolidation (a
    // late re-claim by the reaped pod, or a flap, would show here).
    tokio::time::sleep(Duration::from_secs(5)).await;
    for pid in [&pid1, &pid2] {
        anyhow::ensure!(
            platform.infra_owner_of(pid).await?.as_deref() == Some(survivor.as_str()),
            "owner changed after consolidation settled; expected stable {survivor}"
        );
    }
    // The services rode the migration without going down.
    assert_service_running(&disp, &pid1).await?;
    assert_service_running(&disp, &pid2).await?;

    // ---- 4. Cleanup ----
    infra::terminate_and_wait_gone(&p1, NODE).await?;
    infra::terminate_and_wait_gone(&p2, NODE).await?;
    // Success cleanup: remove the supervisor clone THIS test created (by
    // exact name, so it never touches another test's clone). Success path
    // only; a failing test keeps state for inspection.
    platform.sweep_clone(&pod_b).await?;
    p1.finish().await?;
    p2.finish().await
}

/// Poll until the project has exactly one live `infra_owner`, returning
/// the owning pod. Bounded: cold-start ownership is an internal
/// transition the rig drives (provision the infra), so a project that
/// never gets owned is a bug, not legitimate slow work.
async fn wait_for_single_owner(platform: &Platform, pid: &uuid::Uuid) -> anyhow::Result<String> {
    weft_e2e::client::poll_until(
        "the project to be owned by exactly one supervisor",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || async {
            let count = platform.infra_owner_count(pid).await?;
            anyhow::ensure!(count <= 1, "project owned by {count} supervisors at once (>1)");
            Ok(platform.infra_owner_of(pid).await?)
        },
    )
    .await
}

/// Wait for the pool to consolidate back to ONE live supervisor pod,
/// asserting the single-actor invariant the whole way. Returns the
/// surviving pod.
///
/// The `infra_owner` PK forbids two rows for one project, so a COUNT can
/// never exceed one; a genuine double-actor bug (two pods both
/// reconciling one project) instead shows up as the owner VALUE either
/// (a) being a pod outside the legitimate set, or (b) OSCILLATING (two
/// claim loops fighting: A -> B -> A...). So for EACH project we, on
/// every poll: pin the owner to `{allowed} ∪ {none}`, and track the
/// sequence of distinct owners seen, failing if it ever exceeds one
/// transition from the initial owner (i.e. more than `[initial, other]`).
/// A dropped ownership fails by never converging. `initial_owners[i]` is
/// the owner of `pids[i]` at entry.
///
/// The deadline covers the scale-down sweep interval (60s) plus the
/// survivor's adopt-and-reap with margin.
async fn wait_for_consolidation_owner_stable(
    platform: &Platform,
    disp: &weft_e2e::client::Dispatcher,
    pids: &[uuid::Uuid],
    allowed_owners: &[String],
    initial_owners: &[String],
) -> anyhow::Result<String> {
    let deadline = Instant::now() + Duration::from_secs(150);
    // Per project, the ordered list of DISTINCT owner values observed
    // (ignoring transient `None` gaps). A clean hand-off is at most
    // [initial, survivor]; a third distinct value means a flap.
    let mut seen: Vec<Vec<String>> = initial_owners
        .iter()
        .map(|o| vec![o.clone()])
        .collect();
    loop {
        for (i, pid) in pids.iter().enumerate() {
            if let Some(owner) = platform.infra_owner_of(pid).await? {
                anyhow::ensure!(
                    allowed_owners.contains(&owner),
                    "project owned by an unexpected supervisor '{owner}' during consolidation; \
                     legitimate owners are {allowed_owners:?} (a third owner means ownership was \
                     stolen / split)"
                );
                if seen[i].last() != Some(&owner) {
                    seen[i].push(owner.clone());
                }
                anyhow::ensure!(
                    seen[i].len() <= 2,
                    "project {pid} ownership flapped across {:?} during consolidation; \
                     a clean hand-off transitions at most once (two supervisors fighting over \
                     one project is the double-actor tell the PK hides)",
                    seen[i]
                );
            }
            assert_service_running(disp, pid).await?;
        }

        let live = platform.live_supervisor_pods().await?;
        if live.len() == 1 {
            // Consolidated. Confirm the lone pod owns EVERY project.
            let survivor = live[0].pod_name.clone();
            let mut all_owned = true;
            for pid in pids {
                if platform.infra_owner_of(pid).await?.as_deref() != Some(survivor.as_str()) {
                    all_owned = false;
                    break;
                }
            }
            if all_owned {
                return Ok(survivor);
            }
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "pool did not consolidate to one supervisor within the deadline (still {} live)",
            live.len()
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Assert the project's infra node reports `running` via the
/// dispatcher's public `/infra/status`. Used as a liveness probe across
/// the migration: the backing service must not blip down during a
/// supervisor hand-off.
async fn assert_service_running(
    disp: &weft_e2e::client::Dispatcher,
    pid: &uuid::Uuid,
) -> anyhow::Result<()> {
    let nodes = infra::status(disp, pid).await?;
    let node = nodes
        .iter()
        .find(|n| n.node_id() == Some(NODE))
        .ok_or_else(|| anyhow::anyhow!("infra node '{NODE}' missing from status"))?;
    anyhow::ensure!(
        node.status() == Some("running"),
        "service must stay running across the migration; status={:?}",
        node.status()
    );
    Ok(())
}

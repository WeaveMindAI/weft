//! Layer-4: the pooled listener consolidates a two-pod pool back to one
//! when load drops, WITHOUT losing the signal it holds. This is the
//! listener twin of the supervisor consolidation test, and the
//! complement of `listener_move` (which proves a fire during the move
//! overlap is not double-counted); here we prove the move does not DROP
//! the held signal: after consolidation a pushed event still fires.
//!
//! Setup, with the rig's operator-style levers only (kubectl + Postgres
//! + the listeners' real HTTP):
//!   1. Activate a reach-out (SSE) project: the dispatcher places the
//!      feed signal on listener pod A, which dials the feed and holds
//!      the connection.
//!   2. Clone a second real listener pod B (so the pool has two pods).
//!   3. Let the dispatcher's scale-down sweep run: with both pods idle
//!      (local cgroup uncapped → 0 memory pressure), the planner folds
//!      one pod onto the other, re-placing its signals, then reaps it.
//!   4. The pool is back to one pod. Push an event and assert it STILL
//!      fires exactly one execution: the held signal survived the move
//!      (re-placed + the new holder re-subscribed through its own code).
//!
//! The drain is driven by the dispatcher's real 60s scale-down sweep,
//! not poked from the test; we only create the precondition (a second
//! pod) and let the production reaper consolidate.

#![cfg(feature = "e2e")]

use std::time::{Duration, Instant};

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, platform::Platform, project::Project, run, SettledRun};

#[tokio::test]
async fn listener_pool_consolidates_without_dropping_the_signal() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;
    let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
    let pid = project.id();

    // Activate: pod A dials the feed and holds the SSE connection.
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Baseline: one event fires exactly one execution while the pool is a
    // single pod (sanity that the trigger works before we perturb it).
    let b0 = run::execution_colors(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(30)).await?; // subscription is live, not a fixed sleep
    feed.push_event("tick", &json!({ "value": 1 }).to_string());
    let c0 = run::wait_for_triggered_execution(&disp, &pid, &b0, Duration::from_secs(60)).await?;
    SettledRun::observe(&disp, c0).await?.completed()?;

    // Build a realistic "two partially-loaded pods" pool that scale-down
    // consolidates: clone a second listener (pod B), then MOVE one of the
    // project's signals from pod A onto B (so each pod holds work and
    // neither is idle). Moving work onto B before it can be idle-reaped is
    // also what keeps the fresh clone alive: an empty pool member is
    // reaped within ~10s, so we place a signal on it right after creating
    // it (two adjacent DB writes), the same window the dispatcher's own
    // saturation-spawn uses (it places work on a new pod atomically).
    let pod_a = first_listener_pod(&platform).await?;
    let pod_b = platform.add_second_listener().await?;
    anyhow::ensure!(pod_b != pod_a, "clone produced the origin's name");

    // Pick one of A's signals and move it to B under a bumped generation,
    // then have B register it through its own code (rehydrate). Now A and
    // B each hold at least one signal: a genuine consolidation candidate.
    let on_a = platform.signal_tokens_on_pod(&pod_a).await?;
    let (token, gen) = on_a
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("pod A holds no signals to move onto the clone"))?;
    let b_namespace = platform
        .live_listener_pods()
        .await?
        .into_iter()
        .find(|p| p.pod_name == pod_b)
        .map(|p| p.namespace)
        .ok_or_else(|| anyhow::anyhow!("cloned listener {pod_b} not in the registry"))?;
    platform.set_signal_placement(&token, &pod_b, gen + 1).await?;
    platform.rehydrate_listener(&pod_b, &b_namespace).await?;

    // The clone was created with a spawn-grace window (so the idle reaper
    // did not delete it mid-setup). Now that it is wired up (holds a
    // signal), expire its grace so the scale-down planner treats it as an
    // established pool member and can consolidate it. Without this the
    // planner skips it (it only considers past-grace pods) and the pool
    // never folds back to one. (pod_a is the long-running original, already
    // well past its own spawn grace, so only the clone needs expiring.)
    platform.expire_listener_grace(&pod_b).await?;

    // Let the dispatcher's scale-down sweep consolidate the two
    // partially-loaded pods back to one. Both report 0 memory pressure
    // (uncapped cgroup locally), so the planner has headroom to fold one
    // onto the other; the drained pod's signals are re-placed onto the
    // survivor, then it is reaped.
    //
    // `wait_for_pool_size_one` returns only once the drained pod's
    // registry row is DELETED, and reap deletes that row AFTER
    // `backend.stop` (a foreground-cascade k8s delete) completes, so by
    // the time we proceed the drained pod is fully terminated and its SSE
    // TCP closed. That ordering is what removes the dying-straggler race:
    // any SSE subscriber the fake still sees afterward is the SURVIVOR's,
    // not a lingering receiver from the reaped pod. (We therefore do NOT
    // need to count "new" connections; whichever pod is drained, the lone
    // survivor holds the feed and is the only possible live subscriber.)
    wait_for_pool_size_one(&platform).await?;
    eprintln!("listener pool consolidated; cloned pod was {pod_b}");

    // The held signal survived the move: a pushed event still fires
    // exactly one execution. `wait_for_triggered_execution` bails on more
    // than one new execution and times out on zero, so a drop times out
    // (fail) and a double fires loudly; exactly-one is the pass.
    let b1 = run::execution_colors(&disp, &pid).await?;
    // Wait for the survivor's SSE subscription to be live before pushing.
    // Robust to which pod was drained: if the feed migrated to the
    // survivor it re-subscribes; if the survivor already held it, it is
    // already subscribed. Either way exactly the survivor's connection is
    // live (the straggler is gone, see above), so >= 1 subscriber is the
    // survivor.
    feed.wait_for_subscriber(Duration::from_secs(30)).await?;
    feed.push_event("tick", &json!({ "value": 2 }).to_string());
    let c1 = run::wait_for_triggered_execution(&disp, &pid, &b1, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, c1).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(2))?;

    // Post-wait recheck: a drop would have timed out above, but a LATE
    // second execution (a stale pre-consolidation holder also firing)
    // would land just after wait_for_triggered_execution returns. Settle,
    // then assert still exactly one new execution since b1.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = run::execution_colors(&disp, &pid).await?;
    anyhow::ensure!(
        after.difference(&b1).count() == 1,
        "expected exactly one execution from the surviving signal, saw {} \
         (a stale pre-consolidation holder double-fired)",
        after.difference(&b1).count()
    );

    // Success cleanup: consolidation already reaped the clone (sweep_clone
    // is then a no-op: no registry row, kubectl delete --ignore-not-found),
    // but call it by exact name to clear any residue. Success path only; a
    // failing test keeps state for inspection.
    platform.sweep_clone(&pod_b).await?;
    project.finish().await
}

/// The single live listener pod the dispatcher spawned for the project
/// (the holder of its signals before we clone a second). Errors if the
/// pool is not exactly one pod yet (activation should have spawned it).
async fn first_listener_pod(platform: &Platform) -> anyhow::Result<String> {
    let live = platform.live_listener_pods().await?;
    anyhow::ensure!(
        live.len() == 1,
        "expected exactly one listener pod before cloning, found {}",
        live.len()
    );
    Ok(live[0].pod_name.clone())
}

/// Wait for the listener pool to consolidate back to ONE live pod. The
/// deadline covers the scale-down sweep interval (60s) plus the
/// re-place-and-reap with margin; this is a system transition the rig set
/// up (two partially-loaded pods), so a bound is correct.
async fn wait_for_pool_size_one(platform: &Platform) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(150);
    loop {
        let live = platform.live_listener_pods().await?;
        if live.len() == 1 {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "listener pool did not consolidate to one pod within the deadline (still {} live)",
            live.len()
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

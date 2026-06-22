//! Layer-4: a held-connection signal (SSE) that is briefly live on TWO
//! listener pods at once (the scale-down MOVE overlap) still fires exactly
//! once, no drop, no double. This is the end-to-end proof of the two move
//! fixes whose decision logic is unit-tested elsewhere:
//!
//!   - the placement-generation FENCE (the broker drops a stale old-pod
//!     fire during the overlap), and
//!   - the durable signal survives a holder change (the new pod registers
//!     it through its own production code; the SSE loop reconnects, the
//!     same shape as a heartbeat socket).
//!
//! How the overlap is built, using ONLY the rig's operator-style levers
//! (kubectl + Postgres + the listeners' real HTTP), never a test-only
//! dispatcher endpoint:
//!   1. activate -> the dispatcher registers the SSE signal on pod A.
//!   2. clone a second real listener pod B (kubectl).
//!   3. point the signal's placement at B under generation+1 (the column
//!      write the dispatcher's `set_placement` performs).
//!   4. POST B's real `/rehydrate`: B registers the signal from the
//!      durable row, under the new generation, through its OWN code.
//!   5. A still holds the signal in-RAM under the OLD generation. Both
//!      pods are now live-subscribed to the SSE feed: the real overlap.
//! A pushed event can reach both pods; the broker fences A's old-generation
//! fire, so exactly one execution results. `wait_for_triggered_execution`
//! BAILS on more than one new execution, so a double-fire fails loudly and
//! a dropped fire times out; exactly-one is the pass.

#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, platform::Platform, project::Project, run, SettledRun};

#[tokio::test]
async fn sse_signal_in_a_two_pod_overlap_fires_exactly_once() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;
    let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
    let pid = project.id();

    // Stand up the fake SSE feed and point the trigger at it, then activate
    // (pod A dials the feed and holds the SSE connection open).
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // The signal we will put into a two-pod overlap, and where it is placed
    // (pod A) before we touch anything. `feed` is an ENTRY trigger (SSE,
    // `is_resume=FALSE`), which the consumer-facing signal enumeration
    // deliberately hides (that API lists only human-submittable forms /
    // resumes), so we read its token straight from the registry at the
    // operator layer, the same level the rest of this overlap scenario works
    // at. A short poll covers the tiny window between `activate()` returning
    // and the TriggerSetup registration landing the row.
    let token = weft_e2e::client::poll_until(
        "the 'feed' entry signal to be registered",
        Duration::from_secs(30),
        Duration::from_millis(500),
        || async { platform.signal_token_for_node(&pid, "feed").await },
    )
    .await?;
    let before = platform
        .signal_placement(&token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("feed signal has no durable row"))?;
    let pod_a = before
        .listener_pod
        .clone()
        .ok_or_else(|| anyhow::anyhow!("feed signal is not placed on any pod"))?;

    // Sanity baseline: one event fires exactly one execution pre-overlap.
    let b0 = run::execution_colors(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(30)).await?; // subscription live, not a fixed sleep
    feed.push_event("tick", &json!({ "value": 1 }).to_string());
    let c0 = run::wait_for_triggered_execution(&disp, &pid, &b0, Duration::from_secs(60)).await?;
    SettledRun::observe(&disp, c0).await?.completed()?;

    // ---- Build the real two-pod overlap ----
    // Pod B (clone), then flip the placement to B under gen+1, then have B
    // rehydrate so it registers the signal through its own code. Pod A
    // still holds it under the old generation. Note the ORDER differs from
    // production deliberately: production (`replace_onto_new_pod`,
    // reserve+register+set_placement under the per-token lock) registers
    // on the new pod FIRST, then flips the
    // column; the rig flips the column first because `rehydrate` is
    // column-driven (B registers `WHERE listener_pod = B`, so the column
    // must already point at B). The END STATE is identical (B registered
    // under gen+1, A still under the old gen, column at B), which is all
    // the generation-fence properties under test depend on. The rig
    // replicates the move's column VALUES + generation bump, not the exact
    // step ordering.
    let pod_b = platform.add_second_listener().await?;
    let b_namespace = platform
        .live_listener_pods()
        .await?
        .into_iter()
        .find(|p| p.pod_name == pod_b)
        .map(|p| p.namespace)
        .ok_or_else(|| anyhow::anyhow!("cloned listener {pod_b} not in the registry"))?;
    let new_gen = before.generation + 1;
    platform.set_signal_placement(&token, &pod_b, new_gen).await?;
    platform.rehydrate_listener(&pod_b, &b_namespace).await?;

    // Confirm the overlap precondition: the routing column points at B
    // under the bumped generation (the fence keys on this), while A still
    // holds the signal in-RAM (it was never unregistered).
    let after = platform
        .signal_placement(&token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("feed signal row vanished"))?;
    anyhow::ensure!(
        after.listener_pod.as_deref() == Some(pod_b.as_str()),
        "placement did not move to pod B"
    );
    anyhow::ensure!(
        after.generation > before.generation,
        "placement generation did not bump ({} -> {}); the stale-fire fence would not engage",
        before.generation,
        after.generation
    );
    anyhow::ensure!(pod_a != pod_b, "clone produced the same pod name as the origin");

    // ---- Fire into the overlap: exactly one execution ----
    // Both A (old gen) and B (new gen) are subscribed; a pushed event can
    // reach both. The broker fences A's old-generation fire, so exactly one
    // execution results. More than one new execution fails the wait loudly
    // (double-fire); zero times out (dropped). Exactly-one is the pass.
    let b1 = run::execution_colors(&disp, &pid).await?;
    // Wait until BOTH pods are actively READING the feed (not merely
    // subscribed): only a reading connection is guaranteed to catch the
    // next single emission, so this makes the one push below reach BOTH A
    // and B exactly once each, which is what exercises the fence (A's
    // old-gen fire and B's new-gen fire both arrive; the broker fences A's).
    feed.wait_for_subscribers(2, Duration::from_secs(30)).await?;
    feed.push_event("tick", &json!({ "value": 2 }).to_string());
    let c1 = run::wait_for_triggered_execution(&disp, &pid, &b1, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, c1).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(2))?;

    // Give a beat for any (wrongly) un-fenced second fire to have shown up,
    // then re-check that still exactly one new execution exists. This
    // catches a double-fire that raced in just after the wait returned.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let now = run::execution_colors(&disp, &pid).await?;
    let new_count = now.difference(&b1).count();
    anyhow::ensure!(
        new_count == 1,
        "expected exactly one execution from the overlap fire, found {new_count} \
         (a stale old-pod fire was not fenced)"
    );

    // Success cleanup: remove the listener clone THIS test created (by
    // exact name, so it never touches another test's clone). Only reached
    // on the success path (a failing test returns earlier and KEEPS the
    // clone for inspection, like `Project`'s Drop keeps the project).
    platform.sweep_clone(&pod_b).await?;
    project.finish().await
}

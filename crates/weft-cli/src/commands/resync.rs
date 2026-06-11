//! `weft resync`. Atomic deactivate-then-activate against a fresh
//! worker image. Used after editing the trigger or fire subgraph
//! (drops live signals + cancels in-flight + rebuilds + re-registers
//! everything in one shot). Refuses on the dispatcher side if the
//! project has infra nodes that aren't running.

use super::Ctx;
use crate::commands::ensure::{parse_running_policy_flag, RunningPolicy};
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, running_policy: Option<String>) -> anyhow::Result<()> {
    let policy = parse_running_policy_flag(running_policy.as_deref())?;
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Resync, |progress| async move {
        run_inner(&ctx_inner, &progress, policy).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    running_policy: Option<RunningPolicy>,
) -> anyhow::Result<()> {
    // reactivates_after_gate=true: resync's /resync endpoint is an
    // atomic deactivate-then-activate, so it re-enables triggers and
    // replays any parked fires after the gate; no user warning needed.
    let handle = super::ensure::ensure_registered(ctx, progress, running_policy, true).await?;
    let path = format!("/projects/{}/resync", handle.id);
    let mut body_map = serde_json::Map::new();
    handle.inject_hash_fields(&mut body_map);
    let body = serde_json::Value::Object(body_map);
    progress.trigger_register_start();
    progress.dispatcher_call_start(&path);
    let _: serde_json::Value = handle.client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": handle.id }));
    progress.trigger_register_done();
    if !ctx.json() {
        println!("resynced {} ({})", handle.name, handle.id);
    }
    progress.complete(&format!("resynced {}", handle.name));
    Ok(())
}

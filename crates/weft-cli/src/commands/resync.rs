//! `weft resync`. Atomic deactivate-then-activate against a fresh
//! worker image. Used after editing the trigger or fire subgraph
//! (drops live signals + cancels in-flight + rebuilds + re-registers
//! everything in one shot). Refuses on the dispatcher side if the
//! project has infra nodes that aren't running.

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Resync, |progress| async move {
        run_inner(&ctx_inner, &progress).await
    })
    .await
}

async fn run_inner(ctx: &Ctx, progress: &crate::progress::Progress) -> anyhow::Result<()> {
    let handle = super::ensure::ensure_registered(ctx, progress).await?;
    let path = format!("/projects/{}/resync", handle.id);
    let body = serde_json::json!({
        "sourceHash": handle.source_hash,
        "infraHash": handle.infra_hash,
    });
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

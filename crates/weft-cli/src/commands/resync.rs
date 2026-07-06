//! `weft resync`. Deactivate-then-activate against a fresh worker
//! image, with the USER'S trigger-deactivation choice (mode + running
//! policy + drain cap; same picker as `weft deactivate`). Used after
//! editing the trigger or fire subgraph. Refuses on the dispatcher
//! side if the project has infra nodes that aren't running.

use super::Ctx;
use crate::commands::infra::InfraOpts;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, opts: InfraOpts) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Resync, |progress| async move {
        run_inner(&ctx_inner, &progress, opts).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    opts: InfraOpts,
) -> anyhow::Result<()> {
    let handle = super::ensure::ensure_registered(ctx, progress).await?;
    // The dispatcher requires the trigger-deactivation choice when the
    // project is Active (412 otherwise): resync deactivates with the
    // user's spec, never a hardcoded one. Same shared prompt as
    // `weft deactivate` / the infra verbs.
    let active = super::deactivate::project_is_active(&handle.client, &handle.id).await?;
    let trigger_deactivation = if active {
        Some(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            "resync",
            opts.mode.as_deref(),
            opts.grace,
            opts.running_policy.as_deref(),
            opts.drain_timeout,
        )?)
    } else {
        None
    };
    let path = format!("/projects/{}/resync", handle.id);
    let mut body_map = serde_json::Map::new();
    handle.inject_hash_fields(&mut body_map);
    if let Some(td) = trigger_deactivation {
        body_map.insert("triggerDeactivation".into(), td);
    }
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

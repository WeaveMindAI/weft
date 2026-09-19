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
    // Resync only re-registers an ACTIVE project (the dispatcher
    // refuses anything else with a 409: a parked project is brought
    // back with `weft activate`, by choice, never as a side effect),
    // and it needs the trigger-deactivation choice the shared prompt
    // asks for (`weft deactivate` / the infra verbs use the same one).
    // Both are settled BEFORE the build: registering loads the new
    // image and drops the one the project is active on, so a refusal
    // has to come before anything is touched.
    let project_id = ctx.project()?.id().to_string();
    if !super::deactivate::project_is_active(&ctx.client(), &project_id).await? {
        anyhow::bail!(
            "resync: this project is not active, and resync only re-registers an active \
             project; `weft activate` brings it up on the current source"
        );
    }
    let trigger_deactivation = super::deactivate::prompt_trigger_deactivation(
        ctx.json(),
        opts.mode.as_deref(),
        opts.grace,
        opts.running_policy.as_deref(),
        opts.drain_timeout,
    )?;
    let handle = super::ensure::ensure_registered(ctx, progress, weft_compiler::codegen::NodeSet::Full).await?;
    let path = format!("/projects/{}/resync", handle.id);
    let mut body_map = serde_json::Map::new();
    handle.inject_hash_fields(&mut body_map);
    progress.drain_wait(&trigger_deactivation, opts.drain_timeout);
    body_map.insert("triggerDeactivation".into(), trigger_deactivation);
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

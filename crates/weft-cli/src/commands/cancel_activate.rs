//! `weft cancel-activate [project]`. Cancel an in-flight `activate`:
//! the dispatcher cancels the setup run of each trigger still
//! activating, wipes every signal it registered so far, and leaves
//! those triggers inactive.
//!
//! 412 from the dispatcher when none of them is activating.

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, project: Option<String>, scope: weft_core::activation::ActivationScope) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::CancelActivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, scope).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    let id = super::resolve_project_id(ctx, project)?;
    let client = ctx.client()?;
    let path = format!("/projects/{id}/cancel-activate");
    progress.dispatcher_call_start(&path);
    client.post_with_body(&path, &serde_json::to_value(&scope)?).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    if !ctx.json() {
        println!("cancel-activate issued for {id}");
    }
    progress.complete(&format!("cancel-activate issued for {id}"));
    Ok(())
}

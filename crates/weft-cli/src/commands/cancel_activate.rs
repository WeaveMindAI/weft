//! `weft cancel-activate [project]`. Cancel an in-flight `activate`
//! (status=Activating). Wipes every signal row registered so far,
//! cancels the TriggerSetup color, CAS-flips status to Inactive.
//!
//! 412 from the dispatcher when the project isn't Activating.

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, project: Option<String>) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::CancelActivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
) -> anyhow::Result<()> {
    let id = super::resolve_project_id(ctx, project)?;
    let client = ctx.client();
    let path = format!("/projects/{id}/cancel-activate");
    progress.dispatcher_call_start(&path);
    client.post_with_body(&path, &serde_json::Value::Null).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    if !ctx.json() {
        println!("cancel-activate issued for {id}");
    }
    progress.complete(&format!("cancel-activate issued for {id}"));
    Ok(())
}

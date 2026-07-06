//! `weft cancel-build [project]`. Cancel an in-flight build
//! (transition=building). Flips the transition to cancelling_build;
//! the dispatcher pod driving the build interrupts the builder job and
//! the verb that was building errs "cancelled".
//!
//! 412 from the dispatcher when no build is in flight (which is the
//! case when the build already ran locally before the verb; there,
//! Ctrl+C the CLI to cancel a build).

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, project: Option<String>) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::CancelBuild, |progress| async move {
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
    let path = format!("/projects/{id}/cancel-build");
    progress.dispatcher_call_start(&path);
    client.post_empty(&path).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    if !ctx.json() {
        println!("cancel-build issued for {id}");
    }
    progress.complete(&format!("cancel-build issued for {id}"));
    Ok(())
}

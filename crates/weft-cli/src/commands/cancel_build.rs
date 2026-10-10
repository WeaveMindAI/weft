//! `weft cancel-build [project]`. Cancel an in-flight build
//! (transition=building): the install stops every image build the
//! project started, stops waiting on the ones another project started
//! (they go on for it), and the verb following the build errs
//! "cancelled". Ctrl+C on that verb only stops following: the builds go
//! on, and running it again picks them up.
//!
//! 412 from the dispatcher when no build is in flight.

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
    let client = ctx.client()?;
    let path = format!("/projects/{id}/cancel-build");
    progress.dispatcher_call_start(&path);
    client.post_empty(&path).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    progress.complete(&format!("cancel-build issued for {id}"));
    Ok(())
}

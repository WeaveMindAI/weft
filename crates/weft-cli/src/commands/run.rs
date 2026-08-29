//! `weft run`: compile + register the cwd project, kick off a fresh
//! run, stream logs until completion (or `--detach`).
//!
//! A run has no entry point of its own: the dispatcher collects the
//! project's output nodes and walks upstream from them, so what
//! executes is exactly what some output needs. `--target <node>`,
//! repeatable, narrows that set, which is how you exercise one branch
//! of a project without running its siblings.

use anyhow::Context;

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, detach: bool, targets: Vec<String>) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Run, |progress| async move {
        run_inner(&ctx_inner, &progress, detach, targets).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    detach: bool,
    targets: Vec<String>,
) -> anyhow::Result<()> {
    let handle = super::ensure::ensure_registered(ctx, progress).await?;
    if !ctx.json() {
        println!("registered {} ({})", handle.name, handle.id);
    }

    // `targets` narrows the run to those output nodes' upstream
    // subgraphs; the dispatcher defaults to every output node when the
    // list is empty, and refuses a target that is not an output.
    let body = serde_json::json!({
        "payload": serde_json::Value::Null,
        "targets": targets,
    });

    let path = format!("/projects/{}/run", handle.id);
    progress.dispatcher_call_start(&path);
    let run_resp: serde_json::Value = handle
        .client
        .post_json(&path, &body)
        .await
        .context("run project")?;
    let color = run_resp
        .get("color")
        .and_then(|v| v.as_str())
        .context("run response missing color")?
        .to_string();
    progress.dispatcher_call_done(serde_json::json!({
        "color": color,
        "project_id": handle.id,
    }));

    if !ctx.json() {
        println!("started color {color}");
    }

    progress.complete(&format!("started {color}"));

    // --json implies --detach: the extension uses SSE for execution
    // events, so keeping the CLI alive to follow logs would just
    // hold the action-bar state machine in `cli_running` while the
    // run is actually `execution_running`. Plain CLI users (no
    // --json) keep the follow behavior unless they explicitly pass
    // --detach.
    if detach || ctx.json() {
        return Ok(());
    }
    super::follow::follow_color(&handle.client, &color).await?;
    Ok(())
}

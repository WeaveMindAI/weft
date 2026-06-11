//! `weft run`: compile + register the cwd project, kick off a fresh
//! run, stream logs until completion (or `--detach`).

use anyhow::Context;

use super::Ctx;
use crate::commands::ensure::{parse_running_policy_flag, RunningPolicy};
use crate::progress::ActionVerb;

pub async fn run(
    ctx: Ctx,
    detach: bool,
    running_policy: Option<String>,
) -> anyhow::Result<()> {
    let parsed_policy = parse_running_policy_flag(running_policy.as_deref())?;
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Run, |progress| async move {
        run_inner(&ctx_inner, &progress, detach, parsed_policy).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    detach: bool,
    running_policy: Option<RunningPolicy>,
) -> anyhow::Result<()> {
    // reactivates_after_gate=false: `run` fires a one-off execution and
    // never re-enables triggers, so if the gate parked an active project
    // to drain, ensure_registered warns the user to `weft activate`.
    let handle = super::ensure::ensure_registered(ctx, progress, running_policy, false).await?;
    if !ctx.json() {
        println!("registered {} ({})", handle.name, handle.id);
    }

    let path = format!("/projects/{}/run", handle.id);
    progress.dispatcher_call_start(&path);
    let run_resp: serde_json::Value = handle
        .client
        .post_json(
            &path,
            &serde_json::json!({ "payload": serde_json::Value::Null }),
        )
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

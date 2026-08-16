//! `weft rm [project] [--infra] [--journal] [--local] [--all]`:
//! multi-level project cleanup.
//!
//! Levels, cheapest to most-destructive:
//!
//! | flag        | action                                                 |
//! |-------------|--------------------------------------------------------|
//! | (none)      | deactivate + unregister on dispatcher                  |
//! | `--infra`   | also terminate infra pods (deletes PVCs, auth gone)      |
//! | `--journal` | also drop this project's execution + log rows          |
//! | `--local`   | also wipe `.weft/target/` under the cwd project        |
//! | `--all`     | implies the three levels above                         |
//!
//! Flags are additive. `--all` is pure sugar. The default
//! (no-arg, no-flag) is safe: the user's k8s infra survives unless
//! they explicitly ask for those levels. Worker images are content
//! addressed and shared across projects, so no per-project level can
//! reclaim them; `weft clean --images` is the reclaimer.

use anyhow::{Context, Result};

use super::{resolve_project_id, Ctx};
use crate::progress::{ActionVerb, Progress};

pub struct RmArgs {
    pub project: Option<String>,
    pub infra: bool,
    pub journal: bool,
    pub local: bool,
    pub all: bool,
    /// `weft rm --force`: skip the supervisor terminate-wait window.
    pub force: bool,
}

pub async fn run(ctx: Ctx, args: RmArgs) -> Result<()> {
    let RmArgs {
        project,
        mut infra,
        mut journal,
        mut local,
        all,
        force,
    } = args;
    if all {
        infra = true;
        journal = true;
        local = true;
    }

    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Rm, |progress| async move {
        let ctx = ctx_inner;
        let project_id = resolve_project_id(&ctx, project)?;
        let client = ctx.client();

        // Level 1: always. Deactivate first (so signals leave the
        // listener cleanly), then unregister. Both must succeed: if
        // either fails the project is left in an inconsistent state
        // (deactivate half-done OR registration alive while signals
        // wiped). Bubble loudly so the user sees what to fix.
        // `/deactivate` takes a required `DeactivateSpec` body (the handler's
        // `Json` extractor rejects an empty request). `rm` always means a full
        // teardown, so send the explicit wipe spec: drop every signal and
        // cancel running executions (wipe is only legal with cancel). Same
        // canonical body `weft deactivate` posts.
        progress.dispatcher_call_start(&format!("/projects/{project_id}/deactivate"));
        client
            .post_with_body(
                &format!("/projects/{project_id}/deactivate"),
                &serde_json::json!({ "mode": "wipe", "runningPolicy": "cancel" }),
            )
            .await
            .context("deactivate")?;
        progress.dispatcher_call_done(serde_json::json!({ "step": "deactivate" }));

        if infra {
            progress.dispatcher_call_start(&format!(
                "/projects/{project_id}/infra/terminate"
            ));
            client
                .post_empty(&format!("/projects/{project_id}/infra/terminate"))
                .await
                .context("infra terminate")?;
            progress.dispatcher_call_done(serde_json::json!({ "step": "infra_terminate" }));
        }

        // `--force` flips on the dispatcher's skip-the-wait switch.
        // Without it, the dispatcher waits up to 120s for the
        // supervisor to confirm the terminate command landed before
        // deleting the project namespace (cf docs §13.10).
        let unregister_path = if force {
            format!("/projects/{project_id}?force=true")
        } else {
            format!("/projects/{project_id}")
        };
        progress.dispatcher_call_start(&unregister_path);
        // Idempotent: a marker-404 ("no such project") on a delete means the
        // project is already gone, which is rm's desired end state (a retry
        // after a lost success response must not fail).
        client
            .delete_idempotent(&unregister_path)
            .await
            .context("dispatcher unregister")?;
        progress.dispatcher_call_done(serde_json::json!({ "step": "unregister" }));

        if journal {
            drop_journal_rows(&progress, &client, &project_id).await?;
        }
        if local {
            wipe_local_artifacts(&ctx, &progress)?;
        }
        progress.complete(&format!("rm completed for {project_id}"));
        Ok(())
    })
    .await
}

async fn drop_journal_rows(
    progress: &Progress,
    client: &crate::client::DispatcherClient,
    project_id: &str,
) -> Result<()> {
    // Walk the execution list and delete colors individually (the
    // dispatcher has no bulk DELETE for a project's journal rows).
    // `/executions` is paginated (`{ executions, total }`) with a dispatcher-side
    // project filter; deleting shifts offsets, so re-fetch the FIRST page
    // after each batch until it comes back empty.
    let mut dropped = 0u32;
    loop {
        let page: serde_json::Value = client
            .get_json(&format!("/executions?project_id={project_id}&limit=200"))
            .await
            .context("list executions")?;
        let Some(arr) = page.get("executions").and_then(|v| v.as_array()) else {
            anyhow::bail!("/executions returned no `executions` array: {page}");
        };
        if arr.is_empty() {
            break;
        }
        for e in arr {
            let Some(color) = e.get("color").and_then(|v| v.as_str()) else {
                anyhow::bail!("/executions row without a color: {e}");
            };
            client
                .delete(&format!("/executions/{color}"))
                .await
                .with_context(|| format!("delete execution {color}"))?;
            dropped += 1;
        }
    }
    progress.dispatcher_call_done(serde_json::json!({
        "step": "journal_drop",
        "dropped": dropped,
    }));
    Ok(())
}

fn wipe_local_artifacts(ctx: &Ctx, progress: &Progress) -> Result<()> {
    // Use the ctx-cached project. If the cwd isn't a weft project,
    // surface that loudly: --local was requested but there's nothing
    // local to wipe.
    let project = ctx.project().context("--local requested, but no project in cwd")?;
    let target = project.state_dir().join("target");
    if target.exists() {
        std::fs::remove_dir_all(&target)
            .with_context(|| format!("remove {}", target.display()))?;
        progress.dispatcher_call_done(serde_json::json!({
            "step": "local_wipe",
            "path": target.display().to_string(),
        }));
    } else {
        progress.dispatcher_call_done(serde_json::json!({
            "step": "local_wipe",
            "path": target.display().to_string(),
            "skipped": "missing",
        }));
    }
    Ok(())
}

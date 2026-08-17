//! `weft rm [project] [--journal] [--local] [--all] [--force]`:
//! multi-level project cleanup.
//!
//! Levels, cheapest to most-destructive:
//!
//! | flag        | action                                                  |
//! |-------------|---------------------------------------------------------|
//! | (none)      | unregister: the dispatcher deactivates the project,     |
//! |             | terminates its infra pods (PVCs included), reclaims its |
//! |             | stored data, and drops the row                          |
//! | `--journal` | also drop this project's execution + log rows           |
//! | `--local`   | also wipe `.weft/target/` under the cwd project         |
//! | `--all`     | implies the two levels above                            |
//!
//! Flags are additive. `--all` is pure sugar. The dispatcher-side
//! teardown is ONE call (`DELETE /projects/{id}`): the dispatcher owns
//! the deactivate + infra + storage cascade, so the CLI never re-derives
//! which of those steps apply. Worker images are content addressed and
//! shared across projects, so no per-project level can reclaim them;
//! `weft clean --images` is the reclaimer.

use std::collections::HashSet;

use anyhow::{Context, Result};

use super::{resolve_project_id, Ctx};
use crate::progress::{ActionVerb, Progress};

pub struct RmArgs {
    pub project: Option<String>,
    pub journal: bool,
    pub local: bool,
    pub all: bool,
    /// `weft rm --force`: skip the supervisor terminate-wait window.
    pub force: bool,
}

pub async fn run(ctx: Ctx, args: RmArgs) -> Result<()> {
    let RmArgs {
        project,
        mut journal,
        mut local,
        all,
        force,
    } = args;
    if all {
        journal = true;
        local = true;
    }
    // `--local` wipes the CWD project's build artifacts; combined with
    // an explicit project id it would unregister project A while
    // deleting project B's `.weft/target/`. Refuse up front, before
    // any irreversible dispatcher call.
    if local && project.is_some() {
        anyhow::bail!(
            "--local (or --all, which implies it) wipes the cwd project's build \
             artifacts, so it cannot be combined with an explicit project id; \
             cd into that project and rerun, or use --journal for the \
             dispatcher-side levels only"
        );
    }

    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Rm, |progress| async move {
        let ctx = ctx_inner;
        let project_id = resolve_project_id(&ctx, project)?;
        let client = ctx.client();

        // Journal rows BEFORE unregistering: deleting a color is
        // authorized through its project row, so once the project is
        // unregistered its rows become undeletable until the project
        // re-registers.
        if journal {
            drop_journal_rows(&progress, &client, &project_id).await?;
        }

        // ONE call is the whole dispatcher-side teardown: the dispatcher
        // deactivates the project (wipes its signals, cancels running
        // executions), terminates its infra pods, reclaims its stored
        // data, then drops the row. `--force` flips on the dispatcher's
        // skip-the-wait switch: without it, the dispatcher waits up to
        // 120s for the supervisor to confirm the terminate command
        // landed before deleting the project namespace (cf docs §13.10).
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

        if local {
            wipe_local_artifacts(&ctx, &progress)?;
        }
        progress.complete(&format!("rm completed for {project_id}"));
        Ok(())
    })
    .await
}

/// One page of the project's journal listing: the colors on the first
/// page, newest first.
async fn journal_page(
    client: &crate::client::DispatcherClient,
    project_id: &str,
) -> Result<Vec<String>> {
    let page: serde_json::Value = client
        .get_json(&format!("/executions?project_id={project_id}"))
        .await
        .context("list executions")?;
    let Some(arr) = page.get("executions").and_then(|v| v.as_array()) else {
        anyhow::bail!("/executions returned no `executions` array: {page}");
    };
    arr.iter()
        .map(|e| {
            e.get("color")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("/executions row without a color: {e}"))
        })
        .collect()
}

async fn drop_journal_rows(
    progress: &Progress,
    client: &crate::client::DispatcherClient,
    project_id: &str,
) -> Result<()> {
    // Walk the execution list and delete colors individually (the
    // dispatcher has no bulk DELETE for a project's journal rows).
    // Deleting shifts offsets, so re-fetch the FIRST page after each
    // batch until it comes back empty. Termination is structural:
    // every round must list at least one color we have not deleted
    // yet; a page of only already-deleted colors means DELETE
    // reported success while the row survived, so bail loudly
    // instead of spinning. (Progress is measured on the drained set,
    // never on the live total: a run started mid-drop is just a new
    // color the next round deletes.)
    let mut dropped: HashSet<String> = HashSet::new();
    loop {
        let rows = journal_page(client, project_id).await?;
        if rows.is_empty() {
            break;
        }
        let fresh: Vec<&String> = rows.iter().filter(|c| !dropped.contains(*c)).collect();
        if fresh.is_empty() {
            anyhow::bail!(
                "journal drop stalled: the dispatcher still lists {} colors whose \
                 DELETE already reported success",
                rows.len()
            );
        }
        if dropped.is_empty() {
            // Deactivate before the first delete: a still-running
            // execution would keep appending events to a color
            // mid-erase. The wipe spec drops every signal and cancels
            // running executions (wipe is only legal with cancel);
            // same canonical body `weft deactivate` posts. Only
            // reached when rows exist, so a rerun after a completed rm
            // (project already gone, nothing to drop) skips it instead
            // of failing on the missing project.
            progress.dispatcher_call_start(&format!("/projects/{project_id}/deactivate"));
            client
                .post_with_body(
                    &format!("/projects/{project_id}/deactivate"),
                    &serde_json::json!({ "mode": "wipe", "runningPolicy": "cancel" }),
                )
                .await
                .with_context(|| format!(
                    "could not quiesce project {project_id} before dropping its \
                     journal rows. If the project is no longer registered, its \
                     history is still stored but cannot be dropped until it \
                     re-registers: run `weft run` in the project folder, then \
                     rerun `weft rm --journal`"
                ))?;
            progress.dispatcher_call_done(serde_json::json!({ "step": "deactivate" }));
        }
        for color in fresh {
            client
                .delete(&format!("/executions/{color}"))
                .await
                .with_context(|| format!("delete execution {color}"))?;
            dropped.insert(color.clone());
        }
    }
    progress.dispatcher_call_done(serde_json::json!({
        "step": "journal_drop",
        "dropped": dropped.len(),
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

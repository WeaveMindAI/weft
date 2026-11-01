//! `weft rm [project] [--journal] [--local] [--all] [--force] [--yes]`:
//! multi-level project cleanup.
//!
//! Levels, cheapest to most-destructive:
//!
//! | flag        | action                                                  |
//! |-------------|---------------------------------------------------------|
//! | (none)      | unregister: the dispatcher deactivates the project,     |
//! |             | terminates its infra containers and disks, erases its   |
//! |             | runs, reclaims its stored data, and drops the row       |
//! | `--journal` | also drop this project's run + log rows           |
//! | `--local`   | also wipe this project's build artifacts on the host    |
//! | `--all`     | implies the two levels above                            |
//!
//! Flags are additive. `--all` is pure sugar. The dispatcher-side
//! teardown is two calls: `POST /projects/{id}/quiesce` takes the project
//! down and answers the task waiting for what that cancelled, which is
//! followed in short looks however long it takes (Ctrl+C stops following,
//! the wait goes on, and `weft rm` again picks it up); then `DELETE
//! /projects/{id}` owns the infra + storage cascade, so the CLI never
//! re-derives which of those steps apply. Worker images are content addressed and
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
    /// `weft rm --force`: skip the supervisor terminate-wait window, and
    /// go on past cloud leftovers (never past a worker it cannot kill).
    pub force: bool,
    /// `weft rm --yes`: the confirmation, answered up front.
    pub yes: bool,
}

pub async fn run(ctx: Ctx, args: RmArgs) -> Result<()> {
    let RmArgs {
        project,
        mut journal,
        mut local,
        all,
        force,
        yes,
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

    // The base verb is already the big one: triggers taken down with
    // nothing kept, runs
    // cancelled and erased, infra processes terminated, stored data
    // reclaimed. Nothing
    // that irreversible runs on a bare command: a terminal is asked,
    // and a script has to say `--yes`.
    let project_id = resolve_project_id(&ctx, project)?;
    if !yes {
        let mut levels = vec!["unregister it (triggers taken down with nothing kept, runs cancelled and erased, infra terminated, stored data reclaimed)"];
        if journal {
            levels.push("drop its run and log rows");
        }
        if local {
            levels.push("wipe .weft/target/ here");
        }
        println!("About to remove project {project_id}: {}.", levels.join("; "));
        if !crate::prompt::confirm("Type 'yes' to confirm: ", "--yes")? {
            println!("aborted");
            return Ok(());
        }
    }

    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Rm, |progress| async move {
        let ctx = ctx_inner;
        let client = ctx.client()?;

        // The project marked being removed and taken down whole, every
        // live run cancelled (started by a trigger, by hand, or
        // unrecorded), then what that cancelled followed until it is
        // over: the cancelled runs' last calls, and, for `--journal`,
        // every run ended, since a run still going would keep appending
        // events to a run mid-erase. Already gone (a rerun after a
        // finished rm) is rm's end state: its runs went with it.
        let quiesce = format!("/projects/{project_id}/quiesce");
        progress.dispatcher_call_start(&quiesce);
        let body = serde_json::to_value(weft_core::projects::QuiesceRequest { every_run: journal })?;
        let quiesced = client
            .post_idempotent_json(&quiesce, &body)
            .await
            .with_context(|| format!("could not take project {project_id} down to remove it"))?
            .map(serde_json::from_value::<weft_core::projects::Quiesced>)
            .transpose()
            .context("read the quiesce's answer")?;
        if let Some(quiesced) = quiesced {
            // The wait says nothing on the way (`removal_wait`).
            super::follow::follow_task(&client, &progress, &project_id, quiesced.waiting, async |_| Ok(()))
                .await
                .context("waiting for what the removal cancelled to be over")?;
            progress.dispatcher_call_done(serde_json::json!({ "step": "quiesce" }));
            // Journal rows BEFORE unregistering: deleting a run is
            // authorized through its project row.
            if journal {
                drop_journal_rows(&progress, &client, &project_id).await?;
            }
        }

        // The rest of the teardown is one call: the dispatcher kills the
        // project's workers, terminates its infra processes, reclaims its
        // stored data, then drops the row. `--force` flips
        // on the dispatcher's skip-the-wait switch: without it, the
        // dispatcher waits up to 120s for the supervisor to confirm the
        // terminate command landed before deleting the project.
        let unregister_path = if force {
            format!("/projects/{project_id}?force=true")
        } else {
            format!("/projects/{project_id}")
        };
        progress.dispatcher_call_start(&unregister_path);
        // Idempotent: a marker-404 ("no such project") on a delete means the
        // project is already gone, which is rm's desired end state (a retry
        // after a lost success response must not fail).
        // It answers once what the project held is taken off the install
        // (its services, its stored data), however long that takes.
        let removed = client
            .waiting_on_its_work()
            .delete_idempotent_json(&unregister_path)
            .await
            .context("dispatcher unregister")?
            .map(serde_json::from_value::<weft_core::projects::ProjectRemoved>)
            .transpose()
            .context("read what the removal left")?
            .unwrap_or_default();
        progress.dispatcher_call_done(serde_json::json!({ "step": "unregister" }));
        // What a forced removal could not take off the cloud: the person
        // has to know, it may still run and cost.
        for leftover in &removed.left {
            progress.warn(leftover);
        }

        if local {
            wipe_local_artifacts(&ctx, &progress)?;
        }
        progress.complete(&format!("rm completed for {project_id}"));
        Ok(())
    })
    .await
}

/// One page of the project's journal listing: the runs on the first
/// page, newest first.
async fn journal_page(
    client: &crate::client::DispatcherClient,
    project_id: &str,
) -> Result<Vec<String>> {
    let page: weft_core::program::RunPage = serde_json::from_value(
        client.get_json(&format!("/runs?project_id={project_id}")).await.context("list runs")?,
    )
    .context("read the runs listing")?;
    Ok(page.runs.iter().map(|e| e.run_id.to_string()).collect())
}

async fn drop_journal_rows(
    progress: &Progress,
    client: &crate::client::DispatcherClient,
    project_id: &str,
) -> Result<()> {
    // Walk the run list and delete runs individually (the
    // dispatcher has no bulk DELETE for a project's journal rows).
    // Deleting shifts offsets, so re-fetch the FIRST page after each
    // batch until it comes back empty. Termination is structural:
    // every round must list at least one run we have not deleted
    // yet; a page of only already-deleted runs means DELETE
    // reported success while the row survived, so bail loudly
    // instead of spinning. (Progress is measured on the drained set,
    // never on the live total: a run started mid-drop is just a new
    // run the next round deletes.)
    let mut dropped: HashSet<String> = HashSet::new();
    loop {
        let rows = journal_page(client, project_id).await?;
        if rows.is_empty() {
            break;
        }
        let fresh: Vec<&String> = rows.iter().filter(|c| !dropped.contains(*c)).collect();
        if fresh.is_empty() {
            anyhow::bail!(
                "journal drop stalled: the dispatcher still lists {} run_ids whose \
                 DELETE already reported success",
                rows.len()
            );
        }
        for run_id in fresh {
            // A run that stored many files takes as long as removing them.
            client
                .waiting_on_its_work()
                .delete(&format!("/runs/{run_id}"))
                .await
                .with_context(|| format!("delete run {run_id}"))?;
            dropped.insert(run_id.clone());
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
    // Two places hold this project's build artifacts: its own
    // `.weft/target/`, and its slice of the machine-wide node-test
    // cache. The shared cache's TARGET dir is deliberately left alone,
    // because it belongs to every project on the machine; only this
    // project's emitted crates and listings go.
    for target in [
        project.state_dir().join("target"),
        super::test_node::project_cache_slice(&project.root),
    ] {
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
    }
    Ok(())
}

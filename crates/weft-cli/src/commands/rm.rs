//! `weft rm [project] [--infra] [--journal] [--image] [--local]
//! [--all]`: multi-level project cleanup.
//!
//! Levels, cheapest to most-destructive:
//!
//! | flag        | action                                                 |
//! |-------------|--------------------------------------------------------|
//! | (none)      | deactivate + unregister on dispatcher                  |
//! | `--infra`   | also terminate sidecars (deletes PVCs, auth gone)      |
//! | `--journal` | also drop this project's execution + log rows          |
//! | `--image`   | also remove every `weft-worker-<id>:*` tag from docker+kind |
//! | `--local`   | also wipe `.weft/target/` under the cwd project        |
//! | `--all`     | implies the four levels above                          |
//!
//! Flags are additive. `--all` is pure sugar. The default
//! (no-arg, no-flag) is safe: the user's k8s infra and cached
//! images survive unless they explicitly ask for those levels.

use anyhow::{Context, Result};
use tokio::process::Command;

use super::{resolve_project_id, Ctx};
use crate::commands::daemon::{cluster_config, ClusterBackend};
use crate::progress::{ActionVerb, Progress};

pub struct RmArgs {
    pub project: Option<String>,
    pub infra: bool,
    pub journal: bool,
    pub image: bool,
    pub local: bool,
    pub all: bool,
}

pub async fn run(ctx: Ctx, args: RmArgs) -> Result<()> {
    let RmArgs {
        project,
        mut infra,
        mut journal,
        mut image,
        mut local,
        all,
    } = args;
    if all {
        infra = true;
        journal = true;
        image = true;
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
        progress.dispatcher_call_start(&format!("/projects/{project_id}/deactivate"));
        client
            .post_empty(&format!("/projects/{project_id}/deactivate"))
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

        progress.dispatcher_call_start(&format!("/projects/{project_id}"));
        client
            .delete(&format!("/projects/{project_id}"))
            .await
            .context("dispatcher unregister")?;
        progress.dispatcher_call_done(serde_json::json!({ "step": "unregister" }));

        if journal {
            drop_journal_rows(&progress, &client, &project_id).await?;
        }
        if image {
            remove_worker_image(&progress, &project_id).await?;
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
    let execs: serde_json::Value = client
        .get_json("/executions")
        .await
        .context("list executions")?;
    let Some(arr) = execs.as_array() else {
        anyhow::bail!("/executions returned non-array: {execs}");
    };
    let mut dropped = 0u32;
    for e in arr {
        let pid = e.get("project_id").and_then(|v| v.as_str()).unwrap_or("");
        if pid != project_id {
            continue;
        }
        let Some(color) = e.get("color").and_then(|v| v.as_str()) else {
            continue;
        };
        client
            .delete(&format!("/executions/{color}"))
            .await
            .with_context(|| format!("delete execution {color}"))?;
        dropped += 1;
    }
    progress.dispatcher_call_done(serde_json::json!({
        "step": "journal_drop",
        "dropped": dropped,
    }));
    Ok(())
}

async fn remove_worker_image(progress: &Progress, project_id: &str) -> Result<()> {
    // Worker tags are `weft-worker-<id>:<short-hash>`; the hash
    // changes every rebuild. List every tag for the project and rmi
    // each one.
    let repo = format!("weft-worker-{project_id}");
    let listing = Command::new("docker")
        .args(["images", "--format", "{{.Repository}}:{{.Tag}}", &repo])
        .output()
        .await
        .context("docker images")?;
    if !listing.status.success() {
        anyhow::bail!(
            "docker images failed: {}",
            String::from_utf8_lossy(&listing.stderr)
        );
    }
    let tags: Vec<String> = String::from_utf8_lossy(&listing.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s.starts_with(&format!("{repo}:")))
        .collect();
    if tags.is_empty() {
        progress.dispatcher_call_done(serde_json::json!({
            "step": "image_remove",
            "removed": 0,
        }));
        return Ok(());
    }
    for tag in &tags {
        let st = Command::new("docker")
            .args(["image", "rm", "-f", tag])
            .status()
            .await
            .with_context(|| format!("docker image rm {tag}"))?;
        if !st.success() {
            anyhow::bail!("docker image rm {tag} exited {st}");
        }
    }
    let cfg = cluster_config();
    if cfg.backend == ClusterBackend::Kind {
        let node = format!("{}-control-plane", cfg.cluster_name);
        for tag in &tags {
            // crictl rmi is best-effort: if the kind cluster isn't
            // running OR doesn't have this image cached, that's
            // fine. The tag is gone from the docker host already,
            // which is what matters for the next rebuild.
            let _ = Command::new("docker")
                .args(["exec", &node, "crictl", "rmi", tag])
                .status()
                .await;
        }
    }
    progress.dispatcher_call_done(serde_json::json!({
        "step": "image_remove",
        "removed": tags.len(),
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

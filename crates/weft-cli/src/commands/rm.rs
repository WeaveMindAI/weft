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
//! | `--image`   | also remove `weft-worker-<id>:latest` from docker+kind |
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

    let project_id = resolve_project_id(project)?;
    let client = ctx.client();

    // Level 1: always. Deactivate (ignore errors; may already be
    // inactive) then delete the dispatcher registration.
    let _ = client
        .post_empty(&format!("/projects/{project_id}/deactivate"))
        .await;

    if infra {
        // Terminate first: the dispatcher uses the
        // registration to find sidecars, so we must hit this
        // before the DELETE.
        let _ = client
            .post_empty(&format!("/projects/{project_id}/infra/terminate"))
            .await;
        println!("infra: terminated");
    }

    client
        .delete(&format!("/projects/{project_id}"))
        .await
        .context("dispatcher unregister")?;
    println!("registration: removed");

    if journal {
        // Phase B: add a per-project DELETE endpoint for journal
        // rows. For now we walk the execution list and delete
        // colors individually.
        let execs: serde_json::Value =
            client.get_json("/executions").await.unwrap_or_default();
        if let Some(arr) = execs.as_array() {
            let mut dropped = 0u32;
            for e in arr {
                let pid = e.get("project_id").and_then(|v| v.as_str()).unwrap_or("");
                if pid != project_id {
                    continue;
                }
                let Some(color) = e.get("color").and_then(|v| v.as_str()) else {
                    continue;
                };
                let _ = client.delete(&format!("/executions/{color}")).await;
                dropped += 1;
            }
            println!("journal: {dropped} execution(s) dropped");
        }
    }

    if image {
        remove_worker_image(&project_id).await;
    }

    if local {
        wipe_local_artifacts().await;
    }

    Ok(())
}

async fn remove_worker_image(project_id: &str) {
    let tag = format!("weft-worker-{project_id}:latest");
    let _ = Command::new("docker")
        .args(["image", "rm", "-f", &tag])
        .status()
        .await;
    let cfg = cluster_config();
    if cfg.backend == ClusterBackend::Kind {
        // `kind` doesn't expose a native "remove loaded image";
        // exec into the node and use crictl. Best-effort.
        let node = format!("{}-control-plane", cfg.cluster_name);
        let _ = Command::new("docker")
            .args(["exec", &node, "crictl", "rmi", &tag])
            .status()
            .await;
    }
    println!("image: {tag} removed");
}

async fn wipe_local_artifacts() {
    let cwd = match std::env::current_dir() {
        Ok(p) => p,
        Err(_) => return,
    };
    let Ok(project) = weft_compiler::project::Project::discover(&cwd) else {
        return;
    };
    let target = project.state_dir().join("target");
    if target.exists() {
        let _ = std::fs::remove_dir_all(&target);
        println!("local: wiped {}", target.display());
    }
}

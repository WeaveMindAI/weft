//! `weft executions`, `weft events`, `weft clean`. Journal inspection
//! and cleanup. Graph view replay is an extension command; these are
//! the scripting surface.

use super::Ctx;
use crate::commands::daemon::ClusterBackend;

pub async fn list(ctx: Ctx, limit: u32) -> anyhow::Result<()> {
    let client = ctx.client();
    let resp: serde_json::Value = client
        .get_json(&format!("/executions?limit={limit}"))
        .await?;
    let Some(arr) = resp.as_array() else {
        println!("(no executions)");
        return Ok(());
    };
    if arr.is_empty() {
        println!("(no executions)");
        return Ok(());
    }
    println!(
        "{:<38} {:<38} {:<12} {:<20} {}",
        "color", "project_id", "status", "started_at", "entry_node"
    );
    for row in arr {
        let color = row.get("color").and_then(|v| v.as_str()).unwrap_or("?");
        let project = row.get("project_id").and_then(|v| v.as_str()).unwrap_or("?");
        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
        let entry = row.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?");
        println!("{color:<38} {project:<38} {status:<12} {started:<20} {entry}");
    }
    Ok(())
}

pub async fn events(ctx: Ctx, color: String) -> anyhow::Result<()> {
    let client = ctx.client();
    let resp: serde_json::Value = client
        .get_json(&format!("/executions/{color}/replay"))
        .await?;
    let Some(arr) = resp.as_array() else {
        println!("(no events)");
        return Ok(());
    };
    for row in arr {
        let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
        let node = row.get("node_id").and_then(|v| v.as_str()).unwrap_or("?");
        let at = row.get("at_unix").and_then(|v| v.as_u64()).unwrap_or(0);
        print!("[{at}] {kind:>9} {node}");
        if let Some(err) = row.get("error").and_then(|v| v.as_str()) {
            print!("  error={err}");
        }
        if let Some(output) = row.get("output") {
            if !output.is_null() {
                let summary = serde_json::to_string(output).unwrap_or_default();
                let trimmed = if summary.len() > 120 {
                    format!("{}...", &summary[..117])
                } else {
                    summary
                };
                print!("  output={trimmed}");
            }
        }
        println!();
    }
    Ok(())
}

pub async fn clean(
    ctx: Ctx,
    color: Option<String>,
    keep_days: u32,
    all: bool,
    images: bool,
    build_cache: bool,
) -> anyhow::Result<()> {
    if images || build_cache {
        if images {
            clean_worker_images(&ctx, all).await?;
        }
        if build_cache {
            clean_build_cache().await?;
        }
        return Ok(());
    }

    let client = ctx.client();
    if let Some(c) = color {
        client.delete(&format!("/executions/{c}")).await?;
        println!("deleted {c}");
        return Ok(());
    }

    // Bulk clean: list then delete those older than keep_days (or
    // all, if --all).
    let resp: serde_json::Value = client.get_json("/executions?limit=10000").await?;
    let arr = resp.as_array().cloned().unwrap_or_default();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs();
    let cutoff = if all {
        u64::MAX
    } else {
        now.saturating_sub(keep_days as u64 * 24 * 3600)
    };
    let mut count = 0usize;
    for row in arr {
        let Some(color) = row.get("color").and_then(|v| v.as_str()) else { continue };
        let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
        if all || started < cutoff {
            client.delete(&format!("/executions/{color}")).await?;
            count += 1;
        }
    }
    if all {
        println!("deleted {count} executions (all)");
    } else {
        println!("deleted {count} executions older than {keep_days}d");
    }
    Ok(())
}

/// Reclaim worker images no live project runs. Two layers of junk, both
/// handled:
///
///   1. TAGGED `weft-worker:<binary_hash>` images whose hash the dispatcher's
///      `GET /images/referenced` set does not cover (a rebuilt project leaves
///      its old tag behind; a deleted project leaves all of them; a draining
///      pod's image stays covered until the pod is terminal). The daemon must
///      be up; failing that is a loud error, never a guess (guessing "nothing
///      is referenced" would nuke live images).
///   2. Dangling (untagged) `weft.dev/project`-labelled leftovers.
///
/// Then (with `--all`, kind backend only) the kind node's own cached worker
/// images get the same treatment, computed from the node's own list (see
/// `stale_worker_node_refs`); system images are never touched there, and a
/// scoped (non-`--all`) run skips the node because crictl cannot see the
/// per-project build labels. Without `--all`, the host side is scoped to
/// the cwd project's images (label filter).
///
/// Not concurrent-safe with an in-flight `weft build`/`run` on this host: a
/// freshly built image is referenced by nothing until its register lands,
/// so a clean racing that window deletes it and that run fails loudly
/// (rebuild heals). Run cleans between builds, not during.
async fn clean_worker_images(ctx: &Ctx, all: bool) -> anyhow::Result<()> {
    use tokio::process::Command;

    // Referenced set: the dispatcher's authoritative answer, across all
    // tenants: every project's running binary hash UNION every
    // non-terminal worker pod's hash (a pod draining in-flight work may
    // still run an image its project no longer points at, and deleting
    // that image would strand a restart). Loud error if the daemon is
    // down; guessing "nothing is referenced" would nuke live images.
    // SYNC: response shape (JSON array of bare hash strings) <->
    //       crates/weft-dispatcher/src/api/project.rs referenced_images
    let referenced_json = ctx
        .client()
        .get_json("/images/referenced")
        .await
        .map_err(|e| anyhow::anyhow!("fetch referenced images (is the daemon up?): {e}"))?;
    let referenced: std::collections::BTreeSet<String> = referenced_json
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(str::to_string)
        .collect();

    // Candidate tags, optionally scoped to the cwd project via the
    // `weft.dev/project` label every worker build stamps.
    let mut ls_args: Vec<String> = vec![
        "images".into(),
        weft_compiler::build::WORKER_IMAGE_REPO.into(),
        "--format".into(),
        "{{.Tag}}".into(),
    ];
    if !all {
        // cwd project. If we can't discover one, bail with a hint
        // instead of silently nuking everything.
        let project = ctx.project().map_err(|e| {
            anyhow::anyhow!("{e}; pass --all to clean every project's images")
        })?;
        ls_args.push("--filter".into());
        ls_args.push(format!("label=weft.dev/project={}", project.id()));
        println!(
            "reclaiming worker images for project {} ({})",
            project.manifest.package.name,
            project.id()
        );
    } else {
        println!("reclaiming worker images no live project references");
    }
    let out = Command::new("docker").args(&ls_args).output().await?;
    if !out.status.success() {
        anyhow::bail!(
            "docker images exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let stale: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|t| !t.is_empty() && *t != "<none>" && !referenced.contains(*t))
        .map(|t| format!("{}:{t}", weft_compiler::build::WORKER_IMAGE_REPO))
        .collect();
    if stale.is_empty() {
        println!("no unreferenced worker images");
    } else {
        // No `--force`: nothing on the host should hold these (workers run
        // in the cluster, not host docker), so a refusal is real
        // information about state we did not expect, not an obstacle to
        // steamroll.
        let status = Command::new("docker").arg("rmi").args(&stale).status().await?;
        if !status.success() {
            anyhow::bail!("docker rmi exited {status}");
        }
        println!("removed {} unreferenced worker image(s)", stale.len());
    }

    // Dangling (untagged) leftovers from rebuilds under the same tag.
    let status = Command::new("docker")
        .args([
            "image", "prune", "--force",
            "--filter", "dangling=true",
            "--filter", "label=weft.dev/project",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker image prune exited {status}");
    }

    // The kind node caches every loaded worker image; remove the ones the
    // referenced set does not cover, computed from the NODE'S OWN image list
    // (the node can hold tags the host already dropped, so mirroring the
    // host's stale set would miss them). Only `weft-worker` refs are ever
    // touched. Never a blanket `crictl rmi --prune`: "unused right now"
    // includes the system images (listener pods spawn on demand, so between
    // spawns nothing uses `weft-listener:local`), and pruning those leaves
    // the next on-demand pod in ImagePullBackOff (the exact incident that
    // shaped this). Kind backend only: a k8s backend has no local node cache
    // to clean, so it skips quietly; on kind a failure here is a real error
    // (leftover node images are exactly what this verb exists to reclaim).
    // `--all` only: crictl cannot see docker build labels, so the node
    // sweep is inherently GLOBAL; running it under a project-scoped
    // invocation would remove OTHER projects' cached node images while
    // announcing a scoped clean.
    if !all {
        return Ok(());
    }
    let cfg = crate::commands::daemon::cluster_config();
    if cfg.backend != ClusterBackend::Kind {
        return Ok(());
    }
    let node_stale = stale_worker_node_refs(
        &crate::images::kind_node_repo_tags(&cfg.cluster_name).await?,
        &referenced,
    );
    if !node_stale.is_empty() {
        let node = format!("{}-control-plane", cfg.cluster_name);
        let out = Command::new("docker")
            .args(["exec", &node, "crictl", "rmi"])
            .args(&node_stale)
            .output()
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "crictl rmi on {node} failed for some worker images: {}; \
                 rerun `weft clean --images` after checking the node",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        println!(
            "removed {} stale worker image(s) from the {node} node",
            node_stale.len()
        );
    }
    Ok(())
}

/// From the node's image refs, the `weft-worker` repo tags whose hash is not
/// in `referenced`, ready for `crictl rmi`. Pure so it is unit-testable; only
/// worker-repo refs are ever returned, which is the guarantee that keeps
/// system images (listener & co) safe from this cleanup. Handles bare
/// (`weft-worker:<hash>`), docker-canonical (`docker.io/library/...`), and
/// registry-qualified (`host:port/path/weft-worker:<hash>`) spellings.
fn stale_worker_node_refs(
    node_refs: &[String],
    referenced: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    let worker_prefix = format!("{}:", weft_compiler::build::WORKER_IMAGE_REPO);
    node_refs
        .iter()
        .filter(|full| {
            let repo_tag = full.rsplit_once('/').map_or(full.as_str(), |(_, t)| t);
            match repo_tag.strip_prefix(&worker_prefix) {
                Some(hash) => !referenced.contains(hash),
                None => false,
            }
        })
        .cloned()
        .collect()
}

/// `docker buildx prune` reclaims BuildKit's intermediate layers.
/// This is the heavy reclaim: cargo deps, intermediate Rust compile
/// state, etc. The next build will re-download deps and re-link.
async fn clean_build_cache() -> anyhow::Result<()> {
    use tokio::process::Command;
    println!("pruning docker BuildKit cache (next build will be slower)…");
    let status = Command::new("docker")
        .args(["buildx", "prune", "--force"])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker buildx prune exited {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::stale_worker_node_refs;

    /// Only worker-repo tags leave this function, and only unreferenced ones:
    /// the system images (listener & co) must NEVER be node-pruned (a
    /// blanket prune once stranded on-demand listener pods in
    /// ImagePullBackOff), and a referenced worker must survive. All three
    /// ref spellings are handled: bare, docker-canonical, and
    /// registry-qualified with a host:port.
    #[test]
    fn node_cleanup_targets_only_unreferenced_worker_tags() {
        let refs: Vec<String> = [
            "docker.io/library/weft-listener:local",
            "docker.io/library/weft-worker:aaa111",
            "docker.io/library/weft-worker:bbb222",
            "registry.example.com:5000/weft-images/weft-worker:ccc333",
            "weft-worker:ddd444",
            "docker.io/library/debian:bookworm-slim",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let referenced = ["aaa111".to_string(), "ccc333".to_string()]
            .into_iter()
            .collect();
        assert_eq!(
            stale_worker_node_refs(&refs, &referenced),
            vec![
                "docker.io/library/weft-worker:bbb222".to_string(),
                "weft-worker:ddd444".to_string(),
            ]
        );
    }
}

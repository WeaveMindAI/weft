//! `weft executions`, `weft events`, `weft clean`. Journal inspection
//! and cleanup. Graph view replay is an extension command; these are
//! the scripting surface.

use super::Ctx;
use crate::commands::daemon::ClusterBackend;

/// One page of the dispatcher's execution listing. The body is
/// `{"executions": [...], "total": N}`; anything else is a broken
/// contract and fails loudly rather than reading as "no executions".
async fn executions_page(
    client: &crate::client::DispatcherClient,
    limit: u32,
    offset: u64,
    project: Option<&str>,
) -> anyhow::Result<(Vec<serde_json::Value>, u64)> {
    let filter = project.map(|p| format!("&project_id={p}")).unwrap_or_default();
    let resp: serde_json::Value = client
        .get_json(&format!("/executions?limit={limit}&offset={offset}{filter}"))
        .await?;
    let rows = resp
        .get("executions")
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!("/executions returned no `executions` array: {resp}")
        })?;
    let total = resp
        .get("total")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("/executions returned no `total`: {resp}"))?;
    Ok((rows, total))
}

pub async fn list(ctx: Ctx, limit: u32) -> anyhow::Result<()> {
    let client = ctx.client();
    let (arr, total) = executions_page(&client, limit, 0, None).await?;
    if arr.is_empty() {
        println!("(no executions)");
        return Ok(());
    }
    println!(
        "{:<38} {:<38} {:<12} {:<20} entry_node",
        "color", "project_id", "status", "started_at"
    );
    for row in &arr {
        let color = row.get("color").and_then(|v| v.as_str()).unwrap_or("?");
        let project = row.get("project_id").and_then(|v| v.as_str()).unwrap_or("?");
        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
        let entry = row.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?");
        println!("{color:<38} {project:<38} {status:<12} {started:<20} {entry}");
    }
    // The server clamps the page size, so a big --limit can come back
    // short; say so rather than letting the page read as the total.
    if (arr.len() as u64) < total {
        println!("showing {} of {total} (raise --limit or page with the API)", arr.len());
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
    keep_days: Option<u32>,
    all: bool,
    images: bool,
    build_cache: bool,
    project: Option<String>,
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
        anyhow::ensure!(
            project.is_none(),
            "a color names ONE execution, so --project cannot narrow it further: \
             drop one of them"
        );
        client.delete(&format!("/executions/{c}")).await?;
        println!("deleted {c}");
        return Ok(());
    }

    // Bulk clean: page through the listing and delete what the cutoff
    // selects. Deleting shifts the pages, so every pass re-reads from
    // offset 0 and stops when a pass deletes nothing (rows it chose to
    // keep are all that remain).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs();
    // One predicate: `None` = take them all, `Some(c)` = take what
    // started before c. Naming a SUBJECT means you mean all of it (a
    // color deletes outright; a project takes its whole history), so
    // the 30-day default guards only the sweep that names nothing.
    // `--keep-days` still narrows any of them when asked for.
    let days = match (keep_days, all, project.is_some()) {
        (Some(d), _, _) => Some(d),
        (None, true, _) => None,  // --all: no cutoff, as before
        (None, false, true) => None,  // a named project: all of its runs
        (None, false, false) => Some(30),  // the unnamed sweep's guard
    };
    let cutoff = days.map(|d| now.saturating_sub(d as u64 * 24 * 3600));
    let mut count = 0usize;
    loop {
        let mut offset = 0u64;
        let mut deleted_this_pass = 0usize;
        loop {
            let (rows, total) =
                executions_page(&client, 200, offset, project.as_deref()).await?;
            if rows.is_empty() {
                break;
            }
            let fetched = rows.len() as u64;
            for row in rows {
                let Some(color) = row.get("color").and_then(|v| v.as_str()) else { continue };
                let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
                if cutoff.is_none_or(|c| started < c) {
                    // A failed delete must not hide how far the sweep
                    // got: what is already gone stays gone.
                    if let Err(e) = client.delete(&format!("/executions/{color}")).await {
                        anyhow::bail!(
                            "deleted {count} executions, then deleting {color} failed: {e}. \
                             Re-run to continue the sweep."
                        );
                    }
                    count += 1;
                    deleted_this_pass += 1;
                }
            }
            offset += fetched;
            if offset >= total {
                break;
            }
        }
        if deleted_this_pass == 0 {
            break;
        }
    }
    let scope = match &project {
        Some(p) => format!(" of project {p}"),
        None => String::new(),
    };
    match days {
        Some(d) => println!("deleted {count} executions{scope} older than {d}d"),
        None => println!("deleted {count} executions{scope} (all)"),
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
/// `images::kind_node_image_tag_groups` + `images::node_images_condemned`);
/// system images are never touched there, and a
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

    // Referenced set: the dispatcher's authoritative answer (see
    // `images::referenced_image_hashes`). Loud error if the daemon is
    // down; guessing "nothing is referenced" would nuke live images.
    let referenced = crate::images::referenced_image_hashes(&ctx.client()).await?;

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
        // steamroll. One exception is honest: an image that is ALREADY
        // gone (a concurrent clean between our listing and this delete)
        // is already reclaimed. That case is detected by OBSERVING the
        // state (is the image still present?) rather than parsing the
        // daemon's error prose, which varies across versions and
        // locales. Per-image so one gone tag cannot abort the others.
        let mut removed = 0usize;
        let mut already_gone = 0usize;
        for image in &stale {
            let out = Command::new("docker").args(["rmi", image]).output().await?;
            if out.status.success() {
                removed += 1;
                continue;
            }
            let present = Command::new("docker")
                .args(["image", "inspect", image])
                .output()
                .await?
                .status
                .success();
            if !present {
                // "Absent" is only a verdict when the daemon itself
                // still answers: a daemon that died mid-sweep fails
                // BOTH the rmi and the inspect, and reporting that as
                // "already reclaimed" would announce success over a
                // failure.
                let daemon_up = Command::new("docker")
                    .args(["version", "--format", "{{.Server.Version}}"])
                    .output()
                    .await?
                    .status
                    .success();
                if daemon_up {
                    already_gone += 1;
                    continue;
                }
                anyhow::bail!(
                    "docker stopped answering while removing {image}; check the daemon \
                     and rerun `weft clean --images`"
                );
            }
            anyhow::bail!(
                "docker rmi {image} exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        println!(
            "removed {removed} unreferenced worker image(s){}",
            if already_gone > 0 {
                format!(", {already_gone} already reclaimed")
            } else {
                String::new()
            }
        );
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
    let node_stale = crate::images::node_images_condemned(
        weft_compiler::build::WORKER_IMAGE_REPO,
        &crate::images::kind_node_image_tag_groups(&cfg.cluster_name).await?,
        |hash| !referenced.contains(hash),
    );
    if !node_stale.is_empty() {
        let node = format!("{}-control-plane", cfg.cluster_name);
        // Per-ref for the same reason as the host sweep above: a
        // concurrent clean may have reclaimed a ref between our listing
        // and this delete, and "already gone" is a success, detected by
        // observing presence rather than parsing error prose.
        let mut removed = 0usize;
        let mut already_gone = 0usize;
        for image in &node_stale {
            let out = Command::new("docker")
                .args(["exec", &node, "crictl", "rmi", image])
                .output()
                .await?;
            if out.status.success() {
                removed += 1;
                continue;
            }
            let present = Command::new("docker")
                .args(["exec", &node, "crictl", "inspecti", image])
                .output()
                .await?
                .status
                .success();
            if !present {
                // "Absent" is only a verdict when the node still
                // answers exec at all: an unreachable node fails BOTH
                // the rmi and the inspecti through the same transport,
                // and reporting that as "already reclaimed" would
                // announce success over a dead node.
                let node_up = Command::new("docker")
                    .args(["exec", &node, "crictl", "--version"])
                    .output()
                    .await?
                    .status
                    .success();
                if node_up {
                    already_gone += 1;
                    continue;
                }
                anyhow::bail!(
                    "the {node} node stopped answering while removing {image}; check \
                     the kind cluster and rerun `weft clean --images`"
                );
            }
            anyhow::bail!(
                "crictl rmi {image} on {node} failed: {}; \
                 rerun `weft clean --images` after checking the node",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        println!(
            "removed {removed} stale worker image(s) from the {node} node{}",
            if already_gone > 0 {
                format!(", {already_gone} already reclaimed")
            } else {
                String::new()
            }
        );
    }
    Ok(())
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
    /// Only the named repo's images leave the shared matcher, and only
    /// fully condemned ones: the system images (listener & co) must NEVER
    /// be node-pruned (a blanket prune once stranded on-demand listener
    /// pods in ImagePullBackOff), and a referenced worker must survive.
    /// All three ref spellings are handled: bare, docker-canonical, and
    /// registry-qualified with a host:port.
    #[test]
    fn node_cleanup_targets_only_unreferenced_worker_images() {
        let one = |s: &str| vec![s.to_string()];
        let groups: Vec<Vec<String>> = vec![
            one("docker.io/library/weft-listener:local"),
            one("docker.io/library/weft-worker:aaa111"),
            one("docker.io/library/weft-worker:bbb222"),
            one("registry.example.com:5000/weft-images/weft-worker:ccc333"),
            one("weft-worker:ddd444"),
            one("docker.io/library/debian:bookworm-slim"),
        ];
        let referenced: std::collections::BTreeSet<String> =
            ["aaa111".to_string(), "ccc333".to_string()].into_iter().collect();
        assert_eq!(
            crate::images::node_images_condemned("weft-worker", &groups, |h| !referenced
                .contains(h)),
            vec![
                "docker.io/library/weft-worker:bbb222".to_string(),
                "weft-worker:ddd444".to_string(),
            ]
        );
    }

    /// `crictl rmi` removes the whole image behind a ref, so an image
    /// carrying a live tag alongside a condemned one (identical content
    /// loaded under two tags) must survive untouched; per-tag removal
    /// once deleted a freshly loaded test image this way.
    #[test]
    fn node_cleanup_spares_images_sharing_a_live_tag() {
        let groups: Vec<Vec<String>> = vec![
            vec![
                "docker.io/library/weft-worker:stale1".to_string(),
                "docker.io/library/weft-worker:live1".to_string(),
            ],
            vec![
                "docker.io/library/weft-worker:stale2".to_string(),
                "docker.io/library/weft-listener:local".to_string(),
            ],
            vec![
                "docker.io/library/weft-worker:stale3".to_string(),
                "weft-worker:stale4".to_string(),
            ],
        ];
        let live: std::collections::BTreeSet<String> = ["live1".to_string()].into_iter().collect();
        assert_eq!(
            crate::images::node_images_condemned("weft-worker", &groups, |h| !live.contains(h)),
            // Only the all-condemned image goes (one ref suffices: the
            // rmi takes the whole image with it); the live-tag and the
            // other-repo-tag images both survive.
            vec!["docker.io/library/weft-worker:stale3".to_string()]
        );
    }
}

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
            clean_build_images(&ctx, all).await?;
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

/// Reclaim the images a build produces (worker images, infra images,
/// old builder bases under `--all`) that nothing runs any more. Five
/// layers of junk, each its own sweep, none gating another (a layer
/// that fails is reported at the end, after every other layer ran):
///
///   1. TAGGED `weft-worker:<binary_hash>` images on host docker whose
///      hash the dispatcher's `GET /images/referenced` set does not
///      cover (a rebuilt project leaves its old tag behind; a deleted
///      project leaves all of them; a draining pod's image stays covered
///      until the pod is terminal). The daemon must be up; failing that
///      is a loud error before any layer runs, never a guess (guessing
///      "nothing is referenced" would nuke live images).
///   2. Dangling (untagged) leftovers, under the same project scope as
///      layer 1 (the `weft.dev/project` label value, or any value with
///      `--all`).
///   3. With `--all` only: host `weft-infra-<name>:<hash>` images whose
///      full ref the referenced set's `infraRefs` does not cover (every
///      project's complete tag map plus every recorded unit ref; one
///      image per infra-node content change, nothing else evicts them).
///      The supervisor's repo is excluded: `gc_stale_system_images`
///      (daemon start) owns its stale tags. `--all` only because infra
///      images are content-addressed and deduped ACROSS projects, so a
///      cwd-project scope over them is a fiction.
///   4. With `--all` only, kind backend only: the kind node's own cached
///      worker AND infra images outside the referenced set, in one pass
///      over the node's OWN image list (the node can hold tags the host
///      already dropped, so mirroring the host's stale set would miss
///      them). `--all` only because crictl cannot see docker build
///      labels, so the node sweep is inherently global.
///   5. With `--all` only: builder-base tags other than the current one
///      (each engine bump mints a fresh ~1.4GB base; only the current ref
///      is ever FROMed, and the base is shared across every project, so
///      reclaiming it is inherently global). Needs the weft repo root to
///      compute the current ref.
///
/// Without `--all`, the host side is scoped to the cwd project's images
/// (label filter).
///
/// An image docker or containerd refuses to drop because something
/// still runs it is not an error: it is reported as kept, and the next
/// clean gets it once nothing runs it. That is expected traffic here,
/// not only a race: a unit stamped before image refs were recorded
/// contributes nothing to the keep-set while it keeps running its
/// image.
///
/// Not concurrent-safe with an in-flight `weft build`/`run` on this host: a
/// freshly built image is referenced by nothing until its register lands,
/// so a clean racing that window deletes it and that run fails loudly
/// (rebuild heals). Run cleans between builds, not during.
async fn clean_build_images(ctx: &Ctx, all: bool) -> anyhow::Result<()> {
    // Referenced set: the dispatcher's authoritative answer (see
    // `images::referenced_images`: worker hashes + infra refs). Loud
    // error if the daemon is down; guessing "nothing is referenced"
    // would nuke live images.
    let referenced = crate::images::referenced_images(&ctx.client()).await?;
    // The host scope, settled BEFORE any layer runs: a scoped clean
    // that cannot name its project must delete nothing, not run the
    // global layers and report the error afterwards.
    let scope = if all {
        println!("reclaiming worker images no live project references");
        HostScope::All
    } else {
        let project = ctx.project().map_err(|e| {
            anyhow::anyhow!("{e}; pass --all to clean every project's images")
        })?;
        println!(
            "reclaiming worker images for project {} ({})",
            project.manifest.package.name,
            project.id()
        );
        HostScope::Project(project.id().to_string())
    };

    let mut failures: Vec<anyhow::Error> = Vec::new();
    let mut layer = |name: &str, result: anyhow::Result<()>| {
        if let Err(e) = result {
            failures.push(e.context(name.to_string()));
        }
    };
    layer("worker images", host_worker_sweep(&referenced, &scope).await);
    layer("dangling build leftovers", dangling_prune(&scope).await);
    if all {
        layer("infra images", host_infra_sweep(&referenced).await);
        layer("kind node images", node_sweep(&referenced).await);
        layer("builder-base images", builder_base_sweep().await);
    }
    if failures.is_empty() {
        return Ok(());
    }
    let mut msg = format!("{} reclaim layer(s) failed:", failures.len());
    for e in &failures {
        msg.push_str(&format!("\n  {e:#}"));
    }
    anyhow::bail!("{msg}");
}

/// Which host images layers 1 and 2 may touch: every project's, or
/// one project's through the `weft.dev/project` label every build
/// stamps.
enum HostScope {
    All,
    Project(String),
}

impl HostScope {
    /// The `docker` label filter for this scope: the label with its
    /// value for one project, the label's presence for all.
    fn label_filter(&self) -> String {
        match self {
            HostScope::All => "label=weft.dev/project".to_string(),
            HostScope::Project(id) => format!("label=weft.dev/project={id}"),
        }
    }
}

/// Layer 1: host `weft-worker` tags outside the referenced set, within
/// the scope's label filter, through the one host matcher.
async fn host_worker_sweep(
    referenced: &crate::images::ReferencedImages,
    scope: &HostScope,
) -> anyhow::Result<()> {
    let repo = weft_compiler::build::WORKER_IMAGE_REPO;
    let stale = crate::images::host_images_matching(
        &host_image_listing(&["--filter", &scope.label_filter()]).await?,
        |r, t| r == repo && !referenced.is_referenced(r, t),
    );
    if stale.is_empty() {
        println!("no unreferenced worker images");
        return Ok(());
    }
    println!("{}", reclaim_host_images(&stale).await?.report("unreferenced worker image(s)"));
    Ok(())
}

/// Layer 2: dangling (untagged) leftovers from rebuilds under the same
/// tag, within the scope's label filter.
async fn dangling_prune(scope: &HostScope) -> anyhow::Result<()> {
    let status = crate::images::docker()
        .args(["image", "prune", "--force", "--filter", "dangling=true", "--filter"])
        .arg(scope.label_filter())
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker image prune exited {status}");
    }
    Ok(())
}

/// Layer 3: host `weft-infra-<name>:<hash>` images outside the
/// referenced set (see `clean_build_images` for why `--all`-only and
/// why the supervisor's repo is excluded).
async fn host_infra_sweep(referenced: &crate::images::ReferencedImages) -> anyhow::Result<()> {
    let stale = crate::images::host_images_matching(&host_image_listing(&[]).await?, |r, t| {
        crate::images::is_infra_node_repo(r) && !referenced.is_referenced(r, t)
    });
    if stale.is_empty() {
        println!("no stale infra images");
        return Ok(());
    }
    println!("{}", reclaim_host_images(&stale).await?.report("stale infra image(s)"));
    Ok(())
}

/// Layer 4: the kind node's own cached worker and infra images outside
/// the referenced set, computed from the node's OWN image list through
/// the one node matcher (per IMAGE, never per tag: a group mixing a
/// kept and a stale tag of the same content survives whole; digest-only
/// groups never match). Only worker and infra-node refs are ever
/// touched. Never a blanket `crictl rmi --prune`: "unused right now"
/// includes the system images (listener pods spawn on demand, so
/// between spawns nothing uses the listener image), and pruning those
/// leaves the next on-demand pod in ImagePullBackOff (the exact
/// incident that shaped this). Kind backend only: a k8s backend has no
/// local node cache to clean, so it skips quietly; on kind a failure
/// here is a real error (leftover node images are exactly what this
/// verb exists to reclaim).
async fn node_sweep(referenced: &crate::images::ReferencedImages) -> anyhow::Result<()> {
    let cfg = crate::commands::daemon::cluster_config();
    if cfg.backend != ClusterBackend::Kind {
        return Ok(());
    }
    let node_stale = crate::images::node_images_matching(
        &crate::images::kind_node_image_tag_groups(&cfg.cluster_name).await?,
        |r, t| {
            (r == weft_compiler::build::WORKER_IMAGE_REPO || crate::images::is_infra_node_repo(r))
                && !referenced.is_referenced(r, t)
        },
    );
    if node_stale.is_empty() {
        println!("no stale worker or infra images on the kind node");
        return Ok(());
    }
    let node = format!("{}-control-plane", cfg.cluster_name);
    println!(
        "{}",
        crictl_rmi_refs(&node, &node_stale)
            .await?
            .report(&format!("stale worker/infra image(s) from the {node} node"))
    );
    Ok(())
}

/// Layer 5: old builder bases. Each engine/toolchain bump mints a
/// fresh ~1.4GB `weft-builder-base:<hash>` and nothing evicts the
/// previous one implicitly (an implicit GC would race an in-flight
/// build FROMing it), so this explicit clean is where they go.
/// Everything except the CURRENT ref (the one the next build FROMs) is
/// dead. Host docker on any backend (a k8s backend still builds bases
/// here).
async fn builder_base_sweep() -> anyhow::Result<()> {
    let current_base = crate::images::builder_base_ref()?;
    let stale_bases = crate::images::host_images_matching(
        &host_image_listing(&[]).await?,
        crate::images::outside_current(&current_base)?,
    );
    if stale_bases.is_empty() {
        println!("no stale builder-base images");
        return Ok(());
    }
    println!(
        "{}",
        reclaim_host_images(&stale_bases).await?.report("stale builder-base image(s)")
    );
    Ok(())
}

/// Every host image (narrowed by `filters`, e.g. a label filter) as one
/// `repo:tag` line, the listing the host-side matcher reads.
async fn host_image_listing(filters: &[&str]) -> anyhow::Result<String> {
    let listing = crate::images::docker()
        .args(["images"])
        .args(filters)
        .args(["--format", "{{.Repository}}:{{.Tag}}"])
        .output()
        .await?;
    anyhow::ensure!(
        listing.status.success(),
        "docker images exited {}: {}",
        listing.status,
        String::from_utf8_lossy(&listing.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&listing.stdout).into_owned())
}

/// What one removal pass did with its condemned refs. `already_gone`
/// is a ref a concurrent clean reclaimed between our listing and the
/// delete; `in_use` is a ref the runtime refused to drop because a
/// container still runs it (kept; the next clean gets it).
#[derive(Debug, Default, PartialEq)]
struct Reclaimed {
    removed: usize,
    already_gone: usize,
    in_use: usize,
}

impl Reclaimed {
    /// The one report line every sweep prints, so the five report
    /// sites cannot drift in wording: "removed N <what>", then only the
    /// tails that happened.
    fn report(&self, what: &str) -> String {
        let mut line = format!("removed {} {what}", self.removed);
        if self.already_gone > 0 {
            line.push_str(&format!(", {} already reclaimed", self.already_gone));
        }
        if self.in_use > 0 {
            line.push_str(&format!(", {} still in use (kept)", self.in_use));
        }
        line
    }
}

/// Remove images on the kind node through `crictl rmi`, per-ref so one
/// refused tag cannot abort the others. A refusal is classified by
/// OBSERVING presence (`crictl inspecti`), never by parsing error
/// prose: still present means containerd refused because a pod runs
/// it (kept, with the refusal printed so a permission problem is not
/// invisible); absent means a concurrent cleaner already reclaimed it,
/// which only counts while the node's runtime itself still answers.
/// The liveness probe is `crictl info`, which CONTACTS containerd: a
/// wedged runtime inside a live node fails rmi and inspecti through
/// the socket but still answers `--version` (a client-only print), so
/// a version probe would read as "node up" and report a live image as
/// already reclaimed. A node failing all three through the same
/// transport must not read as success.
async fn crictl_rmi_refs(node: &str, images: &[String]) -> anyhow::Result<Reclaimed> {
    let mut done = Reclaimed::default();
    for image in images {
        let out = crate::images::docker()
            .args(["exec", node, "crictl", "rmi", image])
            .output()
            .await?;
        if out.status.success() {
            done.removed += 1;
            continue;
        }
        let present = crate::images::docker()
            .args(["exec", node, "crictl", "inspecti", image])
            .output()
            .await?
            .status
            .success();
        if present {
            println!(
                "kept {image} on {node}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            done.in_use += 1;
            continue;
        }
        let node_up = crate::images::docker()
            .args(["exec", node, "crictl", "info"])
            .output()
            .await?
            .status
            .success();
        if node_up {
            done.already_gone += 1;
            continue;
        }
        anyhow::bail!(
            "the {node} node stopped answering while removing {image}; check \
             the kind cluster and rerun `weft clean --images`"
        );
    }
    Ok(done)
}

/// Remove host docker images one by one, the host twin of
/// `crictl_rmi_refs`: a tag a concurrent clean already reclaimed is a
/// success, and a tag docker refuses to drop because a container still
/// uses it is kept, both detected by observing presence rather than
/// parsing error prose. No `-f`: force would untag a live container's
/// image anyway and strand its restart. "Absent" is only a verdict
/// while the daemon still answers.
async fn reclaim_host_images(images: &[String]) -> anyhow::Result<Reclaimed> {
    let mut done = Reclaimed::default();
    for image in images {
        let out = crate::images::docker().args(["rmi", image]).output().await?;
        if out.status.success() {
            done.removed += 1;
            continue;
        }
        let present = crate::images::docker()
            .args(["image", "inspect", image])
            .output()
            .await?
            .status
            .success();
        if present {
            println!("kept {image}: {}", String::from_utf8_lossy(&out.stderr).trim());
            done.in_use += 1;
            continue;
        }
        let daemon_up = crate::images::docker()
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .await?
            .status
            .success();
        if daemon_up {
            done.already_gone += 1;
            continue;
        }
        anyhow::bail!(
            "docker stopped answering while removing {image}; check the daemon \
             and rerun `weft clean --images`"
        );
    }
    Ok(done)
}

/// `docker buildx prune` reclaims BuildKit's intermediate layers.
/// This is the heavy reclaim: cargo deps, intermediate Rust compile
/// state, etc. The next build will re-download deps and re-link.
async fn clean_build_cache() -> anyhow::Result<()> {
    println!("pruning docker BuildKit cache (next build will be slower)…");
    let status = crate::images::docker()
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
    use super::Reclaimed;

    /// One report line per sweep: the count always, each tail only
    /// when it happened.
    #[test]
    fn reclaim_report_names_only_what_happened() {
        let plain = Reclaimed { removed: 2, already_gone: 0, in_use: 0 };
        assert_eq!(plain.report("stale infra image(s)"), "removed 2 stale infra image(s)");
        let busy = Reclaimed { removed: 1, already_gone: 1, in_use: 3 };
        assert_eq!(
            busy.report("stale worker image(s)"),
            "removed 1 stale worker image(s), 1 already reclaimed, 3 still in use (kept)"
        );
    }
}

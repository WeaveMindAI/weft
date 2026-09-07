//! `weft executions`, `weft events`, `weft clean`. Journal inspection
//! and cleanup. Graph view replay is an extension command; these are
//! the scripting surface.

use super::{local_time, Ctx};
use crate::commands::daemon::ClusterBackend;

/// One page of the dispatcher's execution listing. The body is
/// `{"executions": [...], "total": N}`; anything else is a broken
/// contract and fails loudly rather than reading as "no executions".
async fn executions_page(
    client: &crate::client::DispatcherClient,
    limit: u32,
    offset: u64,
    project: Option<&str>,
    phase: Option<&str>,
) -> anyhow::Result<(Vec<serde_json::Value>, u64)> {
    let mut path = format!("/executions?limit={limit}&offset={offset}");
    if let Some(p) = project {
        path.push_str(&format!("&project_id={p}"));
    }
    if let Some(p) = phase {
        path.push_str(&format!("&phase={p}"));
    }
    let resp: serde_json::Value = client.get_json(&path).await?;
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

pub async fn list(
    ctx: Ctx,
    limit: u32,
    project: Option<String>,
    phase: Option<weft_core::context::Phase>,
) -> anyhow::Result<()> {
    let client = ctx.client();
    let (arr, total) =
        executions_page(&client, limit, 0, project.as_deref(), phase.map(|p| p.as_str())).await?;
    if ctx.json() {
        println!("{}", serde_json::json!({ "executions": arr, "total": total }));
        return Ok(());
    }
    if arr.is_empty() {
        println!("(no executions)");
        return Ok(());
    }
    println!(
        "{:<36}  {:<9}  {:<13}  {:<19}  {:<36}  entry_node  tags",
        "color", "status", "phase", "started", "project_id"
    );
    for row in &arr {
        let color = row.get("color").and_then(|v| v.as_str()).unwrap_or("?");
        let project = row.get("project_id").and_then(|v| v.as_str()).unwrap_or("?");
        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let phase = row.get("phase").and_then(|v| v.as_str()).unwrap_or("?");
        let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
        let entry = row.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?");
        // The tags the run put on itself (`ctx.tag_execution`), the
        // handle a sibling's `ctx.stop_tagged` selects on.
        let tags: Vec<&str> = row
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|t| t.as_str()).collect())
            .unwrap_or_default();
        let tags = if tags.is_empty() { String::new() } else { format!("  {}", tags.join(",")) };
        println!(
            "{color:<36}  {status:<9}  {phase:<13}  {:<19}  {project:<36}  {entry}{tags}",
            local_time(started)
        );
    }
    // The server clamps the page size, so a big --limit can come back
    // short; say so rather than letting the page read as the total.
    // It does NOT say "raise --limit": past the server's cap that is
    // advice the CLI knows will not work.
    if (arr.len() as u64) < total {
        println!(
            "showing {} of {total} (one page; the dispatcher caps how many a page can hold, \
             so read the rest through the API)",
            arr.len()
        );
    }
    Ok(())
}

/// What `weft events` keeps and how much of each row it shows. The
/// default is the compact read a person or an agent can hold for a
/// long run: every row, values cut short. The filters narrow to one
/// node or one kind of event, and `full` opens the values.
#[derive(Debug, Default, Clone)]
pub struct EventsFilter {
    pub node: Option<String>,
    pub kind: Option<String>,
    pub full: bool,
}

impl EventsFilter {
    /// Whether one replay row survives the filters. A node filter is
    /// exact (a node's id), so a run-level row (a start, a completion,
    /// the run failing) never passes one: it names no node. A kind
    /// filter matches the kind exactly or as a substring, so `failed`
    /// finds both `node_failed` and `execution_failed`, and `loop`
    /// finds the loop lifecycle.
    pub fn keeps(&self, row: &serde_json::Value) -> bool {
        let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let kind_ok = self.kind.as_deref().is_none_or(|k| kind == k || kind.contains(k));
        let node_ok = self.node.as_deref().is_none_or(|n| row_node(row) == Some(n));
        kind_ok && node_ok
    }
}

/// How much of a value the compact line shows before `...`. Long
/// enough to recognise a value, short enough that a row of forty
/// nodes fits a screen.
const SUMMARY_CHARS: usize = 120;

/// The columns every line prints in its own place, plus the two every
/// row of one run repeats (they name the run, which you already have:
/// you asked for it by color). Nothing here reaches the generic tail.
const COLUMNS: &[&str] = &["kind", "node", "node_id", "at_unix", "color", "project_id"];

/// The node a replay row is about. Most rows name it `node`; the two
/// that come off a pulse rather than a journal row (`cost_reported`,
/// `bus_participant`) name it `node_id`. One reader, so the filter and
/// the printed column can never disagree about which rows have a node.
fn row_node(row: &serde_json::Value) -> Option<&str> {
    row.get("node")
        .or_else(|| row.get("node_id"))
        .and_then(|v| v.as_str())
}

/// One replay row as the line `weft events` prints: local time, the
/// kind, the node, then everything else the row carries, `key=value`,
/// in the row's own field order. The tail is generic on purpose: a
/// hand-picked key list silently swallows whatever a new event kind
/// carries (a cost's amount, a suspension's token, a loop's index),
/// and the one thing a reader wants from a row is exactly the field
/// that kind was added for. `full` prints values whole; otherwise
/// each is cut at `SUMMARY_CHARS`, on a character boundary.
pub fn event_line(row: &serde_json::Value, full: bool) -> String {
    let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
    // Every row projected from a journal row carries the journal's
    // `at_unix`; the derived rows (a bus participant sniffed off a
    // pulse, a corruption the replay found) have none and get a blank
    // of the same width, so the columns still line up.
    let at = match row.get("at_unix").and_then(|v| v.as_u64()) {
        Some(at) => format!("[{:<19}]", local_time(at)),
        None => " ".repeat(21),
    };
    let node = row_node(row).unwrap_or("");
    let mut line = format!("{at} {kind:<23} {node}");
    let Some(fields) = row.as_object() else {
        return line;
    };
    for (key, value) in fields {
        // An absent value and an empty one say the same nothing: a
        // `frames=[]` on every root-level firing is noise in the column
        // the reader is scanning.
        let empty = match value {
            serde_json::Value::Null => true,
            serde_json::Value::Array(items) => items.is_empty(),
            serde_json::Value::Object(map) => map.is_empty(),
            serde_json::Value::String(text) => text.is_empty(),
            _ => false,
        };
        if COLUMNS.contains(&key.as_str()) || empty {
            continue;
        }
        line.push_str(&format!("  {key}={}", field_text(value, full)));
    }
    line
}

/// One field of a replay row as the line shows it: a string bare (an
/// error message reads as itself, not as a quoted JSON string), a list
/// of strings joined, anything else as its JSON, and all of it cut to
/// `SUMMARY_CHARS` unless `full`.
fn field_text(value: &serde_json::Value, full: bool) -> String {
    let text = match value {
        serde_json::Value::String(text) => text.clone(),
        // A skip's reason is structured (`{"kind": "did_not_flow"}`);
        // its kind is the readable part, and the rest is machinery.
        serde_json::Value::Object(map) if map.len() == 1 => match map.get("kind") {
            Some(serde_json::Value::String(kind)) => kind.clone(),
            _ => serde_json::to_string(value).unwrap_or_default(),
        },
        serde_json::Value::Array(items) if items.iter().all(|i| i.is_string()) => items
            .iter()
            .filter_map(|i| i.as_str())
            .collect::<Vec<_>>()
            .join(","),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    if full || text.chars().count() <= SUMMARY_CHARS {
        return text;
    }
    let cut: String = text.chars().take(SUMMARY_CHARS.saturating_sub(3)).collect();
    format!("{cut}...")
}

pub async fn events(ctx: Ctx, color: String, filter: EventsFilter) -> anyhow::Result<()> {
    let color = super::resolve_color(&ctx, &color).await?;
    let client = ctx.client();
    let resp: serde_json::Value = client
        .get_json(&format!("/executions/{color}/replay"))
        .await?;
    let arr = resp
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("/executions/{color}/replay returned no array: {resp}"))?;
    let kept: Vec<&serde_json::Value> = arr.iter().filter(|row| filter.keeps(row)).collect();
    if ctx.json() {
        println!("{}", serde_json::Value::Array(kept.into_iter().cloned().collect()));
        return Ok(());
    }
    if kept.is_empty() {
        println!(
            "(no events{})",
            if arr.is_empty() { String::new() } else { format!(" match; the run has {}", arr.len()) }
        );
        return Ok(());
    }
    for row in kept {
        println!("{}", event_line(row, filter.full));
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
    yes: bool,
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

    // A journal row deleted is gone for good, so every execution
    // deletion is confirmed: a terminal is asked, a script says
    // `--yes`. The sweep says how many rows it is about to take.
    let confirm = |what: String| -> anyhow::Result<bool> {
        if yes {
            return Ok(true);
        }
        println!("About to delete {what}.");
        let ok = crate::prompt::confirm("Type 'yes' to confirm: ", "--yes")?;
        if !ok {
            println!("aborted");
        }
        Ok(ok)
    };

    let client = ctx.client();
    if let Some(c) = color {
        anyhow::ensure!(
            project.is_none(),
            "a color names ONE execution, so --project cannot narrow it further: \
             drop one of them"
        );
        let c = super::resolve_color(&ctx, &c).await?;
        if !confirm(format!("execution {c}"))? {
            return Ok(());
        }
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
    let scope = match &project {
        Some(p) => format!(" of project {p}"),
        None => String::new(),
    };
    let subject = match days {
        Some(d) => format!("every execution{scope} older than {d} days"),
        None => format!("every execution{scope}"),
    };
    if !confirm(subject)? {
        return Ok(());
    }
    let mut count = 0usize;
    loop {
        let mut offset = 0u64;
        let mut deleted_this_pass = 0usize;
        loop {
            let (rows, total) =
                executions_page(&client, 200, offset, project.as_deref(), None).await?;
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
    use super::{event_line, EventsFilter, Reclaimed};
    use serde_json::json;

    /// The kind filter matches exactly or by substring, the node filter
    /// exactly, and a run-level row (no node) never passes a node filter.
    #[test]
    fn events_filter_narrows_by_node_and_kind() {
        let failed = json!({"kind": "node_failed", "node": "llm"});
        let done = json!({"kind": "node_completed", "node": "reply"});
        let run_failed = json!({"kind": "execution_failed"});
        let all = EventsFilter::default();
        assert!(all.keeps(&failed) && all.keeps(&done) && all.keeps(&run_failed));
        let by_kind = EventsFilter { kind: Some("failed".into()), ..Default::default() };
        assert!(by_kind.keeps(&failed) && by_kind.keeps(&run_failed) && !by_kind.keeps(&done));
        let exact = EventsFilter { kind: Some("node_completed".into()), ..Default::default() };
        assert!(exact.keeps(&done) && !exact.keeps(&failed));
        let by_node = EventsFilter { node: Some("llm".into()), ..Default::default() };
        assert!(by_node.keeps(&failed) && !by_node.keeps(&done) && !by_node.keeps(&run_failed));
    }

    /// The compact line cuts a long value at the summary width on a
    /// character boundary; `full` prints it whole; and everything the
    /// row carries reaches the line, including the fields only one
    /// kind has.
    #[test]
    fn event_line_summarises_and_expands() {
        let long: String = "é".repeat(300);
        let row = json!({
            "kind": "node_completed",
            "node": "llm",
            "at_unix": 1_756_838_207u64,
            "output": {"text": long},
        });
        let compact = event_line(&row, false);
        assert!(compact.contains(" node_completed ") && compact.contains(" llm"), "{compact}");
        assert!(compact.ends_with("..."), "{compact}");
        assert!(compact.chars().count() < 200, "{}", compact.chars().count());
        let full = event_line(&row, true);
        assert!(full.contains(&long), "full keeps the whole value");

        let cancelled = json!({
            "kind": "execution_cancelled",
            "reason": "stopped by sibling",
            "tags": ["user:1"],
            "at_unix": 1_756_838_207u64,
        });
        let line = event_line(&cancelled, false);
        assert!(line.contains("reason=stopped by sibling") && line.contains("tags=user:1"), "{line}");
        let skipped = json!({
            "kind": "node_skipped",
            "node": "send",
            "reason": {"kind": "did_not_flow"},
            "closed_ports": ["ok"],
            "at_unix": 5u64,
        });
        let line = event_line(&skipped, false);
        assert!(line.contains("reason=did_not_flow") && line.contains("closed_ports=ok"), "{line}");
        // A derived row with no stamp keeps the columns aligned, and
        // the row that names its node `node_id` still shows it.
        let derived = json!({"kind": "journal_corruption", "reason": "bad row"});
        assert!(event_line(&derived, false).starts_with(&" ".repeat(21)));
        let cost = json!({
            "kind": "cost_reported",
            "node_id": "llm",
            "project_id": "e8969195-52aa-42e0-8a9b-f9577ddd8bed",
            "frames": [],
            "service": "openai",
            "amount_usd": 0.0123,
            "at_unix": 5u64,
        });
        let line = event_line(&cost, false);
        assert!(line.contains(" llm") && line.contains("service=openai") && line.contains("amount_usd=0.0123"), "{line}");
        assert!(!line.contains("project_id") && !line.contains("frames"), "the run's own name and an empty frame stack are not news: {line}");
        // The fields the old hand-picked key list dropped.
        let suspended = json!({"kind": "node_suspended", "node": "ask", "token": "a44e5cad", "at_unix": 5u64});
        assert!(event_line(&suspended, false).contains("token=a44e5cad"));
        let iteration = json!({"kind": "loop_iteration_launched", "node": "run__in", "index": 7, "at_unix": 5u64});
        assert!(event_line(&iteration, false).contains("index=7"));
    }

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

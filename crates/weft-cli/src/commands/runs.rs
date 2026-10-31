//! `weft runs`, `weft events`, `weft clean`. Journal inspection
//! and cleanup. Graph view replay is an extension command; these are
//! the scripting surface.

use anyhow::Context;
use weft_core::program::RunPage;

use super::{utc_time, Ctx};

/// A value put into a query string. A node id is the author's own
/// spelling, so it can hold anything they typed; only the handful of
/// characters that would end the value or start another parameter have
/// to move, and everything else stays readable in a log line.
pub(crate) fn query_escaped(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '&' => "%26".to_string(),
            '=' => "%3D".to_string(),
            '#' => "%23".to_string(),
            '+' => "%2B".to_string(),
            '%' => "%25".to_string(),
            ' ' => "%20".to_string(),
            other => other.to_string(),
        })
        .collect()
}

/// One page of the dispatcher's run listing. Anything that does
/// not read as one is a broken contract and fails loudly rather than
/// reading as "no runs".
async fn runs_page(
    client: &crate::client::DispatcherClient,
    filter: &ListFilter,
) -> anyhow::Result<RunPage> {
    let mut path =
        format!("/runs?limit={}&offset={}", filter.limit, filter.offset);
    if let Some(p) = &filter.project {
        path.push_str(&format!("&project_id={p}"));
    }
    if let Some(p) = filter.phase {
        path.push_str(&format!("&phase={}", p.as_str()));
    }
    if let Some(node) = &filter.node {
        path.push_str(&format!("&entry_node={}", query_escaped(node)));
    }
    if let Some(since) = filter.since {
        path.push_str(&format!("&started_after={since}"));
    }
    if let Some(status) = &filter.status {
        path.push_str(&format!("&status={status}"));
    }
    if let Some(instance) = &filter.instance {
        path.push_str(&format!("&instance={instance}"));
    }
    if let Some(tag) = &filter.tag {
        path.push_str(&format!("&tag={}", query_escaped(tag)));
    }
    if let Some(through) = &filter.through {
        path.push_str(&format!("&node={}", query_escaped(through)));
    }
    if let Some(search) = &filter.search {
        path.push_str(&format!("&search={}", query_escaped(search)));
    }
    serde_json::from_value(client.get_json(&path).await?).context("read the runs listing")
}

/// What `weft runs` narrows the listing to. One struct rather
/// than six positional arguments, so adding the next filter does not
/// re-thread every call site.
#[derive(Debug, Clone)]
pub struct ListFilter {
    pub limit: u32,
    pub offset: u32,
    pub project: Option<String>,
    pub phase: Option<weft_core::context::Phase>,
    /// The node whose firing started the run.
    pub node: Option<String>,
    /// Unix second: only runs that started at or after it.
    pub since: Option<u64>,
    /// Where the run stands.
    pub status: Option<weft_core::program::RunStatus>,
    /// Which instance the run is in.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// A tag the run carries.
    pub tag: Option<String>,
    /// A node that fired in the run.
    pub through: Option<String>,
    /// Words the finished run carried among what it recorded.
    pub search: Option<String>,
}

pub async fn list(ctx: Ctx, filter: ListFilter) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let page = runs_page(&client, &filter).await?;
    if ctx.json_out(&serde_json::to_value(&page)?)? {
        return Ok(());
    }
    if page.runs.is_empty() {
        println!("(no runs)");
        return Ok(());
    }
    println!(
        "{:<36}  {:<9}  {:<13}  {:<23}  {:<36}  entryNode  tags",
        "runId", "status", "phase", "started", "projectId"
    );
    for row in &page.runs {
        let run_id = row.run_id.to_string();
        let project = row.project_id.to_string();
        let (status, phase, entry) = (&row.status, row.phase.as_str(), &row.entry_node);
        // The tags the run put on itself (`ctx.tag_run`), the
        // handle a sibling's `ctx.stop_tagged` selects on.
        let tags = if row.tags.is_empty() { String::new() } else { format!("  {}", row.tags.join(",")) };
        // Which instance the run is in, when it is in one.
        let instance = row.instance.as_ref().map(|m| format!("  (instance {m})")).unwrap_or_default();
        println!(
            "{run_id:<36}  {status:<9}  {phase:<13}  {:<23}  {project:<36}  {entry}{tags}{instance}",
            utc_time(row.started_at)
        );
    }
    // The server clamps the page size, so a big --limit can come back
    // short; say so rather than letting the page read as the total.
    // It does NOT say "raise --limit": past the server's cap that is
    // advice the CLI knows will not work.
    let shown = page.runs.len() as u64;
    if shown < page.total {
        println!(
            "showing {shown} of {} (one page; the dispatcher caps how many a page can hold, \
             so walk the rest with --offset {}, or narrow with --node / --since)",
            page.total,
            filter.offset as u64 + shown
        );
    }
    Ok(())
}

/// What `weft events` asks the dispatcher to keep, as typed: the
/// dispatcher reads each against the program the run ran
/// (`weft_core::live_event::spelled::EventsFilter`).
#[derive(Debug, Default, Clone)]
pub struct EventsQuery {
    /// A node, group or loop, spelled the way that program reads.
    pub node: Option<String>,
    /// A kind, matched exactly or as a substring.
    pub kind: Option<String>,
    /// Loop iterations, outermost first, joined by `.` (`3.0`).
    pub iteration: Option<String>,
}

/// How much of a value the compact line shows before `...`. Long
/// enough to recognise a value, short enough that a row of forty
/// nodes fits a screen.
const SUMMARY_CHARS: usize = 120;

/// The columns every line prints in its own place, plus the two every
/// row of one run repeats (they name the run, which you already have:
/// you asked for it by run). Nothing here reaches the generic tail.
const COLUMNS: &[&str] = &["kind", "node", "nodeId", "atUnix", "runId", "projectId"];

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
    // `atUnix`; the derived rows (a bus participant sniffed off a
    // pulse, a corruption the replay found) have none and get a blank
    // of the same width, so the columns still line up.
    let at = match row.get("atUnix").and_then(|v| v.as_u64()) {
        Some(at) => format!("[{:<23}]", utc_time(at)),
        None => " ".repeat(25),
    };
    let node = weft_core::live_event::spelled::row_node(row).unwrap_or("");
    let mut line = format!("{at} {kind:<23} {node}");
    if silent_completion(row) {
        // An empty output is dropped by the tail below, exactly like
        // every other empty field, which left this line identical to a
        // node that emitted plenty. It is not: every port closed, so
        // everything downstream of it skipped, and whoever is reading
        // this is usually reading it because a value never arrived.
        line.push_str("  output=(nothing emitted)");
    }
    if let Some((dropped, why)) = undelivered(row) {
        line.push_str(&format!("  dropped={dropped} ({why})"));
    }
    for (key, value) in printed_fields(row) {
        line.push_str(&format!("  {key}={}", field_text(value, full)));
    }
    line
}

/// A firing that completed without emitting on any port. The run is not
/// wrong (a node may decide it has nothing to say) but the consequence
/// is total: every output closes and the whole branch behind it skips.
/// The row carries `output` as null or an empty object depending on the
/// path that built it, and both mean the same nothing here.
fn silent_completion(row: &serde_json::Value) -> bool {
    if row.get("kind").and_then(|v| v.as_str()) != Some("node_completed") {
        return false;
    }
    match row.get("output") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Object(map)) => map.is_empty(),
        _ => false,
    }
}

/// The rows that can name the program's messages the caller never
/// received, each with why, as its line says it: a window (the caller
/// read too slowly) and the row saying the caller left (they were still
/// waiting to go out).
const UNDELIVERED: &[(&str, &str)] = &[
    ("caller_window", "the caller read too slowly and never received these messages"),
    ("caller_disconnected", "still waiting to go out when the exchange ended; the caller never received these messages"),
];

/// The offsets a caller row names as never received by the caller,
/// joined, with why, or `None` when it names none. The bare list would
/// print as `dropped=[12,13]` and leave the reader guessing what was
/// dropped, on the line they are reading because their client missed
/// part of a stream; the offsets are the `offset` of the program's
/// messages, in an earlier window or this one.
fn undelivered(row: &serde_json::Value) -> Option<(String, &'static str)> {
    let kind = row.get("kind").and_then(|v| v.as_str())?;
    let (_, why) = UNDELIVERED.iter().find(|(k, _)| *k == kind)?;
    let offsets: Vec<String> = row.get("dropped")?.as_array()?.iter().map(|o| o.to_string()).collect();
    (!offsets.is_empty()).then(|| (offsets.join(","), *why))
}

/// The fields one row shows in its generic tail: everything it carries
/// except the columns printed in their own place, and except the ones
/// saying nothing (an absent value and an empty one say the same
/// nothing, and a `frames=[]` on every root-level firing is noise in
/// the column the reader is scanning).
///
/// One reader, so what a line prints and what counts as cut can never
/// disagree.
fn printed_fields(row: &serde_json::Value) -> impl Iterator<Item = (&String, &serde_json::Value)> {
    row.as_object().into_iter().flatten().filter(|(key, value)| {
        let empty = match value {
            serde_json::Value::Null => true,
            serde_json::Value::Array(items) => items.is_empty(),
            serde_json::Value::Object(map) => map.is_empty(),
            serde_json::Value::String(text) => text.is_empty(),
            _ => false,
        };
        // A caller window's drops are said in their own words
        // (`undelivered`).
        let said = key.as_str() == "dropped" && undelivered(row).is_some();
        !COLUMNS.contains(&key.as_str()) && !empty && !said
    })
}

/// Whether the compact line for this row had to cut a value short.
///
/// The cut is silent, and the value it cuts is often the exact thing
/// being chased (a reply body, a model's answer). So the reader is told
/// `--full` exists at the one moment it would help, instead of
/// abandoning the journal for `curl`.
pub fn any_value_cut(row: &serde_json::Value) -> bool {
    printed_fields(row).any(|(_, value)| field_text(value, true).chars().count() > SUMMARY_CHARS)
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

pub async fn events(ctx: Ctx, run_id: String, query: EventsQuery, full: bool) -> anyhow::Result<()> {
    let run_id = super::resolve_run_id(&ctx, &run_id).await?;
    // The dispatcher keeps and spells the rows against the program the run
    // ran: this folder may hold another version of it by now, or another
    // project, so nothing here is read through the folder's program, and
    // a name it does not take is refused there, with the name it would.
    let filters: Vec<String> = [("node", &query.node), ("kind", &query.kind), ("iteration", &query.iteration)]
        .into_iter()
        .filter_map(|(key, value)| value.as_deref().map(|value| format!("{key}={}", query_escaped(value))))
        .collect();
    let path = match filters.as_slice() {
        [] => format!("/runs/{run_id}/events"),
        _ => format!("/runs/{run_id}/events?{}", filters.join("&")),
    };
    let answer: weft_core::program::RunEvents =
        serde_json::from_value(ctx.client()?.get_json(&path).await?).context("read the run's events")?;
    if let Some(why) = &answer.unspelled {
        eprintln!("warning: nodes show as ids, because {why}");
    }
    let kept = &answer.events;
    if ctx.json_out(kept)? {
        return Ok(());
    }
    if kept.is_empty() {
        println!(
            "(no events{})",
            if answer.total == 0 { String::new() } else { format!(" match; the run has {}", answer.total) }
        );
        return Ok(());
    }
    for row in kept {
        println!("{}", event_line(row, full));
    }
    if !full && kept.iter().any(any_value_cut) {
        println!("(some values were cut to fit; `--full` prints them whole, `--json` for a tool)");
    }
    Ok(())
}

/// What narrows a bulk clean beyond project and age: the same filters a
/// program's `ctx.runs()` takes (`weft_core::program::RunFilter`).
#[derive(Debug, Default)]
pub struct CleanNarrowing {
    pub instance: Option<weft_core::instance::InstanceId>,
    pub status: Option<weft_core::program::RunStatus>,
    pub node: Option<String>,
    pub tag: Option<String>,
    /// Cancel matching runs still going, instead of leaving them.
    pub cancel_running: bool,
}

pub async fn clean(
    ctx: Ctx,
    run_id: Option<String>,
    keep_days: Option<u32>,
    all: bool,
    images: bool,
    build_cache: bool,
    project: Option<String>,
    narrow: CleanNarrowing,
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

    // A journal row deleted is gone for good, so every run
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

    let client = ctx.client()?;
    let project = project
        .map(|p| p.parse::<uuid::Uuid>().map_err(|_| anyhow::anyhow!("--project takes a project id, and '{p}' is not one")))
        .transpose()?;
    if let Some(c) = run_id {
        anyhow::ensure!(
            project.is_none(),
            "a run names ONE run, so --project cannot narrow it further: \
             drop one of them"
        );
        let c = super::resolve_run_id(&ctx, &c).await?;
        if !confirm(format!("run {c}"))? {
            return Ok(());
        }
        let deleted: weft_core::program::DeletedRun =
            serde_json::from_value(client.delete_json(&format!("/runs/{c}")).await?)
                .context("read what the delete removed")?;
        println!("deleted {c}");
        // The sweep of whatever version this run left bare happens on the
        // dispatcher, inside the delete: the run's own project is the one
        // to sweep, and a run can be cleaned from anywhere, so no
        // client is in a position to know it. This just reports it.
        let swept = deleted.swept.len();
        if swept > 0 {
            println!("dropped {swept} bare versions");
        }
        return Ok(());
    }

    // Bulk clean. Naming a SUBJECT means you mean all of it (a run
    // deletes outright; a project, an instance or a tag takes every run it
    // names), so the 30-day default guards only the sweep that names
    // nothing. `--keep-days` still narrows any of them when asked for.
    let named = project.is_some() || narrow.instance.is_some() || narrow.tag.is_some();
    let days = match (keep_days, all, named) {
        (Some(d), _, _) => Some(d),
        (None, true, _) => None,  // --all: no cutoff
        (None, false, true) => None,  // a named subject: all of its runs
        (None, false, false) => Some(30),  // the unnamed sweep's guard
    };
    let mut scope = match &project {
        Some(p) => format!(" of project {p}"),
        None => String::new(),
    };
    if let Some(instance) = &narrow.instance {
        scope.push_str(&format!(" in instance {instance}"));
    }
    if let Some(tag) = &narrow.tag {
        scope.push_str(&format!(" tagged {tag}"));
    }
    if let Some(status) = &narrow.status {
        scope.push_str(&format!(" {status}"));
    }
    if let Some(node) = &narrow.node {
        scope.push_str(&format!(" started by {node}"));
    }
    let subject = match days {
        Some(d) => format!("every run{scope} older than {d} days"),
        None => format!("every run{scope}"),
    };
    if !confirm(subject)? {
        return Ok(());
    }
    let filter = weft_core::program::RunFilter {
        instance: narrow.instance,
        status: narrow.status,
        node: narrow.node,
        tag: narrow.tag,
        older_than_secs: days.map(|d| d as u64 * 24 * 3600),
    };
    // The dispatcher deletes what the filter reaches and sweeps the
    // versions the deletes left bare, project by project; a run still
    // going follows `--cancel-running` (stopped now, its rows gone with
    // the next clean) or is left to finish.
    let body = weft_core::program::CleanRequest {
        project,
        filter,
        running: if narrow.cancel_running { weft_core::program::StillGoing::Cancel } else { weft_core::program::StillGoing::Leave },
    };
    let answer: weft_core::program::CleanOutcome = serde_json::from_value(client.post_json("/runs/clean", &serde_json::to_value(&body)?).await?)
        .map_err(|e| anyhow::anyhow!("unexpected /runs/clean answer: {e}"))?;
    if ctx.json_out(&serde_json::to_value(&answer)?)? {
        return Ok(());
    }
    match days {
        Some(d) => println!("deleted {} runs{scope} older than {d}d", answer.deleted),
        None => println!("deleted {} runs{scope}", answer.deleted),
    }
    if answer.cancelled > 0 {
        println!("cancelled {} still running (their rows go with the next clean)", answer.cancelled);
    }
    if answer.left_running > 0 {
        println!("left {} still running (--cancel-running stops them)", answer.left_running);
    }
    Ok(())
}


/// Reclaim what builds produce that nothing runs any more (workers and
/// infra images, and under `--all` old runtimes, worker programs, shared
/// crates and what an older weft left). Each layer is its own sweep, none
/// gating another (a layer that fails is reported at the end, after every
/// other layer ran):
///
///   1. What the install built: it deletes each worker and infra image
///      its keep-set does not cover (`POST /images/prune`), the cwd
///      project's or, with `--all`, every project's. The install is the
///      only one that deletes them: every build claims the images it
///      relies on until they are registered, so a prune never takes an
///      image a build just found or made (it reports it in use), and it
///      forgets what it deleted from its ledger.
///   2. Dangling (untagged) leftovers of the node-test images an older
///      weft built (`weft test-node` stamped them with the
///      `weft.dev/project` label): those of the cwd project, or every one
///      with `--all`.
///   3. With `--all` only: the shared images no install on this machine
///      uses (runtimes, and what an older weft left: builder bases,
///      node-test images, registry-named standard worker images): each
///      engine change mints fresh ones, and every install on the machine
///      may run a different version ([`crate::image_refs`]).
///   4. With `--all` only: the worker programs no install uses and the
///      shared build crates no build staged lately, then the worker image
///      compile caches no install uses (an older weft's, or a `weft
///      build-images` run's).
///   5. With `--all` only: the caches that compiled weft's own crates for
///      an install no longer on this machine.
///   6. With `--all` only: the images an install no longer on this
///      machine held under its own names.
async fn clean_build_images(ctx: &Ctx, all: bool) -> anyhow::Result<()> {
    // The scope, settled BEFORE any layer runs: a scoped clean that cannot
    // name its project must delete nothing, not run the global layers and
    // report the error afterwards.
    let scope = if all {
        println!("reclaiming the images no live project references");
        Scope::All
    } else {
        let project = ctx.project().map_err(|e| {
            anyhow::anyhow!("{e}; pass --all to clean every project's images")
        })?;
        println!(
            "reclaiming the images of project {} ({}) nothing references",
            project.manifest.package.name,
            project.id()
        );
        Scope::Project(project.id())
    };

    let mut failures: Vec<anyhow::Error> = Vec::new();
    let mut layer = |name: &str, result: anyhow::Result<()>| {
        if let Err(e) = result {
            failures.push(e.context(name.to_string()));
        }
    };
    layer("built images", registry_prune(ctx, &scope).await);
    layer("dangling node-test leftovers", dangling_prune(&scope).await);
    if all {
        let data = super::daemon::data_dir();
        for kind in SharedImageKind::ALL {
            layer(kind.layer(), unused_image_sweep(&data, kind).await);
        }
        layer("unused worker programs and shared build crates", unused_worker_files(&weft_platform_local::MachineLayout::new(&data)));
        layer("unused compile caches", unused_compile_cache_sweep(&data).await);
        layer("compile caches of removed installs", removed_install_cache_sweep(&data).await);
        layer("images of removed installs", removed_install_image_sweep(&data).await);
    }
    // The compile caches every build shares are not images, but they are
    // disk, so they are never invisible. Reading their size is not a
    // reclaim: a failure to read it is said, never counted against the
    // sweep.
    if let Err(error) = report_compile_cache_size().await {
        eprintln!("could not read the compile caches' size: {error:#}");
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

/// Layer 1: the install deletes the images it built that nothing references.
async fn registry_prune(ctx: &Ctx, scope: &Scope) -> anyhow::Result<()> {
    let request = weft_core::images::PruneRequest {
        project: match scope {
            Scope::All => None,
            Scope::Project(id) => Some(*id),
        },
    };
    let body = serde_json::to_value(&request).context("encode the prune request")?;
    let report: weft_core::images::PruneReport = serde_json::from_value(ctx.client()?.post_json("/images/prune", &body).await?)
        .context("read the install's prune report")?;
    for image in &report.removed {
        println!("  removed {image}");
    }
    for image in &report.in_use {
        println!("  kept {image}: a container still runs from it, or a build is about to register it; a later clean takes it");
    }
    if report.removed.is_empty() && report.in_use.is_empty() && report.failed.is_empty() {
        println!("no unreferenced built images");
    }
    anyhow::ensure!(
        report.failed.is_empty(),
        "{} image(s) could not be deleted:\n  {}",
        report.failed.len(),
        report.failed.iter().map(|(image, why)| format!("{image}: {why}")).collect::<Vec<_>>().join("\n  ")
    );
    Ok(())
}

/// Every project's images, or one project's.
enum Scope {
    All,
    Project(uuid::Uuid),
}

/// Layer 2: dangling (untagged) leftovers of the node-test images an
/// older weft's `weft test-node` built on this machine, through the
/// `weft.dev/project` label it stamped on them. This weft stamps nothing:
/// a live test's server is a program.
async fn dangling_prune(scope: &Scope) -> anyhow::Result<()> {
    let filter = match scope {
        Scope::All => "label=weft.dev/project".to_string(),
        Scope::Project(id) => format!("label=weft.dev/project={id}"),
    };
    let status = crate::images::docker()
        .args(["image", "prune", "--force", "--filter", "dangling=true", "--filter"])
        .arg(filter)
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker image prune exited {status}");
    }
    Ok(())
}

/// `weft clean --images --build-plane`: the worker programs and shared
/// crates nothing uses, and the builder bases and node-test images an
/// older weft left that no install on this machine uses: the part of
/// layers 3 and 4 that needs no running install (setup.sh runs it after
/// every start, so an engine change leaves nothing of the version it
/// replaced). Fails only when docker cannot be asked.
pub async fn clean_build_plane() -> anyhow::Result<()> {
    let data = super::daemon::data_dir();
    for kind in SharedImageKind::BUILD_PLANE {
        unused_image_sweep(&data, kind).await?;
    }
    unused_worker_files(&weft_platform_local::MachineLayout::new(&data))
}

/// The files host builds leave on the machine that nothing uses any more:
/// the worker programs no install holds or names as its standard worker
/// (the standard worker of a weft an update replaced, a removed install's
/// programs), and the shared crates no program left on the machine was
/// built from (the sources of a package since edited, the workspace of a
/// weft since updated). Needs no running install.
fn unused_worker_files(machine: &weft_platform_local::MachineLayout) -> anyhow::Result<()> {
    let installs: Vec<String> = super::daemon::Install::every_on(machine.root())?.iter().map(|i| i.id.label_value().to_string()).collect();
    let workers = weft_platform_local::images::reclaim_workers(&machine.workers(), &installs)?;
    if workers.is_empty() {
        println!("no unused worker programs");
    } else {
        println!("removed {} unused worker program(s)", workers.len());
    }
    match weft_platform_local::host_build::reclaim_shared_crates(machine)? {
        None => println!("a build is staging its shared crates; they are left for the next clean"),
        Some(crates) if crates.is_empty() => println!("no shared build crates unused"),
        Some(crates) => println!("removed {} shared build crate(s) no program on this machine uses", crates.len()),
    }
    Ok(())
}

/// The kinds of shared image a machine-wide sweep reclaims, by the refs
/// each install records ([`crate::image_refs`]: this weft's start writes
/// its runtime; an older weft's start and builds also record the builder
/// bases and standard worker images it uses). Nothing evicts the previous
/// one implicitly (an implicit sweep would race a build FROMing it or a
/// unit agent running it), so an explicit clean is where they go.
/// Whatever no install's record names is dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedImageKind {
    /// `weft-builder-base:<hash>`, about 1.4GB each: an older weft's
    /// workers compiled on it, and a `weft build-images` run builds it.
    BuilderBase,
    /// `weft-node-tests:<hash>`, an older weft's, kept by the builder
    /// base they were built on.
    NodeTests,
    Runtime,
    /// The registry-named standard worker images an older weft or a
    /// `weft build-images` run left; an install's own name for one is
    /// reclaimed by that install's start.
    StandardWorker,
}

impl SharedImageKind {
    /// Every kind, the order `--all` sweeps them in.
    const ALL: [Self; 4] = [Self::BuilderBase, Self::NodeTests, Self::Runtime, Self::StandardWorker];
    /// The kinds `--build-plane` sweeps.
    const BUILD_PLANE: [Self; 2] = [Self::BuilderBase, Self::NodeTests];

    /// What a person calls one of them.
    fn what(self) -> &'static str {
        match self {
            Self::BuilderBase => "builder-base image",
            Self::NodeTests => "node-test image",
            Self::Runtime => "runtime image",
            Self::StandardWorker => "standard worker image",
        }
    }

    /// The layer's name in a failure report.
    fn layer(self) -> &'static str {
        match self {
            Self::BuilderBase => "builder-base images",
            Self::NodeTests => "node-test images",
            Self::Runtime => "runtime images",
            Self::StandardWorker => "standard worker images",
        }
    }

    /// The refs of an install's record that keep images of this kind.
    fn recorded(self) -> fn(&weft_core::images::ImageRefs) -> Vec<&str> {
        match self {
            Self::BuilderBase | Self::NodeTests => |r| r.builder_bases.iter().map(String::as_str).collect(),
            Self::Runtime => |r| vec![r.runtime.as_str()],
            Self::StandardWorker => |r| vec![r.standard_worker.as_str()],
        }
    }
}

/// Remove the images of `kind` no install in `records` uses, or nothing
/// at all while one has not recorded what it uses (saying which).
async fn unused_image_sweep(data_dir: &std::path::Path, kind: SharedImageKind) -> anyhow::Result<()> {
    // Held from the look at the records through the removal it decides
    // (`crate::image_refs::Records`).
    let records = crate::image_refs::every_install(data_dir).await?;
    let kept = match crate::image_refs::kept(&records.installs, kind.recorded(), kind.what()) {
        crate::image_refs::Kept::Unknown(why) => {
            println!("{why}");
            return Ok(());
        }
        crate::image_refs::Kept::Only(kept) => kept,
    };
    let listing = crate::images::host_image_listing(&[]).await?;
    let kept_refs: Vec<&str> = kept.iter().map(String::as_str).collect();
    let stale = match kind {
        SharedImageKind::BuilderBase => crate::images::host_images_matching(
            &listing,
            crate::images::outside_kept(weft_compiler::worker_image::BUILDER_BASE_REPO, &kept_refs)?,
        ),
        SharedImageKind::Runtime => crate::images::host_images_matching(&listing, crate::images::outside_kept(crate::images::RUNTIME_REPO, &kept_refs)?),
        SharedImageKind::StandardWorker => crate::images::stale_standard_workers(&listing, kept_refs)?,
        SharedImageKind::NodeTests => {
            let mut built_on_kept = String::new();
            for base in kept_refs {
                let (_, tag) = crate::images::ref_repo_tag(base)?;
                built_on_kept.push_str(&crate::images::host_image_listing(&[format!("label={}={tag}", crate::images::NODE_TEST_BASE_LABEL)]).await?);
            }
            crate::images::stale_node_test_images(&listing, &built_on_kept)
        }
    };
    if stale.is_empty() {
        println!("no unused {}s", kind.what());
        return Ok(());
    }
    println!("{}", crate::images::reclaim_host_images(&stale).await?.report(&format!("unused {}(s)", kind.what())));
    Ok(())
}

/// What the compile caches on this machine take on disk and how to drop
/// each: the machine's worker compile caches every host build compiles in
/// (every lane, its caches for nodes' `[build.env]` values, and the seed)
/// with the crates they share, the node-test
/// cache `weft test-node` builds into, and the BuildKit worker compile
/// caches only an older weft or a `weft build-images` run leaves.
async fn report_compile_cache_size() -> anyhow::Result<()> {
    let machine = weft_platform_local::MachineLayout::here();
    let gb = |bytes: u64| bytes as f64 / 1024.0 / 1024.0 / 1024.0;
    let lanes = dir_size(&machine.compile_caches())?;
    let crates = dir_size(&machine.shared_crates())?;
    println!(
        "worker compile caches: {:.1}GB in {} (every lane, its caches for nodes' [build.env] values, and the seed), and {:.1}GB \
         of shared crates in {}; a build trims the shared crates, the cache it compiled in and the seed to what a program on \
         this machine was built from, `weft clean --images --all` drops every cache kept for [build.env] values that no build \
         holds, `weft clean --build-cache` drops everything now",
        gb(lanes),
        machine.compile_caches().display(),
        gb(crates),
        machine.shared_crates().display(),
    );
    match super::test_node::cache_size_bytes()? {
        None => println!("node-test cache: nothing built on this host"),
        Some(bytes) => println!(
            "node-test cache: {:.1}GB; it wipes itself past WEFT_TEST_CACHE_CAP_GB (6 by default), \
             `weft rm --local` drops one project's slice, `weft clean --build-cache` drops it whole",
            gb(bytes)
        ),
    }
    let caches = crate::images::worker_compile_caches().await?;
    if caches.is_empty() {
        return Ok(());
    }
    let sizes: Vec<String> = caches.iter().map(|cache| {
        let mut line = if cache.idle_days == 0 { cache.size.clone() } else { format!("{} (unused for {} days)", cache.size, cache.idle_days) };
        if let Some(lane) = compile_cache_lane(cache) {
            line.push_str(&format!(" ({lane})"));
        }
        line
    }).collect();
    println!(
        "BuildKit worker image compile caches (an older weft's, or a `weft build-images` run's): {} across {} cache(s); \
         `weft clean --images --all` drops every key no install on this machine uses, `weft clean --build-cache` drops everything now",
        sizes.join(" + "),
        caches.len(),
    );
    Ok(())
}

/// The bytes every file under `dir` takes, 0 when there is no `dir`.
fn dir_size(dir: &std::path::Path) -> anyhow::Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    super::test_node::dir_size_bytes(dir).with_context(|| format!("measure {}", dir.display()))
}

/// Layer 4 (`--all` only): the compile caches no build needs kept. First
/// every lane cache this machine keeps for a set of `[build.env]` values
/// ([`tagged_lane_cache_sweep`]), then the BuildKit worker image compile
/// caches no install uses: what an older weft's worker image builds (which record
/// their keys in their install's record) or a `weft build-images` run
/// (which records none) left. The key changes with the build environment
/// or the builder, and nothing mounts an old one again unless an install
/// goes back to it, so its own sweep never runs; this is the only thing
/// that reclaims it. Every lane of a key an install records stays; every
/// lane of any other key goes, at once. While an install has not recorded
/// its keys, nothing goes.
///
/// A lane a build on this machine holds right now (its lock file, the one
/// an older weft's image builds and this weft's host builds both take,
/// `weft_platform_local::lanes`) is skipped whatever its key, and the
/// sweep holds each lane it drops from, so no build starts on it mid-drop.
async fn unused_compile_cache_sweep(data_dir: &std::path::Path) -> anyhow::Result<()> {
    fn keys(r: &weft_core::images::ImageRefs) -> Vec<&str> {
        r.compile_cache_keys.iter().map(String::as_str).collect()
    }
    let machine = weft_platform_local::MachineLayout::new(data_dir);
    tagged_lane_cache_sweep(&machine)?;
    let mut lanes: std::collections::BTreeMap<u32, Option<weft_platform_local::lanes::Lane>> = std::collections::BTreeMap::new();
    let mut failures = Vec::new();
    for cache in crate::images::worker_compile_caches().await?.iter() {
        // The records are looked at again for each cache, and held through
        // its prune: a build that records its key meanwhile either got in
        // first (the cache stays) or waits for this one prune.
        let records = crate::image_refs::every_install(data_dir).await?;
        let kept = match crate::image_refs::kept(&records.installs, keys, "worker compile cache") {
            crate::image_refs::Kept::Unknown(why) => {
                println!("{why}");
                return Ok(());
            }
            crate::image_refs::Kept::Only(kept) => kept,
        };
        if kept.contains(&cache.key) {
            continue;
        }
        let lane = compile_cache_lane(cache).map(|lane| format!(", {lane}")).unwrap_or_default();
        if let Some(number) = cache.lane {
            let taken = match lanes.entry(number) {
                std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::btree_map::Entry::Vacant(e) => e.insert(weft_platform_local::lanes::Lane::try_take(&machine, number)?),
            };
            if taken.is_none() {
                println!("keeping a worker image compile cache no install uses (key {}{lane}, {}): a build holds that lane right now", cache.key, cache.size);
                continue;
            }
        }
        println!("dropping a worker image compile cache no install uses (key {}{lane}, {})", cache.key, cache.size);
        if let Err(error) = crate::images::prune_build_record(&cache.id).await {
            failures.push(format!("{error:#}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

/// Every lane cache kept for a set of `[build.env]` values
/// (`weft_platform_local::host_build::reclaim_tagged_caches`) from the
/// lanes no build holds right now. A build trims only the cache it compiled
/// in, so this is what reclaims the cache of values no build sets any
/// more; one still in use starts its next build from the seed.
fn tagged_lane_cache_sweep(machine: &weft_platform_local::MachineLayout) -> anyhow::Result<()> {
    let done = weft_platform_local::host_build::reclaim_tagged_caches(machine)?;
    for cache in &done.held {
        println!("keeping the compile cache {}: a build holds that lane right now", cache.display());
    }
    match done.removed.len() {
        0 => println!("no compile caches kept for nodes' [build.env] values"),
        n => println!("removed {n} compile cache(s) kept for nodes' [build.env] values"),
    }
    Ok(())
}

/// Layer 5 (`--all` only): the caches that compiled weft's own crates (the
/// runtime image's, the builder base's) for an install no longer on this
/// machine, and those no install owns but this checkout's builder base
/// (one from before each install had its own, a builder base's under a
/// toolchain this checkout no longer pins). `weft daemon remove` drops an
/// install's as it goes; this takes what an install removed otherwise left
/// (by an older weft, or by hand). A cache a build mounts right now is
/// kept and said.
async fn removed_install_cache_sweep(data_dir: &std::path::Path) -> anyhow::Result<()> {
    let installs: Vec<String> = super::daemon::Install::every_on(data_dir)?.iter().map(|i| i.id.label_value().to_string()).collect();
    let root = weft_compiler::build::resolve_weft_root().map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let base_key = weft_compiler::build::builder_base_cache_key(&root).map_err(|e| anyhow::anyhow!("the builder base's compile cache key: {e}"))?;
    let mut failures = Vec::new();
    for cache in crate::images::install_build_caches().await? {
        let whose = match &cache.install {
            Some(install) if installs.contains(install) => continue,
            Some(install) => format!("of install '{install}', which is no longer on this machine"),
            None if cache.key == base_key => continue,
            None => "no install owns, under a key this checkout no longer builds in".to_string(),
        };
        if !cache.reclaimable {
            println!("keeping a compile cache {whose} ({}): a build mounts it right now", cache.size);
            continue;
        }
        println!("dropping a compile cache {whose} ({})", cache.size);
        if let Err(error) = crate::images::prune_build_record(&cache.id).await {
            failures.push(format!("{error:#}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

/// Layer 6 (`--all` only): the images a named install no longer on this
/// machine held under its own names (`localhost/weft-<name>/...`).
/// `weft daemon remove` removes an install's as it goes; this takes what
/// an install removed otherwise left (by an older weft, or by hand).
async fn removed_install_image_sweep(data_dir: &std::path::Path) -> anyhow::Result<()> {
    // The images first: an install's folder exists before its first image
    // does, so every install owning an image in this listing is found by
    // the look at the installs after it, however new.
    let listing = crate::images::host_image_listing(&[]).await?;
    let installs: Vec<weft_core::infra::Install> = super::daemon::Install::every_on(data_dir)?.into_iter().map(|i| i.id).collect();
    let stale = crate::images::images_owned_by(&listing, |owner| !installs.contains(owner));
    if stale.is_empty() {
        println!("no images of removed installs");
        return Ok(());
    }
    println!("{}", crate::images::reclaim_host_images(&stale).await?.report("image(s) of removed installs"));
    Ok(())
}

/// Which lane a compile cache's record names, in words: its number, or
/// what it names that is no lane; `None` for one in no lane.
fn compile_cache_lane(cache: &crate::images::CompileCacheRecord) -> Option<String> {
    match (cache.lane, cache.unreadable_lane.as_deref()) {
        (Some(lane), _) => Some(format!("lane {lane}")),
        (None, Some(suffix)) => Some(format!("record {} has lane '{suffix}', not a number", cache.id)),
        (None, None) => None,
    }
}

/// `docker buildx prune` reclaims BuildKit's intermediate layers.
/// This is the heavy reclaim: cargo deps, intermediate Rust compile
/// state, etc. The next build will re-download deps and re-link. The
/// host's node-test cache and the machine's worker compile caches go
/// with it (every lane and the seed, and the crates the builds share):
/// they are the same kind of thing (compiled engine plus dependencies,
/// re-made by the next build), and "drop every build cache" should mean
/// every one.
async fn clean_build_cache() -> anyhow::Result<()> {
    println!("dropping the node-test cache (the next `weft test-node` builds cold)…");
    super::test_node::wipe_cache()?;
    let machine = weft_platform_local::MachineLayout::here();
    println!("dropping the worker compile caches (the next build of a project's own nodes compiles everything)…");
    // Held until every folder is gone, so no build compiles in what goes.
    let _held = weft_platform_local::lanes::hold_every_cache(&machine, || println!("waiting for the builds compiling right now to end…")).await?;
    for dir in [machine.compile_caches(), machine.shared_crates()] {
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => anyhow::bail!("remove {}: {e}", dir.display()),
        }
    }
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
    use super::event_line;
    use serde_json::json;

    /// The compact line cuts a long value at the summary width on a
    /// character boundary; `full` prints it whole; and everything the
    /// row carries reaches the line, including the fields only one
    /// A node that completed without emitting says so on its own line.
    ///
    /// The generic tail drops empty fields, which is right for almost
    /// everything and wrong for exactly this: a firing that emitted
    /// nothing closed every one of its ports, so the whole branch behind
    /// it skipped. Without the words, the line is indistinguishable from
    /// a node that emitted plenty, and the person reading is usually
    /// reading BECAUSE a value never arrived.
    #[test]
    fn a_completion_that_emitted_nothing_says_so() {
        for output in [json!({}), json!(null)] {
            let row = json!({
                "kind": "node_completed", "node": "pick",
                "atUnix": 1_756_838_207u64, "output": output,
            });
            let line = event_line(&row, false);
            assert!(line.contains("(nothing emitted)"), "{line}");
        }
        // Absent entirely is the same nothing.
        let bare = json!({ "kind": "node_completed", "node": "pick", "atUnix": 1_756_838_207u64 });
        assert!(event_line(&bare, false).contains("(nothing emitted)"));

        // A node that emitted is untouched, and says nothing about silence.
        let spoke = json!({
            "kind": "node_completed", "node": "pick",
            "atUnix": 1_756_838_207u64, "output": {"value": 1},
        });
        let line = event_line(&spoke, false);
        assert!(!line.contains("nothing emitted"), "{line}");
        assert!(line.contains("output="), "{line}");

        // Only completions: a skip carries its own reason and says why
        // already, so the words would be noise on top of a better line.
        let skipped = json!({
            "kind": "node_skipped", "node": "pick",
            "atUnix": 1_756_838_207u64, "reason": {"kind": "did_not_flow"},
        });
        assert!(!event_line(&skipped, false).contains("nothing emitted"));
    }

    /// A caller window that names messages the caller never received
    /// says what they are, and one that names none says nothing about it.
    #[test]
    fn a_caller_window_says_which_messages_never_reached_the_caller() {
        let window = |dropped: serde_json::Value| json!({
            "kind": "caller_window", "firstOffset": 3, "lastOffset": 4,
            "messages": [], "totals": [], "dropped": dropped, "atUnix": 1_756_838_207u64,
        });
        let line = event_line(&window(json!([1, 4])), false);
        assert!(line.contains("dropped=1,4 (the caller read too slowly and never received these messages)"), "{line}");
        assert!(!line.contains("dropped=["), "said once, in words: {line}");
        let line = event_line(&window(json!([])), false);
        assert!(!line.contains("dropped"), "{line}");
        let left = json!({
            "kind": "caller_disconnected", "offset": 9, "reason": "caller hung up",
            "dropped": [7, 8], "atUnix": 1_756_838_207u64,
        });
        let line = event_line(&left, false);
        assert!(line.contains("dropped=7,8 (still waiting to go out when the exchange ended"), "{line}");
        assert!(line.contains("reason=caller hung up") && !line.contains("dropped=["), "{line}");
    }

    /// kind has.
    #[test]
    fn event_line_summarises_and_expands() {
        let long: String = "é".repeat(300);
        let row = json!({
            "kind": "node_completed",
            "node": "llm",
            "atUnix": 1_756_838_207u64,
            "output": {"text": long},
        });
        let compact = event_line(&row, false);
        assert!(compact.contains(" node_completed ") && compact.contains(" llm"), "{compact}");
        assert!(compact.ends_with("..."), "{compact}");
        assert!(compact.chars().count() < 200, "{}", compact.chars().count());
        let full = event_line(&row, true);
        assert!(full.contains(&long), "full keeps the whole value");

        let cancelled = json!({
            "kind": "run_cancelled",
            "reason": "stopped by sibling",
            "tags": ["user:1"],
            "atUnix": 1_756_838_207u64,
        });
        let line = event_line(&cancelled, false);
        assert!(line.contains("reason=stopped by sibling") && line.contains("tags=user:1"), "{line}");
        let skipped = json!({
            "kind": "node_skipped",
            "node": "send",
            "reason": {"kind": "did_not_flow"},
            "closedPorts": ["ok"],
            "atUnix": 5u64,
        });
        let line = event_line(&skipped, false);
        assert!(line.contains("reason=did_not_flow") && line.contains("closedPorts=ok"), "{line}");
        // A derived row with no stamp keeps the columns aligned, and
        // the row that names its node `nodeId` still shows it.
        let derived = json!({"kind": "journal_corruption", "reason": "bad row"});
        assert!(event_line(&derived, false).starts_with(&" ".repeat(21)));
        let cost = json!({
            "kind": "cost_reported",
            "nodeId": "llm",
            "projectId": "e8969195-52aa-42e0-8a9b-f9577ddd8bed",
            "frames": [],
            "service": "openai",
            "amountUsd": 0.0123,
            "atUnix": 5u64,
        });
        let line = event_line(&cost, false);
        assert!(line.contains(" llm") && line.contains("service=openai") && line.contains("amountUsd=0.0123"), "{line}");
        assert!(!line.contains("projectId") && !line.contains("frames"), "the run's own name and an empty frame stack are not news: {line}");
        // The fields the old hand-picked key list dropped.
        let suspended = json!({"kind": "node_suspended", "node": "ask", "token": "a44e5cad", "atUnix": 5u64});
        assert!(event_line(&suspended, false).contains("token=a44e5cad"));
        let iteration = json!({"kind": "loop_iteration_launched", "node": "run__in", "index": 7, "atUnix": 5u64});
        assert!(event_line(&iteration, false).contains("index=7"));
    }
}

#[cfg(test)]
mod cut_notice_tests {
    use serde_json::json;

    use super::any_value_cut;

    /// The cut is silent, and the value it cuts is usually the one
    /// being chased. The line telling the reader `--full` exists has to
    /// appear exactly when something was cut, and not otherwise: a
    /// notice on every run is noise nobody reads.
    #[test]
    fn a_row_says_whether_it_lost_anything() {
        let long = json!({ "kind": "node_completed", "output": { "text": "x".repeat(400) } });
        assert!(any_value_cut(&long));

        let short = json!({ "kind": "node_completed", "output": { "text": "ok" } });
        assert!(!any_value_cut(&short));

        // A long value in a column printed in its own place is not part
        // of the tail, so it cannot be what was cut.
        let column = json!({ "kind": "node_completed", "node": "n".repeat(400) });
        assert!(!any_value_cut(&column));
    }
}

//! `weft tree`: the version tree, one line per version (id prefix,
//! label, what changed against its parent, how many runs its triggers
//! started) with its runs started by hand beneath (run prefix,
//! status, seed, scope, example). Above it, every working copy (each
//! place the project is edited: a folder, or the copy programs write into)
//! with its head, whether it changed since, and its conflict copies; this
//! folder's is marked, and each head is marked on the tree.

use std::collections::BTreeMap;

use super::versions::{fetch_tree_raw, short};
use weft_core::versions::{VersionRun, VersionSummary, VersionTree};
use weft_core::working_copy::{WorkingCopy, WorkingCopyId, WorkingCopyKind};
use super::Ctx;

/// A read: the folder is looked up, never synced, so nothing goes up and
/// nothing is written here.
pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let here = super::copy::look_up_this_copy(&ctx).await?;
    let (tree, mut raw) = fetch_tree_raw(&here.client, &here.project_id).await?;
    if ctx.json() {
        // The tree as the dispatcher answered it, plus the one fact only
        // this side knows: which working copy is this folder's (`null`
        // when the install does not know the folder).
        if let Some(obj) = raw.as_object_mut() {
            obj.insert("workingCopy".into(), serde_json::to_value(here.id())?);
        }
        ctx.json_out(&raw)?;
        return Ok(());
    }
    if here.id().is_none() {
        println!("this folder: not registered (`{}` or `{}` from here registers it)", ctx.weft("checkpoint"), ctx.weft("run"));
    }
    for line in render(&tree, here.id(), &super::utc_time) {
        println!("{line}");
    }
    Ok(())
}

/// The tree as lines, `mine` being this folder's working copy. Pure (the
/// clock formatter is passed in), so the layout is tested.
pub fn render(tree: &VersionTree, mine: Option<WorkingCopyId>, when: &dyn Fn(u64) -> String) -> Vec<String> {
    let mut out: Vec<String> = tree.working_copies.iter().map(|copy| copy_line(copy, mine)).collect();
    if tree.versions.is_empty() {
        out.push("no versions yet: `weft run` or `weft checkpoint` records one".to_string());
        return out;
    }
    let mut children: BTreeMap<Option<&str>, Vec<&VersionSummary>> = BTreeMap::new();
    for v in &tree.versions {
        children.entry(v.parent_id.as_deref()).or_default().push(v);
    }
    let mut runs_by_version: BTreeMap<&str, Vec<&VersionRun>> = BTreeMap::new();
    for r in &tree.runs {
        runs_by_version.entry(r.version_id.as_str()).or_default().push(r);
    }
    let mut roots: Vec<&VersionSummary> = children.get(&None).cloned().unwrap_or_default();
    // A version whose parent is gone (pruned under it) is a root too.
    for v in &tree.versions {
        if v.parent_id.as_deref().is_some_and(|p| !tree.versions.iter().any(|x| x.id == p)) && !roots.iter().any(|r| r.id == v.id) {
            roots.push(v);
        }
    }
    let mut shown: std::collections::BTreeSet<&str> = Default::default();
    for root in roots {
        render_version(tree, mine, root, 0, &children, &runs_by_version, when, &mut shown, &mut out);
    }
    // Whatever the walk did not reach, printed anyway.
    //
    // A version can have a parent that is present and still not hang off
    // any root, if the parent links ever come out in a shape this walk
    // cannot follow. Those used to be dropped from the listing in
    // silence, while `--json` (which forwards the server's answer whole)
    // still showed them, so the two views of one tree disagreed and only
    // one of them said so. A version you own is never invisible; it is
    // printed flat, under a line saying the tree could not place it.
    let unplaced: Vec<&VersionSummary> =
        tree.versions.iter().filter(|v| !shown.contains(v.id.as_str())).collect();
    if !unplaced.is_empty() {
        out.push(format!(
            "({} version(s) below could not be placed in the tree: their parent is \
             recorded but the line back to a root is broken)",
            unplaced.len()
        ));
        for v in unplaced {
            render_version(tree, mine, v, 0, &children, &runs_by_version, when, &mut shown, &mut out);
        }
    }
    out
}

/// Who a working copy is, as a line names it.
fn copy_name(copy: &WorkingCopy, mine: Option<WorkingCopyId>) -> String {
    if Some(copy.id) == mine {
        return "this folder".to_string();
    }
    match (copy.kind, &copy.path, &copy.host) {
        (WorkingCopyKind::Folder, Some(path), Some(host)) => format!("{path} on {host}"),
        (WorkingCopyKind::Folder, Some(path), None) => path.clone(),
        (WorkingCopyKind::Folder, None, _) => format!("folder {}", short(&copy.id.to_string())),
        (WorkingCopyKind::Program, _, _) => "the program copy".to_string(),
    }
}

/// One working copy's line: who it is, its head, whether its files
/// changed since, and the conflict copies its folder holds.
fn copy_line(copy: &WorkingCopy, mine: Option<WorkingCopyId>) -> String {
    let at = match (&copy.head.head_version, copy.head.head_run) {
        (Some(version), Some(run)) => format!("at run {} on version {}", short(&run.to_string()), short(version)),
        (Some(version), None) => format!("at version {}", short(version)),
        (None, _) => "at no version yet".to_string(),
    };
    let changed = if copy.is_changed() { ", changed since" } else { "" };
    let conflicts =
        if copy.conflicts.is_empty() { String::new() } else { format!(", conflict copies: {}", copy.conflicts.join(", ")) };
    format!("{} {at}{changed}{conflicts}", copy_name(copy, mine))
}

fn render_version<'a>(
    tree: &'a VersionTree,
    mine: Option<WorkingCopyId>,
    v: &'a VersionSummary,
    depth: usize,
    children: &BTreeMap<Option<&str>, Vec<&'a VersionSummary>>,
    runs: &BTreeMap<&str, Vec<&'a VersionRun>>,
    when: &dyn Fn(u64) -> String,
    // Every version this walk has printed, so the caller can print what
    // it never reached instead of losing it.
    shown: &mut std::collections::BTreeSet<&'a str>,
    out: &mut Vec<String>,
) {
    // A version reached twice would print its whole subtree twice; a
    // version reached never is the caller's to print flat.
    if !shown.insert(v.id.as_str()) {
        return;
    }
    let indent = "  ".repeat(depth);
    let mut marks: Vec<String> = tree
        .working_copies
        .iter()
        .filter(|copy| copy.head.head_version.as_deref() == Some(v.id.as_str()))
        .map(|copy| format!("HEAD of {}", copy_name(copy, mine)))
        .collect();
    if tree.activated_versions.contains(&v.id) {
        marks.push("activated".to_string());
    }
    let mark = if marks.is_empty() { String::new() } else { format!(" <- {}", marks.join(", ")) };
    let label = v.label.as_deref().map(|l| format!(" ({l})")).unwrap_or_default();
    let changed: Vec<String> = v
        .diff
        .changed
        .iter()
        .map(|p| format!("~{p}"))
        .chain(v.diff.added.iter().map(|p| format!("+{p}")))
        .chain(v.diff.removed.iter().map(|p| format!("-{p}")))
        .collect();
    let change = if v.parent_id.is_none() {
        " root".to_string()
    } else if changed.is_empty() {
        String::new()
    } else {
        format!(" {}", changed.join(" "))
    };
    let fired = match (v.trigger_runs, v.last_trigger_run) {
        (0, _) => String::new(),
        (runs, Some(last)) => format!(" [{runs} trigger run{}, last {}]", if runs == 1 { "" } else { "s" }, short(&last.to_string())),
        (runs, None) => format!(" [{runs} trigger run{}]", if runs == 1 { "" } else { "s" }),
    };
    out.push(format!("{indent}{} {}{label}{change}{fired}{mark}", short(&v.id), when(v.created_at)));
    for r in runs.get(v.id.as_str()).cloned().unwrap_or_default() {
        let heads: Vec<String> = tree
            .working_copies
            .iter()
            .filter(|copy| copy.head.head_run == Some(r.run_id))
            .map(|copy| format!("HEAD run of {}", copy_name(copy, mine)))
            .collect();
        let head = if heads.is_empty() { String::new() } else { format!(" <- {}", heads.join(", ")) };
        let seed = r.seed_run_id.map(|s| format!(" seed {} ({} stale)", short(&s.to_string()), r.stale.len())).unwrap_or_default();
        let scope = r.spec.as_ref().map(started_by).unwrap_or_default();
        let example = r.example.as_deref().map(|e| format!(" example {e}")).unwrap_or_default();
        let listing = r.listing.as_ref();
        let ended = listing.and_then(|l| l.completed_at).map(|t| format!(" -> {}", when(t))).unwrap_or_default();
        // A cancelled run says who ended it, because a person stopping
        // a run and the runtime cutting one are different stories.
        let cause = match listing.and_then(|l| l.cancel_cause.as_ref()) {
            Some(weft_core::run::CancelCause::User) => " (a person stopped it)".to_string(),
            Some(weft_core::run::CancelCause::Run { by, .. }) => {
                format!(" (run {} stopped it)", short(&by.to_string()))
            }
            Some(weft_core::run::CancelCause::CallerGone) => {
                " (the caller went away)".to_string()
            }
            Some(weft_core::run::CancelCause::Runtime { detail }) => format!(" ({detail})"),
            Some(weft_core::run::CancelCause::Limit { detail }) => format!(" (a time limit: {detail})"),
            None => String::new(),
        };
        let skipped = match listing.map(|l| l.skipped_nodes) {
            Some(skipped) if skipped > 0 => format!(" {skipped} skipped"),
            _ => String::new(),
        };
        let status = listing.map_or("unknown", |l| l.status.as_str());
        out.push(format!("{indent}  run {} {}{ended} {status}{cause}{skipped}{seed}{scope}{example}{head}", short(&r.run_id.to_string()), when(r.started_at)));
    }
    for child in children.get(&Some(v.id.as_str())).cloned().unwrap_or_default() {
        render_version(tree, mine, child, depth + 1, children, runs, when, shown, out);
    }
}

/// What started a run, in the words a person reads it by: the trigger
/// that fired it, or the starting parameters it was run with (`weft run
/// --save` names them). A plain run says nothing.
fn started_by(spec: &weft_core::run_spec::RunSpec) -> String {
    match (&spec.fire, spec.name.as_str()) {
        (Some((trigger, _)), _) => format!(" fired by {trigger}"),
        (None, "") => String::new(),
        (None, name) => format!(" with starting parameters '{name}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::versions::ManifestDiff;
    use weft_core::working_copy::Head;

    fn copy(id: u128, kind: WorkingCopyKind, path: Option<&str>, version: Option<&str>, run: Option<uuid::Uuid>, working: &str) -> WorkingCopy {
        WorkingCopy {
            id: uuid::Uuid::from_u128(id),
            kind,
            host: path.map(|_| "box".to_string()),
            path: path.map(str::to_string),
            head: Head { head_version: version.map(|v| format!("{v}00000000")), head_run: run },
            working_version: format!("{working}00000000"),
            revision: 0,
            conflicts: vec![],
            updated_at: 0,
        }
    }

    fn version(id: &str, parent: Option<&str>, label: Option<&str>, changed: &[&str]) -> VersionSummary {
        VersionSummary {
            id: format!("{id}00000000"),
            parent_id: parent.map(|p| format!("{p}00000000")),
            label: label.map(str::to_string),
            created_at: 0,
            diff: ManifestDiff { added: vec![], removed: vec![], changed: changed.iter().map(|s| s.to_string()).collect() },
            manifest: Default::default(),
            trigger_runs: 0,
            last_trigger_run: None,
        }
    }

    /// A run id whose first eight characters are `prefix` padded
    /// with zeros, which is what a line shows.
    fn run_id(prefix: &str) -> uuid::Uuid {
        format!("{prefix:0<8}-0000-0000-0000-000000000000").parse().expect("a uuid")
    }

    fn run(id: &str, version: &str, seed: Option<&str>, status: &str) -> VersionRun {
        VersionRun {
            run_id: run_id(id),
            version_id: format!("{version}00000000"),
            definition_hash: "d".into(),
            seed_run_id: seed.map(run_id),
            stale: vec!["b".into()],
            spec: None,
            example: None,
            started_at: 0,
            listing: weft_core::program::RunStatus::parse(status).map(|status| weft_core::program::RunSummary {
                run_id: run_id(id),
                project_id: uuid::Uuid::nil(),
                entry_node: "n".into(),
                status,
                phase: weft_core::primitive::Phase::Fire,
                started_at: 0,
                completed_at: if status == weft_core::program::RunStatus::Completed { Some(9) } else { None },
                tags: vec![],
                // A cancelled run in this fixture says a person did it, so
                // the line that renders a cause is exercised at all.
                cancel_cause: (status == weft_core::program::RunStatus::Cancelled).then_some(weft_core::run::CancelCause::User),
                error: None,
                skipped_nodes: 0,
                instance: None,
            }),
        }
    }

    #[test]
    fn the_tree_indents_children_lists_runs_under_their_version_and_marks_head() {
        let tree = VersionTree {
            working_copies: vec![copy(1, WorkingCopyKind::Folder, Some("/home/me/shop"), Some("v2"), Some(run_id("c2")), "v2")],
            activated_versions: vec![],
            versions: vec![version("v1", None, Some("base"), &[]), version("v2", Some("v1"), None, &["main.weft"]), version("v3", Some("v1"), None, &["prompts/p.txt"])],
            runs: vec![run("c1", "v1", None, "completed"), run("c2", "v2", Some("c1"), "running")],
        };
        let lines = render(&tree, Some(uuid::Uuid::from_u128(1)), &|t| format!("t{t}"));
        assert_eq!(
            lines,
            vec![
                "this folder at run c2000000 on version v2000000",
                "v1000000 t0 (base) root",
                "  run c1000000 t0 -> t9 completed",
                "  v2000000 t0 ~main.weft <- HEAD of this folder",
                "    run c2000000 t0 running seed c1000000 (1 stale) <- HEAD run of this folder",
                "  v3000000 t0 ~prompts/p.txt",
            ]
        );
    }

    /// Every working copy is listed and its head marked on the tree: this
    /// folder's as yours, another folder by its path, the program's copy as
    /// itself; one whose files moved since its head says so, with the
    /// conflict copies its folder holds.
    #[test]
    fn every_working_copy_is_listed_and_its_head_marked() {
        let mut other = copy(2, WorkingCopyKind::Folder, Some("/srv/shop"), Some("v1"), None, "v9");
        other.conflicts = vec!["main.weft-conflict-1".into()];
        let tree = VersionTree {
            working_copies: vec![
                copy(1, WorkingCopyKind::Folder, Some("/home/me/shop"), Some("v2"), None, "v2"),
                other,
                copy(3, WorkingCopyKind::Program, None, Some("v2"), None, "v2"),
            ],
            activated_versions: vec!["v100000000".into()],
            versions: vec![version("v1", None, None, &[]), version("v2", Some("v1"), None, &["main.weft"])],
            runs: vec![],
        };
        assert_eq!(
            render(&tree, Some(uuid::Uuid::from_u128(1)), &|t| format!("t{t}")),
            vec![
                "this folder at version v2000000",
                "/srv/shop on box at version v1000000, changed since, conflict copies: main.weft-conflict-1",
                "the program copy at version v2000000",
                "v1000000 t0 root <- HEAD of /srv/shop on box, activated",
                "  v2000000 t0 ~main.weft <- HEAD of this folder, HEAD of the program copy",
            ]
        );
    }

    /// A trigger's runs are counted on their version, never listed one by
    /// one.
    #[test]
    fn a_versions_trigger_runs_are_counted_on_its_line() {
        let mut fired = version("v1", None, None, &[]);
        fired.trigger_runs = 1200;
        fired.last_trigger_run = Some(run_id("f9"));
        let mut once = version("v2", Some("v1"), None, &["main.weft"]);
        once.trigger_runs = 1;
        let tree = VersionTree { working_copies: vec![], activated_versions: vec![], versions: vec![fired, once], runs: vec![] };
        assert_eq!(
            render(&tree, None, &|t| format!("t{t}")),
            vec!["v1000000 t0 root [1200 trigger runs, last f9000000]", "  v2000000 t0 ~main.weft [1 trigger run]"]
        );
    }

    #[test]
    fn an_empty_tree_says_how_to_start_one() {
        let tree = VersionTree { working_copies: vec![], activated_versions: vec![], versions: vec![], runs: vec![] };
        assert!(render(&tree, None, &|t| t.to_string())[0].contains("weft checkpoint"));
    }
}

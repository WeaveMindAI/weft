//! `weft tree`: the version tree, one line per version (id prefix,
//! label, what changed against its parent) with its runs beneath
//! (color prefix, status, seed, scope, example). Head is marked.

use std::collections::BTreeMap;

use super::versions::{fetch_tree_raw, short, RunSummary, Tree, VersionSummary};
use super::Ctx;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let project = ctx.project()?;
    let client = ctx.client();
    let (tree, mut raw) = fetch_tree_raw(&client, &project.id().to_string()).await?;
    if ctx.json() {
        // The tree as the dispatcher answered it, plus the one fact only
        // this side knows: which version (if any) the files on disk are.
        // Same answer the human view renders, so the two cannot disagree.
        let disk = weft_core::project::hash::manifest_version_id(&super::versions::local_manifest(project)?);
        let known = tree.versions.iter().any(|v| v.id == disk);
        if let Some(obj) = raw.as_object_mut() {
            obj.insert("disk_version".into(), if known { serde_json::Value::String(disk) } else { serde_json::Value::Null });
        }
        ctx.json_out(&raw)?;
        return Ok(());
    }
    for line in render(&tree, &super::local_time) {
        println!("{line}");
    }
    Ok(())
}

/// The tree as lines. Pure (the clock formatter is passed in), so the
/// layout is tested.
pub fn render(tree: &Tree, when: &dyn Fn(u64) -> String) -> Vec<String> {
    if tree.versions.is_empty() {
        return vec!["no versions yet: `weft run` or `weft checkpoint` records one".to_string()];
    }
    let mut children: BTreeMap<Option<&str>, Vec<&VersionSummary>> = BTreeMap::new();
    for v in &tree.versions {
        children.entry(v.parent_id.as_deref()).or_default().push(v);
    }
    let mut runs_by_version: BTreeMap<&str, Vec<&RunSummary>> = BTreeMap::new();
    for r in &tree.runs {
        runs_by_version.entry(r.version_id.as_str()).or_default().push(r);
    }
    let mut out = Vec::new();
    let mut roots: Vec<&VersionSummary> = children.get(&None).cloned().unwrap_or_default();
    // A version whose parent is gone (pruned under it) is a root too.
    for v in &tree.versions {
        if v.parent_id.as_deref().is_some_and(|p| !tree.versions.iter().any(|x| x.id == p)) && !roots.iter().any(|r| r.id == v.id) {
            roots.push(v);
        }
    }
    let mut shown: std::collections::BTreeSet<&str> = Default::default();
    for root in roots {
        render_version(tree, root, 0, &children, &runs_by_version, when, &mut shown, &mut out);
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
            render_version(tree, v, 0, &children, &runs_by_version, when, &mut shown, &mut out);
        }
    }
    out
}

fn render_version<'a>(
    tree: &'a Tree,
    v: &'a VersionSummary,
    depth: usize,
    children: &BTreeMap<Option<&str>, Vec<&'a VersionSummary>>,
    runs: &BTreeMap<&str, Vec<&'a RunSummary>>,
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
    let mut marks = Vec::new();
    if tree.head.head_version.as_deref() == Some(v.id.as_str()) {
        marks.push("HEAD");
    }
    if tree.head.activation_version.as_deref() == Some(v.id.as_str()) {
        marks.push("activated");
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
    out.push(format!("{indent}{} {}{label}{change}{mark}", short(&v.id), when(v.created_at)));
    for r in runs.get(v.id.as_str()).cloned().unwrap_or_default() {
        let head = if tree.head.head_run.as_deref() == Some(r.color.as_str()) { " <- HEAD run" } else { "" };
        let seed = r.seed_color.as_deref().map(|s| format!(" seed {} ({} stale)", short(s), r.stale.len())).unwrap_or_default();
        let scope = r.spec.as_ref().map(|s| format!(" spec {}", s.name)).unwrap_or_default();
        let example = r.example.as_deref().map(|e| format!(" example {e}")).unwrap_or_default();
        let ended = r.completed_at.map(|t| format!(" -> {}", when(t))).unwrap_or_default();
        // A cancelled run says who ended it, because a person stopping
        // a run and the runtime cutting one are different stories.
        let cause = match &r.cancel_cause {
            Some(weft_core::exec::CancelCause::User) => " (a person stopped it)".to_string(),
            Some(weft_core::exec::CancelCause::Execution { by, .. }) => {
                format!(" (run {} stopped it)", short(&by.to_string()))
            }
            Some(weft_core::exec::CancelCause::CallerGone) => {
                " (the caller went away)".to_string()
            }
            Some(weft_core::exec::CancelCause::Runtime { detail }) => format!(" ({detail})"),
            None => String::new(),
        };
        let skipped = if r.skipped_nodes > 0 {
            format!(" {} skipped", r.skipped_nodes)
        } else {
            String::new()
        };
        out.push(format!("{indent}  run {} {}{ended} {}{cause}{skipped}{seed}{scope}{example}{head}", short(&r.color), when(r.started_at), r.status));
    }
    for child in children.get(&Some(v.id.as_str())).cloned().unwrap_or_default() {
        render_version(tree, child, depth + 1, children, runs, when, shown, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::versions::{Head, ManifestDiff};

    fn version(id: &str, parent: Option<&str>, label: Option<&str>, changed: &[&str]) -> VersionSummary {
        VersionSummary {
            id: format!("{id}00000000"),
            parent_id: parent.map(|p| format!("{p}00000000")),
            label: label.map(str::to_string),
            created_at: 0,
            diff: ManifestDiff { added: vec![], removed: vec![], changed: changed.iter().map(|s| s.to_string()).collect() },
            manifest: Default::default(),
        }
    }

    fn run(color: &str, version: &str, seed: Option<&str>, status: &str) -> RunSummary {
        RunSummary {
            color: format!("{color}00000000"),
            version_id: format!("{version}00000000"),
            definition_hash: "d".into(),
            seed_color: seed.map(|s| format!("{s}00000000")),
            stale: vec!["b".into()],
            spec: None,
            example: None,
            status: status.into(),
            started_at: 0,
            completed_at: if status == "completed" { Some(9) } else { None },
            // A cancelled run in this fixture says a person did it, so
            // the line that renders a cause is exercised at all.
            cancel_cause: (status == "cancelled").then_some(weft_core::exec::CancelCause::User),
            skipped_nodes: 0,
        }
    }

    #[test]
    fn the_tree_indents_children_lists_runs_under_their_version_and_marks_head() {
        let tree = Tree {
            head: Head { head_version: Some("v200000000".into()), head_run: Some("c200000000".into()), activation_version: None },
            versions: vec![version("v1", None, Some("base"), &[]), version("v2", Some("v1"), None, &["main.weft"]), version("v3", Some("v1"), None, &["prompts/p.txt"])],
            runs: vec![run("c1", "v1", None, "completed"), run("c2", "v2", Some("c1"), "running")],
        };
        let lines = render(&tree, &|t| format!("t{t}"));
        assert_eq!(
            lines,
            vec![
                "v1000000 t0 (base) root",
                "  run c1000000 t0 -> t9 completed",
                "  v2000000 t0 ~main.weft <- HEAD",
                "    run c2000000 t0 running seed c1000000 (1 stale) <- HEAD run",
                "  v3000000 t0 ~prompts/p.txt",
            ]
        );
    }

    #[test]
    fn an_empty_tree_says_how_to_start_one() {
        let tree = Tree { head: Head::default(), versions: vec![], runs: vec![] };
        assert!(render(&tree, &|t| t.to_string())[0].contains("weft checkpoint"));
    }
}

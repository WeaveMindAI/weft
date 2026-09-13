//! `weft diff <ref> <ref>`: what two runs put on their wires, compared.
//! A ref is a color (or the start of one) or `example:<name>` (a frozen
//! spec's `expected`). Per node by default ("classify: 3 of 10000
//! frames differ"), per wire with both values under `--full`; media
//! compares by the stored bytes' content hash.

use anyhow::bail;
use std::collections::BTreeSet;
use weft_core::run_spec::{Expected, ExpectedWire};

use super::versions::{diff_wires, fetch_tree, frames_key, read_spec, resolve_run, short, WiresDiff};
use super::Ctx;

/// The wires a ref names, normalized for comparison.
pub async fn wires_of(ctx: &Ctx, client: &crate::client::DispatcherClient, reference: &str) -> anyhow::Result<(String, Expected)> {
    let project = ctx.project()?;
    let project_id = project.id().to_string();
    let mut resolved_color = String::new();
    let wires = if let Some(name) = reference.strip_prefix("example:") {
        let spec = read_spec(project, name)?;
        let Some(expected) = spec.expected else {
            bail!("example {name} is not frozen (no `expected`); `weft freeze {name}` freezes it from a run");
        };
        expected
    } else {
        let tree = fetch_tree(client, &project_id).await?;
        let run = resolve_run(&tree, reference)?;
        resolved_color = run.color.clone();
        super::versions::output_wires(client, &project_id, &run.color).await?
    };
    // Label with the RESOLVED color, not the prefix the user typed, so
    // `weft diff 3f a1b2c3d4` does not print one side as `3f` and the
    // other as `a1b2c3d`.
    let label = if reference.starts_with("example:") { reference.to_string() } else { short(&resolved_color).to_string() };
    Ok((label, wires))
}

pub async fn run(ctx: Ctx, left: String, right: String, full: bool) -> anyhow::Result<()> {
    let client = ctx.client();
    let (left_label, left_outputs) = wires_of(&ctx, &client, &left).await?;
    let (right_label, right_outputs) = wires_of(&ctx, &client, &right).await?;
    let focus: BTreeSet<_> = left_outputs.focus.iter().chain(&right_outputs.focus).cloned().collect();
    let mut diff = diff_wires(&left_outputs.wires, &right_outputs.wires);
    diff.differing.sort_by_key(|wire| !focus.contains(&wire.node));
    let missing_focus: Vec<_> = focus.iter().filter_map(|node| {
        let left_missing = !left_outputs.wires.iter().any(|w| &w.node == node);
        let right_missing = !right_outputs.wires.iter().any(|w| &w.node == node);
        (left_missing || right_missing).then(|| serde_json::json!({"node": node, "left_missing": left_missing, "right_missing": right_missing}))
    }).collect();
    if !ctx.json_out(&serde_json::json!({
        "left": left_label,
        "right": right_label,
        "same": diff.is_same(),
        "compared": diff.compared,
        "differing": diff.differing,
        "focus": focus,
        "missing_focus": missing_focus,
    }))? {
        if !focus.is_empty() {
            println!("Review focus: {}", focus.iter().cloned().collect::<Vec<_>>().join(", "));
            for node in &focus {
                for (label, outputs) in [(&left_label, &left_outputs), (&right_label, &right_outputs)] {
                    if !outputs.wires.iter().any(|wire| &wire.node == node) {
                        println!("{node}: no output recorded in {label}");
                    }
                }
            }
        }
        for line in render(&left_label, &right_label, &diff, &left_outputs.wires, full) {
            println!("{line}");
        }
    }
    Ok(())
}

/// The diff as lines. Pure.
pub fn render(left: &str, right: &str, diff: &WiresDiff, left_wires: &[ExpectedWire], full: bool) -> Vec<String> {
    if diff.is_same() {
        return vec![format!("{left} and {right}: same ({} wires)", diff.compared)];
    }
    let mut out = vec![format!("{left} and {right}: {} of {} wires differ", diff.differing.len(), diff.compared)];
    if full {
        for d in &diff.differing {
            let frames = if d.frames.is_empty() { String::new() } else { format!("[{}]", frames_key(&d.frames)) };
            out.push(format!("{}{frames}.{} #{}", d.node, d.port, d.ordinal));
            out.push(format!("  {left}: {}", output_text(d.left.as_ref())));
            out.push(format!("  {right}: {}", output_text(d.right.as_ref())));
        }
    } else {
        let mut nodes = diff.per_node(left_wires);
        nodes.sort_by_key(|(node, _, _)| diff.differing.iter().position(|wire| &wire.node == node));
        for (node, n, total) in nodes {
            out.push(format!("{node}: {n} of {total} wires differ"));
            for wire in diff.differing.iter().filter(|wire| wire.node == node).take(3) {
                let frames = if wire.frames.is_empty() { String::new() } else { format!("[{}]", frames_key(&wire.frames)) };
                out.push(format!("  {node}{frames}.{} #{}", wire.port, wire.ordinal));
                out.push(format!("    {left}: {}", preview(wire.left.as_ref())));
                out.push(format!("    {right}: {}", preview(wire.right.as_ref())));
            }
            if n > 3 { out.push(format!("  {} more differences; use --full to read all values", n - 3)); }
        }
    }
    out
}

fn output_text(wire: Option<&ExpectedWire>) -> String {
    match wire {
        None => "(missing)".into(),
        Some(wire) if wire.closed => wire.error.as_ref().map(|error| format!("(closed: {error})")).unwrap_or_else(|| "(closed)".into()),
        Some(wire) => wire.value.to_string(),
    }
}

fn preview(value: Option<&ExpectedWire>) -> String {
    let text = output_text(value);
    let mut chars = text.chars();
    let mut out: String = chars.by_ref().take(240).collect();
    if chars.next().is_some() { out.push_str("... (use --full)"); }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_review_shows_values_and_distinguishes_missing_from_null() {
        let wire = |port: &str, value| ExpectedWire { node: "answer".into(), port: port.into(), value, ..Default::default() };
        let left = vec![wire("text", json!("old response")), wire("optional", json!(null))];
        let right = vec![wire("text", json!("new response"))];
        let lines = render("before", "after", &diff_wires(&left, &right), &left, false).join("\n");
        assert!(lines.contains("old response"));
        assert!(lines.contains("new response"));
        assert!(lines.contains("before: null"));
        assert!(lines.contains("after: (missing)"));
        assert!(preview(Some(&wire("text", json!("é".repeat(300))))).ends_with("... (use --full)"));
    }

    #[test]
    fn review_compares_stream_items_by_order_and_reports_changed_termination() {
        let item = |ordinal, value, closed| ExpectedWire { node: "stream".into(), port: "rows".into(), ordinal, value, closed, ..Default::default() };
        let left = vec![item(0, json!(1), false), item(1, json!(2), false), item(2, json!(null), true)];
        let right = vec![item(0, json!(9), false), item(1, json!(2), false), item(2, json!(null), false)];
        let diff = diff_wires(&left, &right);
        assert_eq!(diff.compared, 3);
        assert_eq!(diff.differing.iter().map(|wire| wire.ordinal).collect::<Vec<_>>(), vec![0, 2]);
        let text = render("old", "new", &diff, &left, true).join("\n");
        assert!(text.contains("old: (closed)"));
        assert!(text.contains("new: null"));
    }
}

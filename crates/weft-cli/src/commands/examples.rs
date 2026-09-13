//! `weft examples`: every spec in `examples/`, which are runnable specs
//! and which are frozen, with their latest run.

use super::versions::{fetch_tree, list_specs, short};
use super::Ctx;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let project = ctx.project()?;
    let client = ctx.client();
    let (specs, unreadable) = list_specs(project)?;
    // Shown, not hidden: `weft examples` is the listing of what is in
    // `examples/`, so a file in there that does not read as a spec is
    // part of the answer.
    for problem in &unreadable {
        eprintln!("warning: {problem}");
    }
    let tree = fetch_tree(&client, &project.id().to_string()).await?;
    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    for spec in &specs {
        let last = tree.runs.iter().rev().find(|r| r.example.as_deref() == Some(spec.name.as_str()));
        let kind = if spec.is_frozen() { "frozen" } else { "spec" };
        let latest = last.map(|r| format!("{} (run {})", r.status, short(&r.color)));
        rows.push(serde_json::json!({
            "name": spec.name,
            "frozen": spec.is_frozen(),
            "from": spec.from,
            "target": spec.target,
            "before": spec.before,
            "group": spec.group,
            "last_run": last.map(|r| r.color.clone()),
            "last_status": last.map(|r| r.status.clone()),
        }));
        lines.push(format!("{:<24} {kind:<7} {}", spec.name, latest.unwrap_or_else(|| "never run".into())));
    }
    if ctx.json_out(&rows)? {
        return Ok(());
    }
    if lines.is_empty() {
        println!("no specs in examples/; `weft run --from node='{{\"port\":value}}' --save name` writes one, `weft freeze name` freezes a run");
    }
    for l in lines {
        println!("{l}");
    }
    Ok(())
}

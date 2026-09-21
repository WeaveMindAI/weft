//! `weft logs [color]`: print an execution's log: the lines its nodes
//! wrote, and every failure the journal recorded about it (a node
//! failing, a port refusing a value, the run failing or being
//! cancelled), in the order they were written. The first place to look when a run
//! went wrong; `weft events` is the full replay when the log is not
//! enough.
//!
//! - No argument: resolve the cwd project, fetch its most recent
//!   execution, show those logs.
//! - UUID argument: treat as color, show those logs.

use super::{local_time, resolve_project_id, Ctx};

/// The nodes a run skipped, with why, spelled the way the program
/// reads them (`keep.db` for the db of an included file) when the cwd
/// is the project. What `weft logs` prints for a run that wrote
/// nothing, so the reader learns why nothing happened without a second
/// command.
async fn skipped_nodes(ctx: &Ctx, color: &str) -> anyhow::Result<Vec<(String, String)>> {
    let definition = ctx.project().ok()
        .and_then(|project| weft_compiler::hash::load_enriched_project(project).ok())
        .map(|(definition, _)| definition);
    let replay: serde_json::Value =
        ctx.client().get_json(&format!("/executions/{color}/replay")).await?;
    let rows = replay
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("/executions/{color}/replay returned no array: {replay}"))?;
    let mut skipped = Vec::new();
    for row in rows {
        if row.get("kind").and_then(|v| v.as_str()) != Some("node_skipped") {
            continue;
        }
        // Spelled exactly as `weft events` spells it, including the rows
        // it drops (an included file's own boundary has no place in the
        // program a person wrote, so neither command prints it).
        let row = match &definition {
            Some(definition) => match super::executions::spell_node(row.clone(), definition) {
                Some(row) => row,
                None => continue,
            },
            None => row.clone(),
        };
        let node = row.get("node").and_then(|v| v.as_str()).unwrap_or("?").to_string();
        // The reason is the engine's own, rendered by its own words;
        // a row an older dispatcher wrote without one says so.
        let reason = row
            .get("reason")
            .cloned()
            .and_then(|r| serde_json::from_value::<weft_core::exec::skip::SkipReason>(r).ok())
            .map(|r| r.to_string())
            .unwrap_or_else(|| "(no reason recorded)".to_string());
        skipped.push((format!("{node}{}", frames_suffix(&row)), reason));
    }
    Ok(skipped)
}

/// The loop iteration a line was written in, as `#3` (or `#3.0` for a
/// loop inside a loop). Empty at the root.
fn frames_suffix(entry: &serde_json::Value) -> String {
    let Some(frames) = entry.get("frames").and_then(|v| v.as_array()) else {
        return String::new();
    };
    if frames.is_empty() {
        return String::new();
    }
    let path: Vec<String> = frames
        .iter()
        .filter_map(|f| f.get("index").and_then(|i| i.as_u64()))
        .map(|i| i.to_string())
        .collect();
    format!("#{}", path.join("."))
}

pub async fn run(ctx: Ctx, target: Option<String>, limit: Option<u32>) -> anyhow::Result<()> {
    let color = match target {
        Some(raw) => super::resolve_color(&ctx, &raw).await?,
        None => {
            let project_id = resolve_project_id(&ctx, None)?;
            let resp: serde_json::Value = ctx
                .client()
                .get_json(&format!("/projects/{project_id}/executions/latest"))
                .await
                .map_err(|e| anyhow::anyhow!("no executions for project {project_id}: {e}"))?;
            resp.get("color")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("dispatcher response missing color"))?
                .to_string()
        }
    };

    let query = limit.map(|n| format!("?limit={n}")).unwrap_or_default();
    let mut logs: serde_json::Value =
        ctx.client().get_json(&format!("/executions/{color}/logs{query}")).await?;
    let arr = logs["lines"]
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("/executions/{color}/logs returned no lines: {logs}"))?;
    // The limit that cut the tail comes back with it, so the notice
    // below is right whether the reader chose one or the dispatcher's
    // default applied.
    let limit = logs["limit"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("/executions/{color}/logs returned no limit: {logs}"))?;
    // A run that wrote nothing usually did nothing, and the reason is
    // a skip: a required input closed, a gate said no. The skip is a
    // lifecycle event, not a log line, so it is fetched here rather
    // than sending the reader to `weft events --kind skipped` to learn
    // why nothing happened. Fetched before anything prints, so a
    // replay that cannot be read fails this command whole instead of
    // after a line that read as an answer; and `--json` carries the
    // same list under `skipped`, since an agent reads that shape.
    let skipped = if arr.is_empty() { skipped_nodes(&ctx, &color).await? } else { Vec::new() };
    if arr.is_empty() {
        logs["skipped"] = skipped
            .iter()
            .map(|(node, reason)| serde_json::json!({ "node": node, "reason": reason }))
            .collect();
    }
    if ctx.json_out(&logs)? {
        return Ok(());
    }
    if arr.is_empty() {
        println!("(no logs: the run wrote no log lines and recorded no failure)");
        for (node, reason) in &skipped {
            println!("skipped {node}: {reason}");
        }
        return Ok(());
    }
    for entry in &arr {
        let level = entry.get("level").and_then(|v| v.as_str()).unwrap_or("info");
        let msg = entry.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let at = entry.get("at_unix").and_then(|v| v.as_u64()).unwrap_or(0);
        // The node column only for the lines that are about one
        // firing; a run-level line (the run failing) has none. Inside
        // a loop the iteration rides along (`llm#3:`), because two
        // hundred identical lines name nothing.
        let node = entry
            .get("node")
            .and_then(|v| v.as_str())
            .map(|n| format!(" {n}{}:", frames_suffix(entry)))
            .unwrap_or_default();
        let inherited = entry.get("inherited_from").and_then(|v| v.as_str())
            .map(|color| format!(" [inherited from {}]", super::versions::short(color)))
            .unwrap_or_default();
        println!("[{}] {level:>5}{node}{inherited} {msg}", local_time(at));
    }
    // The dispatcher answers the tail, so a full page means the run
    // may have written more than this; a cut log must never read as
    // the whole one.
    if arr.len() as u64 == limit {
        println!("(the last {limit} lines; the run may have written more, raise --limit to see them)");
    }
    Ok(())
}

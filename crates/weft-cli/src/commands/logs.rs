//! `weft logs [execution_id]`: print an execution's log: the lines its nodes
//! wrote, and every failure the journal recorded about it (a node
//! failing, a port refusing a value, the run failing or being
//! cancelled), in the order they were written. The first place to look when a run
//! went wrong; `weft events` is the full replay when the log is not
//! enough.
//!
//! - No argument: resolve the cwd project, fetch its most recent
//!   execution, show those logs.
//! - UUID argument: treat as execution, show those logs.

use anyhow::Context;
use weft_core::program::{ExecutionLogs, ExecutionSummary};

use super::{local_time, resolve_project_id, Ctx};

/// The nodes a run skipped, with why, spelled the way the program
/// reads them (`keep.db` for the db of an included file) when the cwd
/// is the project. What `weft logs` prints for a run that wrote
/// nothing, so the reader learns why nothing happened without a second
/// command.
async fn skipped_nodes(ctx: &Ctx, execution_id: &str) -> anyhow::Result<Vec<(String, String)>> {
    let definition = ctx.project().ok()
        .and_then(|project| weft_compiler::hash::load_enriched_project(project).ok())
        .map(|(definition, _)| definition);
    let rows = super::versions::replay_rows(&ctx.client()?, execution_id).await?;
    let mut skipped = Vec::new();
    for row in &rows {
        let weft_core::live_event::DispatcherEvent::NodeSkipped { node, frames, reason, .. } = &row.event else {
            continue;
        };
        // Spelled exactly as `weft events` spells it, including the rows
        // it drops (an included file's own boundary has no place in the
        // program a person wrote, so neither command prints it).
        let node = match &definition {
            Some(definition) => match super::executions::spell_node(serde_json::to_value(row)?, definition) {
                Some(spelled) => spelled.get("node").and_then(|v| v.as_str()).unwrap_or(node).to_string(),
                None => continue,
            },
            None => node.clone(),
        };
        // The reason is the engine's own, rendered by its own words; a
        // row written without one says so.
        let reason = reason.as_ref().map(|r| r.to_string()).unwrap_or_else(|| "(no reason recorded)".to_string());
        skipped.push((format!("{node}{}", frames_suffix(frames)), reason));
    }
    Ok(skipped)
}

/// The loop iteration a line was written in, as `#3` (or `#3.0` for a
/// loop inside a loop). Empty at the root.
fn frames_suffix(frames: &[weft_core::frames::Frame]) -> String {
    if frames.is_empty() {
        return String::new();
    }
    let path: Vec<String> = frames.iter().filter_map(|f| f.loop_index()).map(|i| i.to_string()).collect();
    format!("#{}", path.join("."))
}

pub async fn run(ctx: Ctx, target: Option<String>, limit: Option<u32>) -> anyhow::Result<()> {
    let execution_id = match target {
        Some(raw) => super::resolve_execution_id(&ctx, &raw).await?,
        None => {
            let project_id = resolve_project_id(&ctx, None)?;
            let resp = ctx
                .client()?
                .get_json(&format!("/projects/{project_id}/executions/latest"))
                .await
                .map_err(|e| anyhow::anyhow!("no executions for project {project_id}: {e}"))?;
            serde_json::from_value::<ExecutionSummary>(resp)
                .context("read the project's latest execution")?
                .execution_id
                .to_string()
        }
    };

    let query = limit.map(|n| format!("?limit={n}")).unwrap_or_default();
    let mut answer = ctx.client()?.get_json(&format!("/executions/{execution_id}/logs{query}")).await?;
    // The limit that cut the tail comes back with it, so the notice
    // below is right whether the reader chose one or the dispatcher's
    // default applied.
    let logs: ExecutionLogs = serde_json::from_value(answer.clone()).context("read the run's log")?;
    let (arr, limit) = (&logs.lines, logs.limit as u64);
    // A run that wrote nothing usually did nothing, and the reason is
    // a skip: a required input closed, a gate said no. The skip is a
    // lifecycle event, not a log line, so it is fetched here rather
    // than sending the reader to `weft events --kind skipped` to learn
    // why nothing happened. Fetched before anything prints, so a
    // replay that cannot be read fails this command whole instead of
    // after a line that read as an answer; and `--json` carries the
    // same list under `skipped`, since an agent reads that shape.
    let skipped = if arr.is_empty() { skipped_nodes(&ctx, &execution_id).await? } else { Vec::new() };
    if arr.is_empty() {
        answer["skipped"] = skipped
            .iter()
            .map(|(node, reason)| serde_json::json!({ "node": node, "reason": reason }))
            .collect();
    }
    if ctx.json_out(&answer)? {
        return Ok(());
    }
    if arr.is_empty() {
        println!("(no logs: the run wrote no log lines and recorded no failure)");
        for (node, reason) in &skipped {
            println!("skipped {node}: {reason}");
        }
        return Ok(());
    }
    for entry in arr {
        let (level, msg) = (&entry.level, &entry.message);
        // The node column only for the lines that are about one
        // firing; a run-level line (the run failing) has none. Inside
        // a loop the iteration rides along (`llm#3:`), because two
        // hundred identical lines name nothing.
        let node = entry.node.as_ref().map(|n| format!(" {n}{}:", frames_suffix(&entry.frames))).unwrap_or_default();
        let inherited = entry
            .inherited_from
            .map(|execution_id| format!(" [inherited from {}]", super::versions::short(&execution_id.to_string())))
            .unwrap_or_default();
        println!("[{}] {level:>5}{node}{inherited} {msg}", local_time(entry.at_unix));
    }
    // The dispatcher answers the tail, so a full page means the run
    // may have written more than this; a cut log must never read as
    // the whole one.
    if arr.len() as u64 == limit {
        println!("(the last {limit} lines; the run may have written more, raise --limit to see them)");
    }
    Ok(())
}

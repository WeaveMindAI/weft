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
        Some(raw) if uuid::Uuid::parse_str(&raw).is_ok() => raw,
        Some(other) => {
            anyhow::bail!("expected a UUID color; got '{other}'. Run with no arg for the cwd project's latest.")
        }
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
    let logs: serde_json::Value =
        ctx.client().get_json(&format!("/executions/{color}/logs{query}")).await?;
    let arr = logs["lines"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("/executions/{color}/logs returned no lines: {logs}"))?;
    // The limit that cut the tail comes back with it, so the notice
    // below is right whether the reader chose one or the dispatcher's
    // default applied.
    let limit = logs["limit"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("/executions/{color}/logs returned no limit: {logs}"))?;
    if ctx.json() {
        println!("{logs}");
        return Ok(());
    }
    if arr.is_empty() {
        println!("(no logs: the run wrote no log lines and recorded no failure)");
        return Ok(());
    }
    for entry in arr {
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
        println!("[{}] {level:>5}{node} {msg}", local_time(at));
    }
    // The dispatcher answers the tail, so a full page means the run
    // may have written more than this; a cut log must never read as
    // the whole one.
    if arr.len() as u64 == limit {
        println!("(the last {limit} lines; the run may have written more, raise --limit to see them)");
    }
    Ok(())
}

//! `weft logs [color]`: print log lines the dispatcher has
//! journaled for an execution.
//!
//! - No argument: resolve the cwd project, fetch its most recent
//!   execution, show those logs.
//! - UUID argument: treat as color, show those logs.

use super::{resolve_project_id, Ctx};

pub async fn run(ctx: Ctx, target: Option<String>) -> anyhow::Result<()> {
    let color = match target {
        Some(raw) if uuid::Uuid::parse_str(&raw).is_ok() => raw,
        Some(other) => {
            anyhow::bail!("expected a UUID color; got '{other}'. Run with no arg for the cwd project's latest.")
        }
        None => {
            let project_id = resolve_project_id(None)?;
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

    let lines: serde_json::Value = ctx
        .client()
        .get_json(&format!("/executions/{color}/logs"))
        .await?;
    let Some(arr) = lines.as_array() else {
        println!("(no logs)");
        return Ok(());
    };
    if arr.is_empty() {
        println!("(no logs)");
        return Ok(());
    }
    for entry in arr {
        let level = entry.get("level").and_then(|v| v.as_str()).unwrap_or("info");
        let msg = entry.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let at = entry.get("at_unix").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("[{at:>10}] {level:>5} {msg}");
    }
    Ok(())
}

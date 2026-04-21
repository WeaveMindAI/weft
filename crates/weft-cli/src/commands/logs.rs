//! `weft logs <color>`: print the log lines the dispatcher has
//! journaled for an execution. Use `weft follow <color>` for live
//! updates.

use super::Ctx;

pub async fn run(ctx: Ctx, target: String) -> anyhow::Result<()> {
    // Phase A: we only have per-execution logs (keyed by color).
    // Project-level logs (aggregation across executions) arrive in
    // phase B; until then, accept only a color.
    if uuid::Uuid::parse_str(&target).is_err() {
        anyhow::bail!("expected an execution color (UUID). Project-wide log view is phase B.");
    }
    let lines: serde_json::Value = ctx
        .client()
        .get_json(&format!("/executions/{target}/logs"))
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

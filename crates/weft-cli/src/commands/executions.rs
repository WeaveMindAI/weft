//! `weft executions`, `weft events`, `weft clean`. Journal inspection
//! and cleanup. Graph view replay is an extension command; these are
//! the scripting surface.

use super::Ctx;

pub async fn list(ctx: Ctx, limit: u32) -> anyhow::Result<()> {
    let client = ctx.client();
    let resp: serde_json::Value = client
        .get_json(&format!("/executions?limit={limit}"))
        .await?;
    let Some(arr) = resp.as_array() else {
        println!("(no executions)");
        return Ok(());
    };
    if arr.is_empty() {
        println!("(no executions)");
        return Ok(());
    }
    println!(
        "{:<38} {:<38} {:<12} {:<20} {}",
        "color", "project_id", "status", "started_at", "entry_node"
    );
    for row in arr {
        let color = row.get("color").and_then(|v| v.as_str()).unwrap_or("?");
        let project = row.get("project_id").and_then(|v| v.as_str()).unwrap_or("?");
        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
        let entry = row.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?");
        println!("{color:<38} {project:<38} {status:<12} {started:<20} {entry}");
    }
    Ok(())
}

pub async fn events(ctx: Ctx, color: String) -> anyhow::Result<()> {
    let client = ctx.client();
    let resp: serde_json::Value = client
        .get_json(&format!("/executions/{color}/replay"))
        .await?;
    let Some(arr) = resp.as_array() else {
        println!("(no events)");
        return Ok(());
    };
    for row in arr {
        let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
        let node = row.get("node_id").and_then(|v| v.as_str()).unwrap_or("?");
        let at = row.get("at_unix").and_then(|v| v.as_u64()).unwrap_or(0);
        print!("[{at}] {kind:>9} {node}");
        if let Some(err) = row.get("error").and_then(|v| v.as_str()) {
            print!("  error={err}");
        }
        if let Some(output) = row.get("output") {
            if !output.is_null() {
                let summary = serde_json::to_string(output).unwrap_or_default();
                let trimmed = if summary.len() > 120 {
                    format!("{}...", &summary[..117])
                } else {
                    summary
                };
                print!("  output={trimmed}");
            }
        }
        println!();
    }
    Ok(())
}

pub async fn clean(
    ctx: Ctx,
    color: Option<String>,
    keep_days: u32,
    all: bool,
) -> anyhow::Result<()> {
    let client = ctx.client();
    if let Some(c) = color {
        client.delete(&format!("/executions/{c}")).await?;
        println!("deleted {c}");
        return Ok(());
    }

    // Bulk clean: list then delete those older than keep_days (or
    // all, if --all).
    let resp: serde_json::Value = client.get_json("/executions?limit=10000").await?;
    let arr = resp.as_array().cloned().unwrap_or_default();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let cutoff = if all {
        u64::MAX
    } else {
        now.saturating_sub(keep_days as u64 * 24 * 3600)
    };
    let mut count = 0usize;
    for row in arr {
        let Some(color) = row.get("color").and_then(|v| v.as_str()) else { continue };
        let started = row.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
        if all || started < cutoff {
            client.delete(&format!("/executions/{color}")).await?;
            count += 1;
        }
    }
    if all {
        println!("deleted {count} executions (all)");
    } else {
        println!("deleted {count} executions older than {keep_days}d");
    }
    Ok(())
}

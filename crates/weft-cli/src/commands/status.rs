//! `weft status`: discover the cwd project, hit the dispatcher's
//! `/projects/{id}/status` aggregator, print a human-readable
//! summary. Matches the user's plug-and-play UX: one command,
//! everything a user needs to know about their project.

use anyhow::Result;

use super::{resolve_project_id, Ctx};

pub async fn run(ctx: Ctx) -> Result<()> {
    let project_id = resolve_project_id(None)?;
    let data: serde_json::Value = ctx
        .client()
        .get_json(&format!("/projects/{project_id}/status"))
        .await?;

    let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("?");
    let status = data.get("status").and_then(|v| v.as_str()).unwrap_or("?");
    let listener = data
        .get("listener_running")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    println!("project: {name} ({project_id})");
    println!("  registration: {status}");
    println!("  listener: {}", if listener { "running" } else { "stopped" });

    if let Some(infra) = data.get("infra").and_then(|v| v.as_array()) {
        if infra.is_empty() {
            println!("  infra: (no nodes declare requires_infra)");
        } else {
            println!("  infra:");
            for entry in infra {
                let node = entry.get("node_id").and_then(|v| v.as_str()).unwrap_or("?");
                let st = entry.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                let url = entry
                    .get("endpoint_url")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                println!("    {node}: {st} ({url})");
            }
        }
    }

    if let Some(execs) = data.get("executions") {
        let total = execs.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("  executions: {total} total");
        let (Some(color), Some(status)) = (
            execs.get("last_color").and_then(|v| v.as_str()),
            execs.get("last_status").and_then(|v| v.as_str()),
        ) else {
            return Ok(());
        };
        let at = execs.get("last_completed_at").and_then(|v| v.as_u64());
        match at {
            Some(ts) => {
                let age = unix_now().saturating_sub(ts);
                println!("    last: {color} ({status}, completed {age}s ago)");
            }
            None => println!("    last: {color} ({status}, in flight)"),
        }
    }

    Ok(())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

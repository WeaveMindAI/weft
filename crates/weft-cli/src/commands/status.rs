//! `weft status`: discover the cwd project, compute current source
//! hashes, hit the dispatcher's `/projects/{id}/status` aggregator
//! with those hashes as query params (drives drift detection).
//! Print a human-readable summary or, with `--json`, emit the full
//! status payload as a single JSON line for consumption by the
//! VS Code extension's action bar.

use anyhow::Result;

use super::Ctx;

pub async fn run(ctx: Ctx) -> Result<()> {
    let project = ctx.project()?;
    let project_id = project.id().to_string();

    // Compute desired hashes from current source. The dispatcher
    // compares against project.running_source_hash and
    // project.running_infra_hash to decide which drift bits to set.
    let weft_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    // Both hashes are scoped to the compiled project's referenced /
    // infra-closure nodes, so both need the definition + catalog. If
    // the project can't compile, leave both desired hashes unset:
    // status is display-only and tolerates an in-progress project.
    let short = |h: String| crate::commands::build::short_hash(&h);
    let (desired_source, desired_infra) = match crate::hash::load_enriched_project(project) {
        Ok((def, catalog)) => (
            crate::hash::compute_source_hash(&def, &project.root, &weft_root, &catalog)
                .ok()
                .map(short),
            crate::hash::compute_infra_hash(&def, &project.root, &weft_root, &catalog)
                .ok()
                .map(short),
        ),
        Err(_) => (None, None),
    };

    let mut path = format!("/projects/{project_id}/status");
    let mut sep = '?';
    if let Some(h) = desired_source.as_deref() {
        path.push(sep);
        sep = '&';
        path.push_str("desired_source_hash=");
        path.push_str(h);
    }
    if let Some(h) = desired_infra.as_deref() {
        path.push(sep);
        path.push_str("desired_infra_hash=");
        path.push_str(h);
    }

    let data: serde_json::Value = ctx.client().get_json(&path).await?;

    if ctx.json() {
        // One JSON object on stdout; the extension reads it.
        println!("{data}");
        return Ok(());
    }

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
            print_drift(&data);
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
    print_drift(&data);

    Ok(())
}

fn print_drift(data: &serde_json::Value) {
    let drift = match data.get("drift") {
        Some(d) => d,
        None => return,
    };
    let infra = drift.get("infra_drift").and_then(|v| v.as_bool()).unwrap_or(false);
    let source = drift.get("source_drift").and_then(|v| v.as_bool()).unwrap_or(false);
    if !infra && !source {
        return;
    }
    println!("  drift:");
    if infra {
        println!("    infra: source has changed; click Upgrade to rebuild infra");
    }
    if source {
        println!("    source: source has changed; click Resync to re-register");
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs()
}

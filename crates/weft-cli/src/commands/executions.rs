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
    images: bool,
    build_cache: bool,
) -> anyhow::Result<()> {
    if images || build_cache {
        if images {
            clean_worker_images(all).await?;
        }
        if build_cache {
            clean_build_cache().await?;
        }
        return Ok(());
    }

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

/// Reclaim dangling worker images. Without `--all`, scoped to the
/// cwd project (whose id we read from `weft.toml`). With `--all`,
/// nukes every dangling image labelled `weft.dev/project=...`.
async fn clean_worker_images(all: bool) -> anyhow::Result<()> {
    use tokio::process::Command;
    let mut args: Vec<String> = vec![
        "image".into(),
        "prune".into(),
        "--force".into(),
        "--filter".into(),
        "dangling=true".into(),
        "--filter".into(),
    ];
    if all {
        args.push("label=weft.dev/project".into());
        println!("pruning dangling worker images for every project");
    } else {
        // cwd project. If we can't discover one, bail with a hint
        // instead of silently nuking everything.
        let cwd = std::env::current_dir()?;
        let project = weft_compiler::project::Project::discover(&cwd).map_err(|e| {
            anyhow::anyhow!(
                "no Weft project discovered from {} ({e}); pass --all to clean every project's images",
                cwd.display()
            )
        })?;
        args.push(format!("label=weft.dev/project={}", project.id()));
        println!(
            "pruning dangling worker images for project {} ({})",
            project.manifest.package.name,
            project.id()
        );
    }
    let status = Command::new("docker").args(&args).status().await?;
    if !status.success() {
        anyhow::bail!("docker image prune exited {status}");
    }
    Ok(())
}

/// `docker buildx prune` reclaims BuildKit's intermediate layers.
/// This is the heavy reclaim: cargo deps, intermediate Rust compile
/// state, etc. The next build will re-download deps and re-link.
async fn clean_build_cache() -> anyhow::Result<()> {
    use tokio::process::Command;
    println!("pruning docker BuildKit cache (next build will be slower)…");
    let status = Command::new("docker")
        .args(["buildx", "prune", "--force"])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker buildx prune exited {status}");
    }
    Ok(())
}

use super::Ctx;

pub async fn inspect(ctx: Ctx) -> anyhow::Result<()> {
    let client = ctx.client();
    let rows: serde_json::Value = client.get_json("/listener/inspect").await?;
    let arr = rows.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("no active listeners");
        return Ok(());
    }
    for row in arr {
        let tenant = row.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("?");
        let url = row.get("listener_url").and_then(|v| v.as_str()).unwrap_or("?");
        let journal = row
            .get("journal_signal_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let registry = row.get("listener_registry");

        let registry_count = registry
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let reachable = registry.map(|v| !v.is_null()).unwrap_or(false);

        println!("tenant: {tenant}");
        println!("  url:               {url}");
        println!("  journal signals:   {journal}");
        if reachable {
            println!("  listener signals:  {registry_count}");
        } else {
            println!("  listener signals:  (unreachable)");
        }
        if reachable && journal != registry_count as u64 {
            println!("  ⚠ DRIFT: journal and listener disagree.");
        }
        if reachable && registry_count == 0 && journal == 0 {
            println!("  (idle — should be reaped on next cleanup tick)");
        }
        if let Some(arr) = registry.and_then(|v| v.as_array()) {
            for sig in arr {
                let token = sig.get("token").and_then(|v| v.as_str()).unwrap_or("?");
                let node = sig.get("node_id").and_then(|v| v.as_str()).unwrap_or("?");
                let kind = sig.get("kind").map(|v| v.to_string()).unwrap_or_else(|| "?".into());
                println!("    - token={token}  node={node}  kind={kind}");
            }
        }
    }
    Ok(())
}

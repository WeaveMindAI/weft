//! `weft follow <target>`: subscribe to the dispatcher's SSE stream
//! and render live events for a project id or a specific execution
//! color.

use anyhow::Context;
use eventsource_client::Client;
use futures::StreamExt;

use super::Ctx;

pub async fn run(ctx: Ctx, target: String) -> anyhow::Result<()> {
    let client = ctx.client();
    // Target is a project id if it doesn't parse as a color; follow
    // either path on the dispatcher.
    let is_color = uuid::Uuid::parse_str(&target).is_ok();
    let _ = is_color;
    follow_target(&client, &target).await
}

pub async fn follow_color(client: &crate::client::DispatcherClient, color: &str) -> anyhow::Result<()> {
    let path = format!("/events/execution/{color}");
    follow_sse(client, &path).await
}

async fn follow_target(client: &crate::client::DispatcherClient, target: &str) -> anyhow::Result<()> {
    let path = if uuid::Uuid::parse_str(target).is_ok() {
        // Could be a project id OR a color; prefer execution stream
        // only if the dispatcher has an active execution. Simplest
        // rule: treat any valid uuid as a project id; users call
        // `weft follow <color>` explicitly when they want the
        // per-execution stream. (Future: dispatcher resolves the
        // ambiguity.)
        format!("/events/project/{target}")
    } else {
        format!("/events/project/{target}")
    };
    follow_sse(client, &path).await
}

async fn follow_sse(client: &crate::client::DispatcherClient, path: &str) -> anyhow::Result<()> {
    let url = format!("{}{}", client.base(), path);
    let es = eventsource_client::ClientBuilder::for_url(&url)
        .context("build sse client")?
        .build();
    let mut stream = es.stream();
    while let Some(ev) = stream.next().await {
        match ev {
            Ok(eventsource_client::SSE::Event(event)) => {
                println!("{}", format_event(&event.data));
                if is_terminal(&event.data) {
                    break;
                }
            }
            Ok(eventsource_client::SSE::Comment(_)) => {}
            Ok(eventsource_client::SSE::Connected(_)) => {}
            Err(e) => {
                eprintln!("sse error: {e}");
                break;
            }
        }
    }
    Ok(())
}

fn format_event(raw: &str) -> String {
    let value: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return format!("? {raw}"),
    };
    let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
    match kind {
        "execution_started" => format!(
            "→ started color={} entry={}",
            short(value.get("color")),
            value.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "execution_suspended" => format!(
            "… suspended at {} token={}",
            value.get("node").and_then(|v| v.as_str()).unwrap_or("?"),
            short(value.get("token"))
        ),
        "execution_completed" => format!("✓ completed color={}", short(value.get("color"))),
        "execution_failed" => format!(
            "✗ failed color={}: {}",
            short(value.get("color")),
            value.get("error").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "cost_reported" => format!(
            "$ {} +{:.4}",
            value.get("service").and_then(|v| v.as_str()).unwrap_or("?"),
            value.get("amount_usd").and_then(|v| v.as_f64()).unwrap_or(0.0),
        ),
        _ => format!("  {raw}"),
    }
}

fn short(v: Option<&serde_json::Value>) -> String {
    match v.and_then(|v| v.as_str()) {
        Some(s) if s.len() >= 8 => s[..8].to_string(),
        Some(s) => s.to_string(),
        None => "?".into(),
    }
}

fn is_terminal(raw: &str) -> bool {
    let Ok(value): Result<serde_json::Value, _> = serde_json::from_str(raw) else {
        return false;
    };
    matches!(
        value.get("kind").and_then(|v| v.as_str()),
        Some("execution_completed") | Some("execution_failed")
    )
}

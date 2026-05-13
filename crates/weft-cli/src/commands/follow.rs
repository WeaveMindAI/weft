//! `weft follow <project>`: subscribe to the dispatcher's SSE stream
//! for a project and render live events. The single-execution stream
//! is reached via `weft run`'s internal call to `follow_color` after
//! it kicks off a run; users don't address it directly.

use anyhow::Context;
use eventsource_client::Client;
use futures::StreamExt;

use super::Ctx;

pub async fn run(ctx: Ctx, project: String) -> anyhow::Result<()> {
    let client = ctx.client();
    follow_sse(&client, &format!("/events/project/{project}")).await
}

pub async fn follow_color(client: &crate::client::DispatcherClient, color: &str) -> anyhow::Result<()> {
    follow_sse(client, &format!("/events/execution/{color}")).await
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

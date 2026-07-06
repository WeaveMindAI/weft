//! Live two-way WebSocket: open a socket, exchange messages, assert echoes.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, live, project::Project};

#[tokio::test]
async fn websocket_echoes_with_turn_counter() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("live_chat", disp.clone()).await?;

    // Mount paths are namespaced per tenant; still use a per-run-unique one so
    // leftover projects / parallel runs of the SAME tenant never collide.
    // `unique_live_path` returns the tenant-namespaced callable path.
    let path = project.unique_live_path()?;

    // Live triggers must be activated (build + register + enable the endpoint).
    project.activate().await?;

    // Open the live socket and hold a two-way conversation. The echo node
    // replies to each message with { echo: <sent>, turn: <n> }.
    let mut ws = live::open_ws(&disp, &path).await?;
    let timeout = Duration::from_secs(20);

    let r1 = ws.request_json(&json!("hello"), timeout).await?;
    assert_eq!(r1.get("echo"), Some(&json!("hello")), "first echo: {r1}");
    assert_eq!(r1.get("turn"), Some(&json!(1)), "first turn: {r1}");

    let r2 = ws.request_json(&json!("again"), timeout).await?;
    assert_eq!(r2.get("echo"), Some(&json!("again")), "second echo: {r2}");
    assert_eq!(r2.get("turn"), Some(&json!(2)), "second turn: {r2}");

    ws.close().await?;
    project.finish().await
}

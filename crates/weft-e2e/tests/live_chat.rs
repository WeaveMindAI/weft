//! Live two-way WebSocket: open a socket, exchange messages, assert echoes.
//! Two echo nodes share the connection, so this also pins the broadcast
//! contract: every reader sees every inbound message.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::{json, Value};
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

    // Open the live socket and hold a two-way conversation. TWO echo nodes
    // read the same connection: inbound is BROADCAST to every reader, so each
    // sent message must come back twice (once per echo, in either order),
    // each reply carrying { echo: <sent>, turn: <n> } with that echo's own
    // turn counter.
    let mut ws = live::open_ws(&disp, &path).await?;
    let timeout = Duration::from_secs(20);

    for (round, msg) in [json!("hello"), json!("again")].iter().enumerate() {
        ws.send_json(msg).await?;
        for _ in 0..2 {
            let r: Value = ws.recv_json(timeout).await?;
            assert_eq!(r.get("echo"), Some(msg), "round {round} echo: {r}");
            assert_eq!(
                r.get("turn"),
                Some(&json!(round + 1)),
                "round {round} turn: {r}"
            );
        }
    }

    ws.close().await?;
    project.finish().await
}

//! A socket conversation with zero custom Rust: a Loop over the Socket's
//! `inbound` stream answers each message through Reply; a second socket
//! path closes on open through Close, with a code and a reason.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, live, project::Project};

#[tokio::test]
async fn a_loop_over_inbound_echoes_and_close_carries_its_code() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("socket_loop", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let timeout = Duration::from_secs(20);

    // One iteration per message, one Reply per iteration, in order.
    let mut ws = live::open_ws(&disp, &format!("{base}/chat")).await?;
    for text in ["hello", "again", "bye"] {
        let reply = ws.request_json(&json!({ "text": text }), timeout).await?;
        assert_eq!(reply, json!({ "echo": text, "len": text.len() }), "turn '{text}'");
    }
    ws.close().await?;

    // The other path: the program closes the socket itself, with its
    // own code and reason.
    let mut door = live::open_ws(&disp, &format!("{base}/closed")).await?;
    let (code, reason) = door.recv_close(timeout).await?;
    assert_eq!(code, 4002);
    assert_eq!(reason, "closed on open");

    project.finish().await
}

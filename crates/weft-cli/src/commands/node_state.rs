//! `weft node-state <run> <node>`: where one node of a run stands, the
//! same answer a program reads with `GetNodeState`. A node waiting on an
//! outside job shows the raw last answer its wait saw, as the service sent
//! it; one paused only because its run paused says so.

use weft_core::program::{NodeState, NodeStatus};
use weft_core::signal::listener_protocol::LastAnswer;

use super::Ctx;

pub async fn run(ctx: Ctx, run_id: String, node: String) -> anyhow::Result<()> {
    let run_id = super::resolve_run_id(&ctx, &run_id).await?;
    let encoded: String = url::form_urlencoded::byte_serialize(node.as_bytes()).collect();
    let answer = ctx.client()?.get_json(&format!("/runs/{run_id}/nodes/{encoded}/state")).await?;
    let state: NodeState = serde_json::from_value(answer).map_err(|e| anyhow::anyhow!("read the node's state: {e}"))?;
    if ctx.json_out(&state)? {
        return Ok(());
    }
    println!("{}", lines(&node, &state).join("\n"));
    Ok(())
}

/// What `weft node-state` prints: the status on the first line, then the
/// error or the last answer as they came.
fn lines(node: &str, state: &NodeState) -> Vec<String> {
    let mut out = vec![format!("{node}: {}", state.status.as_str())];
    if let Some(error) = &state.error {
        out.push(format!("error: {error}"));
    }
    match (&state.live, state.status) {
        (Some(LastAnswer::Kept(live)), _) => out.push(format!("last answer: {live}")),
        (Some(LastAnswer::TooLarge(bytes)), _) => out.push(format!("last answer: {bytes} bytes, too large for the wait to keep")),
        (None, NodeStatus::Waiting) => out.push("last answer: none yet".to_string()),
        (None, NodeStatus::Paused) => out.push("no wait of its own: its run paused with it inside, and it carries on when the run does".to_string()),
        (None, _) => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_prints_its_status_then_what_came_with_it() {
        let state = |status, live: Option<LastAnswer>, error: Option<&str>| NodeState { status, live, error: error.map(str::to_string) };
        let waiting = state(NodeStatus::Waiting, Some(LastAnswer::Kept("{\"status\":\"IN_QUEUE\"}".into())), None);
        assert_eq!(lines("render", &waiting), vec!["render: waiting", "last answer: {\"status\":\"IN_QUEUE\"}"]);
        let quiet = state(NodeStatus::Waiting, None, None);
        assert_eq!(lines("render", &quiet), vec!["render: waiting", "last answer: none yet"]);
        let large = state(NodeStatus::Waiting, Some(LastAnswer::TooLarge(70_000)), None);
        assert_eq!(lines("render", &large), vec!["render: waiting", "last answer: 70000 bytes, too large for the wait to keep"]);
        let paused = state(NodeStatus::Paused, None, None);
        assert_eq!(
            lines("read", &paused),
            vec!["read: paused", "no wait of its own: its run paused with it inside, and it carries on when the run does"]
        );
        let failed = state(NodeStatus::Failed, None, Some("boom"));
        assert_eq!(lines("one.render", &failed), vec!["one.render: failed", "error: boom"]);
        let done = state(NodeStatus::Completed, None, None);
        assert_eq!(lines("render", &done), vec!["render: completed"]);
    }
}

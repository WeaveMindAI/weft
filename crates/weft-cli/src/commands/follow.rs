//! `weft follow <project>`: subscribe to the dispatcher's SSE stream
//! for a project and render live events. The single-execution stream
//! is reached via `weft run`'s internal call to `follow_color` after
//! it kicks off a run; users don't address it directly.

use anyhow::Context;
use std::collections::HashSet;
use serde_json::Value;
use eventsource_client::Client;
use futures::StreamExt;

use super::Ctx;

#[derive(Clone, Copy)]
enum FollowTarget<'a> {
    Project(&'a str),
    Execution(&'a str),
}

impl FollowTarget<'_> {
    fn path(self) -> String {
        match self {
            Self::Project(id) => format!("/events/project/{id}"),
            Self::Execution(color) => format!("/events/execution/{color}"),
        }
    }

    fn finished(self, event: &Value) -> bool {
        matches!(self, Self::Execution(_)) && is_terminal(event)
    }
}

pub async fn run(ctx: Ctx, project: String) -> anyhow::Result<()> {
    let client = ctx.client();
    follow_sse(&client, FollowTarget::Project(&project), print_event).await
}

pub async fn follow_color(client: &crate::client::DispatcherClient, color: &str) -> anyhow::Result<()> {
    follow_sse(client, FollowTarget::Execution(color), print_event).await
}

fn print_event(event: &Value) {
    println!("{}", format_event(event));
}

async fn follow_sse(
    client: &crate::client::DispatcherClient,
    target: FollowTarget<'_>,
    mut emit: impl FnMut(&Value),
) -> anyhow::Result<()> {
    let url = format!("{}{}", client.base(), target.path());
    let es = eventsource_client::ClientBuilder::for_url(&url)
        .context("build sse client")?
        // Reconnecting without recovering history would hide lost updates.
        // A disconnected follower must tell the user it stopped watching.
        .reconnect(eventsource_client::ReconnectOptions::reconnect(false).build())
        .build();
    let mut stream = es.stream();
    // Only the finite initial history needs overlap detection. Do not grow
    // a seen-event cache for the lifetime of a potentially unbounded run.
    let mut history_ids = HashSet::new();
    while let Some(ev) = stream.next().await {
        match ev.context("live updates interrupted; the run may still be running. Inspect it with `weft executions` and `weft events <color>`")? {
            eventsource_client::SSE::Event(event) => {
                let event: Value = serde_json::from_str(&event.data).context("decode execution event")?;
                if history_ids.contains(event_identity(&event)?) { continue; }
                emit(&event);
                if target.finished(&event) {
                    return Ok(());
                }
            }
            eventsource_client::SSE::Comment(_) => {}
            eventsource_client::SSE::Connected(_) => {
                // The server is listening now. Recover everything that ran
                // before attachment, including an already-finished run.
                // New events remain buffered on the open stream meanwhile.
                if let FollowTarget::Execution(color) = target {
                    let history: Vec<serde_json::Value> = serde_json::from_value(
                        client.get_json(&format!("/executions/{color}/replay")).await?
                    ).context("read execution history")?;
                    for event in history {
                        if !history_ids.insert(event_identity(&event)?.to_owned()) { continue; }
                        emit(&event);
                        if target.finished(&event) {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    anyhow::bail!("live updates ended before following was finished; inspect the run with `weft executions` and `weft events <color>`")
}

fn event_identity(event: &Value) -> anyhow::Result<&str> {
    event.get("event_id").and_then(Value::as_str).filter(|id| !id.is_empty())
        .context("execution event has no delivery identity")
}

fn format_event(value: &Value) -> String {
    let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
    match kind {
        "execution_started" => format!(
            "→ started color={} entry={}",
            short(value.get("color")),
            value.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "node_suspended" => format!(
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
        // The reason says who stopped it: a person, or a sibling run's
        // `ctx.stop_tagged` naming the run and the tag.
        "execution_cancelled" => format!(
            "■ cancelled color={}: {}",
            short(value.get("color")),
            value.get("reason").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "cost_reported" => {
            let service = value.get("service").and_then(|v| v.as_str()).unwrap_or("?");
            match value.get("amount_usd").and_then(|v| v.as_f64()) {
                Some(amount) => format!("$ {service} +{amount:.4}"),
                None => format!("$ {service} cost unknown"),
            }
        }
        // Every other event (a node starting, completing, skipping, a
        // loop turning) renders as the same compact line `weft events`
        // prints, so the live stream and the replay read alike and a
        // node's output is a summary rather than a raw JSON row.
        _ => format!("  {}", crate::commands::executions::event_line(value, false)),
    }
}

fn short(v: Option<&serde_json::Value>) -> String {
    match v.and_then(|v| v.as_str()) {
        Some(s) => s.chars().take(8).collect(),
        None => "?".into(),
    }
}

/// Whether an SSE event ends the followed execution. A cancel is a
/// terminal too. Project-wide follow keeps watching subsequent executions.
// SYNC: is_terminal (SSE kind list) <-> crates/weft-journal/src/events.rs ExecEvent::is_execution_terminal
fn is_terminal(value: &Value) -> bool {
    matches!(
        value.get("kind").and_then(|v| v.as_str()),
        Some("execution_completed") | Some("execution_failed") | Some("execution_cancelled")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use axum::{response::sse::{Event, Sse}, routing::get, Json, Router};
    use futures::stream;
    use serde_json::{json, Value};
    use crate::client::DispatcherClient;

    struct Server {
        client: DispatcherClient,
        requests: Arc<Mutex<Vec<&'static str>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Server {
        fn drop(&mut self) { self.task.abort(); }
    }

    async fn server(history: Vec<Value>, live: Vec<Value>, stay_open: bool) -> Server {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stream_requests = requests.clone();
        let history_requests = requests.clone();
        let app = Router::new()
            .route("/events/{scope}/{id}", get(move || {
                stream_requests.lock().unwrap().push("subscribe");
                let live = live.clone();
                async move {
                    let ready = stream::once(async { Ok::<_, Infallible>(Event::default().comment("ready")) });
                    let events = stream::iter(live.into_iter().map(|v| Ok(Event::default().data(v.to_string()))));
                    let tail = stream::pending().take(if stay_open { 1 } else { 0 });
                    Sse::new(ready.chain(events).chain(tail))
                }
            }))
            .route("/executions/{color}/replay", get(move || {
                history_requests.lock().unwrap().push("history");
                let history = history.clone();
                async move { Json(history) }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DispatcherClient::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        Server { client, requests, task }
    }

    weft_core::stress_test!(
        name: follow_recovers_a_run_that_finished_before_listening,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let terminal = json!({ "event_id": "done", "kind": "execution_completed", "color": "a" });
            let server = server(vec![terminal.clone()], vec![], true).await;
            let mut seen = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Execution("a"), |raw| seen.push(raw.to_owned()),
            )).await.expect("completed history must finish follow").unwrap();
            assert_eq!(seen, vec![terminal]);
            assert_eq!(*server.requests.lock().unwrap(), vec!["subscribe", "history"]);
        }
    );

    weft_core::stress_test!(
        name: follow_applies_history_before_buffered_live_completion,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let started = json!({ "event_id": "start", "kind": "execution_started", "color": "a" });
            let terminal = json!({ "event_id": "done", "kind": "execution_completed", "color": "a" });
            let server = server(vec![started.clone()], vec![terminal.clone()], true).await;
            let mut seen = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Execution("a"), |raw| seen.push(raw.to_owned()),
            )).await.expect("live completion must finish follow").unwrap();
            assert_eq!(seen, vec![started, terminal]);
        }
    );

    weft_core::stress_test!(
        name: project_follow_keeps_watching_after_a_run_finishes,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let events = vec![
                json!({ "event_id": "done:a", "kind": "execution_completed", "color": "a" }),
                json!({ "event_id": "done:b", "kind": "execution_cancelled", "color": "b" }),
            ];
            let server = server(vec![], events.clone(), false).await;
            let mut seen = Vec::new();
            let err = tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Project("p"), |raw| seen.push(raw.to_owned()),
            )).await.expect("a closed stream must stop following").unwrap_err();
            assert!(err.to_string().contains("live updates"), "{err}");
            assert_eq!(seen, events);
            assert_eq!(*server.requests.lock().unwrap(), vec!["subscribe"]);
        }
    );

    weft_core::stress_test!(
        name: follow_deduplicates_replay_overlap_without_merging_distinct_events,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let first = json!({ "event_id": "row:1", "kind": "node_started", "node": "n", "color": "a" });
            let second = json!({ "event_id": "row:2", "kind": "node_started", "node": "n", "color": "a" });
            let terminal = json!({ "event_id": "row:3", "kind": "execution_completed", "color": "a" });
            let server = server(vec![first.clone()], vec![first.clone(), second.clone(), terminal.clone()], true).await;
            let mut seen = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Execution("a"), |event| seen.push(event.clone()),
            )).await.expect("completed follow must return").unwrap();
            assert_eq!(seen, vec![first, second, terminal]);
        }
    );

    #[test]
    fn events_without_identity_are_a_contract_error() {
        assert!(event_identity(&json!({ "kind": "node_started" })).is_err());
        assert!(event_identity(&json!({ "event_id": "" })).is_err());
    }

    #[test]
    fn unknown_cost_is_not_reported_as_free() {
        let line = format_event(&json!({"kind":"cost_reported","service":"billing","amount_usd":null}));
        assert_eq!(line, "$ billing cost unknown");
        let line = format_event(&json!({"kind":"cost_reported","service":"billing","amount_usd":0.0}));
        assert_eq!(line, "$ billing +0.0000");
    }

    #[test]
    fn shortened_identifiers_do_not_split_characters() {
        assert_eq!(short(Some(&json!("ééééééééé"))), "éééééééé");
    }
}

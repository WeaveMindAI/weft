//! `weft follow <project>`: subscribe to the dispatcher's SSE stream
//! for a project and render live events. The single-execution stream
//! is reached via `weft run`'s internal call to `follow_execution_id` after
//! it kicks off a run; users don't address it directly.

use anyhow::Context;
use std::collections::HashSet;
use serde_json::Value;
use eventsource_client::Client;
use futures::StreamExt;
use weft_core::live_event::LiveEvent;

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
            Self::Execution(execution_id) => format!("/events/execution/{execution_id}"),
        }
    }

    fn finished(self, event: &LiveEvent) -> bool {
        matches!(self, Self::Execution(_)) && event.event.is_execution_terminal()
    }
}

pub async fn run(ctx: Ctx, project: String) -> anyhow::Result<()> {
    let client = ctx.client()?;
    follow_sse(&client, FollowTarget::Project(&project), |event| {
        print_event(&serde_json::to_value(event)?);
        Ok(())
    })
    .await
}

/// Follow one run to its end, each event printed the way the program
/// reads (`one.strip`, `gate`) when the project's definition is at
/// hand; ids otherwise.
pub async fn follow_execution_id(client: &crate::client::DispatcherClient, execution_id: &str, definition: Option<&weft_core::ProjectDefinition>) -> anyhow::Result<()> {
    follow_sse(client, FollowTarget::Execution(execution_id), |event| {
        let row = serde_json::to_value(event)?;
        match definition {
            Some(definition) => if let Some(spelled) = super::executions::spell_node(row, definition) { print_event(&spelled) },
            None => print_event(&row),
        }
        Ok(())
    }).await
}

fn print_event(event: &Value) {
    println!("{}", format_event(event));
}

async fn follow_sse(
    client: &crate::client::DispatcherClient,
    target: FollowTarget<'_>,
    mut emit: impl FnMut(&LiveEvent) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let es = client
        .event_stream(&target.path())?
        // Reconnecting without recovering history would hide lost updates.
        // A disconnected follower must tell the user it stopped watching.
        .reconnect(eventsource_client::ReconnectOptions::reconnect(false).build())
        .build();
    let mut stream = es.stream();
    // Only the finite initial history needs overlap detection. Do not grow
    // a seen-event cache for the lifetime of a potentially unbounded run.
    let mut history_ids = HashSet::new();
    while let Some(ev) = stream.next().await {
        match ev.context("live updates interrupted; the run may still be running. Inspect it with `weft executions` and `weft events <execution_id>`")? {
            eventsource_client::SSE::Event(event) => {
                let event: LiveEvent = serde_json::from_str(&event.data).context("decode execution event")?;
                if history_ids.contains(event_identity(&event)?) { continue; }
                emit(&event)?;
                if target.finished(&event) {
                    return Ok(());
                }
            }
            eventsource_client::SSE::Comment(_) => {}
            eventsource_client::SSE::Connected(_) => {
                // The server is listening now. Recover everything that ran
                // before attachment, including an already-finished run.
                // New events remain buffered on the open stream meanwhile.
                if let FollowTarget::Execution(execution_id) = target {
                    for event in super::versions::replay_rows(client, execution_id).await? {
                        if !history_ids.insert(event_identity(&event)?.to_owned()) { continue; }
                        emit(&event)?;
                        if target.finished(&event) {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    anyhow::bail!("live updates ended before following was finished; inspect the run with `weft executions` and `weft events <execution_id>`")
}

fn event_identity(event: &LiveEvent) -> anyhow::Result<&str> {
    Some(event.event_id.as_str()).filter(|id| !id.is_empty())
        .context("execution event has no delivery identity")
}

fn format_event(value: &Value) -> String {
    let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
    match kind {
        "execution_started" => format!(
            "→ started execution={} entry={}",
            short(value.get("execution_id")),
            value.get("entry_node").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "node_suspended" => format!(
            "… suspended at {} token={}",
            value.get("node").and_then(|v| v.as_str()).unwrap_or("?"),
            short(value.get("token"))
        ),
        "execution_completed" => format!("✓ completed execution={}", short(value.get("execution_id"))),
        "execution_failed" => format!(
            "✗ failed execution={}: {}",
            short(value.get("execution_id")),
            value.get("error").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        // The reason says who stopped it: a person, or a sibling run's
        // `ctx.stop_tagged` naming the run and the tag.
        "execution_cancelled" => format!(
            "■ cancelled execution={}: {}",
            short(value.get("execution_id")),
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
            .route("/executions/{execution_id}/replay", get(move || {
                history_requests.lock().unwrap().push("history");
                let history = history.clone();
                async move { Json(history) }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DispatcherClient::new(format!("http://{}", listener.local_addr().unwrap()), None);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        Server { client, requests, task }
    }

    /// One event as the dispatcher sends it: `kind`'s own fields, and the
    /// ones every run event carries.
    fn event(id: &str, kind: &str, run: u128) -> Value {
        let mut fields = match kind {
            "execution_started" => json!({ "entry_node": "n", "phase": "fire" }),
            "execution_completed" => json!({ "outputs": {} }),
            "execution_cancelled" => json!({ "reason": "stopped" }),
            "node_started" => json!({ "node": "n", "frames": [], "input": {}, "closed_ports": [] }),
            other => panic!("no fixture for {other}"),
        };
        let object = fields.as_object_mut().unwrap();
        object.insert("event_id".into(), json!(id));
        object.insert("kind".into(), json!(kind));
        object.insert("execution_id".into(), json!(uuid::Uuid::from_u128(run)));
        object.insert("project_id".into(), json!(uuid::Uuid::nil()));
        object.insert("at_unix".into(), json!(1));
        fields
    }

    fn ids(seen: &[LiveEvent]) -> Vec<String> {
        seen.iter().map(|e| e.event_id.clone()).collect()
    }

    weft_core::stress_test!(
        name: follow_recovers_a_run_that_finished_before_listening,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let terminal = event("done", "execution_completed", 1);
            let server = server(vec![terminal], vec![], true).await;
            let mut seen = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Execution("a"), |raw| { seen.push(raw.clone()); Ok(()) },
            )).await.expect("completed history must finish follow").unwrap();
            assert_eq!(ids(&seen), vec!["done"]);
            assert_eq!(*server.requests.lock().unwrap(), vec!["subscribe", "history"]);
        }
    );

    weft_core::stress_test!(
        name: follow_applies_history_before_buffered_live_completion,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let started = event("start", "execution_started", 1);
            let terminal = event("done", "execution_completed", 1);
            let server = server(vec![started], vec![terminal], true).await;
            let mut seen = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Execution("a"), |raw| { seen.push(raw.clone()); Ok(()) },
            )).await.expect("live completion must finish follow").unwrap();
            assert_eq!(ids(&seen), vec!["start", "done"]);
        }
    );

    weft_core::stress_test!(
        name: project_follow_keeps_watching_after_a_run_finishes,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let events = vec![event("done:a", "execution_completed", 1), event("done:b", "execution_cancelled", 2)];
            let server = server(vec![], events, false).await;
            let mut seen = Vec::new();
            let err = tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Project("p"), |raw| { seen.push(raw.clone()); Ok(()) },
            )).await.expect("a closed stream must stop following").unwrap_err();
            assert!(err.to_string().contains("live updates"), "{err}");
            assert_eq!(ids(&seen), vec!["done:a", "done:b"]);
            assert_eq!(*server.requests.lock().unwrap(), vec!["subscribe"]);
        }
    );

    weft_core::stress_test!(
        name: follow_deduplicates_replay_overlap_without_merging_distinct_events,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let first = event("row:1", "node_started", 1);
            let second = event("row:2", "node_started", 1);
            let terminal = event("row:3", "execution_completed", 1);
            let server = server(vec![first.clone()], vec![first, second, terminal], true).await;
            let mut seen = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), follow_sse(
                &server.client, FollowTarget::Execution("a"), |event| { seen.push(event.clone()); Ok(()) },
            )).await.expect("completed follow must return").unwrap();
            assert_eq!(ids(&seen), vec!["row:1", "row:2", "row:3"]);
        }
    );

    #[test]
    fn events_without_identity_are_a_contract_error() {
        let anonymous: LiveEvent = serde_json::from_value(event("", "node_started", 1)).unwrap();
        assert!(event_identity(&anonymous).is_err());
        let unmarked = { let mut e = event("x", "node_started", 1); e.as_object_mut().unwrap().remove("event_id"); e };
        assert!(serde_json::from_value::<LiveEvent>(unmarked).is_err(), "an event without its identity does not decode");
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

//! `weft follow <project>`: subscribe to the dispatcher's SSE stream
//! for a project and render live events. The single-run stream
//! is reached via `weft run`'s internal call to `follow_run_id` after
//! it kicks off a run, and by `weft wait`, which reads the same stream
//! for its outcome instead of printing it.

use anyhow::Context;
use std::ops::ControlFlow;
use serde_json::Value;
use futures::StreamExt;
use weft_core::live_event::{DispatcherEvent, EventPosition, IdentifiedEvent};

use super::wait::Outcome;
use super::Ctx;

#[derive(Clone, Copy)]
enum FollowTarget<'a> {
    Project(&'a str),
    Run(&'a str),
}

/// How long a follower whose stream dropped waits before opening it again.
const REOPEN_AFTER: std::time::Duration = std::time::Duration::from_secs(1);

/// The longest a follower waits between two attempts at opening a dropped
/// stream again.
const REOPEN_CEILING: std::time::Duration = std::time::Duration::from_secs(30);

/// How long to wait before the next attempt at opening a dropped stream
/// again, after `failed` attempts that failed since it dropped:
/// [`REOPEN_AFTER`] doubling each time, up to [`REOPEN_CEILING`].
fn reopen_wait(failed: u32) -> std::time::Duration {
    REOPEN_AFTER.saturating_mul(2u32.saturating_pow(failed)).min(REOPEN_CEILING)
}

/// The SSE event a run's stream sends once its history is sent; what
/// follows is live. Nothing here waits for it.
// SYNC: CAUGHT_UP <-> crates/weft-dispatcher/src/api/events.rs CAUGHT_UP, extension-vscode/src/runFollower.ts CAUGHT_UP
const CAUGHT_UP: &str = "caught_up";

pub async fn run(ctx: Ctx, project: String) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let never: std::convert::Infallible = watch_sse::<Value, _>(&client, ctx.on(), FollowTarget::Project(&project), |event| {
        print_event(&serde_json::to_value(event)?);
        Ok(ControlFlow::Continue(()))
    })
    .await?;
    match never {}
}

/// Follow one run to its end, each event printed the way the program
/// reads (`one.strip`, `gate`) when the project's definition is at
/// hand; ids otherwise, its values written in. `on` is the target the
/// command was run `--on`, which the hints carry. Answers how the run
/// ended.
pub async fn follow_run_id(
    client: &crate::client::DispatcherClient,
    on: Option<&str>,
    run_id: &str,
    definition: Option<&weft_core::ProjectDefinition>,
) -> anyhow::Result<Outcome> {
    // Indexed once, so spelling each row is a lookup, not a scan.
    let graph = definition.map(weft_core::project::graph::ProjectGraph::new);
    watch_sse::<Value, _>(client, on, FollowTarget::Run(run_id), |event| {
        let row = serde_json::to_value(event)?;
        match &graph {
            Some(graph) => {
                if let Some(row) = weft_core::live_event::spelled::spell_row(row, graph) {
                    print_event(&row);
                }
            }
            None => print_event(&row),
        }
        Ok(match Outcome::ended(&event.event)? {
            Some(outcome) => ControlFlow::Break(outcome),
            None => ControlFlow::Continue(()),
        })
    }).await
}

/// Watch one run's events, its history first and then the live ones,
/// until `watch` breaks with what it was watching for. Its values come by
/// reference: a watcher that prints them follows with [`follow_run_id`].
pub async fn watch_run<T>(
    client: &crate::client::DispatcherClient,
    on: Option<&str>,
    run_id: &str,
    watch: impl FnMut(&weft_core::live_event::LiveEvent) -> anyhow::Result<ControlFlow<T>>,
) -> anyhow::Result<T> {
    watch_sse(client, on, FollowTarget::Run(run_id), watch).await
}

fn print_event(event: &Value) {
    println!("{}", format_event(event));
}

/// The stream of `target`: a run's from its start, or past `after` (the
/// last event of it this follower holds); a project's from now. `V` is how
/// its events hold their values, written in for `serde_json::Value`.
fn stream_path<V: Values>(target: FollowTarget<'_>, after: Option<EventPosition>) -> String {
    let mut query = Vec::new();
    if V::WRITTEN {
        query.push("with_values=true".to_string());
    }
    let path = match target {
        FollowTarget::Project(id) => format!("/events/project/{id}"),
        FollowTarget::Run(run_id) => {
            if let Some(after) = after {
                query.push(format!("after={after}"));
            }
            format!("/events/run/{run_id}")
        }
    };
    match query.as_slice() {
        [] => path,
        _ => format!("{path}?{}", query.join("&")),
    }
}

/// How an event a follower reads holds its values: by reference
/// (`weft_core::record_ref::ValueRef`), or written in.
pub trait Values: serde::de::DeserializeOwned + serde::Serialize {
    const WRITTEN: bool;
}

impl Values for weft_core::record_ref::ValueRef {
    const WRITTEN: bool = false;
}

impl Values for Value {
    const WRITTEN: bool = true;
}

/// Follow `target` until `watch` breaks. A run's stream gives its history
/// then its live events, each once: when it drops, it is opened again past
/// the last event read, so nothing is missed or read twice. A project's
/// stream has no place to go on from, so a follower that falls behind
/// it, or loses it, says what it may have missed and listens again. Only
/// the first open is fatal: once the stream opened, an attempt at opening
/// it again that finds the install out of reach
/// ([`super::ensure::look_again_after`]: its connection failing, its
/// gateway saying it is down or starting) is said on stderr and tried
/// again, [`reopen_wait`] apart, for as long as the follow lasts; anything
/// else the install answers (the run is gone, the key is refused) is
/// raised, since waiting does not lift it.
async fn watch_sse<V: Values, T>(
    client: &crate::client::DispatcherClient,
    on: Option<&str>,
    target: FollowTarget<'_>,
    mut watch: impl FnMut(&IdentifiedEvent<DispatcherEvent<V>>) -> anyhow::Result<ControlFlow<T>>,
) -> anyhow::Result<T> {
    let inspect = || {
        format!(
            "inspect the run with `{}` and `{}`",
            super::weft_on(on, "runs"),
            super::weft_on(on, "events <run-id>")
        )
    };
    let mut after: Option<EventPosition> = None;
    let mut opened_once = false;
    // Attempts at opening the stream again that failed since it dropped.
    let mut failed = 0u32;
    loop {
        let stream = match client.event_stream(&stream_path::<V>(target, after)).await {
            Ok(stream) => stream,
            Err(e) if !opened_once => return Err(e.context(format!("listen for live updates; {}", inspect()))),
            Err(e) => {
                let why = super::ensure::look_again_after(e).with_context(|| format!("listen for live updates again; {}", inspect()))?;
                failed = failed.saturating_add(1);
                let wait = reopen_wait(failed);
                eprintln!("could not listen for live updates again ({why}); still trying, next attempt in {}s", wait.as_secs());
                tokio::time::sleep(wait).await;
                continue;
            }
        };
        opened_once = true;
        failed = 0;
        let mut stream = std::pin::pin!(stream);
        let dropped = loop {
            let Some(ev) = stream.next().await else { break None };
            let ev = match ev {
                Ok(ev) => ev,
                Err(e) => break Some(e),
            };
            if ev.event == CAUGHT_UP {
                continue;
            }
            let event: IdentifiedEvent<DispatcherEvent<V>> = serde_json::from_str(&ev.data).context("decode a live event")?;
            if let Some(at) = event.position() {
                after = Some(at);
            }
            if let ControlFlow::Break(done) = watch(&event)? {
                return Ok(done);
            }
        };
        match target {
            // A run's stream ends on its own only once its ending went
            // out, which the watch breaks on: anything else (a gateway
            // cutting a long answer, the connection dropping) goes on
            // from the last event read.
            FollowTarget::Run(_) => eprintln!(
                "live updates dropped ({}); listening again from the last event read",
                dropped.map_or_else(|| "the stream ended".to_string(), |e| format!("{e:#}"))
            ),
            FollowTarget::Project(_) => eprintln!(
                "live updates dropped ({}); listening again, and what happened meanwhile is not shown here: {}",
                dropped.map_or_else(|| "the stream ended".to_string(), |e| format!("{e:#}")),
                inspect()
            ),
        }
        tokio::time::sleep(reopen_wait(0)).await;
    }
}

fn format_event(value: &Value) -> String {
    let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
    match kind {
        "run_started" => format!(
            "→ started run={} entry={}",
            short(value.get("runId")),
            value.get("entryNode").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "node_suspended" => format!(
            "… suspended at {} token={}",
            value.get("node").and_then(|v| v.as_str()).unwrap_or("?"),
            short(value.get("token"))
        ),
        "run_completed" => format!("✓ completed run={}", short(value.get("runId"))),
        "run_failed" => format!(
            "✗ failed run={}: {}",
            short(value.get("runId")),
            value.get("error").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        // The reason says who stopped it: a person, or a sibling run's
        // `ctx.stop_tagged` naming the run and the tag.
        "run_cancelled" => format!(
            "■ cancelled run={}: {}",
            short(value.get("runId")),
            value.get("reason").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        "cost_reported" => {
            let service = value.get("service").and_then(|v| v.as_str()).unwrap_or("?");
            match value.get("amountUsd").and_then(|v| v.as_f64()) {
                Some(amount) => format!("$ {service} +{amount:.4}"),
                None => format!("$ {service} cost unknown"),
            }
        }
        // Every other event (a node starting, completing, skipping, a
        // loop turning) renders as the same compact line `weft events`
        // prints, so the live stream and the replay read alike and a
        // node's output is a summary rather than a raw JSON row.
        _ => format!("  {}", crate::commands::runs::event_line(value, false)),
    }
}

fn short(v: Option<&serde_json::Value>) -> String {
    match v.and_then(|v| v.as_str()) {
        Some(s) => s.chars().take(8).collect(),
        None => "?".into(),
    }
}

/// How long to wait before looking again at a task when the install could
/// not be reached, or answered that it cannot serve right now.
pub(crate) const TASK_LOOK_AGAIN_AFTER: std::time::Duration = std::time::Duration::from_secs(3);

/// Follow task `task` of project `project_id` (a resync, a deactivate's
/// wait for its runs) until it ends, and answer what it ended with
/// ([`follow_at`] on `GET /projects/{id}/follow/{task}`). `going` hears
/// where the task says it is, in its kind's own shape, which only the
/// caller knows.
pub async fn follow_task(
    client: &crate::client::DispatcherClient,
    progress: &crate::progress::Progress,
    project_id: &str,
    task: uuid::Uuid,
    going: impl AsyncFnMut(Option<Value>) -> anyhow::Result<()>,
) -> anyhow::Result<Value> {
    follow_at(client, &format!("/projects/{project_id}/follow/{task}"), TASK_LOOK, |message| progress.warn(message), going).await
}

/// How long one look at a task waits on the install when it says nothing
/// along the way.
pub const TASK_LOOK: std::time::Duration = std::time::Duration::from_secs(25);

/// Follow the task `follow` looks at (a `.../follow/{task}` route) until it
/// ends, and answer what it ended with: one short look after another, each
/// waiting at most `look` on the install (half a minute at most), so
/// following never rests on one long request a gateway would cut, and
/// never ends on its own. `going` hears where the task says it is after
/// each look that finds it still going. A look the install could not
/// answer (unreachable, restarting, its gateway cutting it) is said and
/// tried again; a refusal is the task's own ending, raised; `warn` says a
/// look that failed. Ctrl+C stops following, never the task.
pub async fn follow_at(
    client: &crate::client::DispatcherClient,
    follow: &str,
    look: std::time::Duration,
    warn: impl Fn(&str),
    mut going: impl AsyncFnMut(Option<Value>) -> anyhow::Result<()>,
) -> anyhow::Result<Value> {
    let path = format!("{follow}?wait_ms={}", look.as_millis());
    let client = client.waiting_up_to(look);
    loop {
        match client.get_json(&path).await {
            Ok(answer) => match crate::client::read_answer(answer, "the task's state")? {
                weft_core::deactivation::Following::Going(progress) => going(progress).await?,
                weft_core::deactivation::Following::Done(value) => return Ok(value),
            },
            Err(e) => {
                let why = super::ensure::look_again_after(e)?;
                warn(&format!("could not look at it just now ({why}); looking again"));
                tokio::time::sleep(TASK_LOOK_AGAIN_AFTER).await;
            }
        }
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
        /// The query of every stream opened, in order.
        requests: Arc<Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Server {
        fn drop(&mut self) { self.task.abort(); }
    }

    /// A dispatcher whose stream answers its `n`th opening with
    /// `connections[n]`, and ends it there.
    async fn server(connections: Vec<Vec<Value>>) -> Server {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let opened = requests.clone();
        let app = Router::new().route(
            "/events/{scope}/{id}",
            get(move |uri: axum::http::Uri| {
                let n = {
                    let mut opened = opened.lock().unwrap();
                    opened.push(uri.query().unwrap_or_default().to_string());
                    opened.len() - 1
                };
                let events = connections.get(n).cloned().unwrap_or_default();
                async move {
                    let events = stream::iter(events.into_iter().map(|v| Ok::<_, Infallible>(Event::default().data(v.to_string()))));
                    Sse::new(events)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DispatcherClient::new(format!("http://{}", listener.local_addr().unwrap()), None);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        Server { client, requests, task }
    }

    /// One event as the dispatcher sends it: `kind`'s own fields, and the
    /// ones every run event carries.
    fn event(id: &str, kind: &str, run: u128) -> Value {
        let mut fields = match kind {
            "run_started" => json!({ "entryNode": "n", "phase": "fire" }),
            "run_completed" => json!({ "outputs": {} }),
            "run_cancelled" => json!({ "reason": "stopped" }),
            "run_failed" => json!({ "error": "this run fails on purpose" }),
            "node_started" => json!({ "node": "n", "frames": [], "input": { "in": 7 }, "closedPorts": [] }),
            other => panic!("no fixture for {other}"),
        };
        let object = fields.as_object_mut().unwrap();
        object.insert("eventId".into(), json!(id));
        object.insert("kind".into(), json!(kind));
        object.insert("runId".into(), json!(uuid::Uuid::from_u128(run)));
        object.insert("projectId".into(), json!(uuid::Uuid::nil()));
        object.insert("atUnix".into(), json!(1));
        fields
    }

    /// Every event's id into `seen`, stopping at a run's ending the way
    /// `follow_run_id` does, or once `stop` events were seen.
    async fn collect(client: &DispatcherClient, target: FollowTarget<'_>, stop: usize, seen: &mut Vec<String>) -> anyhow::Result<()> {
        watch_sse::<Value, _>(client, None, target, |event| {
            seen.push(event.event_id.to_string());
            let ended = matches!(target, FollowTarget::Run(_)) && Outcome::ended(&event.event)?.is_some();
            Ok(if ended || seen.len() == stop { ControlFlow::Break(()) } else { ControlFlow::Continue(()) })
        })
        .await
    }

    weft_core::stress_test!(
        name: a_run_follower_reads_the_history_and_the_live_events_off_one_stream,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let server = server(vec![vec![event("journal:0:0:0", "run_started", 1), event("journal:1:0:0", "run_completed", 1)]]).await;
            let mut seen = Vec::new();
            collect(&server.client, FollowTarget::Run("a"), usize::MAX, &mut seen).await.unwrap();
            assert_eq!(seen, vec!["journal:0:0:0", "journal:1:0:0"]);
            assert_eq!(*server.requests.lock().unwrap(), vec!["with_values=true"], "the values come written in");
        }
    );

    /// `weft run` follows its run to the end and exits the way `weft
    /// wait` does: a run that failed ends the follow in the failed
    /// outcome, whose exit code is not zero, and one that completed in
    /// the completed one.
    #[tokio::test]
    async fn a_followed_run_ends_in_how_the_run_ended() {
        let failed = server(vec![vec![event("journal:0:0:0", "run_started", 1), event("journal:1:0:0", "run_failed", 1)]]).await;
        let outcome = follow_run_id(&failed.client, None, "a", None).await.unwrap();
        assert_eq!(outcome, Outcome::Failed);
        assert_ne!(outcome.code(), 0);
        let completed = server(vec![vec![event("journal:0:0:0", "run_started", 1), event("journal:1:0:0", "run_completed", 1)]]).await;
        assert_eq!(follow_run_id(&completed.client, None, "a", None).await.unwrap(), Outcome::Completed);
    }

    weft_core::stress_test!(
        name: a_run_follower_whose_stream_drops_goes_on_past_the_last_event_it_read,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let server = server(vec![
                vec![event("journal:0:0:0", "run_started", 1), event("journal:3:1:2", "node_started", 1)],
                vec![event("journal:4:0:0", "run_completed", 1)],
            ])
            .await;
            let mut seen = Vec::new();
            collect(&server.client, FollowTarget::Run("a"), usize::MAX, &mut seen).await.unwrap();
            assert_eq!(seen, vec!["journal:0:0:0", "journal:3:1:2", "journal:4:0:0"]);
            assert_eq!(*server.requests.lock().unwrap(), vec!["with_values=true", "with_values=true&after=3:1:2"]);
        }
    );

    weft_core::stress_test!(
        name: a_project_follower_listens_again_after_its_stream_ends,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let server = server(vec![
                vec![event("live:00000000-0000-0000-0000-000000000001", "run_completed", 1), event("journal:2:0:0", "run_cancelled", 2)],
                vec![event("journal:5:0:0", "run_started", 3)],
            ])
            .await;
            let mut seen = Vec::new();
            collect(&server.client, FollowTarget::Project("p"), 3, &mut seen).await.unwrap();
            assert_eq!(seen.len(), 3, "a run finishing ends nothing: {seen:?}");
            assert_eq!(*server.requests.lock().unwrap(), vec!["with_values=true", "with_values=true"], "a project's stream has no place to go on from");
        }
    );

    /// A dispatcher whose `n`th connection is `connections[n]`: `Some`
    /// answers the stream with those events and ends it, `None` hangs up
    /// before answering, the way a restarting install's connection fails.
    async fn flaky_server(connections: Vec<Option<Vec<Value>>>) -> (DispatcherClient, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DispatcherClient::new(format!("http://{}", listener.local_addr().unwrap()), None);
        let task = tokio::spawn(async move {
            for connection in connections {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).await.unwrap();
                    head.push(byte[0]);
                }
                let line = String::from_utf8_lossy(&head).lines().next().unwrap_or_default().to_string();
                seen.lock().unwrap().push(line.split(' ').nth(1).and_then(|path| path.split_once('?')).map(|(_, q)| q.to_string()).unwrap_or_default());
                let Some(events) = connection else { continue };
                let body: String = events.iter().map(|v| format!("data: {v}\n\n")).collect();
                let answer = format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}");
                socket.write_all(answer.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        (client, requests, task)
    }

    #[test]
    fn a_dropped_stream_is_tried_again_further_apart_up_to_a_ceiling() {
        assert_eq!(reopen_wait(0), REOPEN_AFTER);
        assert_eq!(reopen_wait(1), REOPEN_AFTER * 2);
        assert_eq!(reopen_wait(2), REOPEN_AFTER * 4);
        assert_eq!(reopen_wait(10), REOPEN_CEILING);
        assert_eq!(reopen_wait(u32::MAX), REOPEN_CEILING);
    }

    /// A reopen that fails is said and tried again, from the last event
    /// read; it never ends the follow.
    #[tokio::test]
    async fn a_run_follower_keeps_trying_when_opening_its_dropped_stream_again_fails() {
        let (client, requests, task) = flaky_server(vec![
            Some(vec![event("journal:0:0:0", "run_started", 1), event("journal:3:1:2", "node_started", 1)]),
            None,
            Some(vec![event("journal:4:0:0", "run_completed", 1)]),
        ])
        .await;
        let mut seen = Vec::new();
        collect(&client, FollowTarget::Run("a"), usize::MAX, &mut seen).await.unwrap();
        task.abort();
        assert_eq!(seen, vec!["journal:0:0:0", "journal:3:1:2", "journal:4:0:0"]);
        assert_eq!(
            *requests.lock().unwrap(),
            vec!["with_values=true", "with_values=true&after=3:1:2", "with_values=true&after=3:1:2"]
        );
    }

    /// Before the stream ever opened there is nothing to go on from: the
    /// first open failing ends the follow with why.
    #[tokio::test]
    async fn a_run_follower_whose_first_open_fails_ends() {
        let (client, _requests, task) = flaky_server(vec![None]).await;
        let mut seen = Vec::new();
        let err = collect(&client, FollowTarget::Run("a"), usize::MAX, &mut seen).await.unwrap_err();
        task.abort();
        assert!(format!("{err:#}").contains("listen for live updates"), "{err:#}");
        assert!(seen.is_empty());
    }

    #[test]
    fn an_event_without_its_identity_does_not_decode() {
        let unmarked = { let mut e = event("x", "node_started", 1); e.as_object_mut().unwrap().remove("eventId"); e };
        assert!(serde_json::from_value::<IdentifiedEvent<DispatcherEvent<Value>>>(unmarked).is_err(), "an event without its identity does not decode");
        assert!(serde_json::from_value::<IdentifiedEvent<DispatcherEvent<Value>>>(event("", "node_started", 1)).is_err(), "nor one with an empty one");
    }

    #[test]
    fn unknown_cost_is_not_reported_as_free() {
        let line = format_event(&json!({"kind":"cost_reported","service":"billing","amountUsd":null}));
        assert_eq!(line, "$ billing cost unknown");
        let line = format_event(&json!({"kind":"cost_reported","service":"billing","amountUsd":0.0}));
        assert_eq!(line, "$ billing +0.0000");
    }

    #[test]
    fn shortened_identifiers_do_not_split_characters() {
        assert_eq!(short(Some(&json!("ééééééééé"))), "éééééééé");
    }

    /// A task is followed look after look until it ends, where it says it
    /// is heard on each look that finds it going, a look the install could
    /// not answer (its gateway cutting it) is tried again, and what it
    /// ended with is the answer.
    #[tokio::test]
    async fn a_task_is_followed_in_short_looks_until_it_ends() {
        let looks = Arc::new(Mutex::new(0u32));
        let seen = looks.clone();
        let app = Router::new().route(
            "/projects/{id}/follow/{task}",
            get(move || {
                let seen = seen.clone();
                async move {
                    let look = {
                        let mut looks = seen.lock().unwrap();
                        *looks += 1;
                        *looks
                    };
                    match look {
                        1 => (axum::http::StatusCode::OK, Json(json!({ "going": { "at": "building" } }))),
                        2 => (axum::http::StatusCode::GATEWAY_TIMEOUT, Json(json!({ "error": "cut" }))),
                        _ => (axum::http::StatusCode::OK, Json(json!({ "done": { "moved": 2 } }))),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DispatcherClient::new(format!("http://{addr}"), None);
        let mut heard = Vec::new();
        let follow = follow_at(&client, "/projects/p/follow/t", Duration::from_millis(10), |_| {}, async |progress| {
            heard.push(progress);
            Ok(())
        });
        let done = follow.await.unwrap();
        assert_eq!(done, json!({ "moved": 2 }));
        assert_eq!(heard, [Some(json!({ "at": "building" }))]);
        assert_eq!(*looks.lock().unwrap(), 3, "going, cut and looked at again, done");
    }
}

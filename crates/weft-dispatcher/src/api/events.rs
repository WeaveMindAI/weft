//! Server-Sent Events endpoints. Clients (CLI `weft follow`, the VS
//! Code extension's right sidebar, the ops dashboard) subscribe to
//! a per-project stream. The dispatcher's EventBus (in-memory
//! broadcast channel per project) is the source.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

pub async fn project_stream(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.events.subscribe_project(&id).await;
    let stream = BroadcastStream::new(rx)
        .filter_map(|res| res.ok())
        .map(to_sse);
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

pub async fn execution_stream(
    State(state): State<DispatcherState>,
    Path(color): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Execution SSE: we don't index events by color alone (a color
    // belongs to a project). Resolve project_id via the journal's
    // execution row, then subscribe to the project's bus but filter
    // for this color.
    let target_color = color.parse::<uuid::Uuid>().ok();
    let project_id = state
        .journal
        .execution_project(target_color.unwrap_or_else(uuid::Uuid::nil))
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let rx = state.events.subscribe_project(&project_id).await;
    let stream = BroadcastStream::new(rx)
        .filter_map(|res| res.ok())
        .filter(move |event| matches!(event.color(), Some(c) if Some(c) == target_color))
        .map(to_sse);
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn to_sse(event: DispatcherEvent) -> Result<Event, Infallible> {
    let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
    Ok(Event::default().data(payload))
}

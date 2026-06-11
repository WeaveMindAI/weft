//! Server-Sent Events endpoints. Clients (CLI `weft follow`, the VS
//! Code extension's right sidebar, the ops dashboard) subscribe to
//! a per-project stream. The dispatcher's EventBus (in-memory
//! broadcast channel per project) is the source.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
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
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    // Execution SSE: we don't index events by color alone (a color
    // belongs to a project). Resolve project_id via the journal's
    // execution row, then subscribe to the project's bus but filter
    // for this color.
    //
    // A bad color string is 400; a journal lookup failure is 500;
    // an unknown color (no execution row) is 404. The pre-Result
    // shape papered over all three with empty-string project_id,
    // which silently routed events into a phantom bucket.
    let target_color: uuid::Uuid = color
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let project_id = match state
        .journal
        .execution_project(target_color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        crate::journal::ColorLookup::Found(p) => p,
        crate::journal::ColorLookup::NotFound => return Err(StatusCode::NOT_FOUND),
        // Corrupt journal row (logged loud at the decode site): a
        // server-side defect, not a missing execution.
        crate::journal::ColorLookup::Corrupt => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let target_color = Some(target_color);
    let rx = state.events.subscribe_project(&project_id).await;
    let stream = BroadcastStream::new(rx)
        .filter_map(|res| res.ok())
        .filter(move |event| matches!(event.color(), Some(c) if Some(c) == target_color))
        .map(to_sse);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

fn to_sse(event: DispatcherEvent) -> Result<Event, Infallible> {
    // `DispatcherEvent` is a plain enum over `String` / typed enums /
    // `Value`. Serialization is infallible; the previous
    // `unwrap_or_else(|_| "{}".into())` was dead defensive code that
    // would silently ship a `{}`-shaped event the extension would
    // never parse. Assert the invariant so a future variant that
    // breaks it fails loud.
    let payload = serde_json::to_string(&event).expect("DispatcherEvent serializes");
    Ok(Event::default().data(payload))
}

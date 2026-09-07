//! Server-Sent Events endpoints. Clients (CLI `weft follow`, the VS
//! Code extension's right sidebar) subscribe to
//! a per-project stream. The dispatcher's EventBus (in-memory
//! broadcast channel per project) is the source.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use tokio::sync::broadcast;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::LiveEvent;
use crate::state::DispatcherState;

pub async fn project_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, broadcast::error::RecvError>>>, StatusCode> {
    let project_id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    authorize_project(&state, &caller.0, project_id)
        .await
        .map_err(|(s, _)| s)?;
    let rx = state.events.subscribe_project(&id).await;
    let stream = live_events(rx, None);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

pub async fn execution_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(color): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, broadcast::error::RecvError>>>, StatusCode> {
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
    // Resolve + tenant-gate in the ONE place that owns "who owns this
    // execution": an unknown or cross-tenant color is 404 either way,
    // and the project id rides back for the stream's attribution.
    let project_id = crate::authenticator::authorize_execution(&*state.journal, &caller.0, target_color)
        .await
        .map_err(|(s, _)| s)?
        .project_id;

    let rx = state.events.subscribe_project(&project_id).await;
    let stream = live_events(rx, Some(target_color));
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

fn live_events(
    rx: broadcast::Receiver<LiveEvent>,
    color: Option<uuid::Uuid>,
) -> impl Stream<Item = Result<Event, broadcast::error::RecvError>> {
    futures::stream::unfold(Some(rx), move |rx| async move {
        let mut rx = rx?;
        loop {
            match rx.recv().await {
                Ok(event) if color.is_none() || event.event.color() == color => {
                    return Some((Ok(to_sse(event)), Some(rx)));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(err) => {
                    // Dropped updates invalidate the live view. End this
                    // subscription with an error so clients show the loss
                    // instead of continuing with an incomplete history.
                    tracing::warn!(%err, ?color, "ending event stream after lost updates");
                    return Some((Err(err), None));
                }
            }
        }
    })
}

fn to_sse(event: LiveEvent) -> Event {
    // `DispatcherEvent` is a plain enum over `String` / typed enums /
    // `Value`. Serialization is infallible; the previous
    // `unwrap_or_else(|_| "{}".into())` was dead defensive code that
    // would silently ship a `{}`-shaped event the extension would
    // never parse. Assert the invariant so a future variant that
    // breaks it fails loud.
    let payload = serde_json::to_string(&event).expect("DispatcherEvent serializes");
    Event::default().data(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use crate::events::{DispatcherEvent, IdentifiedEvent};

    fn completed(color: uuid::Uuid) -> LiveEvent {
        IdentifiedEvent::transient(DispatcherEvent::ExecutionCompleted {
            color, project_id: "p".into(), outputs: serde_json::json!({}), at_unix: 1,
        })
    }

    weft_core::stress_test!(
        name: lagged_followers_fail_instead_of_silently_skipping_updates,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            for filter in [None, Some(uuid::Uuid::new_v4())] {
                let (tx, rx) = broadcast::channel(1);
                let color = uuid::Uuid::new_v4();
                tx.send(completed(color)).unwrap();
                tx.send(completed(color)).unwrap();
                let stream = live_events(rx, filter);
                futures::pin_mut!(stream);
                assert!(matches!(stream.next().await, Some(Err(broadcast::error::RecvError::Lagged(1)))));
                assert!(stream.next().await.is_none());
            }
        }
    );

    weft_core::stress_test!(
        name: execution_stream_filters_other_colors_without_dropping_its_own,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, rx) = broadcast::channel(4);
            let color = uuid::Uuid::new_v4();
            tx.send(completed(uuid::Uuid::new_v4())).unwrap();
            tx.send(completed(color)).unwrap();
            drop(tx);
            let events = live_events(rx, Some(color)).collect::<Vec<_>>().await;
            assert_eq!(events.len(), 1);
            assert!(events[0].is_ok());
        }
    );
}

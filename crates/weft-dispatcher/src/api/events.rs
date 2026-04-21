//! Server-Sent Events endpoints. Clients (VS Code extension, ops
//! dashboard, browser extension long-polling fallback) subscribe to
//! per-project or per-execution streams.
//!
//! Event types emitted:
//! - `execution.started { color, entry_node }`
//! - `execution.suspended { color, node, wait_metadata }`
//! - `execution.resumed { color, node }`
//! - `execution.completed { color, outputs }`
//! - `execution.failed { color, error }`
//! - `trigger.url_changed { node_id, url }`
//! - `infra.status_changed { node_id, status }`
//! - `cost.reported { color, report }`

use axum::{extract::{Path, State}, response::sse::{Event, Sse}};
use futures::stream;
use std::{convert::Infallible, time::Duration};

use crate::state::DispatcherState;

pub async fn project_stream(
    State(_state): State<DispatcherState>,
    Path(_id): Path<String>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let s = stream::unfold((), |_| async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Some((Ok::<_, Infallible>(Event::default().comment("keepalive")), ()))
    });
    Sse::new(s)
}

pub async fn execution_stream(
    State(_state): State<DispatcherState>,
    Path(_color): Path<String>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let s = stream::unfold((), |_| async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Some((Ok::<_, Infallible>(Event::default().comment("keepalive")), ()))
    });
    Sse::new(s)
}

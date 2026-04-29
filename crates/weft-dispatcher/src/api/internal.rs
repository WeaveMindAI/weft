//! `/internal/*` routes for Pod-to-Pod control. Every route here
//! requires the shared `x-weft-internal-secret` header. Callers go
//! through `crate::routing::forward_to_pod*`; the secret is added
//! there.
//!
//! Each handler does the in-RAM-only work for its Pod (sending on
//! a live WS, accepting a Deliver, etc). Journal-side work is
//! already done by the originating Pod before forwarding.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::primitive::{Delivery, DispatcherToWorker};
use weft_core::Color;

use crate::routing::require_internal_secret;
use crate::slots::Slot;
use crate::state::DispatcherState;

#[derive(Debug, Deserialize, Serialize)]
pub struct DeliverColorRequest {
    pub color: String,
    pub token: String,
    pub value: Value,
}

/// Forward a Deliver to the worker on this Pod. The originating
/// Pod (which received the fire) has already journalled
/// `SuspensionResolved`; we just push the value into the slot.
pub async fn deliver_color(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<DeliverColorRequest>,
) -> Result<StatusCode, StatusCode> {
    require_internal_secret(&state, &headers)?;
    let color: Color = req
        .color
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let sender = state
        .slots
        .with_slot(color, |slot| {
            Box::pin(async move {
                match slot {
                    Slot::Live { sender, .. } => Some(sender.clone()),
                    Slot::StalledGrace { sender, .. } => Some(sender.clone()),
                    _ => None,
                }
            })
        })
        .await;
    let Some(sender) = sender else {
        return Err(StatusCode::CONFLICT);
    };
    let _ = sender
        .send(DispatcherToWorker::Deliver(Delivery {
            token: req.token,
            value: req.value,
        }))
        .await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CancelColorRequest {
    pub color: String,
}

/// Send Cancel to the worker on this Pod. Non-journal side; the
/// originating Pod already journalled the cancellation.
pub async fn cancel_color(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<CancelColorRequest>,
) -> Result<StatusCode, StatusCode> {
    require_internal_secret(&state, &headers)?;
    let color: Color = req
        .color
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let (sender, worker_handle) = state
        .slots
        .with_slot(color, |slot| {
            Box::pin(async move {
                match slot {
                    Slot::Live { sender, .. } => (Some(sender.clone()), None),
                    Slot::StalledGrace { sender, worker, .. } => {
                        (Some(sender.clone()), worker.take())
                    }
                    Slot::Starting { worker, .. } => (None, worker.take()),
                    Slot::WaitingReconnect { worker, .. } => (None, worker.take()),
                    Slot::Idle { .. } => (None, None),
                }
            })
        })
        .await;
    if let Some(handle) = worker_handle {
        let _ = state.workers.kill_worker(handle).await;
    }
    if let Some(sender) = sender {
        let _ = sender.send(DispatcherToWorker::Cancel).await;
    }
    state.slots.drop_slot(color).await;
    Ok(StatusCode::NO_CONTENT)
}

//! Engine-side `ContextHandle`. Routes all node-facing primitives
//! (`await_signal`, `report_cost`, `log`) through the worker's
//! `DispatcherLink` (WebSocket). `ship_node_event` is kept as a
//! helper used by the loop driver for lifecycle events.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Notify;

use weft_core::context::{ContextHandle, LogLevel};
use weft_core::error::{WeftError, WeftResult};
use weft_core::primitive::{CostReport, WakeSignalSpec, WorkerToDispatcher};
use weft_core::Color;

use crate::dispatcher_link::DispatcherLink;

pub struct RunnerHandle {
    execution_id: String,
    project_id: String,
    color: Color,
    node_id: String,
    node_lane: weft_core::lane::Lane,
    link: Option<DispatcherLink>,
    cancellation: Arc<Notify>,
    /// On a resume, the loop driver sets this to the token the
    /// snapshotted suspension registered with the dispatcher. When
    /// the re-run of the node calls `await_signal`, the handle skips
    /// the `SuspensionRequest` round-trip and goes straight to
    /// `wait_for_delivery(expected_token)`, which returns the
    /// pre-seeded delivery value immediately.
    expected_token: Option<String>,
}

impl RunnerHandle {
    pub fn new(
        execution_id: String,
        project_id: String,
        color: Color,
        node_id: String,
        node_lane: weft_core::lane::Lane,
        link: Option<DispatcherLink>,
        cancellation: Arc<Notify>,
    ) -> Self {
        Self {
            execution_id,
            project_id,
            color,
            node_id,
            node_lane,
            link,
            cancellation,
            expected_token: None,
        }
    }

    pub fn with_expected_token(mut self, token: Option<String>) -> Self {
        self.expected_token = token;
        self
    }

    fn require_link(&self) -> WeftResult<&DispatcherLink> {
        self.link
            .as_ref()
            .ok_or_else(|| WeftError::Suspension("dispatcher link required for await_signal".into()))
    }
}

/// Ship a node lifecycle event (started / completed / failed /
/// skipped) to the dispatcher over the WS link. Called by the loop
/// driver, not by node code. No-op when link is absent (detached
/// tests).
/// Send a node lifecycle event to the dispatcher. Queues the
/// message into the link's outbound mpsc; the supervisor's writer
/// forwards it to the WS. Awaits enqueue (not socket-write) so
/// the caller's control flow guarantees ordering: if you call
/// ship_node_event(Started) then ship_node_event(Completed), the
/// dispatcher sees them in that order.
pub async fn ship_node_event(
    link: Option<&DispatcherLink>,
    _color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    kind: &'static str,
    input: Option<&serde_json::Value>,
    output: Option<&serde_json::Value>,
    error: Option<&str>,
    pulses_absorbed: &[uuid::Uuid],
) {
    let Some(link) = link else { return };
    let lane_str = serde_json::to_string(lane).unwrap_or_default();
    let msg = WorkerToDispatcher::NodeEvent {
        node_id: node_id.to_string(),
        lane: lane_str,
        event: kind.to_string(),
        input: input.cloned(),
        output: output.cloned(),
        error: error.map(|s| s.to_string()),
        pulses_absorbed: pulses_absorbed.iter().map(|u| u.to_string()).collect(),
    };
    link.send(msg).await;
}

#[async_trait]
impl ContextHandle for RunnerHandle {
    async fn await_signal(&self, spec: WakeSignalSpec) -> WeftResult<Value> {
        let link = self.require_link()?;

        // Resume path: this run's Start carried a `seeded_deliveries`
        // set listing every token the dispatcher promises a value
        // for. If this node's expected token is in that set, wait
        // on the delivery (it's either already in `Ready` or will
        // arrive as a `Deliver` WS frame momentarily). If not, the
        // fire simply hasn't happened yet for this lane; return
        // `Suspended` so the loop can stall after all other lanes
        // finish.
        if let Some(existing) = &self.expected_token {
            if link.has_seeded_delivery(existing).await {
                let value = link
                    .wait_for_delivery(existing.clone())
                    .await
                    .map_err(|e| WeftError::Suspension(format!("wait delivery: {e}")))?;
                return Ok(value);
            }
            // expected_token known but no fire queued for this
            // lane: fall through to Suspended so the loop stalls.
            // On re-dispatch after the fire, the resume path above
            // will see the seeded delivery and take it.
            return Err(WeftError::Suspended {
                token: existing.clone(),
            });
        }

        // Fresh suspension: register with the dispatcher, mint a
        // token + URL, then return `Suspended` so the loop driver
        // marks this execution WaitingForInput. The worker stalls
        // (via `link.stall()`) when every lane is parked; no socket
        // is held open across the human's response time.
        let reply = link
            .request_suspension(self.node_id.clone(), self.node_lane.clone(), spec)
            .await
            .map_err(|e| WeftError::Suspension(format!("request token: {e}")))?;
        tracing::info!(
            target: "weft_engine::suspend",
            node = %self.node_id,
            color = %self.color,
            token = %reply.token,
            url = reply.user_url.as_deref().unwrap_or(""),
            "await_signal: registered; returning Suspended",
        );
        Err(WeftError::Suspended { token: reply.token })
    }

    fn report_cost(&self, report: CostReport) {
        let Some(link) = &self.link else {
            tracing::info!(
                target: "weft_engine::cost",
                exec = %self.execution_id,
                project = %self.project_id,
                service = %report.service,
                model = ?report.model,
                amount_usd = report.amount_usd,
                "cost reported (detached, not journaled)"
            );
            return;
        };
        let link = link.clone();
        tokio::spawn(async move {
            link.send(WorkerToDispatcher::Cost(report)).await;
        });
    }

    fn log(&self, level: LogLevel, message: String) {
        let level_str = match level {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };
        tracing::info!(
            target: "weft_engine::node",
            exec = %self.execution_id,
            level = level_str,
            "{message}"
        );
        if let Some(link) = &self.link {
            let link = link.clone();
            let payload = WorkerToDispatcher::Log {
                level: level_str.to_string(),
                message,
            };
            tokio::spawn(async move {
                link.send(payload).await;
            });
        }
    }

    fn is_cancelled(&self) -> bool {
        false
    }

    fn cancellation(&self) -> Arc<Notify> {
        self.cancellation.clone()
    }
}

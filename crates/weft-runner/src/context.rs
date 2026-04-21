//! Runner-side ContextHandle. The in-process side of the language
//! primitives. When the dispatcher is present, primitives that
//! require journaling (await_form, await_timer, report_cost) route
//! through it. In detached mode (no dispatcher URL) those primitives
//! error immediately: a pure program has no business calling them.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Notify;

use weft_core::context::{ContextHandle, LogLevel};
use weft_core::error::{WeftError, WeftResult};
use weft_core::primitive::{CostReport, FormSchema, FormSubmission, SubgraphRef};
use weft_core::Color;

pub struct RunnerHandle {
    execution_id: String,
    project_id: String,
    color: Color,
    dispatcher: Option<String>,
    cancellation: Arc<Notify>,
    http: reqwest::Client,
}

impl RunnerHandle {
    pub fn new(
        execution_id: String,
        project_id: String,
        color: Color,
        dispatcher: Option<String>,
        cancellation: Arc<Notify>,
    ) -> Self {
        Self {
            execution_id,
            project_id,
            color,
            dispatcher,
            cancellation,
            http: reqwest::Client::new(),
        }
    }

    fn require_dispatcher(&self) -> WeftResult<&str> {
        self.dispatcher
            .as_deref()
            .ok_or_else(|| WeftError::Suspension("dispatcher required for suspension primitives".into()))
    }
}

#[async_trait]
impl ContextHandle for RunnerHandle {
    async fn await_form(&self, _schema: FormSchema) -> WeftResult<FormSubmission> {
        let _dispatcher = self.require_dispatcher()?;
        // Phase A2: POST to dispatcher /suspensions, receive a token,
        // exit the worker. The dispatcher serves the form URL,
        // receives the submission, wakes a new runner with the
        // resume value; that runner resumes here via replay.
        //
        // For now: unimplemented so the primitive is callable but
        // returns an error. End-to-end scaffold ports this next.
        Err(WeftError::Suspension(
            "await_form: dispatcher integration not yet implemented".into(),
        ))
    }

    async fn await_timer(&self, _duration: std::time::Duration) -> WeftResult<()> {
        let _dispatcher = self.require_dispatcher()?;
        Err(WeftError::Suspension(
            "await_timer: dispatcher integration not yet implemented".into(),
        ))
    }

    async fn await_callback(&self, _subgraph: SubgraphRef, _input: Value) -> WeftResult<Value> {
        Err(WeftError::Suspension(
            "await_callback: not yet implemented".into(),
        ))
    }

    fn report_cost(&self, report: CostReport) {
        let Some(dispatcher) = self.dispatcher.as_deref() else {
            tracing::info!(
                target: "weft_runner::cost",
                exec = %self.execution_id,
                project = %self.project_id,
                service = %report.service,
                model = ?report.model,
                amount_usd = report.amount_usd,
                "cost reported (detached, not journaled)"
            );
            return;
        };

        // Fire-and-forget HTTP POST to dispatcher. We do NOT await;
        // the node execution continues while the report lands.
        let url = format!("{dispatcher}/executions/{}/cost", self.color);
        let http = self.http.clone();
        tokio::spawn(async move {
            if let Err(e) = http.post(&url).json(&report).send().await {
                tracing::warn!(target: "weft_runner::cost", error = %e, "cost report failed to send");
            }
        });
    }

    fn log(&self, level: LogLevel, message: String) {
        match level {
            LogLevel::Trace => tracing::trace!(target: "weft_runner::node", exec = %self.execution_id, "{message}"),
            LogLevel::Debug => tracing::debug!(target: "weft_runner::node", exec = %self.execution_id, "{message}"),
            LogLevel::Info => tracing::info!(target: "weft_runner::node", exec = %self.execution_id, "{message}"),
            LogLevel::Warn => tracing::warn!(target: "weft_runner::node", exec = %self.execution_id, "{message}"),
            LogLevel::Error => tracing::error!(target: "weft_runner::node", exec = %self.execution_id, "{message}"),
        }
    }

    fn is_cancelled(&self) -> bool {
        // Notify doesn't expose a non-blocking check directly; if the
        // caller is polling this, they can inspect their own future
        // state. We return false and let explicit cancellation
        // propagate via `cancellation()`.
        false
    }

    fn cancellation(&self) -> Arc<Notify> {
        self.cancellation.clone()
    }
}

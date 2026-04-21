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
    node_id: String,
    dispatcher: Option<String>,
    cancellation: Arc<Notify>,
    http: reqwest::Client,
}

impl RunnerHandle {
    pub fn new(
        execution_id: String,
        project_id: String,
        color: Color,
        node_id: String,
        dispatcher: Option<String>,
        cancellation: Arc<Notify>,
    ) -> Self {
        Self {
            execution_id,
            project_id,
            color,
            node_id,
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
    async fn await_form(&self, schema: FormSchema) -> WeftResult<FormSubmission> {
        let dispatcher = self.require_dispatcher()?.to_string();
        let url = format!("{dispatcher}/executions/{}/suspensions", self.color);
        let body = serde_json::json!({
            "node_id": self.node_id,
            "project_id": self.project_id,
            "metadata": {
                "kind": "form",
                "schema": schema,
            }
        });

        #[derive(serde::Deserialize)]
        struct Resp {
            token: String,
            form_url: String,
        }

        let resp: Resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| WeftError::Suspension(format!("journal POST: {e}")))?
            .error_for_status()
            .map_err(|e| WeftError::Suspension(format!("journal status: {e}")))?
            .json()
            .await
            .map_err(|e| WeftError::Suspension(format!("journal json: {e}")))?;

        tracing::info!(
            target: "weft_runner::suspend",
            node = %self.node_id,
            color = %self.color,
            form_url = %resp.form_url,
            "await_form suspension recorded; worker exiting"
        );

        // Signal to the loop driver: "I'm suspended, wrap up and
        // exit." The driver catches this variant, marks the
        // execution WaitingForInput, then returns cleanly. A future
        // worker spawn with resume_value seeds the form submission
        // downstream of this node.
        Err(WeftError::Suspended { token: resp.token })
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

use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Notify;

use crate::error::{WeftError, WeftResult};
use crate::lane::Lane;
use crate::primitive::{CostReport, FormSchema, FormSubmission, SubgraphRef};
use crate::Color;

/// The per-execution context handed to `Node::execute`. Exposes the
/// language's primitive surface (await_form, await_timer,
/// await_callback, report_cost, log) plus read helpers for config and
/// input.
///
/// ExecutionContext is constructed by the runtime (inside the user's
/// compiled binary) and passed to each node invocation. It holds an
/// `Arc<dyn ContextHandle>` which abstracts the actual runtime
/// implementation; the scaffolded runtime provides a concrete handle,
/// but the trait allows alternative implementations for testing.
#[derive(Clone)]
pub struct ExecutionContext {
    pub execution_id: String,
    pub project_id: String,
    pub node_id: String,
    pub node_type: String,
    /// The node's user-facing label (the title shown at the top of
    /// the node in the editor). `None` when the user hasn't named
    /// the node; runtime callers decide whether to fall back to
    /// node_id or omit the label entirely.
    pub node_label: Option<String>,
    pub color: Color,
    pub lane: Lane,
    pub config: ConfigBag,
    pub input: InputBag,
    handle: Arc<dyn ContextHandle>,
}

impl ExecutionContext {
    pub fn new(
        execution_id: String,
        project_id: String,
        node_id: String,
        node_type: String,
        node_label: Option<String>,
        color: Color,
        lane: Lane,
        config: ConfigBag,
        input: InputBag,
        handle: Arc<dyn ContextHandle>,
    ) -> Self {
        Self { execution_id, project_id, node_id, node_type, node_label, color, lane, config, input, handle }
    }

    // ----- Suspension primitives --------------------------------------

    /// Wait for a form submission. Framework mints a token, journals
    /// the suspension, returns a URL the caller (or the caller's
    /// parent) can surface to humans. Worker may exit while waiting;
    /// on submit, a new worker resumes here with the submission.
    pub async fn await_form(&self, schema: FormSchema) -> WeftResult<FormSubmission> {
        self.handle.await_form(schema).await
    }

    /// Wait for a duration. Framework schedules a durable timer; on
    /// fire, a new worker resumes here.
    pub async fn await_timer(&self, duration: std::time::Duration) -> WeftResult<()> {
        self.handle.await_timer(duration).await
    }

    /// Invoke a callback subgraph synchronously. Framework runs the
    /// subgraph under the current color (isolated sub-region), returns
    /// its output.
    pub async fn await_callback(&self, subgraph: SubgraphRef, input: Value) -> WeftResult<Value> {
        self.handle.await_callback(subgraph, input).await
    }

    // ----- Fire-and-forget primitives --------------------------------

    /// Report a cost attributable to this execution. Journaled by the
    /// runtime, aggregated by the dispatcher.
    pub fn report_cost(&self, report: CostReport) {
        self.handle.report_cost(report);
    }

    /// Emit a log line from this node. Journaled for dashboard display.
    pub fn log(&self, level: LogLevel, message: impl Into<String>) {
        self.handle.log(level, message.into());
    }

    // ----- Read helpers ----------------------------------------------

    /// Check whether the enclosing execution has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.handle.is_cancelled()
    }

    /// Notify that resolves when the execution is cancelled. Nodes
    /// holding long operations should race their work against this.
    pub fn cancellation(&self) -> Arc<Notify> {
        self.handle.cancellation()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Typed config access. Config is resolved at graph-load time and
/// pre-coerced by the runtime before a node fires.
#[derive(Debug, Clone, Default)]
pub struct ConfigBag {
    pub values: HashMap<String, Value>,
}

impl ConfigBag {
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> WeftResult<T> {
        let v = self
            .values
            .get(key)
            .ok_or_else(|| WeftError::Config(format!("missing config field: {key}")))?;
        serde_json::from_value(v.clone()).map_err(|e| WeftError::Config(format!("field {key}: {e}")))
    }

    pub fn get_optional<T: DeserializeOwned>(&self, key: &str) -> WeftResult<Option<T>> {
        match self.values.get(key) {
            None => Ok(None),
            Some(v) if v.is_null() => Ok(None),
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| WeftError::Config(format!("field {key}: {e}"))),
        }
    }

    pub fn raw(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }
}

/// Typed input access. Inputs are the resolved incoming-edge values
/// for this node's invocation, matched to this color+lane.
#[derive(Debug, Clone, Default)]
pub struct InputBag {
    pub values: HashMap<String, Value>,
}

impl InputBag {
    pub fn get<T: DeserializeOwned>(&self, port: &str) -> WeftResult<T> {
        let v = self
            .values
            .get(port)
            .ok_or_else(|| WeftError::Input(format!("missing input on port: {port}")))?;
        serde_json::from_value(v.clone()).map_err(|e| WeftError::Input(format!("port {port}: {e}")))
    }

    pub fn get_optional<T: DeserializeOwned>(&self, port: &str) -> WeftResult<Option<T>> {
        match self.values.get(port) {
            None => Ok(None),
            Some(v) if v.is_null() => Ok(None),
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| WeftError::Input(format!("port {port}: {e}"))),
        }
    }

    pub fn raw(&self, port: &str) -> Option<&Value> {
        self.values.get(port)
    }

    /// Iterate over every input port (name + raw value). Used by
    /// trigger nodes that forward their input bag to output ports.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter()
    }
}

/// The runtime-facing handle. The runtime crate implements this; the
/// `Node` trait's execute receives an `ExecutionContext` that delegates
/// to an implementation.
#[async_trait::async_trait]
pub trait ContextHandle: Send + Sync {
    async fn await_form(&self, schema: FormSchema) -> WeftResult<FormSubmission>;
    async fn await_timer(&self, duration: std::time::Duration) -> WeftResult<()>;
    async fn await_callback(&self, subgraph: SubgraphRef, input: Value) -> WeftResult<Value>;
    fn report_cost(&self, report: CostReport);
    fn log(&self, level: LogLevel, message: String);
    fn is_cancelled(&self) -> bool;
    fn cancellation(&self) -> Arc<Notify>;
}

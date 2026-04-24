use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;

use crate::error::{WeftError, WeftResult};
use crate::lane::Lane;
use crate::primitive::{CostReport, WakeSignalSpec};
use crate::Color;

/// Which lifecycle phase this invocation belongs to. v2 mirrors v1's
/// three-runtime model: infra setup provisions long-lived resources
/// (sidecars), trigger setup wires up listeners (opens subscriptions,
/// registers URLs), and fire runs the regular execution subgraph.
/// Nodes branch on this to do the right thing per phase.
///
/// - `InfraSetup`: an infra node is being provisioned. Only infra
///   nodes run in this phase.
/// - `TriggerSetup`: a trigger node (or its upstream) is being set
///   up. Trigger nodes produce the wake-signal spec the listener
///   should register.
/// - `Fire`: the normal fire-time execution. Trigger nodes receive
///   the payload, their outputs flow downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    InfraSetup,
    TriggerSetup,
    Fire,
}

impl Default for Phase {
    fn default() -> Self {
        Self::Fire
    }
}

/// The per-execution context handed to `Node::execute`. Exposes the
/// language's primitive surface (`await_signal` for mid-execution
/// suspensions, `report_cost`, `log`) plus read helpers for config
/// and input.
///
/// ExecutionContext is constructed by the engine (inside the user's
/// compiled binary) and passed to each node invocation. It holds an
/// `Arc<dyn ContextHandle>` which abstracts the actual runtime
/// implementation; the engine provides a concrete handle, but the
/// trait allows alternative implementations for testing.
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
    /// Which lifecycle phase this invocation belongs to. Mirrors
    /// v1's `isInfraSetup` / `isTriggerSetup` flags. Nodes use it
    /// to branch their execute body between provisioning, wake-signal
    /// registration, and regular fire.
    pub phase: Phase,
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
        phase: Phase,
        handle: Arc<dyn ContextHandle>,
    ) -> Self {
        Self { execution_id, project_id, node_id, node_type, node_label, color, lane, config, input, phase, handle }
    }

    // ----- Suspension primitive --------------------------------------

    /// Suspend this lane until the given wake signal fires. The engine
    /// forwards the spec to the dispatcher (which registers a webhook,
    /// schedules a timer, etc.), and parks on a oneshot. When the
    /// dispatcher delivers the fire value, this returns.
    ///
    /// `spec.is_resume` must be `true` here; passing `false` is a
    /// contract violation (that's an entry-signal, not a wait).
    pub async fn await_signal(&self, spec: WakeSignalSpec) -> WeftResult<Value> {
        self.handle.await_signal(spec).await
    }

    // ----- Entry-signal registration (TriggerSetup phase) ------------

    /// Declare that when this node is live, the listener should watch
    /// for the given wake signal. Called by trigger nodes during
    /// `Phase::TriggerSetup`. Fire-and-forget: the dispatcher records
    /// the spec and registers it with the per-project listener after
    /// the sub-execution completes. Returns the user-facing URL (if
    /// the signal kind has one).
    ///
    /// `spec.is_resume` must be `false` (entry signals are persistent,
    /// not single-use lane-bound).
    pub async fn register_signal(&self, spec: WakeSignalSpec) -> WeftResult<Option<String>> {
        self.handle.register_signal(spec).await
    }

    // ----- Infra primitive ----------------------------------------

    /// Retrieve the cluster-local endpoint URL of this node's
    /// sidecar. Only valid for nodes declared `requires_infra: true`
    /// and only after `weft infra up` has run. Returns an error
    /// otherwise.
    ///
    /// Node code uses this to call its sidecar (e.g. POST /action,
    /// GET /outputs, subscribe /live). The dispatcher resolves the
    /// URL from its InfraRegistry so node code never touches k8s.
    pub async fn sidecar_endpoint(&self) -> WeftResult<String> {
        self.handle.sidecar_endpoint().await
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

/// The runtime-facing handle. The engine crate implements this; the
/// `Node` trait's execute receives an `ExecutionContext` that
/// delegates to an implementation.
#[async_trait::async_trait]
pub trait ContextHandle: Send + Sync {
    async fn await_signal(&self, spec: WakeSignalSpec) -> WeftResult<Value>;
    async fn register_signal(&self, spec: WakeSignalSpec) -> WeftResult<Option<String>>;
    async fn sidecar_endpoint(&self) -> WeftResult<String>;
    fn report_cost(&self, report: CostReport);
    fn log(&self, level: LogLevel, message: String);
    fn is_cancelled(&self) -> bool;
    fn cancellation(&self) -> Arc<Notify>;
}

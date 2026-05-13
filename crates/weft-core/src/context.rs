use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cancellation::CancellationFlag;
use crate::error::{WeftError, WeftResult};
use crate::lane::Lane;
use crate::node::SidecarSpec;
use crate::primitive::{CostReport, SignalSpec};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    InfraSetup,
    TriggerSetup,
    /// Default for old-schema rows that didn't carry a phase.
    #[default]
    Fire,
}

impl Phase {
    /// Stable wire/storage tag. Matches the serde rename so the
    /// JSON form and the DB form agree.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InfraSetup => "infra_setup",
            Self::TriggerSetup => "trigger_setup",
            Self::Fire => "fire",
        }
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

    // ----- Wait-and-resume primitive ---------------------------------

    /// Stop executing this lane until the given wake signal fires.
    ///
    /// Use when the node needs an answer mid-flow that comes from
    /// outside (a HumanQuery form, a timer, a webhook callback). The
    /// node's execute body parks here and the engine releases the
    /// worker; when the fire arrives, a fresh worker spawns, folds
    /// the journal, and this call returns the fire's payload.
    ///
    /// This is the resume path; pair with `register_signal`
    /// (entry-trigger, persistent) for the other case. Lifecycle
    /// metadata (resume vs entry) lives on the dispatcher's
    /// register request, not on the spec.
    ///
    /// Returns the value the fire carried.
    pub async fn await_signal<K: crate::signal::Signal>(&self, kind: K) -> WeftResult<Value> {
        self.handle
            .await_signal(crate::signal::to_spec(kind))
            .await
    }

    // ----- Entry-trigger registration --------------------------------

    /// Set up a persistent wake signal that fires fresh executions.
    ///
    /// Use when the node is a trigger declaring "while I'm active,
    /// the listener should watch for X" (Webhook, HumanTrigger form,
    /// cron, SSE subscription). Returns synchronously with the
    /// user-facing URL (if the kind mints one) and the worker keeps
    /// executing. Each subsequent fire spawns a brand new execution
    /// of the project; this signal is NOT bound to the current
    /// execution's lane. Called from `Phase::TriggerSetup`.
    ///
    /// This is the entry path; pair with `await_signal` for mid-flow
    /// waits.
    ///
    /// Every public URL is derived from the signal's mount_path on
    /// the dispatcher; nodes don't need the URL handed back. Returns
    /// `()` once the dispatcher has acknowledged the registration.
    pub async fn register_signal<K: crate::signal::Signal>(
        &self,
        kind: K,
    ) -> WeftResult<()> {
        self.handle
            .register_signal(crate::signal::to_spec(kind))
            .await
    }

    // ----- Memoized step ---------------------------------------------

    /// Run `work` once and journal its output, OR return the past
    /// journaled output on replay. Use this to wrap any
    /// non-deterministic / side-effecting work between awaits so
    /// the value stays consistent across replays.
    ///
    /// Example:
    /// ```ignore
    /// let approval_token = ctx.run("mint_token", || async {
    ///     Ok(serde_json::json!(uuid::Uuid::new_v4().to_string()))
    /// }).await?;
    /// let answer = ctx.await_signal(spec).await?;
    /// let api_resp = ctx.run("call_api", || async {
    ///     Ok(call_external_api(&answer).await?)
    /// }).await?;
    /// ```
    ///
    /// The closure runs at most once across all replays of this
    /// (node, lane). On every subsequent replay the journaled
    /// output is returned directly without invoking the closure.
    /// `name` is author-supplied for log traceability; the
    /// runtime keys on call_index ordering, not on the name.
    pub async fn run<F, Fut>(&self, name: &str, work: F) -> WeftResult<Value>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = WeftResult<Value>>,
    {
        if let Some(value) = self.handle.run_step(name).await? {
            return Ok(value);
        }
        let value = work().await?;
        self.handle.run_record(name, &value).await?;
        Ok(value)
    }

    // ----- Infra primitive ----------------------------------------

    /// Retrieve the cluster-local endpoint URL of this node's
    /// sidecar. Only valid for nodes declared `requires_infra:
    /// true` and only after `weft infra start` has run. Returns
    /// an error otherwise.
    ///
    /// Node code uses this to call its sidecar (POST /action,
    /// GET /outputs, subscribe /live). The dispatcher resolves the
    /// URL from the `infra_pod` table so node code never touches k8s.
    pub async fn sidecar_endpoint(&self) -> WeftResult<String> {
        self.handle.sidecar_endpoint().await
    }

    /// Provision this node's sidecar. Called by infra nodes
    /// during `Phase::InfraSetup`. Mirrors v1's `infra_provision`
    /// helper: the node hands its SidecarSpec to the dispatcher,
    /// the dispatcher applies k8s manifests via its
    /// `InfraBackend` and returns the allocated endpoint URL.
    ///
    /// Readiness polling stays on the caller: the node decides
    /// what "ready" means (HTTP /health, a custom ping, nothing
    /// at all). Keeping it here would force a one-size-fits-all
    /// protocol.
    pub async fn provision_sidecar(
        &self,
        spec: SidecarSpec,
    ) -> WeftResult<SidecarHandle> {
        self.handle.provision_sidecar(spec).await
    }

    // ----- Side-effect primitives ------------------------------------

    /// Report a cost attributable to this execution. Durable: the
    /// underlying broker INSERT is the commit point, so the worker
    /// pod can die immediately after `.await` returns and the
    /// dispatcher will still journal the cost on its own timeline.
    /// Returns Err if the broker can't accept the record (e.g.
    /// negative amount rejected, broker unreachable); callers
    /// decide how to handle a cost they couldn't book.
    pub async fn report_cost(&self, report: CostReport) -> WeftResult<()> {
        self.handle.report_cost(report).await
    }

    /// Emit a log line from this node. Same durability shape as
    /// `report_cost`: the broker INSERT is the commit point.
    pub async fn log(&self, level: LogLevel, message: impl Into<String>) -> WeftResult<()> {
        self.handle.log(level, message.into()).await
    }

    // ----- Read helpers ----------------------------------------------

    /// Check whether the enclosing execution has been cancelled.
    /// Cheap synchronous read; safe to poll in tight loops.
    pub fn is_cancelled(&self) -> bool {
        self.handle.cancellation().is_cancelled()
    }

    /// Cancellation flag for the enclosing execution. Long-running
    /// nodes should `tokio::select!` on `flag.cancelled()` against
    /// their work future, e.g.:
    ///
    /// ```ignore
    /// let flag = ctx.cancellation();
    /// tokio::select! {
    ///     out = my_long_request() => out,
    ///     _ = flag.cancelled() => return Err(...),
    /// }
    /// ```
    ///
    /// The flag is persistent: once set, every future check (sync
    /// or async) sees it. No race between `cancel()` and a future
    /// `cancelled().await`.
    pub fn cancellation(&self) -> Arc<CancellationFlag> {
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
    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value>;
    async fn register_signal(&self, spec: SignalSpec) -> WeftResult<()>;
    async fn sidecar_endpoint(&self) -> WeftResult<String>;
    async fn provision_sidecar(&self, spec: SidecarSpec) -> WeftResult<SidecarHandle>;
    /// Replay-side of `ctx.run`. Returns `Some(value)` if a past
    /// invocation of this body already executed the step at the
    /// current call_index and journaled its output; the wrapper
    /// returns this value without invoking the closure. Returns
    /// `None` when the sequence-pop yielded no entry (fresh call).
    /// `name` is author-supplied for traceability; the runtime
    /// keys on call_index, not name.
    async fn run_step(&self, name: &str) -> WeftResult<Option<Value>>;
    /// Persist-side of `ctx.run`. Called only on the fresh path
    /// (no journaled output for the current call_index). Writes a
    /// `RunOutput` event so future replays will short-circuit.
    async fn run_record(&self, name: &str, value: &Value) -> WeftResult<()>;
    async fn report_cost(&self, report: CostReport) -> WeftResult<()>;
    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()>;
    fn cancellation(&self) -> Arc<CancellationFlag>;
}

/// Result of `provision_sidecar`: the handle identifies the
/// provisioned sidecar in the infra registry, the endpoint URL
/// is where the node should send its `/action` / `/outputs`
/// requests at fire time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarHandle {
    pub instance_id: String,
    pub endpoint_url: String,
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cancellation::CancellationFlag;
use crate::error::{WeftError, WeftResult};
use crate::frames::LoopFrames;
use crate::primitive::{CostReport, SignalSpec};
use crate::Color;

/// Which lifecycle phase this invocation belongs to. v2 mirrors v1's
/// three-runtime model: infra setup provisions long-lived resources
/// (infra pods), trigger setup wires up listeners (opens subscriptions,
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
    pub frames: LoopFrames,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: String,
        project_id: String,
        node_id: String,
        node_type: String,
        node_label: Option<String>,
        color: Color,
        frames: LoopFrames,
        config: ConfigBag,
        input: InputBag,
        phase: Phase,
        handle: Arc<dyn ContextHandle>,
    ) -> Self {
        Self {
            execution_id, project_id, node_id, node_type, node_label, color, frames,
            config, input, phase, handle,
        }
    }

    // ----- Wait-and-resume primitive ---------------------------------

    /// Stop executing this firing until the given wake signal fires.
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
    /// firing. Called from `Phase::TriggerSetup`.
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
    /// (node, frames). On every subsequent replay the journaled
    /// output is returned directly without invoking the closure.
    /// `name` is author-supplied for log traceability; the
    /// runtime keys on call_index ordering, not on the name.
    pub async fn run<F, Fut>(&self, name: &str, work: F) -> WeftResult<Value>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = WeftResult<Value>>,
    {
        // run_step returns the call_index it advanced PAST, so the
        // record path doesn't have to read a shared counter and
        // subtract one. The call_index travels with the value
        // explicitly instead of via an in-RAM invariant between
        // two trait calls.
        let (call_index, maybe_value) = self.handle.run_step(name).await?;
        if let Some(value) = maybe_value {
            return Ok(value);
        }
        let value = work().await?;
        self.handle.run_record(name, call_index, &value).await?;
        Ok(value)
    }

    // ----- Downstream emission ---------------------------------------

    /// Fire downstream with `output`. The ONLY way a node emits values.
    /// Call it once at the end (the common case), or early then keep
    /// running (a co-alive node releasing one port before finishing),
    /// or several times with DISJOINT ports (release ports incrementally).
    /// Each output port can be emitted AT MOST ONCE per firing; a second
    /// emission on the same port errors loud. Ports the node never
    /// mentions, neither here nor via `close_port`, get a CLOSURE marker
    /// at termination so downstream consumers learn nothing's coming.
    pub async fn pulse_downstream(&self, output: crate::node::NodeOutput) -> WeftResult<()> {
        self.handle.pulse_downstream(output).await
    }

    /// The output ports this node declared in its metadata. A node
    /// fanning a dynamic object onto ports (an LLM JSON response, a
    /// forwarded HTTP body) intersects its keys with this set so it
    /// emits only declared ports, instead of tripping the
    /// undeclared-port error AFTER a paid / irreversible call. See
    /// [`crate::node::NodeOutput::extend_from_declared`].
    pub fn declared_output_ports(&self) -> &HashSet<String> {
        self.handle.declared_output_ports()
    }

    /// Close an output port mid-firing. The downstream subgraph attached
    /// to `port` receives a CLOSURE pulse (structural "nothing's coming")
    /// at the firing's own frame stack, same shape as the
    /// termination-time sweep. Use this when a node releases a port
    /// early but keeps running on other work (e.g. a chat host that
    /// closes `channel` the moment the conversation ends but is still
    /// finishing bookkeeping). Counts as the firing's one allowed
    /// mention of `port`: any later `pulse_downstream` or `close_port`
    /// on the same port errors loud.
    pub async fn close_port(&self, port: &str) -> WeftResult<()> {
        self.handle.close_port(port).await
    }

    // ----- Bus primitive ---------------------------------------------

    /// Mint a fresh bus for this execution and return `(handle, marker)`.
    /// Put `marker` on an output port via `NodeOutput::set(port, marker)`;
    /// downstream nodes resolve it back to a handle via [`Self::bus`].
    /// The bus IS the marker: a `Bus`-typed value flowing through a
    /// loop, a dict, a passthrough is the same JSON marker, just
    /// like a String. There is no per-port "bus registration" on the
    /// producer.
    ///
    /// `opts` picks the mode (journaled vs ephemeral) and any per-bus
    /// tuning. `BusOptions::default()` is the journaled-mode shape; use
    /// `BusOptions { ephemeral: true, .. }` for video / high-rate
    /// streams where dropping old payloads under load is preferable to
    /// growing the journal unboundedly.
    pub fn create_bus(
        &self,
        opts: crate::bus::BusOptions,
    ) -> WeftResult<(crate::bus::BusHandle, Value)> {
        self.handle.create_bus(opts)
    }

    /// Resolve a Bus-marker JSON value to a fresh handle on the live
    /// channel. The value typically comes from an input port:
    /// `let marker = ctx.input.get::<Value>("ch")?; let bus = ctx.bus(&marker)?;`.
    /// Errors loud if the value is not a marker, the uuid is malformed,
    /// or no bus with that id is live (creator gone / wrong execution).
    pub fn bus(&self, marker: &Value) -> WeftResult<crate::bus::BusHandle> {
        self.handle.bus(marker)
    }

    /// Wake payload for THIS firing. `Some(value)` only when the engine
    /// dispatched this node as the firing trigger of a fresh execution
    /// (the HTTP body for a webhook, the SSE event JSON for an external
    /// feed, the form submission, the timer info for a scheduled tick).
    /// `None` everywhere else: trigger setup, infra setup, non-trigger
    /// nodes, replays after the kick was consumed. A trigger that REQUIRES
    /// a payload should `ok_or_else` with a clear error; the language
    /// doesn't impose a payload contract.
    pub fn wake_payload(&self) -> Option<&Value> {
        self.handle.wake_payload()
    }

    /// Wake payload typed-accessor: same as [`Self::wake_payload`] but
    /// erroring loud when the payload is absent OR not a JSON object.
    /// Convenience for trigger nodes that have chosen an object-shaped
    /// wake contract; nodes wanting raw bytes, a scalar, or an array
    /// keep calling [`Self::wake_payload`] and impose their own shape.
    pub fn wake_payload_object(&self) -> WeftResult<&Value> {
        let payload = self
            .handle
            .wake_payload()
            .ok_or_else(|| WeftError::NodeExecution(
                "Fire: no wake payload (the dispatcher/listener delivered nothing)".into(),
            ))?;
        if !payload.is_object() {
            // Truncate the rendered payload: a malformed multi-MB body
            // would otherwise produce a multi-MB error string that
            // floods logs and the inspector. 512 bytes is enough to
            // diagnose shape (the first {} or [] tells the author the
            // top-level mismatch).
            // `truncate_user_string` walks back to a char boundary: a
            // raw `&rendered[..512]` slice panics when a multi-byte
            // character straddles byte 512, inside the very path whose
            // job is producing a clean error.
            let preview = crate::truncate_user_string(&payload.to_string(), 512);
            return Err(WeftError::NodeExecution(format!(
                "Fire: wake payload must be a JSON object, got {preview}"
            )));
        }
        Ok(payload)
    }

    /// Convenience: read input `port` and resolve it to a bus handle in
    /// one call. Equivalent to `ctx.bus(&ctx.input.get(port)?)` but with a
    /// clearer error message naming the port.
    pub fn bus_from_input(&self, port: &str) -> WeftResult<crate::bus::BusHandle> {
        let value = self
            .input
            .values
            .get(port)
            .ok_or_else(|| WeftError::Input(format!("no value on input port '{port}'")))?;
        self.handle.bus(value).map_err(|e| {
            WeftError::Input(format!("input port '{port}' is not a live bus: {e}"))
        })
    }

    // ----- Infra primitive ----------------------------------------

    /// Resolve one of this node's declared endpoints to a handle.
    /// `name` matches an `Endpoint.name` from the InfraSpec the node
    /// returned during `provision`. One broker round-trip; the
    /// returned handle caches the URL and exposes `.url()` (sync)
    /// and `.call(...)` (HTTP) without further lookups.
    ///
    /// Valid in:
    ///   - `Phase::InfraSetup` AFTER provision + apply have succeeded;
    ///   - `Phase::TriggerSetup` and `Phase::Fire` when the project's
    ///     infra is Running.
    ///
    /// Returns an error if the endpoint doesn't exist or the infra
    /// isn't applied. The dispatcher resolves the URL from the
    /// `infra_node` row so node code never touches k8s.
    pub async fn endpoint(&self, name: &str) -> WeftResult<EndpointHandle> {
        let url = self.handle.endpoint_url(name).await?;
        Ok(EndpointHandle {
            handle: self.handle.clone(),
            url,
        })
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
/// for this node's invocation, matched to this color+frames.
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

    /// Read `port` as a non-empty string. Errors loud, distinguishing
    /// the three failure modes (absent / not a string / empty) so the
    /// caller's inspector message names the actual fault.
    pub fn required_str(&self, port: &str, what: &str) -> WeftResult<String> {
        let Some(value) = self.values.get(port) else {
            return Err(WeftError::Input(format!(
                "missing required {what} on port '{port}'"
            )));
        };
        let Some(s) = value.as_str() else {
            return Err(WeftError::Input(format!(
                "{what} on port '{port}' must be a string, got {value}"
            )));
        };
        if s.is_empty() {
            return Err(WeftError::Input(format!(
                "{what} on port '{port}' is an empty string"
            )));
        }
        Ok(s.to_string())
    }

    /// Iterate over every input port (name + raw value). Used by
    /// trigger nodes that forward their input bag to output ports.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter()
    }
}

/// HTTP method for [`EndpointHandle::call`]. GET / POST cover the
/// catalog node patterns today. Add PUT / DELETE / PATCH when a
/// real need surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointMethod {
    Get,
    Post,
}

/// Resolved handle for one of a node's declared endpoints.
/// Obtained via `ctx.endpoint(name)`: one broker round-trip
/// resolves the URL, the handle caches it. After that:
///
///   - `.url()` is a sync getter for the bare cluster-internal URL
///     (e.g. to forward as a NodeOutput port value);
///   - `.call(method, path, body)` issues an HTTP request to the
///     cached URL + `path` and returns the JSON response.
///
/// One handle, one round-trip. No duplicate `endpoint_url`+`endpoint_call`
/// pattern.
#[derive(Clone)]
pub struct EndpointHandle {
    handle: Arc<dyn ContextHandle>,
    url: String,
}

impl EndpointHandle {
    /// Cluster-internal URL of this endpoint. No broker call; the
    /// URL was resolved by `ctx.endpoint(name)`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// HTTP call to this endpoint. `path` MUST start with `/`.
    /// `body` is serialized as JSON for POST (None = no body).
    /// Returns the JSON response. Non-2xx, network errors, and
    /// non-JSON bodies all surface as `WeftError`. The cached URL
    /// is what gets used; no second broker round-trip.
    pub async fn call(
        &self,
        method: EndpointMethod,
        path: &str,
        body: Option<Value>,
    ) -> WeftResult<Value> {
        if !path.starts_with('/') {
            return Err(WeftError::Config(format!(
                "EndpointHandle::call path must start with '/': got '{path}'"
            )));
        }
        self.handle.endpoint_call(&self.url, method, path, body).await
    }
}

/// The runtime-facing handle. The engine crate implements this; the
/// `Node` trait's execute receives an `ExecutionContext` that
/// delegates to an implementation.
#[async_trait::async_trait]
pub trait ContextHandle: Send + Sync {
    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value>;
    async fn register_signal(&self, spec: SignalSpec) -> WeftResult<()>;
    /// Resolve the cluster-internal URL for a declared endpoint of
    /// the current node. Used internally by
    /// [`ExecutionContext::endpoint`] to build an `EndpointHandle`;
    /// nodes shouldn't call this directly.
    async fn endpoint_url(&self, name: &str) -> WeftResult<String>;
    /// HTTP call against a pre-resolved endpoint URL. Used
    /// internally by [`EndpointHandle::call`]; nodes shouldn't
    /// call this directly. Takes the URL the handle cached at
    /// `ctx.endpoint(name).await?` time so this call costs one
    /// HTTP round-trip (the request), not two (resolve + request).
    async fn endpoint_call(
        &self,
        url: &str,
        method: EndpointMethod,
        path: &str,
        body: Option<Value>,
    ) -> WeftResult<Value>;
    /// Replay-side of `ctx.run`. Advances the call_index counter
    /// and returns:
    ///   - `(call_index, Some(value))` if a past invocation already
    ///     executed at this index and journaled `value`; the
    ///     wrapper returns `value` without invoking the closure;
    ///   - `(call_index, None)` on the fresh path; the wrapper
    ///     runs the closure and passes this same `call_index` back
    ///     to `run_record`.
    /// `call_index` is returned explicitly and threaded into
    /// `run_record` so the two calls agree on the index by passing
    /// it, not by each side independently reading a shared counter.
    async fn run_step(&self, name: &str) -> WeftResult<(u32, Option<Value>)>;
    /// Persist-side of `ctx.run`. Called only on the fresh path
    /// (no journaled output for the current call_index). `call_index`
    /// is the value `run_step` returned; passing it explicitly
    /// removes the read-counter-and-subtract-one coupling.
    async fn run_record(&self, name: &str, call_index: u32, value: &Value) -> WeftResult<()>;
    async fn report_cost(&self, report: CostReport) -> WeftResult<()>;
    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()>;
    fn cancellation(&self) -> Arc<CancellationFlag>;

    /// The output port names this node declares in its metadata.
    /// The runtime already rejects emits on undeclared ports loudly;
    /// this exposes the declared set so a node that fans a dynamic
    /// object onto ports (an LLM JSON response, a forwarded HTTP
    /// body) can INTERSECT its keys with what it declared, emitting
    /// only the declared subset instead of tripping the
    /// undeclared-port error AFTER a paid/irreversible call. Generic
    /// surface: any node can read it; no node-specific knowledge in
    /// the engine.
    fn declared_output_ports(&self) -> &HashSet<String>;

    /// Fire downstream with `output`. The engine turns each mentioned
    /// output port into pulses on its outgoing edges, at the firing's
    /// own frame stack. Each port can be emitted AT MOST ONCE per firing; a
    /// second emission errors loud. Bus values are carried as plain
    /// JSON markers (`{"__weft_bus__": {"id": "<uuid>", "mode":
    /// "journaled" | "ephemeral"}}`); the live channel is resolved
    /// per-consumer via the per-execution `BusRegistry`.
    async fn pulse_downstream(&self, output: crate::node::NodeOutput) -> WeftResult<()>;

    /// Emit a CLOSURE on `port` at the firing's own frame stack.
    /// Same one-emission-per-port rule as `pulse_downstream`: a port
    /// already emitted (whether via `pulse_downstream` or a prior
    /// `close_port`) errors loud. Ports never mentioned through either
    /// API get closed automatically at firing termination, so calling
    /// this is only necessary when a node wants to release downstream
    /// early while it keeps running on other work.
    async fn close_port(&self, port: &str) -> WeftResult<()>;

    /// Mint a fresh bus and register it in this execution's
    /// `BusRegistry`. Returns `(creator-handle, marker-json)`. The
    /// marker is what the producer puts on its output port; consumers
    /// resolve the marker back to a fresh handle via [`Self::bus`].
    fn create_bus(
        &self,
        opts: crate::bus::BusOptions,
    ) -> WeftResult<(crate::bus::BusHandle, Value)>;

    /// Resolve a Bus-marker JSON value to a fresh consumer handle on the
    /// live channel. Errors loud on every failure mode (not a marker,
    /// malformed uuid, no live bus with that id).
    fn bus(&self, marker: &Value) -> WeftResult<crate::bus::BusHandle>;

    /// The wake event's payload for this firing. `Some(value)` only
    /// when the engine dispatched this node as the FIRING TRIGGER of
    /// a fresh execution (the HTTP body for a webhook, the SSE event
    /// JSON for an external feed, the form submission, the timer info
    /// for a scheduled tick). `None` everywhere else: non-trigger
    /// nodes, trigger setup phase, trigger nodes that weren't the one
    /// the listener routed this fire to, every dispatch after the
    /// kick is consumed. Node bodies that REQUIRE a payload should
    /// `ok_or_else` with a clear error; the language doesn't impose a
    /// payload contract.
    fn wake_payload(&self) -> Option<&Value>;
}

#[cfg(test)]
mod input_bag_tests {
    use super::*;
    use serde_json::json;

    fn bag(values: serde_json::Value) -> InputBag {
        InputBag {
            values: values.as_object().unwrap().clone().into_iter().collect(),
        }
    }

    #[test]
    fn required_str_distinguishes_the_three_failure_modes() {
        let b = bag(json!({"num": 7, "empty": "", "ok": "v"}));
        assert_eq!(b.required_str("ok", "thing").unwrap(), "v");
        let absent = b.required_str("missing", "thing").unwrap_err().to_string();
        assert!(absent.contains("missing required thing"), "{absent}");
        let wrong = b.required_str("num", "thing").unwrap_err().to_string();
        assert!(wrong.contains("must be a string"), "{wrong}");
        let empty = b.required_str("empty", "thing").unwrap_err().to_string();
        assert!(empty.contains("empty string"), "{empty}");
    }
}

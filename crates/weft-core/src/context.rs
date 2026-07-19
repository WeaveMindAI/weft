use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cancellation::CancellationFlag;
use crate::error::{WeftError, WeftResult};
use crate::frames::LoopFrames;
use crate::primitive::SignalSpec;
use crate::weft_type::WeftType;
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


/// How long a node's provider work may take, unless it says otherwise
/// ([`ExecutionContext::provider_access_within`]). Generous for a normal API
/// call (including a stream and its cost resolution), short enough that an
/// access left behind by a crashed worker goes stale the same quarter hour.
pub const DEFAULT_PROVIDER_WINDOW: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// The per-execution context handed to `Node::execute`. Exposes the
/// language's primitive surface (`await_signal` for mid-execution
/// suspensions, `provider_access`/`metered_client` for paid calls, `log`)
/// plus read helpers for config and input.
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
    /// the listener should watch for X" (ApiEndpoint, HumanTrigger form,
    /// cron, SseSubscribe). Returns synchronously with the
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

    /// Build a `NodeOutput` by fanning a dynamic object's top-level keys
    /// (an LLM JSON response, a forwarded HTTP body) onto same-named
    /// output ports, intersected with the ports this node declared in
    /// its metadata: payload extras are skipped instead of tripping the
    /// undeclared-port error AFTER a paid / irreversible call. Chain
    /// `.set(..)` after it for ports the node computes itself; the later
    /// set wins, so a payload key can't shadow the node's own truth.
    pub fn fan_declared(&self, source: &serde_json::Value) -> crate::node::NodeOutput {
        crate::node::NodeOutput::empty()
            .extend_from_declared(source, self.handle.declared_output_ports())
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

    // ----- Storage primitive ------------------------------------------

    /// Open a handle on the tenant's storage, walled to `scope`
    /// (`StorageScope::Execution` is the default: per-run scratch,
    /// swept on terminate unless kept). Big bytes flow worker<->broker<->bucket;
    /// only the small self-describing stored-file reference
    /// the handle returns ever rides edges / the journal.
    ///
    /// The scope governs WRITES and LISTS (`put` builds keys under
    /// the scope's prefix; `list` enumerates it). Key-addressed verbs
    /// (`get`/`delete`/`keep`/`presign`) act on the key's OWN scope
    /// (the key encodes its prefix), so a downstream node can `get` a
    /// stored-file value without knowing which scope produced it; the box
    /// still enforces the wall (own color, own project, granted
    /// shared names).
    pub fn storage(&self, scope: crate::storage::StorageScope) -> StorageHandle {
        StorageHandle {
            handle: self.handle.clone(),
            scope,
        }
    }

    // ----- Provider access + metered calls -----------------------------

    /// Your access to a provider: what to authenticate with. `user_key` is
    /// the raw value of the node's key input: a real key string is the
    /// USER'S OWN key (their provider account, used as-is); empty/absent or
    /// the `__PLATFORM__` sentinel asks the DEPLOYMENT to supply its
    /// configured key for `provider`, which it may refuse (none configured,
    /// or this node is not permitted to use it). Errors are loud and name
    /// the fix ("set your own key for `provider`").
    ///
    /// The whole paid-call surface is two steps: open the access, then make
    /// the calls with [`Self::metered_client`]. The runtime routes the call,
    /// measures what it cost (the provider's meter, run around the call),
    /// records the figure on the execution's cost trail, and gives a
    /// deployment-granted access back when the node finishes. The node
    /// declares no estimate, holds nothing, settles nothing, and cannot
    /// misstate a cost.
    ///
    /// Uses the default work window ([`DEFAULT_PROVIDER_WINDOW`]); a node
    /// whose provider work legitimately runs longer declares its own with
    /// [`Self::provider_access_within`].
    pub async fn provider_access(
        &self,
        provider: &str,
        user_key: Option<String>,
    ) -> WeftResult<crate::access::ProviderAccess> {
        self.provider_access_within(provider, user_key, DEFAULT_PROVIDER_WINDOW).await
    }

    /// [`Self::provider_access`] with an explicit `window`: how long this
    /// node's provider work may take. A deployment-granted credential is
    /// guaranteed usable exactly that long (the crash backstop; the runtime
    /// normally retires it when the node finishes). Nodes wrapping genuinely
    /// long actions (a multi-hour generation) raise it.
    pub async fn provider_access_within(
        &self,
        provider: &str,
        user_key: Option<String>,
        window: std::time::Duration,
    ) -> WeftResult<crate::access::ProviderAccess> {
        if let Some(own) = crate::access::user_key_of(user_key.as_deref()) {
            return Ok(crate::access::ProviderAccess::own(provider, own, window));
        }
        let (credential, relay_url) =
            self.handle.open_provider_access(provider, window).await?;
        Ok(crate::access::ProviderAccess::deployment(provider, credential, relay_url, window))
    }

    /// An HTTP client for paid calls on `access`: use it directly, or hand
    /// it to any library that accepts an injected client. Behind it, the
    /// runtime routes the request (straight to the provider, or through the
    /// deployment's relay when the access carries one) and runs the
    /// provider's meter around the call, so the call's real cost lands on
    /// the execution's cost trail without the node doing anything.
    ///
    /// The one rule for paid calls: never construct your own HTTP client
    /// for one; always take it from here. A call made on a hand-rolled
    /// client is invisible to the cost trail (and a relayed access's
    /// credential is useless outside this client's routing).
    pub fn metered_client(
        &self,
        access: &crate::access::ProviderAccess,
    ) -> WeftResult<reqwest_middleware::ClientWithMiddleware> {
        self.handle.metered_client(access)
    }

    /// This node's configuration with the `config` input port overlaid.
    ///
    /// The config-node pattern: a node may take a whole configuration object on
    /// an input port named `config` (fed by a dedicated config node), and each
    /// key it carries wins over the same key set on the node itself. A node
    /// without that port gets its own config unchanged.
    pub fn effective_config(&self) -> crate::context::ConfigBag {
        self.config.overlaid(self.input.raw("config"))
    }

    // ----- Side-effect primitives ------------------------------------

    /// Emit a log line from this node. Durable: the broker INSERT is
    /// the commit point.
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

    // ----- Live caller connection ------------------------------------

    /// Is this execution attached to a live HTTP caller? `true` only on
    /// the worker that received an `http` `live_connection` request.
    /// Pure status read; lets a multi-purpose node gate its behavior.
    /// Distinct from [`Self::is_websocket`] on purpose: a node may branch
    /// THREE ways (http / websocket / neither), so the two queries are
    /// separate and never entangled into one enum.
    pub fn is_api_call(&self) -> bool {
        matches!(
            self.handle.caller_connection().map(|c| c.config().protocol),
            Some(crate::signal::Protocol::Http)
        )
    }

    /// Is this execution attached to a live WebSocket caller? `true` only
    /// on the worker that received a `websocket` `live_connection`
    /// request. See [`Self::is_api_call`] for why these are two queries.
    pub fn is_websocket(&self) -> bool {
        matches!(
            self.handle.caller_connection().map(|c| c.config().protocol),
            Some(crate::signal::Protocol::Websocket)
        )
    }

    /// The declared inbound/outbound data shape of the live connection,
    /// or `None` if this run has no live caller. Queryable so a node can
    /// branch on "do I send bytes or JSON here" (same gating spirit as
    /// `is_api_call` / `is_websocket`).
    pub fn caller_data_type(&self) -> Option<crate::signal::DataType> {
        self.handle.caller_connection().map(|c| c.config().data_type)
    }

    /// The live caller connection as a protocol-typed handle, or `None`
    /// if this run has no live caller (a durable run, or any worker that
    /// did not receive the request). The handle's talk methods are
    /// protocol-specific (HTTP: respond/write/close; WS:
    /// send/receive/request/close); both share `is_connected` and the one
    /// `ensure_connected` barrier. A node that needs the caller but may
    /// run without one checks `is_api_call`/`is_websocket` first, or
    /// handles `None`.
    pub fn caller(&self) -> Option<crate::caller::CallerHandle> {
        self.handle
            .caller_connection()
            .map(crate::caller::CallerHandle::from_connection)
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

    /// This config with `overlay`'s keys winning on collision. A non-object
    /// (or absent) overlay changes nothing. Pure: the mechanism behind
    /// [`ExecutionContext::effective_config`].
    pub fn overlaid(&self, overlay: Option<&Value>) -> ConfigBag {
        let mut values = self.values.clone();
        if let Some(object) = overlay.and_then(|v| v.as_object()) {
            for (k, v) in object {
                values.insert(k.clone(), v.clone());
            }
        }
        ConfigBag { values }
    }
}

#[cfg(test)]
mod config_overlay_tests {
    use super::*;

    fn bag(json: serde_json::Value) -> ConfigBag {
        ConfigBag { values: serde_json::from_value(json).expect("object fixture") }
    }

    /// The config-node pattern: a key from the config input wins over the same
    /// key on the node; keys only one side has all survive.
    #[test]
    fn an_overlay_key_wins_and_disjoint_keys_survive() {
        let own = bag(serde_json::json!({"model": "a", "temperature": 0.2}));
        let overlay = serde_json::json!({"model": "b", "maxTokens": 100});

        let merged = own.overlaid(Some(&overlay));
        assert_eq!(merged.get::<String>("model").unwrap(), "b");
        assert_eq!(merged.get::<f64>("temperature").unwrap(), 0.2);
        assert_eq!(merged.get::<u64>("maxTokens").unwrap(), 100);
    }

    /// No overlay, or one that is not an object, leaves the config unchanged.
    #[test]
    fn a_missing_or_non_object_overlay_changes_nothing() {
        let own = bag(serde_json::json!({"model": "a"}));
        assert_eq!(own.overlaid(None).get::<String>("model").unwrap(), "a");
        assert_eq!(
            own.overlaid(Some(&serde_json::json!("not an object")))
                .get::<String>("model")
                .unwrap(),
            "a"
        );
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

/// Scoped handle on the tenant's storage, minted by
/// [`ExecutionContext::storage`]. Every verb delegates to the
/// `ContextHandle` storage methods; the handle itself only carries
/// the chosen scope.
///
/// File-addressed verbs accept any file value (the concrete
/// `__weft_<kind>__` reference an upstream node emitted, key- or
/// url-backed) or a raw key string wrapped in a JSON string; see
/// [`crate::storage::FileHandle::from_value`]. Verbs that only make
/// sense on bucket-stored bytes (`delete`, `keep`) error loud on a
/// url-backed value.
#[derive(Clone)]
pub struct StorageHandle {
    handle: Arc<dyn ContextHandle>,
    scope: crate::storage::StorageScope,
}

impl StorageHandle {
    /// Store `bytes` under this handle's scope. Returns the
    /// self-describing stored-file value (`key` + `mimeType` +
    /// `sizeBytes` + `filename`, NO url) to emit downstream. `keep` flags an
    /// execution-scoped file to survive the terminate sweep (with the
    /// given access-bumped TTL); it is meaningless for project/shared
    /// scopes (those persist without a flag) and rejected there.
    pub async fn put(
        &self,
        bytes: impl Into<bytes::Bytes>,
        mime_type: &str,
        filename: &str,
        keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        let bytes = bytes.into();
        let declared_size = Some(bytes.len() as u64);
        self.handle
            .storage_put(
                &self.scope,
                crate::storage::bytes_stream(bytes),
                mime_type,
                filename,
                keep,
                declared_size,
            )
            .await
    }

    /// Streaming variant of [`Self::put`]: pipe an incoming body
    /// (an HTTP response, a transform's output) straight into
    /// storage without buffering the whole file.
    pub async fn put_stream(
        &self,
        stream: crate::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        self.handle
            .storage_put(&self.scope, stream, mime_type, filename, keep, None)
            .await
    }

    /// Fetch an HTTP(S) URL straight into this handle's scope and
    /// return the stored-file value to emit downstream. The bytes
    /// stream through (never fully buffered), the mime is taken from
    /// the response Content-Type, and `filename` None derives one from
    /// the URL. The one-call "I want this URL in storage" capability;
    /// nodes never hand-roll an HTTP client for this.
    pub async fn put_from_url(
        &self,
        url: &str,
        filename: Option<&str>,
        keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        self.handle.storage_put_from_url(&self.scope, url, filename, keep).await
    }

    /// Stream a file's bytes. Accepts any file value (a bucket-backed
    /// `key` marker, an external `url` marker) or a raw key (JSON string).
    /// For a bucket-backed file this counts as access (bumps a kept file's
    /// TTL); a url-backed value has no stored TTL to bump. The node holds
    /// an ADDRESS and this reads the bytes behind it, whichever form.
    pub async fn get(
        &self,
        file_or_key: &Value,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        self.get_with_range(file_or_key, None).await
    }

    /// Range read: stream only `range` of the file. The home of the
    /// process-a-huge-file-piecewise pattern (split an audio into
    /// chunks for an API without ever holding the whole file).
    pub async fn get_range(
        &self,
        file_or_key: &Value,
        range: crate::storage::ByteRange,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        self.get_with_range(file_or_key, Some(range)).await
    }

    /// The one read dispatch behind `get` / `get_range`: parse the file value's
    /// HANDLE and route to the bucket (a `key`) or an external fetch (a `url`).
    /// A `url`-backed file is fetched directly by the worker, which is safe
    /// because that fetch only ever runs inside the isolated worker (the same
    /// reason `put_from_url` is safe). The node-facing surface is identical
    /// either way.
    async fn get_with_range(
        &self,
        file_or_key: &Value,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        match crate::storage::FileHandle::from_value(file_or_key)? {
            crate::storage::FileHandle::Key(key) => self.handle.storage_get(&key, range).await,
            crate::storage::FileHandle::Url { url, mime_type, filename, size_bytes } => {
                self.handle.storage_get_url(&url, &mime_type, &filename, size_bytes, range).await
            }
        }
    }

    /// Convenience: [`Self::get`] fully collected into memory. Only
    /// for files known to be small; large files should stream.
    pub async fn get_bytes(
        &self,
        file_or_key: &Value,
    ) -> WeftResult<(crate::storage::StoredFileMeta, bytes::Bytes)> {
        let (meta, stream) = self.get(file_or_key).await?;
        let bytes = crate::storage::collect_stream(stream)
            .await
            .map_err(|e| WeftError::NodeExecution(format!("storage get stream: {e}")))?;
        Ok((meta, bytes))
    }

    /// Delete a stored file. Space is reclaimed in place, instantly.
    /// Bucket-only: a url-backed file value has nothing in storage to
    /// delete and errors loud.
    pub async fn delete(&self, file_or_key: &Value) -> WeftResult<()> {
        let key = Self::bucket_key(file_or_key, "delete")?;
        self.handle.storage_delete(&key).await
    }

    /// List the files under this handle's scope prefix.
    pub async fn list(&self) -> WeftResult<Vec<crate::storage::StoredFileMeta>> {
        self.handle.storage_list(&self.scope).await
    }

    /// Mark an existing execution-scoped file to survive the
    /// terminate sweep (the after-the-fact twin of `put(.., keep)`).
    /// Keep is purely ADDITIVE: there is no un-keep / keep-only verb.
    /// Bucket-only: a url-backed file value is not subject to the
    /// terminate sweep (the bytes were never in storage) and errors loud.
    pub async fn keep(
        &self,
        file_or_key: &Value,
        ttl: crate::storage::KeepTtl,
    ) -> WeftResult<()> {
        let key = Self::bucket_key(file_or_key, "keep")?;
        self.handle.storage_keep(&key, ttl).await
    }

    /// Mint a TEMPORARY signed URL for handing this file to an
    /// external URL-accepting API; the external service streams
    /// directly from the storage bucket. `ttl_secs: None` uses the
    /// service default (~15 min). The URL is an explicit, per-file,
    /// expiring artifact; the stored-file VALUE never carries it. For a
    /// bucket-backed file this counts as access (bumps a kept file's
    /// TTL). A url-backed file value is
    /// ALREADY a URL an external service can fetch: presign returns it
    /// as-is (no expiry to mint, nothing in the bucket to sign), so the
    /// caller's contract ("a URL to hand out") holds for both handles.
    pub async fn presign(
        &self,
        file_or_key: &Value,
        ttl_secs: Option<u64>,
    ) -> WeftResult<String> {
        match crate::storage::FileHandle::from_value(file_or_key)? {
            crate::storage::FileHandle::Key(key) => {
                self.handle.storage_presign(&key, ttl_secs).await
            }
            crate::storage::FileHandle::Url { url, .. } => Ok(url),
        }
    }

    /// Parse a file value down to its bucket key for the verbs that only
    /// make sense on stored bytes (`delete`, `keep`). A url-backed value
    /// errors loud with the verb's name: the bytes live at an external
    /// URL, there is nothing in storage to act on.
    fn bucket_key(file_or_key: &Value, verb: &str) -> WeftResult<String> {
        match crate::storage::FileHandle::from_value(file_or_key)? {
            crate::storage::FileHandle::Key(key) => Ok(key),
            crate::storage::FileHandle::Url { url, .. } => Err(WeftError::Input(format!(
                "storage {verb}: this file value points at an external URL ({url}), not a stored file; only bucket-stored files can be {verb}ed"
            ))),
        }
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
    /// Open access to `provider` on the DEPLOYMENT's configured key for the
    /// calling node: returns the credential to authenticate with and,
    /// optionally, the relay address calls on it must go to (`None` = the
    /// provider's own API). Used internally by
    /// [`ExecutionContext::provider_access`] (a user-supplied key never
    /// reaches this). The deployment decides whether this node may use its
    /// key; a refusal or a missing key is a loud error telling the user to
    /// set their own key. The runtime gives the access back when the node
    /// finishes; nothing node-facing closes it.
    async fn open_provider_access(
        &self,
        provider: &str,
        window: std::time::Duration,
    ) -> WeftResult<(String, Option<String>)>;
    /// The metering HTTP client for calls on `access`: routes the request
    /// (provider or relay) and runs the provider's meter around it, so the
    /// call's real cost is measured and recorded by the runtime. Used
    /// internally by [`ExecutionContext::metered_client`].
    fn metered_client(
        &self,
        access: &crate::access::ProviderAccess,
    ) -> WeftResult<reqwest_middleware::ClientWithMiddleware>;
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
    fn declared_output_ports(&self) -> &HashMap<String, WeftType>;

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

    /// Store a byte stream under `scope`; returns the stored-file
    /// value (see [`crate::storage::StoredFile`]). Implementations
    /// resolve the tenant's box endpoint, attach the caller's
    /// identity, and stream the body; they never buffer the whole
    /// file. `keep` only applies to `StorageScope::Execution`.
    /// `declared_size` is the total byte size when the caller knows it
    /// up front (a buffered payload, a sized HTTP body); `None` for a
    /// genuinely unknown-length stream.
    async fn storage_put(
        &self,
        scope: &crate::storage::StorageScope,
        data: crate::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<crate::storage::KeepTtl>,
        declared_size: Option<u64>,
    ) -> WeftResult<Value>;

    /// Stream an HTTP(S) URL straight into `scope` storage and return
    /// the stored-file value. The implementation GETs the URL, fails
    /// loud on a non-success status, derives the mime from the
    /// response Content-Type (normalized), and streams the body into
    /// storage without buffering the whole file. `filename` None lets
    /// the implementation derive one from the URL's last path segment.
    /// The capability node authors use instead of hand-rolling an HTTP
    /// client; lives on the trait (not `StorageHandle`) because the
    /// HTTP client is an engine dependency, not a core one.
    async fn storage_put_from_url(
        &self,
        scope: &crate::storage::StorageScope,
        url: &str,
        filename: Option<&str>,
        keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value>;

    /// Stream a stored file (optionally a byte range). The key
    /// encodes its own scope; the service enforces the wall from the
    /// caller's verified identity.
    async fn storage_get(
        &self,
        key: &str,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)>;

    /// Stream the bytes of a file that lives at an external URL (a file value
    /// whose handle is a `url`, not a bucket `key`). The worker GETs the URL
    /// directly and streams the body out; the fetch happens only inside the
    /// isolated worker (never a trusted service), so an arbitrary URL is safe
    /// here for the same reason `storage_put_from_url` is. `declared` carries
    /// the marker's mime/filename/size for the returned meta; the response's
    /// own Content-Type wins for the actual byte stream's kind. Lives on the
    /// trait (not `StorageHandle`) because the HTTP client is an engine
    /// dependency, not a core one.
    async fn storage_get_url(
        &self,
        url: &str,
        declared_mime: &str,
        declared_filename: &str,
        declared_size: u64,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)>;

    /// Delete a stored file by key.
    async fn storage_delete(&self, key: &str) -> WeftResult<()>;

    /// List files under `scope`'s prefix.
    async fn storage_list(
        &self,
        scope: &crate::storage::StorageScope,
    ) -> WeftResult<Vec<crate::storage::StoredFileMeta>>;

    /// Flag an execution-scoped file to survive the terminate sweep.
    async fn storage_keep(&self, key: &str, ttl: crate::storage::KeepTtl) -> WeftResult<()>;

    /// Mint a temporary signed URL for an external service to fetch
    /// `key` directly from the box. `None` TTL = service default.
    async fn storage_presign(&self, key: &str, ttl_secs: Option<u64>) -> WeftResult<String>;

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

    /// The live caller connection attached to THIS execution, if any.
    /// `Some` only on the one worker that received a `live_connection`
    /// request, for the life of that worker; `None` for every durable run
    /// and for every other worker. The engine wires the production
    /// connection (worker<->gateway socket) here when the caller attaches;
    /// tests wire a fake. Any node in the execution shares the one `Arc`.
    fn caller_connection(&self) -> Option<Arc<dyn crate::caller::CallerConnection>>;
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

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

/// Which lifecycle phase this invocation belongs to. Three-runtime
/// model: infra setup provisions long-lived resources (infra pods),
/// trigger setup wires up listeners (opens subscriptions, registers
/// URLs), and fire runs the regular execution subgraph.
///
/// Engine/journal vocabulary only: nodes never see it. The engine
/// routes each phase to the right `Node` method from the manifest
/// (`setup_trigger` for a trigger at TriggerSetup, `run` otherwise;
/// a trigger is skipped at InfraSetup).
///
/// - `InfraSetup`: an infra node is being provisioned.
/// - `TriggerSetup`: a trigger node (or its upstream) is being set
///   up. The trigger registers the wake signal the listener
///   should watch.
/// - `Fire`: the normal fire-time execution. The firing trigger
///   receives the wake payload, its outputs flow downstream.
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

/// The per-execution context handed to a node body (`Node::run` /
/// `Node::setup_trigger`). Exposes the language's primitive surface
/// (`await_signal` for mid-execution suspensions,
/// `provider_access`/`metered_client` for paid calls, `log`) plus the
/// two named-value bags (`ctx.inputs` / `ctx.wake`).
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
    /// The node's INPUTS this firing, one bag: wired pulse values,
    /// braces/assignment literals from the `.weft` body, and declared
    /// defaults for anything still absent. However an input got its
    /// value, the node reads it here. Precedence: wire/literal >
    /// default. No name is special: an object wired to an input arrives
    /// as that object.
    pub inputs: ValueBag,
    /// The fire event's payload fields (the HTTP body for a webhook,
    /// the SSE event JSON for a feed, the form submission, the timer
    /// info for a scheduled tick). Populated only when this node is the
    /// FIRING trigger of the execution; empty everywhere else. A
    /// non-object payload has no named fields; the whole-record read
    /// ([`ValueBag::object`]) fails loud on it.
    pub wake: ValueBag,
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
        inputs: ValueBag,
        handle: Arc<dyn ContextHandle>,
    ) -> Self {
        let wake = ValueBag::wake(handle.wake_payload());
        Self {
            execution_id, project_id, node_id, node_type, node_label, color, frames,
            inputs, wake, handle,
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
        // Snapshot the trigger's input values with the registration: at
        // fire time the engine replays them onto `ctx.inputs`, so a
        // trigger's inputs are whatever they were at trigger setup
        // (re-activation re-registers and re-snapshots).
        let port_snapshot =
            Value::Object(self.inputs.values.clone());
        self.handle
            .register_signal(crate::signal::to_spec(kind), port_snapshot)
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
        crate::node::NodeOutput::new()
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

    /// Convenience: read input `name` and resolve it to a bus
    /// handle in one call. Equivalent to `ctx.bus(ctx.inputs.raw(name))`
    /// but with a clearer error message naming the input.
    pub fn bus_from_input(&self, name: &str) -> WeftResult<crate::bus::BusHandle> {
        let value = self
            .inputs
            .raw(name)
            .ok_or_else(|| WeftError::Input(format!("no value on input port '{name}'")))?;
        self.handle.bus(value).map_err(|e| {
            WeftError::Input(format!("input '{name}' is not a live bus: {e}"))
        })
    }

    // ----- Infra primitive ----------------------------------------

    /// Resolve one of this node's declared endpoints to a handle.
    /// `name` matches an `Endpoint.name` from the InfraSpec the node
    /// returned during `provision_infra`. One broker round-trip; the
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
    /// the `__PLATFORM__` sentinel asks the RUNTIME to supply its
    /// configured key for `provider`, which it may refuse (none configured,
    /// or this node is not permitted to use it). Errors are loud and name
    /// the fix ("set your own key for `provider`").
    ///
    /// The whole paid-call surface is two steps: open the access, then make
    /// the calls with [`Self::metered_client`]. The runtime routes the call,
    /// measures what it cost (the provider's meter, run around the call),
    /// records the figure on the execution's cost trail, and gives a
    /// runtime-granted access back when the node finishes. The node
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
    /// node's provider work may take. A runtime-granted credential is
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
        Ok(crate::access::ProviderAccess::runtime(provider, credential, relay_url, window))
    }

    /// An HTTP client for paid calls on `access`: use it directly, or hand
    /// it to any library that accepts an injected client. Behind it, the
    /// runtime routes the request (straight to the provider, or through the
    /// runtime's relay when the access carries one) and runs the
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

    /// The live HTTP caller, attached and connected, for nodes that only
    /// make sense behind an HTTP trigger (ApiEndpoint). One call folds
    /// the whole chain: caller present, protocol is HTTP, connection
    /// barrier passed. A node that may serve BOTH protocols branches on
    /// [`Self::caller`] instead.
    pub async fn http_caller(&self) -> WeftResult<crate::caller::HttpCaller> {
        match self.caller() {
            Some(crate::caller::CallerHandle::Http(h)) => {
                h.ensure_connected().await?;
                Ok(h)
            }
            Some(crate::caller::CallerHandle::Websocket(_)) => Err(WeftError::Input(
                "this node answers an HTTP caller, but the execution is attached to a \
                 WebSocket caller; trigger it through an ApiEndpoint node"
                    .into(),
            )),
            None => Err(WeftError::Input(
                "this node answers an HTTP caller, but no live caller is attached; \
                 trigger it through an ApiEndpoint node"
                    .into(),
            )),
        }
    }

    /// The live WebSocket caller, attached and connected, for nodes that
    /// only make sense behind a WebSocket trigger (LiveSocket). Same
    /// contract as [`Self::http_caller`], for the other protocol.
    pub async fn ws_caller(&self) -> WeftResult<crate::caller::WsCaller> {
        match self.caller() {
            Some(crate::caller::CallerHandle::Websocket(h)) => {
                h.ensure_connected().await?;
                Ok(h)
            }
            Some(crate::caller::CallerHandle::Http(_)) => Err(WeftError::Input(
                "this node talks to a WebSocket caller, but the execution is attached to \
                 an HTTP caller; trigger it through a LiveSocket node"
                    .into(),
            )),
            None => Err(WeftError::Input(
                "this node talks to a WebSocket caller, but no live caller is attached; \
                 trigger it through a LiveSocket node"
                    .into(),
            )),
        }
    }

    // ----- Plain outbound HTTP ---------------------------------------

    /// The shared, pooled HTTP client for plain (unpaid) outbound calls.
    /// One client per process; a node never constructs its own. The one
    /// rule for outbound HTTP: a PAID provider call goes through
    /// [`Self::provider_access`] + [`Self::metered_client`] (that is what
    /// records its cost); everything else goes through here.
    pub fn http(&self) -> &'static reqwest::Client {
        static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
        CLIENT.get_or_init(|| {
            reqwest::Client::builder()
                // Bound connection ESTABLISHMENT only. No overall request
                // timeout: a node may legitimately stream a response for
                // a long time, and user-facing waits take no deadline.
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("default reqwest client always builds")
        })
    }
}

/// Which side of a node's named values a bag holds, so accessor errors
/// stamp the thing the user has to fix (the node's inputs vs the fire
/// event's payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BagSide {
    Inputs,
    Wake,
}

/// One bag of named values: the node's inputs (`ctx.inputs`: wired
/// values, body literals, remaining braces-config values, declared
/// defaults) or the fire event's payload fields (`ctx.wake`).
#[derive(Debug, Clone)]
pub struct ValueBag {
    values: serde_json::Map<String, Value>,
    side: BagSide,
    /// Why [`Self::object`] cannot hand out the whole bag as one
    /// record: `None` everywhere except a wake bag whose firing
    /// delivered a missing or non-object payload.
    no_record: Option<String>,
    /// Names of the node TYPE's own spec-declared inputs (its settings),
    /// so [`Self::custom`] can hand back just the instance data. Empty
    /// on wake and nested bags.
    spec_names: std::collections::HashSet<String>,
}

impl ValueBag {
    pub fn inputs(
        values: serde_json::Map<String, Value>,
        spec_names: std::collections::HashSet<String>,
    ) -> Self {
        Self { values, side: BagSide::Inputs, no_record: None, spec_names }
    }

    /// The wake bag: the fire payload's top-level fields when the
    /// payload is a JSON object, empty otherwise. Every named read on a
    /// non-object payload therefore errors as "missing", which is the
    /// honest answer: there is no such field. The bag remembers the
    /// missing/non-object case so [`Self::object`] can fail loud.
    pub fn wake(payload: Option<&Value>) -> Self {
        let no_record = match payload {
            Some(Value::Object(_)) => None,
            Some(other) => Some(format!("wake payload is not an object: {other}")),
            None => Some("no wake payload was delivered for this firing".into()),
        };
        let values = payload.and_then(Value::as_object).cloned().unwrap_or_default();
        Self { values, side: BagSide::Wake, no_record, spec_names: Default::default() }
    }

    /// The whole bag as one record. The inputs bag always has one; a
    /// wake bag fails loud when the firing delivered a missing or
    /// non-object payload: substituting an empty record would silently
    /// fabricate an "every field absent" reading downstream.
    pub fn object(&self) -> WeftResult<&serde_json::Map<String, Value>> {
        match &self.no_record {
            None => Ok(&self.values),
            Some(reason) => Err(self.err(reason.clone())),
        }
    }

    /// The side's word for one named value, used in every error message.
    fn noun(&self) -> &'static str {
        match self.side {
            BagSide::Inputs => "input",
            BagSide::Wake => "wake field",
        }
    }

    fn err(&self, message: String) -> WeftError {
        // Both sides are inputs of the firing: one delivered by
        // wires/config, one by the wake event.
        WeftError::Input(message)
    }

    /// Read the required value `name`, typed. Errors loud when absent or
    /// when the value doesn't deserialize into `T`.
    pub fn get<T: DeserializeOwned>(&self, name: &str) -> WeftResult<T> {
        let v = self
            .values
            .get(name)
            .ok_or_else(|| self.err(format!("missing required {} '{name}'", self.noun())))?;
        serde_json::from_value(v.clone())
            .map_err(|e| self.err(format!("{} '{name}': {e}", self.noun())))
    }

    /// Read the optional value `name`, typed. Absent or explicitly null
    /// is `Ok(None)`; a PRESENT value that doesn't deserialize into `T`
    /// still errors loud, never a silent `None`.
    pub fn opt<T: DeserializeOwned>(&self, name: &str) -> WeftResult<Option<T>> {
        match self.values.get(name) {
            None => Ok(None),
            Some(v) if v.is_null() => Ok(None),
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| self.err(format!("{} '{name}': {e}", self.noun()))),
        }
    }

    /// Read the value `name` with a default: absent means `default`, but
    /// a present wrong-typed value still errors loud. The blessed
    /// pattern for defaulted knobs; never `.get(..).unwrap_or(..)`,
    /// which would swallow a real type error into the default.
    pub fn get_or<T: DeserializeOwned>(&self, name: &str, default: T) -> WeftResult<T> {
        Ok(self.opt(name)?.unwrap_or(default))
    }

    /// The raw value behind `name`, if any. For pass-through reads that
    /// must not reinterpret the value; a REQUIRED raw read is
    /// `get::<Value>(name)`.
    pub fn raw(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    /// The OBJECT behind `name` as its own bag, with the full accessor
    /// family. The one-call way to consume a config-node's output (or
    /// any object-valued input): absent/null reads as an EMPTY bag
    /// (every knob at its default), while a present non-object value
    /// errors loud, never a silent empty.
    pub fn nested(&self, name: &str) -> WeftResult<ValueBag> {
        let values = match self.values.get(name) {
            None | Some(Value::Null) => serde_json::Map::new(),
            Some(Value::Object(map)) => map.clone(),
            Some(other) => {
                return Err(self.err(format!(
                    "{} '{name}' is not an object: {other}",
                    self.noun()
                )))
            }
        };
        Ok(Self { values, side: self.side, no_record: None, spec_names: Default::default() })
    }

    /// Iterate over every named value (name + raw value), the node's
    /// own settings included. For forwarding nodes; a node projecting
    /// its instance DATA inputs wants [`Self::custom`] instead.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter()
    }

    /// The node's instance DATA inputs: every entry except the node
    /// type's own spec-declared settings (`code` on ExecPython,
    /// `title`/`fields` on a form node). What remains is what exists
    /// on THIS instance only: custom header ports, form-derived
    /// ports, carry ghosts. Nodes that project "whatever the user
    /// wired in" (Python variable bindings, form prefill data) read
    /// this instead of hardcoding their own setting names.
    pub fn custom(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter().filter(|(k, _)| !self.spec_names.contains(k.as_str()))
    }
}

/// Build a node's ONE input bag for a firing. `delivered` is what the
/// ready paths handed over: wired pulse values plus the body literals
/// the enrich normalization homed in `node.port_literals`. On top of
/// that, in precedence order (earlier wins):
///
///   1. `delivered` (wires + `all`/`assignment`-form literals);
///   2. the node's remaining `config` object verbatim (braces literals
///      on `config`-exposure inputs, reserved `_` keys, passthrough
///      keys on nodes that accept them);
///   3. declared defaults, for declared inputs still absent, unless the
///      input's wire arrived CLOSED (`closed_ports`): a closure means
///      upstream produced nothing, and silently substituting the
///      default would mask that.
///
/// No name is special: an object wired to an input (a config node's
/// output, say) arrives AS that object, and the node decides what to
/// read out of it.
pub fn node_input_bag(
    node: &crate::project::NodeDefinition,
    mut delivered: serde_json::Map<String, Value>,
    closed_ports: &[String],
) -> ValueBag {
    // The braces store under-lays what the ready paths delivered.
    // Compiler/editor plumbing keys co-resident in the config blob
    // (`parentId`, `_`-reserved) are not input data and never reach
    // the bag; the engine reads them from `node.config` directly.
    if let Some(cfg) = node.config.as_object() {
        for (k, v) in cfg {
            if crate::project::is_internal_config_key(k) {
                continue;
            }
            delivered.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    for input in &node.inputs {
        let Some(default) = &input.default else { continue };
        if closed_ports.iter().any(|p| p == &input.name) {
            continue;
        }
        delivered
            .entry(input.name.clone())
            .or_insert_with(|| default.clone());
    }

    let spec_names = node
        .inputs
        .iter()
        .filter(|i| i.from_spec)
        .map(|i| i.name.clone())
        .collect();
    ValueBag::inputs(delivered, spec_names)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
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
    /// Register an entry signal. `port_snapshot` is the trigger's
    /// delivered port values at registration time (built by
    /// `ExecutionContext::register_signal`, never by node code); the
    /// dispatcher stores it with the signal and replays it onto the
    /// trigger's ports at every fire.
    async fn register_signal(&self, spec: SignalSpec, port_snapshot: Value) -> WeftResult<()>;
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
    /// Open access to `provider` on the RUNTIME's configured key for the
    /// calling node: returns the credential to authenticate with and,
    /// optionally, the relay address calls on it must go to (`None` = the
    /// provider's own API). Used internally by
    /// [`ExecutionContext::provider_access`] (a user-supplied key never
    /// reaches this). The runtime decides whether this node may use its
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
mod value_bag_tests {
    use super::*;
    use serde_json::json;

    fn inputs_bag(values: serde_json::Value) -> ValueBag {
        ValueBag::inputs(values.as_object().unwrap().clone(), Default::default())
    }

    /// A ContextHandle for accessor tests: every runtime capability is
    /// unreachable (the accessors read only the bags), and the wake
    /// payload is absent.
    struct DeadHandle;
    #[async_trait::async_trait]
    impl ContextHandle for DeadHandle {
        async fn await_signal(&self, _: SignalSpec) -> WeftResult<Value> { unreachable!() }
        async fn register_signal(&self, _: SignalSpec, _: Value) -> WeftResult<()> { unreachable!() }
        async fn endpoint_url(&self, _: &str) -> WeftResult<String> { unreachable!() }
        async fn endpoint_call(&self, _: &str, _: EndpointMethod, _: &str, _: Option<Value>) -> WeftResult<Value> { unreachable!() }
        async fn run_step(&self, _: &str) -> WeftResult<(u32, Option<Value>)> { unreachable!() }
        async fn run_record(&self, _: &str, _: u32, _: &Value) -> WeftResult<()> { unreachable!() }
        async fn open_provider_access(&self, _: &str, _: std::time::Duration) -> WeftResult<(String, Option<String>)> { unreachable!() }
        fn metered_client(&self, _: &crate::access::ProviderAccess) -> WeftResult<reqwest_middleware::ClientWithMiddleware> { unreachable!() }
        async fn log(&self, _: LogLevel, _: String) -> WeftResult<()> { unreachable!() }
        fn cancellation(&self) -> Arc<CancellationFlag> { unreachable!() }
        fn declared_output_ports(&self) -> &HashMap<String, WeftType> { unreachable!() }
        async fn pulse_downstream(&self, _: crate::node::NodeOutput) -> WeftResult<()> { unreachable!() }
        async fn close_port(&self, _: &str) -> WeftResult<()> { unreachable!() }
        fn create_bus(&self, _: crate::bus::BusOptions) -> WeftResult<(crate::bus::BusHandle, Value)> { unreachable!() }
        fn bus(&self, _: &Value) -> WeftResult<crate::bus::BusHandle> { unreachable!() }
        async fn storage_put(&self, _: &crate::storage::StorageScope, _: crate::storage::ByteStream, _: &str, _: &str, _: Option<crate::storage::KeepTtl>, _: Option<u64>) -> WeftResult<Value> { unreachable!() }
        async fn storage_put_from_url(&self, _: &crate::storage::StorageScope, _: &str, _: Option<&str>, _: Option<crate::storage::KeepTtl>) -> WeftResult<Value> { unreachable!() }
        async fn storage_get(&self, _: &str, _: Option<crate::storage::ByteRange>) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> { unreachable!() }
        async fn storage_get_url(&self, _: &str, _: &str, _: &str, _: u64, _: Option<crate::storage::ByteRange>) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> { unreachable!() }
        async fn storage_delete(&self, _: &str) -> WeftResult<()> { unreachable!() }
        async fn storage_list(&self, _: &crate::storage::StorageScope) -> WeftResult<Vec<crate::storage::StoredFileMeta>> { unreachable!() }
        async fn storage_keep(&self, _: &str, _: crate::storage::KeepTtl) -> WeftResult<()> { unreachable!() }
        async fn storage_presign(&self, _: &str, _: Option<u64>) -> WeftResult<String> { unreachable!() }
        fn wake_payload(&self) -> Option<&Value> { None }
        fn caller_connection(&self) -> Option<Arc<dyn crate::caller::CallerConnection>> { None }
    }

    fn ctx(inputs_json: serde_json::Value) -> ExecutionContext {
        ExecutionContext::new(
            "exec-1".into(),
            "project-1".into(),
            "node-1".into(),
            "TestNode".into(),
            None,
            crate::Color::nil(),
            LoopFrames::default(),
            inputs_bag(inputs_json),
            Arc::new(DeadHandle),
        )
    }

    /// The bag accessor family: required, optional, defaulted, raw;
    /// error stamping and the loud wrong-type-behind-a-default rule.
    #[test]
    fn bag_resolves_and_stamps_errors() {
        let c = ctx(json!({"url": "http://x", "n": "not-a-number", "keep": true}));
        assert_eq!(c.inputs.get::<String>("url").unwrap(), "http://x");
        assert_eq!(c.inputs.get::<bool>("keep").unwrap(), true);
        assert_eq!(c.inputs.opt::<String>("missing").unwrap(), None);
        assert_eq!(c.inputs.get_or("ttl_days", 30u64).unwrap(), 30);
        assert_eq!(c.inputs.get::<Value>("url").unwrap(), json!("http://x"));
        assert!(c.inputs.raw("missing").is_none());

        // Wrong type stamps an input error naming the input.
        let e = c.inputs.get::<u64>("n").unwrap_err();
        assert!(matches!(&e, WeftError::Input(m) if m.contains("input 'n'")), "{e}");
        // Absent required values name their side.
        let e = c.inputs.get::<String>("missing").unwrap_err().to_string();
        assert!(e.contains("missing required input 'missing'"), "{e}");
        // A defaulted knob still errors loud on a present wrong type.
        assert!(c.inputs.get_or("n", 5u64).is_err());
        // Explicit null reads as absent for opt.
        let c = ctx(json!({"x": null}));
        assert_eq!(c.inputs.opt::<String>("x").unwrap(), None);
    }

    /// A required wake-field read with no payload is a loud error,
    /// never a default; the whole-record read fails loud too.
    #[test]
    fn wake_without_a_payload_fails_loud() {
        let c = ctx(json!({}));
        let e = c.wake.get::<Value>("anything").unwrap_err().to_string();
        assert!(e.contains("missing required wake field 'anything'"), "{e}");
        let e = c.wake.object().unwrap_err().to_string();
        assert!(e.contains("no wake payload was delivered"), "{e}");
    }

    /// An object payload's top-level fields land in the wake bag with
    /// the full accessor family; a non-object payload has no named
    /// fields and stays reachable raw.
    #[test]
    fn wake_bag_reads_object_payload_fields() {
        let bag = ValueBag::wake(Some(&json!({"scheduledTime": "t1", "n": 3})));
        assert_eq!(bag.get::<String>("scheduledTime").unwrap(), "t1");
        assert_eq!(bag.get_or("absent", 7u64).unwrap(), 7);
        assert!(bag.get::<String>("n").is_err(), "present wrong type errors loud");

        let non_object = ValueBag::wake(Some(&json!([1, 2])));
        let e = non_object.get::<Value>("x").unwrap_err().to_string();
        assert!(e.contains("missing required wake field 'x'"), "{e}");
        let e = non_object.object().unwrap_err().to_string();
        assert!(e.contains("wake payload is not an object"), "{e}");
    }

    /// `nested`: an object-valued input read as its own bag. Absent =
    /// an empty bag (defaulted reads all answer); a present non-object
    /// value errors loud, never a silent empty.
    #[test]
    fn nested_reads_an_object_input_as_a_bag() {
        let bag = inputs_bag(json!({"config": {"model": "m", "temperature": 0.2}, "bad": 5}));
        let cfg = bag.nested("config").unwrap();
        assert_eq!(cfg.get::<String>("model").unwrap(), "m");
        assert_eq!(cfg.get_or("absent", 7u64).unwrap(), 7);

        let empty = bag.nested("missing").unwrap();
        assert_eq!(empty.get_or("model", "default".to_string()).unwrap(), "default");

        let e = bag.nested("bad").unwrap_err().to_string();
        assert!(e.contains("not an object"), "{e}");
    }

    /// The whole-record read: the inputs bag always answers; a wake
    /// bag answers exactly the payload's fields.
    #[test]
    fn object_hands_out_the_whole_bag() {
        assert_eq!(inputs_bag(json!({"a": 1})).object().unwrap().len(), 1);
        assert_eq!(inputs_bag(json!({})).object().unwrap().len(), 0);
        let wake = ValueBag::wake(Some(&json!({"a": 1, "b": 2})));
        assert_eq!(wake.object().unwrap().len(), 2);
    }
}

#[cfg(test)]
mod node_input_bag_tests {
    use super::*;
    use serde_json::json;

    /// A minimal NodeDefinition: declared inputs (name, optional default)
    /// + body config.
    fn node(inputs: &[(&str, Option<Value>)], config: serde_json::Value) -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n1", "nodeType": "Test", "label": null,
            "config": config, "position": {"x": 0.0, "y": 0.0},
            "inputs": inputs.iter().map(|(name, default)| json!({
                "name": name, "portType": "String", "required": false,
                "default": default
            })).collect::<Vec<_>>(),
            "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
            "requiresInfra": false, "images": []
        }))
        .expect("test node")
    }

    fn delivered(values: serde_json::Value) -> serde_json::Map<String, Value> {
        values.as_object().unwrap().clone()
    }

    /// Delivered values and braces config values land in the ONE bag;
    /// delivered wins over a same-named braces value.
    #[test]
    fn delivered_and_config_merge_delivered_wins() {
        let n = node(&[("to", None)], json!({"label": "x", "to": "braces"}));
        let bag = node_input_bag(&n, delivered(json!({"to": 10})), &[]);
        assert_eq!(bag.get::<u64>("to").unwrap(), 10, "delivered beats the braces value");
        assert_eq!(bag.get::<String>("label").unwrap(), "x");
    }

    /// No name is special: an OBJECT wired to an input (a config node's
    /// output, an input that happens to be named `config`) arrives as
    /// that object, never spread into the bag. The node reads it and
    /// decides what to do with it.
    #[test]
    fn a_wired_object_arrives_as_that_object() {
        let n = node(&[("prompt", None), ("config", None)], json!({"own": "braces"}));
        let bag = node_input_bag(
            &n,
            delivered(json!({
                "prompt": "hi",
                "config": {"model": "from-config-node", "temperature": 0.2}
            })),
            &[],
        );
        assert_eq!(
            bag.get::<Value>("config").unwrap(),
            json!({"model": "from-config-node", "temperature": 0.2}),
            "the object is data on its input, not spread"
        );
        assert!(bag.raw("model").is_none(), "no key of the object leaks into the bag");
        assert_eq!(bag.get::<String>("own").unwrap(), "braces");
    }

    /// Defaults fill last: an absent declared input gets its default;
    /// wires and braces values beat it; a CLOSED wired input stays
    /// absent (the closure is not masked by the default).
    #[test]
    fn defaults_fill_last_and_never_mask_a_closure() {
        let n = node(
            &[("method", Some(json!("GET"))), ("model", Some(json!("base")))],
            json!({}),
        );
        let bag = node_input_bag(&n, delivered(json!({"model": "wired"})), &[]);
        assert_eq!(bag.get::<String>("method").unwrap(), "GET", "absent input reads its default");
        assert_eq!(bag.get::<String>("model").unwrap(), "wired", "a wired value beats the default");
        // The default shows through the whole-record read too: the bag
        // is one consistent view, get/object/iter never disagree.
        assert_eq!(bag.object().unwrap().get("method"), Some(&json!("GET")));

        let bag = node_input_bag(&n, delivered(json!({})), &["method".to_string()]);
        assert!(bag.raw("method").is_none(), "a closed input is not defaulted");
    }

    /// Compiler/editor plumbing keys living in the config blob
    /// (`parentId`, `_`-reserved) never reach the bag: node bodies
    /// only ever see their own inputs.
    #[test]
    fn internal_config_keys_never_reach_the_bag() {
        let n = node(
            &[("url", None)],
            json!({"url": "http://x", "parentId": "g1", "_label": "My node", "_tags": ["a"]}),
        );
        let bag = node_input_bag(&n, delivered(json!({})), &[]);
        assert_eq!(bag.get::<String>("url").unwrap(), "http://x");
        assert!(bag.raw("parentId").is_none(), "parentId is compiler plumbing, not input data");
        assert!(bag.raw("_label").is_none(), "_-reserved keys are editor plumbing, not input data");
        assert!(bag.raw("_tags").is_none());
        assert_eq!(bag.object().unwrap().len(), 1, "the whole-record read agrees");
    }

    /// `custom()` hands back the instance's DATA inputs only: the node
    /// type's own spec-declared settings are excluded, without the node
    /// body hardcoding its setting names.
    #[test]
    fn custom_excludes_the_specs_own_settings() {
        let n: crate::project::NodeDefinition = serde_json::from_value(json!({
            "id": "n1", "nodeType": "ExecPython", "label": null,
            "config": {"code": "return {}"},
            "position": {"x": 0.0, "y": 0.0}, "scope": [],
            "inputs": [
                {"name": "code", "portType": "String", "required": true, "fromSpec": true},
                {"name": "a", "portType": "Number", "required": false},
            ],
            "outputs": [],
        }))
        .expect("node json");
        let bag = node_input_bag(&n, delivered(json!({"a": 7})), &[]);
        let data: Vec<&str> = bag.custom().map(|(k, _)| k.as_str()).collect();
        assert_eq!(data, vec!["a"], "settings are excluded, instance ports remain");
        assert!(bag.raw("code").is_some(), "the setting is still readable by name");
    }
}

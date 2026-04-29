use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Color;

// ----- Wake signals (unified trigger + suspension mechanism) ----------
//
// A wake signal is "something the dispatcher listens for on behalf of
// a node." When it fires, the dispatcher either spawns a fresh run
// (is_resume=false) or resumes a paused lane (is_resume=true).
//
// The `kind` is a closed set owned by the dispatcher. Parameters per
// instance are open (path, schedule, form fields, ...). New kinds
// ship as new `WakeSignalKind` variants; handlers live in the
// dispatcher. Users never write signal kinds; they pick from the set
// and parameterize.

/// Resolved wake-signal instance. Carries the full signal kind
/// with values. Produced by `WakeSignalKind::resolve_from_config`
/// during enrich for entry-use signals, and built directly by
/// `ctx.await_signal` at runtime for wait/resume signals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeSignalSpec {
    pub kind: WakeSignalKind,
    /// `false` → entry/trigger: persistent, every fire spawns a
    /// fresh execution. `true` → wait/resume: single-use, fire
    /// resumes a specific paused lane and the signal is torn down
    /// afterwards.
    #[serde(default)]
    pub is_resume: bool,
}

/// What a node's `metadata.entry_signals` actually stores: just
/// the kind's tag plus `is_resume`. No value plumbing. Enrich
/// resolves each tag against the node's config into a full
/// `WakeSignalSpec` and writes it into the enriched project.
/// Node authors declare one of these per entry signal; contract
/// for the required config fields lives on each `WakeSignalKind`
/// variant's doc comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeSignalTag {
    pub kind: WakeSignalKindTag,
    #[serde(default)]
    pub is_resume: bool,
}

/// Closed set of wake-signal kinds. Dispatcher has one handler per
/// variant. Phase A ships Webhook, Timer, Form. Socket is reserved
/// for Phase B (Discord gateway, Telegram long-poll, etc.).
/// Resolved wake signal: the full spec the dispatcher operates on.
/// Each variant documents exactly which node config fields it pulls
/// its values from — that's the contract a node author reads to
/// know what fields their node needs. Resolution (reading a node's
/// config into one of these variants) happens in the compiler's
/// enrich pass via `WakeSignalKind::resolve_from_config`; neither
/// the node's Rust code nor the dispatcher touches node-specific
/// field names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WakeSignalKind {
    /// HTTP POST to a dispatcher-minted URL. Body delivered as the
    /// payload.
    ///
    /// **Required config** on the declaring node: none.
    /// **Optional config**:
    ///   - `path: String` — route suffix under `/w/{token}/…`.
    ///     Empty means bare `/w/{token}`.
    ///   - `apiKey: String` — if set, POSTs must carry a matching
    ///     `X-API-Key` header. Absent = no auth.
    Webhook {
        path: String,
        #[serde(default)]
        auth: WebhookAuth,
    },

    /// Scheduled fire.
    ///
    /// **Required config** on the declaring node: exactly one of:
    ///   - `cron: String` — standard cron expression (recurring).
    ///   - `after_ms: u64` — milliseconds-from-now (single-shot).
    ///   - `at: String` — RFC-3339 timestamp (single-shot).
    Timer { spec: TimerSpec },

    /// Form submission with a rendered schema. Extends webhook with
    /// a shape the extension can render. `form_type` routes the
    /// form to the right UI panel (e.g. "human-trigger" vs
    /// "human-query").
    ///
    /// **Required config** on the declaring node:
    ///   - `fields: Array<FormField>` — the form's fields.
    /// **Optional config**:
    ///   - `title: String`.
    ///   - `description: String`.
    ///
    /// `form_type` is not pulled from config: it's a convention
    /// the node hardcodes when it calls `await_signal` in its own
    /// execute body. Entry-use Form signals default to
    /// `"entry-form"`.
    Form {
        form_type: String,
        schema: FormSchema,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },

    /// Long-lived outbound Server-Sent Events subscription. The
    /// listener opens a GET to `url` with
    /// `Accept: text/event-stream`, parses `data: ...` lines,
    /// relays events whose JSON contains `"event": event_name` (or
    /// every event if `event_name` is empty).
    ///
    /// Used by infra-backed triggers (e.g. WhatsAppReceive
    /// subscribes to `bridge-url/events`). Spec-only; nothing on
    /// the node's config.
    Sse {
        /// Full URL the listener subscribes to.
        url: String,
        /// Optional event-name filter. Empty = relay everything.
        #[serde(default)]
        event_name: String,
    },

    /// Long-lived bidirectional connection. Phase B scope; reserved
    /// so the enum shape is known.
    Socket { spec: SocketSpec },
}

/// Bare tag of a `WakeSignalKind`. Node metadata's `entrySignals`
/// list carries one of these per entry signal; the compiler's
/// enrich pass resolves each tag against the node's config to
/// produce a full `WakeSignalKind`. Separating the tag from the
/// resolved spec keeps node metadata free of value plumbing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeSignalKindTag {
    Webhook,
    Timer,
    Form,
    Sse,
    Socket,
}

/// What the compiler returns on a broken signal/config contract.
/// Callers format `{missing}` / `{offending}` fields into their
/// own diagnostics.
#[derive(Debug, Clone)]
pub struct SignalResolveError {
    pub kind: WakeSignalKindTag,
    pub message: String,
}

impl WakeSignalKind {
    /// Build a resolved signal kind from a bare tag + the node's
    /// config. The contract for each kind is documented on its
    /// variant above. This function is the single place the
    /// compiler and the dispatcher agree on where a signal's
    /// parameters live; adding a new kind means adding one arm
    /// here, nothing else in the engine or dispatcher.
    pub fn resolve_from_config(
        tag: WakeSignalKindTag,
        config: &HashMap<String, Value>,
    ) -> Result<Self, SignalResolveError> {
        match tag {
            WakeSignalKindTag::Webhook => {
                let path = config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let auth = if config.get("apiKey").is_some() {
                    WebhookAuth::OptionalApiKey { field: "apiKey".into() }
                } else {
                    WebhookAuth::None
                };
                Ok(Self::Webhook { path, auth })
            }
            WakeSignalKindTag::Timer => {
                // Exactly one of cron / after_ms / at is required.
                let cron = config.get("cron").and_then(|v| v.as_str());
                let after = config.get("after_ms").and_then(|v| v.as_u64());
                let at = config.get("at").and_then(|v| v.as_str());
                let supplied = [cron.is_some(), after.is_some(), at.is_some()]
                    .iter()
                    .filter(|x| **x)
                    .count();
                if supplied == 0 {
                    return Err(SignalResolveError {
                        kind: WakeSignalKindTag::Timer,
                        message: "Timer requires one of config.cron, \
                                  config.after_ms, or config.at"
                            .into(),
                    });
                }
                if supplied > 1 {
                    return Err(SignalResolveError {
                        kind: WakeSignalKindTag::Timer,
                        message: "Timer has more than one of config.cron, \
                                  config.after_ms, config.at set; pick one"
                            .into(),
                    });
                }
                let spec = if let Some(expr) = cron {
                    TimerSpec::Cron { expression: expr.to_string() }
                } else if let Some(ms) = after {
                    TimerSpec::After { duration_ms: ms }
                } else {
                    let raw = at.unwrap();
                    let when = chrono::DateTime::parse_from_rfc3339(raw)
                        .map_err(|e| SignalResolveError {
                            kind: WakeSignalKindTag::Timer,
                            message: format!("config.at is not a valid RFC-3339 timestamp: {e}"),
                        })?
                        .with_timezone(&chrono::Utc);
                    TimerSpec::At { when }
                };
                Ok(Self::Timer { spec })
            }
            WakeSignalKindTag::Form => {
                let fields_raw = config.get("fields").ok_or_else(|| SignalResolveError {
                    kind: WakeSignalKindTag::Form,
                    message: "Form requires config.fields".into(),
                })?;
                // The weft parser encodes `fields: [...]` as the raw
                // bracketed text, so config.fields can be an Array
                // OR a String wrapping JSON. Accept both; the node's
                // runtime side does the same normalization.
                let parsed: Vec<Value> = match fields_raw {
                    Value::Array(a) => a.clone(),
                    Value::String(s) => serde_json::from_str(s).map_err(|e| {
                        SignalResolveError {
                            kind: WakeSignalKindTag::Form,
                            message: format!("config.fields string is not JSON: {e}"),
                        }
                    })?,
                    _ => {
                        return Err(SignalResolveError {
                            kind: WakeSignalKindTag::Form,
                            message: "config.fields must be an array or a JSON string".into(),
                        });
                    }
                };
                let _ = parsed;
                let title = config
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let description = config
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                Ok(Self::Form {
                    form_type: "entry-form".to_string(),
                    schema: FormSchema {
                        title: title.clone().unwrap_or_default(),
                        description: description.clone(),
                        fields: Vec::new(),
                    },
                    title,
                    description,
                })
            }
            WakeSignalKindTag::Sse => {
                // SSE signals are constructed at runtime in the
                // trigger node's execute (TriggerSetup phase); they
                // don't come from pre-declared config. If a node
                // were to declare `entrySignals: [{kind: "sse"}]`
                // it would need config support here. Today only the
                // TriggerSetup-phase code path builds SSE specs.
                Err(SignalResolveError {
                    kind: WakeSignalKindTag::Sse,
                    message: "SSE wake signals are not declarable in metadata; \
                              build them at runtime via ctx.register_signal"
                        .into(),
                })
            }
            WakeSignalKindTag::Socket => Err(SignalResolveError {
                kind: WakeSignalKindTag::Socket,
                message: "Socket wake signals are Phase B; not implemented".into(),
            }),
        }
    }
}

/// Timer specification. `After` and `At` are single-fire, `Cron`
/// recurs until the signal is torn down.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimerSpec {
    After {
        /// Milliseconds until the signal fires.
        duration_ms: u64,
    },
    At {
        when: chrono::DateTime<chrono::Utc>,
    },
    Cron {
        expression: String,
    },
}

/// Placeholder for Phase B socket kinds. Not implemented yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocketSpec {
    pub protocol: String,
    pub endpoint: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WebhookAuth {
    #[default]
    None,
    OptionalApiKey {
        field: String,
    },
    RequiredApiKey {
        field: String,
    },
    HmacSignature {
        secret_field: String,
        header: String,
    },
}

// ----- Form primitives ------------------------------------------------
//
// FormField carries the runtime form's field-level data the way v1
// did: a string `field_type` (keyed against the node's
// FormFieldSpecs), a free-form `render` hint the consumer (browser
// extension, dashboard) interprets, the optional pre-fill `value`
// (for display/prefilled fields), and the original `config` (for
// options, labels, custom button copy). Keeping the schema close to
// the spec means the dispatcher doesn't have to lossily collapse
// catalog-defined field types into a fixed enum, and consumers can
// render anything the catalog declares.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSchema {
    pub title: String,
    pub description: Option<String>,
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
    pub field_type: String,
    pub key: String,
    pub label: String,
    /// Render hint copied from the spec (component name + flags).
    /// The dashboard / browser extension reads `render.component`
    /// to pick the UI primitive.
    #[serde(default)]
    pub render: Value,
    /// Pre-fill value for fields that need an upstream input port
    /// value (display, display_image, editable_*, *_input). None
    /// for purely interactive fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Per-field config from the source (options, labels, etc).
    #[serde(default)]
    pub config: Value,
}

// ----- Cost report (fire-and-forget primitive) ------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostReport {
    pub service: String,
    pub model: Option<String>,
    pub amount_usd: f64,
    pub metadata: Value,
}

// ----- Execution snapshot ---------------------------------------------
//
// Written by the worker when it stalls (all lanes either terminal or
// waiting). The dispatcher stores this in the journal and hands it to
// the next worker invocation so the run continues exactly where it
// left off. See docs/v2-design.md §3.5.

/// Durable snapshot of an execution's in-progress state. Contains
/// everything a new worker needs to resume: the pulse table, the
/// per-node execution records, and the active suspensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub color: Color,
    pub pulses: crate::pulse::PulseTable,
    pub executions: crate::exec::NodeExecutionTable,
    pub suspensions: HashMap<String, SuspensionInfo>,
    /// Fires that arrived for live suspensions but haven't been
    /// consumed by a worker's node completion yet. The worker
    /// seeds these into its link on startup so every waiting node
    /// finds its value when re-dispatched. Survives worker restarts
    /// because it's derived from journal events, not slot queues.
    #[serde(default)]
    pub pending_deliveries: HashMap<String, Value>,
}

/// Per-paused-lane info stored in the snapshot. `token` is the key in
/// the outer HashMap. Enough to: identify the waiting node/lane,
/// re-register the signal on resume (Phase A always re-registers on
/// fresh worker start), and route the deliver value back to the
/// right oneshot when the fire arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuspensionInfo {
    pub node_id: String,
    pub lane: crate::lane::Lane,
    pub spec: WakeSignalSpec,
    pub created_at_unix: u64,
}

// ----- WebSocket protocol (dispatcher <-> worker) --------------------
//
// Phase A slice 0 defines the message shapes only. Slice 3 plumbs
// them through an actual WebSocket. Until then the engine still uses
// HTTP; these types sit here waiting to be wired.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatcherToWorker {
    /// First message after the worker's Ready handshake. Carries the
    /// initial wake plus any snapshot to resume from and any wakes
    /// that queued while the worker was starting.
    Start {
        wake: WakeMessage,
        /// Folded state of the event log: pulses, executions,
        /// suspensions, and `pending_deliveries` (fires not yet
        /// consumed by a node completion). The worker seeds every
        /// `pending_delivery` into its link on startup.
        snapshot: Option<ExecutionSnapshot>,
        /// Identifier minted by the dispatcher for this worker
        /// session. The worker echoes it back in a
        /// `Reconnected { worker_instance_id }` after a socket drop.
        #[serde(default)]
        worker_instance_id: Option<String>,
    },
    /// A wake signal fired while the worker is Live. Deliver to the
    /// lane that registered `token`.
    Deliver(Delivery),
    /// Dispatcher reply to a `SuspensionRequest`. On failure
    /// (listener spawn failed, transient cluster error) `error` is
    /// `Some(msg)` and `token` is the empty string; the worker
    /// surfaces a per-node failure for the requesting lane and
    /// keeps the WS open.
    SuspensionToken {
        request_id: u64,
        token: String,
        user_url: Option<String>,
        #[serde(default)]
        error: Option<String>,
    },
    /// Dispatcher reply to `RegisterSignalRequest`. `user_url` is
    /// the listener-minted externally-facing URL (if the signal
    /// kind has one; None for Timer / Socket / SSE). `error` set
    /// when the listener spawn or register fails; same per-node
    /// failure semantics as `SuspensionToken`.
    RegisterSignalAck {
        request_id: u64,
        token: String,
        user_url: Option<String>,
        #[serde(default)]
        error: Option<String>,
    },
    /// Dispatcher reply to `SidecarEndpointRequest`. On failure
    /// (infra not up, unknown node), `endpoint` is `None` and
    /// the caller surfaces an error.
    SidecarEndpoint {
        request_id: u64,
        endpoint: Option<String>,
    },
    /// Dispatcher reply to `ProvisionSidecarRequest`. Carries
    /// the handle on success, `error` filled on failure.
    ProvisionSidecarReply {
        request_id: u64,
        instance_id: Option<String>,
        endpoint_url: Option<String>,
        error: Option<String>,
    },
    /// Dispatcher acknowledgement for `Stalled`. The worker stays
    /// parked: it stops dispatching new nodes but keeps the WS
    /// alive listening for `Deliver` (a fire arriving during the
    /// grace window) or `Exit` (grace expired, time to die).
    StalledAck,
    /// Grace window for a stalled worker has expired. Worker
    /// should exit cleanly. The dispatcher kills the pod after
    /// receiving the WS close.
    Exit,
    /// Cancel the execution. Worker should stop ASAP.
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerToDispatcher {
    /// First connection for this color; ready to receive `Start`.
    Ready,
    /// Reconnect after a transient socket drop. The worker kept its
    /// in-memory state (pulse table, executions, pending
    /// suspensions); the dispatcher should resume streaming wakes
    /// over the new socket instead of folding events and spawning a
    /// replacement. `worker_instance_id` lets the dispatcher reject
    /// a stale reconnect if it has already given up on the old
    /// worker and spawned a new one.
    Reconnected { worker_instance_id: String },
    /// Worker wants to register a wake signal for a mid-execution
    /// suspension. Dispatcher replies with `SuspensionToken`.
    SuspensionRequest {
        request_id: u64,
        node_id: String,
        lane: crate::lane::Lane,
        spec: WakeSignalSpec,
    },
    /// Worker wants to register an entry wake signal (TriggerSetup
    /// phase). Dispatcher forwards the spec to the project's
    /// listener, journals the registration, replies with
    /// `RegisterSignalAck` carrying the user-facing URL (if any).
    RegisterSignalRequest {
        request_id: u64,
        node_id: String,
        spec: WakeSignalSpec,
    },
    /// Worker wants the cluster-local endpoint URL of its
    /// sidecar. Dispatcher resolves via InfraRegistry, replies
    /// with `SidecarEndpoint`.
    SidecarEndpointRequest {
        request_id: u64,
        node_id: String,
    },
    /// Worker (an infra node running in `Phase::InfraSetup`)
    /// wants its sidecar provisioned. Dispatcher delegates to
    /// `InfraBackend::provision`, registers the instance, and
    /// replies with `ProvisionSidecarReply`.
    ProvisionSidecarRequest {
        request_id: u64,
        node_id: String,
        spec: crate::node::SidecarSpec,
    },
    /// Worker has nothing left to run and at least one lane is
    /// waiting. Under the event-sourced model the dispatcher already
    /// has every state-change in its event log; the worker just
    /// says "I'm parking" and exits after `StalledAck`. No snapshot
    /// payload needed.
    Stalled,
    /// Execution finished normally.
    Completed {
        outputs: Value,
    },
    /// Execution terminally failed.
    Failed {
        error: String,
    },
    /// First dispatch of a (node, lane) attempt. Creates the
    /// execution record on the dispatcher side. `pulses_absorbed`
    /// is the engine-minted UUIDs of the pulses this attempt
    /// consumed; the fold flips those to Absorbed by exact match.
    NodeStarted {
        node_id: String,
        lane: String,
        input: Value,
        #[serde(default)]
        pulses_absorbed: Vec<String>,
    },
    /// The current attempt called `await_signal` and parked. The
    /// execution record's state flips Running → Suspended; the
    /// `token` ties it to the wake signal registered with the
    /// listener. No new exec record, no new pulse activity.
    NodeSuspended {
        node_id: String,
        lane: String,
        token: String,
    },
    /// The current attempt got its delivery and is continuing on
    /// the same record. State flips Suspended → Running. Carries
    /// the delivered value for replay correctness.
    NodeResumed {
        node_id: String,
        lane: String,
        token: String,
        value: Value,
    },
    /// The current attempt finished successfully. Closes the
    /// attempt and the record's state.
    NodeCompleted {
        node_id: String,
        lane: String,
        output: Value,
    },
    /// The current attempt failed. Closes the attempt with an
    /// error; the record's state flips Failed (unless a future
    /// retry policy opens a new attempt via NodeRetried).
    NodeFailed {
        node_id: String,
        lane: String,
        error: String,
    },
    /// The dispatch was skipped (e.g. group-boundary skip, pulse
    /// arrived but no work to do).
    NodeSkipped {
        node_id: String,
        lane: String,
    },
    /// User-initiated cancellation interrupted this attempt. The
    /// dispatcher emits this for every non-terminal record on the
    /// `cancel_color` path; the worker can also emit it if a node
    /// observes its own task being cancelled mid-run. Closes the
    /// record's state to Cancelled with `reason` as the error.
    NodeCancelled {
        node_id: String,
        lane: String,
        reason: String,
    },
    /// The previous attempt failed and a fresh attempt is opening
    /// on the same execution record. Future-proofing: today no
    /// path emits this, but the fold handles it so retries can
    /// land without further wire-protocol changes.
    NodeRetried {
        node_id: String,
        lane: String,
        reason: String,
    },
    /// Pulse-table mutation: one or more pulses were emitted on
    /// downstream edges. The engine ships this immediately after
    /// each `postprocess_output` call; the dispatcher journals one
    /// `PulseEmitted` per entry. The carried `pulse_id`s are the
    /// UUIDs the engine actually minted in its pulse table, so
    /// replay reconstructs the exact same pulses (and `NodeStarted`
    /// events later flip them to Absorbed by UUID match).
    PulsesEmitted {
        pulses: Vec<EmittedPulse>,
    },
    /// Pulse-table mutation: an Expand work item ran in
    /// `preprocess`. The absorbed pulse flips to Absorbed; the
    /// engine appends N child-lane pulses to the same node's
    /// bucket. `lane_suffix` carries the lane frames the Expand
    /// added (one frame per `lane_depth` level; `expand_recursive`
    /// can produce multiple frames in a single event).
    PulsesExpanded {
        node_id: String,
        port: String,
        absorbed_pulse_id: String,
        color: Color,
        base_lane: crate::lane::Lane,
        children: Vec<ExpandedChild>,
    },
    /// Pulse-table mutation: a Gather work item ran in
    /// `preprocess`. The absorbed pulses flip to Absorbed; the
    /// engine appends one parent-lane pulse with `gathered: true`.
    PulsesGathered {
        node_id: String,
        port: String,
        absorbed_pulse_ids: Vec<String>,
        color: Color,
        parent_lane: crate::lane::Lane,
        pulse_id: String,
        value: Value,
    },
    /// Free-form log line (maps to dispatcher journal + SSE).
    Log {
        level: String,
        message: String,
    },
    /// Cost attribution (aggregated by dispatcher for the color).
    Cost(CostReport),
}

/// Wake kinds in the per-color slot queue. Entry wakes start a new
/// How a worker should bootstrap its pulse loop.
///
/// `Fresh` covers everything that starts a new run: manual runs
/// (seeds are every root of the upstream-of-outputs subgraph) and
/// trigger fires (seeds are every root of the subgraph feeding
/// outputs downstream of the firing trigger, with the firing
/// trigger's root carrying the payload and others carrying null).
/// Either way, the dispatcher computes the seeds and the worker
/// just consumes them.
///
/// `Resume` carries no seed data: the worker restores state from
/// the Start snapshot (including `pending_deliveries` for any
/// fires that arrived).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WakeMessage {
    Fresh {
        seeds: Vec<RootSeed>,
        /// Which lifecycle phase this run is for. Propagated to every
        /// node's `ExecutionContext.phase`. Resume runs don't carry
        /// a phase because they always continue the original run's
        /// phase (the snapshot carries it).
        #[serde(default)]
        phase: crate::context::Phase,
    },
    Resume,
}

/// Root seed for manual runs. Pulse is synthesized on the `__seed__`
/// port; nodes with no inputs become ready immediately. The
/// dispatcher mints `pulse_id` and journals the same UUID in the
/// `PulseSeeded` event, so a fresh worker's fold reconstructs the
/// seed pulse with the same identity the live worker used and
/// `NodeStarted.pulses_absorbed` matches by exact UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootSeed {
    pub node_id: String,
    pub pulse_id: String,
    #[serde(default)]
    pub value: Value,
}

/// A single fire of a `is_resume=true` wake signal, delivered to the
/// lane that registered `token`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delivery {
    pub token: String,
    pub value: Value,
}

/// A single pulse the engine produced for a downstream node
/// during postprocess. The `pulse_id` is the UUID the engine
/// minted in its pulse table; the dispatcher journals one
/// `PulseEmitted` per entry, and `NodeStarted.pulses_absorbed`
/// later carries the same UUID so replay can flip the matching
/// pulse to Absorbed by exact match (no counting heuristics).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmittedPulse {
    pub pulse_id: String,
    pub source_node: String,
    pub source_port: String,
    pub target_node: String,
    pub target_port: String,
    pub lane: crate::lane::Lane,
    pub value: Value,
}

/// One leaf produced by an Expand operation. `lane_suffix` is the
/// list of frames the Expand added (could be > 1 when
/// `lane_depth > 1` peels multiple list layers in a single
/// operation). `pulse_id` is the UUID the engine minted for the
/// child pulse so replay can flip it to Absorbed by exact match
/// when the resulting `NodeStarted` arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandedChild {
    pub pulse_id: String,
    pub lane_suffix: crate::lane::Lane,
    pub value: Value,
}

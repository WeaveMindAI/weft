//! Engine-side `ContextHandle`. Lifecycle events go straight to
//! the journal via the broker. Control-plane round-trips
//! (`await_signal`, `register_signal`) go through the dispatcher's
//! task queue (also via the broker): the worker enqueues a task row
//! and waits for completion. Resume values are seeded into the
//! per-(node, lane) await sequence by the loop driver at boot from
//! the journal fold; the body's `await_signal` calls pop entries in
//! call_index order.
//!
//! Infra-provision (the engine-side counterpart to user code's
//! `Node::provision` returning an `InfraSpec`) is driven by the loop
//! driver, NOT by methods on `RunnerHandle`. The loop driver calls
//! `node.provision`, compiles + hashes the returned spec locally,
//! reads prior applied state via the broker, makes a local
//! skip/fresh/replace decision, and (when not Skip) enqueues an
//! `Apply` lifecycle command via `apply_via_supervisor`. The tenant's
//! supervisor pod handles the kubectl work. Once apply completes,
//! the loop driver runs `node.execute` with `Phase::InfraSetup`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use weft_core::cancellation::CancellationFlag;
use weft_core::context::{ContextHandle, LogLevel};
use weft_core::error::{WeftError, WeftResult};
use weft_core::primitive::{CostReport, SignalSpec};
use weft_core::Color;

use crate::now_unix;
use weft_infra::InfraReader;
use weft_journal::{ExecEvent, JournalClient};

use weft_task_store::tasks as task_store;
use weft_task_store::{TaskKind, TaskStoreClient};

/// Serialize a lane into the canonical string used in task dedup keys.
/// One definition so the side-effect-task and register-signal-task
/// dedup keys can't drift, and so a serialization failure is surfaced
/// (a swallowed `unwrap_or_default()` would collapse distinct lanes to
/// the same empty key and silently drop a task). Lane serialization
/// shouldn't fail in practice, which is exactly why a failure must be
/// loud rather than masked.
fn lane_dedup_key(lane: &weft_core::lane::Lane) -> Result<String, serde_json::Error> {
    serde_json::to_string(lane)
}

/// Bundle of broker-backed clients the engine threads everywhere.
/// Each handle clones cheaply (every field is `Arc<dyn _>`).
#[derive(Clone)]
pub struct EngineClients {
    pub journal: Arc<dyn JournalClient>,
    pub tasks: Arc<dyn TaskStoreClient>,
    pub infra: Arc<dyn InfraReader>,
    /// Broker client for infra applied-state reads + apply enqueue.
    /// Used by the loop driver during `Phase::InfraSetup` to make the
    /// skip/fresh/replace decision locally, then ship the spec to the
    /// supervisor via the `infra_lifecycle_command` table.
    pub infra_state: Arc<dyn InfraStateClient>,
    /// Clock the engine uses for every time-related decision
    /// (deadlines, polling intervals). Production passes the
    /// real clock; layer-3 tests pass `FakeClock` so deadlines
    /// can be exercised without burning real wall-clock seconds.
    pub clock: Arc<dyn weft_platform_traits::Clock>,
}

/// Trait surface over `BrokerInfraStateClient` so tests can inject a
/// no-op (or recording) implementation. Production has one impl: the
/// broker-backed HTTP client.
///
/// The trait has two operations: `enqueue_apply` (ship a fresh spec
/// to the supervisor) and `wait_apply` (poll the resulting command
/// row to terminal). The supervisor owns every other concern
/// end-to-end: read prior `infra_node`, compile + hash, decide
/// skip / fresh / replace, run kubectl, update the row. The worker
/// just hands off the spec and waits.
#[async_trait]
pub trait InfraStateClient: Send + Sync {
    async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> anyhow::Result<i64>;

    async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse>;
}

#[async_trait]
impl InfraStateClient for weft_broker_client::client::BrokerInfraStateClient {
    async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> anyhow::Result<i64> {
        self.enqueue_apply(project_id, node_id, spec_json).await
    }
    async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
        self.wait_apply(project_id, command_id).await
    }
}

/// Round-trip timeout for control-plane tasks. Generous because
/// some involve listener spawn + Pod readiness wait.
const TASK_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const TASK_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Best-effort journal write stamped with this Pod's name for fencing.
/// Logs and swallows errors instead of propagating: a transient DB
/// hiccup must not fail a node mid-flight (the engine keeps going and
/// the next event write retries against a fresh broker request). The
/// fencing trigger rejects writes from a Pod whose row is not in
/// {spawning, alive}; that rejection surfaces here as a logged warning,
/// the in-flight tokio task panics out (anyhow upstack) and the slot
/// frees.
async fn record_from_pod(journal: &dyn JournalClient, event: ExecEvent, pod_name: &str) {
    if let Err(e) = journal.record_event(&event, Some(pod_name)).await {
        tracing::warn!(
            target: "weft_engine::journal",
            error = %e,
            "journal write failed"
        );
    }
}


pub struct RunnerHandle {
    execution_id: String,
    project_id: String,
    color: Color,
    node_id: String,
    node_lane: weft_core::lane::Lane,
    /// Broker-backed clients: journal writes, task enqueue, and
    /// infra reads (infra endpoint lookup) all flow through here.
    clients: EngineClients,
    /// k8s Pod name stamped on every journal write so the fencing
    /// trigger can reject writes from a Pod that has been drained or
    /// reaped.
    pod_name: String,
    /// Tenant id stamped on every task this handle enqueues. The
    /// dispatcher's listener reaper queries the task table by tenant
    /// to tell "in-flight register" from "genuinely idle" before
    /// killing the per-tenant listener pod.
    tenant_id: String,
    cancellation: Arc<CancellationFlag>,
    /// Pre-loaded sequence of past `await_signal` calls for this
    /// (node, lane), seeded by the loop driver from the journal
    /// fold on every dispatch. Each `await_signal` call inside the
    /// node body pops the next entry:
    ///   - resolved=Some(value): return it instantly (replay path).
    ///   - resolved=None (the still-pending tail): suspend with
    ///     this token (we're being re-dispatched but our fire
    ///     hasn't arrived for THIS call yet).
    ///   - exhausted: this is a fresh await; enqueue register_signal
    ///     with the next call_index.
    /// The `Mutex` is just for interior mutability across the
    /// `&self` of the trait method; calls are serialized by the
    /// node body (single Future polling).
    awaited_sequence: Mutex<std::collections::VecDeque<weft_core::primitive::AwaitedEntry>>,
    /// 0-based ordinal of the NEXT `await_signal` call within this
    /// (node, lane) execution. Increments on every call. Combined
    /// with the per-(node, lane) sequence above, it determines
    /// whether the call replays or registers fresh.
    next_call_index: AtomicU32,
    /// 0-based ordinal of the NEXT side-effect call (`report_cost`,
    /// `log`) within this (node, lane). Separate counter from
    /// `next_call_index` so side effects don't shift the replay
    /// alignment of `await_signal` / `run_step`. Used to form
    /// stable dedup keys on the broker task table: under replay,
    /// the body re-runs and emits the same side-effect calls in
    /// the same order, so the dedup keys collapse to the same row.
    next_side_effect_index: AtomicU32,
    /// Number of `register_signal` (entry trigger) calls this node
    /// has made on this invocation. Entry triggers are a one-shot
    /// thing per node per TriggerSetup; we fail loudly on a second
    /// call rather than silently colliding on the dedup key.
    entry_register_count: AtomicU32,
}

impl RunnerHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: String,
        project_id: String,
        color: Color,
        node_id: String,
        node_lane: weft_core::lane::Lane,
        clients: EngineClients,
        pod_name: String,
        tenant_id: String,
        cancellation: Arc<CancellationFlag>,
    ) -> Self {
        Self {
            execution_id,
            project_id,
            color,
            node_id,
            node_lane,
            clients,
            pod_name,
            tenant_id,
            cancellation,
            awaited_sequence: Mutex::new(std::collections::VecDeque::new()),
            next_call_index: AtomicU32::new(0),
            next_side_effect_index: AtomicU32::new(0),
            entry_register_count: AtomicU32::new(0),
        }
    }

    fn next_side_effect_index(&self) -> u32 {
        self.next_side_effect_index.fetch_add(1, Ordering::SeqCst)
    }

    /// Enqueue a side-effect task (cost report, log line, etc.) on
    /// the broker. The broker's INSERT into `task` is the durable
    /// commit; once `Ok` returns, the dispatcher will journal the
    /// event regardless of whether this worker pod survives.
    ///
    /// `dedup_prefix` is the human-readable label used in the dedup
    /// key (`"report_cost"`, `"log"`, ...); the rest of the key
    /// keys to (color, node, lane, side-effect-index) so distinct
    /// calls in one node body produce distinct tasks while a retry
    /// of the same call (e.g. supervisor reconnect, body replay)
    /// collapses to one. Replay is honest because the body re-runs
    /// in the same order and emits the same side-effect sequence.
    async fn enqueue_side_effect_task<P: serde::Serialize>(
        &self,
        dedup_prefix: &str,
        kind: weft_task_store::TaskKind,
        payload: P,
    ) -> WeftResult<()> {
        let payload_json = serde_json::to_value(&payload).map_err(|e| {
            WeftError::Config(format!("{dedup_prefix} payload: {e}"))
        })?;
        let lane = lane_dedup_key(&self.node_lane)
            .map_err(|e| WeftError::Config(format!("{dedup_prefix} lane key: {e}")))?;
        let dedup_key = format!(
            "{dedup_prefix}:{color}:{node}:{lane}:{idx}",
            color = self.color,
            node = self.node_id,
            idx = self.next_side_effect_index(),
        );
        self.clients
            .tasks
            .enqueue_dedup(weft_task_store::NewTask {
                kind,
                target: weft_task_store::TaskTarget::Dispatcher,
                project_id: Some(self.project_id.clone()),
                dedup_key: Some(dedup_key),
                color: Some(self.color.to_string()),
                tenant_id: Some(self.tenant_id.clone()),
                target_pod_name: None,
                payload: payload_json,
            })
            .await
            .map_err(|e| WeftError::Config(format!("{dedup_prefix} enqueue: {e}")))?;
        Ok(())
    }

    /// Seed the per-(node, lane) await-call sequence the loop
    /// driver pulled from the journal fold. Replaces
    /// `with_expected_token` from the single-await world: now we
    /// have an ordered sequence, not just one token.
    pub fn with_awaited_sequence(
        mut self,
        sequence: Vec<weft_core::primitive::AwaitedEntry>,
    ) -> Self {
        self.awaited_sequence = Mutex::new(sequence.into());
        self
    }
}

/// Ship `NodeStarted`: first dispatch of (node, lane). Writes the
/// event directly to the journal. The dispatcher's fold reads it on
/// the next snapshot rebuild.
pub async fn ship_node_started(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    input: &serde_json::Value,
    pulses_absorbed: &[uuid::Uuid],
) {
    record_from_pod(
        journal,
        ExecEvent::NodeStarted {
            color,
            node_id: node_id.to_string(),
            lane: lane.clone(),
            input: input.clone(),
            pulses_absorbed: pulses_absorbed.iter().map(|u| u.to_string()).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_suspended(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    token: &str,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeSuspended {
            color,
            node_id: node_id.to_string(),
            lane: lane.clone(),
            token: token.to_string(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_resumed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    token: &str,
    value: &serde_json::Value,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeResumed {
            color,
            node_id: node_id.to_string(),
            lane: lane.clone(),
            token: token.to_string(),
            value: value.clone(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_completed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    output: &serde_json::Value,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeCompleted {
            color,
            node_id: node_id.to_string(),
            lane: lane.clone(),
            output: output.clone(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_failed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    error: &str,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeFailed {
            color,
            node_id: node_id.to_string(),
            lane: lane.clone(),
            error: error.to_string(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_skipped(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeSkipped {
            color,
            node_id: node_id.to_string(),
            lane: lane.clone(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

/// Ship every pulse-table mutation the engine just performed by
/// writing one journal event per mutation. Order is preserved
/// because journal rows have monotonic ids; the fold replays them
/// in insertion order.
pub async fn ship_pulse_mutations(
    journal: &dyn JournalClient,
    pod_name: &str,
    mutations: Vec<weft_core::exec::PulseMutation>,
) {
    if mutations.is_empty() {
        return;
    }
    for m in mutations {
        match m {
            weft_core::exec::PulseMutation::Emitted {
                pulse_id,
                source_node,
                source_port,
                target_node,
                target_port,
                color,
                lane,
                value,
            } => {
                record_from_pod(
                    journal,
                    ExecEvent::PulseEmitted {
                        color,
                        pulse_id: pulse_id.to_string(),
                        source_node,
                        source_port,
                        target_node,
                        target_port,
                        lane,
                        value,
                        at_unix: now_unix(),
                    },
                    pod_name,
                )
                .await;
            }
            weft_core::exec::PulseMutation::Expanded {
                node_id,
                port,
                absorbed_pulse_id,
                color,
                base_lane,
                children,
            } => {
                let children = children
                    .into_iter()
                    .map(|c| weft_journal::ExpandedChildRecord {
                        pulse_id: c.pulse_id.to_string(),
                        lane_suffix: c.lane_suffix,
                        value: c.value,
                    })
                    .collect();
                record_from_pod(
                    journal,
                    ExecEvent::PulsesExpanded {
                        color,
                        node_id,
                        port,
                        absorbed_pulse_id: absorbed_pulse_id.to_string(),
                        base_lane,
                        children,
                        at_unix: now_unix(),
                    },
                    pod_name,
                )
                .await;
            }
            weft_core::exec::PulseMutation::Gathered {
                node_id,
                port,
                absorbed_pulse_ids,
                color,
                parent_lane,
                pulse_id,
                value,
            } => {
                record_from_pod(
                    journal,
                    ExecEvent::PulsesGathered {
                        color,
                        node_id,
                        port,
                        absorbed_pulse_ids: absorbed_pulse_ids
                            .into_iter()
                            .map(|u| u.to_string())
                            .collect(),
                        parent_lane,
                        pulse_id: pulse_id.to_string(),
                        value,
                        at_unix: now_unix(),
                    },
                    pod_name,
                )
                .await;
            }
        }
    }
}

#[async_trait]
impl ContextHandle for RunnerHandle {
    /// Wait-and-resume primitive. Generalized to N awaits per body:
    /// each call has a 0-based call_index keyed on (node, lane).
    /// On every dispatch, the runtime pre-loads the per-(node, lane)
    /// awaited sequence from the journal fold:
    ///
    /// 1. **Replay path** (entry has `resolved=Some`): the call's
    ///    fire arrived in a prior cycle; return the stored value
    ///    instantly. Body keeps running.
    /// 2. **Suspend path** (entry has `resolved=None`): the call's
    ///    suspension is the still-pending tail of the sequence;
    ///    propagate `WeftError::Suspended` to release the worker.
    /// 3. **Fresh path** (sequence exhausted): no past entry for
    ///    this call_index; this is the first time this await runs.
    ///    Enqueue `register_signal` with is_resume=true and the
    ///    current call_index, then propagate `Suspended`. The next
    ///    re-dispatch after the fire will see this entry resolved.
    ///
    /// The author writes `let x = ctx.await_signal(...).await?;` N
    /// times in a row; each call is replay-instant on resume.
    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value> {
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);

        // Pop the next pre-loaded entry (if any). Replay vs suspend
        // depends on whether its fire already arrived.
        let next_entry = {
            let mut seq = self
                .awaited_sequence
                .lock()
                .expect("awaited_sequence mutex poisoned");
            seq.pop_front()
        };
        if let Some(entry) = next_entry {
            // Sanity-check call_index alignment. If the journal's
            // sequence disagrees with our counter, something
            // replayed out of order; the body is non-deterministic
            // (or the journal is corrupt). Fail the node loudly
            // rather than masking it as a suspension.
            if entry.call_index != call_index {
                return Err(WeftError::NodeExecution(format!(
                    "await_signal call_index mismatch (counter={call_index}, journal={}). \
                     This means the node body's call order changed between replays. \
                     Wrap any non-deterministic logic between awaits in `ctx.run`.",
                    entry.call_index
                )));
            }
            match entry.kind {
                weft_core::primitive::AwaitedEntryKind::Await {
                    token,
                    resolved,
                } => match resolved {
                    Some(value) => return Ok(value),
                    None => {
                        // Pending tail: fire hasn't arrived. Suspend
                        // with the existing token so the dispatcher
                        // doesn't re-register.
                        return Err(WeftError::Suspended { token });
                    }
                },
                weft_core::primitive::AwaitedEntryKind::Run { name, .. } => {
                    return Err(WeftError::NodeExecution(format!(
                        "await_signal at call_index={call_index} but journal has Run('{name}'). \
                         This means the node body called `ctx.run` here on a previous run \
                         and `ctx.await_signal` now; non-deterministic bodies are not safe \
                         to replay. Use `ctx.run` to wrap non-deterministic work."
                    )));
                }
            }
        }

        // Sequence exhausted: this is a fresh await. Enqueue a
        // register_signal task carrying our call_index so the
        // dispatcher journals SuspensionRegistered with the right
        // ordinal. Body propagates Suspended afterwards.
        let reply = enqueue_register_signal_task(
            self.clients.tasks.as_ref(),
            self.color,
            &self.node_id,
            &self.node_lane,
            &spec,
            true,
            &self.tenant_id,
            call_index,
        )
        .await
        .map_err(|e| WeftError::Suspension(format!("request token: {e}")))?;
        tracing::info!(
            target: "weft_engine::suspend",
            node = %self.node_id,
            color = %self.color,
            call_index = call_index,
            token = %reply.token,
            "await_signal: registered; returning Suspended",
        );
        Err(WeftError::Suspended { token: reply.token })
    }

    /// Replay-side of `ctx.run`. Pops the next entry in the
    /// (node, lane) sequence; if it's a Run with our call_index,
    /// return its journaled value. If it's an Await at our index,
    /// the body's call sequence drifted from the journal: error
    /// loudly. If the sequence is exhausted, return None to signal
    /// "fresh path" so the wrapper invokes the closure.
    async fn run_step(&self, name: &str) -> WeftResult<(u32, Option<Value>)> {
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);
        let next_entry = {
            let mut seq = self
                .awaited_sequence
                .lock()
                .expect("awaited_sequence mutex poisoned");
            seq.pop_front()
        };
        match next_entry {
            Some(entry) => {
                if entry.call_index != call_index {
                    return Err(WeftError::NodeExecution(format!(
                        "ctx.run('{name}') call_index mismatch (counter={call_index}, journal={}). \
                         This means the node body's call order changed between replays. \
                         Wrap any non-deterministic logic in `ctx.run`.",
                        entry.call_index
                    )));
                }
                match entry.kind {
                    weft_core::primitive::AwaitedEntryKind::Run { value, .. } => {
                        Ok((call_index, Some(value)))
                    }
                    weft_core::primitive::AwaitedEntryKind::Await { .. } => {
                        Err(WeftError::NodeExecution(format!(
                            "ctx.run('{name}') at call_index={call_index} but journal has Await. \
                             This means the node body called `ctx.await_signal` here on a previous \
                             run and `ctx.run` now; non-deterministic bodies are not safe to \
                             replay."
                        )))
                    }
                }
            }
            None => Ok((call_index, None)),
        }
    }

    async fn run_record(&self, name: &str, call_index: u32, value: &Value) -> WeftResult<()> {
        // call_index is the value run_step returned, passed in so
        // run_step and run_record agree on the index explicitly
        // rather than via a shared counter both sides read.
        record_from_pod(
            self.clients.journal.as_ref(),
            ExecEvent::RunOutput {
                color: self.color,
                node_id: self.node_id.clone(),
                lane: self.node_lane.clone(),
                call_index,
                name: name.to_string(),
                value: value.clone(),
                at_unix: now_unix(),
            },
            &self.pod_name,
        )
        .await;
        Ok(())
    }

    async fn endpoint_url(&self, name: &str) -> WeftResult<String> {
        let endpoint = self
            .clients
            .infra
            .endpoint_url(&self.project_id, &self.node_id, name)
            .await
            .map_err(|e| WeftError::Config(format!("infra_node lookup: {e}")))?;
        endpoint.ok_or_else(|| {
            WeftError::Config(format!(
                "endpoint '{}' for node '{}' is not available; either the infra isn't running \
                 or the endpoint name is not declared. Check `weft infra status` and the node's \
                 InfraSpec.endpoints list.",
                name, self.node_id
            ))
        })
    }

    async fn endpoint_call(
        &self,
        base: &str,
        method: weft_core::EndpointMethod,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> WeftResult<serde_json::Value> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let client = reqwest::Client::new();
        let req = match method {
            weft_core::EndpointMethod::Get => client.get(&url),
            weft_core::EndpointMethod::Post => {
                let mut r = client.post(&url);
                if let Some(b) = &body {
                    r = r.json(b);
                }
                r
            }
        };
        let resp = req.send().await.map_err(|e| {
            WeftError::Runtime(anyhow::anyhow!("endpoint_call {url}: {e}"))
        })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url} returned {status}: {body}"
            )));
        }
        resp.json::<serde_json::Value>().await.map_err(|e| {
            WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url} response not JSON: {e}"
            ))
        })
    }

    /// Entry-trigger registration. Synchronous: enqueues the
    /// register_signal task and waits for the dispatcher to ack.
    /// Worker keeps executing; the signal stays armed and
    /// persistent until the project is deactivated. Each external
    /// fire spawns a fresh execution (the dispatcher's relay path
    /// enqueues route_entry instead of resuming this lane).
    /// Distinct from await_signal: no suspend-then-resume cycle.
    async fn register_signal(&self, spec: SignalSpec) -> WeftResult<()> {
        // Entry triggers are one-shot per node per TriggerSetup.
        // The dedup key for this enqueue is `(color, node, lane,
        // is_resume=false, call_index=0)`; a second call from the
        // same node body would collide and silently drop the
        // second trigger. Catch that loudly here so the offending
        // node fails instead of a trigger going missing.
        let prev = self.entry_register_count.fetch_add(1, Ordering::SeqCst);
        if prev > 0 {
            return Err(WeftError::Config(format!(
                "node '{}' called ctx.register_signal more than once; \
                 entry triggers are one-per-node-per-TriggerSetup",
                self.node_id
            )));
        }
        let reply = enqueue_register_signal_task(
            self.clients.tasks.as_ref(),
            self.color,
            &self.node_id,
            &self.node_lane,
            &spec,
            false,
            &self.tenant_id,
            0,
        )
        .await
        .map_err(|e| WeftError::Suspension(format!("register_signal: {e}")))?;
        tracing::info!(
            target: "weft_engine::register",
            node = %self.node_id,
            color = %self.color,
            token = %reply.token,
            "register_signal: dispatcher ack"
        );
        Ok(())
    }

    async fn report_cost(&self, report: CostReport) -> WeftResult<()> {
        let amount = report.amount_usd;
        if !(amount.is_finite() && amount >= 0.0) {
            return Err(WeftError::Config(format!(
                "report_cost amount_usd must be finite and non-negative; got {amount}"
            )));
        }
        self.enqueue_side_effect_task(
            "report_cost",
            weft_task_store::TaskKind::RecordCost,
            weft_task_store::RecordCostPayload {
                color: self.color.to_string(),
                service: report.service,
                model: report.model,
                amount_usd: amount,
                metadata: report.metadata,
            },
        )
        .await
    }

    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()> {
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
        self.enqueue_side_effect_task(
            "log",
            weft_task_store::TaskKind::RecordLog,
            weft_task_store::RecordLogPayload {
                color: self.color.to_string(),
                level: level_str.to_string(),
                message,
            },
        )
        .await
    }

    fn cancellation(&self) -> Arc<CancellationFlag> {
        self.cancellation.clone()
    }
}

/// Reply shape from a `register_signal` task. Mirrors
/// `weft-dispatcher::task_kinds::register_signal::RegisterSignalResult`
/// but lives here because the engine can't depend on the
/// dispatcher.
#[derive(Debug, serde::Deserialize)]
struct RegisterSignalReply {
    token: String,
}

async fn enqueue_register_signal_task(
    tasks: &dyn TaskStoreClient,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    spec: &SignalSpec,
    is_resume: bool,
    tenant_id: &str,
    call_index: u32,
) -> anyhow::Result<RegisterSignalReply> {
    // Task-level dedup so retries (network blip, supervisor
    // reconnect) converge on the same token. `is_resume` is in the
    // key because the same (color, node, lane, call_index) tuple can
    // be reused across a resume + an entry-trigger registration on
    // the same node body (e.g. a node that awaits its own webhook).
    // Separate from the journal-level dedup the dispatcher's
    // executor uses for SuspensionRegistered: that one omits
    // `is_resume` because only resume registrations journal a
    // SuspensionRegistered event.
    let lane_key = lane_dedup_key(lane)?;
    let dedup_key = format!(
        "{}/{}/{}/{}/{}",
        color, node_id, lane_key, is_resume, call_index,
    );
    let payload = serde_json::json!({
        "color": color.to_string(),
        "node_id": node_id,
        "lane": lane,
        "spec": spec,
        "is_resume": is_resume,
        "call_index": call_index,
    });
    let id = tasks
        .enqueue_dedup(task_store::NewTask {
            kind: TaskKind::RegisterSignal,
            target: task_store::TaskTarget::Dispatcher,
            project_id: None,
            dedup_key: Some(dedup_key),
            color: Some(color.to_string()),
            tenant_id: Some(tenant_id.to_string()),
            target_pod_name: None,
            payload,
        })
        .await?
        .id();
    let outcome = tasks
        .wait_for_terminal(id, TASK_WAIT_TIMEOUT, TASK_POLL_INTERVAL)
        .await?;
    match outcome.status {
        task_store::TaskStatus::Complete => {
            let result = outcome
                .result
                .ok_or_else(|| anyhow::anyhow!("register_signal returned no result"))?;
            Ok(serde_json::from_value(result)?)
        }
        task_store::TaskStatus::Failed => {
            anyhow::bail!("{}", outcome.error.unwrap_or_else(|| "register_signal failed".into()))
        }
        other => anyhow::bail!("register_signal status: {other:?}"),
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use weft_core::context::ContextHandle;
    use weft_core::primitive::{AwaitedEntry, AwaitedEntryKind};

    fn handle_with_sequence(seq: Vec<AwaitedEntry>) -> RunnerHandle {
        // These tests only exercise run_step which never touches the
        // store. The no-op clients below let the trait objects exist
        // without any IO.
        let clients = EngineClients {
            journal: Arc::new(NoopJournal),
            tasks: Arc::new(NoopTaskStore),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
        };
        RunnerHandle::new(
            "exec-1".into(),
            "00000000-0000-0000-0000-000000000000".into(),
            uuid::Uuid::nil(),
            "node-x".into(),
            weft_core::lane::Lane::default(),
            clients,
            "pod-1".into(),
            "tenant-1".into(),
            std::sync::Arc::new(CancellationFlag::new()),
        )
        .with_awaited_sequence(seq)
    }

    struct NoopJournal;
    #[async_trait]
    impl JournalClient for NoopJournal {
        async fn record_event(
            &self,
            _event: &ExecEvent,
            _pod_name: Option<&str>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn events_for_color(
            &self,
            _color: Color,
        ) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(Vec::new())
        }
        async fn has_terminal_event(&self, _color: Color) -> anyhow::Result<bool> {
            Ok(false)
        }
    }
    struct NoopTaskStore;
    #[async_trait]
    impl TaskStoreClient for NoopTaskStore {
        async fn enqueue_dedup(
            &self,
            _spec: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!("replay tests do not enqueue")
        }
        async fn wait_for_terminal(
            &self,
            _task_id: uuid::Uuid,
            _timeout: std::time::Duration,
            _poll_interval: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!("replay tests do not wait")
        }
        async fn claim_one(
            &self,
            _pod_id: &str,
            _filter: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _result: Value,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _error: String,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }
    struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_url(
            &self,
            _project_id: &str,
            _node_id: &str,
            _endpoint_name: &str,
        ) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
    }

    struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(
            &self,
            _project_id: &str,
            _node_id: &str,
            _spec_json: serde_json::Value,
        ) -> anyhow::Result<i64> {
            Ok(0)
        }
        async fn wait_apply(
            &self,
            _project_id: &str,
            _command_id: i64,
        ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
    }

    /// Replay path: a Run entry at the next call_index returns
    /// its journaled value without invoking the closure.
    #[tokio::test]
    async fn run_step_replay_returns_journaled_value() {
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Run {
                name: "decide".into(),
                value: serde_json::json!("go-left"),
            },
        }];
        let handle = handle_with_sequence(seq);
        let (idx, got) = handle
            .run_step("decide")
            .await
            .expect("run_step ok");
        assert_eq!(idx, 0);
        assert_eq!(got, Some(serde_json::json!("go-left")));
    }

    /// Fresh path: sequence is empty, run_step returns None so
    /// the wrapper invokes the closure.
    #[tokio::test]
    async fn run_step_fresh_returns_none() {
        let handle = handle_with_sequence(Vec::new());
        let (idx, got) = handle.run_step("decide").await.expect("run_step ok");
        assert_eq!(idx, 0);
        assert!(got.is_none(), "no journaled output yet");
    }

    /// Mismatched kind at the same call_index: body called
    /// `ctx.run` but the journal has an `Await` at this index.
    /// Surfaces as Suspension error so the body fails loudly
    /// instead of silently desyncing.
    #[tokio::test]
    async fn run_step_mismatch_kind_errors() {
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Await {
                token: "tok-0".into(),
                resolved: Some(serde_json::json!("from-fire")),
            },
        }];
        let handle = handle_with_sequence(seq);
        let err = handle
            .run_step("decide")
            .await
            .expect_err("should fail on kind mismatch");
        let msg = format!("{err}");
        assert!(
            matches!(err, WeftError::NodeExecution(_)),
            "expected NodeExecution variant, got: {err:?}"
        );
        assert!(
            msg.contains("non-deterministic bodies are not safe to replay"),
            "unexpected: {msg}"
        );
    }

    /// Multi-call replay: two Run entries in sequence pop in order.
    #[tokio::test]
    async fn run_step_replay_sequential() {
        let seq = vec![
            AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Run {
                    name: "first".into(),
                    value: serde_json::json!("v0"),
                },
            },
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run {
                    name: "second".into(),
                    value: serde_json::json!("v1"),
                },
            },
        ];
        let handle = handle_with_sequence(seq);
        assert_eq!(
            handle.run_step("first").await.expect("first"),
            (0, Some(serde_json::json!("v0")))
        );
        assert_eq!(
            handle.run_step("second").await.expect("second"),
            (1, Some(serde_json::json!("v1")))
        );
        // Third call: sequence exhausted, returns None.
        let (idx, val) = handle.run_step("third").await.expect("third");
        assert_eq!(idx, 2);
        assert!(val.is_none());
    }

    /// Mixed sequence: Await then Run. Verifies the counter and
    /// pop work across kinds.
    #[tokio::test]
    async fn await_then_run_replay_in_order() {
        let seq = vec![
            AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Await {
                    token: "tok-0".into(),
                    resolved: Some(serde_json::json!("answer")),
                },
            },
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run {
                    name: "process".into(),
                    value: serde_json::json!({"shape": "pre-baked"}),
                },
            },
        ];
        let handle = handle_with_sequence(seq);
        use weft_core::signal::{to_spec, Form, FormSchema};
        let spec = to_spec(Form {
            form_type: "human-query".into(),
            schema: FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
            title: None,
            description: None,
            consumer_kind: None,
        });
        let answer = handle
            .await_signal(spec)
            .await
            .expect("await replay ok");
        assert_eq!(answer, serde_json::json!("answer"));
        let processed = handle
            .run_step("process")
            .await
            .expect("run replay ok");
        assert_eq!(
            processed,
            (1, Some(serde_json::json!({"shape": "pre-baked"})))
        );
    }
}

/// After `node.provision()` returns, the loop driver calls this to
/// ship the spec to the supervisor and wait for it to settle.
///
/// The worker doesn't compile, doesn't hash, doesn't decide
/// skip/fresh/replace. The supervisor owns all of those: it reads
/// the prior `infra_node` row, compiles the new spec with the real
/// image-tag map + instance id (fresh-mint or reused), hashes,
/// makes the decision, and executes. The worker just polls the
/// command row for terminal state.
///
/// This is a single round-trip from the engine's perspective:
/// "supervisor, please apply this spec; tell me when you're done."
/// Skip detection happens supervisor-side and is invisible to the
/// caller (success is success either way).
pub async fn apply_via_supervisor(
    infra_state: &dyn InfraStateClient,
    clock: &dyn weft_platform_traits::Clock,
    project_id: &str,
    node_id: &str,
    spec: &weft_core::infra::InfraSpec,
) -> anyhow::Result<()> {
    let spec_json = serde_json::to_value(spec)?;
    let cmd_id = infra_state
        .enqueue_apply(project_id, node_id, spec_json)
        .await?;
    let deadline = clock.now() + TASK_WAIT_TIMEOUT;
    loop {
        let resp = infra_state.wait_apply(project_id, cmd_id).await?;
        if resp.completed {
            use weft_broker_client::protocol::LifecycleOutcome;
            match resp.outcome {
                Some(LifecycleOutcome::Succeeded) => return Ok(()),
                Some(LifecycleOutcome::Cancelled) => {
                    // The command was abandoned (e.g. the node was
                    // removed by `remove_node` mid-flight). Not a
                    // failure: surface as "no longer applicable"
                    // and let the engine treat it as completed.
                    let reason = resp.outcome_message.as_deref().unwrap_or("cancelled");
                    tracing::info!(
                        target: "weft_engine::context",
                        project_id,
                        node_id,
                        reason,
                        "supervisor apply cancelled; no longer applicable"
                    );
                    return Ok(());
                }
                Some(LifecycleOutcome::Failed) => {
                    let err = resp
                        .outcome_message
                        .unwrap_or_else(|| "supervisor reported no error detail".into());
                    anyhow::bail!("supervisor apply failed: {err}");
                }
                None => {
                    // completed=true with outcome=None means schema
                    // drift the broker should have caught; fail loud.
                    anyhow::bail!(
                        "supervisor apply completed but returned no outcome"
                    );
                }
            }
        }
        if clock.now() >= deadline {
            anyhow::bail!(
                "supervisor did not complete apply command {cmd_id} within {}s",
                TASK_WAIT_TIMEOUT.as_secs()
            );
        }
        clock.sleep(TASK_POLL_INTERVAL).await;
    }
}

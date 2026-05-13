//! Engine-side `ContextHandle`. Lifecycle events go straight to
//! the journal via the broker. Control-plane round-trips
//! (`await_signal`, `register_signal`, `provision_sidecar`) go
//! through the dispatcher's task queue (also via the broker): the
//! worker enqueues a task row and waits for completion. Resume
//! values are seeded into the per-(node, lane) await sequence by
//! the loop driver at boot from the journal fold; the body's
//! `await_signal` calls pop entries in call_index order.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde_json::Value;

use weft_core::cancellation::CancellationFlag;
use weft_core::context::{ContextHandle, LogLevel};
use weft_core::error::{WeftError, WeftResult};
use weft_core::primitive::{CostReport, SignalSpec};
use weft_core::Color;
use weft_infra::InfraReader;
use weft_journal::{ExecEvent, JournalClient};

use weft_task_store::tasks as task_store;
use weft_task_store::{TaskKind, TaskStoreClient};

/// Bundle of broker-backed clients the engine threads everywhere.
/// Each handle clones cheaply (every field is `Arc<dyn _>`).
#[derive(Clone)]
pub struct EngineClients {
    pub journal: Arc<dyn JournalClient>,
    pub tasks: Arc<dyn TaskStoreClient>,
    pub infra: Arc<dyn InfraReader>,
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

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct RunnerHandle {
    execution_id: String,
    project_id: String,
    color: Color,
    node_id: String,
    node_lane: weft_core::lane::Lane,
    /// Broker-backed clients: journal writes, task enqueue, and
    /// infra reads (sidecar endpoint lookup) all flow through here.
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
        let dedup_key = format!(
            "{dedup_prefix}:{color}:{node}:{lane}:{idx}",
            color = self.color,
            node = self.node_id,
            lane = serde_json::to_string(&self.node_lane).unwrap_or_default(),
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
    async fn run_step(&self, name: &str) -> WeftResult<Option<Value>> {
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
                        Ok(Some(value))
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
            None => Ok(None),
        }
    }

    async fn run_record(&self, name: &str, value: &Value) -> WeftResult<()> {
        // call_index already incremented by run_step. The fresh
        // path means run_step returned None, so the index that
        // applies to THIS run output is one less than the current
        // counter. Subtract atomically (no other call could have
        // observed the counter between run_step and run_record
        // because the body is a single async future polled in
        // sequence; the Mutex on awaited_sequence guards races
        // against any cross-thread polling).
        let call_index = self.next_call_index.load(Ordering::SeqCst).saturating_sub(1);
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

    async fn sidecar_endpoint(&self) -> WeftResult<String> {
        let endpoint = self
            .clients
            .infra
            .sidecar_endpoint(&self.project_id, &self.node_id)
            .await
            .map_err(|e| WeftError::Config(format!("infra_pod lookup: {e}")))?;
        endpoint.ok_or_else(|| {
            WeftError::Config(format!(
                "sidecar for node '{}' is not provisioned or has no endpoint URL; run `weft infra start` first",
                self.node_id
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

    async fn provision_sidecar(
        &self,
        spec: weft_core::node::SidecarSpec,
    ) -> WeftResult<weft_core::context::SidecarHandle> {
        let reply = enqueue_provision_sidecar_task(
            self.clients.tasks.as_ref(),
            &self.project_id,
            &self.node_id,
            &spec,
            &self.tenant_id,
        )
        .await
        .map_err(|e| WeftError::Config(format!("provision_sidecar: {e}")))?;
        let endpoint_url = reply.endpoint_url.ok_or_else(|| {
            WeftError::Config(
                "provision_sidecar: dispatcher returned no endpoint URL".into(),
            )
        })?;
        Ok(weft_core::context::SidecarHandle {
            instance_id: reply.instance_id,
            endpoint_url,
        })
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

#[derive(Debug, serde::Deserialize)]
struct ProvisionSidecarReply {
    instance_id: String,
    endpoint_url: Option<String>,
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
    let lane_key = serde_json::to_string(lane)?;
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
        async fn sidecar_endpoint(
            &self,
            _project_id: &str,
            _node_id: &str,
        ) -> anyhow::Result<Option<String>> {
            Ok(None)
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
        let got = handle
            .run_step("decide")
            .await
            .expect("run_step ok");
        assert_eq!(got, Some(serde_json::json!("go-left")));
    }

    /// Fresh path: sequence is empty, run_step returns None so
    /// the wrapper invokes the closure.
    #[tokio::test]
    async fn run_step_fresh_returns_none() {
        let handle = handle_with_sequence(Vec::new());
        let got = handle.run_step("decide").await.expect("run_step ok");
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
            Some(serde_json::json!("v0"))
        );
        assert_eq!(
            handle.run_step("second").await.expect("second"),
            Some(serde_json::json!("v1"))
        );
        // Third call: sequence exhausted, returns None.
        assert!(handle.run_step("third").await.expect("third").is_none());
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
            Some(serde_json::json!({"shape": "pre-baked"}))
        );
    }
}

async fn enqueue_provision_sidecar_task(
    tasks: &dyn TaskStoreClient,
    project_id: &str,
    node_id: &str,
    spec: &weft_core::node::SidecarSpec,
    tenant_id: &str,
) -> anyhow::Result<ProvisionSidecarReply> {
    let dedup_key = format!("{project_id}/{node_id}");
    let payload = serde_json::json!({
        "project_id": project_id,
        "node_id": node_id,
        "spec": spec,
    });
    let id = tasks
        .enqueue_dedup(task_store::NewTask {
            kind: TaskKind::ProvisionSidecar,
            target: task_store::TaskTarget::Dispatcher,
            project_id: Some(project_id.to_string()),
            dedup_key: Some(dedup_key),
            color: None,
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
                .ok_or_else(|| anyhow::anyhow!("provision_sidecar returned no result"))?;
            Ok(serde_json::from_value(result)?)
        }
        task_store::TaskStatus::Failed => {
            anyhow::bail!(
                "{}",
                outcome.error.unwrap_or_else(|| "provision_sidecar failed".into())
            )
        }
        other => anyhow::bail!("provision_sidecar status: {other:?}"),
    }
}

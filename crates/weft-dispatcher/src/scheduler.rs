//! Timer scheduler. Fires `WakeSignalKind::Timer` entry signals at
//! their scheduled times.
//!
//! Runs as a background tokio task per registered timer. Each task
//! computes its next fire instant from its `TimerSpec`, sleeps
//! until then, fires the trigger (same path webhook.rs uses), and
//! either loops (Cron) or exits (After / At).
//!
//! Registered on `/projects/{id}/activate`, cancelled on
//! `/projects/{id}/deactivate`. State lives in the dispatcher's
//! `DispatcherState.scheduler` field as a DashMap<project_id,
//! Vec<JoinHandle>>.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Duration, Instant};

use weft_core::primitive::TimerSpec;

use crate::state::DispatcherState;

/// One timer task to spawn. Collected by `activate` then handed to
/// `Scheduler::replace_project` in a single atomic swap so a second
/// activate call mid-flight cannot interleave and leave duplicate
/// tasks running.
pub struct TimerRegistration {
    pub node_id: String,
    pub entry_token: String,
    pub spec: TimerSpec,
    pub binary_path: PathBuf,
}

/// Handle set keyed by project id. Each project can have multiple
/// timer signals across its nodes; all tasks for a project are
/// spawned together and cancel together.
#[derive(Clone, Default)]
pub struct Scheduler {
    inner: Arc<DashMap<String, Vec<JoinHandle<()>>>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically replace the project's running timer tasks with
    /// tasks for the given registrations. Abort the previous set
    /// (if any) and spawn the new one while holding the per-key
    /// entry lock, so concurrent activate calls cannot leave
    /// orphaned tasks behind.
    pub fn replace_project(
        &self,
        project_id: String,
        registrations: Vec<TimerRegistration>,
        state: DispatcherState,
    ) {
        let mut entry = self.inner.entry(project_id.clone()).or_default();
        for handle in entry.drain(..) {
            handle.abort();
        }
        for reg in registrations {
            let state = state.clone();
            let project_id = project_id.clone();
            let node_id = reg.node_id;
            let entry_token = reg.entry_token;
            let spec = reg.spec;
            let binary_path = reg.binary_path;
            let handle = tokio::spawn(async move {
                run_timer(spec, state, project_id, node_id, entry_token, binary_path).await;
            });
            entry.push(handle);
        }
    }

    /// Cancel every task for the project. Called on deactivate and
    /// on project delete.
    pub fn cancel_project(&self, project_id: &str) {
        if let Some((_, handles)) = self.inner.remove(project_id) {
            for h in handles {
                h.abort();
            }
        }
    }
}

/// Per-timer driver. Sleeps until the next fire, calls the fire
/// helper, loops if `Cron`, else exits.
async fn run_timer(
    spec: TimerSpec,
    state: DispatcherState,
    project_id: String,
    node_id: String,
    entry_token: String,
    binary_path: PathBuf,
) {
    loop {
        let Some(next) = next_fire(&spec) else {
            tracing::warn!(
                target: "weft_dispatcher::scheduler",
                node = %node_id,
                "timer spec has no next fire; task exiting"
            );
            return;
        };
        sleep_until(next).await;

        let now_iso = Utc::now().to_rfc3339();
        let payload = serde_json::json!({
            "scheduledTime": now_iso,
            "actualTime": now_iso,
        });
        fire_trigger(
            &state,
            &project_id,
            &node_id,
            &entry_token,
            payload,
            &binary_path,
        )
        .await;

        // One-shot specs exit after firing; recurring specs loop.
        match &spec {
            TimerSpec::After { .. } | TimerSpec::At { .. } => return,
            TimerSpec::Cron { .. } => {}
        }
    }
}

/// Compute the instant of the next fire for a `TimerSpec`. Returns
/// `None` when the spec can never fire again (past-date `At`, or
/// an invalid cron expression).
fn next_fire(spec: &TimerSpec) -> Option<Instant> {
    match spec {
        TimerSpec::After { duration_ms } => {
            Some(Instant::now() + Duration::from_millis(*duration_ms))
        }
        TimerSpec::At { when } => {
            let now = Utc::now();
            let delta = *when - now;
            let ms = delta.num_milliseconds();
            if ms <= 0 {
                None
            } else {
                Some(Instant::now() + Duration::from_millis(ms as u64))
            }
        }
        TimerSpec::Cron { expression } => {
            let schedule = cron::Schedule::from_str(expression).ok()?;
            let now: DateTime<Utc> = Utc::now();
            let next_dt = schedule.upcoming(Utc).next()?;
            let delta = next_dt - now;
            let ms = delta.num_milliseconds().max(0) as u64;
            Some(Instant::now() + Duration::from_millis(ms))
        }
    }
}

/// Fire the trigger: same shape as webhook.rs's POST handler.
/// Duplicated here so the scheduler doesn't depend on axum request
/// plumbing. Keeps the code DRY would need a small refactor to
/// extract; leave that for when a third firing surface appears.
async fn fire_trigger(
    state: &DispatcherState,
    project_id: &str,
    node_id: &str,
    _entry_token: &str,
    payload: serde_json::Value,
    binary_path: &std::path::Path,
) {
    let Ok(project_uuid) = project_id.parse::<uuid::Uuid>() else { return };
    let Some(project) = state.projects.project(project_uuid).await else {
        return;
    };

    let seeds =
        crate::api::project::compute_trigger_seeds(&project, node_id, &payload);
    if seeds.is_empty() {
        tracing::warn!(
            target: "weft_dispatcher::scheduler",
            %node_id,
            "timer fired but trigger has no output downstream; skipping"
        );
        return;
    }

    let color = uuid::Uuid::new_v4();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.to_string(),
            entry_node: node_id.to_string(),
            at_unix: now,
        })
        .await;
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::PulseSeeded {
                color,
                node_id: seed.node_id.clone(),
                port: "__seed__".to_string(),
                lane: Vec::new(),
                value: seed.value.clone(),
                at_unix: now,
            })
            .await;
    }

    state
        .slots
        .with_slot(color, {
            let seeds = seeds.clone();
            move |slot| {
                Box::pin(async move {
                    let queued = match slot {
                        crate::slots::Slot::Idle { queued, .. }
                        | crate::slots::Slot::Starting { queued, .. }
                        | crate::slots::Slot::WaitingReconnect { queued, .. } => queued,
                        crate::slots::Slot::Live { .. } => {
                            *slot = crate::slots::Slot::Idle {
                                queued: std::collections::VecDeque::new(),
                            };
                            let crate::slots::Slot::Idle { queued, .. } = slot else {
                                unreachable!()
                            };
                            queued
                        }
                    };
                    queued.push_back(crate::slots::QueuedWake::Start(
                        weft_core::primitive::WakeMessage::Fresh { seeds },
                    ));
                })
            }
        })
        .await;

    let wake = crate::backend::WakeContext {
        project_id: project_id.to_string(),
        color,
    };
    match state.workers.spawn_worker(binary_path, wake).await {
        Ok(worker) => {
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::WorkerSpawned { color, at_unix: now })
                .await;
            state
                .slots
                .with_slot(color, move |slot| {
                    Box::pin(async move {
                        if let crate::slots::Slot::Starting { worker: w, .. } = slot {
                            *w = Some(worker);
                        }
                    })
                })
                .await;
            state
                .events
                .publish(crate::events::DispatcherEvent::ExecutionStarted {
                    color,
                    entry_node: node_id.to_string(),
                    project_id: project_id.to_string(),
                })
                .await;
        }
        Err(e) => {
            tracing::error!(
                target: "weft_dispatcher::scheduler",
                %node_id, error = %e,
                "timer fire: spawn_worker failed"
            );
        }
    }
}

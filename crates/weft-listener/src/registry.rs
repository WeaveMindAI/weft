//! In-memory map of the signals whose connection this listener holds.
//!
//! Only a kind that holds a connection (`BetweenFires::Holds`) has an
//! entry: it binds a token to its resolved spec plus the task running its
//! loop, and unregistering tears the task down. Every other kind is read
//! from its durable row per call (see [`held`]).

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use serde_json::Value;
use weft_core::primitive::{SignalRouting, SignalSpec};
use weft_core::signal::listener_protocol::StartMode;

/// Which transport currently serves a signal's held connection,
/// recorded on its [`ServingState`] when the serving task decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    Socket,
    Webhook,
    Unservable(String),
}

/// The LIVE serving state of one signal in this process: what its background
/// task is doing right now and the transport it decided on. Written
/// by the kind's serving task, read by the node's display. An empty
/// status with no transport means the kind reports nothing live.
#[derive(Debug, Default)]
pub struct ServingState {
    pub status: String,
    pub transport: Option<Transport>,
}

#[derive(Clone)]
pub struct RegisteredSignal {
    pub spec: SignalSpec,
    pub node_id: String,
    /// Tenant this signal belongs to. The listener holds signals from
    /// many tenants; the fire path reads this to stamp the correct
    /// tenant on the enqueued `FireSignal` task.
    pub tenant_id: String,
    /// True iff this is a mid-execution resume (HumanQuery, etc).
    /// Used by `process()` to decide which `ProcessTarget` to return
    /// for dual-use kinds like Form.
    pub is_resume: bool,
    /// Execution of the suspended execution to resume. Set iff
    /// `is_resume`. Echoed back into `ProcessTarget::Resume`.
    pub execution_id: Option<String>,
    /// Background task for kinds that hold a connection
    /// (`BetweenFires::Holds`). Dropping the handle via `.abort()`
    /// cancels the loop. `None` for every other kind.
    pub task: Option<Arc<TaskGuard>>,
    /// The signal's durable kind state as read for this call, for a kind
    /// read from its row per call (a poll's cursor and failure streak,
    /// which its display reads). `None` for a kind that holds a
    /// connection: its task owns its state while it runs, so a copy taken
    /// at registration would only go stale here.
    pub kind_state: Option<Value>,
    /// Routing+auth metadata computed by the kind impl at register
    /// time (or reconstructed from the durable row at rehydrate).
    /// The dispatcher copies this onto the signal row; the kind's
    /// `live` reads it to show what mount_path / auth_kind the signal
    /// is using. Always set: both register and rehydrate paths
    /// populate it, so downstream readers don't need to handle a
    /// None case.
    pub routing: SignalRouting,
    /// The live serving state (see [`ServingState`]), created at
    /// registration and shared with the kind's background task so
    /// status updates land where the kind's `live` reads them. Dies
    /// with the entry.
    pub serving: Arc<Mutex<ServingState>>,
}

/// Wrapper so dropping a `RegisteredSignal` aborts its loop
/// exactly once, even when cloned.
pub struct TaskGuard(JoinHandle<()>);

impl TaskGuard {
    pub fn new(handle: JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
pub struct Registry {
    inner: DashMap<String, RegisteredSignal>,
    /// One guard per token being brought up, so two callers bringing the
    /// same row up (a first use racing a rehydrate, two first uses) run
    /// one after the other and the second finds it up. Machine-local like
    /// the entries: a held connection only runs on the machine's single
    /// listener. An entry lives only while someone holds its guard.
    bringing: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Held rows that could not come up, by token. Each has exactly one
    /// retry loop running ([`hold`]), the entry's `owner`; the node's
    /// display says the signal is down and why.
    down: DashMap<String, Down>,
    /// Source of [`Down::owner`] values.
    down_loops: std::sync::atomic::AtomicU64,
    /// Retry loops alive now, so a test can see one loop per token.
    retry_loops_running: Arc<std::sync::atomic::AtomicUsize>,
}

/// Counts one retry loop as running for as long as it lives.
struct RunningLoop(Arc<std::sync::atomic::AtomicUsize>);

impl RunningLoop {
    fn start(count: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for RunningLoop {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A down row: the latest reason, and which retry loop owns it. A loop
/// runs only while the entry is its own, so an entry cleared and marked
/// again while a loop sleeps gets a new loop and the old one ends: one
/// loop per token, never two.
struct Down {
    reason: String,
    owner: u64,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, token: String, signal: RegisteredSignal) {
        self.inner.insert(token, signal);
    }

    pub fn get(&self, token: &str) -> Option<RegisteredSignal> {
        self.inner.get(token).map(|r| r.clone())
    }

    pub fn remove(&self, token: &str) -> Option<RegisteredSignal> {
        self.inner.remove(token).map(|(_, v)| v)
    }

    pub fn list(&self) -> Vec<(String, RegisteredSignal)> {
        self.inner
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Why a held row is down, while it is.
    pub fn down_reason(&self, token: &str) -> Option<String> {
        self.down.get(token).map(|r| r.reason.clone())
    }

    /// Stop counting `token` as down: it came up, or its row is gone.
    /// Its retry loop sees this at its next turn and ends.
    pub fn clear_down(&self, token: &str) {
        self.down.remove(token);
    }

    /// Whether the retry loop `owner` still owns `token`'s down entry.
    fn owns_down(&self, token: &str, owner: u64) -> bool {
        self.down.get(token).is_some_and(|d| d.owner == owner)
    }

    /// How many retry loops are running now, across every token.
    pub fn retry_loops_running(&self) -> usize {
        self.retry_loops_running.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn bring_up_guard(&self, token: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.bringing.entry(token.to_string()).or_default().clone()
    }

    /// Drop the token's guard once nobody else holds a copy. Counted
    /// under the map's lock, so a caller that just took a copy keeps it.
    pub(crate) fn release_bring_up_guard(&self, token: &str) {
        self.bringing.remove_if(token, |_, guard| Arc::strong_count(guard) == 1);
    }
}

/// A signal as it stands now: the registry's entry for a kind that holds
/// a connection, otherwise read fresh from its durable row. `None` when
/// no held row has that token (the signal is gone, or its activation
/// parked).
///
/// Every endpoint that names a signal goes through here, so any copy of
/// the listener answers for any signal: one that restarted, or one of
/// several copies of a listener that scales. The row is the truth the
/// dispatcher routes by, so the routing comes back from its columns,
/// never recomputed from the kind.
///
/// Only a `Holds` kind is kept (its task lives here, on the machine's
/// single listener). Any other kind is read per call and never cached:
/// an unregister reaches one serverless copy, and a sibling that had
/// cached the signal would keep answering `/process` for it.
pub async fn held(state: &crate::ListenerState, token: &str) -> anyhow::Result<Option<RegisteredSignal>> {
    if let Some(sig) = state.registry.get(token) {
        return Ok(Some(sig));
    }
    let Some(row) = state.signals.get_held(token).await? else {
        return Ok(None);
    };
    let spec: SignalSpec = serde_json::from_str(&row.spec_json)
        .map_err(|e| anyhow::anyhow!("malformed spec_json for signal {}: {e}", row.token))?;
    let handler = crate::kinds::lookup(&spec.kind)
        .ok_or_else(|| anyhow::anyhow!("signal {} has an unknown kind '{}'", row.token, spec.kind))?;
    let holds = handler.between_fires() == crate::kinds::BetweenFires::Holds;
    let routing = row.to_routing().map_err(|e| anyhow::anyhow!("to_routing for signal {}: {e}", row.token))?;
    let from_row = RegisteredSignal {
        spec,
        node_id: row.node_id.clone(),
        tenant_id: row.tenant_id.clone(),
        is_resume: row.is_resume,
        execution_id: row.execution_id.clone(),
        task: None,
        // A held connection's task owns its state once it runs.
        kind_state: (!holds).then(|| row.kind_state.clone()),
        routing,
        serving: Arc::default(),
    };
    if !holds {
        return Ok(Some(from_row));
    }
    // A held connection that is down still answers from its row (a fire
    // it raised before going down still processes, its display says it
    // is down): its retry loop owns bringing it back, so a call here does
    // not start a second attempt.
    if state.registry.down_reason(token).is_some() {
        return Ok(Some(from_row));
    }
    match hold(state, row, StartMode::Restore).await {
        // Up, or its row went while it came up (then nothing holds it).
        Ok(()) => Ok(state.registry.get(token)),
        // Down now, logged, retried, and shown as down.
        Err(_) => Ok(Some(from_row)),
    }
}

/// How long the retry of a down row waits before its first attempt,
/// doubled after each failure up to [`DOWN_RETRY_MAX`].
const DOWN_RETRY_FIRST: std::time::Duration = std::time::Duration::from_secs(1);
const DOWN_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(300);
/// How often a row just brought up is read again before the answer
/// counts as unknown, and the wait before the first re-read (doubled
/// after each).
const REREAD_ATTEMPTS: u32 = 5;
const REREAD_FIRST: std::time::Duration = std::time::Duration::from_millis(200);

/// Bring one held row up: the one path every bring-up of a held row
/// takes (the dispatcher's `/start`, a rehydrate at boot or activation,
/// a first use, the retry of a down row).
///
/// Single-flight per token: a second caller waits for the first and, on
/// a restore, finds the connection up and leaves it (a restore that
/// replaced it would stop the first caller's task mid-start).
///
/// A row is a snapshot: it can be deleted after it was read, and the
/// dispatcher's unregister can pass before the task below is in the
/// registry, which would leave that task running forever for a row
/// nobody holds. So a held connection's row is read again once its task
/// is registered, and a row gone by then is let go. A re-read that keeps
/// failing leaves the task running: the row was held when it was read,
/// and an unregister that comes later still finds the registry entry.
///
/// A restore or put-back that fails marks the row down: it is logged,
/// shown on the node's display, and retried with backoff until it comes
/// up or its row is gone. A `StartMode::New` failure is the dispatcher's
/// to handle (it answers the registration with it), so it is only
/// returned.
pub async fn hold(
    state: &crate::ListenerState,
    row: weft_broker_client::protocol::SignalRowWire,
    mode: StartMode,
) -> anyhow::Result<()> {
    let token = row.token.clone();
    let guard = state.registry.bring_up_guard(&token);
    let result = {
        let _held = guard.lock().await;
        hold_once(state, row, mode).await
    };
    drop(guard);
    state.registry.release_bring_up_guard(&token);
    if let Err(e) = &result {
        if mode.retried() {
            mark_down(state, &token, format!("{e:#}"));
        }
    }
    result
}

async fn hold_once(
    state: &crate::ListenerState,
    row: weft_broker_client::protocol::SignalRowWire,
    mode: StartMode,
) -> anyhow::Result<()> {
    let token = row.token.clone();
    if mode == StartMode::Restore && state.registry.get(&token).is_some() {
        return Ok(());
    }
    if let Err(e) = crate::kinds::bring_up(state, row, mode).await {
        // A put-back that cannot come up must not leave the replacement
        // it was undoing running: the row no longer says that spec. The
        // entry stops (its task and its outside teardown), so the retry
        // loop's restore finds nothing running and brings the put-back
        // row up.
        if mode == StartMode::PutBack {
            crate::kinds::stop_held(state, &token).await;
        }
        return Err(e);
    }
    state.registry.clear_down(&token);
    if state.registry.get(&token).is_none() {
        // Not a held connection: nothing of it stays here to let go.
        return Ok(());
    }
    let mut wait = REREAD_FIRST;
    for attempt in 1..=REREAD_ATTEMPTS {
        match state.signals.get_held(&token).await {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {
                crate::kinds::forget(state, &token);
                tracing::info!(target: "weft_listener", token = %token, "a signal's row went while it came up; its task is stopped");
                return Ok(());
            }
            Err(e) if attempt == REREAD_ATTEMPTS => {
                tracing::warn!(
                    target: "weft_listener", token = %token, error = %format!("{e:#}"),
                    "a held signal came up, but its row could not be read again; it keeps running"
                );
            }
            Err(_) => {
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
        }
    }
    Ok(())
}

/// Count `token` as down with `reason`, starting its retry loop unless
/// one already owns the entry (then only the reason is updated, and that
/// loop stays the owner).
fn mark_down(state: &crate::ListenerState, token: &str, reason: String) {
    tracing::error!(target: "weft_listener", token = %token, reason = %reason, "a held signal is down; retrying it");
    match state.registry.down.entry(token.to_string()) {
        dashmap::Entry::Occupied(mut e) => {
            e.get_mut().reason = reason;
        }
        dashmap::Entry::Vacant(e) => {
            let owner = state.registry.down_loops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            e.insert(Down { reason, owner });
            let running = RunningLoop::start(&state.registry.retry_loops_running);
            tokio::spawn(retry_down(state.clone(), token.to_string(), owner, running));
        }
    }
}

/// Bring a down row back, waiting longer after each failure, until it is
/// up, its row is gone, or the down entry is no longer this loop's
/// (`owner`): something else brought it up, or forgot it, or it was
/// cleared and marked again, which started the loop that owns it now.
async fn retry_down(state: crate::ListenerState, token: String, owner: u64, _running: RunningLoop) {
    let mut wait = weft_core::time_scale::scaled(DOWN_RETRY_FIRST);
    loop {
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(DOWN_RETRY_MAX);
        if !state.registry.owns_down(&token, owner) {
            return;
        }
        match state.signals.get_held(&token).await {
            Ok(None) => {
                state.registry.down.remove_if(&token, |_, d| d.owner == owner);
                tracing::info!(target: "weft_listener", token = %token, "a down signal's row is gone; no longer retrying it");
                return;
            }
            Ok(Some(row)) => {
                // A failure is logged and recorded by `hold`.
                if hold(&state, row, StartMode::Restore).await.is_ok() {
                    tracing::info!(target: "weft_listener", token = %token, "a down signal is back up");
                    return;
                }
            }
            Err(e) => {
                tracing::error!(target: "weft_listener", token = %token, error = %format!("{e:#}"), "a down signal's row could not be read; retrying it");
            }
        }
    }
}

/// Reconcile what this process holds with the durable `signal` table.
/// Idempotent. Every held row is brought up ([`hold`]) unless its connection already runs here: a kind that holds a
/// connection gets its loop started, a kind that wakes gets its next wake
/// set (setting the same wake twice is one wake), and a kind the outside
/// calls in to needs nothing.
///
/// Called at boot over every project (`project` is `None`) and by the
/// dispatcher's activate flow (`POST /rehydrate`) over the activating
/// project, after the activation's rows are written.
///
/// Every row that can come up does: one that cannot (a malformed spec, a
/// kind that refuses) does not stop the rest, so one bad row never keeps
/// the others down (at boot, every project's). It is marked down, which
/// logs it, shows it on its node, and retries it until it comes up or its
/// row is gone. The answer also names each row that stayed down: the
/// activate fails on its own project's, the boot carries on. Rows whose
/// token is in `skip` are never brought up: they are on their way out
/// (see `RehydrateRequest`). Failing to read the rows at all is an error.
pub async fn rehydrate(
    state: &crate::ListenerState,
    project: Option<uuid::Uuid>,
    skip: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut failed = Vec::new();
    for row in state.signals.list_held(project).await? {
        if skip.contains(&row.token) || state.registry.get(&row.token).is_some() {
            continue;
        }
        let token = row.token.clone();
        if let Err(e) = hold(state, row, StartMode::Restore).await {
            failed.push(format!("signal {token}: {e:#}"));
        }
    }
    Ok(failed)
}

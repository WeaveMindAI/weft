//! In-memory map of active signals this listener is serving.
//!
//! Each entry binds a token to its resolved spec plus any per-kind
//! runtime state (a task handle for timers, a cancel signal for
//! SSE/socket loops). When a signal is unregistered, the runtime
//! state is torn down.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use weft_core::primitive::{SignalRouting, SignalSpec};

#[derive(Clone)]
pub struct RegisteredSignal {
    pub spec: SignalSpec,
    pub node_id: String,
    /// True iff this is a mid-execution resume (HumanQuery, etc).
    /// Used by `process()` to decide which `ProcessTarget` to return
    /// for dual-use kinds like Form.
    pub is_resume: bool,
    /// Color of the suspended execution to resume. Set iff
    /// `is_resume`. Echoed back into `ProcessTarget::Resume`.
    pub color: Option<String>,
    /// Background task for kinds that run a loop (Timer, SSE).
    /// Dropping the handle via `.abort()` cancels the loop. `None`
    /// for passive kinds (Webhook, Form).
    pub task: Option<Arc<TaskGuard>>,
    /// Routing+auth metadata computed by the kind impl at register
    /// time (or reconstructed from the durable row at rehydrate).
    /// The dispatcher copies this onto the signal row; `/display`
    /// reads it to show what mount_path / auth_kind the signal is
    /// using. Always set: both register and rehydrate paths
    /// populate it, so downstream readers don't need to handle a
    /// None case.
    pub routing: SignalRouting,
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

}

/// Reconcile the in-memory registry against the durable `signal`
/// table. Idempotent: signals already present in the registry are
/// left alone (no insert, no Timer/SSE task restart); signals in DB
/// but absent from the registry get inserted via `rehydrate_one`.
///
/// Called from two places:
///   - listener boot (registry empty, every DB row gets inserted).
///   - dispatcher's activate flow via `POST /rehydrate` (registry
///     has some entries from earlier registers, missing entries
///     come from DB after a deactivate-park unregister-all).
///
/// Reads through the broker's `signal/list_for_tenant` so the
/// listener never opens a Postgres connection. Routing is REBUILT
/// from the persisted columns (surface_kind, mount_path, auth_kind,
/// auth_config), NOT recomputed from the kind impl. Recomputing
/// would mint a fresh API key on every restart and silently
/// invalidate the user's existing one. The plaintext is gone after
/// a Pod restart (secret_cache is per-Pod by design); the user
/// must explicitly regenerate via `/action` if they need plaintext
/// access again.
///
/// Failures here are fatal: a malformed spec_json or unknown kind
/// row means the schema is in an inconsistent state. We bail rather
/// than silently dropping the row, which would leave the listener
/// 404-ing on fires the dispatcher still holds tokens for.
pub async fn rehydrate(
    tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    broker_url: Arc<String>,
    token: weft_broker_client::TokenSource,
    tenant_id: &str,
    registry: Arc<Registry>,
    config: Arc<crate::config::ListenerConfig>,
) -> anyhow::Result<()> {
    let signals = weft_broker_client::BrokerSignalClient::new((*broker_url).clone(), token);
    let rows = signals.list_for_tenant(tenant_id).await?;
    let sink = crate::fire_sink::FireSignalSink::new(tasks, tenant_id.to_string());
    for row in rows {
        // Skip entries the registry already holds. Re-inserting would
        // abort the existing Timer/SSE TaskGuard and spawn a fresh
        // task, restarting the schedule. We want true rebuild only
        // for missing entries.
        if registry.get(&row.token).is_some() {
            continue;
        }
        let spec: SignalSpec = serde_json::from_str(&row.spec_json)
            .map_err(|e| anyhow::anyhow!("malformed spec_json for signal {}: {e}", row.token))?;
        let routing = row.to_routing().map_err(|e| {
            anyhow::anyhow!("to_routing for signal {}: {e}", row.token)
        })?;
        crate::kinds::register_in_registry(
            row.token,
            spec,
            row.node_id,
            row.is_resume,
            row.color,
            crate::kinds::RoutingSource::Restore {
                routing,
                kind_state: row.kind_state,
            },
            registry.clone(),
            sink.clone(),
            config.clone(),
        )
        .await?;
    }
    Ok(())
}

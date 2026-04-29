//! In-memory map of active signals this listener is serving.
//!
//! Each entry binds a token to its resolved spec plus any per-kind
//! runtime state (a task handle for timers, a cancel signal for
//! SSE/socket loops). When a signal is unregistered, the runtime
//! state is torn down.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use weft_core::primitive::WakeSignalSpec;

#[derive(Clone)]
pub struct RegisteredSignal {
    pub spec: WakeSignalSpec,
    pub node_id: String,
    /// Background task for kinds that run a loop (Timer, SSE,
    /// Socket). Dropping this handle via `.abort()` cancels the
    /// loop. `None` for passive kinds (Webhook, Form).
    pub task: Option<Arc<TaskGuard>>,
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

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&self) {
        self.inner.clear();
    }
}

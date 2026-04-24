//! Per-project infra registry. Tracks every sidecar the dispatcher
//! has provisioned for a project so `weft infra stop/terminate`
//! can operate on the right resource and `ctx.sidecar_endpoint()`
//! can resolve a node's URL.
//!
//! In-memory only for now. When the dispatcher moves to Postgres
//! this should reload from durable state on startup so restarts
//! don't orphan sidecars.

use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;

use crate::backend::InfraHandle;

/// Lifecycle state of a provisioned sidecar, from the dispatcher's
/// point of view. Mirrors v1 semantics:
///   - `Running`: k8s Deployment at replicas=1, sidecar reachable.
///   - `Stopped`: Deployment scaled to 0, Service / PVC / Ingress
///     kept so `start` can bring it back with state intact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InfraStatus {
    Running,
    Stopped,
}

#[derive(Debug, Clone)]
pub struct InfraEntry {
    pub handle: InfraHandle,
    pub status: InfraStatus,
}

/// Key: (project_id, node_id). Value: the handle + its current
/// lifecycle status.
#[derive(Default, Clone)]
pub struct InfraRegistry {
    inner: Arc<DashMap<(String, String), InfraEntry>>,
}

impl InfraRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_running(&self, project_id: String, node_id: String, handle: InfraHandle) {
        self.insert_with_status(project_id, node_id, handle, InfraStatus::Running);
    }

    pub fn insert_with_status(
        &self,
        project_id: String,
        node_id: String,
        handle: InfraHandle,
        status: InfraStatus,
    ) {
        self.inner
            .insert((project_id, node_id), InfraEntry { handle, status });
    }

    pub fn set_status(&self, project_id: &str, node_id: &str, status: InfraStatus) {
        if let Some(mut entry) = self
            .inner
            .get_mut(&(project_id.to_string(), node_id.to_string()))
        {
            entry.status = status;
        }
    }

    /// Current entry (handle + status). None if we don't have a
    /// record, which means neither `start` nor a previous dispatcher
    /// session has provisioned this node yet.
    pub fn get(&self, project_id: &str, node_id: &str) -> Option<InfraEntry> {
        self.inner
            .get(&(project_id.to_string(), node_id.to_string()))
            .map(|h| h.clone())
    }

    /// Convenience for the ws.rs sidecar-endpoint lookup path. Only
    /// returns a usable handle when the sidecar is running; a
    /// stopped entry answers None so callers fail loudly instead of
    /// handing the worker a dead DNS name.
    pub fn handle_if_running(&self, project_id: &str, node_id: &str) -> Option<InfraHandle> {
        let entry = self.get(project_id, node_id)?;
        match entry.status {
            InfraStatus::Running => Some(entry.handle),
            InfraStatus::Stopped => None,
        }
    }

    pub fn remove(&self, project_id: &str, node_id: &str) -> Option<InfraEntry> {
        self.inner
            .remove(&(project_id.to_string(), node_id.to_string()))
            .map(|(_, v)| v)
    }

    pub fn list_for_project(&self, project_id: &str) -> Vec<(String, InfraEntry)> {
        self.inner
            .iter()
            .filter(|e| e.key().0 == project_id)
            .map(|e| (e.key().1.clone(), e.value().clone()))
            .collect()
    }

    pub fn remove_project(&self, project_id: &str) -> Vec<(String, InfraEntry)> {
        let keys: Vec<(String, String)> = self
            .inner
            .iter()
            .filter(|e| e.key().0 == project_id)
            .map(|e| e.key().clone())
            .collect();
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some((_, v)) = self.inner.remove(&k) {
                out.push((k.1, v));
            }
        }
        out
    }
}

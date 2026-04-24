use std::sync::Arc;

use crate::backend::{InfraBackend, WorkerBackend};
use crate::config::DispatcherConfig;
use crate::events::EventBus;
use crate::infra::InfraRegistry;
use crate::journal::Journal;
use crate::listener::{ListenerBackend, ListenerRegistry, SignalTracker};
use crate::project_store::ProjectStore;
use crate::slots::Slots;

/// Top-level dispatcher state. Shared across HTTP handlers via
/// `axum::extract::State`. All fields are `Arc`-friendly.
#[derive(Clone)]
pub struct DispatcherState {
    pub config: Arc<DispatcherConfig>,
    pub journal: Arc<dyn Journal>,
    pub workers: Arc<dyn WorkerBackend>,
    pub infra: Arc<dyn InfraBackend>,
    pub projects: ProjectStore,
    pub events: EventBus,
    pub slots: Slots,
    /// Spawns per-project listener instances.
    pub listener_backend: Arc<dyn ListenerBackend>,
    /// Per-project listener handles. One entry per active project.
    pub listeners: ListenerRegistry,
    /// Every signal currently registered with any listener. Used to
    /// look up a fire relay back to its owning project + node.
    pub signal_tracker: SignalTracker,
    /// Provisioned sidecars per (project, node). Populated by
    /// `weft infra up`, cleared by `weft infra down`. Looked up by
    /// `ctx.sidecar_endpoint()` to resolve a node's endpoint URL.
    pub infra_registry: InfraRegistry,
}

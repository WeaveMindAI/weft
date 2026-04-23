use std::sync::Arc;

use crate::backend::{InfraBackend, WorkerBackend};
use crate::config::DispatcherConfig;
use crate::events::EventBus;
use crate::journal::Journal;
use crate::project_store::ProjectStore;
use crate::scheduler::Scheduler;
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
    /// Background tasks that fire `WakeSignalKind::Timer` triggers
    /// at their scheduled times. Registered on activate, cancelled
    /// on deactivate.
    pub scheduler: Scheduler,
}

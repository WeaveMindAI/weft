use std::sync::Arc;

use crate::backend::{InfraBackend, WorkerBackend};
use crate::config::DispatcherConfig;
use crate::journal::Journal;

/// Top-level dispatcher state. Shared across HTTP handlers via
/// `axum::extract::State`. All fields are `Arc`-friendly.
#[derive(Clone)]
pub struct DispatcherState {
    pub config: Arc<DispatcherConfig>,
    pub journal: Arc<dyn Journal>,
    pub workers: Arc<dyn WorkerBackend>,
    pub infra: Arc<dyn InfraBackend>,
}

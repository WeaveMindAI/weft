//! Journal abstraction + sqlite-backed impl for local dev.
//!
//! The journal owns everything durable the dispatcher needs to
//! resume executions across restarts:
//! - Wake tokens (webhook, form, timer) -> color + suspension point.
//! - Suspension state: enough to restart a worker at the same node.
//! - Cost events per color (ledger for the dashboard + billing).
//!
//! Cloud deployments (weavemind) swap in a restate-backed impl via
//! the same trait; local uses `sqlite`.

pub mod sqlite;

use async_trait::async_trait;

use weft_core::Color;

#[async_trait]
pub trait Journal: Send + Sync {
    async fn record_start(&self, color: Color, project_id: &str, entry_node: &str)
        -> anyhow::Result<()>;

    async fn record_suspension(
        &self,
        color: Color,
        node: &str,
        metadata: serde_json::Value,
    ) -> anyhow::Result<()>;

    async fn resolve_wake(&self, token: &str) -> anyhow::Result<Option<WakeTarget>>;

    async fn record_cost(&self, color: Color, report: weft_core::CostReport)
        -> anyhow::Result<()>;

    async fn cancel(&self, color: Color) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct WakeTarget {
    pub color: Color,
    pub node: String,
    pub metadata: serde_json::Value,
}

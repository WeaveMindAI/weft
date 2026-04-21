//! Journal abstraction. Concrete impls back onto restate (embedded
//! locally, managed in cloud). Trait keeps the rest of the dispatcher
//! ignorant of which restate deployment it's talking to.

use async_trait::async_trait;

use weft_core::Color;

#[async_trait]
pub trait Journal: Send + Sync {
    /// Record a new execution starting under the given color.
    async fn record_start(&self, color: Color, project_id: &str, entry_node: &str)
        -> anyhow::Result<()>;

    /// Record a suspension entry for later wake.
    async fn record_suspension(&self, color: Color, node: &str, metadata: serde_json::Value)
        -> anyhow::Result<()>;

    /// Resolve a wake token into (color, suspension marker, metadata)
    /// or `None` if the token doesn't exist.
    async fn resolve_wake(&self, token: &str) -> anyhow::Result<Option<WakeTarget>>;

    /// Record a cost event.
    async fn record_cost(&self, color: Color, report: weft_core::CostReport) -> anyhow::Result<()>;

    /// Cancel a color. Drops suspensions, marks journal entries
    /// cancelled.
    async fn cancel(&self, color: Color) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct WakeTarget {
    pub color: Color,
    pub node: String,
    pub metadata: serde_json::Value,
}

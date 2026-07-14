//! Usage metering for spends on the platform key: before a node makes a
//! paid call, it provisions the call's declared worst-case cost; the
//! gate decides whether the spend proceeds and may track a hold, settled
//! down to the actual cost after the call.
//!
//! [`UnmeteredCostGate`] approves every provision without tracking a
//! hold; there is nothing to settle.
//!
//! User-supplied keys NEVER reach the gate (the engine short-circuits
//! them: the user's own account is not the platform's to meter).

use anyhow::Result;
use serde_json::Value;

/// One provisioned spend: who, from where, and the declared cost.
#[derive(Debug, Clone)]
pub struct SpendRequest {
    pub tenant: String,
    pub project_id: String,
    pub node_id: String,
    pub node_type: String,
    pub provider: String,
    /// The service/model detail from the node's cost report (audit trail).
    pub service: String,
    pub model: Option<String>,
    /// The declared cost of the exact call about to be made: a ceiling
    /// when `exact` is false, the true price when `exact` is true.
    pub amount_usd: f64,
    /// The amount IS the price (fixed-per-call pricing). A gate that
    /// resolves abandoned holds can settle an exact one at the
    /// provisioned amount itself.
    pub exact: bool,
    /// How long the paid action should reasonably take; an unsettled
    /// hold counts as abandoned only well after this window.
    pub expected_duration_secs: u64,
    pub metadata: Value,
}

/// The gate's decision on a provision.
#[derive(Debug)]
pub enum SpendDecision {
    /// Go ahead. `hold_id` is the gate's tracked hold when it meters
    /// (settle it after the call); `None` when nothing is tracked.
    Approved { hold_id: Option<String> },
    /// The budget refuses this spend, with a user-facing reason.
    Denied { reason: String },
}

#[async_trait::async_trait]
pub trait CostGate: Send + Sync {
    async fn provision(&self, req: &SpendRequest) -> Result<SpendDecision>;
    /// Settle a hold down to the actual cost (0 releases it). Only called
    /// with hold ids this gate minted.
    async fn settle(&self, hold_id: &str, actual_usd: f64, metadata: &Value) -> Result<()>;
}

/// Approves every provision and tracks nothing.
pub struct UnmeteredCostGate;

#[async_trait::async_trait]
impl CostGate for UnmeteredCostGate {
    async fn provision(&self, _req: &SpendRequest) -> Result<SpendDecision> {
        Ok(SpendDecision::Approved { hold_id: None })
    }

    async fn settle(&self, hold_id: &str, _actual_usd: f64, _metadata: &Value) -> Result<()> {
        // This gate never mints holds, so a settle reaching it is a bug
        // (a caller invented a hold id); fail loud, never swallow.
        anyhow::bail!("unmetered cost gate cannot settle hold {hold_id}: it mints no holds")
    }
}

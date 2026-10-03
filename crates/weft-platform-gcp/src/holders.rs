//! The holders on a Cloud Run worker pool, as many copies as weft asks
//! for. A worker pool takes no requests and scales only when told, which
//! is what holders want: they call out, and their count follows the held
//! signals, not traffic. At zero copies it costs nothing.

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_platform_traits::HolderPool;

use crate::api::Google;

const RUN: &str = "https://run.googleapis.com/v2";

pub struct WorkerPoolHolders {
    google: Google,
    /// `projects/<p>/locations/<r>/workerPools/<n>`.
    pool: String,
}

impl WorkerPoolHolders {
    pub fn new(google: Google, pool: String) -> Self {
        Self { google, pool }
    }
}

/// The copies a worker pool runs now, as Cloud Run describes it (an
/// absent count is none).
fn copies_of(pool: &Value) -> u64 {
    pool.pointer("/scaling/manualInstanceCount").and_then(Value::as_u64).unwrap_or(0)
}

#[async_trait]
impl HolderPool for WorkerPoolHolders {
    async fn resize(&self, copies: u32) -> anyhow::Result<()> {
        let url = format!("{RUN}/{}", self.pool);
        let pool = self.google.get(&url).await.map_err(|e| e.context(format!("read the holders' worker pool {}", self.pool)))?;
        if copies_of(&pool) == u64::from(copies) {
            return Ok(());
        }
        // Changing the count makes no new revision.
        let op = self
            .google
            .patch(&format!("{url}?update_mask=scaling.manualInstanceCount"), &json!({ "scaling": { "manualInstanceCount": copies } }))
            .await
            .map_err(|e| e.context(format!("run {copies} holder(s) on {}", self.pool)))?;
        self.google.wait(RUN, op).await?;
        tracing::info!(target: "weft_platform_gcp::holders", copies, "the holders' worker pool resized");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pool_with_no_count_runs_none() {
        assert_eq!(copies_of(&json!({ "scaling": { "manualInstanceCount": 2 } })), 2);
        assert_eq!(copies_of(&json!({ "scaling": {} })), 0);
        assert_eq!(copies_of(&json!({})), 0);
    }
}

//! What a node needs, on a machine: each need's `check` run here
//! (`weft_core::needs::Need::is_met`, the same check `weft deps-install`
//! runs), with the `PATH` the runtime has, which is the one its workers
//! get (`runner`).

use async_trait::async_trait;
use weft_core::needs::Need;
use weft_platform_traits::MachineNeeds;

pub struct LocalNeeds;

#[async_trait]
impl MachineNeeds for LocalNeeds {
    /// Every check runs at once, each on a blocking thread.
    async fn unmet(&self, needs: &[Need]) -> anyhow::Result<Vec<Need>> {
        let checks: Vec<_> = needs
            .iter()
            .map(|need| {
                let need = need.clone();
                tokio::task::spawn_blocking(move || (!need.is_met()).then_some(need))
            })
            .collect();
        let mut missing = Vec::new();
        for check in checks {
            if let Some(need) = check.await.map_err(|e| anyhow::anyhow!("a need's check stopped: {e}"))? {
                missing.push(need);
            }
        }
        Ok(missing)
    }

    fn needs_from(&self) -> weft_core::needs::NeedsFrom {
        weft_core::needs::NeedsFrom::Machine
    }
}

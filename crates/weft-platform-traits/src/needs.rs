//! What the machine a program's workers run on lacks of what its nodes
//! need (`weft_core::needs`): asked before a program starts, so one that
//! uses a node whose program is missing is refused naming it, rather than
//! failing at the node; and before an infra unit's process starts, of
//! what the process needs (`weft_core::infra::Process::needs`).

use async_trait::async_trait;
use weft_core::needs::{Need, NeedsFrom, NodeNeed};

#[async_trait]
pub trait MachineNeeds: Send + Sync {
    /// The ones of `needs` the machine its workers run on does not meet,
    /// in the order asked.
    async fn unmet(&self, needs: &[Need]) -> anyhow::Result<Vec<Need>>;

    /// The ones of `needs` (each a node type's need, with the program's
    /// nodes of that type) the machine its workers run on does not meet.
    async fn missing(&self, needs: &[NodeNeed]) -> anyhow::Result<Vec<NodeNeed>> {
        let asked: Vec<Need> = needs.iter().map(|row| row.need.clone()).collect();
        let unmet = self.unmet(&asked).await?;
        Ok(needs.iter().filter(|row| unmet.contains(&row.need)).cloned().collect())
    }

    /// Where this platform's workers get what their nodes need, which
    /// words the way to meet a missing one (`NeedsFrom::remedy`).
    fn needs_from(&self) -> NeedsFrom;
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;

    /// Every need is met except the ones named unmet.
    #[derive(Default)]
    pub struct FakeMachineNeeds {
        unmet: Mutex<std::collections::HashSet<String>>,
    }

    impl FakeMachineNeeds {
        pub fn new() -> Self {
            Self::default()
        }

        /// The need called `name` is not met.
        pub fn lacks(&self, name: &str) {
            self.unmet.lock().insert(name.to_string());
        }
    }

    #[async_trait]
    impl MachineNeeds for FakeMachineNeeds {
        async fn unmet(&self, needs: &[Need]) -> anyhow::Result<Vec<Need>> {
            let unmet = self.unmet.lock();
            Ok(needs.iter().filter(|n| unmet.contains(&n.name)).cloned().collect())
        }

        fn needs_from(&self) -> NeedsFrom {
            NeedsFrom::Machine
        }
    }
}

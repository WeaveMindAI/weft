//! Where the platform sends the requests of this process's own service,
//! among the revisions of it that run.
//!
//! On a cloud each role runs as copies of one revision of its service, and
//! an update or a rollback starts copies of another revision beside them.
//! Which one is current is the platform's to say, since it is the one
//! routing the requests: a copy of a revision it routes nothing to was
//! replaced (`weft_runtime::superseded`).

use async_trait::async_trait;

/// Where a service's requests go, as the platform says it now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Routing {
    /// The revisions that receive a share of the requests now.
    pub serving: Vec<String>,
    /// The revision the platform moves the requests to once it is ready,
    /// when the service follows its latest revision and that one does
    /// not receive any yet: a rollout under way.
    pub incoming: Option<String>,
}

#[async_trait]
pub trait ServiceRevisions: Send + Sync {
    /// Where this process's service sends its requests now. An answer
    /// waiting does not change (a permission the process lacks, a service
    /// name that is wrong) is a [`RoutingRefused`] in the error's chain.
    async fn routing(&self) -> anyhow::Result<Routing>;
}

/// The platform refused to say where the service's requests go, for a
/// reason waiting does not fix: what it answered.
#[derive(Debug)]
pub struct RoutingRefused(pub String);

impl std::fmt::Display for RoutingRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RoutingRefused {}

/// Answers what it was last told to, and counts how often it was asked.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    pub struct FakeServiceRevisions {
        /// The answer, or the error the platform gives.
        pub answer: parking_lot::Mutex<Result<Routing, String>>,
        pub asked: AtomicUsize,
    }

    impl FakeServiceRevisions {
        pub fn new(answer: Result<Routing, String>) -> Self {
            Self { answer: parking_lot::Mutex::new(answer), asked: AtomicUsize::new(0) }
        }

        pub fn answer(&self, answer: Result<Routing, String>) {
            *self.answer.lock() = answer;
        }

        pub fn asked(&self) -> usize {
            self.asked.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ServiceRevisions for FakeServiceRevisions {
        async fn routing(&self) -> anyhow::Result<Routing> {
            self.asked.fetch_add(1, Ordering::SeqCst);
            self.answer.lock().clone().map_err(anyhow::Error::msg)
        }
    }
}

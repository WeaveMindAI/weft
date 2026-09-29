//! Where a signal the listener serves reports what happened to it: a
//! fire (a timer's tick, an event on a held connection) and the kind's
//! evolving durable state (a feed cursor, a timer's next moment).
//!
//! The listener never calls the dispatcher. A fire is a `FireSignal` task
//! enqueued through the broker, which the dispatcher runs through
//! `dispatch_listener_outcome`; a state write goes to the signal row
//! through the broker at once, so the next wake, on whichever copy of the
//! listener it lands, reads it back.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_broker_client::BrokerSignalClient;
use weft_task_store::tasks::{NewTask, TaskTarget};
use weft_task_store::{TaskKind, TaskStoreClient};

/// What makes two fires of one signal the same event.
#[derive(Debug, Clone, Copy)]
pub enum FireIdentity<'a> {
    /// The payload itself: an event carries no name of its own.
    Payload,
    /// A name the kind gives the event (a timer's moment), so a re-fire
    /// with a different payload (a later `actualTime`) is still the same.
    Named(&'a str),
}

/// Listener-wide sink. Cheap to clone. NOT tenant-scoped: the listener
/// holds signals from many tenants, so the tenant travels per fire (it is
/// a property of the signal), never baked into the sink.
#[derive(Clone)]
pub struct FireSignalSink {
    tasks: Arc<dyn TaskStoreClient>,
    signals: Arc<BrokerSignalClient>,
}

impl FireSignalSink {
    pub fn new(tasks: Arc<dyn TaskStoreClient>, signals: Arc<BrokerSignalClient>) -> Self {
        Self { tasks, signals }
    }

    /// Enqueue a FireSignal task for this fire. `tenant_id` is the firing
    /// signal's tenant; the broker checks it against the signal's real one.
    ///
    /// The dedup key is derived from `(token, identity)`, so a retry of the
    /// SAME event collapses onto its task row while it is still queued,
    /// and two distinct events of one signal produce distinct tasks.
    pub async fn fire(
        &self,
        token: &str,
        tenant_id: &str,
        payload: Value,
        identity: FireIdentity<'_>,
    ) -> Result<weft_task_store::tasks::DedupOutcome> {
        let mut h = Sha256::new();
        h.update(token.as_bytes());
        h.update(b"\0");
        match identity {
            FireIdentity::Payload => h.update(serde_json::to_string(&payload)?.as_bytes()),
            // Prefixed so a name can never hash like some payload.
            FireIdentity::Named(name) => {
                h.update(b"name\0");
                h.update(name.as_bytes());
            }
        }
        let digest = h.finalize();
        let mut dedup = String::with_capacity(5 + 64);
        dedup.push_str("fire:");
        for b in digest.iter() {
            use std::fmt::Write;
            let _ = write!(&mut dedup, "{:02x}", b);
        }
        self.tasks
            .enqueue_dedup(NewTask {
                kind: TaskKind::FireSignal.into(),
                target: TaskTarget::Dispatcher,
                project_id: None,
                dedup_key: Some(dedup),
                execution_id: None,
                tenant_id: tenant_id.to_string(),
                target_instance: None,
                binary_hash: None,
                payload: serde_json::json!({ "token": token, "payload": payload }),
            })
            .await
    }

    /// Claim the kind's moment: write its durable state one version past
    /// `from_seq`, only while the row is still at `from_seq`. Whether it
    /// landed.
    pub async fn claim_kind_state(&self, token: &str, kind_state: Value, from_seq: i64) -> Result<bool> {
        self.signals.write_kind_state(token, kind_state, from_seq).await
    }
}

//! Sink that lets stateful kinds (Timer, SSE) report a held event
//! firing. In arch-5 we don't push HTTP to the dispatcher; we
//! enqueue a `FireSignal` task through the broker, and the
//! dispatcher's task picker runs `dispatch_listener_outcome`.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_task_store::tasks::{NewTask, TaskTarget};
use weft_task_store::{TaskKind, TaskStoreClient};

/// Per-listener sink. Cheap to clone (one Arc inside).
#[derive(Clone)]
pub struct FireSignalSink {
    tasks: Arc<dyn TaskStoreClient>,
    tenant_id: String,
}

impl FireSignalSink {
    pub fn new(tasks: Arc<dyn TaskStoreClient>, tenant_id: String) -> Self {
        Self { tasks, tenant_id }
    }

    /// Enqueue a FireSignal task for this fire.
    ///
    /// Dedup key is derived from `(token, payload)` so a transport-
    /// retry of the SAME event collapses onto the same task row, while
    /// two genuinely-distinct events with overlapping `token` but
    /// different `payload` produce distinct tasks. Random-UUID dedup
    /// keys would have collapsed nothing (each retry mints a new
    /// UUID); deterministic content hashing is the only shape that
    /// makes the "dedup" name honest.
    pub async fn fire(&self, token: &str, payload: Value) -> Result<()> {
        let payload_canon = serde_json::to_string(&payload)?;
        let mut h = Sha256::new();
        h.update(token.as_bytes());
        h.update(b"\0");
        h.update(payload_canon.as_bytes());
        let digest = h.finalize();
        let mut dedup = String::with_capacity(7 + 64);
        dedup.push_str("fire:");
        for b in digest.iter() {
            use std::fmt::Write;
            let _ = write!(&mut dedup, "{:02x}", b);
        }
        let task_payload = serde_json::json!({
            "token": token,
            "payload": payload,
        });
        self.tasks
            .enqueue_dedup(NewTask {
                kind: TaskKind::FireSignal,
                target: TaskTarget::Dispatcher,
                project_id: None,
                dedup_key: Some(dedup),
                color: None,
                tenant_id: Some(self.tenant_id.clone()),
                target_pod_name: None,
                payload: task_payload,
            })
            .await?;
        Ok(())
    }
}

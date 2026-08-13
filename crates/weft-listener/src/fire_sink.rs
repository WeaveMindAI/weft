//! Sink that lets stateful kinds (Timer, SSE) report a held event
//! firing. In arch-5 we don't push HTTP to the dispatcher; we
//! enqueue a `FireSignal` task through the broker, and the
//! dispatcher's task picker runs `dispatch_listener_outcome`.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_task_store::tasks::{NewTask, TaskTarget};
use weft_task_store::{TaskKind, TaskStoreClient, UpdateSignalKindStatePayload};

/// Listener-wide sink. Cheap to clone (one Arc inside). NOT tenant-
/// scoped: a pooled listener holds signals from many tenants, so the
/// tenant travels per-fire (it is a property of the signal, read from
/// the signal's registry entry), never baked into the sink.
#[derive(Clone)]
pub struct FireSignalSink {
    tasks: Arc<dyn TaskStoreClient>,
}

impl FireSignalSink {
    pub fn new(tasks: Arc<dyn TaskStoreClient>) -> Self {
        Self { tasks }
    }

    /// Enqueue a FireSignal task for this fire. `tenant_id` is the
    /// firing signal's tenant; the broker stamps it on the task (the
    /// listener is a trusted control-plane caller allowed to enqueue
    /// for any tenant, validated against the signal's real tenant).
    ///
    /// `placement_generation` is the generation this pod holds the signal
    /// under. It rides on the task payload (NOT the dedup key) so the
    /// broker can fence a stale fire: a fire whose generation is below the
    /// signal row's current generation came from a pod that has since been
    /// drained, and is dropped. Including it in the dedup key would be
    /// wrong: a transport-retry of the same event must still collapse, and
    /// a re-placement (new generation) of the same logical event must NOT
    /// resurrect a dropped task.
    ///
    /// Dedup key is derived from `(token, payload)` so a transport-
    /// retry of the SAME event collapses onto the same task row, while
    /// two genuinely-distinct events with overlapping `token` but
    /// different `payload` produce distinct tasks. Random-UUID dedup
    /// keys would have collapsed nothing (each retry mints a new
    /// UUID); deterministic content hashing is the only shape that
    /// makes the "dedup" name honest.
    /// Returns the enqueue's [`DedupOutcome`]: the caller must see a
    /// `Fenced` drop (this pod was drained; the fire was deliberately
    /// NOT delivered) as distinct from a delivered fire, or a
    /// cursor-keeping kind would advance past an undelivered item.
    pub async fn fire(
        &self,
        token: &str,
        tenant_id: &str,
        placement_generation: i64,
        payload: Value,
    ) -> Result<weft_task_store::tasks::DedupOutcome> {
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
            "placement_generation": placement_generation,
        });
        self.tasks
            .enqueue_dedup(NewTask {
                kind: TaskKind::FireSignal.into(),
                target: TaskTarget::Dispatcher,
                project_id: None,
                dedup_key: Some(dedup),
                color: None,
                tenant_id: Some(tenant_id.to_string()),
                target_pod_name: None,
                binary_hash: None,
                payload: task_payload,
            })
            .await
    }

    /// Enqueue a durable kind-state write (a delta-poll cursor
    /// advancing). `seq` must be strictly increasing per holder (the
    /// kind's task counts its own updates); the dispatcher's fenced
    /// write drops an out-of-order or stale-pod update, so reordering
    /// on the queue is harmless. Dedup on `(token, seq)`: an enqueue
    /// retry of the same update collapses, distinct updates never do.
    pub async fn update_kind_state(
        &self,
        token: &str,
        tenant_id: &str,
        placement_generation: i64,
        kind_state: Value,
        seq: i64,
    ) -> Result<()> {
        let payload = UpdateSignalKindStatePayload {
            token: token.to_string(),
            kind_state,
            seq,
            placement_generation,
        };
        self.tasks
            .enqueue_dedup(NewTask {
                kind: TaskKind::UpdateSignalKindState.into(),
                target: TaskTarget::Dispatcher,
                project_id: None,
                dedup_key: Some(format!("kindstate:{token}:{seq}")),
                color: None,
                tenant_id: Some(tenant_id.to_string()),
                target_pod_name: None,
                binary_hash: None,
                payload: serde_json::to_value(&payload)?,
            })
            .await?;
        Ok(())
    }
}

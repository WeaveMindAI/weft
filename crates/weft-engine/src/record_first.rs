//! A broker call that names a run, made only once the run's record so far
//! is written.
//!
//! The broker answers a call about a run (a connection opened, a file
//! stored, an endpoint read, a tag, a signal registered) by the run's row,
//! which the run's first write makes, and it scopes the call by it. A fast
//! run starts before anything of it is written, so every client below
//! first has the process's writer write what the run handed so far
//! ([`WorkerJournal::record_first`]): the broker always finds the row, and
//! nothing about a run reaches it ahead of what the run did before. An
//! unrecorded run leaves a note of itself the same way. These sit beneath
//! the process's kept answers (`crate::held`), so a call answered from
//! those writes nothing.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use weft_core::error::{WeftError, WeftResult};
use weft_core::storage::{ByteRange, ByteStream, KeepTtl, StorageScope, StoredFileMeta};
use weft_core::ExecutionId;
use weft_task_store::{InfraReader, TaskStoreClient};

use crate::context::{AccessBroker, ExecutionSteeringClient};
use crate::journal_writer::WorkerJournal;
use crate::storage::WorkerStorageOps;

/// `inner`, each call naming a run made once `writer` wrote the run's
/// record so far.
pub struct RecordFirst<T: ?Sized> {
    inner: Arc<T>,
    writer: Arc<WorkerJournal>,
}

impl<T: ?Sized> RecordFirst<T> {
    pub fn new(inner: Arc<T>, writer: Arc<WorkerJournal>) -> Arc<Self> {
        Arc::new(Self { inner, writer })
    }

    async fn first(&self, execution_id: ExecutionId) -> anyhow::Result<()> {
        self.writer.record_first(execution_id).await
    }

    /// [`Self::first`] for a storage call, whose failure is a node's.
    async fn first_for_storage(&self, execution_id: ExecutionId) -> WeftResult<()> {
        self.first(execution_id).await.map_err(|e| WeftError::NodeExecution(format!("storage: {e:#}")))
    }
}

/// A task about a run (a signal it registers, a program it calls).
#[async_trait]
impl TaskStoreClient for RecordFirst<dyn TaskStoreClient> {
    async fn enqueue_dedup(&self, spec: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
        if let Some(execution_id) = spec.execution_id {
            self.first(execution_id).await?;
        }
        self.inner.enqueue_dedup(spec).await
    }

    async fn wait_for_terminal(&self, task_id: uuid::Uuid, timeout: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
        self.inner.wait_for_terminal(task_id, timeout).await
    }
}

#[async_trait]
impl AccessBroker for RecordFirst<dyn AccessBroker> {
    async fn resolve_connection(
        &self,
        req: &weft_broker_client::protocol::ResolveConnectionRequest,
        run_instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<weft_broker_client::protocol::ResolveConnectionResponse> {
        self.first(req.execution_id).await?;
        self.inner.resolve_connection(req, run_instance).await
    }

    async fn release_connection(
        &self,
        req: &weft_broker_client::protocol::ReleaseConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ReleaseConnectionResponse> {
        self.first(req.execution_id).await?;
        self.inner.release_connection(req).await
    }

    async fn publish_access(
        &self,
        req: &weft_broker_client::protocol::PublishAccessRequest,
        run_instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishAccessResponse> {
        self.first(req.execution_id).await?;
        self.inner.publish_access(req, run_instance).await
    }

    async fn published_access(
        &self,
        req: &weft_broker_client::protocol::PublishedAccessRequest,
        run_instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishedAccessResponse> {
        self.first(req.execution_id).await?;
        self.inner.published_access(req, run_instance).await
    }

    async fn mint_instance_token(
        &self,
        req: &weft_broker_client::protocol::ProgramMintInstanceTokenRequest,
    ) -> anyhow::Result<weft_core::program::MintedInstanceToken> {
        self.first(req.execution_id).await?;
        self.inner.mint_instance_token(req).await
    }
}

#[async_trait]
impl InfraReader for RecordFirst<dyn InfraReader> {
    async fn endpoint_address(
        &self,
        execution_id: ExecutionId,
        run_instance: Option<&weft_core::instance::InstanceId>,
        infra: &weft_core::infra::InfraHandle,
    ) -> anyhow::Result<Option<weft_core::infra::EndpointAddress>> {
        self.first(execution_id).await?;
        self.inner.endpoint_address(execution_id, run_instance, infra).await
    }

    async fn baked_outputs(
        &self,
        execution_id: ExecutionId,
        run_instance: Option<&weft_core::instance::InstanceId>,
        place: &str,
        copy: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<std::collections::BTreeMap<String, serde_json::Value>> {
        self.first(execution_id).await?;
        self.inner.baked_outputs(execution_id, run_instance, place, copy).await
    }
}

#[async_trait]
impl ExecutionSteeringClient for RecordFirst<dyn ExecutionSteeringClient> {
    async fn tag_execution(&self, execution_id: ExecutionId, tags: Vec<String>) -> anyhow::Result<()> {
        self.first(execution_id).await?;
        self.inner.tag_execution(execution_id, tags).await
    }

    async fn stop_tagged(&self, execution_id: ExecutionId, tag: String, stop_self: weft_core::StopSelf) -> anyhow::Result<bool> {
        self.first(execution_id).await?;
        self.inner.stop_tagged(execution_id, tag, stop_self).await
    }
}

/// A store also says the run stored files of its own, before the bytes
/// move, so its ending's sweep reclaims them
/// ([`WorkerJournal::stored_files`]).
#[async_trait]
impl WorkerStorageOps for RecordFirst<dyn WorkerStorageOps> {
    async fn put(
        &self,
        execution_id: ExecutionId,
        scope: &StorageScope,
        identity: Option<&str>,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value> {
        self.writer.stored_files(execution_id);
        self.first_for_storage(execution_id).await?;
        self.inner.put(execution_id, scope, identity, mime_type, filename, keep, declared_size, data).await
    }
    async fn replace(
        &self,
        execution_id: ExecutionId,
        key: &str,
        expected_version: Option<u64>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<weft_core::storage::ReplaceOutcome> {
        self.writer.stored_files(execution_id);
        self.first_for_storage(execution_id).await?;
        self.inner.replace(execution_id, key, expected_version, declared_size, data).await
    }
    async fn get(&self, execution_id: ExecutionId, key: &str, range: Option<ByteRange>) -> WeftResult<(StoredFileMeta, ByteStream)> {
        self.first_for_storage(execution_id).await?;
        self.inner.get(execution_id, key, range).await
    }
    async fn delete(&self, execution_id: ExecutionId, key: &str) -> WeftResult<()> {
        self.first_for_storage(execution_id).await?;
        self.inner.delete(execution_id, key).await
    }
    async fn list(&self, execution_id: ExecutionId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
        self.first_for_storage(execution_id).await?;
        self.inner.list(execution_id, scope).await
    }
    async fn find(&self, execution_id: ExecutionId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>> {
        self.first_for_storage(execution_id).await?;
        self.inner.find(execution_id, scope, identity).await
    }
    async fn keep(&self, execution_id: ExecutionId, key: &str, ttl: KeepTtl) -> WeftResult<()> {
        self.first_for_storage(execution_id).await?;
        self.inner.keep(execution_id, key, ttl).await
    }
    async fn presign(&self, execution_id: ExecutionId, key: &str, ttl_secs: Option<u64>) -> WeftResult<String> {
        self.first_for_storage(execution_id).await?;
        self.inner.presign(execution_id, key, ttl_secs).await
    }
    async fn public_link(
        &self,
        execution_id: ExecutionId,
        key: &str,
        ttl_secs: Option<u64>,
        reach: weft_core::storage::LinkReach,
    ) -> WeftResult<Option<String>> {
        self.first_for_storage(execution_id).await?;
        self.inner.public_link(execution_id, key, ttl_secs, reach).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_writer::{RunSpec, WriterSettings};
    use crate::test_record::FakeRecord;

    /// Sees what the record holds of a run when a task naming it arrives.
    struct SeeingTasks {
        record: Arc<FakeRecord>,
        seen: std::sync::Mutex<Vec<Vec<String>>>,
    }

    #[async_trait]
    impl TaskStoreClient for SeeingTasks {
        async fn enqueue_dedup(&self, spec: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            self.seen.lock().unwrap().push(self.record.log(spec.execution_id.expect("names a run")));
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(uuid::Uuid::nil()))
        }
        async fn wait_for_terminal(&self, _: uuid::Uuid, _: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unimplemented!("not asked")
        }
    }

    /// A task naming a run reaches the broker only once what the run
    /// handed before it is on record; for an unrecorded run, its birth, so
    /// its row exists.
    #[tokio::test]
    async fn a_call_naming_a_run_follows_its_record() {
        let record = Arc::new(FakeRecord::default());
        let writer = WorkerJournal::start(
            record.clone(),
            WriterSettings { gather: std::time::Duration::from_secs(3600), ..Default::default() },
            &tokio::runtime::Handle::current(),
        );
        let seeing = Arc::new(SeeingTasks { record: record.clone(), seen: Default::default() });
        let tasks = RecordFirst::new(seeing.clone() as Arc<dyn TaskStoreClient>, writer.clone());
        for recorded in [true, false] {
            let run = weft_core::new_execution_id();
            let settings = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Fast, recorded).unwrap();
            let drive = writer.run(RunSpec {
                execution_id: run,
                settings,
                keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
                epoch: 1,
                next_seq: 0,
                redaction: Default::default(),
            });
            let mut birth: weft_journal::ExecEvent = serde_json::from_value(serde_json::json!({
                "kind": "execution_started", "project_id": run, "entry_node": "a", "phase": "fire", "at_unix": 0
            }))
            .unwrap();
            if let weft_journal::ExecEvent::ExecutionStarted { execution_id, settings: kept, .. } = &mut birth {
                *execution_id = run;
                *kept = settings;
            }
            let event = weft_journal::ExecEvent::NodeStarted { execution_id: run, node_id: "a".into(), frames: Vec::new(), at_unix: 0 };
            weft_journal::JournalClient::record_events(drive.as_ref(), &[birth, event], Some("w")).await.unwrap();
            tasks
                .enqueue_dedup(weft_task_store::tasks::NewTask {
                    kind: weft_task_store::kinds::TaskKind::RegisterSignal.as_str().into(),
                    project_id: None,
                    dedup_key: None,
                    execution_id: Some(run),
                    tenant_id: "t".into(),
                    payload: Value::Null,
                })
                .await
                .unwrap();
        }
        assert_eq!(
            *seeing.seen.lock().unwrap(),
            vec![vec!["execution_started".to_string(), "a".to_string()], vec!["execution_started".to_string()]]
        );
    }
}

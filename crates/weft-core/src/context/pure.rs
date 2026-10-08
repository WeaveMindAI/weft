//! The handle a pure node's body runs on (`features.pure`,
//! [`crate::node::NodeFeatures::pure`]). A pure body does nothing outside
//! its run that weft does not put on record first, which is what lets a
//! durable run start it without first putting what came before on record,
//! and run it again when its worker died mid-step. So every call that
//! reaches outside on its own (the network, a connection, a wait,
//! `ctx.run`, the broker's steering, storage beyond this run) fails here,
//! naming the flag: a node marked pure by mistake fails loudly at its
//! first such call, where it runs, in a node test exactly as in a run.
//!
//! What stays open is the run itself: inputs, outputs, ports, buses, logs,
//! the process-wide shared values, and `register_signal`, which is a
//! trigger arming itself in its setup run, never something a run it fires
//! does. And the run's own caller and files: answering the caller (a
//! durable run's answer leaves only once its record is written, and after
//! a crash no caller is left to answer twice), reading stored files,
//! storing files for this run alone, and handing the caller links to
//! them.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use super::{ContextHandle, EndpointMethod, LogLevel};
use crate::cancellation::CancellationFlag;
use crate::caller::CallerConnection;
use crate::error::{WeftError, WeftResult};
use crate::primitive::SignalSpec;
use crate::tag::StopSelf;
use crate::weft_type::WeftType;

/// The refusal a pure node's outside call gets: what it tried, and the way
/// out.
pub(crate) fn not_pure(node_id: &str, tried: &str) -> String {
    format!(
        "node '{node_id}' is marked pure (`features.pure`), so it may not {tried}: a pure node does nothing \
         outside its run beyond answering its caller and the run's own files. If this node reaches \
         outside, remove `pure` from its metadata.json"
    )
}

/// [`ContextHandle`] for a pure node: `inner` for what stays inside the
/// run, a refusal for the rest (see the module doc).
pub(crate) struct PureHandle {
    pub(crate) inner: Arc<dyn ContextHandle>,
    pub(crate) node_id: String,
}

impl PureHandle {
    fn refused<T>(&self, tried: &str) -> WeftResult<T> {
        Err(WeftError::Config(not_pure(&self.node_id, tried)))
    }
}

/// Refuses every request a pure node's HTTP client sends.
struct RefuseRequests {
    message: String,
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for RefuseRequests {
    async fn handle(
        &self,
        _req: reqwest::Request,
        _extensions: &mut http::Extensions,
        _next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        Err(reqwest_middleware::Error::Middleware(anyhow::anyhow!(self.message.clone())))
    }
}

#[async_trait::async_trait]
impl ContextHandle for PureHandle {
    /// A client every request of which fails, naming the flag: `ctx.http()`
    /// hands a client rather than a result, so the refusal comes with the
    /// first request.
    fn plain_http(&self) -> reqwest_middleware::ClientWithMiddleware {
        reqwest_middleware::ClientBuilder::new(crate::access::client::base_client().clone())
            .with(RefuseRequests { message: not_pure(&self.node_id, "send an HTTP request") })
            .build()
    }

    async fn await_signal(&self, _spec: SignalSpec) -> WeftResult<Value> {
        self.refused("wait for a signal")
    }

    async fn register_signal(&self, spec: SignalSpec, port_snapshot: Value) -> WeftResult<()> {
        self.inner.register_signal(spec, port_snapshot).await
    }

    fn own_infra(&self, name: &str, instance: Option<&crate::instance::InstanceId>) -> WeftResult<crate::infra::InfraHandle> {
        self.inner.own_infra(name, instance)
    }

    async fn endpoint_address(&self, _infra: &crate::infra::InfraHandle) -> WeftResult<crate::infra::EndpointAddress> {
        self.refused("reach an endpoint")
    }

    fn shared(&self) -> Arc<crate::shared::Shared> {
        self.inner.shared()
    }

    async fn endpoint_call(&self, _url: &str, _method: EndpointMethod, _path: &str, _body: Option<Value>) -> WeftResult<Value> {
        self.refused("call an endpoint")
    }

    async fn run_step(&self, _name: &str) -> WeftResult<(u32, Option<Value>)> {
        self.refused("record a step with `ctx.run`")
    }

    async fn run_record(&self, _name: &str, _call_index: u32, _value: &Value) -> WeftResult<()> {
        self.refused("record a step with `ctx.run`")
    }

    async fn open_connection(&self, _access: &crate::access::Access, _window: std::time::Duration) -> WeftResult<crate::access::OpenedConnection> {
        self.refused("open a connection")
    }

    async fn publish_access(&self, _values: std::collections::BTreeMap<String, String>) -> WeftResult<crate::access::Access> {
        self.refused("publish a connection")
    }

    async fn published_access(&self) -> WeftResult<Option<crate::access::Access>> {
        self.refused("read a published connection")
    }

    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()> {
        self.inner.log(level, message).await
    }

    async fn tag_execution(&self, _tags: Vec<String>) -> WeftResult<()> {
        self.refused("tag its run")
    }

    async fn stop_tagged(&self, _tag: String, _stop_self: StopSelf) -> WeftResult<()> {
        self.refused("stop tagged runs")
    }

    async fn program_call(&self, _call: crate::program::ProgramCall, _stop_self: StopSelf, _call_index: u32) -> WeftResult<Value> {
        self.refused("call on its program")
    }

    async fn mint_instance_token(
        &self,
        _instance: &crate::instance::InstanceId,
        _expires_in_secs: u64,
        _displays: bool,
        _id: uuid::Uuid,
    ) -> WeftResult<crate::program::MintedInstanceToken> {
        self.refused("mint an instance token")
    }

    fn cancellation(&self) -> Arc<CancellationFlag> {
        self.inner.cancellation()
    }

    fn declared_output_ports(&self) -> &HashMap<String, WeftType> {
        self.inner.declared_output_ports()
    }

    fn declared_input_ports(&self) -> &HashMap<String, WeftType> {
        self.inner.declared_input_ports()
    }

    fn wired_output_ports(&self) -> &std::collections::HashSet<String> {
        self.inner.wired_output_ports()
    }

    fn catches_errors(&self) -> bool {
        self.inner.catches_errors()
    }

    async fn pulse_downstream(&self, output: crate::node::NodeOutput, wait_delivered: bool) -> WeftResult<()> {
        self.inner.pulse_downstream(output, wait_delivered).await
    }

    fn set_max_buffered_items(&self, port: &str, items: usize) -> WeftResult<()> {
        self.inner.set_max_buffered_items(port, items)
    }

    async fn close_port(&self, port: &str) -> WeftResult<()> {
        self.inner.close_port(port).await
    }

    fn create_bus(&self, opts: crate::bus::BusOptions) -> WeftResult<(crate::bus::BusHandle, Value)> {
        self.inner.create_bus(opts)
    }

    fn bus(&self, marker: &Value) -> WeftResult<crate::bus::BusHandle> {
        self.inner.bus(marker)
    }

    /// A file for this run alone: nothing outside the run sees it, and the
    /// run's files go with the run.
    async fn storage_put(
        &self,
        scope: &crate::storage::StorageScope,
        identity: Option<&str>,
        data: crate::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<crate::storage::KeepTtl>,
        declared_size: Option<u64>,
    ) -> WeftResult<Value> {
        match (scope, keep) {
            (crate::storage::StorageScope::Execution, None) => {
                self.inner.storage_put(scope, identity, data, mime_type, filename, keep, declared_size).await
            }
            _ => self.refused("store a file beyond this run"),
        }
    }

    async fn storage_put_from_url(
        &self,
        _scope: &crate::storage::StorageScope,
        _identity: Option<&str>,
        _url: &str,
        _filename: Option<&str>,
        _keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        self.refused("store a file")
    }

    async fn storage_get(
        &self,
        key: &str,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        self.inner.storage_get(key, range).await
    }

    async fn storage_get_url(
        &self,
        _url: &str,
        _declared_mime: &str,
        _declared_filename: &str,
        _declared_size: u64,
        _range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        self.refused("read a file")
    }

    async fn storage_delete(&self, _key: &str) -> WeftResult<()> {
        self.refused("delete a stored file")
    }

    async fn storage_list(&self, scope: &crate::storage::StorageScope) -> WeftResult<Vec<crate::storage::StoredFileMeta>> {
        self.inner.storage_list(scope).await
    }

    async fn storage_replace(
        &self,
        _key: &str,
        _expected_version: Option<u64>,
        _data: crate::storage::ByteStream,
        _declared_size: Option<u64>,
    ) -> WeftResult<crate::storage::ReplaceOutcome> {
        self.refused("change a stored file")
    }

    async fn record_file_edit(&self, _edit: crate::storage::FileEdit) -> WeftResult<()> {
        self.refused("change a stored file")
    }

    async fn storage_keep(&self, _key: &str, _ttl: crate::storage::KeepTtl) -> WeftResult<()> {
        self.refused("keep a stored file")
    }

    async fn storage_presign(&self, _key: &str, _ttl_secs: Option<u64>) -> WeftResult<String> {
        self.refused("link a stored file")
    }

    /// A link for the run's own caller to fetch; one for anybody else
    /// leaves the run.
    async fn storage_public_link(&self, key: &str, ttl_secs: Option<u64>, reach: crate::storage::LinkReach) -> WeftResult<Option<String>> {
        match reach {
            crate::storage::LinkReach::Caller { .. } => self.inner.storage_public_link(key, ttl_secs, reach).await,
            _ => self.refused("link a stored file for anybody but its caller"),
        }
    }

    fn wake_payload(&self) -> Option<&Value> {
        self.inner.wake_payload()
    }

    /// The run's caller, read and answered: a durable run's answer leaves
    /// only once its record is written.
    fn caller_connection(&self) -> Option<Arc<dyn CallerConnection>> {
        self.inner.caller_connection()
    }
}

//! InstanceProbe: a project-local node reporting which instance a run is
//! in. It answers the instance the run carries, the connection it
//! received on `key` (the instance's own connection, for an access step
//! whose connection is `@instance_filled`), and how many files that
//! instance's storage holds once this run has written `text` into it.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct InstanceProbeNode;

#[async_trait]
impl Node for InstanceProbeNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        let key: Option<serde_json::Value> = ctx.inputs.opt("key")?;
        let instance = ctx.instance().map(|m| m.as_str().to_string());
        let files = ctx.storage(StorageScope::instance());
        files.put(text.into_bytes(), "text/plain", "note.txt", None).await?;
        let held = files.list().await?.len();
        let body = serde_json::json!({ "instance": instance, "key": key, "files": held });
        ctx.pulse_downstream(NodeOutput::new().set("body", body)).await
    }
}

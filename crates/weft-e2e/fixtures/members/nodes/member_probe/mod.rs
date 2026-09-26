//! MemberProbe: a project-local node reporting who a run is for. It
//! answers the member the run carries, the connection it received on
//! `key` (the member's own connection, for an access step whose
//! connection is `@member_filled`), and how
//! many files that member's storage holds once this run has written
//! `text` into it.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct MemberProbeNode;

#[async_trait]
impl Node for MemberProbeNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        let key: Option<serde_json::Value> = ctx.inputs.opt("key")?;
        let member = ctx.member().map(|m| m.as_str().to_string());
        let files = ctx.storage(StorageScope::member());
        files.put(text.into_bytes(), "text/plain", "note.txt", None).await?;
        let held = files.list().await?.len();
        let body = serde_json::json!({ "member": member, "key": key, "files": held });
        ctx.pulse_downstream(NodeOutput::new().set("body", body)).await
    }
}

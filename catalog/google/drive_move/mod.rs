//! GoogleDriveMove: move a file into a folder (replacing its current
//! parents), optionally renaming it on the way.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::url;
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::drive;

#[derive(NodeManifest)]
pub struct GoogleDriveMoveNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDriveMoveNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file_id: String = ctx.inputs.get("fileId")?;
        let folder: String = ctx.inputs.get("folder")?;
        let rename: Option<String> = ctx.inputs.opt("rename")?;

        let http = ctx.client(&account).await?;
        let meta: Value =
            drive::file_meta(&http, &file_id, "parents", "google drive: read current parents")
                .await?;
        let old_parents: Vec<&str> = meta["parents"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();

        let url = url(drive::API, &["files", &file_id])?
            .query("addParents", &folder)
            .query("fields", "id")
            .query_opt("removeParents", (!old_parents.is_empty()).then(|| old_parents.join(",")));
        let mut body = json!({});
        if let Some(n) = rename {
            body["name"] = json!(n);
        }
        weft::access::client::json_call(http.patch(url.as_str()).json(&body), "move the file").await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}

//! GoogleDriveCopy: copy a file, optionally into a folder and under a
//! new name. The create-from-template pattern: keep a template doc,
//! copy it per run, fill the copy.

use async_trait::async_trait;

use weft::access::client::url;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::drive;

#[derive(NodeManifest)]
pub struct GoogleDriveCopyNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDriveCopyNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file_id: String = ctx.inputs.get("fileId")?;
        let name: Option<String> = ctx.inputs.opt("name")?;
        let folder: Option<String> = ctx.inputs.opt("folder")?;

        let body = drive::file_metadata(name, folder);
        let http = ctx.client(&account).await?;
        let answer = weft::access::client::json_call(
            http.post(url(drive::API, &["files", &file_id, "copy"])?.query("fields", "id,webViewLink").as_str())
            .json(&body),
            "copy the file",
        )
        .await?;
        ctx.pulse_downstream(drive::created_file_output(&answer, "copy")?).await
    }
}

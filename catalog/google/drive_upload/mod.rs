//! GoogleDriveUpload: put a stored file into Drive (optionally inside
//! a folder), via the multipart upload (metadata + bytes in one
//! request).

use async_trait::async_trait;

use weft::access::client::{url, Multipart, StoredBodyExt};
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::drive;

#[derive(NodeManifest)]
pub struct GoogleDriveUploadNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDriveUploadNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let name: Option<String> = ctx.inputs.opt("name")?;
        let folder: Option<String> = ctx.inputs.opt("folder")?;

        let body = ctx.storage(StorageScope::Run).body(&file).await?;
        let file_meta =
            drive::file_metadata(Some(name.unwrap_or_else(|| body.filename().to_string())), folder);

        // The related-multipart body Drive's one-shot upload expects:
        // the JSON metadata part, then the file, streamed out of storage.
        let form = Multipart::related()
            .part("application/json; charset=UTF-8", file_meta.to_string())
            .stored_part(body);

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::json_call(
            http.post(
                url(drive::UPLOAD_API, &["files"])?
                    .query("uploadType", "multipart")
                    .query("fields", "id,webViewLink")
                    .as_str(),
            )
            .multipart(form),
            "upload the file",
        )
        .await?;
        ctx.pulse_downstream(drive::created_file_output(&answer, "upload")?).await
    }
}

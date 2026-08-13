//! GoogleDriveUpload: put a stored file into Drive (optionally inside
//! a folder), via the multipart upload (metadata + bytes in one
//! request).

use async_trait::async_trait;

use weft::access::client::Multipart;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let name: Option<String> = ctx.inputs.opt("name")?;
        let folder: Option<String> = ctx.inputs.opt("folder")?;

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&file).await?;
        let file_meta = drive::file_metadata(
            Some(name.filter(|n| !n.trim().is_empty()).unwrap_or_else(|| meta.filename.clone())),
            folder,
        );

        // The related-multipart body Drive's one-shot upload expects:
        // the JSON metadata part, then the bytes.
        let (content_type, body) = Multipart::related()
            .part("application/json; charset=UTF-8", file_meta.to_string())
            .part(&meta.mime_type, bytes)
            .build();

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::json_call(
            http.post(format!(
                "{}/files?uploadType=multipart&fields=id,webViewLink",
                drive::UPLOAD_API
            ))
            .header("content-type", content_type)
            .body(body),
            "upload the file",
        )
        .await?;
        ctx.pulse_downstream(drive::created_file_output(&answer, "upload")?).await
    }
}

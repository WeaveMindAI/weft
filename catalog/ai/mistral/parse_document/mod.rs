//! MistralParseDocument: a PDF/scan/image to clean markdown through
//! Mistral's OCR. Three calls: upload the stored file (the OCR
//! endpoint eats URLs, not bytes), mint its short-lived signed URL,
//! run the OCR, and emit the pages' markdown.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::access::client::{get_json, json_call, post_json, Multipart};
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct MistralParseDocumentNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MistralParseDocumentNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let file: FileHandle = ctx.inputs.get("file")?;

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&file).await?;
        let http = ctx.client(&access).await?;

        // 1. Upload for OCR (two fields plus the file bytes).
        let (content_type, body) = Multipart::form_data()
            .text("purpose", "ocr")
            .file("file", &meta.filename, &meta.mime_type, bytes)
            .build();
        let uploaded = json_call(
            http.post("https://api.mistral.ai/v1/files")
                .header("content-type", content_type)
                .body(body),
            "mistral: upload the document",
        )
        .await?;
        let file_id = uploaded["id"].as_str().node_err("mistral: upload carries no id")?;

        // 2. The short-lived signed URL the OCR call reads from.
        let signed: Value = get_json(
            &http,
            &format!("https://api.mistral.ai/v1/files/{file_id}/url?expiry=1"),
            "mistral: mint the document url",
        )
        .await?;
        let doc_url = signed["url"].as_str().node_err("mistral: signed url missing")?;

        // 3. The OCR itself.
        let answer = post_json(
            &http,
            "https://api.mistral.ai/v1/ocr",
            &json!({
                "model": "mistral-ocr-latest",
                "document": { "type": "document_url", "document_url": doc_url },
            }),
            "mistral: ocr",
        )
        .await?;

        // The upload was only ever OCR fuel: delete it so runs don't
        // pile files onto the Mistral account. The OCR result above is
        // the load-bearing outcome, so a failed delete logs loudly and
        // still delivers it (a stray remote file is recoverable from
        // Mistral's file list; a discarded OCR is not).
        let deleted = http
            .delete(format!("https://api.mistral.ai/v1/files/{file_id}"))
            .send()
            .await;
        match deleted {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                let status = resp.status();
                ctx.log(
                    weft::context::LogLevel::Error,
                    format!(
                        "mistral answered {status} deleting uploaded file {file_id} after the \
                         OCR; remove it from the account's file list by hand"
                    ),
                )
                .await?;
            }
            Err(e) => {
                ctx.log(
                    weft::context::LogLevel::Error,
                    format!(
                        "could not delete uploaded file {file_id} after the OCR: {e}; remove \
                         it from the account's file list by hand"
                    ),
                )
                .await?;
            }
        }

        let pages: Vec<Value> = answer["pages"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| json!({ "index": p["index"], "markdown": p["markdown"] }))
            .collect();
        let markdown = pages
            .iter()
            .filter_map(|p| p["markdown"].as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        let count = pages.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("markdown", markdown)
                .set("pages", json!(pages))
                .set("pageCount", count),
        )
        .await
    }
}

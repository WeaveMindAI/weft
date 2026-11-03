//! MistralParseDocument: a PDF/scan/image to clean markdown through
//! Mistral's OCR. One call: the OCR endpoint reads the document from a
//! link (the stored file's public address, or its bytes inline as a
//! `data:` URL where weft serves no public one), and the pages'
//! markdown comes back.
//!
//! The markdown comes out as stored files, never as text on a wire: a
//! long document's markdown weighs far more than the 100 KB a wire
//! carries, and the OCR it came from is paid for per page, so cutting
//! it or refusing it would throw paid work away.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::access::client::post_json;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let file: FileHandle = ctx.inputs.get("file")?;

        let document = ctx.storage(StorageScope::Run).external_file(&file).await?;
        // Mistral reads an image as an `image_url` and every other
        // document (PDF, DOCX, PPTX, ...) as a `document_url`.
        let source = if document.mime_type.starts_with("image/") {
            json!({ "type": "image_url", "image_url": document.url })
        } else {
            json!({ "type": "document_url", "document_url": document.url })
        };
        let http = ctx.client(&access).await?;
        let answer = post_json(
            &http,
            "https://api.mistral.ai/v1/ocr",
            &json!({ "model": "mistral-ocr-latest", "document": source }),
            "mistral: ocr",
        )
        .await?;

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
        let storage = ctx.storage(StorageScope::Run);
        let stem = file_stem(&document.filename);
        let markdown = storage
            .put(markdown.into_bytes(), "text/markdown", &format!("{stem}.md"), None)
            .await?;
        let pages = storage.put_json(&pages, &format!("{stem}.pages.json"), None).await?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("markdown", markdown)
                .set("pages", pages)
                .set("pageCount", count),
        )
        .await
    }
}

/// The document's file name without its extension, to name the files
/// made from it (`report.pdf` gives `report`); a name with no stem of
/// its own is kept as it is.
fn file_stem(filename: &str) -> &str {
    match filename.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => stem,
        _ => filename,
    }
}

//! Document Node - Document/file data input (pdf, docx, pptx, xlsx, csv, txt, zip, etc.)
//!
//! Outputs a standardized media object that can be consumed by send nodes.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct DocumentNode;

const DOC_EXTENSIONS: &[(&str, &str)] = &[
    ("pdf", "application/pdf"),
    ("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ("doc", "application/msword"),
    ("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    ("ppt", "application/vnd.ms-powerpoint"),
    ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ("xls", "application/vnd.ms-excel"),
    ("csv", "text/csv"),
    ("txt", "text/plain"),
    ("md", "text/markdown"),
    ("json", "application/json"),
    ("xml", "application/xml"),
    ("html", "text/html"),
    ("zip", "application/zip"),
    ("rar", "application/vnd.rar"),
    ("7z", "application/x-7z-compressed"),
    ("tar", "application/x-tar"),
    ("gz", "application/gzip"),
    ("rtf", "application/rtf"),
    ("odt", "application/vnd.oasis.opendocument.text"),
    ("ods", "application/vnd.oasis.opendocument.spreadsheet"),
    ("odp", "application/vnd.oasis.opendocument.presentation"),
];

fn guess_mimetype(url: &str) -> &'static str {
    let lower = url.to_lowercase();
    for (ext, mime) in DOC_EXTENSIONS {
        if lower.ends_with(&format!(".{}", ext)) {
            return mime;
        }
    }
    "application/octet-stream"
}

#[async_trait]
impl Node for DocumentNode {
    fn node_type(&self) -> &'static str {
        "Document"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Document",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "Document", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::blob("media", "*/*"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let file_ref = match ctx.config.get("media").filter(|v| v.is_object()) {
            Some(v) => v,
            None => return NodeResult::failed("No document provided. Upload a file or paste a URL."),
        };

        let url = file_ref.get("url").and_then(|v| v.as_str()).unwrap_or("");
        let filename = file_ref.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let mime_type = file_ref.get("mime_type").and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| guess_mimetype(url));

        if url.is_empty() {
            return NodeResult::failed("No document provided. Upload a file or paste a URL.");
        }

        NodeResult::completed(serde_json::json!({
            "value": {
                "url": url,
                "mimeType": mime_type,
                "filename": filename,
            }
        }))
    }
}

register_node!(DocumentNode);

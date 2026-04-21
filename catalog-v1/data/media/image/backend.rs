//! Image Node - Image data input (png, jpg, jpeg, webp, gif, bmp, svg, tiff)
//!
//! Outputs a standardized media object that can be consumed by send nodes.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct ImageNode;

const IMAGE_EXTENSIONS: &[(&str, &str)] = &[
    ("png", "image/png"),
    ("jpg", "image/jpeg"),
    ("jpeg", "image/jpeg"),
    ("webp", "image/webp"),
    ("gif", "image/gif"),
    ("bmp", "image/bmp"),
    ("svg", "image/svg+xml"),
    ("tiff", "image/tiff"),
    ("tif", "image/tiff"),
    ("ico", "image/x-icon"),
    ("heic", "image/heic"),
    ("heif", "image/heif"),
    ("avif", "image/avif"),
];

fn guess_mimetype(url: &str) -> &'static str {
    let lower = url.to_lowercase();
    for (ext, mime) in IMAGE_EXTENSIONS {
        if lower.ends_with(&format!(".{}", ext)) {
            return mime;
        }
    }
    "image/png"
}

#[async_trait]
impl Node for ImageNode {
    fn node_type(&self) -> &'static str {
        "Image"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Image",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "Image", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::blob("media", "image/*"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let file_ref = match ctx.config.get("media").filter(|v| v.is_object()) {
            Some(v) => v,
            None => return NodeResult::failed("No image provided. Upload a file or paste a URL."),
        };

        let url = file_ref.get("url").and_then(|v| v.as_str()).unwrap_or("");
        let filename = file_ref.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let mime_type = file_ref.get("mime_type").and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| guess_mimetype(url));

        if url.is_empty() {
            return NodeResult::failed("No image provided. Upload a file or paste a URL.");
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

register_node!(ImageNode);

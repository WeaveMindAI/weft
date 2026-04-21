//! Video Node - Video data input (mp4, webm, mov, avi, mkv)
//!
//! Outputs a standardized media object that can be consumed by send nodes.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct VideoNode;

const VIDEO_EXTENSIONS: &[(&str, &str)] = &[
    ("mp4", "video/mp4"),
    ("webm", "video/webm"),
    ("mov", "video/quicktime"),
    ("avi", "video/x-msvideo"),
    ("mkv", "video/x-matroska"),
    ("wmv", "video/x-ms-wmv"),
    ("flv", "video/x-flv"),
    ("m4v", "video/x-m4v"),
    ("3gp", "video/3gpp"),
];

fn guess_mimetype(url: &str) -> &'static str {
    let lower = url.to_lowercase();
    for (ext, mime) in VIDEO_EXTENSIONS {
        if lower.ends_with(&format!(".{}", ext)) {
            return mime;
        }
    }
    "video/mp4"
}

#[async_trait]
impl Node for VideoNode {
    fn node_type(&self) -> &'static str {
        "Video"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Video",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "Video", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::blob("media", "video/*"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let file_ref = match ctx.config.get("media").filter(|v| v.is_object()) {
            Some(v) => v,
            None => return NodeResult::failed("No video provided. Upload a file or paste a URL."),
        };

        let url = file_ref.get("url").and_then(|v| v.as_str()).unwrap_or("");
        let filename = file_ref.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let mime_type = file_ref.get("mime_type").and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| guess_mimetype(url));

        if url.is_empty() {
            return NodeResult::failed("No video provided. Upload a file or paste a URL.");
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

register_node!(VideoNode);

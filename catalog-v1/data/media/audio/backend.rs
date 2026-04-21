//! Audio Node - Audio data input (mp3, ogg, wav, flac, m4a, aac, opus)
//!
//! Outputs a standardized media object that can be consumed by send nodes.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct AudioNode;

const AUDIO_EXTENSIONS: &[(&str, &str)] = &[
    ("mp3", "audio/mpeg"),
    ("ogg", "audio/ogg"),
    ("oga", "audio/ogg"),
    ("wav", "audio/wav"),
    ("flac", "audio/flac"),
    ("m4a", "audio/mp4"),
    ("aac", "audio/aac"),
    ("opus", "audio/opus"),
    ("wma", "audio/x-ms-wma"),
    ("webm", "audio/webm"),
];

fn guess_mimetype(url: &str) -> &'static str {
    let lower = url.to_lowercase();
    for (ext, mime) in AUDIO_EXTENSIONS {
        if lower.ends_with(&format!(".{}", ext)) {
            return mime;
        }
    }
    "audio/mpeg"
}

#[async_trait]
impl Node for AudioNode {
    fn node_type(&self) -> &'static str {
        "Audio"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Audio",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "Audio", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::blob("media", "audio/*"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let file_ref = match ctx.config.get("media").filter(|v| v.is_object()) {
            Some(v) => v,
            None => return NodeResult::failed("No audio file provided. Upload a file or paste a URL."),
        };

        let url = file_ref.get("url").and_then(|v| v.as_str()).unwrap_or("");
        let filename = file_ref.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let mime_type = file_ref.get("mime_type").and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| guess_mimetype(url));

        if url.is_empty() {
            return NodeResult::failed("No audio file provided. Upload a file or paste a URL.");
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

register_node!(AudioNode);

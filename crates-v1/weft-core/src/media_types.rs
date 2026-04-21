//! Concrete Rust structs for the rich media port types.
//!
//! These structs define the JSON shape contract for Image, Video, Audio, and Document.
//! Node authors deserialize port data into these types directly:
//!
//! ```ignore
//! let image: Image = serde_json::from_value(ctx.input["media"].clone())?;
//! ```
//!
//! For non-Rust runtimes (Python, Go), the JSON schema is the contract.

#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Image {
    pub url: String,
    pub mimeType: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Video {
    pub url: String,
    pub mimeType: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Audio {
    pub url: String,
    pub mimeType: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub url: String,
    pub mimeType: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

/// Infer media category ("image", "video", "audio", "document") from a MIME type.
pub fn media_category_from_mime(mime: &str) -> &'static str {
    if mime.starts_with("image/") { "image" }
    else if mime.starts_with("video/") { "video" }
    else if mime.starts_with("audio/") { "audio" }
    else { "document" }
}

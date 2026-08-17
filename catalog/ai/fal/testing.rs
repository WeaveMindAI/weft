//! Shared live-test plumbing for the fal package: a real sample image
//! minter. Several live tests need a genuine image as INPUT (animate,
//! edit, upscale); minting one through the cheapest real generation
//! keeps them fixture-free, exactly like the elevenlabs package mints
//! its own speech samples.
#![cfg(feature = "node-tests")]

use std::time::Duration;

use weft::access::client::{get_json, post_json};
use weft::access::OpenedConnection;
use weft::storage::media::{classify_media_slot, MediaSlotContent};
use weft::{NodeErrExt, WeftResult};

use super::fal::API;

/// A small REAL png (one square flux image on the fastest settings,
/// about a cent), through the same queue dance the nodes ride. Bounded
/// polling: a sample that takes minutes is a failure, not a wait.
pub async fn sample_image(conn: &OpenedConnection) -> WeftResult<(String, Vec<u8>)> {
    let http = conn.client();
    let submitted = post_json(
        http,
        &format!("{API}/fal-ai/flux/dev"),
        &serde_json::json!({
            "prompt": "a single blue square on a white background",
            "image_size": "square",
            "num_images": 1,
            "num_inference_steps": 4,
        }),
        "fal: mint the sample image",
    )
    .await?;
    let request_id =
        submitted["request_id"].as_str().node_err("fal: the submit answered no request_id")?;

    let status_url = format!("{API}/fal-ai/flux/requests/{request_id}/status");
    let mut polls = 0;
    loop {
        let status = get_json(http, &status_url, "fal: read the sample's status").await?;
        match status["status"].as_str().unwrap_or_default() {
            "COMPLETED" => break,
            "IN_QUEUE" | "IN_PROGRESS" => {
                polls += 1;
                if polls > 60 {
                    weft::node_bail!("fal: the sample image is still not done after 60 polls");
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            other => weft::node_bail!("fal answered an unexpected status '{other}' for the sample"),
        }
    }

    let answer = get_json(
        http,
        &format!("{API}/fal-ai/flux/requests/{request_id}"),
        "fal: read the sample image",
    )
    .await?;
    let url = answer["images"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|i| i["url"].as_str())
        .node_err("fal answered no image for the sample")?;
    let slot = match classify_media_slot(&serde_json::Value::String(url.to_string())) {
        Ok(slot) => slot,
        Err(e) => weft::node_bail!("fal answered an unreadable image slot: {e}"),
    };
    match slot {
        MediaSlotContent::DataUrl { bytes, mime } => Ok((mime, bytes)),
        MediaSlotContent::ExternalUrl(url) => {
            let resp = http
                .get(&url)
                .send()
                .await
                .node_err("fal: download the sample image")?;
            let mime = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/png")
                .to_string();
            let bytes =
                resp.bytes().await.node_err("fal: read the sample image bytes")?.to_vec();
            Ok((mime, bytes))
        }
        MediaSlotContent::Stored { .. } => {
            weft::node_bail!("fal answered a stored-file marker, which it never mints")
        }
    }
}

//! Layer-4: a file input reaches ExecPython as a plain dict whose `url`
//! is a download link minted for the firing, and fetching it inside the
//! snippet works. The `python_file` fixture posts a picture to a route,
//! the snippet downloads it through the link and answers with what it
//! got, and hands the picture back on a file port.
#![cfg(feature = "e2e")]

use base64::Engine;
use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{ensure, live, run, project::Project};

/// A 1x1 PNG.
const PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x60, 0x00, 0x02, 0x00,
    0x00, 0x05, 0x00, 0x01, 0xE2, 0x26, 0x05, 0x9B, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44,
    0xAE, 0x42, 0x60, 0x82,
];

#[tokio::test]
async fn a_python_snippet_reads_a_file_through_its_minted_link() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("python_file", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = run::execution_colors(&disp, &project.id()).await?;

    let data_url = format!("data:image/png;base64,{}", base64::engine::general_purpose::STANDARD.encode(PNG));
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/look"), &[], &json!({ "photo": data_url })).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v["mimeType"], json!("image/png"));
    assert_eq!(v["fetched"], json!(PNG.len()), "the snippet downloaded the picture through the link: {v}");
    assert_eq!(v["declared"], json!(PNG.len()));

    // The picture went back out on the file port as a stored-file marker
    // with no link on it: the journal never carries one.
    let colors = run::wait_for_triggered_executions(&disp, &project.id(), &before, 1, std::time::Duration::from_secs(60)).await?;
    let settled = project.settled(colors[0]).await?;
    settled.completed()?;
    let same = settled.completed_outputs()["inspect"]["same"].clone();
    anyhow::ensure!(same.get("__weft_image__").is_some(), "the marker is wrapped again on the way out: {same}");
    anyhow::ensure!(same["__weft_image__"].get("url").is_none(), "no link leaves the node: {same}");
    project.finish().await
}

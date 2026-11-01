//! Stored files served to a page, and a page's upload through a slot.
//!
//! - A route answering a stored file hands the caller a link straight to
//!   the bytes (through the install's files door on a local install): the
//!   same link for every call within its window, a download under the
//!   file's name, and any `Range` of it answered `206`.
//! - The link `weft files download` is handed answers a `Range` too, which
//!   is what lets a cut download carry on from where it stopped.
//! - A page uploads a file through a slot a route handed it, with the four
//!   steps `uploadFile` takes (begin, parts PUT straight to their links,
//!   part-done, complete) through the install's `/files/upload/...` door,
//!   and the route it calls next with the slot receives the file, once.
#![cfg(feature = "e2e")]

use anyhow::Context;
use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{ensure, live, project::Project};

/// The fixture's stored file.
const CLIP: &[u8] = b"0123456789abcdefghij";

/// The `X-Amz-Date` a link was signed at.
fn signed_at(url: &str) -> Option<String> {
    url::Url::parse(url).ok()?.query_pairs().find(|(k, _)| k == "X-Amz-Date").map(|(_, v)| v.into_owned())
}

#[tokio::test]
async fn a_stored_file_is_served_by_link_and_a_page_uploads_through_a_slot() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("stored_links", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let http = weft_core::net::http_client().build()?;

    // The same file answered twice: the same link, unless the two calls
    // fell in two windows (a new signing moment, then a new link).
    let answer = || async {
        let (status, _, body) = live::http_request(&disp, Method::GET, &format!("{base}/file"), &[], None).await?;
        anyhow::ensure!(status == 200, "GET file answered {status}: {}", String::from_utf8_lossy(&body));
        Ok::<Value, anyhow::Error>(serde_json::from_slice(&body)?)
    };
    let (first, second) = (answer().await?, answer().await?);
    let url = first["url"].as_str().context("the answer carries the file's url")?.to_string();
    // An asset is named by its path in the project.
    assert_eq!((first["filename"].as_str(), first["sizeBytes"].as_u64()), (Some("assets/clip.txt"), Some(CLIP.len() as u64)));
    let again = second["url"].as_str().context("the second answer carries a url")?;
    if signed_at(&url) == signed_at(again) {
        assert_eq!(url, again, "the same file, the same window, the same link");
    } else {
        eprintln!("[e2e] the two answers fell in two signing windows; the same-link check is skipped this time");
    }

    // Any part of it, straight from the store: what a video player seeking
    // asks for.
    let part = http.get(&url).header("range", "bytes=2-5").send().await?;
    assert_eq!(part.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let headers = part.headers().clone();
    assert_eq!(headers.get("content-range").and_then(|v| v.to_str().ok()), Some("bytes 2-5/20"));
    let disposition = headers.get("content-disposition").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
    assert!(disposition.starts_with("attachment;") && disposition.contains("clip.txt"), "{disposition}");
    assert_eq!(part.bytes().await?.as_ref(), &CLIP[2..6]);
    // A local install's outside link goes through the files door.
    anyhow::ensure!(url.contains("/files/runtime/"), "a local install's outside link goes through the files door: {url}");
    assert_eq!(headers.get("x-content-type-options").and_then(|v| v.to_str().ok()), Some("nosniff"), "the files door says so");

    // The link a download is handed answers a range too, so a cut download
    // carries on where it stopped.
    let key = key_of(&url)?;
    let handed: weft_core::storage::PresignResult =
        disp.post_json("/storage/files/download", &json!({ "key": key })).await?;
    let rest = http.get(&handed.url).header("range", "bytes=10-").send().await?;
    assert_eq!(rest.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(rest.bytes().await?.as_ref(), &CLIP[10..]);

    // A page's upload through a slot.
    let (status, _, body) = live::http_request(&disp, Method::GET, &format!("{base}/slot"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let slot: String = serde_json::from_slice(&body)?;
    let doc = b"a visitor's notes, sent in parts".to_vec();
    upload(&http, disp.base(), &slot, "notes.txt", &doc).await?;
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/receive"), &[], &json!({ "doc": slot })).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(serde_json::from_slice::<String>(&body)?.as_bytes(), doc.as_slice());
    // Received once: another run asking for it is refused, and says why.
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/receive"), &[], &json!({ "doc": slot })).await?;
    let said = String::from_utf8_lossy(&body);
    assert_eq!(status, 400, "{said}");
    assert!(said.contains("cannot be received"), "the refusal names the upload: {said}");

    project.finish().await
}

/// The storage key a stored file's link names, tenant included: the link
/// goes to `.../runtime/<key>?...`.
fn key_of(url: &str) -> anyhow::Result<String> {
    let path = url::Url::parse(url)?.path().to_string();
    path.split_once("/runtime/").map(|(_, key)| key.to_string()).context("a link to a runtime file")
}

/// The four steps of an upload through `slot`, as a page's `uploadFile`
/// takes them, through the install's `/files/upload/...` door at `base`.
async fn upload(http: &reqwest::Client, base: &str, slot: &str, filename: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let verb = |verb: &str| format!("{}/files/upload/{verb}", base.trim_end_matches('/'));
    let call = |verb_name: &'static str, body: Value| {
        let url = verb(verb_name);
        async move {
            let resp = http.post(&url).json(&body).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            anyhow::ensure!(status.is_success(), "{verb_name} answered {status}: {text}");
            Ok::<String, anyhow::Error>(text)
        }
    };
    let begun: Value = serde_json::from_str(
        &call("begin", json!({ "slot": slot, "filename": filename, "mimeType": "text/plain", "sizeBytes": bytes.len() })).await?,
    )?;
    let key = begun["key"].as_str().context("begin names the upload")?.to_string();
    let part_size = begun["partSize"].as_u64().context("begin names the part size")?;
    let parts: Vec<Value> = (0..bytes.len().div_ceil(part_size as usize))
        .map(|i| json!({ "partNumber": i + 1, "sizeBytes": (bytes.len() - i * part_size as usize).min(part_size as usize) }))
        .collect();
    let reserved: Value = serde_json::from_str(&call("parts", json!({ "slot": slot, "key": key, "parts": parts })).await?)?;
    for part in reserved["parts"].as_array().context("parts are answered")? {
        let offset = part["offsetBytes"].as_u64().context("an offset")? as usize;
        let size = part["sizeBytes"].as_u64().context("a size")? as usize;
        let put = http.put(part["url"].as_str().context("a link")?).body(bytes[offset..offset + size].to_vec()).send().await?;
        if !put.status().is_success() {
            let status = put.status();
            anyhow::bail!("a part PUT answered {status}: {}", put.text().await.context("read the refused part PUT's answer")?);
        }
        let etag = put.headers().get("etag").and_then(|v| v.to_str().ok()).context("a part answers its etag")?.to_string();
        call("part-done", json!({ "slot": slot, "key": key, "partNumber": part["partNumber"], "etag": etag })).await?;
    }
    call("complete", json!({ "slot": slot, "key": key })).await?;
    Ok(())
}

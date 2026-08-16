//! Shared fal plumbing: the queue dance (submit, wait on the status,
//! fetch the result) and the media-in/media-out conversions every fal
//! node repeats.
//!
//! fal serves every model through one queue API: POST the payload to
//! the model's route, poll the request's status until COMPLETED, then
//! read the response. A generation the user asked for takes as long
//! as it takes (no deadline here; cancelling the execution cancels
//! the wait).

use std::time::Duration;

use serde_json::Value;

use weft::access::client::{get_json, post_json};
use weft::context::LogLevel;
use weft::storage::{FileHandle, StorageScope};
use weft::{ExecutionContext, NodeErrExt, WeftResult};

pub const API: &str = "https://queue.fal.run";

/// A model route is user-supplied text that becomes part of a URL:
/// allow only the id shapes fal actually mints (at least the
/// `owner/name` app pair, clean segments) so nothing can traverse out
/// of the queue API and every model has an app to poll under.
pub fn checked_model(model: &str) -> WeftResult<&str> {
    let clean = |seg: &str| {
        !seg.is_empty()
            && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
            && !seg.starts_with('.')
    };
    let mut segs = model.split('/');
    let ok = segs.next().is_some_and(clean) && segs.next().is_some_and(clean) && segs.all(clean);
    if !ok {
        weft::node_bail!("'{model}' is not a fal model id (expected e.g. fal-ai/flux/dev)");
    }
    Ok(model)
}

/// Submit `payload` to `model`'s queue, wait on the request, and hand
/// back the completed response. `what` names the attempt in errors
/// and log breadcrumbs.
pub async fn run_queued(
    ctx: &ExecutionContext,
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    model: &str,
    payload: &Value,
    what: &str,
) -> WeftResult<Value> {
    let model = checked_model(model)?;
    let submitted =
        post_json(http, &format!("{API}/{model}"), payload, what).await?;
    let request_id = submitted["request_id"]
        .as_str()
        .node_err("fal: the submit answered no request_id")?
        .to_string();

    // The queue's request routes address the APP (`owner/name`), never a
    // variant subpath: a `fal-ai/flux/dev` submit is polled at
    // `fal-ai/flux/requests/...` (the full path answers 405).
    let app: String =
        model.split('/').take(2).collect::<Vec<_>>().join("/");
    let status_url = format!("{API}/{app}/requests/{request_id}/status");
    let mut polls: u64 = 0;
    loop {
        let status = get_json(http, &status_url, "fal: read the request status").await?;
        match status["status"].as_str().unwrap_or_default() {
            "COMPLETED" => break,
            "IN_QUEUE" | "IN_PROGRESS" => {
                polls += 1;
                if polls % 20 == 0 {
                    ctx.log(LogLevel::Info, format!("{what}: still running on {model}"))
                        .await?;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            other => {
                weft::node_bail!("fal answered an unexpected status '{other}' for {what}")
            }
        }
    }

    // A failed generation still COMPLETES (the queue has no failed
    // status); the failure rides on the result body as `error` /
    // `error_type`, so read the body before trusting it.
    let answer = get_json(http, &format!("{API}/{app}/requests/{request_id}"), what).await?;
    if let Some(error) = answer["error"].as_str() {
        let kind = answer["error_type"].as_str().unwrap_or("no type");
        weft::node_bail!("fal failed {what} on {model}: {error} ({kind})");
    }
    Ok(answer)
}

/// A stored file as something fal can read: its public link when the
/// install serves one, else an inline data: URL (fal accepts both).
pub async fn media_url(ctx: &ExecutionContext, file: &FileHandle) -> WeftResult<String> {
    let storage = ctx.storage(StorageScope::Execution);
    if let Some(url) = storage.public_link(file, None).await? {
        return Ok(url);
    }
    let (meta, bytes) = storage.get_bytes(file).await?;
    Ok(weft::storage::media::data_url(&meta.mime_type, &bytes))
}

/// The answered video's URL, wherever the model family put it
/// (`video.url` for most, a bare `video` string, or the first of a
/// `videos` list).
pub fn video_url(answer: &Value) -> Option<&str> {
    answer["video"]["url"]
        .as_str()
        .or_else(|| answer["video"].as_str())
        .or_else(|| {
            answer["videos"].as_array().and_then(|a| a.first()).and_then(|v| v["url"].as_str())
        })
}

/// Merge the node's raw `params` object (model-specific extras) onto
/// `payload`. The declared knobs win: an extra may add fields the
/// blessed model's knobs don't cover, never silently override one.
pub fn merge_params(payload: &mut Value, params: Option<&Value>) -> WeftResult<()> {
    let Some(params) = params else { return Ok(()) };
    let Some(extra) = params.as_object() else {
        weft::node_bail!("params must be an object of model parameters");
    };
    let base = payload.as_object_mut().expect("payloads are objects");
    for (k, v) in extra {
        if !base.contains_key(k) {
            base.insert(k.clone(), v.clone());
        }
    }
    Ok(())
}

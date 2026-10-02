//! Shared fal plumbing: the queue dance (submit, wait on the status,
//! fetch the result) and the answer-reading helpers every fal
//! node repeats.
//!
//! fal serves every model through one queue API: POST the payload to
//! the model's route, wait until the request's status says COMPLETED,
//! then read the response. The wait is a parked poll, so no worker is
//! held while fal generates, and there is no deadline: a generation the
//! user asked for takes as long as it takes, and cancelling the
//! execution cancels the wait.

use serde_json::{Map, Value};

use weft::access::client::{get_json, post_json};
use weft::signal::{PollEndpoint, Predicate};
use weft::{Access, ExecutionContext, NodeErrExt, WeftResult};

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
        return Err(weft::WeftError::Input(format!(
            "'{model}' is not a fal model id (expected e.g. fal-ai/flux/dev)"
        )));
    }
    Ok(model)
}

/// Submit `payload` to `model`'s queue, wait on the request, and hand
/// back the completed response. `what` names the attempt in errors.
pub async fn run_queued(
    ctx: &ExecutionContext,
    account: &Access,
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    model: &str,
    payload: &Value,
    what: &str,
) -> WeftResult<Value> {
    let model = checked_model(model)?;
    // The submit is the paid call, and the body replays from the top
    // when the wait resumes: journaled, the replay reads this request id
    // back instead of queueing a second generation.
    let submitted = ctx
        .run("fal_submit", || async {
            post_json(http, &format!("{API}/{model}"), payload, what).await
        })
        .await?;
    let request_id = submitted["request_id"]
        .as_str()
        .node_err("fal: the submit answered no request_id")?
        .to_string();

    // The queue's request routes address the APP (`owner/name`), never a
    // variant subpath: a `fal-ai/flux/dev` submit is polled at
    // `fal-ai/flux/requests/...` (the full path answers 405).
    let app: String =
        model.split('/').take(2).collect::<Vec<_>>().join("/");
    // Resume on any status that is not in flight, so a status this node
    // does not know ends the wait and is refused below instead of
    // being waited on forever.
    let status = ctx
        .await_signal(PollEndpoint {
            url: format!("{API}/{app}/requests/{request_id}/status"),
            interval_secs: 5,
            access: Some(weft::primitive::AccessRef::from(account)),
            filters: vec![
                Predicate::neq("status", "IN_QUEUE"),
                Predicate::neq("status", "IN_PROGRESS"),
            ],
            ..Default::default()
        })
        .await?;
    match status["status"].as_str().unwrap_or_default() {
        "COMPLETED" => {}
        other => weft::node_bail!("fal answered an unexpected status '{other}' for {what}"),
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
/// blessed model's knobs don't cover, never silently override one. An
/// extra whose value is `null` removes that key from the request, which
/// is how a caller drops a field the node sends for another model family's
/// sake.
pub fn merge_params(payload: &mut Value, params: Option<&Map<String, Value>>) {
    let Some(extra) = params else { return };
    let base = payload.as_object_mut().expect("payloads are objects");
    for (k, v) in extra {
        // A declared knob still wins over an extra of the same name, which
        // is the rule this function exists to hold. The one thing `params`
        // may do to a key the node filled in is TAKE IT OFF, by naming it
        // `null`: the node sends both spellings of the image argument
        // because different fal model families read different ones, and a
        // model that refuses the unknown one answered 422 with nothing the
        // person could do about it from the node's inputs.
        if v.is_null() {
            base.remove(k);
        } else if !base.contains_key(k) {
            base.insert(k.clone(), v.clone());
        }
    }
}

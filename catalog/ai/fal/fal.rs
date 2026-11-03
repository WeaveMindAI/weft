//! Shared fal plumbing: the queue dance (submit, wait on the status,
//! fetch the result) and the answer-reading helpers every fal
//! node repeats.
//!
//! fal serves every model through one queue API: POST the payload to
//! the model's route, wait until the request's status says COMPLETED,
//! then read the response. The wait is a parked poll, so no worker is
//! held while fal generates, and there is no deadline: a generation the
//! user asked for takes as long as it takes, and cancelling the
//! run cancels the wait and the request on fal.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use weft::access::client::{get_json, post_json, url};
use weft::signal::{CancelCall, CancelWith, PollEndpoint, Predicate};
use weft::{Access, Kept, NodeErrExt, WeftResult};

pub const API: &str = "https://queue.fal.run";

/// Where a queued request stands, the state of a node that runs one
/// ([`run_queued`]): nothing yet, submitted (its id), or out of the queue
/// (the status it left on). A run that pauses while fal generates comes
/// back with the id, so the paid submit is never sent twice.
#[derive(Default, Serialize, Deserialize)]
pub struct Queued {
    request_id: Option<String>,
    status: Option<Status>,
}

/// The status a request left the queue on.
#[derive(Clone, Serialize, Deserialize)]
struct Status {
    #[serde(default)]
    status: String,
}

/// Submit `payload` to `model`'s queue, wait on the request, and hand
/// back the completed response. `what` names the attempt in errors.
pub async fn run_queued(
    state: &mut Kept<Queued>,
    account: &Access,
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    model: &str,
    payload: &Value,
    what: &str,
) -> WeftResult<Value> {
    // The model is a route of the queue API, held to fal's id shape (at
    // least the `owner/name` app pair) by every fal node's `model` rule.
    let route: Vec<&str> = model.split('/').collect();
    let request_id = state
        .once(|s| &mut s.request_id, async |_| {
            let submitted = post_json(http, url(API, &route)?.as_str(), payload, what).await?;
            Ok(submitted["request_id"]
                .as_str()
                .node_err("fal: the submit answered no request_id")?
                .to_string())
        })
        .await?;

    // The queue's request routes address the APP (`owner/name`), never a
    // variant subpath: a `fal-ai/flux/dev` submit is polled at
    // `fal-ai/flux/requests/...` (the full path answers 405).
    let request = url(API, &route[..2])?.segment("requests")?.segment(&request_id)?;
    // Resume on any status that is not in flight, so a status this node
    // does not know ends the wait and is refused below instead of
    // being waited on forever. Cancelling the run cancels the request
    // too: fal drops a queued one, and asks the runner to stop one in
    // progress (https://fal.ai/docs/model-apis/model-endpoints/queue).
    let poll = PollEndpoint {
        url: request.clone().segment("status")?.into(),
        interval_secs: 5,
        access: Some(weft::primitive::AccessRef::from(account)),
        filters: vec![
            Predicate::neq("status", "IN_QUEUE"),
            Predicate::neq("status", "IN_PROGRESS"),
        ],
        ..Default::default()
    };
    let cancel = CancelCall::put(request.clone().segment("cancel")?);
    let status = state.wait(|s| &mut s.status, poll.cancel_with(cancel)).await?;
    match status.status.as_str() {
        "COMPLETED" => {}
        other => weft::node_bail!("fal answered an unexpected status '{other}' for {what}"),
    }

    // A failed generation still COMPLETES (the queue has no failed
    // status); the failure rides on the result body as `error` /
    // `error_type`, so read the body before trusting it.
    let answer = get_json(http, request.as_str(), what).await?;
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

/// The answered image's URL, wherever the model family put it (the first
/// of an `images` list for most, `image.url` for a single-image model).
pub fn image_url(answer: &Value) -> Option<&str> {
    answer["images"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|i| i["url"].as_str())
        .or_else(|| answer["image"]["url"].as_str())
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

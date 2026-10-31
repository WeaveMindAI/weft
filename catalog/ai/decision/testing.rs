//! Shared plumbing for the decision package's tests: the provider object
//! an OpenRouter Decision Model node would emit (on the fake connection,
//! or the live one), a canned Decisions API answer, and the request the
//! node sent.
#![cfg(feature = "node-tests")]

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig};

/// An OpenRouter `DecisionProvider` object on the rig's fake connection.
pub fn provider(rig: &FakeRig) -> Value {
    json!({ "kind": "openrouter", "model": "test/decider", "account": rig.access("openrouter") })
}

/// The cheapest decision model, for the live tests that need no other.
pub const CHEAP_MODEL: &str = "perplexity/pplx-decider-flash-v1.2";

/// An OpenRouter `DecisionProvider` object naming `model` on the live
/// rig's real connection.
pub fn live_provider(rig: &LiveRig, model: &str) -> Value {
    json!({ "kind": "openrouter", "model": model, "account": rig.access("openrouter") })
}

/// Answer the Decisions API route with `answer` as the one question's
/// answer, in the envelope OpenRouter wraps it in.
pub fn answer_decision(rig: &FakeRig, answer: Value) {
    rig.respond(
        "POST",
        "/api/v1/systemone",
        json!({
            "model": "test/decider-20261016",
            "answers": { "answer": answer },
            "usage": { "input_tokens": 120, "output_tokens": 1, "cost": 0.0000012 },
            "id": "gen-dec-1",
            "provider": "Test",
        }),
    );
}

/// The body of the one Decisions API call the node made.
pub fn decision_request(rig: &FakeRig) -> Value {
    let requests = rig.requests();
    assert_eq!(requests.len(), 1, "one call to the Decisions API");
    assert_eq!(requests[0].path, "/api/v1/systemone");
    requests[0].body.clone().expect("a JSON body")
}

/// A weft type, parsed.
pub fn ty(text: &str) -> weft::WeftType {
    weft::WeftType::parse(text).expect("a weft type")
}

//! LlmModerate self-tests: the moderation call on the wired OpenAI
//! connection, the model setting, and the flagged-category extraction.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::LlmModerateNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("flags_and_extracts_the_flagged_categories", flags),
        NodeTest::fake("the_model_setting_names_the_moderation_model", model_setting),
        NodeTest::fake("a_result_without_a_verdict_never_passes_as_safe", missing_verdict),
        NodeTest::live("one_real_moderation_verdict", "openai", live_moderation),
    ]
}

async fn live_moderation(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmModerateNode,
            json!({
                "access": rig.access("openai"),
                "text": "I brought flowers to my neighbor this morning.",
            }),
        )
        .await
        .ok()?;
    let flagged = outcome.output("flagged")?.as_bool().expect("a real verdict");
    assert!(!flagged, "benign text is not flagged");
    assert_eq!(outcome.output("categories")?, &json!([]));
    Ok(())
}

async fn flags(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/moderations",
        json!({ "results": [{
            "flagged": true,
            "categories": { "harassment": true, "violence": false },
            "category_scores": { "harassment": 0.98, "violence": 0.01 },
        }]}),
    );
    let outcome = rig
        .run(
            &LlmModerateNode,
            json!({ "access": rig.access("openai"), "text": "bad words" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["flagged"], json!(true));
    assert_eq!(outcome.outputs["categories"], json!(["harassment"]));
    assert_eq!(outcome.outputs["scores"]["harassment"], json!(0.98));
    Ok(())
}

async fn model_setting(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/moderations",
        json!({ "results": [{ "flagged": false, "categories": {}, "category_scores": {} }]}),
    );
    rig.run(
        &LlmModerateNode,
        json!({
            "access": rig.access("openai"),
            "model": "text-moderation-latest",
            "text": "hi",
        }),
    )
    .await
    .ok()?;
    let requests = rig.requests();
    assert_eq!(requests.len(), 1);
    let body = requests[0].body.clone().expect("json payload");
    assert_eq!(body["model"], json!("text-moderation-latest"));
    Ok(())
}

/// A canned result WITHOUT `flagged`: an unreadable verdict must fail
/// loud, never read as safe.
async fn missing_verdict(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/moderations",
        json!({ "results": [{ "categories": {}, "category_scores": {} }]}),
    );
    let outcome = rig
        .run(
            &LlmModerateNode,
            json!({ "access": rig.access("openai"), "text": "hi" }),
        )
        .await;
    let err = outcome.result.expect_err("a missing verdict must refuse").to_string();
    assert!(err.contains("flagged"), "{err}");
    Ok(())
}

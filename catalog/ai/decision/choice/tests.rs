//! DecisionChoice self-tests: the options as criteria, the pick and its
//! probabilities, and the loud refusals.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::testing::{answer_decision, decision_request, live_provider, provider, ty, CHEAP_MODEL};
use super::{options, DecisionChoiceNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("a_list_of_names_is_options_with_nothing_written", options_from_names),
        NodeTest::basic("fewer_than_two_or_repeated_options_are_refused", bad_options),
        NodeTest::fake("picks_one_option_with_each_probability", picks),
        NodeTest::fake("a_pick_outside_the_options_fails", pick_outside),
        NodeTest::fake("an_option_with_no_probability_fails", missing_probability),
        NodeTest::fake("a_probability_for_no_option_fails", stray_probability),
        NodeTest::fake("an_answer_with_no_choice_fails", no_choice),
        NodeTest::fake("a_list_of_names_is_sent_with_nothing_written", names_sent),
        NodeTest::live("one_real_choice", "openrouter", live_choice),
    ]
}

fn options_from_names() -> WeftResult<()> {
    let written = options(json!(["red", "green"]))?;
    assert_eq!(serde_json::Value::Object(written), json!({ "red": null, "green": null }));
    let described = options(json!({ "red": "Warm.", "green": "Calm." }))?;
    assert_eq!(serde_json::Value::Object(described), json!({ "red": "Warm.", "green": "Calm." }));
    Ok(())
}

fn bad_options() -> WeftResult<()> {
    let one = options(json!(["only"])).expect_err("one option").to_string();
    assert!(one.contains("at least two"), "{one}");
    let twice = options(json!(["a", "a"])).expect_err("a repeat").to_string();
    assert!(twice.contains("`a` twice"), "{twice}");
    let number = options(json!(["a", 2])).expect_err("a number").to_string();
    assert!(number.contains("named by a string"), "{number}");
    let lone = options(json!({ "only": "x" })).expect_err("one described option").to_string();
    assert!(lone.contains("at least two"), "{lone}");
    let neither = options(json!(3)).expect_err("neither a list nor an object").to_string();
    assert!(neither.contains("takes a list of names or an object"), "{neither}");
    Ok(())
}

fn payments() -> serde_json::Value {
    json!({
        "type": "choice",
        "choice": "payments",
        "probabilities": { "account": 0.0014, "frontend": 0.0445, "payments": 0.9541 },
        "confidence": 0.93,
    })
}

async fn picks(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, payments());
    rig.input_type("ticket", ty("String"));
    let outcome = rig
        .run(
            &DecisionChoiceNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Which team should own this ticket?",
                "options": { "account": "Login issues.", "frontend": "Rendering issues.", "payments": "Checkout issues." },
                "ticket": "Blank screen after I click Pay.",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["choice"], json!("payments"));
    assert_eq!(outcome.outputs["probabilities"]["frontend"], json!(0.0445));
    assert_eq!(outcome.outputs["confidence"], json!(0.93));
    let body = decision_request(&rig);
    assert_eq!(body["questions"]["answer"]["type"], json!("choice"));
    assert_eq!(
        body["questions"]["answer"]["criteria"],
        json!({ "account": "Login issues.", "frontend": "Rendering issues.", "payments": "Checkout issues." })
    );
    assert_eq!(body["state"], json!({ "ticket": "Blank screen after I click Pay." }));
    Ok(())
}

async fn pick_outside(rig: FakeRig) -> WeftResult<()> {
    let mut answer = payments();
    answer["choice"] = json!("legal");
    answer_decision(&rig, answer);
    rig.input_type("ticket", ty("String"));
    let err = rig
        .run(
            &DecisionChoiceNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Which team?",
                "options": ["account", "frontend", "payments"],
                "ticket": "x",
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("chose `legal`, which is not one of the options"), "{err}");
    Ok(())
}

async fn missing_probability(rig: FakeRig) -> WeftResult<()> {
    let mut answer = payments();
    answer["probabilities"].as_object_mut().expect("an object").remove("account");
    answer_decision(&rig, answer);
    rig.input_type("ticket", ty("String"));
    let err = rig
        .run(
            &DecisionChoiceNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Which team?",
                "options": ["account", "frontend", "payments"],
                "ticket": "x",
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("no probability for `account`"), "{err}");
    Ok(())
}

async fn choose(rig: &FakeRig, options: serde_json::Value) -> WeftResult<String> {
    rig.input_type("ticket", ty("String"));
    rig.run(
        &DecisionChoiceNode,
        json!({ "provider": provider(rig), "instructions": "Which team?", "options": options, "ticket": "x" }),
    )
    .await
    .failure()
}

async fn stray_probability(rig: FakeRig) -> WeftResult<()> {
    let mut answer = payments();
    answer["probabilities"]["billing"] = json!(0.01);
    answer_decision(&rig, answer);
    let err = choose(&rig, json!(["account", "frontend", "payments"])).await?;
    assert!(err.contains("`billing`, which is not one of the options"), "{err}");
    Ok(())
}

async fn no_choice(rig: FakeRig) -> WeftResult<()> {
    let mut answer = payments();
    answer.as_object_mut().expect("an object").remove("choice");
    answer_decision(&rig, answer);
    let err = choose(&rig, json!(["account", "frontend", "payments"])).await?;
    assert!(err.contains("carries no `choice`"), "{err}");
    Ok(())
}

async fn names_sent(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, payments());
    rig.input_type("ticket", ty("String"));
    rig.run(
        &DecisionChoiceNode,
        json!({
            "provider": provider(&rig),
            "instructions": "Which team?",
            "options": ["account", "frontend", "payments"],
            "ticket": "x",
        }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(body["questions"]["answer"]["criteria"], json!({ "account": null, "frontend": null, "payments": null }));
    Ok(())
}

async fn live_choice(rig: LiveRig) -> WeftResult<()> {
    rig.input_type("ticket", ty("String"));
    let outcome = rig
        .run(
            &DecisionChoiceNode,
            json!({
                "provider": live_provider(&rig, CHEAP_MODEL),
                "instructions": "Which team should own this ticket?",
                "options": {
                    "account": "Login, permissions, or profile issues.",
                    "frontend": "Rendering, layout, or browser compatibility issues.",
                    "payments": "Checkout, billing, or payment processing issues.",
                },
                "ticket": "My checkout page shows a blank screen after I click Pay.",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["choice"], json!("payments"), "{:?}", outcome.outputs);
    Ok(())
}

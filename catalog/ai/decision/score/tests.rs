//! DecisionScore self-tests: the levels as criteria, the probabilities
//! in level order, the most likely level, and the loud refusals.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::testing::{answer_decision, decision_request, live_provider, provider, ty, CHEAP_MODEL};
use super::{level_probabilities, most_likely, DecisionScoreNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("the_most_likely_level_takes_the_lower_on_a_tie", most_likely_level),
        NodeTest::basic("probabilities_come_in_level_order", in_level_order),
        NodeTest::fake("scores_on_the_written_levels", scores),
        NodeTest::fake("fewer_than_two_levels_are_refused", one_level),
        NodeTest::fake("a_level_with_no_probability_fails", level_missing),
        NodeTest::fake("a_score_past_the_levels_fails", score_past_levels),
        NodeTest::live("one_real_score", "openrouter", live_score),
    ]
}

fn most_likely_level() -> WeftResult<()> {
    assert_eq!(most_likely(&[0.1, 0.7, 0.2]), 1);
    assert_eq!(most_likely(&[0.4, 0.2, 0.4]), 0, "a tie goes to the lower level");
    Ok(())
}

fn in_level_order() -> WeftResult<()> {
    let answered = json!({ "2": 0.9, "0": 0.0, "1": 0.1 });
    let answered = answered.as_object().expect("an object");
    assert_eq!(level_probabilities(answered, 3)?, vec![0.0, 0.1, 0.9]);
    let err = level_probabilities(answered, 4).expect_err("level 3 is missing").to_string();
    assert!(err.contains("no probability for level 3"), "{err}");
    Ok(())
}

async fn scores(rig: FakeRig) -> WeftResult<()> {
    answer_decision(
        &rig,
        json!({
            "type": "score",
            "score": 1.8957,
            "legend": { "0": "Can wait", "1": "This week", "2": "Blocking revenue now" },
            "probabilities": { "0": 0.0104, "1": 0.0834, "2": 0.9061 },
            "confidence": 0.8957,
        }),
    );
    rig.input_type("ticket", ty("String"));
    let outcome = rig
        .run(
            &DecisionScoreNode,
            json!({
                "provider": provider(&rig),
                "instructions": "How urgent is this ticket?",
                "levels": ["Can wait", "This week", "Blocking revenue now"],
                "ticket": "Checkout is down.",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["score"], json!(1.8957));
    assert_eq!(outcome.outputs["level"], json!("Blocking revenue now"));
    assert_eq!(outcome.outputs["probabilities"], json!([0.0104, 0.0834, 0.9061]));
    assert_eq!(outcome.outputs["confidence"], json!(0.8957));
    let body = decision_request(&rig);
    assert_eq!(
        body["questions"]["answer"],
        json!({
            "type": "score",
            "instructions": "How urgent is this ticket?",
            "criteria": ["Can wait", "This week", "Blocking revenue now"],
        })
    );
    Ok(())
}

async fn one_level(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("ticket", ty("String"));
    let err = rig
        .run(
            &DecisionScoreNode,
            json!({ "provider": provider(&rig), "instructions": "Urgent?", "levels": ["Yes"], "ticket": "x" }),
        )
        .await
        .failure()?;
    assert!(err.contains("at least two"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

fn urgency(score: f64, probabilities: serde_json::Value) -> serde_json::Value {
    json!({ "type": "score", "score": score, "probabilities": probabilities, "confidence": 0.9 })
}

async fn level_missing(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, urgency(1.0, json!({ "0": 0.1, "1": 0.9 })));
    rig.input_type("ticket", ty("String"));
    let err = rig
        .run(
            &DecisionScoreNode,
            json!({ "provider": provider(&rig), "instructions": "Urgent?", "levels": ["Low", "Mid", "High"], "ticket": "x" }),
        )
        .await
        .failure()?;
    assert!(err.contains("no probability for level 2"), "{err}");
    Ok(())
}

async fn score_past_levels(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, urgency(2.5, json!({ "0": 0.1, "1": 0.2, "2": 0.7 })));
    rig.input_type("ticket", ty("String"));
    let err = rig
        .run(
            &DecisionScoreNode,
            json!({ "provider": provider(&rig), "instructions": "Urgent?", "levels": ["Low", "Mid", "High"], "ticket": "x" }),
        )
        .await
        .failure()?;
    assert!(err.contains("a score of 2.5, outside the levels' 0 to 2"), "{err}");
    Ok(())
}

async fn live_score(rig: LiveRig) -> WeftResult<()> {
    rig.input_type("ticket", ty("String"));
    let outcome = rig
        .run(
            &DecisionScoreNode,
            json!({
                "provider": live_provider(&rig, CHEAP_MODEL),
                "instructions": "How urgent is this ticket?",
                "levels": ["Can wait for the next release", "Should be fixed this week", "Blocking revenue right now"],
                "ticket": "Nobody can pay: the checkout page is blank for every customer since this morning.",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["level"], json!("Blocking revenue right now"), "{:?}", outcome.outputs);
    Ok(())
}

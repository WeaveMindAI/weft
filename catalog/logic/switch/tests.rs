//! Switch self-tests: one branch speaks, the rest close, and every test
//! shape decides on its own terms.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SwitchNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_matching_case_takes_its_branch", matching_case),
        NodeTest::fake("no_match_falls_to_the_catch_all", catch_all),
        NodeTest::fake("no_match_and_no_catch_all_takes_nothing", nothing_matches),
        NodeTest::fake("the_first_matching_case_wins", first_match_wins),
        NodeTest::fake("a_range_takes_the_values_between_its_ends", a_range),
        NodeTest::fake("each_test_shape_decides_on_its_own_terms", every_shape),
        NodeTest::fake("every_metadata_kind_is_one_the_node_reads", metadata_kinds_covered),
    ]
}

/// The metadata's spec list and `Test::read`'s kind match are the same
/// list in two files (the JSON side cannot carry the SYNC marker), so
/// this walks the manifest and reads a minimal entry of every declared
/// kind: a spec added to metadata.json without a `read` arm fails HERE,
/// when the metadata is edited, instead of mid-execution on the first
/// firing that takes the new branch.
async fn metadata_kinds_covered(rig: FakeRig) -> WeftResult<()> {
    use weft::NodeManifest;
    let ports = SwitchNode
        .manifest()
        .ports_from_config
        .as_ref()
        .expect("Switch derives its ports from config");
    for spec in &ports.specs {
        // A minimal well-shaped entry: the kind, its port, and one
        // value per field the spec declares, shaped the way that
        // field's kind reads it.
        let mut entry = serde_json::Map::new();
        entry.insert("kind".into(), json!(spec.kind));
        entry.insert(spec.key_field.clone(), json!("taken"));
        for field in &spec.fields {
            let value = match (spec.kind.as_str(), field.key.as_str()) {
                ("in", "value") => json!([1]),
                ("matches", "value") => json!("x"),
                (_, "min") => json!(0),
                (_, "max") => json!(2),
                _ => json!(1),
            };
            entry.insert(field.key.clone(), value);
        }
        let cases = json!([serde_json::Value::Object(entry)]);
        // A run that reaches the branch decision without an
        // unknown-kind error is the whole assertion; whether the case
        // MATCHES 1 is each kind's own business.
        rig.run(&SwitchNode, json!({ "value": 1, "cases": cases })).await.ok()?;
    }
    Ok(())
}

/// The three cases used by the first tests: two matches and a catch-all,
/// in the order a program would write them.
fn cases() -> serde_json::Value {
    json!([
        { "kind": "equals", "value": "high", "port": "needsAPerson" },
        { "kind": "equals", "value": "low", "port": "trivial" },
        { "kind": "otherwise", "port": "goAhead" }
    ])
}

async fn matching_case(rig: FakeRig) -> WeftResult<()> {
    let outcome =
        rig.run(&SwitchNode, json!({ "value": "high", "cases": cases() })).await.ok()?;
    assert_eq!(outcome.outputs["needsAPerson"], json!(true));
    assert!(
        !outcome.outputs.contains_key("trivial") && !outcome.outputs.contains_key("goAhead"),
        "only the taken branch emits: {:?}",
        outcome.outputs
    );
    Ok(())
}

async fn catch_all(rig: FakeRig) -> WeftResult<()> {
    let outcome =
        rig.run(&SwitchNode, json!({ "value": "medium", "cases": cases() })).await.ok()?;
    assert_eq!(outcome.outputs["goAhead"], json!(true));
    assert!(!outcome.outputs.contains_key("needsAPerson"));
    Ok(())
}

async fn nothing_matches(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SwitchNode,
            json!({
                "value": "medium",
                "cases": [{ "kind": "equals", "value": "high", "port": "needsAPerson" }]
            }),
        )
        .await
        .ok()?;
    assert!(
        outcome.outputs.is_empty(),
        "no case matched, so no branch may be taken: {:?}",
        outcome.outputs
    );
    Ok(())
}

/// Order is the matching order, so a value two cases could match takes
/// the one written first. That is what makes the catch-all's position
/// meaningful rather than a special word.
async fn first_match_wins(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SwitchNode,
            json!({
                "value": "high",
                "cases": [
                    { "kind": "equals", "value": "high", "port": "first" },
                    { "kind": "equals", "value": "high", "port": "second" }
                ]
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["first"], json!(true));
    assert!(!outcome.outputs.contains_key("second"));
    Ok(())
}

/// A range takes both its ends, so a value inside it takes the branch
/// and a value past either end does not.
async fn a_range(rig: FakeRig) -> WeftResult<()> {
    let cases = json!([
        { "kind": "between", "min": 1, "max": 5, "port": "small" },
        { "kind": "otherwise", "port": "other" }
    ]);
    for (value, expected) in [(3, "small"), (1, "small"), (5, "small"), (0, "other"), (6, "other")] {
        let outcome = rig
            .run(&SwitchNode, json!({ "value": value, "cases": cases.clone() }))
            .await
            .ok()?;
        assert_eq!(
            outcome.outputs[expected],
            json!(true),
            "{value} belongs in the {expected} branch: {:?}",
            outcome.outputs
        );
    }
    Ok(())
}

/// One case per test kind, each with a value that passes it and one
/// that does not. A test handed a value it cannot apply to (a number
/// against `matches`) simply does not match.
async fn every_shape(rig: FakeRig) -> WeftResult<()> {
    let hits = [
        // Numbers match by VALUE: an integer case matches the float the
        // wire (or an arithmetic node) delivers.
        (json!({ "kind": "equals", "value": 5 }), json!(5.0), json!(6)),
        (json!({ "kind": "in", "value": ["a", "b"] }), json!("b"), json!("c")),
        (json!({ "kind": "in", "value": [5] }), json!(5.0), json!(4)),
        // Large integers (a Telegram/Discord snowflake) compare
        // exactly: two ids adjacent within f64 rounding must not match.
        (
            json!({ "kind": "equals", "value": 7351442619842213456i64 }),
            json!(7351442619842213456i64),
            json!(7351442619842213457i64),
        ),
        (json!({ "kind": "contains", "value": "err" }), json!("an error"), json!("fine")),
        (json!({ "kind": "contains", "value": 2 }), json!([1, 2]), json!([1, 3])),
        (json!({ "kind": "matches", "value": "^we[fF]t$" }), json!("weft"), json!("weaving")),
        (json!({ "kind": "gt", "value": 10 }), json!(11), json!(10)),
        (json!({ "kind": "gte", "value": 10 }), json!(10), json!(9)),
        (json!({ "kind": "lt", "value": 10 }), json!(9), json!("nine")),
        (json!({ "kind": "lte", "value": 10 }), json!(10), json!(11)),
    ];
    for (test, hit, miss) in hits {
        let mut case = test.as_object().unwrap().clone();
        case.insert("port".into(), json!("taken"));
        let cases = json!([case, { "kind": "otherwise", "port": "fell_through" }]);

        let outcome =
            rig.run(&SwitchNode, json!({ "value": hit, "cases": cases.clone() })).await.ok()?;
        assert_eq!(outcome.outputs["taken"], json!(true), "{test} should match {hit}");

        let outcome =
            rig.run(&SwitchNode, json!({ "value": miss, "cases": cases })).await.ok()?;
        assert_eq!(
            outcome.outputs["fell_through"],
            json!(true),
            "{test} should not match {miss}"
        );
    }
    Ok(())
}

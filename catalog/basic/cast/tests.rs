//! Cast self-tests: the output's type drives the conversion (the rig
//! plays the program pinning the output's variable via
//! `rig.output_type`).

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::CastNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("text_parses_into_the_declared_number", text_to_number),
        NodeTest::fake("a_scalar_stringifies_into_declared_text", scalar_to_text),
        NodeTest::fake("a_structure_becomes_its_json_text", structure_to_text),
        NodeTest::fake("an_impossible_value_fails_loud", impossible_value),
        NodeTest::fake("a_stored_file_marker_casts_into_its_kind_and_no_other", file_marker),
        NodeTest::fake("an_object_casts_into_a_named_type_through_its_body", named_type),
    ]
}

/// A picture kept in a database comes back as a plain object holding
/// its marker: cast into `Image` it flows as the file it is; into the
/// wrong kind, or when it is no marker, the refusal says which.
async fn file_marker(rig: FakeRig) -> WeftResult<()> {
    let image = rig.store_file("cat.png", "image/png", vec![1, 2, 3]);
    rig.output_type("value", WeftType::parse("Image").expect("Image parses"));
    let outcome = rig.run(&CastNode, json!({ "value": image })).await.ok()?;
    assert_eq!(outcome.outputs["value"], image, "the marker flows unchanged");
    rig.output_type("value", WeftType::parse("File").expect("File parses"));
    let outcome = rig.run(&CastNode, json!({ "value": image })).await.ok()?;
    assert_eq!(outcome.outputs["value"], image, "File takes any kind");

    let blob = rig.store_file("notes.pdf", "application/pdf", vec![1]);
    rig.output_type("value", WeftType::parse("Image").expect("Image parses"));
    let err = rig.run(&CastNode, json!({ "value": blob })).await.result.expect_err("a blob is not an image").to_string();
    assert!(err.contains("a stored Blob, not Image"), "{err}");
    let err = rig.run(&CastNode, json!({ "value": { "photo": "cat.png" } })).await.result.expect_err("no marker").to_string();
    assert!(err.contains("__weft_image__") && err.contains("is not one"), "{err}");
    Ok(())
}

/// A type a project declares (`Profile`, from a node's `types`) is a
/// name over a record: an object cast into it is held to that record,
/// its optional field left absent and its text parsed where the record
/// wants a number, and one that does not fit is refused naming the type
/// and the field.
async fn named_type(rig: FakeRig) -> WeftResult<()> {
    let profile = WeftType::Named {
        name: "Profile".into(),
        body: Box::new(WeftType::parse("{ name: String, age: Number, nickname?: String }").expect("the record parses")),
    };
    rig.output_type("value", profile);
    let outcome = rig.run(&CastNode, json!({ "value": { "name": "Ada", "age": "36" } })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!({ "name": "Ada", "age": 36.0 }));
    let err = rig.run(&CastNode, json!({ "value": { "age": 36 } })).await.failure()?;
    assert!(err.contains("not a valid Profile") && err.contains("'name'"), "{err}");
    Ok(())
}

async fn text_to_number(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("Number").expect("Number parses"));
    let outcome = rig.run(&CastNode, json!({ "value": "42.5" })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!(42.5));
    Ok(())
}

async fn scalar_to_text(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("String").expect("String parses"));
    let outcome = rig.run(&CastNode, json!({ "value": 42.5 })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!("42.5"));
    Ok(())
}

/// Asked for text, a list or an object comes out as its JSON, at any
/// depth (a written value never gets this; the literal cast refuses).
async fn structure_to_text(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("String").expect("String parses"));
    let outcome = rig.run(&CastNode, json!({ "value": [[1, "a"], { "b": true }] })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!(r#"[[1,"a"],{"b":true}]"#));
    rig.output_type("value", WeftType::parse("List[String]").expect("parses"));
    let outcome = rig.run(&CastNode, json!({ "value": [{ "a": 1 }, "x"] })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!([r#"{"a":1}"#, "x"]));
    Ok(())
}

async fn impossible_value(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("Number").expect("Number parses"));
    let outcome = rig.run(&CastNode, json!({ "value": "not a number" })).await;
    let err = outcome.result.expect_err("unparseable text must refuse").to_string();
    assert!(err.contains("cannot cast"), "{err}");
    Ok(())
}

//! TagRun self-tests: every wired input is a tag, the list adds to them,
//! any string becomes a safe tag, and a node with nothing to tag fails
//! instead of tagging nothing.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::steering::safe_tag;
use super::TagRunNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_wired_input_is_a_tag", wired_inputs_are_tags),
        NodeTest::fake("the_list_and_the_inputs_add_up", list_and_inputs_add_up),
        NodeTest::fake("an_unsafe_value_is_cleaned_and_fingerprinted", unsafe_value_is_cleaned),
        NodeTest::fake("two_values_alike_once_cleaned_stay_apart", alike_values_stay_apart),
        NodeTest::fake("a_long_value_fits_the_cap", long_value_fits),
        NodeTest::fake("a_clean_tag_is_kept_and_cleaning_is_idempotent", clean_tag_kept),
        NodeTest::fake("no_tags_is_a_failure", no_tags_fails),
    ]
}

async fn wired_inputs_are_tags(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&TagRunNode, json!({ "sender": "user_7", "channel": "support" }))
        .await
        .ok()?;
    let mut tagged = rig.execution_tags();
    assert_eq!(tagged.len(), 1, "one tag_execution call: {tagged:?}");
    let mut tags = tagged.remove(0);
    tags.sort();
    assert_eq!(tags, vec!["support".to_string(), "user_7".to_string()]);
    assert_eq!(outcome.outputs["done"], json!(true));
    assert!(outcome.outputs.get("tags").is_none(), "no tags output: chaining had no use");
    Ok(())
}

/// The declared list and the created ports are one set, a tag named
/// twice counted once.
async fn list_and_inputs_add_up(rig: FakeRig) -> WeftResult<()> {
    rig.run(&TagRunNode, json!({ "sender": "user_7", "tags": ["batch_a", "user_7"] }))
        .await
        .ok()?;
    let tagged = rig.execution_tags();
    assert_eq!(tagged, vec![vec!["user_7".to_string(), "batch_a".to_string()]]);
    Ok(())
}

/// A WhatsApp address or a phone number tags the run as written: the
/// unsafe characters become `_` and a fingerprint of the original is
/// appended, and the ctx accepts the result.
async fn unsafe_value_is_cleaned(rig: FakeRig) -> WeftResult<()> {
    rig.run(&TagRunNode, json!({ "sender": "49151@s.whatsapp.net" })).await.ok()?;
    let tagged = rig.execution_tags();
    assert_eq!(tagged.len(), 1);
    let tag = &tagged[0][0];
    assert!(tag.starts_with("49151_s_whatsapp_net-"), "{tag}");
    assert_eq!(tag.len(), "49151_s_whatsapp_net-".len() + 16, "{tag}");
    assert!(weft::tag::validate_tag(tag).is_ok(), "{tag}");
    assert_eq!(safe_tag("49151@s.whatsapp.net")?, *tag, "the same value gives the same tag");
    Ok(())
}

async fn alike_values_stay_apart(_rig: FakeRig) -> WeftResult<()> {
    let a = safe_tag("+33 6 12")?;
    let b = safe_tag("+33.6.12")?;
    assert!(a.starts_with("_33_6_12-") && b.starts_with("_33_6_12-"), "{a} {b}");
    assert_ne!(a, b, "the fingerprint keeps them apart");
    Ok(())
}

async fn long_value_fits(_rig: FakeRig) -> WeftResult<()> {
    let long = "x".repeat(100);
    let tag = safe_tag(&long)?;
    assert_eq!(tag.len(), 64, "{tag}");
    assert!(weft::tag::validate_tag(&tag).is_ok());
    let other = safe_tag(&format!("{long}y"))?;
    assert_ne!(tag, other, "two long values with one prefix do not collide");
    Ok(())
}

async fn clean_tag_kept(_rig: FakeRig) -> WeftResult<()> {
    assert_eq!(safe_tag("user_7")?, "user_7");
    let cleaned = safe_tag("a b")?;
    assert_eq!(safe_tag(&cleaned)?, cleaned, "cleaning a cleaned tag changes nothing");
    let err = safe_tag("").expect_err("empty");
    assert!(err.to_string().contains("must not be empty"), "{err}");
    Ok(())
}

async fn no_tags_fails(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&TagRunNode, json!({})).await.failure()?;
    assert!(err.contains("no tags"), "{err}");
    Ok(())
}

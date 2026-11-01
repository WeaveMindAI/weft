//! StopTagged self-tests: every wired input is a tag to stop, the self
//! choice follows the toggle, and nothing is stopped in the fake tier
//! (there are no siblings); the rig records the asks.

use serde_json::json;

use weft::{FakeRig, NodeTest, StopSelf, WeftResult};

use super::StopTaggedNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("wired_inputs_are_the_tags_to_stop_and_self_is_kept", keep_by_default),
        NodeTest::fake("the_toggle_includes_this_run", include_self),
        NodeTest::fake("no_tags_is_a_failure", no_tags_fails),
        NodeTest::fake("an_unsafe_value_is_cleaned_into_the_tag_tag_run_gives_it", unsafe_value_meets_its_tag),
    ]
}

async fn keep_by_default(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&StopTaggedNode, json!({ "sender": "user_7", "tags": ["batch_a"] }))
        .await
        .ok()?;
    assert_eq!(
        rig.stops(),
        vec![("user_7".to_string(), StopSelf::Keep), ("batch_a".to_string(), StopSelf::Keep)]
    );
    assert_eq!(outcome.outputs["done"], json!(true));
    assert!(rig.run_tags().is_empty(), "stopping does not tag");
    Ok(())
}

async fn include_self(rig: FakeRig) -> WeftResult<()> {
    rig.run(&StopTaggedNode, json!({ "exp": "exp_3", "includeSelf": true })).await.ok()?;
    assert_eq!(rig.stops(), vec![("exp_3".to_string(), StopSelf::Include)]);
    Ok(())
}

/// A phone number is no valid tag, so it is cleaned the way TagRun
/// cleans it (`_` for each unsafe character, then a fingerprint of the
/// value as written): the same sender reaches the run TagRun tagged, and
/// a lookalike written with dots is another tag and reaches nothing of
/// it.
async fn unsafe_value_meets_its_tag(rig: FakeRig) -> WeftResult<()> {
    rig.run(&StopTaggedNode, json!({ "sender": "+33 6 12" })).await.ok()?;
    rig.run(&StopTaggedNode, json!({ "sender": "+33.6.12" })).await.ok()?;
    let stops = rig.stops();
    assert_eq!(stops.len(), 2, "{stops:?}");
    let (tag, lookalike) = (&stops[0].0, &stops[1].0);
    assert!(tag.starts_with("_33_6_12-") && tag.len() == "_33_6_12-".len() + 16, "{tag}");
    assert_eq!(*tag, weft::tag::normalize_tag("+33 6 12").expect("normalizes"), "the tag TagRun puts on the run");
    assert!(lookalike.starts_with("_33_6_12-"), "{lookalike}");
    assert_ne!(tag, lookalike, "a lookalike sender is another tag");
    Ok(())
}

async fn no_tags_fails(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&StopTaggedNode, json!({})).await.failure()?;
    assert!(err.contains("no tags"), "{err}");
    assert!(rig.stops().is_empty());
    Ok(())
}

//! SlackFindChannel self-tests: paging the listing to resolve a name,
//! and the loud not-found end.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::SlackFindChannelNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("resolves_a_name_across_pages", resolves),
        NodeTest::fake("an_unknown_name_names_the_invite_rule", unknown),
        NodeTest::live("one_real_lookup_of_the_test_channel", "slack", live_lookup),
    ]
}

/// Resolve the standing test channel by name: read-only, so there is
/// nothing to clean up. The workspace keeps a channel named
/// `weft-node-tests` (the same channel the send tests target); a
/// workspace without one fails loud with the node's own not-found
/// message.
async fn live_lookup(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SlackFindChannelNode,
            json!({ "account": rig.access("slack"), "name": "weft-node-tests" }),
        )
        .await
        .ok()?;
    assert!(!outcome.output("id")?.as_str().expect("channel id").is_empty());
    assert_eq!(outcome.output("name")?, &json!("weft-node-tests"));
    Ok(())
}

const LIST: &str = "/api/conversations.list?limit=200&exclude_archived=true\
                    &types=public_channel,private_channel";

async fn resolves(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        LIST,
        json!({
            "ok": true,
            "channels": [{ "id": "C1", "name": "general" }],
            "response_metadata": { "next_cursor": "cur2" },
        }),
    );
    rig.respond(
        "GET",
        &format!("{LIST}&cursor=cur2"),
        json!({
            "ok": true,
            "channels": [{ "id": "C9", "name": "weft-node-tests", "is_private": true }],
        }),
    );
    let outcome = rig
        .run(
            &SlackFindChannelNode,
            json!({ "account": rig.access("slack"), "name": "#weft-node-tests" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["id"], json!("C9"));
    assert_eq!(outcome.outputs["name"], json!("weft-node-tests"), "the # is stripped");
    assert_eq!(outcome.outputs["private"], json!(true));
    Ok(())
}

async fn unknown(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        LIST,
        json!({ "ok": true, "channels": [{ "id": "C1", "name": "general" }] }),
    );
    let outcome = rig
        .run(
            &SlackFindChannelNode,
            json!({ "account": rig.access("slack"), "name": "nope" }),
        )
        .await;
    let err = outcome.result.expect_err("unknown channel must refuse").to_string();
    assert!(err.contains("no channel named 'nope'"), "{err}");
    Ok(())
}

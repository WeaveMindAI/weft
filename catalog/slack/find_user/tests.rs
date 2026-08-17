//! SlackFindUser self-tests: the two lookup keys and their
//! exclusivity.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, NodeErrExt};

use super::SlackFindUserNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("looks_up_by_email", by_email),
        NodeTest::fake("both_keys_at_once_refuse", both_keys),
        NodeTest::live("one_real_lookup_of_the_bot_itself", "slack", live_lookup),
    ]
}

/// One real id lookup, self-provisioned: the connected bot asks
/// auth.test who IT is, then the node looks that id up. No fixture,
/// nothing created, nothing billed.
async fn live_lookup(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let me = crate::api::call_on(conn.client(), "auth.test", json!({})).await?;
    let bot_user = me["user_id"].as_str().node_err("auth.test answered no user_id")?;
    let outcome = rig
        .run(
            &SlackFindUserNode,
            json!({ "account": rig.access("slack"), "id": bot_user }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("id")?.as_str(), Some(bot_user));
    assert!(!outcome.output("name")?.as_str().unwrap_or_default().is_empty());
    Ok(())
}

async fn by_email(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/api/users.lookupByEmail?email=ada@example.com",
        json!({ "ok": true, "user": {
            "id": "U7",
            "tz": "Europe/London",
            "profile": { "display_name": "ada", "email": "ada@example.com" },
        }}),
    );
    let outcome = rig
        .run(
            &SlackFindUserNode,
            json!({ "account": rig.access("slack"), "email": "ada@example.com" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["id"], json!("U7"));
    assert_eq!(outcome.outputs["name"], json!("ada"));
    assert_eq!(outcome.outputs["timezone"], json!("Europe/London"));
    Ok(())
}

async fn both_keys(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SlackFindUserNode,
            json!({ "account": rig.access("slack"), "email": "a@b.c", "id": "U7" }),
        )
        .await;
    let err = outcome.result.expect_err("two keys must refuse").to_string();
    assert!(err.contains("ONE lookup key"), "{err}");
    Ok(())
}

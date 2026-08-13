//! SlackFindUser self-tests: the two lookup keys and their
//! exclusivity.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackFindUserNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("looks_up_by_email", by_email),
        NodeTest::fake("both_keys_at_once_refuse", both_keys),
    ]
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

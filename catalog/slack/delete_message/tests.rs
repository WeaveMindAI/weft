//! SlackDeleteMessage self-tests: the one chat.delete call.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackDeleteMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("deletes_by_channel_and_ts", deletes)]
}

async fn deletes(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/chat.delete", json!({ "ok": true }));
    let outcome = rig
        .run(
            &SlackDeleteMessageNode,
            json!({ "account": rig.access("slack"), "channel": "C1", "ts": "1.2" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[0].body.clone().expect("delete body");
    assert_eq!(body, json!({ "channel": "C1", "ts": "1.2" }));
    Ok(())
}

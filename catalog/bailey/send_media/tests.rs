//! BaileySendMedia self-tests: the media payload (inline data: URL
//! when no public link exists), the caption, and the soft-error path.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::BaileySendMediaNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sends_the_file_inline_with_caption", sends),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn sends(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/action", json!({ "result": { "messageId": "wa-2" } }));
    let file = rig.store_file("cat.png", "image/png", b"png-bytes".to_vec());
    let outcome = rig
        .run(
            &BaileySendMediaNode,
            json!({
                "endpointUrl": "http://bridge.example:8090",
                "to": "4915112345678@s.whatsapp.net",
                "file": file,
                "caption": "look",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["messageId"], json!("wa-2"));
    let body = rig.requests()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("sendMedia"));
    let p = &body["payload"];
    assert_eq!(p["to"], json!("4915112345678@s.whatsapp.net"));
    assert!(
        p["mediaUrl"].as_str().is_some_and(|u| u.starts_with("data:image/png;base64,")),
        "the file rides inline when no public link exists: {}",
        p["mediaUrl"]
    );
    assert_eq!(p["mimetype"], json!("image/png"));
    assert_eq!(p["filename"], json!("cat.png"));
    assert_eq!(p["caption"], json!("look"));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/action",
        json!({ "result": { "error": "WhatsApp not connected" } }),
    );
    let file = rig.store_file("cat.png", "image/png", b"x".to_vec());
    let outcome = rig
        .run(
            &BaileySendMediaNode,
            json!({ "endpointUrl": "http://b:1", "to": "49@s.whatsapp.net", "file": file }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}

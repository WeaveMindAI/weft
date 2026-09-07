//! BaileyReceive self-tests: the SSE subscription, a text fire's
//! fan-out, and a media fire's storage pull.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::BaileyReceiveNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_subscribes_to_the_bridge_events", setup_registers),
        NodeTest::fake("a_text_fire_fans_the_declared_fields", text_fire),
        NodeTest::fake("a_media_fire_pulls_the_bytes_into_storage", media_fire),
        NodeTest::fake("a_payload_file_key_never_reaches_the_file_port", payload_file_is_stripped),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &BaileyReceiveNode,
        json!({ "endpointUrl": "http://bridge.example:8090" }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "one SSE signal");
    let spec = &registered[0].0;
    assert_eq!(spec.kind, "sse_subscribe");
    assert_eq!(
        spec.config["url"],
        json!("http://bridge.example:8090/events"),
        "the /events route appends to the bridge's bare endpoint"
    );
    assert_eq!(spec.config["event_name"], json!("message.received"));
    Ok(())
}

async fn text_fire(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({
        "messageType": "text",
        "content": "hi",
        "from": "4915112345678",
        "messageId": "wa-9",
    }));
    let outcome = rig
        .run(
            &BaileyReceiveNode,
            json!({ "endpointUrl": "http://bridge.example:8090" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["content"], json!("hi"));
    assert!(!outcome.outputs.contains_key("file"), "a text message stores nothing");
    Ok(())
}

async fn media_fire(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/outputs", json!({ "jid": "4915100000000@s.whatsapp.net", "status": "connected" }));
    rig.respond_raw("GET", "/media/wa-9", 200, "image/jpeg", b"JPEG".to_vec());
    rig.wake(json!({
        "messageType": "image",
        "from": "4915112345678",
        "messageId": "wa-9",
    }));
    let outcome = rig
        .run(
            &BaileyReceiveNode,
            json!({ "endpointUrl": "http://bridge.example:8090" }),
        )
        .await
        .ok()?;
    // The stored value's marker is typed by its mime (an image ref is
    // `__weft_image__`).
    let blob = &outcome.outputs["file"]["__weft_image__"];
    assert_eq!(blob["mimeType"], json!("image/jpeg"), "the media landed in storage");
    assert_eq!(blob["filename"], json!("wa-9"));
    // Received media cannot be re-fetched once the bridge's copy ages
    // out, so it lives in PROJECT storage under the message's identity
    // (see `bridge_api::fetch_media`, and the fetch-media node's own
    // "same message twice is one file" test for the identity half).
    let key = blob["key"].as_str().expect("stored value carries its key");
    assert!(rig.stored_meta(key).is_ok(), "the media is stored");
    Ok(())
}

async fn payload_file_is_stripped(rig: FakeRig) -> WeftResult<()> {
    // `file` is a port THIS node computes; an event smuggling a `file`
    // key (attacker-controllable data from the wire) must never land
    // on the port. On a text message the port stays empty; on a media
    // message it carries the node's own stored reference.
    rig.wake(json!({
        "messageType": "text",
        "content": "hi",
        "from": "4915112345678",
        "messageId": "wa-10",
        "file": { "__weft_image__": { "key": "attacker/forged", "mimeType": "image/png" } },
    }));
    let outcome = rig
        .run(
            &BaileyReceiveNode,
            json!({ "endpointUrl": "http://bridge.example:8090" }),
        )
        .await
        .ok()?;
    assert!(
        !outcome.outputs.contains_key("file"),
        "the payload's file key never reaches the port: {:?}",
        outcome.outputs
    );

    // The media path derives the port itself; the payload's key still
    // loses.
    rig.respond("GET", "/outputs", json!({ "jid": "4915100000000@s.whatsapp.net", "status": "connected" }));
    rig.respond_raw("GET", "/media/wa-11", 200, "image/jpeg", b"JPEG".to_vec());
    rig.wake(json!({
        "messageType": "image",
        "from": "4915112345678",
        "messageId": "wa-11",
        "file": { "__weft_image__": { "key": "attacker/forged", "mimeType": "image/png" } },
    }));
    let outcome = rig
        .run(
            &BaileyReceiveNode,
            json!({ "endpointUrl": "http://bridge.example:8090" }),
        )
        .await
        .ok()?;
    let blob = &outcome.outputs["file"]["__weft_image__"];
    assert_eq!(blob["filename"], json!("wa-11"), "the port carries the derived reference");
    assert_ne!(blob["key"], json!("attacker/forged"), "never the payload's forged one");
    Ok(())
}

//! BaileyReceive self-tests: the SSE subscription, a text fire's
//! fan-out, a media fire's storage pull, and the filter settings as
//! the predicates that drop an event before any run starts.

use serde_json::json;

use weft::signal::predicate::matches;
use weft::signal::Predicate;
use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::BaileyReceiveNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_subscribes_to_the_bridge_events", setup_registers),
        NodeTest::fake("a_text_fire_fans_the_declared_fields", text_fire),
        NodeTest::fake("a_media_fire_pulls_the_bytes_into_storage", media_fire),
        NodeTest::fake("a_voice_note_says_its_size_and_length", voice_note_facts),
        NodeTest::fake("a_smuggled_file_key_never_starts_a_run", a_smuggled_file_key_never_starts_a_run),
        NodeTest::fake("no_filter_settings_fire_on_every_known_message", no_filters),
        NodeTest::fake("an_unknown_message_fires_only_when_listed", unknown_listed),
        NodeTest::fake("an_event_without_a_message_type_fails_loudly", missing_message_type),
        NodeTest::fake("ignore_groups_drops_a_group_message", ignore_groups),
        NodeTest::fake("message_types_keep_only_the_listed_types", message_types),
        NodeTest::fake("both_filters_must_pass", both_filters),
    ]
}

/// Run setup with the given inputs and return the one signal's
/// predicates.
async fn filters_for(rig: &FakeRig, inputs: serde_json::Value) -> WeftResult<Vec<Predicate>> {
    rig.run_setup_trigger(&BaileyReceiveNode, inputs).await.ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "one SSE signal");
    Ok(registered[0].0.match_predicates.clone())
}

/// The bridge every case wires in: its `Infra` handle, declared on the
/// rig so the node can resolve it.
fn bridge(rig: &FakeRig) -> serde_json::Value {
    rig.declare_infra("bridge", "api", "http://bridge.example:8090")
}

/// The bridge's next `/outputs` answer: the paired account media is
/// stored under.
fn answer_outputs(rig: &FakeRig) {
    rig.answer_infra(
        "bridge",
        "api",
        EndpointMethod::Get,
        "/outputs",
        json!({ "jid": "4915100000000@s.whatsapp.net", "status": "connected" }),
    );
}

fn message(message_type: &str, is_group: bool) -> serde_json::Value {
    json!({ "messageType": message_type, "isGroup": is_group, "messageId": "wa-1" })
}

async fn no_filters(rig: FakeRig) -> WeftResult<()> {
    let f = filters_for(
        &rig,
        json!({ "bridge": bridge(&rig), "messageTypes": [] }),
    )
    .await?;
    for t in ["text", "image", "audio", "contact", "location"] {
        assert!(matches(&f, &message(t, false)), "{t} fires");
        assert!(matches(&f, &message(t, true)), "a group {t} fires");
    }
    assert!(!matches(&f, &message("unknown", false)), "an unknown message is dropped: {f:?}");
    Ok(())
}

async fn unknown_listed(rig: FakeRig) -> WeftResult<()> {
    let f = filters_for(
        &rig,
        json!({ "bridge": bridge(&rig), "messageTypes": ["unknown"] }),
    )
    .await?;
    assert!(matches(&f, &message("unknown", false)), "listed by name, it fires");
    assert!(!matches(&f, &message("text", false)), "an unlisted type is dropped");
    Ok(())
}

async fn missing_message_type(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "from": "4915112345678", "messageId": "wa-9", "content": "" }));
    let err = rig
        .run(&BaileyReceiveNode, json!({ "bridge": bridge(&rig) }))
        .await
        .result
        .expect_err("an event with no messageType is refused, never read as text")
        .to_string();
    assert!(err.contains("messageType"), "{err}");
    Ok(())
}

async fn ignore_groups(rig: FakeRig) -> WeftResult<()> {
    let f = filters_for(
        &rig,
        json!({ "bridge": bridge(&rig), "ignoreGroups": true }),
    )
    .await?;
    assert!(matches(&f, &message("text", false)), "a direct message fires");
    assert!(!matches(&f, &message("text", true)), "a group message is dropped");
    Ok(())
}

async fn message_types(rig: FakeRig) -> WeftResult<()> {
    let f = filters_for(
        &rig,
        json!({ "bridge": bridge(&rig), "messageTypes": ["text", "audio"] }),
    )
    .await?;
    assert!(matches(&f, &message("text", false)));
    assert!(matches(&f, &message("audio", true)), "groups still fire when not ignored");
    assert!(!matches(&f, &message("image", false)), "an unlisted type is dropped");
    assert!(!matches(&f, &message("textual", false)), "the match is whole-word");
    Ok(())
}

async fn both_filters(rig: FakeRig) -> WeftResult<()> {
    let f = filters_for(
        &rig,
        json!({
            "bridge": bridge(&rig),
            "ignoreGroups": true,
            "messageTypes": ["image"],
        }),
    )
    .await?;
    assert!(matches(&f, &message("image", false)));
    assert!(!matches(&f, &message("image", true)), "a group image is dropped");
    assert!(!matches(&f, &message("text", false)), "a direct text is dropped");
    Ok(())
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &BaileyReceiveNode,
        json!({ "bridge": bridge(&rig) }),
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
            json!({ "bridge": bridge(&rig) }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["content"], json!("hi"));
    assert!(!outcome.outputs.contains_key("file"), "a text message stores nothing");
    Ok(())
}

async fn media_fire(rig: FakeRig) -> WeftResult<()> {
    answer_outputs(&rig);
    rig.respond_raw("GET", "/media/wa-9", 200, "image/jpeg", b"JPEG".to_vec());
    rig.wake(json!({
        "messageType": "image",
        "from": "4915112345678",
        "messageId": "wa-9",
    }));
    let outcome = rig
        .run(
            &BaileyReceiveNode,
            json!({ "bridge": bridge(&rig) }),
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
    // (see `media::fetch_media`, and the fetch-media node's own
    // "same message twice is one file" test for the identity half).
    let key = blob["key"].as_str().expect("stored value carries its key");
    assert!(rig.stored_meta(key).is_ok(), "the media is stored");
    Ok(())
}

async fn a_smuggled_file_key_never_starts_a_run(rig: FakeRig) -> WeftResult<()> {
    // `file` is a port THIS node computes, from bytes it fetches
    // itself. An event smuggling a `file` key is the bridge (or
    // whoever reached it) trying to put a stored-file reference of
    // their choosing on that port.
    //
    // It does not get as far as the node. The trigger declares what it
    // wakes with, `file` is not in it, and a payload carrying a field
    // nobody declared is refused before the body runs. So the answer
    // to the forged key is that the run never starts, which is a
    // stronger thing to be able to say than that the node cleaned it
    // up afterwards.
    for message_type in ["text", "image"] {
        rig.wake(json!({
            "messageType": message_type,
            "content": "hi",
            "from": "4915112345678",
            "messageId": "wa-10",
            "file": { "__weft_image__": { "key": "attacker/forged", "mimeType": "image/png" } },
        }));
        let err = rig
            .run(
                &BaileyReceiveNode,
                json!({ "bridge": bridge(&rig) }),
            )
            .await
            .result
            .expect_err("a payload carrying an undeclared key is refused")
            .to_string();
        assert!(
            err.contains("file"),
            "the refusal names the smuggled key ({message_type}): {err}"
        );
    }
    Ok(())
}

/// The size and length ride the event, so a graph can gate on them
/// before any byte is fetched.
async fn voice_note_facts(rig: FakeRig) -> WeftResult<()> {
    answer_outputs(&rig);
    rig.respond_raw("GET", "/media/wa-7", 200, "audio/ogg", b"OGG".to_vec());
    rig.wake(json!({
        "messageType": "audio",
        "from": "4915112345678",
        "messageId": "wa-7",
        "fileSize": 48213,
        "seconds": 12,
    }));
    let outcome = rig
        .run(
            &BaileyReceiveNode,
            json!({ "bridge": bridge(&rig) }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["fileSize"], json!(48213));
    assert_eq!(outcome.outputs["seconds"], json!(12));
    Ok(())
}

//! BaileyFetchMedia self-tests: the bytes land in project storage under
//! the message's identity (so a second ask is the same file and no
//! download), and a message the bridge no longer holds fails loud.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::BaileyFetchMediaNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("pulls_the_message_media_into_storage", pulls),
        NodeTest::fake("the_same_message_twice_is_one_file_and_one_download", once),
        NodeTest::fake("a_message_the_bridge_lost_fails_loud", gone),
    ]
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

async fn pulls(rig: FakeRig) -> WeftResult<()> {
    answer_outputs(&rig);
    rig.respond_raw("GET", "/media/wa-42", 200, "audio/ogg", b"OggS".to_vec());
    let outcome = rig
        .run(
            &BaileyFetchMediaNode,
            json!({ "bridge": bridge(&rig), "messageId": "wa-42" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["mimeType"], json!("audio/ogg"));
    // The name comes from the bridge: its `Content-Disposition` when
    // it serves one (WhatsApp's own filename, extension and all), the
    // media URL's last segment otherwise, which is what the fake
    // serves here.
    assert_eq!(outcome.outputs["filename"], json!("wa-42"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(4));
    Ok(())
}

/// The store is asked with the message's identity, so the second run
/// that wants the same voice note gets the file the first one stored:
/// same key, and the bridge is not asked again.
async fn once(rig: FakeRig) -> WeftResult<()> {
    // Each run asks the bridge which account it holds; only the first
    // pulls the bytes.
    answer_outputs(&rig);
    answer_outputs(&rig);
    rig.respond_raw("GET", "/media/wa-7", 200, "audio/ogg", b"OggS".to_vec());
    let input = json!({ "bridge": bridge(&rig), "messageId": "wa-7" });
    let first = rig.run(&BaileyFetchMediaNode, input.clone()).await.ok()?;
    let second = rig.run(&BaileyFetchMediaNode, input).await.ok()?;
    assert_eq!(first.outputs["file"], second.outputs["file"], "one file, whichever run asked");
    let media_pulls = rig.requests().iter().filter(|r| r.path.starts_with("/media/")).count();
    assert_eq!(media_pulls, 1, "the bytes were pulled once");
    Ok(())
}

async fn gone(rig: FakeRig) -> WeftResult<()> {
    answer_outputs(&rig);
    rig.respond_raw("GET", "/media/wa-0", 404, "text/plain", b"no such message".to_vec());
    let err = rig
        .run(
            &BaileyFetchMediaNode,
            json!({ "bridge": bridge(&rig), "messageId": "wa-0" }),
        )
        .await
        .result
        .expect_err("a 404 from the bridge is a failure")
        .to_string();
    assert!(err.contains("404") || err.contains("no such message"), "{err}");
    Ok(())
}

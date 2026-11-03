//! Shared test plumbing for the elevenlabs package. Live: the stock
//! voice every speech test uses, a real TTS sample generator (several
//! tests need genuine speech as INPUT), and the one DELETE shape the
//! self-cleaning tests (minted voices, minted agents) share. Fake: the
//! canned answers of an Image & Video job that runs to its end.
#![cfg(feature = "node-tests")]

use base64::Engine as _;
use serde_json::{json, Value};

use weft::access::client::{checked_send, get_json, url};
use weft::access::OpenedConnection;
use weft::{FakeRig, NodeErrExt, WeftResult};

use super::elevenlabs::API;

/// Canned answers for one generation that completes: the create on
/// `/v1/flows/{media}` answers `id`, the wait ends `completed`, the
/// read answers a content URL, and that URL serves `bytes` as `mime`.
pub fn completes(rig: &FakeRig, media: &str, id: &str, mime: &str, bytes: &[u8]) {
    rig.respond("POST", &format!("/v1/flows/{media}"), json!({ "id": id, "status": "pending" }));
    rig.signal(json!({ "status": "completed" }));
    rig.respond(
        "GET",
        &format!("/v1/flows/{media}/{id}"),
        json!({
            "id": id,
            "status": "completed",
            "content_url": format!("https://storage.googleapis.com/generations/{id}"),
            "content_mime_type": mime,
        }),
    );
    rig.respond_raw("GET", &format!("/generations/{id}"), 200, mime, bytes.to_vec());
}

/// Canned answers for one generation that fails with `reason`.
pub fn fails(rig: &FakeRig, media: &str, id: &str, reason: &str, message: &str) {
    rig.respond("POST", &format!("/v1/flows/{media}"), json!({ "id": id, "status": "pending" }));
    rig.signal(json!({ "status": "failed" }));
    rig.respond(
        "GET",
        &format!("/v1/flows/{media}/{id}"),
        json!({ "id": id, "status": "failed", "failure_reason": reason, "error_message": message }),
    );
}

/// The inline reference a file of `mime` holding `bytes` becomes.
pub fn inline(mime: &str, bytes: &[u8]) -> Value {
    json!({
        "type": "inline_base64",
        "mime_type": mime,
        "content_base64": base64::engine::general_purpose::STANDARD.encode(bytes),
    })
}

/// The JSON body of the create request (the first POST the node sent
/// to `/v1/flows/...`).
pub fn create_body(rig: &FakeRig) -> Value {
    rig.requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path.starts_with("/v1/flows/"))
        .and_then(|r| r.body)
        .expect("the node sent a JSON create")
}

/// The bytes of the file a node emitted on `port`, and its stored name
/// and type.
pub fn emitted_file(
    rig: &FakeRig,
    outputs: &serde_json::Map<String, Value>,
    port: &str,
) -> WeftResult<(Vec<u8>, String, String)> {
    let stored = weft::storage::StoredFile::from_value(&outputs[port])?;
    let meta = rig.stored_meta(&stored.key)?;
    Ok((rig.stored_bytes(&stored.key)?.to_vec(), meta.filename, meta.mime_type))
}

/// The account's first premade voice (every account ships the stock
/// set), so speech tests never depend on a fixture.
pub async fn first_voice_id(conn: &OpenedConnection) -> WeftResult<String> {
    let voices =
        get_json(conn.client(), url(API, &["voices"])?.as_str(), "list the ElevenLabs voices").await?;
    voices["voices"]
        .as_array()
        .and_then(|v| v.first())
        .and_then(|v| v["voice_id"].as_str())
        .map(str::to_string)
        .node_err("the account lists no voices")
}

/// A short REAL speech clip (mp3 bytes) minted through the flash TTS
/// route: what the isolation / re-voice / alignment / dub / clone
/// tests feed in, since those routes want genuine speech, not a tone.
pub async fn speech_sample(
    conn: &OpenedConnection,
    voice: &str,
    text: &str,
) -> WeftResult<Vec<u8>> {
    let address = url(API, &["text-to-speech", voice])?.query("output_format", "mp3_44100_128");
    let request = conn.client().post(address.as_str()).json(&json!({ "text": text, "model_id": "eleven_flash_v2_5" }));
    let resp = checked_send(request, "mint an ElevenLabs speech sample").await?;
    Ok(resp.bytes().await.node_err("read the sample bytes")?.to_vec())
}

/// DELETE `address` through the test's own connection, loud on refusal:
/// how minted voices and agents leave the account again.
pub async fn delete(conn: &OpenedConnection, address: &str, what: &str) -> WeftResult<()> {
    checked_send(conn.client().delete(address), what).await.map(drop)
}

/// Delete a minted voice by id.
pub async fn delete_voice(conn: &OpenedConnection, voice_id: &str) -> WeftResult<()> {
    delete(conn, url(API, &["voices", voice_id])?.as_str(), "delete the test voice").await
}

/// Delete a minted agent by id.
pub async fn delete_agent(conn: &OpenedConnection, agent_id: &str) -> WeftResult<()> {
    delete(conn, url(API, &["convai", "agents", agent_id])?.as_str(), "delete the test agent").await
}

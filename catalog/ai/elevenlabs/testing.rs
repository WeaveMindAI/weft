//! Shared live-test plumbing for the elevenlabs package: the stock
//! voice every speech test uses, a real TTS sample generator (several
//! tests need genuine speech as INPUT), and the one DELETE shape the
//! self-cleaning tests (minted voices, minted agents) share.
#![cfg(feature = "node-tests")]

use weft::access::client::get_json;
use weft::access::OpenedConnection;
use weft::{NodeErrExt, WeftResult};

use super::elevenlabs::API;

/// The account's first premade voice (every account ships the stock
/// set), so speech tests never depend on a fixture.
pub async fn first_voice_id(conn: &OpenedConnection) -> WeftResult<String> {
    let voices =
        get_json(conn.client(), &format!("{API}/voices"), "elevenlabs: list the voices").await?;
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
    let resp = conn
        .client()
        .post(format!(
            "{API}/text-to-speech/{voice}?output_format=mp3_44100_128"
        ))
        .json(&serde_json::json!({ "text": text, "model_id": "eleven_flash_v2_5" }))
        .send()
        .await
        .node_err("elevenlabs: mint a speech sample")?;
    let status = resp.status();
    if !status.is_success() {
        weft::node_bail!(
            "the sample TTS answered {status}: {}",
            resp.text().await.unwrap_or_default().chars().take(300).collect::<String>()
        );
    }
    Ok(resp.bytes().await.node_err("read the sample bytes")?.to_vec())
}

/// DELETE `url` through the test's own connection, loud on refusal:
/// how minted voices and agents leave the account again.
pub async fn delete(conn: &OpenedConnection, url: &str, what: &str) -> WeftResult<()> {
    let resp = conn.client().delete(url).send().await.node_err(what)?;
    let status = resp.status();
    if !status.is_success() {
        weft::node_bail!(
            "the service answered {status} trying to {what}: {}",
            resp.text().await.unwrap_or_default().chars().take(300).collect::<String>()
        );
    }
    Ok(())
}

/// Delete a minted voice by id.
pub async fn delete_voice(conn: &OpenedConnection, voice_id: &str) -> WeftResult<()> {
    delete(conn, &format!("{API}/voices/{voice_id}"), "delete the test voice").await
}

/// Delete a minted agent by id.
pub async fn delete_agent(conn: &OpenedConnection, agent_id: &str) -> WeftResult<()> {
    delete(conn, &format!("{API}/convai/agents/{agent_id}"), "delete the test agent").await
}

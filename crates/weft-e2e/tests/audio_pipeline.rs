//! The realtime audio-to-text pipeline, end to end against the REAL
//! ElevenLabs realtime API: a 50s public-domain WAV asset (JFK's
//! inaugural) streamed live onto an ephemeral bus by AudioStream,
//! transcribed over the connection's measured WebSocket by
//! ElevenLabsTranscribe, segments relayed onto a text bus a Debug node
//! follows, and the full transcript emitted at the end.
//!
//! Two runs, one per credential door, mirroring the OpenRouter
//! metering pair: the user's OWN pasted key (cost record says
//! `their-own`) and the runtime's key from the shared-credentials file
//! (cost record says `ours`; a silent fall-through to the wrong
//! credential fails either test).
//!
//! Each run spends real money (~$0.006 of Scribe realtime time) and
//! real wall clock (the stream is deliberately REALTIME, so a run
//! takes about the audio's length). Skips without
//! `WEFT_E2E_ELEVENLABS_API_KEY` (the shared-door run also needs an
//! `elevenlabs` api_key entry in the cluster's credentials file).
#![cfg(feature = "e2e")]

use anyhow::Result;
use serde_json::{json, Value};
use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::ensure::{self, env_or_skip};
use weft_e2e::project::Project;
use weft_e2e::run;

/// Drive the fixture with `handle` picked on the access node and
/// assert the whole pipe: a real transcript out the far end, and ONE
/// resolved cost record for the session, spent on the expected
/// credential.
async fn assert_transcribed(
    disp: weft_e2e::client::Dispatcher,
    conn: weft_e2e::access::Connection,
    origin: &str,
) -> Result<()> {
    let mut project = Project::prepare("audio_transcribe", disp).await?;
    set_account(&project, "ears", "connection", conn.handle())?;
    project.set_node_config("src", "audio", "@asset(\"assets/speech.wav\", Audio)")?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The transcript came through the whole pipe: audio bus -> realtime
    // session -> text bus -> full-text port. The exact words are the
    // model's business; a real transcription of 50s of speech is not a
    // few characters.
    let text = settled
        .input_of("out")
        .and_then(|i| i.get("data").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default();
    anyhow::ensure!(
        text.split_whitespace().count() >= 20,
        "transcript looks empty or truncated: {text:?}"
    );

    // The session was MEASURED: the meter's frame tap priced the audio
    // actually sent (50s of it, a fraction of a cent) and booked one
    // resolved cost record on the expected credential.
    settled.assert_measured("elevenlabs", origin, 1).await?;

    project.finish().await?;
    conn.finish().await
}

/// The user's OWN key: the real paste flow (the key lands in the store
/// sealed, the spec's declared test call proves it against /v1/user),
/// then the pipeline, measured as `their-own`.
#[tokio::test]
async fn a_wav_asset_streams_through_realtime_transcription_to_text() -> Result<()> {
    let Some(key) = env_or_skip("WEFT_E2E_ELEVENLABS_API_KEY") else { return Ok(()) };
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("ai/elevenlabs", "access")?,
        "own",
        json!({ "key": key }),
    )
    .await?;
    assert_transcribed(disp, conn, "their-own").await
}

/// The RUNTIME's key: a one-click shared-door connection resolving to
/// the `elevenlabs` api_key entry in the cluster's credentials file,
/// then the same pipeline, measured as `ours`.
#[tokio::test]
async fn the_runtime_key_door_measures_the_session_as_ours() -> Result<()> {
    // Gate on the same var as the own-key run: it marks "this operator
    // has ElevenLabs set up" (the shared door itself reads the
    // credentials file, not this env var).
    if env_or_skip("WEFT_E2E_ELEVENLABS_API_KEY").is_none() {
        return Ok(());
    }
    let disp = ensure::up().await?;
    let conn =
        connect_direct(&disp, catalog_spec("ai/elevenlabs", "access")?, "shared", json!({}))
            .await?;
    assert_transcribed(disp, conn, "ours").await
}

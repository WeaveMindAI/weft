//! ElevenLabsMusic self-tests: the request the node builds from a prompt
//! or a composition plan, and the stored file and song id it emits. The
//! shapes ElevenLabs refuses are the metadata's rules.

use serde_json::{json, Value};

use weft::context::LogLevel;
use weft::{FakeRig, LiveRig, NodeErrExt, NodeTest, WeftResult};

use super::{file_type, ElevenLabsMusicNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_offered_output_format_has_a_file_type", file_types),
        NodeTest::fake("composes_and_stores_the_track", composes),
        NodeTest::fake("every_prompt_setting_reaches_the_request", every_prompt_setting),
        NodeTest::fake("a_composition_plan_and_its_seed_reach_the_request", composes_from_a_plan),
        NodeTest::fake("each_format_is_stored_under_its_own_name_and_type", formats),
        NodeTest::fake("a_missing_song_id_is_said_only_to_a_program_that_reads_it", missing_song_id),
        NodeTest::live("one_real_three_second_track", "elevenlabs", live_track),
    ]
}

/// One real three-second track (the shortest ElevenLabs composes, and
/// music is priced by the minute) on the default model and format;
/// nothing is stored for editing, so nothing lands on the account.
async fn live_track(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ElevenLabsMusicNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "a short calm solo piano phrase",
                "lengthSecs": 3,
                "forceInstrumental": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("mimeType")?.as_str(), Some("audio/mpeg"), "auto answers an mp3");
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 10_000.0, "three seconds of mp3 is tens of kilobytes: {size}");
    assert!(outcome.output("songId")?.as_str().is_some_and(|id| !id.is_empty()), "ElevenLabs names the song in a header");
    Ok(())
}

/// The formats the metadata offers.
fn offered_formats() -> WeftResult<Vec<String>> {
    let metadata: Value = serde_json::from_str(include_str!("metadata.json")).node_err("parse the music metadata")?;
    let input = metadata["inputs"]
        .as_array()
        .and_then(|inputs| inputs.iter().find(|i| i["name"] == "outputFormat"))
        .node_err("the metadata declares `outputFormat`")?;
    Ok(input["widget"]["options"]
        .as_array()
        .node_err("`outputFormat` offers options")?
        .iter()
        .filter_map(|o| o.as_str().map(str::to_string))
        .collect())
}

/// Every format the metadata offers stores under a real extension and
/// mime, `auto` as the mp3 ElevenLabs answers it with on every model.
fn file_types() -> WeftResult<()> {
    let formats = offered_formats()?;
    assert!(formats.iter().any(|f| f == "auto"), "ElevenLabs's own default is offered");
    for format in &formats {
        file_type(format)?;
    }
    assert_eq!(file_type("auto")?, ("mp3", "audio/mpeg"));
    assert_eq!(file_type("mp3_48000_320")?, ("mp3", "audio/mpeg"));
    assert_eq!(file_type("opus_48000_192")?, ("ogg", "audio/ogg; codecs=opus"));
    Ok(())
}

const MUSIC: &str = "/v1/music";

/// Canned audio for the music route under `query`, named `song-7`.
fn answers(rig: &FakeRig, query: &str, mime: &str) {
    rig.respond_with_headers("POST", &format!("{MUSIC}?{query}"), 200, mime, &[("song-id", "song-7")], b"track".to_vec());
}

/// The node's account plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({ "account": rig.access("elevenlabs") });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn composes(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=auto", "audio/mpeg");
    let outcome = rig
        .run(
            &ElevenLabsMusicNode,
            inputs(&rig, json!({ "prompt": "warm lo-fi piano", "lengthSecs": 30, "forceInstrumental": true })),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("music.mp3"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(5));
    assert_eq!(outcome.outputs["songId"], json!("song-7"), "the id ElevenLabs answered with");
    let sent = &rig.requests()[0];
    assert_eq!(sent.query.as_deref(), Some("output_format=auto"), "ElevenLabs picks the format for the model");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "prompt": "warm lo-fi piano",
            "model_id": "music_v2_5",
            "music_length_ms": 30000,
            "force_instrumental": true,
        })
    );
    Ok(())
}

async fn every_prompt_setting(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=mp3_48000_320", "audio/mpeg");
    rig.run(
        &ElevenLabsMusicNode,
        inputs(
            &rig,
            json!({
                "prompt": "an anthem about Siobhan",
                "model": "music_v2",
                "outputFormat": "mp3_48000_320",
                "lengthSecs": 45,
                "generationMode": "loop",
                "lyrics": "Siobhan, Siobhan",
                "finetune": "ft-1",
                "finetuneStrength": 0.5,
                "usePhoneticNames": true,
                "storeForInpainting": true,
                "signWithC2pa": true,
            }),
        ),
    )
    .await
    .ok()?;
    let sent = &rig.requests()[0];
    assert_eq!(sent.query.as_deref(), Some("output_format=mp3_48000_320"));
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "prompt": "an anthem about Siobhan",
            "model_id": "music_v2",
            "music_length_ms": 45000,
            "generation_mode": "loop",
            "lyrics_text": "Siobhan, Siobhan",
            "finetune_id": "ft-1",
            "finetune_strength": 0.5,
            "use_phonetic_names": true,
            "store_for_inpainting": true,
            "sign_with_c2pa": true,
        })
    );
    Ok(())
}

/// A plan that keeps the opening of a stored song and adds a new
/// chorus, as ElevenLabs's inpainting guide builds one, goes out as it
/// was given, with the seed only a plan takes.
async fn composes_from_a_plan(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=opus_48000_64", "audio/ogg");
    let plan = json!({
        "chunks": [
            { "song_id": "song-1", "range": { "start_ms": 0, "end_ms": 30000 } },
            {
                "text": "[Chorus]\nWe're rising up tonight",
                "duration_ms": 30000,
                "positive_styles": ["bigger drums", "anthemic"],
                "negative_styles": [],
                "context_adherence": "high",
            },
        ]
    });
    let outcome = rig
        .run(
            &ElevenLabsMusicNode,
            inputs(&rig, json!({ "compositionPlan": plan, "seed": 7, "outputFormat": "opus_48000_64" })),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("music.ogg"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/ogg; codecs=opus"));
    assert_eq!(
        rig.requests()[0].body.as_ref().expect("json payload"),
        &json!({ "composition_plan": plan, "seed": 7, "model_id": "music_v2_5" })
    );
    Ok(())
}

async fn formats(rig: FakeRig) -> WeftResult<()> {
    for (format, filename, mime) in [
        ("mp3_44100_128", "music.mp3", "audio/mpeg"),
        ("pcm_44100", "music.pcm", "audio/pcm"),
        ("ulaw_8000", "music.ulaw", "audio/basic"),
        ("alaw_8000", "music.alaw", "audio/PCMA"),
        ("opus_48000_192", "music.ogg", "audio/ogg; codecs=opus"),
    ] {
        answers(&rig, &format!("output_format={format}"), mime);
        let outcome =
            rig.run(&ElevenLabsMusicNode, inputs(&rig, json!({ "prompt": "lo-fi", "outputFormat": format }))).await.ok()?;
        assert_eq!(outcome.outputs["filename"], json!(filename), "{format}");
        assert_eq!(outcome.outputs["mimeType"], json!(mime), "{format}");
    }
    Ok(())
}

async fn missing_song_id(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", &format!("{MUSIC}?output_format=auto"), 200, "audio/mpeg", b"track".to_vec());
    rig.wire_output("songId");
    let outcome = rig.run(&ElevenLabsMusicNode, inputs(&rig, json!({ "prompt": "lo-fi" }))).await.ok()?;
    assert!(outcome.outputs.get("songId").is_none(), "no id is made up");
    assert_eq!(outcome.outputs["filename"], json!("music.mp3"), "the paid-for track still goes out");
    assert!(
        rig.logs().iter().any(|(level, line)| *level == LogLevel::Warn && line.contains("without a song-id header")),
        "the program reading `songId` is told why it is empty: {:?}",
        rig.logs()
    );
    Ok(())
}

//! What ElevenLabs's two speech routes (text to speech and text to
//! dialogue) take beyond the text and the voice settings, read from the
//! same-named inputs of the node that speaks: the language, the seed,
//! number spelling, pronunciation dictionaries, request stitching, the
//! professional voice switch, audio effects and zero retention. Both
//! nodes end on `emit_audio_with_id`, giving out the `request-id` a later
//! request stitches to. What ElevenLabs would refuse among these is the
//! nodes' metadata rules', checked before the body runs.

use serde::Deserialize;
use serde_json::{json, Map, Value};

use weft::access::client::{url, Url};
use weft::{RunContext, WeftResult};

use super::flows::set_some;

/// The address a speech request goes to: `API` and `segments`, with its
/// output format, and zero retention when the node turned logging off.
pub fn speech_url(ctx: &RunContext, segments: &[&str], output_format: &str) -> WeftResult<Url> {
    let logging: bool = ctx.inputs.get("enableLogging")?;
    Ok(url(super::elevenlabs::API, segments)?
        .query("output_format", output_format)
        .query_opt("enable_logging", (!logging).then_some("false")))
}

/// Put every shared setting the node was given on `body`.
pub fn shared_settings(ctx: &RunContext, body: &mut Value) -> WeftResult<()> {
    let language: Option<String> = ctx.inputs.opt("languageCode")?;
    let seed: Option<u64> = ctx.inputs.opt("seed")?;
    let normalization: Option<String> = ctx.inputs.opt("textNormalization")?;
    let pvc_as_ivc: bool = ctx.inputs.get("usePvcAsIvc")?;
    let dictionaries: Vec<Dictionary> = ctx.inputs.list("pronunciationDictionaries")?;
    let previous_ids: Vec<String> = ctx.inputs.list("previousRequestIds")?;
    let next_ids: Vec<String> = ctx.inputs.list("nextRequestIds")?;

    set_some(body, "language_code", language);
    set_some(body, "seed", seed);
    set_some(body, "apply_text_normalization", normalization);
    if pvc_as_ivc {
        body["use_pvc_as_ivc"] = json!(true);
    }
    if !dictionaries.is_empty() {
        body["pronunciation_dictionary_locators"] = dictionaries.into_iter().map(Dictionary::locator).collect();
    }
    for (field, ids) in [("previous_request_ids", previous_ids), ("next_request_ids", next_ids)] {
        if !ids.is_empty() {
            body[field] = json!(ids);
        }
    }
    if let Some(effects) = audio_effects(ctx)? {
        body["audio_effects"] = effects;
    }
    Ok(())
}

/// The `voice_settings` object from the node's `stability`,
/// `similarityBoost`, `style`, `speed` and `speakerBoost` inputs, or
/// `None` when none is set, so the voice keeps its own settings.
pub fn voice_settings(ctx: &RunContext) -> WeftResult<Option<Value>> {
    let mut settings = Map::new();
    for (port, key) in [("stability", "stability"), ("similarityBoost", "similarity_boost"), ("style", "style"), ("speed", "speed")] {
        if let Some(v) = ctx.inputs.opt::<f64>(port)? {
            settings.insert(key.into(), json!(v));
        }
    }
    if let Some(v) = ctx.inputs.opt::<bool>("speakerBoost")? {
        settings.insert("use_speaker_boost".into(), json!(v));
    }
    Ok((!settings.is_empty()).then(|| settings.into()))
}

/// One entry of `pronunciationDictionaries`: a bare id applies the
/// dictionary's latest version, `{ id, version }` pins one.
#[derive(Deserialize)]
#[serde(untagged)]
enum Dictionary {
    Latest(String),
    Pinned { id: String, version: Option<String> },
}

impl Dictionary {
    /// The entry as ElevenLabs's locator.
    fn locator(self) -> Value {
        let (id, version) = match self {
            Dictionary::Latest(id) => (id, None),
            Dictionary::Pinned { id, version } => (id, version),
        };
        let mut locator = json!({ "pronunciation_dictionary_id": id });
        set_some(&mut locator, "version_id", version);
        locator
    }
}

/// The `audio_effects` object, or `None` when the node set no effect.
fn audio_effects(ctx: &RunContext) -> WeftResult<Option<Value>> {
    let mut effects = Map::new();
    for (port, key) in [("filterPreset", "filter_preset_id"), ("environment", "environment_id"), ("backgroundNoise", "background_noise_id")] {
        if let Some(v) = ctx.inputs.opt::<String>(port)? {
            effects.insert(key.into(), v.into());
        }
    }
    for (port, key) in [("distance", "distance"), ("sendLevel", "send_level"), ("pan", "pan")] {
        if let Some(v) = ctx.inputs.opt::<f64>(port)? {
            effects.insert(key.into(), v.into());
        }
    }
    Ok((!effects.is_empty()).then(|| effects.into()))
}

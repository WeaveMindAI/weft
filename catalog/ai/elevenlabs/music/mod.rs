//! ElevenLabsMusic: a text prompt, or a composition plan laid out chunk
//! by chunk, to a music track: POST it to the music route, store the
//! answered audio, and emit it with the id ElevenLabs gave the song.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::url;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio_with_id, API};
use super::flows::set_some;

/// The output format that lets ElevenLabs pick the best one for the
/// model.
const AUTO: &str = "auto";

#[derive(NodeManifest)]
pub struct ElevenLabsMusicNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsMusicNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let body = request_body(&ctx)?;
        let (extension, mime) = file_type(&output_format)?;

        let http = ctx.client(&account).await?;
        let url = url(API, &["music"])?.query("output_format", &output_format);
        let what = "compose the music on ElevenLabs";
        let filename = format!("music.{extension}");
        emit_audio_with_id(&ctx, http.post(url.as_str()).json(&body), what, &filename, mime, ("song-id", "songId")).await
    }
}

/// The JSON body from the node's inputs: every setting it was given,
/// and nothing for one left empty or off, so ElevenLabs applies its own
/// default there.
fn request_body(ctx: &RunContext) -> WeftResult<Value> {
    let prompt: Option<String> = ctx.inputs.opt("prompt")?;
    let plan: Option<Value> = ctx.inputs.opt("compositionPlan")?;
    let model: String = ctx.inputs.get("model")?;
    // Whole numbers: the widgets' step holds a wired value to whole
    // numbers too.
    let length_secs: Option<u64> = ctx.inputs.opt("lengthSecs")?;
    let seed: Option<u64> = ctx.inputs.opt("seed")?;
    let mode: Option<String> = ctx.inputs.opt("generationMode")?;
    let lyrics: Option<String> = ctx.inputs.opt("lyrics")?;
    let finetune: Option<String> = ctx.inputs.opt("finetune")?;
    let strength: Option<f64> = ctx.inputs.opt("finetuneStrength")?;

    let mut body = json!({ "model_id": model });
    set_some(&mut body, "prompt", prompt);
    set_some(&mut body, "composition_plan", plan);
    set_some(&mut body, "music_length_ms", length_secs.map(|secs| secs * 1000));
    set_some(&mut body, "generation_mode", mode);
    set_some(&mut body, "lyrics_text", lyrics);
    set_some(&mut body, "seed", seed);
    set_some(&mut body, "finetune_id", finetune);
    set_some(&mut body, "finetune_strength", strength);
    for (port, field) in [
        ("forceInstrumental", "force_instrumental"),
        ("usePhoneticNames", "use_phonetic_names"),
        ("storeForInpainting", "store_for_inpainting"),
        ("signWithC2pa", "sign_with_c2pa"),
    ] {
        if ctx.inputs.get::<bool>(port)? {
            body[field] = json!(true);
        }
    }
    Ok(body)
}

/// The stored file's extension and mime. ElevenLabs answers `auto`
/// with an mp3 whatever the model (mp3_44100_128 on its v1 models,
/// mp3_48000_192 on its v2 models), so `auto` is stored as one.
fn file_type(output_format: &str) -> WeftResult<(&'static str, &'static str)> {
    audio_file_type(if output_format == AUTO { "mp3" } else { output_format })
}

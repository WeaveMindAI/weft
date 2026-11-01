//! What both voice preview nodes share (Design Voice, which designs a
//! voice from a description, and Remix Voice, which remixes one the
//! account has): the line the previews speak, the request fields both
//! routes take, and the ending that stores each preview and hands them
//! on for Create Voice.

use base64::Engine as _;
use serde_json::{json, Value};

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::storage::{StorageScope};
use weft::{Access, RunContext, NodeErrExt, WeftResult};

use super::elevenlabs::{audio_file_type, API};
use super::flows::set_some;

/// What ElevenLabs refuses in the line voice previews speak, or `None`:
/// both preview routes (design and remix) speak a line of 100 to 1000
/// characters.
pub fn line_refusal(text: Option<&str>) -> Option<String> {
    let spoken = text.map(|t| t.chars().count()).filter(|n| !(100..=1000).contains(n))?;
    Some(format!(
        "`text` is {spoken} characters, and ElevenLabs speaks a line of 100 to 1000: {}, or leave `text` empty and \
         ElevenLabs writes one",
        if spoken < 100 { "write a longer line" } else { "shorten it" }
    ))
}

/// The request fields both voice preview routes (design and remix)
/// take, read from the inputs of the same names on either node: the
/// description, the line the previews speak (or ElevenLabs asked to
/// write one), and the guidance scale, loudness, seed and prompt
/// strength when given.
pub fn preview_request(ctx: &RunContext, description: &str, text: Option<&str>) -> WeftResult<Value> {
    let mut body = json!({ "voice_description": description });
    match text {
        Some(t) => body["text"] = json!(t),
        None => body["auto_generate_text"] = json!(true),
    }
    set_some(&mut body, "guidance_scale", ctx.inputs.opt::<f64>("guidanceScale")?);
    set_some(&mut body, "loudness", ctx.inputs.opt::<f64>("loudness")?);
    // A whole number: the widget's step holds a wired value to whole
    // numbers too.
    set_some(&mut body, "seed", ctx.inputs.opt::<u64>("seed")?);
    set_some(&mut body, "prompt_strength", ctx.inputs.opt::<f64>("promptStrength")?);
    Ok(body)
}

/// Ask the voice preview route `route` under `/text-to-voice/` (`design`,
/// or `{voice_id}/remix`) for previews with `body`, store each one's
/// audio as `output_format`, and hand on `previews` and the `text` they
/// speak. A format with no file type is refused before anything is
/// sent.
pub async fn emit_previews(
    ctx: &RunContext,
    account: &Access,
    route: &str,
    body: &Value,
    output_format: &str,
    what: &str,
) -> WeftResult<()> {
    let (extension, mime) = audio_file_type(output_format)?;
    let http = ctx.client(account).await?;
    let url = format!("{API}/text-to-voice/{route}?output_format={}", urlencoding::encode(output_format));
    let answer = post_json(&http, &url, body, what).await?;

    let storage = ctx.storage(StorageScope::Run);
    let mut previews: Vec<Value> = Vec::new();
    for (n, p) in answer["previews"].as_array().into_iter().flatten().enumerate() {
        // A preview without its audio or its voice id is no preview:
        // emitting it would hand on an empty clip, or one Create Voice
        // cannot mint.
        let voice_id = p["generated_voice_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .node_err("ElevenLabs answered a preview with no generated_voice_id")?;
        let encoded = p["audio_base_64"]
            .as_str()
            .filter(|a| !a.is_empty())
            .node_err("ElevenLabs answered a preview with no audio_base_64")?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .node_err("ElevenLabs answered a preview whose audio is not base64")?;
        // Stored as the format that was asked for, never a type read
        // off the answer: everything downstream reads the stored type
        // as the truth about the bytes.
        let audio = storage.put(bytes, mime, &format!("preview-{}.{extension}", n + 1), None).await?;
        previews.push(json!({
            "voiceId": voice_id,
            "audio": audio,
            "durationSecs": p["duration_secs"],
            "language": p["language"],
        }));
    }
    if previews.is_empty() {
        weft::node_bail!("ElevenLabs answered no previews for this description");
    }

    let mut out = NodeOutput::new().set("previews", json!(previews));
    // The previews are already paid for, so an answer without its line
    // still hands them on, and `text` closes.
    if let Some(spoken) = answer["text"].as_str() {
        out = out.set("text", spoken);
    }
    ctx.pulse_downstream(out).await
}

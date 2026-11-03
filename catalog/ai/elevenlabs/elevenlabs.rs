//! What the ElevenLabs nodes share: the API base and the one ending every
//! batch audio node shares (a binary audio answer streamed into
//! storage, emitted as the stored-file quartet).

use serde::Deserialize;
use serde_json::{json, Value};

use weft::access::client::checked_send;
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::storage::{StorageScope, StoredFile};
use weft::{node_error, RunContext, NodeErrExt, WeftResult};

pub const API: &str = "https://api.elevenlabs.io/v1";
/// The same API over a websocket, for the live (streaming) nodes.
pub const WS_API: &str = "wss://api.elevenlabs.io/v1";

/// The stored file's extension and mime for a declared
/// `output_format` (`mp3_44100_128` -> `("mp3", "audio/mpeg")`).
/// Exhaustive over the format families the nodes declare (every
/// `outputFormat` is a closed select, so another family is a bug in this
/// package, never a program's mistake). The
/// `opus_*` formats come back in an Ogg container, the shape a
/// WhatsApp voice note takes, so they are stored as `.ogg` with the
/// codec on the mime.
pub fn audio_file_type(output_format: &str) -> WeftResult<(&'static str, &'static str)> {
    match output_format.split('_').next().unwrap_or_default() {
        "mp3" => Ok(("mp3", "audio/mpeg")),
        "pcm" => Ok(("pcm", "audio/pcm")),
        "wav" => Ok(("wav", "audio/wav")),
        "ulaw" => Ok(("ulaw", "audio/basic")),
        "alaw" => Ok(("alaw", "audio/PCMA")),
        "opus" => Ok(("ogg", "audio/ogg; codecs=opus")),
        _ => Err(node_error(format!("'{output_format}' is not an output format these nodes declare"))),
    }
}

/// One entity a transcription found, under weft's names, with its
/// character positions moved on by `shift`.
pub fn detected_entity(entity: &Value, shift: u64) -> WeftResult<Value> {
    let position = move |name: &str| {
        entity[name].as_u64().node_err(format!("ElevenLabs answered a detected entity with no {name}: {entity}"))
    };
    let text = move |name: &str| {
        entity[name].as_str().node_err(format!("ElevenLabs answered a detected entity with no {name}: {entity}"))
    };
    Ok(json!({
        "text": text("text")?,
        "entityType": text("entity_type")?,
        "startChar": position("start_char")? + shift,
        "endChar": position("end_char")? + shift,
    }))
}

/// Merge `over` onto `base`: two objects merge key by key, and anything
/// else in `over` replaces what `base` holds there. How a node's typed
/// inputs go on top of the raw ElevenLabs JSON it was also given, so the
/// typed input wins where both set the same key.
pub fn merge_json(base: &mut Value, over: Value) {
    match (base, over) {
        (Value::Object(base), Value::Object(over)) => {
            for (key, value) in over {
                match base.get_mut(&key) {
                    Some(slot) => merge_json(slot, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (slot, value) => *slot = value,
    }
}

/// What a node's `fileFormat` input says about the recording it sends.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum FileFormat {
    /// Any encoded audio or video: ElevenLabs's own default.
    #[serde(rename = "other")]
    Other,
    /// Raw 16-bit PCM at 16 kHz, mono, little-endian.
    #[serde(rename = "pcm_s16le_16")]
    PcmS16le16,
}

/// The `file_format` form field for a node's `fileFormat` input, or
/// `None` for `other`, ElevenLabs's own default, so nothing extra
/// travels. ElevenLabs takes `pcm_s16le_16` only for raw 16-bit PCM
/// at 16 kHz, mono, little-endian, so declaring it on a file whose
/// type says otherwise is refused before anything is sent.
pub fn input_file_format(ctx: &RunContext, file_mime: &str) -> WeftResult<Option<&'static str>> {
    match ctx.inputs.get::<FileFormat>("fileFormat")? {
        FileFormat::Other => Ok(None),
        FileFormat::PcmS16le16 if is_raw_pcm(file_mime) => Ok(Some("pcm_s16le_16")),
        FileFormat::PcmS16le16 => weft::input_bail!(
            "`fileFormat` is pcm_s16le_16, and the recording is typed '{file_mime}': ElevenLabs takes pcm_s16le_16 \
             only for raw 16-bit PCM at 16 kHz, mono, little-endian. Set `fileFormat` to `other`, or pass the raw PCM"
        ),
    }
}

/// Whether a file's type allows raw PCM: weft's own `audio/pcm`,
/// `audio/L16` (16-bit linear PCM's registered type), or an untyped
/// `application/octet-stream`.
pub fn is_raw_pcm(mime: &str) -> bool {
    let bare = mime.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    matches!(bare.as_str(), "audio/pcm" | "audio/l16" | "application/octet-stream")
}

/// Send a prepared request whose success body is audio, stream it into
/// run storage, and emit the stored-file quartet. `what` names
/// the attempt in errors; `filename` names the stored file, and `mime`
/// is the type the requested format really is. A refusal fails loud
/// quoting the provider (via `checked_send`).
///
/// The mime is passed rather than taken from the response header
/// because everything downstream reads it as the truth about the
/// bytes: the WhatsApp bridge picks its media kind from it, so an Ogg
/// Opus clip served under a generic type arrives as a document
/// instead of a voice note. The extension and the mime come from the
/// same [`audio_file_type`] row, so they cannot disagree.
pub async fn emit_audio(
    ctx: &RunContext,
    req: weft::reqwest_middleware::RequestBuilder,
    what: &str,
    filename: &str,
    mime: &str,
) -> WeftResult<()> {
    let resp = checked_send(req, what).await?;
    let stored = store_audio(ctx, resp, what, filename, mime).await?;
    ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
}

/// [`emit_audio`] for an answer that also names the generation in a
/// response header (`request-id` on speech, `song-id` on music): the
/// header's value goes out on the output `port` beside the file. The
/// audio is paid for and stored either way, so a missing header never
/// fails the node; only a program that reads `port` is told.
pub async fn emit_audio_with_id(
    ctx: &RunContext,
    req: weft::reqwest_middleware::RequestBuilder,
    what: &str,
    filename: &str,
    mime: &str,
    (header, port): (&str, &str),
) -> WeftResult<()> {
    let resp = checked_send(req, what).await?;
    // Read before the body is consumed.
    let id = resp.headers().get(header).and_then(|v| v.to_str().ok()).map(str::to_string);
    let stored = store_audio(ctx, resp, what, filename, mime).await?;
    let mut output = NodeOutput::stored_file(stored);
    match id {
        Some(id) => output = output.set(port, id),
        None if ctx.is_output_wired(port) => {
            ctx.log(LogLevel::Warn, format!("ElevenLabs answered without a {header} header, so `{port}` stays empty")).await?
        }
        None => {}
    }
    ctx.pulse_downstream(output).await
}

/// Stream an answer whose success body is audio into run
/// storage, as [`emit_audio`] does.
pub async fn store_audio(
    ctx: &RunContext,
    resp: weft::reqwest::Response,
    what: &str,
    filename: &str,
    mime: &str,
) -> WeftResult<StoredFile> {
    ctx.storage(StorageScope::Run)
        .put_response(resp, what, Some(mime), filename, None)
        .await
}

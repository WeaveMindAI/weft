//! Shared ElevenLabs plumbing: the API base and the one ending every
//! batch audio node shares (a binary audio answer streamed into
//! storage, emitted as the stored-file quartet).

use serde_json::{json, Value};

use weft::bus::StreamEnd;
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::storage::{StorageScope, StoredFile};
use weft::{node_error, RunContext, NodeErrExt, WeftError, WeftResult};

pub const API: &str = "https://api.elevenlabs.io/v1";
/// The same API over a websocket, for the live (streaming) nodes.
pub const WS_API: &str = "wss://api.elevenlabs.io/v1";

/// The stored file's extension and mime for a declared
/// `output_format` (`mp3_44100_128` -> `("mp3", "audio/mpeg")`).
/// Exhaustive over the format families the nodes declare; an unknown
/// family is refused rather than guessed into a wrong filename. The
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
        _ => Err(weft::WeftError::Input(format!(
            "'{output_format}' is not an output format these nodes declare"
        ))),
    }
}

/// The name of the recording a public link points at: the link's last
/// path segment (`talk.mp4`), or `fallback` when its path names nothing.
pub fn link_filename(link: &str, fallback: &str) -> String {
    let address = link.split(['?', '#']).next().unwrap_or_default();
    // Past the scheme, so the host is never taken for the file.
    let address = address.split_once("://").map_or(address, |(_, rest)| rest);
    address
        .split_once('/')
        .and_then(|(_, path)| path.rsplit('/').next())
        .filter(|segment| !segment.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

/// Store `value` as a JSON file of the run, and give back the file marker a port carries:
/// how a result that grows with the input (a transcript, timings, a
/// conversation) leaves a node, since a wire carries at most 100 KB.
pub async fn store_json(ctx: &RunContext, value: &Value, filename: &str) -> WeftResult<Value> {
    let bytes = serde_json::to_vec(value).node_err(format!("serialize {filename}"))?;
    ctx.storage(StorageScope::Run).put(bytes, "application/json", filename, None).await
}

/// An optional text input, with a blank value read as absent.
pub fn text_input(ctx: &RunContext, name: &str) -> WeftResult<Option<String>> {
    Ok(ctx.inputs.opt::<String>(name)?.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()))
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

/// The `file_format` form field for a node's `fileFormat` input, or
/// `None` for `other`, ElevenLabs's own default, so nothing extra
/// travels. ElevenLabs takes `pcm_s16le_16` only for raw 16-bit PCM
/// at 16 kHz, mono, little-endian, so declaring it on a file whose
/// type says otherwise is refused before anything is sent.
pub fn input_file_format(ctx: &RunContext, file_mime: &str) -> WeftResult<Option<&'static str>> {
    let format: String = ctx.inputs.get("fileFormat")?;
    match format.as_str() {
        "other" => Ok(None),
        "pcm_s16le_16" if is_raw_pcm(file_mime) => Ok(Some("pcm_s16le_16")),
        "pcm_s16le_16" => Err(WeftError::Input(format!(
            "`fileFormat` is pcm_s16le_16, and the recording is typed '{file_mime}': ElevenLabs takes pcm_s16le_16 \
             only for raw 16-bit PCM at 16 kHz, mono, little-endian. Set `fileFormat` to `other`, or pass the raw PCM"
        ))),
        other => weft::node_bail!("'{other}' is not a `fileFormat` ElevenLabs takes (`other` or `pcm_s16le_16`)"),
    }
}

/// A live input the node read to its end, refused when its bus ended
/// failed: its producer broke off, so what came in is not the whole
/// input, and speaking or transcribing it as whole would pass a cut
/// input off as complete.
pub fn refuse_broken_input(port: &str, end: Option<StreamEnd>) -> WeftResult<()> {
    match end {
        Some(StreamEnd::Failed { error }) => {
            Err(node_error(format!("the input on '{port}' broke off before it ended: {error}")))
        }
        Some(StreamEnd::Finished) => Ok(()),
        // A cursor runs out only on a closed bus, and every close says
        // how it ended: an input still open here is a bug in the reader.
        None => Err(node_error(format!("the input on '{port}' was read to its end while still open"))),
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
/// quoting the provider (via `put_response`).
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
    let resp = req.send().await.node_err(what)?;
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
    let resp = req.send().await.node_err(what)?;
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

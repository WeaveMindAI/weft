//! Shared ElevenLabs plumbing: the API base and the one ending every
//! batch audio node shares (a binary audio answer streamed into
//! storage, emitted as the stored-file quartet).

use weft::node::NodeOutput;
use weft::storage::{KeepTtl, StorageScope};
use weft::{ExecutionContext, NodeErrExt, WeftResult};

pub const API: &str = "https://api.elevenlabs.io/v1";

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
        "ulaw" => Ok(("ulaw", "audio/basic")),
        "opus" => Ok(("ogg", "audio/ogg; codecs=opus")),
        _ => weft::node_bail!("'{output_format}' is not an output format these nodes declare"),
    }
}

/// Send a prepared request whose success body is audio, stream it into
/// execution storage, and emit the stored-file quartet. `what` names
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
    ctx: &ExecutionContext,
    req: weft::reqwest_middleware::RequestBuilder,
    what: &str,
    filename: &str,
    mime: &str,
) -> WeftResult<()> {
    let resp = req.send().await.node_err(what)?;
    let stored = ctx
        .storage(StorageScope::Execution)
        // The generated audio is the run's product: keep it past the
        // run (default 30-day access-bumped TTL).
        .put_response(resp, what, Some(mime), filename, Some(KeepTtl::Default))
        .await?;
    ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
}

//! Shared ElevenLabs plumbing: the API base and the one ending every
//! batch audio node shares (a binary audio answer streamed into
//! storage, emitted as the stored-file quartet).

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{ExecutionContext, NodeErrExt, WeftResult};

pub const API: &str = "https://api.elevenlabs.io/v1";

/// The stored file's extension and mime for a declared
/// `output_format` (`mp3_44100_128` -> `("mp3", "audio/mpeg")`).
/// Exhaustive over the format families the nodes declare; an unknown
/// family is refused rather than guessed into a wrong filename.
pub fn audio_file_type(output_format: &str) -> WeftResult<(&'static str, &'static str)> {
    match output_format.split('_').next().unwrap_or_default() {
        "mp3" => Ok(("mp3", "audio/mpeg")),
        "pcm" => Ok(("pcm", "audio/pcm")),
        "ulaw" => Ok(("ulaw", "audio/basic")),
        _ => weft::node_bail!("'{output_format}' is not an output format these nodes declare"),
    }
}

/// Send a prepared request whose success body is audio, stream it into
/// execution storage, and emit the stored-file quartet. `what` names
/// the attempt in errors; `filename` names the stored file. A refusal
/// fails loud quoting the provider (via `put_response`).
pub async fn emit_audio(
    ctx: &ExecutionContext,
    req: weft::reqwest_middleware::RequestBuilder,
    what: &str,
    filename: &str,
) -> WeftResult<()> {
    let resp = req.send().await.node_err(what)?;
    let stored = ctx
        .storage(StorageScope::Execution)
        .put_response(resp, what, None, filename, None)
        .await?;
    ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
}

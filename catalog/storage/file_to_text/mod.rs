//! FileToText: read a stored text file and emit its content as a
//! String. Anything that is not text is refused loudly rather than
//! decoded into garbage: a picture, a sound or a video by its type, and
//! any other file by its bytes (not UTF-8, or carrying NUL bytes, which
//! no text has).

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::weft_type::FileKind;
use weft::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct FileToTextNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FileToTextNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        let (meta, bytes) = ctx.storage(weft::storage::StorageScope::Execution).get_bytes(&file).await?;
        let text = text_of(&meta.filename, &meta.mime_type, bytes.to_vec())?;
        ctx.pulse_downstream(NodeOutput::new().set("text", text)).await
    }
}

/// The content of a file as text, or the refusal naming why it is not.
pub fn text_of(filename: &str, mime_type: &str, bytes: Vec<u8>) -> WeftResult<String> {
    let kind = FileKind::from_mime(mime_type);
    if kind != FileKind::Blob {
        return Err(WeftError::Input(format!(
            "'{filename}' is {} ({mime_type}), not text; FileToText reads text files only",
            match kind {
                FileKind::Image => "an image",
                FileKind::Audio => "audio",
                FileKind::Video => "a video",
                FileKind::Blob => unreachable!("handled above"),
            }
        )));
    }
    let text = String::from_utf8(bytes).map_err(|e| {
        WeftError::Input(format!(
            "'{filename}' ({mime_type}) is not text: byte {} is not UTF-8; FileToText reads text files only",
            e.utf8_error().valid_up_to()
        ))
    })?;
    if let Some(at) = text.find('\0') {
        return Err(WeftError::Input(format!(
            "'{filename}' ({mime_type}) is binary, not text: it holds a NUL byte at {at}; \
             FileToText reads text files only"
        )));
    }
    Ok(text)
}

//! FileToText: read a stored text file and emit its content as a
//! String. Anything that is not text is refused loudly rather than
//! decoded into garbage: a picture, a sound or a video by its type, and
//! any other file by its bytes (not UTF-8, or carrying NUL bytes, which
//! no text has).

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::weft_type::FileKind;
use weft::{RunContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        // The text is parsed here, so the bytes are read whole.
        let (meta, bytes) = ctx.storage(weft::storage::StorageScope::Run).get_bytes(&file).await?;
        let text = text_of(&meta.filename, &meta.mime_type, bytes.to_vec())?;
        ctx.pulse_downstream(NodeOutput::new().set("text", text)).await
    }
}

/// The content of a file as text, or the refusal naming why it is not.
pub fn text_of(filename: &str, mime_type: &str, bytes: Vec<u8>) -> WeftResult<String> {
    let what = match FileKind::from_mime(mime_type) {
        FileKind::Blob => None,
        FileKind::Image => Some("an image"),
        FileKind::Audio => Some("audio"),
        FileKind::Video => Some("a video"),
    };
    if let Some(what) = what {
        weft::input_bail!("'{filename}' is {what} ({mime_type}), not text; FileToText reads text files only");
    }
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(e) => weft::input_bail!(
            "'{filename}' ({mime_type}) is not text: byte {} is not UTF-8; FileToText reads text files only",
            e.utf8_error().valid_up_to()
        ),
    };
    if let Some(at) = text.find('\0') {
        weft::input_bail!(
            "'{filename}' ({mime_type}) is binary, not text: it holds a NUL byte at {at}; \
             FileToText reads text files only"
        );
    }
    Ok(text)
}

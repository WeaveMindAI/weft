//! TelegramSendMedia: send a stored file to a chat as a photo,
//! document, voice note, video, or audio. The bytes go up as
//! multipart form data.

use async_trait::async_trait;

use weft::access::client::Multipart;
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

/// The API method + form field per media kind.
fn method_of(kind: &str) -> Option<(&'static str, &'static str)> {
    match kind {
        "photo" => Some(("sendPhoto", "photo")),
        "document" => Some(("sendDocument", "document")),
        "voice" => Some(("sendVoice", "voice")),
        "video" => Some(("sendVideo", "video")),
        "audio" => Some(("sendAudio", "audio")),
        _ => None,
    }
}

#[derive(NodeManifest)]
pub struct TelegramSendMediaNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TelegramSendMediaNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let kind: String = ctx.inputs.get("kind")?;
        let caption: Option<String> = ctx.inputs.opt("caption")?;

        let Some((method, field)) = method_of(&kind) else {
            weft::node_bail!(
                "unknown media kind '{kind}' (photo, document, voice, video, audio)"
            );
        };
        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&file).await?;

        let mut form = Multipart::form_data().text("chat_id", &chat_id);
        if let Some(c) = &caption {
            form = form.text("caption", c);
        }
        let (content_type, body) = form
            .file(field, &meta.filename, &meta.mime_type, bytes)
            .build();

        let answer = api::send_raw(&ctx, &access, method, &content_type, body).await?;
        let message_id = api::result_message_id(&answer, method)?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}

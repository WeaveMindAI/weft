//! TelegramSendMedia: send a stored file to a chat as a photo,
//! document, voice note, video, or audio. The bytes stream from
//! storage as multipart form data.

use async_trait::async_trait;
use serde::Deserialize;

use weft::access::client::{Multipart, StoredBodyExt};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::api;

/// The `kind` select: how Telegram shows the file.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MediaKind {
    Photo,
    Document,
    Voice,
    Video,
    Audio,
}

impl MediaKind {
    /// The API method and its form field.
    fn method(&self) -> (&'static str, &'static str) {
        match self {
            MediaKind::Photo => ("sendPhoto", "photo"),
            MediaKind::Document => ("sendDocument", "document"),
            MediaKind::Voice => ("sendVoice", "voice"),
            MediaKind::Video => ("sendVideo", "video"),
            MediaKind::Audio => ("sendAudio", "audio"),
        }
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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let kind: MediaKind = ctx.inputs.get("kind")?;
        let caption: Option<String> = ctx.inputs.opt("caption")?;

        let (method, field) = kind.method();
        let mut form = Multipart::form_data().text("chat_id", &chat_id);
        if let Some(c) = &caption {
            form = form.text("caption", c);
        }
        let form = form.stored(field, ctx.storage(StorageScope::Run).body(&file).await?);

        let answer = api::call(&ctx.client(&access).await?, method, |req| req.multipart(form)).await?;
        let message_id = api::result_message_id(&answer, method)?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}

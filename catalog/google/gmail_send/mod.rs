//! GmailSend: send mail as the connected account, with attachments
//! and reply-in-thread. Replying to a message id fetches its
//! Message-ID + thread and stamps In-Reply-To / References, so mail
//! clients thread it correctly.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::access::client::{get_json, required_str};
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::gmail::{b64url, build_mime, header, OutAttachment, OutMessage, API};

#[derive(NodeManifest)]
pub struct GmailSendNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GmailSendNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let to = weft::comma_list(ctx.inputs.list::<String>("to")?);
        let cc = weft::comma_list(ctx.inputs.list::<String>("cc")?);
        let bcc = weft::comma_list(ctx.inputs.list::<String>("bcc")?);
        let subject: String = ctx.inputs.get_or("subject", String::new())?;
        let text: Option<String> = ctx.inputs.opt("text")?;
        let html: Option<String> = ctx.inputs.opt("html")?;
        let reply_to: Option<String> = ctx.inputs.opt("replyTo")?;
        let handles: Vec<Value> = ctx.inputs.list("attachments")?;

        if to.is_empty() && cc.is_empty() && bcc.is_empty() {
            return Err(weft::WeftError::Input(
                "no recipient: provide to, cc, or bcc".to_string(),
            ));
        }
        // Gmail sends as the signed-in account; its address is the
        // connection's recorded identity, and the message must name it.
        let Some(from) = account.identity().map(str::to_string) else {
            weft::node_bail!(
                "gmail: the Google connection recorded no account address to send from; \
                 reconnect it on the access node"
            )
        };
        let http = ctx.client(&account).await?;

        // Reply threading: the original's Message-ID header + thread.
        let mut in_reply_to = None;
        let mut thread_id = None;
        if let Some(orig_id) = reply_to.filter(|r| !r.trim().is_empty()) {
            let orig: Value = get_json(
                &http,
                &format!(
                    "{API}/messages/{}?format=metadata&metadataHeaders=Message-ID",
                    super::api::segment(&orig_id)
                ),
                "gmail: read the replied-to message",
            )
            .await?;
            // No Message-ID means the reply cannot thread. Sending it
            // anyway looks like a success and lands as a loose message
            // in the recipient's inbox, so say what happened instead.
            let mid = header(&orig["payload"], "Message-ID").ok_or_else(|| {
                weft::node_error(format!(
                    "gmail: message {orig_id} carries no Message-ID header, so this reply \
                     cannot be threaded onto it. If the account connection was made before \
                     replies were used, reconnect it on the access node and tick \
                     'Read mail headers'; otherwise send this as a new message instead of \
                     a reply."
                ))
            })?;
            in_reply_to = Some(mid.to_string());
            thread_id = orig["threadId"].as_str().map(str::to_string);
        }

        // Attachments: one File or a list of Files, read from storage.
        let mut attachments = Vec::new();
        let storage = ctx.storage(StorageScope::Execution);
        for h in handles {
            let handle = FileHandle::from_value(&h)?;
            let (meta, bytes) = storage.get_bytes(&handle).await?;
            attachments.push(OutAttachment {
                filename: meta.filename,
                mime_type: meta.mime_type,
                bytes: bytes.to_vec(),
            });
        }

        let mime = build_mime(&OutMessage {
            from: &from,
            to: &to,
            cc: &cc,
            bcc: &bcc,
            subject: &subject,
            in_reply_to: in_reply_to.as_deref(),
            text: text.as_deref(),
            html: html.as_deref(),
            attachments: &attachments,
        })?;
        let mut body = json!({ "raw": b64url(&mime) });
        if let Some(t) = thread_id {
            body["threadId"] = json!(t);
        }
        let answer =
            weft::access::client::json_call(http.post(format!("{API}/messages/send")).json(&body), "send the mail")
                .await?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("id", required_str(&answer, "send the mail", "id")?.to_string())
                .set(
                    "threadId",
                    required_str(&answer, "send the mail", "threadId")?.to_string(),
                ),
        )
        .await
    }
}

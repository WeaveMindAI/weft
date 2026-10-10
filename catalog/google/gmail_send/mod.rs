//! GmailSend: send mail as the connected account (or one of its Send
//! mail as aliases), with attachments and reply-in-thread. Replying to
//! a message id fetches its Message-ID + thread and stamps In-Reply-To
//! / References, so mail clients thread it correctly. Both of those
//! read the mailbox, which Send mail alone cannot. Several Gmail
//! permissions can (Read mail, Organize mail, and Read mail headers for
//! a reply), and a requirement is one exact name, so the node asks for
//! none of them up front: Gmail refuses the read before anything is
//! sent, and the attempt's name says which permissions work.

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
        let reply_to = ctx.inputs.opt::<String>("replyTo")?.filter(|r| !r.trim().is_empty());
        let alias = ctx.inputs.opt::<String>("from")?.map(|a| a.trim().to_string()).filter(|a| !a.is_empty());
        let handles: Vec<Value> = ctx.inputs.list("attachments")?;

        if to.is_empty() && cc.is_empty() && bcc.is_empty() {
            return Err(weft::WeftError::Input(
                "no recipient: provide to, cc, or bcc".to_string(),
            ));
        }
        // Gmail sends as the signed-in account; its address is the
        // connection's recorded identity, and the message must name it.
        let Some(own) = account.identity().map(str::to_string) else {
            weft::node_bail!(
                "gmail: the Google connection recorded no account address to send from; \
                 reconnect it on the access node"
            )
        };
        let other_address = alias.as_ref().is_some_and(|a| !a.eq_ignore_ascii_case(&own));
        let http = ctx.client(&account).await?;
        let from = match alias {
            None => own,
            Some(alias) if other_address => {
                send_as_check(&http, &own, &alias).await?;
                alias
            }
            Some(alias) => alias,
        };

        // Reply threading: the original's Message-ID header + thread.
        let mut in_reply_to = None;
        let mut thread_id = None;
        if let Some(orig_id) = reply_to {
            let orig: Value = get_json(
                &http,
                &format!(
                    "{API}/messages/{}?format=metadata&metadataHeaders=Message-ID",
                    super::api::segment(&orig_id)
                ),
                "gmail: read the replied-to message (replying needs 'Read mail', \
                 'Organize mail' or 'Read mail headers' ticked on the Google access node)",
            )
            .await?;
            // No Message-ID means the reply cannot thread. Sending it
            // anyway looks like a success and lands as a loose message
            // in the recipient's inbox, so say what happened instead.
            let mid = header(&orig["payload"], "Message-ID").ok_or_else(|| {
                weft::node_error(format!(
                    "gmail: message {orig_id} carries no Message-ID header, so this reply \
                     cannot be threaded onto it; send this as a new message instead of a reply."
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

/// Gmail honours a From only when it is one of the account's Send mail
/// as addresses, and silently sends from the account's own address
/// otherwise. So a `from` that is not the account's own address is
/// checked against that list first, and a miss fails naming the
/// addresses that would work. Reading the list needs 'Read mail' or
/// 'Organize mail' on the connection (send-mail alone cannot).
async fn send_as_check(
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    own: &str,
    alias: &str,
) -> WeftResult<()> {
    // A connection with send-mail alone is refused here (403,
    // "insufficient authentication scopes"); the attempt's name says
    // which permission to tick so that refusal reads as its own fix.
    let answer = get_json(
        http,
        &format!("{API}/settings/sendAs"),
        "list the account's Send mail as addresses (sending from an alias needs \
         'Read mail' or 'Organize mail' ticked on the Google access node)",
    )
    .await?;
    // The primary address is on the list too. Gmail sets
    // `verificationStatus` only on a custom alias, and one still
    // "pending" is rewritten to the primary exactly like an unknown
    // address; an entry without the field (the primary, a Workspace
    // domain alias) is usable as is.
    let allowed: Vec<&str> = answer["sendAs"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter(|s| s["verificationStatus"].as_str().is_none_or(|v| v == "accepted"))
        .filter_map(|s| s["sendAsEmail"].as_str())
        .collect();
    if allowed.iter().any(|a| a.eq_ignore_ascii_case(alias)) {
        return Ok(());
    }
    Err(weft::WeftError::Input(format!(
        "gmail: '{alias}' is not one of this account's Send mail as addresses, so Gmail \
         would send from {own} instead. Allowed: {}. Add the address under Gmail's \
         Settings > Accounts > Send mail as, or pick one of these.",
        if allowed.is_empty() { own.to_string() } else { allowed.join(", ") }
    )))
}

//! SendEmail: send an email as the connected mailbox, over SMTP.
//! This body only gathers inputs, builds the message, and makes the
//! one submission; the connection's stored servers and credentials
//! say where and as whom.

use async_trait::async_trait;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SendEmailNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SendEmailNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let to: String = ctx.inputs.get("to")?;
        let subject: String = ctx.inputs.get("subject")?;
        let body: String = ctx.inputs.get("body")?;
        let cc: Option<String> = ctx.inputs.opt("cc")?;
        let bcc: Option<String> = ctx.inputs.opt("bcc")?;
        let reply_to: Option<String> = ctx.inputs.opt("replyToMessageId")?;

        let conn = ctx.open(&account).await?;
        let server = super::mailbox::smtp(&conn)?;

        let mut builder = Message::builder()
            .from(mailbox(&server.sender, "the connection's send-as address")?)
            .subject(subject);
        for addr in list(&to) {
            builder = builder.to(mailbox(addr, "a To address")?);
        }
        for addr in cc.as_deref().map(list).unwrap_or_default() {
            builder = builder.cc(mailbox(addr, "a Cc address")?);
        }
        for addr in bcc.as_deref().map(list).unwrap_or_default() {
            builder = builder.bcc(mailbox(addr, "a Bcc address")?);
        }
        if let Some(id) = reply_to.filter(|s| !s.trim().is_empty()) {
            builder = builder.in_reply_to(id.clone()).references(id);
        }
        let email = builder
            // `None` = mint a fresh Message-ID; read back below so
            // downstream can thread replies to what was just sent.
            .message_id(None)
            .body(body)
            .map_err(|e| weft::WeftError::Input(format!("building the email: {e}")))?;
        let message_id = email
            .headers()
            .get_raw("Message-ID")
            .unwrap_or_default()
            .to_string();

        // 587 is the STARTTLS submission port; everything else
        // (465, a custom port) speaks TLS from the first byte.
        let transport = if server.port == 587 {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&server.host)
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::relay(&server.host)
        }
        .map_err(|e| weft::WeftError::NodeExecution(format!("the SMTP server address is unusable: {e}")))?
        .port(server.port)
        .credentials(Credentials::new(server.user, server.password))
        .build();

        transport
            .send(email)
            .await
            .map_err(|e| weft::WeftError::NodeExecution(format!("the SMTP server refused the send: {e}")))?;

        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}

/// A comma-separated address list's non-empty entries.
fn list(s: &str) -> Vec<&str> {
    s.split(',').map(str::trim).filter(|a| !a.is_empty()).collect()
}

fn mailbox(addr: &str, what: &str) -> WeftResult<Mailbox> {
    addr.parse()
        .map_err(|e| weft::WeftError::Input(format!("{what} ('{addr}') is not a valid email address: {e}")))
}

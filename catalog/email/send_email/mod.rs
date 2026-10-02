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
        let to = weft::comma_list(ctx.inputs.list::<String>("to")?);
        let subject: String = ctx.inputs.get("subject")?;
        let body: String = ctx.inputs.get("body")?;
        let cc = weft::comma_list(ctx.inputs.list::<String>("cc")?);
        let bcc = weft::comma_list(ctx.inputs.list::<String>("bcc")?);
        let reply_to: Option<String> = ctx.inputs.opt("replyToMessageId")?;

        let conn = ctx.open(&account).await?;
        let server = super::mailbox::smtp(&conn)?;

        let mut builder = Message::builder()
            .from(mailbox(&server.sender, "the connection's send-as address")?)
            .subject(subject);
        for addr in &to {
            builder = builder.to(mailbox(addr, "a To address")?);
        }
        for addr in &cc {
            builder = builder.cc(mailbox(addr, "a Cc address")?);
        }
        for addr in &bcc {
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

        // 465 is the one implicit-TLS port (RFC 8314): TLS from the
        // first byte. Every other port (587 submission, 25 relay, 2525
        // and other custom ones) opens in plain text and upgrades with
        // STARTTLS, which lettre requires, so the session is never
        // left unencrypted.
        let transport = if server.port == IMPLICIT_TLS_PORT {
            AsyncSmtpTransport::<Tokio1Executor>::relay(&server.host)
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&server.host)
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

/// The SMTP port that speaks TLS from the first byte (RFC 8314).
const IMPLICIT_TLS_PORT: u16 = 465;

fn mailbox(addr: &str, what: &str) -> WeftResult<Mailbox> {
    addr.parse()
        .map_err(|e| weft::WeftError::Input(format!("{what} ('{addr}') is not a valid email address: {e}")))
}

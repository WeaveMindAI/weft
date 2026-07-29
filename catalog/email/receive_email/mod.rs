//! ReceiveEmail: fires when an email lands in the connected inbox.
//!
//!   - `setup_trigger`: register a held IMAP pipe. The language owns
//!     the connection (dial, sign in with the mailbox's stored
//!     credentials, keep the IDLE alive, reconnect on drop) from the
//!     declared dialogue below; the fire pattern is the server's
//!     new-mail announcement.
//!
//!   - `run`: the announcement says only "the inbox changed", so this
//!     body opens its own short IMAP session, reads every unseen
//!     message (which also marks them seen, the natural cursor),
//!     applies the from/subject filters, and fans the newest match
//!     (plus the full list on `messages` for bursts).

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;

use weft::signal::{Framing, SocketFrame, StreamListen};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ReceiveEmailNode;

#[async_trait]
impl Node for ReceiveEmailNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        // The IMAP watch dialogue: sign in, open the inbox, hold an
        // IDLE. The server announces arrivals as "* N EXISTS" (or
        // RECENT), which is the fire. The heartbeat re-issues the
        // IDLE inside the 29-minute window RFC 2177 allows.
        let watch = StreamListen::new(
            "{imap_host}:{imap_port}",
            Framing::Delimiter { bytes: "\r\n".into() },
            r"^\* \d+ (EXISTS|RECENT)",
        )
        .with_access(&account)
        .step(
            SocketFrame::Text { body: "a1 LOGIN \"{user}\" \"{password}\"\r\n".into() },
            "^a1 OK",
        )
        .step(SocketFrame::Text { body: "a2 SELECT INBOX\r\n".into() }, "^a2 OK")
        .step(SocketFrame::Text { body: "a3 IDLE\r\n".into() }, r"^\+")
        .with_heartbeat(
            SocketFrame::Text { body: "DONE\r\na9 IDLE\r\n".into() },
            25 * 60,
        );
        ctx.register_signal(watch).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let from_contains: Option<String> = ctx.inputs.opt("fromContains")?;
        let subject_contains: Option<String> = ctx.inputs.opt("subjectContains")?;
        let conn = ctx.open(&account).await?;

        let address = format!("{}:{}", conn.value("imap_host")?, conn.value("imap_port")?);
        let raw_messages = fetch_unseen(&address, conn.value("user")?, conn.value("password")?)
            .await
            .map_err(|e| weft::WeftError::NodeExecution(format!("reading the inbox over IMAP: {e}")))?;

        // Keep the ones passing the filters, oldest first.
        let mut messages: Vec<Value> = Vec::new();
        for raw in &raw_messages {
            let parsed = parse_message(raw);
            let from_ok = from_contains
                .as_deref()
                .filter(|f| !f.trim().is_empty())
                .map_or(true, |f| text_of(&parsed, "from").contains(f));
            let subject_ok = subject_contains
                .as_deref()
                .filter(|f| !f.trim().is_empty())
                .map_or(true, |f| text_of(&parsed, "subject").contains(f));
            if from_ok && subject_ok {
                messages.push(parsed);
            }
        }

        // Nothing new passed the filters: the execution ends with the
        // ports closed, which downstream reads as "nothing came".
        let Some(newest) = messages.last().cloned() else {
            return ctx.pulse_downstream(weft::node::NodeOutput::new().set("count", 0)).await;
        };
        let mut out = ctx.fan_declared(&newest);
        out = out
            .set("count", messages.len() as i64)
            .set("messages", Value::Array(messages));
        ctx.pulse_downstream(out).await
    }
}

/// Every unseen message's raw bytes, oldest first. Fetching also
/// marks them seen, which is the cursor: the next fire only reads
/// what arrived since.
pub async fn fetch_unseen(
    address: &str,
    user: &str,
    password: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let pipe = weft::net::tls_connect(address).await?;
    let mut client = async_imap::Client::new(pipe);
    // The server speaks first; its greeting must be consumed before
    // the first command or every later reply is read one answer off.
    match client.read_response().await {
        Some(Ok(_greeting)) => {}
        Some(Err(e)) => return Err(format!("reading the server's greeting: {e}")),
        None => return Err("the server closed the connection before greeting".into()),
    }
    let mut session = client
        .login(user, password)
        .await
        .map_err(|(e, _)| format!("the mailbox refused the sign-in: {e}"))?;
    let fetched = async {
        session
            .select("INBOX")
            .await
            .map_err(|e| format!("opening the inbox: {e}"))?;
        // UIDs, not sequence numbers: stable while the session runs,
        // so a concurrent delete cannot shift what gets fetched.
        let mut unseen: Vec<u32> = session
            .uid_search("UNSEEN")
            .await
            .map_err(|e| format!("listing unseen messages: {e}"))?
            .into_iter()
            .collect();
        unseen.sort_unstable();
        let mut messages = Vec::with_capacity(unseen.len());
        if !unseen.is_empty() {
            let set = unseen.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
            let mut fetch = session
                .uid_fetch(&set, "(RFC822)")
                .await
                .map_err(|e| format!("fetching the new messages: {e}"))?;
            while let Some(part) = fetch.next().await {
                let part = part.map_err(|e| format!("fetching the new messages: {e}"))?;
                if let Some(body) = part.body() {
                    messages.push(body.to_vec());
                }
            }
        }
        Ok(messages)
    }
    .await;
    // Say goodbye either way; the fetch result is the answer.
    let _ = session.logout().await;
    fetched
}

fn text_of(parsed: &Value, field: &str) -> String {
    parsed.get(field).and_then(Value::as_str).unwrap_or_default().to_string()
}

/// One raw RFC 822 message as this node's flat shape: the addressing
/// headers decoded, and the first plain-text body.
fn parse_message(raw: &[u8]) -> Value {
    let Some(message) = mail_parser::MessageParser::default().parse(raw) else {
        // An unparsable message still fired the trigger; surface what
        // little is knowable instead of dropping it silently.
        return serde_json::json!({
            "from": "", "to": "", "subject": "(unparsable message)",
            "body": String::from_utf8_lossy(raw), "messageId": "",
        });
    };
    serde_json::json!({
        "from": address_text(message.from()),
        "to": address_text(message.to()),
        "subject": message.subject().unwrap_or_default(),
        "body": message.body_text(0).unwrap_or_default(),
        "messageId": message.message_id().unwrap_or_default(),
    })
}

/// An address header as display text: `Name <box@host>` per mailbox,
/// comma-joined.
fn address_text(header: Option<&mail_parser::Address<'_>>) -> String {
    let Some(header) = header else { return String::new() };
    header
        .iter()
        .map(|a| match (a.name(), a.address()) {
            (Some(name), Some(addr)) => format!("{name} <{addr}>"),
            (None, Some(addr)) => addr.to_string(),
            (Some(name), None) => name.to_string(),
            (None, None) => String::new(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

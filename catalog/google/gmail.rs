//! Shared Gmail plumbing: the API base, base64url codecs, message
//! assembly (lettre, with attachments), and message-part walking.
//! Every gmail node speaks through these so the two directions
//! (build-and-send, fetch-and-decompose) stay exact inverses.

use base64::Engine as _;
use serde_json::Value;

use weft::WeftResult;

pub const API: &str = "https://gmail.googleapis.com/gmail/v1/users/me";

/// Gmail's raw-message encoding (URL-safe base64, no padding).
pub fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Decode a body-part's data (Gmail pads sometimes; accept both).
pub fn b64url_decode(data: &str) -> WeftResult<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(data)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(data))
        .map_err(|e| weft::node_error(format!("gmail: a body part is not base64url: {e}")))
}

/// A one-or-many label input, read and blank-filtered: an unfilled
/// text input arrives as "" and means "none given". Never split on
/// commas, since a Gmail label name may contain one (recipients go
/// through `weft::comma_list` instead).
pub fn non_blank_list(inputs: &weft::ValueBag, name: &str) -> WeftResult<Vec<String>> {
    Ok(inputs
        .list::<String>(name)?
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect())
}

/// One attachment ready for MIME assembly.
pub struct OutAttachment {
    pub filename: String,
    pub mime_type: String,
    pub bytes: Vec<u8>,
}

/// What one outgoing message carries, before assembly.
pub struct OutMessage<'a> {
    /// The sender: the account's own address or one of its Send mail
    /// as aliases (Gmail rewrites any other From to the account's own),
    /// and an RFC 5322 message must name it.
    pub from: &'a str,
    pub to: &'a [String],
    pub cc: &'a [String],
    pub bcc: &'a [String],
    pub subject: &'a str,
    /// The replied-to message's Message-ID, stamped as In-Reply-To
    /// and References so mail clients thread the reply.
    pub in_reply_to: Option<&'a str>,
    pub text: Option<&'a str>,
    pub html: Option<&'a str>,
    pub attachments: &'a [OutAttachment],
}

/// Assemble the RFC 5322 message through lettre's builder, which
/// encodes non-ASCII headers and filenames (RFC 2047 / 2231), picks
/// each body's transfer encoding, mints collision-free multipart
/// boundaries, and refuses CRLF in header values. Text and/or html
/// become the body (both = multipart/alternative); attachments wrap
/// everything in multipart/mixed. Bcc is KEPT in the raw message:
/// Gmail reads it to deliver, then strips it from what recipients see.
pub fn build_mime(m: &OutMessage<'_>) -> WeftResult<Vec<u8>> {
    use lettre::message::header::ContentType;
    use lettre::message::{Attachment, Mailbox, MultiPart, SinglePart};

    fn mailbox(addr: &str, what: &str) -> WeftResult<Mailbox> {
        addr.parse().map_err(|e| {
            weft::WeftError::Input(format!("{what} ('{addr}') is not a valid email address: {e}"))
        })
    }

    let mut builder = lettre::Message::builder()
        .from(mailbox(m.from, "the From address")?)
        .subject(m.subject)
        .keep_bcc();
    for addr in m.to {
        builder = builder.to(mailbox(addr, "a To address")?);
    }
    for addr in m.cc {
        builder = builder.cc(mailbox(addr, "a Cc address")?);
    }
    for addr in m.bcc {
        builder = builder.bcc(mailbox(addr, "a Bcc address")?);
    }
    if let Some(id) = m.in_reply_to {
        builder = builder.in_reply_to(id.to_string()).references(id.to_string());
    }

    // The body: one part, or a plain/html alternative pair.
    enum Body {
        One(SinglePart),
        Alternative(MultiPart),
    }
    let body = match (m.text, m.html) {
        (Some(t), None) => Body::One(SinglePart::plain(t.to_string())),
        (None, Some(h)) => Body::One(SinglePart::html(h.to_string())),
        (Some(t), Some(h)) => {
            Body::Alternative(MultiPart::alternative_plain_html(t.to_string(), h.to_string()))
        }
        (None, None) => {
            return Err(weft::WeftError::Input(
                "nothing to send: provide text, html, or both".to_string(),
            ))
        }
    };
    let message = if m.attachments.is_empty() {
        match body {
            Body::One(part) => builder.singlepart(part),
            Body::Alternative(parts) => builder.multipart(parts),
        }
    } else {
        let mut mixed = match body {
            Body::One(part) => MultiPart::mixed().singlepart(part),
            Body::Alternative(parts) => MultiPart::mixed().multipart(parts),
        };
        for a in m.attachments {
            let content_type = ContentType::parse(&a.mime_type).map_err(|e| {
                weft::WeftError::Input(format!(
                    "attachment '{}' has an unusable content type '{}': {e}",
                    a.filename, a.mime_type
                ))
            })?;
            mixed = mixed.singlepart(
                Attachment::new(a.filename.clone()).body(a.bytes.clone(), content_type),
            );
        }
        builder.multipart(mixed)
    }
    .map_err(|e| weft::WeftError::Input(format!("building the email: {e}")))?;
    Ok(message.formatted())
}

/// A named header out of a message's payload (case-insensitive).
pub fn header<'a>(payload: &'a Value, name: &str) -> Option<&'a str> {
    payload["headers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|h| {
            h["name"]
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
        .and_then(|h| h["value"].as_str())
}

/// Walk a message payload's part tree, calling `visit` on every leaf.
pub fn walk_parts<'a>(payload: &'a Value, visit: &mut dyn FnMut(&'a Value)) {
    match payload["parts"].as_array() {
        Some(parts) => {
            for p in parts {
                walk_parts(p, visit);
            }
        }
        None => visit(payload),
    }
}

/// The message's best-effort text body leaf: the first text/plain
/// leaf, else the first text/html leaf, with whether it is html.
pub fn body_leaf(payload: &Value) -> Option<(&Value, bool)> {
    let mut plain: Option<&Value> = None;
    let mut html: Option<&Value> = None;
    walk_parts(payload, &mut |leaf| {
        let mime = leaf["mimeType"].as_str().unwrap_or_default();
        if mime.starts_with("text/plain") && plain.is_none() {
            plain = Some(leaf);
        }
        if mime.starts_with("text/html") && html.is_none() {
            html = Some(leaf);
        }
    });
    match (plain, html) {
        (Some(p), _) => Some((p, false)),
        (None, Some(h)) => Some((h, true)),
        (None, None) => None,
    }
}

/// One leaf part's bytes. A small part carries them inline as
/// `body.data`; a large one (a big body or an attachment) carries an
/// `attachmentId` instead, fetched through the attachments endpoint.
/// `None` when the part has neither: Gmail sends that only for an
/// empty part.
async fn part_bytes(
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    message_id: &str,
    leaf: &Value,
    what: &str,
) -> WeftResult<Option<Vec<u8>>> {
    use weft::access::client::get_json;
    match (
        leaf.pointer("/body/data").and_then(Value::as_str),
        leaf.pointer("/body/attachmentId").and_then(Value::as_str),
    ) {
        (Some(data), _) => Ok(Some(b64url_decode(data)?)),
        (None, Some(att_id)) => {
            let att: Value = get_json(
                http,
                &format!(
                    "{API}/messages/{}/attachments/{}",
                    super::api::segment(message_id),
                    super::api::segment(att_id)
                ),
                &format!("gmail: read {what}"),
            )
            .await?;
            let Some(data) = att["data"].as_str() else {
                weft::node_bail!(
                    "gmail: {what} of message {message_id} came back with no data \
                     (attachment {att_id})"
                )
            };
            Ok(Some(b64url_decode(data)?))
        }
        (None, None) => Ok(None),
    }
}

/// A fetched message, decomposed for node outputs.
pub struct ReadMessage {
    pub msg: Value,
    pub body: String,
    pub body_is_html: bool,
    /// Stored-file values, one per attachment (empty when not asked).
    pub files: Vec<Value>,
}

impl ReadMessage {
    /// The shared output block both message-reading nodes emit (the
    /// get node as-is, the new-email trigger with its `id` appended):
    /// ONE definition, so the two shapes cannot drift.
    pub fn into_output(self) -> weft::node::NodeOutput {
        let payload = &self.msg["payload"];
        weft::node::NodeOutput::new()
            .set("subject", header(payload, "Subject").unwrap_or_default().to_string())
            .set("from", header(payload, "From").unwrap_or_default().to_string())
            .set("to", header(payload, "To").unwrap_or_default().to_string())
            .set("date", header(payload, "Date").unwrap_or_default().to_string())
            .set("body", self.body)
            .set("bodyIsHtml", self.body_is_html)
            .set("threadId", self.msg["threadId"].as_str().unwrap_or_default().to_string())
            .set("snippet", self.msg["snippet"].as_str().unwrap_or_default().to_string())
            .set("labels", self.msg["labelIds"].clone())
            .set("attachments", Value::Array(self.files))
    }
}

/// Fetch one message (format=full) and decompose it: the best text
/// body, and (optionally) every attachment pulled into execution
/// storage. Shared by the get node and the new-email trigger so the
/// two emit identical shapes.
pub async fn read_message(
    ctx: &weft::ExecutionContext,
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    id: &str,
    include_attachments: bool,
) -> WeftResult<ReadMessage> {
    use weft::access::client::get_json;
    let msg: Value = get_json(
        http,
        &format!("{API}/messages/{}?format=full", super::api::segment(id)),
        "gmail: read the message",
    )
    .await?;
    let payload = &msg["payload"];
    let (body, body_is_html) = match body_leaf(payload) {
        Some((leaf, is_html)) => {
            // No bytes at all is Gmail's shape for an empty part.
            let bytes = part_bytes(http, id, leaf, "the message body").await?.unwrap_or_default();
            (String::from_utf8_lossy(&bytes).into_owned(), is_html)
        }
        None => (String::new(), false),
    };

    let mut files = Vec::new();
    if include_attachments {
        // Attachment leaves carry a filename + either inline data or
        // an attachmentId to fetch separately.
        let mut leaves = Vec::new();
        walk_parts(payload, &mut |leaf| leaves.push(leaf.clone()));
        let storage = ctx.storage(weft::storage::StorageScope::Execution);
        for leaf in leaves {
            let filename = leaf["filename"].as_str().unwrap_or_default();
            if filename.is_empty() {
                continue;
            }
            let mime = leaf["mimeType"].as_str().unwrap_or("application/octet-stream");
            let Some(bytes) = part_bytes(http, id, &leaf, &format!("attachment '{filename}'")).await?
            else {
                continue;
            };
            // Attachments are content the workflow acts on and the
            // editor previews after the run: keep them past the run
            // (default 30-day access-bumped TTL).
            files.push(
                storage
                    .put(bytes, mime, filename, Some(weft::storage::KeepTtl::Default))
                    .await?,
            );
        }
    }
    Ok(ReadMessage { msg, body, body_is_html, files })
}

// This file is a package-level SHARED helper, not a node, so its unit
// tests stay an ordinary `#[cfg(test)]` block (node self-tests in a
// `tests.rs` belong to nodes; see docs/src/nodes/testing.md).
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message<'a>(
        to: &'a [String],
        subject: &'a str,
        text: Option<&'a str>,
        html: Option<&'a str>,
        attachments: &'a [OutAttachment],
    ) -> OutMessage<'a> {
        OutMessage {
            from: "me@x.com",
            to,
            cc: &[],
            bcc: &[],
            subject,
            in_reply_to: None,
            text,
            html,
            attachments,
        }
    }

    #[test]
    fn mime_carries_the_essentials() {
        let to = ["a@x.com".to_string()];
        let mut m = message(&to, "Hi", Some("hello"), None, &[]);
        m.in_reply_to = Some("<m1@x>");
        let text = String::from_utf8(build_mime(&m).unwrap()).unwrap();
        assert!(text.contains("From: me@x.com\r\n"), "{text}");
        assert!(text.contains("To: a@x.com\r\n"), "{text}");
        assert!(text.contains("Subject: Hi\r\n"), "{text}");
        assert!(text.contains("In-Reply-To: <m1@x>\r\n"), "{text}");
        assert!(text.contains("References: <m1@x>\r\n"), "{text}");
        assert!(text.contains("hello"), "{text}");
    }

    /// A non-ASCII subject is RFC 2047 encoded, so no raw UTF-8 lands
    /// in the header block.
    #[test]
    fn a_non_ascii_subject_is_encoded() {
        let to = ["a@x.com".to_string()];
        let m = message(&to, "Café déjà vu", Some("hello"), None, &[]);
        let text = String::from_utf8(build_mime(&m).unwrap()).unwrap();
        let subject = text.lines().find(|l| l.starts_with("Subject: ")).expect("subject");
        assert!(subject.is_ascii(), "{subject}");
        assert!(subject.contains("=?utf-8?"), "{subject}");
    }

    /// Bcc stays in the raw message: Gmail delivers from it.
    #[test]
    fn bcc_is_kept_for_gmail_to_deliver() {
        let to = ["a@x.com".to_string()];
        let bcc = ["hidden@x.com".to_string()];
        let mut m = message(&to, "s", Some("t"), None, &[]);
        m.bcc = &bcc;
        let text = String::from_utf8(build_mime(&m).unwrap()).unwrap();
        assert!(text.contains("Bcc: hidden@x.com\r\n"), "{text}");
    }

    #[test]
    fn an_invalid_address_is_refused() {
        let to = ["not an address".to_string()];
        let err = build_mime(&message(&to, "s", Some("t"), None, &[])).unwrap_err().to_string();
        assert!(err.contains("not an address"), "{err}");
    }

    #[test]
    fn nothing_to_send_is_refused() {
        let to = ["a@x.com".to_string()];
        let err = build_mime(&message(&to, "s", None, None, &[])).unwrap_err().to_string();
        assert!(err.contains("nothing to send"), "{err}");
    }

    #[test]
    fn attachments_wrap_in_multipart_mixed() {
        let to = ["a@x.com".to_string()];
        let attachments = [OutAttachment {
            filename: "a.txt".into(),
            mime_type: "text/plain".into(),
            bytes: b"data".to_vec(),
        }];
        let m = message(&to, "s", Some("body"), Some("<b>body</b>"), &attachments);
        let text = String::from_utf8(build_mime(&m).unwrap()).unwrap();
        assert!(text.contains("multipart/mixed"), "{text}");
        assert!(text.contains("multipart/alternative"), "{text}");
        assert!(text.contains("a.txt"), "{text}");
    }

    #[test]
    fn body_extraction_prefers_plain() {
        let payload = json!({
            "mimeType": "multipart/alternative",
            "parts": [
                { "mimeType": "text/html",
                  "body": { "data": b64url(b"<b>hi</b>") } },
                { "mimeType": "text/plain",
                  "body": { "data": b64url(b"hi") } }
            ]
        });
        let (leaf, is_html) = body_leaf(&payload).expect("a body leaf");
        assert!(!is_html);
        let data = leaf.pointer("/body/data").and_then(Value::as_str).unwrap();
        assert_eq!(b64url_decode(data).unwrap(), b"hi");
    }
}

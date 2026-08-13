//! Shared Gmail plumbing: the API base, base64url codecs, RFC 2822
//! message assembly (with attachments), and message-part walking.
//! Every gmail node speaks through these so the two directions
//! (build-and-send, fetch-and-decompose) stay exact inverses.

use base64::Engine as _;
use serde_json::Value;

use weft::{node_bail, WeftResult};

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

/// A one-or-many recipient/label input, read and blank-filtered: an
/// unfilled text input arrives as "" and means "none given".
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

/// Assemble the RFC 2822 message. Text and/or html become the body
/// (both = multipart/alternative); attachments wrap everything in
/// multipart/mixed. Header VALUES are sanitized against CRLF
/// injection (a subject with a newline must not smuggle headers).
pub fn build_mime(
    to: &[String],
    cc: &[String],
    bcc: &[String],
    subject: &str,
    reply_headers: &[(String, String)],
    text: Option<&str>,
    html: Option<&str>,
    attachments: &[OutAttachment],
) -> WeftResult<Vec<u8>> {
    fn header_value(v: &str) -> String {
        v.replace(['\r', '\n'], " ")
    }
    if text.is_none() && html.is_none() {
        node_bail!("nothing to send: provide text, html, or both");
    }
    let mut head = String::new();
    if !to.is_empty() {
        head.push_str(&format!("To: {}\r\n", header_value(&to.join(", "))));
    }
    if !cc.is_empty() {
        head.push_str(&format!("Cc: {}\r\n", header_value(&cc.join(", "))));
    }
    if !bcc.is_empty() {
        head.push_str(&format!("Bcc: {}\r\n", header_value(&bcc.join(", "))));
    }
    head.push_str(&format!("Subject: {}\r\n", header_value(subject)));
    for (name, value) in reply_headers {
        head.push_str(&format!("{}: {}\r\n", header_value(name), header_value(value)));
    }
    head.push_str("MIME-Version: 1.0\r\n");

    // The body block: one part, or multipart/alternative for both.
    // Boundaries are minted per message and collision-scanned against
    // the exact content they will delimit: a body that happens to
    // contain the boundary marker would otherwise truncate the MIME
    // structure at that point (the recipient sees a mangled mail and
    // everything after the marker vanishes).
    let body_block = match (text, html) {
        (Some(t), None) => part("text/plain; charset=\"UTF-8\"", t),
        (None, Some(h)) => part("text/html; charset=\"UTF-8\"", h),
        (Some(t), Some(h)) => {
            let boundary = mint_boundary("weft-alt", &[t.as_bytes(), h.as_bytes()]);
            let mut b = format!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n\r\n"
            );
            for (ct, body) in [
                ("text/plain; charset=\"UTF-8\"", t),
                ("text/html; charset=\"UTF-8\"", h),
            ] {
                b.push_str(&format!("--{boundary}\r\n"));
                b.push_str(&part(ct, body));
            }
            b.push_str(&format!("--{boundary}--\r\n"));
            b
        }
        (None, None) => unreachable!("checked above"),
    };

    let message = if attachments.is_empty() {
        format!("{head}{body_block}")
    } else {
        // Scan the assembled body block plus every attachment header
        // string; the base64 payload cannot collide (its alphabet has
        // no `-`), so it stays out of the haystack.
        let mut hay: Vec<&[u8]> = vec![body_block.as_bytes()];
        for a in attachments {
            hay.push(a.filename.as_bytes());
            hay.push(a.mime_type.as_bytes());
        }
        let boundary = mint_boundary("weft-mixed", &hay);
        let mut m = format!(
            "{head}Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\r\n\
             --{boundary}\r\n{body_block}"
        );
        for a in attachments {
            m.push_str(&format!("--{boundary}\r\n"));
            let name = header_value(&a.filename).replace('"', "'");
            m.push_str(&format!(
                "Content-Type: {}; name=\"{name}\"\r\n\
                 Content-Disposition: attachment; filename=\"{name}\"\r\n\
                 Content-Transfer-Encoding: base64\r\n\r\n",
                header_value(&a.mime_type),
            ));
            // 76-char lines per RFC 2045.
            let encoded = base64::engine::general_purpose::STANDARD.encode(&a.bytes);
            for chunk in encoded.as_bytes().chunks(76) {
                m.push_str(std::str::from_utf8(chunk).expect("base64 is ascii"));
                m.push_str("\r\n");
            }
        }
        m.push_str(&format!("--{boundary}--\r\n"));
        m
    };
    Ok(message.into_bytes())
}

/// A fresh multipart boundary whose bare `--<boundary>` marker appears
/// in none of `hay`. A v4 uuid collides only by deliberate
/// construction, and the scan makes even that exact rather than
/// probabilistic (a hit re-mints).
fn mint_boundary(prefix: &str, hay: &[&[u8]]) -> String {
    loop {
        let boundary = format!("{prefix}-{}", uuid::Uuid::new_v4().simple());
        let marker = format!("--{boundary}");
        let clash = hay
            .iter()
            .any(|h| h.windows(marker.len()).any(|w| w == marker.as_bytes()));
        if !clash {
            return boundary;
        }
    }
}

/// One simple MIME part (headers + a plain text body). Attachments
/// base64 their bytes inline at their own call site; they never come
/// through here.
fn part(content_type: &str, body: &str) -> String {
    format!("Content-Type: {content_type}\r\n\r\n{body}\r\n")
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

/// The message's best-effort text body: the first text/plain leaf,
/// else the first text/html leaf (returned as-is; the caller knows
/// which it got from the `html` flag).
pub fn body_of(payload: &Value) -> WeftResult<(String, bool)> {
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
    let (leaf, is_html) = match (plain, html) {
        (Some(p), _) => (p, false),
        (None, Some(h)) => (h, true),
        (None, None) => return Ok((String::new(), false)),
    };
    let data = leaf.pointer("/body/data").and_then(Value::as_str).unwrap_or_default();
    let bytes = b64url_decode(data)?;
    Ok((String::from_utf8_lossy(&bytes).into_owned(), is_html))
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
        &format!("{API}/messages/{id}?format=full"),
        "gmail: read the message",
    )
    .await?;
    let payload = &msg["payload"];
    let (body, body_is_html) = body_of(payload)?;

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
            let bytes = match (
                leaf.pointer("/body/data").and_then(Value::as_str),
                leaf.pointer("/body/attachmentId").and_then(Value::as_str),
            ) {
                (Some(data), _) => b64url_decode(data)?,
                (None, Some(att_id)) => {
                    let att: Value = get_json(
                        http,
                        &format!("{API}/messages/{id}/attachments/{att_id}"),
                        "gmail: read an attachment",
                    )
                    .await?;
                    b64url_decode(att["data"].as_str().unwrap_or_default())?
                }
                (None, None) => continue,
            };
            files.push(storage.put(bytes, mime, filename, None).await?);
        }
    }
    Ok(ReadMessage { msg, body, body_is_html, files })
}

// This file is a package-level SHARED helper, not a node, so its unit
// tests stay an ordinary `#[cfg(test)]` block (node self-tests in a
// `tests.rs` belong to nodes; see weft/docs/node-tests.md).
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mime_round_trips_the_essentials() {
        let msg = build_mime(
            &["a@x.com".into()],
            &[],
            &[],
            "Hi\r\nX-Evil: injected",
            &[("In-Reply-To".into(), "<m1@x>".into())],
            Some("hello"),
            None,
            &[],
        )
        .unwrap();
        let text = String::from_utf8(msg).unwrap();
        assert!(text.contains("To: a@x.com\r\n"), "{text}");
        assert!(text.contains("Subject: Hi  X-Evil: injected\r\n"), "header injection folded");
        assert!(text.contains("In-Reply-To: <m1@x>\r\n"));
        assert!(text.ends_with("hello\r\n"));
    }

    #[test]
    fn attachments_wrap_in_multipart_mixed() {
        let msg = build_mime(
            &["a@x.com".into()],
            &[],
            &[],
            "s",
            &[],
            Some("body"),
            None,
            &[OutAttachment {
                filename: "a.txt".into(),
                mime_type: "text/plain".into(),
                bytes: b"data".to_vec(),
            }],
        )
        .unwrap();
        let text = String::from_utf8(msg).unwrap();
        assert!(text.contains("multipart/mixed"));
        assert!(text.contains("filename=\"a.txt\""));
        assert!(text.contains(&base64::engine::general_purpose::STANDARD.encode("data")));
    }

    #[test]
    fn body_extraction_prefers_plain_and_decodes() {
        let payload = json!({
            "mimeType": "multipart/alternative",
            "parts": [
                { "mimeType": "text/html",
                  "body": { "data": b64url(b"<b>hi</b>") } },
                { "mimeType": "text/plain",
                  "body": { "data": b64url(b"hi") } }
            ]
        });
        let (text, is_html) = body_of(&payload).unwrap();
        assert_eq!(text, "hi");
        assert!(!is_html);
    }

    /// A body that contains a plausible boundary literal never breaks
    /// the MIME structure: the minted boundary is scanned against the
    /// content and re-minted on a hit, so the assembled message's
    /// boundary appears only where the structure puts it.
    #[test]
    fn boundaries_never_collide_with_the_content() {
        let b = mint_boundary("weft-alt", &[b"innocent"]);
        assert!(b.starts_with("weft-alt-"));

        let msg = build_mime(
            &["a@x.com".into()],
            &[],
            &[],
            "s",
            &[],
            Some("text with --weft-mixed-8d1e4b and --weft-alt-3f9c2a inside"),
            Some("<b>html</b>"),
            &[OutAttachment {
                filename: "a.txt".into(),
                mime_type: "text/plain".into(),
                bytes: b"data".to_vec(),
            }],
        )
        .unwrap();
        let text = String::from_utf8(msg).unwrap();
        let boundary = text
            .split("multipart/mixed; boundary=\"")
            .nth(1)
            .and_then(|r| r.split('"').next())
            .expect("mixed boundary");
        // The marker appears exactly where the structure uses it: two
        // part openers + one terminator, never inside the body text.
        assert_eq!(text.matches(&format!("--{boundary}")).count(), 3, "{text}");
    }
}

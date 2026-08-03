//! Email Send Node - Send emails via SMTP
//!
//! Supports optional media attachments from Image/Video/Audio/Document nodes.

use async_trait::async_trait;
use lettre::message::{header::{ContentType, InReplyTo, References}, Mailbox, MessageBuilder, MultiPart, SinglePart, Attachment};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

use crate::node::{ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef, FieldDef};
use crate::{register_node, NodeResult};

#[derive(Default)]
pub struct EmailSendNode;

#[async_trait]
impl Node for EmailSendNode {
    fn node_type(&self) -> &'static str {
        "EmailSend"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Email Send",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("to", "List[String]", true),
                PortDef::new("subject", "String", true),
                PortDef::new("body", "String", true),
                PortDef::new("cc", "List[String]", false),
                PortDef::new("bcc", "List[String]", false),
                PortDef::new("replyTo", "String", false),
                PortDef::new("inReplyTo", "String", false),
                PortDef::new("references", "List[String]", false),
                PortDef::new("media", "Media", false),
            ],
            outputs: vec![
                PortDef::new("success", "Boolean", false),
                PortDef::new("messageId", "String", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("fromEmail"),
                FieldDef::checkbox("html"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let config_input = ctx.input.get("config")
            .and_then(|v| v.as_object())
            .map(|o| serde_json::Value::Object(o.clone()))
            .unwrap_or_default();

        let smtp_host = config_input.get("host").and_then(|v| v.as_str())
            .unwrap_or("");
        let smtp_port: u16 = config_input.get("port")
            .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64().map(|n| n as u16)))
            .unwrap_or(587);
        let username = config_input.get("username").and_then(|v| v.as_str())
            .unwrap_or("");
        let password = config_input.get("password").and_then(|v| v.as_str())
            .unwrap_or("");
        let from_email_raw = ctx.config.get("fromEmail")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let from_email = if from_email_raw.trim().is_empty() { username } else { from_email_raw.trim() };
        tracing::info!("EmailSend: username={} fromEmail config={:?} effective from={}", username, ctx.config.get("fromEmail"), from_email);
        let use_html = ctx.config.get("html")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let to_addrs = ctx.input_string_list("to");
        let subject = ctx.input.get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let body = ctx.input.get("body")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let cc_addrs = ctx.input_string_list("cc");
        let bcc_addrs = ctx.input_string_list("bcc");
        let reply_to = ctx.input.get("replyTo")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let in_reply_to = ctx.input.get("inReplyTo")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let references_list = ctx.input_string_list("references");

        if smtp_host.is_empty() {
            return NodeResult::failed("SMTP host is required in node config");
        }
        if username.is_empty() || password.is_empty() {
            return NodeResult::failed("SMTP username and password are required in node config");
        }
        if to_addrs.is_empty() {
            return NodeResult::failed("Recipient (to) is required");
        }

        let from_mailbox: Mailbox = match from_email.parse() {
            Ok(m) => m,
            Err(e) => return NodeResult::failed(&format!("Invalid from email '{}': {}", from_email, e)),
        };

        let mut builder: MessageBuilder = lettre::Message::builder()
            .from(from_mailbox)
            .subject(subject);

        // When sending as an alias (fromEmail != username), set Sender to the authenticated
        // SMTP account. Gmail requires this to respect the From header for aliases.
        if from_email != username && !username.is_empty() {
            if let Ok(sender_mailbox) = username.parse::<Mailbox>() {
                builder = builder.sender(sender_mailbox);
            }
        }

        for addr in &to_addrs {
            match addr.parse() {
                Ok(m) => builder = builder.to(m),
                Err(e) => return NodeResult::failed(&format!("Invalid to address '{}': {}", addr, e)),
            }
        }

        for addr in &cc_addrs {
            match addr.parse() {
                Ok(m) => builder = builder.cc(m),
                Err(e) => return NodeResult::failed(&format!("Invalid cc address '{}': {}", addr, e)),
            }
        }

        for addr in &bcc_addrs {
            match addr.parse() {
                Ok(m) => builder = builder.bcc(m),
                Err(e) => return NodeResult::failed(&format!("Invalid bcc address '{}': {}", addr, e)),
            }
        }

        if !reply_to.is_empty() {
            match reply_to.parse() {
                Ok(m) => builder = builder.reply_to(m),
                Err(e) => return NodeResult::failed(&format!("Invalid reply-to address '{}': {}", reply_to, e)),
            }
        }

        // Threading headers: In-Reply-To and References
        // Message IDs should be in angle-bracket format: <id@domain>
        if !in_reply_to.is_empty() {
            let formatted = if in_reply_to.starts_with('<') { in_reply_to.to_string() } else { format!("<{}>", in_reply_to) };
            builder = builder.header(InReplyTo::from(formatted));
        }
        if !references_list.is_empty() {
            let formatted: Vec<String> = references_list.iter()
                .map(|id| if id.starts_with('<') { id.clone() } else { format!("<{}>", id) })
                .collect();
            builder = builder.header(References::from(formatted.join(" ")));
        }

        let content_type = if use_html {
            ContentType::TEXT_HTML
        } else {
            ContentType::TEXT_PLAIN
        };

        // Check if media attachment is provided
        let media = ctx.input.get("media")
            .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());

        let message = if let Some(media_obj) = media {
            let media_url = media_obj.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let mimetype = media_obj.get("mimeType").and_then(|v| v.as_str()).unwrap_or("application/octet-stream");
            let filename = media_obj.get("filename").and_then(|v| v.as_str()).unwrap_or("attachment");

            if media_url.is_empty() {
                // No URL, send without attachment
                match builder.header(content_type).body(body.to_string()) {
                    Ok(m) => m,
                    Err(e) => return NodeResult::failed(&format!("Failed to build email: {}", e)),
                }
            } else {
                // Download the file
                let http_client = reqwest::Client::new();
                let file_bytes = match http_client.get(media_url)
                    .timeout(std::time::Duration::from_secs(60))
                    .send().await
                {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.bytes().await {
                            Ok(b) => b.to_vec(),
                            Err(e) => return NodeResult::failed(&format!("Failed to read attachment: {}", e)),
                        }
                    }
                    Ok(resp) => return NodeResult::failed(&format!("Failed to download attachment: HTTP {}", resp.status())),
                    Err(e) => return NodeResult::failed(&format!("Failed to download attachment: {}", e)),
                };

                // Build multipart email with body + attachment
                let body_part = SinglePart::builder()
                    .header(content_type)
                    .body(body.to_string());

                let attachment_ct: ContentType = mimetype.parse().unwrap_or(ContentType::parse("application/octet-stream").unwrap());
                let attachment_part = Attachment::new(filename.to_string())
                    .body(file_bytes, attachment_ct);

                let multipart = MultiPart::mixed()
                    .singlepart(body_part)
                    .singlepart(attachment_part);

                match builder.multipart(multipart) {
                    Ok(m) => m,
                    Err(e) => return NodeResult::failed(&format!("Failed to build email with attachment: {}", e)),
                }
            }
        } else {
            match builder.header(content_type).body(body.to_string()) {
                Ok(m) => m,
                Err(e) => return NodeResult::failed(&format!("Failed to build email: {}", e)),
            }
        };

        let creds = Credentials::new(username.to_string(), password.to_string());

        let security = config_input.get("security")
            .and_then(|v| v.as_str())
            .unwrap_or("starttls");
        let tls_accept_invalid = config_input.get("tlsAcceptInvalid")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            .unwrap_or(false);

        let transport = match security {
            "tls" => {
                // Direct TLS (implicit SSL, typically port 465)
                let mut tls_params = lettre::transport::smtp::client::TlsParameters::builder(smtp_host.to_string());
                if tls_accept_invalid {
                    tls_params = tls_params.dangerous_accept_invalid_certs(true);
                }
                let tls_params = match tls_params.build() {
                    Ok(p) => p,
                    Err(e) => return NodeResult::failed(&format!("TLS config error: {}", e)),
                };
                AsyncSmtpTransport::<Tokio1Executor>::relay(smtp_host)
                    .map(|t| t.port(smtp_port).credentials(creds).tls(lettre::transport::smtp::client::Tls::Wrapper(tls_params)).build())
                    .map_err(|e| format!("Failed to create SMTP transport: {}", e))
            }
            "none" => {
                // Plain SMTP (no encryption, typically port 25)
                Ok(AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(smtp_host)
                    .port(smtp_port)
                    .credentials(creds)
                    .build())
            }
            _ => {
                // STARTTLS (default, typically port 587)
                if tls_accept_invalid {
                    let mut tls_params = lettre::transport::smtp::client::TlsParameters::builder(smtp_host.to_string());
                    tls_params = tls_params.dangerous_accept_invalid_certs(true);
                    let tls_params = match tls_params.build() {
                        Ok(p) => p,
                        Err(e) => return NodeResult::failed(&format!("TLS config error: {}", e)),
                    };
                    AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(smtp_host)
                        .map(|t| t.port(smtp_port).credentials(creds).tls(lettre::transport::smtp::client::Tls::Required(tls_params)).build())
                        .map_err(|e| format!("Failed to create SMTP transport: {}", e))
                } else {
                    AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(smtp_host)
                        .map(|t| t.port(smtp_port).credentials(creds).build())
                        .map_err(|e| format!("Failed to create SMTP transport: {}", e))
                }
            }
        };

        let transport = match transport {
            Ok(t) => t,
            Err(e) => return NodeResult::failed(&e),
        };

        match transport.send(message).await {
            Ok(response) => {
                let message_id = response.message().next()
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                NodeResult::completed(serde_json::json!({
                    "success": true,
                    "messageId": message_id,
                }))
            }
            Err(e) => {
                tracing::error!("SMTP send failed: {}", e);
                NodeResult::completed(serde_json::json!({
                    "success": false,
                    "messageId": "",
                }))
            }
        }
    }
}

register_node!(EmailSendNode);

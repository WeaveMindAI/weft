//! Email Receive trigger node - polls IMAP inbox for new emails and fires events.

use async_trait::async_trait;
use std::collections::HashSet;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct EmailReceiveNode;

#[async_trait]
impl Node for EmailReceiveNode {
    fn node_type(&self) -> &'static str {
        "EmailReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Email Receive",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
            ],
            outputs: vec![
                PortDef::new("from", "String", false),
                PortDef::new("to", "List[String]", false),
                PortDef::new("subject", "String", false),
                PortDef::new("body", "String", false),
                PortDef::new("htmlBody", "String", false),
                PortDef::new("cc", "List[String]", false),
                PortDef::new("bcc", "List[String]", false),
                PortDef::new("replyTo", "String", false),
                PortDef::new("date", "String", false),
                PortDef::new("messageId", "String", false),
                PortDef::new("threadId", "String", false),
                PortDef::new("inReplyTo", "String", false),
                PortDef::new("references", "List[String]", false),
                PortDef::new("hasAttachments", "Boolean", false),
                PortDef::new("attachmentCount", "Number", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Polling),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("mailbox"),
                FieldDef::number("pollIntervalSecs").with_default(serde_json::json!(60)),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        if ctx.isTriggerSetup {
            return NodeResult::completed(ctx.input.clone());
        }
        let payload = ctx.input.get("triggerPayload")
            .cloned()
            .unwrap_or(ctx.input.clone());
        NodeResult::completed(payload)
    }

    async fn keep_alive(&self,
        config: TriggerStartConfig,
        ctx: TriggerContext,
    ) -> Result<TriggerHandle, TriggerError> {
        let imap_host = config.require_str("host")
            .map_err(|_| TriggerError::Config("Missing 'host' in config - connect an EmailConfig node".to_string()))?;
        let imap_port: u16 = config.get_u64("port").map(|n| n as u16).unwrap_or(993);
        let username = config.require_str("username")
            .map_err(|_| TriggerError::Config("Missing 'username' in config - connect an EmailConfig node".to_string()))?;
        let password = config.require_str("password")
            .map_err(|_| TriggerError::Config("Missing 'password' in config - connect an EmailConfig node".to_string()))?;
        let mailbox = config.get_str("mailbox").unwrap_or_else(|| "INBOX".to_string());
        let poll_interval_secs = config.get_u64("pollIntervalSecs").unwrap_or(60);
        let poll_interval = std::time::Duration::from_secs(poll_interval_secs);
        let security = config.get_str("security").unwrap_or_else(|| "tls".to_string());
        let tls_accept_invalid = config.config.get("tlsAcceptInvalid")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            .unwrap_or(false);

        ctx.spawn(&config, TriggerCategory::Polling, move |emit, shutdown| async move {
            let mut seen_uids: HashSet<u32> = HashSet::new();
            let mut first_poll = true;

            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(poll_interval) => {
                        let mut session = match email_lib::connect_imap(
                            &imap_host, imap_port, &username, &password, &security, tls_accept_invalid
                        ).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!("Email IMAP connection failed: {}", e);
                                continue;
                            }
                        };

                        if let Err(e) = session.select(&mailbox).await {
                            tracing::warn!("Email select '{}' failed: {}", mailbox, e);
                            let _ = session.logout().await;
                            continue;
                        }

                        let unseen = match session.uid_search("UNSEEN").await {
                            Ok(uids) => uids,
                            Err(e) => {
                                tracing::warn!("Email search failed: {}", e);
                                let _ = session.logout().await;
                                continue;
                            }
                        };

                        let new_uids: Vec<u32> = unseen.iter()
                            .copied()
                            .filter(|uid| !seen_uids.contains(uid))
                            .collect();

                        for uid in &new_uids {
                            seen_uids.insert(*uid);
                        }

                        if first_poll {
                            first_poll = false;
                            let _ = session.logout().await;
                            continue;
                        }

                        if new_uids.is_empty() {
                            let _ = session.logout().await;
                            continue;
                        }

                        let uid_set = new_uids.iter()
                            .map(|u| u.to_string())
                            .collect::<Vec<_>>()
                            .join(",");

                        match session.uid_fetch_bodies(&uid_set).await {
                            Ok(bodies) => {
                                for raw_body in &bodies {
                                    emit.emit(parse_email_payload(raw_body))?;
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Email fetch failed: {}", e);
                            }
                        }

                        let _ = session.logout().await;
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(EmailReceiveNode);

use super::email_lib;

fn parse_email_payload(raw: &[u8]) -> serde_json::Value {
    let parsed = mail_parser::MessageParser::default().parse(raw);

    match parsed {
        Some(msg) => {
            let from = msg.from()
                .and_then(|a| a.first())
                .map(|a| {
                    if let Some(name) = a.name() {
                        format!("{} <{}>", name, a.address().unwrap_or(""))
                    } else {
                        a.address().unwrap_or("").to_string()
                    }
                })
                .unwrap_or_default();

            let to: Vec<String> = msg.to()
                .map(|addrs| addrs.iter().filter_map(|a| a.address()).map(|s| s.to_string()).collect())
                .unwrap_or_default();

            let cc: Vec<String> = msg.cc()
                .map(|addrs| addrs.iter().filter_map(|a| a.address()).map(|s| s.to_string()).collect())
                .unwrap_or_default();

            let bcc: Vec<String> = msg.bcc()
                .map(|addrs| addrs.iter().filter_map(|a| a.address()).map(|s| s.to_string()).collect())
                .unwrap_or_default();

            let reply_to = msg.reply_to()
                .and_then(|a| a.first())
                .and_then(|a| a.address())
                .unwrap_or("")
                .to_string();

            let subject = msg.subject().unwrap_or("").to_string();
            let body = msg.body_text(0).unwrap_or_default().to_string();
            let html_body = msg.body_html(0).unwrap_or_default().to_string();
            let date = msg.date().map(|d| d.to_rfc3339()).unwrap_or_default();
            let message_id = msg.message_id().unwrap_or("").to_string();

            let in_reply_to = match msg.in_reply_to() {
                mail_parser::HeaderValue::Text(t) => t.to_string(),
                mail_parser::HeaderValue::TextList(list) => list.join(", "),
                _ => String::new(),
            };

            let references: Vec<String> = match msg.references() {
                mail_parser::HeaderValue::Text(t) => vec![t.to_string()],
                mail_parser::HeaderValue::TextList(list) => list.iter().map(|s| s.to_string()).collect(),
                _ => vec![],
            };

            let thread_id = references.first()
                .cloned()
                .or_else(|| if !in_reply_to.is_empty() { Some(in_reply_to.clone()) } else { None })
                .unwrap_or_else(|| message_id.clone());

            let attachment_count = msg.attachment_count();

            serde_json::json!({
                "from": from,
                "to": to,
                "cc": cc,
                "bcc": bcc,
                "replyTo": reply_to,
                "subject": subject,
                "body": body,
                "htmlBody": html_body,
                "date": date,
                "messageId": message_id,
                "inReplyTo": in_reply_to,
                "references": references,
                "threadId": thread_id,
                "hasAttachments": attachment_count > 0,
                "attachmentCount": attachment_count,
            })
        }
        None => {
            serde_json::json!({
                "from": "",
                "to": [],
                "cc": [],
                "bcc": [],
                "replyTo": "",
                "subject": "",
                "body": String::from_utf8_lossy(raw).to_string(),
                "htmlBody": "",
                "date": "",
                "messageId": "",
                "inReplyTo": "",
                "references": [],
                "threadId": "",
                "hasAttachments": false,
                "attachmentCount": 0,
            })
        }
    }
}

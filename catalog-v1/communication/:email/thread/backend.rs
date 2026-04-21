//! Email Thread Node - Fetches the full email thread history from IMAP
//! given a threadId or references from an incoming email.

use async_trait::async_trait;

use crate::node::{ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef};
use crate::{register_node, NodeResult};

use super::email_lib;

#[derive(Default)]
pub struct EmailThreadNode;

#[async_trait]
impl Node for EmailThreadNode {
    fn node_type(&self) -> &'static str {
        "EmailThread"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Email Thread",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("threadId", "String", true),
                PortDef::new("messageId", "String", false),
            ],
            outputs: vec![
                PortDef::new("senders", "List[String]", false),
                PortDef::new("subjects", "List[String]", false),
                PortDef::new("bodies", "List[String]", false),
                PortDef::new("dates", "List[String]", false),
                PortDef::new("count", "Number", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        // Get config from input port (from EmailConfig node) or fall back to node's own config
        let email_config = ctx.input.get("config")
            .and_then(|v| v.as_object())
            .map(|o| serde_json::Value::Object(o.clone()));
        let config_source = email_config.as_ref().unwrap_or(&ctx.config);

        let imap_host = config_source.get("host").and_then(|v| v.as_str())
            .unwrap_or("");
        let imap_port: u16 = config_source.get("port")
            .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64().map(|n| n as u16)))
            .unwrap_or(993);
        let username = config_source.get("username").and_then(|v| v.as_str())
            .unwrap_or("");
        let password = config_source.get("password").and_then(|v| v.as_str())
            .unwrap_or("");
        let thread_id = ctx.input.get("threadId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let current_message_id = ctx.input.get("messageId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if imap_host.is_empty() {
            return NodeResult::failed("IMAP host is required in node config");
        }
        if username.is_empty() || password.is_empty() {
            return NodeResult::failed("IMAP username and password are required in node config");
        }
        if thread_id.is_empty() {
            return NodeResult::failed("threadId is required");
        }

        let security = config_source.get("security")
            .and_then(|v| v.as_str())
            .unwrap_or("tls");
        let tls_accept_invalid = config_source.get("tlsAcceptInvalid")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            .unwrap_or(false);

        let mut session = match email_lib::connect_imap(imap_host, imap_port, username, password, security, tls_accept_invalid).await {
            Ok(s) => s,
            Err(e) => return NodeResult::failed(&format!("IMAP connection failed: {}", e)),
        };

        let is_gmail = imap_host.contains("gmail.com") || imap_host.contains("googlemail.com");

        let mut messages: Vec<serde_json::Value> = Vec::new();
        let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

        if is_gmail {
            // Gmail: use X-GM-THRID for reliable thread search across all folders.
            // 1) Select [Gmail]/All Mail (contains sent + received)
            // 2) Find the triggering message by Message-ID to get its X-GM-THRID
            // 3) Search X-GM-THRID <id> to get all messages in the thread
            if let Err(e) = session.select("[Gmail]/All Mail").await {
                let _ = session.logout().await;
                return NodeResult::failed(&format!("Failed to select [Gmail]/All Mail: {}", e));
            }

            // Find the Gmail thread ID by searching for our threadId (which is an RFC Message-ID)
            let msgid_query = format!("HEADER \"Message-ID\" \"{}\"", thread_id);
            let gm_thrid: Option<u64> = match session.uid_search(&msgid_query).await {
                Ok(uids) if !uids.is_empty() => {
                    let first_uid = *uids.iter().next().unwrap();
                    session.gmail_fetch_thrid(first_uid).await
                }
                _ => None,
            };

            if let Some(thrid) = gm_thrid {
                // Search all messages with this Gmail thread ID
                let thread_query = format!("X-GM-THRID {}", thrid);
                if let Ok(uids) = session.uid_search(&thread_query).await {
                    if !uids.is_empty() {
                        let uid_set = uids.iter()
                            .map(|u| u.to_string())
                            .collect::<Vec<_>>()
                            .join(",");

                        if let Ok(bodies) = session.uid_fetch_bodies(&uid_set).await {
                            for raw_body in &bodies {
                                let payload = parse_thread_message(raw_body);
                                let msg_id = payload.get("messageId")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();

                                if !current_message_id.is_empty() && msg_id == current_message_id {
                                    continue;
                                }

                                if !msg_id.is_empty() && !seen_ids.insert(msg_id) {
                                    continue;
                                }

                                messages.push(payload);
                            }
                        }
                    }
                }
            }
        } else {
            // Non-Gmail: search all mailboxes with HEADER References/In-Reply-To/Message-ID
            let search_query = format!(
                "OR OR HEADER \"References\" \"{}\" HEADER \"In-Reply-To\" \"{}\" HEADER \"Message-ID\" \"{}\"",
                thread_id, thread_id, thread_id
            );

            let mut mailboxes_to_search: Vec<String> = session.list(Some(""), Some("*")).await
                .unwrap_or_default();
            if mailboxes_to_search.is_empty() {
                mailboxes_to_search.push("INBOX".to_string());
            }

            for mbox in &mailboxes_to_search {
                if session.select(mbox.as_str()).await.is_err() {
                    continue;
                }

                let uids = match session.uid_search(&search_query).await {
                    Ok(uids) => uids,
                    Err(_) => continue,
                };

                if uids.is_empty() {
                    continue;
                }

                let uid_set = uids.iter()
                    .map(|u| u.to_string())
                    .collect::<Vec<_>>()
                    .join(",");

                if let Ok(bodies) = session.uid_fetch_bodies(&uid_set).await {
                    for raw_body in &bodies {
                        let payload = parse_thread_message(raw_body);
                        let msg_id = payload.get("messageId")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        if !current_message_id.is_empty() && msg_id == current_message_id {
                            continue;
                        }

                        if !msg_id.is_empty() && !seen_ids.insert(msg_id) {
                            continue;
                        }

                        messages.push(payload);
                    }
                }
            }
        }

        let _ = session.logout().await;

        // Sort by date (oldest first)
        messages.sort_by(|a, b| {
            let da = a.get("date").and_then(|v| v.as_str()).unwrap_or("");
            let db = b.get("date").and_then(|v| v.as_str()).unwrap_or("");
            da.cmp(db)
        });

        let count = messages.len();

        let senders: Vec<serde_json::Value> = messages.iter()
            .map(|m| serde_json::json!(m.get("from").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let subjects: Vec<serde_json::Value> = messages.iter()
            .map(|m| serde_json::json!(m.get("subject").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let bodies_list: Vec<serde_json::Value> = messages.iter()
            .map(|m| serde_json::json!(m.get("body").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let dates: Vec<serde_json::Value> = messages.iter()
            .map(|m| serde_json::json!(m.get("date").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();

        NodeResult::completed(serde_json::json!({
            "senders": senders,
            "subjects": subjects,
            "bodies": bodies_list,
            "dates": dates,
            "count": count,
        }))
    }
}

register_node!(EmailThreadNode);

fn parse_thread_message(raw: &[u8]) -> serde_json::Value {
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
                .map(|addrs| {
                    addrs.iter()
                        .filter_map(|a| a.address())
                        .map(|s| s.to_string())
                        .collect()
                })
                .unwrap_or_default();

            let subject = msg.subject().unwrap_or("").to_string();
            let body = msg.body_text(0).unwrap_or_default().to_string();
            let date = msg.date()
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            let message_id = msg.message_id().unwrap_or("").to_string();

            serde_json::json!({
                "from": from,
                "to": to,
                "subject": subject,
                "body": body,
                "date": date,
                "messageId": message_id,
            })
        }
        None => {
            serde_json::json!({
                "from": "",
                "to": [],
                "subject": "",
                "body": String::from_utf8_lossy(raw).to_string(),
                "date": "",
                "messageId": "",
            })
        }
    }
}

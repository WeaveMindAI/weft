//! XPost Node - Post to X (Twitter) via API v2.
//!
//! Uses OAuth 1.0a authentication (api key + access token) to create
//! posts via POST /2/tweets. Supports basic posts, replies, and media attachments.
//! When media is provided, uploads via POST /1.1/media/upload.json first.

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

type HmacSha1 = Hmac<Sha1>;

const X_API_BASE: &str = "https://api.x.com";

#[derive(Default)]
pub struct XPostNode;

#[async_trait]
impl Node for XPostNode {
    fn node_type(&self) -> &'static str {
        "XPost"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "X Post",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("text", "String", false),
                PortDef::new("replyToPostId", "String", false),
                PortDef::new("media", "Media", false),
            ],
            outputs: vec![
                PortDef::new("postId", "String", false),
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures {
                oneOfRequired: vec![vec!["text".into(), "media".into()]],
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let text = ctx.input.get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let reply_to = ctx.input.get("replyToPostId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let media = ctx.input.get("media")
            .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());

        let config = ctx.input.get("config")
            .cloned()
            .unwrap_or(serde_json::Value::Object(Default::default()));

        let api_key = config.get("apiKey").and_then(|v| v.as_str())
            .unwrap_or("");
        let api_key_secret = config.get("apiKeySecret").and_then(|v| v.as_str())
            .unwrap_or("");
        let access_token = config.get("accessToken").and_then(|v| v.as_str())
            .unwrap_or("");
        let access_token_secret = config.get("accessTokenSecret").and_then(|v| v.as_str())
            .unwrap_or("");

        if api_key.is_empty() || api_key_secret.is_empty() || access_token.is_empty() || access_token_secret.is_empty() {
            return NodeResult::failed("X API OAuth 1.0a credentials are required (apiKey, apiKeySecret, accessToken, accessTokenSecret). Connect an XConfig node.");
        }

        if text.is_empty() && media.is_none() {
            return NodeResult::failed("Either post text or media is required");
        }

        // If media is provided, upload it first
        let media_id: Option<String> = if let Some(media_obj) = media {
            let media_url = media_obj.get("url").and_then(|v| v.as_str()).unwrap_or("");
            if !media_url.is_empty() {
                // Download the media
                let http_client = reqwest::Client::new();
                let file_bytes = match http_client.get(media_url)
                    .timeout(std::time::Duration::from_secs(60))
                    .send().await
                {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.bytes().await {
                            Ok(b) => b.to_vec(),
                            Err(e) => return NodeResult::failed(&format!("Failed to read media: {}", e)),
                        }
                    }
                    Ok(resp) => return NodeResult::failed(&format!("Failed to download media: HTTP {}", resp.status())),
                    Err(e) => return NodeResult::failed(&format!("Failed to download media: {}", e)),
                };

                // Upload to X media endpoint (v1.1 simple upload)
                let upload_url = "https://upload.twitter.com/1.1/media/upload.json";
                let upload_auth = build_oauth_header(
                    "POST", upload_url, api_key, api_key_secret, access_token, access_token_secret,
                );

                let media_b64 = base64::Engine::encode(&BASE64, &file_bytes);
                let form = reqwest::multipart::Form::new()
                    .text("media_data", media_b64);

                let upload_resp = http_client.post(upload_url)
                    .header("Authorization", &upload_auth)
                    .multipart(form)
                    .send().await;

                match upload_resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        body.get("media_id_string")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    }
                    Ok(r) => {
                        let text = r.text().await.unwrap_or_default();
                        tracing::error!("X media upload failed: {}", text);
                        None
                    }
                    Err(e) => {
                        tracing::error!("X media upload request failed: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let url = format!("{}/2/tweets", X_API_BASE);

        let mut body = serde_json::json!({});
        if !text.is_empty() {
            body["text"] = serde_json::json!(text);
        }
        if let Some(reply_id) = reply_to {
            body["reply"] = serde_json::json!({ "in_reply_to_tweet_id": reply_id });
        }
        if let Some(ref mid) = media_id {
            body["media"] = serde_json::json!({ "media_ids": [mid] });
        }

        let auth_header = build_oauth_header(
            "POST",
            &url,
            api_key,
            api_key_secret,
            access_token,
            access_token_secret,
        );

        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .header("Authorization", &auth_header)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if resp.status().is_success() {
                    let resp_body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let post_id = resp_body.get("data")
                        .and_then(|d| d.get("id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    NodeResult::completed(serde_json::json!({
                        "postId": post_id,
                        "success": true,
                    }))
                } else {
                    let status = resp.status();
                    let error_text = resp.text().await.unwrap_or_default();
                    tracing::error!("X API error: {} - {}", status, error_text);
                    NodeResult::completed(serde_json::json!({
                        "postId": "",
                        "success": false,
                    }))
                }
            }
            Err(e) => {
                tracing::error!("X request failed: {}", e);
                NodeResult::failed(&format!("Failed to post to X: {}", e))
            }
        }
    }
}

register_node!(XPostNode);

/// Percent-encode a string per RFC 5849 (OAuth 1.0a).
fn percent_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push('%');
                result.push_str(&format!("{:02X}", byte));
            }
        }
    }
    result
}

/// Build an OAuth 1.0a Authorization header for a request with no query params.
/// The request body is JSON (not form-encoded), so only oauth_* params are signed.
pub fn build_oauth_header(
    method: &str,
    url: &str,
    consumer_key: &str,
    consumer_secret: &str,
    token: &str,
    token_secret: &str,
) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();

    let nonce: String = (0..32)
        .map(|_| {
            let idx = rand::random::<u8>() % 36;
            if idx < 10 { (b'0' + idx) as char } else { (b'a' + idx - 10) as char }
        })
        .collect();

    // OAuth params (sorted alphabetically by key)
    let mut params: Vec<(&str, &str)> = vec![
        ("oauth_consumer_key", consumer_key),
        ("oauth_nonce", &nonce),
        ("oauth_signature_method", "HMAC-SHA1"),
        ("oauth_timestamp", &timestamp),
        ("oauth_token", token),
        ("oauth_version", "1.0"),
    ];
    params.sort_by_key(|&(k, _)| k);

    // Build parameter string
    let param_string: String = params.iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    // Build signature base string: METHOD&URL&PARAMS
    let base_string = format!(
        "{}&{}&{}",
        method.to_uppercase(),
        percent_encode(url),
        percent_encode(&param_string),
    );

    // Build signing key: consumer_secret&token_secret
    let signing_key = format!("{}&{}", percent_encode(consumer_secret), percent_encode(token_secret));

    // HMAC-SHA1 sign
    let mut mac = HmacSha1::new_from_slice(signing_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(base_string.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());

    // Build Authorization header
    format!(
        "OAuth oauth_consumer_key=\"{}\", oauth_nonce=\"{}\", oauth_signature=\"{}\", oauth_signature_method=\"HMAC-SHA1\", oauth_timestamp=\"{}\", oauth_token=\"{}\", oauth_version=\"1.0\"",
        percent_encode(consumer_key),
        percent_encode(&nonce),
        percent_encode(&signature),
        percent_encode(&timestamp),
        percent_encode(token),
    )
}

//! Generic webhook handler for trigger-based project execution.
//!
//! This module handles incoming webhook requests and starts project executions.
//! Signature validation is entirely config-driven - nodes specify their validation
//! requirements via config fields (secret, signatureHeader, signaturePrefix).

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::state::AppState;
use crate::trigger_store;

#[derive(Debug, Serialize)]
pub struct WebhookResponse {
    pub status: String,
    pub message: String,
}

/// Handle incoming webhook calls
/// URL format: /api/v1/webhooks/{trigger_id}
/// 
/// The trigger's config can specify signature validation:
/// - secret: The shared secret for HMAC validation
/// - signatureHeader: HTTP header containing the signature (e.g., "X-Hub-Signature-256")
/// - signaturePrefix: Prefix before the hex signature (e.g., "sha256=")
pub async fn handle_webhook(
    State(state): State<Arc<AppState>>,
    Path(trigger_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    tracing::info!("Webhook received for trigger: {}", trigger_id);

    let pool = &state.db_pool;

    // Look up the trigger in the database
    let trigger = match trigger_store::get_trigger(pool, &trigger_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::warn!("Webhook trigger not found: {}", trigger_id);
            return (
                StatusCode::NOT_FOUND,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: "Trigger not found".to_string(),
                }),
            );
        }
        Err(e) => {
            tracing::error!("Database error looking up trigger: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: "Database error".to_string(),
                }),
            );
        }
    };

    // Verify trigger category is Webhook
    if trigger.triggerCategory != "Webhook" {
        tracing::warn!("Trigger {} is not a webhook trigger (category: {})", trigger_id, trigger.triggerCategory);
        return (
            StatusCode::BAD_REQUEST,
            Json(WebhookResponse {
                status: "error".to_string(),
                message: "Trigger is not a webhook type".to_string(),
            }),
        );
    }

    // Parse the body as JSON if possible
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap_or_else(|_| {
        serde_json::json!({
            "raw_body": String::from_utf8_lossy(&body).to_string()
        })
    });

    // Validate API key if configured (simple header-based auth)
    if let Some(expected_key) = trigger.config.get("apiKey").and_then(|s| s.as_str()) {
        if !expected_key.is_empty() {
            let provided = headers.get("x-api-key").and_then(|v| v.to_str().ok());
            match provided {
                Some(key) if subtle::ConstantTimeEq::ct_eq(key.as_bytes(), expected_key.as_bytes()).into() => {}
                _ => {
                    tracing::warn!("Invalid or missing API key for trigger: {}", trigger_id);
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(WebhookResponse {
                            status: "error".to_string(),
                            message: "Invalid or missing API key".to_string(),
                        }),
                    );
                }
            }
        }
    }

    // Validate webhook signature if configured
    // Signature validation is entirely config-driven - no hardcoded defaults
    if let Some(expected_secret) = trigger.config.get("secret").and_then(|s| s.as_str()) {
        if !expected_secret.is_empty() {
            let signature_header = trigger.config
                .get("signatureHeader")
                .and_then(|s| s.as_str());
            
            let signature_prefix = trigger.config
                .get("signaturePrefix")
                .and_then(|s| s.as_str());

            // Only validate if both header and prefix are configured
            if let (Some(header), Some(prefix)) = (signature_header, signature_prefix) {
                let signature = headers
                    .get(header)
                    .and_then(|v| v.to_str().ok());

                if !verify_hmac_signature(&body, expected_secret, signature, prefix) {
                    tracing::warn!("Invalid webhook signature for trigger: {}", trigger_id);
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(WebhookResponse {
                            status: "error".to_string(),
                            message: "Invalid signature".to_string(),
                        }),
                    );
                }
            }
        }
    }

    // Load weft code from trigger record (stored as Value::String by register_trigger)
    let weft_code = match &trigger.projectDefinition {
        Some(wf) => match wf.as_str() {
            Some(code) if !code.is_empty() => code.to_string(),
            _ => {
                tracing::error!(
                    "Trigger {} has non-string or empty project_definition",
                    trigger_id
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(WebhookResponse {
                        status: "error".to_string(),
                        message: "Trigger has invalid project definition".to_string(),
                    }),
                );
            }
        },
        None => {
            tracing::error!(
                "Trigger {} has no stored project definition (weft_code missing from trigger record)",
                trigger_id
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: "Trigger has no project definition".to_string(),
                }),
            );
        }
    };

    // Compile weft code into a ProjectDefinition
    let project_uuid = match uuid::Uuid::parse_str(&trigger.projectId) {
        Ok(p) => p,
        Err(_) => {
            tracing::error!("Invalid project UUID on trigger {}: {}", trigger_id, trigger.projectId);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: "Invalid project UUID on trigger".to_string(),
                }),
            );
        }
    };
    let mut project = match weft_core::weft_compiler::compile(&weft_code, project_uuid) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("Failed to compile weft code for trigger {}: {:?}", trigger_id, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: "Failed to compile project".to_string(),
                }),
            );
        }
    };
    if let Err(errors) = weft_nodes::enrich::enrich_project(&mut project, state.node_registry) {
        tracing::error!("Project validation failed for webhook trigger {}: {}", trigger_id, errors.join("; "));
        return (
            StatusCode::BAD_REQUEST,
            Json(WebhookResponse {
                status: "error".to_string(),
                message: format!("Project validation failed: {}", errors.join("; ")),
            }),
        );
    }

    // Build the trigger event payload
    let event_payload = serde_json::json!({
        "nodeType": trigger.nodeType,
        "headers": headers_to_json(&headers),
        "body": payload,
        "receivedAt": chrono::Utc::now().to_rfc3339(),
    });

    // Start the project execution
    let execution_id = uuid::Uuid::new_v4().to_string();
    let user_id = trigger.userId.as_deref().unwrap_or("local");
    
    tracing::info!(
        "Starting project {} from webhook trigger {} (execution: {})",
        trigger.projectId, trigger_id, execution_id
    );

    let executor_url = std::env::var("EXECUTOR_URL")
        .unwrap_or_else(|_| "http://localhost:9081".to_string());
    let dashboard_url = std::env::var("DASHBOARD_URL")
        .unwrap_or_else(|_| "http://localhost:5174".to_string());
    let status_callback_url = format!("{}/api/executions/{}", dashboard_url, execution_id);

    let start_url = format!(
        "{}/ProjectExecutor/{}/start/send",
        executor_url, execution_id
    );

    let start_request = serde_json::json!({
        "project": project,
        "input": {
            "triggerNodeId": trigger.triggerNodeId,
            "triggerPayload": event_payload,
        },
        "statusCallbackUrl": status_callback_url,
        "userId": user_id,
        "weftCode": weft_code,
        "triggerId": trigger_id,
        "nodeType": trigger.nodeType,
    });

    match state.http_client.post(&start_url)
        .json(&start_request)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            tracing::info!("Project execution {} started successfully", execution_id);
            (
                StatusCode::OK,
                Json(WebhookResponse {
                    status: "success".to_string(),
                    message: format!("Project execution {} started", execution_id),
                }),
            )
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Failed to start project: {} - {}", status, body);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: format!("Failed to start project: {}", status),
                }),
            )
        }
        Err(e) => {
            tracing::error!("Failed to call executor: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WebhookResponse {
                    status: "error".to_string(),
                    message: format!("Failed to start project: {}", e),
                }),
            )
        }
    }
}

/// Verify HMAC-SHA256 signature for webhook payloads
fn verify_hmac_signature(body: &[u8], secret: &str, signature: Option<&str>, prefix: &str) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let signature = match signature {
        Some(s) => s,
        None => return false,
    };

    // Strip the prefix (e.g., "sha256=" for GitHub)
    if !signature.starts_with(prefix) {
        return false;
    }

    let signature_hex = &signature[prefix.len()..];
    let signature_bytes = match hex::decode(signature_hex) {
        Ok(b) => b,
        Err(_) => return false,
    };

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };

    mac.update(body);
    mac.verify_slice(&signature_bytes).is_ok()
}

/// Convert headers to JSON for inclusion in payload
fn headers_to_json(headers: &HeaderMap) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            map.insert(key.to_string(), serde_json::Value::String(v.to_string()));
        }
    }
    serde_json::Value::Object(map)
}

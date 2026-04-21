//! Extension Token Management
//!
//! This module handles ALL extension token logic for both local and cloud modes.
//! - Local mode: weft-api serves this directly
//! - Cloud mode: cloud-api proxies requests here via its fallback handler
//!
//! Related code:
//! - Dashboard UI: dashboard/src/routes/(app)/extension/+page.svelte
//! - Extension client: extension/src/lib/api.ts
//!
//! ## SCHEMA SYNC WARNING
//!
//! The `extension_tokens` table schema is defined in:
//!   - `weavemind/init-db/01-init.sql` (cloud, source of truth)
//!   - `init-db.sql` (local dev, should match)
//!
//! When modifying the schema, you MUST update:
//!   1. The SQL files above
//!   2. `ExtensionToken` struct below

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

use crate::state::AppState;

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct ExtensionToken {
    pub id: String,
    pub userId: String,
    pub name: Option<String>,
    pub createdAt: String,
    pub lastUsedAt: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub struct CreateTokenRequest {
    pub userId: String,
    pub username: String,
    pub keyName: String,
}

#[derive(Debug, Serialize)]
pub struct TokenListResponse {
    pub tokens: Vec<ExtensionToken>,
}

fn generate_token_id(username: &str, key_name: &str) -> String {
    format!("{}_{}", username, key_name)
}

fn validate_key_name(key_name: &str) -> Result<(), &'static str> {
    if key_name.is_empty() {
        return Err("Key name cannot be empty");
    }
    if key_name.len() > 64 {
        return Err("Key name too long (max 64 characters)");
    }
    // Only allow alphanumeric, underscore, hyphen
    if !key_name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        return Err("Key name can only contain letters, numbers, underscores, and hyphens");
    }
    Ok(())
}

/// Create a new extension token
pub async fn create_token(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateTokenRequest>,
) -> impl IntoResponse {
    // Validate userId is not empty
    if req.userId.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "userId is required" })),
        ).into_response();
    }

    let pool = &state.db_pool;

    // Validate key name
    if let Err(e) = validate_key_name(&req.keyName) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        ).into_response();
    }

    let token_id = generate_token_id(&req.username, &req.keyName);
    let now = chrono::Utc::now();

    let result = sqlx::query(
        "INSERT INTO extension_tokens (id, user_id, name, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(&token_id)
    .bind(&req.userId)
    .bind(&req.keyName)
    .bind(now)
    .execute(pool)
    .await;

    match result {
        Ok(_) => {
            tracing::info!("Created extension token {} for user {}", token_id, req.userId);
            Json(ExtensionToken {
                id: token_id,
                userId: req.userId,
                name: Some(req.keyName),
                createdAt: now.to_rfc3339(),
                lastUsedAt: None,
            })
            .into_response()
        }
        Err(e) => {
            // Check for unique constraint violation
            let error_msg = e.to_string();
            if error_msg.contains("UNIQUE") || error_msg.contains("duplicate") {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "A token with this name already exists" })),
                ).into_response();
            }
            tracing::error!("Failed to create extension token: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to create token" })),
            )
                .into_response()
        }
    }
}

/// List all tokens for a user
pub async fn list_tokens(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    let result = sqlx::query_as::<_, (String, String, Option<String>, chrono::DateTime<chrono::Utc>, Option<chrono::DateTime<chrono::Utc>>)>(
        "SELECT id, user_id, name, created_at, last_used_at FROM extension_tokens WHERE user_id = $1 ORDER BY created_at DESC",
    )
    .bind(&user_id)
    .fetch_all(pool)
    .await;

    match result {
        Ok(rows) => {
            let tokens: Vec<ExtensionToken> = rows
                .into_iter()
                .map(|(id, user_id, name, created_at, last_used_at)| ExtensionToken {
                    id,
                    userId: user_id,
                    name,
                    createdAt: created_at.to_rfc3339(),
                    lastUsedAt: last_used_at.map(|dt| dt.to_rfc3339()),
                })
                .collect();
            Json(TokenListResponse { tokens }).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list extension tokens: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to list tokens" })),
            )
                .into_response()
        }
    }
}

/// Delete a token
pub async fn delete_token(
    State(state): State<Arc<AppState>>,
    Path(token_id): Path<String>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    let result = sqlx::query("DELETE FROM extension_tokens WHERE id = $1")
        .bind(&token_id)
        .execute(pool)
        .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => {
            tracing::info!("Deleted extension token {}", token_id);
            Json(serde_json::json!({ "deleted": true })).into_response()
        }
        Ok(_) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Token not found" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to delete extension token: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to delete token" })),
            )
                .into_response()
        }
    }
}

/// Validate a token and return the user_id if valid
/// Also updates last_used_at
pub async fn validate_token(pool: &PgPool, token: &str) -> Option<String> {
    let result = sqlx::query_as::<_, (String,)>(
        "SELECT user_id FROM extension_tokens WHERE id = $1",
    )
    .bind(token)
    .fetch_optional(pool)
    .await;

    match result {
        Ok(Some((user_id,))) => {
            // Update last_used_at
            let now = chrono::Utc::now();
            let _ = sqlx::query("UPDATE extension_tokens SET last_used_at = $1 WHERE id = $2")
                .bind(now)
                .bind(token)
                .execute(pool)
                .await;
            Some(user_id)
        }
        Ok(None) => None,
        Err(e) => {
            tracing::error!("Failed to validate token: {}", e);
            None
        }
    }
}

//! Postgres DurableKV Sidecar
//!
//! Self-contained sidecar implementing the WeaveMind sidecar contract
//! with durable key-value storage backed by PostgreSQL.
//!
//! Actions:
//!   kv_set            { key, value }     -> { stored: true, key }
//!   kv_get            { key }            -> { found, key, value }
//!   kv_delete         { key }            -> {}
//!   kv_list           {}                 -> { entries, count }
//!   kv_query          { pattern }        -> { matches, count }
//!   kv_delete_pattern { pattern }        -> { deleted, count }
//!
//! Environment variables:
//!   DATABASE_URL  Postgres connection string
//!   PORT          HTTP port (default: 8090)

use std::sync::Arc;
use std::convert::Infallible;
use axum::{Router, Json, extract::State, response::sse::{Event, Sse}, routing::{get, post}};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
struct ActionRequest {
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ActionResponse {
    result: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

struct PostgresDurableKv {
    pool: PgPool,
}

impl PostgresDurableKv {
    async fn connect(url: &str) -> Self {
        let pool = PgPool::connect(url).await
            .expect("Failed to connect to Postgres");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )"
        )
        .execute(&pool)
        .await
        .expect("Failed to create kv_store table");

        tracing::info!("Connected to Postgres, kv_store table ready");
        Self { pool }
    }

    async fn kv_set(&self, payload: serde_json::Value) -> Result<serde_json::Value, String> {
        let key = payload.get("key").and_then(|v| v.as_str())
            .ok_or("kv_set requires 'key' (string)")?;
        let value = payload.get("value")
            .ok_or("kv_set requires 'value'")?;

        sqlx::query(
            "INSERT INTO kv_store (key, value, updated_at) VALUES ($1, $2, NOW())
             ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()"
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;

        Ok(serde_json::json!({ "stored": true, "key": key }))
    }

    async fn kv_get(&self, payload: serde_json::Value) -> Result<serde_json::Value, String> {
        let key = payload.get("key").and_then(|v| v.as_str())
            .ok_or("kv_get requires 'key' (string)")?;

        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT value FROM kv_store WHERE key = $1"
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;

        match row {
            Some((value,)) => Ok(serde_json::json!({ "found": true, "key": key, "value": value })),
            None => Ok(serde_json::json!({ "found": false, "key": key, "value": null })),
        }
    }

    async fn kv_delete(&self, payload: serde_json::Value) -> Result<serde_json::Value, String> {
        let key = payload.get("key").and_then(|v| v.as_str())
            .ok_or("kv_delete requires 'key' (string)")?;

        sqlx::query("DELETE FROM kv_store WHERE key = $1")
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        Ok(serde_json::json!({}))
    }

    async fn kv_list(&self) -> Result<serde_json::Value, String> {
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT key, value FROM kv_store ORDER BY key"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;

        let mut entries = serde_json::Map::new();
        for (key, value) in &rows {
            entries.insert(key.clone(), value.clone());
        }

        Ok(serde_json::json!({ "entries": entries, "count": rows.len() }))
    }

    async fn kv_query(&self, payload: serde_json::Value) -> Result<serde_json::Value, String> {
        let pattern = payload.get("pattern").and_then(|v| v.as_str())
            .ok_or("kv_query requires 'pattern' (string)")?;

        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT key, value FROM kv_store WHERE key ~ $1 ORDER BY key"
        )
        .bind(pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;

        let mut matches = serde_json::Map::new();
        for (key, value) in &rows {
            matches.insert(key.clone(), value.clone());
        }

        Ok(serde_json::json!({ "matches": matches, "count": rows.len() }))
    }

    async fn kv_delete_pattern(
        &self,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let pattern = payload.get("pattern").and_then(|v| v.as_str())
            .ok_or("kv_delete_pattern requires 'pattern' (string)")?;

        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT key FROM kv_store WHERE key ~ $1"
        )
        .bind(pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;

        let deleted_keys: Vec<String> = rows.into_iter().map(|(k,)| k).collect();
        let count = deleted_keys.len();

        sqlx::query("DELETE FROM kv_store WHERE key ~ $1")
            .bind(pattern)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        Ok(serde_json::json!({ "deleted": deleted_keys, "count": count }))
    }
}

impl PostgresDurableKv {
    async fn ping(&self) -> Result<serde_json::Value, String> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| format!("DB not ready: {}", e))?;
        Ok(serde_json::json!({ "ready": true }))
    }

    async fn dispatch_action(
        &self,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        match action {
            "ping" => self.ping().await,
            "kv_set" => self.kv_set(payload).await,
            "kv_get" => self.kv_get(payload).await,
            "kv_delete" => self.kv_delete(payload).await,
            "kv_list" => self.kv_list().await,
            "kv_query" => self.kv_query(payload).await,
            "kv_delete_pattern" => self.kv_delete_pattern(payload).await,
            other => Err(format!("Unknown DurableKV action: {}", other)),
        }
    }
}

// =============================================================================
// CONTRACT HANDLERS
// =============================================================================

async fn health() -> &'static str {
    "ok"
}

async fn handle_action(
    State(state): State<Arc<PostgresDurableKv>>,
    Json(req): Json<ActionRequest>,
) -> Result<Json<ActionResponse>, Json<ErrorResponse>> {
    match state.dispatch_action(&req.action, req.payload).await {
        Ok(result) => Ok(Json(ActionResponse { result })),
        Err(e) => Err(Json(ErrorResponse { error: e })),
    }
}

async fn handle_outputs(
    State(_state): State<Arc<PostgresDurableKv>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

async fn handle_live(
    State(_state): State<Arc<PostgresDurableKv>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

async fn handle_events(
    State(_state): State<Arc<PostgresDurableKv>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = futures::stream::pending();
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL env var is required");

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8090);

    let state = Arc::new(PostgresDurableKv::connect(&database_url).await);

    let app = Router::new()
        .route("/health", get(health))
        .route("/action", post(handle_action))
        .route("/outputs", get(handle_outputs))
        .route("/live", get(handle_live))
        .route("/events", get(handle_events))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Sidecar listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await
        .expect("Failed to bind");
    axum::serve(listener, app).await
        .expect("Server error");
}

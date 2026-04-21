use std::sync::Arc;
use tokio::sync::Mutex;
use weft_nodes::TriggerService;
use sqlx::PgPool;

use crate::trigger_store;

pub struct AppState {
    pub trigger_service: Arc<Mutex<TriggerService>>,
    pub restate_url: String,
    pub restate_admin_url: String,
    pub executor_url: String,
    pub db_pool: PgPool,
    pub instance_id: String,
    /// Internal API key, loaded once at startup (no per-request env reads).
    pub internal_api_key: String,
    /// Carries `x-internal-api-key` by default. Internal targets only;
    /// never user-controlled URLs (the key would leak).
    pub http_client: reqwest::Client,
    pub node_registry: &'static weft_nodes::NodeTypeRegistry,
}

impl AppState {
    pub async fn new() -> Self {
        let restate_url = std::env::var("RESTATE_URL")
            .unwrap_or_else(|_| "http://localhost:8180".to_string());
        let restate_admin_url = std::env::var("RESTATE_ADMIN_URL")
            .unwrap_or_else(|_| {
                restate_url.replace(":8080", ":9070").replace(":8180", ":9170")
            });
        let executor_url = std::env::var("EXECUTOR_URL")
            .unwrap_or_else(|_| "http://localhost:9081".to_string());
        
        // Generate unique instance ID for trigger claiming
        let instance_id = trigger_store::generate_instance_id();
        tracing::info!("Instance ID: {}", instance_id);
        
        // Initialize PostgreSQL database - REQUIRED, crash if unavailable
        let db_pool = Self::init_database().await
            .expect("Failed to connect to database. DATABASE_URL must be set and database must be reachable.");
        
        let node_registry: &'static weft_nodes::NodeTypeRegistry =
            Box::leak(Box::new(weft_nodes::NodeTypeRegistry::new()));

        // Load once at startup; never re-read from env per request.
        let internal_api_key = std::env::var("INTERNAL_API_KEY").unwrap_or_default();

        // Default x-internal-api-key header. Internal targets only;
        // never user-controlled URLs (the key would leak).
        let http_client = {
            let mut headers = reqwest::header::HeaderMap::new();
            if !internal_api_key.is_empty() {
                if let Ok(val) = reqwest::header::HeaderValue::from_str(&internal_api_key) {
                    headers.insert("x-internal-api-key", val);
                }
            }
            reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .expect("failed to build internal HTTP client")
        };

        Self {
            trigger_service: Arc::new(Mutex::new(TriggerService::with_registry(node_registry))),
            restate_url,
            restate_admin_url,
            executor_url,
            db_pool,
            instance_id,
            internal_api_key,
            http_client,
            node_registry,
        }
    }
    
    async fn init_database() -> Result<PgPool, sqlx::Error> {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433/weft_local".to_string());
        
        tracing::info!("Connecting to PostgreSQL database");
        
        let pool = PgPool::connect(&database_url).await?;

        // Incremental schema migrations (idempotent)
        sqlx::query(
            "ALTER TABLE triggers ADD COLUMN IF NOT EXISTS project_definition JSONB"
        )
        .execute(&pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to run schema migration: {}", e);
            e
        })?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS infra_pending_action (
                project_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"
        )
        .execute(&pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create infra_pending_action table: {}", e);
            e
        })?;

        // Idempotency for the per-execution fee. One-shot dedupe runs only
        // before the unique index is first created (pre-existing duplicates
        // from older code paths would otherwise make CREATE INDEX fail).
        let index_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes \
             WHERE schemaname = current_schema() \
               AND indexname = 'uniq_usage_events_execution_once')"
        )
        .fetch_one(&pool)
        .await
        .unwrap_or(false);

        if !index_exists {
            let dedup = sqlx::query(
                "DELETE FROM usage_events ue1 \
                 USING usage_events ue2 \
                 WHERE ue1.event_type = 'execution' \
                   AND ue2.event_type = 'execution' \
                   AND ue1.execution_id = ue2.execution_id \
                   AND ue1.id > ue2.id"
            )
            .execute(&pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to dedupe usage_events execution rows before index creation: {}", e);
                e
            })?;
            if dedup.rows_affected() > 0 {
                tracing::warn!(
                    "Removed {} duplicate execution-event rows during one-shot startup dedupe",
                    dedup.rows_affected()
                );
            }

            sqlx::query(
                "CREATE UNIQUE INDEX IF NOT EXISTS uniq_usage_events_execution_once \
                 ON usage_events(execution_id) WHERE event_type = 'execution'"
            )
            .execute(&pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create uniq_usage_events_execution_once index: {}", e);
                e
            })?;
        }

        Ok(pool)
    }
}

//! Trigger persistence for durable trigger management.
//!
//! This module handles storing triggers in PostgreSQL so they survive restarts.
//! On startup, the TriggerService loads all active triggers and restarts them.
//!
//! Multi-instance support:
//! - Each instance has a unique instance_id
//! - Triggers are claimed by instances via instance_id column
//! - Heartbeats update last_heartbeat to detect crashed instances
//! - Stale triggers (no heartbeat for 2 minutes) are recovered by other instances
//!
//! ## SCHEMA SYNC WARNING
//!
//! The `triggers` table schema is defined in:
//!   - `weavemind/init-db/01-init.sql` (cloud, source of truth)
//!   - `init-db.sql` (local dev, should match)
//!
//! When modifying the schema, you MUST update:
//!   1. The SQL files above
//!   2. `TriggerRecord` struct below
//!   3. `TriggerRow` type alias below
//!   4. `row_to_record()` function below

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::crypto;

/// Trigger record stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct TriggerRecord {
    pub id: String,
    pub projectId: String,
    pub triggerNodeId: String,
    pub triggerCategory: String,
    pub nodeType: String,
    pub userId: Option<String>,
    pub config: serde_json::Value,
    pub credentials: Option<serde_json::Value>,
    pub projectDefinition: Option<serde_json::Value>,
    pub status: String,
    pub instanceId: Option<String>,
    pub lastHeartbeat: Option<chrono::DateTime<chrono::Utc>>,
    pub setupRunCounter: i32,
    pub setupExecutionId: Option<String>,
    pub projectHash: Option<String>,
    pub createdAt: chrono::DateTime<chrono::Utc>,
    pub updatedAt: chrono::DateTime<chrono::Utc>,
}

/// Row type returned by trigger queries (matches SELECT column order)
#[derive(sqlx::FromRow)]
struct TriggerRow {
    id: String,
    project_id: String,
    trigger_node_id: String,
    trigger_category: String,
    node_type: String,
    user_id: Option<String>,
    config: serde_json::Value,
    credentials: Option<serde_json::Value>,
    project_definition: Option<serde_json::Value>,
    status: String,
    instance_id: Option<String>,
    last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
    setup_run_counter: i32,
    setup_execution_id: Option<String>,
    project_hash: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

/// Convert a database row to TriggerRecord.
/// Decrypts credentials if they are stored encrypted.
fn row_to_record(row: TriggerRow) -> TriggerRecord {
    // Decrypt credentials if present
    let credentials = row.credentials.map(|cred_value| {
        if let Some(encrypted_str) = cred_value.as_str() {
            match crypto::decrypt_credentials(encrypted_str) {
                Ok(decrypted) => decrypted,
                Err(e) => {
                    tracing::error!("Failed to decrypt credentials: {}", e);
                    serde_json::Value::Null
                }
            }
        } else {
            cred_value
        }
    });
    
    TriggerRecord {
        id: row.id,
        projectId: row.project_id,
        triggerNodeId: row.trigger_node_id,
        triggerCategory: row.trigger_category,
        nodeType: row.node_type,
        userId: row.user_id,
        config: row.config,
        credentials,
        projectDefinition: row.project_definition,
        status: row.status,
        instanceId: row.instance_id,
        lastHeartbeat: row.last_heartbeat,
        setupRunCounter: row.setup_run_counter,
        setupExecutionId: row.setup_execution_id,
        projectHash: row.project_hash,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    }
}

/// Parameters for upserting a trigger.
pub struct UpsertTriggerParams<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub trigger_node_id: &'a str,
    pub trigger_category: &'a str,
    pub node_type: &'a str,
    pub user_id: Option<&'a str>,
    pub config: &'a serde_json::Value,
    pub credentials: Option<&'a serde_json::Value>,
    pub project_definition: Option<&'a serde_json::Value>,
    pub project_hash: Option<&'a str>,
}

/// Insert or update a trigger in the database.
/// Credentials are encrypted before storage.
pub async fn upsert_trigger(
    pool: &PgPool,
    params: &UpsertTriggerParams<'_>,
) -> Result<(), sqlx::Error> {
    let id = params.id;
    // Encrypt credentials before storing
    let encrypted_credentials: Option<serde_json::Value> = match params.credentials {
        Some(cred) => {
            let encrypted = crypto::encrypt_credentials(cred)
                .map_err(|e| {
                    tracing::error!("Failed to encrypt credentials for trigger {}: {}", id, e);
                    sqlx::Error::Protocol(format!("Failed to encrypt credentials: {}", e))
                })?;
            Some(serde_json::Value::String(encrypted))
        }
        None => None,
    };
    
    sqlx::query(
        r#"
        INSERT INTO triggers (id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, project_hash, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
        ON CONFLICT (id) DO UPDATE SET
            project_id = EXCLUDED.project_id,
            trigger_node_id = EXCLUDED.trigger_node_id,
            trigger_category = EXCLUDED.trigger_category,
            node_type = EXCLUDED.node_type,
            config = EXCLUDED.config,
            credentials = EXCLUDED.credentials,
            project_definition = EXCLUDED.project_definition,
            project_hash = EXCLUDED.project_hash,
            status = 'pending',
            instance_id = NULL,
            updated_at = NOW()
        "#,
    )
    .bind(params.id)
    .bind(params.project_id)
    .bind(params.trigger_node_id)
    .bind(params.trigger_category)
    .bind(params.node_type)
    .bind(params.user_id)
    .bind(params.config)
    .bind(&encrypted_credentials)
    .bind(params.project_definition)
    .bind(params.project_hash)
    .execute(pool)
    .await?;

    Ok(())
}

/// Claim pending triggers for this instance (atomic operation)
/// Returns triggers that were successfully claimed
pub async fn claim_pending_triggers(
    pool: &PgPool,
    instance_id: &str,
    limit: i32,
) -> Result<Vec<TriggerRecord>, sqlx::Error> {
    // Use a transaction with row-level locking to avoid race conditions
    let mut tx = pool.begin().await?;

    // Find and lock pending triggers
    let trigger_ids: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT id FROM triggers
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;

    if trigger_ids.is_empty() {
        tx.commit().await?;
        return Ok(vec![]);
    }

    let ids: Vec<&str> = trigger_ids.iter().map(|t| t.0.as_str()).collect();
    let now = chrono::Utc::now();

    // Claim the triggers
    for id in &ids {
        sqlx::query(
            r#"
            UPDATE triggers 
            SET status = 'running', instance_id = $1, last_heartbeat = $2, updated_at = $2
            WHERE id = $3
            "#,
        )
        .bind(instance_id)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }

    // Fetch the full trigger records
    let triggers = sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        WHERE instance_id = $1 AND status = 'running'
        "#,
    )
    .bind(instance_id)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!("Instance {} claimed {} triggers", instance_id, triggers.len());

    Ok(triggers.into_iter().map(row_to_record).collect())
}

/// Update trigger status
pub async fn update_trigger_status(
    pool: &PgPool,
    trigger_id: &str,
    status: &str,
    instance_id: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let now = chrono::Utc::now();
    
    let result = sqlx::query(
        r#"
        UPDATE triggers 
        SET status = $1, instance_id = $2, last_heartbeat = $3, updated_at = $3
        WHERE id = $4
        "#,
    )
    .bind(status)
    .bind(instance_id)
    .bind(now)
    .bind(trigger_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Update trigger config (used after trigger setup resolves upstream values)
pub async fn update_trigger_config(
    pool: &PgPool,
    trigger_id: &str,
    config: &serde_json::Value,
) -> Result<bool, sqlx::Error> {
    let now = chrono::Utc::now();
    
    let result = sqlx::query(
        r#"
        UPDATE triggers 
        SET config = $1, updated_at = $2
        WHERE id = $3
        "#,
    )
    .bind(config)
    .bind(now)
    .bind(trigger_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Update heartbeat for all triggers owned by this instance
pub async fn update_heartbeat(
    pool: &PgPool,
    instance_id: &str,
) -> Result<u64, sqlx::Error> {
    let now = chrono::Utc::now();
    
    let result = sqlx::query(
        r#"
        UPDATE triggers 
        SET last_heartbeat = $1, updated_at = $1
        WHERE instance_id = $2 AND status = 'running'
        "#,
    )
    .bind(now)
    .bind(instance_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Stop all running/pending triggers for a project (returns the trigger IDs that were stopped)
pub async fn stop_triggers_by_project(pool: &PgPool, project_id: &str) -> Result<Vec<String>, sqlx::Error> {
    let now = chrono::Utc::now();
    
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        UPDATE triggers 
        SET status = 'stopped', instance_id = NULL, updated_at = $1
        WHERE project_id = $2 AND status IN ('running', 'pending', 'setup_pending')
        RETURNING id
        "#,
    )
    .bind(now)
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Get a specific trigger by ID
pub async fn get_trigger(pool: &PgPool, trigger_id: &str) -> Result<Option<TriggerRecord>, sqlx::Error> {
    sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        WHERE id = $1
        "#,
    )
    .bind(trigger_id)
    .fetch_optional(pool)
    .await
    .map(|opt| opt.map(row_to_record))
}

/// List all triggers
pub async fn list_all_triggers(pool: &PgPool) -> Result<Vec<TriggerRecord>, sqlx::Error> {
    sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(row_to_record).collect())
}

/// List triggers for a specific project
pub async fn list_triggers_by_project(pool: &PgPool, project_id: &str) -> Result<Vec<TriggerRecord>, sqlx::Error> {
    sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        WHERE project_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(row_to_record).collect())
}

/// List triggers for a specific user
pub async fn list_triggers_by_user(pool: &PgPool, user_id: &str) -> Result<Vec<TriggerRecord>, sqlx::Error> {
    sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        WHERE user_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(row_to_record).collect())
}

/// Recover triggers from crashed instances (stale heartbeats)
pub async fn recover_stale_triggers(
    pool: &PgPool,
    stale_threshold_seconds: i64,
) -> Result<u64, sqlx::Error> {
    let threshold = chrono::Utc::now() - chrono::Duration::seconds(stale_threshold_seconds);
    let now = chrono::Utc::now();
    
    let result = sqlx::query(
        r#"
        UPDATE triggers 
        SET status = 'pending', instance_id = NULL, updated_at = $1
        WHERE status = 'running' AND last_heartbeat < $2
        "#,
    )
    .bind(now)
    .bind(threshold)
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        tracing::info!("Recovered {} stale triggers (heartbeat older than {}s)", result.rows_affected(), stale_threshold_seconds);
    }
    Ok(result.rows_affected())
}

/// List triggers stuck in setup_pending state (for recovery on restart)
pub async fn list_setup_pending_triggers(pool: &PgPool) -> Result<Vec<TriggerRecord>, sqlx::Error> {
    let triggers = sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        WHERE status = 'setup_pending'
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(triggers.into_iter().map(row_to_record).collect())
}

/// Claim orphaned triggers that were running on other instances
/// This is called on startup to reclaim triggers from instances that shut down
pub async fn claim_orphaned_triggers(
    pool: &PgPool,
    current_instance_id: &str,
    limit: i32,
) -> Result<Vec<TriggerRecord>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let now = chrono::Utc::now();

    // Find triggers that are 'running' but owned by a different instance
    // These are orphaned because the owning instance is gone
    let trigger_ids: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT id FROM triggers
        WHERE status = 'running' 
          AND instance_id IS NOT NULL 
          AND instance_id != $1
        ORDER BY created_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(current_instance_id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;

    if trigger_ids.is_empty() {
        tx.commit().await?;
        return Ok(vec![]);
    }

    // Claim the triggers for this instance
    for (id,) in &trigger_ids {
        sqlx::query(
            r#"
            UPDATE triggers 
            SET instance_id = $1, last_heartbeat = $2, updated_at = $2
            WHERE id = $3
            "#,
        )
        .bind(current_instance_id)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }

    // Fetch the full trigger records
    let triggers = sqlx::query_as::<_, TriggerRow>(
        r#"
        SELECT id, project_id, trigger_node_id, trigger_category, node_type, user_id, config, credentials, project_definition, status, instance_id, last_heartbeat, setup_run_counter, setup_execution_id, project_hash, created_at, updated_at
        FROM triggers
        WHERE instance_id = $1 AND status = 'running'
        "#,
    )
    .bind(current_instance_id)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!("Instance {} claimed {} orphaned triggers from other instances", current_instance_id, triggers.len());

    Ok(triggers.into_iter().map(row_to_record).collect())
}

/// Atomically increment the setup_run_counter.
/// Returns the new counter value. Use set_setup_execution_id() afterwards
/// to register the execution ID built from this counter.
pub async fn increment_setup_run_counter(
    pool: &PgPool,
    trigger_id: &str,
) -> Result<i32, sqlx::Error> {
    let row: (i32,) = sqlx::query_as(
        r#"
        UPDATE triggers
        SET setup_run_counter = setup_run_counter + 1,
            updated_at = NOW()
        WHERE id = $1
        RETURNING setup_run_counter
        "#,
    )
    .bind(trigger_id)
    .fetch_one(pool)
    .await?;

    Ok(row.0)
}

/// Set a transitional pending action for a trigger (activating/deactivating).
/// Upserts so repeated calls are idempotent.
pub async fn set_trigger_pending_action(
    pool: &PgPool,
    trigger_id: &str,
    action: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO trigger_pending_action (trigger_id, action)
        VALUES ($1, $2)
        ON CONFLICT (trigger_id) DO UPDATE SET action = $2, created_at = NOW()
        "#,
    )
    .bind(trigger_id)
    .bind(action)
    .execute(pool)
    .await?;

    Ok(())
}

/// Clear the pending action for a trigger.
pub async fn clear_trigger_pending_action(
    pool: &PgPool,
    trigger_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM trigger_pending_action WHERE trigger_id = $1")
        .bind(trigger_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get all pending actions for triggers belonging to a project.
pub async fn get_trigger_pending_actions_by_project(
    pool: &PgPool,
    project_id: &str,
) -> Result<Vec<(String, String, chrono::DateTime<chrono::Utc>)>, sqlx::Error> {
    sqlx::query_as::<_, (String, String, chrono::DateTime<chrono::Utc>)>(
        r#"
        SELECT tpa.trigger_id, tpa.action, tpa.created_at
        FROM trigger_pending_action tpa
        JOIN triggers t ON t.id = tpa.trigger_id
        WHERE t.project_id = $1
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await
}

/// Set the setup_execution_id for a trigger (without incrementing the counter).
pub async fn set_setup_execution_id(
    pool: &PgPool,
    trigger_id: &str,
    execution_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE triggers SET setup_execution_id = $1 WHERE id = $2")
        .bind(execution_id)
        .bind(trigger_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Clear the setup_execution_id for a trigger (set to NULL).
pub async fn clear_setup_execution_id(
    pool: &PgPool,
    trigger_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE triggers SET setup_execution_id = NULL WHERE id = $1")
        .bind(trigger_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Generate a unique instance ID for this process
pub fn generate_instance_id() -> String {
    use std::process;
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let pid = process::id();
    let random: u32 = rand::random();
    format!("{}-{}-{:08x}", hostname, pid, random)
}

//! A program minting an instance token (`ctx.tokens().mint_for_instance`).
//!
//! The token acts inside one instance of the program's own project and
//! nothing else, and it always expires: it is meant for a browser or an
//! extension working with that instance. The value leaves here once, in
//! the answer; the row keeps only its hash, the same show-once shape
//! `weft token mint` has.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};

use weft_broker_client::protocol::ProgramMintInstanceTokenRequest;
use weft_core::program::MintedInstanceToken;

use crate::auth::{AuthedCaller, Role};
use crate::state::BrokerState;

pub async fn mint_instance_token(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ProgramMintInstanceTokenRequest>,
) -> Result<Json<MintedInstanceToken>, (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    let run = crate::scope::require_execution_id_scope(&state.scope_cache, &state.pool, &caller, &req.execution_id).await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    let expires_at = weft_core::signal_token::expiry_at(now, req.expires_in_secs)
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let token = weft_core::signal_token::generate_token();
    let stored = InstanceTokenRow {
        id: req.id,
        tenant: &run.tenant,
        project_id: run.project,
        instance: &req.instance,
        name: req.name.as_deref(),
        displays: req.displays,
        created_at: now,
        expires_at,
    };
    if !store_instance_token(&state.pool, &stored, &token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint instance token: {e}")))?
    {
        return Err((StatusCode::CONFLICT, format!("token id {} already names another token", req.id)));
    }
    let id = req.id;
    Ok(Json(MintedInstanceToken { id, token, expires_at_unix: expires_at }))
}

/// An instance token's row, everything but its value.
pub struct InstanceTokenRow<'a> {
    pub id: uuid::Uuid,
    pub tenant: &'a str,
    pub project_id: uuid::Uuid,
    pub instance: &'a weft_core::instance::InstanceId,
    pub name: Option<&'a str>,
    /// The instance's own copies' displays (a bridge's pairing code): the
    /// display door reads an instance token's own copy only.
    pub displays: bool,
    pub created_at: u64,
    pub expires_at: u64,
}

/// Store the token `value` as `row`, keeping only its hash. An id that
/// already names this instance's token in this project REPLACES it (a new
/// value, the old one dead): a replayed run mints under the id it
/// journaled. `false` when the id names any other token, and nothing was
/// written.
// SYNC: signal_token insert <-> crates/weft-dispatcher/src/journal/postgres.rs (mint_signal_token, revoke_instance_tokens)
pub async fn store_instance_token(pool: &sqlx::PgPool, row: &InstanceTokenRow<'_>, value: &str) -> anyhow::Result<bool> {
    let written = sqlx::query(
        "INSERT INTO signal_token \
         (id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
          allowed_displays, all_displays, created_at, instance_id, expires_at) \
         VALUES ($1, $2, $3, $4, $5, ARRAY[$6]::uuid[], '{}', '{}', $7, $8, $9, $10) \
         ON CONFLICT (id) DO UPDATE SET \
             token_hash = EXCLUDED.token_hash, recognizer = EXCLUDED.recognizer, name = EXCLUDED.name, \
             all_displays = EXCLUDED.all_displays, created_at = EXCLUDED.created_at, \
             expires_at = EXCLUDED.expires_at \
         WHERE signal_token.tenant_id = EXCLUDED.tenant_id \
           AND signal_token.instance_id = EXCLUDED.instance_id \
           AND signal_token.allowed_projects = EXCLUDED.allowed_projects",
    )
    .bind(row.id)
    .bind(weft_core::signal_token::token_hash(value))
    .bind(weft_core::signal_token::recognizer(value))
    .bind(row.tenant)
    .bind(row.name)
    .bind(row.project_id)
    .bind(row.displays)
    .bind(row.created_at as i64)
    .bind(row.instance.as_str())
    .bind(row.expires_at as i64)
    .execute(pool)
    .await?;
    Ok(written.rows_affected() == 1)
}

//! API token administration. Architecture-4 split: token CRUD lives
//! here (mint, list, revoke); kind-specific signal enumeration and
//! firing routes live in `signal.rs` and are general-purpose.
//!
//! Browser extension and any future external consumer hits:
//!   - `POST /api-tokens` (mint), `GET /api-tokens` (list), `DELETE`.
//!   - `GET /api-token/{tk}/signals` for scoped enumeration.
//!   - `GET /api-token/{tk}/health` for auth probe.
//!   - `POST /signal/{token}` (fire), `DELETE /signal/{token}` (cancel).
//! No kind-specific endpoints in the dispatcher.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use crate::state::DispatcherState;

#[derive(Debug, serde::Deserialize)]
pub struct MintTokenBody {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
    /// Token shape:
    ///   - "friendly" (default): `wm_tk_<adj>-<noun>-<NN>`. Easy
    ///     to read, low entropy. Fine on localhost where CORS
    ///     blocks cross-origin probing.
    ///   - "hard": `wm_tk_<32-hex>`. High entropy, ugly. Use
    ///     when exposing the dispatcher beyond localhost.
    #[serde(default)]
    pub style: Option<String>,
    /// Scope vectors. Empty = wildcard. Each non-empty vector
    /// narrows the signals this token can enumerate. Tags must
    /// pass the same charset rule used for `_tags`
    /// (`[A-Za-z0-9_-]{1,64}`); rejected at body parse time.
    #[serde(default, rename = "allowedKinds")]
    pub allowed_kinds: Vec<String>,
    #[serde(default, rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(default, rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct MintedToken {
    pub token: String,
    pub name: Option<String>,
    #[serde(rename = "allowedKinds")]
    pub allowed_kinds: Vec<String>,
    #[serde(rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
}

pub async fn mint_token(
    State(state): State<DispatcherState>,
    Json(body): Json<MintTokenBody>,
) -> Result<Json<MintedToken>, (StatusCode, String)> {
    // Pick the token shape. Default = friendly (`wm_tk_<adj>-
    // <noun>-<NN>`); explicit "hard" gives a uuid-backed body
    // for setups exposed beyond localhost.
    let token = match body.style.as_deref() {
        Some("hard") => crate::api::extension_names::hard_token(),
        _ => crate::api::extension_names::friendly_token(),
    };

    // Optional human label, separate from the token itself. If
    // the caller didn't supply one, mirror the token suffix
    // (without the wm_tk_ prefix) so `weft token ls` still
    // shows something readable instead of an empty column.
    let name = body
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| token.strip_prefix("wm_tk_").unwrap_or(&token).to_string());

    // Validate tag charset on the way in. Same rule as _tags in
    // .weft source so tokens and signals share one vocabulary.
    if let Err(e) = weft_core::tag::validate_tags(&body.allowed_kinds) {
        return Err((StatusCode::BAD_REQUEST, format!("allowed_kinds: {e}")));
    }
    if let Err(e) = weft_core::tag::validate_tags(&body.allowed_tags) {
        return Err((StatusCode::BAD_REQUEST, format!("allowed_tags: {e}")));
    }

    let api_token = crate::journal::ApiToken {
        token: token.clone(),
        name: Some(name.clone()),
        allowed_kinds: body.allowed_kinds,
        allowed_projects: body.allowed_projects,
        allowed_tags: body.allowed_tags,
        metadata: body.metadata,
        created_at: 0, // ignored by mint_api_token; the impl stamps now()
    };

    state
        .journal
        .mint_api_token(&api_token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(Json(MintedToken {
        token,
        name: Some(name),
        allowed_kinds: api_token.allowed_kinds,
        allowed_projects: api_token.allowed_projects,
        allowed_tags: api_token.allowed_tags,
    }))
}

pub async fn list_tokens(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<MintedToken>>, (StatusCode, String)> {
    let tokens = state
        .journal
        .list_api_tokens()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(Json(
        tokens
            .into_iter()
            .map(|t| MintedToken {
                token: t.token,
                name: t.name,
                allowed_kinds: t.allowed_kinds,
                allowed_projects: t.allowed_projects,
                allowed_tags: t.allowed_tags,
            })
            .collect(),
    ))
}

pub async fn revoke_token(
    State(state): State<DispatcherState>,
    Path(identifier): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let removed = state
        .journal
        .revoke_api_token(&identifier)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("no token matching '{identifier}'")))
    }
}



//! Signal-token administration. Token CRUD lives here (mint, list,
//! revoke); kind-specific signal enumeration and firing routes live in
//! `signal.rs` and are general-purpose.
//!
//! Token model (show-once): the value (`wft-` + six words, see
//! `signal_token_names`) is generated server-side, returned EXACTLY ONCE in
//! the mint response, and stored only as a sha256 hash plus a display
//! recognizer. Listing returns metadata only; no endpoint can re-reveal a
//! token. Revocation addresses the token's id, never its value.
//!
//! An external consumer (a client that listens for + answers a project's
//! waiting nodes) hits:
//!   - `POST /signal-tokens` (mint), `GET /signal-tokens` (list),
//!     `DELETE /signal-tokens/{id}` (revoke) — tenant-authenticated admin.
//!   - `GET /signal-token/signals`, `DELETE /signal-token/signals`,
//!     `GET /signal-token/health` — the token itself authenticates, via
//!     `Authorization: Bearer wft-...` (never in the URL, where proxies and
//!     access logs would capture it).
//!   - `POST /signal/{token}` (fire), `DELETE /signal/{token}` (cancel):
//!     per-SIGNAL fire tokens, a separate credential whose whole job is to
//!     be a paste-able URL.
//! No kind-specific endpoints in the dispatcher.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;

use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;

#[derive(Debug, serde::Deserialize)]
pub struct MintTokenBody {
    /// The user-facing label (a display name), never part of the token value.
    #[serde(default)]
    pub name: Option<String>,
    /// Scope vectors. Empty = wildcard. Each non-empty vector narrows
    /// the signals this token can enumerate. Tags must pass the tag
    /// charset (`[A-Za-z0-9_-]{1,64}`); rejected at parse time.
    #[serde(default, rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(default, rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
}

/// The mint response: the ONLY place the full token value ever appears.
#[derive(Debug, Serialize)]
pub struct MintedToken {
    pub id: uuid::Uuid,
    /// The full secret, shown once. The client copies it now; the server
    /// keeps only its hash and can never show it again.
    pub token: String,
    pub recognizer: String,
    pub name: Option<String>,
    /// The paste-able connect string for clients that take one URL
    /// (`<public-base>/signal-token/<token>`): clients PARSE it into base +
    /// token and present the token via `Authorization: Bearer` — the wire
    /// requests never carry it in a path.
    pub url: String,
    #[serde(rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
}

/// A listed token: metadata + recognizer only, no secret.
#[derive(Debug, Serialize)]
pub struct TokenSummary {
    pub id: uuid::Uuid,
    pub recognizer: String,
    pub name: Option<String>,
    #[serde(rename = "createdAtUnix")]
    pub created_at_unix: u64,
    #[serde(rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
}

/// Build a token's paste-able connect string from the dispatcher's public base.
fn token_url(public_base: &str, token: &str) -> String {
    format!("{}/signal-token/{token}", public_base.trim_end_matches('/'))
}

pub async fn mint_token(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(body): Json<MintTokenBody>,
) -> Result<Json<MintedToken>, (StatusCode, String)> {
    use crate::api::signal_token_names as names;

    let name = body
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    // Validate tag charset on the way in. Same rule as _tags in .weft
    // source so tokens and signals share one vocabulary.
    if let Err(e) = weft_core::tag::validate_tags(&body.allowed_tags) {
        return Err((StatusCode::BAD_REQUEST, format!("allowed_tags: {e}")));
    }

    let token = names::generate_token();
    let signal_token = crate::journal::SignalToken {
        id: uuid::Uuid::new_v4(),
        token_hash: names::token_hash(&token),
        recognizer: names::recognizer(&token),
        tenant_id: caller.0.as_str().to_string(),
        name: name.clone(),
        allowed_projects: body.allowed_projects,
        allowed_tags: body.allowed_tags,
        // Stamp the mint time here (the canonical wall clock) and store it
        // verbatim, so both the postgres and mock journals agree instead of one
        // stamping now() and the other keeping a placeholder.
        created_at: crate::lease::now_unix() as u64,
    };

    state
        .journal
        .mint_signal_token(&signal_token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let url = token_url(&state.public_base_url, &token);
    Ok(Json(MintedToken {
        id: signal_token.id,
        token,
        recognizer: signal_token.recognizer,
        name,
        url,
        allowed_projects: signal_token.allowed_projects,
        allowed_tags: signal_token.allowed_tags,
    }))
}

pub async fn list_tokens(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<Vec<TokenSummary>>, (StatusCode, String)> {
    let tokens = state
        .journal
        .list_signal_tokens(caller.0.as_str())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(Json(
        tokens
            .into_iter()
            .map(|t| TokenSummary {
                id: t.id,
                recognizer: t.recognizer,
                name: t.name,
                created_at_unix: t.created_at,
                allowed_projects: t.allowed_projects,
                allowed_tags: t.allowed_tags,
            })
            .collect(),
    ))
}

pub async fn revoke_token(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let removed = state
        .journal
        .revoke_signal_token(id, caller.0.as_str())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("no token with id {id}")))
    }
}

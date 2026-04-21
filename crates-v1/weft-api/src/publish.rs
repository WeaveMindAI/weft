//! Publish handler: CRUD for `published_projects`.
//!
//! Weft-api owns the base publish flow (create, update, fetch by slug, delete).
//! Local OSS deployments get full publish semantics without needing the cloud:
//! you can publish a project to `/p/<slug>` and anyone with network access to
//! your local instance can open it. The cloud-api extends this with credit
//! gating, rate limiting, and visitor sessions, but does NOT duplicate the
//! CRUD path; the dashboard always talks to whichever backend it's configured
//! against for publish CRUD.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::sync::Arc;
use weft_core::executor_core::ProjectExecutionRequest;

use crate::state::AppState;

#[derive(Debug, Serialize, FromRow)]
pub struct PublishedProject {
    pub id: String,
    pub slug: String,
    /// Denormalized from `user.username` at publish time. Combined with
    /// `slug` forms the public URL `/p/<username>/<slug>` and the
    /// uniqueness key for the publication.
    pub username: String,
    pub user_id: String,
    pub project_id: Option<String>,
    pub project_name: String,
    pub description: Option<String>,
    /// Legacy ghost columns kept nullable for backwards compat. Current
    /// publishes leave these null and read the live code from the
    /// deployment `projects` row via `project_id`. See H6.
    pub weft_code: Option<String>,
    pub loom_code: Option<String>,
    pub layout_code: Option<String>,
    pub is_live: bool,
    pub view_count: i64,
    pub run_count: i64,
    pub published_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Deployer-configured per-slug rate limit in requests per minute.
    /// Null means "use default". See cloud-api's
    /// `PUBLISH_RATE_LIMIT_DEFAULT_PER_MINUTE`. Clamped to
    /// `PUBLISH_RATE_LIMIT_MAX_PER_MINUTE` at publish/update time.
    pub rate_limit_per_minute: Option<i32>,
    /// The builder project this deployment was cloned from. Joined
    /// from `projects.origin_project_id` on the deployment row at
    /// list time. Lets the dashboard roll up per-builder runtime
    /// status (purple thunder lit when the builder OR any deployment
    /// descendant has a running trigger). Null for orphan mappings
    /// whose project row has been deleted.
    pub origin_project_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PublishRequest {
    pub project_id: String,
    pub slug: String,
    pub description: Option<String>,
    /// Weft source to publish. The dashboard sends an explicit payload
    /// (optionally with sensitive fields stripped client-side via
    /// `$lib/ai/sanitize`) so the deployer can publish a sanitized view
    /// without mutating their builder project. If null/missing, the
    /// backend falls back to the builder project row's weft_code. That
    /// path exists for CLI/test harnesses that don't implement the
    /// strip-on-publish flow.
    #[serde(default)]
    pub weft_code: Option<String>,
    /// Loom source to publish. Same story as weft_code: the dashboard
    /// serializes the current setup manifest and sends it explicitly.
    #[serde(default)]
    pub loom_code: Option<String>,
    /// Layout source to publish.
    #[serde(default)]
    pub layout_code: Option<String>,
    /// Visitor allowlist computed by the dashboard from the loom's input
    /// and output directives. Shape:
    ///   { "inputs":  { "<nodeId>": ["field1", ...], ... },
    ///     "outputs": { "<nodeId>": ["port1",  ...], ... } }
    /// Stored on the deployment project row and consulted on every visitor
    /// run to gate which fields are writable and which outputs are
    /// visitor-visible. Optional for backwards compatibility; missing or
    /// null means "no visitor access at all", which is the safer default
    /// for legacy deployments but effectively bricks the visitor UI until
    /// they re-publish.
    #[serde(default)]
    pub visitor_access: Option<serde_json::Value>,
    /// Per-slug rate limit (requests per minute) chosen by the deployer.
    /// Hard-capped by the backend at `PUBLISH_RATE_LIMIT_MAX_PER_MINUTE`
    /// (see cloud-api::publish). Zero disables visitor runs entirely
    /// (the /run endpoint returns 429 on every request).
    #[serde(default)]
    pub rate_limit_per_minute: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePublishRequest {
    pub is_live: Option<bool>,
    pub description: Option<String>,
    /// Update the per-slug rate limit. Hard-capped at `RATE_LIMIT_MAX`;
    /// values above the cap are rejected with 400. `Some(0)` effectively
    /// disables the deployment (every visitor run returns 429).
    #[serde(default)]
    pub rate_limit_per_minute: Option<i32>,
}

// ── Slug validation ──────────────────────────────────────────────────────────

const SLUG_MIN: usize = 3;
const SLUG_MAX: usize = 63;

// Slugs we refuse to hand out because they collide with reserved routes on
// the main site. Keep in sync with the frontend list; the backend is the
// authority.
const RESERVED_SLUGS: &[&str] = &[
    "api", "app", "admin", "auth", "login", "logout", "account", "community",
    "share", "publish", "p", "settings", "docs", "help", "pricing", "terms",
    "privacy", "about", "contact", "dashboard", "new", "edit", "delete",
    "health",
];

fn validate_slug(slug: &str) -> Result<(), String> {
    if slug.len() < SLUG_MIN {
        return Err(format!("Slug must be at least {} characters", SLUG_MIN));
    }
    if slug.len() > SLUG_MAX {
        return Err(format!("Slug must be at most {} characters", SLUG_MAX));
    }
    // a-z, 0-9, '-' only, no leading/trailing/consecutive hyphens.
    let mut prev_hyphen = false;
    for (i, c) in slug.chars().enumerate() {
        let is_alnum = c.is_ascii_lowercase() || c.is_ascii_digit();
        let is_hyphen = c == '-';
        if !is_alnum && !is_hyphen {
            return Err("Slug may only contain lowercase letters, digits, and hyphens".into());
        }
        if is_hyphen && (i == 0 || i == slug.len() - 1) {
            return Err("Slug cannot start or end with a hyphen".into());
        }
        if is_hyphen && prev_hyphen {
            return Err("Slug cannot contain consecutive hyphens".into());
        }
        prev_hyphen = is_hyphen;
    }
    if RESERVED_SLUGS.contains(&slug) {
        return Err("This slug is reserved".into());
    }
    Ok(())
}

// ── Auth helpers ─────────────────────────────────────────────────────────────

/// Resolve the caller's user id from request headers.
///
/// In cloud mode: cloud-api's auth middleware decodes the JWT and
/// injects `x-user-id` before forwarding to us, so the header is the
/// single authoritative source. We return `None` when the header is
/// missing so callers can 401 instead of silently operating as some
/// fallback identity.
///
/// In pure-local mode (`DEPLOYMENT_MODE=local`) we fall back to the
/// `"local"` sentinel so OSS standalone runs without any JWT
/// infrastructure.
///
/// Previously this returned `Some("local")` unconditionally, which
/// made the `Option` return type misleading: every caller had a dead
/// `None` branch. The function is now honest about when it can fail.
pub(crate) fn user_id_from_headers(headers: &HeaderMap) -> Option<String> {
    if let Some(hdr) = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        return Some(hdr.to_string());
    }
    let is_local = std::env::var("DEPLOYMENT_MODE").unwrap_or_default() == "local";
    if is_local {
        return Some("local".to_string());
    }
    None
}

// ── Handlers ─────────────────────────────────────────────────────────────────

/// POST /api/v1/publish
///
/// Publish (or re-publish) a builder project as a deployment.
///
/// A deployment is an independent `projects` row (is_deployment=true) cloned
/// from the builder project. It owns its own trigger and infra state via
/// the normal tables keyed on project_id. A thin `published_projects` row
/// maps the slug to the deployment project's id.
///
/// First publish path:
///   1. Validate slug + builder project ownership
///   2. Clone the builder's weft/loom/layout into a new `projects` row with
///      is_deployment=true, origin_project_id=builder_id
///   3. Copy the builder's current triggers into `triggers` keyed on the
///      new deployment project id (fresh IDs, status='pending', no instance)
///   4. Insert the `published_projects` row mapping slug → new project id
///
/// Re-publish path (slug already exists and belongs to caller):
///   1. Look up the existing deployment project via published_projects.project_id
///   2. Overwrite its weft/loom/layout with the builder's fresh copy
///      (preserving the deployment project's UUID so execution history and
///      trigger rows stay linked)
///   3. Replace the deployment's trigger set: delete existing rows, re-copy
///      from the builder. Active triggers are deactivated via the normal
///      unregister path: this is the "replace triggers on re-publish"
///      behaviour Quentin confirmed.
///   4. Admin-tweaked field values in the deployment's weft_code are
///      preserved by the builder's own serializer: the admin edits modify
///      `projects.weft_code` on the deployment row through the normal
///      runner update path, so "re-publish overwrites code" means admin
///      tweaks from before the re-publish are lost. The trade-off is
///      explicit: a re-publish is treated as "ship the current builder
///      state", not "merge admin tweaks". Trigger divergence detection in
///      the dashboard surfaces the difference so the admin can resync
///      admin-level edits after a re-publish.
///
/// The whole operation runs in a single transaction so a partial failure
/// doesn't leave dangling deployment projects or orphan trigger rows.
pub async fn publish_project(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<PublishRequest>,
) -> impl IntoResponse {
    let Some(user_id) = user_id_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" }))).into_response();
    };

    if let Err(msg) = validate_slug(&req.slug) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg }))).into_response();
    }

    // L-1: validate the deployer-chosen rate limit BEFORE opening the
    // transaction. It's a cheap side-effect-free check and failing it
    // inside the tx wastes a connection lease on every rejected
    // request. Null means "use default". The cloud-api read path
    // substitutes `PUBLISH_RATE_LIMIT_DEFAULT_PER_MINUTE`. `< 1`
    // (including zero) is rejected with a clear message pointing the
    // deployer at the pause toggle instead (M-4).
    const RATE_LIMIT_MAX: i32 = 500;
    if let Some(rl) = req.rate_limit_per_minute {
        if rl < 1 || rl > RATE_LIMIT_MAX {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!(
                        "rate_limit_per_minute must be between 1 and {}. To pause visitor runs, set is_live=false instead.",
                        RATE_LIMIT_MAX,
                    ),
                })),
            )
                .into_response();
        }
    }

    // Resolve the publisher's username. In cloud mode the `user` table
    // is owned by Better Auth (in the website's schema) and holds the
    // user's chosen display username, which becomes the first segment
    // of the public URL (`/p/<username>/<slug>`). In OSS local mode
    // there is no Better Auth, no `user` table, and only one user
    // (`local`), so we short-circuit with a fixed `"local"` username
    // instead of hitting a missing table.
    let username: Option<String> = if user_id == "local" {
        Some("local".to_string())
    } else {
        match sqlx::query_scalar(
            r#"SELECT username FROM "user" WHERE id = $1"#,
        )
        .bind(&user_id)
        .fetch_optional(&state.db_pool)
        .await
        {
            Ok(row) => row.and_then(|v: Option<String>| v),
            Err(e) => {
                tracing::error!("Failed to look up username for user {}: {}", user_id, e);
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
            }
        }
    };

    let Some(username) = username else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "You need to set a username before publishing. Go to your profile settings and pick one."
            })),
        )
            .into_response();
    };

    // Look up the builder project to clone. Single query for both the
    // ownership check AND the weft/loom/layout clone source so we don't
    // hit `projects` twice. `is_deployment` is checked first so we can
    // return the correct error (400 vs 404) depending on the reason the
    // publish was refused.
    #[derive(sqlx::FromRow)]
    struct BuilderRow {
        name: String,
        description: Option<String>,
        weft_code: Option<String>,
        loom_code: Option<String>,
        layout_code: Option<String>,
        is_deployment: bool,
    }
    let builder: Option<BuilderRow> = match sqlx::query_as(
        "SELECT name, description, weft_code, loom_code, layout_code, is_deployment
         FROM projects
         WHERE id = $1::uuid AND user_id = $2",
    )
    .bind(&req.project_id)
    .bind(&user_id)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!("Failed to load builder project {}: {}", req.project_id, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    let Some(builder) = builder else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Project not found" })),
        )
            .into_response();
    };
    if builder.is_deployment {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Cannot publish a deployment project. Publish its origin builder project instead." })),
        )
            .into_response();
    }
    // Resolve the weft/loom/layout sources for the deployment. The
    // dashboard sends an explicit payload so it can strip sensitive
    // field values at publish time without mutating the builder row;
    // we prefer that over the builder's DB copy. Missing body fields
    // fall back to the builder row for CLI/test callers that don't
    // implement the strip-on-publish client flow.
    //
    // `Some("")` is treated as "missing" for all three so an empty
    // submission from a misbehaving client falls back to the builder
    // row instead of producing a deployment with uncompilable code
    // that would 500 at visitor-run time (M-3).
    fn non_empty(s: Option<String>) -> Option<String> {
        s.filter(|v| !v.is_empty())
    }
    let weft_code = match (non_empty(req.weft_code.clone()), non_empty(builder.weft_code)) {
        (Some(w), _) => w,
        (None, Some(w)) => w,
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Project is missing weft code" })),
            )
                .into_response();
        }
    };
    let loom_code = match (non_empty(req.loom_code.clone()), non_empty(builder.loom_code)) {
        (Some(l), _) => l,
        (None, Some(l)) => l,
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Project is missing loom code" })),
            )
                .into_response();
        }
    };
    let layout_code = non_empty(req.layout_code.clone()).or_else(|| non_empty(builder.layout_code));
    let project_name = builder.name;
    let project_description = builder.description;

    let description = req.description.or(project_description);

    // Everything below runs in one transaction. On any error we roll
    // back so the DB stays consistent. The H4 drift cleanup (see below)
    // lives inside this tx on purpose: if the cleanup runs and then
    // the upsert fails, rollback restores the old mapping row and the
    // user's slug is never lost.
    let mut tx = match state.db_pool.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to start publish transaction: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    // H4 fix (now inside the tx): look up an existing mapping by BOTH
    // (username, slug) and (user_id, slug). The composite unique key is
    // (username, slug), but if the user renamed their username since
    // their last publish, the stale row sits under the old username,
    // which the first lookup misses. The second lookup catches drift so
    // we re-publish in place (keeping the same deployment project row)
    // instead of creating a new deployment and orphaning the old one.
    //
    // Precedence: the (username, slug) hit wins when both match. If
    // ONLY the (user_id, slug) lookup hits, we DELETE the stale row
    // (so the upsert's ON CONFLICT target matches cleanly) and reuse
    // its deployment_project_id.
    #[derive(sqlx::FromRow)]
    struct ExistingMapping {
        user_id: String,
        project_id: Option<String>,
        stored_username: String,
    }
    let existing: Option<ExistingMapping> = match sqlx::query_as(
        r#"
        SELECT user_id, project_id::text AS project_id, username AS stored_username
        FROM published_projects
        WHERE (username = $1 AND slug = $2)
           OR (user_id = $3 AND slug = $2)
        ORDER BY (username = $1) DESC, (user_id = $3) DESC, updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(&username)
    .bind(&req.slug)
    .bind(&user_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!("Failed to look up existing publication ({}, {}): {}", username, req.slug, e);
            let _ = tx.rollback().await;
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    let existing_deployment_project_id: Option<String> = match existing {
        Some(row) if row.user_id == user_id => {
            // Sweep EVERY stale row for this (user_id, slug) under a
            // different username than the one we're publishing as.
            // The winning row (matching both username and slug) is
            // untouched because the `<> $3` filter excludes it.
            // Handles both the single-drift case and the edge case
            // where multiple stale rows accumulated (e.g. two username
            // rename cycles before re-publishing). All stale rows
            // delete atomically with the rest of the publish tx.
            if let Err(e) = sqlx::query(
                "DELETE FROM published_projects \
                 WHERE user_id = $1 AND slug = $2 AND username <> $3",
            )
            .bind(&user_id)
            .bind(&req.slug)
            .bind(&username)
            .execute(&mut *tx)
            .await
            {
                tracing::error!("Failed to sweep stale publication rows during username drift fix: {}", e);
                let _ = tx.rollback().await;
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
            }
            if row.stored_username != username {
                tracing::info!(
                    "Re-publishing {}/{} with renamed username (was {})",
                    username, req.slug, row.stored_username,
                );
            }
            row.project_id
        }
        Some(_) => {
            // Different user already owns (username, slug). Composite
            // uniqueness means this is a genuine conflict.
            let _ = tx.rollback().await;
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "Slug already taken" })),
            )
                .into_response();
        }
        None => None,
    };

    // Resolve or create the deployment project row.
    //
    // Visitor allowlist persistence: whatever allowlist the dashboard
    // computed from the loom gets stored as a JSONB blob here. The
    // visitor run / trigger broadcast paths read this column and never
    // parse the loom again. The dashboard is the only place that
    // understands the loom syntax.
    let visitor_access_json = req.visitor_access.clone();
    let deployment_project_id: String = match &existing_deployment_project_id {
        Some(existing_id) => {
            // Re-publish: overwrite the existing deployment's weft/loom/layout.
            // Keep the same UUID so trigger IDs, execution history, infra
            // records all stay linked.
            let updated: Result<(String,), sqlx::Error> = sqlx::query_as(
                r#"
                UPDATE projects
                SET name = $1,
                    description = $2,
                    weft_code = $3,
                    loom_code = $4,
                    layout_code = $5,
                    visitor_access = $6,
                    updated_at = NOW()
                WHERE id = $7::uuid AND user_id = $8 AND is_deployment = true
                RETURNING id::text
                "#,
            )
            .bind(&project_name)
            .bind(&description)
            .bind(&weft_code)
            .bind(&loom_code)
            .bind(&layout_code)
            .bind(&visitor_access_json)
            .bind(existing_id)
            .bind(&user_id)
            .fetch_one(&mut *tx)
            .await;
            match updated {
                Ok((id,)) => id,
                Err(e) => {
                    tracing::error!("Failed to overwrite deployment project {}: {}", existing_id, e);
                    let _ = tx.rollback().await;
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "Failed to overwrite deployment" })),
                    )
                        .into_response();
                }
            }
        }
        None => {
            // First publish: create a fresh deployment project row.
            let created: Result<(String,), sqlx::Error> = sqlx::query_as(
                r#"
                INSERT INTO projects (user_id, name, description, weft_code, loom_code, layout_code,
                                      is_deployment, origin_project_id, visitor_access)
                VALUES ($1, $2, $3, $4, $5, $6, true, $7::uuid, $8)
                RETURNING id::text
                "#,
            )
            .bind(&user_id)
            .bind(&project_name)
            .bind(&description)
            .bind(&weft_code)
            .bind(&loom_code)
            .bind(&layout_code)
            .bind(&req.project_id)
            .bind(&visitor_access_json)
            .fetch_one(&mut *tx)
            .await;
            match created {
                Ok((id,)) => id,
                Err(e) => {
                    tracing::error!("Failed to create deployment project: {}", e);
                    let _ = tx.rollback().await;
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "Failed to create deployment" })),
                    )
                        .into_response();
                }
            }
        }
    };

    // Replace triggers on the deployment with fresh copies from the
    // builder. On re-publish this destroys any admin-only tweaks the
    // owner made to the deployment's triggers; that's the "replace on
    // re-publish" contract. The helper returns a list of trigger IDs
    // whose live TriggerService instances must be torn down AFTER the
    // transaction commits (see H3).
    let triggers_to_unregister = match clone_triggers_into_deployment(
        &mut tx,
        &state,
        &req.project_id,
        &deployment_project_id,
        &user_id,
    )
    .await
    {
        Ok(list) => list,
        Err(e) => {
            tracing::error!("Failed to clone triggers into deployment: {}", e);
            let _ = tx.rollback().await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to copy triggers" })),
            )
                .into_response();
        }
    };

    // Upsert the (username, slug) → deployment_project_id mapping.
    // The ON CONFLICT clause uses the composite unique key so re-publishes
    // update the existing row in place. Legacy columns (weft_code / loom_code
    // / layout_code) are written alongside the mapping so read paths that
    // haven't migrated yet still work, but the target `projects` row is the
    // source of truth going forward.
    // Upsert the mapping row. Ghost columns weft_code/loom_code/
    // layout_code are left null. The live code lives on the
    // deployment project row via project_id. See H6. The rate limit
    // was validated at the top of the handler (L-1), so no check here.
    let result = sqlx::query_as::<_, PublishedProject>(
        "INSERT INTO published_projects
            (slug, username, user_id, project_id, project_name, description,
             is_live, rate_limit_per_minute)
         VALUES ($1, $2, $3, $4::uuid, $5, $6, true, $7)
         ON CONFLICT (username, slug) DO UPDATE SET
            project_id = EXCLUDED.project_id,
            project_name = EXCLUDED.project_name,
            description = EXCLUDED.description,
            is_live = true,
            rate_limit_per_minute = EXCLUDED.rate_limit_per_minute,
            updated_at = NOW()
         WHERE published_projects.user_id = $3
         RETURNING id::text, slug, username, user_id, project_id::text,
                   project_name, description, weft_code, loom_code, layout_code,
                   is_live, view_count, run_count, published_at, updated_at,
                   rate_limit_per_minute,
                   (SELECT origin_project_id::text FROM projects
                    WHERE id = published_projects.project_id) AS origin_project_id",
    )
    .bind(&req.slug)
    .bind(&username)
    .bind(&user_id)
    .bind(&deployment_project_id)
    .bind(&project_name)
    .bind(&description)
    .bind(&req.rate_limit_per_minute)
    .fetch_optional(&mut *tx)
    .await;

    let published = match result {
        Ok(Some(p)) => p,
        Ok(None) => {
            // Reachable when a conflicting (username, slug) row exists
            // AND its user_id != current caller. The INSERT hits the
            // ON CONFLICT path, but the `WHERE published_projects.user_id = $3`
            // filter on DO UPDATE blocks the update, so nothing is
            // returned. The upstream existing-mapping check catches
            // most of these with a clean 409 before we reach the
            // transaction, but this guard stays as defense-in-depth
            // for races where another user publishes the slug between
            // our read and our write.
            let _ = tx.rollback().await;
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "Slug already taken" })),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!("Failed to upsert published_projects: {}", e);
            let _ = tx.rollback().await;
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    if let Err(e) = tx.commit().await {
        tracing::error!("Failed to commit publish transaction: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    // H3: teardown of the old deployment's live TriggerService instances
    // happens ONLY after the commit. If any individual unregister fails
    // the next dispatcher restart sweep will reconcile, but the DB
    // rows are gone so it can't accidentally re-register the old ids.
    if !triggers_to_unregister.is_empty() {
        let service = state.trigger_service.lock().await;
        for trigger_id in triggers_to_unregister.iter() {
            if let Err(e) = service.unregister_trigger(trigger_id).await {
                tracing::warn!("Failed to unregister old trigger {} after re-publish: {}", trigger_id, e);
            }
        }
    }

    (StatusCode::CREATED, Json(serde_json::to_value(published).unwrap())).into_response()
}

/// Copy triggers from the builder project into the deployment project.
///
/// Called from within the publish transaction. Writes to `triggers` in
/// the same transaction as the rest of the publish so a partial failure
/// rolls back atomically. Returns the list of previously-running
/// deployment trigger ids that the CALLER must unregister from the live
/// TriggerService AFTER `tx.commit()` succeeds (see H3 in the review).
/// Unregistering before the commit means a rollback leaves the live
/// dispatcher and the DB out of sync.
///
/// Re-publish semantics: existing deployment triggers are marked for
/// teardown (returned to the caller), their DB rows are deleted, and
/// fresh rows are inserted with new UUIDs. The owner manually activates
/// the new triggers from the dashboard (same "new triggers start in
/// pending state" invariant everywhere else).
async fn clone_triggers_into_deployment(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &AppState,
    builder_project_id: &str,
    deployment_project_id: &str,
    user_id: &str,
) -> Result<Vec<String>, sqlx::Error> {
    // Find existing deployment triggers (from a previous publish of the
    // same slug) whose runtime instances still need to be torn down.
    // Only `running` / `setup_pending` have live state in TriggerService;
    // `pending` and `failed` never registered, so there's nothing to
    // unregister for those.
    #[derive(sqlx::FromRow)]
    struct ExistingRow { id: String, status: String }
    let existing: Vec<ExistingRow> = sqlx::query_as(
        "SELECT id, status FROM triggers WHERE project_id = $1",
    )
    .bind(deployment_project_id)
    .fetch_all(&mut **tx)
    .await?;
    let to_unregister: Vec<String> = existing
        .into_iter()
        .filter(|r| r.status == "running" || r.status == "setup_pending")
        .map(|r| r.id)
        .collect();

    // Drop them all from the table. The TriggerService dispatcher state
    // stays as-is until the caller tears it down post-commit.
    sqlx::query("DELETE FROM triggers WHERE project_id = $1")
        .bind(deployment_project_id)
        .execute(&mut **tx)
        .await?;

    // Pull every trigger the builder project currently has. `list_triggers_by_project`
    // already filters to rows with `user_id = builder_project.user_id`
    // since it's called from the authenticated path, so we don't need a
    // second ownership guard inside the loop.
    let builder_triggers = crate::trigger_store::list_triggers_by_project(&state.db_pool, builder_project_id)
        .await?;

    for t in builder_triggers.iter() {
        let new_id = uuid::Uuid::new_v4().to_string();
        // Credentials get re-encrypted fresh so the new row carries its
        // own sealed blob. The old trigger's crypto key / IV never
        // leaks to the clone even if both rows share the same
        // plaintext value.
        let encrypted_credentials: Option<serde_json::Value> = match t.credentials.as_ref() {
            Some(cred) => {
                let encrypted = crate::crypto::encrypt_credentials(cred)
                    .map_err(|e| {
                        tracing::error!("Failed to encrypt credentials for cloned trigger {}: {}", new_id, e);
                        sqlx::Error::Protocol(format!("Failed to encrypt credentials: {}", e))
                    })?;
                Some(serde_json::Value::String(encrypted))
            }
            None => None,
        };
        sqlx::query(
            r#"
            INSERT INTO triggers (id, project_id, trigger_node_id, trigger_category,
                                  node_type, user_id, config, credentials,
                                  project_definition, project_hash, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
            "#,
        )
        .bind(&new_id)
        .bind(deployment_project_id)
        .bind(&t.triggerNodeId)
        .bind(&t.triggerCategory)
        .bind(&t.nodeType)
        .bind(Some(user_id))
        .bind(&t.config)
        .bind(&encrypted_credentials)
        .bind(t.projectDefinition.as_ref())
        .bind(t.projectHash.as_deref())
        .execute(&mut **tx)
        .await?;
    }

    Ok(to_unregister)
}

/// GET /api/v1/publish
///
/// List all publications owned by the caller.
pub async fn list_publications(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(user_id) = user_id_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" }))).into_response();
    };

    // LEFT JOIN projects so the dashboard can roll up per-builder
    // runtime status: `origin_project_id` maps each deployment row
    // back to the builder it was cloned from, which lets the project
    // list show a lit thunder icon on the builder when ANY of its
    // deployment descendants has an active trigger. Orphan mappings
    // whose project row has been deleted return null here.
    let result = sqlx::query_as::<_, PublishedProject>(
        "SELECT pp.id::text, pp.slug, pp.username, pp.user_id, pp.project_id::text, pp.project_name, pp.description,
                pp.weft_code, pp.loom_code, pp.layout_code, pp.is_live, pp.view_count, pp.run_count,
                pp.published_at, pp.updated_at, pp.rate_limit_per_minute,
                pr.origin_project_id::text AS origin_project_id
         FROM published_projects pp
         LEFT JOIN projects pr ON pr.id = pp.project_id
         WHERE pp.user_id = $1
         ORDER BY pp.updated_at DESC",
    )
    .bind(&user_id)
    .fetch_all(&state.db_pool)
    .await;

    match result {
        Ok(list) => Json(list).into_response(),
        Err(e) => {
            tracing::error!("Failed to list publications: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response()
        }
    }
}

/// GET /api/v1/publish/by-user/{username}/{slug}
///
/// Public endpoint: fetch a published snapshot by (username, slug). No
/// auth required. Returns the same public-facing shape as cloud-api's
/// equivalent handler so the dashboard's `/p/<username>/<slug>` route can
/// talk to either backend transparently.
pub async fn get_by_user_slug(
    State(state): State<Arc<AppState>>,
    Path((username, slug)): Path<(String, String)>,
) -> impl IntoResponse {
    // Join published_projects → projects so weft/loom/layout come from the
    // current target project row (admin field edits through the runner
    // path update it in place).
    #[derive(sqlx::FromRow)]
    struct Row {
        project_name: String,
        description: Option<String>,
        weft_code: Option<String>,
        loom_code: Option<String>,
        layout_code: Option<String>,
        is_live: bool,
    }
    let result: Option<Row> = match sqlx::query_as(
        r#"
        SELECT pp.project_name, pp.description, pr.weft_code, pr.loom_code, pr.layout_code, pp.is_live
        FROM published_projects pp
        LEFT JOIN projects pr ON pr.id = pp.project_id
        WHERE pp.username = $1 AND pp.slug = $2
        "#,
    )
    .bind(&username)
    .bind(&slug)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!("Failed to load publication ({}, {}): {}", username, slug, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    let Some(p) = result else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response();
    };

    // Best-effort view counter bump. Failure is non-fatal: a logged warning
    // lets us notice if the counter stops working, but the visitor still
    // gets the page.
    if let Err(e) = sqlx::query(
        "UPDATE published_projects SET view_count = view_count + 1 WHERE username = $1 AND slug = $2",
    )
    .bind(&username)
    .bind(&slug)
    .execute(&state.db_pool)
    .await
    {
        tracing::warn!("Failed to increment view_count for ({}, {}): {}", username, slug, e);
    }

    Json(serde_json::json!({
        "slug": slug,
        "username": username,
        "projectName": p.project_name,
        "description": p.description,
        "weftCode": p.weft_code,
        "loomCode": p.loom_code,
        "layoutCode": p.layout_code,
        "available": p.is_live,
        // OSS local mode has no subscription system, so the
        // "Built with WeaveMind" branding footer is never forced here.
        // Cloud-api enforces it based on the deployer's tier.
        "showBuiltWithFooter": false,
    }))
    .into_response()
}

/// PATCH /api/v1/publish/{slug}
///
/// Toggle the published page between live and paused, or update its
/// description. Owned by the caller only.
pub async fn update_publication(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
    headers: HeaderMap,
    Json(req): Json<UpdatePublishRequest>,
) -> impl IntoResponse {
    let Some(user_id) = user_id_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" }))).into_response();
    };

    // Validate the deployer's requested rate limit against the hard cap.
    // Same bounds as publish(): a 400 surfaces the cap so clients see
    // it instead of us silently clamping to a different value.
    const RATE_LIMIT_MAX: i32 = 500;
    if let Some(rl) = req.rate_limit_per_minute {
        if rl < 1 || rl > RATE_LIMIT_MAX {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!(
                        "rate_limit_per_minute must be between 1 and {}. To pause visitor runs, set is_live=false instead.",
                        RATE_LIMIT_MAX,
                    ),
                })),
            )
                .into_response();
        }
    }

    // Key on (user_id, slug) because (user_id, username) is 1:1 and the
    // pair is effectively unique within a user. We can't key on
    // (username, slug) directly here because the handler only gets the
    // slug from the URL. It would require looking up username first. That
    // extra query isn't worth it; `user_id = $4` provides the same
    // isolation.
    let result = sqlx::query(
        "UPDATE published_projects SET
            is_live = COALESCE($1, is_live),
            description = COALESCE($2, description),
            rate_limit_per_minute = COALESCE($3, rate_limit_per_minute),
            updated_at = NOW()
         WHERE slug = $4 AND user_id = $5",
    )
    .bind(req.is_live)
    .bind(req.description.as_deref())
    .bind(req.rate_limit_per_minute)
    .bind(&slug)
    .bind(&user_id)
    .execute(&state.db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => Json(serde_json::json!({ "success": true })).into_response(),
        Ok(_) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response(),
        Err(e) => {
            tracing::error!("Failed to update publication: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response()
        }
    }
}

// ── Publish execute (internal service-to-service) ───────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishExecuteRequest {
    /// Target deployment identified by its public URL segments. The
    /// backend resolves the deployment server-side and loads the live
    /// weft code from the projects table. Callers never submit code,
    /// so a compromised caller can't run arbitrary weft as any user.
    pub username: String,
    pub slug: String,
    /// Visitor-supplied field values, shape: `{ "<nodeId>": { "<key>": <value>, ... }, ... }`.
    /// The handler ONLY merges keys that the deployment's
    /// `visitor_access.inputs` allowlist names. Anything else is dropped.
    pub inputs: serde_json::Value,
}

/// POST /api/v1/publish/execute
///
/// Internal service route. Called by cloud-api's public `/p/<u>/<slug>/run`
/// after it rate-limited and credit-gated the request. Body is
/// `(username, slug, inputs)`; the backend loads the weft code and visitor
/// allowlist from the deployment project row. Callers cannot supply weft
/// directly. That is the trust boundary.
///
/// Two request headers must be set by the caller:
///   - `x-internal-api-key`: proves the caller is a trusted service.
///     Startup (see main.rs) fails-closed if unset in non-local mode.
///   - `x-weavemind-publish-owner` (legacy header): still tolerated for
///     back-compat but the authoritative owner is the `user_id` on the
///     `projects` row. The header is ignored if it disagrees.
pub async fn publish_execute(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<PublishExecuteRequest>,
) -> impl IntoResponse {
    // Trust boundary between cloud-api and weft-api for visitor runs.
    let configured = state.internal_api_key.as_bytes();
    let deployment_mode = std::env::var("DEPLOYMENT_MODE").unwrap_or_else(|_| "cloud".to_string());
    if configured.is_empty() {
        if deployment_mode != "local" {
            tracing::error!("publish_execute rejected: INTERNAL_API_KEY is empty in non-local mode");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Server misconfiguration" }))).into_response();
        }
    } else {
        use subtle::ConstantTimeEq;
        let provided = headers
            .get("x-internal-api-key")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        let eq: bool = provided.as_bytes().ct_eq(configured).into();
        if !eq {
            tracing::warn!("publish_execute: invalid x-internal-api-key");
            return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" }))).into_response();
        }
    }

    // Load the deployment's live data from the projects table via the
    // mapping row. We need the owning user_id (cost attribution), the
    // is_live flag, the weft code to execute, and the visitor_access
    // allowlist. Everything in one query so a misbehaving DB can't
    // observe a half-populated state.
    #[derive(sqlx::FromRow)]
    struct Row {
        user_id: String,
        // Real project_id for the ownership guard in record_execution.
        project_id: Option<sqlx::types::Uuid>,
        is_live: bool,
        weft_code: Option<String>,
        visitor_access: Option<serde_json::Value>,
    }
    let row: Option<Row> = match sqlx::query_as(
        r#"
        SELECT pp.user_id, pp.project_id, pp.is_live, pr.weft_code, pr.visitor_access
        FROM published_projects pp
        LEFT JOIN projects pr ON pr.id = pp.project_id
        WHERE pp.username = $1 AND pp.slug = $2
        "#,
    )
    .bind(&req.username)
    .bind(&req.slug)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("publish_execute: DB error loading deployment ({}, {}): {}", req.username, req.slug, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    let Some(row) = row else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response();
    };
    if !row.is_live {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response();
    }
    let Some(weft_code) = row.weft_code else {
        tracing::error!("publish_execute: deployment ({}, {}) has no weft_code", req.username, req.slug);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Deployment is missing code" }))).into_response();
    };
    let owner = row.user_id;
    let Some(deployment_project_id) = row.project_id else {
        tracing::error!(
            "publish_execute: deployment ({}, {}) has no project_id mapping",
            req.username, req.slug
        );
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Deployment is missing project mapping" }))).into_response();
    };

    // Compile the live weft code into a ProjectDefinition. Visitors then
    // splice allowlist-filtered field values into the parsed project's
    // node configs before we hand it to the executor.
    let mut project = match weft_core::weft_compiler::compile(&weft_code, deployment_project_id) {
        Ok(p) => p,
        Err(errs) => {
            tracing::error!("publish_execute: weft compile failed for ({}, {}): {:?}", req.username, req.slug, errs);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Deployment has invalid code" }))).into_response();
        }
    };

    // Build the per-node input allowlist from visitor_access.inputs. Any
    // field NOT listed here for a node is silently dropped from the
    // visitor submission. A malicious caller can't overwrite the
    // deployer's system prompts, API keys, or model IDs by sending extra
    // keys. Missing/null allowlist ⇒ nothing is writable ⇒ the visitor
    // sees the deployer's defaults on every run.
    use std::collections::HashMap;
    let input_allowlist: HashMap<String, std::collections::HashSet<String>> = row
        .visitor_access
        .as_ref()
        .and_then(|v| v.get("inputs"))
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(node_id, fields)| {
                    let field_set = fields
                        .as_array()?
                        .iter()
                        .filter_map(|f| f.as_str().map(|s| s.to_string()))
                        .collect::<std::collections::HashSet<String>>();
                    Some((node_id.clone(), field_set))
                })
                .collect()
        })
        .unwrap_or_default();

    // M-2: track mismatches so a misconfigured dashboard (visitor
    // submission uses nodeIds or field keys that don't match the
    // allowlist) surfaces during QA instead of silently no-oping.
    // Logged at debug level to avoid spamming on every normal run.
    let mut unknown_node_ids: Vec<&String> = Vec::new();
    let mut dropped_fields: Vec<(String, String)> = Vec::new();

    if let Some(obj) = req.inputs.as_object() {
        for (node_id, fields) in obj {
            let Some(allowed) = input_allowlist.get(node_id) else {
                unknown_node_ids.push(node_id);
                continue;
            };
            if let Some(node) = project.nodes.iter_mut().find(|n| n.id == *node_id) {
                if let Some(field_map) = fields.as_object() {
                    if let Some(cfg_obj) = node.config.as_object_mut() {
                        for (k, v) in field_map {
                            if allowed.contains(k) {
                                cfg_obj.insert(k.clone(), v.clone());
                            } else {
                                dropped_fields.push((node_id.clone(), k.clone()));
                            }
                        }
                    }
                }
            } else {
                unknown_node_ids.push(node_id);
            }
        }
    }
    if !unknown_node_ids.is_empty() || !dropped_fields.is_empty() {
        tracing::debug!(
            "publish_execute allowlist mismatch for ({}, {}): unknown_nodes={:?} dropped_fields={:?}",
            req.username, req.slug, unknown_node_ids, dropped_fields,
        );
    }

    let execution_id = format!("publish-{}-{}", owner, uuid::Uuid::new_v4());

    let start_req = ProjectExecutionRequest {
        project,
        input: serde_json::json!({}),
        userId: Some(owner.clone()),
        statusCallbackUrl: None,
        isInfraSetup: false,
        isTriggerSetup: false,
        weftCode: Some(weft_code.clone()),
        testMode: false,
        triggerId: None,
        nodeType: None,
        mocks: None,
    };

    // Kick off the run. The executor's `/start` is NOT synchronous in
    // the OSS axum orchestrator: it returns immediately with
    // `{status: "running"}` after dispatching the first pulse and lets
    // the execution continue asynchronously (matching Restate's
    // `/send` semantics). Real Restate ingress at `/start` is
    // synchronous and would return outputs directly, but we can't
    // rely on that here because OSS users run the axum stub.
    //
    // So after dispatching, we poll `/get_status` until the run
    // finishes, then fetch `/get_all_outputs`. Bounded by a hard
    // deadline so a stuck node can't hang the visitor request
    // indefinitely.
    let start_url = format!("{}/ProjectExecutor/{}/start", state.executor_url, execution_id);
    let start_resp = state
        .http_client
        .post(&start_url)
        .json(&start_req)
        .timeout(std::time::Duration::from_secs(60))
        .send()
        .await;

    match start_resp {
        Ok(res) if res.status().is_success() => {}
        Ok(res) => {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            tracing::warn!("publish_execute forward failed: {} {}", status, text);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Executor rejected the run" }))).into_response();
        }
        Err(e) => {
            tracing::error!("publish_execute forward error: {}", e);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Executor unreachable" }))).into_response();
        }
    }

    // Poll get_status until the execution reaches a terminal state
    // (`completed`, `failed`, or `cancelled`). 60s deadline matches
    // the dispatch timeout above.
    let status_url = format!("{}/ProjectExecutor/{}/get_status", state.executor_url, execution_id);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let final_status: String = loop {
        if std::time::Instant::now() >= deadline {
            tracing::warn!("publish_execute: execution {} did not finish within deadline", execution_id);
            return (StatusCode::GATEWAY_TIMEOUT, Json(serde_json::json!({ "error": "Execution timed out" }))).into_response();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let status_resp = state
            .http_client
            .get(&status_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await;
        let s = match status_resp {
            Ok(res) if res.status().is_success() => res.json::<String>().await.unwrap_or_else(|_| "running".to_string()),
            _ => continue,
        };
        if s == "completed" || s == "failed" || s == "cancelled" {
            break s;
        }
    };

    if final_status != "completed" {
        tracing::warn!("publish_execute: execution {} ended with status {}", execution_id, final_status);
        return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": format!("Execution {}", final_status) }))).into_response();
    }

    // Fetch the outputs once the run is complete.
    let outputs_url = format!("{}/ProjectExecutor/{}/get_all_outputs", state.executor_url, execution_id);
    let outputs = match state
        .http_client
        .get(&outputs_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(res) if res.status().is_success() => {
            let body = res.json::<serde_json::Value>().await.unwrap_or_else(|_| serde_json::json!({}));
            // `get_all_outputs` returns `{ outputs: { <nodeId>: { <port>: value, ... }, ... } }`.
            // Cloud-api further restricts outputs to the loom's output
            // allowlist before returning to the visitor; we forward
            // the full map here and let the caller filter.
            body.get("outputs").cloned().unwrap_or(serde_json::Value::Null)
        }
        _ => {
            tracing::warn!("publish_execute: failed to fetch outputs for {}", execution_id);
            serde_json::Value::Null
        }
    };

    // H8: bump run_count on the mapping row. This is the single
    // bump site for both OSS-local and cloud-forwarded runs.
    // Cloud-api no longer increments after forwarding so the
    // counter stays accurate without double-counting.
    if let Err(e) = sqlx::query(
        "UPDATE published_projects SET run_count = run_count + 1 WHERE username = $1 AND slug = $2",
    )
    .bind(&req.username)
    .bind(&req.slug)
    .execute(&state.db_pool)
    .await
    {
        tracing::warn!("Failed to increment run_count for ({}, {}): {}", req.username, req.slug, e);
    }

    Json(serde_json::json!({ "runId": execution_id, "outputs": outputs })).into_response()
}

// ── Public run (OSS-local only) ─────────────────────────────────────────────

/// Body for `POST /api/v1/publish/by-user/{username}/{slug}/run`.
/// Mirrors cloud-api's `RunRequest` in its public-run handler.
#[derive(Debug, Deserialize)]
pub struct PublicRunRequest {
    #[serde(default)]
    pub inputs: serde_json::Value,
}

/// POST /api/v1/publish/by-user/{username}/{slug}/run
///
/// OSS-local visitor run endpoint. In cloud mode this same URL lives
/// on cloud-api (see `weavemind/cloud-api/src/publish.rs::public_run_handler`)
/// and does rate-limiting, credit-gating, and cookie-based visitor
/// session tracking before forwarding to weft-api's
/// `/api/v1/publish/execute`. None of that applies in OSS standalone:
/// there's no billing, no multi-tenancy, and no need to gate runs.
///
/// This handler is a thin wrapper that synthesizes a
/// `PublishExecuteRequest`, delegates straight to `publish_execute`,
/// and re-wraps the response in cloud-api's `{ok, result}` envelope
/// so the dashboard's visitor page reads outputs the same way in
/// both modes.
pub async fn public_run_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((username, slug)): Path<(String, String)>,
    Json(body): Json<PublicRunRequest>,
) -> impl IntoResponse {
    let exec_req = PublishExecuteRequest {
        username,
        slug,
        inputs: body.inputs,
    };
    // Call the inner handler and intercept its response body. The
    // dashboard's visitor page `handleRun` expects the cloud-api
    // envelope shape (`{ok: true, result: {runId, outputs}}`) and
    // bails silently if it reads the raw `{runId, outputs}` shape
    // that `publish_execute` emits. Re-wrap here so OSS and cloud
    // responses are identical on the wire.
    let inner = publish_execute(State(state), headers, Json(exec_req)).await.into_response();
    let (parts, body_bytes) = inner.into_parts();
    let bytes = match axum::body::to_bytes(body_bytes, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("public_run_handler: failed to buffer inner body: {}", e);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Run failed" }))).into_response();
        }
    };
    if !parts.status.is_success() {
        // Forward error responses untouched so the visitor page's
        // error handling (status code + error body) still works.
        return (parts.status, bytes).into_response();
    }
    let inner_json: serde_json::Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::json!({}));
    Json(serde_json::json!({ "ok": true, "result": inner_json })).into_response()
}

/// DELETE /api/v1/publish/{slug}
///
/// Unpublish. The row is deleted; any associated visitor sessions (cloud-only)
/// are orphaned and cleaned up by a maintenance job.
/// DELETE /api/v1/publish/{slug}
///
/// Unpublish a deployment. Steps:
///   1. Look up the deployment's target project_id via published_projects.
///   2. Unregister any running triggers on that project via TriggerService
///      (best-effort; the dispatcher will cleanup drift on restart if this
///      fails).
///   3. Delete the published_projects mapping row.
///   4. Delete the deployment project row. The cascade drops its triggers,
///      executions, infra references, and everything else scoped to it.
///
/// Step 4 is safe because deployment projects are only referenced by
/// the tables that also key on project_id with ON DELETE CASCADE.
pub async fn delete_publication(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(user_id) = user_id_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" }))).into_response();
    };

    // Find the deployment project for this slug so we can clean up triggers
    // before destroying anything.
    let mapping: Option<(String,)> = match sqlx::query_as(
        "SELECT project_id::text FROM published_projects WHERE slug = $1 AND user_id = $2",
    )
    .bind(&slug)
    .bind(&user_id)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!("Failed to look up publication for unpublish ({}): {}", slug, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    let Some((deployment_project_id,)) = mapping else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response();
    };

    // Drop the mapping row and the deployment project together. Use a
    // transaction so a partial failure is recoverable.
    let mut tx = match state.db_pool.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to start unpublish transaction: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    // M-1: enumerate the deployment's triggers INSIDE the tx using
    // FOR UPDATE so the rows are locked for the duration. Without
    // locking, the dispatcher's maintenance task could flip a pending
    // trigger to running between enumeration and the DELETE below.
    // The DELETE would remove the row without us seeing it in the
    // enumeration, the post-commit unregister loop would miss it, and
    // the dispatcher would hold a live instance pointing at a deleted
    // project until the next heartbeat sweep.
    #[derive(sqlx::FromRow)]
    struct Row { id: String, status: String }
    let running: Vec<Row> = match sqlx::query_as(
        "SELECT id, status FROM triggers WHERE project_id = $1 FOR UPDATE",
    )
    .bind(&deployment_project_id)
    .fetch_all(&mut *tx)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("Failed to enumerate triggers during unpublish ({}): {}", slug, e);
            let _ = tx.rollback().await;
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    if let Err(e) = sqlx::query("DELETE FROM published_projects WHERE slug = $1 AND user_id = $2")
        .bind(&slug)
        .bind(&user_id)
        .execute(&mut *tx)
        .await
    {
        tracing::error!("Failed to delete published_projects row: {}", e);
        let _ = tx.rollback().await;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    // M9: `triggers.project_id` is TEXT with no FK to `projects(id)`,
    // so deleting the projects row does NOT cascade-delete the trigger
    // rows. We delete them explicitly inside the same tx (rows were
    // locked above via FOR UPDATE, see M-1) so there's no race
    // between enumeration and deletion. Executions and
    // project_versions DO have ON DELETE CASCADE, so the projects row
    // DELETE below takes care of those.
    if let Err(e) = sqlx::query("DELETE FROM triggers WHERE project_id = $1")
        .bind(&deployment_project_id)
        .execute(&mut *tx)
        .await
    {
        tracing::error!("Failed to delete deployment triggers: {}", e);
        let _ = tx.rollback().await;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    if let Err(e) = sqlx::query(
        "DELETE FROM projects WHERE id = $1::uuid AND user_id = $2 AND is_deployment = true",
    )
    .bind(&deployment_project_id)
    .bind(&user_id)
    .execute(&mut *tx)
    .await
    {
        tracing::error!("Failed to delete deployment project row: {}", e);
        let _ = tx.rollback().await;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    if let Err(e) = tx.commit().await {
        tracing::error!("Failed to commit unpublish transaction: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    // H3: now that the DB rows are gone and the commit is durable, tear
    // down the live TriggerService instances. Best-effort on each one.
    // A failure leaves an orphaned dispatcher entry that the next
    // restart sweep will reconcile, which is safe because the DB rows
    // it would look for no longer exist.
    {
        let service = state.trigger_service.lock().await;
        for row in running.iter() {
            if row.status == "running" || row.status == "setup_pending" {
                if let Err(e) = service.unregister_trigger(&row.id).await {
                    tracing::warn!("Failed to unregister trigger {} post-unpublish: {}", row.id, e);
                }
            }
        }
    }

    Json(serde_json::json!({ "success": true })).into_response()
}

/// GET /api/v1/publish/by-user/{username}/{slug}/latest-trigger-run
///
/// Public endpoint (OSS local mode path): returns the most recent
/// completed trigger-fired execution for this deployment. Outputs are
/// filtered through the deployment's `visitor_access.outputs` allowlist
/// so visitors only see port values the deployer marked visible in the
/// loom. Raw `node_outputs` never leaves the server.
pub async fn latest_trigger_run(
    State(state): State<Arc<AppState>>,
    Path((username, slug)): Path<(String, String)>,
) -> impl IntoResponse {
    #[derive(sqlx::FromRow)]
    struct DeploymentRow {
        project_id: String,
        is_live: bool,
        visitor_access: Option<serde_json::Value>,
    }
    let row: Option<DeploymentRow> = match sqlx::query_as(
        r#"
        SELECT pp.project_id::text AS project_id, pp.is_live, pr.visitor_access
        FROM published_projects pp
        LEFT JOIN projects pr ON pr.id = pp.project_id
        WHERE pp.username = $1 AND pp.slug = $2
        "#,
    )
    .bind(&username)
    .bind(&slug)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!("Failed to look up publication for latest_trigger_run ({}, {}): {}", username, slug, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    let Some(row) = row else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response();
    };
    if !row.is_live {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Not found" }))).into_response();
    }
    let project_id = row.project_id;
    let visitor_access = row.visitor_access;

    let exec: Option<(String, serde_json::Value, chrono::DateTime<chrono::Utc>)> = match sqlx::query_as(
        r#"
        SELECT id, node_outputs, started_at
        FROM executions
        WHERE project_id = $1::uuid
          AND trigger_id IS NOT NULL
          AND status = 'completed'
        ORDER BY started_at DESC
        LIMIT 1
        "#,
    )
    .bind(&project_id)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!("Failed to load latest trigger execution for project {}: {}", project_id, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
        }
    };

    match exec {
        Some((execution_id, node_outputs, started_at)) => {
            let filtered = filter_outputs_by_allowlist(node_outputs, visitor_access.as_ref());
            Json(serde_json::json!({
                "executionId": execution_id,
                "startedAt": started_at,
                "outputs": filtered,
            }))
            .into_response()
        }
        None => Json(serde_json::Value::Null).into_response(),
    }
}

/// Filter a `{ nodeId: { port: value } }` outputs blob through a
/// deployment's `visitor_access.outputs` allowlist. Mirrors the function
/// of the same name in cloud-api. Kept in a separate definition so the
/// OSS crate compiles without depending on cloud-api sources.
fn filter_outputs_by_allowlist(
    outputs: serde_json::Value,
    visitor_access: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(outputs_obj) = outputs.as_object() else {
        return serde_json::Value::Object(Default::default());
    };
    let Some(allowlist) = visitor_access
        .and_then(|v| v.get("outputs"))
        .and_then(|v| v.as_object())
    else {
        return serde_json::Value::Object(Default::default());
    };
    let mut filtered = serde_json::Map::new();
    for (node_id, node_outputs) in outputs_obj {
        let Some(allowed_ports) = allowlist.get(node_id).and_then(|v| v.as_array()) else {
            continue;
        };
        let allowed_names: std::collections::HashSet<&str> = allowed_ports
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        if allowed_names.is_empty() {
            continue;
        }
        let Some(node_obj) = node_outputs.as_object() else {
            continue;
        };
        let mut per_node = serde_json::Map::new();
        for (port, value) in node_obj {
            if allowed_names.contains(port.as_str()) {
                per_node.insert(port.clone(), value.clone());
            }
        }
        if !per_node.is_empty() {
            filtered.insert(node_id.clone(), serde_json::Value::Object(per_node));
        }
    }
    serde_json::Value::Object(filtered)
}

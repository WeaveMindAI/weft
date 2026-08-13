//! Control-plane access admin: the connect/lookup verbs whose work
//! includes an OUTBOUND HTTP call to a URL the tenant influences (a
//! spec's test call, an OAuth token exchange, a `remote_select`
//! lookup, an app mint). They run on the broker, whose network egress
//! is locked to "public internet only", so a crafted URL pointing
//! inside the cluster dies at the network layer. The dispatcher stays
//! the editor's authenticated front door and forwards here with its SA
//! token, exactly like the runtime-file admin verbs.
//!
//! Every connect entry point (direct, oauth begin, oauth complete)
//! runs here so app resolution has ONE home, keyed by the DOOR: the
//! shared door always resolves a registered app from the trusted file (a
//! client-sent app is never honored there; the client is not the
//! security boundary), and the own door uses what the user pasted (or
//! the project's public app). Pure-DB access verbs (grant listing,
//! status polls) stay on the dispatcher: they make no outbound call
//! and need no app.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft_core::access::spec::{lookup_path, Door};
use weft_access_store::{DoorsAnswer, GrantedQuery, MintAppRequest, MintAppResponse, SharedAppChoice, SharedDoorPick};
use weft_core::storage::Tenanted;
use weft_core::AccessSpec;

use crate::app_provider::resolve_shared_app;
use crate::auth::control_plane;
use crate::state::BrokerState;

type ApiError = (StatusCode, String);

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new()
        .route("/v1/access/admin/connect/direct", post(connect_direct))
        .route("/v1/access/admin/oauth/begin", post(oauth_begin))
        .route("/v1/access/admin/oauth/complete", post(oauth_complete))
        .route("/v1/access/admin/doors", post(doors))
        .route("/v1/access/admin/mint-app", post(mint_app))
        .route("/v1/access/admin/lookup", post(lookup))
        .route("/v1/access/admin/granted", post(granted))
        .route("/v1/access/admin/picker-token", post(picker_token))
}

/// Map a store error: client-fixable classes keep their status and
/// message (the dispatcher relays them verbatim to the editor), the
/// rest logs here and answers an opaque 500.
fn access_err(e: anyhow::Error) -> ApiError {
    match weft_access_store::client_status(&e) {
        Some((status, msg)) => {
            (StatusCode::from_u16(status).expect("store status codes are valid"), msg)
        }
        None => {
            tracing::error!(target: "weft_broker::access_admin", "access store error: {e:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "access store error".into())
        }
    }
}

/// A shared-door failure is caller-fixable configuration (the fix is
/// the apps file or picking the other door): keep the message.
fn shared_app_err(e: anyhow::Error) -> ApiError {
    (StatusCode::PRECONDITION_FAILED, format!("{e:#}"))
}

/// The SHARED-door app grant for one oauth connect: resolve the
/// registered app the user picked by label, and answer the app itself
/// (client id + secret for the exchange) plus its fixed `covers` as
/// the connect's permissions. Nothing the client sent survives on
/// either: the client names a choice, never an app or a permission
/// set.
async fn shared_app_grant(
    state: &BrokerState,
    spec: &AccessSpec,
    shared_app: Option<&str>,
) -> Result<(weft_core::AppRegistration, Vec<String>), ApiError> {
    let label = shared_app.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("a shared-door '{}' connect names which registered app to use", spec.service),
        )
    })?;
    let picked = resolve_shared_app(&state.app_provider, spec, label)
        .await
        .map_err(shared_app_err)?;
    Ok((picked.app, picked.covers))
}

/// The spec a connect's DOOR is decided on: the paste variant when
/// the request pastes (the store connects on that variant, so the
/// broker must judge the same spec, or a consent service's paste
/// would be door-gated as oauth while storing a static credential).
fn door_decision_spec(spec: &AccessSpec, paste: bool) -> Result<AccessSpec, String> {
    if !paste {
        return Ok(spec.clone());
    }
    spec.validate()?;
    spec.paste_variant()
        .ok_or_else(|| format!("the '{}' service declares no paste section", spec.service))
}

/// The shared-door gate of a NON-oauth (key) service, pure: such a
/// connect stores a runtime-owned row that resolves through the
/// runtime's credential source, so a connect while no credential is
/// configured would mint a connection that can never authenticate;
/// refuse it now, naming the fix.
fn shared_key_gate(service: &str, credential_available: bool) -> Result<(), String> {
    if credential_available {
        return Ok(());
    }
    Err(format!(
        "the shared door of '{service}' runs on the runtime's own credential, and none is \
         configured; add an api_key entry for '{service}' to the shared-credentials file \
         (the json file named by {}), or connect your own",
        crate::app_provider::APPS_FILE_ENV
    ))
}

/// POST /v1/access/admin/connect/direct: a paste / mint /
/// server-to-server / shared-key connect (test call + token exchange
/// run here). The door decides the app: shared resolves a registered
/// app from the trusted file; own keeps what the request carried. A shared-key
/// connect is additionally gated on the runtime credential existing.
async fn connect_direct(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<SharedDoorPick<weft_access_store::ConnectDirect>>>,
) -> Result<Json<weft_access_store::CompletedConnect>, ApiError> {
    control_plane(&state, &headers).await?;
    let SharedDoorPick { shared_app, mut inner } = req.inner;
    if inner.door == Door::Shared {
        // Decide on the SAME spec the store connects on: the paste
        // variant when pasting.
        let decided = door_decision_spec(&inner.spec, inner.paste)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        if decided.is_oauth() {
            let (app, covers) = shared_app_grant(&state, &decided, shared_app.as_deref()).await?;
            inner.registration = Some(app);
            inner.permissions = covers;
        } else {
            let available = state.credentials.available(&decided.service).await;
            shared_key_gate(&decided.service, available)
                .map_err(|e| (StatusCode::PRECONDITION_FAILED, e))?;
        }
    }
    weft_access_store::connect_direct(&state.pool, &req.tenant, inner)
        .await
        .map(Json)
        .map_err(access_err)
}

/// POST /v1/access/admin/oauth/begin: park a pending browser consent
/// and answer the consent URL. Runs here (not on the dispatcher) so
/// app resolution shares one home with the other connects.
async fn oauth_begin(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<SharedDoorPick<weft_access_store::BeginOAuth>>>,
) -> Result<Json<weft_access_store::StartedOAuth>, ApiError> {
    control_plane(&state, &headers).await?;
    let SharedDoorPick { shared_app, mut inner } = req.inner;
    if inner.door == Door::Shared {
        let (app, covers) = shared_app_grant(&state, &inner.spec, shared_app.as_deref()).await?;
        inner.registration = Some(app);
        inner.permissions = covers;
    }
    weft_access_store::begin_oauth(&state.pool, &req.tenant, inner)
        .await
        .map(Json)
        .map_err(access_err)
}

/// The door probe's request: the service's spec (for the catalogue the
/// registered apps' `covers` are validated against).
#[derive(Deserialize)]
struct DoorsQuery {
    spec: AccessSpec,
}


/// POST /v1/access/admin/doors: which shared-door options exist. Also
/// where a typo'd `covers` entry surfaces loudly (the broker never
/// holds the catalogue at file-load time, so the probe and the
/// connects are where the check can run).
async fn doors(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(q): Json<DoorsQuery>,
) -> Result<Json<DoorsAnswer>, ApiError> {
    control_plane(&state, &headers).await?;
    // Validate before recording anything, like every sibling verb: a
    // malformed spec must not land its events block in the recipe
    // table.
    q.spec.validate().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    // The door probe runs whenever an access node renders, which
    // makes it the earliest moment the broker sees the service's
    // recipe: record its event topics so the public receiver can
    // verify pushes (and answer a provider's address handshake)
    // before any connection exists.
    weft_access_store::record_events_recipes(&state.pool, &q.spec)
        .await
        .map_err(access_err)?;
    if !q.spec.doors.contains(&Door::Shared) {
        return Ok(Json(DoorsAnswer { shared_apps: vec![], shared_credential: false }));
    }
    if q.spec.is_oauth() {
        let apps = state
            .app_provider
            .apps(&q.spec.service)
            .await
            .map_err(shared_app_err)?;
        crate::app_provider::check_covers(&q.spec, &apps).map_err(shared_app_err)?;
        crate::app_provider::check_events(&q.spec, &apps).map_err(shared_app_err)?;
        Ok(Json(DoorsAnswer {
            shared_apps: apps
                .into_iter()
                .map(|a| SharedAppChoice { label: a.app.label, covers: a.covers })
                .collect(),
            shared_credential: false,
        }))
    } else {
        Ok(Json(DoorsAnswer {
            shared_apps: vec![],
            shared_credential: state.credentials.available(&q.spec.service).await,
        }))
    }
}

/// POST /v1/access/admin/mint-app: run the service's declared
/// app-manifest recipe ("Create it for me"): POST the payload (ticked
/// permissions substituted) and capture the fresh app's credentials.
async fn mint_app(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<MintAppRequest>,
) -> Result<Json<MintAppResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    req.spec.validate().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let Some(mint) = req.spec.own_page.as_ref().and_then(|p| p.mint.as_ref()) else {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("the '{}' service declares no app mint", req.spec.service),
        ));
    };
    let payload = substitute_permissions(&mint.payload, &req.permissions);
    let resp = weft_core::access::client::base_client()
        .post(&mint.url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("the app mint call failed: {e}")))?;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("the provider refused the app mint ({status}): {body}"),
        ));
    }
    let mut values = std::collections::BTreeMap::new();
    for capture in &mint.captures {
        match lookup_path(&body, &capture.path) {
            Some(Value::String(s)) => {
                values.insert(capture.name.clone(), s.clone());
            }
            Some(other) => {
                values.insert(capture.name.clone(), other.to_string());
            }
            None if capture.optional => {}
            None => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "the mint response carries nothing at '{}' (capture '{}')",
                        capture.path, capture.name
                    ),
                ))
            }
        }
    }
    Ok(Json(MintAppResponse { values }))
}

/// Replace every string value equal to `"{permissions}"` in the mint
/// payload with the ticked permission ids as a JSON array.
fn substitute_permissions(payload: &Value, permissions: &[String]) -> Value {
    match payload {
        Value::String(s) if s == "{permissions}" => {
            Value::Array(permissions.iter().cloned().map(Value::String).collect())
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| substitute_permissions(v, permissions)).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), substitute_permissions(v, permissions)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// POST /v1/access/admin/oauth/complete: the code-for-token exchange
/// (plus the spec's test call) behind the browser consent.
async fn oauth_complete(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_access_store::OAuthComplete>,
) -> Result<Json<weft_access_store::CompletedConnect>, ApiError> {
    control_plane(&state, &headers).await?;
    weft_access_store::complete_oauth(&state.pool, &req.state, &req.code)
        .await
        .map(Json)
        .map_err(access_err)
}

/// POST /v1/access/admin/lookup: run a `remote_select` list source
/// through the stored connection; the editor sees label/id pairs only.
/// An ours-owned connection stores no credential of its own, so the
/// runtime's credential source is asked for a design-time key,
/// through the same meter allowlist the execution path enforces.
async fn lookup(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<weft_access_store::LookupRequest>>,
) -> Result<Json<weft_access_store::LookupPage>, ApiError> {
    control_plane(&state, &headers).await?;
    let inner = &req.inner;
    let url = weft_access_store::lookup_url(&inner.lookup, &inner.query, &inner.parents)
        .map_err(access_err)?;
    let cursor = inner.cursor.as_deref();
    // A `public` source is credential-free by declaration: no resolve,
    // no signing, whatever connection the node may hold. https only:
    // the call carries no secret, but its answer feeds a picker, and a
    // cleartext-tampered list is a hole nothing else would catch.
    if inner.lookup.public {
        if !url.starts_with("https://") {
            return Err(shared_app_err(anyhow::anyhow!(
                "a public list source must use https (got {url})"
            )));
        }
        return weft_access_store::lookup(None, &inner.lookup, cursor, &url)
            .await
            .map(Json)
            .map_err(access_err);
    }
    let Some(access_id) = inner.access_id else {
        return Err(shared_app_err(anyhow::anyhow!(
            "this list source signs with a connection, but none was given; pick one on the node"
        )));
    };
    let mut resolved = weft_access_store::resolve_for_worker(
        &state.pool,
        &req.tenant,
        access_id,
        &inner.service,
        &[],
        &[],
    )
    .await
    .map_err(access_err)?;
    if resolved.owner == weft_core::CredentialOwner::Ours {
        // A REFUSAL is policy text the editor shows verbatim; an
        // internal failure answers opaquely (its message may quote
        // configuration internals that must never travel).
        let grant = crate::credential::design_sign(
            state.credentials.as_ref(),
            &state.pool,
            &req.tenant,
            &mut resolved,
            url,
        )
        .await
        .map_err(|e| match e {
            crate::credential::DesignError::Refused(m) => shared_app_err(anyhow::anyhow!(m)),
            crate::credential::DesignError::Internal(e) => {
                tracing::error!(target: "weft_broker::access_admin", "design signing: {e:#}");
                (StatusCode::INTERNAL_SERVER_ERROR, "access store error".into())
            }
        })?;
        let page =
            weft_access_store::lookup(Some(&resolved), &inner.lookup, cursor, &grant.url).await;
        // Retire the design credential whatever the call's outcome, so
        // a source that mints short-lived credentials never leaks one
        // per lookup. Best effort; a static key's close is a no-op.
        if let Err(e) =
            state.credentials.close(&state.pool, &grant.credential, &req.tenant).await
        {
            tracing::warn!(
                target: "weft_broker::access_admin",
                "closing a design-time credential failed: {e:#}"
            );
        }
        return page.map(Json).map_err(access_err);
    }
    weft_access_store::lookup(Some(&resolved), &inner.lookup, cursor, &url)
        .await
        .map(Json)
        .map_err(access_err)
}

/// POST /v1/access/admin/granted: read a `granted` source off the
/// connection row. Runs here beside the other resolve-backed reads so
/// the store's resolve path (tenant wall, refresh) has one caller
/// shape; no outbound call is made.
async fn granted(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<GrantedQuery>>,
) -> Result<Json<Vec<weft_access_store::LookupItem>>, ApiError> {
    control_plane(&state, &headers).await?;
    weft_access_store::granted_items(
        &state.pool,
        &req.tenant,
        req.inner.access_id,
        &req.inner.service,
        &req.inner.from,
        &req.inner.label,
        &req.inner.value,
    )
    .await
    .map(Json)
    .map_err(access_err)
}

/// A provider chooser widget (Google Picker class) runs in the
/// EDITOR's webview and signs itself in with the connection's access
/// token, so the token must reach the user's own browser for it.
#[derive(Deserialize)]
struct PickerTokenQuery {
    access_id: uuid::Uuid,
    service: String,
}

#[derive(Serialize)]
struct PickerToken {
    token: String,
    /// The PUBLIC client id of the app behind the connection, when an
    /// app made it. Some choosers require it (Google's picker derives
    /// the app id it must declare from it). Never a secret.
    client_id: Option<String>,
}

/// POST /v1/access/admin/picker-token: the connection's single access
/// token, for a provider chooser running in the user's own editor. The
/// tenant wall applies like every read; only a THEIR-OWN connection
/// answers (the runtime's credential never enters an editor), and only
/// the short-lived access token leaves, never a refresh token or an
/// app secret (the resolve handoff cannot carry them by construction).
async fn picker_token(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<PickerTokenQuery>>,
) -> Result<Json<PickerToken>, ApiError> {
    control_plane(&state, &headers).await?;
    let resolved = weft_access_store::resolve_for_worker(
        &state.pool,
        &req.tenant,
        req.inner.access_id,
        &req.inner.service,
        &[],
        &[],
    )
    .await
    .map_err(access_err)?;
    if resolved.owner != weft_core::CredentialOwner::TheirOwn {
        return Err((
            StatusCode::FORBIDDEN,
            "a provider chooser only runs on your own connection".into(),
        ));
    }
    // The same one-string derivation as `OpenedConnection::credential`:
    // exactly one auth step interpolating exactly one stored value.
    let token = weft_core::access::spec::single_credential(&resolved.auth, &resolved.values)
        .map_err(|why| {
            (
                StatusCode::BAD_REQUEST,
                format!(
                    "the '{}' connection has no single token a chooser could use ({why})",
                    req.inner.service
                ),
            )
        })?
        .to_string();
    Ok(Json(PickerToken {
        token,
        client_id: resolved.app_client_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shared door of a key service is gated on the runtime
    /// credential existing: without one the connect is refused NOW,
    /// naming the shared-credentials file, instead of minting a
    /// connection that could never authenticate.
    #[test]
    fn the_shared_key_gate_refuses_naming_the_file() {
        assert!(shared_key_gate("openrouter", true).is_ok());
        let err = shared_key_gate("openrouter", false).unwrap_err();
        assert!(err.contains("openrouter"), "{err}");
        assert!(err.contains("api_key"), "{err}");
        assert!(err.contains(crate::app_provider::APPS_FILE_ENV), "{err}");
    }

    /// The door is decided on the spec the store connects on: a paste
    /// connect judges the PASTE VARIANT (static), so a consent
    /// service's paste is gated as a key connect, not as oauth; a
    /// paste on a service with no paste section is refused loudly.
    #[test]
    fn the_door_decision_spec_follows_the_paste_variant() {
        let mut spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "fakeoauth",
            "acquisition": {
                "kind": "oauth2",
                "grant": { "kind": "authorization_code", "auth_url": "https://a/x" },
                "token_url": "https://a/t"
            },
            "auth": [{ "kind": "header", "name": "Authorization",
                       "value": "Bearer {token}" }]
        }))
        .unwrap();
        assert!(door_decision_spec(&spec, false).unwrap().is_oauth());
        let err = door_decision_spec(&spec, true).unwrap_err();
        assert!(err.contains("no paste section"), "{err}");

        spec.own_page = serde_json::from_value(serde_json::json!({
            "paste": { "fields": [{ "name": "token", "label": "Bot token" }] }
        }))
        .unwrap();
        let decided = door_decision_spec(&spec, true).unwrap();
        assert!(!decided.is_oauth(), "the paste variant is static, judged as such");
    }

    /// `{permissions}` substitutes ANYWHERE in the mint payload, as a
    /// JSON array of the ticked ids; everything else is untouched.
    #[test]
    fn permissions_substitute_into_the_mint_payload() {
        let payload = serde_json::json!({
            "display_information": { "name": "weft" },
            "oauth_config": { "scopes": { "bot": "{permissions}" } },
            "list": ["{permissions}", "literal"]
        });
        let out = substitute_permissions(
            &payload,
            &["chat:write".to_string(), "files:write".to_string()],
        );
        assert_eq!(
            out["oauth_config"]["scopes"]["bot"],
            serde_json::json!(["chat:write", "files:write"])
        );
        assert_eq!(out["list"][0], serde_json::json!(["chat:write", "files:write"]));
        assert_eq!(out["list"][1], "literal");
        assert_eq!(out["display_information"]["name"], "weft");
    }
}

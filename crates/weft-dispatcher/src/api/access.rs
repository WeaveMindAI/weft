//! Access-store HTTP surface on the dispatcher: registrations, grants,
//! the connect flows the editor drives, and the `remote_select`
//! resource lookups.
//!
//! Verbs that only touch the DB run here; verbs whose work makes an
//! OUTBOUND call to a tenant-influenced URL (direct connect, OAuth
//! completion, lookups) are forwarded to the broker's access admin,
//! because the broker's network egress is locked to the public
//! internet (a crafted internal URL dies at the network layer).
//!
//! Everything here is tenant-authenticated ([`CallerTenant`]) and
//! answers SUMMARIES only: a stored value (token, secret, refresh
//! token) never leaves the store side; pasted secrets arrive here
//! (editor -> store directly) and are never echoed back. The OAuth
//! callback door is the one outside-caller route
//! ([`oauth_callback`], mounted on the outside surface): the provider
//! redirect carries no tenant bearer, so the state nonce's pending row
//! is what authenticates it.


use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::Json;
use serde::{Deserialize, Serialize};

use weft_core::access::wire::{
    BeginOAuth, CompletedConnect, ConnectDirect, DoorsAnswer, DoorsRequest, DoorsStatus,
    GrantSummary, MintAppRequest, MintAppResponse, SharedDoorPick, StartedOAuth,
};
use weft_core::storage::Tenanted;

use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// A store error as this surface answers it. The mapping itself lives
/// in the store (`client_error`), so the dispatcher and the broker
/// answer identically by construction rather than by copy.
fn access_err(e: anyhow::Error) -> ApiError {
    let (status, message) = weft_access_store::client_error(e);
    (StatusCode::from_u16(status).expect("store status codes are valid"), message)
}

/// The callback URL a tenant registers at the provider. Shown by the
/// editor during app registration and used verbatim in the code
/// exchange. The consent rides the operator's browser, so the STABLE
/// base works and never rots; only a provider that refuses plain-http
/// callbacks (`callback_https` on the spec) forces an https address,
/// loudly when this weft has none.
fn redirect_uri(state: &DispatcherState, spec: &weft_core::AccessSpec) -> Result<String, ApiError> {
    let base = callback_base(
        &state.public_base_url,
        state.internet_url.as_deref(),
        spec.callback_https,
    )
    .ok_or_else(|| {
        (
            StatusCode::PRECONDITION_FAILED,
            format!(
                "the '{}' provider only accepts https callback URLs and this weft \
                 has no https public address; start the daemon with --public-url \
                 (or set an https WEFT_DISPATCHER_PUBLIC_BASE_URL)",
                spec.service
            ),
        )
    })?;
    Ok(format!("{}/access/oauth/callback", base.trim_end_matches('/')))
}

/// Which base the OAuth callback rides: the STABLE base, unless the
/// provider demands https and the base is not, in which case the
/// additional internet address serves iff it is https. `None` = no
/// https address exists for a provider that requires one. Pure so the
/// branching is unit-tested below.
fn callback_base<'a>(base: &'a str,
    internet: Option<&'a str>,
    https_only: bool,
) -> Option<&'a str> {
    if !https_only || base.starts_with("https://") {
        return Some(base);
    }
    internet.filter(|u| u.starts_with("https://"))
}

// ---------- Doors (which connect doors are actually open) ----------

/// POST /access/doors: which shared-door options this weft can offer
/// for a service. The probe is forwarded to the broker (which holds
/// the registered apps and the runtime-credential probe); the
/// dispatcher adds the callback URL, since only it knows the public
/// host.
pub async fn doors(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
    Json(req): Json<DoorsRequest>,
) -> Result<Json<DoorsStatus>, ApiError> {
    let doors: DoorsAnswer =
        crate::broker_admin::forward_json(&state, "/v1/access/admin/doors", &req).await?;
    // A provider demanding https on a weft with no https address
    // blocks every CONSENT, not the panel: paste connects need no
    // callback, so the probe reports the block instead of failing.
    let (redirect_uri, consent_blocked) = match redirect_uri(&state, &req.spec) {
        Ok(uri) => (Some(uri), None),
        Err((_, msg)) => (None, Some(msg)),
    };
    Ok(Json(DoorsStatus { doors, redirect_uri, consent_blocked }))
}

/// POST /access/mint-app: run the service's "Create it for me" recipe;
/// forwarded to the broker (its egress is locked to the public
/// internet). Answers the minted app's captured values, for the
/// editor to prefill the "Your own" form.
pub async fn mint_app(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
    Json(req): Json<MintAppRequest>,
) -> Result<Json<MintAppResponse>, ApiError> {
    crate::broker_admin::forward_json(&state, "/v1/access/admin/mint-app", &req)
        .await
        .map(Json)
}

// ---------- Grants ----------

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub service: Option<String>,
}

/// GET /access/grants?service=: the tenant's connected accounts
/// (summaries only).
pub async fn list_grants(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<GrantSummary>>, ApiError> {
    weft_access_store::list_grants(&state.pg_pool, &caller.0 .0, q.service.as_deref())
        .await
        .map(Json)
        .map_err(access_err)
}

/// DELETE /access/grants/{id}
pub async fn delete_grant(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<()>, ApiError> {
    weft_access_store::delete_grant(&state.pg_pool, &caller.0 .0, id)
        .await
        .map(Json)
        .map_err(access_err)
}

// ---------- Connect flows ----------

/// POST /access/connect/direct: a paste / mint / server-to-server
/// connect that completes in one request. Forwarded to the broker,
/// which makes the spec's test call / token exchange (its egress is
/// locked to the public internet; see the broker's access admin).
pub async fn connect_direct(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<SharedDoorPick<ConnectDirect>>,
) -> Result<Json<CompletedConnect>, ApiError> {
    crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/connect/direct",
        &Tenanted { tenant: caller.0 .0, inner: req },
    )
    .await
    .map(Json)
}

/// POST /access/connect/begin: park a pending browser consent and
/// answer the consent URL the editor opens. Forwarded to the broker
/// (which resolves the fallback app and parks the row); the dispatcher
/// fills the callback URL first, since only it knows the public host.
pub async fn connect_begin(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<SharedDoorPick<BeginOAuth>>,
) -> Result<Json<StartedOAuth>, ApiError> {
    req.inner.redirect_uri = redirect_uri(&state, &req.inner.spec)?;
    crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/oauth/begin",
        &Tenanted { tenant: caller.0 .0, inner: req },
    )
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
pub struct ConnectStatusQuery {
    pub state: String,
}

/// GET /access/connect/status?state=: the editor's poll after opening
/// the consent page. Answers the parked outcome once (`{"grant":..}`
/// or `{"error":..}`), or `null` while the consent is still pending.
pub async fn connect_status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Query(q): Query<ConnectStatusQuery>,
) -> Result<Json<Option<serde_json::Value>>, ApiError> {
    weft_access_store::take_connect_result(&state.pg_pool, &caller.0 .0, &q.state)
        .await
        .map(Json)
        .map_err(access_err)
}

// ---------- The OAuth callback door (outside-caller surface) ----------

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub state: Option<String>,
    pub code: Option<String>,
    /// The provider's refusal (the user clicked deny, a bad scope).
    pub error: Option<String>,
    pub error_description: Option<String>,
}

/// GET /access/oauth/callback: where the provider redirects the
/// browser after consent. Completes the exchange and answers a tiny
/// human page; the EDITOR learns the outcome through its status poll,
/// never through this response.
pub async fn oauth_callback(
    State(state): State<DispatcherState>,
    Query(q): Query<CallbackQuery>,
) -> Html<String> {
    let outcome = match (&q.state, &q.code, &q.error) {
        (_, _, Some(err)) => Err(format!(
            "the provider refused the sign-in: {err}{}",
            q.error_description.as_deref().map(|d| format!(" ({d})")).unwrap_or_default()
        )),
        (Some(st), Some(code), None) => {
            // Forwarded to the broker, which makes the code-for-token
            // exchange (its egress is locked to the public internet).
            let fwd: Result<CompletedConnect, ApiError> =
                crate::broker_admin::forward_json(
                    &state,
                    "/v1/access/admin/oauth/complete",
                    &weft_access_store::OAuthComplete {
                        state: st.clone(),
                        code: code.clone(),
                    },
                )
                .await;
            match fwd {
                Ok(done) => Ok(done.grant),
                Err((_, msg)) => Err(msg),
            }
        }
        _ => Err("the sign-in redirect is missing its code or state".into()),
    };
    let body = match outcome {
        Ok(grant) => format!(
            "<h2>Connected{}</h2><p>You can close this tab and return to the editor.</p>",
            grant
                .identity
                .as_deref()
                .map(|i| format!(" as {}", html_escape(i)))
                .unwrap_or_default()
        ),
        Err(reason) => format!(
            "<h2>Sign-in failed</h2><p>{}</p><p>Close this tab and retry from the editor.</p>",
            html_escape(&reason)
        ),
    };
    Html(format!(
        "<!doctype html><meta charset=\"utf-8\"><title>weft</title>\
         <body style=\"font-family: system-ui; margin: 4rem auto; max-width: 32rem\">{body}</body>"
    ))
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

// ---------- Resource lookups (remote_select) ----------

/// POST /access/lookup: run a `remote_select` lookup through the
/// stored access. Forwarded to the broker, which holds the token and
/// makes the call (its egress is locked to the public internet); the
/// editor sees label/id pairs only.
pub async fn lookup(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<weft_access_store::LookupRequest>,
) -> Result<Json<weft_access_store::LookupPage>, ApiError> {
    crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/lookup",
        &Tenanted { tenant: caller.0 .0, inner: req },
    )
    .await
    .map(Json)
}

// ---------- Resource picker sessions ----------

/// POST /access/picker/begin: park a picker session (the node-declared
/// chooser script + glue, and the connection it picks against) and
/// answer the weft-served page's address. The editor opens the page
/// in the user's browser and polls `/access/connect/status` for the
/// parked pick, exactly like a consent.
pub async fn picker_begin(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<weft_access_store::BeginPicker>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let picker_state = weft_access_store::begin_picker(&state.pg_pool, &caller.0 .0, req)
        .await
        .map_err(access_err)?;
    let url = format!(
        "{}/access/picker/{picker_state}",
        state.external_base_url().trim_end_matches('/')
    );
    Ok(Json(serde_json::json!({ "state": picker_state, "url": url })))
}

/// GET /access/picker/{state}: the picker page (outside surface, like
/// the OAuth callback: the state nonce's parked session, minted by the
/// tenant-authenticated begin, is what authenticates it). Serves a
/// page that loads the NODE-DECLARED chooser script, runs the node
/// author's glue with the connection's own token, and posts the picked
/// resource back. The glue runs HERE, scoped to this page, never
/// inside the editor.
pub async fn picker_page(
    State(state): State<DispatcherState>,
    Path(picker_state): Path<String>,
) -> Result<Html<String>, ApiError> {
    let session = weft_access_store::load_picker(&state.pg_pool, &picker_state)
        .await
        .map_err(access_err)?;
    // The connection's token, resolved on the broker (a refresh makes
    // an outbound call, and the broker's egress is the locked one).
    #[derive(Serialize)]
    struct TokenQuery {
        access_id: uuid::Uuid,
        service: String,
    }
    #[derive(Deserialize)]
    struct Token {
        token: String,
        client_id: Option<String>,
    }
    let token: Token = crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/picker-token",
        &Tenanted {
            tenant: session.tenant.clone(),
            inner: TokenQuery {
                access_id: session.access_id,
                service: session.service.clone(),
            },
        },
    )
    .await?;
    let result_url = format!(
        "{}/access/picker/{picker_state}/result",
        state.external_base_url().trim_end_matches('/')
    );
    // The page runs NODE-declared script on purpose, un-sandboxed:
    // provider choosers (Google's included) need their own cookies for
    // the signed-in session their UI rides, and a sandbox blocks every
    // frame's cookies wholesale. What keeps this page harmless is what
    // it can reach: one session's own token, on a host that serves no
    // cookie-authenticated surface.
    Ok(Html(render_picker_page(
        &session.script,
        &session.code,
        &token.token,
        token.client_id.as_deref(),
        &session.mime_types,
        &result_url,
    )))
}

/// The picker page's HTML. The session's dynamic values travel as ONE
/// json block (script-safe escaped); the author's glue is inlined
/// verbatim, deliberately: it is the node's own code, running on this
/// scoped page with this connection's token, which is the whole
/// design (the editor never runs it).
fn render_picker_page(
    script: &str,
    code: &str,
    token: &str,
    client_id: Option<&str>,
    mime_types: &[String],
    result_url: &str,
) -> String {
    let data = serde_json::json!({
        "token": token,
        "clientId": client_id,
        "mimeTypes": mime_types,
        "resultUrl": result_url,
    })
    .to_string()
    // `<` cannot appear un-escaped inside a script-ish block (it could
    // open `</script>`); JSON strings render `<` identically.
    .replace('<', "\\u003c");
    let script_attr = html_escape(script);
    format!(
        "<!doctype html><meta charset=\"utf-8\"><title>weft picker</title>\
         <body style=\"font-family: system-ui; margin: 2rem\">\
         <p id=\"weft-status\">Opening the chooser...</p>\
         <script id=\"weft-picker-data\" type=\"application/json\">{data}</script>\
         <script src=\"{script_attr}\"></script>\
         <script>\
         const weftData = JSON.parse(document.getElementById('weft-picker-data').textContent);\
         let weftDone = false;\
         async function weftPost(body, note) {{\
           if (weftDone) return; weftDone = true;\
           try {{ await fetch(weftData.resultUrl, {{ method: 'POST',\
             headers: {{ 'content-type': 'application/json' }},\
             body: JSON.stringify(body) }}); }} catch (e) {{}}\
           document.getElementById('weft-status').textContent = note;\
         }}\
         const weft = {{\
           token: weftData.token,\
           clientId: weftData.clientId,\
           mimeTypes: weftData.mimeTypes,\
           done(picked) {{ weftPost({{ picked: {{ id: String(picked.id),\
             label: picked.label ? String(picked.label) : String(picked.id) }} }},\
             'Picked. You can close this tab.'); }},\
           cancel() {{ weftPost({{ cancelled: true }},\
             'Nothing picked. You can close this tab.'); }},\
           fail(message) {{ weftPost({{ error: String(message) }},\
             'The chooser failed: ' + String(message)); }},\
         }};\
         (async () => {{\n{code}\n}})().catch((e) => weft.fail(e));\
         </script></body>"
    )
}

/// POST /access/picker/{state}/result: the picker page posts its
/// outcome (outside surface; the single-use state nonce authenticates
/// it). Only the two legal shapes are parked; anything else is a 400.
pub async fn picker_result(
    State(state): State<DispatcherState>,
    Path(picker_state): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<()>, ApiError> {
    let parked = if body.get("cancelled").and_then(serde_json::Value::as_bool) == Some(true) {
        serde_json::json!({ "cancelled": true })
    } else if let Some(err) = body.get("error").and_then(serde_json::Value::as_str) {
        serde_json::json!({ "error": weft_core::truncate_user_string(err, 500) })
    } else if let Some(picked) = body.get("picked") {
        let id = picked.get("id").and_then(serde_json::Value::as_str);
        let label = picked.get("label").and_then(serde_json::Value::as_str);
        match (id, label) {
            (Some(id), Some(label)) if !id.is_empty() => serde_json::json!({ "picked": {
                "id": weft_core::truncate_user_string(id, 500),
                "label": weft_core::truncate_user_string(label, 500),
            }}),
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "a pick needs a non-empty id and a label".into(),
                ))
            }
        }
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "the picker result is `{picked: {id, label}}` or `{error}`".into(),
        ));
    };
    weft_access_store::finish_picker(&state.pg_pool, &picker_state, parked)
        .await
        .map_err(access_err)?;
    Ok(Json(()))
}

/// POST /access/granted: read a `granted` resource source off the
/// connection row (the resources recorded during sign-in). Forwarded
/// to the broker beside the other resolve-backed reads.
pub async fn granted(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<weft_access_store::GrantedQuery>,
) -> Result<Json<Vec<weft_access_store::LookupItem>>, ApiError> {
    crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/granted",
        &Tenanted { tenant: caller.0 .0, inner: req },
    )
    .await
    .map(Json)
}

#[cfg(test)]
mod callback_base_tests {
    use super::callback_base;

    /// The stable base serves every provider without an https demand,
    /// tunnel or not: a tunnel must never rotate the registered
    /// callback of a provider that accepts the stable address.
    #[test]
    fn stable_base_wins_without_an_https_demand() {
        assert_eq!(callback_base("http://127.0.0.1:9998", None, false), Some("http://127.0.0.1:9998"));
        assert_eq!(
            callback_base("http://127.0.0.1:9998", Some("https://x.trycloudflare.com"), false),
            Some("http://127.0.0.1:9998")
        );
    }

    /// An https demand met by the stable base needs no second address.
    #[test]
    fn https_stable_base_meets_the_demand_itself() {
        assert_eq!(
            callback_base("https://weft.example.com", Some("https://x.trycloudflare.com"), true),
            Some("https://weft.example.com")
        );
    }

    /// An https demand on an http base uses the internet address iff
    /// it is https; none = no callback, the caller teaches the fix.
    #[test]
    fn https_demand_on_http_base_needs_an_https_internet_address() {
        assert_eq!(
            callback_base("http://127.0.0.1:9998", Some("https://x.trycloudflare.com"), true),
            Some("https://x.trycloudflare.com")
        );
        assert_eq!(callback_base("http://127.0.0.1:9998", None, true), None);
        assert_eq!(callback_base("http://127.0.0.1:9998", Some("http://tunnel.local"), true), None);
    }
}

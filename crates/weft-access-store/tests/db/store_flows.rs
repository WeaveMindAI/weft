//! Layer-3 contract tests for the access store: the real store code
//! against a fresh Postgres (via `#[weft_db_macros::test]`) and a hand-rolled
//! in-process fake provider (token endpoint + API), so every flow runs
//! end to end without touching a real third party.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;
use weft_task_store::db::{self, Client as _, JsonText, Pool};

use weft_access_store::{
    begin_oauth, complete_oauth, connect_direct, delete_grant, list_grants, lookup,
    resolve_for_worker, take_connect_result, AccessError, GrantUser,
};
use weft_core::access::spec::Door;
use weft_core::access::wire::{BeginOAuth, ConnectDirect, GrantSummary};
use weft_core::{AccessSpec, AppRegistration, CredentialOwner};

/// The app a project declares for the OAuth fixture service (client id +
/// secret). In production the editor sends this from the project's
/// `accessApps`; the store just uses it.
fn oauth_app() -> AppRegistration {
    AppRegistration {
        label: "Test app".into(),
        client_id: "cid".into(),
        client_secret: Some("sec".into()),
        extra: Default::default(),
    }
}

const PROJECT_1: uuid::Uuid = uuid::Uuid::from_u128(0xc0de);
const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// The fake provider: a token endpoint, a test-call endpoint, and a
/// paginated list endpoint, all recording what they saw. Dumb by
/// design: canned answers from a map, calls in an append-only log.
struct FakeProvider {
    /// Every request path received, in order.
    pub calls: Mutex<Vec<String>>,
    /// How many TOKEN requests were served (the single-flight probe).
    pub token_requests: AtomicUsize,
    /// The token the next token request answers with.
    pub next_token: Mutex<String>,
    /// The refresh token the next token request answers with (rotation).
    pub next_refresh: Mutex<Option<String>>,
    /// Scope echo on the token response, if any.
    pub scope_echo: Mutex<Option<String>>,
    /// expires_in on the token response, if any.
    pub expires_in: Mutex<Option<i64>>,
    /// The status a token request is refused with, if any.
    pub token_refusal: Mutex<Option<u16>>,
    /// The status the exchange-style renewal is refused with, if any.
    pub exchange_refusal: Mutex<Option<u16>>,
    /// A whole token answer to send instead of the flat one built from
    /// the fields above (a nested answer, an error object on a 200).
    pub token_answer: Mutex<Option<serde_json::Value>>,
    /// Held by a test to keep every token request from answering until
    /// it lets go.
    pub token_gate: tokio::sync::Mutex<()>,
    /// One permit per token request that arrived, before it waits at
    /// the gate.
    pub token_arrived: tokio::sync::Semaphore,
}

impl FakeProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            token_requests: AtomicUsize::new(0),
            next_token: Mutex::new("tok-1".into()),
            next_refresh: Mutex::new(None),
            scope_echo: Mutex::new(None),
            expires_in: Mutex::new(None),
            token_refusal: Mutex::new(None),
            exchange_refusal: Mutex::new(None),
            token_answer: Mutex::new(None),
            token_gate: tokio::sync::Mutex::new(()),
            token_arrived: tokio::sync::Semaphore::new(0),
        })
    }

    /// Hold the token endpoint from now on: every token request from here
    /// waits until the returned guard is dropped, and only those count as
    /// arrived (the ones before are forgotten).
    async fn hold_tokens(&self) -> tokio::sync::MutexGuard<'_, ()> {
        let gate = self.token_gate.lock().await;
        let earlier = u32::try_from(self.token_arrived.available_permits()).unwrap();
        if earlier > 0 {
            self.token_arrived.try_acquire_many(earlier).unwrap().forget();
        }
        gate
    }

    /// Serve on an ephemeral port; returns the base URL.
    async fn serve(self: &Arc<Self>) -> String {
        use axum::extract::{Query, State};
        use axum::routing::{get, post};
        let app = axum::Router::new()
            .route(
                "/token",
                post(|State(s): State<Arc<FakeProvider>>, body: String| async move {
                    s.calls.lock().unwrap().push(format!("/token {body}"));
                    s.token_requests.fetch_add(1, Ordering::SeqCst);
                    s.token_arrived.add_permits(1);
                    let _gate = s.token_gate.lock().await;
                    if let Some(status) = *s.token_refusal.lock().unwrap() {
                        let status = axum::http::StatusCode::from_u16(status).unwrap();
                        return (status, axum::Json(json!({ "error": "refused" })));
                    }
                    if let Some(answer) = s.token_answer.lock().unwrap().clone() {
                        return (axum::http::StatusCode::OK, axum::Json(answer));
                    }
                    let mut resp = json!({ "access_token": *s.next_token.lock().unwrap() });
                    if let Some(rt) = s.next_refresh.lock().unwrap().clone() {
                        resp["refresh_token"] = json!(rt);
                    }
                    if let Some(scope) = s.scope_echo.lock().unwrap().clone() {
                        resp["scope"] = json!(scope);
                    }
                    if let Some(e) = *s.expires_in.lock().unwrap() {
                        resp["expires_in"] = json!(e);
                    }
                    (axum::http::StatusCode::OK, axum::Json(resp))
                }),
            )
            .route(
                "/whoami",
                get(|State(s): State<Arc<FakeProvider>>, headers: axum::http::HeaderMap| async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    s.calls.lock().unwrap().push(format!("/whoami {auth}"));
                    if auth.starts_with("Bearer tok-") {
                        (axum::http::StatusCode::OK, axum::Json(json!({ "user": "quentin" })))
                    } else {
                        (axum::http::StatusCode::UNAUTHORIZED, axum::Json(json!({})))
                    }
                }),
            )
            .route(
                "/channels",
                get(|State(s): State<Arc<FakeProvider>>, headers: axum::http::HeaderMap, Query(q): Query<BTreeMap<String, String>>| async move {
                    let auth = if headers.contains_key("authorization") { "authed" } else { "bare" };
                    s.calls.lock().unwrap().push(format!("/channels {auth} {q:?}"));
                    let page2 = q.get("cursor").map(String::as_str) == Some("next-1");
                    let items = if page2 {
                        json!([{ "id": "C3", "name": "three" }])
                    } else {
                        json!([{ "id": "C1", "name": "one" }, { "id": "C2", "name": "two" }])
                    };
                    axum::Json(json!({
                        "channels": items,
                        "meta": { "next": if page2 { "" } else { "next-1" } }
                    }))
                }),
            )
            .route(
                "/watch/{target}",
                post(|State(s): State<Arc<FakeProvider>>,
                      axum::extract::Path(target): axum::extract::Path<String>,
                      body: String| async move {
                    s.calls.lock().unwrap().push(format!("/watch/{target} {body}"));
                    // A far-future epoch-millisecond expiry, the shape
                    // the watch-class providers answer.
                    axum::Json(json!({
                        "expiration": "4102444800000",
                        "resourceId": format!("res-{target}")
                    }))
                }),
            )
            // A Slack-class API: every answer is a 200, and a refusal
            // says so inside the body.
            .route(
                "/refusing",
                get(|State(s): State<Arc<FakeProvider>>| async move {
                    s.calls.lock().unwrap().push("/refusing".into());
                    axum::Json(json!({ "ok": false, "error": "invalid_auth" }))
                }),
            )
            .route(
                "/stop",
                post(|State(s): State<Arc<FakeProvider>>, body: String| async move {
                    s.calls.lock().unwrap().push(format!("/stop {body}"));
                    axum::Json(json!({}))
                }),
            )
            // The exchange-style renewal (Meta's long-lived-token
            // shape): a GET interpolating the CURRENT token, answered
            // with a fresh one. Not the standard refresh POST.
            .route(
                "/exchange",
                get(|State(s): State<Arc<FakeProvider>>, Query(q): Query<BTreeMap<String, String>>| async move {
                    s.calls.lock().unwrap().push(format!("/exchange {q:?}"));
                    if let Some(status) = *s.exchange_refusal.lock().unwrap() {
                        let status = axum::http::StatusCode::from_u16(status).unwrap();
                        return (status, axum::Json(json!({ "error": "refused" })));
                    }
                    let mut resp = json!({ "access_token": *s.next_token.lock().unwrap() });
                    if let Some(e) = *s.expires_in.lock().unwrap() {
                        resp["expires_in"] = json!(e);
                    }
                    (axum::http::StatusCode::OK, axum::Json(resp))
                }),
            )
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }
}

fn static_spec(base: &str) -> AccessSpec {
    serde_json::from_value(json!({
        "service": "fakestatic",
        "acquisition": { "kind": "static", "fields": [{ "name": "token" }] },
        "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }],
        "test": { "url": format!("{base}/whoami"), "captures": [{ "name": "user", "path": "user" }] },
        "identity": "{user}"
    }))
    .unwrap()
}

fn oauth_spec(base: &str, grants: &str) -> AccessSpec {
    serde_json::from_value(json!({
        "service": "fakeoauth",
        "grants": grants,
        "doors": ["shared", "own"],
        "acquisition": {
            "kind": "oauth2",
            "grant": { "kind": "authorization_code",
                       "authUrl": format!("{base}/authorize"), "pkce": true },
            "tokenUrl": format!("{base}/token")
        },
        "permissions": [
            { "id": "read", "label": "Read", "description": "Read things.", "default": true },
            { "id": "write", "label": "Write", "description": "Write things." },
            { "id": "admin", "label": "Admin", "description": "Administer things." }
        ],
        "verification": { "rung": "reports_permissions", "cost": "free" },
        "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }],
        "identity": "{user}",
        "test": { "url": format!("{base}/whoami"), "captures": [{ "name": "user", "path": "user" }] }
    }))
    .unwrap()
}

/// Begin one consent for `tenant` through the own door, returning its
/// state.
async fn begin_consent(
    pool: &Pool,
    tenant: &str,
    spec: &AccessSpec,
    scopes: &[&str],
    upgrade: Option<uuid::Uuid>,
) -> anyhow::Result<String> {
    let started = begin_oauth(
        pool,
        tenant,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Own,
            registration: Some(oauth_app()),
            permissions: scopes.iter().map(|s| s.to_string()).collect(),
            project_id: Some(PROJECT_1),
            instance: None,
            upgrade_grant_id: upgrade,
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await?;
    assert!(started.consent_url.contains("code_challenge"), "{}", started.consent_url);
    Ok(started.state)
}

/// Begin + complete one consent for `tenant`, returning the grant id.
async fn full_consent(
    pool: &Pool,
    tenant: &str,
    spec: &AccessSpec,
    scopes: &[&str],
    upgrade: Option<uuid::Uuid>,
) -> anyhow::Result<GrantSummary> {
    let state = begin_consent(pool, tenant, spec, scopes, upgrade).await?;
    // While the consent is open, the poll reads pending.
    assert!(take_connect_result(pool, tenant, &state).await?.is_none());
    let done = complete_oauth(pool, &state, "the-code").await?;
    // The parked outcome is consumable exactly once, by the owning tenant.
    let parked = take_connect_result(pool, tenant, &state).await?;
    assert!(parked.is_some(), "outcome parked for the editor's poll");
    let again = take_connect_result(pool, tenant, &state).await.unwrap_err();
    assert!(matches!(again.downcast_ref::<AccessError>(), Some(AccessError::Gone(_))), "{again}");
    Ok(done.grant)
}

/// A paste connect on a CONSENT service (the `ownPage.paste`
/// section): the stored snapshot is the static variant, so the row
/// resolves like a natively pasted credential and never refreshes
/// through an app.
#[weft_db_macros::test]
async fn paste_connect_on_a_consent_service_stores_the_static_variant(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let mut spec = oauth_spec(&base, "coexisting");
    spec.own_page = serde_json::from_value(json!({
        "paste": { "fields": [{ "name": "token", "label": "Bot token" }] }
    }))
    .unwrap();
    spec.validate().expect("paste section is valid");

    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: true,
            spec,
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-pasted".to_string())].into_iter().collect(),
            label: Some("Hand-made bot".into()),
            permissions: vec!["read".into()],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("a pasted credential connects without an app");
    assert_eq!(done.grant.identity.as_deref(), Some("quentin"), "test call captured identity");

    // The row resolves like any static credential: the pasted token
    // rides the auth step, no app, no refresh.
    let resolved = weft_access_store::resolve_for_worker(
        &pool,
        TENANT_A, GrantUser::Author,
        done.grant.id,
        "fakeoauth",
        &[],
        &[],
    )
    .await
    .expect("a pasted row resolves");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-pasted"));
    assert_eq!(resolved.app_client_id, None, "no app made this row");

    // Refusals stay loud: a paste connect on a service with no paste
    // section is a config error, not a quiet static connect.
    let err = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: true,
            spec: oauth_spec(&base, "coexisting"),
            door: Door::Own,
            registration: None,
            values: BTreeMap::new(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect_err("no paste section declared");
    assert!(err.to_string().contains("no paste section"), "{err}");
}

#[weft_db_macros::test]
async fn static_connect_runs_the_test_call_and_stores_identity(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;

    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: Some("My token".into()),
            permissions: Vec::new(),
            project_id: Some(PROJECT_1),
            instance: None,
        },
    )
    .await
    .expect("a working token connects");
    assert_eq!(done.grant.identity.as_deref(), Some("quentin"));

    // The test call really carried the auth step.
    let calls = fake.calls.lock().unwrap().clone();
    assert!(calls.iter().any(|c| c.contains("Bearer tok-abc")), "{calls:?}");

    // A dead token is refused at connect, loudly, and stores nothing.
    let err = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "bad".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("test call"), "{err}");
    assert_eq!(list_grants(&pool, TENANT_A, Some("fakestatic"), weft_access_store::GrantOwnerScope::Author).await.unwrap().len(), 1);
}

#[weft_db_macros::test]
async fn the_tenant_wall_holds_on_every_surface(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap();
    let id = done.grant.id;

    // Another tenant cannot list, resolve, or delete the grant;
    // resolution answers NOT FOUND (no existence leak). A lookup
    // starts from that same resolve, so the wall covers it too.
    assert!(list_grants(&pool, TENANT_B, None, weft_access_store::GrantOwnerScope::Author).await.unwrap().is_empty());
    let err = resolve_for_worker(&pool, TENANT_B, GrantUser::Author, id, "fakestatic", &[], &[]).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");
    assert!(delete_grant(&pool, TENANT_B, id, weft_access_store::GrantOwnerScope::Author).await.is_err());

    // The owner resolves fine, and receives what the connection
    // holds: the pasted token AND the value its test call captured.
    // Visibility is "everything except the store's keep-alive
    // material", not "only what the auth steps interpolate".
    let resolved = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, id, "fakestatic", &[], &[]).await.unwrap();
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-abc"));
    assert_eq!(resolved.values.get("user").map(String::as_str), Some("quentin"));
    assert!(
        !resolved.values.contains_key("refresh_token"),
        "the store's keep-alive material never travels"
    );
    assert_eq!(resolved.identity.as_deref(), Some("quentin"));

    // A wrong-service marker is refused by name.
    let err = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, id, "slack", &[], &[]).await.unwrap_err();
    assert!(err.to_string().contains("wire the right service"), "{err}");
}

/// A poll under a state nothing is live under ends: an unknown state,
/// and another tenant's open consent, both answer gone.
#[weft_db_macros::test]
async fn a_poll_on_a_state_with_nothing_live_answers_gone(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let started = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: oauth_spec(&base, "coexisting"),
            door: Door::Own,
            registration: Some(oauth_app()),
            permissions: vec!["read".into()],
            project_id: Some(PROJECT_1),
            instance: None,
            upgrade_grant_id: None,
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await
    .unwrap();
    for (tenant, state) in [(TENANT_A, "no-such-state"), (TENANT_B, started.state.as_str())] {
        let err = take_connect_result(&pool, tenant, state).await.unwrap_err();
        assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Gone(_))), "{err}");
    }
    assert!(take_connect_result(&pool, TENANT_A, &started.state).await.unwrap().is_none());
}

/// A permission the service marks `always` rides every consent on top
/// of what was ticked, is recorded with it, and is held to the
/// provider's echo like a ticked one.
#[weft_db_macros::test]
async fn a_consent_asks_for_what_the_service_always_needs(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let mut spec = oauth_spec(&base, "coexisting");
    spec.permissions.insert(
        0,
        serde_json::from_value(json!({ "id": "whoami", "label": "Who", "description": "Know the account.", "always": true })).unwrap(),
    );
    let grant = full_consent(&pool, TENANT_A, &spec, &["write"], None).await.unwrap();
    assert_eq!(grant.scopes, vec!["whoami", "write"], "asked for, and recorded, with the ticked one");

    *fake.scope_echo.lock().unwrap() = Some("write".into());
    let err = full_consent(&pool, TENANT_A, &spec, &["write"], None).await.unwrap_err();
    assert!(err.to_string().contains("missing 'whoami'"), "{err}");
}

#[weft_db_macros::test]
async fn oauth_consent_records_granted_scopes_and_enforces_the_echo(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    let grant = full_consent(&pool, TENANT_A, &spec, &["read", "write"], None).await.unwrap();
    assert_eq!(grant.scopes, vec!["read", "write"], "no echo = the ticked set, recorded claimed");
    assert!(!grant.permissions_verified, "no provider answer = claimed, never verified");
    assert_eq!(grant.project_id, Some(PROJECT_1), "coexisting = per project");

    // With the provider's echo, the recorded set is VERIFIED.
    *fake.scope_echo.lock().unwrap() = Some("read write".into());
    let verified = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    assert!(verified.permissions_verified);
    assert_eq!(verified.scopes, vec!["read", "write"], "the echo is what lands");
    *fake.scope_echo.lock().unwrap() = None;

    // The exchange carried the PKCE verifier and the client secret.
    let calls = fake.calls.lock().unwrap().clone();
    let token_call = calls.iter().find(|c| c.starts_with("/token")).unwrap();
    assert!(token_call.contains("code_verifier"), "{token_call}");
    assert!(token_call.contains("client_secret=sec"), "{token_call}");

    // A provider granting FEWER scopes than ticked is refused loudly.
    *fake.scope_echo.lock().unwrap() = Some("read".into());
    let err = full_consent(&pool, TENANT_A, &spec, &["read", "admin"], None).await.unwrap_err();
    assert!(err.to_string().contains("granted fewer permissions"), "{err}");

    // Ticked permissions outside the catalogue are refused at begin.
    let err = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Own,
            registration: Some(oauth_app()),
            permissions: vec!["not-a-scope".into()],
            project_id: None,
            instance: None,
            upgrade_grant_id: None,
            redirect_uri: "http://disp.example/cb".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("permission catalogue"), "{err}");

    // The backstop: a consent with NO resolved app (project declared
    // none and no fallback) fails loud, naming the fix.
    let err = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Own,
            registration: None,
            permissions: vec!["read".into()],
            project_id: None,
            instance: None,
            upgrade_grant_id: None,
            redirect_uri: "http://disp.example/cb".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("accessApps"), "{err}");
}

#[weft_db_macros::test]
async fn an_exclusive_grant_rotates_in_place_and_upgrades_by_union(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "exclusive");
    // Echo mirrors the ticks, so the recorded sets are VERIFIED (the
    // drift backstop below only refuses a verified shortfall).
    *fake.scope_echo.lock().unwrap() = Some("read".into());
    let first = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    assert_eq!(first.project_id, None, "exclusive = honestly shared, no project pin");

    // A re-consent for the SAME account rotates the row (the provider
    // rotated the token under us anyway): same id, no sibling.
    *fake.next_token.lock().unwrap() = "tok-2".into();
    let second = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    assert_eq!(second.id, first.id, "same account = same row, rotated");
    assert_eq!(list_grants(&pool, TENANT_A, Some("fakeoauth"), weft_access_store::GrantOwnerScope::Author).await.unwrap().len(), 1);

    // An explicit upgrade unions the scopes on the SAME row.
    *fake.scope_echo.lock().unwrap() = Some("write".into());
    let upgraded =
        full_consent(&pool, TENANT_A, &spec, &["write"], Some(first.id)).await.unwrap();
    assert_eq!(upgraded.id, first.id);
    let mut scopes = upgraded.scopes.clone();
    scopes.sort();
    assert_eq!(scopes, vec!["read", "write"], "union-only growth");

    // The drift backstop: a required permission a VERIFIED grant does
    // not hold is refused at resolution with the reconnect affordance.
    let err =
        resolve_for_worker(&pool, TENANT_A, GrantUser::Author, first.id, "fakeoauth", &["admin".to_string()], &[])
            .await
            .unwrap_err();
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })),
        "{err}"
    );
    resolve_for_worker(&pool, TENANT_A, GrantUser::Author, first.id, "fakeoauth", &["write".to_string()], &[])
        .await
        .expect("a held scope resolves");

    // An upgrade id naming a grant of a DIFFERENT service is a clean
    // miss: the row's app must never be parked into another provider's
    // consent.
    let mut other = spec.clone();
    other.service = "otheroauth".into();
    let err = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: other,
            door: Door::Own,
            registration: Some(oauth_app()),
            permissions: vec!["read".into()],
            project_id: Some(PROJECT_1),
            instance: None,
            upgrade_grant_id: Some(first.id),
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)),
        "{err}"
    );

    // A request carrying a DIFFERENT app than the row's is a
    // contradiction, refused loudly instead of silently picking a side.
    let mut other_app = oauth_app();
    other_app.client_id = "cid-2".into();
    let err = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Own,
            registration: Some(other_app),
            permissions: vec!["read".into()],
            project_id: Some(PROJECT_1),
            instance: None,
            upgrade_grant_id: Some(first.id),
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(m)) if m.contains("different app")),
        "{err}"
    );
}

#[weft_db_macros::test]
async fn refresh_is_lazy_single_flight_and_writes_back_rotated_tokens(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    // The consent's token carries a refresh token and is made due at
    // once, so the very next resolution must refresh.
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    let consent_tokens = fake.token_requests.load(Ordering::SeqCst);

    // The refresh rotates both tokens (Xero-class single-use refresh).
    *fake.next_token.lock().unwrap() = "tok-fresh".into();
    *fake.next_refresh.lock().unwrap() = Some("refresh-2".into());
    *fake.expires_in.lock().unwrap() = Some(3600);

    // Parallel resolutions: the renewal's claim on the row is the
    // single-flight, so the provider sees EXACTLY ONE refresh request.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let id = grant.id;
        handles.push(tokio::spawn(async move {
            resolve_for_worker(&pool, TENANT_A, GrantUser::Author, id, "fakeoauth", &[], &[]).await
        }));
    }
    for h in handles {
        let resolved = h.await.unwrap().expect("refresh succeeds");
        assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-fresh"));
    }
    assert_eq!(
        fake.token_requests.load(Ordering::SeqCst) - consent_tokens,
        1,
        "one refresh for four parallel resolutions"
    );
    let refresh_call = fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .rfind(|c| c.contains("grant_type=refresh_token"))
        .cloned()
        .unwrap();
    assert!(refresh_call.contains("refresh_token=refresh-1"), "{refresh_call}");
}

/// A pool of one connection on `pool`'s database, whose wait for it
/// gives up after `wait`: what shows a connection held where it must not
/// be.
fn one_connection(pool: &Pool, wait: std::time::Duration) -> Pool {
    Pool::open(pool.config().clone(), 1, wait, db::Waits::Expected)
}

/// A consent whose token is due a renewal now, and a provider whose next
/// answer rotates both tokens: what the renewal tests start from.
async fn due_for_renewal(pool: &Pool, fake: &FakeProvider, base: &str) -> GrantSummary {
    let spec = oauth_spec(base, "coexisting");
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(pool, grant.id).await;
    *fake.next_token.lock().unwrap() = "tok-fresh".into();
    *fake.next_refresh.lock().unwrap() = Some("refresh-2".into());
    *fake.expires_in.lock().unwrap() = Some(3600);
    grant
}

/// A renewal holds no database connection while the provider answers:
/// on a pool of one connection, other work goes through while the
/// renewal waits on the provider, and so do the resolutions waiting on
/// that same renewal. The provider still sees one renewal.
#[weft_db_macros::test]
async fn a_renewal_holds_no_database_connection_while_the_provider_answers(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let grant = due_for_renewal(&pool, &fake, &base).await;
    let consent_tokens = fake.token_requests.load(Ordering::SeqCst);
    let one = one_connection(&pool, std::time::Duration::from_secs(2));

    let gate = fake.hold_tokens().await;
    let resolves: Vec<_> = (0..3)
        .map(|_| {
            let one = one.clone();
            let id = grant.id;
            tokio::spawn(async move { resolve_for_worker(&one, TENANT_A, GrantUser::Author, id, "fakeoauth", &[], &[]).await })
        })
        .collect();
    fake.token_arrived.acquire().await.unwrap().forget();
    let grants: i64 =
        one.scalar("SELECT count(*) FROM access_grant", &[]).await.expect("the one connection is free while the provider answers");
    assert_eq!(grants, 1);
    drop(gate);

    for resolve in resolves {
        let resolved = resolve.await.unwrap().expect("the renewal lands");
        assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-fresh"));
    }
    assert_eq!(fake.token_requests.load(Ordering::SeqCst) - consent_tokens, 1, "one renewal for three resolutions");
}

/// A renewal whose answer arrives while no database connection can be
/// had waits for one and writes it: the provider may have retired the
/// refresh token the row held, so the rotated one is never dropped, and
/// the next renewal sends it.
#[weft_db_macros::test]
async fn a_renewed_token_is_written_once_a_connection_can_be_had(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let grant = due_for_renewal(&pool, &fake, &base).await;
    let one = one_connection(&pool, std::time::Duration::from_millis(100));

    let gate = fake.hold_tokens().await;
    let resolve = {
        let one = one.clone();
        tokio::spawn(async move { resolve_for_worker(&one, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]).await })
    };
    fake.token_arrived.acquire().await.unwrap().forget();
    // The answer arrives while the one connection is taken, for several
    // of the pool's waits.
    let taken = one.get().await.unwrap();
    drop(gate);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(taken);
    let resolved = resolve.await.unwrap().expect("the renewal is written once the connection is free");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-fresh"));

    expire_now(&pool, grant.id).await;
    resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]).await.unwrap();
    let renewal = fake.calls.lock().unwrap().iter().rfind(|c| c.contains("grant_type=refresh_token")).cloned().unwrap();
    assert!(renewal.contains("refresh_token=refresh-2"), "{renewal}");
}

/// Two upgrades of one exclusive connection landing at once both keep
/// the permission each added: the row's permissions are widened in the
/// statement that writes the sign-in, never from a read before it.
#[weft_db_macros::test]
async fn two_upgrades_landing_at_once_both_keep_their_permissions(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "exclusive");
    let first = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let write = begin_consent(&pool, TENANT_A, &spec, &["write"], Some(first.id)).await.unwrap();
    let admin = begin_consent(&pool, TENANT_A, &spec, &["admin"], Some(first.id)).await.unwrap();

    let gate = fake.hold_tokens().await;
    let completes: Vec<_> = [write, admin]
        .into_iter()
        .map(|state| {
            let pool = pool.clone();
            tokio::spawn(async move { complete_oauth(&pool, &state, "the-code").await })
        })
        .collect();
    fake.token_arrived.acquire_many(2).await.unwrap().forget();
    drop(gate);
    for complete in completes {
        assert_eq!(complete.await.unwrap().unwrap().grant.id, first.id);
    }

    let row = list_grants(&pool, TENANT_A, Some("fakeoauth"), weft_access_store::GrantOwnerScope::Author)
        .await
        .unwrap()
        .into_iter()
        .find(|g| g.id == first.id)
        .unwrap();
    for scope in ["read", "write", "admin"] {
        assert!(row.scopes.iter().any(|s| s == scope), "{scope} kept: {:?}", row.scopes);
    }
}

/// Two first sign-ins to one exclusive account landing at once leave one
/// connection: the second lands on the row the first made, the way a
/// reconnect does (same id, counted as a reconnect, both permissions
/// kept), never beside it.
#[weft_db_macros::test]
async fn two_first_sign_ins_landing_at_once_make_one_connection(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "exclusive");
    let read = begin_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let write = begin_consent(&pool, TENANT_A, &spec, &["write"], None).await.unwrap();

    let gate = fake.hold_tokens().await;
    let completes: Vec<_> = [read, write]
        .into_iter()
        .map(|state| {
            let pool = pool.clone();
            tokio::spawn(async move { complete_oauth(&pool, &state, "the-code").await })
        })
        .collect();
    fake.token_arrived.acquire_many(2).await.unwrap().forget();
    drop(gate);
    let mut ids = Vec::new();
    for complete in completes {
        ids.push(complete.await.unwrap().unwrap().grant.id);
    }
    assert_eq!(ids[0], ids[1], "both sign-ins answer the one connection");

    let rows = list_grants(&pool, TENANT_A, Some("fakeoauth"), weft_access_store::GrantOwnerScope::Author).await.unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    for scope in ["read", "write"] {
        assert!(rows[0].scopes.iter().any(|s| s == scope), "{scope} kept: {:?}", rows[0].scopes);
    }
    let reconnects: i64 =
        pool.scalar("SELECT reconnects FROM access_grant WHERE id = $1", &[&ids[0]]).await.unwrap();
    assert_eq!(reconnects, 1, "the second sign-in counts as a reconnect of the first");
}

/// A declared renewal call (Meta's long-lived-token exchange shape)
/// replaces the standard refresh POST entirely: the stale grant is
/// renewed through the recipe's own GET (interpolating the current
/// token and the app's client id), its captures write the fresh
/// token back, and the answer's expires_in sets the next expiry.
#[weft_db_macros::test]
async fn a_declared_renewal_call_replaces_the_standard_refresh(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let mut spec = oauth_spec(&base, "coexisting");
    if let weft_core::access::spec::Acquisition::OAuth2 { refresh, .. } = &mut spec.acquisition {
        *refresh = Some(
            serde_json::from_value(json!({
                "url": format!(
                    "{base}/exchange?grant_type=fb_exchange_token&client_id={{client_id}}\
                     &client_secret={{client_secret}}&fb_exchange_token={{token}}"
                ),
                "captures": [{ "name": "token", "path": "access_token" }]
            }))
            .unwrap(),
        );
    }
    // The consent's token expires immediately and issues NO refresh
    // token: only the declared renewal can save it.
    *fake.next_refresh.lock().unwrap() = None;
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    let consent_tokens = fake.token_requests.load(Ordering::SeqCst);

    *fake.next_token.lock().unwrap() = "tok-long-lived".into();
    *fake.expires_in.lock().unwrap() = Some(3600);
    let resolved = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[])
        .await
        .expect("the declared renewal succeeds");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-long-lived"));

    // The renewal went through /exchange with the OLD token and the
    // app's credentials; the standard token endpoint saw nothing new.
    let exchange = fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .rfind(|c| c.starts_with("/exchange"))
        .cloned()
        .expect("the declared call ran");
    assert!(exchange.contains("fb_exchange_token") && exchange.contains("tok-1"), "{exchange}");
    assert!(exchange.contains("client_id"), "{exchange}");
    assert_eq!(
        fake.token_requests.load(Ordering::SeqCst),
        consent_tokens,
        "the standard refresh never ran"
    );
}

/// A declared renewal call the provider answers busy or down (429, 503)
/// leaves the grant alone and is tried again; one refusing the credential
/// (401) or the request (400) asks for a reconnect, and the grant still
/// renews once the provider is back.
#[weft_db_macros::test]
async fn a_declared_renewal_the_provider_could_not_answer_is_tried_again_not_a_reconnect(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let mut spec = oauth_spec(&base, "coexisting");
    if let weft_core::access::spec::Acquisition::OAuth2 { refresh, .. } = &mut spec.acquisition {
        *refresh = Some(
            serde_json::from_value(json!({
                "url": format!("{base}/exchange?fb_exchange_token={{token}}"),
                "captures": [{ "name": "token", "path": "access_token" }]
            }))
            .unwrap(),
        );
    }
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    let resolve = || resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]);

    for status in [429, 503] {
        *fake.exchange_refusal.lock().unwrap() = Some(status);
        let err = resolve().await.unwrap_err();
        assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{status}: {err:#}");
    }
    for status in [401, 400] {
        *fake.exchange_refusal.lock().unwrap() = Some(status);
        let err = resolve().await.unwrap_err();
        assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })), "{status}: {err:#}");
    }

    *fake.exchange_refusal.lock().unwrap() = None;
    *fake.next_token.lock().unwrap() = "tok-back".into();
    *fake.expires_in.lock().unwrap() = Some(3600);
    let resolved = resolve().await.expect("the renewal succeeds once the provider answers");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-back"));
}

#[weft_db_macros::test]
async fn a_revoked_refresh_fails_loud_with_the_reconnect_affordance(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    // Expiring token, NO refresh token issued: the grant cannot renew.
    *fake.next_refresh.lock().unwrap() = None;
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    let err =
        resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]).await.unwrap_err();
    match err.downcast_ref::<AccessError>() {
        Some(AccessError::NeedsReconnect { reason, .. }) => {
            assert!(reason.contains("reconnect"), "{reason}");
        }
        other => panic!("expected NeedsReconnect, got {other:?} ({err})"),
    }
}

/// A refresh the token endpoint answers busy or down (429, 503) leaves
/// the grant alone and is tried again; one it refuses (400) asks for a
/// reconnect, and the grant still renews once the provider is back.
#[weft_db_macros::test]
async fn a_refresh_the_provider_could_not_answer_is_tried_again_not_a_reconnect(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    let resolve = || resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]);

    for status in [429, 503] {
        *fake.token_refusal.lock().unwrap() = Some(status);
        let err = resolve().await.unwrap_err();
        assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{status}: {err:#}");
    }
    *fake.token_refusal.lock().unwrap() = Some(400);
    let err = resolve().await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })), "{err:#}");

    *fake.token_refusal.lock().unwrap() = None;
    *fake.next_token.lock().unwrap() = "tok-back".into();
    *fake.expires_in.lock().unwrap() = Some(3600);
    let resolved = resolve().await.expect("the refresh succeeds once the provider answers");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-back"));
}

#[weft_db_macros::test]
async fn lookups_run_authed_substitute_and_paginate(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap();

    let lookup_spec: weft_core::node::Lookup = serde_json::from_value(json!({
        "get": format!("{base}/channels?q={{query}}&team={{team}}"),
        "items": "channels", "label": "name", "value": "id",
        "page": { "cursorParam": "cursor", "cursorPath": "meta.next" }
    }))
    .unwrap();
    let parents: BTreeMap<String, String> =
        [("team".to_string(), "T1".to_string())].into_iter().collect();
    let resolved =
        resolve_for_worker(&pool, TENANT_A, GrantUser::Author, done.grant.id, "fakestatic", &[], &[]).await.unwrap();
    let url = weft_access_store::lookup_url(&lookup_spec, "gen", &parents).unwrap();
    let page = lookup(Some(&resolved), &lookup_spec, None, &url).await.unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, "C1");
    assert_eq!(page.items[0].label, "one");
    assert_eq!(page.next_cursor.as_deref(), Some("next-1"));

    let page2 =
        lookup(Some(&resolved), &lookup_spec, page.next_cursor.as_deref(), &url).await.unwrap();
    assert_eq!(page2.items.len(), 1);
    assert_eq!(page2.next_cursor, None, "an empty cursor ends the pages");

    // Substitution really happened, and the call was authenticated.
    let calls = fake.calls.lock().unwrap().clone();
    let ch = calls.iter().find(|c| c.starts_with("/channels")).unwrap();
    assert!(ch.contains("authed"), "a signed lookup must carry auth: {ch}");
    assert!(ch.contains("\"q\": \"gen\"") && ch.contains("\"team\": \"T1\""), "{ch}");

    // A `public` lookup runs with no access at all: the same URL
    // machinery, a BARE client (the whole point: assert the call
    // arrived with no auth header).
    let public_spec: weft_core::node::Lookup = serde_json::from_value(json!({
        "get": format!("{base}/channels?q={{query}}"),
        "items": "channels", "label": "name", "value": "id",
        "public": true
    }))
    .unwrap();
    let url = weft_access_store::lookup_url(&public_spec, "pub", &BTreeMap::new()).unwrap();
    let page = lookup(None, &public_spec, None, &url).await.unwrap();
    assert_eq!(page.items.len(), 2);
    let calls = fake.calls.lock().unwrap().clone();
    let public_call = calls.iter().find(|c| c.contains("\"q\": \"pub\"")).unwrap();
    assert!(public_call.contains("bare"), "a public lookup must not sign: {public_call}");

    // An unknown parent placeholder is loud, never a literal brace
    // sent upstream.
    let bad: weft_core::node::Lookup = serde_json::from_value(json!({
        "get": format!("{base}/channels?x={{nope}}"),
        "items": "channels", "label": "name", "value": "id"
    }))
    .unwrap();
    let err = weft_access_store::lookup_url(&bad, "", &BTreeMap::new()).unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");
}

/// The SHARED door of a key service stores a runtime-owned row:
/// nothing pasted, nothing to validate, resolution hands back empty
/// values (the broker fills them from its credential source) and the
/// `ours` owner every cost record will carry.
#[weft_db_macros::test]
async fn a_shared_key_connect_stores_a_runtime_owned_row(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let mut spec = static_spec(&base);
    spec.doors = vec![Door::Shared, Door::Own];

    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: spec.clone(),
            door: Door::Shared,
            registration: None,
            values: BTreeMap::new(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("the shared door needs no pasted values");
    assert_eq!(done.grant.owner, CredentialOwner::Platform);
    assert_eq!(done.grant.door, Door::Shared);
    assert!(fake.calls.lock().unwrap().is_empty(), "nothing to validate = no provider call");

    let resolved =
        resolve_for_worker(&pool, TENANT_A, GrantUser::Author, done.grant.id, "fakestatic", &[], &[]).await.unwrap();
    assert_eq!(resolved.owner, CredentialOwner::Platform);
    assert!(resolved.values.is_empty(), "the runtime fills the values per call");
    assert_eq!(resolved.auth.len(), 1, "the declared auth steps carry over");

    // A door the spec does not offer is refused.
    let err = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Shared,
            registration: None,
            values: BTreeMap::new(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("door"), "{err}");
}

/// A CLAIMED (unverified) permission set never blocks resolution: a
/// pasted credential on a service that reports nothing must resolve
/// whatever a consumer requires (nobody actually knows what it holds).
/// A VERIFIED set short of what the consumer requires is refused.
#[weft_db_macros::test]
async fn a_claimed_shortfall_is_let_through_at_resolution(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap();
    assert!(!done.grant.permissions_verified);
    resolve_for_worker(
        &pool,
        TENANT_A, GrantUser::Author,
        done.grant.id,
        "fakestatic",
        &["anything".to_string()],
        &[],
    )
    .await
    .expect("an unknown granted set never blocks");

    // Once the provider has reported what the connection holds, a
    // permission it lacks is refused at resolution, naming the permission
    // and the reconnect that adds it.
    pool.execute(
        "UPDATE access_grant SET permissions_verified = TRUE, granted_scopes = '[\"base.read\"]'::jsonb WHERE id = $1",
        &[&done.grant.id],
    )
    .await
    .unwrap();
    let err = resolve_for_worker(
        &pool,
        TENANT_A, GrantUser::Author,
        done.grant.id,
        "fakestatic",
        &["chat:write".to_string()],
        &[],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("'chat:write'") && err.to_string().contains("reconnect"), "{err}");
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })),
        "{err}"
    );
}


/// A `granted` resource-source read runs the SAME walled read as
/// every resolution (tenant wall, service check) and hands out only
/// the id/label pairs of the named capture, off the FULL stored map
/// (the worker-handoff filter would drop a captured list).
#[weft_db_macros::test]
async fn granted_items_read_the_stored_list_behind_the_wall(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let mut spec = static_spec(&base);
    if let weft_core::access::spec::Acquisition::Static { fields } = &mut spec.acquisition {
        fields.push(serde_json::from_value(json!({ "name": "channels", "optional": true })).unwrap());
    }
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec,
            door: Door::Own,
            registration: None,
            values: [
                ("token".to_string(), "tok-abc".to_string()),
                (
                    "channels".to_string(),
                    r#"[{"id":"C1","name":"one"},{"id":"C2","name":"two"}]"#.to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap();
    let id = done.grant.id;

    let items =
        weft_access_store::granted_items(&pool, TENANT_A, GrantUser::Author, id, "fakestatic", "channels", "name", "id")
            .await
            .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!((items[0].id.as_str(), items[0].label.as_str()), ("C1", "one"));

    // A name the connection recorded nothing under is an empty list
    // (the editor falls through), not an error.
    assert!(
        weft_access_store::granted_items(&pool, TENANT_A, GrantUser::Author, id, "fakestatic", "gone", "name", "id")
            .await
            .unwrap()
            .is_empty()
    );

    // The tenant wall and the service check hold like every read.
    let err = weft_access_store::granted_items(
        &pool, TENANT_B, GrantUser::Author, id, "fakestatic", "channels", "name", "id",
    )
    .await
    .unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");
    let err = weft_access_store::granted_items(
        &pool, TENANT_A, GrantUser::Author, id, "slack", "channels", "name", "id",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("wire the right service"), "{err}");
}

/// The OAuth fixture service WITH an events topic: a watch-class
/// webhook recipe (token-echo verification, subscription routing,
/// expiring + renewable), whose account identity is the `user` value
/// the test call captures.
fn events_spec(base: &str) -> AccessSpec {
    let mut spec = oauth_spec(base, "coexisting");
    let events: std::collections::BTreeMap<String, weft_core::access::events::EventsSpec> =
        serde_json::from_value(json!({
            "things": {
                "fields": { "kind": "kind", "body": "body" },
                "account": { "value": "user", "path": "account" },
                "webhook": {
                    "verify": { "kind": "token_echo" },
                    "routeBy": "subscription",
                    "idPath": "header:X-Chan-Id",
                    "tokenPath": "header:X-Chan-Token",
                    "subscribe": {
                        "subscribe": {
                            "url": format!("{base}/watch/{{target}}"),
                            "method": "POST",
                            "body": { "id": "{subscriptionId}",
                                      "address": "{receiverUrl}",
                                      "token": "{subscriptionToken}" },
                            "auth": [{ "kind": "header", "name": "Authorization",
                                       "value": "Bearer {token}" }],
                            "captures": [
                                { "name": "expiresAt", "path": "expiration" },
                                { "name": "resourceId", "path": "resourceId" }
                            ]
                        },
                        "unsubscribe": {
                            "url": format!("{base}/stop"),
                            "method": "POST",
                            "body": { "id": "{subscriptionId}",
                                      "resourceId": "{resourceId}" },
                            "auth": [{ "kind": "header", "name": "Authorization",
                                       "value": "Bearer {token}" }]
                        },
                        "renewMarginSecs": 60
                    }
                }
            }
        }))
        .unwrap();
    spec.events = events;
    spec.validate().expect("the events fixture validates");
    spec
}

/// A connect on an events-declaring service records the provider
/// account on its indexed column, records the service's recipe for
/// the receiver, and the inbound-event lookup finds the row ONLY
/// through the app that made it (the client_id containment pin).
#[weft_db_macros::test]
async fn a_connect_records_the_provider_account_for_event_routing(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = events_spec(&base);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();

    // The recipe is on file for the receiver (recorded by the
    // connect), under the hash the grant row carries.
    let recipes = weft_access_store::events_recipes_of(&pool, "fakeoauth").await.unwrap();
    assert_eq!(recipes.len(), 1, "one recipe recorded");
    assert!(recipes[0].events.contains_key("things"));
    let hash = weft_access_store::events_recipe_hash(&spec.events).unwrap();
    assert_eq!(recipes[0].recipe_hash, hash);

    // The account column: the `user` value the test call captured.
    let targets = weft_access_store::connections_for_event(
        &pool, "fakeoauth", "quentin", &["cid".to_string()], &hash,
    )
    .await
    .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].id, grant.id);
    assert_eq!(targets[0].tenant_id, TENANT_A);

    // The containment pin: a DIFFERENT app's client id matches
    // nothing, even for the same account (an event verified through
    // one app must never feed another app's connections).
    let foreign = weft_access_store::connections_for_event(
        &pool, "fakeoauth", "quentin", &["someone-elses-cid".to_string()], &hash,
    )
    .await
    .unwrap();
    assert!(foreign.is_empty());

    // And an unknown account matches nothing.
    let unknown = weft_access_store::connections_for_event(
        &pool, "fakeoauth", "nobody", &["cid".to_string()], &hash,
    )
    .await
    .unwrap();
    assert!(unknown.is_empty());
}

/// Recipe-hash scoping end to end at the store: two DIFFERENT
/// recipes for one service stand side by side in the recipe table,
/// and a push that verified under one recipe routes ONLY to the
/// connection whose snapshot declared that exact recipe. A recipe
/// one spec records can never answer for another spec's connections.
#[weft_db_macros::test]
async fn a_push_routes_only_to_connections_carrying_the_verifying_recipe(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;

    // Recipe A and recipe B: the same topic, a different field table
    // (any content difference re-hashes).
    let spec_a = events_spec(&base);
    let mut spec_b = events_spec(&base);
    spec_b
        .events
        .get_mut("things")
        .unwrap()
        .fields
        .insert("extra".to_string(), "extra".to_string());
    let hash_a = weft_access_store::events_recipe_hash(&spec_a.events).unwrap();
    let hash_b = weft_access_store::events_recipe_hash(&spec_b.events).unwrap();
    assert_ne!(hash_a, hash_b);

    // One consent under each recipe (the SAME provider account and
    // the same app: only the recipe differs).
    let grant_a = full_consent(&pool, TENANT_A, &spec_a, &["read"], None).await.unwrap();
    let grant_b = full_consent(&pool, TENANT_B, &spec_b, &["read"], None).await.unwrap();

    // Both recipes are on file, side by side.
    let recipes = weft_access_store::events_recipes_of(&pool, "fakeoauth").await.unwrap();
    let mut hashes: Vec<&str> = recipes.iter().map(|r| r.recipe_hash.as_str()).collect();
    hashes.sort();
    let mut expected = vec![hash_a.as_str(), hash_b.as_str()];
    expected.sort();
    assert_eq!(hashes, expected);

    // A push that verified under recipe B reaches ONLY the grant
    // carrying hash B, never recipe A's connection to the same
    // account through the same app.
    let targets = weft_access_store::connections_for_event(
        &pool, "fakeoauth", "quentin", &["cid".to_string()], &hash_b,
    )
    .await
    .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].id, grant_b.id);
    let targets = weft_access_store::connections_for_event(
        &pool, "fakeoauth", "quentin", &["cid".to_string()], &hash_a,
    )
    .await
    .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].id, grant_a.id);
}

/// The subscription lifecycle against the fake provider: the
/// subscribe call goes out with the minted id + token + receiver
/// address, the row records what the provider answered, a healthy
/// subscription is NOT re-subscribed, and dropping stops the channel
/// at the provider and forgets the row.
#[weft_db_macros::test]
async fn subscriptions_subscribe_once_and_stop_at_the_provider(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = events_spec(&base);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();

    // The event-source resolve: recipe + the values its calls name.
    let source =
        weft_access_store::resolve_event_source(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[])
            .await
            .unwrap();
    assert!(source.events.contains_key("things"));
    assert_eq!(source.provider_account.as_deref(), Some("quentin"));

    let req = weft_access_store::EnsureSubscription {
        tenant: TENANT_A.into(),
        service: "fakeoauth".into(),
        topic: "things".into(),
        access_id: grant.id,
        for_instance: None,
        signal_token: "sig-1".into(),
        params: [("target".to_string(), "file-9".to_string())].into_iter().collect(),
    };
    // The address is read only right before a subscribe call.
    let address_reads = std::sync::atomic::AtomicUsize::new(0);
    let receiver = || async {
        address_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        anyhow::Ok(Some("https://public.example/events/fakeoauth/things".to_string()))
    };
    let ensured = weft_access_store::ensure_subscription(&pool, &pool, &req, receiver).await.unwrap();
    assert_eq!(address_reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(ensured.expires_at.is_some(), "the recipe declares an expiring channel");

    // The provider saw ONE subscribe, at the templated target, with
    // the minted values and the receiver address in the body.
    let watch_calls: Vec<String> = fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.starts_with("/watch/"))
        .cloned()
        .collect();
    assert_eq!(watch_calls.len(), 1, "{watch_calls:?}");
    assert!(watch_calls[0].starts_with("/watch/file-9 "), "{}", watch_calls[0]);
    assert!(watch_calls[0].contains("https://public.example/events/fakeoauth/things"));

    // The row: findable by the minted id (what a push presents), and
    // carrying the provider's captures.
    let row = pool.one::<(String,)>("SELECT id FROM signal_subscription WHERE signal_token = 'sig-1'", &[]).await.unwrap();
    let sub = weft_access_store::subscription_by_id(&pool, &row.0, "fakeoauth", "things")
        .await
        .unwrap()
        .expect("the minted id resolves");
    assert!(
        weft_access_store::subscription_by_id(&pool, &row.0, "fakeoauth", "other")
            .await
            .unwrap()
            .is_none(),
        "a replay at another topic's endpoint finds nothing"
    );
    assert_eq!(sub.tenant_id, TENANT_A);
    assert_eq!(sub.captures.get("resourceId").map(String::as_str), Some("res-file-9"));
    assert!(watch_calls[0].contains(&sub.token), "the minted token traveled to the provider");

    // A healthy subscription is idempotent: no second provider call, and
    // the address is not even read.
    weft_access_store::ensure_subscription(&pool, &pool, &req, receiver).await.unwrap();
    let watch_count =
        fake.calls.lock().unwrap().iter().filter(|c| c.starts_with("/watch/")).count();
    assert_eq!(watch_count, 1, "far from expiry, ensure re-subscribes nothing");
    assert_eq!(address_reads.load(std::sync::atomic::Ordering::SeqCst), 1, "no subscribe, no address read");

    // Dropping stops the channel at the provider (with the captured
    // resource id) and forgets the row.
    let dropped =
        weft_access_store::drop_subscriptions_for_signal(&pool, &pool, TENANT_A, "sig-1").await.unwrap();
    assert_eq!(dropped, 1);
    let stops: Vec<String> = fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.starts_with("/stop"))
        .cloned()
        .collect();
    assert_eq!(stops.len(), 1);
    assert!(stops[0].contains("res-file-9"), "{}", stops[0]);
    assert!(
        weft_access_store::subscription_by_id(&pool, &row.0, "fakeoauth", "things")
            .await
            .unwrap()
            .is_none(),
        "the row died with the channel"
    );
}

/// A topic that must TELL the provider where to send, on a weft with
/// no internet-reachable address, refuses with the teaching error
/// naming the fix; it never subscribes a channel that could not
/// deliver.
#[weft_db_macros::test]
async fn a_subscribe_topic_without_a_public_address_teaches_the_fix(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = events_spec(&base);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let err = weft_access_store::ensure_subscription(
        &pool,
        &pool,
        &weft_access_store::EnsureSubscription {
            tenant: TENANT_A.into(),
            service: "fakeoauth".into(),
            topic: "things".into(),
            access_id: grant.id,
            for_instance: None,
            signal_token: "sig-x".into(),
            params: [("target".to_string(), "f".to_string())].into_iter().collect(),
        },
        || async { anyhow::Ok(None) },
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("--public-url"), "{msg}");
    assert!(msg.contains("connections/events.html"), "{msg}");
    assert!(
        fake.calls.lock().unwrap().iter().all(|c| !c.starts_with("/watch/")),
        "no provider channel was opened"
    );
}

/// Whether a subscription could be made is told without making it: no
/// internet-reachable address teaches the same fix the subscribe would,
/// params naming a value the subscribe mints are refused as the subscribe
/// refuses them, and an install that could subscribe is let through. No call
/// reaches the provider and no row is written either way.
#[weft_db_macros::test]
async fn a_subscription_is_checked_without_being_made(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = events_spec(&base);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let ask = |params: &[(&str, &str)]| weft_access_store::EnsureSubscription {
        tenant: TENANT_A.into(),
        service: "fakeoauth".into(),
        topic: "things".into(),
        access_id: grant.id,
        for_instance: None,
        signal_token: "sig-x".into(),
        params: params.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    };
    let unreachable = weft_access_store::check_subscription(&pool, &ask(&[("target", "f")]), || async { anyhow::Ok(None) })
        .await
        .unwrap_err()
        .to_string();
    assert!(unreachable.contains("--public-url"), "{unreachable}");
    let replacing = weft_access_store::check_subscription(&pool, &ask(&[("subscriptionId", "x")]), || async { anyhow::Ok(Some("https://weft.test".into())) })
        .await
        .unwrap_err()
        .to_string();
    assert!(replacing.contains("may not replace the connection's stored value 'subscriptionId'"), "{replacing}");
    weft_access_store::check_subscription(&pool, &ask(&[("target", "f")]), || async { anyhow::Ok(Some("https://weft.test".into())) })
        .await
        .expect("an install the internet reaches could subscribe");
    assert!(fake.calls.lock().unwrap().iter().all(|c| !c.starts_with("/watch/")), "no provider channel was opened");
    let rows: i64 = pool.scalar("SELECT count(*) FROM signal_subscription", &[]).await.unwrap();
    assert_eq!(rows, 0, "nothing is written");
}

/// A connection to a service with no events section still resolves
/// for the listener (a held raw pipe needs only the stored values);
/// the recipe map simply comes back empty. A subscription needing a
/// topic is refused by its own topic lookup, not here.
#[weft_db_macros::test]
async fn an_eventless_service_resolves_with_an_empty_recipe_map(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap();
    let source =
        weft_access_store::resolve_event_source(&pool, TENANT_A, GrantUser::Author, done.grant.id, "fakestatic", &[])
            .await
            .expect("an eventless service resolves for value-only listening");
    assert!(source.events.is_empty(), "no recipes to serve");
    assert_eq!(source.values.get("token").map(String::as_str), Some("tok-abc"));
}


/// The shared-door containment for event serving: whatever the
/// operator wrote beside the SHARED app (even a value a socket recipe
/// names, like an app-level token) never reaches a shared-door
/// connection's event serving. Only an OWN-door grant's app values do
/// (that app is the user's). Without this, an app-level token in the
/// operator's apps file would let any tenant hold the shared app's
/// APP-WIDE socket and hear every other install's events.
#[weft_db_macros::test]
async fn a_shared_door_grant_never_serves_events_with_the_operators_app_values(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;

    // The events fixture, with a socket recipe whose mint call names
    // {appToken}: exactly the value an operator might (wrongly or
    // for their own use) write beside the shared app.
    let mut spec = events_spec(&base);
    let socket_topic: weft_core::access::events::EventsSpec = serde_json::from_value(json!({
        "fields": { "kind": "kind" },
        "account": { "value": "user", "path": "account" },
        "socket": {
            "connect": {
                "url": format!("{base}/token"),
                "method": "POST",
                "auth": [{ "kind": "header", "name": "Authorization",
                           "value": "Bearer {appToken}" }],
                "captures": [{ "name": "url", "path": "url" }]
            }
        }
    }))
    .unwrap();
    spec.events.insert("live".into(), socket_topic);
    spec.validate().expect("the fixture validates");

    let mut registration = oauth_app();
    registration
        .extra
        .insert("appToken".to_string(), "xapp-the-operators".to_string());

    // Two grants to the same app snapshot: one through each door.
    for (id, door) in [(uuid::Uuid::new_v4(), "shared"), (uuid::Uuid::new_v4(), "own")] {
        pool.execute(
            "INSERT INTO access_grant
               (id, tenant_id, service, registration_sealed, spec, values_sealed,
                granted_scopes, door, identity, spec_version, exclusive)
             VALUES ($1, $2, 'fakeoauth', $3, $4, $5, '[]', $6, 'x', $7, $8)",
            &[
                &id,
                &TENANT_A,
                &weft_access_store::seal_json(&serde_json::to_value(&registration).unwrap()).unwrap(),
                &JsonText(&spec),
                &weft_access_store::seal_json(&json!({ "token": "tok-1" })).unwrap(),
                &door,
                &weft_core::AccessSpec::STORED.version(),
                &(spec.grants == weft_core::access::spec::GrantCoexistence::Exclusive),
            ],
        )
        .await
        .unwrap();
        let source =
            weft_access_store::resolve_event_source(&pool, TENANT_A, GrantUser::Author, id, "fakeoauth", &[])
                .await
                .unwrap();
        match door {
            "shared" => assert!(
                !source.recipe_values.contains_key("appToken"),
                "the operator's app token must never serve a shared-door connection"
            ),
            _ => assert_eq!(
                source.recipe_values.get("appToken").map(String::as_str),
                Some("xapp-the-operators"),
                "an own-door grant's app values are the user's own and do serve"
            ),
        }
    }
}


/// The shared-app wall, stated as the property it must have: a user
/// who connects through the OPERATOR's app gets a connection that
/// acts as THEIR account and nothing more. The app's own secret
/// never reaches them, so they cannot act as the app itself; a user
/// who registered their OWN app does get its material, because it is
/// theirs.
///
/// Pins the whole chain: what a shared connect stores, what a
/// resolution hands over, and the door split on app values.
#[weft_db_macros::test]
async fn a_shared_app_connection_never_carries_the_operators_app_secret(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    *fake.next_refresh.lock().unwrap() = Some("rt-the-users".into());
    *fake.expires_in.lock().unwrap() = Some(3600);

    // A shared-door consent through the operator's app.
    let started = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Shared,
            registration: Some(oauth_app()),
            permissions: vec!["read".into()],
            project_id: Some(PROJECT_1),
            instance: None,
            upgrade_grant_id: None,
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await
    .unwrap();
    let done = complete_oauth(&pool, &started.state, "the-code").await.unwrap();

    // What the row STORES: the provider's answer only. The app's
    // client secret was used to exchange the code and never landed
    // in the connection's values.
    let values_sealed: String =
        pool.scalar("SELECT values_sealed FROM access_grant WHERE id = $1", &[&done.grant.id]).await.unwrap();
    // The at-rest form is ciphertext, not readable JSON.
    assert!(
        serde_json::from_str::<serde_json::Value>(&values_sealed).is_err(),
        "stored values are sealed: {values_sealed}"
    );
    let values = weft_access_store::open_json(&values_sealed).unwrap();
    let stored = values.as_object().unwrap();
    assert!(stored.contains_key("token"), "the user's own token: {stored:?}");
    assert!(
        !stored.values().any(|v| v.as_str() == Some("sec")),
        "the operator's app secret is never a stored value: {stored:?}"
    );

    // What a RESOLUTION hands over: the user's token, never the
    // store's keep-alive material, never the app's secret.
    let resolved =
        resolve_for_worker(&pool, TENANT_A, GrantUser::Author, done.grant.id, "fakeoauth", &[], &[]).await.unwrap();
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-1"));
    assert!(
        !resolved.values.contains_key("refresh_token"),
        "the renewal token stays in the store: {:?}",
        resolved.values.keys().collect::<Vec<_>>()
    );
    assert!(
        !resolved.values.values().any(|v| v == "sec"),
        "the operator's app secret never reaches a worker"
    );
    // The public client id may travel (it is not a secret, and picker
    // widgets need it), but only as the declared field.
    assert_eq!(resolved.app_client_id.as_deref(), Some("cid"));
}

/// The capability + required-value pair, on a service whose optional
/// fields decide what a connection can do (the mail shape): a connect
/// filling one group succeeds, one filling half a group is refused
/// naming the fix, and a consumer needing the OTHER group's values is
/// refused at resolution naming the missing value.
#[weft_db_macros::test]
async fn optional_field_groups_gate_what_a_connection_can_do(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec: AccessSpec = serde_json::from_value(json!({
        "service": "fakemail",
        "acquisition": { "kind": "static", "fields": [
            { "name": "token" },
            { "name": "in_host", "optional": true },
            { "name": "in_port", "optional": true },
            { "name": "out_host", "optional": true },
            { "name": "out_port", "optional": true }
        ]},
        "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }],
        "test": { "url": format!("{base}/whoami"), "captures": [{ "name": "user", "path": "user" }] },
        "identity": "{user}",
        "capabilities": [
            { "label": "receive", "fields": ["in_host", "in_port"] },
            { "label": "send", "fields": ["out_host", "out_port"] }
        ]
    }))
    .unwrap();
    let connect = |values: Vec<(&'static str, &'static str)>| {
        let spec = spec.clone();
        let pool = pool.clone();
        async move {
            connect_direct(
                &pool,
                TENANT_A,
                ConnectDirect {
                    paste: false,
                    spec: spec.clone(),
                    door: Door::Own,
                    registration: None,
                    values: values
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    label: None,
                    permissions: Vec::new(),
                    project_id: None,
                    instance: None,
                },
            )
            .await
        }
    };

    // Half a group names the capability and the missing field.
    let err = connect(vec![("token", "tok-m"), ("in_host", "mail.example.com")])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("receive") && err.to_string().contains("in_port"),
        "{err}"
    );

    // No group at all: the connection could do nothing.
    let err = connect(vec![("token", "tok-m")]).await.unwrap_err();
    assert!(err.to_string().contains("receive") && err.to_string().contains("send"), "{err}");

    // One complete group is a valid connection.
    let done = connect(vec![("token", "tok-m"), ("in_host", "mail.example.com"), ("in_port", "993")])
        .await
        .expect("one complete capability is enough");

    // A consumer needing that group's values resolves.
    resolve_for_worker(
        &pool,
        TENANT_A, GrantUser::Author,
        done.grant.id,
        "fakemail",
        &[],
        &["in_host".to_string(), "in_port".to_string()],
    )
    .await
    .expect("the half this connection has");

    // A consumer needing the OTHER group is refused, naming the value
    // to add. This is the "trigger wired to a send-only mailbox" case.
    let err = resolve_for_worker(
        &pool,
        TENANT_A, GrantUser::Author,
        done.grant.id,
        "fakemail",
        &[],
        &["out_host".to_string()],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("needs the connection's 'out_host'"), "{err}");
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })),
        "{err}"
    );
    // A trigger arming on the same connection (the listener's resolve) is
    // refused the same way, which is what an activation reports.
    let err = weft_access_store::resolve_event_source(
        &pool,
        TENANT_A,
        GrantUser::Author,
        done.grant.id,
        "fakemail",
        &["out_host".to_string()],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("needs the connection's 'out_host'"), "{err}");
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })),
        "{err}"
    );

    // The names of what it stores ride the summary (never the values),
    // which is what the editor's live check reads.
    let listed = weft_access_store::list_grants(&pool, TENANT_A, Some("fakemail"), weft_access_store::GrantOwnerScope::Author).await.unwrap();
    let names = &listed[0].value_names;
    assert!(names.contains(&"in_host".to_string()));
    assert!(!names.contains(&"out_host".to_string()), "a blank optional stores nothing");
}

/// A finished pick lands the session's parked `grants` on the grant
/// row: choosing a resource through the provider's chooser GRANTS the
/// declared permissions, so they union into `granted_scopes` (no
/// duplicates) with `permissions_verified` untouched, and the editor's
/// shortfall check (which reads the summary's scopes) sees them.
#[weft_db_macros::test]
async fn a_finished_pick_lands_its_grants_on_the_row(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;

    // The static fixture plus a permission catalogue, so the connect
    // can start with one claimed permission for the union to dedup.
    let mut spec = static_spec(&base);
    spec.permissions = serde_json::from_value(json!([
        { "id": "base.read", "label": "Read", "description": "Read things." },
        { "id": "drive.file", "label": "Picked files", "description": "Files picked." }
    ]))
    .unwrap();
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec,
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: vec!["base.read".into()],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("the fixture connects");
    let grant = done.grant;
    assert!(!grant.permissions_verified, "a claimed connect stays claimed");

    let state = weft_access_store::begin_picker(
        &pool,
        TENANT_A,
        weft_access_store::BeginPicker {
            access_id: grant.id,
            for_instance: None,
            service: "fakestatic".into(),
            script: "https://chooser.example.com/api.js".into(),
            code: "weft.done({id: 'x', label: 'X'})".into(),
            mime_types: Vec::new(),
            // `base.read` is already held: the union must not duplicate it.
            grants: vec!["drive.file".into(), "base.read".into()],
        },
    )
    .await
    .expect("the picker session parks");

    weft_access_store::finish_picker(
        &pool,
        &state,
        json!({ "picked": { "id": "file-1", "label": "File one" } }),
    )
    .await
    .expect("the pick finishes");

    // The pick parked for the editor's poll, and the grants landed.
    let outcome = take_connect_result(&pool, TENANT_A, &state).await.unwrap();
    assert_eq!(outcome.unwrap()["picked"]["id"], "file-1");
    let listed = list_grants(&pool, TENANT_A, Some("fakestatic"), weft_access_store::GrantOwnerScope::Author).await.unwrap();
    let row = listed.iter().find(|g| g.id == grant.id).unwrap();
    let mut scopes = row.scopes.clone();
    scopes.sort();
    assert_eq!(scopes, vec!["base.read".to_string(), "drive.file".to_string()]);
    assert!(!row.permissions_verified, "the pick never upgrades verification");
}

/// A picker opens only on the caller's own connection: an instance
/// parking one on the author's connection is refused at begin, so no
/// pick can ever widen the author's row.
#[weft_db_macros::test]
async fn a_instance_cannot_open_a_picker_on_the_authors_connection(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("the author connects");
    let err = weft_access_store::begin_picker(
        &pool,
        TENANT_A,
        weft_access_store::BeginPicker {
            access_id: done.grant.id,
            for_instance: Some(weft_core::instance::InstanceScope {
                project_id: PROJECT_1,
                instance: weft_core::instance::InstanceId::new("ada").unwrap(),
            }),
            service: "fakestatic".into(),
            script: "https://chooser.example.com/api.js".into(),
            code: "weft.done({id: 'x', label: 'X'})".into(),
            mime_types: Vec::new(),
            grants: vec!["drive.file".into()],
        },
    )
    .await
    .expect_err("the author's connection is not the instance's");
    assert!(err.to_string().contains("your own connection"), "{err}");
}

// ---------- Published connections (a node opens what it runs) ----------

/// The recipe a node publishes against: settings only, no provider
/// behind it, which is exactly what the store insists on.
fn published_spec() -> AccessSpec {
    serde_json::from_value(json!({
        "service": "selfrun",
        "acquisition": { "kind": "static", "fields": [
            { "name": "host", "secret": false },
            { "name": "password" },
            { "name": "port", "optional": true },
        ]},
        "identity": "{host}"
    }))
    .unwrap()
}

fn publish(values: &[(&str, &str)]) -> weft_access_store::PublishAccess {
    weft_access_store::PublishAccess {
        spec: published_spec(),
        project_id: PROJECT_1,
        instance: None,
        node_id: "db".into(),
        values: values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        label: Some("db".into()),
    }
}

/// A node's connection is keyed by the NODE, not the moment: it
/// publishes once and updates thereafter, so a database that keeps
/// answering on a new address never leaves a second connection behind.
#[weft_db_macros::test]
async fn publishing_twice_updates_one_connection(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let first = weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        publish(&[("host", "db.old"), ("password", "p")]),
    )
    .await
    .expect("first publish");

    let second = weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        publish(&[("host", "db.new"), ("password", "p")]),
    )
    .await
    .expect("second publish");

    assert_eq!(first.connection_id, second.connection_id, "the node's one connection, updated");
    assert_eq!(second.identity.as_deref(), Some("db.new"));
    assert_eq!(list_grants(&pool, TENANT_A, Some("selfrun"), weft_access_store::GrantOwnerScope::Author).await.unwrap().len(), 1);

    let found = weft_access_store::published_connection(&pool, TENANT_A, PROJECT_1, "db", None, "selfrun")
        .await
        .expect("look up")
        .expect("the node finds what it published");
    assert_eq!(found.connection_id, first.connection_id);
    assert!(
        weft_access_store::published_connection(&pool, TENANT_A, PROJECT_1, "other", None, "selfrun")
            .await
            .unwrap()
            .is_none(),
        "another node published nothing"
    );
}

/// A node publishes on every run, and every write tells the workers that
/// may use the connection to drop what they kept. So a publish that changes
/// nothing writes nothing and announces nothing, and one that changes the
/// connection announces it. A shared copy's connection is no instance's
/// own, so any project of the tenant may use it: it is announced for the
/// tenant.
#[weft_db_macros::test]
async fn only_a_publish_that_changes_the_connection_announces_it(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    static CHANNELS: &[&str] = &["weft_access"];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    async fn heard_it(heard: &mut weft_task_store::pg_signal::Subscription) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let tenant = format!("tenant:{TENANT_A}");
        heard.woken_before(deadline, |c, p| c == "weft_access" && p == tenant).await.unwrap()
    }

    weft_access_store::publish_grant(&pool, TENANT_A, publish(&[("host", "db"), ("password", "p")])).await.unwrap();
    assert!(heard_it(&mut heard).await, "the first publish is news");
    weft_access_store::publish_grant(&pool, TENANT_A, publish(&[("host", "db"), ("password", "p")])).await.unwrap();
    assert!(!heard_it(&mut heard).await, "the same values again write nothing");
    let published = weft_access_store::publish_grant(&pool, TENANT_A, publish(&[("host", "db"), ("password", "q")])).await.unwrap();
    assert!(heard_it(&mut heard).await, "a new password is news");

    // A pick is the project's own: announced for the project alone.
    pool.execute(
        "INSERT INTO install_pick (tenant_id, project_id, node, field, grant_id) VALUES ($1, $2, 'db', 'access', $3::text::uuid)",
        &[&TENANT_A, &PROJECT_1, &published.connection_id],
    )
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let project = PROJECT_1.to_string();
    assert!(heard.woken_before(deadline, |c, p| c == "weft_access" && p == project).await.unwrap(), "a pick names its project");
}

/// The shapes a published recipe may not have. Each one would later
/// have weft act on a third party's behalf for a service that has no
/// third party, and the values arrive from a worker.
#[weft_db_macros::test]
async fn a_published_recipe_describes_something_the_project_runs(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let refused = |spec: serde_json::Value| {
        let mut req = publish(&[("host", "db"), ("password", "p")]);
        req.spec = serde_json::from_value(spec).unwrap();
        req
    };

    let e = weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        refused(json!({
            "service": "selfrun",
            "acquisition": { "kind": "static", "fields": [{ "name": "host" }, { "name": "password" }] },
            "events": { "push": {
                "fields": { "id": "body.id" },
                "account": { "value": "host", "path": "body.host" },
                "webhook": {
                    "verify": { "kind": "hmac",
                                "signature": { "in": "header:X-Sig" },
                                "concat": "{body}" }
                }
            }},
        })),
    )
    .await
    .expect_err("a service that reports provider events");
    assert!(e.to_string().contains("provider events"), "{e}");

    let e = weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        refused(json!({
            "service": "selfrun",
            "acquisition": { "kind": "static", "fields": [{ "name": "host" }, { "name": "password" }] },
            "test": { "url": "http://example.invalid/whoami" },
        })),
    )
    .await
    .expect_err("a connect-time call");
    assert!(e.to_string().contains("connect-time check"), "{e}");

    // The shape a worker would actually reach for: a recipe with a
    // real provider behind it, whose token URL the control plane
    // would then be the one to call.
    let e = weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        refused(json!({
            "service": "selfrun",
            "acquisition": {
                "kind": "oauth2",
                "grant": { "kind": "client_credentials" },
                "tokenUrl": "http://attacker.invalid/token",
            },
        })),
    )
    .await
    .expect_err("a service acquired through a provider");
    assert!(e.to_string().contains("acquired another way"), "{e}");

    // A value the service does not declare is refused on the publish
    // path too, not only where a person pastes one.
    let mut extra = publish(&[("host", "db"), ("password", "p")]);
    extra.values.insert("hostname".into(), "db".into());
    let e = weft_access_store::publish_grant(&pool, TENANT_A, extra)
        .await
        .expect_err("an undeclared value");
    assert!(e.to_string().contains("no value named 'hostname'"), "{e}");

    // A blank required value is missing, not stored: an empty password
    // would fail at the database instead of here.
    let e = weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        publish(&[("host", "db"), ("password", "  ")]),
    )
    .await
    .expect_err("a blank password");
    assert!(e.to_string().contains("'password' is required"), "{e}");

    assert!(
        list_grants(&pool, TENANT_A, Some("selfrun"), weft_access_store::GrantOwnerScope::Author).await.unwrap().is_empty(),
        "nothing refused was written"
    );
}

/// Cleanup takes the node's connections and leaves a person's alone,
/// whether one node goes or the whole project does.
#[weft_db_macros::test]
async fn cleanup_removes_what_a_node_published_and_nothing_else(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        publish(&[("host", "db"), ("password", "p")]),
    )
    .await
    .expect("publish");

    // A person's connection in the same project, for the same service.
    let spec = published_spec();
    let theirs = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            spec,
            door: Door::Own,
            values: [("host".to_string(), "elsewhere".to_string()),
                     ("password".to_string(), "p".to_string())]
                .into_iter()
                .collect(),
            label: Some("mine".into()),
            permissions: vec![],
            registration: None,
            paste: false,
            project_id: Some(PROJECT_1),
            instance: None,
        },
    )
    .await
    .expect("a person connects one too");

    let dropped =
        weft_access_store::delete_published_grants(&pool, TENANT_A, PROJECT_1, Some(("db", None)))
            .await
            .expect("delete for one node");
    assert_eq!(dropped, 1);
    let left = list_grants(&pool, TENANT_A, Some("selfrun"), weft_access_store::GrantOwnerScope::Author).await.unwrap();
    assert_eq!(left.len(), 1, "the person's connection stays");
    assert_eq!(left[0].id, theirs.grant.id);

    // The project-wide sweep is the same rule with no node named.
    weft_access_store::publish_grant(
        &pool,
        TENANT_A,
        publish(&[("host", "db"), ("password", "p")]),
    )
    .await
    .expect("publish again");
    let dropped = weft_access_store::delete_published_grants(&pool, TENANT_A, PROJECT_1, None)
        .await
        .expect("delete for the project");
    assert_eq!(dropped, 1);
    let left = list_grants(&pool, TENANT_A, Some("selfrun"), weft_access_store::GrantOwnerScope::Author).await.unwrap();
    assert_eq!(left.len(), 1, "still only the person's");
    assert_eq!(left[0].id, theirs.grant.id);
}

/// An instance's connection is its own alone: only it sees it, use it as a
/// value, or forget it, and forgetting it takes the values naming it
/// along. An instance's values are its own, set together or not at all.
#[weft_db_macros::test]
async fn a_instances_connection_and_values_stay_theirs(pool: Pool) {
    use weft_access_store::{
        change_instance_values, forget_instance_grants, instance_values, ConnectionValue,
        GrantOwnerScope, InstanceValueWrite,
    };
    use weft_core::instance::InstanceId;
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let connect = |instance: &InstanceId, token: &str| ConnectDirect {
        paste: false,
        spec: static_spec(&base),
        door: Door::Own,
        registration: None,
        values: [("token".to_string(), token.to_string())].into_iter().collect(),
        label: None,
        permissions: Vec::new(),
        project_id: Some(PROJECT_1),
        instance: Some(instance.clone()),
    };
    let adas = connect_direct(&pool, TENANT_A, connect(&ada, "tok-ada")).await.expect("ada connects").grant;
    assert_eq!(adas.owner, CredentialOwner::Instance(ada.clone()));
    let bobs = connect_direct(&pool, TENANT_A, connect(&bob, "tok-bob")).await.expect("bob connects").grant;
    let mut both_doors = static_spec(&base);
    both_doors.doors = vec![Door::Shared, Door::Own];
    let err = connect_direct(&pool, TENANT_A, ConnectDirect { door: Door::Shared, spec: both_doors, ..connect(&bob, "unused") })
        .await
        .expect_err("an instance never spends the shared key");
    assert!(err.to_string().contains("the shared door is the program author's"), "{err}");

    let as_ada = GrantOwnerScope::Instance { project_id: PROJECT_1, instance: &ada };
    let seen: Vec<_> = list_grants(&pool, TENANT_A, None, as_ada).await.unwrap().into_iter().map(|g| g.id).collect();
    assert_eq!(seen, vec![adas.id]);
    assert!(list_grants(&pool, TENANT_A, None, GrantOwnerScope::Author).await.unwrap().is_empty(),
        "the author's list holds no instance's connection");

    let pick = |node: &str, grant: uuid::Uuid, service: &str| InstanceValueWrite {
        node: node.into(),
        field: "account".into(),
        value: serde_json::json!({ "id": grant }),
        connection: Some(ConnectionValue { grant_id: grant, service: service.into() }),
    };
    let plain = |node: &str, value: serde_json::Value| InstanceValueWrite {
        node: node.into(),
        field: "sheet".into(),
        value,
        connection: None,
    };
    let conn = pool.get().await.unwrap();
    let err = change_instance_values(&conn, TENANT_A, PROJECT_1, &ada, &[plain("read", serde_json::json!("s-1")), pick("post", bobs.id, "fakestatic")], &[])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not one of instance 'ada''s connections"), "{err}");
    assert!(instance_values(&pool, TENANT_A, PROJECT_1, &ada).await.unwrap().is_empty(), "a refused write stores none of it");
    let err = change_instance_values(&conn, TENANT_A, PROJECT_1, &ada, &[pick("post", adas.id, "slack")], &[]).await.unwrap_err();
    assert!(err.to_string().contains("connects to 'slack'"), "{err}");
    change_instance_values(
        &conn,
        TENANT_A,
        PROJECT_1,
        &ada,
        // A plain value keeps what the person typed exactly, a NUL included.
        &[pick("post", adas.id, "fakestatic"), pick("reply", adas.id, "fakestatic"), plain("read", serde_json::json!("s-\u{0}1"))],
        &[],
    )
    .await
    .unwrap();
    change_instance_values(&conn, TENANT_A, PROJECT_1, &bob, &[pick("post", bobs.id, "fakestatic")], &[]).await.unwrap();
    let nodes = |values: weft_core::instance::InstanceValues| values.into_keys().collect::<Vec<_>>();
    let adas_values = instance_values(&pool, TENANT_A, PROJECT_1, &ada).await.unwrap();
    assert_eq!(nodes(adas_values.clone()), vec!["post", "read", "reply"]);
    assert_eq!(adas_values["post"]["account"]["id"], serde_json::json!(adas.id));
    assert!(adas_values["post"]["account"].get("identity").is_some(), "a connection value reads back as its handle");
    assert_eq!(adas_values["read"]["sheet"], serde_json::json!("s-\u{0}1"));
    assert!(instance_values(&pool, TENANT_B, PROJECT_1, &ada).await.unwrap().is_empty(), "another tenant reads nothing");
    let clear = [("reply".to_string(), "account".to_string())];
    change_instance_values(&conn, TENANT_A, PROJECT_1, &ada, &[], &clear).await.unwrap();
    change_instance_values(&conn, TENANT_A, PROJECT_1, &ada, &[], &clear).await.unwrap();
    assert_eq!(nodes(instance_values(&pool, TENANT_A, PROJECT_1, &ada).await.unwrap()), vec!["post", "read"]);

    let err = delete_grant(&pool, TENANT_A, adas.id, GrantOwnerScope::Instance { project_id: PROJECT_1, instance: &bob })
        .await
        .unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");
    assert!(delete_grant(&pool, TENANT_A, adas.id, GrantOwnerScope::Author).await.is_err(), "the author cannot forget it either");
    delete_grant(&pool, TENANT_A, adas.id, as_ada).await.unwrap();
    assert_eq!(
        nodes(instance_values(&pool, TENANT_A, PROJECT_1, &ada).await.unwrap()),
        vec!["read"],
        "the connection values went with the connection, the plain one stays"
    );
    assert_eq!(nodes(instance_values(&pool, TENANT_A, PROJECT_1, &bob).await.unwrap()), vec!["post"]);
    change_instance_values(&conn, TENANT_A, PROJECT_1, &ada, &[], &[("read".to_string(), "sheet".to_string())])
        .await
        .unwrap();
    assert!(instance_values(&pool, TENANT_A, PROJECT_1, &ada).await.unwrap().is_empty());

    forget_instance_grants(&pool, TENANT_A, PROJECT_1, &bob).await.unwrap();
    assert!(instance_values(&pool, TENANT_A, PROJECT_1, &bob).await.unwrap().is_empty());
    let as_bob = GrantOwnerScope::Instance { project_id: PROJECT_1, instance: &bob };
    assert!(list_grants(&pool, TENANT_A, None, as_bob).await.unwrap().is_empty(), "bob's connection is gone");

    let orphan = pool
        .execute(
            "INSERT INTO access_grant (id, tenant_id, service, spec, values_sealed, instance_id, spec_version, exclusive) \
             VALUES ($1, $2, 'fakestatic', '{}', '', 'ada', $3, FALSE)",
            &[&uuid::Uuid::new_v4(), &TENANT_A, &weft_core::AccessSpec::STORED.version()],
        )
        .await
        .unwrap_err();
    assert!(orphan.to_string().contains("access_grant_instance_has_project"), "{orphan}");
}

/// An instance is a name inside ONE project: the same name in another
/// project is another person. A consent there never upgrades nor
/// rotates the first one's grant, and an instance never picks the shared
/// door, refused before anything is written.
#[weft_db_macros::test]
async fn a_instances_consent_stays_in_their_project(pool: Pool) {
    use weft_core::instance::InstanceId;
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    const PROJECT_2: uuid::Uuid = uuid::Uuid::from_u128(0xc0df);
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "exclusive");
    *fake.scope_echo.lock().unwrap() = Some("read".into());
    let ada = InstanceId::new("ada").unwrap();
    let begin = |project: uuid::Uuid, door: Door, upgrade: Option<uuid::Uuid>| BeginOAuth {
        spec: spec.clone(),
        door,
        registration: Some(oauth_app()),
        permissions: vec!["read".into()],
        project_id: Some(project),
        instance: Some(ada.clone()),
        upgrade_grant_id: upgrade,
        redirect_uri: "http://disp.example/access/oauth/callback".into(),
    };
    let consent = |req: BeginOAuth| {
        let pool = pool.clone();
        async move {
            let started = begin_oauth(&pool, TENANT_A, req).await?;
            anyhow::Ok(complete_oauth(&pool, &started.state, "the-code").await?.grant)
        }
    };

    let in_p1 = consent(begin(PROJECT_1, Door::Own, None)).await.expect("ada consents in P1");
    assert_eq!(in_p1.project_id, Some(PROJECT_1));

    // An upgrade from P2 naming P1's row is a clean miss.
    let err = begin_oauth(&pool, TENANT_A, begin(PROJECT_2, Door::Own, Some(in_p1.id))).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");

    // The same account consented from P2 is a new row; P1's is untouched.
    *fake.next_token.lock().unwrap() = "tok-p2".into();
    let in_p2 = consent(begin(PROJECT_2, Door::Own, None)).await.expect("ada consents in P2");
    assert_ne!(in_p2.id, in_p1.id, "another project's instance never rotates this one's row");
    assert_eq!(in_p2.project_id, Some(PROJECT_2));
    let p1 = resolve_for_worker(
        &pool,
        TENANT_A,
        GrantUser::Instance { project_id: PROJECT_1, instance: &ada },
        in_p1.id,
        "fakeoauth",
        &[],
        &[],
    )
    .await
    .expect("P1's row still resolves for P1's ada");
    assert_eq!(p1.values.get("token").map(String::as_str), Some("tok-1"), "P1's token kept");

    // The shared door is refused before the consent is even parked.
    let err = begin_oauth(&pool, TENANT_A, begin(PROJECT_1, Door::Shared, None)).await.unwrap_err();
    assert!(err.to_string().contains("the shared door is the program author's"), "{err}");
    let parked: i64 = pool.scalar("SELECT count(*) FROM access_connect", &[]).await.unwrap();
    assert_eq!(parked, 0, "the refused consent parked nothing");
}

/// The install keeps each access node's pick: only one of the author's
/// own connections, of the field's service, can be picked (never an
/// instance's), a change is all or none, and forgetting a connection takes
/// every pick of it along.
#[weft_db_macros::test]
async fn the_install_keeps_the_authors_picks(pool: Pool) {
    use weft_access_store::{change_install_picks, install_picks, PickWrite};
    use weft_core::instance::InstanceId;
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let connect = |instance: Option<InstanceId>, token: &str| ConnectDirect {
        paste: false,
        spec: static_spec(&base),
        door: Door::Own,
        registration: None,
        values: [("token".to_string(), token.to_string())].into_iter().collect(),
        label: None,
        permissions: Vec::new(),
        project_id: instance.as_ref().map(|_| PROJECT_1),
        instance,
    };
    let mine = connect_direct(&pool, TENANT_A, connect(None, "tok-mine")).await.expect("the author connects").grant;
    let adas = connect_direct(&pool, TENANT_A, connect(Some(InstanceId::new("ada").unwrap()), "tok-ada"))
        .await
        .expect("ada connects")
        .grant;
    let pick = |node: &str, grant: uuid::Uuid, service: &str| PickWrite {
        node: node.into(),
        field: "account".into(),
        grant_id: grant,
        service: service.into(),
    };
    let conn = pool.get().await.unwrap();
    let unknown = uuid::Uuid::new_v4();
    let err = change_install_picks(&conn, TENANT_A, PROJECT_1, &[pick("post", unknown, "fakestatic")], &[]).await.unwrap_err();
    assert!(err.to_string().contains(&unknown.to_string()), "a connection that is not there is named: {err}");
    let err = change_install_picks(&conn, TENANT_A, PROJECT_1, &[pick("post", mine.id, "fakestatic"), pick("read", adas.id, "fakestatic")], &[])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not one of your connections"), "an instance's is refused: {err}");
    assert!(install_picks(&pool, TENANT_A, PROJECT_1).await.unwrap().is_empty(), "all or none");
    let err = change_install_picks(&conn, TENANT_A, PROJECT_1, &[pick("post", mine.id, "slack")], &[]).await.unwrap_err();
    assert!(err.to_string().contains("is a 'fakestatic' one"), "{err}");
    assert!(change_install_picks(&conn, TENANT_B, PROJECT_1, &[pick("post", mine.id, "fakestatic")], &[]).await.is_err(),
        "another tenant's connection is nobody's to pick");

    change_install_picks(&conn, TENANT_A, PROJECT_1, &[pick("post", mine.id, "fakestatic"), pick("one.post", mine.id, "fakestatic")], &[])
        .await
        .unwrap();
    let picks = install_picks(&pool, TENANT_A, PROJECT_1).await.unwrap();
    assert_eq!(picks["post"]["account"]["id"], serde_json::json!(mine.id));
    assert!(picks.contains_key("one.post"), "each place keeps its own pick");
    change_install_picks(&conn, TENANT_A, PROJECT_1, &[], &[("one.post".to_string(), "account".to_string())]).await.unwrap();
    assert!(!install_picks(&pool, TENANT_A, PROJECT_1).await.unwrap().contains_key("one.post"));

    delete_grant(&pool, TENANT_A, mine.id, weft_access_store::GrantOwnerScope::Author).await.unwrap();
    assert!(install_picks(&pool, TENANT_A, PROJECT_1).await.unwrap().is_empty(), "the pick goes with its connection");
}

/// A move carries a place's pick and every instance's values to the new
/// place at once, and reading back shows them there with their services.
#[weft_db_macros::test]
async fn a_move_carries_a_places_picks_and_every_instances_values(pool: Pool) {
    use weft_access_store::{
        change_install_picks, change_instance_values, move_stored, stored_fields, ConnectionValue, InstanceValueWrite, PickWrite,
    };
    use weft_core::instance::InstanceId;
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let connect = |instance: Option<InstanceId>, token: &str| ConnectDirect {
        paste: false,
        spec: static_spec(&base),
        door: Door::Own,
        registration: None,
        values: [("token".to_string(), token.to_string())].into_iter().collect(),
        label: None,
        permissions: Vec::new(),
        project_id: instance.as_ref().map(|_| PROJECT_1),
        instance,
    };
    let ada = InstanceId::new("ada").unwrap();
    let mine = connect_direct(&pool, TENANT_A, connect(None, "tok-mine")).await.expect("the author connects").grant;
    let adas = connect_direct(&pool, TENANT_A, connect(Some(ada.clone()), "tok-ada")).await.expect("ada connects").grant;
    let conn = pool.get().await.unwrap();
    change_install_picks(
        &conn,
        TENANT_A,
        PROJECT_1,
        &[PickWrite { node: "chat.agent".into(), field: "account".into(), grant_id: mine.id, service: "fakestatic".into() }],
        &[],
    )
    .await
    .unwrap();
    change_instance_values(
        &conn,
        TENANT_A,
        PROJECT_1,
        &ada,
        &[InstanceValueWrite {
            node: "chat.agent".into(),
            field: "other".into(),
            value: serde_json::json!({ "id": adas.id }),
            connection: Some(ConnectionValue { grant_id: adas.id, service: "fakestatic".into() }),
        }],
        &[],
    )
    .await
    .unwrap();

    let (picks, values) = move_stored(&conn, TENANT_A, PROJECT_1, "chat.agent", "studio.chat.agent").await.unwrap();
    assert_eq!((picks, values), (1, 1));
    let mut stored = stored_fields(&pool, TENANT_A, PROJECT_1).await.unwrap();
    stored.sort_by(|a, b| a.field.cmp(&b.field));
    assert!(stored.iter().all(|s| s.node == "studio.chat.agent"), "{stored:?}");
    assert_eq!(stored[0].instance, None);
    assert_eq!(stored[0].service.as_deref(), Some("fakestatic"));
    assert_eq!(stored[1].instance, Some(ada));
    assert!(stored_fields(&pool, TENANT_B, PROJECT_1).await.unwrap().is_empty(), "another tenant sees nothing");
}

/// A picker session lives 15 minutes: one past its life cannot finish (it
/// would widen the connection's grants long after the person picked), and
/// the page loading it finds it expired and deletes it.
#[weft_db_macros::test]
async fn a_picker_past_its_life_cannot_finish(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: vec![],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("the fixture connects");
    let state = weft_access_store::begin_picker(
        &pool,
        TENANT_A,
        weft_access_store::BeginPicker {
            access_id: done.grant.id,
            for_instance: None,
            service: "fakestatic".into(),
            script: "https://chooser.example.com/api.js".into(),
            code: "weft.done({id: 'x', label: 'X'})".into(),
            mime_types: Vec::new(),
            grants: vec!["drive.file".into()],
        },
    )
    .await
    .expect("the picker session parks");
    pool.execute("UPDATE access_picker SET created_at = now() - interval '16 minutes'", &[]).await.unwrap();

    let err = weft_access_store::finish_picker(&pool, &state, json!({ "picked": { "id": "file-1", "label": "File one" } }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("expired"), "{err}");
    let listed = list_grants(&pool, TENANT_A, Some("fakestatic"), weft_access_store::GrantOwnerScope::Author).await.unwrap();
    let row = listed.iter().find(|g| g.id == done.grant.id).unwrap();
    assert!(!row.scopes.contains(&"drive.file".to_string()), "nothing widened: {:?}", row.scopes);

    assert!(weft_access_store::load_picker(&pool, &state).await.is_err());
    let left: i64 = pool.scalar("SELECT COUNT(*) FROM access_picker", &[]).await.unwrap();
    assert_eq!(left, 0, "found expired, deleted");
}

/// Connect attempts past their life go with no timer: when the next one
/// of their kind is made, and when a poll finds them gone.
#[weft_db_macros::test]
async fn an_abandoned_connect_goes_when_the_next_is_made_or_a_poll_finds_it_gone(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let begin = || {
        let (pool, base) = (pool.clone(), base.clone());
        async move {
            begin_oauth(
                &pool,
                TENANT_A,
                BeginOAuth {
                    spec: oauth_spec(&base, "coexisting"),
                    door: Door::Own,
                    registration: Some(oauth_app()),
                    permissions: vec!["read".into()],
                    project_id: Some(PROJECT_1),
                    instance: None,
                    upgrade_grant_id: None,
                    redirect_uri: "http://disp.example/access/oauth/callback".into(),
                },
            )
            .await
            .unwrap()
            .state
        }
    };
    let rows = || async {
        pool.scalars::<String>("SELECT state FROM access_connect ORDER BY state", &[]).await.unwrap()
    };
    begin().await;
    pool.execute("UPDATE access_connect SET created_at = now() - interval '16 minutes'", &[]).await.unwrap();
    let new = begin().await;
    assert_eq!(rows().await, vec![new.clone()], "the abandoned one went as the next was made");

    pool.execute("UPDATE access_connect SET created_at = now() - interval '16 minutes'", &[]).await.unwrap();
    let err = take_connect_result(&pool, TENANT_A, &new).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Gone(_))), "{err}");
    assert!(rows().await.is_empty(), "the poll that found it gone deleted it");
}

/// The fixture OAuth service with `patch`'s keys set on its acquisition.
fn oauth_spec_with(base: &str, patch: serde_json::Value) -> AccessSpec {
    let mut spec = serde_json::to_value(oauth_spec(base, "coexisting")).unwrap();
    for (k, v) in patch.as_object().unwrap() {
        spec["acquisition"][k] = v.clone();
    }
    serde_json::from_value(spec).unwrap()
}

/// The last call the fake recorded that starts with `prefix`.
fn last_call(fake: &FakeProvider, prefix: &str) -> String {
    fake.calls.lock().unwrap().iter().rfind(|c| c.starts_with(prefix)).cloned().unwrap_or_default()
}

/// A provider that names the app's id differently (TikTok's
/// `client_key`) gets it under that name on the consent URL, the
/// exchange and the refresh, never as `client_id`; the declared extra
/// params ride every token call, and the consent's own params read the
/// app's values.
#[weft_db_macros::test]
async fn a_provider_naming_the_client_differently_gets_that_name_everywhere(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec_with(&base, json!({
        "clientParams": { "id": "client_key" },
        "authParams": { "app": "{client_id}" },
        "extraParams": { "audience": "{client_id}-api" }
    }));
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(1);
    let started = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Own,
            registration: Some(oauth_app()),
            permissions: vec!["read".into()],
            project_id: Some(PROJECT_1),
            instance: None,
            upgrade_grant_id: None,
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await
    .unwrap();
    let consent = url::Url::parse(&started.consent_url).unwrap();
    let query: BTreeMap<String, String> = consent.query_pairs().into_owned().collect();
    // Failures name the consent URL's keys only: the URL carries the
    // flow's state, which is never printed.
    let keys: Vec<&String> = query.keys().collect();
    assert_eq!(query.get("client_key").map(String::as_str), Some("cid"), "{keys:?}");
    assert!(!query.contains_key("client_id"), "{keys:?}");
    assert_eq!(query.get("app").map(String::as_str), Some("cid"), "{keys:?}");
    let scopes: Vec<&str> = query.get("scope").map(|s| s.split([' ', ',']).collect()).unwrap_or_default();
    assert!(scopes.contains(&"read"), "the consent asks for every permission the connect carries: {scopes:?}");
    let grant = complete_oauth(&pool, &started.state, "the-code").await.unwrap().grant;
    expire_now(&pool, grant.id).await;

    // The token calls' bodies carry the client secret and the tokens, so
    // a failure says which check failed, never the body.
    let exchange = last_call(&fake, "/token");
    assert!(exchange.contains("client_key=cid") && exchange.contains("client_secret=sec"), "the exchange names the client by client_key");
    assert!(!exchange.contains("client_id="), "the exchange sends no client_id");
    assert!(exchange.contains("audience=cid-api"), "the exchange carries the audience");

    *fake.next_token.lock().unwrap() = "tok-fresh".into();
    *fake.expires_in.lock().unwrap() = Some(3600);
    let resolved = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[])
        .await
        .expect("the refresh succeeds");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-fresh"));
    let refresh = last_call(&fake, "/token");
    assert!(refresh.contains("grant_type=refresh_token"), "the refresh is a refresh_token grant");
    assert!(refresh.contains("client_key=cid") && !refresh.contains("client_id="), "the refresh names the client by client_key");
    assert!(refresh.contains("audience=cid-api"), "the refresh carries the audience");
}

/// The Instagram shape: the exchange answers a short-lived token nested
/// under `data[0]` with no expiry, a declared GET swaps it at once for
/// the token to keep (its answer sets the expiry), and a declared GET
/// renews that one later. The standard token endpoint only ever sees
/// the exchange.
#[weft_db_macros::test]
async fn a_short_lived_token_is_swapped_after_the_exchange_and_renewed_by_a_get(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec_with(&base, json!({
        "tokenPath": "data.0.access_token",
        "scopePath": "data.0.permissions",
        "captures": [{ "name": "userId", "path": "data.0.user_id" }],
        "afterExchange": {
            "url": format!("{base}/exchange?grant_type=swap&client_secret={{client_secret}}&access_token={{token}}")
        },
        "refresh": {
            "url": format!("{base}/exchange?grant_type=renew&access_token={{token}}")
        }
    }));
    *fake.token_answer.lock().unwrap() = Some(json!({
        "data": [{ "access_token": "tok-short", "user_id": "1020", "permissions": "read" }]
    }));
    *fake.next_token.lock().unwrap() = "tok-long-1".into();
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    assert!(grant.expires_at.is_some(), "the swap's answer set the expiry");
    assert_eq!(grant.scopes, vec!["read".to_string()], "the nested scope echo was read");
    let swap = last_call(&fake, "/exchange");
    assert!(swap.contains("\"swap\"") && swap.contains("\"tok-short\"") && swap.contains("\"sec\""), "{swap}");

    *fake.next_token.lock().unwrap() = "tok-long-2".into();
    *fake.expires_in.lock().unwrap() = Some(3600);
    let resolved = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[])
        .await
        .expect("the declared renewal succeeds");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-long-2"));
    assert_eq!(resolved.values.get("userId").map(String::as_str), Some("1020"), "the exchange's capture stays");
    let renew = last_call(&fake, "/exchange");
    assert!(renew.contains("\"renew\"") && renew.contains("\"tok-long-1\""), "{renew}");
    assert_eq!(fake.token_requests.load(Ordering::SeqCst), 1, "only the exchange used the token endpoint");
}

/// A provider answering under its own names: the token, refresh token
/// and absolute expiry are read through the declared paths, on the
/// exchange AND on the standard refresh, whose captures run too and
/// keep what a slimmer refresh answer leaves out.
#[weft_db_macros::test]
async fn a_nested_answer_is_read_through_its_paths_on_the_exchange_and_the_refresh(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec_with(&base, json!({
        "tokenPath": "result.token",
        "refreshTokenPath": "result.renew",
        "expiresPath": "result.until",
        "captures": [
            { "name": "account", "path": "result.account" },
            { "name": "region", "path": "result.region", "optional": true }
        ]
    }));
    let soon = chrono::Utc::now().timestamp() + 5;
    *fake.token_answer.lock().unwrap() = Some(json!({
        "result": { "token": "tok-n1", "renew": "rt-n1", "until": soon, "account": "acme", "region": "eu" }
    }));
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    assert_eq!(grant.expires_at.map(|at| at.timestamp()), Some(soon));
    expire_now(&pool, grant.id).await;

    let far = chrono::Utc::now() + chrono::Duration::hours(1);
    *fake.token_answer.lock().unwrap() = Some(json!({
        "result": { "token": "tok-n2", "until": far.to_rfc3339(), "region": "us" }
    }));
    let resolved = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[])
        .await
        .expect("the refresh reads the same paths");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-n2"));
    assert_eq!(resolved.values.get("region").map(String::as_str), Some("us"), "the refresh's captures ran");
    assert_eq!(resolved.values.get("account").map(String::as_str), Some("acme"), "a capture the refresh lacks is kept");
    let refresh = last_call(&fake, "/token");
    assert!(refresh.contains("refresh_token=rt-n1"), "the nested refresh token was stored: {refresh}");
}

/// A refusal answered with a success status and an error OBJECT (not
/// an OAuth code string) fails the connect with the provider's words.
#[weft_db_macros::test]
async fn a_refusal_answered_200_with_an_error_object_fails_the_connect(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    *fake.token_answer.lock().unwrap() = Some(json!({
        "error": { "message": "This authorization code has been used", "type": "OAuthException", "code": 100 }
    }));
    let err = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{err:#}");
    assert!(err.to_string().contains("This authorization code has been used"), "{err:#}");
}

/// A connection weft could renew (a refresh token came back) whose
/// answer names no expiry would never be renewed, so the connect is
/// refused naming the keys that fix the recipe. Declaring the token
/// never expires connects it with no expiry; a token weft could not
/// renew anyway connects as it always did.
#[weft_db_macros::test]
async fn a_renewable_token_with_no_expiry_is_refused_at_connect(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = None;
    let err = full_consent(&pool, TENANT_A, &oauth_spec(&base, "coexisting"), &["read"], None).await.unwrap_err();
    let message = err.to_string();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{err:#}");
    assert!(message.contains("'expires_in'") && message.contains("expiresInPath"), "{message}");
    assert!(message.contains("neverExpires"), "{message}");

    let never = oauth_spec_with(&base, json!({ "neverExpires": true }));
    let grant = full_consent(&pool, TENANT_A, &never, &["read"], None).await.expect("a declared never-expiring token connects");
    assert_eq!(grant.expires_at, None);

    *fake.next_refresh.lock().unwrap() = None;
    let grant = full_consent(&pool, TENANT_A, &oauth_spec(&base, "coexisting"), &["read"], None)
        .await
        .expect("a token with nothing to renew it connects as before");
    assert_eq!(grant.expires_at, None);
}

/// A Slack-class provider answers a refusal with a 200 and `ok: false`.
/// It is seen exactly where the recipe declares its success flag: the
/// token answer's `okPath` refuses the connect in the provider's words,
/// and a lookup's `okPath` refuses the lookup, classified by its code
/// (`invalid_auth` on a person's own connection asks for a reconnect).
/// Without the flag declared, `ok` is just data.
#[weft_db_macros::test]
async fn a_success_flag_refuses_only_where_the_recipe_declares_it(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    *fake.token_answer.lock().unwrap() = Some(json!({ "ok": false, "error": "invalid_code" }));
    let flagged = oauth_spec_with(&base, json!({ "okPath": "ok" }));
    let err = full_consent(&pool, TENANT_A, &flagged, &["read"], None).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{err:#}");
    assert!(err.to_string().contains("invalid_code"), "{err:#}");

    *fake.token_answer.lock().unwrap() = Some(json!({ "ok": false, "access_token": "tok-1" }));
    full_consent(&pool, TENANT_A, &oauth_spec(&base, "coexisting"), &["read"], None)
        .await
        .expect("no flag declared, so `ok` is just data");

    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: static_spec(&base),
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "tok-abc".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .unwrap();
    let resolved =
        resolve_for_worker(&pool, TENANT_A, GrantUser::Author, done.grant.id, "fakestatic", &[], &[]).await.unwrap();
    let lookup_spec: weft_core::node::Lookup = serde_json::from_value(json!({
        "get": format!("{base}/refusing"), "items": "channels", "label": "name", "value": "id",
        "okPath": "ok", "errorPath": "error"
    }))
    .unwrap();
    let url = weft_access_store::lookup_url(&lookup_spec, "", &BTreeMap::new()).unwrap();
    let err = lookup(Some(&resolved), &lookup_spec, None, &url).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })), "{err:#}");
    assert!(err.to_string().contains("invalid_auth"), "{err:#}");
}

/// A refusal at a declared `errorPath` is classified by the code found
/// THERE: on a refresh, a busy provider is tried again and keeps the
/// connection, a refused grant asks for a reconnect, whatever the
/// conventional `error` field says.
#[weft_db_macros::test]
async fn a_refusal_at_a_declared_error_path_is_classified_by_its_code(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec_with(&base, json!({ "errorPath": "error_type" }));
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;

    *fake.token_answer.lock().unwrap() =
        Some(json!({ "error_type": "temporarily_unavailable", "error": "invalid_grant" }));
    let err = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{err:#}");

    *fake.token_answer.lock().unwrap() = Some(json!({ "error_type": "invalid_grant", "error": "ratelimited" }));
    let err = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })), "{err:#}");
    assert!(err.to_string().contains("invalid_grant"), "{err:#}");
}

/// Make a connection's token due for renewal now, as if its lifetime ran out.
async fn expire_now(pool: &Pool, id: uuid::Uuid) {
    pool.execute("UPDATE access_grant SET expires_at = now() - interval '1 minute' WHERE id = $1", &[&id]).await.unwrap();
}

/// A renewal answer naming no expiry lasts as long as the connection's
/// token did (RFC 6749 lets a provider leave a documented lifetime out),
/// so the connection is not renewed again on the next open. What a
/// rotating provider issued is kept: the next renewal sends the NEW
/// refresh token.
#[weft_db_macros::test]
async fn a_renewal_naming_no_expiry_lasts_the_connections_lifetime(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(3600);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    expire_now(&pool, grant.id).await;
    let resolve = || resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]);

    *fake.next_token.lock().unwrap() = "tok-2".into();
    *fake.next_refresh.lock().unwrap() = Some("refresh-2".into());
    *fake.expires_in.lock().unwrap() = None;
    let before = fake.token_requests.load(Ordering::SeqCst);
    let resolved = resolve().await.expect("the renewal lasts the known lifetime");
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-2"));
    assert!(last_call(&fake, "/token").contains("refresh_token=refresh-1"));
    let fresh_for = resolved.fresh_until.expect("an expiry") - chrono::Utc::now();
    assert!((3500..=3600).contains(&fresh_for.num_seconds()), "{fresh_for}");
    assert_eq!(fake.token_requests.load(Ordering::SeqCst), before + 1);

    resolve().await.expect("still fresh");
    assert_eq!(fake.token_requests.load(Ordering::SeqCst), before + 1, "no renewal before that lifetime");

    expire_now(&pool, grant.id).await;
    *fake.next_refresh.lock().unwrap() = Some("refresh-3".into());
    resolve().await.expect("renews again");
    assert!(last_call(&fake, "/token").contains("refresh_token=refresh-2"), "the rotated token was persisted");
}

/// A renewal answer whose expiry does not read, or leaves the token no
/// time to live, keeps what the provider issued and lasts the
/// connection's known lifetime, so it is never renewed again on every
/// open.
#[weft_db_macros::test]
async fn an_unusable_renewal_expiry_lasts_the_connections_lifetime(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(3600);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let resolve = || resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]);

    for (n, expires_in) in [json!("soon"), json!(0)].into_iter().enumerate() {
        expire_now(&pool, grant.id).await;
        let token = format!("tok-{}", n + 2);
        *fake.token_answer.lock().unwrap() =
            Some(json!({ "access_token": token, "refresh_token": format!("refresh-{}", n + 2), "expires_in": expires_in }));
        let before = fake.token_requests.load(Ordering::SeqCst);
        let resolved = resolve().await.expect("the renewal is kept");
        assert_eq!(resolved.values.get("token"), Some(&token), "{expires_in}");
        let fresh_for = resolved.fresh_until.expect("an expiry") - chrono::Utc::now();
        assert!((3500..=3600).contains(&fresh_for.num_seconds()), "{expires_in}: {fresh_for}");
        resolve().await.expect("still fresh");
        assert_eq!(fake.token_requests.load(Ordering::SeqCst), before + 1, "{expires_in}: one renewal, not one per open");
    }
}

/// `afterExchange` replaces the whole token set: a refresh token from
/// the exchange belongs to the short-lived token it discarded, so when
/// the swap's answer carries none the connection has none, and with no
/// renewal declared and no expiry it connects as a token nothing renews
/// (it is not judged renewable off the stale refresh token).
#[weft_db_macros::test]
async fn after_exchange_drops_the_exchanges_refresh_token(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec_with(&base, json!({
        "afterExchange": {
            "url": format!("{base}/exchange?grant_type=swap&access_token={{token}}")
        }
    }));
    *fake.token_answer.lock().unwrap() =
        Some(json!({ "access_token": "tok-short", "refresh_token": "rt-stale", "expires_in": 3600 }));
    *fake.next_token.lock().unwrap() = "tok-long".into();
    *fake.expires_in.lock().unwrap() = None;
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None)
        .await
        .expect("no refresh token survives the swap, so nothing is renewable and no expiry is needed");
    assert_eq!(grant.expires_at, None);
    let resolved = resolve_for_worker(&pool, TENANT_A, GrantUser::Author, grant.id, "fakeoauth", &[], &[]).await.unwrap();
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-long"));
}

/// A database from before `access_grant_one_exclusive`, where two sign-ins
/// to one exclusive account raced and left rows (and older code stored the
/// author's row with a project), boots: the rows fold into the one written
/// last, with every permission any of them held, and everything that named
/// a folded row (by a column, or inside its text) names the kept one before
/// the index is built. The fold run again changes nothing, and once the
/// index exists a boot no longer runs it.
#[weft_db_macros::test]
async fn a_database_with_duplicate_exclusive_rows_folds_them_before_the_index(pool: Pool) {
    let history = weft_task_store::schema_guard::History::embedded();
    let contract = history
        .migrations()
        .iter()
        .find(|m| m.group == "access_grant" && m.sql.contains("CREATE UNIQUE INDEX access_grant_one_exclusive"))
        .expect("a released migration builds access_grant_one_exclusive")
        .id;
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    // The database as it was before the contract part: no index, the
    // columns it drops still there, the contract still to run.
    pool.batch_execute(
        "DROP INDEX access_grant_one_exclusive; \
         ALTER TABLE access_grant ADD COLUMN spec_json JSONB, ADD COLUMN spec_json_version INTEGER",
    )
    .await
    .unwrap();
    pool.execute("DELETE FROM weft_migration WHERE group_name = 'access_grant' AND id = $1", &[&contract]).await.unwrap();
    // The dispatcher's `signal` table, which an install holds beside the
    // access store's: only the columns the fold reads and writes.
    pool.batch_execute("CREATE TABLE signal (token TEXT PRIMARY KEY, access_id TEXT, spec_json TEXT NOT NULL)").await.unwrap();

    let (oldest, older, newest, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let seed = |id: uuid::Uuid, identity: &'static str, project: Option<uuid::Uuid>, scopes: serde_json::Value, age_secs: i64| {
        let pool = pool.clone();
        async move {
            pool.execute(
                "INSERT INTO access_grant (id, tenant_id, service, client_id, project_id, spec, spec_version, exclusive, \
                     values_sealed, granted_scopes, identity, updated_at) \
                 VALUES ($1, $2, 'fakeoauth', 'cid', $3, '{\"grants\": \"exclusive\"}', 1, TRUE, 'sealed', $4::text::jsonb, $5, \
                     now() - make_interval(secs => $6))",
                &[&id, &TENANT_A, &project, &scopes.to_string(), &identity, &(age_secs as f64)],
            )
            .await
            .unwrap();
        }
    };
    // Stored with a project by older code: the author's all the same.
    seed(oldest, "me@example.com", Some(PROJECT_1), json!(["d"]), 300).await;
    seed(older, "me@example.com", None, json!(["a", "b"]), 200).await;
    seed(newest, "me@example.com", None, json!(["b", "c"]), 100).await;
    seed(other, "someone-else@example.com", None, json!(["a"]), 100).await;

    // Every place that names a connection, each pointing at a row that goes.
    pool.execute(
        "INSERT INTO install_pick (tenant_id, project_id, node, field, grant_id) VALUES ($1, $2, 'mail', 'access', $3)",
        &[&TENANT_A, &PROJECT_1, &older],
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO instance_value (tenant_id, project_id, instance_id, node, field, value, grant_id) \
         VALUES ($1, $2, 'alice', 'mail', 'access', $3, $4)",
        &[&TENANT_A, &PROJECT_1, &JsonText(json!({"id": oldest.to_string(), "identity": "me@example.com"})), &oldest],
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO access_connect (state, tenant_id, service, registration_sealed, spec_json, upgrade_grant_id, redirect_uri) \
         VALUES ('connect-1', $1, 'fakeoauth', 'sealed', '{}', $2, 'http://localhost/callback')",
        &[&TENANT_A, &oldest],
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO access_picker (state, tenant_id, access_id, service, script, code) \
         VALUES ('picker-1', $1, $2, 'fakeoauth', 'script', 'code')",
        &[&TENANT_A, &older],
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO access_connect_result (state, tenant_id, result_json) VALUES ('result-1', $1, $2)",
        &[&TENANT_A, &JsonText(json!({"grant": {"id": older.to_string(), "service": "fakeoauth"}}))],
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO signal_subscription (id, tenant_id, service, topic, access_id, signal_token, token_sealed) \
         VALUES ('sub-1', $1, 'fakeoauth', 'mail', $2, 'signal-1', 'sealed')",
        &[&TENANT_A, &older],
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO signal (token, access_id, spec_json) VALUES ('signal-1', $1, $2)",
        &[&oldest.to_string(), &json!({"kind": "mail", "access": {"id": oldest.to_string()}}).to_string()],
    )
    .await
    .unwrap();

    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.expect("the boot folds, then builds the index");

    let rows: Vec<(uuid::Uuid, Option<uuid::Uuid>, db::Json<serde_json::Value>)> =
        pool.all("SELECT id, project_id, granted_scopes FROM access_grant WHERE identity = 'me@example.com'", &[]).await.unwrap();
    assert_eq!(rows, vec![(newest, None, db::Json(json!(["a", "b", "c", "d"])))], "the newest kept, with every permission");
    let still: i64 = pool.scalar("SELECT count(*) FROM access_grant WHERE id = $1", &[&other]).await.unwrap();
    assert_eq!(still, 1, "another account's row is not touched");
    let pick: uuid::Uuid = pool.scalar("SELECT grant_id FROM install_pick", &[]).await.unwrap();
    assert_eq!(pick, newest);
    let (value_grant, JsonText(value)): (uuid::Uuid, JsonText<serde_json::Value>) =
        pool.one("SELECT grant_id, value FROM instance_value", &[]).await.unwrap();
    assert_eq!((value_grant, value["id"].clone()), (newest, json!(newest.to_string())));
    let upgrading: Option<uuid::Uuid> =
        pool.scalar("SELECT upgrade_grant_id FROM access_connect", &[]).await.unwrap();
    assert_eq!(upgrading, Some(newest));
    let picking: uuid::Uuid = pool.scalar("SELECT access_id FROM access_picker", &[]).await.unwrap();
    assert_eq!(picking, newest);
    let JsonText(parked): JsonText<serde_json::Value> =
        pool.scalar("SELECT result_json FROM access_connect_result", &[]).await.unwrap();
    assert_eq!(parked, json!({"grant": {"id": newest.to_string(), "service": "fakeoauth"}}));
    let subscribed: uuid::Uuid = pool.scalar("SELECT access_id FROM signal_subscription", &[]).await.unwrap();
    assert_eq!(subscribed, newest);
    let (signal_access, signal_spec): (Option<String>, String) =
        pool.one("SELECT access_id, spec_json FROM signal", &[]).await.unwrap();
    assert_eq!(signal_access, Some(newest.to_string()));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&signal_spec).unwrap(),
        json!({"kind": "mail", "access": {"id": newest.to_string()}})
    );
    let indexed: bool = pool.scalar("SELECT to_regclass('access_grant_one_exclusive') IS NOT NULL", &[]).await.unwrap();
    assert!(indexed, "the contract part built the index once the rows fit it");

    // The fold run again finds nothing to do.
    let everything = |pool: Pool| async move {
        let mut tables = Vec::new();
        for table in [
            "access_grant",
            "install_pick",
            "instance_value",
            "access_connect",
            "access_picker",
            "access_connect_result",
            "signal_subscription",
            "signal",
        ] {
            let rows: Option<String> =
                pool.scalar(&format!("SELECT string_agg(t::text, E'\\n' ORDER BY t::text) FROM {table} t"), &[]).await.unwrap();
            tables.push((table, rows));
        }
        tables
    };
    let before = everything(pool.clone()).await;
    let [weft_task_store::DataStep { work: weft_task_store::StepWork::Sql(fold), .. }] = weft_access_store::GROUP.data_steps else {
        panic!("the access store has one data step, the fold, in SQL")
    };
    pool.batch_execute(fold).await.expect("the fold runs again");
    assert_eq!(everything(pool.clone()).await, before, "a second fold changes nothing");

    // With the index built, a boot that has schema work (a stale stamp
    // here) leaves the fold out: an author's row stored with a project,
    // which the fold would make tenant-wide, stays as it is.
    let later = uuid::Uuid::new_v4();
    seed(later, "me@example.com", Some(PROJECT_1), json!(["a"]), 0).await;
    pool.execute("UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'access_grant'", &[]).await.unwrap();
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.expect("the boot restamps");
    let project: Option<uuid::Uuid> =
        pool.scalar("SELECT project_id FROM access_grant WHERE id = $1", &[&later]).await.unwrap();
    assert_eq!(project, Some(PROJECT_1), "the fold did not run once the index existed");
}

/// An upgrade whose consent signs in as an account that already has its
/// own exclusive connection is refused, naming that connection to
/// upgrade instead, and leaves both rows as they were.
#[weft_db_macros::test]
async fn an_upgrade_signed_in_as_an_account_with_its_own_connection_is_refused(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "exclusive");
    // The fake provider always signs in as 'quentin': the first row is
    // made another account's by hand, so the next sign-in makes quentin's.
    let theirs = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    pool.execute("UPDATE access_grant SET identity = 'someone-else' WHERE id = $1", &[&theirs.id]).await.unwrap();
    let quentins = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    assert_ne!(quentins.id, theirs.id);

    let err = full_consent(&pool, TENANT_A, &spec, &["write"], Some(theirs.id)).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{err:#}");
    let said = err.to_string();
    assert!(said.contains("quentin") && said.contains(&quentins.id.to_string()), "{said}");
    let identity: Option<String> =
        pool.scalar("SELECT identity FROM access_grant WHERE id = $1", &[&theirs.id]).await.unwrap();
    assert_eq!(identity.as_deref(), Some("someone-else"), "the upgraded row is left as it was");
}

/// A spec a node publishes is kept exactly, a NUL character in it
/// included: it reads back as published, and publishing it again finds
/// the row unchanged.
#[weft_db_macros::test]
async fn a_published_spec_holding_a_nul_is_kept_exactly(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let mut spec = published_spec();
    spec.label = Some("self\u{0}run".into());
    let with_nul = || weft_access_store::PublishAccess { spec: spec.clone(), ..publish(&[("host", "db"), ("password", "p")]) };
    let first = weft_access_store::publish_grant(&pool, TENANT_A, with_nul()).await.expect("published");
    let id: uuid::Uuid = first.connection_id.parse().unwrap();
    let (JsonText(stored), version, exclusive): (JsonText<serde_json::Value>, i32, bool) =
        pool.one("SELECT spec, spec_version, exclusive FROM access_grant WHERE id = $1", &[&id]).await.unwrap();
    assert_eq!(AccessSpec::STORED.decode(stored, version).unwrap(), spec);
    assert!(!exclusive);
    let written_at = || async {
        pool.scalar::<chrono::DateTime<chrono::Utc>>("SELECT updated_at FROM access_grant WHERE id = $1", &[&id]).await.unwrap()
    };
    let before = written_at().await;
    let again = weft_access_store::publish_grant(&pool, TENANT_A, with_nul()).await.expect("published again");
    assert_eq!(again.connection_id, first.connection_id);
    assert_eq!(written_at().await, before, "it read back equal, so nothing was written");
}

/// The headers a route gated by a connection reads its caller's
/// credential from come off the connection's recipe, behind the same wall
/// as every read: another tenant's connection is not there, and one of
/// another service is refused by name.
#[weft_db_macros::test]
async fn a_gates_credential_headers_come_from_its_recipe(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let spec: AccessSpec = serde_json::from_value(json!({
        "service": "api_key_auth",
        "acquisition": { "kind": "static", "fields": [{ "name": "keys" }] },
        "verify": { "kind": "api_keys", "header": "X-Team-Key" }
    }))
    .expect("a valid auth recipe");
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec,
            door: Door::Own,
            registration: None,
            values: [("keys".to_string(), "key-1".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("a pasted verifier connection stores without a test call");
    let id = done.grant.id;

    let headers = weft_access_store::caller_credential_headers(&pool, TENANT_A, GrantUser::Author, id, "api_key_auth").await.unwrap();
    assert_eq!(headers, ["X-Team-Key", "authorization"]);
    let err = weft_access_store::caller_credential_headers(&pool, TENANT_B, GrantUser::Author, id, "api_key_auth").await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");
    let err = weft_access_store::caller_credential_headers(&pool, TENANT_A, GrantUser::Author, id, "slack").await.unwrap_err();
    assert!(err.to_string().contains("wire the right service"), "{err}");

    // A recipe with no `verify` block gates nothing, as a real call
    // through it is refused.
    let plain: AccessSpec = serde_json::from_value(json!({
        "service": "pasted",
        "acquisition": { "kind": "static", "fields": [{ "name": "token" }] }
    }))
    .expect("a valid recipe");
    let done = connect_direct(
        &pool,
        TENANT_A,
        ConnectDirect {
            paste: false,
            spec: plain,
            door: Door::Own,
            registration: None,
            values: [("token".to_string(), "t".to_string())].into_iter().collect(),
            label: None,
            permissions: Vec::new(),
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("a pasted connection stores without a test call");
    let err = weft_access_store::caller_credential_headers(&pool, TENANT_A, GrantUser::Author, done.grant.id, "pasted").await.unwrap_err();
    assert!(err.to_string().contains("declares no `verify` block"), "{err}");
}

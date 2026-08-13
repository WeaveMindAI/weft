//! Layer-3 contract tests for the access store: the real store code
//! against a fresh Postgres (via `#[sqlx::test]`) and a hand-rolled
//! in-process fake provider (token endpoint + API), so every flow runs
//! end to end without touching a real third party.
//!
//! Gated behind the `db-tests` feature (off by default) so a plain
//! `cargo test --workspace` is green without Postgres. Run with
//! `cargo test -p weft-access-store --features db-tests` and
//! `$DATABASE_URL` set.
#![cfg(feature = "db-tests")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;
use sqlx::PgPool;

use weft_access_store::{
    begin_oauth, complete_oauth, connect_direct, delete_grant, list_grants, lookup,
    resolve_for_worker, take_connect_result, AccessError, BeginOAuth, ConnectDirect,
};
use weft_core::access::spec::Door;
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
        })
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
                    axum::Json(resp)
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
                    let mut resp = json!({ "access_token": *s.next_token.lock().unwrap() });
                    if let Some(e) = *s.expires_in.lock().unwrap() {
                        resp["expires_in"] = json!(e);
                    }
                    axum::Json(resp)
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
                       "auth_url": format!("{base}/authorize"), "pkce": true },
            "token_url": format!("{base}/token")
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

/// Begin + complete one consent for `tenant`, returning the grant id.
async fn full_consent(
    pool: &PgPool,
    tenant: &str,
    spec: &AccessSpec,
    scopes: &[&str],
    upgrade: Option<uuid::Uuid>,
) -> anyhow::Result<weft_access_store::GrantSummary> {
    let started = begin_oauth(
        pool,
        tenant,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Own,
            registration: Some(oauth_app()),
            permissions: scopes.iter().map(|s| s.to_string()).collect(),
            project_id: Some("proj-1".into()),
            upgrade_grant_id: upgrade,
            redirect_uri: "http://disp.example/access/oauth/callback".into(),
        },
    )
    .await?;
    assert!(started.consent_url.contains("code_challenge"), "{}", started.consent_url);
    let done = complete_oauth(pool, &started.state, "the-code").await?;
    // The parked outcome is consumable exactly once, by the owning tenant.
    let parked = take_connect_result(pool, tenant, &started.state).await?;
    assert!(parked.is_some(), "outcome parked for the editor's poll");
    assert!(take_connect_result(pool, tenant, &started.state).await?.is_none());
    Ok(done.grant)
}

/// A paste connect on a CONSENT service (the `own_page.paste`
/// section): the stored snapshot is the static variant, so the row
/// resolves like a natively pasted credential and never refreshes
/// through an app.
#[sqlx::test]
async fn paste_connect_on_a_consent_service_stores_the_static_variant(pool: PgPool) {
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
        },
    )
    .await
    .expect("a pasted credential connects without an app");
    assert_eq!(done.grant.identity.as_deref(), Some("quentin"), "test call captured identity");

    // The row resolves like any static credential: the pasted token
    // rides the auth step, no app, no refresh.
    let resolved = weft_access_store::resolve_for_worker(
        &pool,
        TENANT_A,
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
        },
    )
    .await
    .expect_err("no paste section declared");
    assert!(err.to_string().contains("no paste section"), "{err}");
}

#[sqlx::test]
async fn static_connect_runs_the_test_call_and_stores_identity(pool: PgPool) {
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
            project_id: Some("proj-1".into()),
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
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("test call"), "{err}");
    assert_eq!(list_grants(&pool, TENANT_A, Some("fakestatic")).await.unwrap().len(), 1);
}

#[sqlx::test]
async fn the_tenant_wall_holds_on_every_surface(pool: PgPool) {
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
        },
    )
    .await
    .unwrap();
    let id = done.grant.id;

    // Another tenant cannot list, resolve, or delete the grant;
    // resolution answers NOT FOUND (no existence leak). A lookup
    // starts from that same resolve, so the wall covers it too.
    assert!(list_grants(&pool, TENANT_B, None).await.unwrap().is_empty());
    let err = resolve_for_worker(&pool, TENANT_B, id, "fakestatic", &[], &[]).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");
    assert!(delete_grant(&pool, TENANT_B, id).await.is_err());

    // The owner resolves fine, and receives what the connection
    // holds: the pasted token AND the value its test call captured.
    // Visibility is "everything except the store's keep-alive
    // material", not "only what the auth steps interpolate".
    let resolved = resolve_for_worker(&pool, TENANT_A, id, "fakestatic", &[], &[]).await.unwrap();
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-abc"));
    assert_eq!(resolved.values.get("user").map(String::as_str), Some("quentin"));
    assert!(
        !resolved.values.contains_key("refresh_token"),
        "the store's keep-alive material never travels"
    );
    assert_eq!(resolved.identity.as_deref(), Some("quentin"));

    // A wrong-service marker is refused by name.
    let err = resolve_for_worker(&pool, TENANT_A, id, "slack", &[], &[]).await.unwrap_err();
    assert!(err.to_string().contains("wire the right service"), "{err}");
}

#[sqlx::test]
async fn oauth_consent_records_granted_scopes_and_enforces_the_echo(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    let grant = full_consent(&pool, TENANT_A, &spec, &["read", "write"], None).await.unwrap();
    assert_eq!(grant.scopes, vec!["read", "write"], "no echo = the ticked set, recorded claimed");
    assert!(!grant.permissions_verified, "no provider answer = claimed, never verified");
    assert_eq!(grant.project_id.as_deref(), Some("proj-1"), "coexisting = per project");

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
            upgrade_grant_id: None,
            redirect_uri: "http://disp.example/cb".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("accessApps"), "{err}");
}

#[sqlx::test]
async fn an_exclusive_grant_rotates_in_place_and_upgrades_by_union(pool: PgPool) {
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
    assert_eq!(list_grants(&pool, TENANT_A, Some("fakeoauth")).await.unwrap().len(), 1);

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
        resolve_for_worker(&pool, TENANT_A, first.id, "fakeoauth", &["admin".to_string()], &[])
            .await
            .unwrap_err();
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })),
        "{err}"
    );
    resolve_for_worker(&pool, TENANT_A, first.id, "fakeoauth", &["write".to_string()], &[])
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
            project_id: Some("proj-1".into()),
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
            project_id: Some("proj-1".into()),
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

#[sqlx::test]
async fn refresh_is_lazy_single_flight_and_writes_back_rotated_tokens(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    // The consent's token EXPIRES IMMEDIATELY and carries a refresh
    // token, so the very next resolution must refresh.
    *fake.next_refresh.lock().unwrap() = Some("refresh-1".into());
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let consent_tokens = fake.token_requests.load(Ordering::SeqCst);

    // The refresh rotates both tokens (Xero-class single-use refresh).
    *fake.next_token.lock().unwrap() = "tok-fresh".into();
    *fake.next_refresh.lock().unwrap() = Some("refresh-2".into());
    *fake.expires_in.lock().unwrap() = Some(3600);

    // Parallel resolutions: the row lock is the single-flight, so the
    // provider sees EXACTLY ONE refresh request.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let id = grant.id;
        handles.push(tokio::spawn(async move {
            resolve_for_worker(&pool, TENANT_A, id, "fakeoauth", &[], &[]).await
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

/// A declared renewal call (Meta's long-lived-token exchange shape)
/// replaces the standard refresh POST entirely: the stale grant is
/// renewed through the recipe's own GET (interpolating the current
/// token and the app's client id), its captures write the fresh
/// token back, and the answer's expires_in sets the next expiry.
#[sqlx::test]
async fn a_declared_renewal_call_replaces_the_standard_refresh(pool: PgPool) {
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
    let consent_tokens = fake.token_requests.load(Ordering::SeqCst);

    *fake.next_token.lock().unwrap() = "tok-long-lived".into();
    *fake.expires_in.lock().unwrap() = Some(3600);
    let resolved = resolve_for_worker(&pool, TENANT_A, grant.id, "fakeoauth", &[], &[])
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

#[sqlx::test]
async fn a_revoked_refresh_fails_loud_with_the_reconnect_affordance(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    // Expiring token, NO refresh token issued: the grant cannot renew.
    *fake.next_refresh.lock().unwrap() = None;
    *fake.expires_in.lock().unwrap() = Some(1);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let err =
        resolve_for_worker(&pool, TENANT_A, grant.id, "fakeoauth", &[], &[]).await.unwrap_err();
    match err.downcast_ref::<AccessError>() {
        Some(AccessError::NeedsReconnect { reason, .. }) => {
            assert!(reason.contains("reconnect"), "{reason}");
        }
        other => panic!("expected NeedsReconnect, got {other:?} ({err})"),
    }
}

#[sqlx::test]
async fn lookups_run_authed_substitute_and_paginate(pool: PgPool) {
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
        },
    )
    .await
    .unwrap();

    let lookup_spec: weft_core::node::Lookup = serde_json::from_value(json!({
        "get": format!("{base}/channels?q={{query}}&team={{team}}"),
        "items": "channels", "label": "name", "value": "id",
        "page": { "cursor_param": "cursor", "cursor_path": "meta.next" }
    }))
    .unwrap();
    let parents: BTreeMap<String, String> =
        [("team".to_string(), "T1".to_string())].into_iter().collect();
    let resolved =
        resolve_for_worker(&pool, TENANT_A, done.grant.id, "fakestatic", &[], &[]).await.unwrap();
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
#[sqlx::test]
async fn a_shared_key_connect_stores_a_runtime_owned_row(pool: PgPool) {
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
        },
    )
    .await
    .expect("the shared door needs no pasted values");
    assert_eq!(done.grant.owner, CredentialOwner::Ours);
    assert_eq!(done.grant.door, Door::Shared);
    assert!(fake.calls.lock().unwrap().is_empty(), "nothing to validate = no provider call");

    let resolved =
        resolve_for_worker(&pool, TENANT_A, done.grant.id, "fakestatic", &[], &[]).await.unwrap();
    assert_eq!(resolved.owner, CredentialOwner::Ours);
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
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("door"), "{err}");
}

/// A CLAIMED (unverified) permission set never blocks resolution: a
/// pasted credential on a service that reports nothing must resolve
/// whatever a consumer requires (nobody actually knows what it holds).
#[sqlx::test]
async fn a_claimed_shortfall_is_let_through_at_resolution(pool: PgPool) {
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
        },
    )
    .await
    .unwrap();
    assert!(!done.grant.permissions_verified);
    resolve_for_worker(
        &pool,
        TENANT_A,
        done.grant.id,
        "fakestatic",
        &["anything".to_string()],
        &[],
    )
    .await
    .expect("an unknown granted set never blocks");
}


/// A `granted` resource-source read runs the SAME walled read as
/// every resolution (tenant wall, service check) and hands out only
/// the id/label pairs of the named capture, off the FULL stored map
/// (the worker-handoff filter would drop a captured list).
#[sqlx::test]
async fn granted_items_read_the_stored_list_behind_the_wall(pool: PgPool) {
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
        },
    )
    .await
    .unwrap();
    let id = done.grant.id;

    let items =
        weft_access_store::granted_items(&pool, TENANT_A, id, "fakestatic", "channels", "name", "id")
            .await
            .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!((items[0].id.as_str(), items[0].label.as_str()), ("C1", "one"));

    // A name the connection recorded nothing under is an empty list
    // (the editor falls through), not an error.
    assert!(
        weft_access_store::granted_items(&pool, TENANT_A, id, "fakestatic", "gone", "name", "id")
            .await
            .unwrap()
            .is_empty()
    );

    // The tenant wall and the service check hold like every read.
    let err = weft_access_store::granted_items(
        &pool, TENANT_B, id, "fakestatic", "channels", "name", "id",
    )
    .await
    .unwrap_err();
    assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NotFound)), "{err}");
    let err = weft_access_store::granted_items(
        &pool, TENANT_A, id, "slack", "channels", "name", "id",
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
                    "route_by": "subscription",
                    "id_path": "header:X-Chan-Id",
                    "token_path": "header:X-Chan-Token",
                    "subscribe": {
                        "subscribe": {
                            "url": format!("{base}/watch/{{target}}"),
                            "method": "POST",
                            "body": { "id": "{subscription_id}",
                                      "address": "{receiver_url}",
                                      "token": "{subscription_token}" },
                            "auth": [{ "kind": "header", "name": "Authorization",
                                       "value": "Bearer {token}" }],
                            "captures": [
                                { "name": "expires_at", "path": "expiration" },
                                { "name": "resource_id", "path": "resourceId" }
                            ]
                        },
                        "unsubscribe": {
                            "url": format!("{base}/stop"),
                            "method": "POST",
                            "body": { "id": "{subscription_id}",
                                      "resourceId": "{resource_id}" },
                            "auth": [{ "kind": "header", "name": "Authorization",
                                       "value": "Bearer {token}" }]
                        },
                        "renew_margin_secs": 60
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
#[sqlx::test]
async fn a_connect_records_the_provider_account_for_event_routing(pool: PgPool) {
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
#[sqlx::test]
async fn a_push_routes_only_to_connections_carrying_the_verifying_recipe(pool: PgPool) {
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
#[sqlx::test]
async fn subscriptions_subscribe_once_and_stop_at_the_provider(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = events_spec(&base);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();

    // The event-source resolve: recipe + the values its calls name.
    let source =
        weft_access_store::resolve_event_source(&pool, TENANT_A, grant.id, "fakeoauth", &[])
            .await
            .unwrap();
    assert!(source.events.contains_key("things"));
    assert_eq!(source.provider_account.as_deref(), Some("quentin"));

    let req = weft_access_store::EnsureSubscription {
        tenant: TENANT_A.into(),
        service: "fakeoauth".into(),
        topic: "things".into(),
        access_id: grant.id,
        signal_token: "sig-1".into(),
        params: [("target".to_string(), "file-9".to_string())].into_iter().collect(),
        receiver_url: Some("https://public.example/events/fakeoauth/things".into()),
    };
    let ensured = weft_access_store::ensure_subscription(&pool, &req).await.unwrap();
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
    let row = sqlx::query_as::<_, (String,)>(
        "SELECT id FROM signal_subscription WHERE signal_token = 'sig-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
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
    assert_eq!(sub.captures.get("resource_id").map(String::as_str), Some("res-file-9"));
    assert!(watch_calls[0].contains(&sub.token), "the minted token traveled to the provider");

    // A healthy subscription is idempotent: no second provider call.
    weft_access_store::ensure_subscription(&pool, &req).await.unwrap();
    let watch_count =
        fake.calls.lock().unwrap().iter().filter(|c| c.starts_with("/watch/")).count();
    assert_eq!(watch_count, 1, "far from expiry, ensure re-subscribes nothing");

    // Dropping stops the channel at the provider (with the captured
    // resource id) and forgets the row.
    let dropped =
        weft_access_store::drop_subscriptions_for_signal(&pool, TENANT_A, "sig-1").await.unwrap();
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
#[sqlx::test]
async fn a_subscribe_topic_without_a_public_address_teaches_the_fix(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = events_spec(&base);
    let grant = full_consent(&pool, TENANT_A, &spec, &["read"], None).await.unwrap();
    let err = weft_access_store::ensure_subscription(
        &pool,
        &weft_access_store::EnsureSubscription {
            tenant: TENANT_A.into(),
            service: "fakeoauth".into(),
            topic: "things".into(),
            access_id: grant.id,
            signal_token: "sig-x".into(),
            params: [("target".to_string(), "f".to_string())].into_iter().collect(),
            receiver_url: None,
        },
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("--public-url"), "{msg}");
    assert!(msg.contains("docs/event-triggers.md"), "{msg}");
    assert!(
        fake.calls.lock().unwrap().iter().all(|c| !c.starts_with("/watch/")),
        "no provider channel was opened"
    );
}

/// A connection to a service with no events section still resolves
/// for the listener (a held raw pipe needs only the stored values);
/// the recipe map simply comes back empty. A subscription needing a
/// topic is refused by its own topic lookup, not here.
#[sqlx::test]
async fn an_eventless_service_resolves_with_an_empty_recipe_map(pool: PgPool) {
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
        },
    )
    .await
    .unwrap();
    let source =
        weft_access_store::resolve_event_source(&pool, TENANT_A, done.grant.id, "fakestatic", &[])
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
#[sqlx::test]
async fn a_shared_door_grant_never_serves_events_with_the_operators_app_values(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;

    // The events fixture, with a socket recipe whose mint call names
    // {app_token}: exactly the value an operator might (wrongly or
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
                           "value": "Bearer {app_token}" }],
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
        .insert("app_token".to_string(), "xapp-the-operators".to_string());

    // Two grants to the same app snapshot: one through each door.
    for (id, door) in [(uuid::Uuid::new_v4(), "shared"), (uuid::Uuid::new_v4(), "own")] {
        sqlx::query(
            "INSERT INTO access_grant
               (id, tenant_id, service, registration_sealed, spec_json, values_sealed,
                granted_scopes, door, identity)
             VALUES ($1, $2, 'fakeoauth', $3, $4, $5, '[]', $6, 'x')",
        )
        .bind(id)
        .bind(TENANT_A)
        .bind(weft_access_store::seal_json(&serde_json::to_value(&registration).unwrap()).unwrap())
        .bind(serde_json::to_value(&spec).unwrap())
        .bind(weft_access_store::seal_json(&json!({ "token": "tok-1" })).unwrap())
        .bind(door)
        .execute(&pool)
        .await
        .unwrap();
        let source =
            weft_access_store::resolve_event_source(&pool, TENANT_A, id, "fakeoauth", &[])
                .await
                .unwrap();
        match door {
            "shared" => assert!(
                !source.recipe_values.contains_key("app_token"),
                "the operator's app token must never serve a shared-door connection"
            ),
            _ => assert_eq!(
                source.recipe_values.get("app_token").map(String::as_str),
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
#[sqlx::test]
async fn a_shared_app_connection_never_carries_the_operators_app_secret(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let fake = FakeProvider::new();
    let base = fake.serve().await;
    let spec = oauth_spec(&base, "coexisting");
    *fake.next_refresh.lock().unwrap() = Some("rt-the-users".into());

    // A shared-door consent through the operator's app.
    let started = begin_oauth(
        &pool,
        TENANT_A,
        BeginOAuth {
            spec: spec.clone(),
            door: Door::Shared,
            registration: Some(oauth_app()),
            permissions: vec!["read".into()],
            project_id: Some("proj-1".into()),
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
    let (values_sealed,): (String,) =
        sqlx::query_as("SELECT values_sealed FROM access_grant WHERE id = $1")
            .bind(done.grant.id)
            .fetch_one(&pool)
            .await
            .unwrap();
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
        resolve_for_worker(&pool, TENANT_A, done.grant.id, "fakeoauth", &[], &[]).await.unwrap();
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
#[sqlx::test]
async fn optional_field_groups_gate_what_a_connection_can_do(pool: PgPool) {
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
        TENANT_A,
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
        TENANT_A,
        done.grant.id,
        "fakemail",
        &[],
        &["out_host".to_string()],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("out_host"), "{err}");
    assert!(
        matches!(err.downcast_ref::<AccessError>(), Some(AccessError::NeedsReconnect { .. })),
        "{err}"
    );

    // The names of what it stores ride the summary (never the values),
    // which is what the editor's live check reads.
    let listed = weft_access_store::list_grants(&pool, TENANT_A, Some("fakemail")).await.unwrap();
    let names = &listed[0].value_names;
    assert!(names.contains(&"in_host".to_string()));
    assert!(!names.contains(&"out_host".to_string()), "a blank optional stores nothing");
}

/// A finished pick lands the session's parked `grants` on the grant
/// row: choosing a resource through the provider's chooser GRANTS the
/// declared permissions, so they union into `granted_scopes` (no
/// duplicates) with `permissions_verified` untouched, and the editor's
/// shortfall check (which reads the summary's scopes) sees them.
#[sqlx::test]
async fn a_finished_pick_lands_its_grants_on_the_row(pool: PgPool) {
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
    let listed = list_grants(&pool, TENANT_A, Some("fakestatic")).await.unwrap();
    let row = listed.iter().find(|g| g.id == grant.id).unwrap();
    let mut scopes = row.scopes.clone();
    scopes.sort();
    assert_eq!(scopes, vec!["base.read".to_string(), "drive.file".to_string()]);
    assert!(!row.permissions_verified, "the pick never upgrades verification");
}

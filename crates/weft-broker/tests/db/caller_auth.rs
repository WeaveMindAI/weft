//! Layer-3 contract tests for `caller_auth::verify_caller`: the real
//! check against a REAL Postgres holding a stored connection, so the
//! whole chain (tenant wall, the connection's service, the stored
//! material, the route's own check) runs end to end. The identity-token scheme
//! needs an issuer's published keys over the network, so only what it
//! refuses before any fetch is covered here; the shared-key and HMAC
//! schemes are covered whole.

use serde_json::json;
use weft_task_store::db::Pool;

use weft_access_store::connect_direct;
use weft_core::install::InstallKind;
use weft_broker::caller_auth::{verify_caller, CallerRefusal, HeldVerifiers};
use weft_broker_client::protocol::CallerVerifyRequest;
use weft_core::access::callers::{named_check, CallerScheme};
use weft_core::access::events::VerifyKind;
use weft_core::access::spec::Door;
use weft_core::access::wire::ConnectDirect;
use weft_core::AccessSpec;

const TENANT: &str = "tenant-a";

async fn schema(pool: &Pool) {
    weft_task_store::apply_groups(pool, &[&weft_access_store::GROUP]).await.expect("schema");
}

/// A fresh held copy of the verifiers with nothing kept yet, so every
/// check reads its connection from the database.
fn verifiers() -> std::sync::Arc<HeldVerifiers> {
    HeldVerifiers::unfollowed(16, |_| true)
}

/// A route's check, as its `auth` writes it.
fn check(record: serde_json::Value) -> VerifyKind {
    serde_json::from_value(record).expect("a valid check")
}

async fn store(pool: &Pool, tenant: &str, spec: AccessSpec, values: &[(&str, &str)]) -> String {
    let done = connect_direct(
        pool,
        InstallKind::Local,
        tenant,
        ConnectDirect {
            paste: false,
            way: None,
            spec,
            door: Door::Own,
            registration: None,
            values: values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            label: Some("gate".into()),
            permissions: vec![],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("a pasted verifier connection stores without a test call");
    done.grant.id.to_string()
}

fn request(
    tenant: &str,
    access_id: &str,
    verify: &VerifyKind,
    headers: &[(&str, &str)],
    body: &[u8],
) -> CallerVerifyRequest {
    use base64::Engine as _;
    CallerVerifyRequest {
        tenant: tenant.into(),
        access_id: access_id.into(),
        for_instance: None,
        service: CallerScheme::of(verify).expect("a caller check").service().into(),
        verify: verify.clone(),
        method: "POST".into(),
        path: "chat/room7".into(),
        url: Some("https://acme.example/chat/room7".into()),
        headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        body_b64: base64::engine::general_purpose::STANDARD.encode(body),
    }
}

fn refused(r: Result<weft_broker_client::protocol::CallerVerified, CallerRefusal>) -> String {
    match r {
        Err(CallerRefusal::Refused(why)) => why,
        Err(CallerRefusal::Failed(e)) => panic!("expected a refusal, got a failure: {e:#}"),
        Err(CallerRefusal::Malformed(why)) => panic!("expected a refusal, got a malformed ask: {why}"),
        Ok(verified) => panic!("expected a refusal, got {}", verified.identity),
    }
}

/// A key from the stored list verifies and names its position; the
/// bearer slot works too; a wrong key and a missing key refuse.
#[weft_db_macros::test]
async fn a_stored_key_admits_and_names_which_one(pool: Pool) {
    schema(&pool).await;
    let keys = named_check("api_keys").unwrap().verify();
    let id = store(&pool, TENANT, CallerScheme::ApiKeys.recipe(), &[("keys", "first-key\nsecond-key\n")]).await;

    let ok = verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &keys, &[("x-api-key", "second-key")], b""), 0)
        .await
        .expect("the second key verifies");
    assert_eq!(ok.identity, json!({ "key": 1 }));
    assert_eq!(ok.credential_headers, ["X-Api-Key", "authorization"], "the run never writes the key down");
    let ok = verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &keys, &[("authorization", "Bearer first-key")], b""), 0)
        .await
        .expect("the bearer slot works");
    assert_eq!(ok.identity, json!({ "key": 0 }));

    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &keys, &[("x-api-key", "nope")], b""), 0).await);
    assert!(why.contains("does not match"), "{why}");
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &keys, &[], b""), 0).await);
    assert!(why.contains("X-Api-Key"), "{why}");

    // The same connection under a route that reads its key elsewhere: the
    // route's check decides, the connection only holds the keys.
    let team = check(json!({ "kind": "api_keys", "header": "X-Team-Key", "bearer": false }));
    let ok = verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &team, &[("x-team-key", "first-key")], b""), 0)
        .await
        .expect("the route's own header verifies");
    assert_eq!(ok.credential_headers, ["X-Team-Key"]);
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &team, &[("authorization", "Bearer first-key")], b""), 0).await);
    assert!(why.contains("X-Team-Key"), "{why}");
}

/// An HMAC over the exact body with the connection's secret verifies
/// (an anonymous identity); a tampered body refuses.
#[weft_db_macros::test]
async fn a_signed_body_admits_with_the_stored_secret(pool: Pool) {
    schema(&pool).await;
    let verify = json!({
        "kind": "hmac",
        "signature": { "in": "header:X-Signature" },
        "timestamp": { "in": "header:X-Timestamp" },
        "concat": "{timestamp}.{body}"
    });
    assert_eq!(check(verify.clone()), named_check("hmac_sha256").unwrap().verify(), "plain hmac is this layout");
    let id = store(&pool, TENANT, CallerScheme::Hmac.recipe(), &[("signingSecret", "s3cret")]).await;
    let now = 1_700_000_000;
    let body = br#"{"text":"hi"}"#;
    let signature = sign(&verify, "s3cret", Some("1700000000"), body, &[], None);
    let headers = [("x-signature", signature.as_str()), ("x-timestamp", "1700000000")];
    let hmac = check(verify);
    let ok = verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &hmac, &headers, body), now)
        .await
        .expect("a genuine signature verifies");
    assert_eq!(ok.identity, json!({}));
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &hmac, &headers, b"{}"), now).await);
    assert!(why.contains("does not match"), "{why}");
}

/// The digest `verify` expects under `secret`, as a sender writes it.
fn sign(
    verify: &serde_json::Value,
    secret: &str,
    timestamp: Option<&str>,
    body: &[u8],
    headers: &[(String, String)],
    url: Option<&str>,
) -> String {
    let kind: weft_core::access::events::VerifyKind = serde_json::from_value(verify.clone()).expect("a valid scheme");
    weft_core::access::verify::hmac_digest(
        &kind,
        secret,
        timestamp,
        &weft_core::access::verify::PushParts { body, headers, url, method: "POST" },
    )
    .expect("signs")
}

/// Twilio's scheme gating a Route: the signature covers the full
/// address the caller was told to call, query included, so the gate
/// verifies only with the url the door hands it. The vector is Twilio's
/// own SDK test (twilio-python, tests/unit/test_request_validator.py).
#[weft_db_macros::test]
async fn a_route_gated_by_a_url_signing_scheme_verifies_the_address_called(pool: Pool) {
    schema(&pool).await;
    let twilio = check(json!({
        "kind": "hmac",
        "signature": { "in": "header:X-Twilio-Signature" },
        "concat": "{url}{sorted_form_params}",
        "algorithm": "sha1",
        "encoding": "base64"
    }));
    let id = store(&pool, TENANT, CallerScheme::Hmac.recipe(), &[("signingSecret", "12345")]).await;
    let body = b"CallSid=CA1234567890ABCDE&Digits=1234&From=%2B14158675309&To=%2B18005551212&Caller=%2B14158675309";
    let headers = [("x-twilio-signature", "RSOYDt4T1cUTdK1PDd93/VVr8B8=")];
    let mut req = request(TENANT, &id, &twilio, &headers, body);
    req.url = Some("https://mycompany.com/myapp.php?foo=1&bar=2".into());
    let ok = verify_caller(&verifiers(), &pool, InstallKind::Local, &req, 0).await.expect("the address called verifies");
    assert_eq!(ok.identity, json!({}));

    req.url = Some("https://mycompany.com/myapp.php".into());
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &req, 0).await);
    assert!(why.contains("does not match"), "{why}");
    req.url = None;
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &req, 0).await);
    assert!(why.contains("public receiver address"), "{why}");
}

/// The Standard Webhooks scheme (Svix, Resend, Whop), as a Route's `auth`
/// record writes it, against the
/// reference library's own vector; a signature header sent twice gives
/// the gate every value, so the right one verifies whichever copy
/// carries it.
#[weft_db_macros::test]
async fn a_standard_webhooks_signature_admits_from_any_header_copy(pool: Pool) {
    schema(&pool).await;
    let record = json!({
        "kind": "hmac",
        "signature": { "in": "header:webhook-signature", "list": " ", "pair": ",", "key": "v1" },
        "timestamp": { "in": "header:webhook-timestamp" },
        "concat": "{header:webhook-id}.{timestamp}.{body}",
        "secret": { "stripPrefix": "whsec_", "decode": "base64" },
        "encoding": "base64"
    });
    let standard = weft_core::access::callers::parse_auth(Some(&record)).expect("a valid auth").expect("a check");
    let id = store(&pool, TENANT, CallerScheme::Hmac.recipe(), &[("signingSecret", "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")]).await;
    let body = br#"{"test": 2432232314}"#;
    let ts = 1614265330;
    let base = [("webhook-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"), ("webhook-timestamp", "1614265330")];
    let with = |signatures: &[&str]| {
        let mut h: Vec<(&str, &str)> = base.to_vec();
        h.extend(signatures.iter().map(|s| ("webhook-signature", *s)));
        request(TENANT, &id, &standard, &h, body)
    };
    let ok = verify_caller(&verifiers(), &pool, InstallKind::Local, &with(&["v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="]), ts)
        .await
        .expect("the reference vector verifies");
    assert_eq!(ok.identity, json!({}));
    assert_eq!(ok.credential_headers, ["webhook-signature"]);
    verify_caller(&verifiers(), &pool, InstallKind::Local, &with(&["v1,bm90IGl0", "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="]), ts)
        .await
        .expect("the second copy of the header verifies");
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &with(&["v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OA="]), ts).await);
    assert!(why.contains("does not match"), "{why}");
    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &with(&["v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="]), ts + 301).await);
    assert!(why.contains("replay"), "{why}");
}

/// The tenant wall and a connection of another service refuse; a check
/// that does not read the service it names, or cannot check a caller, is
/// the asker's mistake (a failure of ours is the only other answer).
#[weft_db_macros::test]
async fn the_wall_the_service_and_a_wrong_check_all_refuse(pool: Pool) {
    schema(&pool).await;
    let keys = named_check("api_keys").unwrap().verify();
    let id = store(&pool, TENANT, CallerScheme::ApiKeys.recipe(), &[("keys", "k")]).await;
    let headers = [("x-api-key", "k")];

    let why = refused(verify_caller(&verifiers(), &pool, InstallKind::Local, &request("tenant-b", &id, &keys, &headers, b""), 0).await);
    assert!(why.contains("no such connection"), "{why}");

    // An hmac check against the api_keys connection: the store refuses a
    // connection of another service than the one asked for.
    let hmac = named_check("hmac_sha256").unwrap().verify();
    match verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &hmac, &headers, b""), 0).await {
        Err(CallerRefusal::Failed(e)) => assert!(format!("{e:#}").contains("hmac_auth"), "{e:#}"),
        other => panic!("a wrong service is a loud failure, got {other:?}"),
    }

    let mut mismatched = request(TENANT, &id, &keys, &headers, b"");
    mismatched.service = "hmac_auth".into();
    match verify_caller(&verifiers(), &pool, InstallKind::Local, &mismatched, 0).await {
        Err(CallerRefusal::Malformed(why)) => assert!(why.contains("api_keys") && why.contains("hmac_auth"), "{why}"),
        other => panic!("a check naming another service is the asker's mistake, got {other:?}"),
    }
    let mut echo = request(TENANT, &id, &keys, &headers, b"");
    echo.verify = check(json!({ "kind": "token_echo" }));
    match verify_caller(&verifiers(), &pool, InstallKind::Local, &echo, 0).await {
        Err(CallerRefusal::Malformed(why)) => assert!(why.contains("token_echo"), "{why}"),
        other => panic!("a check that cannot check a caller is the asker's mistake, got {other:?}"),
    }

    match verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, "not-a-uuid", &keys, &headers, b""), 0).await {
        Err(CallerRefusal::Malformed(why)) => assert!(why.contains("not-a-uuid"), "{why}"),
        other => panic!("a malformed id is the asker's mistake, got {other:?}"),
    }
}

/// An identity-token gate (`auth: "oidc"`): the connection stores the
/// issuer, its key address and the audience.
fn jwt_spec() -> AccessSpec {
    CallerScheme::Oidc.recipe()
}

/// A JWT gate needs an audience: the issuer signs tokens for every app it
/// serves, and the audience is the only thing telling this gate's apart.
/// A connection cannot be saved without one.
#[weft_db_macros::test]
async fn a_jwt_gate_connection_cannot_be_saved_without_an_audience(pool: Pool) {
    schema(&pool).await;
    let unsaved = connect_direct(
        &pool,
        InstallKind::Local,
        TENANT,
        ConnectDirect {
            paste: false,
            way: None,
            spec: jwt_spec(),
            door: Door::Own,
            registration: None,
            values: [("issuer", "https://auth.example"), ("jwksUrl", "https://auth.example/jwks.json")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            label: Some("gate".into()),
            permissions: vec![],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect_err("no audience, no connection");
    assert!(format!("{unsaved:#}").contains("audience"), "{unsaved:#}");
}

/// A JWT gate whose stored key address is plain http is refused before
/// any key is fetched: keys fetched over http could be swapped by anyone
/// on the path.
#[weft_db_macros::test]
async fn a_jwt_gate_with_an_http_key_address_is_refused(pool: Pool) {
    schema(&pool).await;
    let values = [("issuer", "https://auth.example"), ("jwksUrl", "http://auth.example/jwks.json"), ("audience", "my-app")];
    let id = store(&pool, TENANT, jwt_spec(), &values).await;
    let why = refused(
        verify_caller(&verifiers(), &pool, InstallKind::Local, &request(TENANT, &id, &named_check("oidc").unwrap().verify(), &[("authorization", "Bearer a.b.c")], b""), 0).await,
    );
    assert!(why.contains("not an https:// address"), "{why}");
}

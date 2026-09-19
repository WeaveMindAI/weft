//! Layer-3 contract tests for `caller_auth::verify_caller`: the real
//! check against a REAL Postgres holding a stored connection, so the
//! whole chain (tenant wall, the recipe's `verify` block, the stored
//! material, the scheme) runs end to end. The identity-token scheme
//! needs an issuer's published keys over the network, so it is not
//! covered here; the shared-key and HMAC schemes are.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use std::collections::BTreeMap;

use serde_json::json;
use sqlx::PgPool;

use weft_access_store::connect_direct;
use weft_broker::caller_auth::{verify_caller, CallerRefusal};
use weft_broker_client::protocol::CallerVerifyRequest;
use weft_core::access::spec::Door;
use weft_core::access::wire::ConnectDirect;
use weft_core::AccessSpec;

const TENANT: &str = "tenant-a";

async fn schema(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&weft_access_store::GROUP]).await.expect("schema");
}

/// The shape of a shipped auth access node's recipe: pasted fields, no
/// test call (a verifier never dials out), a `verify` block.
fn auth_spec(service: &str, fields: &[&str], verify: serde_json::Value) -> AccessSpec {
    serde_json::from_value(json!({
        "service": service,
        "acquisition": {
            "kind": "static",
            "fields": fields.iter().map(|f| json!({ "name": f })).collect::<Vec<_>>()
        },
        "verify": verify
    }))
    .expect("a valid auth recipe")
}

async fn store(pool: &PgPool, tenant: &str, spec: AccessSpec, values: &[(&str, &str)]) -> String {
    let done = connect_direct(
        pool,
        tenant,
        ConnectDirect {
            paste: false,
            spec,
            door: Door::Own,
            registration: None,
            values: values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            label: Some("gate".into()),
            permissions: vec![],
            project_id: None,
        },
    )
    .await
    .expect("a pasted verifier connection stores without a test call");
    done.grant.id.to_string()
}

fn request(
    tenant: &str,
    access_id: &str,
    service: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> CallerVerifyRequest {
    use base64::Engine as _;
    CallerVerifyRequest {
        tenant: tenant.into(),
        access_id: access_id.into(),
        service: service.into(),
        method: "POST".into(),
        path: "chat/room7".into(),
        headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        query: BTreeMap::new(),
        body_b64: base64::engine::general_purpose::STANDARD.encode(body),
    }
}

fn refused(r: Result<serde_json::Value, CallerRefusal>) -> String {
    match r {
        Err(CallerRefusal::Refused(why)) => why,
        Err(CallerRefusal::Failed(e)) => panic!("expected a refusal, got a failure: {e:#}"),
        Ok(identity) => panic!("expected a refusal, got {identity}"),
    }
}

/// A key from the stored list verifies and names its position; the
/// bearer slot works too; a wrong key and a missing key refuse.
#[sqlx::test]
async fn a_stored_key_admits_and_names_which_one(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec("api_key_auth", &["keys"], json!({ "kind": "api_keys" }));
    let id = store(&pool, TENANT, spec, &[("keys", "first-key\nsecond-key\n")]).await;

    let ok = verify_caller(&pool, &request(TENANT, &id, "api_key_auth", &[("x-api-key", "second-key")], b""), 0)
        .await
        .expect("the second key verifies");
    assert_eq!(ok, json!({ "key": 1 }));
    let ok = verify_caller(&pool, &request(TENANT, &id, "api_key_auth", &[("authorization", "Bearer first-key")], b""), 0)
        .await
        .expect("the bearer slot works");
    assert_eq!(ok, json!({ "key": 0 }));

    let why = refused(verify_caller(&pool, &request(TENANT, &id, "api_key_auth", &[("x-api-key", "nope")], b""), 0).await);
    assert!(why.contains("does not match"), "{why}");
    let why = refused(verify_caller(&pool, &request(TENANT, &id, "api_key_auth", &[], b""), 0).await);
    assert!(why.contains("X-Api-Key"), "{why}");
}

/// An HMAC over the exact body with the connection's secret verifies
/// (an anonymous identity); a tampered body refuses.
#[sqlx::test]
async fn a_signed_body_admits_with_the_stored_secret(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec(
        "hmac_auth",
        &["signing_secret"],
        json!({
            "kind": "hmac",
            "signature_header": "X-Signature",
            "timestamp_header": "X-Timestamp",
            "concat": "{timestamp}.{body}"
        }),
    );
    let id = store(&pool, TENANT, spec, &[("signing_secret", "s3cret")]).await;
    let now = 1_700_000_000;
    let body = br#"{"text":"hi"}"#;
    let signature = weft_core::access::verify::hmac_signature(
        "s3cret",
        &"{timestamp}.{body}".parse().unwrap(),
        "",
        weft_core::access::events::HmacAlgorithm::Sha256,
        weft_core::access::events::DigestEncoding::Hex,
        now,
        &weft_core::access::verify::PushParts {
            body,
            headers: &BTreeMap::new(),
            url: None,
            method: "POST",
        },
    )
    .unwrap();
    let headers = [("x-signature", signature.as_str()), ("x-timestamp", "1700000000")];
    let ok = verify_caller(&pool, &request(TENANT, &id, "hmac_auth", &headers, body), now)
        .await
        .expect("a genuine signature verifies");
    assert_eq!(ok, json!({}));
    let why = refused(verify_caller(&pool, &request(TENANT, &id, "hmac_auth", &headers, b"{}"), now).await);
    assert!(why.contains("does not match"), "{why}");
}

/// The tenant wall, a wrong service, and a service with no `verify`
/// block all refuse (a failure of ours is the only non-refusal).
#[sqlx::test]
async fn the_wall_the_service_and_a_missing_scheme_all_refuse(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec("api_key_auth", &["keys"], json!({ "kind": "api_keys" }));
    let id = store(&pool, TENANT, spec, &[("keys", "k")]).await;
    let headers = [("x-api-key", "k")];

    let why = refused(verify_caller(&pool, &request("tenant-b", &id, "api_key_auth", &headers, b""), 0).await);
    assert!(why.contains("no such connection"), "{why}");

    match verify_caller(&pool, &request(TENANT, &id, "other_service", &headers, b""), 0).await {
        Err(CallerRefusal::Failed(e)) => assert!(format!("{e:#}").contains("other_service"), "{e:#}"),
        other => panic!("a wrong service is a loud failure, got {other:?}"),
    }

    let plain: AccessSpec = serde_json::from_value(json!({
        "service": "plain",
        "acquisition": { "kind": "static", "fields": [{ "name": "token" }] }
    }))
    .unwrap();
    let plain_id = store(&pool, TENANT, plain, &[("token", "t")]).await;
    let why = refused(verify_caller(&pool, &request(TENANT, &plain_id, "plain", &headers, b""), 0).await);
    assert!(why.contains("no `verify` block"), "{why}");

    match verify_caller(&pool, &request(TENANT, "not-a-uuid", "api_key_auth", &headers, b""), 0).await {
        Err(CallerRefusal::Failed(_)) => {}
        other => panic!("a malformed id is a loud failure, got {other:?}"),
    }
}

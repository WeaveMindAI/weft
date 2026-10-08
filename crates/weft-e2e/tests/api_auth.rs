//! Gated routes: the gateway refuses a caller the route's auth connection
//! does not verify, and admits one it does with the established identity
//! on the trigger's `caller` port. Two schemes end to end (a key set, a
//! signing secret); the overlap refusal at activation rides along.
#![cfg(feature = "e2e")]

use std::collections::BTreeMap;

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::{ensure, live, project::Project};

#[tokio::test]
async fn api_keys_and_a_signing_secret_gate_their_routes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_auth", disp.clone()).await?;
    let base = project.unique_live_path()?;

    // The connections, stored as the editor stores them, stamped onto
    // the access nodes.
    let keys = connect_direct(
        &disp,
        catalog_spec("api", "api_key_auth")?,
        "own",
        json!({ "keys": "k-one, k-two" }),
    )
    .await?;
    set_account(&project, "keys", keys.handle()).await?;
    let secret = connect_direct(
        &disp,
        catalog_spec("api", "hmac_auth")?,
        "own",
        json!({ "signing_secret": "s3cret-e2e" }),
    )
    .await?;
    set_account(&project, "secret", secret.handle()).await?;
    project.activate().await?;

    // The key set: no key and a wrong key are refused before any run
    // starts; a right key admits and the identity says which one.
    let secure = format!("{base}/secure");
    let payload = json!({ "text": "hi" });
    let (status, _, body) = live::http_json(&disp, Method::POST, &secure, &[], &payload).await?;
    assert_eq!(status, 401, "{}", String::from_utf8_lossy(&body));
    let (status, _, _) =
        live::http_json(&disp, Method::POST, &secure, &[("x-api-key", "k-nope")], &payload).await?;
    assert_eq!(status, 401);
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &secure, &[("x-api-key", "k-two")], &payload).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "caller": { "key": 1 }, "text": "hi" }));
    let (status, _, body) = live::http_json(
        &disp,
        Method::POST,
        &secure,
        &[("authorization", "Bearer k-one")],
        &payload,
    )
    .await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v["caller"], json!({ "key": 0 }));

    // The signing secret: the exact bytes are signed with a timestamp;
    // a good signature admits (an anonymous identity), a tampered body
    // and a stale timestamp are refused.
    let signed = format!("{base}/signed");
    let body_bytes = serde_json::to_vec(&json!({ "text": "signed hi" }))?;
    let now = chrono::Utc::now().timestamp();
    let sign = |ts: i64, body: &[u8]| -> anyhow::Result<String> {
        Ok(weft_core::access::verify::hmac_signature(
            "s3cret-e2e",
            &"{timestamp}.{body}".parse().map_err(anyhow::Error::msg)?,
            "",
            weft_core::access::events::HmacAlgorithm::Sha256,
            weft_core::access::events::DigestEncoding::Hex,
            ts,
            &weft_core::access::verify::PushParts {
                body,
                headers: &BTreeMap::new(),
                url: None,
                method: "POST",
            },
        )?)
    };
    let signature = sign(now, &body_bytes)?;
    let ts = now.to_string();
    let (status, _, body) = live::http_request(
        &disp,
        Method::POST,
        &signed,
        &[("content-type", "application/json"), ("x-signature", &signature), ("x-timestamp", &ts)],
        Some(body_bytes.clone()),
    )
    .await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "caller": {}, "text": "signed hi" }));
    let (status, _, _) = live::http_request(
        &disp,
        Method::POST,
        &signed,
        &[("content-type", "application/json"), ("x-signature", &signature), ("x-timestamp", &ts)],
        Some(serde_json::to_vec(&json!({ "text": "tampered" }))?),
    )
    .await?;
    assert_eq!(status, 401, "a signature over other bytes is refused");
    let stale = now - 3600;
    let stale_sig = sign(stale, &body_bytes)?;
    let stale_ts = stale.to_string();
    let (status, _, _) = live::http_request(
        &disp,
        Method::POST,
        &signed,
        &[("content-type", "application/json"), ("x-signature", &stale_sig), ("x-timestamp", &stale_ts)],
        Some(body_bytes),
    )
    .await?;
    assert_eq!(status, 401, "a replay outside the window is refused");

    project.finish().await?;
    keys.finish().await?;
    secret.finish().await
}

/// Two PROJECTS of one account serving the same path. Each project's
/// routes sit under its own id on the install's shared address, so both
/// activate and each call reaches the project its address names.
///
/// Two routes of ONE file colliding is answered earlier, by the compiler
/// (`route-overlap`), so a program that collides with itself never gets
/// this far.
#[tokio::test]
async fn two_projects_serve_the_same_path_each_at_its_own_address() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut first = Project::prepare("api_overlap", disp.clone()).await?;
    // The bare path is what goes in the file; the callable one carries
    // the tenant and project the dispatcher serves it under, and is what
    // a caller dials.
    let path = first.bare_live_path();
    let first_base = first.mount_at(&path)?;
    first.activate().await?;

    // The same path, from a second project of the same account.
    let mut second = Project::prepare("api_overlap", disp.clone()).await?;
    let second_base = second.mount_at(&path)?;
    second.activate().await?;
    anyhow::ensure!(first_base != second_base, "each project is called at its own address");

    for (base, project) in [(&first_base, &first), (&second_base, &second)] {
        let call = format!("{base}/chat/anything");
        let (status, _, _) = live::http_json(&disp, Method::POST, &call, &[], &serde_json::json!({})).await?;
        anyhow::ensure!(status.is_success(), "{} serves its route at {call}: {status}", project.id());
    }

    second.finish().await?;
    first.finish().await
}

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
    set_account(&project, "keys", "account", keys.handle())?;
    let secret = connect_direct(
        &disp,
        catalog_spec("api", "hmac_auth")?,
        "own",
        json!({ "signing_secret": "s3cret-e2e" }),
    )
    .await?;
    set_account(&project, "secret", "account", secret.handle())?;
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

    // The gate and the program are not the same machine. The gate runs
    // at the dispatcher, which answers a route with a redirect to the
    // worker, so a caller can have one request checked and then send a
    // different one down the redirect they were handed. A signature
    // says nothing about who you are and everything about one exact
    // request, so if that worked, signing anything would sign
    // everything.
    //
    // The worker refuses it. The token it was given carries a
    // fingerprint of the request the gate approved, and the worker
    // takes the fingerprint of what actually arrived.
    let good = serde_json::to_vec(&json!({ "text": "pay 1" }))?;
    let now = chrono::Utc::now().timestamp();
    let signature = sign(now, &good)?;
    let ts = now.to_string();
    let headers = [
        ("content-type", "application/json"),
        ("x-signature", signature.as_str()),
        ("x-timestamp", ts.as_str()),
    ];
    let (status, body) = live::redirect_then_send(
        &disp,
        Method::POST,
        &signed,
        &headers,
        good.clone(),
        serde_json::to_vec(&json!({ "text": "pay 1000000" }))?,
    )
    .await?;
    assert_eq!(
        status, 403,
        "the door was opened for another call: {}",
        String::from_utf8_lossy(&body)
    );

    // The same two hops with the request that was actually approved:
    // it goes through. Without this the check above would pass just as
    // well on a worker that refused every redirected call.
    let (status, body) =
        live::redirect_then_send(&disp, Method::POST, &signed, &headers, good.clone(), good)
            .await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "caller": {}, "text": "pay 1" }));

    project.finish().await?;
    keys.finish().await?;
    secret.finish().await
}

/// Two PROJECTS of one account claiming one address. Each file is fine
/// on its own, so the compiler cannot see this pair; only the
/// dispatcher, which holds every project's claims in one table, can.
/// The second activation is refused naming the overlap, before any
/// route of it is armed, and the first keeps serving.
///
/// The half inside ONE file is answered earlier, by the compiler
/// (`route-overlap`), so a program that collides with itself never gets
/// this far.
#[tokio::test]
async fn overlapping_routes_are_refused_at_activation() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut first = Project::prepare("api_overlap", disp.clone()).await?;
    // The bare path is what goes in the file; the callable one carries
    // the tenant the dispatcher serves it under, and is what a caller
    // dials.
    let path = first.bare_live_path();
    let base = first.mount_at(&path)?;
    first.activate().await?;

    // The first project's route answers before the collision, so a
    // failure after it cannot be blamed on the route never having
    // worked.
    let call = format!("{base}/chat/anything");
    let (status, _, _) =
        live::http_json(&disp, Method::POST, &call, &[], &serde_json::json!({})).await?;
    anyhow::ensure!(status.is_success(), "the first project serves its route: {status}");

    // The same address, from a second project of the same account.
    let mut second = Project::prepare("api_overlap", disp.clone()).await?;
    second.mount_at(&path)?;
    let out = second.activate_refused().await?;
    anyhow::ensure!(
        out.contains("neither is the more specific"),
        "the refusal says why the pair cannot stand: {out}"
    );

    // The half the comment used to promise and never checked: a refused
    // activation must not have armed anything, so the project that was
    // already there keeps answering exactly as before.
    let (status, _, _) =
        live::http_json(&disp, Method::POST, &call, &[], &serde_json::json!({})).await?;
    anyhow::ensure!(
        status.is_success(),
        "the refused second activation left the first project serving: {status}"
    );

    second.finish().await?;
    first.finish().await
}

//! Gated routes: the gateway refuses a caller the route's auth connection
//! does not verify, and admits one it does with the established identity
//! on the trigger's `caller` port. Two schemes end to end (a key set, a
//! signing secret), and a browser page on another origin reaching a gated
//! route through the install's CORS answer.
#![cfg(feature = "e2e")]

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
        json!({ "signingSecret": "s3cret-e2e" }),
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

    // A browser page on another origin: the install answers the
    // preflight itself and stamps the reply with CORS headers, else the
    // browser refuses what the worker sent. The policy allows every
    // origin, answered as `*` or as the origin that asked, which a browser
    // accepts alike.
    let origin = "http://postcards.example";
    let call = live::browser_post_json(&disp, &secure, origin, &[("x-api-key", "k-two")], &payload).await?;
    let allows_page = |headers: &reqwest::header::HeaderMap| {
        headers
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == "*" || v == origin)
    };
    assert!(call.preflight_status.is_success(), "preflight -> HTTP {}", call.preflight_status);
    assert!(allows_page(&call.preflight_headers), "{:?}", call.preflight_headers);
    assert!(
        call.preflight_headers
            .get("access-control-allow-methods")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|m| m == "*" || m.contains("POST")),
        "{:?}", call.preflight_headers
    );
    assert!(call.status.is_success(), "browser POST -> HTTP {}: {}", call.status, String::from_utf8_lossy(&call.body));
    assert!(allows_page(&call.headers), "{:?}", call.headers);
    assert!(
        call.headers.get("access-control-expose-headers").is_some(),
        "the page may read the headers the program set: {:?}", call.headers
    );

    // The signing secret: the exact bytes are signed with a timestamp;
    // a good signature admits (an anonymous identity), a tampered body
    // and a stale timestamp are refused.
    let signed = format!("{base}/signed");
    let body_bytes = serde_json::to_vec(&json!({ "text": "signed hi" }))?;
    let now = chrono::Utc::now().timestamp();
    // The shipped HmacAuth node's own scheme signs, so the test signs
    // exactly what the gate checks.
    let hmac_auth: weft_core::access::events::VerifyKind =
        serde_json::from_value(catalog_spec("api", "hmac_auth")?["verify"].clone())?;
    let sign = |ts: i64, body: &[u8]| -> anyhow::Result<String> {
        Ok(weft_core::access::verify::hmac_digest(
            &hmac_auth,
            "s3cret-e2e",
            Some(&ts.to_string()),
            &weft_core::access::verify::PushParts { body, headers: &[], url: None, method: "POST" },
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

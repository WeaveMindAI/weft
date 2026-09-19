//! A live HTTP endpoint: POST a body, the program echoes it in its response.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, live, project::Project};

#[tokio::test]
async fn http_endpoint_echoes_request_body() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("web_trigger", disp.clone()).await?;

    // Per-run-unique mount path (namespaced per tenant; unique-per-run avoids
    // same-tenant collisions). Returns the tenant-namespaced callable path.
    let path = project.unique_live_path()?;

    // Live HTTP triggers must be activated (build + register + mount the route).
    project.activate().await?;

    // POST a body; the route hands its `message` key to the responder on
    // the port it declared, and the responder streams progress chunks then
    // a final body that echoes both the raw request (`you_sent`) and the
    // port's value (`port_said`).
    let sent = json!({ "message": "weft-e2e" });
    let bytes = live::http_post(&disp, &path, &sent).await?;
    let body = String::from_utf8_lossy(&bytes);

    assert!(
        body.contains("\"stage\":\"done\""),
        "response missing final stage: {body}"
    );
    assert!(
        body.contains("\"you_sent\":{\"message\":\"weft-e2e\"}"),
        "response did not echo the sent body: {body}"
    );
    assert!(
        body.contains("\"port_said\":\"weft-e2e\""),
        "the route did not deliver the body key on its declared port: {body}"
    );

    // A browser page on another origin: the gateway hop the 307 lands on
    // answers the preflight itself and stamps the reply with CORS headers,
    // else the browser refuses what the worker sent. The policy allows
    // every origin, which Envoy answers by echoing the one that asked
    // (a browser accepts that and `*` alike), so the check is against
    // the page's own origin.
    let origin = "http://postcards.example";
    let call = live::browser_post_json(&disp, &path, origin, &sent).await?;
    let allowed = |headers: &reqwest::header::HeaderMap| {
        headers.get("access-control-allow-origin").and_then(|v| v.to_str().ok()).map(str::to_string)
    };
    assert!(call.preflight_status.is_success(), "preflight -> HTTP {}", call.preflight_status);
    assert_eq!(allowed(&call.preflight_headers).as_deref(), Some(origin), "{:?}", call.preflight_headers);
    assert!(
        call.preflight_headers.get("access-control-allow-methods").and_then(|v| v.to_str().ok()).is_some_and(|m| m.contains("POST")),
        "{:?}", call.preflight_headers
    );
    assert!(call.status.is_success(), "browser POST -> HTTP {}: {}", call.status, String::from_utf8_lossy(&call.body));
    assert_eq!(allowed(&call.headers).as_deref(), Some(origin), "{:?}", call.headers);
    assert!(String::from_utf8_lossy(&call.body).contains("\"stage\":\"done\""));

    project.finish().await
}

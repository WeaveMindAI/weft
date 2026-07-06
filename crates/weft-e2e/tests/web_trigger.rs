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

    // POST a body; the responder streams progress chunks then a final body that
    // echoes what we sent under `you_sent`.
    let sent = json!({ "ping": "weft-e2e" });
    let bytes = live::http_post(&disp, &path, &sent).await?;
    let body = String::from_utf8_lossy(&bytes);

    assert!(
        body.contains("\"stage\":\"done\""),
        "response missing final stage: {body}"
    );
    assert!(
        body.contains("weft-e2e"),
        "response did not echo the sent body: {body}"
    );

    project.finish().await
}

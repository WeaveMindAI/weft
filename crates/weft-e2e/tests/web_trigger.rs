//! A live HTTP endpoint: POST a body, the program echoes it in its response,
//! and the trigger's own display names the address that worked.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{display, ensure, live, project::Project};

#[tokio::test]
async fn http_endpoint_echoes_request_body() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("web_trigger", disp.clone()).await?;
    let pid = project.id();

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

    // ---- The trigger's display, through both doors onto it ----
    //
    // Nothing in the node writes this: the signal KIND serves it from
    // the listener, so every node declaring `route` gets the same panel.
    let shown = display::as_editor(&disp, &pid, "route").await?;
    let address = shown.expect_text("Address")?;
    // The address has to be the one that just worked, not the route
    // pattern the listener holds: only the dispatcher knows the host,
    // the tenant segment, and that a held-connection kind is served
    // under `/connect/`. A display nobody can call is worse than none.
    assert!(
        address.ends_with(&format!("/connect/{path}")),
        "the display's address must be the callable one (…/connect/{path}), got {address}"
    );
    assert!(
        address.contains("http"),
        "the display's address must be whole, host included, got {address}"
    );
    // The fixture's route declares `method: "POST"`, and the method
    // rides with the address so a reader copies one thing.
    assert!(
        address.starts_with("POST "),
        "the display's address must carry the method it serves, got {address}"
    );
    // This fixture declares no auth. "Open" is about the door, not about
    // who knows the address.
    assert_eq!(shown.expect_text("Auth")?, "open (anyone with the URL)");

    // A token that was never granted this node's display reads nothing.
    // The display scope is the one that does NOT default to the
    // wildcard, so a project-scoped token alone is not enough.
    let blind = display::mint_display_token(&disp, &pid, "weft-e2e-no-display", &[], false).await?;
    let (status, why) = display::read_status_with_body(&disp, &blind, &pid, "route").await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::FORBIDDEN,
        "a token with no display scope must be refused the read, got {status}: {why}"
    );
    anyhow::ensure!(
        why.contains("--display"),
        "the refusal must name the flag that grants it, got {why}"
    );

    // The listing is scoped like every other door on this token, so a
    // token that reaches no display is refused it too.
    let (status, why) = display::list_status(&disp, &blind).await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::FORBIDDEN,
        "a token with no display scope must be refused the listing, got {status}: {why}"
    );

    // A token granted THIS node reads the same display the editor sees.
    let scoped =
        display::mint_display_token(&disp, &pid, "weft-e2e-display", &["route"], false).await?;
    // The listing carries what this token reaches, and spells each node
    // the way a person writes it: that spelling is what `--display`
    // takes and what the doors below take as `{node}`.
    let listed = display::list_for_token(&disp, &scoped).await?;
    let nodes: Vec<&str> = listed.iter().filter_map(|e| e["node"].as_str()).collect();
    assert_eq!(nodes, vec!["route"], "listing: {listed:?}");
    assert_eq!(listed[0]["kind"], json!("trigger"));
    assert_eq!(listed[0]["project_id"], json!(pid.to_string()));
    assert_eq!(display::as_token(&disp, &scoped, &pid, "route").await?.expect_text("Address")?, address);

    // Pressing a trigger's display is refused, through the token door
    // as through the editor's: a registration is nothing a reader
    // changes from a panel. This is the only door on this surface that
    // mutates, so its refusal is worth holding.
    let (status, why) = display::press_status(&disp, &scoped, &pid, "route", "regenerate").await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::BAD_REQUEST,
        "pressing a trigger's display must be refused, got {status}: {why}"
    );
    anyhow::ensure!(
        why.contains("read-only"),
        "the refusal must say why, got {why}"
    );

    // And only that node: another id under the same project is a 404,
    // the same answer a node that does not exist gets, so walking ids
    // teaches a caller nothing.
    let status = display::read_status(&disp, &scoped, &pid, "responder").await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::NOT_FOUND,
        "a node outside the token's display scope must be 404, got {status}"
    );

    project.finish().await
}

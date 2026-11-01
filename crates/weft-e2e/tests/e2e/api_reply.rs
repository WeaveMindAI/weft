//! Request-response with zero custom Rust, at the project's own address
//! and at the install's shared one: a route answered by a Reply, the
//! `503` a caller gets while the trigger is down, and the answer once it
//! is back.
#![cfg(feature = "e2e")]

use reqwest::Method;
use serde_json::Value;
use weft_e2e::{ensure, live, project::Project, status};

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// A caller reaches the project's own address straight, with nothing of
/// weft's in between: its routes answer there at the root, and while the
/// trigger is down the worker itself refuses with a `503`. A caller is
/// never held: at the install's shared address too, a call is refused at
/// once with a `503` and a `Retry-After`, the way any server that is down
/// answers, and the call made once the trigger is back is answered.
#[tokio::test]
async fn the_project_answers_at_its_own_address() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_reply", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;

    let status: Value = disp.get_json(&format!("/projects/{}/status", project.id())).await?;
    anyhow::ensure!(status["address"]["state"] == "serving", "the project's address: {}", status["address"]);
    let url = status["address"]["url"].as_str().expect("a serving address has a url").to_string();
    let path = base.trim_start_matches(&format!("local/{}/", project.id()));
    let http = weft_core::net::http_client().build()?;
    let answered = http.get(format!("{url}/{path}/users/42")).send().await?;
    assert_eq!(answered.status(), 200);
    let body: Value = answered.json().await?;
    assert_eq!(body["name"], "Ada", "{body}");

    project.weft(&["deactivate", "--running", "cancel"]).await?;
    status::wait_until_status(&disp, &project.id(), "inactive", status::STATUS_DEADLINE).await?;
    let refused = http.get(format!("{url}/{path}/users/42")).send().await?;
    assert_eq!(refused.status(), 503);
    assert!(refused.headers().get("retry-after").is_some(), "the caller is told when to try again");
    let (code, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/users/42"), &[], None).await?;
    assert_eq!(code, 503, "{}", String::from_utf8_lossy(&body));
    assert!(header(&headers, "retry-after").is_some(), "the caller at the shared address is told when to try again");

    project.activate().await?;
    let (code, _, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/users/42"), &[], None).await?;
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(body["name"], "Ada", "{body}");

    project.finish().await
}

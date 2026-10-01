//! A public entry's own limit, end to end: a route that lets one caller
//! in twice a minute answers the third call with 429 and a `Retry-After`,
//! from the dispatcher, before any run starts.
#![cfg(feature = "e2e")]

use reqwest::Method;
use weft_e2e::{ensure, live, project::Project};

#[path = "common/mod.rs"]
mod common;

#[tokio::test]
async fn a_caller_past_the_route_limit_is_refused_before_a_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("entry_limits", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let ping = format!("{base}/ping");

    for call in 1..=2 {
        let (status, _, body) = live::http_request(&disp, Method::GET, &ping, &[], None).await?;
        assert_eq!(status, 200, "call {call}: {}", String::from_utf8_lossy(&body));
    }
    let (status, headers, body) = live::http_request(&disp, Method::GET, &ping, &[], None).await?;
    assert_eq!(status, 429, "{}", String::from_utf8_lossy(&body));
    let retry_after: u64 = headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .expect("a 429 says when to come back");
    assert!((1..=60).contains(&retry_after), "within the minute window: {retry_after}");
    project.finish().await
}

//! Request-response with zero custom Rust: five routes in one project,
//! each answered by a Reply or a Close, plus the gateway's own answers
//! (405 for a served path with the wrong verb, 404 for an unknown one).
//! One ends on a bare Close, one is the "if it did not work, stop
//! here" shape (a Switch on the outcome, a Reply on the passing case and
//! a Close carrying its reason as the body on the failing one), and one
//! is a 304 whose Close carries its own caching headers.
#![cfg(feature = "e2e")]

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{ensure, live, project::Project, run::SettledRun};

#[path = "common/mod.rs"]
mod common;

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

#[tokio::test]
async fn routes_answer_with_status_headers_and_bodies() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_reply", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;

    // POST hello: the declared body key reaches the graph, the Reply's
    // status and header reach the caller, the fixed ports carried the
    // path and the headers.
    let (status, headers, body) = live::http_json(
        &disp,
        Method::POST,
        &format!("{base}/hello"),
        &[("user-agent", "weft-e2e")],
        &json!({ "name": "ada", "ignored": true }),
    )
    .await?;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "x-run"), Some("e2e"));
    assert_eq!(header(&headers, "content-type"), Some("application/json"));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v["greeting"], "hello ada");
    assert_eq!(v["agent"], "weft-e2e");
    assert!(v["path"].as_str().unwrap().ends_with("/hello"), "the path as called: {v}");

    // GET users/{id}: the capture and the query reach the graph; the
    // graph decides the status.
    let (status, _, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/users/42?verbose=1"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v, json!({ "id": "42", "name": "Ada", "verbose": "1" }));
    let (status, _, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/users/7"), &[], None).await?;
    assert_eq!(status, 404, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v["error"], "no user 7");

    // POST say (a text route): the whole body on one port, text back.
    let (status, headers, body) = live::http_request(
        &disp,
        Method::POST,
        &format!("{base}/say"),
        &[("content-type", "text/plain")],
        Some(b"quiet please".to_vec()),
    )
    .await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("text/plain; charset=utf-8"));
    assert_eq!(String::from_utf8_lossy(&body), "QUIET PLEASE");

    // DELETE gone: a bodiless answer through Close, ordered by a Boolean
    // decision on its `_should_flow`.
    let (status, _, body) =
        live::http_request(&disp, Method::DELETE, &format!("{base}/gone"), &[], None).await?;
    assert_eq!(status, 204, "{}", String::from_utf8_lossy(&body));
    assert!(body.is_empty());

    // DELETE sweep/{what}: the Switch picks the Reply when something was
    // removed, and the Close, whose reason is the body, when nothing was.
    let (status, _, body) =
        live::http_request(&disp, Method::DELETE, &format!("{base}/sweep/cards"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(serde_json::from_slice::<Value>(&body)?, json!(3));
    let (status, headers, body) =
        live::http_request(&disp, Method::DELETE, &format!("{base}/sweep/nothing"), &[], None).await?;
    assert_eq!(status, 404, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("application/json"));
    assert_eq!(serde_json::from_slice::<Value>(&body)?, json!("nothing to sweep"));

    // GET cached: an ending with no body still has a head, so a 304
    // carries the caching header the next request will send back. A
    // Close that could not set one left the client reusing whatever it
    // had stored, and the graph unable to say so.
    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/cached"), &[], None).await?;
    assert_eq!(status, 304, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "etag"), Some("v1"));
    assert_eq!(header(&headers, "cache-control"), Some("max-age=60"));
    assert!(body.is_empty(), "a 304 carries no body");

    // The gateway's own answers, before any run starts: a served path
    // with the wrong verb names the verbs it serves; an unknown path is
    // not found.
    let (status, _, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/hello"), &[], None).await?;
    assert_eq!(status, 405, "{}", String::from_utf8_lossy(&body));
    assert!(String::from_utf8_lossy(&body).contains("POST"), "{}", String::from_utf8_lossy(&body));
    let (status, _, _) =
        live::http_request(&disp, Method::GET, &format!("{base}/nowhere"), &[], None).await?;
    assert_eq!(status, 404);

    project.finish().await
}

/// The fast loop: a route answered with NO cluster reachable from
/// outside, no activation and no listener armed.
///
/// `--fire` serves the request itself. The trigger reads the body off
/// what was typed, and a stand-in caller stands in for the socket
/// nobody opened and records what comes back, so the Reply at the end
/// runs for real and its answer is in the journal. Before this a
/// route's program could not be run at all offline: the trigger asked
/// for the caller, found nobody, and failed before a single node of
/// the author's own graph ran.
#[tokio::test]
async fn a_fired_route_runs_its_whole_program_and_answers() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("api_reply", disp.clone()).await?;
    // The path still has to resolve, but nothing listens on it: this
    // run never goes near the gateway.
    project.unique_live_path()?;

    // Bake prepares the triggers without arming them, which is what
    // gives the fire the trigger's signal spec to serve the request
    // under. No activate, so no listener and no URL.
    project.weft(&["bake", "--json"]).await?;
    let stdout = project
        .weft(&[
            "run",
            "--json",
            "--fire",
            r#"hello={"method":"POST","path":"hello","headers":[["user-agent","weft-e2e"]],"body":{"name":"ada"}}"#,
        ])
        .await?;
    let settled = SettledRun::observe(project.dispatcher(), common::color_of(&stdout)?).await?;
    settled.completed()?;

    // The author's own nodes ran on the body, which is the whole point:
    // the trigger took the request apart and the python node saw it.
    settled.assert_completed("greet")?;
    settled.assert_input("greet", "name", &json!("ada"))?;

    // And the conversation records what actually happened, which is
    // that nobody said anything: there is no caller on a fired run, so
    // there is no inbound message. The body the author typed is visible
    // where it really arrived, on the trigger's own firing (the `greet`
    // input asserted above), not as a message in an exchange nobody had.
    let asked = settled.caller_requests();
    anyhow::ensure!(asked.is_empty(), "nobody sent anything, yet: {asked:?}");

    // And the Reply ran for real: this is what a caller WOULD have
    // received, recorded the same way a real exchange is recorded.
    let answered = settled.caller_final_answer();
    anyhow::ensure!(
        answered
            == Some(json!({
                "greeting": "hello ada",
                "path": "hello",
                "agent": "weft-e2e",
            })),
        "the program's answer: {answered:?}"
    );

    project.finish().await
}

/// The same loop for a GET, whose natural spelling carries no body at
/// all. This is the case that broke: a payload with no `body` key was
/// read as "no caller", and the trigger then failed asking to be
/// triggered through a Route, which is exactly the node being fired.
/// A GET has no body, so the spelling that has to work is the one
/// without one.
#[tokio::test]
async fn firing_a_get_route_needs_no_body() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("api_reply", disp.clone()).await?;
    project.unique_live_path()?;
    project.weft(&["bake", "--json"]).await?;

    // No `body` key, and the path's capture supplied the way the
    // gateway would have matched it.
    let stdout = project
        .weft(&[
            "run",
            "--json",
            "--fire",
            r#"user={"method":"GET","path":"users/42","params":{"id":"42"},"query":{"verbose":"1"}}"#,
        ])
        .await?;
    let settled = SettledRun::observe(project.dispatcher(), common::color_of(&stdout)?).await?;
    settled.completed()?;

    // The author's node saw the capture and the query, so the request
    // really was served rather than the run limping on without one.
    settled.assert_completed("lookup")?;
    settled.assert_completed("found")?;

    // And the Reply answered, which only happens with a caller attached.
    let answered = settled.caller_final_answer();
    anyhow::ensure!(
        answered == Some(json!({ "id": "42", "name": "Ada", "verbose": "1" })),
        "the program's answer: {answered:?}"
    );

    project.finish().await
}

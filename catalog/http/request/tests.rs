//! HttpRequest self-tests: the body's declared union stays honest.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::HttpRequestNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_json_object_body_stays_a_dict", object_body),
        NodeTest::fake("a_non_object_body_is_the_verbatim_text", text_body),
        NodeTest::fake("a_refusal_status_still_emits", refusal_status),
        NodeTest::fake("method_headers_and_body_ride_the_request", request_shape),
    ]
}

async fn request_shape(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/items", json!({ "id": 7 }));
    let outcome = rig
        .run(
            &HttpRequestNode,
            json!({
                "url": "https://api.example/api/items",
                "method": "POST",
                "headers": { "authorization": "Bearer tok-1", "x-trace": "t-9" },
                "body": { "name": "widget" },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!(200));

    let sent = rig.requests();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].method, "POST");
    assert_eq!(sent[0].body.as_ref().expect("json body")["name"], json!("widget"));
    assert_eq!(sent[0].header("authorization"), Some("Bearer tok-1"));
    assert_eq!(sent[0].header("x-trace"), Some("t-9"));
    assert_eq!(sent[0].header("content-type"), Some("application/json"));
    Ok(())
}

async fn object_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/api/thing", json!({ "a": 1 }));
    let outcome = rig
        .run(
            &HttpRequestNode,
            json!({ "url": "https://api.example/api/thing", "method": "GET" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!(200));
    assert_eq!(outcome.outputs["ok"], json!(true));
    assert_eq!(outcome.outputs["body"], json!({ "a": 1 }));
    Ok(())
}

async fn text_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/plain", 200, "text/plain", "just text".as_bytes().to_vec());
    let outcome = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/plain", "method": "GET" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["body"], json!("just text"), "non-JSON stays verbatim text");
    Ok(())
}

async fn refusal_status(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status("POST", "/api/thing", 404, json!({ "error": "gone" }));
    let outcome = rig
        .run(
            &HttpRequestNode,
            json!({
                "url": "https://api.example/api/thing",
                "method": "POST",
                "body": { "x": 1 },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!(404));
    assert_eq!(outcome.outputs["ok"], json!(false), "a refusal is data, not a node failure");
    assert_eq!(rig.requests()[0].body, Some(json!({ "x": 1 })));
    Ok(())
}

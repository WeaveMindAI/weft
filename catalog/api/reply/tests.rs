//! Reply self-tests: the one message per wire, the head on HTTP, the
//! refusal of a head on a socket, and the body shapes.

use serde_json::json;

use weft::caller::{CallerCall, CloseReason, InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ReplyNode;
use crate::testing::{http_caller, http_caller_without_body, ws_caller};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("an_http_reply_is_the_final_body_under_its_head", http_reply),
        NodeTest::fake("a_socket_reply_is_one_message_and_stays_open", socket_reply),
        NodeTest::fake("a_socket_reply_it_cannot_send_is_skipped_when_the_socket_says_so", socket_unmade),
        NodeTest::fake("a_status_behind_a_socket_is_refused", socket_status_refused),
        NodeTest::fake("a_text_route_wants_a_string_body", text_shape),
        NodeTest::fake("a_bytes_route_sends_a_stored_file", bytes_shape),
        NodeTest::fake("answer_as_bytes_answers_a_picture_from_a_json_route", answer_as_bytes),
        NodeTest::fake("a_stored_file_in_a_json_answer_goes_out_as_a_link", file_link),
        NodeTest::fake("a_redirect_sends_the_caller_to_the_files_link", redirect),
        NodeTest::basic("a_head_carries_status_and_string_headers", head_shape),
        NodeTest::fake("a_secret_reaches_the_caller_revealed_and_the_record_keeps_its_marker", secret_reply),
        NodeTest::fake("a_secret_text_body_goes_out_as_its_text", secret_text),
        NodeTest::fake("a_secret_its_type_does_not_declare_fails_the_reply", undeclared_secret),
    ]
}

fn ty(text: &str) -> weft::WeftType {
    weft::WeftType::try_parse(text).unwrap_or_else(|why| panic!("`{text}`: {why}"))
}

/// A secret where the body's type says one goes, and one as a header's
/// value, reach the caller revealed; what the run's record keeps of the
/// answer is the body with its marker.
async fn secret_reply(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    rig.input_type("body", ty("{ session: Secret[String], user: String }"));
    let session = rig.secret(json!("sess-42"));
    let cookie = rig.secret(json!("sid=42"));
    let body = json!({ "session": session, "user": "ada" });
    rig.run(&ReplyNode, json!({ "body": body.clone(), "headers": { "set-cookie": cookie, "x-run": "r-1" } })).await.ok()?;
    assert_eq!(
        conn.calls()[1],
        CallerCall::Terminate {
            head: Some(ResponseHead::new(200).with_header("set-cookie", "sid=42").with_header("x-run", "r-1")),
            final_chunk: Some(OutboundChunk::Revealed {
                sent: Box::new(OutboundChunk::Json(json!({ "session": "sess-42", "user": "ada" }))),
                kept: body,
            }),
            close: None,
        }
    );
    Ok(())
}

/// A text route answering a `Secret[String]` sends its text.
async fn secret_text(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Text, LiveRequest::default(), InboundMessage::Text(String::new()));
    rig.attach_caller(conn.clone());
    rig.input_type("body", ty("Secret[String]"));
    let token = rig.secret(json!("tok-7"));
    rig.run(&ReplyNode, json!({ "body": token.clone() })).await.ok()?;
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Revealed { sent: Box::new(OutboundChunk::Text("tok-7".into())), kept: token }]
    );
    Ok(())
}

async fn http_reply(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let outcome = rig
        .run(
            &ReplyNode,
            json!({ "body": { "ok": true }, "status": 201, "headers": { "x-run": "r-1" } }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Terminate {
                head: Some(ResponseHead::new(201).with_header("x-run", "r-1")),
                final_chunk: Some(OutboundChunk::Json(json!({ "ok": true }))),
                close: None,
            },
        ]
    );
    Ok(())
}

async fn socket_reply(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    rig.run(&ReplyNode, json!({ "body": { "echo": "hi" } })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Json(json!({ "echo": "hi" }))]);
    assert_eq!(conn.close_reason(), None::<Option<CloseReason>>, "the socket stays open");
    Ok(())
}

async fn socket_status_refused(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let err = rig.run(&ReplyNode, json!({ "body": "x", "status": 201 })).await.failure()?;
    assert!(err.contains("no meaning on a socket"), "{err}");
    assert!(conn.chunks().is_empty(), "nothing was sent");
    Ok(())
}

async fn text_shape(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Text, LiveRequest::default(), InboundMessage::Text(String::new()));
    rig.attach_caller(conn.clone());
    let err = rig.run(&ReplyNode, json!({ "body": { "not": "text" } })).await.failure()?;
    assert!(err.contains("must be a String"), "{err}");
    rig.run(&ReplyNode, json!({ "body": "plain" })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Text("plain".into())]);
    Ok(())
}

async fn bytes_shape(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Bytes, LiveRequest::default(), InboundMessage::Bytes(vec![]));
    rig.attach_caller(conn.clone());
    let file = rig.store_file("out.bin", "application/octet-stream", vec![9, 9]);
    rig.run(&ReplyNode, json!({ "body": file })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Bytes(vec![9, 9])]);
    let head = &conn.heads()[0];
    assert_eq!(head.header("content-type"), Some("application/octet-stream"), "the file's own type");
    assert_eq!(head.header("content-disposition"), Some("inline; filename=\"out.bin\"; filename*=UTF-8''out.bin"));
    let err = rig.run(&ReplyNode, json!({ "body": "not a file" })).await.failure()?;
    assert!(err.contains("stored file"), "{err}");
    Ok(())
}

/// A GET with no body still answers a picture as bytes: `answerAs` is
/// the answer's shape, apart from the route's, and the file names its
/// own type unless the program set one.
async fn answer_as_bytes(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let file = rig.store_file("view.jpg", "image/jpeg", vec![1, 2]);
    rig.run(&ReplyNode, json!({ "body": file.clone(), "answerAs": "bytes" })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Bytes(vec![1, 2])]);
    assert_eq!(conn.heads()[0].header("content-type"), Some("image/jpeg"));
    let set = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(set.clone());
    let headers = json!({ "content-type": "image/x" });
    rig.run(&ReplyNode, json!({ "body": file.clone(), "answerAs": "bytes", "headers": headers })).await.ok()?;
    assert_eq!(set.heads()[0].header("content-type"), Some("image/x"), "the program's header wins");
    let err = rig.run(&ReplyNode, json!({ "body": file, "answerAs": "xml" })).await.failure()?;
    assert!(err.contains("answerAs must be json, text, bytes or redirect"), "{err}");
    Ok(())
}

async fn file_link(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let file = rig.store_file("cat.png", "image/png", vec![1, 2, 3]);
    let key = weft::storage::StoredFile::from_value(&file)?.key;
    let away = json!({ "__weft_image__": { "url": "https://example.com/dog.png", "mimeType": "image/png", "filename": "dog.png", "sizeBytes": 9 } });
    let body = json!({ "card": { "photo": file, "caption": "a cat" }, "more": [file], "away": away });
    rig.run(&ReplyNode, json!({ "body": body })).await.ok()?;
    let link = json!({
        "url": format!("{}/files/{key}?ttl={}", weft::node_test::FAKE_LINK_BASE, weft::storage::DEFAULT_LINK_SECS),
        "mimeType": "image/png",
        "filename": "cat.png",
        "sizeBytes": 3,
    });
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Json(json!({
            "card": { "photo": link, "caption": "a cat" },
            "more": [link],
            "away": { "url": "https://example.com/dog.png", "mimeType": "image/png", "filename": "dog.png", "sizeBytes": 9 },
        }))],
        "every file, however nested, is a link in the one shape (a url file its own url); nothing else moves"
    );
    Ok(())
}

/// `answerAs: redirect` answers the file's link as `location`, under
/// 302 or the redirect status the program set, with the value's own link
/// lifetime (a url file: its own url); anything but a file, a status no
/// redirect has, or a socket is refused.
async fn redirect(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let mut file = rig.store_file("clip.mp4", "video/mp4", vec![1, 2, 3]);
    file["__weft_video__"]["linkSecs"] = json!(7200);
    let key = weft::storage::StoredFile::from_value(&file)?.key;
    rig.run(&ReplyNode, json!({ "body": file.clone(), "answerAs": "redirect" })).await.ok()?;
    let head = &conn.heads()[0];
    assert_eq!(head.status, 302);
    assert_eq!(head.header("location"), Some(format!("{}/files/{key}?ttl=7200", weft::node_test::FAKE_LINK_BASE).as_str()));
    assert_eq!(conn.chunks(), vec![OutboundChunk::Bytes(Vec::new())], "a redirect carries no body");
    let moved = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(moved.clone());
    rig.run(&ReplyNode, json!({ "body": file.clone(), "answerAs": "redirect", "status": 307 })).await.ok()?;
    assert_eq!(moved.heads()[0].status, 307);
    let err = rig.run(&ReplyNode, json!({ "body": file.clone(), "answerAs": "redirect", "status": 404 })).await.failure()?;
    assert!(err.contains("301, 302, 303, 307 or 308"), "{err}");
    let err = rig.run(&ReplyNode, json!({ "body": "not a file", "answerAs": "redirect" })).await.failure()?;
    assert!(err.contains("redirect answers a file"), "{err}");
    // A url file is a link already, and the redirect goes to it.
    let away = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(away.clone());
    let url_file = json!({ "__weft_image__": { "url": "https://example.com/cat.png", "mimeType": "image/png" } });
    rig.run(&ReplyNode, json!({ "body": url_file, "answerAs": "redirect" })).await.ok()?;
    assert_eq!(away.heads()[0].header("location"), Some("https://example.com/cat.png"));
    rig.attach_caller(ws_caller(DataType::Json, LiveRequest::default(), vec![]));
    let err = rig.run(&ReplyNode, json!({ "body": file, "answerAs": "redirect" })).await.failure()?;
    assert!(err.contains("redirect is an HTTP answer"), "{err}");
    Ok(())
}

fn head_shape() -> WeftResult<()> {
    let head = crate::wire::head_for(201, Some(&json!({ "x-run": "r-1" })))?;
    assert_eq!(head.status, 201);
    assert_eq!(head.headers, vec![("x-run".to_string(), "r-1".to_string())]);
    assert!(crate::wire::head_for(200, None)?.headers.is_empty());
    assert!(crate::wire::head_for(200, Some(&json!(["x"]))).is_err(), "a list is not a header map");
    assert!(crate::wire::head_for(200, Some(&json!({ "x": 1 }))).is_err(), "a number is not a header value");
    Ok(())
}

/// Behind a socket that skips a message it cannot make, a Reply whose
/// body the socket's data type cannot carry sends nothing, says so to the
/// connection, and leaves `done` absent; the session stays open. Behind a
/// route the reply is the whole answer, so the same body still fails the
/// node there (`text_shape`).
async fn socket_unmade(rig: FakeRig) -> WeftResult<()> {
    let conn = crate::testing::caller_skipping_unmade_chunks(weft::signal::Protocol::Websocket, DataType::Text);
    rig.attach_caller(conn.clone());
    let outcome = rig.run(&ReplyNode, json!({ "body": { "not": "text" } })).await.ok()?;
    assert!(outcome.outputs.get("done").is_none(), "{:?}", outcome.outputs);
    assert!(conn.chunks().is_empty(), "{:?}", conn.chunks());
    assert!(
        matches!(conn.calls().as_slice(), [.., CallerCall::ChunkDropped { why }] if why.contains("must be a String")),
        "{:?}",
        conn.calls()
    );
    assert_eq!(conn.close_reason(), None::<Option<CloseReason>>, "the socket stays open");

    let route = crate::testing::caller_skipping_unmade_chunks(weft::signal::Protocol::Http, DataType::Text);
    rig.attach_caller(route.clone());
    let err = rig.run(&ReplyNode, json!({ "body": { "not": "text" } })).await.failure()?;
    assert!(err.contains("must be a String"), "a route's one answer is never skipped: {err}");
    Ok(())
}

/// A secret inside a body whose type declares none (an object left as a
/// `JsonDict`) fails the node naming its place, and nothing goes out: the
/// caller would have received the marker. A socket that skips a message
/// it cannot make still fails, since this is the program's mistake.
async fn undeclared_secret(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    rig.input_type("body", ty("JsonDict"));
    let token = rig.secret(json!("tok-7"));
    let err = rig.run(&ReplyNode, json!({ "body": { "auth": { "token": token } } })).await.failure()?;
    assert!(err.contains("`body` holds a secret at /auth/token"), "{err}");
    assert!(err.contains("{ token: Secret[String] }"), "the fix is named: {err}");
    assert!(conn.chunks().is_empty(), "{:?}", conn.chunks());

    let socket = crate::testing::caller_skipping_unmade_chunks(weft::signal::Protocol::Websocket, DataType::Json);
    rig.attach_caller(socket.clone());
    let err = rig.run(&ReplyNode, json!({ "body": { "token": token } })).await.failure()?;
    assert!(err.contains("`body` holds a secret at /token"), "never skipped: {err}");
    assert!(socket.chunks().is_empty(), "{:?}", socket.chunks());
    Ok(())
}

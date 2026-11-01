//! Route self-tests: the registered signal carries the node's fields, and
//! a firing puts the caller's request on the fixed ports and its body,
//! held to the type the program declared, on `body`, per the trigger's
//! data type. A body that does not fit is answered 400 before anything
//! downstream runs.

use std::sync::Arc;

use serde_json::{json, Value};

use weft::caller::{FakeCallerConnection, InboundMessage, OutboundChunk};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::RouteNode;
use crate::testing::{http_caller, http_caller_without_body, request};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_route_signal", setup_registers),
        NodeTest::fake("a_json_body_lands_whole_on_body", json_body),
        NodeTest::fake("a_body_with_a_key_its_closed_record_does_not_name_is_answered_400", unknown_key_refused),
        NodeTest::fake("a_body_missing_a_required_field_is_answered_400_naming_it", missing_field_refused),
        NodeTest::fake("an_open_record_drops_the_keys_it_does_not_name", open_record_drops),
        NodeTest::fake("a_text_body_lands_whole_on_body", text_body),
        NodeTest::fake("a_bytes_body_is_stored_and_its_file_flows", bytes_body),
        NodeTest::fake("the_body_never_reaches_the_request_ports", request_wins),
        NodeTest::fake("a_capture_comes_out_only_on_the_port_declared_for_it", capture_port),
        NodeTest::fake("a_capture_port_on_a_bytes_route_sits_beside_the_body", bytes_route_capture),
        NodeTest::fake("a_capture_called_params_is_its_text", capture_named_params),
        NodeTest::fake("a_picture_in_a_json_body_is_stored_and_its_file_flows", inline_picture),
        NodeTest::fake("an_upload_slot_in_a_file_field_is_received_by_one_run", upload_slot),
        NodeTest::fake("one_refused_upload_of_a_body_receives_none_of_them", one_refused_upload_receives_none),
        NodeTest::fake("a_bad_picture_beside_an_upload_receives_nothing", bad_picture_receives_nothing),
        NodeTest::fake("a_wrong_field_beside_an_upload_receives_nothing", wrong_field_receives_nothing),
        NodeTest::fake("bare_base64_is_typed_by_its_bytes_and_held_to_its_field", bare_base64),
        NodeTest::fake("an_optional_picture_is_held_to_its_kind_and_typed_by_its_bytes", optional_picture),
        NodeTest::fake("a_fired_body_of_the_wrong_shape_is_answered_400", fired_body_of_the_wrong_shape),
        NodeTest::fake("a_json_body_its_declared_type_does_not_take_is_answered_400", json_body_not_an_object),
        NodeTest::fake("a_body_declared_json_takes_any_json_value", json_body_any_json),
        NodeTest::fake("a_request_with_no_body_still_serves_its_ports", json_body_absent),
        NodeTest::fake("a_fired_runs_body_comes_from_its_wake", fired_body_comes_from_the_wake),
        NodeTest::fake("a_fired_text_body_comes_from_its_wake", fired_text_body_comes_from_the_wake),
        NodeTest::fake("a_request_with_no_body_closes_body_on_any_route", fired_without_a_body),
        NodeTest::fake("a_null_body_flows_where_the_type_takes_null_and_closes_body_elsewhere", null_body),
        NodeTest::fake("a_run_with_no_caller_fails_loud", no_caller),
        NodeTest::fake("the_callers_credentials_come_out_as_secrets_apart_from_headers", credentials),
        NodeTest::fake("a_body_claiming_to_be_a_secret_is_answered_400", body_claims_a_secret),
    ]
}

fn ty(text: &str) -> WeftType {
    WeftType::try_parse(text).unwrap_or_else(|why| panic!("`{text}`: {why}"))
}

/// A connected fake caller sending `body` on the representative request,
/// woken and attached to the rig; handed back so a case can read what
/// the route answered.
fn call(rig: &FakeRig, data_type: DataType, body: InboundMessage) -> Arc<FakeCallerConnection> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let conn = http_caller(data_type, request, body);
    rig.attach_caller(conn.clone());
    conn
}

/// What a refused call got back: the status and the error the json
/// answer carries.
fn refusal(conn: &FakeCallerConnection) -> (u16, String) {
    let status = conn.heads().first().map(|head| head.status).expect("the route answered");
    let error = match conn.chunks().first() {
        Some(OutboundChunk::Json(body)) => body["error"].as_str().expect("an error field").to_string(),
        other => panic!("a json route answers json, got {other:?}"),
    };
    (status, error)
}

async fn json_body(rig: FakeRig) -> WeftResult<()> {
    let body = json!({ "userId": "u-1", "text": "hi", "extra": 3 });
    call(&rig, DataType::Json, InboundMessage::Json(body.clone()));
    // Nothing declared: `body` is a JsonDict and takes the whole object.
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], body);
    assert_eq!(outcome.outputs["method"], json!("POST"));
    assert_eq!(outcome.outputs["path"], json!("chat/room7"));
    assert!(
        !outcome.outputs.contains_key("room") && !outcome.outputs.contains_key("params"),
        "a capture nobody declared comes out nowhere: {:?}",
        outcome.outputs
    );
    assert_eq!(outcome.outputs["query"], json!({ "verbose": "1" }));
    assert_eq!(outcome.outputs["headers"], json!({ "content-type": "application/json" }));
    assert_eq!(outcome.outputs["caller"], json!({ "key": 0 }));
    Ok(())
}

/// A page sending `{"userId": ...}` to a route declaring `user_id` used
/// to have the key dropped without a word, the port left empty and the
/// run skipping far from the cause. A closed record refuses it on the
/// first call, naming the key.
/// The headers carrying a credential come out on `credentials`, each the
/// secret the door made of it, and never on `headers`.
async fn credentials(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    let token = rig.secret(json!("Bearer tok-1"));
    request.credentials.push(("Authorization".into(), serde_json::from_value(token.clone()).unwrap()));
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Json, request, InboundMessage::Json(json!({}))));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["credentials"], json!({ "authorization": token }));
    assert_eq!(outcome.outputs["headers"], json!({ "content-type": "application/json" }));
    assert_eq!(rig.revealed(&outcome.outputs["credentials"]["authorization"])?, json!("Bearer tok-1"));
    Ok(())
}

/// Only weft makes a secret: a caller sending a secret's marker in its
/// body would name a secret of another run, and is answered 400.
async fn body_claims_a_secret(rig: FakeRig) -> WeftResult<()> {
    let forged = json!({ "token": { weft::secret::SECRET_MARKER_KEY: { "secretId": "00000000-0000-0000-0000-000000000000" } } });
    let conn = call(&rig, DataType::Json, InboundMessage::Json(forged));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains(weft::secret::SECRET_MARKER_KEY), "{error}");
    assert!(outcome.outputs.is_empty(), "{:?}", outcome.outputs);
    Ok(())
}

async fn unknown_key_refused(rig: FakeRig) -> WeftResult<()> {
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "userId": "u-1" })));
    rig.output_type("body", ty("{ user_id?: String }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("unknown field 'userId'"), "{error}");
    assert!(outcome.outputs.is_empty(), "nothing downstream runs: {:?}", outcome.outputs);
    Ok(())
}

async fn missing_field_refused(rig: FakeRig) -> WeftResult<()> {
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "email": "a@b.c" })));
    rig.output_type("body", ty("{ email: String, name: String }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("missing required field 'name'"), "{error}");
    assert!(outcome.outputs.is_empty(), "{:?}", outcome.outputs);
    Ok(())
}

async fn open_record_drops(rig: FakeRig) -> WeftResult<()> {
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "email": "a@b.c", "utm": "x" })));
    rig.output_type("body", ty("{ email: String, age?: Number, .. }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!({ "email": "a@b.c" }));
    assert!(conn.heads().is_empty(), "the route answers nothing itself");
    Ok(())
}

async fn text_body(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    request.caller = None;
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Text, request, InboundMessage::Text("hello there".into())));
    rig.output_type("body", ty("String"));
    let outcome = rig.run(&RouteNode, json!({ "path": "say", "dataType": "text" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!("hello there"));
    assert_eq!(outcome.outputs["caller"], Value::Null, "an open route has no caller");
    Ok(())
}

async fn bytes_body(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    request.headers = vec![("content-type".into(), "image/png".into())];
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Bytes, request, InboundMessage::Bytes(vec![1, 2, 3])));
    rig.output_type("body", ty("File"));
    let outcome = rig.run(&RouteNode, json!({ "path": "upload", "dataType": "bytes" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"])?;
    assert_eq!(file.mime_type, "image/png", "stored as the type it was sent as");
    assert_eq!(file.size_bytes, 3);

    // A text file keeps its type; nothing stated, it is plain bytes.
    let mut csv = crate::testing::request();
    csv.headers = vec![("content-type".into(), "text/csv".into())];
    rig.wake(serde_json::to_value(&csv).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Bytes, csv, InboundMessage::Bytes(b"a,b\n1,2".to_vec())));
    let outcome = rig.run(&RouteNode, json!({ "path": "upload", "dataType": "bytes" })).await.ok()?;
    assert_eq!(weft::storage::StoredFile::from_value(&outcome.outputs["body"])?.mime_type, "text/csv");
    let mut unstated = crate::testing::request();
    unstated.headers.retain(|(name, _)| !name.eq_ignore_ascii_case("content-type"));
    rig.wake(serde_json::to_value(&unstated).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Bytes, unstated, InboundMessage::Bytes(b"a,b\n1,2".to_vec())));
    let outcome = rig.run(&RouteNode, json!({ "path": "upload", "dataType": "bytes" })).await.ok()?;
    assert_eq!(weft::storage::StoredFile::from_value(&outcome.outputs["body"])?.mime_type, "application/octet-stream");
    Ok(())
}

async fn request_wins(rig: FakeRig) -> WeftResult<()> {
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "path": "spoofed", "caller": { "key": 9 } })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "the request's path, never the body's");
    assert_eq!(outcome.outputs["caller"], json!({ "key": 0 }), "the gate's identity, never the body's");
    assert_eq!(outcome.outputs["body"]["path"], json!("spoofed"), "the body keeps what it sent");
    Ok(())
}

async fn capture_port(rig: FakeRig) -> WeftResult<()> {
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "room": "spoofed", "text": "hi" })));
    rig.output_type("room", ty("String"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["room"], json!("room7"), "the capture, never the body key");
    assert_eq!(outcome.outputs["body"], json!({ "room": "spoofed", "text": "hi" }));
    assert!(!outcome.outputs.contains_key("params"), "the declared port is the capture's only way out");
    Ok(())
}

/// A capture called `params` is the capture's text, never the map of
/// every capture a Socket hands out under that name.
async fn capture_named_params(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    request.params.clear();
    request.params.insert("params".into(), "42".into());
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Json, request, InboundMessage::Json(json!({}))));
    rig.output_type("params", ty("String"));
    let outcome = rig.run(&RouteNode, json!({ "path": "x/{params}" })).await.ok()?;
    assert_eq!(outcome.outputs["params"], json!("42"));
    Ok(())
}

/// A tiny PNG: the signature every sniffer knows, then padding.
fn png_bytes() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    bytes.extend_from_slice(&[0u8; 8]);
    bytes
}

/// A `POST preview/{view}` taking bytes reads `view` from the path and
/// the stored body beside it.
async fn bytes_route_capture(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    request.headers = vec![("content-type".into(), "image/png".into())];
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Bytes, request, InboundMessage::Bytes(png_bytes())));
    rig.output_type("room", ty("String"));
    rig.output_type("body", ty("Image"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}", "dataType": "bytes" })).await.ok()?;
    assert_eq!(outcome.outputs["room"], json!("room7"), "the capture");
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"])?;
    assert_eq!(file.mime_type, "image/png", "and the body beside it");
    Ok(())
}

async fn inline_picture(rig: FakeRig) -> WeftResult<()> {
    let data_url = weft::storage::media::data_url("image/png", &png_bytes());
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": data_url, "caption": "a cat" })));
    rig.output_type("body", ty("{ photo: Image, caption: String }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["photo"])?;
    assert_eq!(file.mime_type, "image/png");
    assert_eq!(file.size_bytes, png_bytes().len() as u64);
    assert_eq!(file.filename, "body");
    assert!(file.key.contains("/run/"), "run-scoped: the program keeps what it wants: {}", file.key);
    assert_eq!(outcome.outputs["body"]["caption"], json!("a cat"), "the other fields come as sent");

    // A stored-file object is never taken on a caller's word: it would
    // point the program at any file of the project, or make the worker
    // fetch an address of the caller's choosing. Anywhere in the body.
    let stored = rig.store_file("cat.png", "image/png", png_bytes());
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": stored, "caption": "again" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("stored-file object"), "{error}");
    assert!(outcome.outputs.is_empty());
    let marker = json!({ "__weft_image__": { "url": "http://169.254.169.254/latest/meta-data/", "mimeType": "image/png" } });
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": data_url, "caption": { "deep": [marker] } })));
    rig.output_type("body", ty("{ photo: Image, caption: JsonDict }"));
    rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert!(refusal(&conn).1.contains("stored-file object"));
    rig.output_type("body", ty("{ photo: Image, caption: String }"));

    // Anything else in a file field is the caller's mistake, named.
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": 42, "caption": "x" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("photo") && error.contains("Image"), "{error}");
    assert!(outcome.outputs.is_empty());
    Ok(())
}

/// A file field holding an upload slot (`weft-upload:<slot>`) receives
/// the file the page uploaded through it, in the slot's scope. The same
/// run naming the slot again gets the same file; another run naming it,
/// or a slot nothing was uploaded through, is the caller's mistake,
/// answered 400 in the store's words. A file that does not fit its field
/// is refused the same way and the slot stays open, so a call naming it
/// in a field it fits receives it.
async fn upload_slot(rig: FakeRig) -> WeftResult<()> {
    let video = b"\x00\x00\x00\x18ftypmp42".to_vec();
    rig.uploaded("weft-upload:s1", weft::storage::StorageScope::Project, "clip.mp4", "video/mp4", video.clone());
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:s1", "again": "weft-upload:s1", "title": "a clip" })));
    rig.output_type("body", ty("{ video: Video, again: Video, title: String }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["video"])?;
    assert_eq!((file.filename.as_str(), file.mime_type.as_str()), ("clip.mp4", "video/mp4"));
    assert_eq!(rig.stored_bytes(&file.key)?.as_ref(), video.as_slice());
    assert_eq!(outcome.outputs["body"]["again"], outcome.outputs["body"]["video"], "the same run gets the same file");
    assert_eq!(rig.stored_files(&weft::storage::StorageScope::Project)?.len(), 1, "it lands in the slot's scope, once");
    assert_eq!(outcome.outputs["body"]["title"], json!("a clip"));

    rig.output_type("body", ty("{ video: Video, title: String }"));
    for slot in ["weft-upload:s1", "weft-upload:nothing"] {
        let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": slot, "title": "again" })));
        let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
        let (status, error) = refusal(&conn);
        assert_eq!(status, 400, "{slot}");
        assert!(error.contains("video") && error.contains("cannot be received"), "{slot}: {error}");
        assert!(outcome.outputs.is_empty());
    }

    // A video named in an Image field: refused, nothing received.
    rig.uploaded("weft-upload:s2", weft::storage::StorageScope::Project, "clip.mp4", "video/mp4", video.clone());
    rig.output_type("body", ty("{ photo: Image }"));
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": "weft-upload:s2" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("photo") && error.contains("cannot be received"), "{error}");
    assert!(outcome.outputs.is_empty());
    assert_eq!(rig.stored_files(&weft::storage::StorageScope::Project)?.len(), 1, "the refused video was not received");
    rig.output_type("body", ty("{ video: Video }"));
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:s2" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["video"])?;
    assert_eq!(rig.stored_bytes(&file.key)?.as_ref(), video.as_slice(), "the slot stayed open");
    Ok(())
}

/// A body naming two uploads, the second of which does not fit its field
/// (a video in an Image field), receives neither: the video that fit is
/// left open too, so the corrected call (a new run) receives both.
async fn one_refused_upload_receives_none(rig: FakeRig) -> WeftResult<()> {
    let video = b"\x00\x00\x00\x18ftypmp42".to_vec();
    let project = weft::storage::StorageScope::Project;
    rig.uploaded("weft-upload:clip", project.clone(), "clip.mp4", "video/mp4", video.clone());
    rig.uploaded("weft-upload:other", project.clone(), "other.mp4", "video/mp4", video.clone());
    rig.uploaded("weft-upload:cover", project.clone(), "cover.png", "image/png", png_bytes());
    rig.output_type("body", ty("{ video: Video, cover: Image }"));
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:clip", "cover": "weft-upload:other" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("video") && error.contains("cover") && error.contains("weft-upload:other"), "every place, and the slot refused: {error}");
    assert!(outcome.outputs.is_empty());
    assert!(rig.stored_files(&project)?.is_empty(), "the video that fit was not received either");

    call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:clip", "cover": "weft-upload:cover" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let clip = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["video"])?;
    let cover = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["cover"])?;
    assert_eq!(rig.stored_bytes(&clip.key)?.as_ref(), video.as_slice(), "the corrected call receives the video");
    assert_eq!(rig.stored_bytes(&cover.key)?.as_ref(), png_bytes().as_slice(), "and the cover beside it");
    assert_eq!(rig.stored_files(&project)?.len(), 2);
    Ok(())
}

/// A bad inline picture beside an upload refuses the call before the
/// upload is received, so the slot stays open for a call that gets it
/// right.
async fn bad_picture_receives_nothing(rig: FakeRig) -> WeftResult<()> {
    let video = b"\x00\x00\x00\x18ftypmp42".to_vec();
    rig.uploaded("weft-upload:s1", weft::storage::StorageScope::Project, "clip.mp4", "video/mp4", video.clone());
    rig.output_type("body", ty("{ video: Video, photo: Image }"));
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:s1", "photo": "not base64!!" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("not valid base64"), "{error}");
    assert!(outcome.outputs.is_empty());
    assert!(rig.stored_files(&weft::storage::StorageScope::Project)?.is_empty(), "the upload was not received");
    let photo = weft::storage::media::data_url("image/png", &png_bytes());
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:s1", "photo": photo })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    assert!(weft::storage::StoredFile::from_value(&outcome.outputs["body"]["video"]).is_ok());
    Ok(())
}

/// A body whose other fields do not fit its type (a wrong field, a
/// missing one) is refused before its upload is received or its picture
/// stored, so the slot stays open for a call that gets it right.
async fn wrong_field_receives_nothing(rig: FakeRig) -> WeftResult<()> {
    let video = b"\x00\x00\x00\x18ftypmp42".to_vec();
    rig.uploaded("weft-upload:s1", weft::storage::StorageScope::Project, "clip.mp4", "video/mp4", video.clone());
    rig.output_type("body", ty("{ video: Video, photo: Image, title: String }"));
    let photo = weft::storage::media::data_url("image/png", &png_bytes());
    for body in [
        json!({ "video": "weft-upload:s1", "photo": photo, "title": 42 }),
        json!({ "video": "weft-upload:s1", "photo": photo }),
    ] {
        let conn = call(&rig, DataType::Json, InboundMessage::Json(body.clone()));
        let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
        let (status, error) = refusal(&conn);
        assert_eq!(status, 400, "{body}");
        assert!(error.contains("title"), "{body}: {error}");
        assert!(outcome.outputs.is_empty());
        assert!(rig.stored_files(&weft::storage::StorageScope::Project)?.is_empty(), "the upload was not received");
        assert!(rig.stored_files(&weft::storage::StorageScope::Run)?.is_empty(), "the picture was not stored");
    }
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "video": "weft-upload:s1", "photo": photo, "title": "a clip" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "clips" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["video"])?;
    assert_eq!(rig.stored_bytes(&file.key)?.as_ref(), video.as_slice(), "the slot stayed open");
    Ok(())
}

async fn bare_base64(rig: FakeRig) -> WeftResult<()> {
    let data_url = weft::storage::media::data_url("image/png", &png_bytes());
    let bare = data_url.split_once(";base64,").expect("a data url").1.to_string();
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": bare })));
    rig.output_type("body", ty("{ photo: Image }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["photo"])?;
    assert_eq!(file.mime_type, "image/png", "typed by the bytes' own signature");

    // Bytes that are not what the field declares are refused.
    let text = weft::storage::media::data_url("text/plain", b"hello");
    let text = text.split_once(";base64,").expect("a data url").1.to_string();
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": text })));
    rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("not what it takes") && error.starts_with("body.photo:"), "names the field: {error}");

    // Not base64 at all: the string is named for what it is.
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": "not base64!!" })));
    rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("not valid base64"), "{error}");
    Ok(())
}

/// A file field that may be absent or null takes a picture the same way,
/// held to the kind it names, and a `data:` URL's claimed type never outranks
/// what the bytes are.
async fn optional_picture(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("body", ty("{ photo?: Image }"));
    let data_url = weft::storage::media::data_url("image/x-anything", &png_bytes());
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": data_url })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["body"]["photo"])?;
    assert_eq!(file.mime_type, "image/x-anything", "stored as the type it was sent as");

    let bare = weft::storage::media::data_url("image/png", &png_bytes());
    let bare = bare.split_once(";base64,").expect("a data url").1.to_string();
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": bare })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    weft::storage::StoredFile::from_value(&outcome.outputs["body"]["photo"])?;

    let not_a_picture = weft::storage::media::data_url("image/png", b"ID3\x04 an mp3, not a picture");
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": not_a_picture })));
    rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("not what it takes") && error.contains("Audio"), "{error}");

    call(&rig, DataType::Json, InboundMessage::Json(json!({})));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!({}));

    // A union naming a file is held to its file member the same way.
    rig.output_type("body", ty("{ photo: Image | Null }"));
    let bare = weft::storage::media::data_url("image/png", &png_bytes());
    let bare = bare.split_once(";base64,").expect("a data url").1.to_string();
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": bare })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    weft::storage::StoredFile::from_value(&outcome.outputs["body"]["photo"])?;
    let mp3 = weft::storage::media::data_url("image/png", b"ID3\x04 an mp3, not a picture");
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": mp3 })));
    rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert!(refusal(&conn).1.contains("Audio"));
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": null })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"]["photo"], Value::Null);

    // With a `String` member a string is text, a link included: nothing
    // is decoded or fetched.
    rig.output_type("body", ty("{ photo: String | Image }"));
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "photo": "https://example.com/cat.png" })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"]["photo"], json!("https://example.com/cat.png"));

    // A Blob place takes unstated bytes as plain bytes, and refuses a
    // picture sent as one.
    rig.output_type("body", ty("{ attachment: Blob }"));
    let bare_png = weft::storage::media::data_url("image/png", &png_bytes());
    let bare_png = bare_png.split_once(";base64,").expect("a data url").1.to_string();
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "attachment": bare_png })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let attachment = &outcome.outputs["body"]["attachment"];
    assert!(attachment.get("__weft_blob__").is_some(), "{attachment}");
    assert_eq!(weft::storage::StoredFile::from_value(attachment)?.mime_type, "application/octet-stream");
    let png = weft::storage::media::data_url("image/png", &png_bytes());
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!({ "attachment": png })));
    rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert!(refusal(&conn).1.contains("sent as image/png, which is not the Blob"));

    // A WebM voice clip is audio when the place takes audio.
    rig.output_type("body", ty("{ clip: Audio }"));
    let webm = weft::storage::media::data_url("audio/webm", b"\x1aE\xdf\xa3\x01\x00\x00\x00 a voice clip");
    call(&rig, DataType::Json, InboundMessage::Json(json!({ "clip": webm })));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(weft::storage::StoredFile::from_value(&outcome.outputs["body"]["clip"])?.mime_type, "audio/webm");
    Ok(())
}

/// A fired run whose body is an object on a text route is the author's
/// mistake in what they fired, answered like a caller's.
async fn fired_body_of_the_wrong_shape(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!({ "text": "hello there" });
    rig.wake(wake);
    let conn = http_caller_without_body(DataType::Text, request);
    rig.attach_caller(conn.clone());
    rig.output_type("body", ty("String"));
    let outcome = rig
        .run(&RouteNode, json!({ "path": "chat/{room}", "dataType": "text" }))
        .await
        .ok()?;
    assert_eq!(conn.heads().first().map(|head| head.status), Some(400));
    match conn.chunks().first() {
        Some(OutboundChunk::Text(error)) => {
            assert!(error.contains("takes a text body") && error.contains("sent json"), "{error}")
        }
        other => panic!("a text route answers text, got {other:?}"),
    }
    assert!(outcome.outputs.is_empty());
    Ok(())
}

/// The body's declared type decides what JSON it takes: the default
/// `JsonDict` is one object, so a list (live) or a bare string (fired)
/// is answered 400 naming the type, before anything downstream runs.
async fn json_body_not_an_object(rig: FakeRig) -> WeftResult<()> {
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!([1, 2, 3])));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("body does not fit JsonDict"), "{error}");
    assert!(outcome.outputs.is_empty());

    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!("hello there");
    rig.wake(wake);
    let conn = http_caller_without_body(DataType::Json, request);
    rig.attach_caller(conn.clone());
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let (status, error) = refusal(&conn);
    assert_eq!(status, 400);
    assert!(error.contains("body does not fit JsonDict"), "{error}");
    assert!(outcome.outputs.is_empty());
    Ok(())
}

/// Declared `Json`, a body is whatever JSON the caller sent: a list of
/// lists from a live call, a bare string from a fired run.
async fn json_body_any_json(rig: FakeRig) -> WeftResult<()> {
    let conn = call(&rig, DataType::Json, InboundMessage::Json(json!([[1, "a"], [2, "b"]])));
    rig.output_type("body", ty("Json"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!([[1, "a"], [2, "b"]]));
    assert!(conn.heads().is_empty(), "nothing refused");

    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!("hello there");
    rig.wake(wake);
    rig.attach_caller(http_caller_without_body(DataType::Json, request));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!("hello there"));
    Ok(())
}

/// No body at all is not a bad body. It is how every GET arrives, so the
/// request ports still fire and `body` closes.
async fn json_body_absent(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let conn = http_caller_without_body(DataType::Json, request);
    rig.attach_caller(conn.clone());
    rig.output_type("body", ty("{ email: String }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert!(outcome.outputs.get("body").is_none(), "no body, so `body` closes");
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "and the request still arrives");
    assert!(conn.heads().is_empty(), "nothing refused");
    Ok(())
}

/// A FIRED run has no connection, so the body the author typed arrives
/// as this trigger's own wake field and the node reads it there.
///
/// Nothing upstream lifts it out of the payload to serve it back as if
/// a caller had sent it: that would mean the language holding this
/// node's field name. So the node looks at the connection first, finds
/// nothing, and reads its own wake.
async fn fired_body_comes_from_the_wake(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!({ "userId": "u-1", "text": "hi" });
    rig.wake(wake);
    // No connection body, exactly as a fired run arrives.
    rig.attach_caller(http_caller_without_body(DataType::Json, request));
    rig.output_type("body", ty("{ userId: String, text: String }"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!({ "userId": "u-1", "text": "hi" }));
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "and the request still arrives");
    assert_eq!(outcome.outputs["fired"], json!(true), "a program can gate on the call alone");
    Ok(())
}

/// A text route fired by hand carries its body as a string, and it
/// lands whole on `body`, the same as a real text call.
async fn fired_text_body_comes_from_the_wake(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!("hello there");
    rig.wake(wake);
    rig.attach_caller(http_caller_without_body(DataType::Text, request));
    rig.output_type("body", ty("String"));
    let outcome = rig
        .run(&RouteNode, json!({ "path": "chat/{room}", "dataType": "text" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["body"], json!("hello there"));
    Ok(())
}

/// A request with no body (a GET, or a fire with no `body` in its
/// payload) closes `body` on a text or a bytes route as on a json one,
/// and the request ports still fire.
async fn fired_without_a_body(rig: FakeRig) -> WeftResult<()> {
    for (data_type, field, declared) in [(DataType::Text, "text", "String"), (DataType::Bytes, "bytes", "Image")] {
        let request = request();
        rig.wake(serde_json::to_value(&request).expect("request serializes"));
        let conn = http_caller_without_body(data_type, request);
        rig.attach_caller(conn.clone());
        rig.output_type("body", ty(declared));
        let outcome = rig
            .run(&RouteNode, json!({ "path": "chat/{room}", "dataType": field }))
            .await
            .ok()?;
        assert!(outcome.outputs.get("body").is_none(), "{field}: no body, so `body` closes");
        assert_eq!(outcome.outputs["path"], json!("chat/room7"), "{field}: the request still arrives");
        assert!(conn.heads().is_empty(), "{field}: nothing refused");
    }
    Ok(())
}

/// A json body that is `null` is a value, apart from no body at all: a
/// body declared `Json` (which takes null) carries it on, and the default
/// `JsonDict`, which takes no null, closes as for nothing.
async fn null_body(rig: FakeRig) -> WeftResult<()> {
    let conn = call(&rig, DataType::Json, InboundMessage::Json(Value::Null));
    rig.output_type("body", ty("Json"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs.get("body"), Some(&Value::Null), "the null is the body: {:?}", outcome.outputs);
    assert!(conn.heads().is_empty(), "nothing refused");

    let conn = call(&rig, DataType::Json, InboundMessage::Json(Value::Null));
    rig.output_type("body", ty("JsonDict"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert!(outcome.outputs.get("body").is_none(), "a null where no null is taken closes `body`");
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "and the request still arrives");
    assert!(conn.heads().is_empty(), "nothing refused");
    Ok(())
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &RouteNode,
        json!({ "path": "chat/{room}", "method": "post", "dataType": "text", "outlivesCaller": true }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes");
    assert_eq!(spec["kind"], "route");
    assert_eq!(spec["config"]["path"], "chat/{room}");
    assert_eq!(spec["config"]["methods"], json!(["POST"]));
    assert_eq!(spec["config"]["data_type"], "text");
    assert_eq!(spec["config"]["outlives_caller"], true);
    assert_eq!(spec["config"]["auth"]["kind"], "none");
    assert_eq!(spec["config"]["error_mode"], "surface", "a route that says nothing tells its caller");
    assert_eq!(spec["config"]["backpressure"], "block", "a route that says nothing waits for a slow caller");
    assert_eq!(spec["config"]["journal_mode"], "journaled", "a route that says nothing is written down");
    assert_eq!(spec["config"]["journal_window_ms"], 1000, "on the window every channel starts from");

    rig.run_setup_trigger(&RouteNode, json!({ "path": "quiet", "journalEphemeral": true, "journalWindowMs": 250 }))
        .await
        .ok()?;
    let spec = serde_json::to_value(&rig.registered_signals().last().expect("a quiet route").0).expect("spec serializes");
    assert_eq!(spec["config"]["journal_mode"], "ephemeral");
    assert_eq!(spec["config"]["journal_window_ms"], 250, "the journal settings reach the route");

    rig.run_setup_trigger(
        &RouteNode,
        json!({ "path": "feed", "whenChunkFails": "drop_chunk", "whenCallerIsSlow": "drop_oldest" }),
    )
    .await
    .ok()?;
    let spec = serde_json::to_value(&rig.registered_signals().last().expect("a second route").0).expect("spec serializes");
    assert_eq!(spec["config"]["error_mode"], "drop_chunk");
    assert_eq!(spec["config"]["backpressure"], "drop_oldest");
    Ok(())
}

async fn no_caller(rig: FakeRig) -> WeftResult<()> {
    rig.wake(serde_json::to_value(request()).expect("request serializes"));
    let err = rig.run(&RouteNode, json!({ "path": "x" })).await.failure()?;
    assert!(err.contains("Route"), "{err}");
    Ok(())
}

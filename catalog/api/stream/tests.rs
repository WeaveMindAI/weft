//! Stream self-tests: every bus message reaches the caller in order under
//! the format's framing, the head rides the first chunk, the bus closing
//! ends the HTTP response, a socket stays open, a bus that ended failed
//! fails the exchange instead, and the pure framing.

use serde_json::json;

use weft::bus::{BusOptions, StreamEnd};
use weft::caller::{CallerCall, InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult};

use super::StreamNode;
use crate::framing::{ndjson_line, sse_event, Format};
use crate::testing::{caller_skipping_unmade_chunks, http_caller, http_caller_without_body, ws_caller};
use weft::signal::Protocol;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sse_frames_each_message_and_ends_with_the_bus", sse_stream),
        NodeTest::fake("raw_sends_payloads_as_they_are", raw_stream),
        NodeTest::fake("an_empty_bus_still_answers_with_the_head", empty_stream),
        NodeTest::fake("a_socket_gets_one_message_per_bus_message_and_stays_open", socket_stream),
        NodeTest::basic("formats_parse_and_name_their_content_type", formats),
        NodeTest::fake("first_goes_out_ahead_of_the_bus", first_then_bus),
        NodeTest::fake("a_bus_that_broke_off_fails_the_answer_after_what_went_out", failed_mid_stream),
        NodeTest::fake("a_bus_that_broke_off_before_a_message_fails_with_nothing_sent", failed_before_a_message),
        NodeTest::fake("a_bus_that_broke_off_closes_a_socket_as_failed", failed_on_a_socket),
        NodeTest::basic("sse_frames_text_per_line_and_json_whole", sse_framing),
        NodeTest::basic("ndjson_is_one_json_value_per_line", ndjson_framing),
        NodeTest::fake("a_redirect_is_a_replys_answer", redirect_refused),
        NodeTest::fake("a_message_it_cannot_send_fails_the_answer_or_is_skipped", unmade_message),
    ]
}

/// A bus carrying `messages` (as `delta` payloads) and already closed:
/// the producer finished before the stream node reads, which the
/// from-start cursor must cover.
fn closed_bus(rig: &FakeRig, messages: &[serde_json::Value]) -> WeftResult<serde_json::Value> {
    let (mut bus, marker) = rig.seed_bus(BusOptions::default())?;
    bus.register("llm").expect("a fresh bus accepts its first name");
    for m in messages {
        bus.send("delta", m.clone()).expect("an open bus accepts a send");
    }
    bus.close();
    Ok(marker)
}

/// A bus carrying `messages` whose producer then failed: weft ended it
/// failed, as it does a producer's bus when its body returns an error.
fn failed_bus(rig: &FakeRig, messages: &[serde_json::Value]) -> WeftResult<serde_json::Value> {
    let (mut bus, marker) = rig.seed_bus(BusOptions::default())?;
    bus.register("llm").expect("a fresh bus accepts its first name");
    for m in messages {
        bus.send("delta", m.clone()).expect("an open bus accepts a send");
    }
    bus.inner_arc().close(StreamEnd::Failed { error: "the model went away".into() });
    Ok(marker)
}

/// A redirect is one answer that ends the exchange, so a Stream refuses
/// it and names the node that sends one; nothing goes out.
async fn redirect_refused(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("a")])?;
    let err = rig.run(&StreamNode, json!({ "bus": bus, "answerAs": "redirect" })).await.failure()?;
    assert!(err.contains("send it with a Reply"), "{err}");
    assert!(conn.chunks().is_empty(), "{:?}", conn.chunks());
    Ok(())
}

/// The deltas that went out stay sent; then the exchange FAILS, which the
/// connection writes as the format's error marker, and `done` stays
/// absent: a cut answer never reads as a whole one.
async fn failed_mid_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = failed_bus(&rig, &[json!("hel")])?;
    let outcome = rig.run(&StreamNode, json!({ "bus": bus, "format": "sse" })).await.ok()?;
    assert!(outcome.outputs.get("done").is_none(), "{:?}", outcome.outputs);
    let calls = conn.calls();
    assert!(matches!(calls.last(), Some(CallerCall::Fail { message }) if message.contains("the model went away")), "{calls:?}");
    assert_eq!(conn.chunks(), vec![OutboundChunk::Text("data: hel\n\n".into())]);
    assert!(!calls.iter().any(|c| matches!(c, CallerCall::Terminate { .. })), "never ended as whole: {calls:?}");
    Ok(())
}

/// Nothing went out yet: the failure is the whole answer (a real error
/// status, written by the connection), and no head or chunk precedes it.
async fn failed_before_a_message(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = failed_bus(&rig, &[])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson" })).await.ok()?;
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Fail { message: "the stream broke off: the model went away".into() },
        ]
    );
    Ok(())
}

/// Behind a socket, the frames that went out stay sent and the socket
/// closes as failed.
async fn failed_on_a_socket(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let bus = failed_bus(&rig, &[json!("a")])?;
    rig.run(&StreamNode, json!({ "bus": bus })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Json(json!("a"))]);
    assert!(matches!(conn.calls().last(), Some(CallerCall::Fail { .. })), "{:?}", conn.calls());
    Ok(())
}

async fn sse_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("hel"), json!("lo")])?;
    let outcome = rig
        .run(&StreamNode, json!({ "bus": bus, "format": "sse", "headers": { "x-run": "r-1" } }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::SendChunk {
                head: Some(
                    ResponseHead::new(200)
                        .with_header("x-run", "r-1")
                        .with_header("content-type", "text/event-stream")
                        .with_keepalive(": keepalive\n\n")
                ),
                chunk: OutboundChunk::Text("data: hel\n\n".into()),
            },
            CallerCall::SendChunk { head: None, chunk: OutboundChunk::Text("data: lo\n\n".into()) },
            CallerCall::Terminate { head: None, final_chunk: None, close: None },
        ]
    );
    Ok(())
}

/// The header every streaming API has: one value first, then the feed,
/// framed the same way so the caller reads one stream and not two. It
/// used to be inexpressible, and the way round it was writing a node.
async fn first_then_bus(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("hel")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson", "first": { "id": 7 } }))
        .await
        .ok()?;
    let sent: Vec<OutboundChunk> = conn.chunks();
    assert_eq!(
        sent,
        vec![
            OutboundChunk::Text("{\"id\":7}\n".into()),
            OutboundChunk::Text("\"hel\"\n".into()),
        ],
        "the header leads, framed like everything behind it"
    );

    // Without it the response is the bus alone, byte for byte.
    let plain = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(plain.clone());
    let bus = closed_bus(&rig, &[json!("hel")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson" })).await.ok()?;
    assert_eq!(plain.chunks(), vec![OutboundChunk::Text("\"hel\"\n".into())]);
    Ok(())
}

async fn raw_stream(rig: FakeRig) -> WeftResult<()> {
    // Raw is every byte the author's, so the head carries no filler and
    // there is nothing to write into a quiet connection. That used to be
    // refused. It is not any more: a caller who vanishes is found by
    // their machine no longer acknowledging what it was sent, which the
    // connection layer bounds on every socket whatever the framing, so
    // raw is watched like the rest and streams payloads exactly as they
    // are.
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!({ "n": 1 }), json!("t")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "status": 202 })).await.ok()?;
    assert_eq!(conn.heads(), vec![ResponseHead::new(202)], "raw names no content type and no filler (every byte is payload); the wire fills the type");
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Json(json!({ "n": 1 })), OutboundChunk::Json(json!("t"))]
    );
    Ok(())
}

async fn empty_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller_without_body(DataType::Json, LiveRequest::default());
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson" })).await.ok()?;
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Terminate {
                head: Some(ResponseHead::new(200).with_header("content-type", "application/x-ndjson").with_keepalive("\n")),
                final_chunk: None,
                close: None,
            },
        ]
    );
    Ok(())
}

async fn socket_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("a"), json!("b")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "sse" })).await.ok()?;
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Json(json!("a")), OutboundChunk::Json(json!("b"))],
        "a socket message is a frame: no sse framing, no head"
    );
    assert!(conn.heads().is_empty());
    assert_eq!(conn.close_reason(), None, "the socket stays open");
    let err = rig.run(&StreamNode, json!({ "bus": bus, "status": 500 })).await.failure()?;
    assert!(err.contains("no meaning on a socket"), "{err}");
    Ok(())
}

fn formats() -> WeftResult<()> {
    assert_eq!(Format::parse("sse").map_err(weft::node_error)?, Format::Sse);
    assert_eq!(Format::parse("ndjson").map_err(weft::node_error)?.content_type(), Some("application/x-ndjson"));
    assert_eq!(Format::parse("raw").map_err(weft::node_error)?.content_type(), None);
    assert!(Format::parse("xml").is_err());
    // The filler a quiet stream writes is one a reader of the format
    // drops: a comment event, an empty line; raw has none.
    assert_eq!(Format::Sse.keepalive(), Some(": keepalive\n\n"));
    assert_eq!(Format::Ndjson.keepalive(), Some("\n"));
    assert_eq!(Format::Raw.keepalive(), None);
    Ok(())
}

fn sse_framing() -> WeftResult<()> {
    assert_eq!(sse_event(&json!("hel")), "data: hel\n\n");
    assert_eq!(sse_event(&json!("two\nlines")), "data: two\ndata: lines\n\n");
    assert_eq!(sse_event(&json!({ "a": 1 })), "data: {\"a\":1}\n\n");
    assert_eq!(sse_event(&json!("")), "data: \n\n", "an empty delta is still an event");
    Ok(())
}

fn ndjson_framing() -> WeftResult<()> {
    assert_eq!(ndjson_line(&json!("hel")), "\"hel\"\n");
    assert_eq!(ndjson_line(&json!({ "a": 1 })), "{\"a\":1}\n");
    Ok(())
}

/// A message the caller's data type cannot carry (an object on a text
/// route): by default the Stream fails after what already went out; a
/// trigger set to skip such a chunk (`whenChunkFails: drop_chunk`) has it
/// skipped, said to the connection, and the stream carries on to `done`,
/// on a route and on a socket alike.
async fn unmade_message(rig: FakeRig) -> WeftResult<()> {
    let messages = [json!("a"), json!({ "n": 1 }), json!("c")];

    let strict = http_caller(DataType::Text, LiveRequest::default(), InboundMessage::Text(String::new()));
    rig.attach_caller(strict.clone());
    let bus = closed_bus(&rig, &messages)?;
    let err = rig.run(&StreamNode, json!({ "bus": bus })).await.failure()?;
    assert!(err.contains("must be a String"), "{err}");
    assert_eq!(strict.chunks(), vec![OutboundChunk::Text("a".into())], "nothing after the message it could not send");

    for protocol in [Protocol::Http, Protocol::Websocket] {
        let skipping = caller_skipping_unmade_chunks(protocol, DataType::Text);
        rig.attach_caller(skipping.clone());
        let bus = closed_bus(&rig, &messages)?;
        let outcome = rig.run(&StreamNode, json!({ "bus": bus })).await.ok()?;
        assert_eq!(outcome.outputs["done"], json!(true), "{protocol:?}");
        assert_eq!(
            skipping.chunks(),
            vec![OutboundChunk::Text("a".into()), OutboundChunk::Text("c".into())],
            "{protocol:?}: the stream went on past the message it skipped"
        );
        let skipped: Vec<_> = skipping.calls().into_iter().filter(|c| matches!(c, CallerCall::ChunkDropped { .. })).collect();
        assert!(
            matches!(skipped.as_slice(), [CallerCall::ChunkDropped { why }] if why.contains("must be a String")),
            "{protocol:?}: {skipped:?}"
        );
    }
    Ok(())
}

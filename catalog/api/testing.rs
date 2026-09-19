//! Shared fake-test plumbing for the api package: a connected fake
//! caller of either protocol, with a scripted opening request, body and
//! inbound messages, for the nodes that talk to whoever the run has.
#![cfg(feature = "node-tests")]

use std::sync::Arc;

use serde_json::json;

use weft::caller::{
    CallerRuntimeConfig, FakeCallerConnection, InboundMessage, LiveRequest, DEFAULT_INBOUND_WINDOW,
};
use weft::signal::{Backpressure, DataType, ErrorMode, Protocol};
use weft::wait::SuspendPolicy;

fn config(protocol: Protocol, data_type: DataType) -> CallerRuntimeConfig {
    CallerRuntimeConfig {
        protocol,
        data_type,
        backpressure: Backpressure::Block,
        error_mode: ErrorMode::Surface,
        connect_timeout_secs: 5,
        max_inbound_bytes: 1 << 20,
        // The fake caller is never a socket, so nothing here reads this;
        // the default keeps it honest against the real config.
        caller_silence_secs: weft::signal::DEFAULT_CALLER_SILENCE_SECS,
        max_session_secs: 0,
        suspend: SuspendPolicy::default(),
        inbound_window: DEFAULT_INBOUND_WINDOW,
        // A fake run writes no journal, so the default is here to keep
        // this config the same shape as the real one.
        journal: weft::stream_journal::JournalPolicy::default(),
    }
}

/// A connected fake HTTP caller speaking `data_type`, with the given
/// opening request and body.
pub fn http_caller(
    data_type: DataType,
    request: LiveRequest,
    body: InboundMessage,
) -> Arc<FakeCallerConnection> {
    let conn = FakeCallerConnection::connected(config(Protocol::Http, data_type));
    conn.set_handshake(request);
    conn.set_http_body(body);
    conn
}

/// A connected fake WebSocket caller speaking `data_type`, with the
/// given opening request and scripted messages; the script running dry
/// is the caller disconnecting.
pub fn ws_caller(
    data_type: DataType,
    request: LiveRequest,
    messages: Vec<InboundMessage>,
) -> Arc<FakeCallerConnection> {
    let conn = FakeCallerConnection::connected(config(Protocol::Websocket, data_type));
    conn.set_handshake(request);
    for m in messages {
        conn.push_inbound(m);
    }
    conn
}

/// A representative opening request: a POST on `chat/{room}` with a
/// capture, a query parameter, a header and a gate identity.
pub fn request() -> LiveRequest {
    let mut request = LiveRequest {
        method: "POST".into(),
        path: "chat/room7".into(),
        ..Default::default()
    };
    request.params.insert("room".into(), "room7".into());
    request.query.insert("verbose".into(), "1".into());
    request.headers.push(("Content-Type".into(), "application/json".into()));
    request.caller = Some(json!({ "key": 0 }));
    request
}

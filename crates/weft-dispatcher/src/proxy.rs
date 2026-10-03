//! Passing a caller's request on to another server, a WebSocket
//! included: the live door to a project's workers, the install's front
//! door to a role or a public infra endpoint.
//!
//! The request reaches the upstream as the caller sent it: method, path,
//! query, headers and body. Only the hop's own headers are dropped, and
//! weft's credential for the upstream rides a header of its own
//! ([`weft_platform_traits::WORKER_AUTH_HEADER`]), so a caller's own
//! `Authorization` reaches the upstream untouched. The door says who the
//! caller is and which address it used (`X-Forwarded-For`, `-Host`,
//! `-Proto`), since the upstream sees the door as its peer and its own
//! address as the host: that is what it builds links from
//! (`weft_core::net::request_base_url`) and counts callers by
//! (`crate::entry_limits::caller_address`).

use axum::extract::ws::{CloseFrame, Message as CallerMessage, WebSocket, WebSocketUpgrade};
use std::net::{IpAddr, SocketAddr};

use axum::extract::{ConnectInfo, FromRequestParts, Request};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::{self, Message as WorkerMessage};

/// Where to pass a request on to.
pub struct Upstream {
    /// For messages: "the worker", "the dispatcher".
    pub what: &'static str,
    pub base_url: String,
    /// The header and value weft's credential for the upstream rides.
    pub auth: Option<(HeaderName, HeaderValue)>,
    /// Kept for as long as the caller talks to the upstream.
    pub hold: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl Upstream {
    /// A project's worker, at the address the platform gave.
    pub fn worker(endpoint: weft_platform_traits::WorkerEndpoint) -> Result<Self, axum::http::header::InvalidHeaderValue> {
        let auth = HeaderValue::from_str(&endpoint.auth_value())?;
        Ok(Self {
            what: "the worker",
            base_url: endpoint.base_url,
            auth: Some((HeaderName::from_static(weft_platform_traits::WORKER_AUTH_HEADER), auth)),
            hold: endpoint.hold,
        })
    }
}

/// Pass `request` on to `upstream` at `path_and_query`.
pub async fn forward(http: &reqwest::Client, upstream: Upstream, path_and_query: String, request: Request) -> Response {
    let Some(peer) = request.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip()) else {
        // Every server weft runs is started with connection info; a request
        // without it is a wiring bug, refused rather than passed on with
        // its caller unnamed.
        return (StatusCode::INTERNAL_SERVER_ERROR, "no peer address on the request").into_response();
    };
    let forwarded = match forwarded_headers(request.headers(), peer) {
        Ok(forwarded) => forwarded,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("the request's forwarding headers: {e}")).into_response(),
    };
    if is_websocket(request.headers()) {
        forward_socket(upstream, path_and_query, request, forwarded).await
    } else {
        forward_http(http, upstream, path_and_query, request, forwarded).await
    }
}

/// What the upstream is told about the caller. A host and scheme an
/// earlier door set are kept (Cloud Run's front end, which terminated
/// TLS, says `https`); otherwise they are this hop's own: the `Host` the caller
/// sent, over plain HTTP, which is all weft's own ports speak. The peer is
/// appended to the address chain, which is read from the right, so
/// whatever a caller wrote to its left never counts.
fn forwarded_headers(headers: &HeaderMap, peer: IpAddr) -> Result<Vec<(HeaderName, HeaderValue)>, axum::http::header::InvalidHeaderValue> {
    let mut out = Vec::with_capacity(4);
    let chain = match headers.get(X_FORWARDED_FOR).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty()) {
        Some(earlier) => format!("{earlier}, {peer}"),
        None => peer.to_string(),
    };
    out.push((HeaderName::from_static(X_FORWARDED_FOR), HeaderValue::from_str(&chain)?));
    if let Some(host) = headers.get(X_FORWARDED_HOST).or_else(|| headers.get(axum::http::header::HOST)) {
        out.push((HeaderName::from_static(X_FORWARDED_HOST), host.clone()));
    }
    let proto = headers
        .get(weft_core::net::WEFT_FORWARDED_PROTO)
        .or_else(|| headers.get(X_FORWARDED_PROTO))
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("http"));
    out.push((HeaderName::from_static(X_FORWARDED_PROTO), proto.clone()));
    // The scheme again under weft's own name: Cloud Run's front end
    // overwrites `X-Forwarded-Proto` with the https it was reached on,
    // so a caller on the machine's plain-http private port would be
    // handed https links to that port.
    out.push((HeaderName::from_static(weft_core::net::WEFT_FORWARDED_PROTO), proto));
    Ok(out)
}

const X_FORWARDED_FOR: &str = "x-forwarded-for";
const X_FORWARDED_HOST: &str = "x-forwarded-host";
const X_FORWARDED_PROTO: &str = "x-forwarded-proto";

/// Headers that belong to one hop and never travel past it, plus the ones
/// this door sets itself.
fn is_hop_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | X_FORWARDED_FOR
            | X_FORWARDED_HOST
            | X_FORWARDED_PROTO
            | weft_core::net::WEFT_FORWARDED_PROTO
    ) || name.as_str() == weft_platform_traits::WORKER_AUTH_HEADER
}

/// The headers of a WebSocket handshake that the connection to the worker
/// makes afresh (its own key, version and extensions).
fn is_ws_handshake_header(name: &HeaderName) -> bool {
    matches!(name.as_str(), "sec-websocket-key" | "sec-websocket-version" | "sec-websocket-extensions" | "sec-websocket-accept")
}

fn is_websocket(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

async fn forward_http(
    http: &reqwest::Client,
    upstream: Upstream,
    path_and_query: String,
    request: Request,
    forwarded: Vec<(HeaderName, HeaderValue)>,
) -> Response {
    let (parts, body) = request.into_parts();
    let url = format!("{}{path_and_query}", upstream.base_url.trim_end_matches('/'));
    let mut call = http.request(parts.method, &url);
    for (name, value) in parts.headers.iter().filter(|(name, _)| !is_hop_header(name)) {
        call = call.header(name, value);
    }
    for (name, value) in forwarded {
        call = call.header(name, value);
    }
    if let Some((name, value)) = &upstream.auth {
        call = call.header(name, value);
    }
    let sent = call.body(reqwest::Body::wrap_stream(body.into_data_stream())).send().await;
    let answer = match sent {
        Ok(answer) => answer,
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::proxy", upstream = %upstream.what, error = %e, "could not reach the upstream");
            return (StatusCode::BAD_GATEWAY, format!("{} could not be reached: {e}", upstream.what)).into_response();
        }
    };
    let mut response = Response::builder().status(answer.status());
    for (name, value) in answer.headers().iter().filter(|(name, _)| !is_hop_header(name)) {
        response = response.header(name, value);
    }
    // The upstream's hold rides the body: a platform that stops idle
    // workers itself never stops this one while the caller still reads.
    let hold = upstream.hold;
    let body = answer.bytes_stream().map(move |chunk| {
        let _held = &hold;
        chunk
    });
    response
        .body(axum::body::Body::from_stream(body))
        .unwrap_or_else(|e| (StatusCode::BAD_GATEWAY, format!("the answer of {}: {e}", upstream.what)).into_response())
}

/// Open the upstream's socket first, then the caller's: an upstream that
/// refuses (a worker holding a spent ticket) answers the caller in plain
/// HTTP with its own status and words.
async fn forward_socket(
    upstream: Upstream,
    path_and_query: String,
    request: Request,
    forwarded: Vec<(HeaderName, HeaderValue)>,
) -> Response {
    let (mut parts, _body) = request.into_parts();
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(upgrade) => upgrade,
        Err(rejection) => return rejection.into_response(),
    };
    let base = upstream.base_url.trim_end_matches('/');
    let ws_base = match base.split_once("://") {
        Some(("https", rest)) => format!("wss://{rest}"),
        Some(("http", rest)) => format!("ws://{rest}"),
        _ => return (StatusCode::BAD_GATEWAY, format!("the address of {} ('{base}') is not http(s)", upstream.what)).into_response(),
    };
    let mut call = match tungstenite::client::IntoClientRequest::into_client_request(format!("{ws_base}{path_and_query}")) {
        Ok(call) => call,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("the socket address of {}: {e}", upstream.what)).into_response(),
    };
    for (name, value) in parts.headers.iter().filter(|(name, _)| !is_hop_header(name) && !is_ws_handshake_header(name)) {
        call.headers_mut().append(name.clone(), value.clone());
    }
    for (name, value) in forwarded {
        call.headers_mut().insert(name, value);
    }
    if let Some((name, value)) = &upstream.auth {
        call.headers_mut().insert(name.clone(), value.clone());
    }
    let (worker, answer) = match tokio_tungstenite::connect_async(call).await {
        Ok(opened) => opened,
        Err(tungstenite::Error::Http(refused)) => {
            let status = StatusCode::from_u16(refused.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = refused.body().as_deref().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
            // The refusal's own headers ride along: whether a worker gave
            // it is read off them (`WORKER_ANSWER_HEADER`).
            let mut answer = (status, body).into_response();
            for (name, value) in refused.headers().iter().filter(|(name, _)| !is_hop_header(name)) {
                if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(name.as_str().as_bytes()), HeaderValue::from_bytes(value.as_bytes())) {
                    answer.headers_mut().append(name, value);
                }
            }
            return answer;
        }
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::proxy", upstream = %upstream.what, error = %e, "could not open the upstream socket");
            return (StatusCode::BAD_GATEWAY, format!("the socket of {} could not be opened: {e}", upstream.what)).into_response();
        }
    };
    // The caller gets the subprotocol the upstream picked, if any.
    let chosen: Vec<String> = answer
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|p| vec![p.to_string()])
        .unwrap_or_default();
    let hold = upstream.hold;
    upgrade.protocols(chosen).on_upgrade(move |caller| async move {
        pump(caller, worker).await;
        drop(hold);
    })
}

/// Pass messages both ways until either side ends, then end the other.
/// Each hop answers its own pings; a close is passed along with its code.
async fn pump(
    caller: WebSocket,
    worker: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) {
    let (mut to_caller, mut from_caller) = caller.split();
    let (mut to_worker, mut from_worker) = worker.split();
    let upstream = async {
        while let Some(Ok(message)) = from_caller.next().await {
            let Some(message) = caller_to_worker(message) else { continue };
            let closing = matches!(message, WorkerMessage::Close(_));
            if to_worker.send(message).await.is_err() || closing {
                break;
            }
        }
        let _ = to_worker.close().await;
    };
    let downstream = async {
        while let Some(Ok(message)) = from_worker.next().await {
            let Some(message) = worker_to_caller(message) else { continue };
            let closing = matches!(message, CallerMessage::Close(_));
            if to_caller.send(message).await.is_err() || closing {
                break;
            }
        }
        let _ = to_caller.close().await;
    };
    // Whichever side ends first ends the whole connection.
    tokio::select! {
        () = upstream => {}
        () = downstream => {}
    }
}

fn caller_to_worker(message: CallerMessage) -> Option<WorkerMessage> {
    Some(match message {
        CallerMessage::Text(text) => WorkerMessage::Text(text.to_string()),
        CallerMessage::Binary(bytes) => WorkerMessage::Binary(bytes.to_vec()),
        CallerMessage::Close(frame) => WorkerMessage::Close(frame.map(|f| tungstenite::protocol::CloseFrame {
            code: f.code.into(),
            reason: f.reason.to_string().into(),
        })),
        CallerMessage::Ping(_) | CallerMessage::Pong(_) => return None,
    })
}

fn worker_to_caller(message: WorkerMessage) -> Option<CallerMessage> {
    Some(match message {
        WorkerMessage::Text(text) => CallerMessage::Text(text.into()),
        WorkerMessage::Binary(bytes) => CallerMessage::Binary(bytes.into()),
        WorkerMessage::Close(frame) => {
            CallerMessage::Close(frame.map(|f| CloseFrame { code: f.code.into(), reason: f.reason.to_string().into() }))
        }
        WorkerMessage::Ping(_) | WorkerMessage::Pong(_) | WorkerMessage::Frame(_) => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A caller's own credential reaches the program; only the hop's
    /// headers and weft's own credential header are the door's.
    #[test]
    fn only_the_hops_headers_stay_behind() {
        for kept in ["authorization", "cookie", "content-type", "content-length", "sec-websocket-protocol"] {
            assert!(!is_hop_header(&HeaderName::from_static(kept)), "{kept}");
        }
        for dropped in ["host", "connection", "upgrade", "transfer-encoding", "x-forwarded-for", weft_platform_traits::WORKER_AUTH_HEADER] {
            assert!(is_hop_header(&HeaderName::from_static(dropped)), "{dropped}");
        }
    }

    fn told(headers: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(HeaderName::from_bytes(name.as_bytes()).unwrap(), HeaderValue::from_str(value).unwrap());
        }
        forwarded_headers(&map, "203.0.113.9".parse().unwrap())
            .unwrap()
            .into_iter()
            .map(|(n, v)| (n.to_string(), v.to_str().unwrap().to_string()))
            .collect()
    }

    /// The upstream learns the caller's own host and address; what an
    /// earlier door said about host and scheme wins over this hop's.
    #[test]
    fn the_upstream_is_told_who_called_and_at_which_address() {
        assert_eq!(
            told(&[("host", "10.10.0.2:14113")]),
            vec![
                ("x-forwarded-for".into(), "203.0.113.9".into()),
                ("x-forwarded-host".into(), "10.10.0.2:14113".into()),
                ("x-forwarded-proto".into(), "http".into()),
                ("x-weft-forwarded-proto".into(), "http".into()),
            ]
        );
        assert_eq!(
            told(&[("host", "127.0.0.1:14111"), ("x-forwarded-host", "weft.example.com"), ("x-forwarded-proto", "https"), ("x-forwarded-for", "198.51.100.1")]),
            vec![
                ("x-forwarded-for".into(), "198.51.100.1, 203.0.113.9".into()),
                ("x-forwarded-host".into(), "weft.example.com".into()),
                ("x-forwarded-proto".into(), "https".into()),
                ("x-weft-forwarded-proto".into(), "https".into()),
            ]
        );
    }

    /// Cloud Run's front end, between a door and the dispatcher, says
    /// https whatever the caller used; the scheme weft's door passed
    /// under its own name is the one that holds.
    #[test]
    fn the_scheme_a_door_passed_survives_a_front_end_that_rewrites_it() {
        let told = told(&[("host", "dispatcher.run.app"), ("x-forwarded-proto", "https"), ("x-weft-forwarded-proto", "http")]);
        assert!(told.contains(&("x-forwarded-proto".into(), "http".into())), "{told:?}");
        let mut map = HeaderMap::new();
        map.insert("x-forwarded-host", HeaderValue::from_static("10.10.0.2:14113"));
        map.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        map.insert("x-weft-forwarded-proto", HeaderValue::from_static("http"));
        assert_eq!(weft_core::net::request_base_url(&map).as_deref(), Some("http://10.10.0.2:14113"));
    }
}

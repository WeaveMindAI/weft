//! The live-caller path: an outside party holds an HTTP stream or a two-way
//! WebSocket against a running program.
//!
//! A call is `/connect/<tenant>/{path}` on the install's shared address,
//! any method, and is answered in that one request: the install passes it
//! on to the project's workers, whose door checks the caller and starts the
//! run. A socket is opened there the same way, by a client that sends the
//! upgrade itself.
//!
//! A browser cannot put a credential on a socket's opening request, so
//! the route offers it two steps instead, which [`ticket`] and
//! [`open_socket_at`] take: a plain GET answers
//! `200 { "url": "...", "protocol": "websocket" }`, the route's own URL
//! at the address the caller used, carrying a signed ticket, and the socket
//! is opened there. These helpers hit absolute URLs.

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

use crate::client::Dispatcher;

/// A connected live WebSocket to a worker. Send and receive JSON messages until
/// the test closes it (which ends the run for a caller-tied program).
pub struct LiveWs {
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

/// Open a live WebSocket on `mount_path` in one request, the way a client
/// that can send headers does.
pub async fn open_ws(disp: &Dispatcher, mount_path: &str) -> Result<LiveWs> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    match open_socket_at(&url).await? {
        Ok(ws) => Ok(ws),
        Err((status, body)) => anyhow::bail!("WebSocket connect to {url} answered {status}: {body}"),
    }
}

/// Open a socket at `url` (`http(s)` swapped to `ws(s)`), the second step of
/// a [`ticket`] among others: the socket, or the status and words of a
/// refusal, which a test may assert (a late caller's).
pub async fn open_socket_at(url: &str) -> Result<std::result::Result<LiveWs, (reqwest::StatusCode, String)>> {
    let ws_url = url.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
    match tokio_tungstenite::connect_async(&ws_url).await {
        Ok((stream, _)) => Ok(Ok(LiveWs { stream })),
        Err(tokio_tungstenite::tungstenite::Error::Http(refused)) => Ok(Err((
            reqwest::StatusCode::from_u16(refused.status().as_u16()).expect("a status the socket client parsed is a valid status"),
            refused.body().as_deref().map(String::from_utf8_lossy).unwrap_or_default().into_owned(),
        ))),
        Err(e) => Err(e).with_context(|| format!("WebSocket connect to {ws_url}")),
    }
}

impl LiveWs {
    /// Send a JSON value as a text frame. The default trigger data type is JSON,
    /// so a string payload must be sent as JSON (`json!("hi")`), not bare text.
    pub async fn send_json(&mut self, value: &Value) -> Result<()> {
        let text = serde_json::to_string(value).context("serialize ws message")?;
        self.stream
            .send(Message::Text(text))
            .await
            .context("ws send")
    }

    /// Receive the next DATA message and parse it as JSON. Control frames
    /// (Ping/Pong, raw frames) are skipped: a worker sends keepalive pings on a
    /// quiet socket, which are protocol noise, not program messages. Errors if
    /// the socket closes before a data message arrives or the frame is not valid
    /// JSON. Bounded by `timeout` (the whole wait, across any skipped pings) so a
    /// test never hangs on a silent server.
    pub async fn recv_json(&mut self, timeout: std::time::Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let next = tokio::time::timeout_at(deadline, self.stream.next())
                .await
                .context("timed out waiting for a ws data message")?;
            match next {
                Some(Ok(Message::Text(t))) => {
                    return serde_json::from_str(&t)
                        .with_context(|| format!("ws message not JSON: {t}"));
                }
                Some(Ok(Message::Binary(b))) => {
                    return serde_json::from_slice(&b).context("ws binary message not JSON");
                }
                Some(Ok(Message::Close(_))) => {
                    bail!("ws closed by program before a data message arrived")
                }
                // Keepalive / control frames: tungstenite auto-responds to Ping;
                // we just skip them and keep waiting for real data.
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Frame(_))) => continue,
                Some(Err(e)) => return Err(e).context("ws receive error"),
                None => bail!("ws stream ended before a data message arrived"),
            }
        }
    }

    /// Send a JSON message and await the next JSON reply. The common
    /// request/response turn for an echo-style program.
    pub async fn request_json(
        &mut self,
        value: &Value,
        timeout: std::time::Duration,
    ) -> Result<Value> {
        self.send_json(value).await?;
        self.recv_json(timeout).await
    }

    /// Await the program's close frame: its code and reason. Data frames
    /// arriving first are an error (the test expected the socket to end).
    /// Bounded by `timeout` so a program that never closes fails loud.
    pub async fn recv_close(&mut self, timeout: std::time::Duration) -> Result<(u16, String)> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let next = tokio::time::timeout_at(deadline, self.stream.next())
                .await
                .context("timed out waiting for the ws close frame")?;
            match next {
                Some(Ok(Message::Close(frame))) => {
                    return Ok(frame
                        .map(|f| (u16::from(f.code), f.reason.to_string()))
                        .unwrap_or((1005, String::new())));
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {
                    continue
                }
                Some(Ok(other)) => bail!("expected the close frame, got a data message: {other:?}"),
                Some(Err(e)) => return Err(e).context("ws receive error"),
                None => bail!("ws stream ended without a close frame"),
            }
        }
    }

    /// Close the WebSocket cleanly. For a caller-tied program this ends the run.
    pub async fn close(mut self) -> Result<()> {
        self.stream.close(None).await.context("ws close")
    }
}

/// One live HTTP request of any method with explicit headers and a raw
/// body: the status, the response headers and the whole body, whatever
/// the status (a `404`, a `405` and a `401` are answers a test asserts,
/// not failures).
pub async fn http_request(
    disp: &Dispatcher,
    method: reqwest::Method,
    mount_path: &str,
    headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<(reqwest::StatusCode, reqwest::header::HeaderMap, Vec<u8>)> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let client = reqwest::Client::new();
    let mut req = client.request(method.clone(), &url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    if let Some(body) = body {
        req = req.body(body);
    }
    let resp = req.send().await.with_context(|| format!("live {method} {url}"))?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.bytes().await.unwrap_or_default().to_vec();
    Ok((status, headers, bytes))
}

/// What a browser sees when a page on `origin` POSTs JSON to a route: the
/// preflight `OPTIONS` a browser sends before a cross-origin POST with a
/// JSON body, then the POST. The preflight is the install's own answer (it
/// never reaches the worker); the POST's answer is the worker's, with the
/// install's CORS headers on it.
pub struct BrowserCall {
    pub preflight_status: reqwest::StatusCode,
    pub preflight_headers: reqwest::header::HeaderMap,
    pub status: reqwest::StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Vec<u8>,
}

pub async fn browser_post_json(disp: &Dispatcher, mount_path: &str, origin: &str, body: &Value) -> Result<BrowserCall> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let client = reqwest::Client::new();
    let preflight = client
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type")
        .send()
        .await
        .with_context(|| format!("preflight OPTIONS {url}"))?;
    let (preflight_status, preflight_headers) = (preflight.status(), preflight.headers().clone());
    let resp = client.post(&url).header("origin", origin).json(body).send().await
        .with_context(|| format!("live POST {url}"))?;
    let (status, headers) = (resp.status(), resp.headers().clone());
    let body = resp.bytes().await.unwrap_or_default().to_vec();
    Ok(BrowserCall { preflight_status, preflight_headers, status, headers, body })
}

/// Open a streaming GET, read until the first chunk of the body arrives,
/// then HANG UP without reading the rest: what a caller does when they
/// close the tab on a live feed.
///
/// Returns that first chunk. The response (and with it the connection) is
/// dropped before this returns, so by the time the caller asserts, the
/// worker's side of the socket is genuinely gone. Used to prove a run
/// behind a route ends with its caller even while the program is silent.
pub async fn stream_first_chunk_then_hang_up(
    disp: &Dispatcher,
    mount_path: &str,
) -> Result<Vec<u8>> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let client = reqwest::Client::new();
    let mut resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("live GET {url}"))?;
    let status = resp.status();
    anyhow::ensure!(status.is_success(), "live GET {url} -> HTTP {status}");
    let first = resp
        .chunk()
        .await
        .with_context(|| format!("read the first chunk of {url}"))?
        .context("the stream ended before its first chunk")?;
    drop(resp);
    Ok(first.to_vec())
}

/// Open a streaming GET on a raw socket, read the first chunk, then stop
/// reading while holding the socket open: a caller that is still THERE
/// and has gone silent.
///
/// It is NOT a vanished caller, and it cannot be one. This machine's
/// kernel keeps acknowledging whatever the worker sends however little
/// user space reads, so the far side stays demonstrably alive. Faking a
/// caller whose machine stops answering means dropping the packets
/// underneath us (a firewall rule, a severed link), which this rig
/// cannot do.
///
/// What it is good for is the slow-reader case: the exchange must
/// survive it rather than erroring, and the run must end once the
/// socket is finally dropped, which is what dropping the returned guard
/// does.
pub async fn stream_first_chunk_then_stop_reading(
    disp: &Dispatcher,
    mount_path: &str,
) -> Result<SilentCaller> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let parsed = url::Url::parse(&url).with_context(|| format!("route url {url}"))?;
    // The request below is written by hand, in plain HTTP.
    anyhow::ensure!(parsed.scheme() == "http", "{url}: a raw socket here speaks plain http only");
    let host = parsed.host_str().context("route url has no host")?.to_string();
    let port = parsed.port_or_known_default().context("route url has no port")?;
    let path = match parsed.query() {
        Some(q) => format!("{}?{q}", parsed.path()),
        None => parsed.path().to_string(),
    };
    let authority = match parsed.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.clone(),
    };

    let mut socket = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("connect to the install at {authority}"))?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: text/event-stream\r\n\r\n"
    );
    socket
        .write_all(request.as_bytes())
        .await
        .context("send the streaming request")?;

    // Read until the body's first chunk is in hand, so the exchange is
    // genuinely established before the caller goes silent.
    let mut seen = Vec::new();
    loop {
        let mut buf = [0u8; 4096];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            socket.read(&mut buf),
        )
        .await
        .context("waiting for the first chunk of the stream")?
        .context("read the stream")?;
        anyhow::ensure!(read > 0, "the install closed before sending a chunk");
        seen.extend_from_slice(&buf[..read]);
        if let Some(at) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
            // A refusal comes with a body too: only a success opened a stream.
            let head = String::from_utf8_lossy(&seen[..at]).into_owned();
            let status = head.split_whitespace().nth(1).and_then(|s| s.parse::<u16>().ok());
            anyhow::ensure!(
                status.is_some_and(|s| (200..300).contains(&s)),
                "live GET {url} did not open a stream:\n{head}"
            );
            if seen.len() > at + 4 {
                break;
            }
        }
    }
    Ok(SilentCaller { _socket: socket })
}

/// A caller holding its socket open and reading nothing. Dropping it
/// is the caller finally going.
pub struct SilentCaller {
    _socket: tokio::net::TcpStream,
}

/// A browser's first step on a socket route: ask with a plain GET and stop
/// there, holding the URL it answers with, which carries the caller's
/// TICKET. [`open_socket_at`] is the second step, whenever the test chooses.
///
/// This half is the one the gate sees, so a route with auth is checked
/// here.
// SYNC: the ticket answer's shape <-> crates/weft-engine/src/door/mod.rs (the ticket answer), packages/weft-connect/src/core/socket.ts (socketAddress)
pub async fn ticket(disp: &Dispatcher, mount_path: &str, headers: &[(&str, &str)]) -> Result<String> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let mut req = reqwest::Client::new().get(&url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = req.send().await.with_context(|| format!("live GET {url}"))?;
    let status = resp.status();
    let body = resp.text().await.context("read the handshake body")?;
    let v: Value = serde_json::from_str(&body)
        .with_context(|| format!("no ticket: the route answered HTTP {status}, not a JSON ticket: {body}"))?;
    v.get("url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("handshake response missing `url`: {body}"))
}

/// [`http_request`] with a JSON body and the matching content type.
pub async fn http_json(
    disp: &Dispatcher,
    method: reqwest::Method,
    mount_path: &str,
    headers: &[(&str, &str)],
    body: &Value,
) -> Result<(reqwest::StatusCode, reqwest::header::HeaderMap, Vec<u8>)> {
    let mut all: Vec<(&str, &str)> = vec![("content-type", "application/json")];
    all.extend_from_slice(headers);
    let bytes = serde_json::to_vec(body).context("serialize the body")?;
    http_request(disp, method, mount_path, &all, Some(bytes)).await
}

/// Drive an HTTP live request and return the full response body: the
/// worker's responder reads the body, streams progress chunks and sends a
/// final body, and the whole stream comes back (chunks and final
/// concatenated), so callers parse what they expect.
pub async fn http_post(disp: &Dispatcher, mount_path: &str, body: &Value) -> Result<Vec<u8>> {
    let url = format!(
        "{}/connect/{}",
        disp.base(),
        mount_path.trim_start_matches('/')
    );
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("live HTTP POST {url}"))?;
    let status = resp.status();
    let bytes = resp.bytes().await.unwrap_or_default().to_vec();
    if !status.is_success() {
        bail!(
            "live HTTP POST {url} -> HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(bytes)
}

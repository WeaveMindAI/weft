//! The live-caller path: an outside party holds an HTTP stream or a two-way
//! WebSocket against a running program.
//!
//! Flow (proven by hand during the live-caller feature):
//!   1. Handshake: `/connect/<tenant>/{path}` on the dispatcher, any method. For a WebSocket the
//!      response is `200 { "url": "...", "protocol": "websocket" }`; the URL is
//!      a per-pod gateway URL carrying a signed routing token. For HTTP it is a
//!      `307` whose `Location` is the same kind of URL.
//!   2. Connect: open the URL. The gateway routes to the pinned worker pod. WS
//!      clients must swap the `http(s)` scheme to `ws(s)`.
//!   3. Exchange: send / receive messages (the data type is whatever the
//!      trigger declared; JSON by default, so a text payload must be JSON).
//!
//! The handshake URL points at the gateway (port 9097), NOT the dispatcher, so
//! these helpers hit absolute URLs.

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

use crate::client::Dispatcher;

/// Perform the live-caller handshake for `mount_path` and return the per-pod
/// connection URL (as the dispatcher hands it out, `http(s)://...`). The caller
/// then connects via [`open_ws`] (WebSocket) or by streaming the URL (HTTP).
pub async fn handshake(disp: &Dispatcher, mount_path: &str) -> Result<String> {
    // `/connect/{path}` is an external-CALLER endpoint: it authenticates via
    // the route's own auth connection (or is open), NOT the dispatcher's tenant
    // token, so it goes through the UNAUTHED absolute-URL path, exactly like
    // the HTTP-live `http_post` + the webhook `fire_webhook`. (Routing it
    // through the authed `get_raw` would attach a tenant token the gate just
    // ignores, falsely implying `/connect` is a tenant-token endpoint.)
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let (status, bytes) = disp.get_abs_raw(&url).await?;
    let body = String::from_utf8_lossy(&bytes).into_owned();
    if !status.is_success() {
        bail!("live handshake GET {url} -> HTTP {status}: {body}");
    }
    let v: Value =
        serde_json::from_str(&body).with_context(|| format!("handshake body not JSON: {body}"))?;
    v.get("url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("handshake response missing `url`: {body}"))
}

/// A connected live WebSocket to a worker. Send and receive JSON messages until
/// the test closes it (which ends the run for a caller-tied program).
pub struct LiveWs {
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

/// Open a live WebSocket: handshake for `mount_path`, then connect to the
/// returned URL (swapping the scheme to `ws`/`wss`, exactly as a browser does).
pub async fn open_ws(disp: &Dispatcher, mount_path: &str) -> Result<LiveWs> {
    let http_url = handshake(disp, mount_path).await?;
    let ws_url = http_url
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let (stream, _resp) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .with_context(|| format!("WebSocket connect to {ws_url}"))?;
    Ok(LiveWs { stream })
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
/// body, following the dispatcher's `307` to the worker like a browser
/// does: the status, the response headers and the whole body, whatever
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
/// dispatcher's `307` (taken without following it), then, on the gateway
/// hop its `Location` names, the preflight `OPTIONS` a browser sends
/// after a cross-origin redirect, then the real POST. The preflight is
/// the gateway's own answer (it never reaches the worker); the POST's
/// answer is the worker's, with the gateway's CORS headers on it.
pub struct BrowserCall {
    pub preflight_status: reqwest::StatusCode,
    pub preflight_headers: reqwest::header::HeaderMap,
    pub status: reqwest::StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Vec<u8>,
}

pub async fn browser_post_json(disp: &Dispatcher, mount_path: &str, origin: &str, body: &Value) -> Result<BrowserCall> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build()?;
    let resp = client.post(&url).header("origin", origin).json(body).send().await
        .with_context(|| format!("live POST {url} (no redirect)"))?;
    let status = resp.status();
    anyhow::ensure!(status == reqwest::StatusCode::TEMPORARY_REDIRECT, "live POST {url} -> HTTP {status}, expected the 307 to the worker");
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .context("the 307 carries no Location")?
        .to_string();
    let preflight = client
        .request(reqwest::Method::OPTIONS, &location)
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type")
        .send()
        .await
        .with_context(|| format!("preflight OPTIONS {location}"))?;
    let (preflight_status, preflight_headers) = (preflight.status(), preflight.headers().clone());
    let resp = client.post(&location).header("origin", origin).json(body).send().await
        .with_context(|| format!("live POST {location}"))?;
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
    let url = handshake_or_redirect(disp, mount_path).await?;
    let parsed = url::Url::parse(&url).with_context(|| format!("worker url {url}"))?;
    let host = parsed.host_str().context("worker url has no host")?.to_string();
    let port = parsed.port_or_known_default().context("worker url has no port")?;
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
        .with_context(|| format!("connect to the worker at {authority}"))?;
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
        anyhow::ensure!(read > 0, "the worker closed before sending a chunk");
        seen.extend_from_slice(&buf[..read]);
        if let Some(at) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
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

/// Ask for a connection and stop there, holding the worker URL the
/// dispatcher hands back: the caller's TICKET.
///
/// Every other helper here does the handshake and the connection in one
/// go, the way a browser does, which is right for a test about what a
/// route answers and useless for a test about WHEN the caller arrives.
/// This half is the one the gate sees, so a route with auth is checked
/// here; [`follow`] is the other half.
///
/// Works for either kind of route: a WebSocket answers with a JSON
/// `url`, an HTTP one with a redirect, and both name the same thing.
pub async fn ticket(
    disp: &Dispatcher,
    method: reqwest::Method,
    mount_path: &str,
    headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<String> {
    let url = format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build a client that does not follow the redirect")?;
    let mut req = client.request(method.clone(), &url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    if let Some(body) = body {
        req = req.body(body);
    }
    let resp = req.send().await.with_context(|| format!("live {method} {url}"))?;
    if let Some(location) = resp.headers().get(reqwest::header::LOCATION) {
        return Ok(location.to_str().context("Location is not text")?.to_string());
    }
    let status = resp.status();
    let body = resp.text().await.context("read the handshake body")?;
    let v: Value = serde_json::from_str(&body).with_context(|| {
        format!("no ticket: the handshake answered HTTP {status}, neither a redirect nor JSON: {body}")
    })?;
    v.get("url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("handshake response missing `url`: {body}"))
}

/// Use a ticket: go to the worker URL [`ticket`] handed back, whenever
/// the test chooses to. The answer is the worker's, including its
/// refusals, so a test can assert what a caller sees when they arrive
/// too late or with the wrong request.
pub async fn follow(
    url: &str,
    method: reqwest::Method,
    headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build a client that does not follow the redirect")?;
    let mut req = client.request(method.clone(), url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    if let Some(body) = body {
        req = req.body(body);
    }
    let resp = req.send().await.with_context(|| format!("live {method} {url}"))?;
    let status = resp.status();
    let body = resp.bytes().await.unwrap_or_default().to_vec();
    Ok((status, body))
}

/// The worker URL for a bodiless GET, the shape the streaming helpers
/// need.
async fn handshake_or_redirect(disp: &Dispatcher, mount_path: &str) -> Result<String> {
    ticket(disp, reqwest::Method::GET, mount_path, &[], None).await
}

/// Make the handshake with one request and then send a DIFFERENT one to
/// the worker the handshake pointed at.
///
/// This is the attack the caller token's fingerprint exists to stop.
/// The gate runs at the dispatcher and the program runs on the worker,
/// with a redirect in between, so a caller can get one request checked
/// and then send another down the redirect they were handed. Every
/// other helper here follows the redirect the way a client does, which
/// is exactly what this one must not do.
///
/// `gate` is what the dispatcher sees and approves; `sent` is what
/// actually arrives at the worker. Passing the same bytes for both is
/// the honest control case. Returns the worker's answer.
pub async fn redirect_then_send(
    disp: &Dispatcher,
    method: reqwest::Method,
    mount_path: &str,
    headers: &[(&str, &str)],
    gate: Vec<u8>,
    sent: Vec<u8>,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    let url = ticket(disp, method.clone(), mount_path, headers, Some(gate)).await?;
    follow(&url, method, headers, Some(sent)).await
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

/// Drive an HTTP live request and return the full response body.
///
/// Unlike the WebSocket path, the HTTP live connection is NOT a two-step
/// handshake: the `/connect/{path}` request itself carries the caller's body
/// and yields the response. The dispatcher answers with a `307` whose Location
/// is the per-pod gateway URL; reqwest follows it, re-sending the POST body
/// (307 preserves method + body), and the worker's responder reads the body,
/// streams progress chunks, and sends a final body. We return the whole stream
/// (chunks + final concatenated), so callers parse what they expect.
pub async fn http_post(disp: &Dispatcher, mount_path: &str, body: &Value) -> Result<Vec<u8>> {
    let url = format!(
        "{}/connect/{}",
        disp.base(),
        mount_path.trim_start_matches('/')
    );
    // reqwest follows redirects (incl. 307, preserving method + body) by
    // default, so a single POST to /connect lands on the worker.
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

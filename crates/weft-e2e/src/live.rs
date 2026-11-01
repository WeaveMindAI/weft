//! The live-caller path: an outside party holds an HTTP stream or a two-way
//! WebSocket against a running program.
//!
//! A call is `/connect/<tenant>/<project>/{path}` on the install's shared address,
//! any method, and is answered in that one request: the install passes it
//! on to the project's workers, whose door checks the caller and starts the
//! run. A socket is opened there the same way, by a client that sends the
//! upgrade itself. These helpers hit absolute URLs.

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::client::Dispatcher;

/// The absolute URL a caller reaches `mount_path` at.
fn connect_url(disp: &Dispatcher, mount_path: &str) -> String {
    format!("{}/connect/{}", disp.base(), mount_path.trim_start_matches('/'))
}

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
    let url = connect_url(disp, mount_path);
    let ws_url = url.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
    match tokio_tungstenite::connect_async(&ws_url).await {
        Ok((stream, _)) => Ok(LiveWs { stream }),
        Err(tokio_tungstenite::tungstenite::Error::Http(refused)) => bail!(
            "WebSocket connect to {url} answered {}: {}",
            refused.status(),
            refused.body().as_deref().map(String::from_utf8_lossy).unwrap_or_default()
        ),
        Err(e) => Err(e).with_context(|| format!("WebSocket connect to {ws_url}")),
    }
}

impl LiveWs {
    /// Send a JSON value as a text frame. The default trigger data type is JSON,
    /// so a string payload must be sent as JSON (`json!("hi")`), not bare text.
    pub async fn send_json(&mut self, value: &Value) -> Result<()> {
        let text = serde_json::to_string(value).context("serialize ws message")?;
        self.stream
            .send(Message::Text(text.into()))
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
    let url = connect_url(disp, mount_path);
    let client = weft_core::net::http_client().build()?;
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
    let bytes = resp.bytes().await.with_context(|| format!("live {method} {url} -> HTTP {status}: read the body"))?.to_vec();
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

/// POST `body` as JSON to `mount_path` the way a browser page on `origin`
/// does: the CORS preflight first, then the POST, both answers kept.
///
/// `headers` go on the POST, and their names are the ones the preflight
/// asks leave for, beside `content-type`.
pub async fn browser_post_json(disp: &Dispatcher, mount_path: &str, origin: &str, headers: &[(&str, &str)], body: &Value) -> Result<BrowserCall> {
    let url = connect_url(disp, mount_path);
    let client = weft_core::net::http_client().build()?;
    let asked: Vec<&str> = std::iter::once("content-type").chain(headers.iter().map(|(name, _)| *name)).collect();
    let preflight = client
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", asked.join(", "))
        .send()
        .await
        .with_context(|| format!("preflight OPTIONS {url}"))?;
    let (preflight_status, preflight_headers) = (preflight.status(), preflight.headers().clone());
    let mut post = client.post(&url).header("origin", origin);
    for (name, value) in headers {
        post = post.header(*name, *value);
    }
    let resp = post.json(body).send().await
        .with_context(|| format!("live POST {url}"))?;
    let (status, headers) = (resp.status(), resp.headers().clone());
    let body = resp.bytes().await.with_context(|| format!("live POST {url} -> HTTP {status}: read the body"))?.to_vec();
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
    let url = connect_url(disp, mount_path);
    let client = weft_core::net::http_client().build()?;
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

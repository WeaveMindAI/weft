//! The live-caller path: an outside party holds an HTTP stream or a two-way
//! WebSocket against a running program.
//!
//! Flow (proven by hand during the live-caller feature):
//!   1. Handshake: `GET /connect/{path}` on the dispatcher. For a WebSocket the
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
use tokio_tungstenite::tungstenite::Message;

use crate::client::Dispatcher;

/// Perform the live-caller handshake for `mount_path` and return the per-pod
/// connection URL (as the dispatcher hands it out, `http(s)://...`). The caller
/// then connects via [`open_ws`] (WebSocket) or by streaming the URL (HTTP).
pub async fn handshake(disp: &Dispatcher, mount_path: &str) -> Result<String> {
    // `/connect/{path}` is an external-CALLER endpoint: it authenticates via
    // the per-endpoint api-key gate (or "none"), NOT the dispatcher's tenant
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

    /// Close the WebSocket cleanly. For a caller-tied program this ends the run.
    pub async fn close(mut self) -> Result<()> {
        self.stream.close(None).await.context("ws close")
    }
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

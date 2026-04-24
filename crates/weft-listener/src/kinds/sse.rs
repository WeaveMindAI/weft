//! Server-Sent Events subscriber. Opens a long-lived GET to the
//! configured URL with `Accept: text/event-stream`, parses
//! `data: ...` lines, relays every matching event back through
//! `FireRelayer`.
//!
//! Reconnect loop with exponential backoff on failures (up to 60s).
//! Port of v1's WhatsAppReceive keep_alive path, generalized so
//! any SSE-sourced trigger uses this code path, not just WhatsApp.

use std::sync::Arc;

use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::relay::FireRelayer;

pub fn spawn(
    token: String,
    url: String,
    event_filter: String,
    relay: Arc<FireRelayer>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut backoff = 1u64;
        loop {
            let resp = match client
                .get(&url)
                .header("Accept", "text/event-stream")
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    info!(target: "weft_listener::sse", %url, %token, "SSE connected");
                    backoff = 1;
                    r
                }
                Ok(r) => {
                    warn!(target: "weft_listener::sse", %url, status = %r.status(), "non-success; retrying");
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                    continue;
                }
                Err(e) => {
                    warn!(target: "weft_listener::sse", %url, error = %e, "connect failed; retrying");
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                    continue;
                }
            };

            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();
            loop {
                let chunk = match stream.next().await {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(e)) => {
                        warn!(target: "weft_listener::sse", %url, error = %e, "stream error");
                        break;
                    }
                    None => {
                        info!(target: "weft_listener::sse", %url, "stream ended; reconnecting");
                        break;
                    }
                };
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buffer.find("\n\n") {
                    let msg = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();
                    for line in msg.lines() {
                        let Some(json_str) = line.strip_prefix("data: ") else {
                            continue;
                        };
                        let evt: serde_json::Value = match serde_json::from_str(json_str) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if !event_filter.is_empty() {
                            let got = evt.get("event").and_then(|v| v.as_str()).unwrap_or("");
                            if got != event_filter {
                                continue;
                            }
                        }
                        let payload = evt.get("data").cloned().unwrap_or(evt);
                        relay.fire(token.clone(), payload).await;
                    }
                }
            }

            sleep(Duration::from_secs(1)).await;
        }
    })
}

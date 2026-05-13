//! Server-Sent Events handler. Opens a long-lived GET to the
//! configured URL with `Accept: text/event-stream`, parses SSE
//! message blocks per https://html.spec.whatwg.org/multipage/server-sent-events.html,
//! enqueues a `FireSignal` task via the broker for every matching
//! event. Reconnects with exponential backoff up to 60s on failures.

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use futures_util::StreamExt;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, Sse};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::KindHandler;

pub struct SseHandler;

impl KindHandler for SseHandler {
    fn tag(&self) -> &'static str {
        Sse::TAG
    }

    fn compute_routing(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    fn spawn_task(
        &self,
        token: &str,
        spec: &SignalSpec,
        _kind_state: &Value,
        sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        let sse: Sse = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed sse spec: {e}"))?;
        Ok(Some(spawn_loop(token.to_string(), sse.url, sse.event_name, sink)))
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        ProcessOutcome {
            value: payload,
            target: ProcessTarget::Drop {
                reason: Some("sse is internal-fire only".into()),
            },
        }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// One parsed SSE message. `event` defaults to "message" per spec
/// when no `event:` line is present. `data` is the concatenation of
/// every `data:` line in the block (with `\n` separators per spec),
/// trimmed of the trailing newline.
struct SseMessage {
    event: String,
    data: String,
}

/// Parse one SSE message block (the text between two `\n\n`
/// boundaries) into a typed message. Returns None if the block
/// carried no `data:` line (per spec, those blocks are dispatched
/// as events with no data, but for our purposes there's nothing to
/// fire on, so we skip).
/// Find the byte offset of the first `\n\n` in `buf`, or `None`.
/// Pure byte search: works regardless of UTF-8 framing because the
/// SSE delimiter is two ASCII newlines.
fn find_block_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

fn parse_message(block: &str) -> Option<SseMessage> {
    let mut event = String::from("message");
    let mut data = String::new();
    let mut saw_data = false;
    for line in block.lines() {
        // Comments per spec: line starts with ':'. Skip.
        if line.starts_with(':') {
            continue;
        }
        // Field is everything before the first colon; value is
        // everything after (with at most one leading space stripped).
        let (field, value) = match line.find(':') {
            Some(idx) => {
                let (f, rest) = line.split_at(idx);
                let v = &rest[1..]; // strip the ':'
                (f, v.strip_prefix(' ').unwrap_or(v))
            }
            None => (line, ""), // field with empty value, per spec
        };
        match field {
            "event" => {
                if !value.is_empty() {
                    event = value.to_string();
                }
            }
            "data" => {
                if saw_data {
                    data.push('\n');
                }
                data.push_str(value);
                saw_data = true;
            }
            // id/retry/unknown: ignore.
            _ => {}
        }
    }
    if saw_data {
        Some(SseMessage { event, data })
    } else {
        None
    }
}

fn spawn_loop(
    token: String,
    url: String,
    event_filter: String,
    sink: FireSignalSink,
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
            // Byte-level buffer so a multi-byte UTF-8 sequence
            // straddling a chunk boundary is reassembled before
            // decode. A per-chunk `from_utf8_lossy` would replace
            // the trailing partial sequence with U+FFFD even though
            // the next chunk completes it.
            let mut buffer: Vec<u8> = Vec::new();
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
                buffer.extend_from_slice(&chunk);
                while let Some(pos) = find_block_boundary(&buffer) {
                    let block_bytes: Vec<u8> = buffer.drain(..pos + 2).collect();
                    // `block_bytes` ends with `\n\n`; strip before decode.
                    let block = match std::str::from_utf8(&block_bytes[..pos]) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(
                                target: "weft_listener::sse",
                                %url, error = %e,
                                "invalid UTF-8 in SSE block; skipping"
                            );
                            continue;
                        }
                    };
                    let Some(msg) = parse_message(block) else {
                        continue;
                    };
                    // event_filter empty => "match every event" (today's
                    // sentinel). Otherwise filter on the actual SSE
                    // event name from the `event:` line.
                    if !event_filter.is_empty() && msg.event != event_filter {
                        continue;
                    }
                    // Try to parse data as JSON; if it isn't JSON,
                    // forward it as a JSON string. The fire pipeline
                    // is JSON-typed end-to-end.
                    let payload = serde_json::from_str::<Value>(&msg.data)
                        .unwrap_or_else(|_| Value::String(msg.data.clone()));
                    if let Err(e) = sink.fire(&token, payload).await {
                        warn!(
                            target: "weft_listener::sse",
                            %token, error = %e,
                            "fire enqueue failed"
                        );
                    }
                }
            }

            sleep(Duration::from_secs(1)).await;
        }
    })
}

inventory::submit!(&SseHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_message_basic() {
        let m = parse_message("event: tick\ndata: {\"n\":1}").expect("msg");
        assert_eq!(m.event, "tick");
        assert_eq!(m.data, "{\"n\":1}");
    }

    #[test]
    fn parse_message_default_event_name() {
        let m = parse_message("data: hello").expect("msg");
        assert_eq!(m.event, "message");
        assert_eq!(m.data, "hello");
    }

    #[test]
    fn parse_message_multiline_data() {
        let m = parse_message("data: line1\ndata: line2").expect("msg");
        assert_eq!(m.data, "line1\nline2");
    }

    #[test]
    fn parse_message_skips_comments_and_unknown() {
        let m = parse_message(": heartbeat\nid: 7\nretry: 500\nevent: foo\ndata: bar")
            .expect("msg");
        assert_eq!(m.event, "foo");
        assert_eq!(m.data, "bar");
    }

    #[test]
    fn parse_message_no_data_yields_none() {
        // Per spec a block with no `data:` is dispatched as an empty
        // event; we have nothing to fire so we skip.
        assert!(parse_message("event: ping").is_none());
    }
}

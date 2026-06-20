//! Persistent bidirectional outbound WebSocket handler. Dials the gateway
//! URL, sends the optional handshake frame on open, resends the optional
//! heartbeat frame every `heartbeat_secs`, and fires a fresh execution per
//! inbound frame. Reconnects (with the shared backoff ladder) when the
//! socket drops. The SERVICE protocol (Discord op-codes, Slack envelopes)
//! is the node's concern, carried as the literal frames in the spec.

use std::sync::Arc;

use anyhow::Result;
use base64::Engine as _;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, SocketFrame, SocketListen};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::event_source::Backoff;
use super::KindHandler;

pub struct SocketListenHandler;

impl KindHandler for SocketListenHandler {
    fn tag(&self) -> &'static str {
        SocketListen::TAG
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
        tenant_id: &str,
        placement_generation: i64,
        spec: &SignalSpec,
        _kind_state: &Value,
        sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        let cfg: SocketListen = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed socket_listen spec: {e}"))?;
        Ok(Some(spawn_loop(token.to_string(), tenant_id.to_string(), placement_generation, cfg, sink)))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// Convert a spec frame into a tungstenite message. Binary frames carry
/// base64 on the wire (the spec is JSON); decode here.
fn to_message(frame: &SocketFrame) -> Result<Message> {
    Ok(match frame {
        SocketFrame::Text { body } => Message::Text(body.clone()),
        SocketFrame::Binary { base64 } => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(base64)
                .map_err(|e| anyhow::anyhow!("socket_listen binary frame is not valid base64: {e}"))?;
            Message::Binary(bytes)
        }
    })
}

/// Convert an inbound message into the JSON fire payload. Text that parses
/// as JSON fires as JSON; otherwise as a JSON string. Binary fires as a
/// base64 JSON string (the fire pipeline is JSON-typed end to end).
fn inbound_payload(msg: Message) -> Option<Value> {
    match msg {
        // Text shares the JSON-or-string coercion with the other event
        // sources; only the binary->base64 case is socket-specific.
        Message::Text(t) => Some(super::event_source::coerce_text_payload(t)),
        Message::Binary(b) => Some(Value::String(
            base64::engine::general_purpose::STANDARD.encode(b),
        )),
        // Ping/pong/close/frame are transport-level, not events to fire on.
        _ => None,
    }
}

fn spawn_loop(token: String, tenant_id: String, placement_generation: i64, cfg: SocketListen, sink: FireSignalSink) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Backoff::new();
        loop {
            let stream = match connect_async(&cfg.url).await {
                Ok((s, _resp)) => {
                    info!(target: "weft_listener::socket_listen", url = %cfg.url, %token, "socket connected");
                    s
                }
                Err(e) => {
                    warn!(target: "weft_listener::socket_listen", url = %cfg.url, error = %e, "connect failed; retrying");
                    backoff.wait_then_climb().await;
                    continue;
                }
            };
            let connected_at = std::time::Instant::now();
            let (mut write, mut read) = stream.split();

            // Handshake on open.
            if let Some(frame) = &cfg.handshake {
                match to_message(frame) {
                    Ok(m) => {
                        if let Err(e) = write.send(m).await {
                            warn!(target: "weft_listener::socket_listen", error = %e, "handshake send failed; reconnecting");
                            backoff.wait_then_climb().await;
                            continue;
                        }
                    }
                    Err(e) => {
                        // A malformed handshake frame is a config bug, not a
                        // transient failure: log loud and stop retrying this
                        // socket (retrying would spin on the same bad config).
                        warn!(target: "weft_listener::socket_listen", error = %e, "handshake frame invalid; giving up on this socket");
                        return;
                    }
                }
            }

            // Heartbeat ticker (disabled when no heartbeat frame).
            let heartbeat_msg = match &cfg.heartbeat {
                Some(f) => match to_message(f) {
                    Ok(m) => Some(m),
                    Err(e) => {
                        warn!(target: "weft_listener::socket_listen", error = %e, "heartbeat frame invalid; giving up on this socket");
                        return;
                    }
                },
                None => None,
            };
            let mut ticker = interval(Duration::from_secs(cfg.heartbeat_secs));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Consume the immediate first tick so the heartbeat does not
            // fire the instant the loop starts (right after the handshake).
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick(), if heartbeat_msg.is_some() => {
                        if let Some(m) = &heartbeat_msg {
                            if let Err(e) = write.send(m.clone()).await {
                                warn!(target: "weft_listener::socket_listen", error = %e, "heartbeat send failed; reconnecting");
                                break;
                            }
                        }
                    }
                    msg = read.next() => {
                        match msg {
                            Some(Ok(m)) => {
                                if let Some(payload) = inbound_payload(m) {
                                    super::event_source::fire_payload(&sink, &token, &tenant_id, placement_generation, payload, "socket_listen").await;
                                }
                            }
                            Some(Err(e)) => {
                                warn!(target: "weft_listener::socket_listen", error = %e, "socket error; reconnecting");
                                break;
                            }
                            None => {
                                info!(target: "weft_listener::socket_listen", "socket closed; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }

            backoff.reset_if_healthy(connected_at.elapsed());
            backoff.wait_then_climb().await;
        }
    })
}

inventory::submit!(&SocketListenHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_frame_round_trips_to_message() {
        let m = to_message(&SocketFrame::Text { body: "hi".into() }).unwrap();
        assert!(matches!(m, Message::Text(t) if t == "hi"));
    }

    #[test]
    fn binary_frame_decodes_base64() {
        let b64 = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
        let m = to_message(&SocketFrame::Binary { base64: b64 }).unwrap();
        assert!(matches!(m, Message::Binary(b) if b == vec![1, 2, 3]));
    }

    #[test]
    fn inbound_text_json_parses() {
        let p = inbound_payload(Message::Text("{\"a\":1}".into())).unwrap();
        assert_eq!(p, serde_json::json!({"a": 1}));
    }

    #[test]
    fn inbound_text_non_json_is_string() {
        let p = inbound_payload(Message::Text("hello".into())).unwrap();
        assert_eq!(p, Value::String("hello".into()));
    }

    #[test]
    fn inbound_transport_frames_skipped() {
        assert!(inbound_payload(Message::Ping(vec![])).is_none());
    }
}

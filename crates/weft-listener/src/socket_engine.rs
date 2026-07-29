//! The shared outbound-socket engine: dial, handshake, heartbeat,
//! declarative replies, fire per inbound frame, reconnect on the
//! shared backoff ladder. ONE engine, driven by two callers with
//! different configs: the `socket_listen` kind (per-signal socket,
//! static or minted address) and the `provider_events` kind (one
//! socket per connection, address minted from the service recipe,
//! events fanned to every subscription).
//!
//! The caller supplies two closures: `prepare` runs at the start of
//! EVERY connect cycle and answers the cycle's plan (minted addresses
//! are single-use, so the mint must re-run per reconnect), and
//! `on_event` receives each inbound payload after the reply rules ran.

use std::collections::BTreeMap;
use std::time::Instant;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use weft_core::access::client::run_connect_call;
use weft_core::access::spec::{lookup_path, MintedSocket, ReplyRule};

use crate::kinds::event_source::Backoff;

/// One connect cycle's plan, answered by the caller's `prepare`.
pub struct CyclePlan {
    /// The ws(s) address to dial this cycle.
    pub url: String,
    /// Sent once right after the socket opens.
    pub handshake: Option<Message>,
    /// Resent every `heartbeat_secs`; `None` = protocol ping/pong only.
    pub heartbeat: Option<Message>,
    pub heartbeat_secs: u64,
    /// Declarative acks, applied to every inbound frame before
    /// `on_event` sees it.
    pub replies: Vec<ReplyRule>,
}

/// How `prepare` failed, which decides whether the engine retries.
pub enum PrepareError {
    /// Transient (the mint endpoint refused, the broker was
    /// unreachable): wait on the ladder and try again.
    Transient(anyhow::Error),
    /// The configuration itself is broken (a malformed frame, a
    /// template naming a value that does not exist): retrying spins
    /// on the same bad config, so the engine logs loud and exits.
    Fatal(anyhow::Error),
}

/// Mint one cycle's socket address from a [`MintedSocket`]: run the
/// connect call with the resolved values and read the capture named
/// by `url_from`. Shared by every kind that dials a minted address;
/// these addresses are single-use, so callers run this per cycle.
pub async fn mint_socket_url(
    minted: &MintedSocket,
    values: &BTreeMap<String, String>,
) -> Result<String, PrepareError> {
    let Some(call) = &minted.connect else {
        // Validation refuses a recipe without the call; one landing
        // here anyway is misconfigured, and retrying would spin.
        return Err(PrepareError::Fatal(anyhow::anyhow!(
            "the socket recipe declares no connect call to mint the address"
        )));
    };
    let resp = run_connect_call(call, values)
        .await
        .map_err(|e| PrepareError::Transient(anyhow::anyhow!(e)))?;
    let capture = call
        .captures
        .iter()
        .find(|c| c.name == minted.url_from)
        .ok_or_else(|| {
            PrepareError::Fatal(anyhow::anyhow!(
                "the connect call captures nothing named '{}'",
                minted.url_from
            ))
        })?;
    lookup_path(&resp, &capture.path)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            PrepareError::Transient(anyhow::anyhow!(
                "the connect call answered nothing at '{}'",
                capture.path
            ))
        })
}

type Prepare = Box<dyn FnMut() -> BoxFuture<'static, Result<CyclePlan, PrepareError>> + Send>;
type OnEvent = Box<dyn FnMut(Value) -> BoxFuture<'static, ()> + Send>;

/// Run the engine until aborted (or a fatal prepare error). `target`
/// names the driving kind in every log line.
pub fn spawn(mut prepare: Prepare, mut on_event: OnEvent, target: &'static str) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Backoff::new();
        loop {
            let plan = match prepare().await {
                Ok(p) => p,
                Err(PrepareError::Transient(e)) => {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %format!("{e:#}"), "connect preparation failed; retrying");
                    backoff.wait_then_climb().await;
                    continue;
                }
                Err(PrepareError::Fatal(e)) => {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %format!("{e:#}"), "connect preparation is misconfigured; giving up on this socket");
                    return;
                }
            };

            let stream = match connect_async(&plan.url).await {
                Ok((s, _resp)) => {
                    info!(target: "weft_listener::socket_engine", kind = target, "socket connected");
                    s
                }
                Err(e) => {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "connect failed; retrying");
                    backoff.wait_then_climb().await;
                    continue;
                }
            };
            let connected_at = Instant::now();
            let (mut write, mut read) = stream.split();

            if let Some(frame) = &plan.handshake {
                if let Err(e) = write.send(frame.clone()).await {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "handshake send failed; reconnecting");
                    backoff.wait_then_climb().await;
                    continue;
                }
            }

            let mut ticker = interval(Duration::from_secs(plan.heartbeat_secs.max(1)));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Consume the immediate first tick so the heartbeat does
            // not fire the instant the loop starts.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick(), if plan.heartbeat.is_some() => {
                        if let Some(m) = &plan.heartbeat {
                            if let Err(e) = write.send(m.clone()).await {
                                warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "heartbeat send failed; reconnecting");
                                break;
                            }
                        }
                    }
                    msg = read.next() => {
                        match msg {
                            Some(Ok(m)) => {
                                let Some(payload) = inbound_payload(m) else { continue };
                                // Acks first: a gateway that demands
                                // them drops the line on a slow one,
                                // and the fire path may be slower.
                                for rule in &plan.replies {
                                    if let Some(frame) = rule.reply_to(&payload) {
                                        if let Err(e) = write.send(Message::Text(frame)).await {
                                            warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "reply send failed");
                                        }
                                    }
                                }
                                on_event(payload).await;
                            }
                            Some(Err(e)) => {
                                warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "socket error; reconnecting");
                                break;
                            }
                            None => {
                                info!(target: "weft_listener::socket_engine", kind = target, "socket closed; reconnecting");
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

/// Convert an inbound message into the JSON fire payload. Text that
/// parses as JSON fires as JSON; otherwise as a JSON string. Binary
/// fires as a base64 JSON string (the fire pipeline is JSON-typed end
/// to end). Ping/pong/close are transport-level, not events.
pub fn inbound_payload(msg: Message) -> Option<Value> {
    use base64::Engine as _;
    match msg {
        Message::Text(t) => Some(crate::kinds::event_source::coerce_text_payload(t)),
        Message::Binary(b) => Some(Value::String(
            base64::engine::general_purpose::STANDARD.encode(b),
        )),
        _ => None,
    }
}

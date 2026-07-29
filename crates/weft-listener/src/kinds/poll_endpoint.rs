//! Periodic HTTP poll handler. Hits the configured URL every
//! `interval_secs` and fires a fresh execution carrying the response body
//! (JSON if it parses, else a JSON string). Shares the entry routing,
//! fire path, and reconnect-backoff ladder with the other event-source
//! kinds; the only thing specific here is the timer-driven GET.

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::warn;
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{PollEndpoint, Signal};

use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use async_trait::async_trait;

use super::{KindHandler, SpawnCtx};

pub struct PollEndpointHandler;

#[async_trait]
impl KindHandler for PollEndpointHandler {
    fn tag(&self) -> &'static str {
        PollEndpoint::TAG
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

    async fn spawn_task(
        &self,
        spec: &SignalSpec,
        _kind_state: &Value,
        ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>> {
        let poll: PollEndpoint = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed poll_endpoint spec: {e}"))?;
        Ok(Some(spawn_loop(poll.url, poll.interval_secs, spec.access.clone(), ctx)))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        // A poll result (raised internally by spawn_loop via `sink.fire`)
        // routes to the entry trigger.
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

fn spawn_loop(
    url: String,
    interval_secs: u64,
    access: Option<weft_core::primitive::AccessRef>,
    ctx: SpawnCtx,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_secs));
        // A slow poll (response took longer than the interval) must not
        // cause a burst of catch-up polls; skip missed ticks instead.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            // Signed-in polls resolve the connection PER CYCLE: the
            // credential is refreshed store-side and never frozen
            // into this loop. No connection = a plain client.
            let client = match crate::listener_access::client_for(&access, &ctx).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(target: "weft_listener::poll_endpoint", %url, error = %format!("{e:#}"), "connection resolve failed; will retry next tick");
                    continue;
                }
            };
            let resp = match client.get(&url).send().await {
                Ok(r) if r.status().is_success() => r,
                Ok(r) => {
                    warn!(target: "weft_listener::poll_endpoint", %url, status = %r.status(), "non-success poll; will retry next tick");
                    continue;
                }
                Err(e) => {
                    warn!(target: "weft_listener::poll_endpoint", %url, error = %e, "poll request failed; will retry next tick");
                    continue;
                }
            };
            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(target: "weft_listener::poll_endpoint", %url, error = %e, "poll body read failed");
                    continue;
                }
            };
            let payload = super::event_source::coerce_text_payload(body);
            ctx.fire.fire(payload, "poll_endpoint").await;
        }
    })
}

inventory::submit!(&PollEndpointHandler as &dyn KindHandler);

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

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::KindHandler;

pub struct PollEndpointHandler;

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

    fn spawn_task(
        &self,
        token: &str,
        spec: &SignalSpec,
        _kind_state: &Value,
        sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        let poll: PollEndpoint = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed poll_endpoint spec: {e}"))?;
        Ok(Some(spawn_loop(token.to_string(), poll.url, poll.interval_secs, sink)))
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
    token: String,
    url: String,
    interval_secs: u64,
    sink: FireSignalSink,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut ticker = interval(Duration::from_secs(interval_secs));
        // A slow poll (response took longer than the interval) must not
        // cause a burst of catch-up polls; skip missed ticks instead.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
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
            super::event_source::fire_payload(&sink, &token, payload, "poll_endpoint").await;
        }
    })
}

inventory::submit!(&PollEndpointHandler as &dyn KindHandler);

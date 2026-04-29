//! Relays fires from the listener back to the dispatcher.
//!
//! Every kind handler (timer tick, SSE event, webhook POST, form
//! submit, socket message) ends with a call to
//! `FireRelayer::fire(token, payload)`. The relayer:
//!
//! 1. POSTs `/signal-fired` on the dispatcher.
//! 2. Reads the ack: `Consume` → unregister + maybe notify-empty.
//!    `Retry` → exp backoff, repost.
//! 3. After 5 failed attempts, posts `/signal-failed` so the
//!    dispatcher can fail-dispatch the affected node, then
//!    unregisters.
//!
//! When the registry empties as a side effect of consume / fail,
//! the relayer POSTs `/listener/empty` so the dispatcher can
//! decide to kill our pod.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::{error, info};

use crate::config::ListenerConfig;
use crate::protocol::{
    EmptyNotice, FireRelay, RegisterMeNotice, SignalFailedNotice, SignalFiredAck,
};
use crate::registry::Registry;

/// Wall-clock budget for retrying a fire. After this many seconds,
/// the listener gives up and posts `/signal-failed`. Override via
/// `WEFT_LISTENER_FIRE_BUDGET_SECS` env. Default 5 minutes covers a
/// rolling deploy of the dispatcher.
const FIRE_BUDGET_SECS_DEFAULT: u64 = 300;
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

fn fire_budget() -> Duration {
    let secs = std::env::var("WEFT_LISTENER_FIRE_BUDGET_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(FIRE_BUDGET_SECS_DEFAULT);
    Duration::from_secs(secs)
}

pub struct FireRelayer {
    config: Arc<ListenerConfig>,
    http: reqwest::Client,
    /// Registry handle so we can drop the registration on consume
    /// and check for emptiness afterwards.
    registry: Arc<Registry>,
}

impl FireRelayer {
    pub fn new(config: Arc<ListenerConfig>, registry: Arc<Registry>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client");
        Self { config, http, registry }
    }

    /// Fire a signal back to the dispatcher with ack-or-retry
    /// semantics. Retries with exponential backoff until the wall-
    /// clock budget expires. After the budget, posts
    /// `/signal-failed` so the dispatcher records a
    /// SuspensionFailed event.
    pub async fn fire(&self, token: String, payload: Value) {
        let body = FireRelay {
            tenant_id: self.config.tenant_id.clone(),
            token: token.clone(),
            payload,
        };
        let url = format!(
            "{}/signal-fired",
            self.config.dispatcher_url.trim_end_matches('/')
        );

        let budget = fire_budget();
        let deadline = std::time::Instant::now() + budget;
        let mut delay = INITIAL_BACKOFF;
        let mut attempt: u32 = 0;
        while std::time::Instant::now() < deadline {
            attempt += 1;
            match self.post_fire(&url, &body).await {
                Ok(SignalFiredAck::Consume { .. }) => {
                    self.unregister_after_fire(&token).await;
                    return;
                }
                Ok(SignalFiredAck::Retry { retry_after_ms, reason }) => {
                    tracing::warn!(
                        target: "weft_listener::relay",
                        token = %token,
                        attempt,
                        reason = %reason,
                        retry_after_ms,
                        "dispatcher asked for retry"
                    );
                    let sleep_for = Duration::from_millis(retry_after_ms);
                    tokio::time::sleep(sleep_for.min(deadline.saturating_duration_since(std::time::Instant::now()))).await;
                }
                Err(e) => {
                    error!(
                        target: "weft_listener::relay",
                        token = %token,
                        attempt,
                        error = %e,
                        "signal-fired post failed"
                    );
                    let sleep_for =
                        delay.min(deadline.saturating_duration_since(std::time::Instant::now()));
                    tokio::time::sleep(sleep_for).await;
                    delay = (delay * 2).min(MAX_BACKOFF);
                }
            }
        }

        let reason = format!(
            "dispatcher failed to consume after {attempt} attempts ({}s budget)",
            budget.as_secs()
        );
        self.post_signal_failed(&token, &reason).await;
        self.unregister_after_fire(&token).await;
    }

    async fn post_fire(
        &self,
        url: &str,
        body: &FireRelay,
    ) -> Result<SignalFiredAck, anyhow::Error> {
        let resp = self
            .http
            .post(url)
            .bearer_auth(&self.config.relay_token)
            .json(body)
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("dispatcher returned {}", resp.status());
        }
        let ack: SignalFiredAck = resp.json().await?;
        Ok(ack)
    }

    async fn post_signal_failed(&self, token: &str, reason: &str) {
        let url = format!(
            "{}/signal-failed",
            self.config.dispatcher_url.trim_end_matches('/')
        );
        let body = SignalFailedNotice {
            tenant_id: self.config.tenant_id.clone(),
            token: token.to_string(),
            reason: reason.to_string(),
        };
        let req = self
            .http
            .post(&url)
            .bearer_auth(&self.config.relay_token)
            .json(&body);
        if let Err(e) = req.send().await {
            error!(
                target: "weft_listener::relay",
                token = %token,
                error = %e,
                "signal-failed post errored"
            );
        }
    }

    /// Drop the registration, then notify the dispatcher if the
    /// registry just hit zero.
    async fn unregister_after_fire(&self, token: &str) {
        if self.registry.remove(token).is_some() {
            self.maybe_notify_empty().await;
        }
    }

    /// Tell the dispatcher we just booted (or restarted) and ask
    /// it to re-push every signal on file for this tenant. The
    /// dispatcher iterates its `signal` table and POSTs `/register`
    /// for each row. Idempotent on the dispatcher side.
    pub async fn request_rehydrate(&self) -> anyhow::Result<()> {
        let url = format!(
            "{}/listener/register-me",
            self.config.dispatcher_url.trim_end_matches('/')
        );
        let body = RegisterMeNotice {
            tenant_id: self.config.tenant_id.clone(),
        };
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.config.relay_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("register-me returned {}", resp.status());
        }
        Ok(())
    }

    /// Check the registry; if empty, tell the dispatcher we have
    /// nothing left to listen to.
    pub async fn maybe_notify_empty(&self) {
        if !self.registry.is_empty() {
            return;
        }
        let url = format!(
            "{}/listener/empty",
            self.config.dispatcher_url.trim_end_matches('/')
        );
        let body = EmptyNotice {
            tenant_id: self.config.tenant_id.clone(),
        };
        let req = self
            .http
            .post(&url)
            .bearer_auth(&self.config.relay_token)
            .json(&body);
        match req.send().await {
            Ok(r) if r.status().is_success() => {
                info!(
                    target: "weft_listener::relay",
                    "dispatcher acknowledged empty registry"
                );
            }
            Ok(r) => {
                tracing::debug!(
                    target: "weft_listener::relay",
                    status = %r.status(),
                    "dispatcher kept us alive (race or new register)"
                );
            }
            Err(e) => {
                error!(
                    target: "weft_listener::relay",
                    error = %e,
                    "listener/empty post failed"
                );
            }
        }
    }
}

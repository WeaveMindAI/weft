//! Relays fires from the listener back to the dispatcher.
//!
//! Every kind handler (timer tick, SSE event, webhook POST, form
//! submit, socket message) ends with a call to
//! `FireRelayer::fire(token, payload)`. The relayer POSTs
//! `/signal-fired` on the dispatcher carrying the project id and
//! the shared relay token for auth.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::error;

use crate::config::ListenerConfig;
use crate::protocol::FireRelay;

pub struct FireRelayer {
    config: Arc<ListenerConfig>,
    http: reqwest::Client,
}

impl FireRelayer {
    pub fn new(config: Arc<ListenerConfig>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client");
        Self { config, http }
    }

    /// Fire-and-forget relay. Logs on failure; we do not retry
    /// here (dispatcher is expected to be always-up; retries are
    /// the kind handler's call if it wants them).
    pub async fn fire(&self, token: String, payload: Value) {
        let body = FireRelay {
            project_id: self.config.project_id.clone(),
            token: token.clone(),
            payload,
        };
        let url = format!("{}/signal-fired", self.config.dispatcher_url.trim_end_matches('/'));
        let req = self
            .http
            .post(&url)
            .bearer_auth(&self.config.relay_token)
            .json(&body);
        match req.send().await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => {
                error!(
                    target: "weft_listener::relay",
                    status = %r.status(),
                    token = %token,
                    "dispatcher rejected signal-fired"
                );
            }
            Err(e) => {
                error!(
                    target: "weft_listener::relay",
                    error = %e,
                    token = %token,
                    "signal-fired relay failed"
                );
            }
        }
    }
}

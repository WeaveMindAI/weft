//! Caller authentication against the broker's `/storage/authorize`.
//!
//! Neither the storage box nor the dispatcher validates tokens
//! itself (only the broker holds TokenReview authority); both RELAY
//! the presented bearer to the broker and act on the verified
//! verdict. Two views over ONE client + ONE fake:
//!   - `BrokerAuthorizeOps::authorize_raw`: the raw verdict
//!     (worker / control-plane / storage-box). The dispatcher uses
//!     this to authenticate a box's grow/shrink requests.
//!   - `BoxAuthOps::authorize`: the box's data-path view, mapping
//!     the verdict onto the prefix wall's `CallerAuth` (a storage
//!     box has no business on another box's data path and is
//!     denied). Blanket-implemented over `BrokerAuthorizeOps`.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use weft_broker_client::protocol::{StorageAuthorizeRequest, StorageAuthorizeResponse};

use crate::key::CallerAuth;

/// Raw verdict: verified identity or a caller-safe denial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawAuth {
    Allowed(StorageAuthorizeResponse),
    Denied(String),
}

#[async_trait]
pub trait BrokerAuthorizeOps: Send + Sync {
    async fn authorize_raw(&self, bearer: &str, color: Option<&str>) -> Result<RawAuth>;
}

/// The box's data-path view of an authorize outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthOutcome {
    Allowed(CallerAuth),
    Denied(String),
}

#[async_trait]
pub trait BoxAuthOps: Send + Sync {
    async fn authorize(&self, bearer: &str, color: Option<&str>) -> Result<AuthOutcome>;
}

#[async_trait]
impl<T: BrokerAuthorizeOps + ?Sized> BoxAuthOps for T {
    async fn authorize(&self, bearer: &str, color: Option<&str>) -> Result<AuthOutcome> {
        Ok(match self.authorize_raw(bearer, color).await? {
            RawAuth::Denied(reason) => AuthOutcome::Denied(reason),
            RawAuth::Allowed(verdict) => match verdict {
                StorageAuthorizeResponse::Worker { tenant_id, project_id, color } => {
                    AuthOutcome::Allowed(CallerAuth::Worker {
                        tenant: tenant_id,
                        project_id,
                        color,
                    })
                }
                StorageAuthorizeResponse::ControlPlane => {
                    AuthOutcome::Allowed(CallerAuth::ControlPlane)
                }
                StorageAuthorizeResponse::StorageBox { .. } => AuthOutcome::Denied(
                    "storage-box identities cannot use the storage data path".into(),
                ),
            },
        })
    }
}

/// Production impl: HTTP relay to the broker.
pub struct BrokerAuth {
    broker_url: String,
    http: reqwest::Client,
}

impl BrokerAuth {
    pub fn new(broker_url: String) -> Arc<Self> {
        Arc::new(Self { broker_url, http: reqwest::Client::new() })
    }
}

#[async_trait]
impl BrokerAuthorizeOps for BrokerAuth {
    async fn authorize_raw(&self, bearer: &str, color: Option<&str>) -> Result<RawAuth> {
        let url = format!("{}/storage/authorize", self.broker_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(&url)
            .bearer_auth(bearer)
            .json(&StorageAuthorizeRequest { color: color.map(String::from) })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
        {
            let body = resp.text().await.unwrap_or_default();
            return Ok(RawAuth::Denied(body));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("broker authorize returned {status}: {body}"));
        }
        let verdict: StorageAuthorizeResponse =
            resp.json().await.context("parse broker authorize response")?;
        Ok(RawAuth::Allowed(verdict))
    }
}

// ---------- fake (test-helpers) ----------

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeAuth;

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    /// Dumb token -> verdict map: unknown tokens are denied, known
    /// tokens return their seeded verdict verbatim. No business logic
    /// (color-ownership verification lives in the real broker).
    #[derive(Default)]
    pub struct FakeAuth {
        tokens: Mutex<HashMap<String, StorageAuthorizeResponse>>,
    }

    impl FakeAuth {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Seed via the box's wall type (most tests think in
        /// `CallerAuth`).
        pub fn seed(&self, token: &str, caller: CallerAuth) {
            let raw = match caller {
                CallerAuth::Worker { tenant, project_id, color } => {
                    StorageAuthorizeResponse::Worker { tenant_id: tenant, project_id, color }
                }
                CallerAuth::ControlPlane => StorageAuthorizeResponse::ControlPlane,
            };
            self.tokens.lock().insert(token.to_string(), raw);
        }

        /// Seed a raw verdict (e.g. a StorageBox identity for
        /// dispatcher-side tests).
        pub fn seed_raw(&self, token: &str, verdict: StorageAuthorizeResponse) {
            self.tokens.lock().insert(token.to_string(), verdict);
        }
    }

    #[async_trait]
    impl BrokerAuthorizeOps for FakeAuth {
        async fn authorize_raw(&self, bearer: &str, _color: Option<&str>) -> Result<RawAuth> {
            // `_color` is ignored: a test that wants "wrong color is
            // denied" seeds a token mapping to Denied, or seeds the
            // worker verdict with the color it will claim.
            Ok(match self.tokens.lock().get(bearer).cloned() {
                Some(verdict) => RawAuth::Allowed(verdict),
                None => RawAuth::Denied("unknown token".into()),
            })
        }
    }
}

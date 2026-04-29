//! Sticky routing layer for HA dispatcher.
//!
//! Every Pod can serve any HTTP request. State that lives in RAM
//! (slot mpsc senders, listener pool handles, EventBus subscribers)
//! is owned by one Pod per tenant or per color. Postgres lease
//! tables tell us which Pod owns what.
//!
//! Routing pattern:
//!  1. Caller hits any Pod via the cluster Service.
//!  2. Pod looks up lease for the target color/tenant.
//!  3. If lease is unowned or expired: optionally claim it, then
//!     handle locally.
//!  4. If lease is owned by self: handle locally.
//!  5. If lease is owned by another Pod: forward the request to
//!     `<owner-pod>.<headless-service>:<port><path>` over internal
//!     HTTP. Add the shared internal secret header so the receiver
//!     trusts the call without re-authenticating.
//!
//! Pod-to-Pod URLs use the headless-service DNS (StatefulSet sets
//! up stable Pod DNS like `weft-dispatcher-2.weft-dispatcher-headless.weft-system.svc.cluster.local`).
//! Configured via `WEFT_INTERNAL_BASE_URL_TEMPLATE`, defaulting to
//! `http://{pod}.weft-dispatcher-headless.weft-system.svc.cluster.local:9999`.

use anyhow::Result;
use axum::http::HeaderMap;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::lease;
use crate::state::DispatcherState;

/// Header carrying the shared internal secret. The receiving Pod
/// requires it on every `/internal/*` route. Generated at cluster
/// init and stored as a k8s Secret mounted into every dispatcher
/// Pod via env var `WEFT_INTERNAL_SECRET`.
pub const INTERNAL_SECRET_HEADER: &str = "x-weft-internal-secret";

/// Outcome of `route_for_color`.
pub enum ColorRoute {
    /// We own the slot lease (or just claimed it). Caller handles
    /// the request locally.
    Local,
    /// Another Pod owns the lease. Caller should forward to that
    /// Pod's internal endpoint.
    Forward { owner_pod_id: String },
}

/// Look up the slot owner for `color`. If the row is missing or
/// expired, claim it for the local Pod and return `Local`. If a
/// live owner exists and it's us, return `Local`. If a live owner
/// exists and it's someone else, return `Forward`.
pub async fn route_for_color(
    state: &DispatcherState,
    color: weft_core::Color,
) -> Result<ColorRoute> {
    let outcome = lease::claim_slot(&state.pg_pool, color, state.pod_id.as_str()).await?;
    match outcome {
        lease::ClaimOutcome::AcquiredFresh
        | lease::ClaimOutcome::AcquiredAfterExpiry { .. } => Ok(ColorRoute::Local),
        lease::ClaimOutcome::HeldByOther { current_owner } => {
            if current_owner == state.pod_id.as_str() {
                Ok(ColorRoute::Local)
            } else {
                Ok(ColorRoute::Forward {
                    owner_pod_id: current_owner,
                })
            }
        }
    }
}

/// Forward a JSON request to another Pod's `/internal/*` endpoint.
/// Returns the deserialized response body. The internal secret
/// header is added automatically.
pub async fn forward_to_pod<Req, Resp>(
    state: &DispatcherState,
    owner_pod_id: &str,
    path: &str,
    body: &Req,
) -> Result<Resp>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let url = pod_internal_url(state, owner_pod_id, path);
    let resp = http_client()
        .post(&url)
        .header(INTERNAL_SECRET_HEADER, state.config.internal_secret.as_str())
        .json(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "internal forward to {url} returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
    Ok(resp.json::<Resp>().await?)
}

/// Like `forward_to_pod` but doesn't expect a body (returns Ok on
/// any 2xx). Used for fire-and-forget control messages.
pub async fn forward_to_pod_noreply<Req: Serialize>(
    state: &DispatcherState,
    owner_pod_id: &str,
    path: &str,
    body: &Req,
) -> Result<()> {
    let url = pod_internal_url(state, owner_pod_id, path);
    let resp = http_client()
        .post(&url)
        .header(INTERNAL_SECRET_HEADER, state.config.internal_secret.as_str())
        .json(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "internal forward to {url} returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
    Ok(())
}

/// Build the internal URL for a target Pod. Template comes from
/// `DispatcherConfig::internal_url_template`; default points at the
/// headless Service DNS.
fn pod_internal_url(state: &DispatcherState, owner_pod_id: &str, path: &str) -> String {
    let base = state
        .config
        .internal_url_template
        .replace("{pod}", owner_pod_id);
    format!("{}{}", base.trim_end_matches('/'), path)
}

fn http_client() -> reqwest::Client {
    // Cheap: reqwest::Client clones share a connection pool. We
    // build one per call here for simplicity; a real hot path
    // would stash a single client on DispatcherState.
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("reqwest client")
}

/// Verify the incoming `/internal/*` request carries the right
/// internal secret. Returns Ok if authorized.
pub fn require_internal_secret(
    state: &DispatcherState,
    headers: &HeaderMap,
) -> Result<(), axum::http::StatusCode> {
    let got = headers
        .get(INTERNAL_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got != state.config.internal_secret.as_str() {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

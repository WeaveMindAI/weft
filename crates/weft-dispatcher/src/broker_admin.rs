//! The dispatcher's authenticated client of the broker's control-plane
//! admin surfaces (runtime-file admin, access admin). One place owns
//! the SA-token signing and URL joining; each surface builds its own
//! request/response handling on top (the storage sweep wants typed
//! retry classes, the access forwards want verbatim status passthrough).

use anyhow::{Context, Result};
use axum::http::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::state::DispatcherState;

/// Read the dispatcher's own SA token. Re-read every call so kubelet
/// token rotation propagates; async so the read never blocks the
/// runtime (the token is re-projected periodically).
pub async fn read_token(state: &DispatcherState) -> Result<String> {
    let bytes = tokio::fs::read(&state.broker_token_path)
        .await
        .with_context(|| format!("read dispatcher SA token at {}", state.broker_token_path.display()))?;
    Ok(String::from_utf8(bytes).context("SA token not utf8")?.trim().to_string())
}

pub fn admin_url(state: &DispatcherState, path: &str) -> String {
    format!("{}{}", state.broker_url.trim_end_matches('/'), path)
}

/// POST one admin request and hand back the broker's answer with its
/// status class INTACT: a 4xx the broker minted for the editor (bad
/// input, reconnect-needed) must reach the editor as that status and
/// message, not collapse into a dispatcher 500. Transport faults (the
/// broker unreachable) are the only 500s minted here.
pub async fn forward_json<Req: Serialize, Resp: DeserializeOwned>(
    state: &DispatcherState,
    path: &str,
    body: &Req,
) -> Result<Resp, (StatusCode, String)> {
    let internal = |e: String| {
        tracing::error!(target: "weft_dispatcher::broker_admin", "broker admin {path}: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, "broker unreachable".to_string())
    };
    let token = read_token(state).await.map_err(|e| internal(format!("{e:#}")))?;
    let resp = state
        .http
        .post(admin_url(state, path))
        .bearer_auth(token)
        .json(body)
        .send()
        .await
        .map_err(|e| internal(e.to_string()))?;
    let status = resp.status();
    if !status.is_success() {
        let msg = resp.text().await.unwrap_or_default();
        return Err((status, msg));
    }
    resp.json::<Resp>().await.map_err(|e| internal(format!("decode answer: {e}")))
}

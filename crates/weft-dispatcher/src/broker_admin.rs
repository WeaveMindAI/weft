//! The access forwards to the broker's control-plane admin surface, with
//! the broker's status class passed through verbatim. The identity and
//! URL joining live in `crate::role_client`.

use axum::http::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::state::DispatcherState;

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
    let resp = state
        .broker
        .request(reqwest::Method::POST, path)
        .await
        .map_err(|e| internal(format!("{e:#}")))?
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

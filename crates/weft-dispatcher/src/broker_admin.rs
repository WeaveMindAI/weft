//! The access forwards to the broker's control-plane admin surface, with
//! the broker's status class passed through verbatim, on the dispatcher's
//! line to the broker (`DispatcherState::broker_line`).

use axum::http::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::state::DispatcherState;

/// How long the dispatcher waits on one admin call. One stands in front of
/// every gated live call, so a broker that cannot be reached is said at
/// once (`REOPEN_WAIT`). The answer may take long, since some calls make a
/// provider round trip (a connect, a lookup); the bound on it only keeps a
/// broker that vanished mid-call from holding the caller forever.
const ADMIN_WAIT: weft_broker_client::line::CallWait = weft_broker_client::line::CallWait {
    to_send: weft_broker_client::line::REOPEN_WAIT,
    to_answer: std::time::Duration::from_secs(300),
};

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
    let body = serde_json::to_vec(body).map_err(|e| internal(format!("encode the call: {e}")))?;
    let answer = state.broker_line.call(path, body, ADMIN_WAIT).await.map_err(|e| internal(format!("{e:#}")))?;
    if !answer.status.is_success() {
        return Err((answer.status, String::from_utf8_lossy(&answer.body).into_owned()));
    }
    serde_json::from_slice::<Resp>(&answer.body).map_err(|e| internal(format!("decode answer: {e}")))
}

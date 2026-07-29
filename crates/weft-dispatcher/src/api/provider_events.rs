//! The public events receiver: `POST /events/{service}/{topic}`,
//! where providers deliver the pushes that serve `provider_events`
//! subscriptions on the dial-in transport.
//!
//! The dispatcher stays secret-free: the raw request (exact bytes,
//! headers, query) is forwarded to the broker, which holds the
//! registered apps' secrets and answers a verdict. This side then
//! does what it owns: find the matching subscriptions in the signal
//! table, evaluate each one's filter, and push each fire through the
//! same lifecycle gate every external fire passes (a parked project
//! parks the fire; a wiped one refuses it).
//!
//! Delivery is at-least-once: the 200 goes out only after every
//! matched fire is handed to the durable side, so a crash before the
//! answer makes the provider retry and re-fire. The pre-fire
//! predicates and the fire handling itself are where a retried
//! delivery is a no-op or a user-visible re-run; no content dedup
//! runs here.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;

use weft_broker_client::protocol::{EventVerdict, EventVerifyRequest};
use weft_core::signal::predicate;

use crate::state::DispatcherState;

/// One matched subscription, ready to fire.
struct MatchedSignal {
    token: String,
    payload: Value,
}

/// POST /events/{service}/{topic}: accept one provider push.
pub async fn receive_event(
    State(state): State<DispatcherState>,
    Path((service, topic)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // The path segments ride into SQL and log lines; hold them to the
    // service charset before anything else.
    if !weft_core::node::is_valid_provider_name(&service)
        || !weft_core::node::is_valid_provider_name(&topic)
    {
        return (StatusCode::NOT_FOUND, "unknown event source").into_response();
    }

    let header_map: BTreeMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
        .collect();
    let verify = EventVerifyRequest {
        service: service.clone(),
        topic: topic.clone(),
        body_b64: base64::engine::general_purpose::STANDARD.encode(&body),
        headers: header_map,
        query,
        // The route is POST-mounted (address-proving handshakes POST
        // too); schemes that sign the method cover this literal.
        method: "POST".to_string(),
    };
    let verdict: EventVerdict =
        match crate::broker_admin::forward_json(&state, "/v1/events/verify", &verify).await {
            Ok(v) => v,
            // The broker's status class IS the answer: a forged push
            // meets its 401 verbatim, an unknown source its 404.
            Err((status, msg)) => return (status, msg).into_response(),
        };

    match verdict {
        EventVerdict::Challenge { body, content_type } => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, content_type)],
            body,
        )
            .into_response(),
        EventVerdict::Drop { reason } => {
            tracing::debug!(
                target: "weft_dispatcher::provider_events",
                %service, %topic, %reason,
                "verified push dropped"
            );
            StatusCode::OK.into_response()
        }
        EventVerdict::Deliver { named_event, targets, signal_token } => {
            let matched = match match_signals(&state, &topic, &named_event, &targets, signal_token)
                .await
            {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        target: "weft_dispatcher::provider_events",
                        %service, %topic, error = %format!("{e:#}"),
                        "subscription matching failed"
                    );
                    return (StatusCode::INTERNAL_SERVER_ERROR, "matching failed".to_string())
                        .into_response();
                }
            };
            // The fires run BEFORE the 200: the provider is told
            // "delivered" only once every matched fire is on the
            // durable side (dispatched, or parked on its signal
            // row). An infrastructure failure answers 5xx so the
            // provider's retry redelivers; a lifecycle refusal (a
            // wiped project's 410) is a final answer a retry cannot
            // change, so it stays inside the 200.
            for m in matched {
                if let Err(status) = dispatch_one(&state, m).await {
                    return (status, "event fire failed; retry expected".to_string())
                        .into_response();
                }
            }
            StatusCode::OK.into_response()
        }
    }
}

/// The subscriptions this push feeds, with each one's filter already
/// applied. Subscription-routed pushes name their signal; account-
/// routed ones match every `provider_events` entry signal registered
/// against one of the target connections, on the same topic.
async fn match_signals(
    state: &DispatcherState,
    topic: &str,
    named_event: &Value,
    targets: &[weft_broker_client::protocol::EventTargetWire],
    signal_token: Option<String>,
) -> anyhow::Result<Vec<MatchedSignal>> {
    let rows = match signal_token {
        Some(token) => {
            sqlx::query(
                "SELECT token, tenant_id, spec_json FROM signal
                 WHERE token = $1 AND is_resume = FALSE",
            )
            .bind(token)
            .fetch_all(&state.pg_pool)
            .await?
        }
        None => {
            let access_ids: Vec<String> =
                targets.iter().map(|t| t.access_id.clone()).collect();
            // `access_id` is written on the row at register time from
            // the spec, exactly so this match is one indexed filter
            // instead of a spec-parsing table scan.
            sqlx::query(
                "SELECT token, tenant_id, spec_json FROM signal
                 WHERE is_resume = FALSE
                   AND access_id = ANY($1)
                   AND (spec_json::jsonb ->> 'kind') = 'provider_events'",
            )
            .bind(&access_ids)
            .fetch_all(&state.pg_pool)
            .await?
        }
    };

    let mut matched = Vec::new();
    for row in rows {
        let token: String = row.try_get("token")?;
        let tenant_id: String = row.try_get("tenant_id")?;
        let spec_json: String = row.try_get("spec_json")?;
        let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&spec_json)
            .map_err(|e| anyhow::anyhow!("malformed spec_json for signal {token}: {e}"))?;
        // Defense in depth on the account-routed path: the signal's
        // connection must belong to the tenant the broker matched it
        // under. A mismatch means the rows drifted; refuse the pair
        // rather than fire across the wall.
        if !targets.is_empty() {
            let target_ok = targets.iter().any(|t| {
                spec.access.as_ref().map(|a| a.id.as_str()) == Some(t.access_id.as_str())
                    && t.tenant_id == tenant_id
            });
            if !target_ok {
                continue;
            }
        }
        // Only subscriptions on THIS topic: one connection may hold
        // topics with overlapping field names, and a mailbox push
        // must not fire a file-watch trigger.
        let signal_topic = spec.config.get("topic").and_then(Value::as_str).unwrap_or("");
        if signal_topic != topic {
            continue;
        }
        // App-wide subscriptions are served ONLY by the dial-out
        // socket (a push routes to one account's connections, which
        // can never mean "every install of your app"); a push must
        // not half-serve one.
        if spec.config.get("scope").and_then(Value::as_str) == Some("app") {
            continue;
        }
        if !predicate::matches(&spec.match_predicates, named_event) {
            continue;
        }
        matched.push(MatchedSignal { token, payload: named_event.clone() });
    }
    Ok(matched)
}

/// Push one fire through the shared lifecycle gate (park / refuse /
/// dispatch). A lifecycle refusal (4xx: the project is wiped, the
/// token gone) is a final answer, logged and absorbed; an
/// infrastructure failure (5xx) bubbles as `Err` so the receive
/// handler answers 5xx and the provider redelivers.
async fn dispatch_one(state: &DispatcherState, m: MatchedSignal) -> Result<(), StatusCode> {
    match crate::api::signal::fire_registered_signal(state, &m.token, m.payload).await {
        Ok(status) => {
            tracing::debug!(
                target: "weft_dispatcher::provider_events",
                token = %m.token, status = %status.as_u16(),
                "event fire dispatched"
            );
            Ok(())
        }
        Err((status, msg)) if status.is_server_error() => {
            tracing::error!(
                target: "weft_dispatcher::provider_events",
                token = %m.token, status = %status.as_u16(), %msg,
                "event fire failed"
            );
            Err(status)
        }
        Err((status, msg)) => {
            tracing::warn!(
                target: "weft_dispatcher::provider_events",
                token = %m.token, status = %status.as_u16(), %msg,
                "event fire refused"
            );
            Ok(())
        }
    }
}

//! The public events receiver: `POST /events/{service}/{topic}`,
//! where providers deliver the pushes that serve `provider_events`
//! subscriptions on the dial-in transport.
//!
//! The dispatcher stays secret-free: the raw request (exact bytes,
//! headers, query) is forwarded to the broker, which holds the
//! registered apps' secrets and answers a verdict.
//!
//! Then this side does what it owns, and STOPS there. It owns the
//! public door, the verdict, narrowing the candidates to the signals
//! hanging off the connections the broker named, holding each to its
//! tenant, and pushing every resulting fire through the same lifecycle
//! gate every external fire passes (a parked project parks the fire; a
//! wiped one refuses it). It does NOT own which signals a push feeds:
//! that reads a trigger kind's own settings, the kinds live in the
//! listener, and asking it is one call (`/match_push`). Nothing here
//! names a kind, and a new sort of push-fed trigger is listener code
//! only.
//!
//! Delivery is at-least-once: the 200 goes out only after every
//! matched fire is handed to the durable side, so a crash before the
//! answer makes the provider retry and re-fire. The listener's filter
//! pass and the fire handling itself are where a retried delivery is a
//! no-op or a user-visible re-run; no content dedup runs here.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;

use weft_broker_client::protocol::{EventVerdict, EventVerifyRequest};

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
            let matched = match match_signals(
                &state,
                &service,
                &topic,
                &named_event,
                &targets,
                signal_token,
            )
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

/// The signals this push feeds, with each one's payload.
///
/// Two steps, and the split between them is the tier boundary. HERE:
/// narrow the candidates to the signals hanging off the connections the
/// broker named, and hold each to the tenant it was matched under. THE
/// LISTENER: decide which of those the push actually addresses and what
/// it wakes with, because that reads a kind's own settings (its topic,
/// its subscription scope) and applies the signal's filter through the
/// one gate every fire passes.
///
/// Nothing in this function names a signal kind, and nothing in it reads
/// a kind's config. That is the point: a second kind of push-fed trigger
/// lands as listener code, and a push that feeds one is not silently
/// matched by nothing here.
async fn match_signals(
    state: &DispatcherState,
    service: &str,
    topic: &str,
    named_event: &Value,
    targets: &[weft_broker_client::protocol::EventTargetWire],
    signal_token: Option<String>,
) -> anyhow::Result<Vec<MatchedSignal>> {
    // Entry AND resume signals both feed from pushes: a trigger fires
    // a fresh execution, an awaited signal (a node parked on
    // `await_signal(ProviderEvents...)`, e.g. a Slack button wait)
    // resumes its execution. The lifecycle gate downstream already
    // routes each by its row's own is_resume; a resume signal always
    // carries a predicate pinning it to its minted correlation id
    // (registration refuses a predicate-less provider_events resume),
    // so a broad push never resumes the wrong wait.
    let rows = match signal_token {
        Some(token) => {
            sqlx::query(
                "SELECT token, tenant_id, access_id FROM signal
                 WHERE token = $1",
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
                "SELECT token, tenant_id, access_id FROM signal
                 WHERE access_id = ANY($1)",
            )
            .bind(&access_ids)
            .fetch_all(&state.pg_pool)
            .await?
        }
    };

    let mut candidates: Vec<String> = Vec::new();
    for row in rows {
        let token: String = row.try_get("token")?;
        let tenant_id: String = row.try_get("tenant_id")?;
        let access_id: Option<String> = row.try_get("access_id")?;
        // Defense in depth on the account-routed path: the signal's
        // connection must belong to the tenant the broker matched it
        // under. A mismatch means the rows drifted; refuse the pair
        // rather than fire across the wall. This is a tenancy check,
        // not a routing one, which is why it stays on this side.
        if !targets.is_empty() {
            let target_ok = targets.iter().any(|t| {
                access_id.as_deref() == Some(t.access_id.as_str()) && t.tenant_id == tenant_id
            });
            if !target_ok {
                continue;
            }
        }
        candidates.push(token);
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Ask the pods holding them. A push can feed subscriptions spread
    // over several listener pods, so the candidates are grouped by
    // holder and each pod asked once. A signal with no live holder is
    // re-placed first: a parked webhook trigger's holder may have been
    // reaped while the subscription stayed alive at the provider, and
    // dropping the push because nothing happened to be running is the
    // silent loss the fire path already refuses to take.
    let mut by_pod: BTreeMap<String, (crate::listener::ListenerHandle, Vec<String>)> =
        BTreeMap::new();
    for token in candidates {
        let handle = state
            .listeners
            .ensure_placed_handle(&token, state.listener_backend.as_ref(), &state.pg_pool, state.pod_id.as_str())
            .await?;
        by_pod
            .entry(handle.admin_url.clone())
            .or_insert_with(|| (handle, Vec::new()))
            .1
            .push(token);
    }

    let push = weft_core::signal::listener_protocol::PushEvent {
        service: service.to_string(),
        topic: topic.to_string(),
        event: named_event.clone(),
    };
    let mut matched = Vec::new();
    for (handle, tokens) in by_pod.into_values() {
        for m in crate::listener::match_push(&handle, &push, &tokens).await? {
            matched.push(MatchedSignal { token: m.token, payload: m.payload });
        }
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

#[cfg(test)]
mod tier_boundary {
    /// This module's own source, read at compile time.
    const SOURCE: &str = include_str!("provider_events.rs");

    /// The source WITHOUT this test module, since the list of things to
    /// look for would otherwise be found in the looking.
    fn code() -> &'static str {
        SOURCE.split("#[cfg(test)]").next().expect("a split always yields a first piece")
    }

    /// The public events receiver must hold NO knowledge of any trigger
    /// kind. It takes the push at the door, has it verified, narrows the
    /// candidates by connection, and asks the listener which of them the
    /// push feeds; the listener owns the rest because the rest is a
    /// kind's own vocabulary.
    ///
    /// This is a source check rather than a behaviour check on purpose.
    /// The damage from putting the knowledge back is SILENT: a second
    /// sort of push-fed trigger simply never matches, the provider is
    /// answered 200, and nothing fires anywhere with no error to see. So
    /// the thing to catch is the shape, at the moment somebody writes
    /// it, not a symptom later.
    #[test]
    fn the_receiver_names_no_kind_and_reads_no_kind_config() {
        // Each of these was here before the matching moved to the
        // listener, and each is one way the knowledge creeps back.
        let forbidden = [
            // Matching rows by kind tag.
            ("\"provider_events\"", "match signals by kind tag"),
            // Parsing a signal's spec to read a kind's settings.
            ("spec_json", "parse a signal spec to read a kind's config"),
            // A kind's own config keys.
            ("\"topic\"", "read a kind's topic setting"),
            ("\"scope\"", "read a kind's subscription scope"),
            // The filter gate, which the listener owns for every kind.
            ("predicate::matches", "re-implement the shared filter gate"),
        ];
        for (needle, what) in forbidden {
            assert!(
                !code().contains(needle),
                "the events receiver must not {what}: found `{needle}`. \
                 Which signals a push feeds is the listener's answer \
                 (`/match_push`), because it reads a trigger kind's own \
                 settings. Adding it back here means a new push-fed kind \
                 silently matches nothing."
            );
        }
    }
}

//! The per-signal fire plumbing every kind that raises its own fires
//! holds: who the signal is (token, tenant), the sink its fires ride,
//! and the spec-level pre-fire filter.
//!
//! ONE gate for every kind: a payload that fails the signal's
//! declared predicates is dropped here, between "the kind produced a
//! payload" and "a fire is enqueued", so filtering means the same
//! thing on a timer tick, an SSE event, a socket frame, and a
//! provider push. Kinds never re-implement it and never see it.

use serde_json::Value;
use tracing::{debug, info, warn};

use weft_core::signal::predicate::{matches, Predicate};

use crate::fire_sink::FireSignalSink;

/// What one [`FireContext::fire`] call did. `#[must_use]` so every
/// kind decides explicitly whether the outcome matters to it (a
/// cursor-keeping kind gates its cursor on `EnqueueFailed`; a kind
/// with no replay cursor ignores it with a `let _ =` and a comment).
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FireOutcome {
    /// Enqueued for delivery.
    Fired,
    /// The signal's own filter dropped it: a deliberate non-fire, so
    /// a cursor may advance past it.
    Filtered,
    /// The enqueue failed (logged); the item was NOT delivered and a
    /// cursor must not advance past it.
    EnqueueFailed,
    /// This copy no longer holds the signal (HTTP 409): another holder
    /// took it, or it was registered again to be served another way. NOT
    /// delivered; whoever serves it now delivers its own events, and this
    /// copy stops its connection at its next look.
    NotHeld,
    /// The broker does not know this signal's token (HTTP 404). NOT
    /// delivered. It means either the row is not committed yet (a fire
    /// racing its own registration) or the signal is gone; the broker
    /// cannot tell which, so the kind decides from how long ago it
    /// armed.
    UnknownSignal,
}

/// Cheap to clone (Arc inside the sink; the rest is small).
#[derive(Clone)]
pub struct FireContext {
    sink: FireSignalSink,
    token: String,
    /// The signal's tenant, stamped on every enqueued fire (the
    /// listener serves many tenants, so it travels per signal).
    tenant_id: String,
    /// Which instance the signal belongs to (`None` for a shared one), as
    /// the dispatcher registered it: an instance's trigger reads through
    /// that instance's connections alone.
    for_instance: Option<weft_core::instance::InstanceScope>,
    /// The spec-level pre-fire filter. Empty = fire on everything.
    predicates: Vec<Predicate>,
    /// The holder whose claim the signal is served under, for a held
    /// connection: the broker takes its fires only while the row is still
    /// held under that name.
    held_by: Option<String>,
}

impl FireContext {
    pub fn new(
        sink: FireSignalSink,
        token: String,
        tenant_id: String,
        for_instance: Option<weft_core::instance::InstanceScope>,
        predicates: Vec<Predicate>,
    ) -> Self {
        Self { sink, token, tenant_id, for_instance, predicates, held_by: None }
    }

    /// The same context, firing under the claim of the holder `replica`.
    pub fn held_by(self, replica: String) -> Self {
        Self { held_by: Some(replica), ..self }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn for_instance(&self) -> Option<&weft_core::instance::InstanceScope> {
        self.for_instance.as_ref()
    }

    /// Fire one payload: evaluate the signal's filter, then enqueue.
    /// An enqueue failure is logged (never propagated: a dropped fire
    /// must not kill the event-source loop, but is never silent) and
    /// reported as [`FireOutcome::EnqueueFailed`], so a cursor-keeping
    /// caller can hold its cursor at the last delivered item and
    /// re-offer this one next poll instead of losing it. `target` is
    /// the caller's kind tag, so the log names it.
    pub async fn fire(&self, payload: Value, target: &str) -> FireOutcome {
        self.fire_keyed(payload, target, crate::fire_sink::FireIdentity::Payload).await
    }

    /// [`Self::fire`] for an event with a name of its own (a timer's
    /// moment, `tick:{due}`): a re-fire of the same name collapses onto a
    /// fire still queued, whatever its payload says, so a kind that fires
    /// before claiming can retry without firing twice.
    pub async fn fire_as(&self, payload: Value, target: &str, name: &str) -> FireOutcome {
        self.fire_keyed(payload, target, crate::fire_sink::FireIdentity::Named(name)).await
    }

    /// [`Self::fire`] for a payload cut down from what was observed (a
    /// poll that carries only some fields): the filter reads `judged`,
    /// the whole observation, and `payload` is what goes out.
    pub async fn fire_judged(&self, judged: &Value, payload: Value, target: &str) -> FireOutcome {
        self.fire_filtered(Some(judged), payload, target, crate::fire_sink::FireIdentity::Payload).await
    }

    async fn fire_keyed(&self, payload: Value, target: &str, identity: crate::fire_sink::FireIdentity<'_>) -> FireOutcome {
        self.fire_filtered(None, payload, target, identity).await
    }

    async fn fire_filtered(
        &self,
        judged: Option<&Value>,
        payload: Value,
        target: &str,
        identity: crate::fire_sink::FireIdentity<'_>,
    ) -> FireOutcome {
        if !matches(&self.predicates, judged.unwrap_or(&payload)) {
            debug!(
                target: "weft_listener::event_context",
                kind = target, token = %self.token,
                "payload did not match the signal's filter; not firing"
            );
            return FireOutcome::Filtered;
        }
        let not_held = || {
            info!(
                target: "weft_listener::event_context",
                kind = target, token = %self.token,
                "this copy no longer holds the signal; the event is left to the one that does"
            );
            FireOutcome::NotHeld
        };
        match self.sink.fire(&self.token, &self.tenant_id, self.held_by.as_deref(), payload, identity).await {
            Ok(crate::fire_sink::Delivered::Taken) => FireOutcome::Fired,
            Ok(crate::fire_sink::Delivered::NotHeld) => not_held(),
            Err(e) if e
                .downcast_ref::<weft_broker_client::BrokerRefused>()
                .is_some_and(|r| r.status == reqwest::StatusCode::CONFLICT) =>
            {
                info!(
                    target: "weft_listener::event_context",
                    kind = target, token = %self.token, error = %e,
                    "this copy no longer holds the signal; the event is left to the one that does"
                );
                FireOutcome::NotHeld
            }
            Err(e) if e
                .downcast_ref::<weft_broker_client::BrokerRefused>()
                .is_some_and(|r| r.status == reqwest::StatusCode::NOT_FOUND) =>
            {
                debug!(
                    target: "weft_listener::event_context",
                    kind = target, token = %self.token, error = %e,
                    "the broker does not know this signal token"
                );
                FireOutcome::UnknownSignal
            }
            Err(e) => {
                warn!(
                    target: "weft_listener::event_context",
                    kind = target, token = %self.token, error = %e,
                    "fire enqueue failed"
                );
                FireOutcome::EnqueueFailed
            }
        }
    }

    /// Claim a moment by moving the kind's state from `from_seq` to
    /// `kind_state` at `from_seq + 1`: of two copies of the listener
    /// woken for the same moment, exactly one sees `Ok(true)`, and only
    /// that one acts on it. `Err` is a write that could not be made.
    pub async fn claim_kind_state(&self, kind_state: Value, from_seq: i64) -> anyhow::Result<bool> {
        self.sink.claim_kind_state(&self.token, kind_state, from_seq).await
    }
}

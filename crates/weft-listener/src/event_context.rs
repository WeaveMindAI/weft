//! The per-signal fire plumbing every held-event loop holds: who the
//! signal is (token, tenant, placement generation), the sink its
//! fires ride, and the spec-level pre-fire filter.
//!
//! ONE gate for every kind: a payload that fails the signal's
//! declared predicates is dropped here, between "the kind produced a
//! payload" and "a fire is enqueued", so filtering means the same
//! thing on a timer tick, an SSE event, a socket frame, and a
//! provider push. Kinds never re-implement it and never see it.

use serde_json::Value;
use tracing::{debug, warn};

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
    /// The broker fenced the enqueue: this pod was drained and the
    /// fire was deliberately dropped (the replacement pod will offer
    /// it). NOT delivered; a cursor must not advance past it.
    Fenced,
}

/// Cheap to clone (Arc inside the sink; the rest is small).
#[derive(Clone)]
pub struct FireContext {
    sink: FireSignalSink,
    token: String,
    /// The signal's tenant, stamped on every enqueued fire (a pooled
    /// listener serves many tenants, so it travels per-signal).
    tenant_id: String,
    /// The generation this pod holds the signal under, stamped on
    /// every fire so the broker can fence a stale old-pod fire during
    /// a scale-down move overlap.
    placement_generation: i64,
    /// The spec-level pre-fire filter. Empty = fire on everything.
    predicates: Vec<Predicate>,
}

impl FireContext {
    pub fn new(
        sink: FireSignalSink,
        token: String,
        tenant_id: String,
        placement_generation: i64,
        predicates: Vec<Predicate>,
    ) -> Self {
        Self { sink, token, tenant_id, placement_generation, predicates }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Fire one payload: evaluate the signal's filter, then enqueue.
    /// An enqueue failure is logged (never propagated: a dropped fire
    /// must not kill the event-source loop, but is never silent) and
    /// reported as [`FireOutcome::EnqueueFailed`], so a cursor-keeping
    /// caller can hold its cursor at the last delivered item and
    /// re-offer this one next poll instead of losing it. `target` is
    /// the caller's kind tag, so the log names it.
    pub async fn fire(&self, payload: Value, target: &str) -> FireOutcome {
        if !matches(&self.predicates, &payload) {
            debug!(
                target: "weft_listener::event_context",
                kind = target, token = %self.token,
                "payload did not match the signal's filter; not firing"
            );
            return FireOutcome::Filtered;
        }
        use weft_task_store::tasks::DedupOutcome;
        match self
            .sink
            .fire(&self.token, &self.tenant_id, self.placement_generation, payload)
            .await
        {
            Ok(DedupOutcome::Inserted(_)) | Ok(DedupOutcome::AlreadyLive(_)) => {
                FireOutcome::Fired
            }
            Ok(DedupOutcome::Fenced) => {
                debug!(
                    target: "weft_listener::event_context",
                    kind = target, token = %self.token,
                    "fire fenced (this pod was drained); the replacement pod \
                     will offer the event"
                );
                FireOutcome::Fenced
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

    /// Persist the kind's evolving durable state (a delta-poll
    /// cursor). Same never-kill-the-loop error posture as `fire`: an
    /// enqueue failure is logged and the loop keeps serving (the next
    /// advance re-carries the full state, so a lost write only widens
    /// the at-least-once redelivery window, never loses ground
    /// permanently). `target` is the caller's kind tag for the log.
    pub async fn update_kind_state(&self, kind_state: Value, seq: i64, target: &str) {
        if let Err(e) = self
            .sink
            .update_kind_state(
                &self.token,
                &self.tenant_id,
                self.placement_generation,
                kind_state,
                seq,
            )
            .await
        {
            warn!(
                target: "weft_listener::event_context",
                kind = target, token = %self.token, error = %e,
                "kind-state update enqueue failed"
            );
        }
    }
}

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
    /// An enqueue failure is logged (never propagated): a dropped
    /// fire must not kill the event-source loop, but is never silent.
    /// `target` is the caller's kind tag, so the log names it.
    pub async fn fire(&self, payload: Value, target: &str) {
        if !matches(&self.predicates, &payload) {
            debug!(
                target: "weft_listener::event_context",
                kind = target, token = %self.token,
                "payload did not match the signal's filter; not firing"
            );
            return;
        }
        if let Err(e) = self
            .sink
            .fire(&self.token, &self.tenant_id, self.placement_generation, payload)
            .await
        {
            warn!(
                target: "weft_listener::event_context",
                kind = target, token = %self.token, error = %e,
                "fire enqueue failed"
            );
        }
    }
}

//! Per-drive stream bookkeeping: the live generator feeds (one per
//! consuming firing's `Generator[T]` input) and the pending
//! emission-delivery gates. Owned by the drive loop; every mutation of
//! the pulse table stays on the driver (single-writer invariant), this
//! module only carries the objects the driver routes into.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::mpsc;
use uuid::Uuid;

use weft_core::generator::{
    generator_marker, register_feed, unregister_feed, GeneratorFeed, StreamEnd,
};
use weft_core::liveness::{FiringLocation, WaitLiveness};

use crate::context::TaskMsg;
use crate::wait_tracker::{DeliveryGate, WaitTracker};

/// One live feed serving one generator input port of one firing.
pub struct ConsumerFeed {
    /// The handle-registry id behind the bag marker; unregistered when
    /// the firing ends.
    marker_id: Uuid,
    pub feed: Arc<GeneratorFeed>,
}

/// How an absorption relates to a delivery wait: a RUN dispatch or a
/// pull TAKES the value (delivered), a skip dispatch or a dropped
/// leftover consumes the pulse structurally without a delivery (the
/// delivery can never happen; the waiting producer must fail loudly
/// with `reason`, not resume as if the next stage started).
#[derive(Clone, Copy)]
pub enum AbsorbKind<'a> {
    Taken,
    Skipped { reason: &'a str },
}

pub struct StreamRuntime {
    /// The execution's shared wait tracker, handed to every feed so
    /// consumer pulls register their waits.
    waits: Arc<WaitTracker>,
    /// Live feeds by consuming firing and input port. An entry exists
    /// from the firing's dispatch to its terminal; one feed per
    /// `(firing, port)` by construction (the map shape cannot spell a
    /// duplicate).
    feeds: HashMap<FiringLocation, HashMap<String, ConsumerFeed>>,
    /// Pending delivery gates by the pulse ids they still track. One
    /// gate appears under every pulse its emission created; ALL of a
    /// gate's entries drop the moment it resolves (delivered or
    /// failed), so the map never accumulates entries pointing at
    /// settled gates.
    gates: HashMap<Uuid, Arc<DeliveryGate>>,
}

impl StreamRuntime {
    pub fn new(waits: Arc<WaitTracker>) -> Self {
        Self { waits, feeds: HashMap::new(), gates: HashMap::new() }
    }

    /// Create (and register) the live feed for one generator input of a
    /// firing about to run, returning the bag marker the node's
    /// `ctx.inputs.get::<Generator<T>>` read resolves. `taken_tx` is
    /// the drive loop's task channel; every pull reports its take
    /// through it so the driver absorbs the pulse (which is also what
    /// resolves a producer's delivery wait). A second feed for a
    /// `(firing, port)` that already holds one is an engine dispatch
    /// bug (the router would keep feeding the orphan while the body
    /// reads the newcomer): refused loudly.
    pub fn create_feed(
        &mut self,
        loc: &FiringLocation,
        port: &str,
        taken_tx: mpsc::UnboundedSender<TaskMsg>,
    ) -> Result<serde_json::Value, String> {
        let ports = self.feeds.entry(loc.clone()).or_default();
        if ports.contains_key(port) {
            return Err(format!(
                "a live stream feed already exists for '{}.{port}'; a firing dispatches \
                 its generator inputs exactly once",
                loc.node_id
            ));
        }
        let node = loc.clone();
        let on_taken: Box<dyn Fn(Uuid) + Send + Sync> = Box::new(move |pulse_id| {
            // The loop receiver closing mid-take means the drive is
            // tearing down (cancel); the take's durability is then
            // owned by the cancel walk, so a lost message is fine.
            let _ = taken_tx.send(TaskMsg::StreamItemTaken { loc: node.clone(), pulse_id });
        });
        let liveness = Arc::downgrade(&self.waits) as Weak<dyn WaitLiveness>;
        let feed = GeneratorFeed::new(port, liveness, Some(loc.clone()), on_taken);
        let marker_id = register_feed(&feed);
        ports.insert(port.to_string(), ConsumerFeed { marker_id, feed });
        Ok(generator_marker(marker_id))
    }

    /// The live feed serving `loc`'s input `port`, if the firing is
    /// running with one.
    pub fn feed_for(&self, loc: &FiringLocation, port: &str) -> Option<&Arc<GeneratorFeed>> {
        self.feeds.get(loc)?.get(port).map(|f| &f.feed)
    }

    /// The firing ended (any outcome): tear its feeds down, returning
    /// the pulse ids of items that were routed into the feeds but never
    /// taken. The CALLER MUST absorb them (they are still live in the
    /// pulse table; unabsorbed they would block completion forever).
    /// The feeds are force-ended so a straggler pull errors loudly, and
    /// the bag markers unregister so the process-wide handle registry
    /// never accumulates dead feeds.
    #[must_use = "the returned untaken pulse ids must be absorbed by the driver"]
    pub fn retire(&mut self, loc: &FiringLocation) -> Vec<Uuid> {
        let mut leftover = Vec::new();
        for cf in self.feeds.remove(loc).unwrap_or_default().into_values() {
            leftover.extend(cf.feed.abandon());
            unregister_feed(cf.marker_id);
        }
        leftover
    }

    /// Register an armed delivery gate for the pulses its emission
    /// created.
    pub fn register_gate(&mut self, gate: &Arc<DeliveryGate>, pulse_ids: &[Uuid]) {
        gate.arm(pulse_ids.iter().copied());
        for id in pulse_ids {
            self.gates.insert(*id, gate.clone());
        }
    }

    /// Pulses were absorbed: advance (or fail) the gates tracking them,
    /// per `kind` (see [`AbsorbKind`]). A gate that resolves here
    /// (delivered its last pulse, or failed) is evicted under EVERY
    /// pulse id it tracked, not just the ones in `ids`.
    pub fn on_pulses_absorbed(&mut self, ids: &[Uuid], kind: AbsorbKind<'_>) {
        for id in ids {
            if let Some(gate) = self.gates.remove(id) {
                let resolved = match kind {
                    AbsorbKind::Taken => gate.pulse_absorbed(*id),
                    AbsorbKind::Skipped { reason } => {
                        gate.fail(reason.to_string());
                        true
                    }
                };
                if resolved {
                    for tracked in gate.tracked_ids() {
                        self.gates.remove(&tracked);
                    }
                }
            }
        }
    }

    /// Deadlock resolution: fail every still-pending gate AND poison
    /// every live feed, in one verb so no resolution path can do half
    /// of it. Waiting producers resolve with `reason`; every consumer's
    /// next pull errors with it.
    pub fn resolve_deadlock(&mut self, reason: &str) {
        for gate in self.gates.values() {
            gate.fail(reason.to_string());
        }
        self.gates.clear();
        for cf in self.feeds.values().flat_map(|ports| ports.values()) {
            cf.feed.close(StreamEnd::Failed { error: reason.to_string() });
        }
    }

    /// Fail every still-pending gate (teardown: cancel / drive exit).
    pub fn fail_all_gates(&mut self, reason: &str) {
        for gate in self.gates.values() {
            gate.fail(reason.to_string());
        }
        self.gates.clear();
    }
}

impl Drop for StreamRuntime {
    /// Backstop: a drive that returns with feeds still live (cancel,
    /// poison bail) must not leak their process-wide handle
    /// registrations.
    fn drop(&mut self) {
        for cf in self.feeds.values().flat_map(|ports| ports.values()) {
            unregister_feed(cf.marker_id);
        }
    }
}

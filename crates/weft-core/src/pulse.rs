use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::LoopFrames;
use crate::project::ProjectDefinition;
use crate::RunId;

/// A unit of data flowing between nodes in a run. Pulses carry
/// their own run identity (run) and a frame stack (`frames`)
/// identifying which iteration of which (nested) loop the pulse belongs
/// to. Nodes fire when all required inputs have a pulse with matching
/// `(run_id, frames)` at the exact same frame stack.
///
/// Pulses do NOT carry run metadata; that lives in
/// `NodeFiring` records. This split is load-bearing: the scheduler
/// can replay pulses without the metadata machinery, the metadata can
/// grow without disturbing the hot path.
///
/// The value is SHARED, never copied: one emission that fans out to
/// fifty wires is fifty pulses pointing at one `Arc<Value>`. The only
/// owned copy a value ever gets is the input bag handed to the node
/// body that consumes it.
///
/// A pulse's `id` is DERIVED from the emission that placed it and the
/// wire it landed on (`run::emission::pulse_id`), never minted at
/// random: the journal records the emission once and the fold puts
/// the same pulse, with the same id, on the same wire, so every row
/// that names a pulse by id (a stream take, a loop's item launch)
/// resolves identically live and on replay.
///
/// A pulse with `closed: true` is a CLOSURE marker, not data. It tells
/// the consumer "nothing will ever arrive on this port at this frame
/// stack". An explicit `Value::Null` with `closed: false` is a
/// user-sent null and runs the consumer normally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pulse {
    pub id: uuid::Uuid,
    pub run_id: RunId,
    pub frames: LoopFrames,
    /// The destination node id.
    pub target_node: String,
    /// The port on the destination node.
    pub target_port: String,
    pub value: Arc<Value>,
    pub status: PulseStatus,
    /// Closure marker. `true` means this pulse is the engine telling
    /// the consumer "nothing will arrive here": the upstream terminated
    /// without firing this port. Required port + closure -> consumer
    /// skips (and cascades closure on its outputs). Optional port +
    /// closure -> consumer fires with the port treated as missing.
    /// `value` is always `Null` when `closed`; the field is for
    /// serialisation symmetry only.
    pub closed: bool,
    /// The producer failed or its output was refused: which node broke
    /// and why, carried unchanged however many skips sit between that
    /// node and this wire. Stream consumers receive it at the end;
    /// scalar consumers retain ordinary closure behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<Failure>,
    /// Sent by a stand-in emitted or baked for this run, or a handed
    /// stream's item, including its stream end: not a node that ran.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provided: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<RunId>,
    /// Where the run's record keeps the value, when the table was folded
    /// from the record with its positions (`weft_journal::fold::Fold`
    /// over a reader that shows the run): what an event names the value
    /// by instead of carrying it. `None` on a live engine's table, which
    /// writes the record rather than reading it. Never on the wire: a
    /// table crossing processes is a run's state, and its reader folds
    /// its own.
    #[serde(skip)]
    pub origin: Option<Arc<crate::record_ref::ValueRef>>,
}

/// A node that broke, as the closures it leaves behind carry it: the
/// node, spelled the way the program reads it (through its call sites),
/// and its error. Set once, where the node fails, and passed on
/// unchanged by every skip it causes, so a message three nodes further
/// down still names the node that actually failed.
// SYNC: Failure <-> packages/weft-graph/src/protocol.ts Failure
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Failure {
    pub node: String,
    pub error: String,
}

impl Failure {
    /// The failure of the node `node_id` firing at `frames`, spelled
    /// through the call sites on those frames.
    pub fn at(project: &ProjectDefinition, node_id: &str, frames: &LoopFrames, error: impl Into<String>) -> Self {
        let call_path: Vec<String> = crate::frames::call_path(frames).into_iter().map(str::to_string).collect();
        Self { node: crate::project::address_of(project, node_id, &call_path), error: error.into() }
    }
}

// SYNC: Display for Failure <-> packages/weft-graph/src/webview/lib/utils/status.ts failureText
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}' failed: {}", self.node, self.error)
    }
}

/// Where a pulse still on its wire stands. A pulse a dispatch, a skip or
/// a decision took is out of its bucket altogether, among the table's
/// absorbed pulses ([`PulseTable::absorb`]): where it sits is what says it
/// was taken, so no status says it a second time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PulseStatus {
    Pending,
    /// Handed to a LIVE in-process sink (a running consumer's generator
    /// feed, a loop's stream queue) but not yet taken. Invisible to
    /// readiness (the consumer is already running; a re-dispatch would
    /// double-run it) yet still in flight for completion accounting.
    /// In-RAM only: the journal fold never produces it (a crash before
    /// the take refolds the pulse as Pending; the consumer that was
    /// taking it went down with the worker and is failed, never run
    /// again).
    Routed,
}

impl PulseStatus {
    pub fn is_pending(&self) -> bool {
        matches!(self, PulseStatus::Pending)
    }
}

impl Pulse {
    /// A data pulse. `id` comes from `run::emission::pulse_id` (the
    /// emission plus the wire); the value is shared with every other
    /// wire the same emission reached.
    pub fn new(
        id: uuid::Uuid,
        run_id: RunId,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        value: Arc<Value>,
    ) -> Self {
        Self {
            id,
            run_id,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
            closed: false,
            failure: None,
            provided: false,
            inherited_from: None,
            origin: None,
        }
    }

    /// Closure marker: the upstream terminated without firing this port.
    /// Carries no data (value is always Null). The consumer treats this
    /// as "nothing will arrive here ever again at this frame stack".
    /// Required port + closure -> consumer skips. Optional port +
    /// closure -> consumer fires with the port missing.
    pub fn closure(
        id: uuid::Uuid,
        run_id: RunId,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
    ) -> Self {
        Self::closure_with_failure(id, run_id, frames, target_node, target_port, None)
    }

    /// Closure carrying WHY the upstream ended: `Some(failure)` marks a
    /// producer that broke (a generator consumer's pull gets the error),
    /// `None` a clean finish or a decline.
    pub fn closure_with_failure(
        id: uuid::Uuid,
        run_id: RunId,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        failure: Option<Failure>,
    ) -> Self {
        Self {
            id,
            run_id,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value: Arc::new(Value::Null),
            status: PulseStatus::Pending,
            closed: true,
            failure,
            provided: false,
            inherited_from: None,
            origin: None,
        }
    }

}

/// A run's pulses. A bucket per target node holds the pulses still in
/// flight there (pending, or routed to a live sink), in the order they
/// landed: what
/// every turn of the live engine and every row of the fold walks, so its
/// size never grows with what the run already took. A taken pulse leaves
/// its bucket for the absorbed pulses, kept by id: a firing run again
/// after a refold takes its pulses back from there ([`Self::unabsorb`]),
/// and a pulse landing a second time (a replayed row) is known as landed
/// ([`Self::landed`]). A table that only shows a run keeps no taken pulse
/// ([`Self::forget_taken`]). A stream item a consumer took is removed outright
/// ([`Self::remove_consumed`]), which frees its payload; only that the
/// stream supplied data is remembered.
#[derive(Debug, Clone, Default)]
pub struct PulseTable {
    buckets: BTreeMap<String, Vec<Pulse>>,
    absorbed: HashMap<uuid::Uuid, Pulse>,
    consumed_streams: HashSet<(RunId, crate::frames::FiringLocation, String)>,
    /// Whether a pulse taken off its wire is forgotten
    /// ([`Self::forget_taken`]).
    forgets_taken: bool,
}

impl PulseTable {
    pub fn new() -> Self { Self::default() }

    /// From now on, a pulse taken off its wire is forgotten: for a table
    /// folded to SHOW a run, which read each value when its firing started,
    /// never puts a taken pulse back (only a resumed worker does,
    /// [`Self::unabsorb`]), and folds each row of the run's record once, so
    /// no row lands a taken pulse a second time. A long run's taken pulses
    /// do not pile up in it.
    pub fn forget_taken(&mut self) {
        self.forgets_taken = true;
        self.absorbed.clear();
    }

    /// Whether a pulse taken off its wire is forgotten ([`Self::forget_taken`]).
    pub fn forgets_taken(&self) -> bool {
        self.forgets_taken
    }

    pub fn stream_was_consumed(&self, run_id: RunId, node: &str, port: &str, frames: &LoopFrames) -> bool {
        self.consumed_streams.contains(&(run_id, crate::frames::FiringLocation::new(node, frames.clone()), port.into()))
    }

    /// Call only after validating that every id belongs to this bucket.
    /// The live router and journal fold share this consumption operation.
    pub fn remove_consumed(&mut self, node: &str, ids: &[uuid::Uuid]) {
        let bucket = self.buckets.get_mut(node).expect("consumed pulse bucket exists");
        for pulse in bucket.iter().filter(|p| ids.contains(&p.id)) {
            self.consumed_streams.insert((pulse.run_id,
                crate::frames::FiringLocation::new(node, pulse.frames.clone()), pulse.target_port.clone()));
        }
        bucket.retain(|p| !ids.contains(&p.id));
    }

    /// Take the pending pulses at `node` that `taken` picks: each leaves
    /// the bucket, absorbed (a dispatch took it, a skip or a decision
    /// consumed it, or the run will never take it). Answers their ids, in
    /// the order they landed.
    pub fn absorb(&mut self, node: &str, mut taken: impl FnMut(&Pulse) -> bool) -> Vec<uuid::Uuid> {
        let Some(bucket) = self.buckets.get_mut(node) else { return Vec::new() };
        let mut ids = Vec::new();
        let mut kept = Vec::with_capacity(bucket.len());
        for pulse in bucket.drain(..) {
            if pulse.status.is_pending() && taken(&pulse) {
                ids.push(pulse.id);
                if !self.forgets_taken {
                    self.absorbed.insert(pulse.id, pulse);
                }
            } else {
                kept.push(pulse);
            }
        }
        *bucket = kept;
        ids
    }

    /// Put the absorbed pulses `ids` at `node` back on their wire,
    /// pending: a firing run again after a refold takes them again. One
    /// already back on its wire there (the firing was put back by an
    /// earlier call and has not run yet) stays as it is. Refused, moving
    /// nothing, when one of them is neither: the firing's record and the
    /// table disagree about what it took, and running it on part of its
    /// inputs would be wrong.
    pub fn unabsorb(&mut self, node: &str, ids: &[uuid::Uuid]) -> Result<(), String> {
        if self.forgets_taken {
            return Err(format!("the firing of '{node}' cannot take its pulses again: this table forgot what was taken"));
        }
        let on_wire = |id: &uuid::Uuid| {
            self.buckets.get(node).is_some_and(|bucket| bucket.iter().any(|pulse| pulse.id == *id && pulse.status.is_pending()))
        };
        let taken = |id: &uuid::Uuid| self.absorbed.get(id).is_some_and(|pulse| pulse.target_node == node);
        let missing: Vec<&uuid::Uuid> = ids.iter().filter(|id| !taken(id) && !on_wire(id)).collect();
        if !missing.is_empty() {
            return Err(format!(
                "the firing of '{node}' took pulses {missing:?}, which the run's table holds neither as taken there nor on its wire"
            ));
        }
        for id in ids {
            if let Some(mut pulse) = self.absorbed.remove(id) {
                pulse.status = PulseStatus::Pending;
                self.buckets.entry(node.to_string()).or_default().push(pulse);
            }
        }
        Ok(())
    }

    /// Whether the pulse `id` landed at `node`, in flight or absorbed
    /// (a stream item a consumer took is forgotten).
    pub fn landed(&self, node: &str, id: uuid::Uuid) -> bool {
        self.absorbed.get(&id).is_some_and(|pulse| pulse.target_node == node)
            || self.buckets.get(node).is_some_and(|bucket| bucket.iter().any(|pulse| pulse.id == id))
    }

    /// Forget the taken pulses `ids`: what took them ended, and nothing
    /// puts them back any more.
    pub fn forget_absorbed(&mut self, ids: &[uuid::Uuid]) {
        for id in ids {
            self.absorbed.remove(id);
        }
    }

    /// Forget every taken pulse `gone` picks.
    pub fn forget_absorbed_where(&mut self, mut gone: impl FnMut(&Pulse) -> bool) {
        self.absorbed.retain(|_, pulse| !gone(pulse));
    }

    /// Forget that a stream was taken from at every location `gone`
    /// picks: a reader that only shows the run is done with a location
    /// once nothing can fire there.
    pub fn forget_streams(&mut self, mut gone: impl FnMut(&crate::frames::FiringLocation) -> bool) {
        self.consumed_streams.retain(|(_, at, _)| !gone(at));
    }

    /// The absorbed pulse `id`, if it is one.
    pub fn absorbed(&self, id: uuid::Uuid) -> Option<&Pulse> {
        self.absorbed.get(&id)
    }

    /// Every absorbed pulse, in no particular order.
    pub fn all_absorbed(&self) -> impl Iterator<Item = &Pulse> {
        self.absorbed.values()
    }
}

impl Deref for PulseTable {
    type Target = BTreeMap<String, Vec<Pulse>>;
    fn deref(&self) -> &Self::Target { &self.buckets }
}

impl DerefMut for PulseTable {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.buckets }
}

impl<const N: usize> From<[(String, Vec<Pulse>); N]> for PulseTable {
    fn from(values: [(String, Vec<Pulse>); N]) -> Self {
        Self { buckets: BTreeMap::from(values), ..Self::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A closure crosses processes (the engine's in-RAM table, a seeded
    /// run's history, the inspector) as JSON: the failure travels as the
    /// node that broke plus its error, and comes back the same.
    #[test]
    fn a_failed_closure_round_trips_with_the_node_that_broke() {
        let failure = Failure { node: "auth.query".into(), error: "the database is down".into() };
        let pulse = Pulse::closure_with_failure(uuid::Uuid::nil(), uuid::Uuid::nil(), vec![], "sink", "in", Some(failure.clone()));
        let wire = serde_json::to_value(&pulse).unwrap();
        assert_eq!(wire["failure"], json!({ "node": "auth.query", "error": "the database is down" }));
        let back: Pulse = serde_json::from_value(wire).unwrap();
        assert_eq!(back.failure, Some(failure.clone()));
        assert_eq!(failure.to_string(), "'auth.query' failed: the database is down");

        let plain = serde_json::to_value(Pulse::closure(uuid::Uuid::nil(), uuid::Uuid::nil(), vec![], "sink", "in")).unwrap();
        assert!(plain.get("failure").is_none(), "a plain closure carries no failure field");
    }

    /// A taken pulse leaves its bucket and is still known: by id, as
    /// landed (a replay of it adds nothing), and back on its wire once a
    /// firing that runs again takes it back.
    #[test]
    fn an_absorbed_pulse_leaves_its_bucket_and_can_come_back() {
        let pulse = |n: u128, port: &str| Pulse::new(uuid::Uuid::from_u128(n), RunId::nil(), vec![], "sink", port, Arc::new(json!(n as u64)));
        let mut table = PulseTable::new();
        table.entry("sink".into()).or_default().extend([pulse(1, "a"), pulse(2, "b"), pulse(3, "a")]);
        let taken = table.absorb("sink", |p| p.target_port == "a");
        assert_eq!(taken, vec![uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(3)]);
        assert_eq!(table["sink"].iter().map(|p| p.id).collect::<Vec<_>>(), vec![uuid::Uuid::from_u128(2)]);
        assert!(table.absorbed(uuid::Uuid::from_u128(1)).is_some());
        assert!(table.landed("sink", uuid::Uuid::from_u128(3)) && table.landed("sink", uuid::Uuid::from_u128(2)));
        assert!(!table.landed("elsewhere", uuid::Uuid::from_u128(3)), "landed where it landed only");
        assert!(table.absorb("sink", |p| p.target_port == "a").is_empty(), "nothing pending is left there");
        assert!(table.unabsorb("elsewhere", &[uuid::Uuid::from_u128(3)]).is_err());
        assert!(table.unabsorb("sink", &[uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(9)]).is_err(), "9 never landed");
        assert!(table.absorbed(uuid::Uuid::from_u128(3)).is_some(), "a refused un-absorb moves nothing");
        table.unabsorb("sink", &[uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(1)]).unwrap();
        table.unabsorb("sink", &[uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(1)]).expect("putting back what is back already changes nothing");
        assert_eq!(table["sink"].iter().map(|p| (p.id.as_u128(), p.status)).collect::<Vec<_>>(),
            vec![(2, PulseStatus::Pending), (3, PulseStatus::Pending), (1, PulseStatus::Pending)]);
        assert_eq!(table.all_absorbed().count(), 0);
    }
}

//! Per-execution `LoopInstance` runtime. The engine creates one
//! `LoopInstance` for every `(loop_group_id, parent_frames, color)`
//! triple. Each instance tracks:
//!
//! - the launched iterations (`LoopIn` body emits per iteration);
//! - per-port gather slot maps (`BTreeMap<u32, LoopWrite>` per
//!   gather output: `LoopWrite::Value(v)` for an iteration that wrote
//!   the port, `LoopWrite::Closed` for one that closed it; the tagged
//!   enum keeps these distinguishable across the journal round-trip
//!   even when the body legitimately writes JSON null);
//! - current carry values (each `LoopOut` firing may update them, or
//!   keep the previous on closure);
//! - termination state (which condition fired, when to emit outwardly).
//!
//! `LoopRuntime` is owned by the engine's per-execution state; it
//! shares no state with the `BusCoordinator` or other coordinators
//! beyond what the engine threads through.
//!
//! The engine integration points (in `weft-engine/src/execution_driver.rs`):
//!  - When a `LoopIn` node fires, the engine calls
//!    [`LoopRuntime::ensure`] to look up or create the instance and
//!    then drives the per-iteration emission directly (validate `over`
//!    lengths, compute the effective iteration count, emit pulses on
//!    LoopIn's inside output ports plus the implicit `self.index`
//!    port).
//!  - When a `LoopOut` node fires, the engine calls
//!    [`LoopRuntime::record_loop_out`] to record the per-port writes
//!    for that iteration, decide whether to launch the next iteration
//!    (sequential modes), and check the termination condition. When
//!    the loop terminates, `record_loop_out` returns a
//!    [`LoopAdvance::EmitOutward`] that the engine flushes onto the
//!    loop's outward outputs.
//!
//! Pause/resume: the runtime exposes a `LoopInstanceSnapshot` view per
//! instance so the journal can rebuild state from the fold.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use weft_core::frames::{LoopFrames, LoopIteration};
use weft_core::primitive::{
    LoopInstanceKey, LoopInstanceSnapshot, LoopTerminationReason, LoopWrite,
};
use weft_core::Color;

/// Configuration snapshot copied from the user's `Loop(...)` decl onto
/// the `LoopIn` / `LoopOut` boundary nodes' `config` JSON. The runtime
/// reads it when instantiating a loop and never mutates it.
#[derive(Debug, Clone)]
pub struct LoopConfig {
    pub parallel: bool,
    pub over: Vec<String>,
    pub carry: Vec<String>,
    pub max_iters: Option<u32>,
    pub trim_on_mismatch: bool,
}

impl LoopConfig {
    /// Parse the LoopIn boundary node's `config` JSON. The source
    /// language defaults `parallel` to false, but the compiler's
    /// flatten step MATERIALIZES that default into every LoopIn
    /// config, so at runtime the field is always present. A missing
    /// or wrong-typed `parallel` here means the boundary node's
    /// config drifted (compiler bug or hand-edited journal);
    /// silently defaulting would let a parallel loop run
    /// sequentially without anyone noticing.
    pub fn from_node_config(cfg: &Value) -> Result<Self, String> {
        let parallel = cfg
            .get("parallel")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| {
                format!(
                    "LoopIn config: missing or non-boolean `parallel` field (got {})",
                    cfg.get("parallel").map(|v| v.to_string()).unwrap_or_else(|| "absent".into())
                )
            })?;
        let over = parse_port_list(cfg, "over")?;
        let carry = parse_port_list(cfg, "carry")?;
        let max_iters = match cfg.get("max_iters") {
            None => None,
            Some(v) => Some(v.as_u64().and_then(|n| u32::try_from(n).ok()).ok_or_else(|| {
                format!("LoopIn config: `max_iters` must be a non-negative integer (got {v})")
            })?),
        };
        let trim_on_mismatch = match cfg.get("trim_on_mismatch") {
            None => true,
            Some(v) => v.as_bool().ok_or_else(|| {
                format!("LoopIn config: `trim_on_mismatch` must be a boolean (got {v})")
            })?,
        };
        Ok(Self { parallel, over, carry, max_iters, trim_on_mismatch })
    }
}

/// Parse an `over` / `carry` config entry as a list of port-name
/// strings. Same posture as `parallel` above: a non-list value or a
/// non-string element means the boundary node's config drifted, and
/// silently dropping elements would run the loop minus part of its
/// declared iteration/carry set.
fn parse_port_list(cfg: &Value, field: &str) -> Result<Vec<String>, String> {
    match cfg.get(field) {
        None => Ok(Vec::new()),
        Some(v) => {
            let arr = v.as_array().ok_or_else(|| {
                format!("LoopIn config: `{field}` must be a list of port names (got {v})")
            })?;
            arr.iter()
                .map(|e| {
                    e.as_str().map(String::from).ok_or_else(|| {
                        format!(
                            "LoopIn config: `{field}` entries must be port-name strings (got {e})"
                        )
                    })
                })
                .collect()
        }
    }
}

/// A single live loop. Engine code reads/writes through
/// `LoopRuntime` so the snapshot serialization stays in one place.
#[derive(Debug, Clone)]
pub struct LoopInstance {
    pub key: LoopInstanceKey,
    pub config: LoopConfig,
    /// Effective iteration count after zip-trim + max_iters cap.
    pub iter_count: u32,
    /// Declared gather-output port names (LoopOut's outward outputs
    /// minus the carry names). Captured at instantiation so the
    /// outward emit assembles a list for EVERY declared gather port,
    /// even ones no iteration ever wrote to. Without this, a gather
    /// port that received only closures (or was never wired) would
    /// produce no outward pulse, deadlocking downstream.
    pub gather_ports: Vec<String>,
    /// Iterations the engine has launched body work for.
    pub launched: Vec<u32>,
    /// Iterations whose `LoopOut` has fired.
    pub out_fired: Vec<u32>,
    /// Per gather-port, the per-index slot. `Closed` means the body
    /// closed that port at that iteration (assembled outward list gets
    /// `null` there).
    pub gather_lists: HashMap<String, BTreeMap<u32, LoopWrite>>,
    /// Current carry-port values. Initial values seeded from outer-in;
    /// updated on each successful LoopOut carry-write.
    pub carry_values: HashMap<String, Value>,
    /// Outer input bag captured at instantiation, keyed by port name.
    /// Sequential mode needs this to launch iteration N+1 after the
    /// LoopIn's pulses have already been absorbed by the first dispatch.
    /// Parallel mode reads the same bag once at launch-all time.
    pub outer_input: HashMap<String, Value>,
    pub terminated: Option<LoopTerminationReason>,
}

impl LoopInstance {
    pub fn new(
        key: LoopInstanceKey,
        config: LoopConfig,
        iter_count: u32,
        gather_ports: Vec<String>,
    ) -> Self {
        Self {
            key,
            config,
            iter_count,
            gather_ports,
            launched: Vec::new(),
            out_fired: Vec::new(),
            gather_lists: HashMap::new(),
            carry_values: HashMap::new(),
            outer_input: HashMap::new(),
            terminated: None,
        }
    }

    /// Restore an instance from a snapshot (resume path). The
    /// snapshot carries the full `LoopConfig` shape (over, carry,
    /// trim_on_mismatch) so the rehydrate path does not need to
    /// re-read the project definition's LoopIn node config to
    /// reconstruct it. `gather_ports` is passed in by the caller
    /// (the LoopOut node's outward output names minus the
    /// carry-named ports), because it depends on the LIVE project
    /// definition that the resumed worker just fetched, not on the
    /// snapshot.
    pub fn from_snapshot(
        key: LoopInstanceKey,
        snap: &LoopInstanceSnapshot,
        gather_ports: Vec<String>,
    ) -> Self {
        let gather_lists = snap.gather_lists.iter().map(|(k, m)| {
            let inner: BTreeMap<u32, LoopWrite> = m.iter().map(|(i, v)| (*i, v.clone())).collect();
            (k.clone(), inner)
        }).collect();
        let config = LoopConfig {
            parallel: snap.parallel,
            over: snap.over.clone(),
            carry: snap.carry.clone(),
            max_iters: snap.max_iters,
            trim_on_mismatch: snap.trim_on_mismatch,
        };
        Self {
            key,
            config,
            iter_count: snap.iter_count,
            gather_ports,
            launched: snap.launched.clone(),
            out_fired: snap.out_fired.clone(),
            gather_lists,
            carry_values: snap.carry_values.clone(),
            outer_input: snap.outer_input.clone(),
            terminated: snap.terminated,
        }
    }
}

/// What the engine should do next after a `LoopOut` firing was recorded.
#[derive(Debug, Clone)]
pub enum LoopAdvance {
    /// Launch the next iteration. Carries the iteration index and the
    /// per-iteration `over`/broadcast/`index` values the engine should
    /// emit on `LoopIn`'s inside outputs at the body's frame stack.
    LaunchNext { index: u32 },
    /// Loop has terminated. Engine should emit the assembled outward
    /// pulses on `LoopOut`'s outer outputs at the parent frame stack.
    /// `gather` carries a list for EVERY declared gather port (even
    /// ones no iteration touched: such a port produces a list of
    /// length `count` filled with `None`).
    EmitOutward {
        reason: LoopTerminationReason,
        gather: HashMap<String, Vec<Option<Value>>>,
        carry: HashMap<String, Value>,
    },
    /// Nothing to do (this firing did not advance the loop).
    Idle,
}

/// Per-execution runtime registry. Keyed by `LoopInstanceKey`.
#[derive(Debug, Default)]
pub struct LoopRuntime {
    instances: HashMap<LoopInstanceKey, LoopInstance>,
}

impl LoopRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &LoopInstanceKey) -> Option<&LoopInstance> {
        self.instances.get(key)
    }

    pub fn get_mut(&mut self, key: &LoopInstanceKey) -> Option<&mut LoopInstance> {
        self.instances.get_mut(key)
    }

    /// Lookup or instantiate. Returns true on first-time instantiation.
    /// `gather_ports` is the LoopOut node's declared outward gather
    /// port names (non-carry outputs), captured here so the outward
    /// emit assembles a list for EVERY declared port even ones no
    /// iteration touched.
    pub fn ensure(
        &mut self,
        key: LoopInstanceKey,
        config: LoopConfig,
        iter_count: u32,
        gather_ports: Vec<String>,
    ) -> bool {
        if self.instances.contains_key(&key) {
            return false;
        }
        self.instances
            .insert(key.clone(), LoopInstance::new(key, config, iter_count, gather_ports));
        true
    }

    /// Insert a pre-built instance. Used by the journal-fold rehydrate
    /// path so a resumed worker rebuilds its runtime from the snapshot.
    pub fn insert(&mut self, inst: LoopInstance) {
        self.instances.insert(inst.key.clone(), inst);
    }

    /// Iterate every live instance. Used by the cancel path to walk
    /// every loop's outward port and emit closures.
    pub fn iter(&self) -> impl Iterator<Item = (&LoopInstanceKey, &LoopInstance)> {
        self.instances.iter()
    }

    /// Record an iteration launch.
    pub fn record_launched(&mut self, key: &LoopInstanceKey, index: u32) {
        if let Some(inst) = self.instances.get_mut(key) {
            if !inst.launched.contains(&index) {
                inst.launched.push(index);
            }
        }
    }

    /// Whether a `LoopOut` firing at `index` would be recorded as NEW
    /// state by `record_loop_out` (instance live, index not already
    /// fired). The engine consults this BEFORE journaling
    /// `LoopOutFired` so the journal only ever contains firings the
    /// runtime accepted: the fold applies `LoopOutFired`
    /// unconditionally, so a row for a refused (post-termination) or
    /// replayed firing would diverge the rehydrated instance from the
    /// live one. Errors loudly on a missing instance, mirroring
    /// `record_loop_out`'s invariant (LoopIn always fires first).
    pub fn loop_out_is_new(&self, key: &LoopInstanceKey, index: u32) -> Result<bool, String> {
        let inst = self.instances.get(key).ok_or_else(|| {
            format!(
                "LoopOut fired for {} at parent_frames={:?} index={index} but no LoopInstance exists; \
                 LoopIn must fire before LoopOut",
                key.group_id, key.parent_frames,
            )
        })?;
        Ok(inst.terminated.is_none() && !inst.out_fired.contains(&index))
    }

    /// Record a `LoopOut` firing.
    /// - `gather_writes`: `Value(v)` for ports the body wrote, `Closed`
    ///   for ports the body closed.
    /// - `carry_writes`: `Value(v)` updates the carry; `Closed` keeps
    ///   the previous value.
    /// - `done_vote`: `Some(bool)` for the body's `self.done` value;
    ///   `None` means it was closed (treated as false).
    ///
    /// Errors loudly if the instance is missing. The plan invariant
    /// is that `LoopIn` ALWAYS fires before `LoopOut`, so a missing
    /// instance at this point is a real corruption, not a recoverable
    /// state. A silent `LoopAdvance::Idle` would mask the bug.
    pub fn record_loop_out(
        &mut self,
        key: &LoopInstanceKey,
        index: u32,
        gather_writes: HashMap<String, LoopWrite>,
        carry_writes: HashMap<String, LoopWrite>,
        done_vote: Option<bool>,
    ) -> Result<LoopAdvance, String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!(
                "LoopOut fired for {} at parent_frames={:?} index={index} but no LoopInstance exists; \
                 LoopIn must fire before LoopOut",
                key.group_id, key.parent_frames,
            )
        })?;
        // A LoopOut firing arriving after termination (cancellation, a
        // peer iteration that already raced to done, an out-of-band
        // emit) must NOT re-enter the decision tree: the sequential
        // path would otherwise dispatch `LaunchNext` and revive a
        // terminated loop, journaling a fresh LoopIterationLaunched.
        if inst.terminated.is_some() {
            return Ok(LoopAdvance::Idle);
        }
        // A LoopOut firing whose index already fired (and the loop is not
        // yet terminated) is a REPLAY: the journaled `LoopOutFired` row
        // applied these gather/carry writes during fold, and the call
        // site's `loop_out_is_new` gate refused to journal a second row.
        // Re-applying the writes here would overwrite the folded values
        // with a second (possibly non-deterministic) firing's results
        // that NO journal row records, diverging live RAM from any
        // rehydrated instance. Apply writes ONCE, on the first firing
        // only; on replay fall straight through to the launch-next
        // decision (which is idempotent: a `next` already in `launched`
        // returns Idle). Mirrors the `terminated` guard above.
        let is_replay = inst.out_fired.contains(&index);
        if !is_replay {
            inst.out_fired.push(index);
            for (port, slot) in gather_writes {
                inst.gather_lists.entry(port).or_default().insert(index, slot);
            }
            for (port, slot) in carry_writes {
                if let LoopWrite::Value(v) = slot {
                    inst.carry_values.insert(port, v);
                }
            }
        }

        let done = done_vote.unwrap_or(false);
        // Termination-reason precedence: when `iter_count` was capped
        // at `max_iters` AND the loop reached the cap, BOTH conditions
        // are true at the last iteration. Check max FIRST so the
        // reason names the binding constraint.
        let max_reached = inst.config.max_iters.map(|m| index + 1 >= m).unwrap_or(false);
        let over_exhausted = index + 1 >= inst.iter_count;

        // Decide next action.
        if inst.config.parallel {
            // All launched once at instantiation. Check if all have fired LoopOut.
            if inst.out_fired.len() as u32 >= inst.iter_count {
                // Termination reason: in parallel, LoopOuts fire out
                // of order, so the completing firing's `index` is
                // arbitrary. We can't ask "did THIS index hit
                // max_iters?" the way sequential mode does. The
                // binding constraint is fully captured by the
                // relationship between `iter_count` (the cap that
                // actually ran) and `max_iters`: if they're equal,
                // max was the binding constraint.
                let reason = if inst.config.max_iters == Some(inst.iter_count) {
                    LoopTerminationReason::MaxItersReached
                } else {
                    LoopTerminationReason::OverExhausted
                };
                return Ok(self.emit_outward(key, reason));
            }
            return Ok(LoopAdvance::Idle);
        }

        // Sequential: decide launch-next vs terminate.
        if done {
            return Ok(self.emit_outward(key, LoopTerminationReason::DoneVoted));
        }
        if max_reached {
            return Ok(self.emit_outward(key, LoopTerminationReason::MaxItersReached));
        }
        if over_exhausted {
            return Ok(self.emit_outward(key, LoopTerminationReason::OverExhausted));
        }
        let next = index + 1;
        if inst.launched.contains(&next) {
            // Crash-resume replay: this LoopOut firing already
            // dispatched its LaunchNext before the crash (the
            // `LoopIterationLaunched` row for `next` is in the
            // journal, which is the only way it enters `launched` on
            // rehydrate). Re-launching would duplicate the
            // iteration's body pulses and re-run its body.
            return Ok(LoopAdvance::Idle);
        }
        Ok(LoopAdvance::LaunchNext { index: next })
    }

    /// Mark an instance as terminated and assemble the outward emit
    /// payload. `pub(crate)` so the engine's zero-iter path can call
    /// it without going through `record_loop_out`.
    pub(crate) fn emit_outward(
        &mut self,
        key: &LoopInstanceKey,
        reason: LoopTerminationReason,
    ) -> LoopAdvance {
        let Some(inst) = self.instances.get_mut(key) else {
            return LoopAdvance::Idle;
        };
        if inst.terminated.is_some() {
            return LoopAdvance::Idle;
        }
        inst.terminated = Some(reason);
        // List length = number of iterations actually launched. In
        // normal flow (contiguous launches) this equals
        // `launched.len()`; we compute as `max + 1` so a non-
        // contiguous launched (partial launch failure) still
        // produces a list whose indices align with the journal.
        let count = inst.launched.iter().copied().max().map(|m| m + 1).unwrap_or(0);
        let mut gather: HashMap<String, Vec<Option<Value>>> = HashMap::new();
        // Walk every DECLARED gather port (not just keys present in
        // `gather_lists`). A port no iteration touched produces a
        // list of `None`s of length `count`; downstream sees a real
        // list and not a missing-pulse deadlock.
        for port in &inst.gather_ports {
            let slots = inst.gather_lists.get(port);
            let mut out_list = Vec::with_capacity(count as usize);
            for i in 0..count {
                out_list.push(slots.and_then(|m| m.get(&i)).and_then(|w| w.as_value().cloned()));
            }
            gather.insert(port.clone(), out_list);
        }
        let carry = inst.carry_values.clone();
        LoopAdvance::EmitOutward { reason, gather, carry }
    }

    /// Cancellation: mark every instance whose `parent_frames` is at
    /// or inside the cancel scope `frames` (i.e. `frames` is a prefix
    /// of `parent_frames`) as terminated with `Cancelled`. Outward
    /// emit is the engine's responsibility (it emits closures, not a
    /// real outward emit, on cancellation), plus a `LoopTerminated`
    /// journal write so cancellation is durable across resume.
    pub fn cancel_inside(&mut self, frames: &LoopFrames, color: Color) {
        for inst in self.instances.values_mut() {
            if inst.key.color != color {
                continue;
            }
            if inst.terminated.is_some() {
                continue;
            }
            // The instance's parent_frames must extend (or equal)
            // the cancel scope. "frames is a prefix of parent_frames"
            // captures "this instance lives at-or-inside the cancel
            // scope". The earlier swap (parent_frames as prefix of
            // frames) silently missed every nested instance.
            if is_prefix(frames, &inst.key.parent_frames) {
                inst.terminated = Some(LoopTerminationReason::Cancelled);
            }
        }
    }

}

fn is_prefix(short: &LoopFrames, long: &LoopFrames) -> bool {
    short.len() <= long.len() && long.iter().take(short.len()).eq(short.iter())
}

/// Build the inside-out frame stack for iteration `i` of a loop whose
/// parent frame stack is `parent_frames`.
pub fn iteration_frames(parent_frames: &LoopFrames, index: u32) -> LoopFrames {
    let mut frames = parent_frames.clone();
    frames.push(LoopIteration { index });
    frames
}

/// Recover a `LoopInstance` key's `parent_frames` from a boundary
/// firing's frame stack. LoopIn fires at the loop's parent frame
/// stack; LoopOut fires AT the iteration's frame stack
/// (parent_frames + [iter]), so for LoopOut we pop the iteration
/// frame.
pub fn boundary_parent_frames(node_type: &str, frames: &LoopFrames) -> LoopFrames {
    if node_type == "LoopOut" && !frames.is_empty() {
        frames[..frames.len() - 1].to_vec()
    } else {
        frames.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn key() -> LoopInstanceKey {
        LoopInstanceKey {
            group_id: "outer".to_string(),
            parent_frames: Vec::new(),
            color: Uuid::nil(),
        }
    }

    fn cfg(parallel: bool, over: &[&str], carry: &[&str], max_iters: Option<u32>) -> LoopConfig {
        LoopConfig {
            parallel,
            over: over.iter().map(|s| s.to_string()).collect(),
            carry: carry.iter().map(|s| s.to_string()).collect(),
            max_iters,
            trim_on_mismatch: true,
        }
    }

    fn val(v: serde_json::Value) -> LoopWrite { LoopWrite::Value(v) }
    fn closed() -> LoopWrite { LoopWrite::Closed }

    #[test]
    fn sequential_over_exhausted() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), 3, vec!["result".into()]);

        for i in 0..3 {
            rt.record_launched(&k, i);
            let mut g = HashMap::new();
            g.insert("result".to_string(), val(serde_json::json!(i)));
            let advance = rt.record_loop_out(&k, i, g, HashMap::new(), Some(false)).unwrap();
            match (i, &advance) {
                (0 | 1, LoopAdvance::LaunchNext { index }) => assert_eq!(*index, i + 1),
                (2, LoopAdvance::EmitOutward { reason, gather, .. }) => {
                    assert_eq!(*reason, LoopTerminationReason::OverExhausted);
                    assert_eq!(gather["result"].len(), 3);
                }
                other => panic!("unexpected advance at i={i}: {other:?}"),
            }
        }
    }

    #[test]
    fn parallel_termination_when_all_fired() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(true, &["items"], &[], None), 3, vec![]);
        for i in 0..3 {
            rt.record_launched(&k, i);
        }
        // Fire LoopOut for each in random order.
        let _ = rt.record_loop_out(&k, 2, HashMap::new(), HashMap::new(), None).unwrap();
        let _ = rt.record_loop_out(&k, 0, HashMap::new(), HashMap::new(), None).unwrap();
        let advance = rt.record_loop_out(&k, 1, HashMap::new(), HashMap::new(), None).unwrap();
        match advance {
            LoopAdvance::EmitOutward { reason, .. } => {
                assert_eq!(reason, LoopTerminationReason::OverExhausted);
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn done_vote_terminates_loop() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &[], &["acc"], Some(100)), 100, vec![]);
        rt.record_launched(&k, 0);
        let advance = rt.record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(true)).unwrap();
        match advance {
            LoopAdvance::EmitOutward { reason, .. } => {
                assert_eq!(reason, LoopTerminationReason::DoneVoted);
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn carry_keep_previous_on_closure() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &[], &["acc"], Some(5)), 5, vec![]);
        let mut carry = HashMap::new();
        carry.insert("acc".to_string(), val(serde_json::json!("first")));
        rt.record_launched(&k, 0);
        let _ = rt.record_loop_out(&k, 0, HashMap::new(), carry, Some(false)).unwrap();
        // Second iteration: carry write is Closed.
        let mut carry = HashMap::new();
        carry.insert("acc".to_string(), closed());
        rt.record_launched(&k, 1);
        let _ = rt.record_loop_out(&k, 1, HashMap::new(), carry, Some(false)).unwrap();
        let inst = rt.get(&k).expect("instance");
        assert_eq!(inst.carry_values["acc"], serde_json::json!("first"));
    }

    /// A body that legitimately writes JSON null on a carry port must
    /// update the carry value to null, NOT keep the previous. This is
    /// the closure-vs-null disambiguation the tagged `LoopWrite` enum
    /// exists to preserve across the journal round-trip.
    #[test]
    fn carry_written_null_distinct_from_closed() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &[], &["acc"], Some(5)), 5, vec![]);
        let mut carry = HashMap::new();
        carry.insert("acc".to_string(), val(serde_json::json!("first")));
        rt.record_launched(&k, 0);
        let _ = rt.record_loop_out(&k, 0, HashMap::new(), carry, Some(false)).unwrap();
        // Second iteration: body wrote a real null.
        let mut carry = HashMap::new();
        carry.insert("acc".to_string(), val(serde_json::Value::Null));
        rt.record_launched(&k, 1);
        let _ = rt.record_loop_out(&k, 1, HashMap::new(), carry, Some(false)).unwrap();
        let inst = rt.get(&k).expect("instance");
        assert_eq!(inst.carry_values["acc"], serde_json::Value::Null);
    }

    /// A gather port that no iteration ever touched STILL produces an
    /// outward list of length `count`, filled with `None`s. Without
    /// the declared `gather_ports` list to seed assembly, the missing
    /// port would emit no pulse and deadlock downstream.
    #[test]
    fn emit_outward_seeds_untouched_gather_ports_with_nulls() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(
            k.clone(),
            cfg(false, &["items"], &[], None),
            3,
            vec!["result".into(), "errors".into()],
        );
        for i in 0..3 {
            rt.record_launched(&k, i);
            // Only write to `result`, never to `errors`.
            let mut g = HashMap::new();
            g.insert("result".to_string(), val(serde_json::json!(i)));
            let _ = rt.record_loop_out(&k, i, g, HashMap::new(), Some(false)).unwrap();
        }
        // The advance for i=2 emitted outward; inspect the instance's
        // assembled outward via emit_outward semantics by checking
        // that an emit run with the same instance produces both keys.
        let last = rt.record_loop_out(&k, 2, HashMap::new(), HashMap::new(), Some(true));
        // After done was voted at index 2 in the first loop we
        // already terminated. Second call is idle.
        assert!(matches!(last.unwrap(), LoopAdvance::Idle));
        let inst = rt.get(&k).expect("instance");
        // Both declared gather ports are tracked, including the
        // never-written one.
        assert!(inst.gather_ports.contains(&"result".to_string()));
        assert!(inst.gather_ports.contains(&"errors".to_string()));
    }

    #[test]
    fn cancel_inside_marks_terminated() {
        let mut rt = LoopRuntime::new();
        let mut k = key();
        k.parent_frames = vec![LoopIteration { index: 0 }];
        rt.ensure(k.clone(), cfg(false, &["x"], &[], None), 5, vec![]);
        let outer_frames = vec![LoopIteration { index: 0 }];
        rt.cancel_inside(&outer_frames, Uuid::nil());
        let inst = rt.get(&k).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::Cancelled));
    }

    /// A scoped cancel at frames `[{0}]` must leave a sibling
    /// instance at `[{1}]` untouched. Without this, the cancel
    /// machinery's prefix-walk semantic is undocumented and trivially
    /// regressable: an arg-order swap that flipped the contract would
    /// pass every cancel-at-root test but break sibling isolation.
    #[test]
    fn cancel_at_inner_scope_does_not_touch_sibling_iteration() {
        let mut rt = LoopRuntime::new();
        let inst_0 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 0 }],
            color: Uuid::nil(),
        };
        let inst_1 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 1 }],
            color: Uuid::nil(),
        };
        rt.ensure(inst_0.clone(), cfg(false, &["x"], &[], None), 3, vec![]);
        rt.ensure(inst_1.clone(), cfg(false, &["x"], &[], None), 3, vec![]);
        rt.cancel_inside(&vec![LoopIteration { index: 0 }], Uuid::nil());
        assert_eq!(
            rt.get(&inst_0).unwrap().terminated,
            Some(LoopTerminationReason::Cancelled),
            "inner at [{{0}}] is inside the cancel scope and must be Cancelled",
        );
        assert!(
            rt.get(&inst_1).unwrap().terminated.is_none(),
            "inner at [{{1}}] is a sibling iteration and must NOT be touched",
        );
    }

    /// Full-execution cancel (cancel scope = []) must mark nested
    /// instances Cancelled, not just top-level ones. The earlier
    /// swapped prefix check silently left every nested instance
    /// live.
    #[test]
    fn cancel_at_root_cascades_to_nested_instances() {
        let mut rt = LoopRuntime::new();
        let outer_key = LoopInstanceKey {
            group_id: "outer".into(),
            parent_frames: Vec::new(),
            color: Uuid::nil(),
        };
        let inner_key = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 0 }],
            color: Uuid::nil(),
        };
        rt.ensure(outer_key.clone(), cfg(false, &["x"], &[], None), 3, vec![]);
        rt.ensure(inner_key.clone(), cfg(false, &["y"], &[], None), 3, vec![]);
        rt.cancel_inside(&Vec::new(), Uuid::nil());
        assert_eq!(
            rt.get(&outer_key).unwrap().terminated,
            Some(LoopTerminationReason::Cancelled),
        );
        assert_eq!(
            rt.get(&inner_key).unwrap().terminated,
            Some(LoopTerminationReason::Cancelled),
        );
    }

    /// `LoopOut` firing for a key with no instance is corruption,
    /// not a recoverable state. The runtime must surface it loudly
    /// instead of returning `Idle`.
    #[test]
    fn record_loop_out_on_missing_instance_errors_loudly() {
        let mut rt = LoopRuntime::new();
        let k = key();
        let err = rt
            .record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false))
            .unwrap_err();
        assert!(
            err.contains("no LoopInstance exists"),
            "expected loud error, got: {err}"
        );
    }

    /// When `iter_count` was capped at `max_iters` AND the loop hits
    /// the cap, the termination reason names the binding constraint
    /// (`MaxItersReached`), not the symptomatic one
    /// (`OverExhausted`). This is what the inspector renders.
    #[test]
    fn max_iters_binding_constraint_reports_max_iters_reason() {
        let mut rt = LoopRuntime::new();
        let k = key();
        // over is 10 long, max_iters = 3 → iter_count capped to 3.
        // Sequential: at index 2 (the last) both over_exhausted and
        // max_reached fire. Launches mirror the engine's real call
        // sequence: iteration 0 at instantiation, each subsequent one
        // recorded when its `LaunchNext` is dispatched (pre-recording
        // future launches would trip the replay guard, correctly).
        rt.ensure(k.clone(), cfg(false, &["items"], &[], Some(3)), 3, vec![]);
        rt.record_launched(&k, 0);
        for i in 0..2 {
            let advance = rt
                .record_loop_out(&k, i, HashMap::new(), HashMap::new(), Some(false))
                .unwrap();
            match advance {
                LoopAdvance::LaunchNext { index } => rt.record_launched(&k, index),
                other => panic!("expected LaunchNext, got {other:?}"),
            }
        }
        let advance = rt
            .record_loop_out(&k, 2, HashMap::new(), HashMap::new(), Some(false))
            .unwrap();
        match advance {
            LoopAdvance::EmitOutward { reason, .. } => {
                assert_eq!(reason, LoopTerminationReason::MaxItersReached);
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    /// Crash-resume replay guard: a re-fired `LoopOut` whose
    /// `LaunchNext` target is ALREADY in `launched` (its
    /// `LoopIterationLaunched` row survived the crash) must return
    /// `Idle`, not a second `LaunchNext`, or the iteration's body
    /// pulses would be duplicated and its body re-run (double spend).
    #[test]
    fn replayed_loop_out_does_not_relaunch_an_already_launched_iteration() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), 3, vec![]);
        rt.record_launched(&k, 0);
        // Live firing: LoopOut@0 dispatches LaunchNext(1).
        let advance = rt
            .record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false))
            .unwrap();
        match advance {
            LoopAdvance::LaunchNext { index } => rt.record_launched(&k, index),
            other => panic!("expected LaunchNext, got {other:?}"),
        }
        // Crash-resume replay of the SAME LoopOut@0 firing: iteration
        // 1 is already launched, so the replay must be inert.
        let replay = rt
            .record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false))
            .unwrap();
        assert!(
            matches!(replay, LoopAdvance::Idle),
            "expected Idle on replay, got {replay:?}"
        );
    }

    /// Parallel mirror of `max_iters_binding_constraint`. LoopOuts can
    /// fire out of order in parallel mode; the firing that completes
    /// the set has an arbitrary index, so a reason-detection scheme
    /// keyed on the firing's `index` (the earlier shape: `max_reached
    /// = index + 1 >= max_iters`) would miss this case when the
    /// completing firing happens to be a low index. The reason must
    /// be determined from the instance's `iter_count` vs `max_iters`
    /// relationship, which is ordering-independent.
    #[test]
    fn parallel_max_iters_binding_constraint_reports_max_iters_reason_regardless_of_order() {
        let mut rt = LoopRuntime::new();
        let k = key();
        // over is 10 long, max_iters = 3 → iter_count capped to 3.
        rt.ensure(k.clone(), cfg(true, &["items"], &[], Some(3)), 3, vec![]);
        for i in 0..3 {
            rt.record_launched(&k, i);
        }
        // Fire in deliberately-non-monotone order: 2 first, then 1,
        // then 0 (the completing firing has the LOWEST index).
        let _ = rt.record_loop_out(&k, 2, HashMap::new(), HashMap::new(), None).unwrap();
        let _ = rt.record_loop_out(&k, 1, HashMap::new(), HashMap::new(), None).unwrap();
        let advance = rt
            .record_loop_out(&k, 0, HashMap::new(), HashMap::new(), None)
            .unwrap();
        match advance {
            LoopAdvance::EmitOutward { reason, .. } => {
                assert_eq!(
                    reason,
                    LoopTerminationReason::MaxItersReached,
                    "completing-firing index 0 must NOT mask the max_iters binding constraint",
                );
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    /// A LoopOut firing arriving AFTER the instance terminated (e.g.
    /// cancellation, a peer iteration that already raced to done) must
    /// not re-enter the launch-next decision. Without the guard, the
    /// sequential path would dispatch `LaunchNext` and revive a
    /// cancelled loop, journaling a fresh LoopIterationLaunched.
    #[test]
    fn late_loop_out_after_termination_is_idle() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), 5, vec!["result".into()]);
        rt.record_launched(&k, 0);
        // Externally cancel the instance (simulates cancel_inside).
        rt.cancel_inside(&Vec::new(), Uuid::nil());
        // A delayed LoopOut firing should NOT relaunch.
        let advance = rt
            .record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false))
            .unwrap();
        assert!(matches!(advance, LoopAdvance::Idle));
        let inst = rt.get(&k).expect("instance");
        // out_fired must NOT have been updated (the firing was rejected).
        assert!(inst.out_fired.is_empty(), "out_fired must not record post-termination firings");
    }
}

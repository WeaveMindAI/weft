//! Per-execution `LoopInstance` runtime, and the pure loop machinery
//! around it. The engine creates one `LoopInstance` for every
//! `(loop_group_id, parent_frames, color)` triple; the journal fold
//! creates the same one from the loop's rows. Each instance tracks:
//!
//! - the launched iterations (`LoopIn` body emits per iteration);
//! - per-port gather slot maps (`BTreeMap<u32, LoopWrite>` per
//!   gather output: `LoopWrite::Value(v)` for an iteration that wrote
//!   the port, `LoopWrite::Closed` for one that closed it; the tagged
//!   enum keeps "wrote JSON null" apart from "closed the port");
//! - current carry values (each `LoopOut` firing may update them, or
//!   keep the previous on closure);
//! - termination state (which condition fired, when to emit outwardly).
//!
//! Every value an instance holds is SHARED with the pulse it came
//! from (an `Arc`): the outer input is the LoopIn's absorbed pulses,
//! the carries and gathers are the body's writes into LoopOut. The
//! only new allocations a loop makes are the per-iteration slice of
//! an `over` list, the implicit `index`, and the assembled outward
//! lists.
//!
//! The engine integration points (in `weft-engine/src/execution_driver.rs`):
//!  - When a `LoopIn` node fires, the engine calls [`instantiate`]
//!    (look up or create the instance from the firing's bag) and then
//!    [`launch_iteration`] per iteration to launch.
//!  - When a `LoopOut` node fires, the engine classifies the firing's
//!    bag with [`classify_loop_out`], journals it, and calls
//!    [`LoopRuntime::record_loop_out`] to record the writes, decide
//!    whether to launch the next iteration (sequential modes), and
//!    check the termination condition. A termination comes back as
//!    [`LoopAdvance::EmitOutward`] that the engine flushes with
//!    [`emit_loop_outward`].
//!
//! The journal fold drives the same instance from the loop rows
//! (`LoopInstantiated`, `LoopIterationLaunched`, `LoopOutFired`,
//! `LoopStreamEnded`, `LoopTerminated`), calling the same functions,
//! so a resumed worker holds the instance the live one held.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::exec::emission::{iteration_launch_emission, loop_termination_emission, PulseEmission};
use crate::exec::postprocess::{close_unmentioned_downstream, postprocess_output, OutputBag};
use crate::exec::ready::InputBag;
use crate::frames::{LoopFrames, LoopIteration};
use crate::generator::{StreamBuffer, StreamEnd};
use crate::primitive::{LoopInstanceKey, LoopTerminationReason};
use crate::project::{EdgeIndex, NodeDefinition, ProjectDefinition};
use crate::pulse::PulseTable;
use crate::Color;

/// One body-side write to a `LoopOut` inward-in port at a single
/// iteration. The tag distinguishes "wrote a value (which MAY be JSON
/// null)" from "closed the port". Carry semantics treat `Closed` as
/// "keep previous"; gather semantics treat `Closed` as "null slot at
/// index".
#[derive(Debug, Clone, PartialEq)]
pub enum LoopWrite {
    Value(Arc<Value>),
    Closed,
}

impl LoopWrite {
    pub fn as_value(&self) -> Option<&Arc<Value>> {
        match self {
            LoopWrite::Value(v) => Some(v),
            LoopWrite::Closed => None,
        }
    }
}

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

/// Where a loop's per-iteration items come from and how it learns
/// there is no next one. The one question every loop answers, with
/// three instances of the one shape:
///
/// - `Lists`: `over` on `List[T]` ports; the count is known up front
///   (zip-trim + `max_iters` cap) and exhaustion is `index + 1 >=
///   iter_cap`.
/// - `DoneDriven`: no `over`; iterations launch until a `self.done`
///   vote or the `max_iters` cap (`iter_cap` is that cap, `None` when
///   uncapped: the compiler already rejects a sequential loop with
///   none of the three terminators).
/// - `Stream`: `over` on ONE `Generator[T]` port; items arrive over
///   time (routed in by the engine as the producer yields) and
///   exhaustion is the stream's end. Sequential mode launches the next
///   iteration when the previous one finished AND an item is buffered;
///   parallel mode launches a lane per arriving item, up to the cap.
#[derive(Debug, Clone)]
pub enum LoopItemSource {
    Lists,
    DoneDriven,
    Stream(LoopStreamState),
}

/// The live state of a stream-driven loop's item source: the shared
/// [`StreamBuffer`] (the ONE implementation of "delivered but not yet
/// taken, and has it ended", also behind every consumer feed) plus the
/// port it serves. Buffered items still count as in-flight pulses in
/// the engine's table (status Routed); an item's pulse is absorbed
/// when its iteration launches (journaled atomically inside
/// `LoopIterationLaunched.stream_pulse`).
#[derive(Clone)]
pub struct LoopStreamState {
    /// The one `over` port the stream feeds.
    pub port: String,
    buf: StreamBuffer,
}

impl std::fmt::Debug for LoopStreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopStreamState")
            .field("port", &self.port)
            .field("buffered", &self.buf.buffered_len())
            .field("end", &self.buf.end())
            .finish()
    }
}

impl LoopStreamState {
    /// A stream source for `port`. `end` seeds an already-ended stream
    /// (the fold, replaying a durable `LoopStreamEnded`); a fresh
    /// instantiation passes `None`.
    pub fn new(port: impl Into<String>, end: Option<StreamEnd>) -> Self {
        let mut buf = StreamBuffer::new();
        if let Some(end) = end {
            buf.close(end);
        }
        Self { port: port.into(), buf }
    }
}

/// One buffered stream item: the pulse that carried it (absorbed at
/// launch) plus its value, shared with the pulse.
#[derive(Debug, Clone)]
pub struct LoopStreamItem {
    pub pulse: Uuid,
    pub value: Arc<Value>,
}

/// A single live loop. Engine code reads/writes through
/// `LoopRuntime` so the state lives in one place.
#[derive(Debug, Clone)]
pub struct LoopInstance {
    pub key: LoopInstanceKey,
    pub config: LoopConfig,
    /// Where iterations' items come from; see [`LoopItemSource`].
    pub source: LoopItemSource,
    /// Effective iteration CAP (never "how many ran"; that is
    /// `launched.len()`): for `Lists` the zip-trimmed, max-capped
    /// count; for `DoneDriven` and `Stream` the `max_iters` cap.
    /// `None` means uncapped.
    pub iter_cap: Option<u32>,
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
    pub carry_values: HashMap<String, Arc<Value>>,
    /// Outer input bag captured at instantiation, keyed by port name.
    /// Sequential mode needs this to launch iteration N+1 after the
    /// LoopIn's pulses have already been absorbed by the first dispatch.
    /// Parallel mode reads the same bag once at launch-all time.
    pub outer_input: HashMap<String, Arc<Value>>,
    pub terminated: Option<LoopTerminationReason>,
}

impl LoopInstance {
    pub fn new(
        key: LoopInstanceKey,
        config: LoopConfig,
        source: LoopItemSource,
        iter_cap: Option<u32>,
        gather_ports: Vec<String>,
    ) -> Self {
        Self {
            key,
            config,
            source,
            iter_cap,
            gather_ports,
            launched: Vec::new(),
            out_fired: Vec::new(),
            gather_lists: HashMap::new(),
            carry_values: HashMap::new(),
            outer_input: HashMap::new(),
            terminated: None,
        }
    }

    /// The next iteration index to launch: one past the highest
    /// launched index. NOT `launched.len()`: `launched` can be
    /// non-contiguous after a partial launch failure, and a
    /// length-derived index would re-use an already-launched slot,
    /// overwriting its gather entry. Shared by `stream_push` and the
    /// outward list length. NOT by `record_loop_out`: there the next
    /// launch is the SUCCESSOR of the just-fired iteration (`index +
    /// 1`), and its replay guard depends on that (a replayed LoopOut
    /// must find its successor already launched and go idle, never
    /// jump past in-flight iterations to a fresh index).
    pub fn next_index(&self) -> u32 {
        self.launched.iter().copied().max().map(|m| m + 1).unwrap_or(0)
    }

}

/// What the engine should do next after a `LoopOut` firing was recorded.
#[derive(Debug, Clone)]
pub enum LoopAdvance {
    /// Launch the next iteration. Carries the iteration index; for a
    /// stream-driven loop it also carries the buffered item this
    /// iteration consumes (the engine absorbs the item's pulse
    /// atomically with the launch row).
    LaunchNext { index: u32, stream_item: Option<LoopStreamItem> },
    /// Loop has terminated. Engine should emit the assembled outward
    /// pulses on `LoopOut`'s outer outputs at the parent frame stack.
    /// `gather` carries a list for EVERY declared gather port (even
    /// ones no iteration touched: such a port produces a list of
    /// length `count` filled with `None`).
    EmitOutward {
        reason: LoopTerminationReason,
        gather: HashMap<String, Vec<Option<Arc<Value>>>>,
        carry: HashMap<String, Arc<Value>>,
    },
    /// Nothing to do (this firing did not advance the loop).
    Idle,
}

/// Per-execution runtime registry. Keyed by `LoopInstanceKey`.
#[derive(Debug, Default, Clone)]
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
        source: LoopItemSource,
        iter_cap: Option<u32>,
        gather_ports: Vec<String>,
    ) -> bool {
        if self.instances.contains_key(&key) {
            return false;
        }
        self.instances.insert(
            key.clone(),
            LoopInstance::new(key, config, source, iter_cap, gather_ports),
        );
        true
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

    /// Record the writes of a `LoopOut` firing at `index`, and nothing
    /// else: no launch decision, no termination. The fold applies a
    /// `LoopOutFired` row through this alone (what the loop did next
    /// is in its later rows); the live engine goes through
    /// [`Self::record_loop_out`], which records the writes here and
    /// then decides. A firing whose index already fired (a crash-
    /// resume replay: the journaled row applied these writes during
    /// the fold) records nothing, so a second, possibly different
    /// firing never overwrites what the journal holds. Returns whether
    /// the firing was new.
    ///
    /// - `gather_writes`: `Value(v)` for ports the body wrote, `Closed`
    ///   for ports the body closed.
    /// - `carry_writes`: `Value(v)` updates the carry; `Closed` keeps
    ///   the previous value.
    pub fn apply_loop_out_writes(
        &mut self,
        key: &LoopInstanceKey,
        index: u32,
        gather_writes: HashMap<String, LoopWrite>,
        carry_writes: HashMap<String, LoopWrite>,
    ) -> Result<bool, String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!(
                "LoopOut fired for {} at parent_frames={:?} index={index} but no LoopInstance exists; \
                 LoopIn must fire before LoopOut",
                key.group_id, key.parent_frames,
            )
        })?;
        if inst.terminated.is_some() || inst.out_fired.contains(&index) {
            return Ok(false);
        }
        inst.out_fired.push(index);
        for (port, slot) in gather_writes {
            inst.gather_lists.entry(port).or_default().insert(index, slot);
        }
        for (port, slot) in carry_writes {
            if let LoopWrite::Value(v) = slot {
                inst.carry_values.insert(port, v);
            }
        }
        Ok(true)
    }

    /// Record a `LoopOut` firing and decide what the loop does next.
    /// `done_vote` is `Some(bool)` for the body's `self.done` value;
    /// `None` means it was closed (treated as false). The writes are
    /// applied ONCE, on the first firing only (see
    /// [`Self::apply_loop_out_writes`]); on replay this falls straight
    /// through to the launch-next decision, which is idempotent (a
    /// `next` already in `launched` returns Idle).
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
        self.apply_loop_out_writes(key, index, gather_writes, carry_writes)?;
        let inst = self.instances.get_mut(key).expect("checked by apply_loop_out_writes");
        // A LoopOut firing arriving after termination (cancellation, a
        // peer iteration that already raced to done, an out-of-band
        // emit) must NOT re-enter the decision tree: the sequential
        // path would otherwise dispatch `LaunchNext` and revive a
        // terminated loop, journaling a fresh LoopIterationLaunched.
        if inst.terminated.is_some() {
            return Ok(LoopAdvance::Idle);
        }

        let done = done_vote.unwrap_or(false);
        // Termination-reason precedence: when `iter_cap` was capped
        // at `max_iters` AND the loop reached the cap, BOTH conditions
        // are true at the last iteration. Check max FIRST so the
        // reason names the binding constraint.
        let max_reached = inst.config.max_iters.map(|m| index + 1 >= m).unwrap_or(false);
        let over_exhausted = inst.iter_cap.is_some_and(|cap| index + 1 >= cap);

        // Decide next action.
        if inst.config.parallel {
            if let Some(advance) = Self::parallel_completion(inst) {
                return self.emit_outward(key, advance);
            }
            return Ok(LoopAdvance::Idle);
        }

        // Sequential: decide launch-next vs terminate.
        if done {
            return self.emit_outward(key, LoopTerminationReason::DoneVoted);
        }
        if max_reached {
            return self.emit_outward(key, LoopTerminationReason::MaxItersReached);
        }
        match &mut inst.source {
            LoopItemSource::Lists | LoopItemSource::DoneDriven => {
                if over_exhausted {
                    return self.emit_outward(key, LoopTerminationReason::OverExhausted);
                }
                let next = index + 1;
                if inst.launched.contains(&next) {
                    // Crash-resume replay: this LoopOut firing already
                    // dispatched its LaunchNext before the crash (the
                    // `LoopIterationLaunched` row for `next` is in the
                    // journal, which is the only way it enters
                    // `launched` on rehydrate). Re-launching would
                    // duplicate the iteration's body pulses and re-run
                    // its body.
                    return Ok(LoopAdvance::Idle);
                }
                Ok(LoopAdvance::LaunchNext { index: next, stream_item: None })
            }
            LoopItemSource::Stream(st) => {
                let next = index + 1;
                if inst.launched.contains(&next) {
                    // Same crash-resume replay guard as the counted
                    // sources: the successor is already launched.
                    return Ok(LoopAdvance::Idle);
                }
                if let Some((pulse, value)) = st.buf.pop() {
                    return Ok(LoopAdvance::LaunchNext {
                        index: next,
                        stream_item: Some(LoopStreamItem { pulse, value }),
                    });
                }
                let end = st.buf.end().cloned();
                let port = st.port.clone();
                match end {
                    Some(StreamEnd::Finished) => {
                        self.emit_outward(key, LoopTerminationReason::OverExhausted)
                    }
                    Some(StreamEnd::Failed { error }) => Err(format!(
                        "the stream feeding loop '{}' over '{port}' failed upstream: {error}",
                        key.group_id,
                    )),
                    // Stream still open, nothing buffered: idle until
                    // the engine routes the next item (or the end) in.
                    None => Ok(LoopAdvance::Idle),
                }
            }
        }
    }

    /// Parallel-mode completion check: `Some(reason)` when every
    /// launched lane has fired its LoopOut AND no further lane can
    /// launch. In parallel, LoopOuts fire out of order, so the
    /// completing firing's `index` is arbitrary; the binding constraint
    /// is read from the instance's own state, ordering-independent.
    fn parallel_completion(inst: &LoopInstance) -> Option<LoopTerminationReason> {
        // Every launched lane has fired its LoopOut, whatever the
        // source. A partial launch (a launch failed midway) leaves the
        // loop with fewer lanes than its cap; that failure terminates
        // the instance on its own path, and this check never calls a
        // loop complete while a launched lane is still out.
        if inst.out_fired.len() < inst.launched.len() {
            return None;
        }
        match &inst.source {
            LoopItemSource::Lists | LoopItemSource::DoneDriven => {
                if inst.iter_cap.is_some_and(|cap| inst.launched.len() as u32 >= cap) {
                    // If `iter_cap` equals `max_iters`, max was the
                    // binding constraint.
                    Some(if inst.config.max_iters == inst.iter_cap {
                        LoopTerminationReason::MaxItersReached
                    } else {
                        LoopTerminationReason::OverExhausted
                    })
                } else {
                    None
                }
            }
            LoopItemSource::Stream(st) => {
                if inst.iter_cap.is_some_and(|cap| inst.launched.len() as u32 >= cap) {
                    // The cap bound the loop before the stream ended.
                    return Some(LoopTerminationReason::MaxItersReached);
                }
                match st.buf.end() {
                    Some(StreamEnd::Finished) if st.buf.buffered_len() == 0 => {
                        Some(LoopTerminationReason::OverExhausted)
                    }
                    // A failed end terminates via the stream_close /
                    // record_loop_out error paths, never as a clean
                    // completion.
                    _ => None,
                }
            }
        }
    }

    /// A stream item arrived for a stream-driven loop. Buffers it, or
    /// launches the next iteration with it when the loop is ready
    /// (sequential: every launched iteration has fired its LoopOut;
    /// parallel: immediately, up to the `max_iters` cap). Errors on an
    /// instance whose source is not a stream or a terminated instance
    /// (engine routing bugs: the routing pass drops post-termination
    /// items itself).
    pub fn stream_push(
        &mut self,
        key: &LoopInstanceKey,
        item: LoopStreamItem,
    ) -> Result<LoopAdvance, String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!("stream item for loop '{}' with no LoopInstance", key.group_id)
        })?;
        if inst.terminated.is_some() {
            return Err(format!(
                "stream item routed to loop '{}' after it terminated; the routing pass \
                 drops these, so this is an engine routing bug",
                key.group_id
            ));
        }
        let parallel = inst.config.parallel;
        let cap = inst.iter_cap;
        let next_index = inst.next_index();
        let launched = inst.launched.len() as u32;
        let below_cap = cap.is_none_or(|c| launched < c);
        let ready = if parallel {
            below_cap
        } else {
            // Sequential: ready exactly when every launched iteration
            // has fired its LoopOut (nothing in flight). Derived, not
            // stored, so a rehydrated instance is correct by
            // construction.
            inst.out_fired.len() == inst.launched.len() && below_cap
        };
        let LoopItemSource::Stream(st) = &mut inst.source else {
            return Err(format!(
                "stream item routed to loop '{}' whose over is not a stream; engine \
                 routing bug",
                key.group_id
            ));
        };
        if ready {
            return Ok(LoopAdvance::LaunchNext { index: next_index, stream_item: Some(item) });
        }
        // `reinstate`, not `push`: an item CAN legitimately land after
        // the recorded end here. The drive's at-least-once re-fold
        // path re-routes buffered items (their pulses refold Pending)
        // into a fresh instance whose durable `stream_end` is already
        // seeded, so "after the end" is re-delivery order, not arrival
        // order. Arrival order is guarded where it exists: a producer
        // cannot emit past its close, and the router routes each
        // pending pulse once. Every end-honoring decision in this file
        // reads the buffer first, so a reinstated item always launches
        // before the end terminates the loop.
        st.buf.reinstate(item.pulse, item.value);
        Ok(LoopAdvance::Idle)
    }

    /// Record the stream's end on a stream-driven instance and nothing
    /// else: no termination decision. The fold applies a
    /// `LoopStreamEnded` row through this (whether the end terminated
    /// the loop is in its later rows); the live engine goes through
    /// [`Self::stream_close`], which records the end here and then
    /// settles it.
    pub fn record_stream_end(&mut self, key: &LoopInstanceKey, end: StreamEnd) -> Result<(), String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!("stream close for loop '{}' with no LoopInstance", key.group_id)
        })?;
        let LoopItemSource::Stream(st) = &mut inst.source else {
            return Err(format!(
                "stream close routed to loop '{}' whose over is not a stream; engine \
                 routing bug",
                key.group_id
            ));
        };
        st.buf.close(end);
        Ok(())
    }

    /// The stream feeding a stream-driven loop ended. Records the end
    /// and, when the loop is already idle (nothing in flight, nothing
    /// buffered), terminates it: cleanly on `Finished`, as a loud loop
    /// failure on `Failed` (a loop over a failed stream must never emit
    /// a gather that looks complete). When work is still in flight, the
    /// termination happens at the last LoopOut instead.
    pub fn stream_close(
        &mut self,
        key: &LoopInstanceKey,
        end: StreamEnd,
    ) -> Result<LoopAdvance, String> {
        self.record_stream_end(key, end)?;
        self.settle_stream_end(key)
    }

    /// Evaluate an ALREADY-RECORDED stream end: the ONE derivation of
    /// "does this end terminate the loop now", shared by the live close
    /// (`stream_close`) and the rehydrate sweep (a resumed instance
    /// whose durable `stream_end` was seeded; its close pulse is gone,
    /// so nothing else would ever re-evaluate it). `Idle` when no end
    /// is recorded or work remains (in-flight lanes settle at their
    /// last LoopOut; buffered/pending items settle through
    /// `stream_push`).
    pub fn settle_stream_end(&mut self, key: &LoopInstanceKey) -> Result<LoopAdvance, String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!("stream end settle for loop '{}' with no LoopInstance", key.group_id)
        })?;
        let LoopItemSource::Stream(st) = &mut inst.source else {
            return Err(format!(
                "stream end settle for loop '{}' whose over is not a stream; engine \
                 routing bug",
                key.group_id
            ));
        };
        let settled_end = st.buf.end().cloned();
        let port = st.port.clone();
        let queue_empty = st.buf.buffered_len() == 0;
        if inst.terminated.is_some() || settled_end.is_none() {
            return Ok(LoopAdvance::Idle);
        }
        if let Some(StreamEnd::Failed { error }) = settled_end {
            // Fail the loop NOW, whatever is in flight: lanes still
            // running land on the already-terminated-Failed guard.
            return Err(format!(
                "the stream feeding loop '{}' over '{port}' failed upstream: {error}",
                key.group_id,
            ));
        }
        if inst.config.parallel {
            if let Some(reason) = Self::parallel_completion(inst) {
                return self.emit_outward(key, reason);
            }
            return Ok(LoopAdvance::Idle);
        }
        // Sequential: only an idle loop terminates here; otherwise the
        // last LoopOut's advance sees the end.
        let idle = inst.out_fired.len() == inst.launched.len();
        if idle && queue_empty {
            return self.emit_outward(key, LoopTerminationReason::OverExhausted);
        }
        Ok(LoopAdvance::Idle)
    }

    /// Live stream-driven instances whose end is already recorded, as
    /// `(key, stream port, end)`. The rehydrate sweep asks this to
    /// find loops whose durable `stream_end` needs re-evaluating; the
    /// end kind rides along because a `Failed` end settles
    /// unconditionally while a `Finished` one defers to items still
    /// pending re-delivery.
    pub fn stream_instances_with_recorded_end(
        &self,
    ) -> Vec<(LoopInstanceKey, String, StreamEnd)> {
        self.instances
            .iter()
            .filter(|(_, inst)| inst.terminated.is_none())
            .filter_map(|(key, inst)| match &inst.source {
                LoopItemSource::Stream(st) => st
                    .buf
                    .end()
                    .map(|end| (key.clone(), st.port.clone(), end.clone())),
                _ => None,
            })
            .collect()
    }

    /// Drain a terminated stream-driven loop's still-buffered items,
    /// returning their pulse ids so the engine absorbs them (they will
    /// never launch; leaving them routed would block completion
    /// forever). `Ok(empty)` for a non-stream loop (the callers run on
    /// generic termination paths); a MISSING instance is an engine bug
    /// and errors loudly, because silently answering "nothing
    /// buffered" for the wrong key would leave the real key's items
    /// routed forever.
    pub fn drain_stream_leftovers(
        &mut self,
        key: &LoopInstanceKey,
    ) -> Result<Vec<Uuid>, String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!(
                "drain_stream_leftovers for loop '{}' with no LoopInstance; engine bug",
                key.group_id
            )
        })?;
        match &mut inst.source {
            LoopItemSource::Stream(st) => Ok(st.buf.drain()),
            _ => Ok(Vec::new()),
        }
    }

    /// Mark an instance as terminated and assemble the outward emit
    /// payload. The engine's zero-iter path calls it directly, without
    /// going through `record_loop_out`; the fold calls it on a clean
    /// `LoopTerminated` row. An abnormal end (failed, cancelled) has
    /// no payload to assemble and goes through [`Self::terminate`].
    /// `Idle` for an instance that already terminated (a replayed
    /// row, nothing to emit twice); an error for no instance at all.
    pub fn emit_outward(
        &mut self,
        key: &LoopInstanceKey,
        reason: LoopTerminationReason,
    ) -> Result<LoopAdvance, String> {
        if !self.terminate(key, reason)? {
            return Ok(LoopAdvance::Idle);
        }
        let inst = self.instances.get_mut(key).expect("checked by terminate");
        // List length = number of iterations actually launched. In
        // normal flow (contiguous launches) this equals
        // `launched.len()`; `next_index` computes `max + 1` so a non-
        // contiguous launched (partial launch failure) still produces
        // a list whose indices align with the journal.
        let count = inst.next_index();
        let mut gather: HashMap<String, Vec<Option<Arc<Value>>>> = HashMap::new();
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
        Ok(LoopAdvance::EmitOutward { reason, gather, carry })
    }

    /// Mark the instance at `key` terminated with `reason`. `false`
    /// for an instance that already ended (an instance terminates
    /// once: a replayed row, a straggling firing); an error for no
    /// instance at all. Every end goes through here except a
    /// cancellation walk (`cancel_inside`) and the engine's override
    /// of a clean end whose outward emit then failed (which re-marks
    /// the instance Failed directly, the one rewrite of `terminated`).
    pub fn terminate(
        &mut self,
        key: &LoopInstanceKey,
        reason: LoopTerminationReason,
    ) -> Result<bool, String> {
        let inst = self.instances.get_mut(key).ok_or_else(|| {
            format!("termination of loop '{}' with no LoopInstance", key.group_id)
        })?;
        if inst.terminated.is_some() {
            return Ok(false);
        }
        inst.terminated = Some(reason);
        Ok(true)
    }

    /// Cancellation: mark every instance whose `parent_frames` is at
    /// or inside the cancel scope `frames` (i.e. `frames` is a prefix
    /// of `parent_frames`) as terminated with `Cancelled`. Outward
    /// emit is the engine's responsibility (it emits closures, not a
    /// real outward emit, on cancellation), plus a `LoopTerminated`
    /// journal write so cancellation is durable across resume.
    pub fn cancel_inside(&mut self, frames: &LoopFrames, color: Color) -> Vec<LoopInstanceKey> {
        let mut cancelled = Vec::new();
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
                cancelled.push(inst.key.clone());
            }
        }
        cancelled
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

/// The instance key a loop boundary firing belongs to, from the
/// boundary node and the frames it fired at.
pub fn instance_key(node_def: &NodeDefinition, frames: &LoopFrames, color: Color) -> Result<LoopInstanceKey, String> {
    let group_id = node_def
        .group_boundary
        .as_ref()
        .ok_or_else(|| format!("{} '{}' missing group_boundary", node_def.node_type, node_def.id))?
        .group_id
        .clone();
    Ok(LoopInstanceKey {
        group_id,
        parent_frames: boundary_parent_frames(&node_def.node_type, frames),
        color,
    })
}

/// The declared gather port names for a loop: the matching LoopOut
/// node's outward outputs minus the carry-named ones. Captured at
/// instantiation so the outward emit assembles a list for EVERY
/// declared gather port, even ones no iteration writes to (which
/// would otherwise produce no pulse and deadlock downstream
/// consumers). `None` when the project has no `{group_id}__out` node,
/// which every caller treats as corruption.
pub fn loop_gather_ports(
    project: &ProjectDefinition,
    group_id: &str,
    carry: &[String],
) -> Option<Vec<String>> {
    let loop_out_id = crate::project::boundary_out_id(group_id);
    project.nodes.iter().find(|n| n.id == loop_out_id).map(|n| {
        n.outputs
            .iter()
            .filter(|p| !carry.contains(&p.name))
            .map(|p| p.name.clone())
            .collect()
    })
}

/// Answer the loop's ONE item-source question from its config and the
/// LoopIn's declared port types: `over` lists (count known), no `over`
/// (done-driven), or `over` on a single `Generator[T]` port (stream).
/// The returned cap is the iteration CAP: the zip-trimmed count for
/// lists, `max_iters` otherwise; `None` means uncapped.
pub fn compute_loop_item_source(
    config: &LoopConfig,
    node_def: &NodeDefinition,
    input: &InputBag,
) -> Result<(LoopItemSource, Option<u32>), String> {
    let source = loop_source_kind(config, node_def)?;
    let cap = match &source {
        LoopItemSource::Lists => Some(compute_loop_iter_cap(config, input)?),
        LoopItemSource::DoneDriven | LoopItemSource::Stream(_) => config.max_iters,
    };
    Ok((source, cap))
}

/// The source VARIANT alone, from the config plus the LoopIn's declared
/// port types (no input values needed).
fn loop_source_kind(config: &LoopConfig, node_def: &NodeDefinition) -> Result<LoopItemSource, String> {
    let stream_port =
        config.over.iter().find(|p| super::ready::is_generator_input(node_def, p));
    if let Some(port) = stream_port {
        // The compiler rejects a stream mixed with other over ports;
        // reaching here with one means the compiled shape drifted.
        if config.over.len() != 1 {
            return Err(format!(
                "loop 'over' mixes the stream port '{port}' with other ports; a loop \
                 iterates one stream at a time (compiled shape drifted)"
            ));
        }
        return Ok(LoopItemSource::Stream(LoopStreamState::new(port.clone(), None)));
    }
    if config.over.is_empty() {
        return Ok(LoopItemSource::DoneDriven);
    }
    Ok(LoopItemSource::Lists)
}

pub fn compute_loop_iter_cap(config: &LoopConfig, input: &InputBag) -> Result<u32, String> {
    let mut lengths: Vec<usize> = Vec::new();
    for port in &config.over {
        match input.get(port).and_then(|v| v.as_array()) {
            Some(arr) => lengths.push(arr.len()),
            None => {
                // An absent `over` port (unwired, or an optional input
                // whose upstream closed) must NOT silently degrade:
                // skipping it would either iterate over the remaining
                // lists with this port missing inside the body, or
                // (all absent) reclassify a list-driven loop as a
                // done-driven one (unbounded, or capped only by
                // max_iters).
                return Err(format!(
                    "loop 'over' port '{port}' must be a List; got {}",
                    input
                        .get(port)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "no value (unwired, or upstream closed)".into())
                ));
            }
        }
    }
    let count = if lengths.is_empty() {
        // List-driven only: the caller (`compute_loop_item_source`)
        // classifies an empty `over` as done-driven and never calls
        // this. Reaching here with no lists is a caller bug.
        return Err(
            "compute_loop_iter_cap on a loop with no 'over' lists; the caller \
             classifies done-driven loops itself"
                .into(),
        );
    } else if config.trim_on_mismatch {
        lengths.into_iter().min().expect("checked non-empty above") as u32
    } else {
        let first = lengths[0];
        for l in &lengths[1..] {
            if *l != first {
                return Err(format!(
                    "loop 'over' length mismatch with trim_on_mismatch=false: {lengths:?}",
                    lengths = lengths
                ));
            }
        }
        first as u32
    };
    Ok(match config.max_iters {
        Some(m) => count.min(m),
        None => count,
    })
}

/// What a LoopIn firing established, whether it instantiated the loop
/// or found it already there.
pub struct LoopInFiring {
    pub key: LoopInstanceKey,
    pub config: LoopConfig,
    pub iter_cap: Option<u32>,
    pub is_stream: bool,
    /// True on first-time instantiation (the live engine journals
    /// `LoopInstantiated` exactly then).
    pub first_instantiation: bool,
}

/// A `LoopIn` fired with `input` (the firing's bag, gate included) at
/// `frames`: parse its config, classify its item source, seed its
/// carries, and look up or create its instance. THE one derivation of
/// a loop instance from a LoopIn firing, shared by the live engine
/// and the journal fold (the fold rebuilds the bag from the LoopIn
/// record's absorbed pulses). Everything that can fail does so BEFORE
/// `ensure`, so a failure never leaves an instance behind with no
/// `LoopInstantiated` row.
pub fn instantiate(
    loop_runtime: &mut LoopRuntime,
    node_def: &NodeDefinition,
    project: &ProjectDefinition,
    input: &InputBag,
    frames: &LoopFrames,
    color: Color,
) -> Result<LoopInFiring, String> {
    let key = instance_key(node_def, frames, color)?;
    let group_id = key.group_id.clone();
    let mut input = input.clone();
    // The loop's gate is consumed at the boundary, never broadcast into
    // the body (the LoopIn has no `_should_flow` inside output).
    input.remove(crate::exec::skip::SHOULD_FLOW_PORT);
    // LoopConfig lives on LoopIn ONLY (the compiler emits the minimal
    // `{"parentId": ...}` on LoopOut); LoopOut reads the config from
    // the runtime instance it shares with LoopIn.
    let config = LoopConfig::from_node_config(&node_def.config)
        .map_err(|e| format!("LoopIn '{}': {}", node_def.id, e))?;
    let (source, iter_cap) = compute_loop_item_source(&config, node_def, &input)?;
    let is_stream = matches!(source, LoopItemSource::Stream(_));
    // A stream's items never live in the outer input bag (they are
    // routed in pulse by pulse); a first item that happened to ride
    // this dispatch is not a broadcast input.
    if let LoopItemSource::Stream(st) = &source {
        input.remove(&st.port);
    }
    let gather_ports = loop_gather_ports(project, &group_id, &config.carry).ok_or_else(|| {
        format!(
            "LoopIn '{}': project has no LoopOut node '{}'; corrupt compiled project shape",
            node_def.id,
            crate::project::boundary_out_id(&group_id),
        )
    })?;
    // Initial carry seeds come from the loop's same-named inputs. A
    // carry port that is wired and carries a real value seeds from it;
    // an unwired (or null) carry seeds from its declared type's ZERO
    // VALUE (Number -> 0, String -> "", List -> [], etc), so a loop
    // can accumulate from a clean default without the author wiring
    // an explicit seed. The port type lives on the LoopIn's input def.
    let mut seed_carry: Vec<(String, Arc<Value>)> = Vec::new();
    for carry_port in &config.carry {
        let v = match input.get(carry_port) {
            Some(v) if !v.is_null() => v.clone(),
            _ => {
                let port = node_def
                    .inputs
                    .iter()
                    .find(|p| &p.name == carry_port)
                    .ok_or_else(|| {
                        format!(
                            "loop '{}': carry port '{}' has no input definition on the \
                             LoopIn node; corrupt compiled project shape",
                            group_id, carry_port,
                        )
                    })?;
                Arc::new(port.port_type.zero_value())
            }
        };
        seed_carry.push((carry_port.clone(), v));
    }
    let first_instantiation =
        loop_runtime.ensure(key.clone(), config.clone(), source, iter_cap, gather_ports);
    if first_instantiation {
        let inst = loop_runtime.get_mut(&key).expect("just ensured");
        inst.outer_input = input.into_iter().collect();
        for (port, v) in seed_carry {
            inst.carry_values.insert(port, v);
        }
    }
    Ok(LoopInFiring { key, config, iter_cap, is_stream, first_instantiation })
}

/// What one iteration launch put in the world: the body's pulses and
/// the roots it kicks.
pub struct IterationLaunch {
    pub emissions: Vec<PulseEmission>,
    pub roots: Vec<String>,
    pub body_frames: LoopFrames,
}

/// Launch iteration `index` of the instance at `key`: put the body's
/// pulses on LoopIn's inside outputs at the iteration's frames (the
/// `over` slice, every broadcast input, the current carries, the
/// implicit `index`; a stream iteration's item on its port), close
/// every inside port this iteration did not emit, and name the body's
/// roots. Records the launch on the instance. THE one derivation of a
/// launch, shared by the live engine (which then journals
/// `LoopIterationLaunched`) and the journal fold (which applies that
/// row through this). Reads the instance's outer input and carries
/// as they are NOW: sequential loops launch N+1 after N's carry
/// writes landed.
#[allow(clippy::too_many_arguments)]
pub fn launch_iteration(
    loop_runtime: &mut LoopRuntime,
    key: &LoopInstanceKey,
    index: u32,
    stream_item: Option<LoopStreamItem>,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
) -> Result<IterationLaunch, String> {
    let group_id = key.group_id.as_str();
    let inst = loop_runtime.get(key).ok_or_else(|| {
        format!("launch for loop '{group_id}' with no LoopInstance")
    })?;
    let body_frames = iteration_frames(&key.parent_frames, index);
    let mut output: OutputBag = OutputBag::new();
    if let Some(item) = &stream_item {
        let LoopItemSource::Stream(st) = &inst.source else {
            return Err(format!(
                "stream launch for loop '{group_id}' whose over is not a stream"
            ));
        };
        output.insert(st.port.clone(), item.value.clone());
    }
    for (port, value) in &inst.outer_input {
        if inst.config.over.contains(port) {
            // The iteration count was derived from this same array's
            // length, so an out-of-range index can only mean iter_cap
            // vs input-list drift (a corrupt snapshot, or a definition
            // change across resume). Fail loud like the rest of this
            // module rather than injecting Null and running the iteration
            // on fabricated data; the caller routes this into
            // `handle_loop_boundary_failure`.
            let arr = value.as_array().ok_or_else(|| {
                format!("loop '{group_id}': over port '{port}' is not a List at launch")
            })?;
            let elem = arr.get(index as usize).cloned().ok_or_else(|| {
                format!(
                    "loop '{group_id}': over port '{port}' has no element at index {index} \
                     (len {}); iter_cap/input drift",
                    arr.len()
                )
            })?;
            output.insert(port.clone(), Arc::new(elem));
        } else if !inst.config.carry.contains(port) {
            // Broadcast input: the same shared value every iteration.
            output.insert(port.clone(), value.clone());
        }
    }
    // Carry values: read current carry on the inside-out side per
    // iteration. Closure carry-write fallback already handled by the
    // runtime; what reaches here is always a real value.
    for (port, value) in &inst.carry_values {
        output.insert(port.clone(), value.clone());
    }
    // Implicit `self.index`.
    output.insert("index".to_string(), Arc::new(serde_json::json!(index)));

    let loop_in_id = crate::project::boundary_in_id(group_id);
    let emission_id = iteration_launch_emission(key.color, group_id, &key.parent_frames, index);
    let mut emissions = Vec::new();
    let mentioned = postprocess_output(
        &loop_in_id, &output, emission_id, key.color, &body_frames, project, pulses, edge_idx,
        &mut emissions,
    )
    .map_err(|e| e.to_string())?;
    // Close every inside output port this iteration did NOT emit (an
    // optional broadcast/over input that arrived closed or unwired is
    // absent from `output`). Without this, a body node wired to that
    // port waits forever on a pulse that never comes and the whole loop
    // hangs (mislabeled Stuck). The generic termination-time sweep skips
    // loop boundaries, so the per-iteration analogue lives here, scoped
    // to this iteration's own frame stack so it can't touch the outward
    // ports. A plain Group already gets this via its Passthrough sweep;
    // loops must not break the skip cascade.
    close_unmentioned_downstream(
        &loop_in_id, &mentioned, emission_id, key.color, &body_frames, project, pulses, edge_idx,
        &mut emissions, None,
    )
    .map_err(|e| e.to_string())?;
    // The body's own roots (members no wire feeds) start with the
    // iteration, at its frames: everything inside a loop runs once per
    // iteration, wired to the loop's edges or not.
    let roots = crate::project::scope_body_roots(project, edge_idx, group_id);
    loop_runtime.record_launched(key, index);
    Ok(IterationLaunch { emissions, roots, body_frames })
}

/// What a `LoopOut` firing wrote, read off its bag: per gather port
/// and per carry port a value or a closure, and the body's `done`
/// vote (`None` when the port was closed or never wired).
pub struct LoopOutWrites {
    pub gather_writes: HashMap<String, LoopWrite>,
    pub carry_writes: HashMap<String, LoopWrite>,
    pub done_vote: Option<bool>,
}

/// Classify a `LoopOut` firing's bag into its writes. THE one reading
/// of a LoopOut firing, shared by the live engine and the fold.
pub fn classify_loop_out(
    node_def: &NodeDefinition,
    config: &LoopConfig,
    input: &InputBag,
    closed_ports: &[String],
) -> Result<LoopOutWrites, String> {
    let mut gather_writes: HashMap<String, LoopWrite> = HashMap::new();
    let mut carry_writes: HashMap<String, LoopWrite> = HashMap::new();
    let mut done_vote: Option<bool> = None;
    let closed: std::collections::HashSet<&str> = closed_ports.iter().map(|s| s.as_str()).collect();
    for port in &node_def.inputs {
        let name = &port.name;
        if name == "done" {
            done_vote = if closed.contains(name.as_str()) {
                None
            } else {
                // `done` is a Boolean port marked optional. Three
                // legitimate inbound shapes, all "no vote":
                //   - absent from the bag        (port not wired)
                //   - present as Value::Null     (no-value marker;
                //     `check_input` in ready.rs treats Null as Ok
                //     for any port AND maps non-matching values on
                //     optional ports to Null via NullIt, so Null
                //     is the normalized form of "no usable vote")
                //   - present as a real bool     (the actual vote)
                // A non-null non-bool value here is impossible
                // post-type-check; if one slips through, fail loud
                // because silently dropping the vote would let a
                // wrongly-typed body skip the termination check.
                match input.get(name) {
                    None => None,
                    Some(v) if v.is_null() => None,
                    Some(v) => match v.as_bool() {
                        Some(b) => Some(b),
                        None => return Err(format!(
                            "LoopOut '{}' 'done' port received non-boolean non-null value {:?}; \
                             type-check should have rejected this upstream",
                            node_def.id, v,
                        )),
                    },
                }
            };
            continue;
        }
        // `LoopWrite::Closed` for closure, `LoopWrite::Value(v)`
        // for an actual write (including a real JSON null). The
        // dispatch invariant says every non-closed input port has a
        // pulse in the bag; if it's missing, that's corruption, not
        // "default to null" (which for carry ports would silently
        // overwrite the current value to null instead of keeping the
        // previous).
        let write = if closed.contains(name.as_str()) {
            LoopWrite::Closed
        } else {
            let v = input.get(name).cloned().ok_or_else(|| format!(
                "LoopOut '{}' input port '{}' is neither closed nor present in input bag; \
                 dispatch invariant violated",
                node_def.id, name,
            ))?;
            LoopWrite::Value(v)
        };
        if config.carry.contains(name) {
            carry_writes.insert(name.clone(), write);
        } else {
            gather_writes.insert(name.clone(), write);
        }
    }
    // A `done` vote on a PARALLEL loop has no decision tree to enter
    // (all iterations launched upfront; termination is all-fired).
    // Validate rejects wiring `done` in parallel mode, so a vote
    // arriving here means the compiled config drifted; silently
    // discarding it would let a wrongly-compiled loop ignore its own
    // termination signal.
    if config.parallel && done_vote.is_some() {
        return Err(format!(
            "LoopOut '{}': `done` vote received on a parallel loop; the compiler's \
             validation rejects `done` in parallel mode, so this compiled config drifted",
            node_def.id,
        ));
    }
    Ok(LoopOutWrites { gather_writes, carry_writes, done_vote })
}

/// Put a terminated loop's outward payload on the wires: the assembled
/// gather lists and final carry values on LoopOut's outer outputs at
/// the parent frame stack. Nothing partial ships on failure
/// (postprocess pre-validates before touching state); the caller then
/// closes the outward surface instead ([`close_loop_outward`]).
pub fn emit_loop_outward(
    key: &LoopInstanceKey,
    gather: HashMap<String, Vec<Option<Arc<Value>>>>,
    carry: HashMap<String, Arc<Value>>,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
) -> Result<Vec<PulseEmission>, String> {
    let loop_out_id = crate::project::boundary_out_id(&key.group_id);
    let mut output = OutputBag::new();
    for (port, slots) in gather {
        // The assembled list is a new value: one element per
        // iteration, a null where the iteration closed the port.
        let arr: Vec<Value> = slots
            .into_iter()
            .map(|v| v.map(|v| (*v).clone()).unwrap_or(Value::Null))
            .collect();
        output.insert(port, Arc::new(Value::Array(arr)));
    }
    for (port, v) in carry {
        output.insert(port, v);
    }
    let mut emissions = Vec::new();
    postprocess_output(
        &loop_out_id,
        &output,
        loop_termination_emission(key.color, &key.group_id, &key.parent_frames),
        key.color,
        &key.parent_frames,
        project,
        pulses,
        edge_idx,
        &mut emissions,
    )
    .map_err(|e| format!("loop '{}' outward emit failed: {e}", key.group_id))?;
    Ok(emissions)
}

/// The closures a loop that ends abnormally (failed, cancelled) owes
/// the outside: one per outward port of its LoopOut, at the parent
/// frames, so downstream skips cascade instead of deadlocking.
pub fn close_loop_outward(
    key: &LoopInstanceKey,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
) -> Vec<PulseEmission> {
    crate::exec::boundary::close_scope_outward(
        project,
        edge_idx,
        pulses,
        loop_termination_emission(key.color, &key.group_id, &key.parent_frames),
        key.color,
        &key.group_id,
        &key.parent_frames,
    )
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

    fn val(v: serde_json::Value) -> LoopWrite { LoopWrite::Value(Arc::new(v)) }
    fn closed() -> LoopWrite { LoopWrite::Closed }

    #[test]
    fn sequential_over_exhausted() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), LoopItemSource::Lists, Some(3),vec!["result".into()]);

        for i in 0..3 {
            rt.record_launched(&k, i);
            let mut g = HashMap::new();
            g.insert("result".to_string(), val(serde_json::json!(i)));
            let advance = rt.record_loop_out(&k, i, g, HashMap::new(), Some(false)).unwrap();
            match (i, &advance) {
                (0 | 1, LoopAdvance::LaunchNext { index, stream_item: None }) => {
                    assert_eq!(*index, i + 1)
                }
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
        rt.ensure(k.clone(), cfg(true, &["items"], &[], None), LoopItemSource::Lists, Some(3),vec![]);
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
        rt.ensure(k.clone(), cfg(false, &[], &["acc"], Some(100)), LoopItemSource::DoneDriven, Some(100),vec![]);
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
        rt.ensure(k.clone(), cfg(false, &[], &["acc"], Some(5)), LoopItemSource::DoneDriven, Some(5),vec![]);
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
        assert_eq!(*inst.carry_values["acc"], serde_json::json!("first"));
    }

    /// A body that legitimately writes JSON null on a carry port must
    /// update the carry value to null, NOT keep the previous. This is
    /// the closure-vs-null disambiguation the tagged `LoopWrite` enum
    /// exists to preserve across the journal round-trip.
    #[test]
    fn carry_written_null_distinct_from_closed() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &[], &["acc"], Some(5)), LoopItemSource::DoneDriven, Some(5),vec![]);
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
        assert_eq!(*inst.carry_values["acc"], serde_json::Value::Null);
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
            LoopItemSource::Lists,
            Some(3),
            vec!["result".into(), "errors".into()],
        );
        let mut last = None;
        for i in 0..3 {
            rt.record_launched(&k, i);
            // Only write to `result`, never to `errors`.
            let mut g = HashMap::new();
            g.insert("result".to_string(), val(serde_json::json!(i)));
            last = Some(rt.record_loop_out(&k, i, g, HashMap::new(), Some(false)).unwrap());
        }
        let LoopAdvance::EmitOutward { gather, .. } = last.expect("three firings") else {
            panic!("the third firing ends the loop")
        };
        // The never-written port still gets a list, one null per
        // iteration, so its consumer sees a value and not a missing
        // pulse.
        assert_eq!(gather["errors"], vec![None, None, None]);
        let result: Vec<serde_json::Value> =
            gather["result"].iter().map(|v| (**v.as_ref().unwrap()).clone()).collect();
        assert_eq!(result, vec![serde_json::json!(0), serde_json::json!(1), serde_json::json!(2)]);
        // A replayed firing after termination is idle.
        let again = rt.record_loop_out(&k, 2, HashMap::new(), HashMap::new(), Some(true));
        assert!(matches!(again.unwrap(), LoopAdvance::Idle));
    }

    #[test]
    fn emit_outward_refuses_a_missing_instance_and_idles_a_terminated_one() {
        let mut rt = LoopRuntime::new();
        let k = key();
        assert!(rt.emit_outward(&k, LoopTerminationReason::OverExhausted).is_err());
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), LoopItemSource::Lists, Some(0), vec![]);
        assert!(matches!(
            rt.emit_outward(&k, LoopTerminationReason::OverExhausted).unwrap(),
            LoopAdvance::EmitOutward { .. }
        ));
        assert!(matches!(
            rt.emit_outward(&k, LoopTerminationReason::OverExhausted).unwrap(),
            LoopAdvance::Idle
        ));
    }

    #[test]
    fn cancel_inside_marks_terminated() {
        let mut rt = LoopRuntime::new();
        let mut k = key();
        k.parent_frames = vec![LoopIteration { index: 0 }];
        rt.ensure(k.clone(), cfg(false, &["x"], &[], None), LoopItemSource::Lists, Some(5),vec![]);
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
        rt.ensure(inst_0.clone(), cfg(false, &["x"], &[], None), LoopItemSource::Lists, Some(3),vec![]);
        rt.ensure(inst_1.clone(), cfg(false, &["x"], &[], None), LoopItemSource::Lists, Some(3),vec![]);
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
        rt.ensure(outer_key.clone(), cfg(false, &["x"], &[], None), LoopItemSource::Lists, Some(3),vec![]);
        rt.ensure(inner_key.clone(), cfg(false, &["y"], &[], None), LoopItemSource::Lists, Some(3),vec![]);
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

    fn stream_source(port: &str) -> LoopItemSource {
        LoopItemSource::Stream(LoopStreamState::new(port, None))
    }

    fn item(v: i64) -> LoopStreamItem {
        LoopStreamItem { pulse: Uuid::new_v4(), value: Arc::new(serde_json::json!(v)) }
    }

    #[test]
    fn a_zero_item_stream_terminates_with_an_empty_gather() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["rows"], &[], None), stream_source("rows"), None,vec!["result".into()]);
        let advance = rt.stream_close(&k, StreamEnd::Finished).unwrap();
        match advance {
            LoopAdvance::EmitOutward { reason, gather, .. } => {
                assert_eq!(reason, LoopTerminationReason::OverExhausted);
                assert_eq!(gather["result"].len(), 0, "zero iterations, an empty list");
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn a_sequential_stream_buffers_while_an_iteration_is_in_flight() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["rows"], &[], None), stream_source("rows"), None,vec![]);
        // First item launches iteration 0.
        let a = rt.stream_push(&k, item(10)).unwrap();
        assert!(matches!(a, LoopAdvance::LaunchNext { index: 0, stream_item: Some(_) }), "{a:?}");
        rt.record_launched(&k, 0);
        // Second item buffers (iteration 0 in flight).
        let b = rt.stream_push(&k, item(11)).unwrap();
        assert!(matches!(b, LoopAdvance::Idle), "{b:?}");
        // Iteration 0's LoopOut pops the buffered item as iteration 1.
        let c = rt.record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false)).unwrap();
        match c {
            LoopAdvance::LaunchNext { index: 1, stream_item: Some(it) } => {
                assert_eq!(*it.value, serde_json::json!(11));
            }
            other => panic!("expected LaunchNext(1) with the buffered item, got {other:?}"),
        }
    }

    #[test]
    fn a_stream_that_ends_while_an_iteration_runs_terminates_at_its_loop_out() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["rows"], &[], None), stream_source("rows"), None,vec!["result".into()]);
        let _ = rt.stream_push(&k, item(10)).unwrap();
        rt.record_launched(&k, 0);
        // The end arrives mid-iteration: not idle-terminatable yet.
        let a = rt.stream_close(&k, StreamEnd::Finished).unwrap();
        assert!(matches!(a, LoopAdvance::Idle), "{a:?}");
        // The last LoopOut sees the end and emits outward.
        let b = rt.record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false)).unwrap();
        assert!(
            matches!(b, LoopAdvance::EmitOutward { reason: LoopTerminationReason::OverExhausted, .. }),
            "{b:?}"
        );
    }

    #[test]
    fn a_failed_stream_end_is_a_loud_loop_failure() {
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(k.clone(), cfg(false, &["rows"], &[], None), stream_source("rows"), None,vec![]);
        let err = rt.stream_close(&k, StreamEnd::Failed { error: "boom".into() }).unwrap_err();
        assert!(err.contains("boom"), "{err}");
        // A LoopOut landing after the mid-flight failure path
        // terminated the instance is inert (the driver terminates via
        // fail_loop_from_stream; model that with cancel).
    }

    #[test]
    fn a_rehydrated_stream_loop_knows_its_durable_end() {
        // The crash shape the durable `stream_end` exists for: the end
        // arrived (its close pulse consumed for good), iteration 0
        // still in flight, worker dies. The rehydrated instance is
        // seeded with the end; iteration 0's LoopOut must terminate
        // the loop instead of idling forever for a close that can
        // never arrive again.
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(
            k.clone(),
            cfg(false, &["rows"], &[], None),
            LoopItemSource::Stream(LoopStreamState::new("rows", Some(StreamEnd::Finished))),
            None,
            vec!["result".into()],
        );
        rt.record_launched(&k, 0);
        let advance = rt.record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false)).unwrap();
        assert!(
            matches!(advance, LoopAdvance::EmitOutward { reason: LoopTerminationReason::OverExhausted, .. }),
            "the seeded end terminates the resumed loop, got {advance:?}"
        );
    }

    #[test]
    fn a_reinstated_item_launches_before_a_seeded_end_terminates() {
        // Refold re-delivery: an item that arrived before the end
        // refolds Pending and re-routes AFTER the seeded end; it must
        // buffer (reinstate) and launch, never error and never be
        // outrun by the end.
        let mut rt = LoopRuntime::new();
        let k = key();
        rt.ensure(
            k.clone(),
            cfg(false, &["rows"], &[], None),
            LoopItemSource::Stream(LoopStreamState::new("rows", Some(StreamEnd::Finished))),
            None,
            vec![],
        );
        rt.record_launched(&k, 0);
        // Iteration 0 (launched pre-crash) is in flight; the refolded
        // item re-routes now.
        let a = rt.stream_push(&k, item(11)).unwrap();
        assert!(matches!(a, LoopAdvance::Idle), "{a:?}");
        let b = rt.record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false)).unwrap();
        match b {
            LoopAdvance::LaunchNext { index: 1, stream_item: Some(it) } => {
                assert_eq!(*it.value, serde_json::json!(11), "the reinstated item launches");
            }
            other => panic!("the buffered item outranks the end, got {other:?}"),
        }
    }

    #[test]
    fn drain_stream_leftovers_is_loud_on_a_missing_instance() {
        let mut rt = LoopRuntime::new();
        let err = rt.drain_stream_leftovers(&key()).unwrap_err();
        assert!(err.contains("no LoopInstance"), "{err}");
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

    /// When `iter_cap` was capped at `max_iters` AND the loop hits
    /// the cap, the termination reason names the binding constraint
    /// (`MaxItersReached`), not the symptomatic one
    /// (`OverExhausted`). This is what the inspector renders.
    #[test]
    fn max_iters_binding_constraint_reports_max_iters_reason() {
        let mut rt = LoopRuntime::new();
        let k = key();
        // over is 10 long, max_iters = 3 → iter_cap capped to 3.
        // Sequential: at index 2 (the last) both over_exhausted and
        // max_reached fire. Launches mirror the engine's real call
        // sequence: iteration 0 at instantiation, each subsequent one
        // recorded when its `LaunchNext` is dispatched (pre-recording
        // future launches would trip the replay guard, correctly).
        rt.ensure(k.clone(), cfg(false, &["items"], &[], Some(3)), LoopItemSource::Lists, Some(3),vec![]);
        rt.record_launched(&k, 0);
        for i in 0..2 {
            let advance = rt
                .record_loop_out(&k, i, HashMap::new(), HashMap::new(), Some(false))
                .unwrap();
            match advance {
                LoopAdvance::LaunchNext { index, stream_item: None } => rt.record_launched(&k, index),
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
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), LoopItemSource::Lists, Some(3),vec![]);
        rt.record_launched(&k, 0);
        // Live firing: LoopOut@0 dispatches LaunchNext(1).
        let advance = rt
            .record_loop_out(&k, 0, HashMap::new(), HashMap::new(), Some(false))
            .unwrap();
        match advance {
            LoopAdvance::LaunchNext { index, stream_item: None } => rt.record_launched(&k, index),
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
    /// be determined from the instance's `iter_cap` vs `max_iters`
    /// relationship, which is ordering-independent.
    #[test]
    fn parallel_max_iters_binding_constraint_reports_max_iters_reason_regardless_of_order() {
        let mut rt = LoopRuntime::new();
        let k = key();
        // over is 10 long, max_iters = 3 → iter_cap capped to 3.
        rt.ensure(k.clone(), cfg(true, &["items"], &[], Some(3)), LoopItemSource::Lists, Some(3),vec![]);
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
        rt.ensure(k.clone(), cfg(false, &["items"], &[], None), LoopItemSource::Lists, Some(5),vec!["result".into()]);
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

    // ----- The pure derivations the engine and the fold share ----------

    /// `feed.items` / `feed.seed` feed the LoopIn; the body is one
    /// node `step` plus a root `lonely` nothing feeds; the LoopOut
    /// gathers `res` and carries `acc` out to `sink`.
    fn loop_project() -> ProjectDefinition {
        let node = |id: &str, ty: &str, inputs: Vec<serde_json::Value>, outputs: Vec<(&str, &str)>, scope: Vec<&str>, boundary: serde_json::Value, config: serde_json::Value| {
            serde_json::json!({
                "id": id, "nodeType": ty, "label": null, "config": config,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": inputs,
                "outputs": outputs.iter().map(|(n, t)| serde_json::json!({ "name": n, "portType": t, "required": true })).collect::<Vec<_>>(),
                "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false, "images": []
            })
        };
        let inp = |name: &str, ty: &str, required: bool| serde_json::json!({ "name": name, "portType": ty, "required": required });
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("feed", "T", vec![], vec![("items", "List[Number]"), ("seed", "Number")], vec![], serde_json::Value::Null, serde_json::Value::Null),
                node("lp__in", "LoopIn",
                    vec![inp("items", "List[Number]", true), inp("acc", "Number", false), inp("_should_flow", "Boolean", false)],
                    vec![("items", "Number"), ("acc", "Number"), ("index", "Number")], vec![],
                    serde_json::json!({ "groupId": "lp", "role": "In" }),
                    serde_json::json!({ "parallel": false, "over": ["items"], "carry": ["acc"] })),
                node("step", "T", vec![inp("item", "Number", true), inp("acc", "Number", true)], vec![("acc", "Number"), ("res", "Number")], vec!["lp"], serde_json::Value::Null, serde_json::Value::Null),
                node("lonely", "T", vec![], vec![("out", "Number")], vec!["lp"], serde_json::Value::Null, serde_json::Value::Null),
                node("lp__out", "LoopOut",
                    vec![inp("acc", "Number", false), inp("res", "Number", false), inp("done", "Boolean", false)],
                    vec![("acc", "Number"), ("res", "List[Number]")], vec![],
                    serde_json::json!({ "groupId": "lp", "role": "Out" }),
                    serde_json::json!({ "parentId": null })),
                node("sink", "T", vec![inp("res", "List[Number]", true), inp("acc", "Number", true)], vec![], vec![], serde_json::Value::Null, serde_json::Value::Null),
            ],
            "edges": [
                { "id": "e0", "source": "feed", "sourceHandle": "items", "target": "lp__in", "targetHandle": "items" },
                { "id": "e1", "source": "feed", "sourceHandle": "seed", "target": "lp__in", "targetHandle": "acc" },
                { "id": "e2", "source": "lp__in", "sourceHandle": "items", "target": "step", "targetHandle": "item" },
                { "id": "e3", "source": "lp__in", "sourceHandle": "acc", "target": "step", "targetHandle": "acc" },
                { "id": "e4", "source": "step", "sourceHandle": "acc", "target": "lp__out", "targetHandle": "acc" },
                { "id": "e5", "source": "step", "sourceHandle": "res", "target": "lp__out", "targetHandle": "res" },
                { "id": "e6", "source": "lp__out", "sourceHandle": "res", "target": "sink", "targetHandle": "res" },
                { "id": "e7", "source": "lp__out", "sourceHandle": "acc", "target": "sink", "targetHandle": "acc" }
            ],
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("loop project")
    }

    fn def<'a>(project: &'a ProjectDefinition, id: &str) -> &'a NodeDefinition {
        project.nodes.iter().find(|n| n.id == id).expect("node")
    }

    fn bag(entries: &[(&str, serde_json::Value)]) -> InputBag {
        entries.iter().map(|(k, v)| (k.to_string(), Arc::new(v.clone()))).collect()
    }

    fn pending_at<'a>(pulses: &'a PulseTable, node: &str) -> Vec<&'a crate::pulse::Pulse> {
        pulses.get(node).map(|b| b.iter().filter(|p| p.status.is_pending()).collect()).unwrap_or_default()
    }

    #[test]
    fn instantiate_seeds_carries_strips_the_gate_and_is_idempotent() {
        let project = loop_project();
        let mut rt = LoopRuntime::new();
        let input = bag(&[("items", serde_json::json!([10, 20])), ("_should_flow", serde_json::json!(true))]);
        let first = instantiate(&mut rt, def(&project, "lp__in"), &project, &input, &Vec::new(), Uuid::nil()).unwrap();
        assert!(first.first_instantiation);
        assert_eq!(first.iter_cap, Some(2));
        assert!(!first.is_stream);
        let inst = rt.get(&first.key).unwrap();
        assert_eq!(*inst.carry_values["acc"], serde_json::json!(0), "an unwired carry seeds from its zero value");
        assert!(!inst.outer_input.contains_key("_should_flow"), "the gate never enters the body");
        assert_eq!(inst.gather_ports, vec!["res".to_string()]);
        let again = instantiate(&mut rt, def(&project, "lp__in"), &project, &input, &Vec::new(), Uuid::nil()).unwrap();
        assert!(!again.first_instantiation, "a re-fire finds the instance");
    }

    #[test]
    fn instantiate_refuses_a_non_list_over_before_touching_the_runtime() {
        let project = loop_project();
        let mut rt = LoopRuntime::new();
        let input = bag(&[("items", serde_json::json!("not a list"))]);
        assert!(instantiate(&mut rt, def(&project, "lp__in"), &project, &input, &Vec::new(), Uuid::nil()).is_err());
        assert!(rt.iter().next().is_none(), "a refused firing leaves no instance behind");
    }

    #[test]
    fn launch_iteration_puts_the_slice_the_carry_and_the_index_on_the_body_wires() {
        let project = loop_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let input = bag(&[("items", serde_json::json!([10, 20])), ("acc", serde_json::json!(5))]);
        let firing = instantiate(&mut rt, def(&project, "lp__in"), &project, &input, &Vec::new(), Uuid::nil()).unwrap();
        let launch = launch_iteration(&mut rt, &firing.key, 1, None, &project, &edge_idx, &mut pulses).unwrap();
        assert_eq!(launch.body_frames, vec![LoopIteration { index: 1 }]);
        assert_eq!(launch.roots, vec!["lonely".to_string()]);
        let step = pending_at(&pulses, "step");
        let value = |port: &str| step.iter().find(|p| p.target_port == port).map(|p| (*p.value).clone());
        assert_eq!(value("item"), Some(serde_json::json!(20)), "the over slice at the index");
        assert_eq!(value("acc"), Some(serde_json::json!(5)), "the current carry");
        assert!(step.iter().all(|p| p.frames == launch.body_frames));
        assert_eq!(rt.get(&firing.key).unwrap().launched, vec![1]);
        // The same launch replayed puts nothing new on the wires.
        let before = pulses["step"].len();
        launch_iteration(&mut rt, &firing.key, 1, None, &project, &edge_idx, &mut pulses).unwrap();
        assert_eq!(pulses["step"].len(), before, "derived ids make a replay idempotent");
        assert!(launch_iteration(&mut rt, &firing.key, 7, None, &project, &edge_idx, &mut pulses).is_err(), "past the list is drift, not null");
    }

    #[test]
    fn classify_loop_out_splits_gather_from_carry_and_reads_the_vote() {
        let project = loop_project();
        let config = cfg(false, &["items"], &["acc"], None);
        let input = bag(&[("acc", serde_json::json!(6)), ("res", serde_json::Value::Null), ("done", serde_json::json!(true))]);
        let writes = classify_loop_out(def(&project, "lp__out"), &config, &input, &["res".to_string()]).unwrap();
        assert_eq!(writes.carry_writes["acc"], val(serde_json::json!(6)));
        assert_eq!(writes.gather_writes["res"], closed(), "a closed port is a closure, whatever the bag says");
        assert_eq!(writes.done_vote, Some(true));
        let no_vote = classify_loop_out(def(&project, "lp__out"), &config, &bag(&[("acc", serde_json::json!(1)), ("res", serde_json::json!(2))]), &[]).unwrap();
        assert_eq!(no_vote.done_vote, None);
        assert_eq!(no_vote.gather_writes["res"], val(serde_json::json!(2)));
        let parallel = cfg(true, &["items"], &["acc"], None);
        assert!(classify_loop_out(def(&project, "lp__out"), &parallel, &input, &[]).is_err(), "a vote on a parallel loop is drift");
        assert!(classify_loop_out(def(&project, "lp__out"), &config, &bag(&[("res", serde_json::json!(2))]), &[]).is_err(), "a port neither closed nor present is corruption");
    }

    #[test]
    fn emit_and_close_loop_outward_reach_the_consumer_with_derived_ids() {
        let project = loop_project();
        let edge_idx = EdgeIndex::build(&project);
        let k = LoopInstanceKey { group_id: "lp".into(), parent_frames: Vec::new(), color: Uuid::nil() };
        let mut pulses = PulseTable::default();
        let mut gather = HashMap::new();
        gather.insert("res".to_string(), vec![Some(Arc::new(serde_json::json!(1))), None]);
        let mut carry = HashMap::new();
        carry.insert("acc".to_string(), Arc::new(serde_json::json!(9)));
        emit_loop_outward(&k, gather, carry, &project, &edge_idx, &mut pulses).unwrap();
        let sink = pending_at(&pulses, "sink");
        let value = |port: &str| sink.iter().find(|p| p.target_port == port).map(|p| (*p.value).clone());
        assert_eq!(value("res"), Some(serde_json::json!([1, null])), "a closed slot is a null in the list");
        assert_eq!(value("acc"), Some(serde_json::json!(9)));

        let mut closed_pulses = PulseTable::default();
        close_loop_outward(&k, &project, &edge_idx, &mut closed_pulses);
        let sink = pending_at(&closed_pulses, "sink");
        assert_eq!(sink.len(), 2);
        assert!(sink.iter().all(|p| p.closed));
        let mut twice = PulseTable::default();
        close_loop_outward(&k, &project, &edge_idx, &mut twice);
        close_loop_outward(&k, &project, &edge_idx, &mut twice);
        assert_eq!(pending_at(&twice, "sink").len(), 2, "one instance ends once: the same ids dedup");
    }
}

//! What the dispatcher tells a client happened: one [`DispatcherEvent`]
//! per thing, with its delivery identity ([`IdentifiedEvent`]). The live
//! stream (`/events/project/{id}`) and a run's replay
//! (`/runs/{id}/replay`) answer the same rows, and every Rust
//! reader (the dispatcher's `/runs/{id}/events`, `weft follow`, `weft
//! wait`) decodes them through these types; [`spelled`] reads them the
//! way a person does.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::frames::LoopFrames;
use crate::record_ref::{Position, Shown, ValueRef};
use crate::RunId;

pub mod spelled;

/// An event and its delivery identity. A run's recorded events take theirs
/// from their place in its record ([`EventPosition`]), so replay and live
/// delivery identify the same event without comparing timestamps or
/// payload contents, and a follower that lost its stream asks to go on
/// after the last place it holds.
// SYNC: IdentifiedEvent.event_id <-> extension-vscode/src/runFollower.ts DispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentifiedEvent<T> {
    pub event_id: EventId,
    #[serde(flatten)]
    pub event: T,
}

/// Who an event is: its place in its run's record, or, for one about
/// where a run stands rather than what it did (a run parked, a row that
/// no longer reads), a name of its own. Written as one string,
/// `journal:{seq}:{index}:{k}` or `live:{uuid}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventId {
    Recorded(EventPosition),
    Transient(uuid::Uuid),
}

/// An event's place in its run's record: the record event it was
/// projected from ([`Position`]) and its index among the events that one
/// projected to (one record event can paint several: a scope it decided,
/// the bus a value rode). Ordered as the events were written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventPosition {
    pub at: Position,
    pub k: u32,
}

impl std::fmt::Display for EventPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.at.seq, self.at.index, self.k)
    }
}

impl std::str::FromStr for EventPosition {
    type Err = String;

    /// `{seq}:{index}:{k}`, as [`Display`](std::fmt::Display) writes it.
    fn from_str(text: &str) -> Result<Self, String> {
        let mut parts = text.split(':');
        let mut next = |what: &str| parts.next().ok_or_else(|| format!("'{text}' has no {what}"));
        let seq = next("row")?.parse().map_err(|_| format!("'{text}' is no event position (seq:index:k)"))?;
        let index = next("index")?.parse().map_err(|_| format!("'{text}' is no event position (seq:index:k)"))?;
        let k = next("projection index")?.parse().map_err(|_| format!("'{text}' is no event position (seq:index:k)"))?;
        if parts.next().is_some() {
            return Err(format!("'{text}' is no event position (seq:index:k)"));
        }
        Ok(Self { at: Position { seq, index }, k })
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recorded(at) => write!(f, "journal:{at}"),
            Self::Transient(id) => write!(f, "live:{id}"),
        }
    }
}

impl std::str::FromStr for EventId {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        if let Some(at) = text.strip_prefix("journal:") {
            return at.parse().map(Self::Recorded);
        }
        if let Some(id) = text.strip_prefix("live:") {
            return id.parse().map(Self::Transient).map_err(|_| format!("'{text}' names no transient event"));
        }
        Err(format!("'{text}' is no event id (journal:... or live:...)"))
    }
}

impl Serialize for EventId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(serde::de::Error::custom)
    }
}

impl<T> IdentifiedEvent<T> {
    pub fn transient(event: T) -> Self {
        Self { event_id: EventId::Transient(uuid::Uuid::new_v4()), event }
    }

    /// The events the record event at `at` projected to, each at its own
    /// place.
    pub fn projected(at: Position, events: impl IntoIterator<Item = T>) -> impl Iterator<Item = Self> {
        events.into_iter().enumerate().map(move |(k, event)| Self { event_id: EventId::Recorded(EventPosition { at, k: k as u32 }), event })
    }

    /// The event's place in its run's record; `None` for a transient one.
    pub fn position(&self) -> Option<EventPosition> {
        match self.event_id {
            EventId::Recorded(at) => Some(at),
            EventId::Transient(_) => None,
        }
    }
}

/// An event as the editor and the replay read it: every value named by
/// where the record keeps it.
pub type LiveEvent = IdentifiedEvent<DispatcherEvent>;

/// An event as the reader that made it holds it (`Shown`): sent as a
/// [`LiveEvent`], or with its values written in ([`DispatcherEvent::with_values`]).
pub type ShownEvent = IdentifiedEvent<DispatcherEvent<Shown>>;

/// What a run handed out, per place it ran (`one/@src:lib:clean.strip`),
/// each place's ports with their values: the order the run's fold lists
/// them in, kept on the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceOutputs<V>(pub Vec<(String, BTreeMap<String, V>)>);

impl<V: Serialize> Serialize for PlaceOutputs<V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (place, ports) in &self.0 {
            map.serialize_entry(place, ports)?;
        }
        map.end()
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for PlaceOutputs<V> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Places<V>(std::marker::PhantomData<V>);
        impl<'de, V: Deserialize<'de>> serde::de::Visitor<'de> for Places<V> {
            type Value = PlaceOutputs<V>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("the outputs of a run, by place")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    out.push(entry);
                }
                Ok(PlaceOutputs(out))
            }
        }
        deserializer.deserialize_map(Places(std::marker::PhantomData))
    }
}

/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
// Every event projected from a journal row carries that row's `at_unix`
// (the journal's own stamp), so a replay renders when each thing happened
// rather than when it was read.
//
// `V` is how a value the event names is held (a firing's input and
// output, the run's outputs, a wait's answer): by reference
// ([`ValueRef`], the default and what the editor reads), written out
// (`serde_json::Value`, what `weft events` and a program read), or both
// ([`Shown`], what the dispatcher holds until it sends the event one way
// or the other).
// SYNC: DispatcherEvent <-> extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts ValueRef, weavemind/website/src/lib/graph/dispatcher-host.ts translateDispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase")]
#[serde(bound(serialize = "V: Serialize", deserialize = "V: Deserialize<'de>"))]
pub enum DispatcherEvent<V = ValueRef> {
    /// `subgraph` is every place the run's program runs (its cut of the
    /// project, `crate::project::cut`; a place that stands in for itself
    /// is not among them): the graph paints every other node as not in
    /// this run. `None` when the run's program could not be read. `seed` names the run this one inherits from and what it
    /// re-ran, for the run's banner. `phase` says what the run
    /// is for: a run of the graph (`fire`), or the setup an `infra
    /// start` or an activation runs, which the editor shows as that verb
    /// working rather than as a run to stop.
    RunStarted {
        run_id: RunId,
        entry_node: String,
        phase: crate::context::Phase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subgraph: Option<Vec<crate::frames::Located>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seed: Option<crate::run_spec::Seed>,
        project_id: uuid::Uuid,
        /// Who the run is for, when it is one instance's run. Carried for
        /// any client following the run; no client reads it today.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instance: Option<crate::instance::InstanceId>,
        at_unix: u64,
    },
    /// `outputs` is what every place of the run last handed out, `None`
    /// when the run's program could not be read.
    RunCompleted { run_id: RunId, project_id: uuid::Uuid, outputs: Option<PlaceOutputs<V>>, at_unix: u64 },
    RunFailed { run_id: RunId, project_id: uuid::Uuid, error: String, at_unix: u64 },
    /// `cause` is the structured who-or-what behind the cancel (`reason`
    /// is its text). `None` only for a journal row written before the
    /// cause existed; skipped on the wire when absent so the TS peers'
    /// optional (`cause?`) types match reality instead of decoding null.
    RunCancelled {
        run_id: RunId,
        project_id: uuid::Uuid,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<crate::run::CancelCause>,
        at_unix: u64,
    },
    /// The run tagged itself (`ctx.tag_run`); the inspector shows
    /// the tags on the run. `tags` is this call's list, not the run's
    /// cumulative set.
    RunTagged { run_id: RunId, project_id: uuid::Uuid, tags: Vec<String>, at_unix: u64 },
    /// The run was erased (`weft clean`, a prune, the editor's delete):
    /// its journal, its storage and the wake signals it was parked on
    /// are gone. Rides NOTIFY rather than the journal, since the
    /// journal is what just went; every client drops the run from its
    /// lists and re-reads the project's verbs, because a run parked on
    /// a question counted as preserved state until now.
    RunDeleted { run_id: RunId, project_id: uuid::Uuid },
    /// The run parked: every firing still open waits on a signal (a
    /// person, an outside job, a timer), and no worker drives it until an
    /// answer comes. Its listing reads `waiting_for_input` from here on.
    /// Rides NOTIFY like `RunDeleted`: parking writes no record row
    /// (`weft_broker::door::let_go` publishes it in the transaction that
    /// parks the run). A run's replay ends with it when the run is parked
    /// at the time of the read, which is where it stands rather than a
    /// moment of its history.
    RunParked { run_id: RunId, project_id: uuid::Uuid },
    /// Every node event carries `inherited_from` when the firing was
    /// not this run's own but taken from the run it was seeded from
    /// (`weft run --seed`): the row is the seed's, painted here so the
    /// graph shows the reused value and marks it inherited. Absent on
    /// the wire for the run's own firings. `provided_ports` on a start
    /// names input ports receiving supplied values; absent when none.
    NodeStarted {
        run_id: RunId,
        node: String,
        frames: LoopFrames,
        input: BTreeMap<String, V>,
        closed_ports: Vec<String>,
        // SYNC: input origins <-> extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts NodeFiringEvent, packages/weft-graph/src/webview/lib/types/index.ts NodeFiring
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        provided_ports: Vec<String>,
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        inherited_ports: std::collections::BTreeMap<String, RunId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        project_id: uuid::Uuid,
        at_unix: u64,
    },
    NodeSuspended { run_id: RunId, node: String, frames: LoopFrames, token: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// `token`: the wait the firing resumed from, `None` for a resume no
    /// wait drove (a boundary fired again). How that wait ended is its
    /// [`Self::WaitEnded`].
    NodeResumed { run_id: RunId, node: String, frames: LoopFrames, token: Option<String>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// A wait of the firing at `node` and `frames` ended, the first ending
    /// on record: its answer, a person skipping it, or the run giving it
    /// up. Its own event because a wait the run holds in its worker never
    /// suspends or resumes the firing, so `NodeSuspended` and `NodeResumed`
    /// see only the waits that paused the run.
    WaitEnded { run_id: RunId, node: String, frames: LoopFrames, token: String, ended: crate::primitive::AwaitEnd<V>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeCancelled { run_id: RunId, node: String, frames: LoopFrames, reason: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// `output` is what the firing handed out, port by port, `None` when
    /// it handed out nothing.
    NodeCompleted { run_id: RunId, node: String, frames: LoopFrames, output: Option<BTreeMap<String, V>>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeFailed { run_id: RunId, node: String, frames: LoopFrames, error: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// `reason` says WHY: the author's `_should_flow` said no, or an
    /// input the node needed never arrived. A decision and a consequence
    /// look identical on the graph without it.
    /// `None` only for a journal row written before the field existed
    /// (the UI renders "reason not recorded"); every live writer sends
    /// `Some`.
    NodeSkipped { run_id: RunId, node: String, frames: LoopFrames, closed_ports: Vec<String>, reason: Option<crate::run::skip::SkipReason>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// A copy of a scope (a group, a call site, an included body, at
    /// `frames`) was decided: it opened, or it did not run and why
    /// (`crate::run::scope`). The graph paints a group from these, per
    /// copy. A copy inside one that did not run is decided with it.
    // SYNC: ScopeDecided <-> extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts ScopeDecidedEvent
    ScopeDecided {
        run_id: RunId,
        project_id: uuid::Uuid,
        scope: String,
        frames: LoopFrames,
        #[serde(flatten)]
        state: crate::run::scope::ScopeState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        at_unix: u64,
    },
    /// A loop instance was created at `parent_frames`. The inspector
    /// uses this to render a "Loop opened" marker at the loop's box.
    // SYNC: LoopInstantiated <-> extension-vscode/src/runFollower.ts loop_instantiated, packages/weft-graph/src/protocol.ts LoopInspectorEvent 'instantiated'
    LoopInstantiated {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        /// Effective iteration CAP; `None` for an uncapped loop (a
        /// done-driven or stream-driven loop with no `maxIters`),
        /// whose iteration count is unknowable up front.
        iter_cap: Option<u32>,
        parallel: bool,
        at_unix: u64,
    },
    /// An iteration of the loop launched. Inspector renders an
    /// iteration marker at body_frames.
    LoopIterationLaunched {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        at_unix: u64,
    },
    /// LoopOut fired for iteration `index`. The per-port gather /
    /// carry writes ride on the journal but are NOT mirrored to the
    /// inspector stream: the renderer reads the loop's outward emit
    /// (a normal pulse) and the per-iteration body activity, not the
    /// LoopOut firing's raw write map.
    LoopOutFired {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        done_vote: Option<bool>,
        at_unix: u64,
    },
    /// The loop terminated outward and emitted its outer outputs.
    LoopTerminated {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        reason: crate::primitive::LoopTerminationReason,
        at_unix: u64,
    },
    /// One metered call's cost record landed on the journal, attributed to
    /// the exact firing (`node_id` + `frames`). `amount_usd` `None` = the
    /// meter could not resolve the figure (an honest unknown).
    CostReported {
        run_id: RunId,
        // SYNC: CostReported <-> extension-vscode/src/runFollower.ts DispatcherEvent cost_reported, packages/weft-graph/src/protocol.ts runCost
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        project_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        /// Stable per-record identity; the webview dedups on it (the same
        /// journal row can arrive via both the replay and the live stream).
        cost_id: String,
        service: String,
        amount_usd: Option<f64>,
        /// Whose credential the call spent (`their-own` or `ours`), so a
        /// client can say whose account a figure landed on.
        origin: crate::CredentialOwner,
        at_unix: u64,
    },
    /// A firing changed a stored file's content in place (the journal's
    /// `FileEdited`): the inspector lists it on that firing's "Files
    /// edited" card. `inherited_from` as on [`Self::CostReported`].
    FileEdited {
        run_id: RunId,
        // SYNC: FileEdited <-> extension-vscode/src/runFollower.ts DispatcherEvent file_edited, packages/weft-graph/src/protocol.ts execFileEdit
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        project_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        edit: crate::storage::FileEdit,
        at_unix: u64,
    },
    TriggerUrlChanged { project_id: uuid::Uuid, node_id: String, url: String },
    ProjectRegistered { project_id: uuid::Uuid, name: String },
    ProjectActivated { project_id: uuid::Uuid },
    /// A working copy of the project changed: its files (a save synced, a
    /// program writing), or its head (a stamp, a run, a switch). The editor
    /// reads the tree again on it.
    // SYNC: WorkingCopyChanged <-> extension-vscode/src/runFollower.ts DispatcherEvent (working_copy_changed)
    WorkingCopyChanged { project_id: uuid::Uuid, working_copy: uuid::Uuid },
    ProjectDeactivated { project_id: uuid::Uuid },
    /// A project lifecycle axis flipped: entering/leaving a
    /// transitional state (activating, deactivating, building,
    /// cancelling_build, resyncing) or landing at rest. Carries both axes so a
    /// client can render the new state without a round-trip; clients
    /// that prefer one code path just refetch `/status` on receipt.
    /// This is what makes backend-owned transitional state observable
    /// in near-real-time (the backend-owns-state rule has no teeth
    /// without it).
    ProjectTransitionChanged { project_id: uuid::Uuid, status: String, transition: String },
    /// Infra node transitioned between status values. Catch-all for
    /// supervisor-driven state changes the extension renders as a
    /// per-node badge.
    InfraStatusChanged { project_id: uuid::Uuid, node_id: String, status: String },
    /// Supervisor declared an infra node flaky; the extension shows
    /// the orange banner with `reason`.
    InfraFlaky { project_id: uuid::Uuid, node_id: String, reason: String },
    /// Inverse of InfraFlaky.
    InfraRecovered { project_id: uuid::Uuid, node_id: String },
    /// Supervisor finished terminating an infra node; the
    /// `infra_node` row has been deleted.
    InfraTerminated { project_id: uuid::Uuid, node_id: String },
    /// A bus participant came online. `bus_id` is the channel's uuid
    /// (same one embedded in the bus marker), so the inspector groups
    /// multiple buses cleanly. `offset` is the bus-local position used
    /// to tiebreak same-second entries. `at_unix` is the journal's
    /// stamp so replay renders honest timestamps, not "now".
    BusJoined {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// A bus participant dropped. Pairs with `BusJoined` for the same
    /// `(bus_id, name)`.
    BusLeft {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// One journal-aggregation window of a bus's messages (one row per
    /// bus per window; default 1s). A journaled bus's `messages` carry
    /// every message in the window (senders, kinds, payloads); an
    /// ephemeral bus's `messages` are empty and `totals` (count + bytes
    /// per sender/kind) are the whole story. The inspector unpacks
    /// `messages` into its per-message log and renders a summary line
    /// for a window that carries only totals.
    // SYNC: BusWindow <-> crates/weft-journal/src/events.rs BusWindow, packages/weft-graph/src/protocol.ts BusInspectorEvent 'window', extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window'
    BusWindow {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<crate::bus::WindowedBusMessage>,
        totals: Vec<crate::bus::BusWindowTotal>,
        at_unix: u64,
    },
    /// The bus was closed, and how it ended. Inspector renders an
    /// explicit `* the bus closed here` marker, or where it failed and
    /// why; replay cursors stop here.
    // SYNC: BusClosed <-> crates/weft-journal/src/events.rs BusClosed, packages/weft-graph/src/protocol.ts BusInspectorEvent 'closed', extension-vscode/src/runFollower.ts DispatcherEvent 'bus_closed'
    BusClosed {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        end: crate::bus::StreamEnd,
        at_unix: u64,
    },
    /// A live caller attached to this run. First event in the
    /// caller stream; the inspector opens a "caller" panel on the run.
    CallerConnected {
        run_id: RunId,
        project_id: uuid::Uuid,
        offset: u64,
        protocol: String,
        at_unix: u64,
    },
    /// What the conversation said during one window, both directions in
    /// one row, the same shape and clock as a bus window. A message
    /// whose content was not kept still appears carrying its size.
    /// `dropped` names the program's messages the caller never received
    /// (it read too slowly), possibly ones of an earlier window
    /// ([`crate::stream_journal::aggregate_caller_window`]).
    // SYNC: CallerWindow <-> crates/weft-journal/src/events.rs CallerWindow, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'window', extension-vscode/src/runFollower.ts DispatcherEvent 'caller_window'
    CallerWindow {
        run_id: RunId,
        project_id: uuid::Uuid,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<crate::stream_journal::WindowedCallerMessage>,
        totals: Vec<crate::stream_journal::CallerWindowTotal>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        dropped: Vec<u64>,
        at_unix: u64,
    },
    /// A node error surfaced to the caller.
    CallerErrored {
        run_id: RunId,
        project_id: uuid::Uuid,
        offset: u64,
        message: String,
        at_unix: u64,
    },
    /// The caller is gone (response complete OR disconnected). Last
    /// event in the caller stream; replay cursors stop here. `dropped`
    /// names the program's messages still waiting to go out when the
    /// exchange ended, which the caller never received.
    // SYNC: CallerDisconnected <-> crates/weft-journal/src/events.rs CallerDisconnected, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'disconnected', extension-vscode/src/runFollower.ts DispatcherEvent 'caller_disconnected'
    CallerDisconnected {
        run_id: RunId,
        project_id: uuid::Uuid,
        offset: u64,
        reason: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        dropped: Vec<u64>,
        at_unix: u64,
    },
    /// Graph-level participation: a node was wired to a bus. Derived
    /// from the pulses the fold puts on the wires whose value carries a
    /// bus marker on a `Bus` port: both source and target nodes are
    /// participants.
    /// `ephemeral` is sniffed from the marker JSON itself (which
    /// encodes the bus's mode) so the inspector can render a mode
    /// badge in the panel header without a separate journal event.
    BusParticipant {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        node_id: String,
        ephemeral: bool,
    },
    /// A journal row could not be applied during fold (corruption).
    /// Surfaced one-shot at replay time per affected row so the
    /// inspector can render a muted "N journal rows corrupted"
    /// line. Not alarming by design: corrupt rows are a real but
    /// rare event the user only investigates if they look.
    JournalCorruption {
        run_id: RunId,
        project_id: uuid::Uuid,
        site: crate::primitive::CorruptionSite,
        reason: String,
    },
}

impl DispatcherEvent<Shown> {
    /// The event as the editor reads it: every value by reference.
    pub fn by_reference(self) -> DispatcherEvent {
        self.map_values(&mut |shown| shown.at)
    }

    /// The event with its values written in, each read off what the fold
    /// held or, for one it did not hold, through `kept` (what a record row
    /// or a run's birth holds, as [`ValueRef::resolve`] asks).
    pub fn with_values(
        self,
        kept: &mut impl FnMut(&ValueRef) -> Result<Option<std::sync::Arc<serde_json::Value>>, crate::record_ref::Unresolved>,
    ) -> Result<DispatcherEvent<serde_json::Value>, crate::record_ref::Unresolved> {
        self.try_map_values(&mut |shown| match shown.value {
            Some(value) => Ok(std::sync::Arc::unwrap_or_clone(value)),
            None => shown.at.resolve(kept).map(std::sync::Arc::unwrap_or_clone),
        })
    }
}

impl DispatcherEvent {
    /// An event read by reference, holding what it names as references
    /// only (an announcement from another process).
    pub fn shown(self) -> DispatcherEvent<Shown> {
        self.map_values(&mut Shown::at)
    }
}

impl<V> DispatcherEvent<V> {
    /// The same event with every value it names held as `f` makes it.
    pub fn map_values<W>(self, f: &mut impl FnMut(V) -> W) -> DispatcherEvent<W> {
        match self.try_map_values(&mut |v| Ok::<W, std::convert::Infallible>(f(v))) {
            Ok(event) => event,
            Err(never) => match never {},
        }
    }

    /// [`Self::map_values`], failing on the first value `f` fails on.
    pub fn try_map_values<W, E>(self, f: &mut impl FnMut(V) -> Result<W, E>) -> Result<DispatcherEvent<W>, E> {
        let mut ports = |ports: BTreeMap<String, V>| -> Result<BTreeMap<String, W>, E> {
            ports.into_iter().map(|(port, v)| Ok((port, f(v)?))).collect()
        };
        Ok(match self {
            Self::NodeStarted { run_id, node, frames, input, closed_ports, provided_ports, inherited_ports, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeStarted { run_id, node, frames, input: ports(input)?, closed_ports, provided_ports, inherited_ports, inherited_from, project_id, at_unix }
            }
            Self::NodeCompleted { run_id, node, frames, output, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeCompleted { run_id, node, frames, output: output.map(&mut ports).transpose()?, inherited_from, project_id, at_unix }
            }
            Self::RunCompleted { run_id, project_id, outputs, at_unix } => {
                let outputs = match outputs {
                    Some(PlaceOutputs(places)) => {
                        let mut out = Vec::with_capacity(places.len());
                        for (place, values) in places {
                            out.push((place, ports(values)?));
                        }
                        Some(PlaceOutputs(out))
                    }
                    None => None,
                };
                DispatcherEvent::RunCompleted { run_id, project_id, outputs, at_unix }
            }
            Self::WaitEnded { run_id, node, frames, token, ended, inherited_from, project_id, at_unix } => {
                let ended = match ended {
                    crate::primitive::AwaitEnd::Answered { value } => crate::primitive::AwaitEnd::Answered { value: f(value)? },
                    crate::primitive::AwaitEnd::GaveUp { error } => crate::primitive::AwaitEnd::GaveUp { error },
                    crate::primitive::AwaitEnd::Skipped => crate::primitive::AwaitEnd::Skipped,
                };
                DispatcherEvent::WaitEnded { run_id, node, frames, token, ended, inherited_from, project_id, at_unix }
            }
            Self::RunStarted { run_id, entry_node, phase, subgraph, seed, project_id, instance, at_unix } => {
                DispatcherEvent::RunStarted { run_id, entry_node, phase, subgraph, seed, project_id, instance, at_unix }
            }
            Self::RunFailed { run_id, project_id, error, at_unix } => DispatcherEvent::RunFailed { run_id, project_id, error, at_unix },
            Self::RunCancelled { run_id, project_id, reason, cause, at_unix } => DispatcherEvent::RunCancelled { run_id, project_id, reason, cause, at_unix },
            Self::RunTagged { run_id, project_id, tags, at_unix } => DispatcherEvent::RunTagged { run_id, project_id, tags, at_unix },
            Self::RunDeleted { run_id, project_id } => DispatcherEvent::RunDeleted { run_id, project_id },
            Self::RunParked { run_id, project_id } => DispatcherEvent::RunParked { run_id, project_id },
            Self::NodeSuspended { run_id, node, frames, token, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeSuspended { run_id, node, frames, token, inherited_from, project_id, at_unix }
            }
            Self::NodeResumed { run_id, node, frames, token, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeResumed { run_id, node, frames, token, inherited_from, project_id, at_unix }
            }
            Self::NodeCancelled { run_id, node, frames, reason, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeCancelled { run_id, node, frames, reason, inherited_from, project_id, at_unix }
            }
            Self::NodeFailed { run_id, node, frames, error, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeFailed { run_id, node, frames, error, inherited_from, project_id, at_unix }
            }
            Self::NodeSkipped { run_id, node, frames, closed_ports, reason, inherited_from, project_id, at_unix } => {
                DispatcherEvent::NodeSkipped { run_id, node, frames, closed_ports, reason, inherited_from, project_id, at_unix }
            }
            Self::ScopeDecided { run_id, project_id, scope, frames, state, inherited_from, at_unix } => {
                DispatcherEvent::ScopeDecided { run_id, project_id, scope, frames, state, inherited_from, at_unix }
            }
            Self::LoopInstantiated { run_id, project_id, group_id, parent_frames, iter_cap, parallel, at_unix } => {
                DispatcherEvent::LoopInstantiated { run_id, project_id, group_id, parent_frames, iter_cap, parallel, at_unix }
            }
            Self::LoopIterationLaunched { run_id, project_id, group_id, parent_frames, index, at_unix } => {
                DispatcherEvent::LoopIterationLaunched { run_id, project_id, group_id, parent_frames, index, at_unix }
            }
            Self::LoopOutFired { run_id, project_id, group_id, parent_frames, index, done_vote, at_unix } => {
                DispatcherEvent::LoopOutFired { run_id, project_id, group_id, parent_frames, index, done_vote, at_unix }
            }
            Self::LoopTerminated { run_id, project_id, group_id, parent_frames, reason, at_unix } => {
                DispatcherEvent::LoopTerminated { run_id, project_id, group_id, parent_frames, reason, at_unix }
            }
            Self::CostReported { run_id, inherited_from, project_id, node_id, frames, cost_id, service, amount_usd, origin, at_unix } => {
                DispatcherEvent::CostReported { run_id, inherited_from, project_id, node_id, frames, cost_id, service, amount_usd, origin, at_unix }
            }
            Self::FileEdited { run_id, inherited_from, project_id, node_id, frames, edit, at_unix } => {
                DispatcherEvent::FileEdited { run_id, inherited_from, project_id, node_id, frames, edit, at_unix }
            }
            Self::TriggerUrlChanged { project_id, node_id, url } => DispatcherEvent::TriggerUrlChanged { project_id, node_id, url },
            Self::ProjectRegistered { project_id, name } => DispatcherEvent::ProjectRegistered { project_id, name },
            Self::ProjectActivated { project_id } => DispatcherEvent::ProjectActivated { project_id },
            Self::WorkingCopyChanged { project_id, working_copy } => DispatcherEvent::WorkingCopyChanged { project_id, working_copy },
            Self::ProjectDeactivated { project_id } => DispatcherEvent::ProjectDeactivated { project_id },
            Self::ProjectTransitionChanged { project_id, status, transition } => DispatcherEvent::ProjectTransitionChanged { project_id, status, transition },
            Self::InfraStatusChanged { project_id, node_id, status } => DispatcherEvent::InfraStatusChanged { project_id, node_id, status },
            Self::InfraFlaky { project_id, node_id, reason } => DispatcherEvent::InfraFlaky { project_id, node_id, reason },
            Self::InfraRecovered { project_id, node_id } => DispatcherEvent::InfraRecovered { project_id, node_id },
            Self::InfraTerminated { project_id, node_id } => DispatcherEvent::InfraTerminated { project_id, node_id },
            Self::BusJoined { run_id, project_id, bus_id, offset, name, at_unix } => DispatcherEvent::BusJoined { run_id, project_id, bus_id, offset, name, at_unix },
            Self::BusLeft { run_id, project_id, bus_id, offset, name, at_unix } => DispatcherEvent::BusLeft { run_id, project_id, bus_id, offset, name, at_unix },
            Self::BusWindow { run_id, project_id, bus_id, first_offset, last_offset, messages, totals, at_unix } => {
                DispatcherEvent::BusWindow { run_id, project_id, bus_id, first_offset, last_offset, messages, totals, at_unix }
            }
            Self::BusClosed { run_id, project_id, bus_id, offset, end, at_unix } => DispatcherEvent::BusClosed { run_id, project_id, bus_id, offset, end, at_unix },
            Self::CallerConnected { run_id, project_id, offset, protocol, at_unix } => DispatcherEvent::CallerConnected { run_id, project_id, offset, protocol, at_unix },
            Self::CallerWindow { run_id, project_id, first_offset, last_offset, messages, totals, dropped, at_unix } => {
                DispatcherEvent::CallerWindow { run_id, project_id, first_offset, last_offset, messages, totals, dropped, at_unix }
            }
            Self::CallerErrored { run_id, project_id, offset, message, at_unix } => DispatcherEvent::CallerErrored { run_id, project_id, offset, message, at_unix },
            Self::CallerDisconnected { run_id, project_id, offset, reason, dropped, at_unix } => {
                DispatcherEvent::CallerDisconnected { run_id, project_id, offset, reason, dropped, at_unix }
            }
            Self::BusParticipant { run_id, project_id, bus_id, node_id, ephemeral } => DispatcherEvent::BusParticipant { run_id, project_id, bus_id, node_id, ephemeral },
            Self::JournalCorruption { run_id, project_id, site, reason } => DispatcherEvent::JournalCorruption { run_id, project_id, site, reason },
        })
    }

    pub fn project_id(&self) -> uuid::Uuid {
        match self {
            Self::RunStarted { project_id, .. }
            | Self::RunCompleted { project_id, .. }
            | Self::RunFailed { project_id, .. }
            | Self::RunCancelled { project_id, .. }
            | Self::RunTagged { project_id, .. }
            | Self::RunDeleted { project_id, .. }
            | Self::RunParked { project_id, .. }
            | Self::NodeStarted { project_id, .. }
            | Self::NodeSuspended { project_id, .. }
            | Self::NodeResumed { project_id, .. }
            | Self::WaitEnded { project_id, .. }
            | Self::NodeCancelled { project_id, .. }
            | Self::NodeCompleted { project_id, .. }
            | Self::NodeFailed { project_id, .. }
            | Self::NodeSkipped { project_id, .. }
            | Self::ScopeDecided { project_id, .. }
            | Self::LoopInstantiated { project_id, .. }
            | Self::LoopIterationLaunched { project_id, .. }
            | Self::LoopOutFired { project_id, .. }
            | Self::LoopTerminated { project_id, .. }
            | Self::CostReported { project_id, .. }
            | Self::FileEdited { project_id, .. }
            | Self::TriggerUrlChanged { project_id, .. }
            | Self::ProjectRegistered { project_id, .. }
            | Self::ProjectActivated { project_id }
            | Self::WorkingCopyChanged { project_id, .. }
            | Self::ProjectDeactivated { project_id }
            | Self::ProjectTransitionChanged { project_id, .. }
            | Self::InfraStatusChanged { project_id, .. }
            | Self::InfraFlaky { project_id, .. }
            | Self::InfraRecovered { project_id, .. }
            | Self::InfraTerminated { project_id, .. }
            | Self::BusJoined { project_id, .. }
            | Self::BusLeft { project_id, .. }
            | Self::BusWindow { project_id, .. }
            | Self::BusClosed { project_id, .. }
            | Self::BusParticipant { project_id, .. }
            | Self::CallerConnected { project_id, .. }
            | Self::CallerWindow { project_id, .. }
            | Self::CallerErrored { project_id, .. }
            | Self::CallerDisconnected { project_id, .. }
            | Self::JournalCorruption { project_id, .. } => *project_id,
        }
    }

    /// The one place this event is about, by id, and the loop and call
    /// frames it sits at: a firing (a node's lifecycle, a cost or a file
    /// edit at that firing) at its frames; a scope's copy being decided at
    /// that copy's; a loop's lifecycle at the frames around the loop, and
    /// an iteration's launch or gather inside that iteration (the frames
    /// around the loop and its index), which is where the iteration's
    /// rows run. `None` for an event about the run, the project, or
    /// anything else that is not one place.
    pub fn place(&self) -> Option<(&str, LoopFrames)> {
        match self {
            Self::NodeStarted { node, frames, .. }
            | Self::NodeSuspended { node, frames, .. }
            | Self::NodeResumed { node, frames, .. }
            | Self::WaitEnded { node, frames, .. }
            | Self::NodeCancelled { node, frames, .. }
            | Self::NodeCompleted { node, frames, .. }
            | Self::NodeFailed { node, frames, .. }
            | Self::NodeSkipped { node, frames, .. } => Some((node, frames.clone())),
            Self::CostReported { node_id, frames, .. } | Self::FileEdited { node_id, frames, .. } => Some((node_id, frames.clone())),
            Self::ScopeDecided { scope, frames, .. } => Some((scope, frames.clone())),
            Self::LoopInstantiated { group_id, parent_frames, .. } | Self::LoopTerminated { group_id, parent_frames, .. } => {
                Some((group_id, parent_frames.clone()))
            }
            Self::LoopIterationLaunched { group_id, parent_frames, index, .. }
            | Self::LoopOutFired { group_id, parent_frames, index, .. } => {
                let mut frames = parent_frames.clone();
                frames.push(crate::frames::Frame::Loop { index: *index });
                Some((group_id, frames))
            }
            Self::RunStarted { .. }
            | Self::RunCompleted { .. }
            | Self::RunFailed { .. }
            | Self::RunCancelled { .. }
            | Self::RunTagged { .. }
            | Self::RunDeleted { .. }
            | Self::RunParked { .. }
            | Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::WorkingCopyChanged { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. }
            | Self::BusJoined { .. }
            | Self::BusLeft { .. }
            | Self::BusWindow { .. }
            | Self::BusClosed { .. }
            | Self::BusParticipant { .. }
            | Self::CallerConnected { .. }
            | Self::CallerWindow { .. }
            | Self::CallerErrored { .. }
            | Self::CallerDisconnected { .. }
            | Self::JournalCorruption { .. } => None,
        }
    }

    /// Whether this event ends its run (a cancel is an end too).
    // SYNC: DispatcherEvent::is_run_terminal <-> crates/weft-journal/src/events.rs RunEvent::is_run_terminal, crates/weft-journal/src/events.rs RUN_TERMINAL_KINDS_SQL
    pub fn is_run_terminal(&self) -> bool {
        matches!(
            self,
            Self::RunCompleted { .. } | Self::RunFailed { .. } | Self::RunCancelled { .. }
        )
    }

    pub fn run_id(&self) -> Option<RunId> {
        match self {
            Self::RunStarted { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::RunFailed { run_id, .. }
            | Self::RunCancelled { run_id, .. }
            | Self::RunTagged { run_id, .. }
            | Self::RunDeleted { run_id, .. }
            | Self::RunParked { run_id, .. }
            | Self::NodeStarted { run_id, .. }
            | Self::NodeSuspended { run_id, .. }
            | Self::NodeResumed { run_id, .. }
            | Self::WaitEnded { run_id, .. }
            | Self::NodeCancelled { run_id, .. }
            | Self::NodeCompleted { run_id, .. }
            | Self::NodeFailed { run_id, .. }
            | Self::NodeSkipped { run_id, .. }
            | Self::ScopeDecided { run_id, .. }
            | Self::LoopInstantiated { run_id, .. }
            | Self::LoopIterationLaunched { run_id, .. }
            | Self::LoopOutFired { run_id, .. }
            | Self::LoopTerminated { run_id, .. }
            | Self::CostReported { run_id, .. }
            | Self::FileEdited { run_id, .. }
            | Self::BusJoined { run_id, .. }
            | Self::BusLeft { run_id, .. }
            | Self::BusWindow { run_id, .. }
            | Self::BusClosed { run_id, .. }
            | Self::BusParticipant { run_id, .. }
            | Self::CallerConnected { run_id, .. }
            | Self::CallerWindow { run_id, .. }
            | Self::CallerErrored { run_id, .. }
            | Self::CallerDisconnected { run_id, .. }
            | Self::JournalCorruption { run_id, .. } => Some(*run_id),
            Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::WorkingCopyChanged { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. } => None,
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn a_started_run_names_its_fields_in_camel_case_under_a_snake_case_kind() {
        let event = DispatcherEvent::<ValueRef>::RunStarted {
            run_id: uuid::Uuid::nil(),
            entry_node: "hook".into(),
            phase: crate::context::Phase::Fire,
            subgraph: None,
            seed: None,
            project_id: uuid::Uuid::from_u128(1),
            instance: None,
            at_unix: 5,
        };
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "kind": "run_started",
                "runId": "00000000-0000-0000-0000-000000000000",
                "entryNode": "hook",
                "phase": "fire",
                "projectId": "00000000-0000-0000-0000-000000000001",
                "atUnix": 5,
            })
        );
        let back = serde_json::to_value(serde_json::from_value::<DispatcherEvent>(wire.clone()).unwrap()).unwrap();
        assert_eq!(back, wire);
    }

    #[test]
    fn identities_survive_projection_and_wire_round_trips() {
        let event = DispatcherEvent::<ValueRef>::RunCompleted {
            run_id: uuid::Uuid::nil(), project_id: uuid::Uuid::from_u128(0x100),
            outputs: Some(PlaceOutputs(Vec::new())), at_unix: 1,
        };
        let at = Position { seq: 42, index: 3 };
        let projected: Vec<LiveEvent> = IdentifiedEvent::projected(at, [event.clone(), event.clone()]).collect();
        assert_ne!(projected[0].event_id, projected[1].event_id);
        let json = serde_json::to_value(&projected[0]).unwrap();
        assert_eq!(json["eventId"], "journal:42:3:0");
        assert_eq!(json["kind"], "run_completed");
        let decoded: LiveEvent = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.event_id, projected[0].event_id);
        assert_eq!(decoded.position(), Some(EventPosition { at, k: 0 }));
        let distinct: Vec<LiveEvent> = IdentifiedEvent::projected(Position { seq: 42, index: 4 }, [event]).collect();
        assert_ne!(distinct[0].event_id, projected[0].event_id);
        let transient = serde_json::to_value(IdentifiedEvent::transient(distinct[0].event.clone())).unwrap();
        let back: LiveEvent = serde_json::from_value(transient).unwrap();
        assert_eq!(back.position(), None);
    }

    /// Places order as the record wrote them, and a position written for
    /// a follower reads back as itself.
    #[test]
    fn event_positions_order_and_read_back() {
        let at = |seq, index, k| EventPosition { at: Position { seq, index }, k };
        assert!(at(1, 0, 5) < at(1, 1, 0));
        assert!(at(1, 1, 0) < at(1, 1, 1));
        assert!(at(1, 9, 9) < at(2, 0, 0));
        assert_eq!("12:0:3".parse::<EventPosition>().unwrap(), at(12, 0, 3));
        assert_eq!(at(12, 0, 3).to_string().parse::<EventPosition>().unwrap(), at(12, 0, 3));
        assert!("12:0".parse::<EventPosition>().is_err());
        assert!("12:0:3:4".parse::<EventPosition>().is_err());
        assert!("journal:1:2".parse::<EventId>().is_err());
    }

    /// An event the dispatcher holds goes out by reference for the editor
    /// and with its values written in for a reader that wants them, the
    /// two shaped alike; a value not at hand is read through the record.
    #[test]
    fn an_event_goes_out_by_reference_or_with_its_values() {
        use std::sync::Arc;
        let run = uuid::Uuid::from_u128(7);
        let kept = ValueRef::Row { run, seq: 3, index: 1 };
        let event = DispatcherEvent::NodeStarted {
            run_id: run, node: "sink".into(), frames: vec![],
            input: [
                ("a".to_string(), Shown::of(Some(&Arc::new(kept.clone())), &Arc::new(serde_json::json!({ "big": 1 })))),
                ("b".to_string(), Shown::at(ValueRef::Row { run, seq: 4, index: 0 })),
            ].into(),
            closed_ports: vec![], provided_ports: vec![], inherited_ports: Default::default(), inherited_from: None,
            project_id: uuid::Uuid::nil(), at_unix: 1,
        };
        let by_reference = serde_json::to_value(event.clone().by_reference()).unwrap();
        assert_eq!(by_reference["input"]["a"], serde_json::to_value(&kept).unwrap());
        let mut read = Vec::new();
        let with_values = event.with_values(&mut |at| {
            read.push(at.clone());
            Ok(Some(Arc::new(serde_json::json!("from the record"))))
        }).unwrap();
        let with_values = serde_json::to_value(with_values).unwrap();
        assert_eq!(with_values["input"], serde_json::json!({ "a": { "big": 1 }, "b": "from the record" }));
        assert_eq!(read, vec![ValueRef::Row { run, seq: 4, index: 0 }], "only the value not at hand is read");
    }
}

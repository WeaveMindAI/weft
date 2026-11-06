#[cfg(feature = "runtime")]
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RunId;

// ----- Wake signals (unified trigger + suspension mechanism) ----------
//
// A wake signal is "something the listener listens for on behalf of
// a node." When it fires, the dispatcher either spawns a fresh run
// (entry path: `register_signal`) or resumes a paused firing (resume
// path: a wait, `state.wait`). The kind doesn't know which; the
// dispatcher's `PrepareRequest` and the journal's signal row carry
// that lifecycle metadata.
//
// `SignalSpec` is the wire shape: `kind` is a string tag and
// `config` is an opaque JSON blob owned by that kind. Per-kind data
// types and the `Signal` trait live in `crate::signal`; node code
// constructs kinds there and passes them straight to
// `ctx.register_signal(kind)` / `state.wait(field, kind)`. The
// framework projects the typed kind onto this wire shape via
// `signal::to_spec`. Authors never see `SignalSpec` directly.
//
// Adding a kind = one file in `weft-core/src/signal/<name>.rs` plus
// one file in `weft-listener/src/kinds/<name>.rs`. No central enum
// or match dispatch.

/// A wake-signal instance ready to be registered. Per-kind config
/// only; lifecycle metadata (entry vs resume, owning run)
/// rides the dispatcher's `PrepareRequest`, not the spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalSpec {
    /// Kind tag (e.g. `"route"`, `"timer"`). Matched against the
    /// inventory of `SignalKind` impls; unknown tags fail validation.
    pub kind: String,
    /// Kind-specific configuration. Each kind owns its shape.
    #[serde(default)]
    pub config: Value,
    /// Optional consumer label. Set by nodes whose suspensions are
    /// processed by an external consumer (browser extension, etc).
    /// Token-scoped enumeration filters by this field. Charset is
    /// `[A-Za-z0-9_-]{1,64}` (validated in `signal::to_spec`).
    #[serde(default, rename = "consumerKind", alias = "consumer_kind", skip_serializing_if = "Option::is_none")]
    pub consumer_kind: Option<String>,
    /// The connection this signal acts AS, when it needs one: an
    /// authed poll, a subscribed stream, a socket whose URL is minted
    /// by an authenticated call. Kind-independent on purpose: the
    /// listener resolves it through the broker at every use (each poll
    /// cycle, each reconnect) and applies the service's auth steps to
    /// whatever outbound call the kind makes, so no kind reimplements
    /// signing in and no credential is ever frozen onto a row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<AccessRef>,
    /// Pre-fire predicates over the payload the kind produced.
    /// Evaluated by shared plumbing between "the kind has a payload"
    /// and "a fire is enqueued", for every kind: filtering is one
    /// concept, not a per-kind feature. Empty/absent = fire on
    /// everything.
    #[serde(default, rename = "match", skip_serializing_if = "Vec::is_empty")]
    pub match_predicates: Vec<crate::signal::Predicate>,
    /// How often and how many at once outside callers may use this
    /// entry, when it is on a public URL (`crate::signal::limits`).
    /// Kind-independent like `match`: the dispatcher enforces it for
    /// every public entry. Default = the language defaults.
    #[serde(default, skip_serializing_if = "crate::signal::EntryLimits::is_default")]
    pub limits: crate::signal::EntryLimits,
    /// How the runs this signal starts are kept
    /// (`crate::run_settings`): fast or durable, recorded or not. Set by whatever registers the signal; written
    /// beside the other fields, each default left out.
    #[serde(flatten)]
    pub settings: crate::run_settings::RunSettings,
    /// The last call a run's wait sends if its run is cancelled while it
    /// waits (`crate::signal::cancel_call`). Kind-independent like
    /// `access`, which it is sent as. A run's wait only: an entry trigger
    /// has no run to cancel, and its registration is refused.
    #[serde(default, rename = "onCancel", skip_serializing_if = "Option::is_none")]
    pub on_cancel: Option<crate::signal::CancelCall>,
    /// The wait this signal is one branch of, when its run waits on the
    /// first of several (`crate::signal::first_of`): set by the
    /// dispatcher as it splits that wait into its branches, never by a
    /// node. Kind-independent like `onCancel`, and a run's wait only.
    #[serde(default, rename = "branchOf", skip_serializing_if = "Option::is_none")]
    pub branch_of: Option<crate::signal::first_of::BranchOf>,
}

/// Preparing an entry captures it; waiting on a signal registers a live
/// token. The asking worker writes what the answer says into its run's
/// record (`TriggerCaptured`, `SuspensionRegistered`): only the worker
/// driving a run writes its record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegisterSignalResult {
    /// The entry's parameters were captured for the trigger at this
    /// address (the place the capture is stored under).
    Captured { node_id: String },
    Registered { token: String },
}

/// The steps a stored [`SignalSpec`] takes from the shape an older weft
/// wrote; `crate::stored` says how they run. A document holding specs
/// elsewhere (a trigger's bake, a run's record) applies
/// [`SIGNAL_SPEC_V2`] where each sits.
pub const SIGNAL_SPEC_UPGRADES: &[&[crate::stored::Op]] = &[SIGNAL_SPEC_V2];

/// Version 2: a live caller's gate names its check
/// (`crate::signal::auth::auth_to_v2`); only a live-caller kind's config
/// has an `auth`.
pub const SIGNAL_SPEC_V2: &[crate::stored::Op] =
    &[crate::stored::Op::Within { at: "config", ops: crate::signal::auth::AUTH_V2 }];

impl SignalSpec {
    /// A registered signal's spec, as its row stores it (`signal.spec_json`).
    pub const STORED: crate::stored::Kind<SignalSpec> = crate::stored::Kind::new("signal spec", SIGNAL_SPEC_UPGRADES);

    /// A spec carrying only a kind tag and its config: no connection,
    /// no filter, no consumer label. The shape most callers building a
    /// wire spec by hand want, and the one place the "everything else
    /// is optional" default is written down.
    pub fn of_kind(kind: impl Into<String>, config: Value) -> Self {
        Self {
            kind: kind.into(),
            config,
            consumer_kind: None,
            access: None,
            match_predicates: Vec::new(),
            limits: crate::signal::EntryLimits::default(),
            settings: crate::run_settings::RunSettings::default(),
            on_cancel: None,
            branch_of: None,
        }
    }
}

/// The stored connection a signal acts as; see [`SignalSpec::access`].
/// Exactly the reference an [`crate::access::Access`] value carries
/// (id + service), never a credential: the listener re-resolves it per
/// use, behind the tenant wall.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessRef {
    pub id: String,
    pub service: String,
    /// The stored values the trigger's input declared it needs
    /// (`requiresValues`), carried so the listener's resolve refuses a
    /// connection missing one instead of dialing with a blank address.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_values: Vec<String>,
}

impl From<&crate::access::Access> for AccessRef {
    fn from(access: &crate::access::Access) -> Self {
        Self {
            id: access.access_id().to_string(),
            service: access.service().to_string(),
            required_values: access.required_values().to_vec(),
        }
    }
}

/// Where the signal lives on the public HTTP surface.
///
/// Two orthogonal axes drive how a signal is exposed externally:
/// SignalSurface (this enum) and `SignalAuth` (below). The kind
/// impl in the catalog returns one of each at register time; the
/// dispatcher copies them onto the `signal` row and the public
/// router uses them to dispatch incoming HTTP.
///
/// New surface kinds extend this enum without touching dispatcher
/// or listener routing logic: routing is generic in the surface
/// kind, the kind impl picks one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalSurface {
    /// Author-controlled HTTP entrypoint. Mounted under the tenant and
    /// the project at the dispatcher root
    /// ([`crate::route::SharedMount`]): external callers reach it at
    /// `<dispatcher_base>/connect/<tenant>/<project>/<path>` (a live route)
    /// or `POST <dispatcher_base>/<tenant>/<project>/<path>` (a plain
    /// public fire).
    /// `path` is a route pattern (`chat/{room}`); `""` means the root.
    /// `methods` lists the HTTP methods served, uppercase; empty = any.
    /// Two routes of one project may not overlap (same shape, a shared
    /// method), checked at register time. Used by the live-caller kinds
    /// (Route, Socket) and any future public-form-like kind.
    PublicEntry {
        path: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        methods: Vec<String>,
    },
    /// Per-task callback. Mounted at `/signal/<token>` where the
    /// dispatcher mints the UUID at register time. Used by
    /// task-style signals (HumanQuery, future task-callback kinds)
    /// where each fire is a one-shot reply to a specific suspended
    /// firing and the URL is internal to the consumer flow.
    TaskCallback,
    /// Internal: no mount path of its own. The signal fires from
    /// inside the listener (a timer expires, an SSE event arrives),
    /// and `weft_listener::fire_sink` hands it to its project's worker,
    /// or, if it answers a waiting run, enqueues a `FireSignal` task.
    /// Used by the kinds that watch or hold something: timer, poll, SSE,
    /// stream listen, socket listen, and provider events (whose webhook
    /// pushes arrive at the dispatcher's `/events/...` instead, and the
    /// dispatcher asks the listener to match them).
    Internal,
}

/// Authentication policy for the public HTTP surface. Independent
/// of `SignalSurface`: any surface kind can pick any auth kind.
///
/// Marker enum: it identifies the gate to run. The kind-specific
/// configuration lives in the `auth_config` JSON blob alongside,
/// parsed by the dispatcher's gate at connect time. New auth kinds:
/// add a variant here, add a match arm in the dispatcher's
/// `caller_gate`, decide what shape goes in `auth_config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalAuth {
    /// Open. Anyone with the URL can fire. Suitable for raw
    /// webhooks where the URL itself is the secret.
    None,
    /// The caller is checked against a stored connection with the
    /// trigger's own scheme. `auth_config` shape:
    /// `{ access_id, service, verify }` (no `verify`: a gate whose check
    /// was not kept, refused at the door). No secret is ever on the
    /// row: the broker holds the connection and answers the check.
    Connection,
}

impl SignalAuth {
    /// Discriminant string for the `signal.auth_kind` column.
    pub fn kind_tag(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Connection => "connection",
        }
    }
}

impl SignalSurface {
    /// Discriminant string for the `signal.surface_kind` column.
    pub fn kind_tag(&self) -> &'static str {
        match self {
            Self::PublicEntry { .. } => "public_entry",
            Self::TaskCallback => "task_callback",
            Self::Internal => "internal",
        }
    }
}

/// Listener-computed routing + auth metadata returned from
/// `/prepare`. The dispatcher copies these fields onto the
/// signal row.
///
/// `auth_config` is a kind-specific JSON blob the dispatcher's
/// auth gate parses according to `auth.kind_tag()`. For
/// `Connection` the blob is `{access_id, service, verify}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRouting {
    pub surface: SignalSurface,
    pub auth: SignalAuth,
    /// What the gate needs to run the check. Plaintext secrets
    /// NEVER appear here.
    #[serde(default)]
    pub auth_config: Value,
}

#[cfg(feature = "runtime")]
impl SignalRouting {
    /// The routing a public-entry kind computes from its auth policy:
    /// the ONE mapping from [`crate::signal::PublicEntryAuth`] onto the
    /// row's `(auth_kind, auth_config)`, so every kind exposed on a
    /// public URL gates callers the same way.
    pub fn public_entry(
        surface: SignalSurface,
        auth: &crate::signal::PublicEntryAuth,
    ) -> Self {
        match auth {
            crate::signal::PublicEntryAuth::None => Self {
                surface,
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
            crate::signal::PublicEntryAuth::Connection { access_id, service, verify } => Self {
                surface,
                auth: SignalAuth::Connection,
                auth_config: serde_json::json!({
                    "access_id": access_id,
                    "service": service,
                    "verify": verify,
                }),
            },
            // No `verify`: the door refuses every caller, naming the fix.
            crate::signal::PublicEntryAuth::CheckNotKept { access_id, service } => Self {
                surface,
                auth: SignalAuth::Connection,
                auth_config: serde_json::json!({
                    "access_id": access_id,
                    "service": service,
                }),
            },
        }
    }
}

// ----- Run snapshot ---------------------------------------------

/// A run's state as the journal fold rebuilds it: the pulse
/// table, the per-node firing records, the loop instances, the
/// kicks, and the active suspensions. Everything a fresh worker needs
/// to resume; the fold is the only writer, and a resume never reads
/// anything the fold did not derive from the journal's rows plus the
/// program. Never persisted: replay is the source of truth.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone)]
pub struct RunSnapshot {
    pub run_id: RunId,
    /// Every scope copy decided so far (`crate::run::scope`): derived
    /// from the pulses, never from a row of its own.
    pub scopes: crate::run::scope::ScopeTable,
    /// The worker binary the run was born on (`RunStarted`): with
    /// the definition hash it names the program identity, read from the
    /// project's stored code when reuse needs it.
    pub binary_hash: Option<String>,
    /// The hash of the program the run runs (`RunStarted`): its cut, for
    /// a run of a cut.
    pub definition_hash: Option<String>,
    /// The registered program the run's own was cut from (`RunStarted`),
    /// for a run of a cut: the program its results are held against when
    /// a seed reuses them.
    pub cut_from: Option<String>,
    /// What the run is given at the places its program says (`RunStarted`).
    pub given: std::sync::Arc<crate::project::Given>,
    /// Chosen history includes bodies of zero-iteration loops, which have no
    /// firing record on which to store an origin.
    pub inherited_origins: std::collections::BTreeMap<crate::frames::Located, RunId>,
    pub pulses: crate::pulse::PulseTable,
    pub firings: crate::run::NodeFiringTable,
    /// What the run's tables forget once nothing can need it
    /// (`crate::run::forget`), for a holder that forgets; `None` for one
    /// that keeps everything (a seed's reconstruction, a node's states).
    pub forgetting: Option<crate::run::forget::Forgetting>,
    pub suspensions: HashMap<String, SuspensionInfo>,
    /// Every loop instance, as the fold drove it from the loop rows.
    /// A resumed worker takes it over as its own runtime.
    pub loop_runtime: crate::run::loop_runtime::LoopRuntime,
    /// Nodes kicked into this run, keyed by where they fire. The
    /// scheduler dispatches a kicked node once even when it has no
    /// wired pending inputs (it IS an entry point). Folded from
    /// `RunEvent::NodeKicked` (the run's roots, at frames `[]`), from
    /// a scope's copy opening (its members no wire feeds, at its frames)
    /// and from `RunEvent::LoopIterationLaunched` (a loop body's roots,
    /// at the iteration's frames). `dispatched=true` once
    /// the engine has consumed the kick (the node started at that
    /// location; further kicks at the same location are a no-op).
    pub kicked: KickTable,
    /// Wait endings on record before their wait's registration (an
    /// answer the dispatcher wrote while nobody drove the run): the
    /// registration takes its ending from here, so the two rows fold the
    /// same in either order.
    pub pending_deliveries: HashMap<String, AwaitEnd>,
    /// Per firing, every wait it registered, by number
    /// ([`RecordedWait`]).
    pub recorded_waits: HashMap<crate::frames::FiringLocation, Vec<RecordedWait>>,
    /// Per firing, the last state it saved (`RunEvent::NodeStateSaved`):
    /// what its body gets back when it runs again.
    pub saved_states: HashMap<crate::frames::FiringLocation, crate::run::saved::SavedState>,
    /// Every bus the run created, open or ended, by id: what a worker
    /// that picks the run up after a pause opens again before any body
    /// runs, so a marker handed out before the pause names a bus after it
    /// too, open or ended as it was.
    pub buses: std::collections::BTreeMap<uuid::Uuid, RecordedBus>,
    /// Journal rows the fold could not apply because they were
    /// corrupted (unparseable UUID, broken invariants, a row the
    /// program cannot make sense of). Empty in the normal case.
    /// Surfaced to the inspector so the user sees "row N corrupted"
    /// instead of a silently missing pulse; the engine refuses to
    /// resume over any of them. The fold ALSO logs each corruption at
    /// `error!` level for ops observability.
    pub corruptions: Vec<JournalCorruption>,
}

/// One journal row the fold could not apply. The `site` names the
/// fold step that rejected the row (a closed enum over every fold
/// branch that can fail); `reason` says which field was malformed
/// and how. Carries no in-band data because corruption means the
/// data is unusable; the user's recovery is to investigate the
/// journal directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalCorruption {
    pub site: CorruptionSite,
    /// Which field of the row was malformed (e.g. "pulse_id",
    /// "absorbed_pulse_ids[0]") and the parse error or invariant
    /// violation.
    pub reason: String,
}

/// A bus the run created, as its record tells it (`RunEvent::BusCreated`
/// and the bus rows after it): everything a worker needs to open it again
/// where it stood, with the same id, so the markers already handed out
/// keep naming it, and, once it ended, how it ended.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedBus {
    /// The firing that created it, and the output port its marker goes
    /// out on: a resumed body creating its bus on that port gets this one.
    pub creator: crate::frames::FiringLocation,
    pub port: String,
    pub payload: crate::bus::BusPayloadKind,
    pub meta: Value,
    pub window: usize,
    pub journal: crate::stream_journal::JournalPolicy,
    /// The offset its next entry lands at.
    pub next_offset: u64,
    /// Every name that ever joined it.
    pub joined: std::collections::BTreeSet<String>,
    /// Who is on it now: every name whose `BusJoined` has no `BusLeft`
    /// after it. A bus opened again after a pause starts with exactly
    /// these on it, each the name of a firing that paused.
    pub members: std::collections::BTreeSet<String>,
    /// How the last name to leave it left (its last `BusLeft`), `None`
    /// while none has: how a bus nobody is on ends once no node can still
    /// join it.
    pub last_left: Option<crate::bus::StreamEnd>,
    /// The last messages a journaled bus kept whole on its record (at
    /// most `window`), oldest first, every one after `lost_through`: what
    /// a reader reaching back finds again. Empty for an ephemeral bus,
    /// whose record keeps no content.
    pub retained: std::collections::VecDeque<crate::bus::WindowedBusMessage>,
    /// One past the newest message its record cannot hand back whole
    /// (every message of an ephemeral bus; one a journaled bus cut short,
    /// kept without its bytes, or let fall out of `window`): a reader that
    /// had not read it cannot read on as if it had.
    pub lost_through: u64,
    /// How it ended (`RunEvent::BusClosed`), `None` while it is open. An
    /// ended bus keeps its last window of messages like an open one: a
    /// reader given its marker after the pause reads them and comes to
    /// this end, as it would have before the pause.
    pub end: Option<crate::bus::StreamEnd>,
}

/// Closed enum over the places a journal row can be rejected: every
/// fold branch that can fail, plus the row that does not decode at
/// all. Adding a new rejecting site forces adding a variant here; that
/// is the point. Serialised as the variant name on the wire so the
/// inspector renders a stable label.
// SYNC: CorruptionSite <-> packages/weft-graph/src/protocol.ts CorruptionSite
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CorruptionSite {
    /// `RunEvent::PortEmitted` could not be put on the wires (the
    /// program has no such node or port, or a wire cannot read the
    /// value).
    PortEmitted,
    /// `RunEvent::PortClosed` names a port the program does not
    /// declare.
    PortClosed,
    /// `RunEvent::PulsesConsumed` fold path (`parse_absorbed_ids` on
    /// `pulse_ids`).
    PulsesConsumed,
    /// A node lifecycle row (`NodeStarted`, a terminal) names a node
    /// the program does not have, or a firing the fold has no record
    /// for.
    NodeLifecycle,
    /// `RunEvent::LoopInstantiated` arrived for a LoopIn the program
    /// does not have, or its config and inputs do not make a loop.
    LoopInstantiated,
    /// `RunEvent::LoopStreamEnded` arrived for a `LoopInstanceKey`
    /// with no preceding `LoopInstantiated`. Writer-order bug or row
    /// loss.
    LoopStreamEnded,
    /// `LoopIterationLaunched` arrived for a `LoopInstanceKey` with no
    /// preceding `LoopInstantiated`, or the launch cannot be rebuilt.
    LoopIterationLaunched,
    /// `LoopOutFired` arrived for a `LoopInstanceKey` with no preceding
    /// `LoopInstantiated`, or its writes cannot be read off the LoopOut
    /// firing.
    LoopOutFired,
    /// `LoopTerminated` arrived for a `LoopInstanceKey` with no preceding
    /// `LoopInstantiated`, or its outward emit cannot be rebuilt.
    LoopTerminated,
    /// `RunEvent::RunStarted` gives the run values at places its program
    /// does not say it is given something, or misses one it says
    /// (`weft_core::project::Given::check_against`).
    RunGiven,
    /// A bus row (`RunEvent::BusCreated` and the rows after it) names its
    /// bus by an id that is no bus id.
    Bus,
    /// A journal row whose JSON no longer decodes to any `RunEvent`
    /// at all (the display read surfaces it; state-rebuilding reads
    /// refuse the whole log instead).
    UndecodableRow,
    /// Not one row but the whole run: the code it ran cannot be found,
    /// so nothing that needs the program can be worked out. Every value
    /// is missing at once, which is why it is its own site: it belongs
    /// beside the run rather than inside one node.
    MissingProgram,
}

/// One wait a firing registered, by its number: the wait's token and
/// how it ended (`None` while it is open). Rebuilt by the fold from
/// `SuspensionRegistered` and the rows that end a wait. A resumed firing
/// reads the ending of the wait it paused in here
/// (`crate::context::Kept::wait`), a seeded firing the answers its seed
/// was given ([`answered_waits`]), and the node-status view the wait a
/// firing is parked on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedWait {
    /// The wait's number within its firing (`crate::run::saved::Counters::waits`).
    pub wait_index: u32,
    pub token: String,
    /// The signal kind it waited on (`SignalSpec::kind`).
    pub kind: String,
    /// `None` while the wait is open.
    pub ended: Option<AwaitEnd>,
}

/// Refuse a body that asks, under a wait number its record already holds,
/// a wait of another kind than the one recorded there: it did not come
/// back to where it was, so its state does not say where the node is.
pub fn check_wait_kind(recorded: &RecordedWait, asked: &SignalSpec, node: &str) -> crate::error::WeftResult<()> {
    if recorded.kind == asked.kind {
        return Ok(());
    }
    Err(crate::error::WeftError::NodeFailed(format!(
        "node '{node}' waited for a '{}' signal as its wait #{n}, but its record holds wait #{n} as a '{}' signal: \
         a node's saved state must say where it was, so that its body, run again from the top, comes back to the \
         waits it made in the same order (keep what each step decided in the state, and skip what it says is done)",
        asked.kind,
        recorded.kind,
        n = recorded.wait_index,
    )))
}

/// How a past wait (`state.wait`) ended. The first ending on record
/// stands: an answer that comes after a wait was given up is ignored.
/// `V` is how the answer is held: the value itself, or, in an event that
/// names it, where the record keeps it (`crate::live_event::DispatcherEvent`).
// SYNC: AwaitEnd <-> extension-vscode/src/runFollower.ts AwaitEnd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AwaitEnd<V = Value> {
    /// Its answer arrived (`SuspensionResolved`).
    Answered { value: V },
    /// The run held it in its worker and gave it up
    /// (`SuspensionGaveUp`): the call failed with `error`.
    GaveUp { error: String },
    /// A person skipped it (`SuspensionSkipped`): the call ends its node's
    /// firing skipped ([`crate::error::WeftError::WaitSkipped`]).
    Skipped,
}

impl<V> AwaitEnd<V> {
    /// The same ending with its answer held as `f` makes it.
    pub fn map<W>(self, f: impl FnOnce(V) -> W) -> AwaitEnd<W> {
        match self {
            Self::Answered { value } => AwaitEnd::Answered { value: f(value) },
            Self::GaveUp { error } => AwaitEnd::GaveUp { error },
            Self::Skipped => AwaitEnd::Skipped,
        }
    }
}

/// What answers a wait: a value, or a person skipping it (the `/skip`
/// door). One type from the door to the waiting call, so a skip is never
/// spelled as a value a real answer could also be.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitAnswer {
    Given { value: Value },
    Skipped,
}

impl WaitAnswer {
    /// How the wait it answers ends.
    pub fn end(&self) -> AwaitEnd {
        match self {
            Self::Given { value } => AwaitEnd::Answered { value: value.clone() },
            Self::Skipped => AwaitEnd::Skipped,
        }
    }
}

/// The error a waiting call returns when a person skipped it.
pub fn wait_skipped() -> crate::error::WeftError {
    crate::error::WeftError::WaitSkipped("a person skipped what this node was waiting for".into())
}

/// The waits of a firing a new firing of the same code may take the
/// answers of (a seeded run re-running a node, `weft run --seed`): in
/// number order from the first, every wait somebody answered, up to the
/// first that was not (still open, given up, skipped) or a gap in the
/// numbers. The new firing's wait of the same number takes that answer
/// at once; the first one past them asks again.
pub fn answered_waits(waits: &[RecordedWait]) -> Vec<RecordedWait> {
    let mut ordered: Vec<&RecordedWait> = waits.iter().collect();
    ordered.sort_by_key(|wait| wait.wait_index);
    ordered
        .into_iter()
        .enumerate()
        .take_while(|(at, wait)| wait.wait_index as usize == *at && matches!(wait.ended, Some(AwaitEnd::Answered { .. })))
        .map(|(_, wait)| wait.clone())
        .collect()
}

/// Per-paused-(node, frames) info stored in the snapshot. `token` is
/// the key in the outer HashMap. Enough to: identify the waiting
/// node/frames, re-register the signal on every fresh worker boot,
/// and route the delivered value back to the right oneshot when the
/// fire arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuspensionInfo {
    pub node_id: String,
    pub frames: crate::frames::LoopFrames,
    pub spec: SignalSpec,
    pub created_at_unix: u64,
    /// The wait's number within its firing
    /// (`crate::run::saved::Counters::waits`).
    pub wait_index: u32,
}

/// One kicked root node in a folded snapshot. A kicked node is an
/// entry point of a fresh run (a firing trigger, a manual-run
/// root, an InfraSetup root) that has no wired pending inputs and so
/// would never become ready on its own. The scheduler dispatches it
/// once, at the frames its key names. The optional `payload` carries
/// the wake event's data for the firing trigger (the HTTP body, the SSE
/// event JSON, the form submission, the timer info); node bodies read
/// it via the `ctx.wake` bag in Fire phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KickedNode {
    /// This kick IS the firing trigger of the run. Explicit,
    /// never inferred from payload presence: a fire with an empty
    /// body has a `null` payload, which `Option<Value>` cannot keep
    /// apart from "no payload" across a JSON round trip.
    #[serde(default)]
    pub firing: bool,
    /// Wake event payload for the firing trigger. `None` for every
    /// other kicked root.
    pub payload: Option<Value>,
    /// The firing trigger's setup-time port snapshot, seeded onto its
    /// ports at dispatch. `None` for every other kicked root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_snapshot: Option<Value>,
    /// Flips to `true` once the engine has dispatched this kick (the
    /// node started at the kick's frames). A second tick that sees
    /// `dispatched` must NOT re-dispatch. Changed through the table it
    /// sits in ([`KickTable::set_dispatched`]), which keeps the kicks
    /// not dispatched yet.
    #[serde(default)]
    dispatched: bool,
    /// The scope this kick belongs to was gated off: the node is
    /// dispatched straight into a `ScopeSkipped` skip, so every node
    /// inside a scope that did not run says so in the journal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_skipped: Option<String>,
}

impl KickedNode {
    /// A kick not dispatched yet.
    pub fn new(firing: bool, payload: Option<Value>, port_snapshot: Option<Value>, scope_skipped: Option<String>) -> Self {
        Self { firing, payload, port_snapshot, dispatched: false, scope_skipped }
    }

    pub fn dispatched(&self) -> bool {
        self.dispatched
    }

    /// What the body of the node this kick dispatches reads as its wake
    /// (`ctx.wake`): the firing trigger's payload (`null` for a fire with
    /// an empty body), or the payload a manual run set on a plain root.
    pub fn wake_payload(&self) -> Option<Value> {
        if self.firing {
            Some(self.payload.clone().unwrap_or(Value::Null))
        } else {
            self.payload.clone()
        }
    }
}

/// A run's kicks, by where they fire, and which of them are not
/// dispatched yet: what every turn of a drive dispatches from, without
/// walking the kicks it dispatched before (a loop body's roots are kicked
/// once per iteration). A kick's `dispatched` changes only here
/// ([`Self::set_dispatched`]), which keeps the two in step.
#[derive(Debug, Clone, Default)]
pub struct KickTable {
    kicks: std::collections::HashMap<crate::frames::FiringLocation, KickedNode>,
    undispatched: std::collections::BTreeSet<crate::frames::FiringLocation>,
}

impl KickTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// The kick at `at`, made by `kick` when there is none. Answers it,
    /// and whether it was just made: a later kick at a kicked location
    /// keeps the first one (first writer wins), and only changes what the
    /// caller changes on the answer.
    pub fn kick(&mut self, at: crate::frames::FiringLocation, kick: impl FnOnce() -> KickedNode) -> (&mut KickedNode, bool) {
        match self.kicks.entry(at) {
            std::collections::hash_map::Entry::Occupied(found) => (found.into_mut(), false),
            std::collections::hash_map::Entry::Vacant(free) => {
                let made = kick();
                if !made.dispatched {
                    self.undispatched.insert(free.key().clone());
                }
                (free.insert(made), true)
            }
        }
    }

    /// The kick at `at`, to change anything but whether it is dispatched.
    pub fn get_mut(&mut self, at: &crate::frames::FiringLocation) -> Option<&mut KickedNode> {
        self.kicks.get_mut(at)
    }

    /// Mark the kick at `at` dispatched (its node started there), or not
    /// (it is to be dispatched again). Nothing when there is no kick there.
    pub fn set_dispatched(&mut self, at: &crate::frames::FiringLocation, dispatched: bool) {
        let Some(kick) = self.kicks.get_mut(at) else { return };
        kick.dispatched = dispatched;
        if dispatched {
            self.undispatched.remove(at);
        } else {
            self.undispatched.insert(at.clone());
        }
    }

    /// Where the kicks not dispatched yet are, in place order.
    pub fn undispatched(&self) -> impl Iterator<Item = &crate::frames::FiringLocation> {
        self.undispatched.iter()
    }

    /// Whether a kick is not dispatched yet.
    pub fn any_undispatched(&self) -> bool {
        !self.undispatched.is_empty()
    }

    /// Drop every kick `gone` picks: a reader that only shows the run is
    /// done with a kick once nothing can fire where it is.
    pub fn forget(&mut self, mut gone: impl FnMut(&crate::frames::FiringLocation) -> bool) {
        self.kicks.retain(|at, _| !gone(at));
        self.undispatched.retain(|at| self.kicks.contains_key(at));
    }
}

/// Read like the map it holds. Changes go through the table's own methods.
impl std::ops::Deref for KickTable {
    type Target = std::collections::HashMap<crate::frames::FiringLocation, KickedNode>;
    fn deref(&self) -> &Self::Target {
        &self.kicks
    }
}

// ----- Loop instance snapshot ----------------------------------------

/// Key under which a `LoopInstance` is tracked across the engine and
/// the journal fold. Nested loops, parallel sibling iterations, and
/// re-entrant runs each get a distinct key because the
/// `parent_frames` part differs.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopInstanceKey {
    pub group_id: String,
    pub parent_frames: crate::frames::LoopFrames,
    pub run_id: RunId,
}

/// Why a stream ended: the wire vocabulary shared by the generator
/// runtime (`crate::generator` re-exports it), the journal's loop
/// snapshots, and the fold. `Finished` is the clean end; `Failed`
/// carries the producer's error so a consumer's pull surfaces it
/// through `?` instead of reading a truncated stream as complete. A
/// bus's close carries it too (`crate::bus::FiringBuses`).
// SYNC: StreamEnd <-> packages/weft-graph/src/protocol.ts StreamEnd
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamEnd {
    Finished,
    Failed { error: String },
}

// SYNC: LoopTerminationReason <-> packages/weft-graph/src/protocol.ts LoopTerminationReason
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopTerminationReason {
    OverExhausted,
    DoneVoted,
    MaxItersReached,
    Cancelled,
    /// A boundary firing failed (config error, missing carry seed,
    /// outward emit failure): the engine closed the loop's outward
    /// ports and terminated the instance.
    Failed,
}

/// Which lifecycle phase this invocation belongs to. Three-runtime
/// model: infra setup provisions long-lived resources (infra processes),
/// trigger setup captures settings for listeners, and fire runs the
/// regular run subgraph. Activation arms the captured listeners.
///
/// Engine/journal vocabulary: a node never matches on it. The engine
/// routes each phase to the right `Node` method from the manifest
/// (`setup_trigger` for a trigger at TriggerSetup, `run` otherwise;
/// a trigger is skipped at InfraSetup), and a body that has to tell a
/// setup from a real run asks `RunContext::is_setup`.
///
/// - `InfraSetup`: an infra node is being provisioned.
/// - `TriggerSetup`: a trigger node (or its upstream) is being set
///   up. The trigger captures the wake signal and its input values.
/// - `Fire`: the normal fire-time run. The firing trigger
///   receives the wake payload, its outputs flow downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
// SYNC: Phase <-> packages/weft-graph/src/protocol.ts RUN_PHASES
pub enum Phase {
    InfraSetup,
    TriggerSetup,
    Fire,
}

impl Phase {
    /// Stable wire/storage tag. Matches the serde rename so the
    /// JSON form and the DB form agree.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InfraSetup => "infra_setup",
            Self::TriggerSetup => "trigger_setup",
            Self::Fire => "fire",
        }
    }

    /// The phase `tag` names ([`Self::as_str`]). Every phase read back
    /// from text (the DB column, a CLI flag) comes through here, so the
    /// set of names has one definition.
    pub fn from_tag(tag: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.as_str() == tag)
    }

    /// Every phase, for a caller that has to offer the choice.
    pub const ALL: [Phase; 3] = [Self::InfraSetup, Self::TriggerSetup, Self::Fire];
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait(wait_index: u32, ended: Option<AwaitEnd>) -> RecordedWait {
        RecordedWait { wait_index, token: format!("t{wait_index}"), kind: "timer".into(), ended }
    }

    fn answered(wait_index: u32) -> RecordedWait {
        wait(wait_index, Some(AwaitEnd::Answered { value: Value::from(wait_index) }))
    }

    /// A wait under a recorded number is that wait only when it is of the
    /// kind recorded there; another kind is refused, naming both.
    #[test]
    fn a_wait_of_another_kind_under_a_recorded_number_is_refused() {
        let recorded = RecordedWait { wait_index: 2, token: "t".into(), kind: "timer".into(), ended: None };
        let asked = |kind: &str| SignalSpec { kind: kind.into(), ..crate::signal::to_spec(crate::signal::timer::Timer { spec: crate::signal::timer::TimerSpec::After { duration_ms: 1 } }) };
        assert!(check_wait_kind(&recorded, &asked("timer"), "n").is_ok());
        let refused = check_wait_kind(&recorded, &asked("form"), "n").unwrap_err().to_string();
        assert!(refused.contains("'form' signal as its wait #2") && refused.contains("holds wait #2 as a 'timer' signal"), "{refused}");
    }

    /// A new firing takes, in number order, every answer up to the first
    /// wait nobody answered, and asks that one again.
    #[test]
    fn answered_waits_stop_at_the_first_wait_nobody_answered() {
        let waits = vec![wait(2, None), answered(1), answered(0), answered(3)];
        assert_eq!(answered_waits(&waits), vec![answered(0), answered(1)]);
        let gave_up = vec![answered(0), wait(1, Some(AwaitEnd::GaveUp { error: "gone".into() })), answered(2)];
        assert_eq!(answered_waits(&gave_up), vec![answered(0)]);
        assert_eq!(answered_waits(&[wait(0, Some(AwaitEnd::Skipped))]), Vec::new());
        assert_eq!(answered_waits(&[answered(0), answered(2)]), vec![answered(0)], "a gap in the numbers ends them");
    }
}

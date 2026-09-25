//! Wire types for every broker endpoint. Both the server (`weft-broker`)
//! and the client side import from here, so a typo can't drift the
//! two ends apart.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

// The published-connection shape lives in weft-core `access::wire`
// beside the other connect wire types; the store's read-back query
// answers the same struct, so publish and read-back cannot drift.
use weft_core::access::wire::PublishedConnection;
use weft_journal::ExecEvent;
use weft_task_store::tasks::{ClaimFilter, NewTask, Task, TaskOutcome, TaskStatus};

/// The longest the broker holds any request open (every `wait_ms`
/// field is capped at it). A client that wants to wait longer asks
/// again.
pub use weft_task_store::pg_signal::MAX_HOLD;

// =================================================================
// Wire-enum helper
// =================================================================
//
// Every enum that crosses a process boundary as a TEXT column or a
// JSON string field uses this macro. Single source of truth for
// the variant <-> string mapping: the serde `rename_all` attribute.
// No more dual `as_str/parse` tables that drift apart.
//
// Generates:
// - the enum itself with `#[derive(...Serialize, Deserialize)]` and
//   `#[serde(rename_all = "snake_case")]`
// - `as_str(self) -> &'static str` using the same casing rule
// - `parse(s: &str) -> Option<Self>` driven by the same serde derive
//   (via `serde::Deserialize::deserialize(StrDeserializer::new(s))`)
// - `pub const VARIANTS: &[Self]` for iteration (e.g. the
//   auto-generated round-trip tests walk it)
//
// Add or rename a variant in ONE place; everything follows.
macro_rules! wire_enum {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $str:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        #[serde(rename_all = "snake_case")]
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                #[serde(rename = $str)]
                $variant,
            )+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $str, )+
                }
            }
            pub fn parse(s: &str) -> ::core::option::Option<Self> {
                match s {
                    $( $str => ::core::option::Option::Some(Self::$variant), )+
                    _ => ::core::option::Option::None,
                }
            }
            pub const VARIANTS: &'static [Self] = &[ $( Self::$variant ),+ ];
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

/// Generate one round-trip `#[test]` per `wire_enum!`. Each test
/// walks `T::VARIANTS` and asserts `parse(as_str) == Some(v)`, the
/// serde wire form equals `"<as_str>"`, and decode round-trips.
/// New variants are covered automatically (driven off `VARIANTS`);
/// adding a new wire enum means adding one line to the invocation
/// at the bottom of this file.
#[cfg(test)]
macro_rules! wire_enum_roundtrip_tests {
    ( $( $name:ident ),+ $(,)? ) => {
        $(
            #[allow(non_snake_case)]
            #[test]
            fn $name() {
                for v in $name::VARIANTS {
                    assert_eq!($name::parse(v.as_str()), Some(*v), "parse(as_str) {v:?}");
                    let json = serde_json::to_string(v).expect("serialize");
                    assert_eq!(json, format!("\"{}\"", v.as_str()), "wire form {v:?}");
                    let back: $name = serde_json::from_str(&json).expect("deserialize");
                    assert_eq!(back, *v, "round-trip {v:?}");
                }
            }
        )+
    };
}

// =================================================================
// Typed wire enums
// =================================================================

wire_enum! {
    /// Lifecycle verb stored in `infra_lifecycle_command.verb`.
    /// `Apply` / `Stop` / `Terminate` are claimed by the per-tenant
    /// supervisor pod; `Deactivate` / `Reactivate` / `Upgrade` are
    /// claimed by the dispatcher's `lifecycle_claimer` loop. `Upgrade`
    /// is a person's `weft infra upgrade`, issued by the dispatcher
    /// alone (never by a supervisor: [`LifecycleSpec`] cannot name it).
    /// The split is enforced
    /// in the claim queries' `verb IN (...)` predicates (and the
    /// matching partial-index `WHERE` clauses, which can't be
    /// parameterized), in `weft-broker/handlers.rs` and
    /// `weft-dispatcher/lifecycle_claimer.rs`.
    pub enum InfraLifecycleVerb {
        Apply = "apply",
        Stop = "stop",
        Terminate = "terminate",
        Deactivate = "deactivate",
        Reactivate = "reactivate",
        Upgrade = "upgrade",
    }
}

/// The running-work vocabulary (`RunningPolicy`, `RunningChoice`,
/// `DeactivationMode`, `DeactivateSpec`, the default drain cap) is
/// defined once in weft-core, where the CLI reaches it too; re-exported
/// here so the existing `weft_broker_client::protocol::*` paths keep
/// working.
pub use weft_core::{
    default_drain_timeout_secs, DeactivateSpec, DeactivationMode, RunningChoice, RunningPolicy,
    DEFAULT_DRAIN_TIMEOUT_SECS,
};

/// The lifecycle state of an infra node copy lives in weft-core, where a
/// program's `ctx.infra(..).status()` decodes it too; re-exported here so
/// the `weft_broker_client::protocol::*` paths keep working.
pub use weft_core::infra::InfraNodeStatus;

/// Per-unit runtime state carried in `infra_node.units_json`. The map
/// key is the unit name. This is the per-unit truth: the node-level
/// `infra_node.status` is a `InfraNodeStatus::rollup` over these. Also
/// the authoritative roster of a node's EXPECTED units (a unit at 0
/// replicas or a Service-only unit shows no workload, so the supervisor
/// can't learn the roster from observed labels alone). Stamped at apply
/// from the spec's units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitRuntime {
    pub status: InfraNodeStatus,
    /// Resolved at apply from `Unit.on_stop`.
    pub stop_behavior: weft_core::StopBehavior,
    /// Resolved at apply: `Unit.health.flaky_after_seconds` or the
    /// supervisor's global default.
    pub flaky_after_seconds: u32,
    /// Resolved at apply: `Unit.health.recovery_after_seconds` or the
    /// supervisor's global default.
    pub recovery_after_seconds: u32,
    /// The image refs this unit's containers actually run after the
    /// apply that stamped this entry (`Image::Upstream` literals and
    /// resolved `Image::Local` tags alike). A FROZEN unit (left up
    /// across a sync) keeps the refs its last apply recorded, which can
    /// be older than the project's current tag map: image reclamation
    /// (`GET /images/referenced`) unions these so an up unit's old
    /// image is never reclaimed out from under it. Empty for entries
    /// stamped before the field existed. The broker's per-unit status
    /// patches (`jsonb_set ... 'status'`) leave this untouched.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub image_refs: std::collections::BTreeSet<String>,
    /// Resolved at apply: whether the health loop's watch can ever see
    /// this unit ready (`weft_core::infra::Unit::health_watched`). Only a
    /// watched unit counts as seen from its row after the loop's memory
    /// is lost (`NodeHealthState::seeded_from`); any other would read as
    /// vanished and turn flaky. `false` on an entry stamped before the
    /// field existed: such a unit is seeded as unseen until its next
    /// apply, exactly as every unit was before.
    #[serde(default)]
    pub watched: bool,
    /// The replica count weft itself set on this unit's workload, when
    /// that count is one the health loop must know about: `Some(0)` from
    /// an apply of a unit whose spec asks for zero (a fixed count, or an
    /// autoscale floor, of 0), or the count a health protocol's `Scale`
    /// set, recorded once the cluster took it
    /// (`/v1/supervisor/set_scaled`). A reconciled unit's apply stamps
    /// it afresh from the spec; a frozen unit (left up) carries it
    /// forward, since its workload keeps whatever a protocol set. The
    /// health loop reads it to tell a zero weft asked for from a
    /// workload scaled down outside weft. `None` on an entry stamped
    /// before the field existed: any zero there is unintended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaled_to: Option<u32>,
}

impl UnitRuntime {
    /// Whether weft itself asked this unit's workload to run zero
    /// replicas (see [`Self::scaled_to`]).
    pub fn zero_replicas_intended(&self) -> bool {
        self.scaled_to == Some(0)
    }
}

wire_enum! {
    /// Lifecycle status of a project row, written into
    /// `project.status`. Both the dispatcher (writer) and the
    /// supervisor (reader, via the broker) consume this enum.
    /// SYNC: ProjectStatus <-> packages/weft-graph/src/protocol.ts LifecycleStatus,
    ///       crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse.status
    pub enum ProjectStatus {
        /// Fresh row, never activated.
        Registered = "registered",
        /// Transient: user clicked activate; trigger setup in flight.
        Activating = "activating",
        /// Live; worker pool spawns on demand and fires execute.
        Active = "active",
        /// Transient: user clicked deactivate with runningPolicy=wait;
        /// running executions are draining.
        Deactivating = "deactivating",
        /// Idle; gate refuses / parks fires per the lifecycle axes.
        Inactive = "inactive",
    }
}

wire_enum! {
    /// `infra_node.failure_stage` discriminator. Set whenever
    /// `status = Failed`.
    pub enum FailureStage {
        Provision = "provision",
        Apply = "apply",
        Run = "run",
        /// Set when a supervisor-driven stop/terminate aborted.
        ApplyLifecycle = "apply_lifecycle",
    }
}

// ---------- Journal ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecordRequest {
    pub event: ExecEvent,
    /// Worker pod name, used by the journal's fencing trigger. Always
    /// present: only workers write through this request (every caller
    /// passes its own pod), and the dispatcher's own in-process writes
    /// bypass this struct entirely.
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecordResponse {}

/// `POST /v1/journal/record_retroactive`: a failed unrecorded run's whole
/// record, written at once, which turns it into a recorded run. Worker-only
/// and pod-bound like `journal_record`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecordRetroactiveRequest {
    pub events: Vec<ExecEvent>,
    pub pod_name: String,
}

/// `POST /v1/journal/forget_unrecorded`: an unrecorded run ended without
/// failing; drop what is left of it. Worker-only and pod-bound.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalForgetUnrecordedRequest {
    pub color: String,
    pub pod_name: String,
}

// ---------- Execution steering (`ctx.tag_execution` / `ctx.stop_tagged`) ----------

/// `POST /v1/execution/tag`: the worker tags the execution it is
/// driving. Worker-only, pod-bound exactly like `journal_record`: the
/// broker journals `ExecutionTagged` and writes the `execution_tag`
/// rows in one transaction, synchronously, so by the time the node's
/// call returns its tag row exists and a following `stop_tagged` can
/// anchor on it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTagRequest {
    pub color: String,
    /// Already validated by the ctx (`weft_core::tag`); the broker
    /// validates again, because it trusts no pod.
    pub tags: Vec<String>,
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTagResponse {}

/// `POST /v1/execution/stop_tagged`: the worker asks that every live
/// execution of ITS project carrying `tag` be stopped. The broker
/// resolves the ordering anchor at this moment (the asker's own tag
/// seq, or one past the newest row) and enqueues the dispatcher's
/// `stop_tagged` task with it, so a stop that runs late can never reach
/// a sibling that tagged itself after the ask. The project is the
/// color's, read from `execution_color`; the request never names one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStopTaggedRequest {
    /// The asking execution.
    pub color: String,
    pub tag: String,
    pub stop_self: weft_core::StopSelf,
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStopTaggedResponse {
    /// The stop reaches the asking run itself (`StopSelf::Include` and
    /// the run carries the tag). The asker then waits for its own
    /// cancel instead of going on: the dispatcher carries the stop out
    /// a moment later, and a node that returned first would let the run
    /// go past its own stop.
    pub stops_asker: bool,
}

/// The rows of one execution after `after_id`, held open up to
/// `wait_ms` (the broker caps it at `pg_signal::MAX_HOLD`) until at
/// least one exists. `after_id: 0` reads the whole log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalWaitRequest {
    pub color: String,
    pub after_id: i64,
    pub wait_ms: u64,
}

/// RAW payload strings, exactly as journaled, never re-encoded typed
/// events: the broker only ferries these rows, and a typed hop would
/// silently STRIP any field its own build predates (a stale broker
/// once erased `ExecutionStarted.subgraph` this way, and the worker
/// ran an aimed run unbounded). The consumer decodes, loudly. Empty
/// when the hold ended with nothing new.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalWaitResponse {
    pub rows: Vec<weft_journal::RawJournalRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalHasTerminalRequest {
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalHasTerminalResponse {
    pub terminal: bool,
}

// ---------- Tasks ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnqueueDedupRequest {
    pub spec: NewTask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnqueueDedupResponse {
    /// The task id, or `None` when the enqueue was FENCED (a stale
    /// FireSignal from a drained pod during a scale-down move overlap):
    /// no task is created and the caller treats it as a successful
    /// no-op, not an error.
    pub id: Option<Uuid>,
    pub inserted: bool,
    /// True iff the enqueue was fenced out by the placement-generation
    /// check (a stale old-pod fire). Distinct from `inserted=false`,
    /// which means "an identical live task already exists" (dedup hit).
    #[serde(default)]
    pub fenced: bool,
}

/// A task's outcome, held open up to `wait_ms` (the broker caps it at
/// `pg_signal::MAX_HOLD`) until it is terminal; the answer is its state
/// either way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWaitTerminalRequest {
    pub task_id: Uuid,
    pub wait_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWaitTerminalResponse {
    pub status: TaskStatus,
    pub result: Option<Value>,
    pub error: Option<String>,
}

impl TaskWaitTerminalResponse {
    pub fn from_outcome(o: TaskOutcome) -> Self {
        Self {
            status: o.status,
            result: o.result,
            error: o.error,
        }
    }
    pub fn into_outcome(self) -> TaskOutcome {
        TaskOutcome {
            status: self.status,
            result: self.result,
            error: self.error,
        }
    }
}

/// Claim one task, holding up to `wait_ms` (the broker caps it at
/// `pg_signal::MAX_HOLD`) for one to be announced when none is
/// claimable yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaimOneRequest {
    pub pod_id: String,
    pub filter: ClaimFilter,
    pub wait_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaimOneResponse {
    pub task: Option<Task>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHeartbeatRequest {
    pub task_id: Uuid,
    pub pod_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHeartbeatResponse {
    pub renewed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteRequest {
    pub task_id: Uuid,
    pub pod_id: String,
    pub result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequeueRequest {
    pub task_id: Uuid,
    pub pod_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequeueResponse {
    /// True when the surrender landed (the row was still ours and is
    /// now pending again); false when the row had already moved on.
    pub requeued: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailRequest {
    pub task_id: Uuid,
    pub pod_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailResponse {}

// ---------- worker_pod ----------

/// Worker flips its row to `alive` and starts heartbeating. The
/// dispatcher's earlier `insert_spawning` already wrote `namespace`
/// and `owner_dispatcher` from trusted dispatcher-side values, so this
/// request only needs the worker's own identity. The broker re-
/// derives the namespace from the SA token if it ever needs it
/// (caller.namespace) rather than trusting wire data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodRegisterAliveRequest {
    pub pod_name: String,
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodRegisterAliveResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodHeartbeatRequest {
    pub pod_name: String,
    /// The worker's current memory pressure (usage/limit, [0,1]), read
    /// from its own cgroup each tick. The broker writes it to the
    /// `worker_pod` row; the dispatcher places + scales workers on it.
    pub mem_pressure: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodHeartbeatResponse {
    pub renewed: bool,
    /// Meaningful only when `renewed`: the row says the pod is
    /// draining, so it exits as soon as the work it holds lands
    /// instead of keeping the idle grace a warm pod keeps.
    #[serde(default)]
    pub draining: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodMarkDoneRequest {
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodMarkDoneResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodMarkDoneIfIdleRequest {
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodMarkDoneIfIdleResponse {
    /// True if this pod won the guarded `alive -> done` flip and
    /// should now exit. False means work was pending/in-flight, so
    /// the pod stays alive.
    pub exited: bool,
}


// ---------- Infra ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEndpointUrlRequest {
    /// The asking run. The broker resolves its project and member from
    /// it, so a run reaches only its own member's copy.
    pub color: weft_core::Color,
    /// The instance's place spelling (see `InfraEnqueueApplyRequest`):
    /// the asking node's own place, so a node inside a file included
    /// twice reaches the instance of its own call.
    pub node_id: String,
    /// Whether the node exists once per member: then the copy is the
    /// run's member's, else the shared one.
    pub per_member: bool,
    pub endpoint_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEndpointUrlResponse {
    /// `None` when the node is not running or declares no endpoint by
    /// that name.
    pub address: Option<weft_core::infra::EndpointAddress>,
}

// ---------- Connections + cost recording ----------

/// Worker resolves a connection reference for ONE firing: asks for the
/// short-lived pieces it needs to authenticate calls on the stored
/// connection `connection_id`. The store refreshes lazily
/// (single-flight) before answering; for a connection whose credential
/// the runtime supplies, the runtime's credential source answers
/// instead. The caller's execution scope anchors the tenant, and the
/// row's tenant must match (the tenant wall at run time). The firing's
/// identity travels on EVERY resolve, because any connection can be
/// billable and every cost record must be attributable to a firing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveConnectionRequest {
    /// The requesting execution. The broker resolves the owning tenant
    /// AND project from it and enforces both, so neither is taken from
    /// this request: a credential policy that decides per project must
    /// not be deciding on a name the worker chose.
    pub color: String,
    pub node_id: String,
    /// The opening firing's loop-frame coordinate, so anything the
    /// runtime later books against this connection (a measured cost)
    /// can be attributed to the exact firing, not just the node.
    pub frames: weft_core::LoopFrames,
    pub node_type: String,
    /// The connection row the marker references.
    pub connection_id: String,
    /// The service the marker claims; must match the row (a marker
    /// wired at the wrong service fails loud, never resolves to a
    /// different account).
    pub service: String,
    /// The permissions the consuming input declared it needs; the
    /// store refuses a VERIFIED connection short of one (the drift
    /// backstop). A claimed/unknown set passes. Empty = none declared.
    #[serde(default)]
    pub required_permissions: Vec<String>,
    /// The stored values the consuming input declared it needs; the
    /// store refuses a connection missing one, naming it. Unlike
    /// permissions there is no unknown case: the value is stored or
    /// it is not. Empty = none declared.
    #[serde(default)]
    pub required_values: Vec<String>,
    /// How long the work this connection is for may reasonably take: a
    /// runtime-supplied credential is guaranteed usable for that long,
    /// and the runtime may retire it after (the crash backstop for a
    /// worker that dies without releasing).
    pub expected_duration_secs: u64,
}

/// A node handing out a connection to something it runs itself, and
/// the question that precedes it ("did I already publish one?").
/// `values` are the service's own declared fields, exactly what a
/// person would have pasted; the store refuses anything else.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishAccessRequest {
    /// The publishing execution. The broker resolves the owning tenant
    /// AND project from it, so neither is taken from this request.
    pub color: String,
    /// The publishing node's PLACE, spelled the way a person writes it
    /// (`db`, or `one.db` inside the file the site `one` includes). Its
    /// connection: publishing again updates that one row, and
    /// terminating the node deletes it. A node inside a file included
    /// twice publishes once per call, one row each.
    pub node_id: String,
    pub service: String,
    /// The service's own recipe, from the catalog the worker ships.
    pub spec: weft_core::AccessSpec,
    pub values: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub label: Option<String>,
    /// Whether the publishing node exists once per member: its
    /// connection is then the run member's (whose, the broker reads off
    /// the run, never from here), else the shared one.
    #[serde(default)]
    pub per_member: bool,
}

/// The reference the publisher gets back, to put on its output port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishAccessResponse {
    pub connection: PublishedConnection,
}

/// "Which connection did this node publish for this service, if any?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedAccessRequest {
    pub color: String,
    pub node_id: String,
    pub service: String,
    /// As on [`PublishAccessRequest::per_member`].
    #[serde(default)]
    pub per_member: bool,
}

/// Worker: mint a member token for a member of the run's own project
/// (`ctx.tokens().mint_for_member`). The project is the run's.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramMintMemberTokenRequest {
    /// The token's id, chosen by the run and journaled before this
    /// request: a replayed run mints under the same id, which replaces
    /// that token (a new value, the old one dead) instead of leaving a
    /// second one behind.
    pub id: uuid::Uuid,
    pub color: String,
    pub member: weft_core::member::MemberId,
    /// How long it works, in seconds; required, a year at most.
    pub expires_in_secs: u64,
    /// Whether it reads its member's copies' displays (a bridge's pairing
    /// code).
    #[serde(default)]
    pub displays: bool,
    #[serde(default)]
    pub name: Option<String>,
}

/// The answer: the published connection, or nothing published yet.
///
/// Two answers, told apart by a tag that must be THERE. An
/// `Option<PublishedConnection>` field would not do: serde reads a
/// missing key as `None`, so a peer that does not speak this shape
/// would be understood as saying "nothing published" rather than
/// failing. That is the difference between a node reading back the
/// connection it holds and one asking a database for a password it
/// already gave away, then telling its user the data is unreachable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PublishedAccessResponse {
    Published { connection: PublishedConnection },
    NothingPublished,
}

impl PublishedAccessResponse {
    pub fn connection(self) -> Option<PublishedConnection> {
        match self {
            Self::Published { connection } => Some(connection),
            Self::NothingPublished => None,
        }
    }
}

/// The worker handoff: EXACTLY the stored values the service's declared
/// auth steps interpolate (never the refresh token or an app secret),
/// plus the steps to apply them with. Time-bounded by nature (an
/// expiring token is refreshed store-side on the next resolve).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveConnectionResponse {
    pub values: std::collections::BTreeMap<String, String>,
    pub auth: Vec<weft_core::access::spec::AuthStep>,
    /// Display identity, for log/error context only.
    pub identity: Option<String>,
    /// Where calls on the resolved credential must be sent when the
    /// runtime relays them; `None` = straight to the service's own
    /// API. The worker's connection client does that routing.
    pub relay_url: Option<String>,
    /// Whose credential the values carry; rides every cost record so
    /// the trail says whose account each figure landed on.
    pub owner: weft_core::CredentialOwner,
}

// A measured call's cost record rides the generic task rail (a
// `TaskKind::RecordCost` enqueued like any worker side effect), so there is
// no dedicated cost endpoint or wire type: `weft_task_store::RecordCostPayload`
// is the whole contract.

/// The firing that resolved a connection finished: release the lease
/// NOW rather than leaving a runtime-supplied credential usable to its
/// window (which is only the crash backstop). Sent by the engine ONLY
/// for an `Ours`-owned connection (a runtime-supplied credential is
/// the only thing there is to retire); a user's own stored values are
/// theirs and never travel back. Nothing node-facing makes this call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseConnectionRequest {
    pub color: String,
    /// The resolved values being given back; the runtime's credential
    /// source retires the ones it recognizes.
    pub values: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseConnectionResponse {}

// ---------- Project (worker fetches its own definition) ----------

/// Worker call to fetch the project's runtime `ProjectDefinition`
/// at execution claim time. Keyed by `(project_id, expected_hash)`:
/// the hash makes the lookup content-addressed against the
/// append-only `project_definition` history table.
///
/// Server contract: returns the stored `project_json` for the row
/// at `(project_id, expected_hash)` if it exists (200), or 404 if
/// no row was ever recorded under that hash. There is no "raced"
/// case: the history table is append-only, so a hash either has a
/// row or it doesn't.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectFetchDefinitionRequest {
    pub project_id: Uuid,
    /// The definition hash the caller expects the project to have.
    /// Workers learn it from the `Execute` / `Resume` task payload;
    /// the dispatcher (which controls task enqueue) stamps it from
    /// the project row at enqueue time so the worker sees the
    /// definition the user clicked Run against.
    pub expected_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectFetchDefinitionResponse {
    /// Serialized `weft_core::ProjectDefinition`. The client
    /// `serde_json::from_str` it into the typed value once.
    pub project_json: String,
    /// Echoes the `running_definition_hash` the server matched
    /// against. Same as `expected_hash` on success; included so
    /// clients can stamp their cache by the verified value.
    pub definition_hash: String,
}

// The storage caller-identity verdict (`StorageAuthorizeResponse`) used to live
// here as a broker-relay wire type. The runtime-file plane now resolves the
// caller IN-PROCESS in the broker (it is both the identity authority and the
// data path), so there is no relay and no wire type: see
// `weft_broker::auth::resolve_storage_caller`.

// ---------- Supervisor surface (tenant-scoped) ----------

/// Empty: a pooled supervisor lists projects across all tenants. The
/// request body carries nothing; the trusted caller identity is the
/// authority.
/// Claim + renew + report ownership in one atomic broker round-trip.
/// The pooled supervisor calls this on its ownership tick: the broker
/// records the pod's memory pressure, renews this pod's existing project
/// leases, and (only while the pod is below the shared memory saturation
/// threshold) claims a batch more unowned-or-expired projects' infra for
/// it (the EXCLUSIVE lease that keeps two supervisors off the same
/// project), then returns the full set this pod now owns. Both work loops
/// (lifecycle, health) then act ONLY on the returned set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSyncOwnershipRequest {
    /// The pooled supervisor pod syncing its ownership.
    pub pod_name: String,
    /// This pod's current memory pressure (usage/limit, `[0.0, 1.0]`),
    /// the SAME load metric the listener uses. The broker claims new
    /// projects for this pod only while it is below the shared
    /// saturation threshold; at or above it the pod keeps the projects it
    /// owns but takes on no more, so the dispatcher spawns another
    /// supervisor. Recorded on the `supervisor_pod` row so the
    /// dispatcher's placement + scale-down read real pressure (not a
    /// project count). 0.0 when uncapped (local dev) so locally a single
    /// supervisor keeps claiming until the machine is genuinely squeezed.
    pub mem_pressure: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorProject {
    pub project_id: Uuid,
    /// The project's tenant. A pooled supervisor serves many tenants,
    /// so the compile context's tenant comes from the project, not from
    /// a per-supervisor identity (it has none).
    pub tenant_id: String,
    pub project_namespace: String,
    /// The project's listening as one status: the aggregate over all
    /// its trigger activations, every owner's
    /// (`weft_broker_client::activation::aggregate`). The supervisor's
    /// health protocol engine consumes this so a
    /// `HealthCondition::ProjectStatusEq` can fire based on lifecycle
    /// (e.g. "auto-recover only when the project is currently
    /// parked"). No `serde(default)`: broker and supervisor deploy
    /// together, a missing field is schema drift and should fail loud
    /// on deserialize.
    pub status: ProjectStatus,
    /// True iff some activation the health loop took down (it parks
    /// only what reads a broken infra copy) is still down. A person's
    /// deactivate clears the mark on what it takes down, so the default
    /// auto-recover protocol, gated on this, reactivates only what the
    /// health loop took and never overrides a person's stop.
    pub health_parked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSyncOwnershipResponse {
    /// Every project this pod now owns (after renew + claim). The work
    /// loops act only on these.
    pub owned: Vec<SupervisorProject>,
    /// The projects among `owned` this tick took on: freshly claimed, or
    /// this pod's own lease revived after it lapsed. A command issued on
    /// one of them while nobody held it woke none of this pod's claims,
    /// so the pod asks again at once when this is not empty.
    pub claimed: Vec<Uuid>,
}

/// Pure read of the projects a supervisor pod currently owns (no claim,
/// no renew). The work loops (health, lifecycle) and per-command
/// namespace lookups use this; ownership breadth is changed ONLY by
/// `sync_ownership` (the ownership tick), never as a side effect of
/// doing work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorOwnedProjectsRequest {
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorOwnedProjectsResponse {
    pub owned: Vec<SupervisorProject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraNodesRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraNode {
    /// The instance's place spelling (see `InfraEnqueueApplyRequest`).
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    pub instance_id: String,
    pub status: InfraNodeStatus,
    pub applied_spec_hash: Option<String>,
    /// When the last apply of this copy finished (unix seconds), `None`
    /// before any. The health loop's durable "this copy's workloads
    /// exist by now": a copy running past its apply by its flaky window
    /// whose workload is missing is broken, whatever the loop's memory
    /// lost in a restart or a move to another pod.
    pub applied_at_unix: Option<i64>,
    /// Where the last apply stamped the endpoints. The apply's full
    /// skip compares it with what the spec gives now, so a row stamped
    /// before a field existed (or by an older rule) is applied again
    /// rather than skipped with stale addresses.
    #[serde(flatten)]
    pub addresses: AppliedEndpoints,
    /// PVC names to preserve on terminate (see
    /// `SupervisorSetAppliedRequest::preserve_pvcs`). No
    /// `serde(default)`: pre-prod, supervisor + broker deploy
    /// together; a missing field is schema drift, not version
    /// skew.
    pub preserve_pvcs: Vec<String>,
    /// Per-unit runtime: status + resolved health windows +
    /// stop_behavior, keyed by unit name. The authoritative unit
    /// roster + per-unit truth the health/stop loops operate on.
    pub units: std::collections::BTreeMap<String, UnitRuntime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraNodesResponse {
    pub nodes: Vec<SupervisorInfraNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorHealthProtocolsRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorHealthProtocolsResponse {
    pub protocols: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorClaimCommandRequest {
    /// The pooled supervisor pod claiming work. It claims a lifecycle
    /// command ONLY for a project whose infra it currently owns (the
    /// `infra_owner` exclusive lease), so two supervisors never change
    /// the same project's cluster objects.
    pub claimer_pod: String,
    /// The projects this pod is running a command for right now. Their
    /// next command waits until that one completes, so one project's
    /// commands run in order while different projects' run side by side.
    pub busy_projects: Vec<uuid::Uuid>,
    /// How long to hold for a command to be issued when none is
    /// waiting (the broker caps it at `pg_signal::MAX_HOLD`).
    pub wait_ms: u64,
}

/// What a supervisor's claim comes back with.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SupervisorClaim {
    /// The command this pod runs next.
    Command(SupervisorCommandRow),
    /// A command was issued for a project no supervisor owns yet. The
    /// pod takes ownership now rather than at its next ownership tick,
    /// and claims again.
    UnownedWork,
    /// Nothing was issued for this pod for the whole hold.
    Nothing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandRow {
    pub id: i64,
    pub project_id: Uuid,
    pub node_id: Option<String>,
    /// Which copies the command acts on (an apply names exactly one:
    /// shared, or one member's). Absent reads as the shared copies.
    #[serde(default)]
    pub copies: weft_core::member::Copies,
    pub verb: InfraLifecycleVerb,
    /// Whether the supervisor should wait for the project's
    /// running-execution count to reach 0 before performing the
    /// lifecycle op. `Some` for Stop / Terminate (populated by
    /// `issue_lifecycle`); `None` for Apply (irrelevant) and for
    /// dispatcher-owned verbs that don't reach the supervisor's
    /// claim path.
    #[serde(default)]
    pub running_policy: Option<RunningPolicy>,
    /// Set for `verb = apply`: `InfraSpec` serialized as JSON. The
    /// supervisor reads the prior `infra_node` row itself to decide
    /// skip / fresh / replace; the worker doesn't pass a mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_json: Option<serde_json::Value>,
    /// Stop only: force scale-to-zero EVERY unit, ignoring each unit's
    /// `on_stop` (so NoOp units come down too). The user's explicit
    /// "take it all down so I can update it" override.
    #[serde(default)]
    pub force: bool,
    /// Cap on the `running_policy = wait` drain before the op proceeds
    /// anyway. Carried per command (the user picks it with the wait
    /// choice); defaults to `DEFAULT_DRAIN_TIMEOUT_SECS`.
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorEventRecordRequest {
    pub project_id: Uuid,
    pub node_id: Option<String>,
    /// Whose copy the event is about: `None` for the shared one (or a
    /// project-level event).
    #[serde(default)]
    pub member: Option<weft_core::member::MemberId>,
    pub kind: InfraEventKind,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorEventRecordResponse {
    pub id: i64,
}

// =================================================================
// Typed infra_event surface
// =================================================================
//
// The wire format for `infra_event` is `(kind: TEXT, payload: JSONB)`.
// The supervisor writes rows; the dispatcher's `infra_event_bridge`
// reads them and converts each row into a typed `DispatcherEvent`
// for SSE consumers. The bridge needs a stable contract on:
//   1. which kind strings exist;
//   2. what JSON shape each kind's payload carries.
//
// `InfraEvent` collapses (kind, payload) into one tagged enum.
// Writers construct `InfraEvent::Flaky { desired, ready }` and
// call `.into_record()` to get `(kind, payload)` for the wire
// request. Readers call `InfraEvent::from_kind_and_payload(kind,
// payload)` to recover the typed shape. A rename or schema drift
// on either side becomes a compile error at the construction site.

wire_enum! {
    /// Tagged kind for `infra_event` rows. Matches the `kind` TEXT
    /// column verbatim via `as_str()`.
    pub enum InfraEventKind {
        Flaky = "flaky",
        Recovered = "recovered",
        Failed = "failed",
        Stopped = "stopped",
        Terminated = "terminated",
        Started = "started",
        Notify = "notify",
        ProtocolConfigError = "protocol_config_error",
    }
}

/// Typed per-kind payload. Construction-site is the supervisor;
/// destruction-site is the dispatcher's bridge. The `kind` string
/// stored in the DB row drives which arm is selected.
///
/// **No `Serialize` / `Deserialize` on the outer enum**: the wire
/// format is `(kind: TEXT, payload: JSONB)` in two columns, NOT a
/// tagged JSON blob. Callers go through `into_record()` /
/// `from_kind_and_payload()` exclusively; deriving serde here would
/// create a different shape and let a future caller silently emit
/// the wrong bytes via `to_value(&infra_event)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InfraEvent {
    Flaky(FlakyPayload),
    Recovered,
    Failed(FailedPayload),
    Stopped,
    Terminated,
    Started(StartedPayload),
    Notify(NotifyPayload),
    ProtocolConfigError(ProtocolConfigErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedPayload {
    pub instance_id: String,
    pub mode: StartMode,
}

wire_enum! {
    /// How an apply settled. Drives the action-bar distinction
    /// between "first apply" / "re-applied with new spec" / "already
    /// running, no-op".
    pub enum StartMode {
        Fresh = "fresh",
        Replace = "replace",
        Skip = "skip",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlakyPayload {
    /// k8s desired replicas (non-negative; k8s counts them in i32,
    /// widened to i64 because `WorkloadReplicaState` carries them so).
    pub desired: i64,
    pub ready: i64,
    /// Optional human-readable reason; the bridge surfaces it on
    /// the action-bar banner. Defaults to a `desired=N ready=M`
    /// summary if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedPayload {
    pub stage: FailureStage,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyPayload {
    pub protocol: String,
    pub channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolConfigErrorPayload {
    /// The serde error message from the failed `health_protocols_json`
    /// deserialize, verbatim.
    pub error: String,
}

impl InfraEvent {
    /// Wire-shape pair for a `supervisor_event_record` call.
    /// The kind string is the canonical column value; the payload
    /// is the typed body serialized to JSON.
    ///
    /// Serialization is infallible for every payload variant here
    /// (plain structs over `String`, `i64`, typed enums). If a
    /// future variant adds a non-Serialize-friendly field, the
    /// `expect` below is the loud-failure surface we want.
    pub fn into_record(self) -> (InfraEventKind, Value) {
        let kind = match &self {
            Self::Flaky(_) => InfraEventKind::Flaky,
            Self::Recovered => InfraEventKind::Recovered,
            Self::Failed(_) => InfraEventKind::Failed,
            Self::Stopped => InfraEventKind::Stopped,
            Self::Terminated => InfraEventKind::Terminated,
            Self::Started(_) => InfraEventKind::Started,
            Self::Notify(_) => InfraEventKind::Notify,
            Self::ProtocolConfigError(_) => InfraEventKind::ProtocolConfigError,
        };
        let payload = match self {
            Self::Flaky(p) => serde_json::to_value(p).expect("FlakyPayload serializes"),
            Self::Failed(p) => serde_json::to_value(p).expect("FailedPayload serializes"),
            Self::Started(p) => serde_json::to_value(p).expect("StartedPayload serializes"),
            Self::Notify(p) => serde_json::to_value(p).expect("NotifyPayload serializes"),
            Self::ProtocolConfigError(p) => {
                serde_json::to_value(p).expect("ProtocolConfigErrorPayload serializes")
            }
            Self::Recovered | Self::Stopped | Self::Terminated => Value::Null,
        };
        (kind, payload)
    }

    /// Reader: validate that `payload` matches `kind`'s shape.
    /// Returns `Err` if the payload doesn't deserialize as expected
    /// (e.g. a Flaky event missing `desired` / `ready`). Callers
    /// (the bridge) treat this as a writer bug.
    pub fn from_kind_and_payload(
        kind: InfraEventKind,
        payload: &Value,
    ) -> Result<Self, serde_json::Error> {
        Ok(match kind {
            InfraEventKind::Flaky => Self::Flaky(serde_json::from_value(payload.clone())?),
            InfraEventKind::Recovered => Self::Recovered,
            InfraEventKind::Failed => Self::Failed(serde_json::from_value(payload.clone())?),
            InfraEventKind::Stopped => Self::Stopped,
            InfraEventKind::Terminated => Self::Terminated,
            InfraEventKind::Started => Self::Started(serde_json::from_value(payload.clone())?),
            InfraEventKind::Notify => Self::Notify(serde_json::from_value(payload.clone())?),
            InfraEventKind::ProtocolConfigError => {
                Self::ProtocolConfigError(serde_json::from_value(payload.clone())?)
            }
        })
    }
}

/// The supervisor's claim identity on a lifecycle write: its
/// `WEFT_POD_NAME` (the Deployment name, the SAME string it uses to
/// claim commands and that keys its `infra_owner` lease). NOT the auth
/// token's pod name (the real Pod name, with a ReplicaSet suffix) which
/// differs; the broker's ownership gate must compare the lease against
/// THIS, so the supervisor sends it explicitly, exactly as it does on
/// `claim_command` / `sync_ownership`.
// SYNC: supervisor pod_name (WEFT_POD_NAME = Deployment name, the infra_owner lease key) <-> crates/weft-infra-supervisor/src/lib.rs (SupervisorState.pod_name, sent by lifecycle.rs/health.rs/ownership.rs), crates/weft-dispatcher/src/supervisor_pool.rs (render_supervisor_manifest WEFT_POD_NAME env), crates/weft-broker-client/src/lifecycle_command.rs (owns_project_predicate, gating infra_owner.supervisor_pod = req.pod_name in weft-broker/src/handlers.rs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetStatusRequest {
    /// The supervisor's claim identity (`WEFT_POD_NAME`); the broker
    /// checks it holds the project's live `infra_owner` lease.
    pub pod_name: String,
    /// `infra_lifecycle_command.id` the supervisor is executing,
    /// if this write is part of a lifecycle command (Stop /
    /// Terminate / Apply transitions). The broker checks the caller
    /// still OWNS the project (live `infra_owner` lease for `pod_name`)
    /// before applying the write; a supervisor that lost ownership
    /// (drain / lease takeover) can't stamp statuses for a project
    /// another pod now owns.
    ///
    /// `None` for autonomous writes from the health loop
    /// (`Flaky` / `Running` reconciliation) where there is no
    /// command in flight. The ownership check applies all the same, so
    /// a pod that lost the project cannot stamp it either.
    pub command_id: Option<i64>,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    /// The unit whose status this write sets. `Some(unit)` updates that
    /// unit's entry in `units_json` and recomputes the node-level
    /// rollup; the autonomous health loop always targets a unit.
    /// `None` writes the node-level status directly AND every unit (a
    /// lifecycle-driven node-wide transition: Stopping/Terminating/
    /// Failed during a command applies to the whole node uniformly).
    pub unit: Option<String>,
    pub status: InfraNodeStatus,
    /// Required iff `status == Failed`; ignored otherwise (the broker
    /// could enforce, but today it stores as-given).
    pub failure_stage: Option<FailureStage>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetStatusResponse {}

/// Record the replicas a health protocol's `Scale` set on one unit of
/// one copy, into that unit's `UnitRuntime::scaled_to`. The supervisor
/// writes it AFTER the cluster took the scale, so the row never claims
/// a count the cluster does not have; a refused write fails the action
/// and the protocol retries. Fenced like the
/// autonomous `set_status`: the caller must own the project, the unit
/// must be in the row's roster, and no lifecycle command may be in
/// flight for the copy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetScaledRequest {
    /// See [`SupervisorSetStatusRequest::pod_name`].
    pub pod_name: String,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    pub unit: String,
    pub replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetScaledResponse {}

/// Atomic post-apply state write: status, instance_id, applied spec
/// hash, endpoints map. Supervisor calls this on successful apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetAppliedRequest {
    /// The supervisor's claim identity (`WEFT_POD_NAME`); the broker
    /// gates the UPSERT on it holding the project's live `infra_owner`
    /// lease. See [`SupervisorSetStatusRequest::pod_name`].
    pub pod_name: String,
    /// `infra_lifecycle_command.id` the supervisor is executing.
    /// The broker rejects the UPSERT if the caller no longer OWNS the
    /// project; prevents a displaced supervisor from resurrecting a row
    /// that `remove_node` deleted, or stamping over the new owner's
    /// still-running apply.
    pub command_id: i64,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    pub instance_id: String,
    pub applied_spec_hash: String,
    #[serde(flatten)]
    pub addresses: AppliedEndpoints,
    pub namespace: String,
    /// PVC names to preserve on a future terminate. From
    /// `InfraSpec.lifecycle.on_terminate.preserve_pvcs`. Persisted
    /// on the `infra_node` row so the supervisor can honor it on
    /// terminate (terminate has no access to the spec). No
    /// `serde(default)`: pre-prod, broker + supervisor deploy
    /// together; a missing field is schema drift.
    pub preserve_pvcs: Vec<String>,
    /// Per-unit runtime, resolved from the spec's units at apply.
    /// Status is set to `Running` for every unit here (apply success
    /// = all units up). The health loop then maintains per-unit
    /// status from this baseline.
    pub units: std::collections::BTreeMap<String, UnitRuntime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetAppliedResponse {}

/// Where an applied node's endpoints answer, as the apply stamps them
/// on its `infra_node` row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedEndpoints {
    /// Endpoint name to its cluster-internal URL.
    #[serde(rename = "endpoints")]
    pub urls: std::collections::BTreeMap<String, String>,
    /// Endpoint name to the path it answers at on the front door, for
    /// `TenantPublic` endpoints only (`weft_core::infra::tenant_public_path`).
    pub public_paths: std::collections::BTreeMap<String, String>,
}

/// Supervisor-callable: write or update the `infra_node` row at
/// `Provisioning` status BEFORE the apply begins. Locks
/// in the (instance_id, namespace, preserve_pvcs) tuple so that a
/// partial-apply failure leaves a visible row pointing at the
/// labelled-but-incomplete resources. The user's Terminate then
/// works (delete_by_label keyed on the recorded instance_id +
/// preserve_pvcs). On apply success, `set_applied` flips
/// Provisioning -> Running and fills endpoints + applied_spec_hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetProvisioningRequest {
    /// The supervisor's claim identity (`WEFT_POD_NAME`); the broker
    /// gates the write on it holding the project's live `infra_owner`
    /// lease. See [`SupervisorSetStatusRequest::pod_name`].
    pub pod_name: String,
    pub command_id: i64,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    pub instance_id: String,
    pub namespace: String,
    /// Carried forward to `set_applied`; needed at Terminate time
    /// even when the apply never reaches success.
    pub preserve_pvcs: Vec<String>,
    /// Per-unit runtime resolved from the spec's units. Status is
    /// `Provisioning` for every unit at this point. Locked in before
    /// any cluster call so a partial-apply failure leaves the roster visible.
    pub units: std::collections::BTreeMap<String, UnitRuntime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetProvisioningResponse {}

/// Supervisor-callable: enqueue a `deactivate` / `reactivate`
/// lifecycle command for the dispatcher to claim. The supervisor
/// can't touch the signal table directly (it has no Postgres write
/// authority for those rows), so it asks the dispatcher via this
/// command queue.
///
/// `verb` is constrained to the dispatcher-claimable set at the
/// type level via the typed `LifecycleSpec`: only `Deactivate(...)`
/// and `Reactivate` are constructible. There's no way to enqueue
/// `Apply` / `Stop` / `Terminate` through this endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorEnqueueLifecycleRequest {
    pub project_id: Uuid,
    pub spec: LifecycleSpec,
}

/// Typed (verb, payload) pair for a dispatcher-claimable lifecycle
/// command. Both variants live in `protocol.rs` so the supervisor
/// can't accidentally enqueue a verb the dispatcher doesn't handle,
/// and the dispatcher's claim path can't drift the payload shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "verb", rename_all = "snake_case")]
pub enum LifecycleSpec {
    /// Take down what a health protocol reaches, under `spec`.
    Deactivate(TakeDownReaders),
    /// Bring back what the health loop took down: each activation it
    /// took carries the mark (`deactivated_by_health`) the take-down
    /// set, and of those this restores every one that reads none of the
    /// `still_broken` copies.
    Reactivate(RestoreReaders),
}

/// The health loop's recovery: the activations it took down that read
/// none of the copies `still_broken` names. Named by what is still
/// broken rather than by what recovered, so a trigger that also reads a
/// copy the loop cannot see (an apply that failed, a unit with no
/// replicas) comes back with the rest: nothing says that copy is broken.
/// Empty when nothing is: every activation the loop took comes back.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreReaders {
    pub still_broken: Vec<InfraCopy>,
}

/// The health loop's take-down: `spec`, over the activations `reach`
/// names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TakeDownReaders {
    pub spec: DeactivateSpec,
    pub reach: TakeDownReach,
}

/// Which live activations a health take-down reaches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TakeDownReach {
    /// Every live activation of the project: the protocol's condition
    /// names no infra, so nothing says which copies are broken.
    Project,
    /// The activations whose triggers read one of the `broken` copies,
    /// and nothing else. A shared copy is read by every owner's triggers
    /// that read the node; a member's copy only by that member's. Never
    /// empty: a take-down aimed at no copy is refused.
    ReadersOf { broken: Vec<InfraCopy> },
}

impl TakeDownReaders {
    /// The spec's own rule, and a reach that names something.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.spec.validate()?;
        if matches!(&self.reach, TakeDownReach::ReadersOf { broken } if broken.is_empty()) {
            return Err("a health take-down aimed at broken infra copies must name them");
        }
        Ok(())
    }
}

/// One copy of an infra node, as the health loop names what it found
/// broken or recovered: the node's place, and whose copy (`None` for the
/// shared one).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InfraCopy {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<weft_core::member::MemberId>,
}

impl LifecycleSpec {
    /// A take-down's own rule ([`TakeDownReaders::validate`]); a
    /// recovery has none (any set of still-broken copies, empty
    /// included, says what to restore). Checked where the spec is
    /// written and again where it is claimed.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Deactivate(take_down) => take_down.validate(),
            Self::Reactivate(_) => Ok(()),
        }
    }

    /// Project the typed spec onto the (verb, running_policy,
    /// spec_json) columns the `infra_lifecycle_command` schema
    /// stores. `running_policy` is `None` for dispatcher-owned
    /// verbs: Deactivate carries it inside `spec_json` (single
    /// source of truth); Reactivate has no running-fires concept, and
    /// carries its still-broken copies in `spec_json`.
    /// Supervisor-owned verbs (Stop / Terminate) populate the
    /// column directly through `issue_lifecycle`; Apply ignores it.
    pub fn into_row_columns(self) -> (InfraLifecycleVerb, Option<RunningPolicy>, Option<Value>) {
        match self {
            Self::Deactivate(take_down) => {
                let payload = serde_json::to_value(take_down).expect("TakeDownReaders serializes");
                (InfraLifecycleVerb::Deactivate, None, Some(payload))
            }
            Self::Reactivate(restore) => {
                let payload = serde_json::to_value(restore).expect("RestoreReaders serializes");
                (InfraLifecycleVerb::Reactivate, None, Some(payload))
            }
        }
    }

    /// Inverse of `into_row_columns`: rebuild the typed spec from
    /// the row's columns. Used by the dispatcher's claim path so
    /// encode and decode share one source of truth.
    ///
    /// Returns `Err` on:
    /// - a verb this spec does not carry (`Apply`/`Stop`/`Terminate`
    ///   are the supervisor's, and an `Upgrade` carries the
    ///   dispatcher's own spec);
    /// - `Deactivate` or `Reactivate` with NULL or malformed
    ///   `spec_json`.
    pub fn from_row_columns(
        verb: InfraLifecycleVerb,
        spec_json: Option<Value>,
    ) -> Result<Self, FromRowColumnsError> {
        match verb {
            InfraLifecycleVerb::Deactivate => {
                let json = spec_json.ok_or(FromRowColumnsError::DeactivateMissingSpec)?;
                let take_down: TakeDownReaders = serde_json::from_value(json)
                    .map_err(FromRowColumnsError::DeactivateMalformed)?;
                Ok(Self::Deactivate(take_down))
            }
            InfraLifecycleVerb::Reactivate => {
                let json = spec_json.ok_or(FromRowColumnsError::ReactivateMissingSpec)?;
                let restore: RestoreReaders = serde_json::from_value(json)
                    .map_err(FromRowColumnsError::ReactivateMalformed)?;
                Ok(Self::Reactivate(restore))
            }
            other => Err(FromRowColumnsError::NotDispatcherClaimable(other)),
        }
    }
}

/// Error variants from `LifecycleSpec::from_row_columns`. Surfaces
/// with enough structure that callers (today: the dispatcher's
/// `lifecycle_claimer`) can log a specific message and either
/// complete-with-error or hard-bail.
#[derive(Debug, thiserror::Error)]
pub enum FromRowColumnsError {
    #[error("deactivate command missing spec_json")]
    DeactivateMissingSpec,
    #[error("deactivate spec_json malformed: {0}")]
    DeactivateMalformed(#[source] serde_json::Error),
    #[error("reactivate command missing spec_json")]
    ReactivateMissingSpec,
    #[error("reactivate spec_json malformed: {0}")]
    ReactivateMalformed(#[source] serde_json::Error),
    #[error("verb '{0}' carries no supervisor-issued spec")]
    NotDispatcherClaimable(InfraLifecycleVerb),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorEnqueueLifecycleResponse {
    pub command_id: i64,
}

/// Worker-callable: enqueue an Apply lifecycle command for the
/// supervisor that owns the project. The engine uses this after `node.provision_infra()`
/// returns a fresh InfraSpec. The supervisor reads the prior
/// `infra_node` row, compiles the new spec, hashes, and decides
/// skip / fresh / replace internally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEnqueueApplyRequest {
    pub project_id: Uuid,
    /// The instance's PLACE, spelled the way a person writes the node
    /// (`db`, or `one.db` inside the file the site `one` includes): the
    /// key its `infra_node` row and every resource are made under. A
    /// file included twice is applied twice, once per place.
    pub node_id: String,
    /// Whose copy: the setup run's member, for a node that exists once
    /// per member; `None` for a shared node.
    #[serde(default)]
    pub member: Option<weft_core::member::MemberId>,
    pub spec_json: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEnqueueApplyResponse {
    pub command_id: i64,
}

/// Worker-callable: wait for a previously-issued apply command to
/// reach terminal state. The broker holds the request until the
/// command completes or the hold ends; the worker asks again until it
/// completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraWaitApplyRequest {
    pub project_id: Uuid,
    pub command_id: i64,
    /// How long to hold for the command to complete (the broker caps it
    /// at `pg_signal::MAX_HOLD`); the answer is its state either way.
    pub wait_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraWaitApplyResponse {
    pub completed: bool,
    /// Typed outcome once `completed=true`. None while pending.
    pub outcome: Option<LifecycleOutcome>,
    /// Human-readable message attached to a terminal outcome.
    /// None on success or while pending; on Failed it carries the
    /// error message; on Cancelled it carries the reason. The
    /// `outcome` discriminant says which.
    pub outcome_message: Option<String>,
}

wire_enum! {
    /// Terminal state of an `infra_lifecycle_command`. Single source
    /// of truth for the `outcome` TEXT column. Adding a new variant
    /// requires touching this enum and every match site (compile
    /// errors flag the drift).
    pub enum LifecycleOutcome {
        /// The claimer completed the verb cleanly. `error` is None.
        Succeeded = "succeeded",
        /// The claimer hit a real error. `error` carries the
        /// message; callers treat this as a failure.
        Failed = "failed",
        /// The command was abandoned before the claimer ran it
        /// (e.g. the targeted node was removed mid-flight). NOT a
        /// failure; callers treat as "no longer applicable".
        Cancelled = "cancelled",
    }
}

/// Supervisor reads the project's per-(node, image_name) hash map
/// so it can resolve `Image::Local` references at apply time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorProjectImageTagsRequest {
    pub project_id: Uuid,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorProjectImageTagsResponse {
    pub tags: std::collections::HashMap<String, String>,
}

/// Decode one `infra_image_tags_json` value (`{node: {image: tag}}`, the
/// complete per-node map the dispatcher's infra sync writes) into the
/// typed map. THE canonical decode for the column: the broker (the
/// supervisor's read) and the dispatcher (the referenced-images
/// keep-set) both route through it, so no reader hand-rolls the shape
/// and the two cannot drift (a decode that silently skipped or coerced
/// a bad row would either mask schema corruption as "no images
/// registered" or shrink the keep-set below what the supervisor may
/// still apply). `whose` names the row in the error, so one corrupt row
/// among many is diagnosable.
// SYNC: the column's writer <-> crates/weft-dispatcher/src/api/infra.rs
//       (the sync handler that builds the map) +
//       crates/weft-dispatcher/src/project_store.rs (set_running_hashes,
//       the atomic persist)
pub fn decode_infra_image_tags(
    value: serde_json::Value,
    whose: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>> {
    serde_json::from_value(value).map_err(|e| {
        anyhow::anyhow!(
            "infra_image_tags_json for {whose} has wrong shape (expected \
             {{node: {{image: tag}}}}): {e}"
        )
    })
}

/// Decode one `infra_node.units_json` value (`{unit: UnitRuntime}`, the
/// roster the supervisor's apply stamps) into the typed map. THE
/// canonical decode for the column: the broker (the supervisor's
/// reads), the dispatcher's row reader and the referenced-images
/// keep-set all route through it, so no reader hand-rolls the shape.
/// Loud on a bad row, never coerced to empty (an empty roster would
/// read as "nothing runs here" to the stop loop and shrink the keep-set
/// below what still runs). `whose` names the row.
///
/// The one corruption this column has historically carried is a
/// `{"status": ...}` stub: a per-unit status stamp that landed on a
/// unit missing from the roster, back when the broker's SQL seeded the
/// member with COALESCE (it now fences on membership and refuses).
/// Such a stub lacks every other field, so it fails this decode; the
/// error carries the exact repair statement (drop every entry with no
/// `stop_behavior`), because a stub is undeletable through any weft
/// surface and would otherwise brick this row's reads for good.
// SYNC: the column's writers <-> crates/weft-broker/src/handlers.rs
//       (write_apply_row, the full-map stamp; supervisor_set_status,
//       the per-unit status patch)
pub fn decode_units_json(
    value: serde_json::Value,
    project_id: uuid::Uuid,
    node_id: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, UnitRuntime>> {
    serde_json::from_value(value).map_err(|e| {
        anyhow::anyhow!(
            "infra_node.units_json for project={project_id} node={node_id} has wrong \
             shape (expected {{unit: UnitRuntime}}): {e}. If the row holds a \
             status-only stub entry, repair it with: {}",
            units_json_repair_sql(project_id, node_id)
        )
    })
}

/// The statement that drops every status-only stub entry from one
/// row's `units_json` (see `decode_units_json`): keeps exactly the
/// entries carrying `stop_behavior`, which every real `UnitRuntime`
/// has. Printed in the decode error for the operator to run by hand,
/// and executed by the broker's db suite to prove it heals a row.
pub fn units_json_repair_sql(project_id: uuid::Uuid, node_id: &str) -> String {
    format!(
        "UPDATE infra_node SET units_json = (SELECT COALESCE(jsonb_object_agg(k, v), \
         '{{}}'::jsonb) FROM jsonb_each(units_json) AS e(k, v) WHERE v ? 'stop_behavior') \
         WHERE project_id = '{project_id}' AND node_id = '{node_id}';"
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRemoveNodeRequest {
    /// The supervisor's claim identity (`WEFT_POD_NAME`); the broker
    /// gates the cascade-delete on it holding the project's live
    /// `infra_owner` lease. See [`SupervisorSetStatusRequest::pod_name`].
    pub pod_name: String,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    /// The terminate removing it: the one command of this copy the
    /// cascade does not cancel, since it is the one finishing.
    pub command_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRemoveNodeResponse {
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandCompleteRequest {
    /// The supervisor's claim identity (`WEFT_POD_NAME`); the broker
    /// gates the terminal write on it holding the project's live
    /// `infra_owner` lease, so a displaced pod can't complete a command
    /// the new owner must re-run. See [`SupervisorSetStatusRequest::pod_name`].
    pub pod_name: String,
    pub command_id: i64,
    pub error: Option<String>,
    /// True when the supervisor halted the command because the user
    /// requested cancellation (`cancel_requested` on the row). The
    /// broker records outcome `cancelled` (never a failure); `error`
    /// then carries the halt point for the outcome message.
    #[serde(default)]
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandCompleteResponse {}

/// Poll target for an executing supervisor: has the user requested
/// cancellation of this claimed command? Checked between cluster calls
/// and inside readiness/drain waits so a cancel interrupts promptly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandCancelRequestedRequest {
    pub command_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandCancelRequestedResponse {
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorTriggerDepsRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorTriggerDep {
    pub infra_node_id: String,
    pub trigger_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorTriggerDepsResponse {
    pub deps: Vec<SupervisorTriggerDep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRunningCountRequest {
    pub project_id: Uuid,
    /// Whose runs: a member's copy going waits on that member's runs
    /// only; the shared copies wait on every run.
    #[serde(default)]
    pub copies: weft_core::member::Copies,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRunningCountResponse {
    /// Count of non-suspended in-flight execution colors for this
    /// project. Drives the supervisor's `running_policy=wait`
    /// readiness check before scaling / deleting.
    pub running_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraCommandInFlightRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraCommandInFlightResponse {
    /// The project's uncompleted supervisor commands (apply / stop /
    /// terminate), each as the copies it acts on. The health loop stands
    /// down for exactly those copies while they are here, so it never
    /// fights a user action over a copy's status, and leaves every other
    /// copy's health running.
    pub commands: Vec<InFlightCommand>,
}

/// Which copies one uncompleted infra command acts on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightCommand {
    /// The node it names; `None` for every node of the project.
    pub node_id: Option<String>,
    pub copies: weft_core::member::Copies,
}

impl InFlightCommand {
    /// Whether it acts on the copy of `node_id` owned by `member`.
    // SYNC: InFlightCommand::reaches <-> crates/weft-broker-client/src/lifecycle_command.rs (command_reaches_copy)
    pub fn reaches(&self, node_id: &str, member: Option<&weft_core::member::MemberId>) -> bool {
        self.node_id.as_deref().is_none_or(|n| n == node_id) && self.copies.admits(member)
    }
}

// ---------- Signals ----------

/// The activation statuses whose signal rows belong in a listener pod's
/// in-RAM registry: the activation governing the signal (its trigger's
/// for an entry, the trigger's that fired its run for a wait) is being
/// activated (the rehydrate at the end of activate runs before the flip
/// to active) or live. A signal no activation governs reads as live. A
/// hibernated or parked activation keeps its rows in the table so
/// reactivate can restore them, but the pod was told to forget them at
/// deactivate, and a pod restarting must not bring them back. Both
/// readers of the table use this one list: the broker's
/// `signal/list_for_pod` (what a booting pod rehydrates) and the
/// dispatcher's `listener_inspect` (what it counts as placed).
pub const LISTENER_HELD_STATUSES: [&str; 2] =
    [ProjectStatus::Activating.as_str(), ProjectStatus::Active.as_str()];

/// The join every "is this signal held" read goes through: the
/// activation governing signal `s` as `a` (absent when none governs it).
pub const SIGNAL_ACTIVATION_JOIN: &str = "LEFT JOIN trigger_activation a \
    ON a.project_id = s.project_id AND a.trigger = s.activation_trigger \
    AND a.member_id IS NOT DISTINCT FROM s.member_id";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalListForPodRequest {
    /// The pooled listener pod asking for the signals placed on it.
    /// The broker returns rows where `signal.listener_pod = pod_name`
    /// and the governing activation's status is one of
    /// [`LISTENER_HELD_STATUSES`].
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalListForPodResponse {
    pub rows: Vec<SignalRowWire>,
}

wire_enum! {
    /// The user-facing surface a signal exposes. `PublicEntry`
    /// pairs with a `mount_path`; `TaskCallback` and `Internal`
    /// don't. The listener routes incoming fires by this kind.
    pub enum SignalSurfaceKind {
        PublicEntry = "public_entry",
        TaskCallback = "task_callback",
        Internal = "internal",
    }
}

wire_enum! {
    /// Authentication scheme attached to a signal. `None` means
    /// any caller with the URL can fire; `Connection` verifies the
    /// caller against a stored connection (`auth_config` names it).
    pub enum SignalAuthKind {
        None = "none",
        Connection = "connection",
    }
}

impl SignalRowWire {
    /// Reassemble the typed `SignalRouting` from this wire row's
    /// flat columns. The wire stores `surface_kind` as a tag and
    /// keeps the `PublicEntry` pattern in `mount_path` and its
    /// methods in `mount_methods`; the typed
    /// `SignalSurface::PublicEntry { path, methods }` packs them
    /// together. One adapter, one source of truth for the projection.
    ///
    /// Returns Err if the row violates a documented invariant
    /// (`PublicEntry` with `mount_path = NULL`). The signal schema
    /// has a partial unique index on `mount_path WHERE NOT NULL`,
    /// so reaching this branch means the writer drifted from the
    /// schema. Fail loud rather than silently mounting at `""`.
    pub fn to_routing(&self) -> Result<weft_core::primitive::SignalRouting, &'static str> {
        routing_from_columns(
            self.surface_kind,
            self.mount_path.as_deref(),
            &self.mount_methods,
            self.auth_kind,
            self.auth_config.clone(),
        )
    }
}

/// The one row-columns -> [`SignalRouting`] projection, shared by the
/// listener's rehydrate (via [`SignalRowWire::to_routing`]) and the
/// dispatcher's pod-move restore, so the two readers of the same
/// columns can never drift.
pub fn routing_from_columns(
    surface_kind: SignalSurfaceKind,
    mount_path: Option<&str>,
    mount_methods: &[String],
    auth_kind: SignalAuthKind,
    auth_config: Option<Value>,
) -> Result<weft_core::primitive::SignalRouting, &'static str> {
    use weft_core::primitive::{SignalAuth, SignalRouting, SignalSurface};
    let surface = match surface_kind {
        SignalSurfaceKind::PublicEntry => {
            let raw = mount_path.ok_or("signal row: PublicEntry with NULL mount_path")?;
            // Stored mount_path has a leading `/`; the surface
            // field doesn't carry it.
            let path = raw.strip_prefix('/').unwrap_or(raw).to_string();
            SignalSurface::PublicEntry { path, methods: mount_methods.to_vec() }
        }
        SignalSurfaceKind::TaskCallback => SignalSurface::TaskCallback,
        SignalSurfaceKind::Internal => SignalSurface::Internal,
    };
    let auth = match auth_kind {
        SignalAuthKind::None => SignalAuth::None,
        SignalAuthKind::Connection => SignalAuth::Connection,
    };
    Ok(SignalRouting { surface, auth, auth_config: auth_config.unwrap_or(Value::Null) })
}

/// Wire shape for a row of the signal table that the listener
/// rehydrates from. Mirrors the columns the listener reads today.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRowWire {
    pub token: String,
    /// Tenant this signal belongs to. A pooled listener rehydrates
    /// signals from many tenants, so each row carries its own tenant;
    /// the listener stamps it on the registry entry (and thus on any
    /// held-event fire). Filled per row by the `list_for_pod` path the
    /// listener rehydrates from.
    pub tenant_id: String,
    /// Whose signal it is (`None` for a shared one), from the row's
    /// `project_id` and `member_id`: the rehydrated signal reads through
    /// that member's connections alone, as it did when first registered.
    pub for_member: Option<weft_core::member::MemberScope>,
    pub node_id: String,
    pub spec_json: String,
    pub is_resume: bool,
    pub color: Option<String>,
    pub surface_kind: SignalSurfaceKind,
    pub mount_path: Option<String>,
    /// The HTTP methods a `PublicEntry` serves, uppercase; empty = any
    /// (the `signal.mount_methods` column, `'{}'` for every other
    /// surface).
    #[serde(default)]
    pub mount_methods: Vec<String>,
    pub auth_kind: SignalAuthKind,
    pub auth_config: Option<Value>,
    /// Opaque per-kind state. `{}` for kinds that don't persist
    /// anything. Timer uses it to recover the absolute
    /// `next_fire_at_unix_ms` across listener restarts; other stateful
    /// kinds use the same channel for whatever state they need to
    /// survive a restart. The `signal.kind_state` column is
    /// `JSONB NOT NULL DEFAULT '{}'::jsonb`, so the broker always
    /// emits a value; missing on the wire is schema drift.
    pub kind_state: Value,
    /// The kind_state write-fence version the row is at (the
    /// `signal.kind_state_seq` column). The rehydrating pod resumes
    /// its durable cursor writes at `seq + 1` so a restart can never
    /// regress the fence.
    pub kind_state_seq: i64,
    /// The signal's current placement generation. On rehydrate the pod
    /// re-arms the signal under this generation and stamps it on the
    /// held-event fires it enqueues, so the broker's stale-fire fence
    /// stays consistent across a listener restart.
    pub placement_generation: i64,
}

// ---------- Provider-event serving (listener <-> broker) ----------

/// The listener's event-source resolve: taken from the registered
/// signal (the tenant traveled with it at registration). The broker
/// answers from the store's resolve; this crate holds the wire shape
/// so listener and broker cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerResolveRequest {
    pub tenant: String,
    /// Whose signal the connection serves (`None` for a shared one), as
    /// registered, the same trust as `tenant`: a member's trigger reads
    /// through that member's connection alone. Carried rather than read
    /// off the signal row, because a trigger resolves while it is being
    /// registered, before its row exists.
    pub for_member: Option<weft_core::member::MemberScope>,
    pub access_id: String,
    pub service: String,
    /// The stored values the SIGNAL's input declared it needs (the
    /// trigger half of `requiresValues`): a held pipe signing in with
    /// the connection's own values is refused here, naming the
    /// missing one, rather than dialing forever with a blank address.
    #[serde(default)]
    pub required_values: Vec<String>,
}

/// What serving a subscription needs: the connection's resolved auth
/// (steps + exactly the values they interpolate), the service's event
/// topics, and the extra stored values the recipes' own calls name.
/// ONE definition (the store resolves it, the broker relays it, the
/// listener consumes it), living beside the recipe vocabulary in
/// weft-core; re-exported here under both its names so every wire
/// consumer imports it from the protocol.
pub use weft_core::access::events::ResolvedEventSource;
pub use weft_core::access::events::ResolvedEventSource as ListenerResolvedSource;

/// Ask the broker to make sure a live provider subscription serves
/// this signal (subscribing or renewing as needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEnsureRequest {
    pub tenant: String,
    pub service: String,
    pub topic: String,
    pub access_id: String,
    pub signal_token: String,
    /// Whose signal the subscription serves (`None` for a shared one),
    /// as registered: the subscription dials through that member's
    /// connection alone.
    pub for_member: Option<weft_core::member::MemberScope>,
    #[serde(default)]
    pub params: std::collections::BTreeMap<String, String>,
}

/// When the provider will stop sending (unix seconds; `None` =
/// never), so the serving loop knows when to come back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEnsureResponse {
    pub expires_at_unix: Option<i64>,
}

/// Stop (at the provider) and forget every subscription serving a
/// signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionDropRequest {
    pub tenant: String,
    pub signal_token: String,
}

/// One caller of a live route, as the dispatcher forwards it to the
/// broker to check against the connection the route is gated by. The
/// broker holds the connection (its `verify` recipe and its stored
/// material); the dispatcher never sees a secret. The body is base64
/// (an HMAC scheme signs the exact bytes); a scheme that does not read
/// it is handed an empty one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallerVerifyRequest {
    pub tenant: String,
    /// The member whose route this is, when the gate is their own
    /// connection (a member's trigger); `None`: a shared route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_member: Option<weft_core::member::MemberScope>,
    pub access_id: String,
    pub service: String,
    pub method: String,
    /// The path as called under the project (no tenant, no leading
    /// slash).
    pub path: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub query: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub body_b64: String,
}

/// The broker's yes on a caller: the identity the scheme established
/// (a JWT's claims, `{"key": i}` for a shared key, `{}` for an HMAC),
/// which rides the request as its `caller`. A no is the door's `401`,
/// flat, with the reason in the broker's log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallerVerified {
    pub identity: Value,
}

/// One raw push, as the dispatcher forwards it to the broker: the
/// exact bytes (signatures hash them; base64 so the wrapper stays
/// JSON), the headers, and the query string (one handshake shape
/// answers a query parameter).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventVerifyRequest {
    pub service: String,
    pub topic: String,
    pub body_b64: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub query: std::collections::BTreeMap<String, String>,
    /// The HTTP method the provider called with; some signing schemes
    /// cover it.
    #[serde(default = "default_event_method")]
    pub method: String,
}

fn default_event_method() -> String {
    "POST".to_string()
}

/// One connection an inbound event concerns, as the broker answers it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTargetWire {
    pub access_id: String,
    pub tenant_id: String,
}

/// The broker's verdict on one push. `Challenge` and `Drop` end the
/// exchange (the dispatcher relays the answer / answers 200);
/// `Deliver` hands the dispatcher everything the match+fire needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventVerdict {
    /// The push is the provider's address-proving handshake; relay
    /// this body back with this content type.
    Challenge { body: String, content_type: String },
    /// Verified, but nothing here subscribes to it (a valid install
    /// nobody registered a trigger for): answer 200 and drop.
    Drop { reason: String },
    /// Verified and routed: fire the matching subscriptions.
    Deliver {
        /// The event as the service's NAMED fields (predicates and
        /// node payloads speak these).
        named_event: serde_json::Value,
        /// Matched by provider account: the connections the event
        /// concerns; the dispatcher finds their subscriptions.
        targets: Vec<EventTargetWire>,
        /// Matched by minted subscription id: exactly one signal.
        signal_token: Option<String>,
    },
}

#[cfg(test)]
mod wire_enum_roundtrips {
    use super::*;
    // One generated round-trip test per wire enum. Adding a new
    // wire_enum! means adding its name here; missing one is a
    // visible omission in this single list.
    wire_enum_roundtrip_tests!(
        InfraLifecycleVerb,
        InfraNodeStatus,
        ProjectStatus,
        FailureStage,
        InfraEventKind,
        StartMode,
        LifecycleOutcome,
        SignalSurfaceKind,
        SignalAuthKind,
    );
}

#[cfg(test)]
mod supervisor_protocol_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn publish_access_round_trip() {
        // The request carries no project: the broker resolves that
        // from the execution, so a worker cannot name someone else's.
        let spec: weft_core::AccessSpec = serde_json::from_value(json!({
            "service": "postgres",
            "acquisition": { "kind": "static", "fields": [{ "name": "host" }] },
        }))
        .unwrap();
        let req = PublishAccessRequest {
            color: "c1".into(),
            node_id: "db".into(),
            service: "postgres".into(),
            spec,
            values: [("host".to_string(), "db.svc".to_string())].into_iter().collect(),
            label: Some("db".into()),
            per_member: true,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["per_member"], true, "whose copy published it rides as a flag; the member is the run's");
        assert_eq!(v["color"], "c1");
        assert_eq!(v["node_id"], "db");
        assert_eq!(v["values"]["host"], "db.svc");
        assert!(v.get("project_id").is_none(), "the project is never on the wire");

        // Both verbs answer the SAME shape, so a connection read back
        // is the same value as the one published.
        let published: PublishAccessResponse = serde_json::from_value(json!({
            "connection": { "connection_id": "id-1", "identity": "weft@db/app" },
        }))
        .unwrap();
        assert_eq!(published.connection.connection_id, "id-1");
        assert_eq!(published.connection.identity.as_deref(), Some("weft@db/app"));

        // Each answer names itself, so a peer that does not speak
        // this shape fails here rather than being read as the
        // perfectly plausible "nothing published yet".
        let none: PublishedAccessResponse =
            serde_json::from_value(json!({ "state": "nothing_published" })).unwrap();
        assert!(none.connection().is_none(), "nothing published yet");
        assert!(
            serde_json::from_value::<PublishedAccessResponse>(json!({})).is_err(),
            "an untagged answer is refused, not read as nothing published"
        );
        let some: PublishedAccessResponse = serde_json::from_value(json!({
            "state": "published",
            "connection": { "connection_id": "id-1", "identity": null },
        }))
        .unwrap();
        assert_eq!(some.connection().expect("a connection").connection_id, "id-1");
    }

    #[test]
    fn project_fetch_definition_round_trip() {
        let req = ProjectFetchDefinitionRequest {
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            expected_hash: "abc123".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["project_id"], "00000000-0000-0000-0000-0000000000a1");
        assert_eq!(v["expected_hash"], "abc123");
        let resp: ProjectFetchDefinitionResponse = serde_json::from_value(json!({
            "project_json": "{\"nodes\":[]}",
            "definition_hash": "abc123",
        }))
        .unwrap();
        assert_eq!(resp.project_json, "{\"nodes\":[]}");
        assert_eq!(resp.definition_hash, "abc123");
    }

    #[test]
    fn sync_ownership_round_trip() {
        let req = SupervisorSyncOwnershipRequest {
            pod_name: "sup-1".into(),
            mem_pressure: 0.5,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "pod_name": "sup-1", "mem_pressure": 0.5 }));
        let resp: SupervisorSyncOwnershipResponse = serde_json::from_value(json!({
            "owned": [
                {
                    "project_id": "00000000-0000-0000-0000-0000000000a1",
                    "tenant_id": "alice",
                    "project_namespace": "wft-project-alice-p1",
                    "status": "active",
                    "health_parked": false,
                }
            ],
            "claimed": ["00000000-0000-0000-0000-0000000000a1"],
        }))
        .unwrap();
        assert_eq!(resp.owned.len(), 1);
        assert_eq!(resp.owned[0].project_id.to_string(), "00000000-0000-0000-0000-0000000000a1");
        assert_eq!(resp.owned[0].status, ProjectStatus::Active);
        assert_eq!(resp.claimed, vec![resp.owned[0].project_id]);
    }

    #[test]
    fn claim_round_trip() {
        let req = SupervisorClaimCommandRequest {
            claimer_pod: "sup-1".into(),
            busy_projects: vec![uuid::Uuid::from_u128(0xa1)],
            wait_ms: 25_000,
        };
        assert_eq!(
            serde_json::to_value(&req).unwrap(),
            json!({
                "claimer_pod": "sup-1",
                "busy_projects": ["00000000-0000-0000-0000-0000000000a1"],
                "wait_ms": 25_000,
            })
        );
        let command = SupervisorCommandRow {
            id: 7,
            project_id: uuid::Uuid::from_u128(0xa1),
            node_id: Some("db".into()),
            copies: weft_core::member::Copies::Member(weft_core::member::MemberId::new("user-42").unwrap()),
            verb: InfraLifecycleVerb::Apply,
            running_policy: None,
            spec_json: None,
            force: false,
            drain_timeout_secs: 300,
        };
        for claim in [SupervisorClaim::Command(command), SupervisorClaim::UnownedWork, SupervisorClaim::Nothing] {
            let back: SupervisorClaim =
                serde_json::from_value(serde_json::to_value(&claim).unwrap()).unwrap();
            assert_eq!(format!("{back:?}"), format!("{claim:?}"));
        }
        assert_eq!(serde_json::to_value(SupervisorClaim::UnownedWork).unwrap(), json!({ "kind": "unowned_work" }));
    }

    #[test]
    fn owned_projects_round_trip() {
        let req = SupervisorOwnedProjectsRequest {
            pod_name: "sup-1".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "pod_name": "sup-1" }));
        let resp: SupervisorOwnedProjectsResponse = serde_json::from_value(json!({
            "owned": [
                {
                    "project_id": "00000000-0000-0000-0000-0000000000a1",
                    "tenant_id": "alice",
                    "project_namespace": "wft-project-alice-p1",
                    "status": "active",
                    "health_parked": false,
                }
            ]
        }))
        .unwrap();
        assert_eq!(resp.owned.len(), 1);
        assert_eq!(resp.owned[0].tenant_id, "alice");
    }

    /// `SupervisorProject::status` has NO `serde(default)`, so a
    /// missing field must fail deserialize. Pin the property so a
    /// future re-add of the default would break CI.
    #[test]
    fn owned_project_missing_status_fails() {
        let res: Result<SupervisorOwnedProjectsResponse, _> = serde_json::from_value(json!({
            "owned": [
                { "project_id": "00000000-0000-0000-0000-0000000000a1", "project_namespace": "wft-project-alice-p1" }
            ]
        }));
        assert!(
            res.is_err(),
            "missing status should fail deserialize; got {:?}",
            res
        );
    }

    /// `SupervisorProject::health_parked` also has NO `serde(default)`:
    /// a missing field is schema drift and must fail loud (the broker +
    /// supervisor deploy together). Pin it so a future `serde(default)`
    /// slipping in would break CI rather than silently defaulting the
    /// auto-recover gate to false.
    #[test]
    fn owned_project_missing_health_parked_fails() {
        let res: Result<SupervisorOwnedProjectsResponse, _> = serde_json::from_value(json!({
            "owned": [
                { "project_id": "00000000-0000-0000-0000-0000000000a1", "project_namespace": "wft-project-alice-p1", "status": "active" }
            ]
        }));
        assert!(
            res.is_err(),
            "missing health_parked should fail deserialize; got {:?}",
            res
        );
    }

    #[test]
    fn infra_node_response_round_trip() {
        let json = json!({
            "nodes": [
                {
                    "node_id": "tgi",
                    "instance_id": "wn-abc-tgi-12",
                    "status": "running",
                    "applied_spec_hash": "deadbeef",
                    "endpoints": { "api": "http://x.svc:8080" },
                    "public_paths": { "api": "/infra/ns/wn-abc-tgi-12/hooks" },
                    "preserve_pvcs": ["model-cache"],
                    "units": { "main": {
                        "status": "running",
                        "stop_behavior": { "kind": "scale_to_zero" },
                        "flaky_after_seconds": 30,
                        "recovery_after_seconds": 30
                    }}
                }
            ]
        });
        let resp: SupervisorInfraNodesResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.nodes[0].addresses.urls.get("api").unwrap(), "http://x.svc:8080");
        assert_eq!(resp.nodes[0].addresses.public_paths.get("api").unwrap(), "/infra/ns/wn-abc-tgi-12/hooks");
        assert_eq!(resp.nodes[0].units.get("main").unwrap().status, InfraNodeStatus::Running);
        assert_eq!(resp.nodes[0].applied_spec_hash.as_deref(), Some("deadbeef"));
        assert_eq!(resp.nodes[0].preserve_pvcs, vec!["model-cache".to_string()]);
    }

    /// `SupervisorInfraNode::preserve_pvcs` has NO `serde(default)`:
    /// pre-prod, broker + supervisor deploy together, missing field
    /// is schema drift. Pin the property so a future re-add of the
    /// default breaks CI.
    #[test]
    fn infra_node_missing_preserve_pvcs_fails() {
        let json = json!({
            "nodes": [
                {
                    "node_id": "tgi",
                    "instance_id": "wn-abc-tgi-12",
                    "status": "running",
                    "applied_spec_hash": "deadbeef",
                    "endpoints": {}
                }
            ]
        });
        let res: Result<SupervisorInfraNodesResponse, _> = serde_json::from_value(json);
        assert!(res.is_err(), "missing preserve_pvcs must fail deserialize");
    }

    #[test]
    fn set_applied_request_round_trip() {
        let req = SupervisorSetAppliedRequest {
            pod_name: "weft-infra-supervisor-abc".into(),
            command_id: 7,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "tgi".into(),
            member: None,
            instance_id: "wn-abc-tgi-12".into(),
            applied_spec_hash: "deadbeef".into(),
            addresses: AppliedEndpoints {
                urls: [("api".to_string(), "http://x".to_string())].into(),
                public_paths: [("api".to_string(), "/infra/wft-x/wn-abc-tgi-12/hooks".to_string())].into(),
            },
            namespace: "wft-x".into(),
            preserve_pvcs: vec!["data".into()],
            units: std::collections::BTreeMap::new(),
        };
        let v = serde_json::to_value(&req).unwrap();
        // Flattened: the two maps sit on the request itself, and the
        // URLs keep the `endpoints` name the broker writes to its column.
        assert_eq!(v["endpoints"]["api"], "http://x");
        assert_eq!(v["public_paths"]["api"], "/infra/wft-x/wn-abc-tgi-12/hooks");
        let back: SupervisorSetAppliedRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.addresses, req.addresses);
        assert_eq!(back.command_id, 7);
        assert_eq!(back.preserve_pvcs, vec!["data".to_string()]);
        assert_eq!(back.instance_id, "wn-abc-tgi-12");
    }

    #[test]
    fn resolve_connection_request_round_trip() {
        let req = ResolveConnectionRequest {
            color: "c1".into(),
            node_id: "ask".into(),
            frames: vec![weft_core::frames::Frame::Loop { index: 3 }],
            node_type: "openrouter.inference".into(),
            connection_id: "11111111-2222-3333-4444-555555555555".into(),
            service: "openrouter".into(),
            required_permissions: vec!["drive.readonly".into()],
            required_values: vec!["imap_host".into()],
            expected_duration_secs: 120,
        };
        // The full literal object pins the FIELD NAMES on the wire: a
        // symmetric struct-field rename round-trips fine but breaks the
        // peer, so equality on the serialized form is the real contract.
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                // No project: the broker resolves it from the colour,
                // so a worker never names one.
                "color": "c1", "node_id": "ask",
                "frames": [{"index": 3}], "node_type": "openrouter.inference",
                "connection_id": "11111111-2222-3333-4444-555555555555",
                "service": "openrouter",
                "required_permissions": ["drive.readonly"],
                "required_values": ["imap_host"],
                "expected_duration_secs": 120
            })
        );
        let back: ResolveConnectionRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.frames, vec![weft_core::frames::Frame::Loop { index: 3 }]);
        assert_eq!(back.service, "openrouter");
        assert_eq!(back.expected_duration_secs, 120);
    }

    #[test]
    fn resolve_connection_response_round_trip_both_relay_arms() {
        for relay_url in [Some("http://relay/svc".to_string()), None] {
            let resp = ResolveConnectionResponse {
                values: [("token".to_string(), "cred".to_string())].into_iter().collect(),
                auth: vec![weft_core::access::spec::AuthStep::Header {
                    name: "Authorization".into(),
                    value: weft_core::access::spec::Template::new("Bearer {token}"),
                }],
                identity: Some("Q @ Acme".into()),
                relay_url: relay_url.clone(),
                owner: weft_core::CredentialOwner::Platform,
            };
            // Field names pinned literally (a symmetric rename would
            // round-trip but break the peer).
            let v = serde_json::to_value(&resp).unwrap();
            assert_eq!(
                v,
                json!({
                    "values": { "token": "cred" },
                    "auth": [{ "kind": "header", "name": "Authorization",
                               "value": "Bearer {token}" }],
                    "identity": "Q @ Acme",
                    "relay_url": relay_url,
                    "owner": "platform"
                })
            );
            let back: ResolveConnectionResponse = serde_json::from_value(v).unwrap();
            assert_eq!(back.values["token"], "cred");
            assert_eq!(back.relay_url, relay_url);
            assert_eq!(back.owner, weft_core::CredentialOwner::Platform);
        }
    }

    #[test]
    fn release_connection_request_round_trip() {
        let req = ReleaseConnectionRequest {
            color: "c1".into(),
            values: [("token".to_string(), "cred".to_string())].into_iter().collect(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "color": "c1", "values": { "token": "cred" } }));
        let back: ReleaseConnectionRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.values["token"], "cred");
        assert_eq!(back.color, "c1");
    }

    #[test]
    fn set_applied_missing_preserve_pvcs_fails() {
        // Write path mirrors the read path: no serde(default) on
        // preserve_pvcs, so a missing field is loud schema drift.
        let v = json!({
            "command_id": 7, "project_id": "00000000-0000-0000-0000-0000000000a1", "node_id": "tgi",
            "instance_id": "i", "applied_spec_hash": "h",
            "endpoints": {}, "namespace": "ns", "units": {}
        });
        let res: Result<SupervisorSetAppliedRequest, _> = serde_json::from_value(v);
        assert!(res.is_err(), "missing preserve_pvcs must fail deserialize");
    }

    #[test]
    fn set_provisioning_request_round_trip() {
        let req = SupervisorSetProvisioningRequest {
            pod_name: "weft-infra-supervisor-abc".into(),
            command_id: 9,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "tgi".into(),
            member: None,
            instance_id: "wn-abc-tgi-12".into(),
            namespace: "wft-x".into(),
            preserve_pvcs: vec!["data".into()],
            units: std::collections::BTreeMap::new(),
        };
        let v = serde_json::to_value(&req).unwrap();
        let back: SupervisorSetProvisioningRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.command_id, 9);
        assert_eq!(back.preserve_pvcs, vec!["data".to_string()]);
    }

    #[test]
    fn set_provisioning_missing_preserve_pvcs_fails() {
        let v = json!({
            "command_id": 9, "project_id": "00000000-0000-0000-0000-0000000000a1", "node_id": "tgi",
            "instance_id": "i", "namespace": "ns"
        });
        let res: Result<SupervisorSetProvisioningRequest, _> = serde_json::from_value(v);
        assert!(res.is_err(), "missing preserve_pvcs must fail deserialize");
    }

    #[test]
    fn command_row_running_policy_default() {
        // A row that omits `running_policy` deserializes to None
        // (dispatcher verbs + Apply leave the column NULL).
        let v = json!({
            "id": 1,
            "project_id": "00000000-0000-0000-0000-0000000000a1",
            "verb": "stop"
        });
        let row: SupervisorCommandRow = serde_json::from_value(v).unwrap();
        assert_eq!(row.running_policy, None);
        assert_eq!(row.verb, InfraLifecycleVerb::Stop);
        assert_eq!(row.node_id, None);
    }

    #[test]
    fn deactivate_spec_round_trip() {
        let s = DeactivateSpec {
            mode: DeactivationMode::Hibernate,
            grace_minutes: 5,
            running_policy: RunningPolicy::Wait,
            drain_timeout_secs: Some(120),
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["drainTimeoutSecs"], 120);
        let back: DeactivateSpec = serde_json::from_value(v).unwrap();
        assert_eq!(s, back);
    }

    fn take_down(broken: Vec<InfraCopy>) -> TakeDownReaders {
        TakeDownReaders {
            spec: DeactivateSpec {
                mode: DeactivationMode::Park,
                grace_minutes: 0,
                running_policy: RunningPolicy::Cancel,
                drain_timeout_secs: None,
            },
            reach: TakeDownReach::ReadersOf { broken },
        }
    }

    fn ada_svc() -> InfraCopy {
        InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("ada").unwrap()) }
    }

    #[test]
    fn lifecycle_spec_round_trip_deactivate_and_reactivate() {
        let d = LifecycleSpec::Deactivate(take_down(vec![
            ada_svc(),
            InfraCopy { node_id: "db".into(), member: None },
        ]));
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["verb"], "deactivate");
        assert_eq!(v["reach"]["kind"], "readers_of");
        assert_eq!(v["reach"]["broken"], json!([{ "node_id": "svc", "member": "ada" }, { "node_id": "db" }]));
        let back: LifecycleSpec = serde_json::from_value(v).unwrap();
        assert_eq!(d, back);

        let whole = LifecycleSpec::Deactivate(TakeDownReaders { reach: TakeDownReach::Project, ..take_down(vec![]) });
        let v = serde_json::to_value(&whole).unwrap();
        assert_eq!(v["reach"], json!({ "kind": "project" }));
        assert_eq!(serde_json::from_value::<LifecycleSpec>(v).unwrap(), whole);

        let r = LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![ada_svc()] });
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["verb"], "reactivate");
        assert_eq!(v["still_broken"], json!([{ "node_id": "svc", "member": "ada" }]));
        let back: LifecycleSpec = serde_json::from_value(v).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn infra_event_record_then_decode_round_trips_every_variant() {
        // Construct one of each kind, encode via into_record(),
        // decode via from_kind_and_payload(), assert equal. Drift
        // on either side (writer renames a field, reader forgets
        // an arm) trips this test.
        let cases = vec![
            InfraEvent::Flaky(FlakyPayload {
                desired: 3,
                ready: 1,
                reason: Some("crashloop".into()),
            }),
            InfraEvent::Recovered,
            InfraEvent::Failed(FailedPayload {
                stage: FailureStage::Apply,
                message: "apply rejected".into(),
            }),
            InfraEvent::Stopped,
            InfraEvent::Terminated,
            InfraEvent::Started(StartedPayload {
                instance_id: "inst1".into(),
                mode: StartMode::Fresh,
            }),
            InfraEvent::Notify(NotifyPayload {
                protocol: "p".into(),
                channel: "ops".into(),
            }),
            InfraEvent::ProtocolConfigError(ProtocolConfigErrorPayload {
                error: "bad json".into(),
            }),
        ];
        for ev in cases {
            let (kind, payload) = ev.clone().into_record();
            let back = InfraEvent::from_kind_and_payload(kind, &payload)
                .expect("decode after into_record");
            assert_eq!(ev, back, "round-trip mismatch for {kind:?}");
        }
    }

    #[test]
    fn command_row_per_node_serializes() {
        let row = SupervisorCommandRow {
            id: 42,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: Some("n".into()),
            copies: weft_core::member::Copies::Shared,
            verb: InfraLifecycleVerb::Terminate,
            running_policy: Some(RunningPolicy::Cancel),
            spec_json: None,
            force: false,
            drain_timeout_secs: 120,
        };
        let v = serde_json::to_value(&row).unwrap();
        assert_eq!(v["node_id"], "n");
        assert_eq!(v["verb"], "terminate");
        assert_eq!(v["running_policy"], "cancel");
        assert_eq!(v["drain_timeout_secs"], 120);
        // A payload WITHOUT the field (an older writer) decodes to the
        // shared default, never zero.
        let mut old = v.clone();
        old.as_object_mut().unwrap().remove("drain_timeout_secs");
        let back: SupervisorCommandRow = serde_json::from_value(old).unwrap();
        assert_eq!(back.drain_timeout_secs, DEFAULT_DRAIN_TIMEOUT_SECS);
    }

    /// `LifecycleSpec::into_row_columns` no longer populates the
    /// running_policy column for dispatcher verbs (it's NULL in
    /// the DB; Deactivate carries policy inside spec_json). Pin
    /// the property so a future re-add of the column-as-SoT
    /// for these verbs breaks CI.
    #[test]
    fn into_row_columns_returns_none_policy_for_dispatcher_verbs() {
        let d = LifecycleSpec::Deactivate(take_down(vec![ada_svc()]));
        let (verb, policy, spec_json) = d.clone().into_row_columns();
        assert_eq!(verb, InfraLifecycleVerb::Deactivate);
        assert_eq!(policy, None);
        // The row columns carry the whole take-down, broken copies too,
        // and read back as what was written.
        assert_eq!(LifecycleSpec::from_row_columns(verb, spec_json).unwrap(), d);

        let r = LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![ada_svc()] });
        let (verb, policy, spec_json) = r.clone().into_row_columns();
        assert_eq!(verb, InfraLifecycleVerb::Reactivate);
        assert_eq!(policy, None);
        assert_eq!(LifecycleSpec::from_row_columns(verb, spec_json).unwrap(), r);
        assert!(matches!(
            LifecycleSpec::from_row_columns(InfraLifecycleVerb::Reactivate, None),
            Err(FromRowColumnsError::ReactivateMissingSpec)
        ));
    }

    /// A take-down aimed at broken copies names at least one, and one
    /// aimed at the whole project names none; a spec the running-work
    /// rule refuses is refused either way. A recovery has no rule: an
    /// empty still-broken set restores everything the loop took.
    #[test]
    fn a_take_down_names_its_broken_copies() {
        assert!(take_down(vec![ada_svc()]).validate().is_ok());
        assert!(take_down(vec![]).validate().is_err());
        assert!(TakeDownReaders { reach: TakeDownReach::Project, ..take_down(vec![]) }.validate().is_ok());
        assert!(LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![] }).validate().is_ok());
        let mut wipe_wait = take_down(vec![ada_svc()]);
        wipe_wait.spec.mode = DeactivationMode::Wipe;
        wipe_wait.spec.running_policy = RunningPolicy::Wait;
        assert!(wipe_wait.validate().is_err());
    }

    #[test]
    fn set_status_request_optional_fields_skip_when_none() {
        let r = SupervisorSetStatusRequest {
            pod_name: "weft-infra-supervisor-abc".into(),
            command_id: None,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "n".into(),
            member: None,
            unit: None,
            status: InfraNodeStatus::Running,
            failure_stage: None,
            failure_message: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: SupervisorSetStatusRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.status, InfraNodeStatus::Running);
        assert_eq!(back.command_id, None);
    }

    #[test]
    fn set_status_request_with_command_id_round_trip() {
        let r = SupervisorSetStatusRequest {
            pod_name: "weft-infra-supervisor-abc".into(),
            command_id: Some(42),
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "n".into(),
            member: None,
            unit: None,
            status: InfraNodeStatus::Stopping,
            failure_stage: None,
            failure_message: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: SupervisorSetStatusRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.command_id, Some(42));
        assert_eq!(back.status, InfraNodeStatus::Stopping);
    }

    #[test]
    fn event_record_request_round_trip() {
        let r = SupervisorEventRecordRequest {
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: Some("n".into()),
            member: None,
            kind: InfraEventKind::Flaky,
            payload: json!({ "desired": 3, "ready": 1 }),
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: SupervisorEventRecordRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.kind, InfraEventKind::Flaky);
        assert_eq!(back.payload["desired"], 3);
    }

    #[test]
    fn running_count_response_round_trip() {
        let v = json!({ "running_count": 3 });
        let r: SupervisorRunningCountResponse = serde_json::from_value(v).unwrap();
        assert_eq!(r.running_count, 3);
    }

    #[test]
    fn endpoint_url_response_handles_null() {
        let v = json!({ "address": null });
        let r: InfraEndpointUrlResponse = serde_json::from_value(v).unwrap();
        assert!(r.address.is_none());
    }

    #[test]
    fn endpoint_url_response_carries_the_public_url() {
        let v = json!({ "address": { "url": "http://a.ns.svc:80", "public_url": "http://door/infra/ns/a/hooks" } });
        let r: InfraEndpointUrlResponse = serde_json::from_value(v).unwrap();
        let address = r.address.unwrap();
        assert_eq!(address.url, "http://a.ns.svc:80");
        assert_eq!(address.public_url.as_deref(), Some("http://door/infra/ns/a/hooks"));
    }

    #[test]
    fn endpoint_url_request_carries_endpoint_name() {
        let v = json!({
            "color": "00000000-0000-0000-0000-0000000000a1",
            "node_id": "n",
            "per_member": false,
            "endpoint_name": "api"
        });
        let r: InfraEndpointUrlRequest = serde_json::from_value(v).unwrap();
        assert_eq!(r.endpoint_name, "api");
    }

    fn make_row(
        surface_kind: SignalSurfaceKind,
        mount_path: Option<&str>,
        auth_kind: SignalAuthKind,
        auth_config: Option<Value>,
    ) -> SignalRowWire {
        SignalRowWire {
            token: "t".into(),
            tenant_id: "tenant-a".into(),
            for_member: None,
            node_id: "n".into(),
            kind_state_seq: 0,
            spec_json: "{}".into(),
            is_resume: false,
            color: None,
            surface_kind,
            mount_path: mount_path.map(|s| s.to_string()),
            mount_methods: Vec::new(),
            auth_kind,
            auth_config,
            kind_state: Value::Null,
            placement_generation: 0,
        }
    }

    #[test]
    fn to_routing_public_entry_strips_leading_slash() {
        use weft_core::primitive::{SignalAuth, SignalSurface};
        let r = make_row(
            SignalSurfaceKind::PublicEntry,
            Some("/hooks/x"),
            SignalAuthKind::None,
            None,
        )
        .to_routing()
        .unwrap();
        match r.surface {
            SignalSurface::PublicEntry { path, methods } => {
                assert_eq!(path, "hooks/x");
                assert!(methods.is_empty());
            }
            _ => panic!("expected PublicEntry"),
        }
        assert!(matches!(r.auth, SignalAuth::None));
    }

    #[test]
    fn to_routing_public_entry_carries_its_methods() {
        use weft_core::primitive::SignalSurface;
        let mut row = make_row(SignalSurfaceKind::PublicEntry, Some("/t/chat/{room}"), SignalAuthKind::None, None);
        row.mount_methods = vec!["POST".into()];
        let r = row.to_routing().unwrap();
        match r.surface {
            SignalSurface::PublicEntry { path, methods } => {
                assert_eq!(path, "t/chat/{room}");
                assert_eq!(methods, vec!["POST".to_string()]);
            }
            _ => panic!("expected PublicEntry"),
        }
        // A row from before the column existed decodes with no methods.
        let v = serde_json::to_value(&row).unwrap();
        let mut bare = v.clone();
        bare.as_object_mut().unwrap().remove("mount_methods");
        let back: SignalRowWire = serde_json::from_value(bare).unwrap();
        assert!(back.mount_methods.is_empty());
    }

    #[test]
    fn to_routing_public_entry_root_path() {
        use weft_core::primitive::SignalSurface;
        let r = make_row(
            SignalSurfaceKind::PublicEntry,
            Some("/"),
            SignalAuthKind::None,
            None,
        )
        .to_routing()
        .unwrap();
        match r.surface {
            SignalSurface::PublicEntry { path, .. } => assert_eq!(path, ""),
            _ => panic!(),
        }
    }

    #[test]
    fn to_routing_public_entry_null_mount_path_errors() {
        // Schema invariant: PublicEntry rows have NOT NULL mount_path.
        // A NULL-mount_path PublicEntry would silently mount at "";
        // the projection refuses to do that and surfaces the drift.
        let err = make_row(
            SignalSurfaceKind::PublicEntry,
            None,
            SignalAuthKind::None,
            None,
        )
        .to_routing()
        .unwrap_err();
        assert!(err.contains("NULL mount_path"));
    }

    #[test]
    fn to_routing_task_callback() {
        use weft_core::primitive::SignalSurface;
        let r = make_row(
            SignalSurfaceKind::TaskCallback,
            None,
            SignalAuthKind::None,
            None,
        )
        .to_routing()
        .unwrap();
        assert!(matches!(r.surface, SignalSurface::TaskCallback));
    }

    #[test]
    fn to_routing_internal() {
        use weft_core::primitive::SignalSurface;
        let r = make_row(
            SignalSurfaceKind::Internal,
            None,
            SignalAuthKind::None,
            None,
        )
        .to_routing()
        .unwrap();
        assert!(matches!(r.surface, SignalSurface::Internal));
    }

    #[test]
    fn listener_resolve_request_round_trip() {
        let req = ListenerResolveRequest {
            tenant: "tenant-a".into(),
            for_member: Some(weft_core::member::MemberScope {
                project_id: uuid::Uuid::from_u128(0xa1),
                member: weft_core::member::MemberId::new("alice").unwrap(),
            }),
            access_id: "11111111-2222-3333-4444-555555555555".into(),
            service: "slack".into(),
            required_values: vec!["imap_host".into()],
        };
        // The full literal pins the FIELD NAMES on the wire: a
        // symmetric rename round-trips fine but breaks the peer.
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "tenant-a",
                "for_member": {
                    "project_id": "00000000-0000-0000-0000-0000000000a1",
                    "member": "alice"
                },
                "access_id": "11111111-2222-3333-4444-555555555555",
                "service": "slack",
                "required_values": ["imap_host"]
            })
        );
        let back: ListenerResolveRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.service, "slack");
        assert_eq!(back.for_member, req.for_member);
        // required_values defaults when absent; a shared signal's
        // member is null.
        let bare: ListenerResolveRequest = serde_json::from_value(json!({
            "tenant": "t", "for_member": null, "access_id": "a", "service": "s"
        }))
        .unwrap();
        assert!(bare.required_values.is_empty());
        assert!(bare.for_member.is_none());
    }

    #[test]
    fn listener_resolved_source_round_trip() {
        let src = ResolvedEventSource {
            values: [("token".to_string(), "xoxb-1".to_string())].into_iter().collect(),
            auth: vec![weft_core::access::spec::AuthStep::Header {
                name: "Authorization".into(),
                value: weft_core::access::spec::Template::new("Bearer {token}"),
            }],
            events: serde_json::from_value(json!({
                "messages": {
                    "fields": { "text": "text" },
                    "account": { "value": "team", "path": "team_id" },
                    "webhook": { "verify": { "kind": "token_echo" } }
                }
            }))
            .unwrap(),
            recipe_values: [("app_token".to_string(), "xapp-1".to_string())]
                .into_iter()
                .collect(),
            provider_account: Some("T1".into()),
        };
        let v = serde_json::to_value(&src).unwrap();
        assert_eq!(v["values"], json!({ "token": "xoxb-1" }));
        assert_eq!(
            v["auth"],
            json!([{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }])
        );
        assert_eq!(v["recipe_values"], json!({ "app_token": "xapp-1" }));
        assert_eq!(v["provider_account"], "T1");
        assert_eq!(v["events"]["messages"]["account"]["value"], "team");
        let back: ResolvedEventSource = serde_json::from_value(v).unwrap();
        assert_eq!(back.values["token"], "xoxb-1");
        assert!(back.events.contains_key("messages"));
        assert_eq!(back.provider_account.as_deref(), Some("T1"));
    }

    #[test]
    fn subscription_ensure_round_trip() {
        let req = SubscriptionEnsureRequest {
            tenant: "tenant-a".into(),
            service: "google".into(),
            topic: "drive".into(),
            access_id: "a-1".into(),
            signal_token: "sig-1".into(),
            for_member: None,
            params: [("target".to_string(), "file-9".to_string())].into_iter().collect(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "tenant-a", "service": "google", "topic": "drive",
                "access_id": "a-1", "signal_token": "sig-1", "for_member": null,
                "params": { "target": "file-9" }
            })
        );
        let back: SubscriptionEnsureRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.params["target"], "file-9");

        for expiry in [Some(1893553445i64), None] {
            let resp = SubscriptionEnsureResponse { expires_at_unix: expiry };
            let v = serde_json::to_value(&resp).unwrap();
            assert_eq!(v, json!({ "expires_at_unix": expiry }));
            let back: SubscriptionEnsureResponse = serde_json::from_value(v).unwrap();
            assert_eq!(back.expires_at_unix, expiry);
        }
    }

    #[test]
    fn subscription_drop_round_trip() {
        let req = SubscriptionDropRequest {
            tenant: "tenant-a".into(),
            signal_token: "sig-1".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "tenant": "tenant-a", "signal_token": "sig-1" }));
        let back: SubscriptionDropRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.signal_token, "sig-1");
    }

    #[test]
    fn event_verify_request_round_trip() {
        let req = EventVerifyRequest {
            service: "slack".into(),
            topic: "messages".into(),
            body_b64: "eyJ4IjoxfQ==".into(),
            headers: [("x-slack-signature".to_string(), "v0=abc".to_string())]
                .into_iter()
                .collect(),
            query: [("challenge".to_string(), "tok".to_string())].into_iter().collect(),
            method: "POST".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "service": "slack", "topic": "messages", "body_b64": "eyJ4IjoxfQ==",
                "headers": { "x-slack-signature": "v0=abc" },
                "query": { "challenge": "tok" },
                "method": "POST"
            })
        );
        let back: EventVerifyRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.body_b64, "eyJ4IjoxfQ==");
        // headers/query/method default when absent.
        let bare: EventVerifyRequest = serde_json::from_value(json!({
            "service": "s", "topic": "t", "body_b64": ""
        }))
        .unwrap();
        assert_eq!(bare.method, "POST");
        assert!(bare.headers.is_empty() && bare.query.is_empty());
    }

    #[test]
    fn event_target_wire_round_trip() {
        let t = EventTargetWire { access_id: "a-1".into(), tenant_id: "tenant-a".into() };
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v, json!({ "access_id": "a-1", "tenant_id": "tenant-a" }));
        let back: EventTargetWire = serde_json::from_value(v).unwrap();
        assert_eq!(back.access_id, "a-1");
    }

    #[test]
    fn event_verdict_round_trips_all_three_kinds() {
        let challenge = EventVerdict::Challenge {
            body: "tok".into(),
            content_type: "text/plain".into(),
        };
        let v = serde_json::to_value(&challenge).unwrap();
        assert_eq!(
            v,
            json!({ "kind": "challenge", "body": "tok", "content_type": "text/plain" })
        );
        assert!(matches!(
            serde_json::from_value::<EventVerdict>(v).unwrap(),
            EventVerdict::Challenge { body, .. } if body == "tok"
        ));

        let drop = EventVerdict::Drop { reason: "noise".into() };
        let v = serde_json::to_value(&drop).unwrap();
        assert_eq!(v, json!({ "kind": "drop", "reason": "noise" }));
        assert!(matches!(
            serde_json::from_value::<EventVerdict>(v).unwrap(),
            EventVerdict::Drop { reason } if reason == "noise"
        ));

        let deliver = EventVerdict::Deliver {
            named_event: json!({ "text": "hi" }),
            targets: vec![EventTargetWire { access_id: "a-1".into(), tenant_id: "t".into() }],
            signal_token: Some("sig-1".into()),
        };
        let v = serde_json::to_value(&deliver).unwrap();
        assert_eq!(
            v,
            json!({
                "kind": "deliver",
                "named_event": { "text": "hi" },
                "targets": [{ "access_id": "a-1", "tenant_id": "t" }],
                "signal_token": "sig-1"
            })
        );
        match serde_json::from_value::<EventVerdict>(v).unwrap() {
            EventVerdict::Deliver { named_event, targets, signal_token } => {
                assert_eq!(named_event["text"], "hi");
                assert_eq!(targets.len(), 1);
                assert_eq!(signal_token.as_deref(), Some("sig-1"));
            }
            other => panic!("expected Deliver, got {other:?}"),
        }
    }

    #[test]
    fn to_routing_connection_auth_passes_config_through() {
        use weft_core::primitive::SignalAuth;
        let cfg = json!({"access_id": "acc-1", "service": "api_key_auth"});
        let r = make_row(
            SignalSurfaceKind::PublicEntry,
            Some("/x"),
            SignalAuthKind::Connection,
            Some(cfg.clone()),
        )
        .to_routing()
        .unwrap();
        assert!(matches!(r.auth, SignalAuth::Connection));
        assert_eq!(r.auth_config, cfg);
    }

    #[test]
    fn caller_verify_wire_round_trips() {
        let req = CallerVerifyRequest {
            tenant: "alice".into(),
            for_member: None,
            access_id: "acc-1".into(),
            service: "hmac_auth".into(),
            method: "POST".into(),
            path: "chat/room7".into(),
            headers: [("x-signature".to_string(), "abc".to_string())].into_iter().collect(),
            query: [("v".to_string(), "1".to_string())].into_iter().collect(),
            body_b64: "e30=".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "alice", "access_id": "acc-1", "service": "hmac_auth",
                "method": "POST", "path": "chat/room7",
                "headers": { "x-signature": "abc" }, "query": { "v": "1" },
                "body_b64": "e30="
            })
        );
        let back: CallerVerifyRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.path, "chat/room7");
        let bare: CallerVerifyRequest = serde_json::from_value(json!({
            "tenant": "t", "access_id": "a", "service": "s", "method": "GET", "path": ""
        }))
        .unwrap();
        assert!(bare.headers.is_empty() && bare.query.is_empty() && bare.body_b64.is_empty());

        let ok = CallerVerified { identity: json!({ "key": 2 }) };
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v, json!({ "identity": { "key": 2 } }));
        let back: CallerVerified = serde_json::from_value(v).unwrap();
        assert_eq!(back.identity["key"], 2);
    }

}



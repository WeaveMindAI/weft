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
use weft_core::RunId;
use weft_task_store::tasks::{NewTask, TaskOutcome, TaskStatus};

// The wire-enum helper is weft-core's (`weft_core::wire_enum!`), so an
// enum can live beside the messages that carry it in any crate.
use weft_core::wire_enum;

use crate::line::{BrokerCall, Twice};

// =================================================================
// Typed wire enums
// =================================================================

wire_enum! {
    /// Lifecycle verb stored in `infra_lifecycle_command.verb`.
    /// `Apply` / `Stop` / `Terminate` are claimed by the per-tenant
    /// supervisor process; `Deactivate` / `Reactivate` / `Upgrade` are
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

/// The lifecycle state of an infra node copy lives in weft-core, where a
/// program's `ctx.infra(..).status()` decodes it too; re-exported here so
/// the `weft_broker_client::protocol::*` paths keep working.
pub use weft_core::infra::InfraNodeStatus;

/// Per-unit runtime state, one `infra_unit` row each (read as a map of
/// unit name to this, [`units_of_copy_sql`]). This is the per-unit truth:
/// the node-level `infra_node.status` is a `InfraNodeStatus::rollup`
/// over these. Also
/// the authoritative roster of a node's EXPECTED units (a unit at 0
/// replicas or a Service-only unit shows no workload, so the supervisor
/// can't learn the roster from observed labels alone). Stamped at apply
/// from the spec's units.
// SYNC: UnitRuntime <-> crates/weft-dispatcher/src/infra_node.rs (infra_unit, UNITS_MOVE), units_of_copy_sql, crates/weft-broker/src/lifecycle_writes.rs (write_units)
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
    /// Since when (unix seconds) a `running` unit has been seen not ready:
    /// it turns `flaky` once that is `flaky_after_seconds` old. `None`
    /// while it is ready, or once it is declared flaky.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_ready_since_unix: Option<i64>,
    /// Since when (unix seconds) a `flaky` unit has been seen ready again:
    /// it recovers once that is `recovery_after_seconds` old. `None`
    /// otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_since_unix: Option<i64>,
    /// The image refs this unit's containers actually run after the
    /// apply that stamped this entry (`Image::Upstream` literals and
    /// resolved `Image::Local` tags alike). A FROZEN unit (left up
    /// across a sync) keeps the refs its last apply recorded, which can
    /// be older than the project's current tag map: image reclamation
    /// (the dispatcher's keep-set, `build::prune::keep_set`) unions these so an up unit's old
    /// image is never reclaimed out from under it. Empty for entries
    /// stamped before the field existed. The broker's per-unit status
    /// writes leave this untouched.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub image_refs: std::collections::BTreeSet<String>,
    /// The latest time one of its containers stopped on its own and its
    /// host brought it back, since weft last started the unit: written
    /// once by the health check the first time it sees it
    /// ([`SupervisorSetStatusRequest::restart`]), kept for a unit an apply
    /// leaves up, and cleared for one it starts again. `None` when none
    /// was seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restart: Option<weft_core::infra::LastRestart>,
}

/// Lifecycle status of a project row: weft-core's, since the CLI reads it
/// in the project status too.
pub use weft_core::projects::ProjectStatus;

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

/// The body of a 503 the broker answers when a call did nothing at all: it
/// could not even get a connection to its database (every connection
/// busy, or the database refusing new ones), so no statement ran and
/// sending the call again cannot repeat anything. Any other 503 may have
/// half happened; a write is sent again only on this one.
// SYNC: NOT_DONE <-> crates/weft-broker/src/handlers.rs (unavailable_or_internal), crates/weft-broker-client/src/client.rs (turned_away)
pub const NOT_DONE: &str = "the broker could not get a connection to its database, so nothing was done; send it again";

/// The body of a 503 a stopping broker copy answers a call that arrived
/// on its line after the stop began (`crate::line::server::serve`):
/// nothing of it ran, and sent again it goes out on a line to a running
/// copy.
pub const STOPPING: &str = "this copy of the broker is stopping, so nothing was done; send it again";

/// The answer to a call in a shape the broker does not read, and its body
/// (`weft_core::wire_shape`), re-exported here beside the broker's other
/// answers.
pub use weft_core::wire_shape::{unreadable_shape, UNREADABLE_SHAPE};

// ---------- Records ----------

/// A batch of records ([`JOURNAL_RECORD_PATH`]). Harmless twice: the record
/// takes a batch sent again as one already there (`weft_journal::record::Fate::AlreadyApplied`),
/// and so do the cost rows it carries, which go in `ON CONFLICT (run_id, cost_id) DO NOTHING`
/// (`weft_record_costs`, `weft_dispatcher::store::postgres`).
pub const JOURNAL_RECORD: BrokerCall = BrokerCall { path: JOURNAL_RECORD_PATH, twice: Twice::Harmless };

/// `POST /v1/journal/record`: one batch of a worker's writer lane, its
/// body the bytes `weft_journal::frame` encodes (not JSON), answered with
/// `weft_journal::frame::BatchAnswer`. Worker-only: every run of the batch
/// is fenced on the calling replica.
// SYNC: JOURNAL_RECORD_PATH <-> crates/weft-broker/src/records.rs (routes)
pub const JOURNAL_RECORD_PATH: &str = "/v1/journal/record";

/// The header a worker sends its batches with to say it reads every fate
/// of this release (`weft_journal::record::Fate`), [`BATCH_FATES`]. A
/// worker of the previous release sends none, and is answered only with
/// the fates it reads (`weft_broker::records::previous_release_answer`);
/// a broker of the previous release ignores it, and answers with the
/// fates it has, which this release reads too. The release after this
/// one drops it.
pub const BATCH_FATES_HEADER: &str = "x-weft-batch-fates";
pub const BATCH_FATES: &str = "project_removing";

/// The most one batch body may weigh. A lane closes a batch at a mebibyte;
/// a run's next row alone may weigh more (one emission, one value per
/// output port, each under `MAX_WIRE_VALUE_BYTES`, which the engine refuses
/// past at the node), and goes alone, so this leaves room for sixty-four
/// such ports.
pub const JOURNAL_RECORD_BODY_LIMIT: usize = 64 * weft_core::storage::MAX_WIRE_VALUE_BYTES;

/// One page of a run's record. Harmless twice: it only reads.
pub const RECORD_OF: BrokerCall = BrokerCall { path: "/v1/journal/record_of", twice: Twice::Harmless };

/// `POST /v1/journal/record_of`: one page of the record of a run of the
/// calling worker's project, raw (`weft_journal::record::read_page`): its
/// rows after `after`, from its first when `null`, empty once none is
/// left. A worker reads a record a page at a time (a run it claimed, its
/// fold catching up after answers, a seed ancestor), so no answer carries
/// a long run whole. `after` is always sent, `null` included: a worker
/// built by an older weft, which asked for a whole record, is refused
/// naming `weft resync`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordOfRequest {
    pub run_id: RunId,
    #[serde(deserialize_with = "Option::deserialize")]
    pub after: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordOfResponse {
    pub record: weft_journal::record::RawRecord,
}

/// One secret's value. Harmless twice: it only reads.
pub const RECORD_SECRET: BrokerCall = BrokerCall { path: "/v1/journal/secret", twice: Twice::Harmless };

/// `POST /v1/journal/secret`: the value of a secret a run of the calling
/// worker's project made (`weft_core::secret`), opened from the sealed copy
/// kept with that run's record. 404 when no run of the project keeps it:
/// its run's record is gone, or that run never wrote it (a run that is not
/// recorded keeps its secrets in memory alone).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSecretRequest {
    pub secret_id: weft_core::secret::SecretId,
}

/// The value, over the worker's own line, as the run that made it held it.
/// Its `Debug` leaves the value out.
#[derive(Clone, Serialize, Deserialize)]
pub struct RecordSecretResponse {
    pub value: serde_json::Value,
}

impl std::fmt::Debug for RecordSecretResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordSecretResponse").field("value", &"<not shown>").finish()
    }
}

// ---------- Runs ----------

/// A worker claiming a run it was handed. Harmless twice: a claim the caller
/// holds already is given again under a new epoch, for this very case
/// (`weft_journal::record::claim`), and nothing was written under the old
/// one; the run's `attempts` count one more, which nothing acts on.
pub const RUN_CLAIM: BrokerCall = BrokerCall { path: "/v1/run/claim", twice: Twice::Harmless };

/// `POST /v1/run/claim`: the calling worker claims a queued run of its
/// project, the one it was handed (`weft_journal::record::claim`). `None`
/// when it is not queued. Its record the worker reads after, a page at a
/// time ([`RecordOfRequest`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunClaimRequest {
    pub run_id: RunId,
    /// How the worker reads the claimed run's record. Always sent: a
    /// worker built by an older weft, which read the record whole from
    /// the claim's answer, sends none and is refused naming `weft resync`.
    pub reads: RecordReads,
}

/// How a worker reads the record of a run it claimed ([`RunClaimRequest`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordReads {
    /// A page at a time, after the claim ([`RecordOfRequest`]).
    ByPage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunClaimResponse {
    pub claimed: Option<weft_journal::record::Claimed>,
}

/// The answers waiting for a run. Harmless twice: it only reads; answers
/// go once a batch of records resolves them.
pub const RUN_ANSWERS: BrokerCall = BrokerCall { path: "/v1/run/answers", twice: Twice::Harmless };

/// `POST /v1/run/answers`: the answers waiting for the waits of a run the
/// calling worker drives, read once (none when none waits). Only the worker
/// driving a run writes its record, so an answer to a run being driven
/// waits for its worker to take it
/// (`weft_task_store::parked_fires::answers_for`), and the worker is told
/// when one is handed to it (`crate::line::PARKED_FIRE_CHANNEL`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAnswersRequest {
    pub run_id: RunId,
    /// The waits whose answers the worker already took. An answer stays
    /// waiting until the run's record holds it, which an unrecorded run's
    /// never does, so these are left out rather than handed again.
    pub taken: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAnswersResponse {
    pub answers: Vec<RunAnswer>,
}

/// One answer waiting for a run: the wait it resolves, and the answer (a
/// value, or a person's skip).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunAnswer {
    pub token: String,
    pub answer: weft_core::primitive::WaitAnswer,
}

/// What a worker reads once its line may have missed some. Harmless twice: it only reads.
pub const RUN_RECHECKED: BrokerCall = BrokerCall { path: "/v1/run/rechecked", twice: Twice::Harmless };

/// `POST /v1/run/rechecked`: what a worker reads once its line may have
/// missed some (it opened again after a break, or fell behind): the
/// cancels waiting for every run it drives and the answers handed to them,
/// in one read for all of them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunRecheckedRequest {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunRecheckedResponse {
    pub cancels: Vec<RunCancel>,
    /// Every answer handed to one of its runs, oldest first.
    pub answers: Vec<RunAnswerOf>,
}

/// An answer handed to the run `run_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunAnswerOf {
    pub run_id: RunId,
    pub token: String,
    pub answer: weft_core::primitive::WaitAnswer,
}

/// A worker giving a run up. Not safe twice: the first lets go of the run,
/// so the second is refused as from a worker that no longer drives it.
pub const RUN_GIVE_UP: BrokerCall = BrokerCall { path: "/v1/run/give_up", twice: Twice::NotSafe };

/// `POST /v1/run/give_up`: end a run the calling worker drives but whose
/// record it can no longer write (`weft_journal::record::give_up_in`).
/// Answered 409 when the worker does not drive it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunGiveUpRequest {
    pub run_id: RunId,
    /// Why, as the run's failure says it.
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunGiveUpResponse {
    /// The `seq` its ending was written at.
    pub ended_at_seq: i32,
}

/// One cancel waiting for a run, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunCancel {
    pub run_id: RunId,
    pub cause: weft_core::run::CancelCause,
}

// ---------- Run steering (`ctx.tag_run` / `ctx.stop_tagged`) ----------

/// A worker tagging its run. Harmless twice: the tags go in
/// `ON CONFLICT (run_id, tag) DO NOTHING` (`weft_journal::tags`), keeping the first one's place.
pub const RUN_TAG: BrokerCall = BrokerCall { path: "/v1/run/tag", twice: Twice::Harmless };

/// `POST /v1/run/tag`: the worker tags the run it is
/// driving, its `RunTagged` already on record. Worker-only: the
/// broker writes the `run_tag` rows synchronously, so by the time
/// the node's call returns its tag row exists and a following
/// `stop_tagged` can anchor on it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTagRequest {
    pub run_id: RunId,
    /// Already validated by the ctx (`weft_core::tag`); the broker
    /// validates again, because it trusts no process.
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTagResponse {}

/// A worker stopping the runs carrying a tag. Not safe twice: each arrival
/// queues its own stop, and a second one may reach runs tagged in between.
pub const RUN_STOP_TAGGED: BrokerCall = BrokerCall { path: "/v1/run/stop_tagged", twice: Twice::NotSafe };

/// `POST /v1/run/stop_tagged`: the worker asks that every live
/// run of ITS project carrying `tag` be stopped. The broker
/// resolves the ordering anchor at this moment (the asker's own tag
/// seq, or one past the newest row) and enqueues the dispatcher's
/// `stop_tagged` task with it, so a stop that runs late can never reach
/// a sibling that tagged itself after the ask. The project is the
/// run's, read from its row; the request never names one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStopTaggedRequest {
    /// The asking run.
    pub run_id: RunId,
    pub tag: String,
    pub stop_self: weft_core::StopSelf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStopTaggedResponse {
    /// The stop reaches the asking run itself (`StopSelf::Include` and
    /// the run carries the tag). The asker then waits for its own
    /// cancel instead of going on: the dispatcher carries the stop out
    /// a moment later, and a node that returned first would let the run
    /// go past its own stop.
    pub stops_asker: bool,
}

// ---------- Tasks ----------

/// Queueing a task once per key. Not safe twice: the key only holds while
/// the first task waits or runs, so one sent again after it ended queues it again.
pub const TASK_ENQUEUE_DEDUP: BrokerCall = BrokerCall { path: "/v1/task/enqueue_dedup", twice: Twice::NotSafe };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnqueueDedupRequest {
    pub spec: NewTask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnqueueDedupResponse {
    pub id: Uuid,
    /// False when an identical live task already existed (a dedup hit).
    pub inserted: bool,
}

/// Waiting for a task to end. Harmless twice: it only reads.
pub const TASK_WAIT_TERMINAL: BrokerCall = BrokerCall { path: "/v1/task/wait_terminal", twice: Twice::Harmless };

/// A task's outcome, held open up to `wait_ms` until it is terminal; the
/// answer is its state either way.
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

// ---------- Infra ----------

/// Asking for a look at a project's infra. Harmless twice: it only rings
/// the supervisor (`pg_notify`), and a second look finds what the first did.
pub const INFRA_LOOK: BrokerCall = BrokerCall { path: "/v1/infra/look", twice: Twice::Harmless };

/// What changed of an infra copy's values (`weft_core::infra::bake::PushedValues`).
/// Harmless twice: a publish the same as the one there writes nothing, and the
/// baked values are set to the same keys again; its agent sends the next only
/// once this one is answered.
pub const INFRA_PUSHED: BrokerCall = BrokerCall { path: "/v1/infra/pushed", twice: Twice::Harmless };

/// A machine running `project_id`'s infra asking for its health to be
/// looked at now (`BrokerInfraClient::ask_for_a_look`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraLookRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraLookResponse {}

/// Where an infra node answers. Harmless twice: it only reads.
pub const INFRA_ENDPOINT_URL: BrokerCall = BrokerCall { path: "/v1/infra/endpoint_url", twice: Twice::Harmless };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEndpointUrlRequest {
    /// The asking run. The broker resolves its project from it, so a
    /// handle never reaches outside the run's project, and checks the
    /// handle's instance against the run's.
    pub run_id: weft_core::RunId,
    /// The endpoint asked for: an infra node's place (see
    /// `InfraEnqueueApplyRequest`), the endpoint's name, and the instance
    /// whose copy it is (absent for a shared copy).
    pub infra: weft_core::infra::InfraHandle,
}

/// What an infra node's copy saved. Harmless twice: it only reads.
pub const INFRA_BAKED: BrokerCall = BrokerCall { path: "/v1/infra/baked", twice: Twice::Harmless };

/// `POST /v1/infra/baked`: what an infra node's copy saved for its baked
/// outputs, for the run that runs the node (`InfraReader::baked_outputs`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraBakedRequest {
    pub run_id: weft_core::RunId,
    /// The node's place, spelled.
    pub place: String,
    /// Whose copy: absent for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraBakedResponse {
    pub saved: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Saving what an infra setup sent out. Harmless twice: the values are merged
/// onto the same keys again, and the run sends the next only once this one is answered.
pub const INFRA_BAKE: BrokerCall = BrokerCall { path: "/v1/infra/bake", twice: Twice::Harmless };

/// `POST /v1/infra/bake`: what an infra setup's run of an infra node sent
/// out on its baked outputs, which its copy holds from then on, merged
/// port by port over what it held (`weft_core::infra::bake`). Only a setup
/// run saves, and only for a copy it may read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraBakeRequest {
    pub run_id: weft_core::RunId,
    /// The node's place, spelled.
    pub place: String,
    /// Whose copy: absent for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
    pub values: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEndpointUrlResponse {
    /// `None` when the node is not running or declares no endpoint by
    /// that name.
    pub address: Option<weft_core::infra::EndpointAddress>,
}

// ---------- Connections + cost recording ----------

/// Resolving a connection for one firing. Harmless twice: it reads, a
/// token due for a refresh is refreshed by whichever call claims it while it
/// is still due (`weft_access_store::resolve`), so the second reads the fresh
/// one, and a runtime-owned key is leased once per firing
/// (`weft_broker::credential::CredentialSource`).
pub const ACCESS_RESOLVE: BrokerCall = BrokerCall { path: "/v1/access/resolve", twice: Twice::Harmless };

/// Worker resolves a connection reference for ONE firing: asks for the
/// short-lived pieces it needs to authenticate calls on the stored
/// connection `connection_id`. The store refreshes lazily
/// (single-flight) before answering; for a connection whose credential
/// the runtime supplies, the runtime's credential source answers
/// instead. The caller's run scope anchors the tenant, and the
/// row's tenant must match (the tenant wall at run time). The firing's
/// identity travels on EVERY resolve, because any connection can be
/// billable and every cost record must be attributable to a firing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveConnectionRequest {
    /// The requesting run. The broker resolves the owning tenant
    /// AND project from it and enforces both, so neither is taken from
    /// this request: a credential policy that decides per project must
    /// not be deciding on a name the worker chose.
    pub run_id: RunId,
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

/// A node handing out a connection to what it runs itself. Harmless twice: a
/// publish the same as the one there answers its id and writes nothing.
pub const ACCESS_PUBLISH: BrokerCall = BrokerCall { path: "/v1/access/publish", twice: Twice::Harmless };

/// A node handing out a connection to something it runs itself, and
/// the question that precedes it ("did I already publish one?").
/// `values` are the service's own declared fields, exactly what a
/// person would have pasted; the store refuses anything else.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishAccessRequest {
    /// The publishing run. The broker resolves the owning tenant
    /// AND project from it, so neither is taken from this request.
    pub run_id: RunId,
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
    /// Whether the publishing node exists once per instance: its
    /// connection is then the run instance's (whose, the broker reads off
    /// the run, never from here), else the shared one.
    #[serde(default)]
    pub per_instance: bool,
}

/// The reference the publisher gets back, to put on its output port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishAccessResponse {
    pub connection: PublishedConnection,
}

/// A connection a node published. Harmless twice: it only reads.
pub const ACCESS_PUBLISHED: BrokerCall = BrokerCall { path: "/v1/access/published", twice: Twice::Harmless };

/// "Which connection did this node publish for this service, if any?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedAccessRequest {
    pub run_id: RunId,
    pub node_id: String,
    pub service: String,
    /// As on [`PublishAccessRequest::per_instance`].
    #[serde(default)]
    pub per_instance: bool,
}

/// Minting an instance token. Harmless twice: the token row is written over by
/// its id (`weft_access_store::instance_tokens`), so the second mints the token
/// the caller gets, and only the one whose answer was lost stops working.
pub const MINT_INSTANCE_TOKEN: BrokerCall = BrokerCall { path: "/v1/program/mint_instance_token", twice: Twice::Harmless };

/// Worker: mint an instance token for one instance of a project of the
/// run's owner (`ctx.tokens().mint_for_instance`): the run's own project,
/// or the one `project` names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramMintInstanceTokenRequest {
    /// The token's id, chosen by the run and journaled before this
    /// request: a replayed run mints under the same id, which replaces
    /// that token (a new value, the old one dead) instead of leaving a
    /// second one behind.
    pub id: uuid::Uuid,
    pub run_id: RunId,
    /// The project the token acts in, one of the run's owner's; the run's
    /// own when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<uuid::Uuid>,
    pub instance: weft_core::instance::InstanceId,
    /// How long it works, in seconds; required, a year at most.
    pub expires_in_secs: u64,
    /// Whether it reads its instance's copies' displays (a bridge's pairing
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
    /// Until when (unix seconds) a worker may keep this answer and hand
    /// it out again without asking, as long as nothing says the
    /// connection changed (`line::ACCESS_CHANNEL`): `None` for a credential
    /// the runtime supplies per firing, which is never kept, and
    /// `i64::MAX` for stored values that never expire.
    #[serde(default)]
    pub keep_until_unix: Option<i64>,
    /// The infra node (spelled) whose infra setup made this connection,
    /// when one did (`weft_access_store::ResolvedAccess::published_by`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_by: Option<String>,
}

/// Releasing a connection after a firing. Harmless twice: releasing one released already does nothing.
pub const ACCESS_CLOSE: BrokerCall = BrokerCall { path: "/v1/access/close", twice: Twice::Harmless };

/// The firing that resolved a connection finished: release the lease
/// NOW rather than leaving a runtime-supplied credential usable to its
/// window (which is only the crash backstop). Sent by the engine ONLY
/// for an `Ours`-owned connection (a runtime-supplied credential is
/// the only thing there is to retire); a user's own stored values are
/// theirs and never travel back. Nothing node-facing makes this call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseConnectionRequest {
    pub run_id: RunId,
    /// The resolved values being given back; the runtime's credential
    /// source retires the ones it recognizes.
    pub values: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseConnectionResponse {}

// ---------- Project (worker fetches its own definition) ----------

/// A project's definition by hash. Harmless twice: it only reads.
pub const PROJECT_FETCH_DEFINITION: BrokerCall = BrokerCall { path: "/v1/project/fetch_definition", twice: Twice::Harmless };

/// Worker call to fetch the project's runtime `ProjectDefinition`
/// at run claim time. Keyed by `(project_id, expected_hash)`:
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
    /// Serialized `weft_core::ProjectDefinition`, as the row keeps it:
    /// the client reads it once, through `ProjectDefinition::STORED`,
    /// at `project_json_version`.
    pub project_json: String,
    /// The version `project_json` was written at.
    pub project_json_version: weft_core::stored::Version,
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

/// A supervisor renewing and claiming project leases. Not safe twice: the
/// second answers none of the projects the first took on, and counts a
/// trouble it reports twice.
pub const SUPERVISOR_SYNC_OWNERSHIP: BrokerCall = BrokerCall { path: "/v1/supervisor/sync_ownership", twice: Twice::NotSafe };

/// Empty: a pooled supervisor lists projects across all tenants. The
/// request body carries nothing; the trusted caller identity is the
/// authority.
/// Claim + renew + report ownership in one atomic broker round-trip.
/// The pooled supervisor calls this on its ownership tick: the broker
/// renews this replica's existing project leases and claims a batch
/// more unowned-or-expired projects' infra for it (the EXCLUSIVE lease
/// that keeps two supervisors off the same project), then returns the
/// full set this replica now owns. Both work loops
/// (lifecycle, health) then act ONLY on the returned set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSyncOwnershipRequest {
    /// The supervisor replica syncing its ownership.
    pub replica: String,
    /// Every project the caller's host holds a copy of. Each is claimed
    /// and renewed like a project with infra to manage, even when it has
    /// none any more, because the gone-copy sweep judges and deletes a
    /// project's copies only under the project's lease
    /// ([`SupervisorGoneCopiesRequest`]). A lease on a project with no
    /// infra that the host no longer holds is not renewed and lapses.
    /// Every sync of a replica carries its host's latest listing that went
    /// through, less the projects whose every copy this replica deleted
    /// since (a pass's sweep), so whether a project is ownable never
    /// depends on which sync this is. A sync that carries one clears the
    /// failed passes' retries and failures of every project no lease
    /// covers and nothing makes ownable any more. `None` when none did (a
    /// pass of a supervisor that scales to zero whose listing failed): a
    /// project only the listing could make ownable is not claimed then,
    /// and its failed pass's retry stays booked ([`Self::all_failed`]
    /// counts it).
    pub held_projects: Option<Vec<Uuid>>,
    /// Projects this replica's health look went through since its last
    /// sync, whatever the look came to. A failed pass's retry over one of
    /// them, while this replica still holds its lease, is taken: the look
    /// was the retry (a look that failed again sets the next one through
    /// `failed`). One that is not also `failed` (and with no `all_failed`)
    /// went through clean, which ends its failures in a row.
    #[serde(default)]
    pub looked: Vec<Uuid>,
    /// Projects this replica looked at, and ran the commands of, with
    /// nothing given up: what was written down as given up on them is
    /// cleared.
    #[serde(default)]
    pub settled: Vec<Uuid>,
    /// Projects a pass of this replica could not get through (a health
    /// look, a sweep of gone copies, a command's ending that could not be
    /// written), each with why. Each is written down for `weft status`
    /// and counted (`infra_health_look.failures`): a later pass tries it
    /// again after [`supervisor_retry_after`], until one goes through or
    /// it gives up. Only a project whose lease this replica holds, or a
    /// removed one (no row): what the host still holds of those is counted
    /// the same way and written down as one install-wide failure, cleared
    /// once a sync's listing (`held_projects`) holds none of them.
    #[serde(default)]
    pub failed: Vec<ProjectTrouble>,
    /// Projects a health rule of this replica gave up on: written down
    /// for `weft status` until a pass reports them settled, and tried again
    /// only when something real happens (a machine's word, a person's
    /// command or look), never on a clock. Only a project whose lease this
    /// replica holds.
    #[serde(default)]
    pub gave_up: Vec<ProjectTrouble>,
    /// A step of this replica's pass that covers every project failed
    /// (listing what its host holds, reading what it owns), with why: a
    /// failure of the pass over each project whose lease this replica
    /// holds, counted and written down like one in `failed` (once per
    /// project, with its own error when it has one too). With no listing
    /// (`held_projects` is `None`), also over each project whose failed
    /// pass's retry is due, that no lease covers, and that only the
    /// listing could make ownable: that pass was its retry.
    #[serde(default)]
    pub all_failed: Option<String>,
    /// A pass of a supervisor that scales to zero ends: give every lease
    /// back instead of renewing and claiming.
    #[serde(default)]
    pub ending: bool,
}

/// A project a supervisor's pass could not get through, and why
/// ([`SupervisorSyncOwnershipRequest::failed`] and `gave_up`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTrouble {
    pub project_id: Uuid,
    pub error: String,
}

/// How many tries in a row the supervisor makes at one thing before it
/// gives up until something real happens: a health rule's action, a
/// pass over a project ([`SupervisorSyncOwnershipRequest::failed`]), and
/// the runs of one infra command that never ended (its `runs`, counted by
/// the broker's claim).
pub const SUPERVISOR_GIVE_UP_AFTER: u32 = 3;

/// How long after its `failures`th failure in a row the supervisor tries
/// again (5 s, then 10 s), or `None` once it gives up
/// ([`SUPERVISOR_GIVE_UP_AFTER`]).
pub fn supervisor_retry_after(failures: u32) -> Option<std::time::Duration> {
    const BASE_SECS: u64 = 5;
    (failures < SUPERVISOR_GIVE_UP_AFTER)
        .then(|| std::time::Duration::from_secs(BASE_SECS << failures.saturating_sub(1)))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorProject {
    pub project_id: Uuid,
    /// The project's tenant. A pooled supervisor serves many tenants,
    /// so the compile context's tenant comes from the project, not from
    /// a per-supervisor identity (it has none).
    pub tenant_id: String,
    /// The project's listening as one status: the aggregate over all
    /// its trigger activations, every owner's
    /// (`weft_broker_client::activation::aggregate`). The supervisor's
    /// health rules read it (`HealthCondition::Listening` of
    /// `weft_core::infra::health` holds while it is `active`). No
    /// `serde(default)`: broker and supervisor deploy together, a missing
    /// field is schema drift and should fail loud on deserialize.
    pub status: ProjectStatus,
    /// True iff some activation the health loop took down (it parks
    /// only what reads a broken infra copy) is still down. A person's
    /// deactivate clears the mark on what it takes down, so the built-in
    /// bring-back rule, gated on this
    /// (`HealthCondition::TakenDownByHealth`), reactivates only what the
    /// health loop took and never overrides a person's stop.
    pub health_down: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSyncOwnershipResponse {
    /// Every project this replica now owns (after renew + claim). The work
    /// loops act only on these.
    pub owned: Vec<SupervisorProject>,
    /// The projects among `owned` this tick took on: freshly claimed, or
    /// this replica's own lease revived after it lapsed. A command issued
    /// on one of them while nobody held it woke none of this replica's
    /// claims, so it asks again at once when this is not empty.
    pub claimed: Vec<Uuid>,
    /// When a supervisor next has something to look at that nothing
    /// announces (unix seconds, database clock): a unit's health window
    /// ending, a health rule's retry, a command waiting on a project no
    /// lease covers, a lease another replica holds lapsing. The broker
    /// books the supervisor's next pass then; `None` when there is none.
    pub next_due_unix: Option<i64>,
}

/// The projects a supervisor owns. Harmless twice: it only reads.
pub const SUPERVISOR_OWNED_PROJECTS: BrokerCall = BrokerCall { path: "/v1/supervisor/owned_projects", twice: Twice::Harmless };

/// Pure read of the projects a supervisor process currently owns (no claim,
/// no renew). The work loops (health, lifecycle) and per-command
/// tenant lookups use this; ownership breadth is changed ONLY by
/// `sync_ownership` (the ownership tick), never as a side effect of
/// doing work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorOwnedProjectsRequest {
    pub replica: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorOwnedProjectsResponse {
    pub owned: Vec<SupervisorProject>,
}

/// Which copies are gone for good. Harmless twice: it only reads.
pub const SUPERVISOR_GONE_COPIES: BrokerCall = BrokerCall { path: "/v1/supervisor/gone_copies", twice: Twice::Harmless };

/// Which of `copies` (every copy the host holds anything for of
/// `project`, as it names them) are gone for good: what the supervisor
/// sweeps the host against, deleting all a gone copy holds, the disks it
/// kept on terminate included. A copy is gone when its project is, or
/// when it has no `infra_node` row and the program no longer declares it
/// (the node was removed or changed sides).
///
/// Fenced like a lifecycle write: an existing project is judged only for
/// the supervisor holding its `infra_owner` lease, anyone else gets HTTP
/// 410 (displaced), so a copy is only ever deleted by the one supervisor
/// that could also be applying it. A removed project (no row) is judged
/// for anyone: nothing can apply it until it is registered again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorGoneCopiesRequest {
    pub replica: String,
    pub project: Uuid,
    /// Copies of `project` only; any other is refused (HTTP 400).
    pub copies: Vec<weft_core::infra::NodeRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorGoneCopiesResponse {
    /// The gone copies' ids.
    pub gone: Vec<String>,
}

/// A project's infra nodes. Harmless twice: it only reads.
pub const SUPERVISOR_INFRA_NODES: BrokerCall = BrokerCall { path: "/v1/supervisor/infra_nodes", twice: Twice::Harmless };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraNodesRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraNode {
    /// The node's placement spelling (see `InfraEnqueueApplyRequest`).
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub instance: Option<weft_core::instance::InstanceId>,
    pub copy_id: String,
    pub status: InfraNodeStatus,
    pub applied_spec_hash: Option<String>,
    /// When the last apply of this copy finished (unix seconds), `None`
    /// before any. The health loop's durable "this copy's workloads
    /// exist by now": a copy running past its apply by its flaky window
    /// whose workload is missing is broken, whatever the loop's memory
    /// lost in a restart or a move to another process.
    pub applied_at_unix: Option<i64>,
    /// Where the last apply stamped the endpoints. The apply's full
    /// skip compares it with what the spec gives now, so a row stamped
    /// before a field existed (or by an older rule) is applied again
    /// rather than skipped with stale addresses.
    #[serde(flatten)]
    pub addresses: AppliedEndpoints,
    /// Disks to keep on terminate (see
    /// `SupervisorSetAppliedRequest::keep_disks`). No `serde(default)`:
    /// supervisor and broker deploy together; a missing field is schema
    /// drift, not version skew.
    pub keep_disks: Vec<String>,
    /// Per-unit runtime: status + resolved health windows +
    /// stop_behavior, keyed by unit name. The authoritative unit
    /// roster + per-unit truth the health/stop loops operate on.
    pub units: std::collections::BTreeMap<String, UnitRuntime>,
    /// The health rules its node wrote (`InfraSpec::health_rules`), as
    /// its last apply stamped them: what the health loop runs on this
    /// copy. `None`: the node wrote none (or no apply stamped them yet),
    /// so it runs `weft_core::infra::health::built_in_rules`.
    pub health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInfraNodesResponse {
    pub nodes: Vec<SupervisorInfraNode>,
}

/// Where a project's health rules stand. Harmless twice: it only reads.
pub const SUPERVISOR_HEALTH_STATE: BrokerCall = BrokerCall { path: "/v1/supervisor/health_state", twice: Twice::Harmless };

/// Read where each health rule of a project's copies stands, kept across
/// the supervisor's processes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorHealthStateRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorHealthStateResponse {
    pub rules: Vec<HealthRuleState>,
    /// How each command a rule queued ([`HealthRuleState::queued`]) ended,
    /// for the ones that did: read with the state, never written back. A
    /// command not listed is still running (or gone, which its deadline
    /// settles).
    pub ended: Vec<HealthCommandEnding>,
}

/// How a command a health rule queued ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCommandEnding {
    pub command: i64,
    pub outcome: LifecycleOutcome,
    /// The error of a failed one, the reason of a cancelled one.
    pub message: Option<String>,
}

/// The take-down or bring-back command a health rule's action queued for
/// the dispatcher, until a look sees it end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedHealthCommand {
    /// Its `infra_lifecycle_command` id.
    pub command: i64,
    /// Not ended by then (unix seconds: when it was queued plus the
    /// rule's `timeoutSeconds`), it counts as failed. The broker books a
    /// look at it.
    pub deadline_unix: i64,
}

/// Where one health rule stands on one copy, kept across the supervisor's
/// processes (an `infra_health_rule` row). Keyed by the copy and the
/// rule's name, so two nodes' rules of the same name, or one rule on two
/// copies, never share a state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthRuleState {
    /// The copy the rule reads.
    pub copy: InfraCopy,
    /// The rule's name, unique among its node's rules.
    pub rule: String,
    /// Its action was queued for the copy and has not failed, and the
    /// rule has held for it ever since: it runs again only once the rule
    /// stopped holding.
    pub fired: bool,
    /// The command its action queued that no look has seen end yet. A
    /// look that finds it failed, or past its deadline, counts a failure
    /// of the rule and tries again like any failed action; while it runs,
    /// the rule fires nothing else on the copy.
    pub queued: Option<QueuedHealthCommand>,
    /// A take-down rule's: its take-down was queued for the copy and did
    /// not fail, and the copy has not come back since (no bring-back rule
    /// of its node holds for it, and something the health check took down
    /// is still down). What lets a copy whose node runs no bring-back rule
    /// keep down what it took down, and only that.
    pub took_down: bool,
    /// Failed attempts in a row since its last success.
    pub consecutive_failures: u32,
    /// Not before (unix seconds) does it try again after a failure.
    pub next_retry_at_unix: Option<i64>,
    /// When it gave up (unix seconds): it tries again once the copy's
    /// units it saw then (`gave_up_seen`) change, a person's infra command
    /// touches the project, or the rule stops holding for the copy.
    pub gave_up_at_unix: Option<i64>,
    pub gave_up_seen: Option<serde_json::Value>,
    pub last_error: Option<String>,
}

/// Replacing where a project's health rules stand. Harmless twice: the whole
/// set is deleted and written again, the same both times.
pub const SUPERVISOR_SET_HEALTH_STATE: BrokerCall = BrokerCall { path: "/v1/supervisor/set_health_state", twice: Twice::Harmless };

/// Replace a project's [`HealthRuleState`]s (a rule on a copy not listed
/// has none left), for the supervisor holding its lease (410 otherwise).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetHealthStateRequest {
    pub replica: String,
    pub project_id: Uuid,
    pub rules: Vec<HealthRuleState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetHealthStateResponse {}

/// A supervisor claiming a command. Not safe twice: each claim counts one of
/// the command's runs, and it is given up after a few.
pub const SUPERVISOR_CLAIM_COMMAND: BrokerCall = BrokerCall { path: "/v1/supervisor/claim_command", twice: Twice::NotSafe };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorClaimCommandRequest {
    /// The pooled supervisor process claiming work. It claims a lifecycle
    /// command ONLY for a project whose infra it currently owns (the
    /// `infra_owner` exclusive lease), so two supervisors never change
    /// the same project's infrastructure.
    pub claimer_replica: String,
    /// The commands this process runs, or finished and still has to write
    /// the end of: never handed to it again. A command it claimed before
    /// and does not list is handed to it again (its run stopped when
    /// ownership moved and came back).
    pub busy_commands: Vec<i64>,
    /// Busy commands whose cancel this process already heard.
    #[serde(default)]
    pub cancelling: Vec<i64>,
    /// How long to hold for a command to be issued when none is
    /// waiting.
    pub wait_ms: u64,
}

/// What a supervisor's claim comes back with.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SupervisorClaim {
    /// The command this process runs next.
    Command(SupervisorCommandRow),
    /// A command was issued for a project no supervisor owns yet. The
    /// process takes ownership now rather than at its next ownership tick,
    /// and claims again.
    UnownedWork,
    /// Nothing was issued for this process for the whole hold.
    Nothing,
    /// A person asked to cancel these busy commands (none of `cancelling`).
    Cancel { commands: Vec<i64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandRow {
    pub id: i64,
    pub project_id: Uuid,
    pub node_id: Option<String>,
    /// Which copies the command acts on (an apply names exactly one:
    /// shared, or one instance's). Absent reads as the shared copies.
    #[serde(default)]
    pub copies: weft_core::instance::Copies,
    pub verb: InfraLifecycleVerb,
    /// Set for `verb = apply`: `InfraSpec` serialized as JSON. The
    /// supervisor reads the prior `infra_node` row itself to decide
    /// skip / fresh / replace; the worker doesn't pass a mode.
    /// Set for `verb = terminate`: its [`TerminateWork`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_json: Option<serde_json::Value>,
    /// Stop only: force scale-to-zero EVERY unit, ignoring each unit's
    /// `on_stop` (so NoOp units come down too). The user's explicit
    /// "take it all down so I can update it" override.
    #[serde(default)]
    pub force: bool,
    /// A person asked to cancel it before it was handed out.
    #[serde(default)]
    pub cancel_requested: bool,
}

impl SupervisorCommandRow {
    /// Whether this command and `other` reach a copy in common, so the
    /// younger waits for the older.
    // SYNC: SupervisorCommandRow::overlaps <-> crates/weft-broker-client/src/lifecycle_command.rs (commands_overlap), crates/weft-core/src/instance.rs (Copies::overlaps)
    pub fn overlaps(&self, other: &SupervisorCommandRow) -> bool {
        let nodes_meet = self.node_id.is_none() || other.node_id.is_none() || self.node_id == other.node_id;
        nodes_meet && self.copies.overlaps(&other.copies)
    }
}

/// A terminate command's work, carried in its `spec_json`: what the
/// terminate does with the disks the node lists in `keepOnTerminate`.
/// On the command because it is the asker's answer, never the copy's: a
/// person's terminate keeps them, an instance's wipe deletes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminateWork {
    pub disks: weft_core::infra::TerminateDisks,
}

impl SupervisorCommandRow {
    /// This terminate's [`TerminateWork`]. Every terminate is issued with
    /// one, so a row without it is a writer bug and fails loudly.
    pub fn terminate_work(&self) -> Result<TerminateWork, String> {
        let spec = self
            .spec_json
            .as_ref()
            .ok_or_else(|| format!("terminate command {} carries no terminate work in spec_json", self.id))?;
        serde_json::from_value(spec.clone())
            .map_err(|e| format!("terminate command {}: spec_json is not terminate work: {e}", self.id))
    }
}

/// Recording an infra event. Not safe twice: each arrival inserts its own event.
pub const SUPERVISOR_EVENT_RECORD: BrokerCall = BrokerCall { path: "/v1/supervisor/event_record", twice: Twice::NotSafe };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorEventRecordRequest {
    pub project_id: Uuid,
    pub node_id: Option<String>,
    /// Whose copy the event is about: `None` for the shared one (or a
    /// project-level event).
    #[serde(default)]
    pub instance: Option<weft_core::instance::InstanceId>,
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
// The wire format for `infra_event` is `(kind: TEXT, payload: TEXT)`,
// the payload its JSON text.
// The supervisor writes rows; the dispatcher's `infra_event_bridge`
// reads them and converts each row into a typed `DispatcherEvent`
// for SSE consumers. The bridge needs a stable contract on:
//   1. which kind strings exist;
//   2. what JSON shape each kind's payload carries.
//
// `InfraEvent` collapses (kind, payload) into one tagged enum.
// Writers construct `InfraEvent::Flaky { reason }` and
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
    }
}

/// Typed per-kind payload. Construction-site is the supervisor;
/// destruction-site is the dispatcher's bridge. The `kind` string
/// stored in the DB row drives which arm is selected.
///
/// **No `Serialize` / `Deserialize` on the outer enum**: the wire
/// format is `(kind: TEXT, payload: TEXT)` in two columns, NOT a
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedPayload {
    pub copy_id: String,
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
    /// What the host saw of the unit (which unit, and why it is not
    /// ready); the bridge surfaces it on the action-bar banner.
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedPayload {
    pub stage: FailureStage,
    pub message: String,
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
        };
        let payload = match self {
            Self::Flaky(p) => serde_json::to_value(p).expect("FlakyPayload serializes"),
            Self::Failed(p) => serde_json::to_value(p).expect("FailedPayload serializes"),
            Self::Started(p) => serde_json::to_value(p).expect("StartedPayload serializes"),
            Self::Recovered | Self::Stopped | Self::Terminated => Value::Null,
        };
        (kind, payload)
    }

    /// Reader: validate that `payload` matches `kind`'s shape.
    /// Returns `Err` if the payload doesn't deserialize as expected
    /// (e.g. a Flaky event missing `reason`). Callers
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
        })
    }
}

/// Setting an infra copy's status. Harmless twice: it sets the same values
/// again, and what the copy waits on stays, since its status did not change.
pub const SUPERVISOR_SET_STATUS: BrokerCall = BrokerCall { path: "/v1/supervisor/set_status", twice: Twice::Harmless };

/// The supervisor's claim identity on a lifecycle write: its replica
/// id, the SAME string it claims commands with and that keys its
/// `infra_owner` lease. The broker's ownership gate compares the lease
/// against THIS, so the supervisor sends it explicitly, exactly as it
/// does on `claim_command` / `sync_ownership`.
// SYNC: supervisor replica (its replica id, the infra_owner lease key) <-> crates/weft-infra-supervisor/src/lib.rs (SupervisorState.replica, sent by lifecycle.rs/health.rs/ownership.rs), crates/weft-broker-client/src/lifecycle_command.rs (owns_project_predicate, gating infra_owner.supervisor_replica = req.replica in weft-broker/src/handlers.rs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetStatusRequest {
    /// The supervisor's claim identity (its replica id); the broker
    /// checks it holds the project's live `infra_owner` lease.
    pub replica: String,
    /// `infra_lifecycle_command.id` the supervisor is executing,
    /// if this write is part of a lifecycle command (Stop /
    /// Terminate / Apply transitions). The broker checks the caller
    /// still OWNS the project (live `infra_owner` lease for `replica`)
    /// before applying the write; a supervisor that lost ownership
    /// (drain / lease takeover) can't stamp statuses for a project
    /// another process now owns.
    ///
    /// `None` for autonomous writes from the health loop
    /// (`Flaky` / `Running` reconciliation) where there is no
    /// command in flight. The ownership check applies all the same, so
    /// a process that lost the project cannot stamp it either.
    pub command_id: Option<i64>,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// The unit whose status this write sets. `Some(unit)` updates that
    /// unit's `infra_unit` row and recomputes the node-level
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
    /// With `unit`: the unit's window, written with its status (the
    /// health loop's look sets them; a command's per-unit stamp clears
    /// them). Ignored without `unit`.
    #[serde(default)]
    pub not_ready_since_unix: Option<i64>,
    #[serde(default)]
    pub ready_since_unix: Option<i64>,
    /// With `unit`: a restart of the unit the health check saw that the
    /// unit's row does not hold yet ([`UnitRuntime::last_restart`]),
    /// written with its status. `None` leaves the row's as it is, which
    /// is every write but the one that first sees a restart. Ignored
    /// without `unit`.
    #[serde(default)]
    pub restart: Option<weft_core::infra::LastRestart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetStatusResponse {}

/// Recording what a command waits on. Harmless twice: it sets the same value again.
pub const SUPERVISOR_SET_WAITING: BrokerCall = BrokerCall { path: "/v1/supervisor/set_waiting", twice: Twice::Harmless };

/// What a change of one copy (an apply, a stop, a terminate) waits on,
/// so `weft status` can say it. Written under that change's command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetWaitingRequest {
    pub replica: String,
    pub command_id: i64,
    pub project_id: Uuid,
    pub node_id: String,
    pub instance: Option<weft_core::instance::InstanceId>,
    pub waiting: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetWaitingResponse {}

/// Recording an applied infra copy. Harmless twice: the copy's row and its
/// units are written with the same values again (only when it was applied is
/// stamped a moment later).
pub const SUPERVISOR_SET_APPLIED: BrokerCall = BrokerCall { path: "/v1/supervisor/set_applied", twice: Twice::Harmless };

/// Atomic post-apply state write: status, copy_id, applied spec
/// hash, endpoints map. Supervisor calls this on successful apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetAppliedRequest {
    /// The supervisor's claim identity (its replica id); the broker
    /// gates the UPSERT on it holding the project's live `infra_owner`
    /// lease. See [`SupervisorSetStatusRequest::replica`].
    pub replica: String,
    /// `infra_lifecycle_command.id` the supervisor is executing.
    /// The broker rejects the UPSERT if the caller no longer OWNS the
    /// project; prevents a displaced supervisor from resurrecting a row
    /// that `remove_node` deleted, or stamping over the new owner's
    /// still-running apply.
    pub command_id: i64,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub instance: Option<weft_core::instance::InstanceId>,
    pub copy_id: String,
    pub applied_spec_hash: String,
    #[serde(flatten)]
    pub addresses: AppliedEndpoints,
    /// Disks to keep on a future terminate, from
    /// `InfraSpec.keep_on_terminate`. Persisted on the `infra_node` row
    /// so the supervisor can honor it on terminate (terminate has no
    /// access to the spec). No `serde(default)`: broker and supervisor
    /// deploy together; a missing field is schema drift.
    pub keep_disks: Vec<String>,
    /// What the host gives this copy other than what its spec asks, one
    /// plain sentence each (`InfraHost::notes`); empty when it runs the
    /// copy as asked. Stamped on the row, so every status read tells the
    /// person starting it.
    pub notes: Vec<String>,
    /// Per-unit runtime, resolved from the spec's units at apply.
    /// Status is set to `Running` for every unit here (apply success
    /// = all units up). The health loop then maintains per-unit
    /// status from this baseline.
    pub units: std::collections::BTreeMap<String, UnitRuntime>,
    /// `InfraSpec::health_rules`, stamped on the row for the health loop
    /// (see [`SupervisorInfraNode::health_rules`]).
    pub health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetAppliedResponse {}

/// Where an applied node's endpoints answer, as the apply stamps them
/// on its `infra_node` row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedEndpoints {
    /// Endpoint name to where the project's workers and weft's own roles
    /// (the front door passing a `Public` endpoint on, the dispatcher
    /// reading a unit's `/live`) reach it.
    #[serde(rename = "endpoints")]
    pub urls: std::collections::BTreeMap<String, String>,
    /// Endpoint name to the path it answers at on the front door, for
    /// `Public` endpoints only (`weft_core::infra::public_path`).
    pub public_paths: std::collections::BTreeMap<String, String>,
    /// Endpoint name to the `host:port` it answers at on the install's
    /// network, for `SameNetwork` endpoints only.
    pub doors: std::collections::BTreeMap<String, String>,
}

/// Recording an infra copy being provisioned. Harmless twice: the copy's row
/// is written with the same values again (only when provisioning started is
/// stamped a moment later).
pub const SUPERVISOR_SET_PROVISIONING: BrokerCall = BrokerCall { path: "/v1/supervisor/set_provisioning", twice: Twice::Harmless };

/// Supervisor-callable: write or update the `infra_node` row at
/// `Provisioning` status BEFORE the apply begins. Locks in the
/// (copy_id, keep_disks) pair so that a partial-apply failure leaves
/// a visible row naming the half-started copy. The user's Terminate then
/// works (the host removes the copy by its copy id, keeping
/// `keep_disks`). On apply success, `set_applied` flips Provisioning ->
/// Running and fills endpoints + applied_spec_hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetProvisioningRequest {
    /// The supervisor's claim identity (its replica id); the broker
    /// gates the write on it holding the project's live `infra_owner`
    /// lease. See [`SupervisorSetStatusRequest::replica`].
    pub replica: String,
    pub command_id: i64,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub instance: Option<weft_core::instance::InstanceId>,
    pub copy_id: String,
    /// Carried forward to `set_applied`; needed at Terminate time
    /// even when the apply never reaches success.
    pub keep_disks: Vec<String>,
    /// Per-unit runtime resolved from the spec's units. Status is
    /// `Provisioning` for every unit at this point. Locked in before
    /// any platform call so a partial-apply failure leaves the roster visible.
    pub units: std::collections::BTreeMap<String, UnitRuntime>,
    /// `InfraSpec::health_rules`, stamped with the roster (see
    /// [`SupervisorInfraNode::health_rules`]).
    pub health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSetProvisioningResponse {}

/// Queueing a lifecycle command. Not safe twice: each arrival queues its own command.
pub const SUPERVISOR_ENQUEUE_LIFECYCLE: BrokerCall = BrokerCall { path: "/v1/supervisor/enqueue_lifecycle", twice: Twice::NotSafe };

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
    /// Take down the triggers that read the infra copies a health rule
    /// found broken (`weft_core::take_down::TakeDown::HEALTH`).
    Deactivate(TakeDownReaders),
    /// Bring back what the health loop took down: each activation it
    /// took carries the mark (`went_down_with`: health) the take-down
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

/// The health loop's take-down: the live activations whose trigger's cut
/// contains one of the `broken` copies, and nothing else. A shared copy
/// is read by every owner's triggers that read the node; an instance's
/// copy only by that instance's. Its answers are fixed
/// (`weft_core::take_down::TakeDown::HEALTH`): the door holds what
/// arrives, what is held is kept, and the runs going are stopped, since
/// they cannot go on on infra that is down.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TakeDownReaders {
    pub broken: Vec<InfraCopy>,
}

impl TakeDownReaders {
    /// Never empty: a take-down aimed at no copy is refused.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.broken.is_empty() {
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
    pub instance: Option<weft_core::instance::InstanceId>,
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

    /// Project the typed spec onto the (verb, spec_json) columns the
    /// `infra_lifecycle_command` schema stores: Deactivate carries the
    /// copies whose triggers go down (`broken`) inside `spec_json`, the way
    /// they go down being the fixed `TakeDown::HEALTH`; Reactivate carries
    /// its still-broken copies there.
    pub fn into_row_columns(self) -> (InfraLifecycleVerb, Option<Value>) {
        match self {
            Self::Deactivate(take_down) => {
                let payload = serde_json::to_value(take_down).expect("TakeDownReaders serializes");
                (InfraLifecycleVerb::Deactivate, Some(payload))
            }
            Self::Reactivate(restore) => {
                let payload = serde_json::to_value(restore).expect("RestoreReaders serializes");
                (InfraLifecycleVerb::Reactivate, Some(payload))
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

/// Queueing an infra apply. Not safe twice: only an apply still waiting is
/// matched, so one sent again after the first ended applies again.
pub const INFRA_ENQUEUE_APPLY: BrokerCall = BrokerCall { path: "/v1/infra/enqueue_apply", twice: Twice::NotSafe };

/// Worker-callable: enqueue an Apply lifecycle command for the
/// supervisor that owns the project. The engine uses this after `node.provision_infra()`
/// returns a fresh InfraSpec. The supervisor reads the prior
/// `infra_node` row, compiles the new spec, hashes, and decides
/// skip / fresh / replace internally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEnqueueApplyRequest {
    pub project_id: Uuid,
    /// The node's PLACEMENT, spelled the way a person writes the node
    /// (`db`, or `one.db` inside the file the site `one` includes): the
    /// key its `infra_node` row and every resource are made under. A
    /// file included twice is applied twice, once per place.
    pub node_id: String,
    /// Whose copy: the setup run's instance, for a node that exists once
    /// per instance; `None` for a shared node.
    #[serde(default)]
    pub instance: Option<weft_core::instance::InstanceId>,
    pub spec_json: serde_json::Value,
    /// The infra setup run applying it: the build it runs says which
    /// version of the place's infra the copy will be, stamped on the copy
    /// when the apply succeeds.
    pub run_id: weft_core::RunId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEnqueueApplyResponse {
    pub command_id: i64,
}

/// Waiting for an apply to end. Harmless twice: it only reads.
pub const INFRA_WAIT_APPLY: BrokerCall = BrokerCall { path: "/v1/infra/wait_apply", twice: Twice::Harmless };

/// Worker-callable: wait for a previously-issued apply command to
/// reach terminal state. The broker holds the request until the
/// command completes or the hold ends; the worker asks again until it
/// completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraWaitApplyRequest {
    pub project_id: Uuid,
    pub command_id: i64,
    /// How long to hold for the command to complete; the answer is its
    /// state either way.
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

/// A project's image tags. Harmless twice: it only reads.
pub const SUPERVISOR_PROJECT_IMAGE_TAGS: BrokerCall = BrokerCall { path: "/v1/supervisor/project_image_tags", twice: Twice::Harmless };

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
// SYNC: the column's writer <-> crates/weft-dispatcher/src/build/mod.rs
//       (the version build that makes the images and names them) +
//       crates/weft-dispatcher/src/project_store.rs (register_with_hashes,
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

/// The units of the `infra_node` row aliased `n`, as SQL: one JSON
/// object of unit name to [`UnitRuntime`] built from its `infra_unit`
/// rows (`{}` for a copy with none), which [`decode_units`] reads. THE
/// read of a copy's roster: the broker (the supervisor's reads), the
/// dispatcher's row reader and the referenced-images keep-set all select
/// it.
// SYNC: units_of_copy_sql <-> UnitRuntime (its fields), crates/weft-dispatcher/src/infra_node.rs (infra_unit, UNITS_MOVE), crates/weft-broker/src/lifecycle_writes.rs (write_units)
pub fn units_of_copy_sql(n: &str) -> String {
    format!(
        "(SELECT COALESCE(jsonb_object_agg(u.unit, jsonb_build_object( \
             'status', u.status, 'stop_behavior', jsonb_build_object('kind', u.stop_behavior), \
             'flaky_after_seconds', u.flaky_after_seconds, 'recovery_after_seconds', u.recovery_after_seconds, \
             'not_ready_since_unix', u.not_ready_since_unix, 'ready_since_unix', u.ready_since_unix, \
             'image_refs', to_jsonb(u.image_refs), \
             'last_restart', CASE WHEN u.last_restart_at_unix IS NOT NULL THEN jsonb_build_object( \
                 'atUnix', u.last_restart_at_unix, \
                 'cause', jsonb_strip_nulls(jsonb_build_object('kind', u.last_restart_cause, 'code', u.last_restart_exit_code))) END)), \
             '{{}}'::jsonb) \
          FROM infra_unit u WHERE u.project_id = {n}.project_id AND u.node_id = {n}.node_id \
            AND u.instance_id IS NOT DISTINCT FROM {n}.instance_id)"
    )
}

/// One copy's units as [`units_of_copy_sql`] selects them, typed. Loud on
/// a bad value, never coerced to empty (an empty roster would read as
/// "nothing runs here" to the stop loop and shrink the keep-set below
/// what still runs). `project_id` and `node_id` name the copy.
pub fn decode_units(
    value: serde_json::Value,
    project_id: uuid::Uuid,
    node_id: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, UnitRuntime>> {
    serde_json::from_value(value).map_err(|e| {
        anyhow::anyhow!("the units of project={project_id} node={node_id} do not read as units: {e}")
    })
}

/// Removing an infra copy. Harmless twice: the second finds nothing left to
/// remove, and the caller reads only that the write was applied.
pub const SUPERVISOR_REMOVE_NODE: BrokerCall = BrokerCall { path: "/v1/supervisor/remove_node", twice: Twice::Harmless };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRemoveNodeRequest {
    /// The supervisor's claim identity (its replica id); the broker
    /// gates the cascade-delete on it holding the project's live
    /// `infra_owner` lease. See [`SupervisorSetStatusRequest::replica`].
    pub replica: String,
    pub project_id: Uuid,
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// The terminate removing it: the one command of this copy the
    /// cascade does not cancel, since it is the one finishing.
    pub command_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRemoveNodeResponse {
    pub removed: bool,
}

/// Completing a command. Not safe twice: the second finds it completed and
/// is answered that its target is gone ([`WriteOutcome`](crate::client::WriteOutcome)'s `Gone`).
pub const SUPERVISOR_COMMAND_COMPLETE: BrokerCall = BrokerCall { path: "/v1/supervisor/command_complete", twice: Twice::NotSafe };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorCommandCompleteRequest {
    /// The supervisor's claim identity (its replica id); the broker
    /// gates the terminal write on it holding the project's live
    /// `infra_owner` lease, so a displaced process can't complete a command
    /// the new owner must re-run. See [`SupervisorSetStatusRequest::replica`].
    pub replica: String,
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

/// A project's uncompleted commands. Harmless twice: it only reads.
pub const SUPERVISOR_INFRA_COMMAND_IN_FLIGHT: BrokerCall = BrokerCall { path: "/v1/supervisor/infra_command_in_flight", twice: Twice::Harmless };

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
    pub copies: weft_core::instance::Copies,
}

impl InFlightCommand {
    /// Whether it acts on the copy of `node_id` owned by `instance`.
    // SYNC: InFlightCommand::reaches <-> crates/weft-broker-client/src/lifecycle_command.rs (command_reaches_copy)
    pub fn reaches(&self, node_id: &str, instance: Option<&weft_core::instance::InstanceId>) -> bool {
        self.node_id.as_deref().is_none_or(|n| n == node_id) && self.copies.admits(instance)
    }
}

// ---------- Signals ----------

/// Whether the activation `a` governing a signal (its trigger's for an
/// entry, the trigger's that fired its run for a wait; read through
/// [`SIGNAL_ACTIVATION_JOIN`]) has its listeners listen: being activated
/// (the rehydrate at the end of activate runs before the flip to active),
/// live, or down with a door that holds what arrives (within its window,
/// when it has one), since what such a trigger hears waits for it to be
/// back on (`weft_core::arrival`). A signal no activation governs listens.
/// A door that refuses, or whose window ended, stops listening, its rows
/// kept for when the trigger is back. What a starting listener rehydrates,
/// what holders claim, and how many holders run all read this.
// SYNC: ACTIVATION_LISTENS <-> weft_core::arrival::Standing::arrival, crates/weft-dispatcher/src/activation_store.rs (DOOR_WINDOW_OPEN_SQL)
pub const ACTIVATION_LISTENS: &str = "(a.status IS NULL OR a.status IN ('activating', 'active') \
    OR (a.status = 'inactive' AND a.accepting_fires \
        AND (a.fires_deadline_unix IS NULL OR a.fires_deadline_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT)))";

/// The join every "is this signal held" read goes through: the
/// activation governing signal `s` as `a` (absent when none governs it).
pub const SIGNAL_ACTIVATION_JOIN: &str = "LEFT JOIN trigger_activation a \
    ON a.project_id = s.project_id AND a.trigger = s.activation_trigger \
    AND a.instance_id IS NOT DISTINCT FROM s.instance_id";

/// Whether signal `$1` is still held under the claim of holder `$2`: what
/// a held connection's fire must find as it arrives. A lapsed lease nobody
/// took over still counts, since no other copy serves the row. An event
/// that arrived while its holder held the row is delivered even if
/// another holder takes the row a moment later: that one's connection
/// only hears what comes after it took it.
pub const STILL_HELD_BY: &str = "SELECT EXISTS (SELECT 1 FROM signal WHERE token = $1 AND holds AND held_by = $2)";

/// The signals a listener holds. Harmless twice: it only reads.
pub const SIGNAL_LIST_HELD: BrokerCall = BrokerCall { path: "/v1/signal/list_held", twice: Twice::Harmless };

/// Every signal the listener must hold: the rows whose governing
/// activation listens ([`ACTIVATION_LISTENS`]), of one
/// project when `project` names it (an activation's rehydrate), of every
/// project when it is `None` (a listener's boot).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalListHeldRequest {
    pub project: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalListHeldResponse {
    pub rows: Vec<SignalRowWire>,
}

/// Where a signal's event goes. Harmless twice: it only reads.
pub const SIGNAL_FIRE_TARGET: BrokerCall = BrokerCall { path: "/v1/signal/fire_target", twice: Twice::Harmless };

/// Where an event of the signal `token` goes: what the listener asks
/// before handing an event to the worker's door of its project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalFireTargetRequest {
    pub token: String,
}

/// See [`SignalFireTargetRequest`]. `None` when the signal is gone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalFireTarget {
    pub project_id: uuid::Uuid,
    /// It answers one waiting run: the install hands it the answer.
    pub is_resume: bool,
    /// Where the project's front answers (`project.api_address`), `None`
    /// while nothing of the project takes work there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

/// One held signal. Harmless twice: it only reads.
pub const SIGNAL_GET_HELD: BrokerCall = BrokerCall { path: "/v1/signal/get_held", twice: Twice::Harmless };

/// One held signal by token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalGetHeldRequest {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalGetHeldResponse {
    /// `None` when no held row has that token (gone, or parked).
    pub row: Option<SignalRowWire>,
}

/// Writing a signal kind's state. Not safe twice: it writes only over the
/// state it was read from, so the second answers that it did not land.
pub const SIGNAL_WRITE_KIND_STATE: BrokerCall = BrokerCall { path: "/v1/signal/write_kind_state", twice: Twice::NotSafe };

/// Claim a signal kind's moment: write its durable state at version
/// `from_seq + 1`, landing only while the row is still at `from_seq`, so
/// of two listeners woken for the same moment exactly one wins, and a
/// registration that moved the row since the read makes the claim lose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalWriteKindStateRequest {
    pub token: String,
    pub kind_state: Value,
    pub from_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalWriteKindStateResponse {
    pub written: bool,
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
/// dispatcher's process-move restore, so the two readers of the same
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
    /// held-event fire). Filled per row by the `list_for_instance` path the
    /// listener rehydrates from.
    pub tenant_id: String,
    /// Whose signal it is (`None` for a shared one), from the row's
    /// `project_id` and `instance_id`: the rehydrated signal reads through
    /// that instance's connections alone, as it did when first registered.
    pub for_instance: Option<weft_core::instance::InstanceScope>,
    pub node_id: String,
    /// The signal's `SignalSpec`, in the broker's shape of today: it
    /// reads the row's through `SignalSpec::STORED`.
    pub spec_json: String,
    pub is_resume: bool,
    pub run_id: Option<RunId>,
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
    /// `TEXT NOT NULL DEFAULT '{}'` (its JSON text), so the broker always
    /// emits a value; missing on the wire is schema drift.
    pub kind_state: Value,
    /// The kind_state write-fence version the row is at (the
    /// `signal.kind_state_seq` column). The rehydrating process resumes
    /// its durable cursor writes at `seq + 1` so a restart can never
    /// regress the fence.
    pub kind_state_seq: i64,
    /// Whether the signal keeps a connection open between fires, which a
    /// holder holds (the `signal.holds` column).
    pub holds: bool,
    /// For a held signal: what its holder says the connection is doing,
    /// while a holder holds it; `None` while none does.
    pub serving: Option<HeldServing>,
    /// The registration this row is (the `signal.generation` column): a
    /// number every registration upsert and restore of the token moves
    /// to one never used before, so a wake names the registration it was
    /// set for and a stale one is told apart from the current one.
    #[serde(default)]
    pub generation: i64,
    /// Why the listener or its holder gave up on this signal for good
    /// (`signal.held_failed`), shown on its node until a registration
    /// again (or an account reconnect, which registers again) clears it.
    /// `None` while it is not given up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_failed: Option<String>,
}

/// What a holder says one held connection is doing, for its node's
/// display (the `signal.serving` column).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldServing {
    /// The serving task's own words ("connecting", "listening").
    pub status: String,
    /// Which transport serves it, once the task decided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<HeldTransport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HeldTransport {
    Socket,
    Webhook,
    Unservable { reason: String },
}

/// One look of a holder. Not safe twice: with little room, a second look may
/// take another signal in place of one the first took, which then stays
/// claimed by a holder that does not hold it.
pub const SIGNAL_HOLD: BrokerCall = BrokerCall { path: "/v1/signal/hold", twice: Twice::NotSafe };

/// One look of a holder (`signal/hold`): say what each connection it
/// holds is doing when that changed, learn which of them are still its
/// own, and take more when it has room. A claim lasts until its holder
/// lets it go or the holder's liveness check releases it (`/held/check`);
/// holders share the signals by taking only the ones no holder claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalHoldRequest {
    /// The holder; the call must come from this replica.
    pub replica: String,
    /// Every signal it holds now, with what its connection is doing when
    /// that changed since the last look.
    pub holding: Vec<HeldNow>,
    /// How many more it may take; `None` takes every one there is (a
    /// local install's one process holds them all).
    pub room: Option<u32>,
    /// Signals to take ahead of any other, when free: one a local install
    /// brings up the moment it is registered.
    #[serde(default)]
    pub want: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeldNow {
    pub token: String,
    #[serde(default)]
    pub serving: Option<HeldServing>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalHoldResponse {
    /// Of what it holds, what is still its own. The rest it stops.
    pub kept: Vec<String>,
    /// Of what it holds, what has ended: the row is gone, or its
    /// activation is no longer held. Its holder stops it and tears down
    /// what it arranged outside. One neither kept nor ended went to
    /// another holder (or its row was rewritten and is taken afresh), so
    /// its holder only stops it here: a teardown would undo what the new
    /// holder arranged under the same token.
    pub ended: Vec<String>,
    /// What it took on this look, to bring up.
    pub taken: Vec<SignalRowWire>,
    /// The load weft keeps for it as it last said it
    /// ([`SignalHolderLoadRequest`]), in percent of its CPUs, once this
    /// look is done: `None` when it said none, or what it said is behind
    /// what it holds (this look or an earlier one took signals, or it
    /// handed some over in saying it), so it measures afresh and says it
    /// again.
    pub load: Option<u16>,
    /// The most signals a spare up could take, `None` when none is up: a
    /// spare is another holder that said its load and claims nothing, a
    /// copy the sizing added for the load and waiting for work. While one
    /// is, a holder over the target hands over to it, at most this many
    /// ([`SignalHolderLoadRequest::spare_only`]).
    pub spare: Option<u32>,
}

/// A holder saying its load. Not safe twice: a second arrival of a
/// hand-over finds what the first handed over no longer the holder's (or
/// the spare it went to no longer a spare) and is refused, so the holder
/// would read a hand-over that went through as one that did not, and keep
/// its full room. One that hands nothing over is harmless twice, but the
/// line decides per call, not per request.
pub const SIGNAL_HOLDER_LOAD: BrokerCall = BrokerCall { path: "/v1/signal/holder_load", twice: Twice::NotSafe };

/// A holder in a pool saying the CPU it uses (`signal/holder_load`), in
/// percent of the CPUs it is given, whenever that moved a band since it
/// last said it: never on a timer, so a pool whose load holds still
/// writes nothing. A holder handing work over lets go of `let_go` in the
/// same transaction, each connection already stopped on its side, to a
/// spare when one is up (else freed, or nothing written when
/// `spare_only`); a load said that way is behind what it holds, and it
/// says its load again once measured afresh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalHolderLoadRequest {
    /// The holder; the call must come from this replica.
    pub replica: String,
    /// Of what it keeps, after any hand-over: the moved work is counted by
    /// whoever takes it, once measured there.
    pub cpu_percent: u16,
    /// The most signals it takes from now on: what it kept after handing
    /// some over, until a load said under the target gives it its full
    /// room back.
    pub room: u32,
    /// The signals it hands over; empty for a load said with nothing to
    /// hand over.
    pub let_go: Vec<String>,
    /// Hand `let_go` only to a spare, writing nothing when none is free:
    /// a holder only over the target, whose work is worth a reconnect only
    /// when a spare takes it. `false` over the high mark, which sheds its
    /// work either way.
    pub spare_only: bool,
}

/// Recording whether a signal is held. Not safe twice: it clears a give-up,
/// so a second arrival after one would undo it.
pub const SIGNAL_SET_HOLDS: BrokerCall = BrokerCall { path: "/v1/signal/set_holds", twice: Twice::NotSafe };

/// Record whether a signal keeps a connection open (`signal.holds`): what
/// its kind decides for it now, for a row that says otherwise (one
/// registered under a weft whose kind decided differently).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalSetHoldsRequest {
    pub token: String,
    pub holds: bool,
}

/// A holder letting go of its claims. Harmless twice: the second finds
/// nothing claimed by it.
pub const SIGNAL_LET_GO: BrokerCall = BrokerCall { path: "/v1/signal/let_go", twice: Twice::Harmless };

/// Give up every claim `replica` holds (a holder stopping), so another
/// holder takes them at once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalLetGoRequest {
    pub replica: String,
}

// ---------- Provider-event serving (listener <-> broker) ----------

/// Resolving a connection's event source. Harmless twice: it reads, and a
/// token due for a refresh is refreshed once, as for a connection.
pub const LISTENER_RESOLVE: BrokerCall = BrokerCall { path: "/v1/access/listener-resolve", twice: Twice::Harmless };

/// The listener's event-source resolve: taken from the registered
/// signal (the tenant traveled with it at registration). The broker
/// answers from the store's resolve; this crate holds the wire shape
/// so listener and broker cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerResolveRequest {
    pub tenant: String,
    /// Whose signal the connection serves (`None` for a shared one), as
    /// registered, the same trust as `tenant`: an instance's trigger reads
    /// through that instance's connection alone. Carried rather than read
    /// off the signal row, because a trigger resolves while it is being
    /// registered, before its row exists.
    pub for_instance: Option<weft_core::instance::InstanceScope>,
    pub access_id: String,
    pub service: String,
    /// The stored values the SIGNAL's input declared it needs (the
    /// trigger half of `requiresValues`): a held pipe signing in with
    /// the connection's own values is refused here, naming the
    /// missing one, rather than dialing forever with a blank address.
    #[serde(default)]
    pub required_values: Vec<String>,
}

/// Giving up on a signal. Harmless twice: only a signal not given up yet is
/// written, and the answer is the same either way.
pub const SIGNAL_GIVE_UP: BrokerCall = BrokerCall { path: "/v1/signal/give_up", twice: Twice::Harmless };

/// Give up on a signal for good (`signal/give_up`): it could not come up
/// after its retries, or its wake can never be handled (a spec or state
/// nothing can read, a kind no listener knows). Written once with why,
/// fenced on the registration it gave up on (`generation`), so a newer
/// registration of the token is never marked; a held one is also let go
/// (only by the holder whose claim it is, the caller's replica). A row
/// already given up, or registered again since, is left as it is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalGiveUpRequest {
    pub token: String,
    pub generation: i64,
    pub reason: String,
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

/// Keeping a provider-side subscription alive. Harmless twice: under the
/// signal's lock, one still fresh is answered as it is.
pub const SUBSCRIPTION_ENSURE: BrokerCall = BrokerCall { path: "/v1/access/subscription/ensure", twice: Twice::Harmless };

/// Whether keeping a provider-side subscription alive could work on this
/// install, asked before anything is subscribed (a resync, before it takes
/// the trigger down): the same request as [`SUBSCRIPTION_ENSURE`]. Reads
/// only, so harmless twice.
pub const SUBSCRIPTION_CHECK: BrokerCall = BrokerCall { path: "/v1/access/subscription/check", twice: Twice::Harmless };

/// Ask the broker to make sure a live provider subscription serves
/// this signal (subscribing or renewing as needed), or, at
/// [`SUBSCRIPTION_CHECK`], whether it could.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEnsureRequest {
    pub tenant: String,
    pub service: String,
    pub topic: String,
    pub access_id: String,
    pub signal_token: String,
    /// Whose signal the subscription serves (`None` for a shared one),
    /// as registered: the subscription dials through that instance's
    /// connection alone.
    pub for_instance: Option<weft_core::instance::InstanceScope>,
    #[serde(default)]
    pub params: std::collections::BTreeMap<String, String>,
}

/// When the provider will stop sending (unix seconds; `None` =
/// never), so the serving loop knows when to come back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEnsureResponse {
    pub expires_at_unix: Option<i64>,
}

/// Tearing a provider-side subscription down. Harmless twice: the second
/// finds nothing left to drop.
pub const SUBSCRIPTION_DROP: BrokerCall = BrokerCall { path: "/v1/access/subscription/drop", twice: Twice::Harmless };

/// Stop (at the provider) and forget every subscription serving a
/// signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionDropRequest {
    pub tenant: String,
    pub signal_token: String,
}

/// Checking the caller of a gated route. Harmless twice: it only reads.
pub const CALLER_VERIFY: BrokerCall = BrokerCall { path: "/v1/caller/verify", twice: Twice::Harmless };

/// One caller of a live route, as the worker's door forwards it to the
/// broker to check against the connection the route is gated by. The
/// broker holds the connection (its `verify` recipe and its stored
/// material); the door never sees a secret. The body is base64 (an
/// HMAC scheme signs the exact bytes); a scheme that does not read it
/// is handed an empty one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallerVerifyRequest {
    pub tenant: String,
    /// The instance whose route this is, when the gate is its own
    /// connection (an instance's trigger); `None`: a shared route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_instance: Option<weft_core::instance::InstanceScope>,
    pub access_id: String,
    pub service: String,
    pub method: String,
    /// The path as called under the project (no tenant, no leading
    /// slash), for the broker's log.
    pub path: String,
    /// The full address the caller sent the request to, as its own
    /// `Host` / `X-Forwarded-*` name it, with the path and query exactly
    /// as sent (still percent-encoded): what a scheme signing `{url}`
    /// (Twilio's) covers. `None` when the request names no usable host;
    /// such a scheme then refuses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Every header as sent, a repeated one once per value, so a scheme
    /// sees all of them (a signature header sent twice, a header the
    /// signature covers that must not be ambiguous).
    #[serde(default)]
    pub headers: Vec<(String, String)>,
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
    /// The request headers the scheme read a credential from, which the
    /// run never writes down.
    #[serde(default)]
    pub credential_headers: Vec<String>,
}

// ---------- The worker's door (`weft_engine::door`) ----------

/// Putting a fire in its trigger's queue. Not safe twice: a fire already
/// taken off the queue is not recognised, so one sent again after that is
/// delivered again.
pub const DOOR_PARK_FIRE: BrokerCall = BrokerCall { path: "/v1/door/park_fire", twice: Twice::NotSafe };

/// `POST /v1/door/park_fire`: put a fire in the queue of a trigger
/// (`weft_task_store::parked_fires`): one of the asking worker's project's,
/// or, for the listener, the trigger of an event whose worker it could not
/// reach.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorParkRequest {
    pub token: String,
    pub fire: weft_task_store::parked_fires::Waiting,
    /// The holder a held connection picked the event up under: it is
    /// kept only while the signal is still held under that name, and once
    /// kept it is the trigger's, whoever holds the signal by the time it is
    /// handed over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
}

/// What became of a [`DoorParkRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoorParked {
    /// Queued (or queued already under its id).
    Parked,
    /// The queue is at its cap: the fire is not kept.
    QueueFull,
    /// The signal is gone.
    Gone,
    /// The trigger takes no work (its door refuses, or its window ended): the fire
    /// is not kept.
    TakesNoWork,
    /// The holder it was picked up under no longer holds the signal: it is
    /// not kept, and the holder hears so.
    NotHeld,
}

/// Every trigger of the worker's project. Harmless twice: it only reads.
pub const DOOR_TRIGGERS: BrokerCall = BrokerCall { path: "/v1/door/triggers", twice: Twice::Harmless };

/// `POST /v1/door/triggers`: every trigger of the asking worker's project
/// (its entries: a waiting run's wait is no trigger), as it is armed and as
/// its activation stands: what the worker's door admits callers and
/// events by. A worker keeps them and reads them again when its line says
/// they changed (`line::TRIGGERS_CHANNEL`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DoorTriggersRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorTriggers {
    pub triggers: Vec<DoorTrigger>,
}

/// One trigger of the project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorTrigger {
    /// Its signal token: names it in a fire, and in the limits' counts.
    pub token: String,
    /// Its place, spelled the way the program reads it (`door`, `one.door`).
    pub node_id: String,
    /// Where it is mounted, for a route somebody calls: `None` for every
    /// other trigger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<DoorMount>,
    pub entry: DoorEntry,
    /// The holder serving it, for a signal a holder holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
}

/// Where a route is mounted under the project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorMount {
    /// The path pattern under the project (`chat/{room}`, `` for its root).
    pub pattern: String,
    /// The methods it serves; empty serves any.
    pub methods: Vec<String>,
}

/// A trigger as its row arms it, or why work for it cannot be served (a
/// row half written by an older install: activating again re-arms it).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DoorEntry {
    Armed(Box<ArmedEntry>),
    Unservable {
        /// The HTTP status a caller is answered with.
        status: u16,
        why: String,
    },
}

/// What a worker needs to admit a caller and give birth to the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArmedEntry {
    /// The trigger's signal spec: its kind and config, its limits, how its
    /// runs are kept.
    pub spec: weft_core::primitive::SignalSpec,
    /// `none`, or `connection` with its `{access_id, service}` in
    /// `auth_config`.
    pub auth_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_config: Option<Value>,
    /// What the trigger registered at activation, replayed by a fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_snapshot: Option<Value>,
    /// The program the route's runs are born under. A worker serves only
    /// the routes armed for its own binary.
    pub definition_hash: String,
    pub binary_hash: String,
    /// The version its runs are prepared from.
    pub source_version: String,
    /// How the route's activation stands (`weft_core::arrival`).
    pub standing: weft_core::arrival::Standing,
    /// Whose route it is: the instance whose trigger armed it, `None` for
    /// a shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// What a run of the worker's project needs. Harmless twice: it only reads.
pub const DOOR_RUN_FACTS: BrokerCall = BrokerCall { path: "/v1/door/run_facts", twice: Twice::Harmless };

/// `POST /v1/door/run_facts`: what a run of the asking worker's project
/// for `instance` (or for nobody) starts with besides its caller: whether
/// the infra it reads is up, what the instance provides, and the install's
/// picks. Kept by the worker until its line says one of them changed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DoorRunFactsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DoorRunFacts {
    /// Every copy of the project's infra nodes, and whether it is up.
    pub infra: Vec<InfraCopyUp>,
    /// What the instance stores for the fields its program leaves to
    /// instances; `None` for a run for nobody.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_values: Option<weft_core::instance::InstanceValues>,
    /// The connections this install picked for the project.
    #[serde(default)]
    pub picks: weft_core::picks::Picks,
}

pub use weft_core::infra::run_gate::InfraCopyUp;

/// Who an instance token names. Harmless twice: it only reads.
pub const DOOR_INSTANCE_TOKEN: BrokerCall = BrokerCall { path: "/v1/door/instance_token", twice: Twice::Harmless };

/// `POST /v1/door/instance_token`: who an instance token
/// (`weft_core::instance::INSTANCE_TOKEN_HEADER`) presented at the door
/// names. A token that names nobody, or is not an instance token, is the
/// door's `401`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorInstanceTokenRequest {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorInstanceToken {
    pub project_id: Uuid,
    pub instance: weft_core::instance::InstanceId,
}

/// A worker letting go of a run. Not safe twice: a run handed back may be
/// claimed by another worker before the second arrives, which is then refused.
pub const DOOR_LET_GO: BrokerCall = BrokerCall { path: "/v1/door/let_go", twice: Twice::NotSafe };

/// `POST /v1/door/let_go`: the asking worker no longer drives this run of
/// its project, its whole record written, and whoever resumes it owns it
/// next.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorLetGoRequest {
    pub run_id: RunId,
    pub why: LetGo,
}

/// Why a worker lets go of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LetGo {
    /// It is parked on a wait: the signal it waits on resumes it.
    Parked,
    /// It was asked to let go of this worker (the worker is stopping, or
    /// its caller left on a platform that stops a worker no call holds
    /// open): it is queued for another worker as it is let go of.
    /// Refused (`409`) when the run is not the asking worker's.
    HandedBack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorLetGo {}

/// A worker's tick. Harmless twice: its lease, what it has under way and its
/// counts are set to what it sends, never added to (`weft_task_store::worker_door`).
pub const DOOR_TICK: BrokerCall = BrokerCall { path: "/v1/door/tick", twice: Twice::Harmless };

/// `POST /v1/door/tick`: once a second while it has work under way, a
/// worker says it is alive (its `worker_lease`) and how many runs it
/// drives per trigger, states what it counted at its door this minute, and
/// hears what the project's other copies counted. With nothing under way
/// it sends none, and its lease lapses; with only runs that wait, its last
/// tick vouches for it until the earliest of those waits gives up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorTickRequest {
    /// The image it runs (the program's binary hash): a drain of the runs
    /// on older images counts what the workers of those images drive.
    /// `None` for a process that runs no program (a node test's).
    pub binary_hash: Option<String>,
    /// How many runs it drives right now, per trigger token: what a drain
    /// of those triggers waits for.
    #[serde(default)]
    pub in_flight: std::collections::BTreeMap<String, u32>,
    /// The minute the counts are of (unix seconds at its start).
    pub window_start: i64,
    /// This copy's counts this minute, each its total so far.
    #[serde(default)]
    pub counts: Vec<DoorCount>,
    /// Until when (unix seconds) the lease holds without another tick: set
    /// by the last tick before a worker whose runs only wait stops ticking
    /// (`weft_task_store::worker_door::lease_until`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vouch_until: Option<i64>,
    /// Set on the first tick of a worker told to stop while its last tick
    /// vouched for it: its lease now ends before that vouch, and the broker
    /// rings the dispatcher's look at lapsed leases (`lapses`), which was
    /// booked for the vouch and nothing else would bring forward.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub shortens: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoorCount {
    pub key: String,
    pub hits: i64,
    /// The last reason given for the count, for a key that keeps one (a
    /// dropped fire's, `weft_core::signal::limits::dropped_key`). A tick's
    /// answer carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorTick {
    /// What the other copies counted this minute, summed per key.
    pub others: Vec<DoorCount>,
    /// How many copies of the project are alive, this one included.
    pub copies: u32,
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
    /// Every header as sent, a repeated one once per value (a signature
    /// header sent twice keeps both).
    #[serde(default)]
    pub headers: Vec<(String, String)>,
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
    use weft_core::wire_enum_roundtrip_tests;
    // One generated round-trip test per wire enum. Adding a new
    // wire_enum! means adding its name here; missing one is a
    // visible omission in this single list.
    wire_enum_roundtrip_tests!(
        InfraLifecycleVerb,
        InfraNodeStatus,
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

    /// A worker of an older weft asked for a whole record and claimed
    /// a run to get its record in the answer; neither call of its reads
    /// as this release's, so the broker refuses both naming the resync.
    #[test]
    fn the_older_record_and_claim_calls_do_not_read() {
        let run_id = uuid::Uuid::from_u128(0xc2);
        let claim = serde_json::to_value(RunClaimRequest { run_id, reads: RecordReads::ByPage }).unwrap();
        assert_eq!(claim, json!({ "run_id": run_id, "reads": "by_page" }));
        assert_eq!(serde_json::from_value::<RunClaimRequest>(claim).unwrap().reads, RecordReads::ByPage);
        let old_claim = serde_json::from_value::<RunClaimRequest>(json!({ "run_id": run_id })).unwrap_err();
        assert!(old_claim.to_string().contains("missing field `reads`"), "{old_claim}");

        let first = serde_json::to_value(RecordOfRequest { run_id, after: None }).unwrap();
        assert_eq!(first, json!({ "run_id": run_id, "after": null }));
        let old_record = serde_json::from_value::<RecordOfRequest>(json!({ "run_id": run_id })).unwrap_err();
        assert!(old_record.to_string().contains("missing field `after`"), "{old_record}");
    }

    #[test]
    fn publish_access_round_trip() {
        // The request carries no project: the broker resolves that
        // from the run, so a worker cannot name someone else's.
        let spec: weft_core::AccessSpec = serde_json::from_value(json!({
            "service": "postgres",
            "acquisition": { "kind": "static", "fields": [{ "name": "host" }] },
        }))
        .unwrap();
        let req = PublishAccessRequest {
            run_id: uuid::Uuid::from_u128(0xc1),
            node_id: "db".into(),
            service: "postgres".into(),
            spec,
            values: [("host".to_string(), "db.svc".to_string())].into_iter().collect(),
            label: Some("db".into()),
            per_instance: true,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["per_instance"], true, "whose copy published it rides as a flag; the instance is the run's");
        assert_eq!(v["run_id"], json!(uuid::Uuid::from_u128(0xc1)));
        assert_eq!(v["node_id"], "db");
        assert_eq!(v["values"]["host"], "db.svc");
        assert!(v.get("project_id").is_none(), "the project is never on the wire");

        // Both verbs answer the SAME shape, so a connection read back
        // is the same value as the one published.
        let published: PublishAccessResponse = serde_json::from_value(json!({
            "connection": { "connectionId": "id-1", "identity": "weft@db/app" },
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
            "connection": { "connectionId": "id-1", "identity": null },
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
            "project_json_version": 2,
            "definition_hash": "abc123",
        }))
        .unwrap();
        assert_eq!(resp.project_json, "{\"nodes\":[]}");
        assert_eq!(resp.project_json_version, 2);
        assert_eq!(resp.definition_hash, "abc123");
    }

    /// The supervisor tries again after 5 s, then 10 s, and gives up at
    /// the third failure in a row.
    #[test]
    fn the_supervisor_tries_twice_more_then_gives_up() {
        use std::time::Duration;
        assert_eq!(supervisor_retry_after(1), Some(Duration::from_secs(5)));
        assert_eq!(supervisor_retry_after(2), Some(Duration::from_secs(10)));
        assert_eq!(supervisor_retry_after(SUPERVISOR_GIVE_UP_AFTER), None);
        assert_eq!(supervisor_retry_after(40), None);
    }

    #[test]
    fn sync_ownership_round_trip() {
        let req = SupervisorSyncOwnershipRequest {
            replica: "sup-1".into(),
            held_projects: None,
            looked: vec![Uuid::from_u128(0xa1)],
            settled: vec![],
            failed: vec![ProjectTrouble { project_id: Uuid::from_u128(0xa1), error: "no host".into() }],
            gave_up: vec![],
            all_failed: Some("the host did not list".into()),
            ending: true,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "replica": "sup-1",
                "held_projects": null,
                "looked": ["00000000-0000-0000-0000-0000000000a1"],
                "settled": [],
                "failed": [{ "project_id": "00000000-0000-0000-0000-0000000000a1", "error": "no host" }],
                "gave_up": [],
                "all_failed": "the host did not list",
                "ending": true,
            })
        );
        let listed: SupervisorSyncOwnershipRequest = serde_json::from_value(json!({
            "replica": "sup-1",
            "held_projects": ["00000000-0000-0000-0000-0000000000a1"],
        }))
        .unwrap();
        assert_eq!(listed.held_projects, Some(vec![Uuid::from_u128(0xa1)]));
        assert_eq!(listed.all_failed, None);
        let resp: SupervisorSyncOwnershipResponse = serde_json::from_value(json!({
            "owned": [
                {
                    "project_id": "00000000-0000-0000-0000-0000000000a1",
                    "tenant_id": "alice",
                    "status": "active",
                    "health_down": false,
                }
            ],
            "claimed": ["00000000-0000-0000-0000-0000000000a1"],
            "next_due_unix": 12,
        }))
        .unwrap();
        assert_eq!(resp.owned.len(), 1);
        assert_eq!(resp.owned[0].project_id.to_string(), "00000000-0000-0000-0000-0000000000a1");
        assert_eq!(resp.owned[0].status, ProjectStatus::Active);
        assert_eq!(resp.claimed, vec![resp.owned[0].project_id]);
        assert_eq!(resp.next_due_unix, Some(12));
    }

    #[test]
    fn claim_round_trip() {
        let req = SupervisorClaimCommandRequest {
            claimer_replica: "sup-1".into(),
            busy_commands: vec![7],
            cancelling: vec![],
            wait_ms: 25_000,
        };
        assert_eq!(
            serde_json::to_value(&req).unwrap(),
            json!({
                "claimer_replica": "sup-1",
                "busy_commands": [7],
                "cancelling": [],
                "wait_ms": 25_000,
            })
        );
        let command = SupervisorCommandRow {
            id: 7,
            project_id: uuid::Uuid::from_u128(0xa1),
            node_id: Some("db".into()),
            copies: weft_core::instance::Copies::Instance(weft_core::instance::InstanceId::new("user-42").unwrap()),
            verb: InfraLifecycleVerb::Apply,
            spec_json: None,
            force: false,
            cancel_requested: false,
        };
        for claim in [SupervisorClaim::Command(command), SupervisorClaim::UnownedWork, SupervisorClaim::Nothing, SupervisorClaim::Cancel { commands: vec![7] }] {
            let back: SupervisorClaim =
                serde_json::from_value(serde_json::to_value(&claim).unwrap()).unwrap();
            assert_eq!(format!("{back:?}"), format!("{claim:?}"));
        }
        assert_eq!(serde_json::to_value(SupervisorClaim::UnownedWork).unwrap(), json!({ "kind": "unowned_work" }));
    }

    #[test]
    fn owned_projects_round_trip() {
        let req = SupervisorOwnedProjectsRequest {
            replica: "sup-1".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "replica": "sup-1" }));
        let resp: SupervisorOwnedProjectsResponse = serde_json::from_value(json!({
            "owned": [
                {
                    "project_id": "00000000-0000-0000-0000-0000000000a1",
                    "tenant_id": "alice",
                    "status": "active",
                    "health_down": false,
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
                { "project_id": "00000000-0000-0000-0000-0000000000a1", "tenant_id": "alice" }
            ]
        }));
        assert!(
            res.is_err(),
            "missing status should fail deserialize; got {:?}",
            res
        );
    }

    /// `SupervisorProject::health_down` also has NO `serde(default)`:
    /// a missing field is schema drift and must fail loud (the broker +
    /// supervisor deploy together). Pin it so a future `serde(default)`
    /// slipping in would break CI rather than silently defaulting the
    /// auto-recover gate to false.
    #[test]
    fn owned_project_missing_health_parked_fails() {
        let res: Result<SupervisorOwnedProjectsResponse, _> = serde_json::from_value(json!({
            "owned": [
                { "project_id": "00000000-0000-0000-0000-0000000000a1", "tenant_id": "alice", "status": "active" }
            ]
        }));
        assert!(
            res.is_err(),
            "missing health_down should fail deserialize; got {:?}",
            res
        );
    }

    #[test]
    fn infra_node_response_round_trip() {
        let json = json!({
            "nodes": [
                {
                    "node_id": "tgi",
                    "copy_id": "wn-abc-tgi-12",
                    "status": "running",
                    "applied_spec_hash": "deadbeef",
                    "endpoints": { "api": "http://x.svc:8080" },
                    "public_paths": { "api": "/infra/p/wn-abc-tgi-12/hooks" },
                    "doors": { "sql": "10.0.0.7:5432" },
                    "keep_disks": ["model-cache"],
                    "units": { "main": {
                        "status": "running",
                        "stop_behavior": { "kind": "stop" },
                        "flaky_after_seconds": 30,
                        "recovery_after_seconds": 30
                    }},
                    "health_rules": [{
                        "name": "back",
                        "when": { "kind": "unit_flaky", "unit": "main" },
                        "action": { "kind": "bring_back" }
                    }]
                }
            ]
        });
        let resp: SupervisorInfraNodesResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.nodes[0].health_rules.as_ref().unwrap()[0].when.units_named(), vec!["main"]);
        assert_eq!(resp.nodes[0].addresses.urls.get("api").unwrap(), "http://x.svc:8080");
        assert_eq!(resp.nodes[0].addresses.public_paths.get("api").unwrap(), "/infra/p/wn-abc-tgi-12/hooks");
        assert_eq!(resp.nodes[0].addresses.doors.get("sql").unwrap(), "10.0.0.7:5432");
        assert_eq!(resp.nodes[0].units.get("main").unwrap().status, InfraNodeStatus::Running);
        assert_eq!(resp.nodes[0].applied_spec_hash.as_deref(), Some("deadbeef"));
        assert_eq!(resp.nodes[0].keep_disks, vec!["model-cache".to_string()]);
    }

    /// `SupervisorInfraNode::keep_disks` has NO `serde(default)`:
    /// pre-prod, broker + supervisor deploy together, missing field
    /// is schema drift. Pin the property so a future re-add of the
    /// default breaks CI.
    #[test]
    fn infra_node_missing_keep_disks_fails() {
        let json = json!({
            "nodes": [
                {
                    "node_id": "tgi",
                    "copy_id": "wn-abc-tgi-12",
                    "status": "running",
                    "applied_spec_hash": "deadbeef",
                    "endpoints": {}, "public_paths": {}, "doors": {}
                }
            ]
        });
        let res: Result<SupervisorInfraNodesResponse, _> = serde_json::from_value(json);
        assert!(res.is_err(), "missing keep_disks must fail deserialize");
    }

    #[test]
    fn set_applied_request_round_trip() {
        let req = SupervisorSetAppliedRequest {
            replica: "weft-infra-supervisor-abc".into(),
            command_id: 7,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "tgi".into(),
            instance: None,
            copy_id: "wn-abc-tgi-12".into(),
            applied_spec_hash: "deadbeef".into(),
            addresses: AppliedEndpoints {
                urls: [("api".to_string(), "http://x".to_string())].into(),
                public_paths: [("api".to_string(), "/infra/p/wn-abc-tgi-12/hooks".to_string())].into(),
                doors: [("sql".to_string(), "10.0.0.7:5432".to_string())].into(),
            },
            keep_disks: vec!["data".into()],
            notes: vec!["unit 'main' of 'tgi' asked for 1 x l4".into()],
            units: std::collections::BTreeMap::new(),
            health_rules: Some(weft_core::infra::health::built_in_rules()),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["notes"][0], "unit 'main' of 'tgi' asked for 1 x l4");
        // Flattened: the maps sit on the request itself, and the
        // URLs keep the `endpoints` name the broker writes to its column.
        assert_eq!(v["endpoints"]["api"], "http://x");
        assert_eq!(v["public_paths"]["api"], "/infra/p/wn-abc-tgi-12/hooks");
        assert_eq!(v["doors"]["sql"], "10.0.0.7:5432");
        let back: SupervisorSetAppliedRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.addresses, req.addresses);
        assert_eq!(back.command_id, 7);
        assert_eq!(back.keep_disks, vec!["data".to_string()]);
        assert_eq!(back.copy_id, "wn-abc-tgi-12");
        assert_eq!(back.health_rules, req.health_rules);
    }

    #[test]
    fn resolve_connection_request_round_trip() {
        let req = ResolveConnectionRequest {
            run_id: uuid::Uuid::from_u128(0xc1),
            node_id: "ask".into(),
            frames: vec![weft_core::frames::Frame::Loop { index: 3 }],
            node_type: "openrouter.inference".into(),
            connection_id: "11111111-2222-3333-4444-555555555555".into(),
            service: "openrouter".into(),
            required_permissions: vec!["drive.readonly".into()],
            required_values: vec!["imapHost".into()],
            expected_duration_secs: 120,
        };
        // The full literal object pins the FIELD NAMES on the wire: a
        // symmetric struct-field rename round-trips fine but breaks the
        // peer, so equality on the serialized form is the real contract.
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                // No project: the broker resolves it from the run,
                // so a worker never names one.
                "run_id": uuid::Uuid::from_u128(0xc1), "node_id": "ask",
                "frames": [{"index": 3}], "node_type": "openrouter.inference",
                "connection_id": "11111111-2222-3333-4444-555555555555",
                "service": "openrouter",
                "required_permissions": ["drive.readonly"],
                "required_values": ["imapHost"],
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
                keep_until_unix: None,
                published_by: None,
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
                    "owner": "platform",
                    "keep_until_unix": null
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
            run_id: uuid::Uuid::from_u128(0xc1),
            values: [("token".to_string(), "cred".to_string())].into_iter().collect(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "run_id": uuid::Uuid::from_u128(0xc1), "values": { "token": "cred" } }));
        let back: ReleaseConnectionRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.values["token"], "cred");
        assert_eq!(back.run_id, uuid::Uuid::from_u128(0xc1));
    }

    #[test]
    fn set_applied_missing_keep_disks_fails() {
        // Write path mirrors the read path: no serde(default) on
        // keep_disks, so a missing field is loud schema drift.
        let v = json!({
            "command_id": 7, "project_id": "00000000-0000-0000-0000-0000000000a1", "node_id": "tgi",
            "copy_id": "i", "applied_spec_hash": "h",
            "endpoints": {}, "public_paths": {}, "doors": {}, "units": {}
        });
        let res: Result<SupervisorSetAppliedRequest, _> = serde_json::from_value(v);
        assert!(res.is_err(), "missing keep_disks must fail deserialize");
    }

    #[test]
    fn set_provisioning_request_round_trip() {
        let req = SupervisorSetProvisioningRequest {
            replica: "weft-infra-supervisor-abc".into(),
            command_id: 9,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "tgi".into(),
            instance: None,
            copy_id: "wn-abc-tgi-12".into(),
            keep_disks: vec!["data".into()],
            units: std::collections::BTreeMap::new(),
            health_rules: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        let back: SupervisorSetProvisioningRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.command_id, 9);
        assert_eq!(back.keep_disks, vec!["data".to_string()]);
    }

    #[test]
    fn set_provisioning_missing_keep_disks_fails() {
        let v = json!({
            "command_id": 9, "project_id": "00000000-0000-0000-0000-0000000000a1", "node_id": "tgi",
            "copy_id": "i"
        });
        let res: Result<SupervisorSetProvisioningRequest, _> = serde_json::from_value(v);
        assert!(res.is_err(), "missing keep_disks must fail deserialize");
    }

    #[test]
    fn command_row_reads_with_its_defaults() {
        let v = json!({
            "id": 1,
            "project_id": "00000000-0000-0000-0000-0000000000a1",
            "verb": "stop"
        });
        let row: SupervisorCommandRow = serde_json::from_value(v).unwrap();
        assert_eq!(row.verb, InfraLifecycleVerb::Stop);
        assert_eq!(row.node_id, None);
    }

    fn take_down(broken: Vec<InfraCopy>) -> TakeDownReaders {
        TakeDownReaders { broken }
    }

    fn ada_svc() -> InfraCopy {
        InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) }
    }

    #[test]
    fn lifecycle_spec_round_trip_deactivate_and_reactivate() {
        let d = LifecycleSpec::Deactivate(take_down(vec![
            ada_svc(),
            InfraCopy { node_id: "db".into(), instance: None },
        ]));
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["verb"], "deactivate");
        assert_eq!(v["broken"], json!([{ "node_id": "svc", "instance": "ada" }, { "node_id": "db" }]));
        let back: LifecycleSpec = serde_json::from_value(v).unwrap();
        assert_eq!(d, back);

        let r = LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![ada_svc()] });
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["verb"], "reactivate");
        assert_eq!(v["still_broken"], json!([{ "node_id": "svc", "instance": "ada" }]));
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
            InfraEvent::Flaky(FlakyPayload { reason: "unit 'main' is not ready: crashloop".into() }),
            InfraEvent::Recovered,
            InfraEvent::Failed(FailedPayload {
                stage: FailureStage::Apply,
                message: "apply rejected".into(),
            }),
            InfraEvent::Stopped,
            InfraEvent::Terminated,
            InfraEvent::Started(StartedPayload {
                copy_id: "inst1".into(),
                mode: StartMode::Fresh,
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
            copies: weft_core::instance::Copies::Shared,
            verb: InfraLifecycleVerb::Terminate,
            spec_json: None,
            force: false,
            cancel_requested: false,
        };
        let v = serde_json::to_value(&row).unwrap();
        assert_eq!(v["node_id"], "n");
        assert_eq!(v["verb"], "terminate");
        // A terminate issued without its work is a writer bug, refused.
        assert!(row.terminate_work().is_err());
    }

    #[test]
    fn a_terminate_reads_its_disks_off_the_command() {
        let row = |spec_json| SupervisorCommandRow {
            id: 1,
            project_id: uuid::Uuid::from_u128(1),
            node_id: None,
            copies: weft_core::instance::Copies::Shared,
            verb: InfraLifecycleVerb::Terminate,
            spec_json: Some(spec_json),
            force: false,
            cancel_requested: false,
        };
        let wipe = row(json!({ "disks": "delete_all" })).terminate_work().unwrap();
        assert_eq!(wipe.disks, weft_core::infra::TerminateDisks::DeleteAll);
        let keep = row(json!({ "disks": "keep_listed" })).terminate_work().unwrap();
        assert_eq!(keep.disks, weft_core::infra::TerminateDisks::KeepListed);
        assert!(row(json!({ "disks": "some" })).terminate_work().is_err());
    }

    /// A dispatcher verb's spec rides whole in `spec_json` (Deactivate
    /// carries its running policy there) and reads back as written.
    #[test]
    fn a_dispatcher_verbs_spec_rides_in_its_row() {
        let d = LifecycleSpec::Deactivate(take_down(vec![ada_svc()]));
        let (verb, spec_json) = d.clone().into_row_columns();
        assert_eq!(verb, InfraLifecycleVerb::Deactivate);
        // The row columns carry the whole take-down, broken copies too,
        // and read back as what was written.
        assert_eq!(LifecycleSpec::from_row_columns(verb, spec_json).unwrap(), d);

        let r = LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![ada_svc()] });
        let (verb, spec_json) = r.clone().into_row_columns();
        assert_eq!(verb, InfraLifecycleVerb::Reactivate);
        assert_eq!(LifecycleSpec::from_row_columns(verb, spec_json).unwrap(), r);
        assert!(matches!(
            LifecycleSpec::from_row_columns(InfraLifecycleVerb::Reactivate, None),
            Err(FromRowColumnsError::ReactivateMissingSpec)
        ));
    }

    /// A take-down names at least one broken copy. A recovery has no
    /// rule: an empty still-broken set restores everything the loop took.
    #[test]
    fn a_take_down_names_its_broken_copies() {
        assert!(take_down(vec![ada_svc()]).validate().is_ok());
        assert!(take_down(vec![]).validate().is_err());
        assert!(LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![] }).validate().is_ok());
    }

    #[test]
    fn set_status_request_optional_fields_skip_when_none() {
        let r = SupervisorSetStatusRequest {
            replica: "weft-infra-supervisor-abc".into(),
            command_id: None,
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "n".into(),
            instance: None,
            unit: None,
            status: InfraNodeStatus::Running,
            failure_stage: None,
            failure_message: None,
            not_ready_since_unix: None,
            ready_since_unix: None,
            restart: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: SupervisorSetStatusRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.status, InfraNodeStatus::Running);
        assert_eq!(back.command_id, None);
    }

    #[test]
    fn set_status_request_with_command_id_round_trip() {
        let r = SupervisorSetStatusRequest {
            replica: "weft-infra-supervisor-abc".into(),
            command_id: Some(42),
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: "n".into(),
            instance: None,
            unit: None,
            status: InfraNodeStatus::Stopping,
            failure_stage: None,
            failure_message: None,
            not_ready_since_unix: None,
            ready_since_unix: None,
            restart: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: SupervisorSetStatusRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.command_id, Some(42));
        assert_eq!(back.status, InfraNodeStatus::Stopping);
    }

    /// A unit's last restart reads from the object `units_of_copy_sql`
    /// builds out of its columns: `null` when it has none, the cause's
    /// code only for an exit.
    #[test]
    fn a_units_last_restart_reads_as_the_sql_builds_it() {
        let unit = |last_restart: serde_json::Value| {
            json!({ "u": {
                "status": "running", "stop_behavior": { "kind": "stop" }, "flaky_after_seconds": 30,
                "recovery_after_seconds": 30, "not_ready_since_unix": null, "ready_since_unix": null,
                "image_refs": [], "last_restart": last_restart,
            } })
        };
        let project = uuid::Uuid::nil();
        let read = |v| decode_units(v, project, "n").unwrap().remove("u").unwrap().last_restart;
        assert_eq!(read(unit(serde_json::Value::Null)), None);
        assert_eq!(
            read(unit(json!({ "atUnix": 7, "cause": { "kind": "out_of_memory" } }))),
            Some(weft_core::infra::LastRestart { at_unix: 7, cause: weft_core::infra::RestartCause::OutOfMemory })
        );
        assert_eq!(
            read(unit(json!({ "atUnix": 7, "cause": { "kind": "exited", "code": 3 } }))),
            Some(weft_core::infra::LastRestart { at_unix: 7, cause: weft_core::infra::RestartCause::Exited { code: 3 } })
        );
        assert!(decode_units(unit(json!({ "atUnix": 7, "cause": { "kind": "sleepy" } })), project, "n").is_err());
    }

    #[test]
    fn event_record_request_round_trip() {
        let r = SupervisorEventRecordRequest {
            project_id: "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
            node_id: Some("n".into()),
            instance: None,
            kind: InfraEventKind::Flaky,
            payload: json!({ "desired": 3, "ready": 1 }),
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: SupervisorEventRecordRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.kind, InfraEventKind::Flaky);
        assert_eq!(back.payload["desired"], 3);
    }

    #[test]
    fn endpoint_url_response_handles_null() {
        let v = json!({ "address": null });
        let r: InfraEndpointUrlResponse = serde_json::from_value(v).unwrap();
        assert!(r.address.is_none());
    }

    #[test]
    fn endpoint_url_response_carries_the_public_url() {
        let v = json!({ "address": { "url": "http://a.ns.svc:80", "publicUrl": "http://door/infra/ns/a/hooks" } });
        let r: InfraEndpointUrlResponse = serde_json::from_value(v).unwrap();
        let address = r.address.unwrap();
        assert_eq!(address.url, "http://a.ns.svc:80");
        assert_eq!(address.public_url.as_deref(), Some("http://door/infra/ns/a/hooks"));
    }

    #[test]
    fn endpoint_url_request_carries_the_handle() {
        let v = json!({
            "run_id": "00000000-0000-0000-0000-0000000000a1",
            "infra": { "__weft_infra__": { "place": "n", "endpoint": "api", "instance": "ada" } }
        });
        let r: InfraEndpointUrlRequest = serde_json::from_value(v).unwrap();
        assert_eq!((r.infra.place(), r.infra.endpoint()), ("n", "api"));
        assert_eq!(r.infra.instance().map(|i| i.as_str()), Some("ada"));
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
            for_instance: None,
            node_id: "n".into(),
            kind_state_seq: 0,
            spec_json: "{}".into(),
            is_resume: false,
            run_id: None,
            surface_kind,
            mount_path: mount_path.map(|s| s.to_string()),
            mount_methods: Vec::new(),
            auth_kind,
            auth_config,
            kind_state: Value::Null,
            holds: false,
            serving: None,
            generation: 0,
            held_failed: None,
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
            for_instance: Some(weft_core::instance::InstanceScope {
                project_id: uuid::Uuid::from_u128(0xa1),
                instance: weft_core::instance::InstanceId::new("alice").unwrap(),
            }),
            access_id: "11111111-2222-3333-4444-555555555555".into(),
            service: "slack".into(),
            required_values: vec!["imapHost".into()],
        };
        // The full literal pins the FIELD NAMES on the wire: a
        // symmetric rename round-trips fine but breaks the peer.
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "tenant-a",
                "for_instance": {
                    "projectId": "00000000-0000-0000-0000-0000000000a1",
                    "instance": "alice"
                },
                "access_id": "11111111-2222-3333-4444-555555555555",
                "service": "slack",
                "required_values": ["imapHost"]
            })
        );
        let back: ListenerResolveRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.service, "slack");
        assert_eq!(back.for_instance, req.for_instance);
        // required_values defaults when absent; a shared signal's
        // instance is null.
        let bare: ListenerResolveRequest = serde_json::from_value(json!({
            "tenant": "t", "for_instance": null, "access_id": "a", "service": "s"
        }))
        .unwrap();
        assert!(bare.required_values.is_empty());
        assert!(bare.for_instance.is_none());
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
            recipe_values: [("appToken".to_string(), "xapp-1".to_string())]
                .into_iter()
                .collect(),
            provider_account: Some("T1".into()),
            expires_at_unix: None,
        };
        let v = serde_json::to_value(&src).unwrap();
        assert_eq!(v["values"], json!({ "token": "xoxb-1" }));
        assert_eq!(
            v["auth"],
            json!([{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }])
        );
        assert_eq!(v["recipe_values"], json!({ "appToken": "xapp-1" }));
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
            for_instance: None,
            params: [("target".to_string(), "file-9".to_string())].into_iter().collect(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "tenant-a", "service": "google", "topic": "drive",
                "access_id": "a-1", "signal_token": "sig-1", "for_instance": null,
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
    fn signal_hold_round_trip() {
        let req = SignalHoldRequest {
            replica: "holder-1".into(),
            holding: vec![HeldNow {
                token: "a".into(),
                serving: Some(HeldServing { status: "listening".into(), transport: Some(HeldTransport::Socket) }),
            }],
            room: Some(4),
            want: vec!["b".into()],
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "replica": "holder-1",
                "holding": [{ "token": "a", "serving": { "status": "listening", "transport": { "kind": "socket" } } }],
                "room": 4, "want": ["b"]
            })
        );
        let back: SignalHoldRequest = serde_json::from_value(v).unwrap();
        assert_eq!((back.holding[0].token.as_str(), back.room, back.want), ("a", Some(4), vec!["b".to_string()]));

        let mut taken = make_row(SignalSurfaceKind::Internal, None, SignalAuthKind::None, None);
        taken.holds = true;
        let resp = SignalHoldResponse { kept: vec!["a".into()], ended: vec!["c".into()], taken: vec![taken], load: Some(42), spare: Some(200) };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(
            (&v["kept"], &v["ended"], &v["taken"][0]["holds"], &v["load"], &v["spare"]),
            (&json!(["a"]), &json!(["c"]), &json!(true), &json!(42), &json!(200))
        );
        let back: SignalHoldResponse = serde_json::from_value(v).unwrap();
        assert_eq!(
            (back.kept, back.ended, back.taken.len(), back.load, back.spare),
            (vec!["a".to_string()], vec!["c".to_string()], 1, Some(42), Some(200))
        );
    }

    #[test]
    fn signal_holder_load_round_trip() {
        let req = SignalHolderLoadRequest { replica: "holder-1".into(), cpu_percent: 53, room: 3, let_go: vec!["a".into()], spare_only: true };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({ "replica": "holder-1", "cpu_percent": 53, "room": 3, "let_go": ["a"], "spare_only": true }));
        let back: SignalHolderLoadRequest = serde_json::from_value(v).unwrap();
        assert_eq!((back.cpu_percent, back.room, back.let_go, back.spare_only), (53, 3, vec!["a".to_string()], true));
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
            headers: vec![("x-slack-signature".to_string(), "v0=abc".to_string())],
            query: [("challenge".to_string(), "tok".to_string())].into_iter().collect(),
            method: "POST".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "service": "slack", "topic": "messages", "body_b64": "eyJ4IjoxfQ==",
                "headers": [["x-slack-signature", "v0=abc"]],
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
            for_instance: None,
            access_id: "acc-1".into(),
            service: "hmac_auth".into(),
            method: "POST".into(),
            path: "chat/room7".into(),
            url: Some("https://acme.example/p/chat/room7?v=1".into()),
            headers: vec![("x-signature".into(), "abc".into()), ("X-Signature".into(), "def".into())],
            body_b64: "e30=".into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "alice", "access_id": "acc-1", "service": "hmac_auth",
                "method": "POST", "path": "chat/room7",
                "url": "https://acme.example/p/chat/room7?v=1",
                "headers": [["x-signature", "abc"], ["X-Signature", "def"]],
                "body_b64": "e30="
            })
        );
        let back: CallerVerifyRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.path, "chat/room7");
        assert_eq!(back.headers.len(), 2, "a repeated header keeps every value");
        let bare: CallerVerifyRequest = serde_json::from_value(json!({
            "tenant": "t", "access_id": "a", "service": "s", "method": "GET", "path": ""
        }))
        .unwrap();
        assert!(bare.headers.is_empty() && bare.url.is_none() && bare.body_b64.is_empty());

        let ok = CallerVerified { identity: json!({ "key": 2 }), credential_headers: vec!["x-api-key".into()] };
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v, json!({ "identity": { "key": 2 }, "credential_headers": ["x-api-key"] }));
        let back: CallerVerified = serde_json::from_value(v).unwrap();
        assert_eq!(back.identity["key"], 2);
        assert_eq!(back.credential_headers, vec!["x-api-key".to_string()]);
        let older: CallerVerified = serde_json::from_value(json!({ "identity": null })).unwrap();
        assert!(older.credential_headers.is_empty());
    }

}



// ---------- The dispatcher's admin calls (`weft_dispatcher::broker_admin`) ----------

/// A person's lookup of a list source through a stored connection (a
/// `remote_select`). Harmless twice: it reads, from the provider and the
/// connection's row.
pub const ADMIN_LOOKUP: BrokerCall = BrokerCall { path: "/v1/access/admin/lookup", twice: Twice::Harmless };
/// A `granted` source read off a connection's row. Harmless twice: it
/// reads, and makes no outbound call.
pub const ADMIN_GRANTED: BrokerCall = BrokerCall { path: "/v1/access/admin/granted", twice: Twice::Harmless };
/// The services the apps file holds a sign-in app for. Harmless twice: it
/// reads the file.
pub const ADMIN_SIGN_IN_SERVICES: BrokerCall = BrokerCall { path: "/v1/access/admin/sign-in-services", twice: Twice::Harmless };
/// Which of the asked services the runtime holds its own credential for.
/// Harmless twice: it reads the shared-credentials file.
pub const ADMIN_SHARED_CREDENTIALS: BrokerCall = BrokerCall { path: "/v1/access/admin/shared-credentials", twice: Twice::Harmless };
/// The shared-door options of a spec. Harmless twice: the spec's event
/// recipes it records as it answers are upserted by their hash
/// (`weft_access_store::resolve`), and the rest reads.
pub const ADMIN_DOORS: BrokerCall = BrokerCall { path: "/v1/access/admin/doors", twice: Twice::Harmless };
/// A key pair for one move of a tenant's connections. Not safe twice: each
/// call makes a new key.
pub const ADMIN_MIGRATE_KEY: BrokerCall = BrokerCall { path: "/v1/access/admin/migrate/key", twice: Twice::NotSafe };
/// A tenant's picked connections, sealed to another install's key. Not
/// shown harmless twice, so never sent again.
pub const ADMIN_MIGRATE_EXPORT: BrokerCall = BrokerCall { path: "/v1/access/admin/migrate/export", twice: Twice::NotSafe };
/// Connections sealed to this install's key, written as the tenant's. Not
/// safe twice: it writes them.
pub const ADMIN_MIGRATE_IMPORT: BrokerCall = BrokerCall { path: "/v1/access/admin/migrate/import", twice: Twice::NotSafe };
/// An app of the install's own minted for a service. Not safe twice: it
/// writes one.
pub const ADMIN_MINT_APP: BrokerCall = BrokerCall { path: "/v1/access/admin/mint-app", twice: Twice::NotSafe };
/// A connection made from a key given straight. Not safe twice: it writes
/// one.
pub const ADMIN_CONNECT_DIRECT: BrokerCall = BrokerCall { path: "/v1/access/admin/connect/direct", twice: Twice::NotSafe };
/// The start of an OAuth connect. Not safe twice: each starts its own.
pub const ADMIN_OAUTH_BEGIN: BrokerCall = BrokerCall { path: "/v1/access/admin/oauth/begin", twice: Twice::NotSafe };
/// The end of an OAuth connect, spending its one-time code. Not safe twice.
pub const ADMIN_OAUTH_COMPLETE: BrokerCall = BrokerCall { path: "/v1/access/admin/oauth/complete", twice: Twice::NotSafe };
/// A connection's token handed to a provider's chooser. Not shown harmless
/// twice (it may refresh the token), so never sent again.
pub const ADMIN_PICKER_TOKEN: BrokerCall = BrokerCall { path: "/v1/access/admin/picker-token", twice: Twice::NotSafe };
/// A provider's push proven genuine. Not shown harmless twice, so never
/// sent again.
pub const EVENTS_VERIFY: BrokerCall = BrokerCall { path: "/v1/events/verify", twice: Twice::NotSafe };

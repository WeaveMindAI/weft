//! Journal abstraction. Single source of truth for execution state.
//!
//! Every state change the dispatcher cares about is an `ExecEvent`
//! row in the `exec_event` table. Readers fold the log on demand:
//! logs, node events, execution list, etc. See
//! `journal::events::fold_to_snapshot`.
//!
//! Separate tables still exist for lookups that aren't state
//! changes: entry tokens (webhook→project routing), suspension
//! tokens (form URL→color lookup), extension tokens (reviewer
//! auth). Those are indexes, not duplicates.

pub mod postgres;

#[cfg(any(test, feature = "test-helpers"))]
pub mod mock;
#[cfg(any(test, feature = "test-helpers"))]
pub use mock::MockJournal;

use weft_journal::ExecEvent;

use async_trait::async_trait;
use serde_json::Value;

use weft_core::Color;

/// Outcome of looking up a value derived from a color's first
/// `ExecutionStarted` row. `NotFound` = no such row (the color is
/// unknown). `Corrupt` = the row exists but its stored JSON no
/// longer decodes: a PERMANENT poison, so callers must word their
/// failure honestly ("journal row for color X is corrupt; see
/// dispatcher logs") and must NOT retry (retrying cannot fix it;
/// pollers that would loop on an `Err` skip instead). The one
/// producer of `Corrupt` is `execution_definition_hash`: the
/// project/tenant lookups read the `execution_color` mirror and
/// answer `Option` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorLookup<T> {
    Found(T),
    NotFound,
    Corrupt,
}

#[async_trait]
pub trait Journal: Send + Sync {
    // ----- Event log (state source of truth) -------------------------

    /// Append one event to the execution's log. Append-only; only
    /// user-initiated `weft clean` removes events.
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()>;

    /// Idempotent variant: a retry with the same `dedup_key` is a
    /// no-op via a partial UNIQUE index. Used by dispatcher tasks
    /// (e.g. route_entry) that may re-execute after a crash.
    async fn record_event_dedup(
        &self,
        event: &ExecEvent,
        dedup_key: &str,
    ) -> anyhow::Result<()>;

    /// Full ordered event log for a color, for DISPLAY: undecodable
    /// rows come back as their error text instead of failing the read,
    /// so the inspector renders what exists and names the rows it
    /// cannot. The one required read; [`Journal::events_log`] is
    /// derived from it.
    async fn events_log_lossy(
        &self,
        color: Color,
    ) -> anyhow::Result<(Vec<ExecEvent>, Vec<String>)>;

    /// The same log for STATE-REBUILDING (the cancel writers, stall
    /// re-folds): a row that no longer decodes fails the WHOLE read,
    /// naming the color and `weft clean`, because a fold over a
    /// partial log rebuilds a state that never existed.
    async fn events_log(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>> {
        let (events, bad) = self.events_log_lossy(color).await?;
        match bad.into_iter().next() {
            Some(reason) => Err(anyhow::Error::msg(reason)),
            None => Ok(events),
        }
    }

    // ----- Atomic execution birth / teardown --------------------------
    //
    // An execution's birth is ONE atomic fact: the `ExecutionStarted` event,
    // its `execution_color` seed, the entry kicks, AND the work item a worker
    // will claim. Committing them together is what makes a "ghost" (a
    // journaled live execution with no work item, which nothing would ever
    // run or reclaim and which would wedge a later drain) impossible by
    // construction, instead of something a failure path must remember to
    // clean up.

    /// ATOMICALLY journal an execution's birth together with its queued work
    /// item. Either everything commits or nothing does. `start` must be
    /// `ExecEvent::ExecutionStarted`; `kicks` are its `NodeKicked` events.
    async fn start_execution(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<()>;

    /// The live-connection variant of [`Journal::start_execution`]: the birth
    /// commits atomically WITH the pinned-task admission, and ONLY if a worker
    /// admits it. `Saturated` writes nothing (the caller spawns a pod and
    /// retries); `AlreadyAdmitted` (a crash-retry of the same handshake)
    /// writes nothing new and returns the originally chosen pod.
    async fn start_live_execution(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
        saturation: f64,
    ) -> anyhow::Result<weft_task_store::tasks::LiveAdmitOutcome>;

    /// ATOMICALLY tear down a live execution whose setup failed AFTER
    /// admission: delete its still-pending task and journal the cancel
    /// terminals (`NodeCancelled` per non-terminal node + `ExecutionCancelled`)
    /// in one transaction, so "task deleted" and "cancel journaled" can never
    /// disagree. `WorkerOwnsIt` means a worker already claimed the task: it
    /// owns the run and its own terminal, so nothing was cancelled.
    async fn cancel_never_claimed_execution(
        &self,
        color: Color,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<weft_task_store::tasks::SetupFailureOutcome>;

    /// THE dispatcher-side cancel of an execution, in ONE transaction:
    /// strip the color's wake signals (the parked form, the timer, the
    /// webhook, so nothing can revive it), journal its cancel terminals
    /// (`NodeCancelled` per non-terminal node + `ExecutionCancelled`,
    /// skipped when a terminal already exists), and queue the
    /// `cancel_execution` task for the pod driving it (skipped when no
    /// alive pod owns it). Atomic so a failure leaves the run exactly as
    /// it was and the next attempt succeeds; the old three-step shape
    /// could strip the signals and then fail, leaving a run that could
    /// neither wake nor finish. A color with no `execution_color` row
    /// (never started) has its signals stripped and nothing else.
    ///
    /// The listener still holds the stripped signals in RAM: the caller
    /// unregisters them there after the commit (`CancelWrite::removed`).
    async fn cancel_execution(
        &self,
        color: Color,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<CancelWrite>;

    /// Drop the signal row for a single-use resume token. Called
    /// when a suspension's fire is consumed (the engine has handed
    /// the value back to the waiting firing). Returns true if a row
    /// was deleted. Entry-trigger rows (`is_resume=false`) stay
    /// untouched; the deactivate path manages those separately.
    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool>;

    /// Persist a signal token (token-scoped enumeration credential).
    /// Record a freshly minted signal token. The api layer generates the token
    /// VALUE and hands the journal only its at-rest form (`token_hash` +
    /// display `recognizer` + metadata + scope vectors); the raw value is
    /// never stored. Empty scope vector = wildcard for that dimension.
    async fn mint_signal_token(&self, token: &SignalToken) -> anyhow::Result<()>;

    /// Read the full token row (scope vectors included) by the sha256 hex of
    /// the PRESENTED credential. Used by the token-scoped signal handlers.
    async fn get_signal_token(&self, token_hash: &str) -> anyhow::Result<Option<SignalToken>>;

    /// List the signal tokens owned by `tenant` (scoped in the query, so one
    /// tenant never sees another's tokens). Rows carry no secret: only the
    /// hash + recognizer + metadata.
    async fn list_signal_tokens(&self, tenant: &str) -> anyhow::Result<Vec<SignalToken>>;

    /// Delete a signal token by its id, scoped to `tenant`: only the owning
    /// tenant can revoke it. Returns true iff a row was actually removed (a
    /// wrong-tenant id matches nothing, same as a missing one, so revoke
    /// can't probe other tenants' tokens).
    async fn revoke_signal_token(&self, id: uuid::Uuid, tenant: &str) -> anyhow::Result<bool>;

    // ----- Derived views over the event log --------------------------
    //
    // An execution OUTLIVES its project on purpose: the journal is the
    // record of what ran, and it stays readable after the project is
    // removed. So everything about ownership is read from the
    // `execution_color` row, stamped in the same transaction as
    // `ExecutionStarted` and never rewritten, and NEVER re-derived
    // from the project store (which the user can delete out from under
    // it, once leaving 216 executions listed and undeletable because
    // authorization asked a table that no longer had the answer).

    /// Who an execution belongs to, read from its `execution_color`
    /// row: BOTH fields in one lookup, because they are one fact about
    /// one row and reading them apart is how they drift. `None` if the
    /// color is unknown.
    async fn execution_owner(&self, color: Color) -> anyhow::Result<Option<ExecutionOwner>>;

    /// Look up the `definition_hash` an execution was STARTED with.
    /// Resume task producers use this to stamp the resume payload,
    /// so a suspended execution always resumes against the SAME
    /// project shape it was started on (not the project row's
    /// CURRENT hash, which may have moved if the user edited and
    /// re-registered between suspend and webhook-fire). Reads the
    /// first `ExecutionStarted` event of the color. `NotFound` if
    /// the color is unknown; `Corrupt` if the row no longer decodes.
    async fn execution_definition_hash(
        &self,
        color: Color,
    ) -> anyhow::Result<ColorLookup<String>>;

    /// The LAST `limit` log lines of a color, oldest first: every
    /// event `LogEntry::from_event` projects (node log lines and the
    /// failures the journal recorded), in the order they were written
    /// (`LogEntry::tail`). The tail, not the head: a run that wrote
    /// more lines than the limit went wrong at the END, and a head
    /// would cut off exactly the failure the reader came for. A
    /// DISPLAY read, like `events_log_lossy`: a row that no longer
    /// decodes is an `error` line naming it and `weft clean`
    /// (`LogEntry::corrupt_row`), so the lines that survive still
    /// read.
    async fn logs_for(&self, color: Color, limit: u32) -> anyhow::Result<Vec<LogEntry>>;

    /// A page of `tenant`'s executions, newest first, matching `query`'s filters
    /// (project + start-time range) with limit/offset paging, plus the total
    /// matching count. Scoping is in the query (via the `execution_color` table's
    /// `tenant_id`, seeded on every start), so one tenant never sees another's
    /// executions or their count; every filter stays inside that wall.
    async fn list_executions(
        &self,
        tenant: &str,
        query: &ExecutionQuery,
    ) -> anyhow::Result<ExecutionPage>;

    /// The summary for one execution, looked up directly by color (no window
    /// scan). `None` when no `execution_started` row exists for `color`. The
    /// caller authorizes the color against the tenant separately; this is the
    /// pure read.
    async fn execution_summary(
        &self,
        color: Color,
    ) -> anyhow::Result<Option<ExecutionSummary>>;

    /// Every color belonging to `project_id` whose journal has no
    /// terminal event yet, narrowed to one `phase` when given (the
    /// activation sweep wants only the trigger-setup runs). Used by
    /// wipe / cancel_running / the activation sweep to enumerate what
    /// needs cancelling without the limit-truncation problem of
    /// `list_executions`. Single SQL roundtrip, no per-color fold.
    async fn list_non_terminal_colors_for_project(
        &self,
        project_id: &str,
        phase: Option<weft_core::context::Phase>,
    ) -> anyhow::Result<Vec<Color>>;

    /// Every color belonging to `project_id` whose journal HAS a
    /// terminal event (completed / failed / cancelled). The exact
    /// complement of `list_non_terminal_colors_for_project` over the
    /// project's known colors. `running_count` uses it to make sure a
    /// stray `pending`/`claimed` task row can never resurrect a color
    /// whose execution is already finished.
    async fn list_terminal_colors_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<std::collections::HashSet<Color>>;

    /// Every live (non-terminal, project-kind) execution of `project_id`
    /// carrying `tag`, with the sequence its tag row got, oldest tag
    /// first. The read behind `ctx.stop_tagged`; the ordering and
    /// self rules are applied on top by the pure
    /// `weft_journal::tags::select_stop_targets`.
    async fn live_tagged_executions(
        &self,
        project_id: &str,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedExecution>>;

    // ----- Signal registry (durable replacement for in-RAM tracker) ----

    /// Insert a signal registration, born with its placement (holder pod
    /// + generation) so the row is never committed with a NULL holder
    /// while a pod already holds it. Caller mints the token and resolves
    /// the placement before calling.
    async fn signal_insert(
        &self,
        sig: &SignalRegistration,
        placement: &SignalPlacement,
    ) -> anyhow::Result<()>;

    /// Look up a single signal by its token.
    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>>;

    /// Persist a kind's evolving durable state (a delta-poll cursor)
    /// onto its signal row. Two fences: the write is rejected when the
    /// row's placement generation is above `placement_generation` (a
    /// drained pod writing after the signal moved) or when the row's
    /// `kind_state_seq` is at or above `seq` (an older update
    /// arriving late must never regress a newer cursor). Returns
    /// whether a row was written.
    async fn signal_update_kind_state(
        &self,
        token: &str,
        kind_state: &Value,
        seq: i64,
        placement_generation: i64,
    ) -> anyhow::Result<bool>;

    /// Remove signals by token in one SQL statement. Returns the
    /// deleted rows so the caller can drive listener-unregister
    /// against them. Atomic: either every matching row is gone or
    /// the call fails entirely; no partial-loop leaks.
    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    /// All signals currently registered for a project.
    async fn signal_list_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    /// All signals tied to one execution color (resume signals).
    /// Used on cancel to unregister everything that was waiting.
    async fn signal_remove_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    /// All signals tied to a project. Used by deactivate sweeps
    /// after color-by-color cancel has run.
    async fn signal_remove_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    // ----- Administrative ---------------------------------------------

    /// Delete all data for a color. Called only by `weft clean`.
    async fn delete_execution(&self, color: Color) -> anyhow::Result<()>;
}

/// Durable replacement for the in-RAM `SignalTracker` row.
#[derive(Debug, Clone)]
pub struct SignalRegistration {
    pub token: String,
    pub tenant_id: String,
    pub project_id: String,
    /// `Some(color)` for resume (suspension) signals; `None` for
    /// entry signals registered during trigger setup.
    pub color: Option<Color>,
    pub node_id: String,
    pub is_resume: bool,
    /// JSON-serialized `SignalSpec`. Stored so a listener
    /// rehydrate after Pod restart can re-POST `/register` without
    /// re-running trigger-setup.
    pub spec_json: String,
    /// The connection this signal acts as (`spec.access.id`),
    /// denormalized so inbound provider pushes route account-to-
    /// signal on one indexed column. `None` for kinds without one.
    pub access_id: Option<String>,
    /// Free-form consumer label from `SignalSpec.consumer_kind`.
    /// `None` for fire-only signals (raw webhook entries) that
    /// have no enumeration consumer. The signal_token enumeration
    /// filter compares against this.
    pub consumer_kind: Option<String>,
    /// Tags copied from the registering node's `_tags` config.
    /// Used by the signal_token enumeration filter (allowed_tags
    /// overlap). Charset validated upstream by the parser.
    pub tags: Vec<String>,
    /// The trigger's delivered port values at registration time (entry
    /// signals only). Replayed onto the trigger's ports at every fire:
    /// a trigger's inputs are whatever they were at trigger setup.
    pub port_snapshot: Option<serde_json::Value>,
    /// Rendered consumer payload (form schema, decorated webhook
    /// shape, etc). Computed once at register time on the listener
    /// `/render` endpoint; cached here so consumer enumeration is
    /// a pure SQL read with no listener round-trip. Park-mode
    /// projects can serve `/signal-token/.../signals` even with the
    /// listener pod reaped because the payload is on the row.
    pub consumer_payload: Option<serde_json::Value>,
    /// `signal.surface_kind` discriminant: 'public_entry' or
    /// 'task_callback'. Read by `public_url()` to format the
    /// activate-response URLs.
    pub surface_kind: String,
    /// `signal.mount_path`. Some(path) for PublicEntry,
    /// None for TaskCallback. Empty string means root '/'.
    /// UNIQUE in DB. Read by `public_url()`.
    pub mount_path: Option<String>,
    /// `signal.auth_kind` discriminant. Stored on the row and
    /// read directly by the fire-gate SQL in `fire_public_entry`;
    /// the field is part of the struct so writes go through one
    /// shape but reads of this field happen via SQL, not struct.
    pub auth_kind: String,
    /// `signal.auth_config`. Per-auth-kind JSON (e.g. for
    /// api_key: `{header_name, value_hash}`). Plaintext NEVER
    /// stored here. Same write-through-struct / read-via-SQL
    /// pattern as `auth_kind`.
    pub auth_config: Option<Value>,
    /// Opaque per-kind state persisted at register time and read
    /// back at rehydrate time. Empty (`{}`) for most kinds. Timer
    /// uses it to remember absolute `next_fire_at_unix_ms` for
    /// After-style schedules so a listener restart doesn't reset
    /// the clock. The dispatcher treats this field as opaque
    /// JSON; only the kind's handler interprets it.
    pub kind_state: Value,
    /// The kind_state write-fence version this state was read/written
    /// at (see the `signal.kind_state_seq` column). A register that
    /// carries prior state forward passes the seq it read; a fresh
    /// token starts at 0.
    pub kind_state_seq: i64,
}

/// The placement an insert stamps on a new `signal` row: which pod holds
/// it and under what generation. Passed to `signal_insert` SEPARATELY
/// from `SignalRegistration` (the signal's identity/config) because it is
/// WRITE-time-only data: readers resolve the live holder via dedicated
/// SQL, never off the registration struct, so it does not belong on the
/// read+write `SignalRegistration`. Writing it WITH the row (rather than
/// a later UPDATE) closes the window where a committed row had a NULL
/// holder while a pod already held the signal in RAM (a fire in that
/// window would double-place).
#[derive(Debug, Clone)]
pub struct SignalPlacement {
    pub listener_pod: String,
    pub generation: i64,
}

impl SignalRegistration {
    /// Compute the public URL for this signal given a dispatcher base
    /// URL. The route depends on the surface AND, for public entries,
    /// whether it is a LIVE connection (served only at `/connect/...`,
    /// which starts an execution and hands the caller to the gateway)
    /// or a plain public fire (served at the bare `/<mount_path>`
    /// catch-all). TaskCallback → `<base>/signal/<token>`. Returns None
    /// for surface kinds with no public URL.
    pub fn public_url(&self, dispatcher_base: &str) -> Option<String> {
        let base = dispatcher_base.trim_end_matches('/');
        match self.surface_kind.as_str() {
            "public_entry" => {
                let path = self.mount_path.as_deref().unwrap_or("");
                let path = path.trim_start_matches('/');
                // Live-connection kinds (ApiEndpoint/LiveSocket) are ONLY
                // reachable through `/connect/...`; a bare-path fire does not
                // open the held connection. Everything else (a plain public
                // fire) is the bare path. The kind lives in spec_json.
                let is_live = serde_json::from_str::<weft_core::primitive::SignalSpec>(&self.spec_json)
                    .ok()
                    .and_then(|s| weft_core::signal::protocol_for_tag(&s.kind))
                    .is_some();
                let prefix = if is_live { "connect/" } else { "" };
                if path.is_empty() {
                    Some(format!("{base}/{prefix}"))
                } else {
                    Some(format!("{base}/{prefix}{path}"))
                }
            }
            "task_callback" => Some(format!("{base}/signal/{}", self.token)),
            _ => None,
        }
    }
}

// ----- Public types -----------------------------------------------

/// Who an execution belongs to: the project it ran for, and the tenant
/// that owns it. Both are stamped on the `execution_color` row when the
/// execution is born and frozen for its life (a project cannot change
/// tenant: re-registering is guarded to the same one).
///
/// The TENANT is the authority. It keys the execution's storage prefix,
/// it decides who may read or delete the execution, and unlike the
/// project row it cannot be deleted out from under the execution. The
/// project id rides along for attribution (which project's event stream
/// a replay belongs on) and may name a project that no longer exists.
/// What `Journal::cancel_execution` committed.
#[derive(Debug, Default)]
pub struct CancelWrite {
    /// The wake signals stripped, for the listener's in-RAM unregister.
    pub removed: Vec<SignalRegistration>,
    /// Whether a `cancel_execution` task was queued for an alive owner
    /// pod (false: no pod is driving this color, nothing to flag).
    pub task_enqueued: bool,
    /// Per-node cancel rows written; `None` when the journal already
    /// held a terminal and nothing was written.
    pub node_cancellations: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOwner {
    pub project_id: String,
    pub tenant: String,
}

// SYNC: ExecutionSummary <-> weavemind/website/src/routes/(app)/executions/+page.ts (Execution),
//       extension-vscode/src/sidebar/executions.ts (ExecutionSummary)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionSummary {
    pub color: Color,
    pub project_id: String,
    pub entry_node: String,
    /// One of `running`, `completed`, `failed`, `cancelled`, or
    /// `corrupt` (the row no longer decodes; `entry_node` is empty
    /// then, and the row is listed so it can be inspected via replay
    /// and deleted).
    pub status: String,
    /// What kind of run this was: a `fire` (a trigger fired or a
    /// manual run), or one of the two setup phases an activate /
    /// resync / infra start runs. The listing mixes all three, and
    /// "has my trigger fired since the change" is unanswerable without
    /// it. Copied from the `ExecutionStarted` row, or, when that row
    /// no longer decodes, from the `execution_color.phase` column the
    /// listing filtered on.
    pub phase: weft_core::context::Phase,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    /// The tags the run put on itself (`ctx.tag_execution`), in the
    /// order it claimed them. Empty for a run that never tagged.
    pub tags: Vec<String>,
}

/// The query for a page of a tenant's executions: pagination plus optional
/// filters. `project_id` narrows to one project; `started_after`/`started_before`
/// (unix seconds, inclusive/exclusive respectively) narrow by start time so the
/// website can retrieve executions around a specific date. Filtering + paging
/// happen in SQL so a tenant with a huge history never truncates blindly; every
/// filter stays inside the tenant wall.
#[derive(Debug, Clone, Default)]
pub struct ExecutionQuery {
    pub limit: u32,
    pub offset: u32,
    pub project_id: Option<String>,
    pub started_after: Option<u64>,
    pub started_before: Option<u64>,
    /// Only runs of this phase (the `execution_color.phase` column):
    /// `Fire` hides the activate / resync / infra-start runs so a
    /// listing answers "what did my triggers actually do".
    pub phase: Option<weft_core::context::Phase>,
}

/// One page of executions plus the total number matching the same filters
/// (ignoring limit/offset), so a consumer can render page controls without a
/// second count round-trip.
// SYNC: ExecutionPage <-> weavemind/website/src/routes/(app)/executions/+page.ts (ExecutionPage),
//       extension-vscode/src/sidebar/executions.ts (ExecutionPage)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionPage {
    pub executions: Vec<ExecutionSummary>,
    pub total: u64,
}

/// Token-scoped enumeration credential. Used by external consumers
/// (browser extension, future Slack bot, etc.) to fetch the subset
/// of signals they're authorized to see. Each scope vector is
/// independent; empty = wildcard.
#[derive(Debug, Clone)]
pub struct SignalToken {
    /// The token's stable identity: what list/revoke address. Never secret.
    pub id: uuid::Uuid,
    /// sha256 hex of the full token value: the ONLY secret-derived thing at
    /// rest. Lookups hash the presented credential and match this, so a DB
    /// dump exposes no usable token.
    pub token_hash: String,
    /// Display recognizer (`wft-<first-word>-…`): lets a user tell tokens
    /// apart in a list without revealing the secret.
    pub recognizer: String,
    /// The tenant that owns this token. Stamped at mint from the caller's
    /// authenticated tenant; list/revoke are scoped to it so one tenant can
    /// never see or revoke another's tokens.
    pub tenant_id: String,
    pub name: Option<String>,
    /// Allowed project ids. Empty = any project in the tenant.
    pub allowed_projects: Vec<uuid::Uuid>,
    /// Allowed signal tags. Empty = any tag (including untagged).
    /// Strict-untagged rule: when this vector is non-empty, signals
    /// with no tags do NOT match (the array overlap operator
    /// returns false against an empty signal-side array).
    pub allowed_tags: Vec<String>,
    pub created_at: u64,
}

/// One line of a run's log. A `LogLine` a node
/// wrote is one; so is every failure the journal recorded about the
/// run (a node failing, a port refusing a value, the run failing or
/// being cancelled), projected here as an `error` / `warn` line so
/// "why did this run go wrong" is answered by the log and not only
/// by the full replay. `node` names the firing for the node-level
/// ones and is `None` for a run-level line.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub at_unix: u64,
    pub level: String,
    pub node: Option<String>,
    /// The iteration the firing was in, empty at the root. Without it
    /// a loop over two hundred items gives two hundred identical
    /// lines and the reader has to open the replay anyway.
    pub frames: weft_core::LoopFrames,
    pub message: String,
    /// When the line was written, in milliseconds, the key the log is
    /// ordered by: the worker's clock for a node's line, and for a row
    /// without one (a failure the journal wrote, a line from before
    /// the clock was carried) the end of its second.
    pub written_at_ms: u64,
    /// A node's line carries its place among the firing's side
    /// effects, from the worker; the journal's own rows (a failure)
    /// have none. Breaks the tie between lines of one millisecond.
    pub seq: Option<u64>,
}

impl LogEntry {
    /// The lines as the run wrote them. A node's log line reaches the
    /// journal through a task a dispatcher pod drains later, eight at
    /// a time, so the journal's row order is the drain's, not the
    /// run's: the read sorts by the worker's clock, to the
    /// millisecond, then by the firing's own sequence. A row the
    /// journal wrote itself (a failure) has no clock of its own and
    /// sorts at the end of its second. Stable, so what neither key
    /// separates keeps its journal order.
    pub fn in_written_order(mut entries: Vec<LogEntry>) -> Vec<LogEntry> {
        entries.sort_by_key(|e| (e.written_at_ms, e.seq.unwrap_or(u64::MAX)));
        entries
    }

    /// The last `limit` lines in written order: THE `logs_for` answer,
    /// the same code for both journals, so what a mock-backed test
    /// pins is what the real read does. The cut is made after the
    /// sort, so it is the last lines the run wrote and never the last
    /// rows a pod happened to drain.
    pub fn tail(entries: Vec<LogEntry>, limit: u32) -> Vec<LogEntry> {
        let mut entries = Self::in_written_order(entries);
        if entries.len() > limit as usize {
            entries.drain(..entries.len() - limit as usize);
        }
        entries
    }

    /// The end of a second, where a row with no millisecond clock
    /// sorts: after every line written during it.
    pub fn end_of_second_ms(at_unix: u64) -> u64 {
        at_unix * 1000 + 999
    }

    /// The line a journal row that no longer decodes reads as: the
    /// decode error, which names the color and `weft clean`. Its own
    /// clock is unreadable, so it is stamped with when the row was
    /// written and sorted last, where the tail always holds it.
    pub fn corrupt_row(written_at_unix: u64, error: String) -> LogEntry {
        LogEntry {
            at_unix: written_at_unix,
            level: "error".into(),
            node: None,
            frames: Vec::new(),
            message: error,
            written_at_ms: u64::MAX,
            seq: None,
        }
    }

    /// The log line a journal event projects to, or `None` for an
    /// event that is not log-worthy (a pulse, a completion). The ONE
    /// place the journal-to-log projection lives, shared by the
    /// Postgres and mock journals so `weft logs` reads the same thing
    /// against both.
    pub fn from_event(event: &ExecEvent) -> Option<LogEntry> {
        // `KINDS` is the one gate, for both journals: the SQL read
        // fetches those rows and nothing else, and this projection
        // answers for those kinds and nothing else, so a kind the
        // match knows and the list omits is unprojected everywhere
        // rather than reaching the log from the mock alone. A kind
        // the list carries and the match does not is a bug the tests
        // pin (`every_listed_kind_projects`), never a quiet `None`.
        if !Self::KINDS.contains(&event.kind_str()) {
            return None;
        }
        Some(match event {
            ExecEvent::LogLine { node_id, frames, level, message, at_unix, at_unix_ms, seq, .. } => {
                LogEntry {
                    at_unix: *at_unix,
                    level: level.clone(),
                    // Rows written before the line carried its node read as
                    // an empty id; they are run-level lines from here on.
                    node: (!node_id.is_empty()).then(|| node_id.clone()),
                    frames: frames.clone(),
                    message: message.clone(),
                    written_at_ms: at_unix_ms.unwrap_or_else(|| Self::end_of_second_ms(*at_unix)),
                    seq: *seq,
                }
            }
            ExecEvent::NodeFailed { node_id, frames, error, at_unix, .. } => LogEntry {
                at_unix: *at_unix,
                level: "error".into(),
                node: Some(node_id.clone()),
                frames: frames.clone(),
                message: format!("node failed: {error}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            ExecEvent::NodeCancelled { node_id, frames, reason, at_unix, .. } => LogEntry {
                at_unix: *at_unix,
                level: "warn".into(),
                node: Some(node_id.clone()),
                frames: frames.clone(),
                message: format!("node cancelled: {reason}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            ExecEvent::PortTypeMismatch { node_id, frames, port, expected, actual, at_unix, .. } => {
                LogEntry {
                    at_unix: *at_unix,
                    level: "warn".into(),
                    node: Some(node_id.clone()),
                    frames: frames.clone(),
                    message: format!(
                        "port `{port}` refused a value: expected {expected}, got {actual}; \
                         the port was closed"
                    ),
                    written_at_ms: Self::end_of_second_ms(*at_unix),
                    seq: None,
                }
            }
            ExecEvent::ExecutionFailed { error, at_unix, .. } => LogEntry {
                at_unix: *at_unix,
                level: "error".into(),
                node: None,
                frames: Vec::new(),
                message: format!("execution failed: {error}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            ExecEvent::ExecutionCancelled { reason, at_unix, .. } => LogEntry {
                at_unix: *at_unix,
                level: "warn".into(),
                node: None,
                frames: Vec::new(),
                message: format!("execution cancelled: {reason}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            other => unreachable!(
                "`{}` is in LogEntry::KINDS but the projection has no arm for it",
                other.kind_str()
            ),
        })
    }

    /// The `exec_event.kind` values the log is made of: what the SQL
    /// read fetches, and what `from_event` answers for.
    pub const KINDS: &'static [&'static str] = &[
        "log_line",
        "node_failed",
        "node_cancelled",
        "port_type_mismatch",
        "execution_failed",
        "execution_cancelled",
    ];
}

#[cfg(test)]
mod log_entry_tests {
    use super::LogEntry;
    use weft_journal::ExecEvent;

    fn sample_events() -> Vec<ExecEvent> {
        let color = weft_core::Color::new_v4();
        vec![
            ExecEvent::LogLine {
                color,
                node_id: "greet".into(),
                frames: Default::default(),
                level: "info".into(),
                message: "hi".into(),
                at_unix: 1,
                at_unix_ms: Some(1_000),
                seq: Some(0),
            },
            ExecEvent::NodeFailed {
                color,
                node_id: "llm".into(),
                frames: Default::default(),
                error: "boom".into(),
                closure_emissions: Vec::new(),
                at_unix: 2,
            },
            ExecEvent::NodeCancelled {
                color,
                node_id: "llm".into(),
                frames: Default::default(),
                reason: "stopped".into(),
                closure_emissions: Vec::new(),
                at_unix: 3,
            },
            ExecEvent::PortTypeMismatch {
                color,
                node_id: "bridge".into(),
                frames: Default::default(),
                port: "jid".into(),
                expected: "String".into(),
                actual: "Null".into(),
                at_unix: 4,
            },
            ExecEvent::ExecutionFailed { color, error: "stuck".into(), at_unix: 5 },
            ExecEvent::ExecutionCancelled {
                color,
                reason: "by hand".into(),
                cause: None,
                at_unix: 6,
            },
            ExecEvent::ExecutionCompleted { color, outputs: serde_json::Value::Null, at_unix: 7 },
            ExecEvent::ExecutionTagged { color, tags: vec!["t".into()], at_unix: 8 },
        ]
    }

    /// Every failure the journal records about a run reaches the log as
    /// an error / warn line naming its node, so `weft logs` answers "why
    /// did this go wrong" without the full replay.
    #[test]
    fn failures_project_to_log_lines() {
        let events = sample_events();
        let lines: Vec<LogEntry> = events.iter().filter_map(LogEntry::from_event).collect();
        assert_eq!(lines.len(), 6, "six log-worthy events: {lines:?}");
        assert_eq!((lines[0].level.as_str(), lines[0].node.as_deref()), ("info", Some("greet")));
        assert_eq!((lines[1].level.as_str(), lines[1].node.as_deref()), ("error", Some("llm")));
        assert!(lines[1].message.contains("boom"), "{}", lines[1].message);
        assert_eq!(lines[2].level, "warn");
        assert_eq!(lines[3].node.as_deref(), Some("bridge"));
        assert!(lines[3].message.contains("jid"), "{}", lines[3].message);
        assert_eq!((lines[4].level.as_str(), lines[4].node.as_deref()), ("error", None));
        assert!(lines[5].message.contains("by hand"), "{}", lines[5].message);
    }

    /// Every kind in `KINDS` has a sample here and projects: the
    /// projection panics on a listed kind it has no arm for, and this
    /// is the test that reaches every arm. The `kind` column is the
    /// serde tag, so the samples are serialized to read it the way
    /// the row was written.
    #[test]
    fn every_listed_kind_projects() {
        let samples = sample_events();
        for kind in LogEntry::KINDS {
            let event = samples
                .iter()
                .find(|e| serde_json::to_value(e).unwrap()["kind"] == *kind)
                .unwrap_or_else(|| panic!("no sample event of kind `{kind}`; add one"));
            assert!(LogEntry::from_event(event).is_some(), "`{kind}` is listed but does not project");
        }
        for event in samples {
            let kind = serde_json::to_value(&event).unwrap()["kind"].as_str().unwrap().to_string();
            assert_eq!(
                LogEntry::KINDS.contains(&kind.as_str()),
                LogEntry::from_event(&event).is_some(),
                "kind `{kind}` is listed and projected inconsistently"
            );
        }
    }

    /// Nodes' lines drain through tasks in whatever order eight
    /// pickers land them, so the journal's row order is not the
    /// run's; the read puts them back by the worker's millisecond
    /// clock, across nodes, with a failure the journal wrote itself
    /// at the end of its second and a line from before the clock was
    /// carried likewise.
    #[test]
    fn lines_read_in_the_order_they_were_written() {
        let color = weft_core::Color::new_v4();
        let line = |node: &str, seq: u64, at_ms: Option<u64>, at_unix: u64| ExecEvent::LogLine {
            color,
            node_id: node.into(),
            frames: Default::default(),
            level: "info".into(),
            message: format!("{node} {seq}"),
            at_unix,
            at_unix_ms: at_ms,
            seq: Some(seq),
        };
        let failed = ExecEvent::NodeFailed {
            color,
            node_id: "a".into(),
            frames: Default::default(),
            error: "boom".into(),
            closure_emissions: Vec::new(),
            at_unix: 10,
        };
        // Journal (drain) order: the failure first, then a's second
        // line, b's line, a's first line, and an old-style line of the
        // second before, drained last.
        let journal = [
            failed,
            line("a", 1, Some(10_900), 10),
            line("b", 0, Some(10_500), 10),
            line("a", 0, Some(10_100), 10),
            line("c", 0, None, 9),
        ];
        let read = LogEntry::in_written_order(journal.iter().filter_map(LogEntry::from_event).collect());
        let messages: Vec<&str> = read.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, ["c 0", "a 0", "b 0", "a 1", "node failed: boom"]);
        assert_eq!(read[0].written_at_ms, LogEntry::end_of_second_ms(9));
    }
}

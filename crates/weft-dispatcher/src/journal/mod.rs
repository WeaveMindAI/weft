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
/// pollers that would loop on an `Err` skip instead).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorLookup<T> {
    Found(T),
    NotFound,
    Corrupt,
}

impl<T> ColorLookup<T> {
    /// Collapse to `Option` when the caller treats an unknown and a
    /// corrupt color identically (e.g. "skip this row").
    pub fn found(self) -> Option<T> {
        match self {
            Self::Found(t) => Some(t),
            Self::NotFound | Self::Corrupt => None,
        }
    }
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

    /// Full ordered event log for a color.
    async fn events_log(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>>;

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
        reason: &str,
    ) -> anyhow::Result<weft_task_store::tasks::SetupFailureOutcome>;

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

    /// Look up which project a color belongs to. Walks the event
    /// log for the first `ExecutionStarted` event. `NotFound` if
    /// the color is unknown; `Corrupt` if the row no longer decodes.
    async fn execution_project(&self, color: Color) -> anyhow::Result<ColorLookup<String>>;

    /// The tenant a color's execution was STARTED under, from its
    /// `execution_color` row (stamped at start, frozen for the run's life).
    /// This is the authoritative owner for keying the execution's storage,
    /// and it OUTLIVES a project deletion (the row is journal-side, not the
    /// mutable project store), so a terminate sweep can resolve it even for a
    /// since-deleted project. `NotFound` if the color is unknown.
    async fn execution_tenant(&self, color: Color) -> anyhow::Result<ColorLookup<String>>;

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

    /// Log lines for a color, oldest first. Folded from
    /// `ExecEvent::LogLine` events.
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
    /// terminal event yet. Used by wipe / cancel_running to enumerate
    /// what needs cancelling without the limit-truncation problem of
    /// `list_executions`. Single SQL roundtrip, no per-color fold.
    async fn list_non_terminal_colors_for_project(
        &self,
        project_id: &str,
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
    /// Free-form consumer label from `SignalSpec.consumer_kind`.
    /// `None` for fire-only signals (raw webhook entries) that
    /// have no enumeration consumer. The signal_token enumeration
    /// filter compares against this.
    pub consumer_kind: Option<String>,
    /// Tags copied from the registering node's `_tags` config.
    /// Used by the signal_token enumeration filter (allowed_tags
    /// overlap). Charset validated upstream by the parser.
    pub tags: Vec<String>,
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

// SYNC: ExecutionSummary <-> weavemind/website/src/routes/(app)/executions/+page.ts (Execution),
//       extension-vscode/src/sidebar/executions.ts (ExecutionSummary)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionSummary {
    pub color: Color,
    pub project_id: String,
    pub entry_node: String,
    pub status: String,
    pub started_at: u64,
    pub completed_at: Option<u64>,
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

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub at_unix: u64,
    pub level: String,
    pub message: String,
}

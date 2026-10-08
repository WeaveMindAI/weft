//! The dispatcher's side of the run record. A run's whole life is its
//! `run` row and its record (`run_log`, `weft_journal::record`): the
//! worker that drives a run writes both, and the dispatcher reads them
//! (the listing, a summary, the inspector's replay, logs) and writes only
//! a run nobody drives (a run it queues, the ending of a parked or queued
//! run it cancels, an answer to a parked run).
//!
//! Beside the record, the tables of what is not a run: signals (what wakes
//! a trigger or answers a wait), signal tokens, trigger setups and their
//! bakes.

pub mod postgres;

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake;
#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeJournal;

use weft_core::signal_token::TokenKind;
use weft_journal::ExecEvent;

use async_trait::async_trait;
use serde_json::Value;

use weft_core::program::{ExecutionPage, ExecutionSummary};
use weft_core::ExecutionId;

/// A successful setup, independent of whether its listeners are armed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TriggerBake {
    pub project_id: uuid::Uuid,
    /// Whose triggers these are: the setup run's instance, `None` for the
    /// shared ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
    pub source_version: String,
    pub program: weft_core::project::hash::ProgramIdentity,
    pub execution_id: ExecutionId,
    pub captured: std::collections::BTreeMap<String, TriggerCapture>,
    /// The triggers this setup set out to capture (spelled). A setup of
    /// some triggers replaces what an earlier one captured for THOSE (one
    /// it skipped behind a closed gate loses its old capture) and keeps
    /// the rest. Stamped by whoever ran the setup, which is who knows.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub targets: std::collections::BTreeSet<String>,
    pub at_unix: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TriggerCapture {
    pub spec: weft_core::primitive::SignalSpec,
    pub ports: Value,
}

impl TriggerBake {
    /// This bake with `newer`'s captures laid over it: a setup of some
    /// of the triggers refreshes those, and the others keep what an
    /// earlier setup of the same code captured. The rest of the bake
    /// (which setup, when, which source) is `newer`'s.
    pub fn refreshed_by(self, newer: &TriggerBake) -> TriggerBake {
        let mut captured: std::collections::BTreeMap<String, TriggerCapture> = self
            .captured
            .into_iter()
            .filter(|(trigger, _)| !newer.targets.contains(trigger))
            .collect();
        captured.extend(newer.captured.iter().map(|(k, v)| (k.clone(), v.clone())));
        let mut targets = self.targets;
        targets.extend(newer.targets.iter().cloned());
        TriggerBake { captured, targets, ..newer.clone() }
    }

    pub fn summary(&self) -> weft_core::run_spec::BakeSummary {
        weft_core::run_spec::BakeSummary { program: self.program.clone(), captured: self.captured.keys().cloned().collect(),
            execution_id: self.execution_id, at_unix: self.at_unix }
    }

    /// Only a successful setup can replace saved settings. A closed group gate
    /// leaves its trigger absent from this capture, including on a refresh.
    /// `program` is the setup's program identity, read from the project's
    /// stored code for the binary its birth names
    /// (`ProjectStore::program_identity`): the birth carries only the two
    /// hashes, which must be this identity's.
    pub fn from_events(events: &[ExecEvent], program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<Option<Self>> {
        let Some(ExecEvent::ExecutionStarted { execution_id, project_id, binary_hash, definition_hash, source_version: Some(source_version),
            phase: weft_core::context::Phase::TriggerSetup, instance, .. }) = events.first() else {
            anyhow::bail!("trigger setup has no original program or source identity");
        };
        anyhow::ensure!(definition_hash.as_ref() == Some(&program.definition_hash) && binary_hash.as_ref() == Some(&program.binary_hash),
            "trigger setup {execution_id} has conflicting program identities");
        anyhow::ensure!(events.iter().all(|event| event.execution_id() == *execution_id),
            "trigger setup {execution_id} contains another run's history");
        let Some(terminal) = events.iter().find(|event| event.is_execution_terminal()) else {
            anyhow::bail!("trigger setup {execution_id} has not finished");
        };
        let ExecEvent::ExecutionCompleted { at_unix, .. } = terminal else { return Ok(None); };
        let mut captured = std::collections::BTreeMap::new();
        for event in events.iter().take_while(|event| !event.is_execution_terminal()) {
            if let ExecEvent::TriggerCaptured { execution_id: captured_execution_id, node_id, spec, port_snapshot, .. } = event {
                anyhow::ensure!(captured_execution_id == execution_id && port_snapshot.is_object(), "invalid trigger capture in setup {execution_id}");
                weft_core::signal::validate_spec(spec).map_err(anyhow::Error::msg)?;
                anyhow::ensure!(captured.insert(node_id.clone(), TriggerCapture {
                    spec: spec.clone(), ports: port_snapshot.clone(),
                }).is_none(), "trigger '{node_id}' captured twice in setup {execution_id}");
            }
        }
        Ok(Some(Self { project_id: *project_id, instance: instance.clone(), program: program.clone(), execution_id: *execution_id,
            source_version: source_version.clone(),
            captured, targets: Default::default(), at_unix: *at_unix }))
    }
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn is_trigger_setup_pending(&self, execution_id: ExecutionId) -> anyhow::Result<bool>;

    /// Publish one complete setup and release its birth-time ownership atomically.
    /// A failed/cancelled setup releases ownership without replacing any bake.
    async fn finish_trigger_setup(&self, execution_id: ExecutionId, bake: Option<&TriggerBake>) -> anyhow::Result<()>;

    /// The bakes of one owner's triggers: the shared ones for `None`, an
    /// instance's for `Some`.
    async fn trigger_bakes(&self, project_id: uuid::Uuid, instance: Option<&weft_core::instance::InstanceId>) -> anyhow::Result<Vec<TriggerBake>>;

    // ----- The record -------------------------------------------------

    /// Write `events` into the record of `execution_id`, a run nobody
    /// drives, after its last row, and move it as `then` says (an ending
    /// in `events` ends it). Its row is locked for the write, the one
    /// serialization point of a run every writer but its owner goes
    /// through. Nothing is written to a run a worker drives (its owner
    /// writes its record), one that ended, or one there is no row of
    /// (`weft_journal::record::Appended`).
    async fn append(
        &self,
        execution_id: ExecutionId,
        events: &[ExecEvent],
        then: weft_journal::record::Then,
    ) -> anyhow::Result<weft_journal::record::Appended>;

    /// A run's whole record for DISPLAY: a row that does not decode comes
    /// back as its error text instead of failing the read, so the
    /// inspector renders what exists and names the rows it cannot. The
    /// one required read; [`Journal::events_log`] is derived from it.
    async fn events_log_lossy(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<(Vec<crate::events::IdentifiedEvent<ExecEvent>>, Vec<String>)>;

    /// The same record for STATE-REBUILDING (a cancel's per-node rows, a
    /// setup's bake): a row that no longer decodes fails the WHOLE read,
    /// naming the run and `weft clean`, because a fold over a partial
    /// record rebuilds a state that never existed.
    async fn events_log(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<ExecEvent>> {
        let (events, bad) = self.events_log_lossy(execution_id).await?;
        match bad.into_iter().next() {
            Some(reason) => Err(anyhow::Error::msg(reason)),
            None => Ok(events.into_iter().map(|record| record.event).collect()),
        }
    }

    /// Queue a run the dispatcher starts (`weft run`, a setup run) for a
    /// worker to claim: its row, its record's first row, its selection and
    /// its version count, and delivery woken, in one transaction. A trigger
    /// setup is recorded as in flight, and one an activation asked for
    /// (`for_activation`: the activation is the setup's own run) is queued
    /// only while that activation still owns its rows. `false` when the run
    /// is already on record (a retried start, by its id).
    async fn queue_run(&self, queued: weft_journal::record::Queued<'_>, for_activation: bool) -> anyhow::Result<bool>;

    /// THE dispatcher-side cancel of a run, in ONE transaction: strip its
    /// wake signals (the parked form, the timer, the webhook, so nothing
    /// can revive it), then end it. A run a worker drives is asked to stop
    /// (`run.cancel_requested`, announced to its worker on `weft_cancel`
    /// in the same transaction), and its worker writes its ending. A run
    /// nobody drives (parked, queued) is ended here: `NodeCancelled` per
    /// open node, then `ExecutionCancelled`, after its last row. A run that
    /// ended already, or that has no row, only loses its signals.
    ///
    /// The listener still holds the stripped signals in RAM: the caller
    /// unregisters them there after the commit (`CancelWrite::removed`).
    /// `program` is the run's definition (the per-node cancels come off
    /// the fold); `None` for a run with no program, which has no nodes to
    /// cancel.
    async fn cancel_execution(
        &self,
        execution_id: ExecutionId,
        program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<CancelWrite>;

    /// Let go of `execution_id` when it is running and its owner's lease
    /// ran out before `lapsed_before` (unix seconds): its owner is cleared
    /// and its epoch raised, so a late batch from the old owner is
    /// refused. A durable run is queued again, carried on from its record
    /// by the next worker that claims it; a fast run lived in its worker's
    /// memory and ends, cancelled for `CancelCause::fast_run_lost`, its
    /// per-node cancels folded with `program`. All under the run's row
    /// lock, in one transaction.
    async fn let_go_of_lost(
        &self,
        execution_id: ExecutionId,
        lapsed_before: i64,
        program: Option<&weft_core::ProjectDefinition>,
    ) -> anyhow::Result<Lost>;

    /// Answer the wait `token` with `value` (`answer_in`, on a
    /// transaction of its own).
    async fn answer(&self, token: &str, value: &serde_json::Value) -> anyhow::Result<Answered>;

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

    /// Record `token` (an operator key) only when its tenant holds no
    /// operator key at all, in one statement, so replicas booting at
    /// once record it once. The install's bootstrap key: re-seeded after
    /// every operator key is revoked, never beside a live one. Returns
    /// whether it was recorded.
    async fn seed_operator_token(&self, token: &SignalToken) -> anyhow::Result<bool>;

    // ----- What the run rows say -----------------------------------------
    //
    // A run OUTLIVES its project on purpose: the record is the record of
    // what ran, and it stays readable after the project is removed. So
    // everything about ownership is read from the `run` row, written when
    // the run is born and never rewritten, and NEVER re-derived from the
    // project store (which the user can delete out from under it).

    /// Who a run belongs to, read from its `run` row: every field in one
    /// lookup, because they are one fact about one row and reading them
    /// apart is how they drift. `None` if the run is unknown.
    async fn execution_owner(&self, execution_id: ExecutionId) -> anyhow::Result<Option<ExecutionOwner>>;

    /// Every program version the runs still on record were started
    /// against, for one project. What a project removal (and the last
    /// `weft clean` after it) reads to know which recorded programs are
    /// still needed: the record outlives the project, so the programs its
    /// runs point at have to as well, or the rows are there and
    /// unreadable.
    async fn definition_hashes_in_use(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<String>>;

    /// The LAST `limit` log lines of a run, oldest first: every event
    /// `LogEntry::from_event` projects (node log lines and the failures
    /// its record holds), in the order they were written
    /// (`LogEntry::tail`). The tail, not the head: a run that wrote more
    /// lines than the limit went wrong at the END, and a head would cut
    /// off exactly the failure the reader came for. A DISPLAY read, like
    /// `events_log_lossy`: a row that no longer decodes is an `error` line
    /// naming it and `weft clean` (`LogEntry::corrupt_row`), so the lines
    /// that survive still read.
    async fn logs_for(&self, execution_id: ExecutionId, limit: u32) -> anyhow::Result<Vec<LogEntry>>;

    /// A page of `tenant`'s runs, newest first, matching `query`'s filters
    /// with limit/offset paging, plus the total matching count. Scoping is
    /// in the query (`run.tenant_id`), so one tenant never sees another's
    /// runs or their count; every filter stays inside that wall.
    async fn list_executions(
        &self,
        tenant: &str,
        query: &ExecutionQuery,
    ) -> anyhow::Result<ExecutionPage>;

    /// The summary of one run, from its row. `None` for an unknown run.
    /// The caller authorizes the run against the tenant separately; this
    /// is the pure read.
    async fn execution_summary(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Option<ExecutionSummary>>;

    /// Every run of `project_id` still on record.
    async fn execution_ids_for_project(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<ExecutionId>>;

    /// The summaries of `execution_ids`, by id, in ONE read: what `weft
    /// tree` and the editor's version sidebar read a status per run started
    /// by hand from. A run not on record is left out.
    async fn execution_summaries(
        &self,
        execution_ids: &[ExecutionId],
    ) -> anyhow::Result<std::collections::HashMap<ExecutionId, ExecutionSummary>>;

    /// Every execution of `tenant`'s that starts with `prefix`
    /// (the first characters of a uuid, as a person types them). At most
    /// two come back: the caller only needs to know whether the prefix
    /// names one execution, none, or several. Node-test executions never
    /// match, the way they never list.
    async fn execution_ids_with_prefix(&self, tenant: &str, prefix: &str) -> anyhow::Result<Vec<ExecutionId>>;

    /// Every run of `project_id` going right now (queued for a worker, or
    /// being driven), each with its phase (a run, or the setup an `infra
    /// start` or an activation runs, which the editor shows as that verb
    /// working rather than as a run to stop). A run parked on a wait is not
    /// going.
    ///
    /// Oldest first, so the LAST one is the most recently started. The
    /// editor's action bar follows "the latest run" and the wire
    /// carries no other way to tell which that is, so the order is part
    /// of the contract rather than an accident of the query. Ties break
    /// on the execution, so the answer is stable across calls.
    async fn going_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<(ExecutionId, weft_core::context::Phase)>>;

    /// Every run of `project_id` that ended or is parked on a wait: the
    /// runs that write nothing until something answers them.
    async fn settled_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<std::collections::HashSet<ExecutionId>>;

    /// Every live run of `project_id` carrying `tag`, with the sequence its
    /// tag row got, oldest tag first. The read behind `ctx.stop_tagged`; the ordering and
    /// self rules are applied on top by the pure
    /// `weft_journal::tags::select_stop_targets`.
    async fn live_tagged_executions(
        &self,
        project_id: uuid::Uuid,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedExecution>>;

    // ----- Signal registry (durable replacement for in-RAM tracker) ----

    /// Insert a signal registration (or refresh an entry's row in place
    /// on reactivate). The caller mints the token. A compare-and-set on
    /// the kind state's version: see [`SignalWrite`].
    async fn signal_insert(&self, sig: &SignalRegistration) -> anyhow::Result<SignalWrite>;

    /// Write `sig` back over an existing row: the same columns a
    /// registration's refresh of a replaced row writes (its spec,
    /// routing, program, source version and kind state; never the row's
    /// identity), a compare-and-set like [`Self::signal_insert`]: it
    /// lands only while the row is still at `sig.kind_state_seq` and
    /// moves it one past. The undo of a registration that replaced a row.
    /// Unlike `signal_insert` it checks no activation (the one that armed
    /// `sig` may be long over), but it holds the source version `sig`
    /// names the same way, and a version removed since is an error.
    /// `StateMoved` when a claim moved the row; an error when the row is
    /// gone.
    async fn signal_restore(&self, sig: &SignalRegistration) -> anyhow::Result<SignalWrite>;

    /// Look up a single signal by its token.
    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>>;

    /// The ENTRY registered at the place `node` spells in `project_id`
    /// (`door`, or `one.door` inside the file the site `one` includes)
    /// for `instance`'s copy (`None`: the shared one), or `None` when
    /// nothing is armed there. One row at most: entries are unique per
    /// (project, place, instance) (`idx_signal_entry_node`). A resume row
    /// of the same node is another registration entirely and is never
    /// this.
    async fn signal_entry_at(
        &self,
        project_id: uuid::Uuid,
        node: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<Option<SignalRegistration>>;


    /// Remove signals by token in one SQL statement. Returns the
    /// deleted rows so the caller can drive listener-unregister
    /// against them. Atomic: either every matching row is gone or
    /// the call fails entirely; no partial-loop leaks.
    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    /// Delete the wait `token` of run `execution_id`, given up by its run,
    /// and return it for the listener to let go of. A token that is no
    /// wait of that run (an entry trigger, another run's wait, a wait
    /// already answered) is left alone: `None`.
    async fn signal_withdraw(&self, execution_id: ExecutionId, token: &str) -> anyhow::Result<Option<SignalRegistration>>;

    /// The RESUME registrations of one execution: what that execution is
    /// parked on.
    ///
    /// Every poll of a parked run and every `weft wake` asks this. Asked
    /// as "every signal of the project, then filter", it read every
    /// registration the project has, with its kind state, its port
    /// snapshot and its consumer payload, to answer with three fields.
    async fn signal_list_for_execution_id(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>>;

    /// All signals currently registered for a project.
    async fn signal_list_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    /// All signals tied to one execution (resume signals).
    /// Used on cancel to unregister everything that was waiting.
    async fn signal_remove_for_execution_id(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    /// All signals tied to a project. Used by deactivate sweeps
    /// after execution-by-execution cancel has run.
    async fn signal_remove_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>>;

    // ----- Administrative ---------------------------------------------

    /// Delete all data of a run. Called only by `weft clean`.
    ///
    /// Answers the resume signals the run was parked on, which went
    /// with it: a listener process still holds each one in RAM and keeps
    /// answering for a run that no longer exists until the caller
    /// unregisters it there (`unregister_many`), the way a cancel does.
    async fn delete_execution(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>>;

    /// Delete all data of every run of a project, and say how many went.
    /// Called by `weft rm`.
    ///
    /// Removing a project erases its history rather than orphaning it.
    /// Keeping the runs sounded kind and was not: the project row that
    /// named them is gone, so `weft rm` can never run again for them
    /// and `weft clean` has no project to clean, which left rows nobody
    /// could reach or free. What survives a removal is what the person
    /// still has: their files on disk.
    async fn delete_project_executions(&self, project_id: uuid::Uuid) -> anyhow::Result<u64>;

    /// Every project id that still has runs while the project itself is
    /// gone. What the reaper sweeps.
    ///
    /// The erase at removal is best-effort, because a project is
    /// already gone by then and failing the answer would say the
    /// removal did not happen when it did. Nothing else can reach those
    /// rows afterwards (`weft rm` refuses a project it cannot find, and
    /// `weft clean` needs a project), so without this one transient
    /// failure would keep them for good.
    async fn projects_with_orphan_executions(&self) -> anyhow::Result<Vec<uuid::Uuid>>;

    /// Erase up to `limit` ended runs whose time to be kept ran out before
    /// `now` (unix seconds), oldest first, with everything of theirs (the
    /// same one list a `weft clean` erases). Answers how many, and the
    /// resume signals that went with them for the listener to let go of.
    /// A parked or queued run has no end, so it is never erased.
    async fn erase_expired(&self, now: i64, limit: i64) -> anyhow::Result<(usize, Vec<SignalRegistration>)>;
}

/// Durable replacement for the in-RAM `SignalTracker` row.
#[derive(Debug, Clone)]
pub struct SignalRegistration {
    /// Whose signal: the instance whose copy of a per-instance trigger this
    /// is, or whose run waits on it. `None` for the program's shared ones.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// The trigger whose activation gates this signal (with `instance`):
    /// an entry signal's own trigger, or the trigger that fired the run a
    /// wait belongs to. `None` for a wait of a run started by hand, which
    /// no activation governs, so it is always live.
    pub activation_trigger: Option<String>,
    pub source_version: Option<String>,
    /// Setup whose completed capture armed this entry; absent for suspensions.
    pub setup_execution_id: Option<ExecutionId>,
    /// The code armed with these settings. Rebaking cannot retarget a listener.
    pub program: Option<weft_core::project::hash::ProgramIdentity>,
    pub token: String,
    pub tenant_id: String,
    pub project_id: uuid::Uuid,
    /// `Some(execution_id)` for resume (suspension) signals; `None` for
    /// entry signals registered during trigger setup.
    pub execution_id: Option<ExecutionId>,
    /// The PLACE this registration was made at, spelled the way a
    /// person writes the node: `door`, or `one.door` for the `door`
    /// inside the file the site `one` includes. A file called from two
    /// places registers twice, and the spelling is what tells the two
    /// rows apart (`task_kinds::register_signal::registered_place`).
    /// The compiled id behind it is never stored here, so nothing that
    /// reads a row has to translate before it names the node to a
    /// person or looks one up by the name a person gave.
    pub node_id: String,
    pub is_resume: bool,
    /// JSON-serialized `SignalSpec`. Stored so a listener brings the
    /// signal back up from its row after a restart without re-running
    /// trigger-setup.
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
    /// shape, etc). Computed once at register time by the listener's
    /// `/prepare`; cached here so consumer enumeration is
    /// a pure SQL read with no listener round-trip. Park-mode
    /// projects can serve `/signal-token/.../signals` even with the
    /// listener process reaped because the payload is on the row.
    pub consumer_payload: Option<serde_json::Value>,
    /// `signal.surface_kind` discriminant: 'public_entry' or
    /// 'task_callback'. Read by `public_url()`, which formats both the
    /// activate-response URLs and the address a trigger's display
    /// shows; a change here moves both.
    pub surface_kind: String,
    /// `signal.mount_path`. Some(pattern) for PublicEntry (the
    /// tenant-prefixed route pattern, `/<tenant>/chat/{room}`), None
    /// for TaskCallback. Read by `public_url()`.
    pub mount_path: Option<String>,
    /// `signal.mount_methods`. The HTTP methods a PublicEntry serves,
    /// uppercase; empty = any method, and empty for every other
    /// surface.
    pub mount_methods: Vec<String>,
    /// `signal.auth_kind` discriminant. Stored on the row and
    /// read directly by the fire-gate SQL in `fire_public_entry`;
    /// the field is part of the struct so writes go through one
    /// shape but reads of this field happen via SQL, not struct.
    pub auth_kind: String,
    /// `signal.auth_config`. Per-auth-kind JSON: `{access_id,
    /// service}` for `connection` (the connection the broker checks
    /// the caller against), null for `none`. No secret is ever
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
    /// The kind_state version (the `signal.kind_state_seq` column). On
    /// an insert, the version the registration read the row at (0 for a
    /// token with no row yet): the insert lands only while the row is
    /// still there and moves it one past, so every claim a wake took at
    /// the old version fails. Read back, the row's current version.
    pub kind_state_seq: i64,
    /// Whether the signal keeps a connection open between fires, as the
    /// listener's `/prepare` decided: a holder holds it.
    pub holds: bool,
}

/// What a [`Journal::signal_insert`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalWrite {
    /// The row is written, its kind state at a new version.
    Written,
    /// The row's kind state moved past the version the registration read
    /// it at (a wake claimed a moment in between), so nothing was
    /// written: read the row again and compute the state again from it.
    StateMoved,
}


impl SignalRegistration {
    /// The signal's spec, read off `spec_json`.
    pub fn spec(&self) -> anyhow::Result<weft_core::primitive::SignalSpec> {
        serde_json::from_str(&self.spec_json).map_err(|e| anyhow::anyhow!("signal {} spec: {e}", self.token))
    }

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
                // Live-connection kinds (Route/Socket) are ONLY reachable
                // through `/connect/...`; a bare-path fire does not
                // open the held connection. Everything else (a plain public
                // fire) is the bare path. The kind lives in spec_json.
                //
                // A row whose spec does not parse gets NO url rather than
                // a bare-path guess. The two addresses look equally real
                // and only one of them works, so handing out the wrong
                // one sends somebody to debug a route that was answering
                // all along at the address they were not given. No url at
                // least says "I cannot tell you", and the parse failure
                // is a broken row, which is worth a line in the log.
                let spec = serde_json::from_str::<weft_core::primitive::SignalSpec>(&self.spec_json);
                let is_live = match &spec {
                    Ok(spec) => weft_core::signal::caller_protocol(&spec.kind).is_some(),
                    Err(error) => {
                        tracing::error!(
                            target: "weft_dispatcher::journal",
                            token = %self.token,
                            %error,
                            "signal row has an unreadable spec; its public address cannot be told"
                        );
                        return None;
                    }
                };
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

/// Who a run belongs to: the project it ran for, and the tenant that owns
/// it. Both are written on the `run` row when the run is born and frozen
/// for its life (a project cannot change tenant: re-registering is guarded
/// to the same one).
///
/// The TENANT is the authority. It keys the run's storage prefix, it
/// decides who may read or delete the run, and unlike the project row it
/// cannot be deleted out from under the run. The
/// project id rides along for attribution (which project's event stream
/// a replay belongs on) and may name a project that no longer exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOwner {
    pub project_id: uuid::Uuid,
    pub tenant: String,
    /// Who the run is for (`run.instance_id`).
    pub instance: Option<weft_core::instance::InstanceId>,
    /// The trigger that fired the run (`run.fired_by`).
    pub fired_by: Option<String>,
    /// What the run is for (`run.phase`): an infra setup's ending saves
    /// its infra's baked outputs.
    pub phase: weft_core::context::Phase,
    /// The program it was STARTED with (`run.definition_hash`), so it is
    /// always read against the shape it ran on (not the project's CURRENT
    /// hash, which may have moved since). `None` for a run of no program
    /// (a node test).
    pub definition_hash: Option<String>,
    /// The worker binary it runs on (`run.binary_hash`); `None` for a run
    /// of no program.
    pub binary_hash: Option<String>,
    /// The version of the project's source it ran (`run.source_version`).
    pub source_version: Option<String>,
}

/// What became of an answer to a wait (`Journal::answer`).
#[derive(Debug)]
pub enum Answered {
    /// It reached its run: written into its record (the run is queued to
    /// carry on), or handed to the worker that drives it. The wait's
    /// signal is gone; the listener still holds it in memory.
    Reached { consumed: SignalRegistration },
    /// The run had ended: the wait's signal is gone, nothing was written.
    RunEnded { consumed: SignalRegistration },
    /// No such wait: it was answered already, or never existed.
    Gone,
}

/// What `Journal::let_go_of_lost` did with a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lost {
    /// Its owner is alive (or it is not running any more): left as it is.
    NotLost,
    /// A durable run, queued again.
    Requeued,
    /// A fast run, ended.
    Ended,
}

/// What `Journal::cancel_execution` committed.
#[derive(Debug, Default)]
pub struct CancelWrite {
    /// The wake signals stripped, for the listener's in-RAM unregister.
    pub removed: Vec<SignalRegistration>,
    /// A worker drives the run: it was asked to stop, and writes the
    /// run's ending itself.
    pub requested: bool,
    /// The run nobody drove was ended here, with this many per-node cancel
    /// rows; `None` when nothing was ended here.
    pub node_cancellations: Option<usize>,
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
    pub project_id: Option<uuid::Uuid>,
    pub started_after: Option<u64>,
    pub started_before: Option<u64>,
    /// Only runs of this phase (the `execution.phase` column):
    /// `Fire` hides the activate / resync / infra-start runs so a
    /// listing answers "what did my triggers actually do".
    pub phase: Option<weft_core::context::Phase>,
    /// Only runs whose ENTRY NODE is this one: the node whose firing
    /// started the run, as the `execution_started` event recorded it.
    /// The filter that lets a person find their own run in a project
    /// that is answering thousands: everything else about a run
    /// (project, phase, when) is shared by every run beside it.
    pub entry_node: Option<String>,
    /// Only runs standing this way ([`weft_core::program::RunStatus`]).
    /// "Which of mine broke" in one question.
    pub status: Option<weft_core::program::RunStatus>,
    /// Only runs for this instance.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// Only runs carrying this tag (`ctx.tag_execution`).
    pub tag: Option<String>,
    /// Only finished runs in which this node fired (it started at least
    /// once), spelled the way the record names it, once their search entry
    /// is built (`crate::search_index`).
    pub node: Option<String>,
    /// Only finished runs whose recorded values carry every word of this
    /// (a quoted phrase as written), once their search entry is built
    /// (`crate::search_index`).
    pub search: Option<String>,
    /// Keyset cursor: only runs strictly after this `(started_at,
    /// execution)` in the listing's order (newest first, then execution
    /// descending). A walk that hands each page's last run back here
    /// reaches every run exactly once, whatever it deletes or changes
    /// behind itself. `total` ignores it.
    pub below: Option<(u64, ExecutionId)>,
}

/// A hashed bearer credential. A `Caller` token is used by external
/// consumers (a frontend's server, the browser extension) to fetch the
/// subset of signals and displays they're authorized to see; each scope
/// vector is independent, empty = wildcard. An `Operator` token carries
/// no scope: it is the tenant's admin key.
#[derive(Debug, Clone)]
pub struct SignalToken {
    /// The token's stable identity: what list/revoke address. Never secret.
    pub id: uuid::Uuid,
    pub kind: TokenKind,
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
    /// The displays this token may reach, as `<project id>/<node>`
    /// pairs, where the node is spelled the way a person writes it
    /// (`test.whatsapp`). Reaching one means reading its panel AND
    /// pressing the buttons its items carry.
    ///
    /// The pair names one display in one project, because that
    /// spelling is only a name inside a project. The person types the
    /// node alone and the CLI fills in the project they are standing
    /// in. The token's own project scope still applies on top: a scope
    /// narrows, so a grant can never reach a project
    /// [`SignalToken::covers_project`] refuses, and mint rejects one
    /// that would.
    ///
    /// This dimension does NOT follow the empty-means-any convention
    /// the other two use, and deliberately: a display can be a
    /// credential (a bridge's QR code pairs the account to whoever
    /// scans it), so a token minted without a word about displays
    /// reaches none. `--displays` sets [`SignalToken::all_displays`];
    /// `--display <node>` fills this, one pair per display, and the
    /// project scope still bounds it.
    pub allowed_displays: Vec<String>,
    /// Every display in the token's projects, rather than the named
    /// ones. What `weft token mint --displays` sets.
    pub all_displays: bool,
    pub created_at: u64,
    /// An instance token: the one instance it acts inside, in its one
    /// project. It starts that instance's runs, answers its waits, reads
    /// its displays, and connects its accounts; nothing else.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// When it stops working (unix seconds); `None` never. An instance
    /// token always has one.
    pub expires_at: Option<u64>,
}

impl SignalToken {
    /// Whether the token has stopped working at `now` (unix seconds).
    pub fn expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|at| now >= at)
    }

    /// The instance and project an instance token acts inside.
    pub fn instance_scope(&self) -> Option<(uuid::Uuid, &weft_core::instance::InstanceId)> {
        let instance = self.instance.as_ref()?;
        Some((*self.allowed_projects.first()?, instance))
    }

    /// Does this token's project scope cover `project_id`?
    ///
    /// Empty means every project of the tenant, the same
    /// empty-means-any rule `allowed_tags` uses. Every door that reads
    /// a project asks this, so none of them can spell the containment
    /// check slightly differently.
    pub fn covers_project(&self, project_id: &uuid::Uuid) -> bool {
        self.allowed_projects.is_empty() || self.allowed_projects.contains(project_id)
    }

    /// Does this token reach any display at all? What the doors use to
    /// refuse a token that was never given the dimension, with a
    /// message naming the flag, instead of answering an empty list.
    pub fn reaches_displays(&self) -> bool {
        self.all_displays || !self.allowed_displays.is_empty()
    }
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
    pub inherited_from: Option<ExecutionId>,
    pub at_unix: u64,
    pub level: String,
    pub node: Option<String>,
    /// The iteration the firing was in, empty at the root. Without it
    /// a loop over two hundred items gives two hundred identical
    /// lines and the reader has to open the replay anyway.
    pub frames: weft_core::LoopFrames,
    pub message: String,
    /// When the line was written, in milliseconds, the key the log is
    /// ordered by: the worker's clock for a node's line, and for a
    /// failure (which carries no milliseconds) the end of its second.
    pub written_at_ms: u64,
    /// A node's line carries its place among the firing's lines, from the
    /// worker; a failure has none. Breaks the tie between lines of one
    /// millisecond.
    pub seq: Option<u64>,
}

impl LogEntry {
    /// The lines as the run wrote them: by the worker's clock, to the
    /// millisecond, then by the firing's own sequence (parallel firings
    /// hand their lines over in whatever order they ran). A failure has
    /// no clock of its own and sorts at the end of its second. Stable, so
    /// what neither key separates keeps its record order.
    pub fn in_written_order(mut entries: Vec<LogEntry>) -> Vec<LogEntry> {
        entries.sort_by_key(|e| (e.written_at_ms, e.seq.unwrap_or(u64::MAX)));
        entries
    }

    /// The last `limit` lines in written order: THE `logs_for` answer,
    /// the same code for both journals, so what a fake-backed test pins is
    /// what the real read does. The cut is made after the sort, so it is
    /// the last lines the run wrote.
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

    /// The line a record row that no longer decodes reads as: the decode
    /// error, which names the run and `weft clean`. Its own clock is
    /// unreadable, so it is stamped with when the row was written and
    /// sorted last, where the tail always holds it.
    pub fn corrupt_row(written_at_unix: u64, error: String) -> LogEntry {
        LogEntry {
            at_unix: written_at_unix,
            inherited_from: None,
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
    /// Postgres and fake journals so `weft logs` reads the same thing
    /// against both.
    pub fn from_event(event: &ExecEvent) -> Option<LogEntry> {
        // `KINDS` is the one gate: a kind the list carries and the match
        // does not is a bug the tests pin (`every_listed_kind_projects`),
        // never a quiet `None`.
        if !Self::KINDS.contains(&event.kind_str()) {
            return None;
        }
        Some(match event {
            ExecEvent::LogLine { node_id, frames, level, message, at_unix, at_unix_ms, seq, .. } => {
                LogEntry {
                    at_unix: *at_unix,
                    inherited_from: None,
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
                inherited_from: None,
                at_unix: *at_unix,
                level: "error".into(),
                node: Some(node_id.clone()),
                frames: frames.clone(),
                message: format!("node failed: {error}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            ExecEvent::NodeCancelled { node_id, frames, reason, at_unix, .. } => LogEntry {
                inherited_from: None,
                at_unix: *at_unix,
                level: "warn".into(),
                node: Some(node_id.clone()),
                frames: frames.clone(),
                message: format!("node cancelled: {reason}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            ExecEvent::ExecutionFailed { error, at_unix, .. } => LogEntry {
                inherited_from: None,
                at_unix: *at_unix,
                level: "error".into(),
                node: None,
                frames: Vec::new(),
                message: format!("execution failed: {error}"),
                written_at_ms: Self::end_of_second_ms(*at_unix),
                seq: None,
            },
            ExecEvent::ExecutionCancelled { reason, at_unix, .. } => LogEntry {
                inherited_from: None,
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

    /// The event kinds the log is made of: what `from_event` answers for.
    pub const KINDS: &'static [&'static str] = &[
        "log_line",
        "node_failed",
        "node_cancelled",
        "execution_failed",
        "execution_cancelled",
    ];
}

#[cfg(test)]
mod bake_tests {
    use super::*;

    fn completed() -> Vec<ExecEvent> {
        let execution_id = ExecutionId::new_v4();
        vec![
            ExecEvent::ExecutionStarted {
                execution_id, project_id: uuid::Uuid::from_u128(0x100), entry_node: "trigger".into(),
                phase: weft_core::context::Phase::TriggerSetup,
                definition_hash: Some("graph".into()),
                binary_hash: Some("binary".into()),
                source_version: Some("source".into()), run_kind: weft_core::exec::RunKind::Execution, selection: None, seed: None, instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
                settings: weft_core::run_settings::RunSettings::bookkeeping(),
            },
            ExecEvent::ExecutionCompleted { execution_id, at_unix: 2 },
        ]
    }

    fn program() -> weft_core::project::hash::ProgramIdentity {
        weft_core::project::hash::ProgramIdentity {
            definition_hash: "graph".into(), binary_hash: "binary".into(), implementations: Default::default(),
        }
    }

    #[test]
    fn closed_gates_publish_an_empty_bake_but_incomplete_setups_do_not() {
        let rows = completed();
        let bake = TriggerBake::from_events(&rows, &program()).unwrap().unwrap();
        assert!(bake.captured.is_empty());
        assert!(TriggerBake::from_events(&rows[..1], &program()).is_err());
        let mut failed = rows;
        failed[1] = ExecEvent::ExecutionCancelled {
            execution_id: bake.execution_id, reason: "cancelled".into(), cause: Some(weft_core::exec::CancelCause::User), at_unix: 2,
        };
        assert!(TriggerBake::from_events(&failed, &program()).unwrap().is_none());
    }

    #[test]
    fn bake_refuses_mixed_run_history_and_conflicting_program_identity() {
        let mut rows = completed();
        rows[1] = ExecEvent::ExecutionCompleted { execution_id: ExecutionId::new_v4(), at_unix: 2 };
        assert!(TriggerBake::from_events(&rows, &program()).unwrap_err().to_string().contains("another run"));
        let mut rows = completed();
        if let ExecEvent::ExecutionStarted { definition_hash, .. } = &mut rows[0] {
            *definition_hash = Some("another graph".into());
        }
        assert!(TriggerBake::from_events(&rows, &program()).unwrap_err().to_string().contains("conflicting program"));
    }
}

#[cfg(test)]
mod log_entry_tests {
    use super::LogEntry;
    use weft_journal::ExecEvent;

    fn sample_events() -> Vec<ExecEvent> {
        let execution_id = weft_core::ExecutionId::new_v4();
        vec![
            ExecEvent::LogLine {
                execution_id,
                node_id: "greet".into(),
                frames: Default::default(),
                level: "info".into(),
                message: "hi".into(),
                at_unix: 1,
                at_unix_ms: Some(1_000),
                seq: Some(0),
            },
            ExecEvent::NodeFailed {
                execution_id,
                node_id: "llm".into(),
                frames: Default::default(),
                error: "boom".into(),
                at_unix: 2,
            },
            ExecEvent::NodeCancelled {
                execution_id,
                node_id: "llm".into(),
                frames: Default::default(),
                reason: "stopped".into(),
                at_unix: 3,
            },
            ExecEvent::ExecutionFailed { execution_id, error: "stuck".into(), at_unix: 5 },
            ExecEvent::ExecutionCancelled {
                execution_id,
                reason: "by hand".into(),
                cause: None,
                at_unix: 6,
            },
            ExecEvent::ExecutionCompleted { execution_id, at_unix: 7 },
            ExecEvent::ExecutionTagged { execution_id, tags: vec!["t".into()], at_unix: 8 },
        ]
    }

    /// Every failure the journal records about a run reaches the log as
    /// an error / warn line naming its node, so `weft logs` answers "why
    /// did this go wrong" without the full replay.
    #[test]
    fn failures_project_to_log_lines() {
        let events = sample_events();
        let lines: Vec<LogEntry> = events.iter().filter_map(LogEntry::from_event).collect();
        assert_eq!(lines.len(), 5, "five log-worthy events: {lines:?}");
        assert_eq!((lines[0].level.as_str(), lines[0].node.as_deref()), ("info", Some("greet")));
        assert_eq!((lines[1].level.as_str(), lines[1].node.as_deref()), ("error", Some("llm")));
        assert!(lines[1].message.contains("boom"), "{}", lines[1].message);
        assert_eq!(lines[2].level, "warn");
        assert_eq!((lines[3].level.as_str(), lines[3].node.as_deref()), ("error", None));
        assert!(lines[4].message.contains("by hand"), "{}", lines[4].message);
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
        let execution_id = weft_core::ExecutionId::new_v4();
        let line = |node: &str, seq: u64, at_ms: Option<u64>, at_unix: u64| ExecEvent::LogLine {
            execution_id,
            node_id: node.into(),
            frames: Default::default(),
            level: "info".into(),
            message: format!("{node} {seq}"),
            at_unix,
            at_unix_ms: at_ms,
            seq: Some(seq),
        };
        let failed = ExecEvent::NodeFailed {
            execution_id,
            node_id: "a".into(),
            frames: Default::default(),
            error: "boom".into(),
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

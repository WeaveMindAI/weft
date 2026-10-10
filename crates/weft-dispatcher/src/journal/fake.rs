//! In-memory `Journal` implementation for tests. Mirrors the Postgres
//! semantics: a `run` row per run, its record, and the signal and token
//! tables. Compiled only under `cfg(test)` and behind the `test-helpers`
//! feature so dependent crates can pull it in for their own tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use weft_core::ExecutionId;

use weft_journal::record::{Appended, Queued, Then};
use weft_journal::ExecEvent;
use crate::journal::{ExecutionOwner, ExecutionQuery, Journal, LogEntry, SignalRegistration, SignalToken};
use weft_core::program::{ExecutionPage, ExecutionSummary, RunStatus};

#[derive(Default)]
struct FakeState {
    /// Setup run -> its project: several setups of one project may run at
    /// once, each over its own triggers.
    trigger_setups: HashMap<ExecutionId, uuid::Uuid>,
    trigger_bakes: HashMap<(uuid::Uuid, Option<weft_core::instance::InstanceId>, String), super::TriggerBake>,
    /// Every run's record, in the order written: a test reads a run's
    /// record back, and the seeds of a run alongside it.
    events: Vec<ExecEvent>,
    signal_tokens: HashMap<String, SignalToken>,
    /// One entry per `signal` row.
    signals: HashMap<String, SignalRegistration>,
    /// Mirror of the Postgres `run` rows.
    runs: HashMap<ExecutionId, RunRow>,
    /// project_id -> tenant_id, mirroring the `project` table a run's
    /// tenant is read from. Tests register a project's tenant here (via
    /// `set_project_tenant`); a run of an unregistered project is
    /// refused, as Postgres refuses it.
    project_tenants: HashMap<uuid::Uuid, String>,
    /// Mirror of the Postgres `execution_tag` table: (run, tag) -> seq,
    /// seq handed out in write order like the real BIGSERIAL, an existing
    /// pair keeping its seq like the real ON CONFLICT.
    execution_tags: HashMap<(ExecutionId, String), i64>,
    next_tag_seq: i64,
    /// Answers handed to a run its worker drives, by run, for the worker
    /// to take (the Postgres `parked_fire` rows `hand_answer_in` writes).
    handed: HashMap<ExecutionId, Vec<(String, weft_core::primitive::WaitAnswer)>>,
}

#[derive(Default)]
pub struct FakeJournal {
    inner: Mutex<FakeState>,
}

/// One `run` row's mirror, the columns the journal reads.
#[derive(Clone)]
struct RunRow {
    project_id: uuid::Uuid,
    tenant_id: String,
    kind: &'static str,
    phase: weft_core::context::Phase,
    entry_node: String,
    instance: Option<weft_core::instance::InstanceId>,
    fired_by: Option<String>,
    definition_hash: Option<String>,
    binary_hash: Option<String>,
    source_version: Option<String>,
    started_at: u64,
    /// running | parked | queued | ended
    state: &'static str,
    keeping: weft_core::run_settings::Keeping,
    owner: Option<String>,
    ending: Option<ExecEvent>,
    skipped: u64,
}

impl FakeJournal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Put `event` on its run's record the way the run's worker writes it:
    /// a birth makes its row (running, driven by a worker), an ending ends
    /// it. What a test seeds a run with.
    pub async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        let execution_id = event.execution_id();
        if let ExecEvent::ExecutionStarted { .. } = event {
            let row = born(&g, event, "running", Some("worker".into()))?;
            if g.runs.contains_key(&execution_id) {
                anyhow::bail!("run {execution_id} is born twice");
            }
            g.runs.insert(execution_id, row);
        }
        let Some(row) = g.runs.get_mut(&execution_id) else {
            anyhow::bail!("run {execution_id} has no row: its birth comes first");
        };
        anyhow::ensure!(row.state != "ended", "run {execution_id} ended; nothing follows its ending");
        note(row, std::slice::from_ref(event));
        g.events.push(event.clone());
        Ok(())
    }

    /// The answers handed to `execution_id`'s worker, in order.
    pub fn handed(&self, execution_id: ExecutionId) -> Vec<(String, weft_core::primitive::WaitAnswer)> {
        self.inner.lock().unwrap().handed.get(&execution_id).cloned().unwrap_or_default()
    }

    /// Let go of `execution_id` parked on a wait, the way its worker does.
    pub fn park(&self, execution_id: ExecutionId) {
        let mut g = self.inner.lock().unwrap();
        let row = g.runs.get_mut(&execution_id).expect("a run to park");
        row.state = "parked";
        row.owner = None;
    }

    /// Tag a run the way the broker's `/v1/execution/tag` does: one
    /// `execution_tag` row per tag, a re-tag keeping the original seq.
    /// Returns the seq of the FIRST tag in `tags` (the anchor a `Keep`
    /// stop by this run would use).
    pub fn tag_execution(&self, execution_id: ExecutionId, tags: &[&str], _at_unix: u64) -> i64 {
        let mut g = self.inner.lock().unwrap();
        let mut first = None;
        for tag in tags {
            let key = (execution_id, tag.to_string());
            let seq = match g.execution_tags.get(&key) {
                Some(seq) => *seq,
                None => {
                    g.next_tag_seq += 1;
                    let seq = g.next_tag_seq;
                    g.execution_tags.insert(key, seq);
                    seq
                }
            };
            first.get_or_insert(seq);
        }
        first.expect("tag_execution needs at least one tag")
    }

    /// Register a project's owning tenant, mirroring the `project` table a
    /// run's tenant is read from.
    pub fn set_project_tenant(&self, project_id: uuid::Uuid, tenant: &str) {
        self.inner.lock().unwrap().project_tenants.insert(project_id, tenant.to_string());
    }

    /// The summary `execution_id`'s row says, mirroring the Postgres one.
    fn summary(g: &FakeState, execution_id: ExecutionId) -> Option<ExecutionSummary> {
        let row = g.runs.get(&execution_id)?;
        let (status, completed_at, cancel_cause, error) = match &row.ending {
            Some(ExecEvent::ExecutionCompleted { at_unix, .. }) => (RunStatus::Completed, Some(*at_unix), None, None),
            Some(ExecEvent::ExecutionFailed { at_unix, error, .. }) => (RunStatus::Failed, Some(*at_unix), None, Some(error.clone())),
            Some(ExecEvent::ExecutionCancelled { at_unix, cause, .. }) => (RunStatus::Cancelled, Some(*at_unix), cause.clone(), None),
            Some(_) => unreachable!("only an ending is kept"),
            None if row.state == "parked" => (RunStatus::WaitingForInput, None, None, None),
            None => (RunStatus::Running, None, None, None),
        };
        let mut tagged: Vec<(i64, String)> =
            g.execution_tags.iter().filter(|((c, _), _)| *c == execution_id).map(|((_, tag), seq)| (*seq, tag.clone())).collect();
        tagged.sort();
        Some(ExecutionSummary {
            execution_id,
            project_id: row.project_id,
            entry_node: row.entry_node.clone(),
            status,
            phase: row.phase,
            started_at: row.started_at,
            completed_at,
            tags: tagged.into_iter().map(|(_, tag)| tag).collect(),
            cancel_cause,
            error,
            skipped_nodes: row.skipped,
            instance: row.instance.clone(),
        })
    }

    /// Whether `node` started in `execution_id`: the listing's `node` filter.
    fn ran_node(g: &FakeState, execution_id: ExecutionId, node: &str) -> bool {
        g.events.iter().any(|e| matches!(e, ExecEvent::NodeStarted { execution_id: of, node_id, .. } if *of == execution_id && node_id == node))
    }
}

/// The row `birth` makes, in `state`, driven by `owner`, its tenant read
/// from the project's, as Postgres reads it from the `project` row.
fn born(g: &FakeState, birth: &ExecEvent, state: &'static str, owner: Option<String>) -> anyhow::Result<RunRow> {
    let ExecEvent::ExecutionStarted {
        execution_id, project_id, entry_node, phase, definition_hash, binary_hash, source_version, run_kind, instance, fired_trigger,
        settings, at_unix, ..
    } = birth
    else {
        anyhow::bail!("a run's row is made from its birth");
    };
    let tenant = g.project_tenants.get(project_id).cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "run {execution_id} cannot be born: project {project_id} has no tenant registered (call set_project_tenant \
             first); Postgres refuses a run of a project with no row the same way"
        )
    })?;
    Ok(RunRow {
        project_id: *project_id,
        tenant_id: tenant,
        kind: run_kind.as_str(),
        phase: *phase,
        entry_node: entry_node.clone(),
        instance: instance.clone(),
        fired_by: fired_trigger.clone(),
        definition_hash: definition_hash.clone(),
        binary_hash: binary_hash.clone(),
        source_version: source_version.clone(),
        started_at: *at_unix,
        state,
        keeping: settings.keeping(),
        owner,
        ending: None,
        skipped: 0,
    })
}

/// What `events` say about their run's row: its skips, and its ending.
fn note(row: &mut RunRow, events: &[ExecEvent]) {
    for event in events {
        if matches!(event, ExecEvent::NodeSkipped { .. }) {
            row.skipped += 1;
        }
        if event.is_execution_terminal() && row.ending.is_none() {
            row.ending = Some(event.clone());
            row.state = "ended";
            row.owner = None;
        }
    }
}

/// Write `from`'s refreshed columns over `row` and move its version one
/// past `from`'s: what Postgres's `signal_insert` refresh and
/// `signal_restore` write. The row's identity (tenant, project, instance,
/// execution, node, is_resume, activation_trigger) stays as it is.
// SYNC: copy_refreshed <-> journal/postgres.rs SIGNAL_REFRESHED_COLUMNS
fn copy_refreshed(row: &mut SignalRegistration, from: &SignalRegistration) {
    row.spec_json = from.spec_json.clone();
    row.program = from.program.clone();
    row.setup_execution_id = from.setup_execution_id;
    row.source_version = from.source_version.clone();
    row.access_id = from.access_id.clone();
    row.consumer_kind = from.consumer_kind.clone();
    row.tags = from.tags.clone();
    row.port_snapshot = from.port_snapshot.clone();
    row.consumer_payload = from.consumer_payload.clone();
    row.surface_kind = from.surface_kind.clone();
    row.mount_path = from.mount_path.clone();
    row.mount_methods = from.mount_methods.clone();
    row.auth_kind = from.auth_kind.clone();
    row.auth_config = from.auth_config.clone();
    row.kind_state = from.kind_state.clone();
    row.kind_state_seq = from.kind_state_seq + 1;
}

#[async_trait]
impl Journal for FakeJournal {
    async fn is_trigger_setup_pending(&self, execution_id: ExecutionId) -> anyhow::Result<bool> {
        Ok(self.inner.lock().unwrap().trigger_setups.contains_key(&execution_id))
    }

    async fn finish_trigger_setup(&self, execution_id: ExecutionId, bake: Option<&super::TriggerBake>) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        let project = g.trigger_setups.get(&execution_id).copied();
        if let Some(project) = project {
            if let Some(bake) = bake {
                anyhow::ensure!(bake.project_id == project && bake.execution_id == execution_id, "bake does not belong to its setup");
                let key = (project, bake.instance.clone(), bake.program.digest());
                let merged = match g.trigger_bakes.remove(&key) {
                    Some(prior) => prior.refreshed_by(bake),
                    None => bake.clone(),
                };
                g.trigger_bakes.insert(key, merged);
            }
            g.trigger_setups.remove(&execution_id);
        }
        Ok(())
    }

    async fn trigger_bakes(&self, project_id: uuid::Uuid, instance: Option<&weft_core::instance::InstanceId>) -> anyhow::Result<Vec<super::TriggerBake>> {
        Ok(self.inner.lock().unwrap().trigger_bakes.values()
            .filter(|bake| bake.project_id == project_id && bake.instance.as_ref() == instance).cloned().collect())
    }

    async fn append(&self, execution_id: ExecutionId, events: &[ExecEvent], then: Then) -> anyhow::Result<Appended> {
        let mut g = self.inner.lock().unwrap();
        let Some(row) = g.runs.get_mut(&execution_id) else { return Ok(Appended::Missing) };
        if let Some(owner) = &row.owner {
            return Ok(Appended::Driven { owner: owner.clone() });
        }
        if row.state == "ended" {
            return Ok(Appended::Ended);
        }
        if then == Then::Queued {
            row.state = "queued";
        }
        note(row, events);
        g.events.extend(events.iter().cloned());
        Ok(Appended::At(0))
    }

    async fn events_log_lossy(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<(Vec<crate::events::IdentifiedEvent<ExecEvent>>, Vec<String>)> {
        // In-memory events are typed, so nothing can fail to decode. Each
        // event is its own row here.
        let events = self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.execution_id() == execution_id)
            .enumerate()
            .map(|(seq, event)| crate::events::IdentifiedEvent::recorded(seq as i32, 0, event.clone()))
            .collect();
        Ok((events, Vec::new()))
    }

    async fn queue_run(&self, queued: Queued<'_>, _for_activation: bool) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let birth = queued.events.first().ok_or_else(|| anyhow::anyhow!("a run is queued with its birth"))?;
        let execution_id = birth.execution_id();
        if g.runs.contains_key(&execution_id) {
            return Ok(false);
        }
        let mut row = born(&g, birth, "queued", None)?;
        note(&mut row, queued.events);
        if let ExecEvent::ExecutionStarted { phase: weft_core::context::Phase::TriggerSetup, project_id, .. } = birth {
            g.trigger_setups.insert(execution_id, *project_id);
        }
        g.runs.insert(execution_id, row);
        g.events.extend(queued.events.iter().cloned());
        Ok(true)
    }

    async fn cancel_execution(
        &self,
        execution_id: ExecutionId,
        _program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<crate::journal::CancelWrite> {
        let mut g = self.inner.lock().unwrap();
        // The strip is `signal_remove_for_execution_id`'s predicate: every
        // signal tied to the run.
        let keys: Vec<String> = g.signals.iter().filter(|(_, s)| s.execution_id == Some(execution_id)).map(|(k, _)| k.clone()).collect();
        let removed: Vec<SignalRegistration> = keys.into_iter().filter_map(|k| g.signals.remove(&k)).collect();
        let mut write = crate::journal::CancelWrite { removed, ..Default::default() };
        let Some(row) = g.runs.get_mut(&execution_id) else { return Ok(write) };
        match (row.state, &row.owner) {
            ("ended", _) => {}
            (_, Some(_)) => write.requested = true,
            _ => {
                // Same fidelity as the real cancel's ending, without the
                // per-node rows (the fake folds no nodes).
                let ending = ExecEvent::ExecutionCancelled { execution_id, reason: cause.to_string(), cause: Some(cause.clone()), at_unix: 0 };
                note(row, std::slice::from_ref(&ending));
                g.events.push(ending);
                write.node_cancellations = Some(0);
            }
        }
        Ok(write)
    }

    /// The fake holds no leases: every owner it is asked about is gone.
    async fn let_go_of_lost(
        &self,
        execution_id: ExecutionId,
        _lapsed_before: i64,
        _program: Option<&weft_core::ProjectDefinition>,
    ) -> anyhow::Result<crate::journal::Lost> {
        use crate::journal::Lost;
        let mut g = self.inner.lock().unwrap();
        let Some(row) = g.runs.get_mut(&execution_id) else { return Ok(Lost::NotLost) };
        if row.state != "running" || row.owner.is_none() {
            return Ok(Lost::NotLost);
        }
        row.owner = None;
        if row.keeping.is_durable() {
            row.state = "queued";
            return Ok(Lost::Requeued);
        }
        let cause = weft_core::exec::CancelCause::fast_run_lost();
        let ending = ExecEvent::ExecutionCancelled { execution_id, reason: cause.to_string(), cause: Some(cause), at_unix: 0 };
        note(row, std::slice::from_ref(&ending));
        g.events.push(ending);
        Ok(Lost::Ended)
    }

    /// An answer reaches its run as Postgres's does: written into the
    /// record of a run nobody drives (queued to carry on), handed to the
    /// worker of a run one drives ([`Self::handed`]).
    async fn answer(&self, token: &str, answer: &weft_core::primitive::WaitAnswer) -> anyhow::Result<crate::journal::Answered> {
        use crate::journal::Answered;
        let mut g = self.inner.lock().unwrap();
        let Some(consumed) = g.signals.get(token).filter(|s| s.is_resume).cloned() else { return Ok(Answered::Gone) };
        g.signals.remove(token);
        let Some(execution_id) = consumed.execution_id else { return Ok(Answered::Gone) };
        let Some(row) = g.runs.get_mut(&execution_id).filter(|row| row.state != "ended") else { return Ok(Answered::RunEnded { consumed }) };
        if row.owner.is_some() {
            g.handed.entry(execution_id).or_default().push((token.to_string(), answer.clone()));
            return Ok(Answered::Reached { consumed });
        }
        row.state = "queued";
        g.events.push(ExecEvent::wait_answered(execution_id, token.to_string(), answer.clone(), 0));
        Ok(Answered::Reached { consumed })
    }

    async fn mint_signal_token(&self, tok: &SignalToken) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .signal_tokens
            .insert(tok.token_hash.clone(), tok.clone());
        Ok(())
    }

    async fn get_signal_token(&self, token_hash: &str) -> anyhow::Result<Option<SignalToken>> {
        Ok(self.inner.lock().unwrap().signal_tokens.get(token_hash).cloned())
    }

    async fn seed_operator_token(&self, tok: &SignalToken) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let held = g
            .signal_tokens
            .values()
            .any(|t| t.tenant_id == tok.tenant_id && t.kind == weft_core::signal_token::TokenKind::Operator);
        if held || g.signal_tokens.contains_key(&tok.token_hash) {
            return Ok(false);
        }
        g.signal_tokens.insert(tok.token_hash.clone(), tok.clone());
        Ok(true)
    }

    async fn list_signal_tokens(&self, tenant: &str) -> anyhow::Result<Vec<SignalToken>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .signal_tokens
            .values()
            .filter(|tok| tok.tenant_id == tenant)
            .cloned()
            .collect())
    }

    async fn revoke_signal_token(&self, id: uuid::Uuid, tenant: &str) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let keys: Vec<String> = g
            .signal_tokens
            .iter()
            .filter(|(_, tok)| tok.tenant_id == tenant && tok.id == id)
            .map(|(k, _)| k.clone())
            .collect();
        let removed = !keys.is_empty();
        for k in keys {
            g.signal_tokens.remove(&k);
        }
        Ok(removed)
    }

    async fn execution_owner(&self, execution_id: ExecutionId) -> anyhow::Result<Option<ExecutionOwner>> {
        Ok(self.inner.lock().unwrap().runs.get(&execution_id).map(|r| ExecutionOwner {
            project_id: r.project_id,
            tenant: r.tenant_id.clone(),
            instance: r.instance.clone(),
            fired_by: r.fired_by.clone(),
            phase: r.phase,
            definition_hash: r.definition_hash.clone(),
            binary_hash: r.binary_hash.clone(),
            source_version: r.source_version.clone(),
        }))
    }

    async fn definition_hashes_in_use(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        let mut out: Vec<String> =
            self.inner.lock().unwrap().runs.values().filter(|r| r.project_id == project_id).filter_map(|r| r.definition_hash.clone()).collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn logs_for(&self, execution_id: ExecutionId, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        // The same tail as Postgres, through the one `LogEntry::tail`.
        let entries: Vec<LogEntry> = self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.execution_id() == execution_id)
            .filter_map(LogEntry::from_event)
            .collect();
        Ok(LogEntry::tail(entries, limit))
    }

    async fn list_executions(&self, tenant: &str, query: &ExecutionQuery) -> anyhow::Result<ExecutionPage> {
        // The words of a run are Postgres's full-text search, which a fake
        // would only imitate: `search` is tested against Postgres.
        assert!(query.search.is_none(), "the listing's `search` is tested against Postgres, not the fake journal");
        let g = self.inner.lock().unwrap();
        let mut all: Vec<ExecutionSummary> = g
            .runs
            .iter()
            .filter(|(_, row)| row.tenant_id == tenant && row.kind == "execution")
            .filter_map(|(execution_id, _)| Self::summary(&g, *execution_id))
            .filter(|s| query.project_id.is_none_or(|p| s.project_id == p))
            .filter(|s| query.started_after.is_none_or(|a| s.started_at >= a))
            .filter(|s| query.started_before.is_none_or(|b| s.started_at < b))
            .filter(|s| query.phase.is_none_or(|p| s.phase == p))
            .filter(|s| query.entry_node.as_deref().is_none_or(|n| s.entry_node == n))
            .filter(|s| query.status.is_none_or(|st| st.reaches(s.status)))
            .filter(|s| query.instance.as_ref().is_none_or(|m| s.instance.as_ref() == Some(m)))
            .filter(|s| query.tag.as_deref().is_none_or(|t| s.tags.iter().any(|x| x == t)))
            .filter(|s| query.node.as_deref().is_none_or(|node| Self::ran_node(&g, s.execution_id, node)))
            .collect();
        all.sort_by(|a, b| b.started_at.cmp(&a.started_at).then(b.execution_id.cmp(&a.execution_id)));
        let total = all.len() as u64;
        let executions = all
            .into_iter()
            .filter(|s| query.below.is_none_or(|below| (s.started_at, s.execution_id) < below))
            .skip(query.offset as usize)
            .take(query.limit as usize)
            .collect();
        Ok(ExecutionPage { executions, total })
    }

    async fn execution_summary(&self, execution_id: ExecutionId) -> anyhow::Result<Option<ExecutionSummary>> {
        Ok(Self::summary(&self.inner.lock().unwrap(), execution_id))
    }

    async fn execution_summaries(
        &self,
        execution_ids: &[ExecutionId],
    ) -> anyhow::Result<std::collections::HashMap<ExecutionId, ExecutionSummary>> {
        let g = self.inner.lock().unwrap();
        Ok(execution_ids.iter().filter_map(|execution_id| Self::summary(&g, *execution_id).map(|s| (*execution_id, s))).collect())
    }

    async fn execution_ids_for_project(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<ExecutionId>> {
        Ok(self.inner.lock().unwrap().runs.iter().filter(|(_, row)| row.project_id == project_id).map(|(c, _)| *c).collect())
    }

    async fn execution_ids_with_prefix(&self, tenant: &str, prefix: &str) -> anyhow::Result<Vec<ExecutionId>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<ExecutionId> = g
            .runs
            .iter()
            .filter(|(c, row)| row.tenant_id == tenant && row.kind == "execution" && c.to_string().starts_with(prefix))
            .map(|(c, _)| *c)
            .collect();
        out.sort();
        out.truncate(2);
        Ok(out)
    }

    async fn going_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<(ExecutionId, weft_core::context::Phase)>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<(u64, ExecutionId, weft_core::context::Phase)> = g
            .runs
            .iter()
            .filter(|(_, row)| row.project_id == project_id && row.kind == "execution" && matches!(row.state, "queued" | "running"))
            .map(|(execution_id, row)| (row.started_at, *execution_id, row.phase))
            .collect();
        // Oldest first, ties on the id, like Postgres: the editor reads the
        // last one as "the latest run".
        out.sort_by_key(|(started, execution_id, _)| (*started, *execution_id));
        Ok(out.into_iter().map(|(_, execution_id, phase)| (execution_id, phase)).collect())
    }

    async fn settled_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<std::collections::HashSet<ExecutionId>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .runs
            .iter()
            .filter(|(_, row)| row.project_id == project_id && matches!(row.state, "ended" | "parked"))
            .map(|(c, _)| *c)
            .collect())
    }

    async fn delete_execution(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        g.trigger_setups.remove(&execution_id);
        g.events.retain(|e| e.execution_id() != execution_id);
        // Resume rows only, exactly as `PostgresJournal` does.
        let keys: Vec<String> =
            g.signals.iter().filter(|(_, s)| s.execution_id == Some(execution_id) && s.is_resume).map(|(k, _)| k.clone()).collect();
        let removed = keys.into_iter().filter_map(|k| g.signals.remove(&k)).collect();
        g.runs.remove(&execution_id);
        g.execution_tags.retain(|(c, _), _| *c != execution_id);
        Ok(removed)
    }

    async fn delete_project_executions(&self, project_id: uuid::Uuid) -> anyhow::Result<u64> {
        let execution_ids: Vec<ExecutionId> = self.execution_ids_for_project(project_id).await?;
        for execution_id in &execution_ids {
            self.delete_execution(*execution_id).await?;
        }
        Ok(execution_ids.len() as u64)
    }

    /// The fake keeps no end times: nothing expires.
    async fn erase_expired(&self, _now: i64, _limit: i64) -> anyhow::Result<(usize, Vec<SignalRegistration>)> {
        Ok((0, Vec::new()))
    }

    async fn projects_with_orphan_executions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        // The fake has no project table, so it cannot know which project
        // rows are gone. Answering "none" under-reports, so a test can
        // never see a sweep the real one would not do; anything that turns
        // on this predicate belongs in a database test.
        Ok(Vec::new())
    }

    async fn live_tagged_executions(
        &self,
        project_id: uuid::Uuid,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedExecution>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<weft_journal::tags::TaggedExecution> = g
            .execution_tags
            .iter()
            .filter(|((_, t), _)| t == tag)
            .filter(|((execution_id, _), _)| {
                g.runs.get(execution_id).is_some_and(|row| row.project_id == project_id && row.kind == "execution" && row.state != "ended")
            })
            .map(|((execution_id, _), seq)| weft_journal::tags::TaggedExecution { execution_id: *execution_id, seq: *seq })
            .collect();
        out.sort_by_key(|t| t.seq);
        Ok(out)
    }

    async fn signal_insert(&self, sig: &SignalRegistration) -> anyhow::Result<super::SignalWrite> {
        let mut inner = self.inner.lock().unwrap();
        // Mirror Postgres's `idx_signal_entry_node` partial-unique on
        // `(project_id, node_id) WHERE is_resume = FALSE`: at most one ENTRY row per
        // node. Postgres reuses the SAME token across reactivates (ON CONFLICT
        // (token) refresh), so a second entry row for the same node under a
        // DIFFERENT token is a unique violation there. Reject it here too, else a
        // fake test could believe a double-registration is fine when production
        // rejects it. Resume rows (per-suspension tokens) are exempt, matching the
        // index's `WHERE is_resume = FALSE`.
        if !sig.is_resume {
            let collides = inner.signals.values().any(|existing| {
                !existing.is_resume
                    && existing.project_id == sig.project_id
                    && existing.node_id == sig.node_id
                    && existing.token != sig.token
            });
            if collides {
                anyhow::bail!(
                    "entry signal for (project {}, node {}) already exists under a \
                     different token (mirrors idx_signal_entry_node)",
                    sig.project_id,
                    sig.node_id
                );
            }
        }
        // Mirror Postgres: a wait is registered only for a run whose row
        // exists and has not ended.
        if let Some(run) = sig.execution_id.filter(|_| sig.is_resume) {
            match inner.runs.get(&run) {
                None => anyhow::bail!("run {run} has no record, so its wait '{}' cannot be registered", sig.node_id),
                Some(row) if row.state == "ended" => {
                    anyhow::bail!("run {run} ended before its wait '{}' could be registered", sig.node_id)
                }
                Some(_) => {}
            }
        }
        // Mirror the Postgres compare-and-set on kind_state_seq: the
        // write lands only while the row is still at the version the
        // registration read, and moves it one past.
        if inner.signals.get(&sig.token).is_some_and(|existing| existing.kind_state_seq != sig.kind_state_seq) {
            return Ok(super::SignalWrite::StateMoved);
        }
        // Like Postgres's ON CONFLICT refresh, a replaced row keeps its
        // identity and takes only the refreshed columns.
        match inner.signals.get_mut(&sig.token) {
            Some(row) => copy_refreshed(row, sig),
            None => {
                let mut written = sig.clone();
                written.kind_state_seq += 1;
                inner.signals.insert(written.token.clone(), written);
            }
        }
        Ok(super::SignalWrite::Written)
    }

    async fn signal_restore(&self, sig: &SignalRegistration) -> anyhow::Result<super::SignalWrite> {
        let mut inner = self.inner.lock().unwrap();
        let Some(row) = inner.signals.get_mut(&sig.token) else {
            anyhow::bail!("signal {} has no row to restore", sig.token);
        };
        if row.kind_state_seq != sig.kind_state_seq {
            return Ok(super::SignalWrite::StateMoved);
        }
        copy_refreshed(row, sig);
        Ok(super::SignalWrite::Written)
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        Ok(self.inner.lock().unwrap().signals.get(token).cloned())
    }

    async fn signal_entry_at(
        &self,
        project_id: uuid::Uuid,
        node: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<Option<SignalRegistration>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .signals
            .values()
            .find(|s| {
                !s.is_resume && s.project_id == project_id && s.node_id == node && s.instance.as_ref() == instance
            })
            .cloned())
    }

    async fn signal_withdraw(&self, execution_id: ExecutionId, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let of_run = g.signals.get(token).is_some_and(|s| s.execution_id == Some(execution_id));
        Ok(if of_run { g.signals.remove(token) } else { None })
    }

    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for t in tokens {
            if let Some(stored) = g.signals.remove(t) {
                out.push(stored);
            }
        }
        Ok(out)
    }

    async fn signal_list_for_execution_id(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>> {
        let g = self.inner.lock().unwrap();
        Ok(g.signals
            .values()
            .filter(|s| s.is_resume && s.execution_id == Some(execution_id))
            .cloned()
            .collect())
    }

    async fn signal_list_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .signals
            .values()
            .filter(|s| s.project_id == project_id)
            .cloned()
            .collect())
    }

    async fn signal_remove_for_execution_id(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        // Mirror the postgres predicate EXACTLY (`DELETE ... WHERE execution =
        // $1`, `SIGNAL_DELETE_BY_EXECUTION_ID_RETURNING`): every signal tied to
        // the execution goes, whatever its kind, so the fake and the real
        // store cannot diverge the day an entry signal carries an execution.
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.execution_id == Some(execution_id))
            .map(|(k, _)| k.clone())
            .collect();
        Ok(keys
            .into_iter()
            .filter_map(|k| g.signals.remove(&k))
            .collect())
    }

    async fn signal_remove_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.project_id == project_id)
            .map(|(k, _)| k.clone())
            .collect();
        Ok(keys
            .into_iter()
            .filter_map(|k| g.signals.remove(&k))
            .collect())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);
    const OTHER_PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x102);
    const PROJECT_A: uuid::Uuid = uuid::Uuid::from_u128(0x103);
    const PROJECT_B: uuid::Uuid = uuid::Uuid::from_u128(0x104);
    const PROJECT_C: uuid::Uuid = uuid::Uuid::from_u128(0x105);

    /// A bare entry row under `token`, for tests to adjust.
    pub(crate) fn registration(token: &str) -> SignalRegistration {
        SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_execution_id: None,
            program: None,
            token: token.into(),
            tenant_id: "t".into(),
            project_id: PROJECT,
            execution_id: None,
            node_id: "n".into(),
            is_resume: false,
            spec_json: "{}".into(),
            access_id: None,
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: "public_entry".into(),
            mount_path: None,
            mount_methods: Vec::new(),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            holds: false,
        }
    }

    /// Answering a wait hands back its consumed row (the listener learns
    /// of it from nowhere else), queues the parked run with the answer on
    /// record, and a second answer finds the wait gone.
    #[tokio::test]
    async fn an_answer_consumes_its_wait_once() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let run = uuid::Uuid::new_v4();
        j.record_event(&started(run, PROJECT)).await.unwrap();
        j.inner.lock().unwrap().runs.get_mut(&run).unwrap().owner = None;
        let mut resume = registration("tok-r");
        resume.is_resume = true;
        resume.execution_id = Some(run);
        j.signal_insert(&resume).await.unwrap();
        let crate::journal::Answered::Reached { consumed } = j.answer("tok-r", &weft_core::primitive::WaitAnswer::Given { value: serde_json::json!("yes") }).await.unwrap() else {
            panic!("the wait is answered")
        };
        assert_eq!(consumed.token, "tok-r");
        assert!(j.signal_get("tok-r").await.unwrap().is_none(), "single use");
        assert_eq!(j.inner.lock().unwrap().runs[&run].state, "queued");
        assert!(matches!(j.answer("tok-r", &weft_core::primitive::WaitAnswer::Skipped).await.unwrap(), crate::journal::Answered::Gone));

        // An entry row is never answered this way.
        j.signal_insert(&registration("tok-e")).await.unwrap();
        assert!(matches!(j.answer("tok-e", &weft_core::primitive::WaitAnswer::Skipped).await.unwrap(), crate::journal::Answered::Gone));
        assert!(j.signal_get("tok-e").await.unwrap().is_some(), "entry rows stay");
    }

    /// Erasing a run answers the questions it was parked on, so the
    /// caller can tell the listener to let go. A plain
    /// delete left the listener serving a form for a run that no longer
    /// existed. The project's
    /// entry signal is not the run's and stays.
    #[tokio::test]
    async fn delete_execution_hands_back_the_resume_signals_it_removed() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let run = weft_core::ExecutionId::new_v4();
        j.record_event(&started(run, PROJECT)).await.unwrap();
        let mut parked = registration("tok-form");
        parked.execution_id = Some(run);
        parked.is_resume = true;
        j.signal_insert(&parked).await.unwrap();
        j.signal_insert(&registration("tok-entry")).await.unwrap();

        let removed = j.delete_execution(run).await.unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].token, "tok-form");
        assert!(j.signal_get("tok-form").await.unwrap().is_none());
        assert!(j.signal_get("tok-entry").await.unwrap().is_some(), "entry rows stay");
        assert!(j.delete_execution(run).await.unwrap().is_empty(), "a second erase has nothing left");
    }

    /// The kind_state compare-and-set mirrors Postgres: a registration
    /// lands only at the version it read and moves the row one past, so
    /// a claim at the old version (or a registration that read before a
    /// claim) finds the row moved.
    #[tokio::test]
    async fn signal_insert_is_a_compare_and_set_on_the_kind_state_version() {
        use crate::journal::SignalWrite;
        let j = FakeJournal::new();
        let mut first = registration("tok-1");
        first.kind_state = serde_json::json!({ "cursor": 10 });
        first.kind_state_seq = 0;
        assert_eq!(j.signal_insert(&first).await.unwrap(), SignalWrite::Written);
        assert_eq!(j.signal_get("tok-1").await.unwrap().unwrap().kind_state_seq, 1);

        // Read at 0, but the row is at 1 now: nothing is written.
        let mut stale = registration("tok-1");
        stale.kind_state = serde_json::json!({ "cursor": 3 });
        stale.kind_state_seq = 0;
        assert_eq!(j.signal_insert(&stale).await.unwrap(), SignalWrite::StateMoved);
        let row = j.signal_get("tok-1").await.unwrap().unwrap();
        assert_eq!((row.kind_state, row.kind_state_seq), (serde_json::json!({ "cursor": 10 }), 1));

        // Read at the current version: it lands and moves the row past it.
        let mut current = registration("tok-1");
        current.kind_state = serde_json::json!({ "cursor": 12 });
        current.kind_state_seq = 1;
        assert_eq!(j.signal_insert(&current).await.unwrap(), SignalWrite::Written);
        let row = j.signal_get("tok-1").await.unwrap().unwrap();
        assert_eq!((row.kind_state, row.kind_state_seq), (serde_json::json!({ "cursor": 12 }), 2));
    }

    /// A node test's run is a `node_test` row, and the project-lifecycle
    /// reads (`list_non_terminal_execution_ids_for_project`) skip it,
    /// exactly like the Postgres `kind = 'execution'` filter.
    #[tokio::test]
    async fn node_test_execution_ids_stay_out_of_lifecycle_reads() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let run = weft_core::ExecutionId::new_v4();
        let test = weft_core::ExecutionId::new_v4();
        j.record_event(&started(run, PROJECT)).await.unwrap();
        j.record_event(&ExecEvent::ExecutionStarted {
            execution_id: test,
            project_id: PROJECT,
            entry_node: "node-test:MyNode::t".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: None,
            binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::NodeTest,
            selection: None,
            seed: None,
            instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        })
        .await
        .unwrap();
        let live = j.going_execution_ids_for_project(PROJECT).await.unwrap().into_iter().map(|(execution_id, _)| execution_id).collect::<Vec<_>>();
        assert_eq!(live, vec![run], "the node-test execution never counts as a project run");
    }

    fn started(execution_id: weft_core::ExecutionId, project_id: uuid::Uuid) -> ExecEvent {
        ExecEvent::ExecutionStarted {
            execution_id,
            project_id,
            entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
            selection: None,
            seed: None,
            instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }
    }

    /// A run that ended is settled and not going; a run going is not
    /// settled.
    #[tokio::test]
    async fn going_and_settled_runs_split_the_project() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let done = uuid::Uuid::new_v4();
        let live = uuid::Uuid::new_v4();
        j.record_event(&started(done, PROJECT)).await.unwrap();
        j.record_event(&started(live, PROJECT)).await.unwrap();
        j.record_event(&ExecEvent::ExecutionCompleted { execution_id: done, at_unix: 1 }).await.unwrap();

        let settled = j.settled_execution_ids_for_project(PROJECT).await.unwrap();
        let going = j.going_execution_ids_for_project(PROJECT).await.unwrap().into_iter().map(|(execution_id, _)| execution_id).collect::<Vec<_>>();

        assert!(settled.contains(&done) && !settled.contains(&live));
        assert_eq!(going, vec![live]);
    }

    /// A run's owner is read off its row, written with its project's tenant
    /// when it was born, and an unknown run has none.
    #[tokio::test]
    async fn execution_owner_reads_the_seeded_row() {
        let j = FakeJournal::new();
        let execution_id = weft_core::ExecutionId::new_v4();
        j.set_project_tenant(PROJECT, "tenant-x");
        j.record_event(&started(execution_id, PROJECT)).await.unwrap();

        let owner = j.execution_owner(execution_id).await.unwrap().expect("owner");
        assert_eq!(owner.tenant, "tenant-x");
        assert_eq!(owner.project_id, PROJECT);
        assert!(j.execution_owner(weft_core::ExecutionId::new_v4()).await.unwrap().is_none());
    }

    fn started_at(execution_id: weft_core::ExecutionId, project_id: uuid::Uuid, at_unix: u64) -> ExecEvent {
        ExecEvent::ExecutionStarted {
            execution_id,
            project_id,
            entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
            selection: None,
            seed: None,
            instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix,
            settings: Default::default(),
        }
    }

    fn setup_program() -> weft_core::project::hash::ProgramIdentity {
        weft_core::project::hash::ProgramIdentity {
            definition_hash: "h".into(), binary_hash: "binary".into(), implementations: Default::default(),
        }
    }

    fn setup_events(execution_id: ExecutionId, ports: serde_json::Value) -> Vec<ExecEvent> {
        let mut start = started_at(execution_id, PROJECT, 1);
        if let ExecEvent::ExecutionStarted { phase, binary_hash, source_version, .. } = &mut start {
            *source_version = Some("source".into());
            *phase = weft_core::context::Phase::TriggerSetup;
            *binary_hash = Some("binary".into());
        }
        vec![start, ExecEvent::TriggerCaptured {
            execution_id, node_id: "entry".into(),
            spec: weft_core::primitive::SignalSpec::of_kind("timer", serde_json::json!({"spec":{"kind":"after","duration_ms":100}})),
            port_snapshot: ports, at_unix: 2,
        }, ExecEvent::ExecutionCompleted { execution_id, at_unix: 3 }]
    }

    #[test]
    fn trigger_bake_requires_success_and_preserves_empty_ports() {
        let execution_id = ExecutionId::new_v4();
        let mut events = setup_events(execution_id, serde_json::json!({}));
        let bake = super::super::TriggerBake::from_events(&events, &setup_program()).unwrap().unwrap();
        assert_eq!(bake.captured["entry"].ports, serde_json::json!({}));
        let roundtrip: super::super::TriggerBake = serde_json::from_value(serde_json::to_value(&bake).unwrap()).unwrap();
        assert_eq!(roundtrip.program, bake.program);
        events.insert(2, events[1].clone());
        assert!(super::super::TriggerBake::from_events(&events, &setup_program()).is_err());
        events.remove(2);
        events.pop();
        assert!(super::super::TriggerBake::from_events(&events, &setup_program()).is_err());
        events.push(ExecEvent::ExecutionFailed { execution_id, error: "failed".into(), at_unix: 3 });
        assert!(super::super::TriggerBake::from_events(&events, &setup_program()).unwrap().is_none());
    }

    #[tokio::test]
    async fn trigger_bake_publication_is_owned_atomic_and_retained_after_clean() {
        let journal = FakeJournal::new();
        let first = ExecutionId::new_v4();
        let bake = super::super::TriggerBake::from_events(&setup_events(first, serde_json::json!({"x":1})), &setup_program()).unwrap().unwrap();
        journal.inner.lock().unwrap().trigger_setups.insert(first, PROJECT);
        journal.finish_trigger_setup(first, Some(&bake)).await.unwrap();
        assert!(!journal.is_trigger_setup_pending(first).await.unwrap());
        assert!(journal.signal_list_for_project(PROJECT).await.unwrap().is_empty());
        let second = ExecutionId::new_v4();
        journal.inner.lock().unwrap().trigger_setups.insert(second, PROJECT);
        journal.finish_trigger_setup(second, None).await.unwrap();
        assert_eq!(journal.trigger_bakes(PROJECT, None).await.unwrap()[0].execution_id, first);
        assert!(journal.trigger_bakes(OTHER_PROJECT, None).await.unwrap().is_empty());
        let mut refresh = bake.clone();
        refresh.execution_id = second;
        refresh.targets = refresh.captured.keys().cloned().collect();
        refresh.captured.clear();
        journal.inner.lock().unwrap().trigger_setups.insert(second, PROJECT);
        journal.finish_trigger_setup(second, Some(&refresh)).await.unwrap();
        journal.finish_trigger_setup(first, Some(&bake)).await.unwrap();
        journal.delete_execution(second).await.unwrap();
        let saved = journal.trigger_bakes(PROJECT, None).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].execution_id, second);
        assert!(saved[0].captured.is_empty(), "a skipped target cannot keep its old registration");
    }

    /// `execution_summary` reads one run's row and says how it ended.
    #[tokio::test]
    async fn execution_summary_is_a_direct_point_lookup() {
        let j = FakeJournal::new();
        let c = weft_core::ExecutionId::new_v4();
        j.set_project_tenant(PROJECT, "t");
        j.record_event(&started_at(c, PROJECT, 100)).await.unwrap();
        j.record_event(&ExecEvent::ExecutionCompleted { execution_id: c, at_unix: 150 })
            .await
            .unwrap();

        let s = j.execution_summary(c).await.unwrap().expect("found by execution");
        assert_eq!(s.execution_id, c);
        assert_eq!(s.status.as_str(), "completed");
        assert_eq!(s.completed_at, Some(150));
        assert!(j.execution_summary(weft_core::ExecutionId::new_v4()).await.unwrap().is_none());
    }

    /// `list_executions` pages (limit/offset, newest first), reports the true
    /// total, and filters by project + start-time range, all inside the tenant
    /// wall.
    #[tokio::test]
    async fn list_executions_pages_and_filters() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT_A, "t");
        j.set_project_tenant(PROJECT_B, "t");
        j.set_project_tenant(PROJECT_C, "t2");
        // Three in project pa at t=10/20/30, one in pb at t=25, one in another tenant.
        let a1 = weft_core::ExecutionId::new_v4();
        let a2 = weft_core::ExecutionId::new_v4();
        let a3 = weft_core::ExecutionId::new_v4();
        let b1 = weft_core::ExecutionId::new_v4();
        let x1 = weft_core::ExecutionId::new_v4();
        j.record_event(&started_at(a1, PROJECT_A, 10)).await.unwrap();
        j.record_event(&started_at(a2, PROJECT_A, 20)).await.unwrap();
        j.record_event(&started_at(a3, PROJECT_A, 30)).await.unwrap();
        j.record_event(&started_at(b1, PROJECT_B, 25)).await.unwrap();
        j.record_event(&started_at(x1, PROJECT_C, 40)).await.unwrap();

        // Tenant t: 4 executions, newest first, page of 2.
        let q = ExecutionQuery { limit: 2, offset: 0, ..Default::default() };
        let page = j.list_executions("t", &q).await.unwrap();
        assert_eq!(page.total, 4, "count ignores paging");
        assert_eq!(page.executions.len(), 2);
        assert_eq!(page.executions[0].started_at, 30, "newest first");
        assert_eq!(page.executions[1].started_at, 25);
        // Next page.
        let q2 = ExecutionQuery { limit: 2, offset: 2, ..Default::default() };
        let page2 = j.list_executions("t", &q2).await.unwrap();
        assert_eq!(page2.executions.len(), 2);
        assert_eq!(page2.executions[0].started_at, 20);

        // Project filter: only pa's three.
        let qp = ExecutionQuery { limit: 50, project_id: Some(PROJECT_A), ..Default::default() };
        let pagep = j.list_executions("t", &qp).await.unwrap();
        assert_eq!(pagep.total, 3);
        assert!(pagep.executions.iter().all(|e| e.project_id == PROJECT_A));

        // Date filter: started_after=15, started_before=30 (exclusive) -> t=20,25.
        let qd = ExecutionQuery {
            limit: 50,
            started_after: Some(15),
            started_before: Some(30),
            ..Default::default()
        };
        let paged = j.list_executions("t", &qd).await.unwrap();
        assert_eq!(paged.total, 2);
        assert!(paged.executions.iter().all(|e| e.started_at >= 15 && e.started_at < 30));

        // Tenant wall: t2 sees only its one, never t's.
        let qt2 = ExecutionQuery { limit: 50, ..Default::default() };
        let paget2 = j.list_executions("t2", &qt2).await.unwrap();
        assert_eq!(paget2.total, 1);
        assert_eq!(paget2.executions[0].execution_id, x1);
    }

    /// The filter that answers "where is MY run": in a project whose
    /// triggers are all answering at once, the entry node is the only
    /// thing that tells one run from the thousands beside it.
    #[tokio::test]
    async fn list_executions_filters_by_entry_node() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let mine = weft_core::ExecutionId::new_v4();
        let noise = weft_core::ExecutionId::new_v4();
        let mut start = started_at(mine, PROJECT, 10);
        if let ExecEvent::ExecutionStarted { entry_node, .. } = &mut start {
            *entry_node = "cards.post".into();
        }
        j.record_event(&start).await.unwrap();
        j.record_event(&started_at(noise, PROJECT, 20)).await.unwrap();

        let q = ExecutionQuery {
            limit: 50,
            entry_node: Some("cards.post".into()),
            ..Default::default()
        };
        let page = j.list_executions("t", &q).await.unwrap();
        assert_eq!(page.total, 1, "the count matches the filter, not the whole history");
        assert_eq!(page.executions[0].execution_id, mine);

        // A node nothing started by is an empty answer, never everything.
        let none = ExecutionQuery {
            limit: 50,
            entry_node: Some("nobody".into()),
            ..Default::default()
        };
        assert_eq!(j.list_executions("t", &none).await.unwrap().total, 0);
    }

    /// "Which of mine broke": a run's status is which terminal event it
    /// ended on, and `running` is the absence of one, so the filter has
    /// to answer both shapes.
    #[tokio::test]
    async fn list_executions_filters_by_status() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let broke = weft_core::ExecutionId::new_v4();
        let fine = weft_core::ExecutionId::new_v4();
        let going = weft_core::ExecutionId::new_v4();
        j.record_event(&started_at(broke, PROJECT, 10)).await.unwrap();
        j.record_event(&ExecEvent::ExecutionFailed {
            execution_id: broke,
            error: "boom".into(),
            at_unix: 11,
        })
        .await
        .unwrap();
        j.record_event(&started_at(fine, PROJECT, 20)).await.unwrap();
        j.record_event(&ExecEvent::ExecutionCompleted { execution_id: fine, at_unix: 21 }).await.unwrap();
        j.record_event(&started_at(going, PROJECT, 30)).await.unwrap();

        let of = |status: &str| ExecutionQuery {
            limit: 50,
            status: Some(weft_core::program::RunStatus::parse(status).expect("a run status")),
            ..Default::default()
        };
        let failed = j.list_executions("t", &of("failed")).await.unwrap();
        assert_eq!(failed.total, 1);
        assert_eq!(failed.executions[0].execution_id, broke);

        let completed = j.list_executions("t", &of("completed")).await.unwrap();
        assert_eq!(completed.executions[0].execution_id, fine);

        let running = j.list_executions("t", &of("running")).await.unwrap();
        assert_eq!(running.total, 1, "a run with no terminal event is still going");
        assert_eq!(running.executions[0].execution_id, going);
    }

    /// A run parked on a wait is still running, and is the only one
    /// `waiting_for_input` reaches; a finished run is reached by its end.
    #[tokio::test]
    async fn the_status_filter_reaches_runs_as_the_listing_reads_them() {
        use weft_core::program::RunStatus;
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let (going, parked, done) = (ExecutionId::new_v4(), ExecutionId::new_v4(), ExecutionId::new_v4());
        for execution_id in [going, parked, done] {
            j.record_event(&started_at(execution_id, PROJECT, 10)).await.unwrap();
        }
        j.record_event(&ExecEvent::ExecutionCancelled { execution_id: done, reason: "stop".into(), cause: None, at_unix: 11 }).await.unwrap();
        j.park(parked);
        let reached = |status: RunStatus| {
            let q = ExecutionQuery { limit: 10, status: Some(status), ..Default::default() };
            let j = &j;
            async move {
                let mut ids: Vec<ExecutionId> = j.list_executions("t", &q).await.unwrap().executions.into_iter().map(|s| s.execution_id).collect();
                ids.sort();
                ids
            }
        };
        let sorted = |mut ids: Vec<ExecutionId>| { ids.sort(); ids };
        assert_eq!(reached(RunStatus::Running).await, sorted(vec![going, parked]));
        assert_eq!(reached(RunStatus::WaitingForInput).await, vec![parked]);
        assert_eq!(reached(RunStatus::Cancelled).await, vec![done]);
        assert!(reached(RunStatus::Failed).await.is_empty());
    }

    /// A walk that hands each page's last run back as `below` reaches
    /// every run exactly once, even when the runs it passed leave the
    /// filter behind it (cancelled under `status = running`) and they all
    /// started in the same second.
    #[tokio::test]
    async fn a_keyset_walk_reaches_every_run_once_while_the_walked_ones_leave_the_filter() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let runs: Vec<weft_core::ExecutionId> = (0..5).map(|_| weft_core::ExecutionId::new_v4()).collect();
        for execution_id in &runs {
            j.record_event(&started_at(*execution_id, PROJECT, 10)).await.unwrap();
        }
        let mut reached = Vec::new();
        let mut below = None;
        loop {
            let q = ExecutionQuery { limit: 2, status: Some(weft_core::program::RunStatus::Running), below, ..Default::default() };
            let page = j.list_executions("t", &q).await.unwrap();
            let Some(last) = page.executions.last() else { break };
            below = Some((last.started_at, last.execution_id));
            for run in page.executions {
                j.record_event(&ExecEvent::ExecutionCancelled { execution_id: run.execution_id, reason: "cleaned".into(), cause: None, at_unix: 11 }).await.unwrap();
                reached.push(run.execution_id);
            }
        }
        reached.sort();
        let mut expected = runs;
        expected.sort();
        assert_eq!(reached, expected);
    }

    /// The phase filter separates a trigger's real fires from the
    /// setup runs an activate makes, and the summary carries the phase
    /// so a listing can say which is which.
    #[tokio::test]
    async fn list_executions_filters_by_phase() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let fire = weft_core::ExecutionId::new_v4();
        let setup = weft_core::ExecutionId::new_v4();
        j.record_event(&started_at(fire, PROJECT, 10)).await.unwrap();
        let ExecEvent::ExecutionStarted { execution_id, project_id, entry_node, definition_hash, run_kind, selection, seed, at_unix, .. } =
            started_at(setup, PROJECT, 20)
        else {
            unreachable!()
        };
        j.record_event(&ExecEvent::ExecutionStarted {
            execution_id,
            project_id,
            entry_node,
            phase: weft_core::context::Phase::TriggerSetup,
            definition_hash,
            binary_hash: None,
            run_kind,
            source_version: None,
            selection,
            seed,
            instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix,
            settings: Default::default(),
        })
        .await
        .unwrap();

        let all = j.list_executions("t", &ExecutionQuery { limit: 10, ..Default::default() }).await.unwrap();
        assert_eq!(all.total, 2);
        assert_eq!(all.executions[0].phase, weft_core::context::Phase::TriggerSetup, "newest first");
        let fires = j
            .list_executions(
                "t",
                &ExecutionQuery { limit: 10, phase: Some(weft_core::context::Phase::Fire), ..Default::default() },
            )
            .await
            .unwrap();
        assert_eq!(fires.total, 1);
        assert_eq!(fires.executions[0].execution_id, fire);
    }

    /// A program's `ctx.runs()` filter reads as one journal query for
    /// clean, list and count alike: "older than" keeps a run started
    /// exactly that long ago, a count asks for no rows and still gets the
    /// total, and a run started after the question is never reached.
    #[tokio::test]
    async fn a_run_filter_reads_the_same_for_every_door() {
        let j = FakeJournal::new();
        j.set_project_tenant(PROJECT, "t");
        let old = weft_core::ExecutionId::new_v4();
        let edge = weft_core::ExecutionId::new_v4();
        let fresh = weft_core::ExecutionId::new_v4();
        let later = weft_core::ExecutionId::new_v4();
        j.record_event(&started_at(old, PROJECT, 10)).await.unwrap();
        j.record_event(&started_at(edge, PROJECT, 40)).await.unwrap();
        j.record_event(&started_at(fresh, PROJECT, 90)).await.unwrap();
        j.record_event(&started_at(later, PROJECT, 101)).await.unwrap();
        j.tag_execution(old, &["draft"], 11);
        j.tag_execution(edge, &["draft"], 41);

        let now = 100;
        let older = weft_core::program::RunFilter { older_than_secs: Some(60), ..Default::default() };
        let page = j.list_executions("t", &crate::api::execution::run_query(Some(PROJECT), &older, 50, now)).await.unwrap();
        let ids: Vec<_> = page.executions.iter().map(|e| e.execution_id).collect();
        assert_eq!(ids, [edge, old], "started 60s ago counts as at least 60s old");

        let all = weft_core::program::RunFilter::default();
        let page = j.list_executions("t", &crate::api::execution::run_query(Some(PROJECT), &all, 1, now)).await.unwrap();
        assert_eq!((page.total, page.executions[0].execution_id), (3, fresh), "newest first, nothing after the question");

        let tagged = weft_core::program::RunFilter { tag: Some("draft".into()), ..Default::default() };
        let count = j.list_executions("t", &crate::api::execution::run_query(Some(PROJECT), &tagged, 0, now)).await.unwrap();
        assert_eq!((count.total, count.executions.len()), (2, 0), "a count reads the total and no rows");
    }
}

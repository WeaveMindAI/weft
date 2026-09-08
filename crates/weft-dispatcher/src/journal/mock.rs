//! In-memory `Journal` implementation for tests. Mirrors the
//! Postgres semantics: append-only event log + token lookup tables.
//! Compiled only under `cfg(test)` and behind the `test-helpers`
//! feature so dependent crates can pull it in for their own tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use weft_core::Color;

use weft_journal::ExecEvent;
use crate::journal::{
    SignalToken, ColorLookup, ExecutionOwner, ExecutionPage, ExecutionQuery, ExecutionSummary,
    Journal, LogEntry, SignalRegistration,
};

#[derive(Default)]
struct MockState {
    events: Vec<ExecEvent>,
    signal_tokens: HashMap<String, SignalToken>,
    /// One entry per `signal` row: the row (its holder on
    /// `listener_pod`, as the column) plus the placement generation the
    /// insert stamped, so the two die together exactly as one Postgres
    /// row does and no path can leave a placement behind a deleted row.
    signals: HashMap<String, StoredSignal>,
    dedup_keys: std::collections::HashSet<String>,
    /// Mirror of the Postgres `execution_color` denormalization:
    /// seeded on `ExecutionStarted` with `(project_id, tenant_id)`,
    /// cleared on `delete_execution`. The tenant is derived from
    /// `project_tenants` at seed time, exactly as Postgres reads it from
    /// the `project` table. Tests that exercise
    /// `list_non_terminal_colors_for_project`, `delete_execution`
    /// cleanup, or tenant-scoped `list_executions` depend on this
    /// matching real-DB semantics.
    execution_colors: HashMap<Color, ExecutionColorRow>,
    /// project_id -> tenant_id, mirroring the `project` table the Postgres
    /// seed reads. Tests register a project's tenant here (via
    /// `set_project_tenant`) so the execution_color seed stamps the right
    /// tenant. An unset project defaults to `local`.
    project_tenants: HashMap<String, String>,
    /// The work items the atomic-birth writers committed with each execution
    /// (mirror of the `task` rows `start_execution` / `start_live_execution`
    /// insert). Append-only record for assertions; `cancel_never_claimed_
    /// execution` removes the matching entry exactly like the real DELETE.
    tasks: Vec<weft_task_store::tasks::NewTask>,
    /// Mirror of the Postgres `execution_tag` table: (color, tag) ->
    /// seq, seq handed out in write order like the real BIGSERIAL, an
    /// existing pair keeping its seq like the real ON CONFLICT.
    execution_tags: HashMap<(Color, String), i64>,
    next_tag_seq: i64,
}

/// A mock `signal` row: the registration as a read hands it back, and
/// the `placement_generation` column beside it.
struct StoredSignal {
    row: SignalRegistration,
    placement_generation: i64,
}

#[derive(Default)]
pub struct MockJournal {
    inner: Mutex<MockState>,
}

impl MockJournal {
    pub fn new() -> Self {
        Self::default()
    }

    /// The work items the atomic-birth writers committed (see
    /// `MockState::tasks`), for test assertions.
    pub fn enqueued_tasks(&self) -> Vec<weft_task_store::tasks::NewTask> {
        self.inner.lock().unwrap().tasks.clone()
    }

    /// Tag an execution the way the broker's `/v1/execution/tag` does
    /// against Postgres: the `ExecutionTagged` event plus one
    /// `execution_tag` row per tag, a re-tag keeping the original seq.
    /// Returns the seq of the FIRST tag in `tags` (the anchor a `Keep`
    /// stop by this run would use).
    pub fn tag_execution(&self, color: Color, tags: &[&str], at_unix: u64) -> i64 {
        let mut g = self.inner.lock().unwrap();
        g.events.push(ExecEvent::ExecutionTagged {
            color,
            tags: tags.iter().map(|t| t.to_string()).collect(),
            at_unix,
        });
        let mut first = None;
        for tag in tags {
            let key = (color, tag.to_string());
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

    /// Build the `ExecutionSummary` for one color from the recorded events (the
    /// started row plus its latest terminal event), or `None` if there is no
    /// `execution_started` for it. Shared by the tenant listing + the by-color
    /// lookup so the status-fold lives in one place, mirroring the Postgres
    /// `summary_from_payloads` helper.
    fn summary_for_color(&self, color: Color) -> Option<ExecutionSummary> {
        let g = self.inner.lock().unwrap();
        let (project_id, entry_node, phase, started_at) = g.events.iter().find_map(|e| match e {
            ExecEvent::ExecutionStarted { color: c, project_id, entry_node, phase, at_unix, .. }
                if *c == color =>
            {
                Some((project_id.clone(), entry_node.clone(), *phase, *at_unix))
            }
            _ => None,
        })?;
        let mut status = "running".to_string();
        let mut completed_at = None;
        for tail in g.events.iter().filter(|e| e.color() == color) {
            match tail {
                ExecEvent::ExecutionCompleted { at_unix, .. } => {
                    status = "completed".into();
                    completed_at = Some(*at_unix);
                }
                ExecEvent::ExecutionFailed { at_unix, .. } => {
                    status = "failed".into();
                    completed_at = Some(*at_unix);
                }
                ExecEvent::ExecutionCancelled { at_unix, .. } => {
                    status = "cancelled".into();
                    completed_at = Some(*at_unix);
                }
                _ => {}
            }
        }
        let mut tagged: Vec<(i64, String)> = g
            .execution_tags
            .iter()
            .filter(|((c, _), _)| *c == color)
            .map(|((_, tag), seq)| (*seq, tag.clone()))
            .collect();
        tagged.sort();
        let tags = tagged.into_iter().map(|(_, tag)| tag).collect();
        Some(ExecutionSummary { color, project_id, entry_node, status, phase, started_at, completed_at, tags })
    }

    /// Every execution summary owned by `tenant` (unordered). Tenant ownership
    /// mirrors the Postgres join on `execution_color.tenant_id`.
    fn tenant_summaries(&self, tenant: &str) -> Vec<ExecutionSummary> {
        let colors: Vec<Color> = {
            let g = self.inner.lock().unwrap();
            g.events
                .iter()
                .filter_map(|e| match e {
                    ExecEvent::ExecutionStarted { color, .. } => {
                        // Same tenant + kind filter as the Postgres
                        // listing: node-test colors never enumerate.
                        let row = g.execution_colors.get(color);
                        (row.is_some_and(|r| r.tenant_id == tenant && r.kind == "execution"))
                            .then_some(*color)
                    }
                    _ => None,
                })
                .collect()
        };
        colors.into_iter().filter_map(|c| self.summary_for_color(c)).collect()
    }

    /// The placement (holder pod + generation) `signal_insert` recorded
    /// for `token`, or `None` if no signal was inserted under it. Lets a
    /// test assert the placement-born-with-row invariant: the holder is
    /// stamped WITH the row, never left NULL for a later write.
    pub fn signal_placement(&self, token: &str) -> Option<crate::journal::SignalPlacement> {
        let g = self.inner.lock().unwrap();
        let stored = g.signals.get(token)?;
        Some(crate::journal::SignalPlacement {
            listener_pod: stored
                .row
                .listener_pod
                .clone()
                .expect("the mock stamps every stored row with its holder"),
            generation: stored.placement_generation,
        })
    }

    /// Register a project's owning tenant, mirroring the `project` table the
    /// Postgres `execution_color` seed reads `tenant_id` from. A test that
    /// exercises tenant-scoped `list_executions` calls this for each project so
    /// the execution_color seed stamps the right tenant; unset projects seed as
    /// `local`.
    pub fn set_project_tenant(&self, project_id: &str, tenant: &str) {
        self.inner
            .lock()
            .unwrap()
            .project_tenants
            .insert(project_id.to_string(), tenant.to_string());
    }
}

/// Seed the `execution_color` mirror for a started execution, stamping the
/// project's tenant (from `project_tenants`) exactly as the Postgres seed reads it
/// from the `project` table via a JOIN. Idempotent on color.
///
/// Mirrors Postgres's REFUSAL: `record_with_seed` bails when the `ExecutionStarted`
/// project has no `project` row (the JOIN finds no tenant). So an unregistered
/// project is an error here too, NOT a silent `local` default, otherwise a test
/// that starts an execution for a project it never registered would pass on the
/// mock while the identical sequence 500s in production. Register the project's
/// tenant first via `set_project_tenant`.
/// One `execution_color` row's mirror. A named struct (not a tuple)
/// so adding a column is a compile error at every read site instead
/// of a silently-unread field.
#[derive(Clone)]
struct ExecutionColorRow {
    project_id: String,
    tenant_id: String,
    /// 'execution' | 'node_test', mirroring the Postgres `kind`
    /// column: the lifecycle sweeps read only 'execution'.
    kind: &'static str,
    /// The run's phase (`fire`, `trigger_setup`, `infra_setup`),
    /// mirroring the Postgres `phase` column the activation sweep
    /// narrows on.
    phase: &'static str,
}

fn seed_execution_color(
    state: &mut MockState,
    color: Color,
    project_id: &str,
    node_test: bool,
    phase: weft_core::context::Phase,
) -> anyhow::Result<()> {
    // Already-seeded first, EXACTLY like Postgres: an idempotent
    // re-ExecutionStarted for a seeded color succeeds even if the project row
    // has since vanished (the real seed's not-already-seeded guard
    // short-circuits the project lookup).
    if state.execution_colors.contains_key(&color) {
        return Ok(());
    }
    let tenant = state.project_tenants.get(project_id).cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "refuse to journal ExecutionStarted for project {project_id}: no tenant \
             registered (call set_project_tenant first); Postgres fails the same way \
             when the project has no row"
        )
    })?;
    state.execution_colors.insert(
        color,
        ExecutionColorRow {
            project_id: project_id.to_string(),
            tenant_id: tenant,
            kind: if node_test { "node_test" } else { "execution" },
            phase: phase.as_str(),
        },
    );
    Ok(())
}

#[async_trait]
impl Journal for MockJournal {
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        if let ExecEvent::ExecutionStarted { color, project_id, node_test, phase, .. } = event {
            seed_execution_color(&mut g, *color, project_id, *node_test, *phase)?;
        }
        g.events.push(event.clone());
        Ok(())
    }

    async fn record_event_dedup(
        &self,
        event: &ExecEvent,
        dedup_key: &str,
    ) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        if g.dedup_keys.insert(dedup_key.to_string()) {
            if let ExecEvent::ExecutionStarted { color, project_id, node_test, phase, .. } = event {
                seed_execution_color(&mut g, *color, project_id, *node_test, *phase)?;
            }
            g.events.push(event.clone());
        }
        Ok(())
    }

    async fn start_execution(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        let ExecEvent::ExecutionStarted { color, project_id, node_test, phase, .. } = start else {
            anyhow::bail!("start_execution requires an ExecutionStarted event");
        };
        seed_execution_color(&mut g, *color, project_id, *node_test, *phase)?;
        g.events.push(start.clone());
        g.events.extend(kicks.iter().cloned());
        g.tasks.push(task);
        Ok(())
    }

    async fn start_live_execution(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        mut task: weft_task_store::tasks::NewTask,
        _saturation: f64,
    ) -> anyhow::Result<weft_task_store::tasks::LiveAdmitOutcome> {
        // Dumb: one always-admittable pod. Pins the task exactly like the real
        // insert does, so assertions see the pin.
        let pod = weft_task_store::tasks::AdmittedPod {
            pod_name: "mock-worker-0".into(),
            namespace: "mock-ns".into(),
        };
        let mut g = self.inner.lock().unwrap();
        let ExecEvent::ExecutionStarted { color, project_id, node_test, phase, .. } = start else {
            anyhow::bail!("start_live_execution requires an ExecutionStarted event");
        };
        seed_execution_color(&mut g, *color, project_id, *node_test, *phase)?;
        g.events.push(start.clone());
        g.events.extend(kicks.iter().cloned());
        task.target_pod_name = Some(pod.pod_name.clone());
        g.tasks.push(task);
        Ok(weft_task_store::tasks::LiveAdmitOutcome::Admitted(pod))
    }

    async fn cancel_never_claimed_execution(
        &self,
        color: Color,
        _program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<weft_task_store::tasks::SetupFailureOutcome> {
        // Dumb: the mock has no 'claimed' state, so the outcome is always
        // NoWorkerWillRun: drop the recorded task and append the terminal
        // (idempotent, exactly like the real dedup'd write).
        let mut g = self.inner.lock().unwrap();
        let color_str = color.to_string();
        g.tasks.retain(|t| t.color.as_deref() != Some(color_str.as_str()));
        let has_terminal = g.events.iter().any(|e| {
            e.color() == color && e.is_execution_terminal()
        });
        if !has_terminal {
            g.events.push(ExecEvent::ExecutionCancelled {
                color,
                reason: cause.to_string(),
                cause: Some(cause.clone()),
                at_unix: 0,
            });
        }
        Ok(weft_task_store::tasks::SetupFailureOutcome::NoWorkerWillRun)
    }

    async fn cancel_execution(
        &self,
        color: Color,
        _program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<crate::journal::CancelWrite> {
        let mut g = self.inner.lock().unwrap();
        // The strip is `signal_remove_for_color`'s predicate: every
        // signal tied to the color.
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.row.color == Some(color))
            .map(|(k, _)| k.clone())
            .collect();
        let removed: Vec<SignalRegistration> =
            keys.into_iter().filter_map(|k| g.signals.remove(&k).map(|s| s.row)).collect();
        let mut write = crate::journal::CancelWrite { removed, ..Default::default() };
        if g.execution_colors.contains_key(&color) {
            let has_terminal = g.events.iter().any(|e| {
                e.color() == color && e.is_execution_terminal()
            });
            if !has_terminal {
                // Same fidelity as `cancel_never_claimed_execution`: the
                // terminal row, no per-node rows (the mock folds no nodes).
                g.events.push(ExecEvent::ExecutionCancelled {
                    color,
                    reason: cause.to_string(),
                    cause: Some(cause.clone()),
                    at_unix: 0,
                });
                write.node_cancellations = Some(0);
            }
            // The mock has no worker pods, so no color has an alive
            // owner and no cancel task is ever queued.
        }
        Ok(write)
    }

    async fn events_log_lossy(
        &self,
        color: Color,
    ) -> anyhow::Result<(Vec<crate::events::IdentifiedEvent<ExecEvent>>, Vec<String>)> {
        // In-memory events are typed, so nothing can fail to decode.
        let events = self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.color() == color)
            .enumerate()
            .map(|(index, event)| crate::events::IdentifiedEvent {
                event_id: format!("mock:{color}:{index}"), event: event.clone(),
            })
            .collect();
        Ok((events, Vec::new()))
    }

    async fn consume_suspension(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        // Mirror the postgres impl: drop the signal row for a
        // single-use resume token. Entry-trigger rows stay.
        let mut g = self.inner.lock().unwrap();
        match g.signals.get(token) {
            Some(s) if s.row.is_resume => Ok(g.signals.remove(token).map(|s| s.row)),
            _ => Ok(None),
        }
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

    async fn execution_owner(&self, color: Color) -> anyhow::Result<Option<ExecutionOwner>> {
        // Read off the `execution_colors` mirror, exactly like
        // Postgres: ownership must resolve when the started event is
        // unusable AND when the project row is gone, or `weft clean`
        // could never authorize the rows that need it most.
        Ok(self.inner.lock().unwrap().execution_colors.get(&color).map(|r| ExecutionOwner {
            project_id: r.project_id.clone(),
            tenant: r.tenant_id.clone(),
        }))
    }

    async fn execution_definition_hash(
        &self,
        color: Color,
    ) -> anyhow::Result<ColorLookup<String>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .find_map(|e| match e {
                // A definition-less start (a node self-test) answers
                // NotFound, mirroring the postgres impl.
                ExecEvent::ExecutionStarted { color: c, definition_hash, .. } if *c == color => {
                    definition_hash.clone()
                }
                _ => None,
            })
            .map_or(ColorLookup::NotFound, ColorLookup::Found))
    }

    async fn logs_for(&self, color: Color, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        // The same tail as Postgres, through the one `LogEntry::tail`:
        // the two journals have to answer `weft logs` the same way or
        // nothing tested here means anything about the real one.
        let entries: Vec<LogEntry> = self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.color() == color)
            .filter_map(LogEntry::from_event)
            .collect();
        Ok(LogEntry::tail(entries, limit))
    }

    async fn list_executions(
        &self,
        tenant: &str,
        query: &ExecutionQuery,
    ) -> anyhow::Result<ExecutionPage> {
        // Every summary for this tenant, newest first, then apply the same
        // project + start-time filters the Postgres query does, then page.
        let mut all: Vec<ExecutionSummary> = self
            .tenant_summaries(tenant)
            .into_iter()
            .filter(|s| query.project_id.as_deref().is_none_or(|p| s.project_id == p))
            .filter(|s| query.started_after.is_none_or(|a| s.started_at >= a))
            .filter(|s| query.started_before.is_none_or(|b| s.started_at < b))
            .filter(|s| query.phase.is_none_or(|p| s.phase == p))
            .collect();
        all.sort_by(|a, b| b.started_at.cmp(&a.started_at).then(b.color.cmp(&a.color)));
        let total = all.len() as u64;
        let executions = all
            .into_iter()
            .skip(query.offset as usize)
            .take(query.limit as usize)
            .collect();
        Ok(ExecutionPage { executions, total })
    }

    async fn execution_summary(
        &self,
        color: Color,
    ) -> anyhow::Result<Option<ExecutionSummary>> {
        Ok(self.summary_for_color(color))
    }

    async fn colors_with_prefix(&self, tenant: &str, prefix: &str) -> anyhow::Result<Vec<Color>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<Color> = g
            .execution_colors
            .iter()
            .filter(|(c, row)| {
                row.tenant_id == tenant && row.kind == "execution" && c.to_string().starts_with(prefix)
            })
            .map(|(c, _)| *c)
            .collect();
        out.sort();
        out.truncate(2);
        Ok(out)
    }

    async fn list_non_terminal_colors_for_project(
        &self,
        project_id: &str,
        phase: Option<weft_core::context::Phase>,
    ) -> anyhow::Result<Vec<Color>> {
        // Read from `execution_colors` (mirror of Postgres
        // `execution_color`) instead of scanning `events`. Keeps
        // mock semantics aligned with the real DB: `delete_execution`
        // clears the row so cleaned colors don't keep appearing as
        // non-terminal.
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (color, row) in g.execution_colors.iter() {
            // PROJECT EXECUTIONS only, mirroring the Postgres
            // `kind = 'execution'` filter: a node-test color's
            // lifecycle is owned by its task, never by the project's.
            if row.project_id != project_id || row.kind != "execution" {
                continue;
            }
            if phase.is_some_and(|p| row.phase != p.as_str()) {
                continue;
            }
            let terminal = g.events.iter().any(|e2| {
                e2.color() == *color && e2.is_execution_terminal()
            });
            if !terminal {
                out.push(*color);
            }
        }
        Ok(out)
    }

    async fn list_terminal_colors_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<std::collections::HashSet<Color>> {
        let g = self.inner.lock().unwrap();
        let mut out = std::collections::HashSet::new();
        for (color, row) in g.execution_colors.iter() {
            if row.project_id != project_id {
                continue;
            }
            let terminal = g.events.iter().any(|e2| {
                e2.color() == *color && e2.is_execution_terminal()
            });
            if terminal {
                out.insert(*color);
            }
        }
        Ok(out)
    }

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.events.retain(|e| e.color() != color);
        g.signals.retain(|_, s| s.row.color != Some(color));
        g.execution_colors.remove(&color);
        g.execution_tags.retain(|(c, _), _| *c != color);
        Ok(())
    }

    async fn live_tagged_executions(
        &self,
        project_id: &str,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedExecution>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<weft_journal::tags::TaggedExecution> = g
            .execution_tags
            .iter()
            .filter(|((_, t), _)| t == tag)
            .filter(|((color, _), _)| {
                g.execution_colors
                    .get(color)
                    .is_some_and(|row| row.project_id == project_id && row.kind == "execution")
            })
            .filter(|((color, _), _)| {
                !g.events.iter().any(|e| e.color() == *color && e.is_execution_terminal())
            })
            .map(|((color, _), seq)| weft_journal::tags::TaggedExecution { color: *color, seq: *seq })
            .collect();
        out.sort_by_key(|t| t.seq);
        Ok(out)
    }

    async fn signal_insert(
        &self,
        sig: &SignalRegistration,
        placement: &crate::journal::SignalPlacement,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        // Mirror Postgres's `idx_signal_entry_node` partial-unique on
        // `(project_id, node_id) WHERE is_resume = FALSE`: at most one ENTRY row per
        // node. Postgres reuses the SAME token across reactivates (ON CONFLICT
        // (token) refresh), so a second entry row for the same node under a
        // DIFFERENT token is a unique violation there. Reject it here too, else a
        // mock test could believe a double-registration is fine when production
        // rejects it. Resume rows (per-suspension tokens) are exempt, matching the
        // index's `WHERE is_resume = FALSE`.
        if !sig.is_resume {
            let collides = inner.signals.values().map(|s| &s.row).any(|existing| {
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
        // Mirror the Postgres conflict fence on kind_state: a write
        // carrying an older (lower-seq) state loses to the stored one
        // (a reactivate must never rewind an in-flight cursor write),
        // and the row keeps the higher seq either way.
        let mut fenced = sig.clone();
        // The stored row names its holder, as `signal.listener_pod` does.
        fenced.listener_pod = Some(placement.listener_pod.clone());
        if let Some(existing) = inner.signals.get(&fenced.token).map(|s| &s.row) {
            if existing.kind_state_seq > fenced.kind_state_seq {
                fenced.kind_state = existing.kind_state.clone();
            }
            fenced.kind_state_seq = existing.kind_state_seq.max(fenced.kind_state_seq);
        }
        inner.signals.insert(
            fenced.token.clone(),
            StoredSignal { row: fenced, placement_generation: placement.generation },
        );
        Ok(())
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        Ok(self.inner.lock().unwrap().signals.get(token).map(|s| s.row.clone()))
    }

    async fn signal_update_kind_state(
        &self,
        token: &str,
        kind_state: &serde_json::Value,
        seq: i64,
        placement_generation: i64,
    ) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let Some(stored) = g.signals.get_mut(token) else { return Ok(false) };
        if stored.placement_generation > placement_generation {
            return Ok(false);
        }
        let sig = &mut stored.row;
        if sig.kind_state_seq >= seq {
            return Ok(false);
        }
        sig.kind_state = kind_state.clone();
        sig.kind_state_seq = seq;
        Ok(true)
    }

    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for t in tokens {
            if let Some(stored) = g.signals.remove(t) {
                out.push(stored.row);
            }
        }
        Ok(out)
    }

    async fn signal_list_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .signals
            .values()
            .map(|s| &s.row)
            .filter(|s| s.project_id == project_id)
            .cloned()
            .collect())
    }

    async fn signal_remove_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        // Mirror the postgres predicate EXACTLY (`DELETE ... WHERE color =
        // $1`, `SIGNAL_DELETE_BY_COLOR_RETURNING`): every signal tied to
        // the color goes, whatever its kind, so the mock and the real
        // store cannot diverge the day an entry signal carries a color.
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.row.color == Some(color))
            .map(|(k, _)| k.clone())
            .collect();
        Ok(keys
            .into_iter()
            .filter_map(|k| g.signals.remove(&k).map(|s| s.row))
            .collect())
    }

    async fn signal_remove_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.row.project_id == project_id)
            .map(|(k, _)| k.clone())
            .collect();
        Ok(keys
            .into_iter()
            .filter_map(|k| g.signals.remove(&k).map(|s| s.row))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::SignalPlacement;

    fn registration(token: &str) -> SignalRegistration {
        SignalRegistration {
            token: token.into(),
            tenant_id: "t".into(),
            project_id: "p".into(),
            color: None,
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
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            listener_pod: None,
        }
    }

    /// `signal_insert` records the placement (holder pod + generation)
    /// WITH the signal, never separately: the invariant the
    /// placement-born-with-row fix guarantees (a committed signal always
    /// has a non-NULL holder). The accessor reads back exactly what was
    /// stamped.
    #[tokio::test]
    async fn signal_insert_records_placement_with_the_row() {
        let j = MockJournal::new();
        assert!(j.signal_placement("tok-1").is_none(), "no signal yet");
        j.signal_insert(
            &registration("tok-1"),
            &SignalPlacement { listener_pod: "listener-abc".into(), generation: 3 },
        )
        .await
        .unwrap();
        let placement = j.signal_placement("tok-1").expect("placement recorded with the row");
        assert_eq!(placement.listener_pod, "listener-abc");
        assert_eq!(placement.generation, 3);
        // The signal itself is also present (holder + registration land
        // together, not in separate steps), and a read names the holder.
        let row = j.signal_get("tok-1").await.unwrap().expect("row present");
        assert_eq!(row.listener_pod.as_deref(), Some("listener-abc"));
    }

    /// Consuming a resume token hands back the deleted row WITH its
    /// holder: the row is gone by then, so the unregister that follows
    /// has nothing else to learn the pod from.
    #[tokio::test]
    async fn consume_suspension_hands_back_the_row_and_its_holder() {
        let j = MockJournal::new();
        let mut resume = registration("tok-r");
        resume.is_resume = true;
        j.signal_insert(
            &resume,
            &SignalPlacement { listener_pod: "listener-abc".into(), generation: 1 },
        )
        .await
        .unwrap();
        let consumed = j.consume_suspension("tok-r").await.unwrap().expect("the resume row");
        assert_eq!(consumed.listener_pod.as_deref(), Some("listener-abc"));
        assert!(j.signal_get("tok-r").await.unwrap().is_none(), "single use");
        assert!(j.signal_placement("tok-r").is_none(), "the placement dies with the row");
        assert!(j.consume_suspension("tok-r").await.unwrap().is_none(), "already consumed");

        // An entry row is never consumed this way.
        j.signal_insert(
            &registration("tok-e"),
            &SignalPlacement { listener_pod: "listener-abc".into(), generation: 1 },
        )
        .await
        .unwrap();
        assert!(j.consume_suspension("tok-e").await.unwrap().is_none());
        assert!(j.signal_get("tok-e").await.unwrap().is_some(), "entry rows stay");
    }

    /// The kind_state conflict fence mirrors Postgres: a re-insert
    /// carrying an OLDER seq keeps the newer stored state (a
    /// reactivate can never rewind an in-flight cursor write), and
    /// the row keeps the higher seq.
    #[tokio::test]
    async fn signal_insert_never_rewinds_a_newer_kind_state() {
        let j = MockJournal::new();
        let placement =
            SignalPlacement { listener_pod: "listener-abc".into(), generation: 1 };
        let mut fresh = registration("tok-1");
        fresh.kind_state = serde_json::json!({ "cursor": 10 });
        fresh.kind_state_seq = 5;
        j.signal_insert(&fresh, &placement).await.unwrap();

        // A rewind (older seq) loses the state...
        let mut stale = registration("tok-1");
        stale.kind_state = serde_json::json!({ "cursor": 3 });
        stale.kind_state_seq = 2;
        j.signal_insert(&stale, &placement).await.unwrap();
        let row = j.signal_get("tok-1").await.unwrap().unwrap();
        assert_eq!(row.kind_state, serde_json::json!({ "cursor": 10 }));
        assert_eq!(row.kind_state_seq, 5);

        // ...an equal-or-newer seq wins (the deliberate overwrite).
        let mut newer = registration("tok-1");
        newer.kind_state = serde_json::json!({ "cursor": 12 });
        newer.kind_state_seq = 5;
        j.signal_insert(&newer, &placement).await.unwrap();
        let row = j.signal_get("tok-1").await.unwrap().unwrap();
        assert_eq!(row.kind_state, serde_json::json!({ "cursor": 12 }));
        assert_eq!(row.kind_state_seq, 5);
    }

    /// A node-test start seeds the color mirror as `node_test`, and
    /// the project-lifecycle reads (`list_non_terminal_colors_for_project`)
    /// skip it, exactly like the Postgres `kind = 'execution'` filter.
    #[tokio::test]
    async fn node_test_colors_stay_out_of_lifecycle_reads() {
        let j = MockJournal::new();
        j.set_project_tenant("p", "t");
        let run = weft_core::Color::new_v4();
        let test = weft_core::Color::new_v4();
        j.record_event(&started(run, "p")).await.unwrap();
        j.record_event(&ExecEvent::ExecutionStarted {
            color: test,
            project_id: "p".into(),
            entry_node: "node-test:MyNode::t".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: None,
            node_test: true,
            subgraph: None,
            at_unix: 0,
        })
        .await
        .unwrap();
        let live = j.list_non_terminal_colors_for_project("p", None).await.unwrap();
        assert_eq!(live, vec![run], "the node-test color never counts as a project run");
    }

    fn started(color: weft_core::Color, project_id: &str) -> ExecEvent {
        ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.into(),
            entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            node_test: false,
            subgraph: None,
            at_unix: 0,
        }
    }

    /// `list_terminal_colors_for_project` is the exact complement of
    /// `list_non_terminal_colors_for_project` over a project's colors: a color
    /// with a terminal event lands in one, a color without lands in the other.
    /// This is what stops a stray pending task from resurrecting a finished
    /// execution in `running_count`.
    #[tokio::test]
    async fn terminal_and_non_terminal_color_sets_partition_the_project() {
        let j = MockJournal::new();
        j.set_project_tenant("p", "t");
        let done = uuid::Uuid::new_v4();
        let live = uuid::Uuid::new_v4();
        // Both colors start (seeds the execution_color mirror).
        j.record_event(&started(done, "p")).await.unwrap();
        j.record_event(&started(live, "p")).await.unwrap();
        // Only `done` gets a terminal event.
        j.record_event(&ExecEvent::ExecutionCompleted { color: done, at_unix: 1 })
            .await
            .unwrap();

        let terminal = j.list_terminal_colors_for_project("p").await.unwrap();
        let non_terminal = j.list_non_terminal_colors_for_project("p", None).await.unwrap();

        assert!(terminal.contains(&done), "completed color is terminal");
        assert!(!terminal.contains(&live), "still-running color is not terminal");
        assert!(non_terminal.contains(&live), "still-running color is non-terminal");
        assert!(!non_terminal.contains(&done), "completed color is not non-terminal");
    }

    /// `execution_tenant` resolves the tenant stamped at start (from the project's
    /// tenant), and reports NotFound for a color that never started. This is what
    /// lets the terminate sweep key storage by the run's own tenant WITHOUT the
    /// project store, so a since-deleted project's terminal event still resolves.
    #[tokio::test]
    async fn execution_owner_reads_the_seeded_row() {
        let j = MockJournal::new();
        let color = weft_core::Color::new_v4();
        j.set_project_tenant("p", "tenant-x");
        j.record_event(&started(color, "p")).await.unwrap();

        let owner = j.execution_owner(color).await.unwrap().expect("owner");
        // Both fields come off the mirror in one read, so neither can
        // resolve while the other does not.
        assert_eq!(owner.tenant, "tenant-x");
        assert_eq!(owner.project_id, "p");
        // A color that never started has no execution_color row.
        assert!(j.execution_owner(weft_core::Color::new_v4()).await.unwrap().is_none());
    }

    fn started_at(color: weft_core::Color, project_id: &str, at_unix: u64) -> ExecEvent {
        ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.into(),
            entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            node_test: false,
            subgraph: None,
            at_unix,
        }
    }

    /// `execution_summary` is a direct point-lookup by color: it resolves an
    /// execution regardless of how old it is (no windowed scan), and reports the
    /// terminal status. This is what replaced the "fetch a page, scan it" get.
    #[tokio::test]
    async fn execution_summary_is_a_direct_point_lookup() {
        let j = MockJournal::new();
        let c = weft_core::Color::new_v4();
        j.set_project_tenant("p", "t");
        j.record_event(&started_at(c, "p", 100)).await.unwrap();
        j.record_event(&ExecEvent::ExecutionCompleted { color: c, at_unix: 150 })
            .await
            .unwrap();

        let s = j.execution_summary(c).await.unwrap().expect("found by color");
        assert_eq!(s.color, c);
        assert_eq!(s.status, "completed");
        assert_eq!(s.completed_at, Some(150));
        // A color that never started is absent, not an error.
        assert!(j.execution_summary(weft_core::Color::new_v4()).await.unwrap().is_none());
    }

    /// `list_executions` pages (limit/offset, newest first), reports the true
    /// total, and filters by project + start-time range, all inside the tenant
    /// wall.
    #[tokio::test]
    async fn list_executions_pages_and_filters() {
        let j = MockJournal::new();
        j.set_project_tenant("pa", "t");
        j.set_project_tenant("pb", "t");
        j.set_project_tenant("other", "t2");
        // Three in project pa at t=10/20/30, one in pb at t=25, one in another tenant.
        let a1 = weft_core::Color::new_v4();
        let a2 = weft_core::Color::new_v4();
        let a3 = weft_core::Color::new_v4();
        let b1 = weft_core::Color::new_v4();
        let x1 = weft_core::Color::new_v4();
        j.record_event(&started_at(a1, "pa", 10)).await.unwrap();
        j.record_event(&started_at(a2, "pa", 20)).await.unwrap();
        j.record_event(&started_at(a3, "pa", 30)).await.unwrap();
        j.record_event(&started_at(b1, "pb", 25)).await.unwrap();
        j.record_event(&started_at(x1, "other", 40)).await.unwrap();

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
        let qp = ExecutionQuery { limit: 50, project_id: Some("pa".into()), ..Default::default() };
        let pagep = j.list_executions("t", &qp).await.unwrap();
        assert_eq!(pagep.total, 3);
        assert!(pagep.executions.iter().all(|e| e.project_id == "pa"));

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
        assert_eq!(paget2.executions[0].color, x1);
    }

    /// The phase filter separates a trigger's real fires from the
    /// setup runs an activate makes, and the summary carries the phase
    /// so a listing can say which is which.
    #[tokio::test]
    async fn list_executions_filters_by_phase() {
        let j = MockJournal::new();
        j.set_project_tenant("p", "t");
        let fire = weft_core::Color::new_v4();
        let setup = weft_core::Color::new_v4();
        j.record_event(&started_at(fire, "p", 10)).await.unwrap();
        let ExecEvent::ExecutionStarted { color, project_id, entry_node, definition_hash, node_test, subgraph, at_unix, .. } =
            started_at(setup, "p", 20)
        else {
            unreachable!()
        };
        j.record_event(&ExecEvent::ExecutionStarted {
            color,
            project_id,
            entry_node,
            phase: weft_core::context::Phase::TriggerSetup,
            definition_hash,
            node_test,
            subgraph,
            at_unix,
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
        assert_eq!(fires.executions[0].color, fire);
    }
}

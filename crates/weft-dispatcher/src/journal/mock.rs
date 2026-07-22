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
    SignalToken, ColorLookup, ExecutionPage, ExecutionQuery, ExecutionSummary, Journal, LogEntry,
    SignalRegistration,
};

#[derive(Default)]
struct MockState {
    events: Vec<ExecEvent>,
    signal_tokens: HashMap<String, SignalToken>,
    signals: HashMap<String, SignalRegistration>,
    /// Placement (holder pod + generation) recorded WITH each signal,
    /// mirroring the real `signal.listener_pod` / `placement_generation`
    /// columns, so a test can assert what a fresh insert stamped.
    signal_placements: HashMap<String, crate::journal::SignalPlacement>,
    dedup_keys: std::collections::HashSet<String>,
    /// Mirror of the Postgres `execution_color` denormalization:
    /// seeded on `ExecutionStarted` with `(project_id, tenant_id)`,
    /// cleared on `delete_execution`. The tenant is derived from
    /// `project_tenants` at seed time, exactly as Postgres reads it from
    /// the `project` table. Tests that exercise
    /// `list_non_terminal_colors_for_project`, `delete_execution`
    /// cleanup, or tenant-scoped `list_executions` depend on this
    /// matching real-DB semantics.
    execution_colors: HashMap<Color, (String, String)>,
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

    /// Build the `ExecutionSummary` for one color from the recorded events (the
    /// started row plus its latest terminal event), or `None` if there is no
    /// `execution_started` for it. Shared by the tenant listing + the by-color
    /// lookup so the status-fold lives in one place, mirroring the Postgres
    /// `summary_from_payloads` helper.
    fn summary_for_color(&self, color: Color) -> Option<ExecutionSummary> {
        let g = self.inner.lock().unwrap();
        let (project_id, entry_node, started_at) = g.events.iter().find_map(|e| match e {
            ExecEvent::ExecutionStarted { color: c, project_id, entry_node, at_unix, .. }
                if *c == color =>
            {
                Some((project_id.clone(), entry_node.clone(), *at_unix))
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
        Some(ExecutionSummary { color, project_id, entry_node, status, started_at, completed_at })
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
                        let owner = g.execution_colors.get(color).map(|(_, t)| t.as_str());
                        (owner == Some(tenant)).then_some(*color)
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
        self.inner.lock().unwrap().signal_placements.get(token).cloned()
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
fn seed_execution_color(
    state: &mut MockState,
    color: Color,
    project_id: &str,
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
    state.execution_colors.insert(color, (project_id.to_string(), tenant));
    Ok(())
}

#[async_trait]
impl Journal for MockJournal {
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        if let ExecEvent::ExecutionStarted { color, project_id, .. } = event {
            seed_execution_color(&mut g, *color, project_id)?;
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
            if let ExecEvent::ExecutionStarted { color, project_id, .. } = event {
                seed_execution_color(&mut g, *color, project_id)?;
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
        let ExecEvent::ExecutionStarted { color, project_id, .. } = start else {
            anyhow::bail!("start_execution requires an ExecutionStarted event");
        };
        seed_execution_color(&mut g, *color, project_id)?;
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
        let ExecEvent::ExecutionStarted { color, project_id, .. } = start else {
            anyhow::bail!("start_live_execution requires an ExecutionStarted event");
        };
        seed_execution_color(&mut g, *color, project_id)?;
        g.events.push(start.clone());
        g.events.extend(kicks.iter().cloned());
        task.target_pod_name = Some(pod.pod_name.clone());
        g.tasks.push(task);
        Ok(weft_task_store::tasks::LiveAdmitOutcome::Admitted(pod))
    }

    async fn cancel_never_claimed_execution(
        &self,
        color: Color,
        reason: &str,
    ) -> anyhow::Result<weft_task_store::tasks::SetupFailureOutcome> {
        // Dumb: the mock has no 'claimed' state, so the outcome is always
        // NoWorkerWillRun: drop the recorded task and append the terminal
        // (idempotent, exactly like the real dedup'd write).
        let mut g = self.inner.lock().unwrap();
        let color_str = color.to_string();
        g.tasks.retain(|t| t.color.as_deref() != Some(color_str.as_str()));
        let has_terminal = g.events.iter().any(|e| {
            e.color() == color
                && matches!(
                    e,
                    ExecEvent::ExecutionCompleted { .. }
                        | ExecEvent::ExecutionFailed { .. }
                        | ExecEvent::ExecutionCancelled { .. }
                )
        });
        if !has_terminal {
            g.events.push(ExecEvent::ExecutionCancelled {
                color,
                reason: reason.to_string(),
                at_unix: 0,
            });
        }
        Ok(weft_task_store::tasks::SetupFailureOutcome::NoWorkerWillRun)
    }

    async fn events_log(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.color() == color)
            .cloned()
            .collect())
    }

    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool> {
        // Mirror the postgres impl: drop the signal row for a
        // single-use resume token. Entry-trigger rows stay.
        let mut g = self.inner.lock().unwrap();
        match g.signals.get(token) {
            Some(s) if s.is_resume => {
                g.signals.remove(token);
                Ok(true)
            }
            _ => Ok(false),
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

    async fn execution_project(&self, color: Color) -> anyhow::Result<ColorLookup<String>> {
        // The mock stores typed events, so a row can never be
        // corrupt; only Found / NotFound occur here.
        Ok(self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .find_map(|e| match e {
                ExecEvent::ExecutionStarted { color: c, project_id, .. } if *c == color => {
                    Some(project_id.clone())
                }
                _ => None,
            })
            .map_or(ColorLookup::NotFound, ColorLookup::Found))
    }

    async fn execution_tenant(&self, color: Color) -> anyhow::Result<ColorLookup<String>> {
        // Read the tenant off the `execution_colors` mirror (the same
        // `(project_id, tenant_id)` denormalization Postgres keeps), so it
        // resolves even after the execution's project mapping is gone.
        Ok(self
            .inner
            .lock()
            .unwrap()
            .execution_colors
            .get(&color)
            .map(|(_project, tenant)| tenant.clone())
            .map_or(ColorLookup::NotFound, ColorLookup::Found))
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
                ExecEvent::ExecutionStarted { color: c, definition_hash, .. } if *c == color => {
                    Some(definition_hash.clone())
                }
                _ => None,
            })
            .map_or(ColorLookup::NotFound, ColorLookup::Found))
    }

    async fn logs_for(&self, color: Color, _limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::LogLine { color: c, level, message, at_unix } if *c == color => {
                    Some(LogEntry {
                        at_unix: *at_unix,
                        level: level.clone(),
                        message: message.clone(),
                    })
                }
                _ => None,
            })
            .collect())
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

    async fn list_non_terminal_colors_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<Color>> {
        // Read from `execution_colors` (mirror of Postgres
        // `execution_color`) instead of scanning `events`. Keeps
        // mock semantics aligned with the real DB: `delete_execution`
        // clears the row so cleaned colors don't keep appearing as
        // non-terminal.
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (color, (pid, _tenant)) in g.execution_colors.iter() {
            if pid != project_id {
                continue;
            }
            let terminal = g.events.iter().any(|e2| {
                e2.color() == *color
                    && matches!(
                        e2,
                        ExecEvent::ExecutionCompleted { .. }
                            | ExecEvent::ExecutionFailed { .. }
                            | ExecEvent::ExecutionCancelled { .. }
                    )
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
        for (color, (pid, _tenant)) in g.execution_colors.iter() {
            if pid != project_id {
                continue;
            }
            let terminal = g.events.iter().any(|e2| {
                e2.color() == *color
                    && matches!(
                        e2,
                        ExecEvent::ExecutionCompleted { .. }
                            | ExecEvent::ExecutionFailed { .. }
                            | ExecEvent::ExecutionCancelled { .. }
                    )
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
        g.signals.retain(|_, s| s.color != Some(color));
        g.execution_colors.remove(&color);
        Ok(())
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
        inner.signals.insert(sig.token.clone(), sig.clone());
        inner
            .signal_placements
            .insert(sig.token.clone(), placement.clone());
        Ok(())
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        Ok(self.inner.lock().unwrap().signals.get(token).cloned())
    }

    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for t in tokens {
            if let Some(sig) = g.signals.remove(t) {
                out.push(sig);
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
            .filter(|s| s.project_id == project_id)
            .cloned()
            .collect())
    }

    async fn signal_remove_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        // Mirror the postgres predicate EXACTLY (`color = $1 AND is_resume`):
        // matching on `color == Some(..)` alone would rely on the unstated
        // convention that only resume signals carry a color, and diverge from
        // the real impl the moment an entry signal ever got one.
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.color == Some(color) && s.is_resume)
            .map(|(k, _)| k.clone())
            .collect();
        Ok(keys
            .into_iter()
            .filter_map(|k| g.signals.remove(&k))
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
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: "public_entry".into(),
            mount_path: None,
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
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
        // together, not in separate steps).
        assert!(j.signal_get("tok-1").await.unwrap().is_some());
    }

    fn started(color: weft_core::Color, project_id: &str) -> ExecEvent {
        ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.into(),
            entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: "h".into(),
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
        j.record_event(&ExecEvent::ExecutionCompleted { color: done, outputs: serde_json::Value::Null, at_unix: 1 })
            .await
            .unwrap();

        let terminal = j.list_terminal_colors_for_project("p").await.unwrap();
        let non_terminal = j.list_non_terminal_colors_for_project("p").await.unwrap();

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
    async fn execution_tenant_reads_the_seeded_tenant() {
        let j = MockJournal::new();
        let color = weft_core::Color::new_v4();
        j.set_project_tenant("p", "tenant-x");
        j.record_event(&started(color, "p")).await.unwrap();

        assert_eq!(
            j.execution_tenant(color).await.unwrap().found().as_deref(),
            Some("tenant-x"),
        );
        // A color that never started has no execution_color row.
        assert!(matches!(
            j.execution_tenant(weft_core::Color::new_v4()).await.unwrap(),
            ColorLookup::NotFound
        ));
    }

    fn started_at(color: weft_core::Color, project_id: &str, at_unix: u64) -> ExecEvent {
        ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.into(),
            entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: "h".into(),
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
        j.record_event(&ExecEvent::ExecutionCompleted { color: c, outputs: serde_json::Value::Null, at_unix: 150 })
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
}

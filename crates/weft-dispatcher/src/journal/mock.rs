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
    ApiToken, ExecutionSummary, Journal, LogEntry, NodeExecEvent, NodeExecKind, SignalRegistration,
};

#[derive(Default)]
struct MockState {
    events: Vec<ExecEvent>,
    api_tokens: HashMap<String, ApiToken>,
    signals: HashMap<String, SignalRegistration>,
    dedup_keys: std::collections::HashSet<String>,
    /// Mirror of the Postgres `execution_color` denormalization:
    /// seeded on `ExecutionStarted`, cleared on `delete_execution`.
    /// Tests that exercise `list_non_terminal_colors_for_project`
    /// or `delete_execution` cleanup paths depend on this matching
    /// real-DB semantics.
    execution_colors: HashMap<Color, String>,
}

#[derive(Default)]
pub struct MockJournal {
    inner: Mutex<MockState>,
}

impl MockJournal {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Journal for MockJournal {
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        if let ExecEvent::ExecutionStarted { color, project_id, .. } = event {
            g.execution_colors.entry(*color).or_insert_with(|| project_id.clone());
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
                g.execution_colors.entry(*color).or_insert_with(|| project_id.clone());
            }
            g.events.push(event.clone());
        }
        Ok(())
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

    async fn mint_api_token(&self, tok: &ApiToken) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .api_tokens
            .insert(tok.token.clone(), tok.clone());
        Ok(())
    }

    async fn get_api_token(&self, token: &str) -> anyhow::Result<Option<ApiToken>> {
        Ok(self.inner.lock().unwrap().api_tokens.get(token).cloned())
    }

    async fn list_api_tokens(&self) -> anyhow::Result<Vec<ApiToken>> {
        Ok(self.inner.lock().unwrap().api_tokens.values().cloned().collect())
    }

    async fn revoke_api_token(&self, identifier: &str) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let keys: Vec<String> = g
            .api_tokens
            .iter()
            .filter(|(t, tok)| {
                t.as_str() == identifier || tok.name.as_deref() == Some(identifier)
            })
            .map(|(t, _)| t.clone())
            .collect();
        let removed = !keys.is_empty();
        for k in keys {
            g.api_tokens.remove(&k);
        }
        Ok(removed)
    }

    async fn execution_project(&self, color: Color) -> anyhow::Result<Option<String>> {
        Ok(self.inner.lock().unwrap().events.iter().find_map(|e| match e {
            ExecEvent::ExecutionStarted { color: c, project_id, .. } if *c == color => {
                Some(project_id.clone())
            }
            _ => None,
        }))
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

    async fn events_for(&self, color: Color) -> anyhow::Result<Vec<NodeExecEvent>> {
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for e in g.events.iter() {
            let conv = match e {
                ExecEvent::NodeStarted { color: c, node_id, lane, input, at_unix, .. }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Started,
                        input: Some(input.clone()),
                        output: None,
                        error: None,
                        token: None,
                        value: None,
                        reason: None,
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeSuspended { color: c, node_id, lane, token, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Suspended,
                        input: None,
                        output: None,
                        error: None,
                        token: Some(token.clone()),
                        value: None,
                        reason: None,
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeResumed { color: c, node_id, lane, token, value, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Resumed,
                        input: None,
                        output: None,
                        error: None,
                        token: Some(token.clone()),
                        value: Some(value.clone()),
                        reason: None,
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeCancelled { color: c, node_id, lane, reason, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Cancelled,
                        input: None,
                        output: None,
                        error: Some(reason.clone()),
                        token: None,
                        value: None,
                        reason: Some(reason.clone()),
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeCompleted { color: c, node_id, lane, output, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Completed,
                        input: None,
                        output: Some(output.clone()),
                        error: None,
                        token: None,
                        value: None,
                        reason: None,
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeFailed { color: c, node_id, lane, error, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Failed,
                        input: None,
                        output: None,
                        error: Some(error.clone()),
                        token: None,
                        value: None,
                        reason: None,
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeSkipped { color: c, node_id, lane, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).expect("Lane (Vec<LaneFrame>) serializes"),
                        kind: NodeExecKind::Skipped,
                        input: None,
                        output: None,
                        error: None,
                        token: None,
                        value: None,
                        reason: None,
                        at_unix: *at_unix,
                    })
                }
                _ => None,
            };
            if let Some(ne) = conv {
                out.push(ne);
            }
        }
        Ok(out)
    }

    async fn list_executions(&self, _limit: u32) -> anyhow::Result<Vec<ExecutionSummary>> {
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for e in g.events.iter() {
            if let ExecEvent::ExecutionStarted { color, project_id, entry_node, at_unix, .. } = e {
                let mut status = "running".to_string();
                let mut completed_at = None;
                for tail in g.events.iter().filter(|e2| e2.color() == *color) {
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
                out.push(ExecutionSummary {
                    color: *color,
                    project_id: project_id.clone(),
                    entry_node: entry_node.clone(),
                    status,
                    started_at: *at_unix,
                    completed_at,
                });
            }
        }
        out.reverse();
        Ok(out)
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
        for (color, pid) in g.execution_colors.iter() {
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

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.events.retain(|e| e.color() != color);
        g.signals.retain(|_, s| s.color != Some(color));
        g.execution_colors.remove(&color);
        Ok(())
    }

    async fn signal_insert(&self, sig: &SignalRegistration) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .signals
            .insert(sig.token.clone(), sig.clone());
        Ok(())
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        Ok(self.inner.lock().unwrap().signals.get(token).cloned())
    }

    async fn signal_remove(&self, token: &str) -> anyhow::Result<bool> {
        Ok(self.inner.lock().unwrap().signals.remove(token).is_some())
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

    async fn signal_count_for_tenant(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .signals
            .values()
            .filter(|s| s.tenant_id == tenant_id)
            .count())
    }

    async fn signal_remove_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut g = self.inner.lock().unwrap();
        let keys: Vec<String> = g
            .signals
            .iter()
            .filter(|(_, s)| s.color == Some(color))
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

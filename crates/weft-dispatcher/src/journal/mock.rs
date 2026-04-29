//! In-memory `Journal` implementation for tests. Mirrors the
//! Postgres semantics: append-only event log + token lookup tables.
//! Compiled only under `cfg(test)` and behind the `test-helpers`
//! feature so dependent crates can pull it in for their own tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use weft_core::Color;

use crate::journal::events::ExecEvent;
use crate::journal::{
    EntryKind, EntryTarget, ExecutionSummary, ExtToken, Journal, LogEntry, NodeExecEvent,
    NodeExecKind, OpenSuspension, SignalRegistration, WakeTarget,
};

#[derive(Default)]
struct MockState {
    events: Vec<ExecEvent>,
    suspensions: HashMap<String, (Color, String, Value, u64)>,
    entry_tokens: HashMap<String, EntryTarget>,
    ext_tokens: HashMap<String, (Option<String>, u64)>,
    signals: HashMap<String, SignalRegistration>,
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

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[async_trait]
impl Journal for MockJournal {
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        self.inner.lock().unwrap().events.push(event.clone());
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

    async fn record_suspension_with_token(
        &self,
        token: &str,
        color: Color,
        node: &str,
        metadata: Value,
    ) -> anyhow::Result<()> {
        self.inner.lock().unwrap().suspensions.insert(
            token.to_string(),
            (color, node.to_string(), metadata, now_unix()),
        );
        Ok(())
    }

    async fn resolve_wake(&self, token: &str) -> anyhow::Result<Option<WakeTarget>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .suspensions
            .get(token)
            .map(|(color, node, metadata, _)| WakeTarget {
                color: *color,
                node: node.clone(),
                metadata: metadata.clone(),
            }))
    }

    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool> {
        Ok(self.inner.lock().unwrap().suspensions.remove(token).is_some())
    }

    async fn list_open_suspensions(&self) -> anyhow::Result<Vec<OpenSuspension>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .suspensions
            .iter()
            .map(|(token, (color, node, metadata, created_at))| OpenSuspension {
                token: token.clone(),
                color: *color,
                node: node.clone(),
                metadata: metadata.clone(),
                created_at: *created_at,
            })
            .collect())
    }

    async fn mint_entry_token(
        &self,
        project_id: &str,
        node_id: &str,
        kind: EntryKind,
        path: Option<&str>,
        auth: Option<Value>,
    ) -> anyhow::Result<String> {
        let token = uuid::Uuid::new_v4().to_string();
        self.inner.lock().unwrap().entry_tokens.insert(
            token.clone(),
            EntryTarget {
                project_id: project_id.to_string(),
                node_id: node_id.to_string(),
                kind,
                path: path.map(String::from),
                auth,
            },
        );
        Ok(token)
    }

    async fn resolve_entry_token(&self, token: &str) -> anyhow::Result<Option<EntryTarget>> {
        Ok(self.inner.lock().unwrap().entry_tokens.get(token).cloned())
    }

    async fn drop_entry_tokens(&self, project_id: &str) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.entry_tokens.retain(|_, target| target.project_id != project_id);
        Ok(())
    }

    async fn mint_ext_token(
        &self,
        token: &str,
        name: Option<&str>,
        _metadata: Option<Value>,
    ) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .ext_tokens
            .insert(token.to_string(), (name.map(String::from), now_unix()));
        Ok(())
    }

    async fn ext_token_exists(&self, token: &str) -> anyhow::Result<bool> {
        Ok(self.inner.lock().unwrap().ext_tokens.contains_key(token))
    }

    async fn list_ext_tokens(&self) -> anyhow::Result<Vec<ExtToken>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .ext_tokens
            .iter()
            .map(|(token, (name, created_at))| ExtToken {
                token: token.clone(),
                name: name.clone(),
                created_at: *created_at,
            })
            .collect())
    }

    async fn revoke_ext_token(&self, identifier: &str) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let keys: Vec<String> = g
            .ext_tokens
            .iter()
            .filter(|(t, (name, _))| {
                t.as_str() == identifier || name.as_deref() == Some(identifier)
            })
            .map(|(t, _)| t.clone())
            .collect();
        let removed = !keys.is_empty();
        for k in keys {
            g.ext_tokens.remove(&k);
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
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
                ExecEvent::NodeRetried { color: c, node_id, lane, reason, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).unwrap_or_default(),
                        kind: NodeExecKind::Retried,
                        input: None,
                        output: None,
                        error: None,
                        token: None,
                        value: None,
                        reason: Some(reason.clone()),
                        at_unix: *at_unix,
                    })
                }
                ExecEvent::NodeCancelled { color: c, node_id, lane, reason, at_unix }
                    if *c == color =>
                {
                    Some(NodeExecEvent {
                        color: *c,
                        node_id: node_id.clone(),
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
                        lane: serde_json::to_string(lane).unwrap_or_default(),
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
            if let ExecEvent::ExecutionStarted { color, project_id, entry_node, at_unix } = e {
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
                        ExecEvent::Stalled { at_unix, .. } => {
                            status = "stalled".into();
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

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.events.retain(|e| e.color() != color);
        g.suspensions.retain(|_, (c, _, _, _)| *c != color);
        Ok(())
    }

    async fn cancel(&self, color: Color) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.suspensions.retain(|_, (c, _, _, _)| *c != color);
        g.events.push(ExecEvent::ExecutionFailed {
            color,
            error: "cancelled".into(),
            at_unix: now_unix(),
        });
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

    async fn signal_list_for_tenant(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .signals
            .values()
            .filter(|s| s.tenant_id == tenant_id)
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

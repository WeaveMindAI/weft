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

pub mod events;
pub mod sqlite;

pub use events::{fold_to_snapshot, ExecEvent};

use async_trait::async_trait;
use serde_json::Value;

use weft_core::Color;

#[async_trait]
pub trait Journal: Send + Sync {
    // ----- Event log (state source of truth) -------------------------

    /// Append one event to the execution's log. Append-only; only
    /// user-initiated `weft clean` removes events.
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()>;

    /// Full ordered event log for a color.
    async fn events_log(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>>;

    // ----- Token indexes (not state duplication) ---------------------

    /// Journal a mid-execution suspension with a caller-supplied
    /// token. This is a lookup table: token → (color, node,
    /// metadata) so form URLs can route fires to the right lane.
    /// Does NOT record a state event; the caller emits
    /// `ExecEvent::SuspensionRegistered` separately if it wants
    /// the fact journaled.
    async fn record_suspension_with_token(
        &self,
        token: &str,
        color: Color,
        node: &str,
        metadata: Value,
    ) -> anyhow::Result<()>;

    async fn resolve_wake(&self, token: &str) -> anyhow::Result<Option<WakeTarget>>;

    /// Remove the suspension once it has been resolved. Returns
    /// true if the token existed.
    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool>;

    /// Return every live suspension across all projects. Used by
    /// the browser extension's task listing.
    async fn list_open_suspensions(&self) -> anyhow::Result<Vec<OpenSuspension>>;

    async fn mint_entry_token(
        &self,
        project_id: &str,
        node_id: &str,
        kind: EntryKind,
        path: Option<&str>,
        auth: Option<Value>,
    ) -> anyhow::Result<String>;

    async fn resolve_entry_token(&self, token: &str) -> anyhow::Result<Option<EntryTarget>>;

    async fn drop_entry_tokens(&self, project_id: &str) -> anyhow::Result<()>;

    async fn mint_ext_token(
        &self,
        name: Option<&str>,
        metadata: Option<Value>,
    ) -> anyhow::Result<String>;

    async fn ext_token_exists(&self, token: &str) -> anyhow::Result<bool>;

    async fn list_ext_tokens(&self) -> anyhow::Result<Vec<ExtToken>>;

    async fn revoke_ext_token(&self, token: &str) -> anyhow::Result<()>;

    // ----- Derived views over the event log --------------------------

    /// Look up which project a color belongs to. Walks the event
    /// log for the first `ExecutionStarted` event. `Ok(None)` if
    /// the color is unknown.
    async fn execution_project(&self, color: Color) -> anyhow::Result<Option<String>>;

    /// Log lines for a color, oldest first. Folded from
    /// `ExecEvent::LogLine` events.
    async fn logs_for(&self, color: Color, limit: u32) -> anyhow::Result<Vec<LogEntry>>;

    /// Per-node lifecycle events for a color, oldest first.
    /// Folded from `ExecEvent::Node{Started,Completed,Failed,Skipped}`.
    async fn events_for(&self, color: Color) -> anyhow::Result<Vec<NodeExecEvent>>;

    /// Summary row for every execution the dispatcher has ever
    /// seen, newest first.
    async fn list_executions(&self, limit: u32) -> anyhow::Result<Vec<ExecutionSummary>>;

    // ----- Administrative ---------------------------------------------

    /// Delete all data for a color. Called only by `weft clean`.
    async fn delete_execution(&self, color: Color) -> anyhow::Result<()>;

    /// Mark a color cancelled. Phase A: no-op beyond a log event.
    /// Phase B: writes a control row so running workers can poll.
    async fn cancel(&self, color: Color) -> anyhow::Result<()>;
}

// ----- Public types -----------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeExecEvent {
    pub color: Color,
    pub node_id: String,
    /// Encoded lane path; JSON array of LaneFrame. Empty string
    /// for nodes with no expand/gather context.
    pub lane: String,
    pub kind: NodeExecKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub at_unix: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecKind {
    Started,
    Completed,
    Failed,
    Skipped,
}

impl NodeExecKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "started" => Some(Self::Started),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionSummary {
    pub color: Color,
    pub project_id: String,
    pub entry_node: String,
    pub status: String,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ExtToken {
    pub token: String,
    pub name: Option<String>,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct OpenSuspension {
    pub token: String,
    pub color: Color,
    pub node: String,
    pub metadata: Value,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub at_unix: u64,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct WakeTarget {
    pub color: Color,
    pub node: String,
    pub metadata: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Webhook,
    Cron,
    Manual,
}

impl EntryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Webhook => "webhook",
            Self::Cron => "cron",
            Self::Manual => "manual",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "webhook" => Some(Self::Webhook),
            "cron" => Some(Self::Cron),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntryTarget {
    pub project_id: String,
    pub node_id: String,
    pub kind: EntryKind,
    pub path: Option<String>,
    pub auth: Option<Value>,
}

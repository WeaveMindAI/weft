//! Journal abstraction + sqlite-backed impl for local dev.
//!
//! The journal owns everything durable the dispatcher needs to
//! resume executions across restarts:
//! - Entry tokens (webhook URLs, cron schedules) -> project + node.
//! - Suspension tokens -> color + node + metadata for wake.
//! - Cost events per color (ledger for dashboard + billing).
//!
//! Cloud deployments (weavemind) swap in a restate-backed impl via
//! the same trait; local uses sqlite.

pub mod sqlite;

use async_trait::async_trait;
use serde_json::Value;

use weft_core::Color;

#[async_trait]
pub trait Journal: Send + Sync {
    async fn record_start(&self, color: Color, project_id: &str, entry_node: &str)
        -> anyhow::Result<()>;

    /// Journal a mid-execution suspension. Returns the opaque token
    /// the caller serves on the user-facing URL (e.g. form URL).
    async fn record_suspension(
        &self,
        color: Color,
        node: &str,
        metadata: Value,
    ) -> anyhow::Result<String>;

    async fn resolve_wake(&self, token: &str) -> anyhow::Result<Option<WakeTarget>>;

    /// Remove the suspension once it has been resolved. Returns true
    /// if the token existed.
    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool>;

    /// Mint an entry token for a project's trigger node. Returns the
    /// opaque token the dispatcher advertises on the user-facing URL.
    async fn mint_entry_token(
        &self,
        project_id: &str,
        node_id: &str,
        kind: EntryKind,
        path: Option<&str>,
        auth: Option<Value>,
    ) -> anyhow::Result<String>;

    async fn resolve_entry_token(&self, token: &str) -> anyhow::Result<Option<EntryTarget>>;

    /// Drop every entry token for a project (on deactivate or rm).
    async fn drop_entry_tokens(&self, project_id: &str) -> anyhow::Result<()>;

    async fn record_cost(&self, color: Color, report: weft_core::CostReport)
        -> anyhow::Result<()>;

    async fn cancel(&self, color: Color) -> anyhow::Result<()>;

    /// Look up which project a given color belongs to. `Ok(None)`
    /// if the color was never journaled.
    async fn execution_project(&self, color: Color) -> anyhow::Result<Option<String>>;

    /// Append a log line emitted by a running worker.
    async fn append_log(&self, color: Color, level: &str, message: &str)
        -> anyhow::Result<()>;

    /// Return log lines for a color, oldest first, capped at `limit`.
    async fn logs_for(&self, color: Color, limit: u32) -> anyhow::Result<Vec<LogEntry>>;

    /// Mint an extension token for a human reviewer. Returns the
    /// opaque string the user pastes into the browser extension.
    async fn mint_ext_token(
        &self,
        name: Option<&str>,
        metadata: Option<Value>,
    ) -> anyhow::Result<String>;

    async fn ext_token_exists(&self, token: &str) -> anyhow::Result<bool>;

    async fn list_ext_tokens(&self) -> anyhow::Result<Vec<ExtToken>>;

    async fn revoke_ext_token(&self, token: &str) -> anyhow::Result<()>;

    /// Return every live suspension across all projects. Used by the
    /// browser extension's `/ext/{token}/tasks` listing. Phase B
    /// adds per-token metadata filtering.
    async fn list_open_suspensions(&self) -> anyhow::Result<Vec<OpenSuspension>>;

    /// Append a per-node execution event. The worker calls this on
    /// every node lifecycle transition (started / completed / failed
    /// / skipped). Used by SSE `weft follow` and by `/executions/{color}/replay`.
    async fn record_node_event(&self, event: &NodeExecEvent) -> anyhow::Result<()>;

    /// Return every node event for an execution, oldest first. Used
    /// by replay.
    async fn events_for(&self, color: Color) -> anyhow::Result<Vec<NodeExecEvent>>;

    /// Delete all data for a color (execution row, node events,
    /// logs, suspensions, cost). Called by `weft clean <color>`.
    async fn delete_execution(&self, color: Color) -> anyhow::Result<()>;

    /// List past executions, newest first, capped. Used by
    /// `weft clean --list` and "Replay execution..." picker.
    async fn list_executions(&self, limit: u32) -> anyhow::Result<Vec<ExecutionSummary>>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeExecEvent {
    pub color: Color,
    pub node_id: String,
    /// Encoded lane path; JSON array of LaneFrame. Empty string for
    /// nodes with no expand/gather context.
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

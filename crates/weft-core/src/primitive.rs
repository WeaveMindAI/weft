use serde::{Deserialize, Serialize};
use serde_json::Value;

// ----- Entry primitives (declared at NodeMetadata level) --------------

/// Declarative entry primitive. Lives in a node's metadata. Framework
/// reads these at compile time and wires external events (HTTP,
/// timers, infra events) to invocations of this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EntryPrimitive {
    /// Incoming HTTP POST. Framework mints a URL and routes matching
    /// calls to a new execution.
    Webhook {
        /// Path pattern, e.g. "apipost/*". Framework prefixes with a
        /// project-specific random token so URLs are unguessable.
        path: String,
        /// Authentication config. Validation is framework-level.
        auth: WebhookAuth,
    },

    /// Cron schedule. Validated at compile time.
    Cron {
        schedule: String,
    },

    /// Subscription to a long-running infra connection (Slack, Discord,
    /// WhatsApp, etc). Infra node publishes events; entry primitive
    /// subscribes.
    Event {
        connection_port: String,
        filter: Option<FilterSpec>,
    },

    /// Manual/UI-initiated run.
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WebhookAuth {
    None,
    OptionalApiKey { field: String },
    RequiredApiKey { field: String },
    HmacSignature { secret_field: String, header: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    /// Free-form filter config. Each infra backend interprets its own
    /// shape (channel name, event type, route selector, etc).
    pub raw: Value,
}

// ----- Form primitives (await_form suspension) ------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSchema {
    pub title: String,
    pub description: Option<String>,
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormField {
    pub key: String,
    pub label: String,
    pub field_type: FormFieldType,
    pub required: bool,
    pub default: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormFieldType {
    Text,
    Textarea,
    Number,
    Checkbox,
    Select { options: Vec<String> },
    Multiselect { options: Vec<String> },
    Date,
    File,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSubmission {
    pub values: Value,
    pub submitted_at: chrono::DateTime<chrono::Utc>,
    pub submitted_by: Option<String>,
}

// ----- Cost report (fire-and-forget primitive) ------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostReport {
    /// Service that incurred the cost (e.g. "openai", "anthropic",
    /// "elevenlabs"). Free-form string, dispatcher buckets on this.
    pub service: String,
    /// Optional model identifier (e.g. "gpt-4o", "claude-sonnet-4").
    pub model: Option<String>,
    /// Amount in USD. Positive.
    pub amount_usd: f64,
    /// Free-form metadata (token counts, duration, etc).
    pub metadata: Value,
}

// ----- Callback subgraph references -----------------------------------

/// Reference to a callback subgraph, resolved at compile time. The
/// concrete representation is compiler-emitted; nodes treat it as
/// opaque.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubgraphRef {
    pub id: String,
}

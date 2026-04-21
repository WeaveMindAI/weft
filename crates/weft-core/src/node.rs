use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::ExecutionContext;
use crate::error::WeftResult;
use crate::primitive::EntryPrimitive;
use crate::weft_type::WeftType;

/// The core trait every node implements. Stdlib nodes in `catalog/`
/// and user-defined nodes under `myproject/nodes/` both implement this.
#[async_trait]
pub trait Node: Send + Sync {
    /// Stable identifier for this node type. Must be unique across the
    /// project's full catalog (stdlib + user + vendored).
    fn node_type(&self) -> &'static str;

    /// Metadata describing ports, fields, entry primitives. Usually
    /// loaded from a co-located `metadata.json` via `include_str!`.
    fn metadata(&self) -> NodeMetadata;

    /// Run this node. `ctx` provides language primitives (await_form,
    /// report_cost, etc). Input values are pre-resolved on ctx.
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput>;
}

/// Metadata returned from `Node::metadata`. Describes the node's
/// surface to the compiler, the dispatcher, and tooling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// Stable type identifier (matches `Node::node_type`).
    #[serde(rename = "type")]
    pub node_type: String,
    /// Human-readable label shown in UIs.
    pub label: String,
    /// One-line description shown in UIs and AI builder context.
    pub description: String,
    /// Category path (e.g. "communication/email"), used for grouping
    /// in the node picker.
    pub category: String,
    /// Free-form tags for search.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Icon hint (lucide icon name in most cases).
    #[serde(default)]
    pub icon: Option<String>,
    /// Input ports.
    #[serde(default)]
    pub inputs: Vec<PortDef>,
    /// Output ports.
    #[serde(default)]
    pub outputs: Vec<PortDef>,
    /// Configurable fields (shown in the node inspector UI).
    #[serde(default)]
    pub fields: Vec<FieldDef>,
    /// Entry primitives declared by this node. Empty = normal node.
    #[serde(default)]
    pub entry: Vec<EntryPrimitive>,
    /// Whether this node requires wiring to a sidecar-backed infra
    /// node. Computed from `entry` containing `Event`, but allowed as
    /// an explicit flag for nodes that use infra without entry.
    #[serde(default)]
    pub requires_infra: bool,
    /// Node-level semantic constraints. Small, extensible.
    #[serde(default)]
    pub features: NodeFeatures,
}

/// Node-level semantic constraints. All optional; empty by default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeFeatures {
    /// Each inner list is a port group where at least ONE port must
    /// be non-null. If every port in a group is null/missing, the
    /// node is skipped. Example: email send might declare
    /// `one_of_required: [["message", "media"]]`.
    #[serde(default, rename = "oneOfRequired")]
    pub one_of_required: Vec<Vec<String>>,
    /// Port groups where values across the listed ports must share
    /// the same parent lane (for fan-out correlation). Phase A2+
    /// when we need it.
    #[serde(default, rename = "correlatedPorts")]
    pub correlated_ports: Vec<Vec<String>>,
}

use crate::project::LaneMode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDef {
    pub name: String,
    #[serde(rename = "type")]
    pub port_type: WeftType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub lane_mode: LaneMode,
    /// Whether this port can be filled with a config default (no
    /// incoming edge). `wired_only` ports must come from upstream.
    #[serde(default)]
    pub configurable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub key: String,
    pub label: String,
    pub field_type: FieldType,
    #[serde(default)]
    pub default_value: Option<Value>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Textarea,
    Code { language: String },
    Number { min: Option<f64>, max: Option<f64> },
    Checkbox,
    Select { options: Vec<String> },
    Multiselect { options: Vec<String> },
    Password,
    ApiKey { provider: String },
    Blob { accept: Option<String> },
    FormBuilder,
}

/// What a node emits when it's done.
#[derive(Debug, Clone)]
pub struct NodeOutput {
    /// One value per declared output port. Missing ports are treated
    /// as "no pulse emitted" (not "null pulse emitted").
    pub outputs: std::collections::HashMap<String, Value>,
}

impl NodeOutput {
    pub fn empty() -> Self {
        Self { outputs: Default::default() }
    }

    pub fn with(port: impl Into<String>, value: Value) -> Self {
        let mut outputs = std::collections::HashMap::new();
        outputs.insert(port.into(), value);
        Self { outputs }
    }

    pub fn set(mut self, port: impl Into<String>, value: Value) -> Self {
        self.outputs.insert(port.into(), value);
        self
    }
}

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

    /// Optional per-node validation pass. Runs during /validate
    /// after the generic rules. Override to check node-specific
    /// invariants (e.g. EmailSend must have a wired config input
    /// from an EmailConfig node). Default: no-op.
    fn validate(
        &self,
        _node: &crate::project::NodeDefinition,
        _project: &crate::project::ProjectDefinition,
    ) -> Vec<Diagnostic> {
        Vec::new()
    }
}

/// Validation diagnostic. Emitted by the generic validate pass and
/// per-node validators. Mirrored by the VS Code extension's
/// Diagnostic type; wire format matches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub line: usize,
    pub column: usize,
    pub severity: Severity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Info,
    Hint,
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
    /// Color hint. Free-form: hex ("#f59e0b"), CSS var ("var(--...)"), or
    /// a token the webview maps to a palette entry. Opaque to the compiler.
    #[serde(default)]
    pub color: Option<String>,
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
    /// the same parent lane (for fan-out correlation).
    #[serde(default, rename = "correlatedPorts")]
    pub correlated_ports: Vec<Vec<String>>,
    /// The node accepts ad-hoc extra input ports declared in weft
    /// source. If unset, extra ports cause a compile error.
    #[serde(default, rename = "canAddInputPorts")]
    pub can_add_input_ports: bool,
    /// Same for outputs.
    #[serde(default, rename = "canAddOutputPorts")]
    pub can_add_output_ports: bool,
    /// The node derives ports from a FormBuilder field at compile
    /// time. See `form_field_specs` for the derivation rules.
    #[serde(default, rename = "hasFormSchema")]
    pub has_form_schema: bool,
    /// Marks the node as a trigger (fires executions from external
    /// events rather than running as part of an execution).
    #[serde(default, rename = "isTrigger")]
    pub is_trigger: bool,
    /// If `is_trigger`, which category the trigger falls into.
    /// Matches v1's TriggerCategory: Webhook / Polling / Schedule /
    /// Socket / Local / Manual. Serialized as a plain string so the
    /// webview doesn't need a round-trip type.
    #[serde(default, rename = "triggerCategory", skip_serializing_if = "Option::is_none")]
    pub trigger_category: Option<String>,
    /// Webview hint: render the node's latest output as a JSON
    /// preview inline on the node body. Used by Debug.
    #[serde(default, rename = "showDebugPreview")]
    pub show_debug_preview: bool,
}

/// Describes how a config field of a given type contributes to a
/// node's ports at compile time. Used by nodes with
/// `has_form_schema` (HumanQuery, runner triggers). The enrich pass
/// reads this, iterates the configured fields, and materializes
/// inputs/outputs on the NodeDefinition.
#[derive(Debug, Clone)]
pub struct FormFieldSpec {
    /// Value of the field's `field_type.kind` this spec matches
    /// (e.g. "text", "select", "file").
    pub field_type: &'static str,
    /// Default render metadata applied to the field if not
    /// overridden in the weft source.
    pub render: Value,
    pub adds_inputs: Vec<FormFieldPort>,
    pub adds_outputs: Vec<FormFieldPort>,
}

#[derive(Debug, Clone)]
pub struct FormFieldPort {
    pub name_template: &'static str,
    pub port_type: WeftType,
}

impl FormFieldPort {
    pub fn new(name_template: &'static str, type_str: &str) -> Self {
        Self {
            name_template,
            port_type: WeftType::parse(type_str)
                .unwrap_or_else(|| panic!("invalid port type: {type_str}")),
        }
    }

    /// Port template accepting any type, independent from sibling
    /// ports. See `T_Auto` handling in enrich.
    pub fn any(name_template: &'static str) -> Self {
        Self { name_template, port_type: WeftType::type_var("T_Auto") }
    }

    pub fn resolve_name(&self, key: &str) -> String {
        self.name_template.replace("{key}", key)
    }
}

/// Catalog lookup. The compiler uses this to resolve each graph
/// node's `node_type` to its metadata at enrich time. Both the
/// stdlib catalog and the user's project `nodes/` folder produce an
/// implementation of this trait.
pub trait NodeCatalog: Send + Sync {
    fn lookup(&self, node_type: &str) -> Option<&dyn Node>;
    /// All known node types (for describe queries).
    fn all(&self) -> Vec<&'static str>;
    /// FormFieldSpecs for a given node type (nodes with
    /// `has_form_schema` declare these to drive port derivation).
    /// Default implementation returns an empty slice.
    fn form_field_specs(&self, _node_type: &str) -> &[FormFieldSpec] {
        &[]
    }
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

    pub fn get(&self, port: &str) -> Option<&Value> {
        self.outputs.get(port)
    }
}

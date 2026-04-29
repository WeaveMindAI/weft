use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::ExecutionContext;
use crate::error::WeftResult;
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

    /// Run this node. `ctx` provides language primitives
    /// (`await_signal`, `report_cost`, `log`). Input values are
    /// pre-resolved on ctx.
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput>;
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
    /// Entry-use wake signals declared by this node. Empty = normal
    /// node (no trigger role). Only the tag (signal kind +
    /// is_resume) is stored in metadata; values come from the
    /// node's config, resolved by `WakeSignalKind::resolve_from_config`
    /// during enrich. Node authors do no value plumbing here.
    #[serde(default, rename = "entrySignals", alias = "entry")]
    pub entry_signals: Vec<crate::primitive::WakeSignalTag>,
    /// Whether this node requires wiring to a sidecar-backed infra
    /// node. Computed from `entry` containing `Event`, but allowed as
    /// an explicit flag for nodes that use infra without entry.
    #[serde(default)]
    pub requires_infra: bool,
    /// Node-level semantic constraints. Small, extensible.
    #[serde(default)]
    pub features: NodeFeatures,
    /// Declarative validation rules. Evaluated against the project's
    /// graph state by the compiler's validate pass. Closed grammar
    /// (see `ValidationRule` / `Condition`); no user Rust runs.
    /// Rules carry a `level` that distinguishes `structural` (checked
    /// on parse / AI edits) from `runtime` (checked only at run time
    /// so a missing credential is fine in the editor).
    #[serde(default)]
    pub validate: Vec<ValidationRule>,
    /// Optional path to this node's form field specs JSON. Resolved
    /// relative to the owning package root. Lets multi-node
    /// packages share one specs file across their nodes (e.g.
    /// HumanQuery and HumanTrigger both point at the package's
    /// `form_field_specs.json`). Defaults to `form_field_specs.json`
    /// at load time when absent.
    #[serde(default, rename = "formFieldSpecsRef", alias = "form_field_specs_ref")]
    pub form_field_specs_ref: Option<String>,
}

/// One validation rule. The `when` condition is evaluated against the
/// node in context; if it evaluates to `true`, the rule fires and
/// `then` is emitted as a diagnostic. Read as "when X is true, this
/// is a problem: Y."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    pub when: Condition,
    pub then: RuleDiagnostic,
}

/// Severity level of a validation rule. Controls when the rule runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidationLevel {
    /// Checked at parse / edit time. Missing these is a code error.
    Structural,
    /// Checked only at run time (e.g. missing credentials). The
    /// editor does not flag these so the AI builder and the human
    /// user can sketch a project without filling secrets.
    Runtime,
}

impl Default for ValidationLevel {
    fn default() -> Self {
        ValidationLevel::Structural
    }
}

/// Diagnostic body emitted when a rule fires. Placeholder tokens in
/// `message` are replaced from the evaluation context: `{id}` for the
/// node id, `{port}` / `{field}` for `port`/`field` if set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDiagnostic {
    pub message: String,
    /// When the rule runs. `structural` fires at edit/parse time;
    /// `runtime` is deferred to run time (for "missing credential"
    /// style checks that shouldn't block editing).
    #[serde(default)]
    pub level: ValidationLevel,
    /// How serious the diagnostic is. Defaults to `error`.
    #[serde(default)]
    pub severity: RuleSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

impl Default for RuleSeverity {
    fn default() -> Self {
        RuleSeverity::Error
    }
}

/// Closed-grammar condition language. Each variant is evaluated
/// against a `RuleContext` (the node being validated + the enriched
/// project). No loops, no recursion on user data; only structural
/// boolean combinators plus a small set of graph/config queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Condition {
    /// Port has a value available at compile time: either a wired
    /// incoming edge OR a same-named config literal (when the port
    /// is `configurable`). This is the "is my input provided" check
    /// that handles the literal-assignment case (e.g. `Llm { prompt:
    /// "hello" }`) the same as the wired case.
    InputSatisfied { port: String },
    /// Port has a wired incoming edge specifically (no config-literal
    /// shortcut). Rare; prefer `InputSatisfied`.
    InputWired { port: String },
    /// Port's incoming edge(s) all come from a node whose
    /// `node_type` equals `equals`. True vacuously if the port has
    /// no wired edges (pair with `InputWired` or `InputSatisfied`
    /// when you want to require both).
    InputSourceType { port: String, equals: String },
    /// Config field exists (any non-null value).
    ConfigPresent { field: String },
    /// Config field is present and non-empty (for strings: not ""
    /// after trim; for arrays: length > 0; for objects: has keys).
    ConfigNonempty { field: String },
    /// Config field equals a specific JSON value.
    ConfigEquals { field: String, equals: Value },
    /// Config field's string value is in a whitelist.
    ConfigInSet { field: String, values: Vec<String> },
    /// Config field's string value matches a regex. Vacuously true
    /// if the field is absent or non-string; pair with
    /// `config_present` when absence itself should fail.
    ConfigMatches { field: String, regex: String },
    /// All sub-conditions must hold.
    All { of: Vec<Condition> },
    /// At least one sub-condition must hold.
    Any { of: Vec<Condition> },
    /// Negation.
    Not { of: Box<Condition> },
}

/// Node-level semantic constraints. All optional; empty by default.
///
/// KEEP IN SYNC with the TypeScript `NodeDefinition.features` shape
/// in `extension-vscode/src/shared/protocol.ts`. Serde silently drops
/// unknown fields from node metadata.json when this struct doesn't
/// declare them, so a field that only exists in TS (or only in
/// metadata.json) will be invisible to the dispatcher and lost on
/// the wire back to the webview. If you add a feature here, also:
///   1. Add the matching camelCase field to `NodeDefinition.features`
///      in protocol.ts.
///   2. Update any webview code that switches on the new feature.
/// TODO(codegen): replace with ts-rs or specta so TS mirrors Rust
/// automatically.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeFeatures {
    /// Each inner list is a port group where at least ONE port must
    /// be non-null. If every port in a group is null/missing, the
    /// node is skipped. Example: email send might declare
    /// `one_of_required: [["message", "media"]]`.
    #[serde(default, rename = "oneOfRequired")]
    pub one_of_required: Vec<Vec<String>>,
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
    /// Webview hint: the node opts into the inline live-data
    /// strip (in/out chips) under its body. Nodes that don't opt
    /// in fall back to the modal inspector for inputs/outputs.
    /// Used by WhatsAppBridge (QR + connection status) where
    /// live telemetry from the sidecar is core UX. Regular
    /// action nodes (Send/Receive/Debug) leave this off.
    #[serde(default, rename = "hasLiveData")]
    pub has_live_data: bool,
    /// Default value of the node's `is_output` config flag. Nodes that
    /// are semantically "produce this thing" (Debug, Output) default
    /// to true. Any project can override by setting `is_output` in the
    /// node's weft config. Read at run-dispatch time to compute the
    /// subgraph to execute (see docs/v2-design.md section 3.0).
    #[serde(default, rename = "isOutputDefault")]
    pub is_output_default: bool,
    /// Sidecar spec for `requires_infra: true` nodes. Declares the
    /// image/port/manifests the dispatcher applies during
    /// `weft infra up`. None for non-infra nodes.
    #[serde(default, rename = "sidecar", skip_serializing_if = "Option::is_none")]
    pub sidecar: Option<SidecarSpec>,
    /// Hidden from node picker and describe-nodes output. Used for
    /// compiler-internal node types (Passthrough) that are real
    /// executing nodes but must not appear in user-facing tooling.
    /// Users cannot declare hidden node types in source; the parser
    /// rejects them with a dedicated error.
    #[serde(default, rename = "hidden")]
    pub hidden: bool,
}

/// Describes how a config field of a given type contributes to a
/// node's ports at compile time. Used by nodes with
/// `has_form_schema` (HumanQuery, runner triggers). The enrich pass
/// reads this, iterates the configured fields, and materializes
/// inputs/outputs on the NodeDefinition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormFieldSpec {
    /// Value of the field's `field_type` (or `field_type.kind`)
    /// this spec matches (e.g. "text_input", "approve_reject").
    /// Wire shape is camelCase across every field on this struct
    /// so VS Code + browser extension can read the same TS
    /// interface without per-field bridging. The snake_case
    /// aliases let us still load the on-disk JSON files (which
    /// pre-date this rename).
    #[serde(rename = "fieldType", alias = "field_type")]
    pub field_type: String,
    /// Human-readable label for the form_builder editor's
    /// dropdown (e.g. "Text input", "Approve / Reject").
    #[serde(default)]
    pub label: String,
    /// Default render metadata applied to the field if not
    /// overridden in the weft source. The dashboard / browser
    /// extension reads `render.component` (and its sibling flags)
    /// to pick a UI primitive without knowing field-type strings.
    pub render: Value,
    /// Config keys the form_builder editor must collect when the
    /// user adds this field type (e.g. ["options"] for a static
    /// select). The editor validates these before saving.
    #[serde(default, rename = "requiredConfig", alias = "required_config")]
    pub required_config: Vec<String>,
    /// Config keys the editor exposes but doesn't require (e.g.
    /// "approveLabel" / "rejectLabel" for approve_reject).
    #[serde(default, rename = "optionalConfig", alias = "optional_config")]
    pub optional_config: Vec<String>,
    #[serde(default, rename = "addsInputs", alias = "adds_inputs")]
    pub adds_inputs: Vec<FormFieldPort>,
    #[serde(default, rename = "addsOutputs", alias = "adds_outputs")]
    pub adds_outputs: Vec<FormFieldPort>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormFieldPort {
    #[serde(rename = "nameTemplate", alias = "name_template", alias = "name")]
    pub name_template: String,
    #[serde(rename = "portType", alias = "port_type")]
    pub port_type: WeftType,
}

impl FormFieldPort {
    pub fn new(name_template: impl Into<String>, type_str: &str) -> Self {
        Self {
            name_template: name_template.into(),
            port_type: WeftType::parse(type_str)
                .unwrap_or_else(|| panic!("invalid port type: {type_str}")),
        }
    }

    /// Port template accepting any type, independent from sibling
    /// ports. See `T_Auto` handling in enrich.
    pub fn any(name_template: impl Into<String>) -> Self {
        Self { name_template: name_template.into(), port_type: WeftType::type_var("T_Auto") }
    }

    pub fn resolve_name(&self, key: &str) -> String {
        self.name_template.replace("{key}", key)
    }
}

/// Metadata-only catalog. The compiler (enrich, validate), the
/// dispatcher (describe-nodes, activate), and the IDE (`parse_only`)
/// use this to resolve a `node_type` string to its metadata
/// without compiling the node's Rust code.
///
/// Implementations: `weft-catalog::FsCatalog` walks the filesystem.
/// A test harness can also hand-roll a `HashMap`-backed impl.
pub trait MetadataCatalog: Send + Sync {
    fn lookup(&self, node_type: &str) -> Option<&NodeMetadata>;
    /// Every known node's metadata.
    fn all(&self) -> Vec<&NodeMetadata>;
    /// Form field specs for nodes with `features.has_form_schema`.
    /// Defaults to empty; `FsCatalog` returns statically-known specs
    /// for nodes that declare them alongside their metadata.
    fn form_field_specs(&self, _node_type: &str) -> &[FormFieldSpec] {
        &[]
    }
}

/// Runtime node catalog. Produced by codegen inside the emitted
/// project binary. Users of metadata should use [`MetadataCatalog`]
/// instead; this trait is only for runtime dispatch (the engine's
/// pulse loop calls `lookup(...)?.execute(ctx)`).
pub trait NodeCatalog: Send + Sync {
    /// Return a 'static reference to the node implementation. All
    /// emitted project binaries back their catalog with static
    /// globals, so the 'static bound is satisfied; in-process tests
    /// can use `Box::leak` on a once-cell to produce a compatible
    /// reference.
    fn lookup(&self, node_type: &str) -> Option<&'static dyn Node>;
    fn all(&self) -> Vec<&'static str>;
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

/// Sidecar declaration on an infra node. Declarative; the
/// dispatcher reads it during `weft infra up` and applies the
/// manifests through its `InfraBackend`. Node code is never
/// invoked during provisioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarSpec {
    /// Short name used to construct the image tag
    /// (e.g. `whatsapp-bridge` → `ghcr.io/weavemindai/sidecar-whatsapp-bridge:latest`).
    pub name: String,
    /// Port the sidecar listens on.
    pub port: u16,
    /// Optional path suffix for the action endpoint
    /// (e.g. `/action`). Defaults to empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_path: Option<String>,
    /// Raw k8s manifests to apply. Placeholders like
    /// `__INSTANCE_ID__` and `__NAMESPACE__` are substituted by
    /// the infra backend before apply.
    #[serde(default)]
    pub manifests: Vec<Value>,
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

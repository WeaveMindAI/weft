use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::ExecutionContext;
use crate::error::{WeftError, WeftResult};
use crate::infra::{InfraProvisionContext, InfraSpec};
use crate::weft_type::WeftType;

/// The core trait every node implements. Stdlib nodes in `catalog/`
/// and user-defined nodes under `myproject/nodes/` both implement this.
///
/// Infra nodes (those with `metadata.requires_infra == true`)
/// additionally implement `provision`, which returns an [`InfraSpec`]
/// the dispatcher applies BEFORE calling `execute(phase=InfraSetup)`.
/// Non-infra nodes leave `provision` as the default Err impl; the
/// dispatcher never calls it for them.
#[async_trait]
pub trait Node: Send + Sync {
    /// Stable identifier for this node type. Must be unique across the
    /// project's full catalog (stdlib + user + vendored).
    fn node_type(&self) -> &'static str;

    /// Metadata describing ports, fields, entry primitives. Usually
    /// loaded from a co-located `metadata.json` via `include_str!`.
    fn metadata(&self) -> NodeMetadata;

    /// Build the desired infrastructure for this node. Called by the
    /// engine in `Phase::InfraSetup` BEFORE `execute`. Pure-ish: same
    /// inputs as `execute`, returns the desired k8s state as a typed
    /// value. Side effects beyond constructing the spec (registry
    /// lookups, etc) are allowed because provision is async.
    ///
    /// Pulse outputs are NOT emitted from `provision`; they come
    /// from the subsequent `execute` call in the same node task.
    /// This keeps the trait surface honest about which method owns
    /// each concern: provision = infra shape, execute = pulses.
    ///
    /// The default impl returns Err. It is only ever invoked by the
    /// dispatcher when `metadata().requires_infra == true`; nodes
    /// that opt in MUST override.
    async fn provision(
        &self,
        _ctx: InfraProvisionContext,
        _input: NodeInput,
    ) -> WeftResult<InfraSpec> {
        Err(WeftError::Config(format!(
            "node '{}' declared requires_infra=true but did not implement Node::provision",
            self.node_type()
        )))
    }

    /// Run this node. `ctx` provides language primitives
    /// (`await_signal`, `report_cost`, `log`, `endpoint`). Input
    /// values are pre-resolved on ctx.
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput>;
}

/// Input bag handed to `provision` (mirrors what `execute` reads from
/// `ExecutionContext::input`). Same wire format; separate type so the
/// trait signature doesn't drag an ExecutionContext through the
/// pre-apply call site.
#[derive(Debug, Clone, Default)]
pub struct NodeInput {
    pub values: std::collections::HashMap<String, Value>,
}

impl NodeInput {
    pub fn get<T: serde::de::DeserializeOwned>(&self, port: &str) -> WeftResult<T> {
        let v = self
            .values
            .get(port)
            .ok_or_else(|| WeftError::Input(format!("missing input on port: {port}")))?;
        serde_json::from_value(v.clone())
            .map_err(|e| WeftError::Input(format!("port {port}: {e}")))
    }

    pub fn get_optional<T: serde::de::DeserializeOwned>(&self, port: &str) -> WeftResult<Option<T>> {
        match self.values.get(port) {
            None => Ok(None),
            Some(v) if v.is_null() => Ok(None),
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| WeftError::Input(format!("port {port}: {e}"))),
        }
    }

    pub fn raw(&self, port: &str) -> Option<&Value> {
        self.values.get(port)
    }
}

/// Validation diagnostic. Emitted by the generic validate pass and
/// per-node validators. Mirrored by the VS Code extension's
/// Diagnostic type; wire format matches. `line`/`column` are the START of the
/// culprit (1-based line, 0-based char column); `end_line`/`end_column` bound
/// its end (exclusive) so the editor underlines the exact range, not just a
/// caret. End defaults to start when a producer only knows a point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub line: usize,
    pub column: usize,
    #[serde(default, rename = "endLine")]
    pub end_line: usize,
    #[serde(default, rename = "endColumn")]
    pub end_column: usize,
    pub severity: Severity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl Diagnostic {
    /// A diagnostic bounded to a source `Span` (the culprit's exact range).
    pub fn at(span: crate::project::Span, severity: Severity, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            line: span.start_line,
            column: span.start_column,
            end_line: span.end_line,
            end_column: span.end_column,
            severity,
            message: message.into(),
            code: Some(code.into()),
        }
    }
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
    /// Whether this node implements `Node::provision` and needs
    /// dispatcher-driven infrastructure (k8s pods, services, etc).
    /// Explicit flag in metadata.json; mirrored to
    /// `NodeDefinition.requires_infra` at enrich time.
    #[serde(default)]
    pub requires_infra: bool,
    /// Local image source directories the CLI must build for this
    /// node. Each entry is the relative path (from the package root)
    /// of a directory containing a `Dockerfile`. The directory's
    /// basename becomes the name used in `Image::Local { name }` from
    /// the node's `provision()` body. Example: `["images/bridge"]`
    /// makes `Image::Local { name: "bridge" }` resolvable via the
    /// InfraProvisionContext.
    ///
    /// Empty for non-infra nodes and for infra nodes that only use
    /// upstream images.
    #[serde(default)]
    pub images: Vec<String>,
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
    /// Config field's string value matches a regex. Fail-closed: false if the
    /// field is absent, non-string, or the regex is malformed (matching every
    /// sibling ConfigX condition, which all treat an absent field as not-present).
    /// Wrap in `not` to assert a non-match.
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
    /// Webview hint: render the node's latest output as a JSON
    /// preview inline on the node body. Used by Debug.
    #[serde(default, rename = "showDebugPreview")]
    pub show_debug_preview: bool,
    /// Default value of the node's `is_output` config flag. Nodes that
    /// are semantically "produce this thing" (Debug, Output) default
    /// to true. Any project can override by setting `is_output` in the
    /// node's weft config. Read at run-dispatch time to compute the
    /// subgraph to execute (see docs/v2-design.md section 3.0).
    #[serde(default, rename = "isOutputDefault")]
    pub is_output_default: bool,
    /// Which declared `Endpoint` (by name) the dispatcher proxies
    /// `/live` to. `Some("api")` means the node exposes a `/live`
    /// HTTP surface (runtime status for the graph body panel) at that
    /// endpoint; the dispatcher proxies `/projects/.../infra/nodes/{}/live`
    /// to `<that endpoint's URL>/live`. `None` means no `/live` (the
    /// proxy 404s). One field, no separate `has_live` flag: declaring
    /// the endpoint IS opting in, and opting in REQUIRES naming the
    /// endpoint, so the two can't drift out of sync.
    #[serde(default, rename = "liveEndpoint", skip_serializing_if = "Option::is_none")]
    pub live_endpoint: Option<String>,
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

#[cfg(test)]
mod diagnostic_wire_tests {
    use super::*;
    use crate::project::Span;

    /// Layer-2 wire-shape: `Diagnostic` crosses the CLI->editor boundary as JSON
    /// and the VS Code extension reads `endLine`/`endColumn` (camelCase, the
    /// editor's range-underline bounds). Pin those renamed keys AND a full
    /// round-trip so a `#[serde(rename)]` drift fails here, not in the editor.
    #[test]
    fn diagnostic_wire_keys() {
        let d = Diagnostic::at(Span::single_line(3, 4, 10), Severity::Error, "parse", "boom");
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["line"], 3);
        assert_eq!(v["column"], 4);
        assert!(v.get("endLine").is_some(), "endLine key (camelCase): {v}");
        assert!(v.get("endColumn").is_some(), "endColumn key (camelCase): {v}");
        assert!(v.get("end_line").is_none() && v.get("end_column").is_none(), "no snake_case leak: {v}");
        assert_eq!(v["severity"], "error", "severity is lowercase");
        // Round-trip survives, and an OLD diagnostic with no end bounds still
        // loads (the fields default to 0, a point span at the start).
        let back: Diagnostic = serde_json::from_value(v).expect("round-trip");
        assert_eq!(back.end_line, 3);
        assert_eq!(back.end_column, 10);
        let pointy: Diagnostic = serde_json::from_value(serde_json::json!({
            "line": 1, "column": 0, "severity": "warning", "message": "m"
        })).expect("diagnostic without end bounds still deserializes");
        assert_eq!(pointy.end_line, 0, "absent endLine defaults to 0");
    }
}

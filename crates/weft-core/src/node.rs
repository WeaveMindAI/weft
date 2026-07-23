use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::weft_type::{Exposure, WeftPrimitive, WeftType};

// The `Node` trait is the RUNTIME node interface (it runs against the execution
// context + infra provision context), so it is gated behind `runtime`. The
// browser WASM parse build needs only the node METADATA layer
// (`NodeMetadata` / `MetadataCatalog`), which is pure and stays below.
/// The node's declared surface (`metadata.json`), parsed once and shared
/// for the process lifetime. `#[derive(NodeManifest)]` implements it by
/// embedding the `metadata.json` sitting next to the deriving type's
/// source file; hand-built nodes (test doubles) implement it directly.
/// `Node::node_type` and `Node::metadata` are views of it, so the type
/// identifier has a single source of truth: the json's `type` field.
pub trait NodeManifest {
    fn manifest(&self) -> &'static NodeMetadata;
}

#[cfg(feature = "runtime")]
mod node_trait {
    use async_trait::async_trait;

    use super::NodeManifest;
    use crate::context::{ExecutionContext, ValueBag};
    use crate::error::{WeftError, WeftResult};
    use crate::infra::{InfraProvisionContext, InfraSpec};

    /// The core trait every node implements. Stdlib nodes in `catalog/`
    /// and user-defined nodes under `myproject/nodes/` both implement this.
    /// The identity/surface layer comes from the [`NodeManifest`]
    /// supertrait (usually `#[derive(NodeManifest)]` on the node struct,
    /// which picks up the co-located `metadata.json`).
    ///
    /// A node implements up to three separately-named bodies, and the
    /// ENGINE picks which to call from the manifest (a node never inspects
    /// the lifecycle phase itself):
    ///   - every node writes `run`, its normal body;
    ///   - a trigger (`metadata.is_trigger == true`) additionally writes
    ///     `setup_trigger`, called at registration time instead of `run`;
    ///   - an infra node (`metadata.requires_infra == true`) additionally
    ///     writes `provision_infra`, called at provisioning time before
    ///     `run`.
    #[async_trait]
    pub trait Node: NodeManifest + Send + Sync {
        /// Stable identifier for this node type. Must be unique across the
        /// project's full catalog (stdlib + user + vendored).
        fn node_type(&self) -> &'static str {
            &self.manifest().node_type
        }

        /// Build the desired infrastructure for this node. Returns the
        /// desired k8s state as a typed value; the engine has it applied,
        /// then calls `run`. The default impl returns Err; nodes that
        /// declare `requires_infra=true` MUST override.
        async fn provision_infra(
            &self,
            _ctx: InfraProvisionContext,
            _input: ValueBag,
        ) -> WeftResult<InfraSpec> {
            Err(WeftError::Config(format!(
                "node '{}' declared requires_infra=true but did not implement Node::provision_infra",
                self.node_type()
            )))
        }

        /// Register this trigger's wake signal (`ctx.register_signal`).
        /// The engine calls it INSTEAD of `run` when a trigger is being
        /// set up. Only nodes whose manifest declares `is_trigger=true`
        /// implement it; the default fails loud on the mismatch.
        async fn setup_trigger(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Err(WeftError::Config(format!(
                "node '{}' declared is_trigger=true but did not implement Node::setup_trigger",
                self.node_type()
            )))
        }

        /// The node's normal body. `ctx` provides language primitives
        /// (`pulse_downstream`, `create_bus`, `bus`, `await_signal`,
        /// `provider_access`, `log`, `endpoint`). The ONLY way to fire
        /// downstream is `ctx.pulse_downstream(output)`. For a plain
        /// (non-trigger) node the engine calls this in every lifecycle
        /// phase (a value feeding a trigger's config must be produced at
        /// setup time too); for a trigger it runs only on a real firing,
        /// with the fire's payload fields on the `ctx.wake` bag.
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()>;
    }
}

#[cfg(feature = "runtime")]
pub use node_trait::Node;

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

/// A node's declared surface (ports, fields, features) to the compiler, the
/// dispatcher, and tooling. Two producers, one document: the catalog builds it
/// by reading `metadata.json` from disk (merging the package root's shared
/// defaults), and the runtime copy is [`NodeManifest::manifest`], which the
/// `#[derive(NodeManifest)]` expansion builds by embedding the same two files.
// SYNC: NodeMetadata (describe-nodes serialization) <-> packages/weft-graph/src/protocol.ts
//       CatalogEntry/InputSpec/Widget/OutputSpec
// `deny_unknown_fields`: an unknown key in a `metadata.json` (a typo like
// `providr`, or a stale key) is a loud parse error, not silently dropped. This
// matters most for the package-root defaults file, where one typo would
// otherwise make every member quietly miss the shared key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// The node's inputs: ONE list covering everything a node takes,
    /// wired data and design-time configuration alike. Each input's
    /// `exposure` says where its value may come from; its widget (the
    /// editor control) derives from the type unless overridden.
    #[serde(default)]
    pub inputs: Vec<InputSpec>,
    /// Output ports.
    #[serde(default)]
    pub outputs: Vec<OutputSpec>,
    /// Whether this node implements `Node::provision_infra` and needs
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
    /// Form-field vocabulary for nodes whose `features.has_form_schema`
    /// is true (which field types a form config may use, and what ports
    /// each materializes). Empty for everything else. Multi-node
    /// packages declare it once in the package root's partial
    /// `metadata.json` (HumanQuery and HumanTrigger share one
    /// vocabulary); every member inherits it via the catalog's
    /// package-defaults merge unless its own file carries the key.
    #[serde(
        default,
        rename = "formFieldSpecs",
        alias = "form_field_specs",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub form_field_specs: Vec<FormFieldSpec>,
}

/// The one charset a provider name may use: lowercase ASCII letters,
/// digits, and `_`. This is what makes the name the key's identity with no
/// aliasing: the runtime key lives in `<NAME>_API_KEY`, derived by
/// uppercasing the name (`credential::provider_env_var`). Restricting to
/// this set makes that derivation injective (two distinct names can never
/// share one env var) and rules out a name that would form a malformed or
/// surprising env var (spaces, dots, `-` vs `_`, unicode, empty). A raw
/// string comparison of names is therefore exactly a comparison of key
/// identities. Enforced where a provider name enters the system (the
/// access request handler), so a bad name is refused loudly at the door.
pub fn is_valid_provider_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Keys a package root's partial `metadata.json` may never supply as a
/// default, because they are one node's identity, not something a package
/// shares. `type` is the node's catalog id; `label` and `description` name
/// this one node to a human. A package root carrying any of them is an
/// authoring mistake, so it is a loud error, not a silent default.
// SYNC: NON_INHERITABLE_METADATA_KEYS <-> crates/weft-node-derive/src/lib.rs NON_INHERITABLE_KEYS
pub const NON_INHERITABLE_METADATA_KEYS: [&str; 3] = ["type", "label", "description"];

/// Merge a package root's partial defaults into a member's `metadata.json`
/// value, key-by-key at the top level, the member's own key winning
/// wholesale (no deep merge). Both must be JSON objects. This is THE one
/// definition of the package-defaults semantics: the catalog (build side)
/// and `#[derive(NodeManifest)]` (runtime side) both merge through here so
/// a node's metadata is one document, never two that disagree.
///
/// `Err` names the offending key/shape. A `defaults` carrying a
/// [`NON_INHERITABLE_METADATA_KEYS`] key is refused (an identity key is
/// never a package default). Callers map the error into their own type.
pub fn merge_package_defaults(
    member: &mut serde_json::Value,
    defaults: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let member = member.as_object_mut().ok_or("metadata.json must be a JSON object")?;
    for key in NON_INHERITABLE_METADATA_KEYS {
        if defaults.contains_key(key) {
            return Err(format!(
                "package-level metadata.json must not set `{key}`: it is one node's identity, \
                 not a package default"
            ));
        }
    }
    for (key, default) in defaults {
        if !member.contains_key(key) {
            member.insert(key.clone(), default.clone());
        }
    }
    Ok(())
}

impl NodeMetadata {
    /// Parse a compile-time-embedded `metadata.json`, merging the package
    /// root's partial defaults (`defaults_json`, the sibling package
    /// `metadata.json` when the node is a package member; `None` for a bare
    /// node) key-by-key through [`merge_package_defaults`]. Called by the
    /// `#[derive(NodeManifest)]` expansion, which embeds BOTH files, so the
    /// runtime `manifest()` is the SAME merged document the catalog builds.
    ///
    /// `site` names the deriving node and its file so a mismatch panics with
    /// a pointer to the culprit. This is the runtime backstop: the catalog's
    /// typed parse runs the identical merge at `weft build` and fails there
    /// first, so a schema-invalid file never reaches a shipped image.
    pub fn parse_embedded(member_json: &str, defaults_json: Option<&str>, site: &str) -> Self {
        let mut value: serde_json::Value = serde_json::from_str(member_json)
            .unwrap_or_else(|e| panic!("{site}: metadata.json is not valid JSON: {e}"));
        if let Some(defaults_json) = defaults_json {
            // The offender here is the PACKAGE ROOT's metadata.json, so say so:
            // the member file named in `site` is not the one at fault.
            let defaults: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(defaults_json).unwrap_or_else(|e| {
                    panic!(
                        "{site}: the package root's metadata.json is not a JSON object of \
                         defaults: {e}"
                    )
                });
            merge_package_defaults(&mut value, &defaults).unwrap_or_else(|e| {
                panic!("{site}: the package root's metadata.json is invalid: {e}")
            });
        }
        serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("{site}: metadata.json does not fit NodeMetadata: {e}"))
    }

    /// Semantic metadata rules serde cannot express. One name = one
    /// input: duplicate input names are rejected (each name has exactly
    /// one home). A select/multiselect widget must declare options (an
    /// optionless picker can never be filled). A declared `default` must
    /// type-check against the input's type, and a number widget's default
    /// must sit inside its declared min/max, so a default the runtime
    /// would later reject fails the metadata load instead.
    pub fn validate_semantics(&self) -> Result<(), String> {
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for input in &self.inputs {
            if !seen.insert(input.name.as_str()) {
                return Err(format!(
                    "duplicate input '{}': every input name must be unique",
                    input.name
                ));
            }
            match input.effective_widget() {
                Widget::Select { options } | Widget::Multiselect { options }
                    if options.is_empty() =>
                {
                    return Err(format!(
                        "input '{}': a select/multiselect widget must declare a non-empty \
                         `options` list",
                        input.name
                    ));
                }
                // A credential is design-time configuration by nature: it
                // must never arrive over a wire or an assignment
                // statement. Enforcing the exposure here lets every
                // consumer treat "api_key input" and "config input" as
                // the same fact instead of re-checking it.
                Widget::ApiKey { .. } if input.effective_exposure() != Exposure::Config => {
                    return Err(format!(
                        "input '{}': an api_key widget requires `exposure: config`",
                        input.name
                    ));
                }
                _ => {}
            }
            // A widget implies the SHAPE of the value it edits. On an
            // input whose type cannot hold that shape, the widget's own
            // contract (a number widget's min/max, a checkbox's bool)
            // becomes silently unenforceable downstream, so the
            // mismatch is rejected here. Unresolved types (TypeVar,
            // MustOverride) pass: is_compatible is permissive there and
            // the resolved instance re-derives its widget.
            let widget_value_type = match input.effective_widget() {
                Widget::Number { .. } => Some(WeftType::primitive(WeftPrimitive::Number)),
                Widget::Checkbox => Some(WeftType::primitive(WeftPrimitive::Boolean)),
                Widget::Text
                | Widget::Password
                | Widget::Code { .. }
                | Widget::ApiKey { .. }
                | Widget::Select { .. } => Some(WeftType::primitive(WeftPrimitive::String)),
                Widget::Multiselect { .. } => {
                    Some(WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String))))
                }
                // textarea is the GENERIC value editor (the derived
                // default for structural types: the value is edited as
                // JSON text), so it constrains nothing. form_builder
                // edits the form-schema blob, file_drop a file marker;
                // both are shape-checked elsewhere.
                Widget::Textarea | Widget::FormBuilder | Widget::FileDrop { .. } => None,
            };
            if let Some(value_type) = widget_value_type {
                if !WeftType::is_compatible(&value_type, &input.input_type) {
                    return Err(format!(
                        "input '{}': a {} widget edits a {} value, which type {} cannot hold",
                        input.name,
                        input.effective_widget().kind_name(),
                        value_type,
                        input.input_type
                    ));
                }
            }
            if let Some(default) = &input.default {
                let inferred = WeftType::infer(default);
                if !WeftType::is_compatible(&inferred, &input.input_type) {
                    return Err(format!(
                        "input '{}': default value has type {} but the input declares {}",
                        input.name, inferred, input.input_type
                    ));
                }
                if let Widget::Number { min, max, .. } = input.effective_widget() {
                    if let Some(n) = default.as_f64() {
                        if min.is_some_and(|m| n < m) || max.is_some_and(|m| n > m) {
                            return Err(format!(
                                "input '{}': default {} is outside the widget's min/max range",
                                input.name, n
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// A copy with every input's `exposure` and `widget` RESOLVED to
    /// their effective values (the author's explicit choice, else the
    /// type-derived default). This is what ships on the wire to the
    /// editor, so the editor never re-derives either; resolving is
    /// idempotent (a resolved metadata re-parses to itself).
    pub fn resolved(&self) -> Self {
        let mut out = self.clone();
        for input in &mut out.inputs {
            input.exposure = Some(input.exposure.unwrap_or_else(|| input.input_type.default_exposure()));
            input.widget = Some(input.effective_widget());
        }
        out
    }
}

/// One validation rule. The `when` condition is evaluated against the
/// node in context; if it evaluates to `true`, the rule fires and
/// `then` is emitted as a diagnostic. Read as "when X is true, this
/// is a problem: Y."
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Condition {
    /// Port has a value available at compile time: either a wired
    /// incoming edge OR a body literal (where the port's literal
    /// placement allows one). This is the "is my input provided" check
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
/// `deny_unknown_fields`: a key here that this struct doesn't declare (a
/// typo, or a feature that only exists in TS) is a loud parse error at
/// metadata load, not a value silently dropped and lost on the wire back
/// to the webview. If you add a feature here, also:
///   1. Add the matching camelCase field to `NodeFeaturesWire` in
///      protocol.ts.
///   2. Update any webview code that switches on the new feature.
// SYNC: NodeFeatures <-> packages/weft-graph/src/protocol.ts NodeFeaturesWire
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFeatures {
    /// Each inner list is a port group where at least ONE port must
    /// be non-null. If every port in a group is null/missing, the
    /// node is skipped. Example: email send might declare
    /// `one_of_required: [["message", "attachment"]]`.
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
    /// Webview hint: when the node's input is a stored image
    /// reference, render the picture inline on the node body, fetched
    /// through the authenticated download handshake. Used by
    /// ImageDisplay.
    // SYNC: showImagePreview <-> packages/weft-graph/src/protocol.ts NodeFeaturesWire.showImagePreview
    #[serde(default, rename = "showImagePreview")]
    pub show_image_preview: bool,
    /// Webview hint: when the node's input is a stored-file
    /// reference, render a download button inline on the node body
    /// (the click runs the same handshake a CLI download uses). Used
    /// by DownloadLink.
    // SYNC: showDownloadLink <-> packages/weft-graph/src/protocol.ts NodeFeaturesWire.showDownloadLink
    #[serde(default, rename = "showDownloadLink")]
    pub show_download_link: bool,
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
    /// Hidden from the node picker and describe-nodes output. For a
    /// catalog node type that executes but must not appear in
    /// user-facing tooling. Users cannot declare a hidden node type
    /// in source; the parser rejects them with a dedicated error.
    /// (Group/Loop boundary lowering does NOT use this: those are
    /// not catalog nodes, they're inline-dispatched in the engine.)
    #[serde(default, rename = "hidden")]
    pub hidden: bool,
}

/// Describes how a config field of a given type contributes to a
/// node's ports at compile time. Used by nodes with
/// `has_form_schema` (HumanQuery, runner triggers). The enrich pass
/// reads this, iterates the configured fields, and materializes
/// inputs/outputs on the NodeDefinition.
// SYNC: FormFieldSpec <-> packages/weft-graph/src/protocol.ts FormFieldSpecWire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
}

/// Runtime node catalog. Produced by codegen inside the emitted
/// project binary. Users of metadata should use [`MetadataCatalog`]
/// instead; this trait is only for runtime dispatch (the engine's
/// pulse loop calls `lookup(...)?.execute(ctx)`). Runtime-gated because
/// `lookup` returns the runtime `Node` trait.
#[cfg(feature = "runtime")]
pub trait NodeCatalog: Send + Sync {
    /// Return a 'static reference to the node implementation. All
    /// emitted project binaries back their catalog with static
    /// globals, so the 'static bound is satisfied; in-process tests
    /// can use `Box::leak` on a once-cell to produce a compatible
    /// reference.
    fn lookup(&self, node_type: &str) -> Option<&'static dyn Node>;
    fn all(&self) -> Vec<&'static str>;
}

/// One declared INPUT of a node: everything the node takes, wired data
/// and design-time configuration alike, under one name in one namespace.
/// `exposure` says where a value may come from (see [`Exposure`]);
/// `widget` overrides the type-derived editor control; `default` is the
/// value the runtime supplies when nothing else drives the input (never
/// written into source).
// NAMING CONVENTION (the three stages a graph concept lives through):
//   *Spec       = authored intent (metadata.json / a registration): blanks are
//                 meaningful ("derive from the type", "decide later"). InputSpec,
//                 OutputSpec, FormFieldSpec, SignalSpec, InfraSpec.
//   Parsed*     = raw from .weft source, pre-enrichment (compiler-internal).
//   *Definition = compiled, fully resolved; the editor and runtime trust it
//                 blindly and never re-derive. NodeDefinition, InputDefinition,
//                 PortDefinition, GroupDefinition, ProjectDefinition.
// A new type for one of these stages takes the stage's suffix.
// SYNC: InputSpec <-> packages/weft-graph/src/protocol.ts InputSpec
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub input_type: WeftType,
    #[serde(default)]
    pub required: bool,
    /// Where a value may come from. An explicit level is the author's
    /// choice and wins both ways; absent falls to the input TYPE's
    /// default (`default_exposure`). `Option` so an author can both open
    /// a type beyond its default (a file input to `"all"`) and close one
    /// below it (a String input to `"wire"` or `"config"`).
    // SYNC: InputSpec.exposure <-> packages/weft-graph/src/protocol.ts InputSpec.exposure
    #[serde(default)]
    pub exposure: Option<Exposure>,
    /// The editor control for this input's literal. Absent = derived
    /// from the type ([`Widget::default_for_type`]); declared to pick a
    /// richer control (a select over a String, an api_key, a code box).
    #[serde(default)]
    pub widget: Option<Widget>,
    /// The value the runtime supplies when no wire and no literal
    /// drives this input. Consulted at run time and rendered by the
    /// editor as the effective value; never written into source.
    /// `required` + `default` = satisfiable.
    #[serde(default)]
    pub default: Option<Value>,
    /// Human-readable label shown on the editor field. Absent = the name.
    #[serde(default)]
    pub label: Option<String>,
    /// Hint text shown in the editor field when it is empty.
    #[serde(default)]
    pub placeholder: Option<String>,
    /// Human-readable description shown next to the input in the editor.
    /// The webview reads it; the compiler treats it as opaque.
    #[serde(default)]
    pub description: Option<String>,
}

impl InputSpec {
    /// The input's effective exposure: the author's explicit level, else
    /// the type default.
    pub fn effective_exposure(&self) -> Exposure {
        self.exposure.unwrap_or_else(|| self.input_type.default_exposure())
    }

    /// The input's effective widget: the author's declared control, else
    /// the type-derived default.
    pub fn effective_widget(&self) -> Widget {
        self.widget.clone().unwrap_or_else(|| Widget::default_for_type(&self.input_type))
    }
}

// SYNC: OutputSpec <-> packages/weft-graph/src/protocol.ts OutputSpec
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub port_type: WeftType,
    #[serde(default)]
    pub required: bool,
    /// Human-readable description shown next to the port in the editor.
    /// The webview reads it; the compiler treats it as opaque.
    #[serde(default)]
    pub description: Option<String>,
}

/// The editor control an input renders. The vocabulary of the node
/// inspector: every input has exactly one effective widget (declared, or
/// derived from the type via [`Widget::default_for_type`]).
// SYNC: Widget <-> packages/weft-graph/src/protocol.ts Widget/WidgetKind
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Widget {
    Text,
    Textarea,
    /// `language` picks the syntax highlighting in the editor's code box
    /// (e.g. "python", "javascript").
    Code { language: String },
    /// `step` is the input's granularity (the arrow/slider increment), the
    /// third knob of a number input alongside `min`/`max`.
    Number { min: Option<f64>, max: Option<f64>, step: Option<f64> },
    Checkbox,
    Select { options: Vec<String> },
    Multiselect { options: Vec<String> },
    Password,
    ApiKey { provider: String },
    FormBuilder,
    /// Editor file picker: the user picks a project file (drop/browse) or
    /// pastes a URL, and the input's value becomes a media
    /// `@asset("<path-or-url>", <Type>)` ref. The pre-build asset sync
    /// publishes referenced files to storage and the compile substitutes the
    /// stored-file value, so source never carries storage keys.
    ///
    /// `file_type` is the weft file type this widget picks (`Image`, `Audio`,
    /// `Video`, `Blob`, or the `File`/`Media` unions): it drives the editor's
    /// accept filter + drop validation AND the `, <Type>)` annotation written
    /// into source. Deserializing through [`WeftType`] validates the string
    /// at the metadata boundary (a typo'd type fails the node's metadata
    /// load, not a later marker parse). `accept` optionally NARROWS the
    /// derived filter (e.g. `image/png` under `Image`).
    // SYNC: Widget::FileDrop <-> packages/weft-graph/src/protocol.ts WidgetKind 'file_drop' + Widget.type,
    //       packages/weft-graph/src/webview/lib/utils/file-browser.ts acceptForFileType
    FileDrop {
        accept: Option<String>,
        #[serde(default = "file_drop_default_type", rename = "type")]
        file_type: crate::weft_type::WeftType,
    },
}

impl Widget {
    /// Stable lowercase kind tag, matching the serde `kind` form.
    /// The serde tag is the one source of truth; this match only exists
    /// to hand back a `&'static str` (serde would allocate), and a test
    /// (`kind_name_matches_the_serde_tag`) pins every arm to the tag so
    /// the two can never drift. Adding a widget kind: the exhaustive
    /// matches here and in `validate_semantics` are compiler-enforced.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Widget::Text => "text",
            Widget::Textarea => "textarea",
            Widget::Code { .. } => "code",
            Widget::Number { .. } => "number",
            Widget::Checkbox => "checkbox",
            Widget::Select { .. } => "select",
            Widget::Multiselect { .. } => "multiselect",
            Widget::Password => "password",
            Widget::ApiKey { .. } => "api_key",
            Widget::FormBuilder => "form_builder",
            Widget::FileDrop { .. } => "file_drop",
        }
    }

    /// The ONE WeftType -> default-widget mapping. A file-valued type
    /// gets the drop/pick control (its accept filter derives from the
    /// same type), Boolean a checkbox, Number a number box, and
    /// everything else (String, JSON-ish containers, mixed unions) a
    /// text area: JSON is typed as text, exactly like the source, and
    /// the compiler type-checks the literal against the declared type.
    pub fn default_for_type(ty: &WeftType) -> Widget {
        if ty.is_file_valued() {
            return Widget::FileDrop { accept: None, file_type: ty.clone() };
        }
        match ty {
            WeftType::Primitive(crate::weft_type::WeftPrimitive::Boolean) => Widget::Checkbox,
            WeftType::Primitive(crate::weft_type::WeftPrimitive::Number) => {
                Widget::Number { min: None, max: None, step: None }
            }
            _ => Widget::Textarea,
        }
    }
}

/// A `file_drop` field with no declared type picks ANY file (the `File`
/// union): every stored kind qualifies, and the written annotation is
/// `, File)`.
fn file_drop_default_type() -> crate::weft_type::WeftType {
    crate::weft_type::WeftType::file()
}

/// What a node emits when it's done.
#[derive(Debug, Clone, Default)]
pub struct NodeOutput {
    /// One value per declared output port. Missing ports are treated
    /// as "no pulse emitted" (not "null pulse emitted").
    pub outputs: std::collections::HashMap<String, Value>,
}

impl NodeOutput {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set `port`'s value. Takes anything that converts to JSON directly
    /// (bools, numbers, strings, an already-built `Value`); a `Value`
    /// passes through untouched, never double-wrapped. For a struct,
    /// build the value at the call site (`serde_json::json!` / `to_value`).
    pub fn set(mut self, port: impl Into<String>, value: impl Into<Value>) -> Self {
        self.outputs.insert(port.into(), value.into());
        self
    }

    /// Fan every top-level key of a JSON object onto a same-named
    /// output port, preserving the field value. Useful for nodes that
    /// forward an upstream payload verbatim (trigger seeds, bridge
    /// `/outputs` responses). Returns `self` unchanged if the value
    /// isn't an object.
    ///
    /// **Precedence rule: last-write-wins, chain order = precedence.**
    /// `.set(k, x).extend_from_object({k: y})` ends up with `k = y` (the
    /// helper overwrites the prior set). `.extend_from_object({k: y}).set(k, x)`
    /// ends up with `k = x` (the explicit set overrides the merge). Both
    /// orderings are pinned by tests in `node_output_tests`. A node that
    /// wants a port it computes itself to win therefore sets it AFTER
    /// the fan.
    pub fn extend_from_object(mut self, source: &Value) -> Self {
        if let Value::Object(map) = source {
            for (k, v) in map {
                self.outputs.insert(k.clone(), v.clone());
            }
        }
        self
    }

    /// Like [`Self::extend_from_object`], but fans ONLY the keys that
    /// the node actually declared as output ports (`declared`). A key
    /// present in the object but not declared is silently skipped:
    /// the node promised those ports, the dynamic payload may carry
    /// extras, and emitting an undeclared port would trip the
    /// runtime's loud rejection AFTER a paid / irreversible call (an
    /// LLM completion, an HTTP POST already sent). Intersecting here
    /// is the honest contract: "emit the declared fields I have."
    ///
    /// Nodes use it through [`crate::ExecutionContext::fan_declared`],
    /// which supplies the declared set. Same last-write-wins precedence
    /// as `extend_from_object`.
    // The only caller lives in the runtime-gated context module; without
    // the gate this would false-positive as dead in the parse-only build.
    #[cfg_attr(not(feature = "runtime"), allow(dead_code))]
    pub(crate) fn extend_from_declared(
        mut self,
        source: &Value,
        declared: &std::collections::HashMap<String, WeftType>,
    ) -> Self {
        if let Value::Object(map) = source {
            for (k, v) in map {
                if declared.contains_key(k) {
                    self.outputs.insert(k.clone(), v.clone());
                }
            }
        }
        self
    }

    pub fn get(&self, port: &str) -> Option<&Value> {
        self.outputs.get(port)
    }
}

#[cfg(test)]
mod node_output_tests {
    use super::*;
    use serde_json::json;

    /// Build a declared-output map from port names. `extend_from_declared`
    /// only consults membership, so the type is an irrelevant placeholder.
    fn declared_ports(names: &[&str]) -> std::collections::HashMap<String, WeftType> {
        names
            .iter()
            .map(|n| (n.to_string(), WeftType::MustOverride))
            .collect()
    }

    /// `set` accepts plain Rust values AND an already-built `Value`; the
    /// latter passes through byte-identical, never double-wrapped.
    #[test]
    fn set_converts_plain_values_and_passes_json_through_untouched() {
        let prebuilt = json!({"nested": [1, 2, {"deep": true}]});
        let out = NodeOutput::new()
            .set("b", true)
            .set("n", 42u64)
            .set("s", "text")
            .set("v", prebuilt.clone());
        assert_eq!(out.outputs.get("b"), Some(&json!(true)));
        assert_eq!(out.outputs.get("n"), Some(&json!(42)));
        assert_eq!(out.outputs.get("s"), Some(&json!("text")));
        assert_eq!(out.outputs.get("v"), Some(&prebuilt), "a Value must pass through unchanged");
    }

    #[test]
    fn extend_from_object_fans_keys_onto_ports() {
        let src = json!({"a": 1, "b": "two", "c": null});
        let out = NodeOutput::new().extend_from_object(&src);
        assert_eq!(out.outputs.len(), 3);
        assert_eq!(out.outputs.get("a"), Some(&json!(1)));
        assert_eq!(out.outputs.get("c"), Some(&Value::Null), "user-emitted null is data; engine doesn't strip it");
    }

    #[test]
    fn extend_from_declared_only_fans_declared_keys() {
        let src = json!({"sentiment": "positive", "score": 0.9, "extra": "ignored"});
        let declared = declared_ports(&["sentiment", "score", "response"]);
        let out = NodeOutput::new().extend_from_declared(&src, &declared);
        assert_eq!(out.outputs.len(), 2, "only declared keys present in the object are fanned");
        assert!(out.outputs.contains_key("sentiment"));
        assert!(out.outputs.contains_key("score"));
        assert!(
            !out.outputs.contains_key("extra"),
            "an undeclared model key is dropped, not emitted (would trip the undeclared-port error)"
        );
    }

    #[test]
    fn extend_from_object_no_op_on_non_object() {
        for src in [json!(null), json!(42), json!("string"), json!([1, 2, 3])] {
            let out = NodeOutput::new().extend_from_object(&src);
            assert!(out.outputs.is_empty(), "non-object: helper leaves outputs untouched");
        }
    }

    #[test]
    fn extend_from_object_overwrites_a_prior_set() {
        let src = json!({"a": "new"});
        let out = NodeOutput::new()
            .set("a", json!("prior"))
            .extend_from_object(&src);
        // Same key: the fan DOES overwrite. Chain order is precedence.
        assert_eq!(out.outputs.get("a"), Some(&json!("new")));
    }

    /// Mirror of the prior test in the opposite order: fan FIRST, `set`
    /// SECOND, the later `set` wins. This ordering is the contract
    /// catalog callers rely on: a node fans the dynamic payload, then
    /// sets the ports it computes itself (its primary port, a locally
    /// resolved URL), so a payload key can never shadow the node's own
    /// truth. Last-write-wins is the rule; the chain order is the
    /// precedence.
    #[test]
    fn extend_from_object_then_set_lets_set_win() {
        let src = json!({"a": "from_object"});
        let out = NodeOutput::new()
            .extend_from_object(&src)
            .set("a", json!("from_set"));
        assert_eq!(out.outputs.get("a"), Some(&json!("from_set")));
    }

    #[test]
    fn extend_from_declared_then_set_lets_set_win() {
        let src = json!({"response": "payload-shadow", "field": "x"});
        let declared = declared_ports(&["response", "field"]);
        let out = NodeOutput::new()
            .extend_from_declared(&src, &declared)
            .set("response", json!("full-object"));
        assert_eq!(out.outputs.get("response"), Some(&json!("full-object")));
        assert_eq!(out.outputs.get("field"), Some(&json!("x")));
    }
}

/// A typo or stale key ANYWHERE in a `metadata.json` is a loud parse error,
/// not a silently-dropped value. `deny_unknown_fields` lives on every struct
/// the file deserializes into (not just the top-level `NodeMetadata`), so a
/// misspelled key inside a port, field, feature, or form-field entry fails
/// the node's metadata load instead of vanishing on the way to the webview.
#[cfg(test)]
mod deny_unknown_tests {
    use super::*;
    use serde_json::json;

    fn base() -> serde_json::Value {
        json!({
            "type": "T", "label": "L", "description": "D", "category": "C"
        })
    }

    fn err(mut meta: serde_json::Value, key: &str, entry: serde_json::Value) -> String {
        meta.as_object_mut().unwrap().insert(key.into(), json!([entry]));
        serde_json::from_value::<NodeMetadata>(meta)
            .expect_err("unknown nested key must be rejected")
            .to_string()
    }

    #[test]
    fn unknown_port_key_rejected() {
        let e = err(base(), "inputs", json!({"name": "x", "type": "String", "requird": true}));
        assert!(e.contains("requird"), "error names the typo'd key: {e}");
    }

    /// The `fields` metadata array is GONE: a metadata.json still carrying
    /// one fails the load loudly (deny_unknown_fields), pointing at the
    /// unified `inputs` shape instead of silently dropping the widgets.
    #[test]
    fn legacy_fields_array_rejected() {
        let e = err(base(), "fields", json!({
            "key": "k", "label": "l", "field_type": {"kind": "text"}
        }));
        assert!(e.contains("fields"), "error names the dead key: {e}");
    }

    #[test]
    fn input_carries_widget_default_label_placeholder() {
        let mut meta = base();
        meta.as_object_mut().unwrap().insert("inputs".into(), json!([{
            "name": "model", "type": "String", "exposure": "config",
            "widget": {"kind": "text"}, "default": "sonnet", "label": "Model",
            "placeholder": "anthropic/claude-sonnet-4.6"
        }]));
        let parsed: NodeMetadata = serde_json::from_value(meta).expect("input loads");
        let input = &parsed.inputs[0];
        assert_eq!(input.effective_exposure(), crate::weft_type::Exposure::Config);
        assert_eq!(input.effective_widget(), Widget::Text);
        assert_eq!(input.default, Some(json!("sonnet")));
        assert_eq!(input.label.as_deref(), Some("Model"));
        assert_eq!(input.placeholder.as_deref(), Some("anthropic/claude-sonnet-4.6"));
    }

    /// `min`/`max`/`step` are the number widget's three knobs and all three
    /// must survive the load.
    #[test]
    fn number_widget_carries_min_max_step() {
        let mut meta = base();
        meta.as_object_mut().unwrap().insert("inputs".into(), json!([{
            "name": "temperature", "type": "Number", "exposure": "config",
            "widget": {"kind": "number", "min": 0.0, "max": 2.0, "step": 0.1}
        }]));
        let parsed: NodeMetadata = serde_json::from_value(meta).expect("number widget loads");
        match parsed.inputs[0].effective_widget() {
            Widget::Number { min, max, step } => {
                assert_eq!((min, max, step), (Some(0.0), Some(2.0), Some(0.1)));
            }
            other => panic!("expected Number, got {other:?}"),
        }
    }

    #[test]
    fn unknown_feature_key_rejected() {
        let mut meta = base();
        meta.as_object_mut().unwrap().insert("features".into(), json!({"isTriggr": true}));
        let e = serde_json::from_value::<NodeMetadata>(meta)
            .expect_err("unknown feature key must be rejected")
            .to_string();
        assert!(e.contains("isTriggr"), "error names the typo'd key: {e}");
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

    /// The name is the key's identity, so it is validated to [a-z0-9_]+: a
    /// name that would form a surprising or colliding env var (case games,
    /// a hyphen aliasing an underscore, a space, empty) is refused; only
    /// the injective set passes.
    #[test]
    fn provider_names_are_key_identities() {
        for bad in ["OpenRouter", "open-router", "open router", "open.router", ""] {
            assert!(!is_valid_provider_name(bad), "invalid provider name '{bad}' must be refused");
        }
        assert!(is_valid_provider_name("open_router2"));
    }
}

#[cfg(test)]
mod input_semantics_tests {
    use super::*;
    use crate::weft_type::{Exposure, WeftPrimitive, WeftType};
    use serde_json::json;

    fn metadata_with(inputs: Vec<InputSpec>) -> NodeMetadata {
        serde_json::from_str::<NodeMetadata>(
            r#"{ "type": "T", "label": "T", "description": "", "category": "Utility" }"#,
        )
        .map(|mut m| {
            m.inputs = inputs;
            m
        })
        .unwrap()
    }

    fn input(name: &str, ty: WeftType) -> InputSpec {
        InputSpec {
            name: name.into(),
            input_type: ty,
            required: false,
            exposure: None,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            description: None,
        }
    }

    /// Exposure comes from the input alone: its explicit level or the
    /// type default. A file-typed input defaults to assignment; a String
    /// input to `all`; a Bus is wires-only.
    #[test]
    fn exposure_comes_from_the_input_alone() {
        let image = input("image", WeftType::primitive(WeftPrimitive::Image));
        let text = input("prompt", WeftType::primitive(WeftPrimitive::String));
        let bus = input("bus", WeftType::Bus);
        assert_eq!(image.effective_exposure(), Exposure::Assignment, "file types are assignment-only by default");
        assert_eq!(text.effective_exposure(), Exposure::All, "String is fully open by type default");
        assert_eq!(bus.effective_exposure(), Exposure::Wire, "a bus never takes a literal");

        let mut explicit = image;
        explicit.exposure = Some(Exposure::All);
        assert_eq!(explicit.effective_exposure(), Exposure::All, "the explicit level opens a file input");

        let mut closed = text;
        closed.exposure = Some(Exposure::Config);
        assert_eq!(closed.effective_exposure(), Exposure::Config, "the explicit level closes a String input");
    }

    /// The type-derived widget: file-valued types get the drop control,
    /// Boolean a checkbox, Number a number box, everything else a
    /// textarea (JSON typed as in source). Containers of files are NOT
    /// file-valued.
    #[test]
    fn widget_derivation_follows_the_type() {
        let cases = [
            ("Image", "file_drop"),
            ("File", "file_drop"),
            ("Image | Video", "file_drop"),
            ("Boolean", "checkbox"),
            ("Number", "number"),
            ("String", "textarea"),
            ("List[Image]", "textarea"),
            ("List[List[String | Boolean]]", "textarea"),
            ("Dict[String, Number]", "textarea"),
            ("JsonDict", "textarea"),
            ("T", "textarea"),
        ];
        for (ty, expected) in cases {
            let w = Widget::default_for_type(&WeftType::parse(ty).unwrap());
            let tag = serde_json::to_value(&w).unwrap()["kind"].as_str().unwrap().to_string();
            assert_eq!(tag, expected, "type {ty}");
        }
        // The derived file_drop carries the input's own type as its filter.
        match Widget::default_for_type(&WeftType::parse("Media").unwrap()) {
            Widget::FileDrop { file_type, .. } => assert_eq!(file_type, WeftType::media()),
            other => panic!("expected FileDrop, got {other:?}"),
        }
    }

    /// `kind_name` hands back exactly the serde `kind` tag for every
    /// variant: the tag is the one source of truth and this pins the
    /// convenience match to it.
    #[test]
    fn kind_name_matches_the_serde_tag() {
        let all = [
            Widget::Text,
            Widget::Textarea,
            Widget::Code { language: "python".into() },
            Widget::Number { min: None, max: None, step: None },
            Widget::Checkbox,
            Widget::Select { options: vec!["a".into()] },
            Widget::Multiselect { options: vec!["a".into()] },
            Widget::Password,
            Widget::ApiKey { provider: "p".into() },
            Widget::FormBuilder,
            Widget::FileDrop {
                accept: None,
                file_type: WeftType::primitive(WeftPrimitive::Image),
            },
        ];
        for w in all {
            let tag = serde_json::to_value(&w).unwrap()["kind"].as_str().unwrap().to_string();
            assert_eq!(w.kind_name(), tag);
        }
    }

    /// A widget's value shape must fit the input type, or its own
    /// contract (min/max, bool) is silently unenforceable downstream.
    #[test]
    fn validate_semantics_rejects_a_widget_whose_value_cannot_fit_the_type() {
        // Number widget on a String input: the range could never bind.
        let mut n = input("count", WeftType::primitive(WeftPrimitive::String));
        n.widget = Some(Widget::Number { min: Some(0.0), max: Some(5.0), step: None });
        let e = metadata_with(vec![n]).validate_semantics().unwrap_err();
        assert!(e.contains("number widget"), "{e}");

        // Number widget on a Number-containing union: fits.
        let mut ok = input(
            "count",
            WeftType::Union(vec![
                WeftType::primitive(WeftPrimitive::Number),
                WeftType::primitive(WeftPrimitive::Null),
            ]),
        );
        ok.widget = Some(Widget::Number { min: Some(0.0), max: Some(5.0), step: None });
        assert!(metadata_with(vec![ok]).validate_semantics().is_ok());

        // Checkbox on Number: rejected.
        let mut b = input("flag", WeftType::primitive(WeftPrimitive::Number));
        b.widget = Some(Widget::Checkbox);
        assert!(metadata_with(vec![b]).validate_semantics().is_err());

        // A declared widget on an unresolved (TypeVar) input passes:
        // the resolved instance re-derives its surface.
        let mut t = input("value", WeftType::TypeVar("T".into()));
        t.widget = Some(Widget::Number { min: None, max: None, step: None });
        assert!(metadata_with(vec![t]).validate_semantics().is_ok());
    }

    /// A credential input is config-exposure by contract; any other
    /// exposure (wired or assigned keys) is a metadata error.
    #[test]
    fn validate_semantics_rejects_a_wireable_api_key() {
        let mut key = input("apiKey", WeftType::primitive(WeftPrimitive::String));
        key.widget = Some(Widget::ApiKey { provider: "openrouter".into() });
        // String's type-default exposure is `all` (wireable): rejected.
        let meta = metadata_with(vec![key.clone()]);
        let e = meta.validate_semantics().unwrap_err();
        assert!(e.contains("requires `exposure: config`"), "{e}");
        // Declared config exposure passes.
        key.exposure = Some(Exposure::Config);
        assert!(metadata_with(vec![key]).validate_semantics().is_ok());
    }

    /// One name = one input; a duplicate is a metadata error.
    #[test]
    fn validate_semantics_rejects_duplicate_input_names() {
        let meta = metadata_with(vec![
            input("prompt", WeftType::primitive(WeftPrimitive::String)),
            input("prompt", WeftType::primitive(WeftPrimitive::Number)),
        ]);
        let e = meta.validate_semantics().unwrap_err();
        assert!(e.contains("duplicate input 'prompt'"), "{e}");
    }

    /// A select/multiselect widget with no options can never be filled:
    /// metadata-load error, not an editor banner.
    #[test]
    fn validate_semantics_rejects_empty_widget_options() {
        for kind in ["select", "multiselect"] {
            let mut i = input("method", WeftType::primitive(WeftPrimitive::String));
            i.widget = serde_json::from_value(json!({"kind": kind, "options": []})).unwrap();
            let e = metadata_with(vec![i]).validate_semantics().unwrap_err();
            assert!(e.contains("non-empty"), "{kind}: {e}");
        }
    }

    /// A default must fit the input's declared type and (for a number
    /// widget) its min/max range.
    #[test]
    fn validate_semantics_checks_defaults() {
        let mut bad_type = input("n", WeftType::primitive(WeftPrimitive::Number));
        bad_type.default = Some(json!("not-a-number"));
        let e = metadata_with(vec![bad_type]).validate_semantics().unwrap_err();
        assert!(e.contains("default value has type"), "{e}");

        let mut out_of_range = input("n", WeftType::primitive(WeftPrimitive::Number));
        out_of_range.widget =
            serde_json::from_value(json!({"kind": "number", "min": 0.0, "max": 2.0, "step": null})).unwrap();
        out_of_range.default = Some(json!(5.0));
        let e = metadata_with(vec![out_of_range]).validate_semantics().unwrap_err();
        assert!(e.contains("outside"), "{e}");

        let mut fine = input("n", WeftType::primitive(WeftPrimitive::Number));
        fine.default = Some(json!(1.0));
        assert!(metadata_with(vec![fine]).validate_semantics().is_ok());
    }

    /// `resolved()` fills every input's exposure + widget with the
    /// effective values and is idempotent.
    #[test]
    fn resolved_fills_effective_values_idempotently() {
        let meta = metadata_with(vec![input("prompt", WeftType::primitive(WeftPrimitive::String))]);
        let resolved = meta.resolved();
        assert_eq!(resolved.inputs[0].exposure, Some(Exposure::All));
        assert_eq!(resolved.inputs[0].widget, Some(Widget::Textarea));
        let again = resolved.resolved();
        assert_eq!(again.inputs[0].exposure, resolved.inputs[0].exposure);
        assert_eq!(again.inputs[0].widget, resolved.inputs[0].widget);
    }
}

#[cfg(test)]
mod package_defaults_tests {
    use super::*;

    const MEMBER: &str = r#"{ "type": "OpenRouterInference", "label": "OpenRouter",
        "description": "", "category": "AI" }"#;
    const DEFAULTS: &str = r#"{ "formFieldSpecs":
        [{ "fieldType": "root_spec", "label": "Root spec", "render": {} }] }"#;

    /// The one guarantee finding-3 turns on: a package member's metadata is
    /// ONE document. The derive's runtime `parse_embedded(member, defaults)`
    /// must produce exactly what the catalog builds by merging the same two
    /// files, so `manifest()` never disagrees with the compile-side catalog.
    #[test]
    fn derive_and_catalog_merge_produce_the_same_metadata() {
        // Runtime side (the derive expansion calls this):
        let from_derive = NodeMetadata::parse_embedded(MEMBER, Some(DEFAULTS), "test");
        // Catalog side (what `load_node_entry` does): merge then typed parse.
        let mut value: serde_json::Value = serde_json::from_str(MEMBER).unwrap();
        let defaults: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(DEFAULTS).unwrap();
        merge_package_defaults(&mut value, &defaults).unwrap();
        let from_catalog: NodeMetadata = serde_json::from_value(value).unwrap();

        let spec_types = |m: &NodeMetadata| {
            m.form_field_specs.iter().map(|s| s.field_type.clone()).collect::<Vec<_>>()
        };
        assert_eq!(spec_types(&from_derive), spec_types(&from_catalog));
        assert_eq!(
            spec_types(&from_derive),
            vec!["root_spec".to_string()],
            "the package-root defaults reach the merged metadata"
        );
        // A bare node (no defaults) parses its own file unchanged.
        let bare = NodeMetadata::parse_embedded(MEMBER, None, "test");
        assert!(bare.form_field_specs.is_empty());
    }

    /// The member's own key wins wholesale, disjoint keys survive, and an
    /// identity key at the package level is refused (the same rule the
    /// catalog and the derive share).
    #[test]
    fn merge_semantics() {
        let mut member = serde_json::json!({ "a": "mine", "keep": 1 });
        let defaults: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({ "a": "theirs", "add": 2 })).unwrap();
        merge_package_defaults(&mut member, &defaults).unwrap();
        assert_eq!(member["a"], "mine", "member wins");
        assert_eq!(member["keep"], 1);
        assert_eq!(member["add"], 2, "disjoint default survives");

        for key in NON_INHERITABLE_METADATA_KEYS {
            let mut member = serde_json::json!({ "type": "T" });
            let defaults: serde_json::Map<String, serde_json::Value> =
                serde_json::from_value(serde_json::json!({ key: "x" })).unwrap();
            let err = merge_package_defaults(&mut member, &defaults).unwrap_err();
            assert!(err.contains(key), "refusal names `{key}`: {err}");
        }
    }
}


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
        /// `metered_access`, `log`, `endpoint`). The ONLY way to fire
        /// downstream is `ctx.pulse_downstream(output)`. For a plain
        /// (non-trigger) node the engine calls this in every lifecycle
        /// phase (a value feeding a trigger's config must be produced at
        /// setup time too); for a trigger it runs only on a real firing,
        /// with the fire's payload fields on the `ctx.wake` bag.
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()>;

        /// The node's self-tests (see `crate::node_test`), declared in
        /// the node folder's `tests.rs` and bridged here:
        /// `fn tests(&self) -> Vec<NodeTest> { tests::tests() }`.
        /// Default: none. Discovery walks the registry and asks each
        /// node, so a test binary or UI lists tests without any
        /// metadata mirror.
        fn tests(&self) -> Vec<crate::node_test::NodeTest> {
            Vec::new()
        }
    }
}

#[cfg(feature = "runtime")]
pub use node_trait::Node;

/// Validation diagnostic. Emitted by the generic validate pass and
/// per-node validators. Mirrored by the VS Code extension's
// SYNC: Diagnostic/Severity <-> packages/weft-graph/src/protocol.ts Diagnostic, Severity
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
    /// Backend-only (the CLI's image build); deliberately not mirrored
    /// in the editor's `CatalogEntry`, and absent from the wire when
    /// empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<String>,

    /// The service this node hands out a connection to, for a node
    /// that opens something it runs ITSELF (a database it provisions).
    /// Names the service whose recipe the published connection is
    /// held to; the compiler resolves that recipe from the catalog at
    /// enrich time, so a name nothing declares is a build error rather
    /// than a surprise at run time.
    ///
    /// A node declaring this must also declare an `Access` OUTPUT: the
    /// connection it publishes is what that output carries.
    ///
    /// Backend-only (the compiler's enrich-time recipe resolution),
    /// deliberately not mirrored in the editor's `CatalogEntry`: the
    /// connect control keys off an access widget, and a publishing
    /// node has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publishes: Option<String>,
    /// Node-level semantic constraints. Small, extensible, boolean-ish
    /// flags; anything with structure gets its own top-level key (like
    /// `display` below).
    #[serde(default)]
    pub features: NodeFeatures,
    /// The inline per-firing display this node declares (a media
    /// player, a file card) and which PORT it shows, named with its
    /// side. See [`DisplaySpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<DisplaySpec>,
    /// Declarative validation rules. Evaluated against the project's
    /// graph state by the compiler's validate pass. Closed grammar
    /// (see `ValidationRule` / `Condition`); no user Rust runs.
    /// Rules carry a `level` that distinguishes `structural` (checked
    /// on parse / AI edits) from `runtime` (checked only at run time
    /// so a missing credential is fine in the editor).
    /// Backend-only (the compiler's validate pass); deliberately not
    /// mirrored in the editor's `CatalogEntry`, and absent from the
    /// wire when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validate: Vec<ValidationRule>,
    /// This node's ports come from a LIST in its own config: which
    /// config key holds the list, which entry kinds it may use, and
    /// what ports each kind materializes. `None` for everything else.
    /// A form is one thing built with it (`HumanQuery` over `fields`),
    /// a switch is another (`Switch` over `cases`). Multi-node packages
    /// declare it once in the package root's partial `metadata.json`
    /// (HumanQuery and HumanTrigger share one vocabulary); every member
    /// inherits it via the catalog's package-defaults merge unless its
    /// own file carries the key.
    #[serde(default, rename = "portsFromConfig", skip_serializing_if = "Option::is_none")]
    pub ports_from_config: Option<PortsFromConfig>,
    /// The service recipe, present ONLY on an ACCESS NODE (the node
    /// owning the connect for a service). Declares how a connection is
    /// acquired, how requests are authenticated, the permission
    /// catalogue, and the doors; see
    /// [`crate::access::spec::AccessSpec`]. The editor ships it to the
    /// store at connect time; the compiler stamps the service name
    /// onto the node's `access` widget.
    // SYNC: NodeMetadata.service <-> packages/weft-graph/src/protocol.ts CatalogEntry.service
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<crate::access::spec::AccessSpec>,
    /// PUBLIC (PKCE, secretless) OAuth apps this project ships, keyed
    /// by service name. A package root declares its apps here once and
    /// every member node inherits them (via [`merge_package_defaults`]);
    /// at connect the "Your own" door uses the project's app when the
    /// user pastes none. An entry carrying a `client_secret` fails the
    /// metadata load: project metadata is source, and source never
    /// holds secrets (a confidential app goes editor -> store on the
    /// "Your own" page instead). Empty in the shipped catalog.
    // SYNC: NodeMetadata.access_apps <-> packages/weft-graph/src/protocol.ts CatalogEntry.accessApps
    #[serde(
        default,
        rename = "accessApps",
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub access_apps: std::collections::BTreeMap<String, crate::access::spec::AppRegistration>,
    /// User-declared TYPE names this metadata contributes: name to type
    /// string (`"ChatMessage": "{ role: String, ... }"`). Collected
    /// across every `metadata.json` into the project's one
    /// [`crate::weft_type::TypeRegistry`] before any metadata is parsed;
    /// a package root declares shared types once and members inherit the
    /// key (harmless: identical redeclarations absorb). Declared types
    /// are nominal and global: any node's ports may use them by name.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub types: std::collections::BTreeMap<String, String>,
}

/// The one charset a provider name may use: lowercase ASCII letters,
/// digits, and `_`. The name is the identity everything keys on (the
/// shared-credentials file's entries, the meter registry, the
/// connection rows), so it must be exact-comparable with no aliasing
/// or surprising characters (spaces, dots, `-` vs `_`, unicode,
/// empty). Enforced where a provider name enters the system (the
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
    /// Whether this node consumes a stream: any input whose declared
    /// type is a `Generator` (aliases peel). THE one spelling of the
    /// question for manifest-driven callers (both node-test rigs), so
    /// the definition cannot drift between them.
    pub fn has_generator_input(&self) -> bool {
        self.inputs.iter().any(|p| p.input_type.as_generator().is_some())
    }

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
    /// would later reject fails the metadata load instead. A node that
    /// `publishes` a service names it the way a service is named, and
    /// declares the `Access` output it hands the connection out on.
    pub fn validate_semantics(&self) -> Result<(), String> {
        if let Some(display) = &self.display {
            let port = match (&display.input, &display.output) {
                (Some(name), None) => {
                    if !self.inputs.iter().any(|i| i.name == *name) {
                        return Err(format!(
                            "display names input '{name}', which this node does \
                             not declare"
                        ));
                    }
                    name
                }
                (None, Some(name)) => {
                    if !self.outputs.iter().any(|o| o.name == *name) {
                        return Err(format!(
                            "display names output '{name}', which this node does \
                             not declare"
                        ));
                    }
                    name
                }
                _ => {
                    return Err(
                        "display must name exactly one port: either `input` or \
                         `output`"
                            .into(),
                    )
                }
            };
            let _ = port;
        }
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for input in &self.inputs {
            if !seen.insert(input.name.as_str()) {
                return Err(format!(
                    "duplicate input '{}': every input name must be unique",
                    input.name
                ));
            }
            // Every node gets `_should_flow` from the language (the port
            // that decides whether it runs). A node type declaring its
            // own would shadow that decision with node data.
            if input.name == crate::exec::skip::SHOULD_FLOW_PORT {
                return Err(format!(
                    "input '{}' is the language's own port (it decides whether a node runs); a node type cannot declare it",
                    input.name
                ));
            }
            match input.effective_widget() {
                // The entry-list editor draws the kinds a node's
                // `portsFromConfig` declares, so on any other input it
                // would render an empty dropdown nobody can use.
                Widget::EntryList
                    if self.ports_from_config.as_ref().map(|p| p.field.as_str())
                        != Some(input.name.as_str()) =>
                {
                    return Err(format!(
                        "input '{}': an entry_list widget edits the list this node's ports \
                         come from, so `portsFromConfig.field` must name it",
                        input.name
                    ));
                }
                Widget::Select { options } | Widget::Multiselect { options }
                    if options.is_empty() =>
                {
                    return Err(format!(
                        "input '{}': a select/multiselect widget must declare a non-empty \
                         `options` list",
                        input.name
                    ));
                }
                // A connected-account handle is design-time
                // configuration by nature: it must never arrive over a
                // wire or an assignment statement. Enforcing the
                // exposure here lets every consumer treat "access
                // input" and "config input" as the same fact instead
                // of re-checking it.
                Widget::Access { .. } if input.effective_exposure() != Exposure::Config => {
                    return Err(format!(
                        "input '{}': an access widget requires `exposure: config`",
                        input.name
                    ));
                }
                Widget::Access { .. } if self.service.is_none() => {
                    return Err(format!(
                        "input '{}': an access widget needs the node metadata to declare a \
                         `service` recipe (the sign-in it connects)",
                        input.name
                    ));
                }
                Widget::RemoteSelect { access, sources, depends_on, free_text: _ } => {
                    let names_access_input = self.inputs.iter().any(|i| {
                        i.name == *access && i.input_type == WeftType::Access
                    });
                    if !names_access_input {
                        return Err(format!(
                            "input '{}': remote_select's `access` must name an Access-typed \
                             input on this node ('{}' is not one)",
                            input.name, access
                        ));
                    }
                    if sources.is_empty() {
                        return Err(format!(
                            "input '{}': remote_select needs at least one source",
                            input.name
                        ));
                    }
                    for source in sources {
                        match source {
                            ResourceSource::FromUrl { pattern } => {
                                if let Err(e) = regex::Regex::new(pattern.as_str()) {
                                    return Err(format!(
                                        "input '{}': from_url pattern does not parse: {e}",
                                        input.name
                                    ));
                                }
                            }
                            ResourceSource::Picker { script, code, .. } => {
                                if !script.starts_with("https://") {
                                    return Err(format!(
                                        "input '{}': a picker's script must be an https:// \
                                         address, got '{script}'",
                                        input.name
                                    ));
                                }
                                if code.trim().is_empty() {
                                    return Err(format!(
                                        "input '{}': a picker needs its glue `code` (the \
                                         statements that open the chooser and call \
                                         weft.done)",
                                        input.name
                                    ));
                                }
                            }
                            ResourceSource::List { requires, lookup } => {
                                // "public" says the call carries no credential
                                // at all; "requires" says it needs these
                                // permissions on the connection. Declaring
                                // both is self-contradictory and would leave
                                // the editor's source-selection ambiguous.
                                if lookup.public && !requires.is_empty() {
                                    return Err(format!(
                                        "input '{}': a `public` list source cannot also \
                                         declare `requires` (a credential-free call has \
                                         no permissions to hold)",
                                        input.name
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                    for parent in depends_on {
                        if !self.inputs.iter().any(|i| i.name == *parent) {
                            return Err(format!(
                                "input '{}': remote_select depends_on '{}', which is not an \
                                 input of this node",
                                input.name, parent
                            ));
                        }
                    }
                }
                _ => {}
            }
            if input.requires_scopes.is_some() && input.input_type != WeftType::Access {
                return Err(format!(
                    "input '{}': `requiresScopes` is only legal on an Access-typed input",
                    input.name
                ));
            }
            if input.requires_values.is_some() && input.input_type != WeftType::Access {
                return Err(format!(
                    "input '{}': `requiresValues` is only legal on an Access-typed input",
                    input.name
                ));
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
                | Widget::Select { .. } => Some(WeftType::primitive(WeftPrimitive::String)),
                Widget::Multiselect { .. } => {
                    Some(WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String))))
                }
                // remote_select's runtime value is the picked id (the
                // cached label is stripped when the bag is built).
                Widget::RemoteSelect { .. } => Some(WeftType::primitive(WeftPrimitive::String)),
                // The access widget's STORED value is the small
                // {id, identity} handle object (shape-checked by the
                // connect flow), but the value the node READS is the
                // Access marker the input bag builds from it, so the
                // input's declared type is Access. Typing it here is
                // what lets the requiresScopes/requiresValues legality
                // rules and remote_select's `access` reference bind on
                // the access node's own input.
                Widget::Access { .. } => Some(WeftType::Access),
                // textarea is the GENERIC value editor (the derived
                // default for structural types: the value is edited as
                // JSON text), so it constrains nothing. entry_list edits
                // the config-entry list, file_drop a file marker, and
                // text_list a list of short strings; all three are
                // shape-checked elsewhere.
                Widget::Textarea
                | Widget::EntryList
                | Widget::TextList
                | Widget::FileDrop { .. } => None,
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
                // The one runtime gate: declared shapes (Named/Record)
                // validate structurally; inference alone can never
                // produce a nominal name, so an infer-and-compare here
                // would refuse every custom-typed default.
                if !input.input_type.accepts_runtime_value(default) {
                    return Err(format!(
                        "input '{}': default value has type {} but the input declares {}",
                        input.name,
                        WeftType::infer(default),
                        input.input_type
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
        if let Some(ports) = &self.ports_from_config {
            // The list a node derives ports from lives under a key it
            // DECLARES, so that key is an ordinary config input like
            // any other: the editor edits it, and validate's
            // undeclared-config-key check needs no exemption for it.
            if !self.inputs.iter().any(|i| i.name == ports.field) {
                return Err(format!(
                    "portsFromConfig reads config key '{}', which this node does not \
                     declare as an input",
                    ports.field
                ));
            }
            // `matchInput` is the input the compiler checks entry
            // values against; naming a port that does not exist would
            // resolve to nothing and silently disable every value
            // check downstream, so refuse it here.
            if let Some(name) = &ports.match_input {
                if !self.inputs.iter().any(|i| i.name == *name) {
                    return Err(format!(
                        "portsFromConfig matches entries against input '{name}', which \
                         this node does not declare"
                    ));
                }
            }
            let mut kinds: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for spec in &ports.specs {
                if spec.kind.is_empty() {
                    return Err("portsFromConfig: every spec needs a `kind`".into());
                }
                if !kinds.insert(spec.kind.as_str()) {
                    return Err(format!(
                        "portsFromConfig: two specs claim kind '{}'; an entry would match \
                         both",
                        spec.kind
                    ));
                }
                if spec.key_field.is_empty() {
                    return Err(format!(
                        "portsFromConfig: spec '{}' has an empty `keyField` (the entry key \
                         holding its port name)",
                        spec.kind
                    ));
                }
                for field in &spec.fields {
                    if let Some(problem) = field.declaration_problem() {
                        return Err(format!("portsFromConfig: spec '{}' {problem}", spec.kind));
                    }
                }
                if spec.fields.iter().any(|f| f.needs_matched_input())
                    && ports.match_input.is_none()
                {
                    return Err(format!(
                        "portsFromConfig: spec '{}' asks for a value measured against what \
                         the entries match on, so `matchInput` must name the input they are \
                         matched against",
                        spec.kind
                    ));
                }
                if spec.adds_inputs.is_empty() && spec.adds_outputs.is_empty() {
                    return Err(format!(
                        "portsFromConfig: spec '{}' adds no ports, so an entry of that kind \
                         would do nothing",
                        spec.kind
                    ));
                }
            }
        }
        if let Some(service) = &self.publishes {
            // The SAME rule an `AccessSpec.service` is held to (that is
            // what this name has to match), not a lookalike.
            if !is_valid_provider_name(service) {
                return Err(format!(
                    "`publishes` names a service, so it takes a service name \
                     (lowercase letters, digits and underscores): '{service}' is not one"
                ));
            }
            if !self
                .outputs
                .iter()
                .any(|o| matches!(o.port_type, crate::weft_type::WeftType::Access))
            {
                return Err(format!(
                    "this node publishes a '{service}' connection but declares no Access \
                     output to hand it out on"
                ));
            }
        }
        if let Some(spec) = &self.service {
            spec.validate()?;
            let connect_inputs = self
                .inputs
                .iter()
                .filter(|i| matches!(i.effective_widget(), Widget::Access { .. }))
                .count();
            if connect_inputs != 1 {
                return Err(format!(
                    "a node declaring a `service` recipe needs exactly ONE input with the \
                     `access` widget (the connect control); found {connect_inputs}"
                ));
            }
        }
        // A project-declared app is source that ships with the project,
        // so it may never carry a secret: only a public (PKCE) client
        // is shareable this way. A confidential app belongs on the
        // "Your own" page, where its secret goes editor -> store.
        for (service, app) in &self.access_apps {
            if app.client_secret.is_some() {
                return Err(format!(
                    "accessApps.{service}: a project-declared app must not carry a \
                     client_secret (project metadata is source, and source never holds \
                     secrets); declare only a public (PKCE) client here, or connect \
                     through the editor's \"Your own\" page instead"
                ));
            }
        }
        Ok(())
    }

    /// A copy with every input's `exposure` and `widget`, and every
    /// config entry field's `widget`, RESOLVED to their effective values (the author's explicit choice, else the
    /// type-derived default). This is what ships on the wire to the
    /// editor, so the editor never re-derives either; resolving is
    /// idempotent (a resolved metadata re-parses to itself).
    pub fn resolved(&self) -> Self {
        let mut out = self.clone();
        for input in &mut out.inputs {
            input.exposure = Some(input.exposure.unwrap_or_else(|| input.input_type.default_exposure()));
            input.widget = Some(input.effective_widget());
        }
        // A config entry kind's own fields resolve the same way, so the
        // editor draws a control for every one of them without knowing
        // what the key means.
        if let Some(ports) = &mut out.ports_from_config {
            for spec in &mut ports.specs {
                for field in &mut spec.fields {
                    field.widget = Some(field.resolved_widget());
                }
            }
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
#[derive(Default)]
pub enum ValidationLevel {
    /// Checked at parse / edit time. Missing these is a code error.
    #[default]
    Structural,
    /// Not checked when a project is BUILT (e.g. missing
    /// credentials), so an AI builder or a human can compile a
    /// project they are still sketching, secrets unfilled. The
    /// editor's Problems panel does report them: it validates in
    /// `ValidationMode::Runtime` (see `weft-cli`'s `do_validate`),
    /// the only mode that runs them.
    Runtime,
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
#[derive(Default)]
pub enum RuleSeverity {
    #[default]
    Error,
    Warning,
    Info,
    Hint,
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
/// The (input, output) port pair of a node declaring the checked-cast
/// semantic (see [`NodeFeatures::cast_ports`]).
// Backend-only: the editor's NodeFeaturesWire deliberately carries no
// castPorts mirror (the wire field rides unread on the editor side).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CastPorts {
    pub input: String,
    pub output: String,
}

/// The inline per-firing display a node declares
/// (the top-level `display` metadata key): which renderer, and which PORT it shows,
/// named with its side (`"input": "media"` or `"output": "image"`,
/// exactly one). New kinds extend the enum; there is never a flag per
/// renderer.
// SYNC: DisplaySpec <-> packages/weft-graph/src/protocol.ts DisplaySpecWire
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisplaySpec {
    pub kind: DisplayKind,
    /// Show this INPUT port's value (a display sink).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    /// Show this OUTPUT port's value (a generator's result).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// How a displayed port renders.
// SYNC: DisplayKind <-> packages/weft-graph/src/protocol.ts DisplaySpecWire.kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisplayKind {
    /// Render the file by its own mime: an image inline, audio/video
    /// with a real player; anything unplayable falls back to the file
    /// card. A save button rides below either way.
    Media,
    /// The file card with metadata and a download button, never a
    /// player.
    Link,
}

// SYNC: NodeFeatures <-> packages/weft-graph/src/protocol.ts NodeFeaturesWire
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFeatures {
    /// Each inner list is a port group where at least ONE port must
    /// be non-null. If every port in a group is null/missing, the
    /// node is skipped. Example: email send might declare
    /// `one_of_required: [["message", "attachment"]]`.
    #[serde(default, rename = "oneOfRequired", skip_serializing_if = "Vec::is_empty")]
    pub one_of_required: Vec<Vec<String>>,
    /// The node accepts ad-hoc extra input ports declared in weft
    /// source. If unset, extra ports cause a compile error.
    #[serde(default, rename = "canAddInputPorts", skip_serializing_if = "std::ops::Not::not")]
    pub can_add_input_ports: bool,
    /// Same for outputs.
    #[serde(default, rename = "canAddOutputPorts", skip_serializing_if = "std::ops::Not::not")]
    pub can_add_output_ports: bool,
    /// The node's created input ports are OPTIONAL by default (a
    /// closure on one does not skip the node). For node types whose
    /// extra inputs are optional by nature, `FirstInOrder` being the
    /// one that needs it. Without it a created port is required, like
    /// a declared one. A `?` on the config key overrides either way.
    #[serde(default, rename = "optionalCustomInputs", skip_serializing_if = "std::ops::Not::not")]
    pub optional_custom_inputs: bool,
    /// The type every CREATED input port takes (a port a config key
    /// makes on a node that accepts custom inputs). `None` means each
    /// created port gets its own type variable, so it takes whatever
    /// its wire carries, independently of its siblings. Naming a shared
    /// variable (`T`) instead makes every created input and the output
    /// that uses `T` one type, which is what a join wants: two branches
    /// of different types then fail to unify instead of silently
    /// meeting at an unresolved port.
    #[serde(default, rename = "customInputType", skip_serializing_if = "Option::is_none")]
    pub custom_input_type: Option<WeftType>,
    /// Marks the node as a trigger (fires executions from external
    /// events rather than running as part of an execution).
    #[serde(default, rename = "isTrigger", skip_serializing_if = "std::ops::Not::not")]
    pub is_trigger: bool,
    /// Webview hint: render the node's latest output as a JSON
    /// preview inline on the node body. Used by Debug.
    #[serde(default, rename = "showDebugPreview", skip_serializing_if = "std::ops::Not::not")]
    pub show_debug_preview: bool,
    /// Default value of the node's `is_output` config flag. Nodes that
    /// are semantically "produce this thing" (Debug, Output) default
    /// to true. Any project can override by setting `is_output` in the
    /// node's weft config. Read at run-dispatch time to compute the
    /// subgraph to execute (see docs/v2-design.md section 3.0).
    #[serde(default, rename = "isOutputDefault", skip_serializing_if = "std::ops::Not::not")]
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
    /// Declares this node as a CHECKED CAST between two of its ports:
    /// at run time the node converts the named input's value into the
    /// named output's RESOLVED declared type, so at compile time the
    /// resolved (input, output) type pair must be in the one conversion
    /// table (`WeftType::cast_allowed`); a nonsense pair is a compile
    /// error on the node. The compiler reads only this declaration,
    /// never a node-type name: any node may declare the semantic.
    #[serde(default, rename = "castPorts", skip_serializing_if = "Option::is_none")]
    pub cast_ports: Option<CastPorts>,
    /// Hidden from the node picker and describe-nodes output. For a
    /// catalog node type that executes but must not appear in
    /// user-facing tooling. Users cannot declare a hidden node type
    /// in source; the parser rejects them with a dedicated error.
    /// (Group/Loop boundary lowering does NOT use this: those are
    /// not catalog nodes, they're inline-dispatched in the engine.)
    #[serde(default, rename = "hidden", skip_serializing_if = "std::ops::Not::not")]
    pub hidden: bool,
}

/// Where a node's ports come from when they come from its own config:
/// the config key holding the list, and the entry kinds that list may
/// use. One object rather than a flag plus a list, so "this node
/// derives ports" and "here is how" cannot drift apart.
// SYNC: PortsFromConfig <-> packages/weft-graph/src/protocol.ts PortsFromConfigWire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortsFromConfig {
    /// The config key holding the entry list (`"fields"` for a form,
    /// `"cases"` for a switch). The value must be a JSON array; a
    /// missing key means no derived ports.
    pub field: String,
    /// The node input the entries are matched AGAINST, when they compete
    /// and only one wins (a switch's `value`). It is what lets the
    /// compiler hold a test to a value that input could actually hold:
    /// `equals: 5` against a String input is a compile error, not a
    /// branch that silently never fires. Absent for a list whose entries
    /// all contribute (a form's fields).
    #[serde(default, rename = "matchInput", skip_serializing_if = "Option::is_none")]
    pub match_input: Option<String>,
    /// The entry kinds this node accepts, and the ports each one adds.
    pub specs: Vec<PortSpec>,
}

impl PortsFromConfig {
    /// The spec matching an entry's `kind`, or None when the entry
    /// names a kind this node does not accept.
    pub fn spec_for(&self, kind: &str) -> Option<&PortSpec> {
        self.specs.iter().find(|s| s.kind == kind)
    }
}

/// A field type's render metadata: which UI primitive draws it and the
/// primitive's flags. Typed (not a raw `Value`) so the loud-load
/// guarantee holds here too: a typo'd key inside `render` fails the
/// metadata load instead of shipping a component-less field.
// SYNC: FormFieldRender <-> packages/weft-graph/src/protocol.ts FormFieldRenderWire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormFieldRender {
    pub component: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<FormFieldSource>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multiple: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub prefilled: bool,
}

/// Where a select-style field's options come from: baked into the
/// form schema, or fed by a node input at run time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormFieldSource {
    Static,
    Input,
}

/// How one kind of config entry contributes to a node's ports at
/// compile time. The enrich pass reads the list named by
/// [`PortsFromConfig::field`], matches each entry's `kind` here, and
/// materializes the inputs/outputs onto the NodeDefinition.
// SYNC: PortSpec <-> packages/weft-graph/src/protocol.ts PortSpecWire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortSpec {
    /// Value of the entry's `kind` this spec matches (e.g.
    /// "text_input", "approve_reject", "case"). Wire shape is
    /// camelCase across every field on this struct so VS Code + browser
    /// extension can read the same TS interface without per-field
    /// bridging.
    pub kind: String,
    /// The entry key holding the port NAME this spec's templates
    /// substitute for `{key}`. `"key"` for a form field, `"port"` for a
    /// switch case, so an entry reads in its own vocabulary instead of
    /// borrowing another's.
    #[serde(default = "default_key_field", rename = "keyField")]
    pub key_field: String,
    /// Human-readable label for the entry-list editor's dropdown
    /// (e.g. "Text input", "Approve / Reject", "is exactly this value").
    #[serde(default)]
    pub label: String,
    /// Default render metadata applied to the entry if not overridden
    /// in the weft source. The browser extension reads
    /// `render.component` (and its sibling flags) to pick a UI
    /// primitive without knowing kind strings. `None` for a kind
    /// nothing renders, a switch case being the one that has no form
    /// behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub render: Option<FormFieldRender>,
    /// What this kind asks the author to fill in: a form field's
    /// `label`, a select's `options`, a case's `value`. Each carries the
    /// shape its value must have and the control the editor draws for
    /// it, so a node invents a kind without anything downstream
    /// learning that kind's name.
    ///
    /// An entry is a whole declaration, or just a NAME: `"placeholder"`
    /// is the one-line String field of that name, which is what most
    /// of them are. See [`spec_fields`].
    #[serde(default, deserialize_with = "spec_fields", skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<SpecField>,
    /// This kind takes anything, so it is the branch reached when no
    /// earlier entry matched. At most one entry in a list may be one,
    /// and it goes last.
    #[serde(default, rename = "catchAll", skip_serializing_if = "std::ops::Not::not")]
    pub catch_all: bool,
    #[serde(default, rename = "addsInputs")]
    pub adds_inputs: Vec<PortTemplate>,
    #[serde(default, rename = "addsOutputs")]
    pub adds_outputs: Vec<PortTemplate>,
}

/// A spec's field list, where an entry may be a whole declaration or
/// just a name. `"placeholder"` means exactly this:
///
/// ```json
/// { "key": "placeholder", "label": "Placeholder", "shape": "typed", "valueType": "String" }
/// ```
///
/// which is what a label, a placeholder, and most one-line settings
/// are, so writing six lines for each of them is how eleven kinds ended
/// up repeating themselves.
fn spec_fields<'de, D>(deserializer: D) -> Result<Vec<SpecField>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let raw = Vec::<serde_json::Value>::deserialize(deserializer)?;
    raw.into_iter()
        .map(|entry| match entry {
            serde_json::Value::String(key) => Ok(SpecField::named(&key)),
            // Deserialized here rather than through an untagged enum so
            // a mistyped key still names itself in the error.
            object => serde_json::from_value(object).map_err(D::Error::custom),
        })
        .collect()
}

/// The label a shorthand field shows: its key, read as words.
/// `minLength` and `min_length` both become "Min Length".
fn label_from_key(key: &str) -> String {
    let mut words: Vec<String> = Vec::new();
    let mut word = String::new();
    let mut chars = key.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '_' || ch == '-' || ch == ' ' {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            continue;
        }
        // A capital opens a new word, unless it opens this one (`ID`
        // stays whole) or continues an acronym. An acronym ends where
        // a lowercase follows: the last capital of `HTTPMethod` opens
        // `Method`, leaving `HTTP` whole. The one exception is a
        // single lowercase letter that ENDS the word: that is a plural
        // tail (`IDs`, `URLs`), not a new word. A capitalized non-
        // acronym word after a single-letter prefix (`OAuthToken`)
        // still splits wrong ("O Auth Token"); no shorthand key uses
        // that shape today.
        if ch.is_uppercase() && !word.is_empty() {
            let after_acronym = word.ends_with(char::is_uppercase)
                && chars.peek().is_some_and(|next| next.is_lowercase());
            let plural_tail = after_acronym && {
                let mut rest = chars.clone();
                rest.next(); // the lowercase the peek saw
                rest.peek().is_none_or(|c| !c.is_alphanumeric())
            };
            if (!word.ends_with(char::is_uppercase) || after_acronym) && !plural_tail {
                words.push(std::mem::take(&mut word));
            }
        }
        word.push(ch);
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
        .iter()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// One value a kind asks the author to supply, and what that value has
/// to be. The compiler holds the value to its shape and the editor draws
/// `widget` for it, so neither needs to know what any node means by the
/// key.
// SYNC: SpecField <-> packages/weft-graph/src/protocol.ts SpecFieldWire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecField {
    /// The entry key this value lives under (`options`, `value`, `min`).
    pub key: String,
    /// One line for the editor's label and the docs.
    #[serde(default)]
    pub label: String,
    /// Whether an entry of this kind is incomplete without it.
    #[serde(default)]
    pub required: bool,
    pub shape: SpecShape,
    /// The declared type, for `shape: "typed"` and only for it. The
    /// pairing is checked at metadata load.
    #[serde(default, rename = "valueType", skip_serializing_if = "Option::is_none")]
    pub value_type: Option<WeftType>,
    /// The control the editor draws. Defaults from the shape at
    /// metadata load, so a spec only says this to override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub widget: Option<Widget>,
}

impl SpecField {
    /// The field a shorthand entry means: a one-line String box, named
    /// and labelled after the key. The widget is named rather than left
    /// to the shape, whose default for a String is a text AREA.
    fn named(key: &str) -> Self {
        SpecField {
            key: key.to_string(),
            label: label_from_key(key),
            required: false,
            shape: SpecShape::Typed,
            value_type: Some(WeftType::primitive(crate::weft_type::WeftPrimitive::String)),
            widget: Some(Widget::Text),
        }
    }

    /// Whether checking this value needs to know what the entries are
    /// matched against.
    pub fn needs_matched_input(&self) -> bool {
        self.shape != SpecShape::Typed
    }

    /// The control to draw, the declared one or the shape's default.
    pub fn resolved_widget(&self) -> Widget {
        self.widget.clone().unwrap_or_else(|| match self.shape {
            SpecShape::Typed => match &self.value_type {
                Some(t) => Widget::default_for_type(t),
                None => Widget::Text,
            },
            SpecShape::Number => Widget::Number { min: None, max: None, step: None },
            SpecShape::ValueList => Widget::TextList,
            SpecShape::Value | SpecShape::Element | SpecShape::Regex => Widget::Text,
        })
    }

    /// What is wrong with this field's declaration, or `None`. The
    /// `typed` shape is the only one carrying a type, so a type beside
    /// any other shape (or a missing one beside `typed`) is a spec
    /// nothing could check.
    fn declaration_problem(&self) -> Option<String> {
        match (self.shape, &self.value_type) {
            (SpecShape::Typed, None) => {
                Some(format!("field '{}' is `typed`, so it must name a `valueType`", self.key))
            }
            (shape, Some(_)) if shape != SpecShape::Typed => Some(format!(
                "field '{}' carries a `valueType`, which only a `typed` field takes",
                self.key
            )),
            _ => None,
        }
    }
}

/// What a [`SpecField`]'s value has to be: a value of a declared type,
/// or a shape measured against the input the entries are matched on,
/// which is what lets the compiler refuse `equals: 5` on a String
/// without knowing what `equals` means.
// SYNC: SpecShape <-> packages/weft-graph/src/protocol.ts SpecFieldWire.shape
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SpecShape {
    /// A value of the declared `valueType` (`options: List[String]`).
    Typed,
    /// One value the matched input could hold (`equals: "high"`).
    Value,
    /// A list of them (`in: ["high", "urgent"]`).
    ValueList,
    /// A number, whatever the matched input holds (`gte: 3`).
    Number,
    /// ONE ELEMENT of what the matched input holds: an item when it
    /// holds a list, a piece of text when it holds text
    /// (`contains: "err"`). An input that holds neither has nothing to
    /// look inside, which the compiler says so.
    Element,
    /// A regular expression, checked at compile time so a broken one
    /// never reaches a run.
    Regex,
}

/// The default [`PortSpec::key_field`]: a form field names its port
/// with `key`, and that is the shape every existing spec uses.
fn default_key_field() -> String {
    "key".to_string()
}

// SYNC: PortTemplate <-> packages/weft-graph/src/protocol.ts PortTemplateWire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortTemplate {
    #[serde(rename = "nameTemplate")]
    pub name_template: String,
    #[serde(rename = "portType")]
    pub port_type: WeftType,
}

impl PortTemplate {
    pub fn new(name_template: impl Into<String>, type_str: &str) -> Self {
        Self {
            name_template: name_template.into(),
            port_type: WeftType::parse(type_str)
                .unwrap_or_else(|| panic!("invalid port type: {type_str}")),
        }
    }

    /// Port template accepting any type, independent from sibling
    /// ports. See [`AUTO_TYPE_VAR`].
    pub fn any(name_template: impl Into<String>) -> Self {
        Self { name_template: name_template.into(), port_type: WeftType::type_var(AUTO_TYPE_VAR) }
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
    /// The type registry this catalog's metadata was loaded under
    /// (builtin aliases plus the project's `types` declarations). The
    /// compile pipeline activates it so type names in weft source
    /// resolve against the same table. Default: builtin only, the
    /// honest answer for hand-rolled test catalogs.
    fn type_registry(&self) -> std::sync::Arc<crate::weft_type::TypeRegistry> {
        std::sync::Arc::new(crate::weft_type::TypeRegistry::builtin())
    }
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
//                 OutputSpec, PortSpec, SignalSpec, InfraSpec.
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
    /// richer control (a select over a String, a code box).
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
    /// The permissions THIS consumer needs on the wired connection
    /// (only legal on an `Access`-typed input). Checked where a
    /// connection's real granted set is knowable: live in the editor
    /// when one is picked, at connect time, and at run-time resolution
    /// (the drift backstop). A VERIFIED shortfall is a hard error; a
    /// claimed/unknown one passes, because nobody actually knows (a
    /// pasted key on a service that reports nothing must not be
    /// refused). Never checked by the compiler: source holds only a
    /// connection id, so source alone cannot answer.
    // SYNC: InputSpec.requires_scopes <-> packages/weft-graph/src/protocol.ts InputSpec.requiresScopes
    #[serde(
        default,
        rename = "requiresScopes",
        alias = "requires_scopes",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_scopes: Option<Vec<String>>,
    /// The stored VALUES this consumer needs on the wired connection
    /// (only legal on an `Access`-typed input). The sibling of
    /// [`Self::requires_scopes`] for services whose optional fields
    /// decide what a connection can do rather than a permission
    /// grant: a mailbox with only the sending server filled can send
    /// and cannot receive, and each node says which half it needs.
    ///
    /// Checked in the same three places, and unlike permissions the
    /// answer is never "unknown": either the connection stores the
    /// value or it does not, so a shortfall is ALWAYS a hard error
    /// naming the missing value. Still not compiler-checkable: source
    /// holds only a connection id.
    // SYNC: InputSpec.requires_values <-> packages/weft-graph/src/protocol.ts InputSpec.requiresValues
    #[serde(
        default,
        rename = "requiresValues",
        alias = "requires_values",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_values: Option<Vec<String>>,
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
    /// A list of short text values the author adds and removes one at a
    /// time (a select's options, the values a case matches).
    TextList,
    Password,
    /// The connection picker on an ACCESS NODE (the node whose
    /// metadata carries the `service` recipe). Renders the connection
    /// list (existing connections plus the declared doors of "+ Add a
    /// connection"); the stored config value is the small
    /// `{"id": "<connection id>", "identity": "..."}` handle, never a
    /// secret. `service` is COMPILER-STAMPED from the node metadata's
    /// `service.service` at enrich time; authors write only
    /// `{"kind": "access"}`.
    Access { service: Option<String> },
    /// Pick a resource on the connected service (a spreadsheet, a
    /// channel, a repo) on ONE field with several declared SOURCES;
    /// the editor uses the richest source the chosen connection
    /// actually supports (free beats a call, a call beats a popup, a
    /// popup beats typing), dropping each unusable one silently. The
    /// stored config value is `{"id": "...", "label": "..."}` (label
    /// cached for display); the runtime hands the node the bare id
    /// string.
    RemoteSelect {
        /// The name of this node's Access input that authenticates
        /// the sources needing one.
        access: String,
        /// The ways this field can be filled, in preference order.
        sources: Vec<ResourceSource>,
        /// Parent inputs for drill-down (`{repo}` in a list URL); the
        /// editor substitutes their current values and clears this
        /// field when a parent changes.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        depends_on: Vec<String>,
        /// Whether the user may TYPE a value the sources never listed
        /// (a model id shipped yesterday, a custom endpoint's name).
        /// The fetched list is then suggestions, not a closed set.
        /// Default false: a picked resource is normally an id that
        /// must exist.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        free_text: bool,
    },
    /// Build the list of config entries a node's ports come from: pick
    /// a kind, name the port it adds, fill in what that kind asks for.
    EntryList,
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
        /// The port takes several files (`List[Audio]`, or a
        /// `Media | List[Media]` that accepts one or many), so the
        /// control holds a list and writes one marker per file.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        multiple: bool,
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
            Widget::TextList => "text_list",
            Widget::Password => "password",
            Widget::Access { .. } => "access",
            Widget::RemoteSelect { .. } => "remote_select",
            Widget::EntryList => "entry_list",
            Widget::FileDrop { .. } => "file_drop",
        }
    }

    /// The ONE WeftType -> default-widget mapping. A type that can hold
    /// one file gets the drop/pick control (its accept filter derives
    /// from the file half of the type, which is also what it writes),
    /// Boolean a checkbox, Number a number box, a list of
    /// strings the add-one-at-a-time list (typing `["a", "b"]` by hand
    /// into a text area is worse at every length), and everything else
    /// (String, JSON-ish containers, mixed unions) a text area: JSON is
    /// typed as text, exactly like the source, and the compiler
    /// type-checks the literal against the declared type.
    pub fn default_for_type(ty: &WeftType) -> Widget {
        if let Some(files) = ty.file_control() {
            return Widget::FileDrop {
                accept: None,
                file_type: files.file_type,
                multiple: files.multiple,
            };
        }
        match ty {
            WeftType::Primitive(crate::weft_type::WeftPrimitive::Boolean) => Widget::Checkbox,
            WeftType::Primitive(crate::weft_type::WeftPrimitive::Number) => {
                Widget::Number { min: None, max: None, step: None }
            }
            WeftType::List(inner)
                if **inner
                    == WeftType::Primitive(crate::weft_type::WeftPrimitive::String) =>
            {
                Widget::TextList
            }
            // A plain String is a single-line box; a field that really
            // holds prose (a prompt, a message body) declares
            // `"widget": {"kind": "textarea"}` in its metadata.
            WeftType::Primitive(crate::weft_type::WeftPrimitive::String) => Widget::Text,
            // Everything else edits as JSON text, which wraps.
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

/// One way a `remote_select` field can be filled. Declared as a LIST
/// in preference order; the editor uses the richest one the chosen
/// connection supports and silently drops each source whose
/// requirement is not met:
///
/// - `granted`: the resources were recorded on the connection during
///   sign-in (a GitHub App's installed repos); free, no call at all.
///   Requires the connection to have recorded them; contributes
///   nothing on services whose consent never names resources.
/// - `list`: call the service and enumerate. Requires the listed
///   permissions on the connection, unless the lookup is `public`
///   (a credential-free endpoint), which requires nothing.
/// - `picker`: open the provider's own chooser, where choosing GRANTS
///   the picked resource. Requires a connection, nothing more.
/// - `from_url`: the user pastes a link; a pattern extracts the id.
///   Requires nothing at all. It and a `public` list are the sources
///   standing with NO connection (the works-without-signing-in path).
// SYNC: ResourceSource <-> packages/weft-graph/src/protocol.ts ResourceSource
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSource {
    /// Read the options off the connection row itself: `from` names
    /// the stored value (captured at connect) holding a JSON array.
    /// `label` / `value` are dotted paths into one array item (same
    /// vocabulary as [`Lookup`]), defaulting to `label` / `id`.
    Granted {
        from: String,
        #[serde(default = "granted_default_label")]
        label: String,
        #[serde(default = "granted_default_value")]
        value: String,
    },
    /// Today's declarative lookup, unchanged, as one source.
    List {
        #[serde(flatten)]
        lookup: Lookup,
        /// The permissions the lookup call needs on the connection;
        /// the editor drops this source when they are not held.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        requires: Vec<String>,
    },
    /// The provider's own chooser widget, declared ENTIRELY by the
    /// node: the provider script to load and the glue that runs it.
    /// Choosing opens a weft-served page in the user's browser (the
    /// same pattern as the sign-in consent) that loads `script`, runs
    /// `code`, and hands the picked resource back; weft ships no
    /// per-provider chooser code, ever. The glue runs scoped on that
    /// page, with the connection's own token, nowhere near the editor.
    Picker {
        /// The chooser script's address, loaded by the picker page.
        script: String,
        /// The glue, written by the node author: JS statements run on
        /// the picker page after `script` loads, with `weft.token`
        /// (the connection's access token), `weft.clientId` (the app's
        /// public client id, or null for a pasted key),
        /// `weft.mimeTypes`, `weft.done({id, label})`, `weft.cancel()`
        /// and `weft.fail(message)` in scope.
        code: String,
        /// The permissions that choosing a resource GRANTS (recorded
        /// on the picked value, e.g. Google's `drive.file`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        grants: Vec<String>,
        /// Narrow the chooser to these MIME types, threaded to the
        /// glue as `weft.mimeTypes`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        mime_types: Vec<String>,
    },
    /// Regex extracting the id from a pasted URL; first capture group
    /// wins.
    FromUrl { pattern: String },
}

fn granted_default_label() -> String {
    "label".into()
}

fn granted_default_value() -> String {
    "id".into()
}

/// The declarative list request behind a `remote_select` widget's
/// `list` source: ask the service for options given what the user
/// typed (`{query}`) and what's picked above (`{<parent>}` from
/// `depends_on`). The dispatcher runs it through the stored access;
/// the editor only ever sees label/value pairs.
// SYNC: Lookup <-> packages/weft-graph/src/protocol.ts Lookup
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lookup {
    /// GET URL. `{query}` interpolates the user's search text
    /// (url-encoded); `{<parent input name>}` interpolates a
    /// `depends_on` parent's picked id.
    pub get: String,
    /// Dotted path to the items array in the response.
    pub items: String,
    /// Field (dotted path, relative to one item) shown as the label.
    pub label: String,
    /// Field (dotted path, relative to one item) stored as the id.
    pub value: String,
    /// Cursor pagination, for services whose list is windowed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<PageSpec>,
    /// The endpoint is public: the call is made with no credential at
    /// all, so the source works with no connection picked (and never
    /// signs even when one is).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub public: bool,
}

/// Cursor pagination on a [`Lookup`]: the request param the cursor is
/// sent in, and the response path the next cursor is read from (empty
/// or absent = no more pages).
// SYNC: PageSpec <-> packages/weft-graph/src/protocol.ts PageSpec
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageSpec {
    pub cursor_param: String,
    pub cursor_path: String,
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

    /// The standard stored-file output quartet: `file` (the stored-file
    /// value), `filename`, `mimeType`, `sizeBytes`. Every node whose
    /// job is "put bytes in storage and hand the file downstream" emits
    /// this exact port set, so the shape is a constructor rather than
    /// four `set` calls each download node restates.
    pub fn stored_file(stored: crate::storage::StoredFile) -> Self {
        Self::new()
            .set("file", stored.to_value())
            .set("filename", stored.filename)
            .set("mimeType", stored.mime_type)
            .set("sizeBytes", stored.size_bytes)
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
            "type": "T", "label": "L", "description": "D"
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
mod spec_field_tests {
    use super::*;

    fn spec(json: serde_json::Value) -> PortSpec {
        serde_json::from_value(json).unwrap()
    }

    /// A field written as a name is the one-line String box of that
    /// name, so the kinds that only want a label and a placeholder say
    /// exactly that.
    #[test]
    fn a_named_field_expands_to_a_one_line_box() {
        let text_input = spec(serde_json::json!({
            "kind": "text_input", "label": "Text input",
            "fields": ["label", "placeholder"],
            "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }]
        }));
        let keys: Vec<&str> = text_input.fields.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(keys, ["label", "placeholder"]);
        let placeholder = &text_input.fields[1];
        assert_eq!(placeholder.label, "Placeholder");
        assert_eq!(placeholder.shape, SpecShape::Typed);
        assert_eq!(placeholder.value_type, Some(WeftType::parse("String").unwrap()));
        assert_eq!(placeholder.resolved_widget().kind_name(), "text");
        assert!(!placeholder.required);
    }

    /// The two forms mix in one list, in the order they are written.
    #[test]
    fn a_list_mixes_names_and_declarations() {
        let select = spec(serde_json::json!({
            "kind": "select", "label": "Select",
            "fields": [
                "label",
                { "key": "options", "label": "Options", "shape": "typed",
                  "valueType": "List[String]", "required": true }
            ],
            "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }]
        }));
        let keys: Vec<&str> = select.fields.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(keys, ["label", "options"]);
        assert!(select.fields[1].required, "a written declaration keeps everything it said");
    }

    /// A kind nobody reads on a screen asks for neither, which is what
    /// a switch case is.
    #[test]
    fn a_kind_that_names_none_gets_none() {
        let case = spec(serde_json::json!({
            "kind": "equals", "keyField": "port", "label": "is exactly this value",
            "fields": [{ "key": "value", "shape": "value", "required": true }],
            "addsOutputs": [{ "nameTemplate": "{key}", "portType": "Boolean" }]
        }));
        let keys: Vec<&str> = case.fields.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(keys, ["value"]);
    }

    /// A mistyped key inside a written declaration still names itself,
    /// rather than the list failing as a whole.
    #[test]
    fn a_mistyped_key_still_names_itself() {
        let bad = serde_json::from_value::<PortSpec>(serde_json::json!({
            "kind": "select", "label": "Select",
            "fields": [{ "key": "options", "shape": "typed", "valueType": "List[String]",
                         "requried": true }],
            "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }]
        }));
        let message = bad.expect_err("an unknown key fails the load").to_string();
        assert!(message.contains("requried"), "the error names the key: {message}");
    }

    /// A key with more than one word in it reads as words.
    #[test]
    fn a_named_field_reads_its_key_as_words() {
        assert_eq!(SpecField::named("placeholder").label, "Placeholder");
        assert_eq!(SpecField::named("min_length").label, "Min Length");
        assert_eq!(SpecField::named("minLength").label, "Min Length");
        assert_eq!(SpecField::named("ID").label, "ID");
        assert_eq!(SpecField::named("HTTPMethod").label, "HTTP Method");
        // A single lowercase tail on an acronym is a plural, not a word.
        assert_eq!(SpecField::named("IDs").label, "IDs");
        assert_eq!(SpecField::named("channelURLs").label, "Channel URLs");
    }
}

#[cfg(test)]
mod widget_default_tests {
    use super::*;
    use crate::weft_type::{WeftPrimitive, WeftType};

    /// What control a `typed` spec field draws when it declares no
    /// widget. A `List[String]` (a select's options, a switch's cases)
    /// gets the add-one-at-a-time list; everything list-shaped that is
    /// not strings stays a text area, where JSON is the honest form.
    #[test]
    fn a_list_of_strings_gets_the_list_editor() {
        let strings = WeftType::List(Box::new(WeftType::Primitive(WeftPrimitive::String)));
        assert_eq!(Widget::default_for_type(&strings).kind_name(), "text_list");

        let numbers = WeftType::List(Box::new(WeftType::Primitive(WeftPrimitive::Number)));
        assert_eq!(Widget::default_for_type(&numbers).kind_name(), "textarea");

        // A plain String is a single line; a prose field declares
        // `textarea` explicitly in its metadata.
        assert_eq!(
            Widget::default_for_type(&WeftType::Primitive(WeftPrimitive::String)).kind_name(),
            "text"
        );
        assert_eq!(
            Widget::default_for_type(&WeftType::Primitive(WeftPrimitive::Boolean)).kind_name(),
            "checkbox"
        );
    }

    /// A port holding files gets the picker, one file or many: a plain
    /// `List[Media]` picks with `multiple`, and a union like
    /// `Media | List[Media]` picks too. A union mixing files with a
    /// non-file keeps the text area.
    fn file_drop(ty: &str) -> (String, bool) {
        match Widget::default_for_type(&WeftType::parse(ty).expect("parses")) {
            Widget::FileDrop { file_type, multiple, .. } => (file_type.to_string(), multiple),
            other => panic!("expected a file drop for {ty}, got {}", other.kind_name()),
        }
    }

    #[test]
    fn a_port_holding_files_gets_the_picker() {
        // One file, and the type it writes is the one it picks.
        assert_eq!(file_drop("Image"), ("Image".to_string(), false));

        // Several: the control holds a list and writes one marker each.
        assert_eq!(file_drop("List[Audio]"), ("Audio".to_string(), true));

        // One or several: still the multi control, and the type it
        // writes is the file half, never the whole union.
        assert_eq!(file_drop("Media | List[Media]"), ("Media".to_string(), true));
        assert_eq!(file_drop("File | List[File]"), ("File".to_string(), true));
    }

    #[test]
    fn a_union_mixing_a_file_with_anything_else_keeps_the_text_area() {
        // A picker would take the other half of the union away.
        let either = WeftType::parse("String | Image").expect("parses");
        assert_eq!(Widget::default_for_type(&either).kind_name(), "textarea");
    }

    /// The field the screenshot was taken of: a multi-select form field
    /// declares `options: List[String]` with no widget, so it must draw
    /// the list editor rather than a JSON text area.
    #[test]
    fn a_spec_fields_options_draw_the_list_editor() {
        let field: SpecField = serde_json::from_value(serde_json::json!({
            "key": "options", "label": "Options",
            "shape": "typed", "valueType": "List[String]", "required": true
        }))
        .unwrap();
        assert_eq!(field.resolved_widget().kind_name(), "text_list");
    }
}

#[cfg(test)]
mod catalog_wire_tests {
    use super::*;

    /// Layer-2 wire-shape for the CATALOG boundary: `resolved()` metadata is
    /// what `weft describe-nodes` / the parse server ship to the editor, and
    /// the editor's `CatalogEntry`/`InputSpec`/`Widget`/`NodeFeaturesWire`
    /// mirrors read exact key names. Pin the serialized key sets (top level,
    /// per input, per widget variant, features) so a renamed or newly
    /// unrenamed field fails HERE, not as a silently-undefined editor read.
    /// The serialized fixture doubles as the TS side's checked-in fixture
    /// (packages/weft-graph/src/protocol.test.ts).
    // SYNC: catalog wire fixture <-> packages/weft-graph/src/protocol.test.ts catalog wire fixture
    #[test]
    fn resolved_metadata_serializes_the_editor_contract() {
        let meta: NodeMetadata = serde_json::from_value(serde_json::json!({
            "type": "Fixture", "label": "Fixture", "description": "d",
            "tags": ["a"], "icon": "Zap", "color": "#123456",
            "requires_infra": true,
            "inputs": [
                { "name": "code", "type": "String", "required": true,
                  "widget": { "kind": "code", "language": "python" } },
                { "name": "pick", "type": "String", "exposure": "config",
                  "widget": { "kind": "select", "options": ["a", "b"] } },
                { "name": "n", "type": "Number",
                  "widget": { "kind": "number", "min": 0.0, "max": 9.0, "step": 1.0 } },
                { "name": "grant", "type": "Access",
                  "widget": { "kind": "access" },
                  "requiresScopes": ["s.read"], "requiresValues": ["host"] },
                { "name": "sheet", "type": "String",
                  "widget": { "kind": "remote_select", "access": "grant",
                              "sources": [{ "kind": "granted", "from": "sheets" }], "depends_on": ["pick"] } },
                { "name": "img", "type": "Image",
                  "widget": { "kind": "file_drop", "type": "Image", "accept": "image/png" } },
                { "name": "fields", "type": "List[JsonDict]", "exposure": "config",
                  "widget": { "kind": "entry_list" } }
            ],
            "outputs": [{ "name": "out", "type": "String" }],
            "features": { "oneOfRequired": [["code", "img"]], "isTrigger": true,
                          "canAddInputPorts": true,
                          "showDebugPreview": true, "liveEndpoint": "web" },
            "display": { "kind": "media", "output": "out" },
            "portsFromConfig": { "field": "fields", "matchInput": "n", "specs": [
                { "kind": "text", "label": "Text",
                  "render": { "component": "text_input", "source": "input", "multiple": true },
                  "fields": [
                      "label",
                      { "key": "options", "label": "Options", "required": true,
                        "shape": "typed", "valueType": "List[String]" },
                      { "key": "at_least", "label": "At least", "shape": "number" }
                  ],
                  "catchAll": true,
                  "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }] }
            ] }
        }))
        .expect("fixture metadata loads");
        let v = serde_json::to_value(meta.resolved()).expect("serializes");

        // Key ORDER is not part of the contract (whether serde_json
        // iterates sorted or insertion-ordered depends on feature
        // unification across the build); compare sorted sets, with
        // every expectation below listed alphabetically.
        let keys = |o: &serde_json::Value| -> Vec<String> {
            let mut ks: Vec<String> = o.as_object().unwrap().keys().cloned().collect();
            ks.sort();
            ks
        };
        assert_eq!(
            keys(&v),
            ["color", "description", "display", "features", "icon",
             "inputs", "label", "outputs", "portsFromConfig", "requires_infra", "tags", "type"],
            "top-level catalog keys are the editor contract"
        );
        // Every resolved input ships the full editor surface, with the
        // renamed camelCase keys the TS mirror reads.
        assert_eq!(
            keys(&v["inputs"][3]),
            ["default", "description", "exposure", "label", "name", "placeholder",
             "required", "requiresScopes", "requiresValues", "type", "widget"]
        );
        // Widget variants serialize their own payload under the tag.
        assert_eq!(keys(&v["inputs"][0]["widget"]), ["kind", "language"]);
        assert_eq!(keys(&v["inputs"][1]["widget"]), ["kind", "options"]);
        assert_eq!(keys(&v["inputs"][2]["widget"]), ["kind", "max", "min", "step"]);
        assert_eq!(keys(&v["inputs"][4]["widget"]), ["access", "depends_on", "kind", "sources"]);
        assert_eq!(keys(&v["inputs"][5]["widget"]), ["accept", "kind", "type"]);
        assert_eq!(
            keys(&v["features"]),
            ["canAddInputPorts", "isTrigger", "liveEndpoint",
             "oneOfRequired", "showDebugPreview"],
            "feature keys are the camelCase forms the editor reads"
        );
        assert_eq!(
            keys(&v["display"]),
            ["kind", "output"],
            "the display declaration carries only its kind and the named side"
        );
        assert_eq!(keys(&v["portsFromConfig"]), ["field", "matchInput", "specs"]);
        assert_eq!(keys(&v["portsFromConfig"]["specs"][0]),
            ["addsInputs", "addsOutputs", "catchAll", "fields", "keyField", "kind", "label",
             "render"]);
        assert_eq!(keys(&v["portsFromConfig"]["specs"][0]["render"]),
            ["component", "multiple", "source"]);
        // Every entry field ships its RESOLVED widget, so the editor
        // draws a control without deriving one from the shape.
        assert_eq!(keys(&v["portsFromConfig"]["specs"][0]["fields"][0]),
            ["key", "label", "required", "shape", "valueType", "widget"]);
        // A field written as a name (`"label"`) reaches the editor as a
        // whole declaration, drawn as a one-line box.
        assert_eq!(v["portsFromConfig"]["specs"][0]["fields"][0]["key"], "label");
        assert_eq!(v["portsFromConfig"]["specs"][0]["fields"][0]["widget"]["kind"], "text");
        assert_eq!(v["portsFromConfig"]["specs"][0]["fields"][1]["key"], "options");
        assert_eq!(v["portsFromConfig"]["specs"][0]["fields"][2]["widget"]["kind"], "number");
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
            r#"{ "type": "T", "label": "T", "description": "" }"#,
        )
        .map(|mut m| {
            m.inputs = inputs;
            m
        })
        .unwrap()
    }

    /// A node whose ports come from a config list: the list has to live
    /// under a key the node DECLARES, because everything downstream (the
    /// editor's control, validate's undeclared-key check) treats it as
    /// an ordinary config input.
    #[test]
    fn ports_from_config_names_a_declared_input() {
        let spec = || PortSpec {
            kind: "case".into(),
            key_field: "port".into(),
            label: String::new(),
            render: None,
            fields: vec![],
            catch_all: false,
            adds_inputs: vec![],
            adds_outputs: vec![PortTemplate::new("{key}", "Boolean")],
        };

        let mut undeclared = metadata_with(vec![]);
        undeclared.ports_from_config =
            Some(PortsFromConfig { field: "cases".into(), match_input: None, specs: vec![spec()] });
        let e = undeclared.validate_semantics().unwrap_err();
        assert!(e.contains("declare as an input"), "{e}");

        let mut declared = metadata_with(vec![input(
            "cases",
            WeftType::List(Box::new(WeftType::JsonDict)),
        )]);
        declared.ports_from_config =
            Some(PortsFromConfig { field: "cases".into(), match_input: None, specs: vec![spec()] });
        declared.validate_semantics().expect("the list key is declared");
    }

    /// A kind's fields are held to a shape the compiler can check: only
    /// a `typed` field names a type, and it must. An entry-list widget
    /// must edit the very list the node's ports come from, or it would
    /// draw a dropdown of nothing.
    #[test]
    fn ports_from_config_holds_a_kind_s_fields_and_its_editor_honest() {
        let with_field = |field: serde_json::Value| {
            let mut m = metadata_with(vec![input(
                "cases",
                WeftType::List(Box::new(WeftType::JsonDict)),
            )]);
            m.ports_from_config = Some(
                serde_json::from_value(serde_json::json!({
                    "field": "cases",
                    "specs": [{
                        "kind": "equals",
                        "keyField": "port",
                        "fields": [field],
                        "addsOutputs": [{ "nameTemplate": "{key}", "portType": "Boolean" }]
                    }]
                }))
                .expect("spec parses"),
            );
            m
        };

        let e = with_field(serde_json::json!({ "key": "v", "shape": "typed" }))
            .validate_semantics()
            .unwrap_err();
        assert!(e.contains("must name a `valueType`"), "{e}");

        let e = with_field(
            serde_json::json!({ "key": "v", "shape": "number", "valueType": "Number" }),
        )
        .validate_semantics()
        .unwrap_err();
        assert!(e.contains("only a `typed` field takes"), "{e}");

        // A shape measured against the matched input, with nothing
        // saying what that input is.
        let e = with_field(serde_json::json!({ "key": "v", "shape": "value" }))
            .validate_semantics()
            .unwrap_err();
        assert!(e.contains("matchInput"), "{e}");

        // A matchInput naming a port the node does not declare would
        // resolve to nothing and silently disable every value check.
        let mut phantom = with_field(
            serde_json::json!({ "key": "v", "shape": "typed", "valueType": "Number" }),
        );
        phantom.ports_from_config.as_mut().expect("spec present").match_input =
            Some("nope".into());
        let e = phantom.validate_semantics().unwrap_err();
        assert!(e.contains("matches entries against input"), "{e}");

        // The editor widget has to sit on the list it edits.
        let mut stray = metadata_with(vec![InputSpec {
            widget: Some(Widget::EntryList),
            ..input("elsewhere", WeftType::List(Box::new(WeftType::JsonDict)))
        }]);
        stray.ports_from_config = None;
        let e = stray.validate_semantics().unwrap_err();
        assert!(e.contains("portsFromConfig`.field` must name it") || e.contains("must name it"), "{e}");
    }

    /// Two specs claiming one kind would make an entry match both, and a
    /// spec that adds no ports makes an entry that does nothing. Both are
    /// authoring mistakes with no sensible reading.
    #[test]
    fn ports_from_config_refuses_ambiguous_or_empty_specs() {
        let base = || {
            metadata_with(vec![input("cases", WeftType::List(Box::new(WeftType::JsonDict)))])
        };
        let spec = |kind: &str, ports: Vec<PortTemplate>| PortSpec {
            kind: kind.into(),
            key_field: "port".into(),
            label: String::new(),
            render: None,
            fields: vec![],
            catch_all: false,
            adds_inputs: vec![],
            adds_outputs: ports,
        };

        let mut twice = base();
        twice.ports_from_config = Some(PortsFromConfig {
            field: "cases".into(),
            match_input: None,
            specs: vec![
                spec("case", vec![PortTemplate::new("{key}", "Boolean")]),
                spec("case", vec![PortTemplate::new("{key}", "String")]),
            ],
        });
        assert!(twice.validate_semantics().unwrap_err().contains("two specs claim kind"));

        let mut empty = base();
        empty.ports_from_config = Some(PortsFromConfig {
            field: "cases".into(),
            match_input: None,
            specs: vec![spec("case", vec![])],
        });
        assert!(empty.validate_semantics().unwrap_err().contains("adds no ports"));
    }

    /// A node that hands out a connection to a service it runs
    /// itself: the two rules that keeps honest are that the name IS a
    /// service name, and that there is an output to hand the
    /// connection out on.
    #[test]
    fn a_publishing_node_needs_a_service_name_and_an_access_output() {
        let publishing = |service: &str, outputs: Vec<OutputSpec>| {
            let mut m = metadata_with(vec![]);
            m.publishes = Some(service.into());
            m.outputs = outputs;
            m
        };
        let access_out = || OutputSpec {
            name: "access".into(),
            port_type: WeftType::Access,
            required: false,
            description: None,
        };

        publishing("postgres", vec![access_out()])
            .validate_semantics()
            .expect("a service name plus an Access output is the whole rule");

        let e = publishing("Postgres-DB", vec![access_out()])
            .validate_semantics()
            .expect_err("a name that is not a service name refuses");
        assert!(e.contains("is not one"), "{e}");

        let e = publishing("postgres", vec![])
            .validate_semantics()
            .expect_err("nothing to hand the connection out on refuses");
        assert!(e.contains("no Access output"), "{e}");
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
            requires_scopes: None,
            requires_values: None,
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
            ("String", "text"),
            // A list of files is the picker too, holding several.
            ("List[Image]", "file_drop"),
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
            Widget::Access { service: None },
            Widget::RemoteSelect {
                access: "account".into(),
                sources: vec![ResourceSource::FromUrl { pattern: "(x)".into() }],
                depends_on: vec![],
                free_text: false,
            },
            Widget::EntryList,
            Widget::TextList,
            Widget::FileDrop {
                accept: None,
                file_type: WeftType::primitive(WeftPrimitive::Image),
                multiple: false,
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

    /// The display declaration names exactly one existing port with
    /// its side; anything else is refused at load, never a silent
    /// no-preview.
    #[test]
    fn validate_semantics_checks_the_display_declaration() {
        let display = |input: Option<&str>, output: Option<&str>| DisplaySpec {
            kind: DisplayKind::Media,
            input: input.map(str::to_string),
            output: output.map(str::to_string),
        };
        let with = |d: DisplaySpec| {
            let mut m = metadata_with(vec![input(
                "media",
                WeftType::primitive(WeftPrimitive::Image),
            )]);
            m.outputs.push(OutputSpec {
                name: "file".into(),
                port_type: WeftType::primitive(WeftPrimitive::Image),
                required: false,
                description: None,
            });
            m.display = Some(d);
            m
        };
        assert!(with(display(Some("media"), None)).validate_semantics().is_ok());
        assert!(with(display(None, Some("file"))).validate_semantics().is_ok());
        let e = with(display(Some("nope"), None)).validate_semantics().unwrap_err();
        assert!(e.contains("input 'nope'"), "{e}");
        let e = with(display(None, Some("nope"))).validate_semantics().unwrap_err();
        assert!(e.contains("output 'nope'"), "{e}");
        let e = with(display(None, None)).validate_semantics().unwrap_err();
        assert!(e.contains("exactly one port"), "{e}");
        let e = with(display(Some("media"), Some("file"))).validate_semantics().unwrap_err();
        assert!(e.contains("exactly one port"), "{e}");
    }

    /// The access widget's rules bind on the access node's OWN input:
    /// its declared type must be Access (the marker the node reads),
    /// and the Access type's Wire exposure default never satisfies the
    /// widget's `exposure: config` requirement, so an author must
    /// declare it explicitly.
    #[test]
    fn validate_semantics_types_the_access_widget_input() {
        // Default (Wire) exposure: the config requirement fires.
        let mut n = input("account", WeftType::Access);
        n.widget = Some(Widget::Access { service: None });
        let e = metadata_with(vec![n]).validate_semantics().unwrap_err();
        assert!(e.contains("exposure: config"), "{e}");

        // With the service recipe present: an Access-typed input
        // passes, a JsonDict-typed one is rejected by the widget's
        // value-shape check (the value the node reads is the Access
        // marker, which JsonDict cannot hold).
        let with_service = |ty: WeftType| -> NodeMetadata {
            let mut n = input("account", ty);
            n.exposure = Some(Exposure::Config);
            n.widget = Some(Widget::Access { service: None });
            let mut m = metadata_with(vec![n]);
            m.service = Some(
                serde_json::from_value(json!({
                    "service": "tg",
                    "acquisition": {"kind": "static", "fields": [
                        {"name": "token", "label": "Token"}]}
                }))
                .expect("spec"),
            );
            m
        };
        assert!(with_service(WeftType::Access).validate_semantics().is_ok());
        let e = with_service(WeftType::JsonDict).validate_semantics().unwrap_err();
        assert!(e.contains("access widget edits"), "{e}");
    }

    /// A remote_select needs at least one source, and a from_url
    /// source's pattern must parse, so a typo'd extractor fails the
    /// metadata load instead of silently never matching a paste.
    #[test]
    fn validate_semantics_checks_remote_select_sources() {
        let account = input("account", WeftType::Access);
        let mut field = input("sheet", WeftType::primitive(WeftPrimitive::String));
        field.widget = Some(Widget::RemoteSelect {
            access: "account".into(),
            sources: vec![],
            depends_on: vec![],
            free_text: false,
        });
        let e = metadata_with(vec![account.clone(), field.clone()])
            .validate_semantics()
            .unwrap_err();
        assert!(e.contains("at least one source"), "{e}");

        field.widget = Some(Widget::RemoteSelect {
            access: "account".into(),
            sources: vec![ResourceSource::FromUrl { pattern: "([unclosed".into() }],
            depends_on: vec![],
            free_text: false,
        });
        let e = metadata_with(vec![account.clone(), field.clone()])
            .validate_semantics()
            .unwrap_err();
        assert!(e.contains("from_url"), "{e}");

        field.widget = Some(Widget::RemoteSelect {
            access: "account".into(),
            sources: vec![ResourceSource::FromUrl {
                pattern: "/spreadsheets/d/([a-zA-Z0-9_-]+)".into(),
            }],
            depends_on: vec![],
            free_text: false,
        });
        assert!(metadata_with(vec![account, field]).validate_semantics().is_ok());
    }

    /// A project-declared app may never carry a secret: project
    /// metadata is source, and source never holds secrets.
    #[test]
    fn validate_semantics_rejects_a_confidential_project_app() {
        let mut meta = metadata_with(vec![]);
        meta.access_apps.insert(
            "google".into(),
            crate::access::spec::AppRegistration {
                label: "My app".into(),
                client_id: "cid".into(),
                client_secret: Some("sec".into()),
                extra: Default::default(),
            },
        );
        let e = meta.validate_semantics().unwrap_err();
        assert!(e.contains("client_secret"), "{e}");

        let mut public = metadata_with(vec![]);
        public.access_apps.insert(
            "google".into(),
            crate::access::spec::AppRegistration {
                label: "My app".into(),
                client_id: "cid".into(),
                client_secret: None,
                extra: Default::default(),
            },
        );
        assert!(public.validate_semantics().is_ok());
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
        assert_eq!(resolved.inputs[0].widget, Some(Widget::Text));
        let again = resolved.resolved();
        assert_eq!(again.inputs[0].exposure, resolved.inputs[0].exposure);
        assert_eq!(again.inputs[0].widget, resolved.inputs[0].widget);
    }
}


// ── ports derived from a node's own config ──────────────────────────────

use crate::project::{InputDefinition, PortDefinition};

/// The (input, output) ports a node derives from a LIST in its own config (a
/// form's `fields`, a switch's `cases`). Pure: reads each entry's `kind` and
/// its port name, matches the spec, and resolves that spec's `adds_inputs` /
/// `adds_outputs` templates. The enricher folds these into the node's known
/// ports (see the call site).
///
/// Entries this node has no spec for are SKIPPED here rather than rejected:
/// `validate` owns the diagnostics, so an unknown kind surfaces there with a
/// span instead of as a missing port with no explanation.
pub fn derive_config_ports(
    config: &Value,
    ports_from_config: &PortsFromConfig,
) -> (Vec<InputDefinition>, Vec<PortDefinition>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let Some(entries) = config.get(&ports_from_config.field).and_then(|f| f.as_array()) else {
        return (inputs, outputs);
    };

    for entry in entries {
        let Some(obj) = entry.as_object() else { continue };
        let Some(kind) = obj.get("kind").and_then(|v| v.as_str()) else { continue };
        let Some(spec) = ports_from_config.spec_for(kind) else { continue };
        let key = obj.get(&spec.key_field).and_then(|v| v.as_str()).unwrap_or_default();
        if key.is_empty() {
            continue;
        }

        for port in &spec.adds_inputs {
            inputs.push(InputDefinition::from_wire_port(materialize_port(port, key, false)));
        }
        for port in &spec.adds_outputs {
            outputs.push(materialize_port(port, key, true));
        }
    }
    (inputs, outputs)
}

/// The metadata sentinel meaning "this port's own type variable": the
/// materializer turns it into `T__{key}`, one variable per port, so
/// sibling ports never unify by accident.
// SYNC: AUTO_TYPE_VAR <-> packages/weft-graph/src/webview/lib/utils/port-specs.ts
//       AUTO_TYPE_VAR_MARKER
pub const AUTO_TYPE_VAR: &str = "T_Auto";

/// Replace every `T_Auto` placeholder with a TypeVar scoped to the
/// field key, recursing through every container arm: `List[T_Auto]`
/// reads as "a list of anything, scoped to this field", so a nested
/// placeholder scopes exactly like a top-level one.
// SYNC: materialize_auto_type_vars <-> packages/weft-graph/src/webview/lib/utils/port-specs.ts materializeAutoTypeVars
pub fn materialize_auto_type_vars(t: &WeftType, key: &str) -> WeftType {
    match t {
        WeftType::TypeVar(n) if n == AUTO_TYPE_VAR => WeftType::type_var(&format!("T__{key}")),
        WeftType::List(inner) => WeftType::List(Box::new(materialize_auto_type_vars(inner, key))),
        WeftType::Generator(inner) => {
            WeftType::Generator(Box::new(materialize_auto_type_vars(inner, key)))
        }
        WeftType::Dict(k, v) => WeftType::Dict(
            Box::new(materialize_auto_type_vars(k, key)),
            Box::new(materialize_auto_type_vars(v, key)),
        ),
        WeftType::Union(members) => {
            WeftType::Union(members.iter().map(|m| materialize_auto_type_vars(m, key)).collect())
        }
        WeftType::Record(fields) => WeftType::Record(
            fields
                .iter()
                .map(|f| crate::weft_type::RecordField {
                    name: f.name.clone(),
                    ty: materialize_auto_type_vars(&f.ty, key),
                    optional: f.optional,
                })
                .collect(),
        ),
        // No `Named` arm: a declared body is concrete by construction
        // (the registry and the wire parser both refuse a type
        // variable inside one), so there is never a `T_Auto` to
        // materialize beneath an alias, and rebuilding the body here
        // would be the door to two same-named types with different
        // bodies.
        other => other.clone(),
    }
}

fn materialize_port(template: &PortTemplate, key: &str, is_output: bool) -> PortDefinition {
    let name = template.resolve_name(key);
    let port_type = materialize_auto_type_vars(&template.port_type, key);
    PortDefinition {
        name,
        port_type,
        required: !is_output,
        description: None,
        synthesized_from_carry: false,
    }
}


#[cfg(test)]
mod package_defaults_tests {
    use super::*;

    const MEMBER: &str = r#"{ "type": "LlmInference", "label": "LLM",
        "description": "" }"#;
    const DEFAULTS: &str = r#"{ "portsFromConfig": { "field": "fields", "specs":
        [{ "kind": "root_spec", "label": "Root spec", "render": { "component": "text" } }] } }"#;

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

        let spec_kinds = |m: &NodeMetadata| {
            m.ports_from_config
                .as_ref()
                .map(|p| p.specs.iter().map(|s| s.kind.clone()).collect::<Vec<_>>())
                .unwrap_or_default()
        };
        assert_eq!(spec_kinds(&from_derive), spec_kinds(&from_catalog));
        assert_eq!(
            spec_kinds(&from_derive),
            vec!["root_spec".to_string()],
            "the package-root defaults reach the merged metadata"
        );
        // A bare node (no defaults) parses its own file unchanged.
        let bare = NodeMetadata::parse_embedded(MEMBER, None, "test");
        assert!(bare.ports_from_config.is_none());
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

    /// `accessApps` declared at a package root reaches a member node
    /// that declares none of its own, and lands as typed metadata: the
    /// app is declared once and inherited, keyed by service name.
    #[test]
    fn access_apps_inherit_from_the_package_root() {
        let mut member = serde_json::json!({
            "type": "SlackSendMessage",
            "label": "Send Slack message",
            "description": "d",
            "inputs": [],
            "outputs": []
        });
        let defaults: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({
                "accessApps": {
                    "slack": { "label": "Slack", "client_id": "cid" }
                }
            }),
        )
        .unwrap();
        merge_package_defaults(&mut member, &defaults).unwrap();
        let meta: NodeMetadata = serde_json::from_value(member).unwrap();
        let app = meta.access_apps.get("slack").expect("inherited the slack app");
        assert_eq!(app.client_id, "cid");
        assert_eq!(app.label, "Slack");
        assert!(app.client_secret.is_none(), "a project app is a public client");

        // A member's OWN app for a service wins wholesale over the root's.
        let mut member = serde_json::json!({
            "type": "T", "label": "l", "description": "d",
            "inputs": [], "outputs": [],
            "accessApps": { "slack": { "label": "Mine", "client_id": "mine" } }
        });
        merge_package_defaults(&mut member, &defaults).unwrap();
        let meta: NodeMetadata = serde_json::from_value(member).unwrap();
        assert_eq!(meta.access_apps["slack"].client_id, "mine", "member wins wholesale");
    }
}


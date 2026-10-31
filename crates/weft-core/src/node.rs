use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::weft_type::{WeftPrimitive, WeftType};

// The `Node` trait is the RUNTIME node interface (it runs against the run
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
    use crate::context::{RunContext, ValueBag};
    use crate::error::{WeftError, WeftResult};
    use crate::infra::{InfraProvisionContext, InfraSpec};

    /// The core trait every node implements. Stdlib nodes in `catalog/`
    /// and a project's own nodes (under its `nodes/`, or beside its code
    /// under `src/`) all implement this.
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
    ///   - an infra node (`metadata.requiresInfra == true`) additionally
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
        /// desired state as a typed value; the engine has it applied,
        /// then calls `run`. The default impl returns Err; nodes that
        /// declare `requiresInfra: true` MUST override.
        async fn provision_infra(
            &self,
            _ctx: InfraProvisionContext,
            _input: ValueBag,
        ) -> WeftResult<InfraSpec> {
            Err(WeftError::Config(format!(
                "node '{}' declared requiresInfra: true but did not implement Node::provision_infra",
                self.node_type()
            )))
        }

        /// Register this trigger's wake signal (`ctx.register_signal`).
        /// The engine calls it INSTEAD of `run` when a trigger is being
        /// set up. Only nodes whose manifest declares `features.isTrigger`
        /// implement it; the default fails loud on the mismatch.
        async fn setup_trigger(&self, _ctx: RunContext) -> WeftResult<()> {
            Err(WeftError::Config(format!(
                "node '{}' declared features.isTrigger but did not implement Node::setup_trigger",
                self.node_type()
            )))
        }

        /// The node's normal body. `ctx` provides language primitives
        /// (`pulse_downstream`, `create_bus`, `bus`, `await_signal`,
        /// `log`, `endpoint`). The ONLY way to fire
        /// downstream is `ctx.pulse_downstream(output)`. For a plain
        /// (non-trigger) node the engine calls this in every lifecycle
        /// phase (a value feeding a trigger's config must be produced at
        /// setup time too); for a trigger it runs only on a real firing,
        /// with the fire's payload fields on the `ctx.wake` bag.
        async fn run(&self, ctx: RunContext) -> WeftResult<()>;

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
    /// The file the span's coordinates live in, when it is NOT the
    /// source that was compiled: an `@include` splices another file's
    /// nodes into the project with their own line numbers, and without
    /// this a finding on an included node points a reader (and the
    /// editor's click-to-jump) at the wrong file. None = the compiled
    /// source itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// The nodes and groups the finding is about, by id: one for a node or
    /// a group, the two ends for a wire. An error with subjects breaks only
    /// those parts of the program ([`crate::project::Broken`]), and a
    /// command whose cut does not need them goes on; an error with none is
    /// about the program as a whole and stops every command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subjects: Vec<String>,
}

impl Diagnostic {
    /// A diagnostic bounded to a source `Span` (the culprit's exact range).
    /// `file` follows the same rule as the field: the span's own file when
    /// it differs from the compiled source, else None.
    pub fn at(span: crate::project::Span, severity: Severity, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            line: span.start_line,
            column: span.start_column,
            end_line: span.end_line,
            end_column: span.end_column,
            severity,
            message: message.into(),
            code: Some(code.into()),
            file: None,
            subjects: Vec::new(),
        }
    }

    pub fn in_file(mut self, file: Option<&str>) -> Self {
        self.file = file.map(str::to_string);
        self
    }

    /// The parts this finding is about ([`Self::subjects`]).
    pub fn about<S: Into<String>>(mut self, subjects: impl IntoIterator<Item = S>) -> Self {
        self.subjects = subjects.into_iter().map(Into::into).collect();
        self
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// `accepts` says which drivers it takes; its widget (the
    /// editor control) derives from the type unless overridden.
    #[serde(default)]
    pub inputs: Vec<InputSpec>,
    /// Output ports.
    #[serde(default)]
    pub outputs: Vec<OutputSpec>,
    /// Ports whose value is written here, by hand, and never computed by
    /// a run ([`crate::project::MetadataPort`]): a wire leaving one is a
    /// metadata wire, and the compiler writes the value into the input it
    /// points at. Their names share one namespace with the inputs and the
    /// outputs.
    // SYNC: NodeMetadata.metadata_ports <-> packages/weft-graph/src/protocol.ts CatalogEntry.metadataPorts
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_ports: Vec<crate::project::MetadataPort>,
    /// Whether this node implements `Node::provision_infra` and needs
    /// dispatcher-driven infrastructure (containers, disks, endpoints).
    /// Explicit flag in metadata.json; mirrored to
    /// `NodeDefinition.requires_infra` at enrich time.
    #[serde(default)]
    pub requires_infra: bool,
    /// Local image source directories the CLI must build for this
    /// node. Each entry is a path relative to THE NODE'S OWN
    /// DIRECTORY, the one holding its `metadata.json`, naming a
    /// directory that contains a `Dockerfile` (`image_set.rs` joins it
    /// onto `CatalogEntry::source_dir`; a member of a package does not
    /// reach up to the package root for these). The directory's
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// user pastes none. The catalog holds each entry to
    /// [`crate::access::spec::AppRegistration::check_shipped`] once every
    /// service is known (a shipped app is public, so it holds no secret,
    /// and it names a service whose connect takes an app). Empty in the
    /// shipped catalog.
    // SYNC: NodeMetadata.access_apps <-> packages/weft-graph/src/protocol.ts CatalogEntry.accessApps
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
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
    /// What a TRIGGER wakes with: field name to weft type
    /// (`{"scheduledTime": "String", "caller?": "JsonDict"}`). A `?` on
    /// the NAME means the payload may arrive without that field, the
    /// same `?` a record type uses.
    ///
    /// A trigger's fire payload is not its inputs and not its outputs:
    /// it is what the listener (or a person typing `weft run --fire`)
    /// delivers to start one firing, and until now it lived only inside
    /// the node body, where nothing outside could read it or hold the
    /// node to it. Declared here it becomes a contract the engine
    /// checks before the body runs, so a listener that changed its
    /// fields and a hand-typed payload both fail naming the field
    /// rather than somewhere inside the node.
    ///
    /// It names EVERY field that can arrive, not just the ones the node
    /// puts on ports: a payload carrying a field this does not name is
    /// refused, the same as one missing a field it does
    /// ([`check_fire_payload`]). A field that only sometimes arrives
    /// takes the `?`. For a trigger fed by a provider's events, the
    /// list to copy is the service's topic
    /// (`AccessSpec.events.<topic>.fields`), which is exactly what a
    /// named event can carry.
    ///
    /// Nothing renders it in the graph: it is not a port, nothing can
    /// wire to it, and no one types into it.
    ///
    /// Only a trigger is meant to declare it (`features.isTrigger`), and
    /// every trigger is meant to have one: `weft run --fire` prints it
    /// when a payload does not match, which is the only way an author
    /// learns what to send. The load refuses it on a node that is not a
    /// trigger, and refuses a type it cannot read. Nothing makes a
    /// trigger declare one: the repo test
    /// `weft-compiler/tests/integration/fires_with.rs` holds the shipped triggers to
    /// it; a third-party trigger may leave it out.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub fires_with: std::collections::BTreeMap<String, String>,
    /// The public address this node claims when the project goes live,
    /// named by the config fields that hold it. `None` for everything
    /// that mounts nothing.
    ///
    /// One address is served by one node, so two nodes claiming
    /// addresses that overlap is a program with no answer to "which one
    /// does a call reach". The account-wide half of that question can
    /// only be answered at activation, where every project's claims are
    /// in one table. The half INSIDE one program is answerable from the
    /// source alone, and this key is what lets the compiler answer it:
    /// it says which field holds the pattern and which holds the
    /// methods, and route patterns are the language's own vocabulary
    /// (`weft_core::route`), so the check reads no node's mind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claims_route: Option<ClaimsRoute>,
    /// What each type variable this node's ports use may stand for:
    /// variable name to its BOUND (`{"T": "File"}`), so `KeepFile`'s
    /// `T` in and `T` out keeps a `Video` a `Video` and still refuses a
    /// `String`. A bound only checks, never decides: the variable is
    /// worked out from what is connected, as an unbounded one is, and
    /// the compiler then holds that answer to the bound (the other
    /// variables a bound names substituted first). A program writes the
    /// same on a group or an inline node: `Group[T: Media](file: T)`.
    // SYNC: NodeMetadata.type_params <-> packages/weft-graph/src/protocol.ts CatalogEntry.typeParams
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub type_params: std::collections::BTreeMap<String, WeftType>,
    /// The text setting whose placeholders name this node's inputs
    /// (a query's `$userId`, a template's `{{name}}`), and the pattern
    /// that finds them (its group named `name` is the input). The
    /// compiler holds the two to each other, knowing nothing of what
    /// the text is: every placeholder the setting holds (written in the
    /// braces or through `@file`) must be an input of the node, and
    /// every input it adds must appear in it. So a typo in a query's
    /// `$userid` fails the build instead of the first call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholders_are_inputs: Option<PlaceholdersAreInputs>,
}

/// Which setting holds the placeholders and how they are spelled
/// ([`NodeMetadata::placeholders_are_inputs`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceholdersAreInputs {
    /// The input holding the text (`"query"`, `"template"`).
    #[serde(rename = "in")]
    pub setting: String,
    /// A regular expression finding one placeholder, whose group named
    /// `name` is the input it stands for (`"\\$(?<name>[a-zA-Z_][a-zA-Z0-9_]*)"`).
    /// Back-references are allowed (`\\k<tag>`), so a pattern can skip a
    /// span to the closing delimiter that matches its opening one (SQL's
    /// `$tag$ ... $tag$`). A match whose `name` group took no part is
    /// text the pattern skips (a string literal, a comment).
    pub pattern: String,
}

impl PlaceholdersAreInputs {
    /// The pattern, compiled. The load refuses one that does not
    /// compile or has no `name` group, so a caller after it holds a good one.
    pub fn regex(&self) -> Result<fancy_regex::Regex, String> {
        let regex = fancy_regex::Regex::new(&self.pattern)
            .map_err(|e| format!("placeholdersAreInputs.pattern is not a regular expression: {e}"))?;
        if !regex.capture_names().any(|name| name == Some("name")) {
            return Err(format!(
                "placeholdersAreInputs.pattern `{}` has no group named `name`; that group \
                 (`(?<name>...)`) is the input a placeholder stands for",
                self.pattern
            ));
        }
        Ok(regex)
    }

    /// The input names `text`'s placeholders stand for, each once, in
    /// the order they first appear. A pattern that gives up on the text
    /// (fancy-regex's backtracking limit) is an error naming the setting.
    pub fn names_in(&self, regex: &fancy_regex::Regex, text: &str) -> Result<Vec<String>, String> {
        let mut names: Vec<String> = Vec::new();
        for captures in regex.captures_iter(text) {
            let captures = captures.map_err(|e| {
                format!("the placeholders of '{}' could not be read: {e}", self.setting)
            })?;
            if let Some(name) = captures.name("name").map(|m| m.as_str().to_string()) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        Ok(names)
    }
}

/// Which config fields carry the public address a node claims. The
/// pattern field is required; the methods field is optional, because a
/// node may serve every method (a Socket does) and then has no such
/// field at all.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimsRoute {
    /// The config field holding the route pattern (`cards/{id}`).
    #[serde(rename = "pathField")]
    pub path_field: String,
    /// The config field holding the HTTP method, when the node has
    /// one. Absent, or empty on the node in the program, means every method.
    #[serde(default, rename = "methodField", skip_serializing_if = "Option::is_none")]
    pub method_field: Option<String>,
    /// Every output port a program adds to the node reads the path
    /// capture of its name (`-> (id: String)` on `cards/{id}`). The
    /// compiler then holds each added port to a capture the written
    /// path has, typed as the text a capture is, so a port that could
    /// never fill is refused where it is written. Needs
    /// `features.canAddOutputPorts`.
    #[serde(default, rename = "capturesAreOutputs", skip_serializing_if = "std::ops::Not::not")]
    pub captures_are_outputs: bool,
}

/// The declared fire payload as ONE record type, so the whole check is
/// the ordinary type check every other value gets: `{"scheduledTime":
/// "String", "caller?": "JsonDict"}` becomes
/// `{ scheduledTime: String, caller?: JsonDict }`. The `?` rides on the
/// NAME, exactly where a record type wants it, which is why the
/// declaration can be a flat map and still say "may be absent".
///
/// `None` when nothing is declared (every non-trigger, and a trigger
/// that has not been given a shape yet): nothing to hold a payload to.
/// An unparseable entry is `Err` with the field named. The metadata
/// load calls it ([`NodeMetadata::validate_semantics`]), so a typo in a
/// type fails there; the fire check at run time ([`check_fire_payload`])
/// calls it again to build the type it holds a payload to.
pub fn fire_payload_type(
    fires_with: &std::collections::BTreeMap<String, String>,
) -> Result<Option<crate::weft_type::WeftType>, String> {
    if fires_with.is_empty() {
        return Ok(None);
    }
    for (field, ty) in fires_with {
        if let Err(why) = crate::weft_type::WeftType::try_parse(ty) {
            return Err(format!("firesWith field `{field}` is not a weft type: `{ty}` ({why})"));
        }
    }
    let fields: Vec<String> =
        fires_with.iter().map(|(field, ty)| format!("{field}: {ty}")).collect();
    let record = format!("{{ {} }}", fields.join(", "));
    crate::weft_type::WeftType::parse(&record)
        .map(Some)
        .ok_or_else(|| format!("firesWith does not describe a record: `{record}`"))
}

/// Hold one firing's wake payload to what the trigger declared. `Ok`
/// when nothing is declared (the shape is unknown, so nothing is
/// refused) and when the payload matches EXACTLY; `Err` naming the
/// field otherwise.
///
/// Called by the engine before a trigger's body runs, so BOTH ways a
/// payload arrives are covered by the one check: a listener whose
/// fields moved, and a person typing `weft run --fire`.
///
/// ## The declaration is exhaustive, and that is the point
///
/// A payload missing a field declared without `?` is refused. A payload
/// carrying ANY field the declaration does not name is refused too.
/// Both are the same rule: the node said what it wakes with, so at run
/// time it knows exactly what it has, the way a typed language knows
/// what is in a value.
///
/// This is deliberately NOT forgiving of a provider adding a field. The
/// alternative, letting an unknown field through, means a node's real
/// input quietly stops being what the node says it is, and the day that
/// matters is the day something reads a field nobody declared. A
/// provider that adds a field is a node that needs one line changed,
/// which is a small, quick, obvious edit; a node whose declaration
/// silently drifts from reality is none of those things.
///
/// So a declaration has to name EVERY field that can arrive, with `?`
/// on the ones that only sometimes do. For a trigger fed by a
/// provider's events, the authoritative list is the service's topic
/// (`AccessSpec.events.<topic>.fields`): a named event carries those
/// keys and no others, minus any the raw event did not have. The repo
/// test `weft-compiler/tests/integration/fires_with.rs` holds every shipped trigger
/// to that list, so a forgotten field fails there rather than in front
/// of a user.
///
/// There is no field the rule lets through, not one, and nothing here
/// looks at a field's NAME. `body` used to be the exception, cut out of
/// every payload before the check so a hand-fired route would not fail
/// on it. A trigger that can wake with a body now declares one, like
/// any other field, and the node reads it off its own wake.
pub fn check_fire_payload(
    fires_with: &std::collections::BTreeMap<String, String>,
    payload: Option<&serde_json::Value>,
) -> Result<(), String> {
    match fire_payload_type(fires_with)? {
        Some(shape) => check_fire_payload_against(&shape, payload),
        None => Ok(()),
    }
}

/// [`check_fire_payload`] against a shape [`fire_payload_type`] already
/// read.
pub fn check_fire_payload_against(shape: &crate::weft_type::WeftType, payload: Option<&serde_json::Value>) -> Result<(), String> {
    let payload = payload.unwrap_or(&serde_json::Value::Null);
    shape.validate_value(payload).map_err(|why| {
        format!("the fire payload does not match what this trigger wakes with: {why}. It wants {shape}")
    })
}

#[cfg(test)]
mod fire_payload_tests {
    use super::*;
    use serde_json::json;

    fn shape() -> std::collections::BTreeMap<String, String> {
        [("scheduledTime", "String"), ("caller?", "JsonDict")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_declared_shape_holds_the_payload_to_it() {
        let fires = shape();
        check_fire_payload(&fires, Some(&json!({"scheduledTime": "2026-01-01T00:00:00Z"})))
            .expect("the optional field may be absent");
        check_fire_payload(&fires, Some(&json!({"scheduledTime": "t", "caller": {"id": 1}})))
            .expect("the optional field may also be there");

        let err = check_fire_payload(&fires, Some(&json!({"caller": {}})))
            .expect_err("a missing required field is refused");
        assert!(err.contains("scheduledTime"), "{err}");

        let err = check_fire_payload(&fires, Some(&json!({"scheduledTime": 7})))
            .expect_err("a wrong type is refused");
        assert!(err.contains("scheduledTime"), "{err}");

        let err = check_fire_payload(&fires, None).expect_err("no payload at all is refused");
        assert!(err.contains("scheduledTime"), "{err}");
    }

    /// `body` is a field like any other: declared, it is accepted;
    /// undeclared, it is refused.
    ///
    /// It used to be cut out of every payload before the check, which
    /// made it the one field no trigger had to declare and therefore
    /// the one way anything could get past an exhaustive contract. This
    /// is that exception gone: a trigger that CAN carry a body says so
    /// (the Route does), and on one that cannot, the word `body` earns
    /// nothing.
    #[test]
    fn a_body_is_an_ordinary_field_on_both_sides() {
        let timer = shape();
        let err = check_fire_payload(
            &timer,
            Some(&json!({ "scheduledTime": "t", "body": { "name": "ada" } })),
        )
        .expect_err("a timer wakes with no body, so this one is a field nobody declared");
        assert!(err.contains("body"), "and it is named: {err}");

        let route: std::collections::BTreeMap<String, String> =
            [("method", "String"), ("body?", "JsonDict | String")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        check_fire_payload(&route, Some(&json!({ "method": "POST", "body": { "name": "ada" } })))
            .expect("a route declares one, so a json body is what it says it is");
        check_fire_payload(&route, Some(&json!({ "method": "POST", "body": "raw text" })))
            .expect("and a text body too");
        check_fire_payload(&route, Some(&json!({ "method": "GET" })))
            .expect("and a GET carries none, which is what the `?` says");
    }


    /// A field nobody declared is refused, at the top level and nested,
    /// and that is the contract rather than an inconvenience.
    ///
    /// The declaration is the node's promise about what it wakes with.
    /// A payload carrying something it does not name means the promise
    /// is no longer true, and the run stops there, naming the field, so
    /// the node gets the one line it needs. Letting the extra through
    /// instead would leave the node's real input quietly different from
    /// what the node says it is.
    #[test]
    fn a_field_the_trigger_never_declared_is_refused() {
        let slack: std::collections::BTreeMap<String, String> =
            [("type", "String"), ("channel", "String"), ("text?", "String")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

        check_fire_payload(
            &slack,
            Some(&json!({ "type": "message", "channel": "C123", "text": "hi" })),
        )
        .expect("exactly what was declared");
        check_fire_payload(&slack, Some(&json!({ "type": "message", "channel": "C123" })))
            .expect("the optional one may be absent");

        // One field the provider added and the node never declared.
        let err = check_fire_payload(
            &slack,
            Some(&json!({ "type": "message", "channel": "C123", "bot": "B9" })),
        )
        .expect_err("an undeclared field is refused");
        assert!(err.contains("bot"), "the message names the field to add: {err}");

        // The same one level down.
        let nested: std::collections::BTreeMap<String, String> =
            [("event".to_string(), "{ id: String }".to_string())].into_iter().collect();
        check_fire_payload(&nested, Some(&json!({ "event": { "id": "E1" } })))
            .expect("exactly what was declared");
        let err = check_fire_payload(&nested, Some(&json!({ "event": { "id": "E1", "new": 1 } })))
            .expect_err("an undeclared field nested inside is refused too");
        assert!(err.contains("new"), "{err}");
    }

    /// A node that declares nothing is held to nothing: the shape is
    /// unknown, so refusing would be inventing a contract.
    #[test]
    fn an_undeclared_trigger_refuses_nothing() {
        let none = std::collections::BTreeMap::new();
        check_fire_payload(&none, Some(&json!({"anything": true}))).expect("nothing declared");
        check_fire_payload(&none, None).expect("nothing declared");
    }

    #[test]
    fn a_type_that_does_not_parse_is_named() {
        let broken: std::collections::BTreeMap<String, String> =
            [("when".to_string(), "Strng".to_string())].into_iter().collect();
        let err = check_fire_payload(&broken, Some(&json!({"when": "x"}))).expect_err("typo");
        assert!(err.contains("`when`") && err.contains("Strng"), "{err}");
    }
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

/// Refuse the metadata keys the language dropped, with the reason and the
/// replacement, before the typed parse turns them into a bare "unknown
/// field". `deny_unknown_fields` would refuse them anyway; this names what
/// changed so an author holding an older `metadata.json` knows what to do.
/// Both producers of a node's document (the catalog load and the derive's
/// embedded parse) run it, so the message is the same at build and at run.
pub fn refuse_removed_metadata_keys(value: &serde_json::Value) -> Result<(), String> {
    let Some(obj) = value.as_object() else { return Ok(()) };
    if let Some(inputs) = obj.get("inputs").and_then(|o| o.as_array()) {
        for input in inputs {
            if input.get("requiredWhenWired").is_some() {
                let name = input.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                return Err(format!(
                    "input '{name}' sets `requiredWhenWired`, which no longer exists: an optional \
                     input is required once wired already (unwired, the node runs without it). \
                     Remove the key"
                ));
            }
            if let Some(exposure) = input.get("exposure") {
                let name = input.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                let replacement = match exposure.as_str() {
                    Some("wire") => "write `\"accepts\": [\"wire\"]` to refuse written values",
                    Some("config") => {
                        "drop it (a compiler-read port takes an inline value by a fixed rule; \
                         any other port takes both a literal and a wire), or write \
                         `\"accepts\": [\"literal\"]` to refuse wiring"
                    }
                    _ => "drop it (an input takes both a literal and a wire unless \
                          `accepts` narrows it)",
                };
                return Err(format!(
                    "input '{name}' sets `exposure`, which no longer exists: an input's drivers \
                     are `accepts` (a list of \"literal\" and/or \"wire\", both when absent); \
                     {replacement}"
                ));
            }
        }
    }
    if obj.get("features").and_then(|f| f.get("catchErrors")).is_some() {
        return Err(format!(
            "`features.catchErrors` no longer exists: every node has the `{ERROR_PORT}` \
             output, and a failure goes there when a program wires it. Remove the key."
        ));
    }
    if obj.get("features").and_then(|f| f.get("optionalCustomInputs")).is_some() {
        return Err(
            "`features.optionalCustomInputs` is `features.join` now: a node whose added inputs \
             are optional is a join (the first of several branches), and says so."
                .into(),
        );
    }
    refuse_removed_verify_keys(obj)?;
    if obj.get("features").and_then(|f| f.get("isOutputDefault")).is_some() {
        return Err(
            "`features.isOutputDefault` no longer exists: every node runs when it is reached, \
             and `weft run --target <node>` narrows a run. Remove the key."
                .into(),
        );
    }
    if let Some(outputs) = obj.get("outputs").and_then(|o| o.as_array()) {
        for output in outputs {
            if output.get("required").is_some() {
                let name = output.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                return Err(format!(
                    "output '{name}' sets `required`, which outputs no longer carry: an output \
                     port has no optionality (a firing that emits nothing on it closes it). \
                     Remove the key."
                ));
            }
        }
    }
    refuse_renamed_metadata_keys(value)
}

/// The keys a signing `verify` block (`hmac`, `signature`) had before a
/// signature and a timestamp were each a place in the request; what each
/// became is [`crate::access::events::verify_to_v2`]'s.
const REMOVED_VERIFY_KEYS: [&str; 4] = ["signatureHeader", "timestampHeader", "prefix", "packed"];

/// Refuse a `verify` block of the service's (its caller gate, or a topic's
/// push recipe) that still carries a [`REMOVED_VERIFY_KEYS`] key, showing
/// the block written the new way. A recorded recipe is upgraded on read
/// (`EVENTS_V2`); a node's own metadata is source its author edits, so it
/// is pointed at the new shape instead of meeting serde's bare "unknown
/// field".
fn refuse_removed_verify_keys(node: &serde_json::Map<String, serde_json::Value>) -> Result<(), String> {
    let Some(service) = node.get("service") else { return Ok(()) };
    let gate = service.get("verify").map(|verify| ("service.verify".to_string(), verify));
    let pushes = service.get("events").and_then(|e| e.as_object()).into_iter().flat_map(|topics| {
        topics.iter().filter_map(|(topic, spec)| {
            spec.get("webhook")?.get("verify").map(|verify| (format!("service.events.{topic}.webhook.verify"), verify))
        })
    });
    for (at, verify) in gate.into_iter().chain(pushes) {
        let old: Vec<String> =
            REMOVED_VERIFY_KEYS.iter().filter(|key| verify.get(**key).is_some()).map(|key| format!("`{key}`")).collect();
        if old.is_empty() {
            continue;
        }
        let mut upgraded = verify.clone();
        let rewrite = match crate::access::events::verify_to_v2(&mut upgraded) {
            Ok(()) => format!("written the new way it reads {upgraded}"),
            Err(why) => format!("it cannot be rewritten as it stands ({why})"),
        };
        return Err(format!(
            "`{at}` sets {}, which no longer exist: the signature and the timestamp are each a place in \
             the request now (`\"signature\": {{\"in\": \"header:<Name>\", \"pair\": \"=\", \"key\": \"v0\"}}`, \
             `\"timestamp\": {{\"in\": \"header:<Name>\"}}`); {rewrite}. \
             docs/src/connections/verifying-requests.md has every key",
            old.join(", ")
        ));
    }
    Ok(())
}

/// The `metadata.json` keys that were snake_case and are camelCase now
/// (`requires_infra` is `requiresInfra`), old spelling first. The old
/// spelling is refused, never read as an alias; this table only lets the
/// refusal name the new one instead of serde's bare "unknown field".
pub const RENAMED_METADATA_KEYS: &[(&str, &str)] = &[
    ("requires_infra", "requiresInfra"),
    ("per_instance", "perInstance"),
    ("instance_filled", "instanceFilled"),
    ("free_text", "freeText"),
    ("depends_on", "dependsOn"),
    ("mime_types", "mimeTypes"),
    ("cursor_param", "cursorParam"),
    ("cursor_path", "cursorPath"),
    ("connection_optional", "connectionOptional"),
    ("own_page", "ownPage"),
    ("all_permissions_url", "allPermissionsUrl"),
    ("own_only", "ownOnly"),
    ("token_url", "tokenUrl"),
    ("scope_delimiter", "scopeDelimiter"),
    ("auth_params", "authParams"),
    ("extra_params", "extraParams"),
    ("registration_fields", "registrationFields"),
    ("token_auth", "tokenAuth"),
    ("jwt_ttl_secs", "jwtTtlSecs"),
    ("auth_url", "authUrl"),
    ("token_path", "tokenPath"),
    ("expires_path", "expiresPath"),
    ("expect_status", "expectStatus"),
    ("access_key_id", "accessKeyId"),
    ("secret_access_key", "secretAccessKey"),
    ("consumer_key", "consumerKey"),
    ("consumer_secret", "consumerSecret"),
    ("token_secret", "tokenSecret"),
    ("when_field", "whenField"),
    ("url_from", "urlFrom"),
    ("event_path", "eventPath"),
    ("multi_account", "multiAccount"),
    ("id_path", "idPath"),
    ("route_by", "routeBy"),
    ("jwks_url", "jwksUrl"),
    ("renew_margin_secs", "renewMarginSecs"),
];

/// Refuse every metadata key still spelled the way
/// [`RENAMED_METADATA_KEYS`] says it no longer is, at any depth that holds
/// metadata keys ([`METADATA_PLACES`] says where the rest is data), naming
/// where each sits and its new spelling, all in one message, sorted.
fn refuse_renamed_metadata_keys(value: &serde_json::Value) -> Result<(), String> {
    fn walk<'a>(path: &mut Vec<&'a str>, value: &'a serde_json::Value, found: &mut Vec<String>) {
        let place = metadata_place_at(path);
        if matches!(place, Some(MetadataPlace::Data)) {
            return;
        }
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let segment = match place {
                        Some(MetadataPlace::NamesAre(..)) => "*",
                        _ => key.as_str(),
                    };
                    if segment != "*" {
                        if let Some((_, new)) = RENAMED_METADATA_KEYS.iter().find(|(old, _)| old == key) {
                            let under = if path.is_empty() { String::new() } else { format!(" (under `{}`)", path.join(".")) };
                            found.push(format!("`{key}`{under} is spelled `{new}` now"));
                        }
                    }
                    path.push(segment);
                    walk(path, child, found);
                    path.pop();
                }
            }
            serde_json::Value::Array(items) => {
                path.push("[]");
                for item in items {
                    walk(path, item, found);
                }
                path.pop();
            }
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk(&mut Vec::new(), value, &mut found);
    // In one order whatever order the document's keys come in.
    found.sort();
    if found.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{}: every metadata.json key is camelCase, and the old spelling is no longer read. Rename {}",
        found.join("; "),
        if found.len() == 1 { "it" } else { "them" }
    ))
}

/// What the value at one place in a `metadata.json` holds, when it is not
/// metadata keys all the way down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataPlace {
    /// Free data: neither its keys nor anything below them are metadata
    /// keys.
    Data,
    /// A map whose keys are NAMES of something (a type, a field, a topic)
    /// rather than metadata keys, spelled `casing`, described as `what`.
    /// Each entry's value is metadata again, at the segment `*`.
    NamesAre(crate::casing::Casing, &'static str),
}

/// Where a `metadata.json` holds something other than metadata keys. A
/// pattern is the path from the file's root, one segment per item: map
/// keys, `[]` for any list item, `*` for any one key, `**` for any run of
/// segments. Each entry says why. Written pre-split, so a walk over every
/// node discovery loads never allocates to match one. Every reader of
/// "is this a metadata key" asks here: the catalog's spelling check and
/// the refusal of a renamed key.
pub const METADATA_PLACES: &[(&[&str], MetadataPlace, &str)] = {
    use crate::casing::Casing;
    use MetadataPlace::{Data, NamesAre};
    &[
        (&["inputs", "[]", "default"], Data, "the value an input gets when nothing drives it: the node's data"),
        (&["metadataPorts", "[]", "value"], Data, "the value a metadata port holds, written by hand: the node's data"),
        (
            &["validate", "**", "equals"],
            Data,
            "the JSON value a `config_equals` rule compares a setting against: the setting's data",
        ),
        (
            &["inputs", "[]", "widget", "sources", "[]", "where"],
            Data,
            "dotted paths into the PROVIDER's list items (`can_do_text_to_speech`): the provider's names",
        ),
        (&["service", "**", "body"], Data, "the request body sent to the provider as written: the provider's names"),
        (&["service", "**", "form"], Data, "a form body sent to the provider as written: the provider's parameter names"),
        (&["service", "ownPage", "mint", "payload"], Data, "the app-creation payload sent to the provider as written"),
        (
            &["service", "acquisition", "authParams"],
            Data,
            "extra consent-URL parameters (`access_type`): the OAuth provider's parameter names",
        ),
        (&["service", "acquisition", "extraParams"], Data, "extra token-request parameters: the provider's names"),
        (&["service", "acquisition", "claims"], Data, "JWT claims (`iss`, `sub`): the JWT standard's names"),
        (
            &["accessApps", "*"],
            Data,
            "an OAuth app's registration as the token exchange sends it: `client_id` and `client_secret` are \
             OAuth's parameter names, and the extras are named by the service's `registrationFields`, \
             which are checked where the service declares them",
        ),
        // A service name is a value of a fixed set (the services weft
        // knows): a node names it as a value (`"service": "google"`), so it
        // takes the snake_case every fixed-set value takes.
        (&["accessApps"], NamesAre(Casing::Snake, "service name"), ""),
        (&["types"], NamesAre(Casing::Pascal, "declared type"), ""),
        (&["firesWith"], NamesAre(Casing::Camel, "fire payload field"), ""),
        (&["typeParams"], NamesAre(Casing::Pascal, "type variable"), ""),
        (&["service", "events", "*", "fields"], NamesAre(Casing::Camel, "event field"), ""),
        // A topic is a value of a fixed set (the service's event kinds): a
        // trigger names it as a value (`"drive_changes"`), so it takes the
        // snake_case every fixed-set value takes.
        (&["service", "events"], NamesAre(Casing::Snake, "event topic"), ""),
    ]
};

fn place_matches(pattern: &[&str], path: &[&str]) -> bool {
    match (pattern.split_first(), path.split_first()) {
        (None, None) => true,
        (Some((&"**", rest)), _) => place_matches(rest, path) || (!path.is_empty() && place_matches(pattern, &path[1..])),
        (Some((p, rest)), Some((s, tail))) => (*p == "*" || p == s) && place_matches(rest, tail),
        _ => false,
    }
}

/// What [`METADATA_PLACES`] says the value at `path` holds; `None` is
/// metadata keys.
pub fn metadata_place_at(path: &[&str]) -> Option<MetadataPlace> {
    METADATA_PLACES.iter().find_map(|(pattern, place, _)| place_matches(pattern, path).then_some(*place))
}

impl NodeMetadata {
    /// Whether this node consumes a stream: any input whose declared
    /// type is a `Generator` (aliases peel). THE one spelling of the
    /// question for manifest-driven callers (both node-test rigs), so
    /// the definition cannot drift between them.
    pub fn has_generator_input(&self) -> bool {
        self.inputs.iter().any(|p| p.input_type.as_generator().is_some())
    }

    /// The connection-picker input: the one input carrying the `access`
    /// widget. THE one spelling of the lookup for every caller (the
    /// validator, the synthesized connection rule, `weft connect`), so
    /// none re-derives it. Metadata load enforces exactly one on any
    /// node declaring a `service` recipe; `None` means the node
    /// declares no picker at all.
    pub fn access_input(&self) -> Option<&InputSpec> {
        self.inputs.iter().find(|i| matches!(i.effective_widget(), Widget::Access { .. }))
    }

    /// Whether a node of this type may be marked `@per_instance`: it runs a
    /// container of its own (`requiresInfra`), and each instance gets one.
    /// Anything an instance PROVIDES (its connection, its sheet) is a
    /// field written `@instance_filled` instead. The one definition the
    /// compiler's check and the editor's right-click toggle both read.
    // SYNC: per_instance_eligible <-> packages/weft-graph/src/webview/lib/utils/node-roles.ts canBePerInstance
    pub fn per_instance_eligible(&self) -> bool {
        self.requires_infra
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
        refuse_removed_metadata_keys(&value)
            .unwrap_or_else(|e| panic!("{site}: metadata.json: {e}"));
        let mut metadata: Self = serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("{site}: metadata.json does not fit NodeMetadata: {e}"));
        metadata
            .add_language_ports()
            .unwrap_or_else(|e| panic!("{site}: metadata.json: {e}"));
        metadata
    }

    /// Give a trigger the settings the LANGUAGE owns, so no node
    /// declares them and the ctx reads them when the node registers its
    /// signal. Every trigger is an entry the dispatcher limits, so every
    /// trigger gets the per-minute and at-once limits; one somebody
    /// outside calls ([`NodeFeatures::has_outside_caller`]) also gets the
    /// per-caller limit, the only one that needs a caller to count
    /// ([`crate::signal::EntryLimits::node_inputs`]). Every trigger also
    /// gets how its runs are kept, and one that holds a caller on the line
    /// (`features.liveConnection`) whether its run may outlive that caller
    /// ([`crate::run_settings::RunSettings::node_inputs`]). A trigger that
    /// declares one of the names it receives is refused, so each setting
    /// has exactly one spelling; a node that receives none of them may
    /// use those names for its own inputs. Every node gets the
    /// [`ERROR_PORT`] output the runtime fills with its failure, and is
    /// refused when it declares that output itself. Run once, on the authored document,
    /// by both loaders (the catalog's and the derive's `parse_embedded`),
    /// so the editor, the compiler and the worker all see the same
    /// ports.
    pub fn add_language_ports(&mut self) -> Result<(), String> {
        let features = &self.features;
        if (features.live_connection.is_some() || features.called_from_outside) && !features.is_trigger {
            return Err("features.liveConnection and features.calledFromOutside are only for a trigger (features.isTrigger)".into());
        }
        if features.live_connection.is_some() && features.called_from_outside {
            return Err("features.liveConnection already means somebody outside calls the trigger; remove features.calledFromOutside".into());
        }
        if features.live_connection.is_some() && features.journal_settings {
            return Err("features.liveConnection already gives the trigger the journal settings; remove features.journalSettings".into());
        }
        let mut added: Vec<InputSpec> = Vec::new();
        if features.is_trigger {
            added.extend(crate::signal::EntryLimits::node_inputs(features.has_outside_caller()));
            added.extend(crate::run_settings::RunSettings::node_inputs(features.live_connection.is_some()));
        }
        if features.live_connection.is_some() || features.journal_settings {
            added.extend(crate::stream_journal::JournalPolicy::node_inputs());
        }
        for name in added.iter().map(|i| &i.name) {
            if self.inputs.iter().any(|i| i.name == *name) {
                return Err(format!(
                    "input '{name}' is a setting the language gives every node that \
                     needs it; remove it from this node's inputs"
                ));
            }
        }
        self.inputs.extend(added);
        let port = ERROR_PORT;
        if self.outputs.iter().any(|o| o.name == port) {
            return Err(format!(
                "output '{port}' is the port the language gives every node for its \
                 failures; remove it from this node's outputs"
            ));
        }
        self.outputs.push(error_output());
        Ok(())
    }

    /// The node's declared bounds, as the type parameters a program's
    /// node of this type starts with.
    pub fn type_param_list(&self) -> Vec<crate::weft_type::TypeParam> {
        self.type_params
            .iter()
            .map(|(name, bound)| crate::weft_type::TypeParam::new(name.clone(), bound.clone()))
            .collect()
    }

    /// Whether a program may declare `declared` on this node's output
    /// `port`, whose type in the node as built is `current` (the
    /// metadata's, or one its config derived). `Err` finishes the
    /// sentence "the port is declared X, ...". A type variable in the
    /// output is pinned by a declaration that fits it. A narrowable
    /// output takes anything that fits its bound. Every other output is
    /// the node's contract and is only ever restated as it is: the node
    /// emits what its type says, nothing narrower. The compiler's merge
    /// and the node-test rig both ask here.
    // SYNC: output_declaration_fits <-> packages/weft-graph/src/webview/lib/projection/header-ports.ts declarationRule
    pub fn output_declaration_fits(&self, port: &str, current: &WeftType, declared: &WeftType) -> Result<(), String> {
        if current.contains_unresolved_leaf() {
            return if WeftType::is_compatible(declared, current) {
                Ok(())
            } else {
                Err(format!("which does not fit in its type {current}"))
            };
        }
        match self.outputs.iter().find(|o| o.name == port).and_then(OutputSpec::narrowing_bound) {
            Some(bound) if WeftType::fits_within(declared, bound) => Ok(()),
            Some(bound) => Err(format!("which does not fit in {bound}, the widest type the program may declare there")),
            None if declared == current => Ok(()),
            None => Err(format!(
                "but {} always gives it {current}: only an output its node marks narrowable takes \
                 the type the program declares",
                self.node_type
            )),
        }
    }

    /// The type rules of a node's ports: a narrowable output's bound is a
    /// real type its own type fits in, and every `typeParams` entry bounds
    /// a variable a port uses, without a cycle.
    fn check_port_types(&self) -> Result<(), String> {
        let mut used = std::collections::BTreeSet::new();
        for input in &self.inputs {
            input.input_type.collect_type_vars(&mut used);
        }
        if let Some(ty) = &self.features.custom_input_type {
            ty.collect_type_vars(&mut used);
        }
        for output in &self.outputs {
            output.port_type.collect_type_vars(&mut used);
            match &output.narrowable {
                None => {}
                Some(narrowable) => {
                    if output.port_type.contains_unresolved_leaf() {
                        return Err(format!(
                            "output '{}' is narrowable, and its type {} holds a type variable, \
                             which the program already pins by declaring the port; drop one",
                            output.name, output.port_type
                        ));
                    }
                    if let Narrowable::Bound(bound) = narrowable {
                        if bound.contains_unresolved_leaf() {
                            return Err(format!(
                                "output '{}' narrows within {bound}, which is not a concrete type",
                                output.name
                            ));
                        }
                        if !WeftType::fits_within(&output.port_type, bound) {
                            return Err(format!(
                                "output '{}' is typed {} but narrows within {bound}, which that \
                                 type does not fit in",
                                output.name, output.port_type
                            ));
                        }
                    }
                }
            }
        }
        for rule in &self.validate {
            self.check_condition_ports(&rule.when)?;
        }
        let params = self.type_param_list();
        crate::weft_type::TypeParam::check_declarations(&params)
            .and_then(|()| crate::weft_type::TypeParam::check_used(&params, &used))
            .map_err(|why| format!("typeParams: {why}"))
    }

    /// Every port a validation rule's condition names is one of this
    /// node's (or one a program may add, on a node that takes added
    /// ports): a misspelled port reads as never wired, and under a `not`
    /// the rule then fires on every program. For the same reason a rule
    /// asking about added ports is on a node that takes them.
    fn check_condition_ports(&self, condition: &Condition) -> Result<(), String> {
        let input = |port: &str| {
            if self.features.can_add_input_ports || self.inputs.iter().any(|i| i.name == port) {
                Ok(())
            } else {
                Err(format!("a validate rule names input '{port}', which this node does not have"))
            }
        };
        let output = |port: &str| {
            if self.features.can_add_output_ports || self.outputs.iter().any(|o| o.name == port) {
                Ok(())
            } else {
                Err(format!("a validate rule names output '{port}', which this node does not have"))
            }
        };
        match condition {
            Condition::InputSatisfied { port }
            | Condition::InputWired { port }
            | Condition::InputSourceType { port, .. }
            | Condition::InputNames { port, .. } => input(port),
            Condition::OutputWired { port } | Condition::OutputTypeFits { port, .. } => output(port),
            Condition::All { of } | Condition::Any { of } => {
                of.iter().try_for_each(|c| self.check_condition_ports(c))
            }
            Condition::Not { of } => self.check_condition_ports(of),
            Condition::CustomInputsDeclared {} | Condition::CustomInputsFit { .. } if !self.features.can_add_input_ports => {
                Err("a validate rule asks for added inputs, and this node takes none (`canAddInputPorts`)".into())
            }
            Condition::CustomOutputsDeclared {} if !self.features.can_add_output_ports => {
                Err("a validate rule asks for added outputs, and this node takes none (`canAddOutputPorts`)".into())
            }
            Condition::ConfigPresent { .. }
            | Condition::ConfigNonempty { .. }
            | Condition::ConfigEquals { .. }
            | Condition::ConfigInSet { .. }
            | Condition::ConfigMatches { .. }
            | Condition::CustomInputsDeclared {}
            | Condition::CustomInputsFit { .. }
            | Condition::CustomOutputsDeclared {}
            | Condition::RunReaches { .. }
            | Condition::DownstreamOf { .. }
            | Condition::PerInstance {} => Ok(()),
        }
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
        // A fire payload is a trigger's, and its types are read like any
        // port's: a typo fails the load, never the first firing.
        if !self.fires_with.is_empty() && !self.features.is_trigger {
            return Err("firesWith describes what a trigger fires with; only a trigger (features.isTrigger) declares it".into());
        }
        fire_payload_type(&self.fires_with)?;
        if !self.requires_infra {
            if let Some(output) = self.outputs.iter().find(|o| o.baked) {
                return Err(format!(
                    "output '{}' is baked, and only an infra node's outputs are: they are made when its infra is applied",
                    output.name
                ));
            }
        }
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
        // A metadata port's value is written here and wires carry it into
        // the inputs they point at, before anything runs: it has to be
        // there, fit its type, and its name has to be one `node.x` reads
        // unambiguously.
        for (at, port) in self.metadata_ports.iter().enumerate() {
            let name = &port.name;
            if self.inputs.iter().any(|i| i.name == *name)
                || self.outputs.iter().any(|o| o.name == *name)
                || self.metadata_ports[..at].iter().any(|m| m.name == *name)
                || crate::run::skip::is_gate_port(name)
                || name == ERROR_PORT
            {
                return Err(format!(
                    "metadata port '{name}' has the name of another port of this node: inputs, outputs and \
                     metadata ports share one set of names"
                ));
            }
            if port.span.is_some() || port.declared_type.is_some() || port.written.is_some() {
                return Err(format!(
                    "metadata port '{name}': `span`, `declaredType` and `written` say how a program wrote a \
                     group's metadata port, and a node type writes none of them; remove them"
                ));
            }
            let mut vars = std::collections::BTreeSet::new();
            port.port_type.collect_type_vars(&mut vars);
            if !vars.is_empty() {
                return Err(format!(
                    "metadata port '{name}' needs a concrete type: its value is written here, and nothing \
                     wired can tell what a type variable holds"
                ));
            }
            let Some(value) = &port.value else {
                return Err(format!("metadata port '{name}' has no `value`: a node type writes it here, by hand"));
            };
            port.port_type.validate_value(value).map_err(|why| {
                format!("metadata port '{name}': its value does not fit its type {}: {why}", port.port_type)
            })?;
        }
        if self.features.pairs_added_ports && !(self.features.can_add_input_ports && self.features.can_add_output_ports) {
            return Err(
                "pairsAddedPorts pairs the inputs a program adds with outputs of the same name: declare \
                 canAddInputPorts and canAddOutputPorts beside it"
                    .into(),
            );
        }
        if self.features.custom_output_type.is_some() && !self.features.can_add_output_ports {
            return Err("customOutputType types the outputs a program adds: declare canAddOutputPorts beside it".into());
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
            if crate::run::skip::is_gate_port(&input.name) {
                return Err(format!(
                    "input '{}' is the language's own port (it decides whether a node runs); a node type cannot declare it",
                    input.name
                ));
            }
            // The two ports whose drivers are not the author's to pick:
            // a compiler-read port takes an inline value only (a list
            // there would loosen a rule the compiler needs), and a live
            // handle can never be written down. The one list each may
            // carry is the fixed rule's own, which is what `resolved()`
            // stamps and a re-parse of a shipped document hands back.
            if let Some(accepts) = input.accepts {
                if self.is_compiler_read(&input.name) && accepts != Accepts::literal_only() {
                    return Err(format!(
                        "input '{}' is read by the compiler to build the node (a `portsFromConfig` \
                         list or the access picker), so it takes an inline value only; remove \
                         `accepts`",
                        input.name
                    ));
                }
                if accepts.literal && Accepts::type_is_wire_only(&input.input_type) {
                    return Err(format!(
                        "input '{}' is a {} port, a value no human can write, so `accepts` \
                         may only list \"wire\"",
                        input.name, input.input_type
                    ));
                }
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
                Widget::Select { options, .. } | Widget::Multiselect { options, .. }
                    if options.is_empty() =>
                {
                    return Err(format!(
                        "input '{}': a select/multiselect widget must declare a non-empty \
                         `options` list",
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
                    validate_resource_sources(&format!("input '{}'", input.name), &sources)?;
                    for parent in depends_on {
                        if !self.inputs.iter().any(|i| i.name == *parent) {
                            return Err(format!(
                                "input '{}': remote_select dependsOn '{}', which is not an \
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
            // mismatch is rejected here. Unresolved types (a TypeVar) pass:
            // is_compatible is permissive there and
            // the resolved instance re-derives its widget.
            let widget_value_type = match input.effective_widget() {
                Widget::Number { .. } => Some(WeftType::primitive(WeftPrimitive::Number)),
                Widget::Checkbox => Some(WeftType::primitive(WeftPrimitive::Boolean)),
                // A JSON code editor edits a structured value as JSON text,
                // the same way textarea does, so it suits any structural
                // type; every other language edits a string.
                Widget::Code { language: CodeLanguage::Json } => None,
                Widget::Text
                | Widget::Password
                | Widget::Code { .. }
                | Widget::Datetime
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
                // A text_list edits a list of short strings.
                Widget::TextList => {
                    Some(WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String))))
                }
                // textarea is the GENERIC value editor (the derived
                // default for structural types: the value is edited as
                // JSON text), so it constrains nothing. entry_list edits
                // the config-entry list, shape-checked with the
                // portsFromConfig spec; file_drop is held to a type a
                // file control can serve, just below.
                Widget::Textarea | Widget::EntryList | Widget::FileDrop { .. } => None,
            };
            if matches!(input.effective_widget(), Widget::FileDrop { .. })
                && !input.input_type.is_unresolved()
                && input.input_type.file_control().is_none()
            {
                return Err(format!(
                    "input '{}': a file_drop widget picks files, which type {} cannot hold",
                    input.name, input.input_type
                ));
            }
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
            // A widget nothing could satisfy is caught whether or not
            // the input has a default: the metadata is wrong either way,
            // and every other path (the compiler's literal check, the
            // runtime's) would otherwise be the one to report it.
            if let Err(why) = input.effective_widget().check_declaration() {
                return Err(format!("input '{}': {why}", input.name));
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
                // The default is the value that arrives when nothing else
                // does, so it is held to the widget's domain through the
                // same rule as any other value.
                if let Err(why) = input.effective_widget().check_value(default) {
                    return Err(format!("input '{}': its default {why}", input.name));
                }
            }
        }
        if let Some(ports) = &self.ports_from_config {
            // The list a node derives ports from lives under a key it
            // DECLARES, so that key is an ordinary config input like
            // any other: the editor edits it, and validate's
            // undeclared-config-key check needs no exemption for it.
            let Some(list_input) = self.inputs.iter().find(|i| i.name == ports.field) else {
                return Err(format!(
                    "portsFromConfig reads config key '{}', which this node does not \
                     declare as an input",
                    ports.field
                ));
            };
            // The ports derive from what the SOURCE writes, on the
            // compiler and in the editor alike. A default on the list
            // would give the node entries (and emissions) the compiler
            // never declared ports for, so the list has no default.
            if list_input.default.is_some() {
                return Err(format!(
                    "portsFromConfig reads '{}', which declares a default; the ports come \
                     from what the source writes, so the list takes none",
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
            // No specs means no entry could ever add a port, and the
            // editor would render an entry form with an empty kind
            // dropdown; refuse the declaration instead.
            if ports.specs.is_empty() {
                return Err(
                    "portsFromConfig: declares no specs, so no entry could ever add a port"
                        .into(),
                );
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
                let mut field_keys: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for field in &spec.fields {
                    if let Some(problem) = field.declaration_problem() {
                        return Err(format!("portsFromConfig: spec '{}' {problem}", spec.kind));
                    }
                    if let Some(Widget::RemoteSelect { sources, .. }) = &field.widget {
                        validate_resource_sources(
                            &format!("portsFromConfig: spec '{}' field '{}'", spec.kind, field.key),
                            sources,
                        )?;
                    }
                    // `kind` and the key field are written into every
                    // entry FIRST; a spec field reusing either name
                    // would silently overwrite the entry's kind or its
                    // validated port name with a raw draft value.
                    if field.key == "kind" || field.key == spec.key_field {
                        return Err(format!(
                            "portsFromConfig: spec '{}' declares a field named '{}', which \
                             is the key holding the entry's kind or its port name",
                            spec.kind, field.key
                        ));
                    }
                    // Two fields on one entry key is the same silent
                    // overwrite between siblings: last one wins in the
                    // entry map and the editor draws two controls bound
                    // to one value.
                    if !field_keys.insert(field.key.as_str()) {
                        return Err(format!(
                            "portsFromConfig: spec '{}' declares the field '{}' twice",
                            spec.kind, field.key
                        ));
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
        if let Some(claims) = &self.claims_route {
            // Same reason as `matchInput` above: the compiler reads the
            // node's address out of the input this NAMES, so a name that
            // is not an input resolves to nothing, and the route checks
            // (is the pattern servable, does it collide with another)
            // quietly do nothing for every instance of this node type,
            // for ever, with no sign anywhere. Refused at the one moment
            // somebody could notice, which is loading the node.
            if !self.inputs.iter().any(|i| i.name == claims.path_field) {
                return Err(format!(
                    "claimsRoute reads the address from input '{}', which this node does \
                     not declare",
                    claims.path_field
                ));
            }
            if let Some(field) = &claims.method_field {
                if !self.inputs.iter().any(|i| i.name == *field) {
                    return Err(format!(
                        "claimsRoute reads the method from input '{field}', which this \
                         node does not declare"
                    ));
                }
            }
            if claims.captures_are_outputs && !self.features.can_add_output_ports {
                return Err(
                    "claimsRoute.capturesAreOutputs reads the path's captures onto the output \
                     ports a program adds, so the node needs features.canAddOutputPorts"
                        .into(),
                );
            }
        }
        self.check_port_types()?;
        if let Some(placeholders) = &self.placeholders_are_inputs {
            placeholders.regex()?;
            let Some(setting) = self.inputs.iter().find(|i| i.name == placeholders.setting) else {
                return Err(format!(
                    "placeholdersAreInputs reads the placeholders from input '{}', which this node \
                     does not declare",
                    placeholders.setting
                ));
            };
            if !WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &setting.input_type) {
                return Err(format!(
                    "placeholdersAreInputs reads the placeholders from input '{}', which holds {} \
                     rather than text",
                    setting.name, setting.input_type
                ));
            }
            if !self.features.can_add_input_ports {
                return Err(
                    "placeholdersAreInputs matches placeholders against the inputs a program \
                     adds, so the node needs features.canAddInputPorts"
                        .into(),
                );
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
        Ok(())
    }

    /// Is `name` a port the compiler reads to BUILD the node: the list its
    /// ports come from (`portsFromConfig`), or the access picker, whose
    /// id the compiler stamps with the service. Such a port takes an
    /// inline typed value only (no wire, no `@file`, no `@asset`): a port
    /// that shapes a node has to be readable in the source without
    /// following anything. The one definition every reader of that rule
    /// (the loader, enrich, validate) consults.
    pub fn is_compiler_read(&self, name: &str) -> bool {
        if self.ports_from_config.as_ref().is_some_and(|p| p.field == name) {
            return true;
        }
        self.inputs
            .iter()
            .find(|i| i.name == name)
            .is_some_and(|i| matches!(i.effective_widget(), Widget::Access { .. }))
    }

    /// A copy with every input's `accepts` and `widget`, and every config
    /// entry field's `widget`, RESOLVED to their effective values (the
    /// author's explicit choice, else the type-derived default). This is
    /// what ships on the wire to the editor, so the editor never
    /// re-derives either; resolving is idempotent (a resolved metadata
    /// re-parses to itself, the compiler-read ports included: their
    /// resolved list is the fixed rule's, and the loader lets a resolved
    /// document through because `resolved()` marks nothing the author
    /// could not have meant).
    pub fn resolved(&self) -> Self {
        let mut out = self.clone();
        let compiler_read: Vec<bool> =
            self.inputs.iter().map(|i| self.is_compiler_read(&i.name)).collect();
        for (input, compiler_read) in out.inputs.iter_mut().zip(compiler_read) {
            input.accepts = Some(input.effective_accepts(compiler_read));
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

    /// The compact wiring view of this node, as JSON: the resolved
    /// metadata with everything a WIRING reader never needs stripped,
    /// so `weft describe-nodes --compact` can hand an AI the whole
    /// node vocabulary for a fraction of the tokens the full files
    /// cost. What survives is what decides how the node connects:
    /// ports with types, accepts and widgets (an access widget
    /// carrying its stamped service and `optional`), features,
    /// config-derived port shapes, declared types, validation rules.
    /// What goes is presentation (an input's `label`/`placeholder`,
    /// an entry kind's dropdown label and render hint, the palette
    /// dressing), authoring machinery (the `service` connect recipe,
    /// `accessApps`, `images`, `display`), and noise nulls (an
    /// `Option` serialized as null, on port entries and in widgets,
    /// wherever a widget sits, which only ever mean "absent"). Data
    /// is never touched: a remote-select source's `label` path, a
    /// literal's own fields, and the nulls inside a `default`, a
    /// rule's `equals` or a list source's `where` ride through
    /// untouched. Node authors keep
    /// reading the full
    /// `metadata.json`: this is the view for writing programs WITH a
    /// node, not for writing the node.
    pub fn compact_json(&self) -> serde_json::Value {
        let mut resolved = self.resolved();
        // Presentation is removed at its TYPED sites, never by key
        // name: a `label`-shaped key also names real data (a
        // remote-select source's display path, a literal's own
        // fields), and a name sweep over the JSON cannot tell the two
        // apart. An input's `label`/`placeholder` name it to a human,
        // so they are nulled here; the entry-kind presentation inside
        // `portsFromConfig` is swept by key after serialization, a
        // sweep that must STOP at widgets (see
        // `strip_ports_presentation`): a spec field's widget is a full
        // `Widget`, and a remote-select source's `label` under it is
        // a data path, not the editor's name for the entry.
        for input in &mut resolved.inputs {
            input.label = None;
            input.placeholder = None;
        }
        // The access widget's `service`/`optional` are stamped by the
        // compiler at enrich time; stamp them here from the node's
        // own recipe so the compact view answers "can this node run
        // without a connection picked" without the recipe it drops.
        // This covers every access widget that can exist: load
        // validation ties an access widget to a node carrying the
        // recipe (a widget with no recipe, or two widgets, is a load
        // error), so there is no second stamp site to miss.
        if let Some(recipe) = &resolved.service {
            for input in &mut resolved.inputs {
                if let Some(widget) = &mut input.widget {
                    recipe.stamp_onto(widget);
                }
            }
        }
        let mut value = serde_json::to_value(&resolved)
            .expect("NodeMetadata serializes to JSON");
        compact_wiring_view(&mut value);
        value
    }
}

/// Top-level metadata keys the compact wiring view keeps (see
/// [`NodeMetadata::compact_json`]). Everything a wiring reader needs:
/// identity, description, ports, features, port-deriving config
/// shapes, validation rules, declared types, and what service an
/// infra node publishes.
///
/// An allow-list on purpose. A top-level key is schema growth (a new
/// concept the whole toolchain learns), so a new one lands
/// UNCLASSIFIED and is dropped from the compact view until somebody
/// decides whether a wiring reader needs it;
/// `compact_view_tests::every_metadata_top_level_key_is_classified`
/// and the stdlib census in weft-catalog hold the schema to that
/// decision. Below the top level, DATA rides through untouched: an
/// input's presentation is removed at its typed site in
/// `compact_json`, the one key sweep left runs only inside the closed
/// `portsFromConfig` subtree and stops at widgets, and noise nulls
/// are dropped only from port entries and from widgets (input
/// widgets and the spec-field widgets under `portsFromConfig`).
/// Free-form values (a `default`'s contents, a rule's `equals`
/// payload, a list source's `where`) keep every key name and every
/// null they hold.
pub const COMPACT_KEEP_TOP_LEVEL: [&str; 13] = [
    "type",
    "description",
    "inputs",
    "outputs",
    // Ports a wire leaves like an output's, the value written here.
    "metadataPorts",
    "requiresInfra",
    "features",
    "validate",
    "portsFromConfig",
    "publishes",
    "types",
    // What a trigger wakes with. Not a port and not wirable, but it is
    // the only place `weft run --fire`'s payload shape is written
    // down, and a wiring view that hides it leaves a reader guessing
    // at the one thing they have to type by hand.
    "firesWith",
    // What each type variable of the ports may stand for: what a wire
    // into a `T` port has to be.
    "typeParams",
];

/// Top-level metadata keys the compact wiring view drops by NOT being
/// in [`COMPACT_KEEP_TOP_LEVEL`]: the palette dressing (`label`,
/// `tags`, `icon`, `color`), the connect recipe (`service`), the
/// declared OAuth apps (`accessApps`), the image build list
/// (`images`), and the graph-body rendering (`display`).
///
/// This list does no filtering (non-membership in the keep-list does
/// that); it exists so every top-level key has exactly one declared
/// disposition, keep or drop, which is what the classification tests
/// hold to account when the schema grows.
pub const COMPACT_DROP_TOP_LEVEL: [&str; 10] = [
    "label",
    "tags",
    "icon",
    "color",
    "images",
    "display",
    "service",
    "accessApps",
    // Which config fields hold the address this node claims. The
    // compiler's overlap check reads it; nobody writing weft does.
    "claimsRoute",
    // The pattern the compiler holds a setting's placeholders to the
    // inputs with. The description says the rule in words; the regex is
    // the compiler's.
    "placeholdersAreInputs",
];

/// Presentation keys stripped inside the `portsFromConfig` subtree of
/// the compact wiring view: an entry kind's dropdown `label` and its
/// `render` hint. The sweep stops at any `widget` value (see
/// `strip_ports_presentation`): the subtree is closed structs, but a
/// spec field's widget can be a remote_select whose sources carry
/// `label` DATA paths.
const PORTS_PRESENTATION_KEYS: [&str; 2] = ["label", "render"];

/// Filter one RESOLVED metadata serialization (input presentation
/// already nulled at its typed sites, see [`NodeMetadata::compact_json`])
/// down to the compact wiring view: keep only
/// [`COMPACT_KEEP_TOP_LEVEL`] at the top level, sweep
/// [`PORTS_PRESENTATION_KEYS`] through the `portsFromConfig` subtree
/// (stopping at widgets), and drop noise nulls from the port entries
/// and from every widget, wherever one sits. Everything else rides
/// through untouched, INCLUDING free-form data: a `default`'s
/// contents and a rule's `equals` payload keep every key name and
/// every null they hold, because those are values the runtime would
/// really supply or compare, not schema fields.
fn compact_wiring_view(value: &mut serde_json::Value) {
    let Some(map) = value.as_object_mut() else {
        return;
    };
    map.retain(|key, _| COMPACT_KEEP_TOP_LEVEL.contains(&key.as_str()));
    if let Some(ports) = map.get_mut("portsFromConfig") {
        strip_ports_presentation(ports);
        strip_widget_noise(ports);
    }
    for entry_key in ["inputs", "outputs"] {
        if let Some(entries) = map.get_mut(entry_key) {
            for entry in entries.as_array_mut().into_iter().flatten() {
                strip_noise_nulls(entry);
            }
        }
    }
    // The long-list cut runs at the TYPED sites only, like the other
    // presentation passes: each input's own widget, and the widgets
    // under `portsFromConfig` (the one subtree where a `widget` key is
    // always a Widget). A `default`, a rule's `equals` payload or a
    // declared type is free-form data and is never walked.
    if let Some(ports) = map.get_mut("portsFromConfig") {
        elide_long_option_lists(ports);
    }
    if let Some(entries) = map.get_mut("inputs") {
        for entry in entries.as_array_mut().into_iter().flatten() {
            if let Some(widget) = entry.get_mut("widget") {
                elide_long_options(widget);
            }
        }
    }
}

/// How many options a refusal names before it says how many more there
/// are: a time zone picker's six hundred would bury the message.
const REFUSAL_OPTIONS_SHOWN: usize = 12;

/// Whether `picked` is one of a select's `options`, and if not, a refusal
/// naming the ones it may be.
fn check_option(options: &[String], picked: &str) -> Result<(), String> {
    if options.iter().any(|o| o == picked) {
        return Ok(());
    }
    let mut allowed: Vec<String> = options.iter().take(REFUSAL_OPTIONS_SHOWN).map(|o| format!("'{o}'")).collect();
    if options.len() > REFUSAL_OPTIONS_SHOWN {
        allowed.push(format!("and {} more", options.len() - REFUSAL_OPTIONS_SHOWN));
    }
    Err(format!("'{picked}' is not one of the options; give one of {}", allowed.join(", ")))
}

/// How many of a select's options the compact view shows before it
/// says how many more there are.
const COMPACT_OPTIONS_SHOWN: usize = 8;

/// Cut a `select` / `multiselect` widget's options list past
/// [`COMPACT_OPTIONS_SHOWN`] entries to that many plus a `moreOptions`
/// count. The compact view is read to WIRE a node, and eight zones say
/// "an IANA time zone" as well as six hundred do (Cron's timezone
/// picker alone was 11 KB of the view); the full list is on the node's
/// plain `describe-nodes --node <Type>` output. Anything that is not a
/// select is left untouched.
fn elide_long_options(widget: &mut serde_json::Value) {
    let Some(widget) = widget.as_object_mut() else { return };
    if !matches!(widget.get("kind").and_then(|k| k.as_str()), Some("select" | "multiselect")) {
        return;
    }
    let hidden = match widget.get_mut("options") {
        Some(serde_json::Value::Array(options)) if options.len() > COMPACT_OPTIONS_SHOWN => {
            let hidden = options.len() - COMPACT_OPTIONS_SHOWN;
            options.truncate(COMPACT_OPTIONS_SHOWN);
            Some(hidden)
        }
        _ => None,
    };
    if let Some(hidden) = hidden {
        widget.insert("moreOptions".into(), serde_json::Value::from(hidden));
    }
}

/// [`elide_long_options`] on every `widget` under the `portsFromConfig`
/// subtree, the same walk `strip_widget_noise` makes and safe for the
/// same reason: that subtree holds no free-form value a `widget` key of
/// the user's own could hide in.
fn elide_long_option_lists(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(widget) = map.get_mut("widget") {
                elide_long_options(widget);
            }
            for inner in map.values_mut() {
                elide_long_option_lists(inner);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                elide_long_option_lists(item);
            }
        }
        _ => {}
    }
}

/// [`PORTS_PRESENTATION_KEYS`] out at every depth of the
/// `portsFromConfig` subtree, without descending into `widget`
/// values: those are full [`Widget`]s, and a remote-select source's
/// `label` is a data path, not the editor's name for an entry.
fn strip_ports_presentation(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|key, _| !PORTS_PRESENTATION_KEYS.contains(&key.as_str()));
            for (key, inner) in map.iter_mut() {
                if key.as_str() != "widget" {
                    strip_ports_presentation(inner);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_ports_presentation(item);
            }
        }
        _ => {}
    }
}

/// Null-valued keys out of every `widget` value under the
/// `portsFromConfig` subtree: the spec fields' widgets are the same
/// closed [`Widget`] family as input widgets (`resolved()` fills
/// them, and `Number` serializes its absent knobs as null), so the
/// same noise rule applies. Hunting by key is safe ONLY inside this
/// subtree, because it holds no free-form values a `widget` key of
/// the user's own could hide in.
fn strip_widget_noise(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(widget) = map.get_mut("widget") {
                strip_nulls(widget);
            }
            for inner in map.values_mut() {
                strip_widget_noise(inner);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_widget_noise(item);
            }
        }
        _ => {}
    }
}

/// Null-valued keys out of ONE port entry's own fields, and out of
/// its widget at every depth. An `Option` serialized as null only
/// ever means "absent" (`"default": null` IS no default; serde's
/// `Option` already conflates the two on the way in), so on the
/// entry's own typed fields a null is noise. The sweep deliberately
/// stops there: a `default` that IS present keeps its value whole
/// (nulls inside it are data the runtime would supply), and the
/// widget is a closed struct family where every null is an absent
/// knob.
fn strip_noise_nulls(entry: &mut serde_json::Value) {
    let Some(map) = entry.as_object_mut() else {
        return;
    };
    map.retain(|_, value| !value.is_null());
    if let Some(widget) = map.get_mut("widget") {
        strip_nulls(widget);
    }
}

/// Null-valued keys out at every depth, for closed struct families
/// only (a port entry's widget): every null there is an absent
/// optional knob, never data. The one free-form value a widget holds,
/// a list source's `where` (values an item is compared against, so a
/// null in it is a value to match), is left whole.
fn strip_nulls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|_, value| !value.is_null());
            for (key, inner) in map.iter_mut() {
                if key.as_str() != "where" {
                    strip_nulls(inner);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_nulls(item);
            }
        }
        _ => {}
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
    /// Not checked when a project is BUILT (e.g. a provider with no
    /// model named yet), so an AI builder or a human can compile a
    /// project they are still sketching, settings unfilled. The
    /// editor's Problems panel skips them too (it validates in
    /// `ValidationMode::Structural`); they surface in the editor's
    /// pre-flight gate before Run/Activate/Resync and in the terminal
    /// `weft validate`, both of which validate in
    /// `ValidationMode::Runtime` (see `weft-cli`'s `do_validate`).
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
    /// An OUTPUT port with at least one wire leaving it: somebody
    /// downstream reads this value. The mirror of `InputWired`, and
    /// what makes "this port answers a misleading number, but only if
    /// anybody reads it" expressible without shouting at every program
    /// that ignores the port.
    OutputWired { port: String },
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
    /// The source declared input ports beyond the metadata's own (a
    /// node with `canAddInputPorts`, written `Node(ticket: String)`).
    /// For a node whose added inputs ARE what it works on: "added
    /// nothing" is then a program that cannot do anything, refused
    /// with `not` around this. `{customInputs}` in the message names
    /// them. Braced (`{}`) like `CustomOutputsDeclared`.
    CustomInputsDeclared {},
    /// The source declared output ports beyond the metadata's own (a
    /// node with `canAddOutputPorts`, written `-> (verdict: String)`).
    /// The ingredient for "added outputs need X": a node whose added
    /// ports only fire under some setting says so with this and a
    /// `config_equals` on the setting. `{customOutputs}` in the
    /// message names them.
    /// Braced (`{}`) so a stray field on it is refused like on every
    /// other kind.
    CustomOutputsDeclared {},
    /// The node's output `port`, as the program declared it (or as the
    /// node gives it when the program declared nothing), fits in
    /// `within`. For a narrowable output whose sensible types follow one
    /// of the node's settings: a route whose `dataType` is `text` takes
    /// a body declared `String`, and a rule says so at compile time
    /// rather than at the first call. False when the node has no such
    /// output.
    OutputTypeFits { port: String, within: WeftType },
    /// Every input the source added beyond the metadata's own (a node
    /// with `canAddInputPorts`) is declared with a type that fits in one
    /// of the types `one_of` lists; a type still open (a type variable)
    /// fits. Each input is held to ONE listed type, never to their
    /// union: with `["Json", "File | Null"]`, `String | Image` fits neither, so
    /// a node can rely on each added input being exactly one of the
    /// shapes it handles. For a node whose added inputs are sent
    /// somewhere that takes only some shapes, like a model that reads a
    /// JSON value or a file but not a record holding an image. True when
    /// nothing was added. `{names}` in the message lists the added
    /// inputs that do not fit.
    CustomInputsFit {
        #[serde(rename = "oneOf")]
        one_of: Vec<WeftType>,
    },
    /// The run this node is part of holds a node `with` a feature, in
    /// the given direction. `downstream`: every run started from this
    /// node (a trigger's fire, or a run seeded at the node) contains
    /// one; the check a trigger makes to know its caller gets answered
    /// (`{"answersCaller": true}`). `upstream`: this node is in the run
    /// of some such node; the check an answering node makes to know a
    /// caller exists (`{"liveConnection": true}`). Computed on the same
    /// program selection a fire uses, so the diagnostic and the run
    /// cannot disagree.
    RunReaches { direction: RunDirection, with: NodeWith },
    /// A node `with` a feature sits upstream of this one along the
    /// wires (data or `_should_flow`, through group, loop and include
    /// boundaries), so it runs first in any run holding both. The
    /// check for an order that breaks: a second response head after a
    /// `{"answersCaller": ["stream"]}`. Unlike `run_reaches upstream`,
    /// a node the other one merely needs (wired INTO it) is not
    /// upstream here. `{with}` in the message names the nodes found.
    DownstreamOf { with: NodeWith },
    /// This node exists once per instance: marked `@per_instance`, has
    /// an `@instance_filled` field, or reads one of those downstream
    /// (the compiler's per-instance slice). For a node that cannot be
    /// copied per instance, like a trigger serving one public address.
    /// `{perInstanceReason}` in the message says why, path included
    /// ("it reads 'blender'", "it sits inside group 'work', whose gate
    /// reads 'blender'").
    /// Braced (`{}`) so a stray field on it is refused.
    PerInstance {},
    /// Every name the input's written value holds is something of this
    /// program. Where the names come from follows the value's shape: a
    /// String is one name, a list each String in it, an object each of
    /// its keys (`{"send.model": "claude"}`). What they name is `names`:
    /// a node, spelled the way the program writes it (`bridge`,
    /// `one.bridge` inside an included file), or a node's field, written
    /// `node.field` (`send.model`, `one.send.model`).
    /// Nothing written is nothing named, so true. A wired input, or an
    /// `@instance_filled` one, holds names nobody knows yet: no answer,
    /// the rule does not fire. Wrap in `not` to refuse a bad name;
    /// `{names}` in the message lists the ones that fail.
    InputNames { port: String, names: Named },
    /// All sub-conditions must hold.
    All { of: Vec<Condition> },
    /// At least one sub-condition must hold.
    Any { of: Vec<Condition> },
    /// Negation.
    Not { of: Box<Condition> },
}

/// What the names of an `input_names` condition name, and what each
/// must be: `{"node": {"role": "infra", "perInstance": true}}` or
/// `{"field": {"instanceFilled": true}}`, or `{"state": {}}`. Each
/// narrowing is optional (`{"node": {}}` is any node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum Named {
    /// A node. `role` narrows it to an infra node or a trigger;
    /// `perInstance` to a node that is (or is not) per instance.
    Node {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<NodeRole>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        per_instance: Option<bool>,
    },
    /// A field of a node, `node.field`: the node exists and has that
    /// field (an input, or a key written on it). `instanceFilled`
    /// narrows it to a field written (or not written) `@instance_filled`.
    Field {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instance_filled: Option<bool>,
    },
    /// A place whose state a run can tell (`GetNodeState`): a node
    /// outside every loop, or a loop
    /// (`crate::project::places::state_place`).
    State {},
}

/// What kind of node `input_names` with `{"node": {"role": ..}}` requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// A node with infrastructure (`requiresInfra`).
    Infra,
    /// A trigger (`features.isTrigger`).
    Trigger,
}

/// Which way `run_reaches` looks from the node it is evaluated on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunDirection {
    Downstream,
    Upstream,
}

#[cfg(test)]
mod condition_tests {
    use super::Condition;

    /// Metadata is the wire for a rule: every condition kind reads back
    /// as itself, the two port-declaration kinds carrying no fields.
    #[test]
    fn every_condition_kind_round_trips_through_json() {
        let rule: Condition = serde_json::from_value(serde_json::json!({
            "kind": "all",
            "of": [
                { "kind": "custom_outputs_declared" },
                { "kind": "custom_inputs_declared" },
                { "kind": "not", "of": { "kind": "config_equals", "field": "parseJson", "equals": true } },
                { "kind": "any", "of": [
                    { "kind": "input_satisfied", "port": "prompt" },
                    { "kind": "input_wired", "port": "history" },
                    { "kind": "output_wired", "port": "count" },
                    { "kind": "input_source_type", "port": "provider", "equals": "OpenRouter" },
                    { "kind": "config_present", "field": "model" },
                    { "kind": "config_nonempty", "field": "model" },
                    { "kind": "config_in_set", "field": "mode", "values": ["a", "b"] },
                    { "kind": "config_matches", "field": "path", "regex": "^/" },
                    { "kind": "output_type_fits", "port": "body", "within": "JsonDict | String" },
                    { "kind": "custom_inputs_fit", "oneOf": ["Json", "List[Image]"] },
                    { "kind": "run_reaches", "direction": "downstream", "with": { "answersCaller": true } },
                    { "kind": "run_reaches", "direction": "upstream", "with": { "liveConnection": ["http"] } },
                    { "kind": "downstream_of", "with": { "answersCaller": ["stream"] } },
                    { "kind": "per_instance" },
                    { "kind": "input_names", "port": "node", "names": { "node": {} } },
                    { "kind": "input_names", "port": "infra", "names": { "node": { "role": "infra", "perInstance": true } } },
                    { "kind": "input_names", "port": "values", "names": { "field": { "instanceFilled": true } } },
                    { "kind": "input_names", "port": "clear", "names": { "field": {} } }
                ] }
            ]
        }))
        .expect("every kind parses");
        let again: Condition = serde_json::from_value(serde_json::to_value(&rule).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(&again).unwrap(), serde_json::to_value(&rule).unwrap());
        assert!(serde_json::from_value::<Condition>(serde_json::json!({ "kind": "custom_outputs_declared", "port": "x" })).is_err(), "no fields");
        assert!(serde_json::from_value::<Condition>(serde_json::json!({ "kind": "custom_inputs_declared", "port": "x" })).is_err(), "no fields");
        assert!(serde_json::from_value::<Condition>(serde_json::json!({ "kind": "per_instance", "port": "x" })).is_err(), "no fields");
        assert!(serde_json::from_value::<Condition>(serde_json::json!({ "kind": "custom_inputs_fit" })).is_err(), "a list of types");
        let names = |named: serde_json::Value| {
            serde_json::from_value::<Condition>(serde_json::json!({ "kind": "input_names", "port": "x", "names": named }))
        };
        assert!(names(serde_json::json!({ "field": { "role": "infra" } })).is_err(), "a field has no role");
        assert!(names(serde_json::json!({ "node": { "instanceFilled": true } })).is_err(), "a node is not filled");
        assert!(names(serde_json::json!("node")).is_err(), "what is named is an object");
    }

    /// A selector names exactly one feature, and matches something:
    /// `false`, an empty list, two features at once, or a feature the
    /// language does not know are refused when the metadata loads.
    #[test]
    fn a_node_selector_names_one_feature_that_matches_something() {
        let parse = |with: serde_json::Value| {
            serde_json::from_value::<Condition>(serde_json::json!({ "kind": "downstream_of", "with": with }))
        };
        assert!(parse(serde_json::json!({ "answersCaller": true })).is_ok());
        assert!(parse(serde_json::json!({ "liveConnection": ["http", "websocket"] })).is_ok());
        for bad in [
            serde_json::json!({ "answersCaller": false }),
            serde_json::json!({ "answersCaller": [] }),
            serde_json::json!({ "answersCaller": ["loudly"] }),
            serde_json::json!({ "answersCaller": true, "liveConnection": true }),
            serde_json::json!({ "nodeType": "Reply" }),
            serde_json::json!({}),
        ] {
            assert!(parse(bad.clone()).is_err(), "{bad} must be refused");
        }
    }
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

/// The output the language gives every node
/// ([`NodeMetadata::add_language_ports`]), which the runtime fills with
/// the node's failure (`context::caught_failure`). The underscore marks
/// it as the language's, like the gate on the other side
/// (`run::skip::SHOULD_FLOW_PORT`), so `error` stays a name a node may
/// give its own output.
// SYNC: ERROR_PORT <-> packages/weft-graph/src/protocol.ts ERROR_PORT
pub const ERROR_PORT: &str = "_error";

/// The [`ERROR_PORT`] output as every node's metadata carries it.
fn error_output() -> OutputSpec {
    OutputSpec {
        name: ERROR_PORT.to_string(),
        port_type: WeftType::primitive(WeftPrimitive::String),
        description: Some(
            "Why the node failed, when this output is wired: the node then completes, \
             unless it had already started a stream (a bus, a generator). Unwired, a \
             failure stops the run."
                .to_string(),
        ),
        baked: false,
        narrowable: None,
    }
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
    /// The ports a program adds to this node are named after keys of
    /// data from outside weft (a request body's fields, a table's
    /// columns, a template's ids, the keys of a JSON object it
    /// builds), so their spelling is that data's and the compiler's
    /// `not-camel-case` check leaves them alone.
    #[serde(default, rename = "addedPortsAreDataKeys", skip_serializing_if = "std::ops::Not::not")]
    pub added_ports_are_data_keys: bool,
    /// The node is a JOIN: it meets several branches and goes on with
    /// whichever arrived (`FirstInOrder`). Its created input ports are
    /// OPTIONAL (a closure on one does not skip it; a `?` on the config
    /// key changes nothing), it runs when everything it waits on has
    /// arrived or closed, and when nothing arrived it closes its output
    /// itself. That meaning is also what exempts it from the rule that
    /// a node whose wired inputs are all optional needs its gate wired:
    /// "first of these branches" is already when it runs.
    #[serde(default, rename = "join", skip_serializing_if = "std::ops::Not::not")]
    pub join: bool,
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
    /// The type every ADDED output port takes (one a wire reading it
    /// makes, on a node that accepts custom outputs), the mirror of
    /// [`Self::custom_input_type`]. `None` leaves an added output its own
    /// type variable, which only what it feeds can pin. Naming a variable
    /// one of the node's inputs uses (`T`) types each added output as
    /// that input: a router's branches carry what it routes.
    #[serde(default, rename = "customOutputType", skip_serializing_if = "Option::is_none")]
    pub custom_output_type: Option<WeftType>,
    /// Each input a program adds to this node comes with an output of the
    /// same name (typed [`Self::custom_output_type`]): one output per
    /// input it matches against, made by the compiler, so the program
    /// never writes it. An added output with no input of its name, and an
    /// added output nothing reads, are refused at compile time: the value
    /// sent there would be dropped without a word. The catalog requires
    /// `canAddInputPorts` and `canAddOutputPorts` beside it.
    #[serde(default, rename = "pairsAddedPorts", skip_serializing_if = "std::ops::Not::not")]
    pub pairs_added_ports: bool,
    /// Marks the node as a trigger (fires runs from external
    /// events rather than running as part of a run).
    #[serde(default, rename = "isTrigger", skip_serializing_if = "std::ops::Not::not")]
    pub is_trigger: bool,
    /// The node's body does nothing outside the run that weft does not put
    /// on record first: no network, no connection, no wait, no `ctx.run`,
    /// no tag, no storage beyond this run's own files. It may answer its
    /// caller (a durable run's answer leaves only once it is on record) and
    /// read and store the run's files. So running it twice changes nothing
    /// anybody can see: a
    /// durable run starts its body without first putting what came before
    /// on record, and a firing of it whose worker died runs again instead of
    /// failing. Every ctx call that reaches outside fails on a pure node,
    /// naming this flag, so a wrong `pure` cannot quietly break that.
    #[serde(default, rename = "pure", skip_serializing_if = "std::ops::Not::not")]
    pub pure: bool,
    /// The trigger's run answers a caller who holds the connection
    /// open for it, over the named wire (`"http"` for a route,
    /// `"websocket"` for a socket), so the run is always one request
    /// long and the caller's own traffic is what needs bounding.
    /// Decides which settings the language gives the trigger
    /// ([`NodeMetadata::add_language_ports`]): the per-caller limit, and
    /// whether its run may outlive the caller. The wire is what a rule asks when
    /// its advice depends on it (`run_reaches` with `liveConnection`).
    /// Backend-only: the editor never reads it, it only sees the
    /// inputs it produces.
    #[serde(default, rename = "liveConnection", skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<LiveWire>,
    /// The node answers the live caller of the run it is in, and how
    /// (see [`CallerAnswer`]). What the language knows about an
    /// answering node, in place of its type name: a trigger's "never
    /// answers its caller" rule asks for a node declaring it
    /// downstream, and "a second response head" asks which one ran
    /// first. Any node answering through the ctx declares it, a
    /// catalog one or a project's own. Backend-only, like
    /// `liveConnection`.
    #[serde(default, rename = "answersCaller", skip_serializing_if = "Option::is_none")]
    pub answers_caller: Option<CallerAnswer>,
    /// The trigger fires when somebody outside calls its address and
    /// the call ends there (a form somebody submits), so a caller exists
    /// to count per caller. A `liveConnection` trigger is called from
    /// outside already and never declares it; a trigger that picks its
    /// events up itself (a schedule, a feed, a provider's push) has no
    /// caller and does not either. Backend-only, like `liveConnection`.
    #[serde(default, rename = "calledFromOutside", skip_serializing_if = "std::ops::Not::not")]
    pub called_from_outside: bool,
    /// The node opens a bus whose recording its users may tune, so the
    /// language gives it the journal settings (`journalEphemeral`,
    /// `journalWindowMs`, [`crate::stream_journal::JournalPolicy::node_inputs`])
    /// and the node hands what they set to the bus it opens
    /// (`BusOptions { journal: ctx.journal_policy()?, .. }`). A
    /// `liveConnection` trigger has them already and never declares it.
    /// Backend-only, like `liveConnection`.
    #[serde(default, rename = "journalSettings", skip_serializing_if = "std::ops::Not::not")]
    pub journal_settings: bool,
    /// Webview hint: render the node's latest output as a JSON
    /// preview inline on the node body. Used by Debug.
    #[serde(default, rename = "showDebugPreview", skip_serializing_if = "std::ops::Not::not")]
    pub show_debug_preview: bool,
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
    /// (A loop's boundaries do NOT use this: they are not catalog
    /// nodes, the engine runs them inline.)
    #[serde(default, rename = "hidden", skip_serializing_if = "std::ops::Not::not")]
    pub hidden: bool,
}

impl NodeFeatures {
    /// Whether somebody outside calls this trigger, so a caller exists
    /// to count: a live connection, or a trigger that says so.
    pub fn has_outside_caller(&self) -> bool {
        self.live_connection.is_some() || self.called_from_outside
    }
}

/// The wire a live caller holds open (`features.liveConnection`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LiveWire {
    /// An HTTP request waiting for its response.
    Http,
    /// A WebSocket, open until either side closes it.
    Websocket,
}

/// How a node answers its run's live caller (`features.answersCaller`).
/// Told apart by what each does to an HTTP response, the one wire where
/// the order matters: its head (status and headers) goes out once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallerAnswer {
    /// The head and the whole body in one go, which ends the response.
    Whole,
    /// The head, then the body piece by piece, the response open until
    /// the pieces stop. Anything that sends a head after it fails.
    Stream,
    /// The end of the exchange, with a head of its own only when
    /// nothing went out before it.
    End,
}

/// Which values of one feature a rule looks for: `true` for any node
/// that declares the feature at all, or the list of values that count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeatureValues<T> {
    Any,
    OneOf(Vec<T>),
}

impl<T: PartialEq> FeatureValues<T> {
    /// Whether a node declaring `declared` (None: not declaring the
    /// feature) is one of them.
    pub fn includes(&self, declared: Option<&T>) -> bool {
        match (self, declared) {
            (_, None) => false,
            (FeatureValues::Any, Some(_)) => true,
            (FeatureValues::OneOf(values), Some(v)) => values.contains(v),
        }
    }
}

impl<T: Serialize> Serialize for FeatureValues<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            FeatureValues::Any => s.serialize_bool(true),
            FeatureValues::OneOf(values) => values.serialize(s),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for FeatureValues<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw<T> {
            Flag(bool),
            List(Vec<T>),
        }
        match Raw::deserialize(d).map_err(|_| {
            serde::de::Error::custom("expected `true` (any value) or a non-empty list of the values that count")
        })? {
            Raw::Flag(true) => Ok(FeatureValues::Any),
            Raw::List(values) if !values.is_empty() => Ok(FeatureValues::OneOf(values)),
            Raw::Flag(false) | Raw::List(_) => Err(serde::de::Error::custom(
                "matches no node: write `true` for any value, or list the values that count",
            )),
        }
    }
}

/// The nodes a graph rule looks for, by a feature they declare. One
/// feature per selector, keyed by the feature's own metadata name, so a
/// rule reads the way the node it looks for declares itself:
/// `{"answersCaller": true}`, `{"liveConnection": ["http"]}`. A rule
/// never names a node type: a project's own node declaring the feature
/// counts exactly like a catalog one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum NodeWith {
    #[serde(rename = "answersCaller")]
    AnswersCaller(FeatureValues<CallerAnswer>),
    #[serde(rename = "liveConnection")]
    LiveConnection(FeatureValues<LiveWire>),
}

impl NodeWith {
    pub fn matches(&self, features: &NodeFeatures) -> bool {
        match self {
            NodeWith::AnswersCaller(values) => values.includes(features.answers_caller.as_ref()),
            NodeWith::LiveConnection(values) => values.includes(features.live_connection.as_ref()),
        }
    }
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
// SYNC: FormFieldRender <-> packages/weft-graph/src/protocol.ts FormFieldRenderWire,
//       extension-browser/src/lib/api.ts FormFieldRender
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

    /// Whether an empty list in this field means the author CHOSE
    /// NOTHING (a `value_list` of match candidates, a `typed` field
    /// whose declared type is a list of choices), which satisfies no
    /// requirement. For every other shape a list is a single VALUE
    /// being matched (`equals: []` legitimately routes an empty list),
    /// so emptiness says nothing about filled-in-ness there.
    /// SYNC: empty-list-is-nothing-chosen <->
    ///       crates/weft-compiler/src/validate.rs (the spec-field loop),
    ///       packages/weft-graph/src/webview/lib/utils/port-specs.ts
    ///       (isEmptyChoiceSet)
    pub fn empty_list_means_nothing_chosen(&self) -> bool {
        match self.shape {
            SpecShape::ValueList => true,
            SpecShape::Typed => {
                matches!(self.value_type, Some(WeftType::List(_)))
            }
            _ => false,
        }
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
#[serde(rename_all = "snake_case")]
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
    /// Why a type the catalog has SEEN is not in it, as the whole
    /// sentence to show: a node whose code is not written yet, or one
    /// that failed to load (and why). Such a node is left out of the
    /// catalog (a program that never names it builds), and a program
    /// that does name it is told this instead of "unknown node type".
    /// `None` for a type the catalog never saw.
    fn unavailable(&self, _node_type: &str) -> Option<String> {
        None
    }
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
    /// The node packages this binary's build left out because they did
    /// not compile (`crate::builds::LeftOut`, the build's own record): a
    /// program naming one of their types has no implementation here, and
    /// a run whose cut uses one is refused. Empty for a binary built whole.
    fn left_out(&self) -> &'static crate::builds::LeftOut {
        static NOTHING: crate::builds::LeftOut = crate::builds::LeftOut { packages: Vec::new() };
        &NOTHING
    }
}

// NAMING CONVENTION (the three stages a graph concept lives through):
//   *Spec       = authored intent (metadata.json / a registration): blanks are
//                 meaningful ("derive from the type", "decide later"). InputSpec,
//                 OutputSpec, PortSpec, SignalSpec, InfraSpec.
//   Parsed*     = raw from .weft source, pre-enrichment (compiler-internal).
//   *Definition = compiled, fully resolved; the editor and runtime trust it
//                 blindly and never re-derive. NodeDefinition, InputDefinition,
//                 PortDefinition, GroupDefinition, ProjectDefinition.
// A new type for one of these stages takes the stage's suffix.

/// One of the two things that can drive an input: a constant written in
/// the source (whatever its spelling: braces, statement, `@file`,
/// `@asset`), or a value another node produces at run time (an edge, a
/// dotted value in the braces, an inline node).
// SYNC: AcceptedForm <-> packages/weft-graph/src/protocol.ts AcceptedForm, packages/weft-connect/src/core/wire.ts AcceptedForm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceptedForm {
    Literal,
    Wire,
}

/// Which drivers an input takes. On the wire it is the list of accepted
/// forms (`["literal", "wire"]`); an empty list or a repeated entry is
/// refused at parse, so a resolved value always accepts at least one.
// SYNC: Accepts <-> packages/weft-graph/src/protocol.ts Accepts, packages/weft-connect/src/core/wire.ts Accepts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<AcceptedForm>", into = "Vec<AcceptedForm>")]
pub struct Accepts {
    pub literal: bool,
    pub wire: bool,
}

impl Accepts {
    pub fn both() -> Self {
        Self { literal: true, wire: true }
    }

    pub fn wire_only() -> Self {
        Self { literal: false, wire: true }
    }

    pub fn literal_only() -> Self {
        Self { literal: true, wire: false }
    }

    /// What a type takes when nobody says otherwise: a live handle (a
    /// `Bus`, a `Generator`) exists only while something runs, an `Infra`
    /// handle only once its infra node has emitted it, and a `Secret` only
    /// once a node made it, so no human can write one; everything else
    /// takes both.
    pub fn for_type(ty: &WeftType) -> Self {
        if Self::type_is_wire_only(ty) { Self::wire_only() } else { Self::both() }
    }

    /// A type whose values only a run makes (a live handle, a secret),
    /// wire-only by nature.
    pub fn type_is_wire_only(ty: &WeftType) -> bool {
        matches!(ty.structural(), WeftType::Bus | WeftType::Infra | WeftType::Generator(_) | WeftType::Secret(_))
    }

    /// The list as an error message reads it back: `literal, wire`.
    pub fn describe(self) -> String {
        let mut parts = Vec::new();
        if self.literal {
            parts.push("literal");
        }
        if self.wire {
            parts.push("wire");
        }
        parts.join(", ")
    }
}

impl TryFrom<Vec<AcceptedForm>> for Accepts {
    type Error = String;
    fn try_from(forms: Vec<AcceptedForm>) -> Result<Self, String> {
        if forms.is_empty() {
            return Err("`accepts` lists at least one of \"literal\", \"wire\"; an input \
                        that takes nothing cannot be driven"
                .into());
        }
        let mut out = Self { literal: false, wire: false };
        for form in forms {
            let slot = match form {
                AcceptedForm::Literal => &mut out.literal,
                AcceptedForm::Wire => &mut out.wire,
            };
            if *slot {
                return Err(format!("`accepts` repeats {form:?}"));
            }
            *slot = true;
        }
        Ok(out)
    }
}

impl From<Accepts> for Vec<AcceptedForm> {
    fn from(a: Accepts) -> Self {
        let mut out = Vec::new();
        if a.literal {
            out.push(AcceptedForm::Literal);
        }
        if a.wire {
            out.push(AcceptedForm::Wire);
        }
        out
    }
}

/// One declared INPUT of a node: everything the node takes, wired data
/// and design-time configuration alike, under one name in one namespace.
/// `accepts` says which drivers it takes (see [`Accepts`]);
/// `widget` overrides the type-derived editor control; `default` is the
/// value the runtime supplies when nothing else drives the input (never
/// written into source).
// SYNC: InputSpec <-> packages/weft-graph/src/protocol.ts InputSpec
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub input_type: WeftType,
    #[serde(default)]
    pub required: bool,
    /// Which drivers this input takes, when the author narrows it.
    /// Absent means both (a constant in the source, or a wire from
    /// another node); `["wire"]` refuses every written value, `["literal"]`
    /// refuses wiring. A port never adds a form, it only removes one, and
    /// only for a reason the node can name. Two ports never carry a list:
    /// a `Bus`/`Generator` port is wire-only by nature (the loader forces
    /// it), and a compiler-read port (the `portsFromConfig` list, the
    /// access picker) takes an inline value only by a fixed rule.
    // SYNC: InputSpec.accepts <-> packages/weft-graph/src/protocol.ts InputSpec.accepts
    #[serde(default)]
    pub accepts: Option<Accepts>,
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
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_values: Option<Vec<String>>,
}

impl InputSpec {
    /// The drivers this input takes: the fixed rule for a compiler-read
    /// port (an inline value only), else the author's list, else the
    /// type's own answer (a live handle is wire-only, everything else
    /// takes both).
    pub fn effective_accepts(&self, compiler_read: bool) -> Accepts {
        if compiler_read {
            return Accepts::literal_only();
        }
        self.accepts.unwrap_or_else(|| Accepts::for_type(&self.input_type))
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
    /// Human-readable description shown next to the port in the editor.
    /// The webview reads it; the compiler treats it as opaque.
    #[serde(default)]
    pub description: Option<String>,
    /// An infra node's output whose value is made once, when its infra
    /// is applied, and saved: a fire reads the saved value instead of
    /// running the node, and a node whose every wired output is saved
    /// does not run at a fire at all. For a value that holds as long as
    /// the infra does (an address, a connection); one that changes on
    /// its own (a status) is not baked. The infra node only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub baked: bool,
    /// The program may declare what this output holds, for an output
    /// whose shape only the program knows (a model's JSON answer, a
    /// request's body, a query's rows): `-> (response: Verdict)` on the
    /// call. `true` lets the declared type be anything that fits in
    /// `type`; a type string is a wider bound than `type`, which stays
    /// what the port holds when nothing is declared (a route's body is
    /// `JsonDict` unless declared `String` or a file). The compiler
    /// checks the declaration fits; at run time the emit check holds
    /// every value to it, and a value that does not fit is a failure
    /// the program can catch. An output that is not narrowable takes no
    /// other type than its own.
    // SYNC: OutputSpec.narrowable <-> packages/weft-graph/src/protocol.ts OutputSpec.narrowable
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrowable: Option<Narrowable>,
}

/// How far an output may be narrowed ([`OutputSpec::narrowable`]),
/// written `true` or a type string.
#[derive(Debug, Clone, PartialEq)]
pub enum Narrowable {
    /// `true`: anything that fits in the output's own type.
    Within,
    /// A bound wider than the output's own type.
    Bound(WeftType),
}

impl Serialize for Narrowable {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Narrowable::Within => serializer.serialize_bool(true),
            Narrowable::Bound(bound) => bound.serialize(serializer),
        }
    }
}

/// `true` or a type string, anything else refused by name: a typo in
/// the bound says which name is unknown (an untagged enum would only
/// say that nothing matched), and `false` says to drop the key.
impl<'de> Deserialize<'de> for Narrowable {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::Bool(true) => Ok(Narrowable::Within),
            serde_json::Value::Bool(false) => Err(D::Error::custom(
                "`\"narrowable\": false` is what leaving the key out means; remove it",
            )),
            serde_json::Value::String(bound) => WeftType::try_parse(&bound)
                .map(Narrowable::Bound)
                .map_err(|why| D::Error::custom(format!("narrowable bound `{bound}`: {why}"))),
            other => Err(D::Error::custom(format!(
                "narrowable is `true` or the type a declaration must fit in, got {other}"
            ))),
        }
    }
}

impl OutputSpec {
    /// The type a declaration on this output must fit in, when it may be
    /// narrowed at all.
    // SYNC: narrowing_bound <-> packages/weft-graph/src/protocol.ts narrowingBound
    pub fn narrowing_bound(&self) -> Option<&WeftType> {
        match &self.narrowable {
            Some(Narrowable::Within) => Some(&self.port_type),
            Some(Narrowable::Bound(bound)) => Some(bound),
            None => None,
        }
    }
}

/// The languages a code widget can highlight. Closed: a metadata file
/// naming any other word fails to load, instead of the editor quietly
/// rendering plain text.
// SYNC: CodeLanguage <-> packages/weft-connect/src/core/wire.ts CodeLanguage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodeLanguage {
    Python,
    Javascript,
    Sql,
    Json,
}

/// The editor control an input renders. The vocabulary of the node
/// inspector: every input has exactly one effective widget (declared, or
/// derived from the type via [`Widget::default_for_type`]).
// SYNC: Widget <-> packages/weft-graph/src/protocol.ts Widget/WidgetKind, packages/weft-connect/src/core/wire.ts Widget
// The compact wiring view (`NodeMetadata::compact_json`) is a read-only
// projection of this: it may cut a long `options` list and add a
// `moreOptions` count, so its output is never parsed back into a
// `Widget` (which denies unknown fields).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum Widget {
    Text,
    Textarea,
    /// `language` picks the syntax highlighting in the editor's code box.
    Code { language: CodeLanguage },
    /// `step` is the input's granularity (the arrow/slider increment), the
    /// third knob of a number input alongside `min`/`max`.
    Number { min: Option<f64>, max: Option<f64>, step: Option<f64> },
    Checkbox,
    /// A calendar-and-clock picker for a String that holds one moment.
    /// The stored value is ISO-8601 with the zone of whoever picked
    /// it (`2026-09-03T11:00:00+02:00`), so the source says which
    /// eleven o'clock it means.
    Datetime,
    /// `freeText`: the options are suggestions, not a closed set (a
    /// provider's model ids, voices, effort levels: one released after
    /// the list was written must still pass). The editor still shows
    /// the list and lets the user type a value outside it, and
    /// [`Widget::check_value`] takes any string. Default false: the
    /// options are the input's domain, because the node's code matches
    /// on them.
    Select {
        options: Vec<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        free_text: bool,
    },
    /// `freeText` as on [`Widget::Select`], per item.
    Multiselect {
        options: Vec<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        free_text: bool,
    },
    /// A list of short text values the author adds and removes one at a
    /// time (a select's options, the values a case matches).
    TextList,
    Password,
    /// The connection picker on an ACCESS NODE (the node whose
    /// metadata carries the `service` recipe). Renders the connection
    /// list (existing connections plus the declared doors of "+ Add a
    /// connection"); the stored config value is the small
    /// `{"id": "<connection id>", "identity": "..."}` handle, never a
    /// secret. `service` and `optional` are COMPILER-STAMPED from the
    /// node metadata's recipe (`service.service` /
    /// `service.connectionOptional`) at enrich time; authors write only
    /// `{"kind": "access"}`. `optional` = the node runs without a
    /// connection picked, so the editor neither pins it open nor
    /// synthesizes the "no connection picked" rule for it.
    Access {
        service: Option<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        optional: bool,
    },
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
    /// The shape a connection widget's OBJECT value must have: a
    /// remote_select pick is `{id: String, label?: String}`, an access
    /// handle `{id: String, identity?: String}`. One rule for the
    /// compiler (a compile error on the written value) and the runtime
    /// (a loud failure when the bag is built), so a shape that compiles
    /// is a shape that runs. Any other widget, or a non-object value (a
    /// pasted raw id), has no handle shape and passes.
    pub fn check_handle_shape(&self, value: &Value) -> Result<(), String> {
        let (name, extra) = match self {
            Widget::RemoteSelect { .. } => ("a remote_select pick", "label"),
            Widget::Access { .. } => ("an access handle", "identity"),
            _ => return Ok(()),
        };
        let Some(obj) = value.as_object() else { return Ok(()) };
        if !obj.get("id").is_some_and(Value::is_string) {
            return Err(format!("{name} is an {{id, {extra}}} object with a string `id`"));
        }
        if obj.get(extra).is_some_and(|v| !v.is_string() && !v.is_null()) {
            return Err(format!("{name} carries `{extra}` as a string"));
        }
        Ok(())
    }

    /// What a widget's domain refuses: a number below `min`, above
    /// `max`, or off a whole-number `step`; a `Select` value that is not
    /// one of its closed `options`; a `Multiselect` item that is not
    /// (a `freeText` list is suggestions and refuses nothing). `Ok(())`
    /// for every other widget and for a value of the wrong shape, which
    /// the type check owns.
    ///
    /// This is a DOMAIN rule, not a drawing hint: the node's own code
    /// relies on it (a poll interval of zero, a spreadsheet row zero, a
    /// mode it matches on), so it binds a value that arrived over a wire,
    /// or one an instance provides, exactly as it binds one written in
    /// the source. The compiler holds the written constant to it and the
    /// runtime holds the wired value to it, through this one function,
    /// so a program that compiles is a program that runs. A list a
    /// provider can extend (model ids, voices) is declared `freeText`,
    /// so a value released after the node was written still passes.
    pub fn check_value(&self, value: &Value) -> Result<(), String> {
        let (min, max, step) = match self {
            Widget::Number { min, max, step } => (min, max, step),
            // Suggestions bind nothing: any string of the right type passes.
            Widget::Select { free_text: true, .. } | Widget::Multiselect { free_text: true, .. } => {
                return Ok(());
            }
            Widget::Select { options, free_text: false } => {
                return match value.as_str() {
                    Some(picked) => check_option(options, picked),
                    None => Ok(()),
                };
            }
            Widget::Multiselect { options, free_text: false } => {
                let Some(items) = value.as_array() else { return Ok(()) };
                return items.iter().filter_map(Value::as_str).try_for_each(|picked| check_option(options, picked));
            }
            _ => return Ok(()),
        };
        let Some(n) = value.as_f64() else {
            return Ok(());
        };
        if min.is_some_and(|m| n < m) || max.is_some_and(|m| n > m) {
            return Err(format!(
                "{n} is outside the allowed range [{}, {}]",
                min.map_or("-inf".into(), |m| m.to_string()),
                max.map_or("inf".into(), |m| m.to_string()),
            ));
        }
        // A whole-number step says this input takes whole numbers, and
        // the node's code relies on it (a row number, a count of
        // seconds cast to an integer). A FRACTIONAL step is the arrow
        // key's increment and nothing more: a temperature box stepping
        // by 0.1 still takes 0.85, and holding a typed value to the
        // arrows would refuse numbers every model accepts.
        let whole_only = step.is_some_and(|s| s > 0.0 && s.fract() == 0.0);
        if whole_only && n.fract() != 0.0 {
            return Err(format!("{n} is not a whole number"));
        }
        Ok(())
    }

    /// Whether a refusal of [`Widget::check_value`] is about a list of
    /// options (the compiler reports it under its own slug).
    pub fn has_options(&self) -> bool {
        matches!(self, Widget::Select { free_text: false, .. } | Widget::Multiselect { free_text: false, .. })
    }

    /// Is the widget itself satisfiable, or is the node's metadata
    /// wrong: a number box whose minimum is above its maximum, or whose
    /// step is not a size. Checked when a node loads, naming the input,
    /// so a broken declaration is a metadata error at the door rather
    /// than a failure in the middle of somebody's run.
    pub fn check_declaration(&self) -> Result<(), String> {
        let Widget::Number { min, max, step } = self else {
            return Ok(());
        };
        if let (Some(lo), Some(hi)) = (min, max) {
            if lo > hi {
                return Err(format!(
                    "its number box has a minimum of {lo} above its maximum of {hi}, so no value \
                     could ever fit it"
                ));
            }
        }
        if let Some(step) = step {
            if !matches!(step.partial_cmp(&0.0), Some(std::cmp::Ordering::Greater)) {
                return Err(format!("its number box has a step of {step}, which is not a size"));
            }
        }
        Ok(())
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Widget::Text => "text",
            Widget::Textarea => "textarea",
            Widget::Code { .. } => "code",
            Widget::Number { .. } => "number",
            Widget::Checkbox => "checkbox",
            Widget::Datetime => "datetime",
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
// SYNC: ResourceSource <-> packages/weft-connect/src/core/wire.ts ResourceSource
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
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

/// Refuse a `remote_select` source that cannot work, naming `owner` (the
/// input or entry field it sits on): a `from_url` pattern that does not
/// parse, a picker with no https script or no glue code, a list source
/// both `public` and `requires`, or one whose `get` or refusal paths do
/// not read. At load, never on the first dropdown click.
fn validate_resource_sources(owner: &str, sources: &[ResourceSource]) -> Result<(), String> {
    for source in sources {
        match source {
            ResourceSource::FromUrl { pattern } => {
                if let Err(e) = regex::Regex::new(pattern.as_str()) {
                    return Err(format!("{owner}: from_url pattern does not parse: {e}"));
                }
            }
            ResourceSource::Picker { script, code, .. } => {
                if !script.starts_with("https://") {
                    return Err(format!(
                        "{owner}: a picker's script must be an https:// address, got '{script}'"
                    ));
                }
                if code.trim().is_empty() {
                    return Err(format!(
                        "{owner}: a picker needs its glue `code` (the statements that open \
                         the chooser and call weft.done)"
                    ));
                }
            }
            ResourceSource::List { requires, lookup } => {
                // "public" says the call carries no credential at all;
                // "requires" says it needs these permissions on the
                // connection. Declaring both is self-contradictory and
                // would leave the editor's source-selection ambiguous.
                if lookup.public && !requires.is_empty() {
                    return Err(format!(
                        "{owner}: a `public` list source cannot also declare `requires` (a \
                         credential-free call has no permissions to hold)"
                    ));
                }
                crate::access::spec::Template::new(lookup.get.clone())
                    .placeholders()
                    .map_err(|e| format!("{owner}: list source get: {e}"))?;
                crate::access::spec::AnswerReader::validate(lookup, &format!("{owner}: list source"))?;
            }
            ResourceSource::Granted { .. } => {}
        }
    }
    Ok(())
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
/// `dependsOn`). The dispatcher runs it through the stored access;
/// the editor only ever sees label/value pairs.
// SYNC: Lookup <-> packages/weft-connect/src/core/wire.ts Lookup
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lookup {
    /// GET URL. `{query}` interpolates the user's search text
    /// (url-encoded); `{<parent input name>}` interpolates a
    /// `dependsOn` parent's picked id.
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
    /// Keep only the items matching every entry: a dotted path
    /// (relative to one item, like `label` / `value`) mapped to the
    /// value the item must hold there, compared as JSON (an item with
    /// nothing at the path is dropped). For a service answering one
    /// list for several jobs, e.g. `{"can_do_text_to_speech": true}`.
    /// Empty keeps every item.
    #[serde(default, rename = "where", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub where_equals: std::collections::BTreeMap<String, Value>,
    /// Where a refusal answered with a success status sits, read as a
    /// [`RefusalReader`](crate::access::spec::RefusalReader)'s `errorPath`
    /// is. Nothing is read unless declared. Its own field rather than a
    /// flattened reader: `ResourceSource::List` already flattens the
    /// lookup, and serde cannot refuse unknown keys through two flattens.
    #[serde(default, rename = "errorPath", skip_serializing_if = "Option::is_none")]
    pub error_path: Option<String>,
    /// A success flag, read as a reader's `okPath` is: anything there but
    /// `true` or `null` refuses the lookup (Slack's `ok`), and nothing
    /// there passes.
    #[serde(default, rename = "okPath", skip_serializing_if = "Option::is_none")]
    pub ok_path: Option<String>,
}

/// A lookup reads its answer's refusal exactly as every declared call
/// does: the one check and the one validator.
impl crate::access::spec::AnswerReader for Lookup {
    fn refusal_paths(&self) -> crate::access::spec::RefusalPaths<'_> {
        crate::access::spec::RefusalPaths { error: self.error_path.as_deref(), ok: self.ok_path.as_deref() }
    }

    fn validate(&self, what: &str) -> Result<(), String> {
        crate::access::spec::validate_refusal_paths(what, &self.error_path, &self.ok_path)
    }
}

/// Cursor pagination on a [`Lookup`]: the request param the cursor is
/// sent in, and the response path the next cursor is read from (empty
/// or absent = no more pages).
// SYNC: PageSpec <-> packages/weft-connect/src/core/wire.ts PageSpec
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

    /// Set `port` to any serializable value (a struct, a list of
    /// them). A value that cannot become JSON (a map with non-string
    /// keys, a failing custom `Serialize`) fails the node naming the
    /// port.
    pub fn set_serialized(self, port: &str, value: &impl Serialize) -> crate::WeftResult<Self> {
        let value = serde_json::to_value(value).map_err(|e| {
            crate::WeftError::NodeFailed(format!("output '{port}' cannot be serialized: {e}"))
        })?;
        Ok(self.set(port, value))
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
    /// A declared key whose value is NULL is skipped too, when the port
    /// cannot hold a null. A dynamic payload says "this message has no
    /// caption" with a null, and firing it at a `String` port makes the
    /// engine record a type mismatch and CLOSE the port, on every run,
    /// for a message that was simply a picture without a caption. Not
    /// firing says the same thing without the noise, and a port that
    /// declares `Null` among its types still gets the value.
    ///
    /// Nodes use it through [`crate::RunContext::fan_declared`],
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
                let Some(declared_type) = declared.get(k) else { continue };
                if v.is_null() && !declared_type.accepts_runtime_value(v) {
                    continue;
                }
                self.outputs.insert(k.clone(), v.clone());
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
    /// A dynamic payload says "no caption on this picture" with a
    /// null. Sending that at a `String` port is refused by the engine
    /// (the send fails and the port stays open), so the fan drops it
    /// here rather than building a send that could only be turned
    /// down. A port that declares Null among its types still gets it.
    #[test]
    fn a_null_is_skipped_unless_the_port_can_hold_one() {
        use crate::weft_type::{WeftPrimitive, WeftType};
        let declared: std::collections::HashMap<String, WeftType> = [
            ("content".to_string(), WeftType::Primitive(WeftPrimitive::String)),
            (
                "maybe".to_string(),
                WeftType::Union(vec![
                    WeftType::Primitive(WeftPrimitive::String),
                    WeftType::Primitive(WeftPrimitive::Null),
                ]),
            ),
        ]
        .into_iter()
        .collect();
        let out = NodeOutput::new().extend_from_declared(
            &serde_json::json!({ "content": null, "maybe": null, "undeclared": "x" }),
            &declared,
        );
        assert!(!out.outputs.contains_key("content"), "a null on a String port never fires");
        assert_eq!(out.outputs["maybe"], serde_json::Value::Null, "a nullable port takes it");
        assert!(!out.outputs.contains_key("undeclared"));
    }

    /// `set_serialized` stores a struct as its JSON, and a value with no
    /// JSON form fails naming the port.
    #[test]
    fn set_serialized_sets_the_json_or_names_the_port() {
        #[derive(serde::Serialize)]
        struct Run {
            id: u32,
        }
        let out = NodeOutput::new().set_serialized("runs", &vec![Run { id: 7 }]).unwrap();
        assert_eq!(out.outputs["runs"], serde_json::json!([{ "id": 7 }]));
        let bad: std::collections::HashMap<(u8, u8), u8> = [((1, 2), 3)].into_iter().collect();
        let err = NodeOutput::new().set_serialized("pairs", &bad).unwrap_err().to_string();
        assert!(err.contains("'pairs'"), "{err}");
    }

    use super::*;
    use serde_json::json;

    /// Build a declared-output map from port names. `extend_from_declared`
    /// only consults membership, so the type is an irrelevant placeholder.
    fn declared_ports(names: &[&str]) -> std::collections::HashMap<String, WeftType> {
        names
            .iter()
            .map(|n| (n.to_string(), WeftType::type_var("T")))
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
            "name": "model", "type": "String", "accepts": ["literal"],
            "widget": {"kind": "text"}, "default": "sonnet", "label": "Model",
            "placeholder": "anthropic/claude-sonnet-4.6"
        }]));
        let parsed: NodeMetadata = serde_json::from_value(meta).expect("input loads");
        let input = &parsed.inputs[0];
        assert_eq!(input.effective_accepts(false), Accepts::literal_only());
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
            "name": "temperature", "type": "Number",
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

    /// A select's options are its domain: a value outside them is
    /// refused naming the ones allowed, each item of a multiselect is
    /// held to them, and a value of another shape is the type check's.
    #[test]
    fn a_select_refuses_a_value_outside_its_options() {
        let select = Widget::Select { options: vec!["plain".into(), "markdown".into()], free_text: false };
        assert_eq!(select.check_value(&json!("plain")), Ok(()));
        assert_eq!(
            select.check_value(&json!("html")),
            Err("'html' is not one of the options; give one of 'plain', 'markdown'".to_string())
        );
        assert_eq!(select.check_value(&json!(3)), Ok(()));

        let multi = Widget::Multiselect { options: vec!["a".into(), "b".into()], free_text: false };
        assert_eq!(multi.check_value(&json!(["a", "b"])), Ok(()));
        assert_eq!(multi.check_value(&json!([])), Ok(()));
        assert!(multi.check_value(&json!(["a", "c"])).unwrap_err().starts_with("'c' is not one of the options"));

        let long = Widget::Select { options: (0..20).map(|i| format!("z{i}")).collect(), free_text: false };
        let why = long.check_value(&json!("nowhere")).unwrap_err();
        assert!(why.ends_with("'z11', and 8 more"), "{why}");
    }

    /// A `freeText` list is suggestions: a model id released after the
    /// list was written passes, on a select and on each multiselect item,
    /// the flag loads from metadata JSON, and it is not reported under
    /// the options slug.
    #[test]
    fn a_free_text_select_takes_a_value_outside_its_options() {
        let select: Widget =
            serde_json::from_value(json!({"kind": "select", "options": ["m1"], "freeText": true}))
                .expect("freeText select loads");
        assert_eq!(select, Widget::Select { options: vec!["m1".into()], free_text: true });
        assert_eq!(select.check_value(&json!("m1")), Ok(()));
        assert_eq!(select.check_value(&json!("released_tomorrow")), Ok(()));
        assert!(!select.has_options());

        let multi = Widget::Multiselect { options: vec!["a".into()], free_text: true };
        assert_eq!(multi.check_value(&json!(["a", "z"])), Ok(()));

        // Round trip: a closed list serializes without the flag.
        let closed = Widget::Select { options: vec!["a".into()], free_text: false };
        assert_eq!(serde_json::to_value(&closed).unwrap(), json!({"kind": "select", "options": ["a"]}));
        assert_eq!(serde_json::to_value(&select).unwrap()["freeText"], json!(true));
    }

    /// A default outside the select's options is a metadata error when
    /// the node loads, since it is the value that arrives when nothing
    /// else does.
    #[test]
    fn a_default_outside_the_options_fails_the_load() {
        let mut meta = base();
        meta.as_object_mut().unwrap().insert("inputs".into(), json!([{
            "name": "mode", "type": "String", "default": "loud",
            "widget": {"kind": "select", "options": ["quiet", "normal"]}
        }]));
        let parsed: NodeMetadata = serde_json::from_value(meta).expect("the shape loads");
        let err = parsed.validate_semantics().expect_err("the default is no option");
        assert!(err.contains("'loud' is not one of the options"), "{err}");
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

        let numbers = WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::Number)));
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
            "requiresInfra": true,
            "inputs": [
                { "name": "code", "type": "String", "required": true,
                  "widget": { "kind": "code", "language": "python" } },
                { "name": "pick", "type": "String",
                  "widget": { "kind": "select", "options": ["a", "b"] } },
                { "name": "n", "type": "Number",
                  "widget": { "kind": "number", "min": 0.0, "max": 9.0, "step": 1.0 } },
                { "name": "grant", "type": "Access",
                  "widget": { "kind": "access" },
                  "requiresScopes": ["s.read"], "requiresValues": ["host"] },
                { "name": "sheet", "type": "String",
                  "widget": { "kind": "remote_select", "access": "grant",
                              "sources": [{ "kind": "granted", "from": "sheets" }], "dependsOn": ["pick"] } },
                { "name": "img", "type": "Image",
                  "widget": { "kind": "file_drop", "type": "Image", "accept": "image/png" } },
                { "name": "fields", "type": "List[JsonDict]",
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
             "inputs", "label", "outputs", "portsFromConfig", "requiresInfra", "tags", "type"],
            "top-level catalog keys are the editor contract"
        );
        // Every resolved input ships the full editor surface, with the
        // renamed camelCase keys the TS mirror reads.
        assert_eq!(
            keys(&v["inputs"][3]),
            ["accepts", "default", "description", "label", "name", "placeholder",
             "required", "requiresScopes", "requiresValues", "type", "widget"]
        );
        // Widget variants serialize their own payload under the tag.
        assert_eq!(keys(&v["inputs"][0]["widget"]), ["kind", "language"]);
        assert_eq!(keys(&v["inputs"][1]["widget"]), ["kind", "options"]);
        assert_eq!(keys(&v["inputs"][2]["widget"]), ["kind", "max", "min", "step"]);
        // The access widget: `service` always rides (null until the
        // compiler stamps it), `optional` only when true.
        assert_eq!(keys(&v["inputs"][3]["widget"]), ["kind", "service"]);
        let optional_access: Widget = serde_json::from_value(
            serde_json::json!({ "kind": "access", "service": "s", "optional": true }),
        )
        .unwrap();
        assert_eq!(
            keys(&serde_json::to_value(&optional_access).unwrap()),
            ["kind", "optional", "service"]
        );
        assert_eq!(keys(&v["inputs"][4]["widget"]), ["access", "dependsOn", "kind", "sources"]);
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
        assert_eq!(pointy.file, None, "absent file defaults to None (the compiled source)");
        // `file` (an @include's own file) serializes under that key, and
        // is OMITTED when None so old consumers see the old shape.
        let d2 = Diagnostic::at(Span::default(), Severity::Error, "x", "m");
        assert!(serde_json::to_value(&d2).unwrap().get("file").is_none(), "None file emits no key");
        let with_file: Diagnostic = serde_json::from_value(serde_json::json!({
            "line": 1, "column": 0, "severity": "error", "message": "m", "file": "/a/b.weft"
        })).unwrap();
        assert_eq!(with_file.file.as_deref(), Some("/a/b.weft"));
        assert_eq!(
            serde_json::to_value(&with_file).unwrap()["file"], "/a/b.weft",
            "file round-trips under the `file` key"
        );
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
    use crate::weft_type::{WeftPrimitive, WeftType};
    use serde_json::json;

    /// A node type's metadata port: its value is written by hand, fits its
    /// concrete type, and its name is none of its other ports'.
    #[test]
    fn a_metadata_port_is_written_fits_and_is_named_alone() {
        let port = |name: &str, ty: &str, value: Option<serde_json::Value>| crate::project::MetadataPort {
            name: name.into(),
            port_type: WeftType::parse(ty).expect("type"),
            description: None,
            value,
            written: None,
            span: None,
            declared_type: None,
        };
        let mut m = metadata_with(vec![]);
        m.metadata_ports.push(port("tool", "{ name: String }", Some(json!({ "name": "x" }))));
        m.validate_semantics().expect("a written value of its type");

        m.metadata_ports[0].value = Some(json!({ "name": 3 }));
        assert!(m.validate_semantics().unwrap_err().contains("does not fit its type"));
        m.metadata_ports[0].value = None;
        assert!(m.validate_semantics().unwrap_err().contains("has no `value`"));
        m.metadata_ports[0] = port("tool", "T", Some(json!(1)));
        assert!(m.validate_semantics().unwrap_err().contains("concrete type"));
        m.metadata_ports[0] = port("tool", "String", Some(json!("x")));
        m.metadata_ports[0].declared_type = Some("String".into());
        assert!(m.validate_semantics().unwrap_err().contains("remove them"));
        for taken in ["value", "_should_flow", ERROR_PORT] {
            let mut m = metadata_with(vec![]);
            m.outputs.push(OutputSpec { name: "value".into(), port_type: WeftType::Primitive(WeftPrimitive::String), description: None, baked: false, narrowable: None });
            m.metadata_ports.push(port(taken, "String", Some(json!("x"))));
            assert!(m.validate_semantics().unwrap_err().contains("share one set of names"), "{taken}");
        }
    }

    /// The two features that shape a node's added outputs need it to take
    /// added outputs at all.
    #[test]
    fn paired_and_typed_added_outputs_need_added_ports() {
        let mut m = metadata_with(vec![]);
        m.features.pairs_added_ports = true;
        assert!(m.validate_semantics().unwrap_err().contains("canAddInputPorts and canAddOutputPorts"));
        m.features.can_add_input_ports = true;
        m.features.can_add_output_ports = true;
        m.validate_semantics().expect("pairing with both");
        m.features.can_add_output_ports = false;
        m.features.pairs_added_ports = false;
        m.features.custom_output_type = Some(WeftType::type_var("T"));
        assert!(m.validate_semantics().unwrap_err().contains("declare canAddOutputPorts"));
    }

    /// A rule asking for added ports is on a node that takes them: on any
    /// other its answer never changes, and under a `not` it fires on every
    /// program.
    #[test]
    fn a_rule_about_added_ports_needs_a_node_that_takes_them() {
        for (condition, feature) in [
            (json!({ "kind": "custom_inputs_declared" }), "canAddInputPorts"),
            (json!({ "kind": "custom_inputs_fit", "oneOf": ["Json"] }), "canAddInputPorts"),
            (json!({ "kind": "custom_outputs_declared" }), "canAddOutputPorts"),
        ] {
            let mut m = metadata_with(vec![]);
            m.validate.push(
                serde_json::from_value(json!({ "when": { "kind": "not", "of": condition }, "then": { "message": "m" } }))
                    .expect("a rule"),
            );
            assert!(m.validate_semantics().unwrap_err().contains(feature), "{condition}");
            m.features.can_add_input_ports = true;
            m.features.can_add_output_ports = true;
            m.validate_semantics().expect("a node that takes added ports");
        }
    }

    /// The wire shape of a metadata port and the two features, as a
    /// `metadata.json` spells them.
    #[test]
    fn metadata_ports_and_the_pairing_features_round_trip_their_json() {
        let written = json!({
            "type": "Pick",
            "label": "Pick",
            "description": "d",
            "metadataPorts": [{ "name": "tool", "type": "{name: String}", "description": "what", "value": { "name": "x" } }],
            "features": { "canAddInputPorts": true, "canAddOutputPorts": true, "customOutputType": "T", "pairsAddedPorts": true }
        });
        let meta: NodeMetadata = serde_json::from_value(written.clone()).expect("parses");
        assert_eq!(meta.metadata_ports[0].value, Some(json!({ "name": "x" })));
        assert!(meta.features.pairs_added_ports);
        assert_eq!(meta.features.custom_output_type, Some(WeftType::type_var("T")));
        let back = serde_json::to_value(&meta).expect("serializes");
        assert_eq!(back["metadataPorts"], written["metadataPorts"]);
        assert_eq!(back["features"]["customOutputType"], json!("T"));
        assert_eq!(back["features"]["pairsAddedPorts"], json!(true));
        // A key a metadata port does not have is a typo, refused.
        let typo = json!({ "type": "P", "label": "P", "description": "d", "metadataPorts": [{ "name": "t", "type": "String", "valu": "x" }] });
        assert!(serde_json::from_value::<NodeMetadata>(typo).is_err());
    }

    /// Only an infra node's output is made when its infra is applied, so
    /// only one may be baked.
    #[test]
    fn only_an_infra_nodes_output_may_be_baked() {
        let mut m = metadata_with(vec![]);
        m.outputs.push(OutputSpec { name: "access".into(), port_type: WeftType::Access, description: None, baked: true, narrowable: None });
        assert!(m.validate_semantics().unwrap_err().contains("only an infra node's outputs are"));
        m.requires_infra = true;
        m.validate_semantics().expect("an infra node bakes");
        let back: OutputSpec = serde_json::from_value(serde_json::json!({ "name": "a", "type": "Access", "baked": true })).unwrap();
        assert!(back.baked);
        let plain: OutputSpec = serde_json::from_value(serde_json::json!({ "name": "a", "type": "Access" })).unwrap();
        assert!(!plain.baked);
        assert!(serde_json::to_value(&plain).unwrap().get("baked").is_none(), "an output that is not baked says nothing");
    }

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

    /// The language owns how runs are kept and the entry limits: every
    /// trigger gets the per-minute and at-once limits and the durable and
    /// recorded switches, one called from outside also the per-caller
    /// limit, a non-live one also the long-runs switch, a plain node none
    /// of them. A trigger declaring a name it receives is refused; a plain
    /// node may use those names for its own inputs.
    #[test]
    fn a_trigger_gets_the_settings_the_language_owns() {
        let names = |m: &NodeMetadata| m.inputs.iter().map(|i| i.name.clone()).collect::<Vec<_>>();
        let mut plain = metadata_with(vec![]);
        plain.add_language_ports().unwrap();
        assert!(plain.inputs.is_empty());

        let mut trigger = metadata_with(vec![]);
        trigger.features.is_trigger = true;
        trigger.add_language_ports().unwrap();
        // Nobody calls a schedule: no per-caller limit.
        let settings = || crate::run_settings::RunSettings::node_inputs(false).into_iter().map(|i| i.name);
        let mut expected: Vec<String> = crate::signal::EntryLimits::NODE_FIELDS[1..].iter().map(|s| s.to_string()).collect();
        expected.extend(settings());
        assert_eq!(names(&trigger), expected);
        trigger.validate_semantics().expect("the added inputs pass the semantic rules");

        let mut form = metadata_with(vec![]);
        form.features.is_trigger = true;
        form.features.called_from_outside = true;
        form.add_language_ports().unwrap();
        let mut expected: Vec<String> = crate::signal::EntryLimits::NODE_FIELDS.iter().map(|s| s.to_string()).collect();
        expected.extend(settings());
        assert_eq!(names(&form), expected);

        let mut both = metadata_with(vec![]);
        both.features.is_trigger = true;
        both.features.called_from_outside = true;
        both.features.live_connection = Some(LiveWire::Http);
        assert!(both.add_language_ports().unwrap_err().contains("calledFromOutside"));

        let mut live = metadata_with(vec![]);
        live.features.is_trigger = true;
        live.features.live_connection = Some(LiveWire::Http);
        live.add_language_ports().unwrap();
        let mut expected: Vec<String> = crate::signal::EntryLimits::NODE_FIELDS.iter().map(|s| s.to_string()).collect();
        // A trigger holding a caller also gets whether its run may outlive them.
        expected.extend(crate::run_settings::RunSettings::node_inputs(true).into_iter().map(|i| i.name));
        // And the journal settings of the conversation it holds.
        let journal = || crate::stream_journal::JournalPolicy::node_inputs().into_iter().map(|i| i.name);
        expected.extend(journal());
        assert_eq!(names(&live), expected);
        live.validate_semantics().expect("the added inputs pass the semantic rules");

        // A node that opens a bus its users tune gets the journal settings
        // and nothing else; a live trigger has them already and says so.
        let mut opens_bus = metadata_with(vec![]);
        opens_bus.features.journal_settings = true;
        opens_bus.add_language_ports().unwrap();
        assert_eq!(names(&opens_bus), journal().collect::<Vec<_>>());
        opens_bus.validate_semantics().expect("the journal settings pass the semantic rules");
        let mut twice = metadata_with(vec![]);
        twice.features.is_trigger = true;
        twice.features.live_connection = Some(LiveWire::Websocket);
        twice.features.journal_settings = true;
        assert!(twice.add_language_ports().unwrap_err().contains("journalSettings"));
        let own = crate::stream_journal::JournalPolicy::node_inputs().remove(0);
        let mut declares_journal = metadata_with(vec![own]);
        declares_journal.features.journal_settings = true;
        let e = declares_journal.add_language_ports().unwrap_err();
        assert!(e.contains("journalEphemeral") && e.contains("remove it"), "{e}");

        let mut not_a_trigger = metadata_with(vec![]);
        not_a_trigger.features.live_connection = Some(LiveWire::Http);
        assert!(not_a_trigger.add_language_ports().is_err());
        let mut not_a_trigger = metadata_with(vec![]);
        not_a_trigger.features.called_from_outside = true;
        assert!(not_a_trigger.add_language_ports().is_err());

        let durable = || crate::run_settings::RunSettings::node_inputs(false).remove(0);
        let mut declares_it = metadata_with(vec![durable()]);
        declares_it.features.is_trigger = true;
        let e = declares_it.add_language_ports().unwrap_err();
        assert!(e.contains("durable"), "{e}");

        let mut declares_a_limit = metadata_with(vec![input("callsAtOnce", WeftType::primitive(WeftPrimitive::Number))]);
        declares_a_limit.features.is_trigger = true;
        declares_a_limit.features.live_connection = Some(LiveWire::Http);
        let e = declares_a_limit.add_language_ports().unwrap_err();
        assert!(e.contains("callsAtOnce"), "{e}");

        // A rate limiter is no trigger: its own `callsPerMinute` and
        // `durable` inputs are its own.
        let own = vec![input("callsPerMinute", WeftType::primitive(WeftPrimitive::Number)), durable()];
        let mut limiter = metadata_with(own.clone());
        limiter.add_language_ports().expect("a plain node keeps its own names");
        assert_eq!(limiter.inputs.len(), own.len());
    }

    /// Every node gets the language's `_error` output, a trigger and a
    /// node that only shapes values alike; a node declaring it itself is
    /// refused, and `error` is an ordinary name a node may use.
    #[test]
    fn every_node_gets_the_error_output() {
        let output = |name: &str| OutputSpec {
            name: name.into(),
            port_type: WeftType::primitive(WeftPrimitive::String),
            description: None,
            baked: false,
            narrowable: None,
        };
        let mut plain = metadata_with(vec![]);
        plain.add_language_ports().unwrap();
        assert_eq!(plain.outputs.len(), 1);
        assert_eq!(plain.outputs[0].name, ERROR_PORT);
        assert_eq!(plain.outputs[0].port_type, WeftType::primitive(WeftPrimitive::String));
        plain.validate_semantics().expect("the added output passes the semantic rules");

        let mut trigger = metadata_with(vec![]);
        trigger.features.is_trigger = true;
        trigger.add_language_ports().unwrap();
        assert_eq!(trigger.outputs.iter().filter(|o| o.name == ERROR_PORT).count(), 1);

        let mut declares_it = metadata_with(vec![]);
        declares_it.outputs.push(output(ERROR_PORT));
        let e = declares_it.add_language_ports().unwrap_err();
        assert!(e.contains(ERROR_PORT) && e.contains("every node"), "{e}");

        let mut own_error = metadata_with(vec![]);
        own_error.outputs.push(output("error"));
        own_error.add_language_ports().expect("`error` is the node's own name");
        let names: Vec<&str> = own_error.outputs.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(names, ["error", ERROR_PORT]);
    }

    /// A fire payload is a trigger's, and a type it cannot read fails the
    /// load: a typo never waits for the first firing to show.
    #[test]
    fn fires_with_is_a_triggers_and_its_types_are_read_at_load() {
        let mut plain = metadata_with(vec![]);
        plain.fires_with.insert("when".into(), "String".into());
        assert!(plain.validate_semantics().unwrap_err().contains("only a trigger"));

        let mut trigger = metadata_with(vec![]);
        trigger.features.is_trigger = true;
        trigger.fires_with.insert("when".into(), "Strng".into());
        assert!(trigger.validate_semantics().unwrap_err().contains("firesWith field `when` is not a weft type"));

        trigger.fires_with.insert("when".into(), "String".into());
        trigger.validate_semantics().expect("a readable payload on a trigger loads");
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

        // A default on the list would derive ports the source never wrote.
        let mut defaulted = declared.clone();
        defaulted.inputs[0].default = Some(serde_json::json!([{ "kind": "case", "port": "a" }]));
        let e = defaulted.validate_semantics().unwrap_err();
        assert!(e.contains("declares a default"), "{e}");
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

        // No specs at all: no entry could ever add a port, and the
        // editor would render an empty kind dropdown.
        let mut none = base();
        none.ports_from_config = Some(PortsFromConfig {
            field: "cases".into(),
            match_input: None,
            specs: vec![],
        });
        assert!(none.validate_semantics().unwrap_err().contains("declares no specs"));

        // A spec field named after `kind` or the key field would
        // overwrite the entry's kind / validated port name at commit.
        for reserved in ["kind", "port"] {
            let mut clash = base();
            let mut s = spec("case", vec![PortTemplate::new("{key}", "Boolean")]);
            s.fields = vec![SpecField::named(reserved)];
            clash.ports_from_config = Some(PortsFromConfig {
                field: "cases".into(),
                match_input: None,
                specs: vec![s],
            });
            assert!(
                clash.validate_semantics().unwrap_err().contains("declares a field named"),
                "field key '{reserved}' must be refused"
            );
        }

        // Two spec fields on one key: last one wins in the entry map
        // and the editor draws two controls bound to one value.
        let mut dup = base();
        let mut s = spec("case", vec![PortTemplate::new("{key}", "Boolean")]);
        s.fields = vec![SpecField::named("value"), SpecField::named("value")];
        dup.ports_from_config = Some(PortsFromConfig {
            field: "cases".into(),
            match_input: None,
            specs: vec![s],
        });
        assert!(dup.validate_semantics().unwrap_err().contains("declares the field 'value' twice"));
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
            description: None,
            baked: false,
            narrowable: None,
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
            accepts: None,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            description: None,
            requires_scopes: None,
            requires_values: None,
        }
    }

    /// The key that once made an optional input required when wired is
    /// refused, naming that an optional input is that already.
    #[test]
    fn required_when_wired_is_refused_as_removed() {
        let doc = json!({ "type": "T", "label": "T", "description": "",
            "inputs": [{ "name": "instance", "type": "String", "requiredWhenWired": true }] });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.contains("requiredWhenWired") && e.contains("required once wired"), "{e}");
    }

    /// Every input takes both drivers unless something narrows it: the
    /// author's list, a live-handle type, or the compiler-read fixed rule.
    #[test]
    fn accepts_comes_from_the_rule_then_the_author_then_the_type() {
        let image = input("image", WeftType::primitive(WeftPrimitive::Image));
        let text = input("prompt", WeftType::primitive(WeftPrimitive::String));
        let bus = input("bus", WeftType::Bus);
        let stream = input("rows", WeftType::parse("Generator[Number]").unwrap());
        assert_eq!(image.effective_accepts(false), Accepts::both(), "a file input takes a marker or a wire");
        assert_eq!(text.effective_accepts(false), Accepts::both());
        assert_eq!(bus.effective_accepts(false), Accepts::wire_only(), "a bus never takes a literal");
        assert_eq!(stream.effective_accepts(false), Accepts::wire_only());

        let mut closed = text;
        closed.accepts = Some(Accepts::wire_only());
        assert_eq!(closed.effective_accepts(false), Accepts::wire_only(), "the author narrows");
        assert_eq!(closed.effective_accepts(true), Accepts::literal_only(), "the fixed rule wins");
    }

    /// The wire form is the list; an empty or repeated list is refused.
    #[test]
    fn accepts_round_trips_as_a_list() {
        let both: Accepts = serde_json::from_value(json!(["literal", "wire"])).unwrap();
        assert_eq!(both, Accepts::both());
        assert_eq!(serde_json::to_value(Accepts::wire_only()).unwrap(), json!(["wire"]));
        assert!(serde_json::from_value::<Accepts>(json!([])).is_err());
        assert!(serde_json::from_value::<Accepts>(json!(["wire", "wire"])).is_err());
        assert!(serde_json::from_value::<Accepts>(json!(["config"])).is_err());
    }

    /// A number box nothing could satisfy is the node's own metadata
    /// being wrong, and it is refused when the node loads rather than
    /// when somebody's run happens to route a number through it.
    #[test]
    fn a_number_box_no_value_could_satisfy_is_refused_at_load() {
        for (widget, wanted) in [
            (serde_json::json!({ "kind": "number", "min": 5, "max": 1 }), "minimum of 5 above"),
            (serde_json::json!({ "kind": "number", "step": 0 }), "step of 0"),
        ] {
            let mut n = input("count", WeftType::primitive(WeftPrimitive::Number));
            n.widget = Some(serde_json::from_value(widget).unwrap());
            let e = metadata_with(vec![n]).validate_semantics().unwrap_err();
            assert!(e.contains("count") && e.contains(wanted), "{e}");
        }
    }

    /// And a default outside its own box is refused the same way, with
    /// the message reading as one sentence.
    #[test]
    fn a_default_outside_its_own_number_box_is_refused() {
        let mut n = input("count", WeftType::primitive(WeftPrimitive::Number));
        n.widget = Some(serde_json::from_value(
            serde_json::json!({ "kind": "number", "min": 1, "max": 8, "step": 1 }),
        ).unwrap());
        n.default = Some(serde_json::json!(0));
        let e = metadata_with(vec![n]).validate_semantics().unwrap_err();
        assert!(e.contains("its default 0 is outside the allowed range [1, 8]"), "{e}");
    }

    /// A handle port may not list `literal`; a compiler-read port may
    /// carry nothing but the fixed rule's own list.
    #[test]
    fn validate_semantics_holds_the_two_fixed_accepts() {
        let mut bus = input("bus", WeftType::Bus);
        bus.accepts = Some(Accepts::both());
        let e = metadata_with(vec![bus]).validate_semantics().unwrap_err();
        assert!(e.contains("may only list \"wire\""), "{e}");

        let mut fields = input("fields", WeftType::parse("List[JsonDict]").unwrap());
        fields.accepts = Some(Accepts::both());
        let mut m = metadata_with(vec![fields]);
        m.ports_from_config = Some(
            serde_json::from_value(json!({ "field": "fields", "specs": [
                { "kind": "text", "label": "Text", "render": { "component": "text_input" },
                  "addsOutputs": [{ "nameTemplate": "{key}", "portType": "String" }] }
            ] })).unwrap(),
        );
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("read by the compiler"), "{e}");
        m.inputs[0].accepts = Some(Accepts::literal_only());
        assert!(m.validate_semantics().is_ok(), "the fixed rule's own list is fine");
        let resolved = m.resolved();
        assert_eq!(resolved.inputs[0].accepts, Some(Accepts::literal_only()));
        assert!(resolved.validate_semantics().is_ok(), "a resolved document re-validates");
    }

    /// Every dropped metadata key is refused naming what replaced it:
    /// `exposure` (now `accepts`), `features.isOutputDefault` (every
    /// reached node runs; `--target` narrows), `required` on an output
    /// (outputs carry no optionality).
    #[test]
    fn removed_metadata_keys_are_refused_naming_the_replacement() {
        let doc = json!({ "type": "T", "label": "T", "description": "",
            "inputs": [{ "name": "x", "type": "String", "exposure": "wire" }] });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.contains("`accepts`") && e.contains("[\"wire\"]"), "{e}");

        let doc = json!({ "type": "T", "label": "T", "description": "",
            "features": { "isOutputDefault": true } });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.contains("isOutputDefault") && e.contains("--target"), "{e}");

        let doc = json!({ "type": "T", "label": "T", "description": "",
            "features": { "catchErrors": true } });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.contains("catchErrors") && e.contains(ERROR_PORT), "{e}");

        let doc = json!({ "type": "T", "label": "T", "description": "",
            "outputs": [{ "name": "out", "type": "String", "required": false }] });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.contains("output 'out'") && e.contains("no optionality"), "{e}");

        let clean = json!({ "type": "T", "label": "T", "description": "",
            "inputs": [{ "name": "x", "type": "String", "accepts": ["wire"] }],
            "outputs": [{ "name": "out", "type": "String" }] });
        assert!(refuse_removed_metadata_keys(&clean).is_ok());
    }

    /// A key that became camelCase is refused naming its new spelling and
    /// where it sits, every one at once; the same word as DATA (an
    /// input's default, a provider's parameter) is left alone.
    #[test]
    fn renamed_metadata_keys_are_refused_naming_the_new_spelling() {
        let doc = json!({ "type": "T", "label": "T", "description": "",
            "requires_infra": true,
            "inputs": [{ "name": "x", "type": "String", "free_text": true }] });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert_eq!(
            e,
            "`free_text` (under `inputs.[]`) is spelled `freeText` now; `requires_infra` is spelled \
             `requiresInfra` now: every metadata.json key is camelCase, and the old spelling is no \
             longer read. Rename them"
        );

        let doc = json!({ "service": { "acquisition": { "kind": "oauth2", "token_url": "u" } } });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.starts_with("`token_url` (under `service.acquisition`) is spelled `tokenUrl` now:"), "{e}");

        let data = json!({ "type": "T", "label": "T", "description": "",
            "inputs": [{ "name": "x", "type": "JsonDict", "default": { "free_text": 1 } }],
            "service": { "acquisition": { "authParams": { "access_key_id": "a" } } },
            "firesWith": { "own_page": "String" } });
        assert!(refuse_removed_metadata_keys(&data).is_ok());

        for (old, new) in RENAMED_METADATA_KEYS {
            assert_eq!(crate::casing::Casing::Camel.spell(old), *new, "{old}");
        }
    }

    /// A node's own verify block still written the old way is refused
    /// naming the keys, where they sit, and the block written the new way.
    #[test]
    fn an_old_verify_block_is_refused_with_its_new_shape() {
        let old = json!({ "kind": "hmac", "signatureHeader": "X-Slack-Signature", "timestampHeader": "X-Slack-Request-Timestamp",
                          "prefix": "v0=", "concat": "v0:{timestamp}:{body}" });
        let gate = json!({ "type": "T", "label": "T", "description": "", "service": { "verify": old } });
        let e = refuse_removed_metadata_keys(&gate).unwrap_err();
        assert!(e.starts_with("`service.verify` sets `signatureHeader`, `timestampHeader`, `prefix`"), "{e}");
        assert!(e.contains(r#""in":"header:X-Slack-Signature""#), "{e}");

        let push = json!({ "type": "T", "label": "T", "description": "",
            "service": { "events": { "messages": { "webhook": { "verify": { "kind": "hmac", "signatureHeader": "S",
                "packed": { "timestamp": "t", "signature": "v1" }, "concat": "{timestamp}.{body}" } } } } } });
        let e = refuse_removed_metadata_keys(&push).unwrap_err();
        assert!(e.starts_with("`service.events.messages.webhook.verify` sets `signatureHeader`, `packed`"), "{e}");
        assert!(e.contains(r#""in":"signature:t""#), "{e}");

        let new = json!({ "type": "T", "label": "T", "description": "",
            "service": { "verify": { "kind": "hmac", "signature": { "in": "header:S" }, "concat": "{body}" } } });
        assert!(refuse_removed_metadata_keys(&new).is_ok());
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

    /// A code widget's language is a closed set spelled in lowercase: the
    /// shipped words load, anything else fails the metadata load loudly.
    #[test]
    fn code_widget_language_is_a_closed_lowercase_set() {
        for (word, lang) in [
            ("python", CodeLanguage::Python),
            ("javascript", CodeLanguage::Javascript),
            ("sql", CodeLanguage::Sql),
            ("json", CodeLanguage::Json),
        ] {
            let raw = serde_json::json!({ "kind": "code", "language": word });
            let w: Widget = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(w, Widget::Code { language: lang });
            assert_eq!(serde_json::to_value(&w).unwrap(), raw);
        }
        for bad in ["rust", "Python", ""] {
            let raw = serde_json::json!({ "kind": "code", "language": bad });
            let err = serde_json::from_value::<Widget>(raw).unwrap_err().to_string();
            assert!(err.contains("unknown variant"), "{bad}: {err}");
        }
        let missing = serde_json::json!({ "kind": "code" });
        assert!(serde_json::from_value::<Widget>(missing).is_err());
    }

    /// A JSON code editor edits a structured value as JSON text, so it
    /// fits a JsonDict input; the other languages edit text and stay on
    /// String inputs.
    #[test]
    fn a_json_code_widget_fits_a_structured_input() {
        let with = |ty: WeftType, language: CodeLanguage| {
            let mut n = input("schema", ty);
            n.widget = Some(Widget::Code { language });
            metadata_with(vec![n]).validate_semantics()
        };
        assert!(with(WeftType::JsonDict, CodeLanguage::Json).is_ok());
        assert!(with(WeftType::primitive(WeftPrimitive::String), CodeLanguage::Python).is_ok());
        let e = with(WeftType::JsonDict, CodeLanguage::Python).unwrap_err();
        assert!(e.contains("code widget edits"), "{e}");
    }

    /// `kind_name` hands back exactly the serde `kind` tag for every
    /// variant: the tag is the one source of truth and this pins the
    /// convenience match to it.
    #[test]
    fn kind_name_matches_the_serde_tag() {
        let all = [
            Widget::Text,
            Widget::Textarea,
            Widget::Code { language: CodeLanguage::Python },
            Widget::Number { min: None, max: None, step: None },
            Widget::Checkbox,
            Widget::Datetime,
            Widget::Select { options: vec!["a".into()], free_text: false },
            Widget::Multiselect { options: vec!["a".into()], free_text: false },
            Widget::Password,
            Widget::Access { service: None, optional: false },
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
                description: None,
                baked: false,
                narrowable: None,
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
    /// its declared type must be Access (the marker the node reads), and
    /// the picker is compiler-read (the fixed rule, no `accepts`).
    #[test]
    fn validate_semantics_types_the_access_widget_input() {
        let mut n = input("account", WeftType::Access);
        n.widget = Some(Widget::Access { service: None, optional: false });
        let m = metadata_with(vec![n]);
        assert!(m.is_compiler_read("account"), "the picker is compiler-read");
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("service"), "{e}");

        // With the service recipe present: an Access-typed input
        // passes, a JsonDict-typed one is rejected by the widget's
        // value-shape check (the value the node reads is the Access
        // marker, which JsonDict cannot hold).
        let with_service = |ty: WeftType| -> NodeMetadata {
            let mut n = input("account", ty);
            n.widget = Some(Widget::Access { service: None, optional: false });
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

    /// A list source's refusal paths are checked at load like every
    /// declared call's: an empty `errorPath` would refuse every answer
    /// whole, so it is refused, on an input and on an entry kind's field
    /// alike. A `get` that does not read is refused too.
    #[test]
    fn validate_semantics_refuses_a_list_source_that_cannot_read_its_answer() {
        let list = |extra: Value| -> ResourceSource {
            let mut raw = json!({ "kind": "list", "get": "https://x.example/items", "items": "i",
                                  "label": "name", "value": "id" });
            raw.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            serde_json::from_value(raw).unwrap()
        };
        let account = input("account", WeftType::Access);
        let with = |source: ResourceSource| {
            let mut field = input("channel", WeftType::primitive(WeftPrimitive::String));
            field.widget = Some(Widget::RemoteSelect {
                access: "account".into(),
                sources: vec![source],
                depends_on: vec![],
                free_text: false,
            });
            metadata_with(vec![account.clone(), field]).validate_semantics()
        };
        let e = with(list(json!({ "errorPath": "" }))).unwrap_err();
        assert!(e.contains("input 'channel': list source.errorPath is empty"), "{e}");
        let e = with(list(json!({ "okPath": " " }))).unwrap_err();
        assert!(e.contains("okPath is empty"), "{e}");
        let e = with(list(json!({ "get": "https://x.example/{team" }))).unwrap_err();
        assert!(e.contains("unclosed"), "{e}");
        with(list(json!({ "okPath": "ok", "errorPath": "error" }))).expect("declared paths load");

        let mut m = metadata_with(vec![input("rules", WeftType::primitive(WeftPrimitive::String)), account.clone()]);
        m.ports_from_config = Some(
            serde_json::from_value(json!({
                "field": "rules",
                "specs": [{
                    "kind": "pick", "keyField": "key",
                    "fields": [
                        { "key": "target", "required": true, "shape": "typed", "valueType": "String",
                          "widget": { "kind": "remote_select", "access": "account",
                                      "sources": [ { "kind": "list", "get": "u", "items": "i", "label": "name",
                                                     "value": "id", "errorPath": "" } ] } }
                    ],
                    "addsOutputs": [ { "nameTemplate": "{key}", "portType": "String" } ]
                }]
            }))
            .unwrap(),
        );
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("spec 'pick' field 'target': list source.errorPath is empty"), "{e}");
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

    /// `resolved()` fills every input's accepts + widget with the
    /// effective values and is idempotent.
    #[test]
    fn resolved_fills_effective_values_idempotently() {
        let meta = metadata_with(vec![input("prompt", WeftType::primitive(WeftPrimitive::String))]);
        let resolved = meta.resolved();
        assert_eq!(resolved.inputs[0].accepts, Some(Accepts::both()));
        assert_eq!(resolved.inputs[0].widget, Some(Widget::Text));
        let again = resolved.resolved();
        assert_eq!(again.inputs[0].accepts, resolved.inputs[0].accepts);
        assert_eq!(again.inputs[0].widget, resolved.inputs[0].widget);
    }

    /// A node's metadata from its JSON, the way a `metadata.json` reads.
    fn metadata_json(extra: serde_json::Value) -> NodeMetadata {
        let mut doc = json!({ "type": "N", "label": "N", "description": "" });
        doc.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        serde_json::from_value(doc).expect("the metadata parses")
    }

    fn ty(s: &str) -> WeftType {
        WeftType::parse(s).unwrap_or_else(|| panic!("{s} parses"))
    }

    #[test]
    fn a_narrowable_output_narrows_within_its_type_or_a_wider_bound() {
        let m = metadata_json(json!({ "outputs": [{ "name": "response", "type": "JsonDict", "narrowable": true }] }));
        m.validate_semantics().expect("narrowable: true loads");

        // A rule naming an output the node lacks is refused at load.
        let m = metadata_json(json!({
            "outputs": [{ "name": "body", "type": "JsonDict", "narrowable": true }],
            "validate": [{
                "when": { "kind": "not", "of": { "kind": "output_type_fits", "port": "bdy", "within": "String" } },
                "then": { "message": "x" }
            }],
        }));
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("names output 'bdy'"), "{e}");
        assert_eq!(m.outputs[0].narrowing_bound(), Some(&WeftType::JsonDict));

        let m = metadata_json(json!({ "outputs": [
            { "name": "body", "type": "JsonDict", "narrowable": "JsonDict | String" }
        ] }));
        m.validate_semantics().expect("a wider bound loads");
        assert_eq!(m.outputs[0].narrowing_bound(), Some(&ty("JsonDict | String")));

        let m = metadata_json(json!({ "outputs": [{ "name": "body", "type": "JsonDict", "narrowable": "String" }] }));
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("output 'body'") && e.contains("does not fit in"), "{e}");

        let parse = |narrowable: serde_json::Value| {
            let doc = json!({ "type": "N", "label": "N", "description": "",
                "outputs": [{ "name": "body", "type": "JsonDict", "narrowable": narrowable }] });
            serde_json::from_value::<NodeMetadata>(doc).unwrap_err().to_string()
        };
        let e = parse(json!(false));
        assert!(e.contains("\"narrowable\": false"), "{e}");
        // A typo in the bound names the unknown name, not "no variant matched".
        let e = parse(json!("JsonDic | String"));
        assert!(e.contains("narrowable bound") && e.contains("`JsonDic`"), "{e}");
        let e = parse(json!(3));
        assert!(e.contains("narrowable is `true` or the type"), "{e}");

        let m = metadata_json(json!({
            "inputs": [{ "name": "value", "type": "T" }],
            "outputs": [{ "name": "value", "type": "T", "narrowable": true }],
        }));
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("output 'value' is narrowable") && e.contains("type variable"), "{e}");
    }

    #[test]
    fn type_params_refuse_an_unused_variable_and_a_cycle() {
        let m = metadata_json(json!({
            "typeParams": { "T": "Media" },
            "inputs": [{ "name": "file", "type": "Image" }],
        }));
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("typeParams") && e.contains("`T`") && e.contains("no port uses it"), "{e}");

        let m = metadata_json(json!({
            "typeParams": { "T1": "List[T2]", "T2": "List[T1]" },
            "inputs": [{ "name": "a", "type": "T1" }, { "name": "b", "type": "T2" }],
        }));
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("typeParams") && e.contains("cycle"), "{e}");

        let m = metadata_json(json!({
            "typeParams": { "T": "File" },
            "inputs": [{ "name": "file", "type": "T" }],
            "outputs": [{ "name": "file", "type": "T" }],
        }));
        m.validate_semantics().expect("a used, acyclic bound loads");

        // A bound waiting on a variable nothing ever resolves checks nothing.
        let m = metadata_json(json!({
            "typeParams": { "T": "List[T2]" },
            "inputs": [{ "name": "value", "type": "T" }],
            "outputs": [{ "name": "value", "type": "T" }],
        }));
        let e = m.validate_semantics().unwrap_err();
        assert!(e.contains("typeParams") && e.contains("names `T2`, which no port uses"), "{e}");
    }

    #[test]
    fn placeholders_are_inputs_is_held_to_the_node() {
        let query = json!([{ "name": "query", "type": "String" }]);
        let adds = json!({ "canAddInputPorts": true });
        let good = json!({ "in": "query", "pattern": "\\$(?<name>[a-zA-Z_][a-zA-Z0-9_]*)" });
        metadata_json(json!({ "inputs": query, "features": adds, "placeholdersAreInputs": good }))
            .validate_semantics()
            .expect("a text input, a capturing pattern and added inputs load");

        let e = metadata_json(json!({
            "inputs": query, "features": adds,
            "placeholdersAreInputs": { "in": "sql", "pattern": "\\$(?<name>[a-z]+)" },
        }))
        .validate_semantics()
        .unwrap_err();
        assert!(e.contains("input 'sql'") && e.contains("does not declare"), "{e}");

        let e = metadata_json(json!({
            "inputs": query, "features": adds,
            "placeholdersAreInputs": { "in": "query", "pattern": "\\$([a-z]+)" },
        }))
        .validate_semantics()
        .unwrap_err();
        assert!(e.contains("no group named `name`"), "{e}");

        // A back-reference skips a span to its own closing delimiter.
        let tagged = PlaceholdersAreInputs {
            setting: "query".into(),
            pattern: "\\$(?<tag>\\w*)\\$.*?\\$\\k<tag>\\$|\\$(?<name>[a-z]+)".into(),
        };
        let regex = tagged.regex().expect("a back-reference compiles");
        assert_eq!(tagged.names_in(&regex, "$f$ $g$ $no $g$ $f$ $yes").unwrap(), vec!["yes".to_string()]);

        let e = metadata_json(json!({ "inputs": query, "placeholdersAreInputs": good }))
            .validate_semantics()
            .unwrap_err();
        assert!(e.contains("canAddInputPorts"), "{e}");
    }

    #[test]
    fn optional_custom_inputs_is_refused_naming_join() {
        let doc = json!({ "type": "T", "label": "T", "description": "",
            "features": { "optionalCustomInputs": true } });
        let e = refuse_removed_metadata_keys(&doc).unwrap_err();
        assert!(e.contains("optionalCustomInputs") && e.contains("`features.join`"), "{e}");
    }

    /// What a program may declare on an output: a narrowable one takes
    /// anything fitting its bound, a type variable takes anything it is
    /// compatible with, every other output only its own type.
    #[test]
    fn output_declaration_fits_follows_the_output_kind() {
        let m = metadata_json(json!({ "outputs": [
            { "name": "response", "type": "JsonDict", "narrowable": true },
            { "name": "text", "type": "String" },
            { "name": "value", "type": "T" },
        ], "inputs": [{ "name": "value", "type": "T" }] }));
        let json_dict = WeftType::JsonDict;
        m.output_declaration_fits("response", &json_dict, &ty("{ verdict: String, score: Number }"))
            .expect("a record fits JsonDict");
        let e = m.output_declaration_fits("response", &json_dict, &ty("String")).unwrap_err();
        assert!(e.contains("does not fit in JsonDict"), "{e}");

        let string = ty("String");
        m.output_declaration_fits("text", &string, &string).expect("its own type is restated");
        let e = m.output_declaration_fits("text", &string, &ty("String | Null")).unwrap_err();
        assert!(e.contains("narrowable"), "{e}");

        let t = ty("T");
        m.output_declaration_fits("value", &t, &ty("{ a: Number }")).expect("a T output is pinned by any type");
        m.output_declaration_fits("value", &ty("List[T]"), &ty("List[Image]")).expect("compatible with List[T]");
        let e = m.output_declaration_fits("value", &ty("List[T]"), &ty("Number")).unwrap_err();
        assert!(e.contains("does not fit in its type"), "{e}");
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
    list: Option<&Value>,
    ports_from_config: &PortsFromConfig,
) -> (Vec<InputDefinition>, Vec<PortDefinition>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let Some(entries) = list.and_then(|f| f.as_array()) else {
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
            inputs.push(InputDefinition::from_wire_port(materialize_port(port, key)));
        }
        for port in &spec.adds_outputs {
            outputs.push(materialize_port(port, key));
        }
    }
    (inputs, outputs)
}

/// The metadata sentinel meaning "this port's own type variable": the
/// materializer turns it into `T__in__{key}`, one variable per port, so
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
        WeftType::TypeVar(n) if n == AUTO_TYPE_VAR => WeftType::port_var(crate::weft_type::PortSide::Input, key),
        WeftType::List(inner) => WeftType::List(Box::new(materialize_auto_type_vars(inner, key))),
        WeftType::Generator(inner) => {
            WeftType::Generator(Box::new(materialize_auto_type_vars(inner, key)))
        }
        WeftType::Secret(inner) => WeftType::Secret(Box::new(materialize_auto_type_vars(inner, key))),
        WeftType::Dict(v) => WeftType::Dict(Box::new(materialize_auto_type_vars(v, key))),
        WeftType::Union(members) => {
            WeftType::Union(members.iter().map(|m| materialize_auto_type_vars(m, key)).collect())
        }
        WeftType::Record { fields, open } => WeftType::Record {
            fields: fields
                .iter()
                .map(|f| crate::weft_type::RecordField {
                    name: f.name.clone(),
                    ty: materialize_auto_type_vars(&f.ty, key),
                    optional: f.optional,
                })
                .collect(),
            open: *open,
        },
        // No `Named` arm: a declared body is concrete by construction
        // (the registry and the wire parser both refuse a type
        // variable inside one), so there is never a `T_Auto` to
        // materialize beneath an alias, and rebuilding the body here
        // would be the door to two same-named types with different
        // bodies.
        other => other.clone(),
    }
}

fn materialize_port(template: &PortTemplate, key: &str) -> PortDefinition {
    let name = template.resolve_name(key);
    let port_type = materialize_auto_type_vars(&template.port_type, key);
    PortDefinition {
        name,
        port_type,
        // A created input is required (the node asked for it); an output
        // carries no optionality, and `true` is the one value it takes.
        required: true,
        description: None,
        synthesized_from_carry: false,
        declared_type: None,
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

#[cfg(test)]
mod compact_view_tests {
    use super::*;
    use serde_json::json;

    /// The keep/drop lists must cover the WHOLE top-level schema, both
    /// directions: a struct key nobody classified is schema growth
    /// nobody decided about, and a classified key the struct no longer
    /// has is a stale list entry. Keys that serialize only when
    /// non-empty are absent from a minimal parse, so they are pinned
    /// by hand here; the stdlib census in weft-catalog is the net that
    /// catches one of those being added and used before this list
    /// hears of it.
    #[test]
    fn every_metadata_top_level_key_is_classified() {
        let minimal: NodeMetadata =
            serde_json::from_str(r#"{ "type": "T", "label": "", "description": "" }"#)
                .unwrap();
        let serialized = serde_json::to_value(&minimal).unwrap();
        let mut observed: Vec<&str> = serialized
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        const SERIALIZED_WHEN_NON_EMPTY: [&str; 13] = [
            "metadataPorts",
            "images",
            "publishes",
            "display",
            "validate",
            "portsFromConfig",
            "service",
            "accessApps",
            "types",
            "firesWith",
            "claimsRoute",
            "typeParams",
            "placeholdersAreInputs",
        ];
        observed.extend(SERIALIZED_WHEN_NON_EMPTY);

        let classified =
            |key: &str| COMPACT_KEEP_TOP_LEVEL.contains(&key) || COMPACT_DROP_TOP_LEVEL.contains(&key);
        for key in &observed {
            assert!(classified(key), "unclassified top-level metadata key: `{key}`");
        }
        for key in COMPACT_KEEP_TOP_LEVEL {
            assert!(
                observed.contains(&key),
                "keep-list names a key the schema does not have: `{key}`"
            );
            assert!(
                !COMPACT_DROP_TOP_LEVEL.contains(&key),
                "`{key}` is both kept and dropped"
            );
        }
        for key in COMPACT_DROP_TOP_LEVEL {
            assert!(
                observed.contains(&key),
                "drop-list names a key the schema does not have: `{key}`"
            );
        }
    }

    /// A select with more options than anyone reads at a glance is cut
    /// to a sample and a count in the compact view; a short one and the
    /// plain metadata keep every option.
    #[test]
    fn compact_json_cuts_a_long_options_list_to_a_sample() {
        let zones: Vec<String> = (0..600).map(|i| format!("Zone/{i}")).collect();
        let metadata: NodeMetadata = serde_json::from_value(json!({
            "type": "Cron",
            "label": "Cron",
            "description": "Fire on a schedule.",
            "inputs": [
                { "name": "timezone", "type": "String",
                  "widget": { "kind": "select", "options": zones } },
                { "name": "unit", "type": "String",
                  "widget": { "kind": "select", "options": ["sec", "min"] } }
            ],
            "outputs": []
        }))
        .expect("metadata");

        let compact = metadata.compact_json();
        let timezone = &compact["inputs"][0]["widget"];
        assert_eq!(timezone["options"].as_array().map(Vec::len), Some(COMPACT_OPTIONS_SHOWN));
        assert_eq!(timezone["options"][0], json!("Zone/0"));
        assert_eq!(timezone["moreOptions"], json!(600 - COMPACT_OPTIONS_SHOWN));
        let unit = &compact["inputs"][1]["widget"];
        assert_eq!(unit["options"], json!(["sec", "min"]), "a short list is whole");
        assert!(unit.get("moreOptions").is_none(), "and says nothing about more");

        let plain = serde_json::to_value(metadata.resolved()).expect("serializes");
        assert_eq!(plain["inputs"][0]["widget"]["options"].as_array().map(Vec::len), Some(600));

        // Free-form data is never walked: a `default` that happens to
        // hold a select-shaped object under a `widget` key keeps every
        // entry and gains nothing.
        let zones: Vec<String> = (0..20).map(|i| format!("Zone/{i}")).collect();
        let metadata: NodeMetadata = serde_json::from_value(json!({
            "type": "FormLike",
            "label": "Form",
            "description": "Holds a form spec as data.",
            "inputs": [
                { "name": "spec", "type": "JsonDict",
                  "default": { "widget": { "kind": "select", "options": zones } } }
            ],
            "outputs": []
        }))
        .expect("metadata");
        let compact = metadata.compact_json();
        assert_eq!(compact["inputs"][0]["default"]["widget"]["options"].as_array().map(Vec::len), Some(20));
        assert!(compact["inputs"][0]["default"]["widget"].get("moreOptions").is_none());
    }

    /// What `compact_json` keeps on a node carrying the interesting
    /// shapes: an input whose label/placeholder are presentation but
    /// whose description/default/accepts are wiring facts; a
    /// remote-select whose sources' `label`/`value` are DATA paths and
    /// whose list `where` holds a null to match, all of which must
    /// survive untouched; a config-derived port spec whose
    /// kind/keyField/fields/addsOutputs survive while its label and
    /// render hint go; and a validation rule whose `equals` payload
    /// carries a `label` key of its own.
    #[test]
    fn compact_json_keeps_wiring_facts_and_strips_presentation() {
        let metadata: NodeMetadata = serde_json::from_str(
            r##"{
                "type": "HumanQuery",
                "label": "Human query",
                "description": "Ask a person mid-flow.",
                "tags": ["human"],
                "icon": "User",
                "color": "#ffffff",
                "inputs": [{
                    "name": "title",
                    "type": "String",
                    "required": true,
                    "widget": { "kind": "text" },
                    "label": "Title",
                    "placeholder": "Question title",
                    "description": "The form's title.",
                    "default": "Approve?"
                }, {
                    "name": "media",
                    "type": "Image",
                    "default": null,
                    "widget": { "kind": "file_drop", "accept": null, "type": "Image" }
                }, {
                    "name": "account",
                    "type": "Access",
                    "widget": { "kind": "access" }
                }, {
                    "name": "model",
                    "type": "String",
                    "widget": {
                        "kind": "remote_select",
                        "access": "account",
                        "freeText": true,
                        "sources": [
                            { "kind": "list", "get": "https://api.example.com/models", "items": "data", "label": "displayName", "value": "id",
                              "where": { "chat": true, "retired_at": null } },
                            { "kind": "granted", "from": "models", "label": "name", "value": "id" }
                        ]
                    }
                }],
                "outputs": [{
                    "name": "send",
                    "type": "Boolean",
                    "description": "True when approved."
                }],
                "portsFromConfig": {
                    "field": "fields",
                    "specs": [{
                        "kind": "approve_reject",
                        "keyField": "key",
                        "label": "Approve / Reject",
                        "render": { "component": "approve_reject" },
                        "fields": [
                            { "key": "key", "required": true, "shape": "typed", "valueType": "String" },
                            { "key": "threshold", "required": false, "shape": "number" },
                            { "key": "resource", "required": true, "shape": "typed", "valueType": "String",
                              "widget": { "kind": "remote_select", "access": "account",
                                          "sources": [ { "kind": "list", "get": "u", "items": "i", "label": "name", "value": "id" } ] } }
                        ],
                        "addsOutputs": [ { "nameTemplate": "{key}_approved", "portType": "Boolean" } ]
                    }]
                },
                "types": { "ChatMessage": "{ role: String }" },
                "validate": [{
                    "when": { "kind": "config_present", "field": "title" },
                    "then": { "message": "fill the title" }
                }, {
                    "when": { "kind": "config_equals", "field": "model", "equals": { "label": "fast", "id": "gpt-x", "spare": null } },
                    "then": { "message": "pick a faster one", "severity": "warning" }
                }]
            }"##,
        )
        .unwrap();

        let compact = metadata.compact_json();
        let mut top: Vec<&str> = compact.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        top.sort_unstable();
        assert_eq!(
            top,
            vec![
                "description",
                "features",
                "inputs",
                "outputs",
                "portsFromConfig",
                "requiresInfra",
                "type",
                "types",
                "validate",
            ],
            "only the keep-list keys survive, and only the ones this node sets"
        );

        let input = &compact["inputs"][0];
        assert_eq!(input["name"], "title");
        assert_eq!(input["type"], "String");
        assert_eq!(input["accepts"], serde_json::json!(["literal", "wire"]), "resolved accepts is a wiring fact");
        assert_eq!(input["widget"]["kind"], "text");
        assert_eq!(input["description"], "The form's title.");
        assert_eq!(input["default"], "Approve?", "a default changes whether wiring is needed");
        assert!(input.get("label").is_none(), "an input label names it to a human");
        assert!(input.get("placeholder").is_none(), "placeholder is editor hint text");

        // A null value only ever means "absent" (`"default": null` IS
        // no default; `Option` serde already conflates the two), so it
        // is stripped like the presentation keys: pure token noise.
        let media = &compact["inputs"][1];
        assert!(media.get("default").is_none(), "a null default is no default");
        assert_eq!(media["widget"]["kind"], "file_drop");
        assert!(media["widget"].get("accept").is_none(), "a null widget knob is noise");
        assert_eq!(media["widget"]["type"], "Image", "the widget's file type is a wiring fact");

        // The `label` KEY is ambiguous between presentation (an
        // input's human name, removed above at its typed site) and
        // DATA (a remote-select source's display path, `Lookup.label`
        // being a REQUIRED field). The name sweep never runs outside
        // the closed `portsFromConfig` subtree, so these survive.
        let account = &compact["inputs"][2];
        assert_eq!(account["widget"]["kind"], "access");
        let model = &compact["inputs"][3];
        let sources = model["widget"]["sources"].as_array().unwrap();
        assert_eq!(sources[0]["label"], "displayName", "a source's display path is data");
        assert_eq!(sources[0]["value"], "id");
        assert_eq!(
            sources[0]["where"],
            serde_json::json!({ "chat": true, "retired_at": null }),
            "a list source's `where` is data: its null is a value to match, not an absent knob"
        );
        assert_eq!(sources[1]["label"], "name", "a granted source's display path is data");
        assert_eq!(sources[1]["value"], "id");
        assert_eq!(model["widget"]["access"], "account", "the authenticating input is a wiring fact");
        assert_eq!(model["widget"]["freeText"], true, "freeText says typing is allowed");

        let output = &compact["outputs"][0];
        assert_eq!(output["description"], "True when approved.");
        assert!(output.get("required").is_none(), "an output carries no optionality");

        let spec = &compact["portsFromConfig"]["specs"][0];
        assert_eq!(spec["kind"], "approve_reject");
        assert_eq!(spec["keyField"], "key");
        assert_eq!(spec["addsOutputs"][0]["nameTemplate"], "{key}_approved");
        assert!(spec.get("label").is_none(), "the spec label names the dropdown entry");
        assert!(spec.get("render").is_none(), "render picks the UI primitive");
        let field = &spec["fields"][0];
        assert_eq!(field["key"], "key");
        assert_eq!(field["shape"], "typed");
        assert_eq!(field["valueType"], "String");
        assert!(field.get("label").is_none());
        assert_eq!(field["widget"]["kind"], "text", "resolved() fills the spec field's widget");

        // A number-shaped spec field gets a `Number` widget from
        // `resolved()`, whose absent knobs serialize as null: the
        // widget-noise sweep under `portsFromConfig` must take them,
        // end to end through the resolve-and-filter path (the stdlib
        // `Switch` ships six fields of exactly this shape).
        let threshold = &spec["fields"][1];
        assert_eq!(threshold["shape"], "number");
        let threshold_widget = threshold["widget"].as_object().unwrap();
        assert_eq!(threshold_widget["kind"], "number");
        for knob in ["min", "max", "step"] {
            assert!(
                threshold_widget.get(knob).is_none(),
                "an absent `{knob}` knob on a spec field's widget is noise"
            );
        }

        // The widget-stop, end to end: a spec field's remote_select
        // carries `label` DATA paths in its sources, which the
        // presentation sweep must not eat.
        let picker = &spec["fields"][2];
        assert!(picker.get("label").is_none(), "a spec field's label is presentation");
        let source = picker["widget"]["sources"][0].as_object().unwrap();
        assert_eq!(source["label"], "name", "a source's display path is data, under a spec field too");
        assert_eq!(source["value"], "id");

        assert_eq!(compact["types"]["ChatMessage"], "{ role: String }");
        assert_eq!(compact["validate"][0]["then"]["message"], "fill the title");
        // A validation rule's `equals` payload is user JSON the rule
        // matches on exactly: its `label` key AND its null value are
        // data, and the null sweep (scoped to port entries) never
        // reaches them.
        let equals = compact["validate"][1]["when"]["equals"].as_object().unwrap();
        assert_eq!(equals["label"], "fast", "data-shaped keys inside rule payloads are never stripped by name");
        assert_eq!(
            equals.get("spare"),
            Some(&serde_json::Value::Null),
            "a null inside a data payload is a comparison target, not absence"
        );
    }

    /// The JSON half of the filter, pinned without building a full
    /// `AccessSpec`: the authoring machinery blocks (`service`,
    /// `accessApps`, `images`, `display`) go by non-membership in the
    /// keep-list; the `portsFromConfig` subtree is swept for the entry
    /// kind's presentation pair but STOPS at widgets (a spec field's
    /// remote_select carries `label` data paths in its sources); and
    /// noise nulls go from port entries and from widgets, wherever a
    /// widget sits. An input's `label` SURVIVES this half on purpose:
    /// input presentation is nulled at its typed site in
    /// `compact_json` before serialization, precisely so a
    /// `label`-shaped DATA key can never be eaten by a name sweep.
    #[test]
    fn compact_wiring_view_drops_authoring_blocks() {
        let mut value = serde_json::json!({
            "type": "T",
            "label": "Presentation",
            "icon": "Hash",
            "service": { "service": "slack" },
            "images": ["images/bridge"],
            "display": { "port": "file" },
            "accessApps": { "slack": { "label": "app" } },
            "inputs": [
                {
                    "name": "account",
                    "type": "Access",
                    "label": "Workspace",
                    "default": null,
                    "widget": { "kind": "number", "min": null, "max": 5, "step": null }
                }
            ],
            "portsFromConfig": {
                "field": "fields",
                "specs": [{
                    "kind": "approve_reject",
                    "label": "Approve / Reject",
                    "render": { "component": "approve_reject" },
                    "fields": [
                        { "key": "key", "label": "Key", "required": true, "shape": "typed", "valueType": "String" },
                        { "key": "threshold", "required": false, "shape": "number",
                          "widget": { "kind": "number", "min": null, "max": 5, "step": null } },
                        { "key": "resource", "required": true, "shape": "typed", "valueType": "String",
                          "widget": { "kind": "remote_select", "access": "account",
                                      "sources": [ { "kind": "list", "get": "u", "items": "i", "label": "name", "value": "id" } ] } }
                    ],
                    "addsOutputs": [ { "nameTemplate": "{key}_approved", "portType": "Boolean", "spare": null } ]
                }]
            }
        });
        compact_wiring_view(&mut value);
        let top = value.as_object().unwrap();
        for gone in ["label", "icon", "service", "images", "display", "accessApps"] {
            assert!(top.get(gone).is_none(), "`{gone}` must not survive the compact view");
        }
        assert_eq!(top.len(), 3, "type, inputs, portsFromConfig are all this node keeps");

        let input = &value["inputs"][0];
        assert_eq!(input["name"], "account");
        assert_eq!(
            input["label"], "Workspace",
            "the JSON half never strips an input label; the typed half in compact_json does"
        );
        assert!(input.get("default").is_none(), "an absent default is noise and goes");
        let widget = input["widget"].as_object().unwrap();
        assert!(widget.get("min").is_none(), "an absent widget knob is noise and goes");
        assert!(widget.get("step").is_none(), "an absent widget knob is noise and goes");
        assert_eq!(widget["max"], 5, "a set widget knob stays");

        let spec = &value["portsFromConfig"]["specs"][0];
        assert!(spec.get("label").is_none(), "the spec label names the dropdown entry");
        assert!(spec.get("render").is_none(), "render picks the UI primitive");
        let plain_field = &spec["fields"][0];
        assert!(plain_field.get("label").is_none(), "a spec field's label is presentation");
        // A spec field's widget is the same closed family as an input's
        // widget: absent knobs go there too.
        let threshold_widget = spec["fields"][1]["widget"].as_object().unwrap();
        assert!(threshold_widget.get("min").is_none(), "an absent knob on a spec field's widget is noise");
        assert!(threshold_widget.get("step").is_none(), "an absent knob on a spec field's widget is noise");
        assert_eq!(threshold_widget["max"], 5, "a set knob stays");
        // The presentation sweep stops at that same widget: its
        // remote-select source's `label` is a data path.
        let picker_field = &spec["fields"][2];
        assert!(picker_field.get("label").is_none(), "a spec field's label is presentation");
        let source = picker_field["widget"]["sources"][0].as_object().unwrap();
        assert_eq!(
            source["label"], "name",
            "the sweep stops at widgets: a source's display path is data"
        );
        assert_eq!(source["value"], "id");
        // The noise sweep under `portsFromConfig` touches WIDGETS only:
        // a stray null anywhere else in the subtree rides through like
        // any data, because no other noise site exists there.
        assert_eq!(
            spec["addsOutputs"][0].get("spare"),
            Some(&serde_json::Value::Null),
            "the null sweep under portsFromConfig is scoped to widgets"
        );
    }
}


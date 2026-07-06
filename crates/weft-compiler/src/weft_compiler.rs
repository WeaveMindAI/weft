//! Weft Compiler: compiles Weft source code into a flat ProjectDefinition.
//!
//! The compiler:
//! 1. Parses Weft syntax (nodes, groups, connections with assignment syntax)
//! 2. Flattens groups by injecting Passthrough nodes at group boundaries
//! 3. Produces a flat ProjectDefinition ready for execution
//!
//! This is a pure function: &str -> Result<ProjectDefinition, Vec<CompileError>>

use uuid::Uuid;

use crate::file_reader::CompileFs;

use weft_core::node::NodeFeatures;
use weft_core::project::{
    ConfigFieldSpan, Edge, GroupBoundary, GroupBoundaryRole, NodeDefinition,
    PortDefinition, Position, ProjectDefinition, Span,
};
use weft_core::weft_type::WeftType;

// ─── Compiler Error ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CompileError {
    /// Source range of the exact culprit (token/node), bounded as tightly as the
    /// error allows. 1-based lines, 0-based character columns, end-exclusive.
    pub span: Span,
    pub message: String,
}

impl CompileError {
    /// An error anchored to a span (the normal case: the offending token/node).
    pub fn at(span: Span, message: impl Into<String>) -> Self {
        Self { span, message: message.into() }
    }

    /// 1-based start line (convenience for consumers that only show a line).
    pub fn line(&self) -> usize {
        self.span.start_line
    }
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `line:col: message` (rustc/TSC convention; columns shown 1-based).
        write!(f, "{}:{}: {}", self.span.start_line, self.span.start_column + 1, self.message)
    }
}

// ─── Intermediate Representations ────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ParsedPort {
    name: String,
    port_type: WeftType,
    required: bool,
    /// True iff this port was auto-synthesized by the loop-lowering pass
    /// (the input side of a carry port). The editor renders these as ghost
    /// mirrors of the carry output; users edit the output, not this side.
    /// Never set on a user-declared port; never set on a non-loop group.
    synthesized_from_carry: bool,
    /// A non-fatal type error: the port was declared with an invalid / unknown
    /// type. The port is KEPT (as `MustOverride`, rendered red) so it doesn't
    /// vanish; this carries the diagnostic the caller records as a squiggle.
    /// `None` for a well-typed port.
    type_error: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedNode {
    id: String,
    node_type: String,
    label: Option<String>,
    config: serde_json::Map<String, serde_json::Value>,
    parent_id: Option<String>,
    in_ports: Vec<ParsedPort>,
    out_ports: Vec<ParsedPort>,
    one_of_required: Vec<Vec<String>>,
    /// Full source range of the declaration (header + config block).
    /// None for synthetic nodes (inline-expression children created
    /// during parsing have a span covering the inline fragment).
    span: Option<Span>,
    /// Source range of the header line (`id = NodeType`), used when
    /// adding a config field to a bare node.
    header_span: Option<Span>,
    /// Span + origin (inline vs connection-line) per config field name.
    config_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
    /// Resolved `@file(...)` references per config field name. Populated by
    /// the file-ref resolution pass; carried to NodeDefinition so the editor
    /// knows which fields are file-backed.
    file_refs: std::collections::BTreeMap<String, weft_core::project::FileRef>,
    /// Set on an opaque `@include` interface node (Interface mode): the path
    /// of the included `.weft` file. The editor renders this node as an
    /// expandable group that navigates into the file.
    include_path: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedConnection {
    source_id: String,
    source_port: String,
    target_id: String,
    target_port: String,
    /// Source range of the connection line (`target.port = source.port`).
    /// None for synthetic edges produced by inline expressions.
    span: Option<Span>,
}

/// What kind of grouping construct this is. Determines the boundary
/// node types emitted by `flatten_group` (`Passthrough` for groups,
/// `LoopIn` / `LoopOut` for loops), and whether `loop_config` ships
/// onto the boundary nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKind {
    Group,
    Loop,
}

#[derive(Debug, Clone)]
struct ParsedGroup {
    id: String,
    kind: GroupKind,
    in_ports: Vec<ParsedPort>,
    out_ports: Vec<ParsedPort>,
    /// @require_one_of groups declared on the group's input port signature.
    one_of_required: Vec<Vec<String>>,
    nodes: Vec<ParsedNode>,
    connections: Vec<ParsedConnection>,
    child_groups: Vec<ParsedGroup>,
    anonymous: bool,
    includes: Vec<ParsedInclude>,
    /// Loop-only config block: `parallel`, `over`, `carry`, `max_iters`,
    /// `trim_on_mismatch`. None for regular groups.
    loop_config: Option<serde_json::Map<String, serde_json::Value>>,
    /// Source spans of the loop's config fields (`parallel`, `over`, ...).
    /// The validate pass uses these so a diagnostic like `parallel-with-carry`
    /// highlights the `carry:` line, not the loop header.
    loop_config_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
    span: Option<Span>,
    header_span: Option<Span>,
}

impl ParsedGroup {
    /// True if `scoped_id` is already a member of this group by ANY declaration
    /// kind: a child node, a child group, or an `@include` alias. Same shared
    /// id namespace as the top level (`ParseState::has_top_level_id`), so the
    /// nested duplicate check must span all three. Ids are already scoped
    /// (`group_id.local`) when this is called.
    fn has_member_id(&self, scoped_id: &str) -> bool {
        self.nodes.iter().any(|n| n.id == scoped_id)
            || self.child_groups.iter().any(|g| g.id == scoped_id)
            || self.includes.iter().any(|x| x.alias == scoped_id)
    }
}

struct ParseState {
    nodes: Vec<ParsedNode>,
    connections: Vec<ParsedConnection>,
    groups: Vec<ParsedGroup>,
    /// `@include("path")` declarations, resolved after parse by
    /// `crate::include::resolve_includes` into either a full rescoped group
    /// (build) or an opaque interface node (interactive parse).
    includes: Vec<ParsedInclude>,
    errors: Vec<CompileError>,
}

impl ParseState {
    /// True if `id` is already declared at the top level by ANY declaration
    /// kind: a node, a group, or an `@include` alias. Nodes, groups, and
    /// include aliases share one id namespace (an include resolves into a
    /// group or node under its alias), so the duplicate check must span all
    /// three regardless of declaration order.
    fn has_top_level_id(&self, id: &str) -> bool {
        self.nodes.iter().any(|n| n.id == id)
            || self.groups.iter().any(|g| g.id == id)
            || self.includes.iter().any(|x| x.alias == id)
    }
}

/// A `c = @include("path")` declaration captured at parse time. `alias` is
/// the call-site name (the group id after resolution), `path` is the file to
/// splice, `span` is the decl's source range (diagnostics + the opaque node).
#[derive(Debug, Clone)]
pub(crate) struct ParsedInclude {
    pub alias: String,
    pub path: String,
    /// Source range of the `alias = @include(...)` decl (the include IS its
    /// header), used both for diagnostics and as the opaque node's span.
    pub span: Span,
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Compile Weft source code into a flat ProjectDefinition.
///
/// `project_id` must be the real DB project_id. The compiler can't derive
/// it from source (the source has no id field), and downstream consumers
/// (orchestrator ownership guard, billing) trust this id, so making it a
/// required parameter prevents the "forgot to overwrite a random UUID"
/// class of bug.
///
/// Groups are flattened: each group produces two Passthrough nodes
/// ({groupId}__in and {groupId}__out) with edges rewired accordingly.
pub fn compile(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
) -> Result<ProjectDefinition, Vec<CompileError>> {
    compile_with_mode(source, project_id, fs, IncludeMode::Full, None)
}

/// How `@include` is resolved. `Full` inlines the referenced group's whole
/// body (build: one binary). `Interface` emits a single opaque node carrying
/// only the group's ports (interactive parse: the editor renders an opaque
/// block and navigates into the file to edit the body).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncludeMode {
    Full,
    Interface,
}

pub fn compile_with_mode(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    include_mode: IncludeMode,
    source_name: Option<&str>,
) -> Result<ProjectDefinition, Vec<CompileError>> {
    // Strict: any error aborts (the build must not produce a half-project).
    let (project, errors) = compile_lenient(source, project_id, fs, include_mode, source_name);
    if errors.is_empty() {
        Ok(project)
    } else {
        Err(errors)
    }
}

/// Lenient compile: ALWAYS returns a project, parsing as much as possible and
/// collecting per-line errors as diagnostics instead of aborting. A single bad
/// line (e.g. a stray `debug` mid-edit) becomes one diagnostic; every valid
/// node/edge around it still renders. This is the editor's parse path; the
/// build path uses `compile_with_mode` (strict) which fails on any error.
/// `source_name` is the file's identity (e.g. `MyCleaner` from `my-cleaner.weft`,
/// or `Untitled` for an unsaved buffer). It's the id given to an anonymous
/// top-level group (`Group(){...}` with no `name =`), so a file's anon root has
/// the SAME id at parse, edit, and render: there's no sentinel to rename later.
pub fn compile_lenient(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    include_mode: IncludeMode,
    source_name: Option<&str>,
) -> (ProjectDefinition, Vec<CompileError>) {
    let mut errors = Vec::new();

    // Parse (the parser already builds a partial ParseState alongside its
    // errors). Resolve this file's own `@file` markers, collecting errors.
    // The file's identity: derived from the filename for an anonymous top-level
    // group (`Group(){...}`), so the file's anon root has the same id at parse,
    // edit, and render. `None`/an unsaved buffer falls back to `Untitled`.
    let source_id = source_name.unwrap_or("Untitled");
    let mut state = parse_weft(source, source_id);
    errors.append(&mut state.errors);
    for node in &mut state.nodes {
        resolve_node_file_refs_in(node, &fs, &mut errors);
    }
    for group in &mut state.groups {
        resolve_group_file_refs(group, &fs, &mut errors);
    }

    // Resolve `@include` declarations (Full inlines, Interface emits opaque
    // nodes), collecting errors.
    resolve_includes(&mut state, &fs, include_mode, &mut Vec::new(), &mut errors);

    // Flatten the partial state into a project. flatten builds from whatever
    // the parser produced and never fails.
    let project = flatten(state, project_id);
    (project, errors)
}

/// Parse one file and resolve its `@file(...)` config markers against
/// `base_dir`. Shared by the top-level compile and the include resolver so
/// every file's `@file` refs resolve relative to that file's own directory.
fn parse_and_resolve_file_refs(
    source: &str,
    fs: &CompileFs,
    source_id: &str,
) -> Result<ParseState, Vec<CompileError>> {
    let mut state = parse_weft(source, source_id);
    if !state.errors.is_empty() {
        return Err(state.errors);
    }
    let mut errors = Vec::new();
    for node in &mut state.nodes {
        resolve_node_file_refs_in(node, fs, &mut errors);
    }
    for group in &mut state.groups {
        resolve_group_file_refs(group, fs, &mut errors);
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(state)
}

/// Resolve a single node's `@file` markers (top-level helper bridging the
/// private `ParsedNode` fields to `file_ref::resolve_node_file_refs`).
fn resolve_node_file_refs_in(
    node: &mut ParsedNode,
    fs: &CompileFs,
    errors: &mut Vec<CompileError>,
) {
    crate::file_ref::resolve_node_file_refs(
        &mut node.config,
        &node.config_spans,
        &mut node.file_refs,
        node.span.unwrap_or_default(),
        fs,
        errors,
    );
}

/// Resolve `@file` markers in a group body and all its descendants, so a
/// `@file` inside a group (including the anonymous root group of an included
/// file) resolves too.
fn resolve_group_file_refs(
    group: &mut ParsedGroup,
    fs: &CompileFs,
    errors: &mut Vec<CompileError>,
) {
    for node in &mut group.nodes {
        resolve_node_file_refs_in(node, fs, errors);
    }
    for child in &mut group.child_groups {
        resolve_group_file_refs(child, fs, errors);
    }
}

// ─── Include resolution ───────────────────────────────────────────────────────

/// Internal node type for an opaque `@include` block in Interface mode. The
/// editor renders it as an expandable group that navigates into the file.
pub const INCLUDE_NODE_TYPE: &str = "IncludedGroup";

/// Compose a flattened edge id from its endpoints. The single definition of the
/// edge-id shape, used by `parsed_to_edge` at flatten.
fn edge_id(source: &str, source_handle: &str, target: &str, target_handle: &str) -> String {
    format!("e-{source}-{source_handle}-{target}-{target_handle}")
}

/// The display label for a group's boundary Passthrough. Single definition of
/// the shape, used at flatten.
fn boundary_label(group_id: &str, role: weft_core::project::GroupBoundaryRole) -> String {
    let suffix = match role {
        weft_core::project::GroupBoundaryRole::In => "in",
        weft_core::project::GroupBoundaryRole::Out => "out",
    };
    format!("{group_id} ({suffix})")
}

/// Resolve every `@include("path")` in `state`. `Full` inlines each
/// referenced file's single top-level Group (rescoped under the alias) into
/// `state.groups`; `Interface` emits one opaque node per include carrying the
/// group's ports. `in_progress` is the cycle-detection stack of canonical
/// file paths currently being resolved.
fn resolve_includes(
    state: &mut ParseState,
    fs: &CompileFs,
    mode: IncludeMode,
    in_progress: &mut Vec<std::path::PathBuf>,
    errors: &mut Vec<CompileError>,
) {
    let includes = std::mem::take(&mut state.includes);
    for inc in includes {
        if fs.base.is_none() {
            errors.push(CompileError::at(inc.span, format!("@include(\"{}\") cannot be resolved outside a project", inc.path)));
            continue;
        }
        match resolve_one_include(&inc, fs, mode, in_progress) {
            Ok(IncludeResult::Group(group)) => state.groups.push(*group),
            Ok(IncludeResult::Node(node)) => state.nodes.push(*node),
            Err(msg) => errors.push(CompileError::at(inc.span, msg)),
        }
    }
    // Resolve includes nested inside group bodies, anywhere in the tree.
    for group in &mut state.groups {
        resolve_group_includes(group, fs, mode, in_progress, errors);
    }
}

/// Resolve `@include`s declared inside a group body, recursing through child
/// groups. Full-mode includes become child groups; Interface-mode includes
/// become opaque member nodes.
fn resolve_group_includes(
    group: &mut ParsedGroup,
    fs: &CompileFs,
    mode: IncludeMode,
    in_progress: &mut Vec<std::path::PathBuf>,
    errors: &mut Vec<CompileError>,
) {
    let includes = std::mem::take(&mut group.includes);
    for inc in includes {
        if fs.base.is_none() {
            errors.push(CompileError::at(inc.span, format!("@include(\"{}\") cannot be resolved outside a project", inc.path)));
            continue;
        }
        match resolve_one_include(&inc, fs, mode, in_progress) {
            Ok(IncludeResult::Group(g)) => group.child_groups.push(*g),
            Ok(IncludeResult::Node(mut n)) => {
                // The opaque interface node is a member of this group (its id
                // is already scoped `group.child`); set parent_id so flatten
                // assigns it the group's scope, like any other child node.
                // Without this its scope is empty and edges from siblings trip
                // the scope-reachability check.
                n.parent_id = Some(group.id.clone());
                group.nodes.push(*n);
            }
            Err(msg) => errors.push(CompileError::at(inc.span, msg)),
        }
    }
    for child in &mut group.child_groups {
        resolve_group_includes(child, fs, mode, in_progress, errors);
    }
}

enum IncludeResult {
    Group(Box<ParsedGroup>),
    Node(Box<ParsedNode>),
}

fn resolve_one_include(
    inc: &ParsedInclude,
    fs: &CompileFs,
    mode: IncludeMode,
    in_progress: &mut Vec<std::path::PathBuf>,
) -> Result<IncludeResult, String> {
    // The caller has already gated `fs.base.is_none()` (an `@include` outside a
    // project), so an anchor is present here.
    let base = fs
        .base
        .expect("resolve_one_include called with no fs anchor");
    // Resolve + read through the active backing (disk, in-memory map, DB rows):
    // it owns join, the trusted-tree containment check, and the read, and returns
    // the backing-agnostic identity used below for cycle detection and for
    // deriving the included file's own directory.
    let resolved = fs
        .reader
        .resolve_and_read(base, std::path::Path::new(&inc.path))
        .map_err(|e| format!("@include {e}"))?;
    let canonical = resolved.identity;
    if in_progress.contains(&canonical) {
        return Err(format!("@include cycle: {:?} includes itself", inc.path));
    }

    let source = resolved.content;
    let included_dir = canonical.parent().map(|p| p.to_path_buf());
    let included_fs = fs.descend(included_dir.as_deref());

    // Parse the included file with the CALL-SITE ALIAS as its anon-root id, so
    // its top-level group lowers directly to `{alias}` and its internals to
    // `{alias}.*` (the final scoped ids), with NO post-parse rescope pass. The
    // alias is already scoped (`c` at top level, `g.c` nested), so this is the
    // SAME single-pass scoping the rest of the lowering uses; there is no second
    // string-surgery rescoping engine. (`@file` markers still resolve against the
    // included file's own directory.)
    let mut sub = parse_and_resolve_file_refs(source.as_str(), &included_fs, &inc.alias)
        .map_err(|errs| {
            errs.into_iter()
                .map(|e| format!("{}: {}", inc.path, e))
                .collect::<Vec<_>>()
                .join("; ")
        })?;

    // An included file must be exactly one anonymous top-level Group and
    // nothing else: the Group header is the file's interface, and the file
    // name is its identity (no top-level name). A named group, multiple groups,
    // loose nodes, OR a loose top-level connection are rejected. (Only the group
    // is consumed below; any other top-level content would be silently dropped,
    // so the gate must catch it loudly.)
    let single_anon = sub.nodes.is_empty()
        && sub.includes.is_empty()
        && sub.connections.is_empty()
        && sub.groups.len() == 1
        && sub.groups[0].anonymous;
    if !single_anon {
        return Err(format!(
            "@include(\"{}\"): an included file must be exactly one anonymous top-level Group, e.g. `Group(in: T) -> (out: U) {{ ... }}`",
            inc.path
        ));
    }
    let mut group = sub.groups.pop().unwrap();

    match mode {
        IncludeMode::Interface => {
            // Opaque block: a node carrying only the group's ports. The body
            // is not loaded; the editor navigates into the file to edit it.
            let node = ParsedNode {
                id: inc.alias.clone(),
                node_type: INCLUDE_NODE_TYPE.to_string(),
                label: None,
                config: serde_json::Map::new(),
                parent_id: None,
                in_ports: group.in_ports.clone(),
                out_ports: group.out_ports.clone(),
                one_of_required: group.one_of_required.clone(),
                span: Some(inc.span),
                header_span: Some(inc.span),
                config_spans: Default::default(),
                file_refs: Default::default(),
                include_path: Some(inc.path.clone()),
            };
            Ok(IncludeResult::Node(Box::new(node)))
        }
        IncludeMode::Full => {
            // Resolve the included group's OWN nested @includes first, against
            // the included file's directory (not the parent's), so nested
            // composition inlines fully. Cycle stack guards self-inclusion.
            in_progress.push(canonical.clone());
            let mut errs = Vec::new();
            resolve_group_includes(&mut group, &included_fs, mode, in_progress, &mut errs);
            in_progress.pop();
            if !errs.is_empty() {
                return Err(errs
                    .into_iter()
                    .map(|e| format!("{}: {}", inc.path, e))
                    .collect::<Vec<_>>()
                    .join("; "));
            }
            // The group was parsed with the alias as its anon-root id, so its id
            // is already `{alias}` and its internals `{alias}.*`: no rescope.
            // Once spliced under a call-site alias it is a named member group of
            // the parent, NOT a standalone-component root. Clear the flag so it
            // can't trip the component-validation rule (which treats a top-level
            // anonymous group as "this file IS a component").
            group.anonymous = false;
            Ok(IncludeResult::Group(Box::new(group)))
        }
    }
}

// ─── Parser ──────────────────────────────────────────────────────────────────

/// Accumulator for inline-expression children and their connection edges.
/// When a `key: Type { ... }.port` inline is detected inside a config block
/// (or on the RHS of a connection), the parser appends the resulting child
/// node and the synthetic edge to this scope. The caller merges them into
/// its own scope (root project or group body).
#[derive(Default)]
struct InlineScope {
    nodes: Vec<ParsedNode>,
    connections: Vec<ParsedConnection>,
}

// ─── CST lowering: the single parser ─────────────────────────────────────────
//
// `parse_weft` is the one parser for `.weft`. It parses source into the lossless
// CST (`crate::cst`), then LOWERS that tree into the `ParseState` AST that the
// rest of the pipeline (`@file`/`@include` resolution, `flatten`, codegen)
// consumes. So there is ONE place that reads raw text (the CST lexer/parser);
// everything downstream is a projection of it.
//
// The CST handles the delimiting (where a node/group/value begins and ends)
// structurally, so the lowering hands each construct's clean text slice to the
// pure value-parsers (`WeftType::parse`, `parse_kv`, `store_config_value`,
// inline-expression synthesis, ...). Spans are derived from CST byte ranges via
// `LineIndex` (1-based lines, the form `flatten`/`NodeDefinition` and the editor
// diagnostics expect).

/// Byte offset → 1-based (line, char-column), for turning a CST `text_range()`
/// into the `Span`s the AST and diagnostics carry. Columns are CHARACTER counts
/// from the line start (0-based), not byte offsets, so a span over `héllo` is
/// correct in an editor (what rustc/TSC/LSP report).
struct LineIndex<'s> {
    source: &'s str,
    /// Byte offset of the start of each line (line i starts at `starts[i]`).
    starts: Vec<usize>,
}

impl<'s> LineIndex<'s> {
    fn new(source: &'s str) -> Self {
        let mut starts = vec![0usize];
        for (i, b) in source.bytes().enumerate() {
            if b == b'\n' {
                starts.push(i + 1);
            }
        }
        LineIndex { source, starts }
    }

    /// 1-based line number containing byte offset `off`.
    fn line(&self, off: usize) -> usize {
        match self.starts.binary_search(&off) {
            Ok(i) => i + 1,
            Err(i) => i, // i = count of starts <= off = the 1-based line
        }
    }

    /// 0-based CHARACTER column of byte offset `off` within its line.
    fn col(&self, off: usize) -> usize {
        let line_start = self.starts[self.line(off).saturating_sub(1)];
        let off = off.min(self.source.len());
        self.source[line_start..off].chars().count()
    }

    /// The (line, char-col) of a byte offset.
    fn pos(&self, off: usize) -> (usize, usize) {
        (self.line(off), self.col(off))
    }

    /// A `Span` covering a byte range, columns as character counts. `end` is
    /// exclusive (the CST convention), so the span ends at the column just past
    /// the last contained char.
    fn span_for(&self, start: usize, end: usize) -> Span {
        let (start_line, start_column) = self.pos(start);
        let (end_line, end_column) = self.pos(end.max(start));
        Span { start_line, start_column, end_line, end_column }
    }

    /// `Span` of a CST node's CONTENT (leading trivia skipped). The single home
    /// for "where does this node really start", used by diagnostics + config
    /// spans so they point at what the user wrote, not the blank line above it.
    fn span_of(&self, node: &crate::cst::SyntaxNode) -> Span {
        let start: usize = content_start(node).into();
        let end: usize = node.text_range().end().into();
        self.span_for(start, end)
    }

    /// `Span` of a single token (tight: just the token's own range).
    fn span_of_token(&self, tok: &crate::cst::SyntaxToken) -> Span {
        let r = tok.text_range();
        self.span_for(r.start().into(), r.end().into())
    }
}

/// The byte offset where a node's CONTENT begins, skipping leading trivia (a
/// decl/connection owns its leading comment/blank-line trivia per the CST
/// attachment rule). The single trivia-skip, shared by span computation and
/// structural-error reporting.
fn content_start(node: &crate::cst::SyntaxNode) -> rowan::TextSize {
    node.descendants_with_tokens()
        .find(|e| e.as_token().map(|t| !t.kind().is_trivia()).unwrap_or(false))
        .map(|e| e.text_range().start())
        .unwrap_or_else(|| node.text_range().start())
}

/// Parse source into the `ParseState` AST via the CST. The single parser: it
/// reads raw text once (the CST), and lowers that tree into `ParseState`.
fn parse_weft(source: &str, source_id: &str) -> ParseState {
    use crate::cst::nodes::{Decl as CstDecl, WeftFile};
    let root = crate::cst::parse(source);
    let li = LineIndex::new(source);
    let mut state = ParseState {
        nodes: Vec::new(),
        connections: Vec::new(),
        groups: Vec::new(),
        includes: Vec::new(),
        errors: Vec::new(),
    };
    let Some(file) = WeftFile::cast(root) else {
        return state;
    };
    // Top-level items: an InlineScope accumulates inline-expr children + edges.
    let mut inline = InlineScope::default();
    // Defer top-level connections: a `node.field = <literal>` connection is
    // really a config-origin field on `node`, which we can only attribute once
    // the target node is lowered. Collect the CST connection nodes first.
    let mut conn_nodes: Vec<crate::cst::SyntaxNode> = Vec::new();
    for child in file.syntax().children() {
        match child.kind() {
            crate::cst::SyntaxKind::CONNECTION => conn_nodes.push(child.clone()),
            crate::cst::SyntaxKind::NODE_DECL
            | crate::cst::SyntaxKind::GROUP_DECL
            | crate::cst::SyntaxKind::LOOP_DECL
            | crate::cst::SyntaxKind::INCLUDE_DECL => {
                if let Some(decl) = CstDecl::cast(child.clone()) {
                    lower_decl(&decl, None, source_id, &li, &mut state, &mut inline);
                }
            }
            _ => {}
        }
    }
    for cn in conn_nodes {
        lower_top_level_connection(&cn, &li, &mut state, &mut inline);
    }
    // Merge top-level inline-expr children + edges.
    merge_inline_nodes(&mut state.nodes, inline.nodes, &mut state.errors);
    state.connections.extend(inline.connections);

    // Structural error detection over the CST: an ERROR node is unparseable
    // content (e.g. `this is not valid syntax` at root), and a BODY missing its
    // closing `}` is an unclosed block. Both are loud per-line diagnostics.
    detect_structural_errors(file.syntax(), &li, &mut state.errors);
    state
}

/// Walk the CST emitting a CompileError for each ERROR node (unparseable text)
/// and each unclosed BODY (no R_BRACE token).
fn detect_structural_errors(node: &crate::cst::SyntaxNode, li: &LineIndex, errors: &mut Vec<CompileError>) {
    use crate::cst::SyntaxKind as K;
    // A malformed `@require_one_of` is NOT checked here: the lowering validates
    // every `@require_one_of` (port-list + body directive) through the one
    // `marker::require_one_of_ports` gate and fails loud there. This sweep stays
    // purely structural (ERROR nodes + unclosed bodies).
    for n in node.descendants() {
        match n.kind() {
            K::ERROR => {
                let text = n.to_string();
                let snippet = text.trim();
                if !snippet.is_empty() {
                    // Span the ERROR's content (trivia skipped via li.span_of).
                    errors.push(CompileError::at(li.span_of(&n), format!("Unexpected content: '{snippet}'")));
                }
            }
            K::BODY => {
                let has_close = n.children_with_tokens().any(|e| e.kind() == K::R_BRACE);
                if !has_close {
                    // A group body and a node config block get distinct messages.
                    let is_group = n.parent().map(|p| p.kind() == K::GROUP_DECL).unwrap_or(false);
                    let message = if is_group { "Unclosed group" } else { "Unclosed config block" };
                    errors.push(CompileError::at(li.span_of(&n), message));
                }
            }
            _ => {}
        }
    }
}

/// A connection whose RHS is a literal: `target.port = <STRING|NUMBER|HEREDOC|
/// JSON_VALUE|MARKER>`. This is a config-origin field, not an edge. Recognizing
/// and extracting it is the one subtle, shared piece (telling a literal RHS from
/// a `src.port` edge); WHERE the value goes (and whether a missing target is an
/// error or an edge) is scope policy the caller owns.
struct LiteralFill {
    target_id: String,
    port: String,
    value: String,
    /// Span of the whole connection (the config field's source range).
    span: Span,
}

/// Recognize a literal-RHS config fill on `conn` (`node.field = <literal>`) and
/// extract its data. The recognition RULE (one endpoint, no inline-expr, non-self
/// target, non-empty port) lives in `cst::nodes::connection_is_config_origin`,
/// shared with the edit ops so the editor and compiler can't disagree on what a
/// config field is. Here we just extract the value + spans once the connection is
/// classified as a fill.
fn literal_config_fill(conn: &crate::cst::SyntaxNode, li: &LineIndex) -> Option<LiteralFill> {
    use crate::cst::SyntaxKind as K;
    if !crate::cst::nodes::connection_is_config_origin(conn, None, None) {
        return None;
    }
    let target = conn.children().find(|n| n.kind() == K::ENDPOINT)?;
    let (target_id, port) = endpoint_id_port(&target);
    Some(LiteralFill {
        target_id,
        port,
        value: connection_rhs_text(conn),
        span: li.span_of(conn),
    })
}

/// Apply a recognized literal fill to a node's config maps (value + a
/// connection-origin span). The two-line store, shared so the span origin and
/// value parsing can't diverge between scopes.
fn apply_literal_fill(
    fill: &LiteralFill,
    config: &mut serde_json::Map<String, serde_json::Value>,
    config_spans: &mut std::collections::BTreeMap<String, ConfigFieldSpan>,
    errors: &mut Vec<CompileError>,
) {
    if let Some(k) = store_value_text(&fill.port, &fill.value, config, fill.span, errors) {
        config_spans.insert(k, ConfigFieldSpan::connection(fill.span));
    }
}

/// Lower a top-level CONNECTION. A literal RHS (`node.port = "v"`) fills that
/// NODE's config. A literal to anything else (a group/include alias, an
/// undeclared target) has no config to fill: it falls through to
/// `lower_connection`, which rejects the bare literal loudly (only a node's own
/// port takes a literal; a group/include port is driven by wiring). Otherwise
/// (an endpoint or inline-expr RHS) it is an edge / node synthesis.
fn lower_top_level_connection(conn: &crate::cst::SyntaxNode, li: &LineIndex, state: &mut ParseState, inline: &mut InlineScope) {
    if let Some(fill) = literal_config_fill(conn, li) {
        if let Some(node) = state.nodes.iter_mut().find(|n| n.id == fill.target_id) {
            apply_literal_fill(&fill, &mut node.config, &mut node.config_spans, &mut state.errors);
            return;
        }
        // Not a node: a literal can't fill a group/include/undeclared port. Fall
        // through; `lower_connection` emits the single "cannot assign a literal"
        // error (no phantom empty-source edge).
    }
    // No enclosing group, so an inline-expr RHS synthesizes its anon node at the
    // file root (scope None).
    if let Some(c) = lower_connection(conn, None, li, inline, &mut state.errors) {
        state.connections.push(c);
    }
}

/// The RHS text of a connection (everything after the `=`), trimmed.
fn connection_rhs_text(conn: &crate::cst::SyntaxNode) -> String {
    use crate::cst::SyntaxKind as K;
    let mut out = String::new();
    let mut after_eq = false;
    for el in conn.children_with_tokens() {
        match &el {
            rowan::NodeOrToken::Token(t) => {
                if t.kind() == K::EQ && !after_eq {
                    after_eq = true;
                    continue;
                }
                if after_eq && t.kind() == K::COMMENT {
                    continue;
                }
                if after_eq {
                    out.push_str(t.text());
                }
            }
            rowan::NodeOrToken::Node(n) => {
                if after_eq {
                    out.push_str(&n.to_string());
                }
            }
        }
    }
    out.trim().to_string()
}

/// Lower one CST declaration into the ParseState (node / group / include),
/// scoped under `parent` (None = top level). `source_id` is the file's identity
/// (filename-derived; `Untitled` if unsaved), used as the id of an anonymous
/// top-level `Group(){...}`.
fn lower_decl(
    decl: &crate::cst::nodes::Decl,
    parent: Option<&str>,
    source_id: &str,
    li: &LineIndex,
    state: &mut ParseState,
    inline: &mut InlineScope,
) {
    use crate::cst::nodes::Decl as CstDecl;
    match decl {
        CstDecl::Node(n) => {
            if let Some(node) = lower_node(n, parent, li, inline, &mut state.errors) {
                if state.has_top_level_id(&node.id) {
                    state.errors.push(CompileError::at(dup_span(node.header_span, node.span), format!("Duplicate id '{}'", node.id)));
                }
                state.nodes.push(node);
            }
        }
        CstDecl::Loop(l) => {
            if let Some(group) = lower_loop(l, parent, source_id, li, &mut state.errors) {
                if state.has_top_level_id(&group.id) {
                    state.errors.push(CompileError::at(dup_span(group.header_span, group.span), format!("Duplicate id '{}'", group.id)));
                }
                state.groups.push(group);
            }
        }
        CstDecl::Group(g) => {
            if let Some(group) = lower_group(g, parent, source_id, li, &mut state.errors) {
                // An anonymous group's id IS `source_id` (the file's filename id),
                // a second anonymous top-level Group is the shape error "a file
                // must contain exactly one anonymous top-level Group", not a
                // generic duplicate id. A named group with a duplicate id is the
                // normal `Duplicate id` error.
                if group.anonymous && state.groups.iter().any(|x| x.anonymous) {
                    state.errors.push(CompileError::at(
                        dup_span(group.header_span, group.span),
                        "a file must contain exactly one anonymous top-level Group",
                    ));
                    return;
                }
                if state.has_top_level_id(&group.id) {
                    state.errors.push(CompileError::at(dup_span(group.header_span, group.span), format!("Duplicate id '{}'", group.id)));
                }
                state.groups.push(group);
            }
        }
        CstDecl::Include(i) => {
            if let Some(inc) = lower_include(i, parent, li, &mut state.errors) {
                if state.has_top_level_id(&inc.alias) {
                    state.errors.push(CompileError::at(inc.span, format!("Duplicate id '{}'", inc.alias)));
                }
                state.includes.push(inc);
            }
        }
    }
}

/// Pick the tightest available span for a duplicate-id diagnostic: the decl's
/// header (`id = Type`) if known, else its full span. A parsed decl ALWAYS
/// carries one of these (the lowering sets `span`/`header_span` on every node it
/// builds), so the default is unreachable; the `debug_assert` makes a future
/// regression loud in tests rather than silently mislocating to line 0.
fn dup_span(header_span: Option<Span>, full_span: Option<Span>) -> Span {
    debug_assert!(header_span.is_some() || full_span.is_some(), "a parsed decl must carry a span");
    header_span.or(full_span).unwrap_or_default()
}

/// Reject a user-written local id (a node/group name or include alias) that
/// the LANGUAGE owns, pushing a loud error and returning false. The single home
/// for reserved-name rejection, shared by `lower_node`/`lower_group`/
/// `lower_include` so they can't drift. Reserved:
///   - `self` (the boundary keyword), `Group`/`Passthrough` (type keywords);
///   - any id containing `__`: the compiler synthesizes ids with `__` as the
///     separator (`{group}__in`/`__out` boundary passthroughs, `{host}__{field}`
///     inline-expr anon nodes). Letting a user take a `__` name makes those
///     collide at flatten (`foo__in` vs group `foo`'s `__in`) into a silent
///     duplicate id. Reserving the separator makes the collision impossible.
fn reject_reserved_local(local_id: &str, span: Span, errors: &mut Vec<CompileError>) -> bool {
    if !is_reserved_local(local_id) {
        return true;
    }
    // Reserved: pick the message that explains WHICH rule it tripped. The
    // membership decision itself lives only in `is_reserved_local` (above), so
    // this match is presentation, not a second copy of the rule.
    let msg = if local_id == "self" {
        "'self' is a reserved word and cannot be used as an identifier".to_string()
    } else if is_reserved_type_keyword(local_id) {
        format!("'{local_id}' is a reserved type keyword and cannot be used as a node or group name")
    } else {
        format!("'{local_id}' uses '__', which is reserved for compiler-generated ids (group boundaries, inline expressions); pick a name without a double underscore")
    };
    errors.push(CompileError::at(span, msg));
    false
}

/// The canonical list of type names the language reserves. The SINGLE
/// source of truth: identifier reservation, catalog node-type
/// reservation, and any future surface that needs to know "is this name
/// taken by the language" all consult this list. To add a new reserved
/// type, append it here and every consumer picks it up.
pub const RESERVED_TYPE_KEYWORDS: &[&str] =
    &["Group", "Passthrough", "Loop", "LoopIn", "LoopOut"];

/// True iff `name` is one of the language's reserved type keywords.
pub fn is_reserved_type_keyword(name: &str) -> bool {
    RESERVED_TYPE_KEYWORDS.contains(&name)
}

/// True iff `id` is a name the language reserves as an identifier: the
/// `self` boundary keyword, any reserved type keyword, or any `__`
/// -containing id (the compiler-id separator). The SINGLE source of the
/// reserved-name membership rule: `reject_reserved_local` delegates here
/// for the decision (and only adds per-case messages), and
/// `source_name::derive_id` consults it so a filename-derived
/// anonymous-root id can never be reserved either.
pub fn is_reserved_local(id: &str) -> bool {
    id == "self" || is_reserved_type_keyword(id) || id.contains("__")
}

/// The header IDENT (the decl's local id) and the type name, from a HEADER node.
fn header_id_and_type(header: &crate::cst::SyntaxNode) -> (String, String) {
    use crate::cst::SyntaxKind as K;
    let mut id = String::new();
    let mut ty = String::new();
    // A header with NO `=` is an anonymous group (`Group(...)`): the leading
    // `Group` is the TYPE, the id is empty (sentinel-assigned later). A header
    // WITH `=` is `id = Type`; here a leading `Group` before `=` is the reserved
    // keyword used as a NAME (`Group = Text`), captured as the id so the lowering
    // rejects it.
    let has_eq = header.children_with_tokens().any(|e| e.as_token().map(|t| t.kind() == K::EQ).unwrap_or(false));
    let mut seen_eq = false;
    for el in header.children_with_tokens() {
        if let Some(t) = el.as_token() {
            match t.kind() {
                K::EQ => seen_eq = true,
                K::IDENT if !seen_eq && id.is_empty() => id = t.text().to_string(),
                K::IDENT if seen_eq && ty.is_empty() => ty = t.text().to_string(),
                K::KW_GROUP if !seen_eq && has_eq && id.is_empty() => id = "Group".to_string(),
                K::KW_GROUP if seen_eq && ty.is_empty() => ty = "Group".to_string(),
                // anonymous group: `Group` with no `=`, it is the type.
                K::KW_GROUP if !has_eq && ty.is_empty() => ty = "Group".to_string(),
                _ => {}
            }
        }
    }
    (id, ty)
}

/// Parse a `(...)` port-signature CST node into ParsedPorts (reusing the pure
/// `try_parse_port_decl`). Also collects `@require_one_of(...)` markers.
fn lower_port_sig(
    sig: &crate::cst::SyntaxNode,
    direction: &str,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) -> (Vec<ParsedPort>, Vec<Vec<String>>) {
    use crate::cst::SyntaxKind as K;
    let mut ports = Vec::new();
    let mut oor = Vec::new();
    for el in sig.children_with_tokens() {
        match el {
            rowan::NodeOrToken::Node(n) if n.kind() == K::PORT_DECL => {
                let text = n.to_string();
                let text = text.trim().trim_end_matches(',').trim();
                if text.is_empty() {
                    continue;
                }
                // The culprit is this port decl, not the whole header.
                let span = li.span_of(&n);
                match try_parse_port_decl(text) {
                    Ok(p) => {
                        // A bad / unknown port TYPE is kept (as MustOverride, red in
                        // the editor) but still surfaced as a squiggle, so the port
                        // never silently vanishes from the canvas.
                        if let Some(type_err) = &p.type_error {
                            errors.push(CompileError::at(span, type_err.clone()));
                        }
                        if ports.iter().any(|e: &ParsedPort| e.name == p.name) {
                            errors.push(CompileError::at(span, format!("Duplicate {direction} port \"{}\"", p.name)));
                        } else {
                            ports.push(p);
                        }
                    }
                    Err(msg) => errors.push(CompileError::at(span, msg)),
                }
            }
            rowan::NodeOrToken::Token(t) if t.kind() == K::MARKER => {
                // @require_one_of(a, b) in an input list.
                if crate::cst::marker::directive(t.text()) == "require_one_of" {
                    if direction != "in" {
                        errors.push(CompileError::at(li.span_of_token(&t), "@require_one_of is only valid in input port lists"));
                    } else {
                        // Same validity gate as the body directive: a non-empty
                        // port list, or a loud error (never a silent drop).
                        match crate::cst::marker::require_one_of_ports(t.text()) {
                            Ok(ports) => oor.push(ports),
                            Err(msg) => errors.push(CompileError::at(li.span_of_token(&t), msg.to_string())),
                        }
                    }
                }
            }
            _ => {}
        }
    }
    (ports, oor)
}

/// Read a loop config port list (`over` or `carry`) at LOWERING time.
/// Non-string entries are NOT silently dropped: each one pushes a
/// `loop-config-malformed` CompileError so the parse pipeline halts
/// before the lowered project drifts into a half-baked loop. Empty
/// when the key is missing or not an array. Once lowering vets the
/// list this way, subsequent readers (flatten_group, codegen) can
/// trust the entries and use `read_loop_port_list_vetted`.
fn read_loop_port_list(
    loop_id: &str,
    cfg: Option<&serde_json::Map<String, serde_json::Value>>,
    key: &str,
    span: Span,
    errors: &mut Vec<CompileError>,
) -> Vec<String> {
    let Some(arr) = cfg.and_then(|c| c.get(key)).and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        match v.as_str() {
            Some(s) => out.push(s.to_string()),
            None => errors.push(CompileError::at(
                span.clone(),
                format!(
                    "loop '{loop_id}': [loop-config-malformed] '{key}[{i}]' must be a port name (string); got {v}"
                ),
            )),
        }
    }
    out
}

/// Like `read_loop_port_list` but discards the values. Use when the
/// caller only needs the side-effect of validating the list (downstream
/// reads will go through `read_loop_port_list_vetted`).
fn validate_loop_port_list(
    loop_id: &str,
    cfg: Option<&serde_json::Map<String, serde_json::Value>>,
    key: &str,
    span: Span,
    errors: &mut Vec<CompileError>,
) {
    let _ = read_loop_port_list(loop_id, cfg, key, span, errors);
}

/// Read a loop config port list AFTER `read_loop_port_list` already
/// vetted it during lowering. Trusted: non-string entries are not
/// expected and would indicate a caller skipped lowering.
fn read_loop_port_list_vetted(
    cfg: Option<&serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> Vec<String> {
    cfg.and_then(|c| c.get(key))
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

/// Read the input/output port signatures off a HEADER,
/// returning (in_ports, out_ports, one_of_required).
fn lower_header_ports(
    header: &crate::cst::SyntaxNode,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) -> (Vec<ParsedPort>, Vec<ParsedPort>, Vec<Vec<String>>) {
    use crate::cst::SyntaxKind as K;
    let mut in_ports = Vec::new();
    let mut out_ports = Vec::new();
    let mut oor = Vec::new();
    for sig in header.children() {
        match sig.kind() {
            K::PORT_SIG_IN => {
                let (p, o) = lower_port_sig(&sig, "in", li, errors);
                in_ports = p;
                oor.extend(o);
            }
            K::PORT_SIG_OUT => {
                let (p, o) = lower_port_sig(&sig, "out", li, errors);
                out_ports = p;
                oor.extend(o);
            }
            _ => {}
        }
    }
    (in_ports, out_ports, oor)
}

/// Lower a NODE_DECL into a ParsedNode (config + ports + spans), synthesizing
/// inline-expression children/edges into `inline`.
fn lower_node(
    n: &crate::cst::nodes::NodeDecl,
    parent: Option<&str>,
    li: &LineIndex,
    inline: &mut InlineScope,
    errors: &mut Vec<CompileError>,
) -> Option<ParsedNode> {
    let header = n.header()?;
    let (local_id, node_type) = header_id_and_type(header.syntax());
    if local_id.is_empty() || node_type.is_empty() {
        return None;
    }
    // Diagnostics point at the HEADER (not the decl-node start, which includes
    // leading comment/blank-line trivia).
    let header_span = li.span_of(header.syntax());
    // Identifier + type-name validations: reserved names (`self`, type keywords,
    // the `__` compiler-id separator) are rejected loudly in one shared place.
    if !reject_reserved_local(&local_id, header_span, errors) {
        return None;
    }
    if node_type == "Passthrough" {
        errors.push(CompileError::at(header_span, "'Passthrough' is a compiler-internal node type and cannot be used directly. Passthrough nodes are emitted automatically when a group is flattened."));
        return None;
    }
    let id = scoped(parent, &local_id);
    let (in_ports, out_ports, one_of_required) = lower_header_ports(header.syntax(), li, errors);

    let mut config = serde_json::Map::new();
    let mut label = None;
    let mut config_spans: std::collections::BTreeMap<String, ConfigFieldSpan> = Default::default();
    let mut one_of_required = one_of_required;
    let mut body_oor = Vec::new();
    if let Some(body) = n.body() {
        // The inline host is this node's RAW local; its group scope is `parent`.
        // An inline-expr config value synthesizes its anon node `{local}__field`
        // raw, scoped to `g.node__field` by lower_group (one scope pass).
        lower_config_body(&body, &local_id, parent, li, &mut config, &mut label, &mut config_spans, inline, &mut body_oor, errors);
    }
    one_of_required.extend(body_oor);

    Some(ParsedNode {
        id,
        node_type,
        label,
        config,
        parent_id: parent.map(|p| p.to_string()),
        in_ports,
        out_ports,
        one_of_required,
        span: Some(li.span_of(n.syntax())),
        header_span: Some(li.span_of(header.syntax())),
        config_spans,
        file_refs: Default::default(),
        include_path: None,
    })
}

/// Lower the config fields / connections / inline-exprs inside a node body.
/// `host_local` is the host node's RAW local id (matches a `local.key = lit`
/// config-origin connection, and builds inline anon ids `host_local__field`).
/// `group_scope` is the enclosing GROUP's scoped id (None at the file root):
/// the synthesized inline node's scope.
fn lower_config_body(
    body: &crate::cst::nodes::Body,
    host_local: &str,
    group_scope: Option<&str>,
    li: &LineIndex,
    config: &mut serde_json::Map<String, serde_json::Value>,
    label: &mut Option<String>,
    config_spans: &mut std::collections::BTreeMap<String, ConfigFieldSpan>,
    inline: &mut InlineScope,
    body_oor: &mut Vec<Vec<String>>,
    errors: &mut Vec<CompileError>,
) {
    use crate::cst::SyntaxKind as K;
    for child in body.syntax().children() {
        match child.kind() {
            K::CONFIG_FIELD | K::LABEL_FIELD => {
                lower_config_field(&child, host_local, group_scope, li, config, label, config_spans, inline, errors);
            }
            K::CONNECTION => {
                // Inside a node body, `host.key = <literal>` is a config-origin
                // field on THIS node (the body's own config maps); anything else
                // (incl. a literal to a non-host target) is a port-wiring edge.
                let fill = literal_config_fill(&child, li)
                    .filter(|f| f.target_id == host_local);
                match fill {
                    Some(f) => apply_literal_fill(&f, config, config_spans, errors),
                    None => {
                        if let Some(conn) = lower_connection(&child, group_scope, li, inline, errors) {
                            inline.connections.push(conn);
                        }
                    }
                }
            }
            K::DIRECTIVE => {
                // @require_one_of inside a config block: collected into
                // `body_oor` for the caller to merge onto the node/group.
                if let Some(g) = lower_directive_require_one_of(&child, li, errors) {
                    body_oor.push(g);
                }
            }
            _ => {}
        }
    }
}

/// Lower a single CONFIG_FIELD / LABEL_FIELD. `host_local` is the host node's
/// RAW local id (builds the inline anon id `host_local__field`); `group_scope`
/// is the enclosing group's scoped id (None at the file root), the inline node's
/// scope.
fn lower_config_field(
    field: &crate::cst::SyntaxNode,
    host_local: &str,
    group_scope: Option<&str>,
    li: &LineIndex,
    config: &mut serde_json::Map<String, serde_json::Value>,
    label: &mut Option<String>,
    config_spans: &mut std::collections::BTreeMap<String, ConfigFieldSpan>,
    inline: &mut InlineScope,
    errors: &mut Vec<CompileError>,
) {
    use crate::cst::SyntaxKind as K;
    // key = first IDENT
    let key = field
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == K::IDENT)
        .map(|t| t.text().to_string());
    let Some(key) = key else { return };
    let span = li.span_of(field);

    // The value: the element(s) after the COLON. Find the value node/token.
    let value_node = field.children().find(|n| matches!(n.kind(), K::INLINE_EXPR | K::JSON_VALUE | K::ENDPOINT));
    if let Some(vn) = &value_node {
        match vn.kind() {
            K::INLINE_EXPR => {
                // Synthesize the inline-expr anon node + edge structurally from
                // the CST (same machinery node decls use), targeting host.key.
                lower_inline_expr(vn, host_local, &key, group_scope, li, inline, errors);
                return;
            }
            K::ENDPOINT => {
                // Port wiring: key: src.port. Target is the host's RAW local;
                // `lower_group` rescopes it once like every other endpoint.
                let txt = vn.to_string();
                if let Some((src_id, src_port)) = parse_dotted(txt.trim()) {
                    inline.connections.push(ParsedConnection {
                        source_id: src_id,
                        source_port: src_port,
                        target_id: host_local.to_string(),
                        target_port: key.clone(),
                        span: Some(span),
                    });
                    return;
                }
            }
            _ => {}
        }
    }

    // Otherwise a literal value: reconstruct the value text after the colon.
    let value_text = field_value_text(field);
    if field.kind() == K::LABEL_FIELD {
        // The label has ONE home (`node.label`); set twice is a loud error, the
        // same rule `store_value_text` enforces for config keys.
        if label.is_some() {
            errors.push(CompileError::at(span, "duplicate '_label' field: a node's label may be set only once".to_string()));
            return;
        }
        // A label must be a quoted string / heredoc; a bare `_label: raw` fails loud.
        if let Some(text) = parse_label_value(&value_text, span, errors) {
            *label = Some(text);
            config_spans.insert("_label".to_string(), ConfigFieldSpan::inline(span));
        }
        return;
    }
    // Heredoc / JSON / scalar: hand the value text to the value-store helper.
    let stored = store_value_text(&key, &value_text, config, span, errors);
    if let Some(k) = stored {
        config_spans.insert(k, ConfigFieldSpan::inline(span));
    }
}

/// Lower an INLINE_EXPR value (`Type(sig) { body }.port`) into an anonymous
/// child node + an edge from that node's `.port` to `host.field`. Built
/// structurally from the CST node's children using the SAME machinery node
/// declarations use (`lower_header_ports`, `lower_config_body`) and real spans,
/// so inline-expr config, ports, nesting, and diagnostics behave identically to
/// a named node.
///
/// `host_local` is the host the inline fills, AS WRITTEN (the connection target
/// `b`, or a node decl's local id): raw, not group-scoped. The synthesized anon
/// id is `{host_local}__{field}` and the edge targets `host_local` (also raw).
/// Both stay raw on purpose: `lower_group` scopes the anon NODE ids and rescopes
/// ALL edge endpoints together in one pass (a raw `b__field` endpoint and its
/// node both become `g.b__field`). Pre-scoping here would double-scope a child
/// that shadows its group's name (`g` in group `g` -> `g.g.g`).
///
/// `parent_id` is the enclosing GROUP's scoped id (None at the file root): the
/// node's scope, so once `lower_group` scopes the id, `id` and `scope` agree.
fn lower_inline_expr(
    inline_node: &crate::cst::SyntaxNode,
    host_local: &str,
    field_key: &str,
    parent_id: Option<&str>,
    li: &LineIndex,
    inline: &mut InlineScope,
    errors: &mut Vec<CompileError>,
) {
    use crate::cst::SyntaxKind as K;
    let span = li.span_of(inline_node);
    // Type = the leading IDENT/KW_GROUP token of the inline expr.
    let type_tok = inline_node
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| matches!(t.kind(), K::IDENT | K::KW_GROUP));
    let node_type = match type_tok {
        Some(t) => t.text().to_string(),
        None => return,
    };
    if node_type == "Group" {
        errors.push(CompileError::at(span, "Groups cannot be inlined"));
        return;
    }
    if node_type == "Passthrough" {
        errors.push(CompileError::at(span, "'Passthrough' is a compiler-internal node type and cannot be used directly."));
        return;
    }

    // RAW id (`host_local__field`); `lower_group` scopes it to `g.host__field`
    // in the same pass that rescopes the edge endpoints, so node id and edge
    // source stay in lockstep and a name-shadowing child can't double-scope.
    let anon_id = format!("{host_local}__{field_key}");
    // Ports: the inline expr's PORT_DECLs live directly under the INLINE_EXPR
    // (no HEADER sub-node), so read them off the node itself.
    let (in_ports, out_ports, one_of_required) = lower_header_ports(inline_node, li, errors);

    // Body config (recurses for nested inline exprs via lower_config_body). The
    // nested host is THIS anon node (raw `anon_id`), and it shares this node's
    // group scope (all inline children are flattened siblings in one group), so
    // thread `parent_id` unchanged.
    let mut config = serde_json::Map::new();
    let mut label = None;
    let mut config_spans: std::collections::BTreeMap<String, ConfigFieldSpan> = Default::default();
    let mut body_oor = Vec::new();
    let mut one_of_required = one_of_required;
    if let Some(body) = inline_node.children().find(|n| n.kind() == K::BODY).and_then(crate::cst::nodes::Body::cast) {
        lower_config_body(&body, &anon_id, parent_id, li, &mut config, &mut label, &mut config_spans, inline, &mut body_oor, errors);
    }
    one_of_required.extend(body_oor);

    // The required trailing `.port` (the inline expr's output read into the field).
    let output_port = inline_expr_dot_port(inline_node);
    let Some(output_port) = output_port else {
        errors.push(CompileError::at(span, format!("inline expression for '{field_key}' is missing its required '.port'")));
        return;
    };

    inline.nodes.push(ParsedNode {
        id: anon_id.clone(),
        node_type,
        label,
        config,
        // The inline node lives in the host's group scope; setting parent_id makes
        // its `scope` array match its (post-rescope) group-scoped `id`. Without it
        // the id became `g.b__field` but scope was [].
        parent_id: parent_id.map(|s| s.to_string()),
        in_ports,
        out_ports,
        one_of_required,
        span: Some(li.span_of(inline_node)),
        header_span: None,
        config_spans,
        file_refs: Default::default(),
        include_path: None,
    });
    inline.connections.push(ParsedConnection {
        source_id: anon_id,
        source_port: output_port,
        // Raw `host_local`; rescoped to the host's scoped id by `lower_group`.
        target_id: host_local.to_string(),
        target_port: field_key.to_string(),
        span: Some(li.span_of(inline_node)),
    });
}

/// Merge synthesized inline-expr nodes into `dest`. An id collision (two inline
/// exprs on the same `parent.field` -> the same `{parent}__{field}` id) is a LOUD
/// error, not a silent drop: dropping the second one silently discards the user's
/// statement or mis-wires the edge. A collision with a USER node is impossible by
/// construction: `__` is reserved (see `reject_reserved_local`), so no user id
/// can equal a `{parent}__{field}` synthesized id. The single home for the merge
/// so all three lowering scopes agree.
fn merge_inline_nodes(dest: &mut Vec<ParsedNode>, incoming: Vec<ParsedNode>, errors: &mut Vec<CompileError>) {
    for n in incoming {
        if dest.iter().any(|e| e.id == n.id) {
            errors.push(CompileError::at(
                n.span.unwrap_or_default(),
                format!("duplicate id '{}' (an inline expression synthesized a node that collides with an existing one)", n.id),
            ));
        } else {
            dest.push(n);
        }
    }
}

/// Lower a DIRECTIVE body item into an optional `@require_one_of` arg group.
/// The single home for directive handling, shared by the node-body and group-
/// body lowering so they can't drift. Returns the parsed port group, or None for
/// a non-`require_one_of` directive (silently ignored: forward-compatible with
/// future directives the lowering doesn't yet consume).
///
/// A `require_one_of` with NO args fails LOUD rather than silently dropping the
/// constraint: this catches the `@require_one_of (a, b)` space typo (the lexer
/// only folds `(...)` into the marker when it abuts `@name`, so the space splits
/// the args off and the marker arrives bare), and a genuinely empty `()`.
fn lower_directive_require_one_of(
    child: &crate::cst::SyntaxNode,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) -> Option<Vec<String>> {
    use crate::cst::SyntaxKind as K;
    let tok = child.children_with_tokens().filter_map(|e| e.into_token()).find(|t| t.kind() == K::MARKER)?;
    if crate::cst::marker::directive(tok.text()) != "require_one_of" {
        return None;
    }
    match crate::cst::marker::require_one_of_ports(tok.text()) {
        Ok(ports) => Some(ports),
        Err(msg) => {
            errors.push(CompileError::at(li.span_of(child), msg.to_string()));
            None
        }
    }
}

/// The trailing `.port` of an inline expr: the IDENT after the LAST top-level
/// DOT (the `.port` that follows the body/type, not a dot inside the body).
fn inline_expr_dot_port(inline_node: &crate::cst::SyntaxNode) -> Option<String> {
    use crate::cst::SyntaxKind as K;
    // The `.port` tokens are DIRECT children of INLINE_EXPR (the body's dots are
    // nested inside the BODY node), so scan only direct tokens.
    let toks: Vec<crate::cst::SyntaxToken> =
        inline_node.children_with_tokens().filter_map(|e| e.into_token()).collect();
    let dot = toks.iter().rposition(|t| t.kind() == K::DOT)?;
    toks.get(dot + 1).filter(|t| t.kind() == K::IDENT).map(|t| t.text().to_string())
}

/// The raw value text of a config field: everything after the first COLON token,
/// trimmed, reconstructed from the field's tokens/nodes.
fn field_value_text(field: &crate::cst::SyntaxNode) -> String {
    use crate::cst::SyntaxKind as K;
    let mut out = String::new();
    let mut after_colon = false;
    for el in field.children_with_tokens() {
        match &el {
            rowan::NodeOrToken::Token(t) => {
                if t.kind() == K::COLON && !after_colon {
                    after_colon = true;
                    continue;
                }
                if after_colon && t.kind() == K::COMMENT {
                    continue; // drop trailing comment
                }
                if after_colon {
                    out.push_str(t.text());
                }
            }
            rowan::NodeOrToken::Node(n) => {
                if after_colon {
                    out.push_str(&n.to_string());
                }
            }
        }
    }
    out.trim().to_string()
}

/// Store a value text into config: heredoc (```), JSON ([/{), or scalar via the
/// existing `parse_kv`-style logic. Returns the key on success.
///
/// The SINGLE store both config paths funnel through (a body field `key: v` and a
/// connection-origin field `node.key = v`), so a DUPLICATE config key for one
/// node is caught here ONCE rather than silently last-write-wins. A blind
/// overwrite let the editor's per-key SetConfig/RemoveConfig touch only one of
/// two values and strand the other; rejecting the duplicate at the source makes
/// that ambiguous state impossible.
fn store_value_text(
    key: &str,
    value: &str,
    config: &mut serde_json::Map<String, serde_json::Value>,
    span: Span,
    errors: &mut Vec<CompileError>,
) -> Option<String> {
    if config.contains_key(key) {
        errors.push(CompileError::at(span, format!("duplicate config field '{key}': a node's config key may be set only once (as a body field `{key}: ...` OR a connection `node.{key} = ...`, not both)")));
        return None;
    }
    // Heredoc (multiline string): strip fences, single leading/trailing newline,
    // dedent, unescape.
    if let Some(text) = unescape_heredoc(value) {
        config.insert(key.to_string(), serde_json::Value::String(text));
        return Some(key.to_string());
    }
    // JSON or scalar: reuse parse_kv by reconstructing `key: value`.
    let pair = format!("{key}: {value}");
    parse_kv(&pair, config, span, errors)
}

/// Parse a ` ```...``` ` heredoc (multiline string) value into its text, or None
/// if `value` isn't a heredoc. The single home for heredoc unescaping, shared by
/// `store_value_text` and the label path.
fn unescape_heredoc(value: &str) -> Option<String> {
    if !(value.starts_with("```") && value.ends_with("```") && value.len() >= 6) {
        return None;
    }
    let inner = &value[3..value.len() - 3];
    let inner = inner.strip_prefix('\n').unwrap_or(inner);
    let inner = inner.strip_suffix('\n').unwrap_or(inner);
    Some(dedent(inner).replace("\\```", "```").replace("\\`", "`"))
}

/// Parse a `_label` value: it must be a STRING (a quoted `"..."` or a ` ``` `
/// heredoc), same as any string value. A bare/unquoted label (`_label: raw`) or a
/// non-string (`_label: 42`) fails loud rather than silently coercing.
fn parse_label_value(value: &str, span: Span, errors: &mut Vec<CompileError>) -> Option<String> {
    let t = value.trim();
    if let Some(text) = unescape_heredoc(t) {
        Some(text)
    } else if t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
        Some(unescape(&t[1..t.len() - 1]))
    } else {
        errors.push(CompileError::at(span, format!(
            "'_label' has an invalid value `{t}`: a label must be a quoted string (`_label: \"{t}\"`)."
        )));
        None
    }
}

/// Lower a CONNECTION CST node into a ParsedConnection. Endpoints are kept as
/// written; group-internal rescoping (`self` -> boundary, local child -> scoped)
/// is applied by `lower_group` after the group's children are known.
/// `group_scope` is the enclosing GROUP's scoped id (None at the file root),
/// passed to `lower_inline_expr` as the synthesized anon node's scope (its
/// `parent_id`) when the RHS is an inline expr.
fn lower_connection(
    conn: &crate::cst::SyntaxNode,
    group_scope: Option<&str>,
    li: &LineIndex,
    inline: &mut InlineScope,
    errors: &mut Vec<CompileError>,
) -> Option<ParsedConnection> {
    use crate::cst::SyntaxKind as K;
    let eps: Vec<crate::cst::SyntaxNode> = conn.children().filter(|n| n.kind() == K::ENDPOINT).collect();
    // A well-formed endpoint is `id` or `id.port` (1-2 segments). A 3+-segment
    // ref (`a.b.c`) is malformed: reject it loudly rather than silently keeping
    // the first two segments and wiring a wrong edge.
    for ep in &eps {
        if endpoint_overlong(ep) {
            errors.push(CompileError::at(li.span_of(ep), "invalid reference: expected 'id' or 'id.port', not a longer dotted path"));
            return None;
        }
    }
    let target = eps.first()?;
    let (t_id, t_port) = endpoint_id_port(target);
    // RHS = an INLINE_EXPR (`target.port = Type{...}.out`): synthesize the anon
    // node + edge into `target.port`, the SAME path a config-field inline expr
    // takes. Returns no plain edge (the synthesized edge carries the wiring).
    if let Some(rhs_inline) = conn.children().find(|n| n.kind() == K::INLINE_EXPR) {
        // The host is the as-written target `t_id` (raw); the anon node + edge
        // are built raw and scoped by `lower_group` in one pass. `group_scope`
        // is the node's scope (parent_id).
        lower_inline_expr(&rhs_inline, &t_id, &t_port, group_scope, li, inline, errors);
        return None;
    }
    // RHS = a second endpoint (`src.port`): a plain edge. If there is NO second
    // endpoint, the RHS is a LITERAL. A literal is the "visual config" sugar that
    // only a NODE has: `node.port = "v"` fills that node's config. The valid
    // node-config case is peeled off upstream (`literal_config_fill` + node
    // lookup) before reaching here, so a literal arriving in `lower_connection`
    // targets something with no config: a group boundary (`self.port`), a
    // group/include alias port, or an undeclared target. All are invalid: a port
    // that isn't a node's own config is driven by WIRING (`= src.out`, or an
    // inline node `= Text { value: "v" }.value`), never a bare constant. Reject
    // loudly instead of emitting a phantom edge with an empty source.
    let Some(src_ep) = eps.get(1) else {
        let target_desc = if t_port.is_empty() { t_id.clone() } else { format!("{t_id}.{t_port}") };
        // Span the TARGET endpoint (the culprit), not the whole line.
        errors.push(CompileError::at(
            li.span_of(target),
            format!("cannot assign a literal to '{target_desc}': only a node's own port takes a literal config value. Drive this port by wiring (`{target_desc} = source.out`) or an inline node (`{target_desc} = Text {{ value: \"...\" }}.value`)."),
        ));
        return None;
    };
    let (s_id, s_port) = endpoint_id_port(src_ep);
    Some(ParsedConnection {
        source_id: s_id,
        source_port: s_port,
        target_id: t_id,
        target_port: t_port,
        span: Some(li.span_of(conn)),
    })
}

/// True if an ENDPOINT has more than the allowed `id.port` (2) segments.
fn endpoint_overlong(ep: &crate::cst::SyntaxNode) -> bool {
    crate::cst::nodes::Endpoint::cast(ep.clone()).map(|e| e.segments().len() > 2).unwrap_or(false)
}

/// (id, port) of an ENDPOINT node, with absent parts as empty strings. Delegates
/// to the typed view's `Endpoint::parts` (the single endpoint extractor).
fn endpoint_id_port(ep: &crate::cst::SyntaxNode) -> (String, String) {
    let (id, port) = crate::cst::nodes::Endpoint::cast(ep.clone())
        .map(|e| e.parts())
        .unwrap_or((None, None));
    (id.unwrap_or_default(), port.unwrap_or_default())
}

/// Lower a GROUP_DECL recursively into a ParsedGroup. `source_id` is the file's
/// filename-derived identity, used as the local id of a top-level anonymous
/// `Group(){...}` (`Untitled` for an unsaved buffer).
fn lower_group(
    g: &crate::cst::nodes::GroupDecl,
    parent: Option<&str>,
    source_id: &str,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) -> Option<ParsedGroup> {
    let header = g.header()?;
    let (local_id, _ty) = header_id_and_type(header.syntax());
    // Diagnostics point at the HEADER (where the user wrote the decl), not the
    // decl-node start which includes leading comment/blank-line trivia.
    let header_span = li.span_of(header.syntax());
    // Reserved names (`self`, `Group`/`Passthrough`, the `__` compiler-id
    // separator) rejected loudly, mirroring the node path via the shared gate.
    // The anonymous case (empty local) skips the gate: it takes `source_id`
    // below, which `derive_id` already guarantees is a clean bare identifier
    // (no `__`, never a reserved word).
    if !local_id.is_empty() && !reject_reserved_local(&local_id, header_span, errors) {
        return None;
    }
    // An anonymous group (`Group(...)` with no `name =`) IS the file: its id
    // comes from the filename (`my-cleaner.weft` -> `MyCleaner`, an unsaved
    // buffer -> `Untitled`). Same id at parse, edit, and render: no sentinel.
    // Only the FILE's top-level group may be anonymous (it's the file's single
    // interface); a NESTED `Group(){}` with no name has no source to take an id
    // from, so reject it loudly rather than silently inventing `{source_id}.{source_id}`.
    let anonymous = local_id.is_empty();
    if anonymous && parent.is_some() {
        errors.push(CompileError::at(header_span, "a nested group must be named (`name = Group(...)`); only a file's top-level group may be anonymous"));
        return None;
    }
    let local_id = if anonymous { source_id.to_string() } else { local_id };
    let id = scoped(parent, &local_id);
    let (in_ports, out_ports, one_of_required) = lower_header_ports(header.syntax(), li, errors);

    let mut group = ParsedGroup {
        id: id.clone(),
        kind: GroupKind::Group,
        in_ports: Vec::new(),
        out_ports,
        one_of_required: Vec::new(),
        nodes: Vec::new(),
        connections: Vec::new(),
        child_groups: Vec::new(),
        anonymous,
        includes: Vec::new(),
        loop_config: None,
        loop_config_spans: Default::default(),
        span: Some(li.span_of(g.syntax())),
        header_span: Some(header_span),
    };
    lower_grouplike_body(&mut group, g.body(), parent, source_id, in_ports, one_of_required, header_span, li, errors);
    Some(group)
}

/// Lower a LOOP_DECL recursively into a ParsedGroup with kind=Loop and a
/// `loop_config` block. The body is MIXED: it can carry config fields
/// (`parallel: true`, `over: ["x"]`, etc.) AND nested decls/connections.
fn lower_loop(
    l: &crate::cst::nodes::LoopDecl,
    parent: Option<&str>,
    source_id: &str,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) -> Option<ParsedGroup> {
    let header = l.header()?;
    let (local_id, _ty) = header_id_and_type(header.syntax());
    let header_span = li.span_of(header.syntax());
    if local_id.is_empty() {
        errors.push(CompileError::at(header_span, "a Loop must be named (`name = Loop(...)`)"));
        return None;
    }
    if !reject_reserved_local(&local_id, header_span, errors) {
        return None;
    }
    let id = scoped(parent, &local_id);
    let (in_ports, out_ports, one_of_required) = lower_header_ports(header.syntax(), li, errors);

    let mut group = ParsedGroup {
        id: id.clone(),
        kind: GroupKind::Loop,
        in_ports: Vec::new(),
        out_ports,
        one_of_required: Vec::new(),
        nodes: Vec::new(),
        connections: Vec::new(),
        child_groups: Vec::new(),
        anonymous: false,
        includes: Vec::new(),
        loop_config: Some(serde_json::Map::new()),
        loop_config_spans: Default::default(),
        span: Some(li.span_of(l.syntax())),
        header_span: Some(header_span),
    };
    lower_grouplike_body(&mut group, l.body(), parent, source_id, in_ports, one_of_required, header_span, li, errors);
    Some(group)
}

/// Shared body lowering for the two group-like decls (`Group` and `Loop`).
/// Walks the body children (decls, connections, includes, directives),
/// merges inline-expression scratch nodes, applies deferred literal-config
/// fills, and rescopes connection endpoints into the group's scope. The
/// only kind-specific parts are the CONFIG_FIELD / LABEL_FIELD arms (a
/// Loop captures its config block; a plain Group takes neither and errors
/// loudly) and the post-walk carry-port input synthesis (Loop only).
fn lower_grouplike_body(
    group: &mut ParsedGroup,
    body: Option<crate::cst::nodes::Body>,
    parent: Option<&str>,
    source_id: &str,
    mut in_ports: Vec<ParsedPort>,
    mut one_of_required: Vec<Vec<String>>,
    header_span: Span,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) {
    use crate::cst::nodes::Decl as CstDecl;
    use crate::cst::SyntaxKind as K;
    let id = group.id.clone();

    let Some(body) = body else {
        group.in_ports = in_ports;
        group.one_of_required = one_of_required;
        return;
    };

    let mut inline = InlineScope::default();
    // Defer connections so child nodes exist first: a `child.field = <lit>`
    // connection is a config-origin field on that child, not an edge.
    let mut conn_nodes: Vec<crate::cst::SyntaxNode> = Vec::new();
    for child in body.syntax().children() {
        match child.kind() {
            K::CONFIG_FIELD => match group.kind {
                GroupKind::Loop => {
                    // Loop config field. Parse via the shared helper and
                    // copy each value into the loop_config map. Key/value
                    // SEMANTICS (known keys, types, required `parallel`)
                    // are validate's job; lowering rejects only what
                    // validate can never see because it would vanish or
                    // corrupt the shape before validate runs.
                    let mut tmp_cfg: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
                    let mut tmp_spans: std::collections::BTreeMap<String, ConfigFieldSpan> = Default::default();
                    let mut tmp_label: Option<String> = None;
                    let mut tmp_inline = InlineScope::default();
                    lower_config_field(
                        &child,
                        &id,
                        parent,
                        li,
                        &mut tmp_cfg,
                        &mut tmp_label,
                        &mut tmp_spans,
                        &mut tmp_inline,
                        errors,
                    );
                    // An inline expression as a loop-config value would
                    // synthesize helper nodes that nothing wires up; the
                    // value itself would silently disappear.
                    if !tmp_inline.nodes.is_empty() || !tmp_inline.connections.is_empty() {
                        errors.push(CompileError::at(
                            li.span_of(&child),
                            format!("loop '{id}': inline expressions are not valid loop config values"),
                        ));
                    }
                    if let Some(cfg) = group.loop_config.as_mut() {
                        for (k, v) in tmp_cfg {
                            // `parentId` is the boundary pointer flatten
                            // merges into the LoopIn config; a user key
                            // with that name would clobber the loop's
                            // wiring before validate could object.
                            if k == "parentId" {
                                errors.push(CompileError::at(
                                    li.span_of(&child),
                                    format!("loop '{id}': 'parentId' is a reserved loop config key"),
                                ));
                                continue;
                            }
                            cfg.insert(k, v);
                        }
                    }
                    for (k, sp) in tmp_spans {
                        group.loop_config_spans.insert(k, sp);
                    }
                }
                GroupKind::Group => {
                    // Falling into the catch-all would silently eat the
                    // field; plain groups carry no config block.
                    errors.push(CompileError::at(
                        li.span_of(&child),
                        format!("group '{id}': groups do not take config fields; did you mean a Loop?"),
                    ));
                }
            },
            K::LABEL_FIELD => {
                // Falling into the catch-all would silently drop the
                // label; neither group-like decl carries one.
                let noun = match group.kind {
                    GroupKind::Loop => "loop",
                    GroupKind::Group => "group",
                };
                errors.push(CompileError::at(
                    li.span_of(&child),
                    format!("{noun} '{id}': {noun}s do not take a 'label' field"),
                ));
            }
            K::CONNECTION => conn_nodes.push(child.clone()),
            K::NODE_DECL => {
                if let Some(CstDecl::Node(nd)) = CstDecl::cast(child.clone()) {
                    if let Some(node) = lower_node(&nd, Some(&id), li, &mut inline, errors) {
                        if group.has_member_id(&node.id) {
                            errors.push(CompileError::at(dup_span(node.header_span, node.span), format!("Duplicate id '{}'", node.id)));
                        }
                        group.nodes.push(node);
                    }
                }
            }
            K::GROUP_DECL => {
                if let Some(CstDecl::Group(cg)) = CstDecl::cast(child.clone()) {
                    if let Some(child_group) = lower_group(&cg, Some(&id), source_id, li, errors) {
                        if group.has_member_id(&child_group.id) {
                            errors.push(CompileError::at(dup_span(child_group.header_span, child_group.span), format!("Duplicate id '{}'", child_group.id)));
                        }
                        group.child_groups.push(child_group);
                    }
                }
            }
            K::LOOP_DECL => {
                if let Some(CstDecl::Loop(cl)) = CstDecl::cast(child.clone()) {
                    if let Some(child_loop) = lower_loop(&cl, Some(&id), source_id, li, errors) {
                        if group.has_member_id(&child_loop.id) {
                            errors.push(CompileError::at(dup_span(child_loop.header_span, child_loop.span), format!("Duplicate id '{}'", child_loop.id)));
                        }
                        group.child_groups.push(child_loop);
                    }
                }
            }
            K::INCLUDE_DECL => {
                if let Some(CstDecl::Include(ci)) = CstDecl::cast(child.clone()) {
                    if let Some(inc) = lower_include(&ci, Some(&id), li, errors) {
                        // Nodes, child groups, and include aliases share one
                        // id namespace within the group: an alias colliding
                        // with a sibling is a duplicate (parity with the
                        // node/group arms above).
                        if group.has_member_id(&inc.alias) {
                            errors.push(CompileError::at(inc.span, format!("Duplicate id '{}'", inc.alias)));
                        }
                        group.includes.push(inc);
                    }
                }
            }
            K::DIRECTIVE => {
                // @require_one_of directly in the body.
                if let Some(grp) = lower_directive_require_one_of(&child, li, errors) {
                    one_of_required.push(grp);
                }
            }
            _ => {}
        }
    }

    if matches!(group.kind, GroupKind::Loop) {
        // Vet `over` so flatten_group's later read can trust the
        // loop_config map; values themselves aren't needed here.
        // Both calls push CompileErrors on any non-string entry,
        // halting the build before lowering produces a half-baked loop.
        validate_loop_port_list(&id, group.loop_config.as_ref(), "over", header_span, errors);
        // Auto-create carry input ports from the carry config list.
        let carry: Vec<String> = read_loop_port_list(&id, group.loop_config.as_ref(), "carry", header_span, errors);
        // Carry ports: the output side is the source of truth. When
        // the user did not declare the matching input, synthesize it
        // (REQUIRED: it seeds iteration 0, and a fabricated default
        // seed would silently run the whole loop on wrong data).
        // SYNC: carry input synthesis <-> packages/weft-graph/src/webview/lib/
        // projection/apply.ts syncLoopCarryInputs (the editor's optimistic
        // projection derives the same ghost inputs between round-trips).
        // Everything diagnostic about carry (unknown output, type
        // mismatch, optional user-declared input) is reported ONCE,
        // by validate's `check_loop_config`, so a mistake never
        // produces two errors from two layers.
        for carry_name in &carry {
            let out_port = group.out_ports.iter().find(|p| &p.name == carry_name).cloned();
            let already_declared = in_ports.iter().any(|p| &p.name == carry_name);
            if let (Some(out), false) = (out_port, already_declared) {
                in_ports.push(ParsedPort {
                    name: out.name,
                    // The carry input mirrors the carry OUTPUT's
                    // optionality: an optional carry (`acc: Number?`) seeds
                    // the first iteration from its type's zero value when
                    // unwired (the engine fills it), so its input must be
                    // optional too or the LoopIn would never become ready.
                    // A required carry still demands an explicit seed.
                    required: out.required,
                    port_type: out.port_type,
                    synthesized_from_carry: true,
                    type_error: None,
                });
            }
        }
    }
    group.in_ports = in_ports;

    // Scope the inline-expr anon node ids into THIS group (`b__field` ->
    // `g.b__field`), then merge. Inline nodes are synthesized with RAW ids so
    // their ids and their edge endpoints are scoped together below (one pass),
    // which is what keeps a child that shadows the group's name from being
    // double-scoped. `prefix_node_ids` is the ONE place anon ids get scoped.
    prefix_node_ids(&mut inline.nodes, &id);
    merge_inline_nodes(&mut group.nodes, inline.nodes, errors);
    group.connections.extend(inline.connections);

    // Process deferred connections: `child.field = <literal>` is a config-
    // origin field on that local child node; everything else is an edge. A
    // literal to a non-child target is an edge (it may reference an outer or
    // boundary scope, validated downstream), so a missing local target is
    // not an error here.
    // A connection-RHS inline expr (`child.in = T{...}.out`) synthesizes its
    // own anon node + edge into this scratch scope, merged below.
    let mut conn_inline = InlineScope::default();
    for cn in conn_nodes {
        let fill = literal_config_fill(&cn, li)
            .and_then(|f| group.nodes.iter_mut().find(|n| local_of(&n.id) == f.target_id).map(|n| (f, n)));
        match fill {
            Some((f, node)) => apply_literal_fill(&f, &mut node.config, &mut node.config_spans, errors),
            None => {
                if let Some(c) = lower_connection(&cn, Some(&id), li, &mut conn_inline, errors) {
                    group.connections.push(c);
                }
            }
        }
    }
    prefix_node_ids(&mut conn_inline.nodes, &id);
    merge_inline_nodes(&mut group.nodes, conn_inline.nodes, errors);
    group.connections.extend(conn_inline.connections);

    // Rescope group-internal connection ENDPOINTS (the node ids were just
    // scoped by `prefix_node_ids`): `self` -> the group's `__in`/`__out`
    // boundary; a LOCAL-child ref -> `{group}.child` (this also re-prefixes a
    // raw anon endpoint like `b__in` to `g.b__in`, matching its node);
    // anything else is an outer/root ref, left as-is. flatten then rewires
    // passthroughs.
    let local_children: std::collections::HashSet<String> = group
        .nodes
        .iter()
        .map(|n| local_of(&n.id))
        .chain(group.child_groups.iter().map(|g| local_of(&g.id)))
        .chain(group.includes.iter().map(|x| local_of(&x.alias)))
        .collect();
    for conn in &mut group.connections {
        conn.source_id = rescope_endpoint(&conn.source_id, &id, &local_children, true);
        conn.target_id = rescope_endpoint(&conn.target_id, &id, &local_children, false);
    }
    group.one_of_required = one_of_required;
}

/// The local (last `.`-segment) id of a possibly-scoped id.
fn local_of(id: &str) -> String {
    id.rsplit('.').next().unwrap_or(id).to_string()
}

/// Prefix each synthesized inline-expr node's RAW id with the enclosing group id
/// (`b__field` -> `g.b__field`). The ONE place anon node ids get scoped, so they
/// match the edge endpoints `rescope_endpoint` produces from the same raw form,
/// and a child that shadows the group's name can't double-scope.
fn prefix_node_ids(nodes: &mut [ParsedNode], group_id: &str) {
    for n in nodes {
        n.id = scoped(Some(group_id), &n.id);
    }
}

/// Rescope a group-internal connection endpoint id. `self` becomes the group's
/// boundary passthrough (`__in` for a source, `__out` for a target); a local
/// child ref (by its head segment) is prefixed with the group id; an outer ref
/// is left unchanged. The two-probe rule (immediate-child-else-bare) the editor
/// must mirror when validating an edge endpoint.
/// SYNC: rescope_endpoint <-> crates/weft-compiler/src/edit/ops.rs require_endpoint, crates/weft-compiler/src/cst/nodes.rs endpoint_resolves_to
fn rescope_endpoint(
    endpoint: &str,
    group_id: &str,
    local_children: &std::collections::HashSet<String>,
    is_source: bool,
) -> String {
    if endpoint == "self" {
        return format!("{group_id}__{}", if is_source { "in" } else { "out" });
    }
    let head = endpoint.split('.').next().unwrap_or(endpoint);
    if local_children.contains(head) {
        format!("{group_id}.{endpoint}")
    } else {
        endpoint.to_string()
    }
}

/// Lower an INCLUDE_DECL into a ParsedInclude marker.
fn lower_include(
    i: &crate::cst::nodes::IncludeDecl,
    parent: Option<&str>,
    li: &LineIndex,
    errors: &mut Vec<CompileError>,
) -> Option<ParsedInclude> {
    use crate::cst::SyntaxKind as K;
    // alias = leading IDENT; path from the MARKER's @include("...") arg.
    let alias_local = i
        .syntax()
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == K::IDENT)
        .map(|t| t.text().to_string())?;
    // The alias is a user-written name in the same id namespace as nodes/groups;
    // reject a reserved one (`__`, `self`, type keyword) loudly via the shared gate.
    if !reject_reserved_local(&alias_local, li.span_of(i.syntax()), errors) {
        return None;
    }
    let marker = i
        .syntax()
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == K::MARKER)?;
    if crate::cst::marker::directive(marker.text()) != "include" {
        return None;
    }
    // `parse_include_arg` parses the `("path")` arg form; pass the marker's
    // parenthesized body (the single home for marker-arg extraction).
    let after = format!("({})", crate::cst::marker::args_raw(marker.text())?);
    let path = parse_include_arg(&after).ok()?;
    Some(ParsedInclude {
        alias: scoped(parent, &alias_local),
        path,
        span: li.span_of(i.syntax()),
    })
}

/// Scoped id: `parent.local` inside a group, bare local at top level.
fn scoped(parent: Option<&str>, local: &str) -> String {
    match parent {
        Some(p) => format!("{p}.{local}"),
        None => local.to_string(),
    }
}

// ─── Lowering value/id helpers ──────────────────────────────────────────────
// Pure text helpers used by the CST lowering: port-decl text -> ParsedPort,
// include-arg parsing, scoped-id rescoping for includes, scalar value parsing,
// string unquote/unescape, heredoc dedent. (Not parsers; the CST parser owns
// all tokenization. These operate on already-tokenized fragments' text.)

/// Parse a single port declaration.
/// Port declaration syntax: `name: Type` (required by default) or
/// `name: Type?` (optional). No prefix characters.
fn try_parse_port_decl(trimmed: &str) -> Result<ParsedPort, String> {
    let s = trimmed.trim();
    let rest = s;
    let (name, port_type, optional) = if let Some(colon_pos) = rest.find(':') {
        let name = rest[..colon_pos].trim();
        let mut type_str = rest[colon_pos + 1..].trim();

        // Check for `?` suffix (optional marker)
        let optional = type_str.ends_with('?');
        if optional {
            type_str = type_str[..type_str.len() - 1].trim();
        }

        match WeftType::parse(type_str) {
            Some(pt) => (name, pt, optional),
            // An invalid / unknown type is RECOVERABLE: keep the port (with the
            // `MustOverride` placeholder, which the editor renders red as
            // "needs a type") and surface the bad type as a diagnostic, rather
            // than dropping the port so it silently vanishes from the canvas.
            // Mirrors the same keep-and-flag rule enrich applies to a custom port
            // left at `MustOverride`. The error text rides on the returned port
            // via `type_error` so the caller records the squiggle.
            None => return Ok(ParsedPort {
                name: name.to_string(),
                port_type: WeftType::MustOverride,
                required: !optional,
                synthesized_from_carry: false,
                type_error: Some(format!("Invalid port type '{}' on port '{}'", type_str, name)),
            }),
        }
    } else {
        // No type annotation
        let name = rest.trim();
        let optional = name.ends_with('?');
        let name = if optional { name[..name.len() - 1].trim() } else { name };
        if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!("Invalid port name: '{}'", rest.trim()));
        }
        (name, WeftType::default(), optional)
    };

    // Validate port name
    let first = name.chars().next().ok_or_else(|| "Empty port name".to_string())?;
    if !(first.is_alphabetic() || first == '_') {
        return Err(format!("Port name must start with a letter or underscore: '{}'", name));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Port name contains invalid characters: '{}'", name));
    }

    Ok(ParsedPort {
        name: name.to_string(),
        port_type: port_type,
        required: !optional, // v2: required by default, ? makes optional
        synthesized_from_carry: false,
        type_error: None,
    })
}

// ─── Config Block Parsing ───────────────────────────────────────────────────


fn dedent(s: &str) -> String {
    let raw = s.trim_end();
    let min_indent = raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    if min_indent > 0 {
        raw.lines()
            .map(|l| if l.len() >= min_indent { &l[min_indent..] } else { l })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        raw.to_string()
    }
}



/// Parse connections inside a group. Uses `self` instead of `in`/`out`.
/// `child.input = self.port` (child receives from group input)
/// `self.output = child.port` (group output receives from child)
/// `child.port = other_child.port` (internal wiring)
fn parse_dotted(s: &str) -> Option<(String, String)> {
    let dot = s.find('.')?;
    let node = s[..dot].trim();
    let port = s[dot + 1..].trim();
    if node.is_empty() || port.is_empty() {
        return None;
    }
    fn is_bare_ident(s: &str) -> bool {
        let mut chars = s.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
    if !is_bare_ident(node) || !is_bare_ident(port) {
        return None;
    }
    Some((node.to_string(), port.to_string()))
}

/// Parse the argument of `@include(...)`: a single quoted path. `after` is
/// the text following `@include` (e.g. `("components/cleaner.weft")`).
fn parse_include_arg(after: &str) -> Result<String, String> {
    let inner = after
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| "@include expects (\"path\")".to_string())?
        .trim();
    if inner.len() >= 2 && inner.starts_with('"') && inner.ends_with('"') {
        let path = &inner[1..inner.len() - 1];
        if path.is_empty() {
            return Err("@include path is empty".into());
        }
        Ok(path.to_string())
    } else {
        Err(format!("@include path must be a quoted string, got {inner:?}"))
    }
}

/// Parses one `key: value` pair into `config`. Returns the key actually
/// stored (so the caller can record its source span), or `None` if the line
/// wasn't a valid pair or the value was rejected.
fn parse_kv(
    s: &str,
    config: &mut serde_json::Map<String, serde_json::Value>,
    span: Span,
    errors: &mut Vec<CompileError>,
) -> Option<String> {
    let colon_pos = match s.find(':') {
        Some(p) => p,
        None => return None,
    };
    let key = s[..colon_pos].trim();
    let raw = s[colon_pos + 1..].trim();

    // Reject removed config keys
    if key == "mock" || key == "mocked" {
        errors.push(CompileError::at(span, format!("'{}' is not a valid config key. Use test configs for mocking.", key)));
        return None;
    }

    // Hard break: pre-arch4 keys were renamed to leading-underscore
    // form. Surface a clear migration error so projects don't pick
    // up the old behavior silently.
    if key == "label" {
        errors.push(CompileError::at(span, "'label' was renamed to '_label' (reserved internal key)"));
        return None;
    }
    if key == "is_output" {
        errors.push(CompileError::at(span, "'is_output' was renamed to '_is_output' (reserved internal key)"));
        return None;
    }

    // `_label` is NOT a config value: it is the node's LABEL, set ONLY via the
    // body `_label: "..."` field (which routes through `parse_label_value` into
    // `node.label`, never here). Reaching `parse_kv` with `_label` means a
    // connection-origin `node._label = ...`, which would misroute the label into
    // `config["_label"]` where nothing reads it. Reject loud so a label has one
    // home (`node.label`) and one syntax (the body field).
    if key == "_label" {
        errors.push(CompileError::at(span, "a node's label is set with a body field `_label: \"...\"`, not a connection `node._label = ...`".to_string()));
        return None;
    }
    // Other reserved internal keys (`_is_output`, `_tags`) ARE config keys (read
    // from `config` downstream); anything else with a leading underscore is
    // rejected so the user doesn't collide with a future reserved field.
    if key.starts_with('_') {
        const ALLOWED: &[&str] = &["_is_output", weft_core::tag::TAGS_CONFIG_KEY];
        if !ALLOWED.contains(&key) {
            errors.push(CompileError::at(span, format!(
                "'{key}' starts with '_' which is reserved for internal config keys. \
                 Allowed reserved keys: {}",
                ALLOWED.join(", ")
            )));
            return None;
        }
    }

    let value = if raw == "true" {
        serde_json::Value::Bool(true)
    } else if raw == "false" {
        serde_json::Value::Bool(false)
    } else if raw.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') && !raw.is_empty() {
        // A numeric-shaped value: parse as integer first, then float. A value that
        // can't become a finite JSON number (a malformed number like `1.2.3`, or a
        // magnitude that overflows f64 to infinity) is NEVER silently coerced, it
        // fails loud, same as the gate's other branches. `Number::from_f64`
        // returns None for non-finite, which is exactly the "no JSON number for
        // this" signal (`json!(f64::INFINITY)` would otherwise yield a silent
        // `null`). (A malformed numeric is in practice already lexed to an ERROR
        // token and never reaches here as a NUMBER, so the loud arm is the gate
        // holding by construction, not by luck.)
        let number = raw.parse::<i64>().ok().map(serde_json::Value::from)
            .or_else(|| raw.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(serde_json::Value::from));
        match number {
            Some(v) => v,
            None => {
                errors.push(CompileError::at(span, format!(
                    "'{key}' has a malformed or out-of-range numeric value `{raw}`. A literal string must be quoted (`{key}: \"...\"`)."
                )));
                return None;
            }
        }
    } else if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        serde_json::Value::String(unescape(&raw[1..raw.len() - 1]))
    } else if raw.starts_with('[') || raw.starts_with('{') {
        // JSON array/object. Malformed JSON is NOT silently coerced to a string
        // (that hid `[a, b]`-style typos); it fails loud.
        match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(e) => {
                errors.push(CompileError::at(span, format!(
                    "'{key}' has an invalid JSON value `{raw}`: {e}. A literal string must be quoted (`{key}: \"...\"`)."
                )));
                return None;
            }
        }
    } else if raw.starts_with('@') {
        // `@file(...)` is the ONLY marker valid as a config value (resolved
        // downstream by file_ref). Any other `@...` (a typo, `@include`,
        // `@require_one_of`, a bare `@`) is not a value and fails loud rather than
        // becoming a literal string.
        if crate::cst::marker::directive(raw) == "file" {
            serde_json::Value::String(raw.to_string())
        } else {
            errors.push(CompileError::at(span, format!(
                "'{key}' has an invalid marker value `{raw}`: the only marker valid as a config value is `@file(\"path\")`."
            )));
            return None;
        }
    } else if raw.is_empty() {
        // No value text reached us: the source had a key with nothing parseable
        // after it (e.g. a malformed token the lexer peeled off into an ERROR, so
        // `value: 1.2.3` arrives here as an empty value). Name the missing value
        // rather than printing empty backticks (`invalid value ``).
        errors.push(CompileError::at(span, format!(
            "'{key}' is missing a value (or its value is malformed). A literal string must be quoted (`{key}: \"...\"`); to wire a port, reference it as `node_id.port_name`."
        )));
        return None;
    } else {
        // An UNQUOTED, non-bool, non-numeric, non-marker scalar is not a valid
        // value: a string literal must be quoted (`"{raw}"`) and a port reference
        // must be dotted (`node.port`). A bare identifier here (e.g. `text = raw`
        // meaning to wire the port `raw`) was silently coerced to the string
        // `"raw"`, dropping the user's intent with no diagnostic. Reject loudly.
        errors.push(CompileError::at(span, format!(
            "'{key}' has an invalid value `{raw}`: a literal string must be quoted (`{key}: \"{raw}\"`); to wire a port, reference it as `node_id.port_name`."
        )));
        return None;
    };

    // _tags is the only reserved key that carries user-supplied
    // strings used downstream as filter values (token-scoped
    // enumeration). Validate the charset at parse time so the same
    // rule fires regardless of whether the project came from a
    // hand-edited .weft or from the AI builder.
    if key == weft_core::tag::TAGS_CONFIG_KEY {
        if let Some(arr) = value.as_array() {
            // Every element MUST be a string. `filter_map(as_str)` silently DROPPED
            // non-string elements, so `_tags: ["ok", 5, {}]` compiled "successfully"
            // as `["ok"]`, discarding user data with no error. Reject loudly instead.
            let mut tags: Vec<String> = Vec::with_capacity(arr.len());
            for v in arr {
                match v.as_str() {
                    Some(s) => tags.push(s.to_string()),
                    None => {
                        errors.push(CompileError::at(
                            span,
                            format!("_tags must contain only strings; found {v}"),
                        ));
                        return None;
                    }
                }
            }
            if let Err(e) = weft_core::tag::validate_tags(&tags) {
                errors.push(CompileError::at(span, format!("invalid _tags: {e}")));
                return None;
            }
        } else {
            errors.push(CompileError::at(span, "_tags must be a list of strings, e.g. _tags: [\"support\"]"));
            return None;
        }
    }

    config.insert(key.to_string(), value);
    Some(key.to_string())
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some(other) => { out.push('\\'); out.push(other); }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}


// ─── Flattener ──────────────────────────────────────────────────────────────

fn flatten(state: ParseState, project_id: Uuid) -> ProjectDefinition {
    let mut nodes: Vec<NodeDefinition> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();

    let now = chrono::Utc::now();

    // Add top-level nodes
    for pn in &state.nodes {
        nodes.push(parsed_to_node_def(pn));
    }

    // Add top-level connections
    for pc in &state.connections {
        edges.push(parsed_to_edge(pc));
    }

    // Collect the structured group tree BEFORE flattening so the
    // GroupDefinitions capture the pre-flatten shape (which node ids
    // are direct members, which are grandchildren, etc).
    let mut groups: Vec<weft_core::GroupDefinition> = Vec::new();
    for group in &state.groups {
        collect_group_definitions(group, None, &mut groups);
    }

    // Flatten each group (recursively handles nested groups)
    for group in &state.groups {
        flatten_group(group, &mut nodes, &mut edges);
    }

    // Deduplicate nodes by id, then edges. A node-id collision is already a loud
    // parse error (`Duplicate id`) that aborts the strict build, but the lenient
    // render path (`compile_lenient`) always returns a project, so without this
    // it would hand the renderer two `NodeDefinition`s with one id (a broken
    // contract for anything keyed by id, e.g. two `g.c__in` from an alias clashing
    // with a same-named group). Keep the first; the diagnostic already tells the
    // user to fix the source.
    {
        let mut seen = std::collections::HashSet::new();
        nodes.retain(|n| seen.insert(n.id.clone()));
    }
    {
        let mut seen = std::collections::HashSet::new();
        edges.retain(|e| {
            let key = (
                e.source.clone(),
                e.source_handle.clone().unwrap_or_default(),
                e.target.clone(),
                e.target_handle.clone().unwrap_or_default(),
            );
            seen.insert(key)
        });
    }

    ProjectDefinition {
        id: project_id,
        nodes,
        edges,
        groups,
        created_at: now,
        updated_at: now,
    }
}

/// Walk the ParsedGroup tree and emit a GroupDefinition per group.
/// Direct children go into `node_ids`; nested groups go into
/// `child_group_ids` and recurse with their own entry.
fn collect_group_definitions(
    group: &ParsedGroup,
    parent_group_id: Option<String>,
    out: &mut Vec<weft_core::GroupDefinition>,
) {
    let in_ports: Vec<PortDefinition> = group
        .in_ports
        .iter()
        .map(|p| PortDefinition {
            name: p.name.clone(),
            port_type: p.port_type.clone(),
            required: p.required,
            description: None,
            configurable: p.port_type.is_default_configurable(),
            synthesized_from_carry: p.synthesized_from_carry,
        })
        .collect();
    let out_ports: Vec<PortDefinition> = group
        .out_ports
        .iter()
        .map(|p| PortDefinition {
            name: p.name.clone(),
            port_type: p.port_type.clone(),
            required: false,
            description: None,
            configurable: p.port_type.is_default_configurable(),
            synthesized_from_carry: false,
        })
        .collect();

    // Direct node members (already scoped to this group's id by the
    // parser's rescope pass).
    let node_ids: Vec<String> = group.nodes.iter().map(|n| n.id.clone()).collect();
    let child_group_ids: Vec<String> = group.child_groups.iter().map(|g| g.id.clone()).collect();

    let kind = match group.kind {
        GroupKind::Group => weft_core::GroupKind::Group,
        GroupKind::Loop => weft_core::GroupKind::Loop {
            loop_config: serde_json::Value::Object(
                group
                    .loop_config
                    .clone()
                    .expect("lower_loop always seeds loop_config for Loop groups"),
            ),
        },
    };
    out.push(weft_core::GroupDefinition {
        id: group.id.clone(),
        kind,
        label: Some(group.id.clone()),
        in_ports,
        out_ports,
        one_of_required: group.one_of_required.clone(),
        parent_group_id: parent_group_id.clone(),
        child_group_ids,
        node_ids,
        anonymous: group.anonymous,
        span: group.span.clone(),
        header_span: group.header_span.clone(),
    });

    for child in &group.child_groups {
        collect_group_definitions(child, Some(group.id.clone()), out);
    }
}

/// Build the scope chain for a group ID.
/// "outer.inner" → ["outer", "outer.inner"]
/// "mygroup" → ["mygroup"]
fn build_scope_chain(group_id: &str) -> Vec<String> {
    let mut chain = Vec::new();
    let parts: Vec<&str> = group_id.split('.').collect();
    for i in 0..parts.len() {
        chain.push(parts[..=i].join("."));
    }
    chain
}

fn flatten_group(
    group: &ParsedGroup,
    nodes: &mut Vec<NodeDefinition>,
    edges: &mut Vec<Edge>,
) {
    let internal_scope = build_scope_chain(&group.id);
    let boundary_scope = if internal_scope.len() > 1 {
        internal_scope[..internal_scope.len() - 1].to_vec()
    } else {
        vec![]
    };

    let (in_type, out_type) = match group.kind {
        GroupKind::Group => ("Passthrough", "Passthrough"),
        GroupKind::Loop => ("LoopIn", "LoopOut"),
    };

    // Loop-only: parse `over` / `carry` to derive the boundary port shapes.
    // For a Loop:
    //   - LoopIn.inputs (outer-in) mirror the user's input signature as
    //     declared (List[T] for ports in `over`, T for broadcast, T for
    //     carry initial).
    //   - LoopIn.outputs (inside-out) carry the per-iteration element
    //     type for `over` ports (T), the broadcast value (T), and add
    //     the implicit `self.index: Number` port.
    //   - LoopOut.inputs (inside-in) carry the body's writes: gather
    //     ports as T?, carry-write ports as T?, plus the implicit
    //     `self.done: Boolean?` port.
    //   - LoopOut.outputs (outer-out) are the assembled lists for
    //     gather ports (the user-declared List[T | Null]) and the final
    //     carry value for carry ports (T).
    //
    // For a Group, all four sides mirror the user's declared types.
    let (loop_over, loop_carry): (Vec<String>, Vec<String>) = match group.kind {
        GroupKind::Loop => (
            read_loop_port_list_vetted(group.loop_config.as_ref(), "over"),
            read_loop_port_list_vetted(group.loop_config.as_ref(), "carry"),
        ),
        GroupKind::Group => (Vec::new(), Vec::new()),
    };

    let elem_type = |ty: &weft_core::weft_type::WeftType| -> weft_core::weft_type::WeftType {
        match ty {
            weft_core::weft_type::WeftType::List(inner) => (**inner).clone(),
            other => other.clone(),
        }
    };

    let in_pt_id = format!("{}__in", group.id);
    let in_pt_inputs: Vec<PortDefinition> = group.in_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: p.required,
        description: None,
        configurable: p.port_type.is_default_configurable(),
        synthesized_from_carry: p.synthesized_from_carry,
    }).collect();
    let mut in_pt_outputs: Vec<PortDefinition> = group.in_ports.iter().map(|p| {
        let ty = if matches!(group.kind, GroupKind::Loop) && loop_over.contains(&p.name) {
            elem_type(&p.port_type)
        } else {
            p.port_type.clone()
        };
        PortDefinition {
            name: p.name.clone(),
            port_type: ty.clone(),
            required: false,
            description: None,
            configurable: ty.is_default_configurable(),
            synthesized_from_carry: false,
        }
    }).collect();
    // Implicit `self.index: Number` for loops. If the user declared a
    // port named `index` validate already emitted a reserved-name
    // diagnostic; skip pushing the implicit one rather than producing a
    // structurally duplicate port that downstream port-by-name lookups
    // would silently disambiguate.
    if matches!(group.kind, GroupKind::Loop) && !in_pt_outputs.iter().any(|p| p.name == "index") {
        in_pt_outputs.push(PortDefinition {
            name: "index".to_string(),
            port_type: weft_core::weft_type::WeftType::primitive(weft_core::weft_type::WeftPrimitive::Number),
            required: false,
            description: None,
            configurable: false,
            synthesized_from_carry: false,
        });
    }

    let mut in_features = NodeFeatures::default();
    in_features.one_of_required = group.one_of_required.clone();
    // Stash loop config on the boundary node's `config` JSON so the
    // engine reads it without a separate registry. parentId is kept so
    // the existing webview rendering doesn't break.
    let mut in_cfg = serde_json::json!({"parentId": group.id});
    if let (GroupKind::Loop, Some(lc)) = (group.kind, &group.loop_config) {
        if let Some(obj) = in_cfg.as_object_mut() {
            for (k, v) in lc {
                obj.insert(k.clone(), v.clone());
            }
            // `parallel` defaults to false (sequential): materialized
            // HERE so the flattened config always carries one explicit
            // value and the runtime never holds its own default. The
            // other knobs default by absence (`max_iters` = no cap,
            // `over`/`carry` = empty).
            obj.entry("parallel")
                .or_insert(serde_json::Value::Bool(false));
        }
    }
    let loop_spans: std::collections::BTreeMap<String, ConfigFieldSpan> =
        if matches!(group.kind, GroupKind::Loop) {
            group.loop_config_spans.clone()
        } else {
            Default::default()
        };
    nodes.push(NodeDefinition {
        id: in_pt_id.clone(),
        node_type: in_type.to_string(),
        label: Some(boundary_label(&group.id, weft_core::project::GroupBoundaryRole::In)),
        config: in_cfg,
        position: Position { x: 0.0, y: 0.0 },
        inputs: in_pt_inputs,
        outputs: in_pt_outputs,
        features: in_features,
        scope: boundary_scope.clone(),
        group_boundary: Some(GroupBoundary { group_id: group.id.clone(), role: GroupBoundaryRole::In }),
        requires_infra: false,
        images: Vec::new(),
        span: None,
        header_span: None,
        config_spans: loop_spans,
        file_refs: Default::default(),
        include_path: None,
    });

    let out_pt_id = format!("{}__out", group.id);
    let mut out_pt_inputs: Vec<PortDefinition> = group.out_ports.iter().map(|p| {
        let (ty, required) = if matches!(group.kind, GroupKind::Loop) {
            if loop_carry.contains(&p.name) {
                // Carry-write port: T (engine reads optional/closed at runtime).
                (p.port_type.clone(), false)
            } else {
                // Gather-write port: the element under List[T | Null] is what
                // the body writes per iteration; the engine substitutes null
                // on closure.
                (elem_type(&p.port_type), false)
            }
        } else {
            (p.port_type.clone(), false)
        };
        PortDefinition {
            name: p.name.clone(),
            port_type: ty.clone(),
            required,
            description: None,
            configurable: ty.is_default_configurable(),
            synthesized_from_carry: false,
        }
    }).collect();
    // Implicit `self.done: Boolean` for loops. Skip if the user declared a
    // port named `done` (validate reports the reserved-name collision);
    // a duplicate port would silently shadow at the next port-by-name lookup.
    if matches!(group.kind, GroupKind::Loop) && !out_pt_inputs.iter().any(|p| p.name == "done") {
        out_pt_inputs.push(PortDefinition {
            name: "done".to_string(),
            port_type: weft_core::weft_type::WeftType::primitive(weft_core::weft_type::WeftPrimitive::Boolean),
            required: false,
            description: None,
            configurable: false,
            synthesized_from_carry: false,
        });
    }
    let out_pt_outputs: Vec<PortDefinition> = group.out_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: false,
        description: None,
        configurable: p.port_type.is_default_configurable(),
        synthesized_from_carry: false,
    }).collect();

    // The OUT boundary carries only the parent pointer. Loop config
    // (parallel, over, carry, max_iters, trim_on_mismatch) is authoritative
    // on LoopIn; mirroring it on LoopOut would create two sources of truth
    // for the same fields.
    let out_cfg = serde_json::json!({"parentId": group.id});
    nodes.push(NodeDefinition {
        id: out_pt_id.clone(),
        node_type: out_type.to_string(),
        label: Some(boundary_label(&group.id, weft_core::project::GroupBoundaryRole::Out)),
        config: out_cfg,
        position: Position { x: 0.0, y: 0.0 },
        inputs: out_pt_inputs,
        outputs: out_pt_outputs,
        features: NodeFeatures::default(),
        scope: boundary_scope.clone(),
        group_boundary: Some(GroupBoundary { group_id: group.id.clone(), role: GroupBoundaryRole::Out }),
        requires_infra: false,
        images: Vec::new(),
        span: None,
        header_span: None,
        config_spans: Default::default(),
        file_refs: Default::default(),
        include_path: None,
    });

    // 3. Add internal nodes
    for pn in &group.nodes {
        nodes.push(parsed_to_node_def(pn));
    }

    // 4. Add internal connections
    for pc in &group.connections {
        edges.push(parsed_to_edge(pc));
    }

    // 5. Rewrite edges that reference the group ID directly
    for edge in edges.iter_mut() {
        if edge.target == group.id {
            edge.target = in_pt_id.clone();
        }
        if edge.source == group.id {
            edge.source = out_pt_id.clone();
        }
    }

    // 6. Recursively flatten child groups
    for child in &group.child_groups {
        flatten_group(child, nodes, edges);
    }
}

fn parsed_to_node_def(pn: &ParsedNode) -> NodeDefinition {
    let mut config = serde_json::Value::Object(pn.config.clone());
    if let Some(pid) = &pn.parent_id {
        config.as_object_mut().unwrap().insert("parentId".to_string(), serde_json::Value::String(pid.clone()));
    }
    let inputs = pn.in_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: p.required,
        description: None,
        configurable: p.port_type.is_default_configurable(),
        synthesized_from_carry: false,
    }).collect();
    let outputs = pn.out_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: p.required,
        description: None,
        configurable: p.port_type.is_default_configurable(),
        synthesized_from_carry: false,
    }).collect();
    let mut features = NodeFeatures::default();
    features.one_of_required = pn.one_of_required.clone();
    let scope = match &pn.parent_id {
        Some(pid) => build_scope_chain(pid),
        None => vec![],
    };
    NodeDefinition {
        id: pn.id.clone(),
        node_type: pn.node_type.clone(),
        label: pn.label.clone(),
        config,
        position: Position { x: 0.0, y: 0.0 },
        inputs,
        outputs,
        features,
        scope,
        group_boundary: None,
        requires_infra: false,
        images: Vec::new(),
        span: pn.span,
        header_span: pn.header_span,
        config_spans: pn.config_spans.clone(),
        file_refs: pn.file_refs.clone(),
        include_path: pn.include_path.clone(),
    }
}

fn parsed_to_edge(pc: &ParsedConnection) -> Edge {
    Edge {
        id: edge_id(&pc.source_id, &pc.source_port, &pc.target_id, &pc.target_port),
        source: pc.source_id.clone(),
        target: pc.target_id.clone(),
        source_handle: Some(pc.source_port.clone()),
        target_handle: Some(pc.target_port.clone()),
        span: pc.span,
    }
}

// ─── Inline Expressions ─────────────────────────────────────────────────────
//
// Inline syntax lets the user declare a short-lived child node directly in
// the position where its output would otherwise be wired:
//
//     target.port = Template { template: "hi" }.text
//
//     my_llm = LlmInference {
//       systemPrompt: Template { template: "{{x}}" x: other.value }.text
//     }
//
// The parser recognizes the inline form natively during its main pass and
// emits a ParsedNode (the anon child) plus a ParsedConnection (the edge
// from anon.output to parent.field) into the current scope's InlineScope
// accumulator.
//
// Rules:
//   - Inline expressions are only allowed on the RHS of an edge assignment,
//     or as a config-field value inside a node declaration.
//   - The trailing `.portName` is mandatory.
//   - No post-config outputs: writing `Type { ... } -> (out: X).out` inline
//     is rejected. Declare the node with a name if you need post-config outs.
//   - Anon IDs: `{parent_id}__{field_or_port_name}`. Uniqueness is enforced
//     at the scope merge point (state.nodes / group.nodes).
//   - Nested inlines work naturally via recursion: the inline's body is a
//     config block parsed by the same config-body lowering that handles the
//     outer config, so a nested inline in a nested config field is picked
//     up in the same pass.



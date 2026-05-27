//! Structured source editing.
//!
//! Editor frontends (VS Code webview today, others later) never touch `.weft`
//! text. They send edit INTENTS (`EditOp`); this module applies them to the
//! source and returns the new source. Editing is the compiler's job: it has the
//! real AST + spans (no regex guessing), so a rename or a field update is a
//! precise splice, not a fragile pattern match.
//!
//! `apply_edits` takes the source + an ordered list of ops and applies them
//! atomically. After each op the source is re-parsed, so every op sees fresh
//! spans: span-shift bookkeeping disappears because parsing is just a function
//! call here (unlike a frontend, which would need a round-trip per op).
//!
//! Lookups run against the flattened `ProjectDefinition` (the same shape the
//! frontend's ids come from: scoped ids like `grp.child`, every node/edge/group
//! carrying its source span). We reuse `compile` to produce it (no catalog, no
//! enrichment needed for structural editing).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use weft_core::project::{ConfigOrigin, GroupDefinition, NodeDefinition, ProjectDefinition, Span};

use crate::weft_compiler::{compile_with_mode, IncludeMode};

/// A structured edit intent. `serde` tag `op` matches the parse-server wire.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum EditOp {
    /// Set (or insert) a config field. `value` is the already-formatted source
    /// token (`"hi"`, `42`, a `@file(...)` marker, multi-line JSON), produced
    /// by the frontend's value formatter.
    SetConfig { node: String, key: String, value: String },
    /// Remove a config field.
    RemoveConfig { node: String, key: String },
    /// Set or clear a node's label.
    SetLabel { node: String, label: Option<String> },
    /// Add a bare node `id = Type {}` at the end of the scope (top level when
    /// `parent_group` is None).
    AddNode { id: String, node_type: String, parent_group: Option<String> },
    /// Remove a node and every connection referencing it.
    RemoveNode { node: String },
    /// Add `target.port = source.port`, replacing any existing driver of the
    /// same target port (input ports are single-driver).
    AddEdge { source: String, source_port: String, target: String, target_port: String, scope_group: Option<String> },
    /// Remove a connection line. `scope_group` is the group whose body the
    /// connection lives in (None = top level), symmetric with `AddEdge`; it
    /// disambiguates identical local refs that exist in more than one scope.
    RemoveEdge { source: String, source_port: String, target: String, target_port: String, scope_group: Option<String> },
    /// Add an empty group `Label = Group() -> () {}`.
    AddGroup { label: String, parent_group: Option<String> },
    /// Remove a group; its body moves up one scope.
    RemoveGroup { group: String },
    /// Rename a group and rewrite references to its ports.
    RenameGroup { old_label: String, new_label: String },
    /// Move a node into a group (top level when `target_group` is None).
    MoveNodeScope { node: String, target_group: Option<String> },
    /// Move a group into another group (top level when None).
    MoveGroupScope { group: String, target_group: Option<String> },
    /// Rewrite a node's port signature.
    UpdateNodePorts { node: String, inputs: Vec<PortSig>, outputs: Vec<PortSig> },
    /// Rewrite a group's port signature.
    UpdateGroupPorts { group: String, inputs: Vec<PortSig>, outputs: Vec<PortSig> },
    /// Set the `# Project:` / `# Description:` header comments.
    SetProjectMeta { name: Option<String>, description: Option<String> },
}

/// A port in a signature rewrite. `required: false` renders `name: Type?`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortSig {
    pub name: String,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub port_type: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum EditError {
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("group not found: {0}")]
    GroupNotFound(String),
    #[error("id is ambiguous (matches multiple): {0}")]
    AmbiguousId(String),
    #[error("id already exists in scope: {0}")]
    DuplicateId(String),
    #[error("connection not found: {0}.{1} = {2}.{3}")]
    ConnectionNotFound(String, String, String, String),
    #[error("invalid edit argument: {0}")]
    InvalidArgument(String),
    #[error("source does not parse: {0}")]
    Unparseable(String),
}

/// A minimal text edit: replace the byte range `[start, end)` of the source
/// with `text`. This is the editor's reversible-action unit (Monaco's model):
/// applying an edit yields its INVERSE edit (the one that restores the prior
/// source), so undo/redo replay inverse/forward edits without snapshotting the
/// whole file or re-deriving semantic ops. A text-edit inverse restores the
/// exact original bytes, so `@file(...)` markers and formatting survive
/// faithfully (a semantic re-derivation from the resolved project could not).
///
/// Byte offsets (not line/col) so the empty-replacement and trailing-newline
/// boundaries are unambiguous. Offsets are on char boundaries (the diff trims
/// on `char_indices`).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextEdit {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// Apply an ordered batch of edits atomically, returning the new source AND the
/// inverse edit (apply it to the new source to get the original back). Re-parses
/// between ops so each sees fresh spans. On any op failure the whole batch fails
/// (the caller keeps the original source). `base_dir` is the source file's
/// directory, used to resolve `@file`/`@include` during the structural parse;
/// pass None for a detached buffer.
pub fn apply_edits(
    source: &str,
    base_dir: Option<&std::path::Path>,
    ops: &[EditOp],
) -> Result<(String, TextEdit), EditError> {
    let mut current = source.to_string();
    for op in ops {
        let project = structure(&current, base_dir)?;
        current = apply_one(&current, &project, base_dir, op)?;
    }
    let inverse = invert_text_edit(source, &current);
    Ok((current, inverse))
}

/// The inverse text edit for an `old -> new` whole-source change: the edit that,
/// applied to `new`, restores `old`. Trims the common leading + trailing bytes
/// (on char boundaries) so the edit covers only the changed region (minimal
/// hunk, not the whole file). Its `[start, end)` range is in `new`'s byte
/// offsets; its `text` is the original bytes from `old`.
pub fn invert_text_edit(old: &str, new: &str) -> TextEdit {
    let ob = old.as_bytes();
    let nb = new.as_bytes();
    // Common leading bytes, backed off to a char boundary.
    let max_pre = ob.len().min(nb.len());
    let mut prefix = 0;
    while prefix < max_pre && ob[prefix] == nb[prefix] {
        prefix += 1;
    }
    while prefix > 0 && (!old.is_char_boundary(prefix) || !new.is_char_boundary(prefix)) {
        prefix -= 1;
    }
    // Common trailing bytes, not crossing the prefix, backed off to a boundary.
    let mut suffix = 0;
    while suffix < (ob.len() - prefix).min(nb.len() - prefix)
        && ob[ob.len() - 1 - suffix] == nb[nb.len() - 1 - suffix]
    {
        suffix += 1;
    }
    while suffix > 0
        && (!old.is_char_boundary(old.len() - suffix) || !new.is_char_boundary(new.len() - suffix))
    {
        suffix -= 1;
    }
    TextEdit {
        start: prefix,
        end: new.len() - suffix,
        text: old[prefix..old.len() - suffix].to_string(),
    }
}

/// Apply a `TextEdit` to a source string (undo/redo replay on the host),
/// replacing the byte range `[start, end)` with `text`. Total: the offsets are
/// untrusted (host-supplied, possibly stale against a buffer that drifted), so
/// it validates them and fails LOUDLY instead of slicing blind. A non-char-
/// boundary or out-of-range offset would panic `&str` indexing and (with no
/// catch_unwind around the server loop) take the whole parse-server down; this
/// turns that into an `EditError` the request surfaces as a normal envelope.
pub fn apply_text_edit(source: &str, edit: &TextEdit) -> Result<String, EditError> {
    let bad = |why: &str| EditError::InvalidArgument(format!("text edit {}..{} {}", edit.start, edit.end, why));
    if edit.start > edit.end {
        return Err(bad("has start > end"));
    }
    if edit.end > source.len() {
        return Err(bad("is out of range"));
    }
    if !source.is_char_boundary(edit.start) || !source.is_char_boundary(edit.end) {
        return Err(bad("does not land on a char boundary"));
    }
    Ok(format!("{}{}{}", &source[..edit.start], edit.text, &source[edit.end..]))
}

/// Parse to the flattened structure used for span lookups. Editing is
/// structural, so we use `Interface` include-mode (don't inline included files)
/// and a nil id (ids don't affect spans). `base_dir` resolves `@file`/`@include`
/// the same way the live parse does. Parse errors here mean the source is
/// mid-edit-broken; surface loudly rather than splicing blind.
fn structure(source: &str, base_dir: Option<&std::path::Path>) -> Result<ProjectDefinition, EditError> {
    compile_with_mode(source, Uuid::nil(), base_dir, IncludeMode::Interface)
        .map_err(|errs| EditError::Unparseable(errs.iter().map(|e| format!("{}:{}", e.line, e.message)).collect::<Vec<_>>().join("; ")))
}

fn apply_one(source: &str, project: &ProjectDefinition, base_dir: Option<&std::path::Path>, op: &EditOp) -> Result<String, EditError> {
    match op {
        EditOp::SetConfig { node, key, value } => set_config(source, project, node, key, Some(value)),
        EditOp::RemoveConfig { node, key } => set_config(source, project, node, key, None),
        EditOp::SetLabel { node, label } => match label.as_deref().filter(|l| !l.is_empty()) {
            Some(l) => set_config(source, project, node, "_label", Some(&format_string(l))),
            None => set_config(source, project, node, "_label", None),
        },
        EditOp::AddNode { id, node_type, parent_group } => add_node(source, project, id, node_type, parent_group.as_deref()),
        EditOp::RemoveNode { node } => remove_node(source, project, node),
        EditOp::AddEdge { source: s, source_port, target, target_port, scope_group } => {
            add_edge(source, project, base_dir, s, source_port, target, target_port, scope_group.as_deref())
        }
        EditOp::RemoveEdge { source: s, source_port, target, target_port, scope_group } => {
            remove_edge(source, project, s, source_port, target, target_port, scope_group.as_deref())
        }
        EditOp::AddGroup { label, parent_group } => add_group(source, project, label, parent_group.as_deref()),
        EditOp::RemoveGroup { group } => remove_group(source, project, group),
        EditOp::RenameGroup { old_label, new_label } => rename_group(source, project, old_label, new_label),
        EditOp::MoveNodeScope { node, target_group } => move_scope(source, project, base_dir, node, target_group.as_deref(), false),
        EditOp::MoveGroupScope { group, target_group } => move_scope(source, project, base_dir, group, target_group.as_deref(), true),
        EditOp::UpdateNodePorts { node, inputs, outputs } => update_ports(source, project, node, inputs, outputs, false),
        EditOp::UpdateGroupPorts { group, inputs, outputs } => update_ports(source, project, group, inputs, outputs, true),
        EditOp::SetProjectMeta { name, description } => Ok(set_project_meta(source, name.as_deref(), description.as_deref())),
    }
}

// ── lookups against the flattened project ──────────────────────────────────

/// Resolve `wanted` against `candidates` keyed by `id_of`: an exact id match
/// wins; otherwise a UNIQUE local-id (last `.`-segment) match wins; zero
/// matches is `not_found(wanted)`, 2+ local matches is `AmbiguousId`. Editing
/// must never guess which node a bare ambiguous id means (guessing then splices
/// the wrong line) and must never silently no-op, fail loud.
fn resolve_unique<'a, T>(
    candidates: impl Iterator<Item = &'a T> + Clone,
    wanted: &str,
    id_of: impl Fn(&T) -> &str,
    not_found: impl Fn(String) -> EditError,
) -> Result<&'a T, EditError> {
    if let Some(exact) = candidates.clone().find(|c| id_of(c) == wanted) {
        return Ok(exact);
    }
    let mut locals = candidates.filter(|c| local(id_of(c)) == local(wanted));
    let first = locals.next().ok_or_else(|| not_found(wanted.to_string()))?;
    if locals.next().is_some() {
        return Err(EditError::AmbiguousId(wanted.to_string()));
    }
    Ok(first)
}

/// Find a node by the id the frontend sent (scoped `grp.child` or bare local).
/// Boundary passthroughs aren't editable nodes, so they're excluded.
fn find_node<'a>(project: &'a ProjectDefinition, id: &str) -> Result<&'a NodeDefinition, EditError> {
    resolve_unique(
        project.nodes.iter().filter(|n| n.group_boundary.is_none()),
        id,
        |n| n.id.as_str(),
        EditError::NodeNotFound,
    )
}

/// Find a group by the id the frontend sent (scoped `outer.grp` or local).
fn find_group<'a>(project: &'a ProjectDefinition, group: &str) -> Result<&'a GroupDefinition, EditError> {
    resolve_unique(project.groups.iter(), group, |g| g.id.as_str(), EditError::GroupNotFound)
}

fn local(id: &str) -> &str {
    id.rsplit('.').next().unwrap_or(id)
}

// ── line-buffer helpers ────────────────────────────────────────────────────

fn lines_of(source: &str) -> Vec<String> {
    source.split('\n').map(|s| s.to_string()).collect()
}

fn join(lines: Vec<String>) -> String {
    collapse_blank_runs(lines).join("\n")
}

/// Collapse runs of 3+ blank lines to 2 (tidy pass, matches old editor).
fn collapse_blank_runs(lines: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(lines.len());
    let mut blanks = 0;
    for l in lines {
        if l.trim().is_empty() {
            blanks += 1;
            if blanks <= 2 {
                out.push(l);
            }
        } else {
            blanks = 0;
            out.push(l);
        }
    }
    out
}

fn indent_of(line: &str) -> String {
    line.chars().take_while(|c| c.is_whitespace()).collect()
}

/// Render a string to a `.weft` value token (quoted, or a heredoc if it has
/// newlines). Used for labels; config values arrive pre-formatted.
fn format_string(s: &str) -> String {
    if s.contains('\n') {
        let escaped = s.replace("```", "\\```");
        format!("```\n{escaped}\n```")
    } else {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

// ── ops ────────────────────────────────────────────────────────────────────
// One function per EditOp. Each takes the source lines + the flattened project
// (for spans), splices, and returns the new source. The flattened project's
// scoped ids + spans replace the old TS editor's regex location entirely.

/// Set or remove a config field. `value = Some(token)` writes/replaces the
/// field; `None` removes it. Uses the field's `configSpans` entry: replace the
/// spanned lines in place, preserving the connection-line prefix when the field
/// was written as `node.key = ...` (origin Connection). Inserts before the
/// node's closing brace when the field has no existing span.
fn set_config(
    source: &str,
    project: &ProjectDefinition,
    node_id: &str,
    key: &str,
    value: Option<&str>,
) -> Result<String, EditError> {
    let node = find_node(project, node_id)?;
    let mut lines = lines_of(source);

    // The field's span (if any) comes from this source, so its lines are in
    // bounds; index directly.
    if let Some(cs) = node.config_spans.get(key) {
        let start = cs.span.start_line.saturating_sub(1);
        let end = cs.span.end_line; // 1-based inclusive end == exclusive 0-based end
        match value {
            None => {
                lines.drain(start..end);
            }
            Some(val) => {
                let indent = indent_of(&lines[start]);
                let prefix = if cs.origin == ConfigOrigin::Connection {
                    // A connection-origin field is a `target.port = value` line;
                    // keep its exact `target.port = ` prefix (the source may use
                    // a local id while node.id is scoped). The line is a
                    // connection by origin, so the prefix must be present.
                    connection_prefix(&lines[start])
                        .ok_or_else(|| EditError::InvalidArgument(format!("config '{key}' marked connection-origin but line has no `=`")))?
                } else {
                    format!("{key}: ")
                };
                lines.splice(start..end, field_lines(&prefix, val, &indent));
            }
        }
        return Ok(join(lines));
    }

    // No existing span: removing a field that isn't in source is an idempotent
    // no-op; setting one inserts it before the node's closing brace.
    let Some(val) = value else { return Ok(source.to_string()) };
    insert_config_field(source, node, key, val)
}

/// Build the source lines for `<indent><prefix><value>`, indenting JSON/object
/// continuation lines but NOT heredoc lines (the parser dedents heredocs).
fn field_lines(prefix: &str, value: &str, indent: &str) -> Vec<String> {
    let value_lines: Vec<&str> = value.split('\n').collect();
    let mut out = vec![format!("{indent}{prefix}{}", value_lines[0])];
    let heredoc = value_lines[0].starts_with("```");
    for l in &value_lines[1..] {
        out.push(if heredoc { l.to_string() } else { format!("{indent}{l}") });
    }
    out
}

/// The `target.port = ` prefix of a connection-line field, if present.
fn connection_prefix(line: &str) -> Option<String> {
    let eq = line.find('=')?;
    Some(format!("{} = ", line[..eq].trim()))
}

/// Insert `key: value` before the node's closing brace. Expands a bare or
/// one-liner node to multi-line first so the field gets its own line.
fn insert_config_field(source: &str, node: &NodeDefinition, key: &str, value: &str) -> Result<String, EditError> {
    let span = node.span.ok_or_else(|| EditError::NodeNotFound(node.id.clone()))?;
    let mut lines = lines_of(source);
    let start = span.start_line.saturating_sub(1);
    let end = span.end_line.saturating_sub(1);
    let indent = indent_of(&lines[start]);
    let body_indent = format!("{indent}  ");

    if start == end {
        // One-liner or bare node: expand to multi-line, then insert.
        let line = lines[start].clone();
        let expanded = expand_oneliner(&line, &indent, &body_indent, key, value);
        lines.splice(start..=start, expanded);
    } else {
        let field = field_lines(&format!("{key}: "), value, &body_indent);
        lines.splice(end..end, field);
    }
    Ok(join(lines))
}

/// Expand a one-liner/bare node line into multi-line form with the new field.
/// Handles `id = Type { a: 1 }`, `id = Type {}`, and bare `id = Type`.
fn expand_oneliner(line: &str, indent: &str, body_indent: &str, key: &str, value: &str) -> Vec<String> {
    if let (Some(open), Some(close)) = (line.find('{'), line.rfind('}')) {
        let head = line[..open].trim_end();
        let body = line[open + 1..close].trim();
        let mut out = vec![format!("{head} {{")];
        if !body.is_empty() {
            for pair in body.split(',') {
                let p = pair.trim();
                if !p.is_empty() {
                    out.push(format!("{body_indent}{p}"));
                }
            }
        }
        out.extend(field_lines(&format!("{key}: "), value, body_indent));
        out.push(format!("{indent}}}"));
        out
    } else {
        // Bare `id = Type [(...)...]`: append a body.
        let mut out = vec![format!("{} {{", line.trim_end())];
        out.extend(field_lines(&format!("{key}: "), value, body_indent));
        out.push(format!("{indent}}}"));
        out
    }
}

fn add_node(
    source: &str,
    project: &ProjectDefinition,
    id: &str,
    node_type: &str,
    parent_group: Option<&str>,
) -> Result<String, EditError> {
    reject_if_id_taken(project, parent_group, id)?;
    let snippet = format!("{id} = {node_type} {{}}");
    insert_in_scope(source, project, parent_group, &snippet)
}

fn add_group(
    source: &str,
    project: &ProjectDefinition,
    label: &str,
    parent_group: Option<&str>,
) -> Result<String, EditError> {
    reject_if_id_taken(project, parent_group, label)?;
    let snippet = format!("{label} = Group() -> () {{}}");
    insert_in_scope(source, project, parent_group, &snippet)
}

/// The id a new declaration would have once parsed: bare at top level, scoped
/// under the parent group otherwise (matching how the flattened project keys
/// ids). Nodes, groups, and include aliases share one id namespace.
fn scoped_id(parent_group: Option<&str>, local_id: &str) -> String {
    match parent_group {
        Some(g) => format!("{}.{}", g, local_id),
        None => local_id.to_string(),
    }
}

/// Reject (DuplicateId) if `local_id` would collide with an existing node or
/// group in the target scope. Adding a duplicate would write an invalid project
/// that the next strict parse rejects far from the cause; fail loud here.
fn reject_if_id_taken(project: &ProjectDefinition, parent_group: Option<&str>, local_id: &str) -> Result<(), EditError> {
    let want = scoped_id(parent_group, local_id);
    let taken = project.nodes.iter().any(|n| n.id == want && n.group_boundary.is_none())
        || project.groups.iter().any(|g| g.id == want);
    if taken {
        return Err(EditError::DuplicateId(want));
    }
    Ok(())
}

/// The 0-based index of a group's CLOSING `}` line. The parser's `span.end_line`
/// is NOT reliably that line: when a group declares post-body output ports on
/// their own line, the span extends through them:
///   ```text
///   grp = Group() {
///     ...
///   }                 <- the real close
///   -> (out: String)  <- span.end_line points HERE
///   ```
/// Scan back from `end` for the group's closing-brace line. Two shapes:
///   - multi-line body: the `}` is line-leading (`}`, `} -> (...)`, `}->`), on
///     its own line; that's the close.
///   - inline body (`grp = Group() ... {}` on one line, possibly after a
///     multi-line signature): no line in the span is brace-leading, so the
///     close is the last line that CONTAINS a `{` (the body opened and closed
///     on it). open_group_body then splits that line open.
/// A parsed group always has one of these; if neither is found the source and
/// parse disagree, so fail LOUD rather than return a wrong line and corrupt the
/// file. `header..=end` is in bounds (it's the group's own span).
fn close_brace_line(lines: &[String], header: usize, end: usize) -> Result<usize, EditError> {
    let end = end.min(lines.len().saturating_sub(1));
    let leading = |t: &str| t == "}" || t.starts_with("} ") || t.starts_with("}->");
    (header..=end)
        .rev()
        .find(|&i| leading(lines[i].trim_start()) || lines[i].contains('{'))
        .ok_or_else(|| EditError::Unparseable(format!("group spanning lines {}..={} has no closing brace", header + 1, end + 1)))
}

/// Locate the line index at which to insert the first/next child of a group,
/// just before its closing `}`. Resolves the real closing-brace line first
/// (`close_brace_line`, robust to post-body output ports), then:
///   - if that line still holds the body's opening `{` (a fully inline `... {}`),
///     split it open (`{` stays, `}` moves to its own line) and point at the gap;
///   - otherwise insert before the brace line.
/// The split branch only runs when a `{` is present, so it never fails. `lines`
/// is mutated in place on a split. `header` is the group's 0-based start line,
/// `end` its 0-based last line (may sit past the brace; we resolve it).
fn open_group_body(lines: &mut Vec<String>, header: usize, end: usize) -> Result<usize, EditError> {
    let close = close_brace_line(lines, header, end)?;
    let Some(brace) = lines[close].rfind('{') else {
        return Ok(close); // brace on its own line (`}` or `} -> (...)`): insert before it.
    };
    // Fully inline body: split `... {}` into a `{` line and a `}` line.
    let line = lines[close].clone();
    let base_indent = indent_of(&lines[header]);
    lines[close] = line[..=brace].trim_end().to_string();
    lines.insert(close + 1, format!("{base_indent}}}"));
    Ok(close + 1)
}

/// Insert a declaration line at the end of a scope. Top level: append at EOF.
/// Inside a group: just before the group's closing brace, at the body indent.
fn insert_in_scope(
    source: &str,
    project: &ProjectDefinition,
    parent_group: Option<&str>,
    snippet: &str,
) -> Result<String, EditError> {
    let mut lines = lines_of(source);
    match parent_group {
        None => {
            let mut at = lines.len();
            while at > 0 && lines[at - 1].trim().is_empty() {
                at -= 1;
            }
            lines.splice(at..at, vec![String::new(), snippet.to_string()]);
        }
        Some(group) => {
            let g = find_group(project, group)?;
            let span = g.span.ok_or_else(|| EditError::GroupNotFound(group.to_string()))?;
            let header = span.start_line.saturating_sub(1);
            let end = span.end_line.saturating_sub(1);
            let body_indent = format!("{}  ", indent_of(&lines[header]));
            let at = open_group_body(&mut lines, header, end)?;
            lines.splice(at..at, vec![format!("{body_indent}{snippet}")]);
        }
    }
    Ok(join(lines))
}

fn remove_node(source: &str, project: &ProjectDefinition, node_id: &str) -> Result<String, EditError> {
    let node = find_node(project, node_id)?;
    let span = node.span.ok_or_else(|| EditError::NodeNotFound(node_id.to_string()))?;
    // Drop the node's own declaration lines + every connecting edge line. Using
    // the parser's edge spans (not a text heuristic) means we only ever drop
    // lines that ARE edges touching this node.
    let mut drop: std::collections::HashSet<usize> = span_lines(&span).collect();
    for s in edge_spans_touching(project, &node.id) {
        drop.extend(span_lines(&s));
    }
    Ok(drop_lines(source, &drop))
}

/// Spans of every edge whose source or target is `canonical_id`. Edges carry
/// canonical scoped endpoint ids (`grp.a`, `grp__in`), so this matches EXACTLY,
/// the caller must pass an already-resolved canonical id (via `find_node`). A
/// local-id match here would wrongly catch a same-local-name node in another
/// scope (e.g. removing `grp.a` deleting a top-level `a`'s edge).
fn edge_spans_touching(project: &ProjectDefinition, canonical_id: &str) -> Vec<Span> {
    project
        .edges
        .iter()
        .filter(|e| e.source == canonical_id || e.target == canonical_id)
        .filter_map(|e| e.span)
        .collect()
}

/// The 1-based line numbers a span covers (inclusive).
fn span_lines(span: &Span) -> std::ops::RangeInclusive<usize> {
    span.start_line..=span.end_line
}

/// Remove the given 1-based line numbers from the source. The single
/// line-removal primitive (node removal, group ungroup) so overlapping or
/// unordered ranges can't desync indices: it's a set membership filter.
fn drop_lines(source: &str, drop: &std::collections::HashSet<usize>) -> String {
    let out: Vec<String> = lines_of(source)
        .into_iter()
        .enumerate()
        .filter_map(|(i, line)| if drop.contains(&(i + 1)) { None } else { Some(line) })
        .collect();
    join(out)
}

/// Remove a group by UNGROUPING it: its children + pure-internal wiring move up
/// one scope (de-indented), while the group header line, the closing `}`, and
/// every BOUNDARY connection (anything wired to the group's ports, both the
/// internal `self.*` legs and external `group.port` refs) are dropped. Matches
/// the prior editor: deleting a group keeps its nodes. (To delete the contents
/// too, the caller removes the children first.)
///
/// One pass against the original parse: a boundary connection is exactly an
/// edge touching the group's `{id}__in`/`{id}__out` passthrough (the parser
/// already gave each such edge a span), so no text heuristic and no re-parse.
fn remove_group(source: &str, project: &ProjectDefinition, group: &str) -> Result<String, EditError> {
    let g = find_group(project, group)?;
    let span = g.span.ok_or_else(|| EditError::GroupNotFound(group.to_string()))?;
    let header = g.header_span.or(g.span).ok_or_else(|| EditError::GroupNotFound(group.to_string()))?;

    // The real closing-brace line, which `span.end_line` overshoots when the
    // group has post-body output ports on their own line(s). 1-based.
    let lines_now = lines_of(source);
    let close_1based = close_brace_line(&lines_now, header.start_line.saturating_sub(1), span.end_line.saturating_sub(1))? + 1;

    // 1-based line numbers to drop entirely: the header, everything from the
    // closing brace through span.end_line (the `}` plus any post-body `-> (...)`
    // output-port lines, which belong to the group declaration), and every
    // boundary-connection line.
    let mut drop: std::collections::HashSet<usize> = boundary_edge_spans(project, &g.id)
        .iter()
        .flat_map(span_lines)
        .collect();
    drop.insert(header.start_line);
    drop.extend(close_1based..=span.end_line);

    // Lines strictly inside the body (between header line and the close brace)
    // that aren't dropped are the surviving children/wiring: de-indent one level.
    let body = (header.start_line + 1)..close_1based;
    let group_indent = indent_of(&lines_now[header.start_line.saturating_sub(1)]);

    let out: Vec<String> = lines_of(source)
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !drop.contains(&(i + 1)))
        .map(|(i, line)| if body.contains(&(i + 1)) { dedent_one_level(&line, &group_indent) } else { line })
        .collect();
    Ok(join(out))
}

/// Spans of every edge wired to a group's ports: an edge whose source or target
/// is the group's `{id}__in` / `{id}__out` boundary passthrough. Covers both the
/// internal `self.*` legs and external `group.port` references.
fn boundary_edge_spans(project: &ProjectDefinition, group_id: &str) -> Vec<Span> {
    let in_id = format!("{group_id}__in");
    let out_id = format!("{group_id}__out");
    project
        .edges
        .iter()
        .filter(|e| {
            let touches = |ep: &str| ep == in_id || ep == out_id;
            touches(&e.source) || touches(&e.target)
        })
        .filter_map(|e| e.span)
        .collect()
}

/// Strip exactly one indent level (`group_indent` + 2 spaces) from a body line.
fn dedent_one_level(line: &str, group_indent: &str) -> String {
    let inner = format!("{group_indent}  ");
    line.strip_prefix(&inner).map(|r| format!("{group_indent}{r}")).unwrap_or_else(|| line.to_string())
}

fn add_edge(
    source: &str,
    project: &ProjectDefinition,
    base_dir: Option<&std::path::Path>,
    src: &str,
    src_port: &str,
    tgt: &str,
    tgt_port: &str,
    scope_group: Option<&str>,
) -> Result<String, EditError> {
    // Both endpoints must resolve to real nodes (or `self`, the enclosing
    // group's boundary): writing an edge to a non-existent node would produce a
    // dangling connection. Fail loud instead.
    require_endpoint(project, src)?;
    require_endpoint(project, tgt)?;
    // Input ports are single-driver: remove any existing edge into the same
    // target port before adding, then re-parse so the insertion scope's span is
    // correct after the removal shifted lines.
    let without = remove_existing_driver(source, project, tgt, tgt_port, scope_group);
    let conn = format!("{tgt}.{tgt_port} = {src}.{src_port}");
    let proj2 = structure(&without, base_dir)?;
    insert_in_scope(&without, &proj2, scope_group, &conn)
}

/// An edge endpoint must be `self` (the enclosing group's boundary) or resolve
/// to a real node/group. A bare `self` is accepted (resolved at parse time
/// against the scope the connection lands in).
fn require_endpoint(project: &ProjectDefinition, id: &str) -> Result<(), EditError> {
    if id == "self" {
        return Ok(());
    }
    // Accept a node or a group (an edge can target a group's port).
    if find_node(project, id).is_ok() || find_group(project, id).is_ok() {
        return Ok(());
    }
    Err(EditError::NodeNotFound(id.to_string()))
}

/// The canonical edge-endpoint id for a frontend-supplied ref + its scope. The
/// webview sends scope-LOCAL refs (`self`, a local node id, a group id, or a
/// top-level id) plus the enclosing `scope_group`; the flattened project stores
/// CANONICAL scoped endpoints. This reconstructs the canonical id so edges
/// match exactly (no lossy local-id guessing). Crucially, an edge wired to a
/// GROUP's port is stored against the group's boundary passthrough (flatten
/// rewrites `== group.id` to `{id}__in`/`{id}__out`), so a ref that resolves to
/// a group gets the same `__in`/`__out` suffix as `self`:
///   - `self` (always boundary)        => `{scope}__in` (source) / `__out` (target)
///   - a ref resolving to a group `G`  => `G__in` (source) / `G__out` (target)
///   - local node `x` in scope `S`     => `S.x`
///   - node `x` at top level           => `x`
///
/// `self` and a group-ref take OPPOSITE boundary sides, because `self` views
/// the group from INSIDE and a group-ref from OUTSIDE:
///   - `self` as source reads the group's own input  => `{scope}__in`
///   - `self` as target writes the group's own output => `{scope}__out`
///   - a group `G` as source reads G's output         => `G__out`
///   - a group `G` as target writes into G's input     => `G__in`
fn canonical_endpoint(project: &ProjectDefinition, reference: &str, is_source: bool, scope_group: Option<&str>) -> String {
    if reference == "self" {
        // `self` only appears inside a group body, so scope is the boundary id.
        return match scope_group {
            Some(scope) => format!("{scope}__{}", if is_source { "in" } else { "out" }),
            None => reference.to_string(),
        };
    }
    let canon = match scope_group {
        Some(scope) => format!("{scope}.{reference}"),
        None => reference.to_string(),
    };
    if project.groups.iter().any(|g| g.id == canon) {
        format!("{canon}__{}", if is_source { "out" } else { "in" })
    } else {
        canon
    }
}

/// Drop the line of whatever currently drives `tgt.tgt_port` in `scope_group`.
/// No driver is a legitimate no-op (an input port may be unconnected); returns
/// the source unchanged then. Used before adding an edge (input ports are
/// single-driver, so the old driver is replaced).
fn remove_existing_driver(source: &str, project: &ProjectDefinition, tgt: &str, tgt_port: &str, scope_group: Option<&str>) -> String {
    let canon_tgt = canonical_endpoint(project, tgt, false, scope_group);
    let driver = project.edges.iter().find(|e| {
        handle(&e.target_handle) == tgt_port && e.target == canon_tgt
    });
    match driver.and_then(|e| e.span) {
        Some(span) => drop_lines(source, &span_lines(&span).collect()),
        None => source.to_string(),
    }
}

fn remove_edge(
    source: &str,
    project: &ProjectDefinition,
    src: &str,
    src_port: &str,
    tgt: &str,
    tgt_port: &str,
    scope_group: Option<&str>,
) -> Result<String, EditError> {
    let not_found = || EditError::ConnectionNotFound(src.into(), src_port.into(), tgt.into(), tgt_port.into());
    // Resolve the frontend refs to canonical scoped endpoints, then match the
    // edge EXACTLY (scope-aware): two identical local quads in different scopes
    // are disambiguated by `scope_group`.
    let canon_src = canonical_endpoint(project, src, true, scope_group);
    let canon_tgt = canonical_endpoint(project, tgt, false, scope_group);
    let edge = project
        .edges
        .iter()
        .find(|e| {
            handle(&e.source_handle) == src_port
                && handle(&e.target_handle) == tgt_port
                && e.source == canon_src
                && e.target == canon_tgt
        })
        .ok_or_else(not_found)?;
    // A user-written connection always has a span (only synthetic
    // inline-expression edges don't, and those aren't addressable here).
    let span = edge.span.ok_or_else(not_found)?;
    Ok(drop_lines(source, &span_lines(&span).collect()))
}

fn handle(h: &Option<String>) -> &str {
    h.as_deref().unwrap_or("")
}

fn rename_group(source: &str, project: &ProjectDefinition, old_label: &str, new_label: &str) -> Result<String, EditError> {
    if new_label.is_empty() {
        return Err(EditError::InvalidArgument("rename to empty label".into()));
    }
    if old_label == new_label {
        return Ok(source.to_string()); // genuine no-op
    }
    let g = find_group(project, old_label)?;
    let header = g.header_span.or(g.span).ok_or_else(|| EditError::GroupNotFound(old_label.to_string()))?;
    let mut lines = lines_of(source);
    let hi = header.start_line.saturating_sub(1); // header span is from this source: in bounds
    // Rename the header `oldLabel = Group...`.
    lines[hi] = rename_header_decl(&lines[hi], old_label, new_label)
        .ok_or_else(|| EditError::GroupNotFound(old_label.to_string()))?;

    // Rewrite `oldLabel.port` references ONLY on the connection lines that
    // actually reference the group: external `grp.port` refs land on edges
    // whose endpoint is the group's `{id}__in`/`{id}__out` boundary
    // passthrough. Span-driven, not a blind text scan: a `grp.foo` inside a
    // string/heredoc value is never an edge line, so it's left untouched.
    let needle = format!("{old_label}.");
    let repl = format!("{new_label}.");
    let ref_lines: std::collections::HashSet<usize> = boundary_edge_spans(project, &g.id)
        .iter()
        .flat_map(span_lines)
        .collect();
    for li in ref_lines {
        let idx = li.saturating_sub(1);
        if idx == hi || idx >= lines.len() {
            continue;
        }
        lines[idx] = replace_ident_dot(&lines[idx], &needle, &repl);
    }
    Ok(join(lines))
}

/// Replace the leading `oldLabel` in a `oldLabel = Group...` header line. None
/// if the line isn't that declaration (a parser/span inconsistency: the caller
/// turns it into a loud error rather than silently leaving the header).
fn rename_header_decl(line: &str, old_label: &str, new_label: &str) -> Option<String> {
    let indent = indent_of(line);
    let rest = line.trim_start();
    let after = rest.strip_prefix(old_label)?;
    if after.trim_start().starts_with('=') {
        return Some(format!("{indent}{new_label}{after}"));
    }
    None
}

/// Replace `needle` (an `ident.`) with `repl` at token boundaries only.
fn replace_ident_dot(line: &str, needle: &str, repl: &str) -> String {
    // Only replace when preceded by a non-identifier char (or start), so
    // `foo.` inside `barfoo.` isn't touched. `i` always lands on a char
    // boundary (we advance by `len_utf8`), so `line[..i].chars().next_back()`
    // is the real previous char (not a raw byte cast, which mis-reads
    // multi-byte UTF-8).
    let mut out = String::with_capacity(line.len());
    let mut i = 0;
    while i < line.len() {
        if line[i..].starts_with(needle) {
            let prev_ok = match line[..i].chars().next_back() {
                None => true,
                Some(c) => !(c.is_alphanumeric() || c == '_' || c == '.'),
            };
            if prev_ok {
                out.push_str(repl);
                i += needle.len();
                continue;
            }
        }
        let ch = line[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn move_scope(
    source: &str,
    project: &ProjectDefinition,
    base_dir: Option<&std::path::Path>,
    id: &str,
    target_group: Option<&str>,
    is_group: bool,
) -> Result<String, EditError> {
    // Extract the node/group block, re-indent, insert into the target scope.
    let span = if is_group {
        find_group(project, id)?.span.ok_or_else(|| EditError::GroupNotFound(id.to_string()))?
    } else {
        find_node(project, id)?.span.ok_or_else(|| EditError::NodeNotFound(id.to_string()))?
    };
    // The span is from this source: [start_line, end_line] is in bounds and has
    // at least the declaration's first line.
    let mut lines = lines_of(source);
    let start = span.start_line.saturating_sub(1);
    let block: Vec<String> = lines.drain(start..span.end_line).collect();
    let old_indent = indent_of(&block[0]);
    // De-indent the block to column 0; lines without the block's own indent
    // (blank lines, heredoc content) are left as-is.
    let stripped: Vec<String> = block
        .iter()
        .map(|l| l.strip_prefix(&old_indent).map(str::to_string).unwrap_or_else(|| l.clone()))
        .collect();

    // Re-parse after removal so the target group's span is correct.
    let after_removal = join(lines);
    let proj2 = structure(&after_removal, base_dir)?;
    let mut lines2 = lines_of(&after_removal);
    match target_group {
        None => {
            let mut at = lines2.len();
            while at > 0 && lines2[at - 1].trim().is_empty() {
                at -= 1;
            }
            let mut block2 = vec![String::new()];
            block2.extend(stripped);
            lines2.splice(at..at, block2);
        }
        Some(group) => {
            let g = find_group(&proj2, group)?;
            let gspan = g.span.ok_or_else(|| EditError::GroupNotFound(group.to_string()))?;
            let header = gspan.start_line.saturating_sub(1);
            let end = gspan.end_line.saturating_sub(1);
            let body_indent = format!("{}  ", indent_of(&lines2[header]));
            let reindented: Vec<String> = stripped
                .iter()
                .map(|l| if l.trim().is_empty() { l.clone() } else { format!("{body_indent}{l}") })
                .collect();
            let insert_at = open_group_body(&mut lines2, header, end)?;
            lines2.splice(insert_at..insert_at, reindented);
        }
    }
    Ok(join(lines2))
}

fn update_ports(
    source: &str,
    project: &ProjectDefinition,
    id: &str,
    inputs: &[PortSig],
    outputs: &[PortSig],
    is_group: bool,
) -> Result<String, EditError> {
    let header = if is_group {
        let g = find_group(project, id)?;
        g.header_span.or(g.span).ok_or_else(|| EditError::GroupNotFound(id.to_string()))?
    } else {
        let n = find_node(project, id)?;
        n.header_span.or(n.span).ok_or_else(|| EditError::NodeNotFound(id.to_string()))?
    };
    // The header span came from parsing THIS source, so its line is in bounds.
    let mut lines = lines_of(source);
    let hi = header.start_line.saturating_sub(1);
    lines[hi] = rewrite_header_ports(&lines[hi], inputs, outputs)
        .ok_or_else(|| EditError::InvalidArgument(format!("header line for '{id}' is not a declaration")))?;
    Ok(join(lines))
}

/// Rewrite the `id = Type(sig) -> (sig)` portion of a header line, preserving
/// the `id = Type` head and any trailing `{`. None if the line isn't a `id =
/// Type` declaration (a span inconsistency the caller surfaces loudly).
fn rewrite_header_ports(line: &str, inputs: &[PortSig], outputs: &[PortSig]) -> Option<String> {
    let indent = indent_of(line);
    let rest = line.trim();
    let eq = rest.find('=')?; // head = `id = Type`
    let lhs = rest[..eq].trim();
    let after_eq = rest[eq + 1..].trim();
    // Type name = leading identifier of after_eq.
    let type_end = after_eq.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(after_eq.len());
    let type_name = &after_eq[..type_end];
    let trailing_brace = if rest.ends_with('{') { " {" } else { "" };
    let sig = build_signature(inputs, outputs);
    Some(format!("{indent}{lhs} = {type_name}{sig}{trailing_brace}"))
}

fn build_signature(inputs: &[PortSig], outputs: &[PortSig]) -> String {
    let fmt = |p: &PortSig| {
        let ty = p.port_type.as_deref().unwrap_or("MustOverride");
        let opt = if p.required { "" } else { "?" };
        format!("{}: {ty}{opt}", p.name)
    };
    let ins: Vec<String> = inputs.iter().map(fmt).collect();
    let outs: Vec<String> = outputs.iter().map(fmt).collect();
    if inputs.is_empty() && outputs.is_empty() {
        String::new()
    } else if outputs.is_empty() {
        format!("({})", ins.join(", "))
    } else {
        format!("({}) -> ({})", ins.join(", "), outs.join(", "))
    }
}

fn set_project_meta(source: &str, name: Option<&str>, description: Option<&str>) -> String {
    let mut lines = lines_of(source);
    let scan = lines.len().min(10);
    let mut name_idx = None;
    let mut desc_idx = None;
    for i in 0..scan {
        let t = lines[i].trim();
        if t.starts_with("# Project:") {
            name_idx = Some(i);
        } else if t.starts_with("# Description:") {
            desc_idx = Some(i);
        }
    }

    // Update or insert the `# Project:` line. Inserting at the top (never a
    // silent no-op) so meta can be set on a file that has no header yet.
    if let Some(n) = name {
        match name_idx {
            Some(i) => lines[i] = format!("# Project: {n}"),
            None => {
                lines.splice(0..0, vec![format!("# Project: {n}")]);
                name_idx = Some(0);
                if let Some(d) = desc_idx.as_mut() {
                    *d += 1; // everything shifted down by the inserted line
                }
            }
        }
    }

    // Update or insert the `# Description:` line, anchored just after the
    // project line (inserting one if neither exists).
    if let Some(d) = description.filter(|d| !d.is_empty()) {
        match desc_idx {
            Some(i) => lines[i] = format!("# Description: {d}"),
            None => {
                let at = name_idx.map(|i| i + 1).unwrap_or(0);
                lines.splice(at..at, vec![format!("# Description: {d}")]);
            }
        }
    }
    join(lines)
}

#[cfg(test)]
mod tests;


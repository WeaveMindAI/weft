//! The 16 edit ops as CST tree mutations.
//!
//! Each op resolves its target through the typed view (`cst::nodes`), then
//! mutates the mutable (`clone_for_update`) tree via `splice_children`/`detach`.
//! Edits never compute text offsets: the closing `}` is a real token and the
//! body is a real node, so "insert a child" is `splice_children` before the
//! body's `R_BRACE`, and "remove a decl" is `detach` on its node.
//!
//! Resolve-then-mutate discipline (one rowan footgun): we resolve a target to a
//! concrete node handle FIRST, then mutate. We never mutate while iterating the
//! tree, so the iterator-invalidation panic cannot fire.
//!
//! Second rowan footgun: `splice_children(to_delete, ...)` with a MULTI-element
//! `to_delete` range deletes only ONE element (its internal detach shifts
//! indices mid-loop). Only `idx..idx` (insert) and `idx..idx+1` (replace one)
//! are reliable. To remove several children, collect their handles and
//! `detach()` each individually (see `replace_value_after`).
//!
//! Subtrees to insert are built by parsing snippet text and lifting its
//! elements (the rust-analyzer `make`-from-text idiom): one tree-construction
//! path (the parser), so a built NODE_DECL is structurally identical to a parsed
//! one. DRY, and impossible to drift from the grammar.

use rowan::NodeOrToken;

use super::{EditError, PortSig};
use crate::cst::kind::{SyntaxElement, SyntaxKind, SyntaxNode, SyntaxToken};
use crate::cst::nodes::{Body, Decl, FileView, Resolution, WeftFile};
use crate::cst::parse;

/// Apply one op to the mutable CST `view` (the file root + its source identity,
/// the anon-group id). Mutates in place; returns the op's error if its target
/// cannot be resolved (the batch then aborts and the original source is kept by
/// the caller). The `view` carries `source_id` so every scoped-id resolution
/// uses the SAME anon-group prefix the lowering writes.
pub(super) fn apply_op(view: &FileView, op: &super::EditOp) -> Result<(), EditError> {
    use super::EditOp::*;
    match op {
        SetConfig { node, key, value } => set_config(view, node, key, Some(value)),
        RemoveConfig { node, key } => set_config(view, node, key, None),
        SetLabel { node, label } => set_label(view, node, label.as_deref()),
        AddNode { id, node_type, parent_group } => {
            add_decl(view, parent_group.as_deref(), id, &format!("{id} = {node_type} {{}}"))
        }
        RemoveNode { node } => remove_node(view, node),
        AddEdge { source, source_port, target, target_port, scope_group } => {
            add_edge(view, scope_group.as_deref(), source, source_port, target, target_port)
        }
        RemoveEdge { source, source_port, target, target_port, scope_group } => {
            remove_edge(view, scope_group.as_deref(), source, source_port, target, target_port)
        }
        AddGroup { label, parent_group } => {
            add_decl(view, parent_group.as_deref(), label, &format!("{label} = Group() -> () {{}}"))
        }
        RemoveGroup { group } => remove_group(view, group),
        RenameGroup { old_label, new_label } => rename_group(view, old_label, new_label),
        MoveNodeScope { node, target_group } => move_scope(view, node, target_group.as_deref()),
        MoveGroupScope { group, target_group } => move_scope(view, group, target_group.as_deref()),
        UpdateNodePorts { node, inputs, outputs } => update_ports(view, node, inputs, outputs),
        UpdateGroupPorts { group, inputs, outputs } => update_ports(view, group, inputs, outputs),
        SetGroupDescription { group, description } => {
            set_group_description(view, group, description.as_deref())
        }
    }
}

// ── resolution helpers ──────────────────────────────────────────────────────

/// Resolve a scoped id to a decl, mapping the resolution outcome to a loud
/// error (never a silent guess).
fn resolve(view: &FileView, id: &str) -> Result<Decl, EditError> {
    match view.resolve_decl(id) {
        Resolution::Found(d) => Ok(d),
        Resolution::NotFound => Err(EditError::NodeNotFound(id.to_string())),
        Resolution::Ambiguous => Err(EditError::AmbiguousId(id.to_string())),
    }
}

/// Resolve specifically to a group decl.
fn resolve_group(view: &FileView, id: &str) -> Result<crate::cst::nodes::GroupDecl, EditError> {
    match resolve(view, id)? {
        Decl::Group(g) => Ok(g),
        _ => Err(EditError::GroupNotFound(id.to_string())),
    }
}

/// The body to insert into for a given parent-group ref: the group's BODY (None
/// = the file root). Splits an inline `{}` body open isn't needed: an empty body
/// already has an `R_BRACE` to splice before.
fn target_body(view: &FileView, parent_group: Option<&str>) -> Result<InsertTarget, EditError> {
    match parent_group {
        None => Ok(InsertTarget::FileRoot(view.file().clone())),
        Some(g) => {
            let group = resolve_group(view, g)?;
            let body = group
                .body()
                .ok_or_else(|| EditError::GroupNotFound(g.to_string()))?;
            Ok(InsertTarget::GroupBody { body, indent: group_body_indent(&group) })
        }
    }
}

enum InsertTarget {
    FileRoot(WeftFile),
    GroupBody { body: Body, indent: String },
}

// ── tree-edit primitives ────────────────────────────────────────────────────

/// Parse `snippet` and return its element run (mutable, ready to splice). The
/// snippet is parsed as a standalone file, so its WEFT_FILE's children ARE the
/// decl/connection plus surrounding whitespace we authored into the string.
fn snippet_elements(snippet: &str) -> Vec<SyntaxElement> {
    parse(snippet).clone_for_update().children_with_tokens().collect()
}

/// True if `body` is a SINGLE-LINE body (`{}` or `{ x }`): no newline token
/// between its braces. A single-line body must be "opened" (a newline added)
/// before inserting a line that owns its own layout, or that line would glue onto
/// the brace line. The ONE definition, so every body-insert agrees on "inline".
fn body_is_single_line(body: &Body) -> bool {
    !body
        .syntax()
        .children_with_tokens()
        .any(|e| e.as_token().map(|t| t.text().contains('\n')).unwrap_or(false))
}

/// Splice `elements` into `body` immediately before its closing `}`.
fn insert_before_close(body: &Body, elements: Vec<SyntaxElement>) -> Result<(), EditError> {
    let brace = body
        .close_brace()
        .ok_or_else(|| EditError::Unparseable("group body has no closing brace".into()))?;
    let at = brace.index();
    // A single-line body (`{}`, `{ x }`) has its content + close brace on the
    // open-brace line, so the inserted content (which carries its own leading
    // indent + trailing newline) would glue onto it. Open the body by prepending
    // a newline: the content sits on its own indented line and its trailing
    // newline drops `}` to the start of the next line.
    let mut elems = if body_is_single_line(body) {
        raw_token_elements(&[(SyntaxKind::WHITESPACE, "\n")])
    } else {
        Vec::new()
    };
    elems.extend(elements);
    body.syntax().splice_children(at..at, elems);
    Ok(())
}

/// Append `elements` at the end of the file root (after the last child).
fn append_to_file(file: &WeftFile, elements: Vec<SyntaxElement>) {
    let count = file.syntax().children_with_tokens().count();
    file.syntax().splice_children(count..count, elements);
}

/// Detach a node and the contiguous whitespace token that immediately precedes
/// it (its leading newline+indent), so removing a decl doesn't leave a blank
/// line behind. Detaches the node first, then its former preceding sibling if it
/// was pure whitespace.
fn detach_with_leading_ws(node: &SyntaxNode) {
    let prev = node.prev_sibling_or_token();
    node.detach();
    if let Some(NodeOrToken::Token(t)) = prev {
        if t.kind() == SyntaxKind::WHITESPACE {
            t.detach();
        }
    }
}

// ── indentation ─────────────────────────────────────────────────────────────

/// The body indent for children of a group: the group header's own indent + 2
/// spaces. Read from the group decl's leading whitespace.
fn group_body_indent(group: &crate::cst::nodes::GroupDecl) -> String {
    format!("{}  ", leading_indent(group.syntax()))
}

/// The whitespace that precedes `node` in source order, wherever the parser
/// attached it. The parser is inconsistent: inside a group body, a decl's
/// leading newline+indent is a WHITESPACE token that is the decl's PREVIOUS
/// SIBLING; at file root, it is attached as the node's OWN FIRST CHILD token. To
/// read or relocate a node's leading layout correctly in BOTH positions, look at
/// the sibling first, then fall back to the first child.
fn leading_ws(node: &SyntaxNode) -> Option<SyntaxToken> {
    if let Some(NodeOrToken::Token(t)) = node.prev_sibling_or_token() {
        if t.kind() == SyntaxKind::WHITESPACE {
            return Some(t);
        }
    }
    if let Some(NodeOrToken::Token(t)) = node.first_child_or_token() {
        if t.kind() == SyntaxKind::WHITESPACE {
            return Some(t);
        }
    }
    None
}

/// The indent (run of spaces/tabs after the last newline) preceding `node`.
fn leading_indent(node: &SyntaxNode) -> String {
    leading_ws(node)
        .map(|t| t.text().rsplit('\n').next().unwrap_or("").to_string())
        .unwrap_or_default()
}

// ── ops: add ────────────────────────────────────────────────────────────────

/// Add a node or group decl into a scope. Rejects a duplicate local id loudly.
fn add_decl(view: &FileView, parent_group: Option<&str>, local_id: &str, decl_src: &str) -> Result<(), EditError> {
    reject_if_taken(view, parent_group, local_id)?;
    match target_body(view, parent_group)? {
        InsertTarget::FileRoot(f) => {
            // A blank line separates the new decl from the preceding content.
            append_to_file(&f, snippet_elements(&format!("\n{decl_src}\n")));
            Ok(())
        }
        InsertTarget::GroupBody { body, indent } => {
            insert_before_close(&body, snippet_elements(&format!("{indent}{decl_src}\n")))
        }
    }
}

/// Reject (DuplicateId) if `local_id` already names a member of the target scope.
fn reject_if_taken(view: &FileView, parent_group: Option<&str>, local_id: &str) -> Result<(), EditError> {
    let scoped = match parent_group {
        Some(g) => format!("{g}.{local_id}"),
        None => local_id.to_string(),
    };
    // EXACT membership: an id is taken only if THIS scoped id already exists, not
    // if a same-local id exists in some other scope (that's a legal add).
    if view.scoped_id_exists(&scoped) {
        return Err(EditError::DuplicateId(scoped));
    }
    Ok(())
}

// ── ops: remove ───────────────────────────────────────────────────────────────

/// Remove a node and every connection (in ANY scope) that references it. Edge
/// matching is SCOPE-AWARE (via `connections_referencing`): an edge inside a
/// child/sibling group that resolves to this node is dropped, while a same-named
/// node in another scope is left alone. Resolve-then-mutate: collect the edge
/// handles first, then detach.
fn remove_node(view: &FileView, node_id: &str) -> Result<(), EditError> {
    let decl = resolve(view, node_id)?;
    for c in view.connections_referencing(&decl) {
        detach_with_leading_ws(&c);
    }
    detach_with_leading_ws(decl.syntax());
    Ok(())
}

/// True if a CONNECTION node has an endpoint whose id equals `local`.
fn connection_touches_local(conn: &SyntaxNode, local: &str) -> bool {
    conn.children()
        .filter(|n| n.kind() == SyntaxKind::ENDPOINT)
        .any(|ep| ep_parts(&ep).0.as_deref() == Some(local))
}

/// (id, port) of an ENDPOINT node, via the typed view's single extractor.
fn ep_parts(ep: &SyntaxNode) -> (Option<String>, Option<String>) {
    crate::cst::nodes::Endpoint::cast(ep.clone()).map(|e| e.parts()).unwrap_or((None, None))
}

/// Remove a group by UNGROUPING it: header + close brace + boundary wiring go,
/// children move up one scope (de-indented).
fn remove_group(view: &FileView, group_id: &str) -> Result<(), EditError> {
    let group = resolve_group(view, group_id)?;
    let local = group.local_id().unwrap_or_default();
    let body = group.body().ok_or_else(|| EditError::GroupNotFound(group_id.to_string()))?;

    // The decls + connections inside the body that are NOT boundary wiring move
    // up. Boundary wiring is a connection with a `self.*` endpoint or an endpoint
    // referencing the group's own local id from outside.
    let group_indent = leading_indent(group.syntax());
    let inner_indent = format!("{group_indent}  ");

    // Collect the surviving inner elements (decls + non-boundary connections),
    // each de-indented from the body's inner indent down to column 0, as source
    // text we re-parse. This is simpler and safer than element surgery for the
    // ungroup case. The block is reassembled into self-contained source below; we
    // dedent to 0 here so the re-indent owns ALL indentation uniformly.
    let mut moved_src = String::new();
    for child in body.syntax().children() {
        match child.kind() {
            SyntaxKind::NODE_DECL | SyntaxKind::GROUP_DECL | SyntaxKind::INCLUDE_DECL => {
                moved_src.push_str(&dedent_block(&child.to_string(), &inner_indent));
                moved_src.push('\n');
            }
            SyntaxKind::CONNECTION => {
                if !connection_is_boundary(&child) {
                    moved_src.push_str(&dedent_block(&child.to_string(), &inner_indent));
                    moved_src.push('\n');
                }
            }
            _ => {}
        }
    }
    // External connections to the group's ports (in the parent scope) are
    // boundary wiring too: drop them.
    let parent = group.syntax().parent().unwrap_or_else(|| view.file().syntax().clone());
    let external: Vec<SyntaxNode> = parent
        .children()
        .filter(|n| n.kind() == SyntaxKind::CONNECTION && connection_touches_local(n, &local))
        .collect();
    for c in external {
        detach_with_leading_ws(&c);
    }
    // Replace the group with the ungrouped children, in the SLOT the group
    // occupied (children stay where the group was, not appended at the end like
    // `move_scope` does, so order relative to siblings is preserved). The children
    // must carry their OWN complete leading layout (the line break that preceded
    // the group, then the group's indent on EVERY line), because the group node's
    // leading newline does NOT reliably survive its removal: the parser attaches
    // that newline as the group's own first child at file root and as a separate
    // sibling token inside a body. So we re-emit it ourselves and indent all lines.
    // `lead_breaks` is the leading whitespace with its trailing indent stripped
    // (just the newlines), since `indent_block` re-adds the indent on the first
    // line too. We remove the group FIRST (with its leading-ws sibling, detached
    // individually per this file's rule that a multi-child splice deletes only
    // one), then insert the block into the slot they vacated. When nothing survived
    // (empty group, or one with only boundary wiring), there is no block: the
    // ungroup is a pure deletion.
    let moved_src = indent_block(moved_src.trim_end_matches('\n'), &group_indent);
    let lead = leading_ws(group.syntax()).map(|t| t.text().to_string()).unwrap_or_default();
    let lead_breaks = lead.rfind('\n').map(|p| lead[..=p].to_string()).unwrap_or_default();
    // `start` is the slot the group (plus a leading-ws sibling, if any) occupied;
    // after detaching both, the block is inserted there.
    let sibling_ws = matches!(group.syntax().prev_sibling_or_token(), Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::WHITESPACE);
    let start = if sibling_ws { group.syntax().index() - 1 } else { group.syntax().index() };
    detach_with_leading_ws(group.syntax());
    if !moved_src.is_empty() {
        parent.splice_children(start..start, snippet_elements(&format!("{lead_breaks}{moved_src}")));
    }
    Ok(())
}

/// True if a connection INSIDE the group body is boundary wiring, i.e. it has a
/// `self` endpoint (`self.x = ...` / `... = self.x`). A connection is the only
/// internal boundary form; the EXTERNAL legs (parent-scope connections that name
/// the group's port) are detached separately by the caller via
/// `connection_touches_local`. We must NOT also drop an inner connection that
/// merely names the group's local id: inside the body that id resolves to a
/// CHILD of the same name (Weft's same-scope rule), so a real wire between two
/// children where one shadows the group name would be wrongly discarded.
fn connection_is_boundary(conn: &SyntaxNode) -> bool {
    conn.children()
        .filter(|n| n.kind() == SyntaxKind::ENDPOINT)
        .any(|ep| ep_parts(&ep).0.as_deref() == Some("self"))
}

/// De-indent each line of `block` to column 0 by stripping a leading `from`
/// indent. The inverse of `indent_block`: relocating a block to a new scope is
/// `dedent_block(block, old_indent)` then `indent_block(block, new_indent)`, the
/// shape both `remove_group` (ungroup) and `move_scope` use. Each then prepends
/// the relocated block's own leading whitespace at the insertion point, so the
/// re-indent owns ALL indentation uniformly and a block whose FIRST line carries
/// no indent (a decl's `to_string()`) is handled the same as the rest.
fn dedent_block(block: &str, from: &str) -> String {
    // Heredoc body lines are literal text: never de-indent them (a content line
    // that happens to start with the group indent would be silently mangled).
    map_lines_outside_heredoc(block, |l| l.strip_prefix(from).unwrap_or(l).to_string())
}

// ── ops: config / label / description ────────────────────────────────────────

fn set_config(view: &FileView, node_id: &str, key: &str, value: Option<&str>) -> Result<(), EditError> {
    let decl = resolve(view, node_id)?;
    // A connection-origin field is written `node.key = value` (a CONNECTION),
    // not `key: value` inside the body. If one exists, edit IT (keeping the
    // `node.key = ` form), rather than adding a duplicate body field.
    if let Some(conn) = find_connection_origin_field(view, &decl, key) {
        return match value {
            Some(v) => replace_connection_rhs(&conn, v),
            None => { detach_with_leading_ws(&conn); Ok(()) }
        };
    }
    match value {
        Some(v) => set_or_insert_field(&decl, key, v),
        None => { remove_field(&decl, key); Ok(()) }
    }
}

/// A connection-origin config field for `decl.key`: a CONNECTION in the decl's
/// enclosing scope that is a config-origin field on `(decl_local, key)`. Uses the
/// shared `cst::nodes::connection_is_config_origin` so the editor's notion of a
/// config field matches the lowering's exactly (an inline-expr or a two-endpoint
/// edge is NOT a config field, so SetConfig/RemoveConfig can't clobber wiring).
fn find_connection_origin_field(view: &FileView, decl: &Decl, key: &str) -> Option<SyntaxNode> {
    let local = decl.local_id()?;
    let scope = decl.syntax().parent().unwrap_or_else(|| view.file().syntax().clone());
    scope
        .children()
        .find(|n| crate::cst::nodes::connection_is_config_origin(n, Some(&local), Some(key)))
}

/// Replace a connection's RHS (the value after `=`) with `value`, in place. Only
/// the value tokens are swapped: the leading trivia, the `target.port = ` prefix,
/// and any trailing comment are left byte-identical (so editing `t.style = "a"`
/// can't eat the connection's leading newline or its trailing comment).
fn replace_connection_rhs(conn: &SyntaxNode, value: &str) -> Result<(), EditError> {
    replace_value_after(conn, SyntaxKind::EQ, value)
}

/// Replace the VALUE token-run of `node` (everything after the first `sep` token
/// up to a trailing same-line comment) with the tokens of `value`, in place.
/// Leading trivia, the key/prefix + `sep`, and a trailing same-line comment are
/// all preserved verbatim. This is the one in-place value-swap, shared by config
/// fields (sep = COLON) and connection-origin fields (sep = EQ), so neither can
/// drift into reconstructing-the-whole-line (which loses/doubles trivia).
fn replace_value_after(node: &SyntaxNode, sep: SyntaxKind, value: &str) -> Result<(), EditError> {
    let elems: Vec<SyntaxElement> = node.children_with_tokens().collect();
    let sep_idx = elems
        .iter()
        .position(|e| e.kind() == sep)
        .ok_or_else(|| EditError::Unparseable(format!("field has no `{sep:?}` separator")))?;
    // The value run is everything after `sep`, EXCEPT a trailing same-line
    // comment (and the inline whitespace before it), which is layout to keep.
    let value_start = sep_idx + 1;
    let mut value_end = elems.len();
    if let Some(cpos) = elems.iter().rposition(|e| e.kind() == SyntaxKind::COMMENT) {
        let mut keep_from = cpos;
        if cpos > 0 && elems[cpos - 1].kind() == SyntaxKind::WHITESPACE
            && !elems[cpos - 1].as_token().map(|t| t.text().contains('\n')).unwrap_or(false)
        {
            keep_from = cpos - 1;
        }
        if keep_from > value_start {
            value_end = keep_from;
        }
    }
    // Trim TRAILING whitespace out of the value run: the parser sometimes parks a
    // structural token inside the field node (a `\n` for an empty `key:`, or the
    // inline space before a one-liner `}`). That whitespace is layout, not value,
    // so leave it in place rather than detaching it (which collapsed the `}` onto
    // the value line / ate the space before `}`).
    while value_end > value_start && elems[value_end - 1].kind() == SyntaxKind::WHITESPACE {
        value_end -= 1;
    }
    // Detach the existing value-run elements INDIVIDUALLY (collected first, then
    // detached by handle): `splice_children`'s range delete shifts indices
    // mid-operation and removes only one element, so per-handle detach is the
    // reliable removal. Then insert the rebuilt value (a separating space + the
    // lexed value tokens; lexing avoids the ERROR-node wrapper a bare-value parse
    // would produce) at the value position.
    let new_elems = {
        let mut v = raw_token_elements(&[(SyntaxKind::WHITESPACE, " ")]);
        v.extend(value_elements(value)?);
        v
    };
    node.splice_children(value_start..value_start, new_elems);
    // The old value elements are now shifted right by the inserted count; detach
    // them by their (still-valid) handles.
    for el in &elems[value_start..value_end] {
        match el {
            NodeOrToken::Node(n) => n.detach(),
            NodeOrToken::Token(t) => t.detach(),
        }
    }
    Ok(())
}

fn set_label(view: &FileView, node_id: &str, label: Option<&str>) -> Result<(), EditError> {
    let decl = resolve(view, node_id)?;
    match label.filter(|l| !l.is_empty()) {
        Some(l) => set_or_insert_field(&decl, "_label", &format_string(l)?),
        None => { remove_field(&decl, "_label"); Ok(()) }
    }
}

/// Find the CONFIG_FIELD/LABEL_FIELD with key `key` in the decl's body.
fn find_field(decl: &Decl, key: &str) -> Option<SyntaxNode> {
    let body = decl.body()?;
    body.syntax().children().find(|n| {
        matches!(n.kind(), SyntaxKind::CONFIG_FIELD | SyntaxKind::LABEL_FIELD)
            && field_key(n).as_deref() == Some(key)
    })
}

/// The key IDENT of a CONFIG_FIELD/LABEL_FIELD node.
fn field_key(field: &SyntaxNode) -> Option<String> {
    field
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == SyntaxKind::IDENT)
        .map(|t| t.text().to_string())
}

/// Set (replace) or insert a config field `key: value`.
fn set_or_insert_field(decl: &Decl, key: &str, value: &str) -> Result<(), EditError> {
    if let Some(existing) = find_field(decl, key) {
        // Replace only the value tokens (after `:`) in place, leaving the field's
        // leading indent, `key:`, and any trailing comment byte-identical.
        return replace_value_after(&existing, SyntaxKind::COLON, value);
    }
    // No existing field: insert before the body's close brace. The node must
    // have a body; a bare node gets one synthesized.
    insert_field(decl, key, value)
}

/// Insert `key: value` into the decl's body. If the node has no body (bare or a
/// one-liner), a fresh multi-line body is synthesized; otherwise the new field
/// is appended before the existing body's `}`, leaving every existing byte of
/// the body untouched (so heredocs / hand-alignment / comments survive). Splices
/// only the decl node, never lifts elements across trees.
fn insert_field(decl: &Decl, key: &str, value: &str) -> Result<(), EditError> {
    // Same containment gate as the in-place replace path: a value with a bare
    // newline or an unbalanced `}`/`)` would escape the field and corrupt the
    // tree (the insert re-parses `key: value`, so a stray `}` closes the body
    // early). Reject loud before building, so insert and replace agree.
    reject_uncontained_value(value)?;
    let indent = leading_indent(decl.syntax());
    let body_indent = format!("{indent}  ");
    match decl.body() {
        // Has a body: splice the new field before its close brace, in place.
        Some(body) => {
            insert_before_close(&body, snippet_elements(&format!("{body_indent}{key}: {value}\n")))
        }
        // No body: synthesize one with the single field.
        None => {
            let header = decl_header_text(decl);
            let rebuilt = format!(
                "{indent}{} {{\n{body_indent}{key}: {value}\n{indent}}}",
                header.trim()
            );
            splice_decl(decl, &rebuilt)
        }
    }
}

/// The header source of a decl: the `id = Type(sig)->(sig)` text, no body.
fn decl_header_text(decl: &Decl) -> String {
    match decl {
        Decl::Node(n) => n.header().map(|h| h.syntax().to_string()).unwrap_or_default(),
        Decl::Group(g) => g.header().map(|h| h.syntax().to_string()).unwrap_or_default(),
        Decl::Include(i) => i.syntax().to_string(),
    }
}

/// Remove a config field by key. Idempotent (no field = no-op).
fn remove_field(decl: &Decl, key: &str) {
    if let Some(field) = find_field(decl, key) {
        detach_with_leading_ws(&field);
    }
}

/// Set/replace/remove a group's first-body-line `# Description:`.
fn set_group_description(view: &FileView, group_id: &str, desc: Option<&str>) -> Result<(), EditError> {
    let group = resolve_group(view, group_id)?;
    let body = group.body().ok_or_else(|| EditError::GroupNotFound(group_id.to_string()))?;
    let indent = format!("{}  ", leading_indent(group.syntax()));
    let existing = group.description().map(|d| d.syntax().clone());
    match (existing, desc.filter(|d| !d.is_empty())) {
        (Some(node), Some(d)) => {
            let elements = snippet_elements(&format!("# Description: {d}"));
            let idx = node.index();
            node.parent().unwrap().splice_children(idx..idx + 1, elements);
        }
        (Some(node), None) => detach_with_leading_ws(&node),
        (None, Some(d)) => {
            // Insert as the first body line, right after the `{`.
            let brace_idx = body
                .syntax()
                .children_with_tokens()
                .position(|e| e.kind() == SyntaxKind::L_BRACE)
                .ok_or_else(|| EditError::Unparseable("group body missing {".into()))?;
            let at = brace_idx + 1;
            // A single-line body (`{}`) has its `}` on the open-brace line, so a
            // comment first-line would SWALLOW it (`# Description: d}` is one
            // comment). Append a newline + the group's own indent after the
            // description so the `}` drops to its own line. A multi-line body
            // already has structure.
            let group_indent = leading_indent(group.syntax());
            let snippet = if body_is_single_line(&body) {
                format!("\n{indent}# Description: {d}\n{group_indent}")
            } else {
                format!("\n{indent}# Description: {d}")
            };
            body.syntax().splice_children(at..at, snippet_elements(&snippet));
        }
        (None, None) => {}
    }
    Ok(())
}

// ── ops: edges ────────────────────────────────────────────────────────────────

/// Add `target.target_port = source.source_port` into `scope_group`'s body
/// (None = file root). Replaces any existing driver of the same target port
/// (input ports are single-driver).
fn add_edge(
    view: &FileView,
    scope_group: Option<&str>,
    source: &str,
    source_port: &str,
    target: &str,
    target_port: &str,
) -> Result<(), EditError> {
    // Both endpoints must exist (or be `self`).
    require_endpoint(view, source)?;
    require_endpoint(view, target)?;
    // Remove the existing driver of this target port in the same scope.
    remove_driver(view, scope_group, target, target_port);
    let conn = format!("{target}.{target_port} = {source}.{source_port}");
    match target_body(view, scope_group)? {
        InsertTarget::FileRoot(f) => {
            append_to_file(&f, snippet_elements(&format!("{conn}\n")));
            Ok(())
        }
        InsertTarget::GroupBody { body, indent } => {
            insert_before_close(&body, snippet_elements(&format!("{indent}{conn}\n")))
        }
    }
}

/// An endpoint ref must be `self` or resolve to a real node/group.
fn require_endpoint(view: &FileView, id: &str) -> Result<(), EditError> {
    if id == "self" {
        return Ok(());
    }
    match view.resolve_decl(id) {
        Resolution::Found(_) => Ok(()),
        _ => Err(EditError::NodeNotFound(id.to_string())),
    }
}

/// Drop the connection that currently drives `target.target_port` in the scope.
fn remove_driver(view: &FileView, scope_group: Option<&str>, target: &str, target_port: &str) {
    if let Ok(conn) = find_connection(view, scope_group, target, target_port, None, None) {
        detach_with_leading_ws(&conn);
    }
}

/// Remove a connection matching the quad in the given scope. Loud if not found.
fn remove_edge(
    view: &FileView,
    scope_group: Option<&str>,
    source: &str,
    source_port: &str,
    target: &str,
    target_port: &str,
) -> Result<(), EditError> {
    let conn = find_connection(view, scope_group, target, target_port, Some(source), Some(source_port))
        .map_err(|_| EditError::ConnectionNotFound(source.into(), source_port.into(), target.into(), target_port.into()))?;
    detach_with_leading_ws(&conn);
    Ok(())
}

/// Find a CONNECTION in `scope_group`'s body matching the target endpoint
/// (and optionally the source endpoint). Source-side matching is by the as-
/// written ids, exactly what the CST preserves.
fn find_connection(
    view: &FileView,
    scope_group: Option<&str>,
    target: &str,
    target_port: &str,
    source: Option<&str>,
    source_port: Option<&str>,
) -> Result<SyntaxNode, EditError> {
    let scope = match scope_group {
        None => view.file().syntax().clone(),
        Some(g) => resolve_group(view, g)?
            .body()
            .ok_or_else(|| EditError::GroupNotFound(g.to_string()))?
            .syntax()
            .clone(),
    };
    scope
        .children()
        .filter(|n| n.kind() == SyntaxKind::CONNECTION)
        .find(|c| {
            let (t_id, t_port) = endpoint_parts(c, 0);
            let target_ok = t_id.as_deref() == Some(target) && t_port.as_deref() == Some(target_port);
            let source_ok = match (source, source_port) {
                (Some(s), Some(sp)) => {
                    let (s_id, s_port) = endpoint_parts(c, 1);
                    s_id.as_deref() == Some(s) && s_port.as_deref() == Some(sp)
                }
                _ => true,
            };
            target_ok && source_ok
        })
        .ok_or_else(|| EditError::ConnectionNotFound(
            source.unwrap_or("").into(), source_port.unwrap_or("").into(), target.into(), target_port.into(),
        ))
}

/// The (id, port) of the `nth` ENDPOINT child of a CONNECTION node, via the
/// typed view's single extractor.
fn endpoint_parts(conn: &SyntaxNode, nth: usize) -> (Option<String>, Option<String>) {
    match conn.children().filter(|n| n.kind() == SyntaxKind::ENDPOINT).nth(nth) {
        Some(ep) => ep_parts(&ep),
        None => (None, None),
    }
}

// ── ops: rename / move / ports ────────────────────────────────────────────────

/// Rename a group: rewrite its header id and every `oldLabel.port` reference in
/// the same scope (boundary edges referencing the group from outside).
fn rename_group(view: &FileView, old_label: &str, new_label: &str) -> Result<(), EditError> {
    if new_label.is_empty() {
        return Err(EditError::InvalidArgument("rename to empty label".into()));
    }
    if old_label == new_label {
        return Ok(());
    }
    let group = resolve_group(view, old_label)?;
    // Reject a rename that collides with an existing member of the group's own
    // scope (would manufacture two same-id decls + ambiguous references). The
    // parent scope is everything before the group's last id segment.
    let scoped = view.scoped_id_of(&Decl::Group(group.clone()))
        .ok_or_else(|| EditError::GroupNotFound(old_label.to_string()))?;
    let parent_scope = scoped.rsplit_once('.').map(|(p, _)| p);
    reject_if_taken(view, parent_scope, new_label)?;
    // Rewrite the header's leading IDENT token.
    let header = group.header().ok_or_else(|| EditError::GroupNotFound(old_label.to_string()))?;
    let id_tok = header
        .syntax()
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == SyntaxKind::IDENT)
        .ok_or_else(|| EditError::GroupNotFound(old_label.to_string()))?;
    // Rewrite every reference to the group, in ANY scope, via the same
    // scope-aware query RemoveNode uses (so rename and remove agree on what
    // "references this decl" means). An endpoint resolving to the group has its
    // head IDENT (the old label) replaced. Collect the connection handles first
    // (resolve-then-mutate), then rewrite.
    let refs = view.connections_referencing(&Decl::Group(group.clone()));
    replace_token_text(&id_tok, new_label);
    for c in refs {
        for ep in c.children().filter(|n| n.kind() == SyntaxKind::ENDPOINT) {
            if let Some(t) = ep
                .children_with_tokens()
                .filter_map(|e| e.into_token())
                .find(|t| t.kind() == SyntaxKind::IDENT && t.text() == old_label)
            {
                replace_token_text(&t, new_label);
            }
        }
    }
    Ok(())
}

/// Replace a single token's text by splicing a re-built token in its place.
/// rowan tokens are immutable; we replace via the parent's `splice_children`.
fn replace_token_text(tok: &SyntaxToken, new_text: &str) {
    let parent = tok.parent().unwrap();
    let idx = tok.index();
    let replacement = make_token(tok.kind(), new_text);
    parent.splice_children(idx..idx + 1, vec![replacement]);
}

/// Build a single mutable token of `kind` carrying `text`, by parsing a snippet
/// that yields exactly that token and lifting it. For an IDENT we parse a bare
/// word; the lexer tags it IDENT.
fn make_token(kind: SyntaxKind, text: &str) -> SyntaxElement {
    raw_token_elements(&[(kind, text)]).into_iter().next().unwrap()
}

/// Build mutable token elements directly from `(kind, text)` pairs via a
/// throwaway green tree. Used to splice raw tokens (a renamed ident, a config
/// value) into a tree WITHOUT going through the parser, which wraps a bare,
/// out-of-context fragment in an ERROR node. The elements are `clone_for_update`
/// so they're tree-independent and safe to splice anywhere.
fn raw_token_elements(tokens: &[(SyntaxKind, &str)]) -> Vec<SyntaxElement> {
    let mut b = rowan::GreenNodeBuilder::new();
    b.start_node(SyntaxKind::WEFT_FILE.into());
    for (kind, text) in tokens {
        b.token((*kind).into(), text);
    }
    b.finish_node();
    SyntaxNode::new_root(b.finish()).clone_for_update().children_with_tokens().collect()
}

/// Reject a config-value string that would BREAK CONTAINMENT, escaping the field
/// and corrupting the surrounding tree: a raw newline in a WHITESPACE token
/// (would split the line; a heredoc's own newlines live inside its single opaque
/// token, so they don't count), or an unbalanced `}`/`)` that would close the
/// enclosing body/sig early. An ERROR token (an operator like `|`, or an invalid
/// byte) is harmless: it is one lossless token that stays in value position, so
/// it is NOT rejected (rejecting it false-flagged legit type exprs).
///
/// The ONE containment gate, shared by both value-writing paths (in-place
/// replace via `value_elements`, and insert/synthesize-body via `insert_field`),
/// so a value the replace path rejects can't slip through the insert path and
/// corrupt the tree (an unbalanced `}` would close the node body early).
fn reject_uncontained_value(value: &str) -> Result<(), EditError> {
    let toks = crate::cst::lexer::lex(value);
    // A raw newline outside a heredoc would break the value onto a new line.
    let has_bare_newline = toks
        .iter()
        .any(|t| t.kind == SyntaxKind::WHITESPACE && t.text.contains('\n'));
    // Bracket balance: a closer with no opener (depth < 0) escapes the field.
    let mut depth: i32 = 0;
    let mut unbalanced = false;
    // `[...]` arrays + `{...}`/heredoc/marker bodies are single opaque tokens
    // (the lexer doesn't emit bracket tokens for them), so only the paren/brace
    // PAIR tokens can appear unbalanced in a value.
    for t in &toks {
        match t.kind {
            SyntaxKind::L_BRACE | SyntaxKind::L_PAREN => depth += 1,
            SyntaxKind::R_BRACE | SyntaxKind::R_PAREN => {
                depth -= 1;
                if depth < 0 {
                    unbalanced = true;
                    break;
                }
            }
            _ => {}
        }
    }
    if has_bare_newline || unbalanced || depth != 0 {
        return Err(EditError::InvalidArgument(format!(
            "value would break out of its field (unbalanced brackets or a newline): {value:?}"
        )));
    }
    Ok(())
}

/// Lex a contained config-value string into mutable token elements (no parser,
/// no ERROR wrapper) and emit them verbatim. Gated by `reject_uncontained_value`
/// first, so a value that would escape the field is a loud error, not corruption.
fn value_elements(value: &str) -> Result<Vec<SyntaxElement>, EditError> {
    reject_uncontained_value(value)?;
    let toks = crate::cst::lexer::lex(value);
    let pairs: Vec<(SyntaxKind, &str)> = toks.iter().map(|t| (t.kind, t.text)).collect();
    Ok(raw_token_elements(&pairs))
}

/// Move a node/group into a target scope (None = file root). Detach the decl's
/// subtree (as text, re-indented) and re-insert it in the target.
fn move_scope(view: &FileView, id: &str, target_group: Option<&str>) -> Result<(), EditError> {
    let decl = resolve(view, id)?;
    let local = decl.local_id().ok_or_else(|| EditError::InvalidArgument("cannot move an unnamed decl".into()))?;
    // A move into the scope the decl ALREADY lives in is a no-op (the graph view
    // can emit it when a drag ends inside the same parent). Detect it up front and
    // succeed silently: otherwise `reject_if_taken` below would see the decl's own
    // scoped id and wrongly report it as a duplicate of itself.
    let current_parent = view
        .scoped_id_of(&decl)
        .and_then(|scoped| scoped.rsplit_once('.').map(|(parent, _)| parent.to_string()));
    if current_parent.as_deref() == target_group {
        return Ok(());
    }
    // Reject a move into a scope that already has a member with this local id
    // (would make two same-id decls), before mutating anything.
    reject_if_taken(view, target_group, &local)?;

    // The node's connection-origin config fields (`x.style = "v"`, separate
    // CONNECTION lines in the CURRENT scope) belong to the node and travel with
    // it; collect them first.
    let origin_fields: Vec<SyntaxNode> = connections_origin_targeting(view, &decl, &local);

    // Any OTHER edge that references this node (a real wiring edge like
    // `y.data = x.value`, or the source group's boundary wiring `self.o = x.value`)
    // cannot survive a scope change: Weft is same-scope-only, so after the move
    // the edge's two ends are in different scopes and can't reach each other.
    // Refuse the move loudly rather than silently dropping the wire; the user
    // must rewire/disconnect first (this matches the graph view, which blocks
    // moving a node that's wired across the boundary).
    let blocking: Vec<SyntaxNode> = view.connections_referencing(&decl)
        .into_iter()
        .filter(|c| !origin_fields.iter().any(|o| o == c))
        .collect();
    if !blocking.is_empty() {
        return Err(EditError::InvalidArgument(format!(
            "cannot move '{id}': it is wired by {} connection(s) that would cross the scope boundary; disconnect them first",
            blocking.len()
        )));
    }
    // Each origin field's text owns its leading newline trivia; trim block edges
    // so the relocated lines join with single newlines (no stray blank lines).
    let mut field_blocks: Vec<String> = Vec::new();
    for f in &origin_fields {
        field_blocks.push(dedent_block(&f.to_string(), &leading_indent(f)).trim().to_string());
    }

    // Capture the decl's source, de-indented to column 0, then detach the decl
    // and its origin fields (resolve-then-mutate: handles collected above).
    let old_indent = leading_indent(decl.syntax());
    let block = dedent_block(&decl.syntax().to_string(), &old_indent).trim().to_string();
    detach_with_leading_ws(decl.syntax());
    for f in &origin_fields {
        detach_with_leading_ws(f);
    }

    // Re-insert the decl + its origin fields at the target, one per line.
    let mut combined = block;
    for fb in &field_blocks {
        combined.push('\n');
        combined.push_str(fb.trim_end_matches('\n'));
    }
    match target_body(view, target_group)? {
        InsertTarget::FileRoot(f) => {
            append_to_file(&f, snippet_elements(&format!("\n{combined}\n")));
            Ok(())
        }
        InsertTarget::GroupBody { body, indent } => {
            let reindented = indent_block(&combined, &indent);
            insert_before_close(&body, snippet_elements(&format!("{reindented}\n")))
        }
    }
}

/// The connection-origin config fields (`{local}.key = value` CONNECTION lines)
/// in `decl`'s current enclosing scope that target it. These are part of the
/// node's config and travel with it on a scope move.
fn connections_origin_targeting(view: &FileView, decl: &Decl, local: &str) -> Vec<SyntaxNode> {
    let scope = decl.syntax().parent().unwrap_or_else(|| view.file().syntax().clone());
    // Any port (None): every config-origin field on this node travels with it.
    scope
        .children()
        .filter(|n| crate::cst::nodes::connection_is_config_origin(n, Some(local), None))
        .collect()
}

/// Indent every non-empty line of `block` by `indent`, EXCEPT lines inside a
/// triple-backtick heredoc, whose content is literal text and must not move. A
/// line containing a ``` fence toggles heredoc state; the fence line itself is
/// re-indented (it's part of the field's layout), the body lines between fences
/// are left byte-identical.
fn indent_block(block: &str, indent: &str) -> String {
    map_lines_outside_heredoc(block, |l| {
        if l.trim().is_empty() { l.to_string() } else { format!("{indent}{l}") }
    })
}

/// Apply `f` to each line of `block` that is NOT inside a heredoc body; heredoc
/// body lines pass through verbatim. The single home for "transform layout lines
/// but never heredoc content," shared by indent and dedent. Joins with `\n` (no
/// trailing newline; callers add their own line ending, matching `.lines()`).
fn map_lines_outside_heredoc(block: &str, f: impl Fn(&str) -> String) -> String {
    let mut in_heredoc = false;
    let mut out = Vec::new();
    for line in block.lines() {
        let fence = line.matches("```").count() % 2 == 1; // odd # of fences toggles
        if in_heredoc {
            // inside the body: pass verbatim; a fence line ends the heredoc.
            out.push(line.to_string());
            if fence {
                in_heredoc = false;
            }
        } else {
            out.push(f(line));
            if fence {
                in_heredoc = true;
            }
        }
    }
    out.join("\n")
}

/// Rewrite a node/group's COMPLETE port signature: rebuild the decl with
/// `id = Type` + the new signature as its header, preserving the body verbatim.
/// The new signature is the single source of the decl's ports, so any post-body
/// output ports (`} -> (out)`) are normalized into the pre-body signature and
/// the post-body clause is dropped (its outputs, if still wanted, are passed in
/// `outputs` and re-emitted pre-body; this is why `rebuild_decl` reconstructs
/// only `header + body` and omits the post-body sibling).
fn update_ports(view: &FileView, id: &str, inputs: &[PortSig], outputs: &[PortSig]) -> Result<(), EditError> {
    let decl = resolve(view, id)?;
    if let Decl::Include(_) = decl {
        return Err(EditError::InvalidArgument("cannot set ports on an include".into()));
    }
    let header = decl_header_text(&decl);
    // head = `id = Type` (everything up to the first `(` or `->`).
    let (head, _) = split_header_head(&header);
    let new_header = format!("{}{}", head.trim_end(), build_signature(inputs, outputs));
    rebuild_decl(&decl, &new_header)
}

/// Rebuild a decl in place with a new header line, preserving its body content
/// verbatim. The decl is re-parsed from text and the WHOLE decl node is spliced
/// over the original. This is the one decl-reconstruction path: it never lifts
/// still-parented elements across trees (which corrupts a `splice_children`),
/// and the result is structurally identical to a freshly-parsed decl.
fn rebuild_decl(decl: &Decl, new_header: &str) -> Result<(), EditError> {
    let indent = leading_indent(decl.syntax());
    let rebuilt = match decl.body() {
        Some(body) => format!("{indent}{} {}", new_header.trim(), body.syntax().to_string()),
        None => format!("{indent}{}", new_header.trim()),
    };
    splice_decl(decl, &rebuilt)
}

/// Replace `decl`'s subtree with the decl parsed from `text` (which already
/// carries the decl's own leading indent). One splice of the decl node, no
/// element lifting.
fn splice_decl(decl: &Decl, text: &str) -> Result<(), EditError> {
    let elements = snippet_elements(text);
    let idx = decl.syntax().index();
    let parent = decl
        .syntax()
        .parent()
        .ok_or_else(|| EditError::Unparseable("decl has no parent".into()))?;
    parent.splice_children(idx..idx + 1, elements);
    Ok(())
}

/// Split a header string into (`id = Type`, rest-with-sig). The head ends at the
/// first `(` or `->`.
fn split_header_head(header: &str) -> (String, String) {
    let cut = header.find('(').or_else(|| header.find("->")).unwrap_or(header.len());
    (header[..cut].to_string(), header[cut..].to_string())
}

/// Build a `(in) -> (out)` signature string from port sigs.
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

/// Render a string to a `.weft` value token (quoted, or heredoc if multi-line).
fn format_string(s: &str) -> Result<String, EditError> {
    if s.contains('\n') {
        // A multi-line value is emitted as a ```...``` heredoc. The heredoc has
        // NO escape for an inner fence, so a value that itself contains ``` can't
        // be encoded faithfully: reject loudly rather than emit source that
        // re-parses wrong (a silent corrupt encode is the worst outcome).
        if s.contains("```") {
            return Err(EditError::InvalidArgument(
                "multi-line value cannot contain ``` (no heredoc fence escape)".into(),
            ));
        }
        Ok(format!("```\n{s}\n```"))
    } else {
        Ok(format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
    }
}

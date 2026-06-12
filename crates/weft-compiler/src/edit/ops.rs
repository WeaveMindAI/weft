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
/// SYNC: apply_op <-> extension-vscode/src/webview/lib/projection/apply.ts applyOp (the editor's optimistic mirror of these op semantics)
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
        RenameGroup { group, new_label } => rename_container(view, group, new_label, ContainerKind::Group),
        MoveNodeScope { node, target_group } => move_scope(view, node, target_group.as_deref(), ContainerKind::Node),
        MoveGroupScope { group, target_group } => move_scope(view, group, target_group.as_deref(), ContainerKind::Group),
        UpdateNodePorts { node, inputs, outputs } => update_ports(view, node, inputs, outputs, ContainerKind::Node),
        UpdateGroupPorts { group, inputs, outputs } => update_ports(view, group, inputs, outputs, ContainerKind::Group),
        SetGroupDescription { group, description } => {
            set_group_description(view, group, description.as_deref())
        }
        AddLoop { label, parent_group } => add_decl(
            view, parent_group.as_deref(), label,
            // Body left empty: parallel defaults to false, over/carry empty.
            &format!("{label} = Loop() -> () {{}}"),
        ),
        RemoveLoop { loop_id } => remove_loop(view, loop_id),
        RenameLoop { loop_id, new_label } => rename_container(view, loop_id, new_label, ContainerKind::Loop),
        MoveLoopScope { loop_id, target_group } => move_scope(view, loop_id, target_group.as_deref(), ContainerKind::Loop),
        UpdateLoopPorts { loop_id, inputs, outputs } => update_ports(view, loop_id, inputs, outputs, ContainerKind::Loop),
        SetLoopConfig { loop_id, key, value } => set_loop_config(view, loop_id, key, value),
        RemoveLoopConfig { loop_id, key } => remove_loop_config(view, loop_id, key),
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

/// The decl's kind name for error messages. The single home for the
/// kind-mismatch wording every kind-routed op uses.
fn kind_name(decl: &Decl) -> &'static str {
    match decl {
        Decl::Group(_) => "Group",
        Decl::Loop(_) => "Loop",
        Decl::Node(_) => "Node",
        Decl::Include(_) => "Include",
    }
}

/// The honest kind-mismatch error: the id EXISTS but is the wrong
/// kind of decl for the op. Distinct from ContainerNotFound, which
/// would send the user hunting for a typo in an id that is fine.
fn kind_mismatch(op: &str, id: &str, expected: &str, actual: &Decl) -> EditError {
    EditError::InvalidArgument(format!(
        "{op} called on '{id}' which is a {} decl, not a {expected}",
        kind_name(actual),
    ))
}

/// Resolve specifically to a group decl.
fn resolve_group(view: &FileView, id: &str) -> Result<crate::cst::nodes::GroupDecl, EditError> {
    match resolve(view, id)? {
        Decl::Group(g) => Ok(g),
        other => Err(kind_mismatch("a Group op", id, "Group", &other)),
    }
}

/// Resolve specifically to a loop decl.
fn resolve_loop(view: &FileView, id: &str) -> Result<crate::cst::nodes::LoopDecl, EditError> {
    match resolve(view, id)? {
        Decl::Loop(l) => Ok(l),
        other => Err(kind_mismatch("a Loop op", id, "Loop", &other)),
    }
}

/// The body node of a container scope ref (Group OR Loop), or the file root
/// when `scope_group` is None. The ONE scope-resolution rule shared by the
/// connection-finder (`find_connection`) and the body-insert path
/// (`target_body`), so a wire inside a Loop resolves its scope exactly like an
/// insert does. A Group-only resolver here silently failed to find/replace
/// loop-body drivers, appending a second driver on the same input port.
fn scope_container_body(view: &FileView, scope_group: &str) -> Result<Body, EditError> {
    let decl = resolve(view, scope_group)?;
    let body = match &decl {
        Decl::Group(grp) => grp.body(),
        Decl::Loop(lp) => lp.body(),
        _ => return Err(EditError::ContainerNotFound(scope_group.to_string())),
    };
    body.ok_or_else(|| EditError::ContainerNotFound(scope_group.to_string()))
}

fn scope_body(view: &FileView, scope_group: Option<&str>) -> Result<SyntaxNode, EditError> {
    match scope_group {
        None => Ok(view.file().syntax().clone()),
        Some(g) => Ok(scope_container_body(view, g)?.syntax().clone()),
    }
}

/// The body to insert into for a given parent ref. Accepts groups AND
/// loops as containers. None = the file root.
fn target_body(view: &FileView, parent_group: Option<&str>) -> Result<InsertTarget, EditError> {
    match parent_group {
        None => Ok(InsertTarget::FileRoot(view.file().clone())),
        Some(g) => {
            let body = scope_container_body(view, g)?;
            let indent = group_body_indent_decl(&resolve(view, g)?);
            Ok(InsertTarget::GroupBody { body, indent })
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

/// Parse `snippet` as the INSIDE of a body and return the elements that lived
/// inside the synthetic `{ ... }`. Use this when the snippet is body-grammar
/// content (a CONFIG_FIELD, a connection) that the file grammar would parse as
/// an ERROR node. Wrapping in synthetic braces lets the parser use body rules
/// so the result is a real CONFIG_FIELD / CONNECTION node, which later editor
/// passes (find_field, ...) can recognize.
fn snippet_elements_as_body_content(snippet: &str) -> Vec<SyntaxElement> {
    // Synthesize a wrapper node so the snippet is parsed in body context.
    // The wrapper `placeholder = X { ... }` ensures the snippet sits inside a
    // BODY whose children include real CONFIG_FIELD / CONNECTION nodes.
    let wrapper_src = format!("__edit_wrap_placeholder = X {{\n{snippet}\n}}\n");
    let root = parse(&wrapper_src).clone_for_update();
    let file = match WeftFile::cast(root) {
        Some(f) => f,
        None => return Vec::new(),
    };
    let decl = match file.syntax().children().next() {
        Some(n) => n,
        None => return Vec::new(),
    };
    let body = decl
        .descendants()
        .find(|n| n.kind() == SyntaxKind::BODY);
    let body = match body {
        Some(b) => b,
        None => return Vec::new(),
    };
    // Drop the wrapping braces and ALL surrounding whitespace (both wrapper-
    // injected and snippet-author-provided). The caller controls insert
    // layout via the elements it splices around our result, so leaving any
    // leading/trailing trivia here doubles newlines when the target body
    // already has trailing trivia of its own (the classic "blank line
    // accumulates after every edit" bug). Strip everything: parser will
    // re-emit clean elements.
    let mut elems: Vec<SyntaxElement> = body.children_with_tokens().collect();
    while let Some(first) = elems.first() {
        match first {
            NodeOrToken::Token(t)
                if t.kind() == SyntaxKind::L_BRACE || t.kind() == SyntaxKind::WHITESPACE =>
            {
                elems.remove(0);
            }
            _ => break,
        }
    }
    while let Some(last) = elems.last() {
        match last {
            NodeOrToken::Token(t)
                if t.kind() == SyntaxKind::R_BRACE || t.kind() == SyntaxKind::WHITESPACE =>
            {
                elems.pop();
            }
            _ => break,
        }
    }
    // Detach each element so it can be re-spliced into the target tree.
    for el in &elems {
        match el {
            NodeOrToken::Node(n) => n.detach(),
            NodeOrToken::Token(t) => t.detach(),
        }
    }
    elems
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

/// Splice `elements` into `body` immediately before its closing `}`, owning
/// the layout: leading newline+indent before the inserted content, single
/// newline after it. Any existing trailing whitespace inside the body before
/// `}` is replaced so repeated inserts don't accumulate blank lines.
///
/// `indent` is the body's content indent (decl's leading indent + 2 spaces),
/// provided by the caller. Snippet-author whitespace is stripped upstream
/// (snippet_elements_as_body_content), so the caller has full control.
fn insert_before_close_with_indent(
    body: &Body,
    indent: &str,
    elements: Vec<SyntaxElement>,
) -> Result<(), EditError> {
    let brace = body
        .close_brace()
        .ok_or_else(|| EditError::Unparseable("group body has no closing brace".into()))?;
    let at = brace.index();
    // Detach any trailing WHITESPACE immediately before `}` so we own the
    // spacing. Without this, the previous sibling's trailing `\n` (or worse,
    // an accumulated `\n  \n  `) sits between us and `}` and we get blank
    // lines that grow over repeated edits.
    if at > 0 {
        if let Some(NodeOrToken::Token(t)) = body.syntax().children_with_tokens().nth(at - 1) {
            if t.kind() == SyntaxKind::WHITESPACE {
                t.detach();
            }
        }
    }
    let at = body
        .close_brace()
        .map(|b| b.index())
        .ok_or_else(|| EditError::Unparseable("close brace gone after detach".into()))?;
    let mut elems: Vec<SyntaxElement> =
        raw_token_elements(&[(SyntaxKind::WHITESPACE, &format!("\n{indent}"))]);
    elems.extend(elements);
    // The detached trailing whitespace carried the closing brace's own
    // indent; restore it (the content indent minus the 2-space body
    // step, see `group_body_indent_decl`) so a nested container's `}`
    // doesn't land at column 0 after the edit.
    let brace_indent = indent.strip_suffix("  ").unwrap_or("");
    elems.extend(raw_token_elements(&[(
        SyntaxKind::WHITESPACE,
        &format!("\n{brace_indent}"),
    )]));
    body.syntax().splice_children(at..at, elems);
    Ok(())
}

/// Splice `elements` into `body` immediately before its closing `}`. Used
/// by callers that have already authored the surrounding whitespace into
/// `elements` themselves. Prefer `insert_before_close_with_indent` for any
/// new caller so layout stays uniform across repeated edits.
fn insert_before_close(body: &Body, elements: Vec<SyntaxElement>) -> Result<(), EditError> {
    let brace = body
        .close_brace()
        .ok_or_else(|| EditError::Unparseable("group body has no closing brace".into()))?;
    let at = brace.index();
    // A single-line body (`{}`, `{ x }`) has its content + close brace on the
    // open-brace line, so the inserted content (which carries its own leading
    // indent + trailing newline) would glue onto it. Open the body: prepend a
    // newline before the content so it sits on its own indented line, and (because
    // the inserted content's trailing newline would otherwise drop `}` to COLUMN 0)
    // append the group's own indent before `}` so the close brace lines up with its
    // header. For a top-level group that indent is empty; for a NESTED group it is
    // the header's indent, which the old code omitted (close brace landed at col 0).
    let mut elems = Vec::new();
    if body_is_single_line(body) {
        elems.extend(raw_token_elements(&[(SyntaxKind::WHITESPACE, "\n")]));
    }
    elems.extend(elements);
    if body_is_single_line(body) {
        let group_indent = body_owner_indent(body);
        if !group_indent.is_empty() {
            elems.extend(raw_token_elements(&[(SyntaxKind::WHITESPACE, &group_indent)]));
        }
    }
    body.syntax().splice_children(at..at, elems);
    Ok(())
}

/// The indent of the decl that owns `body` (its group header's leading indent).
/// This is the column the body's close brace `}` should sit at. The body's parent
/// in the CST is the owning group decl.
fn body_owner_indent(body: &Body) -> String {
    body.syntax()
        .parent()
        .map(|owner| leading_indent(&owner))
        .unwrap_or_default()
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

/// Body indent for any container decl (group or loop): the header's own indent
/// + 2 spaces. Read from the decl's leading whitespace.
fn group_body_indent_decl(decl: &Decl) -> String {
    format!("{}  ", leading_indent(decl.syntax()))
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
/// children move up one scope (de-indented). Group-only: routing a
/// Loop through this op is a caller bug (the webview emits
/// `RemoveLoop` for loops). Fail loud instead of silently absorbing.
fn remove_group(view: &FileView, group_id: &str) -> Result<(), EditError> {
    let group = resolve_group(view, group_id)?;
    remove_container(view, Decl::Group(group), group_id)
}

/// Loop-only mirror of `remove_group`. The un-loop shape is identical
/// to ungrouping (header + close brace gone, children de-indented
/// into the parent scope, boundary wiring dropped; config fields
/// inside the loop body are also dropped since they have no meaning
/// outside a loop).
fn remove_loop(view: &FileView, loop_id: &str) -> Result<(), EditError> {
    let lp = resolve_loop(view, loop_id)?;
    remove_container(view, Decl::Loop(lp), loop_id)
}

fn remove_container(view: &FileView, decl: Decl, id: &str) -> Result<(), EditError> {
    let local = decl.local_id().unwrap_or_default();
    let body = decl.body().ok_or_else(|| EditError::ContainerNotFound(id.to_string()))?;
    let decl_syntax = decl.syntax();

    let group_indent = leading_indent(decl_syntax);
    let inner_indent = format!("{group_indent}  ");

    let mut moved_src = String::new();
    for child in body.syntax().children() {
        match child.kind() {
            SyntaxKind::NODE_DECL | SyntaxKind::GROUP_DECL | SyntaxKind::LOOP_DECL | SyntaxKind::INCLUDE_DECL => {
                moved_src.push_str(&dedent_block(&child.to_string(), &inner_indent));
                moved_src.push('\n');
            }
            SyntaxKind::CONNECTION => {
                if !connection_is_boundary(&child) {
                    moved_src.push_str(&dedent_block(&child.to_string(), &inner_indent));
                    moved_src.push('\n');
                }
            }
            // CONFIG_FIELD inside a loop body: dropped on ungroup
            // (loop config has no meaning at file/group scope).
            _ => {}
        }
    }
    let parent = decl_syntax.parent().unwrap_or_else(|| view.file().syntax().clone());
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
    let lead = leading_ws(decl_syntax).map(|t| t.text().to_string()).unwrap_or_default();
    let lead_breaks = lead.rfind('\n').map(|p| lead[..=p].to_string()).unwrap_or_default();
    let sibling_ws = matches!(decl_syntax.prev_sibling_or_token(), Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::WHITESPACE);
    let start = if sibling_ws { decl_syntax.index() - 1 } else { decl_syntax.index() };
    detach_with_leading_ws(decl_syntax);
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
    // NODE-only, same kind-honesty discipline as rename/move/ports:
    // loop config rides `SetLoopConfig` / `RemoveLoopConfig` (groups
    // take no config at all). Silently accepting a container here
    // would fork one operation across two op families.
    if !matches!(decl, Decl::Node(_)) {
        return Err(kind_mismatch("SetConfig/RemoveConfig", node_id, "Node", &decl));
    }
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
    // A `_label` field is only valid on a Node. The merged group/loop
    // lowering rejects a label field in a container body as a compile
    // error, so a setLabel targeting a Group/Loop would author an
    // uncompilable file. Fail at edit time with the honest kind error
    // instead (containers are renamed via renameGroup / renameLoop).
    if !matches!(decl, Decl::Node(_)) {
        return Err(kind_mismatch("setLabel", node_id, "Node", &decl));
    }
    match label.filter(|l| !l.is_empty()) {
        Some(l) => set_or_insert_field(&decl, "_label", &format_string(l)?),
        None => { remove_field(&decl, "_label"); Ok(()) }
    }
}

/// The string entries of a loop's `carry: [...]` config field. The `[...]`
/// value lexes as ONE opaque JSON_VALUE token, so parse it as JSON (non-list
/// or non-string entries are a config error the compiler reports; the sweep
/// just sees no carry names). Used by the dangling-wire sweep to recognize
/// the carry-synthesized input side.
fn read_carry_list(decl: &Decl) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for field in find_fields(decl, "carry") {
        for token in field.descendants_with_tokens().filter_map(|e| e.into_token()) {
            if token.kind() == SyntaxKind::JSON_VALUE {
                if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(token.text()) {
                    for item in items {
                        if let serde_json::Value::String(s) = item {
                            out.insert(s);
                        }
                    }
                }
            }
        }
    }
    out
}

/// Every CONFIG_FIELD / LABEL_FIELD child of `decl`'s body whose key matches.
/// Returned in source order. Used to collapse duplicates: edit the first,
/// detach the rest.
fn find_fields(decl: &Decl, key: &str) -> Vec<SyntaxNode> {
    let Some(body) = decl.body() else { return Vec::new(); };
    body.syntax()
        .children()
        .filter(|n| {
            matches!(n.kind(), SyntaxKind::CONFIG_FIELD | SyntaxKind::LABEL_FIELD)
                && field_key(n).as_deref() == Some(key)
        })
        .collect()
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
    let existing = find_fields(decl, key);
    if let Some(first) = existing.first() {
        // Replace only the value tokens (after `:`) on the first match.
        replace_value_after(first, SyntaxKind::COLON, value)?;
        // Detach any duplicates so subsequent reads see a single source of truth.
        // Duplicates can exist when an earlier set_config ran against a stale tree
        // (e.g. a batched op sequence) or after a hand edit. Collapse them here so
        // the tree is self-healing.
        for dup in existing.iter().skip(1) {
            detach_with_leading_ws(dup);
        }
        return Ok(());
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
            // Snippet carries just the `key: value` content; the helper owns
            // surrounding whitespace so repeated edits don't accumulate
            // blank lines.
            let snippet = format!("{key}: {value}");
            insert_before_close_with_indent(&body, &body_indent, snippet_elements_as_body_content(&snippet))
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
        Decl::Loop(l) => l.header().map(|h| h.syntax().to_string()).unwrap_or_default(),
        Decl::Include(i) => i.syntax().to_string(),
    }
}

/// Remove a config field by key. Idempotent (no field = no-op). Removes ALL
/// occurrences so accumulated duplicates are cleaned by a single RemoveConfig.
fn remove_field(decl: &Decl, key: &str) {
    for field in find_fields(decl, key) {
        detach_with_leading_ws(&field);
    }
}

/// Set or insert a loop config field (`parallel: true`, `over: [...]`,
/// `carry: [...]`, `max_iters: 100`, `trim_on_mismatch: false`). The
/// value is a pre-formatted source token. Replaces in place if present.
fn set_loop_config(view: &FileView, loop_id: &str, key: &str, value: &str) -> Result<(), EditError> {
    let lp = resolve_loop(view, loop_id)?;
    let decl = Decl::Loop(lp);
    set_or_insert_field(&decl, key, value)
}

/// Remove a loop config field by key. Idempotent.
fn remove_loop_config(view: &FileView, loop_id: &str, key: &str) -> Result<(), EditError> {
    let lp = resolve_loop(view, loop_id)?;
    remove_field(&Decl::Loop(lp), key);
    Ok(())
}

/// Set/replace/remove a group's first-body-line `# Description:`.
fn set_group_description(view: &FileView, group_id: &str, desc: Option<&str>) -> Result<(), EditError> {
    let group = resolve_group(view, group_id)?;
    let body = group.body().ok_or_else(|| EditError::ContainerNotFound(group_id.to_string()))?;
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
            // already has structure. `body_owner_indent` is the ONE definition of
            // "the column a body's close brace sits at" (also used by
            // `insert_before_close`), so the two never drift.
            let group_indent = body_owner_indent(&body);
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
    // Both endpoints must exist (or be `self`). Refs are SCOPE-LOCAL: `x`
    // inside scope `G` means `G.x`, not a file-wide `x`.
    require_endpoint(view, scope_group, source)?;
    require_endpoint(view, scope_group, target)?;
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

/// An endpoint ref must be `self` (only inside a scope) or resolve by the
/// language's connection-scoping rule, which is exactly TWO probes (mirroring
/// `rescope_endpoint` in the lowering and `endpoint_resolves_to` in the typed
/// view): a ref `x` inside scope `G` is `G.x` if that exact id exists, else
/// the BARE top-level `x`. There is NO intermediate-ancestor resolution: the
/// lowering only prefixes an immediate-scope child and otherwise leaves the id
/// bare (a bare id wires to a top-level node), so accepting `Outer.x` for a
/// ref inside `Outer.Inner` would validate an edge the compiler can't wire.
/// Resolving file-wide instead let an edge validate against a same-named node
/// in an UNRELATED scope; the two-probe rule is scope-local, immediate match
/// winning.
/// SYNC: require_endpoint <-> crates/weft-compiler/src/weft_compiler.rs
/// rescope_endpoint, crates/weft-compiler/src/cst/nodes.rs endpoint_resolves_to
fn require_endpoint(view: &FileView, scope_group: Option<&str>, id: &str) -> Result<(), EditError> {
    if id == "self" {
        // `self` names the enclosing container; it is meaningless at file root.
        return match scope_group {
            Some(_) => Ok(()),
            None => Err(EditError::NodeNotFound("self".into())),
        };
    }
    // An endpoint id is a SINGLE segment (a local name or `self`). A dotted ref
    // would make probe 2 (bare top-level) accept a nested scoped id and author a
    // 3-segment endpoint the grammar silently truncates to (node, port),
    // mis-wiring. Reject it so the malformed state is unrepresentable.
    if id.contains('.') {
        return Err(EditError::InvalidArgument(format!(
            "endpoint id must be a single segment; got '{id}'"
        )));
    }
    // Probe 1: an immediate-scope child `{scope}.{id}`.
    if let Some(g) = scope_group {
        let prefix = view.scoped_id_of(&resolve(view, g)?).ok_or_else(|| EditError::ContainerNotFound(g.to_string()))?;
        if view.scoped_id_exists(&format!("{prefix}.{id}")) {
            return Ok(());
        }
    }
    // Probe 2: a bare top-level `id` (the lowering's outer-ref fallthrough).
    if view.scoped_id_exists(id) {
        return Ok(());
    }
    Err(EditError::NodeNotFound(id.to_string()))
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
        .map_err(|_| EditError::ConnectionNotFound(target.into(), target_port.into(), source.into(), source_port.into()))?;
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
    let scope = scope_body(view, scope_group)?;
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
            target.into(), target_port.into(), source.unwrap_or("").into(), source_port.unwrap_or("").into(),
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

/// Which decl kind an op targets. Used by the rename / remove /
/// update-ports / move-scope dispatch to fail loud if the webview
/// emits a Group-flavored op against a Loop (or vice versa), instead
/// of silently routing through a shared helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerKind {
    Group,
    Loop,
    Node,
}

impl ContainerKind {
    fn op_name(self) -> &'static str {
        match self {
            ContainerKind::Group => "Group",
            ContainerKind::Loop => "Loop",
            ContainerKind::Node => "Node",
        }
    }

    fn matches(self, decl: &Decl) -> bool {
        matches!(
            (self, decl),
            (ContainerKind::Group, Decl::Group(_))
                | (ContainerKind::Loop, Decl::Loop(_))
                | (ContainerKind::Node, Decl::Node(_)),
        )
    }
}

/// Rename a container (Group or Loop). The two cases share the same
/// mechanics: rewrite the header's leading IDENT token, then rewrite
/// every endpoint that resolved to this decl (in any scope). The body
/// and the lowered LoopIn/LoopOut / Passthrough boundary ids are
/// reconstructed on the next re-flatten, so renaming the source-level
/// label is sufficient.
///
/// `id` is the container's SCOPED id (e.g. `Outer.Inner`), the same
/// scoped-id contract `MoveGroupScope` uses, so it is identified
/// unambiguously even when two containers share a local label in
/// different scopes; the old BARE local segment is derived from the
/// resolved decl (endpoint IDENTs hold local segments, not scoped ids).
/// `expected` says which op the caller used so the function fails loud
/// on a kind mismatch (RenameGroup against a Loop, or vice versa).
fn rename_container(
    view: &FileView,
    id: &str,
    new_label: &str,
    expected: ContainerKind,
) -> Result<(), EditError> {
    if new_label.is_empty() {
        return Err(EditError::InvalidArgument("rename to empty label".into()));
    }
    let decl = resolve(view, id)?;
    if !expected.matches(&decl) {
        return Err(kind_mismatch(
            &format!("Rename{}", expected.op_name()),
            id,
            expected.op_name(),
            &decl,
        ));
    }
    let old_local = decl
        .local_id()
        .ok_or_else(|| EditError::ContainerNotFound(id.to_string()))?;
    if old_local == new_label {
        return Ok(());
    }
    // Reject a rename that collides with an existing member of the
    // container's own scope (would manufacture two same-id decls +
    // ambiguous references). The parent scope is everything before the
    // container's last id segment.
    let scoped = view.scoped_id_of(&decl)
        .ok_or_else(|| EditError::ContainerNotFound(id.to_string()))?;
    let parent_scope = scoped.rsplit_once('.').map(|(p, _)| p);
    reject_if_taken(view, parent_scope, new_label)?;
    // Rewrite the header's leading IDENT token.
    let header = match &decl {
        Decl::Group(g) => g.header(),
        Decl::Loop(l) => l.header(),
        _ => None,
    }
    .ok_or_else(|| EditError::ContainerNotFound(id.to_string()))?;
    let id_tok = header
        .syntax()
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == SyntaxKind::IDENT)
        .ok_or_else(|| EditError::ContainerNotFound(id.to_string()))?;
    // Rewrite every reference to the container, in ANY scope, via the same
    // scope-aware query RemoveNode uses (so rename and remove agree on what
    // "references this decl" means). An endpoint resolving to the container
    // has its head IDENT (the old LOCAL label) replaced. Collect the
    // connection handles first (resolve-then-mutate), then rewrite.
    let refs = view.connections_referencing(&decl);
    replace_token_text(&id_tok, new_label);
    for c in refs {
        for ep in c.children().filter(|n| n.kind() == SyntaxKind::ENDPOINT) {
            if let Some(t) = ep
                .children_with_tokens()
                .filter_map(|e| e.into_token())
                .find(|t| t.kind() == SyntaxKind::IDENT && t.text() == old_local.as_str())
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
fn move_scope(
    view: &FileView,
    id: &str,
    target_group: Option<&str>,
    expected: ContainerKind,
) -> Result<(), EditError> {
    let decl = resolve(view, id)?;
    if !expected.matches(&decl) {
        return Err(kind_mismatch(
            &format!("Move{}Scope", expected.op_name()),
            id,
            expected.op_name(),
            &decl,
        ));
    }
    let local = decl.local_id().ok_or_else(|| EditError::InvalidArgument("cannot move an unnamed decl".into()))?;
    let scoped = view.scoped_id_of(&decl);
    // A move into the scope the decl ALREADY lives in is a no-op (the graph view
    // can emit it when a drag ends inside the same parent). Detect it up front and
    // succeed silently: otherwise `reject_if_taken` below would see the decl's own
    // scoped id and wrongly report it as a duplicate of itself.
    let current_parent = scoped
        .as_deref()
        .and_then(|s| s.rsplit_once('.').map(|(parent, _)| parent.to_string()));
    if current_parent.as_deref() == target_group {
        return Ok(());
    }
    // Reject moving a container into itself or its own descendant
    // BEFORE mutating: the detach-then-resolve order below would
    // otherwise fail with a misleading "target not found" (the target
    // detached along with the moved subtree), leaving correctness to
    // rest on `apply_edits` discarding the tree on op failure.
    if let (Some(target), Some(scoped)) = (target_group, scoped.as_deref()) {
        if target == scoped || target.starts_with(&format!("{scoped}.")) {
            return Err(EditError::InvalidArgument(format!(
                "cannot move '{id}' into '{target}': a container cannot move into \
                 itself or its own descendant"
            )));
        }
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
/// `id = Type` + the new signature as its header, preserving the body
/// verbatim. The new signature is the single source of the decl's ports, so
/// connections bound to ports that left the signature are detached first:
/// leaving them would fail validation on the next build (the editor's
/// delete-port gesture relies on the wire dying with the port).
fn update_ports(
    view: &FileView,
    id: &str,
    inputs: &[PortSig],
    outputs: &[PortSig],
    expected: ContainerKind,
) -> Result<(), EditError> {
    let decl = resolve(view, id)?;
    if !expected.matches(&decl) {
        return Err(kind_mismatch(
            &format!("Update{}Ports", expected.op_name()),
            id,
            expected.op_name(),
            &decl,
        ));
    }
    // Match parent-scope legs by the decl's LOCAL id (how endpoints are
    // written in source). None = an anonymous root group (no local name): it
    // can't be named by any parent leg, so the parent-scope sweep is skipped;
    // its `self.<port>` body wiring is still swept.
    detach_dangling_port_connections(&decl, decl.local_id().as_deref(), inputs, outputs);
    let header = decl_header_text(&decl);
    // head = `id = Type` (everything up to the first `(` or `->`).
    let (head, _) = split_header_head(&header);
    let new_header = format!("{}{}", head.trim_end(), build_signature(inputs, outputs));
    rebuild_decl(&decl, &new_header)
}

/// Detach connections bound to ports outside the NEW signature, in the two
/// scopes that can reference the decl: its parent scope (legs naming the
/// decl's local id) and, for a container, its own body (`self.<port>`
/// boundary wiring, where the direction FLIPS: `self` as target writes an
/// OUTPUT port, `self` as source reads an INPUT). Loop-only port surfaces
/// outside the signature survive: the implicit `self.done` (write) /
/// `self.index` (read), and the carry-SYNTHESIZED input side (a carry pairs
/// each listed output with a derived input the lowering creates, so a seed
/// wire `l.acc = ...` or a body read `x.a = self.acc` is valid whenever
/// `acc` is in the carry list AND the new signature keeps the output).
/// Config-origin lines (`n.key = <literal>`) are never touched; an inline-expr
/// RHS (`n.a = Type{...}.out`) IS swept like any other wire when its port left
/// the signature. Both classifications go through the shared
/// `connection_is_config_origin` so the sweep can't drift from the lowering.
fn detach_dangling_port_connections(decl: &Decl, id: Option<&str>, inputs: &[PortSig], outputs: &[PortSig]) {
    let ins: std::collections::HashSet<&str> = inputs.iter().map(|p| p.name.as_str()).collect();
    let outs: std::collections::HashSet<&str> = outputs.iter().map(|p| p.name.as_str()).collect();
    let is_loop = matches!(decl, Decl::Loop(_));
    // Read post-batch state: a `SetLoopConfig carry` earlier in the same op
    // batch already updated the body's carry field, so a dissolved carry's
    // wires sweep and a surviving carry's wires stay.
    let carry = if is_loop { read_carry_list(decl) } else { Default::default() };
    let carry_input_ok = |p: &str| is_loop && carry.contains(p) && outs.contains(p);

    let dangling = |conn: &SyntaxNode, self_side: bool| -> bool {
        // A literal config fill (`n.key = value`) is never a wire and is left
        // untouched: without the catalog the editor cannot tell a fill of a
        // (now-removed) input port from a genuine config KEY that merely shares
        // the name, so sweeping it would risk deleting real config. An inline-
        // expr RHS, despite having one ENDPOINT, IS a wire and must be swept.
        if crate::cst::nodes::connection_is_config_origin(conn, None, None) {
            return false;
        }
        let (t_id, t_port) = endpoint_parts(conn, 0);
        let (s_id, s_port) = endpoint_parts(conn, 1);
        // Parent-scope side needs the decl's local id; an anonymous root has
        // none (the caller skips the parent sweep entirely), so this closure
        // is only ever called with self_side=true in that case.
        let ref_id = if self_side { "self" } else { id.unwrap_or("") };
        let target_bad = t_id.as_deref() == Some(ref_id) && {
            let p = t_port.as_deref().unwrap_or("");
            if self_side {
                !(outs.contains(p) || (is_loop && p == "done"))
            } else {
                !(ins.contains(p) || carry_input_ok(p))
            }
        };
        let source_bad = s_id.as_deref() == Some(ref_id) && {
            let p = s_port.as_deref().unwrap_or("");
            if self_side {
                !(ins.contains(p) || (is_loop && p == "index") || carry_input_ok(p))
            } else {
                !outs.contains(p)
            }
        };
        target_bad || source_bad
    };

    // Parent-scope sweep: only when the decl has a local name a parent leg
    // could reference. An anonymous root group (id None) has no parent legs.
    if id.is_some() {
        if let Some(parent) = decl.syntax().parent() {
            let doomed: Vec<SyntaxNode> = parent
                .children()
                .filter(|n| n.kind() == SyntaxKind::CONNECTION && dangling(n, false))
                .collect();
            for c in doomed {
                detach_with_leading_ws(&c);
            }
        }
    }
    if let Some(body) = decl.body() {
        // Only `self` endpoints: a body child that SHADOWS the container's
        // local id resolves to the child in there, never to the container.
        let doomed: Vec<SyntaxNode> = body
            .syntax()
            .children()
            .filter(|n| n.kind() == SyntaxKind::CONNECTION && dangling(n, true))
            .collect();
        for c in doomed {
            detach_with_leading_ws(&c);
        }
    }
}

/// Rebuild a decl in place with a new header line, preserving its body content
/// verbatim. The decl is re-parsed from text and the WHOLE decl node is spliced
/// over the original. This is the one decl-reconstruction path: it never lifts
/// still-parented elements across trees (which corrupts a `splice_children`),
/// and the result is structurally identical to a freshly-parsed decl.
fn rebuild_decl(decl: &Decl, new_header: &str) -> Result<(), EditError> {
    // The decl's leading whitespace (newline + indent) lives in ONE of two
    // places (the parser is inconsistent): inside a group body it is the decl's
    // PREVIOUS SIBLING token (survives the splice, so the rebuilt text must NOT
    // prepend it or it would DOUBLE it); at file root it is the decl's OWN FIRST
    // CHILD token (replaced by the splice, so the rebuilt text must reprovide the
    // FULL leading WS, newline included, or the new decl glues onto the previous
    // line). Prepend the full leading WS only when it isn't carried by a
    // surviving sibling.
    let lead = if has_leading_ws_sibling(decl.syntax()) {
        String::new()
    } else {
        leading_ws(decl.syntax()).map(|t| t.text().to_string()).unwrap_or_default()
    };
    let rebuilt = match decl.body() {
        Some(body) => format!("{lead}{} {}", new_header.trim(), body.syntax().to_string()),
        None => format!("{lead}{}", new_header.trim()),
    };
    splice_decl(decl, &rebuilt)
}

/// True if `node`'s leading whitespace (newline+indent) is its PREVIOUS SIBLING
/// (group-body decls) rather than its own first child (file-root decls). Mirrors
/// the two-location lookup in `leading_ws`.
fn has_leading_ws_sibling(node: &SyntaxNode) -> bool {
    matches!(node.prev_sibling_or_token(), Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::WHITESPACE)
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
/// SYNC: format_string <-> extension-vscode/src/webview/lib/value-format.ts formatConfigValue (and parseConfigToken, its inverse)
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

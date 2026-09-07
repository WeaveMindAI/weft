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
use crate::cst::nodes::{Body, Decl, Endpoint, FileView, InlineExpr, Resolution, WeftFile};
use crate::cst::parse;

/// Apply one op to the mutable CST `view` (the file root + its source identity,
/// the anon-group id). Mutates in place; returns the op's error if its target
/// cannot be resolved (the batch then aborts and the original source is kept by
/// the caller). The `view` carries `source_id` so every scoped-id resolution
/// uses the SAME anon-group prefix the lowering writes.
/// SYNC: apply_op <-> packages/weft-graph/src/webview/lib/projection/apply.ts applyOp (the editor's optimistic mirror of these op semantics)
pub(super) fn apply_op(view: &FileView, op: &super::EditOp) -> Result<(), EditError> {
    use super::EditOp::*;
    match op {
        SetConfig { node, key, value, form } => set_config(view, node, key, Some(value), *form),
        RemoveConfig { node, key, form } => set_config(view, node, key, None, *form),
        SetLabel { node, label } => set_label(view, node, label.as_deref()),
        AddNode { id, node_type, parent_group } => {
            // The type is written into the source too, so it is validated at the
            // door: a single identifier, and not one of the type names the
            // language reserves (asking `is_reserved_type_keyword`, the one
            // owner of that list, so `Group`/`Loop`/... cannot be authored as a
            // plain node through this door).
            validate_ident("node type", node_type)?;
            if crate::weft_compiler::is_reserved_type_keyword(node_type) {
                return Err(EditError::InvalidArgument(format!(
                    "node type {node_type:?} is a type name the language reserves"
                )));
            }
            add_decl(view, parent_group.as_deref(), id, &format!("{id} = {node_type} {{}}"))
        }
        RemoveNode { node } => remove_node(view, node),
        AddEdge { source, source_port, target, target_port, scope_group, path } => {
            add_edge(view, scope_group.as_deref(), source, source_port, target, target_port, path)
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
        UpdateNodePorts { node, inputs, outputs, removed_inputs, removed_outputs } => {
            update_node_ports(view, node, inputs, outputs, removed_inputs, removed_outputs)
        }
        UpdateGroupPorts { group, inputs, outputs } => update_container_ports(view, group, inputs, outputs, ContainerKind::Group),
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
        UpdateLoopPorts { loop_id, inputs, outputs } => update_container_ports(view, loop_id, inputs, outputs, ContainerKind::Loop),
        SetValueForm { node, key, form } => set_value_form(view, node, key, *form),
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
        Decl::InlineNode(_) => "inline node",
    }
}

/// THE injection guard. Every op that writes a caller-supplied string into the
/// source as a NAME (a node/group/loop id, a node type, a port name, a config
/// key, a rename target) validates it HERE, at the door, before it is
/// interpolated into source text that is then parsed. Without this, a string
/// carrying whitespace, a newline, a brace, or a dot stops being ONE name and
/// becomes extra source: `AddNode { id: "b = Text {}\nevil" }` would splice a
/// second declaration into the file, and so would a port name or a config key.
///
/// The rule is not restated here, it is ASKED of the lexer: the string must lex
/// to exactly ONE token, and that token must be an IDENT. Anything else (a
/// keyword like `Group`/`Loop`, a NUMBER like `true`/`false`, a dotted path, a
/// space, a brace, a newline, a non-ASCII byte) lexes as something other than a
/// single IDENT and is refused. Asking the lexer rather than reimplementing its
/// charset means a future keyword or token rule cannot silently un-guard this
/// door, which a hand-copied `[A-Za-z_][A-Za-z0-9_-]*` would (it wrongly
/// accepts `Group`, `Loop`, `true`, `false`).
///
/// This is the LEXICAL guard only. Whether a name is one the language RESERVES
/// is a separate question with a separate owner: [`validate_local_id`] asks the
/// compiler's `is_reserved_local`, the single source of that rule.
///
/// The sibling guards for the other things written into source are
/// [`validate_port_type`] (a type expression) and `reject_uncontained_value` (a
/// config value).
fn validate_ident(what: &str, s: &str) -> Result<(), EditError> {
    let toks = crate::cst::lexer::lex(s);
    if toks.len() == 1 && toks[0].kind == SyntaxKind::IDENT {
        return Ok(());
    }
    Err(EditError::InvalidArgument(format!(
        "{what} {s:?} is not a valid name: it must be a single identifier (letters, digits, \
         '_' and '-', not starting with a digit, and not a word the language already uses)"
    )))
}

/// A caller-supplied PORT TYPE is written into a decl's signature, which is then
/// reparsed, so it is validated against the language's own definition of a legal
/// type: [`weft_core::WeftType::parse`], the SAME function the compiler's
/// lowering uses. One owner for "what is a legal type".
///
/// This is what a generic containment check cannot do: a bare `[` or an opening
/// heredoc fence is a single UNTERMINATED opaque token, so it balances and
/// carries no newline, yet on reparse it swallows the rest of the file (and a
/// heredoc that closes on a later fence in the user's own source would reparse
/// that source as a value). Only "does this parse as a type" rejects those.
fn validate_port_type(what: &str, ty: &str) -> Result<(), EditError> {
    // The STRUCTURAL gate runs on every spelling, parseable or not: a
    // legal weft type can still be one the header lexer cannot carry
    // (it nests on `[...]` and `{...}` only, so a parenthesized union
    // splits into phantom ports on reparse, verified against the real
    // parser). Parseability decides nothing about that.
    structurally_header_safe(what, ty)?;
    if weft_core::WeftType::parse(ty).is_some() {
        return Ok(());
    }
    // The parser deliberately KEEPS an unknown type in a header (as a
    // recoverable `MustOverride` with a squiggle naming it), so a declared
    // spelling that does not parse must round-trip through here unchanged:
    // refusing it would block every ports gesture on a node whose header
    // holds a typo, and dropping it would erase the author's text. Only a
    // string the header cannot carry is refused, and that was checked
    // above. A colon or a `?` INSIDE the spelling round-trips (the port
    // parser splits on the first colon), so they are allowed; a
    // TRAILING `?` is refused because the port parser refuses the
    // `name: Type?` spelling outright (`?` sits on the name), so the
    // header could never be read back.
    if ty.trim_end().ends_with('?') {
        return Err(EditError::InvalidArgument(format!("{what} {ty:?} is not a valid type")));
    }
    Ok(())
}

/// What the HEADER lexer can carry as one port's type, regardless of
/// whether it parses as a type. It nests on `[...]` and `{...}`: outside
/// them a comma or a paren ends the port (a parenthesized union splits
/// into phantom ports on reparse, verified against the real parser), so
/// a record with any number of fields is one port. A newline, a comment
/// marker, a quote or a backtick would end or swallow the header. An `=`
/// is fine: `Name=Body` is the alias form the catalog itself spells, and
/// the header parser reads it as one type.
fn structurally_header_safe(what: &str, ty: &str) -> Result<(), EditError> {
    let refuse = || Err(EditError::InvalidArgument(format!("{what} {ty:?} is not a valid type")));
    // A stack, not a counter: `[` must close with `]` and `{` with `}`, or
    // the header lexer's balanced scan for the other bracket runs to end
    // of file.
    let mut open: Vec<char> = Vec::new();
    for c in ty.chars() {
        match c {
            '[' | '{' => open.push(c),
            ']' if open.pop() != Some('[') => return refuse(),
            '}' if open.pop() != Some('{') => return refuse(),
            ']' | '}' => {}
            ',' | '(' | ')' if open.is_empty() => return refuse(),
            '\n' | '\r' | '#' | '"' | '`' => return refuse(),
            _ => {}
        }
    }
    if !open.is_empty() || ty.trim().is_empty() {
        return refuse();
    }
    Ok(())
}

/// A caller-supplied LOCAL ID (a node/group/loop id, a rename target): it must
/// lex as one name AND must not be a name the language reserves, or the op
/// would author a file the compiler then refuses. The reserved-name membership
/// rule is NOT restated here: it is `weft_compiler::is_reserved_local`, the
/// single source (the `self` boundary keyword, the reserved type keywords, and
/// any `__`-containing id).
fn validate_local_id(what: &str, s: &str) -> Result<(), EditError> {
    validate_ident(what, s)?;
    if crate::weft_compiler::is_reserved_local(s) {
        return Err(EditError::InvalidArgument(format!(
            "{what} {s:?} is a name the language reserves"
        )));
    }
    Ok(())
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
    // A single-line body stays single-line: `{ x: 1 }` grows to
    // `{ x: 1, v: "2" }` (comma-joined) instead of splicing newline
    // layout mid-line, which would mangle a one-liner (worst on an
    // inline node's body sitting mid-value).
    if body_is_single_line(body) {
        let has_fields = body
            .syntax()
            .children()
            .any(|n| n.kind() == SyntaxKind::CONFIG_FIELD);
        // Own the spacing before `}`: the previous content's trailing
        // space may live INSIDE the last field's subtree, so reach it
        // via prev_token (crosses node boundaries) and detach it.
        if let Some(t) = brace.prev_token() {
            if t.kind() == SyntaxKind::WHITESPACE {
                t.detach();
            }
        }
        let at = body
            .close_brace()
            .map(|b| b.index())
            .ok_or_else(|| EditError::Unparseable("close brace gone after detach".into()))?;
        let mut elems: Vec<SyntaxElement> = if has_fields {
            raw_token_elements(&[(SyntaxKind::COMMA, ","), (SyntaxKind::WHITESPACE, " ")])
        } else {
            raw_token_elements(&[(SyntaxKind::WHITESPACE, " ")])
        };
        elems.extend(elements);
        elems.extend(raw_token_elements(&[(SyntaxKind::WHITESPACE, " ")]));
        body.syntax().splice_children(at..at, elems);
        return Ok(());
    }
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
/// Append `elements` as new lines at the end of the file. A file whose
/// last byte is not a newline (`}` on the final line, no trailing
/// newline) gets one first, so the appended statement never fuses onto
/// that line (`}out.data = ...`).
fn append_to_file(file: &WeftFile, elements: Vec<SyntaxElement>) {
    let count = file.syntax().children_with_tokens().count();
    let text = file.syntax().text().to_string();
    let mut all = Vec::with_capacity(elements.len() + 1);
    if !text.is_empty() && !text.ends_with('\n') {
        all.extend(raw_token_elements(&[(SyntaxKind::WHITESPACE, "\n")]));
    }
    all.extend(elements);
    file.syntax().splice_children(count..count, all);
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

/// Detach a body member by its kind: a CONFIG_FIELD may sit in a
/// comma-separated one-line body and takes its separator with it; any
/// other member (a connection line, a whole decl) has no comma and only
/// carries its leading whitespace. Every removal of something that can
/// be a config field goes through here, so no caller can strand a comma.
fn detach_body_member(node: &SyntaxNode) {
    if node.kind() == SyntaxKind::CONFIG_FIELD {
        detach_field_with_separator(node);
    } else {
        detach_with_leading_ws(node);
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

// ── inline-node extraction (de-inlining) ────────────────────────────────────
//
// Inlining is SOURCE sugar: the graph treats an anon inline node
// (`host__key`) as an ordinary node. Any op whose graph meaning
// conflicts with the inline form therefore DE-INLINES: the expression
// is extracted into a named decl in its own scope (staying inside its
// enclosing group), and the value slot it occupied either becomes an
// explicit wire (`key: name.port`) or is dropped, per the op's meaning.
// Re-rendering a de-inlined but graph-identical program is always
// legal; silently deleting nested nodes, or refusing the op, is not.

/// The node HOLDING an inline expression's value: its parent
/// CONFIG_FIELD (braces form) or CONNECTION (statement form).
fn inline_holder(inline: &InlineExpr) -> Result<SyntaxNode, EditError> {
    let parent = inline
        .syntax()
        .parent()
        .ok_or_else(|| EditError::Unparseable("inline node has no enclosing value".into()))?;
    match parent.kind() {
        SyntaxKind::CONFIG_FIELD | SyntaxKind::CONNECTION => Ok(parent),
        other => Err(EditError::Unparseable(format!(
            "inline node sits under an unexpected {other:?}"
        ))),
    }
}

/// An inline expression split into its DECLARATION text (`Type (sig) {
/// body }`, everything before the trailing `.port`) and that port name.
fn inline_decl_parts(inline: &InlineExpr) -> Result<(String, String), EditError> {
    let elems: Vec<SyntaxElement> = inline.syntax().children_with_tokens().collect();
    let dot = elems
        .iter()
        .rposition(|e| e.kind() == SyntaxKind::DOT)
        .ok_or_else(|| EditError::Unparseable("inline expression has no trailing `.port`".into()))?;
    let port = elems
        .get(dot + 1)
        .and_then(|e| e.as_token().cloned())
        .filter(|t| t.kind() == SyntaxKind::IDENT)
        .map(|t| t.text().to_string())
        .ok_or_else(|| EditError::Unparseable("inline expression has no trailing `.port`".into()))?;
    let rhs: String = elems[..dot].iter().map(|e| e.to_string()).collect();
    Ok((rhs.trim().to_string(), port))
}

/// The scope an inline node's extraction lands in: the nearest enclosing
/// group/loop body (with its content indent) or the file root, plus the
/// container's SCOPED id for fresh-name probing.
#[allow(clippy::type_complexity)]
fn enclosing_scope(
    view: &FileView,
    node: &SyntaxNode,
) -> Result<(Option<(Body, String)>, Option<String>), EditError> {
    for anc in node.ancestors().skip(1) {
        if matches!(anc.kind(), SyntaxKind::GROUP_DECL | SyntaxKind::LOOP_DECL) {
            let decl = Decl::cast(anc.clone())
                .ok_or_else(|| EditError::Unparseable("unreadable enclosing container".into()))?;
            let body = decl
                .body()
                .ok_or_else(|| EditError::Unparseable("enclosing container has no body".into()))?;
            let indent = group_body_indent_decl(&decl);
            let prefix = view.scoped_id_of(&decl);
            return Ok((Some((body, indent)), prefix));
        }
    }
    Ok((None, None))
}

/// A fresh legal local id for an extracted inline node, derived from its
/// anon id (`a__data` -> `a_data`; the `__` separator is reserved), with
/// a numeric suffix until free in the scope.
fn fresh_extract_local(view: &FileView, prefix: Option<&str>, anon_local: &str) -> String {
    let base = anon_local.replace("__", "_");
    let mut name = base.clone();
    let mut n = 1;
    loop {
        let scoped = match prefix {
            Some(p) => format!("{p}.{name}"),
            None => name.clone(),
        };
        if !view.scoped_id_exists(&scoped) && !crate::weft_compiler::is_reserved_local(&name) {
            return name;
        }
        n += 1;
        name = format!("{base}_{n}");
    }
}

/// Extract an inline expression into a named decl in its own scope.
/// `keep_wire` replaces the value slot it occupied with an explicit wire
/// to the extracted node's output (`key: name.port`); `false` drops the
/// holder (field or statement) entirely: the wire is gone, the node
/// survives. Returns the extracted decl's (local id, scoped id).
fn extract_inline(
    view: &FileView,
    inline: &InlineExpr,
    keep_wire: bool,
) -> Result<(String, String), EditError> {
    let holder = inline_holder(inline)?;
    let (rhs, out_port) = inline_decl_parts(inline)?;
    let anon_local = inline
        .anon_local()
        .ok_or_else(|| EditError::Unparseable("inline node has no derivable id".into()))?;
    let (scope, prefix) = enclosing_scope(view, inline.syntax())?;
    let local = fresh_extract_local(view, prefix.as_deref(), &anon_local);
    let scoped = match &prefix {
        Some(p) => format!("{p}.{local}"),
        None => local.clone(),
    };
    let decl_src = format!("{local} = {rhs}");
    match scope {
        Some((body, indent)) => {
            insert_before_close(&body, snippet_elements(&format!("{indent}{decl_src}\n")))?
        }
        None => append_to_file(view.file(), snippet_elements(&format!("\n{decl_src}\n"))),
    }
    if keep_wire {
        let sep = if holder.kind() == SyntaxKind::CONFIG_FIELD {
            SyntaxKind::COLON
        } else {
            SyntaxKind::EQ
        };
        replace_value_after(&holder, sep, &format!("{local}.{out_port}"))?;
    } else {
        detach_body_member(&holder);
    }
    Ok((local, scoped))
}

/// The DIRECT inline-expression children held by a body's config fields
/// and statements (one level; grandchildren stay inline inside their
/// parents' text).
fn direct_inline_children(body: &Body) -> Vec<InlineExpr> {
    body.syntax()
        .children()
        .filter(|c| matches!(c.kind(), SyntaxKind::CONFIG_FIELD | SyntaxKind::CONNECTION))
        .filter_map(|holder| holder.children().find(|n| n.kind() == SyntaxKind::INLINE_EXPR))
        .filter_map(InlineExpr::cast)
        .collect()
}

/// Resolve `id` the way an edge endpoint does: an immediate child of
/// `scope_group` first, else the id as written. The decl-flavored twin
/// of `require_endpoint`'s two probes.
fn resolve_in_scope(view: &FileView, scope_group: Option<&str>, id: &str) -> Option<Decl> {
    if let Some(g) = scope_group {
        if let Ok(gd) = resolve(view, g) {
            if let Some(prefix) = view.scoped_id_of(&gd) {
                let scoped = format!("{prefix}.{id}");
                if view.scoped_id_exists(&scoped) {
                    return resolve(view, &scoped).ok();
                }
            }
        }
    }
    resolve(view, id).ok()
}

/// If an edge-endpoint ref names an INLINE node, extract it (keeping
/// its existing wire) and return the extracted LOCAL id to use in its
/// place; any other ref passes through unchanged.
fn deinline_endpoint(
    view: &FileView,
    scope_group: Option<&str>,
    id: &str,
) -> Result<String, EditError> {
    // `__` is reserved in source identifiers, so only an anon inline id
    // can carry it; anything else can't be an inline and skips the probe.
    if !id.contains("__") {
        return Ok(id.to_string());
    }
    match resolve_in_scope(view, scope_group, id) {
        Some(Decl::InlineNode(inline)) => {
            let (local, _) = extract_inline(view, &inline, true)?;
            Ok(local)
        }
        _ => Ok(id.to_string()),
    }
}

/// If `decl.key`'s current value IS an inline expression (braces field
/// or statement form), extract it as an ORPHAN named node first:
/// overwriting the key with a literal (or removing it) means "replace /
/// drop this input's driver", never "silently delete the driver node
/// and everything nested in it".
fn deinline_value_holder(view: &FileView, decl: &Decl, key: &str) -> Result<(), EditError> {
    if let Some(field) = find_fields(decl, key).into_iter().next() {
        if let Some(inline) = field
            .children()
            .find(|n| n.kind() == SyntaxKind::INLINE_EXPR)
            .and_then(InlineExpr::cast)
        {
            extract_inline(view, &inline, false)?;
            return Ok(());
        }
    }
    if let Some(local) = decl.local_id() {
        let scope = decl
            .syntax()
            .parent()
            .unwrap_or_else(|| view.file().syntax().clone());
        let stmt_inline = scope
            .children()
            .filter(|n| n.kind() == SyntaxKind::CONNECTION)
            .find(|c| {
                let (t_id, t_port) = endpoint_parts(c, 0);
                t_id.as_deref() == Some(local.as_str()) && t_port.as_deref() == Some(key)
            })
            .and_then(|c| c.children().find(|n| n.kind() == SyntaxKind::INLINE_EXPR))
            .and_then(InlineExpr::cast);
        if let Some(inline) = stmt_inline {
            extract_inline(view, &inline, false)?;
        }
    }
    Ok(())
}

// ── ops: add ────────────────────────────────────────────────────────────────

/// Add a node or group decl into a scope. Rejects a duplicate local id loudly.
fn add_decl(view: &FileView, parent_group: Option<&str>, local_id: &str, decl_src: &str) -> Result<(), EditError> {
    validate_local_id("id", local_id)?;
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
    // GRAPH semantics rule: removing a node deletes the node and its
    // wires, and its NEIGHBORS survive. For an inline node that means
    // its nested inline children (nodes feeding its inputs) are
    // extracted as named orphans first, then the expression itself is
    // deleted (the host input is left bare, as if a wire was removed).
    if let Decl::InlineNode(inline) = &decl {
        if let Some(body) = decl.body() {
            for child in direct_inline_children(&body) {
                extract_inline(view, &child, false)?;
            }
        }
        let holder = inline_holder(inline)?;
        detach_body_member(&holder);
        return Ok(());
    }
    // A named node: its inline DRIVERS are neighbor nodes too. Extract
    // each as an orphan (braces-form drivers in the body, statement-form
    // drivers in the scope) before the node and its wires go, and clear
    // braces-endpoint wires (`key: this.port` on other nodes) that
    // would otherwise dangle. Resolve-then-mutate: collect every handle
    // first.
    let body_children: Vec<InlineExpr> =
        decl.body().map(|b| direct_inline_children(&b)).unwrap_or_default();
    let referencing = view.connections_referencing(&decl);
    let endpoint_fields = view.endpoint_fields_referencing(&decl);
    // Endpoint fields FIRST: one may live inside a body inline driver
    // (`a = Sink { x: Wrap { inner: a.value }.out }`), and extraction
    // re-parses the driver from serialized text, orphaning any handle
    // into it. Detached first, the reference simply never appears in
    // the extracted declaration.
    for f in endpoint_fields {
        detach_body_member(&f);
    }
    for child in body_children {
        extract_inline(view, &child, false)?;
    }
    for c in referencing {
        match c
            .children()
            .find(|n| n.kind() == SyntaxKind::INLINE_EXPR)
            .and_then(InlineExpr::cast)
        {
            // A statement-form inline driver (`this.key = X{...}.o`):
            // the extraction detaches the statement itself.
            Some(inline) => {
                extract_inline(view, &inline, false)?;
            }
            None => detach_with_leading_ws(&c),
        }
    }
    detach_with_leading_ws(decl.syntax());
    Ok(())
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
    let before = source_meaning(view);
    let scoped = view.scoped_id_of(&decl).ok_or_else(|| EditError::ContainerNotFound(id.into()))?;
    let body = decl.body().ok_or_else(|| EditError::ContainerNotFound(id.to_string()))?;
    let decl_syntax = decl.syntax();

    // Reuse the scope-aware references for every spelling and every scope.
    // The container's own `self` bindings disappear as well, including ones
    // in a child's braces. Nested containers keep their own `self`.
    let boundary_refs = decl_syntax.descendants().filter(|node| {
        matches!(node.kind(), SyntaxKind::CONNECTION | SyntaxKind::CONFIG_FIELD)
            && connection_is_boundary(node)
            && node.ancestors().find(|ancestor| matches!(ancestor.kind(), SyntaxKind::GROUP_DECL | SyntaxKind::LOOP_DECL))
                .is_some_and(|owner| owner == *decl_syntax)
    });
    let mut references: Vec<_> = view.connections_referencing(&decl).into_iter()
        .chain(view.endpoint_fields_referencing(&decl)).chain(boundary_refs).collect();
    let mut seen = std::collections::HashSet::new();
    references.retain(|reference| seen.insert(reference.clone()));
    // Clear inner references before an enclosing inline is serialized into
    // a named orphan, so serialization cannot revive a deleted wire.
    references.sort_by_key(|reference| std::cmp::Reverse(reference.ancestors().count()));
    for reference in references {
        if reference.parent().is_none() { continue; }
        if let Some(inline) = reference.children().find_map(InlineExpr::cast) {
            extract_inline(view, &inline, false)?;
        } else {
            detach_body_member(&reference);
        }
    }

    let group_indent = leading_indent(decl_syntax);
    let inner_indent = format!("{group_indent}  ");

    let mut moved_src = String::new();
    for child in body.syntax().children() {
        match child.kind() {
            SyntaxKind::NODE_DECL | SyntaxKind::GROUP_DECL | SyntaxKind::LOOP_DECL | SyntaxKind::INCLUDE_DECL | SyntaxKind::TYPE_DECL => {
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
    check_scope_meaning(view, &before, &scoped, None)
}

/// True if a connection INSIDE the group body is boundary wiring, i.e. it has a
/// `self` endpoint (`self.x = ...` / `... = self.x`). A connection is the only
/// internal boundary form; external legs are found by the scope-aware
/// FileView reference queries. We must NOT also drop an inner connection that
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

fn set_config(
    view: &FileView,
    node_id: &str,
    key: &str,
    value: Option<&str>,
    form: Option<super::ValueForm>,
) -> Result<(), EditError> {
    let decl = resolve(view, node_id)?;
    // A container takes values on its INTERFACE PORTS, which is what this
    // op writes. Its loop knobs are a different home and ride
    // `SetLoopConfig` / `RemoveLoopConfig`. An include alias has neither.
    if let Decl::Include(_) = &decl {
        return Err(kind_mismatch("SetConfig/RemoveConfig", node_id, "Node", &decl));
    }
    if matches!(decl, Decl::Group(_) | Decl::Loop(_)) {
        // Inside the braces, the only key a container reads is
        // `_should_flow`: an ordinary key there is a loop knob or an
        // error, never a port value. Every other port is written as a
        // `g.key = value` statement.
        if key != weft_core::exec::skip::SHOULD_FLOW_PORT {
            // Only a DECLARED in-port may be written as `g.key = value`:
            // anything else would emit source the compiler then rejects.
            // Loop knobs (`over`, `parallel`, ...) are not ports and ride
            // `SetLoopConfig`; refuse them here with a pointer. Checked
            // BEFORE the braces-form refusal, so a typo'd key is named a
            // non-port rather than "one of its ports".
            if !header_in_port_names(&decl).iter().any(|p| p == key) {
                return Err(EditError::InvalidArgument(match &decl {
                    Decl::Loop(_) => format!(
                        "'{key}' is not a port of the loop '{node_id}'; loop knobs ride \
                         SetLoopConfig"
                    ),
                    _ => format!("'{key}' is not a port of the group '{node_id}'"),
                }));
            }
            if form == Some(super::ValueForm::Inline) {
                return Err(EditError::InvalidArgument(format!(
                    "'{node_id}' is a container: '{key}' is one of its ports, written `{node_id}.{key} = ...`, and has no braces form"
                )));
            }
            return match (find_connection_origin_field(view, &decl, key), value) {
                (Some(conn), Some(v)) => replace_connection_rhs(&conn, v),
                (Some(conn), None) => { detach_with_leading_ws(&conn); Ok(()) }
                (None, Some(v)) => insert_connection_value(view, node_id, key, v),
                (None, None) => Ok(()),
            };
        }
    }
    // An inline node lives INSIDE a value: its fields are only ever the
    // braces form (no `host__key.field = ...` statement can exist, the
    // `__` id is reserved in source), so route every edit to the body
    // and refuse an explicit statement-form request loudly.
    if let Decl::InlineNode(_) = &decl {
        if form == Some(super::ValueForm::Connection) {
            return Err(EditError::InvalidArgument(format!(
                "'{node_id}' is an inline node: its fields live inside the expression's braces and have no statement form"
            )));
        }
        return match value {
            Some(v) => set_or_insert_field(&decl, key, v),
            None => { remove_field(&decl, key); Ok(()) }
        };
    }
    // If the key's current value is an INLINE EXPRESSION, this write
    // replaces (or drops) that input's driver: extract the inline as an
    // orphan named node first, never silently delete it and its subtree.
    deinline_value_holder(view, &decl, key)?;
    // A same name may legally exist in BOTH forms (a wired-only port's
    // literal next to a same-named config field), so an explicit `form`
    // targets exactly one and never routes to the other.
    match form {
        Some(super::ValueForm::Inline) => {
            return match value {
                Some(v) => set_or_insert_field(&decl, key, v),
                None => { remove_field(&decl, key); Ok(()) }
            };
        }
        Some(super::ValueForm::Connection) => {
            return match (find_connection_origin_field(view, &decl, key), value) {
                (Some(conn), Some(v)) => replace_connection_rhs(&conn, v),
                (Some(conn), None) => { detach_with_leading_ws(&conn); Ok(()) }
                (None, Some(v)) => insert_connection_value(view, node_id, key, v),
                (None, None) => Ok(()),
            };
        }
        None => {}
    }
    // Form-absent auto-routing: a connection-origin field is written
    // `node.key = value` (a CONNECTION), not `key: value` inside the
    // body. If one exists, edit IT (keeping the `node.key = ` form),
    // rather than adding a duplicate body field.
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

/// Insert a fresh `node.key = value` statement line in the node's scope.
/// Shared by the explicit-Connection set path and the form toggle.
fn insert_connection_value(
    view: &FileView,
    node_id: &str,
    key: &str,
    value: &str,
) -> Result<(), EditError> {
    let (scope, local) = match node_id.rsplit_once('.') {
        Some((scope, local)) => (Some(scope), local),
        None => (None, node_id),
    };
    let line = format!("{local}.{key} = {value}");
    match target_body(view, scope)? {
        InsertTarget::FileRoot(f) => {
            append_to_file(&f, snippet_elements(&format!("{line}\n")));
            Ok(())
        }
        InsertTarget::GroupBody { body, indent } => {
            insert_before_close(&body, snippet_elements(&format!("{indent}{line}\n")))
        }
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
    // A `_label` field is only valid on a Node (an inline node included:
    // its label is just a body field). The merged group/loop lowering
    // rejects a label field in a container body as a compile error, so
    // a setLabel targeting a Group/Loop would author an uncompilable
    // file. Fail at edit time with the honest kind error instead
    // (containers are renamed via renameGroup / renameLoop).
    if !matches!(decl, Decl::Node(_) | Decl::InlineNode(_)) {
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
///
/// The KEY is written into the source as an IDENT (the value is separately
/// guarded by `reject_uncontained_value`), so it is validated at the door: an
/// unguarded key would carry structure into the body and inject source.
fn set_or_insert_field(decl: &Decl, key: &str, value: &str) -> Result<(), EditError> {
    validate_ident("config key", key)?;
    let existing = find_fields(decl, key);
    if let Some(first) = existing.first() {
        // Replace only the value tokens (after `:`) on the first match.
        replace_value_after(first, SyntaxKind::COLON, value)?;
        // Detach any duplicates so subsequent reads see a single source of truth.
        // Duplicates can exist when an earlier set_config ran against a stale tree
        // (e.g. a batched op sequence) or after a hand edit. Collapse them here so
        // the tree is self-healing.
        for dup in existing.iter().skip(1) {
            detach_body_member(dup);
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
    // An inline node sits MID-line (`data: Text {...}.out`), so its own
    // leading trivia is not a line indent; the enclosing field/statement
    // holds the line's real column.
    let indent = match decl {
        Decl::InlineNode(inline) => {
            leading_indent(&inline_holder(inline)?)
        }
        _ => leading_indent(decl.syntax()),
    };
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
            let rebuilt = match decl {
                // A bodyless inline (`data: Foo.out`) has no header node;
                // rebuild `Type { field }.port` from its own parts, mid-line
                // (no leading indent), keeping the trailing `.port` so the
                // wire survives.
                Decl::InlineNode(inline) => {
                    let (rhs, port) = inline_decl_parts(inline)?;
                    format!("{rhs} {{\n{body_indent}{key}: {value}\n{indent}}}.{port}")
                }
                _ => {
                    // The decl's ONE leading-whitespace token (see
                    // `leading_ws`) holds both the newlines separating it
                    // from the previous statement and its line indent. The
                    // rebuilt text replaces ONLY the decl node, so trivia the
                    // decl CARRIES as its own first token dies in the splice
                    // and is re-emitted verbatim (or the decl fuses onto the
                    // line above: `...rowsgoogle_access_1 = ...`), while a
                    // token sitting as the decl's PREVIOUS SIBLING survives
                    // the splice already ending with the indent, so nothing
                    // is emitted. Splitting the one token at its last newline
                    // keeps `leading` and the indent complementary by
                    // construction; composing separately-read tokens here
                    // used to double-indent decls nested in a group.
                    let ws = leading_ws(decl.syntax())
                        .map(|t| t.text().to_string())
                        .unwrap_or_default();
                    let carried = decl
                        .syntax()
                        .first_child_or_token()
                        .is_some_and(|t| t.kind() == SyntaxKind::WHITESPACE);
                    let nl = ws.rfind('\n').map_or(0, |i| i + 1);
                    let (leading, ws_indent) = ws.split_at(nl);
                    let prefix =
                        if carried { format!("{leading}{ws_indent}") } else { String::new() };
                    let header = decl_header_text(decl);
                    format!(
                        "{prefix}{} {{\n{body_indent}{key}: {value}\n{ws_indent}}}",
                        header.trim()
                    )
                }
            };
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
        // An inline node has no header node; callers that need its
        // declaration text rebuild it from `inline_decl_parts` (a
        // bodyless `Foo.out` DOES parse as an INLINE_EXPR, so this arm
        // must never feed a rebuild).
        Decl::InlineNode(_) => String::new(),
    }
}

/// Remove a config field by key. Idempotent (no field = no-op). Removes ALL
/// occurrences so accumulated duplicates are cleaned by a single RemoveConfig.
fn remove_field(decl: &Decl, key: &str) {
    for field in find_fields(decl, key) {
        detach_body_member(&field);
    }
}

/// Remove a field together with its LIST SEPARATOR. In a one-line body
/// (`{ a: 1, b: 2 }`) fields are comma-separated, and detaching only the
/// field strands the comma (`{ a: 1,}`, `{ a: 1,, c: 3 }`, `{, b: 2 }`
/// depending on position). Which comma goes: the one before the field,
/// except when a surviving trailing comment sits between them and a
/// comma follows the field, where the one after goes instead (so no
/// separator strands after the comment); a removed FIRST field has no
/// comma before, so the one after goes; and when the removed field was
/// the LAST one, whichever comma remains would dangle before the brace
/// and goes too. In a newline-separated body no comma flanks the field,
/// so only the field and its leading trivia go.
fn detach_field_with_separator(field: &SyntaxNode) {
    // Walk back over the field's leading trivia, deciding what leaves
    // with the field. Whitespace goes; a comment ALONE ON ITS LINE goes
    // too (it describes this field and would orphan onto the next one).
    // A comment trailing a content line (`a: 1 # note`) SURVIVES, and
    // once one survives nothing further is taken: the whitespace run
    // after it holds the newline that terminates it (deleting that
    // newline swallows everything up to `}` into the comment), and the
    // whitespace before it separates it from the value it annotates.
    // The walk still continues past a surviving comment so the
    // separating comma is found wherever it sits.
    let mut taken: Vec<SyntaxToken> = Vec::new();
    let mut pending_ws: Vec<SyntaxToken> = Vec::new();
    let mut comment_survives = false;
    let mut cur = field.prev_sibling_or_token();
    loop {
        match &cur.clone() {
            Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::WHITESPACE => {
                pending_ws.push(t.clone());
                cur = t.prev_sibling_or_token();
            }
            Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::COMMENT => {
                let newline_after = pending_ws.last().is_some_and(|w| w.text().contains('\n'));
                let newline_before = matches!(
                    t.prev_sibling_or_token(),
                    Some(NodeOrToken::Token(w))
                        if w.kind() == SyntaxKind::WHITESPACE && w.text().contains('\n')
                );
                if newline_after && newline_before && !comment_survives {
                    taken.append(&mut pending_ws);
                    taken.push(t.clone());
                } else {
                    pending_ws.clear();
                    comment_survives = true;
                }
                cur = t.prev_sibling_or_token();
            }
            _ => break,
        }
    }
    if !comment_survives {
        taken.append(&mut pending_ws);
    }
    // The separating comma is a sibling token, or (the parser is not
    // consistent here) the LAST token inside the previous field node.
    let comma_before = match &cur {
        Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::COMMA => Some(t.clone()),
        Some(NodeOrToken::Node(n)) => n
            .last_child_or_token()
            .and_then(|e| e.into_token())
            .filter(|t| t.kind() == SyntaxKind::COMMA),
        _ => None,
    };
    // The comma AFTER the field, when one exists (with the whitespace
    // between them). The separator to take when there is none before
    // (the first field), and PREFERRED when a surviving comment sits
    // between the comma before and the field: taking that comma would
    // strand it alone after the comment.
    let mut ws_after: Vec<SyntaxToken> = Vec::new();
    let mut after = field.next_sibling_or_token();
    while let Some(NodeOrToken::Token(t)) = &after {
        if t.kind() != SyntaxKind::WHITESPACE {
            break;
        }
        ws_after.push(t.clone());
        after = t.next_sibling_or_token();
    }
    let comma_after = match &after {
        Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::COMMA => Some(t.clone()),
        _ => None,
    };

    match (comma_before, &comma_after) {
        (Some(comma), after) if !(comment_survives && after.is_some()) => {
            for w in taken {
                w.detach();
            }
            // Same-line whitespace hugging the comma from its left goes with
            // it (`{ a: 1 , b }` minus `b` must not leave a double space),
            // unless a surviving comment sits between comma and field: that
            // whitespace then separates the previous value from the comment.
            if !comment_survives {
                if let Some(NodeOrToken::Token(w)) = comma.prev_sibling_or_token() {
                    if w.kind() == SyntaxKind::WHITESPACE && !w.text().contains('\n') {
                        w.detach();
                    }
                }
            }
            // A trailing comma that would dangle once this LAST field leaves
            // (`{ a, b, }` minus `b`) goes too, with the space before it.
            detach_dangling_trailing_comma(field);
            comma.detach();
            detach_field_leaving_brace_ws(field);
        }
        (before, Some(comma)) => {
            // Take the comma after (the first field, or the surviving-
            // comment preference above): `{ a: 1, b: 2 }` minus `a`
            // leaves `{ b: 2 }`. A plain detach: the whitespace after
            // the taken comma becomes the next field's lead, so no
            // brace-space splice applies (splicing here doubled the
            // space when the field carried its own trailing run).
            //
            // When the removed field was the LAST one, the comma taken
            // here was the body's dangling trailing comma, and the
            // separator BEFORE the field (kept for the surviving
            // comment's sake) would now dangle in its place; it must
            // go too. Decided before anything detaches.
            let field_was_last = {
                let mut nxt = comma.next_sibling_or_token();
                loop {
                    match &nxt {
                        Some(NodeOrToken::Token(t))
                            if matches!(t.kind(), SyntaxKind::WHITESPACE | SyntaxKind::COMMENT) =>
                        {
                            nxt = t.next_sibling_or_token();
                        }
                        Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::R_BRACE => break true,
                        None => break true,
                        _ => break false,
                    }
                }
            };
            for w in ws_after {
                w.detach();
            }
            // When the field's own leading run STAYS (a surviving
            // comment kept it), the same-line whitespace on the comma's
            // far side would double against it, so it leaves too.
            if comment_survives {
                if let Some(NodeOrToken::Token(w)) = comma.next_sibling_or_token() {
                    if w.kind() == SyntaxKind::WHITESPACE && !w.text().contains('\n') {
                        w.detach();
                    }
                }
            }
            comma.detach();
            for t in taken {
                t.detach();
            }
            if field_was_last {
                if let Some(before) = before {
                    // The same-line whitespace hugging the now-dangling
                    // separator goes with it (`"x" ,` must not become
                    // `"x" ` with a double space against what follows).
                    if let Some(NodeOrToken::Token(w)) = before.prev_sibling_or_token() {
                        if w.kind() == SyntaxKind::WHITESPACE && !w.text().contains('\n') {
                            w.detach();
                        }
                    }
                    before.detach();
                }
            }
            field.detach();
        }
        _ => {
            // No comma either side: a newline-separated or single-field
            // body; the field leaves with its leading trivia only.
            for t in taken {
                t.detach();
            }
            detach_field_leaving_brace_ws(field);
        }
    }
}

/// Detach `field`, leaving behind the whitespace it carried before the
/// closing brace. The LAST field of a one-line body holds `}`'s layout
/// space INSIDE itself (`size: 3 ` in `{ a: 1, size: 3 }`), so a plain
/// detach would glue the previous value onto the brace. Skipped when
/// what now precedes the field already ends in whitespace (`{ a: 1 , b }`:
/// the space lives inside the previous field), where the splice would
/// double it.
fn detach_field_leaving_brace_ws(field: &SyntaxNode) {
    let prev_ends_in_ws = match field.prev_sibling_or_token() {
        Some(NodeOrToken::Token(t)) => t.kind() == SyntaxKind::WHITESPACE,
        Some(NodeOrToken::Node(n)) => n
            .last_child_or_token()
            .and_then(|e| e.into_token())
            .is_some_and(|t| t.kind() == SyntaxKind::WHITESPACE),
        None => false,
    };
    let trailing_ws = field
        .last_child_or_token()
        .and_then(|e| e.into_token())
        .filter(|t| t.kind() == SyntaxKind::WHITESPACE)
        .filter(|_| !prev_ends_in_ws)
        .map(|t| t.text().to_string());
    match (trailing_ws, field.parent()) {
        (Some(ws), Some(parent)) => {
            let idx = field.index();
            parent
                .splice_children(idx..idx + 1, raw_token_elements(&[(SyntaxKind::WHITESPACE, &ws)]));
        }
        _ => field.detach(),
    }
}

/// Detach the comma AFTER `field` when nothing but the closing brace
/// follows it: once the field leaves, that trailing comma separates
/// nothing (`{ a, b, }` minus `b` must end `{ a }`, not `{ a, }`).
/// Same-line whitespace between field and comma goes with it.
fn detach_dangling_trailing_comma(field: &SyntaxNode) {
    let mut cur = field.next_sibling_or_token();
    let mut ws: Vec<SyntaxToken> = Vec::new();
    while let Some(NodeOrToken::Token(t)) = &cur {
        if t.kind() != SyntaxKind::WHITESPACE {
            break;
        }
        ws.push(t.clone());
        cur = t.next_sibling_or_token();
    }
    let Some(NodeOrToken::Token(comma)) = cur else { return };
    if comma.kind() != SyntaxKind::COMMA {
        return;
    }
    // Only when the brace is all that follows: a comma with another
    // field after it is that field's own separator.
    let mut after = comma.next_sibling_or_token();
    while let Some(NodeOrToken::Token(t)) = &after {
        if t.kind() != SyntaxKind::WHITESPACE {
            break;
        }
        after = t.next_sibling_or_token();
    }
    match after {
        Some(NodeOrToken::Token(t)) if t.kind() == SyntaxKind::R_BRACE => {}
        None => {}
        _ => return,
    }
    for w in ws {
        w.detach();
    }
    comma.detach();
}

/// The verbatim VALUE text of a field/connection node: the token run
/// after `sep`, minus a trailing same-line comment and trailing
/// whitespace. The read twin of `replace_value_after`'s run
/// computation, so a form move carries exactly what a replace would
/// have replaced.
fn value_text_after(node: &SyntaxNode, sep: SyntaxKind) -> Result<String, EditError> {
    let elems: Vec<SyntaxElement> = node.children_with_tokens().collect();
    let sep_idx = elems
        .iter()
        .position(|e| e.kind() == sep)
        .ok_or_else(|| EditError::Unparseable(format!("field has no `{sep:?}` separator")))?;
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
    while value_end > value_start && elems[value_end - 1].kind() == SyntaxKind::WHITESPACE {
        value_end -= 1;
    }
    let text: String = elems[value_start..value_end].iter().map(|e| e.to_string()).collect();
    let text = text.trim().to_string();
    if text.is_empty() {
        return Err(EditError::Unparseable("field has no value to move".into()));
    }
    Ok(text)
}

/// Rewrite which SOURCE FORM a node's body-set port value is written in.
/// `Inline`: a `node.key = value` statement collapses into a
/// `key: value` braces field. `Connection`: a braces field moves out to
/// a statement line in the node's scope. The value text moves verbatim;
/// already in the requested form is a no-op; no body value at all is a
/// loud error.
fn set_value_form(
    view: &FileView,
    node_id: &str,
    key: &str,
    form: super::ValueForm,
) -> Result<(), EditError> {
    let decl = resolve(view, node_id)?;
    if !matches!(decl, Decl::Node(_)) {
        return Err(kind_mismatch("SetValueForm", node_id, "Node", &decl));
    }
    validate_ident("config key", key)?;
    let conn = find_connection_origin_field(view, &decl, key);
    let field = find_fields(&decl, key).into_iter().next();
    match form {
        super::ValueForm::Inline => {
            let Some(conn) = conn else {
                return if field.is_some() {
                    Ok(())
                } else {
                    Err(EditError::Unparseable(format!(
                        "'{node_id}.{key}' has no body value to move"
                    )))
                };
            };
            let value = value_text_after(&conn, SyntaxKind::EQ)?;
            detach_with_leading_ws(&conn);
            set_or_insert_field(&decl, key, &value)
        }
        super::ValueForm::Connection => {
            if conn.is_some() {
                return Ok(());
            }
            let Some(field) = field else {
                return Err(EditError::Unparseable(format!(
                    "'{node_id}.{key}' has no body value to move"
                )));
            };
            let value = value_text_after(&field, SyntaxKind::COLON)?;
            remove_field(&decl, key);
            insert_connection_value(view, node_id, key, &value)
        }
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

/// The real `GROUP_DESC` node for a description line `# {d}`, parsed in group
/// context so it is a promoted GROUP_DESC (not a bare COMMENT token). Splicing
/// this, rather than raw comment elements, keeps `group.description()` truthful
/// across ops applied to ONE tree in a batch (a later clear/re-set in the same
/// batch finds it). The wrapper trick mirrors `snippet_elements_as_body_content`.
///
/// The caller has already rejected any newline in `d`, so `# {d}` is exactly
/// one comment line and the group body has exactly one first-line comment to
/// promote.
fn group_desc_element(d: &str) -> Result<SyntaxElement, EditError> {
    let wrapper_src = format!("__edit_desc_wrap = Group() {{\n# {d}\n}}\n");
    let root = parse(&wrapper_src).clone_for_update();
    let node = WeftFile::cast(root)
        .and_then(|f| {
            f.syntax()
                .descendants()
                .find(|n| n.kind() == SyntaxKind::GROUP_DESC)
        })
        .ok_or_else(|| EditError::Unparseable("could not synthesize a group description".into()))?;
    node.detach();
    Ok(NodeOrToken::Node(node))
}

/// Set/replace/remove a group's description: the plain `# ...` comment on
/// its first body line. A description is a single line by construction, so a
/// value carrying a newline (which would parse as extra source: injected
/// nodes, connections) is refused loudly. Whitespace-only clears it.
fn set_group_description(view: &FileView, group_id: &str, desc: Option<&str>) -> Result<(), EditError> {
    let desc = desc.map(str::trim).filter(|d| !d.is_empty());
    if let Some(d) = desc {
        if d.contains('\n') || d.contains('\r') {
            return Err(EditError::InvalidArgument(
                "group description must be a single line (no newline)".into(),
            ));
        }
    }
    let group = resolve_group(view, group_id)?;
    let body = group.body().ok_or_else(|| EditError::ContainerNotFound(group_id.to_string()))?;
    let indent = format!("{}  ", leading_indent(group.syntax()));
    let existing = group.description().map(|d| d.syntax().clone());
    match (existing, desc) {
        (Some(node), Some(d)) => {
            let idx = node.index();
            node.parent().unwrap().splice_children(idx..idx + 1, vec![group_desc_element(d)?]);
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
            // comment first-line would SWALLOW it (`# d}` is one
            // comment). Append a newline + the group's own indent after the
            // description so the `}` drops to its own line. A multi-line body
            // already has structure. `body_owner_indent` is the ONE definition of
            // "the column a body's close brace sits at" (also used by
            // `insert_before_close`), so the two never drift.
            let group_indent = body_owner_indent(&body);
            // Splice the REAL GROUP_DESC node (not a bare comment) with its
            // layout trivia, so `group.description()` recognizes it later in
            // the same batch. Single-line bodies get a trailing newline+indent
            // so the `}` drops to its own line (a comment would otherwise
            // swallow it: `# d}` is one comment).
            let mut elements =
                vec![make_token(SyntaxKind::WHITESPACE, &format!("\n{indent}")), group_desc_element(d)?];
            if body_is_single_line(&body) {
                elements.push(make_token(SyntaxKind::WHITESPACE, &format!("\n{group_indent}")));
            }
            body.syntax().splice_children(at..at, elements);
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
    path: &[String],
) -> Result<(), EditError> {
    // An endpoint naming an INLINE node (fan-out from its output, a wire
    // into one of its inputs) de-inlines it first, keeping its existing
    // wire, and the edge then connects to the extracted named node.
    let source = deinline_endpoint(view, scope_group, source)?;
    let target = deinline_endpoint(view, scope_group, target)?;
    let (source, target) = (source.as_str(), target.as_str());
    // Validate the scope even when an in-place rewire needs no insertion.
    let insertion_target = target_body(view, scope_group)?;
    // Both endpoints must exist (or be `self`). Refs are SCOPE-LOCAL: `x`
    // inside scope `G` means `G.x`, not a file-wide `x`.
    require_endpoint(view, scope_group, source)?;
    require_endpoint(view, scope_group, target)?;
    // The port names are written into the source as IDENTs, exactly like the
    // node ids, so they are validated at the door too: an unguarded port name
    // would carry structure into the connection line and inject source.
    validate_ident("source port", source_port)?;
    validate_ident("target port", target_port)?;
    for key in path {
        validate_ident("path key", key)?;
    }
    let mut rhs = format!("{source}.{source_port}");
    for key in path {
        rhs.push('.');
        rhs.push_str(key);
    }
    // A port already driven by a plain wire keeps the spelling it was
    // written in: the statement's right-hand side, or the braces
    // field's value, is swapped in place (a rewire, a key read off the
    // same source). Anything else (an inline driver, an unwired port)
    // clears the driver and writes a fresh statement.
    let is_plain_wire = |n: &SyntaxNode| {
        n.children().any(|c| c.kind() == SyntaxKind::ENDPOINT)
            && !n.children().any(|c| c.kind() == SyntaxKind::INLINE_EXPR)
    };
    let literal_driver_error = || EditError::InvalidArgument(format!(
        "cannot wire '{target}.{target_port}': it has an explicit literal; clear the value first"
    ));
    if let Ok(conn) = find_connection(view, scope_group, target, target_port, None, None) {
        if crate::cst::nodes::connection_is_config_origin(&conn, None, None) {
            return Err(literal_driver_error());
        }
        if is_plain_wire(&conn) {
            return replace_connection_rhs(&conn, &rhs);
        }
    }
    if let Some(decl) = resolve_in_scope(view, scope_group, target) {
        for field in find_fields(&decl, target_port) {
            if is_plain_wire(&field) {
                return replace_value_after(&field, SyntaxKind::COLON, &rhs);
            }
            if !field.children().any(|child| child.kind() == SyntaxKind::INLINE_EXPR) {
                return Err(literal_driver_error());
            }
        }
    }
    // Remove the existing driver of this target port in the same scope.
    remove_driver(view, scope_group, target, target_port)?;
    let conn = format!("{target}.{target_port} = {rhs}");
    match insertion_target {
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
/// rescope_endpoint, crates/weft-compiler/src/cst/nodes.rs endpoint_resolves_to,
/// packages/weft-graph/src/webview/lib/projection/apply.ts resolveEndpoint
fn require_endpoint(view: &FileView, scope_group: Option<&str>, id: &str) -> Result<(), EditError> {
    // An anon inline-node id (`host__key`; `__` is reserved in source
    // identifiers, so nothing else carries it) cannot be an endpoint:
    // an inline node lives inside a value and has no wireable identity
    // in source. Refuse with the fix instead of authoring an id the
    // compiler then rejects.
    if id.contains("__") {
        return Err(EditError::InvalidArgument(format!(
            "'{id}' is an inline node: it lives inside a value and cannot be wired. Declare it as a named node to connect it"
        )));
    }
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

/// Drop whatever currently drives `target.target_port` in the scope: a
/// statement-form connection (plain edge or inline expression), or a
/// braces-form value on the target's body (an inline expression or an
/// endpoint wire). An inline driver is EXTRACTED as an orphan named
/// node, never silently deleted; a plain braces literal is left alone
/// (the webview vetoes docking onto literal-driven inputs and the
/// compile rejects double drivers).
fn remove_driver(
    view: &FileView,
    scope_group: Option<&str>,
    target: &str,
    target_port: &str,
) -> Result<(), EditError> {
    if let Ok(conn) = find_connection(view, scope_group, target, target_port, None, None) {
        match conn
            .children()
            .find(|n| n.kind() == SyntaxKind::INLINE_EXPR)
            .and_then(InlineExpr::cast)
        {
            Some(inline) => {
                extract_inline(view, &inline, false)?;
            }
            None => detach_with_leading_ws(&conn),
        }
        return Ok(());
    }
    let Some(decl) = resolve_in_scope(view, scope_group, target) else { return Ok(()) };
    detach_body_driver(view, &decl, target_port)
}

/// Detach the BODY field driving `key` on `decl`, if it is a wire: an
/// inline-expression driver is extracted to a named decl (node kept, wire
/// dropped), an endpoint driver's line is removed. A LITERAL fill is not a
/// wire and is left alone. Shared by the driver removal and the node
/// port-removal sweep.
fn detach_body_driver(view: &FileView, decl: &Decl, key: &str) -> Result<(), EditError> {
    // ALL same-key fields, not just the first: duplicates accumulate (a
    // batched op sequence, a hand edit; `set_or_insert_field` heals them
    // on write, `remove_field` sweeps them all), and a surviving second
    // driver re-creates the port on the next parse.
    for field in find_fields(decl, key) {
        if let Some(inline) = field
            .children()
            .find(|n| n.kind() == SyntaxKind::INLINE_EXPR)
            .and_then(InlineExpr::cast)
        {
            extract_inline(view, &inline, false)?;
        } else if field.children().any(|n| n.kind() == SyntaxKind::ENDPOINT) {
            detach_body_member(&field);
        }
    }
    Ok(())
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
    if let Ok(conn) = find_connection(view, scope_group, target, target_port, Some(source), Some(source_port)) {
        detach_with_leading_ws(&conn);
        return Ok(());
    }
    // Value-embedded wires: `key: src.port` (braces endpoint),
    // `key: Src {...}.port` (braces inline), `t.p = Src {...}.port`
    // (statement inline). Each IS an edge in the compiled graph, so
    // removing it must work from the graph. Unlinking an inline
    // EXTRACTS it as an orphan named node (the wire is what the user
    // removed, not the node).
    let source_local = source.rsplit('.').next().unwrap_or(source);
    if let Some(decl) = resolve_in_scope(view, scope_group, target) {
        if let Some(field) = find_fields(&decl, target_port).into_iter().next() {
            if let Some(inline) = field
                .children()
                .find(|n| n.kind() == SyntaxKind::INLINE_EXPR)
                .and_then(InlineExpr::cast)
            {
                if inline.anon_local().as_deref() == Some(source_local) {
                    extract_inline(view, &inline, false)?;
                    return Ok(());
                }
            }
            let endpoint_matches = field
                .children()
                .find(|n| n.kind() == SyntaxKind::ENDPOINT)
                .and_then(crate::cst::nodes::Endpoint::cast)
                .map(|ep| ep.parts())
                .is_some_and(|(id, port)| {
                    id.as_deref() == Some(source_local) && port.as_deref() == Some(source_port)
                });
            if endpoint_matches {
                detach_body_member(&field);
                return Ok(());
            }
        }
    }
    if let Ok(scope) = scope_body(view, scope_group) {
        let stmt_inline = scope
            .children()
            .filter(|n| n.kind() == SyntaxKind::CONNECTION)
            .find(|c| {
                let (t_id, t_port) = endpoint_parts(c, 0);
                t_id.as_deref() == Some(target) && t_port.as_deref() == Some(target_port)
            })
            .and_then(|c| c.children().find(|n| n.kind() == SyntaxKind::INLINE_EXPR))
            .and_then(InlineExpr::cast)
            .filter(|inline| inline.anon_local().as_deref() == Some(source_local));
        if let Some(inline) = stmt_inline {
            extract_inline(view, &inline, false)?;
            return Ok(());
        }
    }
    Err(EditError::ConnectionNotFound(target.into(), target_port.into(), source.into(), source_port.into()))
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
    // The new label is written into the source as an IDENT token, so it is
    // validated at the door (this also covers the previous empty-label check).
    validate_local_id("new label", new_label)?;
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
    let refs: Vec<_> = view.connections_referencing(&decl).into_iter()
        .chain(view.endpoint_fields_referencing(&decl)).collect();
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

/// Reject a config-value string that would BREAK CONTAINMENT: once its lexed
/// tokens are spliced into value position, a token that reaches FORWARD past
/// itself (a line comment to the next newline, an unterminated opaque token to
/// end-of-input, an unbalanced closer) consumes the `}` that ends the field and
/// swallows the rest of the file. The check refuses exactly those forward-
/// reaching tokens.
///
/// Whether an opaque token (a `[`-array, a heredoc, a string, a `@marker`) is
/// terminated is ASKED OF THE LEXER (`*_is_closed`), using its own scan, never
/// re-derived here: the two must agree about closed-ness or a token this gate
/// calls contained would re-lex greedily and reach forward anyway.
///
/// The ONE containment gate, shared by both value-writing paths (in-place
/// replace via `value_elements`, and insert/synthesize-body via `insert_field`),
/// so a value the replace path rejects can't slip through the insert path.
fn reject_uncontained_value(value: &str) -> Result<(), EditError> {
    let bad = || {
        Err(EditError::InvalidArgument(format!(
            "value would break out of its field (it does not parse as a single contained \
             value): {value:?}"
        )))
    };
    // The value is spliced (as its lexed tokens) into value position, right
    // before whatever terminates the field: a `}` on the same line for a
    // single-line body, a newline then `}` for a multi-line one. It is safe iff
    // none of its tokens REACH FORWARD past itself to consume that terminator.
    // Exactly the tokens that reach forward are refused, on the lexed value:
    let toks = crate::cst::lexer::lex(value);
    let mut depth: i32 = 0;
    // Brace nesting specifically: a newline INSIDE a balanced `{...}` run is
    // part of a multi-line JSON object value, which the grammar parses in
    // every value position (one-liner bodies and connection lines included:
    // the parser assembles the brace-run across lines; pinned by
    // tests/parser_multiline_object.rs). Only a newline at brace depth 0
    // splits the VALUE itself across lines and breaks out.
    let mut brace_depth: i32 = 0;
    for t in &toks {
        match t.kind {
            // A line comment runs to the next newline, so on a single-line body
            // it eats the `}`. Unsafe wherever it appears in a value.
            SyntaxKind::COMMENT => return bad(),
            // A raw newline OUTSIDE any brace-run splits the value onto a new
            // line. (Inside a brace-run it is multi-line JSON, contained; a
            // heredoc's own newlines live inside its single HEREDOC token, so
            // they never surface as a WHITESPACE token here.)
            SyntaxKind::WHITESPACE if t.text.contains('\n') && brace_depth == 0 => return bad(),
            // An UNTERMINATED opaque token runs to end-of-input, swallowing the
            // rest of the file: a `[`-array missing its `]`, a heredoc/string
            // missing its closing fence/quote, a marker missing its `)`. A
            // terminated one is contained.
            //
            // For a JSON_VALUE / MARKER, endpoint-matching is NOT enough: the
            // lexer's scan is greedy and pads an unbalanced token (`[[]`,
            // `@file("a)`) out to EOF, so it can still end in `]` / `)`. Ask the
            // LEXER whether the token actually closed, using its own scan, so
            // this can never disagree with how the token re-lexes.
            SyntaxKind::JSON_VALUE => {
                if !crate::cst::lexer::json_value_is_closed(t.text) {
                    return bad();
                }
            }
            SyntaxKind::MARKER => {
                if !crate::cst::lexer::marker_is_closed(t.text) {
                    return bad();
                }
            }
            SyntaxKind::HEREDOC => {
                if !crate::cst::lexer::heredoc_is_closed(t.text) {
                    return bad();
                }
            }
            SyntaxKind::STRING => {
                if !crate::cst::lexer::string_is_closed(t.text) {
                    return bad();
                }
            }
            // A closer with no opener inside the value would close the enclosing
            // body/sig early. (`[...]` arrays are single opaque tokens above;
            // `{...}` objects lex as raw brace runs, tracked here.)
            SyntaxKind::L_BRACE => {
                depth += 1;
                brace_depth += 1;
            }
            SyntaxKind::L_PAREN => depth += 1,
            SyntaxKind::R_BRACE => {
                depth -= 1;
                brace_depth -= 1;
                if depth < 0 {
                    return bad();
                }
            }
            SyntaxKind::R_PAREN => {
                depth -= 1;
                if depth < 0 {
                    return bad();
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return bad();
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

/// Use the compiler's actual name/type resolution for scope edits. No file I/O
/// is needed; unresolved external files remain the same diagnostics on both
/// sides. Reimplementing an alias resolver in the editor would drift.
fn source_meaning(view: &FileView) -> (weft_core::project::ProjectDefinition, Vec<crate::weft_compiler::CompileError>) {
    crate::weft_compiler::compile_lenient(
        &view.file().syntax().to_string(), uuid::Uuid::nil(),
        crate::file_reader::CompileFs::none(), crate::weft_compiler::IncludeMode::Full,
        Some(view.source_id()),
    )
}

fn check_scope_meaning(
    view: &FileView,
    before: &(weft_core::project::ProjectDefinition, Vec<crate::weft_compiler::CompileError>),
    from: &str,
    to: Option<&str>,
) -> Result<(), EditError> {
    let after = source_meaning(view);
    let mut prior_errors = std::collections::HashMap::<&str, usize>::new();
    for error in &before.1 { *prior_errors.entry(&error.message).or_default() += 1; }
    for error in &after.1 {
        if let Some(count) = prior_errors.get_mut(error.message.as_str()) {
            if *count > 0 { *count -= 1; continue; }
        }
        return Err(EditError::InvalidArgument(format!(
            "scope change would make the source invalid: {}", error.message
        )));
    }
    let after_nodes: std::collections::HashMap<_, _> = after.0.nodes.iter().map(|node| (node.id.as_str(), node)).collect();
    let parent = from.rsplit_once('.').map(|(parent, _)| parent).unwrap_or("");
    for node in &before.0.nodes {
        let next_id = match node.id.strip_prefix(from) {
            Some(suffix) if suffix.is_empty() || suffix.starts_with('.') || suffix.starts_with("__") => {
                match to {
                    Some(target) => format!("{target}{suffix}"),
                    None if suffix.starts_with('.') => {
                        if parent.is_empty() { suffix[1..].to_string() } else { format!("{parent}{suffix}") }
                    }
                    None => continue, // the removed container's own boundary nodes
                }
            }
            _ => node.id.clone(),
        };
        let Some(next) = after_nodes.get(next_id.as_str()) else {
            // Extracting an anonymous inline changes its generated identity.
            // Named children must survive a move or an ungroup unchanged.
            if node.id.contains("__") { continue; }
            return Err(EditError::InvalidArgument(format!("scope change would lose node '{}'", node.id)));
        };
        let next_ports: std::collections::HashMap<_, _> = port_contracts(next)
            .map(|(side, port)| ((side, port.name.as_str()), port)).collect();
        for (side, port) in port_contracts(node) {
                if port.port_type == weft_core::WeftType::MustOverride { continue; }
                if !next_ports.get(&(side, port.name.as_str()))
                    .is_some_and(|new| new.port_type == port.port_type && new.required == port.required) {
                    return Err(EditError::InvalidArgument(format!(
                        "scope change would change {side} '{}.{}' ({}); keep its type declaration in scope",
                        node.id, port.name, port.port_type.wire_string(),
                    )));
                }
        }
        let marker_counts = |definition: &weft_core::project::NodeDefinition| {
            let mut counts = std::collections::HashMap::<String, usize>::new();
            for value in definition.config.as_object().into_iter().flat_map(|map| map.values())
                .chain(definition.port_literals.values()) {
                for reference in crate::file_ref::refs_in_value(value) {
                    *counts.entry(reference.resolution_key()).or_default() += 1;
                }
            }
            counts
        };
        let next_refs = marker_counts(next);
        for (reference, count) in marker_counts(node) {
            if next_refs.get(&reference).copied().unwrap_or(0) < count {
                return Err(EditError::InvalidArgument(format!(
                    "scope change would change a file reference's declared type on '{}'; keep its type declaration in scope", node.id,
                )));
            }
        }
    }
    Ok(())
}

fn port_contracts(node: &weft_core::project::NodeDefinition)
    -> impl Iterator<Item = (&'static str, &weft_core::project::PortDefinition)> {
    node.inputs.iter().map(|input| ("input", &input.port))
        .chain(node.outputs.iter().map(|output| ("output", output)))
}

/// Move a node/group into a target scope (None = file root). Detach the decl's
/// subtree (as text, re-indented) and re-insert it without changing its bindings.
fn move_scope(
    view: &FileView,
    id: &str,
    target_group: Option<&str>,
    expected: ContainerKind,
) -> Result<(), EditError> {
    let decl = resolve(view, id)?;
    // Moving an INLINE node: de-inline (wire kept) and move the named
    // node; the same cross-scope wiring rules then apply to it. A move
    // into its own current scope is the drag-ended-in-place no-op and
    // must not restructure the source.
    if let (ContainerKind::Node, Decl::InlineNode(inline)) = (expected, &decl) {
        let (_, prefix) = enclosing_scope(view, inline.syntax())?;
        if prefix.as_deref() == target_group {
            return Ok(());
        }
        let (_, scoped) = extract_inline(view, inline, true)?;
        return move_scope(view, &scoped, target_group, expected);
    }
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

    let before = source_meaning(view);
    let previous_id = scoped.as_deref().ok_or_else(|| EditError::NodeNotFound(id.into()))?;
    let next_id = target_group.map(|parent| format!("{parent}.{local}")).unwrap_or_else(|| local.clone());

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
        .chain(view.endpoint_fields_referencing(&decl))
        .collect();
    // A plain node also owns implicit-target wires in its braces (including
    // nested inline nodes). Containers carry their body wiring with them.
    let incoming = if matches!(decl, Decl::Node(_)) {
        decl.body().map(|body| body.syntax().descendants().filter(|node| {
            match node.kind() {
                SyntaxKind::CONFIG_FIELD => node.children().any(|n| n.kind() == SyntaxKind::ENDPOINT),
                SyntaxKind::CONNECTION => node.children().filter(|n| n.kind() == SyntaxKind::ENDPOINT).count() > 1,
                _ => false,
            }
        }).count()).unwrap_or(0)
    } else { 0 };
    if !blocking.is_empty() || incoming != 0 {
        return Err(EditError::InvalidArgument(format!(
            "cannot move '{id}': it is wired by {} connection(s) that would cross the scope boundary; disconnect them first",
            blocking.len() + incoming
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
        detach_body_member(f);
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
        }
        InsertTarget::GroupBody { body, indent } => {
            let reindented = indent_block(&combined, &indent);
            insert_before_close(&body, snippet_elements(&format!("{reindented}\n")))?;
        }
    }
    check_scope_meaning(view, &before, previous_id, Some(&next_id))
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

/// Rewrite a GROUP/LOOP's COMPLETE port signature: rebuild the decl with
/// `id = Type` + the new signature as its header, preserving the body
/// verbatim. For a container the signature is the single source of its
/// ports, so connections bound to ports that left the signature are
/// detached first: leaving them would fail validation on the next build
/// (the editor's delete-port gesture relies on the wire dying with the
/// port). Nodes go through [`update_node_ports`], where the header is
/// NOT the whole surface.
fn update_container_ports(
    view: &FileView,
    id: &str,
    inputs: &[PortSig],
    outputs: &[PortSig],
    expected: ContainerKind,
) -> Result<(), EditError> {
    validate_port_sigs(inputs, outputs)?;
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
    rewrite_port_header(&decl, inputs, outputs)
}

/// Rewrite a NODE's DECLARED port surface (see `EditOp::UpdateNodePorts`):
/// the header lists only the custom/overridden ports; the catalog provides
/// the node type's own ports at enrich, which this edit layer cannot see.
/// So no signature diff can say which ports are gone: only the ports the
/// gesture explicitly REMOVED lose their wires (a port merely absent from
/// the header may be a catalog port with live wires). Removal TRUSTS
/// its producer: whether a name may be removed at all is catalog
/// knowledge, which only the editor holds (it hides the gesture for a
/// port the node type provides and routes an override's deletion
/// through a revert instead), so this layer sweeps what it is told to
/// and a name with nothing to sweep is an idempotent no-op.
fn update_node_ports(
    view: &FileView,
    id: &str,
    inputs: &[PortSig],
    outputs: &[PortSig],
    removed_inputs: &[String],
    removed_outputs: &[String],
) -> Result<(), EditError> {
    validate_port_sigs(inputs, outputs)?;
    let decl = resolve(view, id)?;
    // Editing an INLINE node's port signature: de-inline (wire kept)
    // and rewrite the named decl's header (an inline has none).
    if let Decl::InlineNode(inline) = &decl {
        let (_, scoped) = extract_inline(view, inline, true)?;
        return update_node_ports(view, &scoped, inputs, outputs, removed_inputs, removed_outputs);
    }
    if !ContainerKind::Node.matches(&decl) {
        return Err(kind_mismatch("UpdateNodePorts", id, "Node", &decl));
    }
    let removed_ins: std::collections::HashSet<&str> =
        removed_inputs.iter().map(String::as_str).collect();
    let removed_outs: std::collections::HashSet<&str> =
        removed_outputs.iter().map(String::as_str).collect();
    if let Some(local) = decl.local_id() {
        detach_parent_connections(&decl, |conn| {
            // A literal config fill (`n.key = value`) is never a wire; see
            // detach_dangling_port_connections for why it must survive.
            if crate::cst::nodes::connection_is_config_origin(conn, None, None) {
                return false;
            }
            let (t_id, t_port) = endpoint_parts(conn, 0);
            let (s_id, s_port) = endpoint_parts(conn, 1);
            (t_id.as_deref() == Some(local.as_str())
                && removed_ins.contains(t_port.as_deref().unwrap_or("")))
                || (s_id.as_deref() == Some(local.as_str())
                    && removed_outs.contains(s_port.as_deref().unwrap_or("")))
        });
    }
    // A wire can also be written INSIDE the node's own braces
    // (`n = Custom { b: src.value }`); the parent-scope sweep never sees
    // it, and left behind it re-creates the port (via a config key on a
    // node that accepts custom inputs) or fails the next build. A literal
    // fill is not a wire and survives here; the editor removes it through
    // RemoveConfig, since only it can tell a port literal from a config
    // value. Outputs cannot be driven from a body, so only inputs sweep.
    // The ORDERED list drives the walk (an inline driver's extraction
    // appends a decl per name, and set order would make the emitted
    // source, and its undo TextEdit, nondeterministic).
    for name in removed_inputs {
        detach_body_driver(view, &decl, name)?;
    }
    // Another node's BODY can read a removed OUTPUT (`d = Debug { data:
    // n.custom }`); the parent-scope sweep only sees statement lines, so
    // the braces-endpoint references get walked too and the ones bound
    // to a removed output die with it (leaving one keeps a wire the
    // canvas already dropped, and the next parse resurrects it).
    if !removed_outputs.is_empty() {
        for field in view.endpoint_fields_referencing(&decl) {
            let port = field
                .children()
                .find(|n| n.kind() == SyntaxKind::ENDPOINT)
                .and_then(Endpoint::cast)
                .and_then(|e| e.parts().1);
            if port.as_deref().is_some_and(|p| removed_outs.contains(p)) {
                detach_body_member(&field);
            }
        }
    }
    rewrite_port_header(&decl, inputs, outputs)
}

/// Both halves of every port sig are written into the decl's HEADER, which is
/// then reparsed, so both are validated at the door: the NAME must be a
/// single identifier, and the TYPE must actually parse as a type.
fn validate_port_sigs(inputs: &[PortSig], outputs: &[PortSig]) -> Result<(), EditError> {
    for (side, ports) in [("input", inputs), ("output", outputs)] {
        let mut names = std::collections::HashSet::new();
        for p in ports {
            validate_ident("port name", &p.name)?;
            validate_port_type("port type", &p.port_type)?;
            if !names.insert(&p.name) {
                return Err(EditError::InvalidArgument(format!("duplicate {side} port '{}'", p.name)));
            }
        }
    }
    // An output has no optionality, so the header never carries `?` on
    // one; a sig asking for it would author a line the parser refuses.
    if let Some(p) = outputs.iter().find(|p| !p.required) {
        return Err(EditError::InvalidArgument(format!(
            "output port {:?} cannot be optional: an output carries no `?`",
            p.name
        )));
    }
    Ok(())
}

/// Rebuild the decl with `id = Type` + the new signature as its header,
/// preserving the body verbatim.
fn rewrite_port_header(decl: &Decl, inputs: &[PortSig], outputs: &[PortSig]) -> Result<(), EditError> {
    let header = decl_header_text(decl);
    // head = `id = Type` (everything up to the first `(` or `->`).
    let (head, _) = split_header_head(&header);
    let new_header = format!("{}{}", head.trim_end(), build_signature(inputs, outputs));
    rebuild_decl(decl, &new_header)
}

/// Detach the parent-scope CONNECTION lines around `decl` that `doomed`
/// condemns. Shared by the two port-surface sweeps (a group's signature
/// diff, a node's removed list).
fn detach_parent_connections(decl: &Decl, doomed: impl Fn(&SyntaxNode) -> bool) {
    let Some(parent) = decl.syntax().parent() else { return };
    let victims: Vec<SyntaxNode> = parent
        .children()
        .filter(|n| n.kind() == SyntaxKind::CONNECTION && doomed(n))
        .collect();
    for c in victims {
        detach_with_leading_ws(&c);
    }
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
        detach_parent_connections(decl, |n| dangling(n, false));
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
        Some(body) => format!("{lead}{} {}", new_header.trim(), body.syntax()),
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

/// The container's IN-port names as the COMPILER sees them: the
/// header's own `PORT_SIG_IN` port declarations (read off the parsed
/// tree, never re-parsed from header text, which mis-splits on the
/// commas and parentheses a port TYPE may carry), plus, for a loop, the
/// carry inputs the compiler synthesizes (a `carry:` name matching an
/// out port becomes an in port even when the header never wrote it).
// SYNC: carry input synthesis <-> crates/weft-compiler/src/weft_compiler.rs
//       (lower_group's carry loop), packages/weft-graph/src/webview/lib/
//       projection/apply.ts syncLoopCarryInputs
fn header_in_port_names(decl: &Decl) -> Vec<String> {
    use crate::cst::SyntaxKind as K;
    let header = match decl {
        Decl::Group(g) => g.header().map(|h| h.syntax().clone()),
        Decl::Loop(l) => l.header().map(|h| h.syntax().clone()),
        _ => None,
    };
    let Some(header) = header else { return Vec::new() };
    let mut ins: Vec<String> = Vec::new();
    let mut outs: Vec<String> = Vec::new();
    for sig in header.children() {
        let bucket = match sig.kind() {
            K::PORT_SIG_IN => &mut ins,
            K::PORT_SIG_OUT => &mut outs,
            _ => continue,
        };
        for el in sig.children() {
            if el.kind() != K::PORT_DECL {
                continue;
            }
            let text = el.to_string();
            let text = text.trim().trim_end_matches(',').trim();
            if let Ok(port) = crate::weft_compiler::try_parse_port_decl(text) {
                bucket.push(port.name);
            }
        }
    }
    for carry in read_carry_list(decl) {
        if outs.contains(&carry) && !ins.contains(&carry) {
            ins.push(carry);
        }
    }
    ins
}

/// Split a header string into (`id = Type`, rest-with-sig). The head
/// ends at whichever of the first `(` or the first `->` comes first (a
/// signature can open with `->` when there are no in-ports).
fn split_header_head(header: &str) -> (String, String) {
    let cut = [header.find('('), header.find("->")]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(header.len());
    (header[..cut].to_string(), header[cut..].to_string())
}

/// Build a `(in) -> (out)` signature string from port sigs.
fn build_signature(inputs: &[PortSig], outputs: &[PortSig]) -> String {
    let fmt = |p: &PortSig| {
        let ty = p.port_type.as_str();
        let opt = if p.required { "" } else { "?" };
        format!("{}{opt}: {ty}", p.name)
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
/// SYNC: format_string <-> crates/weft-compiler/src/weft_compiler.rs unescape_heredoc, crates/weft-compiler/src/cst/lexer.rs heredoc_span, packages/weft-graph/src/webview/lib/value-format.ts formatConfigValue (and parseConfigToken, its inverse)
fn format_string(s: &str) -> Result<String, EditError> {
    if s.contains('\n') {
        // A multi-line value is emitted as a ```...``` heredoc. Content is
        // verbatim between the fences; an inner ``` is escaped as \```
        // (the one escape the decoder honors). The escape's own literal
        // spelling (`\```` inside the value) is therefore unencodable:
        // reject loudly rather than emit source that re-parses wrong.
        if s.contains("\\```") {
            return Err(EditError::InvalidArgument(
                "multi-line value cannot contain the sequence \\``` (it is the heredoc's fence escape)".into(),
            ));
        }
        Ok(format!("```\n{}\n```", s.replace("```", "\\```")))
    } else {
        Ok(format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
    }
}

//! Structured source editing on a lossless CST.
//!
//! Editor frontends (the VS Code webview today) never touch `.weft` text. They
//! send edit INTENTS (`EditOp`); this module applies them to the source and
//! returns the new source plus a byte-exact inverse `TextEdit` for undo.
//!
//! How it works: the source is parsed into a lossless concrete syntax tree
//! (`crate::cst`), cloned mutable, and each op is applied as a STRUCTURAL TREE
//! MUTATION (`splice_children`/`detach`) resolved against the typed view. The
//! new source is the tree's `to_string()`, which re-emits only the changed
//! subtree and leaves every other byte identical (structural sharing). Edits
//! never compute text offsets: the closing `}` is a real token and a group body
//! is a real node, so an edit cannot splice in the wrong place. See
//! `docs/cst-node-model.md` and `docs/cst-edit-spec.md`.
//!
//! The parse-server wire: `EditOp`s in, `(new_source, inverse TextEdit)` out.
//! The CST is an internal detail of how an edit produces the new source.

mod ops;

use serde::{Deserialize, Serialize};

use crate::cst::nodes::WeftFile;
use crate::cst::parse;

/// A structured edit intent. `serde` tag `op` matches the parse-server wire.
// SYNC: EditOp <-> extension-vscode/src/shared/protocol.ts EditOp
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
    /// connection lives in (None = top level), symmetric with `AddEdge`.
    RemoveEdge { source: String, source_port: String, target: String, target_port: String, scope_group: Option<String> },
    /// Add an empty group `Label = Group() -> () {}`.
    AddGroup { label: String, parent_group: Option<String> },
    /// Remove a group; its body moves up one scope (ungroup).
    RemoveGroup { group: String },
    /// Rename a group and rewrite references to its ports. `group` is the group's
    /// SCOPED id (e.g. `Outer.Inner`); `new_label` is the new bare local name.
    RenameGroup { group: String, new_label: String },
    /// Move a node into a group (top level when `target_group` is None).
    MoveNodeScope { node: String, target_group: Option<String> },
    /// Move a group into another group (top level when None).
    MoveGroupScope { group: String, target_group: Option<String> },
    /// Rewrite a node's port signature.
    UpdateNodePorts { node: String, inputs: Vec<PortSig>, outputs: Vec<PortSig> },
    /// Rewrite a group's port signature.
    UpdateGroupPorts { group: String, inputs: Vec<PortSig>, outputs: Vec<PortSig> },
    /// Set (or clear) a group's description: the `# Description:` comment that is
    /// the first body line of the group. `description: None`/empty removes it.
    SetGroupDescription { group: String, description: Option<String> },
    /// Add an empty loop `name = Loop() -> () {}` at the end of the
    /// scope (top level when `parent_group` is None). `parallel`
    /// defaults to the validator's default (sequential); the editor
    /// surfaces the field in the config strip for the user to set.
    AddLoop { label: String, parent_group: Option<String> },
    /// Remove a loop; its body moves up one scope (un-loop).
    RemoveLoop { loop_id: String },
    /// Rename a loop. Loop-only mirror of `RenameGroup`; `loop_id` is the
    /// loop's SCOPED id (same contract as `RenameGroup`'s `group`), so a
    /// rename is unambiguous when two loops share a local label in
    /// different scopes. The dispatch refuses a Group target and vice
    /// versa so the webview routing stays honest.
    RenameLoop { loop_id: String, new_label: String },
    /// Move a loop into another container (top level when None).
    /// Loop-only mirror of `MoveGroupScope`.
    MoveLoopScope { loop_id: String, target_group: Option<String> },
    /// Rewrite a loop's port signature. Same shape as UpdateGroupPorts.
    UpdateLoopPorts { loop_id: String, inputs: Vec<PortSig>, outputs: Vec<PortSig> },
    /// Set a single loop config field (`parallel`, `over`, `carry`,
    /// `max_iters`, `trim_on_mismatch`). The value is a pre-formatted
    /// source token: `true`, `["a","b"]`, `100`, etc. Inserts the field
    /// if missing, replaces it if present.
    SetLoopConfig { loop_id: String, key: String, value: String },
    /// Remove a loop config field.
    RemoveLoopConfig { loop_id: String, key: String },
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
    /// Group or Loop (or expected-container) not found. Named
    /// `ContainerNotFound` because both kinds of container ops
    /// (Group AND Loop variants) route through the same resolution
    /// failure path; spelling it `GroupNotFound` misled the Loop
    /// callers about which decl kind the id was looked up against.
    #[error("container not found: {0}")]
    ContainerNotFound(String),
    #[error("id is ambiguous (matches multiple): {0}")]
    AmbiguousId(String),
    #[error("id already exists in scope: {0}")]
    DuplicateId(String),
    /// Args are (target, target_port, source, source_port): the Display renders
    /// real weft syntax `target.port = source.port`.
    #[error("connection not found: {0}.{1} = {2}.{3}")]
    ConnectionNotFound(String, String, String, String),
    #[error("invalid edit argument: {0}")]
    InvalidArgument(String),
    #[error("source does not parse: {0}")]
    Unparseable(String),
}

/// A minimal text edit: replace the byte range `[start, end)` of the source
/// with `text`. The editor's reversible-action unit: applying an edit yields its
/// INVERSE (the one that restores the prior source), so undo/redo replay
/// inverse/forward edits without snapshotting the whole file. A text-edit
/// inverse restores the exact original bytes, so `@file(...)` markers and
/// formatting survive faithfully.
///
/// Byte offsets (not line/col) so empty-replacement and trailing-newline
/// boundaries are unambiguous. Offsets land on char boundaries (the diff trims
/// on `char_indices`).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextEdit {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// Apply an ordered batch of edits atomically, returning the new source AND the
/// inverse edit (apply it to the new source to get the original back). The whole
/// batch runs against ONE mutable CST clone: parse once, mutate op-by-op,
/// serialize once. On any op failure the whole batch fails and the caller keeps
/// the original source. `base_dir` is accepted for signature stability with the
/// parse-server; CST editing is purely structural and does not read included
/// files (an `@include` is an opaque marker token), so it is currently unused.
///
/// `source_id` is the file's identity (e.g. `MyCleaner` from `my-cleaner.weft`,
/// `Untitled` for an unsaved buffer). It's the id an anonymous top-level group
/// takes, so the editor resolves a scoped id (`MyCleaner.child`) against the
/// SAME prefix the lowering renders, with no rename pass between the two.
pub fn apply_edits(
    source: &str,
    _base_dir: Option<&std::path::Path>,
    source_id: &str,
    ops: &[EditOp],
) -> Result<(String, TextEdit), EditError> {
    // Parse to a mutable CST. The root is always a WEFT_FILE (the parser is
    // total), but guard the cast loudly rather than unwrap.
    let root = parse(source).clone_for_update();
    let file = WeftFile::cast(root).ok_or_else(|| EditError::Unparseable("root is not a weft file".into()))?;
    let view = crate::cst::nodes::FileView::new(&file, source_id);

    for op in ops {
        ops::apply_op(&view, op)?;
    }

    let new_source = file.syntax().to_string();
    let inverse = invert_text_edit(source, &new_source);
    Ok((new_source, inverse))
}

/// The inverse text edit for an `old -> new` whole-source change: the edit that,
/// applied to `new`, restores `old`. Trims the common leading + trailing bytes
/// (on char boundaries) so the edit covers only the changed region. Its
/// `[start, end)` range is in `new`'s byte offsets; its `text` is the original
/// bytes from `old`.
pub fn invert_text_edit(old: &str, new: &str) -> TextEdit {
    let ob = old.as_bytes();
    let nb = new.as_bytes();
    let max_pre = ob.len().min(nb.len());
    let mut prefix = 0;
    while prefix < max_pre && ob[prefix] == nb[prefix] {
        prefix += 1;
    }
    while prefix > 0 && (!old.is_char_boundary(prefix) || !new.is_char_boundary(prefix)) {
        prefix -= 1;
    }
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
/// replacing the byte range `[start, end)` with `text`. Total: host-supplied
/// offsets are untrusted (a buffer may have drifted), so it validates them and
/// fails LOUDLY rather than slicing blind (a bad offset would panic `&str`
/// indexing and take the parse-server down).
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

#[cfg(test)]
mod tests;

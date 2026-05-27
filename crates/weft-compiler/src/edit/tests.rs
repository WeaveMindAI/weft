//! Layer-1 tests for the structured editor. Each applies an op (or batch) to
//! source and asserts the new source. These replace the old TS weft-editor
//! suites: same coverage, against the real grammar.

use super::*;

fn apply(source: &str, ops: Vec<EditOp>) -> String {
    apply_edits(source, None, &ops).expect("edits apply").0
}

fn parse_ok(source: &str) {
    structure(source, None).expect("parses");
}

/// Apply ops, then apply the returned inverse edit: the original source must
/// come back byte-for-byte (the reversible-action / undo contract).
fn assert_reversible(source: &str, ops: Vec<EditOp>) {
    let (new_source, inverse) = apply_edits(source, None, &ops).expect("edits apply");
    assert_ne!(new_source, source, "op should change source");
    let restored = apply_text_edit(&new_source, &inverse).expect("inverse applies");
    assert_eq!(restored, source, "inverse edit must restore the original exactly");
}

#[test]
fn apply_text_edit_rejects_bad_offsets_loudly() {
    // Untrusted host offsets must fail loud, never panic the server.
    let src = "café = Text {}\n"; // 'é' is 2 bytes, byte 4 is mid-char
    assert!(matches!(apply_text_edit(src, &TextEdit { start: 4, end: 4, text: "".into() }), Err(EditError::InvalidArgument(_))));
    assert!(matches!(apply_text_edit(src, &TextEdit { start: 0, end: 9999, text: "".into() }), Err(EditError::InvalidArgument(_))));
    assert!(matches!(apply_text_edit(src, &TextEdit { start: 5, end: 2, text: "".into() }), Err(EditError::InvalidArgument(_))));
    // A valid boundary edit still works.
    assert_eq!(apply_text_edit("abc\n", &TextEdit { start: 0, end: 3, text: "xyz".into() }).unwrap(), "xyz\n");
}

#[test]
fn inverse_edit_restores_original_for_each_op() {
    assert_reversible(
        "# Project: T\n\nt = Text {\n  value: \"old\"\n}\n",
        vec![EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"new\"".into() }],
    );
    assert_reversible(
        "# Project: T\n\nt = Text {\n  value: \"x\"\n}\n",
        vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: None }],
    );
    assert_reversible(
        "# Project: T\n\na = Text {\n  value: \"x\"\n}\nb = Debug\nb.data = a.value\n",
        vec![EditOp::RemoveNode { node: "a".into() }],
    );
    assert_reversible(
        "# Project: T\n\ngrp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n",
        vec![EditOp::RemoveGroup { group: "grp".into() }],
    );
}

#[test]
fn inverse_edit_preserves_file_marker() {
    // The faithfulness win over semantic-op inverses: a `@file(...)` token in a
    // field that gets changed-and-undone comes back as the MARKER, not the
    // resolved content (a semantic inverse from the resolved project couldn't).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sys.txt"), "you are helpful").unwrap();
    let src = "# Project: T\n\nt = Text {\n  value: @file(\"sys.txt\")\n}\n";
    let (new_source, inverse) = apply_edits(src, Some(dir.path()), &[
        EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"replaced\"".into() },
    ]).expect("edits apply");
    assert!(new_source.contains("value: \"replaced\""), "{new_source}");
    let restored = apply_text_edit(&new_source, &inverse).expect("inverse applies");
    assert_eq!(restored, src, "the @file marker must be restored verbatim");
    assert!(restored.contains("@file(\"sys.txt\")"), "marker intact: {restored}");
}

#[test]
fn set_config_inline_field_replaces_in_place() {
    let src = "# Project: T\n\nt = Text {\n  value: \"old\"\n}\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"new\"".into() }]);
    assert!(out.contains("value: \"new\""), "{out}");
    assert!(!out.contains("\"old\""), "{out}");
}

#[test]
fn set_config_connection_field_keeps_prefix() {
    // `t.style = "a"` is a connection-line field; replacing it must keep the
    // `t.style = ` prefix, not turn into `style: `.
    let src = "# Project: T\n\nt = Text {\n  value: \"x\"\n}\nt.style = \"a\"\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"b\"".into() }]);
    assert!(out.contains("t.style = \"b\""), "{out}");
}

#[test]
fn set_config_inserts_when_absent() {
    let src = "# Project: T\n\nt = Text {\n  value: \"x\"\n}\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"bold\"".into() }]);
    assert!(out.contains("style: \"bold\""), "{out}");
    parse_ok(&out);
}

#[test]
fn set_config_expands_one_liner() {
    let src = "# Project: T\n\nt = Text { value: \"x\" }\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"bold\"".into() }]);
    assert!(out.contains("value: \"x\""), "{out}");
    assert!(out.contains("style: \"bold\""), "{out}");
    parse_ok(&out);
}

#[test]
fn remove_config_drops_the_line() {
    let src = "# Project: T\n\nt = Text {\n  value: \"x\"\n  style: \"bold\"\n}\n";
    let out = apply(src, vec![EditOp::RemoveConfig { node: "t".into(), key: "style".into() }]);
    assert!(!out.contains("style"), "{out}");
    assert!(out.contains("value: \"x\""), "{out}");
}

#[test]
fn set_label_and_clear() {
    let src = "# Project: T\n\nt = Text {\n  value: \"x\"\n}\n";
    let withlabel = apply(src, vec![EditOp::SetLabel { node: "t".into(), label: Some("Hi".into()) }]);
    assert!(withlabel.contains("_label: \"Hi\""), "{withlabel}");
    let cleared = apply(&withlabel, vec![EditOp::SetLabel { node: "t".into(), label: None }]);
    assert!(!cleared.contains("_label"), "{cleared}");
}

#[test]
fn add_node_top_level() {
    let src = "# Project: T\n\nt = Text {\n  value: \"x\"\n}\n";
    let out = apply(src, vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: None }]);
    assert!(out.contains("d = Debug {}"), "{out}");
    parse_ok(&out);
}

#[test]
fn remove_node_and_its_edges() {
    let src = "# Project: T\n\na = Text {\n  value: \"x\"\n}\nb = Debug\nb.data = a.value\n";
    let out = apply(src, vec![EditOp::RemoveNode { node: "a".into() }]);
    assert!(!out.contains("a = Text"), "{out}");
    assert!(!out.contains("b.data = a.value"), "connection to removed node dropped: {out}");
    assert!(out.contains("b = Debug"), "{out}");
}

#[test]
fn remove_scoped_node_keeps_same_local_name_edge_in_another_scope() {
    // A top-level `a` and a `grp.a` share the local name `a`. Removing grp.a
    // must NOT delete the top-level edge `b.data = a.value`.
    let src = "# Project: T\n\na = Text { value: \"x\" }\nb = Debug\nb.data = a.value\ngrp = Group() -> () {\n  a = Text { value: \"y\" }\n}\n";
    let out = apply(src, vec![EditOp::RemoveNode { node: "grp.a".into() }]);
    assert!(out.contains("b.data = a.value"), "top-level edge survives: {out}");
    assert!(out.contains("a = Text { value: \"x\" }"), "top-level a survives: {out}");
    // grp.a is gone (the group is now empty but present).
    let p = structure(&out, None).unwrap();
    assert!(!p.nodes.iter().any(|n| n.id == "grp.a"), "grp.a removed: {out}");
}

#[test]
fn add_and_remove_edge() {
    let src = "# Project: T\n\na = Text {\n  value: \"x\"\n}\nb = Debug\n";
    let added = apply(src, vec![EditOp::AddEdge {
        source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None,
    }]);
    assert!(added.contains("b.data = a.value"), "{added}");
    let removed = apply(&added, vec![EditOp::RemoveEdge {
        source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None,
    }]);
    assert!(!removed.contains("b.data = a.value"), "{removed}");
}

#[test]
fn remove_edge_is_scope_disambiguated() {
    // Two groups each have `t = Text` wired into a `d = Debug` with the same
    // local quad. Removing the edge in `g1` must leave `g2`'s identical edge.
    let src = "# Project: T\n\ng1 = Group() -> () {\n  t = Text { value: \"x\" }\n  d = Debug\n  d.data = t.value\n}\ng2 = Group() -> () {\n  t = Text { value: \"y\" }\n  d = Debug\n  d.data = t.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveEdge {
        source: "t".into(), source_port: "value".into(), target: "d".into(), target_port: "data".into(), scope_group: Some("g1".into()),
    }]);
    let p = structure(&out, None).unwrap();
    // g1's edge gone, g2's edge kept.
    assert!(!p.edges.iter().any(|e| e.source == "g1.t" && e.target == "g1.d"), "g1 edge removed: {out}");
    assert!(p.edges.iter().any(|e| e.source == "g2.t" && e.target == "g2.d"), "g2 edge kept: {out}");
}

#[test]
fn remove_edge_into_group_port_resolves_boundary() {
    // An edge wired to a group's port is stored against `{grp}__in`/`__out`.
    // removeEdge must resolve the `grp` ref to that boundary, not `grp`.
    let src = "# Project: T\n\na = Text { value: \"x\" }\ngrp = Group(inp: String) -> () {\n  t = Debug\n  t.data = self.inp\n}\ngrp.inp = a.value\n";
    let out = apply(src, vec![EditOp::RemoveEdge {
        source: "a".into(), source_port: "value".into(), target: "grp".into(), target_port: "inp".into(), scope_group: None,
    }]);
    assert!(!out.contains("grp.inp = a.value"), "edge into group port removed: {out}");
    structure(&out, None).expect("parses");
}

#[test]
fn add_edge_into_group_input_replaces_driver_no_double() {
    // Reconnecting a group input must replace the existing driver (single-
    // driver), not append a second line. The old driver edge is stored against
    // `grp__in`, so remove_existing_driver must resolve `grp` -> `grp__in`.
    let src = "# Project: T\n\na = Text { value: \"x\" }\nc = Text { value: \"y\" }\ngrp = Group(inp: String) -> () {\n  t = Debug\n  t.data = self.inp\n}\ngrp.inp = a.value\n";
    let out = apply(src, vec![EditOp::AddEdge {
        source: "c".into(), source_port: "value".into(), target: "grp".into(), target_port: "inp".into(), scope_group: None,
    }]);
    assert!(out.contains("grp.inp = c.value"), "new driver present: {out}");
    assert!(!out.contains("grp.inp = a.value"), "old driver replaced (no double): {out}");
    // Exactly one driver of grp.inp.
    assert_eq!(out.matches("grp.inp =").count(), 1, "single driver: {out}");
}

#[test]
fn add_edge_replaces_existing_driver() {
    let src = "# Project: T\n\na = Text {\n  value: \"x\"\n}\nc = Text {\n  value: \"y\"\n}\nb = Debug\nb.data = a.value\n";
    let out = apply(src, vec![EditOp::AddEdge {
        source: "c".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None,
    }]);
    assert!(out.contains("b.data = c.value"), "{out}");
    assert!(!out.contains("b.data = a.value"), "old driver removed: {out}");
}

#[test]
fn rename_group_updates_header_and_refs() {
    let src = "# Project: T\n\ngrp = Group(inp: String) -> (outp: String) {\n  t = Text { value: \"x\" }\n  self.outp = t.value\n}\nd = Debug\nd.data = grp.outp\n";
    let out = apply(src, vec![EditOp::RenameGroup { old_label: "grp".into(), new_label: "proc".into() }]);
    assert!(out.contains("proc = Group"), "header renamed: {out}");
    assert!(out.contains("d.data = proc.outp"), "ref renamed: {out}");
    assert!(!out.contains("grp"), "no stale grp: {out}");
}

#[test]
fn set_project_meta_inserts_description() {
    let src = "# Project: T\n\nt = Text { value: \"x\" }\n";
    let out = apply(src, vec![EditOp::SetProjectMeta { name: Some("Renamed".into()), description: Some("hi".into()) }]);
    assert!(out.contains("# Project: Renamed"), "{out}");
    assert!(out.contains("# Description: hi"), "{out}");
}

#[test]
fn batch_is_atomic_and_sequential() {
    // remove-then-add edge in one batch (the classic chain) lands correctly.
    let src = "# Project: T\n\na = Text {\n  value: \"x\"\n}\nc = Text {\n  value: \"y\"\n}\nb = Debug\nb.data = a.value\n";
    let out = apply(src, vec![
        EditOp::RemoveEdge { source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None },
        EditOp::AddEdge { source: "c".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None },
    ]);
    assert!(out.contains("b.data = c.value"), "{out}");
}

#[test]
fn add_node_into_group() {
    let src = "# Project: T\n\ngrp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n";
    let out = apply(src, vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: Some("grp".into()) }]);
    parse_ok(&out);
    assert!(out.contains("d = Debug {}"), "{out}");
    let d_line = out.lines().find(|l| l.contains("d = Debug")).unwrap();
    assert!(d_line.starts_with("  "), "indented inside group: {d_line:?}");
}

#[test]
fn add_and_remove_group() {
    let src = "# Project: T\n\nt = Text { value: \"x\" }\n";
    let added = apply(src, vec![EditOp::AddGroup { label: "grp".into(), parent_group: None }]);
    assert!(added.contains("grp = Group() -> () {}"), "{added}");
    parse_ok(&added);
    let removed = apply(&added, vec![EditOp::RemoveGroup { group: "grp".into() }]);
    assert!(!removed.contains("grp = Group"), "{removed}");
}

#[test]
fn move_node_into_group_then_out() {
    let src = "# Project: T\n\ngrp = Group() -> () {\n  x = Text { value: \"a\" }\n}\nt = Text { value: \"b\" }\n";
    let moved_in = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved_in);
    // t now lives inside grp (the grp.t scoped id exists after re-parse).
    let p = structure(&moved_in, None).unwrap();
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t moved into grp: {moved_in}");
}

#[test]
fn move_node_into_single_line_empty_group() {
    // A freshly-added group is `grp = Group() -> () {}` (one line, inline `{}`).
    // Moving a node into it must split the body open and nest the node INSIDE,
    // not splice it above the header. Regression: the inline `{}` case put the
    // node before the group line, leaving an orphan and an empty group.
    let src = "# Project: T\n\ngrp = Group() -> () {}\nt = Text { value: \"b\" }\n";
    let moved = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved);
    let p = structure(&moved, None).unwrap();
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t must nest inside grp: {moved}");
    // The node must not survive at top level (that would be the orphan bug).
    assert!(!p.nodes.iter().any(|n| n.id == "t"), "no top-level orphan t: {moved}");
}

#[test]
fn move_node_into_group_with_post_body_output_ports() {
    // A group can carry output ports AFTER its body: `} -> (out: ...)`. The
    // group's last source line is then `} -> (...)`, not a bare `}`. Inserting a
    // child must still go INSIDE the body (before that closing line), not try to
    // split a non-existent inline brace. Regression: a `== "}"` text check missed
    // this shape and panicked on the absent `{`.
    let src = "# Project: T\n\ngrp = Group() {\n  x = Text { value: \"a\" }\n} -> (out: String)\nt = Text { value: \"b\" }\n";
    let moved = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved);
    let p = structure(&moved, None).unwrap();
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t must nest inside grp: {moved}");
    assert!(!p.nodes.iter().any(|n| n.id == "t"), "no top-level orphan t: {moved}");
}

#[test]
fn move_node_into_group_with_separate_line_output_ports() {
    // Post-body output ports can sit on their OWN line after the `}`. The
    // parser's span.end_line then points at the `-> (...)` line, not the `}`.
    // Insertion must still nest the child inside the body (before the real `}`),
    // not after it. Regression: trusting span.end_line spliced the child after
    // the brace and produced unparseable source written straight to disk.
    let src = "# Project: T\n\ngrp = Group() {\n  x = Text { value: \"a\" }\n}\n-> (out: String)\nt = Text { value: \"b\" }\n";
    let moved = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved);
    let p = structure(&moved, None).unwrap();
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t must nest inside grp: {moved}");
    assert!(!p.nodes.iter().any(|n| n.id == "t"), "no top-level orphan t: {moved}");
}

#[test]
fn remove_group_with_separate_line_output_ports() {
    // Ungrouping a group whose output ports sit on their own line after `}` must
    // drop the header, the `}`, AND the `-> (...)` line (all part of the group
    // declaration), keeping the de-indented child. Regression: trusting
    // span.end_line dropped the arrow line and orphaned the real `}`.
    let src = "# Project: T\n\ngrp = Group() {\n  x = Text { value: \"a\" }\n}\n-> (out: String)\n";
    let removed = apply(src, vec![EditOp::RemoveGroup { group: "grp".into() }]);
    parse_ok(&removed);
    assert!(!removed.contains("Group("), "group declaration gone: {removed}");
    assert!(!removed.contains("-> (out"), "post-body output ports gone: {removed}");
    let p = structure(&removed, None).unwrap();
    assert!(p.nodes.iter().any(|n| n.id == "x"), "child x survives at top level: {removed}");
}

#[test]
fn move_node_into_group_with_multiline_signature() {
    // A group with a multi-line port signature ending in an inline `{}` spans
    // several lines, but its body is still inline. The insertion must split the
    // `{}` open and nest the node inside, NOT splice it into the signature.
    let src = "# Project: T\n\ngrp = Group(\n  a: String\n) -> () {}\nt = Text { value: \"b\" }\n";
    let moved = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved);
    let p = structure(&moved, None).unwrap();
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t must nest inside grp: {moved}");
    assert!(!p.nodes.iter().any(|n| n.id == "t"), "no top-level orphan t: {moved}");
}

#[test]
fn update_node_ports_rewrites_signature() {
    let src = "# Project: T\n\ng = Group() -> () {\n  x = Text { value: \"a\" }\n}\n";
    let out = apply(src, vec![EditOp::UpdateGroupPorts {
        group: "g".into(),
        inputs: vec![PortSig { name: "inp".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![PortSig { name: "outp".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(out.contains("g = Group(inp: String) -> (outp: String)"), "{out}");
    parse_ok(&out);
}

#[test]
fn unparseable_edit_fails_loud_not_silent() {
    // Targeting a node that doesn't exist is a hard error, never a silent no-op
    // that loses the user's intent.
    let src = "# Project: T\n\nt = Text { value: \"x\" }\n";
    let err = apply_edits(src, None, &[EditOp::RemoveNode { node: "nope".into() }]).unwrap_err();
    assert!(matches!(err, EditError::NodeNotFound(_)), "{err:?}");
}

#[test]
fn remove_group_ungroups_children_up_one_scope() {
    // Deleting a group keeps its nodes: they move up to the parent scope.
    let src = "# Project: T\n\ngrp = Group(inp: String) -> (outp: String) {\n  t = Text { value: \"x\" }\n  self.outp = t.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveGroup { group: "grp".into() }]);
    assert!(!out.contains("grp = Group"), "group header gone: {out}");
    assert!(out.contains("t = Text"), "child survives (ungroup): {out}");
    assert!(!out.contains("self.outp"), "group's own boundary wiring dropped: {out}");
    parse_ok(&out);
}

#[test]
fn ambiguous_bare_id_fails_loud() {
    // Two `t` in different groups: a bare `t` op must error, never guess + splice
    // the wrong node.
    let src = "# Project: T\n\ng1 = Group() -> () {\n  t = Text { value: \"a\" }\n}\ng2 = Group() -> () {\n  t = Text { value: \"b\" }\n}\n";
    let err = apply_edits(src, None, &[EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"z\"".into() }]).unwrap_err();
    assert!(matches!(err, EditError::AmbiguousId(_)), "{err:?}");
}

#[test]
fn add_duplicate_id_fails_loud() {
    let src = "# Project: T\n\nt = Text { value: \"x\" }\n";
    let err = apply_edits(src, None, &[EditOp::AddNode { id: "t".into(), node_type: "Debug".into(), parent_group: None }]).unwrap_err();
    assert!(matches!(err, EditError::DuplicateId(_)), "{err:?}");
}

#[test]
fn add_edge_to_missing_endpoint_fails_loud() {
    let src = "# Project: T\n\nb = Debug\n";
    let err = apply_edits(src, None, &[EditOp::AddEdge {
        source: "ghost".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None,
    }]).unwrap_err();
    assert!(matches!(err, EditError::NodeNotFound(_)), "{err:?}");
}

#[test]
fn rename_group_to_empty_fails_loud() {
    let src = "# Project: T\n\ngrp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n";
    let err = apply_edits(src, None, &[EditOp::RenameGroup { old_label: "grp".into(), new_label: "".into() }]).unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "{err:?}");
}

#[test]
fn set_project_meta_inserts_header_when_absent() {
    // No `# Project:` line: meta must be inserted, not silently dropped.
    let src = "t = Text { value: \"x\" }\n";
    let out = apply(src, vec![EditOp::SetProjectMeta { name: Some("New".into()), description: Some("desc".into()) }]);
    assert!(out.contains("# Project: New"), "{out}");
    assert!(out.contains("# Description: desc"), "{out}");
    parse_ok(&out);
}

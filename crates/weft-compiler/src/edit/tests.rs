//! Layer-1 tests for the structured editor. Each applies an op (or batch) to
//! source and asserts the new source, against the real grammar.

use super::*;
use crate::cst::kind::SyntaxKind;
use crate::cst::nodes::WeftFile;
use crate::cst::parse;

fn apply(source: &str, ops: Vec<EditOp>) -> String {
    apply_edits(source, None, "Untitled", &ops).expect("edits apply").0
}

/// Assert the source re-parses into a well-formed tree: it round-trips (lossless
/// guarantee), has no ERROR nodes, and no header carries a second `=` (the
/// signature of a corrupt rewrite like `n = Text(x) = Text {...}` which this
/// lenient grammar would otherwise parse clean). A node/group header has exactly
/// one `=` (`id = Type`); two means a decl got spliced into another.
fn parse_ok(source: &str) {
    let tree = parse(source);
    assert_eq!(tree.to_string(), source, "edited source must round-trip");
    let has_error = tree.descendants().any(|n| n.kind() == SyntaxKind::ERROR);
    assert!(!has_error, "edited source must parse without ERROR nodes:\n{source}");
    // Brace balance: the cheap catch-all for the "an edit left a body unclosed"
    // class (e.g. a comment first-line swallowing the close brace into itself).
    // The lenient parser tolerates an unclosed body at EOF with no ERROR node, so
    // round-trip alone misses it; a `{`/`}` count mismatch catches it.
    let count = |k| tree.descendants_with_tokens().filter(|e| e.kind() == k).count();
    assert_eq!(count(SyntaxKind::L_BRACE), count(SyntaxKind::R_BRACE), "unbalanced braces after edit (a body was left unclosed):\n{source}");
    for header in tree.descendants().filter(|n| n.kind() == SyntaxKind::HEADER) {
        let eqs = header
            .children_with_tokens()
            .filter(|e| e.kind() == SyntaxKind::EQ)
            .count();
        assert!(eqs <= 1, "header has {eqs} `=` (corrupt rewrite):\n{source}");
    }
    // Universal structural invariants a correct edit must preserve, the cheap
    // catch-all for "valid-but-wrong" corruption a lenient round-trip misses:
    //  (1) scoped node ids are unique (no decl spliced into a colliding scope);
    //  (2) no edge dangles, resolved by Weft's SAME-SCOPE rule (mirroring
    //      flatten/`endpoint_resolves_to`): an endpoint `{scope}.x` resolves to
    //      that node if it exists, else to a TOP-LEVEL `x`; `self` is a boundary.
    //      A bare-name-matches-anywhere fallback would miss cross-scope dangles.
    let s = structure(source);
    let ids: std::collections::HashSet<&str> = s.nodes.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(ids.len(), s.nodes.len(), "duplicate scoped node id after edit:\n{source}");
    let id_set: std::collections::HashSet<String> = s.nodes.iter().map(|n| n.id.clone()).collect();
    let resolves = |scoped_ep: &str| {
        let bare = scoped_ep.rsplit('.').next().unwrap_or(scoped_ep);
        // `self`/boundary; the exact scoped node; or a same-scope fall-through to
        // a TOP-LEVEL node of the bare name (a top-level id has no `.`).
        bare == "self" || id_set.contains(scoped_ep) || id_set.contains(bare)
    };
    for e in &s.edges {
        for (which, ep) in [("source", &e.source), ("target", &e.target)] {
            if !ep.is_empty() {
                assert!(resolves(ep), "edge {which} '{ep}' dangles (no such node) after edit:\n{source}");
            }
        }
    }
}

/// A CST-derived structural view for test assertions, replacing the old
/// `ProjectDefinition`-based `structure()`. Exposes scoped node ids and
/// scoped connection endpoints, which is what the assertions check.
struct Structure {
    nodes: Vec<NodeView>,
    edges: Vec<EdgeView>,
}
struct NodeView {
    id: String,
}
struct EdgeView {
    source: String,
    target: String,
}

/// Build the structural view by walking the CST: every decl's scoped id, and
/// every connection's scoped (source, target) endpoint ids. Scoped means the
/// dot-joined enclosing group labels, mirroring flatten.
fn structure(source: &str) -> Structure {
    let file = WeftFile::cast(parse(source)).expect("root is a weft file");
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    walk(file.syntax(), &mut Vec::new(), &mut nodes, &mut edges);
    Structure { nodes, edges }
}

fn walk(parent: &crate::cst::SyntaxNode, prefix: &mut Vec<String>, nodes: &mut Vec<NodeView>, edges: &mut Vec<EdgeView>) {
    let scoped = |prefix: &[String], local: &str| -> String {
        if prefix.is_empty() { local.to_string() } else { format!("{}.{}", prefix.join("."), local) }
    };
    for node in parent.children() {
        match node.kind() {
            SyntaxKind::NODE_DECL | SyntaxKind::GROUP_DECL | SyntaxKind::LOOP_DECL | SyntaxKind::INCLUDE_DECL => {
                if let Some(d) = crate::cst::nodes::Decl::cast(node.clone()) {
                    let local = d.local_id().unwrap_or_default();
                    nodes.push(NodeView { id: scoped(prefix, &local) });
                    let body = match &d {
                        crate::cst::nodes::Decl::Group(g) => g.body(),
                        crate::cst::nodes::Decl::Loop(l)  => l.body(),
                        _ => None,
                    };
                    if let Some(body) = body {
                        prefix.push(local);
                        walk(body.syntax(), prefix, nodes, edges);
                        prefix.pop();
                    }
                }
            }
            SyntaxKind::CONNECTION => {
                let ep = |nth: usize| -> String {
                    node.children()
                        .filter(|n| n.kind() == SyntaxKind::ENDPOINT)
                        .nth(nth)
                        .map(|ep| {
                            let id = ep
                                .children_with_tokens()
                                .filter_map(|e| e.into_token())
                                .find(|t| t.kind() == SyntaxKind::IDENT)
                                .map(|t| t.text().to_string())
                                .unwrap_or_default();
                            scoped(prefix, &id)
                        })
                        .unwrap_or_default()
                };
                // connection is `target = source`: endpoint 0 = target, 1 = source
                edges.push(EdgeView { target: ep(0), source: ep(1) });
            }
            _ => {}
        }
    }
}

/// Apply ops, then apply the returned inverse edit: the original source must
/// come back byte-for-byte (the reversible-action / undo contract).
fn assert_reversible(source: &str, ops: Vec<EditOp>) {
    let (new_source, inverse) = apply_edits(source, None, "Untitled", &ops).expect("edits apply");
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
        "t = Text {\n  value: \"old\"\n}\n",
        vec![EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"new\"".into() }],
    );
    assert_reversible(
        "t = Text {\n  value: \"x\"\n}\n",
        vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: None }],
    );
    assert_reversible(
        "a = Text {\n  value: \"x\"\n}\nb = Debug\nb.data = a.value\n",
        vec![EditOp::RemoveNode { node: "a".into() }],
    );
    assert_reversible(
        "grp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n",
        vec![EditOp::RemoveGroup { group: "grp".into() }],
    );
    // Every remaining op also has a byte-exact inverse (the undo contract is for
    // ALL ops, not the four above).
    assert_reversible(
        "t = Text {\n  value: \"x\"\n  style: \"bold\"\n}\n",
        vec![EditOp::RemoveConfig { node: "t".into(), key: "style".into() }],
    );
    assert_reversible(
        "t = Text {\n  value: \"x\"\n}\n",
        vec![EditOp::SetLabel { node: "t".into(), label: Some("Hi".into()) }],
    );
    assert_reversible(
        "a = Text { value: \"x\" }\nb = Debug\n",
        vec![EditOp::AddEdge { source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None }],
    );
    assert_reversible(
        "a = Text { value: \"x\" }\nb = Debug\nb.data = a.value\n",
        vec![EditOp::RemoveEdge { source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None }],
    );
    assert_reversible(
        "t = Text { value: \"x\" }\n",
        vec![EditOp::AddGroup { label: "grp".into(), parent_group: None }],
    );
    assert_reversible(
        "grp = Group(inp: String) -> (outp: String) {\n  t = Text { value: \"x\" }\n  self.outp = t.value\n}\nd = Debug\nd.data = grp.outp\n",
        vec![EditOp::RenameGroup { group: "grp".into(), new_label: "proc".into() }],
    );
    assert_reversible(
        "grp = Group() -> () {\n  x = Text { value: \"a\" }\n}\nt = Text { value: \"b\" }\n",
        vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }],
    );
    assert_reversible(
        "outer = Group() -> () {\n  x = Debug\n}\ng = Group() -> () {\n  y = Debug\n}\n",
        vec![EditOp::MoveGroupScope { group: "g".into(), target_group: Some("outer".into()) }],
    );
    assert_reversible(
        "n = Text {\n  value: \"x\"\n}\n",
        vec![EditOp::UpdateNodePorts { node: "n".into(), inputs: vec![PortSig { name: "inp".into(), required: true, port_type: Some("String".into()) }], outputs: vec![] }],
    );
    assert_reversible(
        "g = Group() -> () {\n  x = Debug\n}\n",
        vec![EditOp::UpdateGroupPorts { group: "g".into(), inputs: vec![PortSig { name: "inp".into(), required: true, port_type: Some("String".into()) }], outputs: vec![PortSig { name: "outp".into(), required: true, port_type: Some("String".into()) }] }],
    );
    assert_reversible(
        "g = Group() -> () {\n  x = Debug\n}\n",
        vec![EditOp::SetGroupDescription { group: "g".into(), description: Some("does things".into()) }],
    );
}

#[test]
fn inverse_edit_preserves_file_marker() {
    // The faithfulness win over semantic-op inverses: a `@file(...)` token in a
    // field that gets changed-and-undone comes back as the MARKER, not the
    // resolved content (a semantic inverse from the resolved project couldn't).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sys.txt"), "you are helpful").unwrap();
    let src = "t = Text {\n  value: @file(\"sys.txt\")\n}\n";
    let (new_source, inverse) = apply_edits(src, Some(dir.path()), "Untitled", &[
        EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"replaced\"".into() },
    ]).expect("edits apply");
    assert!(new_source.contains("value: \"replaced\""), "{new_source}");
    let restored = apply_text_edit(&new_source, &inverse).expect("inverse applies");
    assert_eq!(restored, src, "the @file marker must be restored verbatim");
    assert!(restored.contains("@file(\"sys.txt\")"), "marker intact: {restored}");
}

#[test]
fn set_config_inline_field_replaces_in_place() {
    let src = "t = Text {\n  value: \"old\"\n}\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"new\"".into() }]);
    assert!(out.contains("value: \"new\""), "{out}");
    assert!(!out.contains("\"old\""), "{out}");
}

#[test]
fn set_config_connection_field_keeps_prefix() {
    // `t.style = "a"` is a connection-line field; replacing it must keep the
    // `t.style = ` prefix, not turn into `style: `.
    let src = "t = Text {\n  value: \"x\"\n}\nt.style = \"a\"\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"b\"".into() }]);
    assert!(out.contains("t.style = \"b\""), "{out}");
}

#[test]
fn set_config_inserts_when_absent() {
    let src = "t = Text {\n  value: \"x\"\n}\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"bold\"".into() }]);
    assert!(out.contains("style: \"bold\""), "{out}");
    parse_ok(&out);
}

#[test]
fn set_config_expands_one_liner() {
    let src = "t = Text { value: \"x\" }\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"bold\"".into() }]);
    assert!(out.contains("value: \"x\""), "{out}");
    assert!(out.contains("style: \"bold\""), "{out}");
    parse_ok(&out);
}

#[test]
fn remove_config_drops_the_line() {
    let src = "t = Text {\n  value: \"x\"\n  style: \"bold\"\n}\n";
    let out = apply(src, vec![EditOp::RemoveConfig { node: "t".into(), key: "style".into() }]);
    assert!(!out.contains("style"), "{out}");
    assert!(out.contains("value: \"x\""), "{out}");
}

#[test]
fn set_label_and_clear() {
    let src = "t = Text {\n  value: \"x\"\n}\n";
    let withlabel = apply(src, vec![EditOp::SetLabel { node: "t".into(), label: Some("Hi".into()) }]);
    assert!(withlabel.contains("_label: \"Hi\""), "{withlabel}");
    let cleared = apply(&withlabel, vec![EditOp::SetLabel { node: "t".into(), label: None }]);
    assert!(!cleared.contains("_label"), "{cleared}");
}

#[test]
fn add_node_top_level() {
    let src = "t = Text {\n  value: \"x\"\n}\n";
    let out = apply(src, vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: None }]);
    assert!(out.contains("d = Debug {}"), "{out}");
    parse_ok(&out);
}

#[test]
fn remove_node_and_its_edges() {
    let src = "a = Text {\n  value: \"x\"\n}\nb = Debug\nb.data = a.value\n";
    let out = apply(src, vec![EditOp::RemoveNode { node: "a".into() }]);
    assert!(!out.contains("a = Text"), "{out}");
    assert!(!out.contains("b.data = a.value"), "connection to removed node dropped: {out}");
    assert!(out.contains("b = Debug"), "{out}");
}

#[test]
fn remove_scoped_node_keeps_same_local_name_edge_in_another_scope() {
    // A top-level `a` and a `grp.a` share the local name `a`. Removing grp.a
    // must NOT delete the top-level edge `b.data = a.value`.
    let src = "a = Text { value: \"x\" }\nb = Debug\nb.data = a.value\ngrp = Group() -> () {\n  a = Text { value: \"y\" }\n}\n";
    let out = apply(src, vec![EditOp::RemoveNode { node: "grp.a".into() }]);
    assert!(out.contains("b.data = a.value"), "top-level edge survives: {out}");
    assert!(out.contains("a = Text { value: \"x\" }"), "top-level a survives: {out}");
    // grp.a is gone (the group is now empty but present).
    let p = structure(&out);
    assert!(!p.nodes.iter().any(|n| n.id == "grp.a"), "grp.a removed: {out}");
}

#[test]
fn add_and_remove_edge() {
    let src = "a = Text {\n  value: \"x\"\n}\nb = Debug\n";
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
    let src = "g1 = Group() -> () {\n  t = Text { value: \"x\" }\n  d = Debug\n  d.data = t.value\n}\ng2 = Group() -> () {\n  t = Text { value: \"y\" }\n  d = Debug\n  d.data = t.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveEdge {
        source: "t".into(), source_port: "value".into(), target: "d".into(), target_port: "data".into(), scope_group: Some("g1".into()),
    }]);
    let p = structure(&out);
    // g1's edge gone, g2's edge kept.
    assert!(!p.edges.iter().any(|e| e.source == "g1.t" && e.target == "g1.d"), "g1 edge removed: {out}");
    assert!(p.edges.iter().any(|e| e.source == "g2.t" && e.target == "g2.d"), "g2 edge kept: {out}");
}

#[test]
fn remove_edge_into_group_port() {
    // An edge into a group's port is written `grp.inp = a.value`; removeEdge
    // matches that connection by its as-written endpoints and drops it.
    let src = "a = Text { value: \"x\" }\ngrp = Group(inp: String) -> () {\n  t = Debug\n  t.data = self.inp\n}\ngrp.inp = a.value\n";
    let out = apply(src, vec![EditOp::RemoveEdge {
        source: "a".into(), source_port: "value".into(), target: "grp".into(), target_port: "inp".into(), scope_group: None,
    }]);
    assert!(!out.contains("grp.inp = a.value"), "edge into group port removed: {out}");
    { parse_ok(&out); structure(&out) };
}

#[test]
fn add_edge_into_group_input_replaces_driver_no_double() {
    // Reconnecting a group input must replace the existing driver (single-
    // driver), not append a second line. The existing `grp.inp = a.value` line
    // is matched by its as-written endpoints and rewritten in place.
    let src = "a = Text { value: \"x\" }\nc = Text { value: \"y\" }\ngrp = Group(inp: String) -> () {\n  t = Debug\n  t.data = self.inp\n}\ngrp.inp = a.value\n";
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
    let src = "a = Text {\n  value: \"x\"\n}\nc = Text {\n  value: \"y\"\n}\nb = Debug\nb.data = a.value\n";
    let out = apply(src, vec![EditOp::AddEdge {
        source: "c".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None,
    }]);
    assert!(out.contains("b.data = c.value"), "{out}");
    assert!(!out.contains("b.data = a.value"), "old driver removed: {out}");
}

#[test]
fn rename_group_updates_header_and_refs() {
    let src = "grp = Group(inp: String) -> (outp: String) {\n  t = Text { value: \"x\" }\n  self.outp = t.value\n}\nd = Debug\nd.data = grp.outp\n";
    let out = apply(src, vec![EditOp::RenameGroup { group: "grp".into(), new_label: "proc".into() }]);
    assert!(out.contains("proc = Group"), "header renamed: {out}");
    assert!(out.contains("d.data = proc.outp"), "ref renamed: {out}");
    assert!(!out.contains("grp"), "no stale grp: {out}");
}

#[test]
fn rename_group_by_scoped_id_disambiguates_same_local_label() {
    // CONTRACT: RenameGroup carries the group's SCOPED id, so a group is identified
    // unambiguously even when two groups share a local label in different scopes.
    // Here `Inner` exists under both `A` and `B`; renaming A.Inner must rename ONLY
    // that one (the old bare-label op would have hit AmbiguousId and refused).
    let src = "A = Group() -> () {\n  Inner = Group() -> () {\n    d = Debug {}\n  }\n}\nB = Group() -> () {\n  Inner = Group() -> () {\n    e = Debug {}\n  }\n}\n";
    let out = apply(src, vec![EditOp::RenameGroup { group: "A.Inner".into(), new_label: "Renamed".into() }]);
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "A.Renamed"), "A.Inner renamed to A.Renamed: {out}");
    assert!(p.nodes.iter().any(|n| n.id == "B.Inner"), "B.Inner untouched: {out}");
    assert!(!p.nodes.iter().any(|n| n.id == "A.Inner"), "no stale A.Inner: {out}");
}

#[test]
fn update_group_ports_by_scoped_id_disambiguates_same_local_label() {
    // Same scoped-id contract for UpdateGroupPorts: editing ports on A.Inner when a
    // B.Inner also exists must resolve to exactly A.Inner, not error as ambiguous.
    let src = "A = Group() -> () {\n  Inner = Group() -> () {\n    d = Debug {}\n  }\n}\nB = Group() -> () {\n  Inner = Group() -> () {\n    e = Debug {}\n  }\n}\n";
    let out = apply(src, vec![EditOp::UpdateGroupPorts {
        group: "A.Inner".into(),
        inputs: vec![PortSig { name: "x".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![],
    }]);
    parse_ok(&out);
    // A.Inner's header gained the input at its OWN (2-space) indent; B.Inner empty.
    // (No `-> ()` is emitted for an empty output clause.)
    assert_eq!(out, "A = Group() -> () {\n  Inner = Group(x: String) {\n    d = Debug {}\n  }\n}\nB = Group() -> () {\n  Inner = Group() -> () {\n    e = Debug {}\n  }\n}\n", "A.Inner got the port at its own indent, B.Inner untouched: {out}");
}

#[test]
fn set_group_description_inserts_as_first_body_line() {
    // A group's description is a plain `# ...` comment as its first body line.
    let src = "g = Group() -> () {\n  t = Text { value: \"x\" }\n}\n";
    let out = apply(src, vec![EditOp::SetGroupDescription { group: "g".into(), description: Some("does things".into()) }]);
    assert!(out.contains("# does things"), "{out}");
    parse_ok(&out);
    // it is the FIRST body line (before the child)
    let desc_pos = out.find("# does things").unwrap();
    let child_pos = out.find("t = Text").unwrap();
    assert!(desc_pos < child_pos, "description precedes the first child: {out}");
}

#[test]
fn set_group_description_on_inline_body_does_not_swallow_close_brace() {
    // Regression: adding a description to an inline `{}` group (the exact shape
    // AddGroup produces) inserted the `# ...` comment after the `{` but left the
    // `}` on the comment line, so the comment SWALLOWED the brace
    // (`# hello}` is one comment) and the group was left unclosed. The body must
    // be opened so the `}` drops to its own line.
    let out = apply("g = Group() -> () {}\n",
        vec![EditOp::SetGroupDescription { group: "g".into(), description: Some("hello".into()) }]);
    assert!(out.contains("# hello"), "{out}");
    parse_ok(&out); // brace-balance check would fail on the swallowed `}`
    // A follow-up edit into the now-well-formed body works.
    let out2 = apply(&out, vec![EditOp::AddNode { id: "n".into(), node_type: "Debug".into(), parent_group: Some("g".into()) }]);
    parse_ok(&out2);
    assert!(out2.contains("n = Debug"), "child added into the group: {out2}");
}

#[test]
fn batch_is_atomic_and_sequential() {
    // remove-then-add edge in one batch (the classic chain) lands correctly.
    let src = "a = Text {\n  value: \"x\"\n}\nc = Text {\n  value: \"y\"\n}\nb = Debug\nb.data = a.value\n";
    let out = apply(src, vec![
        EditOp::RemoveEdge { source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None },
        EditOp::AddEdge { source: "c".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None },
    ]);
    assert!(out.contains("b.data = c.value"), "{out}");
}

#[test]
fn add_node_into_group() {
    let src = "grp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n";
    let out = apply(src, vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: Some("grp".into()) }]);
    parse_ok(&out);
    assert!(out.contains("d = Debug {}"), "{out}");
    let d_line = out.lines().find(|l| l.contains("d = Debug")).unwrap();
    assert!(d_line.starts_with("  "), "indented inside group: {d_line:?}");
}

#[test]
fn add_and_remove_group() {
    let src = "t = Text { value: \"x\" }\n";
    let added = apply(src, vec![EditOp::AddGroup { label: "grp".into(), parent_group: None }]);
    assert!(added.contains("grp = Group() -> () {}"), "{added}");
    parse_ok(&added);
    let removed = apply(&added, vec![EditOp::RemoveGroup { group: "grp".into() }]);
    assert!(!removed.contains("grp = Group"), "{removed}");
}

#[test]
fn move_node_into_group_then_out() {
    let src = "grp = Group() -> () {\n  x = Text { value: \"a\" }\n}\nt = Text { value: \"b\" }\n";
    let moved_in = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved_in);
    // t now lives inside grp (the grp.t scoped id exists after re-parse).
    let p = structure(&moved_in);
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t moved into grp: {moved_in}");
}

#[test]
fn move_into_current_scope_is_a_noop_not_a_self_collision() {
    // CONTRACT: the editor identifies both the moved decl and the target group by
    // their SCOPED id (never a bare label). A move into the scope a decl already
    // lives in is a no-op: source unchanged, no error. (The old label-based op made
    // the no-op guard compare a bare target against a scoped parent path, so at
    // nesting depth >= 2 the guard misfired and the no-op move ran destructively,
    // see `move_into_current_scope_depth2_is_a_clean_noop` below.)
    let nested = apply(
        "MyGroup_2 = Group() -> () {\n  MyGroup = Group() -> () {\n    debug_4 = Debug {}\n  }\n}\n",
        vec![EditOp::MoveGroupScope { group: "MyGroup_2.MyGroup".into(), target_group: Some("MyGroup_2".into()) }],
    );
    assert_eq!(nested, "MyGroup_2 = Group() -> () {\n  MyGroup = Group() -> () {\n    debug_4 = Debug {}\n  }\n}\n", "no-op move leaves source unchanged: {nested:?}");
    parse_ok(&nested);

    let root = apply(
        "a = Debug {}\nb = Debug {}\n",
        vec![EditOp::MoveNodeScope { node: "a".into(), target_group: None }],
    );
    assert_eq!(root, "a = Debug {}\nb = Debug {}\n", "no-op move to current (root) scope unchanged: {root:?}");
    parse_ok(&root);

    // A GENUINE collision (a DIFFERENT decl with the same local id already in the
    // target) must still fail loud, not be swallowed as a no-op.
    let err = apply_edits(
        "g = Group() -> () {\n  d = Debug {}\n}\nd = Debug {}\n",
        None,
        "Untitled",
        &[EditOp::MoveNodeScope { node: "d".into(), target_group: Some("g".into()) }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::DuplicateId(_)), "real same-id collision still errors: {err:?}");
}

#[test]
fn move_into_current_scope_depth2_is_a_clean_noop() {
    // Regression for the depth-2 corruption: a node TWO levels deep, "moved" into
    // the scope it already lives in (target = the SCOPED id of its current parent).
    // The old bare-label op compared "MyGroup" (bare) against "MyGroup_2.MyGroup"
    // (scoped parent), never matched, and the no-op move ran, mangling indentation
    // and braces. With scoped-id targets the no-op guard matches exactly: unchanged.
    let src = "MyGroup_2 = Group() -> () {\n  MyGroup = Group() -> () {\n    debug_4 = Debug {}\n  }\n}\n";
    let out = apply(src, vec![EditOp::MoveNodeScope { node: "MyGroup_2.MyGroup.debug_4".into(), target_group: Some("MyGroup_2.MyGroup".into()) }]);
    assert_eq!(out, src, "depth-2 no-op must leave source byte-identical (no indent/brace corruption): {out:?}");
    parse_ok(&out);
}

#[test]
fn move_node_out_one_level_at_depth2() {
    // A real depth-2 move: debug_4 from MyGroup_2.MyGroup out to MyGroup_2 (target
    // = the scoped id of MyGroup_2). Confirms the scoped-target contract relocates
    // correctly, not just no-ops.
    let src = "MyGroup_2 = Group() -> () {\n  MyGroup = Group() -> () {\n    debug_4 = Debug {}\n  }\n}\n";
    let out = apply(src, vec![EditOp::MoveNodeScope { node: "MyGroup_2.MyGroup.debug_4".into(), target_group: Some("MyGroup_2".into()) }]);
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "MyGroup_2.debug_4"), "debug_4 moved up to MyGroup_2: {out}");
    assert!(!p.nodes.iter().any(|n| n.id == "MyGroup_2.MyGroup.debug_4"), "no longer in inner group: {out}");
}

#[test]
fn move_node_into_single_line_empty_group() {
    // A freshly-added group is `grp = Group() -> () {}` (one line, inline `{}`).
    // Moving a node into it must split the body open and nest the node INSIDE,
    // not splice it above the header. Regression: the inline `{}` case put the
    // node before the group line, leaving an orphan and an empty group.
    let src = "grp = Group() -> () {}\nt = Text { value: \"b\" }\n";
    let moved = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved);
    let p = structure(&moved);
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t must nest inside grp: {moved}");
    // The node must not survive at top level (that would be the orphan bug).
    assert!(!p.nodes.iter().any(|n| n.id == "t"), "no top-level orphan t: {moved}");
}


#[test]
fn move_node_into_group_with_multiline_signature() {
    // A group with a multi-line port signature ending in an inline `{}` spans
    // several lines, but its body is still inline. The insertion must split the
    // `{}` open and nest the node inside, NOT splice it into the signature.
    let src = "grp = Group(\n  a: String\n) -> () {}\nt = Text { value: \"b\" }\n";
    let moved = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("grp".into()) }]);
    parse_ok(&moved);
    let p = structure(&moved);
    assert!(p.nodes.iter().any(|n| n.id == "grp.t"), "t must nest inside grp: {moved}");
    assert!(!p.nodes.iter().any(|n| n.id == "t"), "no top-level orphan t: {moved}");
}

#[test]
fn update_node_ports_rewrites_signature() {
    let src = "g = Group() -> () {\n  x = Text { value: \"a\" }\n}\n";
    let out = apply(src, vec![EditOp::UpdateGroupPorts {
        group: "g".into(),
        inputs: vec![PortSig { name: "inp".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![PortSig { name: "outp".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(out.contains("g = Group(inp: String) -> (outp: String)"), "{out}");
    parse_ok(&out);
}

#[test]
fn update_node_ports_on_node_with_body_preserves_body() {
    // Regression: rewriting a NODE's ports must replace the signature in place
    // and keep the body, not prepend a second `id = Type` (which parsed clean as
    // two nodes and shipped corrupted source). parse_ok now asserts no header
    // carries two `=`.
    let src = "n = Text {\n  value: \"x\"\n}\n";
    let out = apply(src, vec![EditOp::UpdateNodePorts {
        node: "n".into(),
        inputs: vec![PortSig { name: "inp".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![],
    }]);
    assert!(out.contains("n = Text(inp: String)"), "signature rewritten: {out}");
    assert!(out.contains("value: \"x\""), "body preserved: {out}");
    assert_eq!(out.matches("Text").count(), 1, "no duplicated Type head: {out}");
    parse_ok(&out);
}


#[test]
fn update_node_ports_drops_connections_on_removed_ports() {
    // Deleting a port must take its wires with it: the new signature is the
    // single source of the decl's ports, and a leftover `x.y = n.removed`
    // fails validation on the next build (the exact bug: deleting the python
    // node's `stop` output left `self.done = exec_python_1.stop` behind).
    let src = "n = Text() -> (kept: String, removed: String)\nd = Debug { }\nd.data = n.removed\nd2 = Debug { }\nd2.data = n.kept\n";
    let out = apply(src, vec![EditOp::UpdateNodePorts {
        node: "n".into(),
        inputs: vec![],
        outputs: vec![PortSig { name: "kept".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(!out.contains("n.removed"), "wire on the removed port must die: {out}");
    assert!(out.contains("d2.data = n.kept"), "wire on the kept port survives: {out}");
    parse_ok(&out);
}

#[test]
fn update_node_ports_drops_every_fanout_wire_of_a_removed_output() {
    // An output port fans out to many targets; removing the port must drop
    // EVERY wire it feeds, while a sibling port's fan-out is untouched.
    let src = "n = Text() -> (kept: String, removed: String)\nd1 = Debug { }\nd1.data = n.removed\nd2 = Debug { }\nd2.data = n.removed\nd3 = Debug { }\nd3.data = n.removed\nk1 = Debug { }\nk1.data = n.kept\nk2 = Debug { }\nk2.data = n.kept\n";
    let out = apply(src, vec![EditOp::UpdateNodePorts {
        node: "n".into(),
        inputs: vec![],
        outputs: vec![PortSig { name: "kept".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(!out.contains("n.removed"), "all three fan-out wires must die: {out}");
    assert_eq!(out.matches("= n.kept").count(), 2, "both kept-port wires survive: {out}");
    parse_ok(&out);
}

#[test]
fn update_node_ports_drops_incoming_connection_on_removed_input() {
    let src = "src_1 = Text() -> (value: String)\nn = Text(a: String, b: String)\nn.a = src_1.value\nn.b = src_1.value\n";
    let out = apply(src, vec![EditOp::UpdateNodePorts {
        node: "n".into(),
        inputs: vec![PortSig { name: "a".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![],
    }]);
    assert!(!out.contains("n.b ="), "incoming wire on the removed input must die: {out}");
    assert!(out.contains("n.a = src_1.value"), "kept input's wire survives: {out}");
    parse_ok(&out);
}

#[test]
fn update_node_ports_keeps_config_origin_lines() {
    // `n.key = "value"` shares the connection SYNTAX but is a config field
    // (one endpoint); the dangling-wire sweep must never eat it.
    let src = "n = Text(a: String)\nn.value = \"hello\"\n";
    let out = apply(src, vec![EditOp::UpdateNodePorts {
        node: "n".into(),
        inputs: vec![],
        outputs: vec![],
    }]);
    assert!(out.contains("n.value = \"hello\""), "config-origin line survives: {out}");
    parse_ok(&out);
}

#[test]
fn update_group_ports_drops_self_wiring_on_removed_ports() {
    // Inside the body, `self` direction flips: `self.out = child.x` writes an
    // OUTPUT, `child.y = self.in` reads an INPUT. Removing either port must
    // drop its boundary wiring; the kept port's wiring survives.
    let src = "g = Group(inp: String) -> (outp: String, gone: String) {\n  x = Text(v: String) -> (value: String)\n  x.v = self.inp\n  self.outp = x.value\n  self.gone = x.value\n}\n";
    let out = apply(src, vec![EditOp::UpdateGroupPorts {
        group: "g".into(),
        inputs: vec![PortSig { name: "inp".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![PortSig { name: "outp".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(!out.contains("self.gone"), "removed output's boundary wiring must die: {out}");
    assert!(out.contains("self.outp = x.value"), "kept output wiring survives: {out}");
    assert!(out.contains("x.v = self.inp"), "kept input wiring survives: {out}");
    parse_ok(&out);
}

#[test]
fn update_loop_ports_keeps_reserved_done_and_index() {
    // A Loop's implicit `self.done` (write) and `self.index` (read) are
    // reserved ports OUTSIDE the signature; the sweep must not treat them as
    // dangling. Removing the loop's `stop`-feeding output elsewhere is what
    // the gesture does; here we shrink the signature and assert the implicit
    // wiring survives while a removed iter input's wiring dies.
    let src = "l = Loop(items: List[String], extra: String) -> (acc: String) {\n  over: [\n  \"items\"\n]\n  x = Text(v: String, n: Number, e: String) -> (value: String, stop: Boolean)\n  x.v = self.items\n  x.n = self.index\n  x.e = self.extra\n  self.acc = x.value\n  self.done = x.stop\n}\nl2 = Debug { }\nl2.data = l.acc\n";
    let out = apply(src, vec![EditOp::UpdateLoopPorts {
        loop_id: "l".into(),
        inputs: vec![PortSig { name: "items".into(), required: true, port_type: Some("List[String]".into()) }],
        outputs: vec![PortSig { name: "acc".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(out.contains("self.done = x.stop"), "reserved self.done survives: {out}");
    assert!(out.contains("x.n = self.index"), "reserved self.index survives: {out}");
    assert!(!out.contains("self.extra"), "removed input's boundary wiring must die: {out}");
    assert!(out.contains("x.v = self.items"), "kept over-port wiring survives: {out}");
    assert!(out.contains("l2.data = l.acc"), "outer leg on a kept port survives: {out}");
    parse_ok(&out);
}

#[test]
fn carry_to_gather_batch_drops_the_carry_input_and_its_wires() {
    // The editor's "Make gather output" gesture: one batch flipping the carry
    // list AND removing the paired input from the signature. The input's
    // wires (the parent-scope seed, the body's `self.acc` read) die with it;
    // the gather output's wiring survives.
    let src = "seed = Text() -> (value: Number)\nl = Loop(acc: Number, items: List[Number]) -> (acc: Number) {\n  over: [\n  \"items\"\n]\n  carry: [\n  \"acc\"\n]\n  x = Text(v: Number, a: Number) -> (acc: Number)\n  x.v = self.items\n  x.a = self.acc\n  self.acc = x.acc\n}\nl.acc = seed.value\nd = Debug { }\nd.data = l.acc\n";
    let out = apply(src, vec![
        EditOp::SetLoopConfig { loop_id: "l".into(), key: "carry".into(), value: "[]".into() },
        EditOp::UpdateLoopPorts {
            loop_id: "l".into(),
            inputs: vec![PortSig { name: "items".into(), required: true, port_type: Some("List[Number]".into()) }],
            outputs: vec![PortSig { name: "acc".into(), required: true, port_type: Some("Number".into()) }],
        },
    ]);
    assert!(!out.contains("l.acc = seed.value"), "the carry input's seed wire must die: {out}");
    assert!(!out.contains("x.a = self.acc"), "the body read of the carry input must die: {out}");
    assert!(out.contains("self.acc = x.acc"), "the gather output's boundary wiring survives: {out}");
    assert!(out.contains("d.data = l.acc"), "the outer leg on the output survives: {out}");
    assert!(out.contains("x.v = self.items"), "the over port's wiring survives: {out}");
    parse_ok(&out);
}

#[test]
fn update_loop_ports_keeps_wires_of_a_surviving_carry() {
    // A carry input is SYNTHESIZED (not in the signature); a ports update on
    // the loop must not sweep its wires while the carry list still pairs it
    // with an output. Here the signature never declares `acc` as input, yet
    // its seed wire and body read survive an unrelated ports rewrite.
    let src = "seed = Text() -> (value: Number)\nl = Loop(items: List[Number]) -> (acc: Number) {\n  over: [\n  \"items\"\n]\n  carry: [\n  \"acc\"\n]\n  x = Text(v: Number, a: Number) -> (acc: Number)\n  x.v = self.items\n  x.a = self.acc\n  self.acc = x.acc\n}\nl.acc = seed.value\n";
    let out = apply(src, vec![EditOp::UpdateLoopPorts {
        loop_id: "l".into(),
        inputs: vec![PortSig { name: "items".into(), required: true, port_type: Some("List[Number]".into()) }],
        outputs: vec![PortSig { name: "acc".into(), required: true, port_type: Some("Number".into()) }],
    }]);
    assert!(out.contains("l.acc = seed.value"), "surviving carry's seed wire stays: {out}");
    assert!(out.contains("x.a = self.acc"), "surviving carry's body read stays: {out}");
    parse_ok(&out);
}

#[test]
fn update_node_ports_drops_inline_expr_wire_on_removed_input() {
    // An inline-expr RHS (`n.a = Text{...}.value`) is a wire with ONE endpoint,
    // not a config-origin line; removing its input port must take it with the
    // port (the s_id.is_none early-return previously kept it, then validation
    // failed on the next build). A kept input's inline-expr wire survives.
    let src = "n = Debug(a: String, b: String)\nn.a = Upper { text: \"x\" }.out\nn.b = Upper { text: \"y\" }.out\n";
    let out = apply(src, vec![EditOp::UpdateNodePorts {
        node: "n".into(),
        inputs: vec![PortSig { name: "a".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![],
    }]);
    assert!(!out.contains("n.b ="), "inline-expr wire on the removed input must die: {out}");
    assert!(out.contains("n.a = Upper { text: \"x\" }.out"), "inline-expr wire on the kept input survives: {out}");
    parse_ok(&out);
}

#[test]
fn add_edge_inside_loop_body_replaces_existing_driver() {
    // addEdge in a Loop body must REMOVE the existing driver of the target port
    // (one driver per input), exactly as in a Group body. A Group-only scope
    // resolver in find_connection silently failed the removal and appended a
    // SECOND driver line (two drivers on one port).
    let src = "l = Loop(items: List[String]) -> (acc: String) {\n  over: [\n  \"items\"\n]\n  a = Text() -> (value: String)\n  b = Text() -> (value: String)\n  x = Text(v: String) -> (value: String)\n  x.v = a.value\n  self.acc = x.value\n}\n";
    let out = apply(src, vec![EditOp::AddEdge {
        scope_group: Some("l".into()),
        source: "b".into(), source_port: "value".into(),
        target: "x".into(), target_port: "v".into(),
    }]);
    assert_eq!(out.matches("x.v =").count(), 1, "exactly one driver on x.v after re-drive: {out}");
    assert!(out.contains("x.v = b.value"), "the new driver replaced the old: {out}");
    assert!(!out.contains("x.v = a.value"), "the old driver was removed: {out}");
    parse_ok(&out);
}

#[test]
fn remove_edge_inside_loop_body_finds_the_wire() {
    // removeEdge in a Loop body must resolve its scope as a Loop, not error out
    // on a Group kind-mismatch (which would surface ConnectionNotFound).
    let src = "l = Loop(items: List[String]) -> (acc: String) {\n  over: [\n  \"items\"\n]\n  a = Text() -> (value: String)\n  x = Text(v: String) -> (value: String)\n  x.v = a.value\n  self.acc = x.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveEdge {
        scope_group: Some("l".into()),
        source: "a".into(), source_port: "value".into(),
        target: "x".into(), target_port: "v".into(),
    }]);
    assert!(!out.contains("x.v = a.value"), "the loop-body wire was removed: {out}");
    assert!(out.contains("self.acc = x.value"), "unrelated loop wiring stays: {out}");
    parse_ok(&out);
}

#[test]
fn update_group_ports_on_anonymous_root_sweeps_self_wiring() {
    // An anonymous root group (no `name =`) has no local id, so it can't be
    // named by a parent leg; the parent-scope sweep is skipped, but its own
    // `self.<port>` boundary wiring is still swept when a port is removed.
    let src = "Group(raw: String) -> (outp: String, gone: String) {\n  t = Text(v: String) -> (value: String)\n  t.v = self.raw\n  self.outp = t.value\n  self.gone = t.value\n}\n";
    let out = apply(src, vec![EditOp::UpdateGroupPorts {
        group: "Untitled".into(),
        inputs: vec![PortSig { name: "raw".into(), required: true, port_type: Some("String".into()) }],
        outputs: vec![PortSig { name: "outp".into(), required: true, port_type: Some("String".into()) }],
    }]);
    assert!(!out.contains("self.gone"), "removed output's boundary wiring dies on an anon root: {out}");
    assert!(out.contains("self.outp = t.value"), "kept output wiring survives: {out}");
    assert!(out.contains("t.v = self.raw"), "kept input wiring survives: {out}");
    parse_ok(&out);
}

#[test]
fn add_edge_allows_an_outer_ref_from_inside_a_group() {
    // Weft connections may reference an OUTER node (the lowering's outward
    // scoping rule); the editor must accept an edge inside `g` whose source is
    // a top-level node with no `g.`-scoped namesake.
    let src = "a = Text() -> (value: String)\ng = Group() -> () {\n  sink = Debug { }\n}\n";
    let out = apply(src, vec![EditOp::AddEdge {
        scope_group: Some("g".into()),
        source: "a".into(), source_port: "value".into(),
        target: "sink".into(), target_port: "data".into(),
    }]);
    assert!(out.contains("sink.data = a.value"), "outer-ref edge added: {out}");
    parse_ok(&out);
}

#[test]
fn add_edge_rejects_a_dotted_endpoint_ref() {
    // An endpoint id must be a single segment; a dotted ref would author a
    // 3-segment endpoint the grammar silently truncates (mis-wiring). The
    // edit refuses it loudly instead.
    let src = "g = Group() -> () {\n  inner = Text() -> (value: String)\n}\nsink = Debug { }\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::AddEdge {
        scope_group: None,
        source: "g.inner".into(), source_port: "value".into(),
        target: "sink".into(), target_port: "data".into(),
    }]);
    assert!(err.is_err(), "a dotted endpoint ref must be refused: {err:?}");
}

#[test]
fn add_edge_rejects_an_intermediate_ancestor_ref() {
    // Connection scoping is TWO probes: immediate-scope child, else bare
    // top-level. There is NO intermediate-ancestor resolution. A ref `y`
    // inside `outer.inner` must NOT resolve to `outer.y` (the compiler would
    // leave the authored `sink.data = y.value` dangling), so the edit refuses.
    let src = "outer = Group() -> () {\n  y = Text() -> (value: String)\n  inner = Group() -> () {\n    sink = Debug { }\n  }\n}\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::AddEdge {
        scope_group: Some("outer.inner".into()),
        source: "y".into(), source_port: "value".into(),
        target: "sink".into(), target_port: "data".into(),
    }]);
    assert!(err.is_err(), "an intermediate-ancestor ref must be refused: {err:?}");
}

#[test]
fn add_edge_ref_is_scope_local_not_file_wide() {
    // An endpoint ref inside a group is SCOPE-LOCAL: `inner` in scope `g` means
    // `g.inner`. A same-named `inner` at top level must NOT satisfy the
    // endpoint requirement, and the file-wide resolver must not reject the
    // scoped ref as ambiguous because the local name is reused elsewhere.
    let src = "inner = Text() -> (value: String)\ng = Group() -> () {\n  inner = Text() -> (value: String)\n  sink = Debug { }\n}\n";
    let out = apply(src, vec![EditOp::AddEdge {
        scope_group: Some("g".into()),
        source: "inner".into(), source_port: "value".into(),
        target: "sink".into(), target_port: "data".into(),
    }]);
    assert!(out.contains("sink.data = inner.value"), "edge added with scope-local refs: {out}");
    parse_ok(&out);
}

#[test]
fn remove_node_drops_edges_in_other_scopes() {
    // Regression: RemoveNode must drop edges referencing the node in ANY scope,
    // including a child group's body, not only the node's own scope.
    let src = "a = Text { value: \"x\" }\ng = Group() -> () {\n  b = Debug\n  b.data = a.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveNode { node: "a".into() }]);
    assert!(!out.contains("b.data = a.value"), "cross-scope edge to removed node dropped: {out}");
    assert!(!out.contains("a = Text"), "{out}");
    parse_ok(&out);
}

#[test]
fn remove_scoped_node_spares_same_name_other_scope_edges() {
    // Regression: scope-aware matching. Removing `grp.a` must drop only grp.a's
    // edge, never a same-named top-level `a`'s edge.
    let src = "a = Text { value: \"x\" }\nb = Debug\nb.data = a.value\ngrp = Group() -> () {\n  a = Text { value: \"y\" }\n  c = Debug\n  c.data = a.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveNode { node: "grp.a".into() }]);
    assert!(out.contains("b.data = a.value"), "top-level a's edge survives: {out}");
    assert!(!out.contains("c.data = a.value"), "grp.a's edge removed: {out}");
    parse_ok(&out);
}

#[test]
fn rename_group_to_taken_id_fails_loud() {
    // Regression: renaming onto an existing id would make two same-id decls and
    // self-referential edges. Must fail loud instead.
    let src = "d = Debug\ngrp = Group() -> (o: String) {\n  t = Text { value: \"x\" }\n  self.o = t.value\n}\nd.data = grp.o\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::RenameGroup { group: "grp".into(), new_label: "d".into() }]).unwrap_err();
    assert!(matches!(err, EditError::DuplicateId(_)), "{err:?}");
}

#[test]
fn move_node_preserves_heredoc_content() {
    // Regression: moving a node with a heredoc must NOT re-indent the heredoc's
    // literal body (only the decl's own layout shifts).
    let src = "g = Group() -> () {}\nt = Code {\n  src: ```\nline1\n  indented\n```\n}\n";
    let out = apply(src, vec![EditOp::MoveNodeScope { node: "t".into(), target_group: Some("g".into()) }]);
    assert!(out.contains("\nline1\n"), "heredoc line 'line1' not re-indented: {out}");
    assert!(out.contains("\n  indented\n"), "heredoc line '  indented' unchanged: {out}");
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "g.t"), "node moved into g: {out}");
}

#[test]
fn unparseable_edit_fails_loud_not_silent() {
    // Targeting a node that doesn't exist is a hard error, never a silent no-op
    // that loses the user's intent.
    let src = "t = Text { value: \"x\" }\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::RemoveNode { node: "nope".into() }]).unwrap_err();
    assert!(matches!(err, EditError::NodeNotFound(_)), "{err:?}");
}

#[test]
fn set_config_must_not_clobber_a_wiring_edge() {
    // Regression: a configurable input port can be driven by EITHER a config
    // value (`b.data = "lit"`, one endpoint) OR a real edge (`b.data = a.value`,
    // two endpoints). SetConfig/RemoveConfig only edit the one-endpoint
    // config-origin form; a real edge into the same port must be left intact, not
    // rewritten into a literal or deleted. The bug was a missing one-endpoint
    // guard in `find_connection_origin_field` (it matched the edge by target).
    let src = "a = Text { value: \"x\" }\nb = Debug\nb.data = a.value\n";
    // SetConfig on the wired port must ADD a config field, never touch the edge.
    let (out, _) = apply_edits(src, None, "Untitled",
        &[EditOp::SetConfig { node: "b".into(), key: "data".into(), value: "\"lit\"".into() }]).expect("set config");
    assert!(out.contains("b.data = a.value"), "wiring edge survives SetConfig: {out}");
    parse_ok(&out);
    // RemoveConfig on the wired port must NOT delete the edge.
    let (out2, _) = apply_edits(src, None, "Untitled",
        &[EditOp::RemoveConfig { node: "b".into(), key: "data".into() }]).expect("remove config");
    assert!(out2.contains("b.data = a.value"), "wiring edge survives RemoveConfig: {out2}");
    parse_ok(&out2);

    // The genuine config-origin form (one endpoint) is still edited in place.
    let cfg = "b = Debug\nb.data = \"old\"\n";
    let (out3, _) = apply_edits(cfg, None, "Untitled",
        &[EditOp::SetConfig { node: "b".into(), key: "data".into(), value: "\"new\"".into() }]).expect("set config-origin");
    assert!(out3.contains("b.data = \"new\"") && !out3.contains("\"old\""), "config-origin edited in place: {out3}");
    parse_ok(&out3);
}

#[test]
fn set_config_must_not_clobber_an_inline_expression() {
    // An inline-expr RHS (`b.data = Upper{...}.out`) is a node+edge SYNTHESIS, not
    // a config field: the lowering makes a real `Upper` node and wires it. It has
    // exactly ONE endpoint (the inline-expr is a node, not a 2nd endpoint), so the
    // old one-endpoint guard misclassified it as a config field and SetConfig/
    // RemoveConfig destroyed the subgraph. The shared `connection_is_config_origin`
    // (mirroring the lowering's `literal_config_fill`) now rejects an inline-expr
    // RHS, so the wiring survives.
    let src = "b = Debug\nb.data = Upper { text: \"hi\" }.out\n";
    // SetConfig adds a separate config field; the inline-expr wiring is untouched.
    let (out, _) = apply_edits(src, None, "Untitled",
        &[EditOp::SetConfig { node: "b".into(), key: "data".into(), value: "\"lit\"".into() }]).expect("set config");
    assert!(out.contains("Upper { text: \"hi\" }.out"), "inline-expr subgraph survives SetConfig: {out}");
    parse_ok(&out);
    // RemoveConfig must NOT delete the inline-expr wiring.
    let (out2, _) = apply_edits(src, None, "Untitled",
        &[EditOp::RemoveConfig { node: "b".into(), key: "data".into() }]).expect("remove config");
    assert!(out2.contains("Upper { text: \"hi\" }.out"), "inline-expr subgraph survives RemoveConfig: {out2}");
    parse_ok(&out2);
}

#[test]
fn edit_inside_anonymous_root_resolves_by_source_id() {
    // Regression (the bug that motivated dropping the relabel pass): a standalone
    // anonymous component file is rendered with its members scoped under the
    // FILENAME-derived id (`MyCleaner.child`). An edit targeting that scoped id
    // must resolve, because the editor resolves against the SAME source id the
    // render used. Before, render renamed `__include_root__` -> `MyCleaner` but
    // the edit path kept the sentinel, so `MyCleaner.child` was "node not found".
    //
    // Two same-local `strip` nodes (one at root, one in a nested group) make the
    // bare local AMBIGUOUS, so resolution can only succeed via the exact scoped
    // id, which proves the source-id prefix is load-bearing (not bare-local luck).
    let src = "Group(raw: String) -> (cleaned: String) {\n  strip = Text { value: \"x\" }\n  sub = Group() -> (o: String) {\n    strip = Text { value: \"z\" }\n    self.o = strip.value\n  }\n  self.cleaned = strip.value\n}\n";
    // Edit the ROOT child by its source-id-scoped id, the form the editor sends.
    let (out, _) = apply_edits(
        src,
        None,
        "MyCleaner",
        &[EditOp::SetConfig { node: "MyCleaner.strip".into(), key: "value".into(), value: "\"y\"".into() }],
    )
    .expect("edit by source-id-scoped id resolves");
    assert!(out.contains("value: \"y\""), "root child config updated: {out}");
    assert!(out.contains("value: \"z\""), "nested same-local child untouched: {out}");
    parse_ok(&out);

    // The SAME scoped id under the wrong source id must NOT resolve: `MyCleaner`
    // isn't the root prefix, and the bare local `strip` is ambiguous, so it's a
    // hard not-found rather than a silent guess.
    let err = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::SetConfig { node: "MyCleaner.strip".into(), key: "value".into(), value: "\"y\"".into() }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::AmbiguousId(_) | EditError::NodeNotFound(_)), "{err:?}");
}

#[test]
fn remove_group_ungroups_children_up_one_scope() {
    // Deleting a group keeps its nodes: they move up to the parent scope.
    let src = "grp = Group(inp: String) -> (outp: String) {\n  t = Text { value: \"x\" }\n  self.outp = t.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveGroup { group: "grp".into() }]);
    assert!(!out.contains("grp = Group"), "group header gone: {out}");
    assert!(out.contains("t = Text"), "child survives (ungroup): {out}");
    assert!(!out.contains("self.outp"), "group's own boundary wiring dropped: {out}");
    parse_ok(&out);
}

#[test]
fn remove_group_keeps_inner_edge_when_child_shadows_group_name() {
    // Regression: ungroup classified a connection as boundary wiring if any
    // endpoint id equalled the GROUP's local name. But inside the body that name
    // resolves to a CHILD of the same name, so a real wire between two children
    // (`d.data = g.value`, where a child is also named `g`) was wrongly dropped.
    // Only `self.*` is internal boundary wiring; the child-to-child edge survives.
    let src = "g = Group() -> () {\n  g = Text { value: \"x\" }\n  d = Debug\n  d.data = g.value\n}\n";
    let out = apply(src, vec![EditOp::RemoveGroup { group: "g".into() }]);
    assert!(out.contains("d.data = g.value"), "inner child-to-child edge survives ungroup: {out}");
    assert!(out.contains("g = Text"), "child `g` survives: {out}");
    parse_ok(&out);
}

#[test]
fn remove_nested_group_reindents_every_surviving_child() {
    // Regression: ungroup de-indented children by swapping the inner indent for the
    // group's, but a child's `to_string()` has NO indent on its first line (it's a
    // sibling trivia token), so only the FIRST surviving child (which inherits the
    // splice point's whitespace) landed right; every later child fell to column 0.
    // `parse_ok` can't catch it (whitespace is trivia), so we assert exact layout:
    // every surviving child sits at the group's own indent (2 spaces here).
    let src = "outer = Group() -> () {\n  mid = Group() -> () {\n    a = Text {}\n    b = Debug\n    c = Debug\n  }\n}\n";
    let out = apply(src, vec![EditOp::RemoveGroup { group: "outer.mid".into() }]);
    assert_eq!(out, "outer = Group() -> () {\n  a = Text {}\n  b = Debug\n  c = Debug\n}\n", "every ungrouped child re-indented to the group's level: {out:?}");
    parse_ok(&out);
}

#[test]
fn remove_nested_group_preserves_multiline_heredoc_child() {
    // A surviving child carrying a MULTI-LINE heredoc must not have its body lines
    // re-indented (literal content) AND the heredoc must not desync the per-line
    // indent of the children below it. Both are handled in one heredoc-aware pass.
    let src = "outer = Group() -> () {\n  mid = Group() -> () {\n    a = Text { value: ```\nl1\nl2\n``` }\n    b = Debug\n  }\n}\n";
    let out = apply(src, vec![EditOp::RemoveGroup { group: "outer.mid".into() }]);
    assert_eq!(out, "outer = Group() -> () {\n  a = Text { value: ```\nl1\nl2\n``` }\n  b = Debug\n}\n", "heredoc body verbatim, sibling below at group indent: {out:?}");
    parse_ok(&out);
}

#[test]
fn remove_empty_group_leaves_no_dangling_blank_line() {
    // Ungrouping a group with no surviving children (empty, or only boundary
    // wiring) must collapse the slot cleanly, not leave a whitespace-only indented
    // line where the group used to be. `parse_ok` can't catch this (the stray line
    // is trivia), so we assert exact bytes.
    let nested = apply(
        "outer = Group() -> () {\n  mid = Group() -> () {}\n}\n",
        vec![EditOp::RemoveGroup { group: "outer.mid".into() }],
    );
    assert_eq!(nested, "outer = Group() -> () {\n}\n", "no dangling indented blank line: {nested:?}");
    parse_ok(&nested);

    // Deleting the only top-level group collapses to the file's trailing newline.
    // (`detach_with_leading_ws` removes the decl and any leading whitespace; the
    // decl's trailing line terminator is a separate root token that stays. A lone
    // newline is a clean empty-ish file, NOT the indented-blank-line corruption
    // the nested case had.)
    let toplevel = apply(
        "g = Group() -> () {}\n",
        vec![EditOp::RemoveGroup { group: "g".into() }],
    );
    assert_eq!(toplevel, "\n", "top-level empty ungroup leaves only the trailing newline: {toplevel:?}");
    parse_ok(&toplevel);
}

#[test]
fn remove_top_level_group_with_preceding_sibling_keeps_separators() {
    // Regression: a top-level group's leading newline is the group node's OWN first
    // child (not a sibling), so splicing the group out removed that newline and the
    // ungrouped children glued onto the PRECEDING decl (`d = Debugx = Text...`).
    // The fix re-emits the group's leading line break and indents every child, so
    // the children land on their own lines. `parse_ok` can't catch the glue (it's
    // whitespace), so assert exact bytes. The bug only shows when a decl PRECEDES
    // the group (so the group is not the file's first element); this test puts a
    // `d = Debug` in front to hit it.
    let after_decl = apply(
        "d = Debug\ng = Group() -> (out: String) {\n  x = Text { value: \"x\" }\n  self.out = x.value\n}\n",
        vec![EditOp::RemoveGroup { group: "g".into() }],
    );
    assert_eq!(after_decl, "d = Debug\nx = Text { value: \"x\" }\n", "ungrouped child sits on its own line after the preceding decl: {after_decl:?}");
    parse_ok(&after_decl);

    // Ungrouping the SECOND of two top-level groups: the first group's `}` must not
    // glue onto the surviving child.
    let second_of_two = apply(
        "a = Group() -> () {\n  p = Debug\n}\nb = Group() -> () {\n  q = Debug\n}\n",
        vec![EditOp::RemoveGroup { group: "b".into() }],
    );
    assert_eq!(second_of_two, "a = Group() -> () {\n  p = Debug\n}\nq = Debug\n", "second group's child on its own line: {second_of_two:?}");
    parse_ok(&second_of_two);
}

#[test]
fn ambiguous_bare_id_fails_loud() {
    // Two `t` in different groups: a bare `t` op must error, never guess + splice
    // the wrong node.
    let src = "g1 = Group() -> () {\n  t = Text { value: \"a\" }\n}\ng2 = Group() -> () {\n  t = Text { value: \"b\" }\n}\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"z\"".into() }]).unwrap_err();
    assert!(matches!(err, EditError::AmbiguousId(_)), "{err:?}");
}

#[test]
fn add_duplicate_id_fails_loud() {
    let src = "t = Text { value: \"x\" }\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::AddNode { id: "t".into(), node_type: "Debug".into(), parent_group: None }]).unwrap_err();
    assert!(matches!(err, EditError::DuplicateId(_)), "{err:?}");
}

#[test]
fn add_edge_to_missing_endpoint_fails_loud() {
    let src = "b = Debug\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::AddEdge {
        source: "ghost".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None,
    }]).unwrap_err();
    assert!(matches!(err, EditError::NodeNotFound(_)), "{err:?}");
}

#[test]
fn rename_group_to_empty_fails_loud() {
    let src = "grp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::RenameGroup { group: "grp".into(), new_label: "".into() }]).unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "{err:?}");
}

#[test]
fn set_label_on_group_fails_loud() {
    // A setLabel targeting a container would write a `_label` field into
    // the group body, which the lowering rejects as a compile error.
    // The edit must fail at edit time with a kind mismatch instead.
    let src = "grp = Group() -> () {\n  t = Text { value: \"x\" }\n}\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::SetLabel { node: "grp".into(), label: Some("Hi".into()) }]).unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "{err:?}");
}

#[test]
fn set_group_description_replaces_and_clears() {
    // Replace an existing description, then clear it (None removes the line).
    let src = "g = Group() {\n  # old\n  t = Text {}\n}\n";
    let replaced = apply(src, vec![EditOp::SetGroupDescription { group: "g".into(), description: Some("new".into()) }]);
    assert!(replaced.contains("# new"), "{replaced}");
    assert!(!replaced.contains("# old"), "{replaced}");
    parse_ok(&replaced);
    let cleared = apply(&replaced, vec![EditOp::SetGroupDescription { group: "g".into(), description: None }]);
    assert!(!cleared.contains("# new"), "description cleared: {cleared}");
    parse_ok(&cleared);
}

/// Every op that writes a caller-supplied string into the source as an
/// IDENTIFIER refuses anything that is not one, at the door. Without this a
/// string carrying whitespace / a newline / braces stops being one identifier
/// and becomes extra source: the file would gain declarations nobody asked
/// for. Covers the id, the node type, and a rename target.
#[test]
fn ops_refuse_an_identifier_that_would_inject_source() {
    let src = "a = Text {}\n";
    let inject = "b = Text {}\nevil";

    let cases: Vec<EditOp> = vec![
        EditOp::AddNode {
            id: inject.into(),
            node_type: "Text".into(),
            parent_group: None,
        },
        EditOp::AddNode {
            id: "ok".into(),
            node_type: "Text {}\nevil = Text {}\nx".into(),
            parent_group: None,
        },
        EditOp::AddGroup { label: inject.into(), parent_group: None },
        EditOp::AddLoop { label: inject.into(), parent_group: None },
    ];
    for op in cases {
        let err = apply_edits(src, None, "Untitled", &[op.clone()]).unwrap_err();
        assert!(
            matches!(err, EditError::InvalidArgument(_)),
            "op {op:?} must be refused, got {err:?}"
        );
    }

    // A rename target is written as an IDENT too.
    let with_group = "g = Group() {\n  t = Text {}\n}\n";
    let err = apply_edits(
        with_group,
        None,
        "Untitled",
        &[EditOp::RenameGroup { group: "g".into(), new_label: inject.into() }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "got {err:?}");

    // And the ordinary identifiers still work.
    let ok = apply(src, vec![EditOp::AddNode {
        id: "b_2-x".into(),
        node_type: "Text".into(),
        parent_group: None,
    }]);
    assert!(ok.contains("b_2-x = Text {}"), "{ok}");
    parse_ok(&ok);
}

/// A NAME reaches the source in more places than a decl's id: a connection's
/// PORT names, a config KEY, and a port signature's name/type all get written
/// into the file and reparsed. Every one of them is guarded, or the same
/// injection just moves to whichever door was left open.
#[test]
fn every_name_written_into_source_is_guarded() {
    let two = "a = Text {}\nb = Debug {}\n";
    let inject = "x = Text {}\nevil";

    // A connection's port names.
    for op in [
        EditOp::AddEdge {
            source: "a".into(),
            source_port: inject.into(),
            target: "b".into(),
            target_port: "data".into(),
            scope_group: None,
        },
        EditOp::AddEdge {
            source: "a".into(),
            source_port: "value".into(),
            target: "b".into(),
            target_port: inject.into(),
            scope_group: None,
        },
    ] {
        let err = apply_edits(two, None, "Untitled", &[op]).unwrap_err();
        assert!(matches!(err, EditError::InvalidArgument(_)), "port name: {err:?}");
    }

    // A config key.
    let err = apply_edits(
        two,
        None,
        "Untitled",
        &[EditOp::SetConfig { node: "a".into(), key: inject.into(), value: "1".into() }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "config key: {err:?}");

    // A port signature's name, and its type expression.
    let err = apply_edits(
        two,
        None,
        "Untitled",
        &[EditOp::UpdateNodePorts {
            node: "a".into(),
            inputs: vec![PortSig { name: inject.into(), port_type: None, required: true }],
            outputs: vec![],
        }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "port sig name: {err:?}");

    let err = apply_edits(
        two,
        None,
        "Untitled",
        &[EditOp::UpdateNodePorts {
            node: "a".into(),
            inputs: vec![PortSig {
                name: "p".into(),
                port_type: Some("String) -> ()\nevil = Text {}\nq: (r".into()),
                required: true,
            }],
            outputs: vec![],
        }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "port sig type: {err:?}");
}

/// An unterminated OPAQUE token (a bare `[`, an opening heredoc fence, a bare
/// `}`) lexes as one token that balances and carries no newline, yet on reparse
/// swallows the rest of the file. It must be refused wherever a value or a type
/// is written into source: through a config value, and through a port type.
#[test]
fn opaque_token_that_would_swallow_the_file_is_refused() {
    let two = "n = Text {}\nafter = Debug {}\n";

    // Config value. Includes a TRAILING COMMENT (`1 # x`), which on a
    // single-line body runs to end-of-line and swallows the `}`; unterminated
    // opaque tokens (a bare `[`, a heredoc/string fence, an unterminated marker
    // `@file(`); an INTERNALLY-unbalanced array that still ends in `]` (`[[]`,
    // which the lexer pads greedily to EOF); and an unbalanced closer.
    for harmful in [
        "[", "```", "}", "a\nb", "1 # x", "\"", "x)", "\"unterminated",
        "@file(", "@include(\"x\"", "[[]", "[[a]",
        // A `)` inside a marker's string with the real paren unclosed, and a
        // `[`-array whose interior is unbalanced though it ends in `]`: both
        // re-lex greedily and would eat the field's `}`.
        "@file(\"a)", "[[]}", "[\"\nx\"]",
        // An unterminated string whose LAST byte is an ESCAPED quote (never a
        // real close): the scan runs it to end-of-input.
        "\"\\\"", "\"abc\\\"",
    ] {
        let err = apply_edits(
            two,
            None,
            "Untitled",
            &[EditOp::SetConfig { node: "n".into(), key: "value".into(), value: harmful.into() }],
        )
        .unwrap_err();
        assert!(matches!(err, EditError::InvalidArgument(_)), "config value {harmful:?}: {err:?}");
    }
    // Harmless values are still accepted: an operator, terminated literals, a
    // JSON object/array, a marker, a multi-line heredoc (its newlines live
    // inside one token), and a string with an escaped quote.
    for ok in [
        "String | Number",
        "[1, 2]",
        "\"text\"",
        "{ \"a\": 1 }",
        "@file(\"p.txt\")",
        "@include(\"x.weft\")",
        // A `)` inside a marker string with the marker properly closed, and a
        // `]` inside a JSON string: the string-skipping must not miscount these.
        "@file(\"p)q.txt\")",
        "[\"a]b\"]",
        "[{\"a\": 1}, {\"b\": 2}]",
        "{\"a\": [1, 2], \"b\": {\"c\": 3}}",
        "```\ncode\n```",
        "\"has \\\" quote\"",
        "\"a]b[\"",
        "\"ends with escaped backslash \\\\\"",
    ] {
        assert!(
            apply_edits(
                two,
                None,
                "Untitled",
                &[EditOp::SetConfig { node: "n".into(), key: "value".into(), value: ok.into() }],
            )
            .is_ok(),
            "value {ok:?} must be accepted"
        );
    }

    // Port TYPE: an injection through the type position, and a legal type.
    for harmful in ["[", "```", "String) -> ()\nevil = Text {}\nq: (r", "Number, evil: String"] {
        let err = apply_edits(
            two,
            None,
            "Untitled",
            &[EditOp::UpdateNodePorts {
                node: "n".into(),
                inputs: vec![PortSig { name: "p".into(), port_type: Some(harmful.into()), required: true }],
                outputs: vec![],
            }],
        )
        .unwrap_err();
        assert!(matches!(err, EditError::InvalidArgument(_)), "port type {harmful:?}: {err:?}");
    }
    for ok in ["String | Null", "List[Number]", "Dict[String, Number]", "MustOverride"] {
        assert!(
            apply_edits(
                two,
                None,
                "Untitled",
                &[EditOp::UpdateNodePorts {
                    node: "n".into(),
                    inputs: vec![PortSig { name: "p".into(), port_type: Some(ok.into()), required: true }],
                    outputs: vec![],
                }],
            )
            .is_ok(),
            "port type {ok:?} must be accepted"
        );
    }
}

/// `Group`/`Loop`/`true`/`false` are not IDENTs (they lex as keywords/numbers),
/// so `validate_ident` (which asks the lexer) must refuse them wherever a plain
/// name is written; and a node TYPE additionally may not be a reserved type
/// keyword.
#[test]
fn keyword_shaped_names_are_refused() {
    let src = "a = Text {}\n";
    // As a config key (lexes as keyword/number, not IDENT).
    for kw in ["Group", "Loop", "true", "false"] {
        let err = apply_edits(
            src,
            None,
            "Untitled",
            &[EditOp::SetConfig { node: "a".into(), key: kw.into(), value: "1".into() }],
        )
        .unwrap_err();
        assert!(matches!(err, EditError::InvalidArgument(_)), "config key {kw:?}: {err:?}");
    }
    // As a node type: reserved type keywords are refused (they must not be
    // authored as a plain node through this door).
    for kw in ["Group", "Loop", "LoopIn", "LoopOut", "Passthrough"] {
        let err = apply_edits(
            src,
            None,
            "Untitled",
            &[EditOp::AddNode { id: "c".into(), node_type: kw.into(), parent_group: None }],
        )
        .unwrap_err();
        assert!(matches!(err, EditError::InvalidArgument(_)), "node type {kw:?}: {err:?}");
    }
}

/// A name the LANGUAGE reserves must be refused by the op, or it authors a
/// file the compiler then refuses. The membership rule is the compiler's
/// (`is_reserved_local`), not a second list living in the editor.
#[test]
fn ops_refuse_a_reserved_local_id() {
    let src = "a = Text {}\n";
    for bad in ["self", "Group", "Loop", "LoopIn", "Passthrough", "a__b"] {
        let err = apply_edits(
            src,
            None,
            "Untitled",
            &[EditOp::AddNode {
                id: bad.into(),
                node_type: "Text".into(),
                parent_group: None,
            }],
        )
        .unwrap_err();
        assert!(matches!(err, EditError::InvalidArgument(_)), "reserved {bad:?}: {err:?}");
    }
}

/// A description carrying a newline would parse as extra source (injected
/// nodes / connections) spliced into the group body. It is refused loudly at
/// the op boundary, never applied.
#[test]
fn set_group_description_refuses_a_newline() {
    let src = "g = Group() {\n  t = Text {}\n}\n";
    let err = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::SetGroupDescription {
            group: "g".into(),
            description: Some("note\nx = ExecPython { code: \"boom\" }".into()),
        }],
    )
    .unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "got {err:?}");
}

/// Within ONE batch (all ops on one tree, no reparse between them), a
/// just-inserted description is a real GROUP_DESC, so a following clear finds
/// and removes it instead of no-oping and leaving the line behind. This is
/// what the synthetic-wrapper GROUP_DESC splice buys.
#[test]
fn set_group_description_insert_then_clear_in_one_batch_clears() {
    let src = "g = Group() {\n  t = Text {}\n}\n";
    let out = apply(
        src,
        vec![
            EditOp::SetGroupDescription { group: "g".into(), description: Some("temp".into()) },
            EditOp::SetGroupDescription { group: "g".into(), description: None },
        ],
    );
    assert!(!out.contains("# temp"), "the batch's clear must remove the just-inserted line: {out}");
    parse_ok(&out);
}

// ── formatting / comment / error-line preservation guarantees ───────────────
// Formatting, comment, and error-line preservation, the group-layout matrix,
// and an edit-level soak: the proof that the corruption bug class is
// structurally impossible.

#[test]
fn editing_one_node_leaves_other_bytes_identical() {
    // A user's hand-alignment, blank-line sections, and comments OUTSIDE the
    // edited region must come back byte-for-byte: minimal-span re-serialization
    // re-emits only the changed subtree and leaves every other byte untouched.
    let src = "\
# a header comment

a   =   Text {
  value:   \"keep my spacing\"
}


# a section divider, two blank lines above

b = Debug {
  data: \"target\"
}
";
    let out = apply(src, vec![EditOp::SetConfig { node: "b".into(), key: "data".into(), value: "\"changed\"".into() }]);
    // Everything before `b`'s body is byte-identical.
    let untouched_prefix = src.split("b = Debug").next().unwrap();
    assert!(out.starts_with(untouched_prefix), "bytes before the edit are identical:\n{out}");
    assert!(out.contains("value:   \"keep my spacing\""), "hand-alignment preserved: {out}");
    assert!(out.contains("# a section divider, two blank lines above"), "comment preserved: {out}");
    assert!(out.contains("data: \"changed\""), "the edit landed: {out}");
}

#[test]
fn error_line_numbers_outside_edit_are_stable() {
    // An edit to a later line must not shift the line count of earlier lines.
    // We assert by line position: the line index of an early marker is unchanged.
    let src = "a = Text {\n  value: \"x\"\n}\nb = Debug {\n  data: \"y\"\n}\n";
    let a_line_before = src.lines().position(|l| l.contains("a = Text")).unwrap();
    let out = apply(src, vec![EditOp::SetConfig { node: "b".into(), key: "data".into(), value: "\"z\"".into() }]);
    let a_line_after = out.lines().position(|l| l.contains("a = Text")).unwrap();
    assert_eq!(a_line_before, a_line_after, "earlier line numbers unchanged by a later edit:\n{out}");
}

#[test]
fn comment_adjacent_to_edit_survives() {
    // A comment immediately above and a trailing comment on the edited node both
    // survive the edit (trivia attachment is structural, not luck).
    let src = "# documents t\nt = Text {\n  value: \"x\"  # inline note\n}\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "value".into(), value: "\"y\"".into() }]);
    assert!(out.contains("# documents t"), "leading comment survives: {out}");
    assert!(out.contains("# inline note"), "trailing inline comment survives: {out}");
    assert!(out.contains("value: \"y\""), "{out}");
}

#[test]
fn every_group_layout_handles_add_child_without_corruption() {
    // Every group layout (inline `{}`, multi-line, same-line and separate-line
    // post-body output ports, multi-line signature) must accept a new child
    // INSIDE the body and reparse cleanly with the child nested.
    let layouts = [
        "g = Group() -> () {}\n",                              // inline empty
        "g = Group() {\n  x = Text {}\n}\n",                  // multi-line bare close
        "g = Group(\n  a: String\n) -> () {}\n",             // multi-line signature
    ];
    for layout in layouts {
        let out = apply(layout, vec![EditOp::AddNode {
            id: "added".into(), node_type: "Debug".into(), parent_group: Some("g".into()),
        }]);
        parse_ok(&out);
        let p = structure(&out);
        assert!(p.nodes.iter().any(|n| n.id == "g.added"), "child nested in g for layout {layout:?}:\n{out}");
        assert!(!p.nodes.iter().any(|n| n.id == "added"), "no top-level orphan for layout {layout:?}:\n{out}");
    }
}

#[test]
fn every_group_layout_handles_remove_child_and_ungroup() {
    let layouts = [
        "g = Group() {\n  x = Text {}\n}\n",
    ];
    for layout in layouts {
        let out = apply(layout, vec![EditOp::RemoveGroup { group: "g".into() }]);
        parse_ok(&out);
        assert!(!out.contains("Group("), "group declaration gone for {layout:?}:\n{out}");
        let p = structure(&out);
        assert!(p.nodes.iter().any(|n| n.id == "x"), "child survives ungroup for {layout:?}:\n{out}");
    }
}

#[test]
fn edit_soak_never_corrupts() {
    // Apply random edit sequences to varied shapes; every result must reparse
    // cleanly (no ERROR nodes) and round-trip. This is the corruption-impossible
    // guarantee at the EDIT level (the parse-level soak lives in cst::parser).
    let mut rng = 0x9E3779B97F4A7C15u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let bases = [
        "a = Text { value: \"x\" }\nb = Debug\n",
        "g = Group() -> (out: String) {\n  x = Text {}\n  self.out = x.value\n}\n",
        "a = Text {}\nb = Debug\nb.data = a.value\n",
        // a node with a heredoc + a group with a nested group (gnarly shapes)
        "n = Code {\n  src: ```\nline\n  indented\n```\n}\n",
        "outer = Group() -> () {\n  inner = Group() -> () {\n    y = Debug\n  }\n}\nz = Text {}\n",
        // Loop base: forces the fuzzer through the Loop-specific
        // edit paths (rename/remove/move + config fields in the
        // body). Without a loop in the bases, the seven Loop edit
        // ops are exercised but always target a Group/Node id and
        // are silently rejected by the ContainerKind check.
        "myloop = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n  body = Text {}\n}\n",
        // Mixed Group+Loop base. The earlier loop-only base has no
        // Group target, so MoveLoopScope { Some(target) } never
        // lands successfully. This base gives the fuzzer a real
        // target for the move-into-container path.
        "container = Group() -> () {\n}\nmyloop = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n}\n",
    ];
    // Targets drawn from the ids the bases actually contain (top-level + scoped).
    let targets = ["a", "b", "x", "z", "g", "g.x", "outer", "outer.inner", "outer.inner.y", "myloop", "myloop.body", "container"];
    let pick = |n: u64, slice: &[&str]| slice[(n as usize) % slice.len()].to_string();
    for _ in 0..600 {
        let mut src = bases[(next() as usize) % bases.len()].to_string();
        for _ in 0..4 {
            let op = match next() % 23 {
                0 => EditOp::AddNode { id: format!("n{}", next() % 1000), node_type: "Debug".into(), parent_group: None },
                1 => EditOp::SetConfig { node: pick(next(), &targets), key: "value".into(), value: format!("\"{}\"", next() % 100) },
                2 => EditOp::AddGroup { label: format!("grp{}", next() % 1000), parent_group: None },
                3 => EditOp::RemoveNode { node: pick(next(), &targets) },
                4 => EditOp::SetLabel { node: pick(next(), &targets), label: Some(format!("L{}", next() % 100)) },
                5 => EditOp::AddEdge { source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None },
                6 => EditOp::RemoveEdge { source: "a".into(), source_port: "value".into(), target: "b".into(), target_port: "data".into(), scope_group: None },
                7 => EditOp::RemoveConfig { node: pick(next(), &targets), key: "value".into() },
                8 => EditOp::RemoveGroup { group: pick(next(), &targets) },
                9 => EditOp::RenameGroup { group: pick(next(), &targets), new_label: format!("r{}", next() % 1000) },
                10 => EditOp::MoveNodeScope { node: pick(next(), &targets), target_group: Some(pick(next(), &targets)) },
                11 => EditOp::MoveNodeScope { node: pick(next(), &targets), target_group: None },
                12 => EditOp::MoveGroupScope { group: pick(next(), &targets), target_group: Some(pick(next(), &targets)) },
                13 => EditOp::UpdateNodePorts { node: pick(next(), &targets), inputs: vec![PortSig { name: "i".into(), required: true, port_type: Some("String".into()) }], outputs: vec![] },
                14 => EditOp::SetGroupDescription { group: pick(next(), &targets), description: Some(format!("d{}", next() % 100)) },
                // Loop ops. Some will fail kind-mismatch checks (the
                // Group-targeted RenameGroup at arm 9 etc. already do
                // the same); successful Loop edits exercise the
                // body-with-config-fields paths the Group ops don't
                // cover.
                15 => EditOp::AddLoop { label: format!("lp{}", next() % 1000), parent_group: None },
                16 => EditOp::RemoveLoop { loop_id: pick(next(), &targets) },
                17 => EditOp::RenameLoop { loop_id: pick(next(), &targets), new_label: format!("lr{}", next() % 1000) },
                18 => EditOp::MoveLoopScope { loop_id: pick(next(), &targets), target_group: Some(pick(next(), &targets)) },
                19 => EditOp::MoveLoopScope { loop_id: pick(next(), &targets), target_group: None },
                20 => EditOp::UpdateLoopPorts { loop_id: pick(next(), &targets), inputs: vec![PortSig { name: "items".into(), required: true, port_type: Some("List[String]".into()) }], outputs: vec![PortSig { name: "results".into(), required: true, port_type: Some("List[String | Null]".into()) }] },
                21 => EditOp::SetLoopConfig { loop_id: pick(next(), &targets), key: "max_iters".into(), value: format!("{}", next() % 100) },
                _ => EditOp::RemoveLoopConfig { loop_id: pick(next(), &targets), key: "max_iters".into() },
            };
            // Ops that legitimately error (bad target, collision, ...) are skipped;
            // every SUCCESSFUL edit must satisfy the full structural invariants.
            if let Ok((new_src, _)) = apply_edits(&src, None, "Untitled", &[op]) {
                parse_ok(&new_src);
                src = new_src;
            }
        }
    }
}

// ── Layer-2 wire-shape: EditOp serde round-trip ─────────────────────────────
// The editor (TS) emits EditOps as JSON with a camelCase `op` tag + camelCase
// fields. A renamed variant/field silently breaks that contract; these pin it.

#[test]
fn editop_wire_shape_round_trips() {
    use serde_json::json;
    // (json the TS side emits, expected variant predicate)
    let cases: Vec<serde_json::Value> = vec![
        json!({"op":"setConfig","node":"n","key":"k","value":"\"v\""}),
        json!({"op":"removeConfig","node":"n","key":"k"}),
        json!({"op":"setLabel","node":"n","label":"L"}),
        json!({"op":"setLabel","node":"n","label":null}),
        json!({"op":"addNode","id":"n","nodeType":"Debug","parentGroup":null}),
        json!({"op":"removeNode","node":"n"}),
        json!({"op":"addEdge","source":"a","sourcePort":"value","target":"b","targetPort":"data","scopeGroup":null}),
        json!({"op":"removeEdge","source":"a","sourcePort":"value","target":"b","targetPort":"data","scopeGroup":"g"}),
        json!({"op":"addGroup","label":"g","parentGroup":null}),
        json!({"op":"removeGroup","group":"g"}),
        json!({"op":"renameGroup","group":"a","newLabel":"b"}),
        json!({"op":"moveNodeScope","node":"n","targetGroup":"g"}),
        json!({"op":"moveGroupScope","group":"g","targetGroup":null}),
        json!({"op":"updateNodePorts","node":"n","inputs":[{"name":"i","required":true,"portType":"String"}],"outputs":[]}),
        json!({"op":"updateGroupPorts","group":"g","inputs":[],"outputs":[]}),
        json!({"op":"setGroupDescription","group":"g","description":"d"}),
        // Loop ops: same wire-contract guarantee as the Group/Node
        // ops above. A serde rename here breaks the webview silently.
        json!({"op":"addLoop","label":"l","parentGroup":null}),
        json!({"op":"removeLoop","loopId":"l"}),
        json!({"op":"renameLoop","loopId":"a","newLabel":"b"}),
        json!({"op":"moveLoopScope","loopId":"l","targetGroup":"g"}),
        json!({"op":"moveLoopScope","loopId":"l","targetGroup":null}),
        json!({"op":"updateLoopPorts","loopId":"l","inputs":[{"name":"i","required":true,"portType":"List[String]"}],"outputs":[{"name":"o","required":true,"portType":"List[String | Null]"}]}),
        json!({"op":"setLoopConfig","loopId":"l","key":"parallel","value":"true"}),
        json!({"op":"removeLoopConfig","loopId":"l","key":"max_iters"}),
    ];
    for c in cases {
        let op: EditOp = serde_json::from_value(c.clone()).unwrap_or_else(|e| panic!("deserialize {c}: {e}"));
        // FULL round-trip: the re-serialized JSON must equal the input exactly,
        // so a renamed field/tag (the wire contract with the TS extension) fails
        // here, not silently as a defaulted-None.
        let back = serde_json::to_value(&op).unwrap();
        assert_eq!(back, c, "wire shape changed on round-trip");
    }
    // The deleted variant must NOT deserialize (guards against its return).
    assert!(serde_json::from_value::<EditOp>(serde_json::json!({"op":"setProjectMeta","name":"x","description":null})).is_err());
}

// ── round-2 regression tests: exact output, one per fixed bug ───────────────

#[test]
fn set_config_does_not_double_indent_or_compound() {
    // Replacing a field must swap only the value, not re-emit leading indent
    // (which doubled it, compounding on every edit).
    let src = "n = Text {\n  value: \"x\"\n}\n";
    let o1 = apply(src, vec![EditOp::SetConfig { node: "n".into(), key: "value".into(), value: "\"y\"".into() }]);
    assert_eq!(o1, "n = Text {\n  value: \"y\"\n}\n");
    let o2 = apply(&o1, vec![EditOp::SetConfig { node: "n".into(), key: "value".into(), value: "\"z\"".into() }]);
    assert_eq!(o2, "n = Text {\n  value: \"z\"\n}\n", "indent must not compound");
}

#[test]
fn set_config_preserves_trailing_comment() {
    let src = "n = Text {\n  value: \"x\"  # keep me\n}\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "n".into(), key: "value".into(), value: "\"y\"".into() }]);
    assert_eq!(out, "n = Text {\n  value: \"y\"  # keep me\n}\n");
}

#[test]
fn set_config_connection_origin_keeps_newline() {
    // Editing `t.style = "a"` must not eat the leading newline.
    let src = "t = Text { value: \"x\" }\nt.style = \"a\"\n";
    let out = apply(src, vec![EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"b\"".into() }]);
    assert_eq!(out, "t = Text { value: \"x\" }\nt.style = \"b\"\n");
}

#[test]
fn move_group_into_group_nests_and_reparses() {
    let src = "outer = Group() -> () {\n  x = Debug\n}\ng = Group() -> () {\n  y = Debug\n}\n";
    let out = apply(src, vec![EditOp::MoveGroupScope { group: "g".into(), target_group: Some("outer".into()) }]);
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "outer.g"), "g nested under outer: {out}");
    assert!(p.nodes.iter().any(|n| n.id == "outer.g.y"), "g's child re-scoped: {out}");
    assert!(!p.nodes.iter().any(|n| n.id == "g"), "no top-level orphan g: {out}");
}

#[test]
fn set_label_rejects_triple_backtick_value() {
    // A multi-line label containing ``` can't be encoded as a heredoc (no fence
    // escape), so it's a loud error, not silently-corrupt source.
    let src = "n = Text { value: \"x\" }\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::SetLabel { node: "n".into(), label: Some("a\n```\nb".into()) }]).unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "{err:?}");
}

#[test]
fn insert_and_replace_config_value_agree_on_containment() {
    // Symmetry: a value that would break out of its field (an unbalanced `}`)
    // must be rejected by BOTH paths. Before, only the in-place REPLACE path was
    // gated; the INSERT path (a new field with no prior value) re-parsed the line
    // and a stray `}` closed the node body early, silently corrupting the tree.
    let bad = "}}}";
    // INSERT path: the key doesn't exist yet -> insert_field.
    let insert_err = apply_edits("n = Text {}\n", None, "Untitled",
        &[EditOp::SetConfig { node: "n".into(), key: "k".into(), value: bad.into() }]).unwrap_err();
    assert!(matches!(insert_err, EditError::InvalidArgument(_)), "insert must reject uncontained value: {insert_err:?}");
    // REPLACE path: the key already exists -> replace_value_after.
    let replace_err = apply_edits("n = Text { k: \"old\" }\n", None, "Untitled",
        &[EditOp::SetConfig { node: "n".into(), key: "k".into(), value: bad.into() }]).unwrap_err();
    assert!(matches!(replace_err, EditError::InvalidArgument(_)), "replace must reject uncontained value: {replace_err:?}");

    // And BOTH accept a well-formed multi-line heredoc value (its newlines live
    // inside one opaque token, so they don't break containment).
    let (inserted, _) = apply_edits("n = Text {}\n", None, "Untitled",
        &[EditOp::SetLabel { node: "n".into(), label: Some("line1\nline2".into()) }]).expect("insert heredoc label");
    parse_ok(&inserted);
    assert!(inserted.contains("line1\nline2"), "heredoc body present: {inserted}");
    let (replaced, _) = apply_edits("n = Text { _label: \"old\"\n  value: \"x\" }\n", None, "Untitled",
        &[EditOp::SetLabel { node: "n".into(), label: Some("line1\nline2".into()) }]).expect("replace heredoc label");
    parse_ok(&replaced);
    assert!(replaced.contains("line1\nline2"), "heredoc body present: {replaced}");
}

// ── round-3 regression tests ────────────────────────────────────────────────

#[test]
fn set_config_empty_value_does_not_collapse_brace() {
    // `value:` with no value: editing it must produce a clean field, not pull
    // the `}` onto the value line.
    let out = apply("n = Text {\n  value:\n}\n", vec![EditOp::SetConfig { node: "n".into(), key: "value".into(), value: "\"x\"".into() }]);
    assert_eq!(out, "n = Text {\n  value: \"x\"\n}\n");
}

#[test]
fn set_config_oneliner_inline_value_keeps_space_before_brace() {
    let out = apply("g = Template { template: Upper { text: \"hi\" }.out }\n",
        vec![EditOp::SetConfig { node: "g".into(), key: "template".into(), value: "\"plain\"".into() }]);
    assert_eq!(out, "g = Template { template: \"plain\" }\n");
}

#[test]
fn set_config_rejects_value_that_breaks_containment() {
    // The value gate forbids content that would ESCAPE the field and corrupt the
    // tree: an unbalanced closing brace (would close the body early) and a raw
    // newline (would split the line). Lossless content that stays in value
    // position (an operator like `|` in a type expr, even an NBSP) is allowed:
    // it round-trips and the compiler flags an invalid value downstream.
    let src = "n = Text { value: \"x\" }\n";
    let reject = |v: &str| apply_edits(src, None, "Untitled", &[EditOp::SetConfig { node: "n".into(), key: "value".into(), value: v.into() }]);
    assert!(matches!(reject("}"), Err(EditError::InvalidArgument(_))), "bare close brace");
    assert!(matches!(reject("a\nb"), Err(EditError::InvalidArgument(_))), "raw newline");
    // A union type expression is legitimate value content, NOT rejected.
    assert!(reject("String | Number").is_ok(), "union type value must be accepted");
}

#[test]
fn add_node_with_free_local_in_target_scope_succeeds() {
    // `c` is free in `a.b` even though a top-level `c` exists (exact-scoped
    // membership, not bare-local-anywhere).
    let out = apply("c = Text {}\na = Group() -> () {\n  b = Group() -> () {}\n}\n",
        vec![EditOp::AddNode { id: "c".into(), node_type: "Debug".into(), parent_group: Some("a.b".into()) }]);
    parse_ok(&out);
    assert!(structure(&out).nodes.iter().any(|n| n.id == "a.b.c"));
}

#[test]
fn move_node_into_occupied_scope_fails_loud() {
    let src = "g = Group() -> () {\n  x = Debug\n}\nx = Text { value: \"a\" }\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::MoveNodeScope { node: "x".into(), target_group: Some("g".into()) }]).unwrap_err();
    assert!(matches!(err, EditError::DuplicateId(_)), "{err:?}");
}

#[test]
fn move_node_takes_its_connection_origin_config() {
    // `x.style = "bold"` is x's config (written as a connection); moving x takes
    // it along, not stranded in the old scope.
    let out = apply("g = Group() -> () {}\nx = Text { value: \"a\" }\nx.style = \"bold\"\n",
        vec![EditOp::MoveNodeScope { node: "x".into(), target_group: Some("g".into()) }]);
    parse_ok(&out);
    assert!(out.contains("x.style = \"bold\""), "config travelled: {out}");
    // and it's now INSIDE the group (after the moved node), not at top level
    let after_open = out.split_once('{').map(|(_, r)| r).unwrap_or("");
    assert!(after_open.contains("x.style"), "config is inside the group: {out}");
}

// ── round-4 edit regressions ────────────────────────────────────────────────

#[test]
fn move_node_wired_across_scope_refused() {
    // x is wired by a top-level edge; moving it into g would dangle that edge
    // (same-scope-only), so the move is refused loudly (matches the graph view's
    // pre-move guard).
    let src = "g = Group() -> () {}\nx = Text { value: \"a\" }\ny = Debug\ny.data = x.value\n";
    let err = apply_edits(src, None, "Untitled", &[EditOp::MoveNodeScope { node: "x".into(), target_group: Some("g".into()) }]).unwrap_err();
    assert!(matches!(err, EditError::InvalidArgument(_)), "{err:?}");
}

#[test]
fn move_node_into_inline_empty_group_no_glue() {
    // Moving into an inline `{}` group opens the body cleanly, no glued brace line.
    let out = apply("g = Group() -> () {}\nx = Text { value: \"a\" }\n",
        vec![EditOp::MoveNodeScope { node: "x".into(), target_group: Some("g".into()) }]);
    assert_eq!(out, "g = Group() -> () {\n  x = Text { value: \"a\" }\n}\n");
    parse_ok(&out);
}

#[test]
fn add_node_into_inline_empty_group_no_glue() {
    let out = apply("g = Group() -> () {}\n",
        vec![EditOp::AddNode { id: "x".into(), node_type: "Debug".into(), parent_group: Some("g".into()) }]);
    parse_ok(&out);
    assert!(out.contains("{\n"), "body opened on its own line: {out:?}");
    assert!(!out.contains("{  "), "no glue onto the open brace: {out:?}");
}

#[test]
fn add_node_into_nested_inline_empty_group_indents_close_brace() {
    // Opening a NESTED single-line body must drop its `}` to the GROUP's own
    // indent, not to column 0. The old `insert_before_close` only prepended a
    // newline, so `C`'s close brace landed at column 0 (valid-but-corrupt source
    // that compounds on later edits). Byte-exact to pin the indentation.
    let src = "A = Group() -> () {\n  C = Group() -> () {}\n}\n";
    let out = apply(src, vec![EditOp::AddNode { id: "d".into(), node_type: "Debug".into(), parent_group: Some("A.C".into()) }]);
    assert_eq!(out, "A = Group() -> () {\n  C = Group() -> () {\n    d = Debug {}\n  }\n}\n", "C's close brace at its own 2-space indent: {out:?}");
    parse_ok(&out);
}

// ─── Loop edit op tests ─────────────────────────────────────────────────────

#[test]
fn add_loop_at_top_level() {
    let src = "x = Text { value: \"a\" }\n";
    let out = apply(src, vec![EditOp::AddLoop { label: "my_loop".into(), parent_group: None }]);
    parse_ok(&out);
    assert!(out.contains("my_loop = Loop() -> () {"), "loop added: {out:?}");
    // Default body is empty: parallel defaults to false (sequential).
    assert!(!out.contains("parallel:"), "default config is empty: {out:?}");
}

#[test]
fn add_loop_inside_group() {
    let src = "g = Group() -> () {\n  x = Text { value: \"a\" }\n}\n";
    let out = apply(src, vec![EditOp::AddLoop { label: "inner".into(), parent_group: Some("g".into()) }]);
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "g.inner"), "inner loop nested in g: {out}");
}

#[test]
fn add_node_inside_loop() {
    let src = "my = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n}\n";
    let out = apply(src, vec![EditOp::AddNode { id: "p".into(), node_type: "Text".into(), parent_group: Some("my".into()) }]);
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "my.p"), "p nested in my loop: {out}");
}

#[test]
fn remove_loop_ungroups_children() {
    let src = "my = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n  body = Text {}\n}\n";
    let out = apply(src, vec![EditOp::RemoveLoop { loop_id: "my".into() }]);
    parse_ok(&out);
    assert!(!out.contains("Loop("), "loop declaration gone: {out:?}");
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "body"), "child body survives ungroup: {out:?}");
    // The config fields are gone (they belong to the loop).
    assert!(!out.contains("parallel:"), "config field removed: {out:?}");
}

#[test]
fn rename_loop_rewrites_header_and_outside_refs() {
    let src = "my = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n}\nd = Debug\nd.data = my.results\n";
    let out = apply(src, vec![EditOp::RenameLoop {
        loop_id: "my".into(),
        new_label: "renamed".into(),
    }]);
    parse_ok(&out);
    assert!(
        out.contains("renamed = Loop(items: List[String]) -> (results: List[String | Null])"),
        "header renamed: {out}",
    );
    assert!(
        out.contains("d.data = renamed.results"),
        "external reference rewritten: {out}",
    );
    assert!(!out.contains("my ="), "old header gone: {out}");
    assert!(!out.contains("my.results"), "old reference gone: {out}");
}

#[test]
fn rename_loop_scoped_id_disambiguates_same_local_label() {
    // CONTRACT: RenameLoop carries the loop's SCOPED id (mirror of RenameGroup),
    // so a loop is identified unambiguously even when two loops share a local
    // label in different scopes. Here `Inner` is a Loop under both `A` and `B`;
    // renaming A.Inner must rename ONLY that one (a bare-label op would hit
    // AmbiguousId and refuse). This is the depth>=2 case the scoped-id upgrade
    // unlocked.
    let src = "A = Group() -> () {\n  Inner = Loop() -> () {\n    d = Debug {}\n  }\n}\nB = Group() -> () {\n  Inner = Loop() -> () {\n    e = Debug {}\n  }\n}\n";
    let out = apply(src, vec![EditOp::RenameLoop { loop_id: "A.Inner".into(), new_label: "Renamed".into() }]);
    parse_ok(&out);
    let p = structure(&out);
    assert!(p.nodes.iter().any(|n| n.id == "A.Renamed"), "A.Inner loop renamed to A.Renamed: {out}");
    assert!(p.nodes.iter().any(|n| n.id == "B.Inner"), "B.Inner loop untouched: {out}");
    assert!(!p.nodes.iter().any(|n| n.id == "A.Inner"), "no stale A.Inner: {out}");
}

/// `is_err()` alone is too permissive: a regression that errored for
/// the WRONG reason (parse failure, NodeNotFound from a name
/// collision) would pass. Assert the specific kind-mismatch error
/// message so the ContainerKind tightening is locked in.
fn assert_kind_mismatch<T: std::fmt::Debug>(
    result: Result<T, EditError>,
    expected_kind: &str,
    actual_kind: &str,
) {
    let err = result.expect_err("expected kind-mismatch error");
    let msg = format!("{err}");
    assert!(
        msg.contains(&format!("a {actual_kind} decl")) && msg.contains(&format!("not a {expected_kind}")),
        "expected kind-mismatch error citing {expected_kind} vs {actual_kind}, got: {msg}"
    );
}

#[test]
fn rename_group_rejects_loop_target() {
    // RenameGroup is Group-only; routing a loop rename through it is
    // a caller bug, not a fallback we silently absorb. The webview
    // must emit RenameLoop for loops.
    let src = "my = Loop() -> () {\n  parallel: true\n}\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::RenameGroup {
            group: "my".into(),
            new_label: "newname".into(),
        }],
    );
    assert_kind_mismatch(result, "Group", "Loop");
}

#[test]
fn remove_group_rejects_loop_target() {
    // RemoveGroup is Group-only; same rationale as the rename pair.
    let src = "my = Loop() -> () {\n  parallel: true\n  body = Text {}\n}\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::RemoveGroup { group: "my".into() }],
    );
    // The honest kind-mismatch shape: the id EXISTS but is a Loop.
    // A "not found" here would send the user hunting for a typo in
    // an id that is perfectly fine.
    let err = result.expect_err("RemoveGroup on a Loop must error");
    let msg = format!("{err}");
    assert!(msg.contains("my"), "error must name the offending id: {msg}");
    assert!(
        msg.contains("is a Loop decl, not a Group"),
        "error must name the kind mismatch, not claim the container is missing: {msg}"
    );
}

#[test]
fn rename_loop_rejects_group_target() {
    // Symmetric: RenameLoop is Loop-only.
    let src = "g = Group() -> () {\n}\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::RenameLoop {
            loop_id: "g".into(),
            new_label: "other".into(),
        }],
    );
    assert_kind_mismatch(result, "Loop", "Group");
}

#[test]
fn rename_loop_rejects_node_target() {
    let src = "n = Text { value: \"x\" }\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::RenameLoop {
            loop_id: "n".into(),
            new_label: "other".into(),
        }],
    );
    assert_kind_mismatch(result, "Loop", "Node");
}

#[test]
fn update_loop_ports_rewrites_signature() {
    let src = "my = Loop() -> () {\n  parallel: true\n}\n";
    let out = apply(src, vec![EditOp::UpdateLoopPorts {
        loop_id: "my".into(),
        inputs: vec![PortSig { name: "items".into(), required: true, port_type: Some("List[String]".into()) }],
        outputs: vec![PortSig { name: "results".into(), required: true, port_type: Some("List[String | Null]".into()) }],
    }]);
    parse_ok(&out);
    assert!(out.contains("my = Loop(items: List[String]) -> (results: List[String | Null])"), "{out}");
    assert!(out.contains("parallel: true"), "config preserved: {out}");
}

#[test]
fn set_loop_config_inserts_field() {
    let src = "my = Loop() -> () {\n  parallel: true\n}\n";
    let out = apply(src, vec![EditOp::SetLoopConfig {
        loop_id: "my".into(),
        key: "max_iters".into(),
        value: "100".into(),
    }]);
    parse_ok(&out);
    assert!(out.contains("max_iters: 100"), "max_iters inserted: {out}");
    assert!(out.contains("parallel: true"), "existing config kept: {out}");
}

#[test]
fn set_loop_config_replaces_field_in_place() {
    let src = "my = Loop() -> () {\n  parallel: true\n  max_iters: 10\n}\n";
    let out = apply(src, vec![EditOp::SetLoopConfig {
        loop_id: "my".into(),
        key: "max_iters".into(),
        value: "999".into(),
    }]);
    parse_ok(&out);
    assert!(out.contains("max_iters: 999"), "value replaced: {out}");
    assert!(!out.contains("max_iters: 10"), "old value gone: {out}");
}

#[test]
fn remove_loop_config_field() {
    let src = "my = Loop() -> () {\n  parallel: true\n  max_iters: 10\n}\n";
    let out = apply(src, vec![EditOp::RemoveLoopConfig { loop_id: "my".into(), key: "max_iters".into() }]);
    parse_ok(&out);
    assert!(!out.contains("max_iters"), "field removed: {out}");
    assert!(out.contains("parallel: true"), "siblings kept: {out}");
}

#[test]
fn move_node_into_loop_works() {
    let src = "p = Text { value: \"a\" }\nmy = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n}\n";
    let out = apply(src, vec![EditOp::MoveNodeScope { node: "p".into(), target_group: Some("my".into()) }]);
    parse_ok(&out);
    let pp = structure(&out);
    assert!(pp.nodes.iter().any(|n| n.id == "my.p"), "p moved into loop: {out:?}");
    assert!(!pp.nodes.iter().any(|n| n.id == "p"), "no top-level orphan p: {out:?}");
}

#[test]
fn move_loop_scope_relocates_loop_into_group() {
    let src = "outer = Group() -> () {\n}\nmy = Loop() -> () {\n  parallel: true\n}\n";
    let out = apply(src, vec![EditOp::MoveLoopScope {
        loop_id: "my".into(),
        target_group: Some("outer".into()),
    }]);
    parse_ok(&out);
    // The loop's source-level decl now lives inside outer's body.
    // The lowered boundary nodes get scoped ids `outer.my__in` /
    // `outer.my__out` via flatten; here we just check the source
    // shape (the round-trip through structure() is exercised by the
    // group-move tests already).
    assert!(out.contains("outer = Group"), "outer survives: {out:?}");
    assert!(out.contains("my = Loop"), "my still present: {out:?}");
    assert!(out.contains("  my = Loop"), "my is indented under outer: {out:?}");
    assert!(!out.contains("\nmy = Loop"), "no top-level `my = Loop`: {out:?}");
    // Body content must travel with the loop. A regression where
    // move_scope rebuilt only the header would drop this.
    assert!(out.contains("parallel: true"), "loop config preserved across move: {out:?}");
}

#[test]
fn move_loop_scope_blocks_when_wired_across_scope() {
    // Symmetric with MoveGroupScope: Weft is same-scope-only, so a
    // move that would leave external wires referencing the loop
    // across the new scope boundary bails loudly. Exercises the
    // `connections_referencing` path for loops, which the basic
    // relocate test doesn't touch.
    let src = "outer = Group() -> () {\n}\nmy = Loop(items: List[String]) -> (results: List[String | Null]) {\n  parallel: true\n  over: [\"items\"]\n}\nd = Debug\nd.data = my.results\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::MoveLoopScope {
            loop_id: "my".into(),
            target_group: Some("outer".into()),
        }],
    );
    let err = result.expect_err("move with cross-scope wiring must bail");
    let msg = format!("{err}");
    assert!(
        msg.contains("connection") || msg.contains("scope"),
        "error must explain the scope-boundary block: {msg}"
    );
}

#[test]
fn move_group_scope_rejects_loop_target() {
    // MoveGroupScope is Group-only; webview must emit MoveLoopScope
    // for loops. Mirrors the rename/remove tightening.
    let src = "outer = Group() -> () {\n}\nmy = Loop() -> () {\n  parallel: true\n}\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::MoveGroupScope {
            group: "my".into(),
            target_group: Some("outer".into()),
        }],
    );
    assert_kind_mismatch(result, "Group", "Loop");
}

#[test]
fn move_loop_scope_rejects_group_target() {
    let src = "outer = Group() -> () {\n}\ng = Group() -> () {\n}\n";
    let result = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::MoveLoopScope {
            loop_id: "g".into(),
            target_group: Some("outer".into()),
        }],
    );
    assert_kind_mismatch(result, "Loop", "Group");
}

#[test]
fn set_config_multiline_array_replaces_in_place() {
    let src = "my = Loop(test: MustOverride?) {\n  parallel: true\n  over: []\n  carry: []\n}\n";
    let out = apply(src, vec![EditOp::SetLoopConfig {
        loop_id: "my".into(),
        key: "carry".into(),
        value: "[\n  \"test\"\n]".into(),
    }]);
    parse_ok(&out);
    assert_eq!(out.matches("carry:").count(), 1, "carry should appear once: {out}");

    let out2 = apply(&out, vec![EditOp::SetLoopConfig {
        loop_id: "my".into(),
        key: "carry".into(),
        value: "[\n  \"test\",\n  \"foo\"\n]".into(),
    }]);
    parse_ok(&out2);
    assert_eq!(out2.matches("carry:").count(), 1, "second set still one carry: {out2}");
}

#[test]
fn set_config_batched_multiple_keys_replace_in_place() {
    // Simulate what the webview does: one click sends an update containing
    // ALL keys in data.config (because the optimistic update is `{...data.config, [key]: value}`).
    // createNodeUpdateHandler emits one setConfig per key. None should duplicate.
    let src = "my = Loop() -> () {\n  parallel: true\n}\n";
    let out = apply(src, vec![
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "parallel".into(), value: "true".into() },
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "carry".into(), value: "[\n  \"a\"\n]".into() },
    ]);
    parse_ok(&out);
    assert_eq!(out.matches("carry:").count(), 1);
    assert_eq!(out.matches("parallel:").count(), 1);

    let out2 = apply(&out, vec![
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "parallel".into(), value: "true".into() },
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "carry".into(), value: "[\n  \"a\",\n  \"b\"\n]".into() },
    ]);
    parse_ok(&out2);
    assert_eq!(out2.matches("carry:").count(), 1);
    assert_eq!(out2.matches("parallel:").count(), 1);
}

#[test]
fn set_config_collapses_existing_duplicates() {
    // If the source already contains multiple fields with the same key (a
    // legacy file or a state recovered from an earlier bug), set_config
    // edits the first AND removes any duplicates so the next round-trip
    // is clean.
    let src = "my = Loop() -> () {\n  parallel: true\n  carry: []\n  carry: [\"a\"]\n}\n";
    let out = apply(src, vec![
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "carry".into(), value: "[\n  \"b\"\n]".into() },
    ]);
    parse_ok(&out);
    assert_eq!(out.matches("carry:").count(), 1, "duplicates collapsed: {out}");
}

#[test]
fn set_config_same_key_twice_in_batch() {
    // Two setConfig ops for the same key in one batch (two clicks within
    // debounce). The second must REPLACE the value the first set, not
    // append a second field.
    let src = "my = Loop() -> () {\n  parallel: true\n}\n";
    let out = apply(src, vec![
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "carry".into(), value: "[\"a\"]".into() },
        EditOp::SetLoopConfig { loop_id: "my".into(), key: "carry".into(), value: "[\"a\", \"b\"]".into() },
    ]);
    parse_ok(&out);
    assert_eq!(out.matches("carry:").count(), 1, "same-key batch should leave one: {out}");
}

#[test]
fn set_config_node_same_key_twice_in_batch() {
    let src = "t = Text {\n  value: \"x\"\n}\n";
    let out = apply(src, vec![
        EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"a\"".into() },
        EditOp::SetConfig { node: "t".into(), key: "style".into(), value: "\"b\"".into() },
    ]);
    parse_ok(&out);
    assert_eq!(out.matches("style:").count(), 1, "same-key batch should leave one: {out}");
}

#[test]
fn end_to_end_recover_broken_loop_file() {
    // The broken file Quentin saw: duplicate over/carry from a pre-fix
    // race. A single setConfig per key collapses its duplicates.
    let src = "MyLoop = Loop(test: MustOverride?) {\n  parallel: true\n  over: []\n  over: []\n  carry: []\n  carry: [\n  \"test\"\n]\n  carry: [\n  \"test\"\n]\n}\n";
    let out = apply(src, vec![
        EditOp::SetLoopConfig { loop_id: "MyLoop".into(), key: "carry".into(), value: "[\n  \"test\"\n]".into() },
    ]);
    parse_ok(&out);
    assert_eq!(out.matches("carry:").count(), 1, "carry collapsed: {out}");
    // over still duplicated until the user edits it; the next edit on over
    // collapses those too.
    let out2 = apply(&out, vec![
        EditOp::SetLoopConfig { loop_id: "MyLoop".into(), key: "over".into(), value: "[]".into() },
    ]);
    parse_ok(&out2);
    assert_eq!(out2.matches("over:").count(), 1, "over collapsed: {out2}");
}

#[test]
fn set_config_does_not_accumulate_blank_lines() {
    // Each setConfig must layout the inserted field cleanly: one newline
    // between the previous decl's last token and the new one, never more.
    // Without the explicit-layout helper, every iteration leaked an extra
    // blank line into the body (snippet trailing + body trailing).
    let src0 = "MyLoop = Loop() -> () {\n  parallel: true\n}\n";
    let src1 = apply(src0, vec![
        EditOp::SetLoopConfig { loop_id: "MyLoop".into(), key: "over".into(), value: "[\n  \"test\"\n]".into() },
    ]);
    let src2 = apply(&src1, vec![
        EditOp::SetLoopConfig { loop_id: "MyLoop".into(), key: "carry".into(), value: "[]".into() },
    ]);
    let src3 = apply(&src2, vec![
        EditOp::SetLoopConfig { loop_id: "MyLoop".into(), key: "over".into(), value: "[\n  \"test2\"\n]".into() },
    ]);
    parse_ok(&src3);
    // No three-in-a-row newlines: that pattern is the symptom of a leaked
    // blank line between two adjacent decls.
    assert!(!src3.contains("\n\n\n"), "no triple-newline run: {src3}");
    assert!(!src3.contains("\n\n  parallel"), "no blank before parallel: {src3}");
    assert!(!src3.contains("\n\n  over"), "no blank before over: {src3}");
    assert!(!src3.contains("\n\n  carry"), "no blank before carry: {src3}");
}


#[test]
fn update_ports_inside_loop_body_keeps_layout() {
    // Editing a port on a decl that lives inside a loop body must not touch
    // the surrounding decls or the loop's config layout.
    let src = "MyLoop = Loop(numbers: List[Number]?) -> () {\n  carry: []\n  over: [\"numbers\"]\n  exec_python_1 = ExecPython() {}\n}\n\nrange_1 = Range {\n  from: \"0\"\n}\nMyLoop.numbers = range_1.values\n";
    let out = apply(src, vec![
        EditOp::UpdateNodePorts {
            node: "MyLoop.exec_python_1".into(),
            inputs: vec![
                crate::edit::PortSig { name: "number".into(), required: false, port_type: Some("MustOverride".into()) },
            ],
            outputs: vec![],
        },
    ]);
    parse_ok(&out);
    assert!(out.contains("\n  exec_python_1 = ExecPython(number: MustOverride?) {}"), "2-space indent preserved: {out}");
    assert!(out.ends_with("MyLoop.numbers = range_1.values\n"), "trailing connection intact: {out}");
}

#[test]
fn update_ports_at_root_does_not_glue_onto_preceding_connection() {
    // Bug repro: at file root, the parser attaches a decl's leading
    // newline as the decl's OWN first child token. splice_decl removes the
    // whole decl (including that token) on a port edit; if the replacement
    // doesn't re-inject the newline, the new decl glues onto the previous
    // CONNECTION line. Quentin saw this produce
    // `MyLoop.numbers = range_1.valuesexec_python_1 = ExecPython...`.
    let src = "MyLoop = Loop() -> () {\n  parallel: true\n}\n\nrange_1 = Range {\n  from: \"0\"\n}\nMyLoop.numbers = range_1.values\nexec_python_1 = ExecPython() {}\n";
    let out = apply(src, vec![
        EditOp::UpdateNodePorts {
            node: "exec_python_1".into(),
            inputs: vec![
                crate::edit::PortSig { name: "test".into(), required: false, port_type: Some("MustOverride".into()) },
            ],
            outputs: vec![],
        },
    ]);
    parse_ok(&out);
    assert!(!out.contains("range_1.valuesexec_python"), "no glue: {out}");
    assert!(out.contains("range_1.values\nexec_python_1"), "newline kept between connection and decl: {out}");
}

#[test]
fn update_ports_does_not_double_indent() {
    // Bug repro: rebuild_decl used to prepend the decl's current
    // leading_indent AND keep the leading-WS sibling token, doubling the
    // indent on every edit (a 2-space child would become 4, then 8...).
    let src = "MyLoop = Loop() -> () {\n  parallel: true\n  exec_python_1 = ExecPython() {}\n}\n";
    let out = apply(src, vec![
        EditOp::UpdateNodePorts {
            node: "MyLoop.exec_python_1".into(),
            inputs: vec![
                crate::edit::PortSig { name: "test".into(), required: false, port_type: Some("MustOverride".into()) },
            ],
            outputs: vec![],
        },
    ]);
    parse_ok(&out);
    assert!(out.contains("\n  exec_python_1 = ExecPython(test"), "still 2-space: {out}");
    assert!(!out.contains("\n    exec_python_1 = ExecPython(test"), "no 4-space drift: {out}");

    // Edit again. The indent must remain 2-space.
    let out2 = apply(&out, vec![
        EditOp::UpdateNodePorts {
            node: "MyLoop.exec_python_1".into(),
            inputs: vec![
                crate::edit::PortSig { name: "again".into(), required: false, port_type: Some("MustOverride".into()) },
            ],
            outputs: vec![],
        },
    ]);
    parse_ok(&out2);
    assert!(out2.contains("\n  exec_python_1 = ExecPython(again"), "still 2-space after second edit: {out2}");
    assert!(!out2.contains("\n    exec_python_1"), "no drift on repeat: {out2}");
}

#[test]
fn move_scope_into_loop_uses_body_indent() {
    let src = "MyLoop = Loop(numbers: List[Number]?) -> () {\n  parallel: true\n}\nexec_python_1 = ExecPython() {}\n";
    let out = apply(src, vec![
        EditOp::MoveNodeScope {
            node: "exec_python_1".into(),
            target_group: Some("MyLoop".into()),
        },
    ]);
    parse_ok(&out);
    assert!(out.contains("\n  exec_python_1 = ExecPython"), "moved decl gets 2-space indent: {out}");
}

#[test]
fn move_scope_into_loop_with_multiline_carry_field() {
    // The body's last child before `}` is a multi-line JSON value. The
    // helper that finds the body's content indent must read the body's
    // first field, not the JSON value's closing line.
    let src = "MyLoop = Loop(numbers: List[Number]?) -> () {\n  carry: [\n  \"acc\"\n]\n  over: [\n  \"numbers\"\n]\n}\nexec_python_1 = ExecPython() {}\n";
    let out = apply(src, vec![
        EditOp::MoveNodeScope {
            node: "exec_python_1".into(),
            target_group: Some("MyLoop".into()),
        },
    ]);
    parse_ok(&out);
    assert!(out.contains("\n  exec_python_1 = ExecPython"), "moved decl gets body's 2-space indent: {out}");
}

#[test]
fn set_loop_config_preserves_nested_closing_brace_indent() {
    // Inserting a config field into a NESTED container used to splice
    // a bare trailing "\n" before `}`, dropping the brace's own indent
    // to column 0 (and the misalignment persisted across edits).
    let src = "g = Group {\n  my = Loop() -> () {\n    parallel: true\n  }\n}\n";
    let out = apply(src, vec![EditOp::SetLoopConfig {
        loop_id: "g.my".into(),
        key: "max_iters".into(),
        value: "5".into(),
    }]);
    parse_ok(&out);
    assert!(
        out.contains("max_iters: 5\n  }"),
        "the loop's closing brace keeps its 2-space indent: {out}"
    );
}

#[test]
fn move_container_into_own_descendant_is_rejected_before_mutating() {
    // Detach-then-resolve would otherwise fail mid-mutation with a
    // misleading NodeNotFound for a target that exists (it was
    // detached along with the moved subtree).
    let src = "outer = Group {\n  inner = Group {\n    t = Text {}\n  }\n}\n";
    let err = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::MoveGroupScope { group: "outer".into(), target_group: Some("outer.inner".into()) }],
    )
    .expect_err("moving a container into its own descendant must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("itself or its own descendant"),
        "error names the actual problem: {msg}"
    );

    let err2 = apply_edits(
        src,
        None,
        "Untitled",
        &[EditOp::MoveGroupScope { group: "outer".into(), target_group: Some("outer".into()) }],
    )
    .expect_err("moving a container into itself must error");
    let msg2 = format!("{err2}");
    assert!(
        msg2.contains("itself or its own descendant"),
        "error names the actual problem: {msg2}"
    );
}

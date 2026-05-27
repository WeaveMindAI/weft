//! End-to-end tests for the validate pass. Each test compiles a
//! weft source, enriches strictly, then asserts on the diagnostics.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich;
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::compile;
use weft_compiler::{Diagnostic, Severity};

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root()).expect("stdlib catalog")
}

fn parse_enrich(source: &str) -> weft_core::ProjectDefinition {
    let mut project = compile(source, uuid::Uuid::new_v4(), None).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    project
}

fn codes(diagnostics: &[Diagnostic]) -> Vec<&str> {
    diagnostics
        .iter()
        .filter_map(|d| d.code.as_deref())
        .collect()
}

fn errors(diagnostics: &[Diagnostic]) -> Vec<&Diagnostic> {
    diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .collect()
}

#[test]
fn clean_program_has_no_diagnostics() {
    let project = parse_enrich(
        r#"
# Project: Clean

hi = Text { value: "hello" }
out = Debug
out.data = hi.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "unexpected errors: {:?}", d);
}

#[test]
fn node_named_after_a_type_is_flagged() {
    // Naming a node after a catalog type (`Debug`) is ambiguous: a later
    // `Debug.port` reference would parse as an inline Debug node. Flagged as a
    // reserved-name error on the declaration line (line 3 here).
    let project = parse_enrich("# Project: Clash\n\nDebug = Debug {}\n");
    let d = validate(&project, &catalog());
    let reserved: Vec<_> = d.iter().filter(|e| e.code.as_deref() == Some("reserved-name")).collect();
    assert_eq!(reserved.len(), 1, "expected one reserved-name error, got {d:?}");
    assert_eq!(reserved[0].line, 3, "must point at the declaration line");
    assert_eq!(reserved[0].severity, Severity::Error);
}

#[test]
fn node_named_after_a_type_inside_a_group_is_flagged() {
    // The ambiguity is about the LOCAL name. A node `Debug` inside a group gets
    // scoped id `grp.Debug`, but the source reference is still local `Debug.port`.
    // The check must compare the local segment, not the scoped id.
    let project = parse_enrich("# Project: Clash\n\ngrp = Group() -> () {\n  Debug = Debug {}\n}\n");
    let d = validate(&project, &catalog());
    let reserved: Vec<_> = d.iter().filter(|e| e.code.as_deref() == Some("reserved-name")).collect();
    assert_eq!(reserved.len(), 1, "nested type-named node must be flagged, got {d:?}");
    assert!(reserved[0].message.contains("'Debug'"), "message names the local id: {:?}", reserved[0].message);
}

#[test]
fn duplicate_node_id_is_flagged() {
    // Parser rejects same-scope duplicates at parse time. But a
    // hand-constructed project with dup ids (can happen via direct
    // JSON import) should still be caught by validate.
    let mut project = parse_enrich(
        r#"
# Project: Dup
one = Text { value: "a" }
two = Text { value: "b" }
"#,
    );
    project.nodes[1].id = "one".into();
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"duplicate-node-id"), "{d:?}");
}

#[test]
fn unknown_target_port_with_suggestion() {
    let mut project = parse_enrich(
        r#"
# Project: T
hello = Text { value: "a" }
out = Debug
out.data = hello.value
"#,
    );
    // Corrupt the edge to a typo'd input port name.
    project.edges[0].target_handle = Some("dat".into());
    let d = validate(&project, &catalog());
    let hit = d
        .iter()
        .find(|x| x.code.as_deref() == Some("unknown-target-port"))
        .expect("should flag unknown-target-port");
    assert!(
        hit.message.contains("Did you mean 'data'"),
        "expected did-you-mean hint, got: {}",
        hit.message
    );
}

#[test]
fn duplicate_input_port_is_flagged() {
    let mut project = parse_enrich(
        r#"
# Project: Two
a = Text { value: "x" }
b = Text { value: "y" }
out = Debug
out.data = a.value
"#,
    );
    // Add a second edge driving the same target input.
    let dup = weft_core::project::Edge {
        id: "dup".into(),
        source: "b".into(),
        target: "out".into(),
        source_handle: Some("value".into()),
        target_handle: Some("data".into()),
        span: None,
    };
    project.edges.push(dup);
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"duplicate-input-port"), "{d:?}");
}

#[test]
fn type_mismatch_is_flagged() {
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    let mut project = parse_enrich(
        r#"
# Project: M
one = Text { value: "x" }
out = Debug
out.data = one.value
"#,
    );
    let one = project.nodes.iter_mut().find(|n| n.id == "one").unwrap();
    one.outputs[0].port_type = WeftType::primitive(WeftPrimitive::Number);
    let out = project.nodes.iter_mut().find(|n| n.id == "out").unwrap();
    out.inputs[0].port_type = WeftType::primitive(WeftPrimitive::String);
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"type-mismatch"), "{d:?}");
}

#[test]
fn config_type_mismatch_is_flagged() {
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    // Construct a scenario where a port IS configurable and typed:
    // manually inject an input port with a String type and set
    // config to a number. The rule flags incompatible literal.
    let mut project = parse_enrich(r#"# Project: C
t = Text
"#);
    let t = &mut project.nodes[0];
    t.inputs.push(weft_core::project::PortDefinition {
        name: "value".into(),
        port_type: WeftType::primitive(WeftPrimitive::String),
        required: false,
        description: None,
        lane_mode: Default::default(),
        lane_depth: 1,
        configurable: true,
        user_typed: false,
    });
    t.config = serde_json::json!({ "value": 42 });
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"config-type-mismatch"), "{d:?}");
}

#[test]
fn required_port_unmet_is_flagged() {
    // HumanQuery has required form schema fields; ApiPost's body is
    // not required. We'll construct a Text with a manually-required
    // port and no driver.
    let mut project = parse_enrich(r#"# Project: R
t = Text { value: "ok" }
"#);
    project.nodes[0].inputs.push(weft_core::project::PortDefinition {
        name: "foo".into(),
        port_type: weft_core::weft_type::WeftType::primitive(
            weft_core::weft_type::WeftPrimitive::String,
        ),
        required: true,
        description: None,
        lane_mode: Default::default(),
        lane_depth: 1,
        configurable: false,
        user_typed: false,
    });
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"required-port-unmet"), "{d:?}");
}

#[test]
fn unknown_edge_node_ref_is_flagged() {
    let mut project = parse_enrich(
        r#"
# Project: Dangling
a = Text { value: "x" }
out = Debug
out.data = a.value
"#,
    );
    project.edges[0].source = "ghost".into();
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"unknown-source-node"), "{d:?}");
}

#[test]
fn top_level_include_does_not_make_project_look_like_a_component() {
    // Regression: a Full-mode @include must NOT leave its group flagged
    // `anonymous`, or check_output_reachability treats the whole build as a
    // standalone component and skips the no-output-node requirement (a
    // non-runnable project would silently pass the build gate).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("comp.weft"),
        "Group(raw: String) -> (cleaned: String) {\n s = Text { value: \"x\" }\n self.cleaned = s.value\n}\n",
    ).unwrap();

    // No output node anywhere: must fire no-output-node.
    let src_no_out = "c = @include(\"comp.weft\")\n";
    let mut p = compile(src_no_out, uuid::Uuid::new_v4(), Some(dir.path())).expect("compile");
    enrich(&mut p, &catalog()).expect("enrich");
    let d = validate(&p, &catalog());
    assert!(codes(&d).contains(&"no-output-node"), "expected no-output-node, got {d:?}");

    // With a real Debug output downstream of the include: no no-output error,
    // and the Debug node is NOT spuriously flagged unreachable.
    let src_out = "c = @include(\"comp.weft\")\nc.raw = \"hi\"\nout = Debug\nout.data = c.cleaned\n";
    let mut p2 = compile(src_out, uuid::Uuid::new_v4(), Some(dir.path())).expect("compile");
    enrich(&mut p2, &catalog()).expect("enrich");
    let d2 = validate(&p2, &catalog());
    assert!(!codes(&d2).contains(&"no-output-node"), "unexpected no-output: {d2:?}");
    assert!(!d2.iter().any(|x| x.code.as_deref() == Some("unreachable-from-output") && x.message.contains("out")), "Debug wrongly unreachable: {d2:?}");
}

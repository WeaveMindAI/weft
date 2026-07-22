//! End-to-end tests for the validate pass. Each test compiles a
//! weft source, enriches strictly, then asserts on the diagnostics.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich;
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::compile;
use weft_compiler::{CompileFs, Diagnostic, Severity};

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root()).expect("stdlib catalog")
}

fn parse_enrich(source: &str) -> weft_core::ProjectDefinition {
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
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
fn loop_in_missing_parallel_is_a_loud_invariant_error() {
    // Flatten always materializes `parallel` into the LoopIn config, so
    // a clean program never hits this. Simulate the invariant breaking
    // (a LoopIn that reached validate without `parallel`) by stripping
    // the field, and confirm validate fails LOUD instead of silently
    // defaulting to sequential and skipping every parallel rule.
    let mut project = parse_enrich(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
"#,
    );
    let loop_in = project
        .nodes
        .iter_mut()
        .find(|n| n.node_type == "LoopIn")
        .expect("LoopIn present");
    loop_in
        .config
        .as_object_mut()
        .expect("LoopIn config is an object")
        .remove("parallel")
        .expect("parallel was materialized by flatten");

    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"loop-config-missing-parallel"),
        "expected loud missing-parallel invariant error, got: {:?}",
        codes(&d)
    );
}

#[test]
fn clean_program_has_no_diagnostics() {
    let project = parse_enrich(
        r#"

hi = Text { value: "hello" }
out = Debug
out.data = hi.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "unexpected errors: {:?}", d);
}

#[test]
fn a_port_wired_and_body_set_is_a_double_driver_error() {
    // FetchToStorage's `url` is a body-settable input port: setting it in
    // the body routes the value onto the port. Wiring the SAME port from
    // upstream gives it two drivers; validate rejects it.
    let project = parse_enrich(
        r#"
n = Text { value: "https://x.com/a" }
f = FetchToStorage { url: "https://x.com/b" }
f.url = n.value
out = Debug
out.data = f.file
"#,
    );
    let d = validate(&project, &catalog());
    let hits: Vec<_> =
        d.iter().filter(|e| e.code.as_deref() == Some("double-driven-port")).collect();
    assert_eq!(hits.len(), 1, "expected one double-driven-port error, got {d:?}");
    assert!(hits[0].message.contains("'url'"), "{}", hits[0].message);

    // The body-only form (no wire) stays legal: one driver.
    let project = parse_enrich(
        r#"
f = FetchToStorage { url: "https://x.com/b" }
out = Debug
out.data = f.file
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        !codes(&d).contains(&"double-driven-port"),
        "body-only config must not trip the rule: {d:?}"
    );
}

#[test]
fn literal_placement_gates_where_a_literal_may_drive_a_port() {
    // `literal: none` (OpenRouterInference's `config` port): no literal
    // in ANY form. The braces form...
    let project = parse_enrich(
        r#"
n = OpenRouterInference -> (response: String) { config: {"temperature": 0.5} }
n.prompt = "hi"
out = Debug
out.data = n.response
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("port-literal-placement"));
    assert!(hit.is_some_and(|e| e.message.contains("takes no literal")), "braces literal on a none port must error: {d:?}");

    // ...and the assignment form are both rejected.
    let project = parse_enrich(
        r#"
n = OpenRouterInference -> (response: String) {}
n.config = {"temperature": 0.5}
n.prompt = "hi"
out = Debug
out.data = n.response
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("port-literal-placement"));
    assert!(hit.is_some_and(|e| e.message.contains("takes no literal")), "assignment literal on a none port must error: {d:?}");

    // `literal: assignment` (a file-typed port, by type default): the
    // braces form is refused with the assignment remediation...
    let project = parse_enrich(
        r#"
n = ImageDisplay { image: "not-a-file" }
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("port-literal-placement"));
    assert!(hit.is_some_and(|e| e.message.contains("only as an assignment")), "braces literal on an assignment port must error: {d:?}");

    // ...but the assignment form is legal and normalizes into
    // `port_literals`, exactly like a wire would deliver it.
    let project = parse_enrich(
        r#"
n = ImageDisplay
n.image = "a-literal"
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        !codes(&d).contains(&"port-literal-placement"),
        "assignment literal on an assignment port is legal: {d:?}"
    );
    let node = project.nodes.iter().find(|n| n.id == "n").unwrap();
    assert!(
        node.port_literals.contains_key("image"),
        "the literal must normalize into port_literals: {:?}",
        node.port_literals
    );

    // A none port that is BOTH wired and body-assigned is one mistake,
    // not two: double-driven-port owns it ("remove one driver"), and
    // the placement error ("wire it") must stay silent, since the port
    // is already wired.
    let project = parse_enrich(
        r#"
cfg = OpenRouterConfig {}
n = OpenRouterInference -> (response: String) {}
n.config = cfg.config
n.config = {"temperature": 0.5}
n.prompt = "hi"
out = Debug
out.data = n.response
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"double-driven-port"), "{d:?}");
    assert!(
        !codes(&d).contains(&"port-literal-placement"),
        "double-driven-port owns the wired-and-assigned case: {d:?}"
    );
}

#[test]
fn node_named_after_a_type_is_flagged() {
    // Naming a node after a catalog type (`Debug`) is ambiguous: a later
    // `Debug.port` reference would parse as an inline Debug node. Flagged as a
    // reserved-name error on the declaration line (line 1 here).
    let project = parse_enrich("Debug = Debug {}\n");
    let d = validate(&project, &catalog());
    let reserved: Vec<_> = d.iter().filter(|e| e.code.as_deref() == Some("reserved-name")).collect();
    assert_eq!(reserved.len(), 1, "expected one reserved-name error, got {d:?}");
    assert_eq!(reserved[0].line, 1, "must point at the declaration line");
    assert_eq!(reserved[0].severity, Severity::Error);
}

#[test]
fn node_named_after_a_type_inside_a_group_is_flagged() {
    // The ambiguity is about the LOCAL name. A node `Debug` inside a group gets
    // scoped id `grp.Debug`, but the source reference is still local `Debug.port`.
    // The check must compare the local segment, not the scoped id.
    let project = parse_enrich("grp = Group() -> () {\n  Debug = Debug {}\n}\n");
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
    // Construct a scenario where a port takes literals anywhere and is
    // typed: manually inject an input port with a String type and drive
    // it with a number literal. The rule flags the incompatible literal.
    let mut project = parse_enrich(r#"
t = Text
"#);
    let t = &mut project.nodes[0];
    t.inputs.push(weft_core::project::PortDefinition {
        name: "value".into(),
        port_type: WeftType::primitive(WeftPrimitive::String),
        required: false,
        description: None,
        literal: weft_core::weft_type::LiteralPlacement::Anywhere,
        synthesized_from_carry: false,
    });
    t.port_literals.insert("value".into(), serde_json::json!(42));
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"config-type-mismatch"), "{d:?}");
}

#[test]
fn required_port_unmet_is_flagged() {
    // We construct a Text with a manually-required port and no driver
    // to exercise the required-port-unmet diagnostic.
    let mut project = parse_enrich(r#"
t = Text { value: "ok" }
"#);
    project.nodes[0].inputs.push(weft_core::project::PortDefinition {
        name: "foo".into(),
        port_type: weft_core::weft_type::WeftType::primitive(
            weft_core::weft_type::WeftPrimitive::String,
        ),
        required: true,
        description: None,
        literal: weft_core::weft_type::LiteralPlacement::None,
        synthesized_from_carry: false,
    });
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"required-port-unmet"), "{d:?}");
}

#[test]
fn unknown_edge_node_ref_is_flagged() {
    let mut project = parse_enrich(
        r#"
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
    let mut p = compile(src_no_out, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    enrich(&mut p, &catalog()).expect("enrich");
    let d = validate(&p, &catalog());
    assert!(codes(&d).contains(&"no-output-node"), "expected no-output-node, got {d:?}");

    // With a real Debug output downstream of the include: no no-output error,
    // and the Debug node is NOT spuriously flagged unreachable. The include's
    // input is driven by an inline node (a group port takes wiring, not a bare
    // literal).
    let src_out = "c = @include(\"comp.weft\")\nc.raw = Text { value: \"hi\" }.value\nout = Debug\nout.data = c.cleaned\n";
    let mut p2 = compile(src_out, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    enrich(&mut p2, &catalog()).expect("enrich");
    let d2 = validate(&p2, &catalog());
    assert!(!codes(&d2).contains(&"no-output-node"), "unexpected no-output: {d2:?}");
    // The Debug node `out` (the project's output) must NOT be flagged unreachable.
    // Match the node id precisely (`'out'`), not the substring "out" which also
    // appears in "output"/"outputs" in unrelated messages.
    assert!(!d2.iter().any(|x| x.code.as_deref() == Some("unreachable-from-output") && x.message.contains("'out'")), "Debug wrongly unreachable: {d2:?}");
}

// ── declarative-rule engine (ConfigMatches) ──────────────────────────────────

/// The ApiEndpoint node carries a declarative rule:
/// `when config_matches(path, "^/") then warn`. These tests exercise the
/// declarative-rule engine + the `ConfigMatches` condition end-to-end, including
/// the fail-closed behavior on an absent field (the rule must NOT fire when the
/// field is missing, like every sibling ConfigX condition).
#[test]
fn config_matches_rule_fires_only_when_pattern_matches() {
    // path starts with `/` -> the rule fires (warning).
    let with_slash = parse_enrich("t = ApiEndpoint { path: \"/hook\" }\n");
    let d = validate(&with_slash, &catalog());
    assert!(
        d.iter().any(|x| x.message.contains("path starts with '/'")),
        "config_matches must fire for a leading slash: {d:?}"
    );

    // path without a leading slash -> no rule.
    let no_slash = parse_enrich("t = ApiEndpoint { path: \"hook\" }\n");
    let d2 = validate(&no_slash, &catalog());
    assert!(
        !d2.iter().any(|x| x.message.contains("path starts with '/'")),
        "config_matches must NOT fire without a leading slash: {d2:?}"
    );

    // path absent entirely -> no rule (fail-closed: an absent field is not a
    // match, matching every sibling ConfigX condition; the old `unwrap_or(true)`
    // wrongly fired this).
    let absent = parse_enrich("t = ApiEndpoint {}\n");
    let d3 = validate(&absent, &catalog());
    assert!(
        !d3.iter().any(|x| x.message.contains("path starts with '/'")),
        "config_matches must NOT fire when the field is absent: {d3:?}"
    );
}

// ─── Loop validate tests ────────────────────────────────────────────────────

fn parse_enrich_lenient(source: &str) -> (weft_core::ProjectDefinition, Vec<weft_compiler::weft_compiler::CompileError>) {
    use weft_compiler::weft_compiler::{compile_lenient, IncludeMode};
    let (mut project, errs) = compile_lenient(source, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    // Use lenient enrich so an unknown type doesn't bail before validate runs.
    let _ = weft_compiler::enrich::enrich_with_policy(&mut project, &catalog(), weft_compiler::enrich::EnrichPolicy::Lenient);
    (project, errs)
}

#[test]
fn loop_without_parallel_defaults_to_sequential() {
    // No `parallel` field: defaults to false (sequential), with the
    // default MATERIALIZED into the flattened LoopIn config by the
    // compiler (the runtime never carries its own default). No
    // diagnostic, and the sequential-mode rules apply.
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text { value: "x" }
self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let cs = codes(&d);
    assert!(!cs.contains(&"loop-parallel-not-boolean"), "no parallel diagnostic: {cs:?}");
    assert!(!cs.contains(&"parallel-without-over"), "no parallel-without-over: {cs:?}");
    let loop_in = project
        .nodes
        .iter()
        .find(|n| n.node_type == "LoopIn")
        .expect("LoopIn boundary exists");
    assert_eq!(
        loop_in.config.get("parallel"),
        Some(&serde_json::Value::Bool(false)),
        "flatten materializes the sequential default"
    );
}

#[test]
fn loop_parallel_non_boolean_is_rejected() {
    // `parallel: "yes"` must NOT coerce to sequential: it would run
    // the wrong drive mode AND skip the parallel-interplay rules.
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: "yes"
    over: ["items"]
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let cs = codes(&d);
    assert!(cs.contains(&"loop-parallel-not-boolean"), "{cs:?}");
}

#[test]
fn loop_unknown_config_key_is_rejected() {
    // A typo'd knob (`max_itres`) silently running the loop uncapped
    // is the masked-bug class the language forbids.
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    max_itres: 10
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let cs = codes(&d);
    assert!(cs.contains(&"loop-unknown-config-field"), "{cs:?}");
}

#[test]
fn loop_max_iters_and_trim_types_are_enforced() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    max_iters: "ten"
    trim_on_mismatch: "nope"
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let cs = codes(&d);
    assert!(cs.contains(&"loop-max-iters-not-integer"), "{cs:?}");
    assert!(cs.contains(&"loop-trim-not-boolean"), "{cs:?}");
}

#[test]
fn parallel_with_carry_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null], acc: String) {
    parallel: true
    over: ["items"]
    carry: ["acc"]
    p = Text { value: "x" }
    self.results = p.value
    self.acc = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"parallel-with-carry"), "expected parallel-with-carry, got {:?}", codes(&d));
}

#[test]
fn parallel_without_over_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop() -> () {
    parallel: true
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"parallel-without-over"), "expected parallel-without-over, got {:?}", codes(&d));
}

#[test]
fn parallel_with_done_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    p = Text { value: "x" }
    self.results = p.value
    self.done = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"parallel-with-done"), "expected parallel-with-done, got {:?}", codes(&d));
}

#[test]
fn sequential_loop_without_termination_fires() {
    // No `over`, no `max_iters`, no `self.done` write: provably
    // infinite, rejected at compile time.
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop() -> () {
    parallel: false
    p = Text { value: "x" }
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"loop-unbounded-no-termination"),
        "expected loop-unbounded-no-termination, got {:?}", codes(&d));
}

#[test]
fn done_wired_sequential_loop_is_accepted_unbounded() {
    // A `self.done = ...` write is a termination condition: the loop
    // is the user's own program, trusted and unbounded.
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop() -> () {
    parallel: false
    p = Text { value: "x" }
    self.done = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"loop-unbounded-no-termination"),
        "done-wired loop must not be flagged unbounded, got {:?}", codes(&d));
}

#[test]
fn max_iters_only_sequential_loop_is_accepted() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop() -> () {
    parallel: false
    max_iters: 5
    p = Text { value: "x" }
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"loop-unbounded-no-termination"),
        "max_iters-capped loop must not be flagged unbounded, got {:?}", codes(&d));
}

#[test]
fn over_and_carry_overlap_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (items: String) {
    parallel: false
    over: ["items"]
    carry: ["items"]
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"over-and-carry-overlap"), "expected over-and-carry-overlap, got {:?}", codes(&d));
}

#[test]
fn gather_output_must_be_nullable_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String]) {
    parallel: true
    over: ["items"]
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"gather-output-must-be-nullable"),
        "expected gather-output-must-be-nullable, got {:?}", codes(&d));
}

#[test]
fn reserved_port_name_index_input_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(index: Number, items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"reserved-port-name"),
        "expected reserved-port-name for 'index' input, got {:?}", codes(&d));
}

#[test]
fn reserved_port_name_done_output_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null], done: Boolean) {
    parallel: true
    over: ["items"]
    p = Text { value: "x" }
    self.results = p.value
    self.done = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"reserved-port-name"),
        "expected reserved-port-name for 'done' output, got {:?}", codes(&d));
}

#[test]
fn over_not_a_list_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(threshold: Number) -> (results: List[String | Null]) {
    parallel: true
    over: ["threshold"]
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"over-not-a-list"),
        "expected over-not-a-list, got {:?}", codes(&d));
}

#[test]
fn loop_over_unknown_port_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["ghost"]
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"loop-over-unknown-port"),
        "expected loop-over-unknown-port, got {:?}", codes(&d));
}

#[test]
fn loop_carry_unknown_port_fires() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: false
    over: ["items"]
    carry: ["ghost"]
    p = Text { value: "x" }
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"loop-carry-unknown-port"),
        "expected loop-carry-unknown-port, got {:?}", codes(&d));
}

#[test]
fn clean_parallel_map_loop_validates() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let only_loop_errors: Vec<&str> = d.iter()
        .filter(|x| x.severity == Severity::Error)
        .filter_map(|x| x.code.as_deref())
        .filter(|c| c.starts_with("loop-") || c.starts_with("parallel-") || c == &"reserved-port-name" || c == &"gather-output-must-be-nullable" || c == &"over-and-carry-overlap" || c == &"over-not-a-list" || c == &"carry-port-type-mismatch" || c == &"carry-port-missing-output")
        .collect();
    assert!(only_loop_errors.is_empty(), "expected no loop-specific errors, got {:?}", only_loop_errors);
}

#[test]
fn clean_sequential_fold_loop_validates() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null], acc: String) {
    parallel: false
    over: ["items"]
    carry: ["acc"]
    p = Text {}
    p.value = self.items
    self.results = p.value
    self.acc = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let only_loop_errors: Vec<&str> = d.iter()
        .filter(|x| x.severity == Severity::Error)
        .filter_map(|x| x.code.as_deref())
        .filter(|c| c.starts_with("loop-") || c.starts_with("parallel-") || c == &"reserved-port-name" || c == &"gather-output-must-be-nullable" || c == &"over-and-carry-overlap" || c == &"over-not-a-list" || c == &"carry-port-type-mismatch" || c == &"carry-port-missing-output")
        .collect();
    assert!(only_loop_errors.is_empty(), "expected no loop-specific errors, got {:?}", only_loop_errors);
}

#[test]
fn same_name_nested_loops_compile_clean() {
    // Two loops both named `my_loop`, one nested inside the other, are valid:
    // the inner is fully-scoped as `my_loop.my_loop`, so the boundary ids
    // (`my_loop__in/out` vs `my_loop.my_loop__in/out`) cannot collide.
    let (project, _) = parse_enrich_lenient(
        r#"
my_loop = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: false
    over: ["items"]
    my_loop = Loop(inner: List[String]) -> (inner_results: List[String | Null]) {
        parallel: true
        over: ["inner"]
        p = Text {}
        p.value = self.inner
        self.inner_results = p.value
    }
    my_loop.inner = self.items
    self.results = my_loop.inner_results
}
"#,
    );
    let d = validate(&project, &catalog());
    let loop_errs: Vec<&str> = d.iter()
        .filter(|x| x.severity == Severity::Error)
        .filter_map(|x| x.code.as_deref())
        .filter(|c| c.starts_with("loop-") || c.starts_with("parallel-") || *c == "reserved-port-name")
        .collect();
    assert!(loop_errs.is_empty(), "expected clean nested same-name loops, got {:?}", loop_errs);

    // Two distinct LoopIn boundary ids, two distinct LoopOut boundary ids.
    let in_ids: Vec<&str> = project.nodes.iter()
        .filter(|n| n.node_type == "LoopIn")
        .map(|n| n.id.as_str())
        .collect();
    let out_ids: Vec<&str> = project.nodes.iter()
        .filter(|n| n.node_type == "LoopOut")
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(in_ids.len(), 2, "two LoopIns: {:?}", in_ids);
    assert_eq!(out_ids.len(), 2, "two LoopOuts: {:?}", out_ids);
    // Distinct fully-scoped ids: `my_loop__in` and `my_loop.my_loop__in`.
    assert!(in_ids.contains(&"my_loop__in"), "{:?}", in_ids);
    assert!(in_ids.contains(&"my_loop.my_loop__in"), "{:?}", in_ids);
}

#[test]
fn same_name_loops_at_same_scope_clash() {
    // Two loops both named `my_loop` declared at the SAME scope level must
    // fail compile with the existing duplicate-identifier diagnostic.
    use weft_compiler::weft_compiler::{compile_lenient, IncludeMode};
    let src = r#"
my_loop = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
}
my_loop = Loop(other: List[String]) -> (out: List[String | Null]) {
    parallel: true
    over: ["other"]
}
"#;
    let (_, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.iter().any(|e| e.message.contains("Duplicate id")),
        "expected Duplicate id error, got {:?}", errs);
}

#[test]
fn storage_plane_example_chain_validates_clean() {
    // FetchToStorage emits a File (any stored file). DownloadLink and
    // KeepFile take File, so those edges are File -> File. ImageDisplay
    // demands Image, which File does NOT satisfy. The author NARROWS the
    // fetch's output port to Image in the node header (`-> (file: Image)`):
    // legal because Image is a sub-case of the declared File, and the
    // narrowed Image then satisfies ImageDisplay. The runtime enforces the
    // narrow (a non-image fetched here closes the port and warns).
    let project = parse_enrich(
        r#"
file_url = Text { value: "https://example.com/x.png" }

fetch = FetchToStorage -> (file: Image) { keep: false }
fetch.url = file_url.value

show = ImageDisplay
show.image = fetch.file

link = DownloadLink
link.file = fetch.file

kept = KeepFile { ttl_days: 30 }
kept.file = fetch.file
"#,
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "storage chain should validate clean: {:?}", d);
}

#[test]
fn output_narrow_to_incompatible_type_is_rejected() {
    // FetchToStorage outputs File. Narrowing to Image is legal (sub-case);
    // "narrowing" to Number is NOT (Number is not a sub-case of File), so
    // enrich must reject it loud instead of silently adopting the bogus
    // type. This is the legality gate behind output-port type narrowing.
    let mut project = compile(
        r#"
fetch = FetchToStorage -> (file: Number) { keep: false }
"#,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
    )
    .expect("compile ok");
    let err = enrich(&mut project, &catalog());
    assert!(err.is_err(), "narrowing File to Number must be rejected");
    let msg = format!("{:?}", err.unwrap_err());
    assert!(
        msg.contains("incompatible with catalog type"),
        "error should explain the narrow is incompatible: {msg}"
    );
}

// ── graph-shape rules (cycles, trigger placement) ────────────────────────────

#[test]
fn a_wire_cycle_is_a_compile_error() {
    let mut project = parse_enrich(
        r#"
a = Text { value: "x" }
b = Text
b.value = a.value
out = Debug
out.data = b.value
"#,
    );
    // Hand-close the cycle (the parser has no syntax that lowers to a
    // back edge today; the rule guards the definition shape itself).
    project.edges.push(weft_core::project::Edge {
        id: "back".into(),
        source: "b".into(),
        target: "a".into(),
        source_handle: Some("value".into()),
        target_handle: Some("value".into()),
        span: None,
    });
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"graph-cycle"), "{d:?}");
}

#[test]
fn trigger_wiring_rules_are_compile_errors() {
    // trigger-into-trigger, via a plain node in between (transitive).
    let project = parse_enrich(
        r#"
t1 = Cron { cron: "* * * * *" }
mid = Debug
mid.data = t1.scheduledTime
t2 = HumanTrigger
t2.fields = mid.data
out = Debug
out.data = t2.submitted
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"trigger-into-trigger"), "{d:?}");
}

#[test]
fn a_trigger_inside_a_loop_is_a_compile_error() {
    let project = parse_enrich(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    t = Cron { cron: "* * * * *" }
    p = Text {}
    p.value = self.items
    self.results = p.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"trigger-in-loop"), "{d:?}");
}

#[test]
fn a_trigger_wired_into_an_infra_node_is_a_compile_error() {
    // trigger-into-infra, via a plain node in between (transitive). No
    // stdlib infra node takes inputs, so mark the sink infra by hand;
    // the rule guards the definition shape itself.
    let mut project = parse_enrich(
        r#"
t = Cron { cron: "* * * * *" }
mid = Debug
mid.data = t.scheduledTime
sink = Debug
sink.data = mid.data
"#,
    );
    project
        .nodes
        .iter_mut()
        .find(|n| n.id == "sink")
        .expect("sink present")
        .requires_infra = true;
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"trigger-into-infra"), "{d:?}");
}

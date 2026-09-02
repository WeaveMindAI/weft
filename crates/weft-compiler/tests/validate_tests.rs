//! End-to-end tests for the validate pass. Each test compiles a
//! weft source, enriches strictly, then asserts on the diagnostics.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich;
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::compile;
use weft_compiler::{CompileFs, Diagnostic, Severity};

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib catalog")
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
    // `literal: none` (LlmInference's `params` port): no literal
    // in ANY form. The braces form...
    let project = parse_enrich(
        r#"
n = LlmInference -> (response: String) { params: {"temperature": 0.5} }
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
n = LlmInference -> (response: String) {}
n.params = {"temperature": 0.5}
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
n = MediaDisplay { media: "not-a-file" }
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("port-literal-placement"));
    assert!(hit.is_some_and(|e| e.message.contains("only as an assignment")), "braces literal on an assignment port must error: {d:?}");

    // ...but the assignment form is legal and normalizes into
    // `port_literals`, exactly like a wire would deliver it.
    let project = parse_enrich(
        r#"
n = MediaDisplay
n.media = "a-literal"
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        !codes(&d).contains(&"port-literal-placement"),
        "assignment literal on an assignment port is legal: {d:?}"
    );
    let node = project.nodes.iter().find(|n| n.id == "n").unwrap();
    assert!(
        node.port_literals.contains_key("media"),
        "the literal must normalize into port_literals: {:?}",
        node.port_literals
    );

    // A none port that is BOTH wired and body-assigned is one mistake,
    // not two: double-driven-port owns it ("remove one driver"), and
    // the placement error ("wire it") must stay silent, since the port
    // is already wired.
    let project = parse_enrich(
        r#"
cfg = LlmParams {}
n = LlmInference -> (response: String) {}
n.params = cfg.params
n.params = {"temperature": 0.5}
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
        source_file: None,
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
    // it with an UNCASTABLE literal (an object). A castable value (say
    // a bare number) is deliberately silent here: validate only flags
    // what the compile's cast pass cannot fix, and names why.
    let mut project = parse_enrich(r#"
t = Text
"#);
    let t = &mut project.nodes[0];
    t.inputs.push(weft_core::project::InputDefinition {
        port: weft_core::project::PortDefinition {
            name: "value".into(),
            port_type: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            synthesized_from_carry: false,
            declared_type: None,
        },
        exposure: weft_core::weft_type::Exposure::All,
        widget: None,
        default: None,
        label: None,
        placeholder: None,
        from_spec: false,
        requires_scopes: None,
        requires_values: None,
    });
    t.port_literals.insert("value".into(), serde_json::json!({"not": "a string"}));
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
    project.nodes[0].inputs.push(weft_core::project::InputDefinition {
        port: weft_core::project::PortDefinition {
            name: "foo".into(),
            port_type: weft_core::weft_type::WeftType::primitive(
                weft_core::weft_type::WeftPrimitive::String,
            ),
            required: true,
            description: None,
            synthesized_from_carry: false,
            declared_type: None,
        },
        exposure: weft_core::weft_type::Exposure::Wire,
        widget: None,
        default: None,
        label: None,
        placeholder: None,
        from_spec: false,
        requires_scopes: None,
        requires_values: None,
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
    // The loop config keys (`over`, materialized `parallel`) live on
    // the LoopIn boundary node, which declares no matching inputs;
    // check_loop_config owns their validation, so the generic
    // undeclared-key check must not fire on boundary nodes.
    assert!(
        !cs.contains(&"undeclared-port-no-custom"),
        "loop config keys are not undeclared inputs: {cs:?}"
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
    // KeepFile take File, so those edges are File -> File. MediaDisplay
    // demands displayable media, which File does NOT satisfy. The author NARROWS the
    // fetch's output port to Image in the node header (`-> (file: Image)`):
    // legal because Image is a sub-case of the declared File, and the
    // narrowed Image then satisfies MediaDisplay. The runtime enforces the
    // narrow (a non-image fetched here closes the port and warns).
    let project = parse_enrich(
        r#"
file_url = Text { value: "https://example.com/x.png" }

fetch = FetchToStorage -> (file: Image) { keep: false }
fetch.url = file_url.value

show = MediaDisplay
show.media = fetch.file

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
        source_file: None,
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

// ── unified-input diagnostics ───────────────────────────────────────────────

#[test]
fn wiring_a_config_input_is_input_not_wireable() {
    // HttpRequest's `method` is a `config`-exposure input (a select):
    // design-time configuration, never graph data.
    let project = parse_enrich(
        r#"
t = Text { value: "GET" }
req = HttpRequest { url: "http://x" method: "GET" }
req.method = t.value
out = Debug
out.data = req.body
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"input-not-wireable"), "{:?}", d);
}

#[test]
fn a_type_mismatched_wire_onto_a_config_input_fires_only_input_not_wireable() {
    // The edge is illegal as a whole; input-not-wireable owns it. The
    // type machinery must stay silent instead of stacking type-mismatch
    // on top of the real cause.
    let project = parse_enrich(
        r#"
n = Range { from: 1, to: 3 }
req = HttpRequest { url: "http://x", method: "GET" }
req.method = n.values
out = Debug
out.data = req.body
"#,
    );
    let d = validate(&project, &catalog());
    let cs = codes(&d);
    assert!(cs.contains(&"input-not-wireable"), "{d:?}");
    assert!(!cs.contains(&"type-mismatch"), "one mistake, one diagnostic: {d:?}");
}

#[test]
fn an_access_node_stamps_its_service_and_materializes_no_permission_input() {
    // Permissions are ticked once at connect time and live on the
    // stored connection, never in source: enrich stamps the service
    // name onto the access widget and materializes NOTHING else.
    let project = parse_enrich(
        r#"
ws = SlackAccess
send = SlackSendMessage { channel: "C1", text: "hi" }
send.account = ws.access
"#,
    );
    let ws = project.nodes.iter().find(|n| n.id == "ws").expect("ws node");
    assert!(
        !ws.inputs.iter().any(|i| i.name == "scopes"),
        "no permission input is materialized; permissions live on the connection"
    );
    // The connect widget carries the compiler-stamped service.
    let account = ws.inputs.iter().find(|i| i.name == "account").expect("account input");
    match &account.widget {
        Some(weft_core::node::Widget::Access { service, optional }) => {
            assert_eq!(service.as_deref(), Some("slack"));
            assert!(!optional, "slack's recipe does not declare connection_optional");
        }
        other => panic!("account input is not an access widget: {other:?}"),
    }
    // A consumer input's declared requirement is mirrored onto the
    // INSTANCE, for the runtime to stamp onto the marker.
    let send = project.nodes.iter().find(|n| n.id == "send").expect("send node");
    let account = send.inputs.iter().find(|i| i.name == "account").expect("account input");
    assert_eq!(
        account.requires_scopes.as_deref(),
        Some(&["chat:write".to_string()][..]),
        "requiresScopes rides the compiled instance"
    );
}

#[test]
fn a_remote_select_pick_literal_passes_the_type_check() {
    // The channel picker stores `{id, label}` on a String input; the
    // widget's contract legalizes the object form (the runtime unwraps
    // it to the bare id), while a plain pasted string stays a string.
    let project = parse_enrich(
        r##"
ws = SlackAccess
send = SlackSendMessage { channel: {"id": "C42", "label": "#general"}, text: "hi" }
send.account = ws.access
"##,
    );
    let d = validate(&project, &catalog());
    assert!(
        !codes(&d).contains(&"config-type-mismatch"),
        "the {{id, label}} pick form is legal on a remote_select: {d:?}"
    );
}

#[test]
fn a_number_literal_outside_the_widget_range_is_rejected() {
    // LlmParams' `temperature` widget declares max 2.0.
    let project = parse_enrich(
        r#"
cfg = LlmParams { temperature: 5.0 }
out = Debug
out.data = cfg.params
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"literal-out-of-range"), "{:?}", d);

    let project = parse_enrich(
        r#"
cfg = LlmParams { temperature: 0.7 }
out = Debug
out.data = cfg.params
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"literal-out-of-range"), "{:?}", d);
}

#[test]
fn a_required_input_with_a_default_is_satisfied() {
    // HttpRequest's `method` is required but declares default "GET":
    // dragging the node and running without touching Method must compile
    // (the runtime supplies the default). This is the audit's defect 1.
    let project = parse_enrich(
        r#"
req = HttpRequest { url: "http://x" }
out = Debug
out.data = req.body
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        !codes(&d).contains(&"required-port-unmet"),
        "a defaulted required input needs no driver: {:?}",
        d
    );
}

#[test]
fn declaring_a_config_input_as_a_header_port_is_an_enrich_error() {
    let mut project = compile(
        r#"
req = HttpRequest(method: String) { url: "http://x" }
out = Debug
out.data = req.body
"#,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
    )
    .expect("compile ok");
    let err = enrich(&mut project, &catalog()).expect_err("config input as port must fail enrich");
    let msg = format!("{err}");
    assert!(msg.contains("configuration-only"), "{msg}");
}

#[test]
fn a_castable_literal_is_cast_instead_of_failing() {
    // `temperature: "0.7"` (a string on a Number input): unambiguously a
    // number, so the compile CASTS it in the compiled definition rather
    // than failing dumbly. The range check then runs on the cast value.
    let project = parse_enrich(
        r#"
cfg = LlmParams { temperature: "0.7" }
out = Debug
out.data = cfg.params
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"config-type-mismatch"), "{:?}", d);
    let cfg = project.nodes.iter().find(|n| n.id == "cfg").unwrap();
    assert_eq!(
        cfg.config.get("temperature"),
        Some(&serde_json::json!(0.7)),
        "the compiled definition carries the CAST number"
    );

    // An out-of-range value stays out of range after the cast.
    let project = parse_enrich(
        r#"
cfg = LlmParams { temperature: "5.0" }
out = Debug
out.data = cfg.params
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"literal-out-of-range"), "{:?}", d);

    // A genuinely uncastable literal still fails loudly.
    let project = parse_enrich(
        r#"
cfg = LlmParams { temperature: "banana" }
out = Debug
out.data = cfg.params
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"config-type-mismatch"), "{:?}", d);
}

/// The checked-cast semantic (`features.castPorts`): the compiler holds
/// the resolved (input, output) pair to the one weft-core conversion
/// table, keyed on the metadata FEATURE (never a node-type name).
#[test]
fn cast_pair_is_checked_against_the_conversion_table() {
    // Parse text into a number: allowed, no diagnostics.
    let project = parse_enrich(
        r#"
src = Text { value: "42" }
c = Cast() -> (value: Number)
c.value = src.value
out = Debug
out.data = c.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "String -> Number cast must be clean: {:?}", errors(&d));

    // A structured dict into a Number: nonsense, refused at compile time.
    let project = parse_enrich(
        r#"
p = ExecPython() -> (out: JsonDict) {
    code: "out = {}"
}
c = Cast() -> (value: Number)
c.value = p.out
out = Debug
out.data = c.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"cast-not-allowed"),
        "JsonDict -> Number must die at compile time: {:?}",
        codes(&d)
    );
}

/// The build/hash entry (`compile_enriched_with_diagnostics`) resolves
/// catalog-declared type names on inline port signatures by itself: the
/// registry activation lives in the shared compile front half, not in
/// each public entry. Regression: this path once skipped the registry,
/// so a custom type parsed fine in the editor but died at build time.
#[test]
fn build_entry_resolves_declared_types() {
    let source = r#"
src = Text { value: "{}" }
c = Cast() -> (value: ChatMessage)
c.value = src.value
out = Debug
out.data = c.value
"#;
    let result = weft_compiler::compile_enriched_with_diagnostics(
        source,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
        &catalog(),
    );
    assert!(
        result.is_ok(),
        "ChatMessage (declared by the history package) must resolve on the build path: {:?}",
        result.err()
    );
}

/// A NESTED typevar port (`List[T]`) wired to a concrete list on both
/// sides resolves cleanly, and a genuinely unwired one is refused by
/// the unresolved-typevar diagnostic (the binder and the validator
/// must agree on what "unresolved" means; a bare-`T`-only binder left
/// nested vars unresolved forever, which the validator then rejected
/// on a perfectly wired graph).
#[test]
fn nested_typevar_resolves_through_edges_or_fails_loud() {
    let wired = r#"
py = ExecPython(a: Number) -> (b: List[Number]) { code: "return {'b': [a]}", a: 1 }
g = Group(items: List[T]) -> (out: List[T]) {
    self.out = self.items
}
g.items = py.b
sink = Debug
sink.data = g.out
"#;
    let result = weft_compiler::compile_checked(
        wired,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
        &catalog(),
        weft_compiler::validate::ValidationMode::Structural,
    );
    assert!(result.is_ok(), "a wired List[T] must resolve: {:?}", result.err());

    let unwired = r#"
g = Group(items: List[T]) -> (out: List[T]) {
    self.out = self.items
}
sink = Debug
sink.data = g.out
"#;
    let err = weft_compiler::compile_checked(
        unwired,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
        &catalog(),
        weft_compiler::validate::ValidationMode::Structural,
    )
    .expect_err("an unwired List[T] port must be refused");
    assert!(format!("{err:?}").contains("'List[T]' unresolved"), "{err:?}");
}

/// An un-overridden Cast output wired downstream still dies on the
/// existing must-override-unmet rule (the cast check does not eat it).
#[test]
fn cast_without_declared_target_is_must_override_unmet() {
    let project = parse_enrich(
        r#"
src = Text { value: "42" }
c = Cast
c.value = src.value
out = Debug
out.data = c.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"must-override-unmet"),
        "un-overridden Cast output must be refused: {:?}",
        codes(&d)
    );
}


// ----- Generator[T] wiring rules -------------------------------------

#[test]
fn clean_stream_loop_over_a_generator_validates() {
    // The flagship shape: Range yields a stream, the loop pulls one
    // number per iteration. No generator- or loop-specific errors.
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
my = Loop(values: Generator[Number]) -> (results: List[Number | Null]) {
    parallel: false
    over: ["values"]
    p = ExecPython(n: Number) -> (out: Number) { code: "return {'out': n * 2}" }
    p.n = self.values
    self.results = p.out
}
my.values = nums.values
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "expected a clean stream loop, got {:?}", errors(&d));
}

#[test]
fn a_parallel_stream_loop_validates_clean() {
    // Parallel mode over a stream is a supported shape (the runtime
    // launches a lane per arriving item); pin that no rule rejects it.
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
my = Loop(values: Generator[Number]) -> (results: List[Number | Null]) {
    parallel: true
    over: ["values"]
    p = ExecPython(n: Number) -> (out: Number) { code: "return {'out': n * 2}" }
    p.n = self.values
    self.results = p.out
}
my.values = nums.values
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "expected a clean parallel stream loop, got {:?}", errors(&d));
}

#[test]
fn a_stream_in_over_must_be_alone() {
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
my = Loop(values: Generator[Number], names: List[String]) -> (results: List[Number | Null]) {
    over: ["values", "names"]
    p = ExecPython(n: Number) -> (out: Number) { code: "return {'out': n}" }
    p.n = self.values
    self.results = p.out
}
my.values = nums.values
my.names = @json(["a"])
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"over-stream-not-alone"),
        "expected over-stream-not-alone, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_generator_loop_input_outside_over_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
my = Loop(rows: Generator[Number], items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
my.rows = nums.values
my.items = @json(["a"])
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-not-iterated"),
        "expected generator-not-iterated, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_stream_with_two_consumers_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
a = ExecPython(rows: Generator[Number]) -> (done: Boolean) { code: "return {'done': True}" }
b = ExecPython(rows: Generator[Number]) -> (done: Boolean) { code: "return {'done': True}" }
a.rows = nums.values
b.rows = nums.values
out = Debug
out.data = a.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-multiple-consumers"),
        "expected generator-multiple-consumers, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_stream_crossing_a_group_boundary_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 3 }
grp = Group(vals: Generator[Number]) -> (done: Boolean) {
    c = ExecPython(rows: Generator[Number]) -> (done: Boolean) { code: "return {'done': True}" }
    c.rows = self.vals
    self.done = c.done
}
grp.vals = nums.values
out = Debug
out.data = grp.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-through-group"),
        "expected generator-through-group, got {:?}",
        codes(&d)
    );
    // One crossing, ONE diagnostic: the ban lives on the boundary
    // node, and the synthesized boundary's flatten shape must not
    // stack extra rules (generator-input-must-be-required) on top.
    assert_eq!(
        errors(&d).len(),
        1,
        "one stream crossing reports exactly once, got {:?}",
        codes(&d)
    );
}

/// A stream declared on the PROJECT'S OWN input boundary has no
/// incoming edge at all, so an edge-based rule would let it compile
/// clean and ship an unsatisfiable graph (nothing can hand an
/// execution a live stream as an input).
#[test]
fn a_stream_on_the_projects_input_boundary_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
Group(rows: Generator[Number]) -> (done: Boolean) {
    p = ExecPython(rows: Generator[Number]) -> (done: Boolean) { code: "return {'done': True}" }
    p.rows = self.rows
    self.done = p.done
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-through-group"),
        "expected generator-through-group on the project input boundary, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_generator_nested_in_a_container_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
p = ExecPython(xs: List[Generator[Number]]) -> (done: Boolean) { code: "return {'done': True}" }
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-in-container"),
        "expected generator-in-container, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_stream_cannot_be_carried_between_iterations() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(items: List[String]) -> (results: List[String | Null], acc: Generator[Number]) {
    over: ["items"]
    carry: ["acc"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
my.items = @json(["a"])
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-not-carriable"),
        "expected generator-not-carriable, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_stream_into_a_generic_port_is_rejected() {
    // The single most likely user mistake: Range straight into Debug.
    // Debug's `data` is a generic `T`; without a dedicated rule the
    // binding would compile and the consumer would receive the raw
    // handle marker it never pulls (the producer then dies at its
    // buffer cap with a misleading backpressure error at runtime).
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
out = Debug
out.data = nums.values
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-into-generic-port"),
        "expected generator-into-generic-port, got {:?}",
        codes(&d)
    );
    // This rule OWNS the edge's error: the leftover unbound `T` must
    // not also surface as unresolved-typevar ("connect it to something
    // concrete" would mislead: the port IS connected).
    assert!(
        !codes(&d).contains(&"unresolved-typevar"),
        "the generic-port rule owns the edge; got {:?}",
        codes(&d)
    );
}

/// A loop's stream `over` port mirrors the user's own declaration, so
/// marking it optional must still be rejected: an optional stream
/// would compile as a loop nothing can ever wire or satisfy.
#[test]
fn an_optional_stream_over_port_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
my = Loop(values: Generator[Number]?) -> (results: List[Number | Null]) {
    over: ["values"]
    p = ExecPython(n: Number) -> (out: Number) { code: "return {'out': n}" }
    p.n = self.values
    self.results = p.out
}
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-input-must-be-required"),
        "expected generator-input-must-be-required on the loop's over port, got {:?}",
        codes(&d)
    );
}

/// The mirror of the stream-into-generic ban: a generic SOURCE cannot
/// produce the live stream a Generator input needs, and the rule owns
/// that edge too (no misleading unresolved-typevar stacked on it).
#[test]
fn a_generic_source_into_a_stream_port_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
seed = ExecPython() -> (out: T) { code: "return {'out': 1}" }
p = ExecPython(rows: Generator[Number]) -> (done: Boolean) { code: "return {'done': True}" }
p.rows = seed.out
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-into-generic-port"),
        "expected generator-into-generic-port on the generic source, got {:?}",
        codes(&d)
    );
    assert!(
        !d.iter().any(|e| {
            e.code.as_deref() == Some("unresolved-typevar") && e.message.contains("seed.out")
        }),
        "the generic-port rule owns the edge, got {d:?}"
    );
}

/// A generic stream CONSUMER is well-typed: `Generator[T]` wired to a
/// `Generator[Number]` source binds `T = Number` element-wise and
/// compiles clean (only a bare `T` receiving a whole stream is
/// refused).
#[test]
fn a_generic_stream_port_binds_its_element_from_the_source() {
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
p = ExecPython(rows: Generator[T]) -> (done: Boolean) { code: "return {'done': True}" }
p.rows = nums.values
out = Debug
out.data = p.done
"#,
    );
    let rows = project
        .nodes
        .iter()
        .find(|n| n.id == "p")
        .and_then(|n| n.inputs.iter().find(|i| i.name == "rows"))
        .expect("p.rows present");
    assert_eq!(
        rows.port_type.to_string(),
        "Generator[Number]",
        "the element binds from the source"
    );
    let d = validate(&project, &catalog());
    assert!(errors(&d).is_empty(), "a bound generic stream port is clean: {:?}", errors(&d));
}

/// A MustOverride target (an unknown declared type) behind a stream
/// source is ONE mistake with one owner: `generator-into-generic-port`
/// reports it, and `must-override-unmet` stays quiet (its "declare a
/// concrete type" advice would contradict "declare Generator[T]").
#[test]
fn a_stream_into_a_must_override_port_reports_once() {
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
p = ExecPython(xs: Blah) -> (done: Boolean) { code: "return {'done': True}" }
p.xs = nums.values
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-into-generic-port"),
        "expected generator-into-generic-port, got {:?}",
        codes(&d)
    );
    // The stream edge reports ONCE: no must-override-unmet stacked on
    // it, and no unresolved-typevar naming its target port. (Other
    // edges may still report their own problems: the unknown type
    // aborts enrichment, so Debug's generic stays unbound.)
    assert!(
        !codes(&d).contains(&"must-override-unmet"),
        "the generic-port rule owns the stream edge, got {:?}",
        codes(&d)
    );
    assert!(
        !d.iter().any(|e| {
            e.code.as_deref() == Some("unresolved-typevar") && e.message.contains("p.xs")
        }),
        "no unresolved-typevar on the stream edge's target, got {d:?}"
    );
}

/// "Generic" means any unresolved leaf at any depth, not just a bare
/// `T`: enrich refuses the binding for all of them, so without this
/// rule a stream into `List[T]` would compile with ZERO diagnostics.
#[test]
fn a_stream_into_a_container_of_typevars_is_rejected() {
    for target in ["xs: List[T]", "xs: Dict[String, T]"] {
        let (project, _) = parse_enrich_lenient(&format!(
            r#"
nums = Range {{ to: 5 }}
p = ExecPython({target}) -> (done: Boolean) {{ code: "return {{'done': True}}" }}
p.xs = nums.values
out = Debug
out.data = p.done
"#
        ));
        let d = validate(&project, &catalog());
        assert!(
            codes(&d).contains(&"generator-into-generic-port"),
            "{target}: expected generator-into-generic-port, got {:?}",
            codes(&d)
        );
    }
}

#[test]
fn a_stream_element_type_mismatch_is_rejected() {
    // Generator is invariant in T: Number items never satisfy a
    // String stream port.
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
p = ExecPython(rows: Generator[String]) -> (done: Boolean) { code: "return {'done': True}" }
p.rows = nums.values
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"type-mismatch"),
        "expected type-mismatch, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_generator_of_generators_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
p = ExecPython(xs: Generator[Generator[Number]]) -> (done: Boolean) { code: "return {'done': True}" }
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-in-container"),
        "expected generator-in-container for Generator[Generator[..]], got {:?}",
        codes(&d)
    );
}

#[test]
fn a_generator_nested_in_an_output_container_is_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
p = ExecPython() -> (xs: List[Generator[Number]]) { code: "return {'xs': []}" }
out = Debug
out.data = p.xs
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-in-container"),
        "expected generator-in-container on the OUTPUT side, got {:?}",
        codes(&d)
    );
}

#[test]
fn an_optional_generator_input_is_rejected() {
    // An unwired stream has no meaning (no zero value, no default), so
    // a Generator input must be required.
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
p = ExecPython(rows: Generator[Number]?) -> (done: Boolean) { code: "return {'done': True}" }
p.rows = nums.values
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-input-must-be-required"),
        "expected generator-input-must-be-required, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_stream_into_a_loop_gather_port_is_rejected() {
    // A gather is a per-iteration VALUE write; a stream wired into the
    // loop's outward boundary has no meaning.
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
my = Loop(items: List[String]) -> (results: List[String | Null], rows: Generator[Number]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
    self.rows = nums.values
}
my.items = @json(["a"])
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"generator-through-group"),
        "expected generator-through-group on the LoopOut edge, got {:?}",
        codes(&d)
    );
}

#[test]
fn two_streams_in_over_are_rejected() {
    let (project, _) = parse_enrich_lenient(
        r#"
a = Range { to: 5 }
b = Range { to: 5 }
my = Loop(xs: Generator[Number], ys: Generator[Number]) -> (results: List[Number | Null]) {
    over: ["xs", "ys"]
    p = ExecPython(n: Number, m: Number) -> (out: Number) { code: "return {'out': n}" }
    p.n = self.xs
    p.m = self.ys
    self.results = p.out
}
my.xs = a.values
my.ys = b.values
out = Debug
out.data = my.results
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"over-stream-not-alone"),
        "expected over-stream-not-alone for two streams, got {:?}",
        codes(&d)
    );
}

#[test]
fn a_stream_into_a_plain_list_input_is_a_type_mismatch() {
    // The old workaround shape (Range's list into a List port) now
    // reads as what it is: a stream is not a list.
    let (project, _) = parse_enrich_lenient(
        r#"
nums = Range { to: 5 }
p = ExecPython(xs: List[Number]) -> (done: Boolean) { code: "return {'done': True}" }
p.xs = nums.values
out = Debug
out.data = p.done
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"type-mismatch"),
        "expected type-mismatch, got {:?}",
        codes(&d)
    );
}

/// named-type-conflict: two ports restating one type name with two
/// different bodies must be a loud error (nominal compatibility
/// compares the name alone, so a silent fork would wire mismatched
/// shapes together). Built by mutating enriched port types, because
/// the wire form (`Name=Body`) is not weft-source syntax.
#[test]
fn a_named_type_restated_with_two_bodies_is_rejected() {
    let mut project = parse_enrich(
        r#"
a = Text { value: "x" }
b = Debug
b.data = a.value
"#,
    );
    let named = |body: &str| weft_core::weft_type::WeftType::Named {
        name: "Feed".into(),
        body: Box::new(weft_core::weft_type::WeftType::parse(body).expect("body parses")),
    };
    project
        .nodes
        .iter_mut()
        .find(|n| n.id == "a")
        .and_then(|n| n.outputs.first_mut())
        .expect("a.value present")
        .port_type = named("Generator[Number]");
    project
        .nodes
        .iter_mut()
        .find(|n| n.id == "b")
        .and_then(|n| n.inputs.first_mut())
        .expect("b.data present")
        .port_type = named("Generator[String]");
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"named-type-conflict"),
        "expected named-type-conflict, got {:?}",
        codes(&d)
    );
}

/// The registry half of named-type-conflict: a port restating a name
/// the catalog's registry DECLARES with a different body errors, and
/// the scope travels with `validate` itself (a caller cannot forget
/// it; the build path once did, silently no-op-ing this half).
#[test]
fn a_restatement_contradicting_the_declared_type_is_rejected() {
    struct DeclaringCatalog {
        inner: FsCatalog,
        registry: std::sync::Arc<weft_core::weft_type::TypeRegistry>,
    }
    impl weft_core::node::MetadataCatalog for DeclaringCatalog {
        fn lookup(&self, node_type: &str) -> Option<&weft_core::node::NodeMetadata> {
            self.inner.lookup(node_type)
        }
        fn all(&self) -> Vec<&weft_core::node::NodeMetadata> {
            self.inner.all()
        }
        fn type_registry(&self) -> std::sync::Arc<weft_core::weft_type::TypeRegistry> {
            self.registry.clone()
        }
    }
    let declaring = DeclaringCatalog {
        inner: catalog(),
        registry: std::sync::Arc::new(
            weft_core::weft_type::TypeRegistry::build(&[(
                "Feed".to_string(),
                "Generator[Number]".to_string(),
                "test".to_string(),
            )])
            .expect("registry builds"),
        ),
    };
    let mut project = parse_enrich(
        r#"
a = Text { value: "x" }
out = Debug
out.data = a.value
"#,
    );
    project
        .nodes
        .iter_mut()
        .find(|n| n.id == "a")
        .and_then(|n| n.outputs.first_mut())
        .expect("a.value present")
        .port_type = weft_core::weft_type::WeftType::Named {
        name: "Feed".into(),
        body: Box::new(weft_core::weft_type::WeftType::parse("Generator[String]").expect("parses")),
    };
    let d = validate(&project, &declaring);
    assert!(
        d.iter().any(|e| {
            e.code.as_deref() == Some("named-type-conflict")
                && e.message.contains("declared as")
        }),
        "expected the registry half of named-type-conflict, got {:?}",
        codes(&d)
    );
}

/// A `Switch`'s cases are checked the way a type is: before anything
/// runs. Each of these is a case that would otherwise be a branch that
/// silently never fires, or one that swallows the branches after it.
#[test]
fn a_switch_holds_its_cases_to_what_they_claim() {
    let switch_over = |cases: &str| {
        let source = format!(
            r#"
words = Text {{ value: "an error happened" }}

route = Switch {{
  value: words.value
  cases: {cases}
}}

out = Debug {{ data: route.taken }}
"#
        );
        let project = parse_enrich(&source);
        let diagnostics = validate(&project, &catalog());
        codes(&diagnostics).iter().map(|c| c.to_string()).collect::<Vec<_>>()
    };

    // A test whose value the matched input could never hold.
    assert!(switch_over(
        r#"[{ "kind": "equals", "value": 42, "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"config-entry-bad-value".to_string()));

    // A comparison against something that is not a number: the branch
    // could never be taken.
    assert!(switch_over(
        r#"[{ "kind": "gte", "value": 3, "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"config-entry-bad-value".to_string()));

    // A regular expression that does not compile.
    assert!(switch_over(
        r#"[{ "kind": "matches", "value": "[unclosed", "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"config-entry-bad-value".to_string()));

    // A case whose kind needs a value and does not carry one.
    assert!(switch_over(
        r#"[{ "kind": "equals", "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"config-entry-missing-value".to_string()));

    // A mistyped key on a case that does not take it.
    let typo = switch_over(
        r#"[{ "kind": "equals", "value": "err", "vlaue": "err", "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#,
    );
    assert!(typo.contains(&"unknown-config-entry-key".to_string()), "{typo:?}");

    // A range needs both ends.
    assert!(switch_over(
        r#"[{ "kind": "between", "min": 1, "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"config-entry-missing-value".to_string()));

    // A catch-all with entries after it: everything below could never be
    // reached.
    assert!(switch_over(
        r#"[{ "kind": "otherwise", "port": "taken" }, { "kind": "equals", "value": "err", "port": "rest" }]"#
    )
    .contains(&"catch-all-not-last".to_string()));

    // Two catch-alls.
    assert!(switch_over(
        r#"[{ "kind": "otherwise", "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"duplicate-catch-all".to_string()));

    // A kind this node does not offer.
    assert!(switch_over(
        r#"[{ "kind": "text_input", "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#
    )
    .contains(&"unknown-config-entry-kind".to_string()));

    // And the shape that is CORRECT stays clean: a test, then a
    // catch-all last.
    let clean = switch_over(
        r#"[{ "kind": "contains", "value": "err", "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#,
    );
    assert!(
        !clean.iter().any(|c| c.starts_with("config-entry") || c.starts_with("catch-all")
            || c.starts_with("unknown-config-entry") || c == "duplicate-catch-all"),
        "a well-formed switch has nothing to say: {clean:?}"
    );

    // A required field set to null is MISSING, not mistyped: the entry
    // needs a value, and the message says so instead of arguing about
    // null's type. Both directions pinned: the missing-value code
    // appears AND the bad-value code is gone, since emitting both was
    // exactly the pre-fix behavior.
    let nulled = switch_over(
        r#"[{ "kind": "equals", "value": null, "port": "taken" }, { "kind": "otherwise", "port": "rest" }]"#,
    );
    assert!(nulled.contains(&"config-entry-missing-value".to_string()), "{nulled:?}");
    assert!(!nulled.contains(&"config-entry-bad-value".to_string()), "{nulled:?}");
}

/// A present `null` on an OPTIONAL spec field counts as absent, the
/// same rule record types apply: the editor clears a box by writing
/// null, and the runtime reads null as "not filled in" (a form field's
/// label falls back to its key). Refusing it made a graph-built form
/// fail activation over a label nobody typed.
#[test]
fn a_null_optional_entry_field_counts_as_absent() {
    let project = parse_enrich(
        r#"
ask = HumanQuery {
  title: "Approve?"
  fields: [
    { "kind": "display", "key": "question", "label": null },
    { "kind": "approve_reject", "key": "send" }
  ]
}
q = Text { value: "ok to send?" }
ask.question = q.value
out = Debug { data: ask.send_approved }
"#,
    );
    let d = validate(&project, &catalog());
    // A cleared label must trip NO entry diagnostic: `label` is a
    // declared optional field, and null is how the editor clears it.
    let entry_codes: Vec<&str> = codes(&d)
        .into_iter()
        .filter(|c| c.starts_with("config-entry") || c.starts_with("unknown-config-entry"))
        .collect();
    assert!(
        entry_codes.is_empty(),
        "a cleared optional label is absent, not an error: {entry_codes:?}"
    );
}

#[test]
fn a_null_unknown_key_is_still_a_typo() {
    // null-is-absent applies to a DECLARED field's value, never to the
    // key itself: a mistyped key whose value happens to be null must
    // still surface, or the typo silently vanishes.
    let project = parse_enrich(
        r#"
ask = HumanQuery {
  title: "Approve?"
  fields: [
    { "kind": "display", "key": "question", "lbael": null },
    { "kind": "approve_reject", "key": "send" }
  ]
}
q = Text { value: "ok to send?" }
ask.question = q.value
out = Debug { data: ask.send_approved }
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"unknown-config-entry-key"),
        "a null-valued typo'd key must still be flagged: {d:?}"
    );
}

#[test]
fn an_empty_required_choice_set_is_refused() {
    // "Chose nothing" satisfies no requirement: a select with zero
    // options would ship a human a dropdown with nothing in it.
    let project = parse_enrich(
        r#"
ask = HumanQuery {
  title: "Pick"
  fields: [
    { "kind": "select", "key": "choice", "options": [] }
  ]
}
out = Debug { data: ask.choice }
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        codes(&d).contains(&"config-entry-bad-value"),
        "an empty required choice set must be refused: {d:?}"
    );
}

#[test]
fn an_empty_list_as_a_matched_value_is_legitimate() {
    // Where the list is one VALUE being matched (`equals` against a
    // list-typed input), `[]` routes the empty-list case and must
    // compile clean; only a CHOICE SET reads emptiness as "chose
    // nothing".
    let project = parse_enrich(
        r#"
sw = Switch {
  cases: [
    { "kind": "equals", "port": "empty", "value": [] },
    { "kind": "otherwise", "port": "rest" }
  ]
}
a = Debug { data: sw.empty }
b = Debug { data: sw.rest }
"#,
    );
    let d = validate(&project, &catalog());
    let entry_codes: Vec<&str> = codes(&d)
        .into_iter()
        .filter(|c| c.starts_with("config-entry"))
        .collect();
    assert!(
        entry_codes.is_empty(),
        "`equals: []` on a list input is a legitimate value: {entry_codes:?}"
    );
}

#[test]
fn output_sink_is_exempt_from_orphan_outputs() {
    // A node marked `_is_output: true` is a declared terminus: its output
    // ports going nowhere is the point, not a mistake. A node with the
    // same unconsumed outputs and no marker still gets the warning.
    let project = parse_enrich(
        r#"
t = Text { value: "hi" }
sink = Debug { _label: "sink" }
sink.value = t.value
loose = Cast(value: String) -> (value: String) {}
loose.value = t.value
"#,
    );
    let d = validate(&project, &catalog());
    let flagged: Vec<&str> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("orphan-outputs"))
        .filter_map(|x| x.message.split('\'').nth(1))
        .collect();
    assert!(flagged.contains(&"loose"), "{d:?}");
    assert!(!flagged.contains(&"t"), "t IS consumed: {d:?}");

    // Same loose node, now marked as an output: warning gone.
    let project = parse_enrich(
        r#"
t = Text { value: "hi" }
sink = Debug { _label: "sink" }
sink.value = t.value
loose = Cast(value: String) -> (value: String) { _is_output: true }
loose.value = t.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        !d.iter().any(|x| x.code.as_deref() == Some("orphan-outputs")),
        "{d:?}"
    );
}

#[test]
fn group_boundary_nodes_are_not_unknown_types() {
    // Group lowering synthesizes Passthrough boundary nodes. They have no
    // catalog entry by design; the lenient parse path must not paint them
    // as unknown node types (the phantom line-0 warnings).
    let (_, d) = weft_compiler::parse_only(
        r#"
g = Group(text: String) -> (out: String) {
  inner = Cast(value: String) -> (value: String) {}
  inner.value = self.text
  self.out = inner.value
}
g.text = "hi"
sink = Debug {}
sink.value = g.out
"#,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
        &catalog(),
        None,
    );
    assert!(
        !d.iter().any(|x| x.code.as_deref() == Some("unknown-type")),
        "{d:?}"
    );
}

/// The "no connection picked" rule is the LANGUAGE's: any node declaring a
/// `service` recipe gets it synthesized at runtime level (no metadata
/// boilerplate), and `connection_optional: true` on the recipe turns it off.
#[test]
fn access_nodes_require_a_connection_by_default() {
    use weft_compiler::validate::{validate_with_mode, ValidationMode};
    // Unconnected access node: the synthesized rule fires in Runtime mode
    // only (a sketch still builds), naming the access field's message shape.
    let project = parse_enrich("ws = SlackAccess\n");
    let runtime = validate_with_mode(&project, &catalog(), ValidationMode::Runtime);
    let hit = runtime
        .iter()
        .find(|d| d.code.as_deref() == Some("rule-runtime"))
        .expect("unconnected SlackAccess must flag rule-runtime");
    assert!(hit.message.contains("no") && hit.message.contains("connection"), "{}", hit.message);
    let structural = validate_with_mode(&project, &catalog(), ValidationMode::Structural);
    assert!(!codes(&structural).contains(&"rule-runtime"), "{structural:?}");

    // A picked connection satisfies it.
    let connected =
        parse_enrich("ws = SlackAccess { account: {\"id\":\"g-1\",\"identity\":\"q\"} }\n");
    let diags = validate_with_mode(&connected, &catalog(), ValidationMode::Runtime);
    assert!(
        !diags.iter().any(|d| d.code.as_deref() == Some("rule-runtime")
            && d.message.contains("connection")),
        "{diags:?}"
    );

    // `connection_optional: true` (CustomProvider) opts out entirely.
    let optional = parse_enrich(
        "p = CustomProvider { baseUrl: \"http://localhost:1\", model: \"m\" }\n",
    );
    let diags = validate_with_mode(&optional, &catalog(), ValidationMode::Runtime);
    assert!(
        !diags.iter().any(|d| d.message.contains("connection picked")),
        "an optional connection synthesizes no rule: {diags:?}"
    );
}

/// A metadata that still carries its OWN rule on the picker field (a
/// project's copied catalog predating the synthesized rule) reports the
/// mistake ONCE: the declared rule stands, the twin is not synthesized.
#[test]
fn declared_picker_rule_suppresses_the_synthesized_twin() {
    use weft_compiler::validate::{validate_with_mode, ValidationMode};
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("legacy");
    std::fs::create_dir_all(&node_dir).unwrap();
    std::fs::write(
        node_dir.join("metadata.json"),
        r#"{
  "type": "LegacyAccess",
  "label": "Legacy access",
  "description": "a copied catalog predating the synthesized rule",
  "service": { "service": "legacy",
               "acquisition": { "kind": "static", "fields": [{ "name": "key" }] } },
  "inputs": [
    { "name": "account", "type": "Access", "exposure": "config",
      "widget": { "kind": "access" } }
  ],
  "outputs": [{ "name": "access", "type": "Access" }],
  "validate": [
    { "when": { "kind": "not", "of": { "kind": "config_nonempty", "field": "account" } },
      "then": { "message": "the old boilerplate wording",
                "level": "runtime", "field": "account" } }
  ]
}"#,
    )
    .unwrap();
    let legacy = FsCatalog::discover(dir.path()).expect("legacy catalog");
    let mut project =
        compile("a = LegacyAccess\n", uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &legacy).expect("enrich");
    let diags = validate_with_mode(&project, &legacy, ValidationMode::Runtime);
    let connection_hits: Vec<_> =
        diags.iter().filter(|d| d.code.as_deref() == Some("rule-runtime")).collect();
    assert_eq!(connection_hits.len(), 1, "exactly one report: {diags:?}");
    assert!(
        connection_hits[0].message.contains("old boilerplate"),
        "the DECLARED rule wins: {}",
        connection_hits[0].message
    );
}

/// The mirror of the above, pinning the guard's NARROWNESS: an
/// UNRELATED declared rule on the picker field (any condition other
/// than the synthesized not-nonempty shape) must not swallow the
/// connection requirement, so the synthesized "no connection picked"
/// still fires. (The declared rule's own condition needs a picked
/// connection, so on this unpicked node only the synthesized one can.)
#[test]
fn unrelated_picker_rule_does_not_suppress_the_synthesized_one() {
    use weft_compiler::validate::{validate_with_mode, ValidationMode};
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("legacy");
    std::fs::create_dir_all(&node_dir).unwrap();
    std::fs::write(
        node_dir.join("metadata.json"),
        r#"{
  "type": "LegacyAccess",
  "label": "Legacy access",
  "description": "declares its own unrelated runtime rule on the picker field",
  "service": { "service": "legacy",
               "acquisition": { "kind": "static", "fields": [{ "name": "key" }] } },
  "inputs": [
    { "name": "account", "type": "Access", "exposure": "config",
      "widget": { "kind": "access" } }
  ],
  "outputs": [{ "name": "access", "type": "Access" }],
  "validate": [
    { "when": { "kind": "config_nonempty", "field": "account" },
      "then": { "message": "an unrelated declared rule",
                "level": "runtime", "field": "account" } }
  ]
}"#,
    )
    .unwrap();
    let legacy = FsCatalog::discover(dir.path()).expect("legacy catalog");
    let mut project =
        compile("a = LegacyAccess\n", uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &legacy).expect("enrich");
    let diags = validate_with_mode(&project, &legacy, ValidationMode::Runtime);
    let runtime_hits: Vec<_> =
        diags.iter().filter(|d| d.code.as_deref() == Some("rule-runtime")).collect();
    assert!(
        runtime_hits.iter().any(|d| d.message.contains("has no legacy connection picked")),
        "the synthesized rule still fires: {diags:?}"
    );
}

/// The `declared_type` stamp: enrich marks every port with the type the
/// SOURCE header spells for it (the editor rewrites headers from this),
/// and nothing else. A redeclared catalog port keeps the AUTHORED
/// spelling even though the merge clones the catalog port; catalog-only
/// ports, `_should_flow`, and config-created ports carry none.
#[test]
fn enrich_stamps_declared_types_from_the_header_only() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("gadget");
    std::fs::create_dir_all(&node_dir).unwrap();
    std::fs::write(
        node_dir.join("metadata.json"),
        r#"{
  "type": "Gadget",
  "label": "Gadget",
  "description": "declared-type stamping fixture",
  "inputs": [
    { "name": "data", "type": "T" },
    { "name": "seed", "type": "String", "required": false }
  ],
  "outputs": [{ "name": "out", "type": "String" }, { "name": "gen", "type": "T" }],
  "features": { "canAddInputPorts": true, "canAddOutputPorts": true }
}"#,
    )
    .unwrap();
    let catalog = FsCatalog::discover(dir.path()).expect("fixture catalog");
    let mut project = compile(
        "g = Gadget(data: T, extra: String) -> (haiku: String, gen: T) {\n  made: \"x\"\n}\n",
        uuid::Uuid::new_v4(),
        CompileFs::none(),
    )
    .expect("compile");
    enrich(&mut project, &catalog).expect("enrich");
    let g = project.nodes.iter().find(|n| n.id == "g").expect("node g");
    let input_decl = |name: &str| {
        g.inputs.iter().find(|p| p.name == name).unwrap_or_else(|| panic!("input {name}")).declared_type.clone()
    };
    assert_eq!(input_decl("data"), Some("T".into()), "redeclared catalog port keeps the authored spelling");
    assert_eq!(input_decl("extra"), Some("String".into()), "custom header port is stamped");
    assert_eq!(input_decl("seed"), None, "untouched catalog port carries no stamp");
    assert_eq!(input_decl("_should_flow"), None, "enrich's own synthesis carries none");
    assert_eq!(input_decl("made"), None, "a config-created port carries none");
    let haiku = g.outputs.iter().find(|p| p.name == "haiku").expect("output haiku");
    assert_eq!(haiku.declared_type, Some("String".into()), "custom output is stamped");
    let out = g.outputs.iter().find(|p| p.name == "out").expect("output out");
    assert_eq!(out.declared_type, None, "catalog output carries none");
    let gen = g.outputs.iter().find(|p| p.name == "gen").expect("output gen");
    assert_eq!(
        gen.declared_type,
        Some("T".into()),
        "redeclared catalog OUTPUT keeps the authored spelling through the merge"
    );
}

/// `declared_type` is the VERBATIM header annotation, never a re-print of
/// the parsed type. A re-print would expand a registry alias and, worse,
/// turn an unparseable annotation into the `MustOverride` placeholder,
/// which the editor's next ports gesture would write back over the
/// author's text (destroying both the typo and the squiggle naming it).
#[test]
fn declared_type_is_the_verbatim_header_spelling() {
    use weft_compiler::weft_compiler::{compile_lenient, IncludeMode};
    let (project, _) = compile_lenient(
        "n = Debug(data: Strng?)\n",
        uuid::Uuid::new_v4(),
        CompileFs::none(),
        IncludeMode::Full,
        None,
    );
    let n = project.nodes.iter().find(|n| n.id == "n").expect("node n");
    let data = n.inputs.iter().find(|p| p.name == "data").expect("input data");
    assert_eq!(data.declared_type, Some("Strng".into()), "the typo round-trips as typed");
    assert_eq!(data.port_type.wire_string(), "MustOverride", "the parsed type is the placeholder");
}

/// An `@include` line declares no ports: the interface node's ports come
/// from the INCLUDED file's group header, so they must carry no declared
/// spelling (a stamp would make the editor write a signature onto a decl
/// that has no signature slot).
#[test]
fn include_interface_ports_carry_no_declared_type() {
    use weft_compiler::weft_compiler::{compile_lenient, IncludeMode};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("sub.weft"),
        "Group(inp: String) -> (outp: String) {\n  t = Text { value: \"x\" }\n  self.outp = self.inp\n}\n",
    )
    .unwrap();
    let (project, _) = compile_lenient(
        "c = @include(\"sub.weft\")\n",
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
        IncludeMode::Interface,
        None,
    );
    let c = project.nodes.iter().find(|n| n.id == "c").expect("include node c");
    assert!(
        c.inputs.iter().all(|p| p.declared_type.is_none()),
        "include inputs unstamped: {:?}", c.inputs
    );
    assert!(c.outputs.iter().all(|p| p.declared_type.is_none()), "include outputs unstamped: {:?}", c.outputs);
}

/// A finding inside an `@include`d file carries that file's path on the
/// diagnostic, so every consumer can point at the right buffer.
#[test]
fn diagnostics_carry_the_included_file() {
    use weft_compiler::validate::{validate_with_mode, ValidationMode};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("sub.weft"),
        "Group() -> (out: Access) {\n  ws = SlackAccess\n  self.out = ws.access\n}\n",
    )
    .unwrap();
    let mut project = compile(
        "c = @include(\"sub.weft\")\nout = Debug\nout.data = c.out\n",
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
    )
    .expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let diags = validate_with_mode(&project, &catalog(), ValidationMode::Runtime);
    let hit = diags
        .iter()
        .find(|d| d.code.as_deref() == Some("rule-runtime"))
        .expect("the included access node flags rule-runtime");
    assert!(
        hit.file.as_deref().unwrap_or_default().ends_with("sub.weft"),
        "the finding names the included file: {:?}",
        hit.file
    );
}

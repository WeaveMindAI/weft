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
fn input_accepts_gates_each_family_whatever_the_spelling() {
    // `accepts: ["wire"]` (LlmInference's `params` port): no written
    // value in ANY spelling. The braces form...
    let project = parse_enrich(
        r#"
n = LlmInference -> (response: String) { params: {"temperature": 0.5} }
n.prompt = "hi"
out = Debug
out.data = n.response
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("input-accepts"));
    assert!(hit.is_some_and(|e| e.message.contains("`params` accepts: wire")), "braces literal on a wire-only port must error: {d:?}");

    // ...and the statement form are refused the same way.
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
    let hit = d.iter().find(|e| e.code.as_deref() == Some("input-accepts"));
    assert!(hit.is_some_and(|e| e.message.contains("`params` accepts: wire")), "statement literal on a wire-only port must error: {d:?}");

    // A file-typed port takes a marker in either spelling: the value is
    // a constant like any other, and it homes in `port_literals`.
    let project = parse_enrich(
        r#"
n = MediaDisplay { media: @asset("a.png", Image) }
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"input-accepts") && !codes(&d).contains(&"config-type-mismatch"), "{d:?}");
    let node = project.nodes.iter().find(|n| n.id == "n").unwrap();
    assert!(node.port_literals.contains_key("media"), "{:?}", node.port_literals);
    assert!(node.config.get("media").is_none(), "one home: {:?}", node.config);

    // A String where the port wants an Image is a type error, never a
    // placement error: the marker's declared type is what is checked.
    let project = parse_enrich(
        r#"
n = MediaDisplay { media: "not-a-file" }
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"config-type-mismatch"), "{d:?}");
    assert!(!codes(&d).contains(&"input-accepts"), "{d:?}");
    let project = parse_enrich(
        r#"
n = FalUpscaleImage { image: @asset("a.mp4", Video) }
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"config-type-mismatch"), "a Video marker on an Image port: {d:?}");

    // A wire-only port that is BOTH wired and written is one mistake,
    // not two: double-driven-port owns it ("remove one driver"), and the
    // family error stays silent, since the port is already wired.
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
    assert!(!codes(&d).contains(&"input-accepts"), "double-driven-port owns the wired-and-written case: {d:?}");
}

/// On a node that takes no custom inputs, a config key naming one of
/// its outputs is `value-on-output`, never a silently accepted key: an
/// output takes no value.
#[test]
fn a_value_on_an_output_of_a_fixed_node_is_refused() {
    let project = parse_enrich(
        r#"
r = Range { to: 3, values: [1] }
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("value-on-output")).unwrap_or_else(|| panic!("{d:?}"));
    assert!(hit.message.contains("'values' is an output port") && hit.message.contains("`r.values`"), "{}", hit.message);
    assert!(!codes(&d).contains(&"undeclared-port-no-custom"), "one finding, the precise one: {d:?}");
}

#[test]
fn a_written_should_flow_must_be_a_boolean() {
    let project = parse_enrich(
        r#"
t = Text { value: "x", _should_flow: "yes" }
"#,
    );
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"should-flow-not-boolean"), "{d:?}");
    let project = parse_enrich(
        r#"
t = Text { value: "x" }
t._should_flow = false
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"should-flow-not-boolean"), "{d:?}");
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
        path: Vec::new(),
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
        accepts: weft_core::node::Accepts::both(),
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
        accepts: weft_core::node::Accepts::wire_only(),
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
    // `anonymous`. The anonymous root of a component file is that file's
    // own interface (validate treats it as the includer's business); an
    // INCLUDED group is an ordinary node of the project, and the flag is
    // what tells the two apart.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("comp.weft"),
        "Group(raw: String) -> (cleaned: String) {\n s = Text { value: \"x\" }\n self.cleaned = s.value\n}\n",
    ).unwrap();

    let src = "c = @include(\"comp.weft\")\nc.raw = Text { value: \"hi\" }.value\n";
    let mut p = compile(src, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    enrich(&mut p, &catalog()).expect("enrich");
    let included = p.groups.iter().find(|g| g.id == "c").expect("the include is a group of the project");
    assert!(
        !included.anonymous,
        "an included group is a node of the project, not a component's own root: {included:?}"
    );
    let d = validate(&p, &catalog());
    assert!(d.is_empty(), "the include compiles clean: {d:?}");
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
        path: Vec::new(),
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
t1 = Cron { cron: "0 * * * * *" }
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
    t = Cron { cron: "0 * * * * *" }
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
fn an_infra_node_inside_a_loop_is_a_compile_error() {
    // No stdlib infra node fits a loop body's ports, so mark the member
    // infra by hand; the rule guards the definition shape itself.
    let mut project = parse_enrich(
        r#"
my = Loop(items: List[String]) -> (results: List[String]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
"#,
    );
    project
        .nodes
        .iter_mut()
        .find(|n| n.id == "my.p")
        .expect("the body's node is present")
        .requires_infra = true;
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"infra-in-loop"), "{d:?}");
}

#[test]
fn a_trigger_wired_into_an_infra_node_is_a_compile_error() {
    // trigger-into-infra, via a plain node in between (transitive). No
    // stdlib infra node takes inputs, so mark the sink infra by hand;
    // the rule guards the definition shape itself.
    let mut project = parse_enrich(
        r#"
t = Cron { cron: "0 * * * * *" }
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
fn a_setting_takes_a_wire_by_default_and_a_compiler_read_port_never_does() {
    // HttpRequest's `method` is a select: a setting, and a program may
    // still compute it.
    let project = parse_enrich(
        r#"
t = Text { value: "GET" }
req = HttpRequest { url: "http://x" }
req.method = t.value
out = Debug
out.data = req.body
"#,
    );
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"input-accepts"), "a setting is wireable by default: {d:?}");

    // HumanQuery's `fields` is the list its ports come from: the
    // compiler reads it to build the node, so no wire and no marker.
    let project = parse_enrich(
        r#"
src = ExecPython() -> (fields: List[JsonDict]) { code: "return {'fields': []}" }
q = HumanQuery { title: "t" }
q.fields = src.fields
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("input-accepts"));
    assert!(hit.is_some_and(|e| e.message.contains("read by the compiler")), "{d:?}");

    let project = parse_enrich(
        r#"
q = HumanQuery { title: "t", fields: @asset("https://x/fields.json", List[JsonDict]) }
"#,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("input-accepts"));
    assert!(hit.is_some_and(|e| e.message.contains("no @file, no @asset")), "{d:?}");
}

#[test]
fn a_type_mismatched_wire_onto_a_compiler_read_port_fires_only_input_accepts() {
    // The edge is illegal as a whole; input-accepts owns it. The type
    // machinery must stay silent instead of stacking type-mismatch on
    // top of the real cause.
    let project = parse_enrich(
        r#"
n = Range { from: 1, to: 3 }
q = HumanQuery { title: "t" }
q.fields = n.values
"#,
    );
    let d = validate(&project, &catalog());
    let cs = codes(&d);
    assert!(cs.contains(&"input-accepts"), "{d:?}");
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
fn declaring_a_compiler_read_input_as_a_header_port_is_an_enrich_error() {
    let mut project = compile(
        r#"
q = HumanQuery(fields: List[JsonDict]) { title: "t" }
"#,
        uuid::Uuid::new_v4(),
        CompileFs::none(),
    )
    .expect("compile ok");
    let err = enrich(&mut project, &catalog()).expect_err("a compiler-read input as a port must fail enrich");
    let msg = format!("{err}");
    assert!(msg.contains("read by the compiler"), "{msg}");
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
        cfg.port_literals.get("temperature"),
        Some(&serde_json::json!(0.7)),
        "the compiled definition carries the CAST number, in the one home"
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
my = Loop(values?: Generator[Number]) -> (results: List[Number | Null]) {
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
p = ExecPython(rows?: Generator[Number]) -> (done: Boolean) { code: "return {'done': True}" }
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

/// The warning is about upstream values arriving dead: a node whose
/// every wired input is optional gets it, a node built from written
/// constants alone (an inline `LlmParams`) has no upstream and does not.
#[test]
fn no_required_skip_needs_a_wire_to_warn_about() {
    let project = parse_enrich(
        r#"
p = LlmParams { temperature: 0.2 }
t = Text { value: "hi" }
loose = Cast(value?: String) -> (value: String) {}
loose.value = t.value
out = Debug { data: loose.value }
"#,
    );
    let d = validate(&project, &catalog());
    let warned: Vec<&str> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("no-required-skip"))
        .filter_map(|x| x.message.split('\'').nth(1))
        .collect();
    assert!(warned.contains(&"loose"), "an all-optional wired node warns: {d:?}");
    assert!(!warned.contains(&"p"), "a constants-only node has no upstream: {d:?}");
}

/// A leaf is how a program ends (the last node sends the message,
/// writes the row), so a node whose outputs nobody reads is not warned
/// about. Telling one that acts from one that only computes needs
/// something the language does not have; see TODO.md, "A node whose
/// outputs nobody reads".
#[test]
fn a_node_whose_outputs_nobody_reads_is_not_warned_about() {
    let project = parse_enrich(
        r#"
t = Text { value: "hi" }
sink = Debug { _label: "sink" }
sink.data = t.value
loose = Cast(value: String) -> (value: String) {}
loose.value = t.value
"#,
    );
    let d = validate(&project, &catalog());
    assert!(d.is_empty(), "a leaf carries no diagnostic of its own: {d:?}");
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
    { "name": "account", "type": "Access",
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
    { "name": "account", "type": "Access",
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

/// Narrowing a typevar port in the header narrows it on every port of
/// that node that shares the var. `Wait(value: String)` pins the `T`
/// the pass-through carries in AND out; before this, only the input
/// narrowed and the output stayed `T`, so the program that had just
/// named its type failed with `unresolved-typevar`.
#[test]
fn declaring_a_typevar_port_narrows_the_whole_node() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("hold");
    std::fs::create_dir_all(&node_dir).unwrap();
    std::fs::write(
        node_dir.join("metadata.json"),
        r#"{
  "type": "Hold",
  "label": "Hold",
  "description": "pass-through fixture",
  "inputs": [{ "name": "value", "type": "T", "required": false }],
  "outputs": [{ "name": "value", "type": "T" }, { "name": "many", "type": "List[T]" }]
}"#,
    )
    .unwrap();
    let catalog = FsCatalog::discover(dir.path()).expect("fixture catalog");
    let mut project =
        compile("h = Hold(value: String)\n", uuid::Uuid::new_v4(), CompileFs::none())
            .expect("compile");
    enrich(&mut project, &catalog).expect("enrich");
    let h = project.nodes.iter().find(|n| n.id == "h").expect("node h");
    let out = h.outputs.iter().find(|p| p.name == "value").expect("output value");
    assert_eq!(out.port_type.wire_string(), "String", "the pass-through output narrowed too");
    let many = h.outputs.iter().find(|p| p.name == "many").expect("output many");
    assert_eq!(many.port_type.wire_string(), "List[String]", "a nested occurrence narrows as well");
}

/// Two ports that pin the same type variable to different types is a
/// contradiction: the node would get a contract it never had, so
/// enrich refuses it naming both types.
#[test]
fn two_declarations_of_one_typevar_must_agree() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("pick");
    std::fs::create_dir_all(&node_dir).unwrap();
    std::fs::write(
        node_dir.join("metadata.json"),
        r#"{
  "type": "Pick",
  "label": "Pick",
  "description": "two ports of one var",
  "inputs": [
    { "name": "a", "type": "T", "required": false },
    { "name": "b", "type": "T", "required": false }
  ],
  "outputs": [{ "name": "out", "type": "T" }]
}"#,
    )
    .unwrap();
    let catalog = FsCatalog::discover(dir.path()).expect("fixture catalog");
    let mut project =
        compile("p = Pick(a: String, b: Number)\n", uuid::Uuid::new_v4(), CompileFs::none())
            .expect("compile");
    let err = enrich(&mut project, &catalog).expect_err("a contradiction is refused");
    let text = format!("{err:?}");
    assert!(text.contains("String") && text.contains("Number"), "{text}");

    // Agreeing declarations are fine, and narrow the whole node.
    let mut project =
        compile("p = Pick(a: String, b: String)\n", uuid::Uuid::new_v4(), CompileFs::none())
            .expect("compile");
    enrich(&mut project, &catalog).expect("agreeing declarations enrich");
    let p = project.nodes.iter().find(|n| n.id == "p").expect("node p");
    assert_eq!(
        p.outputs.iter().find(|o| o.name == "out").unwrap().port_type.wire_string(),
        "String"
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
        "n = Debug(data?: Strng)\n",
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

/// `@require_one_of` on a catalog-typed node survives enrich (the
/// catalog's features once replaced it wholesale, so it guarded
/// nothing), a name that is not a port is refused naming the inputs,
/// and a satisfied group is not warned about as "no required input".
#[test]
fn require_one_of_survives_the_catalog_merge_and_checks_its_names() {
    let project = parse_enrich(
        r#"
seed = Text { value: "x" }
act = ExecPython(a?: String, b?: String, @require_one_of(a, b)) -> (out: String) {
    code: "return {'out': a or b}"
    a: seed.value
}
"#,
    );
    let act = project.nodes.iter().find(|n| n.id == "act").expect("act");
    assert_eq!(act.features.one_of_required, vec![vec!["a".to_string(), "b".to_string()]]);
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"no-required-skip"), "the group counts as a requirement: {d:?}");
    assert!(!codes(&d).contains(&"require-one-of-unmet"), "a is wired: {d:?}");

    // A name that is not a port on a node whose ports are the
    // catalog's is a typo, and the directive guarded nothing.
    let project = parse_enrich(
        r#"
seed = Text { value: "x" }
look = Debug(@require_one_of(data, bogus))
look.data = seed.value
"#,
    );
    let d = validate(&project, &catalog());
    let unknown = d
        .iter()
        .find(|e| e.code.as_deref() == Some("require-one-of-unknown-port"))
        .expect("an unknown name in the directive is refused");
    assert!(unknown.message.contains("'bogus'") && unknown.message.contains("Inputs: ["), "{}", unknown.message);

    // Where the author can add input ports, a name is a port once the
    // header declares it, a wire lands on it, or a config key names it;
    // a name that is none of those does not exist, whatever the node
    // type, and is refused the same way.
    let project = parse_enrich(
        r#"
seed = Text { value: "x" }
act = ExecPython(@require_one_of(a, b)) -> (out: String) {
    code: "return {'out': a}"
    a: seed.value
}
"#,
    );
    let d = validate(&project, &catalog());
    let unknown = d
        .iter()
        .find(|e| e.code.as_deref() == Some("require-one-of-unknown-port"))
        .expect("`b` is no port of this instance");
    assert!(unknown.message.contains("'b'"), "{}", unknown.message);

    let project = parse_enrich(
        r#"
seed = Text { value: "x" }
act = ExecPython(b?: String, @require_one_of(a, b)) -> (out: String) {
    code: "return {'out': a}"
    a: seed.value
}
"#,
    );
    let d = validate(&project, &catalog());
    assert!(
        !codes(&d).contains(&"require-one-of-unknown-port"),
        "declared in the header, `b` exists even unwired: {d:?}"
    );
}

/// One unknown node type must not switch type inference off for the
/// whole file: the generic nodes elsewhere still resolve, so the
/// only diagnostics are the unknown type's own.
#[test]
fn an_unknown_node_type_does_not_unresolve_every_typevar() {
    let (project, _) = parse_enrich_lenient(
        r#"
seed = Text { value: "yes" }
route = Switch {
    value: seed.value
    cases: [
        { "kind": "equals", "value": "yes", "port": "go" },
        { "kind": "otherwise", "port": "stop" }
    ]
}
show = Debug { data: route.go }
ghost = NoSuchNodeType { x: seed.value }
"#,
    );
    let d = validate(&project, &catalog());
    let typevars: Vec<&Diagnostic> = d
        .iter()
        .filter(|e| e.code.as_deref() == Some("unresolved-typevar"))
        .collect();
    assert!(typevars.is_empty(), "the Switch and Debug resolved against the Text: {typevars:?}");
}

/// `self.<port>` inside a loop nested in a group, where the port is the
/// GROUP's input: the rule holds (`self` is the loop's own boundary)
/// and the message says so, naming the fix, instead of reading like
/// the port does not exist.
#[test]
fn a_loop_reading_the_enclosing_groups_self_is_told_to_thread_it_in() {
    let (project, _) = parse_enrich_lenient(
        r#"
outer = Group(db: String, items: List[String]) -> (done: List[String | Null]) {
    run = Loop(items: List[String]) -> (results: List[String | Null]) {
        over: ["items"]
        step = ExecPython(row: String, db: String) -> (out: String) {
            code: "return {'out': row + db}"
            row: self.items
            db: self.db
        }
        self.results = step.out
    }
    run.items = self.items
    self.done = run.results
}
"#,
    );
    let d = validate(&project, &catalog());
    let hint = d
        .iter()
        .find(|e| e.code.as_deref() == Some("unknown-source-port"))
        .expect("the missing port is reported");
    assert!(hint.message.contains("inside the loop 'outer.run'"), "{}", hint.message);
    assert!(hint.message.contains("enclosing group 'outer'"), "{}", hint.message);
    assert!(hint.message.contains("run.db = self.db"), "{}", hint.message);
}

/// A container's boundaries hold their ports optional by construction
/// (a closed group input reaches the inside as a closure; a loop's
/// gathers are optional), so the "no required input" warning never
/// lands on either side. The OUT side's own warning, nobody consuming
/// the loop's results, names the loop at its header (the boundary id
/// `my__out` appears nowhere in the source, and line 0 is nowhere to
/// point).
#[test]
fn boundary_warnings_name_the_group_at_its_header() {
    let project = parse_enrich(
        r#"
seed = ExecPython() -> (items: List[String]) { code: "return {'items': ['a']}" }
my = Loop(items?: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
my.items = seed.items
out = Debug { data: my.results }
"#,
    );
    let d = validate(&project, &catalog());
    let on_boundaries: Vec<&Diagnostic> = d
        .iter()
        .filter(|e| e.message.contains("__out") || e.message.contains("__in"))
        .collect();
    assert!(on_boundaries.is_empty(), "no diagnostic names a boundary node: {on_boundaries:?}");
    assert!(
        !d.iter().any(|e| e.code.as_deref() == Some("no-required-skip")),
        "a container's optional boundary is not a node with no required input: {d:?}"
    );

    let unread = parse_enrich(
        r#"
seed = ExecPython() -> (items: List[String]) { code: "return {'items': ['a']}" }
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
my.items = seed.items
out = Debug { data: seed.items }
"#,
    );
    let d = validate(&unread, &catalog());
    assert!(
        !d.iter().any(|e| e.message.contains("__out") || e.message.contains("__in")),
        "no diagnostic names a boundary node: {d:?}"
    );

    // A mistyped group port names the group, not the boundary the
    // edge was rewritten onto.
    let typo = parse_enrich(
        r#"
seed = ExecPython() -> (items: List[String]) { code: "return {'items': ['a']}" }
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
my.itmes = seed.items
out = Debug { data: my.results }
"#,
    );
    let d = validate(&typo, &catalog());
    assert!(
        d.iter().any(|e| e.code.as_deref() == Some("unknown-target-port") && e.message.contains("node 'my'")),
        "the typo is refused under the loop's name: {d:?}"
    );
    assert!(
        !d.iter().any(|e| e.message.contains("__out") || e.message.contains("__in")),
        "no diagnostic names a boundary node (the unmet required input included): {d:?}"
    );

    // Two drivers on a group port, and a wrong type into one: named
    // after the group as well.
    let driven_twice = parse_enrich(
        r#"
seed = ExecPython() -> (items: List[String], n: Number) { code: "return {'items': ['a'], 'n': 1}" }
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
my.items = seed.items
my.items = seed.n
out = Debug { data: my.results }
"#,
    );
    let d = validate(&driven_twice, &catalog());
    assert!(
        d.iter().any(|e| e.code.as_deref() == Some("duplicate-input-port") && e.message.contains("'my.items'")),
        "{d:?}"
    );
    assert!(
        !d.iter().any(|e| e.message.contains("__out") || e.message.contains("__in")),
        "no diagnostic names a boundary node: {d:?}"
    );

    // A component file's root boundaries are its interface: nothing
    // inside consumes its outputs by construction, and that warns on
    // nobody.
    let component = parse_enrich(
        r#"
Group(raw?: String) -> (cleaned: String) {
    s = Text { value: "x" }
    self.cleaned = s.value
}
"#,
    );
    let d = validate(&component, &catalog());
    assert!(
        !d.iter().any(|e| e.code.as_deref() == Some("no-required-skip")),
        "a component's optional input is the includer's to make required: {d:?}"
    );
}



// ─── Reading a key off a wire ───────────────────────────────────────────────

/// The type a dereferencing wire carries is the last key's type: a
/// String key into a Number port is a plain type mismatch naming the
/// path, and a Number key into a Number port is clean.
#[test]
fn a_path_is_type_checked_at_the_key_it_reads() {
    let clean = parse_enrich(r#"
s = ExecPython() -> (out: { profile: { wpm: Number, name?: String } }) { code: "x" }
t = ExecPython(n: Number) -> (r: Number) { code: "x" }
t.n = s.out.profile.wpm
"#);
    let d = validate(&clean, &catalog());
    assert!(!codes(&d).contains(&"type-mismatch") && !codes(&d).contains(&"deref-path"), "{d:?}");

    let wrong = parse_enrich(r#"
s = ExecPython() -> (out: { profile: { wpm: Number, name?: String } }) { code: "x" }
t = ExecPython(n: Number) -> (r: Number) { code: "x" }
t.n = s.out.profile.name
"#);
    let d = validate(&wrong, &catalog());
    let m = d.iter().find(|x| x.code.as_deref() == Some("type-mismatch")).expect("type-mismatch");
    assert!(m.message.contains("'s.out.profile.name: String'"), "{}", m.message);
}

/// A path that does not fit the type is its own error: a key that is
/// not there names what is, and a type with no keys at all (JsonDict, a
/// scalar) says to declare the shape or Cast first.
#[test]
fn a_path_that_does_not_fit_the_source_type_is_refused() {
    let missing = parse_enrich(r#"
s = ExecPython() -> (out: { profile: { wpm: Number } }) { code: "x" }
t = ExecPython(n: Number) -> (r: Number) { code: "x" }
t.n = s.out.profile.speed
"#);
    let d = validate(&missing, &catalog());
    let m = d.iter().find(|x| x.code.as_deref() == Some("deref-path")).expect("deref-path");
    assert!(m.message.contains("cannot read 's.out.profile.speed'") && m.message.contains("Available: wpm"), "{}", m.message);

    let dict = parse_enrich(r#"
s = ExecPython() -> (out: JsonDict) { code: "x" }
t = ExecPython(n: Number) -> (r: Number) { code: "x" }
t.n = s.out.wpm
"#);
    let d = validate(&dict, &catalog());
    let m = d.iter().find(|x| x.code.as_deref() == Some("deref-path")).expect("deref-path");
    assert!(m.message.contains("declare the type on the source port or Cast first"), "{}", m.message);
    assert!(!codes(&d).contains(&"type-mismatch"), "one error for one wire: {d:?}");
}

/// A port created by a dereferencing wire takes the type of the key it
/// reads, the way a plain wire gives a created port its source's type.
#[test]
fn a_created_port_takes_the_type_of_the_key_it_reads() {
    let project = parse_enrich(r#"
s = ExecPython() -> (out: { profile: { wpm: Number } }) { code: "x" }
t = ExecPython -> (r: Number) {
  code: "x"
  speed: s.out.profile.wpm
}
"#);
    let t = project.nodes.iter().find(|n| n.id == "t").unwrap();
    let speed = t.inputs.iter().find(|p| p.name == "speed").expect("created port");
    assert_eq!(speed.port_type, weft_core::weft_type::WeftType::parse("Number").unwrap());
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"unresolved-typevar"), "{d:?}");
}

/// level-too-large: 16 nodes flat on the file's top level warn once,
/// anchored on the first item of that level (the level has no header
/// of its own); the same work as three groups of six warns nothing,
/// because the warning is about what one LEVEL holds, not node count.
#[test]
fn a_level_past_fifteen_warns_and_a_grouped_one_does_not() {
    let mut flat = String::from("\n");
    for i in 0..16 {
        flat.push_str(&format!("t{i} = Debug {{ data: \"{i}\" }}\n"));
    }
    let d = validate(&parse_enrich(&flat), &catalog());
    let warned: Vec<&Diagnostic> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("level-too-large"))
        .collect();
    assert_eq!(warned.len(), 1, "one warning for the one crowded level: {d:?}");
    assert!(
        warned[0].message.contains("the top level holds 16 items"),
        "{}",
        warned[0].message
    );
    assert_eq!(warned[0].line, 2, "anchored on the level's first item: {warned:?}");

    let mut grouped = String::from("\n");
    for g in 0..3 {
        grouped.push_str(&format!("g{g} = Group -> (out: String) {{\n  # stage {g}\n"));
        for i in 0..6 {
            grouped.push_str(&format!("  d{i} = Debug {{ data: \"{g}-{i}\" }}\n"));
        }
        grouped.push_str("  last = Text { value: \"end\" }\n  self.out = last.value\n}\n");
    }
    let d2 = validate(&parse_enrich(&grouped), &catalog());
    assert!(
        !codes(&d2).contains(&"level-too-large"),
        "three groups of six is the shape the rule asks for: {d2:?}"
    );
}

/// A group takes ONE seat at its parent's table: its In and Out
/// boundary halves dedupe, so 15 plain nodes plus a group is 16 items
/// and warns, while the group's own inside (one node) stays quiet.
#[test]
fn a_group_counts_as_one_item_and_its_two_halves_dedupe() {
    let mut src = String::from("\n");
    for i in 0..15 {
        src.push_str(&format!("t{i} = Debug {{ data: \"{i}\" }}\n"));
    }
    src.push_str(
        "one = Group -> (out: String) {\n  inner = Text { value: \"x\" }\n  self.out = inner.value\n}\n",
    );
    let d = validate(&parse_enrich(&src), &catalog());
    let warned: Vec<&Diagnostic> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("level-too-large"))
        .collect();
    assert_eq!(warned.len(), 1, "16 items at the top level: {d:?}");
    assert!(
        warned[0].message.contains("the top level holds 16 items"),
        "{}",
        warned[0].message
    );
}

/// The rule reaches the inside of every group, and the warning names
/// which inside it is and lands on that group's own header line, not
/// on the file: 16 direct members of `big`.
#[test]
fn a_group_inside_past_fifteen_warns_naming_the_group() {
    let mut src = String::from("big = Group(x: String) -> (out: String) {\n");
    for i in 0..15 {
        src.push_str(&format!("  d{i} = Debug {{ data: \"{i}\" }}\n"));
    }
    src.push_str("  last = Text { value: self.x }\n  self.out = last.value\n}\nbig.x = \"seed\"\n");
    let d = validate(&parse_enrich(&src), &catalog());
    let warned: Vec<&Diagnostic> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("level-too-large"))
        .collect();
    assert_eq!(warned.len(), 1, "{d:?}");
    assert!(
        warned[0].message.contains("the inside of 'big' holds 16 items"),
        "{}",
        warned[0].message
    );
    assert_eq!(warned[0].line, 1, "anchored on the group's own header line: {warned:?}");
}

/// A level made only of groups still counts its items: 16 groups on
/// the top level with no plain node among them warn, anchored on the
/// first group's line (the level's first item).
#[test]
fn a_top_level_of_only_groups_warns_too() {
    let mut src = String::from("\n");
    for g in 0..16 {
        src.push_str(&format!(
            "g{g} = Group -> (out: String) {{\n  inner = Text {{ value: \"x\" }}\n  self.out = inner.value\n}}\n"
        ));
    }
    let d = validate(&parse_enrich(&src), &catalog());
    let warned: Vec<&Diagnostic> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("level-too-large"))
        .collect();
    assert_eq!(warned.len(), 1, "each group holds one node; only the top level is crowded: {d:?}");
    assert!(
        warned[0].message.contains("the top level holds 16 items"),
        "{}",
        warned[0].message
    );
    assert_eq!(warned[0].line, 2, "anchored on the level's first item: {warned:?}");
}

/// Every level answers for itself, each on its own line: one file with
/// a crowded top level AND a crowded group inside gets two warnings at
/// two different lines, not one warning painted over the file.
#[test]
fn crowded_levels_warn_each_on_its_own_line() {
    let mut lines: Vec<String> = vec![String::new()];
    for i in 0..15 {
        lines.push(format!("a{i} = Debug {{ data: \"{i}\" }}"));
    }
    let wrap_line = lines.len() + 1;
    lines.push("wrap = Group(x: String) -> (out: String) {".to_string());
    for i in 0..15 {
        lines.push(format!("  d{i} = Debug {{ data: \"w{i}\" }}"));
    }
    lines.push("  last = Text { value: self.x }".to_string());
    lines.push("  self.out = last.value".to_string());
    lines.push("}".to_string());
    lines.push("wrap.x = \"seed\"".to_string());
    let src = lines.join("\n");

    let d = validate(&parse_enrich(&src), &catalog());
    let warned: Vec<&Diagnostic> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("level-too-large"))
        .collect();
    assert_eq!(warned.len(), 2, "two crowded levels, two warnings: {d:?}");
    let top = warned
        .iter()
        .find(|x| x.message.contains("the top level"))
        .expect("the top level (15 nodes + the group) holds 16");
    let inside = warned
        .iter()
        .find(|x| x.message.contains("the inside of 'wrap'"))
        .expect("the inside of wrap holds 16");
    assert_eq!(top.line, 2, "the top level's first item: {top:?}");
    assert_eq!(inside.line, wrap_line, "the group's own header line: {inside:?}");
}

/// A loop body is a level like any other: 16 direct members of a loop
/// warn, naming the loop's inside and landing on its header line.
#[test]
fn a_loop_body_past_fifteen_warns_naming_the_loop() {
    let mut src = String::from(
        "l = Loop(items: List[String]) -> (results: List[String | Null]) {\n  over: [\"items\"]\n",
    );
    for i in 0..15 {
        src.push_str(&format!("  d{i} = Debug {{ data: \"{i}\" }}\n"));
    }
    src.push_str("  last = Text { value: \"x\" }\n  self.results = last.value\n}\nl.items = [\"a\"]\n");
    let d = validate(&parse_enrich(&src), &catalog());
    let warned: Vec<&Diagnostic> = d
        .iter()
        .filter(|x| x.code.as_deref() == Some("level-too-large"))
        .collect();
    assert_eq!(warned.len(), 1, "{d:?}");
    assert!(
        warned[0].message.contains("the inside of 'l' holds 16 items"),
        "{}",
        warned[0].message
    );
    assert_eq!(warned[0].line, 1, "anchored on the loop's own header line: {warned:?}");
}

/// A `null` constant is data only on a port whose type admits Null.
/// There it fills a required port and satisfies a `@require_one_of`; on
/// a plain port it is no value, and the rules read it the way the firing
/// will. (The source grammar refuses a bare `null`; the constant reaches
/// the port through the editor's projection.)
#[test]
fn a_null_constant_fills_a_nullable_port_and_no_other() {
    let mut project = parse_enrich(
        r#"
act = ExecPython(a: String | Null, b?: String, @require_one_of(a, b)) -> (out: String) {
  code: "return {'out': 'x'}"
}
"#,
    );
    project.nodes[0].port_literals.insert("a".into(), serde_json::Value::Null);
    let d = validate(&project, &catalog());
    assert!(!codes(&d).contains(&"required-port-unmet"), "null is data on `String | Null`: {d:?}");
    assert!(!codes(&d).contains(&"require-one-of-unmet"), "{d:?}");

    let mut project = parse_enrich(
        r#"
act = ExecPython(a: String, b?: String, @require_one_of(a, b)) -> (out: String) {
  code: "return {'out': 'x'}"
}
"#,
    );
    project.nodes[0].port_literals.insert("a".into(), serde_json::Value::Null);
    let d = validate(&project, &catalog());
    assert!(codes(&d).contains(&"required-port-unmet"), "null on a plain String is no value: {d:?}");
    assert!(codes(&d).contains(&"require-one-of-unmet"), "{d:?}");
}

/// A text marker on a compiler-read port is read into its value by the
/// compile, so the ban has to see the field's file-ref record rather
/// than the marker text.
#[test]
fn a_text_marker_on_a_compiler_read_port_is_refused_after_it_resolved() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("fields.json"), "[]").unwrap();
    let mut project = compile(
        "q = HumanQuery { title: \"t\", fields: @file(\"fields.json\", List[JsonDict]) }\n",
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
    )
    .expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    assert_eq!(project.nodes[0].port_literals["fields"], serde_json::json!([]), "the marker resolved");
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("input-accepts"));
    assert!(hit.is_some_and(|e| e.message.contains("no @file, no @asset")), "{d:?}");
}

/// The object form on a connection widget is legal by the widget's
/// contract, and held to it: a pick without a string `id` is refused
/// at compile time with the rule the runtime applies to the bag.
#[test]
fn a_malformed_widget_handle_is_refused_at_compile_time() {
    let project = parse_enrich(
        r##"
ws = SlackAccess
send = SlackSendMessage { channel: {"label": "#general"}, text: "hi" }
send.account = ws.access
"##,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("config-type-mismatch"));
    assert!(hit.is_some_and(|e| e.message.contains("string `id`")), "{d:?}");

    let project = parse_enrich(
        r##"
ws = SlackAccess { account: {"identity": 42} }
"##,
    );
    let d = validate(&project, &catalog());
    let hit = d.iter().find(|e| e.code.as_deref() == Some("config-type-mismatch"));
    assert!(hit.is_some_and(|e| e.message.contains("string `id`")), "{d:?}");
}

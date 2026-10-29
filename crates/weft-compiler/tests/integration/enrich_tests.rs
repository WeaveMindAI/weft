//! End-to-end compile + enrich tests against the stdlib catalog.

use weft_catalog::{stdlib_root, FsCatalog};
use crate::rig::{compile, enrich};
use weft_compiler::CompileFs;

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib")
}

#[test]
fn enrich_text_debug_chain() {
    let source = r#"

greeting = Text { value: "hello" }
out = Debug

out.data = greeting.value
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");

    let text = project.nodes.iter().find(|n| n.id == "greeting").unwrap();
    assert_eq!(text.node_type, "Text");
    // Its own `value`, plus the failure output every node carries.
    assert_eq!(
        text.outputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        vec!["value", weft_core::node::ERROR_PORT]
    );

    let debug = project.nodes.iter().find(|n| n.id == "out").unwrap();
    assert_eq!(debug.node_type, "Debug");
    // Its own `data`, plus both spellings of the gate, which every node
    // carries (an author uses one of them; wiring both is refused).
    assert_eq!(
        debug.inputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        vec!["data", "_should_flow", "_should_not_flow"]
    );
}

#[test]
fn enrich_marks_spec_inputs_and_not_instance_ports() {
    // The spec/instance split is what `ctx.inputs.custom()` keys off:
    // `code` comes from ExecPython's own metadata (a setting), `a` is a
    // user-declared header port (instance data).
    let source = r#"
py = ExecPython(a: Number) -> (b: Number) { code: "return {'b': a}" }
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let py = project.nodes.iter().find(|n| n.id == "py").unwrap();
    let code = py.inputs.iter().find(|i| i.name == "code").expect("spec input");
    let a = py.inputs.iter().find(|i| i.name == "a").expect("instance port");
    assert!(code.from_spec, "a metadata-declared input is marked from_spec");
    assert!(!a.from_spec, "a user-declared header port is instance data");
}

#[test]
fn enrich_resolves_typevar_through_edge() {
    let source = r#"

hello = Text { value: "hi" }
sink = Debug

sink.data = hello.value
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");

    let sink = project.nodes.iter().find(|n| n.id == "sink").unwrap();
    let value_port = sink.inputs.iter().find(|p| p.name == "data").unwrap();
    // Debug declares `data: T`; wiring Text.value (String) upstream
    // should resolve T to String.
    assert_eq!(
        value_port.port_type.to_string(),
        "String",
        "expected T resolved to String, got {}",
        value_port.port_type,
    );
}

/// A source's type variable binds against the first group port that
/// declares a type, read with only the keys written up to that port: a
/// key the inside reads past it (`self.x.name`) is not part of what the
/// source sends.
#[test]
fn a_typevar_binds_against_the_group_port_with_the_keys_before_it() {
    let source_out = |out: &str| format!(
        r#"
a = ExecPython() -> (out: {out}) {{ code: "return {{'out': {{'name': 'x'}}}}" }}
g = Group(x: {{ name: String }}) -> (y: String) {{
    m = Text {{ value: self.x.name }}
    self.y = m.value
}}
g.x = a.out
"#
    );
    let out_type = |source: &str| {
        let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
        enrich(&mut project, &catalog()).expect("enrich");
        let a = project.nodes.iter().find(|n| n.id == "a").unwrap();
        a.outputs.iter().find(|p| p.name == "out").unwrap().port_type.clone()
    };
    let named = weft_core::weft_type::WeftType::parse("{ name: String }").unwrap();
    // `{ name: T }` against `{ name: String }`: T is String.
    assert_eq!(out_type(&source_out("{ name: T }")), named);
    // `T` against `{name: String}`: T is the whole record.
    assert_eq!(out_type(&source_out("T")), named);
}

#[test]
fn enrich_rejects_unknown_node_type() {
    let source = r#"

bad = NotARealNode
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    let msg = err;
    assert!(msg.contains("NotARealNode"), "expected NotARealNode in error, got: {msg}");
}

#[test]
fn enrich_rejects_custom_port_on_node_that_disallows_it() {
    // `Text` does not set canAddOutputPorts, so a user-declared custom
    // output port on it must fail loud, not vanish silently.
    let source = r#"

t = Text() -> (bogus: String) { value: "hi" }
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    let msg = err;
    assert!(
        msg.contains("does not support custom") && msg.contains("bogus"),
        "expected a loud custom-port rejection naming the port, got: {msg}"
    );
}

/// Every node already has the `_error` output the runtime fills: a
/// source port of that name is refused, naming the fix, on a node that
/// accepts custom outputs and on one that does not. It is the ONE error
/// that port earns: a type that disagrees with the language's own
/// `_error` adds no mismatch on top, and a node that takes no custom
/// outputs adds no custom-port refusal. `error` is an ordinary name.
#[test]
fn enrich_rejects_an_error_port_declared_on_any_node() {
    let cases = [
        ("ExecPython", "String", "code: \"return {}\""),
        ("ExecPython", "Number", "code: \"return {}\""),
        ("Text", "String", "value: \"x\""),
    ];
    for (node, declared, config) in cases {
        let source = format!("\nq = {node}() -> (_error: {declared}) {{ {config} }}\n");
        let mut project = compile(&source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
        let errors = weft_compiler::enrich::enrich_collecting(&mut project, &catalog(), weft_compiler::enrich::EnrichPolicy::Strict);
        assert_eq!(errors.len(), 1, "{node} {declared}: {:?}", errors.iter().map(|e| &e.message).collect::<Vec<_>>());
        assert!(
            errors[0].message.contains("'_error' is the output every node already has for its failures"),
            "{node} {declared}: expected the error-port refusal, got: {}",
            errors[0].message
        );
    }
    let mut project = compile(
        "\nq = ExecPython() -> (error: String) { code: \"return {}\" }\n",
        uuid::Uuid::new_v4(),
        CompileFs::none(),
    )
    .expect("compile");
    enrich(&mut project, &catalog()).expect("`error` is a name a program may give its own port");
    let q = project.nodes.iter().find(|n| n.id == "q").unwrap();
    let names: Vec<&str> = q.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains(&"error") && names.contains(&weft_core::node::ERROR_PORT), "{names:?}");
}

/// An output takes no value: a firing emits on it. On a node that
/// accepts custom inputs, a braces key, a statement, or a wire aimed at
/// one of its outputs is refused instead of quietly creating an input
/// of the same name beside the output.
#[test]
fn a_value_on_an_output_port_is_refused_instead_of_creating_an_input() {
    let cases = [
        ("braces", "t = ExecPython -> (o: String) {\n  code: \"return {}\"\n  o: \"hi\"\n}\n"),
        ("statement", "t = ExecPython -> (o: String) { code: \"return {}\" }\nt.o = \"hi\"\n"),
        ("wire", "s = Text { value: \"x\" }\nt = ExecPython -> (o: String) { code: \"return {}\" }\nt.o = s.value\n"),
    ];
    for (name, src) in cases {
        let mut project = compile(src, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
        let err = enrich(&mut project, &catalog()).expect_err(name);
        let msg = err;
        assert!(
            msg.contains("'o' is an output port") && msg.contains("takes no value") && msg.contains("`t.o`"),
            "{name}: {msg}"
        );
    }
    // The same name on both sides (a passthrough's `value`/`value`)
    // is an input first: the value lands there.
    let mut project = compile("t = Text { value: \"x\" }\n", uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("an input that shares its output's name takes the value");
    // The opt-in on a custom node: declare the input in the signature and
    // every spelling drives it again.
    let declared = [
        ("braces", "t = ExecPython(o: String) -> (o: String) {\n  code: \"return {}\"\n  o: \"hi\"\n}\n"),
        ("statement", "t = ExecPython(o: String) -> (o: String) { code: \"return {}\" }\nt.o = \"hi\"\n"),
        ("wire", "s = Text { value: \"x\" }\nt = ExecPython(o: String) -> (o: String) { code: \"return {}\" }\nt.o = s.value\n"),
    ];
    for (name, src) in declared {
        let mut project = compile(src, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
        enrich(&mut project, &catalog()).unwrap_or_else(|e| panic!("{name}: a declared input takes the value: {e}"));
        let t = project.nodes.iter().find(|n| n.id == "t").unwrap();
        assert!(t.inputs.iter().any(|p| p.name == "o") && t.outputs.iter().any(|p| p.name == "o"), "{name}");
    }
}

// ─── Layer 2 wire-shape: Loop boundary nodes round-trip ────────────────────

#[test]
fn loop_in_loop_out_round_trip_through_serde() {
    let source = r#"
my = Loop(items: List[String], threshold: Number) -> (results: List[String | Null], acc: String) {
    parallel: false
    over: ["items"]
    carry: ["acc"]
    maxIters: 50
    trimOnMismatch: true
    p = Text {}
    p.value = self.items
    self.results = p.value
    self.acc = p.value
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");

    // Round-trip the full project through serde to pin the wire shape.
    let v = serde_json::to_value(&project).expect("serialize");
    let back: weft_core::ProjectDefinition = serde_json::from_value(v.clone()).expect("deserialize");

    let in_node = back.nodes.iter().find(|n| n.node_type == "LoopIn").expect("LoopIn present");
    let out_node = back.nodes.iter().find(|n| n.node_type == "LoopOut").expect("LoopOut present");

    // Loop config lives on LoopIn ONLY (the engine reads it from
    // there; mirroring it on LoopOut would create two sources of
    // truth for the same fields). LoopOut carries only the parent
    // pointer.
    assert_eq!(in_node.config["parallel"], serde_json::json!(false));
    assert_eq!(in_node.config["over"], serde_json::json!(["items"]));
    assert_eq!(in_node.config["carry"], serde_json::json!(["acc"]));
    assert_eq!(in_node.config["maxIters"], serde_json::json!(50));
    assert_eq!(in_node.config["trimOnMismatch"], serde_json::json!(true));
    assert_eq!(out_node.config["parentId"], serde_json::json!(in_node.config["parentId"]));
    assert!(out_node.config.get("parallel").is_none(), "LoopOut must not carry the loop config; LoopIn is authoritative");
    assert!(out_node.config.get("over").is_none());
    assert!(out_node.config.get("carry").is_none());

    // LoopIn ports: outer-in inputs reflect the user signature
    // (`List[String]` for items, `Number` for threshold, `String` for the
    // auto-created `acc` carry input). Inside-out outputs carry the
    // element type for `over` ports plus `index: Number`.
    let in_input_names: Vec<&str> = in_node.inputs.iter().map(|p| p.name.as_str()).collect();
    assert!(in_input_names.contains(&"items"), "{:?}", in_input_names);
    assert!(in_input_names.contains(&"threshold"), "{:?}", in_input_names);
    assert!(in_input_names.contains(&"acc"), "carry input auto-created: {:?}", in_input_names);

    let in_output_names: Vec<&str> = in_node.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(in_output_names.contains(&"items"), "iter inside-out: {:?}", in_output_names);
    assert!(in_output_names.contains(&"threshold"), "broadcast inside-out: {:?}", in_output_names);
    assert!(in_output_names.contains(&"acc"), "carry read inside-out: {:?}", in_output_names);
    assert!(in_output_names.contains(&"index"), "implicit self.index: {:?}", in_output_names);

    // LoopOut: inside-in inputs carry gather-write + carry-write + done.
    let out_input_names: Vec<&str> = out_node.inputs.iter().map(|p| p.name.as_str()).collect();
    assert!(out_input_names.contains(&"results"), "gather write: {:?}", out_input_names);
    assert!(out_input_names.contains(&"acc"), "carry write: {:?}", out_input_names);
    assert!(out_input_names.contains(&"done"), "implicit self.done: {:?}", out_input_names);

    let out_output_names: Vec<&str> = out_node.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(out_output_names.contains(&"results"), "outer gather: {:?}", out_output_names);
    assert!(out_output_names.contains(&"acc"), "outer carry: {:?}", out_output_names);
}

#[test]
fn loop_in_iter_port_inside_type_is_element() {
    let source = r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    let in_node = project.nodes.iter().find(|n| n.node_type == "LoopIn").expect("LoopIn");
    // outer-in items: List[String].
    let outer_items = in_node.inputs.iter().find(|p| p.name == "items").expect("items input");
    assert!(matches!(outer_items.port_type, weft_core::weft_type::WeftType::List(_)),
        "outer-in items should be List[T]: {:?}", outer_items.port_type);
    // inside-out items: String (element type).
    let inside_items = in_node.outputs.iter().find(|p| p.name == "items").expect("items output");
    assert!(matches!(inside_items.port_type, weft_core::weft_type::WeftType::Primitive(_)),
        "inside-out items should be element type, not list: {:?}", inside_items.port_type);
}

#[test]
fn loop_config_defaults_round_trip() {
    // Optional knobs (`maxIters`, `trimOnMismatch`) are absent in the
    // source; the runtime applies its defaults at fire time. The wire
    // shape just preserves what the user wrote.
    let source = r#"
my = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: true
    over: ["items"]
    p = Text {}
    p.value = self.items
    self.results = p.value
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let in_node = project.nodes.iter().find(|n| n.node_type == "LoopIn").expect("LoopIn");
    assert!(in_node.config.get("maxIters").is_none(), "unset maxIters absent from wire");
    assert!(in_node.config.get("trimOnMismatch").is_none(), "unset trimOnMismatch absent from wire");
}

// ─── Container nesting matrix ──────────────────────────────────────────────
// Group/Loop can be nested arbitrarily: Group-in-Group, Group-in-Loop,
// Loop-in-Group, Loop-in-Loop. A regression in the lowering's per-decl
// match would silently drop one of these nestings (the Loop-in-Group case
// historically vanished at compile because lower_group's body match had no
// K::LOOP_DECL arm). These tests pin the matrix so a stray drop fails loud.

#[test]
fn loop_inside_group_lowers() {
    let source = r#"
outer = Group() {
    inner = Loop(items: List[String]) -> (results: List[String | Null]) {
        parallel: false
        over: ["items"]
        p = Text {}
        p.value = self.items
        self.results = p.value
    }
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    // The inner Loop lowers to a LoopIn/LoopOut pair scoped under `outer`.
    let loop_in_id = "outer.inner__in";
    let loop_out_id = "outer.inner__out";
    assert!(
        project.nodes.iter().any(|n| n.id == loop_in_id && n.node_type == "LoopIn"),
        "Loop nested in Group must lower to a LoopIn; node ids: {:?}",
        project.nodes.iter().map(|n| n.id.clone()).collect::<Vec<_>>()
    );
    assert!(
        project.nodes.iter().any(|n| n.id == loop_out_id && n.node_type == "LoopOut"),
        "Loop nested in Group must lower to a LoopOut",
    );
}

#[test]
fn loop_inside_loop_lowers() {
    // Loops nest inside loops the same way Groups do: same id namespace,
    // same per-iteration frame stack push.
    let source = r#"
outer = Loop(rows: List[String]) -> (results: List[String | Null]) {
    parallel: false
    over: ["rows"]
    inner = Loop(items: List[String]) -> (results: List[String | Null]) {
        parallel: false
        over: ["items"]
        p = Text {}
        p.value = self.items
        self.results = p.value
    }
    self.results = inner.results
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    assert!(project.nodes.iter().any(|n| n.id == "outer__in" && n.node_type == "LoopIn"));
    assert!(project.nodes.iter().any(|n| n.id == "outer__out" && n.node_type == "LoopOut"));
    assert!(project.nodes.iter().any(|n| n.id == "outer.inner__in" && n.node_type == "LoopIn"));
    assert!(project.nodes.iter().any(|n| n.id == "outer.inner__out" && n.node_type == "LoopOut"));
}

#[test]
fn group_inside_loop_lowers() {
    let source = r#"
outer = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: false
    over: ["items"]
    inner = Group() {
        p = Text {}
    }
    self.results = self.items
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    // A group is no node: it stays a group, inside the loop, around its
    // member.
    assert!(!project.nodes.iter().any(|n| n.id.starts_with("outer.inner__")), "a group lowers to no boundary node");
    let inner = project.groups.iter().find(|g| g.id == "outer.inner").expect("the group");
    assert_eq!(inner.parent_group_id.as_deref(), Some("outer"));
    let p = project.nodes.iter().find(|n| n.id == "outer.inner.p").expect("its member");
    assert_eq!(p.scope, vec!["outer".to_string(), "outer.inner".to_string()]);
}

#[test]
fn deeply_nested_loop_in_group_in_loop_in_loop_lowers() {
    // The four-way nesting the user asked for. A regression in either
    // lower_group's or lower_loop's per-decl match would lose one of the
    // levels silently.
    let source = r#"
l1 = Loop(items: List[String]) -> (results: List[String | Null]) {
    parallel: false
    over: ["items"]
    l2 = Loop(items: List[String]) -> (results: List[String | Null]) {
        parallel: false
        over: ["items"]
        g = Group() {
            l3 = Loop(items: List[String]) -> (results: List[String | Null]) {
                parallel: false
                over: ["items"]
                p = Text {}
                p.value = self.items
                self.results = p.value
            }
        }
        self.results = self.items
    }
    self.results = self.items
}
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    let want_ids = [
        ("l1__in", "LoopIn"),
        ("l1__out", "LoopOut"),
        ("l1.l2__in", "LoopIn"),
        ("l1.l2__out", "LoopOut"),
        ("l1.l2.g.l3__in", "LoopIn"),
        ("l1.l2.g.l3__out", "LoopOut"),
    ];
    let ids: Vec<(String, String)> = project
        .nodes
        .iter()
        .map(|n| (n.id.clone(), n.node_type.clone()))
        .collect();
    for (want_id, want_type) in want_ids {
        assert!(
            ids.iter().any(|(i, t)| i == want_id && t == want_type),
            "missing {want_id} ({want_type}) in nested lowering; got: {ids:?}"
        );
    }
    let g = project.groups.iter().find(|g| g.id == "l1.l2.g").expect("the group between the loops");
    assert_eq!(g.parent_group_id.as_deref(), Some("l1.l2"));
    assert!(!ids.iter().any(|(id, _)| id.starts_with("l1.l2.g__")), "a group lowers to no boundary node");
}

#[test]
fn human_trigger_derives_ports_from_minimal_fields() {
    // The editor emits fields as `{ kind, key }` ONLY: render + config
    // are inherited from the node's port specs at enrich time, never
    // duplicated into the source. This proves the minimal shape derives the
    // full set of ports (approve_reject -> {key}Approved/{key}Rejected
    // Booleans; text_input -> {key} String), so a lean source is not a lossy
    // one. Regression guard for the "emit render/config baggage" bug.
    let source = r#"

human = HumanTrigger() {
  fields: [
    { "kind": "text_input", "key": "answer" },
    { "kind": "approve_reject", "key": "review" }
  ]
}
out = Debug

out.data = human.answer
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");

    let human = project.nodes.iter().find(|n| n.id == "human").unwrap();
    let out_names: Vec<&str> = human.outputs.iter().map(|p| p.name.as_str()).collect();
    // text_input adds a String OUTPUT named after the key; approve_reject adds
    // the two Boolean decision outputs.
    assert!(out_names.contains(&"answer"), "text_input output missing; got {out_names:?}");
    assert!(out_names.contains(&"reviewApproved"), "approve output missing; got {out_names:?}");
    assert!(out_names.contains(&"reviewRejected"), "reject output missing; got {out_names:?}");

    let approved = human.outputs.iter().find(|p| p.name == "reviewApproved").unwrap();
    assert_eq!(approved.port_type.to_string(), "Boolean");
    let answer = human.outputs.iter().find(|p| p.name == "answer").unwrap();
    assert_eq!(answer.port_type.to_string(), "String");
}

#[test]
fn human_trigger_accepts_a_matching_declared_port_header() {
    // A hand-authored `.weft` MAY re-declare the form-derived ports in the
    // header. That is optional, and accepted IFF each declared port matches a
    // derived one by name AND type (here `testApproved`/`testRejected` are
    // exactly the Booleans `approve_reject` derives). This must NOT error as a
    // "custom output port" even though HumanTrigger forbids custom ports.
    let source = r#"

human = HumanTrigger() -> (testApproved: Boolean, testRejected: Boolean) {
  fields: [ { "kind": "approve_reject", "key": "test" } ]
}
out = Debug

out.data = human.testApproved
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("a matching header must enrich cleanly");

    let human = project.nodes.iter().find(|n| n.id == "human").unwrap();
    let out_names: Vec<&str> = human.outputs.iter().map(|p| p.name.as_str()).collect();
    // No duplicate ports from the redeclaration.
    assert_eq!(
        out_names.iter().filter(|n| **n == "testApproved").count(),
        1,
        "declared port must merge with the derived one, not duplicate; got {out_names:?}"
    );
    assert!(out_names.contains(&"testRejected"));
}

#[test]
fn human_trigger_rejects_a_mismatched_declared_port() {
    // A header port whose NAME matches a derived port but whose TYPE does not
    // (String vs the derived Boolean) is a real authoring error, not a silent
    // coercion. And a header port whose name matches NOTHING derived is the
    // genuine custom-port error. Both must surface.
    let wrong_type = r#"

human = HumanTrigger() -> (testApproved: String) {
  fields: [ { "kind": "approve_reject", "key": "test" } ]
}
out = Debug
out.data = human.testRejected
"#;
    let mut project = compile(wrong_type, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    assert!(err.contains("always gives it Boolean"), "type mismatch must error: {err}");

    let unknown_port = r#"

human = HumanTrigger() -> (not_a_field: Boolean) {
  fields: [ { "kind": "approve_reject", "key": "test" } ]
}
out = Debug
out.data = human.testApproved
"#;
    let mut project = compile(unknown_port, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    assert!(
        err.contains("custom") && err.contains("not_a_field"),
        "a header port matching no derived port must be a custom-port error: {err}"
    );
}

/// A node that hands out a connection to something it runs itself
/// carries that service's recipe from compile time onward. The worker
/// ships only the node types the project uses, so the service's own
/// access node is usually absent from it; resolving here, against the
/// whole catalog, is what makes publishing work without the user
/// dropping an unused access node into their graph.
#[test]
fn enrich_carries_the_recipe_a_node_publishes_against() {
    let source = r#"
db = PostgresDatabase
q = PostgresExecuteQuery { query: "SELECT 1" }
q.account = db.access
out = Debug
out.data = q.rows
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let db = project.nodes.iter().find(|n| n.id == "db").unwrap();
    let spec = db
        .published_service
        .as_ref()
        .expect("the database node carries the recipe it publishes against");
    assert_eq!(spec.service, "postgres");
    assert!(
        spec.own_fields().iter().any(|f| f.name == "password"),
        "the recipe is the real one, fields and all"
    );
    let q = project.nodes.iter().find(|n| n.id == "q").unwrap();
    assert!(q.published_service.is_none(), "a node that publishes nothing carries nothing");
}

/// A config key that names no declared port CREATES one, on a node type
/// that accepts custom inputs. The ports come out in the order the keys
/// were written, which is what a node that reads its inputs by order
/// (`FirstInOrder`) depends on: config lands in a sorted map, so
/// alphabetical order would silently reorder the branches.
#[test]
fn created_ports_follow_source_order() {
    let source = r#"
zulu = Text { value: "z" }
alpha = Text { value: "a" }

pick = FirstInOrder {
  zebra: zulu.value
  apple: alpha.value
}
out = Debug
out.data = pick.value
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let pick = project.nodes.iter().find(|n| n.id == "pick").unwrap();
    let created: Vec<&str> = pick
        .inputs
        .iter()
        .filter(|p| !weft_core::run::skip::is_gate_port(&p.name))
        .map(|p| p.name.as_str())
        .collect();
    assert_eq!(created, vec!["zebra", "apple"], "written order, not alphabetical");
    assert!(
        pick.inputs.iter().all(|p| weft_core::run::skip::is_gate_port(&p.name) || !p.required),
        "FirstInOrder's created inputs are optional by nature: a cut branch must not skip it"
    );
}

/// A created port takes the type of what feeds it: the wire's source
/// port for a wire, the literal's own type for a literal. `FirstInOrder`
/// is the exception that proves the rule for a JOIN: its metadata names
/// ONE type variable for every created input, so two branches of
/// different types fail to unify instead of meeting at an unresolved
/// port.
#[test]
fn a_created_port_takes_the_type_of_what_feeds_it() {
    let source = r#"
words = Text { value: "hi" }

py = ExecPython -> (out: String) {
  code: "return {'out': text}"
  text: words.value
  limit: 3
  flag: true
}
out = Debug
out.data = py.out
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let py = project.nodes.iter().find(|n| n.id == "py").unwrap();
    let port_type = |name: &str| {
        py.inputs.iter().find(|p| p.name == name).map(|p| p.port_type.to_string()).unwrap()
    };
    assert_eq!(port_type("text"), "String", "a wired port takes its source's type");
    assert_eq!(port_type("limit"), "Number", "a literal port takes the literal's type");
    assert_eq!(port_type("flag"), "Boolean");
}

/// Two branches of different types meeting at a join is a type error,
/// not an unresolved port: every created input on `FirstInOrder` shares
/// the output's type variable.
#[test]
fn a_join_refuses_two_branches_of_different_types() {
    let source = r#"
words = Text { value: "hi" }
counter = Range { to: 3 }

pick = FirstInOrder {
  first: words.value
  second: counter.values
}
out = Debug
out.data = pick.value
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich binds T to the first branch");
    let diagnostics = weft_compiler::validate::validate(&project, &catalog());
    assert!(
        diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("type-mismatch")
                && d.message.contains("pick.second")),
        "the second branch cannot also be T, and the diagnostic names it: {diagnostics:?}"
    );
}

/// `?` on a config key makes the port that key creates optional, and it
/// is refused on a key that creates no port, where it would silently
/// mean nothing.
#[test]
fn the_optional_marker_applies_to_created_ports_only() {
    let source = r#"
words = Text { value: "hi" }

py = ExecPython -> (out: String) {
  code: "return {'out': text}"
  text?: words.value
}
out = Debug
out.data = py.out
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let py = project.nodes.iter().find(|n| n.id == "py").unwrap();
    let text = py.inputs.iter().find(|p| p.name == "text").unwrap();
    assert!(!text.required, "`text?:` created an optional port");

    let on_a_declared_key = r#"
py = ExecPython -> (out: String) {
  code?: "return {'out': 1}"
}
out = Debug
out.data = py.out
"#;
    let mut project =
        compile(on_a_declared_key, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).expect_err("`code` is a declared config input");
    assert!(
        err.contains("not a port this node creates"),
        "the marker is refused where it cannot apply: {err}"
    );
}

/// `_should_flow: false` written straight into a node turns it off, in
/// either form a literal can take. It has to reach `port_literals`,
/// which is where the skip rule looks: left in `config` it would be a
/// setting nothing reads.
#[test]
fn a_false_flow_literal_lands_where_the_skip_rule_reads_it() {
    for source in [
        r#"
a = Text { value: "hi" }
off = Debug {
  data: a.value
  _should_flow: false
}
"#,
        r#"
a = Text { value: "hi" }
off = Debug
off.data = a.value
off._should_flow = false
"#,
    ] {
        let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
        enrich(&mut project, &catalog()).expect("enrich");
        let off = project.nodes.iter().find(|n| n.id == "off").unwrap();
        assert_eq!(
            off.port_literals.get("_should_flow"),
            Some(&serde_json::Value::Bool(false)),
            "the literal drives the port, in either form"
        );
    }
}

/// Every node carries `_should_flow`, the port that decides whether it
/// runs, and a group carries one on its In boundary so guarding a
/// subgraph is the same one line.
#[test]
fn every_node_and_every_group_takes_the_flow_port() {
    let source = r#"
gate = Text { value: "true" }
src = Text { value: "hi" }

g = Group(x: String) -> (y: String) {
  # guarded
  echo = ExecPython(text: String) -> (out: String) {
    code: "return {'out': text}"
    text: self.x
  }
  self.y = echo.out
}
g.x = src.value
g._should_flow = gate.value

out = Debug { data: g.y }
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let has_flow = |id: &str| {
        project
            .nodes
            .iter()
            .find(|n| n.id == id)
            .unwrap_or_else(|| panic!("node {id}"))
            .inputs
            .iter()
            .any(|p| p.name == "_should_flow")
    };
    assert!(has_flow("out"), "a catalog node takes the flow port");
    assert!(
        project.edges.iter().any(|e| e.target == "g" && e.via.is_empty()
            && e.target_handle.as_deref() == Some("_should_flow")),
        "the guard wire lands on the group itself"
    );
}

/// A node whose folder holds a `metadata.json` and no `mod.rs` yet is
/// left out of the catalog: a program that never names it enriches as
/// if the folder were not there, and one that names it is told the
/// node is not ready and where it lives, instead of "unknown node type".
#[test]
fn a_program_naming_a_node_without_its_code_is_told_it_is_not_ready() {
    let root = tempfile::tempdir().expect("temp root");
    let folder = root.path().join("nodes/resizer");
    std::fs::create_dir_all(&folder).expect("mkdir");
    std::fs::write(
        folder.join("metadata.json"),
        r#"{"type": "Resizer", "label": "Resizer", "description": "Shrinks a picture.", "inputs": [{"name": "value", "type": "String"}], "outputs": []}"#,
    )
    .expect("write");
    let stdlib = stdlib_root().expect("stdlib root");
    let cat = FsCatalog::discover_roots(&[stdlib.as_path(), &root.path().join("nodes")])
        .expect("a pending node is no error");

    let mut clean = compile("greeting = Text { value: \"hi\" }\nout = Debug\nout.data = greeting.value\n", uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut clean, &cat).expect("a program that never names the pending node enriches");

    let mut names_it = compile("greeting = Text { value: \"hi\" }\nshrink = Resizer\nshrink.value = greeting.value\n", uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let error = enrich(&mut names_it, &cat).expect_err("the pending node is refused").to_string();
    assert!(error.contains("node type 'Resizer' is not ready yet"), "{error}");
    assert!(error.contains("no mod.rs yet") && error.contains(&folder.display().to_string()), "{error}");
}

/// The editor's parse leaves an `@include` opaque: its file's nodes are not
/// in the graph. So the project-level questions the action bar asks, can
/// this be activated, does its infra have to be up, cannot be answered by
/// looking at the graph's nodes. The include node carries which node types
/// its file holds, and enrich settles what they mean against the catalog.
///
/// Without this, a project whose only trigger lives in an included file
/// showed no Activate button, and one whose only infra node did showed no
/// infra controls and let Run start with nothing provisioned.
#[test]
fn enrich_settles_what_an_include_holds() {
    use crate::rig::compile_with_mode;
    use weft_compiler::weft_compiler::IncludeMode;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("front.weft"),
        "Group(who: Access) -> (out: JsonDict) {\n  \
           door = Route { path: \"hi\", method: \"GET\", auth: self.who }\n  \
           self.out = door.body\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("store.weft"),
        "Group() -> (conn: Access) {\n  db = PostgresDatabase\n  self.conn = db.account\n}\n",
    )
    .unwrap();

    let source = "f = @include(\"front.weft\")\ns = @include(\"store.weft\")\n";
    let mut project = compile_with_mode(
        source,
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
        IncludeMode::Interface,
        None,
    )
    .expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");

    let f = project.nodes.iter().find(|n| n.id == "f").unwrap();
    let fc = f.include_contents.as_ref().expect("contents");
    assert!(fc.has_trigger, "Route inside makes the project activatable");
    assert!(!fc.requires_infra, "and it needs no infra");

    let s = project.nodes.iter().find(|n| n.id == "s").unwrap();
    let sc = s.include_contents.as_ref().expect("contents");
    assert!(sc.requires_infra, "PostgresDatabase inside means infra must come up");
    assert!(!sc.has_trigger, "and it is no trigger");

    // The include node itself is NEITHER, on purpose: those two flags
    // drive real per-node work (a provisioned infra slot with its own live
    // row, a trigger's mount URL), and the alias is not the node that does
    // it. Setting them here would conjure a phantom infra slot named `s`.
    for n in [f, s] {
        assert!(!n.requires_infra, "{} is not itself an infra node", n.id);
        assert!(!n.features.is_trigger, "{} is not itself a trigger", n.id);
    }
}

/// An include is gated like anything else, so the editor's parse has
/// to give its node the gate ports too. Without them the interface
/// parse knows only the included group's declared ports, and
/// `f._should_flow = x` comes back underlined on a program the real
/// build compiles clean.
#[test]
fn an_include_carries_the_gate_ports_in_the_editors_parse() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("front.weft"),
        "Group() -> (out: String) {\n  p = Text { value: \"x\" }\n  self.out = p.value\n}\n",
    )
    .unwrap();
    let source = "gate = Text { value: \"go\" }\n\
                  f = @include(\"front.weft\")\n\
                  f._should_flow = gate.value\n";
    let (parsed, diags) = weft_compiler::parse_only(
        source,
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
        &catalog(),
        None,
    );
    let f = parsed.nodes.iter().find(|n| n.id == "f").expect("the include node");
    for gate in ["_should_flow", "_should_not_flow"] {
        assert!(f.inputs.iter().any(|p| p.name == gate), "{gate} is on the include node");
    }
    assert!(
        !diags.iter().any(|d| d.message.contains("_should_flow")),
        "nothing is underlined on a program the build accepts: {diags:?}"
    );
}

/// A value written for an include alias's port (`keep.n = 7`) reaches
/// the editor's parse in the one home every port constant has, so the
/// card shows it, marks the port filled and refuses a wire over it,
/// exactly as on a node. Before this, the include node was skipped by
/// the pass that gives constants that home, and the card read empty.
#[test]
fn a_value_on_an_include_port_is_a_port_literal_in_the_editors_parse() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("store.weft"),
        "Group(note: String, n: Number) -> (stored: String) {\n  p = Text { value: \"x\" }\n  self.stored = p.value\n}\n",
    )
    .unwrap();
    let source = "keep = @include(\"store.weft\")\nkeep.note = \"from main\"\nkeep.n = 7\n";
    let (parsed, diags) = weft_compiler::parse_only(
        source,
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
        &catalog(),
        None,
    );
    assert!(diags.is_empty(), "{diags:?}");
    let keep = parsed.nodes.iter().find(|n| n.id == "keep").expect("the include node");
    assert_eq!(keep.port_literals.get("note"), Some(&serde_json::json!("from main")));
    assert_eq!(keep.port_literals.get("n"), Some(&serde_json::json!(7)));
    assert!(keep.config.as_object().is_none_or(|c| !c.contains_key("n")), "one home: {:?}", keep.config);
    assert_eq!(
        keep.port_literal_spans.get("n").map(|s| s.origin),
        Some(weft_core::project::ConfigOrigin::Connection),
        "written on a statement line"
    );

    // A name outside the file's signature is refused in this parse the
    // way the build refuses it, so the editor never shows a phantom field
    // for a line the build will reject.
    let (_, diags) = weft_compiler::parse_only(
        "keep = @include(\"store.weft\")\nkeep.bogus = 1\n",
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
        &catalog(),
        None,
    );
    assert!(
        diags.iter().any(|d| d.message.contains("'keep' has no input port 'bogus'")),
        "{diags:?}"
    );
}

/// Navigating INTO an included file parses that file alone. Its nodes must
/// come out under the same ids the build gives them, because the editor's
/// live pollers ask the dispatcher about a node by id: a trigger's mount
/// URL and an infra node's live rows are looked up under the id the
/// compiled program registered, which for an included file is the file's
/// path (`@lib:front.door`), never the call-site alias (`f.door`, and a
/// file included twice has two aliases and one body).
#[test]
fn a_navigated_into_include_parses_under_the_ids_the_build_uses() {
    use weft_catalog::stdlib_root;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("weft.toml"), "[project]\nname = \"p\"\n").unwrap();
    std::fs::create_dir(dir.path().join("lib")).unwrap();
    let front = "Group(who: Access) -> (out: JsonDict) {\n  \
        door = Route { path: \"hi\", method: \"GET\", auth: self.who }\n  \
        self.out = door.body\n}\n";
    std::fs::write(dir.path().join("lib/front.weft"), front).unwrap();
    std::fs::write(dir.path().join("main.weft"), "f = @include(\"lib/front.weft\")\n").unwrap();

    // What the BUILD registers, which is what the dispatcher's signal row
    // and the inspector's poll key on.
    let built = compile(
        "f = @include(\"lib/front.weft\")\n",
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()),
    )
    .expect("compile");
    assert!(
        built.nodes.iter().any(|n| n.id == "@lib:front.door"),
        "the build spells it by the file's path: {:?}",
        built.nodes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
    );

    // What the EDITOR parses when a person clicks into the file. Same id,
    // because the file's path is its identity at parse and at build alike.
    let cat = FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib");
    let lib = dir.path().join("lib");
    let source_id = weft_compiler::source_name::file_id(
        Some(dir.path()),
        Some(&dir.path().join("lib/front.weft")),
    )
    .unwrap();
    let (parsed, _diags) = weft_compiler::parse_only(
        front,
        uuid::Uuid::new_v4(),
        CompileFs::disk(dir.path()).anchored_at(Some(&lib)),
        &cat,
        source_id.as_deref(),
    );
    assert!(
        parsed.nodes.iter().any(|n| n.id == "@lib:front.door"),
        "the editor's parse agrees, so the live pollers ask about the right node: {:?}",
        parsed.nodes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
    );
}

/// A program may make an optional input required or optional even when
/// wired, but never a required one optional: that is refused naming the
/// port and why.
#[test]
fn enrich_refuses_making_a_required_input_optional() {
    let errors_for = |node: &str, header: &str| {
        let source = format!("\nn = {node}({header}) {{}}\n");
        let mut project = compile(&source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
        let errors: Vec<String> = weft_compiler::enrich::enrich_collecting(&mut project, &catalog(), weft_compiler::enrich::EnrichPolicy::Strict)
            .into_iter()
            .map(|e| e.message)
            .collect();
        (project, errors)
    };
    let (_, refused) = errors_for("Debug", "data?: JsonDict");
    assert!(refused.iter().any(|m| m.contains("'data'") && m.contains("its code needs it")), "{refused:?}");

    let (project, errors) = errors_for("StartInfra", "instance: String");
    assert!(errors.is_empty(), "{errors:?}");
    let instance = project.nodes[0].inputs.iter().find(|p| p.name == "instance").unwrap();
    assert!(instance.required && !instance.optional, "written without `?`: always required");

    let (project, errors) = errors_for("StartInfra", "instance?: String");
    assert!(errors.is_empty(), "{errors:?}");
    let instance = project.nodes[0].inputs.iter().find(|p| p.name == "instance").unwrap();
    assert!(!instance.required && instance.optional, "written `?`: runs without it even when wired");

    let (project, _) = errors_for("StartInfra", "");
    let instance = project.nodes[0].inputs.iter().find(|p| p.name == "instance").unwrap();
    assert!(!instance.required && !instance.optional, "unwritten: required once wired");
}

/// An enrich error names the node it is about, so a cut without that node
/// goes on.
#[test]
fn enrich_errors_name_the_node_they_are_about() {
    let source = "\nok = Text { value: \"x\" }\nbad = NoSuchNodeType\n";
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let errors = weft_compiler::enrich::enrich_collecting(&mut project, &catalog(), weft_compiler::enrich::EnrichPolicy::Strict);
    assert_eq!(errors.len(), 1, "{:?}", errors.iter().map(|e| &e.message).collect::<Vec<_>>());
    assert_eq!(errors[0].subjects, vec!["bad".to_string()]);
    assert_eq!(errors[0].diagnostic().subjects, vec!["bad".to_string()]);
}

/// A group's input, and a loop's, gets the widget a node input of its type
/// gets, for the editor to draw a value written there: a checkbox for a
/// Boolean. A loop's is its `LoopIn`'s.
#[test]
fn a_container_input_gets_the_widget_of_its_type() {
    let source = r#"
g = Group(flag: Boolean, count: Number) {
  shown = Debug {}
  shown.data = self.flag
  more = Debug {}
  more.data = self.count
}
g.flag = true
g.count = 2
each = Loop(items: List[String], failed: Boolean) -> (seen: List[String | Null]) {
  over: ["items"]
  echo = Text {}
  echo.value = self.items
  self.seen = echo.value
  shown = Debug {}
  shown.data = self.failed
}
each.items = ["a"]
each.failed = false
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let widget = |group: &str, port: &str| {
        let group = project.groups.iter().find(|g| g.id == group).expect("group");
        let input = group.in_ports.iter().find(|p| p.name == port).expect("port");
        serde_json::to_value(input.widget.as_ref().expect("a widget")).unwrap()["kind"].clone()
    };
    assert_eq!(widget("g", "flag"), serde_json::json!("checkbox"));
    assert_eq!(widget("each", "failed"), serde_json::json!("checkbox"));
    assert_ne!(widget("g", "count"), serde_json::json!("checkbox"));
}

/// A group's input passes through, and takes the drivers its resolved
/// type takes, as a node input does. A loop's passes through unless the
/// loop iterates or carries it.
#[test]
fn a_group_input_passes_through_and_takes_its_types_drivers() {
    let source = r#"
g = Group(maybe: String, sure: String) {
  a = Debug {}
  a.data = self.maybe
  b = Debug {}
  b.data = self.sure
}
g.sure = "x"
each = Loop(items: List[String], prefix: String) -> (results: List[String | Null]) {
  over: ["items"]
  t = Text {}
  t.value = self.items
  self.results = t.value
}
each.items = ["a"]
each.prefix = "p"
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let group = project.groups.iter().find(|g| g.id == "g").unwrap();
    let port = |name: &str| group.in_ports.iter().find(|p| p.name == name).unwrap();
    assert!(port("maybe").passes_through && port("sure").passes_through);
    assert_eq!(port("sure").accepts, weft_core::node::Accepts::for_type(&port("sure").port_type));
    let each = project.groups.iter().find(|g| g.id == "each").unwrap();
    let passes = |name: &str| each.in_ports.iter().find(|p| p.name == name).unwrap().passes_through;
    assert!(!passes("items") && passes("prefix"));
}

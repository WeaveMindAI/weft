//! End-to-end compile + enrich tests against the stdlib catalog.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich;
use weft_compiler::weft_compiler::compile;
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
    assert_eq!(text.outputs.len(), 1);
    assert_eq!(text.outputs[0].name, "value");

    let debug = project.nodes.iter().find(|n| n.id == "out").unwrap();
    assert_eq!(debug.node_type, "Debug");
    // Its own `data`, plus `_should_flow`, which every node carries.
    assert_eq!(
        debug.inputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        vec!["data", "_should_flow"]
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

#[test]
fn enrich_rejects_unknown_node_type() {
    let source = r#"

bad = NotARealNode
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("NotARealNode"), "expected NotARealNode in error, got: {msg}");
}

#[test]
fn enrich_rejects_custom_port_on_node_that_disallows_it() {
    // `Text` does not set canAddOutputPorts, so a user-declared custom
    // output port on it must fail loud, not vanish silently. The port
    // carries a concrete type so it reaches the can-add check (a
    // MustOverride placeholder would trip the needs-a-type error first).
    let source = r#"

t = Text() -> (bogus: String) { value: "hi" }
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("does not support custom") && msg.contains("bogus"),
        "expected a loud custom-port rejection naming the port, got: {msg}"
    );
}

// ─── Layer 2 wire-shape: Loop boundary nodes round-trip ────────────────────

#[test]
fn loop_in_loop_out_round_trip_through_serde() {
    let source = r#"
my = Loop(items: List[String], threshold: Number) -> (results: List[String | Null], acc: String) {
    parallel: false
    over: ["items"]
    carry: ["acc"]
    max_iters: 50
    trim_on_mismatch: true
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
    assert_eq!(in_node.config["max_iters"], serde_json::json!(50));
    assert_eq!(in_node.config["trim_on_mismatch"], serde_json::json!(true));
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
    // Optional knobs (`max_iters`, `trim_on_mismatch`) are absent in the
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
    assert!(in_node.config.get("max_iters").is_none(), "unset max_iters absent from wire");
    assert!(in_node.config.get("trim_on_mismatch").is_none(), "unset trim_on_mismatch absent from wire");
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
    // Group lowers to Passthrough boundary nodes.
    assert!(
        project.nodes.iter().any(|n| n.id == "outer.inner__in" && n.node_type == "Passthrough"),
        "Group nested in Loop must lower to a Passthrough boundary; nodes: {:?}",
        project.nodes.iter().map(|n| (n.id.clone(), n.node_type.clone())).collect::<Vec<_>>()
    );
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
        ("l1.l2.g__in", "Passthrough"),
        ("l1.l2.g__out", "Passthrough"),
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
}

#[test]
fn human_trigger_derives_ports_from_minimal_fields() {
    // The editor emits fields as `{ kind, key }` ONLY: render + config
    // are inherited from the node's port specs at enrich time, never
    // duplicated into the source. This proves the minimal shape derives the
    // full set of ports (approve_reject -> {key}_approved/{key}_rejected
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
    assert!(out_names.contains(&"review_approved"), "approve output missing; got {out_names:?}");
    assert!(out_names.contains(&"review_rejected"), "reject output missing; got {out_names:?}");

    let approved = human.outputs.iter().find(|p| p.name == "review_approved").unwrap();
    assert_eq!(approved.port_type.to_string(), "Boolean");
    let answer = human.outputs.iter().find(|p| p.name == "answer").unwrap();
    assert_eq!(answer.port_type.to_string(), "String");
}

#[test]
fn human_trigger_accepts_a_matching_declared_port_header() {
    // A hand-authored `.weft` MAY re-declare the form-derived ports in the
    // header. That is optional, and accepted IFF each declared port matches a
    // derived one by name AND type (here `test_approved`/`test_rejected` are
    // exactly the Booleans `approve_reject` derives). This must NOT error as a
    // "custom output port" even though HumanTrigger forbids custom ports.
    let source = r#"

human = HumanTrigger() -> (test_approved: Boolean?, test_rejected: Boolean?) {
  fields: [ { "kind": "approve_reject", "key": "test" } ]
}
out = Debug

out.data = human.test_approved
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("a matching header must enrich cleanly");

    let human = project.nodes.iter().find(|n| n.id == "human").unwrap();
    let out_names: Vec<&str> = human.outputs.iter().map(|p| p.name.as_str()).collect();
    // No duplicate ports from the redeclaration.
    assert_eq!(
        out_names.iter().filter(|n| **n == "test_approved").count(),
        1,
        "declared port must merge with the derived one, not duplicate; got {out_names:?}"
    );
    assert!(out_names.contains(&"test_rejected"));
}

#[test]
fn human_trigger_rejects_a_mismatched_declared_port() {
    // A header port whose NAME matches a derived port but whose TYPE does not
    // (String vs the derived Boolean) is a real authoring error, not a silent
    // coercion. And a header port whose name matches NOTHING derived is the
    // genuine custom-port error. Both must surface.
    let wrong_type = r#"

human = HumanTrigger() -> (test_approved: String) {
  fields: [ { "kind": "approve_reject", "key": "test" } ]
}
out = Debug
out.data = human.test_rejected
"#;
    let mut project = compile(wrong_type, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    assert!(format!("{err}").contains("incompatible"), "type mismatch must error: {err}");

    let unknown_port = r#"

human = HumanTrigger() -> (not_a_field: Boolean) {
  fields: [ { "kind": "approve_reject", "key": "test" } ]
}
out = Debug
out.data = human.test_approved
"#;
    let mut project = compile(unknown_port, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).unwrap_err();
    assert!(
        format!("{err}").contains("custom") && format!("{err}").contains("not_a_field"),
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
        .filter(|p| p.name != "_should_flow")
        .map(|p| p.name.as_str())
        .collect();
    assert_eq!(created, vec!["zebra", "apple"], "written order, not alphabetical");
    assert!(
        pick.inputs.iter().all(|p| p.name == "_should_flow" || !p.required),
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

step = ExecPython -> (out: String) {
  code: "return {'out': text}"
  text: words.value
  limit: 3
  flag: true
}
out = Debug
out.data = step.out
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let step = project.nodes.iter().find(|n| n.id == "step").unwrap();
    let port_type = |name: &str| {
        step.inputs.iter().find(|p| p.name == name).map(|p| p.port_type.to_string()).unwrap()
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

step = ExecPython -> (out: String) {
  code: "return {'out': text}"
  text?: words.value
}
out = Debug
out.data = step.out
"#;
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    enrich(&mut project, &catalog()).expect("enrich");
    let step = project.nodes.iter().find(|n| n.id == "step").unwrap();
    let text = step.inputs.iter().find(|p| p.name == "text").unwrap();
    assert!(!text.required, "`text?:` created an optional port");

    let on_a_declared_key = r#"
step = ExecPython -> (out: String) {
  code?: "return {'out': 1}"
}
out = Debug
out.data = step.out
"#;
    let mut project =
        compile(on_a_declared_key, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let err = enrich(&mut project, &catalog()).expect_err("`code` is a declared config input");
    assert!(
        format!("{err}").contains("not a port this node creates"),
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
    assert!(has_flow("g__in"), "a group takes it on its In boundary");
    assert!(
        !has_flow("g__out"),
        "the Out boundary forwards what reached it; a group that did not run has nothing to forward"
    );
    assert!(
        project.edges.iter().any(|e| e.target == "g__in"
            && e.target_handle.as_deref() == Some("_should_flow")),
        "the guard wire lands on the In boundary"
    );
}

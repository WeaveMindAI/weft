use weft_compiler::weft_compiler::*;
use weft_compiler::CompileFs;
use weft_core::weft_type::{WeftPrimitive, WeftType};
use weft_core::node::{MetadataCatalog, NodeMetadata};

/// A catalog that knows no node types. The lenient parse path (`parse_only`,
/// which the editor renders from) keeps unknown nodes + ports + surfaces
/// diagnostics, so an empty catalog is enough to exercise the keep-and-flag
/// behavior for a bad port type.
struct EmptyCatalog;
impl MetadataCatalog for EmptyCatalog {
    fn lookup(&self, _node_type: &str) -> Option<&NodeMetadata> {
        None
    }
    fn all(&self) -> Vec<&NodeMetadata> {
        vec![]
    }
}

/// A port declared with an invalid / unknown type must be KEPT (rendered red as
/// MustOverride in the editor) and surfaced as a diagnostic, NOT dropped so it
/// silently vanishes from the canvas. Regression for "the port vanishes when I
/// mistype its type instead of going red".
#[test]
fn invalid_port_type_keeps_the_port_as_must_override_with_a_diagnostic() {
    let source = "n = SomeNode -> (result: Nonexistent)\n";
    let (project, diagnostics) =
        weft_compiler::parse_only(source, uuid::Uuid::nil(), CompileFs::none(), &EmptyCatalog, None);

    // The node survived and STILL HAS the port (it did not vanish).
    let node = project.nodes.iter().find(|n| n.id == "n").expect("node 'n' kept");
    let port = node
        .outputs
        .iter()
        .find(|p| p.name == "result")
        .expect("the bad-typed port is KEPT, not dropped");
    // It is the red 'needs a type' placeholder, which the editor renders as
    // must-override (the same signal the user described as "should be red").
    assert!(port.port_type.is_must_override(), "bad type becomes MustOverride (red)");

    // The bad type is surfaced as a diagnostic so the user knows to fix it.
    assert!(
        diagnostics.iter().any(|d| d.message.contains("Invalid port type")
            && d.message.contains("Nonexistent")),
        "expected an 'Invalid port type Nonexistent' diagnostic, got: {:?}",
        diagnostics.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn test_basic_project() {
    let source = r#"
# A test project

config = OpenRouterConfig {
    model: "gpt-4"
}

llm = Llm {
    temperature: 0.7
}

llm.config = config.value
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    // File-top comments are ordinary comments: the parsed definition carries
    // no name/description (identity is the manifest filename; descriptions
    // are per-group, the first plain comment line of a group body).
    assert_eq!(result.nodes.len(), 2);
    assert_eq!(result.edges.len(), 1);
    // Connection direction: left = target input, right = source output
    let edge = &result.edges[0];
    assert_eq!(edge.target, "llm");
    assert_eq!(edge.target_handle.as_deref(), Some("config"));
    assert_eq!(edge.source, "config");
    assert_eq!(edge.source_handle.as_deref(), Some("value"));
}

#[test]
fn test_bare_node() {
    let source = r#"
node = Debug
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile bare node");
    assert_eq!(result.nodes.len(), 1);
    assert_eq!(result.nodes[0].id, "node");
    assert_eq!(result.nodes[0].node_type, "Debug");
}

#[test]
fn test_node_with_ports() {
    let source = r#"
worker = ExecPython(
    data: String,
    context: String?
) -> (
    result: String,
    score: Number?
) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile node with ports");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 2);
    assert_eq!(node.inputs[0].name, "data");
    assert!(node.inputs[0].required, "data should be required (default)");
    assert_eq!(node.inputs[1].name, "context");
    assert!(!node.inputs[1].required, "context should be optional (?)");
    assert_eq!(node.outputs.len(), 2);
    assert_eq!(node.outputs[0].name, "result");
    assert!(node.outputs[0].required);
    assert!(!node.outputs[1].required, "score should be optional");
}

#[test]
fn test_node_with_ports_no_config() {
    let source = r#"
pass = ExecPython(data: String) -> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 1);
    assert_eq!(node.outputs.len(), 1);
    assert!(node.config.as_object().map(|o| o.is_empty()).unwrap_or(true));
}

#[test]
fn test_node_empty_inputs() {
    let source = r#"
gen = ExecPython() -> (result: String) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile node with empty inputs");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 0);
    assert_eq!(node.outputs.len(), 1);
}

#[test]
fn test_group_basic() {
    let source = r#"

input = Text { value: "hello" }

preprocessor = Group(raw: String) -> (result: String) {
    # Cleans and transforms text

    clean = Template {
        template: "{{raw}}"
    }

    clean.value = self.raw
    self.result = clean.output
}

preprocessor.raw = input.value

output = Debug {}
output.data = preprocessor.result
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    // input, preprocessor__in, preprocessor__out, preprocessor.clean, output = 5
    assert_eq!(result.nodes.len(), 5);

    let pt_in = result.nodes.iter().find(|n| n.id == "preprocessor__in").expect("input passthrough");
    assert_eq!(pt_in.node_type, "Passthrough");
    assert_eq!(pt_in.inputs.len(), 1);
    assert_eq!(pt_in.inputs[0].name, "raw");

    let pt_out = result.nodes.iter().find(|n| n.id == "preprocessor__out").expect("output passthrough");
    assert_eq!(pt_out.node_type, "Passthrough");
    assert_eq!(pt_out.outputs.len(), 1);
    assert_eq!(pt_out.outputs[0].name, "result");

    // Edge from input to preprocessor should be rewritten to preprocessor__in
    let edge_to_group = result.edges.iter().find(|e| e.source == "input").expect("edge to group");
    assert_eq!(edge_to_group.target, "preprocessor__in");

    // Edge from preprocessor to output should be rewritten to preprocessor__out
    let edge_from_group = result.edges.iter().find(|e| e.target == "output").expect("edge from group");
    assert_eq!(edge_from_group.source, "preprocessor__out");

}

#[test]
fn test_nested_groups() {
    let source = r#"

outer = Group(data: String) -> (result: String) {
    inner = Group(x: String) -> (y: String) {
        proc = Template {
            template: "{{x}}"
        }

        proc.value = self.x
        self.y = proc.output
    }

    inner.x = self.data
    self.result = inner.y
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    // outer__in + outer__out + outer.inner__in + outer.inner__out + outer.inner.proc = 5
    assert_eq!(result.nodes.len(), 5);

    let inner_in = result.nodes.iter().find(|n| n.id == "outer.inner__in").unwrap();
    assert_eq!(inner_in.node_type, "Passthrough");
    let inner_out = result.nodes.iter().find(|n| n.id == "outer.inner__out").unwrap();
    assert_eq!(inner_out.node_type, "Passthrough");

    let proc_node = result.nodes.iter().find(|n| n.id == "outer.inner.proc").unwrap();
    assert_eq!(proc_node.node_type, "Template");
}

#[test]
fn test_self_reserved() {
    let source = r#"
self = Debug {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "'self' should be a reserved word");
}

#[test]
fn test_reserved_type_keyword_as_name() {
    // Naming a group with the reserved `Group` keyword must fail loudly ON the
    // declaration line (line 1 here), not only cryptically where it's later
    // referenced.
    let source = "Group = Group() -> (test: MustOverride?) {\n}\n";
    let errors = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).unwrap_err();
    let reserved = errors.iter().find(|e| e.message.contains("reserved type keyword"));
    assert!(reserved.is_some(), "expected a reserved-keyword error, got {errors:?}");
    assert_eq!(reserved.unwrap().line(), 1, "error must point at the declaration line");
}

#[test]
fn test_connection_direction() {
    // target.input = source.output
    let source = r#"
a = Text { value: "hi" }
b = Debug {}
b.data = a.value
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let edge = &result.edges[0];
    assert_eq!(edge.target, "b");
    assert_eq!(edge.target_handle.as_deref(), Some("data"));
    assert_eq!(edge.source, "a");
    assert_eq!(edge.source_handle.as_deref(), Some("value"));
}

#[test]
fn test_group_self_connections() {
    let source = r#"
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "{{data}}" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    // self.data on right = group input = grp__in
    let edge_in = result.edges.iter().find(|e| e.source == "grp__in").expect("self input edge");
    assert_eq!(edge_in.target, "grp.worker");
    assert_eq!(edge_in.target_handle.as_deref(), Some("value"));
    assert_eq!(edge_in.source_handle.as_deref(), Some("data"));

    // self.result on left = group output = grp__out
    let edge_out = result.edges.iter().find(|e| e.target == "grp__out").expect("self output edge");
    assert_eq!(edge_out.source, "grp.worker");
    assert_eq!(edge_out.source_handle.as_deref(), Some("output"));
    assert_eq!(edge_out.target_handle.as_deref(), Some("result"));
}

#[test]
fn test_triple_backtick_multiline() {
    let source = "

node = ExecPython {
    code: ```
print(\"line1\")
print(\"line2\")
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile triple backtick");
    let node = result.nodes.iter().find(|n| n.id == "node").unwrap();
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.contains("print(\"line1\")"));
    assert!(code.contains("print(\"line2\")"));
}

#[test]
fn test_triple_backtick_inline() {
    let source = "
node = ExecPython {
    code: ```print(\"hello\")```
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile inline backtick");
    let node = result.nodes.iter().find(|n| n.id == "node").unwrap();
    assert_eq!(node.config.get("code").unwrap().as_str().unwrap(), "print(\"hello\")");
}

#[test]
fn test_triple_backtick_inline_with_braces() {
    let source = "
node = ExecPython {
    code: ```return {\"result\": f\"{name} ({email})\"}```
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile inline backtick with braces");
    let node = result.nodes.iter().find(|n| n.id == "node").unwrap();
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.contains("return"), "code should contain return: got {:?}", code);
    assert!(code.contains("result"), "code should contain result: got {:?}", code);
}

#[test]
fn test_port_types() {
    let source = r#"
node = ExecPython(
    img: Image,
    text: String,
    nums: List[Number],
    data: Dict[String, String]
) -> (
    result: String | Number,
    items: List[List[String]]
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile typed ports");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::Primitive(WeftPrimitive::Image));
    assert_eq!(node.inputs[1].port_type, WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(node.inputs[2].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::Number)));
    assert_eq!(node.outputs[0].port_type, WeftType::union(vec![
        WeftType::Primitive(WeftPrimitive::String),
        WeftType::Primitive(WeftPrimitive::Number),
    ]));
    assert_eq!(node.outputs[1].port_type, WeftType::list(WeftType::list(WeftType::Primitive(WeftPrimitive::String))));
}

#[test]
fn test_group_ports_types() {
    // Types are declared as-is in the signature.
    let source = r#"
batch = Group(items: List[String]) -> (results: List[String]) {
    worker = Llm {}
    worker.prompt = self.items
    self.results = worker.response
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile group ports");
    let pt_in = result.nodes.iter().find(|n| n.id == "batch__in").unwrap();
    assert_eq!(pt_in.inputs[0].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));

    let pt_out = result.nodes.iter().find(|n| n.id == "batch__out").unwrap();
    assert_eq!(pt_out.outputs[0].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));
    assert_eq!(pt_out.inputs[0].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));
}

#[test]
fn test_require_one_of() {
    let source = r#"
resolver = ExecPython(
    text: String?,
    audio: Audio?,
    @require_one_of(text, audio)
) -> (result: String) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile @require_one_of");
    let node = &result.nodes[0];
    assert_eq!(node.features.one_of_required.len(), 1);
    assert_eq!(node.features.one_of_required[0], vec!["text", "audio"]);
    assert!(!node.inputs[0].required, "text should be optional");
    assert!(!node.inputs[1].required, "audio should be optional");
}

#[test]
fn test_mock_rejected() {
    let source = r#"
node = HttpRequest {
    url: "https://api.test.com"
    mock: {"body": "hello", "status": 200}
    mocked: true
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "mock/mocked should be rejected as compile errors");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("'mock' is not a valid config key")));
    assert!(errors.iter().any(|e| e.message.contains("'mocked' is not a valid config key")));
}

#[test]
fn test_tags_non_string_element_rejected() {
    // A non-string element in `_tags` was SILENTLY DROPPED (filter_map on as_str),
    // so `_tags: ["ok", 5]` compiled as `["ok"]`, discarding user data with no
    // error. It must now fail loudly.
    let source = r#"
node = ExecPython {
    _tags: ["ok", 5]
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "a non-string _tags element must be a compile error");
    let errors = result.unwrap_err();
    assert!(
        errors.iter().any(|e| e.message.contains("_tags must contain only strings")),
        "expected the non-string-tag diagnostic, got {errors:?}"
    );
}

#[test]
fn test_tags_all_strings_compiles() {
    // The valid case still compiles and keeps every tag.
    let source = r#"
node = ExecPython {
    _tags: ["support", "billing"]
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("valid _tags compiles");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("_tags").unwrap(), &serde_json::json!(["support", "billing"]));
}

#[test]
fn test_group_description_from_comments() {
    // First comment block inside group body is the description (like a docstring)
    // The compiler skips it like any other comment
    let source = r#"
grp = Group(data: String) -> (result: String) {
    # This is the group description
    # It can be multiple lines

    worker = Template { template: "{{data}}" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile with group description");
    assert_eq!(result.nodes.len(), 3); // grp__in, grp__out, grp.worker
}

#[test]
fn test_typevar_ports() {
    let source = r#"
node = ExecPython(
    data: T
) -> (
    result: T
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile TypeVar ports");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::TypeVar("T".to_string()));
    assert_eq!(node.outputs[0].port_type, WeftType::TypeVar("T".to_string()));
}

#[test]
fn test_must_override_port() {
    let source = r#"
node = ExecPython(data) -> (result) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile MustOverride ports");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::MustOverride);
    assert_eq!(node.outputs[0].port_type, WeftType::MustOverride);
}

#[test]
fn test_triple_nested_groups() {
    let source = r#"

input_text = Text { value: "hello" }

level1 = Group(data: String) -> (result: String) {
    level2 = Group(data: String) -> (result: String) {
        level3 = Group(data: String) -> (result: String) {
            l3_code = ExecPython(data: String) -> (result: String) {
                code: "return {\"result\": data + \" -> [L3]\"}"
            }
            l3_code.data = self.data
            self.result = l3_code.result
        }

        l2_code = ExecPython(data: String) -> (result: String) {
            code: "return {\"result\": data + \" -> [L2]\"}"
        }
        l2_code.data = self.data
        level3.data = l2_code.result
        self.result = level3.result
    }

    l1_code = ExecPython(data: String) -> (result: String) {
        code: "return {\"result\": data + \" -> [L1]\"}"
    }
    l1_code.data = self.data
    level2.data = l1_code.result
    self.result = level2.result
}

level1.data = input_text.value

output = Debug {}
output.data = level1.result
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile triple nested");

    // The critical edge: input_text -> level1__in must exist
    let has_input_to_l1 = result.edges.iter().any(|e| {
        e.source == "input_text" && e.target == "level1__in"
            && e.source_handle.as_deref() == Some("value")
            && e.target_handle.as_deref() == Some("data")
    });
    assert!(has_input_to_l1, "input_text.value -> level1__in.data edge must exist");

    // level1__in -> level1.l1_code
    let has_l1in_to_code = result.edges.iter().any(|e| {
        e.source == "level1__in" && e.target == "level1.l1_code"
    });
    assert!(has_l1in_to_code, "level1__in -> level1.l1_code edge must exist");

    // level1.l1_code -> level1.level2__in
    let has_l1code_to_l2 = result.edges.iter().any(|e| {
        e.source == "level1.l1_code" && e.target == "level1.level2__in"
    });
    assert!(has_l1code_to_l2, "level1.l1_code -> level1.level2__in edge must exist");

    // level1.level2__in -> level1.level2.l2_code
    let has_l2in_to_code = result.edges.iter().any(|e| {
        e.source == "level1.level2__in" && e.target == "level1.level2.l2_code"
    });
    assert!(has_l2in_to_code, "level1.level2__in -> level1.level2.l2_code edge must exist");
}

#[test]
fn test_duplicate_inner_node_names_scoped() {
    let source = r#"
group_a = Group(data: String) -> (result: String) {
    worker = Template { template: "A" }
    worker.value = self.data
    self.result = worker.output
}
group_b = Group(data: String) -> (result: String) {
    worker = Template { template: "B" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile scoped names");
    let a_worker = result.nodes.iter().find(|n| n.id == "group_a.worker").unwrap();
    let b_worker = result.nodes.iter().find(|n| n.id == "group_b.worker").unwrap();
    assert_eq!(a_worker.config.get("template").unwrap().as_str().unwrap(), "A");
    assert_eq!(b_worker.config.get("template").unwrap().as_str().unwrap(), "B");
}

#[test]
fn test_nested_node_with_multiline_signature_in_group() {
    let source = "
# Test

outer = Group(
  data: Dict[String, Number]
) -> (
  result: String
) {
  # Outer desc

  inner_node = ExecPython(
    input: Dict[String, Number]
  ) -> (
    output: String
  ) {
    _label: \"Inner\"
    code: ```
return {\"output\": \"hello\"}
    ```
  }
  inner_node.input = self.data
  self.result = inner_node.output
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    if let Err(ref errors) = result {
        for e in errors { eprintln!("COMPILE ERROR: {}", e); }
    }
    let result = result.expect("should compile nested node with multi-line signature");
    for n in &result.nodes { eprintln!("NODE: {} ({})", n.id, n.node_type); }
    for e in &result.edges { eprintln!("EDGE: {}.{} -> {}.{}", e.source, e.source_handle.as_deref().unwrap_or("?"), e.target, e.target_handle.as_deref().unwrap_or("?")); }
    // outer__in, outer__out, outer.inner_node = 3 nodes
    assert_eq!(result.nodes.len(), 3);
    let inner = result.nodes.iter().find(|n| n.id == "outer.inner_node").unwrap();
    assert_eq!(inner.node_type, "ExecPython");
    eprintln!("CONFIG: {:?}", inner.config);
    assert_eq!(inner.label.as_deref(), Some("Inner"));
    let code = inner.config.get("code").and_then(|v| v.as_str());
    eprintln!("CODE: {:?}", code);
    assert!(code.is_some() && code.unwrap().contains("hello"));
}

#[test]
fn test_complex_types_in_ports() {
    let source = r#"
node = ExecPython(
    a: Dict[String, Number],
    b: Dict[String, List[String] | Number],
    c: List[Dict[String, Number]]
) -> (
    d: Dict[String, Dict[String, List[String] | Number] | String]
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile complex types");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 3);
    assert_eq!(node.outputs.len(), 1);
    // Verify Dict[String, Number] parsed correctly
    assert_eq!(node.inputs[0].port_type, WeftType::dict(
        WeftType::primitive(WeftPrimitive::String),
        WeftType::primitive(WeftPrimitive::Number),
    ));
}

#[test]
fn test_media_type_alias() {
    let source = r#"
node = ExecPython(input: Media) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile Media alias");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::media());
}

#[test]
fn test_mock_always_rejected() {
    // Even with invalid JSON, mock key itself is rejected before JSON parsing
    let source = r#"
node = HttpRequest {
    url: "https://api.test.com"
    mock: {broken json
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "mock should be rejected as compile error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("'mock' is not a valid config key")));
}

#[test]
fn test_reject_invalid_type() {
    let source = r#"
node = ExecPython(data: Foo) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Unknown type 'Foo' should produce an error");
}

#[test]
fn test_reject_any_type() {
    let source = r#"
node = ExecPython(data: Any) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "'Any' is not a valid type");
}

#[test]
fn test_group_with_no_body() {
    let source = r#"
grp = Group(data: String) -> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile group with no body");
    // Should still create passthrough nodes
    let pt_in = result.nodes.iter().find(|n| n.id == "grp__in");
    assert!(pt_in.is_some());
}

#[test]
fn test_multiple_connections() {
    let source = r#"
a = Text { value: "hi" }
b = Llm {}
c = Debug {}
b.prompt = a.value
c.data = b.response
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile multiple connections");
    assert_eq!(result.edges.len(), 2);
}

#[test]
fn test_pack_with_require_one_of_in_group() {
    let source = "
# Test

grp = Group(
  notes: String?,
  priority: String?
) -> (
  metadata: Dict[String, String]
) {
  # desc

  pack_node = Pack(
    notes: String?,
    priority: String?,
    @require_one_of(notes, priority)
  ) -> (
    out: Dict[String, String]
  ) {
    _label: \"Metadata\"
  }
  pack_node.notes = self.notes
  pack_node.priority = self.priority
  self.metadata = pack_node.out
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    if let Err(ref errors) = result {
        for e in errors { eprintln!("ERR: {}", e); }
    }
    let result = result.expect("should compile");
    let pack = result.nodes.iter().find(|n| n.id == "grp.pack_node").unwrap();
    assert_eq!(pack.label.as_deref(), Some("Metadata"));
}

#[test]
fn test_multiline_json_array_in_config() {
    let source = r#"
# Test

review = HumanQuery {
  _label: "Test"
  fields: [{
    "fieldType":"display",
    "key":"name"
  }, {
    "fieldType":"text_input",
    "key":"notes"
  }]
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    if let Err(ref errors) = result {
        for e in errors { eprintln!("ERR: {}", e); }
    }
    let result = result.expect("should compile multiline JSON array");
    let node = result.nodes.iter().find(|n| n.id == "review").unwrap();
    for (k, v) in node.config.as_object().unwrap() { eprintln!("  {}: {}", k, v); }
    let fields = node.config.get("fields").expect("fields should exist");
    assert!(fields.is_array(), "fields should be a JSON array");
}


#[test]
fn test_post_config_duplicate_output_port_error() {
    let source = r#"
node = ExecPython() -> (result: String) {
    code: "return {}"
} -> (result: Number)
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Duplicate output port 'result' should produce error");
}




#[test]
fn test_output_only_no_inputs_no_config() {
    // Pattern: id = Type -> (output: String)
    let source = r#"
node = ExecPython -> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile output-only declaration");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 0);
    assert_eq!(node.outputs.len(), 1);
    assert_eq!(node.outputs[0].name, "result");
}

// ─── Config Value Parsing ──────────────────────────────────────────────────

#[test]
fn test_config_boolean_values() {
    let source = r#"
node = ExecPython {
    enabled: true
    disabled: false
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile booleans");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("enabled").unwrap(), &serde_json::json!(true));
    assert_eq!(node.config.get("disabled").unwrap(), &serde_json::json!(false));
}

#[test]
fn test_config_numeric_values() {
    let source = r#"
node = ExecPython {
    count: 42
    rate: 0.75
    negative: -10
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile numbers");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("count").unwrap(), &serde_json::json!(42));
    assert_eq!(node.config.get("rate").unwrap(), &serde_json::json!(0.75));
    assert_eq!(node.config.get("negative").unwrap(), &serde_json::json!(-10));
}

#[test]
fn test_config_escaped_string() {
    let source = r#"
node = ExecPython {
    prompt: "line1\nline2\ttab"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile escaped strings");
    let node = &result.nodes[0];
    let prompt = node.config.get("prompt").unwrap().as_str().unwrap();
    assert!(prompt.contains('\n'));
    assert!(prompt.contains('\t'));
}

#[test]
fn test_config_json_array_inline() {
    let source = r#"
node = ExecPython {
    items: ["a", "b", "c"]
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile inline JSON array");
    let node = &result.nodes[0];
    let items = node.config.get("items").unwrap();
    assert!(items.is_array());
    assert_eq!(items.as_array().unwrap().len(), 3);
}

#[test]
fn test_config_json_object_inline() {
    let source = r#"
node = ExecPython {
    headers: {"Authorization": "Bearer token", "Content-Type": "application/json"}
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile inline JSON object");
    let node = &result.nodes[0];
    let headers = node.config.get("headers").unwrap();
    assert!(headers.is_object());
    assert_eq!(headers.get("Authorization").unwrap().as_str().unwrap(), "Bearer token");
}

#[test]
fn bare_unquoted_value_is_loud() {
    // An UNQUOTED scalar (`mode: streaming`) is not a valid value: a string
    // literal must be quoted, a port reference must be dotted (`node.port`), and
    // a type expression is not assignable. A bare identifier was silently coerced
    // into the string `"streaming"` (and a bare port name like `text = raw` into
    // the string `"raw"`, dropping the intended wire). It must fail loud.
    let source = "node = ExecPython {\n    mode: streaming\n}\n";
    let errs = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).unwrap_err();
    assert!(
        errs.iter().any(|e| e.message.contains("has an invalid value `streaming`")),
        "bare unquoted value must be a loud error: {errs:?}"
    );
    // The quoted form is the valid way to write the same literal.
    let quoted = "node = ExecPython {\n    mode: \"streaming\"\n}\n";
    let p = compile(quoted, uuid::Uuid::new_v4(), CompileFs::none()).expect("quoted value compiles");
    assert_eq!(p.nodes[0].config.get("mode").unwrap().as_str().unwrap(), "streaming");
}

#[test]
fn non_file_marker_value_is_loud() {
    // `@file(...)` is the ONLY marker valid as a config value. Any other `@...`
    // (a typo, `@include`, `@require_one_of`, a bare `@`) used to be silently
    // stored as a literal string; now it fails loud.
    let bare = |v: &str| {
        let src = format!("u = Text {{\n  value: {v}\n}}\n");
        compile_lenient(&src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None).1
    };
    for v in ["@bogus(x)", "@include(\"x.weft\")", "@require_one_of(a, b)", "@x"] {
        let errs = bare(v);
        assert!(errs.iter().any(|e| e.message.contains("invalid marker value")), "`{v}` must be loud: {errs:?}");
    }
}

#[test]
fn malformed_json_value_is_loud() {
    // A `[`/`{`-leading value that isn't valid JSON used to be silently coerced
    // to a string (hiding `[a, b]`-style wiring typos). Now it fails loud, while
    // valid JSON still parses.
    let errs = |v: &str| {
        let src = format!("u = Text {{\n  value: {v}\n}}\n");
        compile_lenient(&src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None).1
    };
    for v in ["{bad}", "[a, b]", "{\"k\": }", "[1, 2,]"] {
        assert!(errs(v).iter().any(|e| e.message.contains("invalid JSON value")), "`{v}` must be loud: {:?}", errs(v));
    }
    // Valid JSON parses with no error.
    for v in ["[1, 2, 3]", "{\"k\": \"v\"}", "[]", "{\"a\": [1, {\"b\": 2}]}"] {
        assert!(errs(v).is_empty(), "`{v}` is valid JSON: {:?}", errs(v));
    }
}

#[test]
fn out_of_range_number_is_loud_not_null() {
    // A numeric value whose magnitude overflows f64 to infinity has NO finite JSON
    // representation. `serde_json::json!(f64::INFINITY)` silently yields `null`, so
    // it must fail loud instead of storing a `null` the user never wrote.
    let nines = "9".repeat(320);
    let src = format!("u = Text {{\n  value: {nines}\n}}\n");
    let (project, errs) = compile_lenient(&src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.iter().any(|e| e.message.contains("out-of-range numeric value")), "huge number must be loud: {errs:?}");
    // And it must NOT have been stored as a null in the node's config.
    if let Some(n) = project.nodes.iter().find(|n| n.id == "u") {
        assert!(!n.config.get("value").is_some_and(|v| v.is_null()), "out-of-range number must never become a silent null: {:?}", n.config.get("value"));
    }
    // A normal number still parses fine (regression guard for the happy path).
    let (_, ok) = compile_lenient("u = Text {\n  value: 42\n}\n", uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(ok.is_empty(), "normal integer parses: {ok:?}");
}

#[test]
fn label_must_be_a_quoted_string_or_heredoc() {
    // A `_label` is a STRING; bare/non-string labels fail loud, quoted and heredoc
    // forms are valid. (The label path used to bypass the value gate entirely.)
    let errs = |label: &str| {
        let src = format!("u = Text {{\n  _label: {label}\n  value: \"x\"\n}}\n");
        compile_lenient(&src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None).1
    };
    assert!(errs("raw").iter().any(|e| e.message.contains("'_label' has an invalid value")), "bare label loud: {:?}", errs("raw"));
    assert!(errs("42").iter().any(|e| e.message.contains("'_label' has an invalid value")), "numeric label loud: {:?}", errs("42"));
    assert!(errs("\"Name\"").is_empty(), "quoted label valid: {:?}", errs("\"Name\""));
    // Heredoc label is valid (the editor emits one for a multiline label).
    let (p, herr) = compile_lenient("u = Text {\n  _label: ```\nMulti\nLine\n```\n  value: \"x\"\n}\n", uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(herr.is_empty(), "heredoc label valid: {herr:?}");
    assert_eq!(p.nodes.iter().find(|n| n.id == "u").and_then(|n| n.label.as_deref()), Some("Multi\nLine"));
}

#[test]
fn config_field_in_group_body_is_a_loud_error() {
    // A config field typed into a plain Group body used to fall into
    // the lowering catch-all and vanish silently; only Loops take a
    // config block.
    let src = r#"
g = Group(items: List[String]) -> (out: String) {
    parallel: true
    t = Text { value: "x" }
    self.out = t.value
}
"#;
    let (_, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(
        errs.iter().any(|e| e.message.contains("groups do not take config fields")),
        "stray group config field must error loudly: {errs:?}"
    );
}

#[test]
fn test_config_empty_quoted_string() {
    let source = r#"
node = ExecPython {
    prefix: ""
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile empty string");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("prefix").unwrap().as_str().unwrap(), "");
}

// ─── Label Parsing ─────────────────────────────────────────────────────────

#[test]
fn test_label_quoted() {
    let source = r#"
node = ExecPython {
    _label: "My Worker Node"
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("My Worker Node"));
}

#[test]
fn unquoted_label_is_loud() {
    // A label is a STRING: it must be quoted. A bare `_label: Worker` used to be
    // silently coerced to the string "Worker"; now it fails loud like any other
    // unquoted value.
    let bare = "node = ExecPython {\n    _label: Worker\n    code: \"return {}\"\n}\n";
    let errs = compile(bare, uuid::Uuid::new_v4(), CompileFs::none()).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("'_label' has an invalid value")), "bare label must be loud: {errs:?}");

    // The quoted form is valid.
    let quoted = "node = ExecPython {\n    _label: \"Worker\"\n    code: \"return {}\"\n}\n";
    let p = compile(quoted, uuid::Uuid::new_v4(), CompileFs::none()).expect("quoted label compiles");
    assert_eq!(p.nodes[0].label.as_deref(), Some("Worker"));
}

#[test]
fn label_has_one_home_and_one_syntax() {
    // `_label` is the node's LABEL (stored in `node.label`), set ONLY via the body
    // `_label: "..."` field. Two regressions: (1) two body `_label`s silently kept
    // the last (the label path bypassed the dup gate); (2) a connection-origin
    // `n._label = "x"` silently landed in `config["_label"]` (where nothing reads
    // it) while `node.label` stayed empty. Both must fail loud.
    let dup = "n = Text {\n  _label: \"a\"\n  _label: \"b\"\n}\n";
    let (_p, errs) = compile_lenient(dup, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.iter().any(|e| e.message.contains("duplicate '_label'")), "dup label loud: {errs:?}");

    let conn = "n = Text {}\nn._label = \"viaconn\"\n";
    let (p2, errs2) = compile_lenient(conn, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs2.iter().any(|e| e.message.contains("a node's label is set with a body field")), "conn-origin label loud: {errs2:?}");
    // And it did NOT silently land in config.
    let n = p2.nodes.iter().find(|n| n.id == "n").expect("node n");
    assert!(n.config.get("_label").is_none(), "no silent config['_label']: {:?}", n.config);
}

#[test]
fn flatten_never_ships_duplicate_node_ids() {
    // A node-id collision is a loud parse error, but the LENIENT render path
    // always returns a project. Without a node dedup it would hand the renderer
    // two `NodeDefinition`s with one id (here an include alias `c` colliding with
    // a sibling group `c` -> two `root.c__in`/`root.c__out`). flatten dedups nodes
    // (keeping the first) like it dedups edges, so the rendered set has unique ids.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("coll.weft"),
        "Group(x: String) -> (y: String) {\n  m = Text { value: \"v\" }\n  self.y = m.value\n}\n").unwrap();
    let src = "root = Group() {\n  c = @include(\"coll.weft\")\n  c = Group() -> (z: String) {}\n}\n";
    let (p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), IncludeMode::Full, None);
    // The collision IS reported loudly.
    assert!(errs.iter().any(|e| e.message.contains("Duplicate id")), "collision reported: {errs:?}");
    // But the rendered node set has NO duplicate id.
    let mut ids: Vec<&str> = p.nodes.iter().map(|n| n.id.as_str()).collect();
    ids.sort();
    assert!(ids.windows(2).all(|w| w[0] != w[1]), "no duplicate node id in the rendered set: {ids:?}");
}

#[test]
fn test_label_with_escapes() {
    let source = r#"
node = ExecPython {
    _label: "Has \"quotes\" inside"
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile escaped label");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("Has \"quotes\" inside"));
}

#[test]
fn test_label_in_oneliner() {
    let source = r#"
node = ExecPython { _label: "Quick", code: "return {}" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("Quick"));
    assert!(node.config.get("code").is_some());
}

// ─── Error Cases ───────────────────────────────────────────────────────────

#[test]
fn test_error_unclosed_config_block() {
    let source = r#"
node = ExecPython {
    code: "return {}"
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Unclosed config block should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Unclosed config block")));
}

#[test]
fn test_error_unclosed_group() {
    let source = r#"
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "hi" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Unclosed group should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Unclosed group")));
}

#[test]
fn test_error_duplicate_root_node_id() {
    let source = r#"
node = Text { value: "a" }
node = Text { value: "b" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Duplicate node ID should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate id 'node'")));
}

#[test]
fn test_error_duplicate_group_name() {
    let source = r#"
grp = Group() -> ()
grp = Group() -> ()
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Duplicate group name should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate id 'grp'")));
}

#[test]
fn test_error_duplicate_node_in_group() {
    let source = r#"
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "a" }
    worker = Template { template: "b" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Duplicate node in group should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate id") && e.message.contains("worker")));
}

#[test]
fn test_error_require_one_of_in_outputs() {
    let source = r#"
node = ExecPython() -> (
    a: String?,
    b: String?,
    @require_one_of(a, b)
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "@require_one_of in outputs should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("@require_one_of is only valid in input")));
}

#[test]
fn test_error_require_one_of_missing_paren() {
    let source = r#"
node = ExecPython(
    a: String?,
    b: String?,
    @require_one_of(a, b
) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "@require_one_of missing ) should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("missing closing parenthesis")));
}

#[test]
fn test_error_invalid_port_type() {
    let source = r#"
node = ExecPython(data: Foo) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Invalid type 'Foo' should error");
}

#[test]
fn test_error_duplicate_port_name() {
    let source = r#"
node = ExecPython(data: String, data: Number) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Duplicate port name should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate")));
}

#[test]
fn test_error_port_name_starts_with_number() {
    let source = r#"
node = ExecPython(1data: String) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Port name starting with number should error");
}

#[test]
fn test_error_unexpected_root_content() {
    let source = r#"
node = Text { value: "hi" }
this is not valid syntax
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Random text should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Unexpected")));
}

#[test]
fn test_error_broken_multiline_json() {
    let source = r#"
node = ExecPython {
    data: [{
        "key": "value"

node2 = Text { value: "hi" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none());
    assert!(result.is_err(), "Broken JSON should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Broken JSON") || e.message.contains("Unclosed")));
}

// ─── Empty and Minimal Projects ────────────────────────────────────────────

#[test]
fn test_empty_source() {
    let source = "";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("empty project should compile");
    assert_eq!(result.nodes.len(), 0);
}

#[test]
fn test_comments_only() {
    let source = r#"
# Nothing here

# Just comments
# More comments
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    // `# Project:` is now an ordinary comment, not a name source.
    assert_eq!(result.nodes.len(), 0);
    assert_eq!(result.edges.len(), 0);
}

// ─── Multiline Port Signatures ─────────────────────────────────────────────

#[test]
fn test_multiline_inputs_and_outputs_on_separate_lines() {
    let source = r#"
node = ExecPython(
    input1: String,
    input2: Number
) -> (
    output1: String
) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile multiline sig");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 2);
    assert_eq!(node.outputs.len(), 1);
}

#[test]
fn test_arrow_on_next_line() {
    let source = "
node = ExecPython(data: String)
-> (result: String) {
    code: \"return {}\"
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile arrow on next line");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 1);
    assert_eq!(node.outputs.len(), 1);
}

#[test]
fn test_deeply_split_signature() {
    let source = r#"
node = ExecPython(
    a: String,
    b: Number,
    c: List[String]
) -> (
    x: String,
    y: Number
) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile deeply split sig");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 3);
    assert_eq!(node.outputs.len(), 2);
    assert_eq!(node.inputs[2].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));
}

// ─── Triple Backtick Edge Cases ────────────────────────────────────────────

#[test]
fn test_triple_backtick_dedent() {
    // Indented content should be dedented
    let source = "
node = ExecPython {
    code: ```
        line1
        line2
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = &result.nodes[0];
    let code = node.config.get("code").unwrap().as_str().unwrap();
    // After dedenting, 4 spaces of common indent removed
    assert!(code.contains("line1"), "code should contain line1: got {:?}", code);
    assert!(code.contains("line2"), "code should contain line2: got {:?}", code);
    // Should NOT have leading spaces from common indent
    assert!(!code.starts_with("        "), "common indent should be stripped");
}

#[test]
fn test_triple_backtick_empty_value() {
    let source = "
node = ExecPython {
    code: ```
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile empty triple backtick");
    let node = &result.nodes[0];
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.trim().is_empty(), "empty backtick should produce empty string");
}

#[test]
fn test_triple_backtick_with_escaped_backticks() {
    let source = "
node = ExecPython {
    code: ```
print(\"\\`\\`\\`\")
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile escaped backticks");
    let node = &result.nodes[0];
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.contains("```"), "escaped backticks should become real backticks");
}

// ─── One-liner Config ──────────────────────────────────────────────────────

#[test]
fn test_oneliner_config() {
    let source = r#"
node = ExecPython { code: "return {}", mode: "fast" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile one-liner config");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("code").unwrap().as_str().unwrap(), "return {}");
    assert_eq!(node.config.get("mode").unwrap().as_str().unwrap(), "fast");
}

#[test]
fn test_empty_config_block() {
    let source = r#"
node = ExecPython(data: String) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile empty config");
    let node = &result.nodes[0];
    assert!(node.config.as_object().unwrap().is_empty());
}

// ─── Comments ──────────────────────────────────────────────────────────────

#[test]
fn test_comments_between_declarations() {
    let source = r#"
a = Text { value: "one" }

# This is a comment between nodes
# Another comment

b = Text { value: "two" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    assert_eq!(result.nodes.len(), 2);
}

#[test]
fn test_comment_after_opening_brace() {
    let source = "
node = ExecPython(data: String) -> (result: String) { # This is a config block
    code: \"return {}\"
}
";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile with comment after {");
    let node = &result.nodes[0];
    assert!(node.config.get("code").is_some());
}

// ─── Port Features ─────────────────────────────────────────────────────────

#[test]
fn test_multiple_require_one_of_groups() {
    let source = r#"
node = ExecPython(
    text: String?,
    audio: Audio?,
    url: String?,
    file: String?,
    @require_one_of(text, audio)
    @require_one_of(url, file)
) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile multiple @require_one_of");
    let node = &result.nodes[0];
    assert_eq!(node.features.one_of_required.len(), 2);
    assert_eq!(node.features.one_of_required[0], vec!["text", "audio"]);
    assert_eq!(node.features.one_of_required[1], vec!["url", "file"]);
}

#[test]
fn test_port_underscore_name() {
    let source = r#"
node = ExecPython(_internal: String) -> (_result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile underscore port names");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].name, "_internal");
    assert_eq!(node.outputs[0].name, "_result");
}

#[test]
fn test_port_must_override_optional() {
    let source = r#"
node = ExecPython(data?, required_data) -> (result) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = &result.nodes[0];
    assert!(!node.inputs[0].required, "data? should be optional");
    assert!(node.inputs[1].required, "required_data should be required (default)");
    assert_eq!(node.inputs[0].port_type, WeftType::MustOverride);
}

// ─── Null in Types ─────────────────────────────────────────────────────────

#[test]
fn test_null_in_union_type() {
    let source = r#"
node = ExecPython(data: String | Null) -> (result: String | Null) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile Null in union");
    let node = &result.nodes[0];
    // Both should be union types containing Null
    match &node.inputs[0].port_type {
        WeftType::Union(types) => {
            assert!(types.iter().any(|t| matches!(t, WeftType::Primitive(WeftPrimitive::Null))));
        }
        _ => panic!("Expected union type, got {:?}", node.inputs[0].port_type),
    }
}

// ─── Connection Edge Cases ─────────────────────────────────────────────────

#[test]
fn test_connections_with_whitespace() {
    let source = r#"
a = Text { value: "hi" }
b = Debug {}
b.data   =   a.value
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile connections with whitespace");
    assert_eq!(result.edges.len(), 1);
    assert_eq!(result.edges[0].target, "b");
    assert_eq!(result.edges[0].source, "a");
}

#[test]
fn test_multiple_connections_to_same_node() {
    let source = r#"
src1 = Text { value: "a" }
src2 = Text { value: "b" }
target = ExecPython(x: String, y: String) -> (result: String) {
    code: "return {}"
}
target.x = src1.value
target.y = src2.value
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    assert_eq!(result.edges.len(), 2);
}

// ─── Group Edge Cases ──────────────────────────────────────────────────────

#[test]
fn test_group_empty_body() {
    let source = r#"
grp = Group(data: String) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile empty group body");
    let pt_in = result.nodes.iter().find(|n| n.id == "grp__in").unwrap();
    let pt_out = result.nodes.iter().find(|n| n.id == "grp__out").unwrap();
    assert_eq!(pt_in.node_type, "Passthrough");
    assert_eq!(pt_out.node_type, "Passthrough");
}

#[test]
fn test_group_with_only_connections() {
    let source = r#"
grp = Group(data: String) -> (result: String) {
    self.result = self.data
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile group with passthrough wiring");
    // Only passthrough nodes, no child nodes
    assert_eq!(result.nodes.len(), 2); // grp__in, grp__out
    // Connection from __in to __out
    assert!(result.edges.iter().any(|e| e.source == "grp__in" && e.target == "grp__out"));
}

#[test]
fn test_same_node_id_in_different_groups_allowed() {
    let source = r#"
a = Group(data: String) -> (result: String) {
    proc = Template { template: "A" }
    proc.value = self.data
    self.result = proc.output
}
b = Group(data: String) -> (result: String) {
    proc = Template { template: "B" }
    proc.value = self.data
    self.result = proc.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile: same node ID in different groups is allowed");
    assert!(result.nodes.iter().any(|n| n.id == "a.proc"));
    assert!(result.nodes.iter().any(|n| n.id == "b.proc"));
}

// ─── Config in Group ───────────────────────────────────────────────────────

#[test]
fn test_require_one_of_in_config_block() {
    let source = r#"
node = ExecPython(
    a: String?,
    b: String?
) -> (result: String) {
    @require_one_of(a, b)
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile @require_one_of in config");
    let node = &result.nodes[0];
    assert_eq!(node.features.one_of_required.len(), 1);
    assert_eq!(node.features.one_of_required[0], vec!["a", "b"]);
}

// ─── Mixed Complex Scenarios ───────────────────────────────────────────────

#[test]
fn test_full_workflow_small() {
    let source = r#"
# Small end-to-end test

input = Text { value: "Hello world" }

processor = Group(raw: String) -> (clean: String) {
    # Cleans text

    trimmer = ExecPython(text: String) -> (result: String) {
        code: "return {'result': text.strip()}"
    }
    trimmer.text = self.raw
    self.clean = trimmer.result
}

processor.raw = input.value

llm = Llm {
    _label: "Summarizer"
    temperature: 0.5
    model: "gpt-4"
}
llm.prompt = processor.clean

output = Debug {}
output.data = llm.response
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile full workflow");
    // input + processor__in + processor__out + processor.trimmer + llm + output = 6
    assert_eq!(result.nodes.len(), 6);
    // input->processor__in, processor__in->trimmer, trimmer->processor__out,
    // processor__out->llm, llm->output = 5 edges
    assert_eq!(result.edges.len(), 5);

    let llm = result.nodes.iter().find(|n| n.id == "llm").unwrap();
    assert_eq!(llm.label.as_deref(), Some("Summarizer"));
    assert_eq!(llm.config.get("temperature").unwrap(), &serde_json::json!(0.5));
}


// ─── Scope & GroupBoundary ─────────────────────────────────────────────

#[test]
fn test_scope_top_level_nodes() {
    let source = r#"
a = Text { value: "hello" }
b = Template { template: "{{data}}" }
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    for node in &result.nodes {
        assert!(node.scope.is_empty(), "top-level node '{}' should have empty scope", node.id);
        assert!(node.group_boundary.is_none(), "top-level node '{}' should not be a boundary", node.id);
    }
}

#[test]
fn test_scope_simple_group() {
    let source = r#"
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "{{data}}" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");

    // __in passthrough: boundary In, scope = [] (parent is top-level)
    let pt_in = result.nodes.iter().find(|n| n.id == "grp__in").unwrap();
    assert!(pt_in.scope.is_empty(), "__in scope should be empty (top-level group)");
    let gb_in = pt_in.group_boundary.as_ref().expect("__in should have groupBoundary");
    assert_eq!(gb_in.group_id, "grp");
    assert_eq!(gb_in.role, weft_core::GroupBoundaryRole::In);

    // __out passthrough: boundary Out, scope = []
    let pt_out = result.nodes.iter().find(|n| n.id == "grp__out").unwrap();
    assert!(pt_out.scope.is_empty());
    let gb_out = pt_out.group_boundary.as_ref().expect("__out should have groupBoundary");
    assert_eq!(gb_out.group_id, "grp");
    assert_eq!(gb_out.role, weft_core::GroupBoundaryRole::Out);

    // Internal node: scope = ["grp"], no boundary
    let worker = result.nodes.iter().find(|n| n.id == "grp.worker").unwrap();
    assert_eq!(worker.scope, vec!["grp"]);
    assert!(worker.group_boundary.is_none());
}

#[test]
fn test_scope_nested_groups() {
    let source = r#"
outer = Group(data: String) -> (result: String) {
    inner = Group(data: String) -> (result: String) {
        worker = Template { template: "{{data}}" }
        worker.value = self.data
        self.result = worker.output
    }
    inner.data = self.data
    self.result = inner.result
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile nested groups");

    // outer__in: scope = [], boundary In for "outer"
    let outer_in = result.nodes.iter().find(|n| n.id == "outer__in").unwrap();
    assert!(outer_in.scope.is_empty());
    assert_eq!(outer_in.group_boundary.as_ref().unwrap().group_id, "outer");

    // outer__out: scope = [], boundary Out for "outer"
    let outer_out = result.nodes.iter().find(|n| n.id == "outer__out").unwrap();
    assert!(outer_out.scope.is_empty());

    // inner__in: scope = ["outer"], boundary In for "outer.inner"
    let inner_in = result.nodes.iter().find(|n| n.id == "outer.inner__in").unwrap();
    assert_eq!(inner_in.scope, vec!["outer"]);
    let gb = inner_in.group_boundary.as_ref().unwrap();
    assert_eq!(gb.group_id, "outer.inner");
    assert_eq!(gb.role, weft_core::GroupBoundaryRole::In);

    // inner__out: scope = ["outer"], boundary Out for "outer.inner"
    let inner_out = result.nodes.iter().find(|n| n.id == "outer.inner__out").unwrap();
    assert_eq!(inner_out.scope, vec!["outer"]);
    assert_eq!(inner_out.group_boundary.as_ref().unwrap().group_id, "outer.inner");

    // worker inside inner: scope = ["outer", "outer.inner"]
    let worker = result.nodes.iter().find(|n| n.id == "outer.inner.worker").unwrap();
    assert_eq!(worker.scope, vec!["outer", "outer.inner"]);
    assert!(worker.group_boundary.is_none());
}

#[test]
fn test_scope_triple_nested() {
    let source = r#"
a = Group(x: String) -> (y: String) {
    b = Group(x: String) -> (y: String) {
        c = Group(x: String) -> (y: String) {
            node = Template { template: "{{x}}" }
            node.value = self.x
            self.y = node.output
        }
        c.x = self.x
        self.y = c.y
    }
    b.x = self.x
    self.y = b.y
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile triple nested");

    let node = result.nodes.iter().find(|n| n.id == "a.b.c.node").unwrap();
    assert_eq!(node.scope, vec!["a", "a.b", "a.b.c"]);

    // c's boundaries: scope = ["a", "a.b"]
    let c_in = result.nodes.iter().find(|n| n.id == "a.b.c__in").unwrap();
    assert_eq!(c_in.scope, vec!["a", "a.b"]);
    assert_eq!(c_in.group_boundary.as_ref().unwrap().group_id, "a.b.c");

    // b's boundaries: scope = ["a"]
    let b_in = result.nodes.iter().find(|n| n.id == "a.b__in").unwrap();
    assert_eq!(b_in.scope, vec!["a"]);

    // a's boundaries: scope = []
    let a_in = result.nodes.iter().find(|n| n.id == "a__in").unwrap();
    assert!(a_in.scope.is_empty());
}

#[test]
fn test_scope_mocking_inner_group_skips_only_inner() {
    // Verify that scope allows distinguishing which nodes belong to which group.
    // If "outer.inner" is mocked, only nodes with "outer.inner" in their scope should be skipped.
    // Nodes with just "outer" in scope (but not "outer.inner") should NOT be skipped.
    let source = r#"
outer = Group(data: String) -> (result: String) {
    pre = Template { template: "pre" }
    inner = Group(data: String) -> (result: String) {
        deep = Template { template: "deep" }
        deep.value = self.data
        self.result = deep.output
    }
    inner.data = self.data
    self.result = inner.result
}
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");

    let pre = result.nodes.iter().find(|n| n.id == "outer.pre").unwrap();
    let deep = result.nodes.iter().find(|n| n.id == "outer.inner.deep").unwrap();

    // "pre" is inside "outer" but NOT inside "outer.inner"
    assert!(pre.scope.contains(&"outer".to_string()));
    assert!(!pre.scope.contains(&"outer.inner".to_string()));

    // "deep" is inside both
    assert!(deep.scope.contains(&"outer".to_string()));
    assert!(deep.scope.contains(&"outer.inner".to_string()));

    // If we mock "outer.inner", pre should NOT be skipped, deep SHOULD be skipped
    let mocked_group = "outer.inner";
    assert!(!pre.scope.iter().any(|s| s == mocked_group), "pre should not be inside mocked group");
    assert!(deep.scope.iter().any(|s| s == mocked_group), "deep should be inside mocked group");
}




// ── @file value injection, end to end through compile() ─────────────────

#[test]
fn file_ref_resolves_through_compile() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("system.txt"), "you are a helpful poet").unwrap();

    let source = r#"
poet = OpenRouterConfig {
    systemPrompt: @file("system.txt")
}
"#;
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("should compile");
    let poet = project.nodes.iter().find(|n| n.id == "poet").expect("poet node");
    assert_eq!(
        poet.config.get("systemPrompt").and_then(|v| v.as_str()),
        Some("you are a helpful poet")
    );
}

#[test]
fn file_ref_on_connection_line_inside_group_resolves() {
    // `@file` on a connection-line RHS INSIDE a group body must resolve, same
    // as at top level (both go through the one shared config-fill classifier).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sys.txt"), "you are helpful").unwrap();

    let source = r#"
g = Group() -> () {
    poet = OpenRouterConfig
    poet.systemPrompt = @file("sys.txt")
}
"#;
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("should compile");
    let poet = project.nodes.iter().find(|n| n.id == "g.poet").expect("g.poet node");
    assert_eq!(
        poet.config.get("systemPrompt").and_then(|v| v.as_str()),
        Some("you are helpful")
    );
}

#[test]
fn file_ref_typed_json_resolves_through_compile() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("cfg.json"), r#"{"a": 1, "b": 2}"#).unwrap();

    let source = r#"
node = SomeNode {
    cfg: @file("cfg.json", JsonDict)
}
"#;
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("should compile");
    let node = project.nodes.iter().find(|n| n.id == "node").expect("node");
    assert_eq!(node.config.get("cfg"), Some(&serde_json::json!({"a": 1, "b": 2})));
}

#[test]
fn file_ref_surfaces_to_node_for_editor() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("system.txt"), "be helpful").unwrap();

    let source = r#"
poet = OpenRouterConfig {
    systemPrompt: @file("system.txt")
}
"#;
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let poet = project.nodes.iter().find(|n| n.id == "poet").expect("poet");

    // config holds the resolved value (build/run use this).
    assert_eq!(poet.config.get("systemPrompt").and_then(|v| v.as_str()), Some("be helpful"));

    // file_refs records the reference (the editor uses this to render
    // file-backed and route edits to the file).
    let fr = poet.file_refs.get("systemPrompt").expect("file_ref recorded");
    assert_eq!(fr.path, "system.txt");

    // Wire shape: serializes as `fileRefs: { field: { path, type } }`.
    let json = serde_json::to_value(poet).unwrap();
    assert_eq!(json["fileRefs"]["systemPrompt"]["path"], "system.txt");
    assert_eq!(json["fileRefs"]["systemPrompt"]["type"], "String");
}

#[test]
fn file_ref_inside_group_body_resolves() {
    // Regression: @file in a group child node must resolve too (the pass
    // must walk group descendants, not just top-level nodes).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("p.txt"), "grouped prompt").unwrap();
    let source = r#"
g = Group -> (out: String) {
    inner = Text { value: @file("p.txt") }
    self.out = inner.value
}
"#;
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let inner = project.nodes.iter().find(|n| n.id == "g.inner").expect("g.inner");
    assert_eq!(inner.config.get("value").and_then(|v| v.as_str()), Some("grouped prompt"));
}

#[test]
fn file_ref_missing_file_is_compile_error() {
    let dir = tempfile::tempdir().unwrap();
    let source = r#"
node = SomeNode {
    x: @file("nope.txt")
}
"#;
    let errors = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("nope.txt")), "errors: {errors:?}");
}

// ── @include group injection ───────────────────────────────────────────

const CLEANER_WEFT: &str = r#"
Group(raw: String) -> (cleaned: String) {
    strip = Text { value: "x" }
    self.cleaned = strip.value
}
"#;

#[test]
fn include_full_inlines_group() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("cleaner.weft"), CLEANER_WEFT).unwrap();

    let source = r#"
c = @include("cleaner.weft")
"#;
    // Full mode (build): the group inlines, flattening to c__in / c__out
    // boundary Passthroughs plus the internal node, all scoped under `c`.
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    assert!(project.nodes.iter().any(|n| n.id == "c__in"), "missing c__in: {:?}", project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>());
    assert!(project.nodes.iter().any(|n| n.id == "c__out"));
    assert!(project.nodes.iter().any(|n| n.id == "c.strip"));
}

/// An included file with INTERNAL nesting (a nested group + an inline-expr) must
/// scope every id under the call-site alias in ONE pass (no `rescope_group`
/// string surgery). Pins the include-reshape: the included file is parsed with
/// the alias as its anon-root id, so internals are `{alias}.*` directly.
#[test]
fn include_with_nested_group_scopes_under_alias() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("comp.weft"),
        "Group(raw: String) -> (cleaned: String) {\n  sub = Group(i: String) -> (o: String) {\n    inner = Text { value: \"y\" }\n    self.o = inner.value\n  }\n  pick = Text { value: Upper { text: \"z\" }.out }\n  self.cleaned = sub.o\n}\n",
    ).unwrap();
    let project = compile("c = @include(\"comp.weft\")\n", uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let ids: std::collections::HashSet<&str> = project.nodes.iter().map(|n| n.id.as_str()).collect();
    // Boundary passthroughs of the included root and the nested sub-group, all
    // scoped under the alias `c` in one pass.
    assert!(ids.contains("c__in") && ids.contains("c__out"), "root boundaries: {ids:?}");
    assert!(ids.contains("c.sub__in") && ids.contains("c.sub__out"), "nested-group boundaries: {ids:?}");
    assert!(ids.contains("c.sub.inner"), "deep child: {ids:?}");
    // The inline-expr anon node is scoped under the alias too (`c.pick__value`).
    assert!(ids.contains("c.pick__value"), "inline anon under alias: {ids:?}");
    // No id leaked the included file's own derived id (`Comp`) or stayed unscoped.
    assert!(!ids.iter().any(|i| i.starts_with("Comp")), "no leaked source id: {ids:?}");
}

#[test]
fn include_interface_emits_opaque_node() {
    use weft_compiler::weft_compiler::{compile_with_mode, IncludeMode, INCLUDE_NODE_TYPE};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("cleaner.weft"), CLEANER_WEFT).unwrap();

    let source = r#"
c = @include("cleaner.weft")
"#;
    // Interface mode (editor): one opaque node carrying the group's ports.
    let project = compile_with_mode(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), IncludeMode::Interface, None)
        .expect("compile");
    let c = project.nodes.iter().find(|n| n.id == "c").expect("opaque include node");
    assert_eq!(c.node_type, INCLUDE_NODE_TYPE);
    assert_eq!(c.include_path.as_deref(), Some("cleaner.weft"));
    assert!(c.inputs.iter().any(|p| p.name == "raw"));
    assert!(c.outputs.iter().any(|p| p.name == "cleaned"));
    // No body leaked into the parent graph.
    assert!(!project.nodes.iter().any(|n| n.id == "c.strip"));
}

#[test]
fn include_inside_group_interface_scopes_opaque_node() {
    // Regression: an @include inside a group body, in interface mode, becomes
    // an opaque node that must be scoped to the group. Otherwise a sibling
    // edge into its port trips the scope-reachability check (compile errors).
    use weft_compiler::weft_compiler::{compile_with_mode, IncludeMode};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("inner.weft"), CLEANER_WEFT).unwrap();
    let source = r#"
g = Group -> (out: String) {
    src = Text { value: "x" }
    inc = @include("inner.weft")
    inc.raw = src.value
    self.out = inc.cleaned
}
"#;
    let project = compile_with_mode(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), IncludeMode::Interface, None)
        .expect("compile (sibling edge into opaque include must stay in-scope)");
    let inc = project.nodes.iter().find(|n| n.id == "g.inc").expect("g.inc opaque node");
    assert_eq!(inc.scope, vec!["g".to_string()]);
}

#[test]
fn anonymous_root_takes_source_id_at_every_depth() {
    // A standalone anonymous component takes its id DIRECTLY from the source
    // name (`source_name` passed to the compiler), at parse/flatten time. The
    // id must reach EVERY id-bearing field at EVERY nesting depth, with no
    // sentinel and no rename pass: the root group, its boundary passthroughs,
    // nested child groups (whose label defaults to their id), and edge ids.
    use weft_compiler::weft_compiler::{compile_with_mode, IncludeMode};
    let source = r#"
Group(raw: String) -> (cleaned: String) {
    strip = Text { value: "x" }
    sub = Group(in: String) -> (out: String) {
        inner = Text { value: "y" }
        self.out = inner.value
    }
    sub.in = strip.value
    self.cleaned = sub.out
}
"#;
    let project = compile_with_mode(source, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, Some("Cleaner"))
        .expect("compile");

    let blob = serde_json::to_string(&project).unwrap();
    assert!(!blob.contains("__include_root__"), "no sentinel anywhere: {blob}");
    let root = project.groups.iter().find(|g| g.id == "Cleaner").expect("root group named from source");
    // The anonymous root's label defaults to its id (the filename humanization
    // for display is the CLI/editor's concern, not the flatten's).
    assert_eq!(root.label.as_deref(), Some("Cleaner"));
    // The nested child group's id AND its (id-derived) label use the source id.
    let child = project.groups.iter().find(|g| g.id == "Cleaner.sub").expect("child group under root");
    assert_eq!(child.label.as_deref(), Some("Cleaner.sub"));
    assert!(project.nodes.iter().any(|n| n.id == "Cleaner.sub.inner"));
    assert!(project.nodes.iter().any(|n| n.id == "Cleaner__out"));
}

/// `__` is reserved for compiler-generated ids (`{group}__in`/`__out` boundary
/// passthroughs, `{host}__{field}` inline anon nodes). A user name containing
/// `__` would collide with those at flatten into a SILENT duplicate id (e.g. a
/// node `foo__in` beside a group `foo` -> two `g.foo__in`). It must be rejected
/// loudly at the source for nodes, groups, AND include aliases.
#[test]
fn double_underscore_user_names_rejected_loud() {
    let needs = |src: &str, what: &str| {
        let (p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
        assert!(
            errs.iter().any(|e| e.message.contains("reserved for compiler-generated ids")),
            "{what}: must reject `__` loudly: {errs:?}"
        );
        // And no duplicate flattened id slips through.
        let mut ids: Vec<_> = p.nodes.iter().map(|n| n.id.clone()).collect();
        ids.sort();
        assert!(ids.windows(2).all(|w| w[0] != w[1]), "{what}: no duplicate id: {ids:?}");
    };
    // A node named `foo__in` beside a group `foo` (the original silent-collision).
    needs(
        "g = Group(x: String) -> (y: String) {\n  foo__in = Text { value: \"v\" }\n  foo = Group(p: String) -> (q: String) {\n    inner = Text { value: \"i\" }\n    self.q = inner.value\n  }\n  foo.p = foo__in.value\n  self.y = foo.q\n}\n",
        "node-vs-group-boundary",
    );
    // A group named with `__`.
    needs("bad__name = Group() -> () {\n  n = Debug\n}\n", "group-name");
    // A node named with `__`.
    needs("a__b = Debug\n", "node-name");

    // A plausible legit name WITHOUT `__` still compiles fine (no false positive).
    let (_p, errs) = compile_lenient("my_node = Debug\n", uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.is_empty(), "single underscore is fine: {errs:?}");
}

#[test]
fn include_requires_single_group() {
    let dir = tempfile::tempdir().unwrap();
    // Two top-level groups: not a valid include target.
    std::fs::write(
        dir.path().join("bad.weft"),
        "a = Group -> (x: String) { n = Text { value: \"1\" }\n self.x = n.value }\nb = Group -> (y: String) { m = Text { value: \"2\" }\n self.y = m.value }\n",
    ).unwrap();
    let source = "c = @include(\"bad.weft\")\n";
    let errs = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("anonymous top-level Group")), "errs: {errs:?}");
}

#[test]
fn include_rejects_loose_nodes() {
    let dir = tempfile::tempdir().unwrap();
    // Loose node, no Group wrapper.
    std::fs::write(dir.path().join("loose.weft"), "n = Text { value: \"hi\" }\n").unwrap();
    let source = "c = @include(\"loose.weft\")\n";
    let errs = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("anonymous top-level Group")), "errs: {errs:?}");
}

#[test]
fn include_rejects_loose_top_level_connection() {
    // An included file's only top-level content may be the one anonymous Group.
    // A loose top-level connection alongside it is silently DROPPED (only the
    // group is consumed), so the gate must reject it loudly. Regression: the
    // `single_anon` check didn't look at `connections`.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("comp.weft"),
        "Group(raw: String) -> (cleaned: String) {\n  s = Text { value: \"x\" }\n  self.cleaned = s.value\n}\nlost_src.out = lost_tgt.in\n",
    ).unwrap();
    let errs = compile("c = @include(\"comp.weft\")\n", uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("anonymous top-level Group")), "loose connection must be rejected: {errs:?}");
}

#[test]
fn duplicate_config_key_is_loud() {
    // A config key set twice on one node (a body field AND a connection-origin
    // field, or two body fields) is a LOUD error, not silent last-write-wins.
    // Last-write-wins let the editor's per-key SetConfig/RemoveConfig touch only
    // one of the two values and strand the other.
    let body_then_conn = "t = Text {\n  value: \"a\"\n}\nt.value = \"b\"\n";
    let (_p, errs) = compile_lenient(body_then_conn, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.iter().any(|e| e.message.contains("duplicate config field 'value'")), "body+conn dup: {errs:?}");

    let two_body = "t = Text {\n  value: \"a\"\n  value: \"b\"\n}\n";
    let (_p2, errs2) = compile_lenient(two_body, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs2.iter().any(|e| e.message.contains("duplicate config field 'value'")), "two body fields dup: {errs2:?}");
}

#[test]
fn literal_to_non_node_port_is_loud() {
    // A literal is the "visual config" sugar that only a NODE has. Assigning one
    // to a port with no node config behind it is meaningless and previously
    // produced a phantom edge with an EMPTY source and no diagnostic. All such
    // targets must fail loud: a group boundary (`self.out`), and a group/include
    // alias port (`c.raw`). A node config fill (`n.port = "v"`) and an inline
    // node (`c.raw = Text{...}.value`) are still valid.
    let assert_loud = |src: &str, fs: CompileFs, what: &str| {
        let (p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), fs, IncludeMode::Interface, None);
        assert!(
            errs.iter().any(|e| e.message.contains("only a node's own port takes a literal config value")),
            "{what}: literal to a non-node port must be loud: {errs:?}"
        );
        assert!(!p.edges.iter().any(|e| e.source.is_empty()), "{what}: no empty-source edge: {:?}", p.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>());
    };
    // Literal to a group's own boundary output.
    assert_loud("g = Group() -> (out: String) {\n  self.out = \"lit\"\n}\n", CompileFs::none(), "self-boundary");
    // Literal to an @include alias's input port (a group has no visual config).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("comp.weft"),
        "Group(raw: String) -> (cleaned: String) {\n  s = Text { value: \"x\" }\n  self.cleaned = s.value\n}\n").unwrap();
    assert_loud("c = @include(\"comp.weft\")\nc.raw = \"hi\"\n", CompileFs::disk(dir.path()), "include-alias-port");

    // BUT an inline node driving the include port is still valid (it's wiring,
    // not a literal): a real node is synthesized and wired, no error.
    let (_p, errs) = compile_lenient(
        "c = @include(\"comp.weft\")\nc.raw = Text { value: \"hi\" }.value\nout = Debug\nout.data = c.cleaned\n",
        uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), IncludeMode::Interface, None,
    );
    assert!(!errs.iter().any(|e| e.message.contains("only a node's own port takes")), "inline node to a group port is valid: {errs:?}");
}

#[test]
fn bare_port_name_reference_is_loud_not_a_literal() {
    // To wire a group's own input port `raw` into a node, the form is
    // `u.text = self.raw`. Writing `u.text = raw` (bare port name) used to be
    // SILENTLY coerced to the literal string `"raw"` (config `text: "raw"`), so
    // the group input was left unwired and the user's intent vanished with no
    // diagnostic. A bare identifier RHS must now fail loud.
    let bare = "g = Group(raw: String) -> (o: String) {\n  u = Upper {}\n  u.text = raw\n  self.o = u.out\n}\n";
    let (p, errs) = compile_lenient(bare, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, Some("Top"));
    assert!(
        errs.iter().any(|e| e.message.contains("has an invalid value `raw`")),
        "bare port-name reference must be a loud error: {errs:?}"
    );
    // It must NOT have silently become the literal string "raw" on the node.
    let u = p.nodes.iter().find(|n| n.id == "g.u").expect("node g.u");
    assert!(u.config.get("text").is_none(), "no silent literal `text: \"raw\"`: {:?}", u.config);

    // The correct `self.raw` form wires the group input boundary into the node.
    let wired = "g = Group(raw: String) -> (o: String) {\n  u = Upper {}\n  u.text = self.raw\n  self.o = u.out\n}\n";
    let (p2, errs2) = compile_lenient(wired, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, Some("Top"));
    assert!(!errs2.iter().any(|e| e.message.contains("invalid value")), "self.raw is valid: {errs2:?}");
    assert!(
        p2.edges.iter().any(|e| e.source == "g__in" && e.target == "g.u"),
        "self.raw wires the group input into u: {:?}", p2.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>()
    );
}

#[test]
fn duplicate_id_across_kinds_is_error() {
    // An include alias then a node reusing the alias (and vice versa) is a
    // duplicate id regardless of declaration order: nodes, groups, and include
    // aliases share one namespace.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("comp.weft"), CLEANER_WEFT).unwrap();
    let inc_then_node = "c = @include(\"comp.weft\")\nc = Debug\n";
    let errs = compile(inc_then_node, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("Duplicate id 'c'")), "inc-then-node: {errs:?}");

    let node_then_inc = "c = Debug\nc = @include(\"comp.weft\")\n";
    let errs = compile(node_then_inc, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("Duplicate id 'c'")), "node-then-inc: {errs:?}");
}

#[test]
fn include_two_anonymous_groups_reports_shape_not_sentinel() {
    // Two anonymous top-level groups in one file is illegal (a file is exactly
    // one interface); the error must explain the real cause. The legacy
    // `__include_root__` sentinel is gone, but assert it never resurfaces.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("two.weft"),
        "Group -> (a: String) { n = Text { value: \"1\" }\n self.a = n.value }\nGroup -> (b: String) { m = Text { value: \"2\" }\n self.b = m.value }\n",
    ).unwrap();
    let errs = compile("c = @include(\"two.weft\")\n", uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("exactly one anonymous top-level Group")), "errs: {errs:?}");
    assert!(!errs.iter().any(|e| e.message.contains("__include_root__")), "sentinel leaked: {errs:?}");
}

#[test]
fn nested_include_error_keeps_line() {
    // An error inside a nested include keeps a usable line number through the
    // error mapping (not dropped at the deeper level).
    let dir = tempfile::tempdir().unwrap();
    // inner.weft references a missing @file on a specific line.
    std::fs::write(
        dir.path().join("inner.weft"),
        "Group -> (x: String) {\n n = Text { value: @file(\"missing.txt\") }\n self.x = n.value\n}\n",
    ).unwrap();
    // outer.weft includes inner; outer is included by main -> 2 levels deep.
    std::fs::write(
        dir.path().join("outer.weft"),
        "Group -> (x: String) {\n inner = @include(\"inner.weft\")\n self.x = inner.x\n}\n",
    ).unwrap();
    let errs = compile("c = @include(\"outer.weft\")\n", uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    // The error path carries the inner file + its line (`inner.weft: 2:...`),
    // not a bare message with the location dropped.
    assert!(errs.iter().any(|e| e.message.contains("inner.weft: 2:")), "errs: {errs:?}");
}

#[test]
fn include_rejects_named_top_level_group() {
    let dir = tempfile::tempdir().unwrap();
    // A named top-level group: rejected; included files use an anonymous one.
    std::fs::write(
        dir.path().join("named.weft"),
        "thing = Group(raw: String) -> (cleaned: String) {\n n = Text { value: \"x\" }\n self.cleaned = n.value\n}\n",
    ).unwrap();
    let source = "c = @include(\"named.weft\")\n";
    let errs = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("anonymous top-level Group")), "errs: {errs:?}");
}

#[test]
fn include_detects_cycle() {
    let dir = tempfile::tempdir().unwrap();
    // a.weft (anonymous group) includes itself via a nested @include.
    std::fs::write(
        dir.path().join("a.weft"),
        "Group -> (x: String) {\n inner = @include(\"a.weft\")\n self.x = inner.x\n}\n",
    ).unwrap();
    let source = "c = @include(\"a.weft\")\n";
    let errs = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("cycle")), "errs: {errs:?}");
}

#[test]
fn include_rejects_path_escape() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("secret.weft"), CLEANER_WEFT).unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let source = "c = @include(\"../secret.weft\")\n";
    let errs = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(&root)).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("escapes")), "errs: {errs:?}");
}

#[test]
fn file_and_include_compose_in_one_project() {
    // Realistic combined case: a project that both injects a value from a
    // file and includes a component group, end to end through compile().
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("system.txt"), "be concise").unwrap();
    std::fs::write(
        dir.path().join("cleaner.weft"),
        "Group(raw: String) -> (cleaned: String) {\n strip = Text { value: \"x\" }\n self.cleaned = strip.value\n}\n",
    ).unwrap();

    let source = r#"
cfg = OpenRouterConfig { systemPrompt: @file("system.txt") }
clean = @include("cleaner.weft")
"#;
    let project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let cfg = project.nodes.iter().find(|n| n.id == "cfg").expect("cfg");
    assert_eq!(cfg.config.get("systemPrompt").and_then(|v| v.as_str()), Some("be concise"));
    assert!(project.nodes.iter().any(|n| n.id == "clean__in"));
    assert!(project.nodes.iter().any(|n| n.id == "clean.strip"));
}

#[test]
fn file_ref_outside_project_is_compile_error() {
    let source = r#"
node = SomeNode {
    x: @file("anything.txt")
}
"#;
    // No project root: a @file marker cannot resolve, must error loudly.
    let errors = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).unwrap_err();
    assert!(
        errors.iter().any(|e| e.message.contains("outside a project")),
        "errors: {errors:?}"
    );
}

#[test]
fn config_spans_record_inline_and_connection_origin() {
    use weft_core::project::ConfigOrigin;
    // `value` is set inline in the node body; `template` is set on a
    // connection line after the node. The editor needs both spans with the
    // right origin to rewrite each field in place.
    let source = r#"

t = Template {
  value: "hello"
}
t.template = "world"
"#;
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = result.nodes.iter().find(|n| n.id == "t").expect("node t");

    let value_span = node.config_spans.get("value").expect("value span");
    assert_eq!(value_span.origin, ConfigOrigin::Inline);
    assert_eq!(value_span.span.start_line, 4, "value is on line 4");

    let template_span = node.config_spans.get("template").expect("template span");
    assert_eq!(template_span.origin, ConfigOrigin::Connection);
    assert_eq!(template_span.span.start_line, 6, "t.template is on line 6");
}

#[test]
fn config_spans_one_liner_node() {
    use weft_core::project::ConfigOrigin;
    let source = "t = Text { value: \"hi\" }\n";
    let result = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("should compile");
    let node = result.nodes.iter().find(|n| n.id == "t").expect("node t");
    let span = node.config_spans.get("value").expect("value span");
    assert_eq!(span.origin, ConfigOrigin::Inline);
    assert_eq!(span.span.start_line, 1, "one-liner field on its declaration line");
}

#[test]
fn lenient_parse_keeps_valid_nodes_around_a_bad_line() {
    // A stray bare word mid-edit must NOT blank the graph: the valid nodes
    // around it still parse, the bad line is just an error. (compile_lenient +
    // IncludeMode come from the `use weft_compiler::weft_compiler::*` glob.)
    let src = "a = Text {\n  value: \"hi\"\n}\nb = Debug\nb.data = a.value\ndebug\n";
    let (project, errors) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    let ids: Vec<&str> = project.nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"a") && ids.contains(&"b"), "valid nodes survive: {ids:?}");
    assert!(errors.iter().any(|e| e.line() == 6), "bad line reported: {errors:?}");
}



#[test]
fn literal_config_to_undeclared_node_fails_loud() {
    // Regression: `ghost.temperature = 0.7` with no `ghost` node must be a loud
    // error, never a phantom edge with an empty source that discards the value.
    let src = "ghost.temperature = 0.7\nn = Llm { model: \"x\" }\n";
    let (proj, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.iter().any(|e| e.message.contains("ghost.temperature")), "loud error: {errs:?}");
    assert!(!proj.edges.iter().any(|e| e.source.is_empty()), "no empty-source phantom edge: {:?}", proj.edges);
}

#[test]
fn literal_config_to_group_or_include_port_is_not_an_error() {
    // A literal to a GROUP's input port (resolved downstream) is legitimate, not
    // the undeclared-node error above.
    let src = "g = Group(inp: String) -> () {\n  d = Debug\n  d.data = self.inp\n}\ng.inp = \"hi\"\n";
    let (_proj, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(!errs.iter().any(|e| e.message.contains("is not a declared node")), "group port config must not error: {errs:?}");
}

#[test]
fn include_alias_colliding_with_sibling_in_group_is_duplicate() {
    // Regression: an @include alias colliding with a sibling node name inside a
    // group body must be flagged duplicate (nodes/groups/include-aliases share
    // one id namespace).
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sub.weft"),
        "Group(raw: String) -> (cleaned: String) {\n s = Text { value: \"x\" }\n self.cleaned = s.value\n}\n").unwrap();
    let src = "g = Group() -> () {\n  a = Llm { model: \"x\" }\n  a = @include(\"sub.weft\")\n}\n";
    let errs = compile(src, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("Duplicate id 'a'") || e.message.contains("Duplicate id 'g.a'")), "errs: {errs:?}");
}

// ── round-3: parser-B strictness + inline-expr-in-group regressions ─────────

/// Every malformed statement that fits no accepted form is a loud error AND
/// still round-trips (the CST keeps a byte-covering error span).
#[test]
fn strict_classifier_rejects_malformed_forms() {
    use weft_compiler::cst::parse;
    for src in [
        "a.b.c = d\n",                  // 3-segment LHS
        "=\n",                          // bare =
        "= foo\n",                      // no LHS ident
        "n = @file(\"x\")\n",           // marker (non-include) as a node RHS
        "x = @includes_other(\"a\")\n", // include-prefix marker, not @include
    ] {
        assert_eq!(parse(src).to_string(), src, "must round-trip: {src:?}");
        let (_p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
        assert!(!errs.is_empty(), "must be a loud error: {src:?} -> {errs:?}");
    }
}

/// A connection-RHS inline expr INSIDE a group synthesizes an anon node whose id
/// matches the edge endpoint that names it (no dangling edge).
#[test]
fn inline_expr_in_group_has_no_dangling_edge() {
    let src = "g = Group() -> () {\n  b = Debug\n  b.data = Other { foo: \"y\" }.out\n}\n";
    let (proj, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.is_empty(), "no errors: {errs:?}");
    let ids: std::collections::HashSet<&str> = proj.nodes.iter().map(|n| n.id.as_str()).collect();
    for e in &proj.edges {
        assert!(ids.contains(e.source.as_str()) || e.source.ends_with("__in") || e.source.ends_with("__out"),
            "edge source '{}' must reference an existing node: nodes={ids:?}", e.source);
    }
    assert!(ids.contains("g.b__data"), "anon node scoped into the group: {ids:?}");
}

/// The synthesized inline-expr node's `scope` must MATCH its group-scoped id:
/// id `g.<x>__field` means "in group g", so scope must be `["g"]`, not `[]`.
/// Regression: the node id was scoped into the group but `parent_id` stayed
/// None, so its scope array was empty and a sibling edge tripped the
/// scope-reachability check. Covers BOTH synthesis paths (config-field value
/// and connection-RHS) since they share `lower_inline_expr`.
#[test]
fn inline_expr_node_scope_matches_its_id() {
    // config-field path: `key: Type{}.port` and connection path: `b.in = Type{}.port`
    let src = "g = Group() -> () {\n  b = Sink { data: Make { x: \"1\" }.out }\n  c = Debug\n  c.data = Other { y: \"2\" }.out\n}\n";
    let (proj, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.is_empty(), "no errors: {errs:?}");
    for anon in ["g.b__data", "g.c__data"] {
        let node = proj.nodes.iter().find(|n| n.id == anon).unwrap_or_else(|| panic!("missing {anon}: {:?}", proj.nodes.iter().map(|n| &n.id).collect::<Vec<_>>()));
        assert_eq!(node.scope, vec!["g".to_string()], "{anon} scope must match its group-scoped id");
    }
}

/// Two inline exprs on the same parent.field, or a user node colliding with the
/// synthesized id, is a loud error (not a silent drop / mis-wire).
#[test]
fn inline_expr_id_collision_is_loud() {
    let dup = "n = Foo {\n  x: Bar { k: 1 }.out\n}\nn.x = Baz { k: 2 }.out\n";
    let (_p, e) = compile_lenient(dup, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(e.iter().any(|d| d.message.contains("duplicate id")), "dup inline loud: {e:?}");
}

/// A malformed numeric (`1.2.3`, `--`, `3.`) is an ERROR token, not a NUMBER.
#[test]
fn malformed_numeric_is_not_a_number() {
    use weft_compiler::cst::{parse, SyntaxKind};
    for src in ["1.2.3", "--", "3.", "1-2"] {
        let has_error = parse(src).descendants_with_tokens().any(|e| e.kind() == SyntaxKind::ERROR);
        assert!(has_error, "{src:?} must lex to an ERROR token, not a clean NUMBER");
        assert_eq!(parse(src).to_string(), src, "{src:?} round-trips");
    }
}

// ── round-4 regression tests ────────────────────────────────────────────────

/// The error span must BOUND the culprit (start AND end columns), not just the
/// line, this pins the localization feature that previously shipped untested.
#[test]
fn error_span_bounds_the_culprit() {
    // `ghost.temperature` (cols 0..17 on line 1) is the undeclared config target.
    let src = "ghost.temperature = 0.7\nn = Llm { model: \"x\" }\n";
    let (_p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    let e = errs.iter().find(|e| e.message.contains("ghost.temperature")).expect("the error");
    assert_eq!(e.span.start_line, 1);
    assert_eq!(e.span.start_column, 0, "starts at the culprit, not the line");
    assert_eq!(e.span.end_line, 1);
    assert_eq!(e.span.end_column, "ghost.temperature".chars().count(), "ends at the culprit's end, not the whole line");
}

/// A child named the same as its enclosing group must NOT double-scope (the
/// round-4 regression): `pipe` inside group `pipe` is `pipe.pipe`, not
/// `pipe.pipe.pipe`, and an internal edge stays consistent (no dangling).
#[test]
fn child_named_like_group_not_double_scoped() {
    let src = "g = Group() -> () {\n  g = Debug\n  other = Debug\n  other.in = g.out\n}\n";
    let (p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.is_empty(), "no errors: {errs:?}");
    let ids: std::collections::HashSet<&str> = p.nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains("g.g") && ids.contains("g.other"), "single-scoped: {ids:?}");
    assert!(!ids.iter().any(|i| i.contains("g.g.g")), "no double-scope: {ids:?}");
    for e in &p.edges {
        assert!(ids.contains(e.source.as_str()) || e.source.ends_with("__in") || e.source.ends_with("__out"),
            "edge source '{}' references an existing node: {ids:?}", e.source);
    }
}

/// `Group` followed by anything other than `(`/`->`/`{`/EOL is malformed, ONE
/// error node, not a phantom group decl.
#[test]
fn group_keyword_misuse_is_one_error() {
    use weft_compiler::cst::{parse, SyntaxKind};
    for src in ["Group.x = y\n", "Group: v\n"] {
        let t = parse(src);
        assert_eq!(t.to_string(), src, "round-trips: {src:?}");
        let decls = t.children().filter(|c| matches!(c.kind(), SyntaxKind::NODE_DECL | SyntaxKind::GROUP_DECL)).count();
        assert_eq!(decls, 0, "{src:?} must not produce a phantom decl");
        assert!(t.children().any(|c| c.kind() == SyntaxKind::ERROR), "{src:?} is one ERROR");
    }
    // valid anon-group forms still parse as a group
    for src in ["Group() {\n  a = Debug\n}\n", "Group -> (o: String) {}\n"] {
        let t = parse(src);
        assert!(t.children().any(|c| c.kind() == SyntaxKind::GROUP_DECL), "{src:?} is a group");
    }
}

/// `@require_one_of (a, b)` with a space before `(` must FAIL LOUD, never
/// silently drop the constraint. The lexer only folds `(...)` into the marker
/// token when it abuts `@name`, so the space splits the args off; the lowering's
/// one validity gate then reports a precise, actionable error. The well-formed
/// `@require_one_of(a, b)` (no space) still parses and carries the constraint.
#[test]
fn require_one_of_with_space_before_paren_fails_loud() {
    let with_space = "g = Group(a: String, b: String) -> () {\n  @require_one_of (a, b)\n  n = Debug\n  n.data = self.a\n}\n";
    let (_p, errs) = compile_lenient(with_space, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(
        errs.iter().any(|e| e.message.contains("@require_one_of needs a parenthesized port list")),
        "space before `(` must be a loud, actionable error: {errs:?}"
    );
    // And never the misleading "missing closing parenthesis" (the parens exist).
    assert!(
        !errs.iter().any(|e| e.message.contains("missing closing parenthesis")),
        "must not misreport the spaced form as unbalanced parens: {errs:?}"
    );

    // The well-formed form carries the constraint with no errors.
    let no_space = "g = Group(a: String, b: String) -> () {\n  @require_one_of(a, b)\n  n = Debug\n  n.data = self.a\n}\n";
    let (p2, errs2) = compile_lenient(no_space, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs2.is_empty(), "well-formed @require_one_of has no errors: {errs2:?}");
    let g = p2.groups.iter().find(|g| g.id == "g").expect("group g");
    assert_eq!(g.one_of_required, vec![vec!["a".to_string(), "b".to_string()]], "constraint carried");

    // An empty `@require_one_of()` is also a loud error (not a silent no-op).
    let empty = "g = Group(a: String) -> () {\n  @require_one_of()\n  n = Debug\n  n.data = self.a\n}\n";
    let (_p3, errs3) = compile_lenient(empty, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(
        errs3.iter().any(|e| e.message.contains("@require_one_of needs a parenthesized port list")),
        "empty @require_one_of() must fail loud: {errs3:?}"
    );
}

/// A NESTED anonymous `Group(){}` is meaningless (a file has exactly one
/// interface, the top-level group). It must fail LOUD, not silently invent a
/// `{source_id}.{source_id}` group. Only the file's top-level group may be anon.
#[test]
fn nested_anonymous_group_fails_loud() {
    let src = "Group(a: String) -> (o: String) {\n  Group(b: String) -> (p: String) {\n    n = Debug\n    self.p = n.data\n  }\n  self.o = a\n}\n";
    let (p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, Some("Root"));
    assert!(
        errs.iter().any(|e| e.message.contains("nested group must be named")),
        "nested anonymous group must be a loud error: {errs:?}"
    );
    // And it must NOT have silently produced a `Root.Root` group.
    assert!(
        !p.groups.iter().any(|g| g.id == "Root.Root"),
        "no silently-invented nested-anon group: {:?}",
        p.groups.iter().map(|g| &g.id).collect::<Vec<_>>()
    );
    // The top-level anonymous group is still fine on its own.
    let solo = "Group(a: String) -> (o: String) {\n  n = Debug\n  self.o = n.data\n}\n";
    let (_p2, errs2) = compile_lenient(solo, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, Some("Root"));
    assert!(errs2.is_empty(), "top-level anon group is valid: {errs2:?}");
}

/// A child node whose local id SHADOWS its enclosing group's name (`g` inside
/// group `g`) with an inline-expr config must NOT double-scope. Regression: the
/// synthesized anon node + edge were briefly pre-scoped, so when the host local
/// matched the group name the rescope pass prefixed it twice (`g.g` -> `g.g.g`).
/// Anon ids stay raw until ONE scope pass, so this resolves to exactly `g.g`.
#[test]
fn inline_expr_host_shadowing_group_name_no_double_scope() {
    let src = "g = Group() -> () {\n  g = Sink { data: Make { x: \"1\" }.out }\n}\n";
    let (p, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.is_empty(), "no errors: {errs:?}");
    // The child node is `g.g`; its inline anon is `g.g__data`, both scoped ONCE.
    assert!(p.nodes.iter().any(|n| n.id == "g.g" && n.scope == ["g"]), "child scoped once: {:?}", p.nodes.iter().map(|n| (&n.id, &n.scope)).collect::<Vec<_>>());
    assert!(p.nodes.iter().any(|n| n.id == "g.g__data" && n.scope == ["g"]), "anon scoped once: {:?}", p.nodes.iter().map(|n| (&n.id, &n.scope)).collect::<Vec<_>>());
    // The edge wires `g.g__data.out -> g.g.data`, NOT `g.g.g__data -> g.g.g`.
    assert!(
        p.edges.iter().any(|e| e.source == "g.g__data" && e.target == "g.g"),
        "edge scoped once: {:?}", p.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>()
    );
    assert!(
        !p.nodes.iter().any(|n| n.id.contains("g.g.g")) && !p.edges.iter().any(|e| e.source.contains("g.g.g") || e.target.contains("g.g.g")),
        "nothing triple-scoped"
    );
}



/// Post-body output port syntax (`} -> (out: T)` after a body close) is
/// gone. Such source must NOT silently parse as if the output sig were
/// part of the decl: the `-> (...)` after the `}` is a separate
/// statement that doesn't fit any line shape, so the parser produces
/// an ERROR span there. This pins that the form does not round-trip
/// into a clean `outputs: [...]` on the node anymore.
#[test]
fn post_body_output_syntax_is_no_longer_accepted() {
    let src = "n = Llm { value: \"x\" } -> (out: String)\n";
    let (project, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    let n = project.nodes.iter().find(|x| x.id == "n").expect("node n must exist");
    let has_out = n.outputs.iter().any(|p| p.name == "out");
    // BOTH must hold: an `||` here would let a regression that silently
    // re-adds the port pass whenever any unrelated error exists.
    assert!(
        !has_out,
        "post-body output syntax must not produce an `out` port; got {:?}",
        n.outputs.iter().map(|p| &p.name).collect::<Vec<_>>(),
    );
    assert!(!errs.is_empty(), "the dangling `-> (...)` must produce a parse error");
}

/// Same test for the separate-line form: `}\n-> (out: T)`.
#[test]
fn separate_line_post_body_output_syntax_is_no_longer_accepted() {
    let src = "n = Llm {\n  value: \"x\"\n}\n-> (out: String)\n";
    let (project, errs) = compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    let n = project.nodes.iter().find(|x| x.id == "n").expect("node n must exist");
    let has_out = n.outputs.iter().any(|p| p.name == "out");
    assert!(
        !has_out,
        "separate-line post-body output syntax must not silently add the `out` port; got {:?}",
        n.outputs.iter().map(|p| &p.name).collect::<Vec<_>>(),
    );
    assert!(!errs.is_empty(), "the dangling `-> (...)` must produce a parse error");
}

#[test]
fn include_with_internal_loop_lowers() {
    // Full-mode @include with a Loop inside the included file: the
    // loop's LoopIn/LoopOut boundary nodes must land scoped under the
    // call-site alias, same as Group's boundary Passthroughs. A regression
    // in the include resolution would either drop the Loop or scope it
    // wrong.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("inner.weft"),
        "Group(items: List[String]) -> (results: List[String | Null]) {\n  \
         doit = Loop(items: List[String]) -> (results: List[String | Null]) {\n    \
           parallel: false\n    \
           over: [\"items\"]\n    \
           p = Text { value: self.items }\n    \
           self.results = p.value\n  \
         }\n  \
         doit.items = self.items\n  \
         self.results = doit.results\n\
         }\n",
    ).unwrap();
    let project = compile("c = @include(\"inner.weft\")\n", uuid::Uuid::new_v4(), CompileFs::disk(dir.path()))
        .expect("compile with loop inside include");
    let ids: std::collections::HashSet<(String, String)> = project
        .nodes
        .iter()
        .map(|n| (n.id.clone(), n.node_type.clone()))
        .collect();
    // Outer Group boundary Passthroughs scoped under the alias `c`.
    assert!(ids.contains(&("c__in".into(), "Passthrough".into())), "outer group in: {ids:?}");
    assert!(ids.contains(&("c__out".into(), "Passthrough".into())), "outer group out: {ids:?}");
    // Inner Loop boundary nodes scoped one level deeper.
    assert!(ids.contains(&("c.doit__in".into(), "LoopIn".into())), "loop in: {ids:?}");
    assert!(ids.contains(&("c.doit__out".into(), "LoopOut".into())), "loop out: {ids:?}");
}

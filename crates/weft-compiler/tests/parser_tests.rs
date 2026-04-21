use weft_compiler::weft_compiler::*;
use weft_core::weft_type::{WeftPrimitive, WeftType};
use weft_core::{GroupBoundaryRole, LaneMode};

#[test]
fn test_basic_project() {
    let source = r#"
# Project: Test
# Description: A test project

config = LlmConfig {
    model: "gpt-4"
}

llm = Llm {
    temperature: 0.7
}

llm.config = config.value
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    assert_eq!(result.name, "Test");
    assert_eq!(result.description, Some("A test project".to_string()));
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
# Project: Bare
node = Debug
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile bare node");
    assert_eq!(result.nodes.len(), 1);
    assert_eq!(result.nodes[0].id, "node");
    assert_eq!(result.nodes[0].node_type, "Debug");
}

#[test]
fn test_node_with_ports() {
    let source = r#"
# Project: Ports
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile node with ports");
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
# Project: PortsNoConfig
pass = ExecPython(data: String) -> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 1);
    assert_eq!(node.outputs.len(), 1);
    assert!(node.config.as_object().map(|o| o.is_empty()).unwrap_or(true));
}

#[test]
fn test_node_empty_inputs() {
    let source = r#"
# Project: EmptyInputs
gen = ExecPython() -> (result: String) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile node with empty inputs");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 0);
    assert_eq!(node.outputs.len(), 1);
}

#[test]
fn test_group_basic() {
    let source = r#"
# Project: Group Test

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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
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

    // Passthrough input: outputs should have Single lane mode
    assert_eq!(pt_in.outputs[0].lane_mode, LaneMode::Single);
    assert_eq!(pt_out.inputs[0].lane_mode, LaneMode::Single);
}

#[test]
fn test_nested_groups() {
    let source = r#"
# Project: Nested

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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
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
# Project: Reserved
self = Debug {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "'self' should be a reserved word");
}

#[test]
fn test_connection_direction() {
    // target.input = source.output
    let source = r#"
# Project: Direction
a = Text { value: "hi" }
b = Debug {}
b.data = a.value
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let edge = &result.edges[0];
    assert_eq!(edge.target, "b");
    assert_eq!(edge.target_handle.as_deref(), Some("data"));
    assert_eq!(edge.source, "a");
    assert_eq!(edge.source_handle.as_deref(), Some("value"));
}

#[test]
fn test_group_self_connections() {
    let source = r#"
# Project: Self
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "{{data}}" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
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
# Project: Backtick

node = ExecPython {
    code: ```
print(\"line1\")
print(\"line2\")
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile triple backtick");
    let node = result.nodes.iter().find(|n| n.id == "node").unwrap();
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.contains("print(\"line1\")"));
    assert!(code.contains("print(\"line2\")"));
}

#[test]
fn test_triple_backtick_inline() {
    let source = "
# Project: BacktickInline
node = ExecPython {
    code: ```print(\"hello\")```
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile inline backtick");
    let node = result.nodes.iter().find(|n| n.id == "node").unwrap();
    assert_eq!(node.config.get("code").unwrap().as_str().unwrap(), "print(\"hello\")");
}

#[test]
fn test_triple_backtick_inline_with_braces() {
    let source = "
# Project: InlineBraces
node = ExecPython {
    code: ```return {\"result\": f\"{name} ({email})\"}```
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile inline backtick with braces");
    let node = result.nodes.iter().find(|n| n.id == "node").unwrap();
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.contains("return"), "code should contain return: got {:?}", code);
    assert!(code.contains("result"), "code should contain result: got {:?}", code);
}

#[test]
fn test_port_types() {
    let source = r#"
# Project: Types
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile typed ports");
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
    // v2: no explicit expand/gather. Types are declared as-is.
    // Expand/gather is inferred during enrichment, not compilation.
    let source = r#"
# Project: GroupPorts
batch = Group(items: List[String]) -> (results: List[String]) {
    worker = Llm {}
    worker.prompt = self.items
    self.results = worker.response
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile group ports");
    let pt_in = result.nodes.iter().find(|n| n.id == "batch__in").unwrap();
    // All lane modes are Single after compilation (inference happens in enrichment)
    assert_eq!(pt_in.inputs[0].lane_mode, LaneMode::Single);
    assert_eq!(pt_in.inputs[0].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));
    assert_eq!(pt_in.outputs[0].lane_mode, LaneMode::Single);

    let pt_out = result.nodes.iter().find(|n| n.id == "batch__out").unwrap();
    assert_eq!(pt_out.outputs[0].lane_mode, LaneMode::Single);
    assert_eq!(pt_out.outputs[0].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));
    assert_eq!(pt_out.inputs[0].lane_mode, LaneMode::Single);
    assert_eq!(pt_out.inputs[0].port_type, WeftType::list(WeftType::Primitive(WeftPrimitive::String)));
}

#[test]
fn test_require_one_of() {
    let source = r#"
# Project: RequireOneOf
resolver = ExecPython(
    text: String?,
    audio: Audio?,
    @require_one_of(text, audio)
) -> (result: String) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile @require_one_of");
    let node = &result.nodes[0];
    assert_eq!(node.features.one_of_required.len(), 1);
    assert_eq!(node.features.one_of_required[0], vec!["text", "audio"]);
    assert!(!node.inputs[0].required, "text should be optional");
    assert!(!node.inputs[1].required, "audio should be optional");
}

#[test]
fn test_mock_rejected() {
    let source = r#"
# Project: Mock
node = HttpRequest {
    url: "https://api.test.com"
    mock: {"body": "hello", "status": 200}
    mocked: true
}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "mock/mocked should be rejected as compile errors");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("'mock' is not a valid config key")));
    assert!(errors.iter().any(|e| e.message.contains("'mocked' is not a valid config key")));
}

#[test]
fn test_group_description_from_comments() {
    // First comment block inside group body is the description (like a docstring)
    // The compiler skips it like any other comment
    let source = r#"
# Project: Desc
grp = Group(data: String) -> (result: String) {
    # This is the group description
    # It can be multiple lines

    worker = Template { template: "{{data}}" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile with group description");
    assert_eq!(result.nodes.len(), 3); // grp__in, grp__out, grp.worker
}

#[test]
fn test_typevar_ports() {
    let source = r#"
# Project: TypeVar
node = ExecPython(
    data: T
) -> (
    result: T
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile TypeVar ports");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::TypeVar("T".to_string()));
    assert_eq!(node.outputs[0].port_type, WeftType::TypeVar("T".to_string()));
}

#[test]
fn test_must_override_port() {
    let source = r#"
# Project: MustOverride
node = ExecPython(data) -> (result) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile MustOverride ports");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::MustOverride);
    assert_eq!(node.outputs[0].port_type, WeftType::MustOverride);
}

#[test]
fn test_triple_nested_groups() {
    let source = r#"
# Project: Triple Nested

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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile triple nested");

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
# Project: ScopedNames
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile scoped names");
    let a_worker = result.nodes.iter().find(|n| n.id == "group_a.worker").unwrap();
    let b_worker = result.nodes.iter().find(|n| n.id == "group_b.worker").unwrap();
    assert_eq!(a_worker.config.get("template").unwrap().as_str().unwrap(), "A");
    assert_eq!(b_worker.config.get("template").unwrap().as_str().unwrap(), "B");
}

#[test]
fn test_nested_node_with_multiline_signature_in_group() {
    let source = "
# Project: Nested Multi
# Description: Test

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
    label: \"Inner\"
    code: ```
return {\"output\": \"hello\"}
    ```
  }
  inner_node.input = self.data
  self.result = inner_node.output
}
";
    let result = compile(source, uuid::Uuid::new_v4());
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
# Project: ComplexTypes
node = ExecPython(
    a: Dict[String, Number],
    b: Dict[String, List[String] | Number],
    c: List[Dict[String, Number]]
) -> (
    d: Dict[String, Dict[String, List[String] | Number] | String]
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile complex types");
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
# Project: Media
node = ExecPython(input: Media) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile Media alias");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].port_type, WeftType::media());
}

#[test]
fn test_mock_always_rejected() {
    // Even with invalid JSON, mock key itself is rejected before JSON parsing
    let source = r#"
# Project: BadMock
node = HttpRequest {
    url: "https://api.test.com"
    mock: {broken json
}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "mock should be rejected as compile error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("'mock' is not a valid config key")));
}

#[test]
fn test_reject_invalid_type() {
    let source = r#"
# Project: BadType
node = ExecPython(data: Foo) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Unknown type 'Foo' should produce an error");
}

#[test]
fn test_reject_any_type() {
    let source = r#"
# Project: NoAny
node = ExecPython(data: Any) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "'Any' is not a valid type");
}

#[test]
fn test_group_with_no_body() {
    let source = r#"
# Project: EmptyGroup
grp = Group(data: String) -> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile group with no body");
    // Should still create passthrough nodes
    let pt_in = result.nodes.iter().find(|n| n.id == "grp__in");
    assert!(pt_in.is_some());
}

#[test]
fn test_multiple_connections() {
    let source = r#"
# Project: Multi
a = Text { value: "hi" }
b = Llm {}
c = Debug {}
b.prompt = a.value
c.data = b.response
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile multiple connections");
    assert_eq!(result.edges.len(), 2);
}

#[test]
fn test_pack_with_require_one_of_in_group() {
    let source = "
# Project: Pack Test
# Description: Test

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
    label: \"Metadata\"
  }
  pack_node.notes = self.notes
  pack_node.priority = self.priority
  self.metadata = pack_node.out
}
";
    let result = compile(source, uuid::Uuid::new_v4());
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
# Project: Test JSON
# Description: Test

review = HumanQuery {
  label: "Test"
  fields: [{
    "fieldType":"display",
    "key":"name"
  }, {
    "fieldType":"text_input",
    "key":"notes"
  }]
}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    if let Err(ref errors) = result {
        for e in errors { eprintln!("ERR: {}", e); }
    }
    let result = result.expect("should compile multiline JSON array");
    let node = result.nodes.iter().find(|n| n.id == "review").unwrap();
    for (k, v) in node.config.as_object().unwrap() { eprintln!("  {}: {}", k, v); }
    let fields = node.config.get("fields").expect("fields should exist");
    assert!(fields.is_array(), "fields should be a JSON array");
}

// ─── Post-config Output Ports ──────────────────────────────────────────────

#[test]
fn test_post_config_output_ports() {
    let source = r#"
# Project: PostConfig
node = Llm {
    temperature: 0.7
} -> (
    summary: String,
    score: Number?
)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile post-config output ports");
    let node = &result.nodes[0];
    assert_eq!(node.outputs.len(), 2);
    assert_eq!(node.outputs[0].name, "summary");
    assert!(node.outputs[0].required);
    assert_eq!(node.outputs[1].name, "score");
    assert!(!node.outputs[1].required);
}

#[test]
fn test_post_config_output_ports_with_blank_lines() {
    let source = r#"
# Project: PostConfigBlank
node = Llm {
    temperature: 0.7
}

-> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile post-config with blank lines before ->");
    let node = &result.nodes[0];
    assert_eq!(node.outputs.len(), 1);
    assert_eq!(node.outputs[0].name, "result");
}

#[test]
fn test_post_config_output_ports_multiline() {
    let source = r#"
# Project: PostConfigMulti
node = Llm {
    temperature: 0.7
} -> (
    summary: String,
    keywords: List[String],
    score: Number?
)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.outputs.len(), 3);
    assert_eq!(node.outputs[0].name, "summary");
    assert_eq!(node.outputs[1].name, "keywords");
    assert_eq!(node.outputs[2].name, "score");
}

#[test]
fn test_post_config_duplicate_output_port_error() {
    let source = r#"
# Project: PostConfigDup
node = ExecPython() -> (result: String) {
    code: "return {}"
} -> (result: Number)
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Duplicate output port 'result' should produce error");
}

#[test]
fn test_pre_and_post_config_output_ports_combined() {
    let source = r#"
# Project: PreAndPost
node = ExecPython(data: String) -> (result: String) {
    code: "return {}"
} -> (extra: Number)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile combined pre + post outputs");
    let node = &result.nodes[0];
    assert_eq!(node.outputs.len(), 2);
    assert!(node.outputs.iter().any(|p| p.name == "result"));
    assert!(node.outputs.iter().any(|p| p.name == "extra"));
}

#[test]
fn test_group_post_config_output_ports() {
    let source = r#"
# Project: Test
# Description: Test

test = Group {
  # This is a test

  inner = Debug { label: "X" }
} -> (testing: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    if let Err(ref errors) = result {
        for e in errors { eprintln!("ERR: {}", e); }
    }
    let result = result.expect("should compile group with post-config outputs");
    // The group should have the output port 'testing'
    let pt_out = result.nodes.iter().find(|n| n.id == "test__out").unwrap();
    assert!(pt_out.outputs.iter().any(|p| p.name == "testing"), "test__out should have 'testing' output");
}

#[test]
fn test_pre_config_and_post_config_outputs_merged() {
    // Pattern: id = Type -> (pre_output) { config } -> (post_output1, post_output2)
    // The pre-config output and post-config outputs should all end up on the node.
    let source = r#"
# Project: MergedOutputs
node = ExecPython -> (response: String) {
    parseJson: true
} -> (summary: String, score: Number)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile merged pre+post config outputs");
    let node = &result.nodes[0];
    assert_eq!(node.outputs.len(), 3, "should have 3 outputs: response + summary + score");
    assert!(node.outputs.iter().any(|p| p.name == "response"), "should have response");
    assert!(node.outputs.iter().any(|p| p.name == "summary"), "should have summary");
    assert!(node.outputs.iter().any(|p| p.name == "score"), "should have score");
    assert_eq!(node.config.get("parseJson").unwrap().as_bool().unwrap(), true);
}

#[test]
fn test_output_only_no_inputs_no_config() {
    // Pattern: id = Type -> (output: String)
    let source = r#"
# Project: OutputOnly
node = ExecPython -> (result: String)
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile output-only declaration");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 0);
    assert_eq!(node.outputs.len(), 1);
    assert_eq!(node.outputs[0].name, "result");
}

// ─── Config Value Parsing ──────────────────────────────────────────────────

#[test]
fn test_config_boolean_values() {
    let source = r#"
# Project: Bool
node = ExecPython {
    enabled: true
    disabled: false
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile booleans");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("enabled").unwrap(), &serde_json::json!(true));
    assert_eq!(node.config.get("disabled").unwrap(), &serde_json::json!(false));
}

#[test]
fn test_config_numeric_values() {
    let source = r#"
# Project: Numbers
node = ExecPython {
    count: 42
    rate: 0.75
    negative: -10
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile numbers");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("count").unwrap(), &serde_json::json!(42));
    assert_eq!(node.config.get("rate").unwrap(), &serde_json::json!(0.75));
    assert_eq!(node.config.get("negative").unwrap(), &serde_json::json!(-10));
}

#[test]
fn test_config_escaped_string() {
    let source = r#"
# Project: Escaped
node = ExecPython {
    prompt: "line1\nline2\ttab"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile escaped strings");
    let node = &result.nodes[0];
    let prompt = node.config.get("prompt").unwrap().as_str().unwrap();
    assert!(prompt.contains('\n'));
    assert!(prompt.contains('\t'));
}

#[test]
fn test_config_json_array_inline() {
    let source = r#"
# Project: JsonArr
node = ExecPython {
    items: ["a", "b", "c"]
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile inline JSON array");
    let node = &result.nodes[0];
    let items = node.config.get("items").unwrap();
    assert!(items.is_array());
    assert_eq!(items.as_array().unwrap().len(), 3);
}

#[test]
fn test_config_json_object_inline() {
    let source = r#"
# Project: JsonObj
node = ExecPython {
    headers: {"Authorization": "Bearer token", "Content-Type": "application/json"}
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile inline JSON object");
    let node = &result.nodes[0];
    let headers = node.config.get("headers").unwrap();
    assert!(headers.is_object());
    assert_eq!(headers.get("Authorization").unwrap().as_str().unwrap(), "Bearer token");
}

#[test]
fn test_config_bare_string() {
    let source = r#"
# Project: Bare
node = ExecPython {
    mode: streaming
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile bare string");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("mode").unwrap().as_str().unwrap(), "streaming");
}

#[test]
fn test_config_empty_quoted_string() {
    let source = r#"
# Project: EmptyStr
node = ExecPython {
    prefix: ""
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile empty string");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("prefix").unwrap().as_str().unwrap(), "");
}

// ─── Label Parsing ─────────────────────────────────────────────────────────

#[test]
fn test_label_quoted() {
    let source = r#"
# Project: Label
node = ExecPython {
    label: "My Worker Node"
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("My Worker Node"));
}

#[test]
fn test_label_unquoted() {
    let source = r#"
# Project: LabelBare
node = ExecPython {
    label: Worker
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile unquoted label");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("Worker"));
}

#[test]
fn test_label_with_escapes() {
    let source = r#"
# Project: LabelEsc
node = ExecPython {
    label: "Has \"quotes\" inside"
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile escaped label");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("Has \"quotes\" inside"));
}

#[test]
fn test_label_in_oneliner() {
    let source = r#"
# Project: LabelOneLiner
node = ExecPython { label: "Quick", code: "return {}" }
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let node = &result.nodes[0];
    assert_eq!(node.label.as_deref(), Some("Quick"));
    assert!(node.config.get("code").is_some());
}

// ─── Error Cases ───────────────────────────────────────────────────────────

#[test]
fn test_error_unclosed_config_block() {
    let source = r#"
# Project: UnclosedConfig
node = ExecPython {
    code: "return {}"
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Unclosed config block should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Unclosed config block")));
}

#[test]
fn test_error_unclosed_group() {
    let source = r#"
# Project: UnclosedGroup
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "hi" }
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Unclosed group should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Unclosed group")));
}

#[test]
fn test_error_duplicate_root_node_id() {
    let source = r#"
# Project: DupNode
node = Text { value: "a" }
node = Text { value: "b" }
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Duplicate node ID should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate node ID")));
}

#[test]
fn test_error_duplicate_group_name() {
    let source = r#"
# Project: DupGroup
grp = Group() -> ()
grp = Group() -> ()
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Duplicate group name should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate group name")));
}

#[test]
fn test_error_duplicate_node_in_group() {
    let source = r#"
# Project: DupInGroup
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "a" }
    worker = Template { template: "b" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Duplicate node in group should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate node ID")));
}

#[test]
fn test_error_require_one_of_in_outputs() {
    let source = r#"
# Project: OorOutput
node = ExecPython() -> (
    a: String?,
    b: String?,
    @require_one_of(a, b)
) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "@require_one_of in outputs should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("@require_one_of is only valid in input")));
}

#[test]
fn test_error_require_one_of_missing_paren() {
    let source = r#"
# Project: OorBad
node = ExecPython(
    a: String?,
    b: String?,
    @require_one_of(a, b
) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "@require_one_of missing ) should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("missing closing parenthesis")));
}

#[test]
fn test_error_invalid_port_type() {
    let source = r#"
# Project: BadPortType
node = ExecPython(data: Foo) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Invalid type 'Foo' should error");
}

#[test]
fn test_error_duplicate_port_name() {
    let source = r#"
# Project: DupPort
node = ExecPython(data: String, data: Number) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Duplicate port name should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Duplicate")));
}

#[test]
fn test_error_port_name_starts_with_number() {
    let source = r#"
# Project: BadPortName
node = ExecPython(1data: String) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Port name starting with number should error");
}

#[test]
fn test_error_unexpected_root_content() {
    let source = r#"
# Project: Unexpected
node = Text { value: "hi" }
this is not valid syntax
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Random text should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Unexpected")));
}

#[test]
fn test_error_broken_multiline_json() {
    let source = r#"
# Project: BrokenJson
node = ExecPython {
    data: [{
        "key": "value"

node2 = Text { value: "hi" }
"#;
    let result = compile(source, uuid::Uuid::new_v4());
    assert!(result.is_err(), "Broken JSON should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("Broken JSON") || e.message.contains("Unclosed")));
}

// ─── Empty and Minimal Projects ────────────────────────────────────────────

#[test]
fn test_empty_source() {
    let source = "";
    let result = compile(source, uuid::Uuid::new_v4()).expect("empty project should compile");
    assert_eq!(result.name, "Untitled Project");
    assert_eq!(result.nodes.len(), 0);
}

#[test]
fn test_comments_only() {
    let source = r#"
# Project: CommentsOnly
# Description: Nothing here

# Just comments
# More comments
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    assert_eq!(result.name, "CommentsOnly");
    assert_eq!(result.nodes.len(), 0);
    assert_eq!(result.edges.len(), 0);
}

#[test]
fn test_no_project_name_uses_default() {
    let source = r#"
node = Text { value: "hi" }
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile without project name");
    assert_eq!(result.name, "Untitled Project");
}

// ─── Multiline Port Signatures ─────────────────────────────────────────────

#[test]
fn test_multiline_inputs_and_outputs_on_separate_lines() {
    let source = r#"
# Project: MultiLine
node = ExecPython(
    input1: String,
    input2: Number
) -> (
    output1: String
) {
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile multiline sig");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 2);
    assert_eq!(node.outputs.len(), 1);
}

#[test]
fn test_arrow_on_next_line() {
    let source = "
# Project: ArrowNext
node = ExecPython(data: String)
-> (result: String) {
    code: \"return {}\"
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile arrow on next line");
    let node = &result.nodes[0];
    assert_eq!(node.inputs.len(), 1);
    assert_eq!(node.outputs.len(), 1);
}

#[test]
fn test_deeply_split_signature() {
    let source = r#"
# Project: DeeplySplit
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile deeply split sig");
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
# Project: Dedent
node = ExecPython {
    code: ```
        line1
        line2
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
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
# Project: EmptyBT
node = ExecPython {
    code: ```
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile empty triple backtick");
    let node = &result.nodes[0];
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.trim().is_empty(), "empty backtick should produce empty string");
}

#[test]
fn test_triple_backtick_with_escaped_backticks() {
    let source = "
# Project: EscBT
node = ExecPython {
    code: ```
print(\"\\`\\`\\`\")
    ```
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile escaped backticks");
    let node = &result.nodes[0];
    let code = node.config.get("code").unwrap().as_str().unwrap();
    assert!(code.contains("```"), "escaped backticks should become real backticks");
}

// ─── One-liner Config ──────────────────────────────────────────────────────

#[test]
fn test_oneliner_config() {
    let source = r#"
# Project: OneLiner
node = ExecPython { code: "return {}", mode: "fast" }
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile one-liner config");
    let node = &result.nodes[0];
    assert_eq!(node.config.get("code").unwrap().as_str().unwrap(), "return {}");
    assert_eq!(node.config.get("mode").unwrap().as_str().unwrap(), "fast");
}

#[test]
fn test_empty_config_block() {
    let source = r#"
# Project: EmptyConfig
node = ExecPython(data: String) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile empty config");
    let node = &result.nodes[0];
    assert!(node.config.as_object().unwrap().is_empty());
}

// ─── Comments ──────────────────────────────────────────────────────────────

#[test]
fn test_comments_between_declarations() {
    let source = r#"
# Project: Comments
a = Text { value: "one" }

# This is a comment between nodes
# Another comment

b = Text { value: "two" }
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    assert_eq!(result.nodes.len(), 2);
}

#[test]
fn test_comment_after_opening_brace() {
    let source = "
# Project: BraceComment
node = ExecPython(data: String) -> (result: String) { # This is a config block
    code: \"return {}\"
}
";
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile with comment after {");
    let node = &result.nodes[0];
    assert!(node.config.get("code").is_some());
}

// ─── Port Features ─────────────────────────────────────────────────────────

#[test]
fn test_multiple_require_one_of_groups() {
    let source = r#"
# Project: MultiOor
node = ExecPython(
    text: String?,
    audio: Audio?,
    url: String?,
    file: String?,
    @require_one_of(text, audio)
    @require_one_of(url, file)
) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile multiple @require_one_of");
    let node = &result.nodes[0];
    assert_eq!(node.features.one_of_required.len(), 2);
    assert_eq!(node.features.one_of_required[0], vec!["text", "audio"]);
    assert_eq!(node.features.one_of_required[1], vec!["url", "file"]);
}

#[test]
fn test_port_underscore_name() {
    let source = r#"
# Project: UnderscorePort
node = ExecPython(_internal: String) -> (_result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile underscore port names");
    let node = &result.nodes[0];
    assert_eq!(node.inputs[0].name, "_internal");
    assert_eq!(node.outputs[0].name, "_result");
}

#[test]
fn test_port_must_override_optional() {
    let source = r#"
# Project: MustOverrideOpt
node = ExecPython(data?, required_data) -> (result) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let node = &result.nodes[0];
    assert!(!node.inputs[0].required, "data? should be optional");
    assert!(node.inputs[1].required, "required_data should be required (default)");
    assert_eq!(node.inputs[0].port_type, WeftType::MustOverride);
}

// ─── Null in Types ─────────────────────────────────────────────────────────

#[test]
fn test_null_in_union_type() {
    let source = r#"
# Project: NullType
node = ExecPython(data: String | Null) -> (result: String | Null) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile Null in union");
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
# Project: WsConn
a = Text { value: "hi" }
b = Debug {}
b.data   =   a.value
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile connections with whitespace");
    assert_eq!(result.edges.len(), 1);
    assert_eq!(result.edges[0].target, "b");
    assert_eq!(result.edges[0].source, "a");
}

#[test]
fn test_multiple_connections_to_same_node() {
    let source = r#"
# Project: MultiConn
src1 = Text { value: "a" }
src2 = Text { value: "b" }
target = ExecPython(x: String, y: String) -> (result: String) {
    code: "return {}"
}
target.x = src1.value
target.y = src2.value
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    assert_eq!(result.edges.len(), 2);
}

// ─── Group Edge Cases ──────────────────────────────────────────────────────

#[test]
fn test_group_empty_body() {
    let source = r#"
# Project: EmptyBody
grp = Group(data: String) -> (result: String) {}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile empty group body");
    let pt_in = result.nodes.iter().find(|n| n.id == "grp__in").unwrap();
    let pt_out = result.nodes.iter().find(|n| n.id == "grp__out").unwrap();
    assert_eq!(pt_in.node_type, "Passthrough");
    assert_eq!(pt_out.node_type, "Passthrough");
}

#[test]
fn test_group_with_only_connections() {
    let source = r#"
# Project: ConnOnly
grp = Group(data: String) -> (result: String) {
    self.result = self.data
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile group with passthrough wiring");
    // Only passthrough nodes, no child nodes
    assert_eq!(result.nodes.len(), 2); // grp__in, grp__out
    // Connection from __in to __out
    assert!(result.edges.iter().any(|e| e.source == "grp__in" && e.target == "grp__out"));
}

#[test]
fn test_same_node_id_in_different_groups_allowed() {
    let source = r#"
# Project: SameIdDiffGroup
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile: same node ID in different groups is allowed");
    assert!(result.nodes.iter().any(|n| n.id == "a.proc"));
    assert!(result.nodes.iter().any(|n| n.id == "b.proc"));
}

// ─── Config in Group ───────────────────────────────────────────────────────

#[test]
fn test_require_one_of_in_config_block() {
    let source = r#"
# Project: OorConfig
node = ExecPython(
    a: String?,
    b: String?
) -> (result: String) {
    @require_one_of(a, b)
    code: "return {}"
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile @require_one_of in config");
    let node = &result.nodes[0];
    assert_eq!(node.features.one_of_required.len(), 1);
    assert_eq!(node.features.one_of_required[0], vec!["a", "b"]);
}

// ─── Mixed Complex Scenarios ───────────────────────────────────────────────

#[test]
fn test_full_workflow_small() {
    let source = r#"
# Project: Full Workflow
# Description: Small end-to-end test

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
    label: "Summarizer"
    temperature: 0.5
    model: "gpt-4"
}
llm.prompt = processor.clean

output = Debug {}
output.data = llm.response
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile full workflow");
    assert_eq!(result.name, "Full Workflow");
    assert_eq!(result.description, Some("Small end-to-end test".to_string()));
    // input + processor__in + processor__out + processor.trimmer + llm + output = 6
    assert_eq!(result.nodes.len(), 6);
    // input->processor__in, processor__in->trimmer, trimmer->processor__out,
    // processor__out->llm, llm->output = 5 edges
    assert_eq!(result.edges.len(), 5);

    let llm = result.nodes.iter().find(|n| n.id == "llm").unwrap();
    assert_eq!(llm.label.as_deref(), Some("Summarizer"));
    assert_eq!(llm.config.get("temperature").unwrap(), &serde_json::json!(0.5));
}

#[test]
fn test_group_with_post_config_outputs_on_inner_node() {
    let source = r#"
# Project: InnerPostConfig
grp = Group(data: String) -> (summary: String, score: Number) {
    llm = Llm {
        temperature: 0.7
    } -> (
        summary: String,
        score: Number
    )
    llm.prompt = self.data
    self.summary = llm.summary
    self.score = llm.score
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile inner node with post-config outputs");
    let llm = result.nodes.iter().find(|n| n.id == "grp.llm").unwrap();
    assert_eq!(llm.outputs.len(), 2);
    assert!(llm.outputs.iter().any(|p| p.name == "summary"));
    assert!(llm.outputs.iter().any(|p| p.name == "score"));
}

// ─── Scope & GroupBoundary ─────────────────────────────────────────────

#[test]
fn test_scope_top_level_nodes() {
    let source = r#"
# Project: Scope
a = Text { value: "hello" }
b = Template { template: "{{data}}" }
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");
    for node in &result.nodes {
        assert!(node.scope.is_empty(), "top-level node '{}' should have empty scope", node.id);
        assert!(node.group_boundary.is_none(), "top-level node '{}' should not be a boundary", node.id);
    }
}

#[test]
fn test_scope_simple_group() {
    let source = r#"
# Project: Scope
grp = Group(data: String) -> (result: String) {
    worker = Template { template: "{{data}}" }
    worker.value = self.data
    self.result = worker.output
}
"#;
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");

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
# Project: Nested
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile nested groups");

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
# Project: Triple
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile triple nested");

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
# Project: SelectiveMock
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
    let result = compile(source, uuid::Uuid::new_v4()).expect("should compile");

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

#[test]
fn post_config_outputs_one_liner() {
    // Post-config outputs on a single line must be parsed correctly.
    // This is the syntax LLMs produce most often.
    let src = r#"# Project: Test
# Description: test

draft = LlmInference -> (response: JsonDict) { label: "Draft", parseJson: true } -> (subject: String, body: String)
out = Debug
out.data = draft.subject
"#;
    let project = compile(src, uuid::Uuid::new_v4()).expect("should compile");
    let draft = project.nodes.iter().find(|n| n.id == "draft").expect("draft node");
    let port_names: Vec<&str> = draft.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(port_names.contains(&"response"), "missing response port: {:?}", port_names);
    assert!(port_names.contains(&"subject"), "missing subject post-config port: {:?}", port_names);
    assert!(port_names.contains(&"body"), "missing body post-config port: {:?}", port_names);
}

#[test]
fn post_config_outputs_multi_line() {
    // Same thing but multi-line config block (was already working, regression test).
    let src = r#"# Project: Test
# Description: test

draft = LlmInference -> (response: JsonDict) {
  label: "Draft"
  parseJson: true
} -> (subject: String, body: String)
out = Debug
out.data = draft.subject
"#;
    let project = compile(src, uuid::Uuid::new_v4()).expect("should compile");
    let draft = project.nodes.iter().find(|n| n.id == "draft").expect("draft node");
    let port_names: Vec<&str> = draft.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(port_names.contains(&"response"), "missing response port: {:?}", port_names);
    assert!(port_names.contains(&"subject"), "missing subject post-config port: {:?}", port_names);
    assert!(port_names.contains(&"body"), "missing body post-config port: {:?}", port_names);
}

#[test]
fn post_config_outputs_brace_arrow_same_line() {
    let src = r#"# Project: Test
# Description: test

qualify = LlmInference -> (response: JsonDict) { label: "Qualify Lead", parseJson: true } -> (is_promising: Boolean, reason: String, summary: String)
draft = LlmInference -> (response: JsonDict) { label: "Draft Email", parseJson: true } -> (subject: String, body: String)
out = Debug
out.data = draft.subject
"#;
    let project = compile(src, uuid::Uuid::new_v4()).expect("should compile");

    let qualify = project.nodes.iter().find(|n| n.id == "qualify").expect("qualify node");
    let q_ports: Vec<&str> = qualify.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(q_ports.contains(&"is_promising"), "qualify missing is_promising: {:?}", q_ports);
    assert!(q_ports.contains(&"reason"), "qualify missing reason: {:?}", q_ports);
    assert!(q_ports.contains(&"summary"), "qualify missing summary: {:?}", q_ports);

    let draft = project.nodes.iter().find(|n| n.id == "draft").expect("draft node");
    let d_ports: Vec<&str> = draft.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(d_ports.contains(&"subject"), "draft missing subject: {:?}", d_ports);
    assert!(d_ports.contains(&"body"), "draft missing body: {:?}", d_ports);
}

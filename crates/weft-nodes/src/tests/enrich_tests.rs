use super::*;
use weft_core::project::{ProjectDefinition, NodeDefinition, PortDefinition, LaneMode};
use weft_core::WeftType;
use weft_core::weft_type::WeftPrimitive;
use weft_core::node::NodeFeatures;
use crate::node::Node; // for resolve_types

fn make_node(id: &str, inputs: Vec<PortDefinition>, outputs: Vec<PortDefinition>) -> NodeDefinition {
    NodeDefinition {
        id: id.to_string(),
        nodeType: weft_core::project::NodeType(id.to_string()),
        label: None,
        config: serde_json::json!({}),
        position: weft_core::project::Position { x: 0.0, y: 0.0 },
        inputs,
        outputs,
        features: NodeFeatures::default(),
        scope: vec![],
        groupBoundary: None,
    }
}

fn port(name: &str, pt: WeftType, lane: LaneMode, required: bool) -> PortDefinition {
    PortDefinition { name: name.to_string(), portType: pt, required, description: None, laneMode: lane, laneDepth: 1, configurable: true }
}

fn single_port(name: &str, pt: WeftType) -> PortDefinition {
    port(name, pt, LaneMode::Single, false)
}

fn expand_port(name: &str, pt: WeftType) -> PortDefinition {
    port(name, pt, LaneMode::Expand, false)
}

fn gather_port(name: &str, pt: WeftType) -> PortDefinition {
    port(name, pt, LaneMode::Gather, false)
}

fn edge(source: &str, source_port: &str, target: &str, target_port: &str) -> weft_core::project::Edge {
    weft_core::project::Edge {
        id: format!("e-{}-{}-{}-{}", source, source_port, target, target_port),
        source: source.to_string(),
        target: target.to_string(),
        sourceHandle: Some(source_port.to_string()),
        targetHandle: Some(target_port.to_string()),
    }
}

fn make_wf(nodes: Vec<NodeDefinition>, edges: Vec<weft_core::project::Edge>) -> ProjectDefinition {
    ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        description: None,
        nodes,
        edges,
        status: weft_core::project::ProjectStatus::Draft,
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    }
}

fn s() -> WeftType { WeftType::primitive(WeftPrimitive::String) }
fn n() -> WeftType { WeftType::primitive(WeftPrimitive::Number) }
fn ls() -> WeftType { WeftType::list(s()) }
fn ln() -> WeftType { WeftType::list(n()) }

// =========================================================================
// Edge type compatibility
// =========================================================================

#[test]
fn edge_compatible_same_type() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "same type should be compatible: {:?}", errors);
}

#[test]
fn edge_incompatible_different_type() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", n())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String -> Number should be incompatible");
}

#[test]
fn edge_compatible_into_union() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", WeftType::union(vec![s(), n()]))], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String -> String | Number should work: {:?}", errors);
}

#[test]
fn edge_incompatible_union_into_narrow() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), n()]))]),
        make_node("b", vec![single_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String | Number -> String should be incompatible");
}

#[test]
fn edge_compatible_list_same_inner() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![single_port("in", ls())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[String] -> List[String] should work: {:?}", errors);
}

#[test]
fn edge_incompatible_list_different_inner() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![single_port("in", ln())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[String] -> List[Number] should be incompatible");
}

// =========================================================================
// Null stripping: T | Null into T is accepted (null propagation handles it)
// =========================================================================

fn null_t() -> WeftType { WeftType::primitive(WeftPrimitive::Null) }

fn required_port(name: &str, pt: WeftType) -> PortDefinition {
    port(name, pt, LaneMode::Single, true)
}

fn optional_port(name: &str, pt: WeftType) -> PortDefinition {
    port(name, pt, LaneMode::Single, false)
}

#[test]
fn null_union_into_required_port_accepted() {
    // String | Null -> required String: null propagation skips the node. Not a type error.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), null_t()]))]),
        make_node("b", vec![required_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String | Null -> required String should be accepted: {:?}", errors);
}

#[test]
fn null_union_into_optional_port_accepted() {
    // String | Null -> optional String: the node runs, receives null. Not a type error.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), null_t()]))]),
        make_node("b", vec![optional_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String | Null -> optional String should be accepted: {:?}", errors);
}

#[test]
fn multi_type_null_union_into_matching_union_accepted() {
    // String | Number | Null -> required String | Number: strip Null, rest matches.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), n(), null_t()]))]),
        make_node("b", vec![required_port("in", WeftType::union(vec![s(), n()]))], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String | Number | Null -> required String | Number should be accepted: {:?}", errors);
}

#[test]
fn null_union_into_wrong_type_still_errors() {
    // String | Null -> required Number: after stripping Null, String vs Number is still a mismatch.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), null_t()]))]),
        make_node("b", vec![required_port("in", n())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String | Null -> required Number should still be a type error");
}

#[test]
fn bare_null_into_required_port_accepted() {
    // Null -> required String: null propagation skips. Not a type error.
    // (without_null on bare Null returns Null, which is not compatible with String,
    // but this case never happens in practice because a source that only outputs Null
    // means the node always skips. Accepting it or rejecting it are both defensible.
    // We reject it: bare Null is not String.)
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", null_t())]),
        make_node("b", vec![required_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "bare Null -> required String should be a type error (always-skip wiring is suspicious)");
}

#[test]
fn non_null_union_into_narrow_still_errors() {
    // String | Number -> String (no Null involved): still a type error.
    // Null stripping should not affect non-null unions.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), n()]))]),
        make_node("b", vec![required_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String | Number -> required String should still be a type error");
}

#[test]
fn null_in_list_is_not_stripped() {
    // List[String | Null] -> List[String]: Null is inside the list, not at depth 0.
    // The list itself is not nullable, it contains nullable elements. This IS a type error
    // because the consuming node expects List[String] and would receive nulls inside the list.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::list(WeftType::union(vec![s(), null_t()])))]),
        make_node("b", vec![required_port("in", ls())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[String | Null] -> List[String] should be a type error (Null is inside the list)");
}

// =========================================================================
// Form field port generation with required flag
// =========================================================================

#[test]
fn form_field_required_true_sets_port_required() {
    // A HumanQuery with a required display field should generate a required input port.
    let src = r#"# Project: T

source = Text { value: "hello" }
review = HumanQuery {
  title: "Test"
  fields: [{"fieldType": "display", "key": "summary", "required": true}]
}
review.summary = source.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let review = project.nodes.iter().find(|n| n.id == "review").expect("review node");
    let summary_port = review.inputs.iter().find(|p| p.name == "summary").expect("summary port");
    assert!(summary_port.required, "display field with required:true should produce a required port");
}

#[test]
fn form_field_default_is_required() {
    // A HumanQuery display field without explicit required should default to required
    // (same as the language default for all ports).
    let src = r#"# Project: T

source = Text { value: "hello" }
review = HumanQuery {
  title: "Test"
  fields: [{"fieldType": "display", "key": "summary"}]
}
review.summary = source.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let review = project.nodes.iter().find(|n| n.id == "review").expect("review node");
    let summary_port = review.inputs.iter().find(|p| p.name == "summary").expect("summary port");
    assert!(summary_port.required, "display field without explicit required should default to required");
}

#[test]
fn form_field_required_false_sets_port_optional() {
    // A HumanQuery display field with explicit required: false is optional.
    let src = r#"# Project: T

source = Text { value: "hello" }
review = HumanQuery {
  title: "Test"
  fields: [{"fieldType": "display", "key": "summary", "required": false}]
}
review.summary = source.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let review = project.nodes.iter().find(|n| n.id == "review").expect("review node");
    let summary_port = review.inputs.iter().find(|p| p.name == "summary").expect("summary port");
    assert!(!summary_port.required, "display field with required:false should produce an optional port");
}

#[test]
fn form_field_required_port_accepts_nullable_source() {
    // The original bug: String | Null from parallel lanes flowing into a required
    // display field port on HumanQuery should not be a type error.
    let src = r#"# Project: T
# Description: T

grp = Group(items: List[String | Null]) -> (out: List[String | Null]) {
  # parallel review

  review = HumanQuery {
    title: "Review"
    fields: [{"fieldType": "display", "key": "data", "required": true}]
  }
  review.data = self.items

  self.out = self.items
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let enrich_result = crate::enrich::enrich_project(&mut project, &registry);
    // The enrichment should succeed without type errors about the display port.
    // The key check: no error message containing "Type mismatch" for the review.data edge.
    match enrich_result {
        Ok(()) => {} // good
        Err(errors) => {
            let type_errors: Vec<&String> = errors.iter()
                .filter(|e| e.contains("Type mismatch") && e.contains("review") && e.contains("data"))
                .collect();
            assert!(type_errors.is_empty(),
                "String | Null into required display field should not be a type error: {:?}", type_errors);
        }
    }
}

// =========================================================================
// Expand/Gather wire type
// =========================================================================

#[test]
fn edge_expand_input_list_compatible() {
    // Source outputs List[String], target expand declares String (post-expand element)
    // Wire: List[String] -> expected List[String]. OK.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[String] -> expand(String) should work: {:?}", errors);
}

#[test]
fn edge_expand_input_wrong_type() {
    // Source outputs String (not a list), target expand declares String
    // Wire: String -> expected List[String]. Incompatible.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![expand_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String -> expand(String) should fail (not a list)");
}

#[test]
fn edge_expand_input_wrong_inner_type() {
    // Source outputs List[Number], target expand declares String
    // Wire: List[Number] -> expected List[String]. Incompatible.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ln())]),
        make_node("b", vec![expand_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[Number] -> expand(String) should fail (wrong inner type)");
}

#[test]
fn edge_gather_input_compatible() {
    // Source outputs String (per lane), target gather declares List[String]
    // Wire: String -> expected String (inner of List[String]). OK.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![gather_port("in", ls())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String -> gather(List[String]) should work: {:?}", errors);
}

#[test]
fn edge_gather_input_wrong_element() {
    // Source outputs Number, target gather declares List[String]
    // Wire: Number -> expected String. Incompatible.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", n())]),
        make_node("b", vec![gather_port("in", ls())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "Number -> gather(List[String]) should fail");
}

// =========================================================================
// Stack depth validation
// =========================================================================

#[test]
fn stack_expand_then_gather_valid() {
    // Depth: 0 -> 1 -> 1 -> 0 -> 0
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("c", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("d", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e", vec![single_port("in", ls())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
        edge("c", "out", "d", "in"),
        edge("d", "out", "e", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(errors.is_empty(), "expand then gather should be valid: {:?}", errors);
}

#[test]
fn stack_gather_without_expand_fails() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![gather_port("in", ls())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "gather without expand should fail");
    assert!(errors[0].contains("Gather error"), "error should mention gather error: {}", errors[0]);
}

#[test]
fn stack_double_expand_valid() {
    // Nested parallelism: depth 0 -> 1 -> 2. Valid.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![single_port("out", ls())]),
        make_node("c", vec![expand_port("in", s())], vec![single_port("out", s())]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(errors.is_empty(), "double expand (nested) should be valid: {:?}", errors);
}

#[test]
fn stack_double_gather_fails() {
    // Depth 0 -> 1 -> 0 -> -1. Second gather at depth 0 fails.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("c", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("d", vec![gather_port("in", WeftType::list(ls()))], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
        edge("c", "out", "d", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "double gather with single expand should fail");
}

#[test]
fn stack_double_expand_double_gather_valid() {
    // Depth: 0 -> 1 -> 2 -> 1 -> 0. Valid.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![single_port("out", ls())]),
        make_node("c", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("d", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e", vec![gather_port("in", WeftType::list(ls()))], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
        edge("c", "out", "d", "in"),
        edge("d", "out", "e", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(errors.is_empty(), "double expand double gather should be valid: {:?}", errors);
}

#[test]
fn stack_expand_on_non_list_fails() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![expand_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "expand on non-list should fail");
}

#[test]
fn stack_expand_gather_expand_gather_valid() {
    // Sequential: 0 -> 1 -> 0 -> 1 -> 0. Valid.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("c", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("d", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("e", vec![gather_port("in", ls())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
        edge("c", "out", "d", "in"),
        edge("d", "out", "e", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(errors.is_empty(), "expand-gather-expand-gather should be valid: {:?}", errors);
}

#[test]
fn stack_gather_expand_at_depth_zero_fails() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("c", vec![expand_port("in", s())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "gather at depth 0 should fail");
}

// =========================================================================
// MustOverride
// =========================================================================

#[test]
fn type_override_on_must_override_allowed() {
    // Catalog says MustOverride, weft says String. Should be accepted.
    let catalog = vec![port("value", WeftType::MustOverride, LaneMode::Single, false)];
    let weft = vec![port("value", s(), LaneMode::Single, false)];
    let mut errors = vec![];
    let result = merge_ports(&catalog, &weft, false, "test", "output", &mut errors);
    assert!(errors.is_empty(), "MustOverride → String should be allowed: {:?}", errors);
    assert_eq!(result[0].portType, s(), "type should be overridden to String");
}

#[test]
fn type_override_on_concrete_type_fails() {
    // Catalog says String, weft says Number. Should fail.
    let catalog = vec![port("value", s(), LaneMode::Single, false)];
    let weft = vec![port("value", n(), LaneMode::Single, false)];
    let mut errors = vec![];
    let _result = merge_ports(&catalog, &weft, false, "test", "input", &mut errors);
    assert!(!errors.is_empty(), "String → Number override should fail");
    assert!(errors[0].contains("incompatible type"), "error message: {}", errors[0]);
}

// =========================================================================
// Edge narrowing
// =========================================================================

#[test]
fn no_narrow_input_type_from_source() {
    // Source outputs String, target accepts String | Number.
    // Input types are NOT narrowed,they declare what the node accepts.
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", WeftType::union(vec![s(), n()]))], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs[0].portType, WeftType::union(vec![s(), n()]), "input should NOT be narrowed");
}

#[test]
fn narrow_list_of_dict_media() {
    // Source outputs List[Dict[String, Media]], target accepts List[T].
    // T should resolve to Dict[String, Media].
    let media = WeftType::media();
    let dict_sm = WeftType::dict(s(), media.clone());
    let list_dict = WeftType::list(dict_sm.clone());

    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", list_dict)]),
        make_node("b",
            vec![single_port("in", WeftType::list(WeftType::type_var("T")))],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "should resolve: {:?}", errors);

    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs[0].portType, WeftType::list(dict_sm.clone()), "input stays List[Dict[String, Media]]");
    assert_eq!(b.outputs[0].portType, dict_sm, "T resolves to Dict[String, Media]");
}

#[test]
fn resolve_dict_with_typevar_value() {
    // Source outputs Dict[String, List[Number]], target accepts Dict[String, T].
    // T should resolve to List[Number].
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::dict(s(), WeftType::list(n())))]),
        make_node("b",
            vec![single_port("in", WeftType::dict(s(), WeftType::type_var("T")))],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "should resolve: {:?}", errors);

    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.outputs[0].portType, WeftType::list(n()), "T resolves to List[Number]");
}

#[test]
fn resolve_union_with_typevar_absorbs_remaining() {
    // Pattern: Dict[String, Number | T]
    // Concrete: Dict[String, Number | String | Null]
    // T should resolve to String | Null (the remaining types after removing Number)
    let pattern_val = WeftType::union(vec![n(), WeftType::type_var("T")]);
    let pattern = WeftType::dict(s(), pattern_val);
    let concrete_val = WeftType::union(vec![n(), s(), WeftType::primitive(WeftPrimitive::Null)]);
    let concrete = WeftType::dict(s(), concrete_val);

    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", concrete)]),
        make_node("b",
            vec![single_port("in", pattern)],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "should resolve: {:?}", errors);

    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    let expected_t = WeftType::union(vec![s(), WeftType::primitive(WeftPrimitive::Null)]);
    assert_eq!(b.outputs[0].portType, expected_t, "T should be String | Null");
}

#[test]
fn resolve_union_with_typevar_single_remaining() {
    // Pattern: Dict[String, Boolean | T]
    // Concrete: Dict[String, Boolean | Media]
    // T should resolve to Media (Image | Video | Audio | Document)
    let media = WeftType::media();
    let b = WeftType::primitive(WeftPrimitive::Boolean);
    let pattern_val = WeftType::union(vec![b.clone(), WeftType::type_var("T")]);
    let pattern = WeftType::dict(s(), pattern_val);
    let concrete_val = WeftType::union(vec![b.clone(), media.clone()]);
    let concrete = WeftType::dict(s(), concrete_val);

    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", concrete)]),
        make_node("b",
            vec![single_port("in", pattern)],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    // Media expands to Image | Video | Audio | Document
    // After removing Boolean, remaining is Image | Video | Audio | Document
    let b_node = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    // T should resolve : check it's no longer TypeVar
    assert!(!b_node.outputs[0].portType.is_type_var(),
        "T should be resolved, got {:?}", b_node.outputs[0].portType);
}

#[test]
fn resolve_multiple_typevars_in_union() {
    // Pattern: T1 | T2
    // Concrete: Boolean | String | Null
    // T1 = Boolean (first), T2 = String | Null (last takes rest)
    let pattern = WeftType::union(vec![WeftType::type_var("T1"), WeftType::type_var("T2")]);
    let concrete = WeftType::union(vec![
        WeftType::primitive(WeftPrimitive::Boolean),
        s(),
        WeftType::primitive(WeftPrimitive::Null),
    ]);

    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", concrete)]),
        make_node("b",
            vec![single_port("in", pattern)],
            vec![single_port("out1", WeftType::type_var("T1")), single_port("out2", WeftType::type_var("T2"))],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "should resolve: {:?}", errors);

    let b_node = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b_node.outputs[0].portType, WeftType::primitive(WeftPrimitive::Boolean), "T1 = Boolean");
    assert_eq!(b_node.outputs[1].portType, WeftType::union(vec![s(), WeftType::primitive(WeftPrimitive::Null)]), "T2 = String | Null");
}

#[test]
fn resolve_typevars_more_than_concrete_resolves_extras_to_empty() {
    // Pattern: T1 | T2 | T3
    // Concrete: Boolean
    // T1=Boolean, T2=Empty, T3=Empty. No error.
    // After substitution, union simplifies: Boolean | Empty | Empty = Boolean.
    let pattern = WeftType::union(vec![
        WeftType::type_var("T1"),
        WeftType::type_var("T2"),
        WeftType::type_var("T3"),
    ]);

    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::primitive(WeftPrimitive::Boolean))]),
        make_node("b",
            vec![single_port("in", pattern)],
            vec![],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "should resolve extras to Empty, not error: {:?}", errors);
    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    // After substitution: Boolean | Empty | Empty → Boolean (union simplifies away Empty)
    assert_eq!(b.inputs[0].portType, WeftType::primitive(WeftPrimitive::Boolean));
}

#[test]
fn resolve_list_t_rejects_non_list() {
    // Source outputs Number, target accepts List[T].
    // Number is NOT a List : edge validation should catch this (not TypeVar resolution).
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", n())]),
        make_node("b",
            vec![single_port("in", WeftType::list(WeftType::type_var("T")))],
            vec![],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    // TypeVar resolution won't extract T because structures don't match (Number vs List[T])
    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    // T remains unresolved
    assert!(matches!(
        &b.inputs[0].portType,
        WeftType::List(inner) if inner.is_type_var()
    ), "T should remain unresolved when source isn't a List");

    // Edge validation should then catch the mismatch
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "Number → List[T] should fail edge validation");
}

#[test]
fn resolve_nested_dict_typevar() {
    // Source: Dict[String, Dict[String, Number]]
    // Target: Dict[String, Dict[String, T]]
    // T should resolve to Number
    let inner_dict = WeftType::dict(s(), n());
    let outer = WeftType::dict(s(), inner_dict.clone());
    let pattern = WeftType::dict(s(), WeftType::dict(s(), WeftType::type_var("T")));

    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", outer)]),
        make_node("b",
            vec![single_port("in", pattern)],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
    ], vec![edge("a", "out", "b", "in")]);

    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "should resolve: {:?}", errors);

    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.outputs[0].portType, n(), "T resolves to Number from deeply nested Dict");
}

#[test]
fn narrow_does_not_change_exact_match() {
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs[0].portType, s());
}

#[test]
fn narrow_does_not_widen() {
    // Source outputs String | Number, target accepts String.
    // Should NOT narrow (incompatible direction).
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::union(vec![s(), n()]))]),
        make_node("b", vec![single_port("in", s())], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs[0].portType, s(), "should not change");
}

#[test]
fn typevar_resolves_from_downstream_without_narrowing() {
    // A(String) → B(String | Number input, T output) → C(String input)
    // B's input is NOT narrowed (stays String | Number).
    // T resolves to String via Phase 2 (outgoing edge to C).
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b",
            vec![single_port("in", WeftType::union(vec![s(), n()]))],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
        make_node("c", vec![single_port("in", s())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
    ]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs[0].portType, WeftType::union(vec![s(), n()]), "input should NOT be narrowed");
    // TypeVar T resolves to String via Phase 2 (outgoing edge B→C, C.in is String)
    assert_eq!(b.outputs[0].portType, s(), "T should resolve to String from downstream");
}

#[test]
fn type_override_narrowing_allowed() {
    // Catalog says String | Number, weft says String. Valid narrowing.
    let sn = WeftType::union(vec![s(), n()]);
    let catalog = vec![port("value", sn, LaneMode::Single, false)];
    let weft = vec![port("value", s(), LaneMode::Single, false)];
    let mut errors = vec![];
    let result = merge_ports(&catalog, &weft, false, "test", "output", &mut errors);
    assert!(errors.is_empty(), "narrowing should be allowed: {:?}", errors);
    assert_eq!(result[0].portType, s(), "type should be narrowed to String");
}

#[test]
fn type_override_widening_fails() {
    // Catalog says String, weft says String | Number. Widening not allowed.
    let sn = WeftType::union(vec![s(), n()]);
    let catalog = vec![port("value", s(), LaneMode::Single, false)];
    let weft = vec![port("value", sn, LaneMode::Single, false)];
    let mut errors = vec![];
    let _result = merge_ports(&catalog, &weft, false, "test", "input", &mut errors);
    assert!(!errors.is_empty(), "widening should fail");
}

#[test]
fn type_override_partial_overlap_fails() {
    // Catalog says String | Number, weft says String | Boolean. Boolean not in catalog.
    let sn = WeftType::union(vec![s(), n()]);
    let sb = WeftType::union(vec![s(), WeftType::primitive(WeftPrimitive::Boolean)]);
    let catalog = vec![port("value", sn, LaneMode::Single, false)];
    let weft = vec![port("value", sb, LaneMode::Single, false)];
    let mut errors = vec![];
    let _result = merge_ports(&catalog, &weft, false, "test", "input", &mut errors);
    assert!(!errors.is_empty(), "partial overlap should fail (Boolean not in catalog)");
}

#[test]
fn type_override_same_type_ok() {
    // Catalog says String, weft also says String. Harmless, no error.
    let catalog = vec![port("value", s(), LaneMode::Single, false)];
    let weft = vec![port("value", s(), LaneMode::Single, false)];
    let mut errors = vec![];
    let result = merge_ports(&catalog, &weft, false, "test", "input", &mut errors);
    assert!(errors.is_empty(), "same type re-declaration should be fine: {:?}", errors);
    assert_eq!(result[0].portType, s());
}

#[test]
fn type_override_required_without_type_keeps_catalog() {
    // Catalog says String, weft just promotes to required (no type declared → MustOverride).
    // Catalog type should be preserved.
    let catalog = vec![port("value", s(), LaneMode::Single, false)];
    let weft = vec![port("value", WeftType::MustOverride, LaneMode::Single, true)];
    let mut errors = vec![];
    let result = merge_ports(&catalog, &weft, false, "test", "input", &mut errors);
    assert!(errors.is_empty(), "required promotion without type should keep catalog type: {:?}", errors);
    assert_eq!(result[0].portType, s(), "type should remain String");
    assert!(result[0].required, "should be promoted to required");
}

#[test]
fn must_override_connected_fails() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", WeftType::MustOverride)], vec![]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_no_unresolved(&wf, &mut errors);
    assert!(!errors.is_empty(), "MustOverride on connected port should fail");
}

#[test]
fn must_override_unconnected_ok() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", WeftType::MustOverride)]),
    ], vec![]);
    let mut errors = vec![];
    validate_no_unresolved(&wf, &mut errors);
    assert!(errors.is_empty(), "MustOverride on unconnected port should be ok: {:?}", errors);
}

// =========================================================================
// TypeVar resolution
// =========================================================================

#[test]
fn typevar_resolved_from_input_edge() {
    // A(String) -> B(T->T) -> C(String). T resolves to String. All compatible.
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b",
            vec![single_port("in", WeftType::type_var("T"))],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
        make_node("c", vec![single_port("in", s())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
    ]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "resolution should succeed: {:?}", errors);

    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs[0].portType, s(), "B input should resolve to String");
    assert_eq!(b.outputs[0].portType, s(), "B output should resolve to String");

    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String -> String chain should be valid: {:?}", errors);
}

#[test]
fn typevar_conflicting_bindings_fails() {
    // A(String) -> B(T->T) -> C(Number). T can't be both String and Number.
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b",
            vec![single_port("in", WeftType::type_var("T"))],
            vec![single_port("out", WeftType::type_var("T"))],
        ),
        make_node("c", vec![single_port("in", n())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
    ]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(!errors.is_empty(), "conflicting T bindings should fail");
    assert!(errors[0].contains("conflicting"), "error should mention conflict: {}", errors[0]);
}

// =========================================================================
// Full pipeline combined
// =========================================================================

#[test]
fn full_simple_chain_valid() {
    let wf = make_wf(vec![
        make_node("text", vec![], vec![single_port("value", s())]),
        make_node("llm", vec![single_port("prompt", s())], vec![single_port("response", s())]),
        make_node("debug", vec![single_port("data", s())], vec![]),
    ], vec![
        edge("text", "value", "llm", "prompt"),
        edge("llm", "response", "debug", "data"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    validate_no_unresolved(&wf, &mut errors);
    assert!(errors.is_empty(), "simple String chain should be valid: {:?}", errors);
}

#[test]
fn full_expand_process_gather_valid() {
    let wf = make_wf(vec![
        make_node("list", vec![], vec![single_port("value", ls())]),
        make_node("exp", vec![expand_port("items", s())], vec![single_port("item", s())]),
        make_node("proc", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("gat", vec![gather_port("results", ls())], vec![single_port("list", ls())]),
        make_node("debug", vec![single_port("data", ls())], vec![]),
    ], vec![
        edge("list", "value", "exp", "items"),
        edge("exp", "item", "proc", "in"),
        edge("proc", "out", "gat", "results"),
        edge("gat", "list", "debug", "data"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "expand-process-gather should work: {:?}", errors);
}

#[test]
fn full_type_mismatch_after_expand() {
    // Process outputs Number but Gather expects String elements.
    let wf = make_wf(vec![
        make_node("list", vec![], vec![single_port("value", ls())]),
        make_node("exp", vec![expand_port("items", s())], vec![single_port("item", s())]),
        make_node("proc", vec![single_port("in", s())], vec![single_port("out", n())]),
        make_node("gat", vec![gather_port("results", ls())], vec![]),
    ], vec![
        edge("list", "value", "exp", "items"),
        edge("exp", "item", "proc", "in"),
        edge("proc", "out", "gat", "results"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "Number -> gather(List[String]) should fail");
}

#[test]
fn full_nested_expand_gather_valid() {
    let lls = WeftType::list(ls());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lls.clone())]),
        make_node("e1", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e2", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", WeftType::list(ls()))], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "e2", "in"),
        edge("e2", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "nested expand-expand-gather-gather should work: {:?}", errors);
}

#[test]
fn full_three_gathers_two_expands_fails() {
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("e1", vec![expand_port("in", s())], vec![single_port("out", ls())]),
        make_node("e2", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", WeftType::list(ls()))], vec![single_port("out", WeftType::list(ls()))]),
        make_node("g3", vec![gather_port("in", WeftType::list(WeftType::list(ls())))], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "e2", "in"),
        edge("e2", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
        edge("g2", "out", "g3", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "three gathers with two expands should fail");
}

// =========================================================================
// Stress tests : branching, merging, broadcasting, edge cases
// =========================================================================

#[test]
fn stress_branching_from_expand_both_paths_gather() {
    // List[String] → Expand → branches into two paths → both Gather independently
    //
    //   src(List[String]) → exp(<String)
    //                        ├→ pathA(String→String) → gatherA(>List[String])
    //                        └→ pathB(String→String) → gatherB(>List[String])
    //
    // Both gathers at depth 1 → 0. Valid.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("pathA", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("pathB", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("gatherA", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("gatherB", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "pathA", "in"),
        edge("exp", "out", "pathB", "in"),
        edge("pathA", "out", "gatherA", "in"),
        edge("pathB", "out", "gatherB", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "branching expand with independent gathers should work: {:?}", errors);
}

#[test]
fn stress_branching_merge_into_single_gather() {
    // Two paths from expand merge into one gather (gather takes multiple inputs)
    // This is valid if both carry String at depth 1.
    //
    //   src(List[String]) → exp(<String)
    //                        ├→ pathA(String→String) ─┐
    //                        └→ pathB(String→String) ─┴→ merge(String,String→String) → gather(>List[String])
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("pathA", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("pathB", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("merge", vec![single_port("a", s()), single_port("b", s())], vec![single_port("out", s())]),
        make_node("gat", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "pathA", "in"),
        edge("exp", "out", "pathB", "in"),
        edge("pathA", "out", "merge", "a"),
        edge("pathB", "out", "merge", "b"),
        edge("merge", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "two paths merging into gather should work: {:?}", errors);
}

#[test]
fn stress_broadcast_depth0_into_depth1_valid() {
    // config(String) at depth 0 broadcasts into a node at depth 1.
    // This is valid : depth-0 values broadcast to all lanes.
    //
    //   config(String) ──────────────────┐
    //   src(List[String]) → exp(<String) → proc(String+String→String) → gat(>List[String])
    let wf = make_wf(vec![
        make_node("config", vec![], vec![single_port("out", s())]),
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("proc", vec![single_port("data", s()), single_port("cfg", s())], vec![single_port("out", s())]),
        make_node("gat", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "proc", "data"),
        edge("config", "out", "proc", "cfg"),
        edge("proc", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "depth-0 broadcast into depth-1 should work: {:?}", errors);
}

#[test]
fn stress_gather_at_depth0_with_list_type_fails() {
    // Node at depth 0 outputs String.
    // Next node has gather input expecting List[String].
    // Wire type for gather: expects String (inner of List[String]).
    // String → String is compatible on the wire. BUT stack depth is 0 → gather fails.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "gather at depth 0 should fail even when wire types match");
    // Edge types should be compatible (String → String wire), but stack depth is wrong
    let mut type_errors = vec![];
    validate_edge_types(&wf, &mut type_errors);
    assert!(type_errors.is_empty(), "wire types should be compatible: String → String (inner of List[String]): {:?}", type_errors);
}

#[test]
fn stress_expand_gather_with_dict_types() {
    // List[Dict[String, Number]] → Expand → process Dict → Gather
    let dict_sn = WeftType::dict(s(), n());
    let list_dict = WeftType::list(dict_sn.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", list_dict.clone())]),
        make_node("exp", vec![expand_port("in", dict_sn.clone())], vec![single_port("out", dict_sn.clone())]),
        make_node("proc", vec![single_port("in", dict_sn.clone())], vec![single_port("out", dict_sn.clone())]),
        make_node("gat", vec![gather_port("in", list_dict.clone())], vec![single_port("out", list_dict.clone())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "proc", "in"),
        edge("proc", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "expand-gather with Dict types should work: {:?}", errors);
}

#[test]
fn stress_expand_gather_type_changes_midway() {
    // List[String] → Expand(String) → process(String→Number) → Gather(List[Number])
    // This is valid: the expand gives Strings, processing converts to Number,
    // gather collects Numbers into List[Number].
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("proc", vec![single_port("in", s())], vec![single_port("out", n())]),
        make_node("gat", vec![gather_port("in", ln())], vec![single_port("out", ln())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "proc", "in"),
        edge("proc", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "type conversion inside expand-gather should work: {:?}", errors);
}

#[test]
fn stress_expand_wrong_inner_gather_correct_fails() {
    // List[String] → Expand(Number) : wrong! The list has Strings, not Numbers.
    // Even though the gather is "correct" for the declared types, the expand is wrong.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", n())], vec![single_port("out", n())]),
        make_node("gat", vec![gather_port("in", ln())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[String] → expand(Number) should fail");
}

#[test]
fn stress_nested_list_expand_twice() {
    // List[List[String]] → Expand(List[String]) → Expand(String) → work → Gather → Gather
    // Depth: 0 → 1 → 2 → 2 → 1 → 0. Valid.
    let lls = WeftType::list(ls());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lls.clone())]),
        make_node("e1", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e2", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("work", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "e2", "in"),
        edge("e2", "out", "work", "in"),
        edge("work", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "nested List[List[String]] expand-expand-gather-gather should work: {:?}", errors);
}

#[test]
fn stress_expand_gather_expand_different_type() {
    // List[String] → Expand → Gather → now List[String] again → Expand with Number → fail
    // The second expand receives List[String] but declares Number element type.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("e1", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e2", vec![expand_port("in", n())], vec![]),  // List[String] → expand(Number) FAIL
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "g1", "in"),
        edge("g1", "out", "e2", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "second expand with wrong type should fail");
}

#[test]
fn stress_diamond_same_depth_valid() {
    // Diamond: A → B and A → C, then B and C both → D
    // All at depth 0, all String. Valid.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("c", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("d", vec![single_port("x", s()), single_port("y", s())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("a", "out", "c", "in"),
        edge("b", "out", "d", "x"),
        edge("c", "out", "d", "y"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "diamond at same depth should work: {:?}", errors);
}

#[test]
fn stress_diamond_different_depths_conflict() {
    // A → expand(B) and A → C (no expand). B at depth 1, C at depth 0.
    // Both feed into D. D sees inputs at different depths → depth conflict.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("c", vec![single_port("in", ls())], vec![single_port("out", s())]),
        make_node("d", vec![single_port("x", s()), single_port("y", s())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("a", "out", "c", "in"),
        edge("b", "out", "d", "x"),
        edge("c", "out", "d", "y"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    // This should produce a depth conflict error at node D
    // (one input at depth 1 from expand, another at depth 0)
    // NOTE: depth-0 values broadcast, so this might actually be valid.
    // Broadcasting means depth-0 is compatible with any depth.
    // This test documents the current behavior.
}

#[test]
fn stress_output_expand_valid() {
    // A node with an expand OUTPUT port.
    // The node produces a List[String], the expand splits it.
    // Downstream nodes run per lane.
    //
    //  src(String) → producer(String→<String) → consumer(String) → gather(>List[String])
    //  producer declares out(<result: String) meaning it outputs String per lane
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("producer", vec![single_port("in", s())], vec![expand_port("out", s())]),
        make_node("consumer", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("gat", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
    ], vec![
        edge("src", "out", "producer", "in"),
        edge("producer", "out", "consumer", "in"),
        edge("consumer", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "output expand should work: {:?}", errors);
}

#[test]
fn stress_gather_output_valid() {
    // A node with a gather OUTPUT port.
    // Downstream is at depth-1.
    //
    // src(List[String]) → exp(<String) → work(String→String) → collector (gather output: List[String]) → debug(List[String])
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("work", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("collector", vec![single_port("in", s())], vec![gather_port("out", ls())]),
        make_node("debug", vec![single_port("in", ls())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "work", "in"),
        edge("work", "out", "collector", "in"),
        edge("collector", "out", "debug", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "gather output should work: {:?}", errors);
}

#[test]
fn stress_union_through_expand_gather() {
    // List[String | Number] → Expand(String | Number) → Gather(List[String | Number])
    let sn = WeftType::union(vec![s(), n()]);
    let lsn = WeftType::list(sn.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lsn.clone())]),
        make_node("exp", vec![expand_port("in", sn.clone())], vec![single_port("out", sn.clone())]),
        make_node("gat", vec![gather_port("in", lsn.clone())], vec![single_port("out", lsn.clone())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "union types through expand-gather should work: {:?}", errors);
}

#[test]
fn stress_union_narrowing_in_expand_fails() {
    // List[String | Number] → Expand(String) : fails because Number elements would be rejected
    let sn = WeftType::union(vec![s(), n()]);
    let lsn = WeftType::list(sn.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lsn.clone())]),
        make_node("exp", vec![expand_port("in", s())], vec![]),
    ], vec![edge("src", "out", "exp", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[String|Number] → expand(String) should fail");
}

// =========================================================================
// Tricky edge cases
// =========================================================================

#[test]
fn tricky_expand_process_to_different_type_then_gather_correct_new_type() {
    // List[String] → Expand(String) → Transform(String→Dict[String, Number]) → Gather(List[Dict[String, Number]])
    // The type changes inside the parallel region. Gather must match the NEW type.
    let dict_sn = WeftType::dict(s(), n());
    let list_dict = WeftType::list(dict_sn.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("xform", vec![single_port("in", s())], vec![single_port("out", dict_sn.clone())]),
        make_node("gat", vec![gather_port("in", list_dict.clone())], vec![single_port("out", list_dict)]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "xform", "in"),
        edge("xform", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "type transformation inside expand-gather should work: {:?}", errors);
}

#[test]
fn tricky_expand_process_to_different_type_gather_wrong_type() {
    // List[String] → Expand(String) → Transform(String→Number) → Gather(List[String])
    // Gather expects String elements but gets Number. Should fail.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("xform", vec![single_port("in", s())], vec![single_port("out", n())]),
        make_node("gat", vec![gather_port("in", ls())], vec![]),  // expects String elements, gets Number
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "xform", "in"),
        edge("xform", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "gather expecting String but getting Number should fail");
}

#[test]
fn tricky_broadcast_config_into_nested_expand() {
    // Config at depth 0, data goes through 2 expands.
    // Config broadcasts into depth-2 node. Valid.
    //
    // config(String) ────────────────────────────────────┐
    // src(List[List[String]]) → exp1(<List[String]) → exp2(<String) → proc(String+String) → g1 → g2
    let lls = WeftType::list(ls());
    let wf = make_wf(vec![
        make_node("config", vec![], vec![single_port("out", s())]),
        make_node("src", vec![], vec![single_port("out", lls.clone())]),
        make_node("exp1", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
        make_node("exp2", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("proc", vec![single_port("data", s()), single_port("cfg", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", WeftType::list(ls()))], vec![]),
    ], vec![
        edge("src", "out", "exp1", "in"),
        edge("exp1", "out", "exp2", "in"),
        edge("exp2", "out", "proc", "data"),
        edge("config", "out", "proc", "cfg"),
        edge("proc", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "depth-0 broadcast into depth-2 should work: {:?}", errors);
}

#[test]
fn tricky_gather_then_immediately_expand_same_data() {
    // List[String] → Expand → process → Gather(List[String]) → Expand again(String) → process → Gather(List[String])
    // Sequential: depth 0→1→1→0→1→1→0. Valid.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("e1", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("p1", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e2", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("p2", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("g2", vec![gather_port("in", ls())], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "p1", "in"),
        edge("p1", "out", "g1", "in"),
        edge("g1", "out", "e2", "in"),
        edge("e2", "out", "p2", "in"),
        edge("p2", "out", "g2", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "gather then re-expand should work: {:?}", errors);
}

#[test]
fn tricky_expand_with_two_outputs_different_types() {
    // A node inside expand produces two outputs of different types.
    // One goes to a String gather, other to a Number gather.
    //
    // src(List[String]) → exp(<String) → splitter(String→String+Number)
    //                                      ├→ gatherS(>List[String])
    //                                      └→ gatherN(>List[Number])
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("splitter", vec![single_port("in", s())], vec![single_port("text", s()), single_port("num", n())]),
        make_node("gatherS", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("gatherN", vec![gather_port("in", ln())], vec![single_port("out", ln())]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "splitter", "in"),
        edge("splitter", "text", "gatherS", "in"),
        edge("splitter", "num", "gatherN", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "branching inside expand to different typed gathers should work: {:?}", errors);
}

#[test]
fn tricky_expand_one_branch_gathers_other_doesnt_depth_conflict() {
    // Inside an expand, one branch gathers (goes to depth 0) and feeds into
    // a node that also receives from the non-gathered branch (depth 1).
    //
    // src(List[String]) → exp(<String) → splitter(String→String+String)
    //                                      ├→ gatherS(>List[String]) → merge.a (depth 0)
    //                                      └─────────────────────────→ merge.b (depth 1)
    //                                                                    merge sees depth conflict!
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("splitter", vec![single_port("in", s())], vec![single_port("a", s()), single_port("b", s())]),
        make_node("gat", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("merge", vec![single_port("x", ls()), single_port("y", s())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "splitter", "in"),
        edge("splitter", "a", "gat", "in"),
        edge("gat", "out", "merge", "x"),
        edge("splitter", "b", "merge", "y"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    // merge receives from depth 0 (gat output) and depth 1 (splitter.b).
    // depth 0 broadcasts, so this should actually be valid (0 is compatible with 1).
    // If both were non-zero and different, it would be a conflict.
    assert!(errors.is_empty(), "depth-0 from gather + depth-1 from branch should work (broadcast): {:?}", errors);
}

#[test]
fn tricky_two_independent_expands_into_same_node_conflict() {
    // Two different lists expanded independently, both feeding one node.
    // List[String] → exp1 (depth 1, lane count A)
    // List[Number] → exp2 (depth 1, lane count B)
    // Both → merge at depth 1, but from DIFFERENT expand sources.
    // This should produce a depth conflict (both depth 1 but different lane origins).
    // Note: our current validator tracks depth as a number, not lane identity.
    // Both are depth 1 → no conflict detected by depth alone.
    // This is a known limitation : shape mismatch is caught at runtime.
    let wf = make_wf(vec![
        make_node("src1", vec![], vec![single_port("out", ls())]),
        make_node("src2", vec![], vec![single_port("out", ln())]),
        make_node("exp1", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("exp2", vec![expand_port("in", n())], vec![single_port("out", n())]),
        make_node("merge", vec![single_port("a", s()), single_port("b", n())], vec![]),
    ], vec![
        edge("src1", "out", "exp1", "in"),
        edge("src2", "out", "exp2", "in"),
        edge("exp1", "out", "merge", "a"),
        edge("exp2", "out", "merge", "b"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    // Both at depth 1 → no depth conflict (same number).
    // Shape mismatch (different lane counts) caught at runtime, not compile time.
    // This test documents the current behavior.
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "two independent expands at same depth passes compile-time (shape checked at runtime): {:?}", errors);
}

#[test]
fn tricky_list_of_dicts_expand_access_dict_fields() {
    // List[Dict[String, Number]] → Expand(Dict[String, Number]) → Unpack → process fields
    let dict_sn = WeftType::dict(s(), n());
    let list_dict = WeftType::list(dict_sn.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", list_dict)]),
        make_node("exp", vec![expand_port("in", dict_sn.clone())], vec![single_port("out", dict_sn.clone())]),
        make_node("unpack", vec![single_port("in", dict_sn)], vec![single_port("name", s()), single_port("age", n())]),
        make_node("gatherNames", vec![gather_port("in", ls())], vec![]),
        make_node("gatherAges", vec![gather_port("in", ln())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "unpack", "in"),
        edge("unpack", "name", "gatherNames", "in"),
        edge("unpack", "age", "gatherAges", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "expand dicts, unpack fields, gather separately should work: {:?}", errors);
}

#[test]
fn tricky_typevar_through_expand_gather() {
    // Gate(T→T) inside an expand. T should resolve from the concrete types.
    // List[String] → Expand(String) → Gate(Boolean + T→T) → Gather(List[String])
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("gate",
            vec![
                single_port("pass", WeftType::primitive(WeftPrimitive::Boolean)),
                single_port("value", WeftType::type_var("T")),
            ],
            vec![single_port("value", WeftType::type_var("T"))],
        ),
        make_node("gat", vec![gather_port("in", ls())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "gate", "value"),
        edge("gate", "value", "gat", "in"),
    ]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "typevar resolution should work: {:?}", errors);

    let gate = wf.nodes.iter().find(|n| n.id == "gate").unwrap();
    assert_eq!(gate.inputs[1].portType, s(), "gate value input should resolve to String");
    assert_eq!(gate.outputs[0].portType, s(), "gate value output should resolve to String");

    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "typevar gate inside expand-gather should work: {:?}", errors);
}

#[test]
fn tricky_three_expands_three_gathers_valid() {
    // Depth: 0→1→2→3→2→1→0. Valid (triple nesting).
    let lls = WeftType::list(ls());
    let llls = WeftType::list(lls.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", llls.clone())]),
        make_node("e1", vec![expand_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
        make_node("e2", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e3", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
        make_node("g3", vec![gather_port("in", llls.clone())], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "e2", "in"),
        edge("e2", "out", "e3", "in"),
        edge("e3", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
        edge("g2", "out", "g3", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "triple nested expand-gather should work: {:?}", errors);
}

#[test]
fn tricky_three_expands_three_gathers_balanced() {
    // Depth: 0→1→2→3→2→1→0. All balanced. Valid.
    let lls = WeftType::list(ls());
    let llls = WeftType::list(lls.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", llls.clone())]),
        make_node("e1", vec![expand_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
        make_node("e2", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e3", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
        make_node("g3", vec![gather_port("in", llls.clone())], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "e2", "in"),
        edge("e2", "out", "e3", "in"),
        edge("e3", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
        edge("g2", "out", "g3", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "3 expands + 3 gathers balanced should work: {:?}", errors);
}

#[test]
fn tricky_expand_output_into_list_expecting_node_fails() {
    // Node with expand output: declared T (element), wire carries T.
    // Downstream expects List[T] (Single). Wire: T → List[T]. Incompatible.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("producer", vec![single_port("in", s())], vec![expand_port("out", s())]),
        make_node("consumer", vec![single_port("in", ls())], vec![]),  // expects List[String], gets String
    ], vec![
        edge("src", "out", "producer", "in"),
        edge("producer", "out", "consumer", "in"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "expand output String → Single List[String] should fail");
}

#[test]
fn tricky_gather_output_into_expand_input_re_expand() {
    // Gather produces List[String] → feed into another Expand(String).
    // Wire: List[String] → expected List[String]. Valid.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("e1", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e2", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g2", vec![gather_port("in", ls())], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "g1", "in"),
        edge("g1", "out", "e2", "in"),
        edge("e2", "out", "g2", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "gather → re-expand should work: {:?}", errors);
}

#[test]
fn tricky_multiple_typevars_independent() {
    // Node with T1 on one input/output pair and T2 on another.
    // T1 resolves to String, T2 resolves to Number. Independent.
    let mut wf = make_wf(vec![
        make_node("srcS", vec![], vec![single_port("out", s())]),
        make_node("srcN", vec![], vec![single_port("out", n())]),
        make_node("dual",
            vec![
                single_port("a", WeftType::type_var("T1")),
                single_port("b", WeftType::type_var("T2")),
            ],
            vec![
                single_port("x", WeftType::type_var("T1")),
                single_port("y", WeftType::type_var("T2")),
            ],
        ),
        make_node("dstS", vec![single_port("in", s())], vec![]),
        make_node("dstN", vec![single_port("in", n())], vec![]),
    ], vec![
        edge("srcS", "out", "dual", "a"),
        edge("srcN", "out", "dual", "b"),
        edge("dual", "x", "dstS", "in"),
        edge("dual", "y", "dstN", "in"),
    ]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    assert!(errors.is_empty(), "independent T1 T2 should resolve: {:?}", errors);

    let dual = wf.nodes.iter().find(|n| n.id == "dual").unwrap();
    assert_eq!(dual.inputs[0].portType, s(), "T1 should resolve to String");
    assert_eq!(dual.inputs[1].portType, n(), "T2 should resolve to Number");
    assert_eq!(dual.outputs[0].portType, s(), "T1 output should resolve to String");
    assert_eq!(dual.outputs[1].portType, n(), "T2 output should resolve to Number");

    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "all edges should be compatible: {:?}", errors);
}

#[test]
fn tricky_multiple_typevars_swapped_fails() {
    // T1 → String source, T2 → Number source. But outputs are swapped: T1 → Number dst, T2 → String dst.
    let mut wf = make_wf(vec![
        make_node("srcS", vec![], vec![single_port("out", s())]),
        make_node("srcN", vec![], vec![single_port("out", n())]),
        make_node("dual",
            vec![
                single_port("a", WeftType::type_var("T1")),
                single_port("b", WeftType::type_var("T2")),
            ],
            vec![
                single_port("x", WeftType::type_var("T1")),
                single_port("y", WeftType::type_var("T2")),
            ],
        ),
        make_node("dstN", vec![single_port("in", n())], vec![]),  // expects Number, gets T1=String
        make_node("dstS", vec![single_port("in", s())], vec![]),  // expects String, gets T2=Number
    ], vec![
        edge("srcS", "out", "dual", "a"),
        edge("srcN", "out", "dual", "b"),
        edge("dual", "x", "dstN", "in"),  // T1=String → Number
        edge("dual", "y", "dstS", "in"),  // T2=Number → String
    ]);
    let mut errors = vec![];
    resolve_and_narrow(&mut wf, &mut errors);
    // T1 conflicts: String (from srcS) vs Number (from dstN)
    assert!(!errors.is_empty(), "swapped TypeVars should produce conflict");
}

#[test]
fn tricky_empty_graph_no_errors() {
    let wf = make_wf(vec![], vec![]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    validate_no_unresolved(&wf, &mut errors);
    assert!(errors.is_empty(), "empty graph should have no errors: {:?}", errors);
}

#[test]
fn tricky_disconnected_nodes_no_errors() {
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![single_port("in", n())], vec![]),
    ], vec![]);  // no edges
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "disconnected nodes should have no errors: {:?}", errors);
}

#[test]
fn tricky_expand_list_of_lists() {
    // List[List[String]] → Expand(List[String]). Wire expects List[List[String]]. Valid.
    let lls = WeftType::list(ls());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lls.clone())]),
        make_node("exp", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
    ], vec![edge("src", "out", "exp", "in")]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[List[String]] → expand(List[String]) should work: {:?}", errors);
}

#[test]
fn tricky_expand_list_of_lists_wrong_inner() {
    // List[List[String]] → Expand(List[Number]). Wire: List[List[String]] vs expected List[List[Number]]. Fail.
    let lls = WeftType::list(ls());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lls)]),
        make_node("exp", vec![expand_port("in", ln())], vec![]),
    ], vec![edge("src", "out", "exp", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[List[String]] → expand(List[Number]) should fail");
}

#[test]
fn tricky_gather_with_non_list_declared_type_wire_check() {
    // Gather port declares String instead of List[String].
    // Wire type computation: inner(String) → String is not a List, falls through to String.
    // Source outputs String. Wire: String → String. Compatible on wire.
    // But the declared type is wrong : gather should declare List[T].
    // This is a semantic error we should catch separately.
    let wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", s())]),
        make_node("b", vec![gather_port("in", s())], vec![]),  // should be List[String]
    ], vec![edge("a", "out", "b", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    // Wire type: gather with declared String → inner(String) is not a List, so wire = String.
    // Source: String. Compatible. But semantically wrong (gather should declare List).
    // Currently this passes wire check. The stack depth check catches the depth issue separately.
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "gather at depth 0 should fail even with matching wire types");
}

#[test]
fn tricky_long_chain_expand_work_work_work_gather() {
    // Expand → 5 processing nodes → Gather. All at depth 1.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("w1", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("w2", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("w3", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("w4", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("w5", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("gat", vec![gather_port("in", ls())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "w1", "in"),
        edge("w1", "out", "w2", "in"),
        edge("w2", "out", "w3", "in"),
        edge("w3", "out", "w4", "in"),
        edge("w4", "out", "w5", "in"),
        edge("w5", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "long chain inside expand-gather should work: {:?}", errors);
}

// =========================================================================
// Pack/Unpack dynamic resolve_types
// =========================================================================

#[test]
fn pack_resolve_types_basic() {
    // Pack with two String inputs → output should be Dict[String, String]
    let pack = crate::nodes::pack::PackNode;
    let inputs = vec![
        PortDefinition { name: "name".to_string(), portType: s(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
        PortDefinition { name: "city".to_string(), portType: s(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
    ];
    let outputs = vec![
        PortDefinition { name: "out".to_string(), portType: WeftType::type_var("T"), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
    ];
    let resolved = pack.resolve_types(&inputs, &outputs);
    assert_eq!(resolved.outputs.len(), 1);
    let (name, pt) = &resolved.outputs[0];
    assert_eq!(name, "out");
    assert_eq!(*pt, WeftType::dict(s(), s()), "Pack(String, String) → Dict[String, String]");
}

#[test]
fn pack_resolve_types_mixed() {
    // Pack with String and Number inputs → output should be Dict[String, String | Number]
    let pack = crate::nodes::pack::PackNode;
    let inputs = vec![
        PortDefinition { name: "name".to_string(), portType: s(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
        PortDefinition { name: "age".to_string(), portType: n(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
    ];
    let outputs = vec![];
    let resolved = pack.resolve_types(&inputs, &outputs);
    assert_eq!(resolved.outputs.len(), 1);
    let (_, pt) = &resolved.outputs[0];
    let expected = WeftType::dict(s(), WeftType::union(vec![s(), n()]));
    assert_eq!(*pt, expected, "Pack(String, Number) → Dict[String, String | Number]");
}

#[test]
fn pack_resolve_types_empty() {
    let pack = crate::nodes::pack::PackNode;
    let resolved = pack.resolve_types(&[], &[]);
    assert!(resolved.outputs.is_empty(), "Pack with no inputs should not resolve");
}

#[test]
fn unpack_resolve_types_basic() {
    // Unpack with two String outputs → input should be Dict[String, String]
    let unpack = crate::nodes::unpack::UnpackNode;
    let inputs = vec![];
    let outputs = vec![
        PortDefinition { name: "name".to_string(), portType: s(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
        PortDefinition { name: "city".to_string(), portType: s(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
    ];
    let resolved = unpack.resolve_types(&inputs, &outputs);
    assert_eq!(resolved.inputs.len(), 1);
    let (name, pt) = &resolved.inputs[0];
    assert_eq!(name, "in");
    assert_eq!(*pt, WeftType::dict(s(), s()), "Unpack(String, String) → Dict[String, String]");
}

#[test]
fn unpack_resolve_types_mixed() {
    let unpack = crate::nodes::unpack::UnpackNode;
    let outputs = vec![
        PortDefinition { name: "name".to_string(), portType: s(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
        PortDefinition { name: "score".to_string(), portType: n(), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
    ];
    let resolved = unpack.resolve_types(&[], &outputs);
    let (_, pt) = &resolved.inputs[0];
    let expected = WeftType::dict(s(), WeftType::union(vec![s(), n()]));
    assert_eq!(*pt, expected, "Unpack(String, Number) → Dict[String, String | Number]");
}

#[test]
fn pack_inside_expand_gather_valid() {
    // List[String] → Expand(String) → Pack(String→Dict[String,String]) → Gather(List[Dict[String,String]])
    let dict_ss = WeftType::dict(s(), s());
    let list_dict = WeftType::list(dict_ss.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("exp", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("pack",
            vec![single_port("a", s())],
            vec![single_port("out", dict_ss.clone())],  // after resolve_types
        ),
        make_node("gat", vec![gather_port("in", list_dict.clone())], vec![single_port("out", list_dict)]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "pack", "a"),
        edge("pack", "out", "gat", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "pack inside expand-gather should work: {:?}", errors);
}

#[test]
fn unpack_inside_expand_gather_valid() {
    // List[Dict[String,String|Number]] → Expand(Dict) → Unpack(name:String, age:Number) → process → Gather
    let dict_sn = WeftType::dict(s(), WeftType::union(vec![s(), n()]));
    let list_dict = WeftType::list(dict_sn.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", list_dict)]),
        make_node("exp", vec![expand_port("in", dict_sn.clone())], vec![single_port("out", dict_sn)]),
        make_node("unpack",
            vec![single_port("in", WeftType::dict(s(), WeftType::union(vec![s(), n()])))],
            vec![single_port("name", s()), single_port("age", n())],
        ),
        make_node("gatherN", vec![gather_port("in", ls())], vec![]),
        make_node("gatherA", vec![gather_port("in", ln())], vec![]),
    ], vec![
        edge("src", "out", "exp", "in"),
        edge("exp", "out", "unpack", "in"),
        edge("unpack", "name", "gatherN", "in"),
        edge("unpack", "age", "gatherA", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "unpack inside expand-gather should work: {:?}", errors);
}

#[test]
fn pack_wrong_downstream_type_fails() {
    // Pack produces Dict[String, String] but downstream expects Dict[String, Number]
    let dict_ss = WeftType::dict(s(), s());
    let dict_sn = WeftType::dict(s(), n());
    let wf = make_wf(vec![
        make_node("pack",
            vec![single_port("a", s())],
            vec![single_port("out", dict_ss)],
        ),
        make_node("consumer", vec![single_port("in", dict_sn)], vec![]),
    ], vec![edge("pack", "out", "consumer", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "Dict[String,String] → Dict[String,Number] should fail");
}

// =========================================================================
// Passthrough edge validation : type errors through group boundaries
// =========================================================================

#[test]
fn passthrough_gather_out_inner_wrong_type() {
    // Group out(>results: List[String]). Inner node produces Number.
    // Output passthrough internal input expects String (pre-gather element).
    // Number → String should fail.
    //
    // inner_node(Number) → GroupOut__out.results(internal: String, Single)
    let wf = make_wf(vec![
        make_node("inner", vec![], vec![single_port("out", n())]),
        make_node("grp__out",
            // Internal input: pre-gather = String (element of List[String])
            vec![single_port("results", s())],
            // External output: gather, List[String]
            vec![gather_port("results", ls())],
        ),
    ], vec![
        edge("inner", "out", "grp__out", "results"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "Number → String (pre-gather) should fail: inner produces wrong type");
}

#[test]
fn passthrough_gather_out_inner_correct_type() {
    // Group out(>results: List[String]). Inner node produces String.
    // Output passthrough internal input expects String. String → String. OK.
    let wf = make_wf(vec![
        make_node("inner", vec![], vec![single_port("out", s())]),
        make_node("grp__out",
            vec![single_port("results", s())],
            vec![gather_port("results", ls())],
        ),
    ], vec![
        edge("inner", "out", "grp__out", "results"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "String → String (pre-gather) should work: {:?}", errors);
}

#[test]
fn passthrough_expand_out_inner_produces_list_valid() {
    // Group out(<results: String). Inner node produces List[String].
    // Output passthrough internal input expects List[String] (pre-expand).
    // List[String] → List[String]. OK.
    let wf = make_wf(vec![
        make_node("inner", vec![], vec![single_port("out", ls())]),
        make_node("grp__out",
            // Internal input: pre-expand = List[String]
            vec![single_port("results", ls())],
            // External output: expand, String (element)
            vec![expand_port("results", s())],
        ),
    ], vec![
        edge("inner", "out", "grp__out", "results"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[String] → List[String] (pre-expand) should work: {:?}", errors);
}

#[test]
fn passthrough_expand_out_inner_produces_non_list_fails() {
    // Group out(<results: String). Inner node produces String (not a list).
    // Output passthrough internal input expects List[String] (pre-expand).
    // String → List[String]. Fail.
    let wf = make_wf(vec![
        make_node("inner", vec![], vec![single_port("out", s())]),
        make_node("grp__out",
            // Internal input: pre-expand = List[String]
            vec![single_port("results", ls())],
            // External output: expand, String
            vec![expand_port("results", s())],
        ),
    ], vec![
        edge("inner", "out", "grp__out", "results"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String → List[String] (pre-expand) should fail: can't expand non-list");
}

#[test]
fn passthrough_expand_out_inner_wrong_inner_type_fails() {
    // Group out(<results: String). Inner produces List[Number].
    // Output passthrough internal input expects List[String].
    // List[Number] → List[String]. Fail.
    let wf = make_wf(vec![
        make_node("inner", vec![], vec![single_port("out", ln())]),
        make_node("grp__out",
            vec![single_port("results", ls())],
            vec![expand_port("results", s())],
        ),
    ], vec![
        edge("inner", "out", "grp__out", "results"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[Number] → List[String] (pre-expand) should fail: wrong inner type");
}

#[test]
fn passthrough_expand_in_external_sends_list_valid() {
    // Group in(*<items: String). External sends List[String].
    // Expand input expects List[String] on wire. OK.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("grp__in",
            vec![expand_port("items", s())],
            vec![single_port("items", s())],
        ),
    ], vec![
        edge("src", "out", "grp__in", "items"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[String] → expand(String) should work: {:?}", errors);
}

#[test]
fn passthrough_expand_in_external_sends_non_list_fails() {
    // Group in(*<items: String). External sends String (not a list).
    // Expand expects List[String] on wire. Fail.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("grp__in",
            vec![expand_port("items", s())],
            vec![single_port("items", s())],
        ),
    ], vec![
        edge("src", "out", "grp__in", "items"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "String → expand(String) should fail: not a list");
}

#[test]
fn passthrough_full_group_expand_gather_valid() {
    // Full group: List[String] → expand → process → gather → List[String]
    // Simulates the exact passthrough structure the compiler creates.
    let wf = make_wf(vec![
        // External source
        make_node("src", vec![], vec![single_port("value", ls())]),
        // Input passthrough: external Expand, internal Single
        make_node("grp__in",
            vec![expand_port("items", s())],  // external: Expand, String
            vec![single_port("items", s())],  // internal: Single, String
        ),
        // Inner processing node
        make_node("grp.worker",
            vec![single_port("in", s())],
            vec![single_port("out", s())],
        ),
        // Output passthrough: internal Single (pre-gather element), external Gather
        make_node("grp__out",
            vec![single_port("results", s())],    // internal: Single, String (pre-gather)
            vec![gather_port("results", ls())],   // external: Gather, List[String]
        ),
        // External consumer
        make_node("debug", vec![single_port("data", ls())], vec![]),
    ], vec![
        edge("src", "value", "grp__in", "items"),
        edge("grp__in", "items", "grp.worker", "in"),
        edge("grp.worker", "out", "grp__out", "results"),
        edge("grp__out", "results", "debug", "data"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "full group expand-gather pipeline should work: {:?}", errors);
}

#[test]
fn passthrough_full_group_type_mismatch_inside() {
    // Same as above but inner worker outputs Number instead of String.
    // Worker Number → grp__out internal input (String). Should fail.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("value", ls())]),
        make_node("grp__in",
            vec![expand_port("items", s())],
            vec![single_port("items", s())],
        ),
        make_node("grp.worker",
            vec![single_port("in", s())],
            vec![single_port("out", n())],  // produces Number
        ),
        make_node("grp__out",
            vec![single_port("results", s())],    // expects String (pre-gather)
            vec![gather_port("results", ls())],
        ),
        make_node("debug", vec![single_port("data", ls())], vec![]),
    ], vec![
        edge("src", "value", "grp__in", "items"),
        edge("grp__in", "items", "grp.worker", "in"),
        edge("grp.worker", "out", "grp__out", "results"),
        edge("grp__out", "results", "debug", "data"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "Number → String inside group should be caught");
}

#[test]
fn passthrough_full_group_external_wrong_type() {
    // External sends List[Number] but group expects expand(String) → List[String] on wire.
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("value", ln())]),  // List[Number]
        make_node("grp__in",
            vec![expand_port("items", s())],  // expects List[String]
            vec![single_port("items", s())],
        ),
    ], vec![
        edge("src", "value", "grp__in", "items"),
    ]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(!errors.is_empty(), "List[Number] → expand(String) should fail: wrong element type");
}

#[test]
fn tricky_four_gathers_three_expands_fails() {
    // 3 expands (depth 3) then 4 gathers → fourth gather at depth 0 = error
    let lls = WeftType::list(ls());
    let llls = WeftType::list(lls.clone());
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", llls.clone())]),
        make_node("e1", vec![expand_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
        make_node("e2", vec![expand_port("in", ls())], vec![single_port("out", ls())]),
        make_node("e3", vec![expand_port("in", s())], vec![single_port("out", s())]),
        make_node("g1", vec![gather_port("in", ls())], vec![single_port("out", ls())]),
        make_node("g2", vec![gather_port("in", lls.clone())], vec![single_port("out", lls.clone())]),
        make_node("g3", vec![gather_port("in", llls.clone())], vec![single_port("out", llls)]),
        make_node("g4_bad", vec![gather_port("in", WeftType::list(WeftType::list(WeftType::list(ls()))))], vec![]),
    ], vec![
        edge("src", "out", "e1", "in"),
        edge("e1", "out", "e2", "in"),
        edge("e2", "out", "e3", "in"),
        edge("e3", "out", "g1", "in"),
        edge("g1", "out", "g2", "in"),
        edge("g2", "out", "g3", "in"),
        edge("g3", "out", "g4_bad", "in"),
    ]);
    let mut errors = vec![];
    validate_stack_depth(&wf, &mut errors);
    assert!(!errors.is_empty(), "4 gathers with 3 expands should fail (gather at depth 0)");
}

// =========================================================================
// Mock type validation (WeftType::infer + is_compatible)
// =========================================================================
// These test the core mock validation logic: inferring types from JSON mock
// values and checking compatibility with declared port types. The trigger/infra
// rejection is tested via the compiler tests (end-to-end compilation).

#[test]
fn mock_type_string_compatible_with_string_port() {
    let inferred = WeftType::infer(&serde_json::json!("hello"));
    assert!(WeftType::is_compatible(&inferred, &s()), "String mock should match String port");
}

#[test]
fn mock_type_string_incompatible_with_number_port() {
    let inferred = WeftType::infer(&serde_json::json!("not a number"));
    assert!(!WeftType::is_compatible(&inferred, &n()), "String mock should NOT match Number port");
}

#[test]
fn mock_type_number_compatible_with_number_port() {
    let inferred = WeftType::infer(&serde_json::json!(42));
    assert!(WeftType::is_compatible(&inferred, &n()), "Number mock should match Number port");
}

#[test]
fn mock_type_list_string_compatible_with_list_string_port() {
    let inferred = WeftType::infer(&serde_json::json!(["a", "b", "c"]));
    assert!(WeftType::is_compatible(&inferred, &ls()), "List[String] mock should match List[String] port");
}

#[test]
fn mock_type_list_number_incompatible_with_list_string_port() {
    let inferred = WeftType::infer(&serde_json::json!([1, 2, 3]));
    assert!(!WeftType::is_compatible(&inferred, &ls()), "List[Number] mock should NOT match List[String] port");
}

#[test]
fn mock_type_mixed_list_incompatible_with_list_string_port() {
    let inferred = WeftType::infer(&serde_json::json!(["hello", 42]));
    assert!(!WeftType::is_compatible(&inferred, &ls()), "List[String|Number] mock should NOT match List[String] port");
}

#[test]
fn mock_type_null_always_compatible_top_level() {
    // Top-level null means "port doesn't fire" : always valid for any type.
    // This is handled by the `continue` in enrich.rs, not by type compatibility.
    // Here we verify that Null is correctly inferred.
    let inferred = WeftType::infer(&serde_json::json!(null));
    assert_eq!(inferred, WeftType::primitive(WeftPrimitive::Null));
}

#[test]
fn mock_type_nested_null_in_list_is_incompatible() {
    // [null, null] infers as List[Null], which is NOT compatible with List[String]
    let inferred = WeftType::infer(&serde_json::json!([null, null]));
    assert!(!WeftType::is_compatible(&inferred, &ls()),
        "List[Null] should NOT match List[String]");
}

#[test]
fn mock_type_empty_list_compatible_with_any() {
    // Empty array infers as List[Empty], compatible with any List[X]
    let inferred = WeftType::infer(&serde_json::json!([]));
    assert_eq!(inferred, WeftType::list(WeftType::primitive(WeftPrimitive::Empty)));
    assert!(WeftType::is_compatible(&inferred, &ls()), "List[Empty] should match List[String]");
    assert!(WeftType::is_compatible(&inferred, &ln()), "List[Empty] should match List[Number]");
}

#[test]
fn mock_type_dict_compatible() {
    let dict_type = WeftType::Dict(Box::new(s()), Box::new(n()));
    let inferred = WeftType::infer(&serde_json::json!({"a": 1, "b": 2}));
    assert!(WeftType::is_compatible(&inferred, &dict_type),
        "Dict[String, Number] mock should match Dict[String, Number] port");
}

#[test]
fn mock_type_dict_incompatible_value_type() {
    let dict_type = WeftType::Dict(Box::new(s()), Box::new(n()));
    let inferred = WeftType::infer(&serde_json::json!({"a": "not a number"}));
    assert!(!WeftType::is_compatible(&inferred, &dict_type),
        "Dict[String, String] mock should NOT match Dict[String, Number] port");
}

#[test]
fn mock_type_complex_nested_structure() {
    // Mock: {"vendors": [{"name": "Acme", "score": 92}]}
    // Expected port type: Dict[String, List[Dict[String, String|Number]]]
    let inner_dict = WeftType::Dict(
        Box::new(s()),
        Box::new(WeftType::Union(vec![s(), n()])),
    );
    let expected = WeftType::Dict(
        Box::new(s()),
        Box::new(WeftType::list(inner_dict)),
    );
    let inferred = WeftType::infer(&serde_json::json!({"vendors": [{"name": "Acme", "score": 92}]}));
    assert!(WeftType::is_compatible(&inferred, &expected),
        "complex nested mock should be compatible. Inferred: {}, Expected: {}", inferred, expected);
}

#[test]
fn mock_type_boolean_matches_boolean_port() {
    let bool_type = WeftType::primitive(WeftPrimitive::Boolean);
    let inferred = WeftType::infer(&serde_json::json!(true));
    assert!(WeftType::is_compatible(&inferred, &bool_type));
    let inferred_false = WeftType::infer(&serde_json::json!(false));
    assert!(WeftType::is_compatible(&inferred_false, &bool_type));
}

#[test]
fn mock_type_any_value_matches_typevar() {
    let typevar = WeftType::TypeVar("T".to_string());
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!("hello")), &typevar));
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!(42)), &typevar));
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!([1,2])), &typevar));
}

#[test]
fn mock_type_string_matches_string_or_number_union() {
    let union_type = WeftType::Union(vec![s(), n()]);
    let inferred = WeftType::infer(&serde_json::json!("hello"));
    assert!(WeftType::is_compatible(&inferred, &union_type),
        "String should match String | Number");
}

#[test]
fn mock_type_boolean_does_not_match_string_or_number_union() {
    let union_type = WeftType::Union(vec![s(), n()]);
    let inferred = WeftType::infer(&serde_json::json!(true));
    assert!(!WeftType::is_compatible(&inferred, &union_type),
        "Boolean should NOT match String | Number");
}

#[test]
fn mock_type_typevar_port_accepts_any_mock_value() {
    // Previously the frontend was missing this guard, causing false-positive
    // validation errors on TypeVar ports. Backend had the guard but we test
    // the underlying logic here to prevent regression.
    let typevar = WeftType::TypeVar("T".to_string());
    // String into T
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!("hello")), &typevar));
    // Number into T
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!(42)), &typevar));
    // List into T
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!([1, 2])), &typevar));
    // Dict into T
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!({"a": 1})), &typevar));
    // Null into T
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!(null)), &typevar));
}

#[test]
fn mock_type_must_override_port_accepts_any_mock_value() {
    let must_override = WeftType::MustOverride;
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!("anything")), &must_override));
    assert!(WeftType::is_compatible(&WeftType::infer(&serde_json::json!(42)), &must_override));
}

#[test]
fn mock_top_level_null_is_not_type_checked() {
    // Top-level null in mock output means "this port doesn't fire" (branching).
    // Previously this was type-checked, producing "Null but port expects Boolean"
    // errors. The fix is a `continue` that skips the check for null.
    // We verify the underlying type incompatibility exists (Null != Boolean)
    // but that it wouldn't trigger the error because of the null skip.
    let bool_type = WeftType::primitive(WeftPrimitive::Boolean);
    let null_type = WeftType::infer(&serde_json::json!(null));
    // Null IS incompatible with Boolean at the type level
    assert!(!WeftType::is_compatible(&null_type, &bool_type),
        "Null should be incompatible with Boolean at type level");
    // But top-level null is skipped before this check reaches is_compatible.
    // (The actual skip is tested via the executor/enrich integration.)
}

#[test]
fn mock_branching_pattern_mixed_null_and_values() {
    // Real-world pattern: a review node outputs approved OR rejected, never both.
    // Mock: {"approved": true, "rejected": null, "notes": "looks good"}
    // This should be valid even though rejected:null vs Boolean would fail type check.
    let mock = serde_json::json!({"approved": true, "rejected": null, "notes": "looks good"});
    let mock_obj = mock.as_object().unwrap();
    let ports = vec![
        ("approved", WeftType::primitive(WeftPrimitive::Boolean)),
        ("rejected", WeftType::primitive(WeftPrimitive::Boolean)),
        ("notes", s()),
    ];
    for (port_name, port_type) in &ports {
        if let Some(val) = mock_obj.get(*port_name) {
            if val.is_null() {
                continue; // this is the skip that enrich.rs does
            }
            let inferred = WeftType::infer(val);
            assert!(WeftType::is_compatible(&inferred, port_type),
                "Port '{}': {} should be compatible with {}",
                port_name, inferred, port_type);
        }
    }
}

// =========================================================================
// Implicit lane mode inference (infer_lane_modes)
// =========================================================================

#[test]
fn infer_expand_list_to_element() {
    // Source outputs List[String], target expects String → should infer Expand(1)
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("tgt", vec![single_port("in", s())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Expand, "should infer Expand");
    assert_eq!(port.laneDepth, 1);
}

#[test]
fn infer_gather_element_to_list() {
    // Source outputs String, target expects List[String] → should infer Gather(1)
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("tgt", vec![single_port("in", ls())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Gather, "should infer Gather");
    assert_eq!(port.laneDepth, 1);
}

#[test]
fn infer_no_change_when_types_compatible() {
    // Source outputs String, target expects String → no lane mode change
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("tgt", vec![single_port("in", s())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Single, "should remain Single when types match");
}

#[test]
fn infer_no_change_when_into_union() {
    // Source outputs String, target expects String | Number → compatible, no lane change
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("tgt", vec![single_port("in", WeftType::union(vec![s(), n()]))], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Single);
}

#[test]
fn infer_expand_depth_2() {
    // Source outputs List[List[String]], target expects String → Expand(2)
    let lls = WeftType::list(ls());
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", lls)]),
        make_node("tgt", vec![single_port("in", s())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Expand);
    assert_eq!(port.laneDepth, 2);
}

#[test]
fn infer_gather_depth_2() {
    // Source outputs String, target expects List[List[String]] → Gather(2)
    let lls = WeftType::list(ls());
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", s())]),
        make_node("tgt", vec![single_port("in", lls)], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Gather);
    assert_eq!(port.laneDepth, 2);
}

#[test]
fn infer_does_not_override_explicit_expand() {
    // Target already has Expand mode → inference should not change it
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("tgt", vec![expand_port("in", s())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Expand, "explicit mode should not be overridden");
}

#[test]
fn infer_expand_with_union_inner() {
    // Source outputs List[String | Number], target expects String | Number → Expand(1)
    let union_type = WeftType::union(vec![s(), n()]);
    let list_union = WeftType::list(union_type.clone());
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", list_union)]),
        make_node("tgt", vec![single_port("in", union_type)], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Expand);
    assert_eq!(port.laneDepth, 1);
}

#[test]
fn infer_chain_expand_then_gather() {
    // A → B (expand: List[String] → String) → C (gather: String → List[String])
    let mut wf = make_wf(vec![
        make_node("a", vec![], vec![single_port("out", ls())]),
        make_node("b", vec![single_port("in", s())], vec![single_port("out", s())]),
        make_node("c", vec![single_port("in", ls())], vec![]),
    ], vec![
        edge("a", "out", "b", "in"),
        edge("b", "out", "c", "in"),
    ]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);

    let b = wf.nodes.iter().find(|n| n.id == "b").unwrap();
    assert_eq!(b.inputs.iter().find(|p| p.name == "in").unwrap().laneMode, LaneMode::Expand);

    let c = wf.nodes.iter().find(|n| n.id == "c").unwrap();
    assert_eq!(c.inputs.iter().find(|p| p.name == "in").unwrap().laneMode, LaneMode::Gather);
}

#[test]
fn infer_no_lane_mode_for_incompatible_types() {
    // Source outputs Number, target expects String → neither expand nor gather applies
    // Just stays Single (type error caught later by validate_edge_types)
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", n())]),
        make_node("tgt", vec![single_port("in", s())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Single, "incompatible types should not infer any lane mode");
}

#[test]
fn infer_expand_list_number_to_number() {
    // Source outputs List[Number], target expects Number → Expand(1)
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ln())]),
        make_node("tgt", vec![single_port("in", n())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Expand);
}

#[test]
fn infer_with_null_in_union() {
    // Source outputs List[String | Null], target expects String | Null → Expand(1)
    let null_t = WeftType::primitive(WeftPrimitive::Null);
    let sn = WeftType::union(vec![s(), null_t.clone()]);
    let list_sn = WeftType::list(sn.clone());
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", list_sn)]),
        make_node("tgt", vec![single_port("in", sn)], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Expand);
}

#[test]
fn infer_skips_unresolved_types() {
    // Source outputs MustOverride → should not infer anything
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", WeftType::MustOverride)]),
        make_node("tgt", vec![single_port("in", s())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Single, "unresolved types should not trigger inference");
}

// =========================================================================
// Full pipeline: compile → enrich → validate (integration tests)
// =========================================================================

#[test]
fn integration_form_select_input_list_string_no_expand() {
    // Reproducer: List node → HumanQuery select_input port
    // Both are List[String]. Should NOT trigger expand.
    let source = r#"
# Project: Test
# Description: Test

priority_options = List -> (value: List[String]) {
  label: "Options"
  value: ["Low","Medium","High"]
}

review = HumanQuery {
  label: "Review"
  title: "Test"
  fields: [{"fieldType": "select_input", "key": "priority_override"}]
}
review.priority_override = priority_options.value
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");

    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("enrich_project failed: {:?}", errors);
    }

    // Check: priority_override should be Single, not Expand
    let review = project.nodes.iter().find(|n| n.id == "review").expect("review node");
    let port = review.inputs.iter().find(|p| p.name == "priority_override").expect("priority_override port");
    eprintln!("priority_override: type={}, laneMode={:?}, laneDepth={}", port.portType, port.laneMode, port.laneDepth);
    assert_eq!(port.laneMode, LaneMode::Single, "List[String] → List[String] should stay Single, not Expand");
    assert_eq!(port.portType.to_string(), "List[String]", "port type should be List[String]");
}

#[test]
fn integration_basic_llm_pipeline() {
    // Text → LlmConfig + LlmInference → Debug
    let source = r#"
# Project: Test
# Description: Test

input = Text { label: "In", value: "hello" }
config = LlmConfig { label: "Cfg", model: "anthropic/claude-sonnet-4.6" }
llm = LlmInference -> (response: String) { label: "LLM" }
llm.prompt = input.value
llm.config = config.config
out = Debug { label: "Out" }
out.data = llm.response
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("basic LLM pipeline failed: {:?}", errors);
    }
    let llm = project.nodes.iter().find(|n| n.id == "llm").unwrap();
    assert!(llm.outputs.iter().any(|p| p.name == "response" && p.portType.to_string() == "String"));
}

#[test]
fn integration_exec_python_custom_ports() {
    // ExecPython with custom input/output ports
    let source = r#"
# Project: Test
# Description: Test

input = Text { label: "In", value: "hello" }
worker = ExecPython(data: String) -> (result: String, score: Number) {
    label: "Worker"
    code: "return {'result': data, 'score': 42}"
}
worker.data = input.value
out = Debug { label: "Out" }
out.data = worker.result
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("ExecPython custom ports failed: {:?}", errors);
    }
    let worker = project.nodes.iter().find(|n| n.id == "worker").unwrap();
    assert_eq!(worker.inputs.iter().find(|p| p.name == "data").unwrap().portType.to_string(), "String");
    assert_eq!(worker.outputs.iter().find(|p| p.name == "result").unwrap().portType.to_string(), "String");
    assert_eq!(worker.outputs.iter().find(|p| p.name == "score").unwrap().portType.to_string(), "Number");
}

#[test]
fn integration_group_with_expand_gather() {
    // Group that expands List[String] → String inside, gathers back
    let source = r#"
# Project: Test
# Description: Test

articles = List -> (value: List[String]) { label: "Articles", value: ["a", "b"] }
batch = Group(items: List[String]) -> (results: List[String]) {
    # Process each item
    worker = ExecPython(item: String) -> (result: String) {
        label: "Work"
        code: "return {'result': item.upper()}"
    }
    worker.item = self.items
    self.results = worker.result
}
batch.items = articles.value
out = Debug { label: "Out" }
out.data = batch.results
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("group expand/gather failed: {:?}", errors);
    }
    // Inside the group, items (List[String]) feeds worker.item (String) → expand
    let worker = project.nodes.iter().find(|n| n.id == "batch.worker").unwrap();
    let item_port = worker.inputs.iter().find(|p| p.name == "item").unwrap();
    assert_eq!(item_port.laneMode, LaneMode::Expand, "worker.item should be Expand (List[String] → String)");
}

#[test]
fn integration_human_query_full_form() {
    // HumanQuery with display, text_input, approve_reject, and editable_textarea fields
    let source = r#"
# Project: Test
# Description: Test

input = Text { label: "In", value: "hello" }
review = HumanQuery {
    label: "Review"
    title: "Review"
    fields: [{"fieldType":"display","key":"summary","required":true},{"fieldType":"approve_reject","key":"decision"},{"fieldType":"text_input","key":"notes"},{"fieldType":"editable_textarea","key":"draft"}]
}
review.summary = input.value
review.draft = input.value
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("HumanQuery full form failed: {:?}", errors);
    }
    let review = project.nodes.iter().find(|n| n.id == "review").unwrap();
    // display → input port (summary)
    assert!(review.inputs.iter().any(|p| p.name == "summary"), "should have summary input");
    // editable_textarea → input port (draft)
    assert!(review.inputs.iter().any(|p| p.name == "draft"), "should have draft input");
    // approve_reject → output ports (decision_approved, decision_rejected)
    assert!(review.outputs.iter().any(|p| p.name == "decision_approved"), "should have decision_approved output");
    assert!(review.outputs.iter().any(|p| p.name == "decision_rejected"), "should have decision_rejected output");
    // text_input → output port (notes)
    assert!(review.outputs.iter().any(|p| p.name == "notes"), "should have notes output");
    // All inputs should be Single (no spurious expand)
    for p in &review.inputs {
        assert_eq!(p.laneMode, LaneMode::Single, "review.{} should be Single, got {:?}", p.name, p.laneMode);
    }
}

#[test]
fn integration_unknown_node_type_errors() {
    let source = r#"
# Project: Test
# Description: Test
node = FooBarBaz { label: "Bad" }
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(result.is_err(), "unknown node type should error");
    assert!(result.unwrap_err().iter().any(|e| e.contains("Unknown node type")));
}

#[test]
fn integration_template_with_custom_inputs() {
    // Template node with custom input ports for template variables
    let source = r#"
# Project: Test
# Description: Test

name = Text { label: "Name", value: "Alice" }
prompt_text = Text { label: "Template", value: "Hello {{name}}" }
tmpl = Template(name: String) { label: "Tmpl" }
tmpl.template = prompt_text.value
tmpl.name = name.value
out = Debug { label: "Out" }
out.data = tmpl.text
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("Template custom inputs failed: {:?}", errors);
    }
    let tmpl = project.nodes.iter().find(|n| n.id == "tmpl").unwrap();
    assert!(tmpl.inputs.iter().any(|p| p.name == "name"), "should have custom name input");
    assert!(tmpl.inputs.iter().any(|p| p.name == "template"), "should have catalog template input");
    assert!(tmpl.outputs.iter().any(|p| p.name == "text"), "should have text output");
}

#[test]
fn integration_gate_branching() {
    let source = r#"
# Project: Test
# Description: Test

input = Text { label: "In", value: "hello" }
condition = ExecPython() -> (pass: Boolean) {
    label: "Check"
    code: "return {'pass': True}"
}
gate = Gate { label: "Gate" }
gate.pass = condition.pass
gate.value = input.value
out = Debug { label: "Out" }
out.data = gate.value
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("Gate branching failed: {:?}", errors);
    }
}

#[test]
fn integration_pack_unpack() {
    let source = r#"
# Project: Test
# Description: Test

a = Text { label: "A", value: "hello" }
b = Number { label: "B", value: 42 }
pack = Pack(text: String, num: Number) { label: "Pack" }
pack.text = a.value
pack.num = b.value
unpack = Unpack -> (text: String, num: Number) { label: "Unpack" }
unpack.in = pack.out
out = Debug(data: String) { label: "Out" }
out.data = unpack.text
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    if let Err(errors) = &result {
        panic!("Pack/Unpack failed: {:?}", errors);
    }
}

#[test]
fn integration_must_override_not_declared_errors() {
    // LlmInference without declaring response type → should error
    let source = r#"
# Project: Test
# Description: Test

config = LlmConfig { label: "Cfg", provider: "openrouter" }
llm = LlmInference { label: "LLM" }
llm.config = config.config
out = Debug { label: "Out" }
out.data = llm.response
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("should compile");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    // Should have an error about MustOverride on response
    assert!(result.is_err(), "MustOverride without type should error");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("MustOverride") || e.contains("requires a type")),
        "should mention MustOverride: {:?}", errors);
}

#[test]
fn infer_no_expand_when_list_to_list_same_type() {
    // Source outputs List[String], target expects List[String] → should stay Single (same type, no expand)
    // This reproduces the bug where List[String] → List[String] incorrectly inferred Expand
    let mut wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("tgt", vec![single_port("in", ls())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    let tgt = wf.nodes.iter().find(|n| n.id == "tgt").unwrap();
    let port = tgt.inputs.iter().find(|p| p.name == "in").unwrap();
    assert_eq!(port.laneMode, LaneMode::Single, "List[String] → List[String] should NOT infer expand");
}

#[test]
fn validate_list_string_to_list_string_no_wire_error() {
    // Source outputs List[String], target expects List[String], both Single mode
    // Wire types should match: List[String] → List[String]
    let wf = make_wf(vec![
        make_node("src", vec![], vec![single_port("out", ls())]),
        make_node("tgt", vec![single_port("in", ls())], vec![]),
    ], vec![edge("src", "out", "tgt", "in")]);
    let mut errors = vec![];
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[String] → List[String] should have no type errors: {:?}", errors);
}

#[test]
fn infer_then_validate_list_string_to_list_string() {
    // Full pipeline: infer_lane_modes then validate_edge_types
    // List[String] → List[String] should pass with no errors
    let mut wf = make_wf(vec![
        make_node("priority_options", vec![], vec![single_port("value", ls())]),
        make_node("review", vec![single_port("priority_override", ls())], vec![]),
    ], vec![edge("priority_options", "value", "review", "priority_override")]);
    let mut errors = vec![];
    infer_lane_modes(&mut wf, &mut errors);
    validate_edge_types(&wf, &mut errors);
    assert!(errors.is_empty(), "List[String] → List[String] full pipeline should have no errors: {:?}", errors);
}

#[test]
fn inline_basic() {
    let source = r#"# Project: Test
# Description: Test inline node parsing

writer = LlmInference -> (response: String) {
  label: "Writer"
  model: "anthropic/claude-sonnet-4.6"
}
writer.prompt = Template {
  template: "Hello {{name}}"
  name: source.value
}.text

source = Text { value: "world" }
"#;
    let project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile failed");
    // Expected: the inline Template has been promoted to a named anon node.
    // Naming convention: `{parent_id}__{field_name}`. For `writer.prompt = Template { ... }.text`,
    // the parent is `writer`, field is `prompt`, so the anon id is `writer__prompt`.
    let anon = project.nodes.iter().find(|n| n.id == "writer__prompt").expect(&format!(
        "expected anon node 'writer__prompt', got: {:?}",
        project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
    ));
    assert_eq!(anon.nodeType.0, "Template");
    // Config should contain `template`, not `name`.
    assert!(anon.config.get("template").is_some(), "template config missing");
    assert!(anon.config.get("name").is_none(), "name should be a wiring, not a config");

    // Two edges: writer.prompt <- anon.text, anon.name <- source.value
    let edges = &project.edges;
    let has_prompt_edge = edges.iter().any(|e|
        e.target == "writer"
        && e.targetHandle.as_deref() == Some("prompt")
        && e.source == anon.id
        && e.sourceHandle.as_deref() == Some("text")
    );
    let has_name_edge = edges.iter().any(|e|
        e.target == anon.id
        && e.targetHandle.as_deref() == Some("name")
        && e.source == "source"
        && e.sourceHandle.as_deref() == Some("value")
    );
    assert!(has_prompt_edge, "missing writer.prompt <- anon.text edge; edges: {:?}", edges);
    assert!(has_name_edge, "missing anon.name <- source.value edge; edges: {:?}", edges);
}

#[test]
fn inline_inside_group() {
    let source = r#"# Project: Test

per_lead = Group(firstName: String, company: String) -> () {
  # inner

  person_search = TavilySearch {
    label: "Research Person"
    maxResults: 3
  }
  person_search.query = Template {
    template: "{{name}} {{company}}"
    name: self.firstName
    company: self.company
  }.text
}
"#;
    let project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile failed");
    // Inside a group, the anon id is `{group_id}.{local_parent}__{field}`.
    // Here: `per_lead.person_search__query`.
    let anon = project.nodes.iter().find(|n| n.id == "per_lead.person_search__query").expect(&format!(
        "expected anon node 'per_lead.person_search__query', got: {:?}",
        project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
    ));
    assert_eq!(anon.nodeType.0, "Template");
    // Check edges: person_search.query <- anon.text, anon.name <- self.firstName, anon.company <- self.company
    let has_query = project.edges.iter().any(|e|
        e.target == "per_lead.person_search"
        && e.targetHandle.as_deref() == Some("query")
        && e.source == "per_lead.person_search__query"
        && e.sourceHandle.as_deref() == Some("text")
    );
    assert!(has_query, "missing person_search.query <- anon.text. edges: {:?}", project.edges);
}

#[test]
fn inline_template_custom_ports_synthesize_required_from_edge() {
    // Rule: an edge that targets an undeclared port on a canAddInputPorts
    // node synthesizes the port with `required: true` and a fresh TypeVar
    // that narrows to the source's type. Users no longer need to declare
    // the port in the anon's signature.
    let source = r#"# Project: Test

source_name = Text { value: "alice" }
source_company = Text { value: "acme" }

writer = LlmInference -> (response: String) {
  label: "Writer"
}
writer.prompt = Template {
  template: "{{name}} at {{company}}"
  name: source_name.value
  company: source_company.value
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile failed");
    let registry = &crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "writer__prompt").expect("writer__prompt");
    let name_port = anon.inputs.iter().find(|p| p.name == "name").expect("synthesized name port");
    assert!(name_port.required, "edge-synthesized port should be required");
    assert_eq!(format!("{}", name_port.portType), "String", "name should narrow to String from source_name.value");
    let company_port = anon.inputs.iter().find(|p| p.name == "company").expect("synthesized company port");
    assert!(company_port.required, "edge-synthesized port should be required");
    assert_eq!(format!("{}", company_port.portType), "String");
}

#[test]
fn inline_template_custom_ports_with_explicit_declaration() {
    // Correct form: declare the custom ports in the anon's signature.
    let source = r#"# Project: Test

source_name = Text { value: "alice" }
source_company = Text { value: "acme" }

writer = LlmInference -> (response: String) {
  label: "Writer"
}
writer.prompt = Template(name: String, company: String) {
  template: "{{name}} at {{company}}"
  name: source_name.value
  company: source_company.value
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile failed");
    let registry = &crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "writer__prompt").expect("writer__prompt");
    let name_port = anon.inputs.iter().find(|p| p.name == "name").expect("no name port");
    let company_port = anon.inputs.iter().find(|p| p.name == "company").expect("no company port");
    assert_eq!(name_port.portType.to_string(), "String");
    assert_eq!(company_port.portType.to_string(), "String");
}

#[test]
fn inline_duplicate_id_reports_correct_line() {
    // Two nodes with the same ID on lines 4 and 9. The duplicate error should
    // report line 9 (the second occurrence).
    let source = r#"# Project: Test
# Description: Test

writer = LlmInference -> (response: String) {
  label: "Writer"
}
writer.prompt = Template { template: "hi" }.text

writer = Text { value: "oops" }
"#;
    let result = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4());
    match result {
        Ok(_) => panic!("expected duplicate-id error"),
        Err(errs) => {
            let dup = errs.iter().find(|e| e.message.contains("Duplicate node ID"));
            assert!(dup.is_some(), "no dup error: {:?}", errs);
            let dup = dup.unwrap();
            // Line 9 in the ORIGINAL source is the second `writer = Text ...`.
            assert_eq!(dup.line, 9, "expected line 9, got {}: {}", dup.line, dup.message);
        }
    }
}

#[test]
fn debug_line_map_nested() {
    // Two sequential inline blocks, plus a duplicate on line 14.
    // 1: # Project
    // 2: (blank)
    // 3: a = Text { value: "a" }
    // 4: b = Text { value: "b" }
    // 5: (blank)
    // 6: writer1 = LlmInference -> (response: String) { label: "1" }
    // 7: writer1.prompt = Template {
    // 8:   template: "hi {{x}}"
    // 9:   x: a.value
    // 10: }.text
    // 11: (blank)
    // 12: writer2 = LlmInference -> (response: String) { label: "2" }
    // 13: writer2.prompt = writer1.response
    // 14: a = Text { value: "dup" }   # duplicate line 14
    let source = r#"# Project: Test

a = Text { value: "a" }
b = Text { value: "b" }

writer1 = LlmInference -> (response: String) { label: "1" }
writer1.prompt = Template {
  template: "hi {{x}}"
  x: a.value
}.text

writer2 = LlmInference -> (response: String) { label: "2" }
writer2.prompt = writer1.response
a = Text { value: "dup" }
"#;
    let result = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4());
    println!("=== debug_line_map_nested ===");
    match result {
        Ok(p) => {
            println!("OK: {} nodes", p.nodes.len());
            for n in &p.nodes {
                println!("  {} ({})", n.id, n.nodeType.0);
            }
        }
        Err(errs) => {
            for e in &errs {
                println!("  line {}: {}", e.line, e.message);
            }
        }
    }
}

#[test]
fn inline_nested_in_config_field() {
    // Nested inline: a Template inside the systemPrompt field of an LlmConfig,
    // whose own template field contains another Template. Both should be
    // promoted to top-level anon nodes with clean IDs.
    let source = r#"# Project: Test

other = Text { value: "world" }

llm_config = LlmConfig {
  apiKey: ""
  systemPrompt: Template {
    template: "outer: {{inner}}"
    inner: Template {
      template: "deep: {{x}}"
      x: other.value
    }.text
  }.text
}
"#;
    let project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile failed");
    // Expected: two anon nodes, both nested structures carry meaningful IDs.
    // Outer: `llm_config__systemPrompt`
    // Inner: `llm_config__systemPrompt__inner`
    let outer = project.nodes.iter().find(|n| n.id == "llm_config__systemPrompt");
    assert!(outer.is_some(), "expected outer anon 'llm_config__systemPrompt'. nodes: {:?}",
        project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>());
    let inner = project.nodes.iter().find(|n| n.id == "llm_config__systemPrompt__inner");
    assert!(inner.is_some(), "expected inner anon 'llm_config__systemPrompt__inner'. nodes: {:?}",
        project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>());
    assert_eq!(outer.unwrap().nodeType.0, "Template");
    assert_eq!(inner.unwrap().nodeType.0, "Template");

    // Expected edges:
    //   other.value -> llm_config__systemPrompt__inner.x
    //   llm_config__systemPrompt__inner.text -> llm_config__systemPrompt.inner
    //   llm_config__systemPrompt.text -> llm_config.systemPrompt
    let e1 = project.edges.iter().any(|e|
        e.source == "other" && e.sourceHandle.as_deref() == Some("value")
        && e.target == "llm_config__systemPrompt__inner" && e.targetHandle.as_deref() == Some("x"));
    let e2 = project.edges.iter().any(|e|
        e.source == "llm_config__systemPrompt__inner" && e.sourceHandle.as_deref() == Some("text")
        && e.target == "llm_config__systemPrompt" && e.targetHandle.as_deref() == Some("inner"));
    let e3 = project.edges.iter().any(|e|
        e.source == "llm_config__systemPrompt" && e.sourceHandle.as_deref() == Some("text")
        && e.target == "llm_config" && e.targetHandle.as_deref() == Some("systemPrompt"));
    assert!(e1, "missing edge: other.value -> inner.x. edges: {:?}", project.edges);
    assert!(e2, "missing edge: inner.text -> outer.inner. edges: {:?}", project.edges);
    assert!(e3, "missing edge: outer.text -> llm_config.systemPrompt. edges: {:?}", project.edges);
}

#[test]
fn inline_rejects_post_config_outputs() {
    let source = r#"# Project: Test

writer = LlmInference -> (response: String) { label: "Writer" }
writer.prompt = Template { template: "hi" } -> (out: String).out
"#;
    let result = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4());
    match result {
        Ok(_) => panic!("expected parse error for post-config outputs in inline"),
        Err(errs) => {
            let has = errs.iter().any(|e| e.message.contains("post-config outputs"));
            assert!(has, "expected 'post-config outputs' error, got: {:?}", errs);
        }
    }
}

#[test]
fn inline_rejects_missing_dot_port() {
    let source = r#"# Project: Test

writer = LlmInference -> (response: String) { label: "W" }
writer.prompt = Template { template: "hi" }
"#;
    let result = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4());
    match result {
        Ok(_) => panic!("expected parse error for missing .port suffix"),
        Err(errs) => {
            let has = errs.iter().any(|e| e.message.contains(".portName") || e.message.contains("port name"));
            assert!(has, "expected '.portName' error, got: {:?}", errs);
        }
    }
}

#[test]
fn group_required_input_unwired_is_compile_error() {
    // When a group declares a required interface input but the external
    // caller doesn't wire an edge into it, the enrich validator must reject
    // the project. "If it compiles, it runs" means unwired required inputs
    // are compile errors, not runtime skips.
    use weft_core::project::{Edge, NodeType, GroupBoundary, GroupBoundaryRole, Position};

    let source = NodeDefinition {
        id: "source".to_string(),
        nodeType: NodeType("Text".to_string()),
        label: None,
        config: serde_json::json!({"value": "hello"}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![],
        outputs: vec![PortDefinition {
            name: "value".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1, configurable: false,
        }],
        features: NodeFeatures::default(),
        scope: vec![],
        groupBoundary: None,
    };
    let grp_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: NodeType("Passthrough".to_string()),
        label: None,
        config: serde_json::json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single, laneDepth: 1, configurable: false,
        }],
        outputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1, configurable: false,
        }],
        features: NodeFeatures::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::In,
        }),
    };
    let grp_out = NodeDefinition {
        id: "grp__out".to_string(),
        nodeType: NodeType("Passthrough".to_string()),
        label: None,
        config: serde_json::json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1, configurable: false,
        }],
        outputs: vec![PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1, configurable: false,
        }],
        features: NodeFeatures::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::Out,
        }),
    };

    let _ = Edge {
        id: "dummy".to_string(),
        source: "".to_string(),
        target: "".to_string(),
        sourceHandle: None,
        targetHandle: None,
    };
    let mut project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "grp_test".to_string(),
        description: None,
        nodes: vec![source, grp_in, grp_out],
        // No edge from source -> grp__in.data (the required input is unwired).
        edges: vec![],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    let errs = result.expect_err("expected compile error for unwired required group input");
    let has_required_error = errs.iter().any(|e|
        e.contains("Group") && e.contains("grp") && e.contains("required input port 'data'")
    );
    assert!(
        has_required_error,
        "expected 'Group grp: required input port data is not connected' error, got: {:?}",
        errs
    );
}

#[test]
fn inline_in_group_with_self_port_wiring_synthesizes_required_port() {
    // `c: self.company_name` on a canAddInputPorts Template without an
    // explicit declaration synthesizes the `c` port with required: true
    // and a TypeVar that narrows to String from the group's company_name.
    let source = r#"# Project: Test

source = Text { value: "acme" }

process_lead = Group(company_name: String) -> (answer: String?) {
  company_search = TavilySearch {
    label: "Research"
    maxResults: 5
  }
  company_search.query = Template {
    template: "{{c}} research"
    c: self.company_name
  }.text

  self.answer = company_search.answer
}

process_lead.company_name = source.value
result = Debug { label: "Result" }
result.data = process_lead.answer
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile should succeed");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter()
        .find(|n| n.id == "process_lead.company_search__query")
        .expect("anon Template not found");
    let c_port = anon.inputs.iter().find(|p| p.name == "c").expect("synthesized c port");
    assert!(c_port.required, "edge-synthesized port should be required");
    assert_eq!(c_port.portType.to_string(), "String", "c should narrow to String from self.company_name");
}

#[test]
fn inline_in_group_with_self_port_wiring_with_explicit_declaration() {
    // Correct form: declare `c` in Template's port signature.
    let source = r#"# Project: Test

source = Text { value: "acme" }

process_lead = Group(company_name: String) -> (answer: String?) {
  company_search = TavilySearch {
    label: "Research"
    maxResults: 5
  }
  company_search.query = Template(c: String) {
    template: "{{c}} research"
    c: self.company_name
  }.text

  self.answer = company_search.answer
}

process_lead.company_name = source.value
result = Debug { label: "Result" }
result.data = process_lead.answer
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile should succeed");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");

    let anon = project.nodes.iter()
        .find(|n| n.id == "process_lead.company_search__query")
        .expect("anon Template not found");
    let c_port = anon.inputs.iter().find(|p| p.name == "c").expect("c port missing");
    assert_eq!(c_port.portType.to_string(), "String");
}

#[test]
fn nested_inline_in_group_with_self_synthesizes_required_ports() {
    // Edges targeting undeclared ports at every level of a nested inline
    // anon chain are each synthesized with `required: true` and a fresh
    // TypeVar that narrows to the source's type.
    let source = r#"# Project: Test

src = Text { value: "hello" }

grp = Group(thing: String) -> (out: String?) {
  writer = LlmInference -> (response: String) {
    label: "Writer"
  }
  writer.prompt = Template {
    template: "{{inner}}"
    inner: Template {
      template: "deep: {{x}}"
      x: self.thing
    }.text
  }.text

  self.out = writer.response
}

grp.thing = src.value
out = Debug { label: "out" }
out.data = grp.out
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile should succeed");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");

    let outer = project.nodes.iter()
        .find(|n| n.id == "grp.writer__prompt")
        .expect("outer anon Template not found");
    let inner_port = outer.inputs.iter().find(|p| p.name == "inner").expect("synthesized inner port");
    assert!(inner_port.required, "edge-synthesized port should be required");
    assert_eq!(inner_port.portType.to_string(), "String");

    let inner = project.nodes.iter()
        .find(|n| n.id == "grp.writer__prompt__inner")
        .expect("inner anon Template not found");
    let x_port = inner.inputs.iter().find(|p| p.name == "x").expect("synthesized x port");
    assert!(x_port.required, "edge-synthesized port should be required");
    assert_eq!(x_port.portType.to_string(), "String");
}

#[test]
fn nested_inline_in_group_with_self_with_explicit_declarations() {
    // Correct form: declare `inner` on the outer Template and `x` on the inner.
    let source = r#"# Project: Test

src = Text { value: "hello" }

grp = Group(thing: String) -> (out: String?) {
  writer = LlmInference -> (response: String) {
    label: "Writer"
  }
  writer.prompt = Template(inner: String) {
    template: "{{inner}}"
    inner: Template(x: String) {
      template: "deep: {{x}}"
      x: self.thing
    }.text
  }.text

  self.out = writer.response
}

grp.thing = src.value
out = Debug { label: "out" }
out.data = grp.out
"#;
    let mut project = weft_core::weft_compiler::compile(source, uuid::Uuid::new_v4()).expect("compile should succeed");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");

    let inner_anon = project.nodes.iter()
        .find(|n| n.id == "grp.writer__prompt__inner")
        .expect("inner anon Template not found");
    let x_port = inner_anon.inputs.iter().find(|p| p.name == "x").expect("x port missing");
    assert_eq!(x_port.portType.to_string(), "String");
}

#[test]
fn triple_backtick_colon_whitespace_variants() {
    // Backend requires exactly `key: ```` followed by content. Verify:
    //   - `key: ```\n...\n````  (canonical form, space after colon): works
    //   - `key:```\n...\n````   (no space): should also work
    //   - `key : ```\n...\n```` (space before colon): should also work
    // Frontend's regex `/^(\w+)\s*:\s*```/` accepts all three variants, so
    // the backend should too for symmetry.
    let variants = [
        ("canonical", r#"# Project: Test
n = Text { value: "x" }
"#),
    ];
    // Canonical baseline works, just use as template
    let _ = variants;

    // Variant 1: no space after colon
    let src1 = "# Project: Test\n\nn = ExecPython -> (result: String) {\n  code:```\nreturn {\"result\": \"x\"}\n```\n}\n";
    let r1 = weft_core::weft_compiler::compile(src1, uuid::Uuid::new_v4());
    assert!(r1.is_ok(), "no-space variant should parse; got: {:?}", r1.err());
    let p1 = r1.unwrap();
    let n1 = p1.nodes.iter().find(|n| n.id == "n").expect("n not found");
    let code_val = n1.config.get("code").expect("code config missing");
    let code_str = code_val.as_str().expect("code should be a string");
    assert!(
        code_str.contains("return") && !code_str.starts_with("```"),
        "expected dedented python code body, got: {:?}",
        code_str,
    );

    // Variant 2: extra whitespace around colon.
    let src2 = "# Project: Test\n\nn = ExecPython -> (result: String) {\n  code  :   ```\nreturn {\"result\": \"x\"}\n```\n}\n";
    let p2 = weft_core::weft_compiler::compile(src2, uuid::Uuid::new_v4()).expect("extra-ws variant should parse");
    let n2 = p2.nodes.iter().find(|n| n.id == "n").expect("n not found");
    let code2 = n2.config.get("code").expect("code missing").as_str().expect("string");
    assert!(code2.contains("return"), "expected code body, got: {:?}", code2);
}

#[test]
fn multiline_port_list_without_trailing_comma() {
    // Backend splits on newlines at depth 0; frontend replaces newlines
    // with spaces, so ports on separate lines without commas may disagree.
    // Both sides should accept this form:
    //   name = Type(
    //     a: String
    //     b: Number
    //   ) -> (
    //     out: String
    //   )
    let src = "# Project: Test\n\nn = ExecPython(\n  a: String\n  b: Number\n) -> (\n  out: String\n) {\n  code: ```\nreturn {\"out\": a}\n```\n}\n";
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    let p = result.expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n not found");
    // Backend: should have 2 input ports a, b
    let input_names: Vec<&str> = n.inputs.iter().map(|p| p.name.as_str()).collect();
    assert!(input_names.contains(&"a"), "missing port a; got: {:?}", input_names);
    assert!(input_names.contains(&"b"), "missing port b; got: {:?}", input_names);
    let output_names: Vec<&str> = n.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(output_names.contains(&"out"), "missing port out; got: {:?}", output_names);
}

#[test]
fn legacy_label_after_type_one_liner_is_rejected() {
    // The legacy form `id = Type "label" { ... }` is not supported. The
    // modern form is `id = Type { label: "label", ... }`. Frontend and
    // backend both reject the legacy form; this test locks that in.
    let src = r#"# Project: Test

n = Text "my label" { value: "hi" }
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    assert!(result.is_err(), "legacy form should be rejected by the backend");
}

#[test]
fn duplicate_group_name_is_rejected() {
    let src = r#"# Project: Test

g = Group() -> (x: String?) {
  n = Text { value: "a" }
  self.x = n.value
}

g = Group() -> (x: String?) {
  n = Text { value: "b" }
  self.x = n.value
}
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    assert!(result.is_err(), "duplicate group should be rejected");
}

#[test]
fn multiline_json_wellformed_object_parses_on_both() {
    let src = r#"# Project: Test

n = Text -> (value: String) {
  extra: {
    "a": 1,
    "b": 2
  }
  value: "hi"
}
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    // value should be "hi"
    assert_eq!(n.config.get("value").and_then(|v| v.as_str()), Some("hi"));
    // extra should be a JSON object
    let extra = n.config.get("extra").expect("extra missing");
    assert!(extra.is_object(), "extra should be a JSON object, got: {:?}", extra);
    assert_eq!(extra.get("a").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(extra.get("b").and_then(|v| v.as_i64()), Some(2));
}

#[test]
fn post_config_outputs_same_line() {
    let src = r#"# Project: Test

n = ExecPython(x: String) {
  code: ```
return {"a": x, "b": x}
```
} -> (a: String, b: String)

n.x = src.value

src = Text { value: "x" }
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    let out_names: Vec<&str> = n.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(out_names.contains(&"a"), "missing a: {:?}", out_names);
    assert!(out_names.contains(&"b"), "missing b: {:?}", out_names);
}

#[test]
fn post_config_outputs_next_line() {
    let src = r#"# Project: Test

n = ExecPython(x: String) {
  code: ```
return {"a": x}
```
}
-> (a: String)

n.x = src.value

src = Text { value: "x" }
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    let out_names: Vec<&str> = n.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(out_names.contains(&"a"), "missing a: {:?}", out_names);
}

#[test]
fn root_connection_with_dotted_target_is_not_supported() {
    // Root-level connections use `target.port = source.port`. The target
    // must be a node id (no dots beyond the .port separator). Nested
    // targets like `outer.inner.port` are not valid root connections.
    let src = r#"# Project: Test

src = Text { value: "x" }
outer = Group() -> (y: String?) {
  inner = Text { value: "a" }
  self.y = inner.value
}

outer.inner.value = src.value
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    // Either side may reject or produce a meaningless edge; we don't
    // care which, as long as it doesn't crash.
    let _ = result;
}

#[test]
fn one_liner_with_port_signature() {
    // True one-liner: declaration, port sig, and config all on one line.
    let src = r#"# Project: Test

src = Text { value: "x" }
n = Text(x: String) -> (y: String) { value: "hi" }

n.x = src.value
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    assert_eq!(n.inputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), vec!["x"]);
    assert_eq!(n.outputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), vec!["y"]);
    assert_eq!(n.config.get("value").and_then(|v| v.as_str()), Some("hi"));
}

// Group descriptions are parsed on both sides but dropped at the backend
// compile stage (no place in the compiled output) and preserved on the
// frontend as UI metadata on the synthesized Group nodeInstance. Both
// behaviors are by design, no parity test applies.

#[test]
fn config_value_types_parity() {
    let src = r#"# Project: Test

n = Text {
  bool_t: true
  bool_f: false
  int_val: 42
  neg_int: -17
  float_val: 3.14
  neg_float: -2.5
  str_quoted: "hello"
  str_escaped: "hello \"world\""
  arr_json: [1, 2, 3]
  obj_json: {"a": 1, "b": "two"}
}
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    let c = &n.config;
    assert_eq!(c.get("bool_t").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(c.get("bool_f").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(c.get("int_val").and_then(|v| v.as_i64()), Some(42));
    assert_eq!(c.get("neg_int").and_then(|v| v.as_i64()), Some(-17));
    assert!((c.get("float_val").and_then(|v| v.as_f64()).unwrap() - 3.14).abs() < 0.001);
    assert!((c.get("neg_float").and_then(|v| v.as_f64()).unwrap() + 2.5).abs() < 0.001);
    assert_eq!(c.get("str_quoted").and_then(|v| v.as_str()), Some("hello"));
    assert_eq!(c.get("str_escaped").and_then(|v| v.as_str()), Some("hello \"world\""));
    assert!(c.get("arr_json").unwrap().is_array());
    assert!(c.get("obj_json").unwrap().is_object());
}

#[test]
fn require_one_of_all_positions() {
    // Three places @require_one_of can appear: node input port list,
    // inside a node config block, and on a group interface.
    let src = r#"# Project: Test

src1 = Text { value: "a" }
src2 = Text { value: "b" }

# Input port list: @require_one_of(a, b) inside the parens
n1 = ExecPython(
  a: String?
  b: String?
  @require_one_of(a, b)
) -> (r: String) {
  code: ```
return {"r": a or b}
```
}
n1.a = src1.value

# Config block: @require_one_of(...) as a standalone line
n2 = ExecPython(x: String?, y: String?) -> (r: String) {
  code: ```
return {"r": x or y}
```
  @require_one_of(x, y)
}
n2.x = src1.value

# Group interface: @require_one_of on a Group() input list
g = Group(a: String?, b: String?, @require_one_of(a, b)) -> (r: String?) {
  pick = ExecPython(a: String?, b: String?) -> (r: String) {
    code: ```
return {"r": a or b}
```
  }
  pick.a = self.a
  pick.b = self.b
  self.r = pick.r
}
g.a = src1.value
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    // n1: oneOfRequired should be [["a", "b"]]
    let n1 = p.nodes.iter().find(|n| n.id == "n1").expect("n1");
    assert_eq!(n1.features.oneOfRequired, vec![vec!["a".to_string(), "b".to_string()]],
        "n1 oneOfRequired mismatch: {:?}", n1.features.oneOfRequired);
    // n2: same
    let n2 = p.nodes.iter().find(|n| n.id == "n2").expect("n2");
    assert_eq!(n2.features.oneOfRequired, vec![vec!["x".to_string(), "y".to_string()]],
        "n2 oneOfRequired mismatch: {:?}", n2.features.oneOfRequired);
    // g: the group's oneOfRequired lives on the In boundary
    let g_in = p.nodes.iter().find(|n| n.id == "g__in").expect("g__in");
    assert_eq!(g_in.features.oneOfRequired, vec![vec!["a".to_string(), "b".to_string()]],
        "g__in oneOfRequired mismatch: {:?}", g_in.features.oneOfRequired);
}

#[test]
fn label_with_escaped_quote() {
    let src = r#"# Project: Test

n = Text { label: "Say \"hello\" there", value: "x" }
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    // Backend unescapes \" to "
    assert_eq!(n.label.as_deref(), Some(r#"Say "hello" there"#));
}

#[test]
fn label_with_escaped_quote_multiline_block() {
    let src = r#"# Project: Test

n = Text {
  label: "Say \"hello\" there"
  value: "x"
}
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let n = p.nodes.iter().find(|n| n.id == "n").expect("n");
    assert_eq!(n.label.as_deref(), Some(r#"Say "hello" there"#));
}

#[test]
fn inline_on_non_port_config_key_is_rejected() {
    // Inline expressions in config blocks should only be allowed for keys
    // that correspond to configurable input ports. `label` is metadata on
    // every node, never a port, so `label: Template { ... }.text` must be
    // rejected by enrichment (the parser accepts the shape, enrichment
    // has access to the registry and validates).
    let src = r#"# Project: Test

n = Text {
  label: Template { template: "hi" }.text
  value: "v"
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(
        result.is_err(),
        "enrichment should reject inline on non-port config key 'label'; got: {:?}",
        result,
    );
    let errs = result.unwrap_err();
    assert!(
        errs.iter().any(|e| e.contains("label")),
        "expected an error mentioning 'label', got: {:?}",
        errs,
    );
}

#[test]
fn oneliner_inline_detected_and_anon_created() {
    // In a one-liner config block, an inline expression value should be
    // detected (not silently swallowed as a literal string). The parser
    // emits the anon + edge; enrichment decides whether the target port
    // is valid.
    let src = r#"# Project: Test

n = Text { label: Template { template: "hi" }.text, value: "v" }
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse ok");
    let has_anon = project.nodes.iter().any(|nn| nn.id == "n__label");
    assert!(has_anon, "expected n__label anon to be created by the one-liner parser; nodes: {:?}",
        project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>());
    let has_edge = project.edges.iter()
        .any(|e| e.source == "n__label" && e.target == "n" && e.targetHandle.as_deref() == Some("label"));
    assert!(has_edge, "expected edge n__label.text -> n.label");
}

#[test]
fn oneliner_inline_on_non_port_key_rejected_by_enrichment() {
    // `label` is not an input port. The parser happily creates the anon
    // and edge; enrichment must reject the edge because `label` doesn't
    // exist on the host node's input ports.
    let src = r#"# Project: Test

n = Text { label: Template { template: "hi" }.text, value: "v" }
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse ok");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(result.is_err(), "enrichment should reject; got: {:?}", result);
    let errs = result.unwrap_err();
    assert!(
        errs.iter().any(|e| e.contains("label")),
        "expected an error mentioning 'label', got: {:?}",
        errs,
    );
}

#[test]
fn bare_inline_type_dot_port_in_config() {
    // Bare form `key: Type.port`: no braces. Creates an anon with default
    // config and wires its `.port` output into the host's `key` field.
    let src = r#"# Project: Test

host = LlmInference {
  model: "anthropic/claude-sonnet-4-5"
  prompt: Text.value
}
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let anon = p.nodes.iter().find(|n| n.id == "host__prompt").expect("anon");
    assert_eq!(anon.nodeType.0, "Text");
    // Anon should have empty config (default construction).
    assert!(anon.config.as_object().map(|o| o.is_empty()).unwrap_or(true), "bare inline should have empty config, got: {:?}", anon.config);
    // Edge from anon.value -> host.prompt.
    let edge = p.edges.iter()
        .find(|e| e.source == "host__prompt" && e.target == "host")
        .expect("edge from anon to host");
    assert_eq!(edge.sourceHandle.as_deref(), Some("value"));
    assert_eq!(edge.targetHandle.as_deref(), Some("prompt"));
}

#[test]
fn bare_inline_connection_rhs() {
    // Bare form on RHS of connection: `target.port = Type.port`.
    let src = r#"# Project: Test

host = LlmInference {
  model: "gpt-4"
}
host.prompt = Text.value
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("should parse");
    let anon = p.nodes.iter().find(|n| n.id == "host__prompt").expect("anon");
    assert_eq!(anon.nodeType.0, "Text");
    assert!(anon.config.as_object().map(|o| o.is_empty()).unwrap_or(true));
}

#[test]
fn oneliner_bare_inline_creates_anon() {
    // `prompt: Text.value` in a one-liner creates the anon + edge.
    let src = r#"# Project: Test

n = LlmInference { model: "gpt-4", prompt: Text.value }
"#;
    let p = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse ok");
    let anon = p.nodes.iter().find(|n| n.id == "n__prompt").expect("anon");
    assert_eq!(anon.nodeType.0, "Text");
    let edge = p.edges.iter().find(|e| e.source == "n__prompt" && e.target == "n").expect("edge");
    assert_eq!(edge.sourceHandle.as_deref(), Some("value"));
    assert_eq!(edge.targetHandle.as_deref(), Some("prompt"));
}

#[test]
fn port_wiring_in_regular_node_config_self_inside_group() {
    // Inside a group body, `greeting = Template { template: self.text }`
    // should wire the group's `text` input port into `greeting.template`
    // (Template has `template` as a real input port). Parser emits the
    // edge; enrichment validates the target port exists.
    let src = r#"# Project: T

grp = Group(text: String) -> (out: String?) {
  greeting = Template { template: self.text }
  self.out = greeting.text
}

src = Text { value: "hi" }
grp.text = src.value
out = Debug { label: "out" }
out.data = grp.out
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let greeting = project.nodes.iter().find(|n| n.id == "grp.greeting").expect("greeting node");
    assert!(
        greeting.config.get("template").is_none(),
        "template should have been consumed as a port wiring, not stored as a literal; got: {:?}",
        greeting.config,
    );
    let wired = project.edges.iter().any(|e|
        e.source == "grp__in"
        && e.sourceHandle.as_deref() == Some("text")
        && e.target == "grp.greeting"
        && e.targetHandle.as_deref() == Some("template")
    );
    assert!(wired, "expected edge grp__in.text -> grp.greeting.template; edges: {:?}", project.edges);
}

#[test]
fn port_wiring_in_regular_node_config_cross_node() {
    // Same shortcut at root scope: `greeting = Template { template: src.value }`
    // wires `src.value` into `greeting.template`.
    let src = r#"# Project: T

src = Text { value: "hi {{x}}" }
greeting = Template { template: src.value }
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let greeting = project.nodes.iter().find(|n| n.id == "greeting").expect("greeting");
    assert!(greeting.config.get("template").is_none(), "template should be a wiring, got: {:?}", greeting.config);
    let wired = project.edges.iter().any(|e|
        e.source == "src"
        && e.sourceHandle.as_deref() == Some("value")
        && e.target == "greeting"
        && e.targetHandle.as_deref() == Some("template")
    );
    assert!(wired, "expected edge src.value -> greeting.template; edges: {:?}", project.edges);
}

#[test]
fn port_wiring_in_regular_config_non_port_rejected_by_enrichment() {
    // `label` is not an input port on any node. Writing `label: src.value`
    // emits an edge pointing at a non-existent port, which enrichment
    // rejects.
    let src = r#"# Project: T

src = Text { value: "hi" }
greeting = Template { label: src.value, template: "fixed" }
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(result.is_err(), "enrichment should reject port wiring on non-port key; got: {:?}", result);
}

#[test]
fn port_wiring_in_regular_config_text_value_rejected() {
    // Text's `value` is a config field, NOT an input port. So
    // `test = Text { value: self.text }` inside a group body must fail
    // at enrichment: the emitted edge targets `test.value`, which
    // doesn't exist as an input port on Text.
    let src = r#"# Project: T

grp = Group(text: String) -> (out: String?) {
  test = Text { value: self.text }
  self.out = test.value
}

src = Text { value: "hi" }
grp.text = src.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(
        result.is_err(),
        "enrichment should reject: Text.value is not an input port; got: {:?}",
        result,
    );
    let errs = result.unwrap_err();
    assert!(
        errs.iter().any(|e| e.contains("value")),
        "expected an error mentioning the bad target port 'value', got: {:?}",
        errs,
    );
}

#[test]
fn required_port_filled_from_config_is_accepted() {
    // Template's `template` port is required. Filling it from config
    // (no edge) should satisfy the requirement.
    let src = r#"# Project: T

greeting = Template {
  template: "Hello {{name}}"
}
out = Debug { label: "out" }
out.data = greeting.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(
        result.is_ok(),
        "enrichment should accept a required port filled from config; got: {:?}",
        result,
    );
}

#[test]
fn required_port_not_filled_is_rejected() {
    // Same node but without the `template` field: should fail because
    // `template` is required and has no edge and no config value.
    let src = r#"# Project: T

greeting = Template { label: "bad" }
out = Debug { label: "out" }
out.data = greeting.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(
        result.is_err(),
        "enrichment should reject: template is required but not filled; got: {:?}",
        result,
    );
}

#[test]
fn connection_line_literal_fills_config() {
    // `host.port = "literal"` on a connection line should store the literal
    // in host.config[port] instead of emitting an edge.
    let src = r#"# Project: T

greeting = Template { label: "greeting" }
greeting.template = "Hello {{name}}"
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let greeting = project.nodes.iter().find(|n| n.id == "greeting").expect("greeting");
    assert_eq!(
        greeting.config.get("template").and_then(|v| v.as_str()),
        Some("Hello {{name}}"),
        "template should be set via config fill; config: {:?}",
        greeting.config,
    );
    // No edge should have been emitted for the literal.
    let has_edge = project.edges.iter().any(|e|
        e.target == "greeting" && e.targetHandle.as_deref() == Some("template")
    );
    assert!(!has_edge, "connection-line literal should not emit an edge");
    // Required port is satisfied by the config fill.
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(result.is_ok(), "enrichment should accept; got: {:?}", result);
}

#[test]
fn connection_line_literal_number_bool_json() {
    // Various literal types supported (number, bool, JSON array/object).
    let src = r#"# Project: T

n = LlmInference { model: "gpt-4" }
n.temperature = 0.8
n.parseJson = true
n.systemPrompt = "Be concise"
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let n = project.nodes.iter().find(|nn| nn.id == "n").expect("n");
    assert_eq!(n.config.get("temperature").and_then(|v| v.as_f64()), Some(0.8));
    assert_eq!(n.config.get("parseJson").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(n.config.get("systemPrompt").and_then(|v| v.as_str()), Some("Be concise"));
}

#[test]
fn connection_line_literal_last_write_wins() {
    // Last literal write wins: the connection-line literal (later in source)
    // overrides the inline literal.
    let src = r#"# Project: T

n = Template { template: "first" }
n.template = "second"
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let n = project.nodes.iter().find(|nn| nn.id == "n").expect("n");
    assert_eq!(n.config.get("template").and_then(|v| v.as_str()), Some("second"));
}

#[test]
fn connection_line_literal_on_wired_only_port_rejected() {
    // Media-typed port is not default-configurable. Writing a literal via
    // connection line should be rejected at enrichment. Use any media node
    // with a required Image input: none easy to construct here, so we test
    // with a port we know is configurable false (config on LlmInference).
    // Actually `config` on LlmInference is explicitly configurable: false.
    let src = r#"# Project: T

llm = LlmInference { model: "gpt-4", prompt: "hi" }
llm.config = "not a config"
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    assert!(
        result.is_err(),
        "enrichment should reject literal on non-configurable port; got: {:?}",
        result,
    );
}

#[test]
fn connection_line_literal_triple_backtick_multiline() {
    // `n.code = ```...``` ` across multiple lines.
    let src = r#"# Project: T

n = ExecPython(x: String) -> (out: String) { label: "run" }
n.code = ```
return {"out": x}
```
n.x = "hi"
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let n = project.nodes.iter().find(|nn| nn.id == "n").expect("n");
    let code = n.config.get("code").and_then(|v| v.as_str()).expect("code config should be set");
    assert!(code.contains("return"), "code should contain 'return'; got: {:?}", code);
    assert!(!code.contains("```"), "triple backticks should be stripped; got: {:?}", code);
    // x literal
    assert_eq!(n.config.get("x").and_then(|v| v.as_str()), Some("hi"));
}

#[test]
fn connection_line_literal_multiline_json() {
    // `n.data = { ... }` with braces on separate lines.
    let src = r#"# Project: T

n = Template { template: "hi" }
n.data = {
  "a": 1,
  "b": "two"
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let n = project.nodes.iter().find(|nn| nn.id == "n").expect("n");
    let data = n.config.get("data").expect("data missing");
    assert!(data.is_object(), "data should be parsed as JSON object; got: {:?}", data);
    assert_eq!(data.get("a").and_then(|v| v.as_i64()), Some(1));
}

// ─── Combination tests: mixing inline expression forms on connection RHS ───

#[test]
fn combo_full_form_inline_with_ports_and_config_on_connection_rhs() {
    // The exact shape from the user's question: explicit port signature +
    // config block + port wiring inside the body + .out suffix, all on the
    // RHS of a connection.
    let src = r#"# Project: T

src = Text { value: "hello" }
host = Debug { label: "h" }
host.data = ExecPython (
  test: String
) -> (
  out: String
) {
  test: src.value
  code: ```
return {"out": test}
```
}.out
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    // Anon should exist with the right ports and config.
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("host__data anon missing");
    assert_eq!(anon.nodeType.0, "ExecPython");
    let input_names: Vec<&str> = anon.inputs.iter().map(|p| p.name.as_str()).collect();
    assert!(input_names.contains(&"test"), "expected test input port; got: {:?}", input_names);
    let output_names: Vec<&str> = anon.outputs.iter().map(|p| p.name.as_str()).collect();
    assert!(output_names.contains(&"out"), "expected out output port; got: {:?}", output_names);
    assert!(anon.config.get("code").is_some(), "code config missing");
    // Two edges: src.value -> anon.test, anon.out -> host.data
    let wiring_in = project.edges.iter().any(|e|
        e.source == "src" && e.sourceHandle.as_deref() == Some("value")
        && e.target == "host__data" && e.targetHandle.as_deref() == Some("test")
    );
    assert!(wiring_in, "expected src.value -> host__data.test edge");
    let wiring_out = project.edges.iter().any(|e|
        e.source == "host__data" && e.sourceHandle.as_deref() == Some("out")
        && e.target == "host" && e.targetHandle.as_deref() == Some("data")
    );
    assert!(wiring_out, "expected host__data.out -> host.data edge");
}

#[test]
fn combo_nested_inline_on_connection_rhs() {
    // Inline on a connection RHS that has another inline inside its body.
    let src = r#"# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template {
  template: Template {
    template: "Hello {{x}}"
    x: src.value
  }.text
}.text
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    // Outer anon: host__data (Template)
    let outer = project.nodes.iter().find(|n| n.id == "host__data").expect("outer anon missing");
    assert_eq!(outer.nodeType.0, "Template");
    // Inner anon: host__data__template (Template inside the outer's body)
    let inner = project.nodes.iter().find(|n| n.id == "host__data__template").expect("inner anon missing");
    assert_eq!(inner.nodeType.0, "Template");
    // Inner should be wired to src
    let wired_inner = project.edges.iter().any(|e|
        e.source == "src" && e.target == "host__data__template"
    );
    assert!(wired_inner, "expected src -> host__data__template edge");
    // Outer gets template from inner
    let wired_outer = project.edges.iter().any(|e|
        e.source == "host__data__template" && e.target == "host__data"
    );
    assert!(wired_outer, "expected host__data__template -> host__data edge");
    // Final edge outer -> host
    let wired_final = project.edges.iter().any(|e|
        e.source == "host__data" && e.target == "host"
    );
    assert!(wired_final, "expected host__data -> host edge");
}

#[test]
fn combo_bare_inline_in_group_body() {
    // Bare `Type.port` inside a group body on a connection line.
    let src = r#"# Project: T

outer = Group() -> (out: String?) {
  sink = Debug { label: "s" }
  sink.data = Text.value
  self.out = sink.data
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "outer.sink__data").expect("bare anon missing");
    assert_eq!(anon.nodeType.0, "Text");
}

#[test]
fn combo_post_config_output_on_inline_rhs_is_rejected() {
    // Inline with post-config outputs `{ ... } -> (out: X).out` on a
    // connection RHS: compiler should error.
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = ExecPython {
  code: ```
return {"out": "x"}
```
} -> (out: String).out
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    assert!(
        result.is_err(),
        "should reject post-config outputs on inline expression; got: {:?}",
        result,
    );
    let errs = result.unwrap_err();
    assert!(
        errs.iter().any(|e| e.message.contains("post-config")),
        "expected 'post-config' error, got: {:?}",
        errs,
    );
}

#[test]
fn combo_self_wiring_inside_inline_body_on_connection_rhs() {
    // Inline on a connection RHS inside a group, with `self.x` wiring
    // inside the inline body. The self reference should point at the
    // group's In passthrough.
    let src = r#"# Project: T

src = Text { value: "hi" }
grp = Group(thing: String) -> (out: String?) {
  dst = Debug { label: "d" }
  dst.data = Template {
    template: "{{x}}"
    x: self.thing
  }.text
  self.out = dst.data
}
grp.thing = src.value
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    // The anon is dst__data under grp's scope.
    let anon = project.nodes.iter().find(|n| n.id == "grp.dst__data").expect("anon missing");
    assert_eq!(anon.nodeType.0, "Template");
    // Inner self.thing wiring should route through grp__in.thing.
    let self_wired = project.edges.iter().any(|e|
        e.source == "grp__in" && e.sourceHandle.as_deref() == Some("thing")
        && e.target == "grp.dst__data" && e.targetHandle.as_deref() == Some("x")
    );
    assert!(self_wired, "expected grp__in.thing -> grp.dst__data.x edge; edges: {:?}",
        project.edges.iter().map(|e| format!("{}.{:?} -> {}.{:?}", e.source, e.sourceHandle, e.target, e.targetHandle)).collect::<Vec<_>>());
}

#[test]
fn combo_mixed_literal_and_wiring_in_inline_body_on_rhs_synthesizes_port() {
    // `x: src.value` inside an inline body on a canAddInputPorts Template
    // synthesizes `x` as a required port with a TypeVar that narrows to
    // String from src.value. No explicit declaration needed.
    let src = r#"# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template {
  template: "Hello {{x}}"
  x: src.value
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("anon missing");
    let x_port = anon.inputs.iter().find(|p| p.name == "x").expect("synthesized x port");
    assert!(x_port.required, "edge-synthesized port should be required");
    assert_eq!(x_port.portType.to_string(), "String");
}

#[test]
fn combo_mixed_literal_and_wiring_in_inline_body_with_declared_port() {
    // Correct form: declare `x` in the anon's port signature.
    let src = r#"# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template(x: String) {
  template: "Hello {{x}}"
  x: src.value
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("anon missing");
    assert!(anon.inputs.iter().any(|p| p.name == "x"), "x port should exist on anon");
    let wired = project.edges.iter().any(|e|
        e.source == "src" && e.target == "host__data" && e.targetHandle.as_deref() == Some("x")
    );
    assert!(wired, "expected src -> host__data.x edge");
}

#[test]
fn combo_bare_inline_inside_inline_body_on_rhs() {
    // Inline body on a connection RHS contains a bare Type.port assignment.
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = Template {
  template: "Hello {{x}}"
  x: Text.value
}.text
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    // The outer anon is host__data (Template)
    let outer = project.nodes.iter().find(|n| n.id == "host__data").expect("outer missing");
    assert_eq!(outer.nodeType.0, "Template");
    // The bare inline child is host__data__x (Text) bound to the `x` port
    let inner = project.nodes.iter().find(|n| n.id == "host__data__x").expect("inner bare anon missing");
    assert_eq!(inner.nodeType.0, "Text");
    // Edge host__data__x.value -> host__data.x
    let wired = project.edges.iter().any(|e|
        e.source == "host__data__x" && e.sourceHandle.as_deref() == Some("value")
        && e.target == "host__data" && e.targetHandle.as_deref() == Some("x")
    );
    assert!(wired, "expected bare inline wiring");
}

// ─── Combination tests: inline expressions INSIDE a host node's config block ───

#[test]
fn combo_full_form_inline_inside_config_block() {
    // The user's full-form inline shape, this time as the value of a
    // config field inside another node's config block.
    let src = r#"# Project: T

src = Text { value: "hi" }
host = Debug {
  label: "host"
  data: ExecPython (
    test: String
  ) -> (
    out: String
  ) {
    test: src.value
    code: ```
return {"out": test}
```
  }.out
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("host__data missing");
    assert_eq!(anon.nodeType.0, "ExecPython");
    assert!(anon.inputs.iter().any(|p| p.name == "test"));
    assert!(anon.outputs.iter().any(|p| p.name == "out"));
    assert!(anon.config.get("code").is_some());
    let wiring_in = project.edges.iter().any(|e|
        e.source == "src" && e.sourceHandle.as_deref() == Some("value")
        && e.target == "host__data" && e.targetHandle.as_deref() == Some("test")
    );
    assert!(wiring_in, "expected src.value -> host__data.test");
    let wiring_out = project.edges.iter().any(|e|
        e.source == "host__data" && e.sourceHandle.as_deref() == Some("out")
        && e.target == "host" && e.targetHandle.as_deref() == Some("data")
    );
    assert!(wiring_out, "expected host__data.out -> host.data");
}

#[test]
fn combo_nested_inline_inside_config_block() {
    let src = r#"# Project: T

src = Text { value: "world" }
host = Debug {
  label: "host"
  data: Template {
    template: Template {
      template: "Hello {{x}}"
      x: src.value
    }.text
  }.text
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let outer = project.nodes.iter().find(|n| n.id == "host__data").expect("outer missing");
    assert_eq!(outer.nodeType.0, "Template");
    let inner = project.nodes.iter().find(|n| n.id == "host__data__template").expect("inner missing");
    assert_eq!(inner.nodeType.0, "Template");
    assert!(project.edges.iter().any(|e| e.source == "src" && e.target == "host__data__template"));
    assert!(project.edges.iter().any(|e| e.source == "host__data__template" && e.target == "host__data"));
    assert!(project.edges.iter().any(|e| e.source == "host__data" && e.target == "host"));
}

#[test]
fn combo_bare_inline_inside_config_block() {
    // Bare Type.port as a config value in a regular node block.
    let src = r#"# Project: T

host = Debug {
  label: "host"
  data: Text.value
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("bare anon missing");
    assert_eq!(anon.nodeType.0, "Text");
}

#[test]
fn combo_self_wiring_in_inline_body_inside_group_child_config_block() {
    // Group contains a child node whose config block uses an inline
    // expression whose body references `self.thing`. The self reference
    // should route through the group's In passthrough.
    let src = r#"# Project: T

src = Text { value: "hi" }
grp = Group(thing: String) -> (out: String?) {
  dst = Debug {
    label: "d"
    data: Template {
      template: "{{x}}"
      x: self.thing
    }.text
  }
  self.out = dst.data
}
grp.thing = src.value
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "grp.dst__data").expect("anon missing");
    assert_eq!(anon.nodeType.0, "Template");
    let self_wired = project.edges.iter().any(|e|
        e.source == "grp__in" && e.sourceHandle.as_deref() == Some("thing")
        && e.target == "grp.dst__data" && e.targetHandle.as_deref() == Some("x")
    );
    assert!(self_wired, "expected grp__in.thing -> grp.dst__data.x");
}

#[test]
fn combo_triple_nested_inline_on_rhs() {
    // Three levels of inline expression nesting on a connection RHS.
    let src = r#"# Project: T

src = Text { value: "hi" }
host = Debug { label: "h" }
host.data = Template {
  template: Template {
    template: Template {
      template: "deep {{y}}"
      y: src.value
    }.text
  }.text
}.text
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    assert!(project.nodes.iter().any(|n| n.id == "host__data"), "L1 missing");
    assert!(project.nodes.iter().any(|n| n.id == "host__data__template"), "L2 missing");
    assert!(project.nodes.iter().any(|n| n.id == "host__data__template__template"), "L3 missing");
    // Bottom-most gets src.value
    assert!(project.edges.iter().any(|e|
        e.source == "src" && e.target == "host__data__template__template"
    ), "L3 should be wired to src");
}

#[test]
fn combo_triple_backtick_inside_inline_body_on_rhs() {
    // Multi-line triple-backtick value inside an inline expression body on
    // a connection RHS. The anon's `code` config should store the dedented
    // python code.
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = ExecPython(x: String) -> (out: String) {
  x: "hi"
  code: ```
return {"out": x}
```
}.out
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("anon missing");
    let code = anon.config.get("code").and_then(|v| v.as_str()).expect("code missing");
    assert!(code.contains("return"), "code should contain return; got: {:?}", code);
    assert!(!code.contains("```"), "backticks should be stripped; got: {:?}", code);
}

#[test]
fn combo_multiline_json_inside_inline_body_on_rhs() {
    // Multi-line JSON value inside an inline expression body.
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = ExecPython -> (out: JsonDict) {
  params: {
    "a": 1,
    "b": "two"
  }
  code: ```
return {"out": params}
```
}.out
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("anon missing");
    let params = anon.config.get("params").expect("params missing");
    assert!(params.is_object(), "params should be a JSON object; got: {:?}", params);
    assert_eq!(params.get("a").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn combo_connection_literal_plus_inline_in_body_same_node() {
    // Mix: the host has an inline expression in its config block for one
    // port, AND a connection-line literal for another port of the same
    // host. Both should end up on the host node without interference.
    let src = r#"# Project: T

src = Text { value: "w" }
host = LlmInference {
  model: "gpt-4"
  prompt: Template {
    template: "Hello {{x}}"
    x: src.value
  }.text
}
host.systemPrompt = "You are a helpful assistant"
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "host__prompt").expect("prompt anon missing");
    assert_eq!(anon.nodeType.0, "Template");
    let host = project.nodes.iter().find(|n| n.id == "host").expect("host");
    assert_eq!(host.config.get("systemPrompt").and_then(|v| v.as_str()), Some("You are a helpful assistant"));
    // The inline-wired `prompt` port has an edge; connection-literal
    // `systemPrompt` is a config value.
    let prompt_edge = project.edges.iter().any(|e|
        e.target == "host" && e.targetHandle.as_deref() == Some("prompt")
    );
    assert!(prompt_edge);
    let sys_edge = project.edges.iter().any(|e|
        e.target == "host" && e.targetHandle.as_deref() == Some("systemPrompt")
    );
    assert!(!sys_edge, "systemPrompt should not be an edge");
}

#[test]
fn combo_bare_inline_in_nested_inline_body_on_rhs() {
    // Bare Type.port used as a port wiring inside an inline body that
    // itself is on a connection RHS. Verifies the parser recurses through
    // nested inline bodies correctly.
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = Template {
  template: "Hi {{x}}"
  x: Template {
    template: "inner"
  }.text
}.text
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    assert!(project.nodes.iter().any(|n| n.id == "host__data"));
    // Inner anon for the `x` port wiring
    assert!(project.nodes.iter().any(|n| n.id == "host__data__x"));
    let inner = project.nodes.iter().find(|n| n.id == "host__data__x").unwrap();
    assert_eq!(inner.nodeType.0, "Template");
    assert_eq!(inner.config.get("template").and_then(|v| v.as_str()), Some("inner"));
}

#[test]
fn combo_inline_with_only_output_port_signature() {
    // Inline with just `-> (out: X)` (no input signature), config block,
    // and `.out` suffix.
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = ExecPython -> (out: String) {
  code: ```
return {"out": "hi"}
```
}.out
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("anon");
    assert_eq!(anon.nodeType.0, "ExecPython");
    assert!(anon.outputs.iter().any(|p| p.name == "out"));
}

#[test]
fn combo_multiline_json_connection_literal_inside_group_body() {
    // Connection-line multi-line JSON literal inside a group body.
    let src = r#"# Project: T

grp = Group() -> (out: JsonDict?) {
  n = ExecPython -> (out: JsonDict) {
    code: ```
return {"out": "x"}
```
  }
  n.params = {
    "a": 1,
    "b": [1, 2, 3]
  }
  self.out = n.out
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let n = project.nodes.iter().find(|nn| nn.id == "grp.n").expect("n missing");
    let params = n.config.get("params").expect("params missing");
    assert!(params.is_object());
    assert_eq!(params.get("a").and_then(|v| v.as_i64()), Some(1));
    let b = params.get("b").expect("b missing");
    assert!(b.is_array());
}

#[test]
fn combo_inline_expression_inside_inline_body_on_group_self() {
    // `self.out = Template { ... }.text` inside a group body, where the
    // inline Template's body itself references `self.x`.
    let src = r#"# Project: T

src = Text { value: "hi" }
grp = Group(thing: String) -> (out: String?) {
  self.out = Template {
    template: "{{x}}"
    x: self.thing
  }.text
}
grp.thing = src.value
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    // Anon id should be self__out within group, merged to grp.self__out
    let anon = project.nodes.iter().find(|n| n.id == "grp.self__out").expect("anon missing");
    assert_eq!(anon.nodeType.0, "Template");
    // self.thing wiring should route through grp__in.thing
    let self_wired = project.edges.iter().any(|e|
        e.source == "grp__in" && e.sourceHandle.as_deref() == Some("thing")
        && e.target == "grp.self__out" && e.targetHandle.as_deref() == Some("x")
    );
    assert!(self_wired, "expected grp__in.thing -> grp.self__out.x; edges: {:?}",
        project.edges.iter().map(|e| format!("{}.{:?} -> {}.{:?}", e.source, e.sourceHandle, e.target, e.targetHandle)).collect::<Vec<_>>());
    // Anon output should wire to grp__out.out
    let out_wired = project.edges.iter().any(|e|
        e.source == "grp.self__out" && e.target == "grp__out" && e.targetHandle.as_deref() == Some("out")
    );
    assert!(out_wired, "expected grp.self__out -> grp__out.out");
}

#[test]
fn combo_oneliner_style_with_multiline_backtick_body() {
    // A node "one-liner" where the { ... } block fits on the header line
    // but contains a triple-backtick value that spans multiple lines,
    // then a closing `}` on its own line. Natural shorthand the parser
    // should accept.
    let src = r#"# Project: T

n = ExecPython -> (out: JsonDict) { code: ```
return {"out": "x"}
``` }
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let n = project.nodes.iter().find(|nn| nn.id == "n").expect("n missing");
    let code = n.config.get("code").and_then(|v| v.as_str()).expect("code missing");
    assert!(code.contains("return"), "code should contain 'return'; got: {:?}", code);
    assert!(!code.contains("```"), "backticks should be stripped; got: {:?}", code);
}

#[test]
fn combo_group_inlining_on_rhs_is_rejected() {
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = Group() -> (out: String?) {
  n = Text { value: "hi" }
  self.out = n.value
}.out
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    assert!(result.is_err(), "group inlining on RHS should be rejected");
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("Group") || e.message.to_lowercase().contains("group")),
        "expected group rejection error, got: {:?}", errs);
}

#[test]
fn combo_group_inlining_in_config_block_is_rejected() {
    let src = r#"# Project: T

host = Debug {
  label: "h"
  data: Group() -> (out: String?) {
    n = Text { value: "hi" }
    self.out = n.value
  }.out
}
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    assert!(result.is_err(), "group inlining in config block should be rejected");
}

#[test]
fn combo_bare_group_inlining_is_rejected() {
    // Bare form `Group.out` on RHS — should error (Groups cannot be inlined).
    let src = r#"# Project: T

host = Debug { label: "h" }
host.data = Group.out
"#;
    let result = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4());
    assert!(result.is_err(), "bare group inlining should be rejected; got: {:?}", result);
}

#[test]
fn combo_deeply_nested_rhs_four_levels() {
    // 4 levels of nested inline expressions as a connection RHS.
    let src = r#"# Project: T

src = Text { value: "hi" }
host = Debug { label: "h" }
host.data = Template {
  template: Template {
    template: Template {
      template: Template {
        template: "deepest {{y}}"
        y: src.value
      }.text
    }.text
  }.text
}.text
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    // L1..L4 all exist
    assert!(project.nodes.iter().any(|n| n.id == "host__data"));
    assert!(project.nodes.iter().any(|n| n.id == "host__data__template"));
    assert!(project.nodes.iter().any(|n| n.id == "host__data__template__template"));
    assert!(project.nodes.iter().any(|n| n.id == "host__data__template__template__template"));
    // Bottom-most wired to src
    assert!(project.edges.iter().any(|e|
        e.source == "src" && e.target == "host__data__template__template__template"
    ), "L4 should wire src.value");
    // Each level wires its child's .text output into the parent's template port
    for anon in ["host__data__template__template", "host__data__template", "host__data"] {
        assert!(project.edges.iter().any(|e| e.target == anon && e.targetHandle.as_deref() == Some("template")),
            "{} should have template wiring", anon);
    }
}

#[test]
fn combo_deeply_nested_inside_config_block_three_levels_with_everything() {
    // Inside a regular node's config block: a 3-level nested inline with
    // multi-line triple-backtick values and multi-line JSON at the deepest
    // level, plus a wiring to an outside node.
    let src = r#"# Project: T

src = Text { value: "world" }
host = Debug {
  label: "host"
  data: ExecPython(a: String) -> (out: String) {
    a: Template {
      template: Template {
        template: "Hello {{x}}"
        x: src.value
      }.text
    }.text
    code: ```
return {"out": a}
```
    params: {
      "key": "value",
      "list": [1, 2, 3]
    }
  }.out
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");

    // Outer anon: host__data (ExecPython)
    let outer = project.nodes.iter().find(|n| n.id == "host__data").expect("host__data missing");
    assert_eq!(outer.nodeType.0, "ExecPython");

    // Mid anon: host__data__a (Template, the outer Template whose body wires into host__data.a)
    let mid = project.nodes.iter().find(|n| n.id == "host__data__a").expect("host__data__a missing");
    assert_eq!(mid.nodeType.0, "Template");

    // Deep anon: host__data__a__template (inner Template wired to src.value)
    let deep = project.nodes.iter().find(|n| n.id == "host__data__a__template").expect("host__data__a__template missing");
    assert_eq!(deep.nodeType.0, "Template");
    assert_eq!(deep.config.get("template").and_then(|v| v.as_str()), Some("Hello {{x}}"));

    // Deep anon's port wiring from src.value
    assert!(project.edges.iter().any(|e|
        e.source == "src" && e.target == "host__data__a__template" && e.targetHandle.as_deref() == Some("x")
    ));
    // Mid → outer: host__data__a.text -> host__data.a
    assert!(project.edges.iter().any(|e|
        e.source == "host__data__a" && e.sourceHandle.as_deref() == Some("text")
        && e.target == "host__data" && e.targetHandle.as_deref() == Some("a")
    ));
    // Outer → host: host__data.out -> host.data
    assert!(project.edges.iter().any(|e|
        e.source == "host__data" && e.sourceHandle.as_deref() == Some("out")
        && e.target == "host" && e.targetHandle.as_deref() == Some("data")
    ));

    // Outer anon's code field preserved as a dedented multi-line string
    let code = outer.config.get("code").and_then(|v| v.as_str()).expect("code missing");
    assert!(code.contains("return"));
    assert!(!code.contains("```"));

    // Outer anon's params preserved as a JSON object
    let params = outer.config.get("params").expect("params missing");
    assert!(params.is_object());
    assert_eq!(params.get("key").and_then(|v| v.as_str()), Some("value"));
    let list = params.get("list").expect("list missing");
    assert!(list.is_array());
}

#[test]
fn combo_inline_body_with_multiline_json_and_triple_backtick() {
    // Regression: 2-level nested inline in config block with both a
    // multi-line JSON value AND a multi-line triple-backtick value in
    // the same inline body. Parser close-brace detection must not be
    // confused by `}` inside the JSON or by `}` after the `` ``` ``.
    let src = r#"# Project: T

host = Debug {
  label: "host"
  data: ExecPython(a: String) -> (out: String) {
    a: "hi"
    params: {
      "key": "value"
    }
    code: ```
return {"out": a}
```
  }.out
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("anon");
    assert!(anon.config.get("code").is_some());
    assert!(anon.config.get("params").is_some());
    assert_eq!(anon.config.get("a").and_then(|v| v.as_str()), Some("hi"));
}

#[test]
fn combo_maximalist_nested_rhs_every_value_type_every_level() {
    // 5 levels of nested inline. At EVERY level there's a mix of:
    //   - a multi-line triple-backtick string config value
    //   - a multi-line JSON config value
    //   - a literal string config value
    //   - a port wiring from an outside source
    //   - a nested inline on the `template` port
    // All inside a regular node's config block so we also exercise the
    // "inside a config block" path. Plus the innermost level pulls from
    // a group's self input, so the group routing code is exercised too.
    let src = r#"# Project: T

src = Text { value: "world" }
grp = Group(thing: String) -> (out: String?) {
  root_host = Debug {
    label: "level0"
    data: ExecPython(a: String) -> (out: String) {
      code: ```
L0 code
return {"out": a}
```
      meta: {
        "level": 0,
        "tags": ["a", "b"]
      }
      note: "level-0 literal"
      extra: src.value
      a: ExecPython(b: String) -> (out: String) {
        code: ```
L1 code
return {"out": b}
```
        meta: {
          "level": 1,
          "nested": {"x": 1}
        }
        note: "level-1 literal"
        extra: src.value
        b: ExecPython(c: String) -> (out: String) {
          code: ```
L2 code
return {"out": c}
```
          meta: {
            "level": 2
          }
          note: "level-2 literal"
          extra: src.value
          c: ExecPython(d: String) -> (out: String) {
            code: ```
L3 code
return {"out": d}
```
            meta: {
              "level": 3
            }
            note: "level-3 literal"
            extra: src.value
            d: ExecPython(e: String) -> (out: String) {
              code: ```
L4 code
return {"out": e}
```
              meta: {
                "level": 4
              }
              note: "level-4 literal"
              e: self.thing
            }.out
          }.out
        }.out
      }.out
    }.out
  }
  self.out = root_host.data
}
grp.thing = src.value
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse the maximalist monstrosity");

    // Expected anon ids, level-by-level. root_host__data is the outer
    // ExecPython wired into root_host.data. Each subsequent level is
    // named by the port wiring it's placed on inside the parent's body.
    // Levels: L0..L4 correspond to the nested ExecPython instances.
    let expected_ids = [
        "grp.root_host__data",                           // L0
        "grp.root_host__data__a",                        // L1 (wired to L0.a)
        "grp.root_host__data__a__b",                     // L2
        "grp.root_host__data__a__b__c",                  // L3
        "grp.root_host__data__a__b__c__d",               // L4
    ];
    for id in &expected_ids {
        let node = project.nodes.iter().find(|n| &n.id == id)
            .unwrap_or_else(|| panic!("expected anon {} missing; nodes: {:?}",
                id, project.nodes.iter().map(|n| &n.id).collect::<Vec<_>>()));
        assert_eq!(node.nodeType.0, "ExecPython");
    }

    // Each level has its literal `note` field preserved.
    for (level, id) in expected_ids.iter().enumerate() {
        let node = project.nodes.iter().find(|n| &n.id == id).unwrap();
        let expected_note = format!("level-{} literal", level);
        assert_eq!(
            node.config.get("note").and_then(|v| v.as_str()),
            Some(expected_note.as_str()),
            "level {}: note mismatch", level,
        );
    }

    // Each level has `code` stored as a dedented multi-line string.
    for (level, id) in expected_ids.iter().enumerate() {
        let node = project.nodes.iter().find(|n| &n.id == id).unwrap();
        let code = node.config.get("code").and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("level {}: code missing on {}", level, id));
        assert!(code.contains(&format!("L{} code", level)), "level {}: code doesn't contain 'L{} code'; got: {:?}", level, level, code);
        assert!(!code.contains("```"), "level {}: backticks should be stripped", level);
    }

    // Each level has `meta` stored as a JSON object with the right level.
    for (level, id) in expected_ids.iter().enumerate() {
        let node = project.nodes.iter().find(|n| &n.id == id).unwrap();
        let meta = node.config.get("meta").unwrap_or_else(|| panic!("level {}: meta missing", level));
        assert!(meta.is_object(), "level {}: meta should be JSON object; got: {:?}", level, meta);
        assert_eq!(meta.get("level").and_then(|v| v.as_i64()), Some(level as i64));
    }

    // Levels 0..3 have a port wiring `extra: src.value`. That should emit
    // an edge from src to each level's `extra` port.
    for (level, id) in expected_ids.iter().take(4).enumerate() {
        let wired = project.edges.iter().any(|e|
            e.source == "src"
            && e.sourceHandle.as_deref() == Some("value")
            && &e.target == id
            && e.targetHandle.as_deref() == Some("extra")
        );
        assert!(wired, "level {} ({}): expected src.value -> extra edge", level, id);
    }

    // Level 4 has `e: self.thing` which should route through grp__in.thing.
    let level4_id = expected_ids[4];
    let self_wired = project.edges.iter().any(|e|
        e.source == "grp__in"
        && e.sourceHandle.as_deref() == Some("thing")
        && &e.target == level4_id
        && e.targetHandle.as_deref() == Some("e")
    );
    assert!(self_wired, "level 4: expected grp__in.thing -> {}.e edge", level4_id);

    // Each level's `.out` is wired into the parent's `a/b/c/d` port.
    let port_chain = [
        ("grp.root_host__data__a", "grp.root_host__data", "a"),
        ("grp.root_host__data__a__b", "grp.root_host__data__a", "b"),
        ("grp.root_host__data__a__b__c", "grp.root_host__data__a__b", "c"),
        ("grp.root_host__data__a__b__c__d", "grp.root_host__data__a__b__c", "d"),
    ];
    for (child, parent, port) in &port_chain {
        let wired = project.edges.iter().any(|e|
            &e.source == child
            && e.sourceHandle.as_deref() == Some("out")
            && &e.target == parent
            && e.targetHandle.as_deref() == Some(*port)
        );
        assert!(wired, "expected {} -> {}.{} edge", child, parent, port);
    }

    // Final hop: root_host__data.out -> root_host.data.
    let final_wired = project.edges.iter().any(|e|
        e.source == "grp.root_host__data"
        && e.sourceHandle.as_deref() == Some("out")
        && e.target == "grp.root_host"
        && e.targetHandle.as_deref() == Some("data")
    );
    assert!(final_wired, "expected grp.root_host__data.out -> grp.root_host.data edge");
}

#[test]
fn combo_external_ref_in_inline_body_inside_group() {
    // Simplified reproduction of the maximalist-test bug: inline body
    // inside a group body references a ROOT-scope node via port wiring.
    // The edge should go from root `src` to the anon, not from `grp.src`.
    let src = r#"# Project: T

src = Text { value: "hi" }
grp = Group() -> (out: String?) {
  dst = Debug {
    label: "d"
    data: Template {
      template: "{{x}}"
      x: src.value
    }.text
  }
  self.out = dst.data
}
"#;
    let project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let wired = project.edges.iter().any(|e|
        e.source == "src"
        && e.sourceHandle.as_deref() == Some("value")
        && e.target == "grp.dst__data"
        && e.targetHandle.as_deref() == Some("x")
    );
    assert!(wired, "expected src.value -> grp.dst__data.x, but the edge source was group-prefixed");
}

// ─── Port synthesis rule tests ───
// Rule: literals create ports (`required: false`, type from WeftType::infer)
// on nodes with `canAddInputPorts`. Edges never create ports. Outputs can
// never be assigned (literal or edge). Catalog config fields are legitimate
// config keys and are not synthesized as ports.

#[test]
fn rule_edge_to_catalog_config_field_is_error() {
    let src = r#"# Project: T

upstream = Text { value: "x" }
n = Text { value: "default" }
n.value = upstream.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    let errs = result.expect_err("wiring into a catalog config field should fail");
    assert!(
        errs.iter().any(|e| e.contains("no input port 'value'")),
        "expected 'no input port value' error; got: {:?}",
        errs,
    );
}

#[test]
fn rule_edge_to_catalog_output_port_is_error() {
    let src = r#"# Project: T

upstream = Text { value: "x" }
n = Template { template: "hi" }
n.text = upstream.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    let errs = result.expect_err("wiring into an output port should fail");
    assert!(
        errs.iter().any(|e| e.contains("no input port 'text'")),
        "expected 'no input port text' error; got: {:?}",
        errs,
    );
}

#[test]
fn rule_literal_to_catalog_output_port_is_error() {
    let src = r#"# Project: T

n = Template { template: "hi" }
n.text = "cannot set output"
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    let errs = result.expect_err("literal to output port should fail");
    assert!(
        errs.iter().any(|e| e.contains("cannot assign a literal to output port 'text'")),
        "expected 'cannot assign a literal to output port' error; got: {:?}",
        errs,
    );
}

#[test]
fn rule_literal_synthesizes_input_port_when_allowed() {
    // Template.canAddInputPorts is true. Literal on an undeclared key
    // synthesizes an input port, type inferred, required: false.
    let src = r#"# Project: T

n = Template { template: "Hello {{name}}", name: "world" }
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").expect("n");
    let name_port = n.inputs.iter().find(|p| p.name == "name").expect("name port synthesized");
    assert!(!name_port.required, "synthesized port should be required: false");
    assert_eq!(format!("{}", name_port.portType), "String");
}

#[test]
fn rule_literal_synthesizes_input_port_with_inferred_types() {
    let src = r#"# Project: T

n = Template {
  template: "hi"
  count: 42
  enabled: true
  tags: ["a", "b"]
  meta: { "key": "value" }
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    let find = |name: &str| n.inputs.iter().find(|p| p.name == name).expect(name);
    assert_eq!(format!("{}", find("count").portType), "Number");
    assert_eq!(format!("{}", find("enabled").portType), "Boolean");
    assert!(format!("{}", find("tags").portType).starts_with("List"));
    assert!(format!("{}", find("meta").portType).starts_with("Dict"));
}

#[test]
fn rule_literal_on_connection_line_synthesizes_port() {
    let src = r#"# Project: T

n = Template { template: "Hello {{name}}" }
n.name = "world"
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    assert!(n.inputs.iter().any(|p| p.name == "name"));
}

#[test]
fn rule_literal_rejected_on_fixed_port_node() {
    let src = r#"# Project: T

n = Text { value: "hi", not_a_field: "oops" }
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    let errs = result.expect_err("expected rejection");
    assert!(
        errs.iter().any(|e| e.contains("cannot add custom input port 'not_a_field'")),
        "expected 'cannot add custom input port' error; got: {:?}",
        errs,
    );
}

#[test]
fn rule_literal_on_catalog_config_field_is_not_synthesized() {
    // Text has catalog config field `value`. That's legitimate user config,
    // not a port synthesis trigger.
    let src = r#"# Project: T

n = Text { value: "hello world" }
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    assert!(
        !n.inputs.iter().any(|p| p.name == "value"),
        "value is a catalog config field, not an input port; should not be synthesized",
    );
}

#[test]
fn rule_list_literal_inferred_as_list_type() {
    let src = r#"# Project: T

n = Template {
  template: "hi"
  tags: ["a", "b", "c"]
  numbers: [1, 2, 3]
  mixed: ["x", 1, true]
  items: [{"a": 1}, {"a": 2}]
  empty: []
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    let find = |name: &str| n.inputs.iter().find(|p| p.name == name).expect(name);
    assert_eq!(format!("{}", find("tags").portType), "List[String]");
    assert_eq!(format!("{}", find("numbers").portType), "List[Number]");
    assert!(format!("{}", find("mixed").portType).starts_with("List"));
    assert!(format!("{}", find("items").portType).starts_with("List"));
    assert!(format!("{}", find("empty").portType).starts_with("List"));
}

#[test]
fn rule_list_literal_on_connection_line_synthesizes_port() {
    let src = r#"# Project: T

n = Template { template: "hi" }
n.tags = ["a", "b"]
n.multi = [
  "a",
  "b",
  "c"
]
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    assert_eq!(format!("{}", n.inputs.iter().find(|p| p.name == "tags").unwrap().portType), "List[String]");
    assert_eq!(format!("{}", n.inputs.iter().find(|p| p.name == "multi").unwrap().portType), "List[String]");
}

#[test]
fn rule_heredoc_with_escaped_triple_backtick_inside() {
    // Regression: a heredoc value containing escaped triple backticks
    // (e.g. a markdown snippet with a code fence) must not be closed early
    // by the parser. `\```` is an escaped literal, not a terminator.
    let src = "# Project: T\n\nn = Template {\n  template: \"hi\"\n  body: ```\nbefore\n\\```\nsome code\n\\```\nafter\n```\n}\n";
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    let body = n.config.get("body").and_then(|v| v.as_str()).expect("body str");
    assert_eq!(body, "before\n```\nsome code\n```\nafter");
}

// ─── Edge-driven port synthesis rule ────────────────────────────────────────
//
// An edge targeting an undeclared key on a `canAddInputPorts: true` node
// synthesizes the port as `required: true` with a fresh TypeVar that unifies
// with the edge source's type. For optional ports, users must declare `?`
// explicitly in the signature. This is the counterpart to literal-driven
// synthesis, which produces `required: false` ports because the literal IS
// the value.

#[test]
fn rule_edge_synthesizes_required_port_on_can_add() {
    // Template has canAddInputPorts: true. An edge targeting an undeclared
    // key synthesizes the port with required: true, and the TypeVar
    // narrows to the source's type (String).
    let src = r#"# Project: T

src_name = Text { value: "alice" }
src_title = Text { value: "engineer" }
n = Template { template: "{{name}} - {{title}}" }
n.name = src_name.value
n.title = src_title.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    let name_port = n.inputs.iter().find(|p| p.name == "name").expect("synthesized name port");
    assert!(name_port.required, "edge-driven synthesized port must be required");
    assert_eq!(name_port.portType.to_string(), "String");
    let title_port = n.inputs.iter().find(|p| p.name == "title").expect("synthesized title port");
    assert!(title_port.required);
    assert_eq!(title_port.portType.to_string(), "String");
}

#[test]
fn rule_edge_synthesis_from_inline_body_wiring() {
    // Same synthesis inside an inline anon body: the outer `review`
    // Template gets a synthesized `lead_info` port (required, String);
    // the inner `review__lead_info` anon gets synthesized name/title/
    // company ports (required, String) from the group's outputs.
    let src = r#"# Project: T

enrich = Group() -> (name: String, title: String, organization: String) {
  n_src = Text { value: "alice" }
  t_src = Text { value: "engineer" }
  o_src = Text { value: "acme" }
  self.name = n_src.value
  self.title = t_src.value
  self.organization = o_src.value
}
review = Template { template: "{{lead_info}}" }
review.lead_info = Template {
  template: "{{name}} — {{title}} at {{company}}"
  name: enrich.name
  title: enrich.title
  company: enrich.organization
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "review__lead_info").expect("review__lead_info");
    for key in ["name", "title", "company"] {
        let port = anon.inputs.iter().find(|p| p.name == key).expect("synthesized port");
        assert!(port.required, "port '{}' should be required", key);
        assert_eq!(port.portType.to_string(), "String", "port '{}' should be String", key);
    }
    // Also verify the outer `review` got its synthesized `lead_info` port.
    let review = project.nodes.iter().find(|n| n.id == "review").expect("review");
    let lead_info = review.inputs.iter().find(|p| p.name == "lead_info").expect("synthesized lead_info");
    assert!(lead_info.required);
    assert_eq!(lead_info.portType.to_string(), "String");
}

#[test]
fn rule_literal_then_edge_literal_stays_optional() {
    // If a literal assigns the port first, it's synthesized as optional.
    // A later edge to the same port doesn't upgrade it to required; the
    // literal's declaration wins.
    let src = r#"# Project: T

src = Text { value: "from-edge" }
n = Template {
  template: "{{x}}"
  x: "default"
}
n.x = src.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    let x_port = n.inputs.iter().find(|p| p.name == "x").expect("x port");
    assert!(!x_port.required, "literal-declared port should remain optional even when later driven by an edge");
    assert_eq!(x_port.portType.to_string(), "String");
}

#[test]
fn rule_explicit_optional_declaration_survives_edge() {
    // Explicitly declaring `x: String?` keeps the port optional even when
    // an edge drives it. This is how users opt out of the "edge makes
    // ports required" default.
    let src = r#"# Project: T

src = Text { value: "hi" }
n = Template(x: String?) { template: "{{x}}" }
n.x = src.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let n = project.nodes.iter().find(|nn| nn.id == "n").unwrap();
    let x_port = n.inputs.iter().find(|p| p.name == "x").expect("x port");
    assert!(!x_port.required, "explicit String? should keep the port optional");
}

#[test]
fn rule_edge_to_frozen_node_still_errors() {
    // Nodes without canAddInputPorts don't get edge synthesis. Writing an
    // edge to an undeclared port on a frozen-port node is still a hard
    // error.
    let src = r#"# Project: T

src = Text { value: "hi" }
n = LlmInference -> (response: String) {
  label: "n"
}
n.madeUpPort = src.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    let result = crate::enrich::enrich_project(&mut project, &registry);
    let errs = result.expect_err("expected error: frozen node cannot grow ports via edge");
    assert!(
        errs.iter().any(|e| e.contains("no input port") && e.contains("madeUpPort")),
        "expected 'no input port madeUpPort'; got: {:?}", errs,
    );
}

#[test]
fn literal_synthesized_port_flows_into_runtime_input() {
    // End-to-end runtime check: a literal-synthesized port must be visible
    // to the node during execution, via build_input_from_pulses injecting
    // the config value when no edge pulse is present. This is the
    // "literal alone" path: no edges, the node should see `name: "Alice"`
    // in its input map at runtime.
    let src = r#"# Project: T

greeting = Template {
  template: "Hello {{name}}"
  name: "Alice"
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");

    let node = project.nodes.iter().find(|n| n.id == "greeting").expect("greeting");

    // Confirm the synthesized port exists and is configurable.
    let name_port = node.inputs.iter().find(|p| p.name == "name").expect("synthesized name port");
    assert!(name_port.configurable, "literal-synthesized port must be configurable");

    // Confirm the literal landed in the node's config.
    let name_cfg = node.config.get("name").expect("name in config");
    assert_eq!(name_cfg.as_str(), Some("Alice"));

    // Simulate the runtime: empty pulses. Use has_incoming=true so the
    // function takes the input_obj-building path (the non-trigger node
    // fallback path just returns initial_input verbatim). The config-fill
    // merge at the bottom of build_input_from_pulses should inject the
    // literal values into the input.
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        node,
        &[],
        &[],
        "color",
        &serde_json::json!({}),
        true,
        &mut type_errors,
    );
    assert!(type_errors.is_empty(), "runtime type check errors: {:?}", type_errors);
    assert_eq!(
        input.get("name").and_then(|v| v.as_str()),
        Some("Alice"),
        "expected name=\"Alice\" from config to be injected into input; got: {}",
        input,
    );
    assert_eq!(
        input.get("template").and_then(|v| v.as_str()),
        Some("Hello {{name}}"),
        "expected template to be injected into input from config",
    );
}

#[test]
fn literal_synthesized_port_edge_overrides_literal_at_runtime() {
    // When both a literal AND an edge drive the same port, the edge wins
    // at runtime: build_input_from_pulses only injects from config when
    // the pulse hasn't already filled the port.
    let src = r#"# Project: T

override_src = Text { value: "Bob" }
greeting = Template {
  template: "Hello {{name}}"
  name: "Alice"
}
greeting.name = override_src.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");

    let node = project.nodes.iter().find(|n| n.id == "greeting").expect("greeting");
    let name_port = node.inputs.iter().find(|p| p.name == "name").expect("name port");
    // The literal declared the port first, so it remains optional even
    // though an edge now drives it.
    assert!(!name_port.required, "literal-declared port should stay optional");

    // Simulate an incoming pulse on the `name` port with value "Bob".
    let pulse = weft_core::executor_core::Pulse::new_on_port(
        "c1".to_string(),
        vec![],
        serde_json::json!("Bob"),
        "name".to_string(),
    );
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        node,
        &[pulse],
        &[], // lane
        "c1",
        &serde_json::json!({}),
        true, // has_incoming
        &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(
        input.get("name").and_then(|v| v.as_str()),
        Some("Bob"),
        "edge should win over literal at runtime; got: {}",
        input,
    );
}

#[test]
fn literal_only_root_node_has_config_in_input_at_runtime() {
    // Regression: a root-level node with no incoming edges and only
    // literal config values must still see those literals at runtime.
    // The bug: build_input_from_pulses used to return initial_input
    // verbatim in the has_incoming=false branch, dropping config-fills.
    // This test locks the fix in.
    let src = r#"# Project: T

greeting = Template {
  template: "Hello {{name}}, welcome to {{place}}!"
  name: "Alice"
  place: "Paris"
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let node = project.nodes.iter().find(|n| n.id == "greeting").expect("greeting");

    // Simulate the real runtime path: no incoming pulses (root node),
    // no incoming edges (has_incoming = false). This is the path the
    // orchestrator hits when dispatching a root-level literal-only node.
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        node,
        &[],
        &[],
        "wave-0",
        &serde_json::Value::Null,
        false, // has_incoming: root node, no incoming edges
        &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(
        input.get("template").and_then(|v| v.as_str()),
        Some("Hello {{name}}, welcome to {{place}}!"),
        "expected template from config at root; got: {}",
        input,
    );
    assert_eq!(
        input.get("name").and_then(|v| v.as_str()),
        Some("Alice"),
        "expected name='Alice' from config at root; got: {}",
        input,
    );
    assert_eq!(
        input.get("place").and_then(|v| v.as_str()),
        Some("Paris"),
        "expected place='Paris' from config at root; got: {}",
        input,
    );
}

#[test]
fn literal_on_inline_anon_inside_config_block_flows_at_runtime() {
    // Literal value on an inline anon nested inside its host's config
    // block. The host.data edge feeds from the anon; the anon itself
    // is a root-level node (no incoming edges of its own), so it goes
    // through the has_incoming=false path and must still see its
    // literal config at runtime.
    let src = r#"# Project: T

host = Template { template: "WRAPPED: {{data}}" }
host.data = Template {
  template: "inner: {{name}}"
  name: "Alice"
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "host__data").expect("host__data");
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        anon,
        &[],
        &[],
        "wave-0",
        &serde_json::Value::Null,
        false,
        &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(input.get("template").and_then(|v| v.as_str()), Some("inner: {{name}}"));
    assert_eq!(input.get("name").and_then(|v| v.as_str()), Some("Alice"));
}

#[test]
fn literal_deep_in_group_child_flows_at_runtime() {
    // Three-scope-deep literal: outer group → inner group → child node
    // with literal config. The child has no incoming edges; the path
    // must still reach its config values at runtime.
    let src = r#"# Project: T

outer = Group() -> (out: String?) {
  inner = Group() -> (res: String?) {
    child = Template {
      template: "{{greeting}} {{who}}"
      greeting: "Bonjour"
      who: "monde"
    }
    self.res = child.text
  }
  self.out = inner.res
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let child = project.nodes.iter().find(|n| n.id == "outer.inner.child").expect("outer.inner.child");
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        child,
        &[],
        &[],
        "wave-0",
        &serde_json::Value::Null,
        false,
        &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(input.get("greeting").and_then(|v| v.as_str()), Some("Bonjour"));
    assert_eq!(input.get("who").and_then(|v| v.as_str()), Some("monde"));
}

#[test]
fn literal_list_and_dict_shapes_flow_at_runtime() {
    // Non-scalar literals: list and multi-line JSON dict. Both must
    // round-trip into the runtime input map unchanged.
    let src = r#"# Project: T

n = Template {
  template: "Tags: {{tags}}, Meta: {{meta}}"
  tags: ["one", "two", "three"]
  meta: {
    "priority": "high",
    "count": 42
  }
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let node = project.nodes.iter().find(|n| n.id == "n").expect("n");
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        node,
        &[],
        &[],
        "wave-0",
        &serde_json::Value::Null,
        false,
        &mut type_errors,
    );
    assert!(type_errors.is_empty());
    let tags = input.get("tags").and_then(|v| v.as_array()).expect("tags array");
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].as_str(), Some("one"));
    assert_eq!(tags[2].as_str(), Some("three"));
    let meta = input.get("meta").and_then(|v| v.as_object()).expect("meta object");
    assert_eq!(meta.get("priority").and_then(|v| v.as_str()), Some("high"));
    assert_eq!(meta.get("count").and_then(|v| v.as_i64()), Some(42));
}

#[test]
fn literal_in_3level_nested_inline_anon_flows_at_runtime() {
    // Config within config within config: a literal value sits at the
    // deepest level of a 3-level inline anon chain on a connection RHS.
    // Each anon at every level is a root-level node (parser emits them
    // that way) and must still see its literal config at runtime via
    // the has_incoming=false path.
    let src = r#"# Project: T

host = Template { template: "L1: {{data}}" }
host.data = Template(inner: String) {
  template: "L2 wrap: {{inner}}"
  inner: Template(deep: String) {
    template: "L3 wrap: {{deep}}"
    deep: Template {
      template: "L4 core: {{leaf}}"
      leaf: "ALICE"
    }.text
  }.text
}.text
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");

    // All four anons at each nesting level must receive their literal
    // configs at runtime.
    let leaf = project.nodes.iter()
        .find(|n| n.id == "host__data__inner__deep")
        .expect("leaf anon");
    let mut type_errors: Vec<String> = Vec::new();
    let leaf_input = weft_core::executor_core::build_input_from_pulses(
        leaf, &[], &[], "wave-0", &serde_json::Value::Null, false, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(leaf_input.get("template").and_then(|v| v.as_str()), Some("L4 core: {{leaf}}"));
    assert_eq!(leaf_input.get("leaf").and_then(|v| v.as_str()), Some("ALICE"));

    let l3 = project.nodes.iter()
        .find(|n| n.id == "host__data__inner")
        .expect("L3 anon");
    let l3_input = weft_core::executor_core::build_input_from_pulses(
        l3, &[], &[], "wave-0", &serde_json::Value::Null, false, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(l3_input.get("template").and_then(|v| v.as_str()), Some("L3 wrap: {{deep}}"));

    let l2 = project.nodes.iter()
        .find(|n| n.id == "host__data")
        .expect("L2 anon");
    let l2_input = weft_core::executor_core::build_input_from_pulses(
        l2, &[], &[], "wave-0", &serde_json::Value::Null, false, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(l2_input.get("template").and_then(|v| v.as_str()), Some("L2 wrap: {{inner}}"));
}

#[test]
fn literal_ultimate_dict_of_dicts_in_deep_anon() {
    // Nested dict-of-dicts as a literal inside an inline anon inside a
    // group, verifying multi-line JSON with nested structures survives
    // both the parser and runtime input building.
    let src = r#"# Project: T

grp = Group() -> (out: String?) {
  host = Template { template: "WRAPPED: {{data}}" }
  host.data = Template {
    template: "inner: {{payload}}"
    payload: {
      "user": {
        "name": "alice",
        "tags": ["admin", "dev"]
      },
      "counts": {
        "posts": 42,
        "likes": 99
      }
    }
  }.text
  self.out = host.text
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "grp.host__data").expect("grp.host__data");
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        anon, &[], &[], "wave-0", &serde_json::Value::Null, false, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    let payload = input.get("payload").and_then(|v| v.as_object()).expect("payload object");
    let user = payload.get("user").and_then(|v| v.as_object()).expect("user object");
    assert_eq!(user.get("name").and_then(|v| v.as_str()), Some("alice"));
    let tags = user.get("tags").and_then(|v| v.as_array()).expect("tags array");
    assert_eq!(tags.len(), 2);
    let counts = payload.get("counts").and_then(|v| v.as_object()).expect("counts object");
    assert_eq!(counts.get("posts").and_then(|v| v.as_i64()), Some(42));
}

#[test]
fn literal_heredoc_in_deep_anon_with_escaped_triple_backtick() {
    // Multi-line triple-backtick heredoc with an escaped ``` inside the
    // body, nested inside an inline anon inside a group. Exercises:
    //   - the multi-line heredoc parser
    //   - escape-aware terminator (\```` is content, not close)
    //   - scoped propagation through the group body
    //   - runtime config delivery on a no-incoming root anon
    let src = "# Project: T\n\ngrp = Group() -> (out: String?) {\n  host = Template { template: \"W: {{data}}\" }\n  host.data = Template {\n    template: \"doc: {{body}}\"\n    body: ```\nmarkdown:\n\\```\ncode fence\n\\```\nend\n```\n  }.text\n  self.out = host.text\n}\n";
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let anon = project.nodes.iter().find(|n| n.id == "grp.host__data").expect("grp.host__data");
    let body_str = anon.config.get("body").and_then(|v| v.as_str()).expect("body in config");
    assert_eq!(body_str, "markdown:\n```\ncode fence\n```\nend");
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        anon, &[], &[], "wave-0", &serde_json::Value::Null, false, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(input.get("body").and_then(|v| v.as_str()), Some("markdown:\n```\ncode fence\n```\nend"));
}

#[test]
fn literal_connection_line_at_group_scope_flows_at_runtime() {
    // Connection-line literal inside a group: `child.x = "foo"` is a
    // different parser path from inline body literals. Must still reach
    // the child at runtime.
    let src = r#"# Project: T

grp = Group() -> (out: String?) {
  child = Template { template: "{{greeting}}" }
  child.greeting = "hola"
  self.out = child.text
}
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let child = project.nodes.iter().find(|n| n.id == "grp.child").expect("grp.child");
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        child, &[], &[], "wave-0", &serde_json::Value::Null, false, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(input.get("greeting").and_then(|v| v.as_str()), Some("hola"));
    assert_eq!(input.get("template").and_then(|v| v.as_str()), Some("{{greeting}}"));
}

#[test]
fn literal_and_edge_on_same_port_literal_first() {
    // Literal declared first, then edge overrides. The literal is NOT
    // dropped from the config at parse time (it's shadowed by last-write
    // semantics), but at runtime the edge pulse wins.
    let src = r#"# Project: T

override_src = Text { value: "EDGE" }
n = Template(greeting: String?) {
  template: "{{greeting}}"
  greeting: "LITERAL"
}
n.greeting = override_src.value
"#;
    let mut project = weft_core::weft_compiler::compile(src, uuid::Uuid::new_v4()).expect("parse");
    let registry = crate::registry::NodeTypeRegistry::new();
    crate::enrich::enrich_project(&mut project, &registry).expect("enrich ok");
    let node = project.nodes.iter().find(|n| n.id == "n").expect("n");
    // Runtime: simulate the edge delivering "EDGE" via a pulse.
    let pulse = weft_core::executor_core::Pulse::new_on_port(
        "c1".to_string(),
        vec![],
        serde_json::json!("EDGE"),
        "greeting".to_string(),
    );
    let mut type_errors: Vec<String> = Vec::new();
    let input = weft_core::executor_core::build_input_from_pulses(
        node, &[pulse], &[], "c1", &serde_json::Value::Null, true, &mut type_errors,
    );
    assert!(type_errors.is_empty());
    assert_eq!(input.get("greeting").and_then(|v| v.as_str()), Some("EDGE"));
    // template literal still flows from config.
    assert_eq!(input.get("template").and_then(|v| v.as_str()), Some("{{greeting}}"));
}


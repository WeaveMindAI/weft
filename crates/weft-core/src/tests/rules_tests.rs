use serde_json::{json, Value};

use super::*;
use crate::member::{filled_node, member_filled_literal};

/// One node `n` with a String input `cron`, holding `literal` when given.
fn program(literal: Option<Value>) -> ProjectDefinition {
    let literals = match literal {
        Some(v) => json!({ "cron": v }),
        None => json!({}),
    };
    serde_json::from_value(json!({
        "id": uuid::Uuid::new_v4(),
        "nodes": [{
            "id": "n", "nodeType": "Cron", "label": null,
            "config": null, "position": { "x": 0.0, "y": 0.0 },
            "inputs": [{ "name": "cron", "portType": "String", "required": true }],
            "outputs": [{ "name": "out", "portType": "String", "required": false }],
            "features": {}, "scope": [], "groupBoundary": null,
            "requiresInfra": false, "images": [],
            "portLiterals": literals
        }],
        "edges": [],
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z"
    }))
    .expect("project")
}

fn rule(when: Value) -> ValidationRule {
    serde_json::from_value(json!({ "when": when, "then": { "message": "'{id}' {field} is bad", "field": "cron" } })).unwrap()
}

fn six_fields() -> ValidationRule {
    rule(json!({ "kind": "all", "of": [
        { "kind": "config_nonempty", "field": "cron" },
        { "kind": "not", "of": { "kind": "config_matches", "field": "cron", "regex": "^\\S+(\\s+\\S+){5}$" } }
    ]}))
}

fn fires_on(project: &ProjectDefinition, rule: &ValidationRule) -> bool {
    fires(rule, &project.nodes[0], project, &[])
}

#[test]
fn a_content_rule_fires_on_a_written_value_that_breaks_it() {
    assert!(fires_on(&program(Some(json!("0 3 * * *"))), &six_fields()));
    assert!(!fires_on(&program(Some(json!("0 0 3 * * *"))), &six_fields()));
}

/// A member-filled field with no fallback is there with an unknown value:
/// a presence rule holds, a content rule has no answer and does not fire.
#[test]
fn a_member_filled_field_is_present_with_an_unknown_content() {
    let p = program(Some(member_filled_literal(None)));
    assert!(!fires_on(&p, &six_fields()), "the content is not known yet");
    let missing = rule(json!({ "kind": "not", "of": { "kind": "config_nonempty", "field": "cron" } }));
    assert!(!fires_on(&p, &missing), "the member provides it");
    assert_eq!(evaluate(&six_fields().when, &p.nodes[0], &p, &[]), None);
}

/// A fallback is a known value, checked like a written one.
#[test]
fn a_fallback_is_checked_like_a_written_value() {
    assert!(fires_on(&program(Some(member_filled_literal(Some(json!("0 3 * * *"))))), &six_fields()));
    assert!(!fires_on(&program(Some(member_filled_literal(Some(json!("0 0 3 * * *"))))), &six_fields()));
}

/// The member's value swapped in is what the rule reads, and a member who
/// gave nothing leaves the field unwritten.
#[test]
fn a_members_value_is_checked_on_the_filled_node() {
    let p = program(Some(member_filled_literal(None)));
    let bad = filled_node(&p.nodes[0], Some(&[("cron".to_string(), json!("0 3 * * *"))].into()));
    assert!(fires(&six_fields(), &bad, &p, &[]));
    let good = filled_node(&p.nodes[0], Some(&[("cron".to_string(), json!("0 0 3 * * *"))].into()));
    assert!(!fires(&six_fields(), &good, &p, &[]));
    let none = filled_node(&p.nodes[0], None);
    assert!(none.port_literals.is_empty(), "no value and no fallback leaves the field unwritten");
    let missing = rule(json!({ "kind": "not", "of": { "kind": "config_nonempty", "field": "cron" } }));
    assert!(fires(&missing, &none, &p, &[]));
}

#[test]
fn unknown_parts_combine_three_ways() {
    let p = program(Some(member_filled_literal(None)));
    let known_false = json!({ "kind": "config_equals", "field": "missing", "equals": 1 });
    let known_true = json!({ "kind": "not", "of": known_false });
    let unknown = json!({ "kind": "config_equals", "field": "cron", "equals": "x" });
    let eval = |when: Value| evaluate(&rule(when).when, &p.nodes[0], &p, &[]);
    assert_eq!(eval(json!({ "kind": "all", "of": [known_false, unknown] })), Some(false));
    assert_eq!(eval(json!({ "kind": "all", "of": [known_true, unknown] })), None);
    assert_eq!(eval(json!({ "kind": "any", "of": [known_true, unknown] })), Some(true));
    assert_eq!(eval(json!({ "kind": "any", "of": [known_false, unknown] })), None);
    assert_eq!(eval(json!({ "kind": "not", "of": unknown })), None);
}

#[test]
fn the_message_names_the_node_and_field() {
    let p = program(None);
    assert_eq!(message(&six_fields(), &p.nodes[0], &[]), "'n' cron is bad");
}

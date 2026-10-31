use serde_json::{json, Value};

use super::*;
use crate::instance::{filled_node, instance_filled_literal};

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
    fires(rule, &project.nodes[0], &RuleContext::new(project), &CustomPorts::default())
}

#[test]
fn a_content_rule_fires_on_a_written_value_that_breaks_it() {
    assert!(fires_on(&program(Some(json!("0 3 * * *"))), &six_fields()));
    assert!(!fires_on(&program(Some(json!("0 0 3 * * *"))), &six_fields()));
}

/// An instance-filled field with no fallback is there with an unknown value:
/// a presence rule holds, a content rule has no answer and does not fire.
#[test]
fn an_instance_filled_field_is_present_with_an_unknown_content() {
    let p = program(Some(instance_filled_literal(None)));
    assert!(!fires_on(&p, &six_fields()), "the content is not known yet");
    let missing = rule(json!({ "kind": "not", "of": { "kind": "config_nonempty", "field": "cron" } }));
    assert!(!fires_on(&p, &missing), "the instance is given it");
    assert_eq!(evaluate(&six_fields().when, &p.nodes[0], &RuleContext::new(&p), &CustomPorts::default()), None);
}

/// A fallback is a known value, checked like a written one.
#[test]
fn a_fallback_is_checked_like_a_written_value() {
    assert!(fires_on(&program(Some(instance_filled_literal(Some(json!("0 3 * * *"))))), &six_fields()));
    assert!(!fires_on(&program(Some(instance_filled_literal(Some(json!("0 0 3 * * *"))))), &six_fields()));
}

/// The instance's value swapped in is what the rule reads, and an instance
/// given nothing leaves the field unwritten.
#[test]
fn an_instances_value_is_checked_on_the_filled_node() {
    let p = program(Some(instance_filled_literal(None)));
    let bad = filled_node(&p.nodes[0], Some(&[("cron".to_string(), json!("0 3 * * *"))].into()));
    assert!(fires(&six_fields(), &bad, &RuleContext::new(&p), &CustomPorts::default()));
    let good = filled_node(&p.nodes[0], Some(&[("cron".to_string(), json!("0 0 3 * * *"))].into()));
    assert!(!fires(&six_fields(), &good, &RuleContext::new(&p), &CustomPorts::default()));
    let none = filled_node(&p.nodes[0], None);
    assert!(none.port_literals.is_empty(), "no value and no fallback leaves the field unwritten");
    let missing = rule(json!({ "kind": "not", "of": { "kind": "config_nonempty", "field": "cron" } }));
    assert!(fires(&missing, &none, &RuleContext::new(&p), &CustomPorts::default()));
}

#[test]
fn unknown_parts_combine_three_ways() {
    let p = program(Some(instance_filled_literal(None)));
    let known_false = json!({ "kind": "config_equals", "field": "missing", "equals": 1 });
    let known_true = json!({ "kind": "not", "of": known_false });
    let unknown = json!({ "kind": "config_equals", "field": "cron", "equals": "x" });
    let eval = |when: Value| evaluate(&rule(when).when, &p.nodes[0], &RuleContext::new(&p), &CustomPorts::default());
    assert_eq!(eval(json!({ "kind": "all", "of": [known_false, unknown] })), Some(false));
    assert_eq!(eval(json!({ "kind": "all", "of": [known_true, unknown] })), None);
    assert_eq!(eval(json!({ "kind": "any", "of": [known_true, unknown] })), Some(true));
    assert_eq!(eval(json!({ "kind": "any", "of": [known_false, unknown] })), None);
    assert_eq!(eval(json!({ "kind": "not", "of": unknown })), None);
}

#[test]
fn the_message_names_the_node_and_field() {
    let p = program(None);
    assert_eq!(message(&six_fields(), &p.nodes[0], &RuleContext::new(&p), &CustomPorts::default()), "'n' cron is bad");
}

/// A name written on the input is held to the program's nodes; a name
/// nobody knows yet (an instance fills it) has no answer.
#[test]
fn a_name_on_an_input_is_held_to_the_programs_nodes() {
    let names = rule(json!({ "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": { "node": {} } } }));
    assert!(!fires_on(&program(Some(json!("n"))), &names), "`n` is a node");
    assert!(!fires_on(&program(None), &names), "nothing written names nothing");
    let p = program(Some(json!(["n", "ghost"])));
    assert!(fires_on(&p, &names));
    let named: ValidationRule = serde_json::from_value(json!({
        "when": names.when, "then": { "message": "{names}" }
    }))
    .unwrap();
    assert_eq!(message(&named, &p.nodes[0], &RuleContext::new(&p), &CustomPorts::default()), "'ghost'");
    assert!(!fires_on(&program(Some(instance_filled_literal(None))), &names), "unknown yet");
    let infra = rule(json!({ "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": { "node": { "role": "infra" } } } }));
    assert!(fires_on(&program(Some(json!("n"))), &infra), "`n` has no infra");
    // A place whose state a run tells: the node outside every loop is
    // one, a name nobody wrote is not.
    let state = rule(json!({ "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": { "state": {} } } }));
    assert!(!fires_on(&program(Some(json!("n"))), &state), "`n` is a node outside every loop");
    assert!(fires_on(&program(Some(json!("ghost"))), &state));
}

/// A `node.field` written on an input (list items, or an object's keys)
/// is held to the program's nodes and their fields, and optionally to
/// the fields an instance fills.
#[test]
fn a_field_named_on_an_input_is_held_to_the_programs_fields() {
    let field = |filled: Option<bool>| {
        let named = match filled {
            Some(b) => json!({ "field": { "instanceFilled": b } }),
            None => json!({ "field": {} }),
        };
        let when = json!({ "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": named } });
        serde_json::from_value::<ValidationRule>(json!({ "when": when, "then": { "message": "{names}" } })).unwrap()
    };
    let any = field(None);
    assert!(!fires_on(&program(Some(json!(["n.cron"]))), &any), "`n` has `cron`");
    assert!(!fires_on(&program(Some(json!({ "n.cron": "x" }))), &any), "object keys are names");
    let bad = program(Some(json!({ "n.cron": "x", "n.ghost": 1, "ghost.cron": 2, "n": 3 })));
    assert!(fires_on(&bad, &any));
    assert_eq!(message(&any, &bad.nodes[0], &RuleContext::new(&bad), &CustomPorts::default()), "'ghost.cron', 'n', 'n.ghost'");
    // `cron` holds a list, not `@instance_filled`: no instance fills it.
    let filled = field(Some(true));
    let p = program(Some(json!(["n.cron"])));
    assert!(fires_on(&p, &filled), "not a field an instance fills");
    assert_eq!(message(&filled, &p.nodes[0], &RuleContext::new(&p), &CustomPorts::default()), "'n.cron'");
    assert!(!fires_on(&p, &field(Some(false))));
    assert!(!fires_on(&program(Some(instance_filled_literal(None))), &filled), "unknown yet");
}

/// The instance-filled case with the field really written
/// `@instance_filled`: a second node `m` whose `model` an instance fills.
#[test]
fn a_field_an_instance_fills_is_one_written_instance_filled() {
    let mut p = program(Some(json!({ "m.model": "x" })));
    let mut m = p.nodes[0].clone();
    m.id = "m".into();
    m.port_literals.clear();
    m.port_literals.insert("model".into(), instance_filled_literal(None));
    p.nodes.push(m);
    let when = |b: bool| json!({ "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": { "field": { "instanceFilled": b } } } });
    assert!(!fires_on(&p, &rule(when(true))));
    assert!(fires_on(&p, &rule(when(false))));
}

/// Three routes (triggers with a live connection), each wired to its own
/// ten replies, and one reply nothing reaches.
fn routes_and_replies() -> ProjectDefinition {
    let node = |id: &str, features: Value| json!({
        "id": id, "nodeType": "T", "label": null,
        "config": null, "position": { "x": 0.0, "y": 0.0 },
        "inputs": [{ "name": "in", "portType": "String", "required": false }],
        "outputs": [{ "name": "out", "portType": "String", "required": false }],
        "features": features, "scope": [], "groupBoundary": null,
        "requiresInfra": false, "images": []
    });
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    for route in 0..3 {
        nodes.push(node(&format!("route{route}"), json!({ "isTrigger": true, "liveConnection": "http" })));
        for reply in 0..10 {
            let id = format!("reply{route}_{reply}");
            nodes.push(node(&id, json!({})));
            edges.push(json!({ "id": format!("e{route}_{reply}"), "source": format!("route{route}"), "sourceHandle": "out", "target": id, "targetHandle": "in" }));
        }
    }
    nodes.push(node("stray", json!({})));
    serde_json::from_value(json!({
        "id": uuid::Uuid::new_v4(), "nodes": nodes, "edges": edges,
        "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z"
    }))
    .expect("project")
}

/// A run carved from one seed is the same for every node and rule that
/// asks, so one pass carves each seed once: asking thirty-one nodes
/// whether a live route's run reaches them carves the three routes, not
/// thirty-one times three. This is what keeps validating a program with
/// many replies linear in its routes.
#[test]
fn a_rule_pass_carves_each_seed_once() {
    let p = routes_and_replies();
    let cx = RuleContext::new(&p);
    let reached = rule(json!({ "kind": "run_reaches", "direction": "upstream", "with": { "liveConnection": true } }));
    for node in p.nodes.iter().filter(|n| !n.features.is_trigger) {
        assert_eq!(fires(&reached, node, &cx, &CustomPorts::default()), node.id != "stray", "{}", node.id);
    }
    assert_eq!(cx.carved(), 3);
    for node in &p.nodes {
        fires(&reached, node, &cx, &CustomPorts::default());
    }
    assert_eq!(cx.carved(), 3, "a second pass carves nothing new");
}

/// `all` and `any` stop at the first part that decides them, and keep
/// the three-valued answer otherwise.
#[test]
fn all_and_any_settle_on_the_deciding_part() {
    let p = program(Some(instance_filled_literal(None)));
    let cx = RuleContext::new(&p);
    let unknown = json!({ "kind": "config_equals", "field": "cron", "equals": "x" });
    let yes = json!({ "kind": "config_present", "field": "cron" });
    let no = json!({ "kind": "not", "of": yes.clone() });
    let eval = |when: Value| evaluate(&rule(when).when, &p.nodes[0], &cx, &CustomPorts::default());
    assert_eq!(eval(json!({ "kind": "all", "of": [no.clone(), unknown.clone()] })), Some(false));
    assert_eq!(eval(json!({ "kind": "all", "of": [unknown.clone(), yes.clone()] })), None);
    assert_eq!(eval(json!({ "kind": "all", "of": [yes.clone(), yes.clone()] })), Some(true));
    assert_eq!(eval(json!({ "kind": "any", "of": [unknown.clone(), yes.clone()] })), Some(true));
    assert_eq!(eval(json!({ "kind": "any", "of": [no.clone(), unknown] })), None);
    assert_eq!(eval(json!({ "kind": "any", "of": [no.clone(), no] })), Some(false));
}

/// A node inside a group written by its short name is refused with the
/// full spelling it most likely meant.
#[test]
fn a_short_name_is_refused_with_the_spelling_it_meant() {
    let mut p = program(Some(json!("box")));
    let mut boxed = p.nodes[0].clone();
    boxed.id = "work.box".into();
    boxed.scope = vec!["work".into()];
    boxed.port_literals.clear();
    p.nodes.push(boxed);
    let named: ValidationRule = serde_json::from_value(json!({
        "when": { "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": { "node": {} } } },
        "then": { "message": "{names}" }
    }))
    .unwrap();
    assert!(fires_on(&p, &named));
    assert_eq!(message(&named, &p.nodes[0], &RuleContext::new(&p), &CustomPorts::default()), "'box' (did you mean 'work.box'?)");
}

/// `custom_inputs_fit` holds each added input to ONE of the listed types,
/// at any depth: under `Json`, a file inside a record or a list is no
/// JSON, a file on its own fits `File`, and a union of a value and a file
/// fits neither. A type still open fits. `{names}` lists the added inputs
/// that do not fit, sorted.
#[test]
fn added_inputs_are_held_to_the_bound_and_the_misfits_are_named() {
    let mut p = program(None);
    let declared = [
        ("text", "String"),
        ("scores", "{title: String, score: Number}"),
        ("extra", "JsonDict"),
        ("photo", "Image"),
        ("maybePhoto", "Image | Null"),
        ("photos", "List[Image]"),
        ("open", "T"),
        ("withPhoto", "{caption: String, photo: Image}"),
        ("mixed", "List[Image | String]"),
        ("valueOrFile", "String | Image"),
    ];
    for (name, ty) in declared {
        let mut input: crate::project::InputDefinition =
            serde_json::from_value(json!({ "name": name, "portType": "String", "required": false })).unwrap();
        input.port.port_type = if ty == "T" {
            crate::weft_type::WeftType::type_var("T")
        } else {
            crate::weft_type::WeftType::parse(ty).unwrap_or_else(|| panic!("{ty} parses"))
        };
        p.nodes[0].inputs.push(input);
    }
    let custom = CustomPorts { inputs: declared.iter().map(|(name, _)| name.to_string()).collect(), outputs: Vec::new() };
    let fit: ValidationRule = serde_json::from_value(json!({
        "when": { "kind": "not", "of": { "kind": "custom_inputs_fit",
            "oneOf": ["Json", "File | Null", "List[File] | Null"] } },
        "then": { "message": "{names}" }
    }))
    .unwrap();
    let cx = RuleContext::new(&p);
    assert!(fires(&fit, &p.nodes[0], &cx, &custom));
    assert_eq!(message(&fit, &p.nodes[0], &cx, &custom), "'mixed', 'valueOrFile', 'withPhoto'");
    let fitting = CustomPorts { inputs: custom.inputs[..7].to_vec(), outputs: Vec::new() };
    assert!(!fires(&fit, &p.nodes[0], &cx, &fitting), "every one of the first seven fits");
    assert!(!fires(&fit, &p.nodes[0], &cx, &CustomPorts::default()), "nothing added, nothing to refuse");
}

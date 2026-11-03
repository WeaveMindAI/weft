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
        { "kind": "value", "field": "cron", "nonempty": true },
        { "kind": "not", "of": { "kind": "value", "field": "cron", "matches": "^\\S+(\\s+\\S+){5}$" } }
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
    let missing = rule(json!({ "kind": "not", "of": { "kind": "value", "field": "cron", "nonempty": true } }));
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
    let missing = rule(json!({ "kind": "not", "of": { "kind": "value", "field": "cron", "nonempty": true } }));
    assert!(fires(&missing, &none, &RuleContext::new(&p), &CustomPorts::default()));
}

#[test]
fn unknown_parts_combine_three_ways() {
    let p = program(Some(instance_filled_literal(None)));
    let known_false = json!({ "kind": "value", "field": "missing", "equals": 1 });
    let known_true = json!({ "kind": "not", "of": known_false });
    let unknown = json!({ "kind": "value", "field": "cron", "equals": "x" });
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
    let unknown = json!({ "kind": "value", "field": "cron", "equals": "x" });
    let yes = json!({ "kind": "value", "field": "cron" });
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

/// `n`'s `cron` fed by a wire from `src` (of type `Src`): required, or
/// optional and allowed to run on a wire that closed empty (`x?:`).
fn wired(required: bool, may_close_empty: bool) -> ProjectDefinition {
    let mut p = program(None);
    p.nodes[0].inputs[0].port.required = required;
    p.nodes[0].inputs[0].optional = may_close_empty;
    let mut src = p.nodes[0].clone();
    src.id = "src".into();
    src.node_type = "Src".into();
    p.nodes.insert(0, src);
    p.edges.push(serde_json::from_value(json!({ "id": "e", "source": "src", "sourceHandle": "out", "target": "n", "targetHandle": "cron" })).unwrap());
    p
}

fn the_node(p: &ProjectDefinition) -> &NodeDefinition {
    p.nodes.iter().find(|n| n.id == "n").expect("n")
}

fn eval_on(p: &ProjectDefinition, when: Value) -> Option<bool> {
    evaluate(&rule(when).when, the_node(p), &RuleContext::new(p), &CustomPorts::default())
}

/// What each fires once a firing of `n` delivered `values`: the rules
/// the compile left undecided, recorded and then asked on the values.
fn rechecked(p: &ProjectDefinition, rules: &[ValidationRule], values: Value) -> (usize, crate::error::WeftResult<()>) {
    let mut node = the_node(p).clone();
    node.recheck = undecided(rules, &node, &RuleContext::new(p), &CustomPorts::default());
    let values = values.as_object().cloned().unwrap_or_default();
    (node.recheck.len(), recheck(&node, p, &values))
}

/// Every kind `value` replaced, and `input_source_type`, is refused
/// naming what to write instead, at any depth.
#[test]
fn a_retired_condition_kind_is_refused_naming_its_replacement() {
    let refused = |when: Value| serde_json::from_value::<Condition>(when).unwrap_err().to_string();
    for (kind, names) in [
        ("config_present", "\"kind\": \"value\""),
        ("config_nonempty", "\"nonempty\": true"),
        ("config_equals", "\"equals\""),
        ("config_in_set", "\"oneOf\""),
        ("config_matches", "\"matches\""),
        ("config_item_count", "\"count\""),
        ("config_items_fit", "\"each\""),
        ("config_in_range", "\"range\""),
        ("config_field_in_set", "\"key\""),
    ] {
        let e = refused(json!({ "kind": kind, "field": "x" }));
        assert!(e.contains(&format!("the `{kind}` rule kind is gone")) && e.contains(names), "{e}");
    }
    let e = refused(json!({ "kind": "not", "of": { "kind": "input_source_type", "port": "account", "oneOf": ["SlackAccess"] } }));
    assert!(e.contains("`input_source_type` rule kind is gone") && e.contains("\"type\": \"Access[slack]\""), "{e}");
}

/// A value on a wire is known only when the node fires: a question about
/// its content has no answer at compile time, and whether it is set has
/// one only when the node cannot fire without it.
#[test]
fn a_wired_value_is_undecided_at_compile_time() {
    let required = wired(true, false);
    assert_eq!(eval_on(&required, json!({ "kind": "value", "field": "cron", "matches": "^x$" })), None);
    assert_eq!(eval_on(&required, json!({ "kind": "value", "field": "cron" })), Some(true));
    assert_eq!(eval_on(&required, json!({ "kind": "input_satisfied", "port": "cron" })), Some(true));
    // A wire that may close empty is the trap: set or not is unknown.
    let optional = wired(false, true);
    assert_eq!(eval_on(&optional, json!({ "kind": "value", "field": "cron" })), None);
    assert_eq!(eval_on(&optional, json!({ "kind": "input_satisfied", "port": "cron" })), None);
    assert_eq!(eval_on(&optional, json!({ "kind": "input_wired", "port": "cron" })), Some(true));
}

/// The compile records only what it could not decide, folded down to
/// the questions about values, nested `all`/`any`/`not` included; a rule
/// decided either way, and a warning, are not recorded.
#[test]
fn the_compile_records_the_undecided_rules_folded() {
    let p = wired(false, true);
    let node = the_node(&p);
    let cx = RuleContext::new(&p);
    let decided_true = json!({ "kind": "input_wired", "port": "cron" });
    let decided_false = json!({ "kind": "output_wired", "port": "out" });
    let open = json!({ "kind": "value", "field": "cron", "equals": "x" });
    let rules = [
        rule(json!({ "kind": "all", "of": [decided_true, { "kind": "not", "of": { "kind": "any", "of": [decided_false, open] } }] })),
        rule(json!({ "kind": "all", "of": [decided_false, open] })),
        rule(decided_true.clone()),
        serde_json::from_value(json!({ "when": open, "then": { "message": "w", "severity": "warning" } })).unwrap(),
        rule(json!({ "kind": "input_satisfied", "port": "cron" })),
    ];
    let recorded = undecided(&rules, node, &cx, &CustomPorts::default());
    assert_eq!(recorded.len(), 2, "{recorded:?}");
    assert_eq!(
        serde_json::to_value(&recorded[0].when).unwrap(),
        json!({ "kind": "not", "of": { "kind": "value", "field": "cron", "equals": "x" } })
    );
    assert_eq!(recorded[0].message, "'n' cron is bad");
    assert_eq!(serde_json::to_value(&recorded[1].when).unwrap(), json!({ "kind": "input_satisfied", "port": "cron" }));
}

/// Before the body, a recorded rule is asked on what arrived: it fails
/// the firing with its own message as an input error when it fires, and
/// lets it run when it does not. A node with nothing recorded is never
/// asked.
#[test]
fn a_recorded_rule_is_asked_on_what_arrived() {
    let p = wired(true, false);
    let six = [six_fields()];
    let (recorded, outcome) = rechecked(&p, &six, json!({ "cron": "0 3 * * *" }));
    assert_eq!(recorded, 1);
    assert!(matches!(&outcome, Err(crate::error::WeftError::Input(m)) if m == "'n' cron is bad"), "{outcome:?}");
    assert!(rechecked(&p, &six, json!({ "cron": "0 0 3 * * *" })).1.is_ok());
    // Written, the same rule is decided at compile time and never recorded.
    let written = program(Some(json!("0 0 3 * * *")));
    assert_eq!(undecided(&six, &written.nodes[0], &RuleContext::new(&written), &CustomPorts::default()).len(), 0);
    assert!(recheck(&written.nodes[0], &written, &serde_json::Map::new()).is_ok());
    // An optional wire that closed empty arrives as nothing.
    let paired = [rule(json!({ "kind": "not", "of": { "kind": "input_satisfied", "port": "cron" } }))];
    let optional = wired(false, true);
    assert!(rechecked(&optional, &paired, json!({})).1.is_err());
    assert!(rechecked(&optional, &paired, json!({ "cron": "x" })).1.is_ok());
}

/// A name that arrived on a wire is held to the program's nodes when it
/// arrives, and the message names it.
#[test]
fn a_name_that_arrived_is_held_to_the_program() {
    let p = wired(true, false);
    let names: ValidationRule = serde_json::from_value(json!({
        "when": { "kind": "not", "of": { "kind": "input_names", "port": "cron", "names": { "node": {} } } },
        "then": { "message": "names {names}" }
    }))
    .unwrap();
    let (recorded, outcome) = rechecked(&p, std::slice::from_ref(&names), json!({ "cron": "ghost" }));
    assert_eq!(recorded, 1);
    assert!(matches!(&outcome, Err(crate::error::WeftError::Input(m)) if m == "names 'ghost'"), "{outcome:?}");
    assert!(rechecked(&p, &[names], json!({ "cron": "src" })).1.is_ok());
}

/// Every check of a `value` rule, each on a written value.
#[test]
fn the_value_checks_read_lists_numbers_texts_and_records() {
    let on = |value: Value, checks: Value| {
        let mut when = json!({ "kind": "value", "field": "cron" });
        when.as_object_mut().unwrap().extend(checks.as_object().unwrap().clone());
        eval_on(&program(Some(value)), when).expect("a written value decides")
    };
    let absent = |checks: Value| {
        let mut when = json!({ "kind": "value", "field": "cron" });
        when.as_object_mut().unwrap().extend(checks.as_object().unwrap().clone());
        eval_on(&program(None), when).expect("an absent value decides")
    };
    // Present, and absent false whatever the check.
    assert!(on(json!(0), json!({})));
    assert!(!absent(json!({})));
    assert!(!absent(json!({ "count": { "max": 3 } })), "absent is false whenever a check is given");
    assert!(!on(json!(null), json!({ "nonempty": true })));

    assert!(on(json!(["a"]), json!({ "nonempty": true })));
    assert!(!on(json!("  "), json!({ "nonempty": true })));
    assert!(!on(json!({}), json!({ "nonempty": true })));

    assert!(on(json!(true), json!({ "equals": true })));
    assert!(!on(json!("true"), json!({ "equals": true })));

    assert!(on(json!("b"), json!({ "oneOf": ["a", "b"] })));
    assert!(!on(json!("c"), json!({ "oneOf": ["a", "b"] })));
    assert!(on(json!(2), json!({ "oneOf": [1, 2] })), "JSON values, not only texts");

    assert!(on(json!("abc"), json!({ "matches": "^[a-z]+$" })));
    assert!(!on(json!(3), json!({ "matches": "^3$" })), "the wrong kind at the top is false");

    let count = |min: Option<u64>, max: Option<u64>| json!({ "count": { "min": min, "max": max } });
    assert!(on(json!(["a", "b"]), count(Some(1), Some(2))));
    assert!(!on(json!(["a", "b", "c"]), count(Some(1), Some(2))));
    assert!(!on(json!([]), count(Some(1), None)));
    assert!(on(json!({ "a": 1, "b": 2 }), count(Some(2), None)), "a dict counts its entries");
    assert!(on(json!("one"), count(Some(1), Some(1))), "one value is one item");

    assert!(on(json!(0.5), json!({ "range": { "min": 0.0, "max": 1.0 } })));
    assert!(on(json!(1), json!({ "range": { "min": 0.0, "max": 1.0 } })), "both ends inclusive");
    assert!(!on(json!(1.5), json!({ "range": { "max": 1.0 } })));
    assert!(!on(json!("0.5"), json!({ "range": { "min": 0.0 } })), "a text is no number");

    assert!(on(json!("éé"), json!({ "length": { "max": 2 } })), "characters, not bytes");
    assert!(!on(json!("abc"), json!({ "length": { "max": 2 } })));
    assert!(!on(json!(["ab"]), json!({ "length": { "max": 2 } })), "a list is no text");

    // Checks combine: all must hold.
    assert!(!on(json!("abc"), json!({ "matches": "^[a-z]+$", "length": { "max": 2 } })));
}

/// `unique` compares a list's items whole, or its records (a dict's
/// values) by a key; a dict's keys never repeat.
#[test]
fn unique_compares_items_whole_or_by_a_key() {
    let on = |value: Value, unique: Value| {
        eval_on(&program(Some(value)), json!({ "kind": "value", "field": "cron", "unique": unique })).expect("decides")
    };
    assert!(on(json!(["a", "b"]), json!(true)));
    assert!(!on(json!(["a", "b", "a"]), json!(true)));
    assert!(!on(json!([{ "x": 1 }, { "x": 1 }]), json!(true)), "records compare whole");
    assert!(on(json!({ "a": 1, "b": 1 }), json!(true)), "a dict's keys never repeat");
    assert!(on(json!("one"), json!(true)), "one value is one item");

    assert!(on(json!([{ "name": "a" }, { "name": "b" }, { "other": 1 }]), json!("name")));
    assert!(!on(json!([{ "name": "a", "n": 1 }, { "name": "a", "n": 2 }]), json!("name")));
    assert!(on(json!([{ "name": null }, { "name": null }]), json!("name")), "a record without the key is left out");
    assert!(on(json!({ "x": { "name": "a" }, "y": { "name": "b" } }), json!("name")));
    assert!(!on(json!({ "x": { "name": "a" }, "y": { "name": "a" } }), json!("name")), "a dict's values by the key");
}

/// `each` holds every item (one value is one item) to its checks, an
/// item of the wrong kind passing; with `key`, each record's key, a record
/// without it passing; and an `each` inside an `each`.
#[test]
fn each_holds_every_item_with_and_without_a_key() {
    let on = |value: Value, each: Value| {
        eval_on(&program(Some(value)), json!({ "kind": "value", "field": "cron", "each": each })).expect("decides")
    };
    let fit = json!({ "length": { "min": 2, "max": 4 }, "matches": "^[a-z]+$" });
    assert!(on(json!(["ab", "abcd"]), fit.clone()));
    assert!(!on(json!(["ab", "a"]), fit.clone()), "too short");
    assert!(!on(json!(["AB"]), fit.clone()), "a character it does not take");
    assert!(on(json!("ab"), fit.clone()), "one text is one item");
    assert!(on(json!(["ab", 3]), fit), "an item of another kind is the type check's");
    assert!(!on(json!([60, 40]), json!({ "range": { "min": 50, "max": 500 } })));

    let typed = json!({ "key": "type", "oneOf": ["file", "url"] });
    assert!(on(json!([{ "type": "file" }, { "type": "url", "id": 1 }, { "id": 2 }, { "type": null }]), typed.clone()));
    assert!(!on(json!([{ "type": "file" }, { "type": "text" }]), typed.clone()));
    assert!(on(json!({ "type": "url" }), typed.clone()), "one record is its own");
    assert!(!on(json!({ "type": 3 }), typed));

    let grid = json!({ "each": { "range": { "max": 9 } } });
    assert!(on(json!([[1, 2], [9]]), grid.clone()));
    assert!(!on(json!([[1, 2], [10]]), grid));
}

/// What a value nobody knows yet answers: there and non-empty, nothing
/// about its content; a wired one is there when the node cannot fire
/// without it, and every other answer waits for what arrives.
#[test]
fn an_unknown_or_wired_value_answers_presence_only() {
    let filled = program(Some(instance_filled_literal(None)));
    assert_eq!(eval_on(&filled, json!({ "kind": "value", "field": "cron" })), Some(true));
    assert_eq!(eval_on(&filled, json!({ "kind": "value", "field": "cron", "nonempty": true })), Some(true));
    for content in [json!({ "count": { "min": 1 } }), json!({ "unique": true }), json!({ "each": { "nonempty": true } }), json!({ "nonempty": true, "equals": "x" })] {
        let mut when = json!({ "kind": "value", "field": "cron" });
        when.as_object_mut().unwrap().extend(content.as_object().unwrap().clone());
        assert_eq!(eval_on(&filled, when.clone()), None, "{when}");
        assert_eq!(eval_on(&wired(true, false), when.clone()), None, "{when}");
    }
    assert_eq!(eval_on(&wired(true, false), json!({ "kind": "value", "field": "cron" })), Some(true));
    assert_eq!(eval_on(&wired(false, true), json!({ "kind": "value", "field": "cron" })), None);
    assert_eq!(eval_on(&wired(true, false), json!({ "kind": "value", "field": "cron", "nonempty": true })), None);
    // Asked again on what arrived, the count rule an input's fewest items
    // became holds a wired list.
    let too_few = [rule(json!({ "kind": "all", "of": [
        { "kind": "value", "field": "cron" },
        { "kind": "not", "of": { "kind": "value", "field": "cron", "count": { "min": 2 } } }
    ] }))];
    let optional = wired(false, true);
    assert!(rechecked(&optional, &too_few, json!({ "cron": ["a"] })).1.is_err());
    assert!(rechecked(&optional, &too_few, json!({ "cron": ["a", "b"] })).1.is_ok());
    assert!(rechecked(&optional, &too_few, json!({})).1.is_ok(), "an optional input left empty is fine");
}

/// A blank text on an optional String input of the node's own is no
/// value: a rule sees it not set, unless the input keeps blanks.
#[test]
fn a_blank_text_is_not_set_unless_the_input_keeps_it() {
    let blank = |keeps: bool| {
        let mut p = program(Some(json!("  ")));
        let input = &mut p.nodes[0].inputs[0];
        input.port.required = false;
        input.from_spec = true;
        input.keeps_blank = keeps;
        p
    };
    let set = json!({ "kind": "value", "field": "cron" });
    let satisfied = json!({ "kind": "input_satisfied", "port": "cron" });
    assert_eq!(eval_on(&blank(false), set.clone()), Some(false));
    assert_eq!(eval_on(&blank(false), satisfied.clone()), Some(false));
    assert_eq!(eval_on(&blank(true), set), Some(true));
    assert_eq!(eval_on(&blank(true), satisfied), Some(true));
    // A required input, or one the program added, keeps what is written.
    let mut required = blank(false);
    required.nodes[0].inputs[0].port.required = true;
    assert_eq!(eval_on(&required, json!({ "kind": "value", "field": "cron" })), Some(true));
}

/// Layer 2: the rules a node carries for the runtime ride the compiled
/// program as `recheck`, read back as written, and a node with none
/// carries no key at all (so its program reads as it did before).
#[test]
fn the_recorded_rules_round_trip_in_the_compiled_program() {
    let p = wired(true, false);
    let mut node = the_node(&p).clone();
    assert!(serde_json::to_value(&node).unwrap().get("recheck").is_none(), "nothing recorded, no key");
    node.recheck = undecided(&[six_fields()], &node, &RuleContext::new(&p), &CustomPorts::default());
    let wire = serde_json::to_value(&node).unwrap();
    assert_eq!(
        wire["recheck"],
        json!([{
            "when": { "kind": "all", "of": [
                { "kind": "value", "field": "cron", "nonempty": true },
                { "kind": "not", "of": { "kind": "value", "field": "cron", "matches": "^\\S+(\\s+\\S+){5}$" } }
            ] },
            "message": "'n' cron is bad"
        }])
    );
    let back: NodeDefinition = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(&back).unwrap(), wire);
    assert!(
        serde_json::from_value::<Recheck>(json!({ "when": { "kind": "per_instance" }, "message": "m", "level": "runtime" })).is_err(),
        "a recorded rule carries its condition and its message, nothing else"
    );
}

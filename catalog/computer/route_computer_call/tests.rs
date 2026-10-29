//! RouteComputerCall self-tests: every action in both styles lands on
//! its port in the one shape, a call that is no computer action goes to
//! `other`, malformed arguments fail naming the field, and every tool an
//! action node describes on its `tools` metadata port is one this node
//! recognizes.

use serde_json::{json, Value};

use weft::{FakeRig, NodeManifest, NodeTest, WeftResult};

use super::super::click::ClickNode;
use super::super::cursor_position::CursorPositionNode;
use super::super::drag::DragNode;
use super::super::mouse_button::MouseButtonNode;
use super::super::move_mouse::MoveMouseNode;
use super::super::press_key::PressKeyNode;
use super::super::run_command::RunCommandNode;
use super::super::screenshot::ScreenshotNode;
use super::super::scroll::ScrollNode;
use super::super::type_text::TypeTextNode;
use super::super::zoom::ZoomNode;
use super::{route, xdotool_key, RouteComputerCallNode, Routed};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("anthropic_calls_land_on_their_ports", anthropic),
        NodeTest::basic("openai_calls_land_on_their_ports", openai),
        NodeTest::basic("the_names_both_styles_use_are_told_apart_by_their_fields", shared_names),
        NodeTest::basic("bash_becomes_a_command", bash),
        NodeTest::basic("openai_key_names_become_xdotools", key_names),
        NodeTest::basic("a_call_that_is_no_computer_action_goes_to_other", other),
        NodeTest::basic("malformed_arguments_fail_naming_the_tool_and_the_field", malformed),
        NodeTest::basic("every_declared_tool_is_recognized", every_declared_tool_is_recognized),
        NodeTest::fake("the_node_emits_on_the_one_port", node_emits),
        NodeTest::fake("malformed_arguments_reach_error", node_malformed),
    ]
}

fn call(name: &str, arguments: Value) -> Value {
    json!({ "id": "call_1", "name": name, "arguments": arguments.to_string() })
}

fn routes(name: &str, arguments: Value) -> Routed {
    route(&call(name, arguments)).unwrap_or_else(|e| panic!("{name} did not route: {e}"))
}

fn lands(name: &str, arguments: Value, port: &str, value: Value) {
    let routed = routes(name, arguments);
    assert_eq!((routed.port, &routed.value), (port, &value), "{name}");
}

fn anthropic() -> WeftResult<()> {
    lands("screenshot", json!({}), "screenshot", json!(true));
    lands("zoom", json!({ "region": [0, 0, 320, 200] }), "zoom", json!({ "region": [0, 0, 320, 200] }));
    lands("left_click", json!({ "coordinate": [10, 20] }), "click", json!({ "x": 10, "y": 20, "button": "left", "count": 1 }));
    lands(
        "right_click",
        json!({ "coordinate": [10, 20], "text": "ctrl" }),
        "click",
        json!({ "x": 10, "y": 20, "button": "right", "count": 1, "modifiers": "ctrl" }),
    );
    lands("middle_click", json!({}), "click", json!({ "button": "middle", "count": 1 }));
    lands("triple_click", json!({ "coordinate": [1, 2] }), "click", json!({ "x": 1, "y": 2, "button": "left", "count": 3 }));
    lands(
        "left_click_drag",
        json!({ "start_coordinate": [1, 2], "coordinate": [30, 40], "text": "shift" }),
        "drag",
        json!({ "path": [{ "x": 1, "y": 2 }, { "x": 30, "y": 40 }], "modifiers": "shift" }),
    );
    lands("mouse_move", json!({ "coordinate": [5, 6] }), "move", json!({ "x": 5, "y": 6 }));
    lands("left_mouse_down", json!({}), "mouseButton", json!({ "action": "down", "button": "left" }));
    lands("left_mouse_up", json!({}), "mouseButton", json!({ "action": "up", "button": "left" }));
    lands("cursor_position", json!({}), "cursor", json!(true));
    lands("type", json!({ "text": "hi\n" }), "type", json!({ "text": "hi\n" }));
    lands("key", json!({ "text": "ctrl+s" }), "key", json!({ "keys": "ctrl+s" }));
    lands("key", json!({ "text": "Down", "repeat": 3 }), "key", json!({ "keys": "Down", "repeat": 3 }));
    lands("hold_key", json!({ "text": "shift", "duration": 2.5 }), "key", json!({ "keys": "shift", "holdSecs": 2.5 }));
    Ok(())
}

fn openai() -> WeftResult<()> {
    lands("click", json!({ "button": "left", "x": 10, "y": 20 }), "click", json!({ "x": 10, "y": 20, "button": "left", "count": 1 }));
    lands(
        "click",
        json!({ "button": "wheel", "x": 10, "y": 20, "keys": ["CTRL", "SHIFT"] }),
        "click",
        json!({ "x": 10, "y": 20, "button": "middle", "count": 1, "modifiers": "ctrl+shift" }),
    );
    lands("click", json!({ "button": "back", "x": 1, "y": 1 }), "click", json!({ "x": 1, "y": 1, "button": "back", "count": 1 }));
    lands(
        "drag",
        json!({ "path": [{ "x": 1, "y": 2 }, { "x": 3, "y": 4 }, { "x": 5, "y": 6 }] }),
        "drag",
        json!({ "path": [{ "x": 1, "y": 2 }, { "x": 3, "y": 4 }, { "x": 5, "y": 6 }] }),
    );
    lands("move", json!({ "x": 5, "y": 6, "keys": ["ALT"] }), "move", json!({ "x": 5, "y": 6, "modifiers": "alt" }));
    lands("keypress", json!({ "keys": ["CTRL", "S"] }), "key", json!({ "keys": "ctrl+s" }));
    lands("keypress", json!({ "keys": ["ENTER"] }), "key", json!({ "keys": "Return" }));
    Ok(())
}

fn shared_names() -> WeftResult<()> {
    lands("double_click", json!({ "coordinate": [7, 8] }), "click", json!({ "x": 7, "y": 8, "button": "left", "count": 2 }));
    lands("double_click", json!({ "x": 7, "y": 8 }), "click", json!({ "x": 7, "y": 8, "button": "left", "count": 2 }));
    lands(
        "scroll",
        json!({ "scroll_direction": "down", "scroll_amount": 3, "coordinate": [100, 200] }),
        "scroll",
        json!({ "direction": "down", "amount": 3, "x": 100, "y": 200 }),
    );
    lands("scroll", json!({ "scroll_direction": "left", "scroll_amount": 1 }), "scroll", json!({ "direction": "left", "amount": 1 }));
    lands(
        "scroll",
        json!({ "x": 100, "y": 200, "scroll_x": 0, "scroll_y": 250 }),
        "scroll",
        json!({ "x": 100, "y": 200, "direction": "down", "amount": 3 }),
    );
    lands(
        "scroll",
        json!({ "x": 1, "y": 2, "scroll_x": -40, "scroll_y": 0 }),
        "scroll",
        json!({ "x": 1, "y": 2, "direction": "left", "amount": 1 }),
    );
    lands("wait", json!({ "duration": 3 }), "wait", json!(3.0));
    lands("wait", json!({}), "wait", json!(1.0));
    Ok(())
}

fn bash() -> WeftResult<()> {
    lands("bash", json!({ "command": "ls" }), "command", json!({ "command": "ls" }));
    lands(
        "bash",
        json!({ "command": "make", "restart": true, "timeout_secs": 60 }),
        "command",
        json!({ "command": "make", "restart": true, "timeoutSecs": 60.0 }),
    );
    lands("bash", json!({ "restart": true }), "command", json!({ "restart": true }));
    Ok(())
}

fn key_names() -> WeftResult<()> {
    for (openai, xdotool) in [
        ("CTRL", "ctrl"),
        ("cmd", "super"),
        ("ESC", "Escape"),
        ("ArrowUp", "Up"),
        ("PAGEDOWN", "Page_Down"),
        ("BACKSPACE", "BackSpace"),
        ("SPACE", "space"),
        ("A", "a"),
        ("f5", "F5"),
        ("KP_Enter", "KP_Enter"),
    ] {
        assert_eq!(xdotool_key(openai), xdotool, "{openai}");
    }
    Ok(())
}

fn other() -> WeftResult<()> {
    let weather = call("get_weather", json!({ "city": "Paris" }));
    assert_eq!(route(&weather), Ok(Routed { port: "other", value: weather.clone() }));
    Ok(())
}

fn malformed() -> WeftResult<()> {
    for (name, arguments, says) in [
        ("left_click", json!({ "coordinate": "10,20" }), "left_click: coordinate is [x, y], two numbers"),
        ("left_click", json!({ "coordinate": [10] }), "left_click: coordinate is [x, y], two numbers"),
        ("zoom", json!({}), "zoom: region is missing"),
        ("click", json!({ "button": "side", "x": 1, "y": 1 }), "click: no button \"side\""),
        ("click", json!({ "button": "left", "y": 1 }), "click: x is missing"),
        ("scroll", json!({ "x": 1, "y": 1, "scroll_x": 10, "scroll_y": 10 }), "one axis at a time"),
        ("scroll", json!({ "x": 1, "y": 1, "scroll_x": 0, "scroll_y": 0 }), "nothing to scroll"),
        ("scroll", json!({ "scroll_direction": "sideways", "scroll_amount": 1 }), "scroll_direction is up, down, left or right"),
        ("scroll", json!({ "scroll_direction": "up", "scroll_amount": 0 }), "scroll_amount is a whole number from 1 to 100"),
        ("drag", json!({ "path": [{ "x": 1, "y": 2 }] }), "at least two points"),
        ("keypress", json!({ "keys": [] }), "nothing to press"),
        ("hold_key", json!({ "text": "a", "duration": 0 }), "duration is a number of seconds above 0"),
        ("bash", json!({}), "give a command to run, or restart: true"),
        ("bash", json!({ "restart": "yes" }), "restart is true or false"),
        ("scroll", json!({ "scroll_direction": "down", "scroll_amount": 1e20 }), "scroll_amount is a whole number from 1 to 100"),
        ("scroll", json!({ "x": 1, "y": 1, "scroll_x": 0, "scroll_y": 1000000000 }), "more than one scroll goes"),
        ("scroll", json!({ "scroll_amount": 3, "coordinate": [1, 2] }), "scroll_direction is missing"),
        ("wait", json!({ "duration": null }), "duration is missing"),
        ("wait", json!({ "duration": 86400 }), "at most 300"),
        ("hold_key", json!({ "text": "a", "duration": 301 }), "at most 300"),
        ("key", json!({ "text": "a", "repeat": 101 }), "repeat is a whole number from 1 to 100"),
        ("double_click", json!({ "keys": ["SHIFT"] }), "double_click: x is missing"),
        ("type", json!({ "text": 5 }), "type: text is text, got 5"),
    ] {
        let error = route(&call(name, arguments)).expect_err(name);
        assert!(error.contains(says), "{name}: {error}");
    }
    let not_json = json!({ "id": "c", "name": "type", "arguments": "{oops" });
    assert!(route(&not_json).expect_err("not JSON").contains("type: the arguments are not JSON"));
    let not_object = json!({ "id": "c", "name": "type", "arguments": "[1]" });
    assert!(route(&not_object).expect_err("not an object").contains("must be a JSON object"));
    let empty = json!({ "id": "c", "name": "screenshot", "arguments": "" });
    assert_eq!(route(&empty), Ok(Routed { port: "screenshot", value: json!(true) }), "no arguments at all is {{}}");
    Ok(())
}

/// The arguments a model would send each declared tool, filled from its
/// own schema: what this node must then route to a computer port.
fn arguments_for(schema: &Value) -> Value {
    let mut args = serde_json::Map::new();
    for field in schema["required"].as_array().expect("required") {
        let field = field.as_str().expect("a name");
        let property = &schema["properties"][field];
        let value = match (field, property["type"].as_str()) {
            ("button", _) => json!("left"),
            ("scroll_direction", _) => json!("down"),
            ("scroll_y", _) => json!(100),
            ("scroll_x", _) => json!(0),
            ("region", _) => json!([0, 0, 10, 10]),
            ("path", _) => json!([{ "x": 1, "y": 1 }, { "x": 2, "y": 2 }]),
            ("keys", _) => json!(["ENTER"]),
            (_, Some("array")) => json!([1, 2]),
            (_, Some("string")) => json!("a"),
            _ => json!(1),
        };
        args.insert(field.to_string(), value);
    }
    if schema["properties"].get("command").is_some() {
        args.insert("command".into(), json!("ls"));
    }
    Value::Object(args)
}

fn every_declared_tool_is_recognized() -> WeftResult<()> {
    let described = [
        ScreenshotNode.manifest(), ZoomNode.manifest(), CursorPositionNode.manifest(), ClickNode.manifest(),
        DragNode.manifest(), MoveMouseNode.manifest(), MouseButtonNode.manifest(), ScrollNode.manifest(),
        TypeTextNode.manifest(), PressKeyNode.manifest(), RunCommandNode.manifest(),
    ];
    for manifest in described {
        let port = manifest.metadata_ports.iter().find(|p| p.name == "tools")
            .unwrap_or_else(|| panic!("{} describes its tools", manifest.node_type));
        let value = port.value.as_ref().expect("a node type writes its metadata port's value");
        for style in ["anthropic", "openai"] {
            for tool in value[style].as_array().expect("a list per style") {
                let name = tool["name"].as_str().expect("a name");
                let routed = route(&call(name, arguments_for(&tool["parameters"])))
                    .unwrap_or_else(|e| panic!("{} describes {style} {name}, which does not route: {e}", manifest.node_type));
                assert_ne!(routed.port, "other", "{} describes {style} {name}, which is no computer action here", manifest.node_type);
            }
        }
    }
    Ok(())
}

async fn node_emits(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&RouteComputerCallNode, json!({ "call": call("left_click", json!({ "coordinate": [3, 4] })) })).await.ok()?;
    assert_eq!(outcome.outputs.len(), 1, "one port emits, every other closes: {:?}", outcome.outputs);
    assert_eq!(outcome.outputs["click"], json!({ "x": 3, "y": 4, "button": "left", "count": 1 }));
    Ok(())
}

async fn node_malformed(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
    let outcome = rig.run(&RouteComputerCallNode, json!({ "call": call("mouse_move", json!({})) })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("mouse_move: coordinate is missing"), "{error}");
    Ok(())
}

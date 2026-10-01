//! ExecPython self-tests: the pure interpreter bridge (`run_python`)
//! at the basic tier, the full node body (custom ports in, pulses out)
//! at the fake tier.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftError, WeftResult, WeftType};

use super::{run_python, ExecPythonNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("returned_dict_maps_ports_to_values", || {
            let out = run_python(
                "return {'sum': a + b, 'label': f'{a}+{b}'}",
                vec![("a".into(), json!(2)), ("b".into(), json!(3))],
            )?;
            assert_eq!(
                out,
                vec![
                    ("sum".to_string(), json!(5)),
                    ("label".to_string(), json!("2+3")),
                ]
            );
            Ok(())
        }),
        NodeTest::basic("an_empty_dict_yields_no_pulses", || {
            let out = run_python("return {}", Vec::new())?;
            assert!(out.is_empty());
            Ok(())
        }),
        NodeTest::basic("a_missing_return_is_a_type_error", || {
            for code in ["x = 1", "return", "return None"] {
                let err = run_python(code, Vec::new()).expect_err("no dict, no answer");
                assert!(matches!(&err, WeftError::Type(m) if m.contains("without returning a dict")), "{code}: {err}");
            }
            Ok(())
        }),
        NodeTest::basic("an_exception_is_a_node_failure_carrying_the_traceback", || {
            let err = run_python("raise ValueError('boom')", Vec::new()).expect_err("a raise must fail the run");
            let WeftError::NodeExecution(message) = &err else { panic!("a raise is catchable: {err}") };
            assert!(message.contains("ValueError: boom"), "{message}");
            assert!(message.contains("Traceback"), "{message}");
            Ok(())
        }),
        NodeTest::basic("a_syntax_error_is_an_input_error", || {
            let err = run_python("return {'a': 1", Vec::new()).expect_err("code that does not compile");
            assert!(matches!(&err, WeftError::Input(m) if m.contains("does not compile") && m.contains("SyntaxError")), "{err}");
            Ok(())
        }),
        NodeTest::basic("a_non_dict_return_is_a_type_error", || {
            let err = run_python("return 3", Vec::new()).expect_err("a scalar return must fail");
            assert!(matches!(&err, WeftError::Type(m) if m.contains("must return a dict")), "{err}");
            Ok(())
        }),
        NodeTest::basic("a_value_with_no_json_form_is_a_type_error", || {
            let err = run_python("return {'s': {1, 2}}", Vec::new()).expect_err("a set has no port value");
            assert!(matches!(&err, WeftError::Type(m) if m.contains("'s'") && m.contains("set")), "{err}");
            Ok(())
        }),
        NodeTest::basic("json_round_trips_through_python", || {
            let value = json!({ "s": "x", "n": 1.5, "b": true, "z": Value::Null,
                                "l": [1, 2], "o": { "k": "v" } });
            let out = run_python("return {'echo': data}", vec![("data".into(), value.clone())])?;
            assert_eq!(out, vec![("echo".to_string(), value)]);
            Ok(())
        }),
        NodeTest::fake("custom_ports_bind_in_and_pulse_out", custom_ports),
        NodeTest::fake("a_none_valued_port_pulses_nothing", none_port_skips),
        NodeTest::fake("none_on_a_nullable_port_is_a_null", none_on_a_nullable_port_is_null),
        NodeTest::fake("a_declared_input_nothing_reached_binds_as_none", absent_input_is_none),
        NodeTest::fake("a_file_arrives_unwrapped_and_leaves_wrapped", file_round_trip),
        NodeTest::fake("a_string_on_a_file_port_is_refused", string_on_file_port),
        NodeTest::fake("a_marker_shaped_dict_on_a_dict_port_arrives_whole", marker_shaped_dict),
        NodeTest::fake("a_raise_comes_out_on_error_when_it_is_wired", raise_wired),
        NodeTest::fake("a_raise_fails_the_run_when_error_is_unwired", raise_unwired),
        NodeTest::fake("a_syntax_error_fails_the_run_even_with_error_wired", syntax_error_wired),
        NodeTest::fake("a_key_that_is_no_output_fails_the_run_even_with_error_wired", undeclared_key_wired),
        NodeTest::fake("returning_error_fails_the_run_even_with_error_wired", returned_error_wired),
        NodeTest::fake("a_value_of_the_wrong_type_fails_the_run_even_with_error_wired", wrong_type_wired),
    ]
}

async fn custom_ports(rig: FakeRig) -> WeftResult<()> {
    // The compiler's canAddOutputPorts merge, played by the rig.
    rig.output_type("doubled", WeftType::parse("Number").expect("Number parses"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({ "code": "return {'doubled': n * 2}", "n": 21 }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["doubled"], json!(42));
    Ok(())
}

async fn none_port_skips(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("hit", WeftType::parse("Number").expect("Number parses"));
    rig.output_type("miss", WeftType::parse("Number").expect("Number parses"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({ "code": "return {'hit': 1, 'miss': None}" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["hit"], json!(1));
    assert!(!outcome.outputs.contains_key("miss"), "None means no pulse on a port that takes no Null");
    Ok(())
}

/// On a port that takes `Null`, `None` is the null itself and flows as
/// data; a missing key still sends nothing.
async fn none_on_a_nullable_port_is_null(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("last", WeftType::parse("JsonDict | Null").expect("parses"));
    rig.output_type("other", WeftType::parse("JsonDict | Null").expect("parses"));
    let outcome = rig.run(&ExecPythonNode, json!({ "code": "return {'last': None}" })).await.ok()?;
    assert_eq!(outcome.outputs.get("last"), Some(&json!(null)), "a real null: {:?}", outcome.outputs);
    assert!(!outcome.outputs.contains_key("other"), "a missing key sends nothing");
    Ok(())
}

/// The script names every port it declared, wired or not: an input
/// nobody fed is `None`, never an undefined name.
async fn absent_input_is_none(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("problem", WeftType::parse("String").expect("String parses"));
    rig.input_type("text", WeftType::parse("String").expect("String parses"));
    rig.output_type("seen", WeftType::parse("String").expect("String parses"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({ "code": "return {'seen': 'none' if problem is None else problem}", "text": "hi" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["seen"], json!("none"));
    Ok(())
}

/// A stored file binds as the inside of its marker (`photo["url"]`,
/// `photo["filename"]`), and handed back on a file-typed output it
/// leaves as the marker the wire expects, kind read off its mime.
async fn file_round_trip(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("p.png", "image/png", b"PNG!".to_vec());
    rig.input_type("photo", WeftType::parse("Image").expect("Image parses"));
    rig.output_type("name", WeftType::parse("String").expect("String parses"));
    rig.output_type("same", WeftType::parse("Image").expect("Image parses"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({
                "code": "assert '__weft_image__' not in photo\nreturn {'name': photo['filename'] + ':' + photo['mimeType'], 'same': photo}",
                "photo": file,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["name"], json!("p.png:image/png"));
    assert_eq!(outcome.outputs["same"], file, "the marker comes back whole");
    Ok(())
}

/// The declared type decides, never the value's shape: a plain dict
/// that happens to carry a marker key, on a port declared `JsonDict`,
/// reaches the script with that key still on it.
async fn marker_shaped_dict(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("data", WeftType::parse("JsonDict").expect("JsonDict parses"));
    rig.output_type("keys", WeftType::parse("String").expect("String parses"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({
                "code": "return {'keys': ','.join(sorted(data.keys()))}",
                "data": { "__weft_image__": { "key": "not/a/real/file", "mimeType": "image/png" } },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["keys"], json!("__weft_image__"));
    Ok(())
}

async fn string_on_file_port(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("pic", WeftType::parse("Image").expect("Image parses"));
    let err = rig
        .run(&ExecPythonNode, json!({ "code": "return {'pic': 'not a file'}" }))
        .await
        .result
        .expect_err("a string on a file port is refused")
        .to_string();
    assert!(err.contains("file port"), "{err}");
    Ok(())
}

/// A script that calls out and fails, the case `error` exists for.
const FAILING_CALL: &str = "raise ConnectionError('the weather service is down')";

async fn raise_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("error");
    let outcome = rig.run(&ExecPythonNode, json!({ "code": FAILING_CALL })).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("ConnectionError: the weather service is down"), "{error}");
    assert!(error.contains("Traceback"), "the traceback rides along: {error}");
    assert!(!outcome.outputs.contains_key("weather"), "a caught failure emits nothing else");
    Ok(())
}

async fn raise_unwired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    let err = rig.run(&ExecPythonNode, json!({ "code": FAILING_CALL })).await.failure()?;
    assert!(err.contains("the weather service is down"), "{err}");
    Ok(())
}

async fn syntax_error_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'weather': 'sun'" })).await.failure()?;
    assert!(err.starts_with("input error") && err.contains("does not compile"), "{err}");
    Ok(())
}

async fn undeclared_key_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'wether': 'sun'}" })).await.failure()?;
    assert!(err.starts_with("type error") && err.contains("'wether'") && err.contains("weather"), "{err}");
    Ok(())
}

async fn returned_error_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'error': 'no weather today'}" })).await.failure()?;
    assert!(err.starts_with("type error") && err.contains("raise an exception"), "{err}");
    Ok(())
}

async fn wrong_type_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("degrees", WeftType::parse("Number").expect("Number parses"));
    rig.wire_output("error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'degrees': 'warm'}" })).await.failure()?;
    assert!(err.starts_with("type error") && err.contains("'degrees'"), "{err}");
    Ok(())
}

//! ExecPython self-tests, all on the fake tier: the script's Python
//! process (`run_python`, which is async, and the runner already drives
//! every test inside a runtime of its own), then the full node body
//! (custom ports in, pulses out).

use serde_json::{json, Value};

use weft::{CancellationFlag, FakeRig, NodeTest, WeftError, WeftResult, WeftType};

use super::{run_python, ExecPythonNode, Interpreters};

/// `run_python` on processes of its own, to its end.
async fn python(code: &str, inputs: Vec<(String, Value)>) -> WeftResult<Vec<(String, Value)>> {
    let interpreters = Interpreters::new();
    run_python(&interpreters, code, inputs, &CancellationFlag::new()).await
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("returned_dict_maps_ports_to_values", |_rig| async {
            let out = python(
                "return {'sum': a + b, 'label': f'{a}+{b}'}",
                vec![("a".into(), json!(2)), ("b".into(), json!(3))],
            )
            .await?;
            assert_eq!(
                out,
                vec![
                    ("sum".to_string(), json!(5)),
                    ("label".to_string(), json!("2+3")),
                ]
            );
            Ok(())
        }),
        NodeTest::fake("an_empty_dict_yields_no_pulses", |_rig| async {
            let out = python("return {}", Vec::new()).await?;
            assert!(out.is_empty());
            Ok(())
        }),
        NodeTest::fake("a_missing_return_is_a_type_error", |_rig| async {
            for code in ["x = 1", "return", "return None"] {
                let err = python(code, Vec::new()).await.expect_err("no dict, no answer");
                assert!(matches!(&err, WeftError::Type(m) if m.contains("without returning a dict")), "{code}: {err}");
            }
            Ok(())
        }),
        NodeTest::fake("an_exception_is_a_node_failure_carrying_the_traceback", |_rig| async {
            let err = python("raise ValueError('boom')", Vec::new()).await.expect_err("a raise must fail the run");
            let WeftError::NodeFailed(message) = &err else { panic!("a raise is catchable: {err}") };
            assert!(message.contains("ValueError: boom"), "{message}");
            assert!(message.contains("Traceback"), "{message}");
            Ok(())
        }),
        NodeTest::fake("a_syntax_error_is_an_input_error", |_rig| async {
            let err = python("return {'a': 1", Vec::new()).await.expect_err("code that does not compile");
            assert!(matches!(&err, WeftError::Input(m) if m.contains("does not compile") && m.contains("SyntaxError")), "{err}");
            Ok(())
        }),
        NodeTest::fake("a_non_dict_return_is_a_type_error", |_rig| async {
            let err = python("return 3", Vec::new()).await.expect_err("a scalar return must fail");
            assert!(matches!(&err, WeftError::Type(m) if m.contains("must return a dict")), "{err}");
            Ok(())
        }),
        NodeTest::fake("a_value_with_no_json_form_is_a_type_error", |_rig| async {
            let err = python("return {'s': {1, 2}}", Vec::new()).await.expect_err("a set has no port value");
            assert!(matches!(&err, WeftError::Type(m) if m.contains("'s'") && m.contains("set")), "{err}");
            Ok(())
        }),
        NodeTest::fake("json_round_trips_through_python", |_rig| async {
            let value = json!({ "s": "x", "n": 1.5, "b": true, "z": Value::Null,
                                "l": [1, 2], "o": { "k": "v" } });
            let out = python("return {'echo': data}", vec![("data".into(), value.clone())]).await?;
            assert_eq!(out, vec![("echo".to_string(), value)]);
            Ok(())
        }),
        NodeTest::fake("what_a_script_prints_stays_out_of_its_answer", |_rig| async {
            let out = python("print('hello')\nreturn {'ok': True}", Vec::new()).await?;
            assert_eq!(out, vec![("ok".to_string(), json!(true))]);
            Ok(())
        }),
        NodeTest::fake("a_script_reading_stdin_reads_nothing_and_the_next_request_still_lands", |_rig| async {
            let interpreters = Interpreters::new();
            let flag = CancellationFlag::new();
            let read = run_python(&interpreters, "import sys\nreturn {'read': sys.stdin.read()}", Vec::new(), &flag).await;
            let next = run_python(&interpreters, "return {'ok': True}", Vec::new(), &flag).await;
            assert_eq!(read?, vec![("read".to_string(), json!(""))]);
            assert_eq!(next?, vec![("ok".to_string(), json!(true))]);
            Ok(())
        }),
        NodeTest::fake("scripts_share_processes_and_run_side_by_side", |_rig| async {
            let interpreters = Interpreters::new();
            let flag = CancellationFlag::new();
            let (a, b) = tokio::join!(
                run_python(&interpreters, "import time\ntime.sleep(0.2)\nreturn {'a': 1}", Vec::new(), &flag),
                run_python(&interpreters, "import time\ntime.sleep(0.2)\nreturn {'b': 2}", Vec::new(), &flag),
            );
            assert_eq!(a?, vec![("a".to_string(), json!(1))]);
            assert_eq!(b?, vec![("b".to_string(), json!(2))]);
            Ok(())
        }),
        NodeTest::fake("a_cancel_stops_a_running_script_and_the_next_one_runs", |_rig| async {
            let interpreters = Interpreters::new();
            let flag = std::sync::Arc::new(CancellationFlag::new());
            let stopping = flag.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                stopping.cancel_because(weft::run::CancelCause::User);
            });
            let result = run_python(&interpreters, "while True:\n    pass", Vec::new(), &flag).await;
            assert!(matches!(result, Err(WeftError::Cancelled)), "{result:?}");
            let next = run_python(&interpreters, "return {'ran': True}", Vec::new(), &CancellationFlag::new()).await;
            assert_eq!(next?, vec![("ran".to_string(), json!(true))]);
            Ok(())
        }),
        NodeTest::fake("a_cancelled_script_never_starts", |_rig| async {
            let interpreters = Interpreters::new();
            let flag = CancellationFlag::new();
            flag.cancel_because(weft::run::CancelCause::User);
            let result = run_python(&interpreters, "return {'ran': True}", Vec::new(), &flag).await;
            assert!(matches!(result, Err(WeftError::Cancelled)), "{result:?}");
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
        NodeTest::fake("a_value_heavier_than_a_wire_comes_out_on_error_naming_the_port", heavy_value_wired),
        NodeTest::fake("a_declared_secret_arrives_revealed_and_leaves_as_a_secret", secret_round_trip),
        NodeTest::fake("a_secret_an_input_does_not_declare_fails_before_the_script", undeclared_secret_input),
    ]
}

/// A secret inside an input whose type declares none (a `JsonDict`) fails
/// the node naming the input and where the secret is, before the script
/// runs: the script would have been handed the marker.
async fn undeclared_secret_input(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("data", WeftType::parse("JsonDict").expect("parses"));
    rig.output_type("seen", WeftType::parse("String").expect("parses"));
    let token = rig.secret(json!("tok-123"));
    let err = rig
        .run(&ExecPythonNode, json!({ "code": "return {'seen': str(data)}", "data": { "token": token } }))
        .await
        .failure()?;
    assert!(err.contains("`data` holds a secret at /token"), "{err}");
    assert!(err.contains("the type of what is wired into `data`"), "{err}");
    Ok(())
}

/// An input declared a secret binds as its value, and a value returned on
/// an output declared a secret leaves as one: the output is its marker,
/// what it holds is what the script returned.
async fn secret_round_trip(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("token", WeftType::parse("Secret[String]").expect("parses"));
    rig.output_type("length", WeftType::parse("Number").expect("parses"));
    rig.output_type("renewed", WeftType::parse("Secret[String]").expect("parses"));
    let token = rig.secret(json!("tok-123"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({ "code": "return {'length': len(token), 'renewed': token + '-2'}", "token": token }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["length"], json!(7));
    let renewed = &outcome.outputs["renewed"];
    assert!(weft::secret::Secret::from_value(renewed).is_some(), "{renewed}");
    assert_eq!(rig.revealed(renewed)?, json!("tok-123-2"));
    Ok(())
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

/// A stored file binds as the inside of its marker (`photo["filename"]`)
/// with its links beside it (`internalUrl` for the script, `externalUrl`
/// and `public` for another service), and handed back on a file-typed
/// output it leaves as the marker the wire expects, kind read off its
/// mime, without the links.
async fn file_round_trip(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("p.png", "image/png", b"PNG!".to_vec());
    let key = weft::storage::StoredFile::from_value(&file)?.key;
    rig.input_type("photo", WeftType::parse("Image").expect("Image parses"));
    rig.output_type("name", WeftType::parse("String").expect("String parses"));
    rig.output_type("links", WeftType::parse("String").expect("String parses"));
    rig.output_type("same", WeftType::parse("Image").expect("Image parses"));
    let outcome = rig
        .run(
            &ExecPythonNode,
            json!({
                "code": "assert '__weft_image__' not in photo\nreturn {'name': photo['filename'] + ':' + photo['mimeType'], 'links': ' '.join([photo['internalUrl'], photo['externalUrl'], str(photo['public'])]), 'same': photo}",
                "photo": file,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["name"], json!("p.png:image/png"));
    let ttl = weft::storage::DEFAULT_LINK_SECS;
    assert_eq!(
        outcome.outputs["links"],
        json!(format!(
            "{}/{key}?ttl={ttl} {}/files/{key}?ttl={ttl} False",
            weft::node_test::FAKE_STORE_BASE,
            weft::node_test::FAKE_LINK_BASE
        ))
    );
    assert_eq!(outcome.outputs["same"], file, "the marker comes back whole, without the links");
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

/// A script that calls out and fails, the case `_error` exists for.
const FAILING_CALL: &str = "raise ConnectionError('the weather service is down')";

async fn raise_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("_error");
    let outcome = rig.run(&ExecPythonNode, json!({ "code": FAILING_CALL })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
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
    rig.wire_output("_error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'weather': 'sun'" })).await.failure()?;
    assert!(err.starts_with("input error") && err.contains("does not compile"), "{err}");
    Ok(())
}

async fn undeclared_key_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("_error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'wether': 'sun'}" })).await.failure()?;
    assert!(err.starts_with("type error") && err.contains("'wether'") && err.contains("weather"), "{err}");
    Ok(())
}

async fn returned_error_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("weather", WeftType::parse("String").expect("String parses"));
    rig.wire_output("_error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'_error': 'no weather today'}" })).await.failure()?;
    assert!(err.starts_with("type error") && err.contains("raise an exception"), "{err}");
    Ok(())
}

/// Two hundred thousand characters weigh 196 KB, past the 100 KB a
/// wire carries: the emission is refused as a failure of this run's
/// data, so `_error` catches it, and it says which port and what to do.
async fn heavy_value_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("report", WeftType::parse("String").expect("String parses"));
    rig.wire_output("_error");
    let outcome = rig.run(&ExecPythonNode, json!({ "code": "return {'report': 'x' * 200000}" })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("port 'report'") && error.contains("carries 196 KB"), "{error}");
    assert!(error.contains("Return less"), "it names first the fix a script can make: {error}");
    assert!(!outcome.outputs.contains_key("report"), "nothing heavy went out");
    Ok(())
}

async fn wrong_type_wired(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("degrees", WeftType::parse("Number").expect("Number parses"));
    rig.wire_output("_error");
    let err = rig.run(&ExecPythonNode, json!({ "code": "return {'degrees': 'warm'}" })).await.failure()?;
    assert!(err.starts_with("type error") && err.contains("'degrees'"), "{err}");
    Ok(())
}

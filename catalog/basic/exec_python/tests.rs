//! ExecPython self-tests: the pure interpreter bridge (`run_python`)
//! at the basic tier, the full node body (custom ports in, pulses out)
//! at the fake tier.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

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
        NodeTest::basic("bare_return_yields_no_pulses", || {
            let out = run_python("return", Vec::new())?;
            assert!(out.is_empty());
            Ok(())
        }),
        NodeTest::basic("an_exception_carries_the_traceback", || {
            let err = run_python("raise ValueError('boom')", Vec::new())
                .expect_err("a raise must fail the run")
                .to_string();
            assert!(err.contains("boom"), "{err}");
            assert!(err.contains("Traceback"), "{err}");
            Ok(())
        }),
        NodeTest::basic("a_non_dict_return_is_refused", || {
            let err = run_python("return 3", Vec::new())
                .expect_err("a scalar return must fail")
                .to_string();
            assert!(err.contains("expected a dict return"), "{err}");
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
    assert!(!outcome.outputs.contains_key("miss"), "None means no pulse");
    Ok(())
}

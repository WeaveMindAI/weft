//! ExecPython: run a user-supplied Python snippet inside the
//! worker using an embedded CPython interpreter (PyO3).
//!
//! Wiring:
//!
//! - The catalog metadata declares no inputs/outputs. Users write
//!   the ports inline on the invocation:
//!       `foo = ExecPython(a: Number) -> (b: String) { code: "..." }`.
//!   The compiler's `canAddInputPorts` / `canAddOutputPorts`
//!   feature (handled by `merge_ports` in enrich.rs) materializes
//!   those ports onto the node definition before execution.
//!
//! - The `code` config field carries the Python source. Whatever
//!   the user writes there is wrapped in a zero-arg closure whose
//!   locals include every input port value by name. `return <dict>`
//!   in the user code supplies the output pulses.
//!
//! - Inputs are converted serde_json → Python via `json_to_py`. A
//!   recursive walk over the JSON tree keeps types straightforward:
//!   strings to `str`, numbers to `int`/`float`, nulls to `None`,
//!   arrays to `list`, objects to `dict`. No custom classes leak
//!   across the boundary; we round-trip through JSON twice per
//!   call but the shape is simple and predictable.
//!
//! - The return value must be a dict keyed by output port name.
//!   A missing key OR a key set to `None` means "no pulse on
//!   that port": downstream nodes receive a null pulse and the
//!   normal skip propagation kicks in (this matches how the
//!   user's weather example uses
//!   `{"weather": None, "error": "..."}`).
//!
//! - Python exceptions become `WeftError::NodeExecution` with the
//!   full traceback in the message so the UI modal shows what
//!   actually went wrong instead of a bare `ValueError`.
//!
//! Isolation: the worker pod IS the isolation boundary. We don't
//! sandbox the Python further because a spawned pod already can't
//! see anything outside what the dispatcher grants it. See
//! docs/v2-design.md on worker execution for the full threat
//! model.

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyList, PyString};
use pyo3::ToPyObject;
use serde_json::{Map, Number, Value};

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftError, WeftResult};

pub struct ExecPythonNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for ExecPythonNode {
    fn node_type(&self) -> &'static str {
        "ExecPython"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("ExecPython metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let code: String = ctx.config.get("code")?;

        // Bind every input port value under its port name in the
        // Python namespace. `ctx.input.iter()` only yields ports the
        // runtime delivered a pulse for (including null pulses from
        // skipped upstreams), which matches the "if this input was
        // null, branch" pattern in user code.
        let inputs: Vec<(String, Value)> = ctx
            .input
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // PyO3 needs the GIL which it acquires on whatever sync
        // thread we call from. Hop off the async executor for the
        // blocking call so we don't stall other node invocations.
        let result = tokio::task::spawn_blocking(move || run_python(&code, inputs))
            .await
            .map_err(|e| {
                WeftError::NodeExecution(format!("ExecPython blocking task panicked: {e}"))
            })??;

        // Assemble NodeOutput. `None` / missing keys produce no
        // pulse, matching the Python contract where returning
        // {"x": None} skips port x.
        let mut out = NodeOutput::empty();
        for (port, value) in result {
            if !matches!(value, Value::Null) {
                out = out.set(port, value);
            }
        }
        Ok(out)
    }
}

/// Execute `code` with the given input bindings and return the
/// raw key-value pairs the user returned. The engine drops pulses
/// on ports that aren't wired downstream, so filtering here would
/// be a duplicate guard.
fn run_python(code: &str, inputs: Vec<(String, Value)>) -> WeftResult<Vec<(String, Value)>> {
    Python::with_gil(|py| -> WeftResult<Vec<(String, Value)>> {
        // Build the wrapper source once per call. Wrapping in a
        // function lets the user write `return {...}` naturally.
        // The signature lists every input port so Python's scope
        // rules do the right thing (closures, shadowing, etc).
        let param_names: Vec<String> =
            inputs.iter().map(|(k, _)| k.clone()).collect();
        let wrapper_source = format!(
            "def __weft_user_fn({params}):\n{body}\n",
            params = param_names.join(", "),
            body = indent_block(code, "    "),
        );

        let globals = PyDict::new_bound(py);
        py.run_bound(&wrapper_source, Some(&globals), None)
            .map_err(|err| py_error_to_weft(py, err, "compiling user code"))?;
        let user_fn = globals
            .get_item("__weft_user_fn")
            .map_err(|err| py_error_to_weft(py, err, "locating __weft_user_fn"))?
            .ok_or_else(|| {
                WeftError::NodeExecution(
                    "internal: ExecPython wrapper did not define __weft_user_fn".into(),
                )
            })?;

        // Convert each input into a Python value and call the
        // wrapper as a positional-arg tuple matching the signature.
        let args = PyList::empty_bound(py);
        for (_, v) in &inputs {
            let py_val = json_to_py(py, v)
                .map_err(|err| py_error_to_weft(py, err, "converting input to Python"))?;
            args.append(py_val)
                .map_err(|err| py_error_to_weft(py, err, "building arg list"))?;
        }
        let ret = user_fn
            .call1(args.to_tuple())
            .map_err(|err| py_error_to_weft(py, err, "running user code"))?;

        // `return` with no value or `return None` yields no pulses.
        if ret.is_none() {
            return Ok(Vec::new());
        }

        let dict = ret.downcast::<PyDict>().map_err(|_| {
            let type_name = ret
                .get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_else(|_| "<unknown>".to_string());
            WeftError::NodeExecution(format!(
                "ExecPython: expected a dict return, got {type_name}"
            ))
        })?;

        let mut out: Vec<(String, Value)> = Vec::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract().map_err(|err| {
                py_error_to_weft(py, err, "reading output dict key")
            })?;
            let json_val = py_to_json(py, &v)
                .map_err(|err| py_error_to_weft(py, err, "converting output to JSON"))?;
            out.push((key, json_val));
        }
        Ok(out)
    })
}

/// Indent every line of `s` with `prefix`. Used so the user's code
/// nests correctly under `def __weft_user_fn(...):`.
fn indent_block(s: &str, prefix: &str) -> String {
    if s.is_empty() {
        return format!("{prefix}pass");
    }
    s.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format a PyErr into a `WeftError::NodeExecution` carrying the
/// full Python traceback. Users debugging their own Python code
/// rely on this to see line numbers and the exception type.
fn py_error_to_weft(py: Python<'_>, err: PyErr, stage: &str) -> WeftError {
    // Capture the Python-side formatted traceback. If that fails
    // (because e.g. the traceback module can't be imported on some
    // exotic embedding), fall back to Debug repr.
    let traceback = err
        .traceback_bound(py)
        .and_then(|tb| tb.format().ok())
        .unwrap_or_default();
    let value_repr = err.value_bound(py).to_string();
    let message = if traceback.trim().is_empty() {
        format!("ExecPython failed {stage}: {value_repr}")
    } else {
        format!("ExecPython failed {stage}: {value_repr}\n{traceback}")
    };
    WeftError::NodeExecution(message)
}

/// Convert a serde_json Value to a Python object. Types:
/// - Null → None
/// - Bool → bool
/// - Number → int if it's an exact int, else float
/// - String → str
/// - Array → list of converted items
/// - Object → dict of (String key → converted value)
fn json_to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    match v {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => Ok(PyBool::new_bound(py, *b).to_owned().into_any()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py).into_bound(py))
            } else if let Some(u) = n.as_u64() {
                Ok(u.to_object(py).into_bound(py))
            } else if let Some(f) = n.as_f64() {
                Ok(PyFloat::new_bound(py, f).into_any())
            } else {
                // serde_json's Number should always be one of the
                // above. Kept for completeness.
                Ok(py.None().into_bound(py))
            }
        }
        Value::String(s) => Ok(PyString::new_bound(py, s.as_str()).into_any()),
        Value::Array(items) => {
            let list = PyList::empty_bound(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        Value::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, val) in obj {
                dict.set_item(k, json_to_py(py, val)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Convert a Python object back to serde_json. Unsupported types
/// (sets, custom classes, bytes) fall back to their repr() so the
/// user at least sees something rather than a crash.
fn py_to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(u) = obj.extract::<u64>() {
        return Ok(Value::Number(u.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("non-finite float"));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(py_to_json(py, &item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json(py, &v)?);
        }
        return Ok(Value::Object(map));
    }
    // Fallback: repr() so the user at least sees something
    // recognizable in their Debug output.
    Ok(Value::String(obj.repr()?.to_string()))
}

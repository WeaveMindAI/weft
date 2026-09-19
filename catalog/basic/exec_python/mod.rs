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
//!   locals include every DECLARED input port by name: one that
//!   received nothing is bound to `None`, so a script reads an
//!   optional input with a plain `if problem is None`. `return
//!   <dict>` in the user code supplies the output pulses.
//!
//! - Inputs are converted serde_json → Python via `json_to_py`. A
//!   recursive walk over the JSON tree keeps types straightforward:
//!   strings to `str`, numbers to `int`/`float`, nulls to `None`,
//!   arrays to `list`, objects to `dict`. No custom classes leak
//!   across the boundary; we round-trip through JSON twice per
//!   call but the shape is simple and predictable.
//!
//! - A file arrives UNWRAPPED on a port DECLARED as one (the same
//!   question the way out asks, so a dict that happens to carry a
//!   marker key on a `JsonDict` port stays exactly as it is). On a
//!   wire a file is the marker
//!   `{"__weft_image__": {key, filename, mimeType, sizeBytes}}` (one
//!   sentinel per kind); the engine adds a `url` minted for this
//!   firing before the node runs. The script gets the inside of the
//!   marker as a plain dict, so `photo["url"]` is the download link
//!   and `photo["filename"]` the name, with no sentinel to unwrap.
//!   The kind is not lost: a file returned on a file-typed output is
//!   wrapped back into its marker from its mime type, and the engine
//!   strips the link on the way out as it does for every node.
//!
//! - The return value must be a dict keyed by output port name.
//!   A missing key OR a key set to `None` means "no pulse on
//!   that port": the port is left out of the node's output
//!   entirely, nothing travels down it, and the normal skip
//!   propagation kicks in (this is how the weather example's
//!   `{"weather": None, "error": "..."}` closes one branch and
//!   opens the other).
//!
//! - Python exceptions become node failures with the full traceback
//!   in the message so the UI modal shows what actually went wrong
//!   instead of a bare `ValueError`.
//!
//! Isolation: the worker the node runs in IS the isolation boundary;
//! the Python executes there with the same access that worker already
//! has, and is not sandboxed further. Running a project therefore runs
//! its ExecPython code with that worker's privileges, the same trust
//! model as running the project's own program.

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyList, PyString};
use pyo3::ToPyObject;
use serde_json::{Map, Number, Value};

use weft::node::NodeOutput;
use weft::storage::media::{media_slots, substitute_media};
use weft::weft_type::FileKind;
use weft::{node_error, ExecutionContext, Node, NodeErrExt, NodeManifest, StoredFile, WeftError, WeftResult, WeftType};

#[derive(NodeManifest)]
pub struct ExecPythonNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ExecPythonNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let code: String = ctx.inputs.get("code")?;

        // Bind every DECLARED data input under its name in the Python
        // namespace. The declared set is the user's inline port list
        // (the node's own settings, `code`, never bind as variables);
        // a port with nothing in the bag (a skipped upstream, an
        // optional input nobody wired) binds as `None`, so the script
        // never meets an undefined name. Stored files bind unwrapped.
        let settings: std::collections::HashSet<&str> = ctx.inputs.declared().map(|(k, _)| k.as_str()).collect();
        let names: std::collections::BTreeSet<&String> = ctx
            .declared_inputs()
            .keys()
            .filter(|name| !settings.contains(name.as_str()))
            .chain(ctx.inputs.custom().map(|(name, _)| name))
            .collect();
        let inputs: Vec<(String, Value)> = names
            .into_iter()
            .map(|name| {
                let ty = ctx.declared_inputs().get(name);
                let value = ctx.inputs.raw(name).map(|v| unwrap_files(v, ty)).unwrap_or(Value::Null);
                (name.clone(), value)
            })
            .collect();

        // PyO3 needs the GIL which it acquires on whatever sync
        // thread we call from. Hop off the async executor for the
        // blocking call so we don't stall other node invocations.
        let result = tokio::task::spawn_blocking(move || run_python(&code, inputs))
            .await
            .node_err("ExecPython blocking task panicked")??;

        // Assemble NodeOutput. `None` / missing keys produce no
        // pulse, matching the Python contract where returning
        // {"x": None} skips port x. A file handed back on a
        // file-typed port goes out as the marker the wire expects.
        let mut out = NodeOutput::new();
        for (port, value) in result {
            if matches!(value, Value::Null) {
                continue;
            }
            let value = match ctx.output_type(&port) {
                Some(ty) if ty.references_file() => wrap_files(&value, &ty)?,
                _ => value,
            };
            out = out.set(port, value);
        }
        ctx.pulse_downstream(out).await
    }
}

/// The inside of every stored-file marker sitting where the port's
/// DECLARED type says a file goes: `{"__weft_image__": {...}}` becomes
/// `{...}`. The type is the question, exactly as on the way out
/// ([`wrap_files`]), so a dict that merely happens to carry a marker
/// key on a port declared `JsonDict` flows whole. A port with no file
/// anywhere in its type is copied untouched.
fn unwrap_files(value: &Value, ty: Option<&WeftType>) -> Value {
    let Some(ty) = ty.filter(|ty| ty.references_file()) else {
        return value.clone();
    };
    let replacements = media_slots(value, ty)
        .into_iter()
        .filter_map(|slot| {
            let obj = slot.as_object()?;
            let kind = FileKind::from_marker_obj(obj)?;
            let inner = obj.get(kind.marker_key())?.clone();
            Some((slot.to_string(), inner))
        })
        .collect();
    substitute_media(value, ty, &replacements)
}

/// The inverse of [`unwrap_files`] on an output typed as a file: every
/// media slot of `ty` holding a bare file dict (the shape the script
/// received, either handle) is wrapped back into the marker its mime
/// type says. A slot already carrying a marker passes; anything else
/// on a file slot is refused by name, so a script cannot put a string
/// where a picture goes.
fn wrap_files(value: &Value, ty: &WeftType) -> WeftResult<Value> {
    let mut replacements = std::collections::HashMap::new();
    for slot in media_slots(value, ty) {
        let Some(obj) = slot.as_object() else {
            return Err(node_error(format!(
                "ExecPython returned {} on a file port; return the dict the file arrived as",
                weft::truncate_user_string(&slot.to_string(), 120)
            )));
        };
        if FileKind::from_marker_obj(obj).is_some() {
            continue;
        }
        // A file that lives at an external URL carries `url` where a
        // stored file carries `key` (see `FileHandle::Url`), so it is
        // not a `StoredFile` and never will be. The script was handed
        // that payload and handed it straight back, so wrap it by the
        // mime it declares rather than refusing a file this node
        // unwrapped itself.
        if obj.contains_key("url") {
            let mime = obj
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            replacements.insert(
                slot.to_string(),
                WeftType::file_marker(FileKind::from_mime(mime), slot.clone()),
            );
            continue;
        }
        let file: StoredFile = serde_json::from_value(slot.clone()).map_err(|e| {
            node_error(format!("ExecPython returned a dict on a file port that is not a stored file ({e}); return the dict the file arrived as"))
        })?;
        replacements.insert(slot.to_string(), file.to_value());
    }
    Ok(substitute_media(value, ty, &replacements))
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
            .node_err("internal: ExecPython wrapper did not define __weft_user_fn")?;

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
            node_error(format!("ExecPython: expected a dict return, got {type_name}"))
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

/// Format a PyErr into a node failure carrying the full Python
/// traceback. Users debugging their own Python code rely on this
/// to see line numbers and the exception type.
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
    node_error(message)
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

/// Convert a Python object back to serde_json. Supported: None, bool,
/// int, float, str, list, dict. Anything else (sets, custom classes,
/// bytes, tuples) raises `PyTypeError`: a downstream port expecting a
/// Dict surfaces a structured failure instead of receiving a stringified
/// `repr()` that pretends to be data.
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
    // Unsupported Python type: refuse to silently downgrade to a
    // stringified `repr()`. A user wiring a downstream port expecting
    // a Dict gets a structured failure instead of a `"<set {...}>"`
    // string that pretends to be data.
    let type_name = obj
        .get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "ExecPython: unsupported return type `{type_name}` (supported: None, bool, int, float, str, list, dict)"
    )))
}
